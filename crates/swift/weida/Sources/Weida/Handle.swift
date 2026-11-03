import CWeida
import Dispatch
import Foundation

/// One libweida handle owned by a Swift object (docs/c/README.md R5).
///
/// A call holds the lock for the duration of its C call and throws
/// `.closed` once the handle is closed, without calling C. `close()` and the
/// consuming methods take the lock too, so no call starts after the C drop
/// (or the consuming C call) returned. A waiting task holds no lock while it
/// is suspended.
final class Handle: @unchecked Sendable {
    private let lock = NSLock()
    private var pointer: OpaquePointer?
    private let drop: (OpaquePointer) -> Void
    /// The waits suspended on this handle; closing it ends them.
    private var waiters: [ObjectIdentifier: Waiter] = [:]

    init(_ pointer: OpaquePointer?, drop: @escaping (OpaquePointer) -> Void) {
        self.pointer = pointer
        self.drop = drop
    }

    deinit {
        if let pointer {
            drop(pointer)
        }
    }

    /// Runs `body` with the C pointer under the lock.
    func call<T>(_ body: (OpaquePointer) throws -> T) throws -> T {
        lock.lock()
        defer { lock.unlock() }
        guard let pointer else {
            throw WeidaError(.closed)
        }
        return try body(pointer)
    }

    /// Marks the handle closed without a drop and runs `body`, a C function
    /// that consumes the pointer on every return path (R6).
    func consume<T>(_ body: (OpaquePointer) throws -> T) throws -> T {
        lock.lock()
        defer { lock.unlock() }
        guard let pointer else {
            throw WeidaError(.closed)
        }
        release()
        return try body(pointer)
    }

    /// Drops the C handle once; every later call throws `.closed`, and so
    /// does every wait suspended on it.
    func close() {
        lock.lock()
        defer { lock.unlock() }
        if let pointer {
            release()
            drop(pointer)
        }
    }

    /// Marks the handle closed and ends its waits with `.closed`, before the
    /// C drop: each wait watches a duplicate of the descriptor, which its
    /// source closes once canceled. The lock is held.
    private func release() {
        pointer = nil
        for waiter in waiters.values {
            waiter.finish(.failure(WeidaError(.closed)))
        }
        waiters = [:]
    }

    /// A waiter on the descriptor `fd` reads from the pointer, registered
    /// with the handle so that closing it ends the wait.
    fileprivate func watch(_ fd: (OpaquePointer) throws -> Int64) throws -> Waiter {
        lock.lock()
        defer { lock.unlock() }
        guard let pointer else {
            throw WeidaError(.closed)
        }
        let waiter = try Waiter(fd: fd(pointer))
        waiters[ObjectIdentifier(waiter)] = waiter
        return waiter
    }

    fileprivate func unwatch(_ waiter: Waiter) {
        lock.lock()
        waiters[ObjectIdentifier(waiter)] = nil
        lock.unlock()
    }

    /// How many waits are suspended on the handle.
    var waiting: Int {
        lock.lock()
        defer { lock.unlock() }
        return waiters.count
    }
}

/// One suspension until a readiness descriptor turns readable (R11): a
/// one-shot read source over a duplicate of the descriptor, whose cancel
/// handler closes the duplicate. The first `finish` wins; it cancels the
/// source and resumes the suspended task, or keeps the outcome for the
/// continuation still to come.
private final class Waiter: @unchecked Sendable {
    /// The queue every read source runs its handlers on: they only resume a
    /// continuation or close a descriptor.
    private static let queue = DispatchQueue(label: "weida.readiness")

    private let lock = NSLock()
    private let source: DispatchSourceRead
    private var continuation: CheckedContinuation<Void, any Error>?
    private var outcome: Result<Void, any Error>?

    init(fd: Int64) throws {
        let dup = fcntl(Int32(truncatingIfNeeded: fd), F_DUPFD_CLOEXEC, 0)
        if dup < 0 {
            throw WeidaError(.io, "duplicating a readiness descriptor: errno \(errno)")
        }
        source = DispatchSource.makeReadSource(fileDescriptor: dup, queue: Waiter.queue)
        source.setCancelHandler { close(dup) }
        source.setEventHandler { [weak self] in self?.finish(.success(())) }
        source.activate()
    }

    /// Ends the wait with `result`, once.
    func finish(_ result: Result<Void, any Error>) {
        lock.lock()
        if outcome != nil {
            lock.unlock()
            return
        }
        outcome = result
        let continuation = self.continuation
        self.continuation = nil
        lock.unlock()
        source.cancel()
        continuation?.resume(with: result)
    }

    /// Suspends until `finish`; returns at once when it already ran.
    func suspend() async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
            lock.lock()
            if let outcome {
                lock.unlock()
                continuation.resume(with: outcome)
                return
            }
            self.continuation = continuation
            lock.unlock()
        }
    }
}

/// Runs `attempt` until it yields a value, suspending in between until the
/// descriptor `fd` writes is readable (R11: the next try will not block).
///
/// `attempt` is one non-blocking try (a try or an op wait with
/// `timeout_ms = 0`): it returns nil on `-WEIDA_EAGAIN` and throws any other
/// failure with the thread's record, read in the same synchronous call.
/// `owner`, when not nil, is the handle the wait belongs to: `fd` gets its
/// pointer, and closing it ends the wait with `.closed`. Without an owner
/// `fd` gets nil. Cancelling the task ends the wait with
/// `CancellationError`.
func wait<T>(
    owner: Handle?,
    fd: (OpaquePointer?) throws -> Int64,
    attempt: () throws -> T?
) async throws -> T {
    while true {
        if let value = try attempt() {
            return value
        }
        try Task.checkCancellation()
        let waiter = try owner?.watch { try fd($0) } ?? Waiter(fd: fd(nil))
        defer {
            owner?.unwatch(waiter)
            waiter.finish(.success(()))
        }
        try await withTaskCancellationHandler {
            try await waiter.suspend()
        } onCancel: {
            waiter.finish(.failure(CancellationError()))
        }
    }
}

/// The descriptor a `weida_*_fd` function writes.
func descriptor(_ fd: (UnsafeMutablePointer<Int64>) -> Int32) throws -> Int64 {
    var out: Int64 = -1
    try check(fd(&out))
    return out
}

/// Runs `body` with `string` as a C string; a string with a NUL in it is
/// `.inval`, which C would otherwise see cut short.
func withCString<T>(_ string: String, _ body: (UnsafePointer<CChar>) throws -> T) throws -> T {
    if string.utf8.contains(0) {
        throw WeidaError(.inval, "a string holds a NUL byte")
    }
    return try string.withCString(body)
}

/// Copies a string a handle lends (R7); nil for NULL.
func string(_ pointer: UnsafePointer<CChar>?, _ length: Int) -> String? {
    guard let pointer else {
        return nil
    }
    return String(decoding: UnsafeRawBufferPointer(start: pointer, count: length), as: UTF8.self)
}

/// Copies `length` bytes at `pointer` into a Data; empty for NULL.
func bytes(_ pointer: UnsafePointer<UInt8>?, _ length: Int) -> Data {
    guard let pointer, length > 0 else {
        return Data()
    }
    return Data(bytes: pointer, count: length)
}
