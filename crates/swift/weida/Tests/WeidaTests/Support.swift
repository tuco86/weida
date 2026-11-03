import Foundation
import Testing

@testable import Weida

/// A runtime with a replier at /echo and a requester connected to it over
/// an in-process bus of its own.
struct Pair {
    let runtime: Runtime
    let listener: Listener
    let binding: LocalBinding
    let replier: Replier
    let requester: Requester

    init() async throws {
        let bus = "swift-test-\(UUID().uuidString)"
        runtime = try Runtime.owned()
        listener = try runtime.listener()
        binding = try listener.bindInproc(bus)
        #expect(binding.bus == bus)
        replier = try listener.replier("/echo")
        #expect(replier.path == "/echo")
        let tls = try ClientTLS(trust: Trust())
        requester = try runtime.requester(tls)
        try await requester.connect("weida+inproc://\(bus)/echo")
    }

    /// Releases the pair and shuts its runtime down.
    func close() async throws {
        requester.close()
        replier.close()
        binding.close()
        listener.close()
        try await runtime.shutdown()
    }
}

/// Answers one request on `replier` with `answer(body)`, reading the body 3
/// bytes at a time; returns the body.
func serve(_ replier: Replier, meta: TransferMeta = TransferMeta(), _ answer: (Data) -> Data) async throws -> Data {
    let request = try await replier.accept()
    defer { request.close() }
    let received = try await readAll(request.takeBody(), size: 3)
    let reply = try await request.reply(meta)
    defer { reply.close() }
    try await reply.writeAll(answer(received))
    try await reply.finish().delivered()
    return received
}

/// Reads `transfer` to its end in reads of at most `size` bytes.
func readAll(_ transfer: IncomingTransfer, size: Int) async throws -> Data {
    var all = Data()
    while true {
        let chunk = try await transfer.read(upTo: size)
        if chunk.isEmpty {
            return all
        }
        #expect(chunk.count <= size)
        all.append(chunk)
    }
}

/// Waits until `condition` holds, checking every 10 ms. No bound: the CI
/// job's budget is the hang guard.
func until(_ condition: () -> Bool) async throws {
    while !condition() {
        try await Task.sleep(for: .milliseconds(10))
    }
}

/// The WeidaError `body` throws; records an issue when it throws none or
/// another error.
func weidaError<T>(_ body: () async throws -> T, sourceLocation: SourceLocation = #_sourceLocation) async -> WeidaError? {
    do {
        _ = try await body()
        Issue.record("no error", sourceLocation: sourceLocation)
    } catch let error as WeidaError {
        return error
    } catch {
        Issue.record("\(error) is not a WeidaError", sourceLocation: sourceLocation)
    }
    return nil
}

extension Data {
    init(_ string: String) {
        self.init(string.utf8)
    }
}
