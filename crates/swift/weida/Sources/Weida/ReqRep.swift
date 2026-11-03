import CWeida
import Foundation
import Security

/// The requesting half of Req/Rep: it dials repliers and sends them
/// requests.
public final class Requester: Sendable {
    let handle: Handle

    init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_requester_drop($0) }
    }

    /// Dials `url`, e.g. `weida://sha256:...@127.0.0.1:7443/echo` or
    /// `weida+inproc://bus/echo`. A peer whose key is not trusted is
    /// `.untrusted`. The runtime redials it when the connection is lost.
    public func connect(_ url: String) async throws {
        var op: OpaquePointer?
        try withCString(url) { curl in
            try handle.call { try check(weida_requester_connect($0, curl, &op)) }
        }
        defer { weida_requester_connect_op_drop(op) }
        _ = try await wait(owner: handle) { _ in
            try descriptor { weida_requester_connect_op_fd(op, $0) }
        } attempt: {
            try pending(weida_requester_connect_op_wait(op, 0))
        }
    }

    /// Sends `body`, which is copied, as one request and returns the reply
    /// as soon as its header arrives. A request the replier dropped is
    /// `.noReply`.
    public func request(_ body: Data) async throws -> IncomingTransfer {
        var op: OpaquePointer?
        try body.withUnsafeBytes { raw in
            try handle.call {
                try check(weida_requester_request($0, raw.bindMemory(to: UInt8.self).baseAddress, raw.count, &op))
            }
        }
        defer { weida_requester_request_op_drop(op) }
        let out: OpaquePointer = try await wait(owner: handle) { _ in
            try descriptor { weida_requester_request_op_fd(op, $0) }
        } attempt: {
            var out: OpaquePointer?
            return try pending(weida_requester_request_op_wait(op, 0, &out)).map { _ in out! }
        }
        return IncomingTransfer(out)
    }

    /// Drops the requester's connections; a pending wait throws `.closed`.
    public func close() {
        handle.close()
    }
}

/// The replying half of Req/Rep, serving one endpoint path.
public final class Replier: Sendable {
    let handle: Handle

    init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_replier_drop($0) }
    }

    /// The endpoint path the replier serves; "" once it is closed.
    public var path: String {
        (try? handle.call { r in
            var n = 0
            return string(weida_replier_path(r, &n), n)
        }) ?? ""
    }

    /// Takes the next request as soon as its header arrives. Several tasks
    /// may accept on one replier; a cancelled accept loses nothing.
    public func accept() async throws -> IncomingRequest {
        let out: OpaquePointer = try await wait(owner: handle) { r in
            try descriptor { weida_replier_fd(r, $0) }
        } attempt: {
            try handle.call { r in
                var out: OpaquePointer?
                return try pending(weida_replier_accept(r, 0, &out)).map { _ in out! }
            }
        }
        return IncomingRequest(out)
    }

    /// Releases the replier; a pending accept throws `.closed`.
    public func close() {
        handle.close()
    }
}

/// One accepted request: its body and the right to reply. Closed without a
/// reply, it answers the requester with `.noReply`.
public final class IncomingRequest: Sendable {
    let handle: Handle

    init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_incoming_request_drop($0) }
    }

    /// The request's metadata.
    public func meta() throws -> IncomingMeta {
        try handle.call { q in
            var m: OpaquePointer?
            try check(weida_incoming_request_meta(q, &m))
            defer { weida_incoming_meta_drop(m) }
            return IncomingMeta(m)
        }
    }

    /// Detaches the request's body, to be read or collected; a second call
    /// is `.closed`.
    public func takeBody() throws -> IncomingTransfer {
        let out = try handle.call { q in
            var out: OpaquePointer?
            try check(weida_incoming_request_take_body(q, &out))
            return out
        }
        return IncomingTransfer(out)
    }

    /// Opens the reply stream with `meta`. It consumes the request: a
    /// second reply is `.closed`. Closing the returned transfer unfinished
    /// resets the reply with `.canceled`.
    public func reply(_ meta: TransferMeta = TransferMeta()) async throws -> OutgoingTransfer {
        let cmeta = try meta.open()
        defer { weida_transfer_meta_drop(cmeta) }
        var op: OpaquePointer?
        try handle.consume { try check(weida_incoming_request_reply($0, cmeta, &op)) }
        defer { weida_incoming_request_reply_op_drop(op) }
        let out: OpaquePointer = try await wait(owner: nil) { _ in
            try descriptor { weida_incoming_request_reply_op_fd(op, $0) }
        } attempt: {
            var out: OpaquePointer?
            return try pending(weida_incoming_request_reply_op_wait(op, 0, &out)).map { _ in out! }
        }
        return OutgoingTransfer(out)
    }

    /// Releases the request; one not replied to answers `.noReply`.
    public func close() {
        handle.close()
    }
}

/// An inbound payload, read like a socket or collected whole. Closed
/// unread, it refuses the payload with `.rejected`.
public final class IncomingTransfer: Sendable {
    let handle: Handle

    init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_incoming_transfer_drop($0) }
    }

    /// The transfer's metadata.
    public func meta() throws -> IncomingMeta {
        try handle.call { t in
            var m: OpaquePointer?
            try check(weida_incoming_transfer_meta(t, &m))
            defer { weida_incoming_meta_drop(m) }
            return IncomingMeta(m)
        }
    }

    /// Reads up to `count` bytes of the payload as soon as some arrive:
    /// empty at its end, `.canceled` when the peer canceled it. A count of
    /// 0 is `.inval`.
    public func read(upTo count: Int) async throws -> Data {
        var buffer = Data(count: count)
        let n: Int = try await wait(owner: handle) { t in
            try descriptor { weida_incoming_transfer_fd(t, $0) }
        } attempt: {
            try handle.call { t in
                var n = 0
                let rc = buffer.withUnsafeMutableBytes {
                    weida_incoming_transfer_read(t, $0.bindMemory(to: UInt8.self).baseAddress, $0.count, 0, &n)
                }
                return try pending(rc).map { _ in n }
            }
        }
        buffer.count = n
        return buffer
    }

    /// Reads the whole remaining payload, refusing more than `maxBytes`
    /// with `.limitExceeded`. It consumes the transfer.
    public func collect(maxBytes: UInt64) async throws -> Message {
        var op: OpaquePointer?
        try handle.consume { try check(weida_incoming_transfer_collect($0, maxBytes, &op)) }
        defer { weida_incoming_transfer_collect_op_drop(op) }
        let msg: OpaquePointer = try await wait(owner: nil) { _ in
            try descriptor { weida_incoming_transfer_collect_op_fd(op, $0) }
        } attempt: {
            var out: OpaquePointer?
            return try pending(weida_incoming_transfer_collect_op_wait(op, 0, &out)).map { _ in out! }
        }
        defer { weida_msg_drop(msg) }
        let payload = bytes(weida_msg_data(msg), weida_msg_len(msg))
        var m: OpaquePointer?
        try check(weida_msg_meta(msg, &m))
        defer { weida_incoming_meta_drop(m) }
        return Message(payload: payload, meta: IncomingMeta(m))
    }

    /// Releases the transfer; an unread payload is refused with
    /// `.rejected`.
    public func close() {
        handle.close()
    }
}

/// An outbound payload, written like a socket and ended with `finish()`.
/// Closed unfinished, it resets the payload with `.canceled`.
public final class OutgoingTransfer: Sendable {
    let handle: Handle

    init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_outgoing_transfer_drop($0) }
    }

    /// Writes some of `data` as soon as flow control takes it and returns
    /// how much it wrote; empty data writes 0. A peer that refused the
    /// payload fails it, e.g. with `.rejected`.
    public func write(_ data: Data) async throws -> Int {
        try await wait(owner: handle) { t in
            try descriptor { weida_outgoing_transfer_fd(t, $0) }
        } attempt: {
            try handle.call { t in
                var n = 0
                let rc = data.withUnsafeBytes {
                    weida_outgoing_transfer_write(t, $0.bindMemory(to: UInt8.self).baseAddress, $0.count, 0, &n)
                }
                return try pending(rc).map { _ in n }
            }
        }
    }

    /// Writes all of `data`, the loop R3c leaves to the caller.
    public func writeAll(_ data: Data) async throws {
        var rest = data[...]
        while !rest.isEmpty {
            rest = rest.dropFirst(try await write(Data(rest)))
        }
    }

    /// Ends the payload and returns its delivery receipt; it consumes the
    /// transfer. Fails only with `.expired`.
    public func finish() throws -> Delivery {
        let out = try handle.consume { t in
            var out: OpaquePointer?
            try check(weida_outgoing_transfer_finish(t, &out))
            return out
        }
        return Delivery(out)
    }

    /// Releases the transfer; an unfinished payload is reset with
    /// `.canceled`.
    public func close() {
        handle.close()
    }
}

/// The receipt of a finished outbound transfer.
public final class Delivery: Sendable {
    let handle: Handle

    init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_delivery_drop($0) }
    }

    /// Waits until the peer's transport acknowledged the whole payload, or
    /// throws the refusal or loss that stopped it. It consumes the receipt.
    public func delivered() async throws {
        var op: OpaquePointer?
        try handle.consume { try check(weida_delivery_delivered($0, &op)) }
        defer { weida_delivery_delivered_op_drop(op) }
        _ = try await wait(owner: nil) { _ in
            try descriptor { weida_delivery_delivered_op_fd(op, $0) }
        } attempt: {
            try pending(weida_delivery_delivered_op_wait(op, 0))
        }
    }

    /// Releases the receipt without waiting on it.
    public func close() {
        handle.close()
    }
}

/// The metadata an outgoing transfer carries in its header.
public struct TransferMeta: Sendable, Equatable {
    /// The opaque content type label; nil sets none.
    public var contentType: String?

    public init(contentType: String? = nil) {
        self.contentType = contentType
    }

    /// The metadata as a C handle for one call.
    fileprivate func open() throws -> OpaquePointer? {
        var m: OpaquePointer?
        try check(weida_transfer_meta_new(&m))
        do {
            if let contentType {
                try withCString(contentType) { try check(weida_transfer_meta_set_content_type(m, $0)) }
            }
        } catch {
            weida_transfer_meta_drop(m)
            throw error
        }
        return m
    }
}

/// The metadata of an inbound transfer or request, copied out at once; nil
/// where the value is absent.
public struct IncomingMeta: Sendable, Equatable {
    /// The endpoint path the transfer was sent to; nil on a reply.
    public var endpoint: String?
    /// The opaque content type label.
    public var contentType: String?
    /// The Pub/Sub topic.
    public var topic: String?
    /// The fingerprint (`sha256:` text) the sender proved in the TLS
    /// handshake.
    public var peerFingerprint: String?
    /// The certificates the sender presented, DER, leaf first.
    public var peerChainDER: [Data]?
    /// The advisory payload length the sender stated.
    public var contentLen: UInt64?
    /// The sender's per-producer sequence number.
    public var sequence: UInt64?

    init(_ m: OpaquePointer?) {
        var n = 0
        endpoint = string(weida_incoming_meta_endpoint(m, &n), n)
        contentType = string(weida_incoming_meta_content_type(m, &n), n)
        topic = string(weida_incoming_meta_topic(m, &n), n)
        peerFingerprint = string(weida_incoming_meta_peer_fingerprint(m, &n), n)
        var count = 0
        if weida_incoming_meta_peer_chain_len(m, &count) == 1 {
            peerChainDER = (0..<count).map { i in
                var len = 0
                return bytes(weida_incoming_meta_peer_chain_cert(m, i, &len), len)
            }
        }
        var value: UInt64 = 0
        contentLen = weida_incoming_meta_content_len(m, &value) == 1 ? value : nil
        sequence = weida_incoming_meta_sequence(m, &value) == 1 ? value : nil
    }

    /// The certificates the sender presented, leaf first, parsed from
    /// `peerChainDER` on each call; nil when there is none. A certificate
    /// that does not parse is `.tls`.
    public func peerChain() throws -> [SecCertificate]? {
        try peerChainDER?.map(certificate)
    }
}

/// A collected payload and the metadata of the transfer it consumed.
public struct Message: Sendable, Equatable {
    public var payload: Data
    public var meta: IncomingMeta

    public init(payload: Data, meta: IncomingMeta) {
        self.payload = payload
        self.meta = meta
    }
}
