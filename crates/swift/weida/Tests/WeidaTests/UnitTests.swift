// The unit tier: status codes, the failures that need no peer, and Req/Rep
// over the in-process transport, each test on a bus of its own.

import CWeida
import Foundation
import Testing

@testable import Weida

@Test func everyCodeIsTheHeadersCode() {
    let header: [(WeidaError.Code, Int32)] = [
        (.runtime, WEIDA_ERUNTIME),
        (.invalidAddress, WEIDA_EINVALID_ADDRESS),
        (.invalidEndpointPath, WEIDA_EINVALID_ENDPOINT_PATH),
        (.invalidFingerprint, WEIDA_EINVALID_FINGERPRINT),
        (.alreadyRegistered, WEIDA_EALREADY_REGISTERED),
        (.notConnected, WEIDA_ENOT_CONNECTED),
        (.connectionLost, WEIDA_ECONNECTION_LOST),
        (.negotiation, WEIDA_ENEGOTIATION),
        (.protocol, WEIDA_EPROTOCOL),
        (.rejected, WEIDA_EREJECTED),
        (.unknownEndpoint, WEIDA_EUNKNOWN_ENDPOINT),
        (.unsupported, WEIDA_EUNSUPPORTED),
        (.noParkedConnection, WEIDA_ENO_PARKED_CONNECTION),
        (.noReply, WEIDA_ENO_REPLY),
        (.canceled, WEIDA_ECANCELED),
        (.expired, WEIDA_EEXPIRED),
        (.indeterminate, WEIDA_EINDETERMINATE),
        (.limitExceeded, WEIDA_ELIMIT_EXCEEDED),
        (.datagramsUnavailable, WEIDA_EDATAGRAMS_UNAVAILABLE),
        (.tooLarge, WEIDA_ETOO_LARGE),
        (.tls, WEIDA_ETLS),
        (.untrusted, WEIDA_EUNTRUSTED),
        (.io, WEIDA_EIO),
        (.transport, WEIDA_ETRANSPORT),
        (.again, WEIDA_EAGAIN),
        (.intr, WEIDA_EINTR),
        (.inval, WEIDA_EINVAL),
        (.closed, WEIDA_ECLOSED),
        (.panic, WEIDA_EPANIC),
    ]
    #expect(header.count == WeidaError.Code.allCases.count)
    for (code, value) in header {
        #expect(code.rawValue == value, "\(code)")
        #expect(code.description != "unknown error", "\(code)")
    }
    let lossCauses: [(WeidaError.LossCause, Int32)] = [
        (.idleTimeout, WEIDA_LOSS_IDLE_TIMEOUT),
        (.peerClosed, WEIDA_LOSS_PEER_CLOSED),
        (.peerRestarting, WEIDA_LOSS_PEER_RESTARTING),
        (.locallyClosed, WEIDA_LOSS_LOCALLY_CLOSED),
        (.reset, WEIDA_LOSS_RESET),
        (.transportError, WEIDA_LOSS_TRANSPORT_ERROR),
    ]
    for (cause, value) in lossCauses {
        #expect(cause.rawValue == value, "\(cause)")
    }
}

@Test func aRuntimeWithNoWorkerThreadsIsARuntimeFailure() async {
    let error = await weidaError { try Runtime.owned(RuntimeConfig(workerThreads: 0, shutdownTimeout: .seconds(1))) }
    #expect(error?.code == .runtime)
    #expect(error?.message.isEmpty == false)
}

@Test func aMalformedPinIsAnInvalidFingerprint() async throws {
    let trust = try Trust()
    let error = await weidaError { try trust.addPin("nope") }
    #expect(error?.code == .invalidFingerprint)
}

@Test func aCallOnAClosedHandleIsClosed() async throws {
    let trust = try Trust()
    trust.close()
    trust.close()
    let error = await weidaError { try trust.addPin("nope") }
    #expect(error?.code == .closed)
}

@Test func aStringWithANulIsInval() async throws {
    let runtime = try Runtime.owned()
    let listener = try runtime.listener()
    let error = await weidaError { try listener.replier("/a\0b") }
    #expect(error?.code == .inval)
    listener.close()
    try await runtime.shutdown()
}

@Test func aStreamedRoundTripCarriesTheBodyAndTheContentType() async throws {
    let p = try await Pair()
    async let served = serve(p.replier, meta: TransferMeta(contentType: "text/plain")) {
        Data(String(decoding: $0, as: UTF8.self).uppercased().utf8)
    }
    let reply = try await p.requester.request(Data("ping"))
    #expect(try reply.meta().contentType == "text/plain")
    let message = try await reply.collect(maxBytes: 1024)
    #expect(message.payload == Data("PING"))
    #expect(message.meta.contentType == "text/plain")
    #expect(message.meta.endpoint == nil)
    #expect(try await served == Data("ping"))
    try await p.close()
}

@Test func readsOf3BytesReassembleTheBody() async throws {
    let p = try await Pair()
    let client = Task { try? await p.requester.request(Data("0123456789")) }
    let request = try await p.replier.accept()
    #expect(try request.meta().endpoint == "/echo")
    let body = try request.takeBody()
    #expect(try await readAll(body, size: 3) == Data("0123456789"))
    #expect(try await body.read(upTo: 3).isEmpty)
    request.close()
    _ = await client.value
    try await p.close()
}

@Test func aCollectOverItsCeilingIsLimitExceeded() async throws {
    let p = try await Pair()
    let client = Task { try? await p.requester.request(Data("four")) }
    let request = try await p.replier.accept()
    let body = try request.takeBody()
    let error = await weidaError { try await body.collect(maxBytes: 2) }
    #expect(error?.code == .limitExceeded)
    request.close()
    _ = await client.value
    try await p.close()
}

@Test func aReplyDroppedUnfinishedReadsAsCanceled() async throws {
    let p = try await Pair()
    let server = Task {
        let request = try await p.replier.accept()
        let reply = try await request.reply()
        _ = try await reply.write(Data("ha"))
        reply.close()
    }
    let reply = try await p.requester.request(Data("go"))
    let error = await weidaError { try await readAll(reply, size: 1024) }
    #expect(error?.code == .canceled)
    try await server.value
    try await p.close()
}

@Test func aSecondReplyIsClosed() async throws {
    let p = try await Pair()
    let client = Task { try? await p.requester.request(Data("once")) }
    let request = try await p.replier.accept()
    let reply = try await request.reply()
    reply.close()
    let error = await weidaError { try await request.reply() }
    #expect(error?.code == .closed)
    _ = await client.value
    try await p.close()
}

@Test func twoTasksAcceptingOnOneReplierBothGetARequest() async throws {
    let p = try await Pair()
    let reverse: @Sendable (Data) -> Data = { Data($0.reversed()) }
    let accepts = [
        Task { try await serve(p.replier, reverse) },
        Task { try await serve(p.replier, reverse) },
    ]
    try await until { p.replier.handle.waiting == 2 }
    let replies = ["one", "two"].map { body in
        Task { try await p.requester.request(Data(body)).collect(maxBytes: 1024).payload }
    }
    var answered: [String] = []
    var served: [String] = []
    for i in 0..<2 {
        answered.append(String(decoding: try await replies[i].value, as: UTF8.self))
        served.append(String(decoding: try await accepts[i].value, as: UTF8.self))
    }
    #expect(answered.sorted() == ["eno", "owt"])
    #expect(served.sorted() == ["one", "two"])
    try await p.close()
}

@Test func aCancelledRequestEndsAndTheRuntimeStillShutsDown() async throws {
    let p = try await Pair()
    let request = Task { try await p.requester.request(Data("unanswered")) }
    try await until { p.requester.handle.waiting == 1 }
    request.cancel()
    await #expect(throws: CancellationError.self) { try await request.value }
    try await p.close()
}

@Test func closingAReplierEndsAPendingAcceptWithClosed() async throws {
    let p = try await Pair()
    let accept = Task { try await p.replier.accept() }
    try await until { p.replier.handle.waiting == 1 }
    p.replier.close()
    let error = await weidaError { try await accept.value }
    #expect(error?.code == .closed)
    #expect(p.replier.path == "")
    try await p.close()
}
