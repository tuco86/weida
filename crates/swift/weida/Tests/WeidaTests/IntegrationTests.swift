// The integration tier, run with WEIDA_INTEGRATION=1: Req/Rep over real
// QUIC on localhost with generated identities, client keys, anchors and the
// presented chain (docs/decisions/0047), and identity and anchor files.

import Foundation
import Security
import Testing

@testable import Weida

@Suite(.enabled(if: ProcessInfo.processInfo.environment["WEIDA_INTEGRATION"] == "1"))
struct IntegrationTests {
    @Test func aRoundTripOverQUICWithAPinnedKey() async throws {
        let identity = try Identity.generate()
        let fingerprint = try identity.fingerprint
        let s = try await Serving(identity)
        let requester = try s.runtime.requester(ClientTLS(trust: Trust().addPin(fingerprint)))
        try await requester.connect("weida://\(fingerprint)@\(s.binding.localAddr)/echo")
        async let served = serve(s.replier, meta: TransferMeta(contentType: "text/plain")) { Data($0.reversed()) }
        let message = try await requester.request(Data("ping")).collect(maxBytes: 1024)
        #expect(message.payload == Data("gnip"))
        #expect(message.meta.contentType == "text/plain")
        #expect(try await served == Data("ping"))
        requester.close()
        try await s.close()
    }

    @Test func aWrongPinIsUntrustedAndCarriesTheFingerprint() async throws {
        let identity = try Identity.generate()
        let s = try await Serving(identity)
        let requester = try s.runtime.requester(ClientTLS(trust: Trust().addPin(Identity.generate().fingerprint)))
        let error = await weidaError { try await refused(requester, s.url) }
        #expect(error?.code == .untrusted)
        #expect(error?.fingerprint == (try identity.fingerprint))
        requester.close()
        try await s.close()
    }

    @Test func aPinnedClientKeyIsAcceptedAndTheServerSeesItsKeyAndChain() async throws {
        let server = try Identity.generate()
        let client = try Identity.generate()
        let s = try await Serving(requiringPinned(server, client))
        let requester = try requesterOf(s, pinning(server), client)
        let meta = try await echoOnce(s, requester)
        #expect(meta.peerFingerprint == (try client.fingerprint))
        let chain = try #require(try meta.peerChain())
        #expect(chain.count == 1)
        #expect(chain.first.map { SecCertificateCopyData($0) as Data } == (SecCertificateCopyData(try client.certificates()[0]) as Data))
        requester.close()
        try await s.close()
    }

    @Test func anAnonymousOrUnknownClientIsRefusedWithTLSAndNothingReachesTheServer() async throws {
        let server = try Identity.generate()
        let client = try Identity.generate()
        let s = try await Serving(requiringPinned(server, client))
        for (name, identity) in [("an anonymous client", nil), ("an unknown client", try Identity.generate())] {
            let requester = try requesterOf(s, pinning(server), identity)
            let error = await weidaError { try await refused(requester, s.url) }
            #expect(error?.code == .tls, "\(name)")
            requester.close()
        }
        let accept = Task { try await s.replier.accept() }
        try await Task.sleep(for: .milliseconds(200))
        accept.cancel()
        await #expect(throws: CancellationError.self) { try await accept.value }
        try await s.close()
    }

    @Test func aBindingThatRequiresClientsFromAnEmptyTrustDoesNotBind() async throws {
        let terms = try ServerTLS(identity: Identity.generate())
        try terms.requireClient(Trust())
        let runtime = try Runtime.owned()
        let listener = try runtime.listener()
        let error = await weidaError { try await listener.bindQUIC("127.0.0.1:0", tls: terms) }
        #expect(error?.code == .tls)
        #expect(error?.message.contains("client trust is empty") == true)
        listener.close()
        try await runtime.shutdown()
    }

    @Test func anyKeyExposesTheFingerprintAndTheChain() async throws {
        let server = try Identity.generate()
        let client = try Identity.generate()
        let terms = try ServerTLS(identity: server)
        try terms.requireAnyKey()
        let s = try await Serving(terms)
        let anonymous = try requesterOf(s, pinning(server), nil)
        let error = await weidaError { try await refused(anonymous, s.url) }
        #expect(error?.code == .tls)
        anonymous.close()

        let requester = try requesterOf(s, pinning(server), client)
        let meta = try await echoOnce(s, requester)
        #expect(meta.peerFingerprint == (try client.fingerprint))
        #expect(try meta.peerChain()?.count == 1)
        requester.close()
        try await s.close()
    }

    @Test func anAnchorAcceptsTheDialledNameAndRefusesAnother() async throws {
        let named = try Identity.generate(for: ["127.0.0.1"])
        var s = try await Serving(named)
        var requester = try requesterOf(s, Trust().addAnchor(named.certificates()), nil)
        let meta = try await echoOnce(s, requester)
        #expect(meta.peerChainDER == nil)
        requester.close()
        try await s.close()

        let other = try Identity.generate(for: ["localhost"])
        s = try await Serving(other)
        requester = try requesterOf(s, Trust().addAnchor(other.certificates()), nil)
        let error = await weidaError { try await refused(requester, s.url) }
        #expect(error?.code == .untrusted)
        #expect(error?.fingerprint == (try other.fingerprint))
        requester.close()
        try await s.close()
    }

    @Test func identityAndAnchorFilesReadBack() async throws {
        let generated = try Identity.generate(for: ["127.0.0.1"])
        let want = try generated.fingerprint
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent("weida-swift-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let idPath = dir.appendingPathComponent("id.pem")
        let certPath = dir.appendingPathComponent("cert.pem")
        var idPEM = try generated.toPEM()
        try idPEM.write(to: idPath)
        idPEM.resetBytes(in: 0..<idPEM.count)
        try generated.certificatePEM().write(to: certPath)

        let fromOne = try Identity(pemFile: idPath)
        #expect(try fromOne.fingerprint == want)
        let fromTwo = try Identity(pemCertificateFile: certPath, keyFile: idPath)
        #expect(try fromTwo.fingerprint == want)

        let s = try await Serving(fromOne)
        let requester = try requesterOf(s, Trust().addAnchor(file: certPath), nil)
        _ = try await echoOnce(s, requester)
        requester.close()
        try await s.close()

        let missing = try Identity(pemFile: dir.appendingPathComponent("missing.pem"))
        let error = await weidaError { try missing.fingerprint }
        #expect(error?.code == .tls)
    }
}

/// A runtime serving QUIC on localhost, and a replier at /echo.
private struct Serving {
    let runtime: Runtime
    let listener: Listener
    let binding: Binding
    let replier: Replier

    init(_ identity: Identity) async throws {
        try await self.init(ServerTLS(identity: identity))
    }

    /// Serves on `tls`'s terms, which the binding copies.
    init(_ tls: ServerTLS) async throws {
        runtime = try Runtime.owned()
        listener = try runtime.listener()
        binding = try await listener.bindQUIC("127.0.0.1:0", tls: tls)
        replier = try listener.replier("/echo")
    }

    var url: String { "weida://\(binding.localAddr)/echo" }

    func close() async throws {
        replier.close()
        binding.close()
        listener.close()
        try await runtime.shutdown()
    }
}

/// `server`'s terms, requiring a client key pinned to `client`'s.
private func requiringPinned(_ server: Identity, _ client: Identity) throws -> ServerTLS {
    let terms = try ServerTLS(identity: server)
    try terms.requireClient(Trust().addPin(client.fingerprint))
    return terms
}

/// The trust of a client that dials `server` by its pinned key.
private func pinning(_ server: Identity) throws -> Trust {
    try Trust().addPin(server.fingerprint)
}

/// A requester of `s`'s runtime that dials on `trust` and presents
/// `identity`, unless it is nil.
private func requesterOf(_ s: Serving, _ trust: Trust, _ identity: Identity?) throws -> Requester {
    let terms = try ClientTLS(trust: trust)
    if let identity {
        try terms.setIdentity(identity)
    }
    return try s.runtime.requester(terms)
}

/// Sends one request to `s` and returns the metadata the server saw.
private func echoOnce(_ s: Serving, _ requester: Requester) async throws -> IncomingMeta {
    try await requester.connect(s.url)
    async let served = serveSeeing(s.replier)
    let message = try await requester.request(Data("ping")).collect(maxBytes: 1024)
    #expect(message.payload == Data("hi"))
    return try await served
}

/// Dials `url` and sends a request; throws whichever step failed first.
private func refused(_ requester: Requester, _ url: String) async throws {
    try await requester.connect(url)
    try await requester.request(Data("ping")).close()
}

/// Answers one request with "hi" as `serve` does, and returns the metadata
/// the request carried, read right after the accept.
private func serveSeeing(_ replier: Replier) async throws -> IncomingMeta {
    let request = try await replier.accept()
    defer { request.close() }
    let meta = try request.meta()
    _ = try await readAll(request.takeBody(), size: 3)
    let reply = try await request.reply()
    defer { reply.close() }
    try await reply.writeAll(Data("hi"))
    try await reply.finish().delivered()
    return meta
}
