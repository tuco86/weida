// weida for Swift: the module Weida over the C ABI libweida, linked
// statically from the XCFramework CWeida (docs/decisions/0046). Its API
// mirrors the C ABI's current slice (docs/libraries/weida-swift.md): one
// class per handle, one method per function, `async throws` on every method
// that waits.
//
// No call blocks in C: every C call only tries, and a call that would block
// suspends its task on a DispatchSource over the handle's readiness
// descriptor until the next try can succeed, the task is cancelled, or the
// handle is closed.

import CWeida
import Foundation

/// The version of the linked libweida, e.g. "0.1.0-alpha.11-dev".
public var version: String {
    String(cString: weida_version())
}

/// The runtime's configuration. `Runtime.owned()` without one takes weida's
/// defaults; a configuration sets both fields as given.
public struct RuntimeConfig: Sendable {
    /// The reactor's worker threads; 0 fails `Runtime.owned` with
    /// `.runtime`.
    public var workerThreads: UInt32
    /// The bound on `Runtime.shutdown()`, in milliseconds' resolution.
    public var shutdownTimeout: Duration

    public init(workerThreads: UInt32, shutdownTimeout: Duration) {
        self.workerThreads = workerThreads
        self.shutdownTimeout = shutdownTimeout
    }
}

/// A weida runtime on a reactor of its own; everything made from it keeps
/// that reactor alive (R13).
public final class Runtime: Sendable {
    let handle: Handle

    private init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_runtime_drop($0) }
    }

    /// Creates a runtime (`weida_runtime_owned`), with weida's defaults when
    /// `config` is nil.
    public static func owned(_ config: RuntimeConfig? = nil) throws -> Runtime {
        var cfg: OpaquePointer?
        if let config {
            try check(weida_runtime_config_new(&cfg))
            let ms = config.shutdownTimeout.components
            let millis = UInt64(clamping: ms.seconds) &* 1000 &+ UInt64(clamping: ms.attoseconds / 1_000_000_000_000_000)
            try check(weida_runtime_config_set_worker_threads(cfg, config.workerThreads))
            try check(weida_runtime_config_set_shutdown_timeout_ms(cfg, millis))
        }
        defer { weida_runtime_config_drop(cfg) }
        var out: OpaquePointer?
        try check(weida_runtime_owned(cfg, &out))
        return Runtime(out)
    }

    /// Creates the runtime's listener.
    public func listener() throws -> Listener {
        var out: OpaquePointer?
        try handle.call { try check(weida_runtime_listener($0, &out)) }
        return Listener(out)
    }

    /// Creates a requester that dials on `tls`'s terms, which are copied.
    public func requester(_ tls: ClientTLS) throws -> Requester {
        var out: OpaquePointer?
        try handle.call { rt in
            try tls.handle.call { try check(weida_runtime_requester(rt, $0, &out)) }
        }
        return Requester(out)
    }

    /// Closes every endpoint and waits, bounded by the configured shutdown
    /// timeout, for the connections to go idle. It consumes the runtime: the
    /// runtime is closed whether or not the wait finishes.
    public func shutdown() async throws {
        var op: OpaquePointer?
        try handle.consume { try check(weida_runtime_shutdown($0, &op)) }
        defer { weida_runtime_shutdown_op_drop(op) }
        _ = try await wait(owner: nil) { _ in
            try descriptor { weida_runtime_shutdown_op_fd(op, $0) }
        } attempt: {
            try pending(weida_runtime_shutdown_op_wait(op, 0))
        }
    }

    /// Releases the caller's reference; the reactor lives on while anything
    /// made from the runtime does.
    public func close() {
        handle.close()
    }
}

/// Where a runtime serves: bindings and the endpoints registered on them.
public final class Listener: Sendable {
    let handle: Handle

    fileprivate init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_listener_drop($0) }
    }

    /// Serves QUIC on `addr`, e.g. "127.0.0.1:0", on `tls`'s terms, which
    /// are copied.
    public func bindQUIC(_ addr: String, tls: ServerTLS) async throws -> Binding {
        var op: OpaquePointer?
        try withCString(addr) { caddr in
            try handle.call { l in
                try tls.handle.call { try check(weida_listener_bind_quic(l, caddr, $0, &op)) }
            }
        }
        defer { weida_listener_bind_quic_op_drop(op) }
        let out: OpaquePointer = try await wait(owner: handle) { _ in
            try descriptor { weida_listener_bind_quic_op_fd(op, $0) }
        } attempt: {
            var out: OpaquePointer?
            return try pending(weida_listener_bind_quic_op_wait(op, 0, &out)).map { _ in out! }
        }
        return Binding(out)
    }

    /// Answers on the in-process bus `bus`: a requester in this process
    /// reaches it at `weida+inproc://<bus>/<path>`.
    public func bindInproc(_ bus: String) throws -> LocalBinding {
        var out: OpaquePointer?
        try withCString(bus) { cbus in
            try handle.call { try check(weida_listener_bind_inproc($0, cbus, &out)) }
        }
        return LocalBinding(out)
    }

    /// Registers a replier at the endpoint path; throws
    /// `.invalidEndpointPath` or `.alreadyRegistered`.
    public func replier(_ path: String) throws -> Replier {
        var out: OpaquePointer?
        try withCString(path) { cpath in
            try handle.call { try check(weida_listener_replier($0, cpath, &out)) }
        }
        return Replier(out)
    }

    /// Releases the listener; its bindings and endpoints stay.
    public func close() {
        handle.close()
    }
}

/// A QUIC binding: the socket a listener serves on.
public final class Binding: Sendable {
    let handle: Handle

    fileprivate init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_binding_drop($0) }
    }

    /// The address the binding serves on, with the port resolved; "" once
    /// the binding is closed.
    public var localAddr: String {
        (try? handle.call { b in
            var n = 0
            return string(weida_binding_local_addr(b, &n), n)
        }) ?? ""
    }

    /// Stops serving on the binding.
    public func close() {
        handle.close()
    }
}

/// An in-process binding: the bus a listener answers on.
public final class LocalBinding: Sendable {
    let handle: Handle

    fileprivate init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_local_binding_drop($0) }
    }

    /// The bus the binding answers on; "" once the binding is closed.
    public var bus: String {
        (try? handle.call { b in
            var n = 0
            return string(weida_local_binding_bus(b, &n), n)
        }) ?? ""
    }

    /// Frees the bus name.
    public func close() {
        handle.close()
    }
}

/// Whom a dialler trusts: pins and anchors added to what each dialled
/// address names.
public final class Trust: Sendable {
    let handle: Handle

    /// A trust that accepts only what each dialled address names: a
    /// `weida://sha256:...@host/path` URL pins its key.
    public init() throws {
        var out: OpaquePointer?
        try check(weida_trust_new(&out))
        handle = Handle(out) { weida_trust_drop($0) }
    }

    /// Adds a pinned fingerprint: "sha256:" and 64 hex digits, otherwise
    /// `.invalidFingerprint`.
    @discardableResult
    public func addPin(_ fingerprint: String) throws -> Trust {
        try withCString(fingerprint) { fp in
            try handle.call { try check(weida_trust_add_pin($0, fp)) }
        }
        return self
    }

    /// Adds the certificate authorities in `pem`, PEM certificates: a peer
    /// whose chain leads to one of them and names the dialled host is
    /// accepted. The bytes are copied and parsed at each dial that uses the
    /// trust, which fails with `.tls` when they hold no certificate or do
    /// not parse.
    @discardableResult
    public func addAnchor(_ pem: Data) throws -> Trust {
        try pem.withUnsafeBytes { raw in
            try handle.call {
                try check(weida_trust_add_anchor($0, raw.bindMemory(to: UInt8.self).baseAddress, raw.count))
            }
        }
        return self
    }

    /// Adds `certificates` as certificate authorities: their PEM goes to
    /// `addAnchor(_:)`. No certificate is `.inval`.
    @discardableResult
    public func addAnchor(_ certificates: [SecCertificate]) throws -> Trust {
        if certificates.isEmpty {
            throw WeidaError(.inval, "no anchor certificate")
        }
        return try addAnchor(pemEncode(certificates))
    }

    /// Adds the certificate authorities in the PEM file at `file`. The path
    /// is kept and the file is read at each dial that uses the trust, which
    /// fails with `.tls` when it is missing or does not parse. A URL that is
    /// not a file URL is `.inval`.
    @discardableResult
    public func addAnchor(file: URL) throws -> Trust {
        try withPath(file) { path in
            try handle.call { try check(weida_trust_add_anchor_file($0, path)) }
        }
        return self
    }

    /// Releases the trust.
    public func close() {
        handle.close()
    }
}

/// A certificate chain and its private key.
public final class Identity: Sendable {
    let handle: Handle

    private init(_ pointer: OpaquePointer?) {
        handle = Handle(pointer) { weida_identity_drop($0) }
    }

    /// Generates a self-signed identity with a fresh key, made for pinning
    /// by fingerprint.
    public static func generate() throws -> Identity {
        var out: OpaquePointer?
        try check(weida_identity_generate(&out))
        return Identity(out)
    }

    /// Generates a self-signed identity with a fresh key whose certificate
    /// also names `names`, DNS names or IP literals: a peer that takes the
    /// certificate as an anchor can then verify the host it dialled. No
    /// name is `generate()`.
    public static func generate(for names: [String]) throws -> Identity {
        if names.contains(where: { $0.utf8.contains(0) }) {
            throw WeidaError(.inval, "a name holds a NUL byte")
        }
        let cnames = names.map { strdup($0) }
        defer { cnames.forEach { free($0) } }
        var out: OpaquePointer?
        try cnames.map { UnsafePointer($0) }.withUnsafeBufferPointer {
            try check(weida_identity_generate_for($0.baseAddress, $0.count, &out))
        }
        return Identity(out)
    }

    /// An identity from PEM bytes: the certificate chain, leaf first, and
    /// the private key. The material is checked when it is first used.
    public convenience init(pemCertificate: Data, pemKey: Data) throws {
        var out: OpaquePointer?
        try pemCertificate.withUnsafeBytes { cert in
            try pemKey.withUnsafeBytes { key in
                try check(weida_identity_from_pem(
                    cert.bindMemory(to: UInt8.self).baseAddress, cert.count,
                    key.bindMemory(to: UInt8.self).baseAddress, key.count, &out))
            }
        }
        self.init(out)
    }

    /// An identity from one PEM file holding the certificate chain and the
    /// private key, the form `toPEM()` writes. Nothing is read here: a
    /// missing or bad file is `.tls` at first use (`fingerprint`, the PEM
    /// getters, a bind or a dial).
    public convenience init(pemFile: URL) throws {
        var out: OpaquePointer?
        try withPath(pemFile) { try check(weida_identity_from_pem_file($0, &out)) }
        self.init(out)
    }

    /// An identity from a certificate chain file, leaf first, and a private
    /// key file. Nothing is read here: a missing or bad file is `.tls` at
    /// first use.
    public convenience init(pemCertificateFile: URL, keyFile: URL) throws {
        var out: OpaquePointer?
        try withPath(pemCertificateFile) { cert in
            try withPath(keyFile) { key in
                try check(weida_identity_from_pem_files(cert, key, &out))
            }
        }
        self.init(out)
    }

    /// An identity from Security's types: `certificates`, leaf first, and
    /// `privateKey`, an exportable EC (P-256, P-384, P-521) or RSA key,
    /// which goes to libweida as PKCS#8. A key that cannot be exported,
    /// such as one in the Secure Enclave, is `.inval`: weida takes the
    /// key's bytes (docs/decisions/0047, case 2).
    public convenience init(certificates: [SecCertificate], privateKey: SecKey) throws {
        if certificates.isEmpty {
            throw WeidaError(.inval, "no certificate")
        }
        var key = try pkcs8PEM(privateKey)
        defer { key.resetBytes(in: 0..<key.count) }
        try self.init(pemCertificate: pemEncode(certificates), pemKey: key)
    }

    /// The identity's public-key fingerprint, "sha256:" text; `.tls` when
    /// the certificate does not parse.
    public var fingerprint: String {
        get throws {
            try handle.call { id in
                var out: UnsafePointer<CChar>?
                try check(weida_identity_fingerprint(id, &out))
                return String(cString: out!)
            }
        }
    }

    /// The certificate chain as PEM, without the key: publishable, for a
    /// peer to anchor or pin. Read anew on every call; `.tls` when the
    /// source does not parse.
    public func certificatePEM() throws -> Data {
        try pem(weida_identity_certificate_pem)
    }

    /// The chain and the private key as one PEM document, the form
    /// `init(pemCertificate:pemKey:)` and `init(pemFile:)` read back. It
    /// holds the private key: wipe it with `resetBytes(in:)`; every other
    /// copy is the caller's to wipe. Read anew on every call.
    public func toPEM() throws -> Data {
        try pem(weida_identity_to_pem)
    }

    /// The certificate chain, leaf first, parsed from `certificatePEM()`. A
    /// certificate that does not parse is `.tls`.
    public func certificates() throws -> [SecCertificate] {
        try pemCertificates(certificatePEM())
    }

    /// Copies the PEM `get` writes into a Data and drops it, which wipes it.
    private func pem(_ get: (OpaquePointer?, UnsafeMutablePointer<OpaquePointer?>?) -> Int32) throws -> Data {
        try handle.call { id in
            var out: OpaquePointer?
            try check(get(id, &out))
            defer { weida_pem_drop(out) }
            return bytes(weida_pem_data(out), weida_pem_len(out))
        }
    }

    /// Releases the identity.
    public func close() {
        handle.close()
    }
}

/// The TLS terms a requester dials on.
public final class ClientTLS: Sendable {
    let handle: Handle

    /// Client terms that dial anonymously under `trust`, which is copied.
    public init(trust: Trust) throws {
        var out: OpaquePointer?
        try trust.handle.call { try check(weida_client_tls_new($0, &out)) }
        handle = Handle(out) { weida_client_tls_drop($0) }
    }

    /// Presents `identity`, which is copied, to the servers requesters made
    /// afterwards dial. A server that requires a client key refuses the
    /// dial with `.tls` otherwise.
    public func setIdentity(_ identity: Identity) throws {
        try handle.call { tls in
            try identity.handle.call { try check(weida_client_tls_set_identity(tls, $0)) }
        }
    }

    /// Releases the terms.
    public func close() {
        handle.close()
    }
}

/// The TLS terms a QUIC binding serves on.
public final class ServerTLS: Sendable {
    let handle: Handle

    /// Server terms that present `identity`, which is copied, and accept
    /// anonymous clients.
    public init(identity: Identity) throws {
        var out: OpaquePointer?
        try identity.handle.call { try check(weida_server_tls_new($0, &out)) }
        handle = Handle(out) { weida_server_tls_drop($0) }
    }

    /// Requires every client to present a key `trust` accepts, pinned or
    /// chaining to an anchor. The trust is copied and the last call wins. A
    /// refused client gets `.tls` on its side and nothing reaches a
    /// replier; an empty trust fails the bind with `.tls`.
    public func requireClient(_ trust: Trust) throws {
        try handle.call { tls in
            try trust.handle.call { try check(weida_server_tls_require_client(tls, $0)) }
        }
    }

    /// Requires every client to present a key and prove it holds it,
    /// whatever the key; the key is then `peerFingerprint` and `peerChain`
    /// of the request's metadata. The last call wins.
    public func requireAnyKey() throws {
        try handle.call { try check(weida_server_tls_require_any_key($0)) }
    }

    /// Releases the terms.
    public func close() {
        handle.close()
    }
}

/// Runs `body` with the file system path of the file URL `url`.
private func withPath<T>(_ url: URL, _ body: (UnsafePointer<CChar>) throws -> T) throws -> T {
    guard url.isFileURL else {
        throw WeidaError(.inval, "\(url) is not a file URL")
    }
    return try url.withUnsafeFileSystemRepresentation { path in
        guard let path else {
            throw WeidaError(.inval, "\(url) has no file system representation")
        }
        return try body(path)
    }
}
