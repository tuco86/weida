import CWeida

/// A failure of libweida, with the record the failing call left on its
/// thread (docs/c/README.md R9).
public struct WeidaError: Error, Equatable, Sendable, CustomStringConvertible {
    /// A status code of the C ABI, positive: the 24 variants of weida's
    /// error in declaration order, then the ABI's own five codes.
    public enum Code: Int32, Sendable, CaseIterable, CustomStringConvertible {
        case runtime = 1
        case invalidAddress
        case invalidEndpointPath
        case invalidFingerprint
        case alreadyRegistered
        case notConnected
        case connectionLost
        case negotiation
        case `protocol`
        case rejected
        case unknownEndpoint
        case unsupported
        case noParkedConnection
        case noReply
        case canceled
        case expired
        case indeterminate
        case limitExceeded
        case datagramsUnavailable
        case tooLarge
        case tls
        case untrusted
        case io
        case transport
        case again = 100
        case intr
        case inval
        case closed
        case panic

        /// The code's description (`weida_strerror`).
        public var description: String {
            String(cString: weida_strerror(rawValue))
        }
    }

    /// Why a connection was lost (`Code.connectionLost`).
    public enum LossCause: Int32, Sendable {
        case idleTimeout = 1
        case peerClosed
        case peerRestarting
        case locallyClosed
        case reset
        case transportError
    }

    public let code: Code
    /// The `Display` text of the failure, or the code's description.
    public let message: String
    /// The cause of a `.connectionLost`, otherwise nil.
    public let lossCause: LossCause?
    /// The fingerprint the peer presented on `.untrusted`, otherwise nil.
    public let fingerprint: String?
    /// The largest payload the connection carries on `.tooLarge`, otherwise
    /// nil.
    public let max: Int64?
    /// The raw OS error of an `.io` that carried one, otherwise nil.
    public let osCode: Int32?

    /// An error of this package's own, which no C call recorded.
    public init(_ code: Code, _ message: String? = nil) {
        self.code = code
        self.message = message ?? code.description
        lossCause = nil
        fingerprint = nil
        max = nil
        osCode = nil
    }

    /// The failure `status` (negative) with the record this thread holds.
    /// Called right after the failing C call, in the same synchronous
    /// function, so that no other call on this thread replaced the record.
    init(status: Int32) {
        // The package is built against the header of the library it links
        // (build-xcframework.sh), so every code it returns is one of Code's.
        guard let code = Code(rawValue: -status) else {
            preconditionFailure("libweida returned the unknown status \(status)")
        }
        self.code = code
        message = weida_last_error_message().map { String(cString: $0) } ?? code.description
        lossCause = LossCause(rawValue: weida_last_error_loss_cause())
        fingerprint = weida_last_error_fingerprint().map { String(cString: $0) }
        let max = weida_last_error_max()
        self.max = max >= 0 ? max : nil
        let os = weida_last_error_os_code()
        osCode = os != 0 ? os : nil
    }

    public var description: String { "weida: \(message)" }
}

/// Throws `status` with the record of this thread when it is negative.
func check(_ status: Int32) throws {
    if status < 0 {
        throw WeidaError(status: status)
    }
}

/// One try's status: nil on `-WEIDA_EAGAIN` (the next try may wait), the
/// status when it is not negative, otherwise thrown with the record.
func pending(_ status: Int32) throws -> Int32? {
    if status == -WEIDA_EAGAIN {
        return nil
    }
    try check(status)
    return status
}
