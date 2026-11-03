// Key material between Security's types and the ABI's PEM and DER
// (docs/decisions/0047): certificates are SecCertificate, a private key is
// a SecKey exported as PKCS#8.

import CryptoKit
import Foundation
import Security

/// The PEM of `certificates`, one CERTIFICATE block each, in order.
func pemEncode(_ certificates: [SecCertificate]) -> Data {
    var pem = Data()
    for certificate in certificates {
        pem.append(pemBlock("CERTIFICATE", SecCertificateCopyData(certificate) as Data))
    }
    return pem
}

/// The certificates of the CERTIFICATE blocks in `pem`, in order. A block
/// that does not decode or parse is `.tls`.
func pemCertificates(_ pem: Data) throws -> [SecCertificate] {
    let text = String(decoding: pem, as: UTF8.self)
    let begin = "-----BEGIN CERTIFICATE-----"
    let end = "-----END CERTIFICATE-----"
    var certificates: [SecCertificate] = []
    var rest = text[...]
    while let start = rest.range(of: begin) {
        guard let stop = rest.range(of: end, range: start.upperBound..<rest.endIndex) else {
            throw WeidaError(.tls, "a CERTIFICATE block has no end")
        }
        let body = rest[start.upperBound..<stop.lowerBound]
        guard let der = Data(base64Encoded: String(body), options: .ignoreUnknownCharacters) else {
            throw WeidaError(.tls, "a CERTIFICATE block is not base64")
        }
        certificates.append(try certificate(der))
        rest = rest[stop.upperBound...]
    }
    return certificates
}

/// The certificate in `der`; `.tls` when it does not parse.
func certificate(_ der: Data) throws -> SecCertificate {
    guard let certificate = SecCertificateCreateWithData(nil, der as CFData) else {
        throw WeidaError(.tls, "a certificate does not parse")
    }
    return certificate
}

/// `privateKey` as a PKCS#8 PEM PRIVATE KEY block, the form libweida reads.
/// An EC key on P-256, P-384 or P-521 goes through CryptoKit, an RSA key is
/// wrapped here; a key Security cannot export (Secure Enclave, a token) is
/// `.inval`, naming case 2 of docs/decisions/0047. The caller wipes the
/// result.
func pkcs8PEM(_ privateKey: SecKey) throws -> Data {
    guard let attributes = SecKeyCopyAttributes(privateKey) as? [CFString: Any],
          let type = attributes[kSecAttrKeyType] as? String,
          attributes[kSecAttrKeyClass] as? String == kSecAttrKeyClassPrivate as String
    else {
        throw WeidaError(.inval, "the key is not a private key")
    }
    var error: Unmanaged<CFError>?
    guard var external = SecKeyCopyExternalRepresentation(privateKey, &error) as Data? else {
        error?.release()
        throw WeidaError(
            .inval,
            "the private key cannot be exported: weida takes the key's bytes, and a key that "
                + "never leaves its store is case 2 of docs/decisions/0047")
    }
    defer { external.resetBytes(in: 0..<external.count) }
    var der: Data
    if type == kSecAttrKeyTypeECSECPrimeRandom as String {
        // The X9.63 form 04 || X || Y || K, each a field element long.
        switch external.count {
        case 1 + 3 * 32: der = try P256.Signing.PrivateKey(x963Representation: external).derRepresentation
        case 1 + 3 * 48: der = try P384.Signing.PrivateKey(x963Representation: external).derRepresentation
        case 1 + 3 * 66: der = try P521.Signing.PrivateKey(x963Representation: external).derRepresentation
        default: throw WeidaError(.inval, "an EC key on a curve other than P-256, P-384 and P-521")
        }
    } else if type == kSecAttrKeyTypeRSA as String {
        der = rsaPKCS8(pkcs1: external)
    } else {
        throw WeidaError(.inval, "a key that is neither EC nor RSA")
    }
    defer { der.resetBytes(in: 0..<der.count) }
    return pemBlock("PRIVATE KEY", der)
}

/// The PKCS#8 PrivateKeyInfo around an RSA key's PKCS#1 RSAPrivateKey
/// (RFC 5208): version 0, the rsaEncryption algorithm with NULL
/// parameters, and the key as an OCTET STRING.
func rsaPKCS8(pkcs1: Data) -> Data {
    let version = Data([0x02, 0x01, 0x00])
    let rsaEncryption = Data([
        0x30, 0x0D, 0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01, 0x05, 0x00,
    ])
    return derElement(0x30, version + rsaEncryption + derElement(0x04, pkcs1))
}

/// One DER element: `tag`, the definite length of `content`, `content`.
private func derElement(_ tag: UInt8, _ content: Data) -> Data {
    var element = Data([tag])
    let n = content.count
    if n < 0x80 {
        element.append(UInt8(n))
    } else {
        var length: [UInt8] = []
        var rest = n
        while rest > 0 {
            length.insert(UInt8(rest & 0xFF), at: 0)
            rest >>= 8
        }
        element.append(0x80 | UInt8(length.count))
        element.append(contentsOf: length)
    }
    element.append(content)
    return element
}

/// A PEM block labelled `label` around `der`, in 64-character lines.
private func pemBlock(_ label: String, _ der: Data) -> Data {
    var block = Data("-----BEGIN \(label)-----\n".utf8)
    block.append(der.base64EncodedData(options: [.lineLength64Characters, .endLineWithLineFeed]))
    block.append(Data("\n-----END \(label)-----\n".utf8))
    return block
}
