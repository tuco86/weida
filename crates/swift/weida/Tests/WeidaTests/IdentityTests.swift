// The unit tier: key material in and out of an identity, in Security's and
// CryptoKit's types (docs/decisions/0047). No file or socket is touched.

import CryptoKit
import Foundation
import Security
import Testing

@testable import Weida

@Test func toPEMReadsBackWithTheSameFingerprint() throws {
    let id = try Identity.generate()
    let want = try id.fingerprint
    var data = try id.toPEM()
    defer { data.resetBytes(in: 0..<data.count) }
    #expect(String(decoding: data, as: UTF8.self).contains("PRIVATE KEY"))
    let back = try Identity(pemCertificate: data, pemKey: data)
    #expect(try back.fingerprint == want)
    let certificatePEM = String(decoding: try id.certificatePEM(), as: UTF8.self)
    #expect(certificatePEM.contains("-----BEGIN CERTIFICATE-----"))
    #expect(!certificatePEM.contains("PRIVATE KEY"))
}

@Test func certificatesAreTheIdentitysKey() throws {
    let id = try Identity.generate()
    let certificates = try id.certificates()
    try #require(certificates.count == 1)
    let certificateKey = try #require(SecCertificateCopyKey(certificates[0]))
    let publicKey = try #require(SecKeyCopyExternalRepresentation(certificateKey, nil) as Data?)
    #expect(publicKey == (try privateKey(of: id).publicKey.x963Representation))
}

@Test func aSecKeyAndSecCertificatesMakeTheSameIdentity() throws {
    let generated = try Identity.generate()
    let key = try secKey(privateKey(of: generated).x963Representation, type: kSecAttrKeyTypeECSECPrimeRandom)
    let id = try Identity(certificates: generated.certificates(), privateKey: key)
    #expect(try id.fingerprint == generated.fingerprint)
    let back = try privateKey(of: id)
    #expect(back.rawRepresentation == (try privateKey(of: generated).rawRepresentation))
}

@Test func anRSAKeyGoesToLibweidaAsPKCS8() throws {
    // One 1024-bit RSA key as OpenSSL writes it in PKCS#1 and in PKCS#8.
    let pkcs1 = try #require(Data(base64Encoded: rsaPKCS1))
    let pkcs8 = try #require(Data(base64Encoded: rsaPKCS8))
    let key = try secKey(pkcs1, type: kSecAttrKeyTypeRSA)
    let pem = String(decoding: try pkcs8PEM(key), as: UTF8.self)
    #expect(try block("PRIVATE KEY", in: pem) == pkcs8)
}

@Test func argumentErrors() async throws {
    let id = try Identity.generate()
    let key = try secKey(privateKey(of: id).x963Representation, type: kSecAttrKeyTypeECSECPrimeRandom)
    let publicKey = try #require(SecKeyCopyPublicKey(key))
    let trust = try Trust()
    let cases: [(String, () throws -> Any)] = [
        ("a NUL in a name", { try Identity.generate(for: ["a\0b"]) }),
        ("no certificate", { try Identity(certificates: [], privateKey: key) }),
        ("a public key", { try Identity(certificates: id.certificates(), privateKey: publicKey) }),
        ("no anchor certificate", { try trust.addAnchor([SecCertificate]()) }),
        ("a URL that is not a file", { try trust.addAnchor(file: URL(string: "https://example.com/ca.pem")!) }),
    ]
    for (name, body) in cases {
        let error = await weidaError { try body() }
        #expect(error?.code == .inval, "\(name)")
    }
}

/// The private key in `id`'s PEM, through CryptoKit: weida generates P-256
/// keys.
private func privateKey(of id: Identity) throws -> P256.Signing.PrivateKey {
    var pem = try id.toPEM()
    defer { pem.resetBytes(in: 0..<pem.count) }
    let text = String(decoding: pem, as: UTF8.self)
    return try P256.Signing.PrivateKey(derRepresentation: block("PRIVATE KEY", in: text))
}

/// A private SecKey of `type` from its external representation.
private func secKey(_ data: Data, type: CFString) throws -> SecKey {
    let attributes: [CFString: Any] = [kSecAttrKeyType: type, kSecAttrKeyClass: kSecAttrKeyClassPrivate]
    var error: Unmanaged<CFError>?
    guard let key = SecKeyCreateWithData(data as CFData, attributes as CFDictionary, &error) else {
        throw error!.takeRetainedValue() as Error
    }
    return key
}

/// The DER of the first PEM block labelled `label` in `pem`.
private func block(_ label: String, in pem: String) throws -> Data {
    let begin = try #require(pem.range(of: "-----BEGIN \(label)-----"))
    let end = try #require(pem.range(of: "-----END \(label)-----"))
    return try #require(Data(base64Encoded: String(pem[begin.upperBound..<end.lowerBound]), options: .ignoreUnknownCharacters))
}

private let rsaPKCS1 = """
    MIICXAIBAAKBgQC8ZJn7cVt/CBNWvEWcltfGHWSlT227Z2JE/XJvG4vSktpZt54sbN7w7nZ6YPxP4go8Un3uWvN35x7XJDo+\
    65mM4E4zGP32RhxHh8jwcSMkBWCRTAQroT9hpnB+09ZUegHY9qn9gsdwGtFJL3uUn3dDoMF0AXz0CAjc88e+d7PZQwIDAQAB\
    AoGAP73SE68xA/QZhvlAnFC1RQTg7sXHHOdH6ovOUiWB04d4dW6ZB/L2I5KrZ5C1DdovGGXY0fLX+bOrTaOmSW+xjdH2XClP\
    75LSL/dQ3Dbr12l0Ce9L0EEDvP34pUMb94YuErrmWyWoNaK/aD8WC4erzSEXUz95u77RTk6m/hhhFAECQQDmfjfaQR+uHXfl\
    P2t72+GDRzbrEBz/hzz39pFsjUnoy2AxdIX7tuukFv/d6Sdpkz1S5sAff4gix8cqWPEN1IsBAkEA0T20RRYYKTBOot++RTFM\
    GUa7byIyRfWWAtoJh7CMAjhshXe2PXvZMYO8cLGu+XLT41uEuTl84ivOXqBGnut4QwJAU4ZnrQKufUUWfo+13oN2vEDhSNtk\
    9sBGOsEyP7kPkHmqmSFVsDrAN41SGVN1Qc/miBUwh/mKFrh4lwL2D4H3AQJAZ4VHswaDXZ1wylQ4JrOFGtxLz2fzx3kBhteo\
    72EBY/fWeRIJL9IeWsj6aib+kpulCU82Ew3GI3/Y0igpyGZnUQJBAIY44oCrUMnxwmZX1PyrdlIXEIlweaj7rAUa4IcFigMD\
    uHBrDhhCiBC3w90ImpQu8Ejur9SGUhXBN7OHl+N7LCo=
    """

private let rsaPKCS8 = """
    MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBALxkmftxW38IE1a8RZyW18YdZKVPbbtnYkT9cm8bi9KS2lm3\
    nixs3vDudnpg/E/iCjxSfe5a83fnHtckOj7rmYzgTjMY/fZGHEeHyPBxIyQFYJFMBCuhP2GmcH7T1lR6Adj2qf2Cx3Aa0Ukv\
    e5Sfd0OgwXQBfPQICNzzx753s9lDAgMBAAECgYA/vdITrzED9BmG+UCcULVFBODuxccc50fqi85SJYHTh3h1bpkH8vYjkqtn\
    kLUN2i8YZdjR8tf5s6tNo6ZJb7GN0fZcKU/vktIv91DcNuvXaXQJ70vQQQO8/filQxv3hi4SuuZbJag1or9oPxYLh6vNIRdT\
    P3m7vtFOTqb+GGEUAQJBAOZ+N9pBH64dd+U/a3vb4YNHNusQHP+HPPf2kWyNSejLYDF0hfu266QW/93pJ2mTPVLmwB9/iCLH\
    xypY8Q3UiwECQQDRPbRFFhgpME6i375FMUwZRrtvIjJF9ZYC2gmHsIwCOGyFd7Y9e9kxg7xwsa75ctPjW4S5OXziK85eoEae\
    63hDAkBThmetAq59RRZ+j7Xeg3a8QOFI22T2wEY6wTI/uQ+QeaqZIVWwOsA3jVIZU3VBz+aIFTCH+YoWuHiXAvYPgfcBAkBn\
    hUezBoNdnXDKVDgms4Ua3EvPZ/PHeQGG16jvYQFj99Z5Egkv0h5ayPpqJv6Sm6UJTzYTDcYjf9jSKCnIZmdRAkEAhjjigKtQ\
    yfHCZlfU/Kt2UhcQiXB5qPusBRrghwWKAwO4cGsOGEKIELfD3QialC7wSO6v1IZSFcE3s4eX43ssKg==
    """
