# 0047 — Key material in the language packages

- **Status:** accepted
- **Date:** 2026-11-02
- **Related:** [0045](0045-the-c-abi-is-the-boundary.md);
  [0046](0046-how-each-language-binds-libweida.md);
  [0035](0035-keys-proved-not-judged.md); [0032](0032-identity-sources-and-the-handoff.md);
  [c/tls.md](../c/tls.md)

## 1. The question

`libweida` carries an identity as PEM, bytes or a path (`weida_identity_from_pem`,
`_from_pem_file(s)`), hands it back as an owned `weida_pem`, names a trusted key as `sha256:`
text and a presented chain as DER certificates (`weida_incoming_meta_peer_chain_cert`). Every
language package sits on that boundary ([0046](0046-how-each-language-binds-libweida.md)), and
every platform has its own types for keys and certificates.

So the question is: **which forms does each package take and give for key material, and where do
keys that cannot leave a platform store fit.**

## 2. The evidence

**The Rust API.** `Identity::from_pem`, `from_pem_files`, `from_pem_file` and `to_pem` read and
write PEM; `Trust::and_anchor` and `and_anchor_file` take PEM; `PeerChain::certs` yields DER,
leaf first; a key is named by its `Fingerprint`, `sha256:` over the SubjectPublicKeyInfo. The key
inside an `Identity` is kept in zeroizing storage, and `to_pem`'s output is the caller's to wipe.

**Per platform.**

- Node: `crypto.KeyObject` exports a private key as PKCS#8 PEM
  (`export({ type: "pkcs8", format: "pem" })`); `crypto.X509Certificate` gives PEM
  (`toString()`) and DER (`raw`) and is built from either.
- Java and Android: `KeyStore` (PKCS#12 on disk), `PrivateKey.getEncoded()` (PKCS#8 DER for an
  exportable key) and `X509Certificate.getEncoded()` (DER).
- Erlang: `public_key:pem_decode/1` and `pem_encode/1`; `ssl` takes `certfile`, `keyfile`,
  `cacertfile`, or `cert` (DER), `key` (`{Type, DER}`) and `cacerts` (DER) in memory.
- Go: `tls.Certificate` holds `Certificate [][]byte` (DER) and a `PrivateKey` that
  `x509.MarshalPKCS8PrivateKey` exports when it is an `*ecdsa.PrivateKey`, `*rsa.PrivateKey` or
  `ed25519.PrivateKey`; `x509.CertPool` cannot be enumerated, so its certificates cannot be
  handed on.
- Python: `ssl.SSLContext.load_cert_chain(certfile, keyfile)` takes paths; libraries pass bytes;
  `cryptography` has certificate and private-key objects that serialize to PEM.

## 3. The options

**A. PEM only at every package surface.** Loses: users hand-write PEM conversions where their
platform already has a key or certificate type.

**B. Each package maps its platform's types onto the ABI's PEM and DER.** Loses: one conversion
layer per package, written in that package's language.

**C. DER and platform-specific entry points in the ABI.** Loses: N formats in one boundary, each
tested in Rust for a caller that is not Rust.

## 4. The decision

**B, under these rules.**

- The ABI carries PEM in (bytes or a path), an owned `weida_pem` out, DER for a presented
  chain, and `sha256:` text for keys. It gains no platform format.
- A package accepts its platform's types plus bytes and paths, and converts in the package. A
  platform private key is exported as PKCS#8.
- Key material a package gives out is a byte buffer the caller can wipe: JS `Uint8Array`, Java
  `byte[]`, Go `[]byte`, Python `bytearray`. Erlang binaries cannot be wiped, and the Erlang
  package says so.
- A presented chain is the platform's certificate type where its standard library has one,
  otherwise DER.

| Language | Identity in | Anchors | Out | Presented chain |
| --- | --- | --- | --- | --- |
| JS | `Identity.from({ cert: X509Certificate \| X509Certificate[] \| Bytes, key: KeyObject \| Bytes })`, `fromPem`, `fromPemFile(s)` | `X509Certificate \| Bytes`, a file | `certificatePem(): string`, `toPem(): Uint8Array`, `certificates(): X509Certificate[]` | `X509Certificate[]` |
| JVM/Android | `Identity.from(PrivateKey, X509Certificate...)`, `Identity.fromKeyStore(KeyStore, alias, char[] password)`, PEM bytes and paths | `X509Certificate`, bytes, a path | `toPem(): byte[]`, `certificates(): X509Certificate[]` | `X509Certificate[]` |
| Erlang/Elixir | `ssl`-style options: `certfile`/`keyfile`, `cert` (DER or a list), `key` (`{Type, DER}`) | `cacertfile`, `cacerts` | PEM binary | `[DER binary]` |
| Go | `IdentityFromTLS(tls.Certificate)` (the key must be `*ecdsa.PrivateKey`, `*rsa.PrivateKey` or `ed25519.PrivateKey`; any other `crypto.Signer` is case 2), PEM bytes and paths | `[]*x509.Certificate`, PEM, a path; never `x509.CertPool` | `ToPEM() []byte`, `TLSCertificate() (tls.Certificate, error)` | `[]*x509.Certificate` |
| Python | paths in `load_cert_chain`'s shape, bytes, and `cryptography` certificates and private keys when installed | bytes, a path | `to_pem() -> bytearray` | `list[bytes]` (DER) |

**Case 2**, keys that never leave their store (Secure Enclave, Android Keystore, TPM, PKCS#11),
is outside this note: such a key has no PKCS#8 to export. Its shape is a signer handle whose
signature requests are a queue ([0045](0045-the-c-abi-is-the-boundary.md) decision 10),
proposal #17.

## 5. Follow-ups

- Plan B: Python (proposal #15).
- Plan C: JVM/Android, Erlang/Elixir and Go (proposal #16).
- Case 2: a signer handle in the ABI (proposal #17).
