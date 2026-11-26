# weida-openbao

[OpenBao](https://openbao.org/) (and Vault) as the source of a [weida](https://crates.io/crates/weida)
process's identity and trust.

1. **A client** (`OpenBao`) that authenticates with a token, an AppRole login or a **hand-off**:
   a response-wrapping token minted by a controller and redeemed exactly once. A failed
   redemption means somebody else already redeemed it. The client keeps its token renewed.
2. **An identity signed by a PKI mount** (`PkiSign`). The key stays local, and certificates are
   renewed under the same key, so the peer's fingerprint, and every pin on it, survives
   rotation.
3. **Trust anchored on the mount's CA** (`PkiAnchor`), and material read from KV (`Kv`).

It does not speak `http://` unless told to, does not follow redirects, caps every answer it
reads, and never prints a credential: tokens are wiped on drop and redacted in `Debug`.
Revocation is not checked; short TTLs with automatic renewal take its place.

## Learn more

- [Website](https://weida.doodleshnookie.net/), with the
  [feature sheet](https://weida.doodleshnookie.net/libraries/weida-openbao/) and the
  [identity-source decision](https://weida.doodleshnookie.net/decisions/0032-identity-sources-and-the-handoff/)
- [API documentation](https://docs.rs/weida-openbao)
- [Repository](https://github.com/tuco86/weida), with a systemd hand-off example in
  `crates/openbao/weida-openbao/examples/handoff`

Status: alpha; breaking changes are permitted within `0.x`.

## License

MIT OR Apache-2.0, at your option.
