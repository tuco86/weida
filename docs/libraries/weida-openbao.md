# weida-openbao

OpenBao (and Vault) for weida
([decisions/0032](../decisions/0032-identity-sources-and-the-handoff.md)): the client, the
three ways a process authenticates, and the sources it drives. Its own crate, because the
OpenBao API is JSON over HTTPS and nothing else in the workspace speaks either; `reqwest`
(rustls, no default features, the TLS configuration built here on the same `ring` provider
weida uses) and `serde_json` (`Value` only) are the two dependencies that justify the cut.

## 1. What it is for

A weida process that must keep the same key across restarts — which the redial of
[0031](../decisions/0031-transparent-redial-and-the-sender-outbox.md) §4.7 depends on — and
have its certificate issued and rotated by an authority rather than self-signed, and prove
itself to that authority first. Three parts, in the order a process needs them:

| Part | Type | What it does |
| --- | --- | --- |
| the client | `OpenBao`, `Config`, `Auth`, `Secret` | authenticates by token, AppRole or the wrapped **hand-off**; keeps the token renewed (a failed renewal is retried inside what is left of the token, a token with no TTL or one the server stopped honouring is not renewed); `post_wrapped` and `revoke_accessor` for the controller side. Tokens and secret ids are a `Secret`: wiped on drop, redacted in `Debug`. `https://` only unless `Config::allow_insecure`; no redirect is followed, so the token never leaves for another host; an answer is read under a 16 MiB cap and a server's error text is cut to 1 KiB |
| identity signed by a role | `PkiSign` → `weida::IdentitySource` | a CSR over the key a `files` source holds, `pki/sign/<role>`, renewal under the **same key** at a fraction of the TTL |
| trust anchored on the CA | `PkiAnchor` → `weida::TrustSource` | `pki/cert/ca_chain` (or `cert/ca`), refreshed at an interval |
| material from KV | `Kv` → `weida::IdentitySource` | chain and key as two fields of a KV v2 secret; distribution, not rotation |

`pki/issue` is deliberately absent: it would generate a new key per issuance, and under
pinning a new key every TTL is a new peer every TTL.

## 2. The hand-off

The flow of 0032 §2: a controller mints a child token of the service's role, wrapped —
`auth/token/create/<role>` with `X-Vault-Wrap-TTL` — and the service redeems the wrapping token
as its very first request. `Auth::Handoff` reads it from a file or a systemd credential,
calls `sys/wrapping/unwrap` before anything else, and turns the one failure that matters —
`400 wrapping token is not valid or does not exist` — into `Error::HandoffStolen`, on which
the service exits. The controller keeps the child's `wrapped_accessor` from the minting
answer and revokes by it at every exit, which also kills whatever a thief redeemed.

`examples/handoff/` is the whole thing as a systemd user unit plus a service binary, exercised
against `bao server -dev`: a clean start redeems, signs an
identity with the role and answers a `weida request` pinned by that identity's fingerprint;
a start whose wrapping token was redeemed first by hand exits with status 3 and
`ExecStopPost=` leaves the thief with a revoked token. One constraint is recorded in the
unit: the credentials directory is mounted read-only for `ExecStartPre=` as for the
service, so the wrapping token travels through `RuntimeDirectory=` (0700, the unit's uid,
gone with the unit) and the service unlinks it once redeemed.

## 3. What the role must allow

From 0032 §2, checked where the crate can and stated here where it cannot:

| Role setting | Value | Why |
| --- | --- | --- |
| `key_type`, `key_bits` | `ec`, `256` | weida's generated key is ECDSA P-256; a mismatch fails the sign with the role's own error |
| `allowed_domains`, `allow_bare_domains`, `allow_ip_sans` | what peers dial | the anchor path checks names |
| `server_flag`, `client_flag` | as the peer's role | rustls' verifiers require the EKU |
| `use_csr_sans` | `true` | the names come from the CSR |
| `require_cn` | `false` | a SAN is the name; the crate still sends the first name as CN for a role that insists |
| `max_ttl` | ≥ `PkiSign::ttl` | a request above it is refused |

## 4. What it does not do

- **Revocation.** No CRL, no OCSP, on any path. The TTL is the answer and `PkiSign` is what
  makes a short one affordable.
- **A Python surface.** Nothing in `weida-py` reaches this crate yet.
- **The controller in Rust.** Minting is a `bao` invocation in a unit file; `post_wrapped`
  exists for a controller that is itself a weida process.

## 5. Tests

`tests/scripted.rs` runs the client, the three auths, the renewal pace and its retry, the refusals
(redirects, `http://`, oversized and hostile answers) and the three sources against a scripted HTTP
server in the crate — eighteen tests in about three seconds, in the gate. The scripted server speaks
`http://`, so its tests set `Config::allow_insecure`.
`tests/bao_dev.rs` runs the hand-off and the PKI path against the local `bao` binary in dev
mode, `#[ignore]`d because the gate has no OpenBao:

```text
cargo test -p weida-openbao --test it bao_dev:: -- --ignored
```
