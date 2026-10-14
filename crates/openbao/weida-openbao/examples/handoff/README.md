# The hand-off, end to end

The flow of [0032](../../../../docs/decisions/0032-identity-sources-and-the-handoff.md) §2 on a
workstation: systemd is the controller, `bao server -dev` is OpenBao, and
`examples/handoff.rs` is the service.

## What OpenBao needs

A policy for the service, a token role that grants it, a PKI role that signs the service's
CSR, and a controller token that may mint child tokens of that role and revoke by accessor:

```sh
export BAO_ADDR=http://127.0.0.1:8200 BAO_TOKEN=root
bao policy write weida-handoff - <<'EOF'
path "pki/sign/weida-handoff" { capabilities = ["update"] }
path "auth/token/renew-self"  { capabilities = ["update"] }
EOF
bao write auth/token/roles/weida-handoff allowed_policies=weida-handoff \
    orphan=false renewable=true token_period=60s
bao secrets enable pki
bao write pki/root/generate/internal common_name=weida-dev-ca ttl=24h key_type=ec key_bits=256
bao write pki/roles/weida-handoff allowed_domains=localhost allow_bare_domains=true \
    allow_ip_sans=true server_flag=true client_flag=true key_type=ec key_bits=256 \
    max_ttl=1h require_cn=false use_csr_sans=true
bao policy write weida-controller - <<'EOF'
path "auth/token/create/weida-handoff" { capabilities = ["update"] }
path "auth/token/revoke-accessor"      { capabilities = ["update"] }
EOF
mkdir -p ~/.config/weida-handoff
bao token create -policy=weida-controller -field=token > ~/.config/weida-handoff/controller.token
chmod 600 ~/.config/weida-handoff/controller.token
```

The role settings are the ones 0032 §2 lists: `key_type`/`key_bits` match the key weida
generates (ECDSA P-256), `allowed_domains`/`allow_ip_sans` cover what peers dial,
`server_flag`/`client_flag` give the EKU the verifiers expect, `use_csr_sans` takes the names
from the CSR, `require_cn=false` because a SAN is the name.

## Run it under a user manager

```sh
cargo build -p weida-openbao --example handoff
mkdir -p ~/.config/systemd/user
cp crates/openbao/weida-openbao/examples/handoff/weida-handoff.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user start weida-handoff
journalctl --user -u weida-handoff -f
```

The log shows `redeemed: policies ["default", "weida-handoff"] …`, then the fingerprint, then
`listening on 127.0.0.1:4433`. `weida request weida://<fingerprint>@127.0.0.1:4433/echo hi`
answers `hi`.

## The alarm

Steal the credential before the service reads it: raise `RestartSec=` or add a
`sleep 3` after the `ExecStartPre=` line, restart the unit, and redeem the wrapping token
yourself inside the window:

```sh
bao unwrap "$(cat "$XDG_RUNTIME_DIR/weida-handoff/bao-handoff")"   # as the service's uid, in the window
```

The service then exits with status 3 — `ALARM: the hand-off token was already redeemed` —
`ExecStopPost=` revokes whatever the thief holds by its accessor, and the restart mints a new
wrapping token. The second `unwrap` in OpenBao's audit log is the evidence; a deployment
alerts on both the exit code and that log line.

## What this does not protect

The running process's memory against a process of the same uid. That is
`NoNewPrivileges=`, `ProtectProc=` and `kernel.yama.ptrace_scope`, and the unit sets the two
it can.
