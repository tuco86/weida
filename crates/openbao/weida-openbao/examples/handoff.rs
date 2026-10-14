//! The service half of the hand-off
//! ([0032](../../../docs/decisions/0032-identity-sources-and-the-handoff.md) §2):
//! redeem the wrapped token the controller left in the unit's
//! `RUNTIME_DIRECTORY` — first, before anything else — then live on it: sign an
//! identity with the PKI role, bind a weida replier, answer until stopped.
//!
//! The controller half is `examples/handoff/weida-handoff.service`. Its
//! `bao-controller` credential is read by `ExecStartPre=`, which mints the
//! wrapped token and keeps the child's accessor from the answer, and by
//! `ExecStopPost=`, which revokes the child by that accessor at every exit.
//! See `examples/handoff/README.md`.
//!
//! Exit codes: `0` stopped; `3` the hand-off was stolen — somebody redeemed
//! the wrapping token before this process did, which is the alarm the flow
//! exists to raise; `1` anything else.

use std::process::ExitCode;
use std::time::Duration;

use weida::{FilesOptions, IdentitySource, Runtime, RuntimeConfig, ServerTls, TransferMeta};
use weida_openbao::{Auth, Config, Error, HandoffSource, OpenBao, PkiSign};
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(Error::HandoffStolen) => {
            eprintln!("ALARM: the hand-off token was already redeemed; exiting");
            ExitCode::from(3)
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> weida_openbao::Result<()> {
    // 1. Redeem. Nothing is read, opened or bound before this succeeds. The
    //    controller's ExecStartPre= wrote the wrapping token into the unit's
    //    RUNTIME_DIRECTORY (0700, this uid's, gone with the unit); the
    //    credentials directory would be the better place but is read-only
    //    to ExecStartPre=. `HANDOFF_FILE` names the file; a systemd
    //    credential by name is `Auth::handoff_credential` instead.
    let bao = OpenBao::new(Config::from_env()?)?;
    let handoff = std::env::var_os("HANDOFF_FILE")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("RUNTIME_DIRECTORY")
                .map(|d| std::path::PathBuf::from(d).join("bao-handoff"))
        })
        .ok_or_else(|| Error::Credential("HANDOFF_FILE or RUNTIME_DIRECTORY must be set".into()))?;
    let info = bao
        .login(Auth::Handoff(HandoffSource::File(handoff.clone())))
        .await?;
    // Spent: the token is dead either way, the file need not outlive it.
    let _ = std::fs::remove_file(&handoff);
    eprintln!(
        "redeemed: policies {:?}, ttl {:?}, renewable {}",
        info.policies, info.ttl, info.renewable
    );

    // 2. The identity: a key that stays in STATE_DIRECTORY, a certificate the
    //    role signs and renews under it. The accessor is the controller's:
    //    it read `wrapped_accessor` when it minted the token.
    let state = std::env::var_os("STATE_DIRECTORY")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("weida-handoff"));
    let key = IdentitySource::files(
        state.join("identity"),
        FilesOptions {
            names: vec!["localhost".into()],
            ..FilesOptions::default()
        },
    )?;
    let role = std::env::var("PKI_ROLE").unwrap_or_else(|_| "weida-handoff".into());
    let identity = PkiSign {
        ttl: Some(Duration::from_secs(600)),
        ..PkiSign::new(role, ["localhost"])
    }
    .start(bao.clone(), &key)
    .await?;
    eprintln!("identity: {}", identity.fingerprint()?);

    // 3. Serve until stopped.
    let runtime = Runtime::new(RuntimeConfig::default())?;
    let listener = runtime.listener();
    let bind = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1:4433".into());
    let binding = listener
        .bind_quic(
            bind.parse()
                .map_err(|e| Error::Credential(format!("BIND: {e}")))?,
            ServerTls::new(identity),
        )
        .await?;
    eprintln!("listening on {}", binding.local_addr());
    let replier = listener.replier("/echo")?;
    // Stopped by SIGTERM, whose default action ends the process; the
    // controller's ExecStopPost= revokes the token either way.
    loop {
        let mut request = replier.accept().await?;
        let body = request.take_body().collect(64 * 1024).await?;
        let mut reply = request.reply(TransferMeta::default()).await?;
        reply.write_all(&body).await?;
        reply.finish()?;
    }
}
