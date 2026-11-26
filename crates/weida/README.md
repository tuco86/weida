# weida

A messaging framework for Rust built around QUIC. Every data flow is one QUIC stream, so a
40-byte and a 40-GB transfer use the same protocol, and neither has to sit in memory. Flows
whose units are worthless once late go over QUIC datagrams instead.

- **Patterns:** Req/Rep, Push/Pull, Pub/Sub, PAIR, SURVEY and BUS (the whole nanomsg set),
  RADIO/DISH for lossy fan-out, and SWARM (feature `swarm`) for one verified blob from many
  sources.
- **Transports:** QUIC, in-process, `AF_UNIX` sockets and Windows named pipes, with the same API
  on each.
- **Completions are cursors:** a level plus an absolute byte offset, reported on a stream of its
  own. A sender learns how far the far end got, not only whether it finished.
- **Identity is a key:** an address like `weida://sha256:<fingerprint>@host:port/path` names
  the peer's public key. Nothing else is needed to trust it: no CA, no certificate files.
- **No reactor required:** `Runtime::owned` brings its own Tokio reactor, and the futures it
  returns run on any executor.

## Example

```rust
use weida::{Runtime, RuntimeConfig, TransferMeta, Trust};

async fn example() -> weida::Result<()> {
    let runtime = Runtime::new(RuntimeConfig::default())?;
    let requester = runtime.requester(Trust::by_address());
    requester
        .connect("weida://sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08@127.0.0.1:7443/transform")
        .await?;

    // One bidirectional stream: the request half and the reply half.
    let (mut transfer, reply) = requester.open(TransferMeta::default()).await?;
    transfer.write_all(b"hello weida").await?;
    transfer.finish()?;

    let body = reply.recv().await?.collect(64 * 1024).await?;
    println!("{}", String::from_utf8_lossy(&body));
    Ok(())
}
```

## Command line

`cargo install weida` installs the `weida` binary: `serve`, `request`, `send` and `sub`, with
the payload on stdin and stdout.

```console
$ weida serve --echo 'weida://127.0.0.1:7443/echo'
weida://sha256:22ed30a8…9f25@127.0.0.1:7443/echo
$ printf 'hello' | weida request 'weida://sha256:22ed30a8…9f25@127.0.0.1:7443/echo'
hello
```

## Features

| Feature | Default | Adds |
| --- | --- | --- |
| `generate` | yes | `Identity::generate`, self-signed identities for pinning; the `weida` binary needs it |
| `swarm` | no | `Seeder` and `Swarm`, content-addressed blobs from many sources |
| `test-util` | no | `SimNet`, a simulated network for tests without sockets |

## Learn more

- [Website](https://weida.doodleshnookie.net/): the [guide](https://weida.doodleshnookie.net/guide/),
  the [patterns](https://weida.doodleshnookie.net/reference/patterns/) and the
  [architecture](https://weida.doodleshnookie.net/reference/architecture/)
- [API documentation](https://docs.rs/weida)
- [Repository](https://github.com/tuco86/weida)

Status: alpha. Wire protocol version 0 is experimental, and breaking changes are permitted
within `0.x`.

## License

MIT OR Apache-2.0, at your option.
