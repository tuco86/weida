# weida-http

An HTTP client and server in Rust: HTTP/1.1 over TCP and TLS, HTTP/2 over TLS (ALPN `h2`) and
HTTP/3 over QUIC (feature `h3`). It is built on the sans-I/O
[weida-http-codec](https://crates.io/crates/weida-http-codec) and
[weida-runtime](https://crates.io/crates/weida-runtime), and depends on neither `weida` nor its
protocol.

- **`Client`**: idle HTTP/1.1 connections pooled per origin, and one shared HTTP/2 or HTTP/3
  connection per origin. `request` sends a whole body, `open` streams one.
- **`Server`**: `accept` hands out one `Exchange` per request, answered through its
  `Responder`.
- **Bodies** stream in chunks with flow control behind them, or are collected under a cap.
- **Trust** is the caller's rustls configuration.

```rust
use weida_http::{Client, ClientOptions, Fields, Method};

async fn get() -> weida_http::Result<()> {
    let exec = weida_runtime::Exec::current()?;
    let client = Client::new(&exec, ClientOptions::default())?;
    let mut response = client
        .request(Method::GET, "http://127.0.0.1:8080/", Fields::new(), Default::default())
        .await?;
    let body = response.body.collect(1 << 20).await?;
    println!("{} {}", response.head.status, String::from_utf8_lossy(&body));
    Ok(())
}
```

It follows no redirects, stores no cookies, applies no content coding, speaks to no proxy and
has no h2c, no server push and no Alt-Svc discovery. It is interop-tested against hyper and h3
in both roles.

| Feature | Default | Adds |
| --- | --- | --- |
| `tls` | yes | HTTPS and HTTP/2 through rustls |
| `h3` | no | HTTP/3 over QUIC through quinn |

## Learn more

- [Website](https://weida.doodleshnookie.net/), with the
  [feature sheet](https://weida.doodleshnookie.net/libraries/http/)
- [API documentation](https://docs.rs/weida-http)
- [Repository](https://github.com/tuco86/weida)

Status: alpha; breaking changes are permitted within `0.x`.

## License

MIT OR Apache-2.0, at your option.
