# weida for Python

[weida](https://weida.doodleshnookie.net/) is a messaging framework built around QUIC. Every data
flow is one QUIC stream, so a 40-byte and a 40-GB transfer use the same protocol and neither has
to sit in memory. This package is weida for Python: cffi over `libweida`, weida's C ABI, with a
synchronous API and an asyncio one (`weida.asyncio`) of the same shape.

- **Patterns:** Req/Rep with streamed bodies, Push/Pull, Pub/Sub, PAIR, SURVEY, BUS,
  RADIO/DISH for lossy fan-out, datagram flows, raw peers, and SWARM for one verified blob from
  many sources.
- **Transports:** QUIC, in-process (`weida+inproc://`), Unix sockets and Windows named pipes,
  with the same API on each.
- **Identity is a key:** an address like `weida://sha256:<fingerprint>@host:port/path` names
  the peer's public key. Pinning it is the whole trust configuration: no CA, no certificate
  files.
- **Completions are cursors:** a sender learns how far the far end got (delivered, accepted,
  processed, up to which byte), not only whether it finished.

## Install

```sh
pip install weida
```

`weida` is published as wheels only. Each wheel contains `libweida`, so installing needs no
Rust toolchain and no compiler.

| Platform | Wheel tag |
| --- | --- |
| Linux x86_64 | `manylinux_2_<glibc>_x86_64`, built on Debian 13 (trixie) |
| Linux aarch64 | `manylinux_2_<glibc>_aarch64`, built on Debian 13 (trixie) |
| macOS 11 or later on Apple silicon | `macosx_11_0_arm64` |
| Windows x64 | `win_amd64` |

Every wheel is `py3-none`: one wheel serves CPython 3.9 and later, free-threaded CPython and
PyPy. CI tests CPython 3.13 and 3.14, free-threaded 3.14t and PyPy 3.11 against each wheel. On
any other platform, build a wheel from the repository with `crates/py/weida-py/package.sh`
(needs Rust).

## Synchronous

A server and a client over QUIC on localhost. The client trusts exactly the server's key.
Every blocking call takes a timeout in seconds.

```python
import threading

import weida

identity = weida.Identity.generate()
runtime = weida.Runtime.owned()
listener = runtime.listener()
binding = listener.bind_quic("127.0.0.1:0", weida.ServerTls(identity), 10.0)
replier = listener.replier("/echo")


def serve() -> None:
    request = replier.accept(10.0)
    body = request.take_body().collect(1024, 10.0)
    reply = request.reply(weida.TransferMeta("text/plain"), 10.0)
    reply.write_all(body.payload.upper(), 10.0)
    reply.finish().delivered(10.0)


server = threading.Thread(target=serve)
server.start()

trust = weida.Trust().add_pin(identity.fingerprint())
requester = runtime.requester(weida.ClientTls(trust))
requester.connect(f"weida://{identity.fingerprint()}@{binding.local_addr()}/echo", 10.0)
reply = requester.request(b"hello weida", 10.0)
print(reply.collect(1024, 10.0).payload)  # b'HELLO WEIDA'

server.join()
for handle in (requester, replier, binding, listener):
    handle.drop()
runtime.shutdown(10.0)
```

## asyncio

The same exchange over the in-process transport, on the caller's event loop:

```python
import asyncio

import weida
import weida.asyncio as wa


async def main() -> None:
    runtime = wa.Runtime.owned()
    listener = runtime.listener()
    binding = listener.bind_inproc("example")
    replier = listener.replier("/echo")
    requester = runtime.requester(weida.ClientTls(weida.Trust()))
    await requester.connect("weida+inproc://example/echo")

    async def serve() -> None:
        request = await replier.accept()
        body = await request.take_body().collect(1024)
        reply = await request.reply()
        await reply.write_all(body.payload.upper())
        await reply.finish().delivered()

    server = asyncio.ensure_future(serve())
    reply = await requester.request(b"in process")
    print((await reply.collect(1024)).payload)  # b'IN PROCESS'
    await server
    for handle in (requester, replier, binding, listener):
        handle.drop()
    await runtime.shutdown()


asyncio.run(main())
```

Failures raise a subclass of `weida.WeidaError` that names the cause, such as `Untrusted`,
`Rejected` or `ConnectionLost`. A timeout is `weida.Again`, which is also a `TimeoutError`.

## Learn more

- [Website](https://weida.doodleshnookie.net/): the
  [guide](https://weida.doodleshnookie.net/guide/), the
  [patterns](https://weida.doodleshnookie.net/reference/patterns/), and this package's
  [feature sheet](https://weida.doodleshnookie.net/libraries/weida-py/) with every class and
  method
- [Repository](https://github.com/tuco86/weida), package in `crates/py/weida-py`

Status: alpha (`0.1.0aN`). Wire protocol version 0 is experimental, and breaking changes are
permitted within `0.x`.

## License

MIT OR Apache-2.0, at your option.
