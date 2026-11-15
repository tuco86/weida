# weida for Python

[weida](https://github.com/tuco86/weida) is a messaging framework built around QUIC: every
data flow is one QUIC stream, so a 40-byte and a 40-GB transfer share one protocol and
neither has to sit in memory. This package is weida for Python: cffi in ABI mode over
`libweida`, weida's C ABI, with a synchronous API (timeouts in seconds) and `weida.asyncio`.
It covers Req/Rep with streamed bodies, Push/Pull, Pub/Sub, PAIR, SURVEY, BUS, RADIO/DISH,
datagram flows, raw peers and SWARM.

## Install

    pip install weida

Every version so far is an alpha (`0.1.0aN`). The wire protocol may still change within `0.x`.

## Wheels

`weida` is published as wheels only. Each wheel holds `libweida`, so installing it needs no
Rust toolchain and no compiler:

| Platform | Wheel tag |
| --- | --- |
| Linux x86_64 | `manylinux_2_<glibc>_x86_64`, built on Debian 13 (trixie) |
| Linux aarch64 | `manylinux_2_<glibc>_aarch64`, built on Debian 13 (trixie) |
| macOS 11 or later on Apple silicon | `macosx_11_0_arm64` |
| Windows x64 | `win_amd64` |

Every wheel is `py3-none`: one wheel serves CPython 3.9 and later, free-threaded CPython and
PyPy. CI runs CPython 3.13 and 3.14, free-threaded 3.14t and PyPy 3.11 against each wheel.
There is no source distribution. On any other platform pip finds nothing to install. Build
a wheel from the repository instead, with `crates/py/weida-py/package.sh` (needs Rust).

## Example

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
    print((await reply.collect(1024)).payload)
    await server
    for handle in (requester, replier, binding, listener):
        handle.drop()
    await runtime.shutdown()


asyncio.run(main())
```

The full surface, with a verdict per feature, is in
[docs/libraries/weida-py.md](https://github.com/tuco86/weida/blob/main/docs/libraries/weida-py.md).

## License

MIT OR Apache-2.0.
