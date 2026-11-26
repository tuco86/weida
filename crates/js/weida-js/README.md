# weida for JavaScript

[weida](https://weida.doodleshnookie.net/) is a messaging framework built around QUIC. Every data
flow is one QUIC stream, so a 40-byte and a 40-GB transfer use the same protocol and neither has
to sit in memory. This package is weida for Node, Bun and Deno: one Node-API addon over
`libweida`, weida's C ABI, with an ES module and TypeScript types on top.

It covers Req/Rep with streamed bodies, Push/Pull, Pub/Sub, PAIR, SURVEY, BUS, RADIO/DISH,
datagram flows, raw peers and SWARM, over QUIC, in-process, Unix sockets and Windows named pipes.

## Install

```sh
npm install weida
```

The package ships prebuilt binaries, so installing it needs no Rust toolchain and no compiler.

| Platform | Prebuild |
| --- | --- |
| Linux x64, Linux arm64 (glibc) | `linux-x64`, `linux-arm64` |
| macOS on Apple silicon | `darwin-arm64` |
| Windows x64 | `win32-x64` |

It runs on Node 20 and later, on Bun, and on Deno with
`--allow-read --allow-env --allow-ffi`. CI tests Node 26, Bun 1.4 and Deno 2.9 on every platform.

## Example

A server and a client in one process, over QUIC on localhost. The client trusts exactly one
key, the server's, and nothing else:

```js
import * as weida from "weida";

const runtime = weida.Runtime.owned();
const listener = runtime.listener();

const identity = weida.Identity.generate();
const binding = await listener.bindQuic("127.0.0.1:0", new weida.ServerTls(identity));
const replier = listener.replier("/echo");

const trust = new weida.Trust().addPin(identity.fingerprint());
const requester = runtime.requester(new weida.ClientTls(trust));
await requester.connect(`weida://${identity.fingerprint()}@${binding.localAddr()}/echo`);

const served = (async () => {
  const request = await replier.accept();
  const body = await request.takeBody().collect(1024);
  const reply = await request.reply({ contentType: "text/plain" });
  await reply.writeAll(body.payload);
  await reply.finish().delivered();
})();

const reply = await requester.request("hello weida");
console.log(new TextDecoder().decode((await reply.collect(1024)).payload));
await served;

for (const handle of [requester, replier, binding, listener]) handle.drop();
await runtime.shutdown();
```

The JavaScript thread never blocks: every wait is a promise, and every waiting method takes
`{ signal }`, so `AbortSignal.timeout(ms)` is the timeout. Failures are one class,
`WeidaError`, to branch on by `error.code` (for example `"WEIDA_EUNTRUSTED"`).

## Learn more

- [Website](https://weida.doodleshnookie.net/): the
  [guide](https://weida.doodleshnookie.net/guide/), the
  [patterns](https://weida.doodleshnookie.net/reference/patterns/), and this package's
  [feature sheet](https://weida.doodleshnookie.net/libraries/weida-js/) with every class and
  method
- [Repository](https://github.com/tuco86/weida), package in `crates/js/weida-js`

Status: alpha (`0.1.0-alpha.N`). Wire protocol version 0 is experimental, and breaking changes
are permitted within `0.x`.

## License

MIT OR Apache-2.0, at your option.
