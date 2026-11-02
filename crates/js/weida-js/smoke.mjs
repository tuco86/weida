// What the package must do under each runtime: load the addon and its
// libweida, and make one round trip over the transport named by the first
// argument, `inproc` or `quic` (a generated identity, pinned).
//
// Usage: node smoke.mjs <inproc|quic>
//        bun smoke.mjs <inproc|quic>
//        deno run --allow-read --allow-env --allow-ffi smoke.mjs <inproc|quic>
//
// It imports the package by its name: in this directory that is the
// package itself, in package.sh's fresh project the installed tarball.
import * as weida from "weida";

const runtimeName = globalThis.Deno ? "deno" : globalThis.Bun ? "bun" : "node";
const transport = (globalThis.Deno ? Deno.args : process.argv.slice(2))[0];
if (transport !== "inproc" && transport !== "quic") {
  throw new Error("usage: smoke.mjs <inproc|quic>");
}

const runtime = weida.Runtime.owned();
const listener = runtime.listener();
let binding;
let trust = new weida.Trust();
let url;
if (transport === "quic") {
  const identity = weida.Identity.generate();
  binding = await listener.bindQuic("127.0.0.1:0", new weida.ServerTls(identity));
  trust = trust.addPin(identity.fingerprint());
  url = `weida://${identity.fingerprint()}@${binding.localAddr()}/echo`;
} else {
  const bus = `smoke-${crypto.randomUUID()}`;
  binding = listener.bindInproc(bus);
  url = `weida+inproc://${bus}/echo`;
}
const replier = listener.replier("/echo");
const requester = runtime.requester(new weida.ClientTls(trust));
await requester.connect(url);

const served = (async () => {
  const request = await replier.accept();
  const body = await request.takeBody().collect(1024);
  const reply = await request.reply({ contentType: "text/plain" });
  await reply.writeAll(body.payload.toReversed());
  await reply.finish().delivered();
})();

const reply = await requester.request("ping");
const message = await reply.collect(1024);
await served;
const text = new TextDecoder().decode(message.payload);
if (text !== "gnip" || message.meta.contentType !== "text/plain") {
  throw new Error(`unexpected reply ${JSON.stringify(text)} (${message.meta.contentType})`);
}
for (const handle of [requester, replier, binding, listener]) {
  handle.drop();
}
await runtime.shutdown();
console.log(`js smoke: ok (${runtimeName} ${transport}) libweida ${weida.version()}`);
