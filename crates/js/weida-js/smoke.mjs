// What the package must do under each runtime: load the addon and its
// libweida, and make one round trip over the transport named by the first
// argument, `inproc` or `quic` (a generated identity, pinned, and a client
// key from Node's own key types, which a binding requiring any key admits).
//
// Usage: node smoke.mjs <inproc|quic>
//        bun smoke.mjs <inproc|quic>
//        deno run --allow-read --allow-env --allow-ffi smoke.mjs <inproc|quic>
//
// It imports the package by its name, @tuco86/weida: in this directory that
// is the package itself, in package.sh's (package.ps1's) fresh project the
// installed tarball.
import { X509Certificate, createPrivateKey } from "node:crypto";
import * as weida from "@tuco86/weida";

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
let client;
if (transport === "quic") {
  const identity = weida.Identity.generate();
  binding = await listener.bindQuic(
    "127.0.0.1:0",
    new weida.ServerTls(identity, { requireClient: "any-key" }),
  );
  trust = trust.addPin(identity.fingerprint());
  url = `weida://${identity.fingerprint()}@${binding.localAddr()}/echo`;
  const gen = weida.Identity.generate();
  const pem = new TextDecoder().decode(gen.toPem());
  const key = createPrivateKey(pem.match(/-----BEGIN PRIVATE KEY-----[\s\S]+?-----END PRIVATE KEY-----/)[0]);
  client = weida.Identity.from({ cert: gen.certificates(), key });
} else {
  const bus = `smoke-${crypto.randomUUID()}`;
  binding = listener.bindInproc(bus);
  url = `weida+inproc://${bus}/echo`;
}
const replier = listener.replier("/echo");
const requester = runtime.requester(new weida.ClientTls(trust, { identity: client }));
await requester.connect(url);

const served = (async () => {
  const request = await replier.accept();
  if (client) {
    const meta = request.meta();
    if (meta.peerFingerprint !== client.fingerprint() || !(meta.peerChain[0] instanceof X509Certificate)) {
      throw new Error(`the server did not see the client key: ${meta.peerFingerprint}`);
    }
  }
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
