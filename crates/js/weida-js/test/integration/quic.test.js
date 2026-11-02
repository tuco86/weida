// Req/Rep over real QUIC on localhost, with a generated identity.
import assert from "node:assert/strict";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const INTEGRATION = { timeout: 10_000 };
const text = (bytes) => new TextDecoder().decode(bytes);

test("a round trip over QUIC with a pinned key", INTEGRATION, async () => {
  const identity = weida.Identity.generate();
  const fingerprint = identity.fingerprint();
  const runtime = weida.Runtime.owned();
  const listener = runtime.listener();
  const binding = await listener.bindQuic("127.0.0.1:0", new weida.ServerTls(identity));
  const replier = listener.replier("/echo");
  const requester = runtime.requester(new weida.ClientTls(new weida.Trust().addPin(fingerprint)));
  await requester.connect(`weida://${fingerprint}@${binding.localAddr()}/echo`);

  const served = (async () => {
    const request = await replier.accept();
    const body = await request.takeBody().collect(1024);
    const reply = await request.reply({ contentType: "text/plain" });
    await reply.writeAll(body.payload.toReversed());
    await reply.finish().delivered();
    return text(body.payload);
  })();
  const reply = await requester.request("ping");
  assert.equal(reply.meta().contentType, "text/plain");
  assert.equal(text((await reply.collect(1024)).payload), "gnip");
  assert.equal(await served, "ping");

  for (const handle of [requester, replier, binding, listener]) {
    handle.drop();
  }
  await runtime.shutdown();
});

test("a wrong pin rejects with WEIDA_EUNTRUSTED carrying the fingerprint", INTEGRATION, async () => {
  const identity = weida.Identity.generate();
  const runtime = weida.Runtime.owned();
  const listener = runtime.listener();
  const binding = await listener.bindQuic("127.0.0.1:0", new weida.ServerTls(identity));
  const replier = listener.replier("/echo");
  const other = weida.Identity.generate();
  const requester = runtime.requester(new weida.ClientTls(new weida.Trust().addPin(other.fingerprint())));

  await assert.rejects(
    async () => {
      await requester.connect(`weida://${binding.localAddr()}/echo`);
      await requester.request("hello");
    },
    { code: "WEIDA_EUNTRUSTED", fingerprint: identity.fingerprint() },
  );

  for (const handle of [requester, replier, binding, listener]) {
    handle.drop();
  }
  await runtime.shutdown();
});
