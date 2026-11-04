// Serving through a relay over real QUIC on localhost: a relay, a dial-only replier and a requester, each in a runtime of its own.
import assert from "node:assert/strict";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const INTEGRATION = { timeout: 10_000 };
const text = (bytes) => new TextDecoder().decode(bytes);

/** A relay runtime on localhost: its binding, its relay and the URL a
 * hidden peer registers at. A registration is addressed by its key, so the
 * relay asks every peer for one. */
async function startRelay() {
  const relayId = weida.Identity.generate();
  const runtime = weida.Runtime.owned();
  const listener = runtime.listener();
  const binding = await listener.bindQuic(
    "127.0.0.1:0",
    new weida.ServerTls(relayId, { requireClient: "any-key" }),
  );
  const relay = listener.relay();
  const url = `weida://${relayId.fingerprint()}@${binding.localAddr()}/.weida/relay`;
  return { relayId, runtime, listener, binding, relay, url };
}

test("a dial-only replier reached through a relay answers a requester", INTEGRATION, async () => {
  const r = await startRelay();
  assert.throws(() => r.listener.relay(), { code: "WEIDA_EALREADY_REGISTERED" });
  assert.equal(r.binding.relayEvents(), null);

  const asked = [];
  const serving = (async () => {
    for (;;) {
      let ask;
      try {
        ask = await r.relay.accept();
      } catch (error) {
        if (error.code === "WEIDA_ECLOSED") {
          return;
        }
        throw error;
      }
      asked.push({ peer: ask.peerFingerprint(), target: ask.target() });
      await ask.admit();
    }
  })();

  const hiddenId = weida.Identity.generate();
  const hidden = weida.Runtime.owned();
  const hiddenListener = hidden.listener();
  const hiddenBinding = await hiddenListener.bindRelay(
    r.url,
    new weida.ClientTls(new weida.Trust(), { identity: hiddenId }),
    new weida.ServerTls(hiddenId, { requireClient: "any-key" }),
  );
  const replier = hiddenListener.replier("/echo");

  const diallerId = weida.Identity.generate();
  const dialler = weida.Runtime.owned();
  const requester = dialler.requester(
    new weida.ClientTls(new weida.Trust(), { identity: diallerId }),
  );
  await requester.connect(
    `weida+relay://${hiddenId.fingerprint()}@${r.relayId.fingerprint()}@${r.binding.localAddr()}/echo`,
  );

  const served = (async () => {
    const request = await replier.accept();
    const peer = request.meta().peerFingerprint;
    request.takeBody().drop();
    const out = await request.reply();
    await out.writeAll("pong");
    await out.finish().delivered();
    return peer;
  })();
  const reply = await requester.request("ping");
  assert.equal(text((await reply.collect(1024)).payload), "pong");
  assert.equal(
    await served,
    diallerId.fingerprint(),
    "the replier sees the dialler's key, not the relay's",
  );
  assert.deepEqual(asked, [
    { peer: hiddenId.fingerprint(), target: null },
    { peer: diallerId.fingerprint(), target: hiddenId.fingerprint() },
  ]);

  const events = hiddenBinding.relayEvents();
  r.relay.drop();
  await serving;
  const lost = await events.recv();
  assert.equal(lost.kind, "WEIDA_PEER_EVENT_LOST");
  assert.equal(lost.cause, "WEIDA_LOSS_PEER_CLOSED");

  for (const handle of [events, requester, replier, hiddenBinding, hiddenListener]) {
    handle.drop();
  }
  r.binding.drop();
  r.listener.drop();
  await dialler.shutdown();
  await hidden.shutdown();
  await r.runtime.shutdown();
});

test("a registration the relay refuses rejects bindRelay with WEIDA_EREJECTED", INTEGRATION, async () => {
  const r = await startRelay();
  const hiddenId = weida.Identity.generate();
  const hidden = weida.Runtime.owned();
  const hiddenListener = hidden.listener();
  const via = new weida.ClientTls(new weida.Trust(), { identity: hiddenId });
  const tls = new weida.ServerTls(hiddenId);

  const refusing = (async () => {
    const ask = await r.relay.accept();
    await ask.refuse();
  })();
  await assert.rejects(hiddenListener.bindRelay(r.url, via, tls), { code: "WEIDA_EREJECTED" });
  await refusing;

  for (const handle of [via, tls, hiddenListener, r.relay, r.binding, r.listener]) {
    handle.drop();
  }
  await hidden.shutdown();
  await r.runtime.shutdown();
});
