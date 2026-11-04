// A binding's close, hand-over and disconnect over real QUIC on localhost.
import assert from "node:assert/strict";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const INTEGRATION = { timeout: 10_000 };
const text = (bytes) => new TextDecoder().decode(bytes);

/** One request answered with its own body: the reply as text. */
async function echo(requester, replier, body) {
  const served = (async () => {
    const request = await replier.accept();
    const message = await request.takeBody().collect(1024);
    const out = await request.reply();
    await out.writeAll(message.payload);
    await out.finish().delivered();
  })();
  const reply = await requester.request(body);
  const answer = text((await reply.collect(1024)).payload);
  await served;
  return answer;
}

test("a closed binding refuses the next dial", INTEGRATION, async () => {
  const identity = weida.Identity.generate();
  const server = weida.Runtime.owned();
  const listener = server.listener();
  const binding = await listener.bindQuic("127.0.0.1:0", new weida.ServerTls(identity));
  const replier = listener.replier("/echo");
  const url = `weida://${identity.fingerprint()}@${binding.localAddr()}/echo`;
  const client = weida.Runtime.owned();
  const requester = client.requester(new weida.ClientTls(new weida.Trust()));
  await requester.connect(url);
  assert.equal(await echo(requester, replier, "ping"), "ping");
  const events = requester.events();

  await binding.close();
  const lost = await events.recv();
  assert.equal(lost.kind, "WEIDA_PEER_EVENT_LOST");
  assert.equal(lost.cause, "WEIDA_LOSS_PEER_CLOSED");

  // Nobody answers on the closed port; the handshake times out.
  const late = weida.Runtime.owned({ limits: { idleTimeoutMs: 1000 } });
  const lateRequester = late.requester(new weida.ClientTls(new weida.Trust()));
  await assert.rejects(lateRequester.connect(url), {
    code: "WEIDA_ECONNECTION_LOST",
    lossCause: "WEIDA_LOSS_IDLE_TIMEOUT",
  });
  await assert.rejects(binding.handOver(), { code: "WEIDA_ECLOSED" });

  for (const handle of [events, lateRequester, requester, replier, listener]) {
    handle.drop();
  }
  await late.shutdown();
  await client.shutdown();
  await server.shutdown();
});

test("a hand-over keeps a requester's connection", INTEGRATION, async () => {
  const identity = weida.Identity.generate();
  const old = weida.Runtime.owned();
  const oldListener = old.listener();
  const binding = await oldListener.bindQuic("127.0.0.1:0", new weida.ServerTls(identity));
  const addr = binding.localAddr();
  const oldReplier = oldListener.replier("/echo");
  const client = weida.Runtime.owned();
  const requester = client.requester(new weida.ClientTls(new weida.Trust()));
  await requester.connect(`weida://${identity.fingerprint()}@${addr}/echo`);
  assert.equal(await echo(requester, oldReplier, "before"), "before");
  const events = requester.events();
  oldReplier.drop();

  const handoff = await binding.handOver();
  assert.ok(handoff.held() >= 1, "the client's close is held");
  assert.equal(handoff.localAddr(), addr);
  const socket = handoff.takeSocket();
  assert.throws(() => handoff.takeSocket(), { code: "WEIDA_ECLOSED" });
  const held = handoff.bytes();
  handoff.drop();

  const successor = weida.Runtime.owned();
  const listener = successor.listener();
  const newReplier = listener.replier("/echo");
  const newBinding = await listener.bindQuicSocket(socket, new weida.ServerTls(identity));
  assert.equal(newBinding.localAddr(), addr);
  assert.ok((await newBinding.takeOver(held)) >= 1);

  const lost = await events.recv();
  assert.equal(lost.kind, "WEIDA_PEER_EVENT_LOST");
  assert.equal(lost.cause, "WEIDA_LOSS_PEER_RESTARTING");
  let event = await events.recv();
  while (event.kind !== "WEIDA_PEER_EVENT_CONNECTED") {
    event = await events.recv();
  }
  // The requester kept its address: no second `connect`.
  assert.equal(await echo(requester, newReplier, "after"), "after");

  for (const handle of [events, requester, newReplier, newBinding, listener, oldListener]) {
    handle.drop();
  }
  await successor.shutdown();
  await client.shutdown();
  // The predecessor's runtime lives to the end, as a predecessor process would.
  await old.shutdown();
});

test("disconnect closes a peer's connections by its key", INTEGRATION, async () => {
  const serverId = weida.Identity.generate();
  const clientId = weida.Identity.generate();
  const server = weida.Runtime.owned();
  const listener = server.listener();
  const binding = await listener.bindQuic(
    "127.0.0.1:0",
    new weida.ServerTls(serverId, { requireClient: "any-key" }),
  );
  const replier = listener.replier("/echo");
  const client = weida.Runtime.owned();
  const requester = client.requester(
    new weida.ClientTls(new weida.Trust(), { identity: clientId }),
  );
  await requester.connect(`weida://${serverId.fingerprint()}@${binding.localAddr()}/echo`);
  assert.equal(await echo(requester, replier, "ping"), "ping");
  const events = requester.events();

  assert.equal(binding.disconnect(weida.Identity.generate().fingerprint()), 0);
  assert.throws(() => binding.disconnect("nope"), { code: "WEIDA_EINVALID_FINGERPRINT" });
  assert.equal(binding.disconnect(clientId.fingerprint()), 1);
  const lost = await events.recv();
  assert.equal(lost.kind, "WEIDA_PEER_EVENT_LOST");
  assert.equal(lost.cause, "WEIDA_LOSS_PEER_CLOSED");

  for (const handle of [events, requester, replier, binding, listener]) {
    handle.drop();
  }
  await client.shutdown();
  await server.shutdown();
});
