// Datagram flows over the in-process transport: a raw peer opens them, an
// acceptor takes them.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };
const text = (bytes) => new TextDecoder().decode(bytes);
const FLOWS = { limits: { datagramReceiveBytes: weida.DEFAULT_DATAGRAM_RECEIVE_BYTES } };

test("a flow carries datagrams to an acceptor and ends after them", UNIT, async () => {
  const runtime = weida.Runtime.owned(FLOWS);
  const bus = `test-${randomUUID()}`;
  const listener = runtime.listener();
  const binding = listener.bindInproc(bus);
  const acceptor = listener.acceptor("/v");
  assert.equal(acceptor.path(), "/v");
  const peer = runtime.peer(new weida.ClientTls(new weida.Trust()));
  await peer.connect(`weida+inproc://${bus}/v`);
  const flow = await peer.openFlow({ topic: "mic", contentType: "audio/opus" });
  assert.ok(flow instanceof weida.Flow);
  flow.send("one");
  flow.send("two");
  flow.send(new Uint8Array([3]));

  const incoming = await acceptor.accept();
  assert.equal(incoming.kind, "WEIDA_INCOMING_FLOW");
  assert.ok(incoming.flow instanceof weida.IncomingFlow);
  assert.deepEqual(incoming.flow.info(), {
    endpoint: "/v",
    topic: "mic",
    contentType: "audio/opus",
    trace: null,
    peerFingerprint: null,
    peerChain: null,
    peerLocal: null,
  });
  const max = flow.maxPayload();
  assert.ok(max > 0);
  assert.equal(flow.pathStats(), null);
  assert.equal(incoming.flow.pathStats(), null);
  assert.throws(() => flow.send(new Uint8Array(max + 1)), { code: "WEIDA_ETOO_LARGE", max });
  assert.deepEqual(flow.stats(), { sent: 3n, tooLarge: 1n, discarded: 0n, notLive: 0n });

  flow.drop();
  const got = [];
  for await (const d of incoming.flow) {
    got.push(d);
    if (got.length === 3) {
      assert.deepEqual(incoming.flow.stats(), { received: 3n, overflow: 0n });
    }
  }
  assert.deepEqual(got.slice(0, 2).map(text), ["one", "two"]);
  assert.deepEqual(got[2], new Uint8Array([3]));
  assert.equal(got.length, 3);

  for (const handle of [peer, acceptor, binding, listener]) handle.drop();
  await runtime.shutdown();
});

test(
  "a flow without datagrams is unavailable, other kinds are tagged, and a refusal reaches the sender",
  UNIT,
  async () => {
    const server = weida.Runtime.owned(FLOWS);
    const near = weida.Runtime.owned(FLOWS);
    const far = weida.Runtime.owned();
    const bus = `test-${randomUUID()}`;
    const listener = server.listener();
    const binding = listener.bindInproc(bus);
    const acceptor = listener.acceptor("/v");
    const url = `weida+inproc://${bus}/v`;

    const farPeer = far.peer(new weida.ClientTls(new weida.Trust()));
    await farPeer.connect(url);
    await assert.rejects(farPeer.openFlow(), { code: "WEIDA_EDATAGRAMS_UNAVAILABLE" });

    const requester = near.requester(new weida.ClientTls(new weida.Trust()));
    await requester.connect(url);
    // The handler is attached first: the request rejects once the acceptor
    // drops the exchange.
    const rejected = assert.rejects(requester.request("x"), { code: "WEIDA_ENO_REPLY" });
    assert.deepEqual(await acceptor.accept(), { kind: "WEIDA_INCOMING_EXCHANGE" });
    await rejected;

    const peer = near.peer(new weida.ClientTls(new weida.Trust()));
    await peer.connect(url);
    const flow = await peer.openFlow();
    const { flow: received } = await acceptor.accept();
    received.refuse();
    let error = null;
    while (error === null) {
      try {
        flow.send("x");
        await delay(1);
      } catch (e) {
        error = e;
      }
    }
    assert.equal(error.code, "WEIDA_EREJECTED");
    flow.abort();

    for (const handle of [farPeer, requester, peer, acceptor, binding, listener]) handle.drop();
    await far.shutdown();
    await near.shutdown();
    await server.shutdown();
  },
);
