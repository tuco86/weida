// Raw peers over the in-process transport: a peer's streams, exchanges,
// sends and segments, and a queue's consumers and credit.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };
const text = (bytes) => new TextDecoder().decode(bytes);
const clientTls = () => new weida.ClientTls(new weida.Trust());

test("a raw peer opens a stream that the acceptor reads", UNIT, async () => {
  const server = weida.Runtime.owned();
  const client = weida.Runtime.owned();
  const bus = `test-${randomUUID()}`;
  const url = `weida+inproc://${bus}/raw`;
  const listener = server.listener();
  const binding = listener.bindInproc(bus);
  const acceptor = listener.acceptor("/raw");
  const peer = client.peer(clientTls());
  const events = peer.events();
  await peer.connect(url);
  while ((await events.recv()).kind !== "WEIDA_PEER_EVENT_CONNECTED");
  events.drop();
  assert.equal(peer.peerCount(), 1);
  assert.equal(peer.connectionStats().length, 1);
  assert.equal(peer.dropped(), 0n);

  const out = await peer.open({ contentType: "text/plain" });
  await out.writeAll("one");
  out.finish().drop();
  const stream = await acceptor.accept();
  assert.equal(stream.kind, "WEIDA_INCOMING_STREAM");
  assert.ok(stream.transfer instanceof weida.IncomingTransfer);
  const one = await stream.transfer.collect(64);
  assert.equal(text(one.payload), "one");
  assert.equal(one.meta.contentType, "text/plain");

  const { body, reply } = await peer.openBi(null);
  await body.writeAll("ask");
  body.finish().drop();
  const exchange = await acceptor.accept();
  assert.equal(exchange.kind, "WEIDA_INCOMING_EXCHANGE");
  assert.ok(exchange.request instanceof weida.IncomingRequest);
  assert.equal(text((await exchange.request.takeBody().collect(64)).payload), "ask");
  const answer = await exchange.request.reply();
  await answer.writeAll("answer");
  answer.finish().drop();
  assert.equal(text((await (await reply.recv()).collect(64)).payload), "answer");

  await peer.send(null, "sent");
  const sent = await acceptor.accept();
  assert.equal(sent.kind, "WEIDA_INCOMING_STREAM");
  assert.equal(text((await sent.transfer.collect(64)).payload), "sent");

  const segment = await peer.segment("cam");
  assert.equal(segment.number(), 0n);
  segment.write("frame");
  segment.finish();
  const frame = await acceptor.accept();
  assert.equal(frame.kind, "WEIDA_INCOMING_STREAM");
  assert.equal(frame.transfer.meta().topic, "cam");
  assert.equal(frame.transfer.meta().segment, 0n);
  assert.equal(text((await frame.transfer.collect(64)).payload), "frame");
  assert.equal(peer.segmentDrops("cam"), null);

  assert.equal(peer.disconnect(url), true);
  assert.equal(peer.disconnect(url), false);
  assert.equal(peer.peerCount(), 0);
  await assert.rejects(peer.open(null), { code: "WEIDA_ENOT_CONNECTED" });

  for (const handle of [peer, acceptor, binding, listener]) handle.drop();
  await client.shutdown();
  await server.shutdown();
});

test("credit granted in two steps releases exactly that many deliveries", UNIT, async () => {
  const server = weida.Runtime.owned();
  const client = weida.Runtime.owned();
  const bus = `test-${randomUUID()}`;
  const listener = server.listener();
  const binding = listener.bindInproc(bus);
  const acceptor = listener.acceptor("/q");
  const subscriber = client.subscriber(clientTls());
  await subscriber.connect(`weida+inproc://${bus}/q`);
  await subscriber.subscribe("jobs.#");

  const subscribed = await acceptor.accept();
  assert.equal(subscribed.kind, "WEIDA_INCOMING_SUBSCRIBED");
  assert.ok(subscribed.consumer instanceof weida.Consumer);
  const consumer = subscribed.consumer;
  const id = consumer.id();
  assert.equal(typeof id, "bigint");
  assert.equal(consumer.filter(), "jobs.#");
  assert.equal(consumer.path(), "/q");

  // Each delivery is its own stream, so they may arrive in any order; and
  // nothing beyond the credit arrives.
  const receive = async (n) => {
    const got = [];
    for (let i = 0; i < n; i += 1) {
      got.push(text((await (await subscriber.recv()).collect(64)).payload));
    }
    await assert.rejects(subscriber.recv({ signal: AbortSignal.timeout(100) }), {
      name: "TimeoutError",
    });
    return got.sort();
  };

  // The test is the queue that honours the credit.
  await subscriber.grant("jobs.#", 2);
  assert.deepEqual(await acceptor.accept(), {
    kind: "WEIDA_INCOMING_CREDIT",
    id,
    filter: "jobs.#",
    limit: 2n,
  });
  const out = await consumer.open({ topic: "jobs.a" });
  await out.writeAll("job-0");
  out.finish().drop();
  await consumer.deliver({ topic: "jobs.a" }, "job-1");
  assert.deepEqual(await receive(2), ["job-0", "job-1"]);

  await subscriber.grant("jobs.#", 5);
  assert.deepEqual(await acceptor.accept(), {
    kind: "WEIDA_INCOMING_CREDIT",
    id,
    filter: "jobs.#",
    limit: 5n,
  });
  for (const n of [2, 3, 4]) await consumer.deliver({ topic: "jobs.a" }, `job-${n}`);
  assert.deepEqual(await receive(3), ["job-2", "job-3", "job-4"]);

  await subscriber.unsubscribe("jobs.#");
  assert.deepEqual(await acceptor.accept(), {
    kind: "WEIDA_INCOMING_UNSUBSCRIBED",
    id,
    filter: "jobs.#",
  });

  await subscriber.subscribe("other");
  const second = (await acceptor.accept()).consumer;
  assert.equal(second.id(), id);
  second.close(6, "done");
  for (;;) {
    const next = await acceptor.accept();
    if (next.kind === "WEIDA_INCOMING_UNSUBSCRIBED") {
      assert.deepEqual(next, { kind: next.kind, id, filter: null });
      break;
    }
    // The redialled connection subscribing again, under a new number.
    assert.equal(next.kind, "WEIDA_INCOMING_SUBSCRIBED");
    assert.notEqual(next.consumer.id(), id);
    next.consumer.drop();
  }

  for (const handle of [consumer, second, subscriber, acceptor, binding, listener]) handle.drop();
  await client.shutdown();
  await server.shutdown();
});
