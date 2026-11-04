// The patterns over the in-process transport.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };
const text = (bytes) => new TextDecoder().decode(bytes);

/** A listener of `runtime` bound to a bus of its own, and the bus. */
function bound(runtime) {
  const bus = `test-${randomUUID()}`;
  const listener = runtime.listener();
  return { bus, listener, binding: listener.bindInproc(bus) };
}

function clientTls() {
  return new weida.ClientTls(new weida.Trust());
}

/** Waits until `publisher` holds `n` filters: the subscriptions arrived. */
async function awaitFilters(publisher, n) {
  while (publisher.filterCount() !== n) await delay(1);
}

/** A publisher at `/md` on `server` and a subscriber of `px.#` on a runtime
 * of its own, made from `clientConfig`. */
async function pubSub(server, clientConfig) {
  const at = bound(server);
  const publisher = at.listener.publisher("/md");
  const client = weida.Runtime.owned(clientConfig);
  const subscriber = client.subscriber(clientTls());
  await subscriber.connect(`weida+inproc://${at.bus}/md`);
  await subscriber.subscribe("px.#");
  await awaitFilters(publisher, 1);
  return {
    publisher,
    subscriber,
    async close() {
      for (const handle of [subscriber, publisher, at.binding, at.listener]) handle.drop();
      await client.shutdown();
      await server.shutdown();
    },
  };
}

test("push to pull over inproc arrives in order", UNIT, async () => {
  const config = { guarantees: { ordering: "WEIDA_ORDERING_MODE_PER_PRODUCER_REASSEMBLE" } };
  const server = weida.Runtime.owned(config);
  const client = weida.Runtime.owned(config);
  const at = bound(server);
  const puller = at.listener.puller("/jobs");
  assert.equal(puller.path(), "/jobs");
  const pusher = client.pusher(clientTls());
  await pusher.connect(`weida+inproc://${at.bus}/jobs`);
  assert.equal(pusher.peerCount(), 1);

  await pusher.send("one");
  await pusher.send("two");
  await pusher.sendWith({ contentType: "application/json" }, "three");
  for (const [n, body, contentType] of [
    [0n, "one", null],
    [1n, "two", null],
    [2n, "three", "application/json"],
  ]) {
    const transfer = await puller.recv();
    const meta = transfer.meta();
    assert.equal(meta.sequence, n);
    assert.equal(meta.gap, null);
    assert.equal(meta.contentType, contentType);
    assert.equal(text((await transfer.collect(1024)).payload), body);
  }

  const out = await pusher.open({ contentType: "text/plain" });
  await out.writeAll("four");
  const delivery = out.finish();
  const fourth = await puller.recv();
  assert.equal(fourth.meta().contentType, "text/plain");
  assert.equal(text((await fourth.collect(1024)).payload), "four");
  await delivery.delivered();
  assert.equal(pusher.dropped(), 0n);

  for (const handle of [pusher, puller, at.binding, at.listener]) handle.drop();
  await client.shutdown();
  await server.shutdown();
});

test("a subscriber receives only its topic", UNIT, async () => {
  const server = weida.Runtime.owned();
  const at = bound(server);
  const publisher = at.listener.publisher("/md");
  const url = `weida+inproc://${at.bus}/md`;
  // One runtime per subscriber: two on one runtime collide on the path.
  const clients = [weida.Runtime.owned(), weida.Runtime.owned()];
  const [px, fx] = clients.map((c) => c.subscriber(clientTls()));
  await px.connect(url);
  await fx.connect(url);
  await px.subscribe("px.#");
  await fx.subscribe("fx.#");
  await assert.rejects(px.subscribe("#.px"), { code: "WEIDA_EPROTOCOL" });
  await awaitFilters(publisher, 2);
  assert.equal(publisher.subscriberCount(), 2);

  assert.equal(publisher.publish("px.eur", "price"), 1);
  const trace = weida.newTrace();
  assert.equal(publisher.publishWithTrace("fx.usd", "rate", trace), 1);
  const price = await px.recv();
  assert.equal(price.meta().topic, "px.eur");
  assert.equal(text((await price.collect(1024)).payload), "price");
  const rate = await fx.recv();
  assert.equal(rate.meta().topic, "fx.usd");
  assert.equal(rate.meta().trace, trace);
  assert.equal(text((await rate.collect(1024)).payload), "rate");
  await Promise.all(
    [px, fx].map((s) =>
      assert.rejects(s.recv({ signal: AbortSignal.timeout(200) }), { name: "TimeoutError" }),
    ),
  );

  for (const handle of [px, fx, publisher, at.binding, at.listener]) handle.drop();
  await Promise.all(clients.map((c) => c.shutdown()));
  await server.shutdown();
});

test("a skipped sequence reads as a gap under per-producer detect", UNIT, async () => {
  const MSG = 32 * 1024;
  const detect = { ordering: "WEIDA_ORDERING_MODE_PER_PRODUCER_DETECT" };
  // A budget of two copies, and a stream window the first copy overflows:
  // its writer stalls, the second fills the budget, the third is dropped.
  const server = weida.Runtime.owned({
    guarantees: detect,
    limits: { subscriberBufferBytes: 2 * MSG },
  });
  const p = await pubSub(server, { guarantees: detect, limits: { streamReceiveWindow: 8192 } });
  const payload = new Uint8Array(MSG);

  let published = 0n;
  while (p.publisher.dropped() === 0n) {
    p.publisher.publish("px.eur", payload);
    published += 1n;
    await delay(1);
  }
  const lost = p.publisher.dropped();
  assert.ok(published > lost, "the first copy was deliverable");
  const [drops] = p.publisher.drops();
  assert.equal(drops.topic, "px.eur");
  assert.equal(drops.subscriberBudget, lost);
  assert.ok(Object.isFrozen(drops));
  assert.equal(p.publisher.droppedOn("fx.none"), null);

  for (let n = 0n; n < published - lost; n += 1n) {
    const copy = await p.subscriber.recv();
    assert.equal(copy.meta().gap, null, "the lost copies were the last ones");
    assert.equal((await copy.collect(MSG)).payload.length, MSG);
  }

  let revealing = null;
  while (revealing === null) {
    p.publisher.publish("px.eur", payload);
    try {
      revealing = await p.subscriber.recv({ signal: AbortSignal.timeout(100) });
    } catch (error) {
      if (error.name !== "TimeoutError") throw error;
    }
  }
  const { gap } = revealing.meta();
  assert.ok(Object.isFrozen(gap));
  assert.ok(gap.seen - gap.expected >= lost, `${gap.seen} - ${gap.expected} < ${lost}`);
  revealing.drop();
  await p.close();
});

test("a fan-out streams one payload to every subscriber", UNIT, async () => {
  const p = await pubSub(weida.Runtime.owned());
  const fan = p.publisher.open("px.frame");
  assert.equal(fan.topic(), "px.frame");
  assert.equal(fan.subscribers(), 1);
  assert.equal(await fan.writeWithin("abc", 1000), 1);
  assert.equal(fan.writeNow("def"), 1);
  assert.equal(fan.finish(), 1);
  assert.throws(() => fan.finish(), { code: "WEIDA_ECLOSED" });
  const copy = await p.subscriber.recv();
  assert.equal(copy.meta().topic, "px.frame");
  assert.equal(text((await copy.collect(1024)).payload), "abcdef");
  await p.close();
});

test("a pair exchanges in both directions", UNIT, async () => {
  const server = weida.Runtime.owned();
  const client = weida.Runtime.owned();
  const at = bound(server);
  const url = `weida+inproc://${at.bus}/link`;
  const near = at.listener.pair("/link");
  assert.equal(near.path(), "/link");
  await assert.rejects(near.connect(url), { code: "WEIDA_EUNSUPPORTED" });
  assert.equal(near.events(), null);
  const far = client.pair(clientTls());
  assert.equal(far.path(), "");
  await far.connect(url);
  await assert.rejects(far.connect(url), { code: "WEIDA_ELIMIT_EXCEEDED" });
  assert.equal(far.peerCount(), 1);

  await far.send("ping");
  const ping = await near.recv();
  assert.equal(text((await ping.collect(1024)).payload), "ping");
  await near.sendWith({ contentType: "text/plain" }, "pong");
  const pong = await far.recv();
  assert.equal(pong.meta().contentType, "text/plain");
  assert.equal(text((await pong.collect(1024)).payload), "pong");
  assert.equal(far.dropped(), 0n);

  for (const handle of [far, near, at.binding, at.listener]) handle.drop();
  await client.shutdown();
  await server.shutdown();
});

test("a survey collects two respondents' answers within its window", UNIT, async () => {
  const server = weida.Runtime.owned();
  const client = weida.Runtime.owned();
  const ats = [bound(server), bound(server)];
  const respondents = ats.map((at) => at.listener.respondent("/poll"));
  const surveyor = client.surveyor(clientTls());
  for (const at of ats) await surveyor.connect(`weida+inproc://${at.bus}/poll`);
  assert.equal(surveyor.peerCount(), 2);

  const answering = respondents.map(async (respondent, i) => {
    const request = await respondent.accept();
    assert.equal(text((await request.takeBody().collect(1024)).payload), "who is there");
    const reply = await request.reply();
    await reply.writeAll(["a", "b"][i]);
    await reply.finish().delivered();
  });
  const run = await surveyor.survey("who is there", 60000);
  await Promise.all(answering);
  assert.equal(run.respondents(), 2);
  const answers = [text(await run.next(1024)), text(await run.next(1024))].sort();
  assert.deepEqual(answers, ["a", "b"]);
  assert.equal(await run.next(1024), null);
  assert.equal(run.late(), 0n);

  run.drop();
  surveyor.drop();
  for (const respondent of respondents) respondent.drop();
  for (const at of ats) {
    at.binding.drop();
    at.listener.drop();
  }
  await client.shutdown();
  await server.shutdown();
});

test("a bus message reaches every other member", UNIT, async () => {
  const runtimes = [0, 1, 2].map(() => weida.Runtime.owned());
  const ats = runtimes.map((runtime) => bound(runtime));
  const members = ats.map((at) => at.listener.bus("/bus", clientTls()));
  for (const [i, member] of members.entries()) {
    for (const [j, at] of ats.entries()) {
      if (j !== i) await member.connect(`weida+inproc://${at.bus}/bus`);
    }
  }

  for (const [i, member] of members.entries()) {
    assert.equal(await member.send(`from ${i}`), 2);
  }
  for (const [i, member] of members.entries()) {
    const seen = [];
    for (let n = 0; n < 2; n += 1) {
      seen.push(text((await (await member.recv()).collect(1024)).payload));
    }
    const others = [0, 1, 2].filter((j) => j !== i).map((j) => `from ${j}`);
    assert.deepEqual(seen.sort(), others);
  }
  await Promise.all(
    members.map((m) =>
      assert.rejects(m.recv({ signal: AbortSignal.timeout(200) }), { name: "TimeoutError" }),
    ),
  );
  for (const member of members) assert.equal(member.dropped(), 0n);

  for (const member of members) member.drop();
  for (const at of ats) {
    at.binding.drop();
    at.listener.drop();
  }
  await Promise.all(runtimes.map((runtime) => runtime.shutdown()));
});
