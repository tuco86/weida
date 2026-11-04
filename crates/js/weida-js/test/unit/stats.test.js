// Connection statistics, the runtime's counters, connection numbers and peer
// events over the in-process transport.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };
const text = (bytes) => new TextDecoder().decode(bytes);

/** A runtime with a replier at `/echo` and a requester connected to it over
 * a bus of its own; `url` is the dialled URL. */
async function pair() {
  const bus = `stats-${randomUUID()}`;
  const url = `weida+inproc://${bus}/echo`;
  const runtime = weida.Runtime.owned();
  const listener = runtime.listener();
  const binding = listener.bindInproc(bus);
  const replier = listener.replier("/echo");
  const requester = runtime.requester(new weida.ClientTls(new weida.Trust()));
  await requester.connect(url);
  return {
    runtime,
    url,
    replier,
    requester,
    async close() {
      for (const handle of [requester, replier, binding, listener]) {
        handle.drop();
      }
      await runtime.shutdown();
    },
  };
}

test("an inproc connection is reported with its url and no transport", UNIT, async () => {
  const p = await pair();
  const served = (async () => {
    const request = await p.replier.accept();
    const id = request.connection();
    assert.equal(typeof id, "bigint");
    const body = request.takeBody();
    assert.equal(body.connection(), id);
    body.drop();
    const out = await request.reply();
    await out.writeAll("pong");
    out.finish().drop();
  })();
  const message = await (await p.requester.request("ping")).collect(64);
  assert.equal(text(message.payload), "pong");
  await served;

  const stats = p.requester.connectionStats();
  assert.ok(Object.isFrozen(stats));
  assert.equal(stats.length, 1);
  const [record] = stats;
  assert.ok(Object.isFrozen(record));
  assert.equal(record.url, p.url);
  assert.equal(record.redials, 0n);
  assert.equal(typeof record.ageUs, "bigint");
  assert.equal(record.transport, null);
  assert.equal(record.remote, null);
  assert.equal(p.runtime.suppressedDuplicates(), 0n);
  await p.close();
});

test("expiredTransfers counts a reply expired with expireAfterMs(0)", UNIT, async () => {
  const p = await pair();
  assert.equal(p.runtime.expiredTransfers(), 0n);
  const served = (async () => {
    const request = await p.replier.accept();
    const out = await request.reply();
    out.expireAfterMs(0);
    assert.throws(() => out.finish(), { code: "WEIDA_EEXPIRED" });
  })();
  const reply = await p.requester.request("go");
  await assert.rejects(reply.collect(64), { code: "WEIDA_ECANCELED" });
  await served;
  assert.equal(p.runtime.expiredTransfers(), 1n);
  await p.close();
});

test("a replier runtime shut down yields Lost then Retrying", UNIT, async () => {
  const bus = `stats-${randomUUID()}`;
  const url = `weida+inproc://${bus}/echo`;
  const server = weida.Runtime.owned();
  const listener = server.listener();
  const binding = listener.bindInproc(bus);
  const replier = listener.replier("/echo");
  const client = weida.Runtime.owned();
  const requester = client.requester(new weida.ClientTls(new weida.Trust()));
  await requester.connect(url);
  const events = requester.events()[Symbol.asyncIterator]();
  for (const handle of [replier, binding, listener]) {
    handle.drop();
  }
  await server.shutdown();

  const lost = (await events.next()).value;
  assert.equal(lost.kind, "WEIDA_PEER_EVENT_LOST");
  assert.equal(lost.url, url);
  assert.match(lost.cause, /^WEIDA_LOSS_/);
  const retrying = (await events.next()).value;
  assert.equal(retrying.kind, "WEIDA_PEER_EVENT_RETRYING");
  assert.equal(retrying.attempt, 1);
  assert.equal(typeof retrying.delayUs, "bigint");
  assert.equal(retrying.cause, null);

  requester.drop();
  assert.equal((await events.next()).done, true, "a requester that is gone ends its events");
  await client.shutdown();
});
