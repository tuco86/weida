// Runtime.drain over the in-process transport.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };
const text = (bytes) => new TextDecoder().decode(bytes);
const HOUR_MS = 3_600_000; // a drain that waited for its deadline would time the test out

test("a drain counts a reply finished before it as delivered", UNIT, async () => {
  const bus = `drain-${randomUUID()}`;
  const server = weida.Runtime.owned();
  const listener = server.listener();
  const binding = listener.bindInproc(bus);
  const replier = listener.replier("/echo");
  const client = weida.Runtime.owned();
  const requester = client.requester(new weida.ClientTls(new weida.Trust()));
  await requester.connect(`weida+inproc://${bus}/echo`);
  const served = (async () => {
    const request = await replier.accept();
    const out = await request.reply();
    await out.writeAll("pong");
    out.finish().drop();
  })();
  const message = await (await requester.request("ping")).collect(64);
  assert.equal(text(message.payload), "pong");
  await served;

  const drained = await server.drain(HOUR_MS);
  assert.ok(Object.isFrozen(drained));
  assert.equal(drained.delivered, 1n);
  assert.equal(drained.outstanding, 0n);
  assert.throws(() => server.listener(), { code: "WEIDA_ECLOSED" }, "the drain consumed the runtime");
  for (const handle of [requester, replier, binding, listener]) {
    handle.drop();
  }
  await client.shutdown();
});

test("a drain with nothing pending resolves at once", UNIT, async () => {
  const runtime = weida.Runtime.owned();
  const drained = await runtime.drain(HOUR_MS);
  assert.equal(drained.delivered, 0n);
  assert.equal(drained.outstanding, 0n);
});

test("a deadline out of range rejects with a RangeError and consumes nothing", UNIT, async () => {
  const runtime = weida.Runtime.owned();
  await assert.rejects(runtime.drain(-1), RangeError);
  runtime.listener().drop();
  await runtime.shutdown();
});
