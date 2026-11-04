// The cursor surface over the in-process transport: report orders, the
// sender's queue of cursor sets, and the receiver's reporter.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };
const text = (bytes) => new TextDecoder().decode(bytes);
const ACCEPTED = "WEIDA_ACKNOWLEDGEMENT_ACCEPTED";
const PROGRESS = "WEIDA_REPORT_MODE_PROGRESS";
const FINAL_ONLY = "WEIDA_REPORT_MODE_FINAL_ONLY";

/** A runtime with a replier at `/echo` and a requester connected to it over
 * a bus of its own. */
async function pair() {
  const bus = `test-${randomUUID()}`;
  const runtime = weida.Runtime.owned();
  const listener = runtime.listener();
  const binding = listener.bindInproc(bus);
  const replier = listener.replier("/echo");
  const requester = runtime.requester(new weida.ClientTls(new weida.Trust()));
  await requester.connect(`weida+inproc://${bus}/echo`);
  return {
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

/** `transfer` read to its end, as text. */
async function readAll(transfer) {
  let out = "";
  for (let piece = await transfer.read(4); piece.length > 0; piece = await transfer.read(4)) {
    out += text(piece);
  }
  return out;
}

test("a request that orders Accepted receives the reported offsets in order", UNIT, async () => {
  const p = await pair();
  const { body, reply } = await p.requester.open({ report: [ACCEPTED, ACCEPTED] });
  const cursors = body.cursors();
  assert.ok(cursors instanceof weida.Cursors);
  assert.equal(body.cursors(), null, "the queue is handed out once");
  await body.writeAll("aaaabbbbcccc");
  body.finish().drop();

  const request = await p.replier.accept();
  const meta = request.meta();
  assert.deepEqual(meta.report, [ACCEPTED]);
  assert.equal(meta.reportMode, PROGRESS);
  assert.equal(typeof meta.reportId, "bigint");
  const received = request.takeBody();
  const reporter = received.reporter();
  assert.deepEqual(reporter.levels(), [ACCEPTED]);
  assert.equal(reporter.mode(), PROGRESS);
  reporter.setGranularity(0, 0);
  assert.equal(await readAll(received), "aaaabbbbcccc");
  await reporter.report("WEIDA_ACKNOWLEDGEMENT_STORED", 12);

  for (const offset of [4n, 8n]) {
    await reporter.report(ACCEPTED, offset);
    assert.deepEqual(await cursors.recv(), [{ level: ACCEPTED, offset }]);
  }
  await reporter.report(ACCEPTED, 12);
  await reporter.finish();
  assert.deepEqual(await cursors.recv(), [{ level: ACCEPTED, offset: 12n }]);
  assert.equal(await cursors.recv(), null);

  const answer = await request.reply(null);
  assert.equal(answer.cursors(), null, "the reply ordered nothing");
  await answer.writeAll("ok");
  answer.finish().drop();
  const back = await reply.recv();
  assert.equal(back.reporter(), null);
  assert.equal(await readAll(back), "ok");

  for (const handle of [cursors, received, back, reply]) {
    handle.drop();
  }
  await p.close();
});

test("final-only yields exactly one update per level", UNIT, async () => {
  const p = await pair();
  const tooMany = Array.from({ length: 17 }, (_, i) => weida.CURSOR_LEVEL_APPLICATION_FLOOR + i);
  await assert.rejects(p.requester.open({ report: tooMany }), { code: "WEIDA_ELIMIT_EXCEEDED" });
  await assert.rejects(p.requester.open({ report: [7] }), { code: "WEIDA_EINVAL" });

  const stage = weida.CURSOR_LEVEL_APPLICATION_FLOOR + 1;
  const { body, reply } = await p.requester.open({
    report: [stage, ACCEPTED],
    reportMode: FINAL_ONLY,
  });
  const cursors = body.cursors();
  await body.writeAll("0123456789");
  body.finish().drop();

  const request = await p.replier.accept();
  const meta = request.meta();
  assert.deepEqual(meta.report, [ACCEPTED, BigInt(stage)]);
  assert.equal(meta.reportMode, FINAL_ONLY);
  const received = request.takeBody();
  const reporter = received.reporter();
  assert.deepEqual(reporter.levels(), [ACCEPTED, BigInt(stage)]);
  assert.equal(reporter.mode(), FINAL_ONLY);
  assert.equal(await readAll(received), "0123456789");
  for (const offset of [5, 10]) {
    for (const level of [ACCEPTED, stage]) {
      await reporter.report(level, offset);
    }
  }
  await assert.rejects(cursors.recv({ signal: AbortSignal.timeout(100) }), {
    name: "TimeoutError",
  });

  await reporter.finish();
  const seen = new Set();
  let last = null;
  for await (const set of cursors) {
    for (const { level, offset } of set) seen.add(`${level}@${offset}`);
    last = set;
  }
  assert.deepEqual([...seen].sort(), [`${ACCEPTED}@10`, `${stage}@10`].sort());
  assert.deepEqual(last, [
    { level: ACCEPTED, offset: 10n },
    { level: BigInt(stage), offset: 10n },
  ]);

  for (const handle of [reply, request, received]) {
    handle.drop();
  }
  await p.close();
});
