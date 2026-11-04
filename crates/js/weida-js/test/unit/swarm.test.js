// SWARM over the in-process transport: a blob seeded by one runtime and
// fetched by two others, and a declined session.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };
const clientTls = () => new weida.ClientTls(new weida.Trust());

// 18 pieces of 1 KiB, the last one short.
const LEN = 17 * 1024 + 100;
const data = Uint8Array.from({ length: LEN }, (_, i) => Math.imul(i, 0x9e3779b1) >>> 24);

/** Answers a session's orders from `data` until the fetch finished them,
 * recording each order's kind. */
async function answer(session, data, list, asks) {
  try {
    const blob = session.blob();
    for (let ask = await session.next(); ask !== null; ask = await session.next()) {
      asks.push(ask.kind);
      if (ask.kind === "WEIDA_ASK_LIST") {
        await session.answerList(list);
      } else if (ask.kind === "WEIDA_ASK_HAVE") {
        const have = weida.Bits.full(blob.count());
        await session.answerHave(have);
        have.drop();
      } else {
        const span = blob.span(ask.index);
        await session.answerPiece(data.subarray(Number(span.start), Number(span.end)));
      }
    }
    blob.drop();
  } finally {
    session.drop();
  }
}

/** Accepts sessions until `signal` aborts, then waits for every one. */
async function serve(seeder, data, list, asks, signal) {
  const sessions = [];
  try {
    for (;;) {
      const session = answer(await seeder.accept({ signal }), data, list, asks);
      // Handled here so a failure waits for the `Promise.all` below.
      session.catch(() => {});
      sessions.push(session);
    }
  } catch (e) {
    if (!signal.aborted) {
      throw e;
    }
  }
  await Promise.all(sessions);
}

/** Reads `fetch` to its end, copying each piece to its span of `out`: the
 * indices it yielded, sorted. */
async function gather(fetch, blob, out) {
  const indices = [];
  for await (const piece of fetch) {
    const span = blob.span(piece.index);
    assert.equal(piece.bytes.length, Number(span.end - span.start));
    out.set(piece.bytes, Number(span.start));
    indices.push(piece.index);
  }
  return indices.sort((a, b) => a - b);
}

test("a blob seeded by one runtime is fetched by two others, which verify it", UNIT, async () => {
  const server = weida.Runtime.owned();
  const near = weida.Runtime.owned();
  const far = weida.Runtime.owned();
  const bus = `test-${randomUUID()}`;
  const url = `weida+inproc://${bus}/blob`;
  const listener = server.listener();
  const binding = listener.bindInproc(bus);
  const seeder = listener.seeder("/blob");
  assert.equal(seeder.path(), "/blob");

  const hasher = new weida.TreeHasher(weida.PIECE_SIZE_MIN_LOG2);
  hasher.update(data.subarray(0, 5000)).update(data.subarray(5000));
  const list = hasher.finish();
  const of = weida.PieceList.of(data, weida.PIECE_SIZE_MIN_LOG2);
  assert.deepEqual(list.toBytes(), of.toBytes());
  const blob = list.blob();
  assert.equal(blob.count(), 18);
  assert.equal(blob.size(), BigInt(LEN));
  assert.equal(blob.pieceLog2(), weida.PIECE_SIZE_MIN_LOG2);
  assert.deepEqual(blob.root(), weida.blobIdOf(data));
  assert.equal(blob.root().length, weida.BLOB_ID_LEN);
  assert.equal(list.check(0, data.subarray(0, 1024)), true);
  assert.equal(list.check(1, data.subarray(0, 1024)), false);

  const asks = [];
  const stop = new AbortController();
  const serving = serve(seeder, data, list, asks, stop.signal);

  // A fetches every piece and keeps the list it learnt.
  const a = near.swarm(clientTls());
  await a.connect(url);
  assert.equal(a.peerCount(), 1);
  const haveA = new weida.Bits(18);
  const fetchA = a.fetch(blob, haveA);
  const outA = new Uint8Array(LEN);
  assert.deepEqual(await gather(fetchA, blob, outA), Array.from({ length: 18 }, (_, i) => i));
  assert.deepEqual(weida.blobIdOf(outA), blob.root());
  assert.equal(fetchA.delivered().all(), true);
  assert.equal(fetchA.inFlight().ones(), 0);
  const known = fetchA.list();
  assert.deepEqual(known.toBytes(), list.toBytes());

  // B holds the even pieces and the list: it takes only the odd ones.
  const b = far.swarm(clientTls());
  await b.connect(url);
  const haveB = new weida.Bits(18);
  const outB = new Uint8Array(LEN);
  for (let i = 0; i < 18; i += 2) {
    haveB.set(i, true);
    const span = blob.span(i);
    outB.set(data.subarray(Number(span.start), Number(span.end)), Number(span.start));
  }
  const fetchB = b.fetch(blob, haveB, { list: known, perPeer: 2, maxInflight: 4 });
  assert.deepEqual(
    await gather(fetchB, blob, outB),
    Array.from({ length: 9 }, (_, i) => 2 * i + 1),
  );
  assert.deepEqual(weida.blobIdOf(outB), blob.root());
  assert.equal(asks.filter((kind) => kind === "WEIDA_ASK_LIST").length, 1);

  stop.abort();
  await serving;
  for (const handle of [fetchB, fetchA, haveB, haveA, known, b, a]) handle.drop();
  for (const handle of [seeder, blob, of, list, binding, listener]) handle.drop();
  await far.shutdown();
  await near.shutdown();
  await server.shutdown();
});

test("a declined session leaves the fetch WEIDA_ENOT_CONNECTED", UNIT, async () => {
  const server = weida.Runtime.owned();
  const client = weida.Runtime.owned();
  const bus = `test-${randomUUID()}`;
  const listener = server.listener();
  const binding = listener.bindInproc(bus);
  const seeder = listener.seeder("/blob");
  const swarm = client.swarm(clientTls());
  await swarm.connect(`weida+inproc://${bus}/blob`);

  const list = weida.PieceList.of(data, weida.PIECE_SIZE_MIN_LOG2);
  const blob = list.blob();
  const have = new weida.Bits(18);
  const short = new weida.Bits(17);
  assert.throws(() => swarm.fetch(blob, short), { code: "WEIDA_EINVAL" });
  const fetch = swarm.fetch(blob, have);
  const ended = assert.rejects(fetch.next(), { code: "WEIDA_ENOT_CONNECTED" });
  assert.throws(() => fetch.delivered(), { code: "WEIDA_EINVAL" });

  const session = await seeder.accept();
  assert.notEqual(await session.next(), null);
  await session.decline();
  await ended;
  assert.equal(fetch.delivered().ones(), 0);
  assert.equal(fetch.list(), null);

  for (const handle of [fetch, short, have, blob, list, swarm, seeder, binding, listener]) {
    handle.drop();
  }
  await client.shutdown();
  await server.shutdown();
});
