// SWARM over the in-process transport: a blob seeded by one runtime and
// fetched by two others, a declined session, and a piece a seeder says is
// gone.
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
  assert.equal(fetch.delivered().ones(), 0, "the maps answer while a next is pending");

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

test("a piece a seeder says is gone is unavailable until another seeder serves it", UNIT, async () => {
  const server = weida.Runtime.owned();
  const client = weida.Runtime.owned();
  const [busA, busB] = [`test-${randomUUID()}`, `test-${randomUUID()}`];
  const listenerA = server.listener();
  const bindingA = listenerA.bindInproc(busA);
  const seederA = listenerA.seeder("/blob");
  const listenerB = server.listener();
  const bindingB = listenerB.bindInproc(busB);
  const seederB = listenerB.seeder("/blob");
  const list = weida.PieceList.of(data, weida.PIECE_SIZE_MIN_LOG2);
  const blob = list.blob();

  // Seeder A holds every piece by HAVE, then says piece 3 is gone when it
  // is asked for it.
  const asked = [];
  const servingA = (async () => {
    const session = await seederA.accept();
    const held = weida.Bits.full(18);
    try {
      for (let ask = await session.next(); ask !== null; ask = await session.next()) {
        if (ask.kind === "WEIDA_ASK_LIST") {
          await session.answerList(list);
        } else if (ask.kind === "WEIDA_ASK_HAVE") {
          await session.answerHave(held);
        } else {
          asked.push(ask.index);
          if (ask.index === 3) {
            held.set(3, false);
            await session.answerGone(3);
          } else {
            const span = blob.span(ask.index);
            await session.answerPiece(data.subarray(Number(span.start), Number(span.end)));
          }
        }
      }
    } finally {
      held.drop();
      session.drop();
    }
  })();
  const stop = new AbortController();
  const servingB = serve(seederB, data, list, [], stop.signal);

  const swarm = client.swarm(clientTls());
  await swarm.connect(`weida+inproc://${busA}/blob`);
  const have = new weida.Bits(18);
  const fetch = swarm.fetch(blob, have);
  fetch.prioritize(3, 4);

  const out = new Uint8Array(LEN);
  const keep = (piece) => out.set(piece.bytes, Number(blob.span(piece.index).start));
  for (let i = 0; i < 17; i++) {
    const piece = await fetch.next();
    assert.notEqual(piece, null);
    assert.notEqual(piece.index, 3, "piece 3 is gone at the only seeder");
    keep(piece);
  }
  // Every round reads the map while `last` is pending.
  const last = fetch.next();
  for (;;) {
    const unavailable = fetch.unavailable();
    const [gone, ones] = [unavailable.get(3), unavailable.ones()];
    unavailable.drop();
    if (gone) {
      assert.equal(ones, 1);
      break;
    }
    await new Promise((r) => setTimeout(r, 5));
  }
  assert.equal(asked[0], 3, "the prioritized piece is ordered first");

  fetch.clearPriority();
  await swarm.connect(`weida+inproc://${busB}/blob`);
  const piece = await last;
  assert.equal(piece.index, 3);
  keep(piece);
  assert.equal(await fetch.next(), null);
  assert.deepEqual(weida.blobIdOf(out), blob.root());

  await servingA;
  stop.abort();
  await servingB;
  for (const handle of [fetch, have, swarm, seederA, seederB, blob, list]) handle.drop();
  for (const handle of [bindingA, bindingB, listenerA, listenerB]) handle.drop();
  await client.shutdown();
  await server.shutdown();
});
