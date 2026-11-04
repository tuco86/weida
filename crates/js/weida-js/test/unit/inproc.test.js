// Req/Rep over the in-process transport: every wait on a readiness
// descriptor through the addon's waiter thread, no sockets.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };
const text = (bytes) => new TextDecoder().decode(bytes);

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

/** Answers one request with `answer(body)`, reading the body 3 bytes at a
 * time; resolves with the body. */
async function serve(replier, answer, contentType) {
  const request = await replier.accept();
  const body = request.takeBody();
  const pieces = [];
  for (let piece = await body.read(3); piece.length > 0; piece = await body.read(3)) {
    pieces.push(...piece);
  }
  const received = Uint8Array.from(pieces);
  const reply = await request.reply(contentType ? { contentType } : null);
  await reply.writeAll(answer(received));
  await reply.finish().delivered();
  return text(received);
}

/** A request whose answer the test does not look at. */
function requestQuietly(requester, body) {
  return requester
    .request(body)
    .then((reply) => reply.collect(1024))
    .catch((error) => {
      if (!(error instanceof weida.WeidaError)) {
        throw error;
      }
    });
}

test("a streamed round trip carries the body and the content type", UNIT, async () => {
  const p = await pair();
  const served = serve(p.replier, (body) => text(body).toUpperCase(), "text/plain");
  const reply = await p.requester.request("ping");
  assert.equal(reply.meta().contentType, "text/plain");
  const message = await reply.collect(1024);
  assert.equal(text(message.payload), "PING");
  assert.equal(message.meta.contentType, "text/plain");
  assert.equal(await served, "ping");
  await p.close();
});

test("reads of 3 bytes reassemble the body", UNIT, async () => {
  const p = await pair();
  const client = requestQuietly(p.requester, "0123456789");
  const request = await p.replier.accept();
  const body = request.takeBody();
  const pieces = [];
  for (let piece = await body.read(3); piece.length > 0; piece = await body.read(3)) {
    assert.ok(piece.length <= 3);
    pieces.push(text(piece));
  }
  assert.equal(pieces.join(""), "0123456789");
  assert.equal((await body.read(3)).length, 0);
  request.drop();
  await client;
  await p.close();
});

test("a collect over its ceiling rejects with WEIDA_ELIMIT_EXCEEDED", UNIT, async () => {
  const p = await pair();
  const client = requestQuietly(p.requester, "four");
  const request = await p.replier.accept();
  await assert.rejects(request.takeBody().collect(2), { code: "WEIDA_ELIMIT_EXCEEDED" });
  request.drop();
  await client;
  await p.close();
});

test("a reply dropped unfinished reads as WEIDA_ECANCELED", UNIT, async () => {
  const p = await pair();
  const server = (async () => {
    const request = await p.replier.accept();
    const reply = await request.reply();
    await reply.writeAll("ha");
    reply.drop();
  })();
  const reply = await p.requester.request("go");
  await assert.rejects(async () => {
    while ((await reply.read(64)).length > 0) {
      // drain until the reset arrives
    }
  }, { code: "WEIDA_ECANCELED" });
  await server;
  await p.close();
});

test("a second reply rejects with WEIDA_ECLOSED", UNIT, async () => {
  const p = await pair();
  const client = requestQuietly(p.requester, "once");
  const request = await p.replier.accept();
  (await request.reply()).drop();
  await assert.rejects(request.reply(), { code: "WEIDA_ECLOSED" });
  await client;
  await p.close();
});

test("two concurrent accepts on one replier serve two requests", UNIT, async () => {
  const p = await pair();
  const reverse = (body) => body.toReversed();
  const accepts = [serve(p.replier, reverse), serve(p.replier, reverse)];
  const replies = await Promise.all([p.requester.request("one"), p.requester.request("two")]);
  const payloads = await Promise.all(replies.map(async (reply) => text((await reply.collect(1024)).payload)));
  assert.deepEqual(payloads.sort(), ["eno", "owt"]);
  assert.deepEqual((await Promise.all(accepts)).sort(), ["one", "two"]);
  await p.close();
});

test("an aborted request rejects with the reason and the runtime still shuts down", UNIT, async () => {
  const p = await pair();
  const controller = new AbortController();
  const pending = p.requester.request("unanswered", { signal: controller.signal });
  const reason = new Error("given up");
  setImmediate(() => controller.abort(reason));
  await assert.rejects(pending, (error) => error === reason);
  await p.close();
});

test("dropping a replier rejects its pending accept with WEIDA_ECLOSED", UNIT, async () => {
  const p = await pair();
  const accept = p.replier.accept();
  setImmediate(() => p.replier.drop());
  await assert.rejects(accept, { code: "WEIDA_ECLOSED" });
  await p.close();
});

test("a refused request rejects the requester with the refused code's error", UNIT, async () => {
  const p = await pair();
  for (const [code, expected] of [
    ["WEIDA_ERROR_CODE_REJECTED", "WEIDA_EREJECTED"],
    ["WEIDA_ERROR_CODE_UNKNOWN_ENDPOINT", "WEIDA_EUNKNOWN_ENDPOINT"],
    ["WEIDA_ERROR_CODE_UNSUPPORTED", "WEIDA_EUNSUPPORTED"],
    ["WEIDA_ERROR_CODE_INTERNAL", "WEIDA_ETRANSPORT"],
    ["WEIDA_ERROR_CODE_NO_REPLY", "WEIDA_ENO_REPLY"],
  ]) {
    // The handler is attached first: the request rejects while the
    // refusal's own wait is still settling.
    const rejected = assert.rejects(p.requester.request("ask"), { code: expected });
    const request = await p.replier.accept();
    await request.refuse(code);
    await rejected;
  }
  await p.close();
});

test("a refusal code outside WEIDA_ERROR_CODE_* is a TypeError and leaves the request usable", UNIT, async () => {
  const p = await pair();
  const rejected = assert.rejects(p.requester.request("ask"), { code: "WEIDA_EREJECTED" });
  const request = await p.replier.accept();
  await assert.rejects(request.refuse("WEIDA_EREJECTED"), TypeError);
  await request.refuse("WEIDA_ERROR_CODE_REJECTED");
  await rejected;
  await p.close();
});

test("canceled settles once the requester gives up", UNIT, async () => {
  const p = await pair();
  const controller = new AbortController();
  const pending = p.requester.request("wait", { signal: controller.signal });
  const request = await p.replier.accept();
  const canceled = request.canceled();
  assert.equal(await Promise.race([canceled.then(() => "settled"), delay(50, "pending")]), "pending");
  const reason = new Error("given up");
  controller.abort(reason);
  await assert.rejects(pending, (error) => error === reason);
  await canceled;
  request.drop();
  await p.close();
});

test("an opened exchange reads its reply while the request is still open", UNIT, async () => {
  const p = await pair();
  const { body, reply } = await p.requester.open({ contentType: "text/plain" });
  for (const piece of ["one", "two", "three"]) {
    await body.writeAll(piece);
  }
  const request = await p.replier.accept();
  assert.equal(request.meta().contentType, "text/plain");
  const received = request.takeBody();
  const answer = await request.reply(null);
  await answer.writeAll("ack");
  answer.finish().drop();
  const message = await (await reply.recv()).collect(1024);
  assert.equal(text(message.payload), "ack");
  body.finish().drop();
  const pieces = [];
  for (let piece = await received.read(4); piece.length > 0; piece = await received.read(4)) {
    pieces.push(...piece);
  }
  assert.equal(text(Uint8Array.from(pieces)), "onetwothree");
  await p.close();
});

test("dropping a reply stream tells the replier nobody is listening", UNIT, async () => {
  const p = await pair();
  const { body, reply } = await p.requester.open(null);
  const request = await p.replier.accept();
  const canceled = request.canceled();
  assert.equal(await Promise.race([canceled.then(() => "settled"), delay(50, "pending")]), "pending");
  reply.drop();
  await canceled;
  request.drop();
  body.drop();
  await p.close();
});
