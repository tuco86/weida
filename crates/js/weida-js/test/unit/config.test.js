// The runtime configuration and request metadata over the in-process
// transport: guarantee negotiation, trace propagation, deadlines and the
// errors a configuration object throws at once.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };
const text = (bytes) => new TextDecoder().decode(bytes);

/** A runtime with a replier at `/echo` and a requester connected to it over
 * a bus of its own. */
async function pair() {
  const bus = `config-${randomUUID()}`;
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

test("a guarantee the dialler does not offer fails with WEIDA_ENEGOTIATION", UNIT, async () => {
  const bus = `config-${randomUUID()}`;
  const url = `weida+inproc://${bus}/echo`;
  const guarantees = { ordering: "WEIDA_ORDERING_MODE_PER_PRODUCER_DETECT" };
  const server = weida.Runtime.owned({ guarantees });
  const listener = server.listener();
  const binding = listener.bindInproc(bus);
  const replier = listener.replier("/echo");

  const plain = weida.Runtime.owned();
  const refused = plain.requester(new weida.ClientTls(new weida.Trust()));
  await assert.rejects(async () => {
    await refused.connect(url);
    await refused.request("hello");
  }, { code: "WEIDA_ENEGOTIATION" });

  const alike = weida.Runtime.owned({ guarantees });
  const requester = alike.requester(new weida.ClientTls(new weida.Trust()));
  await requester.connect(url);
  const served = (async () => {
    const request = await replier.accept();
    const reply = await request.reply();
    await reply.writeAll("hi");
    reply.finish();
  })();
  const reply = await requester.request("hello");
  assert.equal(text((await reply.collect(64)).payload), "hi");
  await served;

  for (const handle of [requester, refused, replier, binding, listener]) {
    handle.drop();
  }
  await alike.shutdown();
  await plain.shutdown();
  await server.shutdown();
});

test("metadata travels with a request and its reply", UNIT, async () => {
  const p = await pair();
  const t = weida.newTrace();
  const served = (async () => {
    const request = await p.replier.accept();
    const meta = request.meta();
    assert.equal(meta.endpoint, "/echo");
    assert.equal(meta.contentLen, 4n);
    assert.equal(meta.topic, "jobs");
    assert.equal(meta.trace, t);
    assert.equal(meta.tracestate, null);
    const out = await request.reply({ achieved: "WEIDA_ACKNOWLEDGEMENT_ACCEPTED" });
    out.setPriority(5);
    assert.equal(out.trace(), t, "a reply inherits the request's trace");
    await out.writeAll("pong");
    out.finish();
  })();
  const reply = await p.requester.requestWith({ contentLen: 4, topic: "jobs", trace: t }, "ping");
  const meta = reply.meta();
  assert.equal(meta.trace, t);
  assert.equal(meta.achieved, "WEIDA_ACKNOWLEDGEMENT_ACCEPTED");
  assert.equal(text((await reply.collect(64)).payload), "pong");
  await served;
  await p.close();
});

test("an expired reply fails its finish with WEIDA_EEXPIRED", UNIT, async () => {
  const p = await pair();
  const served = (async () => {
    const request = await p.replier.accept();
    const out = await request.reply();
    out.expireAfterMs(0);
    assert.throws(() => out.finish(), { code: "WEIDA_EEXPIRED" });
  })();
  const reply = await p.requester.request("go");
  await assert.rejects(reply.collect(64), { code: "WEIDA_ECANCELED" });
  await served;
  await p.close();
});

test("a full configuration builds a runtime", UNIT, async () => {
  const runtime = weida.Runtime.owned({
    workerThreads: 2,
    shutdownTimeoutMs: 500,
    maxConnections: 100,
    maxConnectionsPerPeer: 10,
    endpointQueue: 16,
    maxResolvedAddresses: 4,
    connectAttemptTimeoutMs: 100,
    sendTimeoutMs: 2000n,
    outboxMessages: 10,
    outboxBytes: 1 << 20,
    maxHandshakes: 32,
    retryAboveHandshakes: 8,
    outboxFull: "WEIDA_OUTBOX_FULL_REJECT",
    discovery: "WEIDA_DISCOVERY_SINGLE",
    limits: {
      idleTimeoutMs: 6000,
      keepAliveMs: 2000,
      congestion: "WEIDA_CONGESTION_BBR",
      pathReport: true,
    },
    reconnect: { never: true },
  });
  runtime.listener().drop();
  await runtime.shutdown();
});

test("configuration input errors are thrown at once", UNIT, () => {
  assert.throws(() => weida.Runtime.owned({ outboxFull: "WEIDA_OUTBOX_FULL_SPILL" }), TypeError);
  assert.throws(() => weida.Runtime.owned({ limits: { idleTimeout: 1 } }), {
    name: "TypeError",
    message: "unknown limits field: idleTimeout",
  });
  assert.throws(
    () => weida.Runtime.owned({ endpointQueue: 0 }),
    (error) => error instanceof weida.WeidaError && error.code === "WEIDA_EINVAL",
  );
  assert.throws(() => weida.Runtime.owned({ maxConnections: -1 }), RangeError);
});

test("newTrace is a sampled W3C traceparent", UNIT, () => {
  assert.match(weida.newTrace(), /^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
});
