// Client keys over real QUIC on localhost: required clients, the chain the
// server sees, anchors and identity files.
import assert from "node:assert/strict";
import { X509Certificate } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const INTEGRATION = { timeout: 10_000 };

/** A runtime with a binding on 127.0.0.1 under `serverTls` and a replier at
 * `/echo`. */
async function serve(serverTls) {
  const runtime = weida.Runtime.owned();
  const listener = runtime.listener();
  const binding = await listener.bindQuic("127.0.0.1:0", serverTls);
  const replier = listener.replier("/echo");
  const url = `weida://${binding.localAddr()}/echo`;
  const close = async (...more) => {
    for (const handle of [...more, replier, binding, listener]) {
      handle.drop();
    }
    await runtime.shutdown();
  };
  return { runtime, replier, url, close };
}

/** One request answered: the metadata the replier saw. */
async function roundTrip(requester, replier, url) {
  await requester.connect(url);
  const served = (async () => {
    const request = await replier.accept();
    const meta = request.meta();
    const reply = await request.reply();
    await reply.writeAll("hi");
    await reply.finish().delivered();
    return meta;
  })();
  const reply = await requester.request("hello");
  assert.equal(new TextDecoder().decode((await reply.collect(64)).payload), "hi");
  return served;
}

async function refused(requester, url) {
  await assert.rejects(
    async () => {
      await requester.connect(url);
      await requester.request("hello");
    },
    { code: "WEIDA_ETLS" },
  );
}

test("a pinned client key is accepted and the server sees its fingerprint", INTEGRATION, async () => {
  const server = weida.Identity.generate();
  const client = weida.Identity.generate();
  const s = await serve(
    new weida.ServerTls(server, { requireClient: new weida.Trust().addPin(client.fingerprint()) }),
  );
  const trust = new weida.Trust().addPin(server.fingerprint());
  const requester = s.runtime.requester(new weida.ClientTls(trust, { identity: client }));
  const meta = await roundTrip(requester, s.replier, s.url);
  assert.equal(meta.peerFingerprint, client.fingerprint());
  await s.close(requester);
});

test("an anonymous or unknown client is refused with WEIDA_ETLS and nothing reaches the server", INTEGRATION, async () => {
  const server = weida.Identity.generate();
  const client = weida.Identity.generate();
  const s = await serve(
    new weida.ServerTls(server, { requireClient: new weida.Trust().addPin(client.fingerprint()) }),
  );
  const trust = new weida.Trust().addPin(server.fingerprint());
  const anonymous = s.runtime.requester(new weida.ClientTls(trust));
  await refused(anonymous, s.url);
  const stranger = s.runtime.requester(
    new weida.ClientTls(trust, { identity: weida.Identity.generate() }),
  );
  await refused(stranger, s.url);
  await assert.rejects(s.replier.accept({ signal: AbortSignal.timeout(200) }), {
    name: "TimeoutError",
  });
  await s.close(anonymous, stranger);
});

test("any-key exposes the fingerprint and the chain", INTEGRATION, async () => {
  const server = weida.Identity.generate();
  const client = weida.Identity.generate();
  const s = await serve(new weida.ServerTls(server, { requireClient: "any-key" }));
  const trust = new weida.Trust().addPin(server.fingerprint());
  const requester = s.runtime.requester(new weida.ClientTls(trust, { identity: client }));
  const meta = await roundTrip(requester, s.replier, s.url);
  assert.equal(meta.peerFingerprint, client.fingerprint());
  assert.equal(meta.peerChain.length, 1);
  assert.ok(meta.peerChain[0] instanceof X509Certificate);
  assert.deepEqual(meta.peerChain[0].raw, client.certificates()[0].raw);
  await s.close(requester);
});

test("an anchor accepts the dialled name", INTEGRATION, async () => {
  const server = weida.Identity.generateFor(["127.0.0.1"]);
  const s = await serve(new weida.ServerTls(server));
  const trust = new weida.Trust().addAnchor(server.certificates()[0]);
  const requester = s.runtime.requester(new weida.ClientTls(trust));
  const meta = await roundTrip(requester, s.replier, s.url);
  assert.equal(meta.peerFingerprint, null);
  await s.close(requester);
});

test("identity and anchor files read back", INTEGRATION, async () => {
  const dir = mkdtempSync(join(tmpdir(), "weida-js-"));
  try {
    const generated = weida.Identity.generateFor(["127.0.0.1"]);
    const pem = generated.toPem();
    writeFileSync(join(dir, "id.pem"), pem, { mode: 0o600 });
    pem.fill(0);
    writeFileSync(join(dir, "ca.pem"), generated.certificatePem());
    const server = weida.Identity.fromPemFile(join(dir, "id.pem"));
    assert.equal(server.fingerprint(), generated.fingerprint());

    const s = await serve(new weida.ServerTls(server));
    const trust = new weida.Trust().addAnchorFile(join(dir, "ca.pem"));
    const requester = s.runtime.requester(new weida.ClientTls(trust));
    await roundTrip(requester, s.replier, s.url);
    await s.close(requester);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
