// Unix and named-pipe bindings, with the peer principal the kernel proved.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const INTEGRATION = { timeout: 10_000 };

/** Dials `url` and answers one request: the metadata the replier saw. */
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

test(
  "a round trip over a unix socket names the client's uid",
  { ...INTEGRATION, skip: process.platform === "win32" },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "weida-js-"));
    const socket = path.join(dir, "s.sock");
    const runtime = weida.Runtime.owned();
    const listener = runtime.listener();
    const binding = listener.bindUnix(socket);
    assert.equal(binding.path(), socket);
    const replier = listener.replier("/echo");
    const requester = runtime.requester(new weida.ClientTls(new weida.Trust()));

    const meta = await roundTrip(
      requester,
      replier,
      `weida+unix://${encodeURIComponent(socket)}/echo`,
    );
    assert.equal(meta.peerLocal.uid, process.geteuid());
    assert.equal(typeof meta.peerLocal.gid, "number");
    assert.equal("sid" in meta.peerLocal, false);
    assert.ok(Object.isFrozen(meta.peerLocal));
    assert.equal(meta.peerFingerprint, null);
    assert.throws(() => listener.bindPipe("x"), { code: "WEIDA_EUNSUPPORTED" });

    binding.drop();
    assert.equal(fs.existsSync(socket), false, "the drop removes the socket file");
    for (const handle of [requester, replier, listener]) {
      handle.drop();
    }
    await runtime.shutdown();
    fs.rmSync(dir, { recursive: true, force: true });
  },
);

test(
  "a round trip over a named pipe names the client's SID",
  { ...INTEGRATION, skip: process.platform !== "win32" },
  async () => {
    const name = `weida-js-${randomUUID()}`;
    const runtime = weida.Runtime.owned();
    const listener = runtime.listener();
    const binding = listener.bindPipe(name);
    assert.equal(binding.name(), name);
    const replier = listener.replier("/echo");
    const requester = runtime.requester(new weida.ClientTls(new weida.Trust()));

    const meta = await roundTrip(requester, replier, `weida+pipe://${name}/echo`);
    assert.match(meta.peerLocal.sid, /^S-1-/);
    assert.equal(meta.peerLocal.pid, process.pid);
    assert.equal("uid" in meta.peerLocal, false);
    assert.equal(meta.peerFingerprint, null);
    assert.throws(() => listener.bindUnix("x"), { code: "WEIDA_EUNSUPPORTED" });

    for (const handle of [requester, replier, binding, listener]) {
      handle.drop();
    }
    await runtime.shutdown();
  },
);
