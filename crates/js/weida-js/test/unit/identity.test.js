// Identities to and from PEM and Node's key types (docs/decisions/0047).
import assert from "node:assert/strict";
import { createHash, createPrivateKey, createPublicKey } from "node:crypto";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };

/** The `PRIVATE KEY` block of a PEM document. */
function keyBlock(pem) {
  const text = new TextDecoder().decode(pem);
  return text.match(/-----BEGIN PRIVATE KEY-----[\s\S]+?-----END PRIVATE KEY-----\n?/)[0];
}

test("toPem reads back with the same fingerprint", UNIT, () => {
  const id = weida.Identity.generate();
  const pem = id.toPem();
  assert.ok(pem instanceof Uint8Array);
  assert.equal(weida.Identity.fromPem(pem, pem).fingerprint(), id.fingerprint());
  assert.ok(!id.certificatePem().includes("PRIVATE KEY"));
});

test("certificates() are the identity's key", UNIT, () => {
  const id = weida.Identity.generate();
  const spki = id.certificates()[0].publicKey.export({ type: "spki", format: "der" });
  assert.equal(`sha256:${createHash("sha256").update(spki).digest("hex")}`, id.fingerprint());
});

test("Identity.from takes Node's key types", UNIT, () => {
  const id = weida.Identity.generate();
  const key = createPrivateKey(keyBlock(id.toPem()));
  const fromNode = weida.Identity.from({ cert: id.certificates(), key });
  assert.equal(fromNode.fingerprint(), id.fingerprint());
  const mixed = weida.Identity.from({ cert: id.certificates()[0], key: id.toPem() });
  assert.equal(mixed.fingerprint(), id.fingerprint());
});

test("generateFor names the certificate", UNIT, () => {
  const id = weida.Identity.generateFor(["localhost", "127.0.0.1"]);
  const names = id.certificates()[0].subjectAltName;
  assert.match(names, /DNS:localhost/);
  assert.match(names, /IP Address:127\.0\.0\.1/);
});

test("argument errors are TypeErrors", UNIT, () => {
  const id = weida.Identity.generate();
  const cert = id.certificates()[0];
  const publicKey = createPublicKey(createPrivateKey(keyBlock(id.toPem())));
  assert.throws(() => weida.Identity.from({ cert }), TypeError);
  assert.throws(() => weida.Identity.from({ cert, key: publicKey }), TypeError);
  assert.throws(() => weida.Identity.generateFor("x"), TypeError);
  assert.throws(() => new weida.ServerTls(id, { requireClient: "nope" }), TypeError);
});
