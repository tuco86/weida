// Identity and trust sources, their events and the ALPN token, without a
// socket of their own.
import assert from "node:assert/strict";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const UNIT = { timeout: 1000 };

test("an external identity source reports its updates as events", UNIT, async () => {
  const a = weida.Identity.generate();
  const b = weida.Identity.generate();
  const src = weida.IdentitySource.external(a);
  assert.equal(src.fingerprint(), a.fingerprint());
  const events = src.events();

  src.update(a);
  let event = await events.recv();
  assert.ok(Object.isFrozen(event));
  assert.equal(event.kind, "WEIDA_IDENTITY_EVENT_RENEWED");
  assert.equal(event.fingerprint, a.fingerprint());

  src.update(b);
  event = await events.recv();
  assert.equal(event.kind, "WEIDA_IDENTITY_EVENT_KEY_CHANGED");
  assert.equal(event.from, a.fingerprint());
  assert.equal(event.to, b.fingerprint());
  assert.equal(event.fingerprint, null);

  src.reportFailure("denied");
  event = await events.recv();
  assert.equal(event.kind, "WEIDA_IDENTITY_EVENT_RENEWAL_FAILED");
  assert.equal(event.message, "denied");

  src.reportHandoffStolen();
  event = await events.recv();
  assert.equal(event.kind, "WEIDA_IDENTITY_EVENT_HANDOFF_STOLEN");

  assert.equal(src.current().fingerprint(), b.fingerprint());
  assert.equal(src.reload(), false);
  events.drop();
  src.drop();
});

test("a weak source upgrades until the last strong handle is gone and the events end", UNIT, async () => {
  const src = weida.IdentitySource.ephemeral(["localhost"]);
  const weak = src.downgrade();
  const strong = weak.upgrade();
  assert.ok(strong instanceof weida.IdentitySource);
  assert.equal(strong.fingerprint(), src.fingerprint());
  strong.drop();
  const events = src.events();
  src.drop();
  assert.equal(weak.upgrade(), null);
  const seen = [];
  for await (const event of events) {
    seen.push(event);
  }
  assert.deepEqual(seen, []);
  weak.drop();
});

test("a trust source reports a refresh and isEmpty follows it", UNIT, async () => {
  assert.equal(new weida.Trust().isEmpty(), true);
  const pinned = new weida.Trust().addPin(weida.Identity.generate().fingerprint());
  assert.equal(pinned.isEmpty(), false);
  const ts = weida.TrustSource.external(pinned);
  assert.equal(ts.isEmpty(), false);
  const events = ts.events();
  ts.update(new weida.Trust());
  const event = await events.recv();
  assert.equal(event.kind, "WEIDA_IDENTITY_EVENT_TRUST_REFRESHED");
  assert.equal(ts.isEmpty(), true);
  assert.equal(ts.current().isEmpty(), true);
  const weak = ts.downgrade();
  const strong = weak.upgrade();
  assert.ok(strong instanceof weida.TrustSource);
  strong.drop();
  events.drop();
  ts.drop();
  assert.equal(weak.upgrade(), null);
  weak.drop();
});

test("source and ALPN argument errors are TypeErrors", UNIT, () => {
  const id = weida.Identity.generate();
  assert.throws(() => new weida.ClientTls(new weida.Trust(), { alpn: "WEIDA_ALPN_NOPE" }), TypeError);
  assert.throws(() => new weida.ServerTls(id, { alpn: 1 }), TypeError);
  assert.throws(() => new weida.ClientTls("x"), TypeError);
  assert.throws(() => new weida.ClientTls(new weida.Trust(), { identity: "x" }), TypeError);
  assert.throws(() => new weida.ServerTls("x"), TypeError);
  assert.throws(() => new weida.ServerTls(id, { requireClient: "nope" }), TypeError);
  assert.throws(() => weida.IdentitySource.files("x", { poll: 1 }), TypeError);
  assert.throws(() => weida.IdentitySource.ephemeral("x"), TypeError);
  assert.throws(() => new weida.IdentitySource(), TypeError);
});
