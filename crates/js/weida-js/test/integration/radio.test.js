// A radio's eviction by key over real QUIC on localhost.
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import * as weida from "../../lib/index.js";

const INTEGRATION = { timeout: 10_000 };

test("a radio evicts a dish by its key", INTEGRATION, async () => {
  const serverId = weida.Identity.generate();
  const clientId = weida.Identity.generate();
  const server = weida.Runtime.owned();
  const listener = server.listener();
  const binding = await listener.bindQuic(
    "127.0.0.1:0",
    new weida.ServerTls(serverId, { requireClient: "any-key" }),
  );
  const radio = listener.radio("/r");
  const client = weida.Runtime.owned();
  const dish = client.dish(new weida.ClientTls(new weida.Trust(), { identity: clientId }));
  await dish.join("v");
  await dish.connect(`weida://${serverId.fingerprint()}@${binding.localAddr()}/r`);
  while (radio.dishCount() !== 1) await delay(1);
  assert.equal(radio.dishDrops()[0].peerFingerprint, clientId.fingerprint());

  assert.equal(radio.evict(weida.Identity.generate().fingerprint(), "v"), 0);
  assert.throws(() => radio.evict("nope", "v"), { code: "WEIDA_EINVALID_FINGERPRINT" });
  assert.equal(radio.evict(clientId.fingerprint(), "v"), 1);
  const segment = radio.segment("v");
  assert.equal(segment.write("x"), 0, "the evicted dish gets no copy");
  assert.equal(segment.finish(), 0);

  for (const handle of [dish, radio, binding, listener]) handle.drop();
  await client.shutdown();
  await server.shutdown();
});
