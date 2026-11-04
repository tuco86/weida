// Datagram flows over real QUIC on localhost.
import assert from "node:assert/strict";
import { test } from "node:test";

import * as weida from "../../lib/index.js";

const INTEGRATION = { timeout: 10_000 };
const FLOWS = { limits: { datagramReceiveBytes: weida.DEFAULT_DATAGRAM_RECEIVE_BYTES } };

test("a flow carries datagrams over QUIC", INTEGRATION, async () => {
  const serverId = weida.Identity.generate();
  const server = weida.Runtime.owned(FLOWS);
  const listener = server.listener();
  const binding = await listener.bindQuic("127.0.0.1:0", new weida.ServerTls(serverId));
  const acceptor = listener.acceptor("/v");
  const client = weida.Runtime.owned(FLOWS);
  const peer = client.peer(new weida.ClientTls(new weida.Trust()));
  await peer.connect(`weida://${serverId.fingerprint()}@${binding.localAddr()}/v`);
  const flow = await peer.openFlow({ topic: "mic" });
  const { flow: incoming } = await acceptor.accept();
  assert.equal(incoming.info().topic, "mic");

  // QUIC never retransmits a datagram, so each one is sent again until it
  // arrives; a late copy of an earlier one is skipped.
  for (const i of [0, 1, 2]) {
    for (;;) {
      flow.send(new Uint8Array([i]));
      let d;
      try {
        d = await incoming.recv({ signal: AbortSignal.timeout(20) });
      } catch (e) {
        if (e.name === "TimeoutError") continue;
        throw e;
      }
      assert.notEqual(d, null, "the flow ended early");
      if (d[0] < i) continue;
      assert.deepEqual(d, new Uint8Array([i]));
      break;
    }
  }
  // Path MTU discovery moves the limit at any time, so the two are compared
  // only when the flow's limit held still across the path's read.
  for (;;) {
    const max = flow.maxPayload();
    const size = flow.pathStats().maxDatagramSize;
    if (flow.maxPayload() !== max) continue;
    assert.ok(size > BigInt(max), "the size includes the flow's prefix");
    break;
  }
  assert.notEqual(incoming.pathStats(), null);

  flow.drop();
  while ((await incoming.recv()) !== null) {
    // late copies before the end
  }

  for (const handle of [incoming, peer, acceptor, binding, listener]) handle.drop();
  await client.shutdown();
  await server.shutdown();
});
