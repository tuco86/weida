// A resolver function over real QUIC on localhost: a name only it knows,
// refused and malformed answers, and a process that exits by itself.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { test } from "node:test";
import { promisify } from "node:util";

import * as weida from "../../lib/index.js";

const INTEGRATION = { timeout: 10_000 };

/** A runtime with a binding on 127.0.0.1 under `serverTls` and a replier at
 * `/echo`. */
async function serve(serverTls) {
  const runtime = weida.Runtime.owned();
  const listener = runtime.listener();
  const binding = await listener.bindQuic("127.0.0.1:0", serverTls);
  const replier = listener.replier("/echo");
  const port = binding.localAddr().split(":").pop();
  const close = async (...more) => {
    for (const handle of [...more, replier, binding, listener]) {
      handle.drop();
    }
    await runtime.shutdown();
  };
  return { runtime, replier, port, close };
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

/** Client TLS pinning a key nobody holds: a dial with nothing to trust is
 * refused before it resolves. */
function pinnedTls() {
  return new weida.ClientTls(new weida.Trust().addPin(weida.Identity.generate().fingerprint()));
}

test("a name only the resolver knows is dialled through it", INTEGRATION, async () => {
  const server = weida.Identity.generateFor(["svc.weida.test"]);
  const s = await serve(new weida.ServerTls(server));
  const lookups = [];
  const client = weida.Runtime.owned({
    resolver: async (lookup) => {
      lookups.push(lookup);
      return [`127.0.0.1:${s.port}`];
    },
  });
  const requester = client.requester(
    new weida.ClientTls(new weida.Trust().addPin(server.fingerprint())),
  );
  await roundTrip(requester, s.replier, "weida://svc.weida.test/echo");
  assert.deepEqual(lookups, [{ name: "svc.weida.test", port: null, maxAddresses: 8 }]);
  assert.ok(Object.isFrozen(lookups[0]));

  requester.drop();
  await client.shutdown();
  await s.close();
});

test("a refused or malformed answer rejects the connect with WEIDA_EINVALID_ADDRESS", INTEGRATION, async () => {
  const client = weida.Runtime.owned({
    resolver: async ({ name }) => {
      if (name === "garbled.weida.test") {
        return "127.0.0.1:1";
      }
      if (name === "many.weida.test") {
        return Array.from({ length: 9 }, (_, i) => `127.0.0.1:${i + 1}`);
      }
      throw new Error("no such service");
    },
  });
  const requesters = [];
  for (const [name, text] of [
    ["nowhere.weida.test", "no such service"],
    ["garbled.weida.test", "array of address strings"],
    ["many.weida.test", "9 addresses"],
  ]) {
    const requester = client.requester(pinnedTls());
    requesters.push(requester);
    await assert.rejects(
      requester.connect(`weida://${name}:7443/echo`),
      (e) => e.code === "WEIDA_EINVALID_ADDRESS" && e.message.includes(text),
    );
  }

  for (const requester of requesters) {
    requester.drop();
  }
  await client.shutdown();
});

test("a runtime with a resolver does not keep the process alive", INTEGRATION, async () => {
  const index = new URL("../../lib/index.js", import.meta.url).href;
  const script = `
    import * as weida from ${JSON.stringify(index)};
    const runtime = weida.Runtime.owned({ resolver: async () => { throw new Error("none"); } });
    const pin = weida.Identity.generate().fingerprint();
    const requester = runtime.requester(new weida.ClientTls(new weida.Trust().addPin(pin)));
    await requester.connect("weida://x.weida.test:1/p").catch((e) => console.log(e.code));
  `;
  const { stdout } = await promisify(execFile)(process.execPath, [
    "--input-type=module",
    "-e",
    script,
  ]);
  assert.equal(stdout.trim(), "WEIDA_EINVALID_ADDRESS");
});
