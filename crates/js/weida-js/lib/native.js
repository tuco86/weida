// The Node-API addon (src/addon.c), loaded the one way Node, Bun and Deno
// all accept from an ES module.
import { createRequire } from "node:module";

const native = createRequire(import.meta.url)("../build/weida.node");

export default native;
