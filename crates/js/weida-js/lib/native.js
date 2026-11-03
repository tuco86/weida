// The Node-API addon (src/addon.c) of this platform, from
// prebuilds/<platform>-<arch>/ (build.sh writes it there), loaded the one
// way Node, Bun and Deno all accept from an ES module.
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import process from "node:process";
import { fileURLToPath } from "node:url";

const target = `${process.platform}-${process.arch}`;
const path = fileURLToPath(new URL(`../prebuilds/${target}/weida.node`, import.meta.url));
if (!existsSync(path)) {
  throw new Error(
    `weida: no addon for ${target}: ${path} does not exist; the package carries linux-x64, linux-arm64, darwin-arm64 and win32-x64, and build.sh (build.ps1 on Windows) builds one from source`,
  );
}
const native = createRequire(import.meta.url)(path);

export default native;
