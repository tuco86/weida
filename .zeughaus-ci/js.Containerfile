# The image the js-linux-* jobs run in: the three JavaScript runtimes the
# Node-API addon over libweida serves (docs/decisions/0046).
# - node:26-trixie: Node 26 and the C toolchain (gcc, make) the addon's C
#   glue and build.sh's smoke_libweida need; trixie is the glibc libweida is
#   built against.
# - bun, deno: copied from their official images; both load Node-API addons.
# The same file serves amd64 under podman and arm64 under Apple `container`.
FROM docker.io/library/node:26-trixie

COPY --from=docker.io/oven/bun:1.4.4 /usr/local/bin/bun /usr/local/bin/bun
COPY --from=docker.io/denoland/deno:bin-2.9.8 /deno /usr/local/bin/deno
