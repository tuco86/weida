# The image the Linux builds, checks and the publish job run in: Debian 13
# with what the workspace links and the gate's toolchain parts.
# - python3-dev: the `*-py` crates' test binaries link libpython.
# - jq: publish.sh reads `cargo metadata`.
# - cmake, clang, libclang-dev, libzmq3-dev, pkg-config: the doc step's
#   `--all-features` builds `nng-sys` and `zmq-sys`.
# The same file serves amd64 under podman and arm64 under Apple `container`.
# A changed file is a new image tag. The rust tag equals the channel of
# rust-toolchain.toml; change both together.
FROM docker.io/library/rust:1.99.1-trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3-dev jq pkg-config cmake clang libclang-dev libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add clippy rustfmt rust-src
