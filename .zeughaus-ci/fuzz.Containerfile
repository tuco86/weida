# The image of the fuzz job: the Rust base of trixie.Containerfile plus the
# pinned nightly toolchain cargo-fuzz needs and cargo-fuzz itself. The
# nightly date equals `toolchain` in fuzz.sh; change both together.
FROM docker.io/library/rust:1.99.1-trixie

RUN rustup toolchain install nightly-2026-10-28 --profile minimal \
    && cargo install --locked cargo-fuzz --version 0.13.2
