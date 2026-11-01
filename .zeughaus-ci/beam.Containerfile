# The image the beam-linux-* jobs run in: Erlang/OTP 29, rebar3 and Elixir
# 1.20, for the NIF over libweida (docs/decisions/0046).
# - build-essential: the C compiler the NIF and build.sh's smoke_libweida
#   need.
# The same file serves amd64 under podman and arm64 under Apple `container`.
FROM docker.io/library/elixir:1.20-otp-29

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
