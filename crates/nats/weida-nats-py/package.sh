#!/bin/sh
# Builds the release wheel and proves it works with no Rust toolchain present;
# see crates/py/package.sh.
#
# The fresh virtualenv installs no server and no C library: a NATS client that
# needs neither at install time is the claim being checked. It runs a
# publish/subscribe round trip, a request-reply over an inbox and the
# `NATS/1.0 503`. The peer is the tests' scripted server, because `nats-server`
# is absent on most machines and the wheel is what is under test here.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
exec "$here/../../py/package.sh" "$here" weida_nats
