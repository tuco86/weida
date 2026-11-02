#!/bin/sh
# /// ci
# needs = ["c-abi-linux-amd64"]
# platform = "linux-amd64"
# image = "beam"
# timeout_minutes = 30
# ///
# weida for Erlang and Elixir (docs/decisions/0046, docs/libraries/weida-beam.md):
# the image's C compiler builds the C smoke program and the NIF against the
# libweida c-abi-linux-amd64 handed on; rebar3 runs the EUnit unit tests and
# Elixir the in-process smoke. The nightly and a release tag also run the
# QUIC tests and the QUIC smoke.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

step erl -noshell -eval 'io:format("~s~n",[erlang:system_info(otp_release)]),halt().'
step elixir --version
step rebar3 version
step cc --version
smoke_libweida

cd crates/beam/weida
WEIDA_LIB_DIR="$(libweida_input)/lib"
export WEIDA_LIB_DIR
case $(tier) in
    ci) step rebar3 eunit; transports="inproc quic" ;;
    *) step rebar3 eunit --module=weida_tests; transports=inproc ;;
esac
step rebar3 compile
for transport in $transports; do
    step elixir -pa _build/default/lib/weida/ebin smoke.exs "$transport"
done
