#!/bin/sh
# /// ci
# needs = ["c-abi-linux-amd64"]
# platform = "linux-amd64"
# image = "beam"
# timeout_minutes = 30
# ///
# The Erlang/Elixir environment (docs/decisions/0046): Erlang/OTP, Elixir and
# rebar3 run, and the image's C compiler builds and runs the C smoke program
# against the libweida c-abi-linux-amd64 handed on.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

step erl -noshell -eval 'io:format("~s~n",[erlang:system_info(otp_release)]),halt().'
step elixir --version
step rebar3 version
step cc --version
smoke_libweida
