#!/bin/sh
# /// ci
# on = ["push main", "tag v*", "nightly"]
# platform = "linux-amd64"
# image = "trixie"
# timeout_minutes = 45
# ///
# The C ABI and the Python package on it (docs/decisions/0045): the committed
# headers match what cbindgen derives from crates/weida-c, and the Python
# tests run against an editable install (the unit tier on a push or manual
# run, both tiers on the nightly and a release tag, which also run the C
# smoke program and the wheel's smoke in a fresh virtualenv).
set -eu

. "$CI_WORKSPACE/.zeughaus-ci/build.sh"
cargo_env

step crates/weida-c/header.sh --verify

venv="$CI_WORKSPACE/.ci-cache/venv"
step python3 -m venv "$venv"
step "$venv/bin/pip" install cffi hatchling pytest pytest-timeout pytest-socket
step "$venv/bin/pip" install --no-build-isolation -e crates/py/weida-py

case $(tier) in
    ci) step "$venv/bin/python" -m pytest crates/py/weida-py/tests ;;
    *) step "$venv/bin/python" -m pytest crates/py/weida-py/tests/unit ;;
esac

if [ "$(tier)" = ci ]; then
    step crates/weida-c/smoke.sh
    WEIDA_PY_PYTHON="$venv/bin/python" step crates/py/weida-py/package.sh
fi
