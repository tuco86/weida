#!/bin/sh
# /// ci
# needs = ["c-abi-linux-amd64"]
# platform = "linux-amd64"
# image = "python"
# timeout_minutes = 30
# ///
# weida's Python package on CPython 3.14, free-threaded CPython 3.14t and
# PyPy 3.11 (docs/decisions/0046): the wheel c-abi-linux-amd64 handed on
# installs into a fresh virtualenv per interpreter, importing weida on 3.14t
# leaves the GIL disabled, and the tests run against the installed wheel (the
# unit tier on a push or manual run, both tiers on the nightly and a release
# tag).
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

export UV_CACHE_DIR="$CI_WORKSPACE/.ci-cache/uv"
wheel=$(ls "$(libweida_input)"/wheels/weida-*.whl)

for pair in cpython:cpython-3.14.7 cpython-ft:cpython-3.14.7+freethreaded pypy:pypy-3.11.15; do
    name=${pair%%:*} request=${pair#*:}
    venv="$CI_WORKSPACE/.ci-cache/venv-$name"
    step uv venv --clear --python "$request" "$venv"
    step uv pip install --python "$venv" "$wheel" pytest pytest-timeout pytest-socket
    step "$venv/bin/python" -c 'import sys, weida; print(sys.version); print("libweida", weida.version())'
    if [ "$name" = cpython-ft ]; then
        step "$venv/bin/python" -c 'import sys, weida; assert not sys._is_gil_enabled(), "importing weida re-enabled the GIL"; print("free-threaded: GIL disabled")'
    fi
    case $(tier) in
        ci) step "$venv/bin/python" -m pytest crates/py/weida-py/tests ;;
        *) step "$venv/bin/python" -m pytest crates/py/weida-py/tests/unit ;;
    esac
done
