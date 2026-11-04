#!/bin/sh
# /// ci
# needs = ["c-abi-linux-amd64"]
# platform = "linux-amd64"
# image = "python"
# timeout_minutes = 30
# ///
# weida's Python package (docs/decisions/0046): the job takes the wheel
# c-abi-linux-amd64 handed on and retags it manylinux_2_<glibc> with
# `auditwheel repair`. The glibc floor is that of trixie, where libweida is
# built, and libweida is the only native file, so nothing is grafted. The job
# then runs python_check of build.sh on CPython 3.14, free-threaded CPython
# 3.14t and PyPy 3.11 and hands the manylinux wheel on in wheels/.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

export UV_CACHE_DIR="$CI_WORKSPACE/.ci-cache/uv"
mkdir -p "$CI_OUTPUT/wheels"
step uvx --from auditwheel==6.8.2 auditwheel repair --patcher none -w "$CI_OUTPUT/wheels" "$(ls "$(libweida_input)"/wheels/weida-*.whl)"
wheel=$(ls "$CI_OUTPUT"/wheels/weida-*.whl)
case $wheel in
    *-py3-none-manylinux_2_*_x86_64.whl) ;;
    *) echo "the wheel is not py3-none-manylinux_2_*_x86_64: $wheel" >&2; exit 1 ;;
esac
step uvx --from auditwheel==6.8.2 auditwheel show "$wheel"
python_check "$wheel" cpython:cpython-3.14.7 cpython-ft:cpython-3.14.7+freethreaded pypy:pypy-3.11.15
