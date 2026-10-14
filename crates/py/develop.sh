#!/bin/sh
# Builds a Python extension from this checkout and runs its tests.
#
# Usage: develop.sh <binding-dir> [pytest args...]
#
# Every binding's own `develop.sh` is a wrapper that calls this one with its
# directory, so there is one thing to run and one thing to remember. It:
#
#   1. creates or reuses a `uv`-managed virtualenv at the repository root,
#   2. installs `maturin` and `pytest` **into that virtualenv** - never into a
#      system interpreter, which would need root on most machines anyway -
#      plus the packages named in `WEIDA_PY_EXTRA`,
#   3. runs `maturin develop` in the binding directory, which builds the crate
#      with `pyo3/extension-module` (see its pyproject.toml) and installs it
#      into the virtualenv,
#   4. runs `WEIDA_PY_PREPARE`, if set, in the binding directory,
#   5. runs the Python tests, passing any further arguments straight to
#      pytest.
#
# Environment:
#
#   WEIDA_PY_EXTRA    space-separated extra pip packages; default none.
#   WEIDA_PY_PREPARE  a shell command run after `maturin develop` and before
#                     pytest; default none.
#   CARGO_TARGET_DIR  honoured if already set, so a worktree that builds into
#                     its own target directory keeps doing so and two cargo
#                     invocations never share a build lock.
set -eu

dir=${1:?usage: develop.sh <binding-dir> [pytest args...]}
shift

root=$(cd "$(dirname "$0")/../.." && pwd)
venv="$root/.venv"

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR

uv venv --allow-existing "$venv"
# shellcheck disable=SC2086 # the extra packages are a space-separated list
uv pip install --python "$venv" maturin pytest ${WEIDA_PY_EXTRA:-}

cd "$dir"
VIRTUAL_ENV="$venv" "$venv/bin/maturin" develop
if [ -n "${WEIDA_PY_PREPARE:-}" ]; then
    sh -c "$WEIDA_PY_PREPARE"
fi
"$venv/bin/python" -m pytest tests "$@"
