#!/bin/sh
# Installs this package editable into the repository's virtualenv, which
# builds libweida and the cffi module, and runs its tests.
#
# Usage: develop.sh [pytest args...]   (default: every test under tests/)
#
# CARGO_TARGET_DIR is honoured, so a worktree that builds into its own
# target directory keeps doing so.
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$dir/../../.." && pwd)
venv="$root/.venv"

CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_TARGET_DIR

uv venv --allow-existing "$venv"
uv pip install --python "$venv" cffi hatchling pytest pytest-timeout pytest-socket
uv pip install --python "$venv" --no-build-isolation -e "$dir"

cd "$dir"
if [ $# -eq 0 ]; then
    set -- tests
fi
"$venv/bin/python" -m pytest "$@"
