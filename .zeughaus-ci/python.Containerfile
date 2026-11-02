# The image the python-linux-* jobs run in: weida's Python package
# (crates/py/weida-py) tested on CPython 3.14, free-threaded CPython 3.14t
# and PyPy 3.11, from the wheel the c-abi-linux-* job handed on.
# - build-essential: the C compiler build.sh's smoke_libweida needs, and pip
#   builds cffi with it on 3.14t when no wheel matches.
# - libffi-dev: cffi's source build links libffi.
# - git: wipes the workspace on the scheduled nightly (build.sh).
# - uv: installs the three interpreters (managed builds, so no system Python
#   decides which versions exist) and creates the jobs' virtualenvs.
# The same file serves amd64 under podman and arm64 under Apple `container`.
FROM docker.io/library/debian:trixie-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates build-essential libffi-dev git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.2 /uv /uvx /usr/local/bin/

ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python UV_PYTHON_PREFERENCE=only-managed

RUN uv python install cpython-3.14.7 cpython-3.14.7+freethreaded pypy-3.11.15
