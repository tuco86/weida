"""Shared fixtures: the broker address and how a test skips without one.

Every test in this directory that needs a broker asks for the `broker`
fixture, which probes the port once per session and skips with the command
that starts one. That is the Python half of what `#[ignore]` does for the Rust
interop suites (docs/LOOP.md section 2): the file is always collected, it runs
where a broker answers, and it never hangs waiting for one.
"""

import socket

import pytest

import waits

# Where `crates/mqtt/weida-mqtt/tests/interop/rumqttd.toml` listens. Not 1883:
# that is where a developer's own broker lives, and a test that silently
# measured against *that* would be the worst possible outcome.
BROKER = "127.0.0.1:21884"

START_IT = (
    "no MQTT broker on " + BROKER + ". Start one with:\n"
    "  cargo install rumqttd --version 0.20.0\n"
    "  rumqttd -c crates/mqtt/weida-mqtt/tests/interop/rumqttd.toml -q"
)


def _answers(address, timeout=0.3):
    host, port = address.rsplit(":", 1)
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def broker():
    """The broker's address, or a skip naming the command that starts one."""
    if not _answers(BROKER):
        pytest.skip(START_IT)
    return BROKER


@pytest.fixture
def run():
    """Runs a coroutine to completion, reporting while it takes long.

    There is no time limit on a real clock: a slow run is still a run, and
    `waits.run` says on stderr what it is still waiting for. The hang guard is
    the CI job's own budget. `asyncio.run` per test rather than one loop for
    the file: each test gets a fresh loop, and a context that outlived its
    loop is a bug this shape cannot have.
    """
    return waits.run


@pytest.fixture
def client_id():
    """A Client Identifier no other run has used.

    Sessions outlive a test run by design — that is what Session Expiry
    Interval is for — so a fixed identifier makes the *second* run of a session
    test start against state the first one left, and [MQTT-3.2.2-4] then
    obliges the client to close. A fresh identifier per test is what keeps each
    test measuring the thing it is about.
    """
    import time

    return f"w3py-{time.monotonic_ns():x}"
