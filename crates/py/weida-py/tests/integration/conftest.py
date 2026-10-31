"""The integration tier: real sockets on localhost, ten seconds per test."""

import itertools
import pathlib

import pytest

import weida

HERE = pathlib.Path(__file__).parent
_BUSES = itertools.count()


def pytest_collection_modifyitems(config, items):
    for item in items:
        if HERE in pathlib.Path(str(item.fspath)).parents:
            item.add_marker(pytest.mark.timeout(10))


@pytest.fixture
def bus() -> str:
    """An in-process bus name no other test uses."""
    return f"py-test-{next(_BUSES)}"


@pytest.fixture
def server_identity():
    identity = weida.Identity.generate()
    yield identity
    identity.drop()
