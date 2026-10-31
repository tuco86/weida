"""The unit tier: no sockets, and one second per test."""

import pathlib

import pytest
import pytest_socket

HERE = pathlib.Path(__file__).parent


def pytest_collection_modifyitems(config, items):
    for item in items:
        if HERE in pathlib.Path(str(item.fspath)).parents:
            item.add_marker(pytest.mark.timeout(1))


@pytest.fixture(autouse=True)
def _no_sockets():
    pytest_socket.disable_socket()
    yield
    pytest_socket.enable_socket()
