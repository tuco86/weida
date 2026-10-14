"""Test configuration: the peer module on the path, and asyncio mode.

`broker.py` sits beside the tests rather than in a package, because it is test
scaffolding and not something to import from anywhere else. pytest adds a
test file's own directory to `sys.path` only for rootdir-relative layouts, so
this does it explicitly.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def pytest_configure(config):
    # Every coroutine test in this directory is asyncio; saying so once beats
    # a decorator on each.
    config.addinivalue_line("markers", "asyncio: an asyncio test")
