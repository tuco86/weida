"""Waits that report instead of killing.

A test has no hard time limit on a real clock: a slow run is still a run, and
a limit that fires on a loaded machine turns a healthy test red (weida nightly
#81). Every wait here takes an *expected* time. Past it, and at every further
multiple of it, the wait prints what it is still waiting for and goes on
waiting. The hang guard is the CI job's own budget.

The Rust suites do the same through `weida_testkit`.
"""

import asyncio
import queue
import subprocess
import sys

EXPECTED = 10.0


def _report(what, n, expected):
    print(
        f"still waiting for {what}: past {n} x its expected {expected}s",
        file=sys.stderr,
        flush=True,
    )


async def within(what, awaitable, expected=EXPECTED):
    """Awaits `awaitable`, reporting at every multiple of `expected`."""
    task = asyncio.ensure_future(awaitable)
    n = 0
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=expected)
            if done:
                return task.result()
            n += 1
            _report(what, n, expected)
    finally:
        if not task.done():
            task.cancel()


def run(coroutine, expected=EXPECTED):
    """Runs one coroutine to completion, reporting while it takes long."""
    return asyncio.run(within("the test", coroutine, expected))


def join(thread, what, expected=EXPECTED):
    """Joins `thread`, reporting at every multiple of `expected`."""
    n = 0
    while True:
        thread.join(expected)
        if not thread.is_alive():
            return
        n += 1
        _report(what, n, expected)


def wait_set(event, what, expected=EXPECTED):
    """Waits for a `threading.Event`, reporting at every multiple of `expected`."""
    n = 0
    while not event.wait(expected):
        n += 1
        _report(what, n, expected)


def get(source, what, expected=EXPECTED):
    """Takes from a `queue.Queue`, reporting at every multiple of `expected`."""
    n = 0
    while True:
        try:
            return source.get(timeout=expected)
        except queue.Empty:
            n += 1
            _report(what, n, expected)


def communicate(process, what, expected=EXPECTED):
    """`Popen.communicate()`, reporting at every multiple of `expected`."""
    n = 0
    while True:
        try:
            return process.communicate(timeout=expected)
        except subprocess.TimeoutExpired:
            n += 1
            _report(what, n, expected)
