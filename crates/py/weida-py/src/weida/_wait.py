"""Waiting on the C ABI from Python: blocking with a timeout, and from an
asyncio loop through the readiness descriptors (docs/c/README.md R4, R11).

An *attempt* is a callable `attempt(timeout_ms) -> int` that calls one C try
or op wait with its out-parameters already bound.
"""

import asyncio
import collections
import math
import sys
import time
from typing import Callable, Deque, Optional, Tuple

from weida._errors import AGAIN, INTERRUPTED, check

Attempt = Callable[[int], int]

_WINDOWS = sys.platform == "win32"
# Windows has no EINTR, so a blocked call never lets Python run its signal
# handlers; there each attempt waits at most this long.
_SLICE_MS = 100


def call_sync(attempt: Attempt, timeout: Optional[float]) -> int:
    """Runs `attempt` until it succeeds, fails, or `timeout` seconds pass
    (`None` waits forever). An `EINTR` continues with the time that is left,
    after Python ran its signal handlers between two attempts, so Ctrl-C
    raises `KeyboardInterrupt` here and loses nothing."""
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        if deadline is None:
            ms = -1
        else:
            ms = max(0, math.ceil((deadline - time.monotonic()) * 1000))
        if _WINDOWS and (ms < 0 or ms > _SLICE_MS):
            ms = _SLICE_MS
        rc = attempt(ms)
        if rc == INTERRUPTED:
            continue
        if (
            _WINDOWS
            and rc == AGAIN
            and (deadline is None or time.monotonic() < deadline)
        ):
            continue
        return check(rc)


class Waiters:
    """The asyncio side of one readiness descriptor: one `add_reader`
    registration and a FIFO of waiting futures, each with its attempt."""

    def __init__(self, fileno: Callable[[], int]) -> None:
        self._fileno = fileno
        self._fd: Optional[int] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue: Deque[Tuple[asyncio.Future, Attempt]] = collections.deque()

    def _readable(self) -> None:
        while self._queue:
            future, attempt = self._queue[0]
            if future.done():
                self._queue.popleft()
                continue
            rc = attempt(0)
            if rc == AGAIN:
                # The descriptor stays unreadable until there is news.
                return
            self._queue.popleft()
            try:
                future.set_result(check(rc))
            except BaseException as e:  # noqa: BLE001 - handed to the waiter
                future.set_exception(e)
        self._unregister()

    def _unregister(self) -> None:
        if self._fd is not None and self._loop is not None:
            self._loop.remove_reader(self._fd)
        self._fd = None
        self._loop = None

    def close(self) -> None:
        """Before the handle is dropped: its descriptor goes away with it."""
        self._unregister()
        while self._queue:
            future, _ = self._queue.popleft()
            if not future.done():
                future.cancel()

    async def wait(self, attempt: Attempt) -> int:
        rc = attempt(0)
        if rc != AGAIN:
            return check(rc)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._queue.append((future, attempt))
        if self._fd is None:
            fd = self._fileno()
            loop.add_reader(fd, self._readable)
            self._fd, self._loop = fd, loop
        try:
            return await future
        finally:
            if not future.done() or future.cancelled():
                try:
                    self._queue.remove((future, attempt))
                except ValueError:
                    pass
                if not self._queue:
                    self._unregister()


def _proactor(loop: asyncio.AbstractEventLoop) -> bool:
    """Whether `loop` cannot watch a descriptor: `add_reader` raises
    `NotImplementedError` on the Windows Proactor loop."""
    proactor = getattr(asyncio, "ProactorEventLoop", None)
    return proactor is not None and isinstance(loop, proactor)


async def call_async(
    waiters: Waiters, attempt: Attempt, on_cancel: Callable[[], None]
) -> int:
    """Awaits `attempt` through `waiters`. Where the loop cannot watch a
    descriptor (the Windows Proactor loop), the wait runs in an executor
    thread, and a cancellation ends it through `on_cancel`, which drops the
    handle so that thread's wait returns `Closed`."""
    loop = asyncio.get_running_loop()
    if not _proactor(loop):
        return await waiters.wait(attempt)
    try:
        return await loop.run_in_executor(None, call_sync, attempt, None)
    except asyncio.CancelledError:
        on_cancel()
        raise
