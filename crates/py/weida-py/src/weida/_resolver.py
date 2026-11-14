"""The resolver area (`weida_resolver`, `weida_resolved`): a callable that
answers each lookup of a runtime's dials, on a daemon thread of its own or,
for a coroutine function on a `weida.asyncio.Runtime`, on the running loop."""

import asyncio
import inspect
import threading
from dataclasses import dataclass
from typing import Callable, Optional

from weida._errors import check
from weida._native import ffi, lib
from weida._values import _text
from weida._wait import call_sync

_MALFORMED = b"the resolver did not answer a list of address strings"
_LOOP_CLOSED = b"the event loop the resolver answers on is closed"


@dataclass(frozen=True)
class Lookup:
    """One lookup of a dial: `name` the host the URL wrote, `port` its port
    or `None` when it wrote none (the answer chooses the ports), and
    `max_addresses` the most addresses the answer may hold."""

    name: str
    port: Optional[int]
    max_addresses: int


def _lookup(q) -> Lookup:
    port = ffi.new("uint16_t *")
    has_port = lib.weida_resolved_port(q, port)
    return Lookup(
        _text(lib.weida_resolved_name, q),
        port[0] if has_port else None,
        lib.weida_resolved_max_addresses(q),
    )


def _refuse(q, error: BaseException) -> None:
    """Refuses `q` with what `error` says; consumes `q`."""
    reason = str(error) or type(error).__name__
    lib.weida_resolved_refuse(q, ffi.new("char[]", reason.encode("utf-8", "replace")))


def _respond(q, answer) -> None:
    """Answers `q` with `answer`, or refuses it when that is not a list of
    `str`; consumes `q`. libweida itself refuses an answer it cannot dial
    (no address, too many, not `ip:port`), and the dial names why."""
    if not (
        isinstance(answer, list)
        and all(isinstance(a, str) and "\0" not in a for a in answer)
    ):
        lib.weida_resolved_refuse(q, _MALFORMED)
        return
    buffers = [ffi.new("char[]", a.encode("utf-8", "replace")) for a in answer]
    addrs = ffi.new("const char *[]", buffers) if buffers else ffi.NULL
    lib.weida_resolved_answer(q, addrs, len(buffers))


def _answer(q, resolve: Callable) -> None:
    """One lookup on the resolver's thread; any raise refuses it."""
    try:
        answer = resolve(_lookup(q))
    except BaseException as error:  # noqa: BLE001 - refuses the lookup
        _refuse(q, error)
        return
    _respond(q, answer)


async def _answer_async(q, resolve: Callable) -> None:
    """One lookup on the loop; a raise refuses it, and a cancellation or an
    exit propagates once it has."""
    try:
        answer = await resolve(_lookup(q))
    except Exception as error:
        _refuse(q, error)
        return
    except BaseException as error:
        _refuse(q, error)
        raise
    _respond(q, answer)


def _dispatch(q, resolve: Callable, loop: Optional[asyncio.AbstractEventLoop]) -> None:
    if loop is None:
        _answer(q, resolve)
        return
    coroutine = _answer_async(q, resolve)
    try:
        asyncio.run_coroutine_threadsafe(coroutine, loop)
    except RuntimeError:
        # The loop is closed: the coroutine never started, so `q` is unspent.
        coroutine.close()
        lib.weida_resolved_refuse(q, _LOOP_CLOSED)


def _serve(resolver, resolve: Callable, loop: Optional[asyncio.AbstractEventLoop]) -> None:
    """The thread's body: takes each lookup until no configuration or
    runtime holds the resolver, then releases it."""
    out = ffi.new("weida_resolved **")
    try:
        while True:
            call_sync(lambda ms: lib.weida_resolver_recv(resolver, ms, out), None)
            if out[0] == ffi.NULL:
                return
            _dispatch(out[0], resolve, loop)
    finally:
        lib.weida_resolver_drop(resolver)


def loop_for(resolve, on_loop: bool) -> Optional[asyncio.AbstractEventLoop]:
    """The loop a coroutine function `resolve` runs on, `None` for a plain
    callable. `on_loop` says whether the runtime class answers on a loop."""
    if not callable(resolve):
        raise TypeError("resolver is a callable (lookup) -> addresses")
    if not inspect.iscoroutinefunction(resolve):
        return None
    if not on_loop:
        raise TypeError("a coroutine function resolver needs weida.asyncio.Runtime")
    return asyncio.get_running_loop()


def new():
    """A fresh `weida_resolver *`."""
    out = ffi.new("weida_resolver **")
    check(lib.weida_resolver_new(out))
    return out[0]


def serve(resolver, resolve: Callable, loop: Optional[asyncio.AbstractEventLoop]) -> None:
    """Answers `resolver`'s lookups on a daemon thread, which owns
    `resolver` once it has started."""
    threading.Thread(
        target=_serve, args=(resolver, resolve, loop), name="weida-resolver", daemon=True
    ).start()
