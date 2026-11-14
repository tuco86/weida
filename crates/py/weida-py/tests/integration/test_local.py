"""Unix and named-pipe bindings, with the peer principal the kernel proved."""

import asyncio
import dataclasses
import os
import shutil
import sys
import tempfile
import urllib.parse
import uuid

import pytest

import weida
import weida.asyncio as wa


async def round_trip(requester, replier, url: str) -> weida.IncomingMeta:
    """Dials `url` and answers one request: the metadata the replier saw."""
    await requester.connect(url)

    async def serve() -> weida.IncomingMeta:
        request = await replier.accept()
        meta = request.meta()
        out = await request.reply()
        await out.write_all(b"hi")
        await out.finish().delivered()
        return meta

    served = asyncio.ensure_future(serve())
    reply = await requester.request(b"hello")
    assert (await reply.collect(64)).payload == b"hi"
    return await served


@pytest.mark.skipif(sys.platform == "win32", reason="AF_UNIX bindings exist on Unix only")
def test_a_round_trip_over_a_unix_socket_names_the_clients_uid():
    # Not pytest's tmp_path: its long path can pass macOS's 104-byte sun_path.
    directory = tempfile.mkdtemp(prefix="weida-py-")
    socket = os.path.join(directory, "s.sock")

    async def main() -> None:
        runtime = wa.Runtime.owned()
        listener = runtime.listener()
        binding = listener.bind_unix(socket)
        assert binding.path() == socket
        replier = listener.replier("/echo")
        requester = runtime.requester(weida.ClientTls(weida.Trust()))

        url = f"weida+unix://{urllib.parse.quote(socket, safe='')}/echo"
        meta = await round_trip(requester, replier, url)
        peer = meta.peer_local
        assert isinstance(peer, weida.LocalPrincipal)
        assert peer.uid == os.geteuid()
        assert isinstance(peer.gid, int)
        # Linux reports the pid; macOS may not once the peer closed.
        assert peer.pid in (None, os.getpid())
        with pytest.raises(dataclasses.FrozenInstanceError):
            peer.uid = 0
        assert meta.peer_fingerprint is None
        with pytest.raises(weida.Unsupported):
            listener.bind_pipe("x")

        binding.drop()
        assert not os.path.exists(socket), "the drop removes the socket file"
        for handle in (requester, replier, listener):
            handle.drop()
        await runtime.shutdown()

    try:
        asyncio.run(main())
    finally:
        shutil.rmtree(directory, ignore_errors=True)


@pytest.mark.skipif(sys.platform != "win32", reason="named pipes exist on Windows only")
def test_a_round_trip_over_a_named_pipe_names_the_clients_sid():
    name = f"weida-py-{uuid.uuid4()}"

    async def main() -> None:
        runtime = wa.Runtime.owned()
        listener = runtime.listener()
        binding = listener.bind_pipe(name)
        assert binding.name() == name
        replier = listener.replier("/echo")
        requester = runtime.requester(weida.ClientTls(weida.Trust()))

        meta = await round_trip(requester, replier, f"weida+pipe://{name}/echo")
        peer = meta.peer_local
        assert isinstance(peer, weida.WindowsPrincipal)
        assert peer.sid.startswith("S-1-"), peer.sid
        assert peer.pid == os.getpid()
        assert meta.peer_fingerprint is None
        with pytest.raises(weida.Unsupported):
            listener.bind_unix("x")

        for handle in (requester, replier, binding, listener):
            handle.drop()
        await runtime.shutdown()

    asyncio.run(main())
