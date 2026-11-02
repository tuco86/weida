"""Client keys over real QUIC on localhost: required clients, the chain each
side sees, anchors and identity files."""

import os
import threading

import pytest

import weida

WAIT = 5.0


class Server:
    """A runtime with a QUIC binding on `tls` and a replier at `/echo`, and
    the requesters made on it."""

    def __init__(self, tls: weida.ServerTls) -> None:
        self.runtime = weida.Runtime.owned()
        self.listener = self.runtime.listener()
        self.binding = self.listener.bind_quic("127.0.0.1:0", tls, WAIT)
        self.replier = self.listener.replier("/echo")
        self.url = f"weida://{self.binding.local_addr()}/echo"
        self.requesters = []

    def requester(self, trust: weida.Trust, identity=None) -> weida.Requester:
        requester = self.runtime.requester(weida.ClientTls(trust, identity=identity))
        self.requesters.append(requester)
        return requester

    def close(self) -> None:
        for requester in self.requesters:
            requester.drop()
        self.replier.drop()
        self.binding.drop()
        self.listener.drop()
        self.runtime.shutdown(WAIT)


def round_trip(server: Server, requester: weida.Requester):
    """One request answered with `hi`; the metadata the server saw and the
    metadata of the reply."""
    seen = []

    def run() -> None:
        request = server.replier.accept(WAIT)
        seen.append(request.meta())
        request.take_body().collect(64, WAIT)
        reply = request.reply(None, WAIT)
        reply.write_all(b"hi", WAIT)
        reply.finish().delivered(WAIT)

    thread = threading.Thread(target=run)
    thread.start()
    requester.connect(server.url, WAIT)
    reply = requester.request(b"hello", WAIT)
    reply_meta = reply.meta()
    assert reply.collect(64, WAIT).payload == b"hi"
    thread.join(WAIT)
    return seen[0], reply_meta


def refused(server: Server, requester: weida.Requester) -> None:
    with pytest.raises(weida.Tls):
        requester.connect(server.url, WAIT)
        requester.request(b"hello", WAIT)


@pytest.fixture
def client():
    identity = weida.Identity.generate()
    yield identity
    identity.drop()


@pytest.fixture
def pinned(server_identity, client):
    """A server that requires `client`'s key, pinned."""
    tls = weida.ServerTls(
        server_identity, require_client=weida.Trust().add_pin(client.fingerprint())
    )
    server = Server(tls)
    yield server
    server.close()


def test_a_pinned_client_key_is_accepted_and_each_side_sees_the_others_chain(
    pinned, server_identity, client
):
    requester = pinned.requester(
        weida.Trust().add_pin(server_identity.fingerprint()), identity=client
    )
    seen, reply = round_trip(pinned, requester)
    assert seen.peer_fingerprint == client.fingerprint()
    assert seen.peer_chain == tuple(client.certificates())
    assert reply.peer_fingerprint == server_identity.fingerprint()
    assert reply.peer_chain == tuple(server_identity.certificates())


def test_an_anonymous_or_unknown_client_is_refused_with_tls_and_nothing_reaches_the_server(
    pinned, server_identity
):
    trust = weida.Trust().add_pin(server_identity.fingerprint())
    refused(pinned, pinned.requester(trust))
    stranger = weida.Identity.generate()
    refused(pinned, pinned.requester(trust, identity=stranger))
    with pytest.raises(weida.Again):
        pinned.replier.accept(0.2)


def test_a_binding_that_requires_clients_from_an_empty_trust_does_not_bind(server_identity):
    runtime = weida.Runtime.owned()
    listener = runtime.listener()
    tls = weida.ServerTls(server_identity, require_client=weida.Trust())
    with pytest.raises(weida.Tls) as caught:
        listener.bind_quic("127.0.0.1:0", tls, WAIT)
    assert "client trust is empty" in caught.value.detail
    listener.drop()
    runtime.shutdown(WAIT)


def test_any_key_exposes_the_fingerprint_and_the_chain(server_identity, client):
    server = Server(weida.ServerTls(server_identity, require_client=weida.ANY_KEY))
    try:
        trust = weida.Trust().add_pin(server_identity.fingerprint())
        refused(server, server.requester(trust))
        seen, _ = round_trip(server, server.requester(trust, identity=client))
        assert seen.peer_fingerprint == client.fingerprint()
        assert len(seen.peer_chain) == 1
        assert seen.peer_chain[0] == client.certificates()[0]
    finally:
        server.close()


def test_an_anchor_accepts_the_dialled_name_and_refuses_another():
    named = weida.Identity.generate_for(["127.0.0.1"])
    server = Server(weida.ServerTls(named))
    try:
        requester = server.requester(weida.Trust().add_anchor(named.certificate_pem()))
        seen, _ = round_trip(server, requester)
        assert seen.peer_fingerprint is None
        assert seen.peer_chain is None
    finally:
        server.close()

    other = weida.Identity.generate_for(["localhost"])
    server = Server(weida.ServerTls(other))
    try:
        requester = server.requester(weida.Trust().add_anchor(other.certificate_pem()))
        with pytest.raises(weida.Untrusted) as caught:
            requester.connect(server.url, WAIT)
            requester.request(b"hello", WAIT)
        assert caught.value.fingerprint == other.fingerprint()
    finally:
        server.close()


def test_a_cryptography_certificate_is_an_anchor():
    x509 = pytest.importorskip("cryptography.x509")
    named = weida.Identity.generate_for(["127.0.0.1"])
    server = Server(weida.ServerTls(named))
    try:
        anchor = x509.load_der_x509_certificate(named.certificates()[0])
        seen, _ = round_trip(server, server.requester(weida.Trust().add_anchor(anchor)))
        assert seen.peer_fingerprint is None
    finally:
        server.close()


def test_identity_and_anchor_files_read_back(tmp_path):
    generated = weida.Identity.generate_for(["127.0.0.1"])
    pem = generated.to_pem()
    fd = os.open(tmp_path / "id.pem", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as file:
        file.write(pem)
    pem[:] = bytes(len(pem))
    (tmp_path / "cert.pem").write_bytes(generated.certificate_pem())

    from_file = weida.Identity.from_pem_file(tmp_path / "id.pem")
    from_files = weida.Identity.from_pem_files(
        str(tmp_path / "cert.pem"), str(tmp_path / "id.pem")
    )
    assert from_file.fingerprint() == generated.fingerprint()
    assert from_files.fingerprint() == generated.fingerprint()

    server = Server(weida.ServerTls(from_file))
    try:
        requester = server.requester(weida.Trust().add_anchor_file(tmp_path / "cert.pem"))
        round_trip(server, requester)
    finally:
        server.close()

    missing = weida.Identity.from_pem_file(tmp_path / "missing.pem")
    with pytest.raises(weida.Tls):
        missing.fingerprint()
