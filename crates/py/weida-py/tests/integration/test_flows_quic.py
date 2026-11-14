"""Datagram flows over real QUIC on localhost."""

import weida

WAIT = 5.0
FLOWS = weida.RuntimeConfig(limits=weida.Limits(datagram_receive_bytes=weida.DEFAULT_DATAGRAM_RECEIVE_BYTES))


def test_a_flow_carries_datagrams_over_quic():
    server_id = weida.Identity.generate()
    server = weida.Runtime.owned(FLOWS)
    listener = server.listener()
    binding = listener.bind_quic("127.0.0.1:0", weida.ServerTls(server_id), WAIT)
    acceptor = listener.acceptor("/v")
    client = weida.Runtime.owned(FLOWS)
    peer = client.peer(weida.ClientTls(weida.Trust()))
    peer.connect(f"weida://{server_id.fingerprint()}@{binding.local_addr()}/v", WAIT)
    flow = peer.open_flow(weida.FlowMeta(topic="mic"), WAIT)
    incoming = acceptor.accept(WAIT).flow
    assert incoming.info().topic == "mic"

    # QUIC never retransmits a datagram, so each one is sent again until it
    # arrives; a late copy of an earlier one is skipped.
    for i in range(3):
        while True:
            flow.send(bytes([i]))
            try:
                d = incoming.recv(0.02)
            except weida.Again:
                continue
            assert d is not None, "the flow ended early"
            if d[0] < i:
                continue
            assert d == bytes([i])
            break
    # The path's limit only grows, so it is read after the flow's.
    max_payload = flow.max_payload()
    assert flow.path_stats().max_datagram_size > max_payload
    assert incoming.path_stats() is not None

    flow.drop()
    while incoming.recv(WAIT) is not None:
        pass  # late copies before the end

    for handle in (incoming, peer, acceptor, binding, listener):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)
