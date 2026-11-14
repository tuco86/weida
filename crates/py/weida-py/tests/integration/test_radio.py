"""A radio's eviction by key over real QUIC on localhost."""

import time

import pytest

import weida

WAIT = 5.0


def test_a_radio_evicts_a_dish_by_its_key():
    server_id = weida.Identity.generate()
    client_id = weida.Identity.generate()
    server = weida.Runtime.owned()
    listener = server.listener()
    binding = listener.bind_quic("127.0.0.1:0", weida.ServerTls(server_id, weida.ANY_KEY), WAIT)
    radio = listener.radio("/r")
    client = weida.Runtime.owned()
    dish = client.dish(weida.ClientTls(weida.Trust(), client_id))
    dish.join("v", WAIT)
    dish.connect(f"weida://{server_id.fingerprint()}@{binding.local_addr()}/r", WAIT)
    while radio.dish_count() != 1:
        time.sleep(0.001)
    assert radio.dish_drops()[0].peer_fingerprint == client_id.fingerprint()

    assert radio.evict(weida.Identity.generate().fingerprint(), "v") == 0
    with pytest.raises(weida.InvalidFingerprint):
        radio.evict("nope", "v")
    assert radio.evict(client_id.fingerprint(), "v") == 1
    segment = radio.segment("v")
    assert segment.write(b"x") == 0, "the evicted dish gets no copy"
    assert segment.finish() == 0

    for handle in (dish, radio, binding, listener):
        handle.drop()
    client.shutdown(WAIT)
    server.shutdown(WAIT)
