"""`weida.sync`: the same patterns with no event loop anywhere.

No `asyncio.run` in this file, which is the point. The endpoints run on
threads, because that is how a synchronous messaging program is written — and
the GIL is released while a call waits, so the thread that serves and the
thread that requests make progress at the same time.
"""

import os
import signal
import subprocess
import sys
import threading
import time

import pytest

import weida
from weida import sync

CAP = 1 << 20


def test_a_request_is_answered_with_no_event_loop():
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    replier = binding.replier("/echo")

    def serve():
        request = replier.accept(CAP)
        assert request.payload == b"ping"
        assert request.meta.endpoint == "/echo"
        request.reply(b"pong")

    answering = threading.Thread(target=serve, daemon=True)
    answering.start()

    client = sync.Runtime()
    requester = client.requester(weida.Trust.by_address())
    requester.connect(binding.url("/echo"))
    assert requester.request(b"ping", CAP) == b"pong"

    answering.join()
    assert not answering.is_alive(), "the serving thread must have finished"
    client.shutdown()
    server.shutdown()


def test_a_push_and_a_publish_reach_their_endpoints():
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    puller = binding.puller("/ingest")
    publisher = binding.publisher("/md")

    client = sync.Runtime()
    pusher = client.pusher(weida.Trust.by_address())
    pusher.connect(binding.url("/ingest"))
    subscriber = client.subscriber(weida.Trust.by_address())
    subscriber.connect(binding.url("/md"))
    subscriber.subscribe("px.#")

    received = []
    drain = threading.Thread(target=lambda: received.append(puller.recv(CAP)), daemon=True)
    drain.start()
    pusher.send(b"a sample")
    drain.join()
    assert received and received[0][0] == b"a sample"
    assert received[0][1].endpoint == "/ingest"

    # The publisher's counter is how a synchronous caller waits for a
    # subscription without a probe message or a sleep.
    while publisher.subscriber_count() != 1:
        time.sleep(0.005)
    got = []
    reading = threading.Thread(target=lambda: got.append(subscriber.recv(CAP)), daemon=True)
    reading.start()
    while publisher.publish("px.eur", b"1.0812") != 1:
        time.sleep(0.005)
    reading.join()
    assert got and got[0][0] == b"1.0812"
    assert got[0][1].topic == "px.eur"

    client.shutdown()
    server.shutdown()


def test_the_failure_classes_are_the_same_on_both_surfaces():
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    _replier = binding.replier("/echo")

    client = sync.Runtime()
    requester = client.requester(weida.Trust.by_address())
    wrong = "weida://sha256:%s@%s/echo" % ("0" * 64, binding.local_addr())
    with pytest.raises(weida.Untrusted) as refused:
        requester.connect(wrong)
    assert binding.fingerprint() in refused.value.cause

    other = client.requester(weida.Trust.by_address())
    other.connect(binding.url("/nowhere"))
    with pytest.raises(weida.UnknownEndpoint):
        other.request(b"ping", CAP)

    client.shutdown()
    server.shutdown()


def test_a_runtime_is_shut_down_once():
    runtime = sync.Runtime()
    delivered, outstanding = runtime.drain(1.0)
    assert (delivered, outstanding) == (0, 0)
    # The facade's runtime is consumed by a drain, so the second call is a
    # refusal rather than a second drain.
    with pytest.raises(weida.RuntimeFailure):
        runtime.drain(1.0)
    with pytest.raises(weida.RuntimeFailure):
        runtime.requester(weida.Trust.by_address())
    # The one renamed class, and the rename is visible rather than implied:
    # `weida.Runtime` is the runtime, so the failure is `RuntimeFailure` and
    # says so in its own errno.
    assert not issubclass(weida.Runtime, BaseException)


def test_the_submodule_is_importable_and_shares_its_values():
    import weida.sync as imported

    assert imported is sync
    for name in imported.__all__:
        assert hasattr(imported, name), name
    # The value classes are the asyncio surface's, not copies: one Trust, one
    # Identity, one IncomingMeta for the whole module.
    assert weida.Trust.by_address() is not None
    runtime = sync.Runtime()
    binding = runtime.bind("127.0.0.1:0", weida.Identity.generate())
    assert binding.url("/x").startswith("weida://sha256:")
    runtime.shutdown()


def test_a_pair_carries_both_directions_with_no_event_loop():
    """PAIR on threads, and its one rule: the first peer is the one kept."""
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    bound = binding.pair("/link")
    url = binding.url("/link")

    client = sync.Runtime()
    paired = client.pair(weida.Trust.by_address())
    paired.connect(url)
    assert paired.peer_count() == 1

    heard = []
    # A pair is symmetric, so both ends block on `recv`: the bound half
    # answers from a thread of its own.
    def answer():
        heard.append(bound.recv(CAP))
        bound.send(b"pong")

    answering = threading.Thread(target=answer, daemon=True)
    answering.start()
    paired.send(b"ping")
    assert paired.recv(CAP)[0] == b"pong"
    answering.join()
    assert heard and heard[0][0] == b"ping"

    # A second dialler is refused on the transfer, not on the connection,
    # past the 1 MiB stream window (docs/decisions/0005-refusal-race.md).
    newcomer = sync.Runtime()
    second = newcomer.pair(weida.Trust.by_address())
    second.connect(url)
    with pytest.raises(weida.LimitExceeded):
        second.send(b"z" * (2 << 20))

    # The first peer still delivers.
    kept = []
    reading = threading.Thread(target=lambda: kept.append(bound.recv(CAP)), daemon=True)
    reading.start()
    paired.send(b"still mine")
    reading.join()
    assert kept and kept[0][0] == b"still mine"

    newcomer.shutdown()
    client.shutdown()
    server.shutdown()


def test_a_survey_is_a_value_with_no_event_loop():
    """SURVEY on threads: the deadline is the caller's, silence is a number."""
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    answering = binding.respondent("/poll")
    quiet = binding.respondent("/quiet")

    def answer():
        question = answering.accept(CAP)
        assert question.payload == b"who is there"
        question.reply(b"me")

    held = []

    def say_nothing():
        # Accepted and never answered: silence, not a failure. The request is
        # held, because dropping it would report NO_REPLY.
        held.append(quiet.accept(CAP))

    threads = [
        threading.Thread(target=answer, daemon=True),
        threading.Thread(target=say_nothing, daemon=True),
    ]
    for thread in threads:
        thread.start()

    client = sync.Runtime()
    surveyor = client.surveyor(weida.Trust.by_address())
    surveyor.connect(binding.url("/poll"))
    surveyor.connect(binding.url("/quiet"))
    assert surveyor.peer_count() == 2

    survey = surveyor.survey(b"who is there", 0.5, CAP)
    assert survey.asked == 2, repr(survey)
    assert survey.replies == [b"me"], repr(survey)
    assert survey.failed == 0, repr(survey)
    assert survey.silent() == 1, repr(survey)

    for thread in threads:
        thread.join()
    client.shutdown()
    server.shutdown()


def test_a_bus_never_delivers_a_member_its_own_message():
    """BUS on threads, and its one rule: never your own message."""
    first = sync.Runtime()
    first_binding = first.bind("127.0.0.1:0", weida.Identity.generate())
    second = sync.Runtime()
    second_binding = second.bind("127.0.0.1:0", weida.Identity.generate())

    one = first_binding.bus("/bus1", weida.Trust.by_address())
    two = second_binding.bus("/bus2", weida.Trust.by_address())
    one.connect(second_binding.url("/bus2"))
    two.connect(first_binding.url("/bus1"))
    assert one.peer_count() == 1

    heard = []
    listening = threading.Thread(target=lambda: heard.append(two.recv(CAP)), daemon=True)
    listening.start()
    assert one.send(b"hello all") == 1, "one other member"
    listening.join()
    assert heard and heard[0][0] == b"hello all"
    assert heard[0][1].endpoint == "/bus2"

    # The sender never hears itself: with that message delivered, a further
    # send leaves nothing for `one` to receive and everything for `two`.
    got = []
    reading = threading.Thread(target=lambda: got.append(two.recv(CAP)), daemon=True)
    reading.start()
    assert one.send(b"mine alone") == 1
    reading.join()
    assert got and got[0][0] == b"mine alone"
    assert one.dropped() == 0

    first.shutdown()
    second.shutdown()


def test_a_radio_segment_and_datagram_reach_a_dish():
    """RADIO/DISH with no event loop: one stream segment, one datagram."""
    server = sync.Runtime(datagrams=True)
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    radio = binding.radio("/r")
    client = sync.Runtime(datagrams=True)
    dish = client.dish(weida.Trust.by_address())
    dish.join("v")
    dish.connect(binding.url("/r"))
    while radio.dish_count() != 1:
        time.sleep(0.005)

    segment = radio.segment("v")
    assert segment.write(b"key") == 1
    assert segment.write(b"frame") == 1
    assert segment.finish() == 1
    kind, payload, meta = dish.recv(CAP)
    assert (kind, payload) == ("segment", b"keyframe")
    assert (meta.topic, meta.segment) == ("v", 0)

    assert radio.datagram("v", b"voice") == 1
    assert dish.recv(CAP) == ("datagram", "v", 1, b"voice")
    client.shutdown()
    server.shutdown()


def test_a_layered_segment_reaches_a_dish_layer_by_layer():
    """Layered segments with no event loop: each layer is its own arrival."""
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    radio = binding.radio("/r")
    client = sync.Runtime()
    dish = client.dish(weida.Trust.by_address())
    dish.join("v", max_layer=1)
    dish.connect(binding.url("/r"))
    while radio.dish_count() != 1:
        time.sleep(0.005)

    seg = radio.segment("v", max_age=5.0, priority=3)
    assert seg.write_layer(0, b"base") == 1
    assert seg.write_layer(1, b"more") == 1
    assert seg.write_layer(2, b"capped away") == 0
    assert seg.finish() == 1
    heard = set()
    for _ in range(2):
        kind, payload, meta = dish.recv(CAP)
        assert kind == "segment"
        assert meta.segment == 0
        heard.add((meta.layer, payload))
    assert heard == {(0, b"base"), (1, b"more")}

    drops = radio.dish_drops()
    assert len(drops) == 1
    assert drops[0].layers_cut == 0
    client.shutdown()
    server.shutdown()


def test_connection_stats_over_quic_and_in_process():
    """The same reading with no event loop."""
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    binding.puller("/jobs")
    client = sync.Runtime()
    pusher = client.pusher(weida.Trust.by_address())
    assert pusher.connection_stats() == []
    pusher.connect(binding.url("/jobs"))
    [stats] = pusher.connection_stats()
    assert stats.transport.path.rtt > 0
    assert stats.remote is None, "neither runtime asked for reports"

    local = sync.Runtime()
    bus = local.bind_inproc("weida-py-sync-stats")
    bus.puller("/jobs")
    dialler = local.pusher(weida.Trust.by_address())
    dialler.connect(bus.url("/jobs"))
    [record] = dialler.connection_stats()
    assert record.transport is None
    client.shutdown()
    server.shutdown()
    local.shutdown()


def test_a_bind_on_one_thread_never_freezes_a_call_on_another():
    """`bind` used to keep the runtime's lock across its released section, so
    a second thread that took the same lock with the GIL held waited for a
    thread that needed the GIL to finish: both parked, for good. The scenario
    runs in a child so that the failure is a timeout and not a hung suite."""
    script = (
        "import threading\n"
        "import weida\n"
        "from weida import sync\n"
        "runtime = sync.Runtime()\n"
        "identity = weida.Identity.generate()\n"
        "trust = weida.Trust.by_address()\n"
        "done = threading.Event()\n"
        "def bind():\n"
        "    for _ in range(150):\n"
        "        runtime.bind('127.0.0.1:0', identity)\n"
        "    done.set()\n"
        "def build():\n"
        "    while not done.is_set():\n"
        "        runtime.requester(trust)\n"
        "threads = [threading.Thread(target=bind), threading.Thread(target=build)]\n"
        "for thread in threads: thread.start()\n"
        "for thread in threads: thread.join()\n"
        "print('finished')\n"
    )
    finished = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    assert finished.returncode == 0, finished.stderr
    assert "finished" in finished.stdout


@pytest.mark.parametrize("call", ["replier.accept(1 << 20)", "puller.recv(1 << 20)"])
def test_ctrl_c_ends_a_wait_with_no_peer_and_no_deadline(call):
    # A child parks in the call on an endpoint nobody has dialled; the parent
    # interrupts it once it says it is waiting. The wait must end with
    # KeyboardInterrupt rather than hold the process until a peer speaks.
    script = (
        "import weida\n"
        "from weida import sync\n"
        "runtime = sync.Runtime()\n"
        "binding = runtime.bind('127.0.0.1:0', weida.Identity.generate())\n"
        "replier = binding.replier('/in')\n"
        "puller = binding.puller('/pull')\n"
        "print('waiting', flush=True)\n"
        f"{call}\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "waiting"
        time.sleep(0.5)
        child.send_signal(signal.SIGINT)
        _, stderr = child.communicate()
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate()
    assert "KeyboardInterrupt" in stderr


def test_an_interrupted_accept_leaves_the_next_request_for_the_next_accept():
    """Ctrl-C ends the wait and nothing else: the exchange that arrives after
    it is the one the next `accept` gets, not one a dead wait ate."""
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    replier = binding.replier("/echo")

    def interrupt():
        time.sleep(0.4)
        os.kill(os.getpid(), signal.SIGINT)

    interrupting = threading.Thread(target=interrupt, daemon=True)
    interrupting.start()
    with pytest.raises(KeyboardInterrupt):
        replier.accept(CAP)
    interrupting.join()

    def serve():
        request = replier.accept(CAP)
        request.reply(request.payload[::-1])

    answering = threading.Thread(target=serve, daemon=True)
    answering.start()
    client = sync.Runtime()
    requester = client.requester(weida.Trust.by_address())
    requester.connect(binding.url("/echo"))
    assert requester.request(b"abc", CAP) == b"cba"
    answering.join()
    assert not answering.is_alive()
    client.shutdown()
    server.shutdown()
