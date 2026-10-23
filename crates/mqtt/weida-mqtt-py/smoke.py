#!/usr/bin/env python3
"""The whole binding in one file, against nothing but itself.

What it proves without a broker: the module imports, every class is there,
every reason-code class exists and carries its byte, the topic matcher is
MQTT's, and a connect to a closed port fails with a **named** class rather
than a hang. `package.sh` runs exactly this from a wheel with no Rust
toolchain on `PATH`, which is what makes "the wheel works" a fact.

The round trip through a broker is `tests/test_interop.py`, which needs one.
"""

import asyncio
import socket

import weida_mqtt


def unused_port():
    """A port nothing is listening on, for the refusal below."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def the_sync_surface_is_there():
    """`weida_mqtt.sync`, importable under both spellings.

    A PyO3 submodule is not in `sys.modules` unless the binding puts it
    there, so `from weida_mqtt import sync` would work and
    `import weida_mqtt.sync` would not — which is the kind of asymmetry a
    smoke test exists to catch.
    """
    from weida_mqtt import sync

    import importlib

    also = importlib.import_module("weida_mqtt.sync")
    assert also is sync
    for name in ("Context", "Client", "Deliveries"):
        assert hasattr(sync, name), name
    # A context with no loop in the process, which is the whole claim.
    sync.Context(worker_threads=1)
    print("the synchronous surface is there and needs no loop")


def the_module_has_its_surface():
    for name in (
        "Context",
        "Client",
        "Events",
        "Session",
        "ConnectOptions",
        "Will",
        "Message",
        "Delivery",
        "Subscription",
        "Completion",
        "MqttError",
    ):
        assert hasattr(weida_mqtt, name), name
    assert weida_mqtt.NORMAL_DISCONNECTION == 0x00
    assert weida_mqtt.DISCONNECT_WITH_WILL_MESSAGE == 0x04
    assert weida_mqtt.DEFAULT_RECEIVE_MAXIMUM == 65535
    assert weida_mqtt.SHARE_PREFIX == "$share/"
    print("the surface is there")


def the_reason_codes_are_classes_carrying_their_byte():
    # A sample across the groups, each its own class under one base.
    for name in (
        "NotAuthorized",
        "RetainNotSupported",
        "QosNotSupported",
        "SharedSubscriptionsNotSupported",
        "TopicAliasInvalid",
        "SessionTakenOver",
        "KeepAliveTimeout",
    ):
        klass = getattr(weida_mqtt, name)
        assert issubclass(klass, weida_mqtt.MqttError), name
    # And the failures MQTT gives no code, which are classes too.
    for name in ("NotConnected", "ConnectionClosed", "Timeout", "InvalidTopic"):
        assert issubclass(getattr(weida_mqtt, name), weida_mqtt.MqttError), name
    print("every reason code is its own class")


def the_matcher_is_mqtts():
    # The specification's own worked examples, which are the cases a plausible
    # matcher gets wrong in the opposite direction.
    assert weida_mqtt.matches("sport/tennis/player1/#", "sport/tennis/player1")
    assert weida_mqtt.matches("sport/#", "sport")
    assert weida_mqtt.matches("sport/+", "sport/")
    assert not weida_mqtt.matches("sport/+", "sport")
    assert weida_mqtt.matches("+/+", "/finance")
    assert not weida_mqtt.matches("+", "/finance")
    # [MQTT-4.7.2-1]: a leading wildcard never reaches a `$` topic.
    assert not weida_mqtt.matches("#", "$SYS/broker/uptime")
    assert weida_mqtt.matches("$SYS/#", "$SYS/broker/uptime")
    # And the grammar's refusals.
    for illegal in ("sport/tennis#", "sport/#/ranking", "sp+rt", ""):
        try:
            weida_mqtt.check_topic_filter(illegal)
        except ValueError:
            pass
        else:
            raise AssertionError(f"{illegal!r} should be refused")
    weida_mqtt.check_topic_filter("sport/+/player1")
    print("the matcher and the grammar are MQTT's")


def the_values_refuse_what_the_protocol_forbids():
    # Retain Handling 3 "is a Protocol Error to send" (3.8.3.1), so it is not
    # a value this binding has.
    try:
        weida_mqtt.Subscription("a/+", 1, retain_handling=3)
    except ValueError:
        pass
    else:
        raise AssertionError("Retain Handling 3 should be refused")
    # QoS 3 does not exist.
    try:
        weida_mqtt.Message("a", b"x", qos=3)
    except ValueError:
        pass
    else:
        raise AssertionError("QoS 3 should be refused")
    # The delete is RETAIN 1 and an empty payload.
    delete = weida_mqtt.Message.delete_retained("a/b", qos=1)
    assert delete.retain and delete.payload == b""
    print("the values refuse what the protocol forbids")


def a_connect_to_nothing_is_named_and_not_a_hang():
    async def run():
        context = weida_mqtt.Context(worker_threads=1)
        port = unused_port()
        options = weida_mqtt.ConnectOptions("smoke", connect_timeout=2.0)
        try:
            await context.connect(f"127.0.0.1:{port}", options)
        except weida_mqtt.MqttError as failed:
            # `Io` for a refused connection, `Timeout` where nothing answers:
            # either is a name, which is the claim.
            assert failed.errno in ("Io", "Timeout", "Runtime"), failed.errno
            assert failed.cause
            return failed.errno
        raise AssertionError("a connect to a closed port should fail")

    # No outer deadline: `connect_timeout` above is the expected end of a
    # connect that nothing answers, and the CI job's budget is the hang guard.
    errno = asyncio.run(run())
    print(f"a connect to nothing raised {errno} rather than hanging")


if __name__ == "__main__":
    the_module_has_its_surface()
    the_sync_surface_is_there()
    the_reason_codes_are_classes_carrying_their_byte()
    the_matcher_is_mqtts()
    the_values_refuse_what_the_protocol_forbids()
    a_connect_to_nothing_is_named_and_not_a_hang()
    print("smoke: ok")
