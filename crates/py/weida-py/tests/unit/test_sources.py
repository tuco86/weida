"""Identity and trust sources, their events and the ALPN token, without a
socket of their own."""

import dataclasses

import pytest

import weida
from weida import IdentityEventKind, IdentitySource, Trust, TrustSource

WAIT = 5.0


def test_an_external_identity_source_reports_its_updates_as_events():
    a = weida.Identity.generate()
    b = weida.Identity.generate()
    src = IdentitySource.external(a)
    assert src.fingerprint() == a.fingerprint()
    events = src.events()

    src.update(a)
    event = events.recv(WAIT)
    with pytest.raises(dataclasses.FrozenInstanceError):
        event.kind = None
    assert event.kind is IdentityEventKind.RENEWED
    assert event.fingerprint == a.fingerprint()

    src.update(b)
    event = events.recv(WAIT)
    assert event.kind is IdentityEventKind.KEY_CHANGED
    assert event.from_ == a.fingerprint()
    assert event.to == b.fingerprint()
    assert event.fingerprint is None

    src.report_failure("denied")
    event = events.recv(WAIT)
    assert event.kind is IdentityEventKind.RENEWAL_FAILED
    assert event.message == "denied"

    src.report_handoff_stolen()
    assert events.recv(WAIT).kind is IdentityEventKind.HANDOFF_STOLEN

    assert src.current().fingerprint() == b.fingerprint()
    assert src.reload() is False
    events.drop()
    src.drop()


def test_a_weak_source_upgrades_until_the_last_strong_handle_is_gone_and_the_events_end():
    src = IdentitySource.ephemeral(["localhost"])
    weak = src.downgrade()
    strong = weak.upgrade()
    assert isinstance(strong, weida.IdentitySource)
    assert strong.fingerprint() == src.fingerprint()
    strong.drop()
    events = src.events()
    src.drop()
    assert weak.upgrade() is None
    assert list(events) == []
    weak.drop()


def test_a_trust_source_reports_a_refresh_and_is_empty_follows_it():
    assert Trust().is_empty() is True
    pinned = Trust().add_pin(weida.Identity.generate().fingerprint())
    assert pinned.is_empty() is False
    ts = TrustSource.external(pinned)
    assert ts.is_empty() is False
    events = ts.events()
    ts.update(Trust())
    assert events.recv(WAIT).kind is IdentityEventKind.TRUST_REFRESHED
    assert ts.is_empty() is True
    assert ts.current().is_empty() is True
    weak = ts.downgrade()
    strong = weak.upgrade()
    assert isinstance(strong, weida.TrustSource)
    strong.drop()
    events.drop()
    ts.drop()
    assert weak.upgrade() is None
    weak.drop()


def test_source_and_alpn_argument_errors_are_type_errors():
    id = weida.Identity.generate()
    with pytest.raises(TypeError, match="alpn is a weida.Alpn"):
        weida.ClientTls(Trust(), alpn=1)
    with pytest.raises(TypeError):
        weida.ServerTls(id, alpn=1)
    with pytest.raises(TypeError):
        weida.ClientTls(Trust(), alpn=weida.Congestion.BBR)
    with pytest.raises(TypeError):
        weida.ClientTls("x")
    with pytest.raises(TypeError):
        weida.ClientTls(Trust(), identity="x")
    with pytest.raises(TypeError):
        weida.ServerTls("x")
    with pytest.raises(TypeError):
        weida.ServerTls(id, require_client="nope")
    # Both `files` calls fail before any filesystem access.
    with pytest.raises(TypeError):
        IdentitySource.files("x", poll_ms=1)
    with pytest.raises(TypeError):
        IdentitySource.files("x", poll="1")
    with pytest.raises(TypeError):
        IdentitySource.ephemeral("x")
    with pytest.raises(TypeError):
        IdentitySource()
    with pytest.raises(TypeError):
        TrustSource()
    with pytest.raises(TypeError):
        IdentitySource.from_identity("x")
    with pytest.raises(TypeError):
        TrustSource.external("x")

    weida.ClientTls(
        TrustSource.from_trust(Trust()),
        identity=IdentitySource.from_identity(id),
        alpn=weida.Alpn.RAFT,
    ).drop()
    weida.ServerTls(
        IdentitySource.from_identity(id),
        require_client=TrustSource.from_trust(Trust()),
        alpn=weida.Alpn.WEIDA,
    ).drop()
