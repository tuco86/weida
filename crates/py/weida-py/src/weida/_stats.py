"""Connection statistics and peer events (docs/c/stats.md), copied out of
their C handles at once into frozen dataclasses. A duration the C ABI gives
in microseconds is seconds as a float here, its name without `_us`."""

from dataclasses import dataclass
from typing import Callable, Optional, Tuple

from weida._enums import GiveUp, LossCause, PeerEventKind
from weida._native import ffi, lib
from weida._values import _text


def _seconds(us: Optional[int]) -> Optional[float]:
    return None if us is None else us / 1_000_000


@dataclass(frozen=True)
class UdpCounts:
    """UDP datagrams and the bytes inside them, in one direction."""

    datagrams: int
    bytes: int


@dataclass(frozen=True)
class PathStats:
    """The path under one QUIC connection or flow. `max_datagram_size` is
    `None` when the peer accepts no datagram."""

    rtt: float
    min_rtt: float
    cwnd: int
    congestion_events: int
    lost_packets: int
    lost_bytes: int
    sent_packets: int
    current_mtu: int
    max_datagram_size: Optional[int]


@dataclass(frozen=True)
class TransportStats:
    """The transport's view of one QUIC connection."""

    path: PathStats
    tx: UdpCounts
    rx: UdpCounts


@dataclass(frozen=True)
class RemoteStats:
    """The peer's view of the path, from its latest record; `age` is how
    long ago that record arrived."""

    rtt: float
    min_rtt: float
    cwnd: int
    congestion_events: int
    lost_packets: int
    lost_bytes: int
    sent_packets: int
    current_mtu: int
    age: float
    tx: UdpCounts
    rx: UdpCounts


@dataclass(frozen=True)
class ConnectionStats:
    """One live connection of a requester, labelled by the URL it dialled.
    `transport` is `None` on a local transport; `remote` is `None` until the
    peer's first record, on a local transport, or with `path_report` off on
    either side."""

    url: str
    age: float
    redials: int
    transport: Optional[TransportStats]
    remote: Optional[RemoteStats]


def _record(s, index: int) -> ConnectionStats:
    out = ffi.new("uint64_t *")

    def number(getter: Callable) -> Optional[int]:
        return out[0] if getter(s, index, out) == 1 else None

    transport = None
    rtt = number(lib.weida_connection_stats_transport_path_rtt_us)
    if rtt is not None:
        transport = TransportStats(
            path=PathStats(
                rtt=_seconds(rtt),
                min_rtt=_seconds(number(lib.weida_connection_stats_transport_path_min_rtt_us)),
                cwnd=number(lib.weida_connection_stats_transport_path_cwnd),
                congestion_events=number(lib.weida_connection_stats_transport_path_congestion_events),
                lost_packets=number(lib.weida_connection_stats_transport_path_lost_packets),
                lost_bytes=number(lib.weida_connection_stats_transport_path_lost_bytes),
                sent_packets=number(lib.weida_connection_stats_transport_path_sent_packets),
                current_mtu=number(lib.weida_connection_stats_transport_path_current_mtu),
                max_datagram_size=number(lib.weida_connection_stats_transport_path_max_datagram_size),
            ),
            tx=UdpCounts(
                number(lib.weida_connection_stats_transport_tx_datagrams),
                number(lib.weida_connection_stats_transport_tx_bytes),
            ),
            rx=UdpCounts(
                number(lib.weida_connection_stats_transport_rx_datagrams),
                number(lib.weida_connection_stats_transport_rx_bytes),
            ),
        )

    remote = None
    rtt = number(lib.weida_connection_stats_remote_rtt_us)
    if rtt is not None:
        remote = RemoteStats(
            rtt=_seconds(rtt),
            min_rtt=_seconds(number(lib.weida_connection_stats_remote_min_rtt_us)),
            cwnd=number(lib.weida_connection_stats_remote_cwnd),
            congestion_events=number(lib.weida_connection_stats_remote_congestion_events),
            lost_packets=number(lib.weida_connection_stats_remote_lost_packets),
            lost_bytes=number(lib.weida_connection_stats_remote_lost_bytes),
            sent_packets=number(lib.weida_connection_stats_remote_sent_packets),
            current_mtu=number(lib.weida_connection_stats_remote_current_mtu),
            age=_seconds(number(lib.weida_connection_stats_remote_age_us)),
            tx=UdpCounts(
                number(lib.weida_connection_stats_remote_tx_datagrams),
                number(lib.weida_connection_stats_remote_tx_bytes),
            ),
            rx=UdpCounts(
                number(lib.weida_connection_stats_remote_rx_datagrams),
                number(lib.weida_connection_stats_remote_rx_bytes),
            ),
        )

    return ConnectionStats(
        url=_text(lambda h, length: lib.weida_connection_stats_url(h, index, length), s),
        age=_seconds(number(lib.weida_connection_stats_age_us)),
        redials=number(lib.weida_connection_stats_redials),
        transport=transport,
        remote=remote,
    )


def _take_connection_stats(ptr) -> Tuple[ConnectionStats, ...]:
    """Copies every record of a `weida_connection_stats` out, in dial
    order, and drops it."""
    try:
        return tuple(_record(ptr, i) for i in range(lib.weida_connection_stats_len(ptr)))
    finally:
        lib.weida_connection_stats_drop(ptr)


def _take_path_stats(ptr) -> PathStats:
    """Copies a `weida_path_stats` snapshot out and drops it."""
    try:
        size = ffi.new("uint64_t *")
        return PathStats(
            rtt=_seconds(lib.weida_path_stats_rtt_us(ptr)),
            min_rtt=_seconds(lib.weida_path_stats_min_rtt_us(ptr)),
            cwnd=lib.weida_path_stats_cwnd(ptr),
            congestion_events=lib.weida_path_stats_congestion_events(ptr),
            lost_packets=lib.weida_path_stats_lost_packets(ptr),
            lost_bytes=lib.weida_path_stats_lost_bytes(ptr),
            sent_packets=lib.weida_path_stats_sent_packets(ptr),
            current_mtu=lib.weida_path_stats_current_mtu(ptr),
            max_datagram_size=size[0] if lib.weida_path_stats_max_datagram_size(ptr, size) == 1 else None,
        )
    finally:
        lib.weida_path_stats_drop(ptr)


@dataclass(frozen=True)
class PeerEvent:
    """One transition of one dialled address; a field another kind does not
    carry is `None`. `url` is `None` for `MISSED`."""

    kind: PeerEventKind
    url: Optional[str]
    peer_fingerprint: Optional[str]
    cause: Optional[LossCause]
    attempt: Optional[int]
    delay: Optional[float]
    why: Optional[GiveUp]
    why_attempts: Optional[int]
    why_presented: Optional[str]
    why_message: Optional[str]
    missed: Optional[int]

    @classmethod
    def _take(cls, ptr) -> "PeerEvent":
        """Copies a `weida_peer_event` out and drops it."""
        try:
            code = ffi.new("int *")
            small = ffi.new("uint32_t *")
            big = ffi.new("uint64_t *")

            def value(getter: Callable, out):
                return out[0] if getter(ptr, out) == 1 else None

            cause = value(lib.weida_peer_event_cause, code)
            why = value(lib.weida_peer_event_why, code)
            return cls(
                kind=PeerEventKind(lib.weida_peer_event_kind(ptr)),
                url=_text(lib.weida_peer_event_url, ptr),
                peer_fingerprint=_text(lib.weida_peer_event_peer_fingerprint, ptr),
                cause=None if cause is None else LossCause(cause),
                attempt=value(lib.weida_peer_event_attempt, small),
                delay=_seconds(value(lib.weida_peer_event_delay_us, big)),
                why=None if why is None else GiveUp(why),
                why_attempts=value(lib.weida_peer_event_why_attempts, small),
                why_presented=_text(lib.weida_peer_event_why_presented, ptr),
                why_message=_text(lib.weida_peer_event_why_message, ptr),
                missed=value(lib.weida_peer_event_missed, big),
            )
        finally:
            lib.weida_peer_event_drop(ptr)
