"""The enumerations of the C ABI (docs/c/README.md R7) as IntEnums named after weida's Rust types; their values are weida.h's constants."""

import enum

from weida._native import lib


class Congestion(enum.IntEnum):
    """`Congestion`: the QUIC congestion controller."""

    CUBIC = lib.WEIDA_CONGESTION_CUBIC
    NEW_RENO = lib.WEIDA_CONGESTION_NEW_RENO
    BBR = lib.WEIDA_CONGESTION_BBR


class Discovery(enum.IntEnum):
    """`Discovery`: how a requester treats the addresses a name resolves to."""

    AWARE = lib.WEIDA_DISCOVERY_AWARE
    SINGLE = lib.WEIDA_DISCOVERY_SINGLE


class OutboxFull(enum.IntEnum):
    """`OutboxFull`: what a send does when the outbox is full."""

    BLOCK = lib.WEIDA_OUTBOX_FULL_BLOCK
    DROP = lib.WEIDA_OUTBOX_FULL_DROP
    REJECT = lib.WEIDA_OUTBOX_FULL_REJECT


class DeliveryLevel(enum.IntEnum):
    """`DeliveryLevel`: the delivery guarantee."""

    BEST_EFFORT = lib.WEIDA_DELIVERY_LEVEL_BEST_EFFORT
    AT_MOST_ONCE = lib.WEIDA_DELIVERY_LEVEL_AT_MOST_ONCE
    AT_LEAST_ONCE = lib.WEIDA_DELIVERY_LEVEL_AT_LEAST_ONCE


class Acknowledgement(enum.IntEnum):
    """`Acknowledgement`: how far a message got before it is acknowledged."""

    NONE = lib.WEIDA_ACKNOWLEDGEMENT_NONE
    TRANSPORT_RECEIPT = lib.WEIDA_ACKNOWLEDGEMENT_TRANSPORT_RECEIPT
    ACCEPTED = lib.WEIDA_ACKNOWLEDGEMENT_ACCEPTED
    STORED = lib.WEIDA_ACKNOWLEDGEMENT_STORED
    REPLICATED = lib.WEIDA_ACKNOWLEDGEMENT_REPLICATED
    PROCESSED = lib.WEIDA_ACKNOWLEDGEMENT_PROCESSED


class Durability(enum.IntEnum):
    """`Durability`: what a stored message survives."""

    WRITTEN = lib.WEIDA_DURABILITY_WRITTEN
    FLUSHED = lib.WEIDA_DURABILITY_FLUSHED


class OrderingMode(enum.IntEnum):
    """`OrderingMode`: the ordering guarantee."""

    NONE = lib.WEIDA_ORDERING_MODE_NONE
    PER_PRODUCER_DETECT = lib.WEIDA_ORDERING_MODE_PER_PRODUCER_DETECT
    PER_PRODUCER_REASSEMBLE = lib.WEIDA_ORDERING_MODE_PER_PRODUCER_REASSEMBLE
    PER_KEY = lib.WEIDA_ORDERING_MODE_PER_KEY
    TOTAL = lib.WEIDA_ORDERING_MODE_TOTAL


class Deduplication(enum.IntEnum):
    """`Deduplication`: the deduplication guarantee."""

    NONE = lib.WEIDA_DEDUPLICATION_NONE
    BOUNDED = lib.WEIDA_DEDUPLICATION_BOUNDED
    DURABLE = lib.WEIDA_DEDUPLICATION_DURABLE


class Backpressure(enum.IntEnum):
    """`Backpressure`: what a full receiver does."""

    BLOCK = lib.WEIDA_BACKPRESSURE_BLOCK
    REJECT = lib.WEIDA_BACKPRESSURE_REJECT
    DROP = lib.WEIDA_BACKPRESSURE_DROP
    SPILL = lib.WEIDA_BACKPRESSURE_SPILL
    COALESCE = lib.WEIDA_BACKPRESSURE_COALESCE


class ProducerNaming(enum.IntEnum):
    """`ProducerNaming`: how a producer is named for ordering and deduplication."""

    FINGERPRINT = lib.WEIDA_PRODUCER_NAMING_FINGERPRINT
    STABLE = lib.WEIDA_PRODUCER_NAMING_STABLE


class ErrorCode(enum.IntEnum):
    """`ErrorCode`: the code a refusal carries to the requester
    (`IncomingRequest.refuse`)."""

    UNKNOWN_ENDPOINT = lib.WEIDA_ERROR_CODE_UNKNOWN_ENDPOINT
    REJECTED = lib.WEIDA_ERROR_CODE_REJECTED
    UNSUPPORTED = lib.WEIDA_ERROR_CODE_UNSUPPORTED
    INTERNAL = lib.WEIDA_ERROR_CODE_INTERNAL
    NO_REPLY = lib.WEIDA_ERROR_CODE_NO_REPLY


class PeerEventKind(enum.IntEnum):
    """The kind of a `PeerEvent`: which transition of a dialled address."""

    CONNECTED = lib.WEIDA_PEER_EVENT_CONNECTED
    LOST = lib.WEIDA_PEER_EVENT_LOST
    RETRYING = lib.WEIDA_PEER_EVENT_RETRYING
    GAVE_UP = lib.WEIDA_PEER_EVENT_GAVE_UP
    MISSED = lib.WEIDA_PEER_EVENT_MISSED


class LossCause(enum.IntEnum):
    """`LossCause`: why a connection was lost."""

    IDLE_TIMEOUT = lib.WEIDA_LOSS_IDLE_TIMEOUT
    PEER_CLOSED = lib.WEIDA_LOSS_PEER_CLOSED
    PEER_RESTARTING = lib.WEIDA_LOSS_PEER_RESTARTING
    LOCALLY_CLOSED = lib.WEIDA_LOSS_LOCALLY_CLOSED
    RESET = lib.WEIDA_LOSS_RESET
    TRANSPORT_ERROR = lib.WEIDA_LOSS_TRANSPORT_ERROR


class GiveUp(enum.IntEnum):
    """`GiveUp`: why the runtime stopped redialling an address."""

    POLICY = lib.WEIDA_GIVE_UP_POLICY
    PEER_CHANGED = lib.WEIDA_GIVE_UP_PEER_CHANGED
    FAILED = lib.WEIDA_GIVE_UP_FAILED


class Alpn(enum.IntEnum):
    """`Alpn`: the ALPN token a TLS term asks for or offers."""

    WEIDA = lib.WEIDA_ALPN_WEIDA
    RAFT = lib.WEIDA_ALPN_RAFT


class IdentityEventKind(enum.IntEnum):
    """The kind of an `IdentityEvent`: which change of an identity or trust
    source (named after the C constants: the Rust `IdentityEvent` is the
    record)."""

    LOADED = lib.WEIDA_IDENTITY_EVENT_LOADED
    RENEWED = lib.WEIDA_IDENTITY_EVENT_RENEWED
    KEY_CHANGED = lib.WEIDA_IDENTITY_EVENT_KEY_CHANGED
    RENEWAL_FAILED = lib.WEIDA_IDENTITY_EVENT_RENEWAL_FAILED
    HANDOFF_STOLEN = lib.WEIDA_IDENTITY_EVENT_HANDOFF_STOLEN
    TRUST_REFRESHED = lib.WEIDA_IDENTITY_EVENT_TRUST_REFRESHED
    MISSED = lib.WEIDA_IDENTITY_EVENT_MISSED


__all__ = [
    "Acknowledgement",
    "Alpn",
    "Backpressure",
    "Congestion",
    "Deduplication",
    "DeliveryLevel",
    "Discovery",
    "Durability",
    "ErrorCode",
    "GiveUp",
    "IdentityEventKind",
    "LossCause",
    "OrderingMode",
    "OutboxFull",
    "PeerEventKind",
    "ProducerNaming",
]
