//! Resource limits.
//!
//! Master doc §50: no internal queue may be unbounded, and every allocation
//! that a remote peer can influence must have a defensive ceiling. Each field
//! below names the remote-controlled quantity it bounds.
//!
//! **`Limits` is a per-connection profile.** Every field applies to one
//! connection — which is what a runtime holding one profile per connection
//! tier will need
//! (`docs/decisions/0002-control-and-bulk-separation.md` §6.4). Numbers that
//! belong to a runtime rather than to a connection — how many connections a
//! binding accepts, how many one peer may hold, how deep an endpoint's queue
//! is, how many resolved addresses a dial tries — live on `RuntimeConfig`
//! instead, so that no profile can carry a value nothing reads.

use std::time::Duration;

/// Resource limits applied to one connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Largest frame header this side will accept, in bytes. Checked against
    /// the preamble length *before* any allocation.
    pub max_header_bytes: u64,
    /// QUIC concurrent inbound unidirectional streams. Bounds the number of
    /// live per-stream parse tasks for one-way transfers.
    pub max_concurrent_uni_streams: u32,
    /// QUIC concurrent inbound bidirectional streams; bounds live request
    /// exchanges per connection and, with `max_header_bytes`, worst-case
    /// header memory.
    pub max_concurrent_bidi_streams: u32,
    /// QUIC per-stream receive window in bytes. Bounds buffered payload for one
    /// transfer and provides backpressure to the sender.
    pub stream_receive_window: u64,
    /// QUIC per-connection receive window in bytes.
    pub connection_receive_window: u64,
    /// QUIC keep-alive interval. Sent by the dialling side only, so a binding's
    /// value is not used.
    pub keep_alive: Duration,
    /// QUIC idle timeout, applied in both directions.
    pub idle_timeout: Duration,
    /// How long to wait for the peer HELLO before closing the connection with
    /// `NEGOTIATION_FAILED`, in milliseconds.
    pub hello_timeout_ms: u64,
    /// Subscription filters one peer connection may hold at once, summed over
    /// every path. Exceeding it closes the connection with `LIMIT_EXCEEDED`:
    /// SUBSCRIBE has no stream to answer with an ERROR frame.
    pub max_subscriptions: usize,
    /// Payload bytes a publisher may hold queued for one subscriber. A message
    /// that does not fit is dropped for that subscriber and counted; the
    /// publisher never blocks on a slow consumer.
    pub subscriber_buffer_bytes: usize,
    /// Producer scopes — paths and topics — a receiver tracks per connection
    /// for gap detection or reassembly, when `PerProducer` ordering is
    /// negotiated. The peer chooses the scope names, so the table needs a
    /// ceiling; at the cap a new scope is simply not tracked, no gap is
    /// reported for it and nothing is held back for it.
    pub max_sequence_scopes: usize,
    /// Transfers a receiver may hold back at once, summed over scopes, when
    /// `PerProducer(reassemble)` ordering is negotiated. A held transfer is
    /// an unread stream, so the bytes it pins are quinn's — up to
    /// `stream_receive_window` for it and `connection_receive_window` for the
    /// hold as a whole. At the cap the oldest held transfer is released out
    /// of order with its gap reported, never held in a growing buffer.
    pub max_reorder_hold: usize,
    /// Live transfers on one **local** connection, where the OS connection
    /// *is* the stream and there is no multiplexing
    /// ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
    /// §4.2). Windows caps named-pipe instances at 1-255, which is the
    /// tightest platform limit and therefore the one the default respects;
    /// an `open` at the cap waits for a live transfer to end, exactly as a
    /// QUIC `open` waits on the peer's stream budget, rather than refusing.
    /// A peer configured with zero can therefore open nothing and waits
    /// until its caller's deadline, exactly as one granted no QUIC streams.
    pub max_local_streams: usize,
    /// Connections a subscriber parks toward a peer it dialled, so that peer
    /// can open a stream back
    /// ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.4). An accepted socket cannot be dialled, so fan-out over a local
    /// socket transport rides connections the subscriber opened and left
    /// waiting; each is a file descriptor held for a copy that may never
    /// come, and each counts against `max_local_streams` on both sides. A
    /// publisher that finds none parked drops that copy and counts it, the
    /// same answer fan-out already gives an exhausted subscriber budget. Zero
    /// disables the pool, which makes a subscription over such a transport an
    /// error at subscribe time rather than a silence.
    pub max_parked_reverse: usize,

    /// Identities a receiver remembers per connection for `Bounded`
    /// deduplication. The window bounds how long an identity is kept, not
    /// how many arrive within it, so the count needs its own ceiling; at the
    /// cap the oldest entry is evicted, which costs suppression rather than
    /// memory.
    pub max_dedup_entries: usize,

    /// Bytes of a peer's QUIC datagrams buffered unread per connection,
    /// oldest dropped first. **Zero turns flows off**: the connection then
    /// advertises neither capability code `1` nor `max_datagram_frame_size`,
    /// buffers nothing and runs no datagram reader
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.5).
    /// [`DEFAULT_DATAGRAM_RECEIVE_BYTES`] is the documented value for turning
    /// them on.
    pub datagram_receive_bytes: usize,
    /// Bytes of datagrams queued for sending per connection on QUIC, and per
    /// flow on a local transport; beyond it the oldest queued one is
    /// discarded. Bounds what this side holds for a peer that reads slowly.
    pub datagram_send_bytes: usize,
    /// Whether this side lists capability code `2` on a QUIC connection, so
    /// that when the peer lists it too each side sends the other its view of
    /// the path every 2 s and keeps the peer's latest record, read as
    /// `ConnectionStats::remote`
    /// ([decisions/0036](../../../docs/decisions/0036-connection-statistics.md)
    /// §4.5). Off by default; a local transport never lists it.
    pub path_report: bool,
    /// Inbound flows one connection may hold live. Each is a FLOW stream the
    /// peer opened and a ring this side keeps; a FLOW beyond it is stopped
    /// with `LIMIT_EXCEEDED`.
    pub max_flows: usize,
    /// Unread datagram bytes held per inbound flow; a consumer that falls
    /// behind loses its oldest datagrams, counted per flow.
    pub flow_queue_bytes: usize,
    /// Bytes of datagrams held per connection for flow ids with no live flow
    /// yet — a datagram can overtake its FLOW header — and the one place a
    /// peer can make this side hold datagrams it never registered.
    pub flow_early_bytes: usize,
    /// How long a datagram waits in that ring for its FLOW header before it
    /// is dropped and counted.
    pub flow_early_hold: Duration,

    /// The congestion controller of this profile's QUIC connections. Not a
    /// bound, but a per-connection choice, which is why it lives here: a
    /// bulk profile that shares a bottleneck with media may want a different
    /// controller than a media profile
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §6).
    pub congestion: Congestion,

    /// How long one inbound stream may take to deliver its preamble and then
    /// its header, each read counted from the moment a header slot was free
    /// for it, in milliseconds. A stream that stalls inside its frame head
    /// is stopped with `CANCELED` at the deadline and the connection
    /// survives: the peer opened a stream and then said nothing, which holds
    /// a unit of its stream credit and a task for as long as it likes
    /// otherwise, and the QUIC idle timer does not help against a peer that
    /// keeps the connection alive with its own pings. The wait for the
    /// peer's HELLO is not counted ([`Limits::hello_timeout_ms`] bounds it).
    pub header_timeout_ms: u64,
    /// Inbound streams one connection may have reading their frame head at
    /// once; the rest wait for a free slot without holding a buffer. With
    /// `max_header_bytes` this is the worst case of header memory per
    /// connection ([`Limits::worst_case_header_memory`]), independent of how
    /// many streams the peer's stream budget lets it open.
    pub max_header_streams: usize,
}

/// A QUIC congestion controller, as `quinn` implements them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Congestion {
    /// CUBIC (RFC 9438), `quinn`'s default: loss-based.
    #[default]
    Cubic,
    /// NewReno (RFC 9002 §7): loss-based, the conservative baseline.
    NewReno,
    /// BBR: model-based, reacts to delay rather than waiting for loss.
    /// `quinn` marks its implementation experimental.
    Bbr,
}

/// The documented `Limits::datagram_receive_bytes` for a profile that
/// enables datagram flows: 64 KiB, about fifty full datagrams.
pub const DEFAULT_DATAGRAM_RECEIVE_BYTES: usize = 65_536;

impl Limits {
    /// Worst-case header memory a single hostile connection can pin, in bytes.
    ///
    /// Only [`Limits::max_header_streams`] streams read a frame head at once
    /// and the others wait without a buffer, and a header buffer grows with
    /// the bytes that actually arrive, so the bound is the header cap times
    /// the smaller of that count and the stream budgets (a peer may open its
    /// full uni *and* bidi allowance, which is the other way to reach it).
    pub const fn worst_case_header_memory(&self) -> u64 {
        let budget =
            self.max_concurrent_uni_streams as u64 + self.max_concurrent_bidi_streams as u64;
        let reading = self.max_header_streams as u64;
        self.max_header_bytes * if reading < budget { reading } else { budget }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_header_bytes: 16 * 1024,
            max_concurrent_uni_streams: 2048,
            max_concurrent_bidi_streams: 1024,
            stream_receive_window: 1024 * 1024,
            connection_receive_window: 16 * 1024 * 1024,
            keep_alive: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            hello_timeout_ms: 10_000,
            max_subscriptions: 256,
            subscriber_buffer_bytes: 8 * 1024 * 1024,
            max_sequence_scopes: 1024,
            max_reorder_hold: 256,
            max_local_streams: 255,
            max_parked_reverse: 8,

            max_dedup_entries: 4096,
            datagram_receive_bytes: 0,
            datagram_send_bytes: 65_536,
            path_report: false,
            max_flows: 64,
            flow_queue_bytes: 16_384,
            flow_early_bytes: 4_096,
            flow_early_hold: Duration::from_secs(1),
            congestion: Congestion::Cubic,
            header_timeout_ms: 10_000,
            max_header_streams: 64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_protocol_document() {
        let l = Limits::default();
        assert_eq!(l.max_header_bytes, 16384);
        assert_eq!(l.max_concurrent_uni_streams, 2048);
        assert_eq!(l.max_concurrent_bidi_streams, 1024);
        assert_eq!(l.stream_receive_window, 1 << 20);
        assert_eq!(l.connection_receive_window, 16 << 20);
        assert_eq!(l.keep_alive, Duration::from_secs(10));
        assert_eq!(l.idle_timeout, Duration::from_secs(30));
        assert_eq!(l.hello_timeout_ms, 10_000);
        assert_eq!(l.max_subscriptions, 256);
        assert_eq!(l.subscriber_buffer_bytes, 8 << 20);
        assert_eq!(l.max_sequence_scopes, 1024);
        assert_eq!(l.max_reorder_hold, 256);
        assert_eq!(l.max_local_streams, 255);
        assert_eq!(l.max_parked_reverse, 8);
        assert_eq!(l.max_dedup_entries, 4096);
        assert_eq!(l.datagram_receive_bytes, 0);
        assert_eq!(DEFAULT_DATAGRAM_RECEIVE_BYTES, 64 << 10);
        assert_eq!(l.datagram_send_bytes, 64 << 10);
        assert_eq!(l.max_flows, 64);
        assert_eq!(l.flow_queue_bytes, 16 << 10);
        assert_eq!(l.flow_early_bytes, 4 << 10);
        assert_eq!(l.flow_early_hold, Duration::from_secs(1));
        assert_eq!(l.congestion, Congestion::Cubic);
        assert_eq!(l.header_timeout_ms, 10_000);
        assert_eq!(l.max_header_streams, 64);
    }

    #[test]
    fn worst_case_header_memory_follows_the_header_phase_cap() {
        // 16 KiB * 64 streams reading their head at once.
        assert_eq!(Limits::default().worst_case_header_memory(), 1 << 20);
        // A budget smaller than the cap is the tighter bound.
        let few = Limits {
            max_concurrent_uni_streams: 2,
            max_concurrent_bidi_streams: 1,
            ..Limits::default()
        };
        assert_eq!(few.worst_case_header_memory(), 3 * 16 * 1024);
    }
}
