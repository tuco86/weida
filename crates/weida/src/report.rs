//! The remote view of a connection: each side's REPORT stream carries its
//! view of the QUIC path to the other
//! ([decisions/0036](../../../docs/decisions/0036-connection-statistics.md)
//! §4.5, `docs/PROTOCOL.md` §6.10).
//!
//! Only the sender's QUIC stack knows which of its packets were lost, so a
//! receiver learns its download loss here. A side sends one record every
//! 2 s, on its own clock, once both HELLOs listed capability code `2`; the
//! receiver keeps the latest record in one fixed-size slot and nothing else.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use weida_core::Error;
use weida_protocol::header::limits::MAX_PATH_RECORD_BYTES;
use weida_protocol::{FrameKind, PathRecord, ReportHead, encode_frame};

use crate::conn::{ConnHandle, violation};
use crate::runtime::Exec;
use crate::stats::{RemoteStats, TransportStats, UdpCounts};
use crate::transport::{Link, RecvHalf};

/// How often a side sends its record: a constant on its own clock, so the
/// peer cannot choose the rate (`docs/PROTOCOL.md` §6.10 rule 2).
const REPORT_INTERVAL: Duration = Duration::from_secs(2);

/// The peer's latest record on one connection, and whether its REPORT
/// stream was seen: one fixed-size slot, no queue.
#[derive(Default)]
pub(crate) struct RemoteSlot {
    /// The peer opened its REPORT stream; a second one is a violation.
    claimed: AtomicBool,
    latest: Mutex<Option<(PathRecord, Instant)>>,
}

impl RemoteSlot {
    /// The peer's latest record as weida's type, aged from its arrival to `now`.
    pub(crate) fn latest(&self, now: Instant) -> Option<RemoteStats> {
        let latest = *self.latest.lock().unwrap_or_else(PoisonError::into_inner);
        latest.map(|(record, arrived)| RemoteStats {
            rtt: Duration::from_micros(record.rtt_us),
            min_rtt: Duration::from_micros(record.min_rtt_us),
            cwnd: record.cwnd,
            congestion_events: record.congestion_events,
            lost_packets: record.lost_packets,
            lost_bytes: record.lost_bytes,
            sent_packets: record.sent_packets,
            current_mtu: u16::try_from(record.current_mtu).unwrap_or(u16::MAX),
            tx: UdpCounts {
                datagrams: record.tx_datagrams,
                bytes: record.tx_bytes,
            },
            rx: UdpCounts {
                datagrams: record.rx_datagrams,
                bytes: record.rx_bytes,
            },
            age: now.saturating_duration_since(arrived),
        })
    }

    fn store(&self, record: PathRecord, now: Instant) {
        *self.latest.lock().unwrap_or_else(PoisonError::into_inner) = Some((record, now));
    }
}

/// This side's record: the numbers of one `quinn` statistics read, and no
/// address.
fn record_of(stats: &TransportStats) -> PathRecord {
    let path = &stats.path;
    PathRecord {
        rtt_us: u64::try_from(path.rtt.as_micros()).unwrap_or(u64::MAX),
        min_rtt_us: u64::try_from(path.min_rtt.as_micros()).unwrap_or(u64::MAX),
        cwnd: path.cwnd,
        congestion_events: path.congestion_events,
        lost_packets: path.lost_packets,
        lost_bytes: path.lost_bytes,
        sent_packets: path.sent_packets,
        current_mtu: u64::from(path.current_mtu),
        tx_datagrams: stats.tx.datagrams,
        tx_bytes: stats.tx.bytes,
        rx_datagrams: stats.rx.datagrams,
        rx_bytes: stats.rx.bytes,
    }
}

/// Sends this side's REPORT stream: the head frame, a record at once and
/// then one every [`REPORT_INTERVAL`], until the connection goes. Spawned
/// once, when negotiation agreed capability code `2`.
///
/// Holds the link, not the connection's handle: reporting is not a use of
/// the connection, so it must not keep an unused pooled one alive (B-383).
pub(crate) async fn send_reports(link: Arc<Link>, exec: Exec) {
    let mut stream = match link.open_uni().await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::debug!(error = %e, "could not open the REPORT stream");
            return;
        }
    };
    let mut out = encode_frame(FrameKind::Report, &ReportHead.encode());
    loop {
        // Only a QUIC connection agrees code 2, so the statistics exist.
        if let Some(stats) = link.transport_stats() {
            record_of(&stats).encode_into(&mut out);
        }
        if stream.write_all(&out).await.is_err() {
            return;
        }
        out.clear();
        tokio::select! {
            () = exec.sleep(REPORT_INTERVAL) => {}
            _ = link.closed() => return,
        }
    }
}

/// Reads the peer's REPORT stream into the connection's slot, enforcing
/// `docs/PROTOCOL.md` §6.10: the capability was agreed, this is the peer's
/// only REPORT stream, no record is longer than 256 bytes and none ends at
/// FIN half written. Anything else closes the connection with
/// `PROTOCOL_VIOLATION`.
///
/// Takes the handle and keeps only a weak one while it waits: the
/// stream lives as long as the connection, and the peer's reports are not a
/// use of it, so they must not keep an unused pooled one alive (B-383).
pub(crate) async fn receive_reports(ctx: ConnHandle, mut stream: RecvHalf) -> Result<(), Error> {
    if !ctx.agreed_now().is_some_and(|agreed| agreed.path_report) {
        return violation(&ctx, "REPORT without agreed capability 2");
    }
    if ctx.remote.claimed.swap(true, Ordering::AcqRel) {
        return violation(&ctx, "a second REPORT stream on one connection");
    }
    let weak = Arc::downgrade(&ctx);
    drop(ctx);
    // The connection is gone once the handle cannot be upgraded; dropping
    // its last handle closed the link, so the read below ends too.
    let violated = |reason: &str| match weak.upgrade() {
        Some(ctx) => violation(&ctx, reason),
        None => Ok(()),
    };
    // A record is at most 256 bytes plus a two-byte prefix, and every
    // complete one is consumed at once, so `pending` never holds more than
    // one partial record and one read.
    let mut buf = [0u8; 512];
    let mut pending: Vec<u8> = Vec::with_capacity(buf.len() + MAX_PATH_RECORD_BYTES + 2);
    loop {
        match stream.read(&mut buf).await {
            Ok(Some(0)) => continue,
            Ok(Some(n)) => {
                pending.extend_from_slice(&buf[..n]);
                let mut at = 0;
                let mut newest = None;
                while let Some((record, used)) = match PathRecord::decode(&pending[at..]) {
                    Ok(record) => record,
                    Err(e) => return violated(&e.to_string()),
                } {
                    at += used;
                    newest = Some(record);
                }
                if let Some(record) = newest {
                    let Some(ctx) = weak.upgrade() else {
                        return Ok(());
                    };
                    ctx.remote.store(record, ctx.exec.now());
                }
                pending.drain(..at);
            }
            Ok(None) => {
                if !pending.is_empty() {
                    return violated("a REPORT record truncated at FIN");
                }
                return Ok(());
            }
            // The peer reset its stream or the connection went away: the
            // slot keeps the last record it had.
            Err(e) => {
                tracing::debug!(error = %e, "REPORT stream ended");
                return Ok(());
            }
        }
    }
}
