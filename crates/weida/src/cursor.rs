//! Cursors: what a peer reports about one transfer, and how it gets here.
//!
//! A completion is a **cursor** — a level plus an absolute byte offset — not a
//! verdict
//! ([decisions/0023](https://github.com/tuco86/weida/blob/main/docs/decisions/0023-completion-is-a-cursor.md)).
//! Three properties follow, and all three are structural here rather than
//! promised:
//!
//! * **Coalescing is free.** [`CursorSet`] keeps the maximum offset per level,
//!   so a repeated or reordered record changes nothing and a `watch` channel —
//!   which keeps only the latest value — is the right carrier by construction.
//! * **Nothing waits on a cursor.** A report rides a unidirectional stream of
//!   its own, and every write on it is best effort: a peer that never reads its
//!   [`Cursors`] blocks no transfer, and a peer that never takes its
//!   [`Reporter`] fails none.
//! * **A set costs no allocation.** The entries array is fixed at
//!   `MAX_REPORT_LEVELS`, which is also the cap the decoder enforces, so a
//!   snapshot is a `Copy` value a reader can hold without touching the
//!   connection.
//!
//! There is no error variant on the reading side. `Cursors::changed` yields
//! `None` when the reporter FINed, when the stream was reset and when the
//! connection died, and the three are deliberately indistinguishable: a cursor
//! is never load-bearing, so "no more cursors" is the only fact a reader can
//! act on.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use weida_core::Error;
use weida_protocol::header::MAX_CURSOR_RECORD_LEN;
use weida_protocol::header::limits::MAX_REPORT_LEVELS;
use weida_protocol::{
    CursorHeader, CursorLevel, FrameKind, ReportMode, encode_cursor_record, encode_preamble,
};

use crate::conn::ConnHandle;
use crate::transport::SendHalf;

/// The report table of one connection: the ids **this** side handed out.
///
/// A peer reports only on transfers it received, so each direction allocates
/// from its own space and the two cannot collide — which is why there is no
/// shared numbering rule and no direction field on the wire
/// (`docs/PROTOCOL.md` §6.2).
pub(crate) struct ReportTable {
    /// Next id. Starts at 1: `0` is never allocated, so an id nobody handed
    /// out cannot masquerade as one.
    next: AtomicU64,
    live: std::sync::Mutex<HashMap<u64, Arc<watch::Sender<CursorSet>>>>,
}

impl ReportTable {
    pub(crate) fn new() -> ReportTable {
        ReportTable {
            next: AtomicU64::new(1),
            live: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Hands out the sender for `id`, or `None` if no transfer of ours ordered
    /// it.
    ///
    /// The clone is taken once per cursor stream rather than once per record:
    /// a reader holds it for the stream's life and pays no lock per record.
    pub(crate) fn claim(&self, id: u64) -> Option<Arc<watch::Sender<CursorSet>>> {
        self.live
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .map(Arc::clone)
    }

    /// Drops the report's entry, which is what turns a reader's
    /// [`Cursors::changed`] into `None`.
    ///
    /// A poisoned lock is taken rather than propagated: this runs inside
    /// `Drop for ReportGuard`, and a panic in a destructor during an unwind
    /// aborts the process. A map of senders has no invariant a panic could
    /// have broken.
    pub(crate) fn release(&self, id: u64) {
        self.live
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
    }
}

/// Allocates a report id and the channel its cursors will arrive on.
///
/// The [`Cursors`] handle owns the table entry: dropping it releases the id,
/// which is what keeps the table bounded by handles the application holds
/// rather than by transfers it has ever sent.
pub(crate) fn order_report(conn: &ConnHandle) -> (u64, Cursors) {
    let id = conn.reports.next.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = watch::channel(CursorSet::default());
    conn.reports
        .live
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(id, Arc::new(tx));
    (
        id,
        Cursors {
            rx,
            guard: ReportGuard {
                id,
                conn: ConnHandle::clone(conn),
            },
        },
    )
}

/// Releases a report id when the last reader of its cursors goes away.
struct ReportGuard {
    id: u64,
    conn: ConnHandle,
}

impl Drop for ReportGuard {
    fn drop(&mut self) {
        self.conn.reports.release(self.id);
    }
}

/// The latest offset reported per level, for one transfer.
///
/// Absolute offsets, so this is a complete picture however many records were
/// coalesced away: a reader that misses every intermediate record still ends
/// at the same numbers
/// ([0023](https://github.com/tuco86/weida/blob/main/docs/decisions/0023-completion-is-a-cursor.md)
/// §4.3b). Levels are ordered by their wire value, weida's own below the
/// application floor and the application's above it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CursorSet {
    /// `(level wire value, offset)`, sorted by level, `len` entries used.
    entries: [(u64, u64); MAX_REPORT_LEVELS],
    len: u8,
}

impl CursorSet {
    /// The offset reported for `level`, or `None` if it never was.
    pub fn offset(&self, level: CursorLevel) -> Option<u64> {
        let wire = level.to_wire();
        self.used()
            .iter()
            .find(|(w, _)| *w == wire)
            .map(|(_, offset)| *offset)
    }

    /// Every reported level with its offset, in ascending level order.
    pub fn iter(&self) -> impl Iterator<Item = (CursorLevel, u64)> + '_ {
        self.used().iter().map(|(wire, offset)| {
            (
                CursorLevel::from_wire(*wire).expect("only decoded levels are stored"),
                *offset,
            )
        })
    }

    /// How many levels have been reported.
    pub fn len(&self) -> usize {
        usize::from(self.len)
    }

    /// True while nothing has been reported.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn used(&self) -> &[(u64, u64)] {
        &self.entries[..usize::from(self.len)]
    }

    /// Records `offset` for `level`, keeping the **maximum**.
    ///
    /// Returns whether anything moved. Keeping the maximum is what makes a
    /// duplicated or reordered record a no-op rather than a correction, and
    /// returning "did it move" is what lets the connection skip a
    /// notification nobody would learn anything from.
    ///
    /// A level past the cap is dropped: the decoder already refuses a header
    /// that orders more than `MAX_REPORT_LEVELS`, so this is reachable only
    /// from a peer reporting levels it never was asked for, which is ignored
    /// anyway.
    pub(crate) fn advance(&mut self, level: CursorLevel, offset: u64) -> bool {
        let wire = level.to_wire();
        let used = usize::from(self.len);
        match self.entries[..used].binary_search_by_key(&wire, |(w, _)| *w) {
            Ok(at) => {
                if offset <= self.entries[at].1 {
                    return false;
                }
                self.entries[at].1 = offset;
                true
            }
            Err(at) => {
                if used == MAX_REPORT_LEVELS {
                    return false;
                }
                self.entries[at..=used].rotate_right(1);
                self.entries[at] = (wire, offset);
                self.len += 1;
                true
            }
        }
    }
}

/// What a bounded wait for a cursor found.
///
/// Three outcomes rather than two, because a deadline and an ending are
/// different facts about the same report: [`Reported::Waiting`] says nothing
/// new arrived *yet*, [`Reported::Ended`] says nothing more will
/// ([`Cursors::changed_within`]).
///
/// [`Reported::Changed`] carries no set, because the handle already has it:
/// [`Cursors::snapshot`] and [`Cursors::offset`] read the latest values
/// without waiting, so a variant holding a copy of a 272-byte
/// [`CursorSet`] would make this answer large for no fact it does not already
/// have.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reported {
    /// Something moved: read it with [`Cursors::snapshot`] or
    /// [`Cursors::offset`].
    Changed,
    /// The deadline passed. The report may still continue.
    Waiting,
    /// No further cursors are coming: the reporter finished, the stream was
    /// reset, or the connection went away — three cases deliberately
    /// indistinguishable, none of them a failure.
    Ended,
}

/// The reader's end of one transfer's report.
///
/// Independent of the transfer it came from, and deliberately so: the terminal
/// cursor arrives **after** the payload's FIN, so a handle tied to the
/// transfer would be gone exactly when the interesting record lands.
#[derive(Debug)]
pub struct Cursors {
    rx: watch::Receiver<CursorSet>,
    guard: ReportGuard,
}

impl Cursors {
    /// The latest set, without waiting.
    pub fn snapshot(&self) -> CursorSet {
        *self.rx.borrow()
    }

    /// The latest offset for `level`, without waiting.
    pub fn offset(&self, level: CursorLevel) -> Option<u64> {
        self.rx.borrow().offset(level)
    }

    /// Waits for the next change and returns the new set.
    ///
    /// `None` means no further cursors are coming, and it covers three cases
    /// deliberately indistinguishable: the reporter FINed, the cursor stream
    /// was reset, or the **connection** went away. The last one is why the
    /// connection is watched here at all — a peer that simply never reports
    /// opens no stream, so without it a reader would wait for a FIN nobody
    /// is going to send. None of the three is a failure of anything: a cursor
    /// is never load-bearing, so "no more cursors" is the only fact a reader
    /// can act on.
    pub async fn changed(&mut self) -> Option<CursorSet> {
        tokio::select! {
            changed = self.rx.changed() => {
                changed.ok()?;
                Some(*self.rx.borrow_and_update())
            }
            _ = self.guard.conn.conn.closed() => None,
        }
    }

    /// [`Cursors::changed`], giving up after `deadline`.
    ///
    /// The three outcomes are distinct on purpose: a deadline that passes is
    /// **not** the end of the report, and treating it as one would make a
    /// caller stop reading a verdict that is still coming. An asynchronous
    /// caller can wrap [`Cursors::changed`] in its own timeout and rarely
    /// needs this; a **synchronous** one cannot — a parked thread is
    /// interrupted by nothing — which is why the timer lives here, where the
    /// runtime is: `weida::blocking::Cursors::changed` is this call, written
    /// as text rather than as a link because a link to a feature-gated item
    /// is a hard rustdoc error in the configuration that lacks it.
    pub async fn changed_within(&mut self, deadline: Duration) -> Reported {
        let exec = self.guard.conn.exec.clone();
        match exec.within(deadline, self.changed()).await {
            None => Reported::Waiting,
            Some(None) => Reported::Ended,
            Some(Some(_)) => Reported::Changed,
        }
    }
}

impl std::fmt::Debug for ReportGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReportGuard").field("id", &self.id).finish()
    }
}

/// The writer's end: what a receiver uses to report on a transfer it got.
///
/// Every write here is best effort. A peer that reset the cursor stream, or a
/// connection that died, must not fail the application that is doing the
/// reporting — so a failed write is logged at `debug` and the reporter goes
/// quiet. That is the "nothing waits on a cursor" rule seen from the writing
/// side.
///
/// **The granularity is the reporter's own number and is never negotiated**
/// ([0023](https://github.com/tuco86/weida/blob/main/docs/decisions/0023-completion-is-a-cursor.md)
/// §4.5): more often is always allowed, less often never. The default is
/// [`Reporter::DEFAULT_BYTES`] or [`Reporter::DEFAULT_INTERVAL`], whichever
/// comes first, and [`Reporter::with_granularity`] changes it. Coalescing
/// loses nothing because cursors are absolute, and
/// [`Reporter::finish`] always flushes the latest offset per level — so the
/// terminal cursor is never the one that got coalesced away.
pub struct Reporter {
    conn: ConnHandle,
    report_id: u64,
    levels: Vec<CursorLevel>,
    mode: ReportMode,
    /// Opened on the first record: a reporter that never reports costs no
    /// stream, which is what makes an ordered level the receiver cannot honour
    /// free rather than merely cheap.
    stream: Option<SendHalf>,
    /// Set once a write failed: the report is over, and retrying would only
    /// cost the application time.
    broken: bool,
    /// Bytes a level must advance before another record is worth writing.
    bytes: u64,
    /// Time that must pass before another record is worth writing.
    interval: Duration,
    /// The latest offset reported per level, and the last one **emitted**.
    ///
    /// Both are needed: the latest is what `finish` flushes, and the emitted
    /// one is what the two granularity tests are measured against. Fixed at
    /// the level cap, so a reporter costs no allocation per record.
    pending: [Option<Pending>; MAX_REPORT_LEVELS],
}

/// What a reporter remembers about one level between records.
#[derive(Clone, Copy, Debug)]
struct Pending {
    level: CursorLevel,
    /// Latest offset the application reported.
    latest: u64,
    /// Offset of the last record actually written, if any.
    emitted: Option<u64>,
    /// When that record was written.
    at: Option<Instant>,
}

impl Reporter {
    /// Default byte granularity: a level must advance this far before another
    /// record is worth writing.
    pub const DEFAULT_BYTES: u64 = 1024 * 1024;
    /// Default time granularity: a level may be reported again this often
    /// however little it advanced.
    pub const DEFAULT_INTERVAL: Duration = Duration::from_millis(100);

    pub(crate) fn new(
        conn: ConnHandle,
        report_id: u64,
        levels: Vec<CursorLevel>,
        mode: ReportMode,
    ) -> Reporter {
        Reporter {
            conn,
            report_id,
            levels,
            mode,
            stream: None,
            broken: false,
            bytes: Reporter::DEFAULT_BYTES,
            interval: Reporter::DEFAULT_INTERVAL,
            pending: [None; MAX_REPORT_LEVELS],
        }
    }

    /// Sets how often progress records are worth writing.
    ///
    /// "Every `bytes` or every `interval`, **whichever comes first**". The
    /// number is this side's own and is never negotiated: a sender orders
    /// *which* levels it wants, not how finely
    /// ([0023](https://github.com/tuco86/weida/blob/main/docs/decisions/0023-completion-is-a-cursor.md)
    /// §4.5). It has no effect in [`ReportMode::FinalOnly`], where there is
    /// exactly one record per level whatever the granularity says.
    pub fn with_granularity(mut self, bytes: u64, interval: Duration) -> Reporter {
        self.bytes = bytes;
        self.interval = interval;
        self
    }

    /// The levels the sender ordered, ascending.
    pub fn levels(&self) -> &[CursorLevel] {
        &self.levels
    }

    /// The mode the sender asked for.
    pub fn mode(&self) -> ReportMode {
        self.mode
    }

    /// Reports that `level` has reached `offset`.
    ///
    /// A level the sender did not order is ignored rather than refused: the
    /// order says what the sender wants to hear, and volunteering more would
    /// put records on the wire nobody reads.
    ///
    /// What reaches the wire depends on the mode the sender asked for:
    ///
    /// * [`ReportMode::Progress`] writes a record when the level has advanced
    ///   by the byte granularity **or** the interval has passed since that
    ///   level's last record, whichever comes first; otherwise the offset is
    ///   only remembered.
    /// * [`ReportMode::FinalOnly`] writes nothing at all and remembers the
    ///   offset; [`Reporter::finish`] emits one record per level.
    ///
    /// Either way nothing is lost, because a cursor is absolute:
    /// [`Reporter::finish`] flushes the latest offset per level.
    ///
    /// Never fails for a transport reason: a broken cursor stream yields
    /// `Ok(())`, because a cursor is never load-bearing.
    pub async fn report(&mut self, level: CursorLevel, offset: u64) -> Result<(), Error> {
        if !self.levels.contains(&level) {
            return Ok(());
        }
        let Some(slot) = self.slot(level) else {
            return Ok(());
        };
        let entry = self.pending[slot].get_or_insert(Pending {
            level,
            latest: offset,
            emitted: None,
            at: None,
        });
        // Absolute offsets: a record that would move a level backwards says
        // nothing the receiver would keep, so it is not worth a write either.
        entry.latest = entry.latest.max(offset);
        let latest = entry.latest;
        if self.mode == ReportMode::FinalOnly || !self.worth_writing(slot) {
            return Ok(());
        }
        self.emit(slot, latest).await;
        Ok(())
    }

    /// Flushes the latest offset per level and FINs the cursor stream.
    ///
    /// The flush is what makes coalescing lossless: whatever the granularity
    /// suppressed, the last number each level reached is on the wire before
    /// the FIN. A level that was never reported produces **no** record at
    /// all, which is the honest form of "this side could not report it" — the
    /// sender sees the level missing from its final snapshot rather than a
    /// failure.
    pub async fn finish(mut self) -> Result<(), Error> {
        for slot in 0..MAX_REPORT_LEVELS {
            let Some(entry) = self.pending[slot] else {
                continue;
            };
            if entry.emitted == Some(entry.latest) {
                continue;
            }
            self.emit(slot, entry.latest).await;
        }
        if let Some(mut stream) = self.stream.take()
            && let Err(e) = stream.finish()
        {
            tracing::debug!(error = %e, "failed to finish a cursor stream");
        }
        Ok(())
    }

    /// The slot this level occupies, allocating one on first use.
    ///
    /// `None` only past the cap, which the decoder already refuses: the
    /// ordered levels are at most `MAX_REPORT_LEVELS`.
    fn slot(&self, level: CursorLevel) -> Option<usize> {
        self.levels.iter().position(|l| *l == level)
    }

    /// Is another record for this level worth a write yet?
    fn worth_writing(&self, slot: usize) -> bool {
        let Some(entry) = self.pending[slot] else {
            return false;
        };
        match (entry.emitted, entry.at) {
            // Nothing written for this level yet: the first record is always
            // worth it, however small the advance.
            (None, _) | (_, None) => true,
            (Some(emitted), Some(at)) => {
                entry.latest.saturating_sub(emitted) >= self.bytes
                    || self.conn.exec.now().saturating_duration_since(at) >= self.interval
            }
        }
    }

    /// Writes one record for `slot`, opening the stream if this is the first.
    ///
    /// Records what was written and when, which is what the granularity is
    /// measured against — and what makes a `finish` after a fresh record
    /// write nothing rather than a duplicate.
    async fn emit(&mut self, slot: usize, offset: u64) {
        if self.broken {
            return;
        }
        let Some(entry) = self.pending[slot] else {
            return;
        };
        let mut record = Vec::with_capacity(MAX_CURSOR_RECORD_LEN);
        if let Err(e) = encode_cursor_record(entry.level, offset, &mut record) {
            // An application level above the varint range. Refusing the
            // record rather than the transfer keeps the rule that a cursor
            // fails nothing.
            tracing::debug!(error = %e, "a cursor level has no wire representation");
            return;
        }
        if self.stream.is_none() {
            match self.open().await {
                Ok(stream) => self.stream = Some(stream),
                Err(e) => {
                    tracing::debug!(error = %e, "failed to open a cursor stream");
                    self.broken = true;
                    return;
                }
            }
        }
        let stream = self.stream.as_mut().expect("opened just above");
        if let Err(e) = stream.write_all(&record).await {
            tracing::debug!(error = %e, "failed to write a cursor record");
            self.broken = true;
            return;
        }
        if let Some(entry) = self.pending[slot].as_mut() {
            entry.emitted = Some(offset);
            entry.at = Some(self.conn.exec.now());
        }
    }

    async fn open(&self) -> Result<SendHalf, Error> {
        let mut stream = self.conn.open_uni().await?;
        let head = CursorHeader {
            report_id: self.report_id,
        }
        .encode();
        let mut frame = Vec::with_capacity(head.len() + 10);
        encode_preamble(FrameKind::Cursor, head.len() as u64, &mut frame);
        frame.extend_from_slice(&head);
        stream.write_all(&frame).await?;
        Ok(stream)
    }
}

impl std::fmt::Debug for Reporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reporter")
            .field("report_id", &self.report_id)
            .field("levels", &self.levels)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_protocol::header::Acknowledgement;

    #[test]
    fn a_set_keeps_the_maximum_offset_per_level() {
        let mut set = CursorSet::default();
        let stored = CursorLevel::Known(Acknowledgement::Stored);
        assert!(set.advance(stored, 100));
        // Backwards and repeated records are both no-ops, which is what makes
        // coalescing free.
        assert!(!set.advance(stored, 40));
        assert!(!set.advance(stored, 100));
        assert_eq!(set.offset(stored), Some(100));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn levels_are_ordered_by_their_wire_value() {
        let mut set = CursorSet::default();
        let app = CursorLevel::Application(17);
        let accepted = CursorLevel::Known(Acknowledgement::Accepted);
        assert!(set.advance(app, 1));
        assert!(set.advance(accepted, 2));
        assert_eq!(
            set.iter().collect::<Vec<_>>(),
            vec![(accepted, 2), (app, 1)]
        );
    }

    #[test]
    fn a_set_holds_exactly_the_cap() {
        let mut set = CursorSet::default();
        for i in 0..MAX_REPORT_LEVELS as u64 {
            assert!(set.advance(
                CursorLevel::Application(CursorLevel::APPLICATION_FLOOR + i),
                i
            ));
        }
        assert_eq!(set.len(), MAX_REPORT_LEVELS);
        // One past the cap is dropped rather than overwriting a level: the
        // decoder already refuses a header that orders this many.
        assert!(!set.advance(CursorLevel::Known(Acknowledgement::Stored), 9));
        assert_eq!(
            set.offset(CursorLevel::Known(Acknowledgement::Stored)),
            None
        );
    }
}
