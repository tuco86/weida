//! The bound side of SWARM: sessions handed to the application, their orders
//! read one at a time, and its answers checked for shape before they are sent
//! ([decisions/0040](../../../../docs/decisions/0040-swarm-pattern.md) §4.3,
//! [decisions/0041](../../../../docs/decisions/0041-swarm-sessions.md) §4.2).

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use weida_core::{Error, ErrorCode};

use super::wire::{self, FRAME_HEAD_BYTES, Head, MAX_ASK_BYTES, OPEN_BYTES, ORDER_BYTES};
use super::{Ask, Bits, Blob, PieceList};
use crate::runtime::{Exec, RuntimeInner};
use crate::transfer::{
    ConnectionId, IncomingMeta, IncomingRequest, IncomingTransfer, OutgoingTransfer, TransferMeta,
};

/// Sessions a seeder holds at once, from the first byte of their body until
/// the application takes them in [`crate::Seeder::accept`]; a version-1 ask
/// counts as one. One more waits for a place: its request stays in the
/// route's queue (`RuntimeConfig::endpoint_queue`) and, past that, on its
/// stream, as a replier's request does.
pub const MAX_PENDING_SESSIONS: usize = 64;

/// The fixed part of an answer's deadline: the write of an answer of `n`
/// bytes expires after `SEED_STALL + n / MIN_RATE`.
pub const SEED_STALL: Duration = Duration::from_secs(30);

/// The rate, in bytes per second, below which an answer is abandoned; see
/// [`SEED_STALL`].
pub const MIN_RATE: u64 = 64 * 1024;

/// `stall` plus the time `len` bytes take at `rate` bytes per second.
pub(crate) fn deadline_for(stall: Duration, len: u64, rate: u64) -> Duration {
    let rate = rate.max(1);
    let micros = u128::from(len) * 1_000_000 / u128::from(rate);
    stall.saturating_add(Duration::from_micros(
        u64::try_from(micros).unwrap_or(u64::MAX),
    ))
}

/// One session on its way to the application, with the place it holds.
struct Pending {
    session: SeedSession,
    _place: OwnedSemaphorePermit,
}

/// State of a seeder.
pub struct SeedState {
    path: Arc<str>,
    /// An async mutex so `accept(&self)` needs no `&mut self`, as for a
    /// replier.
    sessions: Mutex<mpsc::Receiver<Pending>>,
}

impl SeedState {
    /// Takes over the request route of `path` and starts the task that reads
    /// each session's open, or each version-1 ask, before the application
    /// sees it.
    pub(crate) fn new(
        path: &str,
        route: mpsc::Receiver<IncomingRequest>,
        runtime: &RuntimeInner,
    ) -> SeedState {
        let (tx, rx) = mpsc::channel(MAX_PENDING_SESSIONS);
        let timeout = Duration::from_millis(runtime.config.limits.header_timeout_ms);
        let exec = runtime.exec.clone();
        runtime.exec.spawn(read_sessions(route, tx, exec, timeout));
        SeedState {
            path: Arc::from(path),
            sessions: Mutex::new(rx),
        }
    }
}

/// Moves every request off the route into its own reading task once a place
/// is free, so at most [`MAX_PENDING_SESSIONS`] are read or wait for `accept`
/// while the rest wait in the route's queue. Ends with the seeder, which
/// releases the route.
async fn read_sessions(
    mut route: mpsc::Receiver<IncomingRequest>,
    out: mpsc::Sender<Pending>,
    exec: Exec,
    timeout: Duration,
) {
    let places = Arc::new(Semaphore::new(MAX_PENDING_SESSIONS));
    loop {
        // A place first: while every place is held, the next request stays in
        // the route's queue and, past that, on its stream, as a replier's does.
        // Dropping it instead would read to its fetch as a version-1 seeder
        // (`Rejected`) or a failed one (`NoReply`).
        let place = tokio::select! {
            () = out.closed() => return,
            place = Arc::clone(&places).acquire_owned() => match place {
                Ok(place) => place,
                // Never closed; a closed semaphore would mean no more sessions.
                Err(_) => return,
            },
        };
        let request = tokio::select! {
            () = out.closed() => return,
            request = route.recv() => match request {
                Some(request) => request,
                None => return,
            },
        };
        exec.spawn(open_session(
            request,
            place,
            out.clone(),
            exec.clone(),
            timeout,
        ));
    }
}

/// What the first bytes of a request body said.
enum Opened {
    /// A whole version-1 ask.
    V1(Blob, Ask),
    /// A session open; the orders follow.
    V2(Blob),
}

/// Reads a version-1 ask to its end, or a session's open.
async fn read_open(body: &mut IncomingTransfer) -> Option<Opened> {
    let mut bytes = Vec::with_capacity(OPEN_BYTES);
    if !body.read_exact_into(&mut bytes, 1).await.ok()? {
        return None;
    }
    match bytes[0] {
        wire::VERSION_1 => {
            let rest = body.read_capped(MAX_ASK_BYTES - 1).await.ok()?;
            bytes.extend_from_slice(&rest);
            let (blob, ask) = wire::decode(&bytes)?;
            Some(Opened::V1(blob, ask))
        }
        wire::VERSION_2 => {
            if !body
                .read_exact_into(&mut bytes, OPEN_BYTES - 1)
                .await
                .ok()?
            {
                return None;
            }
            wire::decode_open(bytes.as_slice().try_into().ok()?).map(Opened::V2)
        }
        _ => None,
    }
}

/// Reads one session's open, or one version-1 ask, under `timeout` and
/// hands the session on, or refuses it.
async fn open_session(
    mut request: IncomingRequest,
    place: OwnedSemaphorePermit,
    out: mpsc::Sender<Pending>,
    exec: Exec,
    timeout: Duration,
) {
    let Some(Some(opened)) = exec.within(timeout, read_open(request.body())).await else {
        exec.within(timeout, request.refuse(ErrorCode::Rejected))
            .await;
        return;
    };
    let meta = request.body().meta_arc();
    let connection = request.connection();
    let (blob, body, version) = match opened {
        Opened::V1(blob, ask) => (blob, None, Version::V1 { ask, given: false }),
        Opened::V2(blob) => (
            blob,
            Some(request.take_body()),
            Version::V2 { ended: false },
        ),
    };
    // The channel holds as many sessions as there are places, so this never
    // waits; a seeder dropped meanwhile drops the session.
    let _ = out
        .send(Pending {
            session: SeedSession {
                blob,
                meta,
                connection,
                version,
                request: Some(request),
                body,
                reply: None,
                owed: None,
                dead: false,
            },
            _place: place,
        })
        .await;
}

impl crate::Seeder {
    /// The endpoint path this seeder serves.
    pub fn path(&self) -> &str {
        &self.state().path
    }

    /// Waits for the next session.
    ///
    /// Every session arrives with a well-formed open: a body that is neither
    /// a session open nor one of the three version-1 asks, or names a blob
    /// the piece tree cannot hold, was refused with [`ErrorCode::Rejected`]
    /// before it got here. Whether the blob is one this seeder serves is the
    /// application's to decide, once per session, from
    /// [`SeedSession::blob`] and [`SeedSession::meta`].
    pub async fn accept(&self) -> Result<SeedSession, Error> {
        let mut sessions = self.state().sessions.lock().await;
        sessions
            .recv()
            .await
            .map(|pending| pending.session)
            .ok_or(Error::NotConnected)
    }
}

/// Which wire a session speaks.
enum Version {
    /// A version-1 ask: one order, answered as the whole reply.
    V1 { ask: Ask, given: bool },
    /// A session: orders on the body, framed answers on the reply, which
    /// opens at the first answer.
    V2 {
        /// The fetch finished its orders.
        ended: bool,
    },
}

/// One fetch's session with this seeder, about one [`Blob`].
///
/// Admit it once, from [`SeedSession::blob`] and [`SeedSession::meta`], or
/// [`SeedSession::decline`] it. Then take its orders with
/// [`SeedSession::next`] and answer each with the method that matches it,
/// before the next one. An order is read only when `next` is called, so a
/// seeder that is busy simply calls it later: the fetch's orders wait in the
/// stream, and QUIC flow control holds the rest back.
///
/// A fetch that only speaks the version-1 ask arrives as a session of one
/// order. Dropping a session ends it; its fetch drops this seeder if an
/// answer was still due.
pub struct SeedSession {
    blob: Blob,
    meta: Arc<IncomingMeta>,
    connection: ConnectionId,
    version: Version,
    /// The exchange, until its reply opens or it is refused.
    request: Option<IncomingRequest>,
    /// A session's orders; a version-1 ask was read whole.
    body: Option<IncomingTransfer>,
    /// A session's answers, once the first is written.
    reply: Option<OutgoingTransfer>,
    /// The order taken by `next` and not yet answered.
    owed: Option<Ask>,
    /// A failed read or a refused answer ended the session.
    dead: bool,
}

impl SeedSession {
    /// The blob this session is about.
    pub fn blob(&self) -> Blob {
        self.blob
    }

    /// Metadata of the exchange; [`IncomingMeta::peer`] names the fetching
    /// side where its connection proved a key.
    pub fn meta(&self) -> &IncomingMeta {
        &self.meta
    }

    /// The connection the session arrived on.
    pub fn connection(&self) -> ConnectionId {
        self.connection
    }

    /// The next order, or `Ok(None)` once the fetch has finished its orders.
    ///
    /// Waits as long as the fetch sends nothing, with no timeout: a fetch
    /// whose caller is slow sends its next order only once the caller takes a
    /// piece, and that idleness is its backpressure. A fetch that goes away
    /// ends the wait with the transport's error.
    ///
    /// `Err(Error::Protocol)` when the order before is still unanswered, when
    /// the fetch sent an order this blob cannot have (the session is then
    /// refused), and after the session ended with an error.
    ///
    /// Not cancel-safe: a call dropped while an order is half read loses
    /// the session's framing. Drop the session instead.
    pub async fn next(&mut self) -> Result<Option<Ask>, Error> {
        if self.owed.is_some() {
            return Err(Error::Protocol("swarm order left unanswered".into()));
        }
        if self.dead {
            return Err(Error::Protocol("swarm session ended".into()));
        }
        let ask = match &mut self.version {
            Version::V1 { ask, given } => {
                if *given {
                    return Ok(None);
                }
                *given = true;
                *ask
            }
            Version::V2 { ended: true } => return Ok(None),
            Version::V2 { ended } => {
                let body = self
                    .body
                    .as_mut()
                    .expect("a session keeps its body until it is dropped");
                let mut bytes = Vec::with_capacity(ORDER_BYTES);
                match body.read_exact_into(&mut bytes, ORDER_BYTES).await {
                    Ok(true) => {}
                    Ok(false) => {
                        *ended = true;
                        if let Ok(reply) = self.open_reply().await {
                            reply.expire_at(Instant::now() + SEED_STALL);
                        }
                        if let Some(reply) = self.reply.take() {
                            let _ = reply.finish();
                        }
                        return Ok(None);
                    }
                    Err(e) => {
                        self.dead = true;
                        return Err(e);
                    }
                }
                let order: &[u8; ORDER_BYTES] = bytes
                    .as_slice()
                    .try_into()
                    .expect("read_exact_into read a whole order");
                match wire::decode_order(&self.blob, order) {
                    Some(ask) => ask,
                    None => {
                        self.refuse_session().await;
                        return Err(Error::Protocol("malformed swarm order".into()));
                    }
                }
            }
        };
        self.owed = Some(ask);
        Ok(Some(ask))
    }

    /// Answers [`Ask::List`] with the piece list of [`SeedSession::blob`].
    pub async fn answer_list(&mut self, list: &PieceList) -> Result<(), Error> {
        let owed = self.owed()?;
        if owed != Ask::List {
            return self.misanswer("a piece list answers only LIST").await;
        }
        if list.blob() != self.blob {
            return self.misanswer("the piece list is of another blob").await;
        }
        self.answer(owed, &list.to_bytes()).await
    }

    /// Answers [`Ask::Have`] with the pieces this seeder can serve now.
    pub async fn answer_have(&mut self, have: &Bits) -> Result<(), Error> {
        let owed = self.owed()?;
        if owed != Ask::Have {
            return self.misanswer("a bit map answers only HAVE").await;
        }
        if have.len() != self.blob.count() {
            return self
                .misanswer("the bit map does not have one bit per piece")
                .await;
        }
        self.answer(owed, have.as_bytes()).await
    }

    /// Answers [`Ask::Piece`] with the piece's bytes.
    ///
    /// The bytes are not checked against the root here: the fetcher checks
    /// every piece, and a seeder that sends a wrong one is dropped by it.
    pub async fn answer_piece(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let owed = self.owed()?;
        if !matches!(owed, Ask::Piece(_)) {
            return self.misanswer("piece bytes answer only PIECE").await;
        }
        if bytes.len() as u64 != wire::reply_len(&self.blob, owed) {
            return self
                .misanswer("the piece does not have the length of its span")
                .await;
        }
        self.answer(owed, bytes).await
    }

    /// Ends the session: a blob this seeder does not serve, or a piece it
    /// does not hold. The fetch drops this seeder for the blob.
    ///
    /// A session gets a DECLINE frame; a version-1 ask is refused with
    /// [`ErrorCode::Rejected`].
    pub async fn decline(mut self) {
        if matches!(self.version, Version::V1 { .. }) {
            if let Some(request) = self.request.take() {
                request.refuse(ErrorCode::Rejected).await;
            }
            return;
        }
        let Ok(reply) = self.open_reply().await else {
            return;
        };
        reply.expire_at(
            Instant::now() + deadline_for(SEED_STALL, FRAME_HEAD_BYTES as u64, MIN_RATE),
        );
        if reply
            .write_all(&wire::encode_head(Head::Decline))
            .await
            .is_ok()
            && let Some(reply) = self.reply.take()
        {
            let _ = reply.finish();
        }
    }

    /// The order an answer is for.
    fn owed(&self) -> Result<Ask, Error> {
        self.owed
            .ok_or_else(|| Error::Protocol("no swarm order to answer".into()))
    }

    /// An answer of the wrong kind or shape: the session is refused toward
    /// the fetch as an internal failure, and the error reported to the
    /// application.
    async fn misanswer(&mut self, what: &str) -> Result<(), Error> {
        self.owed = None;
        self.refuse_session().await;
        Err(Error::Protocol(format!("swarm answer refused: {what}")))
    }

    /// Ends the session toward the fetch: the reply is cancelled if it is
    /// open, or refused with [`ErrorCode::Internal`] if it is not.
    async fn refuse_session(&mut self) {
        self.dead = true;
        if let Some(reply) = self.reply.take() {
            reply.cancel();
        } else if let Some(request) = self.request.take() {
            request.refuse(ErrorCode::Internal).await;
        }
    }

    /// The reply of a session, opened at its first use.
    async fn open_reply(&mut self) -> Result<&mut OutgoingTransfer, Error> {
        if self.reply.is_none() {
            let request = self
                .request
                .take()
                .ok_or_else(|| Error::Protocol("swarm session ended".into()))?;
            self.reply = Some(request.reply(TransferMeta::default()).await?);
        }
        Ok(self.reply.as_mut().expect("opened above"))
    }

    /// Sends `bytes` as the answer to `ask`: the whole reply of a version-1
    /// ask, or one framed answer of a session, each write under
    /// `SEED_STALL` plus its length at `MIN_RATE`.
    async fn answer(&mut self, ask: Ask, bytes: &[u8]) -> Result<(), Error> {
        self.owed = None;
        let written = match self.version {
            Version::V1 { .. } => match self.request.take() {
                Some(request) => write_whole(request, bytes).await,
                None => Err(Error::Protocol("swarm session ended".into())),
            },
            Version::V2 { .. } => self.write_framed(ask, bytes).await,
        };
        if written.is_err() {
            self.dead = true;
        }
        written
    }

    /// One framed answer of a session.
    async fn write_framed(&mut self, ask: Ask, bytes: &[u8]) -> Result<(), Error> {
        let len = u32::try_from(bytes.len())
            .map_err(|_| Error::Protocol("a swarm answer longer than 4 GiB".into()))?;
        let reply = self.open_reply().await?;
        reply.expire_at(
            Instant::now()
                + deadline_for(
                    SEED_STALL,
                    (FRAME_HEAD_BYTES + bytes.len()) as u64,
                    MIN_RATE,
                ),
        );
        reply
            .write_all(&wire::encode_head(Head::Answer { ask, len }))
            .await?;
        reply.write_all(bytes).await
    }
}

/// The whole reply of a version-1 ask.
async fn write_whole(request: IncomingRequest, bytes: &[u8]) -> Result<(), Error> {
    let len = bytes.len() as u64;
    let mut reply = request
        .reply(TransferMeta::default().with_content_len(len))
        .await?;
    reply.expire_at(Instant::now() + deadline_for(SEED_STALL, len, MIN_RATE));
    reply.write_all(bytes).await?;
    reply.finish().map(drop)
}

impl std::fmt::Debug for SeedSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let version = match self.version {
            Version::V1 { .. } => 1,
            Version::V2 { .. } => 2,
        };
        f.debug_struct("SeedSession")
            .field("blob", &self.blob)
            .field("version", &version)
            .field("owed", &self.owed)
            .finish_non_exhaustive()
    }
}
