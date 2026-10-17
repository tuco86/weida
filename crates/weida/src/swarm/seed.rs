//! The bound side of SWARM: decoded asks handed to the application, and its
//! answers checked for shape before they are sent
//! ([decisions/0040](../../../../docs/decisions/0040-swarm-pattern.md) §4.3).

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use weida_core::{Error, ErrorCode};

use super::wire::{self, MAX_ASK_BYTES};
use super::{Ask, Bits, Blob, PieceList};
use crate::runtime::{Exec, RuntimeInner};
use crate::transfer::{ConnectionId, Delivery, IncomingMeta, IncomingRequest, TransferMeta};

/// Asks a seeder holds at once, from the first byte of their body until the
/// application takes them in [`crate::Seeder::accept`]. One more is dropped
/// unread, and its asker sees [`Error::NoReply`].
pub const MAX_PENDING_ASKS: usize = 64;

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

/// One ask on its way to the application, with the place it holds.
struct Pending {
    request: SeedRequest,
    _place: OwnedSemaphorePermit,
}

/// State of a seeder.
pub struct SeedState {
    path: Arc<str>,
    /// An async mutex so `accept(&self)` needs no `&mut self`, as for a
    /// replier.
    asks: Mutex<mpsc::Receiver<Pending>>,
}

impl SeedState {
    /// Takes over the request route of `path` and starts the task that reads
    /// and decodes each ask before the application sees it.
    pub(crate) fn new(
        path: &str,
        route: mpsc::Receiver<IncomingRequest>,
        runtime: &RuntimeInner,
    ) -> SeedState {
        let (tx, rx) = mpsc::channel(MAX_PENDING_ASKS);
        let timeout = Duration::from_millis(runtime.config.limits.header_timeout_ms);
        let exec = runtime.exec.clone();
        runtime.exec.spawn(read_asks(route, tx, exec, timeout));
        SeedState {
            path: Arc::from(path),
            asks: Mutex::new(rx),
        }
    }
}

/// Moves every request off the route into its own reading task, at most
/// [`MAX_PENDING_ASKS`] at once. Ends with the seeder, which releases the
/// route.
async fn read_asks(
    mut route: mpsc::Receiver<IncomingRequest>,
    out: mpsc::Sender<Pending>,
    exec: Exec,
    timeout: Duration,
) {
    let places = Arc::new(Semaphore::new(MAX_PENDING_ASKS));
    loop {
        let request = tokio::select! {
            () = out.closed() => return,
            request = route.recv() => match request {
                Some(request) => request,
                None => return,
            },
        };
        let Ok(place) = Arc::clone(&places).try_acquire_owned() else {
            tracing::debug!("a seeder has {MAX_PENDING_ASKS} asks pending; one more is dropped");
            continue;
        };
        exec.spawn(read_one(request, place, out.clone(), exec.clone(), timeout));
    }
}

/// Reads one ask under `timeout` and hands it on, or refuses it.
async fn read_one(
    mut request: IncomingRequest,
    place: OwnedSemaphorePermit,
    out: mpsc::Sender<Pending>,
    exec: Exec,
    timeout: Duration,
) {
    let read = exec
        .within(timeout, request.body().read_capped(MAX_ASK_BYTES))
        .await;
    let decoded = match read {
        Some(Ok(bytes)) => wire::decode(&bytes),
        _ => None,
    };
    let Some((blob, ask)) = decoded else {
        exec.within(timeout, request.refuse(ErrorCode::Rejected))
            .await;
        return;
    };
    // The channel holds as many asks as there are places, so this never
    // waits; a seeder dropped meanwhile drops the ask, and its asker sees
    // `NoReply`.
    let _ = out
        .send(Pending {
            request: SeedRequest { request, blob, ask },
            _place: place,
        })
        .await;
}

impl crate::Seeder {
    /// The endpoint path this seeder serves.
    pub fn path(&self) -> &str {
        &self.state().path
    }

    /// Waits for the next ask.
    ///
    /// Every ask arrives decoded and well formed: a body that is not one of
    /// the three asks, names a piece past the end or a blob the piece tree
    /// cannot hold was refused with [`ErrorCode::Rejected`] before it got
    /// here. Whether the blob is one this seeder serves is the
    /// application's to decide, from [`SeedRequest::blob`] and
    /// [`SeedRequest::meta`].
    pub async fn accept(&self) -> Result<SeedRequest, Error> {
        let mut asks = self.state().asks.lock().await;
        asks.recv()
            .await
            .map(|pending| pending.request)
            .ok_or(Error::NotConnected)
    }
}

/// One ask a [`crate::Swarm`] sent to this seeder: LIST, HAVE or PIECE of
/// one [`Blob`].
///
/// Answer it with the method that matches [`SeedRequest::ask`], or
/// [`SeedRequest::decline`] it. Dropping it unanswered tells the asker
/// [`Error::NoReply`].
pub struct SeedRequest {
    request: IncomingRequest,
    blob: Blob,
    ask: Ask,
}

impl SeedRequest {
    /// The blob asked about.
    pub fn blob(&self) -> Blob {
        self.blob
    }

    /// What is asked.
    pub fn ask(&self) -> Ask {
        self.ask
    }

    /// Metadata of the exchange; [`IncomingMeta::peer`] names the asker
    /// where its connection proved a key.
    pub fn meta(&self) -> &IncomingMeta {
        self.request.meta()
    }

    /// The connection the ask arrived on.
    pub fn connection(&self) -> ConnectionId {
        self.request.connection()
    }

    /// Answers [`Ask::List`] with the piece list of [`SeedRequest::blob`].
    pub async fn answer_list(self, list: &PieceList) -> Result<Delivery, Error> {
        if self.ask != Ask::List {
            return self.misanswer("a piece list answers only LIST").await;
        }
        if list.blob() != self.blob {
            return self.misanswer("the piece list is of another blob").await;
        }
        self.answer(&list.to_bytes()).await
    }

    /// Answers [`Ask::Have`] with the pieces this seeder can serve now.
    pub async fn answer_have(self, have: &Bits) -> Result<Delivery, Error> {
        if self.ask != Ask::Have {
            return self.misanswer("a bit map answers only HAVE").await;
        }
        if have.len() != self.blob.count() {
            return self
                .misanswer("the bit map does not have one bit per piece")
                .await;
        }
        self.answer(have.as_bytes()).await
    }

    /// Answers [`Ask::Piece`] with the piece's bytes.
    ///
    /// The bytes are not checked against the root here: the fetcher checks
    /// every piece, and a seeder that sends a wrong one is dropped by it.
    pub async fn answer_piece(self, bytes: &[u8]) -> Result<Delivery, Error> {
        if !matches!(self.ask, Ask::Piece(_)) {
            return self.misanswer("piece bytes answer only PIECE").await;
        }
        if bytes.len() as u64 != wire::reply_len(&self.blob, self.ask) {
            return self
                .misanswer("the piece does not have the length of its span")
                .await;
        }
        self.answer(bytes).await
    }

    /// Refuses the ask with [`ErrorCode::Rejected`]: a blob this seeder does
    /// not serve, or a piece it does not hold. The fetcher drops this seeder
    /// for the blob.
    pub async fn decline(self) {
        self.request.refuse(ErrorCode::Rejected).await;
    }

    /// An answer of the wrong kind or shape: refused toward the asker as an
    /// internal failure, and reported to the application.
    async fn misanswer(self, what: &str) -> Result<Delivery, Error> {
        self.request.refuse(ErrorCode::Internal).await;
        Err(Error::Protocol(format!("swarm answer refused: {what}")))
    }

    /// Writes `bytes` as the whole reply, under `SEED_STALL` plus its length
    /// at `MIN_RATE`.
    async fn answer(self, bytes: &[u8]) -> Result<Delivery, Error> {
        let len = bytes.len() as u64;
        let mut reply = self
            .request
            .reply(TransferMeta::default().with_content_len(len))
            .await?;
        reply.expire_at(Instant::now() + deadline_for(SEED_STALL, len, MIN_RATE));
        reply.write_all(bytes).await?;
        reply.finish()
    }
}

impl std::fmt::Debug for SeedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeedRequest")
            .field("blob", &self.blob)
            .field("ask", &self.ask)
            .finish_non_exhaustive()
    }
}
