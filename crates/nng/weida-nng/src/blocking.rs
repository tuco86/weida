//! The blocking facade: SP for a caller with no executor.
//!
//! Behind the `blocking` feature, which is **not** default. NNG's own API is
//! synchronous — "every `nng` crate call is blocking, `Socket::recv`
//! included" (`docs/research/nanomsg-nng.md` §13) — and most of this
//! library's audience has no reactor at all. This module is that API: one
//! wrapper per protocol over a [`Context::owned`] reactor the library owns
//! and the caller never sees.
//!
//! # It is a wrapper, and nothing else
//!
//! Every method here is a `block_on` around the asynchronous socket's own
//! method. **No protocol behaviour is decided twice**: REQ's alternation and
//! its resend timer, the tag stacks, the survey deadline, the full-queue
//! actions, the handshake and the timeouts are the async surface's, and this
//! module cannot disagree with them because it does not implement them. Two
//! SP implementations in one crate would be a bug that only shows up in the
//! one the tests do not cover.
//!
//! What a synchronous caller asks about is handled the same way:
//!
//! * `NNG_OPT_SENDTIMEO` and `NNG_OPT_RECVTIMEO` are
//!   [`SocketOptions::send_timeout`] and
//!   [`recv_timeout`](SocketOptions::recv_timeout), honoured by the socket's
//!   own `send` and `recv` — passed down, never re-interpreted here. They
//!   are what turns a stalled exchange into `NNG_ETIMEDOUT` instead of a
//!   thread parked forever.
//! * `NNG_FLAG_NONBLOCK` is `try_send`/`try_recv`, which are already
//!   synchronous on every socket of this crate: no executor is entered,
//!   because there is nothing to wait for.
//!
//! Anything a protocol has beyond sending and receiving — `subscribe`, a
//! surveyor's `survey_time`, a SUB's `discarded` — stays on the asynchronous
//! socket and is reached through [`socket`](ReqSocket::socket), because
//! those calls are synchronous already. The facade adds the blocking forms
//! and nothing more.
//!
//! ```no_run
//! use weida_nng::blocking::{BlockingContext, RepSocket};
//!
//! # fn main() -> weida_nng::Result<()> {
//! let context = BlockingContext::new()?;
//! let responder = RepSocket::new(&context)?;
//! responder.listen("tcp://127.0.0.1:5555")?;
//! loop {
//!     let request = responder.recv()?;
//!     println!("Received {}", String::from_utf8_lossy(request.body()));
//!     responder.send(b"World".to_vec())?;
//! }
//! # }
//! ```

use crate::context::{Context, ContextConfig};
use crate::engine::{Dialer, Listener};
use crate::error::Result;
use crate::message::Message;
use crate::options::SocketOptions;
use crate::socket::Broadcast;

/// A context that owns its reactor, for a synchronous caller.
///
/// [`Context::owned`] is the constructor this exists for: the reactor's
/// threads belong to the library, are sized by
/// [`ContextConfig::worker_threads`], and die with the context. A caller
/// that already has a Tokio runtime wants [`Context::new`] and the
/// asynchronous sockets instead: this facade blocks the **calling** thread,
/// and a caller that blocked a reactor worker would be waiting for the
/// worker that has to drive what it is waiting for.
#[derive(Clone, Debug)]
pub struct BlockingContext {
    context: Context,
}

impl BlockingContext {
    /// A context with NNG's defaults.
    ///
    /// # Errors
    ///
    /// `NNG_ESYSERR` when the OS refuses the reactor's threads.
    pub fn new() -> Result<BlockingContext> {
        BlockingContext::with_config(ContextConfig::default())
    }

    /// A context with `config`.
    ///
    /// # Errors
    ///
    /// `NNG_EINVAL` for an unusable configuration, `NNG_ESYSERR` when the OS
    /// refuses the threads.
    pub fn with_config(config: ContextConfig) -> Result<BlockingContext> {
        Ok(BlockingContext {
            context: Context::owned(config)?,
        })
    }

    /// The asynchronous context underneath, for the calls that are the
    /// context's rather than a socket's.
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// Drives one of this crate's futures to completion on the **calling**
    /// thread.
    ///
    /// Two things happen here and both are load-bearing. The future is
    /// polled by `futures::executor::block_on`, on the thread that asked,
    /// rather than by the reactor: the reactor's threads are busy driving
    /// the connections, and a caller that parked one of them would be
    /// waiting for a worker that is waiting for it. And the reactor's
    /// context is *entered* for the duration, because a socket that binds
    /// or connects registers with the reactor from the polling thread —
    /// entering costs nothing and hands that registration the right
    /// reactor, where without it the first listen would panic for want of
    /// one.
    ///
    /// Two threads may therefore each block on their own socket, which is
    /// how every NNG program is written.
    pub fn drive<F: Future>(&self, future: F) -> F::Output {
        let _entered = self.context.exec().enter();
        futures::executor::block_on(future)
    }
}

/// The struct, the constructors and the endpoint calls, which are the same
/// for every protocol.
macro_rules! blocking_socket {
    ($(#[$attr:meta])* $name:ident, $inner:path) => {
        $(#[$attr])*
        ///
        /// A blocking wrapper: every method is the asynchronous socket's
        /// own, driven on the calling thread. See [the module
        /// documentation](self) for why that is all it is.
        #[derive(Clone, Debug)]
        pub struct $name {
            socket: $inner,
            /// The context whose reactor drives this socket's futures, and
            /// whose context is entered while they are polled here.
            context: BlockingContext,
        }

        impl $name {
            /// A socket on `context`, with NNG's defaults.
            ///
            /// # Errors
            ///
            /// `NNG_ECLOSED` on a closed context, `NNG_ENOFILES` at the
            /// context's socket ceiling, `NNG_EINVAL` or `NNG_ENOTSUP` for
            /// an option this protocol cannot honour.
            pub fn new(context: &BlockingContext) -> Result<$name> {
                $name::with_options(context, SocketOptions::default())
            }

            /// A socket with `options`, refused here if they cannot be
            /// delivered — the same check at the same moment as the
            /// asynchronous constructor's.
            ///
            /// # Errors
            ///
            /// As [`new`](Self::new).
            pub fn with_options(
                context: &BlockingContext,
                options: SocketOptions,
            ) -> Result<$name> {
                Ok($name {
                    socket: <$inner>::with_options(context.context(), options)?,
                    context: context.clone(),
                })
            }

            /// The asynchronous socket underneath, for everything this
            /// facade deliberately does not wrap.
            pub fn socket(&self) -> &$inner {
                &self.socket
            }

            /// `nng_dial()`: dials and returns once the peer's protocol
            /// header has arrived, which is what NNG's synchronous form
            /// does.
            ///
            /// # Errors
            ///
            /// `NNG_ECONNREFUSED`, `NNG_EADDRINVAL` for a malformed URL,
            /// `NNG_ENOTSUP` for a transport this library does not have,
            /// `NNG_ETIMEDOUT` when the handshake does not finish.
            pub fn dial(&self, url: &str) -> Result<Dialer> {
                self.context.drive(self.socket.dial(url))
            }

            /// `nng_dial(NNG_FLAG_NONBLOCK)`: creates the dialer and
            /// returns at once, retrying behind it. Synchronous already, so
            /// nothing is blocked on.
            ///
            /// # Errors
            ///
            /// `NNG_EADDRINVAL`, `NNG_ENOTSUP`, `NNG_ECLOSED`.
            pub fn dial_nonblocking(&self, url: &str) -> Result<Dialer> {
                self.socket.dial_nonblocking(url)
            }

            /// `nng_listen()`: listens and returns the endpoint actually
            /// bound, which is the only way to learn a wildcard port.
            ///
            /// # Errors
            ///
            /// `NNG_EADDRINUSE`, `NNG_EADDRINVAL`, `NNG_ENOTSUP`.
            pub fn listen(&self, url: &str) -> Result<Listener> {
                self.context.drive(self.socket.listen(url))
            }

            /// Pipes this socket can talk to right now.
            pub fn pipe_count(&self) -> usize {
                self.socket.pipe_count()
            }

            /// `nng_close()`. Idempotent.
            pub fn close(&self) {
                self.socket.close();
            }
        }
    };
}

/// The blocking send and receive of one protocol, in whichever forms it has.
macro_rules! blocking_ops {
    ($name:ident, send) => {
        impl $name {
            /// Sends one message, waiting under `NNG_OPT_SENDTIMEO`.
            ///
            /// # Errors
            ///
            /// `NNG_ETIMEDOUT` when the wait expires — which is what makes a
            /// stalled exchange an error rather than a parked thread —
            /// `NNG_ESTATE` out of turn, `NNG_ECLOSED` on a closed socket.
            pub fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
                self.context.drive(self.socket.send(body))
            }
        }
    };
    ($name:ident, try_send) => {
        impl $name {
            /// `NNG_FLAG_NONBLOCK`: queues the message or says why it
            /// cannot.
            ///
            /// # Errors
            ///
            /// `NNG_ETIMEDOUT` when no peer can take it now.
            pub fn try_send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
                self.socket.try_send(body)
            }
        }
    };
    ($name:ident, broadcast) => {
        impl $name {
            /// Offers one copy to every connected peer and reports what
            /// became of them. Never waits.
            ///
            /// # Errors
            ///
            /// `NNG_ECLOSED` on a closed socket.
            pub fn send(&self, body: impl Into<Vec<u8>>) -> Result<Broadcast> {
                self.socket.send(body)
            }
        }
    };
    ($name:ident, recv) => {
        impl $name {
            /// Receives one message, waiting under `NNG_OPT_RECVTIMEO`.
            ///
            /// # Errors
            ///
            /// `NNG_ETIMEDOUT` when the wait expires, `NNG_ESTATE` out of
            /// turn, `NNG_ECLOSED` on a closed socket.
            pub fn recv(&self) -> Result<Message> {
                self.context.drive(self.socket.recv())
            }
        }
    };
    ($name:ident, try_recv) => {
        impl $name {
            /// `NNG_FLAG_NONBLOCK`: the message if one is queued.
            ///
            /// # Errors
            ///
            /// `NNG_EAGAIN` when nothing is.
            pub fn try_recv(&self) -> Result<Message> {
                self.socket.try_recv()
            }
        }
    };
}

blocking_socket!(
    /// A blocking REQ socket.
    ReqSocket,
    crate::reqrep::ReqSocket
);
blocking_ops!(ReqSocket, send);
blocking_ops!(ReqSocket, recv);

blocking_socket!(
    /// A blocking REP socket.
    RepSocket,
    crate::reqrep::RepSocket
);
blocking_ops!(RepSocket, send);
blocking_ops!(RepSocket, recv);

blocking_socket!(
    /// A blocking PUSH socket.
    PushSocket,
    crate::pipeline::PushSocket
);
blocking_ops!(PushSocket, send);
blocking_ops!(PushSocket, try_send);

blocking_socket!(
    /// A blocking PULL socket.
    PullSocket,
    crate::pipeline::PullSocket
);
blocking_ops!(PullSocket, recv);
blocking_ops!(PullSocket, try_recv);

blocking_socket!(
    /// A blocking PUB socket.
    PubSocket,
    crate::pubsub::PubSocket
);
blocking_ops!(PubSocket, broadcast);

blocking_socket!(
    /// A blocking SUB socket. Subscriptions are set on
    /// [`socket`](SubSocket::socket), which is synchronous already.
    SubSocket,
    crate::pubsub::SubSocket
);
blocking_ops!(SubSocket, recv);
blocking_ops!(SubSocket, try_recv);

blocking_socket!(
    /// A blocking PAIR v0 socket.
    Pair0Socket,
    crate::pair::Pair0Socket
);
blocking_ops!(Pair0Socket, send);
blocking_ops!(Pair0Socket, try_send);
blocking_ops!(Pair0Socket, recv);
blocking_ops!(Pair0Socket, try_recv);

blocking_socket!(
    /// A blocking PAIR v1 socket.
    Pair1Socket,
    crate::pair::Pair1Socket
);
blocking_ops!(Pair1Socket, send);
blocking_ops!(Pair1Socket, try_send);
blocking_ops!(Pair1Socket, recv);

blocking_socket!(
    /// A blocking SURVEYOR socket.
    SurveyorSocket,
    crate::survey::SurveyorSocket
);
blocking_ops!(SurveyorSocket, send);
blocking_ops!(SurveyorSocket, recv);

blocking_socket!(
    /// A blocking RESPONDENT socket.
    RespondentSocket,
    crate::survey::RespondentSocket
);
blocking_ops!(RespondentSocket, send);
blocking_ops!(RespondentSocket, recv);

blocking_socket!(
    /// A blocking BUS socket.
    BusSocket,
    crate::bus::BusSocket
);
blocking_ops!(BusSocket, broadcast);
blocking_ops!(BusSocket, recv);
blocking_ops!(BusSocket, try_recv);

/// The blocking form of one `nng_ctx`.
///
/// A context is what makes two transactions on one socket independent, and
/// a synchronous program wants them for the same reason an asynchronous one
/// does: one thread per transaction, each with its own `NNG_ESTATE` rule
/// and its own deadline.
macro_rules! blocking_context {
    ($(#[$attr:meta])* $name:ident, $inner:path) => {
        $(#[$attr])*
        ///
        /// A blocking wrapper around the asynchronous context. It decides
        /// nothing: the state machine is the library's.
        #[derive(Clone, Debug)]
        pub struct $name {
            context: $inner,
            /// The socket's context, whose reactor drives these futures.
            blocking: BlockingContext,
        }

        impl $name {
            /// Wraps one of the library's contexts.
            pub fn new(context: $inner, blocking: BlockingContext) -> $name {
                $name { context, blocking }
            }

            /// The asynchronous context underneath.
            pub fn context(&self) -> &$inner {
                &self.context
            }

            /// Sends this context's message, under `NNG_OPT_SENDTIMEO`.
            ///
            /// # Errors
            ///
            /// `NNG_ESTATE` out of turn, `NNG_ETIMEDOUT` when the wait
            /// expires, `NNG_ECLOSED` on a closed socket.
            pub fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
                self.blocking.drive(self.context.send(body))
            }

            /// Receives this context's message, under `NNG_OPT_RECVTIMEO`
            /// and, for a surveyor, its own deadline.
            ///
            /// # Errors
            ///
            /// As [`send`](Self::send).
            pub fn recv(&self) -> Result<Message> {
                self.blocking.drive(self.context.recv())
            }
        }
    };
}

blocking_context!(
    /// One blocking REQ transaction.
    ReqCtx,
    crate::reqrep::ReqCtx
);
blocking_context!(
    /// One blocking REP or RESPONDENT transaction. The two protocols are
    /// one machine, so they are one type here as they are in the
    /// asynchronous surface.
    ReplierCtx,
    crate::replier::ReplierCtx
);
blocking_context!(
    /// One blocking survey.
    SurveyorCtx,
    crate::survey::SurveyorCtx
);

impl ReqSocket {
    /// `nng_ctx_open()`: a transaction of its own on this socket.
    pub fn context_of(&self) -> ReqCtx {
        ReqCtx::new(self.socket.context(), self.context.clone())
    }
}

impl RepSocket {
    /// `nng_ctx_open()`: a transaction of its own on this socket.
    pub fn context_of(&self) -> ReplierCtx {
        ReplierCtx::new(self.socket.context(), self.context.clone())
    }
}

impl SurveyorSocket {
    /// `nng_ctx_open()`: a survey of its own on this socket.
    pub fn context_of(&self) -> SurveyorCtx {
        SurveyorCtx::new(self.socket.context(), self.context.clone())
    }
}

impl RespondentSocket {
    /// `nng_ctx_open()`: a transaction of its own on this socket.
    pub fn context_of(&self) -> ReplierCtx {
        ReplierCtx::new(self.socket.context(), self.context.clone())
    }
}

impl SubSocket {
    /// Subscribes to a byte prefix. Synchronous already.
    pub fn subscribe(&self, prefix: impl Into<Vec<u8>>) {
        self.socket.subscribe(prefix);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: the whole of a REQ/REP exchange runs with no reactor in the
        /// caller's thread and no `async` anywhere in the program.
        #[test]
        fn a_request_and_its_reply_need_no_executor() {
            let context = BlockingContext::new().expect("context");
            let options = SocketOptions {
                recv_timeout: Some(std::time::Duration::from_secs(5)),
                send_timeout: Some(std::time::Duration::from_secs(5)),
                ..SocketOptions::default()
            };
            let server = RepSocket::with_options(&context, options.clone()).expect("rep");
            let url = server
                .listen("tcp://127.0.0.1:0")
                .expect("listen")
                .url()
                .to_string();

            let answering = std::thread::spawn(move || {
                let request = server.recv().expect("a request");
                server.send(b"pong".to_vec()).expect("the reply");
                request.into_body().to_vec()
            });

            let client = ReqSocket::with_options(&context, options).expect("req");
            client.dial(&url).expect("dial");
            client.send(b"ping".to_vec()).expect("send");
            assert_eq!(client.recv().expect("recv").body(), b"pong");
            assert_eq!(answering.join().expect("thread"), b"ping");
        }
        /// Claim: a stalled exchange is an error, not a hang. The timeouts are
        /// the socket's own options, honoured by the asynchronous socket this
        /// facade drives.
        #[test]
        fn a_stalled_exchange_times_out_rather_than_parking_the_thread() {
            let context = BlockingContext::new().expect("context");
            let options = SocketOptions {
                recv_timeout: Some(std::time::Duration::from_millis(100)),
                send_timeout: Some(std::time::Duration::from_millis(100)),
                ..SocketOptions::default()
            };
            let puller = PullSocket::with_options(&context, options.clone()).expect("pull");
            puller.listen("tcp://127.0.0.1:0").expect("listen");

            let started = std::time::Instant::now();
            let error = puller.recv().unwrap_err();
            assert!(matches!(error, crate::Error::ETIMEDOUT(_)), "{error:?}");

            let pusher = PushSocket::with_options(&context, options).expect("push");
            let error = pusher.send(b"nowhere".to_vec()).unwrap_err();
            assert!(matches!(error, crate::Error::ETIMEDOUT(_)), "{error:?}");
            assert!(
                started.elapsed() < std::time::Duration::from_secs(2),
                "both returned at their own deadline"
            );
        }
    }
}
