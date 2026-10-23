//! The blocking facade: ZeroMQ for a caller with no executor.
//!
//! Behind the `blocking` feature, which is **not** default. libzmq's API is
//! synchronous, and most of a protocol library's audience has no reactor at
//! all; this module is that API, one wrapper per socket type, over a
//! [`Context::owned`] reactor the library owns and the caller never sees.
//!
//! # It is a wrapper, and nothing else
//!
//! Every method here is a `block_on` around the asynchronous socket's
//! own method. **No protocol behaviour is decided twice**: the mute actions,
//! REQ's alternation, ROUTER's routing table, the subscription forms, the
//! handshake and the timeouts are the async surface's, and this module
//! cannot disagree with them because it does not implement them. Two
//! ZeroMQs in one crate would be a bug that only shows up in the one the
//! tests do not cover.
//!
//! The three option groups a blocking caller asks about are the same:
//!
//! * `ZMQ_SNDTIMEO` and `ZMQ_RCVTIMEO` are
//!   [`SocketOptions::send_timeout`](crate::SocketOptions::send_timeout) and
//!   `recv_timeout`, honoured by the socket's own `send` and `recv` — passed
//!   down, never re-interpreted here.
//! * `ZMQ_DONTWAIT` is `try_send`/`try_recv`, which are already synchronous
//!   on every socket of this crate: no reactor is entered at all, because
//!   there is nothing to wait for.
//! * the explicit `*_timeout` forms take a bound per call, for the zguide's
//!   Lazy Pirate shape, which is a poll with a deadline rather than an
//!   option.
//!
//! Anything a pattern has beyond sending and receiving — `subscribe`, a
//! ROUTER's `peers`, XPUB's manual subscriptions, a monitor — stays on the
//! asynchronous socket and is reached through [`socket`](ReqSocket::socket),
//! because those calls are synchronous already. The facade adds the blocking
//! forms and nothing more.
//!
//! # The thread rule survives
//!
//! libzmq: "Do not use or close sockets except in the thread that created
//! them." A wrapper owns its socket, and the socket is `Send` and not `Sync`
//! (`crate::SocketCore`), so a wrapper may be moved to another thread and
//! may not be shared with one. Nothing here lends a socket to a pool.
//!
//! ```no_run
//! use weida_zmq::blocking::{BlockingContext, RepSocket};
//! use weida_zmq::Multipart;
//!
//! # fn main() -> weida_zmq::Result<()> {
//! let context = BlockingContext::new()?;
//! let mut responder = RepSocket::new(&context)?;
//! responder.bind("tcp://127.0.0.1:5555")?;
//! loop {
//!     let request = responder.recv()?;
//!     println!("Received {}", String::from_utf8_lossy(request.frames()[0].as_slice()));
//!     responder.send(Multipart::single("World"))?;
//! }
//! # }
//! ```

use std::time::Duration;

use crate::context::{Context, ContextConfig};
use crate::endpoint::Endpoint;
use crate::error::Result;
use crate::message::Multipart;
use crate::options::SocketOptions;
use crate::pipe::Sent;
use crate::pubsub::Published;

/// A context that owns its reactor, for a synchronous caller.
///
/// [`Context::owned`] is the constructor this exists for: the reactor's
/// threads belong to the library, are named, are sized by
/// [`ContextConfig::worker_threads`], and die with the context. A caller that
/// already has a Tokio runtime wants [`Context::new`] and the asynchronous
/// sockets instead: this facade blocks the **calling** thread, and a caller
/// that blocked a reactor worker would be waiting for the worker that has to
/// drive what it is waiting for.
#[derive(Clone, Debug)]
pub struct BlockingContext {
    context: Context,
}

impl BlockingContext {
    /// A context with libzmq's defaults and one reactor worker thread —
    /// `ZMQ_IO_THREADS`'s default of 1, under the name 0013 §4.4 gave it.
    ///
    /// # Errors
    ///
    /// `EMTHREAD` when the OS refuses the threads.
    pub fn new() -> Result<BlockingContext> {
        BlockingContext::with_config(ContextConfig::default())
    }

    /// A context with `config`.
    ///
    /// # Errors
    ///
    /// `EINVAL` for an unusable configuration, `EMTHREAD` when the OS refuses
    /// the threads.
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
    /// Not the reactor's `block_on`: the reactor's threads are busy driving
    /// the connections, and a caller that parked one of them would be
    /// waiting for a worker that is waiting for it. `weida-runtime`'s
    /// contract is that "the caller may drive the returned futures on any
    /// executor — `futures::executor::block_on` included", and this is that
    /// sentence used: the socket's future is polled here, on the thread that
    /// asked, while the sessions it waits for run on the reactor. Two
    /// threads may therefore each block on their own socket, which is how
    /// every libzmq program is written.
    fn drive<F: Future>(future: F) -> F::Output {
        futures::executor::block_on(future)
    }
}

/// The struct, the constructors and the endpoint calls, which are the same
/// for every socket type.
macro_rules! blocking_socket {
    ($(#[$attr:meta])* $name:ident, $inner:path, $kind:literal) => {
        $(#[$attr])*
        ///
        /// A blocking wrapper: every method is the asynchronous socket's own,
        /// driven on the context's reactor. See [the module
        /// documentation](self) for why that is all it is.
        pub struct $name {
            socket: $inner,
        }

        impl $name {
            /// A socket on `context`, with libzmq's defaults.
            ///
            /// # Errors
            ///
            /// `EMFILE` at `ZMQ_MAX_SOCKETS`, `ETERM` on a terminated
            /// context, `EINVAL` for an option this socket type cannot
            /// honour.
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
                })
            }

            /// `zmq_bind`: binds an endpoint and returns the one actually
            /// bound, which is how a wildcard port is read back.
            ///
            /// # Errors
            ///
            /// `EADDRINUSE`, `EADDRNOTAVAIL`, `EPROTONOSUPPORT` for a
            /// transport this library does not have, `EINVAL` for a
            /// malformed endpoint.
            pub fn bind(&self, endpoint: &str) -> Result<Endpoint> {
                BlockingContext::drive(self.socket.bind(endpoint))
            }

            /// `zmq_connect`: returns as soon as the peer exists, dialling
            /// behind it. Synchronous already, so nothing is blocked on.
            ///
            /// # Errors
            ///
            /// `EINVAL` for a malformed endpoint, `EPROTONOSUPPORT` for an
            /// absent transport.
            pub fn connect(&self, endpoint: &str) -> Result<()> {
                self.socket.connect(endpoint)
            }

            /// `zmq_unbind`.
            ///
            /// # Errors
            ///
            /// `ENOENT` for an endpoint this socket has not bound.
            pub fn unbind(&self, endpoint: &str) -> Result<()> {
                self.socket.unbind(endpoint)
            }

            /// `zmq_disconnect`, reporting what the destroyed queue held.
            ///
            /// # Errors
            ///
            /// `ENOENT` for an endpoint this socket has not connected.
            pub fn disconnect(&self, endpoint: &str) -> Result<crate::engine::Discarded> {
                self.socket.disconnect(endpoint)
            }

            /// `ZMQ_LAST_ENDPOINT`.
            pub fn last_endpoint(&self) -> Option<Endpoint> {
                self.socket.last_endpoint()
            }

            /// `zmq_close`.
            pub fn close(&self) {
                self.socket.close();
            }

            /// The asynchronous socket underneath.
            ///
            /// Where everything this facade deliberately does not wrap lives:
            #[doc = concat!("a ", $kind, "'s own calls are synchronous already, so")]
            /// `subscribe`, the option accessors and `monitor` are used
            /// through here rather than copied.
            pub fn socket(&mut self) -> &mut $inner {
                &mut self.socket
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("socket_type", &$kind)
                    .finish_non_exhaustive()
            }
        }
    };
}

/// `send` returning nothing but success: the socket types that queue or
/// block rather than drop.
macro_rules! blocking_send {
    ($name:ident) => {
        impl $name {
            /// `zmq_send`: blocks until the message is queued, bounded by
            /// `ZMQ_SNDTIMEO`.
            ///
            /// # Errors
            ///
            /// `EAGAIN` when `ZMQ_SNDTIMEO` expires, and whatever the socket
            /// type reports — `EFSM` on a REQ out of turn,
            /// `EHOSTUNREACH` for a peer that is gone.
            pub fn send(&mut self, message: impl Into<Multipart>) -> Result<()> {
                BlockingContext::drive(self.socket.send(message))
            }

            /// `zmq_send` with `ZMQ_DONTWAIT`: `EAGAIN` rather than a wait.
            /// Synchronous already — no reactor is entered.
            ///
            /// # Errors
            ///
            /// `EAGAIN` when the message cannot be queued now.
            pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<()> {
                self.socket.try_send(message)
            }

            /// `send` under an explicit bound, for a poll-and-decide loop.
            ///
            /// # Errors
            ///
            /// `EAGAIN` when `limit` expires.
            pub fn send_timeout(
                &mut self,
                message: impl Into<Multipart>,
                limit: Duration,
            ) -> Result<()> {
                BlockingContext::drive(self.socket.send_timeout(message, limit))
            }
        }
    };
}

/// `send` reporting whether the peer took it: the socket types whose mute
/// action is a drop.
macro_rules! blocking_send_reporting {
    ($name:ident) => {
        impl $name {
            /// `zmq_send`, reporting whether the message was queued or
            /// dropped at the high-water mark — which for this socket type is
            /// the pattern's own rule and not an error.
            ///
            /// # Errors
            ///
            /// As the asynchronous socket: `EFSM` out of turn,
            /// `EHOSTUNREACH` for an unroutable message under
            /// `ZMQ_ROUTER_MANDATORY`.
            pub fn send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
                BlockingContext::drive(self.socket.send(message))
            }

            // A REP socket has no `ZMQ_DONTWAIT` form for its reply, and a
            // ROUTER's is written out below: a socket whose mute action is a
            // drop never waits to send, so there is nothing for
            // `ZMQ_DONTWAIT` to shorten. ROUTER has one anyway, because
            // `ZMQ_ROUTER_MANDATORY` turns that drop into a wait.
        }
    };
}

/// `publish`, for the two socket types that never block a publisher.
macro_rules! blocking_publish {
    ($name:ident) => {
        impl $name {
            /// `zmq_send` on a publisher: matches every subscriber's
            /// subscriptions against the first frame and drops for the ones
            /// whose queue is full, which 29/PUBSUB requires. Synchronous
            /// already, so nothing is blocked on.
            pub fn publish(&mut self, message: impl Into<Multipart>) -> Published {
                self.socket.publish(message)
            }
        }
    };
}

/// `recv`, for every socket type that receives.
macro_rules! blocking_recv {
    ($name:ident) => {
        impl $name {
            /// `zmq_recv`: blocks for the next whole message, bounded by
            /// `ZMQ_RCVTIMEO`.
            ///
            /// # Errors
            ///
            /// `EAGAIN` when `ZMQ_RCVTIMEO` expires, `EFSM` out of turn on
            /// REQ and REP, `ENOTSOCK` once the socket is closed.
            pub fn recv(&mut self) -> Result<Multipart> {
                BlockingContext::drive(self.socket.recv())
            }

            /// `zmq_recv` with `ZMQ_DONTWAIT`. Synchronous already.
            ///
            /// # Errors
            ///
            /// `EAGAIN` when nothing is queued.
            pub fn try_recv(&mut self) -> Result<Multipart> {
                self.socket.try_recv()
            }

            /// `recv` under an explicit bound: the zguide's Lazy Pirate shape,
            /// "poll, resend on timeout, abandon after N attempts".
            ///
            /// # Errors
            ///
            /// `EAGAIN` when `limit` expires.
            pub fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
                BlockingContext::drive(self.socket.recv_timeout(limit))
            }
        }
    };
}

blocking_socket!(
    /// REQ, blocking: `zmq_send` then `zmq_recv`, in that order, for ever.
    ReqSocket,
    crate::reqrep::ReqSocket,
    "REQ"
);
blocking_send!(ReqSocket);
blocking_recv!(ReqSocket);

blocking_socket!(
    /// REP, blocking: `zmq_recv` then `zmq_send`.
    RepSocket,
    crate::reqrep::RepSocket,
    "REP"
);
blocking_send_reporting!(RepSocket);
blocking_recv!(RepSocket);

blocking_socket!(
    /// DEALER, blocking: round-robin out, fair-queued in, no lockstep.
    DealerSocket,
    crate::dealerrouter::DealerSocket,
    "DEALER"
);
blocking_send!(DealerSocket);
blocking_recv!(DealerSocket);

blocking_socket!(
    /// ROUTER, blocking: the routing id is the first frame, both ways.
    RouterSocket,
    crate::dealerrouter::RouterSocket,
    "ROUTER"
);
blocking_send_reporting!(RouterSocket);
blocking_recv!(RouterSocket);

impl RouterSocket {
    /// The `ZMQ_DONTWAIT` form, which a ROUTER has because
    /// `ZMQ_ROUTER_MANDATORY` turns its silent drop into a wait: `EAGAIN` at
    /// the high-water mark and `EHOSTUNREACH` for a routing id it does not
    /// know. Synchronous already.
    ///
    /// # Errors
    ///
    /// `EAGAIN`, `EHOSTUNREACH`, or `EINVAL` for a message with no routing
    /// id frame.
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
        self.socket.try_send(message)
    }
}

blocking_socket!(
    /// PUSH, blocking: round-robin over the workers with room, never
    /// dropping.
    PushSocket,
    crate::pipeline::PushSocket,
    "PUSH"
);
blocking_send!(PushSocket);

blocking_socket!(
    /// PULL, blocking: fair-queued at the sink.
    PullSocket,
    crate::pipeline::PullSocket,
    "PULL"
);
blocking_recv!(PullSocket);

blocking_socket!(
    /// PUB, blocking only in name: a publisher never waits for a subscriber.
    PubSocket,
    crate::pubsub::PubSocket,
    "PUB"
);
blocking_publish!(PubSocket);

blocking_socket!(
    /// SUB, blocking. `subscribe` is on the socket underneath, where it is
    /// synchronous.
    SubSocket,
    crate::pubsub::SubSocket,
    "SUB"
);
blocking_recv!(SubSocket);

blocking_socket!(
    /// XPUB, blocking: subscriptions arrive as messages.
    XPubSocket,
    crate::xpubxsub::XPubSocket,
    "XPUB"
);
blocking_publish!(XPubSocket);
blocking_recv!(XPubSocket);

blocking_socket!(
    /// XSUB, blocking: subscriptions go out as messages.
    XSubSocket,
    crate::xpubxsub::XSubSocket,
    "XSUB"
);
blocking_recv!(XSubSocket);

impl XSubSocket {
    /// Sends upstream — a message or a subscription in the `%x01`/`%x00`
    /// form, which is what an XSUB's send is. Synchronous already.
    pub fn send(&mut self, message: impl Into<Multipart>) -> Published {
        self.socket.send(message)
    }
}

blocking_socket!(
    /// PAIR, blocking: one peer, no auto-reconnect, never dropping.
    PairSocket,
    crate::pair::PairSocket,
    "PAIR"
);
blocking_send!(PairSocket);
blocking_recv!(PairSocket);

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: a caller with no reactor at all runs a request-reply exchange,
        /// with **each side blocking on its own thread** — which is how every
        /// libzmq program is written, and is asserted from a plain `#[test]` for
        /// exactly that reason: no `#[tokio::test]`, no ambient runtime, nothing
        /// but the context's own reactor threads.
        ///
        /// The server serves in a loop and is never joined, which is
        /// `hwserver.c`'s `while (1)` and is also load-bearing here: a reply is
        /// **queued** when `send` returns, and a socket dropped before its
        /// session wrote that queue out takes the reply with it. libzmq answers
        /// that with `ZMQ_LINGER` on `zmq_close`; this library answers it with a
        /// finite close budget on the *context* (`DEFAULT_CLOSE_BUDGET`), so a
        /// socket that is simply let go discards what it had queued. A server
        /// that outlives its clients has nothing to linger for.
        #[test]
        fn a_caller_with_no_reactor_runs_request_reply() {
            let context = BlockingContext::new().expect("context");
            let mut responder = RepSocket::new(&context).expect("rep");
            let bound = responder.bind("tcp://127.0.0.1:0").expect("bind");

            std::thread::spawn(move || {
                while let Ok(request) = responder.recv() {
                    assert_eq!(request.frames()[0].as_slice(), b"Hello");
                    responder.send(Multipart::single("World")).expect("reply");
                }
            });

            let mut requester = ReqSocket::new(&context).expect("req");
            requester.connect(&bound.to_string()).expect("connect");
            for _ in 0..3 {
                requester.send(Multipart::single("Hello")).expect("send");
                let reply = requester.recv().expect("a reply");
                assert_eq!(reply.frames()[0].as_slice(), b"World");
            }
        }
        /// Claim: a pipeline works the same way across two blocking threads —
        /// the facade is not a REQ/REP special case.
        #[test]
        fn a_pipeline_crosses_two_blocking_threads() {
            let context = BlockingContext::new().expect("context");
            let mut puller = PullSocket::new(&context).expect("pull");
            let bound = puller.bind("tcp://127.0.0.1:0").expect("bind");
            let worker = std::thread::spawn(move || puller.recv().expect("a task"));

            let mut pusher = PushSocket::new(&context).expect("push");
            pusher.connect(&bound.to_string()).expect("connect");
            pusher.send(Multipart::single("work")).expect("send");
            let task = worker.join().expect("the worker");
            assert_eq!(task.frames()[0].as_slice(), b"work");
        }
        /// Claim: `ZMQ_RCVTIMEO` and `ZMQ_DONTWAIT` are the asynchronous
        /// socket's, passed down rather than re-decided here — a bounded receive
        /// on a socket with no peer gives `EAGAIN`, and so does the
        /// `ZMQ_DONTWAIT` form, which never touches the reactor at all.
        #[test]
        fn the_timeouts_are_the_options_and_not_a_second_policy() {
            let context = BlockingContext::new().expect("context");
            let options = SocketOptions {
                recv_timeout: Some(Duration::from_millis(50)),
                ..SocketOptions::default()
            };
            let mut puller = PullSocket::with_options(&context, options).expect("pull");
            puller.bind("tcp://127.0.0.1:0").expect("bind");

            let started = std::time::Instant::now();
            let err = puller.recv().unwrap_err();
            assert_eq!(err.errno(), "EAGAIN", "{err}");
            assert!(
                started.elapsed() >= Duration::from_millis(50),
                "the option's own bound was waited out"
            );

            let err = puller.try_recv().unwrap_err();
            assert_eq!(err.errno(), "EAGAIN", "{err}");

            let err = puller.recv_timeout(Duration::from_millis(10)).unwrap_err();
            assert_eq!(err.errno(), "EAGAIN", "{err}");
        }
        /// Claim: the socket underneath is reachable, so nothing pattern-specific
        /// had to be wrapped — a SUB subscribes through it and then receives
        /// through the facade.
        #[test]
        fn pattern_specific_calls_stay_on_the_socket_underneath() {
            let context = BlockingContext::new().expect("context");
            let mut publisher = PubSocket::new(&context).expect("pub");
            let bound = publisher.bind("tcp://127.0.0.1:0").expect("bind");
            let mut subscriber = SubSocket::new(&context).expect("sub");
            subscriber.connect(&bound.to_string()).expect("connect");
            // `subscribe` is synchronous on the asynchronous socket, so the
            // facade does not wrap it.
            subscriber.socket().subscribe("px").expect("subscribe");

            let mut delivered = None;
            weida_testkit::poll_until("a published message", weida_testkit::EXPECTED, || {
                publisher.publish(Multipart::single("px.eur 1.09"));
                delivered = subscriber.try_recv().ok();
                delivered.is_some()
            });
            let delivered = delivered.expect("a message");
            assert_eq!(delivered.frames()[0].as_slice(), b"px.eur 1.09");
        }
    }
}
