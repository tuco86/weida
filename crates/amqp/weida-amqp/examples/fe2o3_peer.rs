//! A foreign AMQP 1.0 peer, on `fe2o3-amqp`'s acceptor, for one connection.
//!
//! The Python binding's tests run against this peer rather than against a
//! scripted server of their own: a client checked only against a script is
//! checked against the opinion of whoever wrote the script. `fe2o3-amqp` is a
//! pure-Rust implementation with an `acceptor`, so this needs no broker and no
//! C toolchain - the same reason `tests/interop_fe2o3.rs` is not `#[ignore]`d.
//!
//! It lives here, as an example of the library, because the binding crate is
//! a `cdylib`: a test binary there would be left with undefined Python
//! symbols.
//!
//! ```text
//! cargo build -p weida-amqp --example fe2o3_peer
//! fe2o3_peer receiver   # accepts one delivery and settles it `accepted`
//! fe2o3_peer sender     # sends one message and waits for its outcome
//! ```
//!
//! The port is chosen by the OS and printed as `PORT <n>` on the first line of
//! stdout, so the caller reads it rather than guessing one. The process then
//! serves exactly one connection and exits.

use std::time::Duration;

use fe2o3_amqp::acceptor::{ConnectionAcceptor, LinkAcceptor, LinkEndpoint, SessionAcceptor};
use tokio::net::TcpListener;

/// Long enough for a Python test to start, short enough that a forgotten
/// process does not outlive the run that spawned it.
const DEADLINE: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() {
    let role = std::env::args().nth(1).unwrap_or_else(|| "receiver".into());

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local_addr").port();
    // Announced rather than agreed: the caller reads the line and dials it.
    // Written with `std`, because it is one line and `tokio`'s `io-std`
    // feature is not in this crate's dev-dependencies.
    println!("PORT {port}");
    std::io::Write::flush(&mut std::io::stdout()).expect("flush");

    let served = tokio::time::timeout(DEADLINE, serve(listener, &role)).await;
    if served.is_err() {
        eprintln!("fe2o3_peer: nothing connected within {DEADLINE:?}");
        std::process::exit(2);
    }
}

async fn serve(listener: TcpListener, role: &str) {
    let acceptor = ConnectionAcceptor::new("fe2o3-peer");
    let (stream, _) = listener.accept().await.expect("accept");
    let mut connection = acceptor.accept(stream).await.expect("the AMQP handshake");
    let mut session = SessionAcceptor::new()
        .accept(&mut connection)
        .await
        .expect("the answering begin");
    let endpoint = LinkAcceptor::new()
        .accept(&mut session)
        .await
        .expect("the answering attach");

    match (role, endpoint) {
        // The client attached as a sender, so this end is the receiver: one
        // delivery, read and settled `accepted`, which is the outcome the
        // client's `await send` has to return.
        ("receiver", LinkEndpoint::Receiver(mut receiver)) => {
            // `Body<Binary>` rather than `Binary`: the payload is a **data**
            // section, and asking this peer for a bare `Binary` asks it for
            // an `amqp-value` of binary - a different descriptor, and a
            // "Descriptor mismatch" if the two disagree.
            let delivery: fe2o3_amqp::link::delivery::Delivery<
                fe2o3_amqp::types::messaging::Body<fe2o3_amqp::types::primitives::Binary>,
            > = receiver.recv().await.expect("a delivery");
            receiver.accept(&delivery).await.expect("the disposition");
            eprintln!("fe2o3_peer: accepted and settled one delivery");
            let _ = receiver.close().await;
        }
        // The client attached as a receiver: one message, and this end waits
        // for the outcome the client settled it with.
        ("sender", LinkEndpoint::Sender(mut sender)) => {
            let outcome = sender
                .send("from fe2o3-amqp")
                .await
                .expect("the client granted credit and answered");
            assert!(
                outcome.is_accepted(),
                "the client settled it as {outcome:?}"
            );
            eprintln!("fe2o3_peer: the client accepted our transfer");
            let _ = sender.close().await;
        }
        (role, endpoint) => {
            let attached = match endpoint {
                LinkEndpoint::Sender(_) => "a sender",
                LinkEndpoint::Receiver(_) => "a receiver",
            };
            panic!("asked for {role}, but the client attached so this end is {attached}");
        }
    }

    let _ = session.end().await;
    let _ = connection.close().await;
}
