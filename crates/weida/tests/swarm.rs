//! SWARM: a seeder answers asks from the application's storage, and a swarm
//! gathers one blob from every seeder at once, verifying each piece against
//! the root ([decisions/0040](../../../docs/decisions/0040-swarm-pattern.md)).

use crate::common;

use std::time::Duration;

use common::Server;
use weida::{Ask, Bits, Blob, BlobId, Error, PieceSize, Trust};

/// Generous ceiling: every assertion below should settle well within it.
const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// A HAVE ask built from the wire table of 0040 §4.2 by hand, not by the
/// encoder, so the layout is pinned.
fn have_ask(root: &BlobId, size: u64, log2: u8) -> Vec<u8> {
    let mut ask = vec![1, 2, log2];
    ask.extend_from_slice(root.as_bytes());
    ask.extend_from_slice(&size.to_be_bytes());
    assert_eq!(ask.len(), 43);
    ask
}

#[tokio::test]
async fn a_malformed_ask_is_refused_before_accept() {
    let server = Server::start().await;
    let seeder = server.listener.seeder("/swarm").expect("seeder");
    let client = server.client_runtime();
    let requester = client.requester(Trust::by_address());
    within(requester.connect(&server.pinned_url("/swarm")))
        .await
        .expect("connect");

    let refused = within(requester.request(b"hello")).await;
    assert!(
        matches!(refused, Err(Error::Rejected)),
        "five bytes are no ask: {refused:?}"
    );

    let data = vec![7u8; 3000];
    let root = BlobId::of(&data);
    let asking = {
        let ask = have_ask(&root, 3000, 10);
        async move { requester.request(&ask).await?.collect(64).await }
    };
    let answering = async {
        let ask = seeder.accept().await.expect("the HAVE reaches the seeder");
        assert_eq!(
            ask.blob(),
            Blob::new(root, 3000, PieceSize::MIN).expect("blob")
        );
        assert_eq!(ask.ask(), Ask::Have);
        let mut have = Bits::new(3);
        have.set(1, true);
        ask.answer_have(&have).await.expect("answered");
    };
    let (answer, ()) = within(async { tokio::join!(asking, answering) }).await;
    assert_eq!(answer.expect("the bit map"), vec![0b010]);
}

#[tokio::test]
async fn an_answer_of_the_wrong_length_is_refused_and_reported() {
    let server = Server::start().await;
    let seeder = server.listener.seeder("/swarm").expect("seeder");
    let client = server.client_runtime();
    let requester = client.requester(Trust::by_address());
    within(requester.connect(&server.pinned_url("/swarm")))
        .await
        .expect("connect");

    let data = vec![9u8; 3000];
    let root = BlobId::of(&data);
    let mut ask = have_ask(&root, 3000, 10);
    ask[1] = 3;
    ask.extend_from_slice(&2u32.to_be_bytes());
    let asking = async move { requester.request(&ask).await?.collect(4096).await };
    let answering = async {
        let ask = seeder.accept().await.expect("the PIECE reaches the seeder");
        assert_eq!(ask.ask(), Ask::Piece(2));
        // Piece 2 of 3000 bytes in 1 KiB pieces is 952 bytes long.
        let short = ask.answer_piece(&data[2048..2999]).await;
        assert!(
            matches!(short, Err(Error::Protocol(_))),
            "a short piece is the application's error: {short:?}"
        );
    };
    let (answer, ()) = within(async { tokio::join!(asking, answering) }).await;
    assert!(
        answer.is_err(),
        "the asker sees a refusal, not bytes: {answer:?}"
    );
}
