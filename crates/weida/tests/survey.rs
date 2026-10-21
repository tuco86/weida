//! SURVEY: one exchange per respondent, and a deadline that is the caller's.
//!
//! The only pattern of the family with a time dimension, which is why it is
//! worth a type: the deadline, the partial result and the late-reply rule are
//! exactly what an application otherwise rebuilds wrongly. Four of the five
//! tests here are about what happens when a respondent does *not* answer the
//! way it should.

use crate::common;

use std::time::Duration;

use common::Server;
use weida::{Error, ErrorCode, SimNet, TransferMeta};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// Answers every question with `body` until the respondent goes away.
fn answer_with(respondent: weida::Respondent, body: &'static [u8]) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok(mut request) = respondent.accept().await {
            let _ = request.body().read_capped(1024).await;
            let Ok(mut reply) = request.reply(TransferMeta::default()).await else {
                return;
            };
            if reply.write_all(body).await.is_err() {
                return;
            }
            let _ = reply.finish();
        }
    })
}

#[tokio::test]
async fn every_respondent_answers_within_the_deadline() {
    let net = SimNet::new();
    // Two *distinct* respondents behind one identity, so one surveyor with
    // one set of trust anchors reaches both: this is the fan-out the pattern
    // exists for, and Req/Rep's round-robin would have asked exactly one.
    let certs = common::Certs::generate();
    let server = weida::Runtime::new(weida::RuntimeConfig {
        network: weida::Network::sim(&net),
        ..weida::RuntimeConfig::default()
    })
    .expect("runtime");
    let mut answering = Vec::new();
    let mut urls = Vec::new();
    let mut bindings = Vec::new();
    for body in [&b"a"[..], &b"b"[..]] {
        let listener = server.listener();
        let binding = listener
            .bind_quic("127.0.0.1:0".parse().expect("loopback"), certs.server_tls())
            .await
            .expect("bind");
        urls.push(format!(
            "weida://127.0.0.1:{}/poll",
            binding.local_addr().port()
        ));
        let respondent = listener.respondent("/poll").expect("respondent");
        answering.push(answer_with(
            respondent,
            if body == b"a" { b"a" } else { b"b" },
        ));
        bindings.push((listener, binding));
    }

    let client = weida::Runtime::new(weida::RuntimeConfig {
        network: weida::Network::sim(&net),
        ..weida::RuntimeConfig::default()
    })
    .expect("client runtime");
    let surveyor = client.surveyor(certs.client_tls());
    for url in &urls {
        within(surveyor.connect(url)).await.expect("connect");
    }
    assert_eq!(surveyor.peer_count(), 2);

    let mut run = within(surveyor.survey(b"who is there", Duration::from_secs(5)))
        .await
        .expect("survey");
    assert_eq!(run.respondents(), 2, "one exchange per respondent");

    let mut answers = Vec::new();
    while let Some(answer) = within(run.next(1024)).await {
        answers.push(answer.expect("a reply, not an error"));
    }
    answers.sort();
    assert_eq!(answers, vec![b"a".to_vec(), b"b".to_vec()]);
    // Every respondent answered, so the run ends **before** its deadline
    // rather than waiting it out, and nothing was late.
    assert_eq!(run.late(), 0);

    for task in answering {
        task.abort();
    }
    client.shutdown().await;
    server.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_late_reply_is_counted_and_not_delivered() {
    let server = Server::start().await;
    let respondent = server.listener.respondent("/poll").expect("respondent");

    // Answers, but well after any deadline this survey will use.
    let slow = tokio::spawn(async move {
        let mut request = respondent.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        let Ok(mut reply) = request.reply(TransferMeta::default()).await else {
            return;
        };
        let _ = reply.write_all(b"too late").await;
        let _ = reply.finish();
    });

    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());
    within(surveyor.connect(&server.url("/poll")))
        .await
        .expect("connect");

    let mut run = within(surveyor.survey(b"quick", Duration::from_millis(100)))
        .await
        .expect("survey");
    assert_eq!(run.respondents(), 1);
    // The deadline, not an error: a partial result is the point of a survey.
    assert!(
        within(run.next(1024)).await.is_none(),
        "the answer is later than the deadline"
    );

    // The answer arrives, is dropped, and is counted — the way the fan-out's
    // drop counter already works (`docs/GUARANTEES.md` §6).
    slow.await.expect("respondent");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(run.late(), 1, "a late answer is counted, not delivered");

    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn an_answer_that_arrived_in_time_survives_a_caller_that_reads_late() {
    // The deadline bounds **waiting**, not what already arrived: a caller
    // that surveys, does other work past its own deadline and then drains
    // must still get the answers that came in time.
    //
    // What makes this worth pinning is how narrowly it holds. `next` takes
    // the buffer before it consults the deadline, and what the deadline
    // branch finds there it counts in `late` instead of dropping. Without
    // that, `Exec::within`'s `select!` (no `biased;`) would see an answer
    // already in the channel and an elapsed deadline both ready and the
    // winner would be a coin flip — in practice hidden by a `tokio` sleep of
    // zero duration not being ready on its first poll, so the contract would
    // hold by accident. Here it holds on purpose, and the narrow race — an
    // answer landing in the same poll the timer fires — is counted rather
    // than silent.
    let server = Server::start().await;
    let respondent = server.listener.respondent("/poll").expect("respondent");
    let answering = answer_with(respondent, b"prompt");

    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());
    within(surveyor.connect(&server.url("/poll")))
        .await
        .expect("connect");

    for round in 0..2 {
        let mut run = within(surveyor.survey(b"who is there", Duration::from_millis(40)))
            .await
            .expect("survey");
        // Long enough that the answer is in the channel and the deadline is
        // spent before the first read.
        tokio::time::sleep(Duration::from_millis(120)).await;
        let answer = within(run.next(1024))
            .await
            .unwrap_or_else(|| panic!("round {round}: the answer arrived inside the deadline"));
        assert_eq!(answer.expect("a reply, not an error"), b"prompt");
        assert_eq!(run.late(), 0, "round {round}: it was not late");
    }

    answering.abort();
    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_respondent_that_never_reads_cannot_hold_the_survey_open() {
    // The deadline is the **survey's**, not just the collection's. Asking is
    // two awaits the peer controls — the exchange's stream budget and its
    // flow-control window — so a respondent that accepts a question and
    // stops reading would otherwise hold `survey()` itself open forever, and every
    // respondent behind it would never be asked at all.
    let server = Server::start().await;
    let respondent = server.listener.respondent("/poll").expect("respondent");
    let silent = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok(request) = respondent.accept().await {
            // Accepted and never read: the question's window fills.
            held.push(request);
        }
    });

    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());
    within(surveyor.connect(&server.url("/poll")))
        .await
        .expect("connect");

    // Larger than the default 1 MiB stream window, so the write cannot
    // complete without a reader.
    let question = vec![0u8; 4 * 1024 * 1024];
    let run = within(surveyor.survey(&question, Duration::from_millis(300)))
        .await
        .expect("the survey returns at its deadline");
    assert_eq!(
        run.respondents(),
        0,
        "a respondent that could not be asked inside the deadline was not asked"
    );

    silent.abort();
    client.shutdown().await;
}

#[tokio::test]
async fn dropping_a_run_ends_the_exchanges_it_was_collecting() {
    // A collector's first await is the answer, so a respondent that accepts
    // and never answers would keep the task — and the bidirectional stream
    // its reply half holds — until the connection died. Dropping the run is
    // the caller saying it wants no more answers, and the respondent is told
    // exactly as it is told about a requester that walks away.
    let server = Server::start().await;
    let respondent = server.listener.respondent("/poll").expect("respondent");
    let (tx, rx) = tokio::sync::oneshot::channel();
    let silent = tokio::spawn(async move {
        let mut request = respondent.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        // Taken before any reply, which is when the signal is available.
        request.canceled().await;
        let _ = tx.send(());
    });

    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());
    within(surveyor.connect(&server.url("/poll")))
        .await
        .expect("connect");

    // A deadline far past the end of this test: the run is ended by being
    // dropped, not by its deadline.
    let run = within(surveyor.survey(b"who is there", Duration::from_secs(300)))
        .await
        .expect("survey");
    assert_eq!(run.respondents(), 1);
    drop(run);

    within(rx)
        .await
        .expect("the respondent learns the surveyor walked away");

    silent.abort();
    client.shutdown().await;
}

#[tokio::test]
async fn a_respondent_that_refuses_is_one_error_among_replies() {
    let server = Server::start().await;
    let respondent = server.listener.respondent("/poll").expect("respondent");

    let refusing = tokio::spawn(async move {
        let request = respondent.accept().await.expect("accept");
        request.refuse(ErrorCode::Rejected).await;
    });

    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());
    within(surveyor.connect(&server.url("/poll")))
        .await
        .expect("connect");

    let mut run = within(surveyor.survey(b"who is there", Duration::from_secs(5)))
        .await
        .expect("survey");
    let outcome = within(run.next(1024)).await.expect("an outcome");
    let err = outcome.expect_err("the respondent refused");
    assert!(matches!(err, Error::Rejected), "{err:?}");
    // A refusal ends that respondent's exchange, not the survey.
    assert!(within(run.next(1024)).await.is_none());

    refusing.await.expect("respondent");
    client.shutdown().await;
}

#[tokio::test]
async fn a_respondent_that_dies_mid_reply_does_not_end_the_survey() {
    let server = Server::start().await;
    let respondent = server.listener.respondent("/poll").expect("respondent");

    let dying = tokio::spawn(async move {
        let mut request = respondent.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
        reply.write_all(b"half").await.expect("write");
        // Abandoned mid-payload: the stream is reset, so the surveyor sees a
        // failure rather than a short answer.
        reply.cancel();
    });

    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());
    within(surveyor.connect(&server.url("/poll")))
        .await
        .expect("connect");

    let mut run = within(surveyor.survey(b"who is there", Duration::from_secs(5)))
        .await
        .expect("survey");
    let outcome = within(run.next(1024)).await.expect("an outcome");
    assert!(
        outcome.is_err(),
        "an interrupted reply is never a complete answer: {outcome:?}"
    );
    assert!(within(run.next(1024)).await.is_none());

    dying.await.expect("respondent");
    client.shutdown().await;
}

#[tokio::test]
async fn a_survey_with_no_respondents_is_empty_not_an_error() {
    let server = Server::start().await;
    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());

    // Nobody connected: "nobody answered" is an answer, so this is an empty
    // run rather than `NotConnected`.
    let mut run = within(surveyor.survey(b"anyone?", Duration::from_secs(5)))
        .await
        .expect("a survey with no respondents is not an error");
    assert_eq!(run.respondents(), 0);
    assert!(within(run.next(1024)).await.is_none());
    assert_eq!(run.late(), 0);

    client.shutdown().await;
}
