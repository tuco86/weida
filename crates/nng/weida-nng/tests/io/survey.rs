//! SURVEYOR and RESPONDENT against each other, over real connections.

use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use weida_nng::{Context, ContextConfig, Error, RespondentSocket, SocketOptions, SurveyorSocket};
use weida_sp::backtrace::{self, DEFAULT_MAX_HOPS};
use weida_sp::header::{EndpointType, HEADER_LEN, ProtocolHeader};
use weida_sp::message;

/// A survey deadline no test reaches: the tests that do not claim an expiry
/// end their surveys by other means.
const HOUR: Duration = Duration::from_secs(3600);

fn options(survey_time: Duration) -> SocketOptions {
    SocketOptions {
        survey_time,
        // No socket timeout: the survey deadline (`survey_time`) is what ends
        // a collect, and a slow run is reported by the test's own waits. The
        // handshake bound is an hour.
        recv_timeout: None,
        send_timeout: None,
        handshake_timeout: Duration::from_secs(3600),
        reconnect_min: Duration::from_millis(10),
        ..SocketOptions::default()
    }
}

async fn survey_of(
    ctx: &Context,
    respondents: usize,
    survey_time: Duration,
) -> (SurveyorSocket, Vec<RespondentSocket>) {
    let surveyor = SurveyorSocket::with_options(ctx, options(survey_time)).expect("surveyor");
    let url = surveyor
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let mut peers = Vec::new();
    for _ in 0..respondents {
        let respondent =
            RespondentSocket::with_options(ctx, options(survey_time)).expect("respondent");
        respondent.dial(&url).await.expect("dial");
        peers.push(respondent);
    }
    while surveyor.pipe_count() < respondents {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    (surveyor, peers)
}

/// Claim: a survey reaches every respondent and each answer comes back
/// matched by the survey ID in the terminal position of the tag stack
/// (§3, §4).
#[tokio::test]
async fn a_survey_reaches_every_respondent_and_the_answers_come_back() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (surveyor, respondents) = survey_of(&ctx, 3, HOUR).await;

    surveyor.send(b"who is there".to_vec()).await.expect("send");

    for (n, respondent) in respondents.iter().enumerate() {
        let survey = respondent.recv().await.expect("the survey");
        assert_eq!(survey.body(), b"who is there");
        assert_eq!(survey.header().len(), 4, "one tag: the survey ID");
        assert_eq!(survey.header()[0] & 0x80, 0x80, "with its terminal bit");
        respondent.send(vec![n as u8]).await.expect("answer");
    }

    let mut answers = Vec::new();
    for _ in 0..3 {
        answers.push(surveyor.recv().await.expect("an answer").body()[0]);
    }
    answers.sort_unstable();
    assert_eq!(answers, [0, 1, 2]);
    assert_eq!(surveyor.discarded_late(), 0);
}

/// Claim: `SURVEYTIME` starts **at the send**, not when a respondent
/// receives the survey — so the budget a slow respondent spends is the
/// budget the surveyor already started (§4).
#[tokio::test]
async fn the_deadline_starts_at_the_send() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (surveyor, respondents) = survey_of(&ctx, 1, Duration::from_millis(300)).await;

    let sent = Instant::now();
    surveyor.send(b"ping".to_vec()).await.expect("send");
    // The respondent takes its time and answers after the deadline.
    let survey = respondents[0].recv().await.expect("the survey");
    assert_eq!(survey.body(), b"ping");
    tokio::time::sleep(Duration::from_millis(400)).await;
    respondents[0]
        .send(b"too late".to_vec())
        .await
        .expect("answer");

    // An answer that reached a clock restarted at the receive would have been
    // delivered; ETIMEDOUT with the answer discarded below is the proof that
    // the clock started at the send.
    let err = surveyor.recv().await.unwrap_err();
    assert!(matches!(err, Error::ETIMEDOUT(_)), "{err:?}");
    eprintln!("the survey ended {:?} after the send", sent.elapsed());
    // The late answer is discarded, and nothing is sent back about it. A
    // response is only routed — and therefore only counted late — by a
    // receive that drains the pipes, so the receive comes first: it discards
    // the answer and then says the survey is over in the protocol's own
    // vocabulary rather than timing out again (§8). Polling the counter
    // before that receive would be a wait on nothing.
    let err = surveyor.recv().await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");
    // `discarded_late` drains the pipes itself, so the only thing left to
    // wait for is the answer's *arrival*: the respondent's `send` returns
    // when the message is queued, not when it is delivered, and on a
    // transport that coalesces writes it may still be in flight here. The
    // counter is monotone, so this poll is exact where a sleep would be a
    // guess.
    weida_testkit::until(
        "the late answer arriving to be discarded",
        Duration::from_secs(5),
        || surveyor.discarded_late() > 0,
    )
    .await;
    assert_eq!(surveyor.discarded_late(), 1);
}

/// Claim: a respondent that declines by silence is **indistinguishable**
/// from one that is slow, unreachable, or dead (§4). The surveyor's own
/// observation is identical in all four cases, and this test asserts that
/// equality rather than calling silence a failure.
#[tokio::test]
async fn silence_is_indistinguishable_from_slowness() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    // Four runs, four reasons for an absent answer, one observation.
    let mut observations = Vec::new();
    for reason in ["declines", "slow", "closed", "never reads"] {
        let (surveyor, respondents) = survey_of(&ctx, 1, Duration::from_millis(200)).await;
        surveyor.send(b"anybody".to_vec()).await.expect("send");
        match reason {
            "declines" => {
                // Reads the survey and deliberately says nothing.
                respondents[0].recv().await.expect("the survey");
            }
            "slow" => {
                respondents[0].recv().await.expect("the survey");
                tokio::time::sleep(Duration::from_millis(300)).await;
                respondents[0].send(b"late".to_vec()).await.expect("answer");
            }
            "closed" => respondents[0].close(),
            _ => {}
        }
        let err = surveyor.recv().await.unwrap_err();
        observations.push(err.name().to_string());
    }

    assert_eq!(
        observations,
        vec!["NNG_ETIMEDOUT"; 4],
        "a surveyor learns the same thing from a decline, a delay, a close and a silent peer: \
         {observations:?}"
    );
}

/// Claim: at most one response is collected per respondent; a second
/// answer from the same pipe is discarded (§4).
#[tokio::test]
async fn at_most_one_response_per_respondent_is_collected() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (surveyor, respondents) = survey_of(&ctx, 1, HOUR).await;
    surveyor.send(b"once".to_vec()).await.expect("send");

    let survey = respondents[0].recv().await.expect("the survey");
    respondents[0].send(b"first".to_vec()).await.expect("first");

    // A second answer to the same survey, sent by hand through a fresh
    // context so the replier's own state rule does not refuse it.
    let second = respondents[0].context();
    let resurveyed = {
        // The respondent must receive something to answer; re-send the
        // survey so the second context has a target, then answer twice.
        surveyor.send(b"twice".to_vec()).await.expect("send again");
        second.recv().await.expect("the second survey")
    };
    assert_eq!(resurveyed.body(), b"twice");
    second.send(b"second".to_vec()).await.expect("second");

    assert_eq!(survey.body(), b"once");
    let collected = surveyor.recv().await.expect("one answer");
    assert_eq!(
        collected.body(),
        b"second",
        "the answer to the newest survey is the one collected"
    );
    // Nothing more is delivered: watched over a short window, because the
    // survey's deadline is an hour.
    let more = tokio::time::timeout(Duration::from_millis(300), surveyor.recv()).await;
    assert!(
        more.is_err(),
        "no second answer from the same respondent: {more:?}"
    );
}

/// Claim: contexts overlap surveys with independent deadlines (§4).
#[tokio::test]
async fn contexts_overlap_surveys_with_their_own_deadlines() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (surveyor, respondents) = survey_of(&ctx, 1, HOUR).await;

    let first = surveyor.context();
    let second = surveyor.context();
    first.send(b"one".to_vec()).await.expect("send one");
    second.send(b"two".to_vec()).await.expect("send two");

    // The respondent answers both, each through its own context.
    let a = respondents[0].context();
    let b = respondents[0].context();
    let survey_a = a.recv().await.expect("a");
    let survey_b = b.recv().await.expect("b");
    assert_ne!(survey_a.header(), survey_b.header(), "two survey IDs");
    b.send(survey_b.body().to_vec()).await.expect("answer b");
    a.send(survey_a.body().to_vec()).await.expect("answer a");

    let answer_first = first.recv().await.expect("first");
    let answer_second = second.recv().await.expect("second");
    assert_eq!(answer_first.body(), b"one");
    assert_eq!(answer_second.body(), b"two");
}

/// Claim: an answer reaches the context that asked for it even when a
/// **sibling** context is the one that takes it off the pipe. Both receives
/// are parked before either answer exists — which is what overlapping
/// surveys are for (§4) — so whichever of them the arrival wakes drains the
/// pipe and files the other's answer into its slot. A filing that does not
/// wake the slot's owner leaves that context waiting out its own deadline
/// with the answer already in memory, so this is asserted on two answers
/// collected concurrently rather than one after the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sibling_that_drains_the_pipe_delivers_the_other_contexts_answer() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (surveyor, respondents) = survey_of(&ctx, 1, HOUR).await;

    let first = surveyor.context();
    let second = surveyor.context();
    first.send(b"one".to_vec()).await.expect("send one");
    second.send(b"two".to_vec()).await.expect("send two");

    let a = respondents[0].context();
    let b = respondents[0].context();
    let survey_a = a.recv().await.expect("a");
    let survey_b = b.recv().await.expect("b");

    let answering = async {
        b.send(survey_b.body().to_vec()).await.expect("answer b");
        a.send(survey_a.body().to_vec()).await.expect("answer a");
    };
    // `join!` polls in order, so both collections are parked before the
    // answers exist and one of them files the other's.
    let collected = tokio::join!(first.recv(), second.recv(), answering);
    assert_eq!(collected.0.expect("first").body(), b"one");
    assert_eq!(collected.1.expect("second").body(), b"two");
}

/// Claim: a receive with no active survey is `NNG_ESTATE`, and a
/// respondent that received nothing cannot answer (§4, §8).
#[tokio::test]
async fn the_forbidden_orders_are_estate() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (surveyor, respondents) = survey_of(&ctx, 1, HOUR).await;

    let err = surveyor.recv().await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");

    let err = respondents[0].send(b"unasked".to_vec()).await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");
}

/// Waits for one await on a peer or on the socket, reporting a step that
/// takes longer than ten seconds instead of failing the test for it.
async fn bounded<T>(step: impl Future<Output = T>) -> T {
    weida_testkit::within(
        "a step on a peer or the socket",
        Duration::from_secs(10),
        step,
    )
    .await
}

/// A respondent driven by hand: a raw connection that handshakes as
/// RESPONDENT and stays open for the surveyor to send to.
async fn raw_respondent(addr: &str) -> TcpStream {
    let mut peer = bounded(TcpStream::connect(addr)).await.expect("connect");
    bounded(peer.write_all(&ProtocolHeader::new(EndpointType::Respondent).encode()))
        .await
        .expect("our header");
    let mut theirs = [0u8; HEADER_LEN];
    bounded(peer.read_exact(&mut theirs))
        .await
        .expect("their header");
    peer
}

/// Reads the survey off the wire and answers it. The answer carries the
/// survey's own stack, which is how a respondent tells the surveyor which
/// survey it is answering.
async fn answer_survey(peer: &mut TcpStream, payload: &[u8]) {
    let mut size = [0u8; message::SIZE_LEN];
    bounded(peer.read_exact(&mut size))
        .await
        .expect("the survey's size field");
    let len = usize::try_from(u64::from_be_bytes(size)).expect("a survey that fits in memory");
    let mut survey = vec![0u8; len];
    bounded(peer.read_exact(&mut survey))
        .await
        .expect("the survey");
    let (stack, _) = backtrace::decode(&survey, DEFAULT_MAX_HOPS).expect("the survey's stack");
    let answer = stack.encode_message(payload);
    bounded(peer.write_all(&message::encode(&answer)))
        .await
        .expect("the answer");
}

/// Waits, bounded, until the surveyor holds `respondents` pipes.
async fn until_connected(surveyor: &SurveyorSocket, respondents: usize) {
    bounded(async {
        while surveyor.pipe_count() < respondents {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

/// Collects the `expected` answers the survey holds, however long they take,
/// then watches a short window to see that no further one is delivered. The
/// survey's deadline is an hour, so it ends neither wait.
async fn collect_answers(surveyor: &SurveyorSocket, expected: usize) -> usize {
    for _ in 0..expected {
        bounded(surveyor.recv())
            .await
            .expect("an answer the survey holds");
    }
    let extra = tokio::time::timeout(Duration::from_millis(300), surveyor.recv()).await;
    assert!(extra.is_err(), "no answer beyond the bound: {extra:?}");
    expected
}

/// Drains the pipes in bulk, with `discarded_late()`, until the surveyor has
/// counted `discarded` answers as refused. That count is reached only once the
/// answers that cause it have all been filed, so the wait ends when every answer
/// has reached the surveyor. A surveyor that never reaches the count is
/// reported as a wait that goes on.
async fn drain_until_discarded(surveyor: &SurveyorSocket, discarded: u64) {
    weida_testkit::until(
        "the surveyor counting the answers as refused",
        Duration::from_secs(10),
        || surveyor.discarded_late() >= discarded,
    )
    .await;
}

/// Claim: a survey files at most `recv_depth` answers and counts the rest as
/// discarded, however many respondents answer it. The bound applies to answers
/// filed in bulk, while nothing takes them: `discarded_late()`, and a receive on
/// a sibling context, file several answers waiting in the pipes before any of
/// them is taken. A lone receive consumes each answer as it files it, so it never
/// holds more than one; what waits for it sits in each pipe's own bounded queue.
/// Without the bound, one bulk filing would hold an answer per connected
/// respondent until the application takes them.
#[tokio::test]
async fn a_survey_keeps_only_as_many_answers_as_its_bound_allows() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let surveyor = SurveyorSocket::with_options(
        &ctx,
        SocketOptions {
            recv_depth: Some(4),
            ..options(HOUR)
        },
    )
    .expect("surveyor");
    let url = bounded(surveyor.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    let addr = url.trim_start_matches("tcp://").to_owned();

    // Twelve respondents, each kept connected until the test ends, so that
    // each one can answer and its answer stays in the socket.
    let mut respondents = Vec::new();
    for _ in 0..12 {
        respondents.push(raw_respondent(&addr).await);
    }
    until_connected(&surveyor, respondents.len()).await;

    bounded(surveyor.send(b"who is there".to_vec()))
        .await
        .expect("send");
    for peer in &mut respondents {
        answer_survey(peer, b"here").await;
    }

    // Eight of the twelve are past the depth. The wait ends once all twelve
    // have reached the surveyor, and they are then filed in bulk.
    drain_until_discarded(&surveyor, 8).await;
    let delivered = collect_answers(&surveyor, 4).await;
    assert_eq!(
        delivered, 4,
        "a survey files its receive depth of answers and no more"
    );
    assert!(
        surveyor.discarded_late() >= 8,
        "the answers past the depth are discarded and counted: {}",
        surveyor.discarded_late()
    );
}

/// Claim: a survey's answers are bounded in octets as well as in count. It files
/// at most one pipe's octet ceiling of answers, and an answer that would cross the
/// ceiling is discarded and counted, except that the first answer is always kept
/// whatever its size. As with the count, the bound applies to answers filed in
/// bulk, while nothing takes them. A lone receive consumes each answer as it
/// files it, and what waits for it sits in each pipe's own bounded queue. Without
/// the octet bound, one bulk filing would hold every answer at its full size.
#[tokio::test]
async fn answers_are_bounded_in_octets_too() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let surveyor = SurveyorSocket::with_options(
        &ctx,
        SocketOptions {
            recv_max_size: 4 * 1024 * 1024,
            ..options(HOUR)
        },
    )
    .expect("surveyor");
    let url = bounded(surveyor.listen("tcp://127.0.0.1:0"))
        .await
        .expect("listen")
        .url()
        .to_string();
    let addr = url.trim_start_matches("tcp://").to_owned();

    let mut respondents = Vec::new();
    for _ in 0..3 {
        respondents.push(raw_respondent(&addr).await);
    }
    until_connected(&surveyor, respondents.len()).await;

    bounded(surveyor.send(b"how large?".to_vec()))
        .await
        .expect("send");
    let large = vec![9u8; 3 * 1024 * 1024];
    for peer in &mut respondents {
        answer_survey(peer, &large).await;
    }

    // Two answers of three mebibytes make six, which is under the eight
    // mebibyte ceiling; the third would make nine, so it is the one refused.
    drain_until_discarded(&surveyor, 1).await;
    let delivered = collect_answers(&surveyor, 2).await;
    assert_eq!(
        delivered, 2,
        "two 3 MiB answers fit the 8 MiB ceiling and a third does not"
    );
    assert!(
        surveyor.discarded_late() >= 1,
        "the answer over the ceiling is discarded and counted: {}",
        surveyor.discarded_late()
    );
}
