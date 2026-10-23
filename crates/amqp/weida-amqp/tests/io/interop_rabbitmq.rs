//! Interop against RabbitMQ 4.x, under the process supervisor.
//!
//! RabbitMQ 4.x speaks AMQP 1.0 natively on 5672 — 3.x needed a plugin and
//! translated 1.0 into 0-9-1 semantics, 4.x does not — so it is the broker
//! worth measuring against. It is a system package rather than a crate, so
//! every test here is `#[ignore]`d with the install command in the reason, and
//! the harness starts and stops the broker itself rather than assuming a
//! running one.
//!
//! Run them with `rabbitmq-server` on `PATH`:
//!
//! ```text
//! cargo test -p weida-amqp --test interop_rabbitmq -- --ignored
//! ```
//!
//! What each test establishes is written where the test is, so that a run is
//! read rather than guessed at. Whether the RabbitMQ column has been run is
//! recorded in `docs/libraries/amqp.md`.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use weida_amqp::link::LinkOptions;
use weida_amqp::session::SessionOptions;
use weida_amqp::{Connection, ConnectionOptions, Outcome, Sasl, Source, Target};
use weida_amqp_codec::message::Message;
use weida_amqp_codec::{ReceiverSettleMode, SenderSettleMode};
use weida_runtime::Exec;

/// What every `#[ignore]` says, so the install command is one grep away from
/// a skipped test.
macro_rules! needs_rabbitmq {
    () => {
        "needs RabbitMQ 4.x: `pacman -S rabbitmq` (Arch), `apt install rabbitmq-server` \
         (Debian/Ubuntu) or `docker run -p 5672:5672 rabbitmq:4`, then \
         `cargo test -p weida-amqp --test interop_rabbitmq -- --ignored`"
    };
}

const DEADLINE: Duration = Duration::from_secs(30);

/// A bounded wait that cannot fail a test for being slow: it has the shape of
/// `tokio::time::timeout(..)`, but past `expected` the step is reported by
/// `weida_testkit::within` and the wait goes on.
#[track_caller]
fn timeout<F: Future>(
    expected: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, std::convert::Infallible>> {
    let at = std::panic::Location::caller();
    async move {
        let what = format!("the step at {}:{}", at.file(), at.line());
        Ok(weida_testkit::within(&what, expected, future).await)
    }
}

/// The default guest credentials, which RabbitMQ restricts to localhost.
const USER: &str = "guest";
const PASSWORD: &str = "guest";

/// A supervised broker, stopped when the guard drops.
struct Broker {
    child: Child,
    port: u16,
}

impl Broker {
    /// Starts `rabbitmq-server` on a port of its own and waits for 5672 to
    /// accept a connection.
    ///
    /// A port of its own because a developer's machine may already be running
    /// one, and a test that silently used it would be measuring an unknown
    /// configuration.
    async fn start(port: u16) -> Self {
        let child = Command::new("rabbitmq-server")
            .env("RABBITMQ_NODE_PORT", port.to_string())
            .env("RABBITMQ_NODENAME", format!("weida-interop-{port}"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("rabbitmq-server starts");
        let broker = Self { child, port };
        broker.wait_for_port().await;
        broker
    }

    /// Readiness is observed rather than assumed: process creation is not a
    /// listening socket, and RabbitMQ takes seconds to boot its Erlang node.
    async fn wait_for_port(&self) {
        weida_testkit::within(
            "rabbitmq-server to accept a connection",
            Duration::from_secs(60),
            async {
                loop {
                    if tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                        .await
                        .is_ok()
                    {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            },
        )
        .await;
    }

    /// Stopped explicitly, because a broker left running would be picked up
    /// by the next run and measured instead of a fresh one.
    fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn options() -> ConnectionOptions {
    let mut options = ConnectionOptions::new("weida-amqp-interop");
    // RabbitMQ requires SASL and refuses `ANONYMOUS` unless configured for
    // it; PLAIN with the guest credentials is what a fresh install accepts,
    // and only from localhost.
    options.sasl = Sasl::Plain {
        username: USER.to_owned(),
        password: PASSWORD.to_owned(),
    };
    options
}

/// The address syntax the broker demanded, which is the thing an application
/// gets wrong first.
///
/// RabbitMQ 4.x uses the AMQP 1.0 address v2 form: `/queues/{name}` for a
/// queue and `/exchanges/{name}/{key}` for an exchange. The v1 form
/// (`/queue/{name}`, `/amq/queue/{name}`) is still accepted and is what 3.x
/// with the plugin used, so which form a test names is itself a measurement.
const QUEUE: &str = "/queues/weida-interop";

#[tokio::test]
#[ignore = needs_rabbitmq!()]
async fn our_sender_reaches_a_rabbitmq_queue() {
    let exec = Exec::current().unwrap();
    let broker = Broker::start(15672).await;

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", broker.port, options()),
    )
    .await
    .expect("the broker answered")
    .expect("SASL PLAIN and `open` succeeded");
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();
    let mut sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("interop-sender", Target::at(QUEUE))),
    )
    .await
    .unwrap()
    .expect("the broker answered the attach; record the target it reports");

    // What this establishes: the broker grants credit to a publisher link
    // (170 initially, per its own `rabbit.max_link_credit`), accepts our
    // `transfer`, and answers with a `disposition`. The outcome is the
    // measurement — an `accepted` here means the queue took the message.
    let sent = timeout(DEADLINE, sender.send(&Message::data(b"from weida-amqp")))
        .await
        .expect("the broker granted credit")
        .expect("the transfer went out");
    let outcome = timeout(DEADLINE, sender.settled(sent.delivery_id))
        .await
        .expect("the broker settled")
        .expect("an outcome");
    assert_eq!(outcome, Outcome::Accepted);

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    broker.stop();
}

#[tokio::test]
#[ignore = needs_rabbitmq!()]
async fn our_receiver_consumes_from_a_rabbitmq_queue() {
    let exec = Exec::current().unwrap();
    let broker = Broker::start(15673).await;

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", broker.port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();

    // Publish first, on its own link, so the consumer has something to take.
    let sender = timeout(
        DEADLINE,
        session.attach(LinkOptions::sender("interop-feed", Target::at(QUEUE))),
    )
    .await
    .unwrap()
    .unwrap();
    let _ = timeout(DEADLINE, sender.send(&Message::data(b"for the consumer")))
        .await
        .unwrap()
        .unwrap();

    let mut receiver = timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("interop-consumer", Source::at(QUEUE))),
    )
    .await
    .unwrap()
    .unwrap();

    // What this establishes: prefetch is link credit. The broker sends
    // nothing until this `flow`, which is the half of the scheme a client
    // owns, and the payload that comes back is the one published above.
    timeout(DEADLINE, receiver.grant_credit(1))
        .await
        .unwrap()
        .unwrap();
    let delivery = timeout(DEADLINE, receiver.next_delivery())
        .await
        .expect("the broker transferred under our credit")
        .expect("a delivery");
    assert_eq!(
        delivery.payload(),
        Message::data(b"for the consumer").to_vec().unwrap()
    );
    timeout(DEADLINE, receiver.accept(delivery.delivery_id))
        .await
        .unwrap()
        .unwrap();

    timeout(DEADLINE, connection.close())
        .await
        .unwrap()
        .unwrap();
    broker.stop();
}

#[tokio::test]
#[ignore = needs_rabbitmq!()]
async fn rcv_settle_mode_second_is_probed_rather_than_assumed() {
    let exec = Exec::current().unwrap();
    let broker = Broker::start(15674).await;

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", broker.port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();

    let mut asked = LinkOptions::sender("second-probe", Target::at(QUEUE));
    asked.snd_settle_mode = SenderSettleMode::Unsettled;
    asked.rcv_settle_mode = ReceiverSettleMode::Second;

    // What this establishes: the research sheet (`docs/research/amqp10.md`
    // §13) records RabbitMQ as listing exactly-once unsupported, which is a
    // documentation claim and not a measurement. Three outcomes are possible
    // and each goes in the parity table as what it is:
    //
    //   * the answering `attach` says `first`  — narrowed, and a client that
    //     did not read the answer would believe it had a guarantee it has not;
    //   * the attach is refused                — an honest refusal;
    //   * the answering `attach` says `second` — the documentation is stale.
    match timeout(DEADLINE, session.attach(asked)).await {
        Ok(Ok(link)) => {
            let mode = link.negotiated().unwrap().rcv_settle_mode;
            println!("MEASURED rcv-settle-mode: asked second, got {mode:?}");
        }
        Ok(Err(error)) => println!("MEASURED rcv-settle-mode: asked second, refused: {error}"),
        Err(_) => panic!("the broker neither answered nor refused within {DEADLINE:?}"),
    }

    let _ = timeout(DEADLINE, connection.close()).await;
    broker.stop();
}

#[tokio::test]
#[ignore = needs_rabbitmq!()]
async fn the_terminus_the_broker_creates_is_recorded_rather_than_assumed() {
    let exec = Exec::current().unwrap();
    let broker = Broker::start(15675).await;

    let connection = timeout(
        DEADLINE,
        Connection::connect(&exec, "127.0.0.1", broker.port, options()),
    )
    .await
    .unwrap()
    .unwrap();
    let session = timeout(DEADLINE, connection.begin(SessionOptions::default()))
        .await
        .unwrap()
        .unwrap();

    // What this establishes: the **address syntax** the broker demanded and
    // the terminus it actually created. "An endpoint that cannot create
    // exactly the requested terminus MAY adjust properties but MUST then
    // report what it actually created", so the answering `attach` is where a
    // v1 address silently becoming a v2 one, or an expiry policy being
    // replaced, becomes visible.
    let link = timeout(
        DEADLINE,
        session.attach(LinkOptions::receiver("terminus-probe", Source::at(QUEUE))),
    )
    .await
    .unwrap()
    .expect("attached");
    let negotiated = link.negotiated().unwrap();
    println!(
        "MEASURED terminus: asked {QUEUE}, broker reported source {:?}, target {:?}",
        negotiated.remote_source, negotiated.remote_target
    );
    println!(
        "MEASURED capabilities offered by the broker: {:?}",
        negotiated.remote_offered_capabilities
    );

    let _ = timeout(DEADLINE, connection.close()).await;
    broker.stop();
}
