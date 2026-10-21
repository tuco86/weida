//! The tests of `tests/raw_streams.rs` that need the network, a process or real time.

use crate::common;
use common::Server;

use crate::raw_streams::*;

/// A weida binding does not advertise `max_datagram_frame_size`, so a peer
/// may send it no datagrams and quinn buffers none (0034 §2.3).
#[tokio::test]
async fn a_weida_binding_advertises_no_datagrams() {
    let server = Server::start_os().await;
    let endpoint = common::raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    assert_eq!(conn.max_datagram_size(), None);
}
