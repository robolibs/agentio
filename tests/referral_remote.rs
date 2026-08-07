use agentio::Agent;
use datapod::datapod;
use peerbus::SecretKey;
use std::time::Duration;

#[datapod(name = "agentio.remote_sample.v1")]
struct RemoteSample {
    sequence: u64,
}

#[test]
fn resolves_and_transfers_over_forced_quic() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let server_id = server_key.public();
    let client_id = client_key.public();

    let server = Agent::builder()
        .identity(server_key)
        .allow_peer(client_id)
        .skip_shm()
        .build()
        .unwrap();
    let client = Agent::builder()
        .identity(client_key)
        .bootstrap([server_id])
        .allow_peer(server_id)
        .skip_shm()
        .build()
        .unwrap();

    server
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();
    client
        .wait_for_direct_addresses(Duration::from_secs(5))
        .unwrap();

    let mut publisher = server.publish::<RemoteSample>("/remote/sample").unwrap();
    let mut subscriber = client.subscribe::<RemoteSample>("/remote/sample").unwrap();

    let mut received = None;
    for _ in 0..20 {
        publisher.send(&RemoteSample { sequence: 42 }).unwrap();
        if let Some(sample) = subscriber.recv_timeout(Duration::from_millis(250)).unwrap() {
            received = Some(sample);
            break;
        }
    }
    let received = received.expect("remote sample timed out");
    assert_eq!(received.header().sequence, 42);
}
