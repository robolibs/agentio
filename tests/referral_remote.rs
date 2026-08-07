use agentio::{Agent, DatapodMsg, DirectoryMode, RESOLUTION_TOPIC};
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
    let mut publisher = server.publish::<RemoteSample>("/remote/sample").unwrap();
    let client = Agent::builder()
        .identity(client_key)
        .directory(DirectoryMode::FrontDoor(server_id))
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

    let mut control = client
        .by_id(server_id)
        .unwrap()
        .req_client::<DatapodMsg, DatapodMsg>(RESOLUTION_TOPIC)
        .unwrap();
    let malformed = DatapodMsg::new(0, vec![0xff, 0xff]);
    let _ = control.call(&malformed);

    assert_eq!(client.reconcile_now().unwrap(), 1);
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

    let mut direct = client
        .by_id(server_id)
        .unwrap()
        .subscribe::<RemoteSample>("/remote/sample")
        .unwrap();
    let mut direct_received = None;
    for _ in 0..20 {
        publisher.send(&RemoteSample { sequence: 84 }).unwrap();
        if let Some(sample) = direct.recv_timeout(Duration::from_millis(250)).unwrap() {
            direct_received = Some(sample);
            break;
        }
    }
    assert_eq!(
        direct_received
            .expect("direct sample timed out")
            .header()
            .sequence,
        84
    );
}

#[test]
fn unlisted_forced_quic_client_is_rejected() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let server_id = server_key.public();
    let server = Agent::builder()
        .identity(server_key)
        .skip_shm()
        .build()
        .unwrap();
    let client = Agent::builder()
        .identity(client_key)
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
    let mut publisher = server.publish::<RemoteSample>("/remote/rejected").unwrap();
    let mut subscriber = client
        .node()
        .subscriber::<RemoteSample>(server.endpoint_addr(), "/remote/rejected")
        .unwrap();

    for sequence in 0..4 {
        publisher.send(&RemoteSample { sequence }).unwrap();
        assert!(
            subscriber
                .recv_timeout(Duration::from_millis(250))
                .unwrap()
                .is_none()
        );
    }
}
