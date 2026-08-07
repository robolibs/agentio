use agentio::Agent;
use peerbus::SecretKey;

#[test]
fn final_drop_releases_the_resolver_and_node() {
    let secret = SecretKey::generate();
    let first = Agent::builder()
        .identity(secret.clone())
        .allow_any_peer()
        .build()
        .unwrap();
    let clone = first.clone();

    drop(clone);
    assert_eq!(first.endpoint_id(), secret.public());
    drop(first);

    let second = Agent::builder()
        .identity(secret.clone())
        .allow_any_peer()
        .build()
        .unwrap();
    assert_eq!(second.endpoint_id(), secret.public());
}
