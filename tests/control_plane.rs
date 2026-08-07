use agentio::{Agent, Error, IdentitySource};
use peerbus::SecretKey;

#[test]
fn invalid_bootstrap_is_a_configuration_error() {
    let result = Agent::builder()
        .identity(IdentitySource::Random)
        .bootstrap(["not-an-endpoint"])
        .build();

    assert!(matches!(result, Err(Error::Configuration(_))));
}

#[test]
fn invalid_allowlist_entry_is_a_configuration_error() {
    let result = Agent::builder()
        .identity(IdentitySource::Random)
        .allow_peer("not-an-endpoint")
        .build();

    assert!(matches!(result, Err(Error::Configuration(_))));
}

#[test]
fn bootstrap_and_inbound_allowlist_are_independent() {
    let seed = SecretKey::generate().public();
    let allowed = SecretKey::generate().public();
    let agent = Agent::builder()
        .identity(IdentitySource::Random)
        .bootstrap([seed])
        .allow_peer(allowed)
        .build()
        .unwrap();

    assert_eq!(agent.bootstrap_peers(), &[seed]);
    assert_eq!(agent.allowed_peers(), &[allowed]);
    assert!(!agent.allows_any_peer());
}
