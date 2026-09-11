use agentio::{Agent, Error, IdentitySource};
use agentio::{ExchangeKind, TopicEntry, TopicRecordSpec};
use peerbus::{DatapodMsg, SecretKey, wire_type_hash};
use std::time::{Duration, Instant};

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

#[test]
fn failed_announcement_is_observable() {
    let unavailable = SecretKey::generate().public();
    let agent = Agent::builder()
        .identity(IdentitySource::Random)
        .bootstrap([unavailable])
        .allow_any_peer()
        .control_timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let _publisher = agent
        .publish::<peerbus::DatapodMsg>("/unreachable")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while agent.directory_health().announcement_failures == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(agent.directory_health().announcement_failures > 0);
}

#[test]
fn type_mismatch_is_rejected_before_dial() {
    let owner = SecretKey::generate();
    let agent = Agent::builder()
        .identity(IdentitySource::Random)
        .build()
        .unwrap();
    let record = TopicEntry::signed(
        TopicRecordSpec::new(
            "/wrong/type",
            ExchangeKind::ReqRes,
            wire_type_hash::<DatapodMsg>() ^ 1,
            Some(wire_type_hash::<DatapodMsg>()),
            1,
            None::<String>,
        ),
        &owner,
    )
    .unwrap();
    agent.directory().register(record).unwrap();
    assert!(matches!(
        agent.req_client::<DatapodMsg, DatapodMsg>("/wrong/type"),
        Err(Error::TypeMismatch { .. })
    ));
}

#[test]
fn reconciliation_honors_configured_caller_timeout() {
    let unavailable = SecretKey::generate().public();
    let agent = Agent::builder()
        .identity(IdentitySource::Random)
        .bootstrap([unavailable])
        .control_timeout(Duration::from_millis(50))
        .build()
        .unwrap();
    let started = Instant::now();
    assert!(matches!(
        agent.reconcile_now(),
        Err(Error::ControlTimeout(duration)) if duration == Duration::from_millis(50)
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(agent.directory_health().stale_seeds, 1);
}
