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

mod candidates {
    use agentio::{Agent, DirectoryMode, IdentitySource};
    use datapod::datapod;
    use std::time::Duration;

    #[datapod(name = "agentio.test.reading_a.v1")]
    struct ReadingA {
        pub value: u32,
    }

    #[datapod(name = "agentio.test.reading_b.v1")]
    struct ReadingB {
        pub value: u64,
    }

    const TOPIC: &str = "/candidates/reading";

    /// The first seed answers with a record of the wrong type; the query
    /// must go on and take the second seed's record once it exists.
    #[test]
    fn query_continues_past_a_bad_candidate() {
        let seed = |name: &str| {
            Agent::builder()
                .identity(IdentitySource::Random)
                .name(name)
                .directory(DirectoryMode::Replicated)
                .allow_any_peer()
                .build()
                .unwrap()
        };
        let wrong = seed("wrong-seed");
        let right = seed("right-seed");
        let _wrong_publisher = wrong.publish::<ReadingA>(TOPIC).unwrap();
        // Polled first: the query walks its candidates from the last one.
        let client = Agent::builder()
            .identity(IdentitySource::Random)
            .name("client")
            .directory(DirectoryMode::Replicated)
            .allow_any_peer()
            .bootstrap([right.endpoint_id(), wrong.endpoint_id()])
            .control_timeout(Duration::from_secs(4))
            .build()
            .unwrap();
        let resolving = std::thread::spawn(move || client.subscribe::<ReadingB>(TOPIC).map(|_| ()));
        std::thread::sleep(Duration::from_millis(400));
        let _right_publisher = right.publish::<ReadingB>(TOPIC).unwrap();
        let outcome = resolving.join().unwrap();
        assert!(
            outcome.is_ok(),
            "query gave up after the bad candidate: {outcome:?}"
        );
    }
}
