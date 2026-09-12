use agentio::{Agent, ExchangeKind};
use datapod::datapod;
use peerbus::{SecretKey, wire_type_hash};
use std::time::{Duration, Instant};

#[datapod(name = "agentio.reconciled_sample.v1")]
struct ReconciledSample {
    value: u64,
}

#[test]
fn late_joiner_learns_a_missed_announcement() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let server_id = server_key.public();
    let client_id = client_key.public();
    let server = Agent::builder()
        .identity(server_key)
        .name("server")
        .allow_peer(client_id)
        .build()
        .unwrap();
    let _publisher = server
        .publish::<ReconciledSample>("/reconcile/missed")
        .unwrap();
    let client = Agent::builder()
        .identity(client_key)
        .name("client")
        .bootstrap([server_id])
        .allow_peer(server_id)
        .build()
        .unwrap();

    assert!(client.reconcile_now().unwrap() <= 1);
    let entry = client
        .directory()
        .lookup_exchange("/reconcile/missed", ExchangeKind::PubSub)
        .unwrap();
    assert_eq!(
        entry.request_type_hash(),
        wire_type_hash::<ReconciledSample>()
    );
    assert_eq!(client.name_table().resolve_name("server"), Some(server_id));
}

#[test]
fn startup_reconciliation_learns_existing_records() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let server_id = server_key.public();
    let client_id = client_key.public();
    let server = Agent::builder()
        .identity(server_key)
        .allow_peer(client_id)
        .build()
        .unwrap();
    let _publisher = server
        .publish::<ReconciledSample>("/reconcile/startup")
        .unwrap();
    let client = Agent::builder()
        .identity(client_key)
        .bootstrap([server_id])
        .allow_peer(server_id)
        .build()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    while client
        .directory()
        .lookup_exchange("/reconcile/startup", ExchangeKind::PubSub)
        .is_none()
        && Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert!(
        client
            .directory()
            .lookup_exchange("/reconcile/startup", ExchangeKind::PubSub)
            .is_some()
    );
}

#[test]
fn healthy_seed_reconciles_when_another_seed_is_stale() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let server_id = server_key.public();
    let client_id = client_key.public();
    let stale_id = SecretKey::generate().public();
    let server = Agent::builder()
        .identity(server_key)
        .allow_peer(client_id)
        .build()
        .unwrap();
    let _publisher = server
        .publish::<ReconciledSample>("/reconcile/healthy")
        .unwrap();
    let client = Agent::builder()
        .identity(client_key)
        .name("client")
        .bootstrap([stale_id, server_id])
        .allow_peer(server_id)
        .build()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(7);
    while (client
        .directory()
        .lookup_exchange("/reconcile/healthy", ExchangeKind::PubSub)
        .is_none()
        || client.directory_health().stale_seeds != 1)
        && Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert!(
        client
            .directory()
            .lookup_exchange("/reconcile/healthy", ExchangeKind::PubSub)
            .is_some()
    );
    assert_eq!(client.directory_health().stale_seeds, 1);
}

#[test]
fn dropping_hosted_handle_withdraws_from_seed() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let server_id = server_key.public();
    let client_id = client_key.public();
    let client = Agent::builder()
        .identity(client_key)
        .bootstrap([server_id])
        .allow_peer(server_id)
        .build()
        .unwrap();
    let server = Agent::builder()
        .identity(server_key)
        .name("server")
        .bootstrap([client_id])
        .allow_peer(client_id)
        .build()
        .unwrap();
    let publisher = server
        .publish::<ReconciledSample>("/reconcile/withdraw")
        .unwrap();
    client.reconcile_now().unwrap();
    assert!(
        client
            .directory()
            .lookup_exchange("/reconcile/withdraw", ExchangeKind::PubSub)
            .is_some()
    );
    assert_eq!(client.name_table().resolve_name("server"), Some(server_id));

    drop(publisher);
    let deadline = Instant::now() + Duration::from_secs(5);
    while client
        .directory()
        .lookup_exchange("/reconcile/withdraw", ExchangeKind::PubSub)
        .is_some()
        && Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert!(
        client
            .directory()
            .lookup_exchange("/reconcile/withdraw", ExchangeKind::PubSub)
            .is_none()
    );
    assert_eq!(client.name_table().resolve_name("server"), None);
}

#[test]
fn hosted_record_renewal_advances_revision() {
    let agent = Agent::builder().allow_any_peer().build().unwrap();
    let _publisher = agent
        .publish::<ReconciledSample>("/reconcile/renew")
        .unwrap();
    let before = agent
        .directory()
        .lookup_exchange("/reconcile/renew", ExchangeKind::PubSub)
        .unwrap();

    assert_eq!(agent.renew_now(), 1);
    let after = agent
        .directory()
        .lookup_exchange("/reconcile/renew", ExchangeKind::PubSub)
        .unwrap();
    assert!(after.revision() > before.revision());
    assert!(after.lease_expires_at_ms() >= before.lease_expires_at_ms());
}

#[test]
fn hosted_record_renews_automatically() {
    let agent = Agent::builder()
        .allow_any_peer()
        .lease_duration(Duration::from_millis(150))
        .build()
        .unwrap();
    let _publisher = agent
        .publish::<ReconciledSample>("/reconcile/automatic-renew")
        .unwrap();
    let before = agent
        .directory()
        .lookup_exchange("/reconcile/automatic-renew", ExchangeKind::PubSub)
        .unwrap()
        .revision();
    let deadline = Instant::now() + Duration::from_secs(1);
    while agent
        .directory()
        .lookup_exchange("/reconcile/automatic-renew", ExchangeKind::PubSub)
        .is_some_and(|entry| entry.revision() == before)
        && Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert!(
        agent
            .directory()
            .lookup_exchange("/reconcile/automatic-renew", ExchangeKind::PubSub)
            .is_some_and(|entry| entry.revision() > before)
    );
}

#[test]
fn crashed_owner_record_expires() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let server_id = server_key.public();
    let client_id = client_key.public();
    let server = Agent::builder()
        .identity(server_key)
        .name("crashed")
        .allow_peer(client_id)
        .lease_duration(Duration::from_millis(150))
        .build()
        .unwrap();
    let publisher = server
        .publish::<ReconciledSample>("/reconcile/crash-expiry")
        .unwrap();
    let client = Agent::builder()
        .identity(client_key)
        .bootstrap([server_id])
        .allow_peer(server_id)
        .build()
        .unwrap();
    client.reconcile_now().unwrap();
    drop(server);

    let deadline = Instant::now() + Duration::from_secs(1);
    while client
        .directory()
        .lookup_exchange("/reconcile/crash-expiry", ExchangeKind::PubSub)
        .is_some()
        && Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert!(
        client
            .directory()
            .lookup_exchange("/reconcile/crash-expiry", ExchangeKind::PubSub)
            .is_none()
    );
    assert_eq!(client.name_table().resolve_name("crashed"), None);
    drop(publisher);
}

#[test]
fn persistent_owner_can_publish_after_restart() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let server_id = server_key.public();
    let client_id = client_key.public();
    let server = Agent::builder()
        .identity(server_key.clone())
        .allow_peer(client_id)
        .build()
        .unwrap();
    let first_publisher = server
        .publish::<ReconciledSample>("/reconcile/restart")
        .unwrap();
    let client = Agent::builder()
        .identity(client_key)
        .bootstrap([server_id])
        .allow_peer(server_id)
        .lease_duration(Duration::from_millis(150))
        .build()
        .unwrap();
    client.reconcile_now().unwrap();
    let before = client
        .directory()
        .lookup_exchange("/reconcile/restart", ExchangeKind::PubSub)
        .unwrap()
        .revision();
    drop(server);

    let restarted = Agent::builder()
        .identity(server_key)
        .allow_peer(client_id)
        .build()
        .unwrap();
    let _second_publisher = restarted
        .publish::<ReconciledSample>("/reconcile/restart")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(7);
    let after = loop {
        let revision = client
            .directory()
            .lookup_exchange("/reconcile/restart", ExchangeKind::PubSub)
            .unwrap()
            .revision();
        if revision > before {
            break revision;
        }
        assert!(Instant::now() < deadline, "restart did not reconcile");
        std::thread::yield_now();
    };
    assert!(after > before);
    drop(first_publisher);
}

mod adoption {
    use agentio::{Agent, DirectoryMode, ExchangeKind, IdentitySource};
    use peerbus::DatapodMsg;
    use std::time::{Duration, Instant};

    fn host(lease: Duration) -> Agent {
        Agent::builder()
            .identity(IdentitySource::Random)
            .name("raw-host")
            .directory(DirectoryMode::Replicated)
            .allow_any_peer()
            .lease_duration(lease)
            .build()
            .unwrap()
    }

    /// A topic opened on the raw peerbus node becomes a signed record on
    /// demand, and a peer resolves it by name like any hosted topic.
    #[test]
    fn raw_node_topics_are_adopted_on_demand() {
        let host = host(Duration::from_secs(30));
        // The maintenance tick adopts at start-up and every lease third;
        // open the raw topic between ticks.
        std::thread::sleep(Duration::from_millis(300));
        let mut raw = host.node().publisher::<DatapodMsg>("/raw/feed").unwrap();
        assert!(host.resolve_topic("/raw/feed").is_err());
        assert_eq!(host.adopt_hosted_topics().unwrap(), 1);
        assert_eq!(host.adopt_hosted_topics().unwrap(), 0);
        let entry = host.resolve_topic("/raw/feed").unwrap();
        assert_eq!(entry.exchange(), ExchangeKind::PubSub);
        assert_eq!(entry.endpoint_id(), host.endpoint_id());

        let client = Agent::builder()
            .identity(IdentitySource::Random)
            .name("raw-client")
            .directory(DirectoryMode::FrontDoor(host.endpoint_id()))
            .bootstrap([host.endpoint_id()])
            .allow_any_peer()
            .build()
            .unwrap();
        let mut feed = client.subscribe::<DatapodMsg>("/raw/feed").unwrap();
        let sent = DatapodMsg::new(7, vec![1, 2, 3]);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut got = None;
        while got.is_none() && Instant::now() < deadline {
            raw.send(&sent).unwrap();
            got = feed.recv_timeout(Duration::from_millis(100)).unwrap();
        }
        let sample = got.expect("the adopted topic delivers");
        assert_eq!(sample.header().type_hash, 7);
        assert_eq!(sample.payload(), &[1, 2, 3]);
    }

    /// Without any call, the maintenance tick adopts raw topics within a
    /// lease period.
    #[test]
    fn raw_node_topics_are_adopted_by_maintenance() {
        let host = host(Duration::from_millis(300));
        let _raw = host.node().publisher::<DatapodMsg>("/raw/tick").unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while host.resolve_topic("/raw/tick").is_err() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(host.resolve_topic("/raw/tick").is_ok());
    }
}

mod participants {
    use agentio::{Agent, DirectoryMode, Error, IdentitySource};
    use peerbus::DatapodMsg;
    use std::time::{Duration, Instant};

    /// `publish_own` hosts under the builder's participant prefix, which a
    /// peer reaches through `subscribe_in`.
    #[test]
    fn own_topics_carry_the_participant_prefix() {
        let lidar = Agent::builder()
            .identity(IdentitySource::Random)
            .name("lidar-agent")
            .participant("/lidar/")
            .directory(DirectoryMode::Replicated)
            .allow_any_peer()
            .build()
            .unwrap();
        assert_eq!(lidar.participant(), Some("lidar"));
        assert_eq!(lidar.own_topic("scan").unwrap(), "/lidar/scan");
        assert_eq!(lidar.own_topic("/clock").unwrap(), "/clock");
        let mut scans = lidar.publish_own::<DatapodMsg>("scan").unwrap();
        assert_eq!(
            lidar.resolve_topic("/lidar/scan").unwrap().endpoint_id(),
            lidar.endpoint_id()
        );
        let mut clock = lidar.publish_in::<DatapodMsg>("global", "clock").unwrap();
        assert!(lidar.resolve_topic("/global/clock").is_ok());

        let reader = Agent::builder()
            .identity(IdentitySource::Random)
            .name("reader")
            .directory(DirectoryMode::FrontDoor(lidar.endpoint_id()))
            .bootstrap([lidar.endpoint_id()])
            .allow_any_peer()
            .build()
            .unwrap();
        assert!(matches!(
            reader.own_topic("scan"),
            Err(Error::Configuration(_))
        ));
        let mut feed = reader.subscribe_in::<DatapodMsg>("lidar", "scan").unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut got = None;
        while got.is_none() && Instant::now() < deadline {
            scans.send(&DatapodMsg::new(1, vec![9])).unwrap();
            clock.send(&DatapodMsg::new(2, vec![0])).unwrap();
            got = feed.recv_timeout(Duration::from_millis(100)).unwrap();
        }
        assert_eq!(got.expect("scan delivered").header().type_hash, 1);
    }
}
