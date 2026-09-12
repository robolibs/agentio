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

mod restart {
    use agentio::{Agent, DirectoryMode, IdentitySource};
    use datapod::datapod;
    use peerbus::SecretKey;
    use std::time::Duration;

    #[datapod(name = "agentio.test.ping.v1")]
    struct Ping {
        pub seq: u64,
    }

    #[datapod(name = "agentio.test.pong.v1")]
    struct Pong {
        pub seq: u64,
    }

    const TOPIC: &str = "/test/restart/ping";

    /// A host that restarts binds the same topic; the requests its
    /// predecessor already answered must not reach it.
    #[test]
    fn rebound_request_server_ignores_its_predecessors_traffic() {
        let host_secret = SecretKey::generate();
        let host = Agent::builder()
            .identity(host_secret.clone())
            .name("host")
            .directory(DirectoryMode::Replicated)
            .allow_any_peer()
            .build()
            .unwrap();
        let client = Agent::builder()
            .identity(IdentitySource::Random)
            .name("client")
            .directory(DirectoryMode::Replicated)
            .allow_any_peer()
            .bootstrap([host.endpoint_id()])
            .build()
            .unwrap();
        let mut server = host.req_server::<Ping, Pong>(TOPIC).unwrap();
        let mut caller = client.req_client::<Ping, Pong>(TOPIC).unwrap();

        let answer = std::thread::spawn(move || {
            let (sample, reply) = server
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .expect("the first host sees the request");
            let seq = sample.header().seq;
            reply.respond(&Pong { seq }).unwrap();
            server
        });
        let pong = caller.call(&Ping { seq: 7 }).unwrap();
        assert_eq!(pong.header().seq, 7);
        // A host killed by a signal never unlinks its rings: the next host
        // attaches to the same segment and its history. Leak the first
        // host the same way.
        std::mem::forget(answer.join().unwrap());
        std::mem::forget(host);

        let restarted = Agent::builder()
            .identity(host_secret)
            .name("host")
            .directory(DirectoryMode::Replicated)
            .allow_any_peer()
            .build()
            .unwrap();
        let mut server = restarted.req_server::<Ping, Pong>(TOPIC).unwrap();
        let stale = server.recv_timeout(Duration::from_millis(300)).unwrap();
        assert!(
            stale.is_none(),
            "the restarted host was handed a request answered by its predecessor"
        );
    }
}

mod rendezvous {
    use agentio::{Agent, DirectoryMode, IdentitySource, find_local, local_agents, rendezvous_dir};

    /// A built agent is findable by name on this host, can seed a peer, and
    /// vanishes with its drop; a record of a dead process is pruned.
    #[test]
    fn live_agents_are_found_by_name_and_pruned_when_gone() {
        let dir = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var("AGENTIO_RENDEZVOUS_DIR", dir.path()) };
        assert_eq!(rendezvous_dir(), dir.path());
        std::fs::write(
            dir.path().join("dead.json"),
            r#"{"name":"ghost","participant":null,"did":"did:key:z6Mkghost","addr":"00","pid":4294967295,"started_unix_ms":0}"#,
        )
        .unwrap();

        let host = Agent::builder()
            .identity(IdentitySource::Random)
            .name("rendezvous-host")
            .participant("sim")
            .directory(DirectoryMode::Replicated)
            .allow_any_peer()
            .build()
            .unwrap();
        assert!(host.rendezvous_path().is_some_and(|p| p.exists()));
        let found = find_local("rendezvous-host").expect("the live host is listed");
        assert_eq!(found.endpoint_id().unwrap(), host.endpoint_id());
        assert_eq!(found.participant.as_deref(), Some("sim"));
        assert_eq!(found.pid, std::process::id());
        assert_eq!(found.endpoint_addr().unwrap().id, host.endpoint_id());
        assert!(find_local("ghost").is_none());
        assert!(!dir.path().join("dead.json").exists());

        let client = Agent::builder()
            .identity(IdentitySource::Random)
            .bootstrap([&found])
            .allow_any_peer()
            .no_rendezvous()
            .build()
            .unwrap();
        assert_eq!(client.bootstrap_peers(), &[host.endpoint_id()]);
        assert!(client.rendezvous_path().is_none());
        assert_eq!(local_agents().len(), 1);

        let path = host.rendezvous_path().unwrap().to_path_buf();
        drop(host);
        assert!(!path.exists());
        assert!(find_local("rendezvous-host").is_none());
        unsafe { std::env::remove_var("AGENTIO_RENDEZVOUS_DIR") };
    }
}
