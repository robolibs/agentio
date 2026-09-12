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
