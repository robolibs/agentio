use agentio::{Agent, ExchangeKind};
use datapod::datapod;
use peerbus::{SecretKey, wire_type_hash};

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

    assert_eq!(client.reconcile_now().unwrap(), 1);
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

    assert_eq!(client.reconcile_now().unwrap(), 1);
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
