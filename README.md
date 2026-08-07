# agentio

`agentio` is a Rust library for composing authenticated `peerbus` endpoints by
topic name. One `Agent` owns one `peerbus::Node`, one Ed25519 identity, a local
directory, and its control-plane workers. A multi-machine composition consists
of several Agents that exchange signed directory records.

## Model

- A **Machine** is one endpoint identity and its `peerbus::Node`.
- An **Agent** is the owning handle for that Machine's IO and directory state.
- A **topic** is a normalized absolute path such as `/perception/pose`.
- `qualify_participant_topic` and `subscribe_in` provide local topic-prefix
  convenience. There is no participant registry or participant discovery.

Each hosted exchange is recorded with its topic, exchange family, request and
response type hashes, owner endpoint, revision, lease expiry, machine name, and
owner signature. Received announcements and snapshots are verified before
insertion. Older revisions, expired records, invalid signatures, type
mismatches, and competing live owners are rejected. Hosted records are renewed;
dropping the returned `Registered` publisher/server handle sends a signed
withdrawal. A missed announcement can be recovered with `reconcile_now` or the
periodic reconciliation worker.

## Membership and authorization

Discovery and authorization are separate:

- `bootstrap([...])` configures outbound directory seeds only.
- `allow_peer(id)` and `allow_peers([...])` configure inbound transport access.
- `allow_any_peer()` explicitly opts out of the deny-by-default allowlist.
- `DirectoryMode::Replicated` reconciles every configured seed.
- `DirectoryMode::FrontDoor(id)` uses one directory authority. The front-door
  ID is not automatically authorized for inbound transport.

For production, exchange endpoint IDs through a trusted channel and use
explicit allowlists. `allow_any_peer` is appropriate only for a deliberately
permissive trust boundary. An ALPN identifies a protocol; it is not a secret or
an authorization mechanism.

## Resolution and transport

A typed client first checks its verified local directory and can query a seed's
reserved `__agentio_resolve` req/res service. It validates the returned topic,
exchange family, and type hashes before dialing the owner directly. `by_id`
bypasses directory resolution but not peerbus transport authorization.

Peerbus can use shared memory for co-located endpoints and iroh QUIC for remote
endpoints. `skip_shm()` forces QUIC; `no_relay()` disables relay fallback.
`wait_for_direct_addresses` provides bounded address readiness for remote
setups.

## Identities

`IdentitySource::Random` creates a non-persistent identity. Named, default, DID,
and explicit-file sources persist 32-byte keys with owner-only Unix permissions
and fail on malformed or insecure existing files. DID-index copy failures are
returned to the caller. A human-readable name is never converted into private
key material. See
[`docs/migrations/identity-derived-name.md`](docs/migrations/identity-derived-name.md)
when migrating from the removed name-derived API.

## Minimal example

```rust
use agentio::{Agent, IdentitySource};
use datapod::datapod;
use std::time::Duration;

#[datapod]
struct Pose {
    x: f32,
    y: f32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let agent = Agent::builder()
        .identity(IdentitySource::Random)
        .name("local")
        .build()?;
    let mut publisher = agent.publish::<Pose>("/pose")?;
    let mut subscriber = agent.subscribe::<Pose>("/pose")?;

    publisher.send(&Pose { x: 1.0, y: 2.0 })?;
    let sample = subscriber
        .recv_timeout(Duration::from_secs(1))?
        .ok_or("pose timed out")?;
    assert_eq!(sample.header().x, 1.0);
    Ok(())
}
```

## Lifecycle and health

`Agent` clones share one inner node. Dropping a non-final clone changes nothing;
dropping the final clone signals and joins both control workers before the node
is released. Publisher and server wrappers withdraw their records on drop while
the Agent remains live.

`directory_health()` snapshots announcement successes/failures, resolver
errors, rejected records, last successful reconciliation, stale seed count,
conflicts, and pending announcements. Configuration, signature, expiry,
ownership, exchange, type, batch, and stale-revision failures are typed `Error`
variants.

## Commands

The Makefile is the supported command surface:

```bash
make run
make run EXAMPLE=05_all_exchanges
make run EXAMPLE=07_client_all ARGS='<SERVER_DID_KEY>'
make integration
make remote-test
make examples-smoke
make verify
```

Examples 06/07 are the two-process exchange demos. Examples 08/09 are
throughput demos that measure actual received payload bytes and validate their
content; they are not repeatable statistical benchmarks.

`agentio` is library-only. CI builds, tests, lints, and documents the library;
it does not package a nonexistent application binary. Source releases remain a
manual maintainer operation.
