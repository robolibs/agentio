# agentio

`agentio` is the agent composition and whole IO interface crate on top of [`peerbus`](../peerbus).

Every agent uses `agentio` to handle identity/key management (`did:key`), machine directory tracking, topic referral resolution, participant namespacing, and multi-machine agent composition.

## Core Concepts

- **Machine**: One `peerbus::Node` bound to one ed25519 key = one iroh `EndpointId` = one shared memory arena on one host.
- **Agent**: A logical set of Machines that belong together (e.g. robot components, compute nodes, server/edge PCs).
- **Participant**: A logical node name (e.g. `camera`, `planner`) hosted on a Machine.
- **Topic**: A `/`-namespaced path (e.g. `/perception/pose`).

## Referral Resolution

`agentio` resolves topics across an Agent composition via **referral**, not relay:
1. When Machine A requests a topic from Agent B, it dials Agent B's referral endpoint (`__agentio_resolve`) via peerbus req/res.
2. Agent B looks up the topic in its directory and returns the owner `(type_hash, EndpointId = Y)`.
3. Machine A subscribes/dials `Y` directly via `peerbus.subscriber(Y, topic)`. If co-located on the same host, peerbus uses zero-copy shared memory; otherwise it streams over iroh QUIC.

## Usage Example

```rust
use agentio::{Agent, DirectoryMode};
use datapod::datapod;

#[datapod]
struct Pose {
    pub x: f32,
    pub y: f32,
    pub yaw: f32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let agent = Agent::builder()
        .name("agent-1")
        .directory(DirectoryMode::Replicated)
        .build()?;

    // Publish topic
    let mut pubr = agent.publish::<Pose>("/perception/pose")?;

    // Subscribe by name (resolved within the Agent)
    let mut sub = agent.subscribe::<Pose>("/perception/pose")?;

    pubr.send(&Pose { x: 1.0, y: 2.0, yaw: 0.5 })?;

    // Direct escape hatch by EndpointId
    let _direct_sub = agent.by_id(agent.endpoint_id())?.subscribe::<Pose>("/perception/pose")?;

    Ok(())
}
```

## Examples

Run the provided examples:

```bash
cargo run --example 01_single_machine
cargo run --example 02_referral_resolution
cargo run --example 03_req_res_service
cargo run --example 04_by_id_escape
cargo run --example 05_all_exchanges
cargo run --example 06_server_all
cargo run --example 07_client_all -- <SERVER_DID_KEY> [--use-shm]
cargo run --example 08_heavy_server
cargo run --example 09_heavy_client -- <SERVER_DID_KEY> [--use-shm]
```
