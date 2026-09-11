use agentio::{Agent, IdentitySource};
use datapod::datapod;
use std::time::Duration;

#[datapod]
struct StatusPing {
    pub seq: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let node1 = Agent::builder()
        .identity(IdentitySource::Random)
        .name("node-1")
        .allow_any_peer()
        .build()?;
    let node2 = Agent::builder()
        .identity(IdentitySource::Random)
        .name("node-2")
        .allow_any_peer()
        .bootstrap([node1.endpoint_id()])
        .build()?;

    // node1 publishes a raw topic
    let mut pub1 = node1.publish::<StatusPing>("/status")?;

    // node2 uses by_id escape hatch to dial node1 directly by EndpointId, skipping directory lookup
    let by_id = node2.by_id(node1.endpoint_id())?;
    let mut sub2 = by_id.subscribe::<StatusPing>("/status")?;

    let ping = StatusPing { seq: 999 };
    pub1.send(&ping)?;
    println!("Node 1 sent ping seq: {}", ping.seq);

    let sample = sub2
        .recv_timeout(Duration::from_secs(1))?
        .ok_or("status sample timed out")?;
    println!(
        "Node 2 received ping via by_id escape hatch: seq={}",
        sample.header().seq
    );

    Ok(())
}
