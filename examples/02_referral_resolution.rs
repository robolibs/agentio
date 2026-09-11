use agentio::{Agent, DirectoryMode, IdentitySource};
use datapod::datapod;
use std::time::Duration;

#[datapod]
struct JointState {
    pub j1: f32,
    pub j2: f32,
    pub j3: f32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // Machine 1: "agent-arm"
    let arm_agent = Agent::builder()
        .identity(IdentitySource::Random)
        .name("agent-arm")
        .directory(DirectoryMode::Replicated)
        .allow_any_peer()
        .build()?;

    // Machine 2: "agent-head", bootstrapped with Machine 1's EndpointId
    let head_agent = Agent::builder()
        .identity(IdentitySource::Random)
        .name("agent-head")
        .directory(DirectoryMode::Replicated)
        .allow_any_peer()
        .bootstrap([arm_agent.endpoint_id()])
        .build()?;

    println!(
        "Machine 1 (arm):  {} ({})",
        arm_agent.name(),
        arm_agent.endpoint_id()
    );
    println!(
        "Machine 2 (head): {} ({})",
        head_agent.name(),
        head_agent.endpoint_id()
    );

    // "agent-arm" publishes "/arm/joints"
    let mut arm_pub = arm_agent.publish::<JointState>("/arm/joints")?;

    head_agent.reconcile_now()?;

    // "agent-head" resolves "/arm/joints" via referral to "agent-arm" and subscribes
    let mut head_sub = head_agent.subscribe::<JointState>("/arm/joints")?;

    // Send payload from arm
    let state_sent = JointState {
        j1: 0.5,
        j2: -1.2,
        j3: std::f32::consts::PI,
    };
    arm_pub.send(&state_sent)?;
    println!(
        "Arm published joint state: j1={}, j2={}, j3={}",
        state_sent.j1, state_sent.j2, state_sent.j3
    );

    let sample = head_sub
        .recv_timeout(Duration::from_secs(1))?
        .ok_or("joint-state sample timed out")?;
    let rec = sample.header();
    println!(
        "Head resolved and received joint state: j1={}, j2={}, j3={}",
        rec.j1, rec.j2, rec.j3
    );

    Ok(())
}
