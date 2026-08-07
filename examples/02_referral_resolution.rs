use agentio::{Agent, DirectoryMode};
use datapod::datapod;
use std::thread;
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
        .name("agent-arm")
        .directory(DirectoryMode::Replicated)
        .build()?;

    // Machine 2: "agent-head", bootstrapped with Machine 1's EndpointId
    let head_agent = Agent::builder()
        .name("agent-head")
        .directory(DirectoryMode::Replicated)
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

    // Allow time for resolution announce / discovery
    thread::sleep(Duration::from_millis(100));

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

    thread::sleep(Duration::from_millis(100));

    if let Some(sample) = head_sub.take()? {
        let rec = sample.header();
        println!(
            "Head resolved and received joint state: j1={}, j2={}, j3={}",
            rec.j1, rec.j2, rec.j3
        );
    } else {
        println!("Head did not receive sample.");
    }

    Ok(())
}
