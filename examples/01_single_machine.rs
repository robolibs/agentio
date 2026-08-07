use agentio::Agent;
use datapod::datapod;

#[datapod]
struct Pose {
    pub x: f32,
    pub y: f32,
    pub yaw: f32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // 1. Create a single-machine Agent instance with an ephemeral key
    let agent = Agent::builder().name("agent-single").build()?;

    println!("Agent initialized!");
    println!("  Machine name: {}", agent.name());
    println!("  Endpoint ID:  {}", agent.endpoint_id());
    println!("  DID:KEY:      {}", agent.did_key()?);

    // 2. Publish a topic ("/perception/pose")
    let mut publisher = agent.publish::<Pose>("/perception/pose")?;

    // 3. Subscribe to the topic by name (resolved within the Agent)
    let mut subscriber = agent.subscribe::<Pose>("/perception/pose")?;

    // 4. Send a payload
    let pose_sent = Pose {
        x: 1.25,
        y: 3.50,
        yaw: 0.78,
    };
    publisher.send(&pose_sent)?;
    println!(
        "Sent pose: x={}, y={}, yaw={}",
        pose_sent.x, pose_sent.y, pose_sent.yaw
    );

    // 5. Receive payload
    std::thread::sleep(std::time::Duration::from_millis(50));
    if let Some(sample) = subscriber.take()? {
        let pose_received = sample.header();
        println!(
            "Received pose: x={}, y={}, yaw={}",
            pose_received.x, pose_received.y, pose_received.yaw
        );
        assert_eq!(pose_received.x, pose_sent.x);
    } else {
        println!("No sample received.");
    }

    Ok(())
}
