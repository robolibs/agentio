use agentio::Agent;
use datapod::datapod;
use std::thread;
use std::time::Duration;

#[datapod]
struct GetPoseRequest {
    pub frame_id: u32,
}

#[datapod]
struct GetPoseResponse {
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // Machine 1 hosting perception service
    let server_agent = Agent::builder()
        .name("perception-node")
        .build()?;

    // Machine 2 calling perception service
    let client_agent = Agent::builder()
        .name("planner-node")
        .bootstrap([server_agent.endpoint_id()])
        .build()?;

    // 1. Server registers req/res topic
    let mut server = server_agent.req_server::<GetPoseRequest, GetPoseResponse>("/perception/get_pose")?;

    thread::sleep(Duration::from_millis(100));

    // 2. Client resolves req/res topic and creates client
    let mut client = client_agent.req_client::<GetPoseRequest, GetPoseResponse>("/perception/get_pose")?;

    // Spawn thread to serve requests
    let server_handle = thread::spawn(move || -> Result<(), String> {
        for _ in 0..5 {
            if let Some((sample, reply)) = server.recv_timeout(Duration::from_millis(200)).map_err(|e| e.to_string())? {
                let req = sample.header();
                println!("Server received request for frame_id={}", req.frame_id);
                let resp = GetPoseResponse {
                    x: 10.0,
                    y: 20.0,
                    z: 5.0,
                };
                reply.respond(&resp).map_err(|e| e.to_string())?;
                break;
            }
        }
        Ok(())
    });

    // Client makes call
    let req = GetPoseRequest { frame_id: 42 };
    println!("Client calling /perception/get_pose with frame_id=42...");
    let response_sample = client.call(&req)?;
    let resp = response_sample.header();
    println!("Client received response: x={}, y={}, z={}", resp.x, resp.y, resp.z);

    server_handle.join().unwrap().map_err(|e| e)?;

    Ok(())
}
