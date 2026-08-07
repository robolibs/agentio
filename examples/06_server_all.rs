use agentio::{Agent, DirectoryMode, IdentitySource};
use datapod::datapod;
use std::env;
use std::thread;
use std::time::Duration;

// 1. Pub/Sub Payload
#[datapod(name = "agentio.telemetry.v1")]
struct Telemetry {
    pub seq: u64,
    pub val: f32,
}

// 2. Req/Res Payloads
#[datapod(name = "agentio.math_req.v1")]
struct MathReq {
    pub x: i32,
    pub y: i32,
}

#[datapod(name = "agentio.math_res.v1")]
struct MathRes {
    pub sum: i32,
}

// 3. Que/Ans Payloads
#[datapod(name = "agentio.range_query.v1")]
struct RangeQuery {
    pub start: i32,
    pub count: u32,
}

#[datapod(name = "agentio.range_hit.v1")]
struct RangeHit {
    pub value: i32,
}

// 4. Put/Ack Payloads
#[datapod(name = "agentio.data_block.v1")]
struct DataBlock {
    pub bytes_count: u32,
}

#[datapod(name = "agentio.upload_result.v1")]
struct UploadResult {
    pub total_blocks: u32,
    pub total_bytes: u32,
}

// 5. Pip Streaming Payloads
#[datapod(name = "agentio.audio_chunk.v1")]
struct AudioChunk {
    pub sample_id: u32,
}

#[datapod(name = "agentio.audio_feedback.v1")]
struct AudioFeedback {
    pub echo_id: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    let use_shm = args.iter().any(|a| a == "--use-shm");

    println!("============================================================");
    println!("          agentio: Standalone Server (All 5 Services)       ");
    println!("============================================================");

    let mut builder = Agent::builder()
        .name("standalone-server")
        .identity(IdentitySource::Random)
        .directory(DirectoryMode::Replicated)
        .allow_any_peer();

    if !use_shm {
        println!("  [*] Transport: Pure Network Sockets / QUIC (skip_shm = true)");
        builder = builder.skip_shm();
    } else {
        println!("  [*] Transport: Local Shared Memory (SHM)");
    }

    let agent = builder.build()?;
    let _ = agent.wait_for_direct_addresses(Duration::from_secs(5));

    let endpoint_id = agent.endpoint_id();
    let did_key = agent.did_key()?;

    println!("\n  Server Agent Initialized!");
    println!("  -> Endpoint ID: {}", endpoint_id);
    println!("  -> DID:KEY:     {}", did_key);
    println!("  -> Addr:        {:?}\n", agent.endpoint_addr());

    println!("  Run the client from another terminal or computer using:");
    println!("    cargo run --example 07_client_all -- {}", did_key);
    if use_shm {
        println!(
            "    cargo run --example 07_client_all -- {} --use-shm",
            did_key
        );
    }
    println!("============================================================\n");

    // 1. Pub/Sub Publisher
    let mut telemetry_pub = agent.publish::<Telemetry>("/telemetry")?;
    thread::spawn(move || {
        let mut seq = 1u64;
        loop {
            let _ = telemetry_pub.send(&Telemetry {
                seq,
                val: 100.0 + (seq as f32) * 0.5,
            });
            seq += 1;
            thread::sleep(Duration::from_millis(500));
        }
    });

    // 2. Req/Res Server
    let mut req_srv = agent.req_server::<MathReq, MathRes>("/service/add")?;
    thread::spawn(move || {
        loop {
            match req_srv.recv_timeout(Duration::from_millis(100)) {
                Ok(Some((sample, reply))) => {
                    let req = sample.header();
                    println!("  [Req/Res] Serving MathReq: {} + {}", req.x, req.y);
                    if let Err(e) = reply.respond(&MathRes { sum: req.x + req.y }) {
                        eprintln!("  [Req/Res] Failed to send response: {}", e);
                    }
                }
                Ok(None) => {}
                Err(e) => eprintln!("  [Req/Res] Server error: {}", e),
            }
        }
    });

    // 3. Que/Ans Server
    let mut que_srv = agent.que_server::<RangeQuery, RangeHit>("/query/range")?;
    thread::spawn(move || {
        loop {
            match que_srv.recv_timeout(Duration::from_millis(100)) {
                Ok(Some((que_sample, mut ans_sender))) => {
                    let q = que_sample.header();
                    println!(
                        "  [Que/Ans] Serving RangeQuery: start={}, count={}",
                        q.start, q.count
                    );
                    for offset in 0..q.count {
                        let _ = ans_sender.send(&RangeHit {
                            value: q.start + offset as i32,
                        });
                    }
                    let _ = ans_sender.finish();
                }
                Ok(None) => {}
                Err(e) => eprintln!("  [Que/Ans] Server error: {}", e),
            }
        }
    });

    // 4. Put/Ack Server
    let mut put_srv = agent.put_server::<DataBlock, UploadResult>("/upload/blocks")?;
    thread::spawn(move || {
        loop {
            match put_srv.recv_timeout(Duration::from_millis(100)) {
                Ok(Some(mut puts_recv)) => {
                    println!("  [Put/Ack] Receiving data blocks stream...");
                    let mut blocks = 0;
                    let mut total_bytes = 0;
                    while let Ok(Some(block)) = puts_recv.next() {
                        blocks += 1;
                        total_bytes += block.header().bytes_count;
                    }
                    println!(
                        "  [Put/Ack] Finished upload. Acking blocks={}, bytes={}",
                        blocks, total_bytes
                    );
                    let _ = puts_recv.ack(&UploadResult {
                        total_blocks: blocks,
                        total_bytes,
                    });
                }
                Ok(None) => {}
                Err(e) => eprintln!("  [Put/Ack] Server error: {}", e),
            }
        }
    });

    // 5. Pip Streaming Server
    let mut pip_srv = agent.pip_server::<AudioChunk, AudioFeedback>("/stream/audio")?;
    thread::spawn(move || {
        loop {
            match pip_srv.recv_timeout(Duration::from_millis(100)) {
                Ok(Some(mut pip_session)) => {
                    println!("  [Pip] Opening bidirectional streaming session...");
                    while let Ok(Some(chunk)) = pip_session.next() {
                        let sample_id = chunk.header().sample_id;
                        println!("  [Pip] Streamed chunk sample_id={}", sample_id);
                        let _ = pip_session.send(&AudioFeedback {
                            echo_id: sample_id * 100,
                        });
                    }
                    let _ = pip_session.finish_send();
                }
                Ok(None) => {}
                Err(e) => eprintln!("  [Pip] Server error: {}", e),
            }
        }
    });

    println!("Server is running and listening for incoming client connections.");
    println!("Press Ctrl+C to stop.\n");

    loop {
        thread::sleep(Duration::from_secs(60));
    }
}
