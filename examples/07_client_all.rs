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
    let target_id = args
        .iter()
        .find(|a| !a.starts_with('-') && !a.ends_with("07_client_all"));

    let server_address = match target_id {
        Some(id) => id.clone(),
        None => {
            eprintln!(
                "Usage: cargo run --example 07_client_all -- <SERVER_ENDPOINT_ID_OR_DID_KEY> [--use-shm]"
            );
            eprintln!("Example:");
            eprintln!(
                "  cargo run --example 07_client_all -- did:key:z6MkoJAH27PmMN5S5YPMpi1MRPPtGFxYaR5DpU8NpK3NunT9"
            );
            std::process::exit(1);
        }
    };

    let use_shm = args.iter().any(|a| a == "--use-shm");

    println!("============================================================");
    println!("          agentio: Standalone Client (All 5 Services)       ");
    println!("============================================================");
    println!("Target Server: {}", server_address);

    let mut builder = Agent::builder()
        .name("standalone-client")
        .identity(IdentitySource::Random)
        .directory(DirectoryMode::Replicated)
        .allow_any_peer()
        .bootstrap([server_address.as_str()]);

    if !use_shm {
        println!("  [*] Transport: Pure Network Sockets / QUIC (skip_shm = true)");
        builder = builder.skip_shm();
    } else {
        println!("  [*] Transport: Local Shared Memory (SHM)");
    }

    let client_agent = builder.build()?;
    let _ = client_agent.wait_for_direct_addresses(Duration::from_secs(5));
    println!("Client Endpoint ID: {}", client_agent.endpoint_id());
    println!("  [*] Waiting 1s for transport initialization...\n");
    thread::sleep(Duration::from_secs(1));

    // ------------------------------------------------------------------------
    // Pattern 1: Pub / Sub
    // ------------------------------------------------------------------------
    println!("[Pattern 1/5] Resolving and Subscribing to /telemetry...");
    match client_agent.subscribe::<Telemetry>("/telemetry") {
        Ok(mut sub) => {
            println!("  -> Topic /telemetry resolved! Waiting for stream samples...");
            let mut received_telemetry = false;
            for i in 1..=60 {
                match sub.recv_timeout(Duration::from_millis(100)) {
                    Ok(Some(sample)) => {
                        let t = sample.header();
                        println!(
                            "  -> Received Telemetry sample #{}: seq={}, val={}",
                            i, t.seq, t.val
                        );
                        received_telemetry = true;
                        break;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        println!("  -> Recv error attempt {}: {}", i, e);
                    }
                }
            }
            if received_telemetry {
                println!("  [OK] Pub/Sub test passed.\n");
            } else {
                println!("  [FAIL] Timed out waiting for telemetry samples.\n");
            }
        }
        Err(e) => println!("  [FAIL] Could not subscribe to /telemetry: {}\n", e),
    }

    // ------------------------------------------------------------------------
    // Pattern 2: Req / Res
    // ------------------------------------------------------------------------
    println!("[Pattern 2/5] Testing Req / Res (/service/add)...");
    match client_agent.req_client::<MathReq, MathRes>("/service/add") {
        Ok(mut req_cli) => match req_cli.call(&MathReq { x: 100, y: 250 }) {
            Ok(res) => {
                println!(
                    "  -> Called MathReq(100, 250), received MathRes sum = {}",
                    res.header().sum
                );
                assert_eq!(res.header().sum, 350);
                println!("  [OK] Req/Res test passed.\n");
            }
            Err(e) => println!("  [FAIL] Req/Res RPC call failed: {}\n", e),
        },
        Err(e) => println!(
            "  [FAIL] Could not create req_client for /service/add: {}\n",
            e
        ),
    }

    // ------------------------------------------------------------------------
    // Pattern 3: Que / Ans
    // ------------------------------------------------------------------------
    println!("[Pattern 3/5] Testing Que / Ans (/query/range)...");
    match client_agent.que_client::<RangeQuery, RangeHit>("/query/range") {
        Ok(mut que_cli) => match que_cli.send(&RangeQuery {
            start: 50,
            count: 4,
        }) {
            Ok(mut answers) => {
                let mut hits = Vec::new();
                while let Ok(Some(hit)) = answers.next() {
                    hits.push(hit.header().value);
                }
                println!(
                    "  -> Queried RangeQuery(50, count=4), received hits: {:?}",
                    hits
                );
                assert_eq!(hits, vec![50, 51, 52, 53]);
                println!("  [OK] Que/Ans test passed.\n");
            }
            Err(e) => println!("  [FAIL] Que/Ans query send failed: {}\n", e),
        },
        Err(e) => println!(
            "  [FAIL] Could not create que_client for /query/range: {}\n",
            e
        ),
    }

    // ------------------------------------------------------------------------
    // Pattern 4: Put / Ack
    // ------------------------------------------------------------------------
    println!("[Pattern 4/5] Testing Put / Ack (/upload/blocks)...");
    match client_agent.put_client::<DataBlock, UploadResult>("/upload/blocks") {
        Ok(mut put_cli) => match put_cli.open() {
            Ok(mut upload) => {
                let _ = upload.send(&DataBlock { bytes_count: 1024 });
                let _ = upload.send(&DataBlock { bytes_count: 2048 });
                let _ = upload.send(&DataBlock { bytes_count: 4096 });
                match upload.finish() {
                    Ok(ack) => {
                        println!(
                            "  -> Uploaded 3 data blocks, Ack received: total_blocks={}, total_bytes={}",
                            ack.header().total_blocks,
                            ack.header().total_bytes
                        );
                        assert_eq!(ack.header().total_blocks, 3);
                        assert_eq!(ack.header().total_bytes, 7168);
                        println!("  [OK] Put/Ack test passed.\n");
                    }
                    Err(e) => println!("  [FAIL] Put/Ack finish failed: {}\n", e),
                }
            }
            Err(e) => println!("  [FAIL] Put/Ack open upload failed: {}\n", e),
        },
        Err(e) => println!(
            "  [FAIL] Could not create put_client for /upload/blocks: {}\n",
            e
        ),
    }

    // ------------------------------------------------------------------------
    // Pattern 5: Pip Streaming
    // ------------------------------------------------------------------------
    println!("[Pattern 5/5] Testing Pip Streaming (/stream/audio)...");
    match client_agent.pip_client::<AudioChunk, AudioFeedback>("/stream/audio") {
        Ok(mut pip_cli) => match pip_cli.open() {
            Ok(mut pip) => {
                let _ = pip.send(&AudioChunk { sample_id: 5 });
                let _ = pip.send(&AudioChunk { sample_id: 6 });
                let _ = pip.finish_send();

                let mut echoes = Vec::new();
                while let Ok(Some(reply)) = pip.next() {
                    echoes.push(reply.header().echo_id);
                }
                println!(
                    "  -> Streamed AudioChunks [5, 6], received feedback echoes: {:?}",
                    echoes
                );
                assert_eq!(echoes, vec![500, 600]);
                println!("  [OK] Pip streaming test passed.\n");
            }
            Err(e) => println!("  [FAIL] Pip stream open failed: {}\n", e),
        },
        Err(e) => println!(
            "  [FAIL] Could not create pip_client for /stream/audio: {}\n",
            e
        ),
    }

    println!("============================================================");
    println!("  ALL 5 EXCHANGE PATTERNS TEST SUITE COMPLETED!            ");
    println!("============================================================");

    Ok(())
}
