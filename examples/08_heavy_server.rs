use agentio::{Agent, DirectoryMode, IdentitySource};
use datapod::datapod;
use std::env;
use std::thread;
use std::time::Duration;

#[datapod(name = "heavy.image_header.v1")]
pub struct HeavyImageHeader {
    pub frame_id: u64,
    pub width: u64,
    pub height: u64,
    pub payload_bytes: u64,
}

#[datapod(name = "heavy.rpc_req.v1")]
pub struct HeavyRpcReq {
    pub requested_bytes: u64,
}

#[datapod(name = "heavy.rpc_res.v1")]
pub struct HeavyRpcRes {
    pub checksum: u64,
    pub payload_bytes: u64,
}

#[datapod(name = "heavy.query.v1")]
pub struct HeavyQuery {
    pub total_chunks: u64,
    pub chunk_bytes: u64,
}

#[datapod(name = "heavy.answer.v1")]
pub struct HeavyAnswer {
    pub chunk_index: u64,
    pub payload_bytes: u64,
}

#[datapod(name = "heavy.upload_block.v1")]
pub struct HeavyUploadBlock {
    pub block_index: u64,
    pub payload_bytes: u64,
}

#[datapod(name = "heavy.upload_ack.v1")]
pub struct HeavyUploadAck {
    pub total_blocks: u64,
    pub total_bytes: u64,
}

#[datapod(name = "heavy.pip_frame.v1")]
pub struct HeavyPipFrame {
    pub frame_seq: u64,
    pub payload_bytes: u64,
}

#[datapod(name = "heavy.pip_ack.v1")]
pub struct HeavyPipAck {
    pub ack_seq: u64,
    pub bytes_received: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    let use_shm = args.iter().any(|a| a == "--use-shm");

    println!("============================================================");
    println!("     agentio: Heavy Data Multi-Megabyte Server Benchmark   ");
    println!("============================================================");

    let mut builder = Agent::builder()
        .name("heavy-server")
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

    println!("  Run the heavy client benchmark using:");
    println!("    cargo run --example 09_heavy_client -- {}", did_key);
    println!("============================================================\n");

    // ------------------------------------------------------------------------
    // Pattern 1: Pub / Sub (5 MB 4K Video Frame Stream)
    // ------------------------------------------------------------------------
    let mut image_pub = agent.publish::<HeavyImageHeader>("/heavy/image_stream")?;
    thread::spawn(move || {
        let mut frame_seq = 1u64;
        let payload_size = 5_000_000u64; // 5 MB per frame
        loop {
            if let Ok(mut loan) = image_pub.loan(payload_size as usize) {
                *loan.header_mut() = HeavyImageHeader {
                    frame_id: frame_seq,
                    width: 3840,
                    height: 2160,
                    payload_bytes: payload_size,
                };
                let payload = loan.payload_mut();
                for (i, byte) in payload.iter_mut().enumerate() {
                    *byte = (frame_seq as usize + i) as u8;
                }
                let _ = image_pub.publish(loan);
            }
            frame_seq += 1;
            thread::sleep(Duration::from_millis(500));
        }
    });

    // ------------------------------------------------------------------------
    // Pattern 2: Req / Res (10 MB Large RPC Payload Response)
    // ------------------------------------------------------------------------
    let mut rpc_srv = agent.req_server::<HeavyRpcReq, HeavyRpcRes>("/heavy/rpc_download")?;
    thread::spawn(move || {
        loop {
            match rpc_srv.recv_timeout(Duration::from_millis(100)) {
                Ok(Some((sample, reply))) => {
                    let req = sample.header();
                    let req_bytes = req.requested_bytes as usize;
                    println!(
                        "  [Req/Res] Generating and responding with {:.2} MB payload...",
                        req_bytes as f64 / 1_000_000.0
                    );

                    let dummy_data = vec![0xAA; req_bytes];
                    let checksum = blake3::hash(&dummy_data);
                    let checksum_u64 =
                        u64::from_le_bytes(checksum.as_bytes()[0..8].try_into().unwrap());

                    let _ = reply.respond(&HeavyRpcRes {
                        checksum: checksum_u64,
                        payload_bytes: req_bytes as u64,
                    });
                }
                Ok(None) => {}
                Err(e) => eprintln!("  [Req/Res] Server error: {}", e),
            }
        }
    });

    // ------------------------------------------------------------------------
    // Pattern 3: Que / Ans (20 Chunks x 1 MB Streamed = 20 MB Total)
    // ------------------------------------------------------------------------
    let mut que_srv = agent.que_server::<HeavyQuery, HeavyAnswer>("/heavy/query_chunks")?;
    thread::spawn(move || {
        loop {
            match que_srv.recv_timeout(Duration::from_millis(100)) {
                Ok(Some((que_sample, mut ans_sender))) => {
                    let q = que_sample.header();
                    println!(
                        "  [Que/Ans] Streaming {} chunks of {:.2} MB each...",
                        q.total_chunks,
                        q.chunk_bytes as f64 / 1_000_000.0
                    );

                    for idx in 0..q.total_chunks {
                        let _ = ans_sender.send(&HeavyAnswer {
                            chunk_index: idx,
                            payload_bytes: q.chunk_bytes,
                        });
                        thread::sleep(Duration::from_millis(100));
                    }
                    let _ = ans_sender.finish();
                }
                Ok(None) => {}
                Err(e) => eprintln!("  [Que/Ans] Server error: {}", e),
            }
        }
    });

    // ------------------------------------------------------------------------
    // Pattern 4: Put / Ack (30 Blocks x 1 MB Streamed Upload = 30 MB Total)
    // ------------------------------------------------------------------------
    let mut put_srv =
        agent.put_server::<HeavyUploadBlock, HeavyUploadAck>("/heavy/upload_blocks")?;
    thread::spawn(move || {
        loop {
            match put_srv.recv_timeout(Duration::from_millis(100)) {
                Ok(Some(mut puts_recv)) => {
                    println!("  [Put/Ack] Receiving heavy file upload stream...");
                    let mut blocks = 0u64;
                    let mut total_bytes = 0u64;
                    while let Ok(Some(block)) = puts_recv.next() {
                        blocks += 1;
                        total_bytes += block.header().payload_bytes;
                        if blocks.is_multiple_of(10) {
                            println!(
                                "  [Put/Ack] Progress: Received {} blocks ({:.2} MB)",
                                blocks,
                                total_bytes as f64 / 1_000_000.0
                            );
                        }
                    }
                    println!(
                        "  [Put/Ack] Upload complete: {} blocks ({:.2} MB). Sending Ack...",
                        blocks,
                        total_bytes as f64 / 1_000_000.0
                    );
                    let _ = puts_recv.ack(&HeavyUploadAck {
                        total_blocks: blocks,
                        total_bytes,
                    });
                }
                Ok(None) => {}
                Err(e) => eprintln!("  [Put/Ack] Server error: {}", e),
            }
        }
    });

    // ------------------------------------------------------------------------
    // Pattern 5: Pip Streaming (Bi-directional Heavy Streaming 15 MB)
    // ------------------------------------------------------------------------
    let mut pip_srv = agent.pip_server::<HeavyPipFrame, HeavyPipAck>("/heavy/bidi_stream")?;
    thread::spawn(move || {
        loop {
            match pip_srv.recv_timeout(Duration::from_millis(100)) {
                Ok(Some(mut pip_session)) => {
                    println!("  [Pip] Bi-directional heavy streaming session opened...");
                    while let Ok(Some(frame)) = pip_session.next() {
                        let seq = frame.header().frame_seq;
                        let bytes = frame.header().payload_bytes;
                        let _ = pip_session.send(&HeavyPipAck {
                            ack_seq: seq,
                            bytes_received: bytes,
                        });
                    }
                    let _ = pip_session.finish_send();
                }
                Ok(None) => {}
                Err(e) => eprintln!("  [Pip] Server error: {}", e),
            }
        }
    });

    println!("Heavy Data Server is running and listening for client benchmarks.");
    println!("Press Ctrl+C to stop.\n");

    loop {
        thread::sleep(Duration::from_secs(60));
    }
}
