use agentio::{Agent, DirectoryMode, IdentitySource};
use agentio_example_messages::{
    HeavyAnswer, HeavyImageHeader, HeavyPipAck, HeavyPipFrame, HeavyQuery, HeavyRpcReq,
    HeavyRpcRes, HeavyUploadAck, HeavyUploadBlock,
};
use std::env;
use std::thread;
use std::time::Duration;

const MAX_RPC_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
const MAX_CHUNKS: u64 = 256;
const MAX_UPLOAD_BYTES: u64 = 128 * 1024 * 1024;
const MAX_PIP_FRAME_BYTES: u64 = 8 * 1024 * 1024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    let use_shm = args.iter().any(|a| a == "--use-shm");

    println!("============================================================");
    println!("   agentio: Heavy Data Multi-Megabyte Throughput Demo     ");
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
    agent.wait_for_direct_addresses(Duration::from_secs(5))?;

    let endpoint_id = agent.endpoint_id();
    let did_key = agent.did_key()?;

    println!("\n  Server Agent Initialized!");
    println!("  -> Endpoint ID: {}", endpoint_id);
    println!("  -> DID:KEY:     {}", did_key);
    println!("  -> Addr:        {:?}\n", agent.endpoint_addr());

    println!("  Run the heavy client throughput demo using:");
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
            let data = (0..payload_size)
                .map(|index| frame_seq.wrapping_add(index) as u8)
                .collect();
            let _ = image_pub.send(&HeavyImageHeader {
                frame_id: frame_seq,
                width: 3840,
                height: 2160,
                payload_bytes: payload_size,
                data,
            });
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
                    let Ok(req_bytes) = usize::try_from(req.requested_bytes) else {
                        eprintln!("  [Req/Res] Rejected unsupported payload size");
                        continue;
                    };
                    if req.requested_bytes > MAX_RPC_BYTES {
                        eprintln!("  [Req/Res] Rejected oversized payload request");
                        continue;
                    }
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
                        data: dummy_data,
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
                    let valid_size = q.chunk_bytes <= MAX_CHUNK_BYTES
                        && q.total_chunks <= MAX_CHUNKS
                        && q.chunk_bytes.checked_mul(q.total_chunks) <= Some(MAX_UPLOAD_BYTES);
                    let Ok(chunk_bytes) = usize::try_from(q.chunk_bytes) else {
                        let _ = ans_sender.finish();
                        continue;
                    };
                    if !valid_size {
                        eprintln!("  [Que/Ans] Rejected oversized stream request");
                        let _ = ans_sender.finish();
                        continue;
                    }
                    println!(
                        "  [Que/Ans] Streaming {} chunks of {:.2} MB each...",
                        q.total_chunks,
                        q.chunk_bytes as f64 / 1_000_000.0
                    );

                    for idx in 0..q.total_chunks {
                        let data = vec![idx as u8; chunk_bytes];
                        let _ = ans_sender.send(&HeavyAnswer {
                            chunk_index: idx,
                            payload_bytes: q.chunk_bytes,
                            data,
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
                        let header = block.header();
                        let payload = block.payload();
                        if payload.len() as u64 != header.payload_bytes
                            || payload.iter().any(|byte| *byte != header.block_index as u8)
                            || total_bytes
                                .checked_add(payload.len() as u64)
                                .is_none_or(|total| total > MAX_UPLOAD_BYTES)
                        {
                            eprintln!("  [Put/Ack] Rejected invalid upload block");
                            continue;
                        }
                        total_bytes += payload.len() as u64;
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
                        let payload = frame.payload();
                        let bytes = payload.len() as u64;
                        if bytes != frame.header().payload_bytes
                            || bytes > MAX_PIP_FRAME_BYTES
                            || payload
                                .iter()
                                .any(|byte| *byte != frame.header().frame_seq as u8)
                        {
                            eprintln!("  [Pip] Rejected invalid frame payload");
                            continue;
                        }
                        let _ = pip_session.send(&HeavyPipAck {
                            ack_seq: seq,
                            bytes_received: bytes,
                            data: payload.to_vec(),
                        });
                    }
                    let _ = pip_session.finish_send();
                }
                Ok(None) => {}
                Err(e) => eprintln!("  [Pip] Server error: {}", e),
            }
        }
    });

    println!("Heavy Data Server is running for throughput demonstrations.");
    println!("Press Ctrl+C to stop.\n");

    loop {
        thread::sleep(Duration::from_secs(60));
    }
}
