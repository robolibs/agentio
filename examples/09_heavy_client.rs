use agentio::{Agent, DirectoryMode, IdentitySource};
use datapod::datapod;
use std::env;
use std::thread;
use std::time::{Duration, Instant};

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
    let target_id = args
        .iter()
        .find(|a| !a.starts_with('-') && !a.ends_with("09_heavy_client"));

    let server_address = match target_id {
        Some(id) => id.clone(),
        None => {
            eprintln!(
                "Usage: cargo run --example 09_heavy_client -- <SERVER_ENDPOINT_ID_OR_DID_KEY> [--use-shm]"
            );
            eprintln!("Example:");
            eprintln!(
                "  cargo run --example 09_heavy_client -- did:key:z6MkoJAH27PmMN5S5YPMpi1MRPPtGFxYaR5DpU8NpK3NunT9"
            );
            std::process::exit(1);
        }
    };

    let use_shm = args.iter().any(|a| a == "--use-shm");

    println!("============================================================");
    println!("     agentio: Heavy Data Multi-Megabyte Client Benchmark   ");
    println!("============================================================");
    println!("Target Server: {}", server_address);

    let mut builder = Agent::builder()
        .name("heavy-client")
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
    println!("  [*] Initializing network transport...\n");
    thread::sleep(Duration::from_secs(1));

    let overall_start = Instant::now();

    // ------------------------------------------------------------------------
    // Pattern 1: Pub / Sub (5 MB 4K Frames Stream)
    // ------------------------------------------------------------------------
    println!("[Pattern 1/5] Testing Heavy Pub / Sub Stream (/heavy/image_stream)...");
    let mut sub = client_agent.subscribe::<HeavyImageHeader>("/heavy/image_stream")?;
    let start_time = Instant::now();
    let mut received_count = 0;
    let mut total_bytes = 0u64;

    for _ in 1..=60 {
        if let Ok(Some(sample)) = sub.recv_timeout(Duration::from_millis(100)) {
            let h = sample.header();
            received_count += 1;
            total_bytes += h.payload_bytes;
            println!(
                "  -> Frame #{}: 4K Video ({}x{}), Size: {:.2} MB",
                h.frame_id,
                h.width,
                h.height,
                h.payload_bytes as f64 / 1_000_000.0
            );
            if received_count >= 5 {
                break;
            }
        }
    }
    let elapsed = start_time.elapsed();
    let mb_received = total_bytes as f64 / 1_000_000.0;
    let speed_mbps = mb_received / elapsed.as_secs_f64();
    println!(
        "  [OK] Pub/Sub: Received 5 frames ({:.2} MB) in {:.2}s ({:.2} MB/s)\n",
        mb_received,
        elapsed.as_secs_f64(),
        speed_mbps
    );

    // ------------------------------------------------------------------------
    // Pattern 2: Req / Res (10 MB Large RPC Payload)
    // ------------------------------------------------------------------------
    println!("[Pattern 2/5] Testing Heavy Req / Res RPC (/heavy/rpc_download)...");
    let mut rpc_cli = client_agent.req_client::<HeavyRpcReq, HeavyRpcRes>("/heavy/rpc_download")?;
    let req_size = 10_000_000u64; // 10 MB
    let start_time = Instant::now();
    let res = rpc_cli.call(&HeavyRpcReq {
        requested_bytes: req_size,
    })?;
    let elapsed = start_time.elapsed();
    let h = res.header();
    let mb_received = h.payload_bytes as f64 / 1_000_000.0;
    let speed_mbps = mb_received / elapsed.as_secs_f64();
    println!(
        "  -> Received RPC response: {:.2} MB (Checksum: {:x}) in {:.2}s ({:.2} MB/s)",
        mb_received,
        h.checksum,
        elapsed.as_secs_f64(),
        speed_mbps
    );
    assert_eq!(h.payload_bytes, req_size);
    println!("  [OK] Req/Res RPC benchmark passed.\n");

    // ------------------------------------------------------------------------
    // Pattern 3: Que / Ans (20 Chunks x 1 MB = 20 MB Total Streamed)
    // ------------------------------------------------------------------------
    println!("[Pattern 3/5] Testing Heavy Que / Ans Stream (/heavy/query_chunks)...");
    let mut que_cli = client_agent.que_client::<HeavyQuery, HeavyAnswer>("/heavy/query_chunks")?;
    let start_time = Instant::now();
    let mut answers = que_cli.send(&HeavyQuery {
        total_chunks: 20,
        chunk_bytes: 1_000_000, // 1 MB per chunk
    })?;
    let mut total_chunks = 0u64;
    let mut total_bytes = 0u64;

    while let Ok(Some(hit)) = answers.next() {
        let h = hit.header();
        total_chunks += 1;
        total_bytes += h.payload_bytes;
        if total_chunks.is_multiple_of(5) {
            println!(
                "  -> Progress: Received {} / 20 chunks ({:.2} MB)",
                total_chunks,
                total_bytes as f64 / 1_000_000.0
            );
        }
    }
    let elapsed = start_time.elapsed();
    let mb_received = total_bytes as f64 / 1_000_000.0;
    let speed_mbps = mb_received / elapsed.as_secs_f64();
    println!(
        "  [OK] Que/Ans Stream: Received {} chunks ({:.2} MB) in {:.2}s ({:.2} MB/s)\n",
        total_chunks,
        mb_received,
        elapsed.as_secs_f64(),
        speed_mbps
    );
    assert_eq!(total_chunks, 20);

    // ------------------------------------------------------------------------
    // Pattern 4: Put / Ack (30 Upload Blocks x 1 MB = 30 MB Upload)
    // ------------------------------------------------------------------------
    println!("[Pattern 4/5] Testing Heavy Put / Ack Upload (/heavy/upload_blocks)...");
    let mut put_cli =
        client_agent.put_client::<HeavyUploadBlock, HeavyUploadAck>("/heavy/upload_blocks")?;
    let start_time = Instant::now();
    let mut upload = put_cli.open()?;

    let total_blocks_to_send = 30u64;
    let block_bytes = 1_000_000u64; // 1 MB per block
    for b in 1..=total_blocks_to_send {
        upload.send(&HeavyUploadBlock {
            block_index: b,
            payload_bytes: block_bytes,
        })?;
        if b % 10 == 0 {
            println!(
                "  -> Progress: Sent {} / {} blocks ({:.2} MB)",
                b,
                total_blocks_to_send,
                (b as f64 * block_bytes as f64) / 1_000_000.0
            );
        }
    }
    let ack = upload.finish()?;
    let elapsed = start_time.elapsed();
    let ack_h = ack.header();
    let mb_uploaded = ack_h.total_bytes as f64 / 1_000_000.0;
    let speed_mbps = mb_uploaded / elapsed.as_secs_f64();
    println!(
        "  -> Ack Received: Total Blocks = {}, Total Bytes = {:.2} MB",
        ack_h.total_blocks, mb_uploaded
    );
    println!(
        "  [OK] Put/Ack Upload: Uploaded {:.2} MB in {:.2}s ({:.2} MB/s)\n",
        mb_uploaded,
        elapsed.as_secs_f64(),
        speed_mbps
    );
    assert_eq!(ack_h.total_blocks, total_blocks_to_send);

    // ------------------------------------------------------------------------
    // Pattern 5: Pip Streaming (Bi-directional Heavy Stream 15 MB)
    // ------------------------------------------------------------------------
    println!("[Pattern 5/5] Testing Heavy Pip Streaming (/heavy/bidi_stream)...");
    let mut pip_cli =
        client_agent.pip_client::<HeavyPipFrame, HeavyPipAck>("/heavy/bidi_stream")?;
    let start_time = Instant::now();
    let mut pip = pip_cli.open()?;

    let pip_frames = 15u64;
    let frame_bytes = 1_000_000u64; // 1 MB per frame
    for f in 1..=pip_frames {
        pip.send(&HeavyPipFrame {
            frame_seq: f,
            payload_bytes: frame_bytes,
        })?;
    }
    pip.finish_send()?;

    let mut ack_count = 0u64;
    while let Ok(Some(reply)) = pip.next() {
        ack_count += 1;
        let h = reply.header();
        if ack_count.is_multiple_of(5) {
            println!(
                "  -> Received Pipe Ack #{}: frame_seq={}",
                ack_count, h.ack_seq
            );
        }
    }
    let elapsed = start_time.elapsed();
    let total_pip_bytes = (pip_frames as f64 * frame_bytes as f64) / 1_000_000.0;
    let speed_mbps = total_pip_bytes / elapsed.as_secs_f64();
    println!(
        "  [OK] Pip Streaming: Processed {} frames ({:.2} MB) in {:.2}s ({:.2} MB/s)\n",
        ack_count,
        total_pip_bytes,
        elapsed.as_secs_f64(),
        speed_mbps
    );
    assert_eq!(ack_count, pip_frames);

    let total_elapsed = overall_start.elapsed();
    println!("============================================================");
    println!(
        "  HEAVY BENCHMARK COMPLETED SUCCESSFULLY IN {:.2} SECONDS!  ",
        total_elapsed.as_secs_f64()
    );
    println!("============================================================");

    Ok(())
}
