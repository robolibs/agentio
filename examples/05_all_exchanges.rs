use agentio::{Agent, DirectoryMode, IdentitySource};
use datapod::datapod;
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
    run(false)
}

fn run(force_quic: bool) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    println!("============================================================");
    println!("  agentio: Master Example Testing All 5 Exchange Patterns   ");
    println!("============================================================");

    // Create Server Agent
    let mut server_builder = Agent::builder()
        .identity(IdentitySource::Random)
        .name("server-agent")
        .directory(DirectoryMode::Replicated)
        .allow_any_peer();
    if force_quic {
        server_builder = server_builder.skip_shm();
    }
    let server_agent = server_builder.build()?;

    // Create Client Agent (bootstrapped with Server Agent's EndpointId)
    let mut client_builder = Agent::builder()
        .identity(IdentitySource::Random)
        .name("client-agent")
        .directory(DirectoryMode::Replicated)
        .allow_any_peer()
        .bootstrap([server_agent.endpoint_id()]);
    if force_quic {
        client_builder = client_builder.skip_shm();
    }
    let client_agent = client_builder.build()?;
    assert_eq!(server_agent.shared_memory_disabled(), force_quic);
    assert_eq!(client_agent.shared_memory_disabled(), force_quic);

    let _ = server_agent.wait_for_direct_addresses(Duration::from_secs(1));
    let _ = client_agent.wait_for_direct_addresses(Duration::from_secs(1));

    println!(
        "Server Agent: {} ({})",
        server_agent.name(),
        server_agent.endpoint_id()
    );
    println!(
        "Client Agent: {} ({})\n",
        client_agent.name(),
        client_agent.endpoint_id()
    );

    // ------------------------------------------------------------------------
    // Pattern 1: Pub / Sub
    // ------------------------------------------------------------------------
    println!("[Pattern 1/5] Pub / Sub (/telemetry)");
    let mut pubr = server_agent.publish::<Telemetry>("/telemetry")?;
    let mut sub = client_agent.subscribe::<Telemetry>("/telemetry")?;

    let mut received = None;
    for _ in 0..20 {
        pubr.send(&Telemetry {
            seq: 101,
            val: 98.6,
        })?;
        if let Some(sample) = sub.recv_timeout(Duration::from_millis(250))? {
            received = Some(sample);
            break;
        }
    }
    let sample = received.ok_or("Pub/Sub timed out")?;
    let telemetry = sample.header();
    println!(
        "  -> Received Telemetry: seq={}, val={}",
        telemetry.seq, telemetry.val
    );
    assert_eq!(telemetry.seq, 101);
    println!("  [OK] Pub/Sub test passed.\n");

    // ------------------------------------------------------------------------
    // Pattern 2: Req / Res (RPC Request / Response)
    // ------------------------------------------------------------------------
    println!("[Pattern 2/5] Req / Res (/service/add)");
    let mut req_srv = server_agent.req_server::<MathReq, MathRes>("/service/add")?;
    let mut req_cli = client_agent.req_client::<MathReq, MathRes>("/service/add")?;

    let srv_handle_req = thread::spawn(move || -> Result<(), String> {
        for _ in 0..10 {
            if let Some((sample, reply)) = req_srv
                .recv_timeout(Duration::from_millis(100))
                .map_err(|e| e.to_string())?
            {
                let req = sample.header();
                let res = MathRes { sum: req.x + req.y };
                reply.respond(&res).map_err(|e| e.to_string())?;
                return Ok(());
            }
        }
        Err("Req/Res server timed out".to_string())
    });

    let res_sample = req_cli.call(&MathReq { x: 15, y: 27 })?;
    println!(
        "  -> Client called 15 + 27, received sum = {}",
        res_sample.header().sum
    );
    assert_eq!(res_sample.header().sum, 42);
    srv_handle_req.join().unwrap().unwrap();
    println!("  [OK] Req/Res test passed.\n");

    // ------------------------------------------------------------------------
    // Pattern 3: Que / Ans (Query 1 -> Stream N Answers)
    // ------------------------------------------------------------------------
    println!("[Pattern 3/5] Que / Ans (/query/range)");
    let mut que_srv = server_agent.que_server::<RangeQuery, RangeHit>("/query/range")?;
    let mut que_cli = client_agent.que_client::<RangeQuery, RangeHit>("/query/range")?;

    let srv_handle_que = thread::spawn(move || -> Result<(), String> {
        for _ in 0..10 {
            if let Some((que_sample, mut ans_sender)) = que_srv.take().map_err(|e| e.to_string())? {
                let q = que_sample.header();
                for offset in 0..q.count {
                    ans_sender
                        .send(&RangeHit {
                            value: q.start + offset as i32,
                        })
                        .map_err(|e| e.to_string())?;
                }
                ans_sender.finish().map_err(|e| e.to_string())?;
                return Ok(());
            }
            thread::sleep(Duration::from_millis(20));
        }
        Err("Que/Ans server timed out".to_string())
    });

    let mut answers_handle = que_cli.send(&RangeQuery {
        start: 10,
        count: 3,
    })?;
    let mut hits = Vec::new();
    while let Some(hit_sample) = answers_handle.next()? {
        hits.push(hit_sample.header().value);
    }
    println!(
        "  -> Client queried range(10, count=3), received hits: {:?}",
        hits
    );
    assert_eq!(hits, vec![10, 11, 12]);
    srv_handle_que.join().unwrap().unwrap();
    println!("  [OK] Que/Ans test passed.\n");

    // ------------------------------------------------------------------------
    // Pattern 4: Put / Ack (Stream N Put Blocks -> Ack Result)
    // ------------------------------------------------------------------------
    println!("[Pattern 4/5] Put / Ack (/upload/blocks)");
    let mut put_srv = server_agent.put_server::<DataBlock, UploadResult>("/upload/blocks")?;
    let mut put_cli = client_agent.put_client::<DataBlock, UploadResult>("/upload/blocks")?;

    let srv_handle_put = thread::spawn(move || -> Result<(), String> {
        for _ in 0..10 {
            if let Some(mut puts_recv) = put_srv.take().map_err(|e| e.to_string())? {
                let mut blocks = 0;
                let mut total_bytes = 0;
                while let Some(block) = puts_recv.next().map_err(|e| e.to_string())? {
                    blocks += 1;
                    total_bytes += block.header().bytes_count;
                }
                puts_recv
                    .ack(&UploadResult {
                        total_blocks: blocks,
                        total_bytes,
                    })
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
            thread::sleep(Duration::from_millis(20));
        }
        Err("Put/Ack server timed out".to_string())
    });

    let mut upload = put_cli.open()?;
    upload.send(&DataBlock { bytes_count: 512 })?;
    upload.send(&DataBlock { bytes_count: 1024 })?;
    let ack_sample = upload.finish()?;
    println!(
        "  -> Client uploaded 2 blocks, Ack received: total_blocks={}, total_bytes={}",
        ack_sample.header().total_blocks,
        ack_sample.header().total_bytes
    );
    assert_eq!(ack_sample.header().total_blocks, 2);
    assert_eq!(ack_sample.header().total_bytes, 1536);
    srv_handle_put.join().unwrap().unwrap();
    println!("  [OK] Put/Ack test passed.\n");

    // ------------------------------------------------------------------------
    // Pattern 5: Pip (Bidirectional Streaming Pipe)
    // ------------------------------------------------------------------------
    println!("[Pattern 5/5] Pip (/stream/audio)");
    let mut pip_srv = server_agent.pip_server::<AudioChunk, AudioFeedback>("/stream/audio")?;
    let mut pip_cli = client_agent.pip_client::<AudioChunk, AudioFeedback>("/stream/audio")?;

    let srv_handle_pip = thread::spawn(move || -> Result<(), String> {
        for _ in 0..10 {
            if let Some(mut pip_session) = pip_srv.take().map_err(|e| e.to_string())? {
                while let Some(chunk) = pip_session.next().map_err(|e| e.to_string())? {
                    let feedback = AudioFeedback {
                        echo_id: chunk.header().sample_id * 100,
                    };
                    pip_session.send(&feedback).map_err(|e| e.to_string())?;
                }
                pip_session.finish_send().map_err(|e| e.to_string())?;
                return Ok(());
            }
            thread::sleep(Duration::from_millis(20));
        }
        Err("Pip server timed out".to_string())
    });

    let mut pip_stream = pip_cli.open()?;
    pip_stream.send(&AudioChunk { sample_id: 1 })?;
    pip_stream.send(&AudioChunk { sample_id: 2 })?;
    pip_stream.finish_send()?;

    let mut feedback_echoes = Vec::new();
    while let Some(reply_sample) = pip_stream.next()? {
        feedback_echoes.push(reply_sample.header().echo_id);
    }
    println!(
        "  -> Client streamed chunks [1, 2], received feedback echoes: {:?}",
        feedback_echoes
    );
    assert_eq!(feedback_echoes, vec![100, 200]);
    srv_handle_pip.join().unwrap().unwrap();
    println!("  [OK] Pip streaming test passed.\n");

    println!("============================================================");
    println!("  ALL 5 PEERBUS EXCHANGE PATTERNS VERIFIED SUCCESSFULLY!   ");
    println!("============================================================");

    Ok(())
}
