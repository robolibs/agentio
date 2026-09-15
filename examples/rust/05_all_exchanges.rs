use agentio::{Agent, DirectoryMode, IdentitySource};
use datapod::datapod;
use std::thread;
use std::time::Duration;

// 1. Pub/Sub Payload
#[datapod(name = "agentio.telemetry.v1")]
struct Telemetry {
    pub seq: u64,
    pub val: f64,
    pub payload_bytes: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
}

// 2. Req/Res Payloads
#[datapod(name = "agentio.math_req.v1")]
struct MathReq {
    pub x: i32,
    pub y: i32,
    pub response_bytes: u64,
}

#[datapod(name = "agentio.math_res.v1")]
struct MathRes {
    pub sum: i64,
    pub payload_bytes: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
}

// 3. Que/Ans Payloads
#[datapod(name = "agentio.range_query.v1")]
struct RangeQuery {
    pub start: i32,
    pub count: u32,
    pub chunk_bytes: u64,
}

#[datapod(name = "agentio.range_hit.v1")]
struct RangeHit {
    pub value: i64,
    pub payload_bytes: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
}

// 4. Put/Ack Payloads
#[datapod(name = "agentio.data_block.v1")]
struct DataBlock {
    pub block_index: u64,
    pub payload_bytes: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
}

#[datapod(name = "agentio.upload_result.v1")]
struct UploadResult {
    pub total_blocks: u64,
    pub total_bytes: u64,
}

// 5. Pip Streaming Payloads
#[datapod(name = "agentio.audio_chunk.v1")]
struct AudioChunk {
    pub sample_id: u64,
    pub payload_bytes: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
}

#[datapod(name = "agentio.audio_feedback.v1")]
struct AudioFeedback {
    pub echo_id: u64,
    pub payload_bytes: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
}

fn payload_matches(claimed: u64, payload: &[u8], expected: u8) -> bool {
    payload.len() as u64 == claimed && payload.iter().all(|byte| *byte == expected)
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
            payload_bytes: 4_096,
            data: vec![0x11; 4_096],
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
    assert!(payload_matches(
        telemetry.payload_bytes,
        sample.payload(),
        0x11
    ));
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
                let response_bytes = usize::try_from(req.response_bytes)
                    .map_err(|_| "response size does not fit usize".to_string())?;
                let res = MathRes {
                    sum: i64::from(req.x) + i64::from(req.y),
                    payload_bytes: req.response_bytes,
                    data: vec![0x22; response_bytes],
                };
                reply.respond(&res).map_err(|e| e.to_string())?;
                return Ok(());
            }
        }
        Err("Req/Res server timed out".to_string())
    });

    let res_sample = req_cli.call(&MathReq {
        x: 15,
        y: 27,
        response_bytes: 8_192,
    })?;
    println!(
        "  -> Client called 15 + 27, received sum = {}",
        res_sample.header().sum
    );
    assert_eq!(res_sample.header().sum, 42);
    assert_eq!(
        res_sample.header().payload_bytes,
        res_sample.payload().len() as u64
    );
    assert!(res_sample.payload().iter().all(|byte| *byte == 0x22));
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
                            value: i64::from(q.start + offset as i32),
                            payload_bytes: q.chunk_bytes,
                            data: vec![(q.start + offset as i32) as u8; q.chunk_bytes as usize],
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
        chunk_bytes: 2_048,
    })?;
    let mut hits = Vec::new();
    while let Some(hit_sample) = answers_handle.next()? {
        assert!(payload_matches(
            hit_sample.header().payload_bytes,
            hit_sample.payload(),
            hit_sample.header().value as u8
        ));
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
                let mut blocks = 0u64;
                let mut total_bytes = 0;
                while let Some(block) = puts_recv.next().map_err(|e| e.to_string())? {
                    if !payload_matches(
                        block.header().payload_bytes,
                        block.payload(),
                        block.header().block_index as u8,
                    ) {
                        return Err("invalid upload payload".to_string());
                    }
                    blocks += 1;
                    total_bytes += block.payload().len() as u64;
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
    upload.send(&DataBlock {
        block_index: 1,
        payload_bytes: 512,
        data: vec![1; 512],
    })?;
    upload.send(&DataBlock {
        block_index: 2,
        payload_bytes: 1_024,
        data: vec![2; 1_024],
    })?;
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
                    if !payload_matches(
                        chunk.header().payload_bytes,
                        chunk.payload(),
                        chunk.header().sample_id as u8,
                    ) {
                        return Err("invalid pipe payload".to_string());
                    }
                    let feedback = AudioFeedback {
                        echo_id: chunk.header().sample_id * 100,
                        payload_bytes: chunk.payload().len() as u64,
                        data: chunk.payload().to_vec(),
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
    pip_stream.send(&AudioChunk {
        sample_id: 1,
        payload_bytes: 1_024,
        data: vec![1; 1_024],
    })?;
    pip_stream.send(&AudioChunk {
        sample_id: 2,
        payload_bytes: 1_024,
        data: vec![2; 1_024],
    })?;
    pip_stream.finish_send()?;

    let mut feedback_echoes = Vec::new();
    while let Some(reply_sample) = pip_stream.next()? {
        assert!(payload_matches(
            reply_sample.header().payload_bytes,
            reply_sample.payload(),
            (reply_sample.header().echo_id / 100) as u8
        ));
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
