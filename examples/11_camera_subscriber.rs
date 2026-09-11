use agentio::{Agent, DirectoryMode, ExchangeKind, IdentitySource};
use agentio_example_messages::CameraFrame;
use std::env;
use std::io::Write;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::time::{Duration, Instant};

const FPS: u64 = 15;
const TOPIC: &str = "/camera/rgb";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let target = args
        .iter()
        .skip(1)
        .find(|arg| !arg.starts_with('-'))
        .ok_or("usage: 11_camera_subscriber <PUBLISHER_DID> [--display] [--frames N]")?;
    let display = args.iter().any(|arg| arg == "--display");
    let frame_limit = option_value(&args, "--frames")
        .map(|value| value.parse::<u64>())
        .transpose()?;
    let use_shm = args.iter().any(|arg| arg == "--use-shm");

    let mut builder = Agent::builder()
        .name("camera-subscriber")
        .identity(IdentitySource::Random)
        .directory(DirectoryMode::Replicated)
        .allow_any_peer()
        .bootstrap([target.as_str()]);
    if !use_shm {
        builder = builder.skip_shm();
    }
    let agent = builder.build()?;
    agent.wait_for_direct_addresses(Duration::from_secs(5))?;
    let resolution_deadline = Instant::now() + Duration::from_secs(15);
    while agent
        .directory()
        .lookup_exchange(TOPIC, ExchangeKind::PubSub)
        .is_none()
        && Instant::now() < resolution_deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut subscriber = agent.subscribe::<CameraFrame>(TOPIC)?;

    println!("Subscribed to {TOPIC} on {target}");
    if display {
        println!("Opening a GStreamer display window");
    }

    let started = Instant::now();
    let mut received = 0u64;
    let mut display_pipeline: Option<(Child, ChildStdin)> = None;
    loop {
        let sample = subscriber
            .recv_timeout(Duration::from_secs(5))?
            .ok_or("camera stream timed out")?;
        let header = sample.header();
        let expected = header
            .width
            .checked_mul(header.height)
            .and_then(|pixels| pixels.checked_mul(header.channels))
            .ok_or("camera frame size overflow")?;
        if header.channels != 3
            || header.payload_bytes != expected
            || sample.payload().len() as u64 != expected
        {
            return Err("invalid camera frame metadata or payload length".into());
        }

        if display && display_pipeline.is_none() {
            display_pipeline = Some(spawn_display(header.width, header.height)?);
        }
        if let Some((_, input)) = &mut display_pipeline {
            input.write_all(sample.payload())?;
        }

        received = received.saturating_add(1);
        if received == 1 || received.is_multiple_of(FPS) {
            let rate = received as f64 / started.elapsed().as_secs_f64();
            println!(
                "Received frame {}: {}x{}, {:.1} FPS average",
                header.sequence, header.width, header.height, rate
            );
        }
        if frame_limit.is_some_and(|limit| received >= limit) {
            break;
        }
    }

    if let Some((mut child, input)) = display_pipeline {
        drop(input);
        let _ = child.wait();
    }
    Ok(())
}

fn option_value(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

fn spawn_display(width: u64, height: u64) -> Result<(Child, ChildStdin), std::io::Error> {
    let mut child = Command::new("gst-launch-1.0")
        .args([
            "-q",
            "fdsrc",
            "fd=0",
            &format!("blocksize={}", width * height * 3),
            "!",
            "rawvideoparse",
            "format=rgb",
            &format!("width={width}"),
            &format!("height={height}"),
            &format!("framerate={FPS}/1"),
            "!",
            "videoconvert",
            "!",
            "autovideosink",
            "sync=false",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;
    let input = child.stdin.take().expect("display stdin was piped");
    Ok((child, input))
}
