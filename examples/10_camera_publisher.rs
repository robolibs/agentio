use agentio::{Agent, DirectoryMode, IdentitySource};
use agentio_example_messages::CameraFrame;
use std::env;
use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const WIDTH: usize = 640;
const HEIGHT: usize = 480;
const CHANNELS: usize = 3;
const FPS: usize = 15;
const TOPIC: &str = "/camera/rgb";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let device = option_value(&args, "--device").unwrap_or_else(|| "/dev/video0".to_string());
    let frame_limit = option_value(&args, "--frames")
        .map(|value| value.parse::<u64>())
        .transpose()?;
    let use_shm = args.iter().any(|arg| arg == "--use-shm");
    let test_pattern = args.iter().any(|arg| arg == "--test-pattern");

    let mut builder = Agent::builder()
        .name("camera-publisher")
        .identity(IdentitySource::Random)
        .directory(DirectoryMode::Replicated)
        .allow_any_peer();
    if !use_shm {
        builder = builder.skip_shm();
    }
    let agent = builder.build()?;
    agent.wait_for_direct_addresses(Duration::from_secs(5))?;
    let mut publisher = agent.publish::<CameraFrame>(TOPIC)?;

    println!("Camera publisher ready");
    println!(
        "  Source:   {}",
        if test_pattern {
            "test pattern"
        } else {
            &device
        }
    );
    println!("  Topic:    {TOPIC}");
    println!("  Format:   RGB {WIDTH}x{HEIGHT} at {FPS} FPS");
    println!("  Endpoint: {}", agent.endpoint_id());
    println!("  DID:      {}", agent.did_key()?);

    let caps = format!("video/x-raw,format=RGB,width={WIDTH},height={HEIGHT},framerate={FPS}/1");
    let mut pipeline = vec!["-q".to_string()];
    if test_pattern {
        pipeline.extend([
            "videotestsrc".to_string(),
            "is-live=true".to_string(),
            "pattern=ball".to_string(),
        ]);
    } else {
        pipeline.extend(["v4l2src".to_string(), format!("device={device}")]);
    }
    pipeline.extend([
        "!".to_string(),
        "videoconvert".to_string(),
        "!".to_string(),
        "videoscale".to_string(),
        "!".to_string(),
        caps,
        "!".to_string(),
        "fdsink".to_string(),
        "fd=1".to_string(),
    ]);
    let mut capture = Command::new("gst-launch-1.0")
        .args(pipeline)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut frames = capture
        .stdout
        .take()
        .ok_or("camera pipeline has no stdout")?;
    let frame_bytes = WIDTH
        .checked_mul(HEIGHT)
        .and_then(|pixels| pixels.checked_mul(CHANNELS))
        .ok_or("camera frame size overflow")?;
    let mut sequence = 0u64;

    loop {
        let mut data = vec![0u8; frame_bytes];
        frames.read_exact(&mut data)?;
        sequence = sequence.saturating_add(1);
        publisher.send(&CameraFrame {
            sequence,
            captured_at_ms: unix_time_ms(),
            width: WIDTH as u64,
            height: HEIGHT as u64,
            channels: CHANNELS as u64,
            payload_bytes: data.len() as u64,
            data,
        })?;
        if sequence == 1 || sequence.is_multiple_of(FPS as u64) {
            println!("Published frame {sequence}");
        }
        if frame_limit.is_some_and(|limit| sequence >= limit) {
            break;
        }
    }

    let _ = capture.kill();
    let _ = capture.wait();
    Ok(())
}

fn option_value(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
