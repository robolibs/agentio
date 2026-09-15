use datapod::datapod;

#[datapod(name = "agentio.camera_rgb.v1")]
pub struct CameraFrame {
    pub sequence: u64,
    pub captured_at_ms: u64,
    pub width: u64,
    pub height: u64,
    pub channels: u64,
    pub payload_bytes: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
}

#[datapod(name = "heavy.image_header.v1")]
pub struct HeavyImageHeader {
    pub frame_id: u64,
    pub width: u64,
    pub height: u64,
    pub payload_bytes: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
}

#[datapod(name = "heavy.rpc_req.v1")]
pub struct HeavyRpcReq {
    pub requested_bytes: u64,
}

#[datapod(name = "heavy.rpc_res.v1")]
pub struct HeavyRpcRes {
    pub checksum: u64,
    pub payload_bytes: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
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
    #[dp(bytes)]
    pub data: Vec<u8>,
}

#[datapod(name = "heavy.upload_block.v1")]
pub struct HeavyUploadBlock {
    pub block_index: u64,
    pub payload_bytes: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
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
    #[dp(bytes)]
    pub data: Vec<u8>,
}

#[datapod(name = "heavy.pip_ack.v1")]
pub struct HeavyPipAck {
    pub ack_seq: u64,
    pub bytes_received: u64,
    #[dp(bytes)]
    pub data: Vec<u8>,
}
