use peerbus::EndpointId;
use serde::{Deserialize, Serialize};

/// Information about a topic hosted within an Agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicEntry {
    /// Normalized absolute topic path (e.g. `/perception/pose`).
    pub topic: String,
    /// Datapod type hash for payload type checking.
    pub type_hash: u64,
    /// `EndpointId` of the Machine hosting this topic (32-byte array).
    pub endpoint_id: [u8; 32],
    /// Optional human-readable machine name (e.g. "head").
    pub machine_name: Option<String>,
}

impl TopicEntry {
    pub fn new(
        topic: impl Into<String>,
        type_hash: u64,
        endpoint_id: EndpointId,
        machine_name: Option<impl Into<String>>,
    ) -> Self {
        Self {
            topic: topic.into(),
            type_hash,
            endpoint_id: *endpoint_id.as_bytes(),
            machine_name: machine_name.map(Into::into),
        }
    }

    pub fn endpoint_id(&self) -> EndpointId {
        EndpointId::from_bytes(&self.endpoint_id).expect("valid endpoint id bytes")
    }
}
