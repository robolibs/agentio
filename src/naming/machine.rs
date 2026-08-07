use crate::error::Result;
use crate::identity::did_key;
use peerbus::EndpointId;

/// Representation of a Machine: one peerbus node instance bound to one `EndpointId`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Machine {
    /// Canonical `EndpointId` of the Machine.
    pub endpoint_id: EndpointId,
    /// Friendly machine name (e.g. "head", "base", "arm").
    pub name: Option<String>,
}

impl Machine {
    pub fn new(endpoint_id: EndpointId, name: Option<impl Into<String>>) -> Self {
        Self {
            endpoint_id,
            name: name.map(Into::into),
        }
    }

    pub fn did_key(&self) -> Result<String> {
        did_key::endpoint_to_did_key(&self.endpoint_id)
    }
}

/// Representation of a Participant ("node" in ROS/robot jargon): a logical module
/// such as `camera` or `planner` running on a Machine.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Participant {
    /// Logical name of the participant.
    pub name: String,
    /// Machine hosting this participant.
    pub machine_id: EndpointId,
}
