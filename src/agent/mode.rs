use crate::error::Result;
use crate::identity::did_key;
use peerbus::EndpointId;

/// Directory mode for topic resolution within a Robot composition.
#[derive(Debug, Clone)]
pub enum DirectoryMode {
    /// Replicated directory model: every Machine maintains a directory and answers resolution requests.
    Replicated,
    /// Front-door model: a specific Machine `EndpointId` acts as the authority for directory resolution.
    FrontDoor(EndpointId),
}

/// Trait for items that can be converted into a bootstrap peer `EndpointId`.
pub trait TryIntoBootstrapPeer {
    /// Convert this value into a validated endpoint identifier.
    fn try_into_bootstrap_peer(self) -> Result<EndpointId>;
}

impl TryIntoBootstrapPeer for EndpointId {
    fn try_into_bootstrap_peer(self) -> Result<EndpointId> {
        Ok(self)
    }
}

impl TryIntoBootstrapPeer for &EndpointId {
    fn try_into_bootstrap_peer(self) -> Result<EndpointId> {
        Ok(*self)
    }
}

impl TryIntoBootstrapPeer for &str {
    fn try_into_bootstrap_peer(self) -> Result<EndpointId> {
        did_key::did_key_to_endpoint(self)
    }
}

impl TryIntoBootstrapPeer for String {
    fn try_into_bootstrap_peer(self) -> Result<EndpointId> {
        did_key::did_key_to_endpoint(&self)
    }
}

impl TryIntoBootstrapPeer for &String {
    fn try_into_bootstrap_peer(self) -> Result<EndpointId> {
        did_key::did_key_to_endpoint(self)
    }
}
