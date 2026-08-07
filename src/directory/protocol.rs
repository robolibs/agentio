use serde::{Deserialize, Serialize};
use crate::error::{Error, Result};
use super::entry::TopicEntry;

/// Reserved topic name used across an Agent for referral resolution.
pub const RESOLUTION_TOPIC: &str = "__agentio_resolve";
/// Unique type hash identifier for resolution messages.
pub const RESOLUTION_TYPE_HASH: u64 = 0xA6E7_1010_5E50_7B00;

/// Request payload sent over peerbus req/res to resolve topics or sync directory state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolveRequest {
    /// Query the owner `EndpointId` and type hash for a specific topic.
    Query { topic: String },
    /// Request all topic entries in the directory.
    List,
    /// Announce newly registered topic entries.
    Announce { entries: Vec<TopicEntry> },
}

/// Response payload returned by resolution queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolveResponse {
    /// Result for `Query`.
    QueryResult { found: bool, entry: Option<TopicEntry> },
    /// Result for `List`.
    ListResult { entries: Vec<TopicEntry> },
    /// Result for `Announce`.
    Announced { count: usize },
}

impl ResolveRequest {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self).map_err(Error::from)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        postcard::from_bytes(bytes).map_err(Error::from)
    }
}

impl ResolveResponse {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self).map_err(Error::from)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        postcard::from_bytes(bytes).map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_serialization() {
        let req = ResolveRequest::Query {
            topic: "/perception/pose".to_string(),
        };
        let bytes = req.to_bytes().unwrap();
        let decoded = ResolveRequest::from_bytes(&bytes).unwrap();
        if let ResolveRequest::Query { topic } = decoded {
            assert_eq!(topic, "/perception/pose");
        } else {
            panic!("mismatched variant");
        }
    }
}
