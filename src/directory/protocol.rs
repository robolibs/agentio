use super::entry::{ExchangeKind, TopicEntry, TopicWithdrawal};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};

pub const RESOLUTION_TOPIC: &str = "__agentio_resolve";
pub const RESOLUTION_TYPE_HASH: u64 = 0xA6E7_1010_5E50_7B00;
pub const MAX_DIRECTORY_BATCH: usize = 256;
pub const MAX_CONTROL_MESSAGE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolveRequest {
    Query {
        topic: String,
        exchange: ExchangeKind,
        request_type_hash: u64,
        response_type_hash: Option<u64>,
    },
    List {
        offset: usize,
        limit: usize,
    },
    Announce {
        entries: Vec<TopicEntry>,
    },
    Withdraw {
        entries: Vec<TopicWithdrawal>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolveResponse {
    QueryResult {
        found: bool,
        entry: Option<TopicEntry>,
    },
    ListResult {
        entries: Vec<TopicEntry>,
        next_offset: Option<usize>,
    },
    Announced {
        count: usize,
    },
    Withdrawn {
        count: usize,
    },
    Rejected {
        message: String,
    },
}

impl ResolveRequest {
    pub fn validate(&self) -> Result<()> {
        let count = match self {
            Self::Announce { entries } => entries.len(),
            Self::Withdraw { entries } => entries.len(),
            Self::List { limit, .. } => *limit,
            Self::Query { .. } => 0,
        };
        if count > MAX_DIRECTORY_BATCH {
            return Err(Error::BatchTooLarge {
                actual: count,
                maximum: MAX_DIRECTORY_BATCH,
            });
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = postcard::to_allocvec(self)?;
        if bytes.len() > MAX_CONTROL_MESSAGE_BYTES {
            return Err(Error::BatchTooLarge {
                actual: bytes.len(),
                maximum: MAX_CONTROL_MESSAGE_BYTES,
            });
        }
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_CONTROL_MESSAGE_BYTES {
            return Err(Error::BatchTooLarge {
                actual: bytes.len(),
                maximum: MAX_CONTROL_MESSAGE_BYTES,
            });
        }
        let request: Self = postcard::from_bytes(bytes)?;
        request.validate()?;
        Ok(request)
    }
}

impl ResolveResponse {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let bytes = postcard::to_allocvec(self)?;
        if bytes.len() > MAX_CONTROL_MESSAGE_BYTES {
            return Err(Error::BatchTooLarge {
                actual: bytes.len(),
                maximum: MAX_CONTROL_MESSAGE_BYTES,
            });
        }
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_CONTROL_MESSAGE_BYTES {
            return Err(Error::BatchTooLarge {
                actual: bytes.len(),
                maximum: MAX_CONTROL_MESSAGE_BYTES,
            });
        }
        postcard::from_bytes(bytes).map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::TopicRecordSpec;
    use peerbus::SecretKey;

    #[test]
    fn query_round_trips() {
        let request = ResolveRequest::Query {
            topic: "/perception/pose".to_string(),
            exchange: ExchangeKind::PubSub,
            request_type_hash: 7,
            response_type_hash: None,
        };
        let bytes = request.to_bytes().unwrap();
        assert!(matches!(
            ResolveRequest::from_bytes(&bytes).unwrap(),
            ResolveRequest::Query { .. }
        ));
    }

    #[test]
    fn oversized_batch_is_rejected() {
        let secret = SecretKey::generate();
        let entries = (0..=MAX_DIRECTORY_BATCH)
            .map(|index| {
                TopicEntry::signed(
                    TopicRecordSpec::new(
                        format!("/topic/{index}"),
                        ExchangeKind::PubSub,
                        7,
                        None,
                        index as u64 + 1,
                        None::<String>,
                    ),
                    &secret,
                )
                .unwrap()
            })
            .collect();
        let request = ResolveRequest::Announce { entries };
        assert!(matches!(
            request.to_bytes(),
            Err(Error::BatchTooLarge { .. })
        ));
    }
}
