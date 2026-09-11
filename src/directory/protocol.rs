use super::entry::{ExchangeKind, TopicEntry, TopicWithdrawal};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};

/// Reserved peerbus topic for directory control traffic.
pub const RESOLUTION_TOPIC: &str = "__agentio_resolve";
/// Wire-type identifier for encoded directory control messages.
pub const RESOLUTION_TYPE_HASH: u64 = 0xA6E7_1010_5E50_7B00;
/// Maximum records in one directory request or response page.
pub const MAX_DIRECTORY_BATCH: usize = 256;
/// Maximum serialized bytes in one directory control message.
pub const MAX_CONTROL_MESSAGE_BYTES: usize = 256 * 1024;

/// Bounded request sent to the directory resolver service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolveRequest {
    /// Resolve one typed topic.
    Query {
        /// Normalized topic.
        topic: String,
        /// Requested exchange family.
        exchange: ExchangeKind,
        /// Requested request or payload wire hash.
        request_type_hash: u64,
        /// Requested response wire hash.
        response_type_hash: Option<u64>,
    },
    /// Read one stable page from a directory generation.
    List {
        /// Generation returned by the first page, or none to begin.
        generation: Option<u64>,
        /// Zero-based page offset.
        offset: usize,
        /// Maximum requested records.
        limit: usize,
    },
    /// Install signed topic entries.
    Announce {
        /// Signed entries to install.
        entries: Vec<TopicEntry>,
    },
    /// Remove exact signed topic entries.
    Withdraw {
        /// Signed withdrawals to apply.
        entries: Vec<TopicWithdrawal>,
    },
}

/// Response returned by the directory resolver service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolveResponse {
    /// Result of a typed topic query.
    QueryResult {
        /// Whether a matching record exists.
        found: bool,
        /// Verified candidate record when found.
        entry: Option<TopicEntry>,
    },
    /// One stable directory page.
    ListResult {
        /// Directory generation used for this page.
        generation: u64,
        /// Signed records in this page.
        entries: Vec<TopicEntry>,
        /// Offset for the next page, if any.
        next_offset: Option<usize>,
    },
    /// Successful announcement result.
    Announced {
        /// Records inserted or updated.
        count: usize,
    },
    /// Successful withdrawal result.
    Withdrawn {
        /// Records removed.
        count: usize,
    },
    /// Bounded control request rejection.
    Rejected {
        /// Human-readable rejection summary.
        message: String,
    },
}

impl ResolveRequest {
    /// Validate request record-count limits.
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

    /// Serialize this request after enforcing protocol limits.
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

    /// Decode and validate one bounded request.
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
    /// Validate response record-count limits.
    pub fn validate(&self) -> Result<()> {
        let count = match self {
            Self::ListResult { entries, .. } => entries.len(),
            _ => 0,
        };
        if count > MAX_DIRECTORY_BATCH {
            return Err(Error::BatchTooLarge {
                actual: count,
                maximum: MAX_DIRECTORY_BATCH,
            });
        }
        Ok(())
    }

    /// Serialize this response after enforcing protocol limits.
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

    /// Decode and validate one bounded response.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_CONTROL_MESSAGE_BYTES {
            return Err(Error::BatchTooLarge {
                actual: bytes.len(),
                maximum: MAX_CONTROL_MESSAGE_BYTES,
            });
        }
        let response: Self = postcard::from_bytes(bytes)?;
        response.validate()?;
        Ok(response)
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

    #[test]
    fn oversized_response_batch_is_rejected() {
        let response = ResolveResponse::ListResult {
            generation: 1,
            entries: vec![
                TopicEntry::signed(
                    TopicRecordSpec::new(
                        "/topic",
                        ExchangeKind::PubSub,
                        7,
                        None,
                        1,
                        None::<String>,
                    ),
                    &SecretKey::generate(),
                )
                .unwrap();
                MAX_DIRECTORY_BATCH + 1
            ],
            next_offset: None,
        };
        assert!(matches!(
            response.to_bytes(),
            Err(Error::BatchTooLarge { .. })
        ));
    }
}
