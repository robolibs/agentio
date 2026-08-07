use thiserror::Error;

use crate::directory::ExchangeKind;

/// Result type for `agentio` operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors encountered within `agentio`.
#[derive(Error, Debug)]
pub enum Error {
    #[error("peerbus error: {0}")]
    Peerbus(#[from] peerbus::Error),

    #[error("identity error: {0}")]
    Identity(String),

    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("did:key error: {0}")]
    DidKey(String),

    #[error("resolution failed: {0}")]
    ResolutionFailed(String),

    #[error("control-plane error: {0}")]
    ControlPlane(String),

    #[error("invalid signature for directory record '{topic}'")]
    InvalidSignature { topic: String },

    #[error("directory record '{topic}' has expired")]
    ExpiredRecord { topic: String },

    #[error("live ownership conflict for topic '{topic}'")]
    OwnershipConflict { topic: String },

    #[error("unsupported directory protocol version {0}")]
    UnsupportedProtocol(u16),

    #[error("exchange mismatch for '{topic}': expected {expected:?}, got {actual:?}")]
    ExchangeMismatch {
        topic: String,
        expected: ExchangeKind,
        actual: ExchangeKind,
    },

    #[error("wire type mismatch for directory record '{topic}'")]
    TypeMismatch { topic: String },

    #[error("directory batch contains {actual} entries; maximum is {maximum}")]
    BatchTooLarge { actual: usize, maximum: usize },

    #[error("stale directory revision for topic '{topic}'")]
    StaleRevision { topic: String },

    #[error("invalid topic path '{topic}': {reason}")]
    InvalidTopic { topic: String, reason: String },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("postcard serialization error: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("formatting error: {0}")]
    Format(String),
}
