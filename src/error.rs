use thiserror::Error;

use crate::directory::ExchangeKind;

/// Result type for `agentio` operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors encountered within `agentio`.
#[derive(Error, Debug)]
pub enum Error {
    /// Underlying peerbus failure.
    #[error("peerbus error: {0}")]
    Peerbus(#[from] peerbus::Error),

    /// Identity loading or persistence failure.
    #[error("identity error: {0}")]
    Identity(String),

    /// Invalid builder configuration.
    #[error("configuration error: {0}")]
    Configuration(String),

    /// DID conversion or validation failure.
    #[error("did:key error: {0}")]
    DidKey(String),

    /// Topic resolution failure.
    #[error("resolution failed: {0}")]
    ResolutionFailed(String),

    /// Directory control-plane failure.
    #[error("control-plane error: {0}")]
    ControlPlane(String),

    /// Invalid owner signature.
    #[error("invalid signature for directory record '{topic}'")]
    InvalidSignature {
        /// Affected topic.
        topic: String,
    },

    /// Record lease has expired.
    #[error("directory record '{topic}' has expired")]
    ExpiredRecord {
        /// Affected topic.
        topic: String,
    },

    /// Different live owners claimed the same topic and exchange.
    #[error("live ownership conflict for topic '{topic}'")]
    OwnershipConflict {
        /// Affected topic.
        topic: String,
    },

    /// Unsupported directory wire version.
    #[error("unsupported directory protocol version {0}")]
    UnsupportedProtocol(u16),

    /// Signed record uses the wrong directory operation domain.
    #[error("invalid directory operation for topic '{topic}'")]
    InvalidDirectoryOperation {
        /// Affected topic.
        topic: String,
    },

    /// Resolved record uses a different exchange family.
    #[error("exchange mismatch for '{topic}': expected {expected:?}, got {actual:?}")]
    ExchangeMismatch {
        /// Affected topic.
        topic: String,
        /// Requested exchange family.
        expected: ExchangeKind,
        /// Record exchange family.
        actual: ExchangeKind,
    },

    /// Resolved record uses different wire types.
    #[error("wire type mismatch for directory record '{topic}'")]
    TypeMismatch {
        /// Affected topic.
        topic: String,
    },

    /// Directory batch or message exceeds its configured boundary.
    #[error("directory batch contains {actual} entries; maximum is {maximum}")]
    BatchTooLarge {
        /// Observed count or byte size.
        actual: usize,
        /// Maximum accepted count or byte size.
        maximum: usize,
    },

    /// Record or withdrawal revision is older than current state.
    #[error("stale directory revision for topic '{topic}'")]
    StaleRevision {
        /// Affected topic.
        topic: String,
    },

    /// Distinct signed records use the same owner revision.
    #[error("conflicting directory records use the same revision for topic '{topic}'")]
    RevisionConflict {
        /// Affected topic.
        topic: String,
    },

    /// Directory changed while a stable page sequence was in progress.
    #[error("directory snapshot changed from generation {expected} to {actual}")]
    SnapshotChanged {
        /// Generation requested by the client.
        expected: u64,
        /// Current directory generation.
        actual: u64,
    },

    /// Caller-visible control operation exceeded its deadline.
    #[error("control-plane operation timed out after {0:?}")]
    ControlTimeout(std::time::Duration),

    /// Topic path is not valid or normalized.
    #[error("invalid topic path '{topic}': {reason}")]
    InvalidTopic {
        /// Rejected topic text.
        topic: String,
        /// Validation failure summary.
        reason: String,
    },

    /// Filesystem or operating-system I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Postcard wire encoding or decoding failure.
    #[error("postcard serialization error: {0}")]
    Postcard(#[from] postcard::Error),

    /// Text formatting failure.
    #[error("formatting error: {0}")]
    Format(String),
}
