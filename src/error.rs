use thiserror::Error;

/// Result type for `agentio` operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors encountered within `agentio`.
#[derive(Error, Debug)]
pub enum Error {
    #[error("peerbus error: {0}")]
    Peerbus(#[from] peerbus::Error),

    #[error("identity error: {0}")]
    Identity(String),

    #[error("did:key error: {0}")]
    DidKey(String),

    #[error("resolution failed: {0}")]
    ResolutionFailed(String),

    #[error("invalid topic path '{topic}': {reason}")]
    InvalidTopic { topic: String, reason: String },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("postcard serialization error: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("formatting error: {0}")]
    Format(String),
}
