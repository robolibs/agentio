//! `agentio` — Agent IO interface and agent composition crate built on top of `peerbus`.
//!
//! `agentio` provides identity management (`did:key`), directory tracking, topic referral resolution,
//! topic namespacing, and an ergonomic `Agent` API for agent composition across machines.

#![warn(missing_docs)]

/// Agent construction, lifecycle, and typed exchange APIs.
pub mod agent;
/// Authenticated directory records, storage, health, and wire protocol.
pub mod directory;
/// Error and result types.
pub mod error;
/// Direct endpoint-addressed escape hatches.
pub mod escape;
/// Identity loading, persistence, and DID helpers.
pub mod identity;
/// Topic normalization and machine-name mappings.
pub mod naming;
/// Python bindings (pyo3), behind the `python` feature.
#[cfg(feature = "python")]
pub mod python;
/// Host-local rendezvous records of live agents.
pub mod rendezvous;

pub use agent::{
    Agent, AgentBuilder, DirectoryMode, HostedGuard, Registered, TryIntoBootstrapPeer,
};
pub use authbox;
pub use authbox::did;
pub use directory::{
    Directory, DirectoryHealth, ExchangeKind, RESOLUTION_TOPIC, ResolveRequest, ResolveResponse,
    TopicEntry, TopicRecordSpec, TopicWithdrawal,
};
pub use error::{Error, Result};
pub use escape::ById;
pub use identity::{
    IdentitySource, default_keys_dir, did_key, load_or_generate_key, resolve_identity, save_did_key,
};
pub use naming::{Machine, NameTable, normalize_topic, qualify_participant_topic};
pub use peerbus::{DatapodMsg, LocalConfig};
pub use rendezvous::{LocalAgent, find_local, find_local_did, local_agents, rendezvous_dir};
