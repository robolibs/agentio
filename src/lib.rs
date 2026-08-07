//! `agentio` — Agent IO interface and agent composition crate built on top of `peerbus`.
//!
//! `agentio` provides identity management (`did:key`), directory tracking, topic referral resolution,
//! participant namespacing, and an ergonomic `Agent` API for agent composition across machines.

pub mod agent;
pub mod directory;
pub mod error;
pub mod escape;
pub mod identity;
pub mod naming;

pub use agent::{Agent, AgentBuilder, DirectoryMode, TryIntoBootstrapPeer};
pub use authbox;
pub use authbox::did;
pub use directory::{
    Directory, DirectoryHealth, ExchangeKind, RESOLUTION_TOPIC, ResolveRequest, ResolveResponse,
    TopicEntry, TopicRecordSpec, TopicWithdrawal,
};
pub use error::{Error, Result};
pub use escape::ById;
pub use identity::{
    IdentitySource, default_keys_dir, derive_secret_from_name, did_key, load_or_generate_key,
    resolve_identity, save_did_key,
};
pub use naming::{Machine, NameTable, Participant, normalize_topic, qualify_participant_topic};
pub use peerbus::DatapodMsg;
