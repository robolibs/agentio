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
pub use directory::{Directory, ResolveRequest, ResolveResponse, TopicEntry, RESOLUTION_TOPIC};
pub use error::{Error, Result};
pub use escape::ById;
pub use authbox;
pub use authbox::did;
pub use identity::{did_key, default_keys_dir, derive_secret_from_name, load_or_generate_key, resolve_identity, save_did_key, IdentitySource};
pub use naming::{qualify_participant_topic, normalize_topic, Machine, NameTable, Participant};
pub use peerbus::DatapodMsg;
