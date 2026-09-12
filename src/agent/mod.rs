/// Agent builder configuration.
pub mod builder;
/// Main Agent API and lifecycle.
pub mod core;
/// Directory topology and peer conversion types.
pub mod mode;
/// Hosted handles with directory lifecycle management.
pub mod registered;

pub use builder::AgentBuilder;
pub use core::Agent;
pub use mode::{DirectoryMode, TryIntoBootstrapPeer};
pub use registered::{HostedGuard, Registered};
