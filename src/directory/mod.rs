/// Signed topic entries and withdrawals.
pub mod entry;
/// Observable control-plane health counters.
pub mod health;
/// Bounded directory wire requests and responses.
pub mod protocol;
/// Verified in-memory directory storage.
pub mod store;

pub(crate) use entry::next_revision_seed;
pub use entry::{
    DEFAULT_LEASE_DURATION, DIRECTORY_PROTOCOL_VERSION, ExchangeKind, TopicEntry, TopicRecordSpec,
    TopicWithdrawal, default_lease_deadline_ms, unix_time_ms,
};
pub(crate) use health::ControlPlaneHealth;
pub use health::DirectoryHealth;
pub use protocol::{
    MAX_CONTROL_MESSAGE_BYTES, MAX_DIRECTORY_BATCH, RESOLUTION_TOPIC, RESOLUTION_TYPE_HASH,
    ResolveRequest, ResolveResponse,
};
pub use store::Directory;
