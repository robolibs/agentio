/// Machine identity metadata.
pub mod machine;
/// Bidirectional machine-name mappings.
pub mod table;
/// Topic normalization and participant qualification.
pub mod topic;

pub use machine::Machine;
pub use table::NameTable;
pub use topic::{normalize_topic, qualify_participant_topic};
