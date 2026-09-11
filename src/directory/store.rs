use super::entry::{ExchangeKind, TopicEntry, TopicWithdrawal, unix_time_ms};
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type DirectoryKey = (String, ExchangeKind);

#[derive(Debug, Default)]
struct DirectoryState {
    entries: HashMap<DirectoryKey, TopicEntry>,
    generation: u64,
}

#[derive(Debug, Clone, Default)]
/// Concurrent verified directory with leased, owner-scoped records.
pub struct Directory {
    state: Arc<RwLock<DirectoryState>>,
}

impl Directory {
    /// Create an empty directory.
    pub fn new() -> Self {
        Self::default()
    }

    /// Verify and register one entry, returning whether state changed.
    pub fn register(&self, entry: TopicEntry) -> Result<bool> {
        let now = unix_time_ms();
        entry.verify(now)?;
        let mut state = self.state.write().unwrap();
        prune_expired(&mut state, now);
        let changed = apply_entry(&mut state.entries, entry)?;
        state.generation = state.generation.wrapping_add(u64::from(changed));
        Ok(changed)
    }

    /// Verify and register multiple entries.
    pub fn register_many(&self, entries: impl IntoIterator<Item = TopicEntry>) -> Result<usize> {
        let entries: Vec<_> = entries.into_iter().collect();
        let now = unix_time_ms();
        for entry in &entries {
            entry.verify(now)?;
        }
        let mut state = self.state.write().unwrap();
        prune_expired(&mut state, now);
        let mut candidate = state.entries.clone();
        let mut count = 0;
        for entry in entries {
            count += usize::from(apply_entry(&mut candidate, entry)?);
        }
        if count > 0 {
            state.entries = candidate;
            state.generation = state.generation.wrapping_add(1);
        }
        Ok(count)
    }

    /// Look up one live record by normalized topic and exchange family.
    pub fn lookup_exchange(&self, topic: &str, exchange: ExchangeKind) -> Option<TopicEntry> {
        self.lookup_exchange_at(topic, exchange, unix_time_ms())
    }

    fn lookup_exchange_at(
        &self,
        topic: &str,
        exchange: ExchangeKind,
        now: u64,
    ) -> Option<TopicEntry> {
        let key = (topic.to_string(), exchange);
        let mut state = self.state.write().unwrap();
        prune_expired(&mut state, now);
        state.entries.get(&key).cloned()
    }

    /// Look up any live exchange record for a normalized topic.
    pub fn lookup(&self, topic: &str) -> Option<TopicEntry> {
        let now = unix_time_ms();
        let mut state = self.state.write().unwrap();
        prune_expired(&mut state, now);
        state
            .entries
            .values()
            .find(|entry| entry.topic() == topic)
            .cloned()
    }

    /// Return all live entries in deterministic order.
    pub fn all_entries(&self) -> Vec<TopicEntry> {
        let now = unix_time_ms();
        let mut state = self.state.write().unwrap();
        prune_expired(&mut state, now);
        sorted_entries(&state.entries)
    }

    #[cfg(test)]
    pub(crate) fn prune_expired_at(&self, now: u64) {
        prune_expired(&mut self.state.write().unwrap(), now);
    }

    pub(crate) fn snapshot_page(
        &self,
        expected_generation: Option<u64>,
        offset: usize,
        limit: usize,
    ) -> Result<(u64, Vec<TopicEntry>, Option<usize>)> {
        let mut state = self.state.write().unwrap();
        prune_expired(&mut state, unix_time_ms());
        if let Some(expected) = expected_generation
            && expected != state.generation
        {
            return Err(Error::SnapshotChanged {
                expected,
                actual: state.generation,
            });
        }
        let entries = sorted_entries(&state.entries);
        let end = offset.saturating_add(limit).min(entries.len());
        let page = entries.get(offset..end).unwrap_or(&[]).to_vec();
        Ok((state.generation, page, (end < entries.len()).then_some(end)))
    }

    /// Verify and apply a withdrawal bound to the exact current record.
    pub fn withdraw(&self, withdrawal: &TopicWithdrawal) -> Result<bool> {
        withdrawal.verify()?;
        let key = (withdrawal.topic().to_string(), withdrawal.exchange());
        let mut state = self.state.write().unwrap();
        prune_expired(&mut state, unix_time_ms());
        let Some(current) = state.entries.get(&key) else {
            return Ok(false);
        };
        if current.endpoint_id() != withdrawal.endpoint_id() {
            return Err(Error::OwnershipConflict {
                topic: withdrawal.topic().to_string(),
            });
        }
        if !withdrawal.targets(current) || withdrawal.revision() <= current.revision() {
            return Err(Error::StaleRevision {
                topic: withdrawal.topic().to_string(),
            });
        }
        state.entries.remove(&key);
        state.generation = state.generation.wrapping_add(1);
        Ok(true)
    }

    /// Remove a local record without a signed remote withdrawal.
    pub fn remove(&self, topic: &str, exchange: ExchangeKind) -> Option<TopicEntry> {
        let mut state = self.state.write().unwrap();
        let removed = state.entries.remove(&(topic.to_string(), exchange));
        if removed.is_some() {
            state.generation = state.generation.wrapping_add(1);
        }
        removed
    }
}

fn prune_expired(state: &mut DirectoryState, now: u64) {
    let before = state.entries.len();
    state
        .entries
        .retain(|_, entry| entry.lease_expires_at_ms() > now);
    if state.entries.len() != before {
        state.generation = state.generation.wrapping_add(1);
    }
}

fn apply_entry(entries: &mut HashMap<DirectoryKey, TopicEntry>, entry: TopicEntry) -> Result<bool> {
    let key = (entry.topic().to_string(), entry.exchange());
    if let Some(current) = entries.get(&key) {
        if current.endpoint_id() != entry.endpoint_id() {
            return Err(Error::OwnershipConflict {
                topic: entry.topic().to_string(),
            });
        }
        if entry.revision() < current.revision() {
            return Err(Error::StaleRevision {
                topic: entry.topic().to_string(),
            });
        }
        if entry.revision() == current.revision() {
            if current == &entry {
                return Ok(false);
            }
            return Err(Error::RevisionConflict {
                topic: entry.topic().to_string(),
            });
        }
    }
    entries.insert(key, entry);
    Ok(true)
}

fn sorted_entries(entries: &HashMap<DirectoryKey, TopicEntry>) -> Vec<TopicEntry> {
    let mut values: Vec<_> = entries.values().cloned().collect();
    values.sort_by(|left, right| {
        left.topic()
            .cmp(right.topic())
            .then_with(|| exchange_rank(left.exchange()).cmp(&exchange_rank(right.exchange())))
            .then_with(|| {
                left.endpoint_id()
                    .as_bytes()
                    .cmp(right.endpoint_id().as_bytes())
            })
            .then_with(|| left.revision().cmp(&right.revision()))
    });
    values
}

fn exchange_rank(exchange: ExchangeKind) -> u8 {
    match exchange {
        ExchangeKind::PubSub => 0,
        ExchangeKind::ReqRes => 1,
        ExchangeKind::QueAns => 2,
        ExchangeKind::PutAck => 3,
        ExchangeKind::Pip => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::{TopicRecordSpec, TopicWithdrawal};
    use peerbus::SecretKey;

    fn entry(secret: &SecretKey, revision: u64) -> TopicEntry {
        TopicEntry::signed(
            TopicRecordSpec::new(
                "/perception/pose",
                ExchangeKind::PubSub,
                12345,
                None,
                revision,
                Some("head"),
            ),
            secret,
        )
        .unwrap()
    }

    #[test]
    fn directory_accepts_valid_owner_updates_and_withdrawal() {
        let directory = Directory::new();
        let secret = SecretKey::generate();
        let first = entry(&secret, 1);
        let second = entry(&secret, 2);

        assert!(directory.register(first).unwrap());
        assert!(directory.register(second.clone()).unwrap());
        assert_eq!(directory.lookup("/perception/pose"), Some(second.clone()));

        let withdrawal = TopicWithdrawal::signed(&second, 3, &secret).unwrap();
        assert!(directory.withdraw(&withdrawal).unwrap());
        assert_eq!(directory.lookup("/perception/pose"), None);
    }

    #[test]
    fn competing_live_owner_is_rejected() {
        let directory = Directory::new();
        directory
            .register(entry(&SecretKey::generate(), 1))
            .unwrap();
        let conflict = directory.register(entry(&SecretKey::generate(), 1));
        assert!(matches!(conflict, Err(Error::OwnershipConflict { .. })));
    }

    #[test]
    fn tampered_signature_is_rejected() {
        let directory = Directory::new();
        let mut record = entry(&SecretKey::generate(), 1);
        record.signature_mut()[0] ^= 1;
        assert!(matches!(
            directory.register(record),
            Err(Error::InvalidSignature { .. })
        ));
    }

    #[test]
    fn older_revision_cannot_replace_newer_state() {
        let directory = Directory::new();
        let secret = SecretKey::generate();
        directory.register(entry(&secret, 2)).unwrap();
        assert!(matches!(
            directory.register(entry(&secret, 1)),
            Err(Error::StaleRevision { .. })
        ));
    }

    #[test]
    fn equal_revision_requires_the_identical_record() {
        let directory = Directory::new();
        let secret = SecretKey::generate();
        let first = entry(&secret, 1);
        let changed = TopicEntry::signed(
            TopicRecordSpec::new(
                "/perception/pose",
                ExchangeKind::PubSub,
                12345,
                None,
                1,
                Some("head"),
            )
            .lease_expires_at_ms(first.lease_expires_at_ms().saturating_add(1)),
            &secret,
        )
        .unwrap();
        assert!(directory.register(first.clone()).unwrap());
        assert!(!directory.register(first).unwrap());
        assert!(matches!(
            directory.register(changed),
            Err(Error::RevisionConflict { .. })
        ));
    }

    #[test]
    fn withdrawal_only_removes_the_exact_signed_record() {
        let directory = Directory::new();
        let secret = SecretKey::generate();
        let old = entry(&secret, 1);
        let withdrawal = TopicWithdrawal::signed(&old, 2, &secret).unwrap();
        directory.register(old).unwrap();
        directory.remove("/perception/pose", ExchangeKind::PubSub);
        let replacement = entry(&secret, 3);
        directory.register(replacement.clone()).unwrap();
        assert!(matches!(
            directory.withdraw(&withdrawal),
            Err(Error::StaleRevision { .. })
        ));
        assert_eq!(directory.lookup("/perception/pose"), Some(replacement));
    }

    #[test]
    fn rejected_batch_does_not_partially_mutate() {
        let directory = Directory::new();
        let first_owner = SecretKey::generate();
        let other_owner = SecretKey::generate();
        let current = entry(&first_owner, 1);
        directory.register(current).unwrap();
        let valid = TopicEntry::signed(
            TopicRecordSpec::new(
                "/batch/valid",
                ExchangeKind::PubSub,
                1,
                None,
                1,
                None::<String>,
            ),
            &first_owner,
        )
        .unwrap();
        let conflict = entry(&other_owner, 2);
        assert!(directory.register_many([valid, conflict]).is_err());
        assert!(directory.lookup("/batch/valid").is_none());
    }

    #[test]
    fn pagination_detects_generation_changes() {
        let directory = Directory::new();
        let secret = SecretKey::generate();
        for index in 0..3 {
            directory
                .register(
                    TopicEntry::signed(
                        TopicRecordSpec::new(
                            format!("/page/{index}"),
                            ExchangeKind::PubSub,
                            1,
                            None,
                            index + 1,
                            None::<String>,
                        ),
                        &secret,
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let (generation, first, next) = directory.snapshot_page(None, 0, 2).unwrap();
        assert_eq!(first.len(), 2);
        directory
            .register(
                TopicEntry::signed(
                    TopicRecordSpec::new(
                        "/page/new",
                        ExchangeKind::PubSub,
                        1,
                        None,
                        4,
                        None::<String>,
                    ),
                    &secret,
                )
                .unwrap(),
            )
            .unwrap();
        assert!(matches!(
            directory.snapshot_page(Some(generation), next.unwrap(), 2),
            Err(Error::SnapshotChanged { .. })
        ));
    }

    #[test]
    fn expired_records_are_not_resolved() {
        let directory = Directory::new();
        let secret = SecretKey::generate();
        let deadline = crate::directory::unix_time_ms().saturating_add(100);
        let record = TopicEntry::signed(
            TopicRecordSpec::new(
                "/short-lived",
                ExchangeKind::PubSub,
                1,
                None,
                1,
                None::<String>,
            )
            .lease_expires_at_ms(deadline),
            &secret,
        )
        .unwrap();
        directory.register(record).unwrap();
        assert_eq!(
            directory.lookup_exchange_at("/short-lived", ExchangeKind::PubSub, deadline),
            None
        );
    }
}
