use super::entry::{ExchangeKind, TopicEntry, TopicWithdrawal, unix_time_ms};
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type DirectoryKey = (String, ExchangeKind);

#[derive(Debug, Clone, Default)]
pub struct Directory {
    entries: Arc<RwLock<HashMap<DirectoryKey, TopicEntry>>>,
}

impl Directory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, entry: TopicEntry) -> Result<bool> {
        let now = unix_time_ms();
        entry.verify(now)?;
        let key = (entry.topic().to_string(), entry.exchange());
        let mut map = self.entries.write().unwrap();
        map.retain(|_, current| current.lease_expires_at_ms() > now);
        if let Some(current) = map.get(&key) {
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
                return Ok(current == &entry);
            }
        }
        map.insert(key, entry);
        Ok(true)
    }

    pub fn register_many(&self, entries: impl IntoIterator<Item = TopicEntry>) -> Result<usize> {
        let mut count = 0;
        for entry in entries {
            count += usize::from(self.register(entry)?);
        }
        Ok(count)
    }

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
        let mut map = self.entries.write().unwrap();
        map.retain(|_, entry| entry.lease_expires_at_ms() > now);
        map.get(&key).cloned()
    }

    pub fn lookup(&self, topic: &str) -> Option<TopicEntry> {
        let now = unix_time_ms();
        let mut map = self.entries.write().unwrap();
        map.retain(|_, entry| entry.lease_expires_at_ms() > now);
        map.values().find(|entry| entry.topic() == topic).cloned()
    }

    pub fn all_entries(&self) -> Vec<TopicEntry> {
        let now = unix_time_ms();
        let mut map = self.entries.write().unwrap();
        map.retain(|_, entry| entry.lease_expires_at_ms() > now);
        map.values().cloned().collect()
    }

    pub fn withdraw(&self, withdrawal: &TopicWithdrawal) -> Result<bool> {
        withdrawal.verify()?;
        let key = (withdrawal.topic().to_string(), withdrawal.exchange());
        let mut map = self.entries.write().unwrap();
        let Some(current) = map.get(&key) else {
            return Ok(false);
        };
        if current.endpoint_id() != withdrawal.endpoint_id() {
            return Err(Error::OwnershipConflict {
                topic: withdrawal.topic().to_string(),
            });
        }
        if withdrawal.revision() < current.revision() {
            return Err(Error::StaleRevision {
                topic: withdrawal.topic().to_string(),
            });
        }
        map.remove(&key);
        Ok(true)
    }

    pub fn remove(&self, topic: &str, exchange: ExchangeKind) -> Option<TopicEntry> {
        self.entries
            .write()
            .unwrap()
            .remove(&(topic.to_string(), exchange))
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
