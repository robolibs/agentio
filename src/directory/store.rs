use super::entry::TopicEntry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// In-memory directory of topic definitions known to this Machine.
#[derive(Debug, Clone, Default)]
pub struct Directory {
    entries: Arc<RwLock<HashMap<String, TopicEntry>>>,
}

impl Directory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register or update a topic entry in the local directory.
    pub fn register(&self, entry: TopicEntry) {
        let mut map = self.entries.write().unwrap();
        map.insert(entry.topic.clone(), entry);
    }

    /// Register multiple topic entries.
    pub fn register_many(&self, entries: impl IntoIterator<Item = TopicEntry>) {
        let mut map = self.entries.write().unwrap();
        for entry in entries {
            map.insert(entry.topic.clone(), entry);
        }
    }

    /// Lookup a topic entry by its normalized topic path.
    pub fn lookup(&self, topic: &str) -> Option<TopicEntry> {
        self.entries.read().unwrap().get(topic).cloned()
    }

    /// Return all registered topic entries.
    pub fn all_entries(&self) -> Vec<TopicEntry> {
        self.entries.read().unwrap().values().cloned().collect()
    }

    /// Remove a topic entry.
    pub fn remove(&self, topic: &str) -> Option<TopicEntry> {
        self.entries.write().unwrap().remove(topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use peerbus::SecretKey;

    #[test]
    fn test_directory_operations() {
        let dir = Directory::new();
        let id = SecretKey::generate().public();
        let entry = TopicEntry::new("/perception/pose", 12345, id, Some("head"));

        dir.register(entry.clone());
        assert_eq!(dir.lookup("/perception/pose"), Some(entry));
        assert_eq!(dir.all_entries().len(), 1);

        dir.remove("/perception/pose");
        assert_eq!(dir.lookup("/perception/pose"), None);
    }
}
