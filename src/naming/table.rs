use peerbus::EndpointId;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::directory::TopicEntry;

#[derive(Debug, Default)]
struct NameMaps {
    name_to_id: HashMap<String, EndpointId>,
    id_to_name: HashMap<EndpointId, String>,
    explicit_name_to_id: HashMap<String, EndpointId>,
    explicit_id_to_name: HashMap<EndpointId, String>,
    directory_supports: HashSet<(String, EndpointId)>,
}

#[derive(Debug, Clone, Default)]
pub struct NameTable {
    maps: Arc<RwLock<NameMaps>>,
}

impl NameTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, name: impl Into<String>, endpoint_id: EndpointId) {
        let name = name.into();
        let mut maps = self.maps.write().unwrap();
        let NameMaps {
            explicit_name_to_id,
            explicit_id_to_name,
            ..
        } = &mut *maps;
        insert_mapping(explicit_name_to_id, explicit_id_to_name, name, endpoint_id);
        rebuild(&mut maps);
    }

    pub(crate) fn sync_from_entries(&self, entries: &[TopicEntry]) {
        let supports = entries
            .iter()
            .filter_map(|entry| {
                entry
                    .machine_name()
                    .map(|name| (name.to_string(), entry.endpoint_id()))
            })
            .collect();
        let mut maps = self.maps.write().unwrap();
        maps.directory_supports = supports;
        rebuild(&mut maps);
    }

    pub fn resolve_name(&self, name: &str) -> Option<EndpointId> {
        self.maps.read().unwrap().name_to_id.get(name).copied()
    }

    pub fn get_name(&self, endpoint_id: &EndpointId) -> Option<String> {
        self.maps
            .read()
            .unwrap()
            .id_to_name
            .get(endpoint_id)
            .cloned()
    }

    pub fn all_mappings(&self) -> HashMap<String, EndpointId> {
        self.maps.read().unwrap().name_to_id.clone()
    }
}

fn insert_mapping(
    name_to_id: &mut HashMap<String, EndpointId>,
    id_to_name: &mut HashMap<EndpointId, String>,
    name: String,
    endpoint_id: EndpointId,
) {
    if let Some(previous_id) = name_to_id.insert(name.clone(), endpoint_id)
        && previous_id != endpoint_id
    {
        id_to_name.remove(&previous_id);
    }
    if let Some(previous_name) = id_to_name.insert(endpoint_id, name.clone())
        && previous_name != name
    {
        name_to_id.remove(&previous_name);
    }
}

fn rebuild(maps: &mut NameMaps) {
    let mut name_to_id = HashMap::new();
    let mut id_to_name = HashMap::new();
    let mut supported: Vec<_> = maps.directory_supports.iter().cloned().collect();
    supported.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.as_bytes().cmp(right.1.as_bytes()))
    });
    for (name, endpoint_id) in supported {
        insert_mapping(&mut name_to_id, &mut id_to_name, name, endpoint_id);
    }
    let mut explicit: Vec<_> = maps
        .explicit_name_to_id
        .iter()
        .map(|(name, endpoint_id)| (name.clone(), *endpoint_id))
        .collect();
    explicit.sort_by(|left, right| left.0.cmp(&right.0));
    for (name, endpoint_id) in explicit {
        insert_mapping(&mut name_to_id, &mut id_to_name, name, endpoint_id);
    }
    maps.name_to_id = name_to_id;
    maps.id_to_name = id_to_name;
}

#[cfg(test)]
mod tests {
    use super::*;
    use peerbus::SecretKey;

    #[test]
    fn same_mapping_is_idempotent() {
        let table = NameTable::new();
        let endpoint = SecretKey::generate().public();
        table.register("head", endpoint);
        table.register("head", endpoint);
        assert_eq!(table.all_mappings().len(), 1);
        assert_eq!(table.resolve_name("head"), Some(endpoint));
        assert_eq!(table.get_name(&endpoint).as_deref(), Some("head"));
    }

    #[test]
    fn reassigning_name_removes_old_reverse_mapping() {
        let table = NameTable::new();
        let old = SecretKey::generate().public();
        let new = SecretKey::generate().public();
        table.register("head", old);
        table.register("head", new);
        assert_eq!(table.resolve_name("head"), Some(new));
        assert_eq!(table.get_name(&old), None);
    }

    #[test]
    fn reassigning_endpoint_removes_old_forward_mapping() {
        let table = NameTable::new();
        let endpoint = SecretKey::generate().public();
        table.register("old", endpoint);
        table.register("new", endpoint);
        assert_eq!(table.resolve_name("old"), None);
        assert_eq!(table.resolve_name("new"), Some(endpoint));
        assert_eq!(table.get_name(&endpoint).as_deref(), Some("new"));
    }
}
