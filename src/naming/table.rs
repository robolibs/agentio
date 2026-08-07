use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use peerbus::EndpointId;

/// Thread-safe registry mapping friendly names to `EndpointId`s and vice versa.
#[derive(Debug, Clone, Default)]
pub struct NameTable {
    name_to_id: Arc<RwLock<HashMap<String, EndpointId>>>,
    id_to_name: Arc<RwLock<HashMap<EndpointId, String>>>,
}

impl NameTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, name: impl Into<String>, endpoint_id: EndpointId) {
        let name = name.into();
        let mut n2i = self.name_to_id.write().unwrap();
        let mut i2n = self.id_to_name.write().unwrap();
        n2i.insert(name.clone(), endpoint_id);
        i2n.insert(endpoint_id, name);
    }

    pub fn resolve_name(&self, name: &str) -> Option<EndpointId> {
        self.name_to_id.read().unwrap().get(name).copied()
    }

    pub fn get_name(&self, endpoint_id: &EndpointId) -> Option<String> {
        self.id_to_name.read().unwrap().get(endpoint_id).cloned()
    }

    pub fn all_mappings(&self) -> HashMap<String, EndpointId> {
        self.name_to_id.read().unwrap().clone()
    }
}
