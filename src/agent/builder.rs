use peerbus::{DatapodMsg, EndpointId, Node};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::core::{Agent, AgentInner, run_resolution_loop};
use super::mode::{DirectoryMode, TryIntoBootstrapPeer};
use crate::directory::{Directory, RESOLUTION_TOPIC};
use crate::error::Result;
use crate::identity::{IdentitySource, resolve_identity};
use crate::naming::NameTable;

/// Builder for constructing an `Agent` instance.
pub struct AgentBuilder {
    pub(crate) name: Option<String>,
    pub(crate) identity_source: IdentitySource,
    pub(crate) directory_mode: DirectoryMode,
    pub(crate) bootstrap_peers: Vec<EndpointId>,
    pub(crate) allow_any_peer: bool,
    pub(crate) no_relay: bool,
    pub(crate) skip_shm: bool,
}

impl AgentBuilder {
    pub fn new() -> Self {
        Self {
            name: None,
            identity_source: IdentitySource::Ephemeral,
            directory_mode: DirectoryMode::Replicated,
            bootstrap_peers: Vec::new(),
            allow_any_peer: false,
            no_relay: false,
            skip_shm: false,
        }
    }

    /// Set a human-readable name for this Machine / Agent instance (e.g. "agent-1", "head").
    /// Automatically persists key at `~/.local/share/agentio/keys/name/{name}.key` unless custom identity is set.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        let n = name.into();
        if matches!(self.identity_source, IdentitySource::Ephemeral) {
            self.identity_source = IdentitySource::Name(n.clone());
        }
        self.name = Some(n);
        self
    }

    /// Set the key source identity.
    pub fn identity(mut self, source: impl Into<IdentitySource>) -> Self {
        self.identity_source = source.into();
        self
    }

    /// Convenience helper to load or store the Machine's ed25519 secret key file.
    pub fn machine_key_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.identity_source = IdentitySource::File(path.into());
        self
    }

    /// Configure the directory resolution mode (e.g. `Replicated` or `FrontDoor(id)`).
    pub fn directory(mut self, mode: DirectoryMode) -> Self {
        self.directory_mode = mode;
        self
    }

    /// Add one or more bootstrap peers to the Agent membership configuration.
    pub fn bootstrap<I, P>(mut self, peers: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: TryIntoBootstrapPeer,
    {
        for p in peers {
            if let Ok(peer) = p.try_into_bootstrap_peer()
                && !self.bootstrap_peers.contains(&peer)
            {
                self.bootstrap_peers.push(peer);
            }
        }
        self
    }

    /// Opt-out of the default inbound peer allowlist.
    pub fn allow_any_peer(mut self) -> Self {
        self.allow_any_peer = true;
        self
    }

    /// Disable iroh relay fallback.
    pub fn no_relay(mut self) -> Self {
        self.no_relay = true;
        self
    }

    /// Skip shared memory transport and force all communications over network/sockets (iroh QUIC).
    pub fn skip_shm(mut self) -> Self {
        self.skip_shm = true;
        self
    }

    /// Build the `Agent` machine instance.
    pub fn build(self) -> Result<Agent> {
        let secret = resolve_identity(&self.identity_source)?;
        let endpoint_id = secret.public();

        let mut builder = Node::builder().secret_key(secret);
        if self.allow_any_peer {
            builder = builder.allow_any_peer();
        } else {
            builder = builder.allow_peer(endpoint_id);
        }
        for &peer in &self.bootstrap_peers {
            builder = builder.allow_peer(peer);
        }

        if self.no_relay {
            builder = builder.no_relay();
        }

        if self.skip_shm {
            builder = builder.skip_shm();
        }

        if let Some(ref label) = self.name {
            builder = builder.label(label);
        }

        let node = builder.bind()?;
        let directory = Directory::new();
        let name_table = NameTable::new();
        let machine_name = self.name.clone().unwrap_or_else(|| "machine".to_string());

        name_table.register(&machine_name, endpoint_id);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        if let Ok(server) = node.req_server::<DatapodMsg, DatapodMsg>(RESOLUTION_TOPIC) {
            let dir_clone = directory.clone();
            std::thread::spawn(move || {
                run_resolution_loop(server, dir_clone, shutdown_rx);
            });
        }

        let inner = Arc::new(AgentInner {
            node,
            directory,
            name_table,
            machine_name,
            endpoint_id,
            directory_mode: self.directory_mode,
            bootstrap_peers: self.bootstrap_peers,
            _shutdown_tx: Mutex::new(Some(shutdown_tx)),
        });

        Ok(Agent { inner })
    }
}

impl Default for AgentBuilder {
    fn default() -> Self {
        Self::new()
    }
}
