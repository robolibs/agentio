use peerbus::{DatapodMsg, EndpointId, Node};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::core::{Agent, AgentInner, ResolverWorker, run_resolution_loop};
use super::mode::{DirectoryMode, TryIntoBootstrapPeer};
use crate::directory::{ControlPlaneHealth, Directory, RESOLUTION_TOPIC};
use crate::error::{Error, Result};
use crate::identity::{IdentitySource, resolve_identity};
use crate::naming::NameTable;

/// Builder for constructing an `Agent` instance.
pub struct AgentBuilder {
    pub(crate) name: Option<String>,
    pub(crate) identity_source: IdentitySource,
    pub(crate) directory_mode: DirectoryMode,
    pub(crate) bootstrap_peers: Vec<EndpointId>,
    pub(crate) allowed_peers: Vec<EndpointId>,
    pub(crate) configuration_errors: Vec<String>,
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
            allowed_peers: Vec::new(),
            configuration_errors: Vec::new(),
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
        for peer in peers {
            match peer.try_into_bootstrap_peer() {
                Ok(peer) if !self.bootstrap_peers.contains(&peer) => {
                    self.bootstrap_peers.push(peer);
                }
                Ok(_) => {}
                Err(error) => self.configuration_errors.push(error.to_string()),
            }
        }
        self
    }

    /// Permit one peer to open inbound transport connections to this Agent.
    pub fn allow_peer<P>(mut self, peer: P) -> Self
    where
        P: TryIntoBootstrapPeer,
    {
        match peer.try_into_bootstrap_peer() {
            Ok(peer) if !self.allowed_peers.contains(&peer) => self.allowed_peers.push(peer),
            Ok(_) => {}
            Err(error) => self.configuration_errors.push(error.to_string()),
        }
        self
    }

    /// Permit multiple peers to open inbound transport connections.
    pub fn allow_peers<I, P>(mut self, peers: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: TryIntoBootstrapPeer,
    {
        for peer in peers {
            self = self.allow_peer(peer);
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
        if !self.configuration_errors.is_empty() {
            return Err(Error::Configuration(self.configuration_errors.join("; ")));
        }

        let secret = resolve_identity(&self.identity_source)?;
        let endpoint_id = secret.public();

        let mut builder = Node::builder().secret_key(secret.clone());
        if self.allow_any_peer {
            builder = builder.allow_any_peer();
        } else {
            builder = builder.allow_peer(endpoint_id);
        }
        for &peer in &self.allowed_peers {
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

        let server = node.req_server::<DatapodMsg, DatapodMsg>(RESOLUTION_TOPIC)?;
        let health = Arc::new(ControlPlaneHealth::default());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let worker_health = health.clone();
        let dir_clone = directory.clone();
        let join_handle = std::thread::Builder::new()
            .name(format!("agentio-resolver-{machine_name}"))
            .spawn(move || {
                run_resolution_loop(server, dir_clone, worker_health, shutdown_rx);
            })?;

        let inner = Arc::new(AgentInner {
            node,
            directory,
            name_table,
            machine_name,
            endpoint_id,
            directory_mode: self.directory_mode,
            bootstrap_peers: self.bootstrap_peers,
            allowed_peers: self.allowed_peers,
            allow_any_peer: self.allow_any_peer,
            no_relay: self.no_relay,
            skip_shm: self.skip_shm,
            health,
            resolver_worker: Mutex::new(Some(ResolverWorker {
                shutdown_tx,
                join_handle,
            })),
        });

        Ok(Agent { inner })
    }
}

impl Default for AgentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::IdentitySource;

    #[test]
    fn invalid_bootstrap_is_reported() {
        let result = Agent::builder()
            .identity(IdentitySource::Random)
            .bootstrap(["not-an-endpoint"])
            .build();
        assert!(matches!(result, Err(Error::Configuration(_))));
    }
}
