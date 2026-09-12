use peerbus::{DatapodMsg, EndpointId, Node, NodeBuilder};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::core::{
    Agent, AgentInner, ControlLoopConfig, ControlWorker, control_channel, run_control_loop,
    run_resolution_loop,
};
use super::mode::{DirectoryMode, TryIntoBootstrapPeer};
use crate::directory::{ControlPlaneHealth, Directory, RESOLUTION_TOPIC, next_revision_seed};
use crate::error::{Error, Result};
use crate::identity::{IdentitySource, resolve_identity};
use crate::naming::NameTable;

/// Builder for constructing an `Agent` instance.
pub struct AgentBuilder {
    pub(crate) name: Option<String>,
    pub(crate) participant: Option<String>,
    pub(crate) identity_source: IdentitySource,
    pub(crate) directory_mode: DirectoryMode,
    pub(crate) bootstrap_peers: Vec<EndpointId>,
    pub(crate) allowed_peers: Vec<EndpointId>,
    pub(crate) configuration_errors: Vec<String>,
    pub(crate) allow_any_peer: bool,
    pub(crate) no_relay: bool,
    pub(crate) skip_shm: bool,
    pub(crate) rendezvous: bool,
    pub(crate) lease_duration: Duration,
    pub(crate) control_timeout: Duration,
    pub(crate) local_config: Option<peerbus::LocalConfig>,
    #[cfg(test)]
    force_resolution_collision: bool,
}

impl AgentBuilder {
    /// Create a builder with deny-by-default authorization and replicated resolution.
    pub fn new() -> Self {
        Self {
            name: None,
            participant: None,
            identity_source: IdentitySource::Ephemeral,
            directory_mode: DirectoryMode::Replicated,
            bootstrap_peers: Vec::new(),
            allowed_peers: Vec::new(),
            configuration_errors: Vec::new(),
            allow_any_peer: false,
            no_relay: false,
            skip_shm: false,
            rendezvous: true,
            lease_duration: crate::directory::DEFAULT_LEASE_DURATION,
            control_timeout: Duration::from_secs(6),
            local_config: None,
            #[cfg(test)]
            force_resolution_collision: false,
        }
    }

    /// Shared-memory ring limits for every topic this agent hosts. Peerbus
    /// pins them at segment creation; its default allows two request
    /// producers per service, which a third local client exceeds.
    pub fn local_config(mut self, config: peerbus::LocalConfig) -> Self {
        self.local_config = Some(config);
        self
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

    /// Participant prefix for this agent's own relative topics:
    /// `publish_own("scan")` on a `participant("lidar")` agent hosts
    /// `/lidar/scan`. Absolute topics are never prefixed.
    pub fn participant(mut self, participant: impl Into<String>) -> Self {
        let trimmed = participant.into().trim().trim_matches('/').to_string();
        if trimmed.is_empty() {
            self.configuration_errors
                .push("participant prefix cannot be empty".to_string());
        } else {
            self.participant = Some(trimmed);
        }
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

    /// Leave no host-local rendezvous record for this agent (see
    /// [`crate::rendezvous`]); other processes on this host then cannot
    /// find it by name.
    pub fn no_rendezvous(mut self) -> Self {
        self.rendezvous = false;
        self
    }

    /// Set the signed directory-record lease duration.
    pub fn lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration.max(Duration::from_millis(30));
        self
    }

    /// Set the caller-visible deadline for directory control operations.
    pub fn control_timeout(mut self, control_timeout: Duration) -> Self {
        self.control_timeout = control_timeout.max(Duration::from_millis(1));
        self
    }

    /// Build the `Agent` machine instance.
    pub fn build(self) -> Result<Agent> {
        if !self.configuration_errors.is_empty() {
            return Err(Error::Configuration(self.configuration_errors.join("; ")));
        }

        let secret = resolve_identity(&self.identity_source)?;
        let endpoint_id = secret.public();

        let mut builder = configure_inbound_policy(
            Node::builder().secret_key(secret.clone()),
            endpoint_id,
            &self.allowed_peers,
            self.allow_any_peer,
        );

        if self.no_relay {
            builder = builder.no_relay();
        }

        if self.skip_shm {
            builder = builder.skip_shm();
        }

        if let Some(config) = self.local_config.clone() {
            builder = builder.local_config(config);
        }

        if let Some(ref label) = self.name {
            builder = builder.label(label);
        }

        let node = builder.bind()?;
        let rendezvous = if self.rendezvous {
            match crate::rendezvous::publish(
                self.name.as_deref(),
                self.participant.as_deref(),
                &endpoint_id,
                &node.endpoint_addr(),
            ) {
                Ok(path) => Some(path),
                Err(error) => {
                    tracing::warn!(%error, "agentio rendezvous record not written");
                    None
                }
            }
        } else {
            None
        };
        let directory = Directory::new();
        let name_table = NameTable::with_directory(directory.clone());
        let machine_name = self.name.clone().unwrap_or_else(|| "machine".to_string());

        name_table.register(&machine_name, endpoint_id);

        #[cfg(test)]
        let _collision = if self.force_resolution_collision {
            Some(node.req_server::<DatapodMsg, DatapodMsg>(RESOLUTION_TOPIC)?)
        } else {
            None
        };
        let server = node.req_server::<DatapodMsg, DatapodMsg>(RESOLUTION_TOPIC)?;
        let health = Arc::new(ControlPlaneHealth::default());
        let hosted_records = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let hosting = Arc::new(Mutex::new(()));
        let withdrawn = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let next_revision = Arc::new(AtomicU64::new(next_revision_seed()));
        let (resolver_shutdown_tx, resolver_shutdown_rx) = std::sync::mpsc::channel();
        let worker_health = health.clone();
        let dir_clone = directory.clone();
        let resolver_names = name_table.clone();
        let resolver_join = std::thread::Builder::new()
            .name(format!("agentio-resolver-{machine_name}"))
            .spawn(move || {
                run_resolution_loop(
                    server,
                    dir_clone,
                    resolver_names,
                    worker_health,
                    resolver_shutdown_rx,
                );
            })?;

        let control_config = ControlLoopConfig {
            node: node.clone(),
            secret: secret.clone(),
            endpoint_id,
            mode: self.directory_mode.clone(),
            seeds: self.bootstrap_peers.clone(),
            directory: directory.clone(),
            name_table: name_table.clone(),
            machine_name: machine_name.clone(),
            hosted_records: hosted_records.clone(),
            hosting: hosting.clone(),
            withdrawn: withdrawn.clone(),
            next_revision: next_revision.clone(),
            health: health.clone(),
            lease_duration: self.lease_duration,
            control_timeout: self.control_timeout,
        };
        let (control_tx, control_rx) = control_channel();
        let (control_shutdown_tx, control_shutdown_rx) = std::sync::mpsc::channel();
        let control_join = match std::thread::Builder::new()
            .name(format!("agentio-control-{machine_name}"))
            .spawn(move || run_control_loop(control_config, control_rx, control_shutdown_rx))
        {
            Ok(join) => join,
            Err(error) => {
                let _ = resolver_shutdown_tx.send(());
                let _ = resolver_join.join();
                return Err(error.into());
            }
        };

        let inner = Arc::new(AgentInner {
            node,
            secret,
            directory,
            name_table,
            machine_name,
            participant: self.participant,
            endpoint_id,
            directory_mode: self.directory_mode,
            bootstrap_peers: self.bootstrap_peers,
            allowed_peers: self.allowed_peers,
            allow_any_peer: self.allow_any_peer,
            no_relay: self.no_relay,
            skip_shm: self.skip_shm,
            lease_duration: self.lease_duration,
            control_timeout: self.control_timeout,
            health,
            next_revision,
            hosted_records,
            rendezvous,
            hosting,
            withdrawn,
            control_tx,
            workers: Mutex::new(vec![
                ControlWorker {
                    shutdown_tx: resolver_shutdown_tx,
                    join_handle: resolver_join,
                },
                ControlWorker {
                    shutdown_tx: control_shutdown_tx,
                    join_handle: control_join,
                },
            ]),
        });

        Ok(Agent { inner })
    }
}

fn configure_inbound_policy(
    mut builder: NodeBuilder,
    endpoint_id: EndpointId,
    allowed_peers: &[EndpointId],
    allow_any_peer: bool,
) -> NodeBuilder {
    if allow_any_peer {
        tracing::warn!("inbound peer allowlist disabled");
        builder = builder.allow_any_peer();
    } else {
        builder = builder.allow_peer(endpoint_id);
    }
    for peer in allowed_peers {
        builder = builder.allow_peer(*peer);
    }
    builder
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
    use std::io::Write;

    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn invalid_bootstrap_is_reported() {
        let result = Agent::builder()
            .identity(IdentitySource::Random)
            .bootstrap(["not-an-endpoint"])
            .build();
        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    #[test]
    fn resolution_server_collision_fails_build() {
        let mut builder = Agent::builder().identity(IdentitySource::Random);
        builder.force_resolution_collision = true;
        assert!(builder.build().is_err());
    }

    #[test]
    fn allow_any_peer_emits_warning() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let writer = SharedWriter(captured.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let secret = peerbus::SecretKey::generate();
            let _ = configure_inbound_policy(
                Node::builder().secret_key(secret.clone()),
                secret.public(),
                &[],
                true,
            );
        });
        let output = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
        assert!(output.contains("inbound peer allowlist disabled"));
    }
}
