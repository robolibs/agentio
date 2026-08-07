use peerbus::{
    AckServer, AnsServer, DatapodMsg, EndpointId, Node, PipClient, PipServer, Publisher, PutClient,
    QueClient, ReqClient, ReqServer, Subscriber, wire_type_hash,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::directory::{
    ControlPlaneHealth, Directory, DirectoryHealth, RESOLUTION_TOPIC, RESOLUTION_TYPE_HASH,
    ResolveRequest, ResolveResponse, TopicEntry,
};
use crate::error::{Error, Result};
use crate::escape::ById;
use crate::identity::did_key;
use crate::naming::{NameTable, normalize_topic, qualify_participant_topic};

use super::builder::AgentBuilder;
use super::mode::{DirectoryMode, TryIntoBootstrapPeer};

pub(crate) struct AgentInner {
    pub(crate) node: Node,
    pub(crate) directory: Directory,
    pub(crate) name_table: NameTable,
    pub(crate) machine_name: String,
    pub(crate) endpoint_id: EndpointId,
    pub(crate) directory_mode: DirectoryMode,
    pub(crate) bootstrap_peers: Vec<EndpointId>,
    pub(crate) allowed_peers: Vec<EndpointId>,
    pub(crate) allow_any_peer: bool,
    pub(crate) no_relay: bool,
    pub(crate) skip_shm: bool,
    pub(crate) health: Arc<ControlPlaneHealth>,
    pub(crate) resolver_worker: Mutex<Option<ResolverWorker>>,
}

pub(crate) struct ResolverWorker {
    pub(crate) shutdown_tx: tokio::sync::oneshot::Sender<()>,
    pub(crate) join_handle: std::thread::JoinHandle<()>,
}

impl Drop for AgentInner {
    fn drop(&mut self) {
        let worker = match self.resolver_worker.get_mut() {
            Ok(worker) => worker.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(worker) = worker {
            let _ = worker.shutdown_tx.send(());
            if worker.join_handle.join().is_err() {
                tracing::error!("agentio resolver worker panicked during shutdown");
            }
        }
    }
}

/// Main `Agent` composition and IO handle on top of `peerbus`.
#[derive(Clone)]
pub struct Agent {
    pub(crate) inner: Arc<AgentInner>,
}

impl Agent {
    pub fn builder() -> AgentBuilder {
        AgentBuilder::new()
    }

    /// Return the canonical `EndpointId` of this Machine.
    pub fn endpoint_id(&self) -> EndpointId {
        self.inner.endpoint_id
    }

    /// Return the `EndpointAddr` of this Machine (ID + direct socket IP addresses).
    pub fn endpoint_addr(&self) -> peerbus::EndpointAddr {
        self.inner.node.endpoint_addr()
    }

    /// Return the `did:key` string representation of this Machine.
    pub fn did_key(&self) -> Result<String> {
        did_key::endpoint_to_did_key(&self.inner.endpoint_id)
    }

    /// Return the full W3C DID Document JSON representation for this Machine using `authbox`.
    pub fn did_document_json(&self) -> Result<String> {
        did_key::endpoint_to_did_document_json(&self.inner.endpoint_id)
    }

    /// Return the human-readable machine name.
    pub fn name(&self) -> &str {
        &self.inner.machine_name
    }

    /// Access the underlying `peerbus::Node`.
    pub fn node(&self) -> &Node {
        &self.inner.node
    }

    /// Wait for iroh transport to discover local network addresses and endpoints.
    pub fn wait_for_direct_addresses(&self, timeout: Duration) -> Result<()> {
        self.inner
            .node
            .wait_for_direct_addresses(timeout)
            .map_err(Into::into)
    }

    /// Access the local `Directory`.
    pub fn directory(&self) -> &Directory {
        &self.inner.directory
    }

    /// Access the `NameTable`.
    pub fn name_table(&self) -> &NameTable {
        &self.inner.name_table
    }

    /// Return the outbound directory seeds configured for this Agent.
    pub fn bootstrap_peers(&self) -> &[EndpointId] {
        &self.inner.bootstrap_peers
    }

    /// Return the peers authorized to open inbound connections.
    pub fn allowed_peers(&self) -> &[EndpointId] {
        &self.inner.allowed_peers
    }

    /// Return the configured directory mode.
    pub fn directory_mode(&self) -> &DirectoryMode {
        &self.inner.directory_mode
    }

    /// Return whether inbound connections from any peer are permitted.
    pub fn allows_any_peer(&self) -> bool {
        self.inner.allow_any_peer
    }

    /// Return whether relay fallback is disabled.
    pub fn relay_disabled(&self) -> bool {
        self.inner.no_relay
    }

    /// Return whether shared-memory transport is disabled.
    pub fn shared_memory_disabled(&self) -> bool {
        self.inner.skip_shm
    }

    /// Snapshot observable directory control-plane counters.
    pub fn directory_health(&self) -> DirectoryHealth {
        self.inner.health.snapshot()
    }

    /// Create an escape hatch handle to call peerbus directly on a specific peer ID without directory resolution.
    pub fn by_id(&self, peer: impl TryIntoBootstrapPeer) -> Result<ById<'_>> {
        let peer_id = peer.try_into_bootstrap_peer()?;
        Ok(ById::new(&self.inner.node, peer_id))
    }

    /// Publish a topic by name. Registers the topic in the local directory and announces it to peer machines.
    pub fn publish<T>(&self, topic: &str) -> Result<Publisher<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let publisher = self.inner.node.publisher::<T>(&normalized)?;
        let entry = TopicEntry::new(
            &normalized,
            wire_type_hash::<T>(),
            self.inner.endpoint_id,
            Some(&self.inner.machine_name),
        );
        self.inner.directory.register(entry.clone());
        self.announce_topic_to_peers(&entry);
        Ok(publisher)
    }

    /// Subscribe to a topic by name, resolving its owner Machine ID within the Agent composition.
    pub fn subscribe<T>(&self, topic: &str) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let entry = self.resolve_topic(&normalized)?;
        let peer_id = entry.endpoint_id();
        self.inner
            .node
            .subscriber::<T>(peer_id, &normalized)
            .map_err(Into::into)
    }

    /// Subscribe to a topic scoped to a specific participant name (e.g. `subscribe_in("perception", "scan")` -> `/perception/scan`).
    pub fn subscribe_in<T>(&self, participant: &str, topic: &str) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let qualified = qualify_participant_topic(participant, topic)?;
        let entry = self.resolve_topic(&qualified)?;
        let peer_id = entry.endpoint_id();
        self.inner
            .node
            .subscriber::<T>(peer_id, &qualified)
            .map_err(Into::into)
    }

    /// Register and serve a req/res service topic.
    pub fn req_server<Req, Res>(&self, topic: &str) -> Result<ReqServer<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let server = self.inner.node.req_server::<Req, Res>(&normalized)?;
        let entry = TopicEntry::new(
            &normalized,
            wire_type_hash::<Req>(),
            self.inner.endpoint_id,
            Some(&self.inner.machine_name),
        );
        self.inner.directory.register(entry.clone());
        self.announce_topic_to_peers(&entry);
        Ok(server)
    }

    /// Create a req/res client targeting a service topic, resolving its owner Machine ID via referral.
    pub fn req_client<Req, Res>(&self, topic: &str) -> Result<ReqClient<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let entry = self.resolve_topic(&normalized)?;
        let peer_id = entry.endpoint_id();
        self.inner
            .node
            .req_client::<Req, Res>(peer_id, &normalized)
            .map_err(Into::into)
    }

    /// Register and serve a que/ans service topic.
    pub fn que_server<Que, Ans>(&self, topic: &str) -> Result<AnsServer<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let server = self.inner.node.que_server::<Que, Ans>(&normalized)?;
        let entry = TopicEntry::new(
            &normalized,
            wire_type_hash::<Que>(),
            self.inner.endpoint_id,
            Some(&self.inner.machine_name),
        );
        self.inner.directory.register(entry.clone());
        self.announce_topic_to_peers(&entry);
        Ok(server)
    }

    /// Create a que/ans client targeting a service topic, resolving its owner Machine ID via referral.
    pub fn que_client<Que, Ans>(&self, topic: &str) -> Result<QueClient<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let entry = self.resolve_topic(&normalized)?;
        let peer_id = entry.endpoint_id();
        self.inner
            .node
            .que_client::<Que, Ans>(peer_id, &normalized)
            .map_err(Into::into)
    }

    /// Register and serve a put/ack service topic.
    pub fn put_server<Put, Ack>(&self, topic: &str) -> Result<AckServer<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let server = self.inner.node.put_server::<Put, Ack>(&normalized)?;
        let entry = TopicEntry::new(
            &normalized,
            wire_type_hash::<Put>(),
            self.inner.endpoint_id,
            Some(&self.inner.machine_name),
        );
        self.inner.directory.register(entry.clone());
        self.announce_topic_to_peers(&entry);
        Ok(server)
    }

    /// Create a put/ack client targeting a service topic, resolving its owner Machine ID via referral.
    pub fn put_client<Put, Ack>(&self, topic: &str) -> Result<PutClient<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let entry = self.resolve_topic(&normalized)?;
        let peer_id = entry.endpoint_id();
        self.inner
            .node
            .put_client::<Put, Ack>(peer_id, &normalized)
            .map_err(Into::into)
    }

    /// Register and serve a streaming pip service topic.
    pub fn pip_server<ClientMsg, ServerMsg>(
        &self,
        topic: &str,
    ) -> Result<PipServer<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let server = self
            .inner
            .node
            .pip_server::<ClientMsg, ServerMsg>(&normalized)?;
        let entry = TopicEntry::new(
            &normalized,
            wire_type_hash::<ClientMsg>(),
            self.inner.endpoint_id,
            Some(&self.inner.machine_name),
        );
        self.inner.directory.register(entry.clone());
        self.announce_topic_to_peers(&entry);
        Ok(server)
    }

    /// Create a streaming pip client targeting a service topic, resolving its owner Machine ID via referral.
    pub fn pip_client<ClientMsg, ServerMsg>(
        &self,
        topic: &str,
    ) -> Result<PipClient<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let entry = self.resolve_topic(&normalized)?;
        let peer_id = entry.endpoint_id();
        self.inner
            .node
            .pip_client::<ClientMsg, ServerMsg>(peer_id, &normalized)
            .map_err(Into::into)
    }

    /// Resolve a topic to its `TopicEntry` by looking up the local directory or performing referral resolution.
    pub fn resolve_topic(&self, topic: &str) -> Result<TopicEntry> {
        let normalized = normalize_topic(topic)?;
        if let Some(entry) = self.inner.directory.lookup(&normalized) {
            return Ok(entry);
        }

        // Referral resolution over peerbus req/res
        let targets = match &self.inner.directory_mode {
            DirectoryMode::FrontDoor(id) => vec![*id],
            DirectoryMode::Replicated => self.inner.bootstrap_peers.clone(),
        };

        for _attempt in 0..10 {
            for target_id in &targets {
                if let Ok(mut client) = self
                    .inner
                    .node
                    .req_client::<DatapodMsg, DatapodMsg>(*target_id, RESOLUTION_TOPIC)
                {
                    let req = ResolveRequest::Query {
                        topic: normalized.clone(),
                    };
                    if let Ok(req_bytes) = req.to_bytes() {
                        let req_msg = DatapodMsg::new(RESOLUTION_TYPE_HASH, req_bytes);
                        if let Ok(sample) = client.call(&req_msg)
                            && let Ok(ResolveResponse::QueryResult {
                                found: true,
                                entry: Some(entry),
                            }) = ResolveResponse::from_bytes(sample.payload())
                        {
                            self.inner.directory.register(entry.clone());
                            return Ok(entry);
                        }
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        Err(Error::ResolutionFailed(format!(
            "topic '{normalized}' could not be resolved within the agent composition"
        )))
    }

    fn announce_topic_to_peers(&self, entry: &TopicEntry) {
        let req = ResolveRequest::Announce {
            entries: vec![entry.clone()],
        };
        let Ok(req_bytes) = req.to_bytes() else {
            self.inner.health.announcement_failed();
            return;
        };
        let req_msg = DatapodMsg::new(RESOLUTION_TYPE_HASH, req_bytes);

        let targets = match &self.inner.directory_mode {
            DirectoryMode::FrontDoor(id) => vec![*id],
            DirectoryMode::Replicated => self.inner.bootstrap_peers.clone(),
        };

        for target_id in targets {
            if target_id == self.inner.endpoint_id {
                continue;
            }
            if let Ok(mut client) = self
                .inner
                .node
                .req_client::<DatapodMsg, DatapodMsg>(target_id, RESOLUTION_TOPIC)
            {
                match client.call(&req_msg) {
                    Ok(_) => self.inner.health.announcement_succeeded(),
                    Err(error) => {
                        self.inner.health.announcement_failed();
                        tracing::warn!(%target_id, %error, "directory announcement failed");
                    }
                }
            } else {
                self.inner.health.announcement_failed();
            }
        }
    }
}

pub(crate) fn run_resolution_loop(
    mut server: ReqServer<DatapodMsg, DatapodMsg>,
    directory: Directory,
    health: Arc<ControlPlaneHealth>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    loop {
        match shutdown_rx.try_recv() {
            Ok(()) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => break,
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
        }
        match server.recv_timeout(Duration::from_millis(50)) {
            Ok(Some((sample, reply))) => {
                let req_bytes = sample.payload();
                if let Ok(req) = ResolveRequest::from_bytes(req_bytes) {
                    let resp = match req {
                        ResolveRequest::Query { topic } => {
                            let entry = directory.lookup(&topic);
                            ResolveResponse::QueryResult {
                                found: entry.is_some(),
                                entry,
                            }
                        }
                        ResolveRequest::List => ResolveResponse::ListResult {
                            entries: directory.all_entries(),
                        },
                        ResolveRequest::Announce { entries } => {
                            let count = entries.len();
                            directory.register_many(entries);
                            ResolveResponse::Announced { count }
                        }
                    };
                    if let Ok(resp_bytes) = resp.to_bytes() {
                        let resp_msg = DatapodMsg::new(RESOLUTION_TYPE_HASH, resp_bytes);
                        if let Err(error) = reply.respond(&resp_msg) {
                            health.resolver_error();
                            tracing::warn!(%error, "directory response failed");
                        }
                    } else {
                        health.resolver_error();
                    }
                } else {
                    health.reject_record();
                }
            }
            Ok(None) => {}
            Err(error) => {
                health.resolver_error();
                tracing::warn!(%error, "directory resolver receive failed");
            }
        }
    }
}
