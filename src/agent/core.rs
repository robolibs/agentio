use peerbus::{
    AckServer, AnsServer, DatapodMsg, EndpointId, Node, PipClient, PipServer, Publisher, PutClient,
    QueClient, ReqClient, ReqServer, Subscriber, wire_type_hash,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use crate::directory::{
    ControlPlaneHealth, Directory, DirectoryHealth, ExchangeKind, MAX_DIRECTORY_BATCH,
    RESOLUTION_TOPIC, RESOLUTION_TYPE_HASH, ResolveRequest, ResolveResponse, TopicEntry,
    TopicRecordSpec, TopicWithdrawal, unix_time_ms,
};
use crate::error::{Error, Result};
use crate::escape::ById;
use crate::identity::did_key;
use crate::naming::{NameTable, normalize_topic, qualify_participant_topic};

use super::builder::AgentBuilder;
use super::mode::{DirectoryMode, TryIntoBootstrapPeer};
use super::registered::Registered;

type HostedKey = (String, ExchangeKind);
const CONTROL_QUEUE_CAPACITY: usize = 64;

fn internal_control_timeout(timeout: Duration) -> Duration {
    timeout.saturating_sub((timeout / 2).min(Duration::from_millis(100)))
}

#[derive(Debug, Clone)]
pub(crate) struct HostedRecord {
    pub(crate) topic: String,
    pub(crate) exchange: ExchangeKind,
    pub(crate) request_type_hash: u64,
    pub(crate) response_type_hash: Option<u64>,
}

pub(crate) struct AgentInner {
    pub(crate) node: Node,
    pub(crate) secret: peerbus::SecretKey,
    pub(crate) directory: Directory,
    pub(crate) name_table: NameTable,
    pub(crate) machine_name: String,
    pub(crate) participant: Option<String>,
    pub(crate) endpoint_id: EndpointId,
    pub(crate) directory_mode: DirectoryMode,
    pub(crate) bootstrap_peers: Vec<EndpointId>,
    pub(crate) allowed_peers: Mutex<Vec<EndpointId>>,
    pub(crate) allow_any_peer: bool,
    pub(crate) no_relay: bool,
    pub(crate) skip_shm: bool,
    pub(crate) lease_duration: Duration,
    pub(crate) control_timeout: Duration,
    pub(crate) health: Arc<ControlPlaneHealth>,
    pub(crate) next_revision: Arc<AtomicU64>,
    pub(crate) hosted_records: Arc<Mutex<std::collections::HashMap<HostedKey, HostedRecord>>>,
    pub(crate) rendezvous: Option<std::path::PathBuf>,
    /// Held while a handle is created and registered, and by the adopter
    /// while it scans, so a topic is never adopted mid-registration.
    pub(crate) hosting: Arc<Mutex<()>>,
    /// Keys the agent withdrew on purpose; the adopter leaves those alone
    /// even while peerbus still lists the topic.
    pub(crate) withdrawn: Arc<Mutex<std::collections::HashSet<HostedKey>>>,
    pub(crate) control_tx: mpsc::SyncSender<ControlCommand>,
    pub(crate) workers: Mutex<Vec<ControlWorker>>,
}

pub(crate) enum ControlCommand {
    Query {
        target_ids: Vec<EndpointId>,
        request: ResolveRequest,
        reply: mpsc::Sender<Result<TopicEntry>>,
    },
    Reconcile {
        reply: mpsc::Sender<Result<usize>>,
    },
    Announce(Vec<TopicEntry>),
    Withdraw(Vec<TopicWithdrawal>),
    Renew {
        reply: mpsc::Sender<usize>,
    },
}

pub(crate) fn control_channel() -> (
    mpsc::SyncSender<ControlCommand>,
    mpsc::Receiver<ControlCommand>,
) {
    mpsc::sync_channel(CONTROL_QUEUE_CAPACITY)
}

pub(crate) struct ControlWorker {
    pub(crate) shutdown_tx: std::sync::mpsc::Sender<()>,
    pub(crate) join_handle: std::thread::JoinHandle<()>,
}

impl Drop for AgentInner {
    fn drop(&mut self) {
        if self.rendezvous.is_some() {
            crate::rendezvous::withdraw(&self.endpoint_id);
        }
        let workers = match self.workers.get_mut() {
            Ok(workers) => std::mem::take(workers),
            Err(poisoned) => std::mem::take(poisoned.into_inner()),
        };
        for worker in &workers {
            let _ = worker.shutdown_tx.send(());
        }
        if let Err(error) = self.node.clone().close() {
            tracing::warn!(%error, "agentio node close failed during shutdown");
        }
        for worker in workers {
            if worker.join_handle.join().is_err() {
                tracing::error!("agentio control worker panicked during shutdown");
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
    /// Create an Agent builder.
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
    pub fn allowed_peers(&self) -> Vec<EndpointId> {
        self.inner.allowed_peers.lock().unwrap().clone()
    }

    /// Permit one more peer to open inbound connections to this live agent.
    /// Takes effect for the peer's next connection; an agent built with
    /// `allow_any_peer` is unchanged.
    pub fn allow_peer(&self, peer: impl TryIntoBootstrapPeer) -> Result<()> {
        let peer = peer.try_into_bootstrap_peer()?;
        self.inner.node.allow_peer(peer);
        let mut allowed = self.inner.allowed_peers.lock().unwrap();
        if !allowed.contains(&peer) {
            allowed.push(peer);
        }
        Ok(())
    }

    /// Whether an inbound connection from `peer` would be accepted now.
    pub fn allows_peer(&self, peer: &EndpointId) -> bool {
        self.inner.node.allows_peer(peer)
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

    /// Fetch bounded signed snapshots from configured directory targets.
    pub fn reconcile_now(&self) -> Result<usize> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.inner
            .control_tx
            .try_send(ControlCommand::Reconcile { reply: reply_tx })
            .map_err(|error| Error::ControlPlane(error.to_string()))?;
        reply_rx
            .recv_timeout(self.inner.control_timeout)
            .map_err(|_| Error::ControlTimeout(self.inner.control_timeout))?
    }

    /// Renew every locally hosted signed record and return the renewed count.
    pub fn renew_now(&self) -> usize {
        let (reply_tx, reply_rx) = mpsc::channel();
        if self
            .inner
            .control_tx
            .try_send(ControlCommand::Renew { reply: reply_tx })
            .is_err()
        {
            return 0;
        }
        reply_rx
            .recv_timeout(self.inner.control_timeout)
            .unwrap_or_default()
    }

    /// Create an escape hatch handle to call peerbus directly on a specific peer ID without directory resolution.
    pub fn by_id(&self, peer: impl TryIntoBootstrapPeer) -> Result<ById<'_>> {
        let peer_id = peer.try_into_bootstrap_peer()?;
        Ok(ById::new(&self.inner.node, peer_id))
    }

    /// Publish a topic by name. Registers the topic in the local directory and announces it to peer machines.
    pub fn publish<T>(&self, topic: &str) -> Result<Registered<Publisher<T>>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let _hosting = self.inner.hosting.lock().unwrap();
        let publisher = self.inner.node.publisher::<T>(&normalized)?;
        let entry = TopicEntry::signed(
            TopicRecordSpec::new(
                &normalized,
                ExchangeKind::PubSub,
                wire_type_hash::<T>(),
                None,
                self.next_revision(),
                Some(&self.inner.machine_name),
            )
            .lease_expires_at_ms(self.lease_deadline()),
            &self.inner.secret,
        )?;
        self.register_hosted(&entry)?;
        Ok(Registered::new(
            publisher,
            entry,
            Arc::downgrade(&self.inner),
        ))
    }

    /// Subscribe to a topic by name, resolving its owner Machine ID within the Agent composition.
    pub fn subscribe<T>(&self, topic: &str) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let entry = self.resolve_exchange(
            &normalized,
            ExchangeKind::PubSub,
            wire_type_hash::<T>(),
            None,
        )?;
        let peer_id = entry.endpoint_id();
        self.inner
            .node
            .subscriber::<T>(peer_id, &normalized)
            .map_err(Into::into)
    }

    /// The host-local rendezvous record this agent wrote, if any.
    pub fn rendezvous_path(&self) -> Option<&std::path::Path> {
        self.inner.rendezvous.as_deref()
    }

    /// Sign directory records for the topics the peerbus node hosts through
    /// [`Agent::node`] that the directory does not know yet, and announce
    /// them. The maintenance tick does this on its own every lease third;
    /// call it to make raw node topics visible at once. Returns how many
    /// records were added. peerbus keeps a topic listed after its raw
    /// handle drops, so an adopted record lives until the agent does; a
    /// record the agent withdrew itself is never adopted again.
    pub fn adopt_hosted_topics(&self) -> Result<usize> {
        let adopted = adopt_node_topics(
            &self.inner.node,
            &self.inner.directory,
            &self.inner.hosted_records,
            &self.inner.hosting,
            &self.inner.withdrawn,
            &self.inner.name_table,
            &self.inner.next_revision,
            &self.inner.machine_name,
            &self.inner.secret,
            self.inner.lease_duration,
        );
        if !adopted.is_empty() {
            queue_control_command(&self.inner, ControlCommand::Announce(adopted.clone()));
        }
        Ok(adopted.len())
    }

    /// Register a hosted publisher for a topic scoped to a participant name
    /// (`publish_in("perception", "scan")` hosts `/perception/scan`); an
    /// absolute topic is left as it is.
    pub fn publish_in<T>(&self, participant: &str, topic: &str) -> Result<Registered<Publisher<T>>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.publish::<T>(&qualify_participant_topic(participant, topic)?)
    }

    /// Register a hosted publisher for a topic under this agent's own
    /// participant prefix (see [`AgentBuilder::participant`]).
    pub fn publish_own<T>(&self, topic: &str) -> Result<Registered<Publisher<T>>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.publish::<T>(&self.own_topic(topic)?)
    }

    /// Subscribe to a topic under this agent's own participant prefix.
    pub fn subscribe_own<T>(&self, topic: &str) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        self.subscribe::<T>(&self.own_topic(topic)?)
    }

    /// The participant prefix this agent qualifies its own relative topics
    /// with, if the builder set one.
    pub fn participant(&self) -> Option<&str> {
        self.inner.participant.as_deref()
    }

    /// A relative topic under this agent's participant prefix; absolute
    /// topics pass through. Fails when no participant is configured.
    pub fn own_topic(&self, topic: &str) -> Result<String> {
        match self.inner.participant.as_deref() {
            Some(participant) => qualify_participant_topic(participant, topic),
            None if topic.trim_start().starts_with('/') => normalize_topic(topic),
            None => Err(Error::Configuration(format!(
                "relative topic '{topic}' needs a participant prefix; set AgentBuilder::participant"
            ))),
        }
    }

    /// Subscribe to a topic scoped to a specific participant name (e.g. `subscribe_in("perception", "scan")` -> `/perception/scan`).
    pub fn subscribe_in<T>(&self, participant: &str, topic: &str) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let qualified = qualify_participant_topic(participant, topic)?;
        let entry = self.resolve_exchange(
            &qualified,
            ExchangeKind::PubSub,
            wire_type_hash::<T>(),
            None,
        )?;
        let peer_id = entry.endpoint_id();
        self.inner
            .node
            .subscriber::<T>(peer_id, &qualified)
            .map_err(Into::into)
    }

    /// Register and serve a req/res service topic.
    pub fn req_server<Req, Res>(&self, topic: &str) -> Result<Registered<ReqServer<Req, Res>>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let _hosting = self.inner.hosting.lock().unwrap();
        let mut server = self.inner.node.req_server::<Req, Res>(&normalized)?;
        drop_predecessor_traffic(&normalized, || Ok(server.take()?.is_some()))?;
        let entry = TopicEntry::signed(
            TopicRecordSpec::new(
                &normalized,
                ExchangeKind::ReqRes,
                wire_type_hash::<Req>(),
                Some(wire_type_hash::<Res>()),
                self.next_revision(),
                Some(&self.inner.machine_name),
            )
            .lease_expires_at_ms(self.lease_deadline()),
            &self.inner.secret,
        )?;
        self.register_hosted(&entry)?;
        Ok(Registered::new(server, entry, Arc::downgrade(&self.inner)))
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
        let entry = self.resolve_exchange(
            &normalized,
            ExchangeKind::ReqRes,
            wire_type_hash::<Req>(),
            Some(wire_type_hash::<Res>()),
        )?;
        let peer_id = entry.endpoint_id();
        self.inner
            .node
            .req_client::<Req, Res>(peer_id, &normalized)
            .map_err(Into::into)
    }

    /// Register and serve a que/ans service topic.
    pub fn que_server<Que, Ans>(&self, topic: &str) -> Result<Registered<AnsServer<Que, Ans>>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let _hosting = self.inner.hosting.lock().unwrap();
        let mut server = self.inner.node.que_server::<Que, Ans>(&normalized)?;
        drop_predecessor_traffic(&normalized, || Ok(server.take()?.is_some()))?;
        let entry = TopicEntry::signed(
            TopicRecordSpec::new(
                &normalized,
                ExchangeKind::QueAns,
                wire_type_hash::<Que>(),
                Some(wire_type_hash::<Ans>()),
                self.next_revision(),
                Some(&self.inner.machine_name),
            )
            .lease_expires_at_ms(self.lease_deadline()),
            &self.inner.secret,
        )?;
        self.register_hosted(&entry)?;
        Ok(Registered::new(server, entry, Arc::downgrade(&self.inner)))
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
        let entry = self.resolve_exchange(
            &normalized,
            ExchangeKind::QueAns,
            wire_type_hash::<Que>(),
            Some(wire_type_hash::<Ans>()),
        )?;
        let peer_id = entry.endpoint_id();
        self.inner
            .node
            .que_client::<Que, Ans>(peer_id, &normalized)
            .map_err(Into::into)
    }

    /// Register and serve a put/ack service topic.
    pub fn put_server<Put, Ack>(&self, topic: &str) -> Result<Registered<AckServer<Put, Ack>>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let _hosting = self.inner.hosting.lock().unwrap();
        let mut server = self.inner.node.put_server::<Put, Ack>(&normalized)?;
        drop_predecessor_traffic(&normalized, || Ok(server.take()?.is_some()))?;
        let entry = TopicEntry::signed(
            TopicRecordSpec::new(
                &normalized,
                ExchangeKind::PutAck,
                wire_type_hash::<Put>(),
                Some(wire_type_hash::<Ack>()),
                self.next_revision(),
                Some(&self.inner.machine_name),
            )
            .lease_expires_at_ms(self.lease_deadline()),
            &self.inner.secret,
        )?;
        self.register_hosted(&entry)?;
        Ok(Registered::new(server, entry, Arc::downgrade(&self.inner)))
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
        let entry = self.resolve_exchange(
            &normalized,
            ExchangeKind::PutAck,
            wire_type_hash::<Put>(),
            Some(wire_type_hash::<Ack>()),
        )?;
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
    ) -> Result<Registered<PipServer<ClientMsg, ServerMsg>>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        let _hosting = self.inner.hosting.lock().unwrap();
        let server = self
            .inner
            .node
            .pip_server::<ClientMsg, ServerMsg>(&normalized)?;
        let entry = TopicEntry::signed(
            TopicRecordSpec::new(
                &normalized,
                ExchangeKind::Pip,
                wire_type_hash::<ClientMsg>(),
                Some(wire_type_hash::<ServerMsg>()),
                self.next_revision(),
                Some(&self.inner.machine_name),
            )
            .lease_expires_at_ms(self.lease_deadline()),
            &self.inner.secret,
        )?;
        self.register_hosted(&entry)?;
        Ok(Registered::new(server, entry, Arc::downgrade(&self.inner)))
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
        let entry = self.resolve_exchange(
            &normalized,
            ExchangeKind::Pip,
            wire_type_hash::<ClientMsg>(),
            Some(wire_type_hash::<ServerMsg>()),
        )?;
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

        Err(Error::ResolutionFailed(format!(
            "topic '{normalized}' is not present in the local directory; use a typed client for referral resolution"
        )))
    }

    fn resolve_exchange(
        &self,
        topic: &str,
        exchange: ExchangeKind,
        request_type_hash: u64,
        response_type_hash: Option<u64>,
    ) -> Result<TopicEntry> {
        if let Some(entry) = self.inner.directory.lookup_exchange(topic, exchange) {
            entry.validate_exchange(exchange, request_type_hash, response_type_hash)?;
            return Ok(entry);
        }

        let targets = match &self.inner.directory_mode {
            DirectoryMode::FrontDoor(id) => vec![*id],
            DirectoryMode::Replicated => self.inner.bootstrap_peers.clone(),
        };

        if targets.is_empty() {
            return Err(Error::ResolutionFailed(format!(
                "topic '{topic}' has no configured resolution target"
            )));
        }
        let request = ResolveRequest::Query {
            topic: topic.to_string(),
            exchange,
            request_type_hash,
            response_type_hash,
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        self.inner
            .control_tx
            .try_send(ControlCommand::Query {
                target_ids: targets,
                request,
                reply: reply_tx,
            })
            .map_err(|error| Error::ControlPlane(error.to_string()))?;
        reply_rx
            .recv_timeout(self.inner.control_timeout)
            .map_err(|_| Error::ControlTimeout(self.inner.control_timeout))?
    }

    fn next_revision(&self) -> u64 {
        self.inner.next_revision.fetch_add(1, Ordering::Relaxed)
    }

    fn lease_deadline(&self) -> u64 {
        unix_time_ms().saturating_add(
            self.inner
                .lease_duration
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        )
    }

    fn register_hosted(&self, entry: &TopicEntry) -> Result<()> {
        self.inner.directory.register(entry.clone())?;
        self.inner
            .withdrawn
            .lock()
            .unwrap()
            .remove(&(entry.topic().to_string(), entry.exchange()));
        self.inner.hosted_records.lock().unwrap().insert(
            (entry.topic().to_string(), entry.exchange()),
            HostedRecord {
                topic: entry.topic().to_string(),
                exchange: entry.exchange(),
                request_type_hash: entry.request_type_hash(),
                response_type_hash: entry.response_type_hash(),
            },
        );
        self.inner
            .name_table
            .sync_from_entries(&self.inner.directory.all_entries());
        queue_control_command(&self.inner, ControlCommand::Announce(vec![entry.clone()]));
        Ok(())
    }
}

fn resolution_targets(mode: &DirectoryMode, seeds: &[EndpointId]) -> Vec<EndpointId> {
    match mode {
        DirectoryMode::FrontDoor(id) => vec![*id],
        DirectoryMode::Replicated => seeds.to_vec(),
    }
}

fn send_control_request(
    client: &mut ReqClient<DatapodMsg, DatapodMsg>,
    request: &ResolveRequest,
) -> Result<ResolveResponse> {
    let request_bytes = request.to_bytes()?;
    let message = DatapodMsg::new(RESOLUTION_TYPE_HASH, request_bytes);
    let sample = client.call(&message)?;
    ResolveResponse::from_bytes(sample.payload())
}

struct RemoteCall {
    request: ResolveRequest,
    reply: mpsc::Sender<Result<ResolveResponse>>,
}

struct RemoteWorker {
    tx: mpsc::SyncSender<RemoteCall>,
    join: std::thread::JoinHandle<()>,
}

struct ControlNetwork {
    workers: HashMap<EndpointId, RemoteWorker>,
}

impl ControlNetwork {
    fn new(config: &ControlLoopConfig) -> Self {
        let mut workers = HashMap::new();
        for target_id in resolution_targets(&config.mode, &config.seeds) {
            if target_id == config.endpoint_id || workers.contains_key(&target_id) {
                continue;
            }
            let (tx, rx) = mpsc::sync_channel::<RemoteCall>(1);
            let node = config.node.clone();
            let join = std::thread::Builder::new()
                .name(format!("agentio-seed-{target_id}"))
                .spawn(move || {
                    let mut client =
                        node.req_client::<DatapodMsg, DatapodMsg>(target_id, RESOLUTION_TOPIC);
                    while let Ok(call) = rx.recv() {
                        let mut response = match &mut client {
                            Ok(client) => send_control_request(client, &call.request),
                            Err(error) => Err(Error::ControlPlane(error.to_string())),
                        };
                        if response.is_err() {
                            client = node
                                .req_client::<DatapodMsg, DatapodMsg>(target_id, RESOLUTION_TOPIC);
                            response = match &mut client {
                                Ok(client) => send_control_request(client, &call.request),
                                Err(error) => Err(Error::ControlPlane(error.to_string())),
                            };
                        }
                        let _ = call.reply.send(response);
                    }
                });
            match join {
                Ok(join) => {
                    workers.insert(target_id, RemoteWorker { tx, join });
                }
                Err(error) => {
                    tracing::error!(%target_id, %error, "directory-target worker creation failed");
                }
            }
        }
        Self { workers }
    }

    fn request(
        &self,
        target_id: EndpointId,
        request: ResolveRequest,
    ) -> Result<mpsc::Receiver<Result<ResolveResponse>>> {
        let worker = self.workers.get(&target_id).ok_or_else(|| {
            Error::ControlPlane(format!(
                "no control worker for directory target {target_id}"
            ))
        })?;
        let (reply, response) = mpsc::channel();
        worker
            .tx
            .try_send(RemoteCall { request, reply })
            .map_err(|error| Error::ControlPlane(error.to_string()))?;
        Ok(response)
    }

    fn shutdown(self) {
        let workers: Vec<_> = self.workers.into_values().collect();
        for worker in workers {
            drop(worker.tx);
            if worker.join.join().is_err() {
                tracing::error!("agentio directory-target worker panicked");
            }
        }
    }
}

fn queue_control_command(inner: &AgentInner, command: ControlCommand) {
    if let Err(error) = inner.control_tx.try_send(command) {
        inner.health.announcement_failed();
        tracing::warn!(%error, "agentio control queue rejected an operation");
    }
}

fn announce_records(
    network: &ControlNetwork,
    endpoint_id: EndpointId,
    mode: &DirectoryMode,
    seeds: &[EndpointId],
    health: &ControlPlaneHealth,
    entries: &[TopicEntry],
    timeout: Duration,
) {
    let targets = resolution_targets(mode, seeds);
    let request = ResolveRequest::Announce {
        entries: entries.to_vec(),
    };
    let mut pending = Vec::new();
    for target_id in targets {
        if target_id == endpoint_id {
            continue;
        }
        match network.request(target_id, request.clone()) {
            Ok(response) => pending.push((target_id, response)),
            Err(_) => health.announcement_failed(),
        }
    }
    health.set_pending_announcements(pending.len().try_into().unwrap_or(u64::MAX));
    let deadline = Instant::now() + internal_control_timeout(timeout);
    while !pending.is_empty() && Instant::now() < deadline {
        let mut progressed = false;
        for index in (0..pending.len()).rev() {
            match pending[index].1.try_recv() {
                Ok(Ok(ResolveResponse::Announced { .. })) => {
                    health.announcement_succeeded();
                    pending.swap_remove(index);
                    progressed = true;
                }
                Ok(Ok(ResolveResponse::Rejected { ref message })) => {
                    health.announcement_failed();
                    tracing::warn!(target_id = %pending[index].0, %message, "directory announcement rejected");
                    pending.swap_remove(index);
                    progressed = true;
                }
                Ok(_) | Err(mpsc::TryRecvError::Disconnected) => {
                    health.announcement_failed();
                    pending.swap_remove(index);
                    progressed = true;
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        health.set_pending_announcements(pending.len().try_into().unwrap_or(u64::MAX));
        if !progressed {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    for _ in pending {
        health.announcement_failed();
    }
    health.set_pending_announcements(0);
}

pub(crate) fn withdraw_owned_entry(inner: &AgentInner, entry: &TopicEntry) {
    // Serialised with renewal and adoption: the withdrawal must target the
    // revision the directory holds at the moment it is signed.
    let _hosting = inner.hosting.lock().unwrap();
    let key = (entry.topic().to_string(), entry.exchange());
    inner.hosted_records.lock().unwrap().remove(&key);
    inner.withdrawn.lock().unwrap().insert(key);
    // Renewals re-sign the record with fresher revisions after the handle
    // took its copy; a withdrawal binds to one exact revision, so it must
    // target what the directory holds now.
    let current = inner
        .directory
        .lookup_exchange(entry.topic(), entry.exchange())
        .filter(|live| live.endpoint_id() == inner.endpoint_id)
        .unwrap_or_else(|| entry.clone());
    let revision = inner.next_revision.fetch_add(1, Ordering::Relaxed);
    let Ok(withdrawal) = TopicWithdrawal::signed(&current, revision, &inner.secret) else {
        inner.health.announcement_failed();
        return;
    };
    if inner.directory.withdraw(&withdrawal).is_err() {
        inner.health.announcement_failed();
    }
    inner
        .name_table
        .sync_from_entries(&inner.directory.all_entries());
    queue_control_command(inner, ControlCommand::Withdraw(vec![withdrawal]));
}

fn reconcile_directory(config: &ControlLoopConfig, network: &ControlNetwork) -> Result<usize> {
    let targets = resolution_targets(&config.mode, &config.seeds);
    let attempted = targets
        .iter()
        .filter(|id| **id != config.endpoint_id)
        .count();
    config
        .health
        .set_stale_seeds(attempted.try_into().unwrap_or(u64::MAX));
    struct PageState {
        target_id: EndpointId,
        generation: Option<u64>,
        offset: usize,
        retries: usize,
        response: mpsc::Receiver<Result<ResolveResponse>>,
    }
    let mut pages = Vec::new();
    for target_id in targets
        .iter()
        .copied()
        .filter(|id| *id != config.endpoint_id)
    {
        let request = ResolveRequest::List {
            generation: None,
            offset: 0,
            limit: MAX_DIRECTORY_BATCH,
        };
        if let Ok(response) = network.request(target_id, request) {
            pages.push(PageState {
                target_id,
                generation: None,
                offset: 0,
                retries: 0,
                response,
            });
        }
    }
    let mut learned = 0;
    let mut healthy = 0;
    let deadline = Instant::now() + internal_control_timeout(config.control_timeout);
    while !pages.is_empty() && Instant::now() < deadline {
        let mut progressed = false;
        for index in (0..pages.len()).rev() {
            match pages[index].response.try_recv() {
                Ok(Ok(ResolveResponse::ListResult {
                    generation,
                    entries,
                    next_offset,
                })) => {
                    progressed = true;
                    pages[index].generation = Some(generation);
                    for entry in entries {
                        match config.directory.register(entry) {
                            Ok(inserted) => learned += usize::from(inserted),
                            Err(
                                Error::OwnershipConflict { .. } | Error::RevisionConflict { .. },
                            ) => {
                                config.health.conflict();
                            }
                            Err(_) => config.health.reject_record(),
                        }
                    }
                    if let Some(next) = next_offset.filter(|next| *next > pages[index].offset) {
                        pages[index].offset = next;
                        let request = ResolveRequest::List {
                            generation: pages[index].generation,
                            offset: next,
                            limit: MAX_DIRECTORY_BATCH,
                        };
                        match network.request(pages[index].target_id, request) {
                            Ok(response) => pages[index].response = response,
                            Err(_) => {
                                pages.swap_remove(index);
                            }
                        }
                    } else {
                        healthy += 1;
                        pages.swap_remove(index);
                    }
                }
                Ok(Ok(ResolveResponse::Rejected { .. })) if pages[index].retries < 2 => {
                    progressed = true;
                    pages[index].retries += 1;
                    pages[index].generation = None;
                    pages[index].offset = 0;
                    let request = ResolveRequest::List {
                        generation: None,
                        offset: 0,
                        limit: MAX_DIRECTORY_BATCH,
                    };
                    match network.request(pages[index].target_id, request) {
                        Ok(response) => pages[index].response = response,
                        Err(_) => {
                            pages.swap_remove(index);
                        }
                    }
                }
                Ok(_) | Err(mpsc::TryRecvError::Disconnected) => {
                    progressed = true;
                    pages.swap_remove(index);
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        if !progressed {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    let timed_out = !pages.is_empty();
    let stale = attempted.saturating_sub(healthy);
    config
        .health
        .set_stale_seeds(stale.try_into().unwrap_or(u64::MAX));
    config
        .name_table
        .sync_from_entries(&config.directory.all_entries());
    if healthy > 0 || attempted == 0 {
        config
            .health
            .reconciled(unix_time_ms(), stale.try_into().unwrap_or(u64::MAX));
        Ok(learned)
    } else if timed_out {
        Err(Error::ControlTimeout(config.control_timeout))
    } else {
        Err(Error::ResolutionFailed(
            "directory reconciliation failed for every configured seed".to_string(),
        ))
    }
}

pub(crate) struct ControlLoopConfig {
    pub(crate) node: Node,
    pub(crate) secret: peerbus::SecretKey,
    pub(crate) endpoint_id: EndpointId,
    pub(crate) mode: DirectoryMode,
    pub(crate) seeds: Vec<EndpointId>,
    pub(crate) directory: Directory,
    pub(crate) name_table: NameTable,
    pub(crate) machine_name: String,
    pub(crate) hosted_records: Arc<Mutex<std::collections::HashMap<HostedKey, HostedRecord>>>,
    pub(crate) hosting: Arc<Mutex<()>>,
    pub(crate) withdrawn: Arc<Mutex<std::collections::HashSet<HostedKey>>>,
    pub(crate) next_revision: Arc<AtomicU64>,
    pub(crate) health: Arc<ControlPlaneHealth>,
    pub(crate) lease_duration: Duration,
    pub(crate) control_timeout: Duration,
}

pub(crate) fn run_control_loop(
    config: ControlLoopConfig,
    command_rx: mpsc::Receiver<ControlCommand>,
    shutdown_rx: std::sync::mpsc::Receiver<()>,
) {
    let network = ControlNetwork::new(&config);
    let control_interval = (config.lease_duration / 3)
        .max(Duration::from_millis(10))
        .min(Duration::from_secs(5));
    let mut next_maintenance = Instant::now();
    loop {
        match shutdown_rx.try_recv() {
            Ok(()) | Err(mpsc::TryRecvError::Disconnected) => break,
            Err(mpsc::TryRecvError::Empty) => {}
        }
        let wait = next_maintenance
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(50));
        match command_rx.recv_timeout(wait) {
            Ok(command) => {
                let ran_maintenance = matches!(
                    &command,
                    ControlCommand::Reconcile { .. } | ControlCommand::Renew { .. }
                );
                handle_control_command(&config, &network, command);
                if ran_maintenance {
                    next_maintenance = Instant::now() + control_interval;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if Instant::now() >= next_maintenance {
            let _ = reconcile_directory(&config, &network);
            adopt_node_topics(
                &config.node,
                &config.directory,
                &config.hosted_records,
                &config.hosting,
                &config.withdrawn,
                &config.name_table,
                &config.next_revision,
                &config.machine_name,
                &config.secret,
                config.lease_duration,
            );
            renew_hosted_records(&config, &network);
            next_maintenance = Instant::now() + control_interval;
        }
    }
    network.shutdown();
}

fn handle_control_command(
    config: &ControlLoopConfig,
    network: &ControlNetwork,
    command: ControlCommand,
) {
    match command {
        ControlCommand::Query {
            target_ids,
            request,
            reply,
        } => {
            let _ = reply.send(query_directory(config, network, &target_ids, &request));
        }
        ControlCommand::Reconcile { reply } => {
            let result = reconcile_directory(config, network);
            let _ = reply.send(result);
        }
        ControlCommand::Announce(entries) => announce_records(
            network,
            config.endpoint_id,
            &config.mode,
            &config.seeds,
            &config.health,
            &entries,
            config.control_timeout,
        ),
        ControlCommand::Withdraw(entries) => {
            let request = ResolveRequest::Withdraw { entries };
            let mut pending = Vec::new();
            for target_id in resolution_targets(&config.mode, &config.seeds) {
                if target_id == config.endpoint_id {
                    continue;
                }
                match network.request(target_id, request.clone()) {
                    Ok(response) => pending.push(response),
                    Err(_) => config.health.announcement_failed(),
                }
            }
            let deadline = Instant::now() + internal_control_timeout(config.control_timeout);
            for response in pending {
                let remaining = deadline.saturating_duration_since(Instant::now());
                match response.recv_timeout(remaining) {
                    Ok(Ok(ResolveResponse::Withdrawn { .. })) => {
                        config.health.announcement_succeeded();
                    }
                    _ => config.health.announcement_failed(),
                }
            }
        }
        ControlCommand::Renew { reply } => {
            let _ = reply.send(renew_hosted_records(config, network));
        }
    }
}

fn query_directory(
    config: &ControlLoopConfig,
    network: &ControlNetwork,
    target_ids: &[EndpointId],
    request: &ResolveRequest,
) -> Result<TopicEntry> {
    let ResolveRequest::Query {
        topic,
        exchange,
        request_type_hash,
        response_type_hash,
    } = request
    else {
        return Err(Error::ControlPlane(
            "query command carried a non-query request".to_string(),
        ));
    };
    let internal_timeout = internal_control_timeout(config.control_timeout);
    let deadline = Instant::now() + internal_timeout;
    let mut pending = Vec::new();
    for target_id in target_ids {
        if let Ok(response) = network.request(*target_id, request.clone()) {
            pending.push((*target_id, response));
        }
    }
    let mut rejected: Option<Error> = None;
    while Instant::now() < deadline && !pending.is_empty() {
        let mut progressed = false;
        for index in (0..pending.len()).rev() {
            match pending[index].1.try_recv() {
                Ok(Ok(ResolveResponse::QueryResult {
                    found: true,
                    entry: Some(entry),
                })) => {
                    if entry.topic() != topic {
                        config.health.reject_record();
                        pending.swap_remove(index);
                        progressed = true;
                        continue;
                    }
                    // One candidate answering with a record this caller cannot use
                    // (wrong exchange, wrong types, a stale revision) must not end
                    // the query while other candidates are still to answer.
                    let accepted = entry
                        .validate_exchange(*exchange, *request_type_hash, *response_type_hash)
                        .and_then(|()| config.directory.register(entry.clone()));
                    if let Err(error) = accepted {
                        tracing::debug!(
                            target_id = %pending[index].0,
                            %error,
                            "directory candidate answer rejected"
                        );
                        config.health.reject_record();
                        rejected = Some(error);
                        pending.swap_remove(index);
                        progressed = true;
                        continue;
                    }
                    config
                        .name_table
                        .sync_from_entries(&config.directory.all_entries());
                    return Ok(entry);
                }
                Ok(Ok(ResolveResponse::QueryResult { .. })) => {
                    let target_id = pending[index].0;
                    match network.request(target_id, request.clone()) {
                        Ok(response) => pending[index].1 = response,
                        Err(_) => {
                            pending.swap_remove(index);
                        }
                    }
                    progressed = true;
                }
                Ok(_) | Err(mpsc::TryRecvError::Disconnected) => {
                    let target_id = pending[index].0;
                    match network.request(target_id, request.clone()) {
                        Ok(response) => pending[index].1 = response,
                        Err(_) => {
                            pending.swap_remove(index);
                        }
                    }
                    progressed = true;
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        if !progressed {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    if let Some(error) = rejected {
        return Err(error);
    }
    Err(Error::ResolutionFailed(format!(
        "topic '{topic}' could not be resolved within the agent composition"
    )))
}

fn renew_hosted_records(config: &ControlLoopConfig, network: &ControlNetwork) -> usize {
    let _hosting = config.hosting.lock().unwrap();
    let hosted: Vec<_> = config
        .hosted_records
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect();
    let mut renewed = Vec::with_capacity(hosted.len());
    for record in hosted {
        let revision = config.next_revision.fetch_add(1, Ordering::Relaxed);
        let spec = TopicRecordSpec::new(
            record.topic,
            record.exchange,
            record.request_type_hash,
            record.response_type_hash,
            revision,
            Some(&config.machine_name),
        )
        .lease_expires_at_ms(
            unix_time_ms().saturating_add(
                config
                    .lease_duration
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
            ),
        );
        match TopicEntry::signed(spec, &config.secret) {
            Ok(entry) => {
                if config.directory.register(entry.clone()).is_ok() {
                    renewed.push(entry);
                }
            }
            Err(_) => config.health.announcement_failed(),
        }
    }
    for batch in renewed.chunks(MAX_DIRECTORY_BATCH) {
        announce_records(
            network,
            config.endpoint_id,
            &config.mode,
            &config.seeds,
            &config.health,
            batch,
            config.control_timeout,
        );
    }
    config
        .name_table
        .sync_from_entries(&config.directory.all_entries());
    renewed.len()
}

pub(crate) fn run_resolution_loop(
    mut server: ReqServer<DatapodMsg, DatapodMsg>,
    directory: Directory,
    name_table: NameTable,
    health: Arc<ControlPlaneHealth>,
    shutdown_rx: std::sync::mpsc::Receiver<()>,
) {
    loop {
        match shutdown_rx.try_recv() {
            Ok(()) | Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
        }
        match server.recv_timeout(Duration::from_millis(50)) {
            Ok(Some((sample, reply))) => {
                let req_bytes = sample.payload();
                let response = match ResolveRequest::from_bytes(req_bytes) {
                    Ok(req) => match req {
                        ResolveRequest::Query {
                            topic,
                            exchange,
                            request_type_hash,
                            response_type_hash,
                        } => {
                            let entry =
                                directory.lookup_exchange(&topic, exchange).filter(|entry| {
                                    entry
                                        .validate_exchange(
                                            exchange,
                                            request_type_hash,
                                            response_type_hash,
                                        )
                                        .is_ok()
                                });
                            ResolveResponse::QueryResult {
                                found: entry.is_some(),
                                entry,
                            }
                        }
                        ResolveRequest::List {
                            generation,
                            offset,
                            limit,
                        } => match directory.snapshot_page(generation, offset, limit) {
                            Ok((generation, entries, next_offset)) => ResolveResponse::ListResult {
                                generation,
                                entries,
                                next_offset,
                            },
                            Err(error) => ResolveResponse::Rejected {
                                message: error.to_string(),
                            },
                        },
                        ResolveRequest::Announce { entries } => {
                            match directory.register_many(entries) {
                                Ok(count) => ResolveResponse::Announced { count },
                                Err(error) => {
                                    if matches!(
                                        error,
                                        Error::OwnershipConflict { .. }
                                            | Error::RevisionConflict { .. }
                                    ) {
                                        health.conflict();
                                    } else {
                                        health.reject_record();
                                    }
                                    ResolveResponse::Rejected {
                                        message: error.to_string(),
                                    }
                                }
                            }
                        }
                        ResolveRequest::Withdraw { entries } => {
                            let mut count = 0;
                            let mut failure = None;
                            for withdrawal in entries.into_iter().take(MAX_DIRECTORY_BATCH) {
                                match directory.withdraw(&withdrawal) {
                                    Ok(removed) => count += usize::from(removed),
                                    Err(error) => {
                                        failure = Some(error);
                                        break;
                                    }
                                }
                            }
                            if let Some(error) = failure {
                                health.reject_record();
                                ResolveResponse::Rejected {
                                    message: error.to_string(),
                                }
                            } else {
                                ResolveResponse::Withdrawn { count }
                            }
                        }
                    },
                    Err(error) => {
                        health.reject_record();
                        ResolveResponse::Rejected {
                            message: error.to_string(),
                        }
                    }
                };
                {
                    name_table.sync_from_entries(&directory.all_entries());
                    if let Ok(resp_bytes) = response.to_bytes() {
                        let resp_msg = DatapodMsg::new(RESOLUTION_TYPE_HASH, resp_bytes);
                        if let Err(error) = reply.respond(&resp_msg) {
                            health.resolver_error();
                            tracing::warn!(%error, "directory response failed");
                        }
                    } else {
                        health.resolver_error();
                    }
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

/// A freshly bound service sees the topic ring's history: traffic sent to
/// whoever served the topic before this agent started. None of it was
/// addressed to this server, so it is dropped unanswered at bind instead of
/// being served a second time by a restarted host.
fn drop_predecessor_traffic(topic: &str, mut take_one: impl FnMut() -> Result<bool>) -> Result<()> {
    let mut dropped = 0usize;
    while take_one()? {
        dropped += 1;
    }
    if dropped > 0 {
        tracing::debug!(topic, dropped, "dropped traffic that predates this server");
    }
    Ok(())
}

/// Sign a directory record for every topic the peerbus node hosts that the
/// directory does not know: topics opened through `Agent::node()` instead of
/// the agent. The response type of two-type exchanges is not known here, so
/// those records carry none. Returns the records added.
#[allow(clippy::too_many_arguments)]
fn adopt_node_topics(
    node: &Node,
    directory: &Directory,
    hosted_records: &Mutex<std::collections::HashMap<HostedKey, HostedRecord>>,
    hosting: &Mutex<()>,
    withdrawn: &Mutex<std::collections::HashSet<HostedKey>>,
    name_table: &NameTable,
    next_revision: &AtomicU64,
    machine_name: &str,
    secret: &peerbus::SecretKey,
    lease_duration: Duration,
) -> Vec<TopicEntry> {
    let _hosting = hosting.lock().unwrap();
    let mut adopted = Vec::new();
    for hosted in node.hosted_topics() {
        let exchange = match hosted.mode {
            peerbus::TopicMode::PubSub => ExchangeKind::PubSub,
            peerbus::TopicMode::ReqRes => ExchangeKind::ReqRes,
            peerbus::TopicMode::QueAns => ExchangeKind::QueAns,
            peerbus::TopicMode::PutAck => ExchangeKind::PutAck,
            peerbus::TopicMode::Pip => ExchangeKind::Pip,
        };
        // The agent's own resolver is peerbus-hosted too, under its raw name.
        if hosted.topic == RESOLUTION_TOPIC {
            continue;
        }
        let Ok(topic) = normalize_topic(&hosted.topic) else {
            continue;
        };
        let key = (topic.clone(), exchange);
        if hosted_records.lock().unwrap().contains_key(&key)
            || withdrawn.lock().unwrap().contains(&key)
        {
            continue;
        }
        let spec = TopicRecordSpec::new(
            &topic,
            exchange,
            hosted.type_hash,
            None,
            next_revision.fetch_add(1, Ordering::Relaxed),
            Some(machine_name),
        )
        .lease_expires_at_ms(
            unix_time_ms()
                .saturating_add(lease_duration.as_millis().try_into().unwrap_or(u64::MAX)),
        );
        let Ok(entry) = TopicEntry::signed(spec, secret) else {
            continue;
        };
        if directory.register(entry.clone()).is_err() {
            continue;
        }
        hosted_records.lock().unwrap().insert(
            key,
            HostedRecord {
                topic,
                exchange,
                request_type_hash: hosted.type_hash,
                response_type_hash: None,
            },
        );
        adopted.push(entry);
    }
    if !adopted.is_empty() {
        name_table.sync_from_entries(&directory.all_entries());
    }
    adopted
}
