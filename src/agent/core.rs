use peerbus::{
    AckServer, AnsServer, DatapodMsg, EndpointId, Node, PipClient, PipServer, Publisher, PutClient,
    QueClient, ReqClient, ReqServer, Subscriber, wire_type_hash,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    pub(crate) endpoint_id: EndpointId,
    pub(crate) directory_mode: DirectoryMode,
    pub(crate) bootstrap_peers: Vec<EndpointId>,
    pub(crate) allowed_peers: Vec<EndpointId>,
    pub(crate) allow_any_peer: bool,
    pub(crate) no_relay: bool,
    pub(crate) skip_shm: bool,
    pub(crate) health: Arc<ControlPlaneHealth>,
    pub(crate) next_revision: Arc<AtomicU64>,
    pub(crate) hosted_records: Arc<Mutex<std::collections::HashMap<HostedKey, HostedRecord>>>,
    pub(crate) workers: Mutex<Vec<ControlWorker>>,
}

pub(crate) struct ControlWorker {
    pub(crate) shutdown_tx: std::sync::mpsc::Sender<()>,
    pub(crate) join_handle: std::thread::JoinHandle<()>,
}

impl Drop for AgentInner {
    fn drop(&mut self) {
        let workers = match self.workers.get_mut() {
            Ok(workers) => std::mem::take(workers),
            Err(poisoned) => std::mem::take(poisoned.into_inner()),
        };
        for worker in &workers {
            let _ = worker.shutdown_tx.send(());
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

    pub fn reconcile_now(&self) -> Result<usize> {
        reconcile_directory(
            &self.inner.node,
            self.inner.endpoint_id,
            &self.inner.directory_mode,
            &self.inner.bootstrap_peers,
            &self.inner.directory,
            &self.inner.name_table,
            &self.inner.health,
        )
    }

    pub fn renew_now(&self) -> usize {
        renew_hosted_records(&ControlLoopConfig {
            node: self.inner.node.clone(),
            secret: self.inner.secret.clone(),
            endpoint_id: self.inner.endpoint_id,
            mode: self.inner.directory_mode.clone(),
            seeds: self.inner.bootstrap_peers.clone(),
            directory: self.inner.directory.clone(),
            name_table: self.inner.name_table.clone(),
            machine_name: self.inner.machine_name.clone(),
            hosted_records: self.inner.hosted_records.clone(),
            next_revision: self.inner.next_revision.clone(),
            health: self.inner.health.clone(),
        })
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
        let publisher = self.inner.node.publisher::<T>(&normalized)?;
        let entry = TopicEntry::signed(
            TopicRecordSpec::new(
                &normalized,
                ExchangeKind::PubSub,
                wire_type_hash::<T>(),
                None,
                self.next_revision(),
                Some(&self.inner.machine_name),
            ),
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
        let server = self.inner.node.req_server::<Req, Res>(&normalized)?;
        let entry = TopicEntry::signed(
            TopicRecordSpec::new(
                &normalized,
                ExchangeKind::ReqRes,
                wire_type_hash::<Req>(),
                Some(wire_type_hash::<Res>()),
                self.next_revision(),
                Some(&self.inner.machine_name),
            ),
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
        let server = self.inner.node.que_server::<Que, Ans>(&normalized)?;
        let entry = TopicEntry::signed(
            TopicRecordSpec::new(
                &normalized,
                ExchangeKind::QueAns,
                wire_type_hash::<Que>(),
                Some(wire_type_hash::<Ans>()),
                self.next_revision(),
                Some(&self.inner.machine_name),
            ),
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
        let server = self.inner.node.put_server::<Put, Ack>(&normalized)?;
        let entry = TopicEntry::signed(
            TopicRecordSpec::new(
                &normalized,
                ExchangeKind::PutAck,
                wire_type_hash::<Put>(),
                Some(wire_type_hash::<Ack>()),
                self.next_revision(),
                Some(&self.inner.machine_name),
            ),
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
            ),
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

        for target_id in &targets {
            if let Ok(mut client) = self
                .inner
                .node
                .req_client::<DatapodMsg, DatapodMsg>(*target_id, RESOLUTION_TOPIC)
            {
                let request = ResolveRequest::Query {
                    topic: topic.to_string(),
                    exchange,
                    request_type_hash,
                    response_type_hash,
                };
                if let Ok(request_bytes) = request.to_bytes() {
                    let message = DatapodMsg::new(RESOLUTION_TYPE_HASH, request_bytes);
                    if let Ok(sample) = client.call(&message)
                        && let Ok(ResolveResponse::QueryResult {
                            found: true,
                            entry: Some(entry),
                        }) = ResolveResponse::from_bytes(sample.payload())
                    {
                        entry.validate_exchange(exchange, request_type_hash, response_type_hash)?;
                        self.inner.directory.register(entry.clone())?;
                        return Ok(entry);
                    }
                }
            }
        }

        Err(Error::ResolutionFailed(format!(
            "topic '{topic}' could not be resolved within the agent composition"
        )))
    }

    fn next_revision(&self) -> u64 {
        self.inner.next_revision.fetch_add(1, Ordering::Relaxed)
    }

    fn register_hosted(&self, entry: &TopicEntry) -> Result<()> {
        self.inner.directory.register(entry.clone())?;
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
        announce_entry(&self.inner, entry);
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
    node: &Node,
    target_id: EndpointId,
    request: &ResolveRequest,
) -> Result<ResolveResponse> {
    let request_bytes = request.to_bytes()?;
    let message = DatapodMsg::new(RESOLUTION_TYPE_HASH, request_bytes);
    let mut client = node.req_client::<DatapodMsg, DatapodMsg>(target_id, RESOLUTION_TOPIC)?;
    let sample = client.call(&message)?;
    ResolveResponse::from_bytes(sample.payload())
}

fn announce_entry(inner: &AgentInner, entry: &TopicEntry) {
    announce_records(
        &inner.node,
        inner.endpoint_id,
        &inner.directory_mode,
        &inner.bootstrap_peers,
        &inner.health,
        std::slice::from_ref(entry),
    );
}

fn announce_records(
    node: &Node,
    endpoint_id: EndpointId,
    mode: &DirectoryMode,
    seeds: &[EndpointId],
    health: &ControlPlaneHealth,
    entries: &[TopicEntry],
) {
    let targets = resolution_targets(mode, seeds);
    health.set_pending_announcements(targets.len().try_into().unwrap_or(u64::MAX));
    let request = ResolveRequest::Announce {
        entries: entries.to_vec(),
    };
    let mut pending = targets.len();
    for target_id in targets {
        if target_id == endpoint_id {
            pending = pending.saturating_sub(1);
            continue;
        }
        match send_control_request(node, target_id, &request) {
            Ok(ResolveResponse::Announced { .. }) => health.announcement_succeeded(),
            Ok(ResolveResponse::Rejected { message }) => {
                health.announcement_failed();
                tracing::warn!(%target_id, %message, "directory announcement rejected");
            }
            Ok(_) | Err(_) => health.announcement_failed(),
        }
        pending = pending.saturating_sub(1);
        health.set_pending_announcements(pending.try_into().unwrap_or(u64::MAX));
    }
    health.set_pending_announcements(0);
}

pub(crate) fn withdraw_owned_entry(inner: &AgentInner, entry: &TopicEntry) {
    inner
        .hosted_records
        .lock()
        .unwrap()
        .remove(&(entry.topic().to_string(), entry.exchange()));
    let revision = inner.next_revision.fetch_add(1, Ordering::Relaxed);
    let Ok(withdrawal) = TopicWithdrawal::signed(entry, revision, &inner.secret) else {
        inner.health.announcement_failed();
        return;
    };
    if inner.directory.withdraw(&withdrawal).is_err() {
        inner.health.announcement_failed();
    }
    inner
        .name_table
        .sync_from_entries(&inner.directory.all_entries());
    let request = ResolveRequest::Withdraw {
        entries: vec![withdrawal],
    };
    for target_id in resolution_targets(&inner.directory_mode, &inner.bootstrap_peers) {
        if target_id == inner.endpoint_id {
            continue;
        }
        match send_control_request(&inner.node, target_id, &request) {
            Ok(ResolveResponse::Withdrawn { .. }) => inner.health.announcement_succeeded(),
            _ => inner.health.announcement_failed(),
        }
    }
}

pub(crate) fn reconcile_directory(
    node: &Node,
    endpoint_id: EndpointId,
    mode: &DirectoryMode,
    seeds: &[EndpointId],
    directory: &Directory,
    name_table: &NameTable,
    health: &ControlPlaneHealth,
) -> Result<usize> {
    let targets = resolution_targets(mode, seeds);
    let mut learned = 0;
    let mut healthy = 0;
    for target_id in targets.iter().copied().filter(|id| *id != endpoint_id) {
        let mut offset = 0;
        let mut seed_healthy = false;
        loop {
            let request = ResolveRequest::List {
                offset,
                limit: MAX_DIRECTORY_BATCH,
            };
            let response = send_control_request(node, target_id, &request);
            let Ok(ResolveResponse::ListResult {
                entries,
                next_offset,
            }) = response
            else {
                break;
            };
            seed_healthy = true;
            for entry in entries {
                match directory.register(entry) {
                    Ok(inserted) => learned += usize::from(inserted),
                    Err(Error::OwnershipConflict { .. }) => health.conflict(),
                    Err(_) => health.reject_record(),
                }
            }
            let Some(next) = next_offset else {
                break;
            };
            if next <= offset {
                break;
            }
            offset = next;
        }
        healthy += usize::from(seed_healthy);
    }
    let attempted = targets.iter().filter(|id| **id != endpoint_id).count();
    let stale = attempted.saturating_sub(healthy);
    name_table.sync_from_entries(&directory.all_entries());
    if healthy > 0 || attempted == 0 {
        health.reconciled(unix_time_ms(), stale.try_into().unwrap_or(u64::MAX));
        Ok(learned)
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
    pub(crate) next_revision: Arc<AtomicU64>,
    pub(crate) health: Arc<ControlPlaneHealth>,
}

pub(crate) fn run_control_loop(
    config: ControlLoopConfig,
    shutdown_rx: std::sync::mpsc::Receiver<()>,
) {
    const CONTROL_INTERVAL: Duration = Duration::from_secs(5);
    loop {
        let _ = reconcile_directory(
            &config.node,
            config.endpoint_id,
            &config.mode,
            &config.seeds,
            &config.directory,
            &config.name_table,
            &config.health,
        );
        renew_hosted_records(&config);
        match shutdown_rx.recv_timeout(CONTROL_INTERVAL) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}

fn renew_hosted_records(config: &ControlLoopConfig) -> usize {
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
            &config.node,
            config.endpoint_id,
            &config.mode,
            &config.seeds,
            &config.health,
            batch,
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
                if let Ok(req) = ResolveRequest::from_bytes(req_bytes) {
                    let resp = match req {
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
                        ResolveRequest::List { offset, limit } => {
                            let entries = directory.all_entries();
                            let end = offset.saturating_add(limit).min(entries.len());
                            let page = entries.get(offset..end).unwrap_or(&[]).to_vec();
                            ResolveResponse::ListResult {
                                entries: page,
                                next_offset: (end < entries.len()).then_some(end),
                            }
                        }
                        ResolveRequest::Announce { entries } => {
                            match directory.register_many(entries) {
                                Ok(count) => ResolveResponse::Announced { count },
                                Err(error) => {
                                    health.reject_record();
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
                    };
                    name_table.sync_from_entries(&directory.all_entries());
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
