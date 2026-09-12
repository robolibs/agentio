//! Python bindings for agentio (pyo3). `agentio.Agent` wraps [`Agent`];
//! the typed clients and servers it hands out are peerbus's datapod Python
//! classes, so a script written against `peerbus` keeps its message code and
//! only swaps peer ids for topic names. Hosted handles are subclasses that
//! also own the directory record, which is withdrawn when they are dropped.

use std::path::PathBuf;
use std::time::Duration;

use peerbus::python::{
    PyDatapodAckServer, PyDatapodAnsServer, PyDatapodPipClient, PyDatapodPipServer,
    PyDatapodPublisher, PyDatapodPutClient, PyDatapodQueClient, PyDatapodReqClient,
    PyDatapodReqServer, PyDatapodSubscriber, PyNode,
};
use peerbus::{DatapodMsg, EndpointId};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};

use crate::agent::{Agent, DirectoryMode, HostedGuard, TryIntoBootstrapPeer};
use crate::directory::{ExchangeKind, TopicEntry};
use crate::identity::IdentitySource;
use crate::identity::did_key::{did_key_to_endpoint, endpoint_to_did_key};
use crate::rendezvous::{self, LocalAgent};

fn py_err(error: crate::Error) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

/// A peer given as `did:key:…`, or as the hex endpoint address peerbus's
/// Python binding uses.
fn peer_from_str(text: &str) -> PyResult<EndpointId> {
    if text.starts_with("did:key:") {
        return did_key_to_endpoint(text).map_err(py_err);
    }
    if let Ok(peer) = text.try_into_bootstrap_peer() {
        return Ok(peer);
    }
    let bytes = hex_decode(text)?;
    let addr: peerbus::EndpointAddr = postcard::from_bytes(&bytes).map_err(|e| {
        PyRuntimeError::new_err(format!("not a did:key nor an endpoint address: {e}"))
    })?;
    Ok(addr.id)
}

fn hex_decode(text: &str) -> PyResult<Vec<u8>> {
    if text.len() % 2 != 0 {
        return Err(PyRuntimeError::new_err("odd-length hex"));
    }
    (0..text.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&text[i..i + 2], 16)
                .map_err(|_| PyRuntimeError::new_err("not a hex string"))
        })
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn exchange_name(exchange: ExchangeKind) -> &'static str {
    match exchange {
        ExchangeKind::PubSub => "pubsub",
        ExchangeKind::ReqRes => "reqres",
        ExchangeKind::QueAns => "queans",
        ExchangeKind::PutAck => "putack",
        ExchangeKind::Pip => "pip",
    }
}

fn entry_dict(py: Python<'_>, entry: &TopicEntry) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("topic", entry.topic())?;
    dict.set_item("exchange", exchange_name(entry.exchange()))?;
    dict.set_item(
        "did",
        endpoint_to_did_key(&entry.endpoint_id()).map_err(py_err)?,
    )?;
    dict.set_item("request_type_hash", entry.request_type_hash())?;
    dict.set_item("response_type_hash", entry.response_type_hash())?;
    dict.set_item("revision", entry.revision())?;
    dict.set_item("lease_expires_at_ms", entry.lease_expires_at_ms())?;
    dict.set_item("machine_name", entry.machine_name())?;
    Ok(dict.into())
}

fn local_agent_dict(py: Python<'_>, agent: &LocalAgent) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", agent.name.as_deref())?;
    dict.set_item("participant", agent.participant.as_deref())?;
    dict.set_item("did", &agent.did)?;
    dict.set_item("addr", &agent.addr)?;
    dict.set_item("pid", agent.pid)?;
    dict.set_item("started_unix_ms", agent.started_unix_ms)?;
    Ok(dict.into())
}

/// A named agent on peerbus with identity and directory resolution.
#[pyclass(name = "Agent")]
pub struct PyAgent {
    agent: Agent,
}

#[pymethods]
impl PyAgent {
    /// Build an agent. `name` persists a key under the agentio keys dir;
    /// `key_file` or `secret_key` (32 bytes) pin the identity instead;
    /// `random` makes an in-memory key. `bootstrap`, `front_door` and
    /// `allow` take `did:key` strings or hex endpoint addresses.
    #[new]
    #[pyo3(signature = (
        name=None,
        key_file=None,
        secret_key=None,
        random=false,
        participant=None,
        bootstrap=None,
        front_door=None,
        allow=None,
        allow_any=false,
        no_relay=false,
        skip_shm=false,
        rendezvous=true,
        lease_ms=None,
        control_timeout_ms=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        name: Option<String>,
        key_file: Option<PathBuf>,
        secret_key: Option<Vec<u8>>,
        random: bool,
        participant: Option<String>,
        bootstrap: Option<Vec<String>>,
        front_door: Option<String>,
        allow: Option<Vec<String>>,
        allow_any: bool,
        no_relay: bool,
        skip_shm: bool,
        rendezvous: bool,
        lease_ms: Option<u64>,
        control_timeout_ms: Option<u64>,
    ) -> PyResult<Self> {
        let mut builder = Agent::builder();
        if let Some(name) = name {
            builder = builder.name(name);
        }
        if let Some(path) = key_file {
            builder = builder.identity(IdentitySource::File(path));
        }
        if let Some(bytes) = secret_key {
            let key: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
                PyRuntimeError::new_err(format!("secret_key must be 32 bytes, got {}", bytes.len()))
            })?;
            builder = builder.identity(peerbus::SecretKey::from_bytes(&key));
        }
        if random {
            builder = builder.identity(IdentitySource::Random);
        }
        if let Some(participant) = participant {
            builder = builder.participant(participant);
        }
        for peer in bootstrap.unwrap_or_default() {
            builder = builder.bootstrap([peer_from_str(&peer)?]);
        }
        if let Some(front_door) = front_door {
            let id = peer_from_str(&front_door)?;
            builder = builder
                .bootstrap([id])
                .directory(DirectoryMode::FrontDoor(id));
        }
        for peer in allow.unwrap_or_default() {
            builder = builder.allow_peer(peer_from_str(&peer)?);
        }
        if allow_any {
            builder = builder.allow_any_peer();
        }
        if no_relay {
            builder = builder.no_relay();
        }
        if skip_shm {
            builder = builder.skip_shm();
        }
        if !rendezvous {
            builder = builder.no_rendezvous();
        }
        if let Some(ms) = lease_ms {
            builder = builder.lease_duration(Duration::from_millis(ms));
        }
        if let Some(ms) = control_timeout_ms {
            builder = builder.control_timeout(Duration::from_millis(ms));
        }
        Ok(Self {
            agent: builder.build().map_err(py_err)?,
        })
    }

    /// This agent's `did:key`.
    fn did(&self) -> PyResult<String> {
        self.agent.did_key().map_err(py_err)
    }

    /// This agent's endpoint id as hex.
    fn endpoint_id(&self) -> String {
        hex_encode(self.agent.endpoint_id().as_bytes())
    }

    /// The full endpoint address as hex, the form peerbus's Python binding
    /// dials directly.
    fn endpoint_addr(&self) -> PyResult<String> {
        let bytes = postcard::to_stdvec(&self.agent.endpoint_addr())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(hex_encode(&bytes))
    }

    fn name(&self) -> String {
        self.agent.name().to_string()
    }

    fn participant(&self) -> Option<String> {
        self.agent.participant().map(str::to_string)
    }

    /// A relative topic under this agent's participant prefix.
    fn own_topic(&self, topic: &str) -> PyResult<String> {
        self.agent.own_topic(topic).map_err(py_err)
    }

    fn bootstrap_peers(&self) -> PyResult<Vec<String>> {
        self.agent
            .bootstrap_peers()
            .iter()
            .map(|id| endpoint_to_did_key(id).map_err(py_err))
            .collect()
    }

    fn allowed_peers(&self) -> PyResult<Vec<String>> {
        self.agent
            .allowed_peers()
            .iter()
            .map(|id| endpoint_to_did_key(id).map_err(py_err))
            .collect()
    }

    /// Permit one more peer to connect, at runtime.
    fn allow_peer(&self, peer: &str) -> PyResult<()> {
        self.agent.allow_peer(peer_from_str(peer)?).map_err(py_err)
    }

    fn allows_peer(&self, peer: &str) -> PyResult<bool> {
        Ok(self.agent.allows_peer(&peer_from_str(peer)?))
    }

    fn wait_for_direct_addresses(&self, py: Python<'_>, timeout_s: f64) -> PyResult<()> {
        let timeout = Duration::from_secs_f64(timeout_s.max(0.0));
        py.allow_threads(|| self.agent.wait_for_direct_addresses(timeout))
            .map_err(py_err)
    }

    /// The local directory's record for `topic`, as a dict.
    fn resolve(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        let entry = self.agent.resolve_topic(topic).map_err(py_err)?;
        entry_dict(py, &entry)
    }

    /// Every record the local directory holds.
    fn directory(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for entry in self.agent.directory().all_entries() {
            list.append(entry_dict(py, &entry)?)?;
        }
        Ok(list.into())
    }

    fn reconcile_now(&self, py: Python<'_>) -> PyResult<usize> {
        py.allow_threads(|| self.agent.reconcile_now())
            .map_err(py_err)
    }

    fn renew_now(&self, py: Python<'_>) -> usize {
        py.allow_threads(|| self.agent.renew_now())
    }

    fn adopt_hosted_topics(&self) -> PyResult<usize> {
        self.agent.adopt_hosted_topics().map_err(py_err)
    }

    /// The path of this agent's rendezvous record, if it wrote one.
    fn rendezvous_path(&self) -> Option<String> {
        self.agent
            .rendezvous_path()
            .map(|p| p.display().to_string())
    }

    /// The underlying peerbus node, for peer-addressed escape hatches.
    fn node(&self) -> PyNode {
        PyNode::from(self.agent.node().clone())
    }

    // ── hosting ───────────────────────────────────────────────────────

    /// Host a pub/sub topic; the record is withdrawn when the handle drops.
    fn publish(&self, py: Python<'_>, topic: &str) -> PyResult<Py<HostedPublisher>> {
        hosted_publisher(py, self.agent.publish::<DatapodMsg>(topic).map_err(py_err)?)
    }

    fn publish_in(
        &self,
        py: Python<'_>,
        participant: &str,
        topic: &str,
    ) -> PyResult<Py<HostedPublisher>> {
        hosted_publisher(
            py,
            self.agent
                .publish_in::<DatapodMsg>(participant, topic)
                .map_err(py_err)?,
        )
    }

    fn publish_own(&self, py: Python<'_>, topic: &str) -> PyResult<Py<HostedPublisher>> {
        hosted_publisher(
            py,
            self.agent
                .publish_own::<DatapodMsg>(topic)
                .map_err(py_err)?,
        )
    }

    fn req_server(&self, py: Python<'_>, topic: &str) -> PyResult<Py<HostedReqServer>> {
        let (server, guard) = self
            .agent
            .req_server::<DatapodMsg, DatapodMsg>(topic)
            .map_err(py_err)?
            .into_parts();
        Py::new(
            py,
            PyClassInitializer::from(PyDatapodReqServer::from(server))
                .add_subclass(HostedReqServer { _guard: guard }),
        )
    }

    fn que_server(&self, py: Python<'_>, topic: &str) -> PyResult<Py<HostedAnsServer>> {
        let (server, guard) = self
            .agent
            .que_server::<DatapodMsg, DatapodMsg>(topic)
            .map_err(py_err)?
            .into_parts();
        Py::new(
            py,
            PyClassInitializer::from(PyDatapodAnsServer::from(server))
                .add_subclass(HostedAnsServer { _guard: guard }),
        )
    }

    fn put_server(&self, py: Python<'_>, topic: &str) -> PyResult<Py<HostedAckServer>> {
        let (server, guard) = self
            .agent
            .put_server::<DatapodMsg, DatapodMsg>(topic)
            .map_err(py_err)?
            .into_parts();
        Py::new(
            py,
            PyClassInitializer::from(PyDatapodAckServer::from(server))
                .add_subclass(HostedAckServer { _guard: guard }),
        )
    }

    fn pip_server(&self, py: Python<'_>, topic: &str) -> PyResult<Py<HostedPipServer>> {
        let (server, guard) = self
            .agent
            .pip_server::<DatapodMsg, DatapodMsg>(topic)
            .map_err(py_err)?
            .into_parts();
        Py::new(
            py,
            PyClassInitializer::from(PyDatapodPipServer::from(server))
                .add_subclass(HostedPipServer { _guard: guard }),
        )
    }

    // ── consuming, by topic name ──────────────────────────────────────

    fn subscribe(&self, py: Python<'_>, topic: &str) -> PyResult<PyDatapodSubscriber> {
        py.allow_threads(|| self.agent.subscribe::<DatapodMsg>(topic))
            .map(Into::into)
            .map_err(py_err)
    }

    fn subscribe_in(
        &self,
        py: Python<'_>,
        participant: &str,
        topic: &str,
    ) -> PyResult<PyDatapodSubscriber> {
        py.allow_threads(|| self.agent.subscribe_in::<DatapodMsg>(participant, topic))
            .map(Into::into)
            .map_err(py_err)
    }

    fn subscribe_own(&self, py: Python<'_>, topic: &str) -> PyResult<PyDatapodSubscriber> {
        py.allow_threads(|| self.agent.subscribe_own::<DatapodMsg>(topic))
            .map(Into::into)
            .map_err(py_err)
    }

    fn req_client(&self, py: Python<'_>, topic: &str) -> PyResult<PyDatapodReqClient> {
        py.allow_threads(|| self.agent.req_client::<DatapodMsg, DatapodMsg>(topic))
            .map(Into::into)
            .map_err(py_err)
    }

    fn que_client(&self, py: Python<'_>, topic: &str) -> PyResult<PyDatapodQueClient> {
        py.allow_threads(|| self.agent.que_client::<DatapodMsg, DatapodMsg>(topic))
            .map(Into::into)
            .map_err(py_err)
    }

    fn put_client(&self, py: Python<'_>, topic: &str) -> PyResult<PyDatapodPutClient> {
        py.allow_threads(|| self.agent.put_client::<DatapodMsg, DatapodMsg>(topic))
            .map(Into::into)
            .map_err(py_err)
    }

    fn pip_client(&self, py: Python<'_>, topic: &str) -> PyResult<PyDatapodPipClient> {
        py.allow_threads(|| self.agent.pip_client::<DatapodMsg, DatapodMsg>(topic))
            .map(Into::into)
            .map_err(py_err)
    }
}

fn hosted_publisher(
    py: Python<'_>,
    registered: crate::agent::Registered<peerbus::Publisher<DatapodMsg>>,
) -> PyResult<Py<HostedPublisher>> {
    let (publisher, guard) = registered.into_parts();
    Py::new(
        py,
        PyClassInitializer::from(PyDatapodPublisher::from(publisher))
            .add_subclass(HostedPublisher { _guard: guard }),
    )
}

/// A `DatapodPublisher` that also holds its directory record.
#[pyclass(name = "HostedPublisher", extends = PyDatapodPublisher)]
pub struct HostedPublisher {
    _guard: HostedGuard,
}

/// A `DatapodReqServer` that also holds its directory record.
#[pyclass(name = "HostedReqServer", extends = PyDatapodReqServer)]
pub struct HostedReqServer {
    _guard: HostedGuard,
}

/// A `DatapodAnsServer` that also holds its directory record.
#[pyclass(name = "HostedAnsServer", extends = PyDatapodAnsServer)]
pub struct HostedAnsServer {
    _guard: HostedGuard,
}

/// A `DatapodAckServer` that also holds its directory record.
#[pyclass(name = "HostedAckServer", extends = PyDatapodAckServer)]
pub struct HostedAckServer {
    _guard: HostedGuard,
}

/// A `DatapodPipServer` that also holds its directory record.
#[pyclass(name = "HostedPipServer", extends = PyDatapodPipServer)]
pub struct HostedPipServer {
    _guard: HostedGuard,
}

/// Every live agent on this host, as dicts (see `agentio::rendezvous`).
#[pyfunction]
fn local_agents(py: Python<'_>) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for agent in rendezvous::local_agents() {
        list.append(local_agent_dict(py, &agent)?)?;
    }
    Ok(list.into())
}

/// The live agent with this builder name, if any.
#[pyfunction]
fn find_local(py: Python<'_>, name: &str) -> PyResult<Option<Py<PyDict>>> {
    rendezvous::find_local(name)
        .map(|agent| local_agent_dict(py, &agent))
        .transpose()
}

/// Where this host's rendezvous records live.
#[pyfunction]
fn rendezvous_dir() -> String {
    rendezvous::rendezvous_dir().display().to_string()
}

/// Convert between `did:key` and the hex endpoint id.
#[pyfunction]
fn did_to_endpoint_id(did: &str) -> PyResult<String> {
    Ok(hex_encode(
        did_key_to_endpoint(did).map_err(py_err)?.as_bytes(),
    ))
}

#[pymodule]
fn agentio(module: &Bound<'_, PyModule>) -> PyResult<()> {
    peerbus::python::register_python_module(module)?;
    module.add_class::<PyAgent>()?;
    module.add_class::<HostedPublisher>()?;
    module.add_class::<HostedReqServer>()?;
    module.add_class::<HostedAnsServer>()?;
    module.add_class::<HostedAckServer>()?;
    module.add_class::<HostedPipServer>()?;
    module.add_function(wrap_pyfunction!(local_agents, module)?)?;
    module.add_function(wrap_pyfunction!(find_local, module)?)?;
    module.add_function(wrap_pyfunction!(rendezvous_dir, module)?)?;
    module.add_function(wrap_pyfunction!(did_to_endpoint_id, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
