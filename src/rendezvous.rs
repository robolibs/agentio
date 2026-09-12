//! Host-local rendezvous. Every live agent leaves one JSON record under
//! `$XDG_RUNTIME_DIR/agentio/agents/`, named by its endpoint id, and removes
//! it on drop; a process on the same host lists them to find an agent by
//! name without knowing its id. Records whose process is gone are pruned by
//! whoever lists next, so a crashed agent does not linger.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use peerbus::{EndpointAddr, EndpointId};
use serde::{Deserialize, Serialize};

use crate::agent::TryIntoBootstrapPeer;
use crate::error::{Error, Result};
use crate::identity::did_key::{did_key_to_endpoint, endpoint_to_did_key};

/// One live agent on this host.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalAgent {
    /// The builder name, if the agent has one.
    pub name: Option<String>,
    /// The participant prefix, if the agent has one.
    pub participant: Option<String>,
    /// `did:key` of the endpoint.
    pub did: String,
    /// Postcard encoding of the endpoint address, lowercase hex.
    pub addr: String,
    /// The hosting process.
    pub pid: u32,
    /// Unix milliseconds when the agent was built.
    pub started_unix_ms: u64,
}

impl LocalAgent {
    /// The endpoint id behind `did`.
    pub fn endpoint_id(&self) -> Result<EndpointId> {
        did_key_to_endpoint(&self.did)
    }

    /// The endpoint address behind `addr`.
    pub fn endpoint_addr(&self) -> Result<EndpointAddr> {
        let bytes = hex_to_bytes(&self.addr).ok_or_else(|| {
            Error::Format(format!("rendezvous address is not hex: {}", self.addr))
        })?;
        Ok(postcard::from_bytes(&bytes)?)
    }
}

impl TryIntoBootstrapPeer for &LocalAgent {
    fn try_into_bootstrap_peer(self) -> Result<EndpointId> {
        self.endpoint_id()
    }
}

impl TryIntoBootstrapPeer for LocalAgent {
    fn try_into_bootstrap_peer(self) -> Result<EndpointId> {
        self.endpoint_id()
    }
}

/// Where this host's records live: `AGENTIO_RENDEZVOUS_DIR`, else
/// `$XDG_RUNTIME_DIR/agentio/agents`, else a per-user directory under the
/// system temp dir.
pub fn rendezvous_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("AGENTIO_RENDEZVOUS_DIR") {
        return PathBuf::from(dir);
    }
    if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(runtime).join("agentio").join("agents");
    }
    let user = std::env::var("USER").unwrap_or_else(|_| "user".to_string());
    std::env::temp_dir()
        .join(format!("agentio-{user}"))
        .join("agents")
}

/// Every live agent on this host, oldest first. Stale records are removed.
pub fn local_agents() -> Vec<LocalAgent> {
    let Ok(dir) = std::fs::read_dir(rendezvous_dir()) else {
        return Vec::new();
    };
    let mut agents = Vec::new();
    for item in dir.flatten() {
        let path = item.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let record = std::fs::read_to_string(&path)
            .ok()
            .and_then(|text| serde_json::from_str::<LocalAgent>(&text).ok());
        match record {
            Some(record) if pid_alive(record.pid) => agents.push(record),
            _ => {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    agents.sort_by_key(|a| a.started_unix_ms);
    agents
}

/// The live agent with this builder name, the oldest when several share it.
pub fn find_local(name: &str) -> Option<LocalAgent> {
    local_agents()
        .into_iter()
        .find(|agent| agent.name.as_deref() == Some(name))
}

/// The live agent with this `did:key`.
pub fn find_local_did(did: &str) -> Option<LocalAgent> {
    local_agents().into_iter().find(|agent| agent.did == did)
}

pub(crate) fn record_path(endpoint_id: &EndpointId) -> PathBuf {
    rendezvous_dir().join(format!("{}.json", bytes_to_hex(endpoint_id.as_bytes())))
}

/// Write this agent's record; called once the node is bound.
pub(crate) fn publish(
    name: Option<&str>,
    participant: Option<&str>,
    endpoint_id: &EndpointId,
    addr: &EndpointAddr,
) -> Result<PathBuf> {
    let record = LocalAgent {
        name: name.map(str::to_string),
        participant: participant.map(str::to_string),
        did: endpoint_to_did_key(endpoint_id)?,
        addr: bytes_to_hex(&postcard::to_stdvec(addr)?),
        pid: std::process::id(),
        started_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    };
    let path = record_path(endpoint_id);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let json =
        serde_json::to_string_pretty(&record).map_err(|error| Error::Format(error.to_string()))?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json)?;
    std::fs::rename(&tmp, &path)?;
    Ok(path)
}

pub(crate) fn withdraw(endpoint_id: &EndpointId) {
    let _ = std::fs::remove_file(record_path(endpoint_id));
}

fn pid_alive(pid: u32) -> bool {
    if cfg!(target_os = "linux") {
        std::path::Path::new("/proc").join(pid.to_string()).exists()
    } else {
        true
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_to_bytes(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips() {
        let bytes = [0u8, 1, 0xab, 0xff];
        assert_eq!(bytes_to_hex(&bytes), "0001abff");
        assert_eq!(hex_to_bytes("0001abff").unwrap(), bytes);
        assert!(hex_to_bytes("abc").is_none());
        assert!(hex_to_bytes("zz").is_none());
    }
}
