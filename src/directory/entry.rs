use authbox::pki::{sign_ed25519_detached, verify_ed25519_signature};
use peerbus::{EndpointId, SecretKey};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};

pub const DIRECTORY_PROTOCOL_VERSION: u16 = 1;
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeKind {
    PubSub,
    ReqRes,
    QueAns,
    PutAck,
    Pip,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRecordSpec {
    topic: String,
    exchange: ExchangeKind,
    request_type_hash: u64,
    response_type_hash: Option<u64>,
    revision: u64,
    lease_expires_at_ms: u64,
    machine_name: Option<String>,
}

impl TopicRecordSpec {
    pub fn new(
        topic: impl Into<String>,
        exchange: ExchangeKind,
        request_type_hash: u64,
        response_type_hash: Option<u64>,
        revision: u64,
        machine_name: Option<impl Into<String>>,
    ) -> Self {
        Self {
            topic: topic.into(),
            exchange,
            request_type_hash,
            response_type_hash,
            revision,
            lease_expires_at_ms: default_lease_deadline_ms(),
            machine_name: machine_name.map(Into::into),
        }
    }

    pub fn lease_expires_at_ms(mut self, lease_expires_at_ms: u64) -> Self {
        self.lease_expires_at_ms = lease_expires_at_ms;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TopicRecordBody {
    protocol_version: u16,
    topic: String,
    exchange: ExchangeKind,
    request_type_hash: u64,
    response_type_hash: Option<u64>,
    owner: [u8; 32],
    revision: u64,
    lease_expires_at_ms: u64,
    machine_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicEntry {
    body: TopicRecordBody,
    signature: Vec<u8>,
}

impl TopicEntry {
    pub fn signed(spec: TopicRecordSpec, secret: &SecretKey) -> Result<Self> {
        let body = TopicRecordBody {
            protocol_version: DIRECTORY_PROTOCOL_VERSION,
            topic: spec.topic,
            exchange: spec.exchange,
            request_type_hash: spec.request_type_hash,
            response_type_hash: spec.response_type_hash,
            owner: *secret.public().as_bytes(),
            revision: spec.revision,
            lease_expires_at_ms: spec.lease_expires_at_ms,
            machine_name: spec.machine_name,
        };
        let signature = sign_ed25519_detached(&postcard::to_allocvec(&body)?, &secret.to_bytes())
            .map_err(|error| Error::ControlPlane(error.to_string()))?;
        Ok(Self { body, signature })
    }

    pub fn verify(&self, now_ms: u64) -> Result<()> {
        if self.body.protocol_version != DIRECTORY_PROTOCOL_VERSION {
            return Err(Error::UnsupportedProtocol(self.body.protocol_version));
        }
        if self.body.lease_expires_at_ms <= now_ms {
            return Err(Error::ExpiredRecord {
                topic: self.body.topic.clone(),
            });
        }
        let valid = verify_ed25519_signature(
            &postcard::to_allocvec(&self.body)?,
            &self.signature,
            &self.body.owner,
        )
        .map_err(|error| Error::ControlPlane(error.to_string()))?;
        if !valid {
            return Err(Error::InvalidSignature {
                topic: self.body.topic.clone(),
            });
        }
        Ok(())
    }

    pub fn validate_exchange(
        &self,
        exchange: ExchangeKind,
        request_type_hash: u64,
        response_type_hash: Option<u64>,
    ) -> Result<()> {
        if self.body.exchange != exchange {
            return Err(Error::ExchangeMismatch {
                topic: self.body.topic.clone(),
                expected: exchange,
                actual: self.body.exchange,
            });
        }
        if self.body.request_type_hash != request_type_hash
            || self.body.response_type_hash != response_type_hash
        {
            return Err(Error::TypeMismatch {
                topic: self.body.topic.clone(),
            });
        }
        Ok(())
    }

    pub fn topic(&self) -> &str {
        &self.body.topic
    }

    pub fn exchange(&self) -> ExchangeKind {
        self.body.exchange
    }

    pub fn request_type_hash(&self) -> u64 {
        self.body.request_type_hash
    }

    pub fn response_type_hash(&self) -> Option<u64> {
        self.body.response_type_hash
    }

    pub fn endpoint_id(&self) -> EndpointId {
        EndpointId::from_bytes(&self.body.owner).expect("verified endpoint id bytes")
    }

    pub fn revision(&self) -> u64 {
        self.body.revision
    }

    pub fn lease_expires_at_ms(&self) -> u64 {
        self.body.lease_expires_at_ms
    }

    pub fn machine_name(&self) -> Option<&str> {
        self.body.machine_name.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn signature_mut(&mut self) -> &mut Vec<u8> {
        &mut self.signature
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WithdrawalBody {
    protocol_version: u16,
    topic: String,
    exchange: ExchangeKind,
    owner: [u8; 32],
    revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicWithdrawal {
    body: WithdrawalBody,
    signature: Vec<u8>,
}

impl TopicWithdrawal {
    pub fn signed(entry: &TopicEntry, revision: u64, secret: &SecretKey) -> Result<Self> {
        if secret.public() != entry.endpoint_id() {
            return Err(Error::OwnershipConflict {
                topic: entry.topic().to_string(),
            });
        }
        let body = WithdrawalBody {
            protocol_version: DIRECTORY_PROTOCOL_VERSION,
            topic: entry.topic().to_string(),
            exchange: entry.exchange(),
            owner: *secret.public().as_bytes(),
            revision,
        };
        let signature = sign_ed25519_detached(&postcard::to_allocvec(&body)?, &secret.to_bytes())
            .map_err(|error| Error::ControlPlane(error.to_string()))?;
        Ok(Self { body, signature })
    }

    pub fn verify(&self) -> Result<()> {
        if self.body.protocol_version != DIRECTORY_PROTOCOL_VERSION {
            return Err(Error::UnsupportedProtocol(self.body.protocol_version));
        }
        let valid = verify_ed25519_signature(
            &postcard::to_allocvec(&self.body)?,
            &self.signature,
            &self.body.owner,
        )
        .map_err(|error| Error::ControlPlane(error.to_string()))?;
        if !valid {
            return Err(Error::InvalidSignature {
                topic: self.body.topic.clone(),
            });
        }
        Ok(())
    }

    pub fn topic(&self) -> &str {
        &self.body.topic
    }

    pub fn exchange(&self) -> ExchangeKind {
        self.body.exchange
    }

    pub fn endpoint_id(&self) -> EndpointId {
        EndpointId::from_bytes(&self.body.owner).expect("verified endpoint id bytes")
    }

    pub fn revision(&self) -> u64 {
        self.body.revision
    }
}

pub fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub fn default_lease_deadline_ms() -> u64 {
    unix_time_ms().saturating_add(
        DEFAULT_LEASE_DURATION
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(secret: &SecretKey) -> TopicEntry {
        TopicEntry::signed(
            TopicRecordSpec::new(
                "/signed/topic",
                ExchangeKind::ReqRes,
                10,
                Some(20),
                1,
                Some("owner"),
            ),
            secret,
        )
        .unwrap()
    }

    #[test]
    fn signed_record_verifies() {
        record(&SecretKey::generate())
            .verify(unix_time_ms())
            .unwrap();
    }

    #[test]
    fn mutations_invalidate_signature() {
        let mut cases = Vec::new();

        let mut topic = record(&SecretKey::generate());
        topic.body.topic.push_str("/changed");
        cases.push(topic);

        let mut owner = record(&SecretKey::generate());
        owner.body.owner = *SecretKey::generate().public().as_bytes();
        cases.push(owner);

        let mut request_hash = record(&SecretKey::generate());
        request_hash.body.request_type_hash ^= 1;
        cases.push(request_hash);

        let mut response_hash = record(&SecretKey::generate());
        response_hash.body.response_type_hash = Some(21);
        cases.push(response_hash);

        let mut expiry = record(&SecretKey::generate());
        expiry.body.lease_expires_at_ms += 1;
        cases.push(expiry);

        let mut signature = record(&SecretKey::generate());
        signature.signature[0] ^= 1;
        cases.push(signature);

        for case in cases {
            assert!(matches!(
                case.verify(unix_time_ms()),
                Err(Error::InvalidSignature { .. })
            ));
        }
    }

    #[test]
    fn expired_record_is_rejected() {
        let secret = SecretKey::generate();
        let expired = TopicEntry::signed(
            TopicRecordSpec::new(
                "/expired",
                ExchangeKind::PubSub,
                10,
                None,
                1,
                None::<String>,
            )
            .lease_expires_at_ms(unix_time_ms().saturating_sub(1)),
            &secret,
        )
        .unwrap();
        assert!(matches!(
            expired.verify(unix_time_ms()),
            Err(Error::ExpiredRecord { .. })
        ));
    }

    #[test]
    fn exchange_and_type_mismatches_are_typed() {
        let record = record(&SecretKey::generate());
        assert!(matches!(
            record.validate_exchange(ExchangeKind::PubSub, 10, Some(20)),
            Err(Error::ExchangeMismatch { .. })
        ));
        assert!(matches!(
            record.validate_exchange(ExchangeKind::ReqRes, 11, Some(20)),
            Err(Error::TypeMismatch { .. })
        ));
        assert!(matches!(
            record.validate_exchange(ExchangeKind::ReqRes, 10, Some(21)),
            Err(Error::TypeMismatch { .. })
        ));
    }

    #[test]
    fn another_owner_cannot_sign_withdrawal() {
        let owner = SecretKey::generate();
        let record = record(&owner);
        let result = TopicWithdrawal::signed(&record, 2, &SecretKey::generate());
        assert!(matches!(result, Err(Error::OwnershipConflict { .. })));
    }
}
