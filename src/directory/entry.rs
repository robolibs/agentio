use authbox::pki::{sign_ed25519_detached, verify_ed25519_signature};
use peerbus::{EndpointId, SecretKey};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};
use crate::naming::normalize_topic;

/// Current signed directory wire version.
pub const DIRECTORY_PROTOCOL_VERSION: u16 = 1;
/// Default lifetime of a hosted directory record.
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum DirectoryOperation {
    Announce,
    Withdraw,
}

/// The peerbus exchange family hosted at a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeKind {
    /// Publish/subscribe stream.
    PubSub,
    /// Request/response call.
    ReqRes,
    /// Query/answer stream.
    QueAns,
    /// Upload/acknowledgement stream.
    PutAck,
    /// Bidirectional pipe.
    Pip,
}

/// Input used to create and sign a current directory record.
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
    /// Create unsigned record input using the default lease duration.
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

    /// Override the absolute Unix-millisecond lease deadline.
    pub fn lease_expires_at_ms(mut self, lease_expires_at_ms: u64) -> Self {
        self.lease_expires_at_ms = lease_expires_at_ms;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TopicRecordBody {
    protocol_version: u16,
    operation: DirectoryOperation,
    topic: String,
    exchange: ExchangeKind,
    request_type_hash: u64,
    response_type_hash: Option<u64>,
    owner: [u8; 32],
    revision: u64,
    lease_expires_at_ms: u64,
    machine_name: Option<String>,
}

/// An owner-signed, leased topic record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicEntry {
    body: TopicRecordBody,
    signature: Vec<u8>,
}

impl TopicEntry {
    /// Create an owner-signed topic entry from normalized record input.
    pub fn signed(spec: TopicRecordSpec, secret: &SecretKey) -> Result<Self> {
        let normalized = normalize_topic(&spec.topic)?;
        if normalized != spec.topic {
            return Err(Error::InvalidTopic {
                topic: spec.topic,
                reason: "directory records require a normalized absolute topic".to_string(),
            });
        }
        let body = TopicRecordBody {
            protocol_version: DIRECTORY_PROTOCOL_VERSION,
            operation: DirectoryOperation::Announce,
            topic: normalized,
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

    /// Verify protocol version, lease validity, and owner signature.
    pub fn verify(&self, now_ms: u64) -> Result<()> {
        if self.body.protocol_version != DIRECTORY_PROTOCOL_VERSION {
            return Err(Error::UnsupportedProtocol(self.body.protocol_version));
        }
        if self.body.operation != DirectoryOperation::Announce {
            return Err(Error::InvalidDirectoryOperation {
                topic: self.body.topic.clone(),
            });
        }
        if normalize_topic(&self.body.topic)? != self.body.topic {
            return Err(Error::InvalidTopic {
                topic: self.body.topic.clone(),
                reason: "directory records require a normalized absolute topic".to_string(),
            });
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

    /// Verify an exchange family and its request and response wire hashes.
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
        // A record adopted from a raw peerbus topic carries no response
        // type; peerbus still checks it on the wire.
        let response_known = self.body.response_type_hash.is_some();
        if self.body.request_type_hash != request_type_hash
            || (response_known && self.body.response_type_hash != response_type_hash)
        {
            return Err(Error::TypeMismatch {
                topic: self.body.topic.clone(),
            });
        }
        Ok(())
    }

    /// Return the normalized topic.
    pub fn topic(&self) -> &str {
        &self.body.topic
    }

    /// Return the hosted exchange family.
    pub fn exchange(&self) -> ExchangeKind {
        self.body.exchange
    }

    /// Return the request or payload wire-type hash.
    pub fn request_type_hash(&self) -> u64 {
        self.body.request_type_hash
    }

    /// Return the response wire-type hash for bidirectional exchanges.
    pub fn response_type_hash(&self) -> Option<u64> {
        self.body.response_type_hash
    }

    /// Return the signing owner's endpoint identifier.
    pub fn endpoint_id(&self) -> EndpointId {
        EndpointId::from_bytes(&self.body.owner).expect("verified endpoint id bytes")
    }

    /// Return the owner-scoped monotonic revision.
    pub fn revision(&self) -> u64 {
        self.body.revision
    }

    /// Return the absolute Unix-millisecond lease deadline.
    pub fn lease_expires_at_ms(&self) -> u64 {
        self.body.lease_expires_at_ms
    }

    /// Return the optional machine name carried by this record.
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
    operation: DirectoryOperation,
    topic: String,
    exchange: ExchangeKind,
    owner: [u8; 32],
    revision: u64,
    target_revision: u64,
    target_signature: Vec<u8>,
}

/// An owner-signed request to remove a hosted topic record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicWithdrawal {
    body: WithdrawalBody,
    signature: Vec<u8>,
}

impl TopicWithdrawal {
    /// Sign a withdrawal bound to one exact current topic entry.
    pub fn signed(entry: &TopicEntry, revision: u64, secret: &SecretKey) -> Result<Self> {
        if secret.public() != entry.endpoint_id() {
            return Err(Error::OwnershipConflict {
                topic: entry.topic().to_string(),
            });
        }
        let body = WithdrawalBody {
            protocol_version: DIRECTORY_PROTOCOL_VERSION,
            operation: DirectoryOperation::Withdraw,
            topic: entry.topic().to_string(),
            exchange: entry.exchange(),
            owner: *secret.public().as_bytes(),
            revision,
            target_revision: entry.revision(),
            target_signature: entry.signature.clone(),
        };
        let signature = sign_ed25519_detached(&postcard::to_allocvec(&body)?, &secret.to_bytes())
            .map_err(|error| Error::ControlPlane(error.to_string()))?;
        Ok(Self { body, signature })
    }

    /// Verify the withdrawal protocol version and owner signature.
    pub fn verify(&self) -> Result<()> {
        if self.body.protocol_version != DIRECTORY_PROTOCOL_VERSION {
            return Err(Error::UnsupportedProtocol(self.body.protocol_version));
        }
        if self.body.operation != DirectoryOperation::Withdraw {
            return Err(Error::InvalidDirectoryOperation {
                topic: self.body.topic.clone(),
            });
        }
        if normalize_topic(&self.body.topic)? != self.body.topic {
            return Err(Error::InvalidTopic {
                topic: self.body.topic.clone(),
                reason: "directory withdrawals require a normalized absolute topic".to_string(),
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

    /// Return the normalized topic to withdraw.
    pub fn topic(&self) -> &str {
        &self.body.topic
    }

    /// Return the exchange family to withdraw.
    pub fn exchange(&self) -> ExchangeKind {
        self.body.exchange
    }

    /// Return the signing owner's endpoint identifier.
    pub fn endpoint_id(&self) -> EndpointId {
        EndpointId::from_bytes(&self.body.owner).expect("verified endpoint id bytes")
    }

    /// Return the withdrawal revision.
    pub fn revision(&self) -> u64 {
        self.body.revision
    }

    pub(crate) fn targets(&self, entry: &TopicEntry) -> bool {
        self.body.target_revision == entry.revision()
            && self.body.target_signature == entry.signature
    }
}

/// Return the current Unix time in milliseconds.
pub fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Return a default lease deadline relative to the current time.
pub fn default_lease_deadline_ms() -> u64 {
    unix_time_ms().saturating_add(
        DEFAULT_LEASE_DURATION
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    )
}

pub(crate) fn next_revision_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX.saturating_sub(1_000_000))
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

    #[test]
    fn signed_records_require_normalized_topics() {
        let result = TopicEntry::signed(
            TopicRecordSpec::new(
                "relative/topic",
                ExchangeKind::PubSub,
                1,
                None,
                1,
                None::<String>,
            ),
            &SecretKey::generate(),
        );
        assert!(matches!(result, Err(Error::InvalidTopic { .. })));
    }

    #[test]
    fn verification_enforces_topic_and_operation_semantics() {
        let secret = SecretKey::generate();
        let mut non_normalized = record(&secret);
        non_normalized.body.topic = "relative/topic".to_string();
        non_normalized.signature = sign_ed25519_detached(
            &postcard::to_allocvec(&non_normalized.body).unwrap(),
            &secret.to_bytes(),
        )
        .unwrap();
        assert!(matches!(
            non_normalized.verify(unix_time_ms()),
            Err(Error::InvalidTopic { .. })
        ));

        let current = record(&secret);
        let mut wrong_record_operation = current.clone();
        wrong_record_operation.body.operation = DirectoryOperation::Withdraw;
        wrong_record_operation.signature = sign_ed25519_detached(
            &postcard::to_allocvec(&wrong_record_operation.body).unwrap(),
            &secret.to_bytes(),
        )
        .unwrap();
        assert!(matches!(
            wrong_record_operation.verify(unix_time_ms()),
            Err(Error::InvalidDirectoryOperation { .. })
        ));

        let mut withdrawal = TopicWithdrawal::signed(&current, 2, &secret).unwrap();
        withdrawal.body.operation = DirectoryOperation::Announce;
        withdrawal.signature = sign_ed25519_detached(
            &postcard::to_allocvec(&withdrawal.body).unwrap(),
            &secret.to_bytes(),
        )
        .unwrap();
        assert!(matches!(
            withdrawal.verify(),
            Err(Error::InvalidDirectoryOperation { .. })
        ));
    }

    #[test]
    fn withdrawal_signature_is_domain_separated() {
        let secret = SecretKey::generate();
        let record = record(&secret);
        let withdrawal = TopicWithdrawal::signed(&record, 2, &secret).unwrap();
        assert_eq!(record.body.operation, DirectoryOperation::Announce);
        assert_eq!(withdrawal.body.operation, DirectoryOperation::Withdraw);
        assert_ne!(
            postcard::to_allocvec(&record.body).unwrap(),
            postcard::to_allocvec(&withdrawal.body).unwrap()
        );
    }
}
