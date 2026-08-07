pub use authbox::did;
pub mod source;

use crate::error::{Error, Result};
use peerbus::EndpointId;

/// Extension helpers for converting between peerbus `EndpointId` and `authbox::did`.
pub mod endpoint_ext {
    use super::*;

    /// Convert a peerbus `EndpointId` to its canonical `did:key` string representation using `authbox`.
    pub fn endpoint_to_did_key(endpoint_id: &EndpointId) -> Result<String> {
        let pubkey_bytes = *endpoint_id.as_bytes();
        authbox::did::encode_ed25519_did_key(pubkey_bytes).map_err(|e| Error::DidKey(e.to_string()))
    }

    /// Parse a `did:key` or hex string into a peerbus `EndpointId` using `authbox`.
    pub fn did_key_to_endpoint(did: &str) -> Result<EndpointId> {
        let s = did.trim();
        if let Ok(id) = s.parse::<EndpointId>() {
            return Ok(id);
        }
        let parsed = authbox::did::parse_did_key(s).map_err(|e| Error::DidKey(e.to_string()))?;
        let pubkey_bytes: [u8; 32] = parsed
            .public_key
            .as_slice()
            .try_into()
            .map_err(|_| Error::DidKey("invalid public key length".to_string()))?;
        let endpoint_id =
            EndpointId::from_bytes(&pubkey_bytes).map_err(|e| Error::DidKey(e.to_string()))?;
        Ok(endpoint_id)
    }

    /// Resolve the full W3C DID Document JSON for an `EndpointId` using `authbox`.
    pub fn endpoint_to_did_document_json(endpoint_id: &EndpointId) -> Result<String> {
        let did = endpoint_to_did_key(endpoint_id)?;
        authbox::did::resolve_did_key_document_json(&did).map_err(|e| Error::DidKey(e.to_string()))
    }
}

pub use endpoint_ext as did_key;
pub use source::{
    IdentitySource, default_keys_dir, load_or_generate_key, resolve_identity, save_did_key,
};

#[cfg(test)]
mod tests {
    use super::*;
    use peerbus::SecretKey;

    #[test]
    fn test_did_key_roundtrip() {
        let key = SecretKey::generate();
        let endpoint_id = key.public();
        let did = did_key::endpoint_to_did_key(&endpoint_id).unwrap();
        assert!(did.starts_with("did:key:z"));

        let recovered_endpoint = did_key::did_key_to_endpoint(&did).unwrap();
        assert_eq!(endpoint_id, recovered_endpoint);

        let doc_json = did_key::endpoint_to_did_document_json(&endpoint_id).unwrap();
        assert!(doc_json.contains(&did));
    }
}
