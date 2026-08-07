use std::env;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use peerbus::SecretKey;

use crate::error::{Error, Result};
use super::endpoint_ext::endpoint_to_did_key;

/// Get the base directory for storing agent keys (`~/.local/share/agentio/keys`).
pub fn default_keys_dir() -> PathBuf {
    if let Ok(dir) = env::var("AGENTIO_KEYS_DIR") {
        return PathBuf::from(dir);
    }
    if let Ok(data_home) = env::var("XDG_DATA_HOME") {
        return PathBuf::from(data_home).join("agentio").join("keys");
    }
    if let Ok(home) = env::var("HOME") {
        return PathBuf::from(home).join(".local").join("share").join("agentio").join("keys");
    }
    PathBuf::from(".agentio_keys")
}

/// Source specification for initializing an Agent's ed25519 identity key.
#[derive(Debug, Clone)]
pub enum IdentitySource {
    /// Default persistent ephemeral key stored at `~/.local/share/agentio/keys/ephemeral.key`.
    Ephemeral,
    /// Pure random, non-persistent in-memory ed25519 key (useful for tests).
    Random,
    /// Persistent key for a named agent stored at `~/.local/share/agentio/keys/name/{name}.key`.
    Name(String),
    /// Persistent key for a specific DID string stored at `~/.local/share/agentio/keys/did/{did}.key`.
    DidKey(String),
    /// Deterministic key derived from a name string using BLAKE3 in RAM.
    DerivedName(String),
    /// Persistent key file stored at the explicit given path (mode `0600`).
    File(PathBuf),
    /// Explicit in-memory ed25519 `SecretKey`.
    Key(SecretKey),
}

impl Default for IdentitySource {
    fn default() -> Self {
        IdentitySource::Ephemeral
    }
}

impl From<SecretKey> for IdentitySource {
    fn from(key: SecretKey) -> Self {
        IdentitySource::Key(key)
    }
}

impl From<PathBuf> for IdentitySource {
    fn from(path: PathBuf) -> Self {
        IdentitySource::File(path)
    }
}

impl From<&Path> for IdentitySource {
    fn from(path: &Path) -> Self {
        IdentitySource::File(path.to_path_buf())
    }
}

/// Derive a deterministic `SecretKey` from a string (e.g. human-readable machine name) using BLAKE3.
pub fn derive_secret_from_name(name: &str) -> SecretKey {
    let hash = blake3::hash(name.as_bytes());
    SecretKey::from_bytes(hash.as_bytes())
}

/// Save a secret key under its canonical `did:key` string at `~/.local/share/agentio/keys/did/{did}.key`.
pub fn save_did_key(key: &SecretKey) -> Result<String> {
    let did = endpoint_to_did_key(&key.public())?;
    let safe_did = did.replace(':', "_");
    let key_path = default_keys_dir().join("did").join(format!("{safe_did}.key"));
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = key.to_bytes();

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&key_path)?;
        file.write_all(&bytes)?;
    }

    #[cfg(not(unix))]
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&key_path)?;
        file.write_all(&bytes)?;
    }

    Ok(did)
}

/// Load an ed25519 `SecretKey` from a file, or generate and persist a fresh key with `0600` permissions.
pub fn load_or_generate_key(path: impl AsRef<Path>) -> Result<SecretKey> {
    let path = path.as_ref();
    if path.exists() {
        let mut file = fs::File::open(path)?;
        let mut bytes = [0u8; 32];
        file.read_exact(&mut bytes)?;
        Ok(SecretKey::from_bytes(&bytes))
    } else {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let key = SecretKey::generate();
        let bytes = key.to_bytes();

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)?;
            file.write_all(&bytes)?;
        }

        #[cfg(not(unix))]
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            file.write_all(&bytes)?;
        }

        tracing::info!(path = %path.display(), "generated fresh machine secret key file (0600)");
        Ok(key)
    }
}

/// Resolve an `IdentitySource` into a peerbus `SecretKey`.
pub fn resolve_identity(source: &IdentitySource) -> Result<SecretKey> {
    match source {
        IdentitySource::Ephemeral => {
            let key_path = default_keys_dir().join("ephemeral.key");
            let key = load_or_generate_key(key_path)?;
            let _ = save_did_key(&key);
            Ok(key)
        }
        IdentitySource::Random => Ok(SecretKey::generate()),
        IdentitySource::Name(name) => {
            let safe_name = name.replace('/', "_");
            let key_path = default_keys_dir().join("name").join(format!("{safe_name}.key"));
            let key = load_or_generate_key(key_path)?;
            let _ = save_did_key(&key);
            Ok(key)
        }
        IdentitySource::DidKey(did_str) => {
            let safe_did = did_str.replace(':', "_");
            let key_path = default_keys_dir().join("did").join(format!("{safe_did}.key"));
            if key_path.exists() {
                let key = load_or_generate_key(&key_path)?;
                let derived_did = endpoint_to_did_key(&key.public())?;
                if derived_did != *did_str {
                    return Err(Error::Identity(format!(
                        "key at {} does not match requested DID '{}' (got '{}')",
                        key_path.display(),
                        did_str,
                        derived_did
                    )));
                }
                Ok(key)
            } else {
                Err(Error::Identity(format!(
                    "no secret key found for DID '{did_str}' at {}",
                    key_path.display()
                )))
            }
        }
        IdentitySource::File(path) => {
            let key = load_or_generate_key(path)?;
            let _ = save_did_key(&key);
            Ok(key)
        }
        IdentitySource::DerivedName(name) => Ok(derive_secret_from_name(name)),
        IdentitySource::Key(key) => Ok(key.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_secret_from_name() {
        let key1 = derive_secret_from_name("r2d2-head");
        let key2 = derive_secret_from_name("r2d2-head");
        let key3 = derive_secret_from_name("r2d2-base");

        assert_eq!(key1.public(), key2.public());
        assert_ne!(key1.public(), key3.public());
    }

    #[test]
    fn test_load_or_generate_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let key_path = temp_dir.path().join("machine.key");

        let key1 = load_or_generate_key(&key_path).unwrap();
        assert!(key_path.exists());

        let key2 = load_or_generate_key(&key_path).unwrap();
        assert_eq!(key1.public(), key2.public());
    }

    #[test]
    fn test_did_identity_resolution() {
        let temp_dir = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("AGENTIO_KEYS_DIR", temp_dir.path());
        }

        let key = SecretKey::generate();
        let did = save_did_key(&key).unwrap();

        let resolved = resolve_identity(&IdentitySource::DidKey(did.clone())).unwrap();
        assert_eq!(key.public(), resolved.public());

        unsafe {
            std::env::remove_var("AGENTIO_KEYS_DIR");
        }
    }
}
