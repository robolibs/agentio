use peerbus::SecretKey;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use super::endpoint_ext::endpoint_to_did_key;
use crate::error::{Error, Result};

/// Get the base directory for storing agent keys (`~/.local/share/agentio/keys`).
pub fn default_keys_dir() -> PathBuf {
    if let Ok(dir) = env::var("AGENTIO_KEYS_DIR") {
        return PathBuf::from(dir);
    }
    if let Ok(data_home) = env::var("XDG_DATA_HOME") {
        return PathBuf::from(data_home).join("agentio").join("keys");
    }
    if let Ok(home) = env::var("HOME") {
        return PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("agentio")
            .join("keys");
    }
    PathBuf::from(".agentio_keys")
}

/// Source specification for initializing an Agent's ed25519 identity key.
#[derive(Debug, Clone, Default)]
pub enum IdentitySource {
    /// Default persistent ephemeral key stored at `~/.local/share/agentio/keys/ephemeral.key`.
    #[default]
    Ephemeral,
    /// Pure random, non-persistent in-memory ed25519 key (useful for tests).
    Random,
    /// Persistent key for a named agent stored at `~/.local/share/agentio/keys/name/{name}.key`.
    Name(String),
    /// Persistent key for a specific DID string stored at `~/.local/share/agentio/keys/did/{did}.key`.
    DidKey(String),
    /// Persistent key file stored at the explicit given path (mode `0600`).
    File(PathBuf),
    /// Explicit in-memory ed25519 `SecretKey`.
    Key(SecretKey),
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

/// Save a secret key under its canonical `did:key` string at `~/.local/share/agentio/keys/did/{did}.key`.
pub fn save_did_key(key: &SecretKey) -> Result<String> {
    let did = endpoint_to_did_key(&key.public())?;
    let safe_did = did.replace(':', "_");
    let key_path = default_keys_dir()
        .join("did")
        .join(format!("{safe_did}.key"));
    if key_path.exists() {
        let existing = load_existing_key(&key_path)?;
        if existing.public() != key.public() {
            return Err(Error::Identity(format!(
                "DID index key at {} does not match the requested identity",
                key_path.display()
            )));
        }
        return Ok(did);
    }
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = key.to_bytes();

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&key_path)?;
        file.write_all(&bytes)?;
    }

    #[cfg(not(unix))]
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&key_path)?;
        file.write_all(&bytes)?;
    }

    Ok(did)
}

/// Load an ed25519 `SecretKey` from a file, or generate and persist a fresh key with `0600` permissions.
pub fn load_or_generate_key(path: impl AsRef<Path>) -> Result<SecretKey> {
    let path = path.as_ref();
    if path.exists() {
        load_existing_key(path)
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
            let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
            file.write_all(&bytes)?;
        }

        tracing::info!(path = %path.display(), "generated fresh machine secret key file (0600)");
        Ok(key)
    }
}

fn load_existing_key(path: &Path) -> Result<SecretKey> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(path)?.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(Error::Identity(format!(
                "key file {} has insecure permissions {mode:o}; expected 600 or stricter",
                path.display()
            )));
        }
    }

    let mut file = fs::File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let key_bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        Error::Identity(format!(
            "key file {} contains {} bytes; expected 32",
            path.display(),
            bytes.len()
        ))
    })?;
    Ok(SecretKey::from_bytes(&key_bytes))
}

/// Resolve an `IdentitySource` into a peerbus `SecretKey`.
pub fn resolve_identity(source: &IdentitySource) -> Result<SecretKey> {
    match source {
        IdentitySource::Ephemeral => {
            let key_path = default_keys_dir().join("ephemeral.key");
            let key = load_or_generate_key(key_path)?;
            save_did_key(&key)?;
            Ok(key)
        }
        IdentitySource::Random => Ok(SecretKey::generate()),
        IdentitySource::Name(name) => {
            let safe_name = name.replace('/', "_");
            let key_path = default_keys_dir()
                .join("name")
                .join(format!("{safe_name}.key"));
            let key = load_or_generate_key(key_path)?;
            save_did_key(&key)?;
            Ok(key)
        }
        IdentitySource::DidKey(did_str) => {
            let safe_did = did_str.replace(':', "_");
            let key_path = default_keys_dir()
                .join("did")
                .join(format!("{safe_did}.key"));
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
            save_did_key(&key)?;
            Ok(key)
        }
        IdentitySource::Key(key) => Ok(key.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct KeysDirGuard {
        previous: Option<std::ffi::OsString>,
        _lock: MutexGuard<'static, ()>,
    }

    impl KeysDirGuard {
        fn set(path: &Path) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
            let previous = std::env::var_os("AGENTIO_KEYS_DIR");
            unsafe {
                std::env::set_var("AGENTIO_KEYS_DIR", path);
            }
            Self {
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for KeysDirGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var("AGENTIO_KEYS_DIR", value),
                    None => std::env::remove_var("AGENTIO_KEYS_DIR"),
                }
            }
        }
    }

    #[test]
    fn random_identities_differ() {
        let first = resolve_identity(&IdentitySource::Random).unwrap();
        let second = resolve_identity(&IdentitySource::Random).unwrap();
        assert_ne!(first.public(), second.public());
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
        let _guard = KeysDirGuard::set(temp_dir.path());

        let key = SecretKey::generate();
        let did = save_did_key(&key).unwrap();

        let resolved = resolve_identity(&IdentitySource::DidKey(did.clone())).unwrap();
        assert_eq!(key.public(), resolved.public());
    }

    #[test]
    fn named_identity_reloads_from_disk() {
        let temp_dir = tempfile::tempdir().unwrap();
        let _guard = KeysDirGuard::set(temp_dir.path());
        let source = IdentitySource::Name("persistent".to_string());
        let first = resolve_identity(&source).unwrap();
        let second = resolve_identity(&source).unwrap();
        assert_eq!(first.public(), second.public());
    }

    #[test]
    fn invalid_key_length_is_rejected() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("bad.key");
        fs::write(&path, [0u8; 31]).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert!(matches!(
            load_or_generate_key(path),
            Err(Error::Identity(_))
        ));
    }

    #[test]
    fn did_index_write_failure_is_visible() {
        let temp_dir = tempfile::tempdir().unwrap();
        let not_a_directory = temp_dir.path().join("file");
        fs::write(&not_a_directory, b"occupied").unwrap();
        let _guard = KeysDirGuard::set(&not_a_directory);
        assert!(resolve_identity(&IdentitySource::Name("agent".to_string())).is_err());
    }
}
