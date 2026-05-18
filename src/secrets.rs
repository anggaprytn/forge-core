use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::storage::atomic_write;

const MASTER_KEY_ENV: &str = "FORGE_MASTER_KEY";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretWriteRequest {
    pub project_id: String,
    pub environment: String,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretWriteResult {
    pub secret_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretResolution {
    pub key: String,
    pub value: String,
    pub sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EncryptedSecretRecord {
    version: u8,
    nonce_b64: String,
    ciphertext_b64: String,
}

#[derive(Debug)]
pub enum SecretError {
    MissingMasterKey,
    InvalidMasterKey,
    Io(std::io::Error),
    Crypto(String),
    MissingSecret(String),
    InvalidRequest(String),
}

impl Display for SecretError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingMasterKey => write!(f, "FORGE_MASTER_KEY is required"),
            Self::InvalidMasterKey => write!(f, "FORGE_MASTER_KEY must be 64 hex characters"),
            Self::Io(err) => write!(f, "{err}"),
            Self::Crypto(err) => write!(f, "{err}"),
            Self::MissingSecret(key) => write!(f, "missing required secret {key}"),
            Self::InvalidRequest(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for SecretError {}

impl From<std::io::Error> for SecretError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone)]
pub struct SecretStore {
    root: PathBuf,
}

impl SecretStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, SecretError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn write_environment_secret(
        &self,
        request: &SecretWriteRequest,
    ) -> Result<SecretWriteResult, SecretError> {
        validate_secret_request(request)?;
        let cipher = cipher_from_env()?;
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce), request.value.as_bytes())
            .map_err(|err| SecretError::Crypto(err.to_string()))?;
        let record = EncryptedSecretRecord {
            version: 1,
            nonce_b64: base64::engine::general_purpose::STANDARD.encode(nonce),
            ciphertext_b64: base64::engine::general_purpose::STANDARD.encode(ciphertext),
        };
        let bytes = serde_json::to_vec_pretty(&record)
            .map_err(|err| SecretError::Crypto(err.to_string()))?;
        atomic_write(self.path_for(&request.project_id, &request.environment, &request.key), &bytes)
            .map_err(|err| SecretError::Io(std::io::Error::other(err.to_string())))?;
        Ok(SecretWriteResult {
            secret_id: format!("{}:{}:{}", request.project_id, request.environment, request.key),
        })
    }

    pub fn read_environment_secret(
        &self,
        project_id: &str,
        environment: &str,
        key: &str,
    ) -> Result<String, SecretError> {
        let path = self.path_for(project_id, environment, key);
        if !path.exists() {
            return Err(SecretError::MissingSecret(key.to_string()));
        }
        let cipher = cipher_from_env()?;
        let raw = fs::read_to_string(path)?;
        let record: EncryptedSecretRecord =
            serde_json::from_str(&raw).map_err(|err| SecretError::Crypto(err.to_string()))?;
        let nonce = base64::engine::general_purpose::STANDARD
            .decode(record.nonce_b64)
            .map_err(|err| SecretError::Crypto(err.to_string()))?;
        let ciphertext = base64::engine::general_purpose::STANDARD
            .decode(record.ciphertext_b64)
            .map_err(|err| SecretError::Crypto(err.to_string()))?;
        let plaintext = cipher
            .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
            .map_err(|err| SecretError::Crypto(err.to_string()))?;
        String::from_utf8(plaintext).map_err(|err| SecretError::Crypto(err.to_string()))
    }

    fn path_for(&self, project_id: &str, environment: &str, key: &str) -> PathBuf {
        self.root
            .join(project_id)
            .join(environment)
            .join(format!("{key}.json"))
    }
}

fn validate_secret_request(request: &SecretWriteRequest) -> Result<(), SecretError> {
    if request.project_id.trim().is_empty() {
        return Err(SecretError::InvalidRequest(
            "project_id must not be empty".into(),
        ));
    }
    if request.key.trim().is_empty() {
        return Err(SecretError::InvalidRequest("key must not be empty".into()));
    }
    if request.value.is_empty() {
        return Err(SecretError::InvalidRequest("value must not be empty".into()));
    }
    Ok(())
}

fn cipher_from_env() -> Result<Aes256Gcm, SecretError> {
    let raw = std::env::var(MASTER_KEY_ENV).map_err(|_| SecretError::MissingMasterKey)?;
    let bytes = hex::decode(raw).map_err(|_| SecretError::InvalidMasterKey)?;
    if bytes.len() != 32 {
        return Err(SecretError::InvalidMasterKey);
    }
    Aes256Gcm::new_from_slice(&bytes).map_err(|_| SecretError::InvalidMasterKey)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn environment_secret_round_trip_is_encrypted_at_rest() {
        unsafe {
            std::env::set_var(
                MASTER_KEY_ENV,
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        let root = test_root("secret-round-trip");
        let store = SecretStore::new(root.join("secrets")).unwrap();
        let request = SecretWriteRequest {
            project_id: "api".into(),
            environment: "production".into(),
            key: "DATABASE_URL".into(),
            value: "postgres://secret-value".into(),
        };

        let result = store.write_environment_secret(&request).unwrap();
        assert_eq!(result.secret_id, "api:production:DATABASE_URL");

        let raw = fs::read_to_string(root.join("secrets/api/production/DATABASE_URL.json")).unwrap();
        assert!(!raw.contains("postgres://secret-value"));
        assert_eq!(
            store
                .read_environment_secret("api", "production", "DATABASE_URL")
                .unwrap(),
            "postgres://secret-value"
        );
    }

    fn test_root(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let base = std::env::temp_dir().join(format!(
            "forge-core-tests-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }
}
