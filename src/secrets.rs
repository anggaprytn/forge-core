use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::api::{SecretListEntry, SecretListResponse, SecretUnsetResponse};
use crate::storage::atomic_write;

const MASTER_KEY_ENV: &str = "FORGE_MASTER_KEY";
const SECRET_REDACTION: &str = "<secret>";

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
    pub source_key: String,
    pub value: String,
    pub sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedValueRecord {
    pub version: u8,
    pub nonce_b64: String,
    pub ciphertext_b64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretMutationRecord {
    pub action: String,
    pub timestamp_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct SecretLifecycleMetadata {
    pub key: String,
    #[serde(default)]
    pub created_at_unix: Option<u64>,
    #[serde(default)]
    pub updated_at_unix: Option<u64>,
    #[serde(default)]
    pub referenced_by_generations: Vec<u64>,
    #[serde(default)]
    pub mutations: Vec<SecretMutationRecord>,
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
        let record = seal_with_cipher(&cipher, &request.value)?;
        let bytes = serde_json::to_vec_pretty(&record)
            .map_err(|err| SecretError::Crypto(err.to_string()))?;
        atomic_write(
            self.path_for(&request.project_id, &request.environment, &request.key),
            &bytes,
        )
        .map_err(|err| SecretError::Io(std::io::Error::other(err.to_string())))?;
        self.update_metadata_on_set(&request.project_id, &request.environment, &request.key)?;
        Ok(SecretWriteResult {
            secret_id: format!(
                "{}:{}:{}",
                request.project_id, request.environment, request.key
            ),
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
        let raw = fs::read_to_string(path)?;
        let record: SealedValueRecord =
            serde_json::from_str(&raw).map_err(|err| SecretError::Crypto(err.to_string()))?;
        unseal_value(&record)
    }

    pub fn list_environment_secrets(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<SecretListResponse, SecretError> {
        validate_secret_scope(project_id, environment)?;
        let dir = self.root.join(project_id).join(environment);
        let mut secrets = Vec::new();
        if dir.exists() {
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                let path = entry.path();
                if !entry.file_type()?.is_file() {
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                    continue;
                }
                let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                    continue;
                };
                if stem.ends_with(".meta") {
                    continue;
                }
                let metadata = self
                    .read_metadata(project_id, environment, stem)?
                    .unwrap_or_else(|| fallback_metadata(stem, &path));
                secrets.push(SecretListEntry {
                    key: stem.to_string(),
                    value: SECRET_REDACTION.into(),
                    created_at_unix: metadata.created_at_unix.unwrap_or_default(),
                    updated_at_unix: metadata
                        .updated_at_unix
                        .or(metadata.created_at_unix)
                        .unwrap_or_default(),
                    referenced_by_generations: metadata.referenced_by_generations,
                });
            }
        }
        secrets.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(SecretListResponse {
            project_id: project_id.to_string(),
            environment: environment.to_string(),
            secrets,
        })
    }

    pub fn unset_environment_secret(
        &self,
        project_id: &str,
        environment: &str,
        key: &str,
    ) -> Result<SecretUnsetResponse, SecretError> {
        validate_secret_scope(project_id, environment)?;
        if key.trim().is_empty() {
            return Err(SecretError::InvalidRequest("key must not be empty".into()));
        }
        let path = self.path_for(project_id, environment, key);
        if !path.exists() {
            return Err(SecretError::MissingSecret(key.to_string()));
        }
        fs::remove_file(path)?;
        self.update_metadata_on_unset(project_id, environment, key)?;
        Ok(SecretUnsetResponse {
            secret_id: format!("{project_id}:{environment}:{key}"),
            removed: true,
        })
    }

    pub fn record_generation_references(
        &self,
        project_id: &str,
        environment: &str,
        generation: u64,
        keys: &[String],
    ) -> Result<(), SecretError> {
        validate_secret_scope(project_id, environment)?;
        for key in keys {
            let mut metadata = self
                .read_metadata(project_id, environment, key)?
                .unwrap_or_else(|| SecretLifecycleMetadata {
                    key: key.clone(),
                    ..Default::default()
                });
            if metadata.created_at_unix.is_none() {
                metadata.created_at_unix = Some(current_unix_timestamp());
            }
            if !metadata.referenced_by_generations.contains(&generation) {
                metadata.referenced_by_generations.push(generation);
                metadata.referenced_by_generations.sort_unstable();
            }
            self.write_metadata(project_id, environment, key, &metadata)?;
        }
        Ok(())
    }

    pub fn current_secret_value(
        &self,
        project_id: &str,
        environment: &str,
        key: &str,
    ) -> Result<Option<String>, SecretError> {
        match self.read_environment_secret(project_id, environment, key) {
            Ok(value) => Ok(Some(value)),
            Err(SecretError::MissingSecret(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn metadata_for_secret(
        &self,
        project_id: &str,
        environment: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<SecretMutationRecord>)>, SecretError> {
        Ok(self
            .read_metadata(project_id, environment, key)?
            .and_then(|metadata| {
                metadata
                    .updated_at_unix
                    .map(|updated_at| (updated_at, metadata.mutations))
            }))
    }

    pub fn has_environment_secret(&self, project_id: &str, environment: &str, key: &str) -> bool {
        self.path_for(project_id, environment, key).exists()
    }

    fn path_for(&self, project_id: &str, environment: &str, key: &str) -> PathBuf {
        self.root
            .join(project_id)
            .join(environment)
            .join(format!("{key}.json"))
    }

    fn metadata_path_for(&self, project_id: &str, environment: &str, key: &str) -> PathBuf {
        self.root
            .join(project_id)
            .join(environment)
            .join(format!("{key}.meta.json"))
    }

    fn update_metadata_on_set(
        &self,
        project_id: &str,
        environment: &str,
        key: &str,
    ) -> Result<(), SecretError> {
        let now = current_unix_timestamp();
        let mut metadata = self
            .read_metadata(project_id, environment, key)?
            .unwrap_or_else(|| SecretLifecycleMetadata {
                key: key.to_string(),
                ..Default::default()
            });
        if metadata.created_at_unix.is_none() {
            metadata.created_at_unix = Some(now);
        }
        metadata.updated_at_unix = Some(now);
        metadata.mutations.push(SecretMutationRecord {
            action: "set".into(),
            timestamp_unix: now,
        });
        self.write_metadata(project_id, environment, key, &metadata)
    }

    fn update_metadata_on_unset(
        &self,
        project_id: &str,
        environment: &str,
        key: &str,
    ) -> Result<(), SecretError> {
        let now = current_unix_timestamp();
        let mut metadata = self
            .read_metadata(project_id, environment, key)?
            .unwrap_or_else(|| SecretLifecycleMetadata {
                key: key.to_string(),
                ..Default::default()
            });
        if metadata.created_at_unix.is_none() {
            metadata.created_at_unix = Some(now);
        }
        metadata.updated_at_unix = Some(now);
        metadata.mutations.push(SecretMutationRecord {
            action: "unset".into(),
            timestamp_unix: now,
        });
        self.write_metadata(project_id, environment, key, &metadata)
    }

    fn read_metadata(
        &self,
        project_id: &str,
        environment: &str,
        key: &str,
    ) -> Result<Option<SecretLifecycleMetadata>, SecretError> {
        let path = self.metadata_path_for(project_id, environment, key);
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw)
            .map(Some)
            .map_err(|err| SecretError::Crypto(err.to_string()))
    }

    fn write_metadata(
        &self,
        project_id: &str,
        environment: &str,
        key: &str,
        metadata: &SecretLifecycleMetadata,
    ) -> Result<(), SecretError> {
        let bytes = serde_json::to_vec_pretty(metadata)
            .map_err(|err| SecretError::Crypto(err.to_string()))?;
        atomic_write(self.metadata_path_for(project_id, environment, key), &bytes)
            .map_err(|err| SecretError::Io(std::io::Error::other(err.to_string())))
    }
}

fn validate_secret_request(request: &SecretWriteRequest) -> Result<(), SecretError> {
    validate_secret_scope(&request.project_id, &request.environment)?;
    if request.key.trim().is_empty() {
        return Err(SecretError::InvalidRequest("key must not be empty".into()));
    }
    if request.value.is_empty() {
        return Err(SecretError::InvalidRequest(
            "value must not be empty".into(),
        ));
    }
    Ok(())
}

fn validate_secret_scope(project_id: &str, environment: &str) -> Result<(), SecretError> {
    if project_id.trim().is_empty() {
        return Err(SecretError::InvalidRequest(
            "project_id must not be empty".into(),
        ));
    }
    if environment.trim().is_empty() {
        return Err(SecretError::InvalidRequest(
            "environment must not be empty".into(),
        ));
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

pub fn seal_value(value: &str) -> Result<SealedValueRecord, SecretError> {
    let cipher = cipher_from_env()?;
    seal_with_cipher(&cipher, value)
}

pub fn unseal_value(record: &SealedValueRecord) -> Result<String, SecretError> {
    let cipher = cipher_from_env()?;
    let nonce = base64::engine::general_purpose::STANDARD
        .decode(&record.nonce_b64)
        .map_err(|err| SecretError::Crypto(err.to_string()))?;
    let ciphertext = base64::engine::general_purpose::STANDARD
        .decode(&record.ciphertext_b64)
        .map_err(|err| SecretError::Crypto(err.to_string()))?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
        .map_err(|err| SecretError::Crypto(err.to_string()))?;
    String::from_utf8(plaintext).map_err(|err| SecretError::Crypto(err.to_string()))
}

fn seal_with_cipher(cipher: &Aes256Gcm, value: &str) -> Result<SealedValueRecord, SecretError> {
    let mut nonce = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), value.as_bytes())
        .map_err(|err| SecretError::Crypto(err.to_string()))?;
    Ok(SealedValueRecord {
        version: 1,
        nonce_b64: base64::engine::general_purpose::STANDARD.encode(nonce),
        ciphertext_b64: base64::engine::general_purpose::STANDARD.encode(ciphertext),
    })
}

fn current_unix_timestamp() -> u64 {
    crate::storage::current_unix_timestamp()
}

fn fallback_metadata(key: &str, path: &Path) -> SecretLifecycleMetadata {
    let timestamp = fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|value| value.as_secs())
        .unwrap_or_default();
    SecretLifecycleMetadata {
        key: key.to_string(),
        created_at_unix: Some(timestamp),
        updated_at_unix: Some(timestamp),
        referenced_by_generations: Vec::new(),
        mutations: Vec::new(),
    }
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

        let raw =
            fs::read_to_string(root.join("secrets/api/production/DATABASE_URL.json")).unwrap();
        assert!(!raw.contains("postgres://secret-value"));
        assert_eq!(
            store
                .read_environment_secret("api", "production", "DATABASE_URL")
                .unwrap(),
            "postgres://secret-value"
        );
    }

    #[test]
    fn secret_list_never_exposes_plaintext() {
        unsafe {
            std::env::set_var(
                MASTER_KEY_ENV,
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        let root = test_root("secret-list-redaction");
        let store = SecretStore::new(root.join("secrets")).unwrap();
        store
            .write_environment_secret(&SecretWriteRequest {
                project_id: "api".into(),
                environment: "production".into(),
                key: "DATABASE_URL".into(),
                value: "postgres://super-secret".into(),
            })
            .unwrap();

        let listed = store.list_environment_secrets("api", "production").unwrap();

        assert_eq!(listed.secrets.len(), 1);
        assert_eq!(listed.secrets[0].key, "DATABASE_URL");
        assert_eq!(listed.secrets[0].value, "<secret>");
        let rendered = serde_json::to_string(&listed).unwrap();
        assert!(!rendered.contains("postgres://super-secret"));
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
