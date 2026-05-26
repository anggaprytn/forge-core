use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};

use crate::secrets::{SealedValueRecord, SecretError, SecretResolution, seal_value};
use crate::storage::{
    EnvStore, PersistedDesiredEnvDeletedKey, PersistedResolvedRuntime,
    PersistedResolvedRuntimeEntry, PersistedRuntimeEnvEntry, PersistedRuntimeEnvSnapshot,
    PersistedRuntimeEnvSource, PersistedSecretReference, StorageError,
};

pub const GENERATED_FORGE_ENV_KEYS: [&str; 7] = [
    "FORGE_PROJECT_ID",
    "FORGE_ENVIRONMENT",
    "FORGE_GENERATION",
    "FORGE_DEPLOYMENT_ID",
    "FORGE_COMMIT_SHA",
    "FORGE_SOURCE_REF",
    "FORGE_DOMAIN",
];

/// Lowest-to-highest precedence for Forge runtime environment resolution.
pub const RUNTIME_ENV_RESOLUTION_ORDER: [&str; 5] = [
    "forge_yml",
    "project_environment_secret",
    "desired_env_config",
    "deploy_time_override",
    "forge_generated",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEnvArtifacts {
    pub container_env: BTreeMap<String, String>,
    pub snapshot: PersistedRuntimeEnvSnapshot,
    pub resolved: PersistedResolvedRuntime,
    pub redacted_preview: Vec<String>,
    pub redaction_values: Vec<String>,
    pub generated_forge_vars: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEnvMetadata {
    pub project_id: String,
    pub environment: String,
    pub generation: u64,
    pub deployment_id: String,
    pub source_ref: Option<String>,
    pub commit_sha: Option<String>,
    pub domain: Option<String>,
}

#[derive(Debug)]
pub enum RuntimeEnvError {
    Secret(SecretError),
    Storage(StorageError),
    ReservedKey(String),
}

impl Display for RuntimeEnvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Secret(err) => write!(f, "{err}"),
            Self::Storage(err) => write!(f, "{err}"),
            Self::ReservedKey(key) => write!(
                f,
                "reserved Forge runtime key cannot be configured or deleted: {key}"
            ),
        }
    }
}

impl std::error::Error for RuntimeEnvError {}

impl From<SecretError> for RuntimeEnvError {
    fn from(value: SecretError) -> Self {
        Self::Secret(value)
    }
}

impl From<StorageError> for RuntimeEnvError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeEnvValue {
    value: String,
    source: PersistedRuntimeEnvSource,
    secret_reference: Option<PersistedSecretReference>,
    sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DesiredRuntimeEnvConfig {
    pub values: BTreeMap<String, String>,
    pub deleted_keys: BTreeMap<String, String>,
}

pub fn build_runtime_env_artifacts(
    metadata: &RuntimeEnvMetadata,
    forge_yaml_values: &BTreeMap<String, String>,
    secret_values: &[SecretResolution],
    desired_env: &DesiredRuntimeEnvConfig,
    deploy_time_overrides: &BTreeMap<String, String>,
) -> Result<RuntimeEnvArtifacts, RuntimeEnvError> {
    let generated_forge_vars = generated_forge_vars(metadata);
    let mut values = BTreeMap::new();

    apply_plain_layer(
        &mut values,
        forge_yaml_values,
        PersistedRuntimeEnvSource::ForgeYaml,
    );
    apply_secret_layer(
        &mut values,
        &metadata.project_id,
        &metadata.environment,
        secret_values,
    );
    apply_plain_layer(
        &mut values,
        &desired_env.values,
        PersistedRuntimeEnvSource::DesiredEnvConfig,
    );
    apply_deleted_keys(&mut values, &desired_env.deleted_keys);
    apply_plain_layer(
        &mut values,
        deploy_time_overrides,
        PersistedRuntimeEnvSource::DeployTimeOverride,
    );
    apply_plain_layer(
        &mut values,
        &generated_forge_vars,
        PersistedRuntimeEnvSource::ForgeGenerated,
    );

    let mut redaction_values = BTreeSet::new();
    let mut container_env = BTreeMap::new();
    let mut snapshot_entries = BTreeMap::new();
    let mut resolved_entries = BTreeMap::new();

    for (key, value) in values {
        let should_redact = value.sensitive || is_sensitive_key(&key);
        if should_redact {
            redaction_values.insert(value.value.clone());
        }
        container_env.insert(key.clone(), value.value.clone());

        let secret_reference = value.secret_reference.clone();
        let sealed = if should_redact {
            Some(seal_value(&value.value)?)
        } else {
            None
        };

        snapshot_entries.insert(
            key.clone(),
            PersistedRuntimeEnvEntry {
                source: value.source.clone(),
                value: if should_redact {
                    None
                } else {
                    Some(value.value.clone())
                },
                secret_reference: secret_reference.clone(),
                sensitive: should_redact,
                redacted: should_redact,
            },
        );
        resolved_entries.insert(
            key,
            PersistedResolvedRuntimeEntry {
                source: value.source,
                value: if should_redact {
                    None
                } else {
                    Some(value.value)
                },
                secret_reference,
                sealed_value: sealed,
                sensitive: should_redact,
            },
        );
    }

    let snapshot = PersistedRuntimeEnvSnapshot {
        snapshot_version: 1,
        project_id: metadata.project_id.clone(),
        environment: metadata.environment.clone(),
        generation: metadata.generation,
        deployment_id: metadata.deployment_id.clone(),
        source_environment: metadata.environment.clone(),
        source_ref: metadata.source_ref.clone(),
        commit_sha: metadata.commit_sha.clone(),
        domain: metadata.domain.clone(),
        resolution_order: RUNTIME_ENV_RESOLUTION_ORDER
            .iter()
            .map(|value| value.to_string())
            .collect(),
        entries: snapshot_entries,
    };
    let resolved = PersistedResolvedRuntime {
        snapshot_version: 1,
        project_id: metadata.project_id.clone(),
        environment: metadata.environment.clone(),
        generation: metadata.generation,
        deployment_id: metadata.deployment_id.clone(),
        source_environment: metadata.environment.clone(),
        source_ref: metadata.source_ref.clone(),
        commit_sha: metadata.commit_sha.clone(),
        domain: metadata.domain.clone(),
        entries: resolved_entries,
    };

    Ok(RuntimeEnvArtifacts {
        redacted_preview: render_redacted_preview(&snapshot),
        redaction_values: redaction_values.into_iter().collect(),
        container_env,
        snapshot,
        resolved,
        generated_forge_vars,
    })
}

pub fn generated_forge_vars(metadata: &RuntimeEnvMetadata) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("FORGE_PROJECT_ID".into(), metadata.project_id.clone()),
        ("FORGE_ENVIRONMENT".into(), metadata.environment.clone()),
        ("FORGE_GENERATION".into(), metadata.generation.to_string()),
        ("FORGE_DEPLOYMENT_ID".into(), metadata.deployment_id.clone()),
        (
            "FORGE_COMMIT_SHA".into(),
            metadata.commit_sha.clone().unwrap_or_default(),
        ),
        (
            "FORGE_SOURCE_REF".into(),
            metadata.source_ref.clone().unwrap_or_default(),
        ),
        (
            "FORGE_DOMAIN".into(),
            metadata.domain.clone().unwrap_or_default(),
        ),
    ])
}

pub fn load_desired_runtime_env_config(
    storage_root: &std::path::Path,
    project_id: &str,
    environment: &str,
) -> Result<DesiredRuntimeEnvConfig, RuntimeEnvError> {
    let Some(config) =
        EnvStore::new(storage_root).load_desired_environment(project_id, environment)?
    else {
        return Ok(DesiredRuntimeEnvConfig::default());
    };

    let mut values = BTreeMap::new();
    for entry in &config.entries {
        ensure_not_reserved_entry(entry.key.as_str(), entry.normalized_key.as_str())?;
        values.insert(
            entry.key.clone(),
            crate::secrets::unseal_value(&entry.sealed_value)?,
        );
    }

    let mut deleted_keys = BTreeMap::new();
    for entry in &config.deleted_keys {
        ensure_not_reserved_entry(entry.key.as_str(), entry.normalized_key.as_str())?;
        deleted_keys.insert(entry.key.clone(), entry.normalized_key.clone());
    }

    Ok(DesiredRuntimeEnvConfig {
        values,
        deleted_keys,
    })
}

pub fn render_snapshot_value(entry: &PersistedRuntimeEnvEntry) -> String {
    if entry.redacted {
        "<secret>".into()
    } else {
        entry.value.clone().unwrap_or_default()
    }
}

pub fn render_redacted_preview(snapshot: &PersistedRuntimeEnvSnapshot) -> Vec<String> {
    snapshot
        .entries
        .iter()
        .map(|(key, entry)| {
            let value = if entry.redacted {
                "[REDACTED]"
            } else {
                entry.value.as_deref().unwrap_or_default()
            };
            format!("{key}={value}")
        })
        .collect()
}

pub fn is_sensitive_key(key: &str) -> bool {
    let uppercase = key.to_ascii_uppercase();
    [
        "SECRET",
        "TOKEN",
        "PASSWORD",
        "PASSWD",
        "SESSION",
        "OAUTH",
        "BEARER",
        "PRIVATE_KEY",
        "CREDENTIAL",
        "DATABASE_URL",
        "API_KEY",
        "ACCESS_KEY",
    ]
    .iter()
    .any(|needle| uppercase.contains(needle))
}

fn apply_plain_layer(
    target: &mut BTreeMap<String, RuntimeEnvValue>,
    values: &BTreeMap<String, String>,
    source: PersistedRuntimeEnvSource,
) {
    for (key, value) in values {
        target.insert(
            key.clone(),
            RuntimeEnvValue {
                value: value.clone(),
                source: source.clone(),
                secret_reference: None,
                sensitive: false,
            },
        );
    }
}

fn apply_deleted_keys(
    target: &mut BTreeMap<String, RuntimeEnvValue>,
    deleted_keys: &BTreeMap<String, String>,
) {
    if deleted_keys.is_empty() {
        return;
    }

    let deleted = deleted_keys.values().cloned().collect::<BTreeSet<_>>();
    target.retain(|key, _| !deleted.contains(&key.to_ascii_lowercase()));
}

fn apply_secret_layer(
    target: &mut BTreeMap<String, RuntimeEnvValue>,
    project_id: &str,
    environment: &str,
    secrets: &[SecretResolution],
) {
    for secret in secrets {
        target.insert(
            secret.key.clone(),
            RuntimeEnvValue {
                value: secret.value.clone(),
                source: PersistedRuntimeEnvSource::ProjectEnvironmentSecret,
                secret_reference: Some(PersistedSecretReference {
                    scope: "environment".into(),
                    key: secret.source_key.clone(),
                    secret_id: Some(format!("{project_id}:{environment}:{}", secret.source_key)),
                    sensitive: true,
                }),
                sensitive: true,
            },
        );
    }
}

pub fn restore_runtime_env(
    resolved: &PersistedResolvedRuntime,
) -> Result<BTreeMap<String, String>, SecretError> {
    let mut restored = BTreeMap::new();
    for (key, entry) in &resolved.entries {
        let value = if let Some(value) = entry.value.clone() {
            value
        } else if let Some(sealed_value) = entry.sealed_value.as_ref() {
            crate::secrets::unseal_value(sealed_value)?
        } else {
            String::new()
        };
        restored.insert(key.clone(), value);
    }
    Ok(restored)
}

pub fn is_reserved_forge_env_key(key: &str) -> bool {
    GENERATED_FORGE_ENV_KEYS
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(key))
}

pub fn ensure_not_reserved_entry(key: &str, normalized_key: &str) -> Result<(), RuntimeEnvError> {
    if is_reserved_forge_env_key(key) || is_reserved_forge_env_key(normalized_key) {
        return Err(RuntimeEnvError::ReservedKey(key.to_string()));
    }
    Ok(())
}

#[allow(dead_code)]
fn _assert_sealed_value_record_is_used(_: &SealedValueRecord) {}

#[allow(dead_code)]
fn _assert_persisted_desired_env_deleted_key_is_used(_: &PersistedDesiredEnvDeletedKey) {}
