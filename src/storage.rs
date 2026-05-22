use crate::events::{EventRecord, redact_text};
use crate::secrets::SealedValueRecord;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const LOCK_RETRY_DELAY: Duration = Duration::from_millis(10);
const LOCK_RETRY_LIMIT: usize = 200;
const DIAGNOSTIC_LOG_MAX_LINES: usize = 64;
const DIAGNOSTIC_LOG_MAX_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotState {
    Healthy,
    Degraded,
    Failed,
    Stopped,
    Rollback,
}

impl SnapshotState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
            Self::Rollback => "rollback",
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    LockTimeout(PathBuf),
    InvalidPointer(PathBuf),
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::LockTimeout(path) => write!(f, "timed out acquiring lock at {}", path.display()),
            Self::InvalidPointer(path) => write!(f, "invalid pointer at {}", path.display()),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeHealthState {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeState {
    pub active_generation: Option<u64>,
    pub health_state: RuntimeHealthState,
    pub failed_probe_count: u32,
    pub successful_probe_count: u32,
    pub restart_attempted: bool,
    pub degraded_since_unix: Option<u64>,
    pub last_transition: String,
    pub last_error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedServiceBuildInfo {
    pub service_id: String,
    pub image_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dockerfile_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub build_args: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_config: Option<PersistedStateConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedBuildInfo {
    pub deployment_id: String,
    pub image_ref: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub services: BTreeMap<String, PersistedServiceBuildInfo>,
    #[serde(default)]
    pub source_ref: Option<String>,
    #[serde(default)]
    pub repo_url: Option<String>,
    #[serde(default)]
    pub commit_sha: Option<String>,
    #[serde(default)]
    pub source_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedSnapshotMetadata {
    pub snapshot_version: u64,
    pub project_id: String,
    pub environment: String,
    pub generation: u64,
    pub state: String,
    pub finalized_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PersistedRouteTargetSource {
    #[default]
    ContainerIp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedActivationMode {
    Direct,
    Http {
        internal_port: u16,
        #[serde(default)]
        route_subtree_id: Option<String>,
        #[serde(default)]
        target_source: PersistedRouteTargetSource,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedServiceState {
    Queued,
    Building,
    Starting,
    Warming,
    Validating,
    Healthy,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedVolumeRetention {
    Persistent,
    Ephemeral,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedStateConfig {
    pub volume: String,
    pub mount_path: String,
    pub retention: PersistedVolumeRetention,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedVolumeMount {
    pub volume_id: String,
    pub docker_volume_name: String,
    pub mount_path: String,
    pub service_id: String,
    pub generation: u64,
    pub retention: PersistedVolumeRetention,
}

impl Default for PersistedServiceState {
    fn default() -> Self {
        Self::Healthy
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedServiceRuntimeInfo {
    pub service_id: String,
    pub container_name: String,
    pub image_ref: String,
    pub running: bool,
    #[serde(default)]
    pub state: PersistedServiceState,
    #[serde(default)]
    pub network_name: Option<String>,
    #[serde(default)]
    pub probe_path: Option<String>,
    #[serde(default)]
    pub activation: Option<PersistedActivationMode>,
    #[serde(default)]
    pub command: Option<Vec<String>>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub required_for_promotion: bool,
    #[serde(default)]
    pub externally_exposed: bool,
    #[serde(default)]
    pub environment_variables: BTreeMap<String, PersistedSecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_config: Option<PersistedStateConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volume_mounts: Vec<PersistedVolumeMount>,
    #[serde(default)]
    pub source_ref: Option<String>,
    #[serde(default)]
    pub repo_url: Option<String>,
    #[serde(default)]
    pub commit_sha: Option<String>,
    #[serde(default)]
    pub source_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedSecretReference {
    pub scope: String,
    pub key: String,
    #[serde(default)]
    pub secret_id: Option<String>,
    pub sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedRuntimeInfo {
    pub container_name: String,
    pub running: bool,
    #[serde(default)]
    pub network_name: Option<String>,
    #[serde(default)]
    pub probe_path: Option<String>,
    #[serde(default)]
    pub activation: Option<PersistedActivationMode>,
    #[serde(default)]
    pub environment_variables: BTreeMap<String, PersistedSecretReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volume_mounts: Vec<PersistedVolumeMount>,
    #[serde(default)]
    pub source_ref: Option<String>,
    #[serde(default)]
    pub repo_url: Option<String>,
    #[serde(default)]
    pub commit_sha: Option<String>,
    #[serde(default)]
    pub source_path: Option<PathBuf>,
    #[serde(default)]
    pub services: BTreeMap<String, PersistedServiceRuntimeInfo>,
    #[serde(default)]
    pub startup_order: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentLifecycleState {
    Queued,
    Building,
    Starting,
    Warming,
    Validating,
    Promoted,
    Rollback,
    Failed,
    GcEligible,
}

impl DeploymentLifecycleState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Building => "building",
            Self::Starting => "starting",
            Self::Warming => "warming",
            Self::Validating => "validating",
            Self::Promoted => "promoted",
            Self::Rollback => "rollback",
            Self::Failed => "failed",
            Self::GcEligible => "gc_eligible",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedValidationSummary {
    #[serde(default)]
    pub tcp_consecutive_passes: u32,
    #[serde(default)]
    pub http_consecutive_passes: u32,
    #[serde(default)]
    pub required_consecutive_passes: u32,
    #[serde(default)]
    pub minimum_uptime_seconds: u64,
    #[serde(default)]
    pub observed_uptime_seconds: u64,
    #[serde(default)]
    pub restart_count_initial: u64,
    #[serde(default)]
    pub restart_count_current: u64,
    #[serde(default)]
    pub restart_count_stable: bool,
    #[serde(default)]
    pub route_verification_stable: bool,
    #[serde(default)]
    pub validation_succeeded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_probe_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedProbeType {
    Tcp,
    Http,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedProbeHistoryEntry {
    pub timestamp_unix: u64,
    pub probe_type: PersistedProbeType,
    pub success: bool,
    pub latency_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedProbeHistory {
    #[serde(default)]
    pub entries: Vec<PersistedProbeHistoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedPromotionSummary {
    #[serde(default)]
    pub warmup_succeeded: bool,
    #[serde(default)]
    pub validation_succeeded: bool,
    #[serde(default)]
    pub route_verification_succeeded: bool,
    #[serde(default)]
    pub runtime_snapshot_persisted: bool,
    #[serde(default)]
    pub convergence_target_stable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promoted_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentLifecycleTransition {
    pub state: DeploymentLifecycleState,
    pub entered_at_unix: u64,
    pub transition_reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_summary: Option<PersistedValidationSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_summary: Option<PersistedPromotionSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedDeploymentLifecycle {
    pub lifecycle_version: u64,
    pub project_id: String,
    pub environment: String,
    pub generation: u64,
    pub state: DeploymentLifecycleState,
    pub entered_at_unix: u64,
    pub transition_reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_summary: Option<PersistedValidationSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_summary: Option<PersistedPromotionSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transitions: Vec<DeploymentLifecycleTransition>,
}

impl PersistedDeploymentLifecycle {
    pub fn transition(
        &mut self,
        state: DeploymentLifecycleState,
        entered_at_unix: u64,
        transition_reason: impl Into<String>,
        validation_summary: Option<PersistedValidationSummary>,
        promotion_summary: Option<PersistedPromotionSummary>,
    ) {
        let transition_reason = transition_reason.into();
        self.state = state.clone();
        self.entered_at_unix = entered_at_unix;
        self.transition_reason = transition_reason.clone();
        self.validation_summary = validation_summary.clone();
        self.promotion_summary = promotion_summary.clone();
        self.transitions.push(DeploymentLifecycleTransition {
            state,
            entered_at_unix,
            transition_reason,
            validation_summary,
            promotion_summary,
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedRuntimeEnvSource {
    ForgeYaml,
    ProjectEnvironmentSecret,
    DeployTimeOverride,
    ForgeGenerated,
    SystemRuntimeReserved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedRuntimeEnvEntry {
    pub source: PersistedRuntimeEnvSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_reference: Option<PersistedSecretReference>,
    #[serde(default)]
    pub sensitive: bool,
    #[serde(default)]
    pub redacted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedRuntimeEnvSnapshot {
    pub snapshot_version: u64,
    pub project_id: String,
    pub environment: String,
    pub generation: u64,
    pub deployment_id: String,
    pub source_environment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolution_order: Vec<String>,
    pub entries: BTreeMap<String, PersistedRuntimeEnvEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedResolvedRuntimeEntry {
    pub source: PersistedRuntimeEnvSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_reference: Option<PersistedSecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed_value: Option<SealedValueRecord>,
    #[serde(default)]
    pub sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedResolvedRuntime {
    pub snapshot_version: u64,
    pub project_id: String,
    pub environment: String,
    pub generation: u64,
    pub deployment_id: String,
    pub source_environment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    pub entries: BTreeMap<String, PersistedResolvedRuntimeEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanupRecord {
    pub timestamp_unix: u64,
    pub failure_reason: String,
    pub container_name: Option<String>,
    pub route_subtree_id: Option<String>,
    #[serde(default)]
    pub image_ref: Option<String>,
    pub container_removed: bool,
    pub route_removed: bool,
    #[serde(default = "default_true")]
    pub image_removed: bool,
    pub tombstoned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticSummary {
    pub deployment_id: Option<String>,
    pub failure_stage: String,
    pub failure_reason: String,
    pub container_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_service_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_target_host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_target_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_target_path: Option<String>,
    pub cleanup_recorded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_graph_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_env_preview: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GenerationHistoryRecord {
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalized_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promoted_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalized_state: Option<String>,
    #[serde(default)]
    pub restored_by_rollback: bool,
    #[serde(default)]
    pub rollback_target: bool,
    #[serde(default)]
    pub retained: bool,
    #[serde(default)]
    pub eligible_for_gc: bool,
    #[serde(default)]
    pub missing_artifacts: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained_reasons: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored_from_backup_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored_from_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored_from_deployment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored_at_unix: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedBackupVolumeRecord {
    pub volume_id: String,
    pub docker_volume_name: String,
    pub service_id: String,
    pub mount_path: String,
    pub archive_file: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedBackupRestoreRecord {
    pub restored_generation: u64,
    pub restored_deployment_id: String,
    pub restored_at_unix: u64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedBackupMetadata {
    pub backup_version: u64,
    pub backup_id: String,
    pub project_id: String,
    pub environment: String,
    pub created_at_unix: u64,
    pub source_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_deployment_id: Option<String>,
    pub snapshot_metadata: PersistedSnapshotMetadata,
    pub build_info: PersistedBuildInfo,
    pub runtime_info: PersistedRuntimeInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_env_snapshot: Option<PersistedRuntimeEnvSnapshot>,
    pub resolved_runtime: PersistedResolvedRuntime,
    #[serde(default)]
    pub services: Vec<String>,
    #[serde(default)]
    pub volumes: Vec<PersistedBackupVolumeRecord>,
    #[serde(default)]
    pub restores: Vec<PersistedBackupRestoreRecord>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RetentionMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_unix: Option<u64>,
    #[serde(default)]
    pub generations: Vec<GenerationHistoryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GcActionRecord {
    pub timestamp_unix: u64,
    pub project_id: String,
    pub environment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    pub dry_run: bool,
    pub action: String,
    pub reason: String,
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub protected: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GcMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_unix: Option<u64>,
    #[serde(default)]
    pub actions: Vec<GcActionRecord>,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            active_generation: None,
            health_state: RuntimeHealthState::Healthy,
            failed_probe_count: 0,
            successful_probe_count: 0,
            restart_attempted: false,
            degraded_since_unix: None,
            last_transition: "initialized".into(),
            last_error_code: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnvironmentPaths {
    pub root: PathBuf,
}

impl EnvironmentPaths {
    pub fn new(root: impl AsRef<Path>, project_id: &str, environment: &str) -> Self {
        Self {
            root: root
                .as_ref()
                .join("projects")
                .join(project_id)
                .join("environments")
                .join(environment),
        }
    }

    pub fn ensure_exists(&self) -> StorageResult<()> {
        fs::create_dir_all(self.generations_dir())?;
        self.ensure_pointer_file("current")?;
        self.ensure_pointer_file("previous")?;
        self.ensure_pointer_file("promoted")?;
        if !self.generation_counter().exists() {
            atomic_write(&self.generation_counter(), b"0\n")?;
        }
        Ok(())
    }

    pub fn generations_dir(&self) -> PathBuf {
        self.root.join("generations")
    }

    pub fn generation_dir(&self, generation: u64) -> PathBuf {
        self.generations_dir().join(generation.to_string())
    }

    pub fn generation_counter(&self) -> PathBuf {
        self.root.join("generation.counter")
    }

    pub fn current_pointer(&self) -> PathBuf {
        self.root.join("current")
    }

    pub fn previous_pointer(&self) -> PathBuf {
        self.root.join("previous")
    }

    pub fn promoted_pointer(&self) -> PathBuf {
        self.root.join("promoted")
    }

    pub fn runtime_state_file(&self) -> PathBuf {
        self.root.join("runtime_state.json")
    }

    pub fn retention_file(&self) -> PathBuf {
        self.root.join("retention.json")
    }

    pub fn gc_file(&self) -> PathBuf {
        self.root.join("gc.json")
    }

    pub fn backups_root(storage_root: impl AsRef<Path>) -> PathBuf {
        storage_root.as_ref().join("backups")
    }

    fn ensure_pointer_file(&self, name: &str) -> StorageResult<()> {
        let path = self.root.join(name);
        if !path.exists() {
            atomic_write(&path, b"\n")?;
        }
        Ok(())
    }
}

pub struct GenerationAllocator {
    env: EnvironmentPaths,
}

impl GenerationAllocator {
    pub fn new(env: EnvironmentPaths) -> Self {
        Self { env }
    }

    pub fn allocate(&self) -> StorageResult<u64> {
        self.env.ensure_exists()?;
        let _guard = FileLock::acquire(self.env.generation_counter().with_extension("lock"))?;
        let current = fs::read_to_string(self.env.generation_counter())?;
        let next = current.trim().parse::<u64>().unwrap_or(0) + 1;
        atomic_write(
            self.env.generation_counter(),
            format!("{next}\n").as_bytes(),
        )?;
        Ok(next)
    }
}

pub struct SnapshotWriter {
    env: EnvironmentPaths,
    generation: u64,
}

impl SnapshotWriter {
    pub fn new(env: EnvironmentPaths, generation: u64) -> StorageResult<Self> {
        env.ensure_exists()?;
        fs::create_dir_all(env.generation_dir(generation).join("diagnostics"))?;
        Ok(Self { env, generation })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn generation_dir(&self) -> PathBuf {
        self.env.generation_dir(self.generation)
    }

    pub fn write_artifact(&self, name: &str, contents: &str) -> StorageResult<()> {
        atomic_write(self.generation_dir().join(name), contents.as_bytes())
    }

    pub fn finalize(
        &self,
        project_id: &str,
        environment: &str,
        state: SnapshotState,
    ) -> StorageResult<()> {
        let finalized_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let snapshot_json = format!(
            "{{\n  \"snapshot_version\": 1,\n  \"project_id\": \"{}\",\n  \"environment\": \"{}\",\n  \"generation\": {},\n  \"state\": \"{}\",\n  \"finalized_at_unix\": {}\n}}\n",
            project_id,
            environment,
            self.generation,
            state.as_str(),
            finalized_at,
        );
        atomic_write(
            self.generation_dir().join("snapshot.json"),
            snapshot_json.as_bytes(),
        )
    }
}

pub struct PointerStore {
    env: EnvironmentPaths,
}

pub struct RuntimeStateStore {
    env: EnvironmentPaths,
}

pub struct EventStore {
    env: EnvironmentPaths,
    generation: u64,
}

pub struct DiagnosticsStore {
    env: EnvironmentPaths,
    generation: u64,
}

pub struct LifecycleStore {
    env: EnvironmentPaths,
    generation: u64,
}

pub struct ProbeHistoryStore {
    env: EnvironmentPaths,
    generation: u64,
}

pub struct RetentionStore {
    env: EnvironmentPaths,
}

pub struct GcStore {
    env: EnvironmentPaths,
}

impl RuntimeStateStore {
    pub fn new(env: EnvironmentPaths) -> Self {
        Self { env }
    }

    pub fn load(&self) -> StorageResult<RuntimeState> {
        self.env.ensure_exists()?;
        let path = self.env.runtime_state_file();
        if !path.exists() {
            return Ok(RuntimeState::default());
        }
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })
    }

    pub fn save(&self, state: &RuntimeState) -> StorageResult<()> {
        self.env.ensure_exists()?;
        let bytes = serde_json::to_vec_pretty(state).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        atomic_write(self.env.runtime_state_file(), &bytes)
    }
}

pub fn load_generation_build_info(
    env: &EnvironmentPaths,
    generation: u64,
) -> StorageResult<Option<PersistedBuildInfo>> {
    load_generation_json(env, generation, "build.json")
}

pub fn load_generation_runtime_info(
    env: &EnvironmentPaths,
    generation: u64,
) -> StorageResult<Option<PersistedRuntimeInfo>> {
    load_generation_json(env, generation, "runtime.json")
}

pub fn load_generation_lifecycle(
    env: &EnvironmentPaths,
    generation: u64,
) -> StorageResult<Option<PersistedDeploymentLifecycle>> {
    load_generation_json(env, generation, "lifecycle.json")
}

pub fn load_generation_probe_history(
    env: &EnvironmentPaths,
    generation: u64,
) -> StorageResult<Option<PersistedProbeHistory>> {
    load_generation_json(env, generation, "probe_history.json")
}

pub fn load_generation_snapshot_metadata(
    env: &EnvironmentPaths,
    generation: u64,
) -> StorageResult<Option<PersistedSnapshotMetadata>> {
    load_generation_json(env, generation, "snapshot.json")
}

pub fn load_generation_runtime_env_snapshot(
    env: &EnvironmentPaths,
    generation: u64,
) -> StorageResult<Option<PersistedRuntimeEnvSnapshot>> {
    load_generation_json(env, generation, "runtime_env_snapshot.json")
}

pub fn load_generation_resolved_runtime(
    env: &EnvironmentPaths,
    generation: u64,
) -> StorageResult<Option<PersistedResolvedRuntime>> {
    load_generation_json(env, generation, "resolved_runtime.json")
}

impl EventStore {
    pub fn new(env: EnvironmentPaths, generation: u64) -> Self {
        Self { env, generation }
    }

    pub fn append(&self, event: &EventRecord) -> StorageResult<()> {
        self.env.ensure_exists()?;
        fs::create_dir_all(self.env.generation_dir(self.generation))?;
        let path = self
            .env
            .generation_dir(self.generation)
            .join("events.jsonl");
        let mut existing = if path.exists() {
            fs::read_to_string(&path)?
        } else {
            String::new()
        };
        let line = serde_json::to_string(event).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        existing.push_str(&line);
        existing.push('\n');
        atomic_write(path, existing.as_bytes())
    }

    pub fn list_all(root: impl AsRef<Path>) -> StorageResult<Vec<EventRecord>> {
        let root = root.as_ref().join("projects");
        let mut events = Vec::new();
        if !root.exists() {
            return Ok(events);
        }
        for project in fs::read_dir(root)? {
            let project = project?;
            if !project.file_type()?.is_dir() {
                continue;
            }
            let envs = project.path().join("environments");
            if !envs.exists() {
                continue;
            }
            for env in fs::read_dir(envs)? {
                let env = env?;
                let generations = env.path().join("generations");
                if !generations.exists() {
                    continue;
                }
                for generation in fs::read_dir(generations)? {
                    let generation = generation?;
                    let path = generation.path().join("events.jsonl");
                    if !path.exists() {
                        continue;
                    }
                    let raw = fs::read_to_string(path)?;
                    for line in raw.lines() {
                        if line.trim().is_empty() {
                            continue;
                        }
                        let event = serde_json::from_str::<EventRecord>(line).map_err(|err| {
                            StorageError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                err.to_string(),
                            ))
                        })?;
                        events.push(event);
                    }
                }
            }
        }
        Ok(events)
    }
}

impl DiagnosticsStore {
    pub fn new(env: EnvironmentPaths, generation: u64) -> Self {
        Self { env, generation }
    }

    pub fn write_failure_reason(&self, reason: &str, secrets: &[String]) -> StorageResult<()> {
        self.env.ensure_exists()?;
        fs::create_dir_all(self.env.generation_dir(self.generation).join("diagnostics"))?;
        let path = self
            .env
            .generation_dir(self.generation)
            .join("diagnostics")
            .join("failure_reason.log");
        let redacted = redact_text(reason, secrets);
        let bounded = truncate_to_recent_bytes(&redacted, DIAGNOSTIC_LOG_MAX_BYTES);
        atomic_write(path, bounded.as_bytes())
    }

    pub fn append_log_line(&self, line: &str, secrets: &[String]) -> StorageResult<()> {
        self.env.ensure_exists()?;
        fs::create_dir_all(self.env.generation_dir(self.generation).join("diagnostics"))?;
        let path = self
            .env
            .generation_dir(self.generation)
            .join("diagnostics")
            .join("deployment.log");
        let mut lines = if path.exists() {
            fs::read_to_string(&path)?
                .lines()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        for value in redact_text(line, secrets).lines() {
            lines.push(value.to_string());
        }
        if lines.is_empty() {
            return Ok(());
        }

        while lines.len() > DIAGNOSTIC_LOG_MAX_LINES {
            lines.remove(0);
        }

        let mut bounded = lines.join("\n");
        while bounded.len() > DIAGNOSTIC_LOG_MAX_BYTES && lines.len() > 1 {
            lines.remove(0);
            bounded = lines.join("\n");
        }
        if bounded.len() > DIAGNOSTIC_LOG_MAX_BYTES {
            bounded = truncate_to_recent_bytes(&bounded, DIAGNOSTIC_LOG_MAX_BYTES);
        }
        bounded.push('\n');
        atomic_write(path, bounded.as_bytes())
    }

    pub fn write_artifact(
        &self,
        name: &str,
        contents: &str,
        secrets: &[String],
    ) -> StorageResult<()> {
        self.env.ensure_exists()?;
        fs::create_dir_all(self.env.generation_dir(self.generation).join("diagnostics"))?;
        let path = self
            .env
            .generation_dir(self.generation)
            .join("diagnostics")
            .join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let redacted = redact_text(contents, secrets);
        let bounded = truncate_to_recent_bytes(&redacted, DIAGNOSTIC_LOG_MAX_BYTES);
        atomic_write(path, bounded.as_bytes())
    }

    pub fn write_summary(&self, summary: &DiagnosticSummary) -> StorageResult<()> {
        self.env.ensure_exists()?;
        fs::create_dir_all(self.env.generation_dir(self.generation).join("diagnostics"))?;
        let path = self
            .env
            .generation_dir(self.generation)
            .join("diagnostics")
            .join("summary.json");
        let bytes = serde_json::to_vec_pretty(summary).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        atomic_write(path, &bytes)
    }

    pub fn read_failure_reason(&self) -> StorageResult<Option<String>> {
        let path = self
            .env
            .generation_dir(self.generation)
            .join("diagnostics")
            .join("failure_reason.log");
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(fs::read_to_string(path)?))
    }

    pub fn read_log_lines(&self) -> StorageResult<Vec<String>> {
        let path = self
            .env
            .generation_dir(self.generation)
            .join("diagnostics")
            .join("deployment.log");
        if !path.exists() {
            return Ok(Vec::new());
        }
        Ok(fs::read_to_string(path)?
            .lines()
            .map(|value| value.to_string())
            .collect())
    }

    pub fn diagnostics_dir(&self) -> PathBuf {
        self.env.generation_dir(self.generation).join("diagnostics")
    }

    pub fn artifact_path(&self, name: &str) -> PathBuf {
        self.diagnostics_dir().join(name)
    }

    pub fn read_summary(&self) -> StorageResult<Option<DiagnosticSummary>> {
        self.read_json_artifact("summary.json")
    }

    pub fn read_text_artifact(&self, name: &str) -> StorageResult<Option<String>> {
        let path = self.artifact_path(name);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(fs::read_to_string(path)?))
    }

    pub fn read_json_artifact<T: DeserializeOwned>(&self, name: &str) -> StorageResult<Option<T>> {
        let Some(raw) = self.read_text_artifact(name)? else {
            return Ok(None);
        };
        serde_json::from_str(&raw).map(Some).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })
    }
}

impl LifecycleStore {
    pub fn new(env: EnvironmentPaths, generation: u64) -> Self {
        Self { env, generation }
    }

    pub fn read(&self) -> StorageResult<Option<PersistedDeploymentLifecycle>> {
        load_generation_lifecycle(&self.env, self.generation)
    }

    pub fn write(&self, lifecycle: &PersistedDeploymentLifecycle) -> StorageResult<()> {
        self.env.ensure_exists()?;
        let bytes = serde_json::to_vec_pretty(lifecycle).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        atomic_write(
            self.env
                .generation_dir(self.generation)
                .join("lifecycle.json"),
            &bytes,
        )
    }
}

impl ProbeHistoryStore {
    pub fn new(env: EnvironmentPaths, generation: u64) -> Self {
        Self { env, generation }
    }

    pub fn read(&self) -> StorageResult<PersistedProbeHistory> {
        self.env.ensure_exists()?;
        Ok(load_generation_probe_history(&self.env, self.generation)?.unwrap_or_default())
    }

    pub fn append(
        &self,
        entry: PersistedProbeHistoryEntry,
        max_entries: usize,
    ) -> StorageResult<()> {
        let mut history = self.read()?;
        history.entries.push(entry);
        if history.entries.len() > max_entries {
            let drop_count = history.entries.len() - max_entries;
            history.entries.drain(0..drop_count);
        }
        self.write(&history)
    }

    pub fn write(&self, history: &PersistedProbeHistory) -> StorageResult<()> {
        self.env.ensure_exists()?;
        let bytes = serde_json::to_vec_pretty(history).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        atomic_write(
            self.env
                .generation_dir(self.generation)
                .join("probe_history.json"),
            &bytes,
        )
    }
}

impl RetentionStore {
    pub fn new(env: EnvironmentPaths) -> Self {
        Self { env }
    }

    pub fn read(&self) -> StorageResult<RetentionMetadata> {
        let path = self.env.retention_file();
        if !path.exists() {
            return Ok(RetentionMetadata::default());
        }
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })
    }

    pub fn write(&self, metadata: &RetentionMetadata) -> StorageResult<()> {
        self.env.ensure_exists()?;
        let bytes = serde_json::to_vec_pretty(metadata).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        atomic_write(self.env.retention_file(), &bytes)
    }
}

impl GcStore {
    pub fn new(env: EnvironmentPaths) -> Self {
        Self { env }
    }

    pub fn read(&self) -> StorageResult<GcMetadata> {
        let path = self.env.gc_file();
        if !path.exists() {
            return Ok(GcMetadata::default());
        }
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })
    }

    pub fn append(&self, action: GcActionRecord) -> StorageResult<()> {
        let mut metadata = self.read()?;
        metadata.updated_at_unix = Some(action.timestamp_unix);
        metadata.actions.push(action);
        let bytes = serde_json::to_vec_pretty(&metadata).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        atomic_write(self.env.gc_file(), &bytes)
    }
}

impl CleanupRecord {
    pub fn new(
        failure_reason: impl Into<String>,
        container_name: Option<String>,
        route_subtree_id: Option<String>,
        container_removed: bool,
        route_removed: bool,
        tombstoned: bool,
    ) -> Self {
        Self {
            timestamp_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            failure_reason: failure_reason.into(),
            container_name,
            route_subtree_id,
            image_ref: None,
            container_removed,
            route_removed,
            image_removed: true,
            tombstoned,
        }
    }
}

pub struct CleanupStore {
    env: EnvironmentPaths,
    generation: u64,
}

impl CleanupStore {
    pub fn new(env: EnvironmentPaths, generation: u64) -> Self {
        Self { env, generation }
    }

    pub fn write_record(&self, record: &CleanupRecord) -> StorageResult<()> {
        self.env.ensure_exists()?;
        fs::create_dir_all(self.env.generation_dir(self.generation))?;
        let bytes = serde_json::to_vec_pretty(record).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        atomic_write(
            self.env
                .generation_dir(self.generation)
                .join("cleanup.json"),
            &bytes,
        )?;
        if record.tombstoned {
            atomic_write(
                self.env.generation_dir(self.generation).join("tombstone"),
                b"cleanup_incomplete\n",
            )?;
        } else {
            let tombstone = self.env.generation_dir(self.generation).join("tombstone");
            if tombstone.exists() {
                fs::remove_file(tombstone)?;
            }
        }
        Ok(())
    }

    pub fn read_record(&self) -> StorageResult<Option<CleanupRecord>> {
        let path = self
            .env
            .generation_dir(self.generation)
            .join("cleanup.json");
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw).map(Some).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })
    }
}

impl PointerStore {
    pub fn new(env: EnvironmentPaths) -> Self {
        Self { env }
    }

    pub fn swap_current(&self, generation: u64) -> StorageResult<()> {
        let generation_dir = self.env.generation_dir(generation);
        if !generation_dir.join("snapshot.json").exists() {
            return Err(StorageError::InvalidPointer(self.env.current_pointer()));
        }

        let _guard = FileLock::acquire(self.env.root.join("pointers.lock"))?;
        let current = self.read_authoritative_pointer()?;
        if let Some(previous_generation) = current.filter(|current| *current != generation) {
            atomic_write(
                self.env.previous_pointer(),
                format!("{previous_generation}\n").as_bytes(),
            )?;
        }

        atomic_write(
            self.env.current_pointer(),
            format!("{generation}\n").as_bytes(),
        )?;
        atomic_write(
            self.env.promoted_pointer(),
            format!("{generation}\n").as_bytes(),
        )
    }

    pub fn read_authoritative_pointer(&self) -> StorageResult<Option<u64>> {
        self.read_pointer("promoted").and_then(|promoted| {
            promoted.map_or_else(|| self.read_pointer("current"), |value| Ok(Some(value)))
        })
    }

    pub fn read_pointer(&self, name: &str) -> StorageResult<Option<u64>> {
        let path = self.env.root.join(name);
        let raw = fs::read_to_string(path)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let value = trimmed
            .parse::<u64>()
            .map_err(|_| StorageError::InvalidPointer(self.env.root.join(name)))?;
        Ok(Some(value))
    }
}

struct FileLock {
    path: PathBuf,
}

impl FileLock {
    fn acquire(path: PathBuf) -> StorageResult<Self> {
        for _ in 0..LOCK_RETRY_LIMIT {
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(b"locked\n")?;
                    file.sync_all()?;
                    return Ok(Self { path });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    thread::sleep(LOCK_RETRY_DELAY);
                }
                Err(err) => return Err(StorageError::Io(err)),
            }
        }
        Err(StorageError::LockTimeout(path))
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) fn atomic_write(path: impl AsRef<Path>, contents: &[u8]) -> StorageResult<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let temp_name = format!(
        ".{}.tmp-{}-{}",
        path.file_name().and_then(|s| s.to_str()).unwrap_or("tmp"),
        std::process::id(),
        unique_suffix()
    );
    let temp_path = path.with_file_name(temp_name);

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;
        file.write_all(contents)?;
        file.sync_all()?;
    }

    fs::rename(&temp_path, path)?;
    sync_dir(path.parent().unwrap_or_else(|| Path::new(".")))?;
    Ok(())
}

pub fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn load_generation_json<T: for<'de> Deserialize<'de>>(
    env: &EnvironmentPaths,
    generation: u64,
    file_name: &str,
) -> StorageResult<Option<T>> {
    let path = env.generation_dir(generation).join(file_name);
    if !path.exists() {
        return Ok(None);
    }
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    serde_json::from_str(&raw).map(Some).map_err(|err| {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            err.to_string(),
        ))
    })
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn default_true() -> bool {
    true
}

fn truncate_to_recent_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_string();
    }
    let mut start = input.len() - max_bytes;
    while !input.is_char_boundary(start) {
        start += 1;
    }
    input[start..].to_string()
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> StorageResult<()> {
    let file = File::open(path)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> StorageResult<()> {
    Ok(())
}

#[cfg(test)]
fn test_root(name: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let base = std::env::temp_dir().join(format!(
        "forge-core-tests-{name}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&base).unwrap();
    base
}

#[cfg(test)]
pub mod snapshot_atomicity {
    use super::*;

    #[test]
    fn snapshot_is_not_considered_finalized_until_snapshot_json_exists() {
        let root = test_root("snapshot-not-finalized");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let writer = SnapshotWriter::new(env.clone(), 1).unwrap();

        writer
            .write_artifact("desired_state.json", "{\n  \"ok\": true\n}\n")
            .unwrap();

        assert!(!writer.generation_dir().join("snapshot.json").exists());
        assert!(writer.generation_dir().join("desired_state.json").exists());
    }

    #[test]
    fn finalize_writes_snapshot_json_and_pointer_swap_requires_it() {
        let root = test_root("snapshot-finalize");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let writer = SnapshotWriter::new(env.clone(), 1).unwrap();
        let pointers = PointerStore::new(env.clone());

        assert!(pointers.swap_current(1).is_err());

        writer
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        pointers.swap_current(1).unwrap();

        assert!(writer.generation_dir().join("snapshot.json").exists());
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
    }
}

#[cfg(test)]
pub mod generation_allocator {
    use super::*;

    #[test]
    fn allocated_generations_are_monotonic_and_unique() {
        let root = test_root("generation-allocator");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let allocator = GenerationAllocator::new(env);

        let first = allocator.allocate().unwrap();
        let second = allocator.allocate().unwrap();
        let third = allocator.allocate().unwrap();

        assert_eq!((first, second, third), (1, 2, 3));
    }
}
