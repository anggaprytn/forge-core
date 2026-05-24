use crate::events::{EventRecord, redact_text};
use crate::secrets::SealedValueRecord;
use libc::{EPERM, ESRCH, kill};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const LOCK_RETRY_DELAY: Duration = Duration::from_millis(10);
const LOCK_RETRY_LIMIT: usize = 200;
const LOCK_STALE_AFTER: Duration = Duration::from_secs(1);
const DIAGNOSTIC_LOG_MAX_LINES: usize = 64;
const DIAGNOSTIC_LOG_MAX_BYTES: usize = 4096;
pub const CONTROL_PLANE_SCHEMA_VERSION: u64 = 1;
pub const SNAPSHOT_SCHEMA_VERSION: u64 = 1;
pub const CHECKPOINT_SCHEMA_VERSION: u64 = CONTROL_PLANE_SCHEMA_VERSION;
pub const BACKUP_METADATA_VERSION: u64 = 1;
pub const CONTROL_PLANE_SNAPSHOT_RETENTION_LIMIT: usize = 12;
const OPERATIONAL_JOURNAL_MAX_BYTES: u64 = 256 * 1024;
const OPERATIONAL_JOURNAL_ROTATIONS: usize = 2;

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
    LockTimeout {
        path: PathBuf,
        holder_pid: Option<u32>,
    },
    InvalidPointer(PathBuf),
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::LockTimeout { path, holder_pid } => {
                write!(f, "timed out acquiring lock at {}", path.display())?;
                if let Some(holder_pid) = holder_pid {
                    write!(f, " (holder_pid={holder_pid})")?;
                }
                Ok(())
            }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedDependencyState {
    pub reachable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_latency_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedBreakerState {
    #[serde(default = "default_breaker_state")]
    pub state: String,
    #[serde(default)]
    pub failure_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_latency_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedEnvironmentCheckpoint {
    #[serde(default = "default_snapshot_version")]
    pub snapshot_version: u64,
    #[serde(default = "default_control_plane_schema_version")]
    pub schema_version: u64,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub environment: String,
    #[serde(default)]
    pub checkpointed_at_unix: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_successful_convergence_unix: Option<u64>,
    #[serde(default)]
    pub last_convergence_duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_convergence_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_convergence_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_generation: Option<u64>,
    #[serde(default = "default_runtime_health_state")]
    pub health_state: RuntimeHealthState,
    #[serde(default)]
    pub dependency_states: BTreeMap<String, PersistedDependencyState>,
    #[serde(default)]
    pub breaker_states: BTreeMap<String, PersistedBreakerState>,
    #[serde(default)]
    pub queue_depth_snapshot: usize,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub lease_epoch: u64,
    #[serde(default)]
    pub convergence_owner: String,
    #[serde(default)]
    pub readyz_reasons: Vec<String>,
    #[serde(default)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedControlPlaneSnapshot {
    #[serde(default = "default_snapshot_version")]
    pub snapshot_version: u64,
    #[serde(default = "default_control_plane_schema_version")]
    pub schema_version: u64,
    #[serde(default)]
    pub snapshot_kind: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub environment: String,
    #[serde(default)]
    pub cycle_id: String,
    #[serde(default)]
    pub created_at_unix: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub lease_epoch: u64,
    #[serde(default)]
    pub convergence_owner: String,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedLeaderLease {
    #[serde(default = "default_control_plane_schema_version")]
    pub schema_version: u64,
    #[serde(default)]
    pub leader_node_id: String,
    #[serde(default)]
    pub acquired_at_unix: u64,
    #[serde(default)]
    pub expires_at_unix: u64,
    #[serde(default)]
    pub lease_epoch: u64,
    #[serde(default)]
    pub last_heartbeat_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedNodeMetadata {
    #[serde(default = "default_control_plane_schema_version")]
    pub schema_version: u64,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub booted_at_unix: u64,
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub runtime_info: BTreeMap<String, String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedClusterNode {
    #[serde(default = "default_control_plane_schema_version")]
    pub schema_version: u64,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub advertised_addr: String,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub last_seen_unix: u64,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub lease_epoch_seen: u64,
    #[serde(default)]
    pub control_plane_version: String,
    #[serde(default)]
    pub reconciliation_enabled: bool,
    #[serde(default)]
    pub active_reconciler: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedClusterTopology {
    #[serde(default = "default_control_plane_schema_version")]
    pub schema_version: u64,
    #[serde(default)]
    pub nodes: Vec<PersistedClusterNode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OperationalJournalEntry {
    #[serde(default = "default_control_plane_schema_version")]
    pub schema_version: u64,
    #[serde(default)]
    pub timestamp_unix: u64,
    #[serde(default)]
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeHealthState {
    Healthy,
    Degraded,
    Unavailable,
}

impl Default for RuntimeHealthState {
    fn default() -> Self {
        Self::Healthy
    }
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
    Unstable,
    CrashLoop,
    OomKilled,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_backup_command: Option<String>,
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
    pub runtime_policy: PersistedRuntimePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_usage: Option<PersistedRuntimeUsageSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination: Option<PersistedTerminationInfo>,
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
    pub runtime_policy: PersistedRuntimePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_usage: Option<PersistedRuntimeUsageSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination: Option<PersistedTerminationInfo>,
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
    Unstable,
    CrashLoop,
    OomKilled,
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
            Self::Unstable => "unstable",
            Self::CrashLoop => "crash_loop",
            Self::OomKilled => "oom_killed",
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
    #[serde(default)]
    pub unstable_probe_failures: u32,
    #[serde(default)]
    pub restart_storm_detected: bool,
    #[serde(default)]
    pub oom_detected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedRuntimePolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_mb: Option<u64>,
    #[serde(
        default = "default_restart_policy",
        deserialize_with = "deserialize_restart_policy"
    )]
    pub restart_policy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedRuntimeUsageSnapshot {
    #[serde(default)]
    pub captured_at_unix: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_percent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_usage_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_mb: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedTerminationInfo {
    #[serde(default)]
    pub oom_killed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logs_tail: Option<String>,
    #[serde(default)]
    pub restart_count: u64,
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
    #[serde(default)]
    pub timestamp: u64,
    #[serde(default)]
    pub timestamp_unix: u64,
    #[serde(default)]
    pub generation_failure_reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(default = "default_true")]
    pub cleanup_attempted: bool,
    #[serde(default)]
    pub cleanup_completed: bool,
    #[serde(default)]
    pub removed_containers: Vec<String>,
    #[serde(default)]
    pub removed_images: Vec<String>,
    #[serde(default)]
    pub removed_volumes: Vec<String>,
    #[serde(default)]
    pub skipped: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_subtree_id: Option<String>,
    #[serde(default)]
    pub image_ref: Option<String>,
    #[serde(default)]
    pub container_removed: bool,
    #[serde(default)]
    pub route_removed: bool,
    #[serde(default = "default_true")]
    pub image_removed: bool,
    #[serde(default)]
    pub tombstoned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticSummary {
    pub deployment_id: Option<String>,
    pub failure_stage: String,
    pub failure_reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocking_reason: Option<String>,
    pub container_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_service_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocking_service_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_target_host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_target_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_target_path: Option<String>,
    #[serde(default)]
    pub restart_storm: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_count_delta: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oom_killed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination_reason: Option<String>,
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
pub struct PersistedBackupArchiveFileRecord {
    pub path: String,
    pub size_bytes: u64,
    pub sha256: String,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub archive_files: Vec<PersistedBackupArchiveFileRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedBackupRestoreRecord {
    pub restored_generation: u64,
    pub restored_deployment_id: String,
    pub restored_at_unix: u64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedBackupHookRecord {
    pub service_id: String,
    pub volume_id: String,
    pub container_name: String,
    #[serde(alias = "command")]
    pub pre_backup_command: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "executed_at_unix"
    )]
    pub started_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix: Option<u64>,
    pub timeout_seconds: u64,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
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
    pub hooks: Vec<PersistedBackupHookRecord>,
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

    pub fn checkpoint_file(&self) -> PathBuf {
        self.root.join("convergence_checkpoint.json")
    }

    pub fn control_plane_snapshots_dir(&self) -> PathBuf {
        self.root.join("control_plane_snapshots")
    }

    pub fn leader_lease_file(storage_root: impl AsRef<Path>) -> PathBuf {
        storage_root
            .as_ref()
            .join("control_plane")
            .join("leader_lease.json")
    }

    pub fn backups_root(storage_root: impl AsRef<Path>) -> PathBuf {
        storage_root.as_ref().join("backups")
    }

    pub fn node_metadata_file(storage_root: impl AsRef<Path>) -> PathBuf {
        storage_root
            .as_ref()
            .join("control_plane")
            .join("node.json")
    }

    pub fn operational_journal_file(storage_root: impl AsRef<Path>) -> PathBuf {
        storage_root
            .as_ref()
            .join("control_plane")
            .join("operations.jsonl")
    }

    pub fn reconciliation_log_file(storage_root: impl AsRef<Path>) -> PathBuf {
        storage_root
            .as_ref()
            .join("control_plane")
            .join("reconciliation_log.jsonl")
    }

    pub fn reconciliation_cursor_file(storage_root: impl AsRef<Path>) -> PathBuf {
        storage_root
            .as_ref()
            .join("control_plane")
            .join("reconciliation_cursor.json")
    }

    pub fn reconciliation_quarantine_dir(storage_root: impl AsRef<Path>) -> PathBuf {
        storage_root
            .as_ref()
            .join("control_plane")
            .join("quarantine")
    }

    pub fn cluster_nodes_file(storage_root: impl AsRef<Path>) -> PathBuf {
        storage_root
            .as_ref()
            .join("control_plane")
            .join("cluster_nodes.json")
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

pub struct ConvergenceCheckpointStore {
    env: EnvironmentPaths,
}

pub struct ControlPlaneSnapshotStore {
    env: EnvironmentPaths,
}

pub struct LeaderLeaseStore {
    storage_root: PathBuf,
}

pub struct NodeMetadataStore {
    storage_root: PathBuf,
}

pub struct ClusterTopologyStore {
    storage_root: PathBuf,
}

pub struct OperationalJournalStore {
    storage_root: PathBuf,
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

impl ConvergenceCheckpointStore {
    pub fn new(env: EnvironmentPaths) -> Self {
        Self { env }
    }

    pub fn load(&self) -> StorageResult<Option<PersistedEnvironmentCheckpoint>> {
        load_json_file(self.env.checkpoint_file())
    }

    pub fn save(&self, checkpoint: &PersistedEnvironmentCheckpoint) -> StorageResult<()> {
        self.env.ensure_exists()?;
        write_pretty_json(self.env.checkpoint_file(), checkpoint)
    }
}

impl ControlPlaneSnapshotStore {
    pub fn new(env: EnvironmentPaths) -> Self {
        Self { env }
    }

    pub fn append(
        &self,
        snapshot: &PersistedControlPlaneSnapshot,
        retention_limit: usize,
    ) -> StorageResult<()> {
        self.env.ensure_exists()?;
        fs::create_dir_all(self.env.control_plane_snapshots_dir())?;
        let file_name = format!(
            "{}-{}.json",
            snapshot.created_at_unix, snapshot.snapshot_kind
        );
        write_pretty_json(
            self.env.control_plane_snapshots_dir().join(file_name),
            snapshot,
        )?;
        self.gc(retention_limit)?;
        Ok(())
    }

    pub fn list(&self) -> StorageResult<Vec<PersistedControlPlaneSnapshot>> {
        let dir = self.env.control_plane_snapshots_dir();
        let mut paths = Vec::new();
        if !dir.exists() {
            return Ok(Vec::new());
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                paths.push(entry.path());
            }
        }
        paths.sort();
        let mut snapshots = Vec::new();
        for path in paths {
            match load_json_file(&path) {
                Ok(Some(snapshot)) => snapshots.push(snapshot),
                Ok(None) => {}
                Err(err) => eprintln!(
                    "warning: ignoring malformed control plane snapshot {}: {}",
                    path.display(),
                    err
                ),
            }
        }
        Ok(snapshots)
    }

    pub fn latest_by_kind(
        &self,
        snapshot_kind: &str,
    ) -> StorageResult<Option<PersistedControlPlaneSnapshot>> {
        let mut snapshots = self
            .list()?
            .into_iter()
            .filter(|snapshot| snapshot.snapshot_kind == snapshot_kind)
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.created_at_unix.cmp(&right.created_at_unix));
        Ok(snapshots.pop())
    }

    pub fn gc(&self, retention_limit: usize) -> StorageResult<()> {
        let dir = self.env.control_plane_snapshots_dir();
        if !dir.exists() {
            return Ok(());
        }
        let mut paths = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                paths.push(entry.path());
            }
        }
        paths.sort();
        let excess = paths.len().saturating_sub(retention_limit);
        for path in paths.into_iter().take(excess) {
            let _ = fs::remove_file(path);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseAcquireOutcome {
    Leader(PersistedLeaderLease),
    Follower(PersistedLeaderLease),
}

impl LeaderLeaseStore {
    pub fn new(storage_root: impl AsRef<Path>) -> Self {
        Self {
            storage_root: storage_root.as_ref().to_path_buf(),
        }
    }

    pub fn load(&self) -> StorageResult<Option<PersistedLeaderLease>> {
        load_json_file(EnvironmentPaths::leader_lease_file(&self.storage_root))
    }

    pub fn try_acquire_or_renew(
        &self,
        node_id: &str,
        now_unix: u64,
        ttl_seconds: u64,
    ) -> StorageResult<LeaseAcquireOutcome> {
        let path = EnvironmentPaths::leader_lease_file(&self.storage_root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let _guard = FileLock::acquire(path.with_extension("lock"))?;
        let current = load_json_file::<PersistedLeaderLease>(&path)?.unwrap_or_default();
        let is_active = !current.leader_node_id.is_empty() && current.expires_at_unix > now_unix;
        if is_active && current.leader_node_id != node_id {
            return Ok(LeaseAcquireOutcome::Follower(current));
        }

        let next = if current.leader_node_id == node_id {
            PersistedLeaderLease {
                schema_version: CONTROL_PLANE_SCHEMA_VERSION,
                leader_node_id: node_id.to_string(),
                acquired_at_unix: current.acquired_at_unix.max(1),
                expires_at_unix: now_unix.saturating_add(ttl_seconds),
                lease_epoch: current.lease_epoch.max(1),
                last_heartbeat_unix: now_unix,
            }
        } else {
            PersistedLeaderLease {
                schema_version: CONTROL_PLANE_SCHEMA_VERSION,
                leader_node_id: node_id.to_string(),
                acquired_at_unix: now_unix,
                expires_at_unix: now_unix.saturating_add(ttl_seconds),
                lease_epoch: current.lease_epoch.saturating_add(1).max(1),
                last_heartbeat_unix: now_unix,
            }
        };
        write_pretty_json(&path, &next)?;
        Ok(LeaseAcquireOutcome::Leader(next))
    }

    pub fn release_if_owner(&self, node_id: &str, now_unix: u64) -> StorageResult<()> {
        let path = EnvironmentPaths::leader_lease_file(&self.storage_root);
        if !path.exists() {
            return Ok(());
        }
        let _guard = FileLock::acquire(path.with_extension("lock"))?;
        let Some(mut current) = load_json_file::<PersistedLeaderLease>(&path)? else {
            return Ok(());
        };
        if current.leader_node_id != node_id {
            return Ok(());
        }
        current.expires_at_unix = now_unix;
        current.last_heartbeat_unix = now_unix;
        write_pretty_json(&path, &current)?;
        Ok(())
    }
}

impl NodeMetadataStore {
    pub fn new(storage_root: impl AsRef<Path>) -> Self {
        Self {
            storage_root: storage_root.as_ref().to_path_buf(),
        }
    }

    pub fn load(&self) -> StorageResult<Option<PersistedNodeMetadata>> {
        load_json_file(EnvironmentPaths::node_metadata_file(&self.storage_root))
    }

    pub fn load_or_create(&self) -> StorageResult<PersistedNodeMetadata> {
        if let Some(metadata) = self.load()? {
            return Ok(metadata);
        }
        let metadata = PersistedNodeMetadata {
            schema_version: CONTROL_PLANE_SCHEMA_VERSION,
            node_id: generated_node_id(),
            booted_at_unix: current_unix_timestamp(),
            hostname: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
            capabilities: vec!["docker".into(), "caddy".into(), "backups".into()],
            runtime_info: BTreeMap::from([
                ("pid".into(), std::process::id().to_string()),
                ("version".into(), env!("CARGO_PKG_VERSION").into()),
            ]),
            metadata: BTreeMap::new(),
        };
        write_pretty_json(
            EnvironmentPaths::node_metadata_file(&self.storage_root),
            &metadata,
        )?;
        Ok(metadata)
    }
}

impl ClusterTopologyStore {
    pub fn new(storage_root: impl AsRef<Path>) -> Self {
        Self {
            storage_root: storage_root.as_ref().to_path_buf(),
        }
    }

    pub fn load(&self) -> StorageResult<PersistedClusterTopology> {
        Ok(
            load_json_file(EnvironmentPaths::cluster_nodes_file(&self.storage_root))?
                .unwrap_or_else(|| PersistedClusterTopology {
                    schema_version: CONTROL_PLANE_SCHEMA_VERSION,
                    nodes: Vec::new(),
                }),
        )
    }

    pub fn save(&self, topology: &PersistedClusterTopology) -> StorageResult<()> {
        write_pretty_json(
            EnvironmentPaths::cluster_nodes_file(&self.storage_root),
            topology,
        )
    }

    pub fn upsert_node(&self, node: &PersistedClusterNode) -> StorageResult<()> {
        let path = EnvironmentPaths::cluster_nodes_file(&self.storage_root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let _guard = FileLock::acquire(path.with_extension("lock"))?;
        let mut topology = self.load()?;
        topology.schema_version = CONTROL_PLANE_SCHEMA_VERSION;
        if let Some(existing) = topology
            .nodes
            .iter_mut()
            .find(|existing| existing.node_id == node.node_id)
        {
            *existing = node.clone();
        } else {
            topology.nodes.push(node.clone());
        }
        topology
            .nodes
            .sort_by(|left, right| left.node_id.cmp(&right.node_id));
        write_pretty_json(path, &topology)
    }
}

impl OperationalJournalStore {
    pub fn new(storage_root: impl AsRef<Path>) -> Self {
        Self {
            storage_root: storage_root.as_ref().to_path_buf(),
        }
    }

    pub fn append(&self, entry: &OperationalJournalEntry) -> StorageResult<()> {
        let path = EnvironmentPaths::operational_journal_file(&self.storage_root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        rotate_journal_if_needed(&path)?;
        let line = serde_json::to_string(entry).map_err(json_io_error)?;
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        Ok(())
    }

    pub fn read_all(&self) -> StorageResult<Vec<OperationalJournalEntry>> {
        let path = EnvironmentPaths::operational_journal_file(&self.storage_root);
        let mut paths = vec![path.clone()];
        for index in 1..=OPERATIONAL_JOURNAL_ROTATIONS {
            paths.push(path.with_extension(format!("jsonl.{index}")));
        }
        let mut entries = Vec::new();
        for file in paths {
            if !file.exists() {
                continue;
            }
            let raw = fs::read_to_string(file)?;
            for line in raw.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str(line) {
                    Ok(parsed) => entries.push(parsed),
                    Err(err) => eprintln!(
                        "warning: skipping malformed journal entry in {}: {}",
                        path.display(),
                        err
                    ),
                }
            }
        }
        Ok(entries)
    }
}

impl CleanupRecord {
    pub fn new(
        generation_failure_reason: impl Into<String>,
        container_name: Option<String>,
        route_subtree_id: Option<String>,
        container_removed: bool,
        route_removed: bool,
        tombstoned: bool,
    ) -> Self {
        let timestamp = current_unix_timestamp();
        Self {
            timestamp,
            timestamp_unix: timestamp,
            generation_failure_reason: generation_failure_reason.into(),
            failure_reason: None,
            cleanup_attempted: true,
            cleanup_completed: !tombstoned,
            removed_containers: Vec::new(),
            removed_images: Vec::new(),
            removed_volumes: Vec::new(),
            skipped: Vec::new(),
            container_name,
            route_subtree_id,
            image_ref: None,
            container_removed,
            route_removed,
            image_removed: true,
            tombstoned,
        }
    }

    pub fn skipped_failed_generation(generation_failure_reason: impl Into<String>) -> Self {
        let mut cleanup = Self::new(generation_failure_reason, None, None, true, true, false);
        cleanup.skipped = vec![
            "container:not_created".into(),
            "image:not_built".into(),
            "route:not_created".into(),
        ];
        cleanup
    }

    pub fn normalized(mut self) -> Self {
        if self.timestamp == 0 {
            self.timestamp = current_unix_timestamp();
        }
        if self.timestamp_unix == 0 {
            self.timestamp_unix = self.timestamp;
        }
        self.cleanup_attempted = true;
        if self.container_removed
            && self.removed_containers.is_empty()
            && self.container_name.is_some()
        {
            self.removed_containers
                .push(self.container_name.clone().expect("checked is_some"));
        }
        if self.image_removed && self.removed_images.is_empty() && self.image_ref.is_some() {
            self.removed_images
                .push(self.image_ref.clone().expect("checked is_some"));
        }
        if !self.container_removed
            && let Some(container_name) = self.container_name.as_deref()
        {
            self.skipped.push(format!("container:{container_name}"));
        }
        if !self.route_removed
            && let Some(route_subtree_id) = self.route_subtree_id.as_deref()
        {
            self.skipped.push(format!("route:{route_subtree_id}"));
        }
        if !self.image_removed
            && let Some(image_ref) = self.image_ref.as_deref()
        {
            self.skipped.push(format!("image:{image_ref}"));
        }
        self.removed_containers.sort();
        self.removed_containers.dedup();
        self.removed_images.sort();
        self.removed_images.dedup();
        self.removed_volumes.sort();
        self.removed_volumes.dedup();
        self.skipped.sort();
        self.skipped.dedup();
        self.cleanup_completed = self.failure_reason.is_none()
            && self.container_removed
            && self.route_removed
            && self.image_removed;
        if !self.cleanup_completed && self.failure_reason.is_none() {
            self.failure_reason = Some(self.cleanup_failure_reason());
        }
        self.tombstoned = !self.cleanup_completed;
        self
    }

    fn cleanup_failure_reason(&self) -> String {
        let mut pending = Vec::new();
        if !self.container_removed {
            pending.push(
                self.container_name
                    .as_deref()
                    .map(|value| format!("container `{value}`"))
                    .unwrap_or_else(|| "container".into()),
            );
        }
        if !self.route_removed {
            pending.push(
                self.route_subtree_id
                    .as_deref()
                    .map(|value| format!("route `{value}`"))
                    .unwrap_or_else(|| "route".into()),
            );
        }
        if !self.image_removed {
            pending.push(
                self.image_ref
                    .as_deref()
                    .map(|value| format!("image `{value}`"))
                    .unwrap_or_else(|| "image".into()),
            );
        }
        if pending.is_empty() {
            "cleanup incomplete".into()
        } else {
            format!("cleanup incomplete for {}", pending.join(", "))
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
        let record = record.clone().normalized();
        let bytes = serde_json::to_vec_pretty(&record).map_err(|err| {
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

#[derive(Debug)]
struct FileLock {
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LockFileRecord {
    pid: u32,
    acquired_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LockInspection {
    pub holder_pid: Option<u32>,
    pub recoverable: bool,
    pub details: String,
}

impl FileLock {
    fn acquire(path: PathBuf) -> StorageResult<Self> {
        let mut last_holder_pid = None;
        for _ in 0..LOCK_RETRY_LIMIT {
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(mut file) => {
                    let record = LockFileRecord {
                        pid: std::process::id(),
                        acquired_at_unix: current_unix_timestamp(),
                    };
                    let bytes = serde_json::to_vec(&record).map_err(json_io_error)?;
                    file.write_all(&bytes)?;
                    file.write_all(b"\n")?;
                    file.sync_all()?;
                    return Ok(Self { path });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if let Some(inspection) = inspect_lock_file(&path)? {
                        last_holder_pid = inspection.holder_pid.or(last_holder_pid);
                        if inspection.recoverable {
                            match fs::remove_file(&path) {
                                Ok(()) => continue,
                                Err(remove_err)
                                    if remove_err.kind() == std::io::ErrorKind::NotFound =>
                                {
                                    continue;
                                }
                                Err(remove_err) => return Err(StorageError::Io(remove_err)),
                            }
                        }
                    }
                    thread::sleep(LOCK_RETRY_DELAY);
                }
                Err(err) => return Err(StorageError::Io(err)),
            }
        }
        Err(StorageError::LockTimeout {
            path,
            holder_pid: last_holder_pid,
        })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) fn inspect_lock_file(path: &Path) -> StorageResult<Option<LockInspection>> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(StorageError::Io(err)),
    };
    if metadata.is_dir() {
        return Ok(Some(LockInspection {
            holder_pid: None,
            recoverable: false,
            details: "lock path is a directory".into(),
        }));
    }

    let raw = fs::read(path)?;
    let record = serde_json::from_slice::<LockFileRecord>(&raw).ok();
    if let Some(record) = record {
        if process_is_alive(record.pid) {
            return Ok(Some(LockInspection {
                holder_pid: Some(record.pid),
                recoverable: false,
                details: format!("lock held by pid {}", record.pid),
            }));
        }
        return Ok(Some(LockInspection {
            holder_pid: Some(record.pid),
            recoverable: true,
            details: format!("stale lock from dead pid {}", record.pid),
        }));
    }

    let age = lock_file_age(&metadata).unwrap_or(LOCK_STALE_AFTER);
    Ok(Some(LockInspection {
        holder_pid: None,
        recoverable: age >= LOCK_STALE_AFTER,
        details: if age >= LOCK_STALE_AFTER {
            format!(
                "stale legacy lock older than {}ms",
                LOCK_STALE_AFTER.as_millis()
            )
        } else {
            "legacy lock file is still fresh".into()
        },
    }))
}

pub(crate) fn clear_stale_lock_file(path: &Path) -> StorageResult<Option<LockInspection>> {
    let Some(inspection) = inspect_lock_file(path)? else {
        return Ok(None);
    };
    if !inspection.recoverable {
        return Ok(Some(inspection));
    }
    match fs::remove_file(path) {
        Ok(()) => Ok(Some(inspection)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StorageError::Io(err)),
    }
}

fn lock_file_age(metadata: &fs::Metadata) -> Option<Duration> {
    metadata.modified().ok()?.elapsed().ok()
}

fn process_is_alive(pid: u32) -> bool {
    let result = unsafe { kill(pid as i32, 0) };
    if result == 0 {
        return true;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(ESRCH) => false,
        Some(EPERM) => true,
        _ => Command::new("ps")
            .args(["-p", &pid.to_string()])
            .status()
            .map(|status| status.success())
            .unwrap_or(false),
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

fn default_breaker_state() -> String {
    "closed".into()
}

fn default_control_plane_schema_version() -> u64 {
    CONTROL_PLANE_SCHEMA_VERSION
}

fn default_snapshot_version() -> u64 {
    SNAPSHOT_SCHEMA_VERSION
}

fn default_runtime_health_state() -> RuntimeHealthState {
    RuntimeHealthState::Healthy
}

fn default_restart_policy() -> String {
    "no".into()
}

fn generated_node_id() -> String {
    format!("forge-node-{}", unique_suffix())
}

fn json_io_error(err: impl ToString) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
}

fn write_pretty_json(path: impl AsRef<Path>, value: &impl Serialize) -> StorageResult<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(json_io_error)?;
    atomic_write(path, &bytes)
}

fn load_json_file<T: for<'de> Deserialize<'de>>(
    path: impl AsRef<Path>,
) -> StorageResult<Option<T>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(None);
    }
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    serde_json::from_str(&raw)
        .map(Some)
        .map_err(|err| StorageError::Io(json_io_error(err)))
}

fn rotate_journal_if_needed(path: &Path) -> StorageResult<()> {
    let size = fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
    if size < OPERATIONAL_JOURNAL_MAX_BYTES {
        return Ok(());
    }
    for index in (1..=OPERATIONAL_JOURNAL_ROTATIONS).rev() {
        let source = if index == 1 {
            path.to_path_buf()
        } else {
            path.with_extension(format!("jsonl.{}", index - 1))
        };
        let target = path.with_extension(format!("jsonl.{index}"));
        if source.exists() {
            let _ = fs::remove_file(&target);
            fs::rename(source, target)?;
        }
    }
    Ok(())
}

pub fn normalize_restart_policy_name(policy: &str) -> String {
    let policy = policy.trim();
    if policy.is_empty() {
        "no".into()
    } else {
        policy.into()
    }
}

fn deserialize_restart_policy<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?.unwrap_or_else(default_restart_policy);
    Ok(normalize_restart_policy_name(&value))
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
pub mod control_plane_persistence_semantics {
    use super::*;

    #[test]
    fn snapshot_write_is_atomic() {
        let root = test_root("snapshot-write-is-atomic");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let store = ControlPlaneSnapshotStore::new(env.clone());
        store
            .append(
                &PersistedControlPlaneSnapshot {
                    snapshot_version: 1,
                    schema_version: 1,
                    snapshot_kind: "runtime_snapshot".into(),
                    project_id: "api".into(),
                    environment: "production".into(),
                    cycle_id: "cycle-1".into(),
                    created_at_unix: 1,
                    generation: Some(1),
                    node_id: "node-test".into(),
                    lease_epoch: 1,
                    convergence_owner: "node-test".into(),
                    payload: serde_json::json!({"ok": true}),
                },
                12,
            )
            .unwrap();

        let dir = env.control_plane_snapshots_dir();
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        assert!(
            fs::read_dir(&dir)
                .unwrap()
                .flatten()
                .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp"))
        );
    }

    #[test]
    fn corrupted_snapshot_ignored_and_rebuilt() {
        let root = test_root("corrupted-snapshot-ignored-and-rebuilt");
        let env = EnvironmentPaths::new(&root, "api", "production");
        fs::create_dir_all(env.control_plane_snapshots_dir()).unwrap();
        fs::write(
            env.control_plane_snapshots_dir()
                .join("1-runtime_snapshot.json"),
            "{ invalid json",
        )
        .unwrap();
        let store = ControlPlaneSnapshotStore::new(env.clone());
        assert!(store.latest_by_kind("runtime_snapshot").unwrap().is_none());

        store
            .append(
                &PersistedControlPlaneSnapshot {
                    snapshot_version: 1,
                    schema_version: 1,
                    snapshot_kind: "runtime_snapshot".into(),
                    project_id: "api".into(),
                    environment: "production".into(),
                    cycle_id: "cycle-2".into(),
                    created_at_unix: 2,
                    generation: Some(2),
                    node_id: "node-test".into(),
                    lease_epoch: 2,
                    convergence_owner: "node-test".into(),
                    payload: serde_json::json!({"rebuilt": true}),
                },
                12,
            )
            .unwrap();

        let latest = store.latest_by_kind("runtime_snapshot").unwrap().unwrap();
        assert_eq!(latest.generation, Some(2));
    }

    #[test]
    fn snapshot_gc_preserves_recent_snapshots() {
        let root = test_root("snapshot-gc-preserves-recent-snapshots");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let store = ControlPlaneSnapshotStore::new(env.clone());
        for created_at_unix in 1..=5 {
            store
                .append(
                    &PersistedControlPlaneSnapshot {
                        snapshot_version: 1,
                        schema_version: 1,
                        snapshot_kind: "runtime_snapshot".into(),
                        project_id: "api".into(),
                        environment: "production".into(),
                        cycle_id: format!("cycle-{created_at_unix}"),
                        created_at_unix,
                        generation: Some(created_at_unix),
                        node_id: "node-test".into(),
                        lease_epoch: created_at_unix,
                        convergence_owner: "node-test".into(),
                        payload: serde_json::json!({ "generation": created_at_unix }),
                    },
                    2,
                )
                .unwrap();
        }

        let generations = store
            .list()
            .unwrap()
            .into_iter()
            .map(|snapshot| snapshot.generation.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(generations, vec![4, 5]);
    }

    #[test]
    fn malformed_journal_entry_skipped() {
        let root = test_root("storage-malformed-journal-entry-skipped");
        let path = EnvironmentPaths::operational_journal_file(&root);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            concat!(
                "{\"schema_version\":1,\"timestamp_unix\":1,\"event_type\":\"ok\",\"payload\":{}}\n",
                "{ invalid json\n"
            ),
        )
        .unwrap();

        let entries = OperationalJournalStore::new(root).read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event_type, "ok");
    }

    #[test]
    fn journal_rotation_keeps_recent_entries() {
        let root = test_root("journal-rotation-keeps-recent-entries");
        let journal = OperationalJournalStore::new(&root);
        for index in 0..2000 {
            journal
                .append(&OperationalJournalEntry {
                    schema_version: 1,
                    timestamp_unix: current_unix_timestamp(),
                    event_type: "gc_action".into(),
                    project_id: None,
                    environment: None,
                    generation: None,
                    payload: serde_json::json!({
                        "index": index,
                        "padding": "x".repeat(256),
                    }),
                })
                .unwrap();
        }

        let entries = journal.read_all().unwrap();
        assert!(entries.iter().any(|entry| entry.payload["index"] == 1999));
    }
}

#[cfg(test)]
pub mod leader_lease_semantics {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn leader_lease_survives_restart() {
        let root = test_root("leader-lease-survives-restart");
        let store = LeaderLeaseStore::new(&root);
        match store.try_acquire_or_renew("node-a", 100, 5).unwrap() {
            LeaseAcquireOutcome::Leader(lease) => {
                assert_eq!(lease.leader_node_id, "node-a");
            }
            LeaseAcquireOutcome::Follower(_) => panic!("expected leader lease"),
        }

        let persisted = LeaderLeaseStore::new(&root).load().unwrap().unwrap();
        assert_eq!(persisted.leader_node_id, "node-a");
        assert_eq!(persisted.lease_epoch, 1);
    }

    #[test]
    fn stale_leader_lease_can_be_taken_over() {
        let root = test_root("stale-leader-lease-can-be-taken-over");
        let store = LeaderLeaseStore::new(&root);
        store.try_acquire_or_renew("node-a", 100, 5).unwrap();

        match store.try_acquire_or_renew("node-b", 106, 5).unwrap() {
            LeaseAcquireOutcome::Leader(lease) => {
                assert_eq!(lease.leader_node_id, "node-b");
                assert_eq!(lease.lease_epoch, 2);
            }
            LeaseAcquireOutcome::Follower(_) => panic!("expected takeover"),
        }
    }

    #[test]
    fn concurrent_leader_acquisition_single_winner() {
        let root = test_root("concurrent-leader-acquisition-single-winner");
        let barrier = Arc::new(Barrier::new(2));
        let left_root = root.clone();
        let right_root = root.clone();
        let left_barrier = barrier.clone();
        let right_barrier = barrier.clone();
        let left = std::thread::spawn(move || {
            left_barrier.wait();
            LeaderLeaseStore::new(left_root)
                .try_acquire_or_renew("node-a", 100, 5)
                .unwrap()
        });
        let right = std::thread::spawn(move || {
            right_barrier.wait();
            LeaderLeaseStore::new(right_root)
                .try_acquire_or_renew("node-b", 100, 5)
                .unwrap()
        });

        let outcomes = vec![left.join().unwrap(), right.join().unwrap()];
        let leaders = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, LeaseAcquireOutcome::Leader(_)))
            .count();
        let followers = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, LeaseAcquireOutcome::Follower(_)))
            .count();
        assert_eq!(leaders, 1);
        assert_eq!(followers, 1);
    }

    #[test]
    fn lease_file_lock_released_after_acquisition() {
        let root = test_root("lease-file-lock-released-after-acquisition");
        let store = LeaderLeaseStore::new(&root);
        store.try_acquire_or_renew("node-a", 100, 5).unwrap();

        let lock_path = EnvironmentPaths::leader_lease_file(&root).with_extension("lock");
        let _guard = FileLock::acquire(lock_path).expect("lease lock should be released");
    }

    #[test]
    fn active_lock_holder_diagnostics_include_pid() {
        let root = test_root("active-lock-holder-diagnostics-include-pid");
        let lock_path = EnvironmentPaths::leader_lease_file(&root).with_extension("lock");
        fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
        let _guard = FileLock::acquire(lock_path.clone()).unwrap();

        let err = FileLock::acquire(lock_path).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("timed out acquiring lock"));
        assert!(message.contains(&format!("holder_pid={}", std::process::id())));
    }

    #[test]
    fn stale_legacy_lock_file_is_reclaimed() {
        let root = test_root("stale-legacy-lock-file-is-reclaimed");
        let lock_path = EnvironmentPaths::leader_lease_file(&root).with_extension("lock");
        fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
        fs::write(&lock_path, "locked\n").unwrap();
        thread::sleep(LOCK_STALE_AFTER + Duration::from_millis(50));

        match LeaderLeaseStore::new(&root)
            .try_acquire_or_renew("node-a", 100, 5)
            .unwrap()
        {
            LeaseAcquireOutcome::Leader(lease) => assert_eq!(lease.leader_node_id, "node-a"),
            LeaseAcquireOutcome::Follower(_) => panic!("expected stale legacy lock takeover"),
        }
    }

    #[test]
    fn stale_leader_takeover_does_not_duplicate_epoch() {
        let root = test_root("stale-leader-takeover-does-not-duplicate-epoch");
        let store = LeaderLeaseStore::new(&root);
        store.try_acquire_or_renew("node-a", 100, 5).unwrap();

        let first_takeover = match store.try_acquire_or_renew("node-b", 106, 5).unwrap() {
            LeaseAcquireOutcome::Leader(lease) => lease,
            LeaseAcquireOutcome::Follower(_) => panic!("expected stale lease takeover"),
        };
        let renewed = match store.try_acquire_or_renew("node-b", 107, 5).unwrap() {
            LeaseAcquireOutcome::Leader(lease) => lease,
            LeaseAcquireOutcome::Follower(_) => panic!("expected leader renewal"),
        };

        assert_eq!(first_takeover.lease_epoch, 2);
        assert_eq!(renewed.lease_epoch, first_takeover.lease_epoch);
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

#[cfg(test)]
pub mod runtime_policy_normalization {
    use super::*;

    #[test]
    fn empty_restart_policy_normalizes_to_no() {
        let policy: PersistedRuntimePolicy = serde_json::from_str(
            "{\n  \"cpu_limit\": null,\n  \"memory_limit_mb\": null,\n  \"restart_policy\": \"\"\n}\n",
        )
        .unwrap();

        assert_eq!(policy.restart_policy, "no");
    }
}
