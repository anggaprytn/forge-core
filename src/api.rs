use crate::events::EventRecord;
use crate::storage::{
    DeploymentLifecycleState, PersistedEnvironmentCheckpoint, PersistedPromotionSummary,
    PersistedRuntimePolicy, PersistedRuntimeUsageSnapshot, PersistedServiceState,
    PersistedTerminationInfo, PersistedValidationSummary,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionRole {
    Current,
    RollbackTarget,
    Retained,
    GcEligible,
}

impl RetentionRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::RollbackTarget => "rollback_target",
            Self::Retained => "retained",
            Self::GcEligible => "gc_eligible",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentRequest {
    pub project_id: String,
    pub environment: String,
    pub intent: String,
    #[serde(default)]
    pub source_path: Option<PathBuf>,
    #[serde(default)]
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentAccepted {
    pub deployment_id: String,
    pub queue_position: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentStatus {
    pub deployment_id: String,
    pub project_id: String,
    pub environment: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentLogs {
    pub deployment_id: String,
    pub project_id: String,
    pub environment: String,
    #[serde(default)]
    pub lines: Vec<String>,
    #[serde(default)]
    pub lifecycle: Vec<String>,
    #[serde(default)]
    pub container_logs: Vec<String>,
    #[serde(default)]
    pub services: Vec<ServiceLogGroup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_failure_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceLogGroup {
    pub service_id: String,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    #[serde(default)]
    pub lines: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeTargetDiagnostics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerRuntimeDiagnostics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    pub running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_policy: Option<PersistedRuntimePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_usage: Option<PersistedRuntimeUsageSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination: Option<PersistedTerminationInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceRuntimeStatus {
    pub service_id: String,
    pub role: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub dns_aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_ref: Option<String>,
    pub running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_state: Option<PersistedServiceState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_path: Option<String>,
    #[serde(default)]
    pub runtime_policy: PersistedRuntimePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_usage: Option<PersistedRuntimeUsageSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination: Option<PersistedTerminationInfo>,
    #[serde(default)]
    pub restart_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit_code: Option<i32>,
    pub route: String,
    pub health: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<VolumeRuntimeStatus>,
    #[serde(default)]
    pub logs_tail: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeRuntimeStatus {
    pub volume_id: String,
    pub docker_volume_name: String,
    pub mount_path: String,
    pub retention: String,
    pub attached: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteDiagnostics {
    pub route_required: bool,
    pub route_active: bool,
    pub matches_expected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mismatch_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeStabilityDiagnostics {
    pub sample_size: usize,
    pub success_rate: f64,
    pub consecutive_success_streak: usize,
    pub recent_failure_count: usize,
    pub flapping_window_summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecentDeploymentFailure {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    pub generation: u64,
    pub failure_stage: String,
    pub failure_reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocking_service_name: Option<String>,
    #[serde(default)]
    pub historical: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_failure_summary: Option<String>,
    pub diagnostics_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretListEntry {
    pub key: String,
    pub value: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
    #[serde(default)]
    pub referenced_by_generations: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretListResponse {
    pub project_id: String,
    pub environment: String,
    #[serde(default)]
    pub secrets: Vec<SecretListEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretUnsetResponse {
    pub secret_id: String,
    pub removed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenMetadata {
    pub token_id: String,
    pub name: String,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<u64>,
    pub github_login: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenListResponse {
    #[serde(default)]
    pub tokens: Vec<TokenMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenCreateRequest {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenCreateResponse {
    pub token: String,
    pub metadata: TokenMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenRevokeResponse {
    pub token_id: String,
    pub revoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeVersionOutput {
    pub version: String,
    pub git_commit: String,
    pub git_dirty: String,
    pub build_timestamp: String,
    pub target_triple: String,
    pub schema_versions: ForgeSchemaVersions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeSchemaVersions {
    pub manifest_schema: u64,
    pub snapshot_schema: u64,
    pub checkpoint_schema: u64,
    pub reconciliation_log_schema: u64,
    pub storage_compatibility: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentDiffEntry {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentValueChange {
    pub key: String,
    pub before: String,
    pub after: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretReferenceChange {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_reference: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_reference: Option<String>,
    pub before: String,
    pub after: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentDiffResponse {
    pub project_id: String,
    pub environment: String,
    pub from_generation: u64,
    pub to_generation: u64,
    #[serde(default)]
    pub added: Vec<EnvironmentDiffEntry>,
    #[serde(default)]
    pub removed: Vec<EnvironmentDiffEntry>,
    #[serde(default)]
    pub changed_values: Vec<EnvironmentValueChange>,
    #[serde(default)]
    pub changed_secret_references: Vec<SecretReferenceChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentDiffSummary {
    pub from_generation: u64,
    pub to_generation: u64,
    pub added: usize,
    pub removed: usize,
    pub changed_values: usize,
    pub changed_secret_references: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretMutationDiagnostic {
    pub key: String,
    pub mutation: String,
    pub updated_at_unix: u64,
    pub active_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentDiagnostics {
    pub project_id: String,
    pub environment: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_deployment_id: Option<String>,
    #[serde(default)]
    pub container: ContainerRuntimeDiagnostics,
    #[serde(default)]
    pub route: RouteDiagnostics,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_target: Option<ProbeTargetDiagnostics>,
    #[serde(default)]
    pub startup_order: Vec<String>,
    #[serde(default)]
    pub services: Vec<ServiceRuntimeStatus>,
    #[serde(default)]
    pub recent_failures: Vec<RecentDeploymentFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_validation_failure: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_route_activation_failure: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub likely_failure_stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_env_snapshot: Option<RuntimeEnvSnapshotMetadata>,
    #[serde(default)]
    pub retained_generations: Vec<DeploymentHistoryEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_safe_generation: Option<u64>,
    #[serde(default)]
    pub recent_gc_actions: Vec<RecentGcAction>,
    #[serde(default)]
    pub missing_required_secrets: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_drift: Option<EnvironmentDiffSummary>,
    #[serde(default)]
    pub recent_secret_mutations: Vec<SecretMutationDiagnostic>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub orphaned_state_warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volume_repair_events: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_volume_repair_events: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub historical_volume_repair_events: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_lifecycle_state: Option<DeploymentLifecycleState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_role: Option<RetentionRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_summary: Option<PersistedValidationSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_summary: Option<PersistedPromotionSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failed_transition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_gate_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warmup_failure_summary: Option<String>,
    #[serde(default)]
    pub restart_instability: bool,
    #[serde(default)]
    pub probe_flapping: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_stability: Option<ProbeStabilityDiagnostics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_restore: Option<RestoreLineage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub state_restore_warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backup_restore_events: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_upgrade_events: Vec<crate::upgrade::UpgradeEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_drift_repairs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_policy_drift_repairs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub historical_policy_drift_repairs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_checkpoint: Option<PersistedEnvironmentCheckpoint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domain_summaries: Vec<ConvergenceDomainSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeInfo>,
    #[serde(default)]
    pub cluster: ClusterDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConvergenceDomainSummary {
    pub domain: String,
    pub status: String,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NodeInfo {
    pub node_id: String,
    pub booted_at_unix: u64,
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEnvSnapshotMetadata {
    pub generation: u64,
    pub deployment_id: String,
    pub source_environment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    pub total_keys: usize,
    #[serde(default)]
    pub secret_backed_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub generated_forge_vars: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentVariableValue {
    pub key: String,
    pub value: String,
    pub source: String,
    pub generated: bool,
    pub redacted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentVariableReport {
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
    #[serde(default)]
    pub values: Vec<EnvironmentVariableValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventList {
    pub events: Vec<EventRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentHistoryEntry {
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
    pub created_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promoted_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalized_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalized_at_unix: Option<u64>,
    #[serde(default)]
    pub rollback_target: bool,
    #[serde(default)]
    pub restored_by_rollback: bool,
    #[serde(default)]
    pub retained: bool,
    #[serde(default)]
    pub eligible_for_gc: bool,
    #[serde(default)]
    pub missing_artifacts: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained_reasons: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_state: Option<DeploymentLifecycleState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_role: Option<RetentionRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entered_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_summary: Option<PersistedValidationSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_summary: Option<PersistedPromotionSummary>,
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
pub struct DeploymentHistoryResponse {
    pub project_id: String,
    pub environment: String,
    #[serde(default)]
    pub entries: Vec<DeploymentHistoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupArchiveFileRecord {
    pub path: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupVolumeRecord {
    pub volume_id: String,
    pub docker_volume_name: String,
    pub service_id: String,
    pub mount_path: String,
    pub archive_file: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub archive_files: Vec<BackupArchiveFileRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored_docker_volume_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupHookRecord {
    pub service_id: String,
    pub volume_id: String,
    pub container_name: String,
    pub pre_backup_command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix: Option<u64>,
    pub timeout_seconds: u64,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreRecord {
    pub restored_generation: u64,
    pub restored_deployment_id: String,
    pub restored_at_unix: u64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupRecord {
    pub backup_id: String,
    pub project_id: String,
    pub environment: String,
    pub created_at_unix: u64,
    pub source_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_deployment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<BackupVolumeRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hooks: Vec<BackupHookRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub restores: Vec<RestoreRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupListResponse {
    pub project_id: String,
    pub environment: String,
    #[serde(default)]
    pub backups: Vec<BackupRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupRestoreResponse {
    pub backup_id: String,
    pub restored_generation: u64,
    pub restored_deployment_id: String,
    pub restored_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyzReason {
    pub project_id: String,
    pub environment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub unresolved: bool,
    pub source: String,
    pub marker: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checked_unix: Option<u64>,
    #[serde(default)]
    pub cache_age_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<ReadyzReasonDiagnostics>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadyzReasonDiagnostics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stall_threshold_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startup_phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_in_progress: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_convergence_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyzResponse {
    pub status: String,
    #[serde(default)]
    pub startup_phase: String,
    #[serde(default)]
    pub active_failure: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<ReadyzReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DependencyBreakerDiagnostics {
    pub state: String,
    pub failure_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetricsDependencySnapshot {
    pub probe_latency_ms: u64,
    pub breaker: DependencyBreakerDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClusterNodeStatus {
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
pub struct ClusterDiagnostics {
    #[serde(default)]
    pub observed_nodes: usize,
    #[serde(default)]
    pub active_reconcilers: usize,
    #[serde(default)]
    pub lease_epoch_divergence: bool,
    #[serde(default)]
    pub split_brain_suspected: bool,
    #[serde(default)]
    pub cluster_size: usize,
    #[serde(default)]
    pub local_role: String,
    #[serde(default)]
    pub heartbeat_age_ms: u64,
    #[serde(default)]
    pub multiple_active_reconcilers: bool,
    #[serde(default)]
    pub checkpoint_owner_mismatch: bool,
    #[serde(default)]
    pub snapshot_owner_mismatch: bool,
    #[serde(default)]
    pub stale_reconciler: bool,
    #[serde(default)]
    pub reconciliation_blocked: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub degraded_markers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<ClusterNodeStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetricsResponse {
    pub queue_depth: usize,
    #[serde(default)]
    pub readiness_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_reason: Option<String>,
    #[serde(default)]
    pub startup_phase: String,
    #[serde(default)]
    pub startup_recovery_duration_ms: u64,
    pub convergence_loop_duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_last_success_unix: Option<u64>,
    #[serde(default)]
    pub convergence_active_failure: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_active_failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_last_failure_historical_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_last_failure_unix: Option<u64>,
    pub convergence_failures_total: u64,
    pub readiness_cache_age_ms: u64,
    pub readyz_requests_total: u64,
    pub readyz_latency_ms: u64,
    pub readyz_degraded_total: u64,
    pub docker_probe_latency_ms: u64,
    pub caddy_probe_latency_ms: u64,
    #[serde(default)]
    pub leader: bool,
    #[serde(default)]
    pub lease_epoch: u64,
    #[serde(default)]
    pub lease_age_ms: u64,
    #[serde(default)]
    pub lease_expiry_ms: u64,
    #[serde(default)]
    pub convergence_owner: String,
    #[serde(default)]
    pub reconciliation_enabled: bool,
    #[serde(default)]
    pub follower_mode: bool,
    #[serde(default)]
    pub pending_intents: usize,
    #[serde(default)]
    pub replay_queue_depth: usize,
    #[serde(default)]
    pub replay_in_progress: bool,
    #[serde(default)]
    pub replay_paused: bool,
    #[serde(default)]
    pub replay_duration_ms: u64,
    #[serde(default)]
    pub replay_failures_total: u64,
    #[serde(default)]
    pub replay_quarantined_total: u64,
    #[serde(default)]
    pub replay_aborted_total: u64,
    #[serde(default)]
    pub lease_fencing_failures: u64,
    #[serde(default)]
    pub unrecoverable_operations: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_replayed_intent: Option<String>,
    #[serde(default)]
    pub reconciliation_log_size_bytes: u64,
    #[serde(default)]
    pub convergence_start_blocked: bool,
    pub docker: MetricsDependencySnapshot,
    pub caddy: MetricsDependencySnapshot,
    #[serde(default)]
    pub cluster: ClusterDiagnostics,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub convergence_domains: Vec<ConvergenceDomainSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadinessExplainResponse {
    #[serde(default = "default_readiness_explain_source")]
    pub source: String,
    #[serde(default = "default_readiness_explain_live")]
    pub live: bool,
    pub taxonomy: String,
    pub readiness_status: String,
    pub startup_phase: String,
    pub active_failure: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_failure_reason: Option<String>,
    pub failure_scope: String,
    #[serde(default)]
    pub historical_failures: bool,
    #[serde(default)]
    pub convergence_blocked: bool,
    #[serde(default)]
    pub replay_running: bool,
    #[serde(default)]
    pub leader: bool,
    #[serde(default)]
    pub follower_mode: bool,
    pub node_role: String,
    #[serde(default)]
    pub leadership_healthy: bool,
    pub leadership_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_successful_convergence_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_historical_failure_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_updated_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_age_ms: Option<u64>,
    #[serde(default = "default_readiness_explain_confidence")]
    pub confidence: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
    pub operator_interpretation: String,
    pub safe_next_action: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadinessTimelineRelatedFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_start_blocked: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_in_progress: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follower_mode: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_verification_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filesystem_scan_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadinessTimelineEntry {
    #[serde(default)]
    pub timestamp_unix: u64,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub blocker_type: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub startup_phase: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub active_failure: bool,
    #[serde(default)]
    pub suggested_action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_fields: Option<ReadinessTimelineRelatedFields>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadinessTimelineResponse {
    #[serde(default = "default_readiness_explain_source")]
    pub source: String,
    #[serde(default = "default_readiness_explain_live")]
    pub live: bool,
    #[serde(default)]
    pub generated_at_unix: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<ReadinessTimelineEntry>,
}

fn default_readiness_explain_source() -> String {
    "daemon_api".into()
}

fn default_readiness_explain_live() -> bool {
    true
}

fn default_readiness_explain_confidence() -> String {
    "high".into()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreLineage {
    pub backup_id: String,
    pub restored_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_deployment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook_succeeded: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub restored_volumes: Vec<BackupVolumeRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecentGcAction {
    pub timestamp_unix: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    pub action: String,
    pub reason: String,
    pub outcome: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default)]
    pub deleted: Vec<String>,
    #[serde(default)]
    pub protected: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectUpsertRequest {
    #[serde(default)]
    pub project_id: Option<String>,
    pub repo_url: String,
    pub default_branch: String,
    #[serde(default)]
    pub base_domain: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub project_id: String,
    pub repo_url: String,
    pub default_branch: String,
    pub base_domain: String,
    pub domain_mode: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectList {
    pub projects: Vec<ProjectRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliLoginStartResponse {
    pub code: String,
    pub expires_at_unix: u64,
    pub poll_interval_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliLoginPollRequest {
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliLoginPollResponse {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

pub fn validate_deployment_request(request: &DeploymentRequest) -> Result<(), ErrorResponse> {
    if request.project_id.is_empty() {
        return Err(ErrorResponse {
            code: "invalid_project_id".into(),
            message: "project_id must not be empty".into(),
        });
    }

    if !matches!(
        request.environment.as_str(),
        "development" | "staging" | "production"
    ) {
        return Err(ErrorResponse {
            code: "invalid_environment".into(),
            message: "environment must be one of development, staging, production".into(),
        });
    }

    if !matches!(request.intent.as_str(), "deploy" | "redeploy" | "rollback") {
        return Err(ErrorResponse {
            code: "invalid_intent".into(),
            message: "intent must be one of deploy, redeploy, rollback".into(),
        });
    }

    if request
        .source_path
        .as_ref()
        .is_some_and(|path| path.as_os_str().is_empty())
    {
        return Err(ErrorResponse {
            code: "invalid_source_path".into(),
            message: "source_path must not be empty".into(),
        });
    }

    if request
        .source_ref
        .as_ref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(ErrorResponse {
            code: "invalid_source_ref".into(),
            message: "source_ref must not be empty".into(),
        });
    }

    if request.source_path.is_some() && request.source_ref.is_some() {
        return Err(ErrorResponse {
            code: "invalid_source".into(),
            message: "source_path and source_ref are mutually exclusive".into(),
        });
    }

    if request.intent == "rollback"
        && (request.source_path.is_some() || request.source_ref.is_some())
    {
        return Err(ErrorResponse {
            code: "invalid_source".into(),
            message: "source_path and source_ref are only supported for deploy intents".into(),
        });
    }

    Ok(())
}
