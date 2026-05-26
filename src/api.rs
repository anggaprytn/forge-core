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
pub struct EnvInventoryCell {
    pub exists: bool,
    pub value: String,
    #[serde(default)]
    pub configured_exists: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured_value: Option<String>,
    #[serde(default)]
    pub deployed_exists: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_value: Option<String>,
    #[serde(default)]
    pub pending_next_deploy: bool,
    #[serde(default)]
    pub matches_deployed: bool,
    #[serde(default)]
    pub value_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_deploy_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvInventoryVariable {
    pub key: String,
    pub environments: BTreeMap<String, EnvInventoryCell>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvInventoryEnvironmentSource {
    pub environment: String,
    pub source_kind: String,
    pub source_label: String,
    #[serde(default)]
    pub env_store_revision: u64,
    #[serde(default)]
    pub revision_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured_source_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_source_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvInventoryResponse {
    pub project_id: String,
    pub source_kind: String,
    pub source_label: String,
    pub partial_metadata: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partial_metadata_notice: Option<String>,
    #[serde(default)]
    pub environments: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<String>,
    pub total_variables: usize,
    #[serde(default)]
    pub variables: Vec<EnvInventoryVariable>,
    #[serde(default)]
    pub environment_sources: Vec<EnvInventoryEnvironmentSource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvPreviewChanges {
    #[serde(default)]
    pub development: String,
    #[serde(default)]
    pub staging: String,
    #[serde(default)]
    pub production: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvPreviewRequest {
    pub changes: EnvPreviewChanges,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EnvRevisionVector {
    #[serde(default)]
    pub development: u64,
    #[serde(default)]
    pub staging: u64,
    #[serde(default)]
    pub production: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EnvPreviewHashVector {
    #[serde(default)]
    pub development: String,
    #[serde(default)]
    pub staging: String,
    #[serde(default)]
    pub production: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvApplyRequest {
    pub changes: EnvPreviewChanges,
    pub expected_base_revisions: EnvRevisionVector,
    pub preview_hashes: EnvPreviewHashVector,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvPreviewError {
    pub line: usize,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvPreviewDiffEntry {
    pub key: String,
    pub before_masked: String,
    pub after_masked: String,
    pub action: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvPreviewEnvironmentResponse {
    pub environment: String,
    pub valid: bool,
    #[serde(default)]
    pub base_revision: u64,
    #[serde(default)]
    pub revision_label: String,
    #[serde(default)]
    pub preview_hash: String,
    #[serde(default)]
    pub source_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub added: Vec<EnvPreviewDiffEntry>,
    #[serde(default)]
    pub updated: Vec<EnvPreviewDiffEntry>,
    #[serde(default)]
    pub deleted: Vec<EnvPreviewDiffEntry>,
    #[serde(default)]
    pub unchanged: Vec<EnvPreviewDiffEntry>,
    #[serde(default)]
    pub errors: Vec<EnvPreviewError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvPreviewResponse {
    pub project_id: String,
    pub applies: bool,
    pub message: String,
    pub partial_metadata: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
    #[serde(default)]
    pub environments: Vec<EnvPreviewEnvironmentResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvApplyResponse {
    pub project_id: String,
    pub applied: bool,
    pub status: String,
    pub message: String,
    pub audit_id: String,
    pub env_store_revision_before: EnvRevisionVector,
    pub env_store_revision_after: EnvRevisionVector,
    #[serde(default)]
    pub environments: Vec<EnvApplyEnvironmentResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvApplyEnvironmentResponse {
    pub environment: String,
    pub applied: bool,
    pub valid: bool,
    pub status: String,
    pub message: String,
    #[serde(default)]
    pub env_store_revision_before: u64,
    #[serde(default)]
    pub env_store_revision_after: u64,
    #[serde(default)]
    pub revision_label: String,
    #[serde(default)]
    pub preview_hash: String,
    #[serde(default)]
    pub source_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(default)]
    pub added: Vec<EnvPreviewDiffEntry>,
    #[serde(default)]
    pub updated: Vec<EnvPreviewDiffEntry>,
    #[serde(default)]
    pub deleted: Vec<EnvPreviewDiffEntry>,
    #[serde(default)]
    pub unchanged: Vec<EnvPreviewDiffEntry>,
    #[serde(default)]
    pub errors: Vec<EnvPreviewError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EnvAuditSummary {
    #[serde(default)]
    pub added: usize,
    #[serde(default)]
    pub updated: usize,
    #[serde(default)]
    pub deleted: usize,
    #[serde(default)]
    pub unchanged: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvAuditEntry {
    pub audit_id: String,
    pub project_id: String,
    pub environment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_by: Option<String>,
    pub modified_at_unix: u64,
    pub status: String,
    #[serde(default)]
    pub audit_status_label: String,
    #[serde(default)]
    pub env_store_revision_before: u64,
    #[serde(default)]
    pub env_store_revision_after: u64,
    #[serde(default)]
    pub revision_label: String,
    #[serde(default)]
    pub source_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key_hash: Option<String>,
    pub summary: EnvAuditSummary,
    #[serde(default)]
    pub diff: Vec<EnvPreviewDiffEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvAuditResponse {
    pub project_id: String,
    pub total: usize,
    #[serde(default)]
    pub entries: Vec<EnvAuditEntry>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<ReadinessSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recommendations: Vec<ReadinessRecommendation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadinessSummary {
    #[serde(default)]
    pub active_count: usize,
    #[serde(default)]
    pub cleared_count: usize,
    #[serde(default)]
    pub historical_count: usize,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub highest_severity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_recommendation: Option<ReadinessRecommendation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadinessRecommendation {
    #[serde(default)]
    pub action_id: String,
    #[serde(default)]
    pub severity: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub command_hint: String,
    #[serde(default)]
    pub safe_to_run: bool,
    #[serde(default)]
    pub scope: String,
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
    pub recommendation: Option<ReadinessRecommendation>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<ReadinessSummary>,
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
pub struct GitHubRepositorySummary {
    pub full_name: String,
    pub html_url: String,
    pub clone_url: String,
    pub default_branch: String,
    pub private: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitHubRepoListResponse {
    #[serde(default)]
    pub repositories: Vec<GitHubRepositorySummary>,
    #[serde(default)]
    pub private_repo_authorization_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterProjectFromGitHubPreviewRequest {
    pub repo_url: String,
    pub default_branch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_domain: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterProjectFromGitHubPreviewResponse {
    pub valid: bool,
    pub project_id: String,
    pub repo_url: String,
    pub default_branch: String,
    pub base_domain: String,
    pub domain_source: String,
    pub project_id_status: String,
    pub base_domain_status: String,
    pub environment_routes: ProjectRegistrationRoutePreview,
    #[serde(default)]
    pub project_id_alternatives: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_domain_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_domain_suggestion: Option<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRegistrationRoutePreview {
    pub production: String,
    pub staging: String,
    pub development: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterProjectFromGitHubRequest {
    pub project_id: String,
    pub repo_url: String,
    pub default_branch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_domain: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterProjectFromGitHubResponse {
    pub registered: bool,
    pub project_id: String,
    pub repo_url: String,
    pub default_branch: String,
    pub base_domain: String,
    pub domain_source: String,
    pub environment_routes: ProjectRegistrationRoutePreview,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebDeployPreviewRequest {
    pub environment: String,
    #[serde(rename = "ref")]
    pub git_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebDeployPreviewResponse {
    pub valid: bool,
    pub project_id: String,
    pub environment: String,
    pub repo_url: String,
    #[serde(rename = "ref")]
    pub git_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    pub manifest: WebDeployManifestSummary,
    pub route: WebDeployRouteSummary,
    pub env: WebDeployEnvPreviewSummary,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebDeployManifestSummary {
    pub name: String,
    pub schema_version: u64,
    #[serde(default)]
    pub services: Vec<String>,
    #[serde(default)]
    pub exposed_services: Vec<String>,
    #[serde(default)]
    pub healthchecks: Vec<WebDeployHealthcheckSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebDeployHealthcheckSummary {
    pub service_id: String,
    pub path: String,
    pub expected_status: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebDeployRouteSummary {
    pub domain: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebDeployEnvPreviewSummary {
    pub pending_desired_env: bool,
    pub source: String,
    #[serde(default)]
    pub missing_required_secrets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebDeployRequest {
    pub environment: String,
    #[serde(rename = "ref")]
    pub git_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preflight_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebDeployResponse {
    pub deployment_id: String,
    pub queued: bool,
    pub message: String,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub environments: Vec<ProjectEnvironmentSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectList {
    pub projects: Vec<ProjectRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentSummary {
    pub environment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_deployment_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_summary: Option<ProjectEnvironmentReadinessSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentReadinessSummary {
    pub health_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_successful_convergence_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentInventoryList {
    pub project_id: String,
    #[serde(default)]
    pub environments: Vec<ProjectEnvironmentDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentDetail {
    pub environment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_services: Vec<ProjectEnvironmentServiceSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_deployment_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_deployment_timestamp: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_eligibility: Option<ProjectEnvironmentRollbackSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_lineage: Option<ProjectEnvironmentRestoreSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_policy: Option<ProjectEnvironmentRuntimePolicySummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_summary: Option<ProjectEnvironmentReadinessSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentServiceSummary {
    pub service_id: String,
    pub role: String,
    pub running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentRollbackSummary {
    pub eligible: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_generation: Option<u64>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentRestoreSummary {
    pub backup_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_deployment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored_at_unix: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentRuntimePolicySummary {
    pub restart_policy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<ProjectEnvironmentServiceRuntimePolicySummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentServiceRuntimePolicySummary {
    pub service_id: String,
    pub restart_policy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_mb: Option<u64>,
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
