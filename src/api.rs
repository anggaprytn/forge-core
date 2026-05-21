use crate::events::EventRecord;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_failure_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeTargetDiagnostics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecentDeploymentFailure {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    pub generation: u64,
    pub failure_stage: String,
    pub failure_reason: String,
    #[serde(default)]
    pub historical: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_failure_summary: Option<String>,
    pub diagnostics_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentDiagnostics {
    pub project_id: String,
    pub environment: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_deployment_id: Option<String>,
    pub container: ContainerRuntimeDiagnostics,
    pub route: RouteDiagnostics,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_target: Option<ProbeTargetDiagnostics>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentHistoryResponse {
    pub project_id: String,
    pub environment: String,
    #[serde(default)]
    pub entries: Vec<DeploymentHistoryEntry>,
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
