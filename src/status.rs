use std::fmt::{Display, Formatter};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::{
    ContainerRuntimeDiagnostics, EnvironmentDiagnostics, EnvironmentVariableReport,
    EnvironmentVariableValue, ErrorResponse, ProbeTargetDiagnostics, RecentDeploymentFailure,
    RouteDiagnostics, RuntimeEnvSnapshotMetadata,
};
use crate::projects::ProjectRegistryStore;
use crate::queue::{PersistentQueue, QueueError};
use crate::runtime::{
    ContainerInspection, DockerRuntime, DockerRuntimeError, RouteInspection, RoutingRuntime,
    RoutingRuntimeError,
};
use crate::runtime_env::{GENERATED_FORGE_ENV_KEYS, render_snapshot_value};
use crate::storage::{
    DiagnosticsStore, EnvironmentPaths, PersistedActivationMode, PersistedRouteTargetSource,
    PersistedRuntimeInfo, PointerStore, RuntimeStateStore, StorageError,
    load_generation_build_info, load_generation_runtime_env_snapshot, load_generation_runtime_info,
    load_generation_snapshot_metadata,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentStatus {
    pub project_id: String,
    pub environment: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_generation: Option<u64>,
    pub domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    pub container_running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_ip: Option<String>,
    pub route_active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_deployment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_env_snapshot: Option<RuntimeEnvSnapshotMetadata>,
}

#[derive(Debug)]
pub enum ProjectStatusError {
    Storage(StorageError),
    Queue(QueueError),
    Routing(RoutingRuntimeError),
    Docker(DockerRuntimeError),
    ProjectLookup(String),
    ProjectNotFound,
    InvalidEnvironment,
    RuntimeEnvSnapshotUnavailable,
}

impl Display for ProjectStatusError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "{err}"),
            Self::Queue(err) => write!(f, "{err}"),
            Self::Routing(err) => write!(f, "{err}"),
            Self::Docker(err) => write!(f, "{err}"),
            Self::ProjectLookup(message) => write!(f, "{message}"),
            Self::ProjectNotFound => write!(f, "project not found"),
            Self::InvalidEnvironment => {
                write!(
                    f,
                    "environment must be one of development, staging, production"
                )
            }
            Self::RuntimeEnvSnapshotUnavailable => write!(f, "runtime env snapshot unavailable"),
        }
    }
}

impl std::error::Error for ProjectStatusError {}

impl From<StorageError> for ProjectStatusError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<std::io::Error> for ProjectStatusError {
    fn from(value: std::io::Error) -> Self {
        Self::Storage(StorageError::Io(value))
    }
}

impl From<QueueError> for ProjectStatusError {
    fn from(value: QueueError) -> Self {
        Self::Queue(value)
    }
}

impl From<RoutingRuntimeError> for ProjectStatusError {
    fn from(value: RoutingRuntimeError) -> Self {
        Self::Routing(value)
    }
}

impl From<DockerRuntimeError> for ProjectStatusError {
    fn from(value: DockerRuntimeError) -> Self {
        Self::Docker(value)
    }
}

pub fn derive_environment_domain(base_domain: &str, environment: &str) -> String {
    match environment {
        "production" => base_domain.to_string(),
        "staging" => format!("staging-{base_domain}"),
        "development" => format!("development-{base_domain}"),
        other => format!("{other}-{base_domain}"),
    }
}

pub fn route_subtree_id(project_id: &str, environment: &str) -> String {
    format!("forge:{project_id}:{environment}")
}

pub fn project_status_error_response(
    err: ProjectStatusError,
) -> (axum::http::StatusCode, ErrorResponse) {
    match err {
        ProjectStatusError::ProjectNotFound => (
            axum::http::StatusCode::NOT_FOUND,
            ErrorResponse {
                code: "project_not_found".into(),
                message: "project not found".into(),
            },
        ),
        ProjectStatusError::InvalidEnvironment => (
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse {
                code: "invalid_environment".into(),
                message: "environment must be one of development, staging, production".into(),
            },
        ),
        ProjectStatusError::RuntimeEnvSnapshotUnavailable => (
            axum::http::StatusCode::NOT_FOUND,
            ErrorResponse {
                code: "runtime_env_snapshot_unavailable".into(),
                message: "runtime env snapshot unavailable".into(),
            },
        ),
        other => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorResponse {
                code: "project_status_unavailable".into(),
                message: other.to_string(),
            },
        ),
    }
}

pub fn load_project_environment_status<D, R>(
    storage_root: &Path,
    queue: Option<&PersistentQueue>,
    docker: &mut D,
    routing: &mut R,
    project_id: &str,
    environment: &str,
) -> Result<ProjectEnvironmentStatus, ProjectStatusError>
where
    D: DockerRuntime,
    R: RoutingRuntime,
{
    if !matches!(environment, "development" | "staging" | "production") {
        return Err(ProjectStatusError::InvalidEnvironment);
    }

    let project = ProjectRegistryStore::new(storage_root)
        .get(project_id)
        .map_err(|err| {
            ProjectStatusError::ProjectLookup(format!(
                "project lookup failed for {project_id}: {err}"
            ))
        })?
        .ok_or(ProjectStatusError::ProjectNotFound)?;
    let domain = derive_environment_domain(&project.base_domain, environment);

    let env = EnvironmentPaths::new(storage_root, project_id, environment);
    env.ensure_exists()?;
    let current_generation = PointerStore::new(env.clone()).read_pointer("current")?;
    let runtime_state = RuntimeStateStore::new(env.clone()).load()?;
    let active_generation = current_generation.or(runtime_state.active_generation);
    let latest_generation = latest_generation(&env)?;

    let promoted_snapshot = current_generation
        .map(|generation| load_generation_snapshot_metadata(&env, generation))
        .transpose()?
        .flatten();
    let promoted_runtime = active_generation
        .map(|generation| load_generation_runtime_info(&env, generation))
        .transpose()?
        .flatten();
    let promoted_build = active_generation
        .map(|generation| load_generation_build_info(&env, generation))
        .transpose()?
        .flatten();

    let latest_snapshot = latest_generation
        .map(|generation| load_generation_snapshot_metadata(&env, generation))
        .transpose()?
        .flatten();
    let latest_build = latest_generation
        .map(|generation| load_generation_build_info(&env, generation))
        .transpose()?
        .flatten();
    let promoted_runtime_env_snapshot = active_generation
        .map(|generation| load_generation_runtime_env_snapshot(&env, generation))
        .transpose()?
        .flatten();

    let deploying = queue
        .map(|queue| queue.load_state())
        .transpose()?
        .is_some_and(|state| {
            state.active.as_ref().is_some_and(|record| {
                record.project_id == project_id && record.environment == environment
            }) || state
                .queued
                .iter()
                .any(|record| record.project_id == project_id && record.environment == environment)
        });

    let container_name = promoted_runtime
        .as_ref()
        .map(|runtime| runtime.container_name.clone());
    let container_inspection = inspect_promoted_container(docker, promoted_runtime.as_ref());
    let container_running = container_inspection
        .as_ref()
        .is_some_and(|inspection| inspection.running);
    let container_status = container_inspection
        .as_ref()
        .map(|inspection| inspection.state_status.clone());
    let container_started_at = container_inspection
        .as_ref()
        .and_then(|inspection| inspection.started_at.clone());
    let network_name =
        select_network_name(promoted_runtime.as_ref(), container_inspection.as_ref());
    let container_ip = network_name
        .as_deref()
        .and_then(|network| {
            container_inspection
                .as_ref()
                .and_then(|inspection| inspection.network_ips.get(network).cloned())
        })
        .or_else(|| {
            container_inspection
                .as_ref()
                .and_then(|inspection| inspection.network_ips.values().next().cloned())
        });
    let image_ref = container_inspection
        .as_ref()
        .map(|inspection| inspection.image_ref.clone())
        .or_else(|| promoted_build.as_ref().map(|build| build.image_ref.clone()));

    let route_details = inspect_route_status(
        routing,
        project_id,
        environment,
        &domain,
        promoted_runtime.as_ref(),
        container_inspection.as_ref(),
        network_name.as_deref(),
    );

    let route_active = route_details
        .as_ref()
        .and_then(|details| details.inspection.as_ref())
        .is_some();
    let route_matches = route_details
        .as_ref()
        .is_some_and(RouteStatusDetails::matches_truth);
    let route_required = promoted_runtime
        .as_ref()
        .and_then(|runtime| runtime.activation.as_ref())
        .is_some_and(|activation| matches!(activation, PersistedActivationMode::Http { .. }));
    let runtime_matches_promoted = current_generation == active_generation;
    let promoted_snapshot_healthy = promoted_snapshot
        .as_ref()
        .is_some_and(|snapshot| snapshot.state == "healthy");

    let status = if deploying {
        "deploying"
    } else if current_generation.is_some()
        && promoted_snapshot_healthy
        && container_running
        && runtime_matches_promoted
        && (!route_required || route_matches)
    {
        "healthy"
    } else if current_generation.is_none()
        && latest_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.state == "failed")
    {
        "failed"
    } else if current_generation.is_none()
        && active_generation.is_none()
        && latest_snapshot.is_none()
        && promoted_runtime.is_none()
        && promoted_build.is_none()
    {
        "missing"
    } else {
        "degraded"
    };

    Ok(ProjectEnvironmentStatus {
        project_id: project_id.to_string(),
        environment: environment.to_string(),
        status: status.into(),
        active_generation,
        domain,
        commit_sha: promoted_build
            .as_ref()
            .and_then(|build| build.commit_sha.clone())
            .or_else(|| {
                promoted_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.commit_sha.clone())
            }),
        source_ref: promoted_build
            .as_ref()
            .and_then(|build| build.source_ref.clone())
            .or_else(|| {
                promoted_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.source_ref.clone())
            }),
        container_name,
        container_running,
        container_status,
        network_name,
        container_ip,
        route_active,
        probe_path: promoted_runtime
            .as_ref()
            .and_then(|runtime| runtime.probe_path.clone()),
        image_ref,
        last_deployment_id: promoted_build
            .as_ref()
            .map(|build| build.deployment_id.clone())
            .or_else(|| {
                latest_build
                    .as_ref()
                    .map(|build| build.deployment_id.clone())
            }),
        deployed_at_unix: promoted_snapshot
            .as_ref()
            .map(|snapshot| snapshot.finalized_at_unix)
            .or_else(|| {
                latest_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.finalized_at_unix)
            }),
        container_started_at,
        runtime_env_snapshot: promoted_runtime_env_snapshot
            .as_ref()
            .map(runtime_env_snapshot_metadata),
    })
}

pub fn load_environment_diagnostics<D, R>(
    storage_root: &Path,
    queue: Option<&PersistentQueue>,
    docker: &mut D,
    routing: &mut R,
    project_id: &str,
    environment: &str,
) -> Result<EnvironmentDiagnostics, ProjectStatusError>
where
    D: DockerRuntime,
    R: RoutingRuntime,
{
    let status = load_project_environment_status(
        storage_root,
        queue,
        docker,
        routing,
        project_id,
        environment,
    )?;
    let env = EnvironmentPaths::new(storage_root, project_id, environment);
    let current_generation = PointerStore::new(env.clone()).read_pointer("current")?;
    let runtime_state = RuntimeStateStore::new(env.clone()).load()?;
    let active_generation = current_generation.or(runtime_state.active_generation);
    let promoted_runtime = active_generation
        .map(|generation| load_generation_runtime_info(&env, generation))
        .transpose()?
        .flatten();
    let promoted_runtime_env_snapshot = active_generation
        .map(|generation| load_generation_runtime_env_snapshot(&env, generation))
        .transpose()?
        .flatten();
    let container_inspection = inspect_promoted_container(docker, promoted_runtime.as_ref());
    let network_name =
        select_network_name(promoted_runtime.as_ref(), container_inspection.as_ref());
    let route_details = inspect_route_status(
        routing,
        project_id,
        environment,
        &status.domain,
        promoted_runtime.as_ref(),
        container_inspection.as_ref(),
        network_name.as_deref(),
    );

    let recent_failure_generations = list_recent_failure_generations(&env)?;
    let latest_failed_generation = recent_failure_generations.first().copied();
    let latest_failure = latest_failed_generation
        .map(|generation| load_failure_details_internal(&env, generation))
        .transpose()?
        .flatten();
    let recent_failures = recent_failure_generations
        .into_iter()
        .map(|generation| load_failure_details(&env, generation))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    let probe_target = latest_failure
        .as_ref()
        .and_then(|failure| failure.probe_target.clone())
        .or_else(|| {
            promoted_runtime
                .as_ref()
                .map(|runtime| ProbeTargetDiagnostics {
                    host: status.container_ip.clone(),
                    port: activation_port(runtime.activation.as_ref()),
                    path: runtime.probe_path.clone(),
                })
        });

    let route = if let Some(details) = route_details.as_ref() {
        RouteDiagnostics {
            route_required: details.route_required(),
            route_active: details.inspection.is_some(),
            matches_expected: details.matches_truth(),
            current_target: details
                .inspection
                .as_ref()
                .map(|inspection| inspection.active_target.clone()),
            expected_target: details.expected_target.clone(),
            domain: Some(details.expected_domain.clone()),
            mismatch_reason: details.mismatch_reason(),
        }
    } else {
        RouteDiagnostics {
            route_required: false,
            route_active: false,
            matches_expected: true,
            current_target: None,
            expected_target: None,
            domain: Some(status.domain.clone()),
            mismatch_reason: None,
        }
    };

    let status_value = status.status.clone();
    Ok(EnvironmentDiagnostics {
        project_id: project_id.to_string(),
        environment: environment.to_string(),
        status: status.status,
        active_generation,
        last_deployment_id: status.last_deployment_id,
        container: ContainerRuntimeDiagnostics {
            container_name: status.container_name,
            running: status.container_running,
            state_status: status.container_status,
            image_ref: status.image_ref,
            network_name,
            container_ip: status.container_ip,
            started_at: status.container_started_at,
        },
        route,
        probe_target,
        recent_failures,
        latest_validation_failure: latest_failure
            .as_ref()
            .and_then(|failure| failure.validation_failure.clone()),
        latest_route_activation_failure: latest_failure
            .as_ref()
            .and_then(|failure| failure.route_activation_failure.clone()),
        likely_failure_stage: latest_failure
            .as_ref()
            .map(|failure| failure.failure_stage.clone())
            .or_else(|| {
                if status_value == "degraded" {
                    Some("runtime".into())
                } else {
                    None
                }
            }),
        diagnostics_source: latest_failure.map(|failure| failure.diagnostics_source),
        runtime_env_snapshot: promoted_runtime_env_snapshot
            .as_ref()
            .map(runtime_env_snapshot_metadata),
    })
}

pub fn load_project_environment_env_report(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
) -> Result<EnvironmentVariableReport, ProjectStatusError> {
    if !matches!(environment, "development" | "staging" | "production") {
        return Err(ProjectStatusError::InvalidEnvironment);
    }

    ProjectRegistryStore::new(storage_root)
        .get(project_id)
        .map_err(|err| {
            ProjectStatusError::ProjectLookup(format!(
                "project lookup failed for {project_id}: {err}"
            ))
        })?
        .ok_or(ProjectStatusError::ProjectNotFound)?;

    let env = EnvironmentPaths::new(storage_root, project_id, environment);
    env.ensure_exists()?;
    let current_generation = PointerStore::new(env.clone()).read_pointer("current")?;
    let runtime_state = RuntimeStateStore::new(env.clone()).load()?;
    let generation = current_generation
        .or(runtime_state.active_generation)
        .ok_or(ProjectStatusError::RuntimeEnvSnapshotUnavailable)?;
    let snapshot = load_generation_runtime_env_snapshot(&env, generation)?
        .ok_or(ProjectStatusError::RuntimeEnvSnapshotUnavailable)?;
    let values = snapshot
        .entries
        .iter()
        .map(|(key, entry)| EnvironmentVariableValue {
            key: key.clone(),
            value: render_snapshot_value(entry),
            source: runtime_env_source_name(&entry.source).to_string(),
            generated: GENERATED_FORGE_ENV_KEYS.contains(&key.as_str()),
            redacted: entry.redacted,
        })
        .collect();

    Ok(EnvironmentVariableReport {
        project_id: snapshot.project_id,
        environment: snapshot.environment,
        generation: snapshot.generation,
        deployment_id: snapshot.deployment_id,
        source_environment: snapshot.source_environment,
        source_ref: snapshot.source_ref,
        commit_sha: snapshot.commit_sha,
        domain: snapshot.domain,
        values,
    })
}

fn latest_generation(env: &EnvironmentPaths) -> Result<Option<u64>, ProjectStatusError> {
    let generations_dir = env.generations_dir();
    if !generations_dir.exists() {
        return Ok(None);
    }

    let mut latest = None;
    for entry in fs::read_dir(generations_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(generation) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse().ok())
        else {
            continue;
        };
        if latest.is_none_or(|current| generation > current) {
            latest = Some(generation);
        }
    }
    Ok(latest)
}

fn list_generation_numbers(env: &EnvironmentPaths) -> Result<Vec<u64>, ProjectStatusError> {
    let generations_dir = env.generations_dir();
    if !generations_dir.exists() {
        return Ok(Vec::new());
    }

    let mut generations = Vec::new();
    for entry in fs::read_dir(generations_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(generation) = entry.file_name().to_string_lossy().parse::<u64>().ok() else {
            continue;
        };
        generations.push(generation);
    }
    generations.sort_unstable();
    Ok(generations)
}

fn list_recent_failure_generations(env: &EnvironmentPaths) -> Result<Vec<u64>, ProjectStatusError> {
    let mut failures = Vec::new();
    for generation in list_generation_numbers(env)?.into_iter().rev() {
        let diagnostics = DiagnosticsStore::new(env.clone(), generation);
        let Some(summary) = diagnostics.read_summary()? else {
            continue;
        };
        failures.push((generation, summary));
        if failures.len() >= 5 {
            break;
        }
    }
    Ok(failures
        .into_iter()
        .map(|(generation, _)| generation)
        .collect())
}

#[derive(Debug, Clone)]
struct FailureDetails {
    failure_stage: String,
    diagnostics_source: String,
    probe_target: Option<ProbeTargetDiagnostics>,
    validation_failure: Option<Value>,
    route_activation_failure: Option<Value>,
    rendered_summary: RecentDeploymentFailure,
}

fn load_failure_details(
    env: &EnvironmentPaths,
    generation: u64,
) -> Result<Option<RecentDeploymentFailure>, ProjectStatusError> {
    Ok(load_failure_details_internal(env, generation)?.map(|failure| failure.rendered_summary))
}

fn load_failure_details_internal(
    env: &EnvironmentPaths,
    generation: u64,
) -> Result<Option<FailureDetails>, ProjectStatusError> {
    let diagnostics = DiagnosticsStore::new(env.clone(), generation);
    let Some(summary) = diagnostics.read_summary()? else {
        return Ok(None);
    };
    let validation_failure = diagnostics.read_json_artifact::<Value>("validation_failure.json")?;
    let route_activation_failure =
        diagnostics.read_json_artifact::<Value>("route_activation_failure.json")?;
    let diagnostics_source = diagnostics_dir_source(env, generation);
    let validation_failure_summary = validation_failure
        .as_ref()
        .and_then(validation_failure_summary);
    Ok(Some(FailureDetails {
        failure_stage: summary.failure_stage.clone(),
        diagnostics_source: diagnostics_source.clone(),
        probe_target: Some(ProbeTargetDiagnostics {
            host: summary.probe_target_host.clone(),
            port: summary.probe_target_port,
            path: summary.probe_target_path.clone(),
        })
        .filter(|target| target.host.is_some() || target.port.is_some() || target.path.is_some()),
        validation_failure: validation_failure.clone(),
        route_activation_failure,
        rendered_summary: RecentDeploymentFailure {
            deployment_id: summary.deployment_id,
            generation,
            failure_stage: summary.failure_stage,
            failure_reason: summary.failure_reason,
            validation_failure_summary,
            diagnostics_source,
        },
    }))
}

fn diagnostics_dir_source(env: &EnvironmentPaths, generation: u64) -> String {
    format!(
        "projects/{}/environments/{}/generations/{generation}/diagnostics",
        env.root
            .parent()
            .and_then(|path| path.parent())
            .and_then(|path| path.file_name())
            .and_then(|name| name.to_str())
            .unwrap_or_default(),
        env.root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
    )
}

fn runtime_env_snapshot_metadata(
    snapshot: &crate::storage::PersistedRuntimeEnvSnapshot,
) -> RuntimeEnvSnapshotMetadata {
    RuntimeEnvSnapshotMetadata {
        generation: snapshot.generation,
        deployment_id: snapshot.deployment_id.clone(),
        source_environment: snapshot.source_environment.clone(),
        source_ref: snapshot.source_ref.clone(),
        commit_sha: snapshot.commit_sha.clone(),
        domain: snapshot.domain.clone(),
        total_keys: snapshot.entries.len(),
        secret_backed_keys: snapshot
            .entries
            .iter()
            .filter(|(_, entry)| entry.redacted)
            .map(|(key, _)| key.clone())
            .collect(),
        generated_forge_vars: snapshot
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                GENERATED_FORGE_ENV_KEYS
                    .contains(&key.as_str())
                    .then(|| (key.clone(), render_snapshot_value(entry)))
            })
            .collect(),
    }
}

fn runtime_env_source_name(source: &crate::storage::PersistedRuntimeEnvSource) -> &'static str {
    match source {
        crate::storage::PersistedRuntimeEnvSource::ForgeYaml => "forge_yml",
        crate::storage::PersistedRuntimeEnvSource::ProjectEnvironmentSecret => {
            "project_environment_secret"
        }
        crate::storage::PersistedRuntimeEnvSource::DeployTimeOverride => "deploy_time_override",
        crate::storage::PersistedRuntimeEnvSource::ForgeGenerated => "forge_generated",
        crate::storage::PersistedRuntimeEnvSource::SystemRuntimeReserved => {
            "system_runtime_reserved"
        }
    }
}

fn validation_failure_summary(value: &Value) -> Option<String> {
    let probe = value.get("probe_target")?;
    let host = probe
        .get("host")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let port = probe
        .get("port")
        .and_then(Value::as_u64)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".into());
    let path = probe.get("path").and_then(Value::as_str);
    let last_error = value
        .get("last_error")
        .and_then(Value::as_str)
        .unwrap_or("validation failed");
    Some(match path {
        Some(path) => format!("{last_error} ({host}:{port}{path})"),
        None => format!("{last_error} ({host}:{port})"),
    })
}

fn activation_port(activation: Option<&PersistedActivationMode>) -> Option<u16> {
    match activation {
        Some(PersistedActivationMode::Http { internal_port, .. }) => Some(*internal_port),
        _ => None,
    }
}

fn inspect_promoted_container<D: DockerRuntime>(
    docker: &mut D,
    runtime: Option<&PersistedRuntimeInfo>,
) -> Option<ContainerInspection> {
    let container_name = runtime.map(|runtime| runtime.container_name.as_str())?;
    docker.inspect_container(container_name).ok()
}

fn select_network_name(
    runtime: Option<&PersistedRuntimeInfo>,
    inspection: Option<&ContainerInspection>,
) -> Option<String> {
    runtime
        .and_then(|runtime| runtime.network_name.clone())
        .or_else(|| inspection.and_then(|inspection| inspection.network_ips.keys().next().cloned()))
}

#[derive(Debug, Clone)]
struct RouteStatusDetails {
    inspection: Option<RouteInspection>,
    expected_target: Option<String>,
    expected_domain: String,
    route_required: bool,
}

impl RouteStatusDetails {
    fn route_required(&self) -> bool {
        self.route_required
    }

    fn matches_truth(&self) -> bool {
        if !self.route_required {
            return true;
        }
        let Some(inspection) = &self.inspection else {
            return false;
        };
        let Some(expected_target) = self.expected_target.as_deref() else {
            return false;
        };
        inspection.active_target == expected_target
            && inspection.domain.as_deref() == Some(self.expected_domain.as_str())
    }

    fn mismatch_reason(&self) -> Option<String> {
        if !self.route_required || self.matches_truth() {
            return None;
        }
        let Some(inspection) = &self.inspection else {
            return Some("route missing".into());
        };
        match self.expected_target.as_deref() {
            Some(expected) if inspection.active_target != expected => Some(format!(
                "route target mismatch: current={} expected={expected}",
                inspection.active_target
            )),
            Some(_) if inspection.domain.as_deref() != Some(self.expected_domain.as_str()) => {
                Some(format!(
                    "route domain mismatch: current={} expected={}",
                    inspection.domain.as_deref().unwrap_or("unknown"),
                    self.expected_domain
                ))
            }
            _ => Some("route truth unavailable".into()),
        }
    }
}

fn inspect_route_status<R: RoutingRuntime>(
    routing: &mut R,
    project_id: &str,
    environment: &str,
    domain: &str,
    runtime: Option<&PersistedRuntimeInfo>,
    container: Option<&ContainerInspection>,
    network_name: Option<&str>,
) -> Option<RouteStatusDetails> {
    let runtime = runtime?;
    let PersistedActivationMode::Http {
        internal_port,
        route_subtree_id: persisted_subtree_id,
        target_source,
    } = runtime.activation.as_ref()?
    else {
        return None;
    };
    let subtree_id = persisted_subtree_id
        .clone()
        .unwrap_or_else(|| route_subtree_id(project_id, environment));
    let inspection = routing.inspect_route(&subtree_id).ok();
    let expected_target = container.and_then(|container| {
        resolve_route_target(container, *internal_port, network_name, target_source)
    });
    Some(RouteStatusDetails {
        inspection,
        expected_target,
        expected_domain: domain.to_string(),
        route_required: true,
    })
}

fn resolve_route_target(
    container: &ContainerInspection,
    internal_port: u16,
    network_name: Option<&str>,
    target_source: &PersistedRouteTargetSource,
) -> Option<String> {
    match target_source {
        PersistedRouteTargetSource::ContainerIp => {
            let ip = network_name
                .and_then(|network| container.network_ips.get(network))
                .or_else(|| container.network_ips.values().next())?;
            Some(format!("{ip}:{internal_port}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crate::api::ProjectUpsertRequest;
    use crate::runtime::{
        BuildImageRequest, CreateContainerRequest, ManagedImage, RouteUpdateRequest,
    };
    use crate::storage::{
        PersistedRuntimeInfo, PointerStore, RuntimeHealthState, RuntimeState, RuntimeStateStore,
        SnapshotState, SnapshotWriter, atomic_write,
    };

    #[derive(Default)]
    struct StubDockerRuntime {
        inspection: Option<ContainerInspection>,
    }

    impl DockerRuntime for StubDockerRuntime {
        fn build_image(
            &mut self,
            request: BuildImageRequest,
        ) -> Result<String, DockerRuntimeError> {
            Ok(request.image_tag)
        }

        fn ensure_network(&mut self, _network_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn create_container(
            &mut self,
            request: CreateContainerRequest,
        ) -> Result<String, DockerRuntimeError> {
            Ok(request.container_name)
        }

        fn start_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn inspect_container(
            &mut self,
            _container_name: &str,
        ) -> Result<ContainerInspection, DockerRuntimeError> {
            self.inspection.clone().ok_or_else(|| {
                DockerRuntimeError::CommandFailed("Error: No such object: container".into())
            })
        }

        fn container_logs(
            &mut self,
            _container_name: &str,
            _tail_lines: usize,
        ) -> Result<String, DockerRuntimeError> {
            Ok(String::new())
        }

        fn list_managed_containers(
            &mut self,
        ) -> Result<Vec<ContainerInspection>, DockerRuntimeError> {
            Ok(self.inspection.clone().into_iter().collect())
        }

        fn list_managed_images(&mut self) -> Result<Vec<ManagedImage>, DockerRuntimeError> {
            Ok(Vec::new())
        }

        fn stop_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn remove_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn remove_image(&mut self, _image_ref: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubRoutingRuntime {
        inspection: Option<RouteInspection>,
    }

    impl RoutingRuntime for StubRoutingRuntime {
        fn update_route(
            &mut self,
            _request: RouteUpdateRequest,
        ) -> Result<(), RoutingRuntimeError> {
            Ok(())
        }

        fn inspect_route(
            &mut self,
            _subtree_id: &str,
        ) -> Result<RouteInspection, RoutingRuntimeError> {
            self.inspection
                .clone()
                .ok_or_else(|| RoutingRuntimeError::InspectionFailed("missing route".into()))
        }

        fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
            Ok(self.inspection.clone().into_iter().collect())
        }

        fn remove_route(&mut self, _subtree_id: &str) -> Result<(), RoutingRuntimeError> {
            Ok(())
        }
    }

    fn test_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "forge-status-tests-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn register_project(root: &Path, project_id: &str, base_domain: &str) {
        ProjectRegistryStore::new(root)
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some(project_id.into()),
                    repo_url: format!("https://github.com/example/{project_id}.git"),
                    default_branch: "main".into(),
                    base_domain: Some(base_domain.into()),
                },
                None,
            )
            .unwrap();
    }

    fn write_generation(root: &Path, generation: u64) {
        let env = EnvironmentPaths::new(root, "api", "staging");
        let writer = SnapshotWriter::new(env.clone(), generation).unwrap();
        writer
            .write_artifact(
                "build.json",
                &format!(
                    concat!(
                        "{{\n",
                        "  \"deployment_id\": \"dep-{}\",\n",
                        "  \"image_ref\": \"forge/api:staging-gen-{}\",\n",
                        "  \"source_ref\": \"main\",\n",
                        "  \"commit_sha\": \"340ac8108006d84dbf951d8c0bb04ecfaf0eccac\"\n",
                        "}}\n"
                    ),
                    generation, generation,
                ),
            )
            .unwrap();
        let runtime = serde_json::to_string_pretty(&PersistedRuntimeInfo {
            container_name: format!("staging-api-gen-{generation}"),
            running: true,
            network_name: Some("forge-managed".into()),
            probe_path: Some("/health".into()),
            activation: Some(PersistedActivationMode::Http {
                internal_port: 3000,
                route_subtree_id: Some("forge:api:staging".into()),
                target_source: PersistedRouteTargetSource::ContainerIp,
            }),
            environment_variables: BTreeMap::new(),
            source_ref: Some("main".into()),
            repo_url: None,
            commit_sha: Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac".into()),
            source_path: None,
        })
        .unwrap();
        writer
            .write_artifact("runtime.json", &format!("{runtime}\n"))
            .unwrap();
        writer
            .write_artifact(
                "runtime_env_snapshot.json",
                &format!(
                    concat!(
                        "{{\n",
                        "  \"snapshot_version\": 1,\n",
                        "  \"project_id\": \"api\",\n",
                        "  \"environment\": \"staging\",\n",
                        "  \"generation\": {generation},\n",
                        "  \"deployment_id\": \"dep-{generation}\",\n",
                        "  \"source_environment\": \"staging\",\n",
                        "  \"source_ref\": \"main\",\n",
                        "  \"commit_sha\": \"340ac8108006d84dbf951d8c0bb04ecfaf0eccac\",\n",
                        "  \"domain\": \"staging-api.example.com\",\n",
                        "  \"entries\": {{\n",
                        "    \"FORGE_PROJECT_ID\": {{ \"source\": \"forge_generated\", \"value\": \"api\", \"sensitive\": false, \"redacted\": false }},\n",
                        "    \"FORGE_ENVIRONMENT\": {{ \"source\": \"forge_generated\", \"value\": \"staging\", \"sensitive\": false, \"redacted\": false }},\n",
                        "    \"API_BASE_URL\": {{ \"source\": \"forge_yaml\", \"value\": \"https://api.example.com\", \"sensitive\": false, \"redacted\": false }},\n",
                        "    \"DATABASE_URL\": {{ \"source\": \"project_environment_secret\", \"secret_reference\": {{ \"scope\": \"environment\", \"key\": \"DATABASE_URL\", \"secret_id\": \"api:staging:DATABASE_URL\", \"sensitive\": true }}, \"sensitive\": true, \"redacted\": true }}\n",
                        "  }}\n",
                        "}}\n"
                    ),
                    generation = generation,
                ),
            )
            .unwrap();
        writer
            .finalize("api", "staging", SnapshotState::Healthy)
            .unwrap();
        PointerStore::new(env.clone())
            .swap_current(generation)
            .unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(generation),
                health_state: RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 1,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "healthy".into(),
                last_error_code: None,
            })
            .unwrap();
    }

    fn healthy_container(generation: u64) -> ContainerInspection {
        ContainerInspection {
            container_name: format!("staging-api-gen-{generation}"),
            running: true,
            state_status: "running".into(),
            exit_code: Some(0),
            started_at: Some("2026-05-21T12:00:00Z".into()),
            image_ref: format!("forge/api:staging-gen-{generation}"),
            labels: BTreeMap::new(),
            network_ips: BTreeMap::from([("forge-managed".into(), "172.29.0.2".into())]),
            restart_policy: "no".into(),
        }
    }

    fn healthy_route() -> RouteInspection {
        RouteInspection {
            subtree_id: "forge:api:staging".into(),
            active_target: "172.29.0.2:3000".into(),
            domain: Some("staging-api.example.com".into()),
            activation_verified: true,
            verification_url: None,
            verification_host: None,
            verification_status_code: None,
            verification_response_body: None,
            health_checks_enabled: false,
        }
    }

    #[test]
    fn status_reports_promoted_generation_runtime() {
        let root = test_root("reports-promoted-generation-runtime");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "healthy");
        assert_eq!(status.active_generation, Some(7));
        assert_eq!(status.domain, "staging-api.example.com");
        assert_eq!(status.container_name.as_deref(), Some("staging-api-gen-7"));
        assert!(status.container_running);
        assert_eq!(status.network_name.as_deref(), Some("forge-managed"));
        assert_eq!(status.container_ip.as_deref(), Some("172.29.0.2"));
        assert!(status.route_active);
    }

    #[test]
    fn status_detects_missing_container() {
        let root = test_root("detects-missing-container");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut docker = StubDockerRuntime { inspection: None };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "degraded");
        assert!(!status.container_running);
    }

    #[test]
    fn status_detects_route_target_mismatch() {
        let root = test_root("detects-route-target-mismatch");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(RouteInspection {
                active_target: "172.29.0.55:3000".into(),
                ..healthy_route()
            }),
        };

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "degraded");
        assert!(status.route_active);
    }

    #[test]
    fn status_derives_environment_domain_correctly() {
        assert_eq!(
            derive_environment_domain("api.example.com", "production"),
            "api.example.com"
        );
        assert_eq!(
            derive_environment_domain("api.example.com", "staging"),
            "staging-api.example.com"
        );
        assert_eq!(
            derive_environment_domain("api.example.com", "development"),
            "development-api.example.com"
        );
    }

    #[test]
    fn status_reports_degraded_when_route_missing() {
        let root = test_root("reports-degraded-when-route-missing");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime { inspection: None };

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "degraded");
        assert!(!status.route_active);
    }

    #[test]
    fn status_json_matches_runtime_truth() {
        let root = test_root("json-matches-runtime-truth");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        atomic_write(
            root.join("queue").join("queued.db"),
            b"{\"deployment_id\":\"dep-8\",\"project_id\":\"api\",\"environment\":\"staging\"}\n",
        )
        .unwrap();
        atomic_write(root.join("queue").join("active.db"), b"\n").unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let status = load_project_environment_status(
            &root,
            Some(&queue),
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();
        let json = serde_json::to_value(&status).unwrap();

        assert_eq!(json["project_id"], "api");
        assert_eq!(json["environment"], "staging");
        assert_eq!(json["status"], "deploying");
        assert_eq!(json["active_generation"], 7);
        assert_eq!(json["container_running"], true);
        assert_eq!(json["route_active"], true);
        assert_eq!(json["image_ref"], "forge/api:staging-gen-7");
    }

    #[test]
    fn status_handles_missing_generation_gracefully() {
        let root = test_root("handles-missing-generation-gracefully");
        register_project(&root, "api", "api.example.com");

        let mut docker = StubDockerRuntime::default();
        let mut routing = StubRoutingRuntime::default();

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "missing");
        assert_eq!(status.active_generation, None);
        assert!(!status.container_running);
        assert!(!status.route_active);
    }

    #[test]
    fn status_reports_failed_without_healthy_promoted_generation() {
        let root = test_root("reports-failed-without-healthy-promoted-generation");
        register_project(&root, "api", "api.example.com");
        let env = EnvironmentPaths::new(&root, "api", "staging");
        SnapshotWriter::new(env.clone(), 3)
            .unwrap()
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-3\",\n  \"image_ref\": \"forge/api:staging-gen-3\"\n}\n",
            )
            .unwrap();
        SnapshotWriter::new(env, 3)
            .unwrap()
            .finalize("api", "staging", SnapshotState::Failed)
            .unwrap();

        let mut docker = StubDockerRuntime::default();
        let mut routing = StubRoutingRuntime::default();

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "failed");
        assert_eq!(status.last_deployment_id.as_deref(), Some("dep-3"));
    }

    #[test]
    fn status_after_rollback_reports_restored_generation() {
        let root = test_root("status-after-rollback-reports-restored-generation");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 1);
        write_generation(&root, 2);
        let env = EnvironmentPaths::new(&root, "api", "staging");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(1),
                health_state: RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 1,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "rollback_completed".into(),
                last_error_code: None,
            })
            .unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(1)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "healthy");
        assert_eq!(status.active_generation, Some(1));
        assert_eq!(
            status.commit_sha.as_deref(),
            Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac")
        );
        assert_eq!(status.source_ref.as_deref(), Some("main"));
        assert_eq!(status.image_ref.as_deref(), Some("forge/api:staging-gen-1"));
        assert_eq!(status.last_deployment_id.as_deref(), Some("dep-1"));
    }

    #[test]
    fn diagnose_reports_runtime_truth() {
        let root = test_root("diagnose-reports-runtime-truth");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert_eq!(diagnostics.active_generation, Some(7));
        assert_eq!(
            diagnostics.container.container_name.as_deref(),
            Some("staging-api-gen-7")
        );
        assert!(diagnostics.container.running);
        assert_eq!(
            diagnostics.route.current_target.as_deref(),
            Some("172.29.0.2:3000")
        );
        assert_eq!(
            diagnostics.route.expected_target.as_deref(),
            Some("172.29.0.2:3000")
        );
        assert!(diagnostics.route.matches_expected);
        assert_eq!(
            diagnostics
                .probe_target
                .as_ref()
                .and_then(|target| target.path.as_deref()),
            Some("/health")
        );
    }

    #[test]
    fn diagnose_reports_recent_failure_summary() {
        let root = test_root("diagnose-reports-recent-failure-summary");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        let failed = SnapshotWriter::new(env.clone(), 8).unwrap();
        failed
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-8\",\n  \"image_ref\": \"forge/api:staging-gen-8\"\n}\n",
            )
            .unwrap();
        failed
            .finalize("api", "staging", SnapshotState::Failed)
            .unwrap();
        let diagnostics_store = DiagnosticsStore::new(env, 8);
        diagnostics_store
            .write_summary(&crate::storage::DiagnosticSummary {
                deployment_id: Some("dep-8".into()),
                failure_stage: "validating_runtime".into(),
                failure_reason: "http health probe failed".into(),
                container_name: "staging-api-gen-8".into(),
                probe_target_host: Some("172.29.0.3".into()),
                probe_target_port: Some(3000),
                probe_target_path: Some("/health".into()),
                cleanup_recorded: true,
                runtime_env_preview: Vec::new(),
            })
            .unwrap();
        diagnostics_store
            .write_artifact(
                "validation_failure.json",
                "{\n  \"probe_target\": {\"host\": \"172.29.0.3\", \"port\": 3000, \"path\": \"/health\"},\n  \"last_error\": \"http health probe returned unhealthy\"\n}\n",
                &[],
            )
            .unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert_eq!(diagnostics.recent_failures.len(), 1);
        assert_eq!(diagnostics.recent_failures[0].generation, 8);
        assert_eq!(
            diagnostics.recent_failures[0].failure_stage,
            "validating_runtime"
        );
        assert!(
            diagnostics.recent_failures[0]
                .validation_failure_summary
                .as_deref()
                .unwrap()
                .contains("http health probe returned unhealthy")
        );
        assert_eq!(
            diagnostics.likely_failure_stage.as_deref(),
            Some("validating_runtime")
        );
    }

    #[test]
    fn diagnose_handles_missing_diagnostics() {
        let root = test_root("diagnose-handles-missing-diagnostics");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(RouteInspection {
                active_target: "172.29.0.99:3000".into(),
                ..healthy_route()
            }),
        };

        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert!(diagnostics.recent_failures.is_empty());
        assert!(diagnostics.latest_validation_failure.is_none());
        assert!(diagnostics.route.mismatch_reason.is_some());
        assert!(diagnostics.diagnostics_source.is_none());
    }

    #[test]
    fn runtime_env_snapshot_metadata_is_exposed() {
        let root = test_root("runtime-env-snapshot-metadata-is-exposed");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let report = load_project_environment_env_report(&root, "api", "staging").unwrap();
        assert_eq!(report.generation, 7);
        assert!(
            report
                .values
                .iter()
                .any(|entry| entry.key == "DATABASE_URL" && entry.value == "<secret>")
        );

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();
        assert_eq!(
            status
                .runtime_env_snapshot
                .as_ref()
                .unwrap()
                .generated_forge_vars["FORGE_PROJECT_ID"],
            "api"
        );
    }
}
