use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::{
    ContainerRuntimeDiagnostics, DeploymentHistoryEntry, DeploymentHistoryResponse,
    EnvironmentDiagnostics, EnvironmentDiffEntry, EnvironmentDiffResponse, EnvironmentDiffSummary,
    EnvironmentValueChange, EnvironmentVariableReport, EnvironmentVariableValue, ErrorResponse,
    ProbeStabilityDiagnostics, ProbeTargetDiagnostics, RecentDeploymentFailure, RecentGcAction,
    RetentionRole, RouteDiagnostics, RuntimeEnvSnapshotMetadata, SecretMutationDiagnostic,
    SecretReferenceChange, ServiceRuntimeStatus, VolumeRuntimeStatus,
};
use crate::backups::load_backup_restore_lineage;
use crate::events::EventRecord;
use crate::forge_yaml::load_optional_forge_yaml;
use crate::manifest::load_optional_manifest;
use crate::projects::ProjectRegistryStore;
use crate::queue::{PersistentQueue, QueueError};
use crate::route_truth::expected_route_for_runtime;
use crate::runtime::{
    ContainerInspection, DockerRuntime, DockerRuntimeError, RouteInspection, RoutingRuntime,
    RoutingRuntimeError,
};
use crate::runtime_env::restore_runtime_env;
use crate::runtime_env::{GENERATED_FORGE_ENV_KEYS, render_snapshot_value};
use crate::secrets::SecretStore;
use crate::storage::{
    DeploymentLifecycleState, DiagnosticsStore, EnvironmentPaths, GcStore, GenerationHistoryRecord,
    PersistedActivationMode, PersistedBuildInfo, PersistedDeploymentLifecycle,
    PersistedProbeHistory, PersistedProbeType, PersistedPromotionSummary, PersistedResolvedRuntime,
    PersistedRuntimeEnvSnapshot, PersistedRuntimeInfo, PersistedRuntimePolicy,
    PersistedRuntimeUsageSnapshot, PersistedServiceRuntimeInfo, PersistedServiceState,
    PersistedSnapshotMetadata, PersistedTerminationInfo, PersistedValidationSummary,
    PersistedVolumeRetention, PointerStore, RetentionMetadata, RetentionStore, StorageError,
    load_generation_build_info, load_generation_lifecycle, load_generation_probe_history,
    load_generation_resolved_runtime, load_generation_runtime_env_snapshot,
    load_generation_runtime_info, load_generation_snapshot_metadata,
};
use crate::topology::runtime_with_primary_service;

const HEALTHY_FINALIZED_RETENTION_LIMIT: usize = 2;
const FAILED_GENERATION_RETENTION_LIMIT: usize = 2;
const PROBE_FLAPPING_WINDOW: usize = 8;
const PROBE_CLEAR_STREAK_MIN: usize = 4;
const PROBE_MIN_SAMPLES_FOR_RATE: usize = 4;
const PROBE_MIN_FAILURES_FOR_FLAPPING: usize = 2;
const PROBE_MIN_ALTERNATIONS_FOR_FLAPPING: usize = 3;
const PROBE_SUCCESS_RATE_THRESHOLD: f64 = 0.75;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RepairEventBuckets {
    current: Vec<String>,
    historical: Vec<String>,
}

#[derive(Debug, Clone)]
struct ProbeFlappingAssessment {
    flapping: bool,
    diagnostics: ProbeStabilityDiagnostics,
}

fn load_recent_probe_history(
    env: &EnvironmentPaths,
    generation: Option<u64>,
) -> Result<PersistedProbeHistory, ProjectStatusError> {
    let Some(generation) = generation else {
        return Ok(PersistedProbeHistory::default());
    };
    Ok(load_generation_probe_history(env, generation)?.unwrap_or_default())
}

fn probe_type_name(probe_type: &PersistedProbeType) -> &'static str {
    match probe_type {
        PersistedProbeType::Tcp => "tcp",
        PersistedProbeType::Http => "http",
    }
}

fn assess_probe_flapping(
    history: &PersistedProbeHistory,
    validation_summary: Option<&PersistedValidationSummary>,
    promotion_summary: Option<&PersistedPromotionSummary>,
) -> Option<ProbeFlappingAssessment> {
    if history.entries.is_empty() {
        return None;
    }

    let required_passes = validation_summary
        .map(|summary| summary.required_consecutive_passes as usize)
        .unwrap_or(0);
    let clear_streak = required_passes.max(PROBE_CLEAR_STREAK_MIN).max(1);
    let recent_entries = history
        .entries
        .iter()
        .rev()
        .take(PROBE_FLAPPING_WINDOW)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();
    let sample_size = recent_entries.len();
    let success_count = recent_entries.iter().filter(|entry| entry.success).count();
    let recent_failure_count = sample_size.saturating_sub(success_count);
    let success_rate = if sample_size == 0 {
        1.0
    } else {
        success_count as f64 / sample_size as f64
    };
    let consecutive_success_streak = recent_entries
        .iter()
        .rev()
        .take_while(|entry| entry.success)
        .count();
    let alternations = recent_entries
        .windows(2)
        .filter(|window| window[0].success != window[1].success)
        .count();
    let unstable_after_warmup = promotion_summary.is_some_and(|summary| summary.warmup_succeeded)
        && sample_size >= clear_streak + PROBE_MIN_FAILURES_FOR_FLAPPING
        && recent_failure_count >= PROBE_MIN_FAILURES_FOR_FLAPPING + 1
        && consecutive_success_streak < clear_streak;
    let oscillating = sample_size >= PROBE_MIN_SAMPLES_FOR_RATE + 1
        && recent_failure_count >= PROBE_MIN_FAILURES_FOR_FLAPPING
        && success_count >= PROBE_MIN_FAILURES_FOR_FLAPPING
        && alternations >= PROBE_MIN_ALTERNATIONS_FOR_FLAPPING;
    let low_success_rate = sample_size >= PROBE_MIN_SAMPLES_FOR_RATE
        && recent_failure_count >= PROBE_MIN_FAILURES_FOR_FLAPPING
        && success_rate < PROBE_SUCCESS_RATE_THRESHOLD;
    let flapping = if consecutive_success_streak >= clear_streak {
        false
    } else {
        oscillating || low_success_rate || unstable_after_warmup
    };

    let by_type = [PersistedProbeType::Tcp, PersistedProbeType::Http]
        .into_iter()
        .filter_map(|probe_type| {
            let matching = recent_entries
                .iter()
                .filter(|entry| entry.probe_type == probe_type)
                .collect::<Vec<_>>();
            if matching.is_empty() {
                return None;
            }
            let successes = matching.iter().filter(|entry| entry.success).count();
            let failures = matching.len().saturating_sub(successes);
            let alternations = matching
                .windows(2)
                .filter(|window| window[0].success != window[1].success)
                .count();
            Some(format!(
                "{}={}/{} ok, failures={}, alternations={}",
                probe_type_name(&probe_type),
                successes,
                matching.len(),
                failures,
                alternations
            ))
        })
        .collect::<Vec<_>>()
        .join("; ");
    let latest_failure = recent_entries
        .iter()
        .rev()
        .find(|entry| !entry.success)
        .and_then(|entry| entry.failure_reason.as_deref())
        .unwrap_or("none");
    let flapping_window_summary = format!(
        "window={} success_rate={:.0}% failures={} alternations={} stable_streak={} latest_failure={} [{}]",
        sample_size,
        success_rate * 100.0,
        recent_failure_count,
        alternations,
        consecutive_success_streak,
        latest_failure,
        by_type
    );

    Some(ProbeFlappingAssessment {
        flapping,
        diagnostics: ProbeStabilityDiagnostics {
            sample_size,
            success_rate,
            consecutive_success_streak,
            recent_failure_count,
            flapping_window_summary,
        },
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentStatus {
    pub project_id: String,
    pub environment: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_generation: Option<u64>,
    #[serde(default)]
    pub domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    #[serde(default)]
    pub container_running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_ip: Option<String>,
    #[serde(default)]
    pub route_active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_ref: Option<String>,
    #[serde(default)]
    pub runtime_policy: PersistedRuntimePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_usage: Option<PersistedRuntimeUsageSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination: Option<PersistedTerminationInfo>,
    #[serde(default)]
    pub restart_count: u64,
    #[serde(default)]
    pub startup_order: Vec<String>,
    #[serde(default)]
    pub services: Vec<ServiceRuntimeStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_deployment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_env_snapshot: Option<RuntimeEnvSnapshotMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_state: Option<DeploymentLifecycleState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_role: Option<RetentionRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_summary: Option<PersistedValidationSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_summary: Option<PersistedPromotionSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uptime_seconds: Option<u64>,
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
    RuntimeEnvSnapshotUnavailable(String),
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
            Self::RuntimeEnvSnapshotUnavailable(message) => write!(f, "{message}"),
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

#[derive(Debug, Clone)]
struct EnvironmentRuntimeTruth {
    current_generation: Option<u64>,
    active_generation: Option<u64>,
    latest_generation: Option<u64>,
    promoted_snapshot: Option<PersistedSnapshotMetadata>,
    promoted_runtime: Option<PersistedRuntimeInfo>,
    promoted_build: Option<PersistedBuildInfo>,
    latest_snapshot: Option<PersistedSnapshotMetadata>,
    latest_build: Option<PersistedBuildInfo>,
    active_lifecycle: Option<PersistedDeploymentLifecycle>,
    latest_lifecycle: Option<PersistedDeploymentLifecycle>,
    promoted_runtime_env_snapshot: Option<PersistedRuntimeEnvSnapshot>,
    promoted_generation_issue: Option<String>,
    container_running: bool,
    container_status: Option<String>,
    container_started_at: Option<String>,
    network_name: Option<String>,
    container_ip: Option<String>,
    image_ref: Option<String>,
    runtime_policy: PersistedRuntimePolicy,
    runtime_usage: Option<PersistedRuntimeUsageSnapshot>,
    termination: Option<PersistedTerminationInfo>,
    restart_count: u64,
    startup_order: Vec<String>,
    services: Vec<ServiceRuntimeStatus>,
    route_details: Option<RouteStatusDetails>,
}

#[derive(Debug, Clone, Default)]
struct HistoryReferences {
    current: Option<u64>,
    previous: Option<u64>,
    promoted: Option<u64>,
    route_generation: Option<u64>,
    converging_generation: Option<u64>,
}

impl HistoryReferences {
    fn contains(&self, generation: u64) -> bool {
        self.current == Some(generation)
            || self.previous == Some(generation)
            || self.promoted == Some(generation)
            || self.route_generation == Some(generation)
            || self.converging_generation == Some(generation)
    }
}

fn deployment_history_entry(record: GenerationHistoryRecord) -> DeploymentHistoryEntry {
    DeploymentHistoryEntry {
        generation: record.generation,
        deployment_id: record.deployment_id,
        commit_sha: record.commit_sha,
        source_ref: record.source_ref,
        image_ref: record.image_ref,
        created_at_unix: record.created_at_unix,
        promoted_at_unix: record.promoted_at_unix,
        finalized_state: record.finalized_state,
        finalized_at_unix: record.finalized_at_unix,
        rollback_target: record.rollback_target,
        restored_by_rollback: record.restored_by_rollback,
        retained: record.retained,
        eligible_for_gc: record.eligible_for_gc,
        missing_artifacts: record.missing_artifacts,
        retained_reasons: record.retained_reasons,
        lifecycle_state: None,
        retention_role: None,
        entered_at_unix: None,
        transition_reason: None,
        validation_summary: None,
        promotion_summary: None,
        restored_from_backup_id: record.restored_from_backup_id,
        restored_from_generation: record.restored_from_generation,
        restored_from_deployment_id: record.restored_from_deployment_id,
        restored_at_unix: record.restored_at_unix,
    }
}

fn retention_role_for_generation(
    references: &HistoryReferences,
    generation: u64,
    retained: bool,
    eligible_for_gc: bool,
) -> Option<RetentionRole> {
    if references.current == Some(generation) || references.promoted == Some(generation) {
        Some(RetentionRole::Current)
    } else if references.previous == Some(generation) {
        Some(RetentionRole::RollbackTarget)
    } else if retained {
        Some(RetentionRole::Retained)
    } else if eligible_for_gc {
        Some(RetentionRole::GcEligible)
    } else {
        None
    }
}

#[cfg(test)]
fn status_label(
    lifecycle_state: Option<&DeploymentLifecycleState>,
    retention_role: Option<&RetentionRole>,
) -> &'static str {
    match retention_role {
        Some(RetentionRole::Current) => "active",
        Some(RetentionRole::RollbackTarget) => "rollback_target",
        Some(RetentionRole::GcEligible) => "gc_eligible",
        Some(RetentionRole::Retained) => match lifecycle_state {
            Some(DeploymentLifecycleState::Promoted) => "historical_promoted",
            Some(DeploymentLifecycleState::Failed) => "failed",
            Some(DeploymentLifecycleState::Rollback) => "rollback",
            Some(DeploymentLifecycleState::GcEligible) => "gc_eligible",
            _ => "historical",
        },
        None => match lifecycle_state {
            Some(DeploymentLifecycleState::Promoted) => "historical_promoted",
            Some(DeploymentLifecycleState::Failed) => "failed",
            Some(DeploymentLifecycleState::Rollback) => "rollback",
            Some(DeploymentLifecycleState::GcEligible) => "gc_eligible",
            _ => "historical",
        },
    }
}

fn merge_live_generation_metadata(
    env: &EnvironmentPaths,
    record: &mut GenerationHistoryRecord,
) -> Result<(), ProjectStatusError> {
    if let Some(build) = load_generation_build_info(env, record.generation)? {
        if record.deployment_id.is_none() {
            record.deployment_id = Some(build.deployment_id);
        }
        if record.image_ref.is_none() {
            record.image_ref = Some(build.image_ref);
        }
        if record.source_ref.is_none() {
            record.source_ref = build.source_ref;
        }
        if record.commit_sha.is_none() {
            record.commit_sha = build.commit_sha;
        }
        if record.source_path.is_none() {
            record.source_path = build.source_path;
        }
    }
    if let Some(runtime) = load_generation_runtime_info(env, record.generation)? {
        if record.source_ref.is_none() {
            record.source_ref = runtime.source_ref;
        }
        if record.commit_sha.is_none() {
            record.commit_sha = runtime.commit_sha;
        }
        if record.source_path.is_none() {
            record.source_path = runtime.source_path;
        }
    }
    if let Some(snapshot) = load_generation_snapshot_metadata(env, record.generation)? {
        record.finalized_state = Some(snapshot.state);
        record.finalized_at_unix = Some(snapshot.finalized_at_unix);
    }
    Ok(())
}

fn generation_has_failure_diagnostics(
    env: &EnvironmentPaths,
    generation: u64,
) -> Result<bool, ProjectStatusError> {
    let diagnostics = env.generation_dir(generation).join("diagnostics");
    let summary_path = diagnostics.join("summary.json");
    if summary_path.exists() {
        let raw = match fs::read_to_string(summary_path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err.into()),
        };
        let summary: crate::storage::DiagnosticSummary =
            serde_json::from_str(&raw).map_err(|err| {
                ProjectStatusError::Storage(StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    err.to_string(),
                )))
            })?;
        if summary.failure_stage != "startup_recovery" {
            return Ok(true);
        }
    }
    Ok(diagnostics.join("failure_reason.log").exists()
        || diagnostics.join("deployment.log").exists())
}

fn compute_history_references(
    env: &EnvironmentPaths,
    route_generation: Option<u64>,
    converging_generation: Option<u64>,
) -> Result<HistoryReferences, ProjectStatusError> {
    let pointers = PointerStore::new(env.clone());
    Ok(HistoryReferences {
        current: pointers.read_pointer("current")?,
        previous: pointers.read_pointer("previous")?,
        promoted: pointers.read_pointer("promoted")?,
        route_generation,
        converging_generation,
    })
}

fn active_restore_lineage(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
    generation: u64,
    last_deployment_id: Option<&str>,
    history_entry: Option<&DeploymentHistoryEntry>,
) -> Option<crate::api::RestoreLineage> {
    let mut record = GenerationHistoryRecord {
        generation,
        deployment_id: last_deployment_id.map(str::to_string),
        ..GenerationHistoryRecord::default()
    };
    if let Some(entry) = history_entry {
        if record.deployment_id.is_none() {
            record.deployment_id = entry.deployment_id.clone();
        }
        record.commit_sha = entry.commit_sha.clone();
        record.source_ref = entry.source_ref.clone();
        record.image_ref = entry.image_ref.clone();
        record.created_at_unix = entry.created_at_unix;
        record.finalized_at_unix = entry.finalized_at_unix;
        record.promoted_at_unix = entry.promoted_at_unix;
        record.finalized_state = entry.finalized_state.clone();
        record.restored_by_rollback = entry.restored_by_rollback;
        record.rollback_target = entry.rollback_target;
        record.retained = entry.retained;
        record.eligible_for_gc = entry.eligible_for_gc;
        record.missing_artifacts = entry.missing_artifacts;
        record.retained_reasons = entry.retained_reasons.clone();
        record.restored_from_backup_id = entry.restored_from_backup_id.clone();
        record.restored_from_generation = entry.restored_from_generation;
        record.restored_from_deployment_id = entry.restored_from_deployment_id.clone();
        record.restored_at_unix = entry.restored_at_unix;
    }
    load_backup_restore_lineage(storage_root, project_id, environment, &record)
}

fn retained_healthy_generations(
    records: &[GenerationHistoryRecord],
    references: &HistoryReferences,
) -> BTreeSet<u64> {
    records
        .iter()
        .filter(|record| {
            !references.current.is_some_and(|current| {
                record.generation > current && !references.contains(record.generation)
            })
        })
        .filter(|record| record.finalized_state.as_deref() == Some("healthy"))
        .map(|record| record.generation)
        .rev()
        .take(HEALTHY_FINALIZED_RETENTION_LIMIT)
        .collect()
}

fn retained_failed_generations(
    env: &EnvironmentPaths,
    records: &[GenerationHistoryRecord],
    references: &HistoryReferences,
) -> Result<BTreeSet<u64>, ProjectStatusError> {
    let mut retained = BTreeSet::new();
    for generation in records.iter().map(|record| record.generation).rev() {
        if references.contains(generation) {
            continue;
        }
        if !generation_has_failure_diagnostics(env, generation)? {
            continue;
        }
        retained.insert(generation);
        if retained.len() >= FAILED_GENERATION_RETENTION_LIMIT {
            break;
        }
    }
    Ok(retained)
}

fn refresh_history_metadata(
    env: &EnvironmentPaths,
    references: &HistoryReferences,
) -> Result<RetentionMetadata, ProjectStatusError> {
    let store = RetentionStore::new(env.clone());
    let mut metadata = store.read()?;
    let mut by_generation = metadata
        .generations
        .into_iter()
        .map(|record| (record.generation, record))
        .collect::<BTreeMap<_, _>>();

    for generation in list_generation_numbers(env)? {
        let record = by_generation
            .entry(generation)
            .or_insert_with(|| GenerationHistoryRecord {
                generation,
                ..GenerationHistoryRecord::default()
            });
        merge_live_generation_metadata(env, record)?;
    }

    let mut records = by_generation.into_values().collect::<Vec<_>>();
    records.sort_by_key(|record| record.generation);

    let healthy_retained = retained_healthy_generations(&records, references);
    let failed_retained = retained_failed_generations(env, &records, references)?;

    for record in &mut records {
        let generation_dir_exists = env.generation_dir(record.generation).exists();
        record.rollback_target = references.previous == Some(record.generation);
        record.missing_artifacts = record.retained && !generation_dir_exists;
        let mut reasons = Vec::new();
        if references.current == Some(record.generation)
            || references.promoted == Some(record.generation)
        {
            reasons.push("current/promoted generation".into());
        }
        if references.previous == Some(record.generation) {
            reasons.push("rollback-safe generation".into());
        }
        if references.route_generation == Some(record.generation) {
            reasons.push("route reference".into());
        }
        if references.converging_generation == Some(record.generation) {
            reasons.push("deployment in progress".into());
        }
        if healthy_retained.contains(&record.generation) {
            reasons.push("recent healthy finalized generation".into());
        }
        if failed_retained.contains(&record.generation) {
            reasons.push("recent failed generation with diagnostics".into());
        }
        record.retained = !reasons.is_empty();
        record.eligible_for_gc = !record.retained;
        record.missing_artifacts = record.retained && !generation_dir_exists;
        record.retained_reasons = reasons;
        if !generation_dir_exists && record.archived_at_unix.is_none() {
            record.archived_at_unix = record
                .finalized_at_unix
                .or(record.promoted_at_unix)
                .or(record.created_at_unix);
        }
    }

    metadata.updated_at_unix = Some(crate::storage::current_unix_timestamp());
    metadata.generations = records;
    store.write(&metadata)?;
    Ok(metadata)
}

pub fn load_environment_history<D, R>(
    storage_root: &Path,
    queue: Option<&PersistentQueue>,
    docker: &mut D,
    routing: &mut R,
    project_id: &str,
    environment: &str,
) -> Result<DeploymentHistoryResponse, ProjectStatusError>
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
    let truth =
        load_environment_runtime_truth(&env, docker, routing, project_id, environment, &domain)?;
    let route_generation = truth
        .route_details
        .as_ref()
        .and_then(|details| details.inspection.as_ref())
        .and_then(|inspection| inspection.active_target.rsplit("-gen-").next())
        .and_then(|suffix| suffix.split(':').next())
        .and_then(|value| value.parse::<u64>().ok());
    let converging_generation =
        queue
            .map(|queue| queue.load_state())
            .transpose()?
            .and_then(|state| {
                state.active.and_then(|record| {
                    (record.project_id == project_id && record.environment == environment)
                        .then(|| list_generation_numbers(&env).ok())
                        .flatten()
                        .and_then(|generations| {
                            generations.into_iter().rev().find(|generation| {
                                load_generation_snapshot_metadata(&env, *generation)
                                    .ok()
                                    .flatten()
                                    .is_none()
                            })
                        })
                })
            });
    let references = compute_history_references(&env, route_generation, converging_generation)?;
    let metadata = refresh_history_metadata(&env, &references)?;
    let mut entries = metadata
        .generations
        .into_iter()
        .map(|record| {
            let generation = record.generation;
            let mut entry = deployment_history_entry(record);
            if let Some(lifecycle) = load_generation_lifecycle(&env, generation)? {
                entry.lifecycle_state = Some(lifecycle.state.clone());
                entry.entered_at_unix = Some(lifecycle.entered_at_unix);
                entry.transition_reason = Some(lifecycle.transition_reason);
                entry.validation_summary = lifecycle.validation_summary;
                entry.promotion_summary = lifecycle.promotion_summary;
            }
            if entry.promoted_at_unix.is_some() && entry.lifecycle_state.is_none() {
                entry.lifecycle_state = Some(DeploymentLifecycleState::Promoted);
            }
            if entry.eligible_for_gc && entry.lifecycle_state.is_none() {
                entry.lifecycle_state = Some(DeploymentLifecycleState::GcEligible);
            }
            entry.retention_role = retention_role_for_generation(
                &references,
                generation,
                entry.retained,
                entry.eligible_for_gc,
            );
            Ok::<_, ProjectStatusError>(entry)
        })
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_by(|left, right| right.generation.cmp(&left.generation));
    Ok(DeploymentHistoryResponse {
        project_id: project_id.into(),
        environment: environment.into(),
        entries,
    })
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
        ProjectStatusError::RuntimeEnvSnapshotUnavailable(message) => (
            axum::http::StatusCode::NOT_FOUND,
            ErrorResponse {
                code: "runtime_env_snapshot_unavailable".into(),
                message,
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
    let truth =
        load_environment_runtime_truth(&env, docker, routing, project_id, environment, &domain)?;

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

    let container_name = truth
        .promoted_runtime
        .as_ref()
        .map(|runtime| runtime.container_name.clone());
    let route_active = truth
        .route_details
        .as_ref()
        .and_then(|details| details.inspection.as_ref())
        .is_some();
    let route_matches = truth
        .route_details
        .as_ref()
        .is_some_and(RouteStatusDetails::matches_truth);
    let route_required = truth
        .route_details
        .as_ref()
        .is_some_and(RouteStatusDetails::route_required);
    let promoted_snapshot_healthy = truth
        .promoted_snapshot
        .as_ref()
        .is_some_and(|snapshot| snapshot.state == "healthy");
    let latest_failed_without_promotion = truth.current_generation.is_none()
        && truth
            .latest_lifecycle
            .as_ref()
            .is_some_and(|lifecycle| lifecycle.state == DeploymentLifecycleState::Failed);
    let status = if deploying {
        "deploying"
    } else if truth.active_generation.is_some()
        && truth.promoted_generation_issue.is_none()
        && promoted_snapshot_healthy
        && truth.container_running
        && (!route_required || route_matches)
    {
        "healthy"
    } else if truth.current_generation.is_none()
        && (truth
            .latest_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.state == "failed")
            || latest_failed_without_promotion)
    {
        "failed"
    } else if truth.current_generation.is_none()
        && truth.active_generation.is_none()
        && truth.latest_snapshot.is_none()
        && truth.promoted_runtime.is_none()
        && truth.promoted_build.is_none()
    {
        "missing"
    } else {
        "degraded"
    };
    let visible_lifecycle = truth
        .active_lifecycle
        .as_ref()
        .or(truth.latest_lifecycle.as_ref());

    Ok(ProjectEnvironmentStatus {
        project_id: project_id.to_string(),
        environment: environment.to_string(),
        status: status.into(),
        active_generation: truth.active_generation,
        domain,
        commit_sha: truth
            .promoted_build
            .as_ref()
            .and_then(|build| build.commit_sha.clone())
            .or_else(|| {
                truth
                    .promoted_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.commit_sha.clone())
            }),
        source_ref: truth
            .promoted_build
            .as_ref()
            .and_then(|build| build.source_ref.clone())
            .or_else(|| {
                truth
                    .promoted_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.source_ref.clone())
            }),
        container_name,
        container_running: truth.container_running,
        container_status: truth.container_status,
        network_name: truth.network_name,
        container_ip: truth.container_ip,
        route_active,
        probe_path: truth
            .promoted_runtime
            .as_ref()
            .and_then(|runtime| runtime.probe_path.clone()),
        image_ref: truth.image_ref,
        runtime_policy: truth.runtime_policy.clone(),
        runtime_usage: truth.runtime_usage.clone(),
        termination: truth.termination.clone(),
        restart_count: truth.restart_count,
        startup_order: truth.startup_order.clone(),
        services: truth.services.clone(),
        last_deployment_id: truth
            .promoted_build
            .as_ref()
            .map(|build| build.deployment_id.clone())
            .or_else(|| {
                truth
                    .latest_build
                    .as_ref()
                    .map(|build| build.deployment_id.clone())
            }),
        deployed_at_unix: truth
            .promoted_snapshot
            .as_ref()
            .map(|snapshot| snapshot.finalized_at_unix)
            .or_else(|| {
                truth
                    .latest_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.finalized_at_unix)
            }),
        container_started_at: truth.container_started_at,
        runtime_env_snapshot: truth
            .promoted_runtime_env_snapshot
            .as_ref()
            .map(runtime_env_snapshot_metadata),
        lifecycle_state: visible_lifecycle.map(|lifecycle| lifecycle.state.clone()),
        retention_role: truth.active_generation.map(|_| RetentionRole::Current),
        validation_summary: visible_lifecycle
            .and_then(|lifecycle| lifecycle.validation_summary.clone()),
        promotion_summary: visible_lifecycle
            .and_then(|lifecycle| lifecycle.promotion_summary.clone()),
        uptime_seconds: visible_lifecycle.and_then(|lifecycle| {
            lifecycle
                .validation_summary
                .as_ref()
                .map(|summary| summary.observed_uptime_seconds)
        }),
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
    let truth =
        load_environment_runtime_truth(&env, docker, routing, project_id, environment, &domain)?;
    let status =
        build_environment_status_from_truth(queue, project_id, environment, &domain, &truth)?;

    let recent_failure_generations = list_recent_failure_generations(&env)?;
    let latest_failed_generation = recent_failure_generations.first().copied();
    let latest_failure = latest_failed_generation
        .map(|generation| load_failure_details_internal(&env, generation))
        .transpose()?
        .flatten();
    let latest_failure_is_current = latest_failed_generation.is_some_and(|generation| {
        status.status != "healthy"
            && (truth.active_generation == Some(generation) || truth.active_generation.is_none())
    });
    let services = enrich_services_with_diagnostics(
        &env,
        truth.active_generation.or(latest_failed_generation),
        &truth.services,
        latest_failure
            .as_ref()
            .filter(|_| latest_failure_is_current),
    )?;
    let recent_failures = recent_failure_generations
        .into_iter()
        .map(|generation| load_failure_details(&env, generation))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .map(|failure| {
            mark_failure_historical(failure, truth.active_generation, status.status.as_str())
        })
        .collect::<Vec<_>>();

    let probe_target = latest_failure
        .as_ref()
        .filter(|_| latest_failure_is_current)
        .and_then(|failure| failure.probe_target.clone())
        .or_else(|| {
            truth
                .promoted_runtime
                .as_ref()
                .map(|runtime| ProbeTargetDiagnostics {
                    host: status.container_ip.clone(),
                    port: activation_port(runtime.activation.as_ref()),
                    path: runtime.probe_path.clone(),
                })
        });

    let route = if let Some(details) = truth.route_details.as_ref() {
        RouteDiagnostics {
            route_required: details.route_required(),
            route_active: details.inspection.is_some(),
            matches_expected: details.matches_truth() && truth.promoted_generation_issue.is_none(),
            current_target: details
                .inspection
                .as_ref()
                .map(|inspection| inspection.active_target.clone()),
            expected_target: details.expected_target.clone(),
            domain: Some(details.expected_domain.clone()),
            mismatch_reason: truth
                .promoted_generation_issue
                .clone()
                .or_else(|| details.mismatch_reason()),
        }
    } else {
        RouteDiagnostics {
            route_required: false,
            route_active: false,
            matches_expected: truth.promoted_generation_issue.is_none(),
            current_target: None,
            expected_target: None,
            domain: Some(status.domain.clone()),
            mismatch_reason: truth.promoted_generation_issue.clone(),
        }
    };

    let status_value = status.status.clone();
    let history = load_environment_history(
        storage_root,
        queue,
        docker,
        routing,
        project_id,
        environment,
    )?;
    let recent_gc_actions = GcStore::new(env.clone())
        .read()?
        .actions
        .into_iter()
        .rev()
        .take(5)
        .map(|action| RecentGcAction {
            timestamp_unix: action.timestamp_unix,
            generation: action.generation,
            action: action.action,
            reason: action.reason,
            outcome: action.outcome,
            dry_run: action.dry_run,
            subject_kind: action.subject_kind,
            subject: action.subject,
            deleted: action.deleted,
            protected: action.protected,
        })
        .collect::<Vec<_>>();
    let missing_required_secrets =
        missing_required_secrets(storage_root, project_id, environment, &truth)?;
    let env_drift = match (
        truth.active_generation,
        history.entries.iter().find(|entry| entry.rollback_target),
    ) {
        (Some(active_generation), Some(rollback_target))
            if rollback_target.generation != active_generation =>
        {
            Some(summarize_environment_diff(&load_environment_diff(
                storage_root,
                project_id,
                environment,
                rollback_target.generation,
                active_generation,
            )?))
        }
        _ => None,
    };
    let recent_secret_mutations =
        recent_secret_mutations(storage_root, project_id, environment, &truth)?;
    let orphaned_state_warnings = orphaned_state_warnings(&services);
    let volume_repair_events = recent_volume_repair_events(
        &env,
        &[
            truth.active_generation,
            latest_failed_generation,
            truth.latest_generation,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>(),
        truth.active_generation,
        &status_value,
    )?;
    let visible_lifecycle = truth
        .active_lifecycle
        .as_ref()
        .or(truth.latest_lifecycle.as_ref());
    let promotion_gate_reason = visible_lifecycle
        .and_then(|lifecycle| lifecycle.promotion_summary.as_ref())
        .and_then(|summary| summary.gate_reason.clone());
    let validation_summary =
        visible_lifecycle.and_then(|lifecycle| lifecycle.validation_summary.clone());
    let promotion_summary =
        visible_lifecycle.and_then(|lifecycle| lifecycle.promotion_summary.clone());
    let probe_history = load_recent_probe_history(&env, truth.active_generation)?;
    let probe_flapping_assessment = assess_probe_flapping(
        &probe_history,
        validation_summary.as_ref(),
        promotion_summary.as_ref(),
    );
    let warmup_failure_summary = validation_summary.as_ref().and_then(|summary| {
        (!summary.validation_succeeded).then(|| {
            format!(
                "uptime={}s/{}, probes tcp={}/{} http={}/{} restart_stable={} route_stable={}",
                summary.observed_uptime_seconds,
                summary.minimum_uptime_seconds,
                summary.tcp_consecutive_passes,
                summary.required_consecutive_passes,
                summary.http_consecutive_passes,
                summary.required_consecutive_passes,
                summary.restart_count_stable,
                summary.route_verification_stable
            )
        })
    });
    let active_restore = truth.active_generation.and_then(|generation| {
        active_restore_lineage(
            storage_root,
            project_id,
            environment,
            generation,
            status.last_deployment_id.as_deref(),
            history
                .entries
                .iter()
                .find(|entry| entry.generation == generation),
        )
    });
    let backup_restore_events = recent_backup_restore_events(
        &env,
        &truth.active_generation.into_iter().collect::<Vec<_>>(),
    )?;
    let policy_drift_repairs = recent_policy_drift_repairs(
        &env,
        &truth.active_generation.into_iter().collect::<Vec<_>>(),
        truth.active_generation,
        &status_value,
    )?;
    Ok(EnvironmentDiagnostics {
        project_id: project_id.to_string(),
        environment: environment.to_string(),
        status: status.status,
        active_generation: truth.active_generation,
        last_deployment_id: status.last_deployment_id,
        container: ContainerRuntimeDiagnostics {
            container_name: status.container_name,
            running: status.container_running,
            state_status: status.container_status,
            image_ref: status.image_ref,
            network_name: truth.network_name,
            container_ip: status.container_ip,
            started_at: status.container_started_at,
            runtime_policy: Some(status.runtime_policy.clone()),
            runtime_usage: status.runtime_usage.clone(),
            termination: status.termination.clone(),
        },
        route,
        probe_target,
        startup_order: truth.startup_order.clone(),
        services,
        recent_failures,
        latest_validation_failure: latest_failure
            .as_ref()
            .filter(|_| latest_failure_is_current)
            .and_then(|failure| failure.validation_failure.clone()),
        latest_route_activation_failure: latest_failure
            .as_ref()
            .filter(|_| latest_failure_is_current)
            .and_then(|failure| failure.route_activation_failure.clone()),
        likely_failure_stage: latest_failure
            .as_ref()
            .filter(|_| latest_failure_is_current && status_value != "healthy")
            .map(|failure| failure.failure_stage.clone())
            .or_else(|| {
                if status_value == "degraded" {
                    Some("runtime".into())
                } else {
                    None
                }
            }),
        diagnostics_source: latest_failure
            .filter(|_| latest_failure_is_current && status_value != "healthy")
            .map(|failure| failure.diagnostics_source),
        runtime_env_snapshot: truth
            .promoted_runtime_env_snapshot
            .as_ref()
            .map(runtime_env_snapshot_metadata),
        retained_generations: history
            .entries
            .iter()
            .filter(|entry| entry.retained)
            .cloned()
            .collect(),
        rollback_safe_generation: history
            .entries
            .iter()
            .find(|entry| entry.rollback_target)
            .map(|entry| entry.generation),
        recent_gc_actions,
        missing_required_secrets,
        env_drift,
        recent_secret_mutations,
        orphaned_state_warnings: orphaned_state_warnings.clone(),
        volume_repair_events: volume_repair_events.current.clone(),
        current_volume_repair_events: volume_repair_events.current,
        historical_volume_repair_events: volume_repair_events.historical,
        active_lifecycle_state: visible_lifecycle.map(|lifecycle| lifecycle.state.clone()),
        retention_role: truth.active_generation.map(|_| RetentionRole::Current),
        validation_summary,
        promotion_summary,
        last_failed_transition: visible_lifecycle.and_then(|lifecycle| {
            lifecycle
                .transitions
                .iter()
                .rev()
                .find(|transition| transition.state == DeploymentLifecycleState::Failed)
                .map(|transition| transition.transition_reason.clone())
        }),
        promotion_gate_reason,
        warmup_failure_summary,
        restart_instability: visible_lifecycle
            .and_then(|lifecycle| lifecycle.validation_summary.as_ref())
            .is_some_and(|summary| !summary.restart_count_stable),
        probe_flapping: probe_flapping_assessment
            .as_ref()
            .is_some_and(|assessment| assessment.flapping),
        probe_stability: probe_flapping_assessment.map(|assessment| assessment.diagnostics),
        active_restore,
        state_restore_warnings: orphaned_state_warnings.clone(),
        backup_restore_events,
        policy_drift_repairs: policy_drift_repairs.current.clone(),
        current_policy_drift_repairs: policy_drift_repairs.current,
        historical_policy_drift_repairs: policy_drift_repairs.historical,
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
    let generation = load_environment_active_generation(&env)?.ok_or_else(|| {
        ProjectStatusError::RuntimeEnvSnapshotUnavailable("runtime env snapshot unavailable".into())
    })?;
    let snapshot = load_generation_runtime_env_snapshot(&env, generation)?.ok_or_else(|| {
        ProjectStatusError::RuntimeEnvSnapshotUnavailable(
            "runtime env snapshot unavailable for this promoted generation; legacy metadata unavailable, redeploy required".into(),
        )
    })?;
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

pub fn load_environment_diff(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
    from_generation: u64,
    to_generation: u64,
) -> Result<EnvironmentDiffResponse, ProjectStatusError> {
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
    let from_snapshot =
        load_generation_runtime_env_snapshot(&env, from_generation)?.ok_or_else(|| {
            ProjectStatusError::RuntimeEnvSnapshotUnavailable(format!(
                "runtime env snapshot unavailable for generation {from_generation}"
            ))
        })?;
    let to_snapshot =
        load_generation_runtime_env_snapshot(&env, to_generation)?.ok_or_else(|| {
            ProjectStatusError::RuntimeEnvSnapshotUnavailable(format!(
                "runtime env snapshot unavailable for generation {to_generation}"
            ))
        })?;
    let from_resolved =
        load_generation_resolved_runtime(&env, from_generation)?.ok_or_else(|| {
            ProjectStatusError::RuntimeEnvSnapshotUnavailable(format!(
                "resolved runtime unavailable for generation {from_generation}"
            ))
        })?;
    let to_resolved = load_generation_resolved_runtime(&env, to_generation)?.ok_or_else(|| {
        ProjectStatusError::RuntimeEnvSnapshotUnavailable(format!(
            "resolved runtime unavailable for generation {to_generation}"
        ))
    })?;

    compute_environment_diff(
        project_id,
        environment,
        from_generation,
        to_generation,
        &from_snapshot,
        &to_snapshot,
        &from_resolved,
        &to_resolved,
    )
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

fn load_environment_active_generation(
    env: &EnvironmentPaths,
) -> Result<Option<u64>, ProjectStatusError> {
    env.ensure_exists()?;
    Ok(PointerStore::new(env.clone()).read_authoritative_pointer()?)
}

fn load_environment_runtime_truth<D, R>(
    env: &EnvironmentPaths,
    docker: &mut D,
    routing: &mut R,
    project_id: &str,
    environment: &str,
    domain: &str,
) -> Result<EnvironmentRuntimeTruth, ProjectStatusError>
where
    D: DockerRuntime,
    R: RoutingRuntime,
{
    env.ensure_exists()?;
    let current_generation = PointerStore::new(env.clone()).read_pointer("current")?;
    let active_generation = PointerStore::new(env.clone()).read_authoritative_pointer()?;
    let latest_generation = latest_generation(env)?;

    let promoted_snapshot = active_generation
        .map(|generation| load_generation_snapshot_metadata(env, generation))
        .transpose()?
        .flatten();
    let promoted_runtime = active_generation
        .map(|generation| load_generation_runtime_info(env, generation))
        .transpose()?
        .flatten()
        .map(|runtime| runtime_with_primary_service(&runtime));
    let promoted_build = active_generation
        .map(|generation| load_generation_build_info(env, generation))
        .transpose()?
        .flatten();
    let latest_snapshot = latest_generation
        .map(|generation| load_generation_snapshot_metadata(env, generation))
        .transpose()?
        .flatten();
    let latest_build = latest_generation
        .map(|generation| load_generation_build_info(env, generation))
        .transpose()?
        .flatten();
    let active_lifecycle = active_generation
        .map(|generation| load_generation_lifecycle(env, generation))
        .transpose()?
        .flatten();
    let latest_lifecycle = latest_generation
        .map(|generation| load_generation_lifecycle(env, generation))
        .transpose()?
        .flatten();
    let promoted_runtime_env_snapshot = active_generation
        .map(|generation| load_generation_runtime_env_snapshot(env, generation))
        .transpose()?
        .flatten();
    let promoted_generation_issue = active_generation.and_then(|generation| {
        match (
            promoted_runtime.as_ref(),
            promoted_runtime_env_snapshot.as_ref(),
        ) {
            (None, None) => Some(format!(
                "generation {generation} is a legacy promoted generation with incomplete runtime metadata and no runtime env snapshot"
            )),
            (None, Some(_)) => Some(format!(
                "generation {generation} is a legacy promoted generation with incomplete runtime metadata"
            )),
            (Some(_), None) => Some(format!(
                "generation {generation} is a legacy promoted generation; runtime env snapshot metadata unavailable"
            )),
            (Some(_), Some(_)) => None,
        }
    });

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
    let runtime_policy = container_inspection
        .as_ref()
        .map(|inspection| PersistedRuntimePolicy {
            cpu_limit: inspection.cpu_limit.clone(),
            memory_limit_mb: inspection.memory_limit_mb,
            restart_policy: crate::storage::normalize_restart_policy_name(
                &inspection.restart_policy,
            ),
            max_retries: crate::deployments::normalize_restart_max_retries(
                &crate::storage::normalize_restart_policy_name(&inspection.restart_policy),
                inspection.restart_max_retries,
            ),
        })
        .or_else(|| {
            promoted_runtime
                .as_ref()
                .map(|runtime| runtime.runtime_policy.clone())
        })
        .unwrap_or_else(|| PersistedRuntimePolicy {
            restart_policy: "no".into(),
            ..PersistedRuntimePolicy::default()
        });
    let runtime_usage = container_inspection.as_ref().and_then(|inspection| {
        docker
            .container_usage(&inspection.container_name)
            .ok()
            .map(|usage| PersistedRuntimeUsageSnapshot {
                captured_at_unix: usage.captured_at_unix,
                cpu_percent: usage.cpu_percent,
                memory_usage_mb: usage.memory_usage_mb,
                memory_limit_mb: usage.memory_limit_mb,
            })
    });
    let termination = container_inspection
        .as_ref()
        .map(|inspection| PersistedTerminationInfo {
            oom_killed: inspection.oom_killed,
            observed_at_unix: None,
            exit_code: inspection.exit_code,
            last_exit_code: inspection.exit_code,
            exit_signal: inspection.exit_signal,
            finished_at: inspection.finished_at.clone(),
            error: inspection.error.clone(),
            reason: inspection.termination_reason.clone(),
            termination_reason: inspection.termination_reason.clone(),
            stderr_tail: None,
            logs_tail: None,
            restart_count: inspection.restart_count,
        });
    let restart_count = container_inspection
        .as_ref()
        .map(|inspection| inspection.restart_count)
        .unwrap_or(0);
    let startup_order = promoted_runtime
        .as_ref()
        .map(service_startup_order)
        .unwrap_or_default();
    let services = collect_service_runtime_truth(
        docker,
        routing,
        project_id,
        environment,
        &domain,
        promoted_runtime.as_ref(),
        promoted_build.as_ref(),
    );
    let route_details = inspect_route_status(
        routing,
        project_id,
        environment,
        domain,
        promoted_runtime.as_ref(),
        container_inspection.as_ref(),
        network_name.as_deref(),
    );

    Ok(EnvironmentRuntimeTruth {
        current_generation,
        active_generation,
        latest_generation,
        promoted_snapshot,
        promoted_runtime,
        promoted_build,
        latest_snapshot,
        latest_build,
        active_lifecycle,
        latest_lifecycle,
        promoted_runtime_env_snapshot,
        promoted_generation_issue,
        container_running,
        container_status,
        container_started_at,
        network_name,
        container_ip,
        image_ref,
        runtime_policy,
        runtime_usage,
        termination,
        restart_count,
        startup_order,
        services,
        route_details,
    })
}

fn service_startup_order(runtime: &PersistedRuntimeInfo) -> Vec<String> {
    if !runtime.startup_order.is_empty() {
        runtime.startup_order.clone()
    } else if runtime.services.is_empty() {
        vec!["default".into()]
    } else {
        runtime.services.keys().cloned().collect()
    }
}

fn collect_service_runtime_truth<D: DockerRuntime, R: RoutingRuntime>(
    docker: &mut D,
    routing: &mut R,
    project_id: &str,
    environment: &str,
    domain: &str,
    promoted_runtime: Option<&PersistedRuntimeInfo>,
    promoted_build: Option<&PersistedBuildInfo>,
) -> Vec<ServiceRuntimeStatus> {
    let Some(runtime) = promoted_runtime else {
        return Vec::new();
    };
    let is_multi_service = !runtime.services.is_empty();
    let services = if runtime.services.is_empty() {
        BTreeMap::from([(
            "default".into(),
            crate::storage::PersistedServiceRuntimeInfo {
                service_id: "default".into(),
                container_name: runtime.container_name.clone(),
                image_ref: promoted_build
                    .map(|build| build.image_ref.clone())
                    .unwrap_or_default(),
                running: runtime.running,
                state: crate::storage::PersistedServiceState::Healthy,
                network_name: runtime.network_name.clone(),
                probe_path: runtime.probe_path.clone(),
                activation: runtime.activation.clone(),
                command: None,
                runtime_policy: runtime.runtime_policy.clone(),
                runtime_usage: runtime.runtime_usage.clone(),
                termination: runtime.termination.clone(),
                depends_on: Vec::new(),
                required_for_promotion: true,
                externally_exposed: matches!(
                    runtime.activation,
                    Some(PersistedActivationMode::Http { .. })
                ),
                environment_variables: runtime.environment_variables.clone(),
                state_config: None,
                volume_mounts: runtime.volume_mounts.clone(),
                source_ref: runtime.source_ref.clone(),
                repo_url: runtime.repo_url.clone(),
                commit_sha: runtime.commit_sha.clone(),
                source_path: runtime.source_path.clone(),
            },
        )])
    } else {
        runtime.services.clone()
    };

    service_startup_order(runtime)
        .into_iter()
        .filter_map(|service_id| {
            let service = services.get(&service_id)?;
            let inspection = docker.inspect_container(&service.container_name).ok();
            let network_name = service.network_name.clone().or_else(|| {
                inspection
                    .as_ref()
                    .and_then(|value| value.network_ips.keys().next().cloned())
            });
            let container_ip = network_name
                .as_deref()
                .and_then(|network| {
                    inspection
                        .as_ref()
                        .and_then(|value| value.network_ips.get(network).cloned())
                })
                .or_else(|| {
                    inspection
                        .as_ref()
                        .and_then(|value| value.network_ips.values().next().cloned())
                });
            let running = inspection
                .as_ref()
                .map(|value| value.running)
                .unwrap_or(service.running);
            Some(ServiceRuntimeStatus {
                service_id: service.service_id.clone(),
                role: if service.externally_exposed {
                    "exposed".into()
                } else {
                    "internal".into()
                },
                depends_on: service.depends_on.clone(),
                dns_aliases: vec![service.service_id.clone()],
                container_name: Some(service.container_name.clone()),
                image_ref: inspection
                    .as_ref()
                    .map(|value| value.image_ref.clone())
                    .or_else(|| Some(service.image_ref.clone())),
                running,
                state_status: inspection.as_ref().map(|value| value.state_status.clone()),
                lifecycle_state: Some(service.state.clone()),
                network_name: network_name.clone(),
                container_ip,
                internal_port: service_internal_port(service),
                probe_path: service.probe_path.clone(),
                runtime_policy: inspection
                    .as_ref()
                    .map(|value| PersistedRuntimePolicy {
                        cpu_limit: value.cpu_limit.clone(),
                        memory_limit_mb: value.memory_limit_mb,
                        restart_policy: crate::storage::normalize_restart_policy_name(
                            &value.restart_policy,
                        ),
                        max_retries: crate::deployments::normalize_restart_max_retries(
                            &crate::storage::normalize_restart_policy_name(&value.restart_policy),
                            value.restart_max_retries,
                        ),
                    })
                    .unwrap_or_else(|| service.runtime_policy.clone()),
                runtime_usage: inspection.as_ref().and_then(|value| {
                    docker
                        .container_usage(&value.container_name)
                        .ok()
                        .map(|usage| PersistedRuntimeUsageSnapshot {
                            captured_at_unix: usage.captured_at_unix,
                            cpu_percent: usage.cpu_percent,
                            memory_usage_mb: usage.memory_usage_mb,
                            memory_limit_mb: usage.memory_limit_mb,
                        })
                }),
                termination: inspection.as_ref().map(|value| PersistedTerminationInfo {
                    oom_killed: value.oom_killed,
                    observed_at_unix: None,
                    exit_code: value.exit_code,
                    last_exit_code: value.exit_code,
                    exit_signal: value.exit_signal,
                    finished_at: value.finished_at.clone(),
                    error: value.error.clone(),
                    reason: value.termination_reason.clone(),
                    termination_reason: value.termination_reason.clone(),
                    stderr_tail: None,
                    logs_tail: None,
                    restart_count: value.restart_count,
                }),
                restart_count: inspection
                    .as_ref()
                    .map(|value| value.restart_count)
                    .unwrap_or(0),
                last_exit_code: inspection.as_ref().and_then(|value| value.exit_code),
                route: if is_multi_service {
                    service_route_status(
                        routing,
                        project_id,
                        environment,
                        domain,
                        service,
                        inspection.as_ref(),
                        network_name.as_deref(),
                    )
                } else if service.externally_exposed {
                    "active".into()
                } else {
                    "none".into()
                },
                health: service_health_status(service, running),
                failure_reason: None,
                volumes: service_volume_statuses(service, inspection.as_ref()),
                logs_tail: Vec::new(),
            })
        })
        .collect()
}

fn service_volume_statuses(
    service: &PersistedServiceRuntimeInfo,
    inspection: Option<&ContainerInspection>,
) -> Vec<VolumeRuntimeStatus> {
    service
        .volume_mounts
        .iter()
        .map(|mount| {
            let attached = inspection.is_some_and(|value| {
                value.volume_mounts.iter().any(|actual| {
                    actual.volume_name == mount.docker_volume_name
                        && actual.mount_path == mount.mount_path
                })
            });
            let mut warnings = Vec::new();
            if !attached {
                warnings.push(format!(
                    "expected {} at {} is not attached",
                    mount.docker_volume_name, mount.mount_path
                ));
            }
            VolumeRuntimeStatus {
                volume_id: mount.volume_id.clone(),
                docker_volume_name: mount.docker_volume_name.clone(),
                mount_path: mount.mount_path.clone(),
                retention: match mount.retention {
                    PersistedVolumeRetention::Persistent => "persistent".into(),
                    PersistedVolumeRetention::Ephemeral => "ephemeral".into(),
                },
                attached,
                warnings,
            }
        })
        .collect()
}

fn service_internal_port(service: &PersistedServiceRuntimeInfo) -> Option<u16> {
    match service.activation.as_ref() {
        Some(PersistedActivationMode::Http { internal_port, .. }) => Some(*internal_port),
        Some(PersistedActivationMode::Direct) | None => None,
    }
}

fn service_health_status(service: &PersistedServiceRuntimeInfo, running: bool) -> String {
    if !running {
        return "stopped".into();
    }
    if matches!(
        service.state,
        PersistedServiceState::Failed
            | PersistedServiceState::Unstable
            | PersistedServiceState::CrashLoop
            | PersistedServiceState::OomKilled
    ) {
        return "failed".into();
    }
    if service.externally_exposed || service.probe_path.is_some() {
        return "healthy".into();
    }
    "running".into()
}

fn service_route_status<R: RoutingRuntime>(
    routing: &mut R,
    project_id: &str,
    environment: &str,
    domain: &str,
    service: &PersistedServiceRuntimeInfo,
    inspection: Option<&ContainerInspection>,
    network_name: Option<&str>,
) -> String {
    if !service.externally_exposed {
        return "none".into();
    }
    let Some(details) = inspect_service_route_status(
        routing,
        project_id,
        environment,
        domain,
        service,
        inspection,
        network_name,
    ) else {
        return "missing".into();
    };
    if details.matches_truth() {
        "active".into()
    } else if details.inspection.is_some() {
        "mismatch".into()
    } else {
        "missing".into()
    }
}

fn build_environment_status_from_truth(
    queue: Option<&PersistentQueue>,
    project_id: &str,
    environment: &str,
    domain: &str,
    truth: &EnvironmentRuntimeTruth,
) -> Result<ProjectEnvironmentStatus, ProjectStatusError> {
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

    let container_name = truth
        .promoted_runtime
        .as_ref()
        .map(|runtime| runtime.container_name.clone());
    let route_active = truth
        .route_details
        .as_ref()
        .and_then(|details| details.inspection.as_ref())
        .is_some();
    let route_matches = truth
        .route_details
        .as_ref()
        .is_some_and(RouteStatusDetails::matches_truth);
    let route_required = truth
        .route_details
        .as_ref()
        .is_some_and(RouteStatusDetails::route_required);
    let promoted_snapshot_healthy = truth
        .promoted_snapshot
        .as_ref()
        .is_some_and(|snapshot| snapshot.state == "healthy");
    let visible_lifecycle = truth
        .active_lifecycle
        .as_ref()
        .or(truth.latest_lifecycle.as_ref());
    let latest_failed_without_promotion = truth.current_generation.is_none()
        && truth
            .latest_lifecycle
            .as_ref()
            .is_some_and(|lifecycle| lifecycle.state == DeploymentLifecycleState::Failed);

    let status = if deploying {
        "deploying"
    } else if truth.active_generation.is_some()
        && truth.promoted_generation_issue.is_none()
        && promoted_snapshot_healthy
        && truth.container_running
        && (!route_required || route_matches)
    {
        "healthy"
    } else if truth.current_generation.is_none()
        && (truth
            .latest_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.state == "failed")
            || latest_failed_without_promotion)
    {
        "failed"
    } else if truth.current_generation.is_none()
        && truth.active_generation.is_none()
        && truth.latest_generation.is_none()
        && truth.promoted_runtime.is_none()
        && truth.promoted_build.is_none()
    {
        "missing"
    } else {
        "degraded"
    };

    Ok(ProjectEnvironmentStatus {
        project_id: project_id.to_string(),
        environment: environment.to_string(),
        status: status.into(),
        active_generation: truth.active_generation,
        domain: domain.to_string(),
        commit_sha: truth
            .promoted_build
            .as_ref()
            .and_then(|build| build.commit_sha.clone())
            .or_else(|| {
                truth
                    .promoted_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.commit_sha.clone())
            }),
        source_ref: truth
            .promoted_build
            .as_ref()
            .and_then(|build| build.source_ref.clone())
            .or_else(|| {
                truth
                    .promoted_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.source_ref.clone())
            }),
        container_name,
        container_running: truth.container_running,
        container_status: truth.container_status.clone(),
        network_name: truth.network_name.clone(),
        container_ip: truth.container_ip.clone(),
        route_active,
        probe_path: truth
            .promoted_runtime
            .as_ref()
            .and_then(|runtime| runtime.probe_path.clone()),
        image_ref: truth.image_ref.clone(),
        runtime_policy: truth.runtime_policy.clone(),
        runtime_usage: truth.runtime_usage.clone(),
        termination: truth.termination.clone(),
        restart_count: truth.restart_count,
        startup_order: truth.startup_order.clone(),
        services: truth.services.clone(),
        last_deployment_id: truth
            .promoted_build
            .as_ref()
            .map(|build| build.deployment_id.clone())
            .or_else(|| {
                truth
                    .latest_build
                    .as_ref()
                    .map(|build| build.deployment_id.clone())
            }),
        deployed_at_unix: truth
            .promoted_snapshot
            .as_ref()
            .map(|snapshot| snapshot.finalized_at_unix)
            .or_else(|| {
                truth
                    .latest_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.finalized_at_unix)
            }),
        container_started_at: truth.container_started_at.clone(),
        runtime_env_snapshot: truth
            .promoted_runtime_env_snapshot
            .as_ref()
            .map(runtime_env_snapshot_metadata),
        lifecycle_state: visible_lifecycle.map(|lifecycle| lifecycle.state.clone()),
        retention_role: truth.active_generation.map(|_| RetentionRole::Current),
        validation_summary: visible_lifecycle
            .and_then(|lifecycle| lifecycle.validation_summary.clone()),
        promotion_summary: visible_lifecycle
            .and_then(|lifecycle| lifecycle.promotion_summary.clone()),
        uptime_seconds: visible_lifecycle.and_then(|lifecycle| {
            lifecycle
                .validation_summary
                .as_ref()
                .map(|summary| summary.observed_uptime_seconds)
        }),
    })
}

fn orphaned_state_warnings(services: &[ServiceRuntimeStatus]) -> Vec<String> {
    services
        .iter()
        .flat_map(|service| {
            service.volumes.iter().flat_map(|volume| {
                volume
                    .warnings
                    .iter()
                    .map(|warning| format!("service {}: {warning}", service.service_id))
            })
        })
        .collect()
}

fn normalize_repair_event_line(line: &str) -> String {
    line.replace("restart_policy: \"\"", "restart_policy: no")
        .replace("restart_policy=\"\"", "restart_policy=no")
}

fn bucket_repair_events(
    env_status: &str,
    active_generation: Option<u64>,
    generations: &[u64],
    matches_event: impl Fn(&EventRecord) -> bool,
    render_default_reason: impl Fn(u64) -> String,
    env: &EnvironmentPaths,
) -> Result<RepairEventBuckets, ProjectStatusError> {
    if env_status == "healthy" {
        return Ok(RepairEventBuckets::default());
    }

    let mut current = Vec::new();
    let mut historical = Vec::new();
    let mut seen_current = BTreeSet::new();
    let mut seen_historical = BTreeSet::new();

    for generation in generations {
        let path = env.generation_dir(*generation).join("events.jsonl");
        if !path.exists() {
            continue;
        }
        let raw = fs::read_to_string(path)?;
        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let event = serde_json::from_str::<EventRecord>(line).map_err(|err| {
                ProjectStatusError::Storage(StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    err.to_string(),
                )))
            })?;
            if !matches_event(&event) {
                continue;
            }

            let rendered = normalize_repair_event_line(&format!(
                "gen-{}: {}",
                generation,
                event
                    .reason
                    .unwrap_or_else(|| render_default_reason(*generation))
            ));
            if Some(*generation) == active_generation {
                if seen_current.insert(rendered.clone()) {
                    current.push(rendered);
                }
            } else {
                let rendered = format!("historical {rendered}");
                if seen_historical.insert(rendered.clone()) {
                    historical.push(rendered);
                }
            }
        }
    }

    current.reverse();
    current.truncate(5);
    historical.reverse();
    historical.truncate(5);

    Ok(RepairEventBuckets {
        current,
        historical,
    })
}

fn recent_volume_repair_events(
    env: &EnvironmentPaths,
    generations: &[u64],
    active_generation: Option<u64>,
    env_status: &str,
) -> Result<RepairEventBuckets, ProjectStatusError> {
    bucket_repair_events(
        env_status,
        active_generation,
        generations,
        |event| event.event_type == "VOLUME_ATTACHMENT_REPAIRED",
        |generation| format!("generation {} volume attachment repaired", generation),
        env,
    )
}

fn recent_backup_restore_events(
    env: &EnvironmentPaths,
    generations: &[u64],
) -> Result<Vec<String>, ProjectStatusError> {
    let mut events = Vec::new();
    for generation in generations {
        let path = env.generation_dir(*generation).join("events.jsonl");
        if !path.exists() {
            continue;
        }
        let raw = fs::read_to_string(path)?;
        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let event = serde_json::from_str::<EventRecord>(line).map_err(|err| {
                ProjectStatusError::Storage(StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    err.to_string(),
                )))
            })?;
            if matches!(
                event.event_type.as_str(),
                "BACKUP_CREATED" | "BACKUP_RESTORE_COMPLETED"
            ) {
                events.push(event.reason.unwrap_or_else(|| event.event_type));
            }
        }
    }
    events.reverse();
    events.truncate(5);
    Ok(events)
}

fn recent_policy_drift_repairs(
    env: &EnvironmentPaths,
    generations: &[u64],
    active_generation: Option<u64>,
    env_status: &str,
) -> Result<RepairEventBuckets, ProjectStatusError> {
    bucket_repair_events(
        env_status,
        active_generation,
        generations,
        |event| event.event_type == "RUNTIME_POLICY_DRIFT_REPAIRED",
        |generation| format!("generation {} runtime policy drift repaired", generation),
        env,
    )
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
    failed_service_name: Option<String>,
    failure_reason: String,
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
        failed_service_name: summary.failed_service_name.clone(),
        failure_reason: summary.failure_reason.clone(),
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
            blocking_service_name: summary
                .blocking_service_name
                .or(summary.failed_service_name.clone()),
            historical: false,
            validation_failure_summary,
            diagnostics_source,
        },
    }))
}

fn mark_failure_historical(
    mut failure: RecentDeploymentFailure,
    active_generation: Option<u64>,
    status: &str,
) -> RecentDeploymentFailure {
    failure.historical = failure.generation != active_generation.unwrap_or(failure.generation)
        || status == "healthy";
    failure
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

fn service_logs_artifact_name(service_id: &str) -> String {
    format!("service-{service_id}-container_logs_tail.log")
}

fn structured_service_logs_artifact_name(service_id: &str) -> String {
    format!("services/{service_id}/container_logs_tail.log")
}

fn load_service_logs_tail(
    diagnostics: &DiagnosticsStore,
    service_id: &str,
) -> Result<Vec<String>, ProjectStatusError> {
    let logs = diagnostics
        .read_text_artifact(&structured_service_logs_artifact_name(service_id))?
        .or(diagnostics.read_text_artifact(&service_logs_artifact_name(service_id))?)
        .or(if service_id == "default" {
            diagnostics.read_text_artifact("container_logs_tail.log")?
        } else {
            None
        })
        .unwrap_or_default();
    Ok(logs.lines().map(|line| line.to_string()).collect())
}

fn enrich_services_with_diagnostics(
    env: &EnvironmentPaths,
    generation: Option<u64>,
    services: &[ServiceRuntimeStatus],
    latest_failure: Option<&FailureDetails>,
) -> Result<Vec<ServiceRuntimeStatus>, ProjectStatusError> {
    let Some(generation) = generation else {
        return Ok(services.to_vec());
    };
    let diagnostics = DiagnosticsStore::new(env.clone(), generation);
    services
        .iter()
        .cloned()
        .map(|mut service| {
            service.logs_tail = load_service_logs_tail(&diagnostics, &service.service_id)?;
            service.failure_reason = match latest_failure {
                Some(failure)
                    if failure.failed_service_name.as_deref() == Some(&service.service_id) =>
                {
                    Some(failure.failure_reason.clone())
                }
                _ if service.health == "failed" => Some(
                    service
                        .state_status
                        .clone()
                        .unwrap_or_else(|| "service reported failed state".into()),
                ),
                _ => None,
            };
            Ok(service)
        })
        .collect()
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

fn compute_environment_diff(
    project_id: &str,
    environment: &str,
    from_generation: u64,
    to_generation: u64,
    from_snapshot: &PersistedRuntimeEnvSnapshot,
    to_snapshot: &PersistedRuntimeEnvSnapshot,
    from_resolved: &PersistedResolvedRuntime,
    to_resolved: &PersistedResolvedRuntime,
) -> Result<EnvironmentDiffResponse, ProjectStatusError> {
    let from_values = restore_runtime_env(from_resolved).map_err(|err| {
        ProjectStatusError::RuntimeEnvSnapshotUnavailable(format!(
            "failed to restore generation {from_generation} runtime env: {err}"
        ))
    })?;
    let to_values = restore_runtime_env(to_resolved).map_err(|err| {
        ProjectStatusError::RuntimeEnvSnapshotUnavailable(format!(
            "failed to restore generation {to_generation} runtime env: {err}"
        ))
    })?;

    let mut keys = BTreeSet::new();
    keys.extend(from_snapshot.entries.keys().cloned());
    keys.extend(to_snapshot.entries.keys().cloned());

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed_values = Vec::new();
    let mut changed_secret_references = Vec::new();

    for key in keys {
        let left = from_snapshot.entries.get(&key);
        let right = to_snapshot.entries.get(&key);
        match (left, right) {
            (None, Some(entry)) => added.push(EnvironmentDiffEntry {
                key,
                value: render_snapshot_value(entry),
            }),
            (Some(entry), None) => removed.push(EnvironmentDiffEntry {
                key,
                value: render_snapshot_value(entry),
            }),
            (Some(left_entry), Some(right_entry)) => {
                let left_reference = left_entry
                    .secret_reference
                    .as_ref()
                    .map(secret_reference_name);
                let right_reference = right_entry
                    .secret_reference
                    .as_ref()
                    .map(secret_reference_name);
                if left_reference != right_reference {
                    changed_secret_references.push(SecretReferenceChange {
                        key: key.clone(),
                        before_reference: left_reference,
                        after_reference: right_reference,
                        before: render_diff_value(left_entry, "<secret reference changed>"),
                        after: render_diff_value(right_entry, "<secret reference changed>"),
                    });
                    continue;
                }

                let left_value = from_values.get(&key).cloned().unwrap_or_default();
                let right_value = to_values.get(&key).cloned().unwrap_or_default();
                if left_value != right_value {
                    changed_values.push(EnvironmentValueChange {
                        key,
                        before: render_diff_value(left_entry, "<secret changed>"),
                        after: render_diff_value(right_entry, "<secret changed>"),
                    });
                }
            }
            (None, None) => {}
        }
    }

    Ok(EnvironmentDiffResponse {
        project_id: project_id.to_string(),
        environment: environment.to_string(),
        from_generation,
        to_generation,
        added,
        removed,
        changed_values,
        changed_secret_references,
    })
}

fn render_diff_value(
    entry: &crate::storage::PersistedRuntimeEnvEntry,
    secret_label: &str,
) -> String {
    if entry.redacted {
        secret_label.into()
    } else {
        entry.value.clone().unwrap_or_default()
    }
}

fn secret_reference_name(reference: &crate::storage::PersistedSecretReference) -> String {
    format!("{}:{}", reference.scope, reference.key)
}

fn summarize_environment_diff(diff: &EnvironmentDiffResponse) -> EnvironmentDiffSummary {
    EnvironmentDiffSummary {
        from_generation: diff.from_generation,
        to_generation: diff.to_generation,
        added: diff.added.len(),
        removed: diff.removed.len(),
        changed_values: diff.changed_values.len(),
        changed_secret_references: diff.changed_secret_references.len(),
    }
}

fn missing_required_secrets(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
    truth: &EnvironmentRuntimeTruth,
) -> Result<Vec<String>, ProjectStatusError> {
    let project = ProjectRegistryStore::new(storage_root)
        .get(project_id)
        .map_err(|err| {
            ProjectStatusError::ProjectLookup(format!(
                "project lookup failed for {project_id}: {err}"
            ))
        })?
        .ok_or(ProjectStatusError::ProjectNotFound)?;
    let mut contexts = Vec::new();
    if let Some(path) = truth
        .latest_build
        .as_ref()
        .and_then(|build| build.source_path.clone())
    {
        contexts.push(path);
    }
    if let Some(path) = truth
        .promoted_build
        .as_ref()
        .and_then(|build| build.source_path.clone())
    {
        contexts.push(path);
    }
    let repo_path = Path::new(&project.repo_url);
    if repo_path.exists() {
        contexts.push(repo_path.to_path_buf());
    }

    let store = SecretStore::new(storage_root.join("secrets")).map_err(|err| {
        ProjectStatusError::Storage(StorageError::Io(std::io::Error::other(err.to_string())))
    })?;

    for context in contexts {
        if let Some(manifest) = load_optional_manifest(&context)
            .map_err(|err| ProjectStatusError::ProjectLookup(err.to_string()))?
        {
            let mut missing = manifest
                .environment_variables
                .into_iter()
                .filter(|(_, reference)| reference.scope == "environment")
                .filter_map(|(_, reference)| {
                    (!store.has_environment_secret(project_id, environment, &reference.key))
                        .then_some(reference.key)
                })
                .collect::<Vec<_>>();
            missing.sort();
            missing.dedup();
            return Ok(missing);
        }
        if let Some(forge_yaml) = load_optional_forge_yaml(&context, project_id)
            .map_err(|err| ProjectStatusError::ProjectLookup(err.to_string()))?
        {
            let mut missing = forge_yaml
                .environment()
                .keys()
                .filter(|key| crate::runtime_env::is_sensitive_key(key))
                .filter(|key| !store.has_environment_secret(project_id, environment, key))
                .cloned()
                .collect::<Vec<_>>();
            missing.sort();
            missing.dedup();
            return Ok(missing);
        }
    }

    Ok(Vec::new())
}

fn recent_secret_mutations(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
    truth: &EnvironmentRuntimeTruth,
) -> Result<Vec<SecretMutationDiagnostic>, ProjectStatusError> {
    let Some(active_generation) = truth.active_generation else {
        return Ok(Vec::new());
    };
    let Some(active_snapshot) = truth.promoted_runtime_env_snapshot.as_ref() else {
        return Ok(Vec::new());
    };
    let env = EnvironmentPaths::new(storage_root, project_id, environment);
    let Some(active_resolved) = load_generation_resolved_runtime(&env, active_generation)? else {
        return Ok(Vec::new());
    };
    let active_values = restore_runtime_env(&active_resolved).map_err(|err| {
        ProjectStatusError::RuntimeEnvSnapshotUnavailable(format!(
            "failed to restore generation {active_generation} runtime env: {err}"
        ))
    })?;
    let store = SecretStore::new(storage_root.join("secrets")).map_err(|err| {
        ProjectStatusError::Storage(StorageError::Io(std::io::Error::other(err.to_string())))
    })?;
    let finalized_at = truth
        .promoted_snapshot
        .as_ref()
        .map(|snapshot| snapshot.finalized_at_unix)
        .unwrap_or_default();

    let mut diagnostics = Vec::new();
    for (env_key, entry) in &active_snapshot.entries {
        let Some(reference) = entry.secret_reference.as_ref() else {
            continue;
        };
        if reference.scope != "environment" {
            continue;
        }
        let Some((updated_at, mutations)) = store
            .metadata_for_secret(project_id, environment, &reference.key)
            .map_err(|err| {
                ProjectStatusError::Storage(StorageError::Io(std::io::Error::other(
                    err.to_string(),
                )))
            })?
        else {
            continue;
        };
        if updated_at <= finalized_at {
            continue;
        }
        let current_value = store
            .current_secret_value(project_id, environment, &reference.key)
            .map_err(|err| {
                ProjectStatusError::Storage(StorageError::Io(std::io::Error::other(
                    err.to_string(),
                )))
            })?;
        let active_value = active_values.get(env_key).cloned().unwrap_or_default();
        let mutation = match current_value {
            None => "unset",
            Some(ref value) if *value != active_value => "rotated",
            Some(_) => mutations
                .last()
                .map(|mutation| mutation.action.as_str())
                .unwrap_or("updated"),
        };
        diagnostics.push(SecretMutationDiagnostic {
            key: reference.key.clone(),
            mutation: mutation.to_string(),
            updated_at_unix: updated_at,
            active_generation,
        });
    }
    diagnostics.sort_by(|left, right| right.updated_at_unix.cmp(&left.updated_at_unix));
    diagnostics.dedup_by(|left, right| left.key == right.key);
    Ok(diagnostics)
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
        route_subtree_id: persisted_subtree_id,
        ..
    } = runtime.activation.as_ref()?
    else {
        return None;
    };
    let subtree_id = persisted_subtree_id
        .clone()
        .unwrap_or_else(|| route_subtree_id(project_id, environment));
    let inspection = routing.inspect_route(&subtree_id).ok();
    let expected_target = container.and_then(|container| {
        expected_route_for_runtime(
            project_id,
            environment,
            Some(domain.to_string()),
            runtime,
            container,
            network_name,
        )
        .map(|route| route.target)
    });
    Some(RouteStatusDetails {
        inspection,
        expected_target,
        expected_domain: domain.to_string(),
        route_required: true,
    })
}

fn inspect_service_route_status<R: RoutingRuntime>(
    routing: &mut R,
    project_id: &str,
    environment: &str,
    domain: &str,
    service: &PersistedServiceRuntimeInfo,
    container: Option<&ContainerInspection>,
    network_name: Option<&str>,
) -> Option<RouteStatusDetails> {
    let PersistedActivationMode::Http {
        route_subtree_id: persisted_subtree_id,
        ..
    } = service.activation.as_ref()?
    else {
        return None;
    };
    let inspection = routing
        .inspect_route(
            persisted_subtree_id
                .as_deref()
                .unwrap_or(&route_subtree_id(project_id, environment)),
        )
        .ok();
    let service_runtime = PersistedRuntimeInfo {
        container_name: service.container_name.clone(),
        running: service.running,
        network_name: service.network_name.clone(),
        probe_path: service.probe_path.clone(),
        activation: service.activation.clone(),
        runtime_policy: service.runtime_policy.clone(),
        runtime_usage: service.runtime_usage.clone(),
        termination: service.termination.clone(),
        environment_variables: service.environment_variables.clone(),
        volume_mounts: service.volume_mounts.clone(),
        source_ref: service.source_ref.clone(),
        repo_url: service.repo_url.clone(),
        commit_sha: service.commit_sha.clone(),
        source_path: service.source_path.clone(),
        services: BTreeMap::new(),
        startup_order: Vec::new(),
    };
    let expected_target = container.and_then(|container| {
        expected_route_for_runtime(
            project_id,
            environment,
            Some(domain.to_string()),
            &service_runtime,
            container,
            network_name,
        )
        .map(|route| route.target)
    });
    Some(RouteStatusDetails {
        inspection,
        expected_target,
        expected_domain: domain.to_string(),
        route_required: true,
    })
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
        DiagnosticSummary, EventStore, LifecycleStore, PersistedProbeHistory,
        PersistedProbeHistoryEntry, PersistedProbeType, PersistedRouteTargetSource,
        PersistedRuntimeInfo, PersistedServiceRuntimeInfo, PersistedServiceState, PointerStore,
        ProbeHistoryStore, RuntimeHealthState, RuntimeState, RuntimeStateStore, SnapshotState,
        SnapshotWriter, atomic_write,
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

        fn ensure_volume(
            &mut self,
            _request: crate::runtime::CreateVolumeRequest,
        ) -> Result<(), DockerRuntimeError> {
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

        fn list_managed_volumes(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedVolume>, DockerRuntimeError> {
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

        fn remove_volume(&mut self, _volume_name: &str) -> Result<(), DockerRuntimeError> {
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
            runtime_policy: PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            environment_variables: BTreeMap::new(),
            volume_mounts: Vec::new(),
            source_ref: Some("main".into()),
            repo_url: None,
            commit_sha: Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac".into()),
            source_path: None,
            services: BTreeMap::new(),
            startup_order: Vec::new(),
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
            .write_artifact(
                "resolved_runtime.json",
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
                        "    \"FORGE_PROJECT_ID\": {{ \"source\": \"forge_generated\", \"value\": \"api\", \"sensitive\": false }},\n",
                        "    \"FORGE_ENVIRONMENT\": {{ \"source\": \"forge_generated\", \"value\": \"staging\", \"sensitive\": false }},\n",
                        "    \"API_BASE_URL\": {{ \"source\": \"forge_yaml\", \"value\": \"https://api.example.com\", \"sensitive\": false }},\n",
                        "    \"DATABASE_URL\": {{ \"source\": \"project_environment_secret\", \"value\": \"<secret>\", \"sensitive\": true }}\n",
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

    fn write_generation_with_deployment_id(root: &Path, generation: u64, deployment_id: &str) {
        write_generation(root, generation);
        let env = EnvironmentPaths::new(root, "api", "staging");
        let generation_dir = env.generation_dir(generation);
        fs::write(
            generation_dir.join("build.json"),
            format!(
                concat!(
                    "{{\n",
                    "  \"deployment_id\": \"{deployment_id}\",\n",
                    "  \"image_ref\": \"forge/api:staging-gen-{generation}\",\n",
                    "  \"source_ref\": \"main\",\n",
                    "  \"commit_sha\": \"340ac8108006d84dbf951d8c0bb04ecfaf0eccac\"\n",
                    "}}\n"
                ),
                deployment_id = deployment_id,
                generation = generation,
            ),
        )
        .unwrap();
    }

    fn write_backup_metadata_fixture(
        root: &Path,
        backup_id: &str,
        restored_generation: u64,
        restored_deployment_id: &str,
        restored_at_unix: u64,
    ) {
        let backup_dir = EnvironmentPaths::backups_root(root)
            .join("api")
            .join("staging")
            .join(backup_id);
        fs::create_dir_all(&backup_dir).unwrap();
        let metadata = crate::storage::PersistedBackupMetadata {
            backup_version: 1,
            backup_id: backup_id.into(),
            project_id: "api".into(),
            environment: "staging".into(),
            created_at_unix: 10,
            source_generation: 3,
            source_deployment_id: Some("dep-3".into()),
            snapshot_metadata: crate::storage::PersistedSnapshotMetadata {
                snapshot_version: 1,
                project_id: "api".into(),
                environment: "staging".into(),
                generation: 3,
                state: "healthy".into(),
                finalized_at_unix: 10,
            },
            build_info: crate::storage::PersistedBuildInfo {
                deployment_id: "dep-3".into(),
                image_ref: "forge/api:staging-gen-3".into(),
                services: BTreeMap::new(),
                source_ref: Some("main".into()),
                repo_url: None,
                commit_sha: Some("deadbeef".into()),
                source_path: None,
            },
            runtime_info: crate::storage::PersistedRuntimeInfo {
                container_name: "staging-api-gen-3".into(),
                running: true,
                network_name: Some("forge-managed".into()),
                probe_path: Some("/health".into()),
                activation: Some(PersistedActivationMode::Http {
                    internal_port: 3000,
                    route_subtree_id: Some("forge:api:staging".into()),
                    target_source: crate::storage::PersistedRouteTargetSource::ContainerIp,
                }),
                runtime_policy: PersistedRuntimePolicy::default(),
                runtime_usage: None,
                termination: None,
                environment_variables: BTreeMap::new(),
                volume_mounts: Vec::new(),
                source_ref: Some("main".into()),
                repo_url: None,
                commit_sha: Some("deadbeef".into()),
                source_path: None,
                services: BTreeMap::new(),
                startup_order: Vec::new(),
            },
            runtime_env_snapshot: None,
            resolved_runtime: crate::storage::PersistedResolvedRuntime {
                snapshot_version: 1,
                project_id: "api".into(),
                environment: "staging".into(),
                generation: 3,
                deployment_id: "dep-3".into(),
                source_environment: "staging".into(),
                source_ref: Some("main".into()),
                commit_sha: Some("deadbeef".into()),
                domain: Some("staging-api.example.com".into()),
                entries: BTreeMap::new(),
            },
            services: vec!["default".into()],
            volumes: Vec::new(),
            hooks: Vec::new(),
            restores: vec![crate::storage::PersistedBackupRestoreRecord {
                restored_generation,
                restored_deployment_id: restored_deployment_id.into(),
                restored_at_unix,
                status: "completed".into(),
            }],
            warnings: Vec::new(),
        };
        fs::write(
            backup_dir.join("metadata.json"),
            serde_json::to_vec_pretty(&metadata).unwrap(),
        )
        .unwrap();
    }

    fn write_multiservice_generation(root: &Path, generation: u64) {
        let env = EnvironmentPaths::new(root, "api", "staging");
        let writer = SnapshotWriter::new(env.clone(), generation).unwrap();
        writer
            .write_artifact(
                "build.json",
                &format!(
                    concat!(
                        "{{\n",
                        "  \"deployment_id\": \"dep-ms-{generation}\",\n",
                        "  \"image_ref\": \"forge/api:staging-gen-{generation}\",\n",
                        "  \"services\": {{\n",
                        "    \"api\": {{\"service_id\": \"api\", \"image_ref\": \"forge/api:staging-gen-{generation}\"}},\n",
                        "    \"worker\": {{\"service_id\": \"worker\", \"image_ref\": \"forge/worker:staging-gen-{generation}\"}}\n",
                        "  }}\n",
                        "}}\n"
                    ),
                    generation = generation,
                ),
            )
            .unwrap();
        let runtime = PersistedRuntimeInfo {
            container_name: format!("staging-api-gen-{generation}"),
            running: true,
            network_name: Some("forge-managed".into()),
            probe_path: Some("/health".into()),
            activation: Some(PersistedActivationMode::Http {
                internal_port: 3000,
                route_subtree_id: Some("forge:api:staging:api".into()),
                target_source: PersistedRouteTargetSource::ContainerIp,
            }),
            runtime_policy: PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            environment_variables: BTreeMap::new(),
            volume_mounts: Vec::new(),
            source_ref: Some("main".into()),
            repo_url: None,
            commit_sha: Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac".into()),
            source_path: None,
            services: BTreeMap::from([
                (
                    "api".into(),
                    PersistedServiceRuntimeInfo {
                        service_id: "api".into(),
                        container_name: format!("staging-api-api-gen-{generation}"),
                        image_ref: format!("forge/api:staging-gen-{generation}"),
                        running: true,
                        state: PersistedServiceState::Healthy,
                        network_name: Some("forge-managed".into()),
                        probe_path: Some("/health".into()),
                        activation: Some(PersistedActivationMode::Http {
                            internal_port: 3000,
                            route_subtree_id: Some("forge:api:staging:api".into()),
                            target_source: PersistedRouteTargetSource::ContainerIp,
                        }),
                        command: None,
                        runtime_policy: PersistedRuntimePolicy {
                            restart_policy: "no".into(),
                            ..PersistedRuntimePolicy::default()
                        },
                        runtime_usage: None,
                        termination: None,
                        depends_on: Vec::new(),
                        required_for_promotion: true,
                        externally_exposed: true,
                        environment_variables: BTreeMap::new(),
                        state_config: None,
                        volume_mounts: Vec::new(),
                        source_ref: Some("main".into()),
                        repo_url: None,
                        commit_sha: Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac".into()),
                        source_path: None,
                    },
                ),
                (
                    "worker".into(),
                    PersistedServiceRuntimeInfo {
                        service_id: "worker".into(),
                        container_name: format!("staging-api-worker-gen-{generation}"),
                        image_ref: format!("forge/worker:staging-gen-{generation}"),
                        running: true,
                        state: PersistedServiceState::Healthy,
                        network_name: Some("forge-managed".into()),
                        probe_path: None,
                        activation: Some(PersistedActivationMode::Direct),
                        command: None,
                        runtime_policy: PersistedRuntimePolicy {
                            restart_policy: "no".into(),
                            ..PersistedRuntimePolicy::default()
                        },
                        runtime_usage: None,
                        termination: None,
                        depends_on: vec!["api".into()],
                        required_for_promotion: false,
                        externally_exposed: false,
                        environment_variables: BTreeMap::new(),
                        state_config: None,
                        volume_mounts: Vec::new(),
                        source_ref: Some("main".into()),
                        repo_url: None,
                        commit_sha: Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac".into()),
                        source_path: None,
                    },
                ),
            ]),
            startup_order: vec!["api".into(), "worker".into()],
        };
        writer
            .write_artifact(
                "runtime.json",
                &format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()),
            )
            .unwrap();
        writer
            .finalize("api", "staging", SnapshotState::Healthy)
            .unwrap();
        PointerStore::new(env.clone())
            .swap_current(generation)
            .unwrap();
        RuntimeStateStore::new(env.clone())
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
        let diagnostics = DiagnosticsStore::new(env, generation);
        diagnostics
            .write_artifact("service-api-container_logs_tail.log", "api ready\n", &[])
            .unwrap();
        diagnostics
            .write_artifact(
                "service-worker-container_logs_tail.log",
                "worker polling\n",
                &[],
            )
            .unwrap();
    }

    fn write_stateful_generation(root: &Path, generation: u64) {
        let env = EnvironmentPaths::new(root, "api", "staging");
        let writer = SnapshotWriter::new(env.clone(), generation).unwrap();
        let runtime = PersistedRuntimeInfo {
            container_name: format!("staging-api-db-gen-{generation}"),
            running: true,
            network_name: Some("forge-managed".into()),
            probe_path: None,
            activation: Some(PersistedActivationMode::Direct),
            runtime_policy: PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            environment_variables: BTreeMap::new(),
            volume_mounts: vec![crate::storage::PersistedVolumeMount {
                volume_id: "postgres-data".into(),
                docker_volume_name: "forge-api-staging-vol-postgres-data".into(),
                mount_path: "/var/lib/postgresql/data".into(),
                service_id: "db".into(),
                generation,
                retention: PersistedVolumeRetention::Persistent,
            }],
            source_ref: Some("main".into()),
            repo_url: None,
            commit_sha: Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac".into()),
            source_path: None,
            services: BTreeMap::from([(
                "db".into(),
                PersistedServiceRuntimeInfo {
                    service_id: "db".into(),
                    container_name: format!("staging-api-db-gen-{generation}"),
                    image_ref: "postgres:16".into(),
                    running: true,
                    state: PersistedServiceState::Healthy,
                    network_name: Some("forge-managed".into()),
                    probe_path: None,
                    activation: Some(PersistedActivationMode::Direct),
                    command: None,
                    runtime_policy: PersistedRuntimePolicy {
                        restart_policy: "no".into(),
                        ..PersistedRuntimePolicy::default()
                    },
                    runtime_usage: None,
                    termination: None,
                    depends_on: Vec::new(),
                    required_for_promotion: true,
                    externally_exposed: false,
                    environment_variables: BTreeMap::new(),
                    state_config: None,
                    volume_mounts: vec![crate::storage::PersistedVolumeMount {
                        volume_id: "postgres-data".into(),
                        docker_volume_name: "forge-api-staging-vol-postgres-data".into(),
                        mount_path: "/var/lib/postgresql/data".into(),
                        service_id: "db".into(),
                        generation,
                        retention: PersistedVolumeRetention::Persistent,
                    }],
                    source_ref: Some("main".into()),
                    repo_url: None,
                    commit_sha: Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac".into()),
                    source_path: None,
                },
            )]),
            startup_order: vec!["db".into()],
        };
        writer
            .write_artifact(
                "runtime.json",
                &format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()),
            )
            .unwrap();
        writer
            .write_artifact(
                "build.json",
                &format!(
                    "{{\"deployment_id\":\"dep-{generation}\",\"image_ref\":\"postgres:16\"}}\n"
                ),
            )
            .unwrap();
        writer
            .write_artifact(
                "runtime_env_snapshot.json",
                &format!(
                    "{{\"snapshot_version\":1,\"project_id\":\"api\",\"environment\":\"staging\",\"generation\":{generation},\"deployment_id\":\"dep-{generation}\",\"source_environment\":\"staging\",\"entries\":{{}}}}\n"
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

    fn write_lifecycle_state(root: &Path, generation: u64, state: DeploymentLifecycleState) {
        let env = EnvironmentPaths::new(root, "api", "staging");
        LifecycleStore::new(env, generation)
            .write(&PersistedDeploymentLifecycle {
                lifecycle_version: 1,
                project_id: "api".into(),
                environment: "staging".into(),
                generation,
                state: state.clone(),
                entered_at_unix: generation,
                transition_reason: format!("gen-{generation}-{state:?}").to_lowercase(),
                validation_summary: None,
                promotion_summary: None,
                transitions: vec![crate::storage::DeploymentLifecycleTransition {
                    state,
                    entered_at_unix: generation,
                    transition_reason: format!("gen-{generation}"),
                    validation_summary: None,
                    promotion_summary: None,
                }],
            })
            .unwrap();
    }

    fn write_probe_history(
        root: &Path,
        generation: u64,
        entries: Vec<(u64, PersistedProbeType, bool, u64, Option<&str>)>,
    ) {
        let env = EnvironmentPaths::new(root, "api", "staging");
        ProbeHistoryStore::new(env, generation)
            .write(&PersistedProbeHistory {
                entries: entries
                    .into_iter()
                    .map(
                        |(timestamp_unix, probe_type, success, latency_ms, failure_reason)| {
                            PersistedProbeHistoryEntry {
                                timestamp_unix,
                                probe_type,
                                success,
                                latency_ms,
                                failure_reason: failure_reason.map(str::to_string),
                            }
                        },
                    )
                    .collect(),
            })
            .unwrap();
    }

    fn write_validation_lifecycle(
        root: &Path,
        generation: u64,
        state: DeploymentLifecycleState,
        validation_summary: PersistedValidationSummary,
        promotion_summary: PersistedPromotionSummary,
    ) {
        let env = EnvironmentPaths::new(root, "api", "staging");
        LifecycleStore::new(env, generation)
            .write(&PersistedDeploymentLifecycle {
                lifecycle_version: 1,
                project_id: "api".into(),
                environment: "staging".into(),
                generation,
                state: state.clone(),
                entered_at_unix: generation,
                transition_reason: format!("gen-{generation}-{state:?}").to_lowercase(),
                validation_summary: Some(validation_summary.clone()),
                promotion_summary: Some(promotion_summary.clone()),
                transitions: vec![crate::storage::DeploymentLifecycleTransition {
                    state,
                    entered_at_unix: generation,
                    transition_reason: format!("gen-{generation}"),
                    validation_summary: Some(validation_summary),
                    promotion_summary: Some(promotion_summary),
                }],
            })
            .unwrap();
    }

    fn write_failed_first_generation(root: &Path, generation: u64) {
        let env = EnvironmentPaths::new(root, "api", "staging");
        let writer = SnapshotWriter::new(env.clone(), generation).unwrap();
        writer
            .write_artifact(
                "build.json",
                &format!(
                    "{{\n  \"deployment_id\": \"dep-{generation}\",\n  \"image_ref\": \"forge/api:staging-gen-{generation}\"\n}}\n"
                ),
            )
            .unwrap();
        writer
            .finalize("api", "staging", SnapshotState::Failed)
            .unwrap();
        write_lifecycle_state(root, generation, DeploymentLifecycleState::Failed);
        DiagnosticsStore::new(env, generation)
            .write_summary(&crate::storage::DiagnosticSummary {
                deployment_id: Some(format!("dep-{generation}")),
                failure_stage: "topology".into(),
                failure_reason: "service dependency graph contains a cycle".into(),
                blocking_reason: Some("service dependency graph contains a cycle".into()),
                container_name: "staging-api-api-gen-1".into(),
                failed_service_name: Some("api".into()),
                blocking_service_name: Some("api".into()),
                probe_target_host: None,
                probe_target_port: None,
                probe_target_path: None,
                restart_storm: false,
                restart_policy: None,
                restart_count_delta: None,
                oom_killed: None,
                last_exit_code: None,
                exit_signal: None,
                termination_reason: None,
                cleanup_recorded: false,
                dependency_graph_summary: Some("api<-worker; worker<-api".into()),
                runtime_env_preview: Vec::new(),
            })
            .unwrap();
    }

    fn write_generation_with_runtime(
        root: &Path,
        generation: u64,
        api_base_url: &str,
        secret_key: &str,
        secret_value: &str,
    ) {
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
            runtime_policy: PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            environment_variables: BTreeMap::new(),
            volume_mounts: Vec::new(),
            source_ref: Some("main".into()),
            repo_url: None,
            commit_sha: Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac".into()),
            source_path: None,
            services: BTreeMap::new(),
            startup_order: Vec::new(),
        })
        .unwrap();
        writer
            .write_artifact("runtime.json", &format!("{runtime}\n"))
            .unwrap();
        let snapshot = serde_json::json!({
            "snapshot_version": 1,
            "project_id": "api",
            "environment": "staging",
            "generation": generation,
            "deployment_id": format!("dep-{generation}"),
            "source_environment": "staging",
            "source_ref": "main",
            "commit_sha": "340ac8108006d84dbf951d8c0bb04ecfaf0eccac",
            "domain": "staging-api.example.com",
            "entries": {
                "API_BASE_URL": {
                    "source": "forge_yaml",
                    "value": api_base_url,
                    "sensitive": false,
                    "redacted": false
                },
                "DATABASE_URL": {
                    "source": "project_environment_secret",
                    "secret_reference": {
                        "scope": "environment",
                        "key": secret_key,
                        "secret_id": format!("api:staging:{secret_key}"),
                        "sensitive": true
                    },
                    "sensitive": true,
                    "redacted": true
                }
            }
        });
        writer
            .write_artifact(
                "runtime_env_snapshot.json",
                &format!("{}\n", serde_json::to_string_pretty(&snapshot).unwrap()),
            )
            .unwrap();
        let resolved = serde_json::json!({
            "snapshot_version": 1,
            "project_id": "api",
            "environment": "staging",
            "generation": generation,
            "deployment_id": format!("dep-{generation}"),
            "source_environment": "staging",
            "source_ref": "main",
            "commit_sha": "340ac8108006d84dbf951d8c0bb04ecfaf0eccac",
            "domain": "staging-api.example.com",
            "entries": {
                "API_BASE_URL": {
                    "source": "forge_yaml",
                    "value": api_base_url,
                    "sensitive": false
                },
                "DATABASE_URL": {
                    "source": "project_environment_secret",
                    "secret_reference": {
                        "scope": "environment",
                        "key": secret_key,
                        "secret_id": format!("api:staging:{secret_key}"),
                        "sensitive": true
                    },
                    "sealed_value": crate::secrets::seal_value(secret_value).unwrap(),
                    "sensitive": true
                }
            }
        });
        writer
            .write_artifact(
                "resolved_runtime.json",
                &format!("{}\n", serde_json::to_string_pretty(&resolved).unwrap()),
            )
            .unwrap();
        writer
            .finalize("api", "staging", SnapshotState::Healthy)
            .unwrap();
    }

    fn healthy_container(generation: u64) -> ContainerInspection {
        ContainerInspection {
            container_name: format!("staging-api-gen-{generation}"),
            running: true,
            state_status: "running".into(),
            exit_code: Some(0),
            restart_count: 0,
            started_at: Some("2026-05-21T12:00:00Z".into()),
            finished_at: None,
            oom_killed: false,
            error: None,
            image_ref: format!("forge/api:staging-gen-{generation}"),
            labels: BTreeMap::new(),
            network_ips: BTreeMap::from([("forge-managed".into(), "172.29.0.2".into())]),
            volume_mounts: Vec::new(),
            restart_policy: "no".into(),
            restart_max_retries: None,
            cpu_limit: None,
            memory_limit_mb: None,
            exit_signal: None,
            termination_reason: None,
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
    fn stable_promoted_generation_not_flapping() {
        let root = test_root("stable-promoted-generation-not-flapping");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        write_validation_lifecycle(
            &root,
            7,
            DeploymentLifecycleState::Promoted,
            PersistedValidationSummary {
                tcp_consecutive_passes: 5,
                http_consecutive_passes: 5,
                required_consecutive_passes: 3,
                minimum_uptime_seconds: 10,
                observed_uptime_seconds: 60,
                restart_count_initial: 0,
                restart_count_current: 0,
                restart_count_stable: true,
                route_verification_stable: true,
                validation_succeeded: true,
                last_probe_error: None,
                unstable_probe_failures: 0,
                restart_storm_detected: false,
                oom_detected: false,
            },
            PersistedPromotionSummary {
                warmup_succeeded: true,
                validation_succeeded: true,
                route_verification_succeeded: true,
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                promoted_at_unix: Some(7),
                gate_reason: None,
            },
        );
        write_probe_history(
            &root,
            7,
            vec![
                (1, PersistedProbeType::Tcp, true, 12, None),
                (2, PersistedProbeType::Http, true, 14, None),
                (3, PersistedProbeType::Tcp, true, 11, None),
                (4, PersistedProbeType::Http, true, 13, None),
                (5, PersistedProbeType::Tcp, true, 10, None),
                (6, PersistedProbeType::Http, true, 12, None),
            ],
        );

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert!(!diagnostics.probe_flapping);
        let stability = diagnostics.probe_stability.unwrap();
        assert_eq!(stability.recent_failure_count, 0);
        assert_eq!(stability.consecutive_success_streak, 6);
    }

    #[test]
    fn alternating_probe_failures_detect_flapping() {
        let root = test_root("alternating-probe-failures-detect-flapping");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        write_validation_lifecycle(
            &root,
            7,
            DeploymentLifecycleState::Promoted,
            PersistedValidationSummary {
                tcp_consecutive_passes: 1,
                http_consecutive_passes: 1,
                required_consecutive_passes: 3,
                minimum_uptime_seconds: 10,
                observed_uptime_seconds: 60,
                restart_count_initial: 0,
                restart_count_current: 0,
                restart_count_stable: true,
                route_verification_stable: true,
                validation_succeeded: true,
                last_probe_error: Some("http health probe returned unhealthy for /health".into()),
                unstable_probe_failures: 0,
                restart_storm_detected: false,
                oom_detected: false,
            },
            PersistedPromotionSummary {
                warmup_succeeded: true,
                validation_succeeded: true,
                route_verification_succeeded: true,
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                promoted_at_unix: Some(7),
                gate_reason: None,
            },
        );
        write_probe_history(
            &root,
            7,
            vec![
                (1, PersistedProbeType::Tcp, true, 12, None),
                (
                    2,
                    PersistedProbeType::Tcp,
                    false,
                    12,
                    Some("tcp probe returned unhealthy"),
                ),
                (3, PersistedProbeType::Tcp, true, 11, None),
                (
                    4,
                    PersistedProbeType::Tcp,
                    false,
                    11,
                    Some("tcp probe returned unhealthy"),
                ),
                (5, PersistedProbeType::Tcp, true, 10, None),
                (
                    6,
                    PersistedProbeType::Tcp,
                    false,
                    10,
                    Some("tcp probe returned unhealthy"),
                ),
            ],
        );

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert!(diagnostics.probe_flapping);
        assert!(
            diagnostics
                .probe_stability
                .unwrap()
                .flapping_window_summary
                .contains("alternations=")
        );
    }

    #[test]
    fn transient_single_failure_does_not_trigger_flapping() {
        let root = test_root("transient-single-failure-does-not-trigger-flapping");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        write_validation_lifecycle(
            &root,
            7,
            DeploymentLifecycleState::Promoted,
            PersistedValidationSummary {
                tcp_consecutive_passes: 4,
                http_consecutive_passes: 4,
                required_consecutive_passes: 3,
                minimum_uptime_seconds: 10,
                observed_uptime_seconds: 60,
                restart_count_initial: 0,
                restart_count_current: 0,
                restart_count_stable: true,
                route_verification_stable: true,
                validation_succeeded: true,
                last_probe_error: None,
                unstable_probe_failures: 0,
                restart_storm_detected: false,
                oom_detected: false,
            },
            PersistedPromotionSummary {
                warmup_succeeded: true,
                validation_succeeded: true,
                route_verification_succeeded: true,
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                promoted_at_unix: Some(7),
                gate_reason: None,
            },
        );
        write_probe_history(
            &root,
            7,
            vec![
                (1, PersistedProbeType::Tcp, true, 12, None),
                (2, PersistedProbeType::Tcp, true, 12, None),
                (
                    3,
                    PersistedProbeType::Tcp,
                    false,
                    12,
                    Some("tcp probe returned unhealthy"),
                ),
                (4, PersistedProbeType::Tcp, true, 11, None),
                (5, PersistedProbeType::Tcp, true, 10, None),
                (6, PersistedProbeType::Tcp, true, 10, None),
            ],
        );

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert!(!diagnostics.probe_flapping);
        assert_eq!(diagnostics.probe_stability.unwrap().recent_failure_count, 1);
    }

    #[test]
    fn flapping_clears_after_stable_success_window() {
        let root = test_root("flapping-clears-after-stable-success-window");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        write_validation_lifecycle(
            &root,
            7,
            DeploymentLifecycleState::Promoted,
            PersistedValidationSummary {
                tcp_consecutive_passes: 4,
                http_consecutive_passes: 4,
                required_consecutive_passes: 3,
                minimum_uptime_seconds: 10,
                observed_uptime_seconds: 60,
                restart_count_initial: 0,
                restart_count_current: 0,
                restart_count_stable: true,
                route_verification_stable: true,
                validation_succeeded: true,
                last_probe_error: None,
                unstable_probe_failures: 0,
                restart_storm_detected: false,
                oom_detected: false,
            },
            PersistedPromotionSummary {
                warmup_succeeded: true,
                validation_succeeded: true,
                route_verification_succeeded: true,
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                promoted_at_unix: Some(7),
                gate_reason: None,
            },
        );
        write_probe_history(
            &root,
            7,
            vec![
                (1, PersistedProbeType::Tcp, true, 12, None),
                (
                    2,
                    PersistedProbeType::Tcp,
                    false,
                    12,
                    Some("tcp probe returned unhealthy"),
                ),
                (3, PersistedProbeType::Tcp, true, 12, None),
                (
                    4,
                    PersistedProbeType::Tcp,
                    false,
                    12,
                    Some("tcp probe returned unhealthy"),
                ),
                (5, PersistedProbeType::Tcp, true, 11, None),
                (6, PersistedProbeType::Tcp, true, 11, None),
                (7, PersistedProbeType::Tcp, true, 10, None),
                (8, PersistedProbeType::Tcp, true, 10, None),
            ],
        );

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert!(!diagnostics.probe_flapping);
        assert_eq!(
            diagnostics
                .probe_stability
                .unwrap()
                .consecutive_success_streak,
            4
        );
    }

    #[test]
    fn diagnose_reports_probe_statistics() {
        let root = test_root("diagnose-reports-probe-statistics");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        write_validation_lifecycle(
            &root,
            7,
            DeploymentLifecycleState::Promoted,
            PersistedValidationSummary {
                tcp_consecutive_passes: 2,
                http_consecutive_passes: 2,
                required_consecutive_passes: 3,
                minimum_uptime_seconds: 10,
                observed_uptime_seconds: 60,
                restart_count_initial: 0,
                restart_count_current: 0,
                restart_count_stable: true,
                route_verification_stable: true,
                validation_succeeded: true,
                last_probe_error: Some("http health probe returned unhealthy for /health".into()),
                unstable_probe_failures: 0,
                restart_storm_detected: false,
                oom_detected: false,
            },
            PersistedPromotionSummary {
                warmup_succeeded: true,
                validation_succeeded: true,
                route_verification_succeeded: true,
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                promoted_at_unix: Some(7),
                gate_reason: None,
            },
        );
        write_probe_history(
            &root,
            7,
            vec![
                (1, PersistedProbeType::Tcp, true, 9, None),
                (2, PersistedProbeType::Http, true, 15, None),
                (
                    3,
                    PersistedProbeType::Tcp,
                    false,
                    9,
                    Some("tcp probe returned unhealthy"),
                ),
                (
                    4,
                    PersistedProbeType::Http,
                    false,
                    16,
                    Some("http health probe returned unhealthy for /health"),
                ),
                (5, PersistedProbeType::Tcp, true, 8, None),
                (6, PersistedProbeType::Http, true, 14, None),
            ],
        );

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        let stability = diagnostics.probe_stability.unwrap();
        assert_eq!(stability.sample_size, 6);
        assert_eq!(stability.consecutive_success_streak, 2);
        assert_eq!(stability.recent_failure_count, 2);
        assert!(stability.success_rate > 0.6 && stability.success_rate < 0.7);
        assert!(stability.flapping_window_summary.contains("tcp="));
        assert!(stability.flapping_window_summary.contains("http="));
    }

    #[test]
    fn diagnose_reports_recent_failure_summary() {
        let root = test_root("diagnose-reports-recent-failure-summary");
        register_project(&root, "api", "api.example.com");
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
                blocking_reason: Some("http health probe failed".into()),
                container_name: "staging-api-gen-8".into(),
                failed_service_name: None,
                blocking_service_name: None,
                probe_target_host: Some("172.29.0.3".into()),
                probe_target_port: Some(3000),
                probe_target_path: Some("/health".into()),
                restart_storm: false,
                restart_policy: None,
                restart_count_delta: None,
                oom_killed: None,
                last_exit_code: None,
                exit_signal: None,
                termination_reason: None,
                cleanup_recorded: true,
                dependency_graph_summary: None,
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

        let mut docker = StubDockerRuntime::default();
        let mut routing = StubRoutingRuntime::default();

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
    fn status_failed_when_first_deploy_fails_before_promotion() {
        let root = test_root("status-failed-when-first-deploy-fails-before-promotion");
        register_project(&root, "api", "api.example.com");
        write_failed_first_generation(&root, 1);

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
        assert_eq!(status.last_deployment_id.as_deref(), Some("dep-1"));
        assert_eq!(
            status.lifecycle_state,
            Some(DeploymentLifecycleState::Failed)
        );
    }

    #[test]
    fn diagnose_reports_failed_first_deploy_without_promoted_generation() {
        let root = test_root("diagnose-reports-failed-first-deploy-without-promoted-generation");
        register_project(&root, "api", "api.example.com");
        write_failed_first_generation(&root, 1);

        let mut docker = StubDockerRuntime::default();
        let mut routing = StubRoutingRuntime::default();
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert_eq!(diagnostics.status, "failed");
        assert_eq!(diagnostics.recent_failures.len(), 1);
        assert_eq!(diagnostics.recent_failures[0].generation, 1);
        assert_eq!(
            diagnostics.recent_failures[0].deployment_id.as_deref(),
            Some("dep-1")
        );
    }

    #[test]
    fn history_shows_failed_first_generation() {
        let root = test_root("history-shows-failed-first-generation");
        register_project(&root, "api", "api.example.com");
        write_failed_first_generation(&root, 1);

        let mut docker = StubDockerRuntime::default();
        let mut routing = StubRoutingRuntime::default();
        let history =
            load_environment_history(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert_eq!(history.entries.len(), 1);
        assert_eq!(history.entries[0].generation, 1);
        assert_eq!(
            history.entries[0].lifecycle_state,
            Some(DeploymentLifecycleState::Failed)
        );
        assert_eq!(history.entries[0].deployment_id.as_deref(), Some("dep-1"));
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

    #[test]
    fn env_reports_helpful_message_for_legacy_generation_without_snapshot() {
        let root = test_root("env-reports-helpful-message-for-legacy-generation-without-snapshot");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        fs::remove_file(env.generation_dir(7).join("runtime_env_snapshot.json")).unwrap();

        let err = load_project_environment_env_report(&root, "api", "staging").unwrap_err();
        let (_, response) = project_status_error_response(err);
        assert_eq!(response.code, "runtime_env_snapshot_unavailable");
        assert_eq!(
            response.message,
            "runtime env snapshot unavailable for this promoted generation; legacy metadata unavailable, redeploy required"
        );
    }

    #[test]
    fn status_reports_legacy_generation_missing_env_snapshot_without_false_unknowns() {
        let root = test_root("status-reports-legacy-generation-missing-env-snapshot");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        fs::remove_file(env.generation_dir(7).join("runtime_env_snapshot.json")).unwrap();

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

        assert_eq!(status.active_generation, Some(7));
        assert_eq!(status.status, "degraded");
        assert_eq!(status.container_name.as_deref(), Some("staging-api-gen-7"));
        assert_eq!(status.container_ip.as_deref(), Some("172.29.0.2"));
        assert!(status.route_active);
        assert!(status.runtime_env_snapshot.is_none());

        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();
        assert!(!diagnostics.route.matches_expected);
        assert_eq!(
            diagnostics.route.mismatch_reason.as_deref(),
            Some(
                "generation 7 is a legacy promoted generation; runtime env snapshot metadata unavailable"
            )
        );
    }

    #[test]
    fn status_reads_newest_promoted_generation() {
        let root = test_root("status-reads-newest-promoted-generation");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 6);
        write_generation(&root, 7);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        PointerStore::new(env.clone()).swap_current(6).unwrap();
        atomic_write(env.promoted_pointer(), b"7\n").unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(6),
                health_state: RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 1,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "healthy".into(),
                last_error_code: None,
            })
            .unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(RouteInspection {
                subtree_id: "forge:api:staging".into(),
                active_target: "172.29.0.2:3000".into(),
                domain: Some("staging-api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: Some(200),
                verification_response_body: None,
                health_checks_enabled: false,
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
        assert_eq!(status.active_generation, Some(7));
        assert_eq!(status.last_deployment_id.as_deref(), Some("dep-7"));
    }

    #[test]
    fn status_reports_progressive_state() {
        let root = test_root("status-reports-progressive-state");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 31);
        let env = EnvironmentPaths::new(&root, "api", "staging");
        PointerStore::new(env.clone()).swap_current(31).unwrap();
        let lifecycle_store = crate::storage::LifecycleStore::new(env.clone(), 31);
        let mut lifecycle = PersistedDeploymentLifecycle {
            lifecycle_version: 1,
            project_id: "api".into(),
            environment: "staging".into(),
            generation: 31,
            state: DeploymentLifecycleState::Queued,
            entered_at_unix: crate::storage::current_unix_timestamp(),
            transition_reason: String::new(),
            validation_summary: None,
            promotion_summary: None,
            transitions: Vec::new(),
        };
        lifecycle.transition(
            DeploymentLifecycleState::Warming,
            crate::storage::current_unix_timestamp(),
            "awaiting final warmup probe",
            Some(PersistedValidationSummary {
                tcp_consecutive_passes: 2,
                http_consecutive_passes: 2,
                required_consecutive_passes: 3,
                minimum_uptime_seconds: 10,
                observed_uptime_seconds: 8,
                restart_count_initial: 0,
                restart_count_current: 0,
                restart_count_stable: true,
                route_verification_stable: true,
                validation_succeeded: false,
                last_probe_error: None,
                unstable_probe_failures: 0,
                restart_storm_detected: false,
                oom_detected: false,
            }),
            Some(PersistedPromotionSummary {
                gate_reason: Some("warmup pending".into()),
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                ..PersistedPromotionSummary::default()
            }),
        );
        lifecycle_store.write(&lifecycle).unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(31)),
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
            status.lifecycle_state,
            Some(DeploymentLifecycleState::Warming)
        );
        assert_eq!(status.retention_role, Some(RetentionRole::Current));
        let summary = status.validation_summary.unwrap();
        assert_eq!(summary.tcp_consecutive_passes, 2);
        assert_eq!(summary.required_consecutive_passes, 3);
        assert_eq!(status.uptime_seconds, Some(8));
    }

    #[test]
    fn env_reads_snapshot_for_promoted_generation() {
        let root = test_root("env-reads-snapshot-for-promoted-generation");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 6);
        write_generation(&root, 7);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        PointerStore::new(env.clone()).swap_current(6).unwrap();
        atomic_write(env.promoted_pointer(), b"7\n").unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(6),
                health_state: RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 1,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "healthy".into(),
                last_error_code: None,
            })
            .unwrap();

        let report = load_project_environment_env_report(&root, "api", "staging").unwrap();
        assert_eq!(report.generation, 7);
        assert_eq!(report.deployment_id, "dep-7");
    }

    #[test]
    fn status_healthy_when_container_and_route_active() {
        let root = test_root("status-healthy-when-container-and-route-active");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 6);
        write_generation(&root, 7);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        PointerStore::new(env.clone()).swap_current(6).unwrap();
        atomic_write(env.promoted_pointer(), b"7\n").unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(7),
                health_state: RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 1,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "healthy".into(),
                last_error_code: None,
            })
            .unwrap();

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

        assert_eq!(status.active_generation, Some(7));
        assert!(status.container_running);
        assert!(status.route_active);
        assert_eq!(status.status, "healthy");
    }

    #[test]
    fn diagnose_uses_same_runtime_truth_as_status() {
        let root = test_root("diagnose-uses-same-runtime-truth-as-status");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 6);
        write_generation(&root, 7);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        PointerStore::new(env.clone()).swap_current(6).unwrap();
        atomic_write(env.promoted_pointer(), b"7\n").unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(7),
                health_state: RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 1,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "healthy".into(),
                last_error_code: None,
            })
            .unwrap();

        let mut status_docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut status_routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let status = load_project_environment_status(
            &root,
            None,
            &mut status_docker,
            &mut status_routing,
            "api",
            "staging",
        )
        .unwrap();

        struct SingleInspectionRoutingRuntime {
            inspection: Option<RouteInspection>,
        }

        impl RoutingRuntime for SingleInspectionRoutingRuntime {
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
                    .take()
                    .ok_or_else(|| RoutingRuntimeError::InspectionFailed("missing route".into()))
            }

            fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
                Ok(self.inspection.clone().into_iter().collect())
            }

            fn remove_route(&mut self, _subtree_id: &str) -> Result<(), RoutingRuntimeError> {
                Ok(())
            }
        }

        let mut diagnose_docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut diagnose_routing = SingleInspectionRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics = load_environment_diagnostics(
            &root,
            None,
            &mut diagnose_docker,
            &mut diagnose_routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.active_generation, diagnostics.active_generation);
        assert_eq!(status.container_name, diagnostics.container.container_name);
        assert_eq!(status.container_ip, diagnostics.container.container_ip);
        assert_eq!(
            diagnostics.route.current_target.as_deref(),
            Some("172.29.0.2:3000")
        );
        assert_eq!(
            diagnostics.route.expected_target.as_deref(),
            Some("172.29.0.2:3000")
        );
        assert!(diagnostics.route.route_active);
        assert!(diagnostics.route.matches_expected);
        assert_eq!(diagnostics.status, "healthy");
    }

    #[test]
    fn diagnose_and_status_share_route_truth() {
        let root = test_root("diagnose-and-status-share-route-truth");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut status_docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut status_routing = StubRoutingRuntime {
            inspection: Some(RouteInspection {
                active_target: "172.29.0.99:3000".into(),
                ..healthy_route()
            }),
        };
        let status = load_project_environment_status(
            &root,
            None,
            &mut status_docker,
            &mut status_routing,
            "api",
            "staging",
        )
        .unwrap();

        struct SingleInspectionRoutingRuntime {
            inspection: Option<RouteInspection>,
        }

        impl RoutingRuntime for SingleInspectionRoutingRuntime {
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
                    .take()
                    .ok_or_else(|| RoutingRuntimeError::InspectionFailed("missing route".into()))
            }

            fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
                Ok(self.inspection.clone().into_iter().collect())
            }

            fn remove_route(&mut self, _subtree_id: &str) -> Result<(), RoutingRuntimeError> {
                Ok(())
            }
        }

        let mut diagnose_docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut diagnose_routing = SingleInspectionRoutingRuntime {
            inspection: Some(RouteInspection {
                active_target: "172.29.0.99:3000".into(),
                ..healthy_route()
            }),
        };
        let diagnostics = load_environment_diagnostics(
            &root,
            None,
            &mut diagnose_docker,
            &mut diagnose_routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.active_generation, diagnostics.active_generation);
        assert_eq!(status.container_ip, diagnostics.container.container_ip);
        assert_eq!(
            diagnostics.route.current_target.as_deref(),
            Some("172.29.0.99:3000")
        );
        assert_eq!(
            diagnostics.route.expected_target.as_deref(),
            Some("172.29.0.2:3000")
        );
        assert_eq!(
            diagnostics.route.mismatch_reason.as_deref(),
            Some("route target mismatch: current=172.29.0.99:3000 expected=172.29.0.2:3000")
        );
        assert_eq!(status.status, "degraded");
        assert_eq!(diagnostics.status, "degraded");
    }

    #[test]
    fn diagnose_healthy_status_does_not_report_old_failure_stage() {
        let root = test_root("diagnose-healthy-status-does-not-report-old-failure-stage");
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
        DiagnosticsStore::new(env, 8)
            .write_summary(&crate::storage::DiagnosticSummary {
                deployment_id: Some("dep-8".into()),
                failure_stage: "startup_recovery".into(),
                failure_reason: "retention cleanup removed diagnostics".into(),
                blocking_reason: Some("retention cleanup removed diagnostics".into()),
                container_name: "staging-api-gen-8".into(),
                failed_service_name: None,
                blocking_service_name: None,
                probe_target_host: None,
                probe_target_port: None,
                probe_target_path: None,
                restart_storm: false,
                restart_policy: None,
                restart_count_delta: None,
                oom_killed: None,
                last_exit_code: None,
                exit_signal: None,
                termination_reason: None,
                cleanup_recorded: true,
                dependency_graph_summary: None,
                runtime_env_preview: Vec::new(),
            })
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

        assert_eq!(diagnostics.status, "healthy");
        assert!(diagnostics.likely_failure_stage.is_none());
        assert!(diagnostics.diagnostics_source.is_none());
        assert!(diagnostics.latest_validation_failure.is_none());
    }

    #[test]
    fn env_diff_reports_added_removed_changed_keys() {
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        let root = test_root("env-diff-added-removed-changed");
        register_project(&root, "api", "api.example.com");
        write_generation_with_runtime(
            &root,
            1,
            "https://api-v1.example.com",
            "DATABASE_URL",
            "postgres://db-v1",
        );
        write_generation_with_runtime(
            &root,
            2,
            "https://api-v2.example.com",
            "DATABASE_URL",
            "postgres://db-v2",
        );
        let env = EnvironmentPaths::new(&root, "api", "staging");
        let snapshot = load_generation_runtime_env_snapshot(&env, 2)
            .unwrap()
            .unwrap();
        let mut snapshot_value = snapshot;
        snapshot_value.entries.remove("API_BASE_URL");
        snapshot_value.entries.insert(
            "FEATURE_FLAG".into(),
            crate::storage::PersistedRuntimeEnvEntry {
                source: crate::storage::PersistedRuntimeEnvSource::ForgeYaml,
                value: Some("true".into()),
                secret_reference: None,
                sensitive: false,
                redacted: false,
            },
        );
        atomic_write(
            env.generation_dir(2).join("runtime_env_snapshot.json"),
            format!(
                "{}\n",
                serde_json::to_string_pretty(&snapshot_value).unwrap()
            )
            .as_bytes(),
        )
        .unwrap();
        let resolved = load_generation_resolved_runtime(&env, 2).unwrap().unwrap();
        let mut resolved_value = resolved;
        resolved_value.entries.remove("API_BASE_URL");
        resolved_value.entries.insert(
            "FEATURE_FLAG".into(),
            crate::storage::PersistedResolvedRuntimeEntry {
                source: crate::storage::PersistedRuntimeEnvSource::ForgeYaml,
                value: Some("true".into()),
                secret_reference: None,
                sealed_value: None,
                sensitive: false,
            },
        );
        atomic_write(
            env.generation_dir(2).join("resolved_runtime.json"),
            format!(
                "{}\n",
                serde_json::to_string_pretty(&resolved_value).unwrap()
            )
            .as_bytes(),
        )
        .unwrap();

        let diff = load_environment_diff(&root, "api", "staging", 1, 2).unwrap();

        assert!(diff.added.iter().any(|entry| entry.key == "FEATURE_FLAG"));
        assert!(diff.removed.iter().any(|entry| entry.key == "API_BASE_URL"));
        assert!(
            diff.changed_values
                .iter()
                .any(|entry| entry.key == "DATABASE_URL")
        );
    }

    #[test]
    fn env_diff_redacts_secret_values() {
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        let root = test_root("env-diff-redacts-secret-values");
        register_project(&root, "api", "api.example.com");
        write_generation_with_runtime(
            &root,
            1,
            "https://api.example.com",
            "DATABASE_URL",
            "postgres://db-v1",
        );
        write_generation_with_runtime(
            &root,
            2,
            "https://api.example.com",
            "DATABASE_URL",
            "postgres://db-v2",
        );

        let diff = load_environment_diff(&root, "api", "staging", 1, 2).unwrap();
        let changed = diff
            .changed_values
            .iter()
            .find(|entry| entry.key == "DATABASE_URL")
            .unwrap();
        let rendered = serde_json::to_string(&diff).unwrap();

        assert_eq!(changed.before, "<secret changed>");
        assert_eq!(changed.after, "<secret changed>");
        assert!(!rendered.contains("postgres://db-v1"));
        assert!(!rendered.contains("postgres://db-v2"));
    }

    #[test]
    fn diagnose_reports_future_secret_drift() {
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        let root = test_root("diagnose-reports-future-secret-drift");
        register_project(&root, "api", "api.example.com");
        write_generation_with_runtime(
            &root,
            1,
            "https://api.example.com",
            "DATABASE_URL",
            "postgres://db-v1",
        );
        let env = EnvironmentPaths::new(&root, "api", "staging");
        atomic_write(
            env.generation_dir(1).join("snapshot.json"),
            concat!(
                "{\n",
                "  \"snapshot_version\": 1,\n",
                "  \"project_id\": \"api\",\n",
                "  \"environment\": \"staging\",\n",
                "  \"generation\": 1,\n",
                "  \"state\": \"healthy\",\n",
                "  \"finalized_at_unix\": 1\n",
                "}\n"
            )
            .as_bytes(),
        )
        .unwrap();
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let store = SecretStore::new(root.join("secrets")).unwrap();
        store
            .write_environment_secret(&crate::secrets::SecretWriteRequest {
                project_id: "api".into(),
                environment: "staging".into(),
                key: "DATABASE_URL".into(),
                value: "postgres://db-v2".into(),
            })
            .unwrap();

        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(
            repo.join("forge.project.json"),
            r#"{
  "project_id": "api",
  "secrets": {
    "environment_variables": {
      "DATABASE_URL": { "scope": "environment", "key": "DATABASE_URL", "sensitive": true }
    }
  }
}"#,
        )
        .unwrap();
        ProjectRegistryStore::new(&root)
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some("api".into()),
                    repo_url: repo.to_string_lossy().into_owned(),
                    default_branch: "main".into(),
                    base_domain: Some("api.example.com".into()),
                },
                None,
            )
            .unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(1)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert!(
            diagnostics
                .recent_secret_mutations
                .iter()
                .any(|mutation| mutation.key == "DATABASE_URL" && mutation.mutation == "rotated")
        );
    }

    #[test]
    fn diagnose_labels_old_cleanup_events_as_historical() {
        let root = test_root("diagnose-labels-old-cleanup-events-as-historical");
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
        DiagnosticsStore::new(env, 8)
            .write_summary(&crate::storage::DiagnosticSummary {
                deployment_id: Some("dep-8".into()),
                failure_stage: "startup_recovery".into(),
                failure_reason: "retention cleanup removed diagnostics".into(),
                blocking_reason: Some("retention cleanup removed diagnostics".into()),
                container_name: "staging-api-gen-8".into(),
                failed_service_name: None,
                blocking_service_name: None,
                probe_target_host: None,
                probe_target_port: None,
                probe_target_path: None,
                restart_storm: false,
                restart_policy: None,
                restart_count_delta: None,
                oom_killed: None,
                last_exit_code: None,
                exit_signal: None,
                termination_reason: None,
                cleanup_recorded: true,
                dependency_graph_summary: None,
                runtime_env_preview: Vec::new(),
            })
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
        assert!(diagnostics.recent_failures[0].historical);
        assert_eq!(
            diagnostics.recent_failures[0].failure_stage,
            "startup_recovery"
        );
    }

    #[test]
    fn history_reports_retention_state() {
        let root = test_root("history-reports-retention-state");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 4);
        write_generation(&root, 6);
        write_generation(&root, 7);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        let failed = SnapshotWriter::new(env.clone(), 5).unwrap();
        failed
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-5\",\n  \"image_ref\": \"forge/api:staging-gen-5\",\n  \"source_ref\": \"main\",\n  \"commit_sha\": \"deadbeef\"\n}\n",
            )
            .unwrap();
        failed
            .finalize("api", "staging", SnapshotState::Failed)
            .unwrap();
        DiagnosticsStore::new(env.clone(), 5)
            .write_summary(&crate::storage::DiagnosticSummary {
                deployment_id: Some("dep-5".into()),
                failure_stage: "validation".into(),
                failure_reason: "http health probe failed".into(),
                blocking_reason: Some("http health probe failed".into()),
                container_name: "staging-api-gen-5".into(),
                failed_service_name: None,
                blocking_service_name: None,
                probe_target_host: None,
                probe_target_port: None,
                probe_target_path: None,
                restart_storm: false,
                restart_policy: None,
                restart_count_delta: None,
                oom_killed: None,
                last_exit_code: None,
                exit_signal: None,
                termination_reason: None,
                cleanup_recorded: false,
                dependency_graph_summary: None,
                runtime_env_preview: Vec::new(),
            })
            .unwrap();
        PointerStore::new(env.clone()).swap_current(7).unwrap();
        atomic_write(env.previous_pointer(), b"6\n").unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(7),
                health_state: RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 1,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "healthy".into(),
                last_error_code: None,
            })
            .unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let history =
            load_environment_history(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        let current = history
            .entries
            .iter()
            .find(|entry| entry.generation == 7)
            .unwrap();
        let previous = history
            .entries
            .iter()
            .find(|entry| entry.generation == 6)
            .unwrap();
        let failed = history
            .entries
            .iter()
            .find(|entry| entry.generation == 5)
            .unwrap();
        let eligible = history
            .entries
            .iter()
            .find(|entry| entry.generation == 4)
            .unwrap();

        assert!(current.retained);
        assert!(previous.retained);
        assert!(previous.rollback_target);
        assert!(failed.retained);
        assert!(!eligible.retained);
        assert!(eligible.eligible_for_gc);
    }

    #[test]
    fn diagnose_active_generation_not_reported_as_rollback_target() {
        let root = test_root("diagnose-active-generation-not-reported-as-rollback-target");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 29);
        write_generation(&root, 30);
        write_lifecycle_state(&root, 29, DeploymentLifecycleState::Promoted);
        write_lifecycle_state(&root, 30, DeploymentLifecycleState::Promoted);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        atomic_write(env.previous_pointer(), b"29\n").unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(30)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert_eq!(diagnostics.active_generation, Some(30));
        assert_eq!(
            diagnostics.active_lifecycle_state,
            Some(DeploymentLifecycleState::Promoted)
        );
        assert_eq!(diagnostics.retention_role, Some(RetentionRole::Current));
        assert_eq!(
            status_label(
                diagnostics.active_lifecycle_state.as_ref(),
                diagnostics.retention_role.as_ref()
            ),
            "active"
        );
    }

    #[test]
    fn history_distinguishes_current_promoted_from_historical_promoted() {
        let root = test_root("history-distinguishes-current-promoted-from-historical-promoted");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 28);
        write_generation(&root, 29);
        write_generation(&root, 30);
        write_lifecycle_state(&root, 28, DeploymentLifecycleState::Promoted);
        write_lifecycle_state(&root, 29, DeploymentLifecycleState::Promoted);
        write_lifecycle_state(&root, 30, DeploymentLifecycleState::Promoted);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        atomic_write(env.previous_pointer(), b"28\n").unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(30)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let history =
            load_environment_history(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        let current = history
            .entries
            .iter()
            .find(|entry| entry.generation == 30)
            .unwrap();
        let historical = history
            .entries
            .iter()
            .find(|entry| entry.generation == 29)
            .unwrap();

        assert_eq!(current.retention_role, Some(RetentionRole::Current));
        assert_eq!(
            status_label(
                current.lifecycle_state.as_ref(),
                current.retention_role.as_ref()
            ),
            "active"
        );
        assert_eq!(historical.retention_role, Some(RetentionRole::Retained));
        assert_eq!(
            status_label(
                historical.lifecycle_state.as_ref(),
                historical.retention_role.as_ref()
            ),
            "historical_promoted"
        );
    }

    #[test]
    fn rollback_target_only_applies_to_previous_generation() {
        let root = test_root("rollback-target-only-applies-to-previous-generation");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 28);
        write_generation(&root, 29);
        write_generation(&root, 30);
        write_lifecycle_state(&root, 28, DeploymentLifecycleState::Promoted);
        write_lifecycle_state(&root, 29, DeploymentLifecycleState::Promoted);
        write_lifecycle_state(&root, 30, DeploymentLifecycleState::Promoted);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        atomic_write(env.previous_pointer(), b"29\n").unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(30)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let history =
            load_environment_history(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        let rollback_targets = history
            .entries
            .iter()
            .filter(|entry| entry.retention_role == Some(RetentionRole::RollbackTarget))
            .map(|entry| entry.generation)
            .collect::<Vec<_>>();
        assert_eq!(rollback_targets, vec![29]);
        assert!(
            history
                .entries
                .iter()
                .filter(|entry| entry.generation != 29)
                .all(|entry| !entry.rollback_target)
        );
    }

    #[test]
    fn lifecycle_state_and_retention_role_are_separate() {
        let root = test_root("lifecycle-state-and-retention-role-are-separate");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 29);
        write_generation(&root, 30);
        write_lifecycle_state(&root, 29, DeploymentLifecycleState::Promoted);
        write_lifecycle_state(&root, 30, DeploymentLifecycleState::Promoted);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        atomic_write(env.previous_pointer(), b"29\n").unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(30)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();
        let history =
            load_environment_history(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert_eq!(
            diagnostics.active_lifecycle_state,
            Some(DeploymentLifecycleState::Promoted)
        );
        assert_eq!(diagnostics.retention_role, Some(RetentionRole::Current));

        let previous = history
            .entries
            .iter()
            .find(|entry| entry.generation == 29)
            .unwrap();
        assert_eq!(
            previous.lifecycle_state,
            Some(DeploymentLifecycleState::Promoted)
        );
        assert_eq!(previous.retention_role, Some(RetentionRole::RollbackTarget));
    }

    #[test]
    fn diagnose_reports_internal_worker_service() {
        let root = test_root("diagnose-reports-internal-worker-service");
        register_project(&root, "api", "api.example.com");
        write_multiservice_generation(&root, 7);
        DiagnosticsStore::new(EnvironmentPaths::new(&root, "api", "staging"), 7)
            .write_summary(&DiagnosticSummary {
                deployment_id: Some("dep-ms-7".into()),
                failure_stage: "warming".into(),
                failure_reason: "worker queue disconnected".into(),
                blocking_reason: Some("worker queue disconnected".into()),
                container_name: "staging-api-worker-gen-7".into(),
                failed_service_name: Some("worker".into()),
                blocking_service_name: Some("worker".into()),
                probe_target_host: None,
                probe_target_port: None,
                probe_target_path: None,
                restart_storm: false,
                restart_policy: None,
                restart_count_delta: None,
                oom_killed: None,
                last_exit_code: None,
                exit_signal: None,
                termination_reason: None,
                cleanup_recorded: false,
                dependency_graph_summary: Some("api<-none; worker<-api".into()),
                runtime_env_preview: Vec::new(),
            })
            .unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(ContainerInspection {
                container_name: "staging-api-api-gen-7".into(),
                running: true,
                state_status: "running".into(),
                exit_code: None,
                restart_count: 0,
                started_at: None,
                finished_at: None,
                oom_killed: false,
                error: None,
                image_ref: "forge/api:staging-gen-7".into(),
                labels: BTreeMap::new(),
                network_ips: BTreeMap::from([("forge-managed".into(), "172.29.0.2".into())]),
                volume_mounts: Vec::new(),
                restart_policy: "always".into(),
                restart_max_retries: None,
                cpu_limit: None,
                memory_limit_mb: None,
                exit_signal: None,
                termination_reason: None,
            }),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(RouteInspection {
                subtree_id: "forge:api:staging:api".into(),
                domain: Some("staging-api.example.com".into()),
                active_target: "172.29.0.2:3000".into(),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
        };

        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();
        let worker = diagnostics
            .services
            .iter()
            .find(|service| service.service_id == "worker")
            .unwrap();
        assert_eq!(worker.role, "internal");
        assert_eq!(worker.route, "none");
        assert_eq!(worker.health, "running");
        assert_eq!(worker.depends_on, vec!["api".to_string()]);
        assert_eq!(worker.dns_aliases, vec!["worker".to_string()]);
        assert_eq!(
            worker.failure_reason.as_deref(),
            Some("worker queue disconnected")
        );
        assert_eq!(worker.logs_tail, vec!["worker polling".to_string()]);
    }

    #[test]
    fn healthy_service_does_not_show_stale_failure_reason() {
        let root = test_root("healthy-service-does-not-show-stale-failure-reason");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        DiagnosticsStore::new(EnvironmentPaths::new(&root, "api", "staging"), 7)
            .write_summary(&DiagnosticSummary {
                deployment_id: Some("dep-7".into()),
                failure_stage: "warming".into(),
                failure_reason: "route activation verification failed".into(),
                blocking_reason: Some("route activation verification failed".into()),
                container_name: "staging-api-gen-7".into(),
                failed_service_name: Some("default".into()),
                blocking_service_name: Some("default".into()),
                probe_target_host: None,
                probe_target_port: None,
                probe_target_path: None,
                restart_storm: false,
                restart_policy: None,
                restart_count_delta: None,
                oom_killed: None,
                last_exit_code: None,
                exit_signal: None,
                termination_reason: None,
                cleanup_recorded: false,
                dependency_graph_summary: None,
                runtime_env_preview: Vec::new(),
            })
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

        let service = diagnostics
            .services
            .iter()
            .find(|service| service.service_id == "default")
            .unwrap();
        assert_eq!(diagnostics.status, "healthy");
        assert_eq!(service.route, "active");
        assert_eq!(service.failure_reason, None);
        assert_eq!(diagnostics.recent_failures.len(), 1);
        assert!(diagnostics.recent_failures[0].historical);
    }

    #[test]
    fn route_repair_success_clears_service_failure_reason() {
        let root = test_root("route-repair-success-clears-service-failure-reason");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        DiagnosticsStore::new(EnvironmentPaths::new(&root, "api", "staging"), 7)
            .write_summary(&DiagnosticSummary {
                deployment_id: Some("dep-7".into()),
                failure_stage: "warming".into(),
                failure_reason: "route activation verification failed".into(),
                blocking_reason: Some("route activation verification failed".into()),
                container_name: "staging-api-gen-7".into(),
                failed_service_name: Some("default".into()),
                blocking_service_name: Some("default".into()),
                probe_target_host: None,
                probe_target_port: None,
                probe_target_path: None,
                restart_storm: false,
                restart_policy: None,
                restart_count_delta: None,
                oom_killed: None,
                last_exit_code: None,
                exit_signal: None,
                termination_reason: None,
                cleanup_recorded: false,
                dependency_graph_summary: None,
                runtime_env_preview: Vec::new(),
            })
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

        assert_eq!(diagnostics.status, "healthy");
        assert!(
            diagnostics
                .services
                .iter()
                .all(|service| service.failure_reason.is_none())
        );
    }

    #[test]
    fn active_restore_detected_from_last_deployment_restore_id() {
        let root = test_root("active-restore-detected-from-last-deployment-restore-id");
        register_project(&root, "api", "api.example.com");
        write_generation_with_deployment_id(&root, 9, "restore-backup-1779481391-gen-9");
        write_backup_metadata_fixture(
            &root,
            "backup-1779481391",
            9,
            "restore-backup-1779481391-gen-9",
            20,
        );

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(9)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        let active_restore = diagnostics.active_restore.unwrap();
        assert_eq!(active_restore.backup_id, "backup-1779481391");
        assert_eq!(active_restore.restored_generation, 9);
        assert_eq!(active_restore.source_generation, Some(3));
        assert_eq!(active_restore.restored_at_unix, Some(20));
    }

    #[test]
    fn active_restore_renders_with_partial_backup_metadata() {
        let root = test_root("active-restore-renders-with-partial-backup-metadata");
        register_project(&root, "api", "api.example.com");
        write_generation_with_deployment_id(&root, 9, "restore-backup-1779481391-gen-9");

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(9)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        let active_restore = diagnostics.active_restore.expect("restore lineage");
        assert_eq!(active_restore.backup_id, "backup-1779481391");
        assert_eq!(active_restore.restored_generation, 9);
        assert_eq!(active_restore.source_generation, None);
        assert_eq!(active_restore.source_deployment_id, None);
        assert_eq!(active_restore.restored_at_unix, None);
    }

    #[test]
    fn active_restore_prefers_backup_metadata_when_available() {
        let root = test_root("active-restore-prefers-backup-metadata-when-available");
        register_project(&root, "api", "api.example.com");
        write_generation_with_deployment_id(&root, 9, "restore-backup-1-gen-9");
        write_backup_metadata_fixture(&root, "backup-1", 9, "restore-backup-1-gen-9", 20);

        let env = EnvironmentPaths::new(&root, "api", "staging");
        RetentionStore::new(env.clone())
            .write(&RetentionMetadata {
                updated_at_unix: Some(20),
                generations: vec![GenerationHistoryRecord {
                    generation: 9,
                    deployment_id: Some("dep-9".into()),
                    restored_from_backup_id: Some("backup-1".into()),
                    restored_from_generation: Some(99),
                    restored_from_deployment_id: Some("dep-99".into()),
                    restored_at_unix: Some(999),
                    retained: true,
                    ..GenerationHistoryRecord::default()
                }],
            })
            .unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(9)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        let active_restore = diagnostics.active_restore.expect("restore lineage");
        assert_eq!(active_restore.backup_id, "backup-1");
        assert_eq!(active_restore.restored_generation, 9);
        assert_eq!(active_restore.source_generation, Some(3));
        assert_eq!(
            active_restore.source_deployment_id.as_deref(),
            Some("dep-3")
        );
        assert_eq!(active_restore.restored_at_unix, Some(20));
    }

    #[test]
    fn active_restore_none_only_for_non_restore_generation() {
        let root = test_root("active-restore-none-only-for-non-restore-generation");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 9);

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(9)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert_eq!(diagnostics.active_restore, None);
    }

    #[test]
    fn diagnose_reports_oom_details() {
        let root = test_root("diagnose-reports-oom-details");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        write_validation_lifecycle(
            &root,
            7,
            DeploymentLifecycleState::OomKilled,
            PersistedValidationSummary {
                restart_count_initial: 0,
                restart_count_current: 1,
                restart_count_stable: false,
                validation_succeeded: false,
                oom_detected: true,
                ..PersistedValidationSummary::default()
            },
            PersistedPromotionSummary {
                gate_reason: Some("container OOMKilled during warmup".into()),
                ..PersistedPromotionSummary::default()
            },
        );
        let mut docker = StubDockerRuntime {
            inspection: Some(ContainerInspection {
                container_name: "staging-api-gen-7".into(),
                running: false,
                state_status: "exited".into(),
                exit_code: Some(137),
                restart_count: 1,
                started_at: Some("2026-05-21T12:00:00Z".into()),
                finished_at: Some("2026-05-21T12:01:00Z".into()),
                oom_killed: true,
                error: None,
                image_ref: "forge/api:staging-gen-7".into(),
                labels: BTreeMap::new(),
                network_ips: BTreeMap::from([("forge-managed".into(), "172.29.0.2".into())]),
                volume_mounts: Vec::new(),
                restart_policy: "on-failure".into(),
                restart_max_retries: Some(3),
                cpu_limit: Some("1.5".into()),
                memory_limit_mb: Some(512),
                exit_signal: Some(9),
                termination_reason: Some("oom_killed".into()),
            }),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();
        let termination = diagnostics.container.termination.unwrap();
        assert!(termination.oom_killed);
        assert_eq!(termination.last_exit_code, Some(137));
        assert_eq!(termination.exit_signal, Some(9));
        assert_eq!(
            termination.termination_reason.as_deref(),
            Some("oom_killed")
        );
        assert_eq!(
            diagnostics.active_lifecycle_state,
            Some(DeploymentLifecycleState::OomKilled)
        );
    }

    #[test]
    fn diagnose_reports_restart_loop() {
        let root = test_root("diagnose-reports-restart-loop");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        write_validation_lifecycle(
            &root,
            7,
            DeploymentLifecycleState::CrashLoop,
            PersistedValidationSummary {
                restart_count_initial: 0,
                restart_count_current: 4,
                restart_count_stable: false,
                validation_succeeded: false,
                restart_storm_detected: true,
                last_probe_error: Some("restart storm detected during warmup".into()),
                ..PersistedValidationSummary::default()
            },
            PersistedPromotionSummary {
                gate_reason: Some("required service entered restart storm".into()),
                ..PersistedPromotionSummary::default()
            },
        );
        let mut docker = StubDockerRuntime {
            inspection: Some(ContainerInspection {
                container_name: "staging-api-gen-7".into(),
                running: false,
                state_status: "restarting".into(),
                exit_code: Some(1),
                restart_count: 4,
                started_at: Some("2026-05-21T12:00:00Z".into()),
                finished_at: Some("2026-05-21T12:01:00Z".into()),
                oom_killed: false,
                error: Some("back-off".into()),
                image_ref: "forge/api:staging-gen-7".into(),
                labels: BTreeMap::new(),
                network_ips: BTreeMap::from([("forge-managed".into(), "172.29.0.2".into())]),
                volume_mounts: Vec::new(),
                restart_policy: "on-failure".into(),
                restart_max_retries: Some(5),
                cpu_limit: None,
                memory_limit_mb: None,
                exit_signal: None,
                termination_reason: Some("exit_code_1".into()),
            }),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();
        assert!(diagnostics.restart_instability);
        assert!(
            diagnostics
                .warmup_failure_summary
                .as_deref()
                .is_some_and(|summary| summary.contains("restart_stable=false"))
        );
        assert_eq!(diagnostics.container.termination.unwrap().restart_count, 4);
        assert_eq!(
            diagnostics.active_lifecycle_state,
            Some(DeploymentLifecycleState::CrashLoop)
        );
    }

    #[test]
    fn internal_service_has_no_route() {
        let root = test_root("internal-service-has-no-route");
        register_project(&root, "api", "api.example.com");
        write_multiservice_generation(&root, 9);

        let mut docker = StubDockerRuntime {
            inspection: Some(ContainerInspection {
                container_name: "staging-api-api-gen-9".into(),
                running: true,
                state_status: "running".into(),
                exit_code: None,
                restart_count: 0,
                started_at: None,
                finished_at: None,
                oom_killed: false,
                error: None,
                image_ref: "forge/api:staging-gen-9".into(),
                labels: BTreeMap::new(),
                network_ips: BTreeMap::from([("forge-managed".into(), "172.29.0.2".into())]),
                volume_mounts: Vec::new(),
                restart_policy: "always".into(),
                restart_max_retries: None,
                cpu_limit: None,
                memory_limit_mb: None,
                exit_signal: None,
                termination_reason: None,
            }),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(RouteInspection {
                subtree_id: "forge:api:staging:api".into(),
                domain: Some("staging-api.example.com".into()),
                active_target: "172.29.0.2:3000".into(),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
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
        let worker = status
            .services
            .iter()
            .find(|service| service.service_id == "worker")
            .unwrap();
        assert_eq!(worker.route, "none");
        assert_eq!(worker.health, "running");
    }

    #[test]
    fn diagnose_reports_volume_state() {
        let root = test_root("diagnose-reports-volume-state");
        register_project(&root, "api", "api.example.com");
        write_stateful_generation(&root, 4);
        let mut docker = StubDockerRuntime {
            inspection: Some(ContainerInspection {
                container_name: "staging-api-db-gen-4".into(),
                running: true,
                state_status: "running".into(),
                exit_code: None,
                restart_count: 0,
                started_at: None,
                finished_at: None,
                oom_killed: false,
                error: None,
                image_ref: "postgres:16".into(),
                labels: BTreeMap::new(),
                network_ips: BTreeMap::from([("forge-managed".into(), "172.29.0.9".into())]),
                volume_mounts: vec![crate::runtime::ContainerVolumeMount {
                    volume_name: "forge-api-staging-vol-postgres-data".into(),
                    mount_path: "/var/lib/postgresql/data".into(),
                }],
                restart_policy: "always".into(),
                restart_max_retries: None,
                cpu_limit: None,
                memory_limit_mb: None,
                exit_signal: None,
                termination_reason: None,
            }),
        };
        let mut routing = StubRoutingRuntime::default();

        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();
        let db = diagnostics
            .services
            .iter()
            .find(|service| service.service_id == "db")
            .unwrap();
        assert_eq!(db.volumes.len(), 1);
        assert_eq!(db.volumes[0].retention, "persistent");
        assert!(db.volumes[0].attached);
        assert!(diagnostics.orphaned_state_warnings.is_empty());
    }

    #[test]
    fn diagnostics_api_hides_historical_policy_repairs_for_healthy_env() {
        let root = test_root("diagnostics-api-hides-historical-policy-repairs-for-healthy-env");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        let env = EnvironmentPaths::new(&root, "api", "staging");
        let events = EventStore::new(env.clone(), 7);
        events
            .append(&EventRecord {
                timestamp_unix: 1,
                project_id: "api".into(),
                environment: "staging".into(),
                generation: Some(7),
                deployment_id: Some("dep-7".into()),
                event_type: "RUNTIME_POLICY_DRIFT_REPAIRED".into(),
                reason: Some("recreated container staging-api-gen-7".into()),
            })
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

        assert!(diagnostics.policy_drift_repairs.is_empty());
        assert!(diagnostics.current_policy_drift_repairs.is_empty());
        assert!(diagnostics.historical_policy_drift_repairs.is_empty());
    }

    #[test]
    fn diagnostics_api_hides_historical_volume_repairs_for_healthy_env() {
        let root = test_root("diagnostics-api-hides-historical-volume-repairs-for-healthy-env");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        let env = EnvironmentPaths::new(&root, "api", "staging");
        let events = EventStore::new(env.clone(), 7);
        for _ in 0..2 {
            events
                .append(&EventRecord {
                    timestamp_unix: 1,
                    project_id: "api".into(),
                    environment: "staging".into(),
                    generation: Some(7),
                    deployment_id: Some("dep-7".into()),
                    event_type: "VOLUME_ATTACHMENT_REPAIRED".into(),
                    reason: Some("recreated container staging-api-gen-7".into()),
                })
                .unwrap();
        }

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert!(diagnostics.volume_repair_events.is_empty());
        assert!(diagnostics.current_volume_repair_events.is_empty());
        assert!(diagnostics.historical_volume_repair_events.is_empty());
    }

    #[test]
    fn diagnostics_api_keeps_current_unresolved_repairs_visible() {
        let root = test_root("diagnostics-api-keeps-current-unresolved-repairs-visible");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        let env = EnvironmentPaths::new(&root, "api", "staging");
        let events = EventStore::new(env.clone(), 7);
        events
            .append(&EventRecord {
                timestamp_unix: 1,
                project_id: "api".into(),
                environment: "staging".into(),
                generation: Some(7),
                deployment_id: Some("dep-7".into()),
                event_type: "VOLUME_ATTACHMENT_REPAIRED".into(),
                reason: Some(
                    "recreated container staging-api-gen-7 due to stale volume attachment state"
                        .into(),
                ),
            })
            .unwrap();
        events
            .append(&EventRecord {
                timestamp_unix: 2,
                project_id: "api".into(),
                environment: "staging".into(),
                generation: Some(7),
                deployment_id: Some("dep-7".into()),
                event_type: "RUNTIME_POLICY_DRIFT_REPAIRED".into(),
                reason: Some("recreated container staging-api-gen-7 to restore runtime policy PersistedRuntimePolicy { restart_policy: \"\", max_retries: None, cpu_limit: None, memory_limit_mb: None }".into()),
            })
            .unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(ContainerInspection {
                running: false,
                state_status: "exited".into(),
                ..healthy_container(7)
            }),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert_eq!(diagnostics.status, "degraded");
        assert_eq!(diagnostics.volume_repair_events.len(), 1);
        assert_eq!(diagnostics.current_volume_repair_events.len(), 1);
        assert!(diagnostics.historical_volume_repair_events.is_empty());
        assert!(diagnostics.volume_repair_events[0].starts_with("gen-7:"));
        assert_eq!(diagnostics.policy_drift_repairs.len(), 1);
        assert_eq!(diagnostics.current_policy_drift_repairs.len(), 1);
        assert!(diagnostics.historical_policy_drift_repairs.is_empty());
        assert!(diagnostics.policy_drift_repairs[0].contains("restart_policy: no"));
        assert!(!diagnostics.policy_drift_repairs[0].contains("restart_policy: \"\""));
    }

    #[test]
    fn diagnostics_api_does_not_expose_empty_restart_policy() {
        let root = test_root("diagnostics-api-does-not-expose-empty-restart-policy");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);
        let env = EnvironmentPaths::new(&root, "api", "staging");
        let events = EventStore::new(env.clone(), 7);
        events
            .append(&EventRecord {
                timestamp_unix: 1,
                project_id: "api".into(),
                environment: "staging".into(),
                generation: Some(7),
                deployment_id: Some("dep-7".into()),
                event_type: "RUNTIME_POLICY_DRIFT_REPAIRED".into(),
                reason: Some("recreated container staging-api-gen-7 to restore runtime policy PersistedRuntimePolicy { restart_policy: \"\", max_retries: None }".into()),
            })
            .unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(ContainerInspection {
                running: false,
                state_status: "exited".into(),
                ..healthy_container(7)
            }),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };
        let diagnostics =
            load_environment_diagnostics(&root, None, &mut docker, &mut routing, "api", "staging")
                .unwrap();

        assert!(
            diagnostics
                .current_policy_drift_repairs
                .iter()
                .all(|line| !line.contains("restart_policy: \"\""))
        );
        assert!(
            diagnostics
                .current_policy_drift_repairs
                .iter()
                .any(|line| line.contains("restart_policy: no"))
        );
    }
}
