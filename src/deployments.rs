use serde_json::Value;
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use crate::events::{EventRecord, redact_text};
use crate::forge_yaml::{
    ForgeRuntimePolicy, ForgeServiceConfig, ForgeStateConfig, ForgeYamlConfig,
    load_optional_forge_yaml,
};
use crate::manifest::{ManifestError, SecretReference, load_optional_manifest};
use crate::metrics::registry as metrics_registry;
use crate::projects::ProjectRegistryStore;
use crate::queue::{DeploymentRecord, PersistentQueue, QueueError};
use crate::reconciliation::{
    ReconciliationIntentStatus, ReconciliationStore, intent_request_for_storage_root,
};
use crate::route_truth::resolve_route_target;
use crate::runtime::{
    BuildImageRequest, ContainerInspection, ContainerRuntimePolicy, CreateContainerRequest,
    CreateVolumeRequest, DockerRuntime, DockerRuntimeError, ProbeError, ProbeRuntime,
    RouteInspection, RouteUpdateRequest, RoutingRuntime, RoutingRuntimeError, VolumeMountRequest,
};
use crate::runtime_env::{
    RuntimeEnvError, RuntimeEnvMetadata, build_runtime_env_artifacts,
    load_desired_runtime_env_config,
};
use crate::secrets::{SecretError, SecretResolution, SecretStore};
use crate::status::derive_environment_domain;
use crate::storage::{
    CleanupRecord, CleanupStore, DeploymentLifecycleState, DiagnosticSummary, DiagnosticsStore,
    EnvironmentPaths, EventStore, GenerationAllocator, GenerationHistoryRecord, LifecycleStore,
    PersistedActivationMode, PersistedBuildInfo, PersistedDeploymentLifecycle,
    PersistedProbeHistoryEntry, PersistedProbeType, PersistedPromotionSummary,
    PersistedRouteTargetSource, PersistedRuntimeInfo, PersistedRuntimePolicy,
    PersistedRuntimeUsageSnapshot, PersistedServiceBuildInfo, PersistedServiceRuntimeInfo,
    PersistedServiceState, PersistedStateConfig, PersistedTerminationInfo,
    PersistedValidationSummary, PersistedVolumeMount, PersistedVolumeRetention, PointerStore,
    ProbeHistoryStore, RetentionStore, RuntimeHealthState, RuntimeState, RuntimeStateStore,
    SnapshotState, SnapshotWriter, StorageError, current_unix_timestamp,
    load_generation_build_info, load_generation_runtime_info, load_generation_snapshot_metadata,
};
use crate::topology::select_primary_service_id;

#[derive(Debug)]
pub enum DeploymentError {
    Queue(QueueError),
    Storage(StorageError),
    Docker(DockerRuntimeError),
    Probe(ProbeError),
    Routing(RoutingRuntimeError),
    Secret(SecretError),
    InvalidInspection(String),
    ValidationFailed(&'static str),
    MissingSecret(String),
    InvalidDesiredEnvConfig(String),
    RollbackUnavailable,
}

#[cfg(test)]
pub mod runtime_policy_drift_normalization {
    use super::*;

    #[test]
    fn default_restart_policy_does_not_trigger_drift() {
        let inspection = ContainerInspection {
            container_name: "prod-api-gen-1".into(),
            running: true,
            state_status: "running".into(),
            exit_code: None,
            restart_count: 0,
            started_at: None,
            image_ref: "forge/api:prod-gen-1".into(),
            labels: BTreeMap::new(),
            network_ips: BTreeMap::new(),
            volume_mounts: Vec::new(),
            restart_policy: "no".into(),
            restart_max_retries: None,
            cpu_limit: None,
            memory_limit_mb: None,
            oom_killed: false,
            finished_at: None,
            error: None,
            exit_signal: None,
            termination_reason: None,
        };

        let result = validate_inspection(
            &inspection,
            "prod-api-gen-1",
            &PersistedRuntimePolicy {
                restart_policy: String::new(),
                ..PersistedRuntimePolicy::default()
            },
        );

        assert!(result.is_ok());
    }
}

impl Display for DeploymentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queue(err) => write!(f, "{err}"),
            Self::Storage(err) => write!(f, "{err}"),
            Self::Docker(err) => write!(f, "{err}"),
            Self::Probe(err) => write!(f, "{err}"),
            Self::Routing(err) => write!(f, "{err}"),
            Self::Secret(err) => write!(f, "{err}"),
            Self::InvalidInspection(err) => write!(f, "{err}"),
            Self::ValidationFailed(err) => write!(f, "{err}"),
            Self::MissingSecret(err) => write!(f, "{err}"),
            Self::InvalidDesiredEnvConfig(err) => write!(f, "{err}"),
            Self::RollbackUnavailable => write!(f, "rollback target unavailable"),
        }
    }
}

impl std::error::Error for DeploymentError {}

const MAX_PROBE_HISTORY_ENTRIES: usize = 64;
const RESTART_STORM_DELTA_THRESHOLD: u64 = 3;
const EXITS_BEFORE_VALIDATION_THRESHOLD: u64 = 2;
const PROBE_INSTABILITY_THRESHOLD: u32 = 3;

fn sanitize_volume_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn deployment_error_from_runtime_env(err: RuntimeEnvError) -> DeploymentError {
    match err {
        RuntimeEnvError::Secret(err) => DeploymentError::Secret(err),
        RuntimeEnvError::Storage(err) => DeploymentError::Storage(err),
        RuntimeEnvError::ReservedKey(key) => DeploymentError::InvalidDesiredEnvConfig(format!(
            "reserved Forge runtime key cannot be configured or deleted: {key}"
        )),
    }
}

fn stateful_volume_name(
    record: &DeploymentRecord,
    generation: u64,
    state: &ForgeStateConfig,
) -> String {
    let project = sanitize_volume_component(&record.project_id);
    let environment = sanitize_volume_component(&record.environment);
    let volume = sanitize_volume_component(&state.volume);
    match state.retention {
        PersistedVolumeRetention::Persistent => {
            format!("forge-{project}-{environment}-vol-{volume}")
        }
        PersistedVolumeRetention::Ephemeral => {
            format!("forge-{project}-{environment}-gen-{generation}-vol-{volume}")
        }
    }
}

fn stateful_volume_labels(
    record: &DeploymentRecord,
    generation: u64,
    service_id: &str,
    state: &ForgeStateConfig,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("forge.managed".into(), "true".into()),
        ("forge.project_id".into(), record.project_id.clone()),
        ("forge.environment".into(), record.environment.clone()),
        ("forge.generation".into(), generation.to_string()),
        ("forge.service_id".into(), service_id.to_string()),
        ("forge.volume_id".into(), state.volume.clone()),
        (
            "forge.volume_retention".into(),
            match state.retention {
                PersistedVolumeRetention::Persistent => "persistent".into(),
                PersistedVolumeRetention::Ephemeral => "ephemeral".into(),
            },
        ),
    ])
}

fn persisted_volume_mount(
    record: &DeploymentRecord,
    generation: u64,
    service_id: &str,
    state: &ForgeStateConfig,
) -> PersistedVolumeMount {
    PersistedVolumeMount {
        volume_id: state.volume.clone(),
        docker_volume_name: stateful_volume_name(record, generation, state),
        mount_path: state.mount_path.clone(),
        service_id: service_id.to_string(),
        generation,
        retention: state.retention.clone(),
    }
}

fn persisted_state_config(state: &ForgeStateConfig) -> PersistedStateConfig {
    PersistedStateConfig {
        volume: state.volume.clone(),
        mount_path: state.mount_path.clone(),
        retention: state.retention.clone(),
        pre_backup_command: state.pre_backup_command.clone(),
    }
}

fn ensure_stateful_volume<RtD: DockerRuntime>(
    docker: &mut RtD,
    record: &DeploymentRecord,
    generation: u64,
    service_id: &str,
    state: &ForgeStateConfig,
) -> Result<PersistedVolumeMount, DeploymentError> {
    let mount = persisted_volume_mount(record, generation, service_id, state);
    docker.ensure_volume(CreateVolumeRequest {
        volume_name: mount.docker_volume_name.clone(),
        labels: stateful_volume_labels(record, generation, service_id, state),
    })?;
    Ok(mount)
}

fn append_probe_history_entry(
    env: &EnvironmentPaths,
    generation: u64,
    probe_type: PersistedProbeType,
    success: bool,
    latency_ms: u64,
    failure_reason: Option<String>,
) -> Result<(), DeploymentError> {
    ProbeHistoryStore::new(env.clone(), generation).append(
        PersistedProbeHistoryEntry {
            timestamp_unix: current_unix_timestamp(),
            probe_type,
            success,
            latency_ms,
            failure_reason,
        },
        MAX_PROBE_HISTORY_ENTRIES,
    )?;
    Ok(())
}

impl From<QueueError> for DeploymentError {
    fn from(value: QueueError) -> Self {
        Self::Queue(value)
    }
}

impl From<StorageError> for DeploymentError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<DockerRuntimeError> for DeploymentError {
    fn from(value: DockerRuntimeError) -> Self {
        Self::Docker(value)
    }
}

impl From<ProbeError> for DeploymentError {
    fn from(value: ProbeError) -> Self {
        Self::Probe(value)
    }
}

impl From<RoutingRuntimeError> for DeploymentError {
    fn from(value: RoutingRuntimeError) -> Self {
        Self::Routing(value)
    }
}

impl From<SecretError> for DeploymentError {
    fn from(value: SecretError) -> Self {
        Self::Secret(value)
    }
}

impl From<ManifestError> for DeploymentError {
    fn from(value: ManifestError) -> Self {
        Self::InvalidInspection(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentExecution {
    pub deployment_id: String,
    pub generation: u64,
    pub image_ref: String,
    pub container_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProbeTargetContext {
    host: String,
    port: u16,
    path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValidationFailureContext {
    inspection: Option<ContainerInspection>,
    probe_target: Option<ProbeTargetContext>,
    attempts: Option<u32>,
    elapsed_ms: Option<u128>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WarmupObservation {
    validation_summary: PersistedValidationSummary,
    promotion_summary: PersistedPromotionSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServiceWarmupObservation {
    service_id: String,
    state: PersistedServiceState,
    validation_summary: PersistedValidationSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct FailureSummaryDetails {
    blocking_service_name: Option<String>,
    blocking_reason: Option<String>,
    restart_storm: bool,
    restart_policy: Option<String>,
    restart_count_delta: Option<u64>,
    oom_killed: Option<bool>,
    last_exit_code: Option<i32>,
    exit_signal: Option<i32>,
    termination_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteActivationContext {
    route_id: String,
    domain: Option<String>,
    upstream_target: String,
    verification_url: Option<String>,
    verification_host: Option<String>,
    verification_status_code: Option<u16>,
    verification_response_body: Option<String>,
    network_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationPolicy {
    pub tcp_required: bool,
    pub http_health_path: Option<String>,
    pub activation: ActivationMode,
    pub minimum_uptime_seconds: u64,
    pub required_consecutive_probe_passes: u32,
}

impl Default for ValidationPolicy {
    fn default() -> Self {
        Self {
            tcp_required: true,
            http_health_path: None,
            activation: ActivationMode::Direct,
            minimum_uptime_seconds: if cfg!(test) { 0 } else { 10 },
            required_consecutive_probe_passes: if cfg!(test) { 1 } else { 3 },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationMode {
    Direct,
    Http { internal_port: u16 },
}

pub const FORGE_MANAGED_DOCKER_NETWORK: &str = "forge-managed";
pub const DEFAULT_VALIDATION_TIMEOUT_MS: u64 = 15_000;
const WARMUP_LOOP_INTERVAL_MS: u64 = 250;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionConfig {
    pub context_path: PathBuf,
    pub dockerfile_path: PathBuf,
    pub network_name: Option<String>,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            context_path: PathBuf::from("."),
            dockerfile_path: PathBuf::from("./Dockerfile"),
            network_name: None,
        }
    }
}

pub struct DeploymentExecutor<'a, D, P, R> {
    storage_root: PathBuf,
    queue: &'a PersistentQueue,
    docker: &'a mut D,
    probes: &'a mut P,
    routing: &'a mut R,
    validation: ValidationPolicy,
    execution: ExecutionConfig,
}

struct DeploymentArtifacts {
    env: EnvironmentPaths,
    generation: u64,
    events: EventStore,
    diagnostics: DiagnosticsStore,
    lifecycle_store: LifecycleStore,
    writer: SnapshotWriter,
}

impl<'a, D: DockerRuntime, P: ProbeRuntime, R: RoutingRuntime> DeploymentExecutor<'a, D, P, R> {
    pub fn new(
        storage_root: impl Into<PathBuf>,
        queue: &'a PersistentQueue,
        docker: &'a mut D,
        probes: &'a mut P,
        routing: &'a mut R,
        validation: ValidationPolicy,
    ) -> Self {
        Self {
            storage_root: storage_root.into(),
            queue,
            docker,
            probes,
            routing,
            validation,
            execution: ExecutionConfig::default(),
        }
    }

    pub fn with_execution_config(mut self, execution: ExecutionConfig) -> Self {
        self.execution = execution;
        self
    }

    fn initialize_deployment_artifacts(
        &self,
        record: &DeploymentRecord,
    ) -> Result<DeploymentArtifacts, DeploymentError> {
        let env =
            EnvironmentPaths::new(&self.storage_root, &record.project_id, &record.environment);
        let generation = GenerationAllocator::new(env.clone()).allocate()?;
        let events = EventStore::new(env.clone(), generation);
        let diagnostics = DiagnosticsStore::new(env.clone(), generation);
        let lifecycle_store = LifecycleStore::new(env.clone(), generation);
        let writer = SnapshotWriter::new(env.clone(), generation)?;
        update_generation_history(&env, generation, |history| {
            history.deployment_id = Some(record.deployment_id.clone());
            history.commit_sha = record.commit_sha.clone();
            history.source_ref = record.source_ref.clone();
            history.source_path = record.source_path.clone();
            history.created_at_unix = history.created_at_unix.or(Some(current_unix_timestamp()));
        })?;
        persist_lifecycle_transition(
            &lifecycle_store,
            &record.project_id,
            &record.environment,
            generation,
            DeploymentLifecycleState::Queued,
            "deployment dequeued for execution",
            None,
            None,
        )?;
        Ok(DeploymentArtifacts {
            env,
            generation,
            events,
            diagnostics,
            lifecycle_store,
            writer,
        })
    }

    pub fn execute_next(&mut self) -> Result<Option<DeploymentExecution>, DeploymentError> {
        let Some(record) = self.queue.start_next()? else {
            return Ok(None);
        };

        match self.execute_record(&record) {
            Ok(execution) => {
                metrics_registry().record_deployment_success();
                self.queue.complete_active()?;
                Ok(Some(execution))
            }
            Err(err) => {
                metrics_registry().record_deployment_failure();
                let _ = self.queue.complete_active();
                Err(err)
            }
        }
    }

    fn execute_record(
        &mut self,
        record: &DeploymentRecord,
    ) -> Result<DeploymentExecution, DeploymentError> {
        if record.intent == "rollback" {
            return self.execute_rollback_record(record);
        }

        let source_root = record
            .source_path
            .clone()
            .unwrap_or_else(|| self.execution.context_path.clone());
        let artifacts = self.initialize_deployment_artifacts(record)?;
        append_event(
            &artifacts.events,
            record,
            artifacts.generation,
            "DEPLOYMENT_STARTED",
            None,
        )?;
        artifacts.diagnostics.append_log_line(
            &format!("deployment started for {}", record.deployment_id),
            &[],
        )?;

        let forge_yaml = match load_optional_forge_yaml(&source_root, &record.project_id) {
            Ok(config) => config,
            Err(err) => {
                let message = err.to_string();
                self.record_preparation_failure(
                    record,
                    &artifacts,
                    classify_forge_yaml_failure_stage(&message),
                    &message,
                    generation_container_name(record, artifacts.generation),
                    None,
                    None,
                    FailureSummaryDetails::default(),
                    &[],
                    &[],
                    CleanupRecord::skipped_failed_generation(&message),
                )?;
                return Err(DeploymentError::InvalidInspection(message));
            }
        };
        if forge_yaml.as_ref().is_some_and(is_multi_service_config) {
            return self.execute_multi_service_record(record, &source_root, artifacts, forge_yaml);
        }
        let default_execution = ExecutionConfig {
            context_path: source_root.clone(),
            dockerfile_path: source_root.join("Dockerfile"),
            network_name: self.execution.network_name.clone(),
        };
        let execution = forge_yaml
            .as_ref()
            .map(|config| {
                let mut execution = config.execution().clone();
                execution.network_name = self.execution.network_name.clone();
                execution
            })
            .unwrap_or(default_execution);
        let validation = forge_yaml
            .as_ref()
            .map(|config| config.validation().clone())
            .unwrap_or_else(|| self.validation.clone());
        let validation_timeout_ms = forge_yaml
            .as_ref()
            .and_then(|config| config.validation_timeout_ms())
            .unwrap_or(DEFAULT_VALIDATION_TIMEOUT_MS);
        let requested_runtime_policy = forge_yaml
            .as_ref()
            .and_then(|config| config.services().values().next())
            .map(|service| service.runtime_policy.clone())
            .unwrap_or_default();
        let container_name = generation_container_name(record, artifacts.generation);
        let domain = match load_environment_domain(
            &self.storage_root,
            &record.project_id,
            &record.environment,
        ) {
            Ok(domain) => domain,
            Err(err) => {
                let message = err.to_string();
                self.record_preparation_failure(
                    record,
                    &artifacts,
                    "runtime_env",
                    &message,
                    container_name.clone(),
                    None,
                    None,
                    FailureSummaryDetails::default(),
                    &[],
                    &[],
                    CleanupRecord::skipped_failed_generation(&message),
                )?;
                return Err(err);
            }
        };
        let runtime_secrets = match self.resolve_runtime_secrets(&execution.context_path, record) {
            Ok(secrets) => secrets,
            Err(DeploymentError::MissingSecret(message)) => {
                self.record_preparation_failure(
                    record,
                    &artifacts,
                    "runtime_env",
                    &message,
                    container_name.clone(),
                    None,
                    Some("required secret resolution failed".into()),
                    FailureSummaryDetails::default(),
                    &[],
                    &[],
                    CleanupRecord::skipped_failed_generation(&message),
                )?;
                append_event(
                    &artifacts.events,
                    record,
                    artifacts.generation,
                    "REQUIRED_SECRET_MISSING",
                    Some(message.clone()),
                )?;
                return Err(DeploymentError::MissingSecret(message));
            }
            Err(err) => return Err(err),
        };
        let DeploymentArtifacts {
            env,
            generation,
            events,
            diagnostics,
            lifecycle_store,
            writer,
        } = artifacts;
        let labels = forge_labels(record, generation);
        let image_tag = format!(
            "forge/{}:{}-gen-{}",
            record.project_id, record.environment, generation
        );
        let forge_yaml_values = forge_yaml
            .as_ref()
            .map(|config| config.environment().clone())
            .unwrap_or_default();
        let desired_env = match load_desired_runtime_env_config(
            &self.storage_root,
            &record.project_id,
            &record.environment,
        ) {
            Ok(value) => value,
            Err(err) => {
                let message = err.to_string();
                self.record_preparation_failure(
                    record,
                    &DeploymentArtifacts {
                        env: env.clone(),
                        generation,
                        events: EventStore::new(env.clone(), generation),
                        diagnostics: DiagnosticsStore::new(env.clone(), generation),
                        lifecycle_store: LifecycleStore::new(env.clone(), generation),
                        writer: SnapshotWriter::new(env.clone(), generation)?,
                    },
                    "runtime_env",
                    &message,
                    container_name.clone(),
                    None,
                    None,
                    FailureSummaryDetails::default(),
                    &[],
                    &[],
                    CleanupRecord::skipped_failed_generation(&message),
                )?;
                return Err(deployment_error_from_runtime_env(err));
            }
        };
        let runtime_env = match build_runtime_env_artifacts(
            &RuntimeEnvMetadata {
                project_id: record.project_id.clone(),
                environment: record.environment.clone(),
                generation,
                deployment_id: record.deployment_id.clone(),
                source_ref: record.source_ref.clone(),
                commit_sha: record.commit_sha.clone(),
                domain: domain.clone(),
            },
            &forge_yaml_values,
            &runtime_secrets,
            &desired_env,
            &BTreeMap::new(),
        ) {
            Ok(runtime_env) => runtime_env,
            Err(err) => {
                let message = err.to_string();
                self.record_preparation_failure(
                    record,
                    &DeploymentArtifacts {
                        env: env.clone(),
                        generation,
                        events: EventStore::new(env.clone(), generation),
                        diagnostics: DiagnosticsStore::new(env.clone(), generation),
                        lifecycle_store: LifecycleStore::new(env.clone(), generation),
                        writer: SnapshotWriter::new(env.clone(), generation)?,
                    },
                    "runtime_env",
                    &message,
                    container_name.clone(),
                    None,
                    None,
                    FailureSummaryDetails::default(),
                    &[],
                    &[],
                    CleanupRecord::skipped_failed_generation(&message),
                )?;
                return Err(deployment_error_from_runtime_env(err));
            }
        };
        let redacted_env_preview = runtime_env.redacted_preview.clone();
        let secret_values = runtime_env.redaction_values.clone();
        persist_lifecycle_transition(
            &lifecycle_store,
            &record.project_id,
            &record.environment,
            generation,
            DeploymentLifecycleState::Building,
            "image build started",
            None,
            None,
        )?;

        let image_ref = match self.docker.build_image(BuildImageRequest {
            image_tag: image_tag.clone(),
            context_path: execution.context_path.clone(),
            dockerfile_path: execution.dockerfile_path.clone(),
            build_args: BTreeMap::new(),
            labels: labels.clone(),
        }) {
            Ok(image_ref) => image_ref,
            Err(err) => {
                let failure_reason = format!("image build failed: {err}");
                persist_lifecycle_transition(
                    &lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::Failed,
                    &failure_reason,
                    None,
                    Some(PersistedPromotionSummary {
                        gate_reason: Some(failure_reason.clone()),
                        ..PersistedPromotionSummary::default()
                    }),
                )?;
                self.record_failed_attempt(
                    &events,
                    &diagnostics,
                    record,
                    generation,
                    "building",
                    &failure_reason,
                    CleanupRecord::skipped_failed_generation(&failure_reason),
                    None,
                    FailureSummaryDetails::default(),
                    &redacted_env_preview,
                    &secret_values,
                )?;
                return Err(err.into());
            }
        };
        append_event(&events, record, generation, "IMAGE_BUILT", None)?;
        diagnostics.append_log_line(&format!("image built: {image_ref}"), &secret_values)?;
        update_generation_history(&env, generation, |history| {
            history.image_ref = Some(image_ref.clone());
        })?;
        if let Some(network_name) = execution.network_name.as_deref() {
            if let Err(err) = self.docker.ensure_network(network_name) {
                let failure_reason =
                    format!("docker network ensure failed for {network_name}: {err}");
                persist_lifecycle_transition(
                    &lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::Failed,
                    &failure_reason,
                    None,
                    Some(PersistedPromotionSummary {
                        gate_reason: Some(failure_reason.clone()),
                        ..PersistedPromotionSummary::default()
                    }),
                )?;
                let cleanup = self.cleanup_failed_generation_artifacts(
                    &env,
                    generation,
                    &failure_reason,
                    None,
                    Some(image_ref.clone()),
                    None,
                )?;
                self.record_failed_attempt(
                    &events,
                    &diagnostics,
                    record,
                    generation,
                    "preparing",
                    &failure_reason,
                    cleanup,
                    None,
                    FailureSummaryDetails::default(),
                    &redacted_env_preview,
                    &secret_values,
                )?;
                return Err(err.into());
            }
            diagnostics.append_log_line(
                &format!("docker network ready: {network_name}"),
                &secret_values,
            )?;
        }

        let volume_mounts: Vec<PersistedVolumeMount> = Vec::new();

        match self.docker.create_container(CreateContainerRequest {
            container_name: container_name.clone(),
            image_ref: image_ref.clone(),
            labels: labels.clone(),
            environment: runtime_env.container_env.clone(),
            network_name: execution.network_name.clone(),
            network_aliases: Vec::new(),
            volume_mounts: volume_mounts
                .iter()
                .map(|mount| VolumeMountRequest {
                    volume_name: mount.docker_volume_name.clone(),
                    mount_path: mount.mount_path.clone(),
                })
                .collect(),
            command: None,
            runtime_policy: container_runtime_policy(&requested_runtime_policy),
        }) {
            Ok(_) => {}
            Err(err) => {
                let failure_reason = format!("container create failed: {err}");
                persist_lifecycle_transition(
                    &lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::Failed,
                    &failure_reason,
                    None,
                    Some(PersistedPromotionSummary {
                        gate_reason: Some(failure_reason.clone()),
                        ..PersistedPromotionSummary::default()
                    }),
                )?;
                let cleanup = self.cleanup_failed_generation_artifacts(
                    &env,
                    generation,
                    &failure_reason,
                    None,
                    Some(image_ref.clone()),
                    None,
                )?;
                self.record_failed_attempt(
                    &events,
                    &diagnostics,
                    record,
                    generation,
                    "preparing",
                    &failure_reason,
                    cleanup,
                    None,
                    FailureSummaryDetails::default(),
                    &redacted_env_preview,
                    &secret_values,
                )?;
                return Err(err.into());
            }
        }
        append_redacted_event(
            &events,
            record,
            generation,
            "RUNTIME_ENV_PREPARED",
            Some(redacted_env_preview.join(", ")),
            &secret_values,
        )?;
        diagnostics.append_log_line(
            &format!(
                "runtime environment prepared: {}",
                redacted_env_preview.join(", ")
            ),
            &secret_values,
        )?;
        persist_lifecycle_transition(
            &lifecycle_store,
            &record.project_id,
            &record.environment,
            generation,
            DeploymentLifecycleState::Starting,
            "container start initiated",
            None,
            None,
        )?;
        if let Err(err) = self.docker.start_container(&container_name) {
            let failure_reason = format!("container start failed: {err}");
            persist_lifecycle_transition(
                &lifecycle_store,
                &record.project_id,
                &record.environment,
                generation,
                DeploymentLifecycleState::Failed,
                &failure_reason,
                None,
                Some(PersistedPromotionSummary {
                    gate_reason: Some(failure_reason.clone()),
                    ..PersistedPromotionSummary::default()
                }),
            )?;
            self.record_failed_generation(
                &env,
                &events,
                &diagnostics,
                record,
                generation,
                &container_name,
                Some(&image_ref),
                None,
                "starting",
                &failure_reason,
                None,
                FailureSummaryDetails::default(),
                &redacted_env_preview,
                &secret_values,
            )?;
            return Err(err.into());
        }
        append_event(&events, record, generation, "CONTAINER_STARTED", None)?;
        diagnostics.append_log_line(
            &format!("container started: {container_name}"),
            &secret_values,
        )?;
        let inspection = match self.docker.inspect_container(&container_name) {
            Ok(inspection) => inspection,
            Err(err) => {
                let failure_reason = format!("container inspection failed: {err}");
                persist_lifecycle_transition(
                    &lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::Failed,
                    &failure_reason,
                    None,
                    Some(PersistedPromotionSummary {
                        gate_reason: Some(failure_reason.clone()),
                        ..PersistedPromotionSummary::default()
                    }),
                )?;
                self.record_failed_generation(
                    &env,
                    &events,
                    &diagnostics,
                    record,
                    generation,
                    &container_name,
                    Some(&image_ref),
                    None,
                    "validating_runtime",
                    &failure_reason,
                    None,
                    FailureSummaryDetails::default(),
                    &redacted_env_preview,
                    &secret_values,
                )?;
                return Err(err.into());
            }
        };
        let runtime_policy = persisted_runtime_policy(&requested_runtime_policy);
        if let Err(err) = validate_inspection(&inspection, &container_name, &runtime_policy) {
            let failure_reason = err.to_string();
            persist_lifecycle_transition(
                &lifecycle_store,
                &record.project_id,
                &record.environment,
                generation,
                DeploymentLifecycleState::Failed,
                &failure_reason,
                None,
                Some(PersistedPromotionSummary {
                    gate_reason: Some(failure_reason.clone()),
                    ..PersistedPromotionSummary::default()
                }),
            )?;
            self.record_failed_generation(
                &env,
                &events,
                &diagnostics,
                record,
                generation,
                &container_name,
                Some(&image_ref),
                None,
                "validating_runtime",
                &failure_reason,
                None,
                FailureSummaryDetails::default(),
                &redacted_env_preview,
                &secret_values,
            )?;
            return Err(err);
        }
        let build_json = serde_json::to_string_pretty(&PersistedBuildInfo {
            deployment_id: record.deployment_id.clone(),
            image_ref: image_ref.clone(),
            services: BTreeMap::new(),
            source_ref: record.source_ref.clone(),
            repo_url: record.repo_url.clone(),
            commit_sha: record.commit_sha.clone(),
            source_path: record.source_path.clone(),
        })
        .map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        writer.write_artifact("build.json", &format!("{build_json}\n"))?;
        let runtime_json = serde_json::to_string_pretty(&PersistedRuntimeInfo {
            container_name: inspection.container_name.clone(),
            running: inspection.running,
            network_name: execution.network_name.clone(),
            probe_path: validation.http_health_path.clone(),
            activation: Some(match &validation.activation {
                ActivationMode::Direct => PersistedActivationMode::Direct,
                ActivationMode::Http { internal_port } => PersistedActivationMode::Http {
                    internal_port: *internal_port,
                    route_subtree_id: Some(route_subtree_id(record)),
                    target_source: PersistedRouteTargetSource::ContainerIp,
                },
            }),
            runtime_policy: runtime_policy.clone(),
            runtime_usage: inspection_runtime_usage(self.docker, &inspection.container_name),
            termination: Some(inspection_termination_info(&inspection, None)),
            environment_variables: runtime_env
                .snapshot
                .entries
                .iter()
                .filter_map(|(key, entry)| {
                    entry
                        .secret_reference
                        .clone()
                        .map(|reference| (key.clone(), reference))
                })
                .collect(),
            volume_mounts: volume_mounts.clone(),
            source_ref: record.source_ref.clone(),
            repo_url: record.repo_url.clone(),
            commit_sha: record.commit_sha.clone(),
            source_path: record.source_path.clone(),
            services: BTreeMap::new(),
            startup_order: Vec::new(),
        })
        .map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        writer.write_artifact("runtime.json", &format!("{runtime_json}\n"))?;
        let runtime_env_snapshot =
            serde_json::to_string_pretty(&runtime_env.snapshot).map_err(|err| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    err.to_string(),
                ))
            })?;
        let runtime_env_snapshot_path = writer.generation_dir().join("runtime_env_snapshot.json");
        writer
            .write_artifact(
                "runtime_env_snapshot.json",
                &format!("{runtime_env_snapshot}\n"),
            )
            .map_err(|err| {
                StorageError::Io(std::io::Error::other(format!(
                    "failed to write {}: {err}",
                    runtime_env_snapshot_path.display()
                )))
            })?;
        diagnostics.append_log_line(
            &format!(
                "runtime env snapshot written: {}",
                runtime_env_snapshot_path.display()
            ),
            &secret_values,
        )?;
        let resolved_runtime =
            serde_json::to_string_pretty(&runtime_env.resolved).map_err(|err| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    err.to_string(),
                ))
            })?;
        writer.write_artifact("resolved_runtime.json", &format!("{resolved_runtime}\n"))?;
        diagnostics.append_log_line("runtime inspection passed", &secret_values)?;
        let warmup = self.validate_candidate(
            &validation,
            validation_timeout_ms,
            &env,
            &lifecycle_store,
            &container_name,
            &image_ref,
            &events,
            &diagnostics,
            record,
            generation,
            &redacted_env_preview,
            &secret_values,
        )?;

        let reconciliation = ReconciliationStore::new(&self.storage_root);
        let snapshot_intent = reconciliation.append_pending(intent_request_for_storage_root(
            &self.storage_root,
            "snapshot_persistence",
            &record.project_id,
            &record.environment,
            Some(generation),
            "healthy",
            "runtime_container_reconciliation",
            BTreeMap::new(),
        ))?;
        writer.finalize(
            &record.project_id,
            &record.environment,
            SnapshotState::Healthy,
        )?;
        let _ = reconciliation.append_status(
            &snapshot_intent,
            ReconciliationIntentStatus::Applied,
            BTreeMap::new(),
        )?;
        let referenced_secret_keys = runtime_env
            .snapshot
            .entries
            .values()
            .filter_map(|entry| {
                entry.secret_reference.as_ref().and_then(|reference| {
                    (reference.scope == "environment").then(|| reference.key.clone())
                })
            })
            .collect::<Vec<_>>();
        SecretStore::new(self.storage_root.join("secrets"))?.record_generation_references(
            &record.project_id,
            &record.environment,
            generation,
            &referenced_secret_keys,
        )?;
        let snapshot = load_generation_snapshot_metadata(&env, generation)?;
        update_generation_history(&env, generation, |history| {
            history.finalized_state = Some("healthy".into());
            history.finalized_at_unix = snapshot.as_ref().map(|value| value.finalized_at_unix);
        })?;
        append_event(&events, record, generation, "SNAPSHOT_FINALIZED", None)?;
        diagnostics.append_log_line("snapshot finalized", &secret_values)?;
        persist_lifecycle_transition(
            &lifecycle_store,
            &record.project_id,
            &record.environment,
            generation,
            DeploymentLifecycleState::Validating,
            "promotion gate satisfied; route verification starting",
            Some(warmup.validation_summary.clone()),
            Some(warmup.promotion_summary.clone()),
        )?;
        if let Err(err) = self.activate_generation(
            &validation,
            &execution,
            record,
            &env,
            &lifecycle_store,
            generation,
            &container_name,
            &inspection,
            &diagnostics,
            &secret_values,
            warmup,
        ) {
            self.record_failed_generation(
                &env,
                &events,
                &diagnostics,
                record,
                generation,
                &container_name,
                Some(&image_ref),
                Some(route_subtree_id(record)),
                "routing",
                &err.to_string(),
                None,
                FailureSummaryDetails::default(),
                &redacted_env_preview,
                &secret_values,
            )?;
            return Err(err);
        }
        append_event(&events, record, generation, "GENERATION_PROMOTED", None)?;
        diagnostics.append_log_line("generation promoted", &secret_values)?;
        update_generation_history(&env, generation, |history| {
            history.promoted_at_unix = Some(current_unix_timestamp());
        })?;
        self.capture_service_container_logs_tail(
            &diagnostics,
            "default",
            &container_name,
            &secret_values,
        )?;
        self.capture_container_logs_tail(&diagnostics, &container_name, &secret_values)?;

        Ok(DeploymentExecution {
            deployment_id: record.deployment_id.clone(),
            generation,
            image_ref,
            container_name,
        })
    }

    fn execute_rollback_record(
        &mut self,
        record: &DeploymentRecord,
    ) -> Result<DeploymentExecution, DeploymentError> {
        let env =
            EnvironmentPaths::new(&self.storage_root, &record.project_id, &record.environment);
        env.ensure_exists()?;
        let pointers = PointerStore::new(env.clone());
        let target = pointers
            .read_pointer("previous")?
            .ok_or(DeploymentError::RollbackUnavailable)?;
        let lifecycle_store = LifecycleStore::new(env.clone(), target);
        let snapshot = load_generation_snapshot_metadata(&env, target)?
            .ok_or(DeploymentError::RollbackUnavailable)?;
        if snapshot.state != "healthy" {
            return Err(DeploymentError::RollbackUnavailable);
        }

        let build = load_generation_build_info(&env, target)?
            .ok_or(DeploymentError::RollbackUnavailable)?;
        let runtime = load_generation_runtime_info(&env, target)?
            .ok_or(DeploymentError::RollbackUnavailable)?;
        let service_runtime = runtime_services(&runtime);
        let reconciliation = ReconciliationStore::new(&self.storage_root);
        let mut inspections = BTreeMap::new();
        for (service_id, service) in &service_runtime {
            let inspection = self
                .docker
                .inspect_container(&service.container_name)
                .map_err(|_| DeploymentError::RollbackUnavailable)?;
            if !inspection.running {
                return Err(DeploymentError::RollbackUnavailable);
            }
            inspections.insert(service_id.clone(), inspection);
        }

        let domain =
            load_environment_domain(&self.storage_root, &record.project_id, &record.environment)?;
        for (service_id, service) in &service_runtime {
            let Some(PersistedActivationMode::Http {
                internal_port,
                route_subtree_id: persisted_subtree_id,
                target_source,
            }) = service.activation.as_ref()
            else {
                continue;
            };
            if !service.externally_exposed {
                continue;
            }
            let inspection = inspections
                .get(service_id)
                .expect("inspection collected for every rollback service");
            let upstream_target = resolve_route_target(
                inspection,
                *internal_port,
                service.network_name.as_deref(),
                target_source,
            )
            .ok_or_else(|| {
                DeploymentError::InvalidInspection(match service.network_name.as_deref() {
                    Some(network_name) => {
                        format!("container missing IP on docker network {network_name}")
                    }
                    None => "container missing network IP".into(),
                })
            })?;
            let subtree_id = persisted_subtree_id.clone().unwrap_or_else(|| {
                route_subtree_id_for_service(record, service_id, service_runtime.len())
            });
            let route_intent = reconciliation.append_pending(intent_request_for_storage_root(
                &self.storage_root,
                "route_activation",
                &record.project_id,
                &record.environment,
                Some(target),
                "healthy",
                "routing_reconciliation",
                BTreeMap::from([
                    ("subtree_id".into(), Value::String(subtree_id.clone())),
                    ("target".into(), Value::String(upstream_target.clone())),
                    (
                        "domain".into(),
                        Value::String(domain.clone().unwrap_or_default()),
                    ),
                    (
                        "probe_path".into(),
                        service
                            .probe_path
                            .clone()
                            .map(Value::String)
                            .unwrap_or(Value::Null),
                    ),
                ]),
            ))?;
            self.routing.update_route(RouteUpdateRequest {
                subtree_id: subtree_id.clone(),
                target: upstream_target.clone(),
                domain: domain.clone(),
                health_checks_enabled: false,
                probe_path: service.probe_path.clone(),
            })?;
            let _ = reconciliation.append_status(
                &route_intent,
                ReconciliationIntentStatus::Applied,
                BTreeMap::new(),
            )?;
            let route_inspection = self.routing.inspect_route(&subtree_id)?;
            validate_route_activation(
                &route_inspection,
                &RouteActivationContext {
                    route_id: subtree_id,
                    domain: domain.clone(),
                    upstream_target,
                    verification_url: route_inspection.verification_url.clone(),
                    verification_host: route_inspection.verification_host.clone(),
                    verification_status_code: route_inspection.verification_status_code,
                    verification_response_body: route_inspection.verification_response_body.clone(),
                    network_name: service.network_name.clone(),
                },
            )?;
        }

        persist_lifecycle_transition(
            &lifecycle_store,
            &record.project_id,
            &record.environment,
            target,
            DeploymentLifecycleState::Rollback,
            "rollback requested",
            None,
            Some(PersistedPromotionSummary {
                gate_reason: Some("rollback requested".into()),
                ..PersistedPromotionSummary::default()
            }),
        )?;
        let rollback_intent = reconciliation.append_pending(intent_request_for_storage_root(
            &self.storage_root,
            "rollback",
            &record.project_id,
            &record.environment,
            Some(target),
            "healthy",
            "runtime_container_reconciliation",
            BTreeMap::new(),
        ))?;
        pointers.swap_current(target)?;
        let _ = reconciliation.append_status(
            &rollback_intent,
            ReconciliationIntentStatus::Applied,
            BTreeMap::new(),
        )?;
        update_generation_history(&env, target, |history| {
            history.restored_by_rollback = true;
            history.promoted_at_unix = Some(current_unix_timestamp());
        })?;
        RuntimeStateStore::new(env.clone()).save(&RuntimeState {
            active_generation: Some(target),
            health_state: RuntimeHealthState::Healthy,
            failed_probe_count: 0,
            successful_probe_count: 0,
            restart_attempted: false,
            degraded_since_unix: None,
            last_transition: "rollback_completed".into(),
            last_error_code: None,
        })?;
        persist_lifecycle_transition(
            &lifecycle_store,
            &record.project_id,
            &record.environment,
            target,
            DeploymentLifecycleState::Promoted,
            "rollback restored generation",
            None,
            Some(PersistedPromotionSummary {
                warmup_succeeded: true,
                validation_succeeded: true,
                route_verification_succeeded: true,
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                promoted_at_unix: Some(current_unix_timestamp()),
                gate_reason: None,
            }),
        )?;
        append_simple_event(
            &EventStore::new(env, target),
            &record.project_id,
            &record.environment,
            target,
            Some(record.deployment_id.clone()),
            "ROLLBACK_COMPLETED",
            None,
        )?;
        metrics_registry().record_rollback();

        Ok(DeploymentExecution {
            deployment_id: record.deployment_id.clone(),
            generation: target,
            image_ref: build.image_ref,
            container_name: runtime.container_name,
        })
    }

    fn execute_multi_service_record(
        &mut self,
        record: &DeploymentRecord,
        source_root: &Path,
        artifacts: DeploymentArtifacts,
        forge_yaml: Option<ForgeYamlConfig>,
    ) -> Result<DeploymentExecution, DeploymentError> {
        let config = forge_yaml.as_ref().ok_or_else(|| {
            DeploymentError::InvalidInspection("multi-service deployment requires forge.yml".into())
        })?;
        let mut execution = config.execution().clone();
        execution.network_name = self.execution.network_name.clone();
        let DeploymentArtifacts {
            env,
            generation,
            events,
            diagnostics,
            lifecycle_store,
            writer,
        } = artifacts;
        let labels = forge_labels(record, generation);
        let validation_timeout_ms = config
            .validation_timeout_ms()
            .unwrap_or(DEFAULT_VALIDATION_TIMEOUT_MS);
        let dependency_graph_summary = format_dependency_graph_summary(config);
        diagnostics.append_log_line(
            &format!("dependency graph: {dependency_graph_summary}"),
            &[],
        )?;
        diagnostics.append_log_line(
            &format!("startup order: {}", config.startup_order().join(" -> ")),
            &[],
        )?;
        let default_container_name =
            generation_service_container_name(record, generation, "api", config.services().len());
        let domain = match load_environment_domain(
            &self.storage_root,
            &record.project_id,
            &record.environment,
        ) {
            Ok(domain) => domain,
            Err(err) => {
                let message = err.to_string();
                self.record_preparation_failure(
                    record,
                    &DeploymentArtifacts {
                        env,
                        generation,
                        events,
                        diagnostics,
                        lifecycle_store,
                        writer,
                    },
                    "runtime_env",
                    &message,
                    default_container_name,
                    None,
                    Some(dependency_graph_summary),
                    FailureSummaryDetails::default(),
                    &[],
                    &[],
                    CleanupRecord::skipped_failed_generation(&message),
                )?;
                return Err(err);
            }
        };
        let runtime_secrets = match self.resolve_runtime_secrets(source_root, record) {
            Ok(secrets) => secrets,
            Err(err) => {
                let message = err.to_string();
                self.record_preparation_failure(
                    record,
                    &DeploymentArtifacts {
                        env,
                        generation,
                        events,
                        diagnostics,
                        lifecycle_store,
                        writer,
                    },
                    "runtime_env",
                    &message,
                    default_container_name,
                    None,
                    Some(dependency_graph_summary),
                    FailureSummaryDetails::default(),
                    &[],
                    &[],
                    CleanupRecord::skipped_failed_generation(&message),
                )?;
                return Err(err);
            }
        };
        let desired_env = match load_desired_runtime_env_config(
            &self.storage_root,
            &record.project_id,
            &record.environment,
        ) {
            Ok(value) => value,
            Err(err) => {
                let message = err.to_string();
                self.record_preparation_failure(
                    record,
                    &DeploymentArtifacts {
                        env,
                        generation,
                        events,
                        diagnostics,
                        lifecycle_store,
                        writer,
                    },
                    "runtime_env",
                    &message,
                    default_container_name,
                    None,
                    Some(dependency_graph_summary.clone()),
                    FailureSummaryDetails::default(),
                    &[],
                    &[],
                    CleanupRecord::skipped_failed_generation(&message),
                )?;
                return Err(deployment_error_from_runtime_env(err));
            }
        };
        let runtime_env = match build_runtime_env_artifacts(
            &RuntimeEnvMetadata {
                project_id: record.project_id.clone(),
                environment: record.environment.clone(),
                generation,
                deployment_id: record.deployment_id.clone(),
                source_ref: record.source_ref.clone(),
                commit_sha: record.commit_sha.clone(),
                domain: domain.clone(),
            },
            config.environment(),
            &runtime_secrets,
            &desired_env,
            &BTreeMap::new(),
        ) {
            Ok(runtime_env) => runtime_env,
            Err(err) => {
                let message = err.to_string();
                self.record_preparation_failure(
                    record,
                    &DeploymentArtifacts {
                        env,
                        generation,
                        events,
                        diagnostics,
                        lifecycle_store,
                        writer,
                    },
                    "runtime_env",
                    &message,
                    default_container_name,
                    None,
                    Some(dependency_graph_summary),
                    FailureSummaryDetails::default(),
                    &[],
                    &[],
                    CleanupRecord::skipped_failed_generation(&message),
                )?;
                return Err(deployment_error_from_runtime_env(err));
            }
        };
        let redacted_env_preview = runtime_env.redacted_preview.clone();
        let secret_values = runtime_env.redaction_values.clone();
        diagnostics.append_log_line(
            &format!(
                "multi-service deployment started for {}",
                record.deployment_id
            ),
            &secret_values,
        )?;

        if let Some(network_name) = execution.network_name.as_deref() {
            if let Err(err) = self.docker.ensure_network(network_name) {
                let message = format!("docker network ensure failed for {network_name}: {err}");
                self.record_preparation_failure(
                    record,
                    &DeploymentArtifacts {
                        env: env.clone(),
                        generation,
                        events: EventStore::new(env.clone(), generation),
                        diagnostics: DiagnosticsStore::new(env.clone(), generation),
                        lifecycle_store: LifecycleStore::new(env.clone(), generation),
                        writer: SnapshotWriter::new(env.clone(), generation)?,
                    },
                    "preparing",
                    &message,
                    generation_service_container_name(
                        record,
                        generation,
                        "api",
                        config.services().len(),
                    ),
                    None,
                    Some(dependency_graph_summary.clone()),
                    FailureSummaryDetails::default(),
                    &redacted_env_preview,
                    &secret_values,
                    CleanupRecord::skipped_failed_generation(&message),
                )?;
                return Err(err.into());
            }
            diagnostics.append_log_line(
                &format!("docker network ready: {network_name}"),
                &secret_values,
            )?;
        }

        let mut service_builds = BTreeMap::new();
        let mut service_runtime: BTreeMap<String, PersistedServiceRuntimeInfo> = BTreeMap::new();
        for service_id in config.startup_order() {
            let service = config
                .services()
                .get(service_id)
                .expect("startup order references known service");
            diagnostics.append_log_line(
                &format!(
                    "[service:{service_id}] role: {}",
                    service_role_label(service)
                ),
                &secret_values,
            )?;
            diagnostics.append_log_line(
                &format!(
                    "[service:{service_id}] depends_on: {}",
                    service_dependency_list(service)
                ),
                &secret_values,
            )?;
            if let Some((dependency_id, dependency_state)) =
                service.depends_on.iter().find_map(|dependency_id| {
                    service_runtime
                        .get(dependency_id)
                        .filter(|runtime| runtime.state != PersistedServiceState::Healthy)
                        .map(|runtime| (dependency_id, runtime.state.clone()))
                })
            {
                let failure_reason = format!(
                    "service `{service_id}` blocked by unstable dependency `{dependency_id}` ({dependency_state:?})"
                );
                self.record_preparation_failure(
                    record,
                    &DeploymentArtifacts {
                        env: env.clone(),
                        generation,
                        events: EventStore::new(env.clone(), generation),
                        diagnostics: DiagnosticsStore::new(env.clone(), generation),
                        lifecycle_store: LifecycleStore::new(env.clone(), generation),
                        writer: SnapshotWriter::new(env.clone(), generation)?,
                    },
                    "warming",
                    &failure_reason,
                    generation_service_container_name(
                        record,
                        generation,
                        service_id,
                        config.services().len(),
                    ),
                    Some(service_id.clone()),
                    Some(dependency_graph_summary.clone()),
                    FailureSummaryDetails {
                        blocking_service_name: Some(dependency_id.clone()),
                        blocking_reason: Some(failure_reason.clone()),
                        ..FailureSummaryDetails::default()
                    },
                    &redacted_env_preview,
                    &secret_values,
                    CleanupRecord::skipped_failed_generation(&failure_reason),
                )?;
                persist_lifecycle_transition(
                    &lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::Unstable,
                    &failure_reason,
                    None,
                    Some(PersistedPromotionSummary {
                        gate_reason: Some(failure_reason.clone()),
                        runtime_snapshot_persisted: true,
                        convergence_target_stable: true,
                        ..PersistedPromotionSummary::default()
                    }),
                )?;
                return Err(DeploymentError::ValidationFailed(
                    "unstable dependency blocked promotion",
                ));
            }
            let container_name = generation_service_container_name(
                record,
                generation,
                service_id,
                config.services().len(),
            );
            let build_config = service
                .build
                .as_ref()
                .or_else(|| config.default_service_build());
            let image_ref = if let Some(image_ref) = service.image.clone() {
                diagnostics.append_log_line(
                    &format!("[service:{service_id}] using runtime image {image_ref}"),
                    &secret_values,
                )?;
                image_ref
            } else if let Some(build_config) = build_config {
                persist_lifecycle_transition(
                    &lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::Building,
                    format!("service `{service_id}` image build started"),
                    None,
                    None,
                )?;
                diagnostics
                    .append_log_line(&format!("[service:{service_id}] building"), &secret_values)?;
                let mut build_labels = labels.clone();
                build_labels.insert("forge.service_id".into(), service_id.clone());
                let image_ref = match self.docker.build_image(BuildImageRequest {
                    image_tag: format!(
                        "forge/{}:{}-gen-{}-{}",
                        record.project_id, record.environment, generation, service_id
                    ),
                    context_path: build_config.context_path.clone(),
                    dockerfile_path: build_config.dockerfile_path.clone(),
                    build_args: build_config.build_args.clone(),
                    labels: build_labels,
                }) {
                    Ok(image_ref) => image_ref,
                    Err(err) => {
                        let message = format!("service `{service_id}` build failed: {err}");
                        self.record_preparation_failure(
                            record,
                            &DeploymentArtifacts {
                                env: env.clone(),
                                generation,
                                events: EventStore::new(env.clone(), generation),
                                diagnostics: DiagnosticsStore::new(env.clone(), generation),
                                lifecycle_store: LifecycleStore::new(env.clone(), generation),
                                writer: SnapshotWriter::new(env.clone(), generation)?,
                            },
                            "building",
                            &message,
                            container_name.clone(),
                            Some(service_id.clone()),
                            Some(dependency_graph_summary.clone()),
                            FailureSummaryDetails::default(),
                            &redacted_env_preview,
                            &secret_values,
                            CleanupRecord::skipped_failed_generation(&message),
                        )?;
                        return Err(err.into());
                    }
                };
                diagnostics.append_log_line(
                    &format!("[service:{service_id}] built image {image_ref}"),
                    &secret_values,
                )?;
                image_ref
            } else {
                return Err(DeploymentError::InvalidInspection(format!(
                    "service `{service_id}` has no runtime image and no build configuration"
                )));
            };
            service_builds.insert(
                service_id.clone(),
                PersistedServiceBuildInfo {
                    service_id: service_id.clone(),
                    image_ref: image_ref.clone(),
                    context_path: build_config.map(|config| config.context_path.clone()),
                    dockerfile_path: build_config.map(|config| config.dockerfile_path.clone()),
                    build_args: build_config
                        .map(|config| config.build_args.clone())
                        .unwrap_or_default(),
                    state_config: service.state.as_ref().map(persisted_state_config),
                },
            );
            let mut service_labels = labels.clone();
            service_labels.insert("forge.service_id".into(), service_id.clone());
            service_labels.insert(
                "forge.route_id".into(),
                route_subtree_id_for_service(record, service_id, config.services().len()),
            );
            let volume_mounts = match service.state.as_ref() {
                Some(state) => vec![ensure_stateful_volume(
                    self.docker,
                    record,
                    generation,
                    service_id,
                    state,
                )?],
                None => Vec::new(),
            };
            diagnostics
                .append_log_line(&format!("[service:{service_id}] starting"), &secret_values)?;
            self.docker.create_container(CreateContainerRequest {
                container_name: container_name.clone(),
                image_ref: image_ref.clone(),
                labels: service_labels,
                environment: runtime_env.container_env.clone(),
                network_name: execution.network_name.clone(),
                network_aliases: vec![service_id.clone()],
                volume_mounts: volume_mounts
                    .iter()
                    .map(|mount| VolumeMountRequest {
                        volume_name: mount.docker_volume_name.clone(),
                        mount_path: mount.mount_path.clone(),
                    })
                    .collect(),
                command: service_command(service),
                runtime_policy: container_runtime_policy(&service.runtime_policy),
            })?;
            self.docker.start_container(&container_name)?;
            let inspection = self.docker.inspect_container(&container_name)?;
            let runtime_policy = persisted_runtime_policy(&service.runtime_policy);
            validate_inspection(&inspection, &container_name, &runtime_policy)?;
            diagnostics.append_log_line(
                &format!("[service:{service_id}] validating"),
                &secret_values,
            )?;
            let warmup = self.validate_service_candidate(
                service_id,
                service,
                validation_timeout_ms,
                &env,
                &lifecycle_store,
                generation,
                &container_name,
                &events,
                record,
                &diagnostics,
                &redacted_env_preview,
                &secret_values,
            )?;
            service_runtime.insert(
                service_id.clone(),
                PersistedServiceRuntimeInfo {
                    service_id: service_id.clone(),
                    container_name: inspection.container_name.clone(),
                    image_ref,
                    running: inspection.running,
                    state: warmup.state,
                    network_name: execution.network_name.clone(),
                    probe_path: service.validation.http_health_path.clone(),
                    activation: Some(match service.validation.activation {
                        ActivationMode::Direct => PersistedActivationMode::Direct,
                        ActivationMode::Http { internal_port } => PersistedActivationMode::Http {
                            internal_port,
                            route_subtree_id: Some(route_subtree_id_for_service(
                                record,
                                service_id,
                                config.services().len(),
                            )),
                            target_source: PersistedRouteTargetSource::ContainerIp,
                        },
                    }),
                    command: service_command(service),
                    runtime_policy,
                    runtime_usage: inspection_runtime_usage(
                        self.docker,
                        &inspection.container_name,
                    ),
                    termination: Some(inspection_termination_info(&inspection, None)),
                    depends_on: service.depends_on.clone(),
                    required_for_promotion: service.required_for_promotion,
                    externally_exposed: service.externally_exposed,
                    environment_variables: runtime_env
                        .snapshot
                        .entries
                        .iter()
                        .filter_map(|(key, entry)| {
                            entry
                                .secret_reference
                                .clone()
                                .map(|reference| (key.clone(), reference))
                        })
                        .collect(),
                    state_config: service.state.as_ref().map(persisted_state_config),
                    volume_mounts: volume_mounts.clone(),
                    source_ref: record.source_ref.clone(),
                    repo_url: record.repo_url.clone(),
                    commit_sha: record.commit_sha.clone(),
                    source_path: record.source_path.clone(),
                },
            );
        }

        let primary_service = select_primary_service(config, &service_runtime)?;
        let primary_runtime = service_runtime
            .get(&primary_service)
            .expect("primary service exists");
        update_generation_history(&env, generation, |history| {
            history.image_ref = Some(primary_runtime.image_ref.clone());
        })?;
        let build_json = serde_json::to_string_pretty(&PersistedBuildInfo {
            deployment_id: record.deployment_id.clone(),
            image_ref: primary_runtime.image_ref.clone(),
            services: service_builds,
            source_ref: record.source_ref.clone(),
            repo_url: record.repo_url.clone(),
            commit_sha: record.commit_sha.clone(),
            source_path: record.source_path.clone(),
        })
        .map_err(json_storage_error)?;
        writer.write_artifact("build.json", &format!("{build_json}\n"))?;
        let runtime_json = serde_json::to_string_pretty(&PersistedRuntimeInfo {
            container_name: primary_runtime.container_name.clone(),
            running: primary_runtime.running,
            network_name: primary_runtime.network_name.clone(),
            probe_path: primary_runtime.probe_path.clone(),
            activation: primary_runtime.activation.clone(),
            runtime_policy: primary_runtime.runtime_policy.clone(),
            runtime_usage: primary_runtime.runtime_usage.clone(),
            termination: primary_runtime.termination.clone(),
            environment_variables: primary_runtime.environment_variables.clone(),
            volume_mounts: primary_runtime.volume_mounts.clone(),
            source_ref: record.source_ref.clone(),
            repo_url: record.repo_url.clone(),
            commit_sha: record.commit_sha.clone(),
            source_path: record.source_path.clone(),
            services: service_runtime.clone(),
            startup_order: config.startup_order().to_vec(),
        })
        .map_err(json_storage_error)?;
        writer.write_artifact("runtime.json", &format!("{runtime_json}\n"))?;
        let runtime_env_snapshot =
            serde_json::to_string_pretty(&runtime_env.snapshot).map_err(json_storage_error)?;
        writer.write_artifact(
            "runtime_env_snapshot.json",
            &format!("{runtime_env_snapshot}\n"),
        )?;
        let resolved_runtime =
            serde_json::to_string_pretty(&runtime_env.resolved).map_err(json_storage_error)?;
        writer.write_artifact("resolved_runtime.json", &format!("{resolved_runtime}\n"))?;
        writer.finalize(
            &record.project_id,
            &record.environment,
            SnapshotState::Healthy,
        )?;
        persist_lifecycle_transition(
            &lifecycle_store,
            &record.project_id,
            &record.environment,
            generation,
            DeploymentLifecycleState::Validating,
            "all required services healthy; route activation starting",
            None,
            Some(PersistedPromotionSummary {
                warmup_succeeded: true,
                validation_succeeded: true,
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                gate_reason: None,
                ..PersistedPromotionSummary::default()
            }),
        )?;

        let reconciliation = ReconciliationStore::new(&self.storage_root);
        for (service_id, runtime) in &service_runtime {
            if !runtime.externally_exposed {
                continue;
            }
            let inspection = self.docker.inspect_container(&runtime.container_name)?;
            let PersistedActivationMode::Http {
                internal_port,
                route_subtree_id,
                target_source,
            } = runtime.activation.clone().ok_or_else(|| {
                DeploymentError::InvalidInspection(format!(
                    "service `{service_id}` missing activation metadata"
                ))
            })?
            else {
                continue;
            };
            let target = resolve_route_target(
                &inspection,
                internal_port,
                execution.network_name.as_deref(),
                &target_source,
            )
            .ok_or_else(|| {
                DeploymentError::InvalidInspection(format!(
                    "service `{service_id}` missing network IP for route activation"
                ))
            })?;
            let subtree_id = route_subtree_id.unwrap_or_else(|| {
                route_subtree_id_for_service(record, service_id, config.services().len())
            });
            let route_intent = reconciliation.append_pending(intent_request_for_storage_root(
                &self.storage_root,
                "route_activation",
                &record.project_id,
                &record.environment,
                Some(generation),
                "healthy",
                "routing_reconciliation",
                BTreeMap::from([
                    ("subtree_id".into(), Value::String(subtree_id.clone())),
                    ("target".into(), Value::String(target.clone())),
                    (
                        "domain".into(),
                        Value::String(domain.clone().unwrap_or_default()),
                    ),
                    (
                        "probe_path".into(),
                        runtime
                            .probe_path
                            .clone()
                            .map(Value::String)
                            .unwrap_or(Value::Null),
                    ),
                ]),
            ))?;
            self.routing.update_route(RouteUpdateRequest {
                subtree_id: subtree_id.clone(),
                target: target.clone(),
                domain: domain.clone(),
                health_checks_enabled: false,
                probe_path: runtime.probe_path.clone(),
            })?;
            let _ = reconciliation.append_status(
                &route_intent,
                ReconciliationIntentStatus::Applied,
                BTreeMap::new(),
            )?;
            let route_inspection = self.routing.inspect_route(&subtree_id)?;
            validate_route_activation(
                &route_inspection,
                &RouteActivationContext {
                    route_id: subtree_id,
                    domain: domain.clone(),
                    upstream_target: target,
                    verification_url: route_inspection.verification_url.clone(),
                    verification_host: route_inspection.verification_host.clone(),
                    verification_status_code: route_inspection.verification_status_code,
                    verification_response_body: route_inspection.verification_response_body.clone(),
                    network_name: execution.network_name.clone(),
                },
            )?;
        }

        let referenced_secret_keys = runtime_env
            .snapshot
            .entries
            .values()
            .filter_map(|entry| {
                entry.secret_reference.as_ref().and_then(|reference| {
                    (reference.scope == "environment").then(|| reference.key.clone())
                })
            })
            .collect::<Vec<_>>();
        SecretStore::new(self.storage_root.join("secrets"))?.record_generation_references(
            &record.project_id,
            &record.environment,
            generation,
            &referenced_secret_keys,
        )?;
        let promotion_intent = reconciliation.append_pending(intent_request_for_storage_root(
            &self.storage_root,
            "deployment_promotion",
            &record.project_id,
            &record.environment,
            Some(generation),
            "healthy",
            "runtime_container_reconciliation",
            BTreeMap::new(),
        ))?;
        PointerStore::new(env.clone()).swap_current(generation)?;
        let _ = reconciliation.append_status(
            &promotion_intent,
            ReconciliationIntentStatus::Applied,
            BTreeMap::new(),
        )?;
        RuntimeStateStore::new(env.clone()).save(&RuntimeState {
            active_generation: Some(generation),
            health_state: RuntimeHealthState::Healthy,
            failed_probe_count: 0,
            successful_probe_count: 0,
            restart_attempted: false,
            degraded_since_unix: None,
            last_transition: "promotion_completed".into(),
            last_error_code: None,
        })?;
        persist_lifecycle_transition(
            &lifecycle_store,
            &record.project_id,
            &record.environment,
            generation,
            DeploymentLifecycleState::Promoted,
            "multi-service generation promoted",
            None,
            Some(PersistedPromotionSummary {
                warmup_succeeded: true,
                validation_succeeded: true,
                route_verification_succeeded: true,
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                promoted_at_unix: Some(current_unix_timestamp()),
                gate_reason: None,
            }),
        )?;
        for runtime in service_runtime.values() {
            self.capture_service_container_logs_tail(
                &diagnostics,
                &runtime.service_id,
                &runtime.container_name,
                &secret_values,
            )?;
        }
        self.capture_container_logs_tail(
            &diagnostics,
            &primary_runtime.container_name,
            &secret_values,
        )?;
        update_generation_history(&env, generation, |history| {
            history.promoted_at_unix = Some(current_unix_timestamp());
            history.finalized_state = Some("healthy".into());
            history.finalized_at_unix = Some(current_unix_timestamp());
        })?;
        append_event(&events, record, generation, "GENERATION_PROMOTED", None)?;

        Ok(DeploymentExecution {
            deployment_id: record.deployment_id.clone(),
            generation,
            image_ref: primary_runtime.image_ref.clone(),
            container_name: primary_runtime.container_name.clone(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_service_candidate(
        &mut self,
        service_id: &str,
        service: &ForgeServiceConfig,
        validation_timeout_ms: u64,
        env: &EnvironmentPaths,
        lifecycle_store: &LifecycleStore,
        generation: u64,
        container_name: &str,
        events: &EventStore,
        record: &DeploymentRecord,
        diagnostics: &DiagnosticsStore,
        redacted_env_preview: &[String],
        secret_values: &[String],
    ) -> Result<ServiceWarmupObservation, DeploymentError> {
        let internal_port = match service.validation.activation {
            ActivationMode::Http { internal_port } => Some(internal_port),
            ActivationMode::Direct => None,
        };
        let initial_inspection = self.docker.inspect_container(container_name)?;
        let restart_count_initial = initial_inspection.restart_count;
        if !service.validation.tcp_required && service.validation.http_health_path.is_none() {
            if !initial_inspection.running {
                let failure_reason =
                    format!("service `{service_id}` exited before validation completed");
                let termination = self.persist_service_runtime_diagnostics(
                    diagnostics,
                    service_id,
                    &initial_inspection,
                    secret_values,
                )?;
                let validation_summary = PersistedValidationSummary {
                    restart_count_initial,
                    restart_count_current: initial_inspection.restart_count,
                    restart_count_stable: true,
                    validation_succeeded: false,
                    last_probe_error: Some(failure_reason.clone()),
                    oom_detected: initial_inspection.oom_killed,
                    ..PersistedValidationSummary::default()
                };
                persist_lifecycle_transition(
                    lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    if initial_inspection.oom_killed {
                        DeploymentLifecycleState::OomKilled
                    } else {
                        DeploymentLifecycleState::Failed
                    },
                    &failure_reason,
                    Some(validation_summary),
                    Some(PersistedPromotionSummary {
                        gate_reason: Some(failure_reason.clone()),
                        runtime_snapshot_persisted: true,
                        convergence_target_stable: true,
                        ..PersistedPromotionSummary::default()
                    }),
                )?;
                let cleanup = self.cleanup_failed_generation_artifacts(
                    env,
                    generation,
                    &failure_reason,
                    Some(container_name.to_string()),
                    Some(initial_inspection.image_ref.clone()),
                    None,
                )?;
                self.record_failed_attempt(
                    events,
                    diagnostics,
                    record,
                    generation,
                    "warming",
                    &failure_reason,
                    cleanup,
                    None,
                    FailureSummaryDetails {
                        blocking_service_name: Some(service_id.to_string()),
                        blocking_reason: Some(failure_reason.clone()),
                        oom_killed: Some(initial_inspection.oom_killed),
                        last_exit_code: termination.last_exit_code,
                        exit_signal: termination.exit_signal,
                        termination_reason: termination.termination_reason,
                        restart_policy: Some(initial_inspection.restart_policy.clone()),
                        ..FailureSummaryDetails::default()
                    },
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed(
                    "required service exited before validation completed",
                ));
            }
            diagnostics.append_log_line(
                &format!("service `{service_id}` marked healthy without active probes"),
                secret_values,
            )?;
            return Ok(ServiceWarmupObservation {
                service_id: service_id.to_string(),
                state: PersistedServiceState::Healthy,
                validation_summary: PersistedValidationSummary {
                    validation_succeeded: true,
                    ..PersistedValidationSummary::default()
                },
            });
        }
        let probe_host = resolve_selected_network_host(
            &initial_inspection,
            self.execution.network_name.as_deref(),
        )?;
        let started = Instant::now();
        let required_passes = service.validation.required_consecutive_probe_passes.max(1);
        let minimum_uptime_seconds = service.validation.minimum_uptime_seconds;
        let mut tcp_passes = 0u32;
        let mut http_passes = if service.validation.http_health_path.is_some() {
            0
        } else {
            required_passes
        };
        let mut last_error = None;
        let mut unstable_probe_failures = 0u32;
        let budget = Duration::from_millis(validation_timeout_ms);
        loop {
            let inspection = self.docker.inspect_container(container_name)?;
            let restart_count_current = inspection.restart_count;
            let restart_count_delta = restart_count_delta(restart_count_initial, &inspection);
            let restart_storm = restart_storm_detected(restart_count_initial, &inspection);
            let observed_exit_count =
                observed_exit_count_before_validation(restart_count_initial, &inspection);
            let prior_probe_success = if service.validation.http_health_path.is_some() {
                http_passes > 0
            } else {
                tcp_passes > 0
            };
            if inspection.oom_killed
                || (!inspection.running && inspection.exit_code.unwrap_or(0) != 0)
            {
                let lifecycle_state = if inspection.oom_killed {
                    DeploymentLifecycleState::OomKilled
                } else if restart_storm || observed_exit_count > EXITS_BEFORE_VALIDATION_THRESHOLD {
                    DeploymentLifecycleState::CrashLoop
                } else {
                    DeploymentLifecycleState::Failed
                };
                let service_state = if inspection.oom_killed {
                    PersistedServiceState::OomKilled
                } else if restart_storm || observed_exit_count > EXITS_BEFORE_VALIDATION_THRESHOLD {
                    PersistedServiceState::CrashLoop
                } else {
                    PersistedServiceState::Failed
                };
                let failure_reason = if inspection.oom_killed {
                    format!("service `{service_id}` OOMKilled during warmup")
                } else {
                    format!(
                        "service `{service_id}` exited with nonzero status before validation completed"
                    )
                };
                let termination = self.persist_service_runtime_diagnostics(
                    diagnostics,
                    service_id,
                    &inspection,
                    secret_values,
                )?;
                let validation_summary = PersistedValidationSummary {
                    tcp_consecutive_passes: tcp_passes,
                    http_consecutive_passes: http_passes,
                    required_consecutive_passes: required_passes,
                    minimum_uptime_seconds,
                    observed_uptime_seconds: started.elapsed().as_secs(),
                    restart_count_initial,
                    restart_count_current,
                    restart_count_stable: restart_count_delta == 0,
                    route_verification_stable: true,
                    validation_succeeded: false,
                    last_probe_error: Some(failure_reason.clone()),
                    unstable_probe_failures,
                    restart_storm_detected: restart_storm,
                    oom_detected: inspection.oom_killed,
                };
                persist_lifecycle_transition(
                    lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    lifecycle_state,
                    &failure_reason,
                    Some(validation_summary),
                    Some(PersistedPromotionSummary {
                        gate_reason: Some(failure_reason.clone()),
                        runtime_snapshot_persisted: true,
                        convergence_target_stable: true,
                        ..PersistedPromotionSummary::default()
                    }),
                )?;
                let probe_target = internal_port.map(|port| ProbeTargetContext {
                    host: probe_host.clone(),
                    port,
                    path: service.validation.http_health_path.clone(),
                });
                let cleanup = self.cleanup_failed_generation_artifacts(
                    env,
                    generation,
                    &failure_reason,
                    Some(container_name.to_string()),
                    Some(inspection.image_ref.clone()),
                    None,
                )?;
                self.record_failed_attempt(
                    events,
                    diagnostics,
                    record,
                    generation,
                    "warming",
                    &failure_reason,
                    cleanup,
                    probe_target.as_ref(),
                    FailureSummaryDetails {
                        blocking_service_name: Some(service_id.to_string()),
                        blocking_reason: Some(failure_reason.clone()),
                        restart_storm,
                        restart_policy: Some(inspection.restart_policy.clone()),
                        restart_count_delta: Some(restart_count_delta),
                        oom_killed: Some(inspection.oom_killed),
                        last_exit_code: termination.last_exit_code,
                        exit_signal: termination.exit_signal,
                        termination_reason: termination.termination_reason,
                    },
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed(match service_state {
                    PersistedServiceState::OomKilled => "required service was OOMKilled",
                    PersistedServiceState::CrashLoop => "required service entered crash loop",
                    _ => "required service exited before validation completed",
                }));
            }
            if restart_storm {
                let failure_reason =
                    format!("service `{service_id}` entered restart storm during warmup");
                let termination = self.persist_service_runtime_diagnostics(
                    diagnostics,
                    service_id,
                    &inspection,
                    secret_values,
                )?;
                let validation_summary = PersistedValidationSummary {
                    tcp_consecutive_passes: tcp_passes,
                    http_consecutive_passes: http_passes,
                    required_consecutive_passes: required_passes,
                    minimum_uptime_seconds,
                    observed_uptime_seconds: started.elapsed().as_secs(),
                    restart_count_initial,
                    restart_count_current,
                    restart_count_stable: false,
                    route_verification_stable: true,
                    validation_succeeded: false,
                    last_probe_error: Some(failure_reason.clone()),
                    unstable_probe_failures,
                    restart_storm_detected: true,
                    oom_detected: inspection.oom_killed,
                };
                persist_lifecycle_transition(
                    lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::CrashLoop,
                    &failure_reason,
                    Some(validation_summary),
                    Some(PersistedPromotionSummary {
                        gate_reason: Some(failure_reason.clone()),
                        runtime_snapshot_persisted: true,
                        convergence_target_stable: true,
                        ..PersistedPromotionSummary::default()
                    }),
                )?;
                let probe_target = internal_port.map(|port| ProbeTargetContext {
                    host: probe_host.clone(),
                    port,
                    path: service.validation.http_health_path.clone(),
                });
                let cleanup = self.cleanup_failed_generation_artifacts(
                    env,
                    generation,
                    &failure_reason,
                    Some(container_name.to_string()),
                    Some(inspection.image_ref.clone()),
                    None,
                )?;
                self.record_failed_attempt(
                    events,
                    diagnostics,
                    record,
                    generation,
                    "warming",
                    &failure_reason,
                    cleanup,
                    probe_target.as_ref(),
                    FailureSummaryDetails {
                        blocking_service_name: Some(service_id.to_string()),
                        blocking_reason: Some(failure_reason.clone()),
                        restart_storm: true,
                        restart_policy: Some(inspection.restart_policy.clone()),
                        restart_count_delta: Some(restart_count_delta),
                        oom_killed: Some(inspection.oom_killed),
                        last_exit_code: termination.last_exit_code,
                        exit_signal: termination.exit_signal,
                        termination_reason: termination.termination_reason,
                    },
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed(
                    "required service entered restart storm",
                ));
            }

            let tcp_ok = if let Some(port) = internal_port {
                let probe_started = Instant::now();
                let ok = self.probes.probe_tcp(&probe_host, port).unwrap_or(false);
                append_probe_history_entry(
                    env,
                    generation,
                    PersistedProbeType::Tcp,
                    ok,
                    probe_started.elapsed().as_millis() as u64,
                    (!ok).then(|| format!("service `{service_id}` tcp probe failed")),
                )?;
                ok
            } else {
                true
            };
            if tcp_ok {
                tcp_passes += 1;
                if let (Some(port), Some(path)) = (
                    internal_port,
                    service.validation.http_health_path.as_deref(),
                ) {
                    let probe_started = Instant::now();
                    let ok = self
                        .probes
                        .probe_http(&probe_host, port, path)
                        .unwrap_or(false);
                    append_probe_history_entry(
                        env,
                        generation,
                        PersistedProbeType::Http,
                        ok,
                        probe_started.elapsed().as_millis() as u64,
                        (!ok).then(|| format!("service `{service_id}` http health probe failed")),
                    )?;
                    if ok {
                        http_passes += 1;
                    } else {
                        http_passes = 0;
                        last_error =
                            Some(format!("service `{service_id}` http health probe failed"));
                    }
                }
            } else {
                tcp_passes = 0;
                http_passes = 0;
                last_error = Some(format!("service `{service_id}` tcp probe failed"));
            }

            if tcp_ok && http_passes >= required_passes {
                unstable_probe_failures = 0;
            } else if !tcp_ok || service.validation.http_health_path.is_some() && http_passes == 0 {
                if prior_probe_success {
                    unstable_probe_failures += 1;
                }
            }

            if tcp_passes >= required_passes && http_passes >= required_passes {
                append_event(events, record, generation, "VALIDATION_PASSED", None)?;
                diagnostics
                    .append_log_line(&format!("service `{service_id}` healthy"), secret_values)?;
                return Ok(ServiceWarmupObservation {
                    service_id: service_id.to_string(),
                    state: PersistedServiceState::Healthy,
                    validation_summary: PersistedValidationSummary {
                        tcp_consecutive_passes: tcp_passes,
                        http_consecutive_passes: http_passes,
                        required_consecutive_passes: required_passes,
                        minimum_uptime_seconds,
                        observed_uptime_seconds: started.elapsed().as_secs(),
                        restart_count_initial,
                        restart_count_current,
                        restart_count_stable: restart_count_current == restart_count_initial,
                        validation_succeeded: true,
                        unstable_probe_failures,
                        ..PersistedValidationSummary::default()
                    },
                });
            }

            if unstable_probe_failures >= PROBE_INSTABILITY_THRESHOLD {
                let failure_reason =
                    format!("service `{service_id}` probe instability exceeded threshold");
                let termination = self.persist_service_runtime_diagnostics(
                    diagnostics,
                    service_id,
                    &inspection,
                    secret_values,
                )?;
                let validation_summary = PersistedValidationSummary {
                    tcp_consecutive_passes: tcp_passes,
                    http_consecutive_passes: http_passes,
                    required_consecutive_passes: required_passes,
                    minimum_uptime_seconds,
                    observed_uptime_seconds: started.elapsed().as_secs(),
                    restart_count_initial,
                    restart_count_current,
                    restart_count_stable: restart_count_current == restart_count_initial,
                    route_verification_stable: true,
                    validation_succeeded: false,
                    last_probe_error: Some(failure_reason.clone()),
                    unstable_probe_failures,
                    restart_storm_detected: false,
                    oom_detected: inspection.oom_killed,
                };
                persist_lifecycle_transition(
                    lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::Unstable,
                    &failure_reason,
                    Some(validation_summary),
                    Some(PersistedPromotionSummary {
                        gate_reason: Some(failure_reason.clone()),
                        runtime_snapshot_persisted: true,
                        convergence_target_stable: true,
                        ..PersistedPromotionSummary::default()
                    }),
                )?;
                let probe_target = internal_port.map(|port| ProbeTargetContext {
                    host: probe_host.clone(),
                    port,
                    path: service.validation.http_health_path.clone(),
                });
                let cleanup = self.cleanup_failed_generation_artifacts(
                    env,
                    generation,
                    &failure_reason,
                    Some(container_name.to_string()),
                    Some(inspection.image_ref.clone()),
                    None,
                )?;
                self.record_failed_attempt(
                    events,
                    diagnostics,
                    record,
                    generation,
                    "warming",
                    &failure_reason,
                    cleanup,
                    probe_target.as_ref(),
                    FailureSummaryDetails {
                        blocking_service_name: Some(service_id.to_string()),
                        blocking_reason: Some(failure_reason.clone()),
                        restart_policy: Some(inspection.restart_policy.clone()),
                        restart_count_delta: Some(restart_count_delta),
                        oom_killed: Some(inspection.oom_killed),
                        last_exit_code: termination.last_exit_code,
                        exit_signal: termination.exit_signal,
                        termination_reason: termination.termination_reason,
                        ..FailureSummaryDetails::default()
                    },
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed(
                    "required service probe instability exceeded threshold",
                ));
            }

            if started.elapsed() >= budget {
                let failure_reason = last_error.unwrap_or_else(|| {
                    format!("service `{service_id}` did not become healthy before timeout")
                });
                let probe_target = internal_port.map(|port| ProbeTargetContext {
                    host: probe_host.clone(),
                    port,
                    path: service.validation.http_health_path.clone(),
                });
                self.capture_service_container_logs_tail(
                    diagnostics,
                    service_id,
                    container_name,
                    secret_values,
                )?;
                let cleanup = self.cleanup_failed_generation_artifacts(
                    env,
                    generation,
                    &failure_reason,
                    Some(container_name.to_string()),
                    Some(inspection.image_ref.clone()),
                    None,
                )?;
                self.record_failed_attempt(
                    events,
                    diagnostics,
                    record,
                    generation,
                    "warming",
                    &failure_reason,
                    cleanup,
                    probe_target.as_ref(),
                    FailureSummaryDetails {
                        blocking_service_name: Some(service_id.to_string()),
                        blocking_reason: Some(failure_reason.clone()),
                        restart_policy: Some(inspection.restart_policy.clone()),
                        restart_count_delta: Some(restart_count_delta),
                        oom_killed: Some(inspection.oom_killed),
                        last_exit_code: inspection.exit_code,
                        exit_signal: inspection.exit_signal,
                        termination_reason: inspection.termination_reason.clone(),
                        ..FailureSummaryDetails::default()
                    },
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed(
                    "required service failed health gating",
                ));
            }

            thread::sleep(Duration::from_millis(WARMUP_LOOP_INTERVAL_MS));
        }
    }

    fn validate_candidate(
        &mut self,
        validation: &ValidationPolicy,
        validation_timeout_ms: u64,
        env: &EnvironmentPaths,
        lifecycle_store: &LifecycleStore,
        container_name: &str,
        image_ref: &str,
        events: &EventStore,
        diagnostics: &DiagnosticsStore,
        record: &DeploymentRecord,
        generation: u64,
        redacted_env_preview: &[String],
        secret_values: &[String],
    ) -> Result<WarmupObservation, DeploymentError> {
        let validation_started = Instant::now();
        let internal_port = match validation.activation {
            ActivationMode::Direct => 3000,
            ActivationMode::Http { internal_port } => internal_port,
        };
        let probe_path = validation.http_health_path.as_deref();
        let selected_network = self.execution.network_name.clone();
        persist_lifecycle_transition(
            lifecycle_store,
            &record.project_id,
            &record.environment,
            generation,
            DeploymentLifecycleState::Warming,
            "warmup started",
            None,
            None,
        )?;
        let inspection = match self.docker.inspect_container(container_name) {
            Ok(inspection) => inspection,
            Err(err) => {
                let failure_reason =
                    format!("container re-inspection failed before tcp probe: {err}");
                self.record_failed_generation(
                    env,
                    events,
                    diagnostics,
                    record,
                    generation,
                    container_name,
                    Some(image_ref),
                    None,
                    "validation",
                    &failure_reason,
                    None,
                    FailureSummaryDetails::default(),
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(err.into());
            }
        };
        let required_passes = validation.required_consecutive_probe_passes.max(1);
        let minimum_uptime_seconds = validation.minimum_uptime_seconds;
        let restart_count_initial = inspection.restart_count;
        if validation.tcp_required && !inspection.running {
            let failure_reason = container_exited_failure_reason("tcp probe", &inspection);
            let validation_summary = PersistedValidationSummary {
                required_consecutive_passes: required_passes,
                minimum_uptime_seconds,
                restart_count_initial,
                restart_count_current: inspection.restart_count,
                restart_count_stable: true,
                route_verification_stable: true,
                last_probe_error: Some(failure_reason.clone()),
                ..PersistedValidationSummary::default()
            };
            let promotion_summary = PersistedPromotionSummary {
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                gate_reason: Some(failure_reason.clone()),
                ..PersistedPromotionSummary::default()
            };
            let context = ValidationFailureContext {
                inspection: Some(inspection.clone()),
                probe_target: None,
                attempts: None,
                elapsed_ms: Some(validation_started.elapsed().as_millis()),
                last_error: Some(failure_reason.clone()),
            };
            self.capture_validation_failure_diagnostics(
                diagnostics,
                container_name,
                &context,
                internal_port,
                probe_path,
                selected_network.as_deref(),
                secret_values,
            )?;
            persist_lifecycle_transition(
                lifecycle_store,
                &record.project_id,
                &record.environment,
                generation,
                if inspection.oom_killed {
                    DeploymentLifecycleState::OomKilled
                } else {
                    DeploymentLifecycleState::Failed
                },
                &failure_reason,
                Some(validation_summary),
                Some(promotion_summary),
            )?;
            self.record_failed_generation(
                env,
                events,
                diagnostics,
                record,
                generation,
                container_name,
                Some(image_ref),
                None,
                "validation",
                &failure_reason,
                None,
                FailureSummaryDetails {
                    blocking_reason: Some(failure_reason.clone()),
                    restart_policy: Some(inspection.restart_policy.clone()),
                    restart_count_delta: Some(0),
                    oom_killed: Some(inspection.oom_killed),
                    last_exit_code: inspection.exit_code,
                    exit_signal: inspection.exit_signal,
                    termination_reason: inspection.termination_reason.clone(),
                    ..FailureSummaryDetails::default()
                },
                redacted_env_preview,
                secret_values,
            )?;
            return Err(DeploymentError::ValidationFailed(
                "container exited before tcp probe",
            ));
        }
        let probe_host = match resolve_selected_network_host(
            &inspection,
            self.execution.network_name.as_deref(),
        ) {
            Ok(probe_host) => probe_host,
            Err(err) => {
                let failure_reason = err.to_string();
                let validation_summary = PersistedValidationSummary {
                    required_consecutive_passes: required_passes,
                    minimum_uptime_seconds,
                    restart_count_initial,
                    restart_count_current: inspection.restart_count,
                    restart_count_stable: true,
                    route_verification_stable: true,
                    last_probe_error: Some(failure_reason.clone()),
                    ..PersistedValidationSummary::default()
                };
                let promotion_summary = PersistedPromotionSummary {
                    runtime_snapshot_persisted: true,
                    convergence_target_stable: true,
                    gate_reason: Some(failure_reason.clone()),
                    ..PersistedPromotionSummary::default()
                };
                let context = ValidationFailureContext {
                    inspection: Some(inspection.clone()),
                    probe_target: None,
                    attempts: None,
                    elapsed_ms: Some(validation_started.elapsed().as_millis()),
                    last_error: Some(failure_reason.clone()),
                };
                self.capture_validation_failure_diagnostics(
                    diagnostics,
                    container_name,
                    &context,
                    internal_port,
                    probe_path,
                    selected_network.as_deref(),
                    secret_values,
                )?;
                persist_lifecycle_transition(
                    lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::Failed,
                    &failure_reason,
                    Some(validation_summary),
                    Some(promotion_summary),
                )?;
                self.record_failed_generation(
                    env,
                    events,
                    diagnostics,
                    record,
                    generation,
                    container_name,
                    Some(image_ref),
                    None,
                    "validation",
                    &failure_reason,
                    None,
                    FailureSummaryDetails::default(),
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(err);
            }
        };
        let tcp_probe_target = ProbeTargetContext {
            host: probe_host.clone(),
            port: internal_port,
            path: None,
        };
        if let Some(note) = bridge_reachability_diagnostic(
            &inspection,
            selected_network.as_deref(),
            Some(&tcp_probe_target),
        ) {
            diagnostics.append_log_line(&note, secret_values)?;
        }
        let mut tcp_consecutive_passes = 0u32;
        let mut http_consecutive_passes = if probe_path.is_none() {
            required_passes
        } else {
            0
        };
        let mut attempts = 0u32;
        let mut last_probe_error = None;
        let budget = Duration::from_millis(validation_timeout_ms);
        let inspect_each_attempt = minimum_uptime_seconds > 0 || required_passes > 1;
        let mut unstable_probe_failures = 0u32;

        loop {
            attempts += 1;
            let mut current_inspection = if attempts == 1 || !inspect_each_attempt {
                inspection.clone()
            } else {
                self.docker.inspect_container(container_name)?
            };
            let prior_probe_success = if probe_path.is_some() {
                http_consecutive_passes > 0
            } else {
                tcp_consecutive_passes > 0
            };
            if !current_inspection.running {
                let restart_delta = restart_count_delta(restart_count_initial, &current_inspection);
                let restart_storm =
                    restart_storm_detected(restart_count_initial, &current_inspection);
                let lifecycle_state = if current_inspection.oom_killed {
                    DeploymentLifecycleState::OomKilled
                } else if restart_storm {
                    DeploymentLifecycleState::CrashLoop
                } else {
                    DeploymentLifecycleState::Failed
                };
                let failure_reason = if current_inspection.oom_killed {
                    "container OOMKilled during warmup".to_string()
                } else {
                    container_exited_failure_reason("warmup", &current_inspection)
                };
                let validation_summary = PersistedValidationSummary {
                    tcp_consecutive_passes,
                    http_consecutive_passes,
                    required_consecutive_passes: required_passes,
                    minimum_uptime_seconds,
                    observed_uptime_seconds: validation_started.elapsed().as_secs(),
                    restart_count_initial,
                    restart_count_current: current_inspection.restart_count,
                    restart_count_stable: restart_delta == 0,
                    route_verification_stable: true,
                    validation_succeeded: false,
                    last_probe_error: Some(failure_reason.clone()),
                    oom_detected: current_inspection.oom_killed,
                    restart_storm_detected: restart_storm,
                    unstable_probe_failures,
                };
                let promotion_summary = PersistedPromotionSummary {
                    runtime_snapshot_persisted: true,
                    convergence_target_stable: true,
                    gate_reason: Some(failure_reason.clone()),
                    ..PersistedPromotionSummary::default()
                };
                persist_lifecycle_transition(
                    lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    lifecycle_state,
                    &failure_reason,
                    Some(validation_summary.clone()),
                    Some(promotion_summary.clone()),
                )?;
                let context = ValidationFailureContext {
                    inspection: Some(current_inspection.clone()),
                    probe_target: Some(tcp_probe_target.clone()),
                    attempts: Some(attempts),
                    elapsed_ms: Some(validation_started.elapsed().as_millis()),
                    last_error: Some(failure_reason.clone()),
                };
                self.capture_validation_failure_diagnostics(
                    diagnostics,
                    container_name,
                    &context,
                    internal_port,
                    probe_path,
                    selected_network.as_deref(),
                    secret_values,
                )?;
                self.record_failed_generation(
                    env,
                    events,
                    diagnostics,
                    record,
                    generation,
                    container_name,
                    Some(image_ref),
                    None,
                    "warming",
                    &failure_reason,
                    Some(&tcp_probe_target),
                    FailureSummaryDetails {
                        blocking_reason: Some(failure_reason.clone()),
                        restart_storm,
                        restart_policy: Some(current_inspection.restart_policy.clone()),
                        restart_count_delta: Some(restart_delta),
                        oom_killed: Some(current_inspection.oom_killed),
                        last_exit_code: current_inspection.exit_code,
                        exit_signal: current_inspection.exit_signal,
                        termination_reason: current_inspection.termination_reason.clone(),
                        ..FailureSummaryDetails::default()
                    },
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed(
                    "container exited before tcp probe",
                ));
            }

            if restart_storm_detected(restart_count_initial, &current_inspection) {
                let restart_delta = restart_count_delta(restart_count_initial, &current_inspection);
                let failure_reason = format!(
                    "restart storm detected during warmup ({} -> {}, delta={restart_delta})",
                    restart_count_initial, current_inspection.restart_count
                );
                let validation_summary = PersistedValidationSummary {
                    tcp_consecutive_passes,
                    http_consecutive_passes,
                    required_consecutive_passes: required_passes,
                    minimum_uptime_seconds,
                    observed_uptime_seconds: validation_started.elapsed().as_secs(),
                    restart_count_initial,
                    restart_count_current: current_inspection.restart_count,
                    restart_count_stable: false,
                    route_verification_stable: true,
                    validation_succeeded: false,
                    last_probe_error: Some(failure_reason.clone()),
                    oom_detected: current_inspection.oom_killed,
                    restart_storm_detected: true,
                    unstable_probe_failures,
                };
                let promotion_summary = PersistedPromotionSummary {
                    runtime_snapshot_persisted: true,
                    convergence_target_stable: true,
                    gate_reason: Some(failure_reason.clone()),
                    ..PersistedPromotionSummary::default()
                };
                update_lifecycle_progress(
                    lifecycle_store,
                    Some(validation_summary.clone()),
                    Some(promotion_summary.clone()),
                )?;
                persist_lifecycle_transition(
                    lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::CrashLoop,
                    &failure_reason,
                    Some(validation_summary),
                    Some(promotion_summary),
                )?;
                self.record_failed_generation(
                    env,
                    events,
                    diagnostics,
                    record,
                    generation,
                    container_name,
                    Some(image_ref),
                    None,
                    "warming",
                    &failure_reason,
                    Some(&tcp_probe_target),
                    FailureSummaryDetails {
                        blocking_reason: Some(failure_reason.clone()),
                        restart_storm: true,
                        restart_policy: Some(current_inspection.restart_policy.clone()),
                        restart_count_delta: Some(restart_delta),
                        oom_killed: Some(current_inspection.oom_killed),
                        last_exit_code: current_inspection.exit_code,
                        exit_signal: current_inspection.exit_signal,
                        termination_reason: current_inspection.termination_reason.clone(),
                        ..FailureSummaryDetails::default()
                    },
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed(
                    "restart instability prevented promotion",
                ));
            }

            let tcp_ok = if validation.tcp_required {
                let tcp_started = Instant::now();
                match self.probes.probe_tcp(&probe_host, internal_port) {
                    Ok(true) => {
                        append_probe_history_entry(
                            env,
                            generation,
                            PersistedProbeType::Tcp,
                            true,
                            tcp_started.elapsed().as_millis() as u64,
                            None,
                        )?;
                        tcp_consecutive_passes += 1;
                        true
                    }
                    Ok(false) => {
                        let failure_reason = "tcp probe returned unhealthy".to_string();
                        append_probe_history_entry(
                            env,
                            generation,
                            PersistedProbeType::Tcp,
                            false,
                            tcp_started.elapsed().as_millis() as u64,
                            Some(failure_reason.clone()),
                        )?;
                        tcp_consecutive_passes = 0;
                        last_probe_error = Some(failure_reason);
                        false
                    }
                    Err(err) => {
                        let failure_reason = err.to_string();
                        append_probe_history_entry(
                            env,
                            generation,
                            PersistedProbeType::Tcp,
                            false,
                            tcp_started.elapsed().as_millis() as u64,
                            Some(failure_reason.clone()),
                        )?;
                        tcp_consecutive_passes = 0;
                        last_probe_error = Some(failure_reason);
                        false
                    }
                }
            } else {
                true
            };
            if !tcp_ok && !inspect_each_attempt {
                if let Ok(reinspected) = self.docker.inspect_container(container_name) {
                    current_inspection = reinspected;
                    if !current_inspection.running {
                        last_probe_error = Some(container_exited_failure_reason(
                            "tcp probe",
                            &current_inspection,
                        ));
                    }
                }
            }
            if !tcp_ok {
                http_consecutive_passes = if probe_path.is_none() {
                    required_passes
                } else {
                    0
                };
                if prior_probe_success {
                    unstable_probe_failures += 1;
                }
            } else if let Some(path) = probe_path {
                let http_started = Instant::now();
                match self.probes.probe_http(&probe_host, internal_port, path) {
                    Ok(true) => {
                        append_probe_history_entry(
                            env,
                            generation,
                            PersistedProbeType::Http,
                            true,
                            http_started.elapsed().as_millis() as u64,
                            None,
                        )?;
                        http_consecutive_passes += 1;
                        unstable_probe_failures = 0;
                    }
                    Ok(false) => {
                        let failure_reason =
                            format!("http health probe returned unhealthy for {path}");
                        append_probe_history_entry(
                            env,
                            generation,
                            PersistedProbeType::Http,
                            false,
                            http_started.elapsed().as_millis() as u64,
                            Some(failure_reason.clone()),
                        )?;
                        http_consecutive_passes = 0;
                        last_probe_error = Some(failure_reason);
                        if prior_probe_success {
                            unstable_probe_failures += 1;
                        }
                    }
                    Err(err) => {
                        let failure_reason = err.to_string();
                        append_probe_history_entry(
                            env,
                            generation,
                            PersistedProbeType::Http,
                            false,
                            http_started.elapsed().as_millis() as u64,
                            Some(failure_reason.clone()),
                        )?;
                        http_consecutive_passes = 0;
                        last_probe_error = Some(failure_reason);
                        if prior_probe_success {
                            unstable_probe_failures += 1;
                        }
                    }
                }
            } else {
                unstable_probe_failures = 0;
            }

            let validation_summary = PersistedValidationSummary {
                tcp_consecutive_passes,
                http_consecutive_passes,
                required_consecutive_passes: required_passes,
                minimum_uptime_seconds,
                observed_uptime_seconds: validation_started.elapsed().as_secs(),
                restart_count_initial,
                restart_count_current: current_inspection.restart_count,
                restart_count_stable: true,
                route_verification_stable: true,
                validation_succeeded: false,
                last_probe_error: last_probe_error.clone(),
                oom_detected: current_inspection.oom_killed,
                restart_storm_detected: false,
                unstable_probe_failures,
            };
            let promotion_summary = PersistedPromotionSummary {
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                gate_reason: Some(format!(
                    "warmup pending: uptime={}s probes tcp={}/{} http={}/{}",
                    validation_summary.observed_uptime_seconds,
                    validation_summary.tcp_consecutive_passes,
                    validation_summary.required_consecutive_passes,
                    validation_summary.http_consecutive_passes,
                    validation_summary.required_consecutive_passes
                )),
                ..PersistedPromotionSummary::default()
            };
            update_lifecycle_progress(
                lifecycle_store,
                Some(validation_summary.clone()),
                Some(promotion_summary.clone()),
            )?;

            let uptime_ready = validation_summary.observed_uptime_seconds >= minimum_uptime_seconds;
            let tcp_ready = !validation.tcp_required
                || validation_summary.tcp_consecutive_passes >= required_passes;
            let http_ready = probe_path.is_none()
                || validation_summary.http_consecutive_passes >= required_passes;
            if uptime_ready && tcp_ready && http_ready {
                diagnostics.append_log_line("warmup stability checks passed", secret_values)?;
                append_event(events, record, generation, "VALIDATION_PASSED", None)?;
                return Ok(WarmupObservation {
                    validation_summary: PersistedValidationSummary {
                        validation_succeeded: true,
                        last_probe_error: None,
                        ..validation_summary
                    },
                    promotion_summary: PersistedPromotionSummary {
                        warmup_succeeded: true,
                        validation_succeeded: true,
                        runtime_snapshot_persisted: true,
                        convergence_target_stable: true,
                        gate_reason: None,
                        ..PersistedPromotionSummary::default()
                    },
                });
            }

            if unstable_probe_failures >= PROBE_INSTABILITY_THRESHOLD {
                let failure_reason =
                    "probe instability exceeded threshold during warmup".to_string();
                let validation_summary = PersistedValidationSummary {
                    validation_succeeded: false,
                    last_probe_error: Some(
                        last_probe_error
                            .clone()
                            .unwrap_or_else(|| failure_reason.clone()),
                    ),
                    unstable_probe_failures,
                    ..validation_summary
                };
                let promotion_summary = PersistedPromotionSummary {
                    runtime_snapshot_persisted: true,
                    convergence_target_stable: true,
                    gate_reason: Some(failure_reason.clone()),
                    ..PersistedPromotionSummary::default()
                };
                persist_lifecycle_transition(
                    lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::Unstable,
                    &failure_reason,
                    Some(validation_summary.clone()),
                    Some(promotion_summary.clone()),
                )?;
                self.record_failed_generation(
                    env,
                    events,
                    diagnostics,
                    record,
                    generation,
                    container_name,
                    Some(image_ref),
                    None,
                    "warming",
                    &failure_reason,
                    Some(&tcp_probe_target),
                    FailureSummaryDetails {
                        blocking_reason: Some(failure_reason.clone()),
                        restart_policy: Some(current_inspection.restart_policy.clone()),
                        restart_count_delta: Some(restart_count_delta(
                            restart_count_initial,
                            &current_inspection,
                        )),
                        oom_killed: Some(current_inspection.oom_killed),
                        last_exit_code: current_inspection.exit_code,
                        exit_signal: current_inspection.exit_signal,
                        termination_reason: current_inspection.termination_reason.clone(),
                        ..FailureSummaryDetails::default()
                    },
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed(
                    "probe instability exceeded threshold",
                ));
            }

            if validation_started.elapsed() >= budget {
                let observed_failure_reason = last_probe_error
                    .clone()
                    .unwrap_or_else(|| "warmup stability window not reached".into());
                let failure_reason = match observed_failure_reason.as_str() {
                    reason if reason.starts_with("http health probe returned unhealthy") => {
                        "http health probe failed".to_string()
                    }
                    reason if reason.starts_with("tcp probe returned unhealthy") => {
                        "tcp probe failed".to_string()
                    }
                    _ => observed_failure_reason.clone(),
                };
                let validation_summary = PersistedValidationSummary {
                    validation_succeeded: false,
                    last_probe_error: Some(observed_failure_reason.clone()),
                    ..validation_summary
                };
                let promotion_summary = PersistedPromotionSummary {
                    runtime_snapshot_persisted: true,
                    convergence_target_stable: true,
                    gate_reason: Some(failure_reason.clone()),
                    ..PersistedPromotionSummary::default()
                };
                persist_lifecycle_transition(
                    lifecycle_store,
                    &record.project_id,
                    &record.environment,
                    generation,
                    DeploymentLifecycleState::Failed,
                    &failure_reason,
                    Some(validation_summary.clone()),
                    Some(promotion_summary.clone()),
                )?;
                let context = ValidationFailureContext {
                    inspection: Some(current_inspection.clone()),
                    probe_target: Some(ProbeTargetContext {
                        host: probe_host.clone(),
                        port: internal_port,
                        path: probe_path.map(|value| value.to_string()),
                    }),
                    attempts: Some(attempts),
                    elapsed_ms: Some(validation_started.elapsed().as_millis()),
                    last_error: Some(observed_failure_reason.clone()),
                };
                self.capture_validation_failure_diagnostics(
                    diagnostics,
                    container_name,
                    &context,
                    internal_port,
                    probe_path,
                    selected_network.as_deref(),
                    secret_values,
                )?;
                self.record_failed_generation(
                    env,
                    events,
                    diagnostics,
                    record,
                    generation,
                    container_name,
                    Some(image_ref),
                    None,
                    "warming",
                    &failure_reason,
                    context.probe_target.as_ref(),
                    FailureSummaryDetails {
                        blocking_reason: Some(failure_reason.clone()),
                        restart_policy: Some(current_inspection.restart_policy.clone()),
                        restart_count_delta: Some(restart_count_delta(
                            restart_count_initial,
                            &current_inspection,
                        )),
                        oom_killed: Some(current_inspection.oom_killed),
                        last_exit_code: current_inspection.exit_code,
                        exit_signal: current_inspection.exit_signal,
                        termination_reason: current_inspection.termination_reason.clone(),
                        ..FailureSummaryDetails::default()
                    },
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(match failure_reason.as_str() {
                    "http health probe failed" => {
                        DeploymentError::ValidationFailed("http health probe failed")
                    }
                    "tcp probe failed" => DeploymentError::ValidationFailed("tcp probe failed"),
                    reason if reason.starts_with("container exited before") => {
                        DeploymentError::ValidationFailed("container exited before tcp probe")
                    }
                    _ => DeploymentError::ValidationFailed("warmup stability window not reached"),
                });
            }

            thread::sleep(Duration::from_millis(WARMUP_LOOP_INTERVAL_MS));
        }
    }

    fn record_failed_attempt(
        &self,
        events: &EventStore,
        diagnostics: &DiagnosticsStore,
        record: &DeploymentRecord,
        generation: u64,
        failure_stage: &str,
        failure_reason: &str,
        cleanup: CleanupRecord,
        probe_target: Option<&ProbeTargetContext>,
        summary_details: FailureSummaryDetails,
        redacted_env_preview: &[String],
        secret_values: &[String],
    ) -> Result<(), DeploymentError> {
        diagnostics.write_failure_reason(failure_reason, secret_values)?;
        diagnostics.append_log_line(failure_reason, secret_values)?;
        if let Some(probe_target) = probe_target {
            diagnostics.append_log_line(&format_probe_target_log(probe_target), secret_values)?;
        }
        append_redacted_event(
            events,
            record,
            generation,
            "GENERATION_FAILED",
            Some(failure_reason.into()),
            secret_values,
        )?;
        let env =
            EnvironmentPaths::new(&self.storage_root, &record.project_id, &record.environment);
        CleanupStore::new(env, generation).write_record(&cleanup)?;
        diagnostics.write_summary(&DiagnosticSummary {
            deployment_id: Some(record.deployment_id.clone()),
            failure_stage: failure_stage.into(),
            failure_reason: failure_reason.into(),
            blocking_reason: summary_details
                .blocking_reason
                .clone()
                .or_else(|| Some(failure_reason.into())),
            container_name: String::new(),
            failed_service_name: summary_details.blocking_service_name.clone(),
            blocking_service_name: summary_details.blocking_service_name,
            probe_target_host: probe_target.map(|target| target.host.clone()),
            probe_target_port: probe_target.map(|target| target.port),
            probe_target_path: probe_target.and_then(|target| target.path.clone()),
            restart_storm: summary_details.restart_storm,
            restart_policy: summary_details.restart_policy,
            restart_count_delta: summary_details.restart_count_delta,
            oom_killed: summary_details.oom_killed,
            last_exit_code: summary_details.last_exit_code,
            exit_signal: summary_details.exit_signal,
            termination_reason: summary_details.termination_reason,
            cleanup_recorded: true,
            dependency_graph_summary: None,
            runtime_env_preview: redacted_env_preview.to_vec(),
        })?;
        finalize_failed_generation(&self.storage_root, record, generation, diagnostics)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record_preparation_failure(
        &self,
        record: &DeploymentRecord,
        artifacts: &DeploymentArtifacts,
        failure_stage: &str,
        failure_reason: &str,
        container_name: String,
        failed_service_name: Option<String>,
        dependency_graph_summary: Option<String>,
        summary_details: FailureSummaryDetails,
        redacted_env_preview: &[String],
        secret_values: &[String],
        cleanup: CleanupRecord,
    ) -> Result<(), DeploymentError> {
        persist_lifecycle_transition(
            &artifacts.lifecycle_store,
            &record.project_id,
            &record.environment,
            artifacts.generation,
            DeploymentLifecycleState::Failed,
            failure_reason,
            None,
            Some(PersistedPromotionSummary {
                gate_reason: Some(failure_reason.into()),
                ..PersistedPromotionSummary::default()
            }),
        )?;
        artifacts
            .diagnostics
            .write_failure_reason(failure_reason, secret_values)?;
        artifacts
            .diagnostics
            .append_log_line(failure_reason, secret_values)?;
        if let Some(service_id) = failed_service_name.as_deref() {
            artifacts
                .diagnostics
                .append_log_line(&format!("failed service: {service_id}"), secret_values)?;
        }
        if let Some(summary) = dependency_graph_summary.as_deref() {
            artifacts
                .diagnostics
                .append_log_line(&format!("dependency graph: {summary}"), secret_values)?;
        }
        append_redacted_event(
            &artifacts.events,
            record,
            artifacts.generation,
            "GENERATION_FAILED",
            Some(failure_reason.into()),
            secret_values,
        )?;
        CleanupStore::new(artifacts.env.clone(), artifacts.generation).write_record(&cleanup)?;
        artifacts.diagnostics.write_summary(&DiagnosticSummary {
            deployment_id: Some(record.deployment_id.clone()),
            failure_stage: failure_stage.into(),
            failure_reason: failure_reason.into(),
            blocking_reason: summary_details
                .blocking_reason
                .clone()
                .or_else(|| Some(failure_reason.into())),
            container_name,
            failed_service_name: failed_service_name
                .clone()
                .or_else(|| summary_details.blocking_service_name.clone()),
            blocking_service_name: summary_details.blocking_service_name,
            probe_target_host: None,
            probe_target_port: None,
            probe_target_path: None,
            restart_storm: summary_details.restart_storm,
            restart_policy: summary_details.restart_policy,
            restart_count_delta: summary_details.restart_count_delta,
            oom_killed: summary_details.oom_killed,
            last_exit_code: summary_details.last_exit_code,
            exit_signal: summary_details.exit_signal,
            termination_reason: summary_details.termination_reason,
            cleanup_recorded: true,
            dependency_graph_summary,
            runtime_env_preview: redacted_env_preview.to_vec(),
        })?;
        finalize_failed_generation(
            &self.storage_root,
            record,
            artifacts.generation,
            &artifacts.diagnostics,
        )?;
        Ok(())
    }

    fn capture_validation_failure_diagnostics(
        &mut self,
        diagnostics: &DiagnosticsStore,
        container_name: &str,
        context: &ValidationFailureContext,
        internal_port: u16,
        probe_path: Option<&str>,
        selected_network: Option<&str>,
        secret_values: &[String],
    ) -> Result<(), DeploymentError> {
        if let Some(inspection) = &context.inspection {
            diagnostics.append_log_line(
                &format!(
                    "container state: status={} running={} exit_code={}",
                    inspection.state_status,
                    inspection.running,
                    inspection
                        .exit_code
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unknown".into())
                ),
                secret_values,
            )?;
            diagnostics.append_log_line(
                &format!(
                    "network map: {}",
                    format_network_map(&inspection.network_ips)
                ),
                secret_values,
            )?;
            if let Some(note) = bridge_reachability_diagnostic(
                inspection,
                selected_network,
                context.probe_target.as_ref(),
            ) {
                diagnostics.append_log_line(&note, secret_values)?;
            }
        }
        if let Some(attempts) = context.attempts {
            diagnostics.append_log_line(
                &format!(
                    "validation attempts: attempts={} elapsed_ms={} last_error={}",
                    attempts,
                    context.elapsed_ms.unwrap_or_default(),
                    context.last_error.as_deref().unwrap_or("unknown")
                ),
                secret_values,
            )?;
        }

        let logs_tail = self.read_container_logs_tail(container_name);
        diagnostics.write_artifact("container_logs_tail.log", &logs_tail, secret_values)?;
        if !logs_tail.trim().is_empty() {
            diagnostics.append_log_line("container logs tail:", secret_values)?;
            diagnostics.append_log_line(&logs_tail, secret_values)?;
        }

        let artifact = serde_json::json!({
            "container_name": container_name,
            "probe_target": {
                "host": context.probe_target.as_ref().map(|target| target.host.clone()),
                "port": context.probe_target.as_ref().map(|target| target.port).unwrap_or(internal_port),
                "path": context
                    .probe_target
                    .as_ref()
                    .and_then(|target| target.path.clone())
                    .or_else(|| probe_path.map(|value| value.to_string())),
            },
            "attempts": context.attempts,
            "elapsed_ms": context.elapsed_ms,
            "last_error": context.last_error,
            "inspect_state": context.inspection.as_ref().map(|inspection| {
                serde_json::json!({
                    "status": inspection.state_status,
                    "running": inspection.running,
                    "exit_code": inspection.exit_code,
                    "restart_policy": inspection.restart_policy,
                    "image_ref": inspection.image_ref,
                })
            }),
            "network_map": context
                .inspection
                .as_ref()
                .map(|inspection| inspection.network_ips.clone())
                .unwrap_or_default(),
            "container_logs_tail": logs_tail,
        });
        let artifact = serde_json::to_string_pretty(&artifact).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        diagnostics.write_artifact(
            "validation_failure.json",
            &format!("{artifact}\n"),
            secret_values,
        )?;
        Ok(())
    }

    fn capture_route_activation_failure_diagnostics(
        &mut self,
        diagnostics: &DiagnosticsStore,
        inspection: &RouteInspection,
        context: &RouteActivationContext,
        secret_values: &[String],
    ) -> Result<(), DeploymentError> {
        diagnostics.append_log_line(&format!("route id: {}", context.route_id), secret_values)?;
        if let Some(domain) = context.domain.as_deref() {
            diagnostics.append_log_line(&format!("route domain: {domain}"), secret_values)?;
        }
        diagnostics.append_log_line(
            &format!("route upstream target: {}", context.upstream_target),
            secret_values,
        )?;
        if let Some(url) = context.verification_url.as_deref() {
            diagnostics
                .append_log_line(&format!("route verification url: {url}"), secret_values)?;
        }
        if let Some(host) = context.verification_host.as_deref() {
            diagnostics
                .append_log_line(&format!("route verification host: {host}"), secret_values)?;
        }
        if let Some(status_code) = context.verification_status_code {
            diagnostics.append_log_line(
                &format!("route verification status: {status_code}"),
                secret_values,
            )?;
        }
        if let Some(body) = context.verification_response_body.as_deref() {
            diagnostics
                .append_log_line(&format!("route verification body: {body}"), secret_values)?;
        }
        if let Some(note) = caddy_network_reachability_note(context.network_name.as_deref()) {
            diagnostics.append_log_line(&note, secret_values)?;
        }

        let artifact = serde_json::json!({
            "route_id": context.route_id,
            "domain": context.domain,
            "upstream_target": context.upstream_target,
            "active_target": inspection.active_target,
            "verification_url": context.verification_url,
            "verification_host": context.verification_host,
            "verification_status_code": context.verification_status_code,
            "verification_response_body": context.verification_response_body,
            "activation_verified": inspection.activation_verified,
            "health_checks_enabled": inspection.health_checks_enabled,
            "network_name": context.network_name,
            "network_reachability_note": caddy_network_reachability_note(
                context.network_name.as_deref()
            ),
        });
        let artifact = serde_json::to_string_pretty(&artifact).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        diagnostics.write_artifact(
            "route_activation_failure.json",
            &format!("{artifact}\n"),
            secret_values,
        )?;
        Ok(())
    }

    fn capture_container_logs_tail(
        &mut self,
        diagnostics: &DiagnosticsStore,
        container_name: &str,
        secret_values: &[String],
    ) -> Result<(), DeploymentError> {
        let logs_tail = self.read_container_logs_tail(container_name);
        diagnostics.write_artifact("container_logs_tail.log", &logs_tail, secret_values)?;
        Ok(())
    }

    fn capture_service_container_logs_tail(
        &mut self,
        diagnostics: &DiagnosticsStore,
        service_id: &str,
        container_name: &str,
        secret_values: &[String],
    ) -> Result<(), DeploymentError> {
        let logs_tail = self.read_container_logs_tail(container_name);
        diagnostics.write_artifact(
            &format!("services/{service_id}/container_logs_tail.log"),
            &logs_tail,
            secret_values,
        )?;
        diagnostics.write_artifact(
            &format!("service-{service_id}-container_logs_tail.log"),
            &logs_tail,
            secret_values,
        )?;
        Ok(())
    }

    fn persist_service_runtime_diagnostics(
        &mut self,
        diagnostics: &DiagnosticsStore,
        service_id: &str,
        inspection: &ContainerInspection,
        secret_values: &[String],
    ) -> Result<PersistedTerminationInfo, DeploymentError> {
        let logs_tail = self.read_container_logs_tail(&inspection.container_name);
        diagnostics.write_artifact(
            &format!("services/{service_id}/logs_tail.log"),
            &logs_tail,
            secret_values,
        )?;
        let termination = inspection_termination_info(inspection, Some(logs_tail.clone()));
        let payload = serde_json::to_string_pretty(&termination).map_err(json_storage_error)?;
        diagnostics.write_artifact(
            &format!("services/{service_id}/termination.json"),
            &format!("{payload}\n"),
            secret_values,
        )?;
        Ok(termination)
    }

    fn read_container_logs_tail(&mut self, container_name: &str) -> String {
        for _ in 0..5 {
            let logs_tail = self
                .docker
                .container_logs(container_name, 50)
                .unwrap_or_else(|err| format!("container logs unavailable: {err}"));
            if !looks_like_inspect_output(&logs_tail) {
                return logs_tail;
            }
        }
        "container logs unavailable: inspect output repeated".into()
    }

    #[allow(clippy::too_many_arguments)]
    fn record_failed_generation(
        &mut self,
        env: &EnvironmentPaths,
        events: &EventStore,
        diagnostics: &DiagnosticsStore,
        record: &DeploymentRecord,
        generation: u64,
        container_name: &str,
        image_ref: Option<&str>,
        route_subtree_id: Option<String>,
        failure_stage: &str,
        failure_reason: &str,
        probe_target: Option<&ProbeTargetContext>,
        summary_details: FailureSummaryDetails,
        redacted_env_preview: &[String],
        secret_values: &[String],
    ) -> Result<(), DeploymentError> {
        diagnostics.write_failure_reason(failure_reason, secret_values)?;
        diagnostics.append_log_line(failure_reason, secret_values)?;
        if let Some(probe_target) = probe_target {
            diagnostics.append_log_line(&format_probe_target_log(probe_target), secret_values)?;
        }
        append_redacted_event(
            events,
            record,
            generation,
            match failure_reason {
                reason if reason.starts_with("tcp probe failed") => "TCP_PROBE_FAILED",
                reason if reason.starts_with("http health probe failed") => "HTTP_PROBE_FAILED",
                _ => "GENERATION_FAILED",
            },
            Some(failure_reason.into()),
            secret_values,
        )?;
        let cleanup = self.cleanup_failed_generation(
            env,
            generation,
            container_name,
            image_ref,
            route_subtree_id.clone(),
            failure_reason,
        )?;
        diagnostics.write_summary(&DiagnosticSummary {
            deployment_id: Some(record.deployment_id.clone()),
            failure_stage: failure_stage.into(),
            failure_reason: failure_reason.into(),
            blocking_reason: summary_details
                .blocking_reason
                .clone()
                .or_else(|| Some(failure_reason.into())),
            container_name: container_name.into(),
            failed_service_name: summary_details.blocking_service_name.clone(),
            blocking_service_name: summary_details.blocking_service_name,
            probe_target_host: probe_target.map(|target| target.host.clone()),
            probe_target_port: probe_target.map(|target| target.port),
            probe_target_path: probe_target.and_then(|target| target.path.clone()),
            restart_storm: summary_details.restart_storm,
            restart_policy: summary_details.restart_policy,
            restart_count_delta: summary_details.restart_count_delta,
            oom_killed: summary_details.oom_killed,
            last_exit_code: summary_details.last_exit_code,
            exit_signal: summary_details.exit_signal,
            termination_reason: summary_details.termination_reason,
            cleanup_recorded: true,
            dependency_graph_summary: None,
            runtime_env_preview: redacted_env_preview.to_vec(),
        })?;
        finalize_failed_generation(&self.storage_root, record, generation, diagnostics)?;
        append_redacted_event(
            events,
            record,
            generation,
            if cleanup.tombstoned {
                "FAILED_GENERATION_TOMBSTONED"
            } else {
                "FAILED_GENERATION_CLEANED"
            },
            Some(failure_reason.into()),
            secret_values,
        )?;
        Ok(())
    }

    fn cleanup_failed_generation(
        &mut self,
        env: &EnvironmentPaths,
        generation: u64,
        container_name: &str,
        image_ref: Option<&str>,
        route_subtree_id: Option<String>,
        failure_reason: &str,
    ) -> Result<CleanupRecord, DeploymentError> {
        self.cleanup_failed_generation_artifacts(
            env,
            generation,
            failure_reason,
            Some(container_name.into()),
            image_ref.map(str::to_string),
            route_subtree_id,
        )
    }

    fn cleanup_failed_generation_artifacts(
        &mut self,
        env: &EnvironmentPaths,
        generation: u64,
        generation_failure_reason: &str,
        container_name: Option<String>,
        image_ref: Option<String>,
        route_subtree_id: Option<String>,
    ) -> Result<CleanupRecord, DeploymentError> {
        let container_removed = if let Some(container_name) = container_name.as_deref() {
            let _ = self.docker.stop_container(container_name);
            self.docker.remove_container(container_name).is_ok()
        } else {
            true
        };
        let route_removed = if let Some(route_subtree_id) = route_subtree_id.as_deref() {
            self.routing.remove_route(route_subtree_id).is_ok()
        } else {
            true
        };
        let image_removed = if let Some(image_ref) = image_ref.as_deref() {
            self.docker.remove_image(image_ref).is_ok()
        } else {
            true
        };
        let mut cleanup = CleanupRecord::new(
            generation_failure_reason,
            container_name,
            route_subtree_id,
            container_removed,
            route_removed,
            !(container_removed && route_removed && image_removed),
        );
        cleanup.image_ref = image_ref;
        cleanup.image_removed = image_removed;
        if cleanup.container_name.is_none() {
            cleanup.skipped.push("container:not_created".into());
        }
        if cleanup.image_ref.is_none() {
            cleanup.skipped.push("image:not_built".into());
        }
        if cleanup.route_subtree_id.is_none() {
            cleanup.skipped.push("route:not_created".into());
        }
        CleanupStore::new(env.clone(), generation).write_record(&cleanup)?;
        Ok(cleanup.normalized())
    }

    fn activate_generation(
        &mut self,
        validation: &ValidationPolicy,
        execution: &ExecutionConfig,
        record: &DeploymentRecord,
        env: &EnvironmentPaths,
        lifecycle_store: &LifecycleStore,
        generation: u64,
        container_name: &str,
        inspection: &ContainerInspection,
        diagnostics: &DiagnosticsStore,
        secret_values: &[String],
        warmup: WarmupObservation,
    ) -> Result<(), DeploymentError> {
        let pointers = PointerStore::new(env.clone());
        let previous_authoritative = pointers.read_authoritative_pointer()?;
        let mut promotion_summary = warmup.promotion_summary.clone();
        let reconciliation = ReconciliationStore::new(&self.storage_root);

        match validation.activation {
            ActivationMode::Direct => {
                let promotion_intent =
                    reconciliation.append_pending(intent_request_for_storage_root(
                        &self.storage_root,
                        "deployment_promotion",
                        &record.project_id,
                        &record.environment,
                        Some(generation),
                        "healthy",
                        "runtime_container_reconciliation",
                        BTreeMap::new(),
                    ))?;
                pointers.swap_current(generation)?;
                let _ = reconciliation.append_status(
                    &promotion_intent,
                    ReconciliationIntentStatus::Applied,
                    BTreeMap::new(),
                )?;
                promotion_summary.route_verification_succeeded = true;
            }
            ActivationMode::Http { internal_port } => {
                let subtree_id = route_subtree_id(record);
                let target = resolve_route_target(
                    inspection,
                    internal_port,
                    execution.network_name.as_deref(),
                    &PersistedRouteTargetSource::ContainerIp,
                )
                .ok_or_else(|| {
                    DeploymentError::InvalidInspection(
                        execution.network_name.as_deref().map_or_else(
                            || "container missing network IP".into(),
                            |network_name| {
                                format!("container missing IP on docker network {network_name}")
                            },
                        ),
                    )
                })?;
                let domain = load_environment_domain(
                    &self.storage_root,
                    &record.project_id,
                    &record.environment,
                )?;
                let route_intent =
                    reconciliation.append_pending(intent_request_for_storage_root(
                        &self.storage_root,
                        "route_activation",
                        &record.project_id,
                        &record.environment,
                        Some(generation),
                        "healthy",
                        "routing_reconciliation",
                        BTreeMap::from([
                            ("subtree_id".into(), Value::String(subtree_id.clone())),
                            ("target".into(), Value::String(target.clone())),
                            (
                                "domain".into(),
                                Value::String(domain.clone().unwrap_or_default()),
                            ),
                            (
                                "probe_path".into(),
                                validation
                                    .http_health_path
                                    .clone()
                                    .map(Value::String)
                                    .unwrap_or(Value::Null),
                            ),
                        ]),
                    ))?;
                self.routing.update_route(RouteUpdateRequest {
                    subtree_id: subtree_id.clone(),
                    target: target.clone(),
                    domain: domain.clone(),
                    health_checks_enabled: false,
                    probe_path: validation.http_health_path.clone(),
                })?;
                let _ = reconciliation.append_status(
                    &route_intent,
                    ReconciliationIntentStatus::Applied,
                    BTreeMap::new(),
                )?;
                let inspection = self.routing.inspect_route(&subtree_id)?;
                let context = RouteActivationContext {
                    route_id: subtree_id,
                    domain,
                    upstream_target: target,
                    verification_url: inspection.verification_url.clone(),
                    verification_host: inspection.verification_host.clone(),
                    verification_status_code: inspection.verification_status_code,
                    verification_response_body: inspection.verification_response_body.clone(),
                    network_name: execution.network_name.clone(),
                };
                if let Err(err) = validate_route_activation(&inspection, &context) {
                    self.capture_container_logs_tail(diagnostics, container_name, secret_values)?;
                    self.capture_route_activation_failure_diagnostics(
                        diagnostics,
                        &inspection,
                        &context,
                        secret_values,
                    )?;
                    promotion_summary.gate_reason = Some(err.to_string());
                    persist_lifecycle_transition(
                        lifecycle_store,
                        &record.project_id,
                        &record.environment,
                        generation,
                        DeploymentLifecycleState::Failed,
                        err.to_string(),
                        Some(warmup.validation_summary.clone()),
                        Some(promotion_summary),
                    )?;
                    return Err(err);
                }
                promotion_summary.route_verification_succeeded = true;
                let promotion_intent =
                    reconciliation.append_pending(intent_request_for_storage_root(
                        &self.storage_root,
                        "deployment_promotion",
                        &record.project_id,
                        &record.environment,
                        Some(generation),
                        "healthy",
                        "runtime_container_reconciliation",
                        BTreeMap::new(),
                    ))?;
                pointers.swap_current(generation)?;
                let _ = reconciliation.append_status(
                    &promotion_intent,
                    ReconciliationIntentStatus::Applied,
                    BTreeMap::new(),
                )?;
            }
        }

        diagnostics.append_log_line(
            &format!(
                "current pointer updated: {} -> {generation}",
                previous_authoritative
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unset".into())
            ),
            secret_values,
        )?;
        diagnostics.append_log_line(
            &format!("promotion pointer updated: {generation}"),
            secret_values,
        )?;
        RuntimeStateStore::new(env.clone()).save(&RuntimeState {
            active_generation: Some(generation),
            health_state: RuntimeHealthState::Healthy,
            failed_probe_count: 0,
            successful_probe_count: 0,
            restart_attempted: false,
            degraded_since_unix: None,
            last_transition: "promotion_completed".into(),
            last_error_code: None,
        })?;
        promotion_summary.promoted_at_unix = Some(current_unix_timestamp());
        promotion_summary.gate_reason = None;
        persist_lifecycle_transition(
            lifecycle_store,
            &record.project_id,
            &record.environment,
            generation,
            DeploymentLifecycleState::Promoted,
            "generation promoted",
            Some(warmup.validation_summary),
            Some(promotion_summary),
        )?;
        Ok(())
    }

    fn resolve_runtime_secrets(
        &self,
        context_path: &Path,
        record: &DeploymentRecord,
    ) -> Result<Vec<SecretResolution>, DeploymentError> {
        let Some(manifest) = load_optional_manifest(context_path)? else {
            return Ok(Vec::new());
        };
        let store = SecretStore::new(self.storage_root.join("secrets"))?;
        let mut resolved = Vec::new();
        for (env_name, reference) in manifest.environment_variables {
            resolved.push(resolve_secret_reference(
                &store, record, env_name, reference,
            )?);
        }
        Ok(resolved)
    }
}

pub struct RollbackExecutor {
    storage_root: PathBuf,
}

impl RollbackExecutor {
    pub fn new(storage_root: impl Into<PathBuf>) -> Self {
        Self {
            storage_root: storage_root.into(),
        }
    }

    pub fn rollback_previous(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<u64, DeploymentError> {
        let env = EnvironmentPaths::new(&self.storage_root, project_id, environment);
        env.ensure_exists()?;
        let pointers = PointerStore::new(env.clone());
        let target = pointers
            .read_pointer("previous")?
            .ok_or(DeploymentError::RollbackUnavailable)?;
        let snapshot = env.generation_dir(target).join("snapshot.json");
        if !snapshot.exists() {
            return Err(DeploymentError::RollbackUnavailable);
        }
        let reconciliation = ReconciliationStore::new(&self.storage_root);
        let rollback_intent = reconciliation.append_pending(intent_request_for_storage_root(
            &self.storage_root,
            "rollback",
            project_id,
            environment,
            Some(target),
            "healthy",
            "runtime_container_reconciliation",
            BTreeMap::new(),
        ))?;
        pointers.swap_current(target)?;
        let _ = reconciliation.append_status(
            &rollback_intent,
            ReconciliationIntentStatus::Applied,
            BTreeMap::new(),
        )?;
        append_simple_event(
            &EventStore::new(env.clone(), target),
            project_id,
            environment,
            target,
            None,
            "ROLLBACK_COMPLETED",
            None,
        )?;
        metrics_registry().record_rollback();
        Ok(target)
    }
}

fn update_generation_history<F>(
    env: &EnvironmentPaths,
    generation: u64,
    mut apply: F,
) -> Result<(), DeploymentError>
where
    F: FnMut(&mut GenerationHistoryRecord),
{
    let store = RetentionStore::new(env.clone());
    let mut metadata = store.read()?;
    let mut updated = false;
    for record in &mut metadata.generations {
        if record.generation == generation {
            apply(record);
            updated = true;
            break;
        }
    }
    if !updated {
        let mut record = GenerationHistoryRecord {
            generation,
            ..GenerationHistoryRecord::default()
        };
        apply(&mut record);
        metadata.generations.push(record);
        metadata.generations.sort_by_key(|record| record.generation);
    }
    metadata.updated_at_unix = Some(current_unix_timestamp());
    store.write(&metadata)?;
    Ok(())
}

fn persist_lifecycle_transition(
    store: &LifecycleStore,
    project_id: &str,
    environment: &str,
    generation: u64,
    state: DeploymentLifecycleState,
    transition_reason: impl Into<String>,
    validation_summary: Option<PersistedValidationSummary>,
    promotion_summary: Option<PersistedPromotionSummary>,
) -> Result<(), DeploymentError> {
    let entered_at_unix = current_unix_timestamp();
    let mut lifecycle = store.read()?.unwrap_or(PersistedDeploymentLifecycle {
        lifecycle_version: 1,
        project_id: project_id.into(),
        environment: environment.into(),
        generation,
        state: state.clone(),
        entered_at_unix,
        transition_reason: String::new(),
        validation_summary: None,
        promotion_summary: None,
        transitions: Vec::new(),
    });
    lifecycle.transition(
        state,
        entered_at_unix,
        transition_reason,
        validation_summary,
        promotion_summary,
    );
    store.write(&lifecycle)?;
    Ok(())
}

fn update_lifecycle_progress(
    store: &LifecycleStore,
    validation_summary: Option<PersistedValidationSummary>,
    promotion_summary: Option<PersistedPromotionSummary>,
) -> Result<(), DeploymentError> {
    let Some(mut lifecycle) = store.read()? else {
        return Ok(());
    };
    lifecycle.validation_summary = validation_summary;
    lifecycle.promotion_summary = promotion_summary;
    store.write(&lifecycle)?;
    Ok(())
}

fn append_event(
    store: &EventStore,
    record: &DeploymentRecord,
    generation: u64,
    event_type: &str,
    reason: Option<String>,
) -> Result<(), DeploymentError> {
    append_simple_event(
        store,
        &record.project_id,
        &record.environment,
        generation,
        Some(record.deployment_id.clone()),
        event_type,
        reason.as_deref(),
    )
}

fn append_redacted_event(
    store: &EventStore,
    record: &DeploymentRecord,
    generation: u64,
    event_type: &str,
    reason: Option<String>,
    secrets: &[String],
) -> Result<(), DeploymentError> {
    let redacted = reason.map(|value| redact_text(&value, secrets));
    append_simple_event(
        store,
        &record.project_id,
        &record.environment,
        generation,
        Some(record.deployment_id.clone()),
        event_type,
        redacted.as_deref(),
    )
}

fn append_simple_event(
    store: &EventStore,
    project_id: &str,
    environment: &str,
    generation: u64,
    deployment_id: Option<String>,
    event_type: &str,
    reason: Option<&str>,
) -> Result<(), DeploymentError> {
    let timestamp_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    store.append(&EventRecord {
        timestamp_unix,
        project_id: project_id.into(),
        environment: environment.into(),
        generation: Some(generation),
        deployment_id,
        event_type: event_type.into(),
        reason: reason.map(|value| value.to_string()),
    })?;
    Ok(())
}

fn validate_inspection(
    inspection: &ContainerInspection,
    expected_container_name: &str,
    expected_policy: &PersistedRuntimePolicy,
) -> Result<(), DeploymentError> {
    if inspection.container_name != expected_container_name {
        return Err(DeploymentError::InvalidInspection(
            "inspected container name mismatch".into(),
        ));
    }
    if !inspection.running {
        return Err(DeploymentError::InvalidInspection(
            "container is not running".into(),
        ));
    }
    let actual_policy = inspection_runtime_policy(inspection);
    let expected_policy = PersistedRuntimePolicy {
        restart_policy: crate::storage::normalize_restart_policy_name(
            &expected_policy.restart_policy,
        ),
        ..expected_policy.clone()
    };
    if actual_policy != expected_policy {
        return Err(DeploymentError::InvalidInspection(format!(
            "runtime policy mismatch: expected {:?}, got {:?}",
            expected_policy, actual_policy
        )));
    }
    Ok(())
}

fn resolve_selected_network_host(
    inspection: &ContainerInspection,
    network_name: Option<&str>,
) -> Result<String, DeploymentError> {
    if let Some(network_name) = network_name {
        return inspection
            .network_ips
            .get(network_name)
            .filter(|ip| !ip.is_empty())
            .cloned()
            .ok_or_else(|| {
                DeploymentError::InvalidInspection(format!(
                    "container missing IP on docker network {network_name}"
                ))
            });
    }

    inspection
        .network_ips
        .values()
        .find(|ip| !ip.is_empty())
        .cloned()
        .ok_or_else(|| DeploymentError::InvalidInspection("container missing network IP".into()))
}

fn forge_labels(record: &DeploymentRecord, generation: u64) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("forge.managed".into(), "true".into()),
        ("forge.project_id".into(), record.project_id.clone()),
        ("forge.environment".into(), record.environment.clone()),
        ("forge.generation".into(), generation.to_string()),
        ("forge.deployment_id".into(), record.deployment_id.clone()),
    ])
}

fn resolve_secret_reference(
    store: &SecretStore,
    record: &DeploymentRecord,
    env_name: String,
    reference: SecretReference,
) -> Result<SecretResolution, DeploymentError> {
    match reference.scope.as_str() {
        "environment" => match store.read_environment_secret(
            &record.project_id,
            &record.environment,
            &reference.key,
        ) {
            Ok(value) => Ok(SecretResolution {
                key: env_name,
                source_key: reference.key.clone(),
                value,
                sensitive: reference.sensitive,
            }),
            Err(SecretError::MissingSecret(key)) => Err(DeploymentError::MissingSecret(format!(
                "missing required secret {key}"
            ))),
            Err(err) => Err(DeploymentError::Secret(err)),
        },
        other => Err(DeploymentError::InvalidInspection(format!(
            "unsupported secret scope {other}"
        ))),
    }
}

fn generation_container_name(record: &DeploymentRecord, generation: u64) -> String {
    let env = match record.environment.as_str() {
        "production" => "prod",
        "staging" => "staging",
        "development" => "dev",
        other => other,
    };
    format!("{env}-{}-gen-{generation}", record.project_id)
}

fn generation_service_container_name(
    record: &DeploymentRecord,
    generation: u64,
    service_id: &str,
    service_count: usize,
) -> String {
    if service_count <= 1 && service_id == record.project_id {
        return generation_container_name(record, generation);
    }
    let env = match record.environment.as_str() {
        "production" => "prod",
        "staging" => "staging",
        "development" => "dev",
        other => other,
    };
    format!("{env}-{}-{service_id}-gen-{generation}", record.project_id)
}

fn format_probe_target_log(target: &ProbeTargetContext) -> String {
    match &target.path {
        Some(path) => format!(
            "probe target: host={} port={} path={}",
            target.host, target.port, path
        ),
        None => format!("probe target: host={} port={}", target.host, target.port),
    }
}

fn format_network_map(network_ips: &BTreeMap<String, String>) -> String {
    if network_ips.is_empty() {
        return "none".into();
    }
    network_ips
        .iter()
        .map(|(network, ip)| format!("{network}={ip}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn looks_like_inspect_output(value: &str) -> bool {
    value.contains("name=")
        && value.contains("status=")
        && value.contains("running=")
        && value.contains("image=")
}

fn bridge_reachability_diagnostic(
    inspection: &ContainerInspection,
    selected_network: Option<&str>,
    probe_target: Option<&ProbeTargetContext>,
) -> Option<String> {
    let network_name = selected_network?;
    if network_name != "bridge" {
        return None;
    }

    let bridge_ip = inspection.network_ips.get("bridge")?;
    let probe_host = probe_target
        .map(|target| target.host.as_str())
        .unwrap_or(bridge_ip.as_str());
    Some(format!(
        "selected docker network is bridge and probe target {probe_host} comes from that network; bridge IPs are not assumed reachable from the Forge daemon host or Caddy. Use a dedicated shared Docker network such as {FORGE_MANAGED_DOCKER_NETWORK} so validation and route activation use the same reachability semantics."
    ))
}

fn container_exited_failure_reason(probe_name: &str, inspection: &ContainerInspection) -> String {
    match inspection.exit_code {
        Some(exit_code) => format!(
            "container exited before {probe_name} (status={} exit_code={exit_code})",
            inspection.state_status
        ),
        None => format!(
            "container exited before {probe_name} (status={})",
            inspection.state_status
        ),
    }
}

fn route_subtree_id(record: &DeploymentRecord) -> String {
    format!("forge:{}:{}", record.project_id, record.environment)
}

fn route_subtree_id_for_service(
    record: &DeploymentRecord,
    service_id: &str,
    service_count: usize,
) -> String {
    if service_count <= 1 {
        return route_subtree_id(record);
    }
    format!(
        "forge:{}:{}:{service_id}",
        record.project_id, record.environment
    )
}

fn classify_forge_yaml_failure_stage(message: &str) -> &'static str {
    if message.contains("service `")
        || message.contains("services")
        || message.contains("depends on")
        || message.contains("depends_on")
        || message.contains("dependency graph")
        || message.contains("cycle")
    {
        "topology"
    } else {
        "preparing"
    }
}

fn finalize_failed_generation(
    storage_root: &Path,
    record: &DeploymentRecord,
    generation: u64,
    diagnostics: &DiagnosticsStore,
) -> Result<(), DeploymentError> {
    let env = EnvironmentPaths::new(storage_root, &record.project_id, &record.environment);
    SnapshotWriter::new(env.clone(), generation)?.finalize(
        &record.project_id,
        &record.environment,
        SnapshotState::Failed,
    )?;
    update_generation_history(&env, generation, |history| {
        history.finalized_state = Some("failed".into());
        history.finalized_at_unix = Some(current_unix_timestamp());
    })?;
    diagnostics.append_log_line("failed generation persisted", &[])?;
    Ok(())
}

fn service_dependency_list(service: &ForgeServiceConfig) -> String {
    if service.depends_on.is_empty() {
        "none".into()
    } else {
        service.depends_on.join(", ")
    }
}

fn service_role_label(service: &ForgeServiceConfig) -> &'static str {
    if service.externally_exposed {
        "exposed"
    } else {
        "internal"
    }
}

fn format_dependency_graph_summary(config: &ForgeYamlConfig) -> String {
    config
        .startup_order()
        .iter()
        .map(|service_id| {
            let depends_on = config
                .services()
                .get(service_id)
                .map(service_dependency_list)
                .unwrap_or_else(|| "unknown".into());
            format!("{service_id}<-{depends_on}")
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn service_command(service: &ForgeServiceConfig) -> Option<Vec<String>> {
    service
        .command
        .as_ref()
        .map(|command| vec!["sh".into(), "-lc".into(), command.clone()])
}

fn select_primary_service(
    config: &ForgeYamlConfig,
    runtime: &BTreeMap<String, PersistedServiceRuntimeInfo>,
) -> Result<String, DeploymentError> {
    select_primary_service_id(
        &PersistedRuntimeInfo {
            container_name: String::new(),
            running: false,
            network_name: None,
            probe_path: None,
            activation: None,
            runtime_policy: PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            environment_variables: BTreeMap::new(),
            volume_mounts: Vec::new(),
            source_ref: None,
            repo_url: None,
            commit_sha: None,
            source_path: None,
            services: runtime.clone(),
            startup_order: config.startup_order().to_vec(),
        },
        runtime,
    )
    .ok_or_else(|| DeploymentError::InvalidInspection("service topology is empty".into()))
}

fn is_multi_service_config(config: &ForgeYamlConfig) -> bool {
    config.services().len() > 1
}

fn runtime_services(
    runtime: &PersistedRuntimeInfo,
) -> BTreeMap<String, PersistedServiceRuntimeInfo> {
    if !runtime.services.is_empty() {
        return runtime.services.clone();
    }
    BTreeMap::from([(
        "default".into(),
        PersistedServiceRuntimeInfo {
            service_id: "default".into(),
            container_name: runtime.container_name.clone(),
            image_ref: String::new(),
            running: runtime.running,
            state: PersistedServiceState::Healthy,
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
}

fn json_storage_error(err: serde_json::Error) -> DeploymentError {
    DeploymentError::Storage(StorageError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        err.to_string(),
    )))
}

fn load_environment_domain(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
) -> Result<Option<String>, DeploymentError> {
    let project = ProjectRegistryStore::new(storage_root)
        .get(project_id)
        .map_err(|err| {
            DeploymentError::InvalidInspection(format!(
                "project lookup failed for {project_id}: {err}"
            ))
        })?;
    Ok(project.map(|project| derive_environment_domain(&project.base_domain, environment)))
}

fn caddy_network_reachability_note(network_name: Option<&str>) -> Option<String> {
    network_name.map(|network_name| {
        format!(
            "route activation assumes Caddy is attached to docker network {network_name}; upstream target reachability is only guaranteed when Caddy shares the selected deploy network."
        )
    })
}

fn container_runtime_policy(policy: &ForgeRuntimePolicy) -> ContainerRuntimePolicy {
    ContainerRuntimePolicy {
        cpu_limit: policy.cpu_limit.clone(),
        memory_limit_mb: policy.memory_limit_mb,
        restart_policy: crate::storage::normalize_restart_policy_name(&policy.restart_policy),
        max_retries: policy.max_retries,
    }
}

fn persisted_runtime_policy(policy: &ForgeRuntimePolicy) -> PersistedRuntimePolicy {
    PersistedRuntimePolicy {
        cpu_limit: policy.cpu_limit.clone(),
        memory_limit_mb: policy.memory_limit_mb,
        restart_policy: crate::storage::normalize_restart_policy_name(&policy.restart_policy),
        max_retries: policy.max_retries,
    }
}

fn inspection_runtime_policy(inspection: &ContainerInspection) -> PersistedRuntimePolicy {
    PersistedRuntimePolicy {
        cpu_limit: inspection.cpu_limit.clone(),
        memory_limit_mb: inspection.memory_limit_mb,
        restart_policy: crate::storage::normalize_restart_policy_name(&inspection.restart_policy),
        max_retries: normalize_restart_max_retries(
            &crate::storage::normalize_restart_policy_name(&inspection.restart_policy),
            inspection.restart_max_retries,
        ),
    }
}

pub(crate) fn normalize_restart_max_retries(
    restart_policy: &str,
    max_retries: Option<u64>,
) -> Option<u64> {
    if crate::storage::normalize_restart_policy_name(restart_policy) == "no" {
        None
    } else {
        max_retries
    }
}

fn inspection_runtime_usage<D: DockerRuntime>(
    docker: &mut D,
    container_name: &str,
) -> Option<PersistedRuntimeUsageSnapshot> {
    docker
        .container_usage(container_name)
        .ok()
        .map(|usage| PersistedRuntimeUsageSnapshot {
            captured_at_unix: usage.captured_at_unix,
            cpu_percent: usage.cpu_percent,
            memory_usage_mb: usage.memory_usage_mb,
            memory_limit_mb: usage.memory_limit_mb,
        })
}

fn inspection_termination_info(
    inspection: &ContainerInspection,
    stderr_tail: Option<String>,
) -> PersistedTerminationInfo {
    PersistedTerminationInfo {
        oom_killed: inspection.oom_killed,
        observed_at_unix: Some(current_unix_timestamp()),
        exit_code: inspection.exit_code,
        last_exit_code: inspection.exit_code,
        exit_signal: inspection.exit_signal,
        finished_at: inspection.finished_at.clone(),
        error: inspection.error.clone(),
        reason: inspection.termination_reason.clone(),
        termination_reason: inspection.termination_reason.clone(),
        stderr_tail: stderr_tail.clone(),
        logs_tail: stderr_tail,
        restart_count: inspection.restart_count,
    }
}

fn restart_count_delta(initial: u64, inspection: &ContainerInspection) -> u64 {
    inspection.restart_count.saturating_sub(initial)
}

fn observed_exit_count_before_validation(initial: u64, inspection: &ContainerInspection) -> u64 {
    restart_count_delta(initial, inspection)
        + u64::from(!inspection.running && inspection.exit_code.unwrap_or(0) != 0)
}

fn restart_storm_detected(initial: u64, inspection: &ContainerInspection) -> bool {
    restart_count_delta(initial, inspection) >= RESTART_STORM_DELTA_THRESHOLD
        || observed_exit_count_before_validation(initial, inspection)
            > EXITS_BEFORE_VALIDATION_THRESHOLD
}

fn validate_route_activation(
    inspection: &RouteInspection,
    context: &RouteActivationContext,
) -> Result<(), DeploymentError> {
    if inspection.subtree_id != context.route_id {
        return Err(DeploymentError::InvalidInspection(
            "route subtree mismatch".into(),
        ));
    }
    if !inspection.activation_verified {
        return Err(DeploymentError::ValidationFailed(
            "route activation verification failed",
        ));
    }
    if inspection.active_target != context.upstream_target {
        return Err(DeploymentError::ValidationFailed("route target mismatch"));
    }
    if inspection.domain != context.domain {
        return Err(DeploymentError::ValidationFailed(
            "route activation domain mismatch",
        ));
    }
    if inspection.health_checks_enabled {
        return Err(DeploymentError::ValidationFailed(
            "routing health checks must remain disabled",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn test_root(name: &str) -> PathBuf {
    use std::fs;
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
#[derive(Default)]
struct TestProbeRuntime {
    tcp_ok: bool,
    http_ok: bool,
}

#[cfg(test)]
impl ProbeRuntime for TestProbeRuntime {
    fn probe_tcp(
        &mut self,
        _container_name: &str,
        _internal_port: u16,
    ) -> Result<bool, ProbeError> {
        Ok(self.tcp_ok)
    }

    fn probe_http(
        &mut self,
        _container_name: &str,
        _internal_port: u16,
        _path: &str,
    ) -> Result<bool, ProbeError> {
        Ok(self.http_ok)
    }
}

#[cfg(test)]
#[derive(Default)]
struct RecordingProbeRuntime {
    tcp_ok: bool,
    http_ok: bool,
    tcp_hosts: Vec<(String, u16)>,
    http_hosts: Vec<(String, u16, String)>,
}

#[cfg(test)]
impl ProbeRuntime for RecordingProbeRuntime {
    fn probe_tcp(&mut self, host: &str, internal_port: u16) -> Result<bool, ProbeError> {
        self.tcp_hosts.push((host.to_string(), internal_port));
        Ok(self.tcp_ok)
    }

    fn probe_http(
        &mut self,
        host: &str,
        internal_port: u16,
        path: &str,
    ) -> Result<bool, ProbeError> {
        self.http_hosts
            .push((host.to_string(), internal_port, path.to_string()));
        Ok(self.http_ok)
    }
}

#[cfg(test)]
#[derive(Default)]
struct SequencedProbeRuntime {
    tcp_results: VecDeque<Result<bool, ProbeError>>,
    http_results: VecDeque<Result<bool, ProbeError>>,
    tcp_attempts: u32,
    http_attempts: u32,
}

#[cfg(test)]
impl ProbeRuntime for SequencedProbeRuntime {
    fn probe_tcp(&mut self, _host: &str, _internal_port: u16) -> Result<bool, ProbeError> {
        self.tcp_attempts += 1;
        self.tcp_results.pop_front().unwrap_or(Ok(true))
    }

    fn probe_http(
        &mut self,
        _host: &str,
        _internal_port: u16,
        _path: &str,
    ) -> Result<bool, ProbeError> {
        self.http_attempts += 1;
        self.http_results.pop_front().unwrap_or(Ok(true))
    }
}

#[cfg(test)]
#[derive(Default)]
struct TestRoutingRuntime {
    updates: Vec<RouteUpdateRequest>,
    inspections: Vec<RouteInspection>,
}

#[cfg(test)]
impl RoutingRuntime for TestRoutingRuntime {
    fn update_route(&mut self, request: RouteUpdateRequest) -> Result<(), RoutingRuntimeError> {
        self.updates.push(request);
        Ok(())
    }

    fn inspect_route(&mut self, _subtree_id: &str) -> Result<RouteInspection, RoutingRuntimeError> {
        if self.inspections.is_empty() {
            return Err(RoutingRuntimeError::InspectionFailed(
                "missing inspection".into(),
            ));
        }
        Ok(self.inspections.remove(0))
    }

    fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
        Ok(self.inspections.clone())
    }

    fn remove_route(&mut self, _subtree_id: &str) -> Result<(), RoutingRuntimeError> {
        Ok(())
    }
}

#[cfg(test)]
fn queued_record(queue: &PersistentQueue) {
    queue
        .enqueue(DeploymentRecord {
            deployment_id: "dep-1".into(),
            project_id: "api".into(),
            environment: "production".into(),
            intent: "deploy".into(),
            source_path: None,
            source_ref: None,
            repo_url: None,
            commit_sha: None,
        })
        .unwrap();
}

#[cfg(test)]
fn register_project(root: &std::path::Path, project_id: &str, base_domain: &str) {
    ProjectRegistryStore::new(root)
        .upsert(
            crate::api::ProjectUpsertRequest {
                project_id: Some(project_id.into()),
                repo_url: format!("https://example.com/{project_id}.git"),
                default_branch: "main".into(),
                base_domain: Some(base_domain.into()),
            },
            None,
        )
        .unwrap();
}

#[cfg(test)]
fn default_execution_config(root: &std::path::Path) -> ExecutionConfig {
    ExecutionConfig {
        context_path: root.to_path_buf(),
        dockerfile_path: root.join("Dockerfile"),
        network_name: Some(FORGE_MANAGED_DOCKER_NETWORK.into()),
    }
}

#[cfg(test)]
fn success_outputs(generation: u64) -> Vec<String> {
    success_outputs_with_network(generation, &[("forge-test", "172.18.0.2")])
}

#[cfg(test)]
fn success_outputs_with_network(generation: u64, networks: &[(&str, &str)]) -> Vec<String> {
    let inspection = inspection_output(generation, "running", true, 0, networks);
    vec![
        format!("image_ref=forge/api:production-gen-{generation}"),
        format!("prod-api-gen-{generation}"),
        String::new(),
        inspection.clone(),
        inspection.clone(),
        inspection.clone(),
        inspection.clone(),
        inspection,
        String::new(),
    ]
}

#[cfg(test)]
fn success_outputs_with_runtime_policy(
    generation: u64,
    networks: &[(&str, &str)],
    cpu_limit: &str,
    memory_limit_mb: u64,
    restart_policy: &str,
    restart_max_retries: Option<u64>,
) -> Vec<String> {
    let mut extra = vec![
        (
            "nano_cpus".to_string(),
            cpu_limit_to_nano_cpus(cpu_limit).to_string(),
        ),
        (
            "memory_bytes".to_string(),
            (memory_limit_mb * 1024 * 1024).to_string(),
        ),
        ("restart_policy".to_string(), restart_policy.to_string()),
    ];
    if let Some(retries) = restart_max_retries {
        extra.push(("restart_max_retries".to_string(), retries.to_string()));
    }
    let inspection = inspection_output_with_details(
        generation,
        "running",
        true,
        0,
        0,
        networks,
        &extra
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<Vec<_>>(),
    );
    vec![
        format!("image_ref=forge/api:production-gen-{generation}"),
        format!("prod-api-gen-{generation}"),
        String::new(),
        inspection.clone(),
        inspection.clone(),
        inspection.clone(),
        inspection.clone(),
        inspection,
        String::new(),
    ]
}

#[cfg(test)]
fn networked_success_outputs_with_network(
    generation: u64,
    networks: &[(&str, &str)],
) -> Vec<String> {
    let mut outputs = success_outputs_with_network(generation, networks);
    outputs.insert(1, String::new());
    outputs
}

#[cfg(test)]
fn inspection_output(
    generation: u64,
    status: &str,
    running: bool,
    exit_code: i32,
    networks: &[(&str, &str)],
) -> String {
    inspection_output_with_restart_count(generation, status, running, exit_code, 0, networks)
}

#[cfg(test)]
fn inspection_output_with_restart_count(
    generation: u64,
    status: &str,
    running: bool,
    exit_code: i32,
    restart_count: u64,
    networks: &[(&str, &str)],
) -> String {
    std::iter::once(format!("name=prod-api-gen-{generation}"))
        .chain(std::iter::once(format!("status={status}")))
        .chain(std::iter::once(format!("running={running}")))
        .chain(std::iter::once(format!("exit_code={exit_code}")))
        .chain(std::iter::once(format!("restart_count={restart_count}")))
        .chain(std::iter::once(format!(
            "image=forge/api:production-gen-{generation}"
        )))
        .chain(std::iter::once("restart_policy=no".into()))
        .chain(
            networks
                .iter()
                .map(|(name, ip)| format!("network:{name}={ip}")),
        )
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
fn cpu_limit_to_nano_cpus(cpu_limit: &str) -> u64 {
    (cpu_limit.parse::<f64>().expect("cpu limit should parse") * 1_000_000_000_f64).round() as u64
}

#[cfg(test)]
fn inspection_output_with_details(
    generation: u64,
    status: &str,
    running: bool,
    exit_code: i32,
    restart_count: u64,
    networks: &[(&str, &str)],
    extra: &[(&str, &str)],
) -> String {
    let mut lines = inspection_output_with_restart_count(
        generation,
        status,
        running,
        exit_code,
        restart_count,
        networks,
    )
    .lines()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();
    for (key, value) in extra {
        lines.push(format!("{key}={value}"));
    }
    lines.join("\n")
}

#[cfg(test)]
pub mod deployment_fails_if_tcp_unreachable {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn tcp_probe_failure_rejects_deployment() {
        let root = test_root("tcp-unreachable");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(1, &[("forge-test", "172.18.0.2")]),
        ));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next();

        assert!(result.is_err());
        let snapshot = std::fs::read_to_string(
            root.join("projects/api/environments/production/generations/1/snapshot.json"),
        )
        .unwrap();
        assert!(snapshot.contains("\"state\": \"failed\""));
    }
}

#[cfg(test)]
pub mod validation_probe_retries {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use std::fs;

    fn write_forge_yaml(root: &std::path::Path, timeout_ms: u64) {
        fs::create_dir_all(root).unwrap();
        fs::write(
            root.join("forge.yml"),
            format!(
                concat!(
                    "version: 1\n",
                    "name: api\n",
                    "type: web\n",
                    "build:\n",
                    "  dockerfile: Dockerfile\n",
                    "  context: .\n",
                    "runtime:\n",
                    "  port: 3000\n",
                    "  healthcheck:\n",
                    "    path: /health\n",
                    "    expected_status: 200\n",
                    "invariants:\n",
                    "  - name: health\n",
                    "    path: /health\n",
                    "    expect_status: 200\n",
                    "    timeout_ms: {timeout_ms}\n",
                ),
                timeout_ms = timeout_ms
            ),
        )
        .unwrap();
    }

    #[test]
    fn tcp_probe_retries_until_container_listens() {
        let root = test_root("tcp-probe-retries-until-container-listens");
        let source_root = root.join("source");
        write_forge_yaml(&source_root, 500);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(
                1,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.18.0.2")],
            ),
        ));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(false), Ok(false), Ok(true)]),
            http_results: VecDeque::from(vec![Ok(true)]),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap()
        .unwrap();

        assert_eq!(probes.tcp_attempts, 3);
    }

    #[test]
    fn tcp_probe_fails_after_bounded_timeout() {
        let root = test_root("tcp-probe-fails-after-bounded-timeout");
        let source_root = root.join("source");
        write_forge_yaml(&source_root, 250);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(
                1,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.18.0.2")],
            ),
        ));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(false), Ok(false), Ok(false), Ok(false)]),
            http_results: VecDeque::new(),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next();

        assert!(result.is_err());
        assert!(probes.tcp_attempts >= 2);
        assert!(probes.tcp_attempts <= 4);
    }

    #[test]
    fn http_probe_retries_until_health_passes() {
        let root = test_root("http-probe-retries-until-health-passes");
        let source_root = root.join("source");
        write_forge_yaml(&source_root, 5_000);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(
                1,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.18.0.2")],
            ),
        ));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(true)]),
            http_results: VecDeque::from(vec![Ok(false), Ok(false), Ok(true)]),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap()
        .unwrap();

        assert_eq!(probes.http_attempts, 3);
    }

    #[test]
    fn validation_failure_records_attempt_count_and_elapsed_ms() {
        let root = test_root("validation-failure-records-attempt-count-and-elapsed-ms");
        let source_root = root.join("source");
        write_forge_yaml(&source_root, 250);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            "image_ref=forge/api:production-gen-1".into(),
            String::new(),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            "npm start\nnode backend/index.js".into(),
        ]));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(false), Ok(false), Ok(false)]),
            http_results: VecDeque::new(),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next();

        let artifact = fs::read_to_string(
            root.join(
                "projects/api/environments/production/generations/1/diagnostics/validation_failure.json",
            ),
        )
        .unwrap();
        assert!(artifact.contains("\"attempts\":"));
        assert!(artifact.contains("\"elapsed_ms\":"));
        assert!(artifact.contains("\"last_error\": \"tcp probe returned unhealthy\""));
        assert!(artifact.contains("\"host\": \"172.29.0.2\""));
        assert!(artifact.contains("\"port\": 3000"));
        assert!(artifact.contains("\"path\": \"/health\""));
        assert!(artifact.contains("npm start"));
    }
}

#[cfg(test)]
pub mod deployment_fails_if_http_health_invalid {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn http_probe_failure_rejects_deployment() {
        let root = test_root("http-invalid");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut outputs = success_outputs(1);
        outputs.insert(
            outputs.len() - 1,
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
        );
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(outputs));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: false,
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Direct,
                ..ValidationPolicy::default()
            },
        )
        .execute_next();

        assert!(matches!(
            result,
            Err(DeploymentError::ValidationFailed(
                "http health probe failed"
            ))
        ));
        let snapshot = std::fs::read_to_string(
            root.join("projects/api/environments/production/generations/1/snapshot.json"),
        )
        .unwrap();
        assert!(snapshot.contains("\"state\": \"failed\""));
    }
}

#[cfg(test)]
pub mod progressive_promotion_guards {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use crate::storage::load_generation_lifecycle;
    use std::time::Instant;

    #[test]
    fn promotion_requires_stable_uptime() {
        let root = test_root("promotion-requires-stable-uptime");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        // Keep this fixture aligned with the docker command sequence in `success_outputs()`.
        // `create_container` and `start_container` do not return meaningful stdout; if an
        // extra placeholder is inserted before the first inspect output, the test will start
        // failing with `missing container name` from the production parser.
        let mut outputs = success_outputs(1);
        for _ in 0..8 {
            outputs.insert(
                outputs.len() - 1,
                inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            );
        }
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(outputs));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();
        let started = Instant::now();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                minimum_uptime_seconds: 1,
                ..ValidationPolicy::default()
            },
        )
        .execute_next()
        .unwrap();

        assert!(started.elapsed() >= Duration::from_secs(1));
        let env = EnvironmentPaths::new(&root, "api", "production");
        let lifecycle = load_generation_lifecycle(&env, 1).unwrap().unwrap();
        assert_eq!(lifecycle.state, DeploymentLifecycleState::Promoted);
        assert!(
            lifecycle
                .validation_summary
                .as_ref()
                .unwrap()
                .observed_uptime_seconds
                >= 1
        );
    }

    #[test]
    fn promotion_requires_multiple_successful_probes() {
        let root = test_root("promotion-requires-multiple-successful-probes");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            format!("image_ref=forge/api:production-gen-1"),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            String::new(),
        ]));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(true), Ok(true), Ok(true)]),
            http_results: VecDeque::from(vec![Ok(true), Ok(true), Ok(true)]),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                required_consecutive_probe_passes: 3,
                http_health_path: Some("/health".into()),
                ..ValidationPolicy::default()
            },
        )
        .execute_next()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let lifecycle = load_generation_lifecycle(&env, 1).unwrap().unwrap();
        let summary = lifecycle.validation_summary.unwrap();
        assert_eq!(summary.tcp_consecutive_passes, 3);
        assert_eq!(summary.http_consecutive_passes, 3);
        assert_eq!(summary.required_consecutive_passes, 3);
    }

    #[test]
    fn unstable_restart_prevents_promotion() {
        let root = test_root("unstable-restart-prevents-promotion");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            format!("image_ref=forge/api:production-gen-1"),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output_with_restart_count(
                1,
                "running",
                true,
                0,
                0,
                &[("forge-test", "172.18.0.2")],
            ),
            inspection_output_with_restart_count(
                1,
                "running",
                true,
                0,
                1,
                &[("forge-test", "172.18.0.2")],
            ),
            "restarting".into(),
        ]));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(true), Ok(true)]),
            http_results: VecDeque::new(),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                required_consecutive_probe_passes: 2,
                ..ValidationPolicy::default()
            },
        )
        .execute_next();
        let env = EnvironmentPaths::new(&root, "api", "production");
        let lifecycle = load_generation_lifecycle(&env, 1).unwrap().unwrap();
        assert_ne!(lifecycle.state, DeploymentLifecycleState::Promoted);
        assert!(
            PointerStore::new(env)
                .read_pointer("current")
                .unwrap()
                .is_none()
        );
        assert!(routing.updates.is_empty());
    }

    #[test]
    fn restart_storm_detected() {
        let inspection = ContainerInspection {
            container_name: "prod-api-gen-1".into(),
            running: false,
            state_status: "restarting".into(),
            exit_code: Some(1),
            restart_count: 3,
            started_at: None,
            finished_at: Some("2026-05-22T00:00:05Z".into()),
            oom_killed: false,
            error: None,
            image_ref: "forge/api:production-gen-1".into(),
            labels: BTreeMap::new(),
            network_ips: BTreeMap::new(),
            volume_mounts: Vec::new(),
            restart_policy: "no".into(),
            restart_max_retries: None,
            cpu_limit: None,
            memory_limit_mb: None,
            exit_signal: None,
            termination_reason: Some("exit_code_1".into()),
        };
        assert!(super::restart_storm_detected(0, &inspection));
    }

    #[test]
    fn promotion_blocks_on_nonzero_exit_before_validation() {
        let root = test_root("promotion-blocks-on-nonzero-exit-before-validation");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let exited = inspection_output_with_details(
            1,
            "exited",
            false,
            1,
            0,
            &[("forge-test", "172.18.0.2")],
            &[("finished_at", "2026-05-22T00:00:05Z")],
        );
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            "image_ref=forge/api:production-gen-1".into(),
            "prod-api-gen-1".into(),
            String::new(),
            exited.clone(),
            "service exited".into(),
        ]));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(true)]),
            http_results: VecDeque::new(),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: false,
                http_health_path: None,
                activation: ActivationMode::Direct,
                ..ValidationPolicy::default()
            },
        )
        .execute_next();

        assert!(result.is_err());
        let env = EnvironmentPaths::new(&root, "api", "production");
        let lifecycle = load_generation_lifecycle(&env, 1).unwrap().unwrap();
        assert_eq!(lifecycle.state, DeploymentLifecycleState::Failed);
        assert_eq!(
            PointerStore::new(env).read_pointer("current").unwrap(),
            None
        );
    }

    #[test]
    fn crash_loop_lifecycle_state_persisted() {
        let root = test_root("crash-loop-lifecycle-state-persisted");
        let env = EnvironmentPaths::new(&root, "api", "production");
        persist_lifecycle_transition(
            &LifecycleStore::new(env.clone(), 1),
            "api",
            "production",
            1,
            DeploymentLifecycleState::CrashLoop,
            "restart storm during warmup",
            Some(PersistedValidationSummary {
                restart_count_initial: 0,
                restart_count_current: 3,
                restart_count_stable: false,
                validation_succeeded: false,
                restart_storm_detected: true,
                ..PersistedValidationSummary::default()
            }),
            None,
        )
        .unwrap();

        let lifecycle = load_generation_lifecycle(&env, 1).unwrap().unwrap();
        assert_eq!(lifecycle.state, DeploymentLifecycleState::CrashLoop);
    }

    #[test]
    fn oom_killed_service_blocks_promotion() {
        let root = test_root("oom-killed-service-blocks-promotion");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let oom_killed = inspection_output_with_details(
            1,
            "exited",
            false,
            137,
            0,
            &[("forge-test", "172.18.0.2")],
            &[
                ("oom_killed", "true"),
                ("finished_at", "2026-05-22T00:00:05Z"),
            ],
        );
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            "image_ref=forge/api:production-gen-1".into(),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output_with_restart_count(
                1,
                "running",
                true,
                0,
                0,
                &[("forge-test", "172.18.0.2")],
            ),
            oom_killed.clone(),
            "killed by oom".into(),
        ]));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(true)]),
            http_results: VecDeque::new(),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                required_consecutive_probe_passes: 2,
                ..ValidationPolicy::default()
            },
        )
        .execute_next();

        assert!(result.is_err());
        let env = EnvironmentPaths::new(&root, "api", "production");
        assert_eq!(
            PointerStore::new(env.clone())
                .read_pointer("current")
                .unwrap(),
            None
        );
    }

    #[test]
    fn oom_lifecycle_state_persisted() {
        let root = test_root("oom-lifecycle-state-persisted");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let oom_killed = inspection_output_with_details(
            1,
            "exited",
            false,
            137,
            0,
            &[("forge-test", "172.18.0.2")],
            &[
                ("oom_killed", "true"),
                ("finished_at", "2026-05-22T00:00:05Z"),
            ],
        );
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            "image_ref=forge/api:production-gen-1".into(),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output_with_restart_count(
                1,
                "running",
                true,
                0,
                0,
                &[("forge-test", "172.18.0.2")],
            ),
            oom_killed.clone(),
            oom_killed,
            "killed by oom".into(),
        ]));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(true)]),
            http_results: VecDeque::new(),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                required_consecutive_probe_passes: 2,
                ..ValidationPolicy::default()
            },
        )
        .execute_next();

        let lifecycle =
            load_generation_lifecycle(&EnvironmentPaths::new(&root, "api", "production"), 1)
                .unwrap()
                .unwrap();
        assert_eq!(lifecycle.state, DeploymentLifecycleState::OomKilled);
    }

    #[test]
    fn lifecycle_transitions_are_persisted() {
        let root = test_root("lifecycle-transitions-are-persisted");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let lifecycle = load_generation_lifecycle(&env, 1).unwrap().unwrap();
        let states = lifecycle
            .transitions
            .iter()
            .map(|transition| transition.state.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            states,
            vec![
                "queued",
                "building",
                "starting",
                "warming",
                "validating",
                "promoted"
            ]
        );
    }

    #[test]
    fn convergence_does_not_route_unstable_candidate() {
        let root = test_root("convergence-does-not-route-unstable-candidate");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            format!("image_ref=forge/api:production-gen-1"),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output_with_restart_count(
                1,
                "running",
                true,
                0,
                0,
                &[("forge-test", "172.18.0.2")],
            ),
            inspection_output_with_restart_count(
                1,
                "running",
                true,
                0,
                1,
                &[("forge-test", "172.18.0.2")],
            ),
            "restarting".into(),
        ]));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(true), Ok(true)]),
            http_results: VecDeque::new(),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                required_consecutive_probe_passes: 2,
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .execute_next();

        assert!(routing.updates.is_empty());
    }
}

#[cfg(test)]
pub mod failed_generation_is_cleaned_up {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use crate::runtime::DockerRuntimeError;
    use crate::storage::CleanupStore;
    use std::collections::VecDeque;
    use std::fs;

    #[test]
    fn failed_generation_is_stopped_and_removed() {
        let root = test_root("failed-cleanup");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut runner = RecordingCommandRunner::with_outputs(success_outputs(1));
        let mut docker = DockerCliRuntime::new(std::mem::take(&mut runner));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next();

        let commands = &docker.runner.commands;
        assert!(
            commands
                .iter()
                .any(|cmd| cmd.args.first() == Some(&"stop".to_string()))
        );
        assert!(
            commands
                .iter()
                .any(|cmd| cmd.args.first() == Some(&"rm".to_string()))
        );
        assert!(
            commands
                .iter()
                .any(|cmd| cmd.args.first() == Some(&"rmi".to_string()))
        );
    }

    #[derive(Default)]
    struct BuildFailDockerRuntime;

    impl DockerRuntime for BuildFailDockerRuntime {
        fn build_image(
            &mut self,
            _request: BuildImageRequest,
        ) -> Result<String, DockerRuntimeError> {
            Err(DockerRuntimeError::CommandFailed("boom".into()))
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
            _request: CreateContainerRequest,
        ) -> Result<String, DockerRuntimeError> {
            unreachable!("build failure should not create containers")
        }

        fn start_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
            unreachable!("build failure should not start containers")
        }

        fn inspect_container(
            &mut self,
            _container_name: &str,
        ) -> Result<ContainerInspection, DockerRuntimeError> {
            unreachable!("build failure should not inspect containers")
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
            Ok(Vec::new())
        }

        fn list_managed_images(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedImage>, DockerRuntimeError> {
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

    fn cleanup_record(root: &std::path::Path) -> CleanupRecord {
        let env = EnvironmentPaths::new(root, "api", "production");
        CleanupStore::new(env, 1)
            .read_record()
            .unwrap()
            .expect("cleanup record should exist")
    }

    fn write_test_forge_yaml(root: &std::path::Path, timeout_ms: u64) {
        fs::create_dir_all(root).unwrap();
        fs::write(
            root.join("forge.yml"),
            format!(
                concat!(
                    "version: 1\n",
                    "name: api\n",
                    "type: web\n",
                    "build:\n",
                    "  dockerfile: Dockerfile\n",
                    "  context: .\n",
                    "runtime:\n",
                    "  port: 3000\n",
                    "  healthcheck:\n",
                    "    path: /health\n",
                    "    expected_status: 200\n",
                    "invariants:\n",
                    "  - name: health\n",
                    "    path: /health\n",
                    "    expect_status: 200\n",
                    "    timeout_ms: {timeout_ms}\n",
                ),
                timeout_ms = timeout_ms
            ),
        )
        .unwrap();
    }

    #[test]
    fn failed_generation_always_writes_cleanup_json() {
        let root = test_root("failed-generation-always-writes-cleanup-json");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next();

        let cleanup = cleanup_record(&root);
        assert!(cleanup.cleanup_attempted);
        assert!(cleanup.cleanup_completed);
        assert_eq!(
            cleanup.removed_containers,
            vec!["prod-api-gen-1".to_string()]
        );
        assert_eq!(
            cleanup.removed_images,
            vec!["forge/api:production-gen-1".to_string()]
        );
        assert!(cleanup.removed_volumes.is_empty());
        assert!(cleanup.skipped.contains(&"route:not_created".to_string()));
        assert!(cleanup.failure_reason.is_none());
        assert!(cleanup.timestamp > 0);
    }

    #[test]
    fn build_failure_writes_cleanup_json() {
        let root = test_root("build-failure-writes-cleanup-json");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = BuildFailDockerRuntime;
        let mut probes = TestProbeRuntime::default();
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next();

        assert!(result.is_err());
        let cleanup = cleanup_record(&root);
        assert!(cleanup.cleanup_attempted);
        assert!(cleanup.cleanup_completed);
        assert!(cleanup.removed_containers.is_empty());
        assert!(cleanup.removed_images.is_empty());
        assert!(cleanup.removed_volumes.is_empty());
        assert!(
            cleanup
                .skipped
                .contains(&"container:not_created".to_string())
        );
        assert!(cleanup.skipped.contains(&"image:not_built".to_string()));
        assert!(cleanup.skipped.contains(&"route:not_created".to_string()));
    }

    #[test]
    fn validation_failure_writes_cleanup_json() {
        let root = test_root("validation-failure-writes-cleanup-json");
        let source_root = root.join("source");
        write_test_forge_yaml(&source_root, 250);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            "image_ref=forge/api:production-gen-1".into(),
            String::new(),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.29.0.2")],
            ),
            "npm start\nnode backend/index.js".into(),
        ]));
        let mut probes = SequencedProbeRuntime {
            tcp_results: VecDeque::from(vec![Ok(false), Ok(false), Ok(false)]),
            http_results: VecDeque::new(),
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next();

        let cleanup = cleanup_record(&root);
        assert!(cleanup.cleanup_attempted);
        assert!(cleanup.cleanup_completed);
        assert_eq!(
            cleanup.removed_containers,
            vec!["prod-api-gen-1".to_string()]
        );
        assert_eq!(
            cleanup.removed_images,
            vec!["forge/api:production-gen-1".to_string()]
        );
    }

    #[test]
    fn pre_container_failure_writes_empty_cleanup_json() {
        let root = test_root("pre-container-failure-writes-empty-cleanup-json");
        let source_root = root.join("source");
        fs::create_dir_all(&source_root).unwrap();
        fs::write(source_root.join("forge.yml"), "version: [\n").unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = BuildFailDockerRuntime;
        let mut probes = TestProbeRuntime::default();
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next();

        let cleanup = cleanup_record(&root);
        assert!(cleanup.cleanup_attempted);
        assert!(cleanup.cleanup_completed);
        assert!(cleanup.removed_containers.is_empty());
        assert!(cleanup.removed_images.is_empty());
        assert!(cleanup.removed_volumes.is_empty());
        assert!(
            cleanup
                .skipped
                .contains(&"container:not_created".to_string())
        );
        assert!(cleanup.skipped.contains(&"image:not_built".to_string()));
        assert!(cleanup.skipped.contains(&"route:not_created".to_string()));
    }
}

#[cfg(test)]
pub mod events_are_appended_for_state_transitions {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use crate::storage::EventStore;

    #[test]
    fn transition_events_are_persisted() {
        let root = test_root("transition-events");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let inspection = RouteInspection {
            subtree_id: "forge:api:production".into(),
            active_target: "172.18.0.2:3000".into(),
            domain: Some("api.example.com".into()),
            activation_verified: true,
            verification_url: None,
            verification_host: None,
            verification_status_code: None,
            verification_response_body: None,
            health_checks_enabled: false,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![inspection; 8],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Direct,
                ..ValidationPolicy::default()
            },
        )
        .execute_next()
        .unwrap();

        let events = EventStore::list_all(&root).unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "DEPLOYMENT_STARTED")
        );
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "VALIDATION_PASSED")
        );
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "GENERATION_PROMOTED")
        );
    }
}

#[cfg(test)]
pub mod failed_probe_records_diagnostic_reason {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use crate::storage::DiagnosticsStore;

    #[test]
    fn diagnostic_reason_is_persisted_for_failed_probe() {
        let root = test_root("failed-probe-diagnostic");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next();

        let diagnostics =
            DiagnosticsStore::new(EnvironmentPaths::new(&root, "api", "production"), 1);
        let reason = diagnostics.read_failure_reason().unwrap().unwrap();
        assert!(reason.contains("tcp probe failed"));
    }
}

#[cfg(test)]
pub mod snapshot_not_finalized_before_validation {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn build_and_runtime_artifacts_exist_but_snapshot_does_not() {
        let root = test_root("snapshot-before-validation");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next();

        let generation_dir = root.join("projects/api/environments/production/generations/1");
        assert!(generation_dir.join("build.json").exists());
        assert!(generation_dir.join("runtime.json").exists());
        let snapshot = std::fs::read_to_string(generation_dir.join("snapshot.json")).unwrap();
        assert!(snapshot.contains("\"state\": \"failed\""));
    }
}

#[cfg(test)]
pub mod finalized_runtime_persists_http_recovery_metadata {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use crate::storage::load_generation_runtime_info;

    #[test]
    fn runtime_artifact_contains_restart_safe_http_route_metadata() {
        let root = test_root("persist-http-runtime-metadata");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(1, &[("forge-net", "172.18.0.2")]),
        ));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: ".".into(),
            dockerfile_path: "Dockerfile".into(),
            network_name: Some("forge-net".into()),
        })
        .execute_next()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let runtime = load_generation_runtime_info(&env, 1).unwrap().unwrap();
        assert_eq!(runtime.network_name.as_deref(), Some("forge-net"));
        assert_eq!(runtime.probe_path.as_deref(), Some("/health"));
        assert_eq!(
            runtime.activation,
            Some(PersistedActivationMode::Http {
                internal_port: 3000,
                route_subtree_id: Some("forge:api:production".into()),
                target_source: PersistedRouteTargetSource::ContainerIp,
            })
        );
    }
}

#[cfg(test)]
pub mod rollback_restores_previous_generation {
    use super::*;
    use crate::storage::EventStore;

    #[test]
    fn rollback_moves_current_pointer_back_to_previous() {
        let root = test_root("rollback-previous");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let writer1 = SnapshotWriter::new(env.clone(), 1).unwrap();
        writer1
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        let writer2 = SnapshotWriter::new(env.clone(), 2).unwrap();
        writer2
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let restored = RollbackExecutor::new(&root)
            .rollback_previous("api", "production")
            .unwrap();

        assert_eq!(restored, 1);
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
        let events = EventStore::list_all(&root).unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "ROLLBACK_COMPLETED")
        );
    }
}

#[cfg(test)]
pub mod git_backed_rollback_status_correctness {
    use super::*;
    use crate::status::{load_project_environment_env_report, load_project_environment_status};
    use crate::storage::load_generation_lifecycle;
    use std::collections::BTreeMap;
    use std::path::Path;

    #[derive(Default)]
    struct RollbackDockerRuntime {
        inspections: BTreeMap<String, ContainerInspection>,
    }

    impl DockerRuntime for RollbackDockerRuntime {
        fn build_image(
            &mut self,
            _request: BuildImageRequest,
        ) -> Result<String, DockerRuntimeError> {
            unreachable!("rollback tests do not build images")
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
            _request: CreateContainerRequest,
        ) -> Result<String, DockerRuntimeError> {
            unreachable!("rollback tests do not create containers")
        }

        fn start_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn inspect_container(
            &mut self,
            container_name: &str,
        ) -> Result<ContainerInspection, DockerRuntimeError> {
            self.inspections
                .get(container_name)
                .cloned()
                .ok_or_else(|| DockerRuntimeError::InvalidResponse("missing inspection".into()))
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
            Ok(self.inspections.values().cloned().collect())
        }

        fn list_managed_images(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedImage>, DockerRuntimeError> {
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

    fn generation_container_name(environment: &str, project_id: &str, generation: u64) -> String {
        let env = match environment {
            "production" => "prod",
            "staging" => "staging",
            "development" => "dev",
            other => other,
        };
        format!("{env}-{project_id}-gen-{generation}")
    }

    fn write_git_generation(
        root: &Path,
        project_id: &str,
        environment: &str,
        generation: u64,
        state: SnapshotState,
        source_ref: &str,
        commit_sha: &str,
    ) {
        let env = EnvironmentPaths::new(root, project_id, environment);
        let writer = SnapshotWriter::new(env.clone(), generation).unwrap();
        let image_ref = format!("forge/{project_id}:{environment}-gen-{generation}");
        let build = serde_json::to_string_pretty(&PersistedBuildInfo {
            deployment_id: format!("dep-{generation}"),
            image_ref: image_ref.clone(),
            services: BTreeMap::new(),
            source_ref: Some(source_ref.into()),
            repo_url: Some(format!("https://github.com/example/{project_id}.git")),
            commit_sha: Some(commit_sha.into()),
            source_path: Some(
                root.join("source-checkouts")
                    .join(project_id)
                    .join(commit_sha),
            ),
        })
        .unwrap();
        writer
            .write_artifact("build.json", &format!("{build}\n"))
            .unwrap();
        let runtime = serde_json::to_string_pretty(&PersistedRuntimeInfo {
            container_name: generation_container_name(environment, project_id, generation),
            running: true,
            network_name: Some(FORGE_MANAGED_DOCKER_NETWORK.into()),
            probe_path: Some("/health".into()),
            activation: Some(PersistedActivationMode::Http {
                internal_port: 3000,
                route_subtree_id: Some(format!("forge:{project_id}:{environment}")),
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
            source_ref: Some(source_ref.into()),
            repo_url: Some(format!("https://github.com/example/{project_id}.git")),
            commit_sha: Some(commit_sha.into()),
            source_path: Some(
                root.join("source-checkouts")
                    .join(project_id)
                    .join(commit_sha),
            ),
            services: BTreeMap::new(),
            startup_order: Vec::new(),
        })
        .unwrap();
        writer
            .write_artifact("runtime.json", &format!("{runtime}\n"))
            .unwrap();
        let runtime_env_snapshot = serde_json::json!({
            "snapshot_version": 1,
            "project_id": project_id,
            "environment": environment,
            "generation": generation,
            "deployment_id": format!("dep-{generation}"),
            "source_environment": environment,
            "source_ref": source_ref,
            "commit_sha": commit_sha,
            "domain": derive_environment_domain("api.example.com", environment),
            "resolution_order": [
                "forge_yml",
                "project_environment_secret",
                "deploy_time_override",
                "forge_generated",
                "system_runtime_reserved"
            ],
            "entries": {
                "FORGE_PROJECT_ID": {
                    "source": "forge_generated",
                    "value": project_id,
                    "sensitive": false,
                    "redacted": false
                },
                "FORGE_ENVIRONMENT": {
                    "source": "forge_generated",
                    "value": environment,
                    "sensitive": false,
                    "redacted": false
                },
                "FORGE_GENERATION": {
                    "source": "forge_generated",
                    "value": generation.to_string(),
                    "sensitive": false,
                    "redacted": false
                },
                "FORGE_DEPLOYMENT_ID": {
                    "source": "forge_generated",
                    "value": format!("dep-{generation}"),
                    "sensitive": false,
                    "redacted": false
                },
                "FORGE_COMMIT_SHA": {
                    "source": "forge_generated",
                    "value": commit_sha,
                    "sensitive": false,
                    "redacted": false
                },
                "FORGE_SOURCE_REF": {
                    "source": "forge_generated",
                    "value": source_ref,
                    "sensitive": false,
                    "redacted": false
                },
                "FORGE_DOMAIN": {
                    "source": "forge_generated",
                    "value": derive_environment_domain("api.example.com", environment),
                    "sensitive": false,
                    "redacted": false
                }
            }
        });
        writer
            .write_artifact(
                "runtime_env_snapshot.json",
                &format!(
                    "{}\n",
                    serde_json::to_string_pretty(&runtime_env_snapshot).unwrap()
                ),
            )
            .unwrap();
        let resolved_runtime = serde_json::json!({
            "snapshot_version": 1,
            "project_id": project_id,
            "environment": environment,
            "generation": generation,
            "deployment_id": format!("dep-{generation}"),
            "source_environment": environment,
            "source_ref": source_ref,
            "commit_sha": commit_sha,
            "domain": derive_environment_domain("api.example.com", environment),
            "entries": {
                "FORGE_PROJECT_ID": { "source": "forge_generated", "value": project_id, "sensitive": false },
                "FORGE_ENVIRONMENT": { "source": "forge_generated", "value": environment, "sensitive": false },
                "FORGE_GENERATION": { "source": "forge_generated", "value": generation.to_string(), "sensitive": false },
                "FORGE_DEPLOYMENT_ID": { "source": "forge_generated", "value": format!("dep-{generation}"), "sensitive": false },
                "FORGE_COMMIT_SHA": { "source": "forge_generated", "value": commit_sha, "sensitive": false },
                "FORGE_SOURCE_REF": { "source": "forge_generated", "value": source_ref, "sensitive": false },
                "FORGE_DOMAIN": { "source": "forge_generated", "value": derive_environment_domain("api.example.com", environment), "sensitive": false }
            }
        });
        writer
            .write_artifact(
                "resolved_runtime.json",
                &format!(
                    "{}\n",
                    serde_json::to_string_pretty(&resolved_runtime).unwrap()
                ),
            )
            .unwrap();
        writer.finalize(project_id, environment, state).unwrap();
        let lifecycle_store = LifecycleStore::new(env.clone(), generation);
        persist_lifecycle_transition(
            &lifecycle_store,
            project_id,
            environment,
            generation,
            DeploymentLifecycleState::Promoted,
            "seeded promoted generation",
            None,
            Some(PersistedPromotionSummary {
                warmup_succeeded: true,
                validation_succeeded: true,
                route_verification_succeeded: true,
                runtime_snapshot_persisted: true,
                convergence_target_stable: true,
                promoted_at_unix: Some(current_unix_timestamp()),
                gate_reason: None,
            }),
        )
        .unwrap();
    }

    fn rollback_record(project_id: &str, environment: &str) -> DeploymentRecord {
        DeploymentRecord {
            deployment_id: "dep-rollback".into(),
            project_id: project_id.into(),
            environment: environment.into(),
            intent: "rollback".into(),
            source_path: None,
            source_ref: None,
            repo_url: None,
            commit_sha: None,
        }
    }

    fn route_inspection(
        project_id: &str,
        environment: &str,
        ip: &str,
        domain: Option<&str>,
    ) -> RouteInspection {
        RouteInspection {
            subtree_id: format!("forge:{project_id}:{environment}"),
            active_target: format!("{ip}:3000"),
            domain: domain.map(|value| value.to_string()),
            activation_verified: true,
            verification_url: None,
            verification_host: None,
            verification_status_code: Some(200),
            verification_response_body: None,
            health_checks_enabled: false,
        }
    }

    fn container_inspection(
        project_id: &str,
        environment: &str,
        generation: u64,
        ip: &str,
    ) -> ContainerInspection {
        ContainerInspection {
            container_name: generation_container_name(environment, project_id, generation),
            running: true,
            state_status: "running".into(),
            exit_code: Some(0),
            restart_count: 0,
            started_at: Some("2026-05-21T12:00:00Z".into()),
            finished_at: None,
            oom_killed: false,
            error: None,
            image_ref: format!("forge/{project_id}:{environment}-gen-{generation}"),
            labels: BTreeMap::new(),
            network_ips: BTreeMap::from([(FORGE_MANAGED_DOCKER_NETWORK.into(), ip.into())]),
            volume_mounts: Vec::new(),
            restart_policy: "no".into(),
            restart_max_retries: None,
            cpu_limit: None,
            memory_limit_mb: None,
            exit_signal: None,
            termination_reason: None,
        }
    }

    fn write_generation_runtime_policy(
        root: &Path,
        project_id: &str,
        environment: &str,
        generation: u64,
        runtime_policy: PersistedRuntimePolicy,
    ) {
        let env = EnvironmentPaths::new(root, project_id, environment);
        let runtime_path = env.generation_dir(generation).join("runtime.json");
        let mut runtime: PersistedRuntimeInfo =
            serde_json::from_str(&std::fs::read_to_string(&runtime_path).unwrap()).unwrap();
        runtime.runtime_policy = runtime_policy;
        std::fs::write(
            runtime_path,
            format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()),
        )
        .unwrap();
    }

    #[test]
    fn git_deploy_rollback_restores_previous_generation() {
        let root = test_root("git-deploy-rollback-restores-previous-generation");
        register_project(&root, "api", "api.example.com");
        write_git_generation(
            &root,
            "api",
            "production",
            1,
            SnapshotState::Healthy,
            "main",
            "aaa111",
        );
        write_git_generation(
            &root,
            "api",
            "production",
            2,
            SnapshotState::Healthy,
            "release",
            "bbb222",
        );
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = RollbackDockerRuntime {
            inspections: BTreeMap::from([(
                generation_container_name("production", "api", 1),
                container_inspection("api", "production", 1, "172.29.0.11"),
            )]),
        };
        let mut probes = TestProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![route_inspection(
                "api",
                "production",
                "172.29.0.11",
                Some("api.example.com"),
            )],
        };

        let execution = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap()
        .unwrap();

        assert_eq!(execution.generation, 1);
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
        assert_eq!(pointers.read_pointer("previous").unwrap(), Some(2));
        assert_eq!(routing.updates[0].target, "172.29.0.11:3000");
    }

    #[test]
    fn rollback_preserves_lifecycle_history() {
        let root = test_root("rollback-preserves-lifecycle-history");
        register_project(&root, "api", "api.example.com");
        write_git_generation(
            &root,
            "api",
            "production",
            1,
            SnapshotState::Healthy,
            "main",
            "aaa111",
        );
        write_git_generation(
            &root,
            "api",
            "production",
            2,
            SnapshotState::Healthy,
            "release",
            "bbb222",
        );
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = RollbackDockerRuntime {
            inspections: BTreeMap::from([(
                generation_container_name("production", "api", 1),
                container_inspection("api", "production", 1, "172.29.0.11"),
            )]),
        };
        let mut probes = TestProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![route_inspection(
                "api",
                "production",
                "172.29.0.11",
                Some("api.example.com"),
            )],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        let lifecycle = load_generation_lifecycle(&env, 1).unwrap().unwrap();
        let states = lifecycle
            .transitions
            .iter()
            .map(|transition| transition.state.as_str())
            .collect::<Vec<_>>();
        assert_eq!(states, vec!["promoted", "rollback", "promoted"]);
    }

    #[test]
    fn rollback_preserves_source_metadata() {
        let root = test_root("rollback-preserves-source-metadata");
        register_project(&root, "api", "api.example.com");
        write_git_generation(
            &root,
            "api",
            "production",
            1,
            SnapshotState::Healthy,
            "main",
            "aaa111",
        );
        write_git_generation(
            &root,
            "api",
            "production",
            2,
            SnapshotState::Healthy,
            "release",
            "bbb222",
        );
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = RollbackDockerRuntime {
            inspections: BTreeMap::from([(
                generation_container_name("production", "api", 1),
                container_inspection("api", "production", 1, "172.29.0.11"),
            )]),
        };
        let mut probes = TestProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![
                route_inspection("api", "production", "172.29.0.11", Some("api.example.com")),
                route_inspection("api", "production", "172.29.0.11", Some("api.example.com")),
            ],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "production",
        )
        .unwrap();

        assert_eq!(status.commit_sha.as_deref(), Some("aaa111"));
        assert_eq!(status.source_ref.as_deref(), Some("main"));
        assert_eq!(
            status.image_ref.as_deref(),
            Some("forge/api:production-gen-1")
        );
        assert_eq!(status.container_name.as_deref(), Some("prod-api-gen-1"));
    }

    #[test]
    fn rollback_restores_generation_env_snapshot() {
        let root = test_root("rollback-restores-generation-env-snapshot");
        register_project(&root, "api", "api.example.com");
        write_git_generation(
            &root,
            "api",
            "production",
            1,
            SnapshotState::Healthy,
            "main",
            "aaa111",
        );
        write_git_generation(
            &root,
            "api",
            "production",
            2,
            SnapshotState::Healthy,
            "release",
            "bbb222",
        );
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = RollbackDockerRuntime {
            inspections: BTreeMap::from([(
                generation_container_name("production", "api", 1),
                container_inspection("api", "production", 1, "172.29.0.11"),
            )]),
        };
        let mut probes = TestProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![route_inspection(
                "api",
                "production",
                "172.29.0.11",
                Some("api.example.com"),
            )],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        let report = load_project_environment_env_report(&root, "api", "production").unwrap();
        assert_eq!(report.generation, 1);
        assert_eq!(report.deployment_id, "dep-1");
        assert!(
            report
                .values
                .iter()
                .any(|entry| entry.key == "FORGE_COMMIT_SHA" && entry.value == "aaa111")
        );
    }

    #[test]
    fn rollback_restores_resource_limits() {
        let root = test_root("rollback-restores-resource-limits");
        register_project(&root, "api", "api.example.com");
        write_git_generation(
            &root,
            "api",
            "production",
            1,
            SnapshotState::Healthy,
            "main",
            "aaa111",
        );
        write_git_generation(
            &root,
            "api",
            "production",
            2,
            SnapshotState::Healthy,
            "release",
            "bbb222",
        );
        write_generation_runtime_policy(
            &root,
            "api",
            "production",
            1,
            PersistedRuntimePolicy {
                cpu_limit: Some("1.5".into()),
                memory_limit_mb: Some(512),
                restart_policy: "on-failure".into(),
                max_retries: Some(4),
            },
        );
        write_generation_runtime_policy(
            &root,
            "api",
            "production",
            2,
            PersistedRuntimePolicy {
                cpu_limit: Some("2.0".into()),
                memory_limit_mb: Some(1024),
                restart_policy: "always".into(),
                max_retries: None,
            },
        );
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut generation_one = container_inspection("api", "production", 1, "172.29.0.11");
        generation_one.restart_policy = "on-failure".into();
        generation_one.restart_max_retries = Some(4);
        generation_one.cpu_limit = Some("1.5".into());
        generation_one.memory_limit_mb = Some(512);
        let mut docker = RollbackDockerRuntime {
            inspections: BTreeMap::from([(generation_one.container_name.clone(), generation_one)]),
        };
        let mut probes = TestProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![route_inspection(
                "api",
                "production",
                "172.29.0.11",
                Some("api.example.com"),
            )],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "production",
        )
        .unwrap();
        assert_eq!(status.active_generation, Some(1));
        assert_eq!(status.runtime_policy.cpu_limit.as_deref(), Some("1.5"));
        assert_eq!(status.runtime_policy.memory_limit_mb, Some(512));
        assert_eq!(status.runtime_policy.restart_policy, "on-failure");
        assert_eq!(status.runtime_policy.max_retries, Some(4));
    }

    #[test]
    fn rollback_restores_historical_env_snapshot() {
        let root = test_root("rollback-restores-historical-env-snapshot");
        register_project(&root, "api", "api.example.com");
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        write_git_generation(
            &root,
            "api",
            "production",
            1,
            SnapshotState::Healthy,
            "main",
            "aaa111",
        );
        write_git_generation(
            &root,
            "api",
            "production",
            2,
            SnapshotState::Healthy,
            "release",
            "bbb222",
        );
        crate::deployments::runtime_environment_snapshots::write_desired_env(
            &root,
            "production",
            &[("V75_TEST_KEY", "hello-v75")],
            &[],
        );
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = RollbackDockerRuntime {
            inspections: BTreeMap::from([(
                generation_container_name("production", "api", 1),
                container_inspection("api", "production", 1, "172.29.0.11"),
            )]),
        };
        let mut probes = TestProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![route_inspection(
                "api",
                "production",
                "172.29.0.11",
                Some("api.example.com"),
            )],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        let report = load_project_environment_env_report(&root, "api", "production").unwrap();
        assert_eq!(report.generation, 1);
        assert!(
            report
                .values
                .iter()
                .any(|entry| entry.key == "FORGE_COMMIT_SHA" && entry.value == "aaa111")
        );
        assert!(
            !report
                .values
                .iter()
                .any(|entry| entry.key == "V75_TEST_KEY")
        );
    }

    #[test]
    fn failed_git_deploy_does_not_replace_current() {
        let root = test_root("failed-git-deploy-does-not-replace-current");
        let source_root = root.join("source-checkouts").join("api").join("bbb222");
        std::fs::create_dir_all(&source_root).unwrap();
        let env = EnvironmentPaths::new(&root, "api", "production");
        write_git_generation(
            &root,
            "api",
            "production",
            1,
            SnapshotState::Healthy,
            "main",
            "aaa111",
        );
        PointerStore::new(env.clone()).swap_current(1).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-2".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: Some("release".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("bbb222".into()),
            })
            .unwrap();
        let mut docker = crate::docker::DockerCliRuntime::new(
            crate::docker::RecordingCommandRunner::with_outputs(success_outputs(2)),
        );
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next();

        assert!(result.is_err());
        assert_eq!(
            PointerStore::new(env.clone())
                .read_pointer("current")
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            PointerStore::new(env).read_pointer("previous").unwrap(),
            None
        );
    }

    #[test]
    fn rollback_uses_derived_environment_domain() {
        let root = test_root("rollback-uses-derived-environment-domain");
        register_project(&root, "api", "api.example.com");
        write_git_generation(
            &root,
            "api",
            "staging",
            1,
            SnapshotState::Healthy,
            "release",
            "aaa111",
        );
        write_git_generation(
            &root,
            "api",
            "staging",
            2,
            SnapshotState::Healthy,
            "release-hotfix",
            "bbb222",
        );
        let env = EnvironmentPaths::new(&root, "api", "staging");
        let pointers = PointerStore::new(env);
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue.enqueue(rollback_record("api", "staging")).unwrap();
        let mut docker = RollbackDockerRuntime {
            inspections: BTreeMap::from([(
                generation_container_name("staging", "api", 1),
                container_inspection("api", "staging", 1, "172.29.0.21"),
            )]),
        };
        let mut probes = TestProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![route_inspection(
                "api",
                "staging",
                "172.29.0.21",
                Some("staging-api.example.com"),
            )],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        assert_eq!(
            routing.updates[0].domain.as_deref(),
            Some("staging-api.example.com")
        );
    }

    #[test]
    fn rollback_does_not_route_to_failed_generation() {
        let root = test_root("rollback-does-not-route-to-failed-generation");
        register_project(&root, "api", "api.example.com");
        write_git_generation(
            &root,
            "api",
            "production",
            1,
            SnapshotState::Healthy,
            "main",
            "aaa111",
        );
        write_git_generation(
            &root,
            "api",
            "production",
            2,
            SnapshotState::Healthy,
            "release",
            "bbb222",
        );
        write_git_generation(
            &root,
            "api",
            "production",
            3,
            SnapshotState::Failed,
            "broken",
            "ccc333",
        );
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env);
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = RollbackDockerRuntime {
            inspections: BTreeMap::from([(
                generation_container_name("production", "api", 1),
                container_inspection("api", "production", 1, "172.29.0.31"),
            )]),
        };
        let mut probes = TestProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![route_inspection(
                "api",
                "production",
                "172.29.0.31",
                Some("api.example.com"),
            )],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        assert_eq!(routing.updates[0].target, "172.29.0.31:3000");
        assert_ne!(routing.updates[0].target, "172.29.0.33:3000");
    }
}

#[cfg(test)]
pub mod current_pointer_never_advances_before_validation {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn current_pointer_remains_unset_when_validation_fails() {
        let root = test_root("pointer-before-validation");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next();

        let pointers = PointerStore::new(EnvironmentPaths::new(&root, "api", "production"));
        assert_eq!(pointers.read_pointer("current").unwrap(), None);
    }
}

#[cfg(test)]
pub mod queued_deployment_builds_starts_validates_and_writes_snapshot {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn successful_deployment_advances_current_after_validation() {
        let root = test_root("deployment-executor-success");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let execution = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Direct,
                ..ValidationPolicy::default()
            },
        )
        .execute_next()
        .unwrap()
        .unwrap();

        assert_eq!(execution.generation, 1);
        assert!(
            root.join("projects/api/environments/production/generations/1/snapshot.json")
                .exists()
        );
        let pointers = PointerStore::new(EnvironmentPaths::new(&root, "api", "production"));
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
    }
}

#[cfg(test)]
pub mod validation_probes_configured_network_ip {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn deployment_validation_uses_inspected_ip_from_execution_network() {
        let root = test_root("validation-probe-ip");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(
                1,
                &[("bridge", "172.17.0.2"), ("forge-net", "172.19.0.5")],
            ),
        ));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Direct,
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: PathBuf::from("."),
            dockerfile_path: PathBuf::from("./Dockerfile"),
            network_name: Some("forge-net".into()),
        })
        .execute_next()
        .unwrap();

        assert_eq!(probes.tcp_hosts, vec![("172.19.0.5".to_string(), 3000)]);
        assert_eq!(
            probes.http_hosts,
            vec![("172.19.0.5".to_string(), 3000, "/health".to_string())]
        );
    }

    #[test]
    fn git_deploy_probe_targets_resolved_container_ip_and_port() {
        let root = test_root("git-deploy-probe-target-ip-port");
        let source_root = root.join("source-checkouts").join("api").join("abc123");
        std::fs::create_dir_all(&source_root).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(
                1,
                &[("bridge", "172.17.0.2"), ("forge-net", "172.19.0.5")],
            ),
        ));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Direct,
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: PathBuf::from("."),
            dockerfile_path: PathBuf::from("./Dockerfile"),
            network_name: Some("forge-net".into()),
        })
        .execute_next()
        .unwrap();

        assert_eq!(probes.tcp_hosts, vec![("172.19.0.5".to_string(), 3000)]);
        assert_eq!(
            probes.http_hosts,
            vec![("172.19.0.5".to_string(), 3000, "/health".to_string())]
        );
    }
}

#[cfg(test)]
pub mod deploy_loads_forge_yml {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use crate::storage::load_generation_runtime_info;
    use std::fs;

    #[test]
    fn deploy_from_path_loads_forge_yml_from_source() {
        let root = test_root("deploy-loads-forge-yml");
        let source_root = root.join("source");
        fs::create_dir_all(&source_root).unwrap();
        fs::write(
            source_root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "\n",
                "build:\n",
                "  dockerfile: deploy/Dockerfile\n",
                "  context: app\n",
                "\n",
                "runtime:\n",
                "  port: 4010\n",
                "  healthcheck:\n",
                "    path: /ready\n",
                "    expected_status: 200\n",
                "\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /ready\n",
                "    expect_status: 200\n",
            ),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root.clone()),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(1, &[("forge-net", "172.18.0.2")]),
        ));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:4010".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("forge-net".into()),
        })
        .execute_next()
        .unwrap();

        let runtime =
            load_generation_runtime_info(&EnvironmentPaths::new(&root, "api", "production"), 1)
                .unwrap()
                .unwrap();
        assert_eq!(runtime.probe_path.as_deref(), Some("/ready"));
        assert_eq!(
            runtime.activation,
            Some(PersistedActivationMode::Http {
                internal_port: 4010,
                route_subtree_id: Some("forge:api:production".into()),
                target_source: PersistedRouteTargetSource::ContainerIp,
            })
        );
        assert_eq!(routing.updates[0].target, "172.18.0.2:4010");
        assert_eq!(
            probes.http_hosts,
            vec![("172.18.0.2".to_string(), 4010, "/ready".to_string())]
        );
    }

    #[test]
    fn git_deploy_uses_forge_yml_runtime_port() {
        let root = test_root("git-deploy-uses-forge-yml-runtime-port");
        let source_root = root.join("source-checkouts").join("api").join("abc123");
        fs::create_dir_all(source_root.join("app")).unwrap();
        fs::create_dir_all(source_root.join("deploy")).unwrap();
        fs::write(
            source_root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "\n",
                "build:\n",
                "  dockerfile: deploy/Dockerfile\n",
                "  context: app\n",
                "\n",
                "runtime:\n",
                "  port: 4010\n",
                "  healthcheck:\n",
                "    path: /ready\n",
                "    expected_status: 200\n",
                "\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /ready\n",
                "    expect_status: 200\n",
            ),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root.clone()),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(1, &[("bridge", "172.18.0.2")]),
        ));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:4010".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("bridge".into()),
        })
        .execute_next()
        .unwrap();

        assert_eq!(routing.updates[0].target, "172.18.0.2:4010");
        assert_eq!(probes.tcp_hosts, vec![("172.18.0.2".to_string(), 4010)]);
        assert_eq!(
            probes.http_hosts,
            vec![("172.18.0.2".to_string(), 4010, "/ready".to_string())]
        );
    }
}

#[cfg(test)]
pub mod runtime_environment_snapshots {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use crate::status::{load_project_environment_env_report, load_project_environment_status};
    use crate::storage::{
        EnvStore, PersistedDesiredEnvConfig, PersistedDesiredEnvDeletedKey,
        PersistedDesiredEnvEntry,
    };
    use crate::storage::{load_generation_resolved_runtime, load_generation_runtime_env_snapshot};
    use std::fs;

    struct StickyRoutingRuntime {
        inspection: RouteInspection,
    }

    impl RoutingRuntime for StickyRoutingRuntime {
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
            Ok(self.inspection.clone())
        }

        fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
            Ok(vec![self.inspection.clone()])
        }

        fn remove_route(&mut self, _subtree_id: &str) -> Result<(), RoutingRuntimeError> {
            Ok(())
        }
    }

    struct InspectOnlyDockerRuntime {
        inspection: ContainerInspection,
    }

    impl DockerRuntime for InspectOnlyDockerRuntime {
        fn build_image(
            &mut self,
            _request: BuildImageRequest,
        ) -> Result<String, DockerRuntimeError> {
            unreachable!()
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
            _request: CreateContainerRequest,
        ) -> Result<String, DockerRuntimeError> {
            unreachable!()
        }

        fn start_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn inspect_container(
            &mut self,
            _container_name: &str,
        ) -> Result<ContainerInspection, DockerRuntimeError> {
            Ok(self.inspection.clone())
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
            Ok(vec![self.inspection.clone()])
        }

        fn list_managed_images(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedImage>, DockerRuntimeError> {
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

    fn write_env_forge_yaml(root: &std::path::Path, extra: &str) {
        fs::write(
            root.join("forge.yml"),
            format!(
                concat!(
                    "version: 1\n",
                    "name: api\n",
                    "type: web\n",
                    "env:\n",
                    "  API_BASE_URL: https://api.example.com\n",
                    "  FORGE_PROJECT_ID: bad-override\n",
                    "  APP_MODE: yaml-value\n",
                    "{extra}",
                    "build:\n",
                    "  dockerfile: Dockerfile\n",
                    "  context: .\n",
                    "runtime:\n",
                    "  port: 3000\n",
                    "  healthcheck:\n",
                    "    path: /health\n",
                    "    expected_status: 200\n",
                    "invariants:\n",
                    "  - name: health\n",
                    "    path: /health\n",
                    "    expect_status: 200\n",
                ),
                extra = extra
            ),
        )
        .unwrap();
    }

    fn write_runtime_policy_forge_yaml(
        root: &std::path::Path,
        cpu_limit: &str,
        memory_limit_mb: u64,
        restart_policy: &str,
        max_retries: u64,
    ) {
        fs::write(
            root.join("forge.yml"),
            format!(
                concat!(
                    "version: 1\n",
                    "name: api\n",
                    "type: web\n",
                    "build:\n",
                    "  dockerfile: Dockerfile\n",
                    "  context: .\n",
                    "runtime:\n",
                    "  cpu:\n",
                    "    limit: \"{cpu_limit}\"\n",
                    "  memory:\n",
                    "    limit_mb: {memory_limit_mb}\n",
                    "  restart:\n",
                    "    policy: {restart_policy}\n",
                    "    max_retries: {max_retries}\n",
                    "  port: 3000\n",
                    "  healthcheck:\n",
                    "    path: /health\n",
                    "    expected_status: 200\n",
                    "invariants:\n",
                    "  - name: health\n",
                    "    path: /health\n",
                    "    expect_status: 200\n",
                ),
                cpu_limit = cpu_limit,
                memory_limit_mb = memory_limit_mb,
                restart_policy = restart_policy,
                max_retries = max_retries,
            ),
        )
        .unwrap();
    }

    fn write_secret_manifest(root: &std::path::Path, env_name: &str, secret_key: &str) {
        fs::write(
            root.join("forge.project.json"),
            format!(
                r#"{{
  "project_id": "api",
  "secrets": {{
    "environment_variables": {{
      "{env_name}": {{
        "scope": "environment",
        "key": "{secret_key}",
        "sensitive": true
      }}
    }}
  }}
}}"#
            ),
        )
        .unwrap();
    }

    fn execute_with_runtime_env(
        root: &std::path::Path,
    ) -> DockerCliRuntime<RecordingCommandRunner> {
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.to_path_buf()),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = StickyRoutingRuntime {
            inspection: RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            },
        };

        DeploymentExecutor::new(
            root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();
        docker
    }

    pub(crate) fn write_desired_env(
        root: &std::path::Path,
        environment: &str,
        entries: &[(&str, &str)],
        deleted_keys: &[&str],
    ) {
        let mut persisted_entries = entries
            .iter()
            .map(|(key, value)| PersistedDesiredEnvEntry {
                key: (*key).to_string(),
                normalized_key: key.to_ascii_lowercase(),
                sealed_value: crate::secrets::seal_value(value).unwrap(),
            })
            .collect::<Vec<_>>();
        persisted_entries.sort_by(|left, right| left.normalized_key.cmp(&right.normalized_key));
        let mut persisted_deleted_keys = deleted_keys
            .iter()
            .map(|key| PersistedDesiredEnvDeletedKey {
                key: (*key).to_string(),
                normalized_key: key.to_ascii_lowercase(),
            })
            .collect::<Vec<_>>();
        persisted_deleted_keys
            .sort_by(|left, right| left.normalized_key.cmp(&right.normalized_key));
        EnvStore::new(root)
            .write_desired_environment(&PersistedDesiredEnvConfig {
                snapshot_version: 1,
                project_id: "api".into(),
                environment: environment.into(),
                updated_at_unix: 1,
                entries: persisted_entries,
                deleted_keys: persisted_deleted_keys,
            })
            .unwrap();
    }

    #[test]
    fn runtime_snapshot_persisted_per_generation() {
        let root = test_root("runtime-snapshot-persisted-per-generation");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");
        write_secret_manifest(&root, "DATABASE_URL", "DATABASE_URL");
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        SecretStore::new(root.join("secrets"))
            .unwrap()
            .write_environment_secret(&crate::secrets::SecretWriteRequest {
                project_id: "api".into(),
                environment: "production".into(),
                key: "DATABASE_URL".into(),
                value: "postgres://snapshot-secret".into(),
            })
            .unwrap();

        execute_with_runtime_env(&root);

        let env = EnvironmentPaths::new(&root, "api", "production");
        let snapshot = load_generation_runtime_env_snapshot(&env, 1)
            .unwrap()
            .unwrap();
        let resolved = load_generation_resolved_runtime(&env, 1).unwrap().unwrap();
        assert_eq!(snapshot.generation, 1);
        assert_eq!(snapshot.deployment_id, "dep-1");
        assert!(
            env.generation_dir(1)
                .join("runtime_env_snapshot.json")
                .exists()
        );
        assert!(env.generation_dir(1).join("resolved_runtime.json").exists());
        assert_eq!(
            snapshot.entries["API_BASE_URL"].value.as_deref(),
            Some("https://api.example.com")
        );
        assert!(snapshot.entries["DATABASE_URL"].redacted);
        assert!(resolved.entries["DATABASE_URL"].sealed_value.is_some());
    }

    #[test]
    fn runtime_policy_persisted_per_generation() {
        let root = test_root("runtime-policy-persisted-per-generation");
        register_project(&root, "api", "api.example.com");
        write_runtime_policy_forge_yaml(&root, "1.5", 512, "on-failure", 4);

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-policy-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.clone()),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("sha-policy-1".into()),
            })
            .unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-policy-2".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.clone()),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("sha-policy-2".into()),
            })
            .unwrap();

        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            success_outputs_with_runtime_policy(
                1,
                &[("forge-test", "172.18.0.2")],
                "1.5",
                512,
                "on-failure",
                Some(4),
            )
            .into_iter()
            .chain(success_outputs_with_runtime_policy(
                2,
                &[("forge-test", "172.18.0.2")],
                "1.5",
                512,
                "on-failure",
                Some(4),
            ))
            .collect(),
        ));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![
                RouteInspection {
                    subtree_id: "forge:api:production".into(),
                    active_target: "172.18.0.2:3000".into(),
                    domain: Some("api.example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                };
                2
            ],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();
        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let first = load_generation_runtime_info(&env, 1).unwrap().unwrap();
        let second = load_generation_runtime_info(&env, 2).unwrap().unwrap();
        for runtime in [&first, &second] {
            assert_eq!(runtime.runtime_policy.cpu_limit.as_deref(), Some("1.5"));
            assert_eq!(runtime.runtime_policy.memory_limit_mb, Some(512));
            assert_eq!(runtime.runtime_policy.restart_policy, "on-failure");
            assert_eq!(runtime.runtime_policy.max_retries, Some(4));
        }
    }

    #[test]
    fn env_snapshot_available_after_successful_deploy() {
        let root = test_root("env-snapshot-available-after-successful-deploy");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");

        execute_with_runtime_env(&root);

        let report = load_project_environment_env_report(&root, "api", "production").unwrap();
        assert_eq!(report.generation, 1);
        assert_eq!(report.deployment_id, "dep-1");
        assert!(
            report
                .values
                .iter()
                .any(|entry| entry.key == "FORGE_DEPLOYMENT_ID" && entry.value == "dep-1")
        );
    }

    #[test]
    fn secret_unset_does_not_mutate_historical_generation() {
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        let root = test_root("secret-unset-does-not-mutate-historical-generation");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");
        write_secret_manifest(&root, "DATABASE_URL", "DATABASE_URL");
        let store = SecretStore::new(root.join("secrets")).unwrap();
        store
            .write_environment_secret(&crate::secrets::SecretWriteRequest {
                project_id: "api".into(),
                environment: "production".into(),
                key: "DATABASE_URL".into(),
                value: "postgres://historical-secret".into(),
            })
            .unwrap();

        execute_with_runtime_env(&root);
        store
            .unset_environment_secret("api", "production", "DATABASE_URL")
            .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let snapshot = load_generation_runtime_env_snapshot(&env, 1)
            .unwrap()
            .unwrap();
        let resolved = load_generation_resolved_runtime(&env, 1).unwrap().unwrap();
        let restored = crate::runtime_env::restore_runtime_env(&resolved).unwrap();

        assert!(snapshot.entries["DATABASE_URL"].redacted);
        assert_eq!(
            restored.get("DATABASE_URL").map(String::as_str),
            Some("postgres://historical-secret")
        );
        assert!(!store.has_environment_secret("api", "production", "DATABASE_URL"));
    }

    #[test]
    fn new_deploy_writes_env_snapshot_and_status_reads_it() {
        let root = test_root("new-deploy-writes-env-snapshot-and-status-reads-it");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.to_path_buf()),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(1, &[("forge-net", "172.19.0.5")]),
        ));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.19.0.5:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("forge-net".into()),
        })
        .execute_next()
        .unwrap();

        let mut status_docker = InspectOnlyDockerRuntime {
            inspection: ContainerInspection {
                container_name: "prod-api-gen-1".into(),
                running: true,
                state_status: "running".into(),
                exit_code: Some(0),
                restart_count: 0,
                started_at: Some("2026-05-21T12:00:00Z".into()),
                finished_at: None,
                oom_killed: false,
                error: None,
                image_ref: "forge/api:production-gen-1".into(),
                labels: BTreeMap::new(),
                network_ips: BTreeMap::from([("forge-net".into(), "172.19.0.5".into())]),
                volume_mounts: Vec::new(),
                restart_policy: "no".into(),
                restart_max_retries: None,
                cpu_limit: None,
                memory_limit_mb: None,
                exit_signal: None,
                termination_reason: None,
            },
        };
        let mut status_routing = StickyRoutingRuntime {
            inspection: RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.19.0.5:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            },
        };
        let status = load_project_environment_status(
            &root,
            None,
            &mut status_docker,
            &mut status_routing,
            "api",
            "production",
        )
        .unwrap();

        assert_eq!(status.status, "healthy");
        assert_eq!(routing.updates[0].target, "172.19.0.5:3000");
        assert_eq!(status.container_ip.as_deref(), Some("172.19.0.5"));
        assert_eq!(
            status
                .runtime_env_snapshot
                .as_ref()
                .map(|snapshot| snapshot.deployment_id.as_str()),
            Some("dep-1")
        );
    }

    #[test]
    fn successful_deploy_persists_env_snapshot_before_promotion() {
        let root = test_root("successful-deploy-persists-env-snapshot-before-promotion");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");

        execute_with_runtime_env(&root);

        let env = EnvironmentPaths::new(&root, "api", "production");
        let snapshot_path = env.generation_dir(1).join("runtime_env_snapshot.json");
        assert!(snapshot_path.exists());
        assert_eq!(
            PointerStore::new(env.clone())
                .read_pointer("current")
                .unwrap(),
            Some(1)
        );

        let diagnostics = DiagnosticsStore::new(env, 1);
        let log_lines = diagnostics.read_log_lines().unwrap();
        let snapshot_line = log_lines
            .iter()
            .position(|line| line.contains("runtime env snapshot written:"))
            .unwrap();
        let promotion_line = log_lines
            .iter()
            .position(|line| line == "generation promoted")
            .unwrap();
        assert!(snapshot_line < promotion_line);
        assert!(log_lines[snapshot_line].contains(snapshot_path.to_string_lossy().as_ref()));
    }

    #[test]
    fn successful_deploy_updates_current_pointer() {
        let root = test_root("successful-deploy-updates-current-pointer");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");

        execute_with_runtime_env(&root);

        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
        assert_eq!(pointers.read_pointer("promoted").unwrap(), Some(1));
        assert_eq!(pointers.read_authoritative_pointer().unwrap(), Some(1));
        assert_eq!(
            RuntimeStateStore::new(env)
                .load()
                .unwrap()
                .active_generation,
            Some(1)
        );
    }

    #[test]
    fn deployment_fails_if_env_snapshot_write_fails() {
        let root = test_root("deployment-fails-if-env-snapshot-write-fails");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");

        let env = EnvironmentPaths::new(&root, "api", "production");
        std::fs::create_dir_all(env.generation_dir(1).join("runtime_env_snapshot.json")).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.to_path_buf()),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = StickyRoutingRuntime {
            inspection: RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            },
        };

        let err = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap_err();

        assert!(err.to_string().contains("runtime_env_snapshot.json"));
        assert_eq!(
            PointerStore::new(env.clone())
                .read_pointer("current")
                .unwrap(),
            None
        );
        assert!(!env.generation_dir(1).join("snapshot.json").exists());
    }

    #[test]
    fn env_resolution_order_is_deterministic() {
        let root = test_root("env-resolution-order-is-deterministic");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");
        write_secret_manifest(&root, "APP_MODE", "APP_MODE");
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        SecretStore::new(root.join("secrets"))
            .unwrap()
            .write_environment_secret(&crate::secrets::SecretWriteRequest {
                project_id: "api".into(),
                environment: "production".into(),
                key: "APP_MODE".into(),
                value: "secret-mode".into(),
            })
            .unwrap();

        let docker = execute_with_runtime_env(&root);
        let create_env = docker.runner.envs[1].clone();
        assert_eq!(create_env["APP_MODE"], "secret-mode");
        assert_eq!(create_env["FORGE_PROJECT_ID"], "api");
    }

    #[test]
    fn desired_env_is_consumed_by_next_deployment_snapshot() {
        let root = test_root("desired-env-is-consumed-by-next-deployment-snapshot");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        write_desired_env(
            &root,
            "production",
            &[("APP_MODE", "desired-mode"), ("EMPTY_VALUE", "")],
            &[],
        );

        let docker = execute_with_runtime_env(&root);
        let create_env = docker.runner.envs[1].clone();
        assert_eq!(create_env["APP_MODE"], "desired-mode");
        assert_eq!(create_env["EMPTY_VALUE"], "");

        let env = EnvironmentPaths::new(&root, "api", "production");
        let snapshot = load_generation_runtime_env_snapshot(&env, 1)
            .unwrap()
            .unwrap();
        let resolved = load_generation_resolved_runtime(&env, 1).unwrap().unwrap();
        assert_eq!(
            snapshot.entries["APP_MODE"].source,
            crate::storage::PersistedRuntimeEnvSource::DesiredEnvConfig
        );
        assert_eq!(
            resolved.entries["APP_MODE"].value.as_deref(),
            Some("desired-mode")
        );
        assert_eq!(snapshot.resolution_order[2], "desired_env_config");
    }

    #[test]
    fn desired_env_does_not_mutate_historical_snapshot_and_applies_to_next_generation() {
        let root = test_root(
            "desired-env-does-not-mutate-historical-snapshot-and-applies-to-next-generation",
        );
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.to_path_buf()),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("aaa111".into()),
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            success_outputs(1)
                .into_iter()
                .chain(success_outputs(2))
                .collect(),
        ));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = StickyRoutingRuntime {
            inspection: RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            },
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let generation_one_snapshot_path = env.generation_dir(1).join("runtime_env_snapshot.json");
        let generation_one_snapshot_before =
            fs::read_to_string(&generation_one_snapshot_path).unwrap();

        write_desired_env(&root, "production", &[("V75_TEST_KEY", "hello-v75")], &[]);
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-2".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.to_path_buf()),
                source_ref: Some("release".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("bbb222".into()),
            })
            .unwrap();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap();

        let generation_one_snapshot_after =
            fs::read_to_string(&generation_one_snapshot_path).unwrap();
        let generation_two_snapshot = load_generation_runtime_env_snapshot(&env, 2)
            .unwrap()
            .unwrap();
        assert_eq!(
            generation_one_snapshot_before,
            generation_one_snapshot_after
        );
        assert!(
            !load_generation_runtime_env_snapshot(&env, 1)
                .unwrap()
                .unwrap()
                .entries
                .contains_key("V75_TEST_KEY")
        );
        assert_eq!(
            generation_two_snapshot.entries["V75_TEST_KEY"]
                .value
                .as_deref(),
            Some("hello-v75")
        );
    }

    #[test]
    fn desired_env_deletion_removes_key_from_next_deployment() {
        let root = test_root("desired-env-deletion-removes-key-from-next-deployment");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");
        write_desired_env(
            &root,
            "production",
            &[("APP_MODE", "configured")],
            &["API_BASE_URL"],
        );

        let docker = execute_with_runtime_env(&root);
        let create_env = docker.runner.envs[1].clone();
        assert_eq!(create_env["APP_MODE"], "configured");
        assert!(!create_env.contains_key("API_BASE_URL"));

        let env = EnvironmentPaths::new(&root, "api", "production");
        let resolved = load_generation_resolved_runtime(&env, 1).unwrap().unwrap();
        assert!(!resolved.entries.contains_key("API_BASE_URL"));
    }

    #[test]
    fn desired_env_rejects_reserved_forge_override() {
        let root = test_root("desired-env-rejects-reserved-forge-override");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        write_desired_env(&root, "production", &[("FORGE_PROJECT_ID", "bad")], &[]);

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.to_path_buf()),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = StickyRoutingRuntime {
            inspection: RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            },
        };

        let err = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next()
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("reserved Forge runtime key cannot be configured or deleted")
        );
        let env = EnvironmentPaths::new(&root, "api", "production");
        assert!(
            !env.generation_dir(1)
                .join("runtime_env_snapshot.json")
                .exists()
        );
    }

    #[test]
    fn generated_forge_vars_injected() {
        let root = test_root("generated-forge-vars-injected");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");

        let docker = execute_with_runtime_env(&root);
        let create_env = docker.runner.envs[1].clone();
        assert_eq!(create_env["FORGE_PROJECT_ID"], "api");
        assert_eq!(create_env["FORGE_ENVIRONMENT"], "production");
        assert_eq!(create_env["FORGE_GENERATION"], "1");
        assert_eq!(create_env["FORGE_DEPLOYMENT_ID"], "dep-1");
        assert_eq!(create_env["FORGE_COMMIT_SHA"], "abc123");
        assert_eq!(create_env["FORGE_SOURCE_REF"], "main");
        assert_eq!(create_env["FORGE_DOMAIN"], "api.example.com");
    }

    #[test]
    fn runtime_container_receives_generated_vars() {
        let root = test_root("runtime-container-receives-generated-vars");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");

        let docker = execute_with_runtime_env(&root);
        let create_env = docker.runner.envs[1].clone();
        assert!(create_env.contains_key("FORGE_PROJECT_ID"));
        assert!(create_env.contains_key("FORGE_ENVIRONMENT"));
        assert!(create_env.contains_key("FORGE_GENERATION"));
        assert!(create_env.contains_key("FORGE_DEPLOYMENT_ID"));
        assert!(create_env.contains_key("FORGE_COMMIT_SHA"));
        assert!(create_env.contains_key("FORGE_SOURCE_REF"));
        assert!(create_env.contains_key("FORGE_DOMAIN"));
    }

    #[test]
    fn diagnostics_redacts_secret_values() {
        let root = test_root("diagnostics-redacts-secret-values");
        register_project(&root, "api", "api.example.com");
        write_env_forge_yaml(&root, "");
        write_secret_manifest(&root, "DATABASE_URL", "DATABASE_URL");
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            );
        }
        SecretStore::new(root.join("secrets"))
            .unwrap()
            .write_environment_secret(&crate::secrets::SecretWriteRequest {
                project_id: "api".into(),
                environment: "production".into(),
                key: "DATABASE_URL".into(),
                value: "postgres://diagnostic-secret".into(),
            })
            .unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.clone()),
                source_ref: Some("main".into()),
                repo_url: None,
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .execute_next();

        let diagnostics =
            fs::read_to_string(root.join(
                "projects/api/environments/production/generations/1/diagnostics/summary.json",
            ))
            .unwrap();
        assert!(diagnostics.contains("DATABASE_URL=[REDACTED]"));
        assert!(!diagnostics.contains("postgres://diagnostic-secret"));
    }
}

#[cfg(test)]
pub mod git_deploy_non_api_project_staging {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn git_deploy_non_api_project_staging_validates_health() {
        let root = test_root("git-deploy-non-api-project-staging");
        let source_root = root.join("source-checkouts").join("web").join("abc123");
        std::fs::create_dir_all(source_root.join("deploy")).unwrap();
        std::fs::write(
            source_root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: web\n",
                "type: web\n",
                "build:\n",
                "  dockerfile: deploy/Dockerfile\n",
                "  context: .\n",
                "runtime:\n",
                "  port: 4100\n",
                "  healthcheck:\n",
                "    path: /healthz\n",
                "    expected_status: 200\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /healthz\n",
                "    expect_status: 200\n",
            ),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "web".into(),
                environment: "staging".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: Some("release".into()),
                repo_url: Some("https://github.com/example/web.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            "image_ref=forge/web:staging-gen-1".into(),
            String::new(),
            "staging-web-gen-1".into(),
            String::new(),
            [
                "name=/staging-web-gen-1",
                "status=running",
                "running=true",
                "exit_code=0",
                "image=forge/web:staging-gen-1",
                "restart_policy=no",
                "network:forge-net=172.19.0.8",
            ]
            .join("\n"),
            [
                "name=/staging-web-gen-1",
                "status=running",
                "running=true",
                "exit_code=0",
                "image=forge/web:staging-gen-1",
                "restart_policy=no",
                "network:forge-net=172.19.0.8",
            ]
            .join("\n"),
            [
                "name=/staging-web-gen-1",
                "status=running",
                "running=true",
                "exit_code=0",
                "image=forge/web:staging-gen-1",
                "restart_policy=no",
                "network:forge-net=172.19.0.8",
            ]
            .join("\n"),
            String::new(),
        ]));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:web:staging".into(),
                active_target: "172.19.0.8:4100".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        let execution = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("forge-net".into()),
        })
        .execute_next()
        .unwrap()
        .unwrap();

        assert_eq!(execution.container_name, "staging-web-gen-1");
        assert_eq!(probes.tcp_hosts, vec![("172.19.0.8".to_string(), 4100)]);
        assert_eq!(
            probes.http_hosts,
            vec![("172.19.0.8".to_string(), 4100, "/healthz".to_string())]
        );
        assert_eq!(routing.updates[0].subtree_id, "forge:web:staging");
        assert_eq!(routing.updates[0].target, "172.19.0.8:4100");
    }

    #[test]
    fn route_activation_uses_generated_staging_domain() {
        let root = test_root("route-activation-uses-generated-staging-domain");
        register_project(&root, "web", "web.forge.example.com");
        let source_root = root.join("source-checkouts").join("web").join("abc123");
        std::fs::create_dir_all(source_root.join("deploy")).unwrap();
        std::fs::write(
            source_root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: web\n",
                "type: web\n",
                "build:\n",
                "  dockerfile: deploy/Dockerfile\n",
                "  context: .\n",
                "runtime:\n",
                "  port: 4100\n",
                "  healthcheck:\n",
                "    path: /healthz\n",
                "    expected_status: 200\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /healthz\n",
                "    expect_status: 200\n",
            ),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "web".into(),
                environment: "staging".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: Some("release".into()),
                repo_url: Some("https://github.com/example/web.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            "image_ref=forge/web:staging-gen-1".into(),
            String::new(),
            "staging-web-gen-1".into(),
            String::new(),
            [
                "name=/staging-web-gen-1",
                "status=running",
                "running=true",
                "exit_code=0",
                "image=forge/web:staging-gen-1",
                "restart_policy=no",
                "network:forge-net=172.19.0.8",
            ]
            .join("\n"),
            [
                "name=/staging-web-gen-1",
                "status=running",
                "running=true",
                "exit_code=0",
                "image=forge/web:staging-gen-1",
                "restart_policy=no",
                "network:forge-net=172.19.0.8",
            ]
            .join("\n"),
            [
                "name=/staging-web-gen-1",
                "status=running",
                "running=true",
                "exit_code=0",
                "image=forge/web:staging-gen-1",
                "restart_policy=no",
                "network:forge-net=172.19.0.8",
            ]
            .join("\n"),
            String::new(),
        ]));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:web:staging".into(),
                active_target: "172.19.0.8:4100".into(),
                domain: Some("staging-web.forge.example.com".into()),
                activation_verified: true,
                verification_url: Some("http://127.0.0.1/healthz".into()),
                verification_host: Some("staging-web.forge.example.com".into()),
                verification_status_code: Some(200),
                verification_response_body: Some("ok".into()),
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("forge-net".into()),
        })
        .execute_next()
        .unwrap()
        .unwrap();

        assert_eq!(
            routing.updates[0].domain.as_deref(),
            Some("staging-web.forge.example.com")
        );
    }
}

#[cfg(test)]
pub mod tcp_probe_failure_target_context {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use std::fs;

    #[test]
    fn tcp_probe_failure_records_target_context() {
        let root = test_root("tcp-probe-failure-target-context");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(1, &[("bridge", "172.18.0.2")]),
        ));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("bridge".into()),
        })
        .execute_next();

        let summary =
            fs::read_to_string(root.join(
                "projects/api/environments/production/generations/1/diagnostics/summary.json",
            ))
            .unwrap();
        let logs =
            fs::read_to_string(root.join(
                "projects/api/environments/production/generations/1/diagnostics/deployment.log",
            ))
            .unwrap();

        assert!(summary.contains("\"probe_target_host\": \"172.18.0.2\""));
        assert!(summary.contains("\"probe_target_port\": 3000"));
        assert!(logs.contains("probe target: host=172.18.0.2 port=3000"));
    }
}

#[cfg(test)]
pub mod tcp_probe_failure_preserves_container_logs {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use std::fs;

    #[test]
    fn tcp_probe_failure_preserves_container_logs() {
        let root = test_root("tcp-probe-failure-preserves-container-logs");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "build:\n",
                "  dockerfile: Dockerfile\n",
                "  context: .\n",
                "runtime:\n",
                "  port: 3000\n",
                "  healthcheck:\n",
                "    path: /health\n",
                "    expected_status: 200\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /health\n",
                "    expect_status: 200\n",
                "    timeout_ms: 50\n",
            ),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            format!("image_ref=forge/api:production-gen-1"),
            String::new(),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output(1, "running", true, 0, &[("bridge", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("bridge", "172.18.0.3")]),
            inspection_output(1, "running", true, 0, &[("bridge", "172.18.0.3")]),
            inspection_output(1, "running", true, 0, &[("bridge", "172.18.0.3")]),
            inspection_output(1, "running", true, 0, &[("bridge", "172.18.0.3")]),
            "boot line\nlisten failed".into(),
        ]));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("bridge".into()),
        })
        .execute_next();

        let diagnostics = fs::read_to_string(
            root.join(
                "projects/api/environments/production/generations/1/diagnostics/validation_failure.json",
            ),
        )
        .unwrap();
        assert!(diagnostics.contains("boot line"));
        assert!(diagnostics.contains("listen failed"));
    }
}

#[cfg(test)]
pub mod tcp_probe_failure_records_network_map {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use std::fs;

    #[test]
    fn tcp_probe_failure_records_network_map() {
        let root = test_root("tcp-probe-failure-records-network-map");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            format!("image_ref=forge/api:production-gen-1"),
            String::new(),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[("bridge", "172.17.0.4"), ("forge-net", "172.19.0.5")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[("bridge", "172.17.0.4"), ("forge-net", "172.19.0.6")],
            ),
            inspection_output(
                1,
                "running",
                true,
                0,
                &[("bridge", "172.17.0.4"), ("forge-net", "172.19.0.6")],
            ),
            "readying".into(),
        ]));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("forge-net".into()),
        })
        .execute_next();

        let diagnostics = fs::read_to_string(
            root.join(
                "projects/api/environments/production/generations/1/diagnostics/validation_failure.json",
            ),
        )
        .unwrap();
        assert!(diagnostics.contains("\"forge-net\": \"172.19.0.6\""));
        assert!(diagnostics.contains("\"bridge\": \"172.17.0.4\""));
    }
}

#[cfg(test)]
pub mod tcp_probe_reinspects_container_before_probe {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn tcp_probe_reinspects_container_before_probe() {
        let root = test_root("tcp-probe-reinspect-before-probe");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            format!("image_ref=forge/api:production-gen-1"),
            String::new(),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output(1, "running", true, 0, &[("bridge", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("bridge", "172.18.0.7")]),
            inspection_output(1, "running", true, 0, &[("bridge", "172.18.0.7")]),
            String::new(),
        ]));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("bridge".into()),
        })
        .execute_next()
        .unwrap();

        assert_eq!(probes.tcp_hosts, vec![("172.18.0.7".to_string(), 3000)]);
    }
}

#[cfg(test)]
pub mod exited_container_reports_exit_state_not_generic_tcp_failure {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use crate::storage::DiagnosticsStore;
    use std::fs;

    #[test]
    fn exited_container_reports_exit_state_not_generic_tcp_failure() {
        let root = test_root("exited-container-reports-exit-state");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "build:\n",
                "  dockerfile: Dockerfile\n",
                "  context: .\n",
                "runtime:\n",
                "  port: 3000\n",
                "  healthcheck:\n",
                "    path: /health\n",
                "    expected_status: 200\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /health\n",
                "    expect_status: 200\n",
                "    timeout_ms: 50\n",
            ),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            format!("image_ref=forge/api:production-gen-1"),
            String::new(),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output(1, "running", true, 0, &[("bridge", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("bridge", "172.18.0.2")]),
            inspection_output(1, "exited", false, 137, &[("bridge", "172.18.0.2")]),
            inspection_output(1, "exited", false, 137, &[("bridge", "172.18.0.2")]),
            "panic: bind failed".into(),
        ]));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("bridge".into()),
        })
        .execute_next();

        assert!(matches!(result, Err(DeploymentError::ValidationFailed(_))));
        let diagnostics =
            DiagnosticsStore::new(EnvironmentPaths::new(&root, "api", "production"), 1);
        let reason = diagnostics.read_failure_reason().unwrap().unwrap();
        assert!(reason.contains("container exited before tcp probe"));
        assert!(reason.contains("exit_code=137"));
        assert!(!reason.contains("tcp probe failed"));

        let summary =
            fs::read_to_string(root.join(
                "projects/api/environments/production/generations/1/diagnostics/summary.json",
            ))
            .unwrap();
        assert!(summary.contains("container exited before tcp probe"));
    }
}

#[cfg(test)]
pub mod deploy_from_path_uses_project_directory {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn deploy_from_path_uses_project_directory() {
        let root = test_root("deploy-from-path-project-directory");
        let source_root = root.join("source");
        std::fs::create_dir_all(&source_root).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root.clone()),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: None,
        })
        .execute_next()
        .unwrap();

        let build_args = &docker.runner.commands[0].args;
        assert!(
            build_args
                .iter()
                .any(|arg| arg == &source_root.display().to_string())
        );
        assert!(
            build_args
                .windows(2)
                .any(|pair| pair == ["-f", &source_root.join("Dockerfile").display().to_string()])
        );
    }
}

#[cfg(test)]
pub mod deploy_without_from_preserves_working_directory_behavior {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn deploy_without_from_preserves_working_directory_behavior() {
        let root = test_root("deploy-without-from-working-directory");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: None,
        })
        .execute_next()
        .unwrap();

        let build_args = &docker.runner.commands[0].args;
        assert!(
            build_args
                .iter()
                .any(|arg| arg == &root.display().to_string())
        );
        assert!(
            build_args
                .windows(2)
                .any(|pair| pair == ["-f", &root.join("Dockerfile").display().to_string()])
        );
    }
}

#[cfg(test)]
pub mod deploy_by_ref_preserves_existing_deploy_fsm {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn deploy_by_ref_preserves_existing_deploy_fsm() {
        let root = test_root("deploy-by-ref-preserves-fsm");
        let source_root = root.join("source-checkouts").join("api").join("abc123");
        std::fs::create_dir_all(&source_root).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: None,
        })
        .execute_next()
        .unwrap();

        let event_types = EventStore::list_all(&root)
            .unwrap()
            .into_iter()
            .filter(|event| event.deployment_id.as_deref() == Some("dep-1"))
            .map(|event| event.event_type)
            .collect::<Vec<_>>();
        assert_eq!(
            event_types,
            vec![
                "DEPLOYMENT_STARTED",
                "IMAGE_BUILT",
                "RUNTIME_ENV_PREPARED",
                "CONTAINER_STARTED",
                "VALIDATION_PASSED",
                "SNAPSHOT_FINALIZED",
                "GENERATION_PROMOTED",
            ]
        );
    }
}

#[cfg(test)]
pub mod deployment_metadata_records_commit_sha {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use crate::storage::{load_generation_build_info, load_generation_runtime_info};

    #[test]
    fn deployment_metadata_records_commit_sha() {
        let root = test_root("deployment-metadata-records-commit");
        let source_root = root.join("source-checkouts").join("api").join("abc123");
        std::fs::create_dir_all(&source_root).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source_root.clone()),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: None,
        })
        .execute_next()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let build = load_generation_build_info(&env, 1).unwrap().unwrap();
        let runtime = load_generation_runtime_info(&env, 1).unwrap().unwrap();

        assert_eq!(build.commit_sha.as_deref(), Some("abc123"));
        assert_eq!(build.source_ref.as_deref(), Some("main"));
        assert_eq!(build.source_path.as_ref(), Some(&source_root));
        assert_eq!(runtime.commit_sha.as_deref(), Some("abc123"));
        assert_eq!(runtime.source_ref.as_deref(), Some("main"));
        assert_eq!(runtime.source_path.as_ref(), Some(&source_root));
    }
}

#[cfg(test)]
pub mod deploy_rejects_invalid_yaml {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use std::fs;

    #[test]
    fn deploy_rejects_invalid_yaml() {
        let root = test_root("deploy-invalid-forge-yaml");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "build:\n",
                "  dockerfile: Dockerfile\n",
                "  context: .\n",
                "runtime:\n",
                "  port: 3000\n",
                "  healthcheck:\n",
                "    path /health\n",
                "    expected_status: 200\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /health\n",
                "    expect_status: 200\n",
            ),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: None,
        })
        .execute_next();

        assert!(
            matches!(result, Err(DeploymentError::InvalidInspection(message)) if message.contains("invalid forge.yml"))
        );
    }
}

#[cfg(test)]
pub mod deploy_rejects_missing_required_fields {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use std::fs;

    #[test]
    fn deploy_rejects_missing_required_fields() {
        let root = test_root("deploy-missing-forge-yaml-field");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "build:\n",
                "  dockerfile: Dockerfile\n",
                "  context: .\n",
                "runtime:\n",
                "  healthcheck:\n",
                "    path: /health\n",
                "    expected_status: 200\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /health\n",
                "    expect_status: 200\n",
            ),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: None,
        })
        .execute_next();

        assert!(
            matches!(result, Err(DeploymentError::InvalidInspection(message)) if
                message.contains("runtime.port") || message.contains("missing field `port`"))
        );
    }
}

#[cfg(test)]
pub mod deploy_uses_runtime_port_from_yaml {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use std::fs;

    #[test]
    fn deploy_uses_runtime_port_from_yaml() {
        let root = test_root("deploy-runtime-port-from-yaml");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "build:\n",
                "  dockerfile: Dockerfile\n",
                "  context: .\n",
                "runtime:\n",
                "  port: 4010\n",
                "  healthcheck:\n",
                "    path: /health\n",
                "    expected_status: 200\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /health\n",
                "    expect_status: 200\n",
            ),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(1, &[("forge-net", "172.18.0.2")]),
        ));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:4010".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("forge-net".into()),
        })
        .execute_next()
        .unwrap();

        assert_eq!(probes.tcp_hosts, vec![("172.18.0.2".to_string(), 4010)]);
        assert_eq!(routing.updates[0].target, "172.18.0.2:4010");
    }
}

#[cfg(test)]
pub mod deploy_uses_healthcheck_from_yaml {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use std::fs;

    #[test]
    fn deploy_uses_healthcheck_from_yaml() {
        let root = test_root("deploy-healthcheck-from-yaml");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "build:\n",
                "  dockerfile: Dockerfile\n",
                "  context: .\n",
                "runtime:\n",
                "  port: 3000\n",
                "  healthcheck:\n",
                "    path: /livez\n",
                "    expected_status: 200\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /livez\n",
                "    expect_status: 200\n",
            ),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("forge-test".into()),
        })
        .execute_next()
        .unwrap();

        assert_eq!(
            probes.http_hosts,
            vec![("172.18.0.2".to_string(), 3000, "/livez".to_string())]
        );
        assert_eq!(routing.updates[0].probe_path.as_deref(), Some("/livez"));
    }
}

#[cfg(test)]
pub mod route_updates_only_after_snapshot_finalized {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn route_update_happens_after_snapshot_exists() {
        let root = test_root("route-after-finalize");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .execute_next()
        .unwrap();

        assert!(
            root.join("projects/api/environments/production/generations/1/snapshot.json")
                .exists()
        );
        assert_eq!(routing.updates.len(), 1);
    }
}

#[cfg(test)]
pub mod route_targets_inspected_container_ip {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn route_target_points_to_inspected_container_ip() {
        let root = test_root("route-target");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .execute_next()
        .unwrap();

        assert_eq!(routing.updates[0].target, "172.18.0.2:3000");
    }
}

#[cfg(test)]
pub mod managed_container_attached_to_forge_network {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn create_container_uses_forge_managed_network() {
        let root = test_root("managed-container-attached-to-forge-network");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(
                1,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.18.0.2")],
            ),
        ));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        assert!(docker.runner.commands.iter().any(|command| {
            let args = command.args.iter().map(String::as_str).collect::<Vec<_>>();
            args == vec!["network", "inspect", FORGE_MANAGED_DOCKER_NETWORK]
                || args == vec!["network", "create", FORGE_MANAGED_DOCKER_NETWORK]
        }));
        assert!(docker.runner.commands.iter().any(|command| {
            command
                .args
                .windows(2)
                .any(|pair| pair == ["--network", FORGE_MANAGED_DOCKER_NETWORK])
        }));
    }
}

#[cfg(test)]
pub mod probe_uses_forge_network_ip {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn validation_probes_use_selected_forge_network_ip() {
        let root = test_root("probe-uses-forge-network-ip");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(
                1,
                &[
                    ("bridge", "172.17.0.4"),
                    (FORGE_MANAGED_DOCKER_NETWORK, "172.19.0.6"),
                ],
            ),
        ));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Direct,
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        assert_eq!(probes.tcp_hosts, vec![("172.19.0.6".to_string(), 3000)]);
        assert_eq!(
            probes.http_hosts,
            vec![("172.19.0.6".to_string(), 3000, "/health".to_string())]
        );
    }
}

#[cfg(test)]
pub mod caddy_route_uses_same_network_reachable_ip {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn route_activation_uses_same_forge_network_ip_as_validation() {
        let root = test_root("caddy-route-uses-same-network-reachable-ip");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(
                1,
                &[
                    ("bridge", "172.17.0.4"),
                    (FORGE_MANAGED_DOCKER_NETWORK, "172.19.0.6"),
                ],
            ),
        ));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.19.0.6:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        assert_eq!(probes.tcp_hosts, vec![("172.19.0.6".to_string(), 3000)]);
        assert_eq!(routing.updates[0].target, "172.19.0.6:3000");
    }

    #[test]
    fn route_activation_uses_same_target_as_validation() {
        let root = test_root("route-activation-uses-same-target-as-validation");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(
                1,
                &[
                    ("bridge", "172.17.0.4"),
                    (FORGE_MANAGED_DOCKER_NETWORK, "172.19.0.6"),
                ],
            ),
        ));
        let mut probes = RecordingProbeRuntime {
            tcp_ok: true,
            http_ok: true,
            ..Default::default()
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.19.0.6:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        let validation_host = &probes.tcp_hosts[0].0;
        assert_eq!(routing.updates[0].target, format!("{validation_host}:3000"));
    }
}

#[cfg(test)]
pub mod bridge_ip_unreachable_diagnostic_is_clear {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use std::fs;

    #[test]
    fn failure_logs_explain_bridge_reachability_mismatch() {
        let root = test_root("bridge-ip-unreachable-diagnostic-is-clear");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            format!("image_ref=forge/api:production-gen-1"),
            String::new(),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output(1, "running", true, 0, &[("bridge", "172.17.0.4")]),
            inspection_output(1, "running", true, 0, &[("bridge", "172.17.0.4")]),
            inspection_output(1, "running", true, 0, &[("bridge", "172.17.0.4")]),
            "Server is running on 0.0.0.0:3000".into(),
        ]));
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("bridge".into()),
        })
        .execute_next();

        let logs =
            fs::read_to_string(root.join(
                "projects/api/environments/production/generations/1/diagnostics/deployment.log",
            ))
            .unwrap();
        assert!(logs.contains("selected docker network is bridge"));
        assert!(logs.contains("not assumed reachable from the Forge daemon host or Caddy"));
        assert!(logs.contains(FORGE_MANAGED_DOCKER_NETWORK));
    }
}

#[cfg(test)]
pub mod route_activation_failure_rolls_back_pointer {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn current_pointer_remains_on_previous_generation_if_activation_fails() {
        let root = test_root("route-activation-failure");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let writer1 = SnapshotWriter::new(env.clone(), 1).unwrap();
        writer1
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        crate::storage::atomic_write(env.generation_counter(), b"1\n").unwrap();
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(2)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: None,
                activation_verified: false,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .execute_next();

        assert!(matches!(
            result,
            Err(DeploymentError::ValidationFailed(
                "route activation verification failed"
            ))
        ));
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
    }
}

#[cfg(test)]
pub mod route_activation_failure_diagnostics {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;
    use std::fs;

    #[test]
    fn route_activation_failure_records_route_diagnostics() {
        let root = test_root("route-activation-failure-records-route-diagnostics");
        register_project(&root, "api", "api.example.com");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(1, &[("forge-net", "172.18.0.2")]),
        ));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: false,
                verification_url: Some("http://127.0.0.1:8080/health".into()),
                verification_host: Some("api.example.com".into()),
                verification_status_code: Some(404),
                verification_response_body: Some("stale route".into()),
                health_checks_enabled: false,
            }],
        };

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some("forge-net".into()),
        })
        .execute_next();

        assert!(matches!(
            result,
            Err(DeploymentError::ValidationFailed(
                "route activation verification failed"
            ))
        ));

        let artifact = fs::read_to_string(root.join(
            "projects/api/environments/production/generations/1/diagnostics/route_activation_failure.json",
        ))
        .unwrap();
        assert!(artifact.contains("\"route_id\": \"forge:api:production\""));
        assert!(artifact.contains("\"domain\": \"api.example.com\""));
        assert!(artifact.contains("\"upstream_target\": \"172.18.0.2:3000\""));
        assert!(artifact.contains("\"verification_url\": \"http://127.0.0.1:8080/health\""));
        assert!(artifact.contains("\"verification_host\": \"api.example.com\""));
        assert!(artifact.contains("\"verification_status_code\": 404"));
        assert!(artifact.contains("\"verification_response_body\": \"stale route\""));
    }

    #[test]
    fn caddy_network_reachability_is_verified_or_documented() {
        let root = test_root("caddy-network-reachability-is-verified-or-documented");
        register_project(&root, "api", "api.example.com");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            networked_success_outputs_with_network(
                1,
                &[(FORGE_MANAGED_DOCKER_NETWORK, "172.18.0.2")],
            ),
        ));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: false,
                verification_url: Some("http://127.0.0.1:8080/health".into()),
                verification_host: Some("api.example.com".into()),
                verification_status_code: Some(502),
                verification_response_body: Some("bad gateway".into()),
                health_checks_enabled: false,
            }],
        };

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: root.clone(),
            dockerfile_path: root.join("Dockerfile"),
            network_name: Some(FORGE_MANAGED_DOCKER_NETWORK.into()),
        })
        .execute_next();

        let artifact = fs::read_to_string(root.join(
            "projects/api/environments/production/generations/1/diagnostics/route_activation_failure.json",
        ))
        .unwrap();
        assert!(artifact.contains(FORGE_MANAGED_DOCKER_NETWORK));
        assert!(artifact.contains("Caddy is attached to docker network"));
    }
}

#[cfg(test)]
pub mod caddy_health_checks_are_not_enabled {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn route_update_disables_caddy_health_checks() {
        let root = test_root("route-health-disabled");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .execute_next()
        .unwrap();

        assert!(!routing.updates[0].health_checks_enabled);
    }
}

#[cfg(test)]
pub mod forge_owns_only_dedicated_route_subtree {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn route_update_uses_forge_owned_subtree_id() {
        let root = test_root("route-subtree");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.18.0.2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
                ..ValidationPolicy::default()
            },
        )
        .execute_next()
        .unwrap();

        assert_eq!(routing.updates[0].subtree_id, "forge:api:production");
    }
}

#[cfg(test)]
pub mod multi_service_orchestration {
    use super::*;
    use crate::storage::{
        PersistedActivationMode, PersistedServiceState, load_generation_build_info,
        load_generation_runtime_info,
    };
    use std::collections::BTreeMap;
    use std::fs;

    #[derive(Default)]
    struct MultiServiceDockerRuntime {
        containers: BTreeMap<String, bool>,
        container_images: BTreeMap<String, String>,
        container_logs: BTreeMap<String, String>,
        volumes: BTreeMap<String, BTreeMap<String, String>>,
        build_requests: Vec<BuildImageRequest>,
        ensured_volumes: Vec<crate::runtime::CreateVolumeRequest>,
        created: Vec<String>,
        create_requests: Vec<CreateContainerRequest>,
        started: Vec<String>,
        removed_volumes: Vec<String>,
    }

    impl DockerRuntime for MultiServiceDockerRuntime {
        fn build_image(
            &mut self,
            request: BuildImageRequest,
        ) -> Result<String, DockerRuntimeError> {
            self.build_requests.push(request.clone());
            Ok(request.image_tag)
        }

        fn ensure_network(&mut self, _network_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn ensure_volume(
            &mut self,
            request: crate::runtime::CreateVolumeRequest,
        ) -> Result<(), DockerRuntimeError> {
            self.volumes
                .entry(request.volume_name.clone())
                .or_insert(request.labels.clone());
            self.ensured_volumes.push(request);
            Ok(())
        }

        fn create_container(
            &mut self,
            request: CreateContainerRequest,
        ) -> Result<String, DockerRuntimeError> {
            self.create_requests.push(request.clone());
            self.created.push(request.container_name.clone());
            self.container_images
                .insert(request.container_name.clone(), request.image_ref.clone());
            self.containers
                .insert(request.container_name.clone(), false);
            Ok(request.container_name)
        }

        fn start_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError> {
            self.started.push(container_name.into());
            self.containers.insert(container_name.into(), true);
            Ok(())
        }

        fn inspect_container(
            &mut self,
            container_name: &str,
        ) -> Result<ContainerInspection, DockerRuntimeError> {
            let running = *self
                .containers
                .get(container_name)
                .ok_or_else(|| DockerRuntimeError::InvalidResponse("missing container".into()))?;
            let service = container_name
                .split("-gen-")
                .next()
                .and_then(|value| value.rsplit_once('-').map(|(_, service)| service))
                .unwrap_or("api");
            let ip = match service {
                "redis" => "172.18.0.11",
                "api" => "172.18.0.12",
                "worker" => "172.18.0.13",
                _ => "172.18.0.14",
            };
            let image_ref = self
                .container_images
                .get(container_name)
                .cloned()
                .unwrap_or_else(|| "forge/api:production-gen-1".into());
            Ok(ContainerInspection {
                container_name: container_name.into(),
                running,
                state_status: if running {
                    "running".into()
                } else {
                    "created".into()
                },
                exit_code: Some(0),
                restart_count: 0,
                started_at: Some("2026-05-22T00:00:00Z".into()),
                finished_at: None,
                oom_killed: false,
                error: None,
                image_ref,
                labels: BTreeMap::new(),
                network_ips: BTreeMap::from([(FORGE_MANAGED_DOCKER_NETWORK.into(), ip.into())]),
                volume_mounts: request_volume_mounts(&self.create_requests, container_name),
                restart_policy: "no".into(),
                restart_max_retries: None,
                cpu_limit: None,
                memory_limit_mb: None,
                exit_signal: None,
                termination_reason: None,
            })
        }

        fn container_logs(
            &mut self,
            container_name: &str,
            _tail_lines: usize,
        ) -> Result<String, DockerRuntimeError> {
            Ok(self
                .container_logs
                .get(container_name)
                .cloned()
                .unwrap_or_default())
        }

        fn list_managed_containers(
            &mut self,
        ) -> Result<Vec<ContainerInspection>, DockerRuntimeError> {
            Ok(Vec::new())
        }

        fn list_managed_images(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedImage>, DockerRuntimeError> {
            Ok(Vec::new())
        }

        fn list_managed_volumes(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedVolume>, DockerRuntimeError> {
            Ok(self
                .volumes
                .iter()
                .map(|(volume_name, labels)| crate::runtime::ManagedVolume {
                    volume_name: volume_name.clone(),
                    labels: labels.clone(),
                })
                .collect())
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

        fn remove_volume(&mut self, volume_name: &str) -> Result<(), DockerRuntimeError> {
            self.removed_volumes.push(volume_name.to_string());
            self.volumes.remove(volume_name);
            Ok(())
        }
    }

    fn request_volume_mounts(
        requests: &[CreateContainerRequest],
        container_name: &str,
    ) -> Vec<crate::runtime::ContainerVolumeMount> {
        requests
            .iter()
            .rev()
            .find(|request| request.container_name == container_name)
            .map(|request| {
                request
                    .volume_mounts
                    .iter()
                    .map(|mount| crate::runtime::ContainerVolumeMount {
                        volume_name: mount.volume_name.clone(),
                        mount_path: mount.mount_path.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[derive(Default)]
    struct HostProbeRuntime {
        unhealthy_hosts: Vec<String>,
    }

    impl ProbeRuntime for HostProbeRuntime {
        fn probe_tcp(&mut self, host: &str, _internal_port: u16) -> Result<bool, ProbeError> {
            Ok(!self.unhealthy_hosts.iter().any(|value| value == host))
        }

        fn probe_http(
            &mut self,
            host: &str,
            _internal_port: u16,
            _path: &str,
        ) -> Result<bool, ProbeError> {
            Ok(!self.unhealthy_hosts.iter().any(|value| value == host))
        }
    }

    fn write_multi_service_forge_yaml(root: &std::path::Path, worker_port: Option<u16>) {
        let worker_runtime = worker_port.map_or_else(
            || "    runtime:\n      command: node worker.js\n".to_string(),
            |port| format!("    runtime:\n      command: node worker.js\n      port: {port}\n"),
        );
        fs::write(
            root.join("forge.yml"),
            format!(
                "{}{}",
                concat!(
                    "version: 1\n",
                    "name: api\n",
                    "type: web\n",
                    "build:\n",
                    "  dockerfile: Dockerfile\n",
                    "  context: .\n",
                    "services:\n",
                    "  redis:\n",
                    "    runtime:\n",
                    "      image: redis:7\n",
                    "  api:\n",
                    "    runtime:\n",
                    "      port: 3000\n",
                    "      depends_on:\n",
                    "        - redis\n",
                    "  worker:\n",
                ),
                format!(
                    "{}{}",
                    worker_runtime,
                    concat!("      depends_on:\n", "        - api\n",)
                )
            ),
        )
        .unwrap();
    }

    fn write_stateful_multi_service_forge_yaml(root: &std::path::Path, retention: &str) {
        fs::write(
            root.join("forge.yml"),
            format!(
                concat!(
                    "version: 1\n",
                    "name: api\n",
                    "type: web\n",
                    "build:\n",
                    "  dockerfile: Dockerfile\n",
                    "  context: .\n",
                    "services:\n",
                    "  postgres:\n",
                    "    runtime:\n",
                    "      image: postgres:16\n",
                    "    state:\n",
                    "      volume: postgres-data\n",
                    "      mount_path: /var/lib/postgresql/data\n",
                    "      retention: {retention}\n",
                    "  api:\n",
                    "    runtime:\n",
                    "      port: 3000\n",
                    "      depends_on:\n",
                    "        - postgres\n",
                ),
                retention = retention
            ),
        )
        .unwrap();
    }

    fn write_invalid_multi_service_forge_yaml(root: &std::path::Path) {
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "build:\n",
                "  dockerfile: Dockerfile\n",
                "  context: .\n",
                "services:\n",
                "  api:\n",
                "    runtime:\n",
                "      port: 3000\n",
                "      depends_on:\n",
                "        - worker\n",
                "  worker:\n",
                "    runtime:\n",
                "      depends_on:\n",
                "        - api\n",
            ),
        )
        .unwrap();
    }

    fn write_service_build_forge_yaml(root: &std::path::Path) {
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "services:\n",
                "  api:\n",
                "    build:\n",
                "      dockerfile: Dockerfile\n",
                "      context: .\n",
                "    runtime:\n",
                "      port: 3000\n",
                "      healthcheck:\n",
                "        path: /health\n",
                "        expected_status: 200\n",
                "    expose: true\n",
                "  worker:\n",
                "    build:\n",
                "      dockerfile: Dockerfile.worker\n",
                "      context: .\n",
                "    runtime:\n",
                "      command: node worker.js\n",
                "    depends_on:\n",
                "      - api\n",
                "    expose: false\n",
            ),
        )
        .unwrap();
    }

    fn deploy_once_with_stateful_yaml(
        root: &std::path::Path,
        docker: &mut MultiServiceDockerRuntime,
        deployment_id: &str,
    ) {
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: deployment_id.into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production:api".into(),
                active_target: "172.18.0.12:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            root,
            &queue,
            docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(root))
        .execute_next()
        .unwrap();
    }

    #[test]
    fn early_deploy_failure_persists_generation_diagnostics() {
        let root = test_root("early-deploy-failure-persists-generation-diagnostics");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_invalid_multi_service_forge_yaml(&root);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next();

        assert!(matches!(result, Err(DeploymentError::InvalidInspection(_))));
        let generation_dir = root.join("projects/api/environments/production/generations/1");
        assert!(generation_dir.exists());
        assert!(generation_dir.join("diagnostics/deployment.log").exists());
        assert!(generation_dir.join("diagnostics/summary.json").exists());
        assert!(
            generation_dir
                .join("diagnostics/failure_reason.log")
                .exists()
        );
        let summary = fs::read_to_string(generation_dir.join("diagnostics/summary.json")).unwrap();
        assert!(summary.contains("\"failure_stage\": \"topology\""));
        assert!(summary.contains("\"deployment_id\": \"dep-1\""));
        let snapshot = fs::read_to_string(generation_dir.join("snapshot.json")).unwrap();
        assert!(snapshot.contains("\"state\": \"failed\""));
    }

    #[test]
    fn persistent_volume_survives_redeploy() {
        let root = test_root("persistent-volume-survives-redeploy");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_stateful_multi_service_forge_yaml(&root, "persistent");
        let mut docker = MultiServiceDockerRuntime::default();

        deploy_once_with_stateful_yaml(&root, &mut docker, "dep-1");
        deploy_once_with_stateful_yaml(&root, &mut docker, "dep-2");

        let postgres_requests = docker
            .create_requests
            .iter()
            .filter(|request| request.container_name.contains("postgres"))
            .collect::<Vec<_>>();
        assert_eq!(postgres_requests.len(), 2);
        assert_eq!(
            postgres_requests[0].volume_mounts[0].volume_name,
            postgres_requests[1].volume_mounts[0].volume_name
        );
        assert!(
            docker.removed_volumes.is_empty(),
            "persistent volumes must not be removed during redeploy"
        );
    }

    #[test]
    fn rollback_reuses_persistent_volume() {
        let root = test_root("rollback-reuses-persistent-volume");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_stateful_multi_service_forge_yaml(&root, "persistent");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![
                RouteInspection {
                    subtree_id: "forge:api:production:api".into(),
                    active_target: "172.18.0.12:3000".into(),
                    domain: Some("example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                },
                RouteInspection {
                    subtree_id: "forge:api:production:api".into(),
                    active_target: "172.18.0.12:3000".into(),
                    domain: Some("example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                },
                RouteInspection {
                    subtree_id: "forge:api:production:api".into(),
                    active_target: "172.18.0.12:3000".into(),
                    domain: Some("example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                },
            ],
        };
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-2".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        for _ in 0..2 {
            DeploymentExecutor::new(
                &root,
                &queue,
                &mut docker,
                &mut probes,
                &mut routing,
                ValidationPolicy::default(),
            )
            .with_execution_config(default_execution_config(&root))
            .execute_next()
            .unwrap();
        }
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let gen1 = load_generation_runtime_info(&env, 1)
            .unwrap()
            .unwrap()
            .services["postgres"]
            .volume_mounts[0]
            .docker_volume_name
            .clone();
        let gen2 = load_generation_runtime_info(&env, 2)
            .unwrap()
            .unwrap()
            .services["postgres"]
            .volume_mounts[0]
            .docker_volume_name
            .clone();
        assert_eq!(gen1, gen2);
        assert_eq!(
            PointerStore::new(env).read_pointer("current").unwrap(),
            Some(1)
        );
    }

    #[test]
    fn rollback_does_not_restore_database_history() {
        let root = test_root("rollback-does-not-restore-database-history");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_stateful_multi_service_forge_yaml(&root, "persistent");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![
                RouteInspection {
                    subtree_id: "forge:api:production:api".into(),
                    active_target: "172.18.0.12:3000".into(),
                    domain: Some("example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                };
                3
            ],
        };
        for deployment_id in ["dep-1", "dep-2"] {
            queue
                .enqueue(DeploymentRecord {
                    deployment_id: deployment_id.into(),
                    project_id: "api".into(),
                    environment: "production".into(),
                    intent: "deploy".into(),
                    source_path: None,
                    source_ref: None,
                    repo_url: None,
                    commit_sha: None,
                })
                .unwrap();
            DeploymentExecutor::new(
                &root,
                &queue,
                &mut docker,
                &mut probes,
                &mut routing,
                ValidationPolicy::default(),
            )
            .with_execution_config(default_execution_config(&root))
            .execute_next()
            .unwrap();
        }
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let generation_one_volume = load_generation_runtime_info(&env, 1)
            .unwrap()
            .unwrap()
            .services["postgres"]
            .volume_mounts[0]
            .docker_volume_name
            .clone();
        let generation_two_volume = load_generation_runtime_info(&env, 2)
            .unwrap()
            .unwrap()
            .services["postgres"]
            .volume_mounts[0]
            .docker_volume_name
            .clone();
        assert_eq!(generation_one_volume, generation_two_volume);
        assert_eq!(
            PointerStore::new(env).read_pointer("current").unwrap(),
            Some(1)
        );
    }

    #[test]
    fn stateful_service_runtime_truth_persisted() {
        let root = test_root("stateful-service-runtime-truth-persisted");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_stateful_multi_service_forge_yaml(&root, "ephemeral");
        let mut docker = MultiServiceDockerRuntime::default();

        deploy_once_with_stateful_yaml(&root, &mut docker, "dep-1");

        let env = EnvironmentPaths::new(&root, "api", "production");
        let build = load_generation_build_info(&env, 1).unwrap().unwrap();
        let runtime = load_generation_runtime_info(&env, 1).unwrap().unwrap();
        let postgres_build = &build.services["postgres"];
        let postgres = &runtime.services["postgres"];
        assert_eq!(
            postgres_build.state_config.as_ref().unwrap(),
            &PersistedStateConfig {
                volume: "postgres-data".into(),
                mount_path: "/var/lib/postgresql/data".into(),
                retention: PersistedVolumeRetention::Ephemeral,
                pre_backup_command: None,
            }
        );
        assert_eq!(
            postgres.state_config.as_ref().unwrap(),
            &PersistedStateConfig {
                volume: "postgres-data".into(),
                mount_path: "/var/lib/postgresql/data".into(),
                retention: PersistedVolumeRetention::Ephemeral,
                pre_backup_command: None,
            }
        );
        assert_eq!(postgres.volume_mounts.len(), 1);
        assert_eq!(postgres.volume_mounts[0].volume_id, "postgres-data");
        assert_eq!(
            postgres.volume_mounts[0].mount_path,
            "/var/lib/postgresql/data"
        );
        assert!(matches!(
            postgres.volume_mounts[0].retention,
            PersistedVolumeRetention::Ephemeral
        ));
    }

    #[test]
    fn multi_service_generation_promotes_only_when_all_required_services_healthy() {
        let root = test_root("multi-service-promote-gated");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_multi_service_forge_yaml(&root, Some(3100));
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime {
            unhealthy_hosts: vec!["172.18.0.13".into()],
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next();

        assert!(matches!(
            result,
            Err(DeploymentError::ValidationFailed(
                "required service failed health gating"
            ))
        ));
        let env = EnvironmentPaths::new(&root, "api", "production");
        assert_eq!(
            PointerStore::new(env).read_pointer("current").unwrap(),
            None
        );
    }

    #[test]
    fn unstable_dependency_blocks_exposed_service_promotion() {
        let root = test_root("unstable-dependency-blocks-exposed-service-promotion");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_multi_service_forge_yaml(&root, None);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime {
            unhealthy_hosts: vec!["172.18.0.11".into()],
        };
        let mut routing = TestRoutingRuntime::default();

        let result = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next();

        assert!(result.is_err());
        let env = EnvironmentPaths::new(&root, "api", "production");
        assert_eq!(
            PointerStore::new(env).read_pointer("current").unwrap(),
            None
        );
    }

    #[test]
    fn multi_service_depends_on_topological_ordering() {
        let root = test_root("multi-service-startup-order");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_multi_service_forge_yaml(&root, None);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production:api".into(),
                active_target: "172.18.0.12:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        assert_eq!(
            docker.started,
            vec![
                "prod-api-redis-gen-1".to_string(),
                "prod-api-api-gen-1".to_string(),
                "prod-api-worker-gen-1".to_string()
            ]
        );
    }

    #[test]
    fn successful_multiservice_deploy_captures_service_logs() {
        let root = test_root("successful-multiservice-deploy-captures-service-logs");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_multi_service_forge_yaml(&root, None);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production:api".into(),
                active_target: "172.18.0.12:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        docker.container_images.insert(
            "prod-api-api-gen-1".into(),
            "forge/api:production-gen-1".into(),
        );
        docker
            .container_logs
            .insert("prod-api-api-gen-1".into(), "api ready\n".into());
        docker.container_images.insert(
            "prod-api-worker-gen-1".into(),
            "forge/worker:production-gen-1".into(),
        );
        docker
            .container_logs
            .insert("prod-api-worker-gen-1".into(), "worker polling\n".into());
        docker
            .container_images
            .insert("prod-api-redis-gen-1".into(), "redis:7".into());

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        let diagnostics =
            DiagnosticsStore::new(EnvironmentPaths::new(&root, "api", "production"), 1);
        assert!(
            diagnostics
                .artifact_path("services/api/container_logs_tail.log")
                .exists()
        );
        assert!(
            diagnostics
                .artifact_path("services/worker/container_logs_tail.log")
                .exists()
        );
        assert_eq!(
            diagnostics
                .read_text_artifact("services/api/container_logs_tail.log")
                .unwrap()
                .unwrap()
                .trim(),
            "api ready"
        );
        assert_eq!(
            diagnostics
                .read_text_artifact("services/worker/container_logs_tail.log")
                .unwrap()
                .unwrap()
                .trim(),
            "worker polling"
        );
    }

    #[test]
    fn worker_service_without_healthcheck_allowed_as_internal_service() {
        let root = test_root("worker-service-without-healthcheck-allowed-as-internal-service");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_multi_service_forge_yaml(&root, None);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production:api".into(),
                active_target: "172.18.0.12:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        assert_eq!(routing.updates.len(), 1);
        let runtime =
            load_generation_runtime_info(&EnvironmentPaths::new(&root, "api", "production"), 1)
                .unwrap()
                .unwrap();
        assert!(matches!(
            runtime.services["worker"].activation,
            Some(PersistedActivationMode::Direct)
        ));
        assert!(!runtime.services["worker"].externally_exposed);
    }

    #[test]
    fn route_activation_skips_internal_services() {
        let root = test_root("multi-service-route-skip-internal");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_multi_service_forge_yaml(&root, None);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production:api".into(),
                active_target: "172.18.0.12:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        assert_eq!(routing.updates.len(), 1);
        assert_eq!(routing.updates[0].subtree_id, "forge:api:production:api");
    }

    #[test]
    fn service_network_alias_allows_internal_dns() {
        let root = test_root("service-network-alias-allows-internal-dns");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_multi_service_forge_yaml(&root, None);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production:api".into(),
                active_target: "172.18.0.12:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        let api_request = docker
            .create_requests
            .iter()
            .find(|request| request.container_name == "prod-api-api-gen-1")
            .unwrap();
        let worker_request = docker
            .create_requests
            .iter()
            .find(|request| request.container_name == "prod-api-worker-gen-1")
            .unwrap();
        assert_eq!(api_request.network_aliases, vec!["api".to_string()]);
        assert_eq!(worker_request.network_aliases, vec!["worker".to_string()]);
    }

    #[test]
    fn runtime_command_overrides_dockerfile_cmd() {
        let root = test_root("runtime-command-overrides-dockerfile-cmd");
        register_project(&root, "api", "example.com");
        fs::write(
            root.join("Dockerfile"),
            "FROM busybox\nCMD [\"sleep\",\"999\"]\n",
        )
        .unwrap();
        write_multi_service_forge_yaml(&root, None);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production:api".into(),
                active_target: "172.18.0.12:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        let worker_request = docker
            .create_requests
            .iter()
            .find(|request| request.container_name == "prod-api-worker-gen-1")
            .unwrap();
        assert_eq!(
            worker_request.command,
            Some(vec![
                "sh".to_string(),
                "-lc".to_string(),
                "node worker.js".to_string()
            ])
        );
    }

    #[test]
    fn service_specific_dockerfile_builds_correctly() {
        let root = test_root("service-specific-dockerfile-builds-correctly");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        fs::write(root.join("Dockerfile.worker"), "FROM busybox\n").unwrap();
        write_service_build_forge_yaml(&root);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production:api".into(),
                active_target: "172.18.0.12:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        assert_eq!(docker.build_requests.len(), 2);
        assert_eq!(
            docker.build_requests[0].dockerfile_path,
            root.join("Dockerfile")
        );
        assert_eq!(
            docker.build_requests[1].dockerfile_path,
            root.join("Dockerfile.worker")
        );
        assert_eq!(
            docker.build_requests[0].image_tag,
            "forge/api:production-gen-1-api"
        );
        assert_eq!(
            docker.build_requests[1].image_tag,
            "forge/api:production-gen-1-worker"
        );
    }

    #[test]
    fn deployments_persist_per_service_runtime_metadata() {
        let root = test_root("deployments-persist-per-service-runtime-metadata");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        fs::write(root.join("Dockerfile.worker"), "FROM busybox\n").unwrap();
        write_service_build_forge_yaml(&root);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production:api".into(),
                active_target: "172.18.0.12:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let build = load_generation_build_info(&env, 1).unwrap().unwrap();
        let runtime = load_generation_runtime_info(&env, 1).unwrap().unwrap();
        assert_eq!(
            build.services["api"].image_ref,
            "forge/api:production-gen-1-api"
        );
        assert_eq!(
            build.services["worker"].image_ref,
            "forge/api:production-gen-1-worker"
        );
        assert_eq!(
            runtime.startup_order,
            vec!["api".to_string(), "worker".to_string()]
        );
        assert_eq!(
            runtime.services["worker"].depends_on,
            vec!["api".to_string()]
        );
        assert_eq!(
            runtime.services["api"].state,
            PersistedServiceState::Healthy
        );
        assert!(runtime.services["api"].externally_exposed);
        assert!(!runtime.services["worker"].externally_exposed);
    }

    #[test]
    fn diagnostics_render_service_build_sections() {
        let root = test_root("diagnostics-render-service-build-sections");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        fs::write(root.join("Dockerfile.worker"), "FROM busybox\n").unwrap();
        write_service_build_forge_yaml(&root);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production:api".into(),
                active_target: "172.18.0.12:3000".into(),
                domain: Some("example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        let log =
            fs::read_to_string(root.join(
                "projects/api/environments/production/generations/1/diagnostics/deployment.log",
            ))
            .unwrap();
        assert!(log.contains("[service:api] building"));
        assert!(log.contains("[service:worker] starting"));
    }

    #[test]
    fn rollback_restores_multi_service_topology() {
        let root = test_root("multi-service-rollback-topology");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        write_multi_service_forge_yaml(&root, None);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![
                RouteInspection {
                    subtree_id: "forge:api:production:api".into(),
                    active_target: "172.18.0.12:3000".into(),
                    domain: Some("example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                },
                RouteInspection {
                    subtree_id: "forge:api:production:api".into(),
                    active_target: "172.18.0.12:3000".into(),
                    domain: Some("example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                },
                RouteInspection {
                    subtree_id: "forge:api:production:api".into(),
                    active_target: "172.18.0.12:3000".into(),
                    domain: Some("example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                },
            ],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();
        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        assert_eq!(
            PointerStore::new(env).read_pointer("current").unwrap(),
            Some(1)
        );
        assert!(docker.containers.contains_key("prod-api-worker-gen-1"));
        assert!(docker.containers.contains_key("prod-api-redis-gen-1"));
    }

    #[test]
    fn rollback_restores_service_image_refs() {
        let root = test_root("rollback-restores-service-image-refs");
        register_project(&root, "api", "example.com");
        fs::write(root.join("Dockerfile"), "FROM busybox\n").unwrap();
        fs::write(root.join("Dockerfile.worker"), "FROM busybox\n").unwrap();
        write_service_build_forge_yaml(&root);
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        queued_record(&queue);
        let mut docker = MultiServiceDockerRuntime::default();
        let mut probes = HostProbeRuntime::default();
        let mut routing = TestRoutingRuntime {
            updates: vec![],
            inspections: vec![
                RouteInspection {
                    subtree_id: "forge:api:production:api".into(),
                    active_target: "172.18.0.12:3000".into(),
                    domain: Some("example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                },
                RouteInspection {
                    subtree_id: "forge:api:production:api".into(),
                    active_target: "172.18.0.12:3000".into(),
                    domain: Some("example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                },
                RouteInspection {
                    subtree_id: "forge:api:production:api".into(),
                    active_target: "172.18.0.12:3000".into(),
                    domain: Some("example.com".into()),
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: false,
                },
            ],
        };

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();
        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-rollback".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();

        let execution = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        )
        .with_execution_config(default_execution_config(&root))
        .execute_next()
        .unwrap()
        .unwrap();

        let env = EnvironmentPaths::new(&root, "api", "production");
        let build = load_generation_build_info(&env, 1).unwrap().unwrap();
        let runtime = load_generation_runtime_info(&env, 1).unwrap().unwrap();
        assert_eq!(execution.generation, 1);
        assert_eq!(execution.image_ref, "forge/api:production-gen-1-api");
        assert_eq!(
            build.services["api"].image_ref,
            "forge/api:production-gen-1-api"
        );
        assert_eq!(
            build.services["worker"].image_ref,
            "forge/api:production-gen-1-worker"
        );
        assert_eq!(
            runtime.services["api"].image_ref,
            "forge/api:production-gen-1-api"
        );
        assert_eq!(
            runtime.services["worker"].image_ref,
            "forge/api:production-gen-1-worker"
        );
    }
}
