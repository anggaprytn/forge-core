use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use crate::events::{EventRecord, redact_text};
use crate::forge_yaml::{ForgeServiceConfig, ForgeYamlConfig, load_optional_forge_yaml};
use crate::manifest::{ManifestError, SecretReference, load_optional_manifest};
use crate::metrics::registry as metrics_registry;
use crate::projects::ProjectRegistryStore;
use crate::queue::{DeploymentRecord, PersistentQueue, QueueError};
use crate::route_truth::resolve_route_target;
use crate::runtime::{
    BuildImageRequest, ContainerInspection, CreateContainerRequest, DockerRuntime,
    DockerRuntimeError, ProbeError, ProbeRuntime, RouteInspection, RouteUpdateRequest,
    RoutingRuntime, RoutingRuntimeError,
};
use crate::runtime_env::{RuntimeEnvMetadata, build_runtime_env_artifacts};
use crate::secrets::{SecretError, SecretResolution, SecretStore};
use crate::status::derive_environment_domain;
use crate::storage::{
    CleanupRecord, CleanupStore, DeploymentLifecycleState, DiagnosticSummary, DiagnosticsStore,
    EnvironmentPaths, EventStore, GenerationAllocator, GenerationHistoryRecord, LifecycleStore,
    PersistedActivationMode, PersistedBuildInfo, PersistedDeploymentLifecycle,
    PersistedProbeHistoryEntry, PersistedProbeType, PersistedPromotionSummary,
    PersistedRouteTargetSource, PersistedRuntimeInfo, PersistedServiceRuntimeInfo,
    PersistedServiceState, PersistedValidationSummary, PointerStore, ProbeHistoryStore,
    RetentionStore, RuntimeHealthState, RuntimeState, RuntimeStateStore, SnapshotState,
    SnapshotWriter, StorageError, current_unix_timestamp, load_generation_build_info,
    load_generation_runtime_info, load_generation_snapshot_metadata,
};

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
    RollbackUnavailable,
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
            Self::RollbackUnavailable => write!(f, "rollback target unavailable"),
        }
    }
}

impl std::error::Error for DeploymentError {}

const MAX_PROBE_HISTORY_ENTRIES: usize = 64;

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
        let forge_yaml = load_optional_forge_yaml(&source_root, &record.project_id)
            .map_err(|err| DeploymentError::InvalidInspection(err.to_string()))?;
        if forge_yaml.as_ref().is_some_and(is_multi_service_config) {
            return self.execute_multi_service_record(record, &source_root, forge_yaml.as_ref());
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
        let env =
            EnvironmentPaths::new(&self.storage_root, &record.project_id, &record.environment);
        let generation = GenerationAllocator::new(env.clone()).allocate()?;
        let events = EventStore::new(env.clone(), generation);
        let diagnostics = DiagnosticsStore::new(env.clone(), generation);
        let lifecycle_store = LifecycleStore::new(env.clone(), generation);
        let labels = forge_labels(record, generation);
        let container_name = generation_container_name(record, generation);
        let image_tag = format!(
            "forge/{}:{}-gen-{}",
            record.project_id, record.environment, generation
        );
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
        let domain =
            load_environment_domain(&self.storage_root, &record.project_id, &record.environment)?;
        let runtime_secrets = match self.resolve_runtime_secrets(&execution.context_path, record) {
            Ok(secrets) => secrets,
            Err(DeploymentError::MissingSecret(message)) => {
                diagnostics.write_failure_reason(&message, &[])?;
                diagnostics.append_log_line(
                    &format!("deployment started for {}", record.deployment_id),
                    &[],
                )?;
                diagnostics.append_log_line(&message, &[])?;
                diagnostics.write_summary(&DiagnosticSummary {
                    deployment_id: Some(record.deployment_id.clone()),
                    failure_stage: "preparing".into(),
                    failure_reason: message.clone(),
                    container_name: container_name.clone(),
                    probe_target_host: None,
                    probe_target_port: None,
                    probe_target_path: None,
                    cleanup_recorded: false,
                    runtime_env_preview: Vec::new(),
                })?;
                append_event(
                    &events,
                    record,
                    generation,
                    "REQUIRED_SECRET_MISSING",
                    Some(message.clone()),
                )?;
                return Err(DeploymentError::MissingSecret(message));
            }
            Err(err) => return Err(err),
        };
        let forge_yaml_values = forge_yaml
            .as_ref()
            .map(|config| config.environment().clone())
            .unwrap_or_default();
        let runtime_env = build_runtime_env_artifacts(
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
            &BTreeMap::new(),
        )?;
        let redacted_env_preview = runtime_env.redacted_preview.clone();
        let secret_values = runtime_env.redaction_values.clone();
        append_event(&events, record, generation, "DEPLOYMENT_STARTED", None)?;
        diagnostics.append_log_line(
            &format!("deployment started for {}", record.deployment_id),
            &secret_values,
        )?;
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
                    None,
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
                self.record_failed_attempt(
                    &events,
                    &diagnostics,
                    record,
                    generation,
                    "preparing",
                    &failure_reason,
                    None,
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

        match self.docker.create_container(CreateContainerRequest {
            container_name: container_name.clone(),
            image_ref: image_ref.clone(),
            labels: labels.clone(),
            environment: runtime_env.container_env.clone(),
            network_name: execution.network_name.clone(),
            command: None,
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
                self.record_failed_attempt(
                    &events,
                    &diagnostics,
                    record,
                    generation,
                    "preparing",
                    &failure_reason,
                    None,
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
                    &redacted_env_preview,
                    &secret_values,
                )?;
                return Err(err.into());
            }
        };
        if let Err(err) = validate_inspection(&inspection, &container_name) {
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
                &redacted_env_preview,
                &secret_values,
            )?;
            return Err(err);
        }
        let build_json = serde_json::to_string_pretty(&PersistedBuildInfo {
            deployment_id: record.deployment_id.clone(),
            image_ref: image_ref.clone(),
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

        writer.finalize(
            &record.project_id,
            &record.environment,
            SnapshotState::Healthy,
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
            self.routing.update_route(RouteUpdateRequest {
                subtree_id: subtree_id.clone(),
                target: upstream_target.clone(),
                domain: domain.clone(),
                health_checks_enabled: false,
                probe_path: service.probe_path.clone(),
            })?;
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
        pointers.swap_current(target)?;
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
        forge_yaml: Option<&ForgeYamlConfig>,
    ) -> Result<DeploymentExecution, DeploymentError> {
        let config = forge_yaml.ok_or_else(|| {
            DeploymentError::InvalidInspection("multi-service deployment requires forge.yml".into())
        })?;
        let mut execution = config.execution().clone();
        execution.network_name = self.execution.network_name.clone();
        let env =
            EnvironmentPaths::new(&self.storage_root, &record.project_id, &record.environment);
        let generation = GenerationAllocator::new(env.clone()).allocate()?;
        let events = EventStore::new(env.clone(), generation);
        let diagnostics = DiagnosticsStore::new(env.clone(), generation);
        let lifecycle_store = LifecycleStore::new(env.clone(), generation);
        let labels = forge_labels(record, generation);
        let writer = SnapshotWriter::new(env.clone(), generation)?;
        let validation_timeout_ms = config
            .validation_timeout_ms()
            .unwrap_or(DEFAULT_VALIDATION_TIMEOUT_MS);
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
            "deployment dequeued for multi-service execution",
            None,
            None,
        )?;
        let domain =
            load_environment_domain(&self.storage_root, &record.project_id, &record.environment)?;
        let runtime_secrets = self.resolve_runtime_secrets(source_root, record)?;
        let runtime_env = build_runtime_env_artifacts(
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
            &BTreeMap::new(),
        )?;
        let redacted_env_preview = runtime_env.redacted_preview.clone();
        let secret_values = runtime_env.redaction_values.clone();
        append_event(&events, record, generation, "DEPLOYMENT_STARTED", None)?;
        diagnostics.append_log_line(
            &format!(
                "multi-service deployment started for {}",
                record.deployment_id
            ),
            &secret_values,
        )?;

        let requires_shared_build = config
            .services()
            .values()
            .any(|service| service.image.is_none());
        let shared_image_ref = if requires_shared_build {
            persist_lifecycle_transition(
                &lifecycle_store,
                &record.project_id,
                &record.environment,
                generation,
                DeploymentLifecycleState::Building,
                "shared application image build started",
                None,
                None,
            )?;
            let image_tag = format!(
                "forge/{}:{}-gen-{}",
                record.project_id, record.environment, generation
            );
            let image_ref = self.docker.build_image(BuildImageRequest {
                image_tag: image_tag.clone(),
                context_path: execution.context_path.clone(),
                dockerfile_path: execution.dockerfile_path.clone(),
                labels: labels.clone(),
            })?;
            diagnostics
                .append_log_line(&format!("shared image built: {image_ref}"), &secret_values)?;
            update_generation_history(&env, generation, |history| {
                history.image_ref = Some(image_ref.clone());
            })?;
            Some(image_ref)
        } else {
            None
        };

        if let Some(network_name) = execution.network_name.as_deref() {
            self.docker.ensure_network(network_name)?;
            diagnostics.append_log_line(
                &format!("docker network ready: {network_name}"),
                &secret_values,
            )?;
        }

        let mut service_runtime = BTreeMap::new();
        let mut warmed_services = Vec::new();
        let mut route_ids = Vec::new();
        for service_id in config.startup_order() {
            let service = config
                .services()
                .get(service_id)
                .expect("startup order references known service");
            diagnostics.append_log_line(
                &format!(
                    "service `{service_id}` starting after dependencies: {}",
                    if service.depends_on.is_empty() {
                        "none".into()
                    } else {
                        service.depends_on.join(", ")
                    }
                ),
                &secret_values,
            )?;
            let container_name = generation_service_container_name(
                record,
                generation,
                service_id,
                config.services().len(),
            );
            let image_ref = service
                .image
                .clone()
                .or_else(|| shared_image_ref.clone())
                .ok_or_else(|| {
                    DeploymentError::InvalidInspection(format!(
                        "service `{service_id}` has no runtime image and no shared build image"
                    ))
                })?;
            let mut service_labels = labels.clone();
            service_labels.insert("forge.service_id".into(), service_id.clone());
            service_labels.insert(
                "forge.route_id".into(),
                route_subtree_id_for_service(record, service_id, config.services().len()),
            );
            self.docker.create_container(CreateContainerRequest {
                container_name: container_name.clone(),
                image_ref: image_ref.clone(),
                labels: service_labels,
                environment: runtime_env.container_env.clone(),
                network_name: execution.network_name.clone(),
                command: service_command(service),
            })?;
            self.docker.start_container(&container_name)?;
            let inspection = self.docker.inspect_container(&container_name)?;
            validate_inspection(&inspection, &container_name)?;
            let warmup = self.validate_service_candidate(
                service_id,
                service,
                validation_timeout_ms,
                &env,
                generation,
                &container_name,
                &events,
                record,
                &diagnostics,
                &redacted_env_preview,
                &secret_values,
            )?;
            warmed_services.push(warmup);
            if service.externally_exposed {
                route_ids.push(route_subtree_id_for_service(
                    record,
                    service_id,
                    config.services().len(),
                ));
            }
            service_runtime.insert(
                service_id.clone(),
                PersistedServiceRuntimeInfo {
                    service_id: service_id.clone(),
                    container_name: inspection.container_name.clone(),
                    image_ref,
                    running: inspection.running,
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
        let build_json = serde_json::to_string_pretty(&PersistedBuildInfo {
            deployment_id: record.deployment_id.clone(),
            image_ref: shared_image_ref
                .clone()
                .or_else(|| Some(primary_runtime.image_ref.clone()))
                .unwrap_or_default(),
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
            environment_variables: primary_runtime.environment_variables.clone(),
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
                ..PersistedPromotionSummary::default()
            }),
        )?;

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
            self.routing.update_route(RouteUpdateRequest {
                subtree_id: subtree_id.clone(),
                target: target.clone(),
                domain: domain.clone(),
                health_checks_enabled: false,
                probe_path: runtime.probe_path.clone(),
            })?;
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
        PointerStore::new(env.clone()).swap_current(generation)?;
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
        diagnostics.append_log_line(
            &format!("startup order: {}", config.startup_order().join(" -> ")),
            &secret_values,
        )?;
        for runtime in service_runtime.values() {
            self.capture_container_logs_tail(
                &diagnostics,
                &runtime.container_name,
                &secret_values,
            )?;
        }
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
        if !service.validation.tcp_required && service.validation.http_health_path.is_none() {
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

        let inspection = self.docker.inspect_container(container_name)?;
        let probe_host =
            resolve_selected_network_host(&inspection, self.execution.network_name.as_deref())?;
        let started = Instant::now();
        let required_passes = service.validation.required_consecutive_probe_passes.max(1);
        let mut tcp_passes = 0u32;
        let mut http_passes = if service.validation.http_health_path.is_some() {
            0
        } else {
            required_passes
        };
        let mut last_error = None;
        let budget = Duration::from_millis(validation_timeout_ms);
        loop {
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
                        observed_uptime_seconds: started.elapsed().as_secs(),
                        validation_succeeded: true,
                        ..PersistedValidationSummary::default()
                    },
                });
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
                self.record_failed_attempt(
                    events,
                    diagnostics,
                    record,
                    generation,
                    "warming",
                    &failure_reason,
                    probe_target.as_ref(),
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
                inspection: Some(inspection),
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
                    inspection: Some(inspection),
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

        loop {
            attempts += 1;
            let mut current_inspection = if attempts == 1 || !inspect_each_attempt {
                inspection.clone()
            } else {
                self.docker.inspect_container(container_name)?
            };
            if !current_inspection.running {
                let failure_reason = container_exited_failure_reason("warmup", &current_inspection);
                let validation_summary = PersistedValidationSummary {
                    tcp_consecutive_passes,
                    http_consecutive_passes,
                    required_consecutive_passes: required_passes,
                    minimum_uptime_seconds,
                    observed_uptime_seconds: validation_started.elapsed().as_secs(),
                    restart_count_initial,
                    restart_count_current: current_inspection.restart_count,
                    restart_count_stable: current_inspection.restart_count == restart_count_initial,
                    route_verification_stable: true,
                    validation_succeeded: false,
                    last_probe_error: Some(failure_reason.clone()),
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
                    inspection: Some(current_inspection),
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
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed(
                    "container exited before tcp probe",
                ));
            }

            if current_inspection.restart_count != restart_count_initial {
                let failure_reason = format!(
                    "restart count increased during warmup ({} -> {})",
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
                    "warming",
                    &failure_reason,
                    Some(&tcp_probe_target),
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
                    }
                }
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
                    inspection: Some(current_inspection),
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
        probe_target: Option<&ProbeTargetContext>,
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
        diagnostics.write_summary(&DiagnosticSummary {
            deployment_id: Some(record.deployment_id.clone()),
            failure_stage: failure_stage.into(),
            failure_reason: failure_reason.into(),
            container_name: String::new(),
            probe_target_host: probe_target.map(|target| target.host.clone()),
            probe_target_port: probe_target.map(|target| target.port),
            probe_target_path: probe_target.and_then(|target| target.path.clone()),
            cleanup_recorded: false,
            runtime_env_preview: redacted_env_preview.to_vec(),
        })?;
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
            container_name: container_name.into(),
            probe_target_host: probe_target.map(|target| target.host.clone()),
            probe_target_port: probe_target.map(|target| target.port),
            probe_target_path: probe_target.and_then(|target| target.path.clone()),
            cleanup_recorded: true,
            runtime_env_preview: redacted_env_preview.to_vec(),
        })?;
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
        let _ = self.docker.stop_container(container_name);
        let container_removed = self.docker.remove_container(container_name).is_ok();
        let route_removed = if let Some(subtree_id) = route_subtree_id.clone() {
            self.routing.remove_route(&subtree_id).is_ok()
        } else {
            true
        };
        let cleanup = CleanupRecord::new(
            failure_reason,
            Some(container_name.into()),
            route_subtree_id,
            container_removed,
            route_removed,
            !(container_removed && route_removed),
        );
        let image_removed = if let Some(image_ref) = image_ref {
            self.docker.remove_image(image_ref).is_ok()
        } else {
            true
        };
        let cleanup = CleanupRecord {
            image_ref: image_ref.map(|value| value.to_string()),
            image_removed,
            tombstoned: !(container_removed && route_removed && image_removed),
            ..cleanup
        };
        CleanupStore::new(env.clone(), generation).write_record(&cleanup)?;
        Ok(cleanup)
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

        match validation.activation {
            ActivationMode::Direct => {
                pointers.swap_current(generation)?;
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
                self.routing.update_route(RouteUpdateRequest {
                    subtree_id: subtree_id.clone(),
                    target: target.clone(),
                    domain: domain.clone(),
                    health_checks_enabled: false,
                    probe_path: validation.http_health_path.clone(),
                })?;
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
                pointers.swap_current(generation)?;
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
        pointers.swap_current(target)?;
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
    if inspection.restart_policy != "no" {
        return Err(DeploymentError::InvalidInspection(
            "restart policy must be disabled".into(),
        ));
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
    config
        .startup_order()
        .iter()
        .find(|service_id| {
            runtime
                .get(*service_id)
                .is_some_and(|service| service.externally_exposed)
        })
        .cloned()
        .or_else(|| config.startup_order().first().cloned())
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
            network_name: runtime.network_name.clone(),
            probe_path: runtime.probe_path.clone(),
            activation: runtime.activation.clone(),
            command: None,
            depends_on: Vec::new(),
            required_for_promotion: true,
            externally_exposed: matches!(
                runtime.activation,
                Some(PersistedActivationMode::Http { .. })
            ),
            environment_variables: runtime.environment_variables.clone(),
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
pub mod deployment_fails_if_tcp_unreachable {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn tcp_probe_failure_rejects_deployment() {
        let root = test_root("tcp-unreachable");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
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
        assert!(
            !root
                .join("projects/api/environments/production/generations/1/snapshot.json")
                .exists()
        );
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
        let mut docker =
            DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
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
        assert!(
            !root
                .join("projects/api/environments/production/generations/1/snapshot.json")
                .exists()
        );
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
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            format!("image_ref=forge/api:production-gen-1"),
            "prod-api-gen-1".into(),
            String::new(),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            inspection_output(1, "running", true, 0, &[("forge-test", "172.18.0.2")]),
            String::new(),
        ]));
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
        assert!(!generation_dir.join("snapshot.json").exists());
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
            environment_variables: BTreeMap::new(),
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
            image_ref: format!("forge/{project_id}:{environment}-gen-{generation}"),
            labels: BTreeMap::new(),
            network_ips: BTreeMap::from([(FORGE_MANAGED_DOCKER_NETWORK.into(), ip.into())]),
            restart_policy: "no".into(),
        }
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
        queue.enqueue(rollback_record("api", "production")).unwrap();
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
        queue.enqueue(rollback_record("api", "production")).unwrap();
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
        queue.enqueue(rollback_record("api", "production")).unwrap();
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
        queue.enqueue(rollback_record("api", "production")).unwrap();
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
    fn rollback_restores_historical_env_snapshot() {
        let root = test_root("rollback-restores-historical-env-snapshot");
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
        queue.enqueue(rollback_record("api", "production")).unwrap();
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
        queue.enqueue(rollback_record("api", "production")).unwrap();
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
                image_ref: "forge/api:production-gen-1".into(),
                labels: BTreeMap::new(),
                network_ips: BTreeMap::from([("forge-net".into(), "172.19.0.5".into())]),
                restart_policy: "no".into(),
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
    use std::collections::BTreeMap;
    use std::fs;

    #[derive(Default)]
    struct MultiServiceDockerRuntime {
        containers: BTreeMap<String, bool>,
        created: Vec<String>,
        started: Vec<String>,
    }

    impl DockerRuntime for MultiServiceDockerRuntime {
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
            self.created.push(request.container_name.clone());
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
                image_ref: "forge/api:production-gen-1".into(),
                labels: BTreeMap::new(),
                network_ips: BTreeMap::from([(FORGE_MANAGED_DOCKER_NETWORK.into(), ip.into())]),
                restart_policy: "no".into(),
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
            Ok(Vec::new())
        }

        fn list_managed_images(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedImage>, DockerRuntimeError> {
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
    fn startup_dependency_ordering_respected() {
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
}
