use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::path::PathBuf;

use crate::events::{EventRecord, redact_text};
use crate::forge_yaml::load_optional_forge_yaml;
use crate::manifest::{ManifestError, SecretReference, load_optional_manifest};
use crate::metrics::registry as metrics_registry;
use crate::queue::{DeploymentRecord, PersistentQueue, QueueError};
use crate::runtime::{
    BuildImageRequest, ContainerInspection, CreateContainerRequest, DockerRuntime,
    DockerRuntimeError, ProbeError, ProbeRuntime, RouteInspection, RouteUpdateRequest,
    RoutingRuntime, RoutingRuntimeError,
};
use crate::secrets::{SecretError, SecretResolution, SecretStore};
use crate::storage::{
    CleanupRecord, CleanupStore, DiagnosticSummary, DiagnosticsStore, EnvironmentPaths, EventStore,
    GenerationAllocator, PersistedActivationMode, PersistedBuildInfo, PersistedRouteTargetSource,
    PersistedRuntimeInfo, PersistedSecretReference, PointerStore, SnapshotState, SnapshotWriter,
    StorageError,
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
pub struct ValidationPolicy {
    pub tcp_required: bool,
    pub http_health_path: Option<String>,
    pub activation: ActivationMode,
}

impl Default for ValidationPolicy {
    fn default() -> Self {
        Self {
            tcp_required: true,
            http_health_path: None,
            activation: ActivationMode::Direct,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationMode {
    Direct,
    Http { internal_port: u16 },
}

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
        let source_root = record
            .source_path
            .clone()
            .unwrap_or_else(|| self.execution.context_path.clone());
        let forge_yaml = load_optional_forge_yaml(&source_root, &record.project_id)
            .map_err(|err| DeploymentError::InvalidInspection(err.to_string()))?;
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
        let env =
            EnvironmentPaths::new(&self.storage_root, &record.project_id, &record.environment);
        let generation = GenerationAllocator::new(env.clone()).allocate()?;
        let events = EventStore::new(env.clone(), generation);
        let diagnostics = DiagnosticsStore::new(env.clone(), generation);
        let labels = forge_labels(record, generation);
        let container_name = generation_container_name(record, generation);
        let image_tag = format!(
            "forge/{}:{}-gen-{}",
            record.project_id, record.environment, generation
        );
        let writer = SnapshotWriter::new(env.clone(), generation)?;
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
        let secret_values = runtime_secrets
            .iter()
            .map(|secret| secret.value.clone())
            .collect::<Vec<_>>();
        let redacted_env_preview = runtime_env_preview(&runtime_secrets);
        append_event(&events, record, generation, "DEPLOYMENT_STARTED", None)?;
        diagnostics.append_log_line(
            &format!("deployment started for {}", record.deployment_id),
            &secret_values,
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

        match self.docker.create_container(CreateContainerRequest {
            container_name: container_name.clone(),
            image_ref: image_ref.clone(),
            labels: labels.clone(),
            environment: runtime_environment(&runtime_secrets),
            network_name: execution.network_name.clone(),
        }) {
            Ok(_) => {}
            Err(err) => {
                let failure_reason = format!("container create failed: {err}");
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
        if let Err(err) = self.docker.start_container(&container_name) {
            let failure_reason = format!("container start failed: {err}");
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
        let probe_host =
            match resolve_validation_probe_host(&inspection, execution.network_name.as_deref()) {
                Ok(probe_host) => probe_host,
                Err(err) => {
                    let failure_reason = err.to_string();
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
            };
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
            environment_variables: runtime_secret_references(&runtime_secrets),
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
        writer.write_artifact("runtime.json", &format!("{runtime_json}\n"))?;
        diagnostics.append_log_line("runtime inspection passed", &secret_values)?;
        self.validate_candidate(
            &validation,
            &env,
            &container_name,
            &image_ref,
            &probe_host,
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
        append_event(&events, record, generation, "SNAPSHOT_FINALIZED", None)?;
        diagnostics.append_log_line("snapshot finalized", &secret_values)?;
        if let Err(err) = self.activate_generation(
            &validation,
            &execution,
            record,
            &env,
            generation,
            &container_name,
            &inspection,
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

        Ok(DeploymentExecution {
            deployment_id: record.deployment_id.clone(),
            generation,
            image_ref,
            container_name,
        })
    }

    fn validate_candidate(
        &mut self,
        validation: &ValidationPolicy,
        env: &EnvironmentPaths,
        container_name: &str,
        image_ref: &str,
        probe_host: &str,
        events: &EventStore,
        diagnostics: &DiagnosticsStore,
        record: &DeploymentRecord,
        generation: u64,
        redacted_env_preview: &[String],
        secret_values: &[String],
    ) -> Result<(), DeploymentError> {
        let internal_port = match validation.activation {
            ActivationMode::Direct => 3000,
            ActivationMode::Http { internal_port } => internal_port,
        };
        let tcp_probe_target = ProbeTargetContext {
            host: probe_host.to_string(),
            port: internal_port,
            path: None,
        };
        if validation.tcp_required && !self.probes.probe_tcp(probe_host, internal_port)? {
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
                "tcp probe failed",
                Some(&tcp_probe_target),
                redacted_env_preview,
                secret_values,
            )?;
            return Err(DeploymentError::ValidationFailed("tcp probe failed"));
        }
        diagnostics.append_log_line("tcp validation passed", secret_values)?;

        if let Some(path) = &validation.http_health_path {
            if !self.probes.probe_http(probe_host, internal_port, path)? {
                let http_probe_target = ProbeTargetContext {
                    host: probe_host.to_string(),
                    port: internal_port,
                    path: Some(path.clone()),
                };
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
                    "http health probe failed",
                    Some(&http_probe_target),
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed(
                    "http health probe failed",
                ));
            }
            diagnostics
                .append_log_line(&format!("http validation passed: {path}"), secret_values)?;
        }

        append_event(events, record, generation, "VALIDATION_PASSED", None)?;
        Ok(())
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
        generation: u64,
        _container_name: &str,
        inspection: &ContainerInspection,
    ) -> Result<(), DeploymentError> {
        match validation.activation {
            ActivationMode::Direct => {
                PointerStore::new(env.clone()).swap_current(generation)?;
                Ok(())
            }
            ActivationMode::Http { internal_port } => {
                let subtree_id = route_subtree_id(record);
                let target_host =
                    resolve_route_target_host(inspection, execution.network_name.as_deref())?;
                let target = format!("{target_host}:{internal_port}");
                self.routing.update_route(RouteUpdateRequest {
                    subtree_id: subtree_id.clone(),
                    target: target.clone(),
                    health_checks_enabled: false,
                    probe_path: validation.http_health_path.clone(),
                })?;
                let inspection = self.routing.inspect_route(&subtree_id)?;
                validate_route_activation(&inspection, &subtree_id, &target)?;
                PointerStore::new(env.clone()).swap_current(generation)?;
                Ok(())
            }
        }
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

fn resolve_validation_probe_host(
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
        .or_else(|| Some(inspection.container_name.clone()))
        .ok_or_else(|| DeploymentError::InvalidInspection("container missing network IP".into()))
}

fn resolve_route_target_host(
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

fn runtime_environment(secrets: &[SecretResolution]) -> BTreeMap<String, String> {
    secrets
        .iter()
        .map(|secret| (secret.key.clone(), secret.value.clone()))
        .collect()
}

fn runtime_secret_references(
    secrets: &[SecretResolution],
) -> BTreeMap<String, PersistedSecretReference> {
    secrets
        .iter()
        .map(|secret| {
            (
                secret.key.clone(),
                PersistedSecretReference {
                    scope: "environment".into(),
                    key: secret.source_key.clone(),
                    sensitive: secret.sensitive,
                },
            )
        })
        .collect()
}

fn runtime_env_preview(secrets: &[SecretResolution]) -> Vec<String> {
    secrets
        .iter()
        .map(|secret| {
            let value = if secret.sensitive || secret.value.len() >= 8 {
                "[REDACTED]".to_string()
            } else {
                secret.value.clone()
            };
            format!("{}={value}", secret.key)
        })
        .collect()
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

fn format_probe_target_log(target: &ProbeTargetContext) -> String {
    match &target.path {
        Some(path) => format!(
            "probe target: host={} port={} path={}",
            target.host, target.port, path
        ),
        None => format!("probe target: host={} port={}", target.host, target.port),
    }
}

fn route_subtree_id(record: &DeploymentRecord) -> String {
    format!("forge:{}:{}", record.project_id, record.environment)
}

fn validate_route_activation(
    inspection: &RouteInspection,
    expected_subtree_id: &str,
    expected_target: &str,
) -> Result<(), DeploymentError> {
    if inspection.subtree_id != expected_subtree_id {
        return Err(DeploymentError::InvalidInspection(
            "route subtree mismatch".into(),
        ));
    }
    if !inspection.activation_verified {
        return Err(DeploymentError::ValidationFailed(
            "route activation verification failed",
        ));
    }
    if inspection.active_target != expected_target {
        return Err(DeploymentError::ValidationFailed("route target mismatch"));
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
            source_path: None,
            source_ref: None,
            repo_url: None,
            commit_sha: None,
        })
        .unwrap();
}

#[cfg(test)]
fn success_outputs(generation: u64) -> Vec<String> {
    success_outputs_with_network(generation, &[("forge-test", "172.18.0.2")])
}

#[cfg(test)]
fn success_outputs_with_network(generation: u64, networks: &[(&str, &str)]) -> Vec<String> {
    vec![
        format!("image_ref=forge/api:production-gen-{generation}"),
        format!("prod-api-gen-{generation}"),
        String::new(),
        std::iter::once(format!("name=prod-api-gen-{generation}"))
            .chain(std::iter::once("running=true".into()))
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
            .join("\n"),
    ]
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

        assert!(matches!(
            result,
            Err(DeploymentError::ValidationFailed("tcp probe failed"))
        ));
        assert!(
            !root
                .join("projects/api/environments/production/generations/1/snapshot.json")
                .exists()
        );
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
            success_outputs_with_network(1, &[("forge-net", "172.18.0.2")]),
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
                activation_verified: true,
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
            success_outputs_with_network(
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
                source_path: Some(source_root),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            success_outputs_with_network(
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
                source_path: Some(source_root.clone()),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            success_outputs_with_network(1, &[("forge-net", "172.18.0.2")]),
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
                activation_verified: true,
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
                source_path: Some(source_root.clone()),
                source_ref: Some("main".into()),
                repo_url: Some("https://github.com/example/api.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(
            success_outputs_with_network(1, &[("bridge", "172.18.0.2")]),
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
                activation_verified: true,
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
                source_path: Some(source_root),
                source_ref: Some("release".into()),
                repo_url: Some("https://github.com/example/web.git".into()),
                commit_sha: Some("abc123".into()),
            })
            .unwrap();
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(vec![
            "image_ref=forge/web:staging-gen-1".into(),
            "staging-web-gen-1".into(),
            String::new(),
            [
                "name=/staging-web-gen-1",
                "running=true",
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
                activation_verified: true,
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
            success_outputs_with_network(1, &[("bridge", "172.18.0.2")]),
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
            matches!(result, Err(DeploymentError::InvalidInspection(message)) if message.contains("missing field `port`"))
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
            success_outputs_with_network(1, &[("forge-net", "172.18.0.2")]),
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
                activation_verified: true,
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
                activation_verified: true,
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
                activation_verified: true,
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
                activation_verified: true,
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
            },
        )
        .execute_next()
        .unwrap();

        assert_eq!(routing.updates[0].target, "172.18.0.2:3000");
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
                activation_verified: false,
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
                activation_verified: true,
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
                activation_verified: true,
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
            },
        )
        .execute_next()
        .unwrap();

        assert_eq!(routing.updates[0].subtree_id, "forge:api:production");
    }
}
