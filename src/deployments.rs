use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use crate::events::{redact_text, EventRecord};
use crate::manifest::{load_optional_manifest, ManifestError, SecretReference};
use crate::metrics::registry as metrics_registry;
use crate::queue::{DeploymentRecord, PersistentQueue, QueueError};
use crate::runtime::{
    BuildImageRequest, ContainerInspection, CreateContainerRequest, DockerRuntime,
    DockerRuntimeError, ProbeError, ProbeRuntime, RouteInspection, RouteUpdateRequest,
    RoutingRuntime, RoutingRuntimeError,
};
use crate::secrets::{SecretError, SecretResolution, SecretStore};
use crate::storage::{
    CleanupRecord, CleanupStore, DiagnosticSummary, DiagnosticsStore, EnvironmentPaths,
    EventStore, GenerationAllocator, PointerStore, SnapshotState, SnapshotWriter, StorageError,
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
        let env = EnvironmentPaths::new(&self.storage_root, &record.project_id, &record.environment);
        let generation = GenerationAllocator::new(env.clone()).allocate()?;
        let events = EventStore::new(env.clone(), generation);
        let diagnostics = DiagnosticsStore::new(env.clone(), generation);
        let labels = forge_labels(record, generation);
        let container_name = generation_container_name(record, generation);
        let image_tag = format!("forge/{}:{}-gen-{}", record.project_id, record.environment, generation);
        let writer = SnapshotWriter::new(env.clone(), generation)?;
        let runtime_secrets = match self.resolve_runtime_secrets(record) {
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
            context_path: self.execution.context_path.clone(),
            dockerfile_path: self.execution.dockerfile_path.clone(),
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
            network_name: self.execution.network_name.clone(),
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
            &format!("runtime environment prepared: {}", redacted_env_preview.join(", ")),
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
                None,
                "starting",
                &failure_reason,
                &redacted_env_preview,
                &secret_values,
            )?;
            return Err(err.into());
        }
        append_event(&events, record, generation, "CONTAINER_STARTED", None)?;
        diagnostics.append_log_line(&format!("container started: {container_name}"), &secret_values)?;
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
                    None,
                    "validating_runtime",
                    &failure_reason,
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
                None,
                "validating_runtime",
                &failure_reason,
                &redacted_env_preview,
                &secret_values,
            )?;
            return Err(err);
        }
        writer.write_artifact(
            "build.json",
            &format!(
                "{{\n  \"deployment_id\": \"{}\",\n  \"image_ref\": \"{}\"\n}}\n",
                record.deployment_id, image_ref
            ),
        )?;
        writer.write_artifact(
            "runtime.json",
            &format!(
                "{{\n  \"container_name\": \"{}\",\n  \"running\": {}\n}}\n",
                inspection.container_name, inspection.running
            ),
        )?;
        diagnostics.append_log_line("runtime inspection passed", &secret_values)?;
        self.validate_candidate(
            &env,
            &container_name,
            &events,
            &diagnostics,
            record,
            generation,
            &redacted_env_preview,
            &secret_values,
        )?;

        writer.finalize(&record.project_id, &record.environment, SnapshotState::Healthy)?;
        append_event(&events, record, generation, "SNAPSHOT_FINALIZED", None)?;
        diagnostics.append_log_line("snapshot finalized", &secret_values)?;
        if let Err(err) = self.activate_generation(record, &env, generation, &container_name) {
            self.record_failed_generation(
                &env,
                &events,
                &diagnostics,
                record,
                generation,
                &container_name,
                Some(route_subtree_id(record)),
                "routing",
                &err.to_string(),
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
        env: &EnvironmentPaths,
        container_name: &str,
        events: &EventStore,
        diagnostics: &DiagnosticsStore,
        record: &DeploymentRecord,
        generation: u64,
        redacted_env_preview: &[String],
        secret_values: &[String],
    ) -> Result<(), DeploymentError> {
        if self.validation.tcp_required && !self.probes.probe_tcp(container_name)? {
            self.record_failed_generation(
                env,
                events,
                diagnostics,
                record,
                generation,
                container_name,
                None,
                "validation",
                "tcp probe failed",
                redacted_env_preview,
                secret_values,
            )?;
            return Err(DeploymentError::ValidationFailed("tcp probe failed"));
        }
        diagnostics.append_log_line("tcp validation passed", secret_values)?;

        if let Some(path) = &self.validation.http_health_path {
            if !self.probes.probe_http(container_name, path)? {
                self.record_failed_generation(
                    env,
                    events,
                    diagnostics,
                    record,
                    generation,
                    container_name,
                    None,
                    "validation",
                    "http health probe failed",
                    redacted_env_preview,
                    secret_values,
                )?;
                return Err(DeploymentError::ValidationFailed("http health probe failed"));
            }
            diagnostics.append_log_line(&format!("http validation passed: {path}"), secret_values)?;
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
        redacted_env_preview: &[String],
        secret_values: &[String],
    ) -> Result<(), DeploymentError> {
        diagnostics.write_failure_reason(failure_reason, secret_values)?;
        diagnostics.append_log_line(failure_reason, secret_values)?;
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
        route_subtree_id: Option<String>,
        failure_stage: &str,
        failure_reason: &str,
        redacted_env_preview: &[String],
        secret_values: &[String],
    ) -> Result<(), DeploymentError> {
        diagnostics.write_failure_reason(failure_reason, secret_values)?;
        diagnostics.append_log_line(failure_reason, secret_values)?;
        append_redacted_event(
            events,
            record,
            generation,
            match failure_reason {
                "tcp probe failed" => "TCP_PROBE_FAILED",
                "http health probe failed" => "HTTP_PROBE_FAILED",
                _ => "GENERATION_FAILED",
            },
            Some(failure_reason.into()),
            secret_values,
        )?;
        let cleanup = self.cleanup_failed_generation(
            env,
            generation,
            container_name,
            route_subtree_id.clone(),
            failure_reason,
        )?;
        diagnostics.write_summary(&DiagnosticSummary {
            deployment_id: Some(record.deployment_id.clone()),
            failure_stage: failure_stage.into(),
            failure_reason: failure_reason.into(),
            container_name: container_name.into(),
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
        CleanupStore::new(env.clone(), generation).write_record(&cleanup)?;
        Ok(cleanup)
    }

    fn activate_generation(
        &mut self,
        record: &DeploymentRecord,
        env: &EnvironmentPaths,
        generation: u64,
        container_name: &str,
    ) -> Result<(), DeploymentError> {
        match self.validation.activation {
            ActivationMode::Direct => {
                PointerStore::new(env.clone()).swap_current(generation)?;
                Ok(())
            }
            ActivationMode::Http { internal_port } => {
                let subtree_id = route_subtree_id(record);
                let target = format!("{container_name}:{internal_port}");
                self.routing.update_route(RouteUpdateRequest {
                    subtree_id: subtree_id.clone(),
                    target: target.clone(),
                    health_checks_enabled: false,
                    probe_path: self.validation.http_health_path.clone(),
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
        record: &DeploymentRecord,
    ) -> Result<Vec<SecretResolution>, DeploymentError> {
        let Some(manifest) = load_optional_manifest(&self.execution.context_path)? else {
            return Ok(Vec::new());
        };
        let store = SecretStore::new(self.storage_root.join("secrets"))?;
        let mut resolved = Vec::new();
        for (env_name, reference) in manifest.environment_variables {
            resolved.push(resolve_secret_reference(&store, record, env_name, reference)?);
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
                value,
                sensitive: reference.sensitive,
            }),
            Err(SecretError::MissingSecret(key)) => {
                Err(DeploymentError::MissingSecret(format!("missing required secret {key}")))
            }
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
        return Err(DeploymentError::ValidationFailed(
            "route target mismatch",
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
    fn probe_tcp(&mut self, _container_name: &str) -> Result<bool, ProbeError> {
        Ok(self.tcp_ok)
    }

    fn probe_http(&mut self, _container_name: &str, _path: &str) -> Result<bool, ProbeError> {
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
        })
        .unwrap();
}

#[cfg(test)]
fn success_outputs(generation: u64) -> Vec<String> {
    vec![
        format!("image_ref=forge/api:production-gen-{generation}"),
        format!("prod-api-gen-{generation}"),
        String::new(),
        [
            format!("name=prod-api-gen-{generation}"),
            format!("running=true"),
            format!("image=forge/api:production-gen-{generation}"),
            "restart_policy=no".into(),
        ]
        .join("\n"),
    ]
}

#[cfg(test)]
pub mod deployment_fails_if_tcp_unreachable {
    use super::*;
    use crate::docker::RecordingCommandRunner;
    use crate::docker::DockerCliRuntime;

    #[test]
    fn tcp_probe_failure_rejects_deployment() {
        let root = test_root("tcp-unreachable");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
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

        assert!(matches!(result, Err(DeploymentError::ValidationFailed("tcp probe failed"))));
        assert!(!root
            .join("projects/api/environments/production/generations/1/snapshot.json")
            .exists());
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
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
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
            Err(DeploymentError::ValidationFailed("http health probe failed"))
        ));
        assert!(!root
            .join("projects/api/environments/production/generations/1/snapshot.json")
            .exists());
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
        assert!(commands.iter().any(|cmd| cmd.args.first() == Some(&"stop".to_string())));
        assert!(commands.iter().any(|cmd| cmd.args.first() == Some(&"rm".to_string())));
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
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime { tcp_ok: true, http_ok: true };
        let mut routing = TestRoutingRuntime::default();

        DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy { tcp_required: true, http_health_path: Some("/health".into()), activation: ActivationMode::Direct },
        ).execute_next().unwrap();

        let events = EventStore::list_all(&root).unwrap();
        assert!(events.iter().any(|event| event.event_type == "DEPLOYMENT_STARTED"));
        assert!(events.iter().any(|event| event.event_type == "VALIDATION_PASSED"));
        assert!(events.iter().any(|event| event.event_type == "GENERATION_PROMOTED"));
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
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime { tcp_ok: false, http_ok: true };
        let mut routing = TestRoutingRuntime::default();

        let _ = DeploymentExecutor::new(
            &root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy::default(),
        ).execute_next();

        let diagnostics = DiagnosticsStore::new(EnvironmentPaths::new(&root, "api", "production"), 1);
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
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
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
pub mod rollback_restores_previous_generation {
    use super::*;
    use crate::storage::EventStore;

    #[test]
    fn rollback_moves_current_pointer_back_to_previous() {
        let root = test_root("rollback-previous");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let writer1 = SnapshotWriter::new(env.clone(), 1).unwrap();
        writer1.finalize("api", "production", SnapshotState::Healthy).unwrap();
        let writer2 = SnapshotWriter::new(env.clone(), 2).unwrap();
        writer2.finalize("api", "production", SnapshotState::Healthy).unwrap();
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();

        let restored = RollbackExecutor::new(&root)
            .rollback_previous("api", "production")
            .unwrap();

        assert_eq!(restored, 1);
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
        let events = EventStore::list_all(&root).unwrap();
        assert!(events.iter().any(|event| event.event_type == "ROLLBACK_COMPLETED"));
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
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
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

        let pointers =
            PointerStore::new(EnvironmentPaths::new(&root, "api", "production"));
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
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
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
        assert!(root
            .join("projects/api/environments/production/generations/1/snapshot.json")
            .exists());
        let pointers =
            PointerStore::new(EnvironmentPaths::new(&root, "api", "production"));
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
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
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "prod-api-gen-1:3000".into(),
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
                activation: ActivationMode::Http { internal_port: 3000 },
            },
        )
        .execute_next()
        .unwrap();

        assert!(root
            .join("projects/api/environments/production/generations/1/snapshot.json")
            .exists());
        assert_eq!(routing.updates.len(), 1);
    }
}

#[cfg(test)]
pub mod route_targets_generation_specific_container {
    use super::*;
    use crate::docker::DockerCliRuntime;
    use crate::docker::RecordingCommandRunner;

    #[test]
    fn route_target_points_to_generation_specific_container() {
        let root = test_root("route-target");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "prod-api-gen-1:3000".into(),
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
                activation: ActivationMode::Http { internal_port: 3000 },
            },
        )
        .execute_next()
        .unwrap();

        assert_eq!(routing.updates[0].target, "prod-api-gen-1:3000");
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
        writer1.finalize("api", "production", SnapshotState::Healthy).unwrap();
        crate::storage::atomic_write(env.generation_counter(), b"1\n").unwrap();
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queued_record(&queue);
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(2)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "prod-api-gen-2:3000".into(),
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
                activation: ActivationMode::Http { internal_port: 3000 },
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
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "prod-api-gen-1:3000".into(),
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
                activation: ActivationMode::Http { internal_port: 3000 },
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
        let mut docker = DockerCliRuntime::new(RecordingCommandRunner::with_outputs(success_outputs(1)));
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            updates: Vec::new(),
            inspections: vec![RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "prod-api-gen-1:3000".into(),
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
                activation: ActivationMode::Http { internal_port: 3000 },
            },
        )
        .execute_next()
        .unwrap();

        assert_eq!(routing.updates[0].subtree_id, "forge:api:production");
    }
}
