use std::fmt::{Display, Formatter};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::api::{
    BackupListResponse, BackupRecord, BackupRestoreResponse, DeploymentAccepted,
    DeploymentHistoryResponse, DeploymentLogs, DeploymentRequest, DeploymentStatus,
    EnvironmentDiagnostics, EnvironmentDiffResponse, EnvironmentVariableReport, ErrorResponse,
    EventList, ServiceLogGroup, validate_deployment_request,
};
use crate::backups::{create_backup, inspect_backup, list_backups, restore_backup};
use crate::bootstrap::{BootstrapContext, BootstrapState};
use crate::config::DaemonConfig;
use crate::convergence::{
    ActiveDeploymentDecider, ConvergenceError, RecoveryOutcome, StartupConvergence,
};
use crate::deployments::{
    DeploymentError, DeploymentExecution, DeploymentExecutor, ExecutionConfig, ValidationPolicy,
};
use crate::events::EventRecord;
use crate::queue::{DeploymentRecord, PersistentQueue, QueueError};
use crate::runtime::{DockerRuntime, ProbeRuntime, RoutingRuntime};
use crate::source::{ResolvedDeploymentSource, SourceResolver, SourceResolverError};
use crate::status::{
    ProjectEnvironmentStatus, load_environment_diagnostics, load_environment_diff,
    load_environment_history, load_project_environment_env_report, load_project_environment_status,
};
use crate::storage::{
    DiagnosticsStore, EnvironmentPaths, EventStore, PersistedActivationMode, PersistedRuntimeInfo,
    RuntimeHealthState, RuntimeStateStore, load_generation_runtime_info,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonState {
    Created,
    WaitingForBootstrap(PathBuf),
    Recovering,
    Ready,
    ShuttingDown,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupStep {
    ConfigLoaded,
    BootstrapReady,
    QueueRecovered,
    HealthLoopsStarted,
}

fn service_log_groups_from_runtime(runtime: &PersistedRuntimeInfo) -> Vec<ServiceLogGroup> {
    if runtime.services.is_empty() {
        return vec![ServiceLogGroup {
            service_id: "default".into(),
            role: if matches!(
                runtime.activation,
                Some(PersistedActivationMode::Http { .. })
            ) {
                "exposed".into()
            } else {
                "internal".into()
            },
            container_name: Some(runtime.container_name.clone()),
            lines: Vec::new(),
        }];
    }

    let startup_order = if runtime.startup_order.is_empty() {
        runtime.services.keys().cloned().collect::<Vec<_>>()
    } else {
        runtime.startup_order.clone()
    };
    startup_order
        .into_iter()
        .filter_map(|service_id| {
            let service = runtime.services.get(&service_id)?;
            Some(ServiceLogGroup {
                service_id: service.service_id.clone(),
                role: if service.externally_exposed {
                    "exposed".into()
                } else {
                    "internal".into()
                },
                container_name: Some(service.container_name.clone()),
                lines: Vec::new(),
            })
        })
        .collect()
}

fn structured_service_log_artifact_name(service_id: &str) -> String {
    format!("services/{service_id}/container_logs_tail.log")
}

fn flat_service_log_artifact_name(service_id: &str) -> String {
    format!("service-{service_id}-container_logs_tail.log")
}

fn read_service_log_lines(
    diagnostics: &DiagnosticsStore,
    service_id: &str,
) -> Result<Option<Vec<String>>, ErrorResponse> {
    let logs = diagnostics
        .read_text_artifact(&structured_service_log_artifact_name(service_id))
        .map_err(|err| ErrorResponse {
            code: "logs_unavailable".into(),
            message: err.to_string(),
        })?
        .or(diagnostics
            .read_text_artifact(&flat_service_log_artifact_name(service_id))
            .map_err(|err| ErrorResponse {
                code: "logs_unavailable".into(),
                message: err.to_string(),
            })?);
    Ok(logs.map(|value| value.lines().map(|line| line.to_string()).collect()))
}

fn discover_service_log_artifacts(
    diagnostics: &DiagnosticsStore,
) -> Result<Vec<String>, ErrorResponse> {
    let dir = diagnostics.diagnostics_dir();
    let mut service_ids = std::collections::BTreeSet::new();
    if let Ok(entries) = fs::read_dir(&dir) {
        for entry in entries {
            let entry = entry.map_err(|err| ErrorResponse {
                code: "logs_unavailable".into(),
                message: err.to_string(),
            })?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(service_id) = name
                .strip_prefix("service-")
                .and_then(|value| value.strip_suffix("-container_logs_tail.log"))
            {
                service_ids.insert(service_id.to_string());
            }
        }
    }
    let services_dir = dir.join("services");
    if let Ok(entries) = fs::read_dir(services_dir) {
        for entry in entries {
            let entry = entry.map_err(|err| ErrorResponse {
                code: "logs_unavailable".into(),
                message: err.to_string(),
            })?;
            if entry.path().join("container_logs_tail.log").exists() {
                service_ids.insert(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    Ok(service_ids.into_iter().collect())
}

#[derive(Debug)]
pub enum DaemonError {
    Bootstrap(crate::bootstrap::BootstrapError),
    Convergence(ConvergenceError),
    Queue(QueueError),
}

impl Display for DaemonError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bootstrap(err) => write!(f, "{err}"),
            Self::Convergence(err) => write!(f, "{err}"),
            Self::Queue(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for DaemonError {}

impl From<crate::bootstrap::BootstrapError> for DaemonError {
    fn from(value: crate::bootstrap::BootstrapError) -> Self {
        Self::Bootstrap(value)
    }
}

impl From<QueueError> for DaemonError {
    fn from(value: QueueError) -> Self {
        Self::Queue(value)
    }
}

impl From<ConvergenceError> for DaemonError {
    fn from(value: ConvergenceError) -> Self {
        Self::Convergence(value)
    }
}

pub struct Daemon<D, R, A> {
    config: DaemonConfig,
    docker_runtime: D,
    routing_runtime: R,
    recovery_decider: A,
    state: DaemonState,
    startup_steps: Vec<StartupStep>,
    queue: Option<PersistentQueue>,
    health_loops_started: bool,
    last_recovery_outcome: Option<RecoveryOutcome>,
}

impl<D, R, A> Daemon<D, R, A>
where
    D: DockerRuntime,
    R: RoutingRuntime,
    A: ActiveDeploymentDecider,
{
    pub fn new(
        config: DaemonConfig,
        docker_runtime: D,
        routing_runtime: R,
        recovery_decider: A,
    ) -> Self {
        let startup_steps = vec![StartupStep::ConfigLoaded];
        Self {
            config,
            docker_runtime,
            routing_runtime,
            recovery_decider,
            state: DaemonState::Created,
            startup_steps,
            queue: None,
            health_loops_started: false,
            last_recovery_outcome: None,
        }
    }

    pub fn start(&mut self) -> Result<(), DaemonError> {
        let bootstrap = BootstrapContext::new(self.config.clone());
        match bootstrap.initialize()? {
            BootstrapState::WaitingForStorage(path) => {
                self.state = DaemonState::WaitingForBootstrap(path);
                self.health_loops_started = false;
                Ok(())
            }
            BootstrapState::Ready => {
                self.state = DaemonState::Recovering;
                self.startup_steps.push(StartupStep::BootstrapReady);

                let queue = PersistentQueue::new(self.config.storage_root.join("queue"))?;
                let convergence = StartupConvergence::new(
                    self.config.storage_root.clone(),
                    &queue,
                    &self.recovery_decider,
                );
                let outcome = convergence.recover_active_deployment(
                    &mut self.docker_runtime,
                    &mut self.routing_runtime,
                )?;
                self.last_recovery_outcome = Some(outcome);
                self.startup_steps.push(StartupStep::QueueRecovered);

                self.queue = Some(queue);
                self.health_loops_started = true;
                self.startup_steps.push(StartupStep::HealthLoopsStarted);
                self.state = DaemonState::Ready;
                Ok(())
            }
        }
    }

    pub fn handle_post_deployments(
        &mut self,
        request: DeploymentRequest,
    ) -> Result<DeploymentAccepted, ErrorResponse> {
        validate_deployment_request(&request)?;
        if self.state != DaemonState::Ready {
            return Err(ErrorResponse {
                code: "daemon_not_ready".into(),
                message: "daemon is not ready to accept commands".into(),
            });
        }

        let queue = self.queue.as_ref().ok_or_else(|| ErrorResponse {
            code: "queue_unavailable".into(),
            message: "queue is unavailable".into(),
        })?;

        let deployment_id = next_deployment_id();
        let resolved_source = resolve_deployment_source(&self.config.storage_root, &request)?;
        let record = DeploymentRecord {
            deployment_id: deployment_id.clone(),
            project_id: request.project_id,
            environment: request.environment,
            intent: request.intent,
            source_path: resolved_source.source_path,
            source_ref: resolved_source.source_ref,
            repo_url: resolved_source.repo_url,
            commit_sha: resolved_source.commit_sha,
        };
        queue.enqueue(record).map_err(queue_error_to_response)?;
        let queue_position = queue.queued_len().map_err(queue_error_to_response)?;

        Ok(DeploymentAccepted {
            deployment_id,
            queue_position,
        })
    }

    pub fn get_deployment(
        &self,
        deployment_id: &str,
    ) -> Result<Option<DeploymentStatus>, ErrorResponse> {
        if let Some(queue) = self.queue.as_ref() {
            let found = queue
                .find_deployment(deployment_id)
                .map_err(queue_error_to_response)?;

            if let Some(item) = found {
                return Ok(Some(DeploymentStatus {
                    deployment_id: item.record.deployment_id,
                    project_id: item.record.project_id,
                    environment: item.record.environment,
                    state: item.state,
                }));
            }
        }

        for entry in
            persisted_deployments(&self.config.storage_root).map_err(|err| ErrorResponse {
                code: "status_lookup_failed".into(),
                message: err.to_string(),
            })?
        {
            if entry.deployment_id == deployment_id {
                let runtime_state = RuntimeStateStore::new(EnvironmentPaths::new(
                    &self.config.storage_root,
                    &entry.project_id,
                    &entry.environment,
                ))
                .load()
                .map_err(|err| ErrorResponse {
                    code: "status_lookup_failed".into(),
                    message: err.to_string(),
                })?;
                let state = match runtime_state.health_state {
                    RuntimeHealthState::Healthy => "healthy",
                    RuntimeHealthState::Degraded => "degraded",
                    RuntimeHealthState::Unavailable => "unavailable",
                };
                return Ok(Some(DeploymentStatus {
                    deployment_id: entry.deployment_id,
                    project_id: entry.project_id,
                    environment: entry.environment,
                    state: state.into(),
                }));
            }
        }

        Ok(None)
    }

    pub fn list_events(&self) -> Result<EventList, ErrorResponse> {
        let events =
            EventStore::list_all(&self.config.storage_root).map_err(|err| ErrorResponse {
                code: "events_unavailable".into(),
                message: err.to_string(),
            })?;
        Ok(EventList { events })
    }

    pub fn get_project_environment_status(
        &mut self,
        project_id: &str,
        environment: &str,
    ) -> Result<ProjectEnvironmentStatus, ErrorResponse> {
        load_project_environment_status(
            &self.config.storage_root,
            self.queue.as_ref(),
            &mut self.docker_runtime,
            &mut self.routing_runtime,
            project_id,
            environment,
        )
        .map_err(|err| {
            let (status, response) = crate::status::project_status_error_response(err);
            let _ = status;
            response
        })
    }

    pub fn get_project_environment_diagnostics(
        &mut self,
        project_id: &str,
        environment: &str,
    ) -> Result<EnvironmentDiagnostics, ErrorResponse> {
        load_environment_diagnostics(
            &self.config.storage_root,
            self.queue.as_ref(),
            &mut self.docker_runtime,
            &mut self.routing_runtime,
            project_id,
            environment,
        )
        .map_err(|err| {
            let (status, response) = crate::status::project_status_error_response(err);
            let _ = status;
            response
        })
    }

    pub fn get_project_environment_history(
        &mut self,
        project_id: &str,
        environment: &str,
    ) -> Result<DeploymentHistoryResponse, ErrorResponse> {
        load_environment_history(
            &self.config.storage_root,
            self.queue.as_ref(),
            &mut self.docker_runtime,
            &mut self.routing_runtime,
            project_id,
            environment,
        )
        .map_err(|err| {
            let (status, response) = crate::status::project_status_error_response(err);
            let _ = status;
            response
        })
    }

    pub fn get_project_environment_env(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<EnvironmentVariableReport, ErrorResponse> {
        load_project_environment_env_report(&self.config.storage_root, project_id, environment)
            .map_err(|err| {
                let (status, response) = crate::status::project_status_error_response(err);
                let _ = status;
                response
            })
    }

    pub fn get_project_environment_env_diff(
        &self,
        project_id: &str,
        environment: &str,
        from_generation: u64,
        to_generation: u64,
    ) -> Result<EnvironmentDiffResponse, ErrorResponse> {
        load_environment_diff(
            &self.config.storage_root,
            project_id,
            environment,
            from_generation,
            to_generation,
        )
        .map_err(|err| {
            let (status, response) = crate::status::project_status_error_response(err);
            let _ = status;
            response
        })
    }

    pub fn create_backup(
        &mut self,
        project_id: &str,
        environment: &str,
    ) -> Result<BackupRecord, ErrorResponse> {
        create_backup(
            &self.config.storage_root,
            &mut self.docker_runtime,
            project_id,
            environment,
        )
        .map_err(|err| ErrorResponse {
            code: "backup_create_failed".into(),
            message: err.to_string(),
        })
    }

    pub fn list_backups(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<BackupListResponse, ErrorResponse> {
        list_backups(&self.config.storage_root, project_id, environment).map_err(|err| {
            ErrorResponse {
                code: "backup_list_failed".into(),
                message: err.to_string(),
            }
        })
    }

    pub fn inspect_backup(&self, backup_id: &str) -> Result<BackupRecord, ErrorResponse> {
        inspect_backup(&self.config.storage_root, backup_id).map_err(|err| ErrorResponse {
            code: "backup_inspect_failed".into(),
            message: err.to_string(),
        })
    }

    pub fn restore_backup(
        &mut self,
        backup_id: &str,
    ) -> Result<BackupRestoreResponse, ErrorResponse> {
        restore_backup(
            &self.config.storage_root,
            &mut self.docker_runtime,
            &mut self.routing_runtime,
            backup_id,
        )
        .map_err(|err| ErrorResponse {
            code: "backup_restore_failed".into(),
            message: err.to_string(),
        })
    }

    pub fn get_deployment_logs(
        &self,
        deployment_id: &str,
        service_id: Option<&str>,
    ) -> Result<DeploymentLogs, ErrorResponse> {
        let Some(entry) = persisted_deployments(&self.config.storage_root)
            .map_err(|err| ErrorResponse {
                code: "logs_unavailable".into(),
                message: err.to_string(),
            })?
            .into_iter()
            .find(|entry| entry.deployment_id == deployment_id)
        else {
            return Err(ErrorResponse {
                code: "deployment_not_found".into(),
                message: "deployment logs unavailable; run `forge diagnose <project> <environment>` or diagnostics may have been removed by retention".into(),
            });
        };

        let env = EnvironmentPaths::new(
            &self.config.storage_root,
            &entry.project_id,
            &entry.environment,
        );
        let diagnostics = DiagnosticsStore::new(env.clone(), entry.generation);
        let lines = diagnostics.read_log_lines().map_err(|err| ErrorResponse {
            code: "logs_unavailable".into(),
            message: err.to_string(),
        })?;
        let runtime =
            load_generation_runtime_info(&env, entry.generation).map_err(|err| ErrorResponse {
                code: "logs_unavailable".into(),
                message: err.to_string(),
            })?;
        let mut services = runtime
            .as_ref()
            .map(service_log_groups_from_runtime)
            .unwrap_or_default();
        let discovered_service_ids = discover_service_log_artifacts(&diagnostics)?;
        if services.is_empty() && !discovered_service_ids.is_empty() {
            services = discovered_service_ids
                .iter()
                .map(|service_id| {
                    let runtime_service = runtime
                        .as_ref()
                        .and_then(|value| value.services.get(service_id));
                    ServiceLogGroup {
                        service_id: service_id.clone(),
                        role: runtime_service
                            .map(|service| {
                                if service.externally_exposed {
                                    "exposed".to_string()
                                } else {
                                    "internal".to_string()
                                }
                            })
                            .unwrap_or_else(|| "unknown".into()),
                        container_name: runtime_service
                            .map(|service| service.container_name.clone()),
                        lines: Vec::new(),
                    }
                })
                .collect();
        }
        let runtime_is_multiservice = runtime
            .as_ref()
            .is_some_and(|value| !value.services.is_empty());
        for group in &mut services {
            group.lines =
                read_service_log_lines(&diagnostics, &group.service_id)?.unwrap_or_else(|| {
                    if runtime_is_multiservice {
                        vec!["service logs unavailable for this generation".into()]
                    } else {
                        Vec::new()
                    }
                });
        }
        if services.is_empty() {
            let container_logs = diagnostics
                .read_text_artifact("container_logs_tail.log")
                .map_err(|err| ErrorResponse {
                    code: "logs_unavailable".into(),
                    message: err.to_string(),
                })?
                .unwrap_or_default()
                .lines()
                .map(|line| line.to_string())
                .collect::<Vec<_>>();
            services.push(ServiceLogGroup {
                service_id: "default".into(),
                role: "exposed".into(),
                container_name: runtime.as_ref().map(|value| value.container_name.clone()),
                lines: container_logs.clone(),
            });
        }
        let selected_service = service_id.map(|value| value.to_string());
        let services = if let Some(service_id) = service_id {
            let Some(group) = services
                .into_iter()
                .find(|group| group.service_id == service_id)
            else {
                return Err(ErrorResponse {
                    code: "service_not_found".into(),
                    message: format!(
                        "service `{service_id}` not found for deployment {deployment_id}"
                    ),
                });
            };
            vec![group]
        } else {
            services
        };
        let container_logs = if services.len() == 1 {
            services[0].lines.clone()
        } else {
            Vec::new()
        };
        let validation_failure_summary = diagnostics
            .read_summary()
            .map_err(|err| ErrorResponse {
                code: "logs_unavailable".into(),
                message: err.to_string(),
            })?
            .map(|summary| format!("{}: {}", summary.failure_stage, summary.failure_reason));
        let lifecycle = lines.clone();

        let diagnostics_source = format!(
            "projects/{}/environments/{}/generations/{}/diagnostics",
            entry.project_id, entry.environment, entry.generation
        );

        Ok(DeploymentLogs {
            deployment_id: entry.deployment_id,
            project_id: entry.project_id,
            environment: entry.environment,
            lines,
            lifecycle,
            container_logs,
            services,
            selected_service,
            validation_failure_summary,
            diagnostics_source: Some(diagnostics_source),
        })
    }

    pub fn queue_depth(&self) -> Result<usize, ErrorResponse> {
        let Some(queue) = self.queue.as_ref() else {
            return Err(ErrorResponse {
                code: "queue_unavailable".into(),
                message: "queue is unavailable".into(),
            });
        };
        queue.queued_len().map_err(queue_error_to_response)
    }

    pub fn graceful_shutdown(&mut self) {
        self.state = DaemonState::ShuttingDown;
        self.health_loops_started = false;
        self.state = DaemonState::Stopped;
    }

    pub fn state(&self) -> &DaemonState {
        &self.state
    }

    pub fn startup_steps(&self) -> &[StartupStep] {
        &self.startup_steps
    }

    pub fn health_loops_started(&self) -> bool {
        self.health_loops_started
    }

    pub fn last_recovery_outcome(&self) -> Option<&RecoveryOutcome> {
        self.last_recovery_outcome.as_ref()
    }

    pub fn queue(&self) -> Option<&PersistentQueue> {
        self.queue.as_ref()
    }

    pub fn runtimes(&self) -> (&D, &R) {
        (&self.docker_runtime, &self.routing_runtime)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentWorkerSettings {
    pub validation: ValidationPolicy,
    pub execution: ExecutionConfig,
    pub idle_sleep: Duration,
}

impl Default for DeploymentWorkerSettings {
    fn default() -> Self {
        Self {
            validation: ValidationPolicy::default(),
            execution: ExecutionConfig::default(),
            idle_sleep: Duration::from_millis(200),
        }
    }
}

pub fn execute_next_queued_deployment<D, P, R>(
    storage_root: impl Into<PathBuf>,
    queue: &PersistentQueue,
    docker: &mut D,
    probes: &mut P,
    routing: &mut R,
    settings: &DeploymentWorkerSettings,
) -> Result<Option<DeploymentExecution>, DeploymentError>
where
    D: DockerRuntime,
    P: ProbeRuntime,
    R: RoutingRuntime,
{
    DeploymentExecutor::new(
        storage_root,
        queue,
        docker,
        probes,
        routing,
        settings.validation.clone(),
    )
    .with_execution_config(settings.execution.clone())
    .execute_next()
}

pub fn run_deployment_worker_loop<D, P, R>(
    storage_root: impl Into<PathBuf>,
    queue: PersistentQueue,
    mut docker: D,
    mut probes: P,
    mut routing: R,
    settings: DeploymentWorkerSettings,
) -> !
where
    D: DockerRuntime,
    P: ProbeRuntime,
    R: RoutingRuntime,
{
    let storage_root = storage_root.into();
    loop {
        let did_work = match execute_next_queued_deployment(
            storage_root.clone(),
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            &settings,
        ) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(err) => {
                eprintln!("forge daemon worker deployment failed: {err}");
                true
            }
        };

        if !did_work {
            thread::sleep(settings.idle_sleep);
        }
    }
}

fn queue_error_to_response(error: QueueError) -> ErrorResponse {
    ErrorResponse {
        code: "queue_error".into(),
        message: error.to_string(),
    }
}

fn next_deployment_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("dep-{now}-{seq}")
}

fn resolve_deployment_source(
    storage_root: &std::path::Path,
    request: &DeploymentRequest,
) -> Result<ResolvedDeploymentSource, ErrorResponse> {
    if request.intent == "rollback" {
        return Ok(ResolvedDeploymentSource {
            source_path: None,
            source_ref: None,
            repo_url: None,
            commit_sha: None,
        });
    }

    SourceResolver::new(storage_root)
        .resolve(
            &request.project_id,
            request.source_path.as_deref(),
            request.source_ref.as_deref(),
        )
        .map_err(|err| {
            let response = source_resolver_error_to_response(err);
            eprintln!(
                "forge source resolution failed: project={} environment={} reason={}",
                request.project_id, request.environment, response.message
            );
            response
        })
}

fn source_resolver_error_to_response(err: SourceResolverError) -> ErrorResponse {
    match err {
        SourceResolverError::ProjectNotFound(project_id) => ErrorResponse {
            code: "project_not_found".into(),
            message: format!("project is not registered: {project_id}"),
        },
        SourceResolverError::InvalidSourcePath(message) => ErrorResponse {
            code: "invalid_source_path".into(),
            message,
        },
        SourceResolverError::InvalidSourceRef => ErrorResponse {
            code: "invalid_source_ref".into(),
            message: "source_ref must not be empty".into(),
        },
        SourceResolverError::InvalidRepoUrl(message) => ErrorResponse {
            code: "invalid_repo_url".into(),
            message,
        },
        SourceResolverError::ProjectRegistry(err) => ErrorResponse {
            code: "project_registry_unavailable".into(),
            message: err.to_string(),
        },
        SourceResolverError::GitCommand(message) => ErrorResponse {
            code: "git_source_unavailable".into(),
            message,
        },
        SourceResolverError::CheckoutConflict {
            path,
            repo_url,
            source_ref,
            commit_sha,
        } => ErrorResponse {
            code: "source_checkout_conflict".into(),
            message: format!(
                "source checkout path already exists but does not match the requested commit: path={} repo={} ref={} sha={}",
                path.display(),
                repo_url,
                source_ref,
                commit_sha
            ),
        },
        SourceResolverError::Io(err) => ErrorResponse {
            code: "source_resolution_failed".into(),
            message: err.to_string(),
        },
    }
}

struct PersistedDeployment {
    deployment_id: String,
    project_id: String,
    environment: String,
    generation: u64,
}

fn persisted_deployments(
    root: &std::path::Path,
) -> Result<Vec<PersistedDeployment>, std::io::Error> {
    let projects_root = root.join("projects");
    let mut deployments = Vec::new();
    if !projects_root.exists() {
        return Ok(deployments);
    }
    for project in std::fs::read_dir(projects_root)? {
        let project = project?;
        if !project.file_type()?.is_dir() {
            continue;
        }
        let project_id = project.file_name().to_string_lossy().to_string();
        let envs = project.path().join("environments");
        if !envs.exists() {
            continue;
        }
        for env in std::fs::read_dir(envs)? {
            let env = env?;
            let environment = env.file_name().to_string_lossy().to_string();
            let generations = env.path().join("generations");
            if !generations.exists() {
                continue;
            }
            for generation in std::fs::read_dir(generations)? {
                let generation = generation?;
                let generation_id = generation.file_name().to_string_lossy().parse::<u64>().ok();
                let deployment_id = read_generation_deployment_id(&generation.path())?;
                if let (Some(generation), Some(deployment_id)) = (generation_id, deployment_id) {
                    deployments.push(PersistedDeployment {
                        deployment_id,
                        project_id: project_id.clone(),
                        environment: environment.clone(),
                        generation,
                    });
                }
            }
        }
    }
    Ok(deployments)
}

fn read_generation_deployment_id(path: &std::path::Path) -> Result<Option<String>, std::io::Error> {
    let build = path.join("build.json");
    if build.exists() {
        let raw = std::fs::read_to_string(build)?;
        if let Some(deployment_id) = extract_json_string(&raw, "deployment_id") {
            return Ok(Some(deployment_id));
        }
    }

    let summary = path.join("diagnostics").join("summary.json");
    if summary.exists() {
        let raw = std::fs::read_to_string(summary)?;
        if let Some(deployment_id) = extract_json_string(&raw, "deployment_id") {
            return Ok(Some(deployment_id));
        }
    }

    let events = path.join("events.jsonl");
    if !events.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(events)?;
    for line in raw.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let event = serde_json::from_str::<EventRecord>(line)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
        if let Some(deployment_id) = event.deployment_id {
            return Ok(Some(deployment_id));
        }
    }
    Ok(None)
}

fn extract_json_string(raw: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\": \"");
    let start = raw.find(&needle)? + needle.len();
    let tail = &raw[start..];
    let end = tail.find('"')?;
    Some(tail[..end].to_string())
}

#[cfg(test)]
fn test_root(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let base = std::env::temp_dir().join(format!(
        "forge-core-tests-{name}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&base).unwrap();
    base
}

#[cfg(test)]
#[derive(Default)]
struct NoopDockerRuntime;

#[cfg(test)]
impl DockerRuntime for NoopDockerRuntime {
    fn build_image(
        &mut self,
        request: crate::runtime::BuildImageRequest,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        Ok(request.image_tag)
    }

    fn ensure_network(
        &mut self,
        _network_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }

    fn ensure_volume(
        &mut self,
        _request: crate::runtime::CreateVolumeRequest,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }

    fn create_container(
        &mut self,
        request: crate::runtime::CreateContainerRequest,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        Ok(request.container_name)
    }

    fn start_container(
        &mut self,
        _container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }

    fn inspect_container(
        &mut self,
        container_name: &str,
    ) -> Result<crate::runtime::ContainerInspection, crate::runtime::DockerRuntimeError> {
        Ok(crate::runtime::ContainerInspection {
            container_name: container_name.to_string(),
            running: true,
            state_status: "running".into(),
            exit_code: Some(0),
            restart_count: 0,
            started_at: None,
            finished_at: None,
            oom_killed: false,
            error: None,
            image_ref: "noop".into(),
            labels: Default::default(),
            network_ips: std::collections::BTreeMap::from([(
                "forge-managed".into(),
                "172.18.0.2".into(),
            )]),
            volume_mounts: Vec::new(),
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
        _container_name: &str,
        _tail_lines: usize,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        Ok(String::new())
    }

    fn list_managed_containers(
        &mut self,
    ) -> Result<Vec<crate::runtime::ContainerInspection>, crate::runtime::DockerRuntimeError> {
        Ok(Vec::new())
    }

    fn list_managed_images(
        &mut self,
    ) -> Result<Vec<crate::runtime::ManagedImage>, crate::runtime::DockerRuntimeError> {
        Ok(Vec::new())
    }

    fn list_managed_volumes(
        &mut self,
    ) -> Result<Vec<crate::runtime::ManagedVolume>, crate::runtime::DockerRuntimeError> {
        Ok(Vec::new())
    }

    fn stop_container(
        &mut self,
        _container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }

    fn remove_container(
        &mut self,
        _container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }

    fn remove_image(&mut self, _image_ref: &str) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }

    fn remove_volume(
        &mut self,
        _volume_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }
}

#[cfg(test)]
#[derive(Default)]
struct NoopRoutingRuntime;

#[cfg(test)]
impl RoutingRuntime for NoopRoutingRuntime {
    fn update_route(
        &mut self,
        _request: crate::runtime::RouteUpdateRequest,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        Ok(())
    }

    fn inspect_route(
        &mut self,
        subtree_id: &str,
    ) -> Result<crate::runtime::RouteInspection, crate::runtime::RoutingRuntimeError> {
        let environment = subtree_id.rsplit(':').next().unwrap_or("production");
        let domain = match environment {
            "staging" => Some("staging-api.example.com".into()),
            "development" => Some("development-api.example.com".into()),
            _ => Some("api.example.com".into()),
        };
        Ok(crate::runtime::RouteInspection {
            subtree_id: subtree_id.to_string(),
            active_target: "172.18.0.2:3000".into(),
            domain,
            activation_verified: true,
            verification_url: None,
            verification_host: None,
            verification_status_code: None,
            verification_response_body: None,
            health_checks_enabled: false,
        })
    }

    fn list_managed_routes(
        &mut self,
    ) -> Result<Vec<crate::runtime::RouteInspection>, crate::runtime::RoutingRuntimeError> {
        Ok(Vec::new())
    }

    fn remove_route(
        &mut self,
        _subtree_id: &str,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        Ok(())
    }
}

#[cfg(test)]
struct StaticProbeRuntime {
    tcp_ok: bool,
    http_ok: bool,
}

#[cfg(test)]
impl ProbeRuntime for StaticProbeRuntime {
    fn probe_tcp(
        &mut self,
        _container_name: &str,
        _internal_port: u16,
    ) -> Result<bool, crate::runtime::ProbeError> {
        Ok(self.tcp_ok)
    }

    fn probe_http(
        &mut self,
        _container_name: &str,
        _internal_port: u16,
        _path: &str,
    ) -> Result<bool, crate::runtime::ProbeError> {
        Ok(self.http_ok)
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct StaticDecider(bool);

#[cfg(test)]
impl ActiveDeploymentDecider for StaticDecider {
    fn should_resume(&self, _deployment: &DeploymentRecord) -> bool {
        self.0
    }
}

#[cfg(test)]
fn config_with_root(root: PathBuf) -> DaemonConfig {
    DaemonConfig {
        storage_root: root,
        api_bind: "127.0.0.1:8080".into(),
        bearer_token: "test-token".into(),
        github_webhook_secret: None,
        repository_cache_root: None,
        sqlite_path: None,
    }
}

#[cfg(test)]
pub mod daemon_starts_only_after_bootstrap_succeeds {
    use super::*;

    #[test]
    fn daemon_waits_when_storage_root_is_missing() {
        let root = test_root("daemon-bootstrap-waiting").join("missing");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );

        daemon.start().unwrap();

        assert_eq!(daemon.state(), &DaemonState::WaitingForBootstrap(root));
        assert!(!daemon.health_loops_started());
    }

    #[test]
    fn daemon_becomes_ready_after_bootstrap_and_recovery() {
        let root = test_root("daemon-bootstrap-ready");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );

        daemon.start().unwrap();

        assert_eq!(daemon.state(), &DaemonState::Ready);
        assert_eq!(
            daemon.startup_steps(),
            &[
                StartupStep::ConfigLoaded,
                StartupStep::BootstrapReady,
                StartupStep::QueueRecovered,
                StartupStep::HealthLoopsStarted
            ]
        );
    }
}

#[cfg(test)]
pub mod daemon_refuses_api_commands_before_ready {
    use super::*;

    #[test]
    fn post_deployments_is_rejected_when_daemon_is_not_ready() {
        let root = test_root("daemon-not-ready").join("missing");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );

        let response = daemon.handle_post_deployments(DeploymentRequest {
            project_id: "api".into(),
            environment: "production".into(),
            intent: "deploy".into(),
            source_path: None,
            source_ref: None,
        });

        assert_eq!(
            response.unwrap_err(),
            ErrorResponse {
                code: "daemon_not_ready".into(),
                message: "daemon is not ready to accept commands".into(),
            }
        );
    }
}

#[cfg(test)]
pub mod daemon_recovers_queue_before_accepting_deploys {
    use super::*;
    use crate::queue::PersistentQueue;

    #[test]
    fn active_queue_recovery_happens_before_ready_state() {
        let root = test_root("daemon-recovery-before-ready");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "d1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        queue.start_next().unwrap().unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(false),
        );

        daemon.start().unwrap();

        assert_eq!(
            daemon.last_recovery_outcome(),
            Some(&RecoveryOutcome::Failed(DeploymentRecord {
                deployment_id: "d1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            }))
        );
        assert_eq!(daemon.startup_steps()[1], StartupStep::BootstrapReady);
        assert_eq!(daemon.startup_steps()[2], StartupStep::QueueRecovered);
        assert_eq!(daemon.state(), &DaemonState::Ready);
    }
}

#[cfg(test)]
pub mod daemon_drains_shutdown_safely {
    use super::*;

    #[test]
    fn shutdown_stops_health_loops_and_refuses_new_commands() {
        let root = test_root("daemon-shutdown");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        daemon.graceful_shutdown();

        assert_eq!(daemon.state(), &DaemonState::Stopped);
        assert!(!daemon.health_loops_started());
        let response = daemon.handle_post_deployments(DeploymentRequest {
            project_id: "api".into(),
            environment: "production".into(),
            intent: "deploy".into(),
            source_path: None,
            source_ref: None,
        });
        assert!(response.is_err());
    }
}

#[cfg(test)]
pub mod daemon_does_not_start_health_loops_before_convergence_completes {
    use super::*;
    use crate::queue::PersistentQueue;

    #[test]
    fn startup_order_keeps_health_loops_after_queue_recovery() {
        let root = test_root("daemon-health-loop-order");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "d1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        queue.start_next().unwrap().unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );

        assert!(!daemon.health_loops_started());
        daemon.start().unwrap();

        assert_eq!(
            daemon.startup_steps(),
            &[
                StartupStep::ConfigLoaded,
                StartupStep::BootstrapReady,
                StartupStep::QueueRecovered,
                StartupStep::HealthLoopsStarted
            ]
        );
        assert!(daemon.health_loops_started());
    }
}

#[cfg(test)]
pub mod post_deployments_enqueues_job_and_persists_across_restart {
    use super::*;

    #[test]
    fn valid_request_enqueues_and_survives_daemon_restart() {
        let root = test_root("daemon-post-deployments");
        std::fs::create_dir_all(root.join("source")).unwrap();
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let accepted = daemon
            .handle_post_deployments(DeploymentRequest {
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.join("source")),
                source_ref: None,
            })
            .unwrap();

        assert_eq!(accepted.queue_position, 1);
        assert!(accepted.deployment_id.starts_with("dep-"));
        assert_eq!(daemon.queue().unwrap().queued_len().unwrap(), 1);

        let mut restarted = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        restarted.start().unwrap();

        let state = restarted.queue().unwrap().load_state().unwrap();
        assert_eq!(state.queued.len(), 1);
        assert_eq!(state.queued[0].deployment_id, accepted.deployment_id);
        assert_eq!(
            state.queued[0].source_path,
            Some(root.join("source").canonicalize().unwrap())
        );
    }
}

#[cfg(test)]
pub mod deploy_from_path_rejects_missing_directory {
    use super::*;

    #[test]
    fn deploy_from_path_rejects_missing_directory() {
        let root = test_root("daemon-missing-source-path");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let response = daemon
            .handle_post_deployments(DeploymentRequest {
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.join("missing")),
                source_ref: None,
            })
            .unwrap_err();

        assert_eq!(response.code, "invalid_source_path");
        assert!(response.message.contains("missing"));
    }
}

#[cfg(test)]
pub mod source_resolution_failure_reports_repo_ref_and_sha {
    use super::*;

    #[test]
    fn checkout_conflict_response_includes_resolution_context() {
        let response = source_resolver_error_to_response(SourceResolverError::CheckoutConflict {
            path: PathBuf::from("/tmp/source-checkouts/api/abc123"),
            repo_url: "https://github.com/example/api.git".into(),
            source_ref: "main".into(),
            commit_sha: "abc123".into(),
        });

        assert_eq!(response.code, "source_checkout_conflict");
        assert!(
            response
                .message
                .contains("repo=https://github.com/example/api.git")
        );
        assert!(response.message.contains("ref=main"));
        assert!(response.message.contains("sha=abc123"));
    }
}

#[cfg(test)]
pub mod daemon_consumes_queued_deployment {
    use super::*;
    use crate::storage::PointerStore;

    #[test]
    fn queued_deployment_executes_through_worker_helper() {
        let root = test_root("daemon-consumes-queued-deployment");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
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

        let execution = execute_next_queued_deployment(
            root.clone(),
            &queue,
            &mut NoopDockerRuntime,
            &mut StaticProbeRuntime {
                tcp_ok: true,
                http_ok: true,
            },
            &mut NoopRoutingRuntime,
            &DeploymentWorkerSettings::default(),
        )
        .unwrap()
        .expect("queued deployment should execute");

        assert_eq!(execution.deployment_id, "dep-1");
        assert_eq!(queue.load_state().unwrap().active, None);
        assert!(queue.load_state().unwrap().queued.is_empty());
        assert_eq!(
            PointerStore::new(EnvironmentPaths::new(&root, "api", "production"))
                .read_pointer("current")
                .unwrap(),
            Some(1)
        );
    }
}

#[cfg(test)]
pub mod daemon_worker_leaves_no_active_queue_item_after_success_or_failure {
    use super::*;

    #[test]
    fn active_queue_item_is_cleared_after_success_and_failure() {
        let success_root = test_root("daemon-worker-clears-active-success");
        let success_queue = PersistentQueue::new(success_root.join("queue")).unwrap();
        success_queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-success".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();

        let success = execute_next_queued_deployment(
            success_root,
            &success_queue,
            &mut NoopDockerRuntime,
            &mut StaticProbeRuntime {
                tcp_ok: true,
                http_ok: true,
            },
            &mut NoopRoutingRuntime,
            &DeploymentWorkerSettings::default(),
        )
        .unwrap();
        assert!(success.is_some());
        assert!(success_queue.load_state().unwrap().active.is_none());

        let failure_root = test_root("daemon-worker-clears-active-failure");
        let failure_queue = PersistentQueue::new(failure_root.join("queue")).unwrap();
        failure_queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-failure".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();

        let failure = execute_next_queued_deployment(
            failure_root,
            &failure_queue,
            &mut NoopDockerRuntime,
            &mut StaticProbeRuntime {
                tcp_ok: false,
                http_ok: false,
            },
            &mut NoopRoutingRuntime,
            &DeploymentWorkerSettings::default(),
        );
        assert!(matches!(
            failure,
            Err(DeploymentError::ValidationFailed("tcp probe failed"))
        ));
        let state = failure_queue.load_state().unwrap();
        assert!(state.active.is_none());
        assert!(state.queued.is_empty());
    }
}

#[cfg(test)]
pub mod deployment_status_reflects_runtime_state {
    use super::*;
    use crate::storage::{
        EnvironmentPaths, RuntimeHealthState, RuntimeState, RuntimeStateStore, SnapshotState,
        SnapshotWriter,
    };

    #[test]
    fn persisted_runtime_state_drives_status_lookup() {
        let root = test_root("deployment-status-runtime-state");
        let env = EnvironmentPaths::new(&root, "api", "production");
        SnapshotWriter::new(env.clone(), 1)
            .unwrap()
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-persisted\",\n  \"image_ref\": \"forge:test\"\n}\n",
            )
            .unwrap();
        SnapshotWriter::new(env.clone(), 1)
            .unwrap()
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(1),
                health_state: RuntimeHealthState::Degraded,
                failed_probe_count: 3,
                successful_probe_count: 0,
                restart_attempted: true,
                degraded_since_unix: Some(100),
                last_transition: "degraded".into(),
                last_error_code: Some("tcp_unreachable".into()),
            })
            .unwrap();

        let daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );

        let status = daemon.get_deployment("dep-persisted").unwrap().unwrap();
        assert_eq!(status.state, "degraded");
        assert_eq!(status.project_id, "api");
    }

    #[test]
    fn logs_missing_deployment_returns_helpful_error() {
        let root = test_root("logs-missing-deployment-returns-helpful-error");
        let daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );

        let err = daemon.get_deployment_logs("dep-missing", None).unwrap_err();
        assert_eq!(err.code, "deployment_not_found");
        assert!(
            err.message
                .contains("forge diagnose <project> <environment>")
        );
        assert!(err.message.contains("removed by retention"));
    }

    #[test]
    fn logs_work_for_early_deploy_failure() {
        let root = test_root("logs-work-for-early-deploy-failure");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
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
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "dep-1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(root.clone()),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
            .unwrap();
        let mut docker = NoopDockerRuntime;
        let mut probes = StaticProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = NoopRoutingRuntime;
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
            network_name: None,
        })
        .execute_next();

        let daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        let logs = daemon.get_deployment_logs("dep-1", None).unwrap();
        assert_eq!(logs.deployment_id, "dep-1");
        assert!(
            logs.lines
                .iter()
                .any(|line| line.contains("deployment started"))
        );
        assert!(
            logs.validation_failure_summary
                .as_deref()
                .unwrap()
                .contains("topology")
        );
    }

    #[test]
    fn logs_can_select_service_for_multiservice_deploy() {
        let root = test_root("logs-can-select-service-for-multiservice-deploy");
        let env = EnvironmentPaths::new(&root, "api", "staging");
        SnapshotWriter::new(env.clone(), 1)
            .unwrap()
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-ms-1\",\n  \"image_ref\": \"forge/api:staging-gen-1\"\n}\n",
            )
            .unwrap();
        SnapshotWriter::new(env.clone(), 1)
            .unwrap()
            .write_artifact(
                "runtime.json",
                concat!(
                    "{\n",
                    "  \"container_name\": \"staging-api-gen-1\",\n",
                    "  \"running\": true,\n",
                    "  \"services\": {\n",
                    "    \"api\": {\n",
                    "      \"service_id\": \"api\",\n",
                    "      \"container_name\": \"staging-api-api-gen-1\",\n",
                    "      \"image_ref\": \"forge/api:staging-gen-1\",\n",
                    "      \"running\": true,\n",
                    "      \"externally_exposed\": true,\n",
                    "      \"activation\": {\"Http\": {\"internal_port\": 3000, \"route_subtree_id\": \"forge:api:staging:api\", \"target_source\": \"ContainerIp\"}}\n",
                    "    },\n",
                    "    \"worker\": {\n",
                    "      \"service_id\": \"worker\",\n",
                    "      \"container_name\": \"staging-api-worker-gen-1\",\n",
                    "      \"image_ref\": \"forge/worker:staging-gen-1\",\n",
                    "      \"running\": true,\n",
                    "      \"depends_on\": [\"api\"],\n",
                    "      \"activation\": \"Direct\"\n",
                    "    }\n",
                    "  },\n",
                    "  \"startup_order\": [\"api\", \"worker\"],\n",
                    "  \"activation\": {\"Http\": {\"internal_port\": 3000, \"route_subtree_id\": \"forge:api:staging\", \"target_source\": \"ContainerIp\"}},\n",
                    "  \"environment_variables\": {}\n",
                    "}\n"
                ),
            )
            .unwrap();
        SnapshotWriter::new(env.clone(), 1)
            .unwrap()
            .finalize("api", "staging", SnapshotState::Healthy)
            .unwrap();
        let diagnostics = DiagnosticsStore::new(env, 1);
        diagnostics
            .append_log_line("generation promoted", &[])
            .unwrap();
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

        let daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        let logs = daemon
            .get_deployment_logs("dep-ms-1", Some("worker"))
            .unwrap();
        assert_eq!(logs.selected_service.as_deref(), Some("worker"));
        assert_eq!(logs.services.len(), 1);
        assert_eq!(logs.services[0].service_id, "worker");
        assert_eq!(logs.services[0].lines, vec!["worker polling".to_string()]);
    }
}
