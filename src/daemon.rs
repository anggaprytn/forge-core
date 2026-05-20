use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::api::{
    DeploymentAccepted, DeploymentLogs, DeploymentRequest, DeploymentStatus, ErrorResponse,
    EventList, validate_deployment_request,
};
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
use crate::storage::{
    DiagnosticsStore, EnvironmentPaths, EventStore, RuntimeHealthState, RuntimeStateStore,
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
        let record = DeploymentRecord {
            deployment_id: deployment_id.clone(),
            project_id: request.project_id,
            environment: request.environment,
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

    pub fn get_deployment_logs(
        &self,
        deployment_id: &str,
    ) -> Result<Option<DeploymentLogs>, ErrorResponse> {
        let Some(entry) = persisted_deployments(&self.config.storage_root)
            .map_err(|err| ErrorResponse {
                code: "logs_unavailable".into(),
                message: err.to_string(),
            })?
            .into_iter()
            .find(|entry| entry.deployment_id == deployment_id)
        else {
            return Ok(None);
        };

        let lines = DiagnosticsStore::new(
            EnvironmentPaths::new(
                &self.config.storage_root,
                &entry.project_id,
                &entry.environment,
            ),
            entry.generation,
        )
        .read_log_lines()
        .map_err(|err| ErrorResponse {
            code: "logs_unavailable".into(),
            message: err.to_string(),
        })?;

        Ok(Some(DeploymentLogs {
            deployment_id: entry.deployment_id,
            lines,
        }))
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
            image_ref: "noop".into(),
            labels: Default::default(),
            network_ips: Default::default(),
            restart_policy: "no".into(),
        })
    }

    fn list_managed_containers(
        &mut self,
    ) -> Result<Vec<crate::runtime::ContainerInspection>, crate::runtime::DockerRuntimeError> {
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
        Ok(crate::runtime::RouteInspection {
            subtree_id: subtree_id.to_string(),
            active_target: String::new(),
            activation_verified: true,
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
            })
            .unwrap();

        assert_eq!(accepted.queue_position, 1);
        assert!(accepted.deployment_id.starts_with("dep-"));
        assert_eq!(daemon.queue().unwrap().queued_len().unwrap(), 1);

        let mut restarted = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        restarted.start().unwrap();

        let state = restarted.queue().unwrap().load_state().unwrap();
        assert_eq!(state.queued.len(), 1);
        assert_eq!(state.queued[0].deployment_id, accepted.deployment_id);
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
}
