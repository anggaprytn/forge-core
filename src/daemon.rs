use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::api::{
    validate_deployment_request, DeploymentAccepted, DeploymentRequest, DeploymentStatus,
    ErrorResponse,
};
use crate::bootstrap::{BootstrapContext, BootstrapState};
use crate::config::DaemonConfig;
use crate::convergence::{ActiveDeploymentDecider, ConvergenceError, RecoveryOutcome, StartupConvergence};
use crate::queue::{DeploymentRecord, PersistentQueue, QueueError};
use crate::runtime::{DockerRuntime, RoutingRuntime};

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
    pub fn new(config: DaemonConfig, docker_runtime: D, routing_runtime: R, recovery_decider: A) -> Self {
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
                let convergence = StartupConvergence::new(&queue, &self.recovery_decider);
                let outcome = convergence.recover_active_deployment()?;
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
        let queue = self.queue.as_ref().ok_or_else(|| ErrorResponse {
            code: "queue_unavailable".into(),
            message: "queue is unavailable".into(),
        })?;
        let found = queue
            .find_deployment(deployment_id)
            .map_err(queue_error_to_response)?;

        Ok(found.map(|item| DeploymentStatus {
            deployment_id: item.record.deployment_id,
            project_id: item.record.project_id,
            environment: item.record.environment,
            state: item.state,
        }))
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
            restart_policy: "no".into(),
        })
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
}

#[cfg(test)]
#[derive(Default)]
struct NoopRoutingRuntime;

#[cfg(test)]
impl RoutingRuntime for NoopRoutingRuntime {}

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
