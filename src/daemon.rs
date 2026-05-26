use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::api::{
    BackupListResponse, BackupRecord, BackupRestoreResponse, ClusterDiagnostics, ClusterNodeStatus,
    ConvergenceDomainSummary, DependencyBreakerDiagnostics, DeploymentAccepted,
    DeploymentHistoryResponse, DeploymentLogs, DeploymentRequest, DeploymentStatus,
    EnvironmentDiagnostics, EnvironmentDiffResponse, EnvironmentVariableReport, ErrorResponse,
    EventList, MetricsDependencySnapshot, MetricsResponse, NodeInfo, ReadinessTimelineResponse,
    ReadyzReason, ReadyzReasonDiagnostics, ReadyzResponse, ServiceLogGroup,
    validate_deployment_request,
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
use crate::projects::ProjectRegistryStore;
use crate::queue::{DeploymentRecord, PersistentQueue, QueueError};
use crate::readiness::{build_timeline, load_persisted_timeline, persist_timeline};
use crate::reconciliation::{ReconciliationDiagnostics, ReconciliationStore, ReplayOptions};
use crate::route_truth::expected_route_for_runtime;
use crate::runtime::{DockerRuntime, ProbeRuntime, RoutingRuntime};
use crate::source::{ResolvedDeploymentSource, SourceResolver, SourceResolverError};
use crate::status::{
    ProjectEnvironmentStatus, derive_environment_domain, load_environment_diagnostics,
    load_environment_diff, load_environment_history, load_project_environment_env_report,
    load_project_environment_status,
};
use crate::storage::{
    CONTROL_PLANE_SNAPSHOT_RETENTION_LIMIT, ClusterTopologyStore, ControlPlaneSnapshotStore,
    ConvergenceCheckpointStore, DiagnosticsStore, EnvironmentPaths, EventStore, LeaderLeaseStore,
    LeaseAcquireOutcome, NodeMetadataStore, OperationalJournalEntry, OperationalJournalStore,
    PersistedActivationMode, PersistedBreakerState, PersistedClusterNode,
    PersistedControlPlaneSnapshot, PersistedDependencyState, PersistedEnvironmentCheckpoint,
    PersistedLeaderLease, PersistedNodeMetadata, PersistedRuntimeInfo, PersistedServiceRuntimeInfo,
    RuntimeHealthState, RuntimeStateStore, current_unix_timestamp, load_generation_runtime_info,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const READYZ_CACHE_STALE_AFTER_MS: u64 = 5_000;
pub const READYZ_HANDLER_TIMEOUT_MS: u64 = 500;
const READYZ_REFRESH_INTERVAL_MS: u64 = 250;
const CONVERGENCE_STALLED_AFTER_MS: u64 = 15_000;
// This bounds the background readiness refresh loop, not the /readyz request path.
const FILESYSTEM_SCAN_BUDGET_MS: u64 = 500;
const CIRCUIT_BREAKER_FAILURE_THRESHOLD: u32 = 3;
const CIRCUIT_BREAKER_INITIAL_BACKOFF_MS: u64 = 250;
const CIRCUIT_BREAKER_MAX_BACKOFF_MS: u64 = 5_000;
const LEADER_LEASE_TTL_SECONDS: u64 = 5;
const HEARTBEAT_STALE_AFTER_SECONDS: u64 = LEADER_LEASE_TTL_SECONDS * 3;
const READYZ_REASON_DETAILS_KEY: &str = "readyz_reason_details";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupPhase {
    Booting,
    Replaying,
    LeaderAcquiring,
    Follower,
    LeaderActive,
    Degraded,
}

impl StartupPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Booting => "booting",
            Self::Replaying => "replaying",
            Self::LeaderAcquiring => "leader_acquiring",
            Self::Follower => "follower",
            Self::LeaderActive => "leader_active",
            Self::Degraded => "degraded",
        }
    }

    fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Booting, Self::Booting)
                | (Self::Booting, Self::Replaying)
                | (Self::Booting, Self::LeaderAcquiring)
                | (Self::Booting, Self::Follower)
                | (Self::Booting, Self::LeaderActive)
                | (Self::Booting, Self::Degraded)
                | (Self::Replaying, Self::LeaderAcquiring)
                | (Self::Replaying, Self::Degraded)
                | (Self::LeaderAcquiring, Self::Follower)
                | (Self::LeaderAcquiring, Self::LeaderActive)
                | (Self::LeaderAcquiring, Self::Degraded)
                | (Self::Follower, Self::LeaderAcquiring)
                | (Self::Follower, Self::LeaderActive)
                | (Self::Follower, Self::Degraded)
                | (Self::LeaderActive, Self::LeaderAcquiring)
                | (Self::LeaderActive, Self::Follower)
                | (Self::LeaderActive, Self::Degraded)
                | (Self::Degraded, Self::LeaderAcquiring)
                | (Self::Degraded, Self::Follower)
                | (Self::Degraded, Self::LeaderActive)
                | (Self::Degraded, Self::Degraded)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LeadershipState {
    lease: Option<PersistedLeaderLease>,
    uncertain: bool,
    ownership_lost: bool,
    last_error: Option<String>,
}

impl Default for LeadershipState {
    fn default() -> Self {
        Self {
            lease: None,
            uncertain: false,
            ownership_lost: false,
            last_error: None,
        }
    }
}

impl LeadershipState {
    fn is_leader(&self, node_id: &str, now_unix: u64) -> bool {
        self.lease.as_ref().is_some_and(|lease| {
            lease.leader_node_id == node_id && lease.expires_at_unix > now_unix
        })
    }

    fn convergence_owner(&self) -> String {
        self.lease
            .as_ref()
            .map(|lease| lease.leader_node_id.clone())
            .unwrap_or_default()
    }

    fn lease_epoch(&self) -> u64 {
        self.lease
            .as_ref()
            .map(|lease| lease.lease_epoch)
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerLeadership {
    pub node_id: String,
}

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
    StorageInit,
    NodeIdentityLoad,
    LeaseRecovery,
    ReplayCursorLoad,
    ReplayScan,
    ReplayExecution,
    LeadershipAcquisition,
    HeartbeatStart,
    ConvergenceEnable,
    ReadinessPublish,
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
    Storage(crate::storage::StorageError),
}

impl Display for DaemonError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bootstrap(err) => write!(f, "{err}"),
            Self::Convergence(err) => write!(f, "{err}"),
            Self::Queue(err) => write!(f, "{err}"),
            Self::Storage(err) => write!(f, "{err}"),
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

impl From<crate::storage::StorageError> for DaemonError {
    fn from(value: crate::storage::StorageError) -> Self {
        Self::Storage(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct DependencyReadinessState {
    last_known_reachable: Option<bool>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreakerState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DependencyCircuitBreaker {
    state: CircuitBreakerState,
    failure_count: u32,
    last_success_unix: Option<u64>,
    next_retry_unix: Option<u64>,
    last_error: Option<String>,
    last_latency_ms: u64,
}

impl Default for DependencyCircuitBreaker {
    fn default() -> Self {
        Self {
            state: CircuitBreakerState::Closed,
            failure_count: 0,
            last_success_unix: None,
            next_retry_unix: None,
            last_error: None,
            last_latency_ms: 0,
        }
    }
}

impl DependencyCircuitBreaker {
    fn allow_request(&mut self, now_unix: u64) -> bool {
        match self.state {
            CircuitBreakerState::Closed | CircuitBreakerState::HalfOpen => true,
            CircuitBreakerState::Open => {
                if self.next_retry_unix.is_some_and(|value| now_unix >= value) {
                    self.state = CircuitBreakerState::HalfOpen;
                    true
                } else {
                    false
                }
            }
        }
    }

    fn record_success(&mut self, now_unix: u64, latency_ms: u64) {
        self.state = CircuitBreakerState::Closed;
        self.failure_count = 0;
        self.last_success_unix = Some(now_unix);
        self.next_retry_unix = None;
        self.last_error = None;
        self.last_latency_ms = latency_ms;
    }

    fn record_failure(&mut self, now_unix: u64, latency_ms: u64, error: String) {
        self.failure_count = self.failure_count.saturating_add(1);
        self.last_error = Some(error);
        self.last_latency_ms = latency_ms;
        if self.failure_count >= CIRCUIT_BREAKER_FAILURE_THRESHOLD {
            let exponent = self
                .failure_count
                .saturating_sub(CIRCUIT_BREAKER_FAILURE_THRESHOLD)
                .min(5);
            let backoff_ms = (CIRCUIT_BREAKER_INITIAL_BACKOFF_MS << exponent)
                .min(CIRCUIT_BREAKER_MAX_BACKOFF_MS);
            self.state = CircuitBreakerState::Open;
            self.next_retry_unix = Some(now_unix.saturating_add(backoff_ms.div_ceil(1_000)));
        } else {
            self.state = CircuitBreakerState::Closed;
            self.next_retry_unix = None;
        }
    }

    fn diagnostics(&self) -> DependencyBreakerDiagnostics {
        DependencyBreakerDiagnostics {
            state: self.state.as_str().into(),
            failure_count: self.failure_count as u64,
            last_success_unix: self.last_success_unix,
            next_retry_unix: self.next_retry_unix,
            last_error: self.last_error.clone(),
        }
    }

    fn metrics_snapshot(&self) -> MetricsDependencySnapshot {
        MetricsDependencySnapshot {
            probe_latency_ms: self.last_latency_ms,
            breaker: self.diagnostics(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ControlPlaneSnapshot {
    pub readyz: DaemonReadyzCache,
    pub metrics: MetricsResponse,
    pub timeline: ReadinessTimelineResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ClusterSignalState {
    split_brain_suspected: bool,
    multiple_active_reconcilers: bool,
    checkpoint_owner_mismatch: bool,
    snapshot_owner_mismatch: bool,
    lease_epoch_divergence: bool,
    stale_reconciler: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonReadyzCache {
    pub response: ReadyzResponse,
    pub updated_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveReadinessState {
    readiness_status: String,
    readiness_reason: Option<String>,
    active_failure: bool,
}

impl Default for DaemonReadyzCache {
    fn default() -> Self {
        Self {
            response: annotate_readyz_response(ReadyzResponse {
                status: "not_ready".into(),
                startup_phase: StartupPhase::Booting.as_str().into(),
                active_failure: true,
                reason: None,
                reasons: Vec::new(),
            }),
            updated_at_unix_ms: now_unix_ms(),
        }
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
    readyz_cache: DaemonReadyzCache,
    docker_readiness: DependencyReadinessState,
    caddy_readiness: DependencyReadinessState,
    control_plane_snapshot: ControlPlaneSnapshot,
    convergence_loop_duration_ms: u64,
    convergence_last_success_unix: Option<u64>,
    convergence_last_success_restored: bool,
    convergence_last_failure_unix: Option<u64>,
    convergence_failures_total: u64,
    docker_breaker: DependencyCircuitBreaker,
    caddy_breaker: DependencyCircuitBreaker,
    node_metadata: PersistedNodeMetadata,
    leadership: LeadershipState,
    convergence_domains: Vec<ConvergenceDomainSummary>,
    cluster_diagnostics: ClusterDiagnostics,
    cluster_signals: ClusterSignalState,
    reconciliation: ReconciliationDiagnostics,
    startup_phase: StartupPhase,
    startup_started_unix: u64,
    startup_recovery_duration_ms: u64,
    convergence_start_blocked: bool,
}

impl<D, R, A> Drop for Daemon<D, R, A> {
    fn drop(&mut self) {
        let _ = LeaderLeaseStore::new(&self.config.storage_root)
            .release_if_owner(&self.node_metadata.node_id, current_unix_timestamp());
    }
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
        let startup_steps = vec![StartupStep::StorageInit];
        let node_metadata = NodeMetadataStore::new(&config.storage_root)
            .load()
            .ok()
            .flatten()
            .unwrap_or_default();
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
            readyz_cache: DaemonReadyzCache::default(),
            docker_readiness: DependencyReadinessState::default(),
            caddy_readiness: DependencyReadinessState::default(),
            control_plane_snapshot: ControlPlaneSnapshot::default(),
            convergence_loop_duration_ms: 0,
            convergence_last_success_unix: None,
            convergence_last_success_restored: false,
            convergence_last_failure_unix: None,
            convergence_failures_total: 0,
            docker_breaker: DependencyCircuitBreaker::default(),
            caddy_breaker: DependencyCircuitBreaker::default(),
            node_metadata,
            leadership: LeadershipState::default(),
            convergence_domains: Vec::new(),
            cluster_diagnostics: ClusterDiagnostics::default(),
            cluster_signals: ClusterSignalState::default(),
            reconciliation: ReconciliationDiagnostics::default(),
            startup_phase: StartupPhase::Booting,
            startup_started_unix: current_unix_timestamp(),
            startup_recovery_duration_ms: 0,
            convergence_start_blocked: true,
        }
    }

    pub fn start(&mut self) -> Result<(), DaemonError> {
        let startup_started = Instant::now();
        self.startup_started_unix = current_unix_timestamp();
        let bootstrap = BootstrapContext::new(self.config.clone());
        match bootstrap.initialize()? {
            BootstrapState::WaitingForStorage(path) => {
                self.state = DaemonState::WaitingForBootstrap(path);
                self.startup_phase = StartupPhase::Booting;
                self.health_loops_started = false;
                self.convergence_start_blocked = true;
                self.refresh_readyz_cache();
                Ok(())
            }
            BootstrapState::Ready => {
                self.set_startup_phase(StartupPhase::Booting);
                self.node_metadata = NodeMetadataStore::new(&self.config.storage_root)
                    .load_or_create()
                    .unwrap_or_default();
                self.state = DaemonState::Recovering;
                self.record_startup_step(StartupStep::NodeIdentityLoad);
                self.refresh_leadership(current_unix_timestamp());
                self.record_startup_step(StartupStep::LeaseRecovery);
                self.reconciliation =
                    ReconciliationStore::new(&self.config.storage_root).diagnostics();
                self.record_startup_step(StartupStep::ReplayCursorLoad);
                self.record_startup_step(StartupStep::ReplayScan);
                if self.reconciliation_enabled(current_unix_timestamp()) {
                    self.set_startup_phase(StartupPhase::Replaying);
                    let node_id = self.node_id().to_string();
                    let lease_epoch = self.leadership.lease_epoch();
                    self.reconciliation = ReconciliationStore::new(&self.config.storage_root)
                        .replay(
                            &mut self.routing_runtime,
                            &node_id,
                            lease_epoch,
                            ReplayOptions {
                                dry_run: false,
                                resume: true,
                                max_duration_ms: Some(self.config.startup_replay_max_duration_ms),
                                max_entries: Some(self.config.startup_replay_max_entries),
                            },
                        )?
                        .diagnostics;
                }
                self.record_startup_step(StartupStep::ReplayExecution);
                self.set_startup_phase(StartupPhase::LeaderAcquiring);
                self.refresh_leadership(current_unix_timestamp());
                self.record_startup_step(StartupStep::LeadershipAcquisition);
                let _ = OperationalJournalStore::new(&self.config.storage_root).append(
                    &OperationalJournalEntry {
                        schema_version: 1,
                        timestamp_unix: current_unix_timestamp(),
                        event_type: "daemon_restart".into(),
                        project_id: None,
                        environment: None,
                        generation: None,
                        payload: serde_json::json!({ "node_id": self.node_metadata.node_id }),
                    },
                );

                let queue = PersistentQueue::new(self.config.storage_root.join("queue"))?;
                self.queue = Some(queue);
                self.health_loops_started = true;
                self.record_startup_step(StartupStep::HeartbeatStart);
                self.convergence_start_blocked = self.startup_convergence_blocked();
                if !self.convergence_start_blocked {
                    let convergence = StartupConvergence::new(
                        self.config.storage_root.clone(),
                        self.queue.as_ref().expect("queue initialized"),
                        &self.recovery_decider,
                    );
                    let outcome = convergence.recover_active_deployment(
                        &mut self.docker_runtime,
                        &mut self.routing_runtime,
                    )?;
                    self.last_recovery_outcome = Some(outcome);
                }
                self.record_startup_step(StartupStep::ConvergenceEnable);
                if self.reconciliation_enabled(current_unix_timestamp())
                    && !self.convergence_start_blocked
                {
                    self.set_startup_phase(StartupPhase::LeaderActive);
                } else if self.local_role(current_unix_timestamp()) == "follower" {
                    self.set_startup_phase(StartupPhase::Follower);
                } else if self.convergence_start_blocked {
                    self.set_startup_phase(StartupPhase::Degraded);
                }
                self.state = DaemonState::Ready;
                self.startup_recovery_duration_ms = startup_started.elapsed().as_millis() as u64;
                self.restore_readyz_cache_from_checkpoints();
                self.record_startup_step(StartupStep::ReadinessPublish);
                self.refresh_readyz_cache();
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
        self.refresh_leadership(current_unix_timestamp());
        if !self.reconciliation_enabled(current_unix_timestamp()) {
            return Err(not_leader_error_response());
        }

        let queue = self.queue.as_ref().ok_or_else(|| ErrorResponse {
            code: "queue_unavailable".into(),
            message: "queue is unavailable".into(),
        })?;

        let deployment_id = next_deployment_id();
        let resolved_source = resolve_deployment_source(&self.config.storage_root, &request)?;
        let journal_project_id = request.project_id.clone();
        let journal_environment = request.environment.clone();
        let journal_intent = request.intent.clone();
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
        let _ = OperationalJournalStore::new(&self.config.storage_root).append(
            &OperationalJournalEntry {
                schema_version: 1,
                timestamp_unix: current_unix_timestamp(),
                event_type: "deployment".into(),
                project_id: Some(journal_project_id),
                environment: Some(journal_environment),
                generation: None,
                payload: serde_json::json!({
                    "deployment_id": deployment_id,
                    "queue_position": queue_position,
                    "intent": journal_intent,
                }),
            },
        );

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
        let mut diagnostics = load_environment_diagnostics(
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
        })?;
        diagnostics.cluster = self.cluster_diagnostics.clone();
        Ok(diagnostics)
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
        self.refresh_leadership(current_unix_timestamp());
        if !self.reconciliation_enabled(current_unix_timestamp()) {
            return Err(not_leader_error_response());
        }
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
        self.refresh_leadership(current_unix_timestamp());
        if !self.reconciliation_enabled(current_unix_timestamp()) {
            return Err(not_leader_error_response());
        }
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
        let store = LeaderLeaseStore::new(&self.config.storage_root);
        let _ = store.release_if_owner(self.node_id(), current_unix_timestamp());
        self.leadership = LeadershipState {
            lease: store.load().ok().flatten(),
            uncertain: false,
            ownership_lost: false,
            last_error: None,
        };
        self.state = DaemonState::Stopped;
        self.refresh_readyz_cache();
    }

    pub fn state(&self) -> &DaemonState {
        &self.state
    }

    pub fn startup_steps(&self) -> &[StartupStep] {
        &self.startup_steps
    }

    pub fn startup_phase(&self) -> StartupPhase {
        self.startup_phase
    }

    pub fn health_loops_started(&self) -> bool {
        self.health_loops_started
    }

    pub fn last_recovery_outcome(&self) -> Option<&RecoveryOutcome> {
        self.last_recovery_outcome.as_ref()
    }

    fn record_startup_step(&mut self, step: StartupStep) {
        if !self.startup_steps.contains(&step) {
            self.startup_steps.push(step);
        }
    }

    fn set_startup_phase(&mut self, next: StartupPhase) {
        if self.startup_phase == next {
            return;
        }
        if self.startup_phase.can_transition_to(next) {
            self.startup_phase = next;
        } else {
            self.startup_phase = StartupPhase::Degraded;
            self.convergence_start_blocked = true;
        }
    }

    pub fn readyz_status(&mut self) -> &'static str {
        match self.readyz_response().status.as_str() {
            "ready" => "ready",
            "degraded" => "degraded",
            _ => "not_ready",
        }
    }

    pub fn readyz_response(&mut self) -> ReadyzResponse {
        self.cached_readyz_response()
    }

    pub fn queue(&self) -> Option<&PersistentQueue> {
        self.queue.as_ref()
    }

    pub fn runtimes(&self) -> (&D, &R) {
        (&self.docker_runtime, &self.routing_runtime)
    }

    pub fn readyz_cache_snapshot(&self) -> DaemonReadyzCache {
        self.control_plane_snapshot.readyz.clone()
    }

    pub fn control_plane_snapshot(&self) -> ControlPlaneSnapshot {
        self.control_plane_snapshot.clone()
    }

    pub fn cached_readyz_response(&self) -> ReadyzResponse {
        let now = now_unix_ms();
        if now.saturating_sub(self.control_plane_snapshot.readyz.updated_at_unix_ms)
            > READYZ_CACHE_STALE_AFTER_MS
        {
            return annotate_readyz_response(ReadyzResponse {
                status: "degraded".into(),
                startup_phase: self.startup_phase.as_str().into(),
                active_failure: true,
                reason: Some("readiness cache stale".into()),
                reasons: Vec::new(),
            });
        }
        let mut response = self.control_plane_snapshot.readyz.response.clone();
        let cache_age_ms =
            now.saturating_sub(self.control_plane_snapshot.readyz.updated_at_unix_ms);
        annotate_readyz_reasons(&mut response.reasons, cache_age_ms);
        annotate_readyz_response(response)
    }

    fn restore_readyz_cache_from_checkpoints(&mut self) {
        let mut reasons = Vec::new();
        let now_unix = current_unix_timestamp();
        let mut last_success = None;
        for (project_id, environment, env) in self.environment_paths() {
            let Ok(Some(checkpoint)) = ConvergenceCheckpointStore::new(env).load() else {
                continue;
            };
            last_success = last_success.max(checkpoint.last_successful_convergence_unix);
            let age_ms = now_unix
                .saturating_sub(checkpoint.checkpointed_at_unix)
                .saturating_mul(1_000);
            if age_ms > READYZ_CACHE_STALE_AFTER_MS {
                reasons.push(ReadyzReason {
                    project_id,
                    environment,
                    generation: checkpoint.active_generation,
                    active: checkpoint.active_generation.is_some(),
                    unresolved: true,
                    source: "convergence_checkpoint".into(),
                    marker: "stale_checkpoint".into(),
                    message: "checkpoint stale until next refresh".into(),
                    last_checked_unix: Some(checkpoint.checkpointed_at_unix),
                    cache_age_ms: age_ms,
                    diagnostics: None,
                });
                continue;
            }
            reasons.extend(restored_readyz_reasons_from_checkpoint(
                &checkpoint,
                &project_id,
                &environment,
                age_ms,
            ));
        }
        self.convergence_last_success_unix = last_success;
        self.convergence_last_success_restored = last_success.is_some();
        self.control_plane_snapshot.readyz = DaemonReadyzCache {
            response: annotate_readyz_response(ReadyzResponse {
                status: if reasons.is_empty() {
                    "ready".into()
                } else {
                    "degraded".into()
                },
                startup_phase: self.startup_phase.as_str().into(),
                active_failure: !reasons.is_empty(),
                reason: reasons.first().map(|value| value.message.clone()),
                reasons,
            }),
            updated_at_unix_ms: now_unix_ms(),
        };
    }

    fn convergence_enabled_for_readyz(&self, now_unix: u64) -> bool {
        self.reconciliation_enabled(now_unix)
            && !self.convergence_start_blocked
            && !self.reconciliation.replay_in_progress
            && !self.reconciliation.replay_paused
            && !self.reconciliation.replay_incomplete
            && self.reconciliation.lease_fencing_failures == 0
    }

    fn latest_convergence_error_from_checkpoints(&self) -> Option<String> {
        self.environment_paths()
            .into_iter()
            .filter_map(|(_, _, env)| {
                ConvergenceCheckpointStore::new(env)
                    .load()
                    .ok()
                    .flatten()
                    .and_then(|checkpoint| {
                        checkpoint
                            .last_convergence_error
                            .map(|error| (checkpoint.checkpointed_at_unix, error))
                    })
            })
            .max_by_key(|(timestamp, _)| *timestamp)
            .map(|(_, error)| error)
    }

    fn convergence_stalled_reason(&self, now_unix: u64) -> ReadyzReason {
        let cache_age_ms = self
            .control_plane_snapshot
            .readyz
            .updated_at_unix_ms
            .checked_div(1)
            .map(|updated| now_unix_ms().saturating_sub(updated))
            .unwrap_or(0);
        ReadyzReason {
            diagnostics: Some(ReadyzReasonDiagnostics {
                last_success_unix: self.convergence_last_success_unix,
                stall_threshold_ms: Some(CONVERGENCE_STALLED_AFTER_MS),
                startup_phase: Some(self.startup_phase.as_str().into()),
                replay_in_progress: Some(self.reconciliation.replay_in_progress),
                leader: Some(self.reconciliation_enabled(now_unix)),
                convergence_enabled: Some(self.convergence_enabled_for_readyz(now_unix)),
                last_convergence_error: self.latest_convergence_error_from_checkpoints(),
            }),
            cache_age_ms,
            ..control_plane_reason("convergence_stalled", "convergence stalled".into())
        }
    }

    fn environment_paths(&self) -> Vec<(String, String, EnvironmentPaths)> {
        let projects_root = self.config.storage_root.join("projects");
        let Ok(projects) = fs::read_dir(projects_root) else {
            return Vec::new();
        };
        let mut environments = Vec::new();
        for project in projects.flatten() {
            let project_id = project.file_name().to_string_lossy().into_owned();
            let envs_dir = project.path().join("environments");
            let Ok(envs) = fs::read_dir(envs_dir) else {
                continue;
            };
            for env_entry in envs.flatten() {
                let environment = env_entry.file_name().to_string_lossy().into_owned();
                environments.push((
                    project_id.clone(),
                    environment.clone(),
                    EnvironmentPaths::new(&self.config.storage_root, &project_id, &environment),
                ));
            }
        }
        environments
    }

    fn node_info(&self) -> NodeInfo {
        NodeInfo {
            node_id: self.node_metadata.node_id.clone(),
            booted_at_unix: self.node_metadata.booted_at_unix,
            hostname: self.node_metadata.hostname.clone(),
            capabilities: self.node_metadata.capabilities.clone(),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_metadata.node_id
    }

    pub fn heartbeat_interval_ms(&self) -> u64 {
        self.config.heartbeat_interval_ms.max(100)
    }

    pub fn heartbeat_tick(&mut self) {
        if matches!(
            self.state(),
            DaemonState::WaitingForBootstrap(_) | DaemonState::ShuttingDown | DaemonState::Stopped
        ) {
            return;
        }
        let now_unix = current_unix_timestamp();
        self.refresh_leadership(now_unix);
        self.convergence_start_blocked = self.startup_convergence_blocked();
        self.refresh_runtime_startup_phase(now_unix);
        self.persist_local_cluster_node(now_unix);
    }

    fn reconciliation_enabled(&self, now_unix: u64) -> bool {
        !self.leadership.uncertain && self.leadership.is_leader(self.node_id(), now_unix)
    }

    fn startup_convergence_blocked(&self) -> bool {
        self.reconciliation.replay_in_progress
            || self.reconciliation.replay_paused
            || self.reconciliation.replay_incomplete
            || self.reconciliation.lease_fencing_failures > 0
    }

    fn local_role(&self, now_unix: u64) -> String {
        if self.leadership.uncertain {
            "uncertain".into()
        } else if self.reconciliation_enabled(now_unix) {
            "leader".into()
        } else if self
            .leadership
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at_unix > now_unix)
        {
            "follower".into()
        } else {
            "candidate".into()
        }
    }

    fn persist_local_cluster_node(&self, now_unix: u64) {
        let node = PersistedClusterNode {
            schema_version: 1,
            node_id: self.node_metadata.node_id.clone(),
            hostname: self.node_metadata.hostname.clone(),
            advertised_addr: self.config.api_bind.clone(),
            role: self.local_role(now_unix),
            last_seen_unix: now_unix,
            capabilities: self.node_metadata.capabilities.clone(),
            lease_epoch_seen: self.leadership.lease_epoch(),
            control_plane_version: env!("CARGO_PKG_VERSION").into(),
            reconciliation_enabled: self.reconciliation_enabled(now_unix),
            active_reconciler: self.reconciliation_enabled(now_unix),
        };
        let _ = ClusterTopologyStore::new(&self.config.storage_root).upsert_node(&node);
    }

    fn load_cluster_diagnostics(&self, now_unix: u64) -> (ClusterDiagnostics, ClusterSignalState) {
        let nodes = ClusterTopologyStore::new(&self.config.storage_root)
            .load()
            .map(|topology| topology.nodes)
            .unwrap_or_default();
        let current_epoch = self.leadership.lease_epoch();
        let recent_nodes = nodes
            .iter()
            .filter(|node| {
                now_unix.saturating_sub(node.last_seen_unix) <= HEARTBEAT_STALE_AFTER_SECONDS
            })
            .collect::<Vec<_>>();
        let observed_nodes = recent_nodes.len();
        let active_reconcilers = recent_nodes
            .iter()
            .filter(|node| node.active_reconciler)
            .count();
        let leader_claims = recent_nodes
            .iter()
            .filter(|node| {
                node.role == "leader"
                    && node.active_reconciler
                    && (current_epoch == 0 || node.lease_epoch_seen == current_epoch)
            })
            .count();
        let mut distinct_epochs = recent_nodes
            .iter()
            .filter_map(|node| (node.lease_epoch_seen > 0).then_some(node.lease_epoch_seen))
            .collect::<Vec<_>>();
        distinct_epochs.sort_unstable();
        distinct_epochs.dedup();
        let lease_epoch_divergence = distinct_epochs.len() > 1
            || recent_nodes.iter().any(|node| {
                current_epoch > 0
                    && node.lease_epoch_seen > 0
                    && node.lease_epoch_seen != current_epoch
            });
        let stale_reconciler = nodes.iter().any(|node| {
            node.active_reconciler
                && now_unix.saturating_sub(node.last_seen_unix) > HEARTBEAT_STALE_AFTER_SECONDS
        });
        let split_brain_suspected = leader_claims > 1 || stale_reconciler;
        let local_node = nodes.iter().find(|node| node.node_id == self.node_id());
        let heartbeat_age_ms = local_node
            .map(|node| {
                now_unix
                    .saturating_sub(node.last_seen_unix)
                    .saturating_mul(1_000)
            })
            .unwrap_or(0);
        let multiple_active_reconcilers = active_reconcilers > 1;
        let nodes = nodes
            .into_iter()
            .map(|node| ClusterNodeStatus {
                node_id: node.node_id,
                hostname: node.hostname,
                advertised_addr: node.advertised_addr,
                role: node.role,
                last_seen_unix: node.last_seen_unix,
                capabilities: node.capabilities,
                lease_epoch_seen: node.lease_epoch_seen,
                control_plane_version: node.control_plane_version,
                reconciliation_enabled: node.reconciliation_enabled,
                active_reconciler: node.active_reconciler,
            })
            .collect::<Vec<_>>();
        let mut degraded_markers = Vec::new();
        if split_brain_suspected {
            degraded_markers.push("split_brain_suspected".into());
        }
        if multiple_active_reconcilers {
            degraded_markers.push("multiple_active_reconcilers".into());
        }
        if lease_epoch_divergence {
            degraded_markers.push("lease_epoch_divergence".into());
        }
        (
            ClusterDiagnostics {
                observed_nodes,
                active_reconcilers,
                lease_epoch_divergence,
                split_brain_suspected,
                cluster_size: nodes.len(),
                local_role: self.local_role(now_unix),
                heartbeat_age_ms,
                multiple_active_reconcilers,
                checkpoint_owner_mismatch: false,
                snapshot_owner_mismatch: false,
                stale_reconciler,
                reconciliation_blocked: !self.reconciliation_enabled(now_unix),
                degraded_markers,
                nodes,
            },
            ClusterSignalState {
                split_brain_suspected,
                multiple_active_reconcilers,
                checkpoint_owner_mismatch: false,
                snapshot_owner_mismatch: false,
                lease_epoch_divergence,
                stale_reconciler,
            },
        )
    }

    fn refresh_leadership(&mut self, now_unix: u64) {
        let store = LeaderLeaseStore::new(&self.config.storage_root);
        let was_leader = self.leadership.is_leader(self.node_id(), now_unix);
        match store.try_acquire_or_renew(self.node_id(), now_unix, LEADER_LEASE_TTL_SECONDS) {
            Ok(LeaseAcquireOutcome::Leader(lease)) => {
                self.leadership = LeadershipState {
                    lease: Some(lease),
                    uncertain: false,
                    ownership_lost: false,
                    last_error: None,
                };
            }
            Ok(LeaseAcquireOutcome::Follower(lease)) => {
                self.leadership = LeadershipState {
                    lease: Some(lease),
                    uncertain: false,
                    ownership_lost: was_leader,
                    last_error: None,
                };
            }
            Err(err) => {
                self.leadership.uncertain = true;
                self.leadership.ownership_lost = was_leader;
                self.leadership.last_error = Some(err.to_string());
            }
        }
        let is_leader = self.reconciliation_enabled(now_unix);
        if !was_leader && is_leader {
            let _ = OperationalJournalStore::new(&self.config.storage_root).append(
                &OperationalJournalEntry {
                    schema_version: 1,
                    timestamp_unix: now_unix,
                    event_type: "leader_acquired".into(),
                    project_id: None,
                    environment: None,
                    generation: None,
                    payload: serde_json::json!({
                        "node_id": self.node_id(),
                        "lease_epoch": self.leadership.lease_epoch(),
                    }),
                },
            );
        }
        if was_leader && !is_leader {
            let _ = OperationalJournalStore::new(&self.config.storage_root).append(
                &OperationalJournalEntry {
                    schema_version: 1,
                    timestamp_unix: now_unix,
                    event_type: "leader_lost".into(),
                    project_id: None,
                    environment: None,
                    generation: None,
                    payload: serde_json::json!({
                        "node_id": self.node_id(),
                        "lease_epoch": self.leadership.lease_epoch(),
                    }),
                },
            );
        }
        if !is_leader && self.local_role(now_unix) == "follower" {
            let _ = OperationalJournalStore::new(&self.config.storage_root).append(
                &OperationalJournalEntry {
                    schema_version: 1,
                    timestamp_unix: now_unix,
                    event_type: "follower_transition".into(),
                    project_id: None,
                    environment: None,
                    generation: None,
                    payload: serde_json::json!({
                        "node_id": self.node_id(),
                        "leader_node_id": self.leadership.convergence_owner(),
                        "lease_epoch": self.leadership.lease_epoch(),
                    }),
                },
            );
        }
    }

    fn refresh_runtime_startup_phase(&mut self, now_unix: u64) {
        if self.state != DaemonState::Ready {
            return;
        }
        if self.reconciliation_enabled(now_unix) && !self.convergence_start_blocked {
            self.set_startup_phase(StartupPhase::LeaderActive);
        } else if self.local_role(now_unix) == "follower" {
            self.set_startup_phase(StartupPhase::Follower);
        } else if self.convergence_start_blocked {
            self.set_startup_phase(StartupPhase::Degraded);
        } else {
            self.set_startup_phase(StartupPhase::LeaderAcquiring);
        }
    }

    fn persist_environment_checkpoint(
        &self,
        env: EnvironmentPaths,
        project_id: &str,
        environment: &str,
        queue_depth: usize,
        reasons: &[ReadyzReason],
    ) {
        let now_unix = current_unix_timestamp();
        if !self.reconciliation_enabled(now_unix) {
            return;
        }
        let runtime_state = RuntimeStateStore::new(env.clone())
            .load()
            .unwrap_or_default();
        let checkpoint = PersistedEnvironmentCheckpoint {
            snapshot_version: 1,
            schema_version: 1,
            project_id: project_id.into(),
            environment: environment.into(),
            checkpointed_at_unix: now_unix,
            last_successful_convergence_unix: self.convergence_last_success_unix,
            last_convergence_duration_ms: self.convergence_loop_duration_ms,
            last_convergence_generation: runtime_state.active_generation,
            last_convergence_error: runtime_state.last_error_code.clone(),
            active_generation: runtime_state.active_generation,
            health_state: runtime_state.health_state,
            dependency_states: BTreeMap::from([
                (
                    "docker".into(),
                    PersistedDependencyState {
                        reachable: self.docker_readiness.last_known_reachable.unwrap_or(false),
                        last_error: self.docker_readiness.last_error.clone(),
                        last_latency_ms: self.docker_breaker.last_latency_ms,
                    },
                ),
                (
                    "caddy".into(),
                    PersistedDependencyState {
                        reachable: self.caddy_readiness.last_known_reachable.unwrap_or(false),
                        last_error: self.caddy_readiness.last_error.clone(),
                        last_latency_ms: self.caddy_breaker.last_latency_ms,
                    },
                ),
            ]),
            breaker_states: BTreeMap::from([
                ("docker".into(), breaker_state(&self.docker_breaker)),
                ("caddy".into(), breaker_state(&self.caddy_breaker)),
            ]),
            queue_depth_snapshot: queue_depth,
            node_id: self.node_id().to_string(),
            lease_epoch: self.leadership.lease_epoch(),
            convergence_owner: self.leadership.convergence_owner(),
            readyz_reasons: reasons
                .iter()
                .filter(|reason| {
                    reason.project_id == project_id && reason.environment == environment
                })
                .map(|reason| reason.message.clone())
                .collect(),
            extra: BTreeMap::from([
                (
                    READYZ_REASON_DETAILS_KEY.into(),
                    serde_json::to_value(
                        reasons
                            .iter()
                            .filter(|reason| {
                                reason.project_id == project_id && reason.environment == environment
                            })
                            .map(|reason| PersistedReadyzReasonDetail {
                                marker: reason.marker.clone(),
                                message: reason.message.clone(),
                            })
                            .collect::<Vec<_>>(),
                    )
                    .unwrap_or(Value::Array(Vec::new())),
                ),
                (
                    "convergence_domains".into(),
                    serde_json::to_value(&self.convergence_domains)
                        .unwrap_or(Value::Array(Vec::new())),
                ),
                (
                    "startup_phase".into(),
                    Value::String(self.startup_phase.as_str().into()),
                ),
                (
                    "readyz_status".into(),
                    Value::String(if reasons.is_empty() {
                        "ready".into()
                    } else {
                        "degraded".into()
                    }),
                ),
            ]),
        };
        let _ = ConvergenceCheckpointStore::new(env.clone()).save(&checkpoint);
        self.persist_runtime_truth_snapshots(env, &checkpoint);
    }

    fn persist_runtime_truth_snapshots(
        &self,
        env: EnvironmentPaths,
        checkpoint: &PersistedEnvironmentCheckpoint,
    ) {
        let cycle_id = format!(
            "{}-{}",
            checkpoint.checkpointed_at_unix,
            checkpoint
                .active_generation
                .unwrap_or(checkpoint.last_convergence_generation.unwrap_or(0))
        );
        let store = ControlPlaneSnapshotStore::new(env.clone());
        let generation = checkpoint.active_generation;
        let runtime_snapshot = PersistedControlPlaneSnapshot {
            snapshot_version: 1,
            schema_version: 1,
            snapshot_kind: "runtime_snapshot".into(),
            project_id: checkpoint.project_id.clone(),
            environment: checkpoint.environment.clone(),
            cycle_id: cycle_id.clone(),
            created_at_unix: checkpoint.checkpointed_at_unix,
            generation,
            node_id: checkpoint.node_id.clone(),
            lease_epoch: checkpoint.lease_epoch,
            convergence_owner: checkpoint.convergence_owner.clone(),
            payload: serde_json::json!({
                "checkpoint": checkpoint,
                "node": self.node_info(),
                "domains": self.convergence_domains,
            }),
        };
        let route_snapshot = PersistedControlPlaneSnapshot {
            snapshot_version: 1,
            schema_version: 1,
            snapshot_kind: "route_snapshot".into(),
            project_id: checkpoint.project_id.clone(),
            environment: checkpoint.environment.clone(),
            cycle_id: cycle_id.clone(),
            created_at_unix: checkpoint.checkpointed_at_unix,
            generation,
            node_id: checkpoint.node_id.clone(),
            lease_epoch: checkpoint.lease_epoch,
            convergence_owner: checkpoint.convergence_owner.clone(),
            payload: serde_json::json!({
                "active_generation": checkpoint.active_generation,
                "health_state": checkpoint.health_state,
            }),
        };
        let dependency_snapshot = PersistedControlPlaneSnapshot {
            snapshot_version: 1,
            schema_version: 1,
            snapshot_kind: "dependency_snapshot".into(),
            project_id: checkpoint.project_id.clone(),
            environment: checkpoint.environment.clone(),
            cycle_id,
            created_at_unix: checkpoint.checkpointed_at_unix,
            generation,
            node_id: checkpoint.node_id.clone(),
            lease_epoch: checkpoint.lease_epoch,
            convergence_owner: checkpoint.convergence_owner.clone(),
            payload: serde_json::json!({
                "dependencies": checkpoint.dependency_states,
                "breakers": checkpoint.breaker_states,
            }),
        };
        let _ = store.append(&runtime_snapshot, CONTROL_PLANE_SNAPSHOT_RETENTION_LIMIT);
        let _ = store.append(&route_snapshot, CONTROL_PLANE_SNAPSHOT_RETENTION_LIMIT);
        let _ = store.append(&dependency_snapshot, CONTROL_PLANE_SNAPSHOT_RETENTION_LIMIT);
    }

    pub fn refresh_readyz_cache(&mut self) {
        let started = Instant::now();
        let now_unix = current_unix_timestamp();
        let updated_at_unix_ms = now_unix_ms();
        if !matches!(
            self.state(),
            DaemonState::ShuttingDown | DaemonState::Stopped
        ) {
            self.refresh_leadership(now_unix);
        }
        let previous_cluster_signals = self.cluster_signals.clone();
        let (mut cluster_diagnostics, mut cluster_signals) =
            self.load_cluster_diagnostics(now_unix);
        self.reconciliation = ReconciliationStore::new(&self.config.storage_root).diagnostics();
        self.convergence_start_blocked = self.startup_convergence_blocked();
        let queue_depth = self.queue_depth().unwrap_or_default();
        self.convergence_domains.clear();
        let dependency_started = Instant::now();
        let mut reasons = self.compute_readyz_reasons(now_unix, &cluster_diagnostics);
        self.convergence_domains.push(ConvergenceDomainSummary {
            domain: "dependency_probing".into(),
            status: if reasons
                .iter()
                .any(|reason| reason.project_id == "_control_plane")
            {
                "degraded".into()
            } else {
                "healthy".into()
            },
            duration_ms: dependency_started.elapsed().as_millis() as u64,
            detail: None,
        });
        let runtime_started = Instant::now();
        reasons.extend(self.cached_environment_readyz_reasons_with_budget(
            now_unix,
            &mut cluster_signals,
            &mut cluster_diagnostics,
        ));
        self.convergence_domains.push(ConvergenceDomainSummary {
            domain: "runtime_container_reconciliation".into(),
            status: if reasons
                .iter()
                .any(|reason| reason.source == "runtime_state_cache")
            {
                "degraded".into()
            } else {
                "healthy".into()
            },
            duration_ms: runtime_started.elapsed().as_millis() as u64,
            detail: None,
        });
        self.convergence_domains.push(ConvergenceDomainSummary {
            domain: "routing_reconciliation".into(),
            status: if reasons
                .iter()
                .any(|reason| reason.marker.contains("route") || reason.marker.contains("caddy"))
            {
                "degraded".into()
            } else {
                "healthy".into()
            },
            duration_ms: self.caddy_breaker.last_latency_ms,
            detail: None,
        });
        self.convergence_domains.push(ConvergenceDomainSummary {
            domain: "retention_reconciliation".into(),
            status: "healthy".into(),
            duration_ms: 0,
            detail: Some("bounded no-op in single-node mode".into()),
        });
        self.convergence_domains.push(ConvergenceDomainSummary {
            domain: "backup_reconciliation".into(),
            status: "healthy".into(),
            duration_ms: 0,
            detail: Some("bounded no-op in single-node mode".into()),
        });
        self.convergence_domains.push(ConvergenceDomainSummary {
            domain: "metrics_refresh".into(),
            status: "healthy".into(),
            duration_ms: 0,
            detail: None,
        });
        reasons.sort_by(|left, right| {
            (
                left.project_id.as_str(),
                left.environment.as_str(),
                left.generation.unwrap_or(0),
                left.source.as_str(),
                left.marker.as_str(),
                left.message.as_str(),
            )
                .cmp(&(
                    right.project_id.as_str(),
                    right.environment.as_str(),
                    right.generation.unwrap_or(0),
                    right.source.as_str(),
                    right.marker.as_str(),
                    right.message.as_str(),
                ))
        });
        reasons.dedup();
        let healthy_convergence_tick =
            self.convergence_enabled_for_readyz(now_unix) && reasons.is_empty();
        if healthy_convergence_tick {
            self.convergence_last_success_unix = Some(now_unix);
            self.convergence_last_success_restored = false;
        }
        let stalled = self.convergence_enabled_for_readyz(now_unix)
            && self.convergence_last_success_unix.is_some_and(|value| {
                now_unix.saturating_sub(value) * 1_000 > CONVERGENCE_STALLED_AFTER_MS
            });
        if stalled {
            reasons.push(self.convergence_stalled_reason(now_unix));
        }
        self.record_cluster_events(now_unix, &previous_cluster_signals, &cluster_signals);
        self.cluster_signals = cluster_signals;
        cluster_diagnostics.reconciliation_blocked =
            !self.reconciliation_enabled(now_unix) || cluster_diagnostics.split_brain_suspected;
        self.cluster_diagnostics = cluster_diagnostics.clone();
        annotate_readyz_reasons(&mut reasons, 0);
        let degraded = !reasons.is_empty();
        if !degraded {
            self.refresh_runtime_startup_phase(now_unix);
        }
        let loop_duration_ms = started.elapsed().as_millis() as u64;
        self.convergence_loop_duration_ms = loop_duration_ms;
        if degraded {
            self.convergence_last_failure_unix = Some(now_unix);
            self.convergence_failures_total = self.convergence_failures_total.saturating_add(1);
        } else if self.convergence_enabled_for_readyz(now_unix) {
            self.convergence_last_success_unix = Some(now_unix);
            self.convergence_last_success_restored = false;
        }
        let reason = reasons
            .iter()
            .find(|reason| reason.marker == "convergence_stalled")
            .map(|_| "convergence stalled".to_string())
            .or_else(|| {
                reasons.first().and_then(|value| {
                    (value.project_id == "_control_plane").then(|| value.message.clone())
                })
            });
        let readyz = DaemonReadyzCache {
            response: annotate_readyz_response(ReadyzResponse {
                status: if self.state() != &DaemonState::Ready {
                    "not_ready".into()
                } else if degraded {
                    "degraded".into()
                } else {
                    "ready".into()
                },
                startup_phase: self.startup_phase.as_str().into(),
                active_failure: self.state() != &DaemonState::Ready || degraded,
                reason,
                reasons: reasons.clone(),
            }),
            updated_at_unix_ms,
        };
        self.readyz_cache = readyz.clone();
        let metrics = self.build_metrics_snapshot(queue_depth, &readyz);
        let persisted_timeline = if self.control_plane_snapshot.timeline.entries.is_empty() {
            load_persisted_timeline(&self.config.storage_root)
                .ok()
                .flatten()
        } else {
            None
        };
        let previous_timeline = if self.control_plane_snapshot.timeline.entries.is_empty() {
            persisted_timeline.as_ref()
        } else {
            Some(&self.control_plane_snapshot.timeline)
        };
        let timeline = build_timeline(
            &readyz.response,
            &metrics,
            previous_timeline,
            now_unix,
            "daemon_api",
            true,
            None,
        );
        self.control_plane_snapshot = ControlPlaneSnapshot {
            readyz: readyz.clone(),
            metrics,
            timeline: timeline.clone(),
        };
        let _ = persist_timeline(&self.config.storage_root, &timeline);
        for (project_id, environment, env) in self.environment_paths() {
            self.persist_environment_checkpoint(
                env,
                &project_id,
                &environment,
                queue_depth,
                &reasons,
            );
        }
    }

    fn compute_readyz_reasons(
        &mut self,
        now_unix: u64,
        cluster: &ClusterDiagnostics,
    ) -> Vec<ReadyzReason> {
        if self.state() != &DaemonState::Ready {
            return Vec::new();
        }

        let mut reasons = Vec::new();
        if self.leadership.uncertain {
            reasons.push(control_plane_reason(
                "leadership_uncertain",
                self.leadership
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "leadership uncertain".into()),
            ));
            return reasons;
        }
        if self.leadership.ownership_lost {
            reasons.push(control_plane_reason(
                "convergence_ownership_lost",
                "convergence ownership lost".into(),
            ));
        }
        if self.convergence_start_blocked {
            reasons.push(control_plane_reason(
                "convergence_disabled",
                "convergence disabled pending stable startup".into(),
            ));
        }
        if self.reconciliation.replay_incomplete {
            reasons.push(control_plane_reason(
                "replay_incomplete",
                "reconciliation replay incomplete".into(),
            ));
        }
        if self.reconciliation.replay_paused {
            reasons.push(control_plane_reason(
                "replay_paused",
                "replay paused after bounded startup budget".into(),
            ));
        }
        if self.reconciliation.unrecoverable_pending_intents {
            reasons.push(control_plane_reason(
                "unrecoverable_pending_intents",
                "unrecoverable pending intents require operator intervention".into(),
            ));
        }
        if self.reconciliation.lease_fencing_failures > 0 {
            reasons.push(control_plane_reason(
                "lease_fencing_failed",
                "lease fencing failed during replay".into(),
            ));
        }
        if self.reconciliation.destructive_replay_blocked {
            reasons.push(control_plane_reason(
                "destructive_replay_blocked",
                "destructive replay blocked".into(),
            ));
        }
        if self.reconciliation.replay_cursor_corrupted {
            reasons.push(control_plane_reason(
                "replay_cursor_corrupted",
                "replay cursor corrupted".into(),
            ));
        }
        if self.reconciliation.reconciliation_log_corrupted {
            reasons.push(control_plane_reason(
                "reconciliation_log_corrupted",
                "reconciliation log corrupted".into(),
            ));
        }
        if let Some(lease) = self.leadership.lease.as_ref() {
            if lease.expires_at_unix <= now_unix {
                reasons.push(control_plane_reason(
                    "lease_stale",
                    "leader lease stale".into(),
                ));
            }
        }
        if cluster.split_brain_suspected {
            reasons.push(control_plane_reason(
                "split_brain_suspected",
                "split-brain suspected".into(),
            ));
        }
        if cluster.multiple_active_reconcilers {
            reasons.push(control_plane_reason(
                "multiple_active_reconcilers",
                "multiple active reconcilers observed".into(),
            ));
        }
        if cluster.lease_epoch_divergence {
            reasons.push(control_plane_reason(
                "lease_epoch_divergence",
                "lease epoch divergence observed".into(),
            ));
        }
        if cluster.stale_reconciler {
            reasons.push(control_plane_reason(
                "stale_reconciler",
                "stale leader with active reconciler observed".into(),
            ));
        }
        if !self.reconciliation_enabled(now_unix) {
            let marker = match self.local_role(now_unix).as_str() {
                "follower" => "follower_mode",
                _ => "convergence_disabled",
            };
            let message = match marker {
                "follower_mode" => "convergence disabled while following active leader".into(),
                _ => "convergence disabled until leadership is established".into(),
            };
            reasons.push(control_plane_reason(marker, message));
            return reasons;
        }
        if fs::metadata(&self.config.storage_root).is_err() {
            reasons.push(control_plane_reason(
                "storage_unavailable",
                format!(
                    "storage root inaccessible: {}",
                    self.config.storage_root.display()
                ),
            ));
        }

        let queue_alive = self
            .queue
            .as_ref()
            .and_then(|queue| queue.queued_len().ok())
            .is_some();
        if !queue_alive {
            reasons.push(control_plane_reason(
                "queue_unavailable",
                "deployment queue unavailable".into(),
            ));
        }

        if let Some(reason) = self.probe_docker_dependency(now_unix) {
            reasons.push(reason);
        }
        if let Some(reason) = self.probe_caddy_dependency(now_unix) {
            reasons.push(reason);
        }

        reasons
    }

    fn cached_environment_readyz_reasons_with_budget(
        &mut self,
        now_unix: u64,
        cluster_signals: &mut ClusterSignalState,
        cluster: &mut ClusterDiagnostics,
    ) -> Vec<ReadyzReason> {
        let started = Instant::now();
        let projects_root = self.config.storage_root.join("projects");
        let Ok(projects) = fs::read_dir(projects_root) else {
            return Vec::new();
        };
        let expected_owner = self.leadership.convergence_owner();
        let expected_epoch = self.leadership.lease_epoch();
        let mut reasons = Vec::new();
        for project in projects.flatten() {
            if started.elapsed() >= Duration::from_millis(FILESYSTEM_SCAN_BUDGET_MS) {
                reasons.push(control_plane_reason(
                    "filesystem_scan_timeout",
                    "filesystem scan budget exceeded".into(),
                ));
                break;
            }
            let envs_dir = project.path().join("environments");
            let Ok(envs) = fs::read_dir(envs_dir) else {
                continue;
            };
            for env_entry in envs.flatten() {
                if started.elapsed() >= Duration::from_millis(FILESYSTEM_SCAN_BUDGET_MS) {
                    reasons.push(control_plane_reason(
                        "filesystem_scan_timeout",
                        "filesystem scan budget exceeded".into(),
                    ));
                    return reasons;
                }
                let environment = env_entry.file_name().to_string_lossy().into_owned();
                let project_id = project.file_name().to_string_lossy().into_owned();
                let env =
                    EnvironmentPaths::new(&self.config.storage_root, &project_id, &environment);
                if let Ok(Some(checkpoint)) = ConvergenceCheckpointStore::new(env.clone()).load() {
                    let cache_age_ms = now_unix
                        .saturating_sub(checkpoint.checkpointed_at_unix)
                        .saturating_mul(1_000);
                    if (expected_epoch > 0 && checkpoint.lease_epoch != expected_epoch)
                        || (!expected_owner.is_empty()
                            && checkpoint.convergence_owner != expected_owner)
                    {
                        reasons.push(ReadyzReason {
                            project_id: project_id.clone(),
                            environment: environment.clone(),
                            generation: checkpoint.active_generation,
                            active: checkpoint.active_generation.is_some(),
                            unresolved: true,
                            source: "convergence_checkpoint".into(),
                            marker: "checkpoint_epoch_mismatch".into(),
                            message: "checkpoint epoch mismatch".into(),
                            last_checked_unix: Some(checkpoint.checkpointed_at_unix),
                            cache_age_ms,
                            diagnostics: None,
                        });
                    }
                    if !checkpoint.convergence_owner.is_empty()
                        && checkpoint.node_id != checkpoint.convergence_owner
                    {
                        cluster_signals.checkpoint_owner_mismatch = true;
                        cluster.checkpoint_owner_mismatch = true;
                        if !cluster
                            .degraded_markers
                            .iter()
                            .any(|marker| marker == "checkpoint_owner_mismatch")
                        {
                            cluster
                                .degraded_markers
                                .push("checkpoint_owner_mismatch".into());
                        }
                        reasons.push(ReadyzReason {
                            project_id: project_id.clone(),
                            environment: environment.clone(),
                            generation: checkpoint.active_generation,
                            active: checkpoint.active_generation.is_some(),
                            unresolved: true,
                            source: "convergence_checkpoint".into(),
                            marker: "checkpoint_owner_mismatch".into(),
                            message: "checkpoint owner mismatch".into(),
                            last_checked_unix: Some(checkpoint.checkpointed_at_unix),
                            cache_age_ms,
                            diagnostics: None,
                        });
                    }
                }
                if let Ok(Some(snapshot)) =
                    ControlPlaneSnapshotStore::new(env.clone()).latest_by_kind("runtime_snapshot")
                {
                    let snapshot_owner = snapshot.convergence_owner.clone();
                    if !snapshot_owner.is_empty() && snapshot.node_id != snapshot_owner {
                        cluster_signals.snapshot_owner_mismatch = true;
                        cluster.snapshot_owner_mismatch = true;
                        if !cluster
                            .degraded_markers
                            .iter()
                            .any(|marker| marker == "snapshot_owner_mismatch")
                        {
                            cluster
                                .degraded_markers
                                .push("snapshot_owner_mismatch".into());
                        }
                        reasons.push(ReadyzReason {
                            project_id: project_id.clone(),
                            environment: environment.clone(),
                            generation: snapshot.generation,
                            active: snapshot.generation.is_some(),
                            unresolved: true,
                            source: "control_plane_snapshot".into(),
                            marker: "snapshot_owner_mismatch".into(),
                            message: "snapshot owner mismatch".into(),
                            last_checked_unix: Some(snapshot.created_at_unix),
                            cache_age_ms: now_unix
                                .saturating_sub(snapshot.created_at_unix)
                                .saturating_mul(1_000),
                            diagnostics: None,
                        });
                    }
                }
                let Ok(runtime_state) = RuntimeStateStore::new(env.clone()).load() else {
                    continue;
                };
                if !self.reconciliation_enabled(now_unix) {
                    continue;
                }
                let Some(generation) = runtime_state.active_generation else {
                    continue;
                };
                if matches!(
                    runtime_state.last_error_code.as_deref(),
                    Some(
                        "route_activation_verification_failed"
                            | "route_fallback_served"
                            | "application_route_not_active"
                            | "gateway_fallback_response"
                    )
                ) {
                    if let Some(reason) = self.resolve_active_route_failure_readyz_reason(
                        &env,
                        &project_id,
                        &environment,
                        &runtime_state,
                        generation,
                        now_unix,
                    ) {
                        reasons.push(reason);
                    }
                    continue;
                }
                if runtime_state.health_state == RuntimeHealthState::Healthy {
                    continue;
                }
                reasons.extend(readyz_reasons_for_runtime_state(
                    &env,
                    &project_id,
                    &environment,
                    &runtime_state,
                    now_unix,
                ));
            }
        }
        reasons
    }

    fn resolve_active_route_failure_readyz_reason(
        &mut self,
        env: &EnvironmentPaths,
        project_id: &str,
        environment: &str,
        runtime_state: &crate::storage::RuntimeState,
        generation: u64,
        now_unix: u64,
    ) -> Option<ReadyzReason> {
        let summary = DiagnosticsStore::new(env.clone(), generation)
            .read_summary()
            .ok()
            .flatten();
        let marker = runtime_state
            .last_error_code
            .clone()
            .unwrap_or_else(|| "route_activation_verification_failed".into());
        let route_check = self.active_route_failure_state(env, project_id, environment, generation);
        match route_check {
            RouteFailureState::Resolved => {
                let _ = clear_resolved_route_failure_marker(env, generation);
                None
            }
            RouteFailureState::Unresolved(message) => Some(ReadyzReason {
                project_id: project_id.into(),
                environment: environment.into(),
                generation: Some(generation),
                active: true,
                unresolved: true,
                source: "runtime_state_cache".into(),
                marker: marker.clone(),
                message,
                last_checked_unix: Some(now_unix),
                cache_age_ms: 0,
                diagnostics: None,
            }),
            RouteFailureState::Unknown => {
                let historical_startup_failure = summary
                    .as_ref()
                    .is_some_and(|summary| summary.failure_stage == "startup_recovery");
                if runtime_state.health_state == RuntimeHealthState::Healthy {
                    let _ = clear_resolved_route_failure_marker(env, generation);
                    return None;
                }
                if historical_startup_failure {
                    let current_startup_failure = runtime_state
                        .degraded_since_unix
                        .is_some_and(|value| value >= self.startup_started_unix.saturating_sub(1));
                    if current_startup_failure {
                        let message = summary
                            .as_ref()
                            .and_then(|summary| summary.blocking_reason.clone())
                            .unwrap_or_else(|| "route activation verification failed".into());
                        return Some(ReadyzReason {
                            project_id: project_id.into(),
                            environment: environment.into(),
                            generation: Some(generation),
                            active: true,
                            unresolved: true,
                            source: "runtime_state_cache".into(),
                            marker: marker.clone(),
                            message,
                            last_checked_unix: Some(now_unix),
                            cache_age_ms: 0,
                            diagnostics: None,
                        });
                    }
                    // A startup recovery failure is historical unless we can still confirm an
                    // active route mismatch. Keep the marker on disk for diagnostics, but do not
                    // let an unverified stale error pin `/readyz` degraded forever.
                    return None;
                }
                let message = summary
                    .as_ref()
                    .and_then(|summary| summary.blocking_reason.clone())
                    .unwrap_or_else(|| "route activation verification failed".into());
                Some(ReadyzReason {
                    project_id: project_id.into(),
                    environment: environment.into(),
                    generation: Some(generation),
                    active: true,
                    unresolved: true,
                    source: "runtime_state_cache".into(),
                    marker,
                    message,
                    last_checked_unix: Some(now_unix),
                    cache_age_ms: 0,
                    diagnostics: None,
                })
            }
        }
    }

    fn active_route_failure_state(
        &mut self,
        env: &EnvironmentPaths,
        project_id: &str,
        environment: &str,
        generation: u64,
    ) -> RouteFailureState {
        let Some(runtime) = load_generation_runtime_info(env, generation).ok().flatten() else {
            return RouteFailureState::Unknown;
        };
        let Some(domain) = ProjectRegistryStore::new(&self.config.storage_root)
            .get(project_id)
            .ok()
            .flatten()
            .map(|project| derive_environment_domain(&project.base_domain, environment))
        else {
            return RouteFailureState::Unknown;
        };

        let checks = collect_expected_route_checks(project_id, environment, &domain, &runtime);
        if checks.is_empty() {
            return RouteFailureState::Resolved;
        }

        for check in checks {
            let Ok(container) = self.docker_runtime.inspect_container(&check.container_name) else {
                return RouteFailureState::Unknown;
            };
            let Some(expected_route) = expected_route_for_runtime(
                project_id,
                environment,
                Some(domain.clone()),
                &check.runtime,
                &container,
                check.network_name.as_deref(),
            ) else {
                return RouteFailureState::Unknown;
            };
            let inspection = match self.routing_runtime.inspect_route(&check.subtree_id) {
                Ok(inspection) => inspection,
                Err(_) => return RouteFailureState::Unknown,
            };
            if !inspection.activation_verified {
                return RouteFailureState::Unresolved(format!(
                    "route activation not verified for {}",
                    check.subtree_id
                ));
            }
            if inspection.health_checks_enabled {
                return RouteFailureState::Unresolved(format!(
                    "route health checks still enabled for {}",
                    check.subtree_id
                ));
            }
            if inspection.active_target != expected_route.target {
                return RouteFailureState::Unresolved(format!(
                    "route target mismatch: current={} expected={}",
                    inspection.active_target, expected_route.target
                ));
            }
            if inspection.domain.as_deref() != Some(domain.as_str()) {
                return RouteFailureState::Unresolved(format!(
                    "route domain mismatch: current={} expected={}",
                    inspection.domain.as_deref().unwrap_or("unknown"),
                    domain
                ));
            }
        }

        RouteFailureState::Resolved
    }
}

impl<D, R, A> Daemon<D, R, A>
where
    D: DockerRuntime,
    R: RoutingRuntime,
    A: ActiveDeploymentDecider,
{
    fn probe_docker_dependency(&mut self, now_unix: u64) -> Option<ReadyzReason> {
        let previous_state = self.docker_breaker.state.clone();
        if !self.docker_breaker.allow_request(now_unix) {
            self.docker_readiness.last_error = Some("docker circuit breaker open".into());
            self.docker_readiness.last_known_reachable = Some(false);
            return Some(control_plane_reason(
                "docker_unreachable",
                "docker circuit breaker open".into(),
            ));
        }
        let started = Instant::now();
        match self.docker_runtime.probe_control_plane() {
            Ok(_) => {
                let latency_ms = started.elapsed().as_millis() as u64;
                self.docker_breaker.record_success(now_unix, latency_ms);
                self.record_breaker_transition(
                    "docker",
                    &previous_state,
                    &self.docker_breaker.state,
                );
                self.docker_readiness.last_known_reachable = Some(true);
                self.docker_readiness.last_error = None;
                None
            }
            Err(err) => {
                let latency_ms = started.elapsed().as_millis() as u64;
                let message = err.to_string();
                self.docker_breaker
                    .record_failure(now_unix, latency_ms, message.clone());
                self.record_breaker_transition(
                    "docker",
                    &previous_state,
                    &self.docker_breaker.state,
                );
                self.docker_readiness.last_known_reachable = Some(false);
                self.docker_readiness.last_error = Some(message.clone());
                Some(control_plane_reason(
                    "docker_unreachable",
                    if self.docker_breaker.state == CircuitBreakerState::Open {
                        "docker daemon unavailable; breaker open".into()
                    } else {
                        message
                    },
                ))
            }
        }
    }

    fn probe_caddy_dependency(&mut self, now_unix: u64) -> Option<ReadyzReason> {
        let previous_state = self.caddy_breaker.state.clone();
        if !self.caddy_breaker.allow_request(now_unix) {
            self.caddy_readiness.last_error = Some("caddy circuit breaker open".into());
            self.caddy_readiness.last_known_reachable = Some(false);
            return Some(control_plane_reason(
                "caddy_admin_unreachable",
                "caddy circuit breaker open".into(),
            ));
        }
        let started = Instant::now();
        match self.routing_runtime.probe_control_plane() {
            Ok(_) => {
                let latency_ms = started.elapsed().as_millis() as u64;
                self.caddy_breaker.record_success(now_unix, latency_ms);
                self.record_breaker_transition("caddy", &previous_state, &self.caddy_breaker.state);
                self.caddy_readiness.last_known_reachable = Some(true);
                self.caddy_readiness.last_error = None;
                None
            }
            Err(err) => {
                let latency_ms = started.elapsed().as_millis() as u64;
                let message = err.to_string();
                self.caddy_breaker
                    .record_failure(now_unix, latency_ms, message.clone());
                self.record_breaker_transition("caddy", &previous_state, &self.caddy_breaker.state);
                self.caddy_readiness.last_known_reachable = Some(false);
                self.caddy_readiness.last_error = Some(message.clone());
                Some(control_plane_reason(
                    "caddy_admin_unreachable",
                    if self.caddy_breaker.state == CircuitBreakerState::Open {
                        "caddy admin API unavailable; breaker open".into()
                    } else {
                        message
                    },
                ))
            }
        }
    }

    fn record_breaker_transition(
        &self,
        dependency: &str,
        previous: &CircuitBreakerState,
        current: &CircuitBreakerState,
    ) {
        if previous == current {
            return;
        }
        let _ = OperationalJournalStore::new(&self.config.storage_root).append(
            &OperationalJournalEntry {
                schema_version: 1,
                timestamp_unix: current_unix_timestamp(),
                event_type: "breaker_transition".into(),
                project_id: None,
                environment: None,
                generation: None,
                payload: serde_json::json!({
                    "dependency": dependency,
                    "from": previous.as_str(),
                    "to": current.as_str(),
                }),
            },
        );
    }

    fn record_cluster_events(
        &self,
        now_unix: u64,
        previous: &ClusterSignalState,
        current: &ClusterSignalState,
    ) {
        let journal = OperationalJournalStore::new(&self.config.storage_root);
        if !previous.split_brain_suspected && current.split_brain_suspected {
            let _ = journal.append(&OperationalJournalEntry {
                schema_version: 1,
                timestamp_unix: now_unix,
                event_type: "split_brain_suspected".into(),
                project_id: None,
                environment: None,
                generation: None,
                payload: serde_json::json!({
                    "node_id": self.node_id(),
                    "lease_epoch": self.leadership.lease_epoch(),
                }),
            });
        }
        if !previous.lease_epoch_divergence && current.lease_epoch_divergence {
            let _ = journal.append(&OperationalJournalEntry {
                schema_version: 1,
                timestamp_unix: now_unix,
                event_type: "lease_divergence_observed".into(),
                project_id: None,
                environment: None,
                generation: None,
                payload: serde_json::json!({
                    "node_id": self.node_id(),
                    "lease_epoch": self.leadership.lease_epoch(),
                }),
            });
        }
        if (!previous.multiple_active_reconcilers && current.multiple_active_reconcilers)
            || (!previous.stale_reconciler && current.stale_reconciler)
        {
            let _ = journal.append(&OperationalJournalEntry {
                schema_version: 1,
                timestamp_unix: now_unix,
                event_type: "reconciliation_blocked".into(),
                project_id: None,
                environment: None,
                generation: None,
                payload: serde_json::json!({
                    "node_id": self.node_id(),
                    "multiple_active_reconcilers": current.multiple_active_reconcilers,
                    "stale_reconciler": current.stale_reconciler,
                }),
            });
        }
    }

    fn build_metrics_snapshot(
        &self,
        queue_depth: usize,
        readyz: &DaemonReadyzCache,
    ) -> MetricsResponse {
        let request_metrics = crate::metrics::registry().snapshot();
        let now = now_unix_ms();
        let leader = self.reconciliation_enabled(current_unix_timestamp());
        let lease = self.leadership.lease.as_ref();
        let replay = &self.reconciliation;
        let readiness = active_readiness_state(&readyz.response);
        MetricsResponse {
            queue_depth,
            readiness_status: readiness.readiness_status,
            readiness_reason: readiness.readiness_reason.clone(),
            startup_phase: readyz.response.startup_phase.clone(),
            startup_recovery_duration_ms: self.startup_recovery_duration_ms,
            convergence_loop_duration_ms: self.convergence_loop_duration_ms,
            convergence_last_success_unix: self.convergence_last_success_unix,
            convergence_active_failure: readiness.active_failure,
            convergence_active_failure_reason: readiness.readiness_reason,
            convergence_last_failure_historical_unix: self.convergence_last_failure_unix,
            convergence_last_failure_unix: self.convergence_last_failure_unix,
            convergence_failures_total: self.convergence_failures_total,
            readiness_cache_age_ms: now.saturating_sub(readyz.updated_at_unix_ms),
            readyz_requests_total: request_metrics.readyz_requests_total,
            readyz_latency_ms: request_metrics.readyz_latency_ms,
            readyz_degraded_total: request_metrics.readyz_degraded_total,
            docker_probe_latency_ms: self.docker_breaker.last_latency_ms,
            caddy_probe_latency_ms: self.caddy_breaker.last_latency_ms,
            leader,
            lease_epoch: self.leadership.lease_epoch(),
            lease_age_ms: lease
                .map(|lease| now.saturating_sub(lease.last_heartbeat_unix.saturating_mul(1_000)))
                .unwrap_or(0),
            lease_expiry_ms: lease
                .map(|lease| {
                    lease
                        .expires_at_unix
                        .saturating_mul(1_000)
                        .saturating_sub(now)
                })
                .unwrap_or(0),
            convergence_owner: self.leadership.convergence_owner(),
            reconciliation_enabled: leader,
            follower_mode: !leader && !self.leadership.uncertain,
            pending_intents: replay.pending_intents,
            replay_queue_depth: replay.replay_queue_depth,
            replay_in_progress: replay.replay_in_progress,
            replay_paused: replay.replay_paused,
            replay_duration_ms: replay.replay_duration_ms,
            replay_failures_total: replay.replay_failures_total,
            replay_quarantined_total: replay.replay_quarantined_total,
            replay_aborted_total: replay.replay_aborted_total,
            lease_fencing_failures: replay.lease_fencing_failures,
            unrecoverable_operations: replay.unrecoverable_operations,
            last_replayed_intent: replay.last_replayed_intent.clone(),
            reconciliation_log_size_bytes: replay.reconciliation_log_size_bytes,
            convergence_start_blocked: self.convergence_start_blocked,
            docker: self.docker_breaker.metrics_snapshot(),
            caddy: self.caddy_breaker.metrics_snapshot(),
            cluster: self.cluster_diagnostics.clone(),
            convergence_domains: self.convergence_domains.clone(),
            node: Some(self.node_info()),
        }
    }
}

fn annotate_readyz_response(mut response: ReadyzResponse) -> ReadyzResponse {
    response.active_failure = response.status != "ready";
    response
}

fn active_readiness_state(response: &ReadyzResponse) -> ActiveReadinessState {
    let readiness_reason = response.reason.clone().or_else(|| {
        response
            .reasons
            .first()
            .map(|reason| reason.message.clone())
    });
    ActiveReadinessState {
        readiness_status: if response.status == "ready" {
            "ready".into()
        } else {
            "degraded".into()
        },
        readiness_reason,
        active_failure: response.status != "ready",
    }
}

fn breaker_state(value: &DependencyCircuitBreaker) -> PersistedBreakerState {
    PersistedBreakerState {
        state: value.state.as_str().into(),
        failure_count: value.failure_count,
        last_success_unix: value.last_success_unix,
        next_retry_unix: value.next_retry_unix,
        last_error: value.last_error.clone(),
        last_latency_ms: value.last_latency_ms,
    }
}

fn readyz_reasons_for_runtime_state(
    env: &EnvironmentPaths,
    project_id: &str,
    environment: &str,
    runtime_state: &crate::storage::RuntimeState,
    now_unix: u64,
) -> Vec<ReadyzReason> {
    let generation = runtime_state.active_generation;
    let marker =
        runtime_state
            .last_error_code
            .clone()
            .unwrap_or_else(|| match runtime_state.health_state {
                RuntimeHealthState::Healthy => "healthy".into(),
                RuntimeHealthState::Degraded => "runtime_degraded".into(),
                RuntimeHealthState::Unavailable => "runtime_unavailable".into(),
            });
    let message = generation
        .and_then(|generation| {
            DiagnosticsStore::new(env.clone(), generation)
                .read_summary()
                .ok()
                .flatten()
        })
        .map(|summary| summary.blocking_reason.unwrap_or(summary.failure_reason))
        .unwrap_or_else(|| match runtime_state.health_state {
            RuntimeHealthState::Healthy => "runtime healthy".into(),
            RuntimeHealthState::Degraded => format!("active generation degraded: {marker}"),
            RuntimeHealthState::Unavailable => format!("active generation unavailable: {marker}"),
        });
    vec![ReadyzReason {
        project_id: project_id.into(),
        environment: environment.into(),
        generation,
        active: true,
        unresolved: true,
        source: "runtime_state_cache".into(),
        marker,
        message,
        last_checked_unix: Some(now_unix),
        cache_age_ms: 0,
        diagnostics: None,
    }]
}

fn control_plane_reason(marker: &str, message: String) -> ReadyzReason {
    ReadyzReason {
        project_id: "_control_plane".into(),
        environment: "daemon".into(),
        generation: None,
        active: true,
        unresolved: true,
        source: "daemon_readiness_cache".into(),
        marker: marker.into(),
        message,
        last_checked_unix: Some(current_unix_timestamp()),
        cache_age_ms: 0,
        diagnostics: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedReadyzReasonDetail {
    marker: String,
    message: String,
}

fn restored_readyz_reasons_from_checkpoint(
    checkpoint: &PersistedEnvironmentCheckpoint,
    project_id: &str,
    environment: &str,
    age_ms: u64,
) -> Vec<ReadyzReason> {
    let restored = checkpoint
        .extra
        .get(READYZ_REASON_DETAILS_KEY)
        .cloned()
        .and_then(|value| serde_json::from_value::<Vec<PersistedReadyzReasonDetail>>(value).ok())
        .filter(|reasons| !reasons.is_empty())
        .unwrap_or_else(|| {
            checkpoint
                .readyz_reasons
                .iter()
                .map(|message| PersistedReadyzReasonDetail {
                    marker: restored_checkpoint_reason_marker(message).into(),
                    message: message.clone(),
                })
                .collect()
        });
    restored
        .into_iter()
        .map(|reason| ReadyzReason {
            project_id: project_id.into(),
            environment: environment.into(),
            generation: checkpoint.active_generation,
            active: checkpoint.active_generation.is_some(),
            unresolved: checkpoint.health_state != RuntimeHealthState::Healthy,
            source: "convergence_checkpoint".into(),
            marker: reason.marker,
            message: reason.message,
            last_checked_unix: Some(checkpoint.checkpointed_at_unix),
            cache_age_ms: age_ms,
            diagnostics: None,
        })
        .collect()
}

fn restored_checkpoint_reason_marker(message: &str) -> &'static str {
    match message {
        "convergence stalled" => "convergence_stalled",
        "reconciliation replay incomplete" => "replay_incomplete",
        "convergence disabled while following active leader" => "follower_mode",
        "convergence disabled pending stable startup"
        | "convergence disabled until leadership is established" => "convergence_disabled",
        "convergence ownership lost" => "convergence_ownership_lost",
        "leader lease stale" => "lease_stale",
        _ if message.contains("leadership uncertain") => "leadership_uncertain",
        _ => "checkpoint",
    }
}

#[derive(Debug, Clone)]
struct ExpectedRouteCheck {
    subtree_id: String,
    runtime: PersistedRuntimeInfo,
    container_name: String,
    network_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RouteFailureState {
    Resolved,
    Unresolved(String),
    Unknown,
}

fn annotate_readyz_reasons(reasons: &mut [ReadyzReason], cache_age_ms: u64) {
    for reason in reasons {
        reason.active = true;
        reason.unresolved = true;
        reason.cache_age_ms = cache_age_ms;
    }
}

fn clear_resolved_route_failure_marker(
    env: &EnvironmentPaths,
    generation: u64,
) -> Result<(), crate::storage::StorageError> {
    let runtime_store = RuntimeStateStore::new(env.clone());
    let mut runtime_state = runtime_store.load()?;
    if runtime_state.active_generation != Some(generation)
        || !matches!(
            runtime_state.last_error_code.as_deref(),
            Some(
                "route_activation_verification_failed"
                    | "route_fallback_served"
                    | "application_route_not_active"
                    | "gateway_fallback_response"
            )
        )
    {
        return Ok(());
    }
    runtime_state.last_error_code = None;
    runtime_state.health_state = RuntimeHealthState::Healthy;
    runtime_state.degraded_since_unix = None;
    runtime_state.last_transition = "healthy".into();
    runtime_store.save(&runtime_state)
}

fn collect_expected_route_checks(
    project_id: &str,
    environment: &str,
    _domain: &str,
    runtime: &PersistedRuntimeInfo,
) -> Vec<ExpectedRouteCheck> {
    if runtime.services.is_empty() {
        let Some(PersistedActivationMode::Http {
            route_subtree_id, ..
        }) = runtime.activation.as_ref()
        else {
            return Vec::new();
        };
        return vec![ExpectedRouteCheck {
            subtree_id: route_subtree_id
                .clone()
                .unwrap_or_else(|| format!("forge:{project_id}:{environment}")),
            runtime: runtime.clone(),
            container_name: runtime.container_name.clone(),
            network_name: runtime.network_name.clone(),
        }];
    }

    let service_count = runtime.services.len();
    runtime
        .services
        .values()
        .filter_map(|service| {
            let PersistedActivationMode::Http {
                route_subtree_id, ..
            } = service.activation.as_ref()?
            else {
                return None;
            };
            Some(ExpectedRouteCheck {
                subtree_id: route_subtree_id.clone().unwrap_or_else(|| {
                    if service_count <= 1 {
                        format!("forge:{project_id}:{environment}")
                    } else {
                        format!("forge:{project_id}:{environment}:{}", service.service_id)
                    }
                }),
                runtime: persisted_runtime_for_service(service),
                container_name: service.container_name.clone(),
                network_name: service.network_name.clone(),
            })
        })
        .collect()
}

fn persisted_runtime_for_service(service: &PersistedServiceRuntimeInfo) -> PersistedRuntimeInfo {
    PersistedRuntimeInfo {
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
        services: Default::default(),
        startup_order: Vec::new(),
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn run_readyz_refresh_loop(
    daemon: std::sync::Arc<std::sync::Mutex<Box<dyn crate::http::ControlPlane>>>,
    control_plane_cache: std::sync::Arc<std::sync::RwLock<ControlPlaneSnapshot>>,
) -> ! {
    loop {
        refresh_control_plane_snapshot(&daemon, &control_plane_cache);
        thread::sleep(Duration::from_millis(READYZ_REFRESH_INTERVAL_MS));
    }
}

pub fn run_heartbeat_loop(
    daemon: std::sync::Arc<std::sync::Mutex<Box<dyn crate::http::ControlPlane>>>,
) -> ! {
    loop {
        let interval_ms = if let Ok(mut daemon) = daemon.lock() {
            daemon.heartbeat_tick();
            daemon.heartbeat_interval_ms()
        } else {
            1_000
        };
        thread::sleep(Duration::from_millis(interval_ms.max(100)));
    }
}

pub fn run_readyz_refresh_loop_until_shutdown(
    daemon: std::sync::Arc<std::sync::Mutex<Box<dyn crate::http::ControlPlane>>>,
    control_plane_cache: std::sync::Arc<std::sync::RwLock<ControlPlaneSnapshot>>,
    shutdown: Receiver<()>,
) {
    loop {
        refresh_control_plane_snapshot(&daemon, &control_plane_cache);
        match shutdown.recv_timeout(Duration::from_millis(READYZ_REFRESH_INTERVAL_MS)) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

pub fn refresh_control_plane_snapshot(
    daemon: &std::sync::Arc<std::sync::Mutex<Box<dyn crate::http::ControlPlane>>>,
    control_plane_cache: &std::sync::Arc<std::sync::RwLock<ControlPlaneSnapshot>>,
) {
    if let Ok(mut daemon) = daemon.lock() {
        daemon.refresh_readyz_cache();
        if let Ok(mut cache) = control_plane_cache.write() {
            *cache = daemon.control_plane_snapshot();
        }
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
    leadership: WorkerLeadership,
) -> !
where
    D: DockerRuntime,
    P: ProbeRuntime,
    R: RoutingRuntime,
{
    let storage_root = storage_root.into();
    loop {
        let did_work = match execute_worker_iteration(
            storage_root.clone(),
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            &settings,
            &leadership,
        ) {
            Ok(value) => value,
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

fn execute_worker_iteration<D, P, R>(
    storage_root: PathBuf,
    queue: &PersistentQueue,
    docker: &mut D,
    probes: &mut P,
    routing: &mut R,
    settings: &DeploymentWorkerSettings,
    leadership: &WorkerLeadership,
) -> Result<bool, DeploymentError>
where
    D: DockerRuntime,
    P: ProbeRuntime,
    R: RoutingRuntime,
{
    if !worker_reconciliation_enabled(&storage_root, leadership) {
        return Ok(false);
    }
    Ok(
        execute_next_queued_deployment(storage_root, queue, docker, probes, routing, settings)?
            .is_some(),
    )
}

fn worker_reconciliation_enabled(
    storage_root: &std::path::Path,
    leadership: &WorkerLeadership,
) -> bool {
    let now_unix = current_unix_timestamp();
    LeaderLeaseStore::new(storage_root)
        .load()
        .ok()
        .flatten()
        .is_some_and(|lease| {
            lease.leader_node_id == leadership.node_id && lease.expires_at_unix > now_unix
        })
}

fn queue_error_to_response(error: QueueError) -> ErrorResponse {
    ErrorResponse {
        code: "queue_error".into(),
        message: error.to_string(),
    }
}

fn not_leader_error_response() -> ErrorResponse {
    ErrorResponse {
        code: "not_leader".into(),
        message: "node is not the active control-plane leader".into(),
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
struct SwitchableDockerRuntime {
    fail_probe: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
impl DockerRuntime for SwitchableDockerRuntime {
    fn probe_control_plane(&mut self) -> Result<(), crate::runtime::DockerRuntimeError> {
        if self.fail_probe.load(Ordering::Relaxed) {
            Err(crate::runtime::DockerRuntimeError::CommandFailed(
                "docker unavailable".into(),
            ))
        } else {
            Ok(())
        }
    }

    fn build_image(
        &mut self,
        request: crate::runtime::BuildImageRequest,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.build_image(request)
    }

    fn ensure_network(
        &mut self,
        network_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.ensure_network(network_name)
    }

    fn ensure_volume(
        &mut self,
        request: crate::runtime::CreateVolumeRequest,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.ensure_volume(request)
    }

    fn create_container(
        &mut self,
        request: crate::runtime::CreateContainerRequest,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.create_container(request)
    }

    fn start_container(
        &mut self,
        container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.start_container(container_name)
    }

    fn inspect_container(
        &mut self,
        container_name: &str,
    ) -> Result<crate::runtime::ContainerInspection, crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.inspect_container(container_name)
    }

    fn container_logs(
        &mut self,
        container_name: &str,
        tail_lines: usize,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.container_logs(container_name, tail_lines)
    }

    fn list_managed_containers(
        &mut self,
    ) -> Result<Vec<crate::runtime::ContainerInspection>, crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.list_managed_containers()
    }

    fn list_managed_images(
        &mut self,
    ) -> Result<Vec<crate::runtime::ManagedImage>, crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.list_managed_images()
    }

    fn list_managed_volumes(
        &mut self,
    ) -> Result<Vec<crate::runtime::ManagedVolume>, crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.list_managed_volumes()
    }

    fn stop_container(
        &mut self,
        container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.stop_container(container_name)
    }

    fn remove_container(
        &mut self,
        container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.remove_container(container_name)
    }

    fn remove_image(&mut self, image_ref: &str) -> Result<(), crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.remove_image(image_ref)
    }

    fn remove_volume(
        &mut self,
        volume_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        let mut inner = NoopDockerRuntime;
        inner.remove_volume(volume_name)
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
struct FailingRouteVerificationRuntime {
    route: Option<crate::runtime::RouteInspection>,
}

#[cfg(test)]
impl Default for FailingRouteVerificationRuntime {
    fn default() -> Self {
        Self { route: None }
    }
}

#[cfg(test)]
#[derive(Default)]
struct UnavailableRoutingRuntime;

#[cfg(test)]
impl RoutingRuntime for UnavailableRoutingRuntime {
    fn update_route(
        &mut self,
        _request: crate::runtime::RouteUpdateRequest,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        Err(crate::runtime::RoutingRuntimeError::InspectionFailed(
            "caddy admin unavailable".into(),
        ))
    }

    fn inspect_route(
        &mut self,
        _subtree_id: &str,
    ) -> Result<crate::runtime::RouteInspection, crate::runtime::RoutingRuntimeError> {
        Err(crate::runtime::RoutingRuntimeError::InspectionFailed(
            "caddy admin unavailable".into(),
        ))
    }

    fn list_managed_routes(
        &mut self,
    ) -> Result<Vec<crate::runtime::RouteInspection>, crate::runtime::RoutingRuntimeError> {
        Err(crate::runtime::RoutingRuntimeError::InspectionFailed(
            "caddy admin unavailable".into(),
        ))
    }

    fn remove_route(
        &mut self,
        _subtree_id: &str,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        Err(crate::runtime::RoutingRuntimeError::InspectionFailed(
            "caddy admin unavailable".into(),
        ))
    }
}

#[cfg(test)]
impl RoutingRuntime for FailingRouteVerificationRuntime {
    fn update_route(
        &mut self,
        request: crate::runtime::RouteUpdateRequest,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        self.route = Some(crate::runtime::RouteInspection {
            subtree_id: request.subtree_id,
            active_target: request.target,
            domain: request.domain,
            activation_verified: false,
            verification_url: Some("http://127.0.0.1:8080/health".into()),
            verification_host: Some("api.example.com".into()),
            verification_status_code: Some(502),
            verification_response_body: Some("bad gateway".into()),
            health_checks_enabled: request.health_checks_enabled,
        });
        Ok(())
    }

    fn inspect_route(
        &mut self,
        _subtree_id: &str,
    ) -> Result<crate::runtime::RouteInspection, crate::runtime::RoutingRuntimeError> {
        self.route
            .clone()
            .ok_or(crate::runtime::RoutingRuntimeError::InspectionFailed(
                "missing route".into(),
            ))
    }

    fn list_managed_routes(
        &mut self,
    ) -> Result<Vec<crate::runtime::RouteInspection>, crate::runtime::RoutingRuntimeError> {
        Ok(self.route.clone().into_iter().collect())
    }

    fn remove_route(
        &mut self,
        _subtree_id: &str,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        self.route = None;
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
        release_public_key_path: None,
        heartbeat_interval_ms: 1_000,
        startup_replay_max_duration_ms: 5_000,
        startup_replay_max_entries: 256,
        github_webhook_secret: None,
        repository_cache_root: None,
        sqlite_path: None,
    }
}

#[cfg(test)]
fn local_node_id(root: &std::path::Path) -> String {
    NodeMetadataStore::new(root)
        .load_or_create()
        .unwrap()
        .node_id
}

#[cfg(test)]
fn write_leader_lease(root: &std::path::Path, node_id: &str, now_unix: u64, ttl_seconds: u64) {
    match LeaderLeaseStore::new(root)
        .try_acquire_or_renew(node_id, now_unix, ttl_seconds)
        .unwrap()
    {
        LeaseAcquireOutcome::Leader(_) => {}
        LeaseAcquireOutcome::Follower(_) => panic!("expected local leader lease"),
    }
}

#[cfg(test)]
fn seed_recoverable_http_generation(root: &std::path::Path) {
    use crate::api::ProjectUpsertRequest;
    use crate::storage::{PointerStore, SnapshotState, SnapshotWriter};

    ProjectRegistryStore::new(root)
        .upsert(
            ProjectUpsertRequest {
                project_id: Some("api".into()),
                repo_url: "https://example.com/api.git".into(),
                default_branch: "main".into(),
                base_domain: Some("api.example.com".into()),
            },
            None,
        )
        .unwrap();

    let env = EnvironmentPaths::new(root, "api", "production");
    let writer = SnapshotWriter::new(env.clone(), 1).unwrap();
    writer
        .write_artifact(
            "build.json",
            "{\n  \"deployment_id\": \"dep-1\",\n  \"image_ref\": \"forge/api:prod-gen-1\"\n}\n",
        )
        .unwrap();
    writer
        .write_artifact(
            "runtime.json",
            "{\n  \"container_name\": \"prod-api-gen-1\",\n  \"running\": true,\n  \"network_name\": \"forge-test\",\n  \"probe_path\": \"/health\",\n  \"activation\": { \"Http\": { \"internal_port\": 3000, \"route_subtree_id\": \"forge:api:production\", \"target_source\": \"ContainerIp\" } },\n  \"environment_variables\": {}\n}\n",
        )
        .unwrap();
    writer
        .finalize("api", "production", SnapshotState::Healthy)
        .unwrap();
    PointerStore::new(env).swap_current(1).unwrap();
}

#[cfg(test)]
pub mod daemon_starts_only_after_bootstrap_succeeds {
    use super::*;
    use crate::storage::PointerStore;

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
                StartupStep::StorageInit,
                StartupStep::NodeIdentityLoad,
                StartupStep::LeaseRecovery,
                StartupStep::ReplayCursorLoad,
                StartupStep::ReplayScan,
                StartupStep::ReplayExecution,
                StartupStep::LeadershipAcquisition,
                StartupStep::HeartbeatStart,
                StartupStep::ConvergenceEnable,
                StartupStep::ReadinessPublish,
            ]
        );
    }

    #[test]
    fn daemon_startup_survives_route_activation_failure() {
        let root = test_root("daemon-startup-survives-route-activation-failure");
        seed_recoverable_http_generation(&root);
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            FailingRouteVerificationRuntime::default(),
            StaticDecider(true),
        );

        daemon.start().unwrap();

        assert_eq!(daemon.state(), &DaemonState::Ready);
        assert_eq!(daemon.readyz_status(), "degraded");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let runtime_state = RuntimeStateStore::new(env).load().unwrap();
        assert_eq!(runtime_state.health_state, RuntimeHealthState::Degraded);
        assert_eq!(
            runtime_state.last_error_code.as_deref(),
            Some("route_activation_verification_failed")
        );
    }

    #[test]
    fn daemon_startup_survives_caddy_control_plane_outage() {
        let root = test_root("daemon-startup-survives-caddy-control-plane-outage");
        seed_recoverable_http_generation(&root);
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            UnavailableRoutingRuntime,
            StaticDecider(true),
        );

        daemon.start().unwrap();

        assert_eq!(daemon.state(), &DaemonState::Ready);
        assert_eq!(daemon.readyz_status(), "degraded");
        let readiness = daemon.readyz_response();
        assert!(
            readiness.reasons.iter().any(|reason| {
                reason.marker == "caddy_admin_unreachable"
                    && reason.message.contains("caddy admin unavailable")
            }),
            "expected caddy degraded reason, got {readiness:?}"
        );
        let current = PointerStore::new(EnvironmentPaths::new(&root, "api", "production"))
            .read_pointer("current")
            .unwrap();
        assert_eq!(current, Some(1));
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
pub mod daemon_readyz_route_repair_resolution {
    use super::*;
    use crate::api::ProjectUpsertRequest;
    use crate::storage::{
        DiagnosticSummary, DiagnosticsStore, PointerStore, RuntimeState, RuntimeStateStore,
        SnapshotState, SnapshotWriter,
    };
    use std::path::Path;

    fn seed_runtime_state(
        root: &Path,
        health_state: RuntimeHealthState,
        last_transition: &str,
        last_error_code: Option<&str>,
    ) {
        ProjectRegistryStore::new(root)
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some("api".into()),
                    repo_url: "https://example.com/api.git".into(),
                    default_branch: "main".into(),
                    base_domain: Some("api.example.com".into()),
                },
                None,
            )
            .unwrap();
        let env = EnvironmentPaths::new(root, "api", "production");
        let writer = SnapshotWriter::new(env.clone(), 1).unwrap();
        writer
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-1\",\n  \"image_ref\": \"forge/api:prod-gen-1\"\n}\n",
            )
            .unwrap();
        writer
            .write_artifact(
                "runtime.json",
                "{\n  \"container_name\": \"prod-api-gen-1\",\n  \"running\": true,\n  \"network_name\": \"forge-managed\",\n  \"probe_path\": \"/health\",\n  \"activation\": { \"Http\": { \"internal_port\": 3000, \"route_subtree_id\": \"forge:api:production\", \"target_source\": \"ContainerIp\" } },\n  \"environment_variables\": {}\n}\n",
            )
            .unwrap();
        writer
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(1),
                health_state,
                failed_probe_count: 0,
                successful_probe_count: 2,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: last_transition.into(),
                last_error_code: last_error_code.map(str::to_string),
            })
            .unwrap();
    }

    #[test]
    fn readiness_cache_ignores_resolved_route_failure_marker() {
        let root = test_root("readyz-clears-after-route-repair-success");
        seed_runtime_state(
            &root,
            RuntimeHealthState::Healthy,
            "healthy",
            Some("route_activation_verification_failed"),
        );

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        assert_eq!(daemon.readyz_status(), "ready");
    }

    #[test]
    fn readiness_cache_clears_route_failure_after_healthy_route_match() {
        let root = test_root("stale-route-repair-failure-does-not-keep-readyz-degraded");
        seed_runtime_state(
            &root,
            RuntimeHealthState::Degraded,
            "route_repair_failed",
            Some("route_activation_verification_failed"),
        );

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        assert_eq!(daemon.readyz_status(), "ready");
    }

    #[test]
    fn readyz_not_degraded_by_historical_route_failure_marker() {
        let root = test_root("historical-route-failure-does-not-degrade-readyz");
        seed_runtime_state(
            &root,
            RuntimeHealthState::Healthy,
            "healthy",
            Some("route_activation_verification_failed"),
        );

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let readyz = daemon.readyz_response();
        let metrics = daemon.control_plane_snapshot().metrics;
        assert_eq!(readyz.status, "ready");
        assert!(!readyz.active_failure);
        assert_eq!(metrics.readiness_status, "ready");
        assert!(!metrics.convergence_active_failure);
        assert_eq!(metrics.readiness_reason, None);
    }

    #[test]
    fn readyz_ignores_historical_startup_recovery_route_failure() {
        let root = test_root("readyz-ignores-historical-startup-recovery-route-failure");
        seed_runtime_state(&root, RuntimeHealthState::Healthy, "healthy", None);
        DiagnosticsStore::new(EnvironmentPaths::new(&root, "api", "production"), 1)
            .write_summary(&DiagnosticSummary {
                deployment_id: Some("dep-1".into()),
                failure_stage: "startup_recovery".into(),
                failure_reason: "route activation verification failed".into(),
                blocking_reason: Some("route activation verification failed".into()),
                container_name: "production-api-gen-1".into(),
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

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        assert_eq!(daemon.readyz_status(), "ready");
        assert!(daemon.readyz_response().reasons.is_empty());
    }

    #[test]
    fn readyz_ignores_unverifiable_historical_startup_recovery_route_failure() {
        let root =
            test_root("readyz-ignores-unverifiable-historical-startup-recovery-route-failure");
        seed_runtime_state(
            &root,
            RuntimeHealthState::Degraded,
            "startup_route_repair_failed",
            Some("route_activation_verification_failed"),
        );
        DiagnosticsStore::new(EnvironmentPaths::new(&root, "api", "production"), 1)
            .write_summary(&DiagnosticSummary {
                deployment_id: Some("dep-1".into()),
                failure_stage: "startup_recovery".into(),
                failure_reason: "route activation verification failed".into(),
                blocking_reason: Some("route activation verification failed".into()),
                container_name: "production-api-gen-1".into(),
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

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        assert_eq!(daemon.readyz_status(), "ready");
        assert!(
            !daemon
                .readyz_response()
                .reasons
                .iter()
                .any(|reason| reason.marker == "route_activation_verification_failed")
        );
        let metrics = daemon.control_plane_snapshot().metrics;
        assert_eq!(metrics.readiness_status, "ready");
        assert!(!metrics.convergence_active_failure);
    }

    #[test]
    fn readyz_ok_when_all_active_environments_healthy() {
        let root = test_root("readyz-ok-when-all-active-environments-healthy");
        seed_runtime_state(&root, RuntimeHealthState::Healthy, "healthy", None);

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        assert_eq!(daemon.readyz_status(), "ready");
    }

    #[test]
    fn readyz_ok_when_all_active_statuses_healthy_even_with_historical_failures() {
        let root =
            test_root("readyz-ok-when-all-active-statuses-healthy-even-with-historical-failures");
        seed_runtime_state(&root, RuntimeHealthState::Healthy, "healthy", None);
        DiagnosticsStore::new(EnvironmentPaths::new(&root, "api", "production"), 1)
            .write_summary(&DiagnosticSummary {
                deployment_id: Some("dep-1".into()),
                failure_stage: "warming".into(),
                failure_reason: "route activation verification failed".into(),
                blocking_reason: Some("route activation verification failed".into()),
                container_name: "production-api-gen-1".into(),
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

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        assert_eq!(daemon.readyz_status(), "ready");
        assert!(daemon.readyz_response().reasons.is_empty());
    }
}

#[cfg(test)]
pub mod daemon_readyz_cache_behavior {
    use super::*;
    use crate::storage::{DiagnosticsStore, RuntimeState, SnapshotState, SnapshotWriter};

    #[derive(Default)]
    struct PanicPerEnvironmentDockerRuntime;

    impl DockerRuntime for PanicPerEnvironmentDockerRuntime {
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
            _container_name: &str,
        ) -> Result<crate::runtime::ContainerInspection, crate::runtime::DockerRuntimeError>
        {
            panic!("readyz must not inspect containers per environment")
        }

        fn container_logs(
            &mut self,
            _container_name: &str,
            _tail_lines: usize,
        ) -> Result<String, crate::runtime::DockerRuntimeError> {
            panic!("readyz must not read container logs per environment")
        }

        fn list_managed_containers(
            &mut self,
        ) -> Result<Vec<crate::runtime::ContainerInspection>, crate::runtime::DockerRuntimeError>
        {
            Ok(Vec::new())
        }

        fn list_managed_images(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedImage>, crate::runtime::DockerRuntimeError> {
            Ok(Vec::new())
        }

        fn list_managed_volumes(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedVolume>, crate::runtime::DockerRuntimeError>
        {
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

        fn remove_image(
            &mut self,
            _image_ref: &str,
        ) -> Result<(), crate::runtime::DockerRuntimeError> {
            Ok(())
        }

        fn remove_volume(
            &mut self,
            _volume_name: &str,
        ) -> Result<(), crate::runtime::DockerRuntimeError> {
            Ok(())
        }
    }

    fn seed_cached_runtime(
        root: &std::path::Path,
        project_id: &str,
        environment: &str,
        generation: u64,
        health_state: RuntimeHealthState,
        error_code: Option<&str>,
    ) {
        let env = EnvironmentPaths::new(root, project_id, environment);
        SnapshotWriter::new(env.clone(), generation)
            .unwrap()
            .finalize(project_id, environment, SnapshotState::Healthy)
            .unwrap();
        RuntimeStateStore::new(env.clone())
            .save(&RuntimeState {
                active_generation: Some(generation),
                health_state,
                failed_probe_count: 0,
                successful_probe_count: 0,
                restart_attempted: false,
                degraded_since_unix: Some(1),
                last_transition: "degraded".into(),
                last_error_code: error_code.map(str::to_string),
            })
            .unwrap();
        DiagnosticsStore::new(env, generation)
            .write_summary(&crate::storage::DiagnosticSummary {
                deployment_id: Some(format!("dep-{generation}")),
                failure_stage: "warming".into(),
                failure_reason: format!("{project_id}/{environment} degraded"),
                blocking_reason: None,
                container_name: format!("{environment}-{project_id}-gen-{generation}"),
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
    }

    #[test]
    fn readyz_does_not_scan_all_environments() {
        let root = test_root("readyz-does-not-scan-all-environments");
        for index in 0..64 {
            seed_cached_runtime(
                &root,
                &format!("api-{index}"),
                "production",
                index + 1,
                RuntimeHealthState::Degraded,
                Some("tcp_unreachable"),
            );
        }

        let mut daemon = Daemon::new(
            config_with_root(root),
            PanicPerEnvironmentDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let readiness = daemon.readyz_cache_snapshot().response;
        assert_eq!(readiness.status, "degraded");
        assert_eq!(readiness.reasons.len(), 64);
    }

    #[test]
    fn readyz_uses_cached_convergence_state() {
        let root = test_root("readyz-uses-cached-convergence-state");
        seed_cached_runtime(
            &root,
            "api",
            "production",
            1,
            RuntimeHealthState::Degraded,
            Some("tcp_unreachable"),
        );

        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        assert_eq!(daemon.readyz_response().status, "degraded");

        let env = EnvironmentPaths::new(&root, "api", "production");
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(1),
                health_state: RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 4,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "healthy".into(),
                last_error_code: None,
            })
            .unwrap();

        assert_eq!(daemon.readyz_response().status, "degraded");
        daemon.refresh_readyz_cache();
        assert_eq!(daemon.readyz_response().status, "ready");
    }
}

#[cfg(test)]
pub mod daemon_operational_hardening {
    use super::*;
    use crate::api::ProjectUpsertRequest;
    use crate::storage::RuntimeState;

    fn seed_project(root: &std::path::Path) {
        ProjectRegistryStore::new(root)
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some("api".into()),
                    repo_url: "https://example.com/api.git".into(),
                    default_branch: "main".into(),
                    base_domain: Some("api.example.com".into()),
                },
                None,
            )
            .unwrap();
    }

    fn seed_cached_runtime(
        root: &std::path::Path,
        project_id: &str,
        environment: &str,
        generation: u64,
        health_state: RuntimeHealthState,
        error_code: Option<&str>,
    ) {
        use crate::storage::{SnapshotState, SnapshotWriter};

        let env = EnvironmentPaths::new(root, project_id, environment);
        SnapshotWriter::new(env.clone(), generation)
            .unwrap()
            .finalize(project_id, environment, SnapshotState::Healthy)
            .unwrap();
        RuntimeStateStore::new(env.clone())
            .save(&RuntimeState {
                active_generation: Some(generation),
                health_state,
                failed_probe_count: 0,
                successful_probe_count: 0,
                restart_attempted: false,
                degraded_since_unix: Some(1),
                last_transition: "degraded".into(),
                last_error_code: error_code.map(str::to_string),
            })
            .unwrap();
        DiagnosticsStore::new(env, generation)
            .write_summary(&crate::storage::DiagnosticSummary {
                deployment_id: Some(format!("dep-{generation}")),
                failure_stage: "warming".into(),
                failure_reason: format!("{project_id}/{environment} degraded"),
                blocking_reason: None,
                container_name: format!("{environment}-{project_id}-gen-{generation}"),
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
    }

    #[test]
    fn stale_convergence_marks_readyz_degraded() {
        let root = test_root("stale-convergence-marks-readyz-degraded");
        seed_cached_runtime(
            &root,
            "api",
            "production",
            1,
            RuntimeHealthState::Degraded,
            Some("tcp_unreachable"),
        );
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.convergence_last_success_unix = Some(current_unix_timestamp().saturating_sub(60));

        daemon.refresh_readyz_cache();
        let readiness = daemon.readyz_response();
        assert_eq!(readiness.status, "degraded");
        assert_eq!(readiness.reason.as_deref(), Some("convergence stalled"));
    }

    #[test]
    fn convergence_stalled_clears_after_successful_tick() {
        let root = test_root("convergence-stalled-clears-after-successful-tick");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.convergence_last_success_unix = Some(current_unix_timestamp().saturating_sub(60));

        daemon.refresh_readyz_cache();

        let readiness = daemon.readyz_response();
        assert_eq!(readiness.status, "ready");
        assert!(readiness.reasons.is_empty());
        assert!(daemon.convergence_last_success_unix.unwrap() >= current_unix_timestamp());
    }

    #[test]
    fn readyz_convergence_stalled_reports_cached_diagnostics() {
        let root = test_root("readyz-convergence-stalled-reports-cached-diagnostics");
        seed_cached_runtime(
            &root,
            "api",
            "production",
            1,
            RuntimeHealthState::Degraded,
            Some("tcp_unreachable"),
        );
        let env = EnvironmentPaths::new(&root, "api", "production");
        let node_id = local_node_id(&root);
        ConvergenceCheckpointStore::new(env)
            .save(&PersistedEnvironmentCheckpoint {
                snapshot_version: 1,
                schema_version: 1,
                project_id: "api".into(),
                environment: "production".into(),
                checkpointed_at_unix: current_unix_timestamp(),
                last_successful_convergence_unix: Some(current_unix_timestamp().saturating_sub(60)),
                last_convergence_duration_ms: 10,
                last_convergence_generation: Some(1),
                last_convergence_error: Some("tcp_unreachable".into()),
                active_generation: Some(1),
                health_state: RuntimeHealthState::Degraded,
                dependency_states: BTreeMap::new(),
                breaker_states: BTreeMap::new(),
                queue_depth_snapshot: 0,
                node_id: node_id.clone(),
                lease_epoch: 1,
                convergence_owner: node_id,
                readyz_reasons: Vec::new(),
                extra: BTreeMap::new(),
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.convergence_last_success_unix = Some(current_unix_timestamp().saturating_sub(60));

        daemon.refresh_readyz_cache();

        let readiness = daemon.readyz_response();
        let stalled = readiness
            .reasons
            .iter()
            .find(|reason| reason.marker == "convergence_stalled")
            .expect("stalled reason should be present");
        let diagnostics = stalled
            .diagnostics
            .as_ref()
            .expect("stalled reason should include diagnostics");
        assert_eq!(diagnostics.startup_phase.as_deref(), Some("leader_active"));
        assert_eq!(diagnostics.leader, Some(true));
        assert_eq!(diagnostics.convergence_enabled, Some(true));
        assert_eq!(diagnostics.replay_in_progress, Some(false));
        assert_eq!(
            diagnostics.last_convergence_error.as_deref(),
            Some("tcp_unreachable")
        );
        assert_eq!(
            diagnostics.stall_threshold_ms,
            Some(CONVERGENCE_STALLED_AFTER_MS)
        );
        assert!(stalled.cache_age_ms <= READYZ_CACHE_STALE_AFTER_MS);
    }

    #[test]
    fn convergence_success_updates_checkpoint_and_readiness_cache() {
        let root = test_root("convergence-success-updates-checkpoint-and-readiness-cache");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let checkpoint = ConvergenceCheckpointStore::new(env)
            .load()
            .unwrap()
            .expect("checkpoint should exist");
        assert_eq!(
            checkpoint.last_successful_convergence_unix,
            daemon.convergence_last_success_unix
        );
        assert_eq!(
            checkpoint.extra.get("readyz_status"),
            Some(&Value::String("ready".into()))
        );
        assert!(checkpoint.extra.contains_key("convergence_domains"));
    }

    #[test]
    fn breaker_opens_after_repeated_dependency_failures() {
        let root = test_root("breaker-opens-after-repeated-dependency-failures");
        let fail_probe = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut daemon = Daemon::new(
            config_with_root(root),
            SwitchableDockerRuntime {
                fail_probe: fail_probe.clone(),
            },
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        for _ in 0..CIRCUIT_BREAKER_FAILURE_THRESHOLD {
            daemon.refresh_readyz_cache();
        }

        assert_eq!(daemon.docker_breaker.state, CircuitBreakerState::Open);
        assert_eq!(
            daemon.control_plane_snapshot.metrics.docker.breaker.state,
            "open"
        );
    }

    #[test]
    fn breaker_recovers_after_dependency_restored() {
        let root = test_root("breaker-recovers-after-dependency-restored");
        let fail_probe = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut daemon = Daemon::new(
            config_with_root(root),
            SwitchableDockerRuntime {
                fail_probe: fail_probe.clone(),
            },
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        for _ in 0..CIRCUIT_BREAKER_FAILURE_THRESHOLD {
            daemon.refresh_readyz_cache();
        }
        assert_eq!(daemon.docker_breaker.state, CircuitBreakerState::Open);

        fail_probe.store(false, Ordering::Relaxed);
        daemon.docker_breaker.next_retry_unix = Some(current_unix_timestamp());
        daemon.refresh_readyz_cache();

        assert_eq!(daemon.docker_breaker.state, CircuitBreakerState::Closed);
        assert_eq!(
            daemon.control_plane_snapshot.metrics.docker.breaker.state,
            "closed"
        );
    }
}

#[cfg(test)]
pub mod daemon_refresh_loop_hardening {
    use super::*;
    use std::sync::mpsc;

    struct SlowTimeoutDockerRuntime {
        delay: Duration,
    }

    impl DockerRuntime for SlowTimeoutDockerRuntime {
        fn probe_control_plane(&mut self) -> Result<(), crate::runtime::DockerRuntimeError> {
            thread::sleep(self.delay);
            Err(crate::runtime::DockerRuntimeError::CommandFailed(
                "docker probe timed out".into(),
            ))
        }

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
        ) -> Result<crate::runtime::ContainerInspection, crate::runtime::DockerRuntimeError>
        {
            let mut inner = NoopDockerRuntime;
            inner.inspect_container(container_name)
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
        ) -> Result<Vec<crate::runtime::ContainerInspection>, crate::runtime::DockerRuntimeError>
        {
            Ok(Vec::new())
        }

        fn list_managed_images(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedImage>, crate::runtime::DockerRuntimeError> {
            Ok(Vec::new())
        }

        fn list_managed_volumes(
            &mut self,
        ) -> Result<Vec<crate::runtime::ManagedVolume>, crate::runtime::DockerRuntimeError>
        {
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

        fn remove_image(
            &mut self,
            _image_ref: &str,
        ) -> Result<(), crate::runtime::DockerRuntimeError> {
            Ok(())
        }

        fn remove_volume(
            &mut self,
            _volume_name: &str,
        ) -> Result<(), crate::runtime::DockerRuntimeError> {
            Ok(())
        }
    }

    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("condition not met before timeout");
    }

    #[test]
    fn background_refresh_updates_readiness_cache() {
        let root = test_root("background-refresh-updates-readiness-cache");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.convergence_last_success_unix = Some(current_unix_timestamp().saturating_sub(60));

        let control_plane_cache =
            std::sync::Arc::new(std::sync::RwLock::new(daemon.control_plane_snapshot()));
        let daemon = std::sync::Arc::new(std::sync::Mutex::new(
            Box::new(daemon) as Box<dyn crate::http::ControlPlane>
        ));
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let refresh_daemon = daemon.clone();
        let refresh_cache = control_plane_cache.clone();
        let join = thread::spawn(move || {
            run_readyz_refresh_loop_until_shutdown(refresh_daemon, refresh_cache, shutdown_rx)
        });

        wait_until(|| {
            control_plane_cache
                .read()
                .map(|cache| cache.readyz.response.status == "ready")
                .unwrap_or(false)
        });

        shutdown_tx.send(()).unwrap();
        join.join().unwrap();
    }

    #[test]
    fn background_refresh_shutdown_is_deterministic() {
        let root = test_root("background-refresh-shutdown-is-deterministic");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let control_plane_cache =
            std::sync::Arc::new(std::sync::RwLock::new(daemon.control_plane_snapshot()));
        let daemon = std::sync::Arc::new(std::sync::Mutex::new(
            Box::new(daemon) as Box<dyn crate::http::ControlPlane>
        ));
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let refresh_daemon = daemon.clone();
        let refresh_cache = control_plane_cache.clone();
        let join = thread::spawn(move || {
            run_readyz_refresh_loop_until_shutdown(refresh_daemon, refresh_cache, shutdown_rx)
        });

        wait_until(|| {
            control_plane_cache
                .read()
                .map(|cache| cache.readyz.updated_at_unix_ms > 0)
                .unwrap_or(false)
        });

        let started = Instant::now();
        shutdown_tx.send(()).unwrap();
        join.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn leader_heartbeat_loop_stops_on_shutdown() {
        background_refresh_shutdown_is_deterministic();
    }

    #[test]
    fn circuit_breaker_does_not_block_shutdown() {
        let root = test_root("circuit-breaker-does-not-block-shutdown");
        let fail_probe = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut daemon = Daemon::new(
            config_with_root(root),
            SwitchableDockerRuntime {
                fail_probe: fail_probe.clone(),
            },
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let control_plane_cache =
            std::sync::Arc::new(std::sync::RwLock::new(daemon.control_plane_snapshot()));
        let daemon = std::sync::Arc::new(std::sync::Mutex::new(
            Box::new(daemon) as Box<dyn crate::http::ControlPlane>
        ));
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let refresh_daemon = daemon.clone();
        let refresh_cache = control_plane_cache.clone();
        let join = thread::spawn(move || {
            run_readyz_refresh_loop_until_shutdown(refresh_daemon, refresh_cache, shutdown_rx)
        });

        wait_until(|| {
            control_plane_cache
                .read()
                .map(|cache| cache.metrics.docker.breaker.state == "open")
                .unwrap_or(false)
        });

        let started = Instant::now();
        shutdown_tx.send(()).unwrap();
        join.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn dependency_probe_timeout_does_not_hang_refresh_loop() {
        let root = test_root("dependency-probe-timeout-does-not-hang-refresh-loop");
        let mut daemon = Daemon::new(
            config_with_root(root),
            SlowTimeoutDockerRuntime {
                delay: Duration::from_millis(50),
            },
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let started = Instant::now();
        daemon.refresh_readyz_cache();
        let elapsed = started.elapsed();

        assert!(elapsed < Duration::from_millis(250));
        assert_eq!(daemon.readyz_response().status, "degraded");
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
        assert_eq!(daemon.startup_steps()[1], StartupStep::NodeIdentityLoad);
        assert_eq!(daemon.startup_steps()[2], StartupStep::LeaseRecovery);
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

    #[test]
    fn leader_takeover_after_shutdown() {
        let root = test_root("leader-takeover-after-shutdown");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();
        let first_lease = LeaderLeaseStore::new(&root).load().unwrap().unwrap();

        daemon.graceful_shutdown();

        let takeover = LeaderLeaseStore::new(&root)
            .try_acquire_or_renew("node-b", current_unix_timestamp(), LEADER_LEASE_TTL_SECONDS)
            .unwrap();
        let takeover = match takeover {
            LeaseAcquireOutcome::Leader(lease) => lease,
            LeaseAcquireOutcome::Follower(_) => panic!("expected takeover after shutdown"),
        };

        assert_eq!(takeover.leader_node_id, "node-b");
        assert_eq!(
            takeover.lease_epoch,
            first_lease.lease_epoch.saturating_add(1)
        );
    }

    #[test]
    fn lease_lock_released_on_daemon_shutdown() {
        let root = test_root("lease-lock-released-on-daemon-shutdown");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let lock_path = EnvironmentPaths::leader_lease_file(&root).with_extension("lock");
        daemon.graceful_shutdown();

        assert!(!lock_path.exists());
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
                StartupStep::StorageInit,
                StartupStep::NodeIdentityLoad,
                StartupStep::LeaseRecovery,
                StartupStep::ReplayCursorLoad,
                StartupStep::ReplayScan,
                StartupStep::ReplayExecution,
                StartupStep::LeadershipAcquisition,
                StartupStep::HeartbeatStart,
                StartupStep::ConvergenceEnable,
                StartupStep::ReadinessPublish,
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

#[cfg(test)]
mod daemon_control_plane_durability {
    use super::*;
    use crate::api::{DeploymentRequest, ProjectUpsertRequest};
    use crate::reconciliation::{ReconciliationStore, intent_request_for_storage_root};
    use crate::storage::{
        ControlPlaneSnapshotStore, ConvergenceCheckpointStore, EnvironmentPaths,
        OperationalJournalStore, PointerStore,
    };

    fn seed_project(root: &std::path::Path) {
        ProjectRegistryStore::new(root)
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some("api".into()),
                    repo_url: "https://example.com/api.git".into(),
                    default_branch: "main".into(),
                    base_domain: Some("api.example.com".into()),
                },
                None,
            )
            .unwrap();
    }

    #[allow(dead_code)]
    #[derive(Clone, Copy)]
    enum ClusterSimulation {
        DualWriterRace,
        StaleLeaderTakeover,
        NetworkPartition,
        HeartbeatFreeze,
        LeaseCorruption,
        CheckpointDivergence,
    }

    fn cluster_node(
        node_id: &str,
        role: &str,
        lease_epoch_seen: u64,
        last_seen_unix: u64,
        active_reconciler: bool,
    ) -> PersistedClusterNode {
        PersistedClusterNode {
            schema_version: 1,
            node_id: node_id.into(),
            hostname: format!("{node_id}.local"),
            advertised_addr: format!("{node_id}:8080"),
            role: role.into(),
            last_seen_unix,
            capabilities: vec!["control_plane".into()],
            lease_epoch_seen,
            control_plane_version: env!("CARGO_PKG_VERSION").into(),
            reconciliation_enabled: active_reconciler,
            active_reconciler,
        }
    }

    fn write_cluster_nodes(root: &std::path::Path, nodes: Vec<PersistedClusterNode>) {
        ClusterTopologyStore::new(root)
            .save(&crate::storage::PersistedClusterTopology {
                schema_version: 1,
                nodes,
            })
            .unwrap();
    }

    fn force_leader_lease(
        root: &std::path::Path,
        node_id: &str,
        acquired_at_unix: u64,
        ttl_seconds: u64,
        lease_epoch: u64,
    ) {
        let lease = PersistedLeaderLease {
            schema_version: 1,
            leader_node_id: node_id.into(),
            acquired_at_unix,
            expires_at_unix: acquired_at_unix.saturating_add(ttl_seconds),
            lease_epoch,
            last_heartbeat_unix: acquired_at_unix,
        };
        let path = EnvironmentPaths::leader_lease_file(root);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_vec_pretty(&lease).unwrap()).unwrap();
    }

    #[allow(dead_code)]
    fn write_runtime_snapshot(
        root: &std::path::Path,
        node_id: &str,
        convergence_owner: &str,
        lease_epoch: u64,
    ) {
        let env = EnvironmentPaths::new(root, "api", "production");
        env.ensure_exists().unwrap();
        ControlPlaneSnapshotStore::new(env)
            .append(
                &PersistedControlPlaneSnapshot {
                    snapshot_version: 1,
                    schema_version: 1,
                    snapshot_kind: "runtime_snapshot".into(),
                    project_id: "api".into(),
                    environment: "production".into(),
                    cycle_id: "test".into(),
                    created_at_unix: current_unix_timestamp(),
                    generation: Some(1),
                    node_id: node_id.into(),
                    lease_epoch,
                    convergence_owner: convergence_owner.into(),
                    payload: serde_json::json!({}),
                },
                CONTROL_PLANE_SNAPSHOT_RETENTION_LIMIT,
            )
            .unwrap();
    }

    fn apply_cluster_simulation(
        root: &std::path::Path,
        local_node: &str,
        simulation: ClusterSimulation,
    ) {
        let now = current_unix_timestamp();
        match simulation {
            ClusterSimulation::DualWriterRace => {
                force_leader_lease(root, local_node, now, LEADER_LEASE_TTL_SECONDS, 1);
                write_cluster_nodes(
                    root,
                    vec![
                        cluster_node(local_node, "leader", 1, now, true),
                        cluster_node("peer-node", "leader", 1, now, true),
                    ],
                );
            }
            ClusterSimulation::StaleLeaderTakeover => {
                force_leader_lease(root, "peer-node", now, LEADER_LEASE_TTL_SECONDS, 2);
                write_cluster_nodes(
                    root,
                    vec![
                        cluster_node(local_node, "leader", 1, now.saturating_sub(60), true),
                        cluster_node("peer-node", "leader", 2, now, true),
                    ],
                );
            }
            ClusterSimulation::NetworkPartition => {
                force_leader_lease(root, local_node, now, LEADER_LEASE_TTL_SECONDS, 3);
                write_cluster_nodes(
                    root,
                    vec![
                        cluster_node(local_node, "leader", 3, now, true),
                        cluster_node("peer-node", "follower", 4, now, false),
                    ],
                );
            }
            ClusterSimulation::HeartbeatFreeze => {
                force_leader_lease(root, "peer-node", now, LEADER_LEASE_TTL_SECONDS, 4);
                write_cluster_nodes(
                    root,
                    vec![
                        cluster_node(
                            "peer-node",
                            "leader",
                            4,
                            now.saturating_sub(HEARTBEAT_STALE_AFTER_SECONDS + 5),
                            true,
                        ),
                        cluster_node(local_node, "follower", 4, now, false),
                    ],
                );
            }
            ClusterSimulation::LeaseCorruption => {
                let path = EnvironmentPaths::leader_lease_file(root);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, "{ invalid json").unwrap();
            }
            ClusterSimulation::CheckpointDivergence => {
                let env = EnvironmentPaths::new(root, "api", "production");
                env.ensure_exists().unwrap();
                ConvergenceCheckpointStore::new(env)
                    .save(&PersistedEnvironmentCheckpoint {
                        snapshot_version: 1,
                        schema_version: 1,
                        project_id: "api".into(),
                        environment: "production".into(),
                        checkpointed_at_unix: now,
                        last_successful_convergence_unix: Some(now),
                        last_convergence_duration_ms: 1,
                        last_convergence_generation: Some(1),
                        last_convergence_error: None,
                        active_generation: Some(1),
                        health_state: RuntimeHealthState::Healthy,
                        dependency_states: BTreeMap::new(),
                        breaker_states: BTreeMap::new(),
                        queue_depth_snapshot: 0,
                        node_id: local_node.into(),
                        lease_epoch: 1,
                        convergence_owner: "peer-node".into(),
                        readyz_reasons: Vec::new(),
                        extra: BTreeMap::new(),
                    })
                    .unwrap();
            }
        }
    }

    #[test]
    fn convergence_checkpoint_survives_restart() {
        let root = test_root("convergence-checkpoint-survives-restart");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        RuntimeStateStore::new(env.clone())
            .save(&crate::storage::RuntimeState {
                active_generation: Some(1),
                health_state: RuntimeHealthState::Degraded,
                failed_probe_count: 1,
                successful_probe_count: 0,
                restart_attempted: false,
                degraded_since_unix: Some(current_unix_timestamp()),
                last_transition: "degraded".into(),
                last_error_code: Some("tcp_unreachable".into()),
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let checkpoint = ConvergenceCheckpointStore::new(env)
            .load()
            .unwrap()
            .expect("checkpoint should exist");
        assert_eq!(checkpoint.active_generation, Some(1));

        let mut restarted = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        restarted.start().unwrap();
        assert_ne!(
            restarted.readyz_cache_snapshot().response.status,
            "not_ready"
        );
    }

    #[test]
    fn readiness_cache_restores_from_checkpoint() {
        let root = test_root("readiness-cache-restores-from-checkpoint");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        ConvergenceCheckpointStore::new(env)
            .save(&PersistedEnvironmentCheckpoint {
                snapshot_version: 1,
                schema_version: 1,
                project_id: "api".into(),
                environment: "production".into(),
                checkpointed_at_unix: current_unix_timestamp(),
                last_successful_convergence_unix: Some(current_unix_timestamp()),
                last_convergence_duration_ms: 10,
                last_convergence_generation: Some(1),
                last_convergence_error: Some("tcp_unreachable".into()),
                active_generation: Some(1),
                health_state: RuntimeHealthState::Degraded,
                dependency_states: BTreeMap::new(),
                breaker_states: BTreeMap::new(),
                queue_depth_snapshot: 0,
                node_id: "node-test".into(),
                lease_epoch: 1,
                convergence_owner: "node-test".into(),
                readyz_reasons: vec!["restored from checkpoint".into()],
                extra: BTreeMap::new(),
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.restore_readyz_cache_from_checkpoints();
        let readyz = daemon.cached_readyz_response();
        assert_eq!(readyz.status, "degraded");
        assert_eq!(readyz.reasons[0].source, "convergence_checkpoint");
    }

    #[test]
    fn stale_checkpoint_convergence_stalled_does_not_survive_fresh_success() {
        let root = test_root("stale-checkpoint-convergence-stalled-does-not-survive-fresh-success");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        let stale_success = current_unix_timestamp().saturating_sub(60);
        let node_id = local_node_id(&root);
        ConvergenceCheckpointStore::new(env)
            .save(&PersistedEnvironmentCheckpoint {
                snapshot_version: 1,
                schema_version: 1,
                project_id: "api".into(),
                environment: "production".into(),
                checkpointed_at_unix: current_unix_timestamp(),
                last_successful_convergence_unix: Some(stale_success),
                last_convergence_duration_ms: 10,
                last_convergence_generation: None,
                last_convergence_error: None,
                active_generation: None,
                health_state: RuntimeHealthState::Healthy,
                dependency_states: BTreeMap::new(),
                breaker_states: BTreeMap::new(),
                queue_depth_snapshot: 0,
                node_id: node_id.clone(),
                lease_epoch: 1,
                convergence_owner: node_id,
                readyz_reasons: vec!["convergence stalled".into()],
                extra: BTreeMap::from([
                    ("readyz_status".into(), Value::String("degraded".into())),
                    (
                        READYZ_REASON_DETAILS_KEY.into(),
                        serde_json::json!([{
                            "marker": "convergence_stalled",
                            "message": "convergence stalled"
                        }]),
                    ),
                ]),
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.restore_readyz_cache_from_checkpoints();
        let restored = daemon.cached_readyz_response();
        assert_eq!(restored.status, "degraded");
        assert_eq!(restored.reason.as_deref(), Some("convergence stalled"));
        assert!(
            restored
                .reasons
                .iter()
                .any(|reason| reason.marker == "convergence_stalled")
        );
        assert_eq!(daemon.convergence_failures_total, 0);
        assert_eq!(daemon.convergence_last_success_unix, Some(stale_success));

        daemon.start().unwrap();

        let readyz = daemon.readyz_response();
        let metrics = daemon.control_plane_snapshot().metrics;
        assert_eq!(readyz.status, "ready");
        assert_eq!(readyz.reason, None);
        assert!(readyz.reasons.is_empty());
        assert_eq!(metrics.convergence_failures_total, 0);
        assert!(daemon.convergence_last_success_unix.unwrap() >= stale_success + 1);
    }

    #[test]
    fn leader_active_triggers_immediate_convergence_after_restart() {
        let root = test_root("leader-active-triggers-immediate-convergence-after-restart");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        let stale_success = current_unix_timestamp().saturating_sub(60);
        let node_id = local_node_id(&root);
        ConvergenceCheckpointStore::new(env)
            .save(&PersistedEnvironmentCheckpoint {
                snapshot_version: 1,
                schema_version: 1,
                project_id: "api".into(),
                environment: "production".into(),
                checkpointed_at_unix: current_unix_timestamp(),
                last_successful_convergence_unix: Some(stale_success),
                last_convergence_duration_ms: 10,
                last_convergence_generation: None,
                last_convergence_error: None,
                active_generation: None,
                health_state: RuntimeHealthState::Healthy,
                dependency_states: BTreeMap::new(),
                breaker_states: BTreeMap::new(),
                queue_depth_snapshot: 0,
                node_id: node_id.clone(),
                lease_epoch: 1,
                convergence_owner: node_id,
                readyz_reasons: Vec::new(),
                extra: BTreeMap::new(),
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let metrics = daemon.control_plane_snapshot().metrics;
        assert_eq!(metrics.startup_phase, "leader_active");
        assert!(metrics.leader);
        assert_eq!(daemon.readyz_response().status, "ready");
        assert!(metrics.convergence_last_success_unix.is_some());
    }

    #[test]
    fn upgrade_restart_clears_stale_convergence_stalled() {
        let root = test_root("upgrade-restart-clears-stale-convergence-stalled");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        let stale_success = current_unix_timestamp().saturating_sub(60);
        let node_id = local_node_id(&root);
        ConvergenceCheckpointStore::new(env)
            .save(&PersistedEnvironmentCheckpoint {
                snapshot_version: 1,
                schema_version: 1,
                project_id: "api".into(),
                environment: "production".into(),
                checkpointed_at_unix: stale_success,
                last_successful_convergence_unix: Some(stale_success),
                last_convergence_duration_ms: 10,
                last_convergence_generation: Some(1),
                last_convergence_error: Some("tcp_unreachable".into()),
                active_generation: Some(1),
                health_state: RuntimeHealthState::Healthy,
                dependency_states: BTreeMap::new(),
                breaker_states: BTreeMap::new(),
                queue_depth_snapshot: 0,
                node_id: node_id.clone(),
                lease_epoch: 1,
                convergence_owner: node_id,
                readyz_reasons: vec!["convergence stalled".into()],
                extra: BTreeMap::from([
                    ("readyz_status".into(), Value::String("degraded".into())),
                    (
                        READYZ_REASON_DETAILS_KEY.into(),
                        serde_json::json!([{
                            "marker": "convergence_stalled",
                            "message": "convergence stalled"
                        }]),
                    ),
                ]),
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let readyz = daemon.readyz_response();
        let metrics = daemon.control_plane_snapshot().metrics;
        assert_eq!(readyz.status, "ready");
        assert_eq!(readyz.reason, None);
        assert!(
            !readyz
                .reasons
                .iter()
                .any(|reason| reason.marker == "convergence_stalled")
        );
        assert_eq!(metrics.startup_phase, "leader_active");
        assert_eq!(metrics.convergence_failures_total, 0);
        assert!(metrics.convergence_last_success_unix.unwrap() >= stale_success + 1);
    }

    #[test]
    fn stale_checkpoint_degrades_until_refresh() {
        let root = test_root("stale-checkpoint-degrades-until-refresh");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        ConvergenceCheckpointStore::new(env)
            .save(&PersistedEnvironmentCheckpoint {
                snapshot_version: 1,
                schema_version: 1,
                project_id: "api".into(),
                environment: "production".into(),
                checkpointed_at_unix: current_unix_timestamp().saturating_sub(60),
                last_successful_convergence_unix: Some(current_unix_timestamp().saturating_sub(60)),
                last_convergence_duration_ms: 10,
                last_convergence_generation: Some(1),
                last_convergence_error: None,
                active_generation: Some(1),
                health_state: RuntimeHealthState::Healthy,
                dependency_states: BTreeMap::new(),
                breaker_states: BTreeMap::new(),
                queue_depth_snapshot: 0,
                node_id: "node-test".into(),
                lease_epoch: 1,
                convergence_owner: "node-test".into(),
                readyz_reasons: Vec::new(),
                extra: BTreeMap::new(),
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.restore_readyz_cache_from_checkpoints();
        let readyz = daemon.cached_readyz_response();
        assert_eq!(readyz.status, "degraded");
        assert_eq!(readyz.reasons[0].marker, "stale_checkpoint");
    }

    #[test]
    fn corrupted_checkpoint_fails_gracefully() {
        let root = test_root("corrupted-checkpoint-fails-gracefully");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        std::fs::write(env.checkpoint_file(), "{ invalid json").unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.restore_readyz_cache_from_checkpoints();
        assert!(daemon.readyz_cache_snapshot().response.reasons.is_empty());
    }

    #[test]
    fn checkpoint_corruption_does_not_block_daemon_startup() {
        let root = test_root("checkpoint-corruption-does-not-block-daemon-startup");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        std::fs::write(env.checkpoint_file(), "{ invalid json").unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        assert_eq!(daemon.state(), &DaemonState::Ready);
        assert!(daemon.readyz_cache_snapshot().updated_at_unix_ms > 0);
    }

    #[test]
    fn runtime_snapshot_written_after_convergence() {
        let root = test_root("runtime-snapshot-written-after-convergence");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        RuntimeStateStore::new(env.clone())
            .save(&crate::storage::RuntimeState {
                active_generation: Some(1),
                health_state: RuntimeHealthState::Healthy,
                ..crate::storage::RuntimeState::default()
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let latest = ControlPlaneSnapshotStore::new(env)
            .latest_by_kind("runtime_snapshot")
            .unwrap()
            .expect("runtime snapshot should exist");
        assert_eq!(latest.snapshot_kind, "runtime_snapshot");
    }

    #[test]
    fn corrupted_snapshot_rebuilds_cleanly() {
        let root = test_root("corrupted-snapshot-rebuilds-cleanly");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        std::fs::create_dir_all(env.control_plane_snapshots_dir()).unwrap();
        std::fs::write(
            env.control_plane_snapshots_dir()
                .join("1-runtime_snapshot.json"),
            "{ invalid json",
        )
        .unwrap();
        RuntimeStateStore::new(env.clone())
            .save(&crate::storage::RuntimeState {
                active_generation: Some(7),
                health_state: RuntimeHealthState::Healthy,
                ..crate::storage::RuntimeState::default()
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let latest = ControlPlaneSnapshotStore::new(env)
            .latest_by_kind("runtime_snapshot")
            .unwrap()
            .expect("runtime snapshot should be rebuilt");
        assert_eq!(latest.generation, Some(7));
    }

    #[test]
    fn breaker_transition_written_to_journal() {
        let root = test_root("breaker-transition-written-to-journal");
        seed_project(&root);
        let fail_probe = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            SwitchableDockerRuntime { fail_probe },
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        for _ in 0..CIRCUIT_BREAKER_FAILURE_THRESHOLD {
            daemon.refresh_readyz_cache();
        }
        let entries = OperationalJournalStore::new(root).read_all().unwrap();
        assert!(
            entries
                .iter()
                .any(|entry| entry.event_type == "breaker_transition")
        );
    }

    #[test]
    fn malformed_journal_entry_skipped() {
        let root = test_root("malformed-journal-entry-skipped");
        let path = EnvironmentPaths::operational_journal_file(&root);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            concat!(
                "{\"schema_version\":1,\"timestamp_unix\":1,\"event_type\":\"ok\",\"payload\":{}}\n",
                "{ invalid json\n"
            ),
        )
        .unwrap();

        let entries = OperationalJournalStore::new(root).read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event_type, "ok");
    }

    #[test]
    fn deployment_written_to_journal() {
        let root = test_root("deployment-written-to-journal");
        seed_project(&root);
        let source = root.join("app");
        std::fs::create_dir_all(&source).unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon
            .handle_post_deployments(DeploymentRequest {
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source),
                source_ref: None,
            })
            .unwrap();

        let entries = OperationalJournalStore::new(root).read_all().unwrap();
        assert!(entries.iter().any(|entry| entry.event_type == "deployment"));
    }

    #[test]
    fn journal_rotation_preserves_recent_entries() {
        let root = test_root("journal-rotation-preserves-recent-entries");
        let journal = OperationalJournalStore::new(&root);
        for index in 0..2000 {
            journal
                .append(&OperationalJournalEntry {
                    schema_version: 1,
                    timestamp_unix: current_unix_timestamp(),
                    event_type: "gc_action".into(),
                    project_id: None,
                    environment: None,
                    generation: None,
                    payload: serde_json::json!({
                        "index": index,
                        "padding": "x".repeat(256),
                    }),
                })
                .unwrap();
        }
        let entries = journal.read_all().unwrap();
        assert!(entries.iter().any(|entry| entry.payload["index"] == 1999));
    }

    #[test]
    fn journal_write_failure_does_not_abort_convergence() {
        let root = test_root("journal-write-failure-does-not-abort-convergence");
        seed_project(&root);
        std::fs::create_dir_all(EnvironmentPaths::operational_journal_file(&root)).unwrap();
        let env = EnvironmentPaths::new(&root, "api", "production");
        RuntimeStateStore::new(env.clone())
            .save(&crate::storage::RuntimeState {
                active_generation: Some(1),
                health_state: RuntimeHealthState::Degraded,
                ..crate::storage::RuntimeState::default()
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let checkpoint = ConvergenceCheckpointStore::new(env).load().unwrap();
        assert!(checkpoint.is_some());
        assert_eq!(daemon.state(), &DaemonState::Ready);
    }

    #[test]
    fn journal_write_failure_degrades_observability_not_daemon() {
        journal_write_failure_does_not_abort_convergence();
    }

    #[test]
    fn failed_caddy_domain_does_not_block_metrics_refresh() {
        let root = test_root("failed-caddy-domain-does-not-block-metrics-refresh");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            UnavailableRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let domains = daemon.control_plane_snapshot().metrics.convergence_domains;
        assert!(domains.iter().any(|domain| {
            domain.domain == "routing_reconciliation" && domain.status == "degraded"
        }));
        assert!(
            domains
                .iter()
                .any(|domain| { domain.domain == "metrics_refresh" && domain.status == "healthy" })
        );
    }

    #[test]
    fn convergence_domains_run_independently() {
        let root = test_root("convergence-domains-run-independently");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        RuntimeStateStore::new(env)
            .save(&crate::storage::RuntimeState {
                active_generation: Some(2),
                health_state: RuntimeHealthState::Degraded,
                last_error_code: Some("tcp_unreachable".into()),
                ..crate::storage::RuntimeState::default()
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let domains = daemon.control_plane_snapshot().metrics.convergence_domains;
        assert!(domains.iter().any(|domain| {
            domain.domain == "runtime_container_reconciliation" && domain.status == "degraded"
        }));
        assert!(domains.iter().any(|domain| {
            domain.domain == "routing_reconciliation" && domain.status == "healthy"
        }));
    }

    #[test]
    fn domain_failure_recorded_without_aborting_convergence() {
        let root = test_root("domain-failure-recorded-without-aborting-convergence");
        let fail_probe = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut daemon = Daemon::new(
            config_with_root(root),
            SwitchableDockerRuntime { fail_probe },
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        assert_eq!(daemon.state(), &DaemonState::Ready);
        assert!(
            daemon
                .readyz_cache_snapshot()
                .response
                .reasons
                .iter()
                .any(|reason| reason.project_id == "_control_plane")
        );
    }

    #[test]
    fn domain_metrics_are_persisted_to_checkpoint() {
        let root = test_root("domain-metrics-are-persisted-to-checkpoint");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        RuntimeStateStore::new(env.clone())
            .save(&crate::storage::RuntimeState {
                active_generation: Some(3),
                health_state: RuntimeHealthState::Healthy,
                ..crate::storage::RuntimeState::default()
            })
            .unwrap();

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let checkpoint = ConvergenceCheckpointStore::new(env)
            .load()
            .unwrap()
            .expect("checkpoint should exist");
        let domains = checkpoint.extra["convergence_domains"]
            .as_array()
            .expect("domain summaries should be persisted");
        assert!(!domains.is_empty());
    }

    #[test]
    fn daemon_survives_missing_docker() {
        let root = test_root("daemon-survives-missing-docker");
        let fail_probe = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut daemon = Daemon::new(
            config_with_root(root),
            SwitchableDockerRuntime { fail_probe },
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        assert_eq!(daemon.state(), &DaemonState::Ready);
        assert_eq!(daemon.readyz_cache_snapshot().response.status, "degraded");
    }

    #[test]
    fn daemon_survives_caddy_outage() {
        let root = test_root("daemon-survives-caddy-outage");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            UnavailableRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        assert_eq!(daemon.state(), &DaemonState::Ready);
        assert_eq!(daemon.readyz_cache_snapshot().response.status, "degraded");
    }

    #[test]
    fn readyz_degrades_when_lease_uncertain() {
        let root = test_root("readyz-degrades-when-lease-uncertain");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        let lock_path = EnvironmentPaths::leader_lease_file(&root).with_extension("lock");
        fs::create_dir_all(&lock_path).unwrap();
        daemon.refresh_readyz_cache();
        let readyz = daemon.readyz_response();
        assert_eq!(readyz.status, "degraded");
        assert!(
            readyz
                .reasons
                .iter()
                .any(|reason| reason.marker == "leadership_uncertain")
        );
    }

    #[test]
    fn stale_leader_lock_file_does_not_degrade_readyz() {
        let root = test_root("stale-leader-lock-file-does-not-degrade-readyz");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        let lock_path = EnvironmentPaths::leader_lease_file(&root).with_extension("lock");
        fs::write(
            &lock_path,
            format!(
                "{{\"pid\":{},\"acquired_at_unix\":1}}\n",
                u32::MAX.saturating_sub(1)
            ),
        )
        .unwrap();

        daemon.refresh_readyz_cache();

        let readyz = daemon.readyz_response();
        let metrics = daemon.control_plane_snapshot().metrics;
        assert_eq!(readyz.status, "ready");
        assert!(readyz.reasons.is_empty());
        assert!(metrics.leader);
        assert!(!metrics.follower_mode);
        assert!(metrics.lease_epoch >= 1);
    }

    #[test]
    fn checkpoint_epoch_mismatch_detected() {
        let root = test_root("checkpoint-epoch-mismatch-detected");
        seed_project(&root);
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        let local_node = daemon.node_id().to_string();
        ConvergenceCheckpointStore::new(env)
            .save(&PersistedEnvironmentCheckpoint {
                snapshot_version: 1,
                schema_version: 1,
                project_id: "api".into(),
                environment: "production".into(),
                checkpointed_at_unix: current_unix_timestamp(),
                last_successful_convergence_unix: Some(current_unix_timestamp()),
                last_convergence_duration_ms: 1,
                last_convergence_generation: None,
                last_convergence_error: None,
                active_generation: None,
                health_state: RuntimeHealthState::Healthy,
                dependency_states: BTreeMap::new(),
                breaker_states: BTreeMap::new(),
                queue_depth_snapshot: 0,
                node_id: local_node.clone(),
                lease_epoch: 0,
                convergence_owner: "stale-node".into(),
                readyz_reasons: Vec::new(),
                extra: BTreeMap::new(),
            })
            .unwrap();
        daemon.refresh_readyz_cache();

        assert!(daemon.readyz_response().reasons.iter().any(|reason| {
            reason.marker == "checkpoint_epoch_mismatch" && reason.project_id == "api"
        }));
    }

    #[test]
    fn metrics_expose_leader_state() {
        let root = test_root("metrics-expose-leader-state");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        let local_node = daemon.node_id().to_string();
        write_leader_lease(
            &root,
            &local_node,
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
        );
        daemon.refresh_readyz_cache();

        let metrics = daemon.control_plane_snapshot().metrics;
        assert!(metrics.leader);
        assert_eq!(metrics.convergence_owner, local_node);
        assert!(metrics.reconciliation_enabled);
        assert!(!metrics.follower_mode);
        assert!(metrics.lease_epoch >= 1);
        assert_eq!(metrics.startup_phase, "leader_active");
        assert_eq!(metrics.readiness_status, "ready");
        assert!(!metrics.convergence_active_failure);
        assert!(!metrics.convergence_start_blocked);
    }

    #[test]
    fn metrics_keep_historical_failures_separate_from_active_readiness() {
        let root = test_root("metrics-keep-historical-failures-separate-from-active-readiness");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.convergence_last_failure_unix = Some(current_unix_timestamp().saturating_sub(10));
        daemon.convergence_failures_total = 3;
        daemon.refresh_readyz_cache();

        let readyz = daemon.readyz_response();
        let metrics = daemon.control_plane_snapshot().metrics;
        assert_eq!(readyz.status, "ready");
        assert!(!readyz.active_failure);
        assert_eq!(metrics.readiness_status, "ready");
        assert_eq!(metrics.readiness_reason, None);
        assert!(!metrics.convergence_active_failure);
        assert_eq!(metrics.convergence_active_failure_reason, None);
        assert_eq!(
            metrics.convergence_last_failure_historical_unix,
            metrics.convergence_last_failure_unix
        );
        assert_eq!(metrics.convergence_failures_total, 3);
    }

    #[test]
    fn ready_without_active_failure_never_reports_degraded_phase() {
        let root = test_root("ready-without-active-failure-never-reports-degraded-phase");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.startup_phase = StartupPhase::Degraded;
        let local_node = daemon.node_id().to_string();
        write_leader_lease(
            &root,
            &local_node,
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
        );
        daemon.refresh_readyz_cache();

        let readyz = daemon.readyz_response();
        let metrics = daemon.control_plane_snapshot().metrics;
        assert_eq!(readyz.status, "ready");
        assert!(!readyz.active_failure);
        assert_eq!(readyz.startup_phase, "leader_active");
        assert_eq!(metrics.readiness_status, "ready");
        assert!(!metrics.convergence_active_failure);
        assert_eq!(metrics.startup_phase, "leader_active");
    }

    #[test]
    fn metrics_report_active_failure_when_readyz_degraded() {
        let root = test_root("metrics-report-active-failure-when-readyz-degraded");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        let lock_path = EnvironmentPaths::leader_lease_file(&root).with_extension("lock");
        fs::create_dir_all(&lock_path).unwrap();
        daemon.refresh_readyz_cache();

        let readyz = daemon.readyz_response();
        let metrics = daemon.control_plane_snapshot().metrics;
        assert_eq!(readyz.status, "degraded");
        assert!(readyz.active_failure);
        assert_eq!(metrics.readiness_status, "degraded");
        assert!(metrics.convergence_active_failure);
        assert!(metrics.readiness_reason.is_some());
        assert_eq!(
            metrics.convergence_active_failure_reason,
            metrics.readiness_reason
        );
    }

    #[test]
    fn leadership_heartbeat_refreshes_lease() {
        let root = test_root("leadership-heartbeat-refreshes-lease");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();
        let first = LeaderLeaseStore::new(&root).load().unwrap().unwrap();
        std::thread::sleep(Duration::from_secs(1));
        daemon.refresh_readyz_cache();
        let second = LeaderLeaseStore::new(&root).load().unwrap().unwrap();

        assert_eq!(first.leader_node_id, second.leader_node_id);
        assert!(second.last_heartbeat_unix > first.last_heartbeat_unix);
        assert_eq!(second.lease_epoch, first.lease_epoch);
    }

    #[test]
    fn convergence_disabled_when_not_leader() {
        let root = test_root("convergence-disabled-when-not-leader");
        let local_node = local_node_id(&root);
        write_leader_lease(
            &root,
            "other-node",
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
        );
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

        let did_work = execute_worker_iteration(
            root.clone(),
            &queue,
            &mut NoopDockerRuntime,
            &mut StaticProbeRuntime {
                tcp_ok: true,
                http_ok: true,
            },
            &mut NoopRoutingRuntime,
            &DeploymentWorkerSettings::default(),
            &WorkerLeadership {
                node_id: local_node,
            },
        )
        .unwrap();

        assert!(!did_work);
        assert_eq!(queue.queued_len().unwrap(), 1);
    }

    #[test]
    fn stale_in_memory_convergence_block_does_not_survive_refresh() {
        let root = test_root("stale-in-memory-convergence-block-does-not-survive-refresh");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.convergence_start_blocked = true;

        daemon.refresh_readyz_cache();

        let readiness = daemon.readyz_response();
        let metrics = daemon.control_plane_snapshot().metrics;
        assert!(metrics.leader);
        assert_eq!(readiness.status, "ready");
        assert!(
            !readiness
                .reasons
                .iter()
                .any(|reason| reason.marker == "convergence_disabled")
        );
        assert!(
            !readiness
                .reasons
                .iter()
                .any(|reason| reason.marker == "convergence_stalled")
        );
    }

    #[test]
    fn follower_node_does_not_reconcile() {
        let root = test_root("follower-node-does-not-reconcile");
        let local_node = local_node_id(&root);
        write_leader_lease(
            &root,
            "leader-node",
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
        );
        assert!(!worker_reconciliation_enabled(
            &root,
            &WorkerLeadership {
                node_id: local_node,
            }
        ));
    }

    #[test]
    fn deploy_rejected_on_follower() {
        let root = test_root("deploy-rejected-on-follower");
        seed_project(&root);
        let source = root.join("app");
        std::fs::create_dir_all(&source).unwrap();
        write_leader_lease(
            &root,
            "leader-node",
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
        );

        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let err = daemon
            .handle_post_deployments(DeploymentRequest {
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(source),
                source_ref: None,
            })
            .unwrap_err();
        assert_eq!(err.code, "not_leader");
    }

    #[test]
    fn backup_restore_rejected_on_follower() {
        let root = test_root("backup-restore-rejected-on-follower");
        write_leader_lease(
            &root,
            "leader-node",
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
        );
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let err = daemon.restore_backup("backup-1").unwrap_err();
        assert_eq!(err.code, "not_leader");
    }

    #[test]
    fn split_brain_detection_triggers_degraded_readyz() {
        let root = test_root("split-brain-detection-triggers-degraded-readyz");
        seed_project(&root);
        let local_node = local_node_id(&root);
        apply_cluster_simulation(&root, &local_node, ClusterSimulation::DualWriterRace);
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let readyz = daemon.readyz_response();
        assert_eq!(readyz.status, "degraded");
        assert!(
            readyz
                .reasons
                .iter()
                .any(|reason| reason.marker == "split_brain_suspected")
        );
        assert!(
            readyz
                .reasons
                .iter()
                .any(|reason| reason.marker == "multiple_active_reconcilers")
        );
    }

    #[test]
    fn follower_never_reconciles_after_leader_loss() {
        let root = test_root("follower-never-reconciles-after-leader-loss");
        let local_node = local_node_id(&root);
        write_leader_lease(
            &root,
            &local_node,
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
        );
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
        force_leader_lease(
            &root,
            "peer-node",
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
            2,
        );

        let did_work = execute_worker_iteration(
            root.clone(),
            &queue,
            &mut NoopDockerRuntime,
            &mut StaticProbeRuntime {
                tcp_ok: true,
                http_ok: true,
            },
            &mut NoopRoutingRuntime,
            &DeploymentWorkerSettings::default(),
            &WorkerLeadership {
                node_id: local_node,
            },
        )
        .unwrap();

        assert!(!did_work);
        assert_eq!(queue.queued_len().unwrap(), 1);
    }

    #[test]
    fn heartbeat_updates_without_reconciliation() {
        let root = test_root("heartbeat-updates-without-reconciliation");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        force_leader_lease(
            &root,
            "peer-node",
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
            1,
        );
        daemon.heartbeat_tick();
        let first = ClusterTopologyStore::new(&root)
            .load()
            .unwrap()
            .nodes
            .into_iter()
            .find(|node| node.node_id == daemon.node_id())
            .unwrap();
        std::thread::sleep(Duration::from_secs(1));
        daemon.heartbeat_tick();
        let second = ClusterTopologyStore::new(&root)
            .load()
            .unwrap()
            .nodes
            .into_iter()
            .find(|node| node.node_id == daemon.node_id())
            .unwrap();

        assert_eq!(first.role, "follower");
        assert!(second.last_seen_unix > first.last_seen_unix);
        assert!(!second.active_reconciler);
    }

    #[test]
    fn heartbeat_promotes_leader_acquiring_to_leader_active() {
        let root = test_root("heartbeat-promotes-leader-acquiring-to-leader-active");
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.startup_phase = StartupPhase::LeaderAcquiring;
        daemon.convergence_start_blocked = true;

        daemon.heartbeat_tick();

        assert_eq!(daemon.startup_phase(), StartupPhase::LeaderActive);
        assert!(!daemon.convergence_start_blocked);
    }

    #[test]
    fn checkpoint_owner_mismatch_detected() {
        let root = test_root("checkpoint-owner-mismatch-detected");
        seed_project(&root);
        let local_node = local_node_id(&root);
        apply_cluster_simulation(&root, &local_node, ClusterSimulation::CheckpointDivergence);
        force_leader_lease(
            &root,
            "peer-node",
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
            1,
        );
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        assert!(
            daemon
                .readyz_response()
                .reasons
                .iter()
                .any(|reason| reason.marker == "checkpoint_owner_mismatch")
        );
    }

    #[test]
    fn snapshot_owner_mismatch_detected() {
        let root = test_root("snapshot-owner-mismatch-detected");
        seed_project(&root);
        let local_node = local_node_id(&root);
        force_leader_lease(
            &root,
            "peer-node",
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
            1,
        );
        write_runtime_snapshot(&root, &local_node, "peer-node", 1);
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        assert!(
            daemon
                .readyz_response()
                .reasons
                .iter()
                .any(|reason| reason.marker == "snapshot_owner_mismatch")
        );
    }

    #[test]
    fn divergent_epoch_observation_detected() {
        let root = test_root("divergent-epoch-observation-detected");
        seed_project(&root);
        let local_node = local_node_id(&root);
        apply_cluster_simulation(&root, &local_node, ClusterSimulation::NetworkPartition);
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        assert!(
            daemon
                .control_plane_snapshot()
                .metrics
                .cluster
                .lease_epoch_divergence
        );
        assert!(
            daemon
                .readyz_response()
                .reasons
                .iter()
                .any(|reason| reason.marker == "lease_epoch_divergence")
        );
    }

    #[test]
    fn stale_reconciler_marked_degraded() {
        let root = test_root("stale-reconciler-marked-degraded");
        seed_project(&root);
        let local_node = local_node_id(&root);
        apply_cluster_simulation(&root, &local_node, ClusterSimulation::HeartbeatFreeze);
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        assert!(
            daemon
                .control_plane_snapshot()
                .metrics
                .cluster
                .stale_reconciler
        );
        assert!(
            daemon
                .readyz_response()
                .reasons
                .iter()
                .any(|reason| reason.marker == "stale_reconciler")
        );
    }

    #[test]
    fn diagnostics_expose_cluster_state() {
        let root = test_root("diagnostics-expose-cluster-state");
        seed_project(&root);
        let local_node = local_node_id(&root);
        apply_cluster_simulation(&root, &local_node, ClusterSimulation::DualWriterRace);
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let diagnostics = daemon
            .get_project_environment_diagnostics("api", "production")
            .unwrap();
        assert!(diagnostics.cluster.split_brain_suspected);
        assert_eq!(diagnostics.cluster.active_reconcilers, 2);
        assert_eq!(diagnostics.cluster.cluster_size, 2);
    }

    #[test]
    fn request_paths_remain_constant_time_under_cluster_divergence() {
        let root = test_root("request-paths-remain-constant-time-under-cluster-divergence");
        seed_project(&root);
        let local_node = local_node_id(&root);
        apply_cluster_simulation(&root, &local_node, ClusterSimulation::DualWriterRace);
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let started = Instant::now();
        for _ in 0..256 {
            let _ = daemon.cached_readyz_response();
            let _ = daemon.control_plane_snapshot();
        }
        assert!(started.elapsed() < Duration::from_millis(250));
    }

    #[test]
    fn journal_records_leadership_transitions() {
        let root = test_root("journal-records-leadership-transitions");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.heartbeat_tick();
        force_leader_lease(
            &root,
            "peer-node",
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
            2,
        );
        daemon.heartbeat_tick();

        let entries = OperationalJournalStore::new(&root).read_all().unwrap();
        assert!(
            entries
                .iter()
                .any(|entry| entry.event_type == "leader_acquired")
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.event_type == "leader_lost")
        );
    }

    #[test]
    fn only_single_reconciler_allowed_per_epoch() {
        let root = test_root("only-single-reconciler-allowed-per-epoch");
        seed_project(&root);
        let local_node = local_node_id(&root);
        apply_cluster_simulation(&root, &local_node, ClusterSimulation::DualWriterRace);
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();

        let cluster = daemon.control_plane_snapshot().metrics.cluster;
        assert_eq!(cluster.active_reconcilers, 2);
        assert!(cluster.multiple_active_reconcilers);
        assert!(cluster.split_brain_suspected);
    }

    #[test]
    fn reconciliation_log_corruption_degrades_readyz() {
        let root = test_root("reconciliation-log-corruption-degrades-readyz");
        let path = EnvironmentPaths::reconciliation_log_file(&root);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "{not-json\n").unwrap();
        let mut daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        daemon.refresh_readyz_cache();
        let readyz = daemon.readyz_response();
        assert_eq!(readyz.status, "degraded");
        assert!(
            readyz
                .reasons
                .iter()
                .any(|reason| reason.marker == "reconciliation_log_corrupted")
        );
    }

    #[test]
    fn replay_metrics_exposed() {
        let root = test_root("replay-metrics-exposed");
        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        ReconciliationStore::new(&root)
            .append_pending(intent_request_for_storage_root(
                &root,
                "gc_action",
                "api",
                "production",
                Some(3),
                "deleted",
                "retention_reconciliation",
                BTreeMap::new(),
            ))
            .unwrap();
        daemon.refresh_readyz_cache();
        let metrics = daemon.control_plane_snapshot().metrics;
        assert!(metrics.pending_intents >= 1);
        assert!(metrics.replay_queue_depth >= 1);
        assert!(metrics.reconciliation_log_size_bytes > 0);
    }

    fn seed_pending_promotion_intent(
        root: &std::path::Path,
        node_id: &str,
        lease_epoch: u64,
        generation: u64,
    ) {
        use crate::storage::SnapshotWriter;

        let env = EnvironmentPaths::new(root, "api", "production");
        SnapshotWriter::new(env.clone(), generation)
            .unwrap()
            .finalize("api", "production", crate::storage::SnapshotState::Healthy)
            .unwrap();
        ReconciliationStore::new(root)
            .append_pending(crate::reconciliation::ReconciliationIntentRequest {
                node_id: node_id.into(),
                lease_epoch,
                operation_type: "deployment_promotion".into(),
                project_id: "api".into(),
                environment: "production".into(),
                target_generation: Some(generation),
                target_state: "healthy".into(),
                reconciliation_domain: "runtime_container_reconciliation".into(),
                triggered_by: Some("startup".into()),
                previous_intent_id: None,
                recovery_of: None,
                replay_safety: crate::reconciliation::ReplaySafety::Idempotent,
                metadata: BTreeMap::from([(
                    "storage_root".into(),
                    Value::String(root.display().to_string()),
                )]),
            })
            .unwrap();
    }

    #[test]
    fn single_node_always_becomes_leader_after_restart() {
        let root = test_root("single-node-always-becomes-leader-after-restart");
        for _ in 0..5 {
            let mut daemon = Daemon::new(
                config_with_root(root.clone()),
                NoopDockerRuntime,
                NoopRoutingRuntime,
                StaticDecider(true),
            );
            daemon.start().unwrap();
            daemon.refresh_readyz_cache();
            let metrics = daemon.control_plane_snapshot().metrics;
            assert!(metrics.leader);
            assert_eq!(metrics.startup_phase, "leader_active");
            assert!(!metrics.convergence_start_blocked);
        }
    }

    #[test]
    fn stale_follower_never_replays() {
        let root = test_root("stale-follower-never-replays");
        let local_node = local_node_id(&root);
        force_leader_lease(
            &root,
            "peer-node",
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
            9,
        );
        seed_pending_promotion_intent(&root, "peer-node", 9, 3);

        let mut daemon = Daemon::new(
            config_with_root(root.clone()),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        let metrics = daemon.control_plane_snapshot().metrics;
        assert_eq!(daemon.startup_phase(), StartupPhase::Follower);
        assert!(!metrics.leader);
        assert!(metrics.follower_mode);
        assert_eq!(
            PointerStore::new(EnvironmentPaths::new(&root, "api", "production"))
                .read_authoritative_pointer()
                .unwrap(),
            None
        );
        assert_ne!(local_node, "peer-node");
    }

    #[test]
    fn convergence_never_starts_before_replay_complete() {
        let root = test_root("convergence-never-starts-before-replay-complete");
        let mut config = config_with_root(root.clone());
        config.startup_replay_max_duration_ms = 0;
        let node_id = local_node_id(&root);
        force_leader_lease(
            &root,
            &node_id,
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
            4,
        );
        seed_pending_promotion_intent(&root, &node_id, 4, 2);

        let mut daemon = Daemon::new(
            config,
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();

        assert!(daemon.last_recovery_outcome().is_none());
        let metrics = daemon.control_plane_snapshot().metrics;
        assert!(metrics.replay_paused);
        assert!(metrics.convergence_start_blocked);
        let readyz = daemon.readyz_response();
        assert!(
            readyz
                .reasons
                .iter()
                .any(|reason| reason.marker == "replay_incomplete")
        );
        assert!(
            !readyz
                .reasons
                .iter()
                .any(|reason| reason.marker == "convergence_stalled")
        );
    }

    #[test]
    fn replay_complete_unblocks_convergence_tick() {
        let root = test_root("replay-complete-unblocks-convergence-tick");
        let mut config = config_with_root(root.clone());
        config.startup_replay_max_duration_ms = 0;
        let node_id = local_node_id(&root);
        force_leader_lease(
            &root,
            &node_id,
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
            5,
        );
        seed_pending_promotion_intent(&root, &node_id, 5, 2);

        let mut blocked = Daemon::new(
            config,
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        blocked.start().unwrap();
        assert!(
            blocked
                .readyz_response()
                .reasons
                .iter()
                .any(|reason| reason.marker == "replay_incomplete")
        );

        std::fs::write(EnvironmentPaths::reconciliation_log_file(&root), "").unwrap();
        let cursor = EnvironmentPaths::reconciliation_cursor_file(&root);
        if cursor.exists() {
            std::fs::remove_file(cursor).unwrap();
        }

        let mut restarted = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        restarted.start().unwrap();
        let readyz = restarted.readyz_response();
        assert_eq!(readyz.status, "ready");
        assert!(restarted.convergence_last_success_unix.is_some());
    }

    #[test]
    fn startup_state_machine_transitions_valid() {
        assert!(StartupPhase::Booting.can_transition_to(StartupPhase::Replaying));
        assert!(StartupPhase::Replaying.can_transition_to(StartupPhase::LeaderAcquiring));
        assert!(StartupPhase::LeaderAcquiring.can_transition_to(StartupPhase::Follower));
        assert!(StartupPhase::LeaderAcquiring.can_transition_to(StartupPhase::LeaderActive));
        assert!(!StartupPhase::LeaderActive.can_transition_to(StartupPhase::Replaying));
    }

    #[test]
    fn readiness_not_published_before_stable() {
        let root = test_root("readiness-not-published-before-stable");
        let daemon = Daemon::new(
            config_with_root(root),
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        let readyz = daemon.readyz_cache_snapshot().response;
        assert_eq!(readyz.status, "not_ready");
        assert_eq!(readyz.startup_phase, "booting");
    }

    #[test]
    fn replay_timeout_degrades_without_blocking_requests() {
        let root = test_root("replay-timeout-degrades-without-blocking-requests");
        let mut config = config_with_root(root.clone());
        config.startup_replay_max_duration_ms = 0;
        let node_id = local_node_id(&root);
        force_leader_lease(
            &root,
            &node_id,
            current_unix_timestamp(),
            LEADER_LEASE_TTL_SECONDS,
            7,
        );
        seed_pending_promotion_intent(&root, &node_id, 7, 5);

        let mut daemon = Daemon::new(
            config,
            NoopDockerRuntime,
            NoopRoutingRuntime,
            StaticDecider(true),
        );
        daemon.start().unwrap();
        let started = Instant::now();
        let readyz = daemon.readyz_response();
        let metrics = daemon.control_plane_snapshot().metrics;
        assert!(started.elapsed() < Duration::from_millis(250));
        assert_eq!(readyz.status, "degraded");
        assert_eq!(readyz.startup_phase, "degraded");
        assert!(metrics.replay_paused);
    }

    #[test]
    fn concurrent_restart_single_leader() {
        let root = test_root("concurrent-restart-single-leader");
        let now = current_unix_timestamp();
        let handles = ["node-a", "node-b"]
            .into_iter()
            .map(|node_id| {
                let root = root.clone();
                std::thread::spawn(move || {
                    LeaderLeaseStore::new(&root).try_acquire_or_renew(
                        node_id,
                        now,
                        LEADER_LEASE_TTL_SECONDS,
                    )
                })
            })
            .collect::<Vec<_>>();
        let leaders = handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap().ok())
            .filter(|outcome| matches!(outcome, LeaseAcquireOutcome::Leader(_)))
            .count();
        assert_eq!(leaders, 1);
    }

    #[test]
    fn repeated_restart_no_split_brain() {
        let root = test_root("repeated-restart-no-split-brain");
        for round in 0..25 {
            let now = current_unix_timestamp().saturating_add(round);
            let _ = LeaderLeaseStore::new(&root).release_if_owner("node-a", now);
            let _ = LeaderLeaseStore::new(&root).release_if_owner("node-b", now);
            let first = LeaderLeaseStore::new(&root)
                .try_acquire_or_renew("node-a", now, LEADER_LEASE_TTL_SECONDS)
                .unwrap();
            let second = LeaderLeaseStore::new(&root)
                .try_acquire_or_renew("node-b", now, LEADER_LEASE_TTL_SECONDS)
                .unwrap();
            let leaders = [first, second]
                .into_iter()
                .filter(|outcome| matches!(outcome, LeaseAcquireOutcome::Leader(_)))
                .count();
            assert_eq!(leaders, 1);
        }
    }

    #[test]
    fn control_plane_restart_stress() {
        let root = test_root("control-plane-restart-stress");
        let node_id = local_node_id(&root);
        for cycle in 0..25 {
            force_leader_lease(
                &root,
                &node_id,
                current_unix_timestamp().saturating_add(cycle),
                LEADER_LEASE_TTL_SECONDS,
                cycle.saturating_add(1),
            );
            seed_pending_promotion_intent(&root, &node_id, cycle.saturating_add(1), cycle + 1);
            let mut config = config_with_root(root.clone());
            config.startup_replay_max_entries = 1;
            let mut daemon = Daemon::new(
                config,
                NoopDockerRuntime,
                NoopRoutingRuntime,
                StaticDecider(true),
            );
            daemon.start().unwrap();
            daemon.refresh_readyz_cache();
            let metrics = daemon.control_plane_snapshot().metrics;
            assert!(metrics.readiness_cache_age_ms <= READYZ_CACHE_STALE_AFTER_MS);
            assert!(metrics.replay_queue_depth <= 256);
        }
    }
}
