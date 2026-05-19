use std::fmt::{Display, Formatter};
use std::fs;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::queue::{DeploymentRecord, PersistentQueue};
#[cfg(test)]
use crate::runtime::RouteInspection;
use crate::runtime::{
    ContainerInspection, CreateContainerRequest, DockerRuntime, ProbeRuntime, RouteUpdateRequest,
    RoutingRuntime,
};
use crate::secrets::SecretStore;
#[cfg(test)]
use crate::storage::SnapshotState;
use crate::storage::{
    CleanupRecord, CleanupStore, DiagnosticSummary, DiagnosticsStore, EnvironmentPaths,
    PersistedActivationMode, PersistedSecretReference, PointerStore, RuntimeHealthState,
    RuntimeStateStore, load_generation_build_info, load_generation_runtime_info,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    Recovered(DeploymentRecord),
    Failed(DeploymentRecord),
    NoActiveDeployment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActiveTruth {
    HttpRouted { internal_port: u16 },
    Direct,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickInput {
    pub project_id: String,
    pub environment: String,
    pub now_unix: u64,
    pub truth: ActiveTruth,
    pub http_health_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TickOutcome {
    Healthy(u64),
    Degraded(u64),
    RolledBack(u64),
    Unavailable,
    NoActiveGeneration,
}

#[derive(Debug)]
pub enum ConvergenceError {
    Queue(crate::queue::QueueError),
    Storage(crate::storage::StorageError),
    Docker(crate::runtime::DockerRuntimeError),
    Probe(crate::runtime::ProbeError),
    Routing(crate::runtime::RoutingRuntimeError),
    Secret(crate::secrets::SecretError),
}

impl Display for ConvergenceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queue(err) => write!(f, "{err}"),
            Self::Storage(err) => write!(f, "{err}"),
            Self::Docker(err) => write!(f, "{err}"),
            Self::Probe(err) => write!(f, "{err}"),
            Self::Routing(err) => write!(f, "{err}"),
            Self::Secret(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ConvergenceError {}

impl From<crate::queue::QueueError> for ConvergenceError {
    fn from(value: crate::queue::QueueError) -> Self {
        Self::Queue(value)
    }
}

impl From<crate::storage::StorageError> for ConvergenceError {
    fn from(value: crate::storage::StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<crate::runtime::DockerRuntimeError> for ConvergenceError {
    fn from(value: crate::runtime::DockerRuntimeError) -> Self {
        Self::Docker(value)
    }
}

impl From<crate::runtime::ProbeError> for ConvergenceError {
    fn from(value: crate::runtime::ProbeError) -> Self {
        Self::Probe(value)
    }
}

impl From<crate::runtime::RoutingRuntimeError> for ConvergenceError {
    fn from(value: crate::runtime::RoutingRuntimeError) -> Self {
        Self::Routing(value)
    }
}

impl From<crate::secrets::SecretError> for ConvergenceError {
    fn from(value: crate::secrets::SecretError) -> Self {
        Self::Secret(value)
    }
}

impl From<std::io::Error> for ConvergenceError {
    fn from(value: std::io::Error) -> Self {
        Self::Storage(crate::storage::StorageError::Io(value))
    }
}

pub trait ActiveDeploymentDecider {
    fn should_resume(&self, deployment: &DeploymentRecord) -> bool;
}

pub struct StartupConvergence<'a, D> {
    storage_root: PathBuf,
    queue: &'a PersistentQueue,
    decider: &'a D,
}

impl<'a, D: ActiveDeploymentDecider> StartupConvergence<'a, D> {
    pub fn new(
        storage_root: impl Into<PathBuf>,
        queue: &'a PersistentQueue,
        decider: &'a D,
    ) -> Self {
        Self {
            storage_root: storage_root.into(),
            queue,
            decider,
        }
    }

    pub fn recover_active_deployment<RtD, RtR>(
        &self,
        docker: &mut RtD,
        routing: &mut RtR,
    ) -> Result<RecoveryOutcome, ConvergenceError>
    where
        RtD: DockerRuntime,
        RtR: RoutingRuntime,
    {
        let state = self.queue.load_state()?;
        let active = state.active;

        let outcome = if let Some(active) = active.clone() {
            if self.decider.should_resume(&active) {
                RecoveryOutcome::Recovered(active)
            } else {
                let failed = self.queue.complete_active()?.expect("active just checked");
                RecoveryOutcome::Failed(failed)
            }
        } else {
            RecoveryOutcome::NoActiveDeployment
        };

        let resumable_active = match &outcome {
            RecoveryOutcome::Recovered(active) => Some(active),
            _ => None,
        };
        self.scan_runtime_orphans(resumable_active, docker, routing)?;
        self.recover_finalized_current_generations(docker, routing)?;
        Ok(outcome)
    }

    fn scan_runtime_orphans<RtD, RtR>(
        &self,
        resumable_active: Option<&DeploymentRecord>,
        docker: &mut RtD,
        routing: &mut RtR,
    ) -> Result<(), ConvergenceError>
    where
        RtD: DockerRuntime,
        RtR: RoutingRuntime,
    {
        let managed_containers = docker.list_managed_containers()?;

        for container in &managed_containers {
            let Some((project_id, environment, generation)) = container_identity(&container) else {
                continue;
            };
            if should_preserve_runtime_resource(
                resumable_active,
                &project_id,
                &environment,
                generation,
                &self.storage_root,
            ) {
                continue;
            }

            let env = EnvironmentPaths::new(&self.storage_root, &project_id, &environment);
            let _ = docker.stop_container(&container.container_name);
            let container_removed = docker.remove_container(&container.container_name).is_ok();
            persist_cleanup_state(
                &env,
                generation,
                CleanupRecord::new(
                    "startup orphan container cleanup",
                    Some(container.container_name.clone()),
                    None,
                    container_removed,
                    true,
                    !container_removed,
                ),
                None,
            )?;
        }

        for route in routing.list_managed_routes()? {
            let Some((project_id, environment)) = parse_route_identity(&route.subtree_id) else {
                continue;
            };
            let Some(generation) =
                resolve_generation_from_target(&route.active_target, &managed_containers)
            else {
                let _ = routing.remove_route(&route.subtree_id);
                continue;
            };
            if should_preserve_runtime_resource(
                resumable_active,
                &project_id,
                &environment,
                generation,
                &self.storage_root,
            ) {
                continue;
            }

            let env = EnvironmentPaths::new(&self.storage_root, &project_id, &environment);
            let route_removed = routing.remove_route(&route.subtree_id).is_ok();
            persist_cleanup_state(
                &env,
                generation,
                CleanupRecord::new(
                    "startup orphan route cleanup",
                    None,
                    Some(route.subtree_id.clone()),
                    true,
                    route_removed,
                    !route_removed,
                ),
                None,
            )?;
        }
        Ok(())
    }

    fn recover_finalized_current_generations<RtD, RtR>(
        &self,
        docker: &mut RtD,
        routing: &mut RtR,
    ) -> Result<(), ConvergenceError>
    where
        RtD: DockerRuntime,
        RtR: RoutingRuntime,
    {
        for (project_id, environment, env) in list_environments(&self.storage_root)? {
            let current = match PointerStore::new(env.clone()).read_pointer("current") {
                Ok(value) => value,
                Err(crate::storage::StorageError::InvalidPointer(_)) => continue,
                Err(err) => return Err(err.into()),
            };
            let Some(generation) = current else {
                continue;
            };
            if !snapshot_is_finalized(&env, generation) {
                continue;
            }

            let Some(runtime_info) = load_generation_runtime_info(&env, generation)? else {
                continue;
            };
            let Some(build_info) = load_generation_build_info(&env, generation)? else {
                continue;
            };

            let inspection = ensure_generation_container_running(
                &self.storage_root,
                &project_id,
                &environment,
                generation,
                &build_info.deployment_id,
                &build_info.image_ref,
                &runtime_info.container_name,
                runtime_info.network_name.clone(),
                &runtime_info.environment_variables,
                docker,
            )?;

            if let Some(PersistedActivationMode::Http { internal_port }) = runtime_info.activation {
                ensure_http_route_matches_generation(
                    routing,
                    &project_id,
                    &environment,
                    &inspection,
                    internal_port,
                    runtime_info.probe_path.clone(),
                )?;
            }
        }
        Ok(())
    }
}

pub struct ConvergenceEngine<'a, D, P, R> {
    storage_root: PathBuf,
    queue: &'a PersistentQueue,
    docker: &'a mut D,
    probes: &'a mut P,
    routing: &'a mut R,
}

impl<'a, D, P, R> ConvergenceEngine<'a, D, P, R>
where
    D: DockerRuntime,
    P: ProbeRuntime,
    R: RoutingRuntime,
{
    pub fn new(
        storage_root: impl Into<PathBuf>,
        queue: &'a PersistentQueue,
        docker: &'a mut D,
        probes: &'a mut P,
        routing: &'a mut R,
    ) -> Self {
        Self {
            storage_root: storage_root.into(),
            queue,
            docker,
            probes,
            routing,
        }
    }

    pub fn tick(&mut self, input: TickInput) -> Result<TickOutcome, ConvergenceError> {
        let env = EnvironmentPaths::new(&self.storage_root, &input.project_id, &input.environment);
        env.ensure_exists()?;
        self.reconcile_orphans(&input, &env)?;

        let active_generation = self.reconstruct_active_generation(&input, &env)?;
        let mut runtime_state = RuntimeStateStore::new(env.clone()).load()?;
        runtime_state.active_generation = active_generation;

        let Some(active_generation) = active_generation else {
            runtime_state.health_state = RuntimeHealthState::Unavailable;
            runtime_state.last_transition = "no_active_generation".into();
            RuntimeStateStore::new(env).save(&runtime_state)?;
            return Ok(TickOutcome::NoActiveGeneration);
        };

        let container_name =
            generation_container_name(&input.environment, &input.project_id, active_generation);
        let tcp_ok = self.probes.probe_tcp(&container_name)?;
        let http_ok = if let Some(path) = &input.http_health_path {
            self.probes.probe_http(&container_name, path)?
        } else {
            true
        };

        if tcp_ok && http_ok {
            runtime_state.failed_probe_count = 0;
            runtime_state.successful_probe_count += 1;
            if runtime_state.successful_probe_count >= 2 {
                runtime_state.restart_attempted = false;
                runtime_state.degraded_since_unix = None;
                runtime_state.health_state = RuntimeHealthState::Healthy;
                runtime_state.last_transition = "healthy".into();
                runtime_state.last_error_code = None;
            }
            RuntimeStateStore::new(env).save(&runtime_state)?;
            return Ok(TickOutcome::Healthy(active_generation));
        }

        runtime_state.successful_probe_count = 0;
        runtime_state.failed_probe_count += 1;

        if runtime_state.failed_probe_count >= 3 {
            runtime_state.health_state = RuntimeHealthState::Degraded;
            runtime_state.last_transition = "degraded".into();
            runtime_state.last_error_code = Some(if !tcp_ok {
                "tcp_unreachable".into()
            } else {
                "http_unhealthy".into()
            });
            runtime_state
                .degraded_since_unix
                .get_or_insert(input.now_unix);

            if !runtime_state.restart_attempted {
                let _ = self.docker.stop_container(&container_name);
                self.docker.start_container(&container_name)?;
                runtime_state.restart_attempted = true;
                runtime_state.last_transition = "restart_attempted".into();
                RuntimeStateStore::new(env).save(&runtime_state)?;
                return Ok(TickOutcome::Degraded(active_generation));
            }

            if input
                .now_unix
                .saturating_sub(runtime_state.degraded_since_unix.unwrap_or(input.now_unix))
                >= 30
            {
                if let Some(previous) = PointerStore::new(env.clone()).read_pointer("previous")? {
                    if self.rollback_to_previous(&input, &env, previous)? {
                        runtime_state.active_generation = Some(previous);
                        runtime_state.health_state = RuntimeHealthState::Healthy;
                        runtime_state.failed_probe_count = 0;
                        runtime_state.successful_probe_count = 0;
                        runtime_state.restart_attempted = false;
                        runtime_state.degraded_since_unix = None;
                        runtime_state.last_transition = "rollback_completed".into();
                        runtime_state.last_error_code = None;
                        RuntimeStateStore::new(env).save(&runtime_state)?;
                        return Ok(TickOutcome::RolledBack(previous));
                    }
                }

                if !tcp_ok {
                    if let ActiveTruth::HttpRouted { .. } = input.truth {
                        self.routing.remove_route(&route_subtree_id(
                            &input.project_id,
                            &input.environment,
                        ))?;
                    }
                    runtime_state.health_state = RuntimeHealthState::Unavailable;
                    runtime_state.last_transition = "service_unavailable".into();
                    RuntimeStateStore::new(env).save(&runtime_state)?;
                    return Ok(TickOutcome::Unavailable);
                }
            }
        }

        RuntimeStateStore::new(env).save(&runtime_state)?;
        Ok(TickOutcome::Degraded(active_generation))
    }

    fn reconcile_orphans(
        &mut self,
        input: &TickInput,
        env: &EnvironmentPaths,
    ) -> Result<(), ConvergenceError> {
        let current = PointerStore::new(env.clone()).read_pointer("current")?;
        let previous = PointerStore::new(env.clone()).read_pointer("previous")?;
        let queue_state = self.queue.load_state()?;
        let route_generation = match input.truth {
            ActiveTruth::HttpRouted { .. } => self.route_generation(input)?,
            ActiveTruth::Direct => None,
        };

        if !env.generations_dir().exists() {
            return Ok(());
        }

        for entry in fs::read_dir(env.generations_dir())? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let generation_name = entry.file_name();
            let generation_name = generation_name.to_string_lossy();
            let Ok(generation) = generation_name.parse::<u64>() else {
                continue;
            };
            let generation_dir = env.generation_dir(generation);
            if generation_dir.join("snapshot.json").exists() {
                continue;
            }
            if Some(generation) == current
                || Some(generation) == previous
                || Some(generation) == route_generation
            {
                continue;
            }
            let active_queue_refs = queue_state
                .active
                .as_ref()
                .map(|record| {
                    record.project_id == input.project_id && record.environment == input.environment
                })
                .unwrap_or(false);
            if active_queue_refs {
                continue;
            }
            let container_name =
                generation_container_name(&input.environment, &input.project_id, generation);
            if self.docker.inspect_container(&container_name).is_ok() {
                let _ = self.docker.stop_container(&container_name);
                let _ = self.docker.remove_container(&container_name);
            }
        }
        Ok(())
    }

    fn reconstruct_active_generation(
        &mut self,
        input: &TickInput,
        env: &EnvironmentPaths,
    ) -> Result<Option<u64>, ConvergenceError> {
        match input.truth {
            ActiveTruth::HttpRouted { internal_port } => {
                let current = PointerStore::new(env.clone()).read_pointer("current")?;
                let route_generation = self.route_generation(input)?;
                match (route_generation, current) {
                    (Some(route_generation), _) if snapshot_is_finalized(env, route_generation) => {
                        if current != Some(route_generation) {
                            PointerStore::new(env.clone()).swap_current(route_generation)?;
                        }
                        Ok(Some(route_generation))
                    }
                    (Some(_), Some(current_generation))
                        if snapshot_is_finalized(env, current_generation) =>
                    {
                        let container_name = generation_container_name(
                            &input.environment,
                            &input.project_id,
                            current_generation,
                        );
                        let inspection = self.docker.inspect_container(&container_name)?;
                        let target =
                            resolve_route_target(&inspection, internal_port).ok_or_else(|| {
                                ConvergenceError::Docker(
                                    crate::runtime::DockerRuntimeError::InvalidResponse(
                                        "container missing network IP".into(),
                                    ),
                                )
                            })?;
                        self.routing.update_route(RouteUpdateRequest {
                            subtree_id: route_subtree_id(&input.project_id, &input.environment),
                            target,
                            health_checks_enabled: false,
                            probe_path: input.http_health_path.clone(),
                        })?;
                        Ok(Some(current_generation))
                    }
                    (None, Some(current_generation))
                        if snapshot_is_finalized(env, current_generation) =>
                    {
                        let container_name = generation_container_name(
                            &input.environment,
                            &input.project_id,
                            current_generation,
                        );
                        let inspection = self.docker.inspect_container(&container_name)?;
                        let target =
                            resolve_route_target(&inspection, internal_port).ok_or_else(|| {
                                ConvergenceError::Docker(
                                    crate::runtime::DockerRuntimeError::InvalidResponse(
                                        "container missing network IP".into(),
                                    ),
                                )
                            })?;
                        self.routing.update_route(RouteUpdateRequest {
                            subtree_id: route_subtree_id(&input.project_id, &input.environment),
                            target,
                            health_checks_enabled: false,
                            probe_path: input.http_health_path.clone(),
                        })?;
                        Ok(Some(current_generation))
                    }
                    _ => Ok(None),
                }
            }
            ActiveTruth::Direct => {
                let current = PointerStore::new(env.clone()).read_pointer("current")?;
                let Some(current_generation) = current else {
                    return Ok(None);
                };
                if !snapshot_is_finalized(env, current_generation) {
                    return Ok(None);
                }
                let container_name = generation_container_name(
                    &input.environment,
                    &input.project_id,
                    current_generation,
                );
                match self.docker.inspect_container(&container_name) {
                    Ok(ContainerInspection { running: true, .. }) => Ok(Some(current_generation)),
                    _ => Ok(None),
                }
            }
        }
    }

    fn route_generation(&mut self, input: &TickInput) -> Result<Option<u64>, ConvergenceError> {
        let inspection = self
            .routing
            .inspect_route(&route_subtree_id(&input.project_id, &input.environment))?;
        let containers = self.docker.list_managed_containers()?;
        Ok(resolve_generation_from_target(
            &inspection.active_target,
            &containers,
        ))
    }

    fn rollback_to_previous(
        &mut self,
        input: &TickInput,
        env: &EnvironmentPaths,
        previous: u64,
    ) -> Result<bool, ConvergenceError> {
        if !snapshot_is_finalized(env, previous) {
            return Ok(false);
        }
        match input.truth {
            ActiveTruth::HttpRouted { internal_port } => {
                let Some(runtime_info) = load_generation_runtime_info(env, previous)? else {
                    return Ok(false);
                };
                let Some(build_info) = load_generation_build_info(env, previous)? else {
                    return Ok(false);
                };
                let inspection = ensure_generation_container_running(
                    &self.storage_root,
                    &input.project_id,
                    &input.environment,
                    previous,
                    &build_info.deployment_id,
                    &build_info.image_ref,
                    &runtime_info.container_name,
                    runtime_info.network_name.clone(),
                    &runtime_info.environment_variables,
                    self.docker,
                )?;
                let target = resolve_route_target(&inspection, internal_port).ok_or_else(|| {
                    ConvergenceError::Docker(crate::runtime::DockerRuntimeError::InvalidResponse(
                        "container missing network IP".into(),
                    ))
                })?;
                self.routing.update_route(RouteUpdateRequest {
                    subtree_id: route_subtree_id(&input.project_id, &input.environment),
                    target: target.clone(),
                    health_checks_enabled: false,
                    probe_path: input.http_health_path.clone(),
                })?;
                let inspection = self
                    .routing
                    .inspect_route(&route_subtree_id(&input.project_id, &input.environment))?;
                if inspection.activation_verified
                    && inspection.active_target == target
                    && !inspection.health_checks_enabled
                {
                    PointerStore::new(env.clone()).swap_current(previous)?;
                    return Ok(true);
                }
                Ok(false)
            }
            ActiveTruth::Direct => {
                let Some(runtime_info) = load_generation_runtime_info(env, previous)? else {
                    return Ok(false);
                };
                let Some(build_info) = load_generation_build_info(env, previous)? else {
                    return Ok(false);
                };
                let _ = ensure_generation_container_running(
                    &self.storage_root,
                    &input.project_id,
                    &input.environment,
                    previous,
                    &build_info.deployment_id,
                    &build_info.image_ref,
                    &runtime_info.container_name,
                    runtime_info.network_name.clone(),
                    &runtime_info.environment_variables,
                    self.docker,
                )?;
                PointerStore::new(env.clone()).swap_current(previous)?;
                Ok(true)
            }
        }
    }
}

fn snapshot_is_finalized(env: &EnvironmentPaths, generation: u64) -> bool {
    env.generation_dir(generation)
        .join("snapshot.json")
        .exists()
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

fn route_subtree_id(project_id: &str, environment: &str) -> String {
    format!("forge:{project_id}:{environment}")
}

fn parse_generation_from_target(target: &str) -> Option<u64> {
    let container = target.split(':').next()?;
    let generation = container.rsplit("-gen-").next()?;
    generation.parse::<u64>().ok()
}

fn resolve_generation_from_target(target: &str, containers: &[ContainerInspection]) -> Option<u64> {
    if let Some(generation) = parse_generation_from_target(target) {
        return Some(generation);
    }

    let target_host = target.rsplit_once(':')?.0;
    containers
        .iter()
        .find(|inspection| inspection.network_ips.values().any(|ip| ip == target_host))
        .and_then(container_identity)
        .map(|(_, _, generation)| generation)
}

fn resolve_route_target(inspection: &ContainerInspection, internal_port: u16) -> Option<String> {
    inspection
        .network_ips
        .values()
        .find(|ip| !ip.is_empty())
        .map(|ip| format!("{ip}:{internal_port}"))
}

fn parse_route_identity(subtree_id: &str) -> Option<(String, String)> {
    let mut parts = subtree_id.split(':');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("forge"), Some(project_id), Some(environment), None) => {
            Some((project_id.to_string(), environment.to_string()))
        }
        _ => None,
    }
}

fn container_identity(inspection: &ContainerInspection) -> Option<(String, String, u64)> {
    let project_id = inspection.labels.get("forge.project_id")?.to_string();
    let environment = inspection.labels.get("forge.environment")?.to_string();
    let generation = inspection
        .labels
        .get("forge.generation")?
        .parse::<u64>()
        .ok()?;
    Some((project_id, environment, generation))
}

fn should_preserve_runtime_resource(
    resumable_active: Option<&DeploymentRecord>,
    project_id: &str,
    environment: &str,
    generation: u64,
    storage_root: &std::path::Path,
) -> bool {
    if snapshot_is_finalized(
        &EnvironmentPaths::new(storage_root, project_id, environment),
        generation,
    ) {
        return true;
    }
    resumable_active
        .is_some_and(|active| active.project_id == project_id && active.environment == environment)
}

fn persist_cleanup_state(
    env: &EnvironmentPaths,
    generation: u64,
    new_record: CleanupRecord,
    deployment_id: Option<String>,
) -> Result<(), ConvergenceError> {
    let store = CleanupStore::new(env.clone(), generation);
    let merged = if let Some(existing) = store.read_record()? {
        CleanupRecord {
            timestamp_unix: new_record.timestamp_unix,
            failure_reason: if existing.failure_reason.is_empty() {
                new_record.failure_reason.clone()
            } else {
                format!("{}, {}", existing.failure_reason, new_record.failure_reason)
            },
            container_name: existing
                .container_name
                .or(new_record.container_name.clone()),
            route_subtree_id: existing
                .route_subtree_id
                .or(new_record.route_subtree_id.clone()),
            container_removed: existing.container_removed || new_record.container_removed,
            route_removed: existing.route_removed || new_record.route_removed,
            tombstoned: existing.tombstoned || new_record.tombstoned,
        }
    } else {
        new_record
    };
    store.write_record(&merged)?;
    DiagnosticsStore::new(env.clone(), generation).write_summary(&DiagnosticSummary {
        deployment_id,
        failure_stage: "startup_recovery".into(),
        failure_reason: merged.failure_reason.clone(),
        container_name: merged.container_name.clone().unwrap_or_default(),
        cleanup_recorded: true,
        runtime_env_preview: Vec::new(),
    })?;
    Ok(())
}

fn list_environments(
    storage_root: &std::path::Path,
) -> Result<Vec<(String, String, EnvironmentPaths)>, ConvergenceError> {
    let projects_root = storage_root.join("projects");
    if !projects_root.exists() {
        return Ok(Vec::new());
    }

    let mut environments = Vec::new();
    for project_entry in fs::read_dir(projects_root)? {
        let project_entry = project_entry?;
        if !project_entry.file_type()?.is_dir() {
            continue;
        }
        let project_id = project_entry.file_name().to_string_lossy().to_string();
        let environments_root = project_entry.path().join("environments");
        if !environments_root.exists() {
            continue;
        }
        for environment_entry in fs::read_dir(environments_root)? {
            let environment_entry = environment_entry?;
            if !environment_entry.file_type()?.is_dir() {
                continue;
            }
            let environment = environment_entry.file_name().to_string_lossy().to_string();
            environments.push((
                project_id.clone(),
                environment.clone(),
                EnvironmentPaths::new(storage_root, &project_id, &environment),
            ));
        }
    }

    Ok(environments)
}

fn ensure_generation_container_running<RtD: DockerRuntime>(
    storage_root: &std::path::Path,
    project_id: &str,
    environment: &str,
    generation: u64,
    deployment_id: &str,
    image_ref: &str,
    container_name: &str,
    network_name: Option<String>,
    environment_variables: &BTreeMap<String, PersistedSecretReference>,
    docker: &mut RtD,
) -> Result<ContainerInspection, ConvergenceError> {
    match docker.inspect_container(container_name) {
        Ok(inspection) if inspection.running => return Ok(inspection),
        Ok(_) => {
            docker.start_container(container_name)?;
            return Ok(docker.inspect_container(container_name)?);
        }
        Err(_) => {}
    }

    let labels = BTreeMap::from([
        ("forge.managed".into(), "true".into()),
        ("forge.project_id".into(), project_id.to_string()),
        ("forge.environment".into(), environment.to_string()),
        ("forge.generation".into(), generation.to_string()),
        ("forge.deployment_id".into(), deployment_id.to_string()),
    ]);
    let environment = resolve_recovery_environment(
        storage_root,
        project_id,
        environment,
        environment_variables,
    )?;
    docker.create_container(CreateContainerRequest {
        container_name: container_name.to_string(),
        image_ref: image_ref.to_string(),
        labels,
        environment,
        network_name,
    })?;
    docker.start_container(container_name)?;
    Ok(docker.inspect_container(container_name)?)
}

fn resolve_recovery_environment(
    storage_root: &std::path::Path,
    project_id: &str,
    environment: &str,
    environment_variables: &BTreeMap<String, PersistedSecretReference>,
) -> Result<BTreeMap<String, String>, ConvergenceError> {
    let store = SecretStore::new(storage_root.join("secrets"))?;
    let mut resolved = BTreeMap::new();
    for (env_name, reference) in environment_variables {
        match reference.scope.as_str() {
            "environment" => {
                let value =
                    store.read_environment_secret(project_id, environment, &reference.key)?;
                resolved.insert(env_name.clone(), value);
            }
            other => {
                return Err(ConvergenceError::Storage(
                    crate::storage::StorageError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unsupported secret scope {other}"),
                    )),
                ));
            }
        }
    }
    Ok(resolved)
}

fn ensure_http_route_matches_generation<RtR: RoutingRuntime>(
    routing: &mut RtR,
    project_id: &str,
    environment: &str,
    inspection: &ContainerInspection,
    internal_port: u16,
    probe_path: Option<String>,
) -> Result<(), ConvergenceError> {
    let target = resolve_route_target(inspection, internal_port).ok_or_else(|| {
        ConvergenceError::Docker(crate::runtime::DockerRuntimeError::InvalidResponse(
            "container missing network IP".into(),
        ))
    })?;
    let subtree_id = route_subtree_id(project_id, environment);
    let route_matches = routing
        .inspect_route(&subtree_id)
        .map(|route| {
            route.active_target == target && route.activation_verified && !route.health_checks_enabled
        })
        .unwrap_or(false);
    if route_matches {
        return Ok(());
    }

    routing.update_route(RouteUpdateRequest {
        subtree_id: subtree_id.clone(),
        target: target.clone(),
        health_checks_enabled: false,
        probe_path,
    })?;
    let route = routing.inspect_route(&subtree_id)?;
    if route.active_target != target || !route.activation_verified || route.health_checks_enabled {
        return Err(ConvergenceError::Routing(
            crate::runtime::RoutingRuntimeError::UpdateFailed(
                "route activation verification failed".into(),
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn test_root(name: &str) -> std::path::PathBuf {
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
struct ResumeDecider(bool);

#[cfg(test)]
impl ActiveDeploymentDecider for ResumeDecider {
    fn should_resume(&self, _deployment: &DeploymentRecord) -> bool {
        self.0
    }
}

#[cfg(test)]
#[derive(Default)]
struct TestProbeRuntime {
    tcp_ok: bool,
    http_ok: bool,
}

#[cfg(test)]
impl ProbeRuntime for TestProbeRuntime {
    fn probe_tcp(&mut self, _container_name: &str) -> Result<bool, crate::runtime::ProbeError> {
        Ok(self.tcp_ok)
    }

    fn probe_http(
        &mut self,
        _container_name: &str,
        _path: &str,
    ) -> Result<bool, crate::runtime::ProbeError> {
        Ok(self.http_ok)
    }
}

#[cfg(test)]
#[derive(Default)]
struct TestDockerRuntime {
    containers: std::collections::BTreeMap<String, bool>,
    build_calls: Vec<String>,
    create_calls: Vec<String>,
    start_calls: Vec<String>,
    stop_calls: Vec<String>,
    remove_calls: Vec<String>,
}

#[cfg(test)]
impl DockerRuntime for TestDockerRuntime {
    fn build_image(
        &mut self,
        request: crate::runtime::BuildImageRequest,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        self.build_calls.push(request.image_tag.clone());
        Ok(request.image_tag)
    }

    fn create_container(
        &mut self,
        request: crate::runtime::CreateContainerRequest,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        self.create_calls.push(request.container_name.clone());
        self.containers.insert(request.container_name.clone(), true);
        Ok(request.container_name)
    }

    fn start_container(
        &mut self,
        container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        self.start_calls.push(container_name.into());
        self.containers.insert(container_name.into(), true);
        Ok(())
    }

    fn inspect_container(
        &mut self,
        container_name: &str,
    ) -> Result<ContainerInspection, crate::runtime::DockerRuntimeError> {
        let Some(running) = self.containers.get(container_name).copied() else {
            return Err(crate::runtime::DockerRuntimeError::InvalidResponse(
                "missing container".into(),
            ));
        };
        Ok(ContainerInspection {
            container_name: container_name.into(),
            running,
            image_ref: "noop".into(),
            labels: Default::default(),
            network_ips: std::collections::BTreeMap::from([(
                "forge-test".into(),
                test_container_ip(container_name),
            )]),
            restart_policy: "no".into(),
        })
    }

    fn list_managed_containers(
        &mut self,
    ) -> Result<Vec<ContainerInspection>, crate::runtime::DockerRuntimeError> {
        Ok(self
            .containers
            .iter()
            .map(|(container_name, running)| ContainerInspection {
                container_name: container_name.clone(),
                running: *running,
                image_ref: "noop".into(),
                labels: std::collections::BTreeMap::from([
                    ("forge.managed".into(), "true".into()),
                    ("forge.project_id".into(), "api".into()),
                    ("forge.environment".into(), "production".into()),
                    (
                        "forge.generation".into(),
                        container_name
                            .rsplit("-gen-")
                            .next()
                            .unwrap_or("0")
                            .to_string(),
                    ),
                ]),
                network_ips: std::collections::BTreeMap::from([(
                    "forge-test".into(),
                    test_container_ip(container_name),
                )]),
                restart_policy: "no".into(),
            })
            .collect())
    }

    fn stop_container(
        &mut self,
        container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        self.stop_calls.push(container_name.into());
        self.containers.insert(container_name.into(), false);
        Ok(())
    }

    fn remove_container(
        &mut self,
        container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        self.remove_calls.push(container_name.into());
        self.containers.remove(container_name);
        Ok(())
    }
}

#[cfg(test)]
fn test_container_ip(container_name: &str) -> String {
    let generation = container_name
        .rsplit("-gen-")
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    format!("172.19.0.{}", generation + 10)
}

#[cfg(test)]
#[derive(Default)]
struct TestRoutingRuntime {
    route: Option<RouteInspection>,
    updates: Vec<RouteUpdateRequest>,
}

#[cfg(test)]
impl RoutingRuntime for TestRoutingRuntime {
    fn update_route(
        &mut self,
        request: RouteUpdateRequest,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        self.route = Some(RouteInspection {
            subtree_id: request.subtree_id.clone(),
            active_target: request.target.clone(),
            activation_verified: true,
            health_checks_enabled: request.health_checks_enabled,
        });
        self.updates.push(request);
        Ok(())
    }

    fn inspect_route(
        &mut self,
        _subtree_id: &str,
    ) -> Result<RouteInspection, crate::runtime::RoutingRuntimeError> {
        self.route
            .clone()
            .ok_or(crate::runtime::RoutingRuntimeError::InspectionFailed(
                "missing route".into(),
            ))
    }

    fn list_managed_routes(
        &mut self,
    ) -> Result<Vec<RouteInspection>, crate::runtime::RoutingRuntimeError> {
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
fn setup_active_generation(root: &std::path::Path, generation: u64) {
    let env = EnvironmentPaths::new(root, "api", "production");
    let writer = crate::storage::SnapshotWriter::new(env.clone(), generation).unwrap();
    writer
        .finalize("api", "production", SnapshotState::Healthy)
        .unwrap();
    crate::storage::atomic_write(
        env.generation_dir(generation).join("build.json"),
        format!(
            "{{\n  \"deployment_id\": \"dep-{generation}\",\n  \"image_ref\": \"forge/api:production-gen-{generation}\"\n}}\n"
        )
        .as_bytes(),
    )
    .unwrap();
    crate::storage::atomic_write(
        env.generation_dir(generation).join("runtime.json"),
        format!(
            "{{\n  \"container_name\": \"prod-api-gen-{generation}\",\n  \"running\": true,\n  \"network_name\": \"forge-test\",\n  \"activation\": \"Direct\",\n  \"environment_variables\": {{}}\n}}\n"
        )
        .as_bytes(),
    )
    .unwrap();
    crate::storage::atomic_write(
        env.generation_counter(),
        format!("{generation}\n").as_bytes(),
    )
    .unwrap();
}

#[cfg(test)]
fn setup_recoverable_http_generation(root: &std::path::Path, generation: u64) {
    let env = EnvironmentPaths::new(root, "api", "production");
    setup_active_generation(root, generation);
    crate::storage::atomic_write(
        env.generation_dir(generation).join("runtime.json"),
        format!(
            "{{\n  \"container_name\": \"prod-api-gen-{generation}\",\n  \"running\": true,\n  \"network_name\": \"forge-test\",\n  \"probe_path\": \"/health\",\n  \"activation\": {{ \"Http\": {{ \"internal_port\": 3000 }} }},\n  \"environment_variables\": {{}}\n}}\n"
        )
        .as_bytes(),
    )
    .unwrap();
}

#[cfg(test)]
pub mod in_flight_deployment_is_recovered_or_failed_deterministically {
    use super::*;

    #[test]
    fn resumable_active_deployment_is_preserved() {
        let root = test_root("recover-active");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "d1".into(),
                project_id: "api".into(),
                environment: "production".into(),
            })
            .unwrap();
        let active = queue.start_next().unwrap().unwrap();

        let mut docker = TestDockerRuntime::default();
        let mut routing = TestRoutingRuntime::default();
        let convergence = StartupConvergence::new(&root, &queue, &ResumeDecider(true));
        let recovered = convergence
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert_eq!(recovered, RecoveryOutcome::Recovered(active));
        assert!(queue.load_state().unwrap().active.is_some());
    }

    #[test]
    fn non_resumable_active_deployment_is_failed_and_cleared() {
        let root = test_root("fail-active");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "d1".into(),
                project_id: "api".into(),
                environment: "production".into(),
            })
            .unwrap();
        let active = queue.start_next().unwrap().unwrap();

        let mut docker = TestDockerRuntime::default();
        let mut routing = TestRoutingRuntime::default();
        let convergence = StartupConvergence::new(&root, &queue, &ResumeDecider(false));
        let failed = convergence
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert_eq!(failed, RecoveryOutcome::Failed(active));
        assert!(queue.load_state().unwrap().active.is_none());
    }
}

#[cfg(test)]
pub mod steady_state_marks_generation_degraded {
    use super::*;

    #[test]
    fn three_failed_probes_mark_runtime_degraded() {
        let root = test_root("steady-degraded");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();
        let mut engine =
            ConvergenceEngine::new(&root, &queue, &mut docker, &mut probes, &mut routing);

        let result = engine
            .tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: 100,
                truth: ActiveTruth::Direct,
                http_health_path: None,
            })
            .unwrap();
        assert_eq!(result, TickOutcome::Degraded(1));
        engine
            .tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: 101,
                truth: ActiveTruth::Direct,
                http_health_path: None,
            })
            .unwrap();
        engine
            .tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: 102,
                truth: ActiveTruth::Direct,
                http_health_path: None,
            })
            .unwrap();

        let state = RuntimeStateStore::new(env).load().unwrap();
        assert_eq!(state.health_state, RuntimeHealthState::Degraded);
    }
}

#[cfg(test)]
pub mod steady_state_restart_attempt_occurs_once {
    use super::*;

    #[test]
    fn restart_is_attempted_only_once_per_degradation_episode() {
        let root = test_root("steady-restart-once");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();
        let mut engine =
            ConvergenceEngine::new(&root, &queue, &mut docker, &mut probes, &mut routing);

        for now in 100..105 {
            let _ = engine.tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: now,
                truth: ActiveTruth::Direct,
                http_health_path: None,
            });
        }

        assert_eq!(
            docker
                .start_calls
                .iter()
                .filter(|name| *name == "prod-api-gen-1")
                .count(),
            1
        );
    }
}

#[cfg(test)]
pub mod steady_state_rollback_restores_previous_generation {
    use super::*;

    #[test]
    fn rollback_restores_previous_after_failed_restart_window() {
        let root = test_root("steady-rollback");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-2".into(), true);
        let mut probes = TestProbeRuntime {
            tcp_ok: false,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();
        let mut engine =
            ConvergenceEngine::new(&root, &queue, &mut docker, &mut probes, &mut routing);

        for now in [100, 101, 102, 133] {
            let outcome = engine
                .tick(TickInput {
                    project_id: "api".into(),
                    environment: "production".into(),
                    now_unix: now,
                    truth: ActiveTruth::Direct,
                    http_health_path: None,
                })
                .unwrap();
            if now == 133 {
                assert_eq!(outcome, TickOutcome::RolledBack(1));
            }
        }

        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
    }
}

#[cfg(test)]
pub mod orphaned_candidate_generation_is_cleaned {
    use super::*;

    #[test]
    fn nonfinalized_orphan_container_is_removed() {
        let root = test_root("orphan-cleanup");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let orphan_dir = env.generation_dir(2);
        fs::create_dir_all(&orphan_dir).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.containers.insert("prod-api-gen-2".into(), true);
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();
        let mut engine =
            ConvergenceEngine::new(&root, &queue, &mut docker, &mut probes, &mut routing);

        let _ = engine
            .tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: 100,
                truth: ActiveTruth::Direct,
                http_health_path: None,
            })
            .unwrap();

        assert!(
            docker
                .remove_calls
                .iter()
                .any(|name| name == "prod-api-gen-2")
        );
    }
}

#[cfg(test)]
pub mod daemon_restart_reconstructs_active_generation {
    use super::*;

    #[test]
    fn runtime_state_is_reconstructed_from_current_for_direct_services() {
        let root = test_root("restart-reconstruct-direct");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime::default();
        let mut engine =
            ConvergenceEngine::new(&root, &queue, &mut docker, &mut probes, &mut routing);

        let outcome = engine
            .tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: 100,
                truth: ActiveTruth::Direct,
                http_health_path: None,
            })
            .unwrap();

        assert_eq!(outcome, TickOutcome::Healthy(1));
        let state = RuntimeStateStore::new(env).load().unwrap();
        assert_eq!(state.active_generation, Some(1));
    }
}

#[cfg(test)]
pub mod current_pointer_matches_active_route_after_restart {
    use super::*;

    #[test]
    fn route_truth_repairs_current_pointer_for_http_services() {
        let root = test_root("restart-repair-http");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-2".into(), true);
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            route: Some(RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.19.0.12:3000".into(),
                activation_verified: true,
                health_checks_enabled: false,
            }),
            updates: Vec::new(),
        };
        let mut engine =
            ConvergenceEngine::new(&root, &queue, &mut docker, &mut probes, &mut routing);

        let outcome = engine
            .tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: 100,
                truth: ActiveTruth::HttpRouted {
                    internal_port: 3000,
                },
                http_health_path: Some("/health".into()),
            })
            .unwrap();

        assert_eq!(outcome, TickOutcome::Healthy(2));
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(2));
    }
}

#[cfg(test)]
pub mod startup_recovery_reconstructs_finalized_current_generation {
    use super::*;

    #[test]
    fn startup_recovers_missing_current_container_and_route_without_rebuild() {
        let root = test_root("startup-recover-current-http");
        setup_recoverable_http_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        let mut routing = TestRoutingRuntime::default();

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert!(docker.build_calls.is_empty());
        assert_eq!(docker.create_calls, vec!["prod-api-gen-1".to_string()]);
        assert_eq!(docker.start_calls, vec!["prod-api-gen-1".to_string()]);
        assert_eq!(
            routing
                .route
                .as_ref()
                .map(|route| route.active_target.clone()),
            Some("172.19.0.11:3000".into())
        );
    }
}
