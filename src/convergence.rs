use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::PathBuf;

use crate::events::EventRecord;
use crate::projects::ProjectRegistryStore;
use crate::queue::{DeploymentRecord, PersistentQueue};
use crate::runtime::{
    ContainerInspection, CreateContainerRequest, DockerRuntime, ManagedImage, ProbeRuntime,
    RouteInspection, RouteUpdateRequest, RoutingRuntime,
};
use crate::secrets::SecretStore;
use crate::status::derive_environment_domain;
#[cfg(test)]
use crate::storage::SnapshotState;
use crate::storage::{
    CleanupRecord, CleanupStore, DiagnosticSummary, DiagnosticsStore, EnvironmentPaths, EventStore,
    PersistedActivationMode, PersistedRouteTargetSource, PersistedRuntimeInfo,
    PersistedSecretReference, PointerStore, RuntimeHealthState, RuntimeStateStore,
    load_generation_build_info, load_generation_runtime_info,
};

// Beyond current/previous, retain only a small recent diagnostic tail of failed generations.
const FAILED_GENERATION_RETENTION_LIMIT: usize = 2;

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

impl TickInput {
    fn internal_port(&self) -> u16 {
        match self.truth {
            ActiveTruth::HttpRouted { internal_port } => internal_port,
            ActiveTruth::Direct => 3000,
        }
    }
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
        let mut managed_containers = docker.list_managed_containers()?;
        managed_containers.sort_by(|left, right| left.container_name.cmp(&right.container_name));
        let mut managed_images = docker.list_managed_images()?;
        managed_images.sort_by(|left, right| left.image_ref.cmp(&right.image_ref));
        let mut managed_routes = routing.list_managed_routes()?;
        managed_routes.sort_by(|left, right| left.subtree_id.cmp(&right.subtree_id));
        let queue_state = self.queue.load_state()?;
        let active_record = resumable_active.or(queue_state.active.as_ref());
        let mut attempted_cleanup = BTreeSet::new();

        let environments = cleanup_scan_environments(
            &self.storage_root,
            &managed_containers,
            &managed_images,
            &managed_routes,
        )?;
        for (project_id, environment, env) in &environments {
            retry_tombstoned_cleanup(
                docker,
                routing,
                project_id,
                environment,
                env,
                active_record,
                &managed_containers,
                &managed_images,
                &managed_routes,
                &mut attempted_cleanup,
            )?;
        }

        let removed_containers = cleanup_orphaned_containers(
            docker,
            &self.storage_root,
            active_record,
            &managed_containers,
            &managed_images,
            &managed_routes,
            &mut attempted_cleanup,
        )?;
        cleanup_orphaned_images(
            docker,
            &self.storage_root,
            active_record,
            &managed_containers,
            &managed_images,
            &managed_routes,
            &mut attempted_cleanup,
        )?;
        for (project_id, environment, env) in &environments {
            enforce_generation_retention(
                docker,
                routing,
                project_id,
                environment,
                env,
                active_record,
                &managed_containers,
                &managed_images,
                &managed_routes,
                &mut attempted_cleanup,
            )?;
        }

        for route in &managed_routes {
            let Some((project_id, environment)) = parse_route_identity(&route.subtree_id) else {
                continue;
            };
            let env = EnvironmentPaths::new(&self.storage_root, &project_id, &environment);
            let references = environment_runtime_references(
                &env,
                &project_id,
                &environment,
                active_record,
                &managed_containers,
                &managed_routes,
            )?;
            let Some(generation) = cleanup_generation_for_route(&env, route, &references) else {
                continue;
            };
            if attempted_cleanup.contains(&(project_id.clone(), environment.clone(), generation))
                && !env.generation_dir(generation).exists()
            {
                continue;
            }
            if route_has_valid_backing(
                route,
                &project_id,
                &environment,
                &env,
                &references,
                &managed_containers,
                &removed_containers,
            ) {
                continue;
            }
            attempt_cleanup(
                docker,
                routing,
                &env,
                &project_id,
                &environment,
                generation,
                CleanupRecord::new(
                    "startup orphan route cleanup",
                    None,
                    Some(route.subtree_id.clone()),
                    true,
                    false,
                    true,
                ),
                None,
                "ORPHANED_ROUTE_REMOVED",
                "ORPHANED_ROUTE_TOMBSTONED",
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

            if let Some(route_recovery) = persisted_http_route_recovery(
                &self.storage_root,
                &runtime_info,
                &project_id,
                &environment,
            )? {
                ensure_http_route_matches_generation(
                    routing,
                    &route_recovery.subtree_id,
                    &inspection,
                    route_recovery.internal_port,
                    runtime_info.network_name.as_deref(),
                    route_recovery.domain,
                    &route_recovery.target_source,
                    route_recovery.probe_path,
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
        let tcp_ok = self
            .probes
            .probe_tcp(&container_name, input.internal_port())?;
        let http_ok = if let Some(path) = &input.http_health_path {
            self.probes
                .probe_http(&container_name, input.internal_port(), path)?
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
        let mut managed_containers = self.docker.list_managed_containers()?;
        managed_containers.sort_by(|left, right| left.container_name.cmp(&right.container_name));
        let mut managed_images = self.docker.list_managed_images()?;
        managed_images.sort_by(|left, right| left.image_ref.cmp(&right.image_ref));
        let mut managed_routes = self.routing.list_managed_routes()?;
        managed_routes.sort_by(|left, right| left.subtree_id.cmp(&right.subtree_id));
        let queue_state = self.queue.load_state()?;
        let mut attempted_cleanup = BTreeSet::new();

        retry_tombstoned_cleanup(
            self.docker,
            self.routing,
            &input.project_id,
            &input.environment,
            env,
            queue_state.active.as_ref(),
            &managed_containers,
            &managed_images,
            &managed_routes,
            &mut attempted_cleanup,
        )?;

        let removed_containers = cleanup_orphaned_containers(
            self.docker,
            &self.storage_root,
            queue_state.active.as_ref(),
            &managed_containers,
            &managed_images,
            &managed_routes,
            &mut attempted_cleanup,
        )?;
        cleanup_orphaned_images(
            self.docker,
            &self.storage_root,
            queue_state.active.as_ref(),
            &managed_containers,
            &managed_images,
            &managed_routes,
            &mut attempted_cleanup,
        )?;
        enforce_generation_retention(
            self.docker,
            self.routing,
            &input.project_id,
            &input.environment,
            env,
            queue_state.active.as_ref(),
            &managed_containers,
            &managed_images,
            &managed_routes,
            &mut attempted_cleanup,
        )?;

        for route in &managed_routes {
            let Some((project_id, environment)) = parse_route_identity(&route.subtree_id) else {
                continue;
            };
            if project_id != input.project_id || environment != input.environment {
                continue;
            }
            let references = environment_runtime_references(
                env,
                &project_id,
                &environment,
                queue_state.active.as_ref(),
                &managed_containers,
                &managed_routes,
            )?;
            let Some(generation) = cleanup_generation_for_route(env, route, &references) else {
                continue;
            };
            if attempted_cleanup.contains(&(project_id.clone(), environment.clone(), generation)) {
                continue;
            }
            if route_has_valid_backing(
                route,
                &project_id,
                &environment,
                env,
                &references,
                &managed_containers,
                &removed_containers,
            ) {
                continue;
            }
            attempt_cleanup(
                self.docker,
                self.routing,
                env,
                &project_id,
                &environment,
                generation,
                CleanupRecord::new(
                    "orphaned route cleanup",
                    None,
                    Some(route.subtree_id.clone()),
                    true,
                    false,
                    true,
                ),
                None,
                "ORPHANED_ROUTE_REMOVED",
                "ORPHANED_ROUTE_TOMBSTONED",
            )?;
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
                        let runtime_info = load_generation_runtime_info(env, current_generation)?;
                        let route_recovery = http_route_recovery_input(
                            &self.storage_root,
                            runtime_info.as_ref(),
                            &input.project_id,
                            &input.environment,
                            internal_port,
                            input.http_health_path.clone(),
                        )?;
                        let preferred_network = runtime_info
                            .as_ref()
                            .and_then(|info| info.network_name.as_deref());
                        let container_name = generation_container_name(
                            &input.environment,
                            &input.project_id,
                            current_generation,
                        );
                        let inspection = self.docker.inspect_container(&container_name)?;
                        ensure_http_route_matches_generation(
                            self.routing,
                            &route_recovery.subtree_id,
                            &inspection,
                            route_recovery.internal_port,
                            preferred_network,
                            route_recovery.domain,
                            &route_recovery.target_source,
                            route_recovery.probe_path,
                        )?;
                        Ok(Some(current_generation))
                    }
                    (None, Some(current_generation))
                        if snapshot_is_finalized(env, current_generation) =>
                    {
                        let runtime_info = load_generation_runtime_info(env, current_generation)?;
                        let route_recovery = http_route_recovery_input(
                            &self.storage_root,
                            runtime_info.as_ref(),
                            &input.project_id,
                            &input.environment,
                            internal_port,
                            input.http_health_path.clone(),
                        )?;
                        let preferred_network = runtime_info
                            .as_ref()
                            .and_then(|info| info.network_name.as_deref());
                        let container_name = generation_container_name(
                            &input.environment,
                            &input.project_id,
                            current_generation,
                        );
                        let inspection = self.docker.inspect_container(&container_name)?;
                        ensure_http_route_matches_generation(
                            self.routing,
                            &route_recovery.subtree_id,
                            &inspection,
                            route_recovery.internal_port,
                            preferred_network,
                            route_recovery.domain,
                            &route_recovery.target_source,
                            route_recovery.probe_path,
                        )?;
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
        let inspection = match self
            .routing
            .inspect_route(&route_subtree_id(&input.project_id, &input.environment))
        {
            Ok(inspection) => inspection,
            Err(crate::runtime::RoutingRuntimeError::InspectionFailed(message))
                if message.contains("missing route") =>
            {
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };
        if !inspection.activation_verified || inspection.health_checks_enabled {
            return Ok(None);
        }
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
                let route_recovery = http_route_recovery_input(
                    &self.storage_root,
                    Some(&runtime_info),
                    &input.project_id,
                    &input.environment,
                    internal_port,
                    input.http_health_path.clone(),
                )?;
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
                ensure_http_route_matches_generation(
                    self.routing,
                    &route_recovery.subtree_id,
                    &inspection,
                    route_recovery.internal_port,
                    runtime_info.network_name.as_deref(),
                    route_recovery.domain,
                    &route_recovery.target_source,
                    route_recovery.probe_path,
                )?;
                PointerStore::new(env.clone()).swap_current(previous)?;
                Ok(true)
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpRouteRecoveryInput {
    subtree_id: String,
    internal_port: u16,
    domain: Option<String>,
    probe_path: Option<String>,
    target_source: PersistedRouteTargetSource,
}

fn persisted_http_route_recovery(
    storage_root: &std::path::Path,
    runtime_info: &PersistedRuntimeInfo,
    project_id: &str,
    environment: &str,
) -> Result<Option<HttpRouteRecoveryInput>, ConvergenceError> {
    match &runtime_info.activation {
        Some(PersistedActivationMode::Http {
            internal_port,
            route_subtree_id: persisted_subtree_id,
            target_source,
        }) => Ok(Some(HttpRouteRecoveryInput {
            subtree_id: persisted_subtree_id
                .clone()
                .unwrap_or_else(|| route_subtree_id(project_id, environment)),
            internal_port: *internal_port,
            domain: load_environment_domain(storage_root, project_id, environment)?,
            probe_path: runtime_info.probe_path.clone(),
            target_source: target_source.clone(),
        })),
        _ => Ok(None),
    }
}

fn http_route_recovery_input(
    storage_root: &std::path::Path,
    runtime_info: Option<&PersistedRuntimeInfo>,
    project_id: &str,
    environment: &str,
    internal_port: u16,
    probe_path: Option<String>,
) -> Result<HttpRouteRecoveryInput, ConvergenceError> {
    if let Some(runtime_info) = runtime_info {
        if let Some(mut persisted) =
            persisted_http_route_recovery(storage_root, runtime_info, project_id, environment)?
        {
            if persisted.probe_path.is_none() {
                persisted.probe_path = probe_path;
            }
            return Ok(persisted);
        }

        return Ok(HttpRouteRecoveryInput {
            subtree_id: route_subtree_id(project_id, environment),
            internal_port,
            domain: load_environment_domain(storage_root, project_id, environment)?,
            probe_path: runtime_info.probe_path.clone().or(probe_path),
            target_source: PersistedRouteTargetSource::ContainerIp,
        });
    }

    Ok(HttpRouteRecoveryInput {
        subtree_id: route_subtree_id(project_id, environment),
        internal_port,
        domain: load_environment_domain(storage_root, project_id, environment)?,
        probe_path,
        target_source: PersistedRouteTargetSource::ContainerIp,
    })
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

fn resolve_route_target(
    inspection: &ContainerInspection,
    internal_port: u16,
    preferred_network: Option<&str>,
    target_source: &PersistedRouteTargetSource,
) -> Option<String> {
    match target_source {
        PersistedRouteTargetSource::ContainerIp => {
            if let Some(network_name) = preferred_network {
                return inspection
                    .network_ips
                    .get(network_name)
                    .filter(|ip| !ip.is_empty())
                    .map(|ip| format!("{ip}:{internal_port}"));
            }

            inspection
                .network_ips
                .values()
                .find(|ip| !ip.is_empty())
                .map(|ip| format!("{ip}:{internal_port}"))
        }
    }
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

fn image_identity(image: &ManagedImage) -> Option<(String, String, u64)> {
    let project_id = image.labels.get("forge.project_id")?.to_string();
    let environment = image.labels.get("forge.environment")?.to_string();
    let generation = image.labels.get("forge.generation")?.parse::<u64>().ok()?;
    Some((project_id, environment, generation))
}

fn container_for_generation<'a>(
    managed_containers: &'a [ContainerInspection],
    project_id: &str,
    environment: &str,
    generation: u64,
) -> Option<&'a ContainerInspection> {
    managed_containers.iter().find(|container| {
        container_identity(container)
            == Some((project_id.to_string(), environment.to_string(), generation))
    })
}

fn image_for_generation<'a>(
    managed_images: &'a [ManagedImage],
    project_id: &str,
    environment: &str,
    generation: u64,
) -> Option<&'a ManagedImage> {
    managed_images.iter().find(|image| {
        image_identity(image) == Some((project_id.to_string(), environment.to_string(), generation))
    })
}

fn image_ref_for_generation(
    project_id: &str,
    environment: &str,
    generation: u64,
    managed_containers: &[ContainerInspection],
    managed_images: &[ManagedImage],
) -> Option<String> {
    container_for_generation(managed_containers, project_id, environment, generation)
        .map(|container| container.image_ref.clone())
        .or_else(|| {
            image_for_generation(managed_images, project_id, environment, generation)
                .map(|image| image.image_ref.clone())
        })
}

#[derive(Debug, Clone, Default)]
struct RuntimeReferences {
    current: Option<u64>,
    previous: Option<u64>,
    route_generation: Option<u64>,
    converging_generation: Option<u64>,
}

impl RuntimeReferences {
    fn contains(&self, generation: u64) -> bool {
        self.current == Some(generation)
            || self.previous == Some(generation)
            || self.route_generation == Some(generation)
            || self.converging_generation == Some(generation)
    }
}

fn environment_runtime_references(
    env: &EnvironmentPaths,
    project_id: &str,
    environment: &str,
    active_record: Option<&DeploymentRecord>,
    managed_containers: &[ContainerInspection],
    managed_routes: &[RouteInspection],
) -> Result<RuntimeReferences, ConvergenceError> {
    env.ensure_exists()?;
    let active_matches = active_record
        .is_some_and(|record| record.project_id == project_id && record.environment == environment);
    let route_generation = managed_routes
        .iter()
        .find(|route| route.subtree_id == route_subtree_id(project_id, environment))
        .and_then(|route| {
            resolve_generation_from_target(&route.active_target, managed_containers)
                .or_else(|| parse_generation_from_target(&route.active_target))
        })
        .filter(|generation| active_matches || snapshot_is_finalized(env, *generation));
    Ok(RuntimeReferences {
        current: PointerStore::new(env.clone()).read_pointer("current")?,
        previous: PointerStore::new(env.clone()).read_pointer("previous")?,
        route_generation,
        converging_generation: if active_matches {
            latest_nonfinalized_generation(env)?
        } else {
            None
        },
    })
}

fn latest_nonfinalized_generation(env: &EnvironmentPaths) -> Result<Option<u64>, ConvergenceError> {
    if !env.generations_dir().exists() {
        return Ok(None);
    }
    let mut latest: Option<u64> = None;
    for entry in fs::read_dir(env.generations_dir())? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let generation = entry.file_name().to_string_lossy().parse::<u64>().ok();
        let Some(generation) = generation else {
            continue;
        };
        if snapshot_is_finalized(env, generation) {
            continue;
        }
        latest = Some(latest.map_or(generation, |current| current.max(generation)));
    }
    Ok(latest)
}

fn cleanup_scan_environments(
    storage_root: &std::path::Path,
    managed_containers: &[ContainerInspection],
    managed_images: &[ManagedImage],
    managed_routes: &[RouteInspection],
) -> Result<Vec<(String, String, EnvironmentPaths)>, ConvergenceError> {
    let mut environments = list_environments(storage_root)?;
    let mut seen = environments
        .iter()
        .map(|(project_id, environment, _)| (project_id.clone(), environment.clone()))
        .collect::<BTreeSet<_>>();
    for container in managed_containers {
        if let Some((project_id, environment, _)) = container_identity(container) {
            if seen.insert((project_id.clone(), environment.clone())) {
                environments.push((
                    project_id.clone(),
                    environment.clone(),
                    EnvironmentPaths::new(storage_root, &project_id, &environment),
                ));
            }
        }
    }
    for image in managed_images {
        if let Some((project_id, environment, _)) = image_identity(image) {
            if seen.insert((project_id.clone(), environment.clone())) {
                environments.push((
                    project_id.clone(),
                    environment.clone(),
                    EnvironmentPaths::new(storage_root, &project_id, &environment),
                ));
            }
        }
    }
    for route in managed_routes {
        if let Some((project_id, environment)) = parse_route_identity(&route.subtree_id) {
            if seen.insert((project_id.clone(), environment.clone())) {
                environments.push((
                    project_id.clone(),
                    environment.clone(),
                    EnvironmentPaths::new(storage_root, &project_id, &environment),
                ));
            }
        }
    }
    environments.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    Ok(environments)
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
            image_ref: existing.image_ref.or(new_record.image_ref.clone()),
            container_removed: existing.container_removed || new_record.container_removed,
            route_removed: existing.route_removed || new_record.route_removed,
            image_removed: existing.image_removed || new_record.image_removed,
            tombstoned: !(existing.container_removed || new_record.container_removed)
                || !(existing.route_removed || new_record.route_removed)
                || !(existing.image_removed || new_record.image_removed),
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
        probe_target_host: None,
        probe_target_port: None,
        probe_target_path: None,
        cleanup_recorded: true,
        runtime_env_preview: Vec::new(),
    })?;
    Ok(())
}

fn append_cleanup_event(
    env: &EnvironmentPaths,
    project_id: &str,
    environment: &str,
    generation: u64,
    deployment_id: Option<String>,
    event_type: &str,
    reason: Option<String>,
) -> Result<(), ConvergenceError> {
    let timestamp_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    EventStore::new(env.clone(), generation).append(&EventRecord {
        timestamp_unix,
        project_id: project_id.to_string(),
        environment: environment.to_string(),
        generation: Some(generation),
        deployment_id,
        event_type: event_type.to_string(),
        reason,
    })?;
    Ok(())
}

fn append_retention_event(
    env: &EnvironmentPaths,
    references: &RuntimeReferences,
    project_id: &str,
    environment: &str,
    generation: u64,
    event_type: &str,
    reason: Option<String>,
) -> Result<(), ConvergenceError> {
    let anchor_generation = references
        .current
        .or(references.previous)
        .or(references.route_generation)
        .or(references.converging_generation)
        .unwrap_or(generation);
    let timestamp_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    EventStore::new(env.clone(), anchor_generation).append(&EventRecord {
        timestamp_unix,
        project_id: project_id.to_string(),
        environment: environment.to_string(),
        generation: Some(generation),
        deployment_id: None,
        event_type: event_type.to_string(),
        reason,
    })?;
    Ok(())
}

fn attempt_cleanup<RtD, RtR>(
    docker: &mut RtD,
    routing: &mut RtR,
    env: &EnvironmentPaths,
    project_id: &str,
    environment: &str,
    generation: u64,
    cleanup: CleanupRecord,
    deployment_id: Option<String>,
    success_event: &str,
    tombstone_event: &str,
) -> Result<CleanupRecord, ConvergenceError>
where
    RtD: DockerRuntime,
    RtR: RoutingRuntime,
{
    let mut container_removed = cleanup.container_removed;
    if let Some(container_name) = cleanup.container_name.as_deref() {
        if !container_removed {
            let _ = docker.stop_container(container_name);
            container_removed = docker.remove_container(container_name).is_ok();
        }
    } else {
        container_removed = true;
    }

    let mut route_removed = cleanup.route_removed;
    if let Some(subtree_id) = cleanup.route_subtree_id.as_deref() {
        if !route_removed {
            route_removed = routing.remove_route(subtree_id).is_ok();
        }
    } else {
        route_removed = true;
    }

    let mut image_removed = cleanup.image_removed;
    let image_ref = cleanup.image_ref.clone();
    if let Some(image_ref) = cleanup.image_ref.as_deref() {
        if !image_removed {
            image_removed = docker.remove_image(image_ref).is_ok();
        }
    } else {
        image_removed = true;
    }

    let cleanup = CleanupRecord::new(
        cleanup.failure_reason,
        cleanup.container_name,
        cleanup.route_subtree_id,
        container_removed,
        route_removed,
        !(container_removed && route_removed && image_removed),
    );
    let cleanup = CleanupRecord {
        image_ref,
        image_removed,
        ..cleanup
    };
    persist_cleanup_state(env, generation, cleanup.clone(), deployment_id.clone())?;
    append_cleanup_event(
        env,
        project_id,
        environment,
        generation,
        deployment_id,
        if cleanup.tombstoned {
            tombstone_event
        } else {
            success_event
        },
        Some(cleanup.failure_reason.clone()),
    )?;
    Ok(cleanup)
}

fn retry_tombstoned_cleanup<RtD, RtR>(
    docker: &mut RtD,
    routing: &mut RtR,
    project_id: &str,
    environment: &str,
    env: &EnvironmentPaths,
    active_record: Option<&DeploymentRecord>,
    managed_containers: &[ContainerInspection],
    managed_images: &[ManagedImage],
    managed_routes: &[RouteInspection],
    attempted_cleanup: &mut BTreeSet<(String, String, u64)>,
) -> Result<(), ConvergenceError>
where
    RtD: DockerRuntime,
    RtR: RoutingRuntime,
{
    if !env.generations_dir().exists() {
        return Ok(());
    }
    let references = environment_runtime_references(
        env,
        project_id,
        environment,
        active_record,
        managed_containers,
        managed_routes,
    )?;
    let mut generations = Vec::new();
    for entry in fs::read_dir(env.generations_dir())? {
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

    for generation in generations {
        if references.contains(generation) {
            continue;
        }
        let store = CleanupStore::new(env.clone(), generation);
        let Some(cleanup) = store.read_record()? else {
            continue;
        };
        if !cleanup.tombstoned {
            continue;
        }
        let cleanup = hydrate_cleanup_record(
            cleanup,
            project_id,
            environment,
            generation,
            managed_containers,
            managed_images,
        );
        let cleanup = attempt_cleanup(
            docker,
            routing,
            env,
            project_id,
            environment,
            generation,
            cleanup,
            None,
            "CLEANUP_RETRY_SUCCEEDED",
            "CLEANUP_RETRY_TOMBSTONED",
        )?;
        if cleanup.container_removed {
            attempted_cleanup.insert((project_id.to_string(), environment.to_string(), generation));
        }
    }
    Ok(())
}

fn hydrate_cleanup_record(
    mut cleanup: CleanupRecord,
    project_id: &str,
    environment: &str,
    generation: u64,
    managed_containers: &[ContainerInspection],
    managed_images: &[ManagedImage],
) -> CleanupRecord {
    if cleanup.container_name.is_none() {
        cleanup.container_name = Some(generation_container_name(
            environment,
            project_id,
            generation,
        ));
    }
    if cleanup.image_ref.is_none() {
        cleanup.image_ref = image_ref_for_generation(
            project_id,
            environment,
            generation,
            managed_containers,
            managed_images,
        );
    }
    if cleanup.container_name.is_none() {
        cleanup.container_removed = true;
    }
    if cleanup.image_ref.is_none() {
        cleanup.image_removed = true;
    }
    cleanup
}

fn cleanup_orphaned_containers<RtD>(
    docker: &mut RtD,
    storage_root: &std::path::Path,
    active_record: Option<&DeploymentRecord>,
    managed_containers: &[ContainerInspection],
    managed_images: &[ManagedImage],
    managed_routes: &[RouteInspection],
    attempted_cleanup: &mut BTreeSet<(String, String, u64)>,
) -> Result<BTreeSet<String>, ConvergenceError>
where
    RtD: DockerRuntime,
{
    let mut removed = BTreeSet::new();
    for container in managed_containers {
        let Some((project_id, environment, generation)) = container_identity(container) else {
            continue;
        };
        if attempted_cleanup.contains(&(project_id.clone(), environment.clone(), generation)) {
            continue;
        }
        let env = EnvironmentPaths::new(storage_root, &project_id, &environment);
        let references = environment_runtime_references(
            &env,
            &project_id,
            &environment,
            active_record,
            managed_containers,
            managed_routes,
        )?;
        if references.contains(generation) {
            continue;
        }
        let cleanup = attempt_cleanup(
            docker,
            &mut NoopRouteRemover,
            &env,
            &project_id,
            &environment,
            generation,
            CleanupRecord {
                image_ref: image_ref_for_generation(
                    &project_id,
                    &environment,
                    generation,
                    managed_containers,
                    managed_images,
                ),
                image_removed: image_ref_for_generation(
                    &project_id,
                    &environment,
                    generation,
                    managed_containers,
                    managed_images,
                )
                .is_none(),
                ..CleanupRecord::new(
                    "orphaned container cleanup",
                    Some(container.container_name.clone()),
                    None,
                    false,
                    true,
                    true,
                )
            },
            None,
            "ORPHANED_CONTAINER_REMOVED",
            "ORPHANED_CONTAINER_TOMBSTONED",
        )?;
        if cleanup.container_removed {
            removed.insert(container.container_name.clone());
        }
    }
    Ok(removed)
}

struct NoopRouteRemover;

impl RoutingRuntime for NoopRouteRemover {
    fn update_route(
        &mut self,
        _request: RouteUpdateRequest,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        Ok(())
    }

    fn inspect_route(
        &mut self,
        _subtree_id: &str,
    ) -> Result<RouteInspection, crate::runtime::RoutingRuntimeError> {
        Err(crate::runtime::RoutingRuntimeError::InspectionFailed(
            "missing route".into(),
        ))
    }

    fn list_managed_routes(
        &mut self,
    ) -> Result<Vec<RouteInspection>, crate::runtime::RoutingRuntimeError> {
        Ok(Vec::new())
    }

    fn remove_route(
        &mut self,
        _subtree_id: &str,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        Ok(())
    }
}

fn cleanup_orphaned_images<RtD>(
    docker: &mut RtD,
    storage_root: &std::path::Path,
    active_record: Option<&DeploymentRecord>,
    managed_containers: &[ContainerInspection],
    managed_images: &[ManagedImage],
    managed_routes: &[RouteInspection],
    attempted_cleanup: &mut BTreeSet<(String, String, u64)>,
) -> Result<(), ConvergenceError>
where
    RtD: DockerRuntime,
{
    for image in managed_images {
        let Some((project_id, environment, generation)) = image_identity(image) else {
            continue;
        };
        if attempted_cleanup.contains(&(project_id.clone(), environment.clone(), generation)) {
            continue;
        }
        let env = EnvironmentPaths::new(storage_root, &project_id, &environment);
        let references = environment_runtime_references(
            &env,
            &project_id,
            &environment,
            active_record,
            managed_containers,
            managed_routes,
        )?;
        if references.contains(generation) {
            continue;
        }
        let _ = attempt_cleanup(
            docker,
            &mut NoopRouteRemover,
            &env,
            &project_id,
            &environment,
            generation,
            CleanupRecord {
                image_ref: Some(image.image_ref.clone()),
                image_removed: false,
                ..CleanupRecord::new("orphaned image cleanup", None, None, true, true, true)
            },
            None,
            "ORPHANED_IMAGE_REMOVED",
            "ORPHANED_IMAGE_TOMBSTONED",
        )?;
        attempted_cleanup.insert((project_id, environment, generation));
    }
    Ok(())
}

fn enforce_generation_retention<RtD, RtR>(
    docker: &mut RtD,
    routing: &mut RtR,
    project_id: &str,
    environment: &str,
    env: &EnvironmentPaths,
    active_record: Option<&DeploymentRecord>,
    managed_containers: &[ContainerInspection],
    managed_images: &[ManagedImage],
    managed_routes: &[RouteInspection],
    attempted_cleanup: &mut BTreeSet<(String, String, u64)>,
) -> Result<(), ConvergenceError>
where
    RtD: DockerRuntime,
    RtR: RoutingRuntime,
{
    if !env.generations_dir().exists() {
        return Ok(());
    }

    let references = environment_runtime_references(
        env,
        project_id,
        environment,
        active_record,
        managed_containers,
        managed_routes,
    )?;
    let generations = list_generation_numbers(env)?;
    let retained_failed = retained_failed_generations(env, &references, &generations)?;

    for generation in generations {
        if references.contains(generation) || retained_failed.contains(&generation) {
            continue;
        }
        let cleanup = retention_cleanup_generation(
            docker,
            routing,
            env,
            &references,
            project_id,
            environment,
            generation,
            managed_containers,
            managed_images,
        )?;
        if cleanup.container_removed {
            attempted_cleanup.insert((project_id.to_string(), environment.to_string(), generation));
        }
    }
    Ok(())
}

fn retained_failed_generations(
    env: &EnvironmentPaths,
    references: &RuntimeReferences,
    generations: &[u64],
) -> Result<BTreeSet<u64>, ConvergenceError> {
    let mut retained = BTreeSet::new();
    for generation in generations.iter().rev().copied() {
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

fn generation_has_failure_diagnostics(
    env: &EnvironmentPaths,
    generation: u64,
) -> Result<bool, ConvergenceError> {
    let diagnostics = env.generation_dir(generation).join("diagnostics");
    let summary_path = diagnostics.join("summary.json");
    if summary_path.exists() {
        let raw = fs::read_to_string(summary_path)?;
        let summary: DiagnosticSummary = serde_json::from_str(&raw).map_err(|err| {
            ConvergenceError::Storage(crate::storage::StorageError::Io(std::io::Error::new(
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

fn list_generation_numbers(env: &EnvironmentPaths) -> Result<Vec<u64>, ConvergenceError> {
    let mut generations = Vec::new();
    for entry in fs::read_dir(env.generations_dir())? {
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

fn retention_cleanup_generation<RtD, RtR>(
    docker: &mut RtD,
    routing: &mut RtR,
    env: &EnvironmentPaths,
    references: &RuntimeReferences,
    project_id: &str,
    environment: &str,
    generation: u64,
    managed_containers: &[ContainerInspection],
    managed_images: &[ManagedImage],
) -> Result<CleanupRecord, ConvergenceError>
where
    RtD: DockerRuntime,
    RtR: RoutingRuntime,
{
    let runtime_info = match load_generation_runtime_info(env, generation) {
        Ok(value) => value,
        Err(crate::storage::StorageError::Io(err))
            if err.kind() == std::io::ErrorKind::InvalidData =>
        {
            None
        }
        Err(err) => return Err(err.into()),
    };
    let build_info = match load_generation_build_info(env, generation) {
        Ok(value) => value,
        Err(crate::storage::StorageError::Io(err))
            if err.kind() == std::io::ErrorKind::InvalidData =>
        {
            None
        }
        Err(err) => return Err(err.into()),
    };
    let route_subtree_id = runtime_info.as_ref().and_then(retention_route_subtree_id);
    let existing_cleanup = CleanupStore::new(env.clone(), generation).read_record()?;
    let mut cleanup = existing_cleanup
        .clone()
        .unwrap_or_else(|| CleanupRecord::new("retention cleanup", None, None, true, true, false));
    if cleanup.container_name.is_none() {
        cleanup.container_name = runtime_info
            .as_ref()
            .map(|info| info.container_name.clone())
            .or_else(|| {
                container_for_generation(managed_containers, project_id, environment, generation)
                    .map(|container| container.container_name.clone())
            })
            .or_else(|| {
                Some(generation_container_name(
                    environment,
                    project_id,
                    generation,
                ))
            });
    }
    if cleanup.route_subtree_id.is_none() {
        cleanup.route_subtree_id = route_subtree_id;
    }
    if cleanup.image_ref.is_none() {
        cleanup.image_ref = build_info
            .as_ref()
            .map(|info| info.image_ref.clone())
            .or_else(|| {
                image_ref_for_generation(
                    project_id,
                    environment,
                    generation,
                    managed_containers,
                    managed_images,
                )
            });
    }
    if existing_cleanup.is_none() {
        cleanup.container_removed = cleanup.container_name.is_none();
        cleanup.route_removed = cleanup.route_subtree_id.is_none();
        cleanup.image_removed = cleanup.image_ref.is_none();
    }
    let cleanup = attempt_cleanup(
        docker,
        routing,
        env,
        project_id,
        environment,
        generation,
        cleanup,
        None,
        "RETENTION_RUNTIME_ARTIFACTS_REMOVED",
        "RETENTION_RUNTIME_ARTIFACTS_TOMBSTONED",
    )?;
    if cleanup.tombstoned {
        append_retention_event(
            env,
            references,
            project_id,
            environment,
            generation,
            "GENERATION_RETENTION_TOMBSTONED",
            Some(cleanup.failure_reason.clone()),
        )?;
        return Ok(cleanup);
    }

    match fs::remove_dir_all(env.generation_dir(generation)) {
        Ok(()) => append_retention_event(
            env,
            references,
            project_id,
            environment,
            generation,
            "GENERATION_RETENTION_REMOVED",
            Some("retention cleanup".into()),
        )?,
        Err(err) => {
            let cleanup = CleanupRecord {
                failure_reason: format!("retention directory removal failed: {err}"),
                tombstoned: true,
                ..cleanup
            };
            persist_cleanup_state(env, generation, cleanup.clone(), None)?;
            append_retention_event(
                env,
                references,
                project_id,
                environment,
                generation,
                "GENERATION_RETENTION_TOMBSTONED",
                Some(cleanup.failure_reason.clone()),
            )?;
            return Ok(cleanup);
        }
    }
    Ok(cleanup)
}

fn retention_route_subtree_id(runtime_info: &PersistedRuntimeInfo) -> Option<String> {
    match runtime_info.activation.as_ref() {
        Some(PersistedActivationMode::Http {
            route_subtree_id, ..
        }) => route_subtree_id.clone(),
        _ => None,
    }
}

fn cleanup_generation_for_route(
    env: &EnvironmentPaths,
    route: &RouteInspection,
    references: &RuntimeReferences,
) -> Option<u64> {
    resolve_generation_from_target(&route.active_target, &[])
        .or_else(|| parse_generation_from_target(&route.active_target))
        .or(references.route_generation)
        .or(references.current)
        .or(references.previous)
        .or(references.converging_generation)
        .or_else(|| latest_nonfinalized_generation(env).ok().flatten())
}

fn route_has_valid_backing(
    route: &RouteInspection,
    project_id: &str,
    environment: &str,
    env: &EnvironmentPaths,
    references: &RuntimeReferences,
    managed_containers: &[ContainerInspection],
    removed_containers: &BTreeSet<String>,
) -> bool {
    let Some(generation) = resolve_generation_from_target(&route.active_target, managed_containers)
        .or_else(|| parse_generation_from_target(&route.active_target))
    else {
        return false;
    };
    if !references.contains(generation) {
        return false;
    }
    if !generation_artifacts_exist(env, generation) {
        return false;
    }
    managed_containers.iter().any(|container| {
        !removed_containers.contains(&container.container_name)
            && container_identity(container)
                == Some((project_id.to_string(), environment.to_string(), generation))
    })
}

fn generation_artifacts_exist(env: &EnvironmentPaths, generation: u64) -> bool {
    let generation_dir = env.generation_dir(generation);
    generation_dir.join("snapshot.json").exists()
        || generation_dir.join("runtime.json").exists()
        || generation_dir.join("build.json").exists()
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

    environments.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
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
    let environment =
        resolve_recovery_environment(storage_root, project_id, environment, environment_variables)?;
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
                return Err(ConvergenceError::Storage(crate::storage::StorageError::Io(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unsupported secret scope {other}"),
                    ),
                )));
            }
        }
    }
    Ok(resolved)
}

fn load_environment_domain(
    storage_root: &std::path::Path,
    project_id: &str,
    environment: &str,
) -> Result<Option<String>, ConvergenceError> {
    let project = ProjectRegistryStore::new(storage_root)
        .get(project_id)
        .map_err(|err| {
            ConvergenceError::Storage(crate::storage::StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("project lookup failed for {project_id}: {err}"),
            )))
        })?;
    Ok(project.map(|project| derive_environment_domain(&project.base_domain, environment)))
}

fn ensure_http_route_matches_generation<RtR: RoutingRuntime>(
    routing: &mut RtR,
    subtree_id: &str,
    inspection: &ContainerInspection,
    internal_port: u16,
    preferred_network: Option<&str>,
    domain: Option<String>,
    target_source: &PersistedRouteTargetSource,
    probe_path: Option<String>,
) -> Result<(), ConvergenceError> {
    let target = resolve_route_target(inspection, internal_port, preferred_network, target_source)
        .ok_or_else(|| {
            let message = preferred_network.map_or_else(
                || "container missing network IP".to_string(),
                |network_name| format!("container missing IP on docker network {network_name}"),
            );
            ConvergenceError::Docker(crate::runtime::DockerRuntimeError::InvalidResponse(message))
        })?;
    let route_matches = routing
        .inspect_route(subtree_id)
        .map(|route| {
            route.active_target == target
                && route.domain == domain
                && route.activation_verified
                && !route.health_checks_enabled
        })
        .unwrap_or(false);
    if route_matches {
        return Ok(());
    }

    routing.update_route(RouteUpdateRequest {
        subtree_id: subtree_id.to_string(),
        target: target.clone(),
        domain: domain.clone(),
        health_checks_enabled: false,
        probe_path,
    })?;
    let route = routing.inspect_route(subtree_id)?;
    if route.active_target != target
        || route.domain != domain
        || !route.activation_verified
        || route.health_checks_enabled
    {
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
#[derive(Default)]
struct TestDockerRuntime {
    containers: std::collections::BTreeMap<String, bool>,
    images: std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    network_ips: std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    remove_failures: std::collections::BTreeMap<String, usize>,
    image_remove_failures: std::collections::BTreeMap<String, usize>,
    build_calls: Vec<String>,
    create_calls: Vec<String>,
    start_calls: Vec<String>,
    stop_calls: Vec<String>,
    remove_calls: Vec<String>,
    image_remove_calls: Vec<String>,
}

#[cfg(test)]
impl TestDockerRuntime {
    fn inspection_network_ips(
        &self,
        container_name: &str,
    ) -> std::collections::BTreeMap<String, String> {
        self.network_ips
            .get(container_name)
            .cloned()
            .unwrap_or_else(|| {
                std::collections::BTreeMap::from([(
                    "forge-test".into(),
                    test_container_ip(container_name),
                )])
            })
    }

    fn seed_image(
        &mut self,
        project_id: &str,
        environment: &str,
        generation: u64,
        image_ref: &str,
    ) {
        self.images.insert(
            image_ref.into(),
            std::collections::BTreeMap::from([
                ("forge.managed".into(), "true".into()),
                ("forge.project_id".into(), project_id.into()),
                ("forge.environment".into(), environment.into()),
                ("forge.generation".into(), generation.to_string()),
            ]),
        );
    }
}

#[cfg(test)]
impl DockerRuntime for TestDockerRuntime {
    fn build_image(
        &mut self,
        request: crate::runtime::BuildImageRequest,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        self.build_calls.push(request.image_tag.clone());
        self.images
            .insert(request.image_tag.clone(), request.labels.clone());
        Ok(request.image_tag)
    }

    fn ensure_network(
        &mut self,
        _network_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
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
            state_status: if running {
                "running".into()
            } else {
                "exited".into()
            },
            exit_code: if running { Some(0) } else { Some(1) },
            started_at: None,
            image_ref: test_image_ref(container_name),
            labels: Default::default(),
            network_ips: self.inspection_network_ips(container_name),
            restart_policy: "no".into(),
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
    ) -> Result<Vec<ContainerInspection>, crate::runtime::DockerRuntimeError> {
        Ok(self
            .containers
            .iter()
            .map(|(container_name, running)| ContainerInspection {
                container_name: container_name.clone(),
                running: *running,
                state_status: if *running {
                    "running".into()
                } else {
                    "exited".into()
                },
                exit_code: if *running { Some(0) } else { Some(1) },
                started_at: None,
                image_ref: test_image_ref(container_name),
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
                network_ips: self.inspection_network_ips(container_name),
                restart_policy: "no".into(),
            })
            .collect())
    }

    fn list_managed_images(
        &mut self,
    ) -> Result<Vec<ManagedImage>, crate::runtime::DockerRuntimeError> {
        Ok(self
            .images
            .iter()
            .map(|(image_ref, labels)| ManagedImage {
                image_ref: image_ref.clone(),
                labels: labels.clone(),
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
        if let Some(remaining) = self.remove_failures.get_mut(container_name) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(crate::runtime::DockerRuntimeError::CommandFailed(
                    "forced remove failure".into(),
                ));
            }
        }
        self.containers.remove(container_name);
        Ok(())
    }

    fn remove_image(&mut self, image_ref: &str) -> Result<(), crate::runtime::DockerRuntimeError> {
        self.image_remove_calls.push(image_ref.into());
        if let Some(remaining) = self.image_remove_failures.get_mut(image_ref) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(crate::runtime::DockerRuntimeError::CommandFailed(
                    "forced image remove failure".into(),
                ));
            }
        }
        self.images.remove(image_ref);
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
fn test_image_ref(container_name: &str) -> String {
    let generation = container_name
        .rsplit("-gen-")
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    format!("forge/api:production-gen-{generation}")
}

#[cfg(test)]
#[derive(Default)]
struct TestRoutingRuntime {
    route: Option<RouteInspection>,
    remove_failures: std::collections::BTreeMap<String, usize>,
    remove_calls: Vec<String>,
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
            domain: request.domain.clone(),
            activation_verified: true,
            verification_url: None,
            verification_host: None,
            verification_status_code: None,
            verification_response_body: None,
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
        subtree_id: &str,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        self.remove_calls.push(subtree_id.into());
        if let Some(remaining) = self.remove_failures.get_mut(subtree_id) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(crate::runtime::RoutingRuntimeError::UpdateFailed(
                    "forced route remove failure".into(),
                ));
            }
        }
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
            "{{\n  \"container_name\": \"prod-api-gen-{generation}\",\n  \"running\": true,\n  \"network_name\": \"forge-test\",\n  \"probe_path\": \"/health\",\n  \"activation\": {{ \"Http\": {{ \"internal_port\": 3000, \"route_subtree_id\": \"forge:api:production\", \"target_source\": \"ContainerIp\" }} }},\n  \"environment_variables\": {{}}\n}}\n"
        )
        .as_bytes(),
    )
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
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
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
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
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

    #[test]
    fn orphaned_container_is_removed() {
        let root = test_root("orphaned-container-removed");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
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
        assert!(
            docker
                .remove_calls
                .iter()
                .any(|name| name == "prod-api-gen-2")
        );
        assert!(!env.generation_dir(2).exists());
    }

    #[test]
    fn orphaned_container_cleanup_emits_event() {
        let root = test_root("orphaned-container-event");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
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
        let events = EventStore::list_all(&root).unwrap();
        assert!(events.iter().any(|event| {
            event.project_id == "api"
                && event.environment == "production"
                && event.generation == Some(2)
                && event.event_type == "GENERATION_RETENTION_REMOVED"
                && event.reason.as_deref() == Some("retention cleanup")
        }));
    }

    #[test]
    fn orphaned_route_is_removed() {
        let root = test_root("orphaned-route-removed");
        setup_recoverable_http_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            route: Some(RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "prod-api-gen-2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
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

        assert_eq!(outcome, TickOutcome::Healthy(1));
        assert_eq!(
            routing.remove_calls,
            vec!["forge:api:production".to_string()]
        );
        let cleanup = CleanupStore::new(env.clone(), 2)
            .read_record()
            .unwrap()
            .unwrap();
        assert!(cleanup.route_removed);
        assert!(!cleanup.tombstoned);
    }

    #[test]
    fn orphaned_route_cleanup_emits_event() {
        let root = test_root("orphaned-route-event");
        setup_recoverable_http_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            route: Some(RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "prod-api-gen-2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
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

        assert_eq!(outcome, TickOutcome::Healthy(1));
        let events = EventStore::list_all(&root).unwrap();
        assert!(events.iter().any(|event| {
            event.project_id == "api"
                && event.environment == "production"
                && event.generation == Some(2)
                && event.event_type == "ORPHANED_ROUTE_REMOVED"
                && event.reason.as_deref() == Some("orphaned route cleanup")
        }));
    }

    #[test]
    fn cleanup_failure_is_tombstoned() {
        let root = test_root("cleanup-failure-tombstoned");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        fs::create_dir_all(env.generation_dir(2)).unwrap();
        crate::storage::atomic_write(env.generation_dir(2).join("runtime.json"), b"{}\n").unwrap();
        CleanupStore::new(env.clone(), 2)
            .write_record(&CleanupRecord::new(
                "forced cleanup retry",
                Some("prod-api-gen-2".into()),
                None,
                false,
                true,
                true,
            ))
            .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.remove_failures.insert("prod-api-gen-2".into(), 1);
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
        assert!(
            !env.generation_dir(2).exists() || env.generation_dir(2).join("tombstone").exists()
        );
    }

    #[test]
    fn cleanup_retry_eventually_succeeds() {
        let root = test_root("cleanup-retry-succeeds");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        fs::create_dir_all(env.generation_dir(2)).unwrap();
        crate::storage::atomic_write(env.generation_dir(2).join("runtime.json"), b"{}\n").unwrap();
        CleanupStore::new(env.clone(), 2)
            .write_record(&CleanupRecord::new(
                "forced cleanup retry",
                Some("prod-api-gen-2".into()),
                None,
                false,
                true,
                true,
            ))
            .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.remove_failures.insert("prod-api-gen-2".into(), 1);
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
        let _ = engine
            .tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: 101,
                truth: ActiveTruth::Direct,
                http_health_path: None,
            })
            .unwrap();

        assert!(!env.generation_dir(2).exists());
    }

    #[test]
    fn convergence_recovers_from_partial_cleanup_failure() {
        let root = test_root("partial-cleanup-recovery");
        setup_recoverable_http_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        fs::create_dir_all(env.generation_dir(2)).unwrap();
        CleanupStore::new(env.clone(), 2)
            .write_record(&CleanupRecord::new(
                "partial cleanup retry",
                None,
                Some("forge:api:production".into()),
                true,
                false,
                true,
            ))
            .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            route: Some(RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "prod-api-gen-2:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: std::collections::BTreeMap::from([("forge:api:production".into(), 2)]),
            remove_calls: Vec::new(),
            updates: Vec::new(),
        };
        let mut engine =
            ConvergenceEngine::new(&root, &queue, &mut docker, &mut probes, &mut routing);

        assert_eq!(
            engine
                .tick(TickInput {
                    project_id: "api".into(),
                    environment: "production".into(),
                    now_unix: 100,
                    truth: ActiveTruth::HttpRouted {
                        internal_port: 3000,
                    },
                    http_health_path: Some("/health".into()),
                })
                .unwrap(),
            TickOutcome::Healthy(1)
        );
        assert!(
            CleanupStore::new(env.clone(), 2)
                .read_record()
                .unwrap()
                .unwrap()
                .tombstoned
        );

        assert_eq!(
            engine
                .tick(TickInput {
                    project_id: "api".into(),
                    environment: "production".into(),
                    now_unix: 101,
                    truth: ActiveTruth::HttpRouted {
                        internal_port: 3000,
                    },
                    http_health_path: Some("/health".into()),
                })
                .unwrap(),
            TickOutcome::Healthy(1)
        );
        assert_eq!(
            engine
                .tick(TickInput {
                    project_id: "api".into(),
                    environment: "production".into(),
                    now_unix: 102,
                    truth: ActiveTruth::HttpRouted {
                        internal_port: 3000,
                    },
                    http_health_path: Some("/health".into()),
                })
                .unwrap(),
            TickOutcome::Healthy(1)
        );
        assert!(
            !env.generation_dir(2).exists()
                || !CleanupStore::new(env.clone(), 2)
                    .read_record()
                    .unwrap()
                    .unwrap()
                    .tombstoned
        );
        let events = EventStore::list_all(&root).unwrap();
        assert!(events.iter().any(|event| {
            event.event_type == "CLEANUP_RETRY_SUCCEEDED"
                || event.event_type == "GENERATION_RETENTION_REMOVED"
        }));
    }
}

#[cfg(test)]
pub mod bounded_generation_retention {
    use super::*;

    #[test]
    fn retention_preserves_current_and_previous() {
        let root = test_root("retention-preserves-current-previous");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(2).unwrap();
        pointers.swap_current(3).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
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

        assert_eq!(outcome, TickOutcome::Healthy(3));
        assert!(env.generation_dir(2).exists());
        assert!(env.generation_dir(3).exists());
        assert!(!env.generation_dir(1).exists());
    }

    #[test]
    fn retention_removes_old_unreferenced_generations() {
        let root = test_root("retention-removes-old-unreferenced");
        for generation in 1..=5 {
            setup_active_generation(&root, generation);
        }
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(4).unwrap();
        pointers.swap_current(5).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        for generation in 1..=5 {
            docker
                .containers
                .insert(format!("prod-api-gen-{generation}"), true);
        }
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

        for generation in 1..=3 {
            assert!(!env.generation_dir(generation).exists());
        }
        assert!(env.generation_dir(4).exists());
        assert!(env.generation_dir(5).exists());
    }

    #[test]
    fn retention_does_not_delete_rollback_target() {
        let root = test_root("retention-does-not-delete-rollback-target");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(2).unwrap();
        pointers.swap_current(3).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
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

        assert_eq!(pointers.read_pointer("previous").unwrap(), Some(2));
        assert!(env.generation_dir(2).exists());
    }

    #[test]
    fn retention_cleanup_failure_is_retried() {
        let root = test_root("retention-cleanup-failure-is-retried");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(2).unwrap();
        pointers.swap_current(3).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
        docker
            .image_remove_failures
            .insert("forge/api:production-gen-1".into(), 2);
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
        let cleanup = CleanupStore::new(env.clone(), 1)
            .read_record()
            .unwrap()
            .unwrap();
        assert!(cleanup.tombstoned);

        let _ = engine
            .tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: 101,
                truth: ActiveTruth::Direct,
                http_health_path: None,
            })
            .unwrap();
        let cleanup = CleanupStore::new(env.clone(), 1).read_record().unwrap();
        if let Some(cleanup) = cleanup {
            assert!(!cleanup.tombstoned);
        }

        let _ = engine
            .tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: 102,
                truth: ActiveTruth::Direct,
                http_health_path: None,
            })
            .unwrap();
        let _ = engine
            .tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: 103,
                truth: ActiveTruth::Direct,
                http_health_path: None,
            })
            .unwrap();

        assert!(!env.generation_dir(1).exists());
        let events = EventStore::list_all(&root).unwrap();
        assert!(events.iter().any(|event| {
            event.generation == Some(1) && event.event_type == "GENERATION_RETENTION_TOMBSTONED"
        }));
    }

    #[test]
    fn retention_removes_old_runtime_artifacts() {
        let root = test_root("retention-removes-old-runtime-artifacts");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(2).unwrap();
        pointers.swap_current(3).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
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
                .any(|name| name == "prod-api-gen-1")
        );
        assert!(
            docker
                .image_remove_calls
                .iter()
                .any(|image| image == "forge/api:production-gen-1")
        );
        assert!(!env.generation_dir(1).exists());
    }

    #[test]
    fn retention_removes_image_when_build_metadata_is_missing() {
        let root = test_root("retention-removes-image-missing-build-metadata");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(2).unwrap();
        pointers.swap_current(3).unwrap();
        fs::remove_file(env.generation_dir(1).join("build.json")).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        for generation in 1..=3 {
            docker
                .containers
                .insert(format!("prod-api-gen-{generation}"), true);
            docker.seed_image(
                "api",
                "production",
                generation,
                &format!("forge/api:production-gen-{generation}"),
            );
        }
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
                .image_remove_calls
                .iter()
                .any(|image| image == "forge/api:production-gen-1")
        );
        assert!(!env.generation_dir(1).exists());
    }

    #[test]
    fn orphaned_runtime_artifacts_are_removed_after_generation_metadata_deletion() {
        let root = test_root("orphaned-runtime-artifacts-after-metadata-deletion");
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(2).unwrap();
        pointers.swap_current(3).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
        docker.seed_image("api", "production", 1, "forge/api:production-gen-1");
        docker.seed_image("api", "production", 2, "forge/api:production-gen-2");
        docker.seed_image("api", "production", 3, "forge/api:production-gen-3");
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
                .any(|name| name == "prod-api-gen-1")
        );
        assert!(
            docker
                .image_remove_calls
                .iter()
                .any(|image| image == "forge/api:production-gen-1")
        );
        assert!(!env.generation_dir(1).join("snapshot.json").exists());
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
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
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
    fn startup_recovery_restores_missing_route_for_current_generation() {
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

    #[test]
    fn startup_recovery_uses_persisted_execution_network_for_ip_route_targets() {
        let root = test_root("startup-recover-current-http-network");
        setup_recoverable_http_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.network_ips.insert(
            "prod-api-gen-1".into(),
            std::collections::BTreeMap::from([
                ("bridge".into(), "172.17.0.5".into()),
                ("forge-test".into(), "172.19.0.11".into()),
            ]),
        );
        let mut routing = TestRoutingRuntime::default();

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert_eq!(
            routing
                .route
                .as_ref()
                .map(|route| route.active_target.clone()),
            Some("172.19.0.11:3000".into())
        );
    }

    #[test]
    fn convergence_rewrites_legacy_unmatched_route_when_project_domain_known() {
        let root = test_root("startup-recover-rewrites-legacy-route");
        setup_recoverable_http_generation(&root, 1);
        register_project(&root, "api", "api.example.com");
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        let mut routing = TestRoutingRuntime {
            route: Some(RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.19.0.11:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
            updates: Vec::new(),
        };

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert_eq!(routing.updates.len(), 1);
        assert_eq!(
            routing.updates[0].domain.as_deref(),
            Some("api.example.com")
        );
        assert_eq!(
            routing
                .route
                .as_ref()
                .and_then(|route| route.domain.as_deref()),
            Some("api.example.com")
        );
    }

    #[test]
    fn startup_recovery_removes_nonfinalized_candidate_route_after_container_cleanup() {
        let root = test_root("startup-recover-removes-orphan-route");
        let env = EnvironmentPaths::new(&root, "api", "production");
        env.ensure_exists().unwrap();
        fs::create_dir_all(env.generation_dir(1).join("diagnostics")).unwrap();
        crate::storage::atomic_write(
            env.generation_dir(1).join("build.json"),
            b"{\n  \"deployment_id\": \"d1\",\n  \"image_ref\": \"forge/api:gen-1\"\n}\n",
        )
        .unwrap();
        crate::storage::atomic_write(
            env.generation_dir(1).join("runtime.json"),
            b"{\n  \"container_name\": \"prod-api-gen-1\",\n  \"running\": true\n}\n",
        )
        .unwrap();

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

        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut routing = TestRoutingRuntime {
            route: Some(RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "prod-api-gen-1:3000".into(),
                domain: None,
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
            updates: Vec::new(),
        };

        let outcome = StartupConvergence::new(&root, &queue, &ResumeDecider(false))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert_eq!(
            outcome,
            RecoveryOutcome::Failed(DeploymentRecord {
                deployment_id: "d1".into(),
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            })
        );
        assert_eq!(
            routing.remove_calls,
            vec!["forge:api:production".to_string()]
        );
        let cleanup = CleanupStore::new(env, 1).read_record().unwrap().unwrap();
        assert!(cleanup.container_removed);
        assert!(cleanup.route_removed);
        assert!(!cleanup.tombstoned);
    }
}
