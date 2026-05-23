use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use crate::backups::scan_backup_gc_actions;
use crate::events::EventRecord;
use crate::projects::ProjectRegistryStore;
use crate::queue::{DeploymentRecord, PersistentQueue};
use crate::route_truth::resolve_route_target;
use crate::runtime::{
    ContainerInspection, ContainerRuntimePolicy, ContainerVolumeMount, CreateContainerRequest,
    CreateVolumeRequest, DockerRuntime, ManagedImage, ManagedVolume, ProbeRuntime, RouteInspection,
    RouteUpdateRequest, RoutingRuntime, VolumeMountRequest,
};
use crate::runtime_env::restore_runtime_env;
use crate::secrets::SecretStore;
use crate::status::derive_environment_domain;
#[cfg(test)]
use crate::storage::SnapshotState;
use crate::storage::{
    CleanupRecord, CleanupStore, DiagnosticSummary, DiagnosticsStore, EnvironmentPaths, EventStore,
    GcActionRecord, GcStore, PersistedActivationMode, PersistedBuildInfo,
    PersistedProbeHistoryEntry, PersistedProbeType, PersistedRouteTargetSource,
    PersistedRuntimeInfo, PersistedRuntimePolicy, PersistedSecretReference,
    PersistedServiceRuntimeInfo, PersistedVolumeRetention, PointerStore, ProbeHistoryStore,
    RuntimeHealthState, RuntimeStateStore, StorageError, atomic_write, current_unix_timestamp,
    load_generation_build_info, load_generation_resolved_runtime, load_generation_runtime_info,
    load_generation_snapshot_metadata,
};
use serde::{Deserialize, Serialize};

// Beyond current/previous, retain only a small recent diagnostic tail of failed generations.
const HEALTHY_FINALIZED_RETENTION_LIMIT: usize = 2;
const FAILED_GENERATION_RETENTION_LIMIT: usize = 2;
const MAX_PROBE_HISTORY_ENTRIES: usize = 64;

fn expected_volume_mounts(runtime: &PersistedServiceRuntimeInfo) -> Vec<ContainerVolumeMount> {
    runtime
        .volume_mounts
        .iter()
        .map(|mount| ContainerVolumeMount {
            volume_name: mount.docker_volume_name.clone(),
            mount_path: mount.mount_path.clone(),
        })
        .collect()
}

fn volume_mounts_match(expected: &[ContainerVolumeMount], actual: &[ContainerVolumeMount]) -> bool {
    let normalize = |mounts: &[ContainerVolumeMount]| {
        let mut values = mounts
            .iter()
            .map(|mount| (mount.volume_name.clone(), mount.mount_path.clone()))
            .collect::<Vec<_>>();
        values.sort();
        values
    };
    normalize(expected) == normalize(actual)
}

fn runtime_policy_matches(
    expected: &PersistedRuntimePolicy,
    inspection: &ContainerInspection,
) -> bool {
    let expected_restart_policy =
        crate::storage::normalize_restart_policy_name(&expected.restart_policy);
    let actual_restart_policy =
        crate::storage::normalize_restart_policy_name(&inspection.restart_policy);
    expected.cpu_limit == inspection.cpu_limit
        && expected.memory_limit_mb == inspection.memory_limit_mb
        && expected_restart_policy == actual_restart_policy
        && expected.max_retries
            == crate::deployments::normalize_restart_max_retries(
                &actual_restart_policy,
                inspection.restart_max_retries,
            )
}

fn runtime_policy_as_container(expected: &PersistedRuntimePolicy) -> ContainerRuntimePolicy {
    ContainerRuntimePolicy {
        cpu_limit: expected.cpu_limit.clone(),
        memory_limit_mb: expected.memory_limit_mb,
        restart_policy: crate::storage::normalize_restart_policy_name(&expected.restart_policy),
        max_retries: expected.max_retries,
    }
}

fn ensure_runtime_volumes<RtD: DockerRuntime>(
    project_id: &str,
    environment: &str,
    runtime: &PersistedServiceRuntimeInfo,
    docker: &mut RtD,
) -> Result<(), ConvergenceError> {
    for mount in &runtime.volume_mounts {
        docker.ensure_volume(CreateVolumeRequest {
            volume_name: mount.docker_volume_name.clone(),
            labels: BTreeMap::from([
                ("forge.managed".into(), "true".into()),
                ("forge.project_id".into(), project_id.to_string()),
                ("forge.environment".into(), environment.to_string()),
                ("forge.generation".into(), mount.generation.to_string()),
                ("forge.service_id".into(), mount.service_id.clone()),
                ("forge.volume_id".into(), mount.volume_id.clone()),
                (
                    "forge.volume_retention".into(),
                    match mount.retention {
                        PersistedVolumeRetention::Persistent => "persistent".into(),
                        PersistedVolumeRetention::Ephemeral => "ephemeral".into(),
                    },
                ),
            ]),
        })?;
    }
    Ok(())
}

fn append_probe_history_entry(
    env: &EnvironmentPaths,
    generation: u64,
    probe_type: PersistedProbeType,
    success: bool,
    latency_ms: u64,
    failure_reason: Option<String>,
) -> Result<(), ConvergenceError> {
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

fn clear_resolved_route_repair_state(
    env: &EnvironmentPaths,
    generation: u64,
) -> Result<(), ConvergenceError> {
    let runtime_store = RuntimeStateStore::new(env.clone());
    let mut runtime_state = runtime_store.load()?;
    if runtime_state.active_generation != Some(generation)
        || runtime_state.last_error_code.as_deref() != Some("route_activation_verification_failed")
    {
        return Ok(());
    }
    runtime_state.last_error_code = None;
    runtime_state.health_state = RuntimeHealthState::Healthy;
    runtime_state.degraded_since_unix = None;
    runtime_state.last_transition = "healthy".into();
    runtime_store.save(&runtime_state)?;
    Ok(())
}

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

struct ReconstructedActiveGeneration {
    generation: Option<u64>,
    route_repair_failed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GarbageCollectionReport {
    pub actions: Vec<GcActionRecord>,
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

fn gc_path_error(action: &str, path: &std::path::Path, err: std::io::Error) -> ConvergenceError {
    std::io::Error::new(
        err.kind(),
        format!("failed to {action} {}: {err}", path.display()),
    )
    .into()
}

fn gc_read_dir_optional(
    path: &std::path::Path,
    action: &str,
) -> Result<Option<std::fs::ReadDir>, ConvergenceError> {
    match fs::read_dir(path) {
        Ok(entries) => Ok(Some(entries)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(gc_path_error(action, path, err)),
    }
}

fn gc_read_to_string_optional(
    path: &std::path::Path,
    action: &str,
) -> Result<Option<String>, ConvergenceError> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(gc_path_error(action, path, err)),
    }
}

fn gc_read_pointer_optional(
    env: &EnvironmentPaths,
    name: &str,
) -> Result<Option<u64>, ConvergenceError> {
    let path = env.root.join(name);
    let Some(raw) = gc_read_to_string_optional(&path, "read GC pointer")? else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed
        .parse::<u64>()
        .map(Some)
        .map_err(|_| ConvergenceError::Storage(crate::storage::StorageError::InvalidPointer(path)))
}

pub trait ActiveDeploymentDecider {
    fn should_resume(&self, deployment: &DeploymentRecord) -> bool;
}

pub struct StartupConvergence<'a, D> {
    storage_root: PathBuf,
    queue: &'a PersistentQueue,
    decider: &'a D,
}

struct RouteRepairFailure {
    message: String,
    artifact: serde_json::Value,
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
        repair_missing_previous_generation(&self.storage_root)?;
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
        let mut managed_volumes = docker.list_managed_volumes()?;
        managed_volumes.sort_by(|left, right| left.volume_name.cmp(&right.volume_name));
        let mut managed_routes = routing.list_managed_routes()?;
        managed_routes.sort_by(|left, right| left.subtree_id.cmp(&right.subtree_id));
        let queue_state = self.queue.load_state()?;
        let active_record = resumable_active.or(queue_state.active.as_ref());
        if active_record.is_some() {
            return Ok(());
        }
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
                &managed_volumes,
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
            let pointers = PointerStore::new(env.clone());
            let current_pointer = match pointers.read_pointer("current") {
                Ok(value) => value,
                Err(crate::storage::StorageError::InvalidPointer(_)) => continue,
                Err(err) => return Err(err.into()),
            };
            let authoritative = match pointers.read_authoritative_pointer() {
                Ok(value) => value,
                Err(crate::storage::StorageError::InvalidPointer(_)) => continue,
                Err(err) => return Err(err.into()),
            };
            let route_generation =
                active_http_route_generation(docker, routing, &project_id, &environment)?;
            let selected =
                select_newest_complete_generation(&env, [authoritative, route_generation])?;
            if let Some(generation) = selected {
                if current_pointer != Some(generation) {
                    eprintln!(
                        "forge convergence repaired stale promoted/current pointer for {project_id}/{environment}: {:?} -> {generation}",
                        current_pointer
                    );
                    pointers.swap_current(generation)?;
                }
            }

            let Some(generation) = selected.or(authoritative) else {
                continue;
            };
            if !snapshot_is_finalized(&env, generation) {
                continue;
            }
            if let Err(err) = self.recover_finalized_current_generation_environment(
                docker,
                routing,
                &project_id,
                &environment,
                &env,
                generation,
            ) {
                record_route_repair_degraded_state(
                    &env,
                    &project_id,
                    &environment,
                    generation,
                    None,
                    "",
                    None,
                    "startup_recovery",
                    &err.to_string(),
                    None,
                    true,
                )?;
            }
        }
        Ok(())
    }

    fn recover_finalized_current_generation_environment<RtD, RtR>(
        &self,
        docker: &mut RtD,
        routing: &mut RtR,
        project_id: &str,
        environment: &str,
        env: &EnvironmentPaths,
        generation: u64,
    ) -> Result<(), ConvergenceError>
    where
        RtD: DockerRuntime,
        RtR: RoutingRuntime,
    {
        let Some(runtime_info) = load_generation_runtime_info(env, generation)? else {
            return Ok(());
        };
        let Some(build_info) = load_generation_build_info(env, generation)? else {
            return Ok(());
        };

        let service_runtime = runtime_services(&runtime_info, &build_info.image_ref);
        let mut inspections = BTreeMap::new();
        for (service_id, service) in &service_runtime {
            let inspection = ensure_generation_service_running(
                &self.storage_root,
                project_id,
                environment,
                generation,
                &build_info.deployment_id,
                service,
                docker,
            )?;
            inspections.insert(service_id.clone(), inspection);
        }

        for (service_id, service) in &service_runtime {
            if !service.externally_exposed {
                continue;
            }
            if let Some(route_recovery) = persisted_http_route_recovery(
                &self.storage_root,
                service,
                project_id,
                environment,
                service_runtime.len(),
                Some(service_id.as_str()),
            )? {
                if let Err(err) = ensure_http_route_matches_generation(
                    routing,
                    &route_recovery.subtree_id,
                    inspections.get(service_id).expect("inspection collected"),
                    route_recovery.internal_port,
                    service.network_name.as_deref(),
                    route_recovery.domain,
                    &route_recovery.target_source,
                    route_recovery.probe_path,
                ) {
                    record_route_repair_degraded_state(
                        env,
                        project_id,
                        environment,
                        generation,
                        Some(build_info.deployment_id.clone()),
                        &inspection_container_name(
                            inspections.get(service_id).expect("inspection collected"),
                        ),
                        Some(service_id.as_str()),
                        "startup_recovery",
                        &err.message,
                        Some(&err.artifact),
                        true,
                    )?;
                }
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

        let reconstructed = self.reconstruct_active_generation(&input, &env)?;
        let active_generation = reconstructed.generation;
        let route_repair_failed = reconstructed.route_repair_failed;
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
        let tcp_started = Instant::now();
        let tcp_ok = match self
            .probes
            .probe_tcp(&container_name, input.internal_port())
        {
            Ok(tcp_ok) => {
                append_probe_history_entry(
                    &env,
                    active_generation,
                    PersistedProbeType::Tcp,
                    tcp_ok,
                    tcp_started.elapsed().as_millis() as u64,
                    (!tcp_ok).then(|| "tcp probe returned unhealthy".to_string()),
                )?;
                tcp_ok
            }
            Err(err) => {
                append_probe_history_entry(
                    &env,
                    active_generation,
                    PersistedProbeType::Tcp,
                    false,
                    tcp_started.elapsed().as_millis() as u64,
                    Some(err.to_string()),
                )?;
                return Err(err.into());
            }
        };
        let http_ok = if let Some(path) = &input.http_health_path {
            let http_started = Instant::now();
            match self
                .probes
                .probe_http(&container_name, input.internal_port(), path)
            {
                Ok(http_ok) => {
                    append_probe_history_entry(
                        &env,
                        active_generation,
                        PersistedProbeType::Http,
                        http_ok,
                        http_started.elapsed().as_millis() as u64,
                        (!http_ok)
                            .then(|| format!("http health probe returned unhealthy for {path}")),
                    )?;
                    http_ok
                }
                Err(err) => {
                    append_probe_history_entry(
                        &env,
                        active_generation,
                        PersistedProbeType::Http,
                        false,
                        http_started.elapsed().as_millis() as u64,
                        Some(err.to_string()),
                    )?;
                    return Err(err.into());
                }
            }
        } else {
            true
        };

        if tcp_ok && http_ok && !route_repair_failed {
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

        if route_repair_failed {
            runtime_state.health_state = RuntimeHealthState::Degraded;
            runtime_state.last_transition = "route_repair_failed".into();
            runtime_state.last_error_code = Some("route_activation_verification_failed".into());
            runtime_state
                .degraded_since_unix
                .get_or_insert(input.now_unix);
            RuntimeStateStore::new(env).save(&runtime_state)?;
            return Ok(TickOutcome::Degraded(active_generation));
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
        let mut managed_volumes = self.docker.list_managed_volumes()?;
        managed_volumes.sort_by(|left, right| left.volume_name.cmp(&right.volume_name));
        let mut managed_routes = self.routing.list_managed_routes()?;
        managed_routes.sort_by(|left, right| left.subtree_id.cmp(&right.subtree_id));
        let queue_state = self.queue.load_state()?;
        if queue_state.active.is_some() {
            return Ok(());
        }
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
            &managed_volumes,
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
    ) -> Result<ReconstructedActiveGeneration, ConvergenceError> {
        match input.truth {
            ActiveTruth::HttpRouted { internal_port } => {
                let pointers = PointerStore::new(env.clone());
                let current_pointer = pointers.read_pointer("current")?;
                let authoritative = pointers.read_authoritative_pointer()?;
                let route_generation = self.route_generation(input)?;
                let selected =
                    select_newest_complete_generation(env, [authoritative, route_generation])?;

                let Some(selected_generation) = selected else {
                    return Ok(ReconstructedActiveGeneration {
                        generation: None,
                        route_repair_failed: false,
                    });
                };
                if current_pointer != Some(selected_generation) {
                    eprintln!(
                        "forge convergence rewrote stale promoted/current pointer for {}/{}: {:?} -> {}",
                        input.project_id, input.environment, current_pointer, selected_generation
                    );
                    pointers.swap_current(selected_generation)?;
                }
                let route_was_stale = route_generation != Some(selected_generation);
                let (generation, route_repair_failed) = self.repair_current_http_route(
                    input,
                    env,
                    Some(selected_generation),
                    internal_port,
                )?;
                if generation.is_some() {
                    if route_was_stale {
                        eprintln!(
                            "forge convergence repaired route truth for {}/{} to generation {}",
                            input.project_id, input.environment, selected_generation
                        );
                    }
                    Ok(ReconstructedActiveGeneration {
                        generation: Some(selected_generation),
                        route_repair_failed,
                    })
                } else {
                    Ok(ReconstructedActiveGeneration {
                        generation: None,
                        route_repair_failed,
                    })
                }
            }
            ActiveTruth::Direct => {
                let pointers = PointerStore::new(env.clone());
                let current_pointer = pointers.read_pointer("current")?;
                let authoritative = pointers.read_authoritative_pointer()?;
                let Some(current_generation) = authoritative else {
                    return Ok(ReconstructedActiveGeneration {
                        generation: None,
                        route_repair_failed: false,
                    });
                };
                if !snapshot_is_finalized(env, current_generation) {
                    return Ok(ReconstructedActiveGeneration {
                        generation: None,
                        route_repair_failed: false,
                    });
                }
                let container_name = generation_container_name(
                    &input.environment,
                    &input.project_id,
                    current_generation,
                );
                match self.docker.inspect_container(&container_name) {
                    Ok(ContainerInspection { running: true, .. }) => {
                        if current_pointer != Some(current_generation) {
                            eprintln!(
                                "forge convergence rewrote stale promoted/current pointer for {}/{}: {:?} -> {}",
                                input.project_id,
                                input.environment,
                                current_pointer,
                                current_generation
                            );
                            pointers.swap_current(current_generation)?;
                        }
                        Ok(ReconstructedActiveGeneration {
                            generation: Some(current_generation),
                            route_repair_failed: false,
                        })
                    }
                    _ => Ok(ReconstructedActiveGeneration {
                        generation: None,
                        route_repair_failed: false,
                    }),
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

    fn repair_current_http_route(
        &mut self,
        input: &TickInput,
        env: &EnvironmentPaths,
        current: Option<u64>,
        _internal_port: u16,
    ) -> Result<(Option<u64>, bool), ConvergenceError> {
        let Some(current_generation) = current else {
            return Ok((None, false));
        };
        if !snapshot_is_finalized(env, current_generation) {
            return Ok((None, false));
        }
        let Some(runtime_info) = load_generation_runtime_info(env, current_generation)? else {
            return Ok((None, false));
        };
        let build_info = load_generation_build_info(env, current_generation)?.ok_or_else(|| {
            ConvergenceError::Storage(crate::storage::StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "missing build info",
            )))
        })?;
        let service_runtime = runtime_services(&runtime_info, &build_info.image_ref);
        let mut route_repair_failed = false;
        for (service_id, service) in &service_runtime {
            let inspection = ensure_generation_service_running(
                &self.storage_root,
                &input.project_id,
                &input.environment,
                current_generation,
                &build_info.deployment_id,
                service,
                self.docker,
            )?;
            if !service.externally_exposed {
                continue;
            }
            let route_recovery = persisted_http_route_recovery(
                &self.storage_root,
                service,
                &input.project_id,
                &input.environment,
                service_runtime.len(),
                Some(service_id.as_str()),
            )?
            .ok_or_else(|| {
                ConvergenceError::Storage(crate::storage::StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "missing route metadata",
                )))
            })?;
            if let Err(err) = ensure_http_route_matches_generation(
                self.routing,
                &route_recovery.subtree_id,
                &inspection,
                route_recovery.internal_port,
                service.network_name.as_deref(),
                route_recovery.domain,
                &route_recovery.target_source,
                route_recovery.probe_path,
            ) {
                route_repair_failed = true;
                record_route_repair_degraded_state(
                    env,
                    &input.project_id,
                    &input.environment,
                    current_generation,
                    Some(build_info.deployment_id.clone()),
                    &inspection.container_name,
                    Some(service_id.as_str()),
                    "runtime",
                    &err.message,
                    Some(&err.artifact),
                    false,
                )?;
            }
        }
        if !route_repair_failed {
            clear_resolved_route_repair_state(env, current_generation)?;
        }
        Ok((Some(current_generation), route_repair_failed))
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
                let service_runtime = runtime_services(&runtime_info, &build_info.image_ref);
                for (service_id, service) in &service_runtime {
                    let inspection = ensure_generation_service_running(
                        &self.storage_root,
                        &input.project_id,
                        &input.environment,
                        previous,
                        &build_info.deployment_id,
                        service,
                        self.docker,
                    )?;
                    if !service.externally_exposed {
                        continue;
                    }
                    let route_recovery = persisted_http_route_recovery(
                        &self.storage_root,
                        service,
                        &input.project_id,
                        &input.environment,
                        service_runtime.len(),
                        Some(service_id.as_str()),
                    )?
                    .or_else(|| {
                        http_route_recovery_input(
                            &self.storage_root,
                            Some(service),
                            &input.project_id,
                            &input.environment,
                            internal_port,
                            input.http_health_path.clone(),
                        )
                        .ok()
                    })
                    .ok_or_else(|| {
                        ConvergenceError::Storage(crate::storage::StorageError::Io(
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "missing route metadata",
                            ),
                        ))
                    })?;
                    if let Err(err) = ensure_http_route_matches_generation(
                        self.routing,
                        &route_recovery.subtree_id,
                        &inspection,
                        route_recovery.internal_port,
                        service.network_name.as_deref(),
                        route_recovery.domain,
                        &route_recovery.target_source,
                        route_recovery.probe_path,
                    ) {
                        record_route_repair_degraded_state(
                            env,
                            &input.project_id,
                            &input.environment,
                            previous,
                            Some(build_info.deployment_id.clone()),
                            &inspection.container_name,
                            Some(service_id.as_str()),
                            "runtime",
                            &err.message,
                            Some(&err.artifact),
                            false,
                        )?;
                        return Ok(false);
                    }
                }
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
                let service_runtime = runtime_services(&runtime_info, &build_info.image_ref);
                for service in service_runtime.values() {
                    let _ = ensure_generation_service_running(
                        &self.storage_root,
                        &input.project_id,
                        &input.environment,
                        previous,
                        &build_info.deployment_id,
                        service,
                        self.docker,
                    )?;
                }
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

fn select_newest_complete_generation<I>(
    env: &EnvironmentPaths,
    candidates: I,
) -> Result<Option<u64>, ConvergenceError>
where
    I: IntoIterator<Item = Option<u64>>,
{
    let mut selected = None;
    for generation in candidates.into_iter().flatten() {
        if !generation_has_complete_metadata(env, generation)? {
            continue;
        }
        if selected.is_none_or(|current| generation > current) {
            selected = Some(generation);
        }
    }
    Ok(selected)
}

fn generation_has_complete_metadata(
    env: &EnvironmentPaths,
    generation: u64,
) -> Result<bool, ConvergenceError> {
    if !snapshot_is_finalized(env, generation) {
        return Ok(false);
    }
    let Some(snapshot) = load_generation_snapshot_metadata(env, generation)? else {
        return Ok(false);
    };
    if snapshot.state != "healthy" {
        return Ok(false);
    }
    Ok(load_generation_runtime_info(env, generation)?.is_some()
        && load_generation_build_info(env, generation)?.is_some())
}

fn active_http_route_generation<D, R>(
    docker: &mut D,
    routing: &mut R,
    project_id: &str,
    environment: &str,
) -> Result<Option<u64>, ConvergenceError>
where
    D: DockerRuntime,
    R: RoutingRuntime,
{
    let inspection = match routing.inspect_route(&route_subtree_id(project_id, environment)) {
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
    let containers = docker.list_managed_containers()?;
    Ok(resolve_generation_from_target(
        &inspection.active_target,
        &containers,
    ))
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

fn route_subtree_id_for_service(
    project_id: &str,
    environment: &str,
    service_id: Option<&str>,
    service_count: usize,
) -> String {
    if service_count <= 1 {
        return route_subtree_id(project_id, environment);
    }
    format!(
        "forge:{project_id}:{environment}:{}",
        service_id.unwrap_or("default")
    )
}

fn runtime_services(
    runtime_info: &PersistedRuntimeInfo,
    default_image_ref: &str,
) -> BTreeMap<String, PersistedServiceRuntimeInfo> {
    if !runtime_info.services.is_empty() {
        return runtime_info.services.clone();
    }
    BTreeMap::from([(
        "default".into(),
        PersistedServiceRuntimeInfo {
            service_id: "default".into(),
            container_name: runtime_info.container_name.clone(),
            image_ref: default_image_ref.to_string(),
            running: runtime_info.running,
            state: crate::storage::PersistedServiceState::Healthy,
            network_name: runtime_info.network_name.clone(),
            probe_path: runtime_info.probe_path.clone(),
            activation: runtime_info.activation.clone(),
            command: None,
            runtime_policy: runtime_info.runtime_policy.clone(),
            runtime_usage: runtime_info.runtime_usage.clone(),
            termination: runtime_info.termination.clone(),
            depends_on: Vec::new(),
            required_for_promotion: true,
            externally_exposed: matches!(
                runtime_info.activation,
                Some(PersistedActivationMode::Http { .. })
            ),
            environment_variables: runtime_info.environment_variables.clone(),
            state_config: None,
            volume_mounts: runtime_info.volume_mounts.clone(),
            source_ref: runtime_info.source_ref.clone(),
            repo_url: runtime_info.repo_url.clone(),
            commit_sha: runtime_info.commit_sha.clone(),
            source_path: runtime_info.source_path.clone(),
        },
    )])
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
    runtime_info: &PersistedServiceRuntimeInfo,
    project_id: &str,
    environment: &str,
    service_count: usize,
    service_id: Option<&str>,
) -> Result<Option<HttpRouteRecoveryInput>, ConvergenceError> {
    match &runtime_info.activation {
        Some(PersistedActivationMode::Http {
            internal_port,
            route_subtree_id: persisted_subtree_id,
            target_source,
        }) => Ok(Some(HttpRouteRecoveryInput {
            subtree_id: persisted_subtree_id.clone().unwrap_or_else(|| {
                route_subtree_id_for_service(project_id, environment, service_id, service_count)
            }),
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
    runtime_info: Option<&PersistedServiceRuntimeInfo>,
    project_id: &str,
    environment: &str,
    internal_port: u16,
    probe_path: Option<String>,
) -> Result<HttpRouteRecoveryInput, ConvergenceError> {
    if let Some(runtime_info) = runtime_info {
        if let Some(mut persisted) = persisted_http_route_recovery(
            storage_root,
            runtime_info,
            project_id,
            environment,
            1,
            None,
        )? {
            if persisted.probe_path.is_none() {
                persisted.probe_path = probe_path;
            }
            return Ok(persisted);
        }

        return Ok(HttpRouteRecoveryInput {
            subtree_id: route_subtree_id_for_service(project_id, environment, None, 1),
            internal_port,
            domain: load_environment_domain(storage_root, project_id, environment)?,
            probe_path: runtime_info.probe_path.clone().or(probe_path),
            target_source: PersistedRouteTargetSource::ContainerIp,
        });
    }

    Ok(HttpRouteRecoveryInput {
        subtree_id: route_subtree_id_for_service(project_id, environment, None, 1),
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
        current: gc_read_pointer_optional(env, "current")?,
        previous: gc_read_pointer_optional(env, "previous")?,
        route_generation,
        converging_generation: if active_matches {
            latest_nonfinalized_generation(env)?
        } else {
            None
        },
    })
}

fn latest_nonfinalized_generation(env: &EnvironmentPaths) -> Result<Option<u64>, ConvergenceError> {
    let Some(entries) = gc_read_dir_optional(&env.generations_dir(), "scan generation root")?
    else {
        return Ok(None);
    };
    let mut latest: Option<u64> = None;
    for entry in entries {
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

fn project_environment_paths(
    storage_root: &std::path::Path,
) -> Result<Vec<(String, String, EnvironmentPaths)>, ConvergenceError> {
    let mut environments = Vec::new();
    let projects_root = storage_root.join("projects");
    let Some(projects) = gc_read_dir_optional(&projects_root, "scan projects root")? else {
        return Ok(environments);
    };
    for project in projects {
        let project = project?;
        if !project.file_type()?.is_dir() {
            continue;
        }
        let project_id = project.file_name().to_string_lossy().to_string();
        let envs_root = project.path().join("environments");
        let Some(environments_root) =
            gc_read_dir_optional(&envs_root, "scan project environments root")?
        else {
            continue;
        };
        for environment in environments_root {
            let environment = environment?;
            if !environment.file_type()?.is_dir() {
                continue;
            }
            let env_name = environment.file_name().to_string_lossy().to_string();
            environments.push((
                project_id.clone(),
                env_name.clone(),
                EnvironmentPaths::new(storage_root, &project_id, &env_name),
            ));
        }
    }
    Ok(environments)
}

fn repair_missing_previous_generation(
    storage_root: &std::path::Path,
) -> Result<(), ConvergenceError> {
    for (_, _, env) in project_environment_paths(storage_root)? {
        env.ensure_exists()?;
        let pointers = PointerStore::new(env.clone());
        let Some(previous) = pointers.read_pointer("previous")? else {
            continue;
        };
        if env.generation_dir(previous).join("snapshot.json").exists() {
            continue;
        }
        let current = pointers.read_pointer("current")?;
        let fallback = list_generation_numbers(&env)?
            .into_iter()
            .rev()
            .find(|generation| {
                Some(*generation) != current && snapshot_is_finalized(&env, *generation)
            });
        let contents = fallback
            .map(|generation| format!("{generation}\n"))
            .unwrap_or_else(|| "\n".into());
        atomic_write(env.previous_pointer(), contents.as_bytes())?;
    }
    Ok(())
}

fn gc_action_record(
    project_id: &str,
    environment: &str,
    generation: Option<u64>,
    dry_run: bool,
    subject_kind: Option<&str>,
    subject: Option<String>,
    action: &str,
    reason: &str,
    outcome: &str,
    deleted: Vec<String>,
    protected: Vec<String>,
) -> GcActionRecord {
    GcActionRecord {
        timestamp_unix: current_unix_timestamp(),
        project_id: project_id.to_string(),
        environment: environment.to_string(),
        generation,
        dry_run,
        action: action.to_string(),
        reason: reason.to_string(),
        outcome: outcome.to_string(),
        subject_kind: subject_kind.map(|value| value.to_string()),
        subject,
        deleted,
        protected,
    }
}

fn gc_missing_artifact_action(
    project_id: &str,
    environment: &str,
    generation: Option<u64>,
    dry_run: bool,
    subject_kind: &str,
    subject: String,
    reason: &str,
) -> GcActionRecord {
    gc_action_record(
        project_id,
        environment,
        generation,
        dry_run,
        Some(subject_kind),
        Some(subject),
        "remove",
        reason,
        "skipped_missing",
        Vec::new(),
        Vec::new(),
    )
}

fn gc_missing_root_action(
    project_id: &str,
    environment: &str,
    dry_run: bool,
    root_kind: &str,
    path: &std::path::Path,
) -> GcActionRecord {
    gc_action_record(
        project_id,
        environment,
        None,
        dry_run,
        Some("root"),
        Some(format!("root={}", path.display())),
        "scan",
        &format!("optional GC {root_kind} root is missing"),
        "skipped_missing",
        Vec::new(),
        Vec::new(),
    )
}

fn gc_candidate_generations(env: &EnvironmentPaths) -> Result<Vec<u64>, ConvergenceError> {
    let mut generations = BTreeSet::new();
    if env.generations_dir().exists() {
        generations.extend(list_generation_numbers(env)?);
    }
    for record in crate::storage::RetentionStore::new(env.clone())
        .read()?
        .generations
    {
        generations.insert(record.generation);
    }
    Ok(generations.into_iter().collect())
}

fn gc_retention_reasons(
    references: &RuntimeReferences,
    retained_healthy: &BTreeSet<u64>,
    retained_failed: &BTreeSet<u64>,
    generation: u64,
) -> Vec<String> {
    let mut protected = Vec::new();
    if references.current == Some(generation) {
        protected.push("current/promoted generation".into());
    }
    if references.previous == Some(generation) {
        protected.push("rollback-safe generation".into());
    }
    if references.route_generation == Some(generation) {
        protected.push("route reference".into());
    }
    if references.converging_generation == Some(generation) {
        protected.push("deployment in progress".into());
    }
    if retained_healthy.contains(&generation) {
        protected.push("recent healthy finalized generation".into());
    }
    if retained_failed.contains(&generation) {
        protected.push("recent failed generation with diagnostics".into());
    }
    protected
}

fn gc_generation_reason(
    env: &EnvironmentPaths,
    generation: u64,
) -> Result<String, ConvergenceError> {
    if let Some(snapshot) = load_generation_snapshot_metadata(env, generation)? {
        if snapshot.state == "healthy" {
            return Ok("exceeded healthy retention window".into());
        }
        return Ok("outside failed diagnostic retention window".into());
    }
    if generation_has_failure_diagnostics(env, generation)? {
        return Ok("outside failed diagnostic retention window".into());
    }
    Ok("historical generation is no longer protected".into())
}

pub fn garbage_collect<RtD, RtR>(
    storage_root: &std::path::Path,
    queue: &PersistentQueue,
    docker: &mut RtD,
    routing: &mut RtR,
    dry_run: bool,
) -> Result<GarbageCollectionReport, ConvergenceError>
where
    RtD: DockerRuntime,
    RtR: RoutingRuntime,
{
    let mut actions = Vec::new();
    for (root_kind, path) in [
        ("source-checkouts", storage_root.join("source-checkouts")),
        ("repositories", storage_root.join("repositories")),
    ] {
        if !path.exists() {
            actions.push(gc_missing_root_action("*", "*", dry_run, root_kind, &path));
        }
    }

    let mut managed_containers = docker.list_managed_containers()?;
    managed_containers.sort_by(|left, right| left.container_name.cmp(&right.container_name));
    let mut managed_images = docker.list_managed_images()?;
    managed_images.sort_by(|left, right| left.image_ref.cmp(&right.image_ref));
    let mut managed_volumes = docker.list_managed_volumes()?;
    managed_volumes.sort_by(|left, right| left.volume_name.cmp(&right.volume_name));
    let mut managed_routes = routing.list_managed_routes()?;
    managed_routes.sort_by(|left, right| left.subtree_id.cmp(&right.subtree_id));
    let queue_state = queue.load_state()?;
    if queue_state.active.is_some() {
        actions.push(gc_action_record(
            "*",
            "*",
            None,
            dry_run,
            None,
            Some("Global GC".into()),
            "skip",
            "deployment in progress",
            "protected",
            Vec::new(),
            vec!["active deployment".into()],
        ));
        return Ok(GarbageCollectionReport { actions });
    }

    let environments = cleanup_scan_environments(
        storage_root,
        &managed_containers,
        &managed_images,
        &managed_routes,
    )?;
    let mut reported_images = BTreeSet::new();

    for (project_id, environment, env) in &environments {
        if !env.root.exists() {
            actions.push(gc_missing_root_action(
                project_id,
                environment,
                dry_run,
                "gc metadata",
                &env.root,
            ));
        }
        if !env.generations_dir().exists() {
            actions.push(gc_missing_root_action(
                project_id,
                environment,
                dry_run,
                "project environment generations",
                &env.generations_dir(),
            ));
        }

        let references = environment_runtime_references(
            env,
            project_id,
            environment,
            None,
            &managed_containers,
            &managed_routes,
        )?;
        let generations = gc_candidate_generations(env)?;
        let retained_healthy = retained_healthy_generations(env, &references, &generations)?;
        let retained_failed = retained_failed_generations(env, &references, &generations)?;

        for generation in generations {
            let protected =
                gc_retention_reasons(&references, &retained_healthy, &retained_failed, generation);
            if !protected.is_empty() {
                actions.push(gc_action_record(
                    project_id,
                    environment,
                    Some(generation),
                    dry_run,
                    Some("generation"),
                    Some(format!("Generation {generation}")),
                    "retain",
                    "retained by policy",
                    "protected",
                    Vec::new(),
                    protected,
                ));
                continue;
            }

            let generation_dir = env.generation_dir(generation);
            let reason = gc_generation_reason(env, generation)?;
            if generation_dir.exists() {
                actions.push(gc_action_record(
                    project_id,
                    environment,
                    Some(generation),
                    dry_run,
                    Some("generation"),
                    Some(format!("Generation {generation}")),
                    "gc-eligible",
                    &reason,
                    if dry_run { "would_remove" } else { "planned" },
                    vec![generation_dir.display().to_string()],
                    Vec::new(),
                ));
            } else {
                actions.push(gc_missing_artifact_action(
                    project_id,
                    environment,
                    Some(generation),
                    dry_run,
                    "generation",
                    format!("Generation {generation}"),
                    "generation directory already removed",
                ));
            }

            let build_info = load_generation_build_info(env, generation)?;
            let runtime_info = load_generation_runtime_info(env, generation)?;
            let image_ref = build_info
                .as_ref()
                .map(|build| build.image_ref.clone())
                .or_else(|| {
                    image_ref_for_generation(
                        project_id,
                        environment,
                        generation,
                        &managed_containers,
                        &managed_images,
                    )
                });
            if let Some(image_ref) = image_ref {
                if reported_images.insert(image_ref.clone()) {
                    let image_present = managed_images
                        .iter()
                        .any(|image| image.image_ref == image_ref);
                    actions.push(if image_present {
                        gc_action_record(
                            project_id,
                            environment,
                            Some(generation),
                            dry_run,
                            Some("image"),
                            Some(image_ref.clone()),
                            "remove",
                            "orphaned",
                            if dry_run { "would_remove" } else { "planned" },
                            vec![image_ref],
                            Vec::new(),
                        )
                    } else {
                        gc_missing_artifact_action(
                            project_id,
                            environment,
                            Some(generation),
                            dry_run,
                            "image",
                            image_ref,
                            "orphaned image already removed",
                        )
                    });
                }
            }

            if let Some(source_path) = build_info
                .as_ref()
                .and_then(|build| build.source_path.clone())
            {
                if checkout_is_still_referenced(storage_root, project_id, generation, &source_path)?
                {
                    actions.push(gc_action_record(
                        project_id,
                        environment,
                        Some(generation),
                        dry_run,
                        Some("checkout"),
                        Some(source_path.display().to_string()),
                        "retain",
                        "still referenced by another generation",
                        "protected",
                        Vec::new(),
                        vec!["shared checkout".into()],
                    ));
                } else if source_path.exists() {
                    actions.push(gc_action_record(
                        project_id,
                        environment,
                        Some(generation),
                        dry_run,
                        Some("checkout"),
                        Some(source_path.display().to_string()),
                        "remove",
                        "unreferenced",
                        if dry_run { "would_remove" } else { "planned" },
                        vec![source_path.display().to_string()],
                        Vec::new(),
                    ));
                } else {
                    actions.push(gc_missing_artifact_action(
                        project_id,
                        environment,
                        Some(generation),
                        dry_run,
                        "checkout",
                        source_path.display().to_string(),
                        "unreferenced checkout already removed",
                    ));
                }
            }

            if let Some(runtime_info) = runtime_info.as_ref() {
                for service in runtime_services(runtime_info, "").values() {
                    for mount in &service.volume_mounts {
                        let subject = mount.docker_volume_name.clone();
                        if matches!(mount.retention, PersistedVolumeRetention::Persistent) {
                            actions.push(gc_action_record(
                                project_id,
                                environment,
                                Some(generation),
                                dry_run,
                                Some("volume"),
                                Some(subject),
                                "retain",
                                "persistent state is operator-owned durability",
                                "protected",
                                Vec::new(),
                                vec!["persistent volume".into()],
                            ));
                        } else if managed_volumes
                            .iter()
                            .any(|volume| volume.volume_name == mount.docker_volume_name)
                        {
                            actions.push(gc_action_record(
                                project_id,
                                environment,
                                Some(generation),
                                dry_run,
                                Some("volume"),
                                Some(subject.clone()),
                                "remove",
                                "ephemeral generation-scoped volume",
                                if dry_run { "would_remove" } else { "planned" },
                                vec![subject],
                                Vec::new(),
                            ));
                        } else {
                            actions.push(gc_missing_artifact_action(
                                project_id,
                                environment,
                                Some(generation),
                                dry_run,
                                "volume",
                                subject,
                                "ephemeral volume already removed",
                            ));
                        }
                    }
                }
            }

            let diagnostics_dir = generation_dir.join("diagnostics");
            actions.push(if diagnostics_dir.exists() {
                gc_action_record(
                    project_id,
                    environment,
                    Some(generation),
                    dry_run,
                    Some("diagnostics"),
                    Some(diagnostics_dir.display().to_string()),
                    "remove",
                    "stale",
                    if dry_run { "would_remove" } else { "planned" },
                    vec![diagnostics_dir.display().to_string()],
                    Vec::new(),
                )
            } else {
                gc_missing_artifact_action(
                    project_id,
                    environment,
                    Some(generation),
                    dry_run,
                    "diagnostics",
                    diagnostics_dir.display().to_string(),
                    "stale diagnostics already removed",
                )
            });

            let runtime_snapshot = generation_dir.join("runtime_env_snapshot.json");
            actions.push(if runtime_snapshot.exists() {
                gc_action_record(
                    project_id,
                    environment,
                    Some(generation),
                    dry_run,
                    Some("runtime_snapshot"),
                    Some(runtime_snapshot.display().to_string()),
                    "remove",
                    "stale",
                    if dry_run { "would_remove" } else { "planned" },
                    vec![runtime_snapshot.display().to_string()],
                    Vec::new(),
                )
            } else {
                gc_missing_artifact_action(
                    project_id,
                    environment,
                    Some(generation),
                    dry_run,
                    "runtime_snapshot",
                    runtime_snapshot.display().to_string(),
                    "stale runtime snapshot already removed",
                )
            });
        }
    }

    for (project_id, environment, generation, backup_id, reason) in
        scan_backup_gc_actions(storage_root).map_err(|err| {
            ConvergenceError::Storage(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                err.to_string(),
            )))
        })?
    {
        actions.push(gc_action_record(
            &project_id,
            &environment,
            generation,
            dry_run,
            Some("backup"),
            Some(backup_id),
            "retain",
            &reason,
            "protected",
            Vec::new(),
            vec!["backups are never removed automatically".into()],
        ));
    }

    if !dry_run {
        for action in &actions {
            if action.outcome == "protected" || action.outcome == "skipped_missing" {
                if action.project_id == "*" || action.environment == "*" {
                    continue;
                }
                let env =
                    EnvironmentPaths::new(storage_root, &action.project_id, &action.environment);
                if !env.root.exists() {
                    continue;
                }
                GcStore::new(env).append(action.clone())?;
            }
        }
        let mut attempted_cleanup = BTreeSet::new();
        for (project_id, environment, env) in &environments {
            retry_tombstoned_cleanup(
                docker,
                routing,
                project_id,
                environment,
                env,
                None,
                &managed_containers,
                &managed_images,
                &managed_routes,
                &mut attempted_cleanup,
            )?;
            let _ = cleanup_orphaned_containers(
                docker,
                storage_root,
                None,
                &managed_containers,
                &managed_images,
                &managed_routes,
                &mut attempted_cleanup,
            )?;
            cleanup_orphaned_images(
                docker,
                storage_root,
                None,
                &managed_containers,
                &managed_images,
                &managed_routes,
                &mut attempted_cleanup,
            )?;
            enforce_generation_retention(
                docker,
                routing,
                project_id,
                environment,
                env,
                None,
                &managed_containers,
                &managed_images,
                &managed_volumes,
                &managed_routes,
                &mut attempted_cleanup,
            )?;
        }
    }

    Ok(GarbageCollectionReport { actions })
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
            timestamp: new_record.timestamp,
            timestamp_unix: new_record.timestamp_unix,
            generation_failure_reason: if existing.generation_failure_reason.is_empty() {
                new_record.generation_failure_reason.clone()
            } else {
                format!(
                    "{}, {}",
                    existing.generation_failure_reason, new_record.generation_failure_reason
                )
            },
            failure_reason: new_record.failure_reason.or(existing.failure_reason),
            cleanup_attempted: true,
            cleanup_completed: false,
            removed_containers: existing
                .removed_containers
                .into_iter()
                .chain(new_record.removed_containers)
                .collect(),
            removed_images: existing
                .removed_images
                .into_iter()
                .chain(new_record.removed_images)
                .collect(),
            removed_volumes: existing
                .removed_volumes
                .into_iter()
                .chain(new_record.removed_volumes)
                .collect(),
            skipped: existing
                .skipped
                .into_iter()
                .chain(new_record.skipped)
                .collect(),
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
        failure_reason: merged.generation_failure_reason.clone(),
        blocking_reason: Some(merged.generation_failure_reason.clone()),
        container_name: merged.container_name.clone().unwrap_or_default(),
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
        cleanup_recorded: true,
        dependency_graph_summary: None,
        runtime_env_preview: Vec::new(),
    })?;
    Ok(())
}

fn cleanup_event_reason(cleanup: &CleanupRecord) -> String {
    cleanup
        .failure_reason
        .clone()
        .unwrap_or_else(|| cleanup.generation_failure_reason.clone())
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

fn inspection_container_name(inspection: &ContainerInspection) -> String {
    inspection.container_name.clone()
}

#[allow(clippy::too_many_arguments)]
fn record_route_repair_degraded_state(
    env: &EnvironmentPaths,
    project_id: &str,
    environment: &str,
    generation: u64,
    deployment_id: Option<String>,
    container_name: &str,
    failed_service_name: Option<&str>,
    failure_stage: &str,
    failure_reason: &str,
    route_activation_failure: Option<&serde_json::Value>,
    startup_failure: bool,
) -> Result<(), ConvergenceError> {
    let diagnostics = DiagnosticsStore::new(env.clone(), generation);
    diagnostics.write_failure_reason(failure_reason, &[])?;
    diagnostics.append_log_line(failure_reason, &[])?;
    if let Some(artifact) = route_activation_failure {
        let artifact = serde_json::to_string_pretty(artifact).map_err(|err| {
            ConvergenceError::Storage(crate::storage::StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            )))
        })?;
        diagnostics.write_artifact(
            "route_activation_failure.json",
            &format!("{artifact}\n"),
            &[],
        )?;
    }
    diagnostics.write_summary(&DiagnosticSummary {
        deployment_id: deployment_id.clone(),
        failure_stage: failure_stage.into(),
        failure_reason: failure_reason.into(),
        blocking_reason: Some(failure_reason.into()),
        container_name: container_name.into(),
        failed_service_name: failed_service_name.map(str::to_string),
        blocking_service_name: failed_service_name.map(str::to_string),
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
    })?;

    let runtime_store = RuntimeStateStore::new(env.clone());
    let mut runtime_state = runtime_store.load()?;
    runtime_state.active_generation = Some(generation);
    runtime_state.health_state = RuntimeHealthState::Degraded;
    runtime_state
        .degraded_since_unix
        .get_or_insert(current_unix_timestamp());
    runtime_state.last_transition = if startup_failure {
        "startup_route_repair_failed".into()
    } else {
        "route_repair_failed".into()
    };
    runtime_state.last_error_code = Some("route_activation_verification_failed".into());
    runtime_store.save(&runtime_state)?;

    let events = EventStore::new(env.clone(), generation);
    events.append(&EventRecord {
        timestamp_unix: current_unix_timestamp(),
        project_id: project_id.into(),
        environment: environment.into(),
        generation: Some(generation),
        deployment_id: deployment_id.clone(),
        event_type: "ROUTE_ACTIVATION_VERIFICATION_FAILED".into(),
        reason: Some(failure_reason.into()),
    })?;
    if startup_failure {
        events.append(&EventRecord {
            timestamp_unix: current_unix_timestamp(),
            project_id: project_id.into(),
            environment: environment.into(),
            generation: Some(generation),
            deployment_id,
            event_type: "STARTUP_ROUTE_REPAIR_FAILED".into(),
            reason: Some(failure_reason.into()),
        })?;
    }
    Ok(())
}

fn append_gc_action(
    env: &EnvironmentPaths,
    project_id: &str,
    environment: &str,
    generation: Option<u64>,
    action: &str,
    reason: &str,
    outcome: &str,
    deleted: Vec<String>,
    protected: Vec<String>,
) -> Result<(), ConvergenceError> {
    let subject_kind = match action {
        "ORPHANED_IMAGE_REMOVED" | "ORPHANED_IMAGE_TOMBSTONED" => Some("image"),
        "GENERATION_RETENTION_REMOVED" | "GENERATION_RETENTION_TOMBSTONED" => Some("generation"),
        "RETENTION_RUNTIME_ARTIFACTS_REMOVED" | "RETENTION_RUNTIME_ARTIFACTS_TOMBSTONED" => {
            Some("generation")
        }
        "ORPHANED_CONTAINER_REMOVED"
        | "ORPHANED_CONTAINER_TOMBSTONED"
        | "ORPHANED_ROUTE_REMOVED"
        | "ORPHANED_ROUTE_TOMBSTONED"
        | "CLEANUP_RETRY_SUCCEEDED"
        | "CLEANUP_RETRY_TOMBSTONED" => Some("generation"),
        _ => None,
    };
    let subject = match subject_kind {
        Some("generation") => generation.map(|value| format!("Generation {value}")),
        Some("image") => deleted.first().cloned(),
        _ => None,
    };
    GcStore::new(env.clone()).append(gc_action_record(
        project_id,
        environment,
        generation,
        false,
        subject_kind,
        subject,
        action,
        reason,
        outcome,
        deleted,
        protected,
    ))?;
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
        cleanup.generation_failure_reason,
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
        Some(cleanup_event_reason(&cleanup)),
    )?;
    append_gc_action(
        env,
        project_id,
        environment,
        Some(generation),
        if cleanup.tombstoned {
            tombstone_event
        } else {
            success_event
        },
        &cleanup_event_reason(&cleanup),
        if cleanup.tombstoned {
            "tombstoned"
        } else {
            "removed"
        },
        [
            cleanup
                .container_removed
                .then(|| cleanup.container_name.clone())
                .flatten(),
            cleanup
                .route_removed
                .then(|| cleanup.route_subtree_id.clone())
                .flatten(),
            cleanup
                .image_removed
                .then(|| cleanup.image_ref.clone())
                .flatten(),
        ]
        .into_iter()
        .flatten()
        .collect(),
        Vec::new(),
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
    let Some(entries) = gc_read_dir_optional(&env.generations_dir(), "scan generation root")?
    else {
        return Ok(());
    };
    let references = environment_runtime_references(
        env,
        project_id,
        environment,
        active_record,
        managed_containers,
        managed_routes,
    )?;
    let mut generations = Vec::new();
    for entry in entries {
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
    managed_volumes: &[ManagedVolume],
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
    let retained_healthy = retained_healthy_generations(env, &references, &generations)?;
    let retained_failed = retained_failed_generations(env, &references, &generations)?;

    for generation in generations {
        if references.contains(generation)
            || retained_healthy.contains(&generation)
            || retained_failed.contains(&generation)
        {
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
            managed_volumes,
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

fn retained_healthy_generations(
    env: &EnvironmentPaths,
    references: &RuntimeReferences,
    generations: &[u64],
) -> Result<BTreeSet<u64>, ConvergenceError> {
    let mut retained = BTreeSet::new();
    let authoritative_ceiling = references.current;
    for generation in generations.iter().rev().copied() {
        if authoritative_ceiling.is_some_and(|ceiling| generation > ceiling)
            && !references.contains(generation)
        {
            continue;
        }
        let Some(snapshot) = load_generation_snapshot_metadata(env, generation)? else {
            continue;
        };
        if snapshot.state != "healthy" {
            continue;
        }
        retained.insert(generation);
        if retained.len() >= HEALTHY_FINALIZED_RETENTION_LIMIT {
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
    if let Some(raw) = gc_read_to_string_optional(&summary_path, "read diagnostics summary")? {
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
    let Some(entries) = gc_read_dir_optional(&env.generations_dir(), "scan generation root")?
    else {
        return Ok(generations);
    };
    for entry in entries {
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

fn storage_root_from_env(env: &EnvironmentPaths) -> Option<PathBuf> {
    env.root
        .parent()?
        .parent()?
        .parent()?
        .parent()
        .map(|path| path.to_path_buf())
}

fn checkout_is_still_referenced(
    storage_root: &std::path::Path,
    project_id: &str,
    generation_to_skip: u64,
    checkout_path: &std::path::Path,
) -> Result<bool, ConvergenceError> {
    let environments_root = storage_root
        .join("projects")
        .join(project_id)
        .join("environments");
    let Some(environments) =
        gc_read_dir_optional(&environments_root, "scan project environments root")?
    else {
        return Ok(false);
    };
    for environment in environments {
        let environment = environment?;
        let generations_root = environment.path().join("generations");
        let Some(generations) = gc_read_dir_optional(
            &generations_root,
            "scan project environment generations root",
        )?
        else {
            continue;
        };
        for generation in generations {
            let generation = generation?;
            if !generation.file_type()?.is_dir() {
                continue;
            }
            let Some(candidate_generation) =
                generation.file_name().to_string_lossy().parse::<u64>().ok()
            else {
                continue;
            };
            if candidate_generation == generation_to_skip {
                continue;
            }
            let env = EnvironmentPaths {
                root: environment.path(),
            };
            let referenced = load_generation_build_info(&env, candidate_generation)?
                .and_then(|build| build.source_path)
                .or_else(|| {
                    load_generation_runtime_info(&env, candidate_generation)
                        .ok()
                        .flatten()
                        .and_then(|runtime| runtime.source_path)
                });
            if referenced.as_deref() == Some(checkout_path) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn cleanup_source_checkout_if_unreferenced(
    env: &EnvironmentPaths,
    project_id: &str,
    generation: u64,
    build_info: Option<&PersistedBuildInfo>,
) -> Result<Option<String>, ConvergenceError> {
    let Some(source_path) = build_info.and_then(|build| build.source_path.as_ref()) else {
        return Ok(None);
    };
    let Some(storage_root) = storage_root_from_env(env) else {
        return Ok(None);
    };
    let checkouts_root = storage_root.join("source-checkouts").join(project_id);
    if !source_path.starts_with(&checkouts_root) {
        return Ok(None);
    }
    if checkout_is_still_referenced(&storage_root, project_id, generation, source_path)? {
        return Ok(None);
    }
    match fs::remove_dir_all(source_path) {
        Ok(()) => Ok(Some(source_path.display().to_string())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn cleanup_ephemeral_volumes<RtD: DockerRuntime>(
    docker: &mut RtD,
    runtime_info: Option<&PersistedRuntimeInfo>,
    generation: u64,
    managed_volumes: &[ManagedVolume],
) -> Result<Vec<String>, ConvergenceError> {
    let Some(runtime_info) = runtime_info else {
        return Ok(Vec::new());
    };
    let mut removed = Vec::new();
    for service in runtime_services(runtime_info, "").values() {
        for mount in &service.volume_mounts {
            if mount.generation != generation
                || !matches!(mount.retention, PersistedVolumeRetention::Ephemeral)
            {
                continue;
            }
            if !managed_volumes
                .iter()
                .any(|volume| volume.volume_name == mount.docker_volume_name)
            {
                continue;
            }
            docker.remove_volume(&mount.docker_volume_name)?;
            removed.push(mount.docker_volume_name.clone());
        }
    }
    Ok(removed)
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
    managed_volumes: &[ManagedVolume],
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
            Some(cleanup_event_reason(&cleanup)),
        )?;
        return Ok(cleanup);
    }

    let removed_checkout =
        cleanup_source_checkout_if_unreferenced(env, project_id, generation, build_info.as_ref())?;
    let removed_volumes =
        cleanup_ephemeral_volumes(docker, runtime_info.as_ref(), generation, managed_volumes)?;
    match fs::remove_dir_all(env.generation_dir(generation)) {
        Ok(()) => {
            append_retention_event(
                env,
                references,
                project_id,
                environment,
                generation,
                "GENERATION_RETENTION_REMOVED",
                Some("retention cleanup".into()),
            )?;
            append_gc_action(
                env,
                project_id,
                environment,
                Some(generation),
                "GENERATION_RETENTION_REMOVED",
                "retention cleanup",
                "removed",
                std::iter::once(env.generation_dir(generation).display().to_string())
                    .chain(removed_checkout.into_iter())
                    .chain(removed_volumes.into_iter())
                    .collect(),
                Vec::new(),
            )?;
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            append_gc_action(
                env,
                project_id,
                environment,
                Some(generation),
                "GENERATION_RETENTION_REMOVED",
                "generation directory already removed",
                "skipped_missing",
                vec![env.generation_dir(generation).display().to_string()],
                Vec::new(),
            )?;
        }
        Err(err) => {
            let cleanup = CleanupRecord {
                failure_reason: Some(format!("retention directory removal failed: {err}")),
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
                Some(cleanup_event_reason(&cleanup)),
            )?;
            append_gc_action(
                env,
                project_id,
                environment,
                Some(generation),
                "GENERATION_RETENTION_TOMBSTONED",
                &cleanup_event_reason(&cleanup),
                "tombstoned",
                Vec::new(),
                Vec::new(),
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
    let Some(project_entries) = gc_read_dir_optional(&projects_root, "scan projects root")? else {
        return Ok(Vec::new());
    };

    let mut environments = Vec::new();
    for project_entry in project_entries {
        let project_entry = project_entry?;
        if !project_entry.file_type()?.is_dir() {
            continue;
        }
        let project_id = project_entry.file_name().to_string_lossy().to_string();
        let environments_root = project_entry.path().join("environments");
        let Some(environment_entries) =
            gc_read_dir_optional(&environments_root, "scan project environments root")?
        else {
            continue;
        };
        for environment_entry in environment_entries {
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

fn ensure_generation_service_running<RtD: DockerRuntime>(
    storage_root: &std::path::Path,
    project_id: &str,
    environment: &str,
    generation: u64,
    deployment_id: &str,
    runtime: &PersistedServiceRuntimeInfo,
    docker: &mut RtD,
) -> Result<ContainerInspection, ConvergenceError> {
    let expected_mounts = expected_volume_mounts(runtime);
    match docker.inspect_container(&runtime.container_name) {
        Ok(_) => {
            let inspection = docker.inspect_container(&runtime.container_name)?;
            let policy_drift = !runtime_policy_matches(&runtime.runtime_policy, &inspection);
            let volume_drift = !volume_mounts_match(&expected_mounts, &inspection.volume_mounts);
            if inspection.running && !policy_drift && !volume_drift {
                return Ok(inspection);
            }
            if !policy_drift && !volume_drift {
                docker.start_container(&runtime.container_name)?;
                return Ok(docker.inspect_container(&runtime.container_name)?);
            }
            docker.remove_container(&runtime.container_name)?;
            if policy_drift {
                append_cleanup_event(
                    &EnvironmentPaths::new(storage_root, project_id, environment),
                    project_id,
                    environment,
                    generation,
                    Some(deployment_id.to_string()),
                    "RUNTIME_POLICY_DRIFT_REPAIRED",
                    Some(format!(
                        "recreated container {} to restore runtime policy {:?}",
                        runtime.container_name, runtime.runtime_policy
                    )),
                )?;
            }
            if volume_drift {
                append_cleanup_event(
                    &EnvironmentPaths::new(storage_root, project_id, environment),
                    project_id,
                    environment,
                    generation,
                    Some(deployment_id.to_string()),
                    "VOLUME_ATTACHMENT_REPAIRED",
                    Some(format!(
                        "recreated container {} due to stale volume attachment state",
                        runtime.container_name
                    )),
                )?;
            }
        }
        Err(_) => {}
    }

    ensure_runtime_volumes(project_id, environment, runtime, docker)?;
    let labels = BTreeMap::from([
        ("forge.managed".into(), "true".into()),
        ("forge.project_id".into(), project_id.to_string()),
        ("forge.environment".into(), environment.to_string()),
        ("forge.generation".into(), generation.to_string()),
        ("forge.deployment_id".into(), deployment_id.to_string()),
    ]);
    let container_environment = resolve_recovery_environment(
        storage_root,
        project_id,
        environment,
        generation,
        &runtime.environment_variables,
    )?;
    docker.create_container(CreateContainerRequest {
        container_name: runtime.container_name.to_string(),
        image_ref: runtime.image_ref.to_string(),
        labels,
        environment: container_environment,
        network_name: runtime.network_name.clone(),
        network_aliases: Vec::new(),
        volume_mounts: runtime
            .volume_mounts
            .iter()
            .map(|mount| VolumeMountRequest {
                volume_name: mount.docker_volume_name.clone(),
                mount_path: mount.mount_path.clone(),
            })
            .collect(),
        command: runtime.command.clone(),
        runtime_policy: runtime_policy_as_container(&runtime.runtime_policy),
    })?;
    docker.start_container(&runtime.container_name)?;
    let inspection = docker.inspect_container(&runtime.container_name)?;
    if !volume_mounts_match(&expected_mounts, &inspection.volume_mounts) {
        return Err(ConvergenceError::Storage(crate::storage::StorageError::Io(
            std::io::Error::other(format!(
                "recreated container {} without expected volume attachments",
                runtime.container_name
            )),
        )));
    }
    if !expected_mounts.is_empty() {
        append_cleanup_event(
            &EnvironmentPaths::new(storage_root, project_id, environment),
            project_id,
            environment,
            generation,
            Some(deployment_id.to_string()),
            "VOLUME_ATTACHMENT_REPAIRED",
            Some(format!(
                "restored {} volume attachment(s) for {}",
                expected_mounts.len(),
                runtime.container_name
            )),
        )?;
    }
    Ok(inspection)
}

fn resolve_recovery_environment(
    storage_root: &std::path::Path,
    project_id: &str,
    environment: &str,
    generation: u64,
    environment_variables: &BTreeMap<String, PersistedSecretReference>,
) -> Result<BTreeMap<String, String>, ConvergenceError> {
    let env_paths = EnvironmentPaths::new(storage_root, project_id, environment);
    if let Some(resolved) = load_generation_resolved_runtime(&env_paths, generation)? {
        return restore_runtime_env(&resolved).map_err(ConvergenceError::Secret);
    }

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
) -> Result<(), RouteRepairFailure> {
    let target = resolve_route_target(inspection, internal_port, preferred_network, target_source)
        .ok_or_else(|| {
            let message = preferred_network.map_or_else(
                || "container missing network IP".to_string(),
                |network_name| format!("container missing IP on docker network {network_name}"),
            );
            RouteRepairFailure {
                message: message.clone(),
                artifact: serde_json::json!({
                    "route_id": subtree_id,
                    "domain": domain,
                    "upstream_target": serde_json::Value::Null,
                    "active_target": serde_json::Value::Null,
                    "activation_verified": serde_json::Value::Null,
                    "health_checks_enabled": serde_json::Value::Null,
                    "network_name": preferred_network,
                    "error": message,
                }),
            }
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

    routing
        .update_route(RouteUpdateRequest {
            subtree_id: subtree_id.to_string(),
            target: target.clone(),
            domain: domain.clone(),
            health_checks_enabled: false,
            probe_path,
        })
        .map_err(|err| RouteRepairFailure {
            message: err.to_string(),
            artifact: serde_json::json!({
                "route_id": subtree_id,
                "domain": domain,
                "upstream_target": target,
                "active_target": serde_json::Value::Null,
                "activation_verified": serde_json::Value::Null,
                "health_checks_enabled": serde_json::Value::Null,
                "network_name": preferred_network,
                "error": err.to_string(),
            }),
        })?;
    let route = routing
        .inspect_route(subtree_id)
        .map_err(|err| RouteRepairFailure {
            message: err.to_string(),
            artifact: serde_json::json!({
                "route_id": subtree_id,
                "domain": domain,
                "upstream_target": target,
                "active_target": serde_json::Value::Null,
                "activation_verified": serde_json::Value::Null,
                "health_checks_enabled": serde_json::Value::Null,
                "network_name": preferred_network,
                "error": err.to_string(),
            }),
        })?;
    if route.active_target != target
        || route.domain != domain
        || !route.activation_verified
        || route.health_checks_enabled
    {
        return Err(RouteRepairFailure {
            message: "route activation verification failed".into(),
            artifact: serde_json::json!({
                "route_id": subtree_id,
                "domain": domain,
                "upstream_target": target,
                "active_target": route.active_target,
                "verification_url": route.verification_url,
                "verification_host": route.verification_host,
                "verification_status_code": route.verification_status_code,
                "verification_response_body": route.verification_response_body,
                "activation_verified": route.activation_verified,
                "health_checks_enabled": route.health_checks_enabled,
                "network_name": preferred_network,
                "error": "route activation verification failed",
            }),
        });
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
    volumes: std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    container_volume_mounts:
        std::collections::BTreeMap<String, Vec<crate::runtime::ContainerVolumeMount>>,
    network_ips: std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    remove_failures: std::collections::BTreeMap<String, usize>,
    image_remove_failures: std::collections::BTreeMap<String, usize>,
    build_calls: Vec<String>,
    create_calls: Vec<String>,
    start_calls: Vec<String>,
    stop_calls: Vec<String>,
    remove_calls: Vec<String>,
    removed_volumes: Vec<String>,
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
        self.container_volume_mounts.insert(
            request.container_name.clone(),
            request
                .volume_mounts
                .iter()
                .map(|mount| crate::runtime::ContainerVolumeMount {
                    volume_name: mount.volume_name.clone(),
                    mount_path: mount.mount_path.clone(),
                })
                .collect(),
        );
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
            restart_count: 0,
            started_at: None,
            image_ref: test_image_ref(container_name),
            labels: Default::default(),
            network_ips: self.inspection_network_ips(container_name),
            volume_mounts: self
                .container_volume_mounts
                .get(container_name)
                .cloned()
                .unwrap_or_default(),
            restart_policy: "no".into(),
            restart_max_retries: None,
            cpu_limit: None,
            memory_limit_mb: None,
            oom_killed: false,
            finished_at: None,
            error: None,
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
                restart_count: 0,
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
                volume_mounts: self
                    .container_volume_mounts
                    .get(container_name)
                    .cloned()
                    .unwrap_or_default(),
                restart_policy: "no".into(),
                restart_max_retries: None,
                cpu_limit: None,
                memory_limit_mb: None,
                oom_killed: false,
                finished_at: None,
                error: None,
                exit_signal: None,
                termination_reason: None,
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

    fn ensure_volume(
        &mut self,
        request: crate::runtime::CreateVolumeRequest,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        self.volumes
            .entry(request.volume_name)
            .or_insert(request.labels);
        Ok(())
    }

    fn list_managed_volumes(
        &mut self,
    ) -> Result<Vec<ManagedVolume>, crate::runtime::DockerRuntimeError> {
        Ok(self
            .volumes
            .iter()
            .map(|(volume_name, labels)| ManagedVolume {
                volume_name: volume_name.clone(),
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

    fn remove_volume(
        &mut self,
        volume_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        self.removed_volumes.push(volume_name.to_string());
        self.volumes.remove(volume_name);
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
struct TestRoutingRuntime {
    route: Option<RouteInspection>,
    updated_route_activation_verified: bool,
    remove_failures: std::collections::BTreeMap<String, usize>,
    remove_calls: Vec<String>,
    updates: Vec<RouteUpdateRequest>,
}

#[cfg(test)]
impl Default for TestRoutingRuntime {
    fn default() -> Self {
        Self {
            route: None,
            updated_route_activation_verified: true,
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
            updates: Vec::new(),
        }
    }
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
            activation_verified: self.updated_route_activation_verified,
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
            updated_route_activation_verified: true,
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
            updated_route_activation_verified: true,
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
            updated_route_activation_verified: true,
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
            updated_route_activation_verified: true,
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

    #[test]
    fn convergence_repairs_stale_current_pointer() {
        let root = test_root("convergence-repairs-stale-current-pointer");
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
            updated_route_activation_verified: true,
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
        assert_eq!(pointers.read_pointer("promoted").unwrap(), Some(2));
    }
}

#[cfg(test)]
pub mod promoted_runtime_route_drift_is_repaired {
    use super::*;

    #[test]
    fn convergence_repairs_route_target_drift_for_promoted_generation() {
        let root = test_root("convergence-repairs-route-target-drift");
        register_project(&root, "api", "api.example.com");
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
                active_target: "172.19.0.99:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
            updated_route_activation_verified: true,
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
        assert_eq!(routing.updates.len(), 1);
        assert_eq!(routing.updates[0].target, "172.19.0.11:3000");
        assert_eq!(
            PointerStore::new(env).read_pointer("current").unwrap(),
            Some(1)
        );
    }

    #[test]
    fn convergence_updates_route_when_container_ip_changes() {
        let root = test_root("convergence-updates-route-when-container-ip-changes");
        register_project(&root, "api", "api.example.com");
        setup_recoverable_http_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.network_ips.insert(
            "prod-api-gen-1".into(),
            BTreeMap::from([("forge-test".into(), "172.19.0.44".into())]),
        );
        let mut probes = TestProbeRuntime {
            tcp_ok: true,
            http_ok: true,
        };
        let mut routing = TestRoutingRuntime {
            route: Some(RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.19.0.11:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
            updated_route_activation_verified: true,
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
        assert_eq!(routing.updates.len(), 1);
        assert_eq!(routing.updates[0].target, "172.19.0.44:3000");
    }

    #[test]
    fn legacy_generation_does_not_override_newer_healthy_generation() {
        let root = test_root("legacy-generation-does-not-override-newer-healthy-generation");
        register_project(&root, "api", "api.example.com");
        setup_recoverable_http_generation(&root, 1);
        setup_recoverable_http_generation(&root, 2);
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
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
            updated_route_activation_verified: true,
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
        assert_eq!(pointers.read_authoritative_pointer().unwrap(), Some(2));
    }

    #[test]
    fn route_truth_and_promoted_generation_agree() {
        let root = test_root("route-truth-and-promoted-generation-agree");
        register_project(&root, "api", "api.example.com");
        setup_recoverable_http_generation(&root, 1);
        setup_recoverable_http_generation(&root, 2);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        crate::storage::atomic_write(env.promoted_pointer(), b"1\n").unwrap();
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
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
            updated_route_activation_verified: true,
            updates: Vec::new(),
        };
        let mut engine =
            ConvergenceEngine::new(&root, &queue, &mut docker, &mut probes, &mut routing);

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
            .unwrap();

        assert_eq!(pointers.read_pointer("current").unwrap(), Some(2));
        assert_eq!(pointers.read_pointer("promoted").unwrap(), Some(2));
        assert_eq!(
            routing
                .route
                .as_ref()
                .map(|route| route.active_target.as_str()),
            Some("172.19.0.12:3000")
        );
    }

    #[test]
    fn convergence_does_not_promote_incomplete_legacy_generation() {
        let root = test_root("convergence-does-not-promote-incomplete-legacy-generation");
        register_project(&root, "api", "api.example.com");
        setup_recoverable_http_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        fs::remove_file(env.generation_dir(1).join("runtime.json")).unwrap();
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
                truth: ActiveTruth::HttpRouted {
                    internal_port: 3000,
                },
                http_health_path: Some("/health".into()),
            })
            .unwrap();

        assert_eq!(outcome, TickOutcome::NoActiveGeneration);
        assert_eq!(
            RuntimeStateStore::new(env)
                .load()
                .unwrap()
                .active_generation,
            None
        );
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
    fn startup_repair_prefers_newest_healthy_generation() {
        let root = test_root("startup-repair-prefers-newest-healthy-generation");
        register_project(&root, "api", "api.example.com");
        setup_recoverable_http_generation(&root, 1);
        setup_recoverable_http_generation(&root, 2);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-2".into(), true);
        let mut routing = TestRoutingRuntime {
            route: Some(RouteInspection {
                subtree_id: "forge:api:production".into(),
                active_target: "172.19.0.12:3000".into(),
                domain: Some("api.example.com".into()),
                activation_verified: true,
                verification_url: None,
                verification_host: None,
                verification_status_code: None,
                verification_response_body: None,
                health_checks_enabled: false,
            }),
            remove_failures: Default::default(),
            remove_calls: Vec::new(),
            updated_route_activation_verified: true,
            updates: Vec::new(),
        };

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert_eq!(pointers.read_pointer("current").unwrap(), Some(2));
        assert_eq!(pointers.read_pointer("promoted").unwrap(), Some(2));
    }

    #[test]
    fn startup_route_repair_failure_marks_environment_degraded() {
        let root = test_root("startup-route-repair-failure-marks-environment-degraded");
        register_project(&root, "api", "api.example.com");
        setup_recoverable_http_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        let mut routing = TestRoutingRuntime {
            updated_route_activation_verified: false,
            ..Default::default()
        };

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        let runtime_state = RuntimeStateStore::new(env.clone()).load().unwrap();
        assert_eq!(runtime_state.health_state, RuntimeHealthState::Degraded);
        assert_eq!(
            runtime_state.last_error_code.as_deref(),
            Some("route_activation_verification_failed")
        );

        let summary = DiagnosticsStore::new(env.clone(), 1)
            .read_summary()
            .unwrap()
            .unwrap();
        assert_eq!(summary.failure_stage, "startup_recovery");
        assert_eq!(
            summary.failure_reason,
            "route activation verification failed"
        );
        assert!(
            DiagnosticsStore::new(env.clone(), 1)
                .read_text_artifact("route_activation_failure.json")
                .unwrap()
                .is_some()
        );

        let event_types = EventStore::list_all(&root)
            .unwrap()
            .into_iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>();
        assert!(event_types.contains(&"STARTUP_ROUTE_REPAIR_FAILED".to_string()));
        assert!(event_types.contains(&"ROUTE_ACTIVATION_VERIFICATION_FAILED".to_string()));
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
            updated_route_activation_verified: true,
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
            updated_route_activation_verified: true,
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

    #[test]
    fn retention_preserves_current_generation() {
        let root = test_root("retention-preserves-current-generation");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(2).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.containers.insert("prod-api-gen-2".into(), true);
        let mut routing = TestRoutingRuntime::default();

        garbage_collect(&root, &queue, &mut docker, &mut routing, false).unwrap();

        assert!(env.generation_dir(2).exists());
    }

    #[test]
    fn retention_preserves_previous_rollback_generation() {
        let root = test_root("retention-preserves-previous-rollback-generation");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(3).unwrap();
        atomic_write(env.previous_pointer(), b"2\n").unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        for generation in 1..=3 {
            docker
                .containers
                .insert(format!("prod-api-gen-{generation}"), true);
        }
        let mut routing = TestRoutingRuntime::default();

        garbage_collect(&root, &queue, &mut docker, &mut routing, false).unwrap();

        assert!(env.generation_dir(2).exists());
        assert_eq!(
            PointerStore::new(env).read_pointer("previous").unwrap(),
            Some(2)
        );
    }

    #[test]
    fn gc_removes_orphaned_images() {
        let root = test_root("gc-removes-orphaned-images");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        docker.seed_image("api", "production", 2, "forge/api:production-gen-2");
        let mut routing = TestRoutingRuntime::default();

        garbage_collect(&root, &queue, &mut docker, &mut routing, false).unwrap();

        assert!(
            docker
                .image_remove_calls
                .iter()
                .any(|image| image == "forge/api:production-gen-2")
        );
    }

    #[test]
    fn gc_removes_unreferenced_checkouts() {
        let root = test_root("gc-removes-unreferenced-checkouts");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(3).unwrap();
        let checkout = root.join("source-checkouts/api/sha-old");
        fs::create_dir_all(&checkout).unwrap();
        atomic_write(
            env.generation_dir(1).join("build.json"),
            format!(
                "{{\n  \"deployment_id\": \"dep-1\",\n  \"image_ref\": \"forge/api:production-gen-1\",\n  \"source_path\": \"{}\"\n}}\n",
                checkout.display()
            )
            .as_bytes(),
        )
        .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
        let mut routing = TestRoutingRuntime::default();

        garbage_collect(&root, &queue, &mut docker, &mut routing, false).unwrap();

        assert!(!checkout.exists());
    }

    #[test]
    fn gc_does_not_break_rollback() {
        let root = test_root("gc-does-not-break-rollback");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(3).unwrap();
        atomic_write(env.previous_pointer(), b"2\n").unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        for generation in 1..=3 {
            docker
                .containers
                .insert(format!("prod-api-gen-{generation}"), true);
        }
        let mut routing = TestRoutingRuntime::default();

        garbage_collect(&root, &queue, &mut docker, &mut routing, false).unwrap();

        assert_eq!(
            PointerStore::new(env.clone())
                .read_pointer("previous")
                .unwrap(),
            Some(2)
        );
        assert!(env.generation_dir(2).join("snapshot.json").exists());
    }

    #[test]
    fn gc_preserves_backup_artifacts() {
        let root = test_root("gc-preserves-backup-artifacts");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let backup_root = EnvironmentPaths::backups_root(&root)
            .join("api")
            .join("production")
            .join("backup-1");
        std::fs::create_dir_all(backup_root.join("volumes")).unwrap();
        crate::storage::atomic_write(
            backup_root.join("metadata.json"),
            br#"{
  "backup_version": 1,
  "backup_id": "backup-1",
  "project_id": "api",
  "environment": "production",
  "created_at_unix": 1,
  "source_generation": 1,
  "snapshot_metadata": {"snapshot_version":1,"project_id":"api","environment":"production","generation":1,"state":"healthy","finalized_at_unix":1},
  "build_info": {"deployment_id":"dep-1","image_ref":"forge/api:production-gen-1","services":{}},
  "runtime_info": {"container_name":"prod-api-gen-1","running":true,"environment_variables":{}},
  "resolved_runtime": {"snapshot_version":1,"project_id":"api","environment":"production","generation":1,"deployment_id":"dep-1","source_environment":"production","entries":{}},
  "services": ["api"],
  "volumes": [],
  "restores": [],
  "warnings": []
}
"#,
        )
        .unwrap();
        crate::storage::atomic_write(backup_root.join("volumes/manifest.txt"), b"present\n")
            .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut routing = TestRoutingRuntime::default();

        garbage_collect(&root, &queue, &mut docker, &mut routing, false).unwrap();

        assert!(backup_root.join("metadata.json").exists());
        assert!(backup_root.join("volumes/manifest.txt").exists());
    }

    #[test]
    fn convergence_handles_missing_gc_generation() {
        let root = test_root("convergence-handles-missing-gc-generation");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(3).unwrap();
        atomic_write(env.previous_pointer(), b"2\n").unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-3".into(), true);
        let mut routing = TestRoutingRuntime::default();

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert_eq!(
            PointerStore::new(env).read_pointer("previous").unwrap(),
            Some(1)
        );
    }

    #[test]
    fn convergence_tolerates_partial_restore_failure() {
        let root = test_root("convergence-tolerates-partial-restore-failure");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        setup_active_generation(&root, 2);
        crate::storage::atomic_write(
            env.generation_dir(2).join("snapshot.json"),
            br#"{
  "snapshot_version": 1,
  "project_id": "api",
  "environment": "production",
  "generation": 2,
  "state": "healthy",
  "finalized_at_unix": 2
}
"#,
        )
        .unwrap();
        let _ = std::fs::remove_file(env.generation_dir(2).join("runtime.json"));
        PointerStore::new(env.clone()).swap_current(2).unwrap();
        crate::storage::atomic_write(env.previous_pointer(), b"1\n").unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut routing = TestRoutingRuntime::default();

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert_eq!(
            PointerStore::new(env.clone())
                .read_pointer("previous")
                .unwrap(),
            Some(1)
        );
        assert!(env.generation_dir(1).join("snapshot.json").exists());
    }

    #[test]
    fn gc_dry_run_is_non_mutating() {
        let root = test_root("gc-dry-run-is-non-mutating");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(3).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
        let mut routing = TestRoutingRuntime::default();

        let report = garbage_collect(&root, &queue, &mut docker, &mut routing, true).unwrap();

        assert!(report.actions.iter().any(|action| {
            action.generation == Some(1)
                && action.subject_kind.as_deref() == Some("generation")
                && action.outcome == "would_remove"
        }));
        assert!(env.generation_dir(1).exists());
        assert!(docker.remove_calls.is_empty());
        assert!(!env.gc_file().exists());
    }

    #[test]
    fn gc_dry_run_handles_missing_artifacts() {
        let root = test_root("gc-dry-run-handles-missing-artifacts");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(3).unwrap();
        crate::storage::atomic_write(
            env.generation_dir(1).join("build.json"),
            b"{\n  \"deployment_id\": \"dep-1\",\n  \"image_ref\": \"forge/api:production-gen-1\",\n  \"source_path\": \"/tmp/forge-missing-checkout\"\n}\n",
        )
        .unwrap();
        fs::remove_file(env.generation_dir(1).join("runtime_env_snapshot.json")).ok();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
        let mut routing = TestRoutingRuntime::default();

        let report = garbage_collect(&root, &queue, &mut docker, &mut routing, true).unwrap();

        assert!(
            report
                .actions
                .iter()
                .any(|action| action.subject_kind.as_deref() == Some("checkout")
                    && action.outcome == "skipped_missing")
        );
        assert!(
            report
                .actions
                .iter()
                .any(
                    |action| action.subject_kind.as_deref() == Some("runtime_snapshot")
                        && action.outcome == "skipped_missing"
                )
        );
    }

    #[test]
    fn gc_dry_run_handles_missing_source_checkouts_root() {
        let root = test_root("gc-dry-run-handles-missing-source-checkouts-root");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut routing = TestRoutingRuntime::default();
        let source_checkouts_root = root.join("source-checkouts");

        let report = garbage_collect(&root, &queue, &mut docker, &mut routing, true).unwrap();

        assert!(report.actions.iter().any(|action| {
            action.subject_kind.as_deref() == Some("root")
                && action.subject.as_deref()
                    == Some(&format!("root={}", source_checkouts_root.display()))
                && action.outcome == "skipped_missing"
        }));
    }

    #[test]
    fn gc_dry_run_handles_missing_repositories_root() {
        let root = test_root("gc-dry-run-handles-missing-repositories-root");
        setup_active_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut routing = TestRoutingRuntime::default();
        let repositories_root = root.join("repositories");

        let report = garbage_collect(&root, &queue, &mut docker, &mut routing, true).unwrap();

        assert!(report.actions.iter().any(|action| {
            action.subject_kind.as_deref() == Some("root")
                && action.subject.as_deref()
                    == Some(&format!("root={}", repositories_root.display()))
                && action.outcome == "skipped_missing"
        }));
    }

    #[test]
    fn gc_dry_run_error_includes_path_context() {
        let root = test_root("gc-dry-run-error-includes-path-context");
        atomic_write(root.join("projects"), b"not a directory\n").unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        let mut routing = TestRoutingRuntime::default();

        let err = garbage_collect(&root, &queue, &mut docker, &mut routing, true).unwrap_err();

        assert_eq!(
            err.to_string(),
            format!(
                "failed to scan projects root {}: Not a directory (os error 20)",
                root.join("projects").display()
            )
        );
    }

    #[test]
    fn gc_dry_run_succeeds_on_partial_storage_root() {
        let root = test_root("gc-dry-run-succeeds-on-partial-storage-root");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);
        let mut routing = TestRoutingRuntime::default();
        let env = EnvironmentPaths::new(&root, "api", "production");

        let report = garbage_collect(&root, &queue, &mut docker, &mut routing, true).unwrap();

        assert!(report.actions.iter().any(|action| {
            action.subject_kind.as_deref() == Some("root")
                && action.subject.as_deref() == Some(&format!("root={}", env.root.display()))
                && action.outcome == "skipped_missing"
        }));
        assert!(report.actions.iter().any(|action| {
            action.subject_kind.as_deref() == Some("root")
                && action.subject.as_deref()
                    == Some(&format!("root={}", env.generations_dir().display()))
                && action.outcome == "skipped_missing"
        }));
        assert!(!env.root.exists());
    }

    #[test]
    fn gc_json_output_reports_actions() {
        let root = test_root("gc-json-output-reports-actions");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(3).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.seed_image("api", "production", 1, "forge/api:production-gen-1");
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
        let mut routing = TestRoutingRuntime::default();

        let report = garbage_collect(&root, &queue, &mut docker, &mut routing, true).unwrap();
        let json = serde_json::to_value(&report).unwrap();
        let actions = json["actions"].as_array().unwrap();

        assert!(actions.iter().any(|action| {
            action["subject_kind"] == "generation" && action["action"] == "gc-eligible"
        }));
        assert!(
            actions.iter().any(|action| {
                action["subject_kind"] == "image" && action["action"] == "remove"
            })
        );
    }

    #[test]
    fn gc_skips_missing_generation_without_failure() {
        let root = test_root("gc-skips-missing-generation-without-failure");
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(3).unwrap();
        crate::storage::RetentionStore::new(env.clone())
            .write(&crate::storage::RetentionMetadata {
                updated_at_unix: Some(current_unix_timestamp()),
                generations: vec![crate::storage::GenerationHistoryRecord {
                    generation: 1,
                    image_ref: Some("forge/api:production-gen-1".into()),
                    source_path: Some(PathBuf::from("/tmp/missing-checkout")),
                    ..crate::storage::GenerationHistoryRecord::default()
                }],
            })
            .unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
        let mut routing = TestRoutingRuntime::default();

        let report = garbage_collect(&root, &queue, &mut docker, &mut routing, true).unwrap();

        assert!(report.actions.iter().any(|action| {
            action.generation == Some(1)
                && action.subject_kind.as_deref() == Some("generation")
                && action.outcome == "skipped_missing"
        }));
    }

    #[test]
    fn convergence_tolerates_partially_removed_generation() {
        let root = test_root("convergence-tolerates-partially-removed-generation");
        setup_active_generation(&root, 1);
        setup_active_generation(&root, 2);
        setup_active_generation(&root, 3);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(2).unwrap();
        pointers.swap_current(3).unwrap();
        fs::remove_file(env.generation_dir(1).join("build.json")).unwrap();
        fs::remove_file(env.generation_dir(1).join("runtime.json")).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-2".into(), true);
        docker.containers.insert("prod-api-gen-3".into(), true);
        let mut routing = TestRoutingRuntime::default();

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();
    }
}

#[cfg(test)]
pub mod route_repair_failure_during_convergence {
    use super::*;

    #[test]
    fn convergence_failure_does_not_exit_daemon() {
        let root = test_root("convergence-failure-does-not-exit-daemon");
        register_project(&root, "api", "api.example.com");
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
            updated_route_activation_verified: false,
            ..Default::default()
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

        assert_eq!(outcome, TickOutcome::Degraded(1));
        let runtime_state = RuntimeStateStore::new(env).load().unwrap();
        assert_eq!(runtime_state.health_state, RuntimeHealthState::Degraded);
        assert_eq!(
            runtime_state.last_error_code.as_deref(),
            Some("route_activation_verification_failed")
        );
    }
}

#[cfg(test)]
pub mod multi_service_convergence_and_gc {
    use super::*;
    use crate::storage::{
        PersistedActivationMode, PersistedBuildInfo, PersistedRouteTargetSource,
        PersistedRuntimeInfo, PersistedServiceRuntimeInfo, SnapshotWriter,
    };
    use std::collections::BTreeMap;

    fn setup_multi_service_generation(root: &std::path::Path, generation: u64) {
        let env = EnvironmentPaths::new(root, "api", "production");
        SnapshotWriter::new(env.clone(), generation)
            .unwrap()
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        let build = serde_json::to_string_pretty(&PersistedBuildInfo {
            deployment_id: format!("dep-{generation}"),
            image_ref: format!("forge/api:production-gen-{generation}"),
            services: BTreeMap::new(),
            source_ref: None,
            repo_url: None,
            commit_sha: None,
            source_path: None,
        })
        .unwrap();
        crate::storage::atomic_write(
            env.generation_dir(generation).join("build.json"),
            format!("{build}\n").as_bytes(),
        )
        .unwrap();
        let services = BTreeMap::from([
            (
                "api".into(),
                PersistedServiceRuntimeInfo {
                    service_id: "api".into(),
                    container_name: format!("prod-api-api-gen-{generation}"),
                    image_ref: format!("forge/api:production-gen-{generation}"),
                    running: true,
                    state: crate::storage::PersistedServiceState::Healthy,
                    network_name: Some("forge-test".into()),
                    probe_path: Some("/health".into()),
                    activation: Some(PersistedActivationMode::Http {
                        internal_port: 3000,
                        route_subtree_id: Some("forge:api:production:api".into()),
                        target_source: PersistedRouteTargetSource::ContainerIp,
                    }),
                    command: None,
                    runtime_policy: PersistedRuntimePolicy {
                        restart_policy: "no".into(),
                        ..PersistedRuntimePolicy::default()
                    },
                    runtime_usage: None,
                    termination: None,
                    depends_on: vec!["redis".into()],
                    required_for_promotion: true,
                    externally_exposed: true,
                    environment_variables: BTreeMap::new(),
                    state_config: None,
                    volume_mounts: Vec::new(),
                    source_ref: None,
                    repo_url: None,
                    commit_sha: None,
                    source_path: None,
                },
            ),
            (
                "worker".into(),
                PersistedServiceRuntimeInfo {
                    service_id: "worker".into(),
                    container_name: format!("prod-api-worker-gen-{generation}"),
                    image_ref: format!("forge/api:production-gen-{generation}"),
                    running: true,
                    state: crate::storage::PersistedServiceState::Healthy,
                    network_name: Some("forge-test".into()),
                    probe_path: None,
                    activation: Some(PersistedActivationMode::Direct),
                    command: Some(vec!["sh".into(), "-lc".into(), "node worker.js".into()]),
                    runtime_policy: PersistedRuntimePolicy {
                        restart_policy: "no".into(),
                        ..PersistedRuntimePolicy::default()
                    },
                    runtime_usage: None,
                    termination: None,
                    depends_on: vec!["api".into()],
                    required_for_promotion: true,
                    externally_exposed: false,
                    environment_variables: BTreeMap::new(),
                    state_config: None,
                    volume_mounts: Vec::new(),
                    source_ref: None,
                    repo_url: None,
                    commit_sha: None,
                    source_path: None,
                },
            ),
        ]);
        let runtime = serde_json::to_string_pretty(&PersistedRuntimeInfo {
            container_name: format!("prod-api-api-gen-{generation}"),
            running: true,
            network_name: Some("forge-test".into()),
            probe_path: Some("/health".into()),
            activation: Some(PersistedActivationMode::Http {
                internal_port: 3000,
                route_subtree_id: Some("forge:api:production:api".into()),
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
            source_ref: None,
            repo_url: None,
            commit_sha: None,
            source_path: None,
            services,
            startup_order: vec!["redis".into(), "api".into(), "worker".into()],
        })
        .unwrap();
        crate::storage::atomic_write(
            env.generation_dir(generation).join("runtime.json"),
            format!("{runtime}\n").as_bytes(),
        )
        .unwrap();
    }

    fn set_multi_service_runtime_policy(
        root: &std::path::Path,
        generation: u64,
        cpu_limit: &str,
        memory_limit_mb: u64,
        restart_policy: &str,
        max_retries: Option<u64>,
    ) {
        let env = EnvironmentPaths::new(root, "api", "production");
        let runtime_path = env.generation_dir(generation).join("runtime.json");
        let mut runtime: PersistedRuntimeInfo =
            serde_json::from_str(&std::fs::read_to_string(&runtime_path).unwrap()).unwrap();
        runtime.runtime_policy = PersistedRuntimePolicy {
            cpu_limit: Some(cpu_limit.into()),
            memory_limit_mb: Some(memory_limit_mb),
            restart_policy: restart_policy.into(),
            max_retries,
        };
        if let Some(api) = runtime.services.get_mut("api") {
            api.runtime_policy = runtime.runtime_policy.clone();
        }
        crate::storage::atomic_write(
            runtime_path,
            format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()).as_bytes(),
        )
        .unwrap();
    }

    fn setup_stateful_generation(
        root: &std::path::Path,
        generation: u64,
        retention: PersistedVolumeRetention,
    ) -> String {
        let env = EnvironmentPaths::new(root, "api", "production");
        SnapshotWriter::new(env.clone(), generation)
            .unwrap()
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        let volume_name = match retention {
            PersistedVolumeRetention::Persistent => {
                "forge-api-production-vol-postgres-data".to_string()
            }
            PersistedVolumeRetention::Ephemeral => {
                format!("forge-api-production-gen-{generation}-vol-postgres-data")
            }
        };
        let runtime = serde_json::to_string_pretty(&PersistedRuntimeInfo {
            container_name: format!("prod-api-postgres-gen-{generation}"),
            running: true,
            network_name: Some("forge-test".into()),
            probe_path: None,
            activation: Some(PersistedActivationMode::Direct),
            runtime_policy: PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            environment_variables: BTreeMap::new(),
            volume_mounts: vec![crate::storage::PersistedVolumeMount {
                volume_id: "postgres-data".into(),
                docker_volume_name: volume_name.clone(),
                mount_path: "/var/lib/postgresql/data".into(),
                service_id: "postgres".into(),
                generation,
                retention: retention.clone(),
            }],
            source_ref: None,
            repo_url: None,
            commit_sha: None,
            source_path: None,
            services: BTreeMap::from([(
                "postgres".into(),
                PersistedServiceRuntimeInfo {
                    service_id: "postgres".into(),
                    container_name: format!("prod-api-postgres-gen-{generation}"),
                    image_ref: "postgres:16".into(),
                    running: true,
                    state: crate::storage::PersistedServiceState::Healthy,
                    network_name: Some("forge-test".into()),
                    probe_path: None,
                    activation: Some(PersistedActivationMode::Direct),
                    command: None,
                    runtime_policy: PersistedRuntimePolicy {
                        restart_policy: "no".into(),
                        ..PersistedRuntimePolicy::default()
                    },
                    runtime_usage: None,
                    termination: None,
                    depends_on: Vec::new(),
                    required_for_promotion: true,
                    externally_exposed: false,
                    environment_variables: BTreeMap::new(),
                    state_config: None,
                    volume_mounts: vec![crate::storage::PersistedVolumeMount {
                        volume_id: "postgres-data".into(),
                        docker_volume_name: volume_name.clone(),
                        mount_path: "/var/lib/postgresql/data".into(),
                        service_id: "postgres".into(),
                        generation,
                        retention,
                    }],
                    source_ref: None,
                    repo_url: None,
                    commit_sha: None,
                    source_path: None,
                },
            )]),
            startup_order: vec!["postgres".into()],
        })
        .unwrap();
        crate::storage::atomic_write(
            env.generation_dir(generation).join("runtime.json"),
            format!("{runtime}\n").as_bytes(),
        )
        .unwrap();
        crate::storage::atomic_write(
            env.generation_dir(generation).join("build.json"),
            format!("{{\"deployment_id\":\"dep-{generation}\",\"image_ref\":\"postgres:16\"}}\n")
                .as_bytes(),
        )
        .unwrap();
        volume_name
    }

    #[test]
    fn convergence_repairs_partial_service_drift() {
        let root = test_root("multi-service-convergence-repairs-drift");
        register_project(&root, "api", "example.com");
        setup_multi_service_generation(&root, 1);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-api-gen-1".into(), true);
        docker.network_ips.insert(
            "prod-api-api-gen-1".into(),
            BTreeMap::from([("forge-test".into(), "172.18.0.12".into())]),
        );
        let mut routing = TestRoutingRuntime::default();

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert!(
            docker
                .create_calls
                .iter()
                .any(|call| call == "prod-api-worker-gen-1")
        );
    }

    #[test]
    fn convergence_repairs_resource_policy_drift() {
        let root = test_root("convergence-repairs-resource-policy-drift");
        register_project(&root, "api", "example.com");
        setup_multi_service_generation(&root, 1);
        set_multi_service_runtime_policy(&root, 1, "1.5", 512, "on-failure", Some(4));
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-api-gen-1".into(), true);
        docker
            .containers
            .insert("prod-api-worker-gen-1".into(), true);
        docker.network_ips.insert(
            "prod-api-api-gen-1".into(),
            BTreeMap::from([("forge-test".into(), "172.18.0.12".into())]),
        );
        docker.network_ips.insert(
            "prod-api-worker-gen-1".into(),
            BTreeMap::from([("forge-test".into(), "172.18.0.13".into())]),
        );
        let mut routing = TestRoutingRuntime::default();

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert!(
            docker
                .remove_calls
                .iter()
                .any(|call| call == "prod-api-api-gen-1")
        );
        assert!(
            docker
                .create_calls
                .iter()
                .any(|call| call == "prod-api-api-gen-1")
        );
    }

    #[test]
    fn manual_docker_update_detected_as_drift() {
        let root = test_root("manual-docker-update-detected-as-drift");
        register_project(&root, "api", "example.com");
        setup_multi_service_generation(&root, 1);
        set_multi_service_runtime_policy(&root, 1, "1.5", 512, "on-failure", Some(4));
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-api-gen-1".into(), true);
        docker
            .containers
            .insert("prod-api-worker-gen-1".into(), true);
        let mut routing = TestRoutingRuntime::default();

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        let events = EventStore::list_all(&root).unwrap();
        assert!(events.iter().any(|event| {
            event.event_type == "RUNTIME_POLICY_DRIFT_REPAIRED" && event.generation == Some(1)
        }));
    }

    #[test]
    fn gc_preserves_multi_service_rollback_generation() {
        let root = test_root("multi-service-gc-preserves-rollback");
        setup_multi_service_generation(&root, 1);
        setup_multi_service_generation(&root, 2);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        let mut routing = TestRoutingRuntime::default();

        let report = garbage_collect(&root, &queue, &mut docker, &mut routing, true).unwrap();

        assert!(report.actions.iter().any(|action| {
            action.generation == Some(1)
                && action.outcome == "protected"
                && action
                    .protected
                    .iter()
                    .any(|reason| reason == "rollback-safe generation")
        }));
    }

    #[test]
    fn convergence_repairs_missing_volume_attachment() {
        let root = test_root("convergence-repairs-missing-volume-attachment");
        register_project(&root, "api", "example.com");
        let volume_name = setup_stateful_generation(&root, 1, PersistedVolumeRetention::Persistent);
        let env = EnvironmentPaths::new(&root, "api", "production");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker
            .containers
            .insert("prod-api-postgres-gen-1".into(), true);
        docker.network_ips.insert(
            "prod-api-postgres-gen-1".into(),
            BTreeMap::from([("forge-test".into(), "172.18.0.11".into())]),
        );
        let mut routing = TestRoutingRuntime::default();

        StartupConvergence::new(&root, &queue, &ResumeDecider(true))
            .recover_active_deployment(&mut docker, &mut routing)
            .unwrap();

        assert!(
            docker
                .create_calls
                .iter()
                .any(|name| name == "prod-api-postgres-gen-1")
        );
        assert!(docker.volumes.contains_key(&volume_name));
    }

    #[test]
    fn ephemeral_volume_removed_by_gc() {
        let root = test_root("ephemeral-volume-removed-by-gc");
        let volume_name = setup_stateful_generation(&root, 1, PersistedVolumeRetention::Ephemeral);
        setup_stateful_generation(&root, 2, PersistedVolumeRetention::Ephemeral);
        setup_stateful_generation(&root, 3, PersistedVolumeRetention::Ephemeral);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();
        pointers.swap_current(3).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.volumes.insert(
            volume_name.clone(),
            BTreeMap::from([
                ("forge.managed".into(), "true".into()),
                ("forge.project_id".into(), "api".into()),
                ("forge.environment".into(), "production".into()),
                ("forge.generation".into(), "1".into()),
                ("forge.volume_retention".into(), "ephemeral".into()),
            ]),
        );
        let mut routing = TestRoutingRuntime::default();

        garbage_collect(&root, &queue, &mut docker, &mut routing, false).unwrap();

        assert!(docker.image_remove_calls.is_empty() || !docker.removed_volumes.is_empty());
        assert!(
            docker
                .removed_volumes
                .iter()
                .any(|name| name == &volume_name)
        );
    }

    #[test]
    fn gc_preserves_persistent_volumes() {
        let root = test_root("gc-preserves-persistent-volumes");
        let volume_name = setup_stateful_generation(&root, 1, PersistedVolumeRetention::Persistent);
        setup_stateful_generation(&root, 2, PersistedVolumeRetention::Persistent);
        setup_stateful_generation(&root, 3, PersistedVolumeRetention::Persistent);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let pointers = PointerStore::new(env.clone());
        pointers.swap_current(1).unwrap();
        pointers.swap_current(2).unwrap();
        pointers.swap_current(3).unwrap();
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        let mut docker = TestDockerRuntime::default();
        docker.volumes.insert(
            volume_name.clone(),
            BTreeMap::from([
                ("forge.managed".into(), "true".into()),
                ("forge.project_id".into(), "api".into()),
                ("forge.environment".into(), "production".into()),
                ("forge.generation".into(), "1".into()),
                ("forge.volume_retention".into(), "persistent".into()),
            ]),
        );
        let mut routing = TestRoutingRuntime::default();

        let report = garbage_collect(&root, &queue, &mut docker, &mut routing, true).unwrap();

        assert!(
            !docker
                .removed_volumes
                .iter()
                .any(|name| name == &volume_name)
        );
        assert!(report.actions.iter().any(|action| {
            action.subject_kind.as_deref() == Some("volume")
                && action.subject.as_deref() == Some(volume_name.as_str())
                && action.outcome == "protected"
        }));
    }
}

#[cfg(test)]
pub mod drift_normalization {
    use super::*;

    fn volume_less_runtime(restart_policy: &str) -> PersistedServiceRuntimeInfo {
        PersistedServiceRuntimeInfo {
            service_id: "api".into(),
            container_name: "prod-api-gen-1".into(),
            image_ref: "forge/api:prod-gen-1".into(),
            running: true,
            state: crate::storage::PersistedServiceState::Healthy,
            network_name: Some("forge-test".into()),
            probe_path: Some("/health".into()),
            activation: Some(PersistedActivationMode::Http {
                internal_port: 3000,
                route_subtree_id: Some("forge:api:production".into()),
                target_source: PersistedRouteTargetSource::ContainerIp,
            }),
            command: None,
            runtime_policy: PersistedRuntimePolicy {
                restart_policy: restart_policy.into(),
                ..PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            depends_on: Vec::new(),
            required_for_promotion: true,
            externally_exposed: true,
            environment_variables: BTreeMap::new(),
            state_config: None,
            volume_mounts: Vec::new(),
            source_ref: None,
            repo_url: None,
            commit_sha: None,
            source_path: None,
        }
    }

    #[test]
    fn empty_volume_mounts_compare_equal() {
        assert!(volume_mounts_match(&[], &[]));
    }

    #[test]
    fn convergence_does_not_recreate_for_empty_restart_policy() {
        let root = test_root("convergence-does-not-recreate-for-empty-restart-policy");
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);

        ensure_generation_service_running(
            &root,
            "api",
            "production",
            1,
            "dep-1",
            &volume_less_runtime(""),
            &mut docker,
        )
        .unwrap();

        assert!(docker.remove_calls.is_empty());
        assert!(docker.create_calls.is_empty());
    }

    #[test]
    fn no_volume_service_does_not_trigger_volume_repair() {
        let root = test_root("no-volume-service-does-not-trigger-volume-repair");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), true);

        ensure_generation_service_running(
            &root,
            "api",
            "production",
            1,
            "dep-1",
            &volume_less_runtime("no"),
            &mut docker,
        )
        .unwrap();

        let events = EventStore::list_all(&root).unwrap();
        assert!(events.is_empty(), "{events:?}");
        assert!(!env.generation_dir(1).join("events.jsonl").exists());
    }

    #[test]
    fn convergence_does_not_recreate_volume_less_services() {
        let root = test_root("convergence-does-not-recreate-volume-less-services");
        let mut docker = TestDockerRuntime::default();
        docker.containers.insert("prod-api-gen-1".into(), false);

        ensure_generation_service_running(
            &root,
            "api",
            "production",
            1,
            "dep-1",
            &volume_less_runtime("no"),
            &mut docker,
        )
        .unwrap();

        assert_eq!(docker.start_calls, vec!["prod-api-gen-1".to_string()]);
        assert!(docker.remove_calls.is_empty());
        assert!(docker.create_calls.is_empty());
    }

    #[test]
    fn healthy_environment_clears_startup_route_failure_marker() {
        let root = test_root("healthy-environment-clears-startup-route-failure-marker");
        let env = EnvironmentPaths::new(&root, "api", "production");
        RuntimeStateStore::new(env.clone())
            .save(&crate::storage::RuntimeState {
                active_generation: Some(1),
                health_state: RuntimeHealthState::Degraded,
                failed_probe_count: 0,
                successful_probe_count: 0,
                restart_attempted: false,
                degraded_since_unix: Some(1),
                last_transition: "startup_route_repair_failed".into(),
                last_error_code: Some("route_activation_verification_failed".into()),
            })
            .unwrap();

        clear_resolved_route_repair_state(&env, 1).unwrap();

        let runtime_state = RuntimeStateStore::new(env).load().unwrap();
        assert_eq!(runtime_state.health_state, RuntimeHealthState::Healthy);
        assert_eq!(runtime_state.last_transition, "healthy");
        assert_eq!(runtime_state.last_error_code, None);
    }
}
