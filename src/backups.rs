use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(test)]
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::api::{
    BackupArchiveFileRecord, BackupHookRecord, BackupListResponse, BackupRecord,
    BackupRestoreResponse, BackupVolumeRecord, RestoreRecord,
};
use crate::events::EventRecord;
use crate::projects::ProjectRegistryStore;
use crate::queue::DeploymentRecord;
use crate::route_truth::resolve_route_target;
use crate::runtime::{
    ContainerInspection, CreateContainerRequest, CreateVolumeRequest, DockerRuntime,
    DockerRuntimeError, ExecInContainerRequest, RouteInspection, RouteUpdateRequest,
    RoutingRuntime, VolumeArchiveHelperRequest, VolumeArchiveMode, VolumeMountRequest,
};
use crate::runtime_env::{RuntimeEnvMetadata, generated_forge_vars, restore_runtime_env};
use crate::status::derive_environment_domain;
use crate::storage::{
    DeploymentLifecycleState, DiagnosticsStore, EnvironmentPaths, EventStore, GenerationAllocator,
    GenerationHistoryRecord, LifecycleStore, PersistedActivationMode, PersistedBackupHookRecord,
    PersistedBackupMetadata, PersistedBackupRestoreRecord, PersistedBackupVolumeRecord,
    PersistedBuildInfo, PersistedDeploymentLifecycle, PersistedPromotionSummary,
    PersistedResolvedRuntime, PersistedResolvedRuntimeEntry, PersistedRuntimeEnvEntry,
    PersistedRuntimeEnvSnapshot, PersistedRuntimeInfo, PersistedServiceRuntimeInfo,
    PersistedServiceState, PersistedSnapshotMetadata, PersistedValidationSummary,
    PersistedVolumeMount, PersistedVolumeRetention, PointerStore, RetentionStore,
    RuntimeHealthState, RuntimeState, RuntimeStateStore, SnapshotState, SnapshotWriter,
    StorageError, atomic_write, current_unix_timestamp, load_generation_build_info,
    load_generation_resolved_runtime, load_generation_runtime_env_snapshot,
    load_generation_runtime_info, load_generation_snapshot_metadata,
};
use crate::topology::{runtime_with_primary_service, select_primary_service_id};

#[derive(Debug)]
pub enum BackupError {
    Storage(StorageError),
    Docker(DockerRuntimeError),
    Routing(crate::runtime::RoutingRuntimeError),
    Command(String),
    NotFound(String),
    Invalid(String),
}

impl Display for BackupError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "{err}"),
            Self::Docker(err) => write!(f, "{err}"),
            Self::Routing(err) => write!(f, "{err}"),
            Self::Command(err) => write!(f, "{err}"),
            Self::NotFound(err) => write!(f, "{err}"),
            Self::Invalid(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for BackupError {}

impl From<StorageError> for BackupError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<DockerRuntimeError> for BackupError {
    fn from(value: DockerRuntimeError) -> Self {
        Self::Docker(value)
    }
}

impl From<crate::runtime::RoutingRuntimeError> for BackupError {
    fn from(value: crate::runtime::RoutingRuntimeError) -> Self {
        Self::Routing(value)
    }
}

impl From<std::io::Error> for BackupError {
    fn from(value: std::io::Error) -> Self {
        Self::Command(value.to_string())
    }
}

const VOLUME_HELPER_TIMEOUT: Duration = Duration::from_secs(60);
const PRE_BACKUP_HOOK_TIMEOUT: Duration = Duration::from_secs(30);

pub fn create_backup<D: DockerRuntime>(
    storage_root: &Path,
    docker: &mut D,
    project_id: &str,
    environment: &str,
) -> Result<BackupRecord, BackupError> {
    let env = EnvironmentPaths::new(storage_root, project_id, environment);
    env.ensure_exists()?;
    let pointers = PointerStore::new(env.clone());
    let generation = pointers
        .read_authoritative_pointer()?
        .ok_or_else(|| BackupError::NotFound("no active generation available for backup".into()))?;
    let snapshot = load_generation_snapshot_metadata(&env, generation)?.ok_or_else(|| {
        BackupError::NotFound(format!("generation {generation} snapshot missing"))
    })?;
    let build = load_generation_build_info(&env, generation)?
        .ok_or_else(|| BackupError::NotFound(format!("generation {generation} build missing")))?;
    let runtime = load_generation_runtime_info(&env, generation)?
        .ok_or_else(|| BackupError::NotFound(format!("generation {generation} runtime missing")))?;
    let runtime = runtime_with_primary_service(&runtime);
    let runtime_env_snapshot =
        load_generation_runtime_env_snapshot(&env, generation)?.ok_or_else(|| {
            BackupError::Invalid(format!(
                "active generation lacks runtime env snapshot; redeploy before backup (project={project_id}, environment={environment}, generation={generation})"
            ))
        })?;
    let resolved = load_generation_resolved_runtime(&env, generation)?.ok_or_else(|| {
        BackupError::NotFound(format!("generation {generation} resolved runtime missing"))
    })?;

    let services = runtime_services(&runtime);
    let volume_mounts = services
        .values()
        .flat_map(|service| service.volume_mounts.iter())
        .filter(|mount| matches!(mount.retention, PersistedVolumeRetention::Persistent))
        .cloned()
        .collect::<Vec<_>>();
    if volume_mounts.is_empty() {
        return Err(BackupError::Invalid(
            "no persistent Docker volumes are attached to the active generation".into(),
        ));
    }

    let backup_id = format!("backup-{}", current_unix_timestamp());
    let backup_dir = backup_dir(storage_root, project_id, environment, &backup_id);
    fs::create_dir_all(backup_dir.join("volumes"))?;
    let backup_result = (|| -> Result<
        (Vec<PersistedBackupHookRecord>, Vec<PersistedBackupVolumeRecord>),
        BackupError,
    > {
        let hooks = run_pre_backup_hooks(docker, &services)?;
        let mut manifest = Vec::new();
        for mount in volume_mounts {
            let archive_file = format!("{}-{}.tar.gz", mount.service_id, mount.volume_id);
            let archive_dir = backup_dir.join("volumes");
            docker.run_volume_archive_helper(VolumeArchiveHelperRequest {
                volume_name: mount.docker_volume_name.clone(),
                archive_dir: archive_dir.clone(),
                archive_file: archive_file.clone(),
                mode: VolumeArchiveMode::Backup,
                timeout: VOLUME_HELPER_TIMEOUT,
            })?;
            let archive_path = archive_dir.join(&archive_file);
            let bytes =
                fs::read(&archive_path).map_err(|err| BackupError::Command(err.to_string()))?;
            manifest.push(PersistedBackupVolumeRecord {
                volume_id: mount.volume_id,
                docker_volume_name: mount.docker_volume_name,
                service_id: mount.service_id,
                mount_path: mount.mount_path,
                archive_file,
                archive_size_bytes: bytes.len() as u64,
                archive_sha256: hex::encode(Sha256::digest(bytes)),
                archive_files: inspect_archive_files(&archive_path)?,
            });
        }
        Ok((hooks, manifest))
    })();
    let (hooks, manifest) = match backup_result {
        Ok(result) => result,
        Err(err) => {
            cleanup_partial_backup_dir(&backup_dir)?;
            return Err(err);
        }
    };

    let source_deployment_id = build.deployment_id.clone();
    let metadata = PersistedBackupMetadata {
        backup_version: 1,
        backup_id: backup_id.clone(),
        project_id: project_id.into(),
        environment: environment.into(),
        created_at_unix: current_unix_timestamp(),
        source_generation: generation,
        source_deployment_id: Some(source_deployment_id.clone()),
        snapshot_metadata: snapshot,
        build_info: build,
        runtime_info: runtime,
        runtime_env_snapshot: Some(runtime_env_snapshot),
        resolved_runtime: resolved,
        services: services.keys().cloned().collect(),
        volumes: manifest.clone(),
        hooks,
        restores: Vec::new(),
        warnings: vec![
            "backups are crash-consistent only".into(),
            "Forge does not coordinate database quiescing".into(),
            "DB-consistent backups require service-level pre_backup_command hooks".into(),
            "backups are not PITR snapshots".into(),
        ],
    };
    write_backup_metadata(storage_root, &metadata)?;
    append_backup_event(
        &EventStore::new(env, generation),
        project_id,
        environment,
        generation,
        Some(source_deployment_id),
        "BACKUP_CREATED",
        Some(&backup_id),
    )?;
    Ok(api_backup_record(metadata))
}

pub fn list_backups(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
) -> Result<BackupListResponse, BackupError> {
    let root = backups_environment_root(storage_root, project_id, environment);
    let mut backups = Vec::new();
    let mut warnings = Vec::new();
    if !root.exists() {
        return Ok(BackupListResponse {
            project_id: project_id.into(),
            environment: environment.into(),
            backups,
            warnings,
        });
    }

    for entry in fs::read_dir(&root)
        .map_err(|err| backup_scan_error("failed to read backup directory", &root, err))?
    {
        let entry = entry.map_err(|err| {
            BackupError::Command(format!("failed to scan {}: {err}", root.display()))
        })?;
        if !entry
            .file_type()
            .map_err(|err| {
                BackupError::Command(format!(
                    "failed to inspect backup entry {}: {err}",
                    entry.path().display()
                ))
            })?
            .is_dir()
        {
            continue;
        }
        match read_backup_metadata(&entry.path()) {
            Ok(metadata) => backups.push(api_backup_record(metadata)),
            Err(BackupError::Command(message))
                if message.contains("metadata.json")
                    && message.contains("No such file or directory") =>
            {
                let backup_name = entry.file_name().to_string_lossy().into_owned();
                warnings.push(format!(
                    "skipped corrupt backup {backup_name}: missing metadata.json"
                ));
                warnings.push(format!(
                    "cleanup partial backup directory: {}",
                    entry.path().display()
                ));
            }
            Err(BackupError::Command(message)) | Err(BackupError::Invalid(message)) => {
                let backup_name = entry.file_name().to_string_lossy().into_owned();
                warnings.push(format!("skipped corrupt backup {backup_name}: {message}"));
                warnings.push(format!(
                    "cleanup partial backup directory: {}",
                    entry.path().display()
                ));
            }
            Err(err) => return Err(err),
        }
    }
    backups.sort_by(|left, right| right.created_at_unix.cmp(&left.created_at_unix));
    Ok(BackupListResponse {
        project_id: project_id.into(),
        environment: environment.into(),
        backups,
        warnings,
    })
}

pub fn inspect_backup(storage_root: &Path, backup_id: &str) -> Result<BackupRecord, BackupError> {
    Ok(api_backup_record(find_backup_metadata(
        storage_root,
        backup_id,
    )?))
}

pub fn restore_backup<D: DockerRuntime, R: RoutingRuntime>(
    storage_root: &Path,
    docker: &mut D,
    routing: &mut R,
    backup_id: &str,
) -> Result<BackupRestoreResponse, BackupError> {
    let mut metadata = find_backup_metadata(storage_root, backup_id)?;
    let env = EnvironmentPaths::new(storage_root, &metadata.project_id, &metadata.environment);
    env.ensure_exists()?;
    let generation = GenerationAllocator::new(env.clone()).allocate()?;
    let deployment_id = format!("restore-{}-gen-{}", backup_id, generation);
    let record = DeploymentRecord {
        deployment_id: deployment_id.clone(),
        project_id: metadata.project_id.clone(),
        environment: metadata.environment.clone(),
        intent: "restore".into(),
        source_path: None,
        source_ref: metadata.build_info.source_ref.clone(),
        repo_url: metadata.build_info.repo_url.clone(),
        commit_sha: metadata.build_info.commit_sha.clone(),
    };
    let writer = SnapshotWriter::new(env.clone(), generation)?;
    let lifecycle_store = LifecycleStore::new(env.clone(), generation);
    let diagnostics = DiagnosticsStore::new(env.clone(), generation);
    diagnostics.append_log_line("backup restore started", &[])?;
    persist_lifecycle(
        &lifecycle_store,
        &record,
        generation,
        DeploymentLifecycleState::Starting,
        "backup restore started",
        None,
        Some(PersistedPromotionSummary {
            gate_reason: Some(format!("restoring backup {backup_id}")),
            ..PersistedPromotionSummary::default()
        }),
    )?;

    let restored_at_unix = current_unix_timestamp();
    let domain =
        load_environment_domain(storage_root, &metadata.project_id, &metadata.environment)?;
    let runtime_env = restore_runtime_env(&metadata.resolved_runtime)
        .map_err(|err| BackupError::Invalid(err.to_string()))?;
    let mut runtime_env = runtime_env;
    runtime_env.extend(generated_forge_vars(&RuntimeEnvMetadata {
        project_id: metadata.project_id.clone(),
        environment: metadata.environment.clone(),
        generation,
        deployment_id: deployment_id.clone(),
        source_ref: metadata.build_info.source_ref.clone(),
        commit_sha: metadata.build_info.commit_sha.clone(),
        domain: domain.clone(),
    }));

    metadata.runtime_info = runtime_with_primary_service(&metadata.runtime_info);
    let source_services = runtime_services(&metadata.runtime_info);
    let service_count = source_services.len();
    let service_order = restore_service_order(&metadata.runtime_info, &source_services);
    let service_container_names = source_services
        .keys()
        .map(|service_id| {
            (
                service_id.clone(),
                generation_service_container_name(&record, generation, service_id, service_count),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut restored_services = BTreeMap::new();
    for service_id in &service_order {
        let service = source_services
            .get(service_id)
            .expect("restore service order should reference known services");
        let container_name = service_container_names
            .get(service_id)
            .cloned()
            .expect("container name should be precomputed");
        let volume_mounts = restore_volume_mounts(
            storage_root,
            docker,
            &diagnostics,
            generation,
            &record,
            service,
            &metadata,
        )?;
        docker.create_container(CreateContainerRequest {
            container_name: container_name.clone(),
            image_ref: service.image_ref.clone(),
            labels: service_labels(&record, generation, service_id),
            environment: runtime_env.clone(),
            network_name: service.network_name.clone(),
            network_aliases: if service_count > 1 {
                vec![service_id.clone(), container_name.clone()]
            } else {
                Vec::new()
            },
            volume_mounts: volume_mounts
                .iter()
                .map(|mount| VolumeMountRequest {
                    volume_name: mount.docker_volume_name.clone(),
                    mount_path: mount.mount_path.clone(),
                })
                .collect(),
            command: service.command.clone(),
            runtime_policy: crate::runtime::ContainerRuntimePolicy {
                cpu_limit: service.runtime_policy.cpu_limit.clone(),
                memory_limit_mb: service.runtime_policy.memory_limit_mb,
                restart_policy: service.runtime_policy.restart_policy.clone(),
                max_retries: service.runtime_policy.max_retries,
            },
        })?;
        docker.start_container(&container_name)?;
        let inspection = docker.inspect_container(&container_name)?;
        validate_inspection(&inspection, &container_name, &service.runtime_policy)?;
        restored_services.insert(
            service_id.clone(),
            PersistedServiceRuntimeInfo {
                service_id: service_id.clone(),
                container_name: inspection.container_name.clone(),
                image_ref: service.image_ref.clone(),
                running: inspection.running,
                state: PersistedServiceState::Healthy,
                network_name: service.network_name.clone(),
                probe_path: service.probe_path.clone(),
                activation: service.activation.clone(),
                command: service.command.clone(),
                runtime_policy: service.runtime_policy.clone(),
                runtime_usage: service.runtime_usage.clone(),
                termination: service.termination.clone(),
                depends_on: service.depends_on.clone(),
                required_for_promotion: service.required_for_promotion,
                externally_exposed: service.externally_exposed,
                environment_variables: service.environment_variables.clone(),
                state_config: service.state_config.clone(),
                volume_mounts: volume_mounts.clone(),
                source_ref: service.source_ref.clone(),
                repo_url: service.repo_url.clone(),
                commit_sha: service.commit_sha.clone(),
                source_path: service.source_path.clone(),
            },
        );
    }

    let primary_service = primary_service_id(&metadata.runtime_info, &restored_services);
    let primary_runtime = restored_services
        .get(&primary_service)
        .ok_or_else(|| BackupError::Invalid("restore topology has no primary service".into()))?;
    let restored_runtime = PersistedRuntimeInfo {
        container_name: primary_runtime.container_name.clone(),
        running: primary_runtime.running,
        network_name: primary_runtime.network_name.clone(),
        probe_path: primary_runtime.probe_path.clone(),
        activation: primary_runtime.activation.clone(),
        runtime_policy: primary_runtime.runtime_policy.clone(),
        runtime_usage: primary_runtime.runtime_usage.clone(),
        termination: primary_runtime.termination.clone(),
        environment_variables: primary_runtime.environment_variables.clone(),
        volume_mounts: primary_runtime.volume_mounts.clone(),
        source_ref: metadata.build_info.source_ref.clone(),
        repo_url: metadata.build_info.repo_url.clone(),
        commit_sha: metadata.build_info.commit_sha.clone(),
        source_path: metadata.build_info.source_path.clone(),
        services: restored_services.clone(),
        startup_order: metadata.runtime_info.startup_order.clone(),
    };
    let restored_build = PersistedBuildInfo {
        deployment_id: deployment_id.clone(),
        image_ref: primary_runtime.image_ref.clone(),
        services: metadata.build_info.services.clone(),
        source_ref: metadata.build_info.source_ref.clone(),
        repo_url: metadata.build_info.repo_url.clone(),
        commit_sha: metadata.build_info.commit_sha.clone(),
        source_path: metadata.build_info.source_path.clone(),
    };
    let restored_resolved = rewrite_resolved_runtime(
        &metadata.resolved_runtime,
        generation,
        &deployment_id,
        domain.clone(),
    );
    let source_runtime_env_snapshot = metadata
        .runtime_env_snapshot
        .clone()
        .unwrap_or_else(|| derive_runtime_env_snapshot(&metadata.resolved_runtime));
    let restored_runtime_env_snapshot = rewrite_runtime_env_snapshot(
        &source_runtime_env_snapshot,
        &restored_resolved,
        generation,
        &deployment_id,
        domain.clone(),
    );
    let restored_snapshot = rewrite_snapshot_metadata(
        &metadata.snapshot_metadata,
        generation,
        SnapshotState::Healthy,
    );

    writer.write_artifact(
        "build.json",
        &format!(
            "{}\n",
            serde_json::to_string_pretty(&restored_build).unwrap()
        ),
    )?;
    writer.write_artifact(
        "runtime.json",
        &format!(
            "{}\n",
            serde_json::to_string_pretty(&restored_runtime).unwrap()
        ),
    )?;
    writer.write_artifact(
        "resolved_runtime.json",
        &format!(
            "{}\n",
            serde_json::to_string_pretty(&restored_resolved).unwrap()
        ),
    )?;
    writer.write_artifact(
        "runtime_env_snapshot.json",
        &format!(
            "{}\n",
            serde_json::to_string_pretty(&restored_runtime_env_snapshot).unwrap()
        ),
    )?;
    writer.finalize(
        &metadata.project_id,
        &metadata.environment,
        SnapshotState::Healthy,
    )?;
    atomic_write(
        env.generation_dir(generation).join("snapshot.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&restored_snapshot).unwrap()
        )
        .as_bytes(),
    )?;

    for service in source_services.values() {
        if service.externally_exposed {
            continue;
        }
        docker.stop_container(&service.container_name)?;
    }
    for (service_id, service) in &restored_services {
        if !service.externally_exposed {
            continue;
        }
        let Some(PersistedActivationMode::Http {
            internal_port,
            route_subtree_id,
            target_source,
        }) = service.activation.as_ref()
        else {
            continue;
        };
        let inspection = docker.inspect_container(&service.container_name)?;
        let target = resolve_route_target(
            &inspection,
            *internal_port,
            service.network_name.as_deref(),
            target_source,
        )
        .ok_or_else(|| BackupError::Invalid("restored service missing route target".into()))?;
        let subtree_id = route_subtree_id
            .clone()
            .unwrap_or_else(|| route_subtree_id_for_service(&record, service_id, service_count));
        routing.update_route(RouteUpdateRequest {
            subtree_id: subtree_id.clone(),
            target: target.clone(),
            domain: domain.clone(),
            health_checks_enabled: false,
            probe_path: service.probe_path.clone(),
        })?;
        let route = routing.inspect_route(&subtree_id)?;
        validate_route_activation(&route, &target)?;
    }

    PointerStore::new(env.clone()).swap_current(generation)?;
    RuntimeStateStore::new(env.clone()).save(&RuntimeState {
        active_generation: Some(generation),
        health_state: RuntimeHealthState::Healthy,
        failed_probe_count: 0,
        successful_probe_count: 0,
        restart_attempted: false,
        degraded_since_unix: None,
        last_transition: "restore_completed".into(),
        last_error_code: None,
    })?;
    persist_lifecycle(
        &lifecycle_store,
        &record,
        generation,
        DeploymentLifecycleState::Promoted,
        "backup restore completed",
        None,
        Some(PersistedPromotionSummary {
            warmup_succeeded: true,
            validation_succeeded: true,
            route_verification_succeeded: true,
            runtime_snapshot_persisted: true,
            convergence_target_stable: true,
            promoted_at_unix: Some(restored_at_unix),
            gate_reason: None,
        }),
    )?;
    diagnostics.append_log_line(
        &format!("runtime env snapshot restored for generation {generation}"),
        &[],
    )?;
    diagnostics.append_log_line(
        &format!(
            "restored backup {} from gen-{}",
            backup_id, metadata.source_generation
        ),
        &[],
    )?;
    for volume in &metadata.volumes {
        diagnostics.append_log_line(
            &format!(
                "restored volume {}:{} -> {}",
                volume.service_id, volume.volume_id, volume.mount_path
            ),
            &[],
        )?;
    }
    update_generation_history(&env, generation, |entry| {
        entry.deployment_id = Some(deployment_id.clone());
        entry.commit_sha = metadata.build_info.commit_sha.clone();
        entry.source_ref = metadata.build_info.source_ref.clone();
        entry.image_ref = Some(primary_runtime.image_ref.clone());
        entry.created_at_unix = Some(restored_at_unix);
        entry.finalized_at_unix = Some(restored_at_unix);
        entry.promoted_at_unix = Some(restored_at_unix);
        entry.finalized_state = Some("healthy".into());
        entry.retained = true;
        entry.restored_from_backup_id = Some(backup_id.into());
        entry.restored_from_generation = Some(metadata.source_generation);
        entry.restored_from_deployment_id = metadata.source_deployment_id.clone();
        entry.restored_at_unix = Some(restored_at_unix);
    })?;
    metadata.restores.push(PersistedBackupRestoreRecord {
        restored_generation: generation,
        restored_deployment_id: deployment_id.clone(),
        restored_at_unix,
        status: "completed".into(),
    });
    write_backup_metadata(storage_root, &metadata)?;
    append_backup_event(
        &EventStore::new(env.clone(), generation),
        &metadata.project_id,
        &metadata.environment,
        generation,
        Some(deployment_id.clone()),
        "BACKUP_RESTORE_COMPLETED",
        Some(backup_id),
    )?;
    Ok(BackupRestoreResponse {
        backup_id: backup_id.into(),
        restored_generation: generation,
        restored_deployment_id: deployment_id,
        restored_at_unix,
    })
}

pub fn scan_backup_gc_actions(
    storage_root: &Path,
) -> Result<Vec<(String, String, Option<u64>, String, String)>, BackupError> {
    let root = EnvironmentPaths::backups_root(storage_root);
    let mut actions = Vec::new();
    if !root.exists() {
        return Ok(actions);
    }
    for project in fs::read_dir(&root).map_err(|err| BackupError::Command(err.to_string()))? {
        let project = project.map_err(|err| BackupError::Command(err.to_string()))?;
        if !project
            .file_type()
            .map_err(|err| BackupError::Command(err.to_string()))?
            .is_dir()
        {
            continue;
        }
        let project_id = project.file_name().to_string_lossy().into_owned();
        for environment in
            fs::read_dir(project.path()).map_err(|err| BackupError::Command(err.to_string()))?
        {
            let environment = environment.map_err(|err| BackupError::Command(err.to_string()))?;
            if !environment
                .file_type()
                .map_err(|err| BackupError::Command(err.to_string()))?
                .is_dir()
            {
                continue;
            }
            let env_name = environment.file_name().to_string_lossy().into_owned();
            for backup in fs::read_dir(environment.path())
                .map_err(|err| BackupError::Command(err.to_string()))?
            {
                let backup = backup.map_err(|err| BackupError::Command(err.to_string()))?;
                if !backup
                    .file_type()
                    .map_err(|err| BackupError::Command(err.to_string()))?
                    .is_dir()
                {
                    continue;
                }
                let metadata = read_backup_metadata(&backup.path())?;
                let env = EnvironmentPaths::new(storage_root, &project_id, &env_name);
                let reason = if env.generation_dir(metadata.source_generation).exists() {
                    "backup preserved".to_string()
                } else {
                    "backup references removed generation".to_string()
                };
                actions.push((
                    project_id.clone(),
                    env_name.clone(),
                    Some(metadata.source_generation),
                    metadata.backup_id,
                    reason,
                ));
            }
        }
    }
    Ok(actions)
}

pub fn load_backup_restore_lineage(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
    record: &GenerationHistoryRecord,
) -> Option<crate::api::RestoreLineage> {
    let (backup_id, metadata, restore_record) =
        if let Some(backup_id) = record.restored_from_backup_id.clone() {
            let metadata = find_backup_metadata(storage_root, &backup_id).ok();
            let restore_record = metadata
                .as_ref()
                .and_then(|metadata| matching_restore_record(metadata, record));
            (backup_id, metadata, restore_record)
        } else if let Some(backup_id) =
            backup_id_from_restore_deployment_id(record.deployment_id.as_deref())
        {
            let metadata = find_backup_metadata(storage_root, &backup_id).ok();
            let restore_record = metadata
                .as_ref()
                .and_then(|metadata| matching_restore_record(metadata, record));
            (backup_id, metadata, restore_record)
        } else {
            let (backup_id, metadata, restore_record) =
                find_backup_restore_metadata(storage_root, project_id, environment, record)?;
            (backup_id, Some(metadata), restore_record)
        };

    let restored_volumes = metadata
        .as_ref()
        .map(|metadata| {
            restored_backup_volumes(storage_root, project_id, environment, record, metadata)
        })
        .unwrap_or_default();

    Some(crate::api::RestoreLineage {
        backup_id,
        restored_generation: record.generation,
        source_generation: metadata
            .as_ref()
            .map(|metadata| metadata.source_generation)
            .or(record.restored_from_generation),
        source_deployment_id: metadata
            .as_ref()
            .and_then(|metadata| metadata.source_deployment_id.clone())
            .or_else(|| record.restored_from_deployment_id.clone()),
        restored_at_unix: restore_record
            .as_ref()
            .map(|restore| restore.restored_at_unix)
            .or(record.restored_at_unix),
        hook_succeeded: metadata.as_ref().and_then(|metadata| {
            (!metadata.hooks.is_empty())
                .then(|| metadata.hooks.iter().all(|hook| hook.exit_code == 0))
        }),
        restored_volumes,
    })
}

fn backup_id_from_restore_deployment_id(deployment_id: Option<&str>) -> Option<String> {
    let deployment_id = deployment_id?;
    let suffix = deployment_id.strip_prefix("restore-")?;
    let backup_suffix = suffix.rsplit_once("-gen-")?.0;
    (!backup_suffix.is_empty()).then(|| backup_suffix.to_string())
}

fn find_backup_restore_metadata(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
    record: &GenerationHistoryRecord,
) -> Option<(
    String,
    PersistedBackupMetadata,
    Option<PersistedBackupRestoreRecord>,
)> {
    let backups_root = backups_environment_root(storage_root, project_id, environment);
    let entries = fs::read_dir(backups_root).ok()?;
    for entry in entries.flatten() {
        let metadata = read_backup_metadata(&entry.path()).ok()?;
        let restore_record = metadata
            .restores
            .iter()
            .find(|restore| {
                restore.restored_generation == record.generation
                    || Some(restore.restored_deployment_id.as_str())
                        == record.deployment_id.as_deref()
            })
            .cloned();
        if restore_record.is_some() {
            return Some((metadata.backup_id.clone(), metadata, restore_record));
        }
    }
    None
}

fn matching_restore_record(
    metadata: &PersistedBackupMetadata,
    record: &GenerationHistoryRecord,
) -> Option<PersistedBackupRestoreRecord> {
    metadata
        .restores
        .iter()
        .find(|restore| {
            restore.restored_generation == record.generation
                || Some(restore.restored_deployment_id.as_str()) == record.deployment_id.as_deref()
        })
        .cloned()
}

fn restored_backup_volumes(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
    record: &GenerationHistoryRecord,
    metadata: &PersistedBackupMetadata,
) -> Vec<BackupVolumeRecord> {
    let restored_runtime = load_generation_runtime_info(
        &EnvironmentPaths::new(storage_root, project_id, environment),
        record.generation,
    )
    .ok()
    .flatten();
    let restored_mounts = restored_runtime
        .as_ref()
        .map(runtime_services)
        .unwrap_or_default()
        .into_values()
        .flat_map(|service| service.volume_mounts.into_iter())
        .map(|mount| {
            (
                (mount.service_id.clone(), mount.volume_id.clone()),
                mount.docker_volume_name,
            )
        })
        .collect::<BTreeMap<_, _>>();

    metadata
        .volumes
        .iter()
        .map(|volume| BackupVolumeRecord {
            volume_id: volume.volume_id.clone(),
            docker_volume_name: volume.docker_volume_name.clone(),
            service_id: volume.service_id.clone(),
            mount_path: volume.mount_path.clone(),
            archive_file: volume.archive_file.clone(),
            archive_size_bytes: volume.archive_size_bytes,
            archive_sha256: volume.archive_sha256.clone(),
            archive_files: volume
                .archive_files
                .iter()
                .map(|file| BackupArchiveFileRecord {
                    path: file.path.clone(),
                    size_bytes: file.size_bytes,
                    sha256: file.sha256.clone(),
                })
                .collect(),
            restored_docker_volume_name: restored_mounts
                .get(&(volume.service_id.clone(), volume.volume_id.clone()))
                .cloned(),
        })
        .collect()
}

fn inspect_archive_files(
    archive_path: &Path,
) -> Result<Vec<crate::storage::PersistedBackupArchiveFileRecord>, BackupError> {
    let bytes = fs::read(archive_path).map_err(|err| BackupError::Command(err.to_string()))?;
    if let Ok(entries) = serde_json::from_slice::<Vec<(String, Vec<u8>)>>(&bytes) {
        let mut files = entries
            .into_iter()
            .map(
                |(path, contents)| crate::storage::PersistedBackupArchiveFileRecord {
                    path,
                    size_bytes: contents.len() as u64,
                    sha256: hex::encode(Sha256::digest(&contents)),
                },
            )
            .collect::<Vec<_>>();
        files.sort_by(|left, right| left.path.cmp(&right.path));
        return Ok(files);
    }
    let list_output = Command::new("tar")
        .args(["-tzf", archive_path.to_string_lossy().as_ref()])
        .output()
        .map_err(|err| BackupError::Command(format!("failed to list archive files: {err}")))?;
    if !list_output.status.success() {
        return Err(BackupError::Invalid(format!(
            "failed to list archive files in {}: {}",
            archive_path.display(),
            String::from_utf8_lossy(&list_output.stderr).trim()
        )));
    }
    let mut files = Vec::new();
    for raw_path in String::from_utf8_lossy(&list_output.stdout).lines() {
        let path = raw_path.trim().trim_start_matches("./");
        if path.is_empty() || path.ends_with('/') {
            continue;
        }
        let entry_output = Command::new("tar")
            .arg("-xOzf")
            .arg(archive_path)
            .arg(raw_path)
            .output()
            .map_err(|err| {
                BackupError::Command(format!(
                    "failed to read archive entry {raw_path} from {}: {err}",
                    archive_path.display()
                ))
            })?;
        if !entry_output.status.success() {
            return Err(BackupError::Invalid(format!(
                "failed to read archive entry {raw_path} from {}: {}",
                archive_path.display(),
                String::from_utf8_lossy(&entry_output.stderr).trim()
            )));
        }
        files.push(crate::storage::PersistedBackupArchiveFileRecord {
            path: path.to_string(),
            size_bytes: entry_output.stdout.len() as u64,
            sha256: hex::encode(Sha256::digest(&entry_output.stdout)),
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn backups_environment_root(storage_root: &Path, project_id: &str, environment: &str) -> PathBuf {
    EnvironmentPaths::backups_root(storage_root)
        .join(project_id)
        .join(environment)
}

fn backup_dir(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
    backup_id: &str,
) -> PathBuf {
    backups_environment_root(storage_root, project_id, environment).join(backup_id)
}

fn write_backup_metadata(
    storage_root: &Path,
    metadata: &PersistedBackupMetadata,
) -> Result<(), BackupError> {
    let path = backup_dir(
        storage_root,
        &metadata.project_id,
        &metadata.environment,
        &metadata.backup_id,
    )
    .join("metadata.json");
    atomic_write(
        path,
        format!("{}\n", serde_json::to_string_pretty(metadata).unwrap()).as_bytes(),
    )?;
    Ok(())
}

fn cleanup_partial_backup_dir(path: &Path) -> Result<(), BackupError> {
    if !path.exists() {
        return Ok(());
    }
    fs::remove_dir_all(path).map_err(|err| {
        BackupError::Command(format!(
            "backup failed and cleanup of partial backup directory {} also failed: {err}",
            path.display()
        ))
    })
}

fn read_backup_metadata(path: &Path) -> Result<PersistedBackupMetadata, BackupError> {
    let metadata_path = path.join("metadata.json");
    let raw = fs::read_to_string(&metadata_path).map_err(|err| {
        BackupError::Command(format!(
            "failed to read backup metadata {}: {err}",
            metadata_path.display()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|err| {
        BackupError::Invalid(format!(
            "failed to parse backup metadata {}: {err}",
            metadata_path.display()
        ))
    })
}

fn find_backup_metadata(
    storage_root: &Path,
    backup_id: &str,
) -> Result<PersistedBackupMetadata, BackupError> {
    let root = EnvironmentPaths::backups_root(storage_root);
    if !root.exists() {
        return Err(BackupError::NotFound(format!(
            "backup {backup_id} not found"
        )));
    }
    for project in fs::read_dir(&root)
        .map_err(|err| backup_scan_error("failed to read backup root", &root, err))?
    {
        let project = project.map_err(|err| {
            BackupError::Command(format!("failed to scan {}: {err}", root.display()))
        })?;
        for environment in fs::read_dir(project.path()).map_err(|err| {
            backup_scan_error(
                "failed to read project backup directory",
                &project.path(),
                err,
            )
        })? {
            let environment = environment.map_err(|err| {
                BackupError::Command(format!(
                    "failed to scan backup project directory {}: {err}",
                    project.path().display()
                ))
            })?;
            let candidate = environment.path().join(backup_id);
            if candidate.exists() {
                return read_backup_metadata(&candidate);
            }
        }
    }
    Err(BackupError::NotFound(format!(
        "backup {backup_id} not found"
    )))
}

fn api_backup_record(metadata: PersistedBackupMetadata) -> BackupRecord {
    BackupRecord {
        backup_id: metadata.backup_id,
        project_id: metadata.project_id,
        environment: metadata.environment,
        created_at_unix: metadata.created_at_unix,
        source_generation: metadata.source_generation,
        source_deployment_id: metadata.source_deployment_id,
        services: metadata.services,
        volumes: metadata
            .volumes
            .into_iter()
            .map(|volume| BackupVolumeRecord {
                volume_id: volume.volume_id,
                docker_volume_name: volume.docker_volume_name,
                service_id: volume.service_id,
                mount_path: volume.mount_path,
                archive_file: volume.archive_file,
                archive_size_bytes: volume.archive_size_bytes,
                archive_sha256: volume.archive_sha256,
                archive_files: volume
                    .archive_files
                    .into_iter()
                    .map(|file| BackupArchiveFileRecord {
                        path: file.path,
                        size_bytes: file.size_bytes,
                        sha256: file.sha256,
                    })
                    .collect(),
                restored_docker_volume_name: None,
            })
            .collect(),
        hooks: metadata
            .hooks
            .into_iter()
            .map(|hook| BackupHookRecord {
                service_id: hook.service_id,
                volume_id: hook.volume_id,
                container_name: hook.container_name,
                pre_backup_command: hook.pre_backup_command,
                started_at_unix: hook.started_at_unix,
                completed_at_unix: hook.completed_at_unix,
                timeout_seconds: hook.timeout_seconds,
                stdout: hook.stdout,
                stderr: hook.stderr,
                exit_code: hook.exit_code,
            })
            .collect(),
        restores: metadata
            .restores
            .into_iter()
            .map(|restore| RestoreRecord {
                restored_generation: restore.restored_generation,
                restored_deployment_id: restore.restored_deployment_id,
                restored_at_unix: restore.restored_at_unix,
                status: restore.status,
            })
            .collect(),
    }
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
            image_ref: runtime.container_name.clone(),
            running: runtime.running,
            state: PersistedServiceState::Healthy,
            network_name: runtime.network_name.clone(),
            probe_path: runtime.probe_path.clone(),
            activation: runtime.activation.clone(),
            command: None,
            runtime_policy: runtime.runtime_policy.clone(),
            runtime_usage: runtime.runtime_usage.clone(),
            termination: runtime.termination.clone(),
            depends_on: Vec::new(),
            required_for_promotion: true,
            externally_exposed: matches!(
                runtime.activation,
                Some(PersistedActivationMode::Http { .. })
            ),
            environment_variables: runtime.environment_variables.clone(),
            state_config: None,
            volume_mounts: runtime.volume_mounts.clone(),
            source_ref: runtime.source_ref.clone(),
            repo_url: runtime.repo_url.clone(),
            commit_sha: runtime.commit_sha.clone(),
            source_path: runtime.source_path.clone(),
        },
    )])
}

fn restore_service_order(
    runtime: &PersistedRuntimeInfo,
    services: &BTreeMap<String, PersistedServiceRuntimeInfo>,
) -> Vec<String> {
    let mut ordered = runtime
        .startup_order
        .iter()
        .filter(|service_id| services.contains_key(*service_id))
        .cloned()
        .collect::<Vec<_>>();
    for service_id in services.keys() {
        if !ordered.contains(service_id) {
            ordered.push(service_id.clone());
        }
    }
    ordered
}

fn run_pre_backup_hooks<D: DockerRuntime>(
    docker: &mut D,
    services: &BTreeMap<String, PersistedServiceRuntimeInfo>,
) -> Result<Vec<PersistedBackupHookRecord>, BackupError> {
    let mut hooks = Vec::new();
    for service in services.values() {
        for mount in &service.volume_mounts {
            let Some(state_config) = service.state_config.as_ref() else {
                continue;
            };
            let Some(command) = state_config.pre_backup_command.as_ref() else {
                continue;
            };
            if mount.volume_id != state_config.volume {
                continue;
            }
            if !service.running {
                return Err(BackupError::Invalid(format!(
                    "pre_backup_command requires running container for service `{}`",
                    service.service_id
                )));
            }
            let output = docker.exec_in_container(ExecInContainerRequest {
                container_name: service.container_name.clone(),
                command: vec!["sh".into(), "-lc".into(), command.clone()],
                timeout: PRE_BACKUP_HOOK_TIMEOUT,
            })?;
            let completed_at_unix = current_unix_timestamp();
            let record = PersistedBackupHookRecord {
                service_id: service.service_id.clone(),
                volume_id: mount.volume_id.clone(),
                container_name: service.container_name.clone(),
                pre_backup_command: command.clone(),
                started_at_unix: Some(completed_at_unix),
                completed_at_unix: Some(completed_at_unix),
                timeout_seconds: PRE_BACKUP_HOOK_TIMEOUT.as_secs(),
                stdout: output.stdout,
                stderr: output.stderr,
                exit_code: output.exit_code,
            };
            if record.exit_code != 0 {
                return Err(BackupError::Command(format!(
                    "pre_backup_command failed for service `{}` volume `{}` with exit code {}",
                    record.service_id, record.volume_id, record.exit_code
                )));
            }
            hooks.push(record);
        }
    }
    Ok(hooks)
}

fn restore_volume_mounts<D: DockerRuntime>(
    storage_root: &Path,
    docker: &mut D,
    diagnostics: &DiagnosticsStore,
    generation: u64,
    record: &DeploymentRecord,
    service: &PersistedServiceRuntimeInfo,
    metadata: &PersistedBackupMetadata,
) -> Result<Vec<PersistedVolumeMount>, BackupError> {
    let mut restored = Vec::new();
    let backup_root = backup_dir(
        storage_root,
        &metadata.project_id,
        &metadata.environment,
        &metadata.backup_id,
    );
    for mount in &service.volume_mounts {
        if !matches!(mount.retention, PersistedVolumeRetention::Persistent) {
            continue;
        }
        let volume_name = format!(
            "forge-{}-{}-restore-gen-{}-vol-{}",
            record.project_id, record.environment, generation, mount.volume_id
        );
        docker.ensure_volume(CreateVolumeRequest {
            volume_name: volume_name.clone(),
            labels: BTreeMap::from([
                ("forge.managed".into(), "true".into()),
                ("forge.project_id".into(), record.project_id.clone()),
                ("forge.environment".into(), record.environment.clone()),
                ("forge.generation".into(), generation.to_string()),
                ("forge.service_id".into(), service.service_id.clone()),
                ("forge.volume_id".into(), mount.volume_id.clone()),
                ("forge.volume_retention".into(), "persistent".into()),
            ]),
        })?;
        let backup = metadata
            .volumes
            .iter()
            .find(|entry| {
                entry.service_id == service.service_id && entry.volume_id == mount.volume_id
            })
            .ok_or_else(|| {
                BackupError::NotFound(format!(
                    "backup archive missing for service {} volume {}",
                    service.service_id, mount.volume_id
                ))
            })?;
        let helper_result = docker.run_volume_archive_helper(VolumeArchiveHelperRequest {
            volume_name: volume_name.clone(),
            archive_dir: backup_root.join("volumes"),
            archive_file: backup.archive_file.clone(),
            mode: VolumeArchiveMode::Restore,
            timeout: VOLUME_HELPER_TIMEOUT,
        });
        match helper_result {
            Ok(output) => {
                if !output.stdout.is_empty() {
                    diagnostics.append_log_line(
                        &format!(
                            "restore helper stdout for {}:{}: {}",
                            service.service_id, mount.volume_id, output.stdout
                        ),
                        &[],
                    )?;
                }
                if !output.stderr.is_empty() {
                    diagnostics.append_log_line(
                        &format!(
                            "restore helper stderr for {}:{}: {}",
                            service.service_id, mount.volume_id, output.stderr
                        ),
                        &[],
                    )?;
                }
            }
            Err(err) => {
                let reason = format!(
                    "restore helper failed for service {} volume {}: {}",
                    service.service_id, mount.volume_id, err
                );
                diagnostics.append_log_line(&reason, &[])?;
                diagnostics.write_failure_reason(&reason, &[])?;
                return Err(BackupError::Docker(err));
            }
        }
        restored.push(PersistedVolumeMount {
            volume_id: mount.volume_id.clone(),
            docker_volume_name: volume_name,
            mount_path: mount.mount_path.clone(),
            service_id: service.service_id.clone(),
            generation,
            retention: PersistedVolumeRetention::Persistent,
        });
    }
    Ok(restored)
}

fn rewrite_resolved_runtime(
    resolved: &PersistedResolvedRuntime,
    generation: u64,
    deployment_id: &str,
    domain: Option<String>,
) -> PersistedResolvedRuntime {
    let mut restored = resolved.clone();
    restored.generation = generation;
    restored.deployment_id = deployment_id.into();
    restored.domain = domain.clone();
    for (key, value) in generated_forge_vars(&RuntimeEnvMetadata {
        project_id: restored.project_id.clone(),
        environment: restored.environment.clone(),
        generation,
        deployment_id: deployment_id.into(),
        source_ref: restored.source_ref.clone(),
        commit_sha: restored.commit_sha.clone(),
        domain,
    }) {
        restored.entries.insert(
            key,
            PersistedResolvedRuntimeEntry {
                source: crate::storage::PersistedRuntimeEnvSource::ForgeGenerated,
                value: Some(value),
                secret_reference: None,
                sealed_value: None,
                sensitive: false,
            },
        );
    }
    restored
}

fn derive_runtime_env_snapshot(resolved: &PersistedResolvedRuntime) -> PersistedRuntimeEnvSnapshot {
    let entries = resolved
        .entries
        .iter()
        .map(|(key, entry)| {
            (
                key.clone(),
                PersistedRuntimeEnvEntry {
                    source: entry.source.clone(),
                    value: if entry.sensitive {
                        None
                    } else {
                        entry.value.clone()
                    },
                    secret_reference: entry.secret_reference.clone(),
                    sensitive: entry.sensitive,
                    redacted: entry.sensitive,
                },
            )
        })
        .collect();
    PersistedRuntimeEnvSnapshot {
        snapshot_version: resolved.snapshot_version,
        project_id: resolved.project_id.clone(),
        environment: resolved.environment.clone(),
        generation: resolved.generation,
        deployment_id: resolved.deployment_id.clone(),
        source_environment: resolved.source_environment.clone(),
        source_ref: resolved.source_ref.clone(),
        commit_sha: resolved.commit_sha.clone(),
        domain: resolved.domain.clone(),
        resolution_order: Vec::new(),
        entries,
    }
}

fn rewrite_runtime_env_snapshot(
    snapshot: &PersistedRuntimeEnvSnapshot,
    resolved: &PersistedResolvedRuntime,
    generation: u64,
    deployment_id: &str,
    domain: Option<String>,
) -> PersistedRuntimeEnvSnapshot {
    let mut restored = snapshot.clone();
    restored.generation = generation;
    restored.deployment_id = deployment_id.into();
    restored.domain = domain;
    for (key, entry) in &resolved.entries {
        let rendered_value = if entry.sensitive {
            None
        } else {
            entry.value.clone()
        };
        restored.entries.insert(
            key.clone(),
            PersistedRuntimeEnvEntry {
                source: entry.source.clone(),
                value: rendered_value,
                secret_reference: entry.secret_reference.clone(),
                sensitive: entry.sensitive,
                redacted: entry.sensitive,
            },
        );
    }
    restored
}

fn rewrite_snapshot_metadata(
    snapshot: &PersistedSnapshotMetadata,
    generation: u64,
    state: SnapshotState,
) -> PersistedSnapshotMetadata {
    PersistedSnapshotMetadata {
        snapshot_version: snapshot.snapshot_version,
        project_id: snapshot.project_id.clone(),
        environment: snapshot.environment.clone(),
        generation,
        state: match state {
            SnapshotState::Healthy => "healthy".into(),
            SnapshotState::Degraded => "degraded".into(),
            SnapshotState::Failed => "failed".into(),
            SnapshotState::Stopped => "stopped".into(),
            SnapshotState::Rollback => "rollback".into(),
        },
        finalized_at_unix: current_unix_timestamp(),
    }
}

fn update_generation_history<F>(
    env: &EnvironmentPaths,
    generation: u64,
    mut apply: F,
) -> Result<(), BackupError>
where
    F: FnMut(&mut GenerationHistoryRecord),
{
    let store = RetentionStore::new(env.clone());
    let mut metadata = store.read()?;
    let mut updated = false;
    for entry in &mut metadata.generations {
        if entry.generation == generation {
            apply(entry);
            updated = true;
            break;
        }
    }
    if !updated {
        let mut entry = GenerationHistoryRecord {
            generation,
            ..GenerationHistoryRecord::default()
        };
        apply(&mut entry);
        metadata.generations.push(entry);
        metadata.generations.sort_by_key(|entry| entry.generation);
    }
    metadata.updated_at_unix = Some(current_unix_timestamp());
    store.write(&metadata)?;
    Ok(())
}

fn persist_lifecycle(
    store: &LifecycleStore,
    record: &DeploymentRecord,
    generation: u64,
    state: DeploymentLifecycleState,
    transition_reason: &str,
    validation_summary: Option<PersistedValidationSummary>,
    promotion_summary: Option<PersistedPromotionSummary>,
) -> Result<(), BackupError> {
    let entered_at_unix = current_unix_timestamp();
    let mut lifecycle = store.read()?.unwrap_or(PersistedDeploymentLifecycle {
        lifecycle_version: 1,
        project_id: record.project_id.clone(),
        environment: record.environment.clone(),
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

fn append_backup_event(
    store: &EventStore,
    project_id: &str,
    environment: &str,
    generation: u64,
    deployment_id: Option<String>,
    event_type: &str,
    reason: Option<&str>,
) -> Result<(), BackupError> {
    store.append(&EventRecord {
        timestamp_unix: current_unix_timestamp(),
        project_id: project_id.into(),
        environment: environment.into(),
        generation: Some(generation),
        deployment_id,
        event_type: event_type.into(),
        reason: reason.map(|value| value.to_string()),
    })?;
    Ok(())
}

fn backup_scan_error(prefix: &str, path: &Path, err: std::io::Error) -> BackupError {
    BackupError::Command(format!("{prefix} {}: {err}", path.display()))
}

fn validate_inspection(
    inspection: &ContainerInspection,
    expected_container_name: &str,
    expected_policy: &crate::storage::PersistedRuntimePolicy,
) -> Result<(), BackupError> {
    if inspection.container_name != expected_container_name {
        return Err(BackupError::Invalid(
            "inspected container name mismatch".into(),
        ));
    }
    if !inspection.running {
        return Err(BackupError::Invalid(
            "restored container is not running".into(),
        ));
    }
    if crate::storage::normalize_restart_policy_name(&inspection.restart_policy)
        != crate::storage::normalize_restart_policy_name(&expected_policy.restart_policy)
        || crate::deployments::normalize_restart_max_retries(
            &crate::storage::normalize_restart_policy_name(&inspection.restart_policy),
            inspection.restart_max_retries,
        ) != expected_policy.max_retries
        || inspection.cpu_limit != expected_policy.cpu_limit
        || inspection.memory_limit_mb != expected_policy.memory_limit_mb
    {
        return Err(BackupError::Invalid(
            "restored container runtime policy mismatch".into(),
        ));
    }
    Ok(())
}

fn validate_route_activation(
    inspection: &RouteInspection,
    expected_target: &str,
) -> Result<(), BackupError> {
    if inspection.active_target != expected_target {
        return Err(BackupError::Invalid(format!(
            "route target mismatch: current={} expected={expected_target}",
            inspection.active_target
        )));
    }
    Ok(())
}

fn load_environment_domain(
    storage_root: &Path,
    project_id: &str,
    environment: &str,
) -> Result<Option<String>, BackupError> {
    let project = ProjectRegistryStore::new(storage_root)
        .get(project_id)
        .map_err(|err| BackupError::Invalid(err.to_string()))?;
    Ok(project.map(|project| derive_environment_domain(&project.base_domain, environment)))
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
    if service_count <= 1 && (service_id == record.project_id || service_id == "default") {
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

fn route_subtree_id_for_service(
    record: &DeploymentRecord,
    service_id: &str,
    service_count: usize,
) -> String {
    if service_count <= 1 {
        return format!("forge:{}:{}", record.project_id, record.environment);
    }
    format!(
        "forge:{}:{}:{service_id}",
        record.project_id, record.environment
    )
}

fn service_labels(
    record: &DeploymentRecord,
    generation: u64,
    service_id: &str,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("forge.managed".into(), "true".into()),
        ("forge.project_id".into(), record.project_id.clone()),
        ("forge.environment".into(), record.environment.clone()),
        ("forge.generation".into(), generation.to_string()),
        ("forge.service_id".into(), service_id.into()),
    ])
}

fn primary_service_id(
    runtime: &PersistedRuntimeInfo,
    restored_services: &BTreeMap<String, PersistedServiceRuntimeInfo>,
) -> String {
    select_primary_service_id(runtime, restored_services).unwrap_or_else(|| {
        restored_services
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| "default".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::api::ProjectUpsertRequest;
    use crate::runtime::{
        ExecInContainerOutput, ExecInContainerRequest, ManagedImage, ManagedVolume,
        RouteUpdateRequest, VolumeArchiveHelperOutput, VolumeArchiveHelperRequest,
        VolumeArchiveMode, VolumeInspection,
    };
    use crate::secrets::seal_value;
    use crate::status::{load_environment_diagnostics, load_project_environment_env_report};
    use crate::storage::{
        PersistedResolvedRuntime, PersistedResolvedRuntimeEntry, PersistedRuntimeEnvSource,
        PersistedServiceState, PointerStore, SnapshotWriter, load_generation_runtime_env_snapshot,
    };

    #[derive(Default)]
    struct TestRoutingRuntime {
        routes: BTreeMap<String, RouteInspection>,
        event_log: Option<Arc<Mutex<Vec<String>>>>,
    }

    impl RoutingRuntime for TestRoutingRuntime {
        fn update_route(
            &mut self,
            request: RouteUpdateRequest,
        ) -> Result<(), crate::runtime::RoutingRuntimeError> {
            if let Some(log) = &self.event_log {
                log.lock()
                    .unwrap()
                    .push(format!("route:update:{}", request.subtree_id));
            }
            self.routes.insert(
                request.subtree_id.clone(),
                RouteInspection {
                    subtree_id: request.subtree_id,
                    active_target: request.target,
                    domain: request.domain,
                    activation_verified: true,
                    verification_url: None,
                    verification_host: None,
                    verification_status_code: None,
                    verification_response_body: None,
                    health_checks_enabled: request.health_checks_enabled,
                },
            );
            Ok(())
        }

        fn inspect_route(
            &mut self,
            subtree_id: &str,
        ) -> Result<RouteInspection, crate::runtime::RoutingRuntimeError> {
            if let Some(log) = &self.event_log {
                log.lock()
                    .unwrap()
                    .push(format!("route:inspect:{subtree_id}"));
            }
            self.routes.get(subtree_id).cloned().ok_or_else(|| {
                crate::runtime::RoutingRuntimeError::InspectionFailed(subtree_id.into())
            })
        }

        fn list_managed_routes(
            &mut self,
        ) -> Result<Vec<RouteInspection>, crate::runtime::RoutingRuntimeError> {
            Ok(self.routes.values().cloned().collect())
        }

        fn remove_route(
            &mut self,
            subtree_id: &str,
        ) -> Result<(), crate::runtime::RoutingRuntimeError> {
            self.routes.remove(subtree_id);
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestDockerRuntime {
        volume_inspections: BTreeMap<String, VolumeInspection>,
        container_inspections: BTreeMap<String, ContainerInspection>,
        created_containers: Vec<CreateContainerRequest>,
        started_containers: Vec<String>,
        stopped_containers: Vec<String>,
        next_container_ip: VecDeque<String>,
        helper_requests: Vec<VolumeArchiveHelperRequest>,
        helper_results: VecDeque<Result<VolumeArchiveHelperOutput, DockerRuntimeError>>,
        exec_requests: Vec<ExecInContainerRequest>,
        exec_results: VecDeque<Result<ExecInContainerOutput, DockerRuntimeError>>,
        exec_file_writes: VecDeque<Vec<(PathBuf, Vec<u8>)>>,
        inspect_volume_calls: Vec<String>,
        fail_inspect_volume: bool,
        event_log: Option<Arc<Mutex<Vec<String>>>>,
    }

    impl DockerRuntime for TestDockerRuntime {
        fn build_image(
            &mut self,
            _request: crate::runtime::BuildImageRequest,
        ) -> Result<String, DockerRuntimeError> {
            unreachable!("backup tests do not build images")
        }

        fn ensure_network(&mut self, _network_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn ensure_volume(
            &mut self,
            request: CreateVolumeRequest,
        ) -> Result<(), DockerRuntimeError> {
            if self.volume_inspections.contains_key(&request.volume_name) {
                return Ok(());
            }
            let mountpoint = std::env::temp_dir().join(&request.volume_name);
            fs::create_dir_all(&mountpoint).unwrap();
            self.volume_inspections.insert(
                request.volume_name.clone(),
                VolumeInspection {
                    volume_name: request.volume_name,
                    mountpoint,
                    labels: request.labels,
                },
            );
            Ok(())
        }

        fn create_container(
            &mut self,
            request: CreateContainerRequest,
        ) -> Result<String, DockerRuntimeError> {
            let ip = self
                .next_container_ip
                .pop_front()
                .unwrap_or_else(|| "172.19.0.20".into());
            self.container_inspections.insert(
                request.container_name.clone(),
                ContainerInspection {
                    container_name: request.container_name.clone(),
                    running: false,
                    state_status: "created".into(),
                    exit_code: Some(0),
                    restart_count: 0,
                    started_at: Some("2026-05-23T00:00:00Z".into()),
                    finished_at: None,
                    oom_killed: false,
                    error: None,
                    image_ref: request.image_ref.clone(),
                    labels: request.labels.clone(),
                    network_ips: request
                        .network_name
                        .clone()
                        .into_iter()
                        .map(|network| (network, ip.clone()))
                        .collect(),
                    volume_mounts: request
                        .volume_mounts
                        .iter()
                        .map(|mount| crate::runtime::ContainerVolumeMount {
                            volume_name: mount.volume_name.clone(),
                            mount_path: mount.mount_path.clone(),
                        })
                        .collect(),
                    restart_policy: request.runtime_policy.restart_policy.clone(),
                    restart_max_retries: request.runtime_policy.max_retries,
                    cpu_limit: request.runtime_policy.cpu_limit.clone(),
                    memory_limit_mb: request.runtime_policy.memory_limit_mb,
                    exit_signal: None,
                    termination_reason: None,
                },
            );
            self.created_containers.push(request.clone());
            Ok(request.container_name)
        }

        fn start_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError> {
            self.started_containers.push(container_name.into());
            self.container_inspections
                .get_mut(container_name)
                .unwrap()
                .running = true;
            self.container_inspections
                .get_mut(container_name)
                .unwrap()
                .state_status = "running".into();
            Ok(())
        }

        fn inspect_container(
            &mut self,
            container_name: &str,
        ) -> Result<ContainerInspection, DockerRuntimeError> {
            self.container_inspections
                .get(container_name)
                .cloned()
                .ok_or_else(|| DockerRuntimeError::CommandFailed(container_name.into()))
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
            Ok(self.container_inspections.values().cloned().collect())
        }

        fn list_managed_images(&mut self) -> Result<Vec<ManagedImage>, DockerRuntimeError> {
            Ok(Vec::new())
        }

        fn list_managed_volumes(&mut self) -> Result<Vec<ManagedVolume>, DockerRuntimeError> {
            Ok(Vec::new())
        }

        fn inspect_volume(
            &mut self,
            volume_name: &str,
        ) -> Result<VolumeInspection, DockerRuntimeError> {
            self.inspect_volume_calls.push(volume_name.into());
            if self.fail_inspect_volume {
                return Err(DockerRuntimeError::CommandFailed(format!(
                    "host volume inspection denied for {volume_name}"
                )));
            }
            self.volume_inspections
                .get(volume_name)
                .cloned()
                .ok_or_else(|| DockerRuntimeError::CommandFailed(volume_name.into()))
        }

        fn run_volume_archive_helper(
            &mut self,
            request: VolumeArchiveHelperRequest,
        ) -> Result<VolumeArchiveHelperOutput, DockerRuntimeError> {
            self.helper_requests.push(request.clone());
            if let Some(result) = self.helper_results.pop_front() {
                return result;
            }

            let inspection = self
                .volume_inspections
                .get(&request.volume_name)
                .cloned()
                .ok_or_else(|| DockerRuntimeError::CommandFailed(request.volume_name.clone()))?;
            fs::create_dir_all(&request.archive_dir).unwrap();
            match request.mode {
                VolumeArchiveMode::Backup => {
                    let snapshot = encode_directory_snapshot(&inspection.mountpoint);
                    fs::write(request.archive_dir.join(&request.archive_file), snapshot).unwrap();
                }
                VolumeArchiveMode::Restore => {
                    let snapshot =
                        fs::read(request.archive_dir.join(&request.archive_file)).unwrap();
                    restore_directory_snapshot(&inspection.mountpoint, &snapshot);
                }
            }

            Ok(VolumeArchiveHelperOutput {
                stdout: String::new(),
                stderr: String::new(),
            })
        }

        fn exec_in_container(
            &mut self,
            request: ExecInContainerRequest,
        ) -> Result<ExecInContainerOutput, DockerRuntimeError> {
            self.exec_requests.push(request);
            if let Some(writes) = self.exec_file_writes.pop_front() {
                for (path, bytes) in writes {
                    if let Some(parent) = path.parent() {
                        fs::create_dir_all(parent).unwrap();
                    }
                    fs::write(path, bytes).unwrap();
                }
            }
            self.exec_results
                .pop_front()
                .unwrap_or(Ok(ExecInContainerOutput {
                    stdout: String::new(),
                    stderr: String::new(),
                    exit_code: 0,
                }))
        }

        fn stop_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError> {
            self.stopped_containers.push(container_name.into());
            if let Some(log) = &self.event_log {
                log.lock()
                    .unwrap()
                    .push(format!("docker:stop:{container_name}"));
            }
            Ok(())
        }

        fn remove_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn remove_image(&mut self, _image_ref: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn remove_volume(&mut self, _volume_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }
    }

    fn encode_directory_snapshot(root: &Path) -> Vec<u8> {
        fn walk(root: &Path, current: &Path, entries: &mut Vec<(String, Vec<u8>)>) {
            let mut children = fs::read_dir(current)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>();
            children.sort();
            for path in children {
                if path.is_dir() {
                    walk(root, &path, entries);
                    continue;
                }
                let relative = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                entries.push((relative, fs::read(&path).unwrap()));
            }
        }

        let mut entries = Vec::new();
        walk(root, root, &mut entries);
        serde_json::to_vec(&entries).unwrap()
    }

    fn restore_directory_snapshot(root: &Path, snapshot: &[u8]) {
        let entries: Vec<(String, Vec<u8>)> = serde_json::from_slice(snapshot).unwrap();
        fs::create_dir_all(root).unwrap();
        for (relative, contents) in entries {
            let path = root.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, contents).unwrap();
        }
    }

    struct SeededEnvironment {
        root: PathBuf,
        original_persistent_volume: String,
        original_mountpoint: PathBuf,
    }

    fn test_root(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "forge-backups-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn ensure_test_master_key() {
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            );
        }
    }

    fn register_project(root: &Path) {
        ProjectRegistryStore::new(root)
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some("api".into()),
                    repo_url: "https://github.com/example/api.git".into(),
                    default_branch: "main".into(),
                    base_domain: Some("api.example.com".into()),
                },
                None,
            )
            .unwrap();
    }

    fn seed_environment(root: &Path, docker: &mut TestDockerRuntime) -> SeededEnvironment {
        ensure_test_master_key();
        register_project(root);
        let env = EnvironmentPaths::new(root, "api", "production");
        env.ensure_exists().unwrap();
        let generation = GenerationAllocator::new(env.clone()).allocate().unwrap();
        let writer = SnapshotWriter::new(env.clone(), generation).unwrap();
        let original_volume = "forge-api-production-vol-redis".to_string();
        let original_mountpoint = root.join("volumes").join("redis-source");
        let ephemeral_mountpoint = root.join("volumes").join("cache-source");
        fs::create_dir_all(&original_mountpoint).unwrap();
        fs::create_dir_all(&ephemeral_mountpoint).unwrap();
        fs::write(original_mountpoint.join("counter.txt"), "7").unwrap();
        fs::write(ephemeral_mountpoint.join("scratch.txt"), "warm").unwrap();
        docker.volume_inspections.insert(
            original_volume.clone(),
            VolumeInspection {
                volume_name: original_volume.clone(),
                mountpoint: original_mountpoint.clone(),
                labels: BTreeMap::new(),
            },
        );
        docker.volume_inspections.insert(
            "forge-api-production-gen-1-vol-cache".into(),
            VolumeInspection {
                volume_name: "forge-api-production-gen-1-vol-cache".into(),
                mountpoint: ephemeral_mountpoint,
                labels: BTreeMap::new(),
            },
        );
        let persistent_mount = PersistedVolumeMount {
            volume_id: "redis".into(),
            docker_volume_name: original_volume.clone(),
            mount_path: "/data".into(),
            service_id: "api".into(),
            generation,
            retention: PersistedVolumeRetention::Persistent,
        };
        let ephemeral_mount = PersistedVolumeMount {
            volume_id: "cache".into(),
            docker_volume_name: "forge-api-production-gen-1-vol-cache".into(),
            mount_path: "/cache".into(),
            service_id: "api".into(),
            generation,
            retention: PersistedVolumeRetention::Ephemeral,
        };
        let service = PersistedServiceRuntimeInfo {
            service_id: "api".into(),
            container_name: "prod-api-gen-1".into(),
            image_ref: "forge/api:production-gen-1".into(),
            running: true,
            state: PersistedServiceState::Healthy,
            network_name: Some("forge-test".into()),
            probe_path: Some("/health".into()),
            activation: None,
            command: None,
            runtime_policy: crate::storage::PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..crate::storage::PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            depends_on: Vec::new(),
            required_for_promotion: true,
            externally_exposed: false,
            environment_variables: BTreeMap::new(),
            state_config: None,
            volume_mounts: vec![persistent_mount.clone(), ephemeral_mount],
            source_ref: Some("main".into()),
            repo_url: Some("https://github.com/example/api.git".into()),
            commit_sha: Some("abc123".into()),
            source_path: Some(root.join("checkout")),
        };
        let runtime = PersistedRuntimeInfo {
            container_name: "prod-api-gen-1".into(),
            running: true,
            network_name: Some("forge-test".into()),
            probe_path: Some("/health".into()),
            activation: None,
            runtime_policy: crate::storage::PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..crate::storage::PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            environment_variables: BTreeMap::new(),
            volume_mounts: vec![persistent_mount],
            source_ref: Some("main".into()),
            repo_url: Some("https://github.com/example/api.git".into()),
            commit_sha: Some("abc123".into()),
            source_path: Some(root.join("checkout")),
            services: BTreeMap::from([("api".into(), service)]),
            startup_order: vec!["api".into()],
        };
        let build = PersistedBuildInfo {
            deployment_id: "dep-1".into(),
            image_ref: "forge/api:production-gen-1".into(),
            services: BTreeMap::new(),
            source_ref: Some("main".into()),
            repo_url: Some("https://github.com/example/api.git".into()),
            commit_sha: Some("abc123".into()),
            source_path: Some(root.join("checkout")),
        };
        let resolved = PersistedResolvedRuntime {
            snapshot_version: 1,
            project_id: "api".into(),
            environment: "production".into(),
            generation,
            deployment_id: "dep-1".into(),
            source_environment: "production".into(),
            source_ref: Some("main".into()),
            commit_sha: Some("abc123".into()),
            domain: Some("api.example.com".into()),
            entries: BTreeMap::from([
                (
                    "DATABASE_URL".into(),
                    PersistedResolvedRuntimeEntry {
                        source: PersistedRuntimeEnvSource::ProjectEnvironmentSecret,
                        value: None,
                        secret_reference: None,
                        sealed_value: Some(seal_value("postgres://supersecret").unwrap()),
                        sensitive: true,
                    },
                ),
                (
                    "FORGE_PROJECT_ID".into(),
                    PersistedResolvedRuntimeEntry {
                        source: PersistedRuntimeEnvSource::ForgeGenerated,
                        value: Some("api".into()),
                        secret_reference: None,
                        sealed_value: None,
                        sensitive: false,
                    },
                ),
            ]),
        };
        let runtime_env_snapshot = derive_runtime_env_snapshot(&resolved);
        writer
            .write_artifact(
                "build.json",
                &format!("{}\n", serde_json::to_string_pretty(&build).unwrap()),
            )
            .unwrap();
        writer
            .write_artifact(
                "runtime.json",
                &format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()),
            )
            .unwrap();
        writer
            .write_artifact(
                "resolved_runtime.json",
                &format!("{}\n", serde_json::to_string_pretty(&resolved).unwrap()),
            )
            .unwrap();
        writer
            .write_artifact(
                "runtime_env_snapshot.json",
                &format!(
                    "{}\n",
                    serde_json::to_string_pretty(&runtime_env_snapshot).unwrap()
                ),
            )
            .unwrap();
        writer
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        PointerStore::new(env).swap_current(generation).unwrap();
        SeededEnvironment {
            root: root.to_path_buf(),
            original_persistent_volume: original_volume,
            original_mountpoint,
        }
    }

    fn seed_multiservice_environment(root: &Path, docker: &mut TestDockerRuntime) {
        ensure_test_master_key();
        register_project(root);
        let env = EnvironmentPaths::new(root, "api", "production");
        env.ensure_exists().unwrap();
        let generation = GenerationAllocator::new(env.clone()).allocate().unwrap();
        let writer = SnapshotWriter::new(env.clone(), generation).unwrap();
        let redis_volume = "forge-api-production-vol-redis-data".to_string();
        let redis_mountpoint = root.join("volumes").join("redis-source");
        fs::create_dir_all(&redis_mountpoint).unwrap();
        fs::write(redis_mountpoint.join("counter.txt"), "44").unwrap();
        docker.volume_inspections.insert(
            redis_volume.clone(),
            VolumeInspection {
                volume_name: redis_volume.clone(),
                mountpoint: redis_mountpoint,
                labels: BTreeMap::new(),
            },
        );

        let redis_service = PersistedServiceRuntimeInfo {
            service_id: "redis".into(),
            container_name: "prod-api-redis-gen-1".into(),
            image_ref: "redis:7".into(),
            running: true,
            state: PersistedServiceState::Healthy,
            network_name: Some("forge-test".into()),
            probe_path: None,
            activation: Some(PersistedActivationMode::Direct),
            command: None,
            runtime_policy: crate::storage::PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..crate::storage::PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            depends_on: Vec::new(),
            required_for_promotion: true,
            externally_exposed: false,
            environment_variables: BTreeMap::new(),
            state_config: Some(crate::storage::PersistedStateConfig {
                volume: "redis-data".into(),
                mount_path: "/data".into(),
                retention: PersistedVolumeRetention::Persistent,
                pre_backup_command: None,
            }),
            volume_mounts: vec![PersistedVolumeMount {
                volume_id: "redis-data".into(),
                docker_volume_name: redis_volume,
                mount_path: "/data".into(),
                service_id: "redis".into(),
                generation,
                retention: PersistedVolumeRetention::Persistent,
            }],
            source_ref: Some("main".into()),
            repo_url: Some("https://github.com/example/api.git".into()),
            commit_sha: Some("abc123".into()),
            source_path: Some(root.join("checkout")),
        };
        let api_service = PersistedServiceRuntimeInfo {
            service_id: "api".into(),
            container_name: "prod-api-api-gen-1".into(),
            image_ref: "forge/api:production-gen-1".into(),
            running: true,
            state: PersistedServiceState::Healthy,
            network_name: Some("forge-test".into()),
            probe_path: Some("/health".into()),
            activation: Some(PersistedActivationMode::Http {
                internal_port: 3000,
                route_subtree_id: Some("forge:api:production:api".into()),
                target_source: crate::storage::PersistedRouteTargetSource::ContainerIp,
            }),
            command: None,
            runtime_policy: crate::storage::PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..crate::storage::PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            depends_on: vec!["redis".into()],
            required_for_promotion: true,
            externally_exposed: true,
            environment_variables: BTreeMap::new(),
            state_config: None,
            volume_mounts: Vec::new(),
            source_ref: Some("main".into()),
            repo_url: Some("https://github.com/example/api.git".into()),
            commit_sha: Some("abc123".into()),
            source_path: Some(root.join("checkout")),
        };
        let runtime = PersistedRuntimeInfo {
            container_name: redis_service.container_name.clone(),
            running: true,
            network_name: redis_service.network_name.clone(),
            probe_path: redis_service.probe_path.clone(),
            activation: redis_service.activation.clone(),
            runtime_policy: redis_service.runtime_policy.clone(),
            runtime_usage: redis_service.runtime_usage.clone(),
            termination: redis_service.termination.clone(),
            environment_variables: redis_service.environment_variables.clone(),
            volume_mounts: redis_service.volume_mounts.clone(),
            source_ref: Some("main".into()),
            repo_url: Some("https://github.com/example/api.git".into()),
            commit_sha: Some("abc123".into()),
            source_path: Some(root.join("checkout")),
            services: BTreeMap::from([
                ("redis".into(), redis_service),
                ("api".into(), api_service),
            ]),
            startup_order: vec!["redis".into(), "api".into()],
        };
        let build = PersistedBuildInfo {
            deployment_id: "dep-1".into(),
            image_ref: "redis:7".into(),
            services: BTreeMap::new(),
            source_ref: Some("main".into()),
            repo_url: Some("https://github.com/example/api.git".into()),
            commit_sha: Some("abc123".into()),
            source_path: Some(root.join("checkout")),
        };
        let resolved = PersistedResolvedRuntime {
            snapshot_version: 1,
            project_id: "api".into(),
            environment: "production".into(),
            generation,
            deployment_id: "dep-1".into(),
            source_environment: "production".into(),
            source_ref: Some("main".into()),
            commit_sha: Some("abc123".into()),
            domain: Some("api.example.com".into()),
            entries: BTreeMap::new(),
        };
        writer
            .write_artifact(
                "build.json",
                &format!("{}\n", serde_json::to_string_pretty(&build).unwrap()),
            )
            .unwrap();
        writer
            .write_artifact(
                "runtime.json",
                &format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()),
            )
            .unwrap();
        writer
            .write_artifact(
                "resolved_runtime.json",
                &format!("{}\n", serde_json::to_string_pretty(&resolved).unwrap()),
            )
            .unwrap();
        writer
            .write_artifact(
                "runtime_env_snapshot.json",
                &format!(
                    "{}\n",
                    serde_json::to_string_pretty(&derive_runtime_env_snapshot(&resolved)).unwrap()
                ),
            )
            .unwrap();
        writer
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        PointerStore::new(env).swap_current(generation).unwrap();
    }

    fn multiservice_restore_event_log() -> Arc<Mutex<Vec<String>>> {
        Arc::new(Mutex::new(Vec::new()))
    }

    #[test]
    fn persistent_volume_backup_created() {
        let root = test_root("persistent-volume-backup-created");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);

        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();

        assert_eq!(backup.volumes.len(), 1);
        assert!(
            backup_dir(&root, "api", "production", &backup.backup_id)
                .join("volumes")
                .join(&backup.volumes[0].archive_file)
                .exists()
        );
    }

    #[test]
    fn backup_uses_helper_container_not_host_volume_path() {
        let root = test_root("backup-uses-helper-container");
        let mut docker = TestDockerRuntime {
            fail_inspect_volume: true,
            ..Default::default()
        };
        let seeded = seed_environment(&root, &mut docker);

        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();

        assert!(docker.inspect_volume_calls.is_empty());
        assert_eq!(docker.helper_requests.len(), 1);
        let request = &docker.helper_requests[0];
        assert_eq!(request.volume_name, seeded.original_persistent_volume);
        assert_eq!(request.mode, VolumeArchiveMode::Backup);
        assert_eq!(
            request.archive_dir,
            backup_dir(&root, "api", "production", &backup.backup_id).join("volumes")
        );
    }

    #[test]
    fn backup_only_includes_persistent_volumes() {
        let root = test_root("backup-only-includes-persistent");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);

        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();

        assert_eq!(backup.volumes.len(), 1);
        assert_eq!(backup.volumes[0].volume_id, "redis");
    }

    #[test]
    fn backup_excludes_ephemeral_volumes() {
        let root = test_root("backup-excludes-ephemeral");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);

        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();

        assert!(
            backup
                .volumes
                .iter()
                .all(|volume| volume.volume_id != "cache")
        );
    }

    #[test]
    fn backup_inspect_reports_archive_file_list() {
        let root = test_root("backup-inspect-reports-archive-file-list");
        let mut docker = TestDockerRuntime::default();
        let seeded = seed_environment(&root, &mut docker);
        let created = create_backup(&root, &mut docker, "api", "production").unwrap();

        let inspected = inspect_backup(&seeded.root, &created.backup_id).unwrap();

        assert_eq!(inspected.volumes.len(), 1);
        assert_eq!(
            inspected.volumes[0].docker_volume_name,
            seeded.original_persistent_volume
        );
        assert_eq!(inspected.volumes[0].mount_path, "/data");
        assert_eq!(inspected.volumes[0].archive_files.len(), 1);
        assert_eq!(inspected.volumes[0].archive_files[0].path, "counter.txt");
        assert_eq!(inspected.volumes[0].archive_files[0].size_bytes, 1);
    }

    #[test]
    fn backup_list_empty_when_backup_root_missing() {
        let root = test_root("backup-list-empty-when-root-missing");

        let listed = list_backups(&root, "api", "production").unwrap();

        assert!(listed.backups.is_empty());
        assert!(listed.warnings.is_empty());
    }

    #[test]
    fn backup_create_metadata_listed_by_backup_list() {
        let root = test_root("backup-create-metadata-listed-by-backup-list");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let created = create_backup(&root, &mut docker, "api", "production").unwrap();

        let listed = list_backups(&root, "api", "production").unwrap();

        assert_eq!(listed.backups.len(), 1);
        assert_eq!(listed.backups[0].backup_id, created.backup_id);
        assert!(listed.warnings.is_empty());
    }

    #[test]
    fn backup_list_empty_when_no_backups() {
        let root = test_root("backup-list-empty-when-no-backups");
        register_project(&root);

        let listed = list_backups(&root, "api", "production").unwrap();

        assert!(listed.backups.is_empty());
        assert!(listed.warnings.is_empty());
    }

    #[test]
    fn backup_list_skips_missing_metadata_backup_dir() {
        let root = test_root("backup-list-skips-missing-metadata");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let created = create_backup(&root, &mut docker, "api", "production").unwrap();
        let corrupt_dir = backup_dir(&root, "api", "production", "backup-corrupt");
        fs::create_dir_all(corrupt_dir.join("volumes")).unwrap();

        let listed = list_backups(&root, "api", "production").unwrap();

        assert_eq!(listed.backups.len(), 1);
        assert_eq!(listed.backups[0].backup_id, created.backup_id);
        assert!(listed.warnings.iter().any(
            |warning| warning == "skipped corrupt backup backup-corrupt: missing metadata.json"
        ));
    }

    #[test]
    fn backup_list_reports_corrupt_backup_warning() {
        let root = test_root("backup-list-reports-corrupt-warning");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let corrupt_dir = backup_dir(&root, "api", "production", "backup-corrupt");
        fs::create_dir_all(&corrupt_dir).unwrap();
        fs::write(corrupt_dir.join("metadata.json"), "{not-json").unwrap();

        let listed = list_backups(&root, "api", "production").unwrap();

        assert!(listed.backups.is_empty());
        assert!(
            listed
                .warnings
                .iter()
                .any(|warning| warning.starts_with("skipped corrupt backup backup-corrupt:"))
        );
        assert!(
            listed
                .warnings
                .iter()
                .any(|warning| warning.contains("cleanup partial backup directory:"))
        );
    }

    #[test]
    fn restore_creates_new_generation() {
        let root = test_root("restore-creates-new-generation");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        let restore = restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        assert_eq!(restore.restored_generation, 2);
        assert!(restore.restored_deployment_id.starts_with("restore-"));
        assert_eq!(
            PointerStore::new(EnvironmentPaths::new(&root, "api", "production"))
                .read_authoritative_pointer()
                .unwrap(),
            Some(2)
        );
    }

    #[test]
    fn restore_uses_helper_container_to_populate_new_volume() {
        let root = test_root("restore-uses-helper-container");
        let mut docker = TestDockerRuntime {
            fail_inspect_volume: true,
            ..Default::default()
        };
        seed_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();
        docker.helper_requests.clear();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        assert!(docker.inspect_volume_calls.is_empty());
        assert_eq!(docker.helper_requests.len(), 1);
        let request = &docker.helper_requests[0];
        assert!(request.volume_name.contains("restore-gen-2-vol-redis"));
        assert_eq!(request.mode, VolumeArchiveMode::Restore);
        assert_eq!(
            request.archive_dir,
            backup_dir(&root, "api", "production", &backup.backup_id).join("volumes")
        );
    }

    #[test]
    fn restore_extracts_archive_into_fresh_volume() {
        let root = test_root("restore-extracts-archive-into-fresh-volume");
        let mut docker = TestDockerRuntime::default();
        let seeded = seed_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let restored_volume = docker
            .volume_inspections
            .keys()
            .find(|name| name.contains("restore-gen-2-vol-redis"))
            .unwrap()
            .clone();
        let restored_mountpoint = docker.volume_inspections[&restored_volume]
            .mountpoint
            .clone();
        assert_eq!(
            fs::read_to_string(restored_mountpoint.join("counter.txt")).unwrap(),
            "7"
        );
        assert_ne!(restored_mountpoint, seeded.original_mountpoint);
        assert!(restored_volume.contains("restore-gen-2-vol-redis"));
    }

    #[test]
    fn restore_does_not_mutate_existing_persistent_volume() {
        let root = test_root("restore-does-not-mutate-existing-volume");
        let mut docker = TestDockerRuntime::default();
        let seeded = seed_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        assert_eq!(
            fs::read_to_string(seeded.original_mountpoint.join("counter.txt")).unwrap(),
            "7"
        );
    }

    #[test]
    fn restore_persists_runtime_env_snapshot() {
        let root = test_root("restore-persists-runtime-env-snapshot");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        let restore = restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();
        let env = EnvironmentPaths::new(&root, "api", "production");
        let snapshot = load_generation_runtime_env_snapshot(&env, restore.restored_generation)
            .unwrap()
            .expect("runtime env snapshot should exist");

        assert_eq!(snapshot.generation, restore.restored_generation);
        assert_eq!(snapshot.deployment_id, restore.restored_deployment_id);
        assert_eq!(
            snapshot
                .entries
                .get("FORGE_GENERATION")
                .and_then(|entry| entry.value.as_deref()),
            Some("2")
        );
    }

    #[test]
    fn diagnose_works_after_backup_restore() {
        let root = test_root("diagnose-works-after-backup-restore");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let diagnostics = load_environment_diagnostics(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "production",
        )
        .unwrap();

        assert_eq!(diagnostics.active_generation, Some(2));
        assert_eq!(
            diagnostics
                .runtime_env_snapshot
                .as_ref()
                .map(|snapshot| snapshot.generation),
            Some(2)
        );
    }

    #[test]
    fn env_works_after_backup_restore() {
        let root = test_root("env-works-after-backup-restore");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        let restore = restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let report = load_project_environment_env_report(&root, "api", "production").unwrap();

        assert_eq!(report.generation, restore.restored_generation);
        assert_eq!(report.deployment_id, restore.restored_deployment_id);
        assert!(
            report
                .values
                .iter()
                .any(|value| value.key == "FORGE_GENERATION")
        );
    }

    #[test]
    fn restored_multiservice_status_uses_exposed_service_as_primary() {
        let root = test_root("restored-multiservice-status-uses-exposed-service-as-primary");
        let mut docker = TestDockerRuntime::default();
        seed_multiservice_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let runtime =
            load_generation_runtime_info(&EnvironmentPaths::new(&root, "api", "production"), 2)
                .unwrap()
                .unwrap();
        assert_eq!(runtime.container_name, "prod-api-api-gen-2");
        assert_eq!(runtime.probe_path.as_deref(), Some("/health"));
        assert!(matches!(
            runtime.activation,
            Some(PersistedActivationMode::Http {
                internal_port: 3000,
                ..
            })
        ));
    }

    #[test]
    fn internal_stateful_service_not_selected_as_top_level_route_target() {
        let root = test_root("internal-stateful-service-not-selected-as-top-level-route-target");
        let mut docker = TestDockerRuntime::default();
        seed_multiservice_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let route = routing.inspect_route("forge:api:production:api").unwrap();
        assert_eq!(route.active_target, "172.19.0.20:3000");
    }

    #[test]
    fn diagnose_after_restore_reports_api_route_target() {
        let root = test_root("diagnose-after-restore-reports-api-route-target");
        let mut docker = TestDockerRuntime::default();
        seed_multiservice_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let diagnostics = load_environment_diagnostics(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "production",
        )
        .unwrap();
        assert_eq!(
            diagnostics.container.container_name.as_deref(),
            Some("prod-api-api-gen-2")
        );
        assert_eq!(
            diagnostics.route.current_target.as_deref(),
            Some("172.19.0.20:3000")
        );
        assert_eq!(
            diagnostics
                .probe_target
                .as_ref()
                .and_then(|value| value.port),
            Some(3000)
        );
    }

    #[test]
    fn backup_runs_pre_backup_hook_before_archiving() {
        let root = test_root("backup-runs-pre-backup-hook-before-archiving");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let mut runtime = load_generation_runtime_info(&env, 1).unwrap().unwrap();
        runtime.services.get_mut("api").unwrap().state_config =
            Some(crate::storage::PersistedStateConfig {
                volume: "redis".into(),
                mount_path: "/data".into(),
                retention: PersistedVolumeRetention::Persistent,
                pre_backup_command: Some("echo saved".into()),
            });
        atomic_write(
            env.generation_dir(1).join("runtime.json"),
            format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()).as_bytes(),
        )
        .unwrap();
        docker.exec_results.push_back(Ok(ExecInContainerOutput {
            stdout: "saved".into(),
            stderr: String::new(),
            exit_code: 0,
        }));

        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();

        assert_eq!(docker.exec_requests.len(), 1);
        assert_eq!(docker.exec_requests[0].container_name, "prod-api-gen-1");
        assert_eq!(
            docker.exec_requests[0].command,
            vec![
                "sh".to_string(),
                "-lc".to_string(),
                "echo saved".to_string()
            ]
        );
        assert_eq!(docker.helper_requests.len(), 1);
        let metadata = find_backup_metadata(&root, &backup.backup_id).unwrap();
        assert_eq!(metadata.hooks.len(), 1);
        assert_eq!(metadata.hooks[0].stdout, "saved");
    }

    #[test]
    fn backup_fails_if_pre_backup_hook_fails() {
        let root = test_root("backup-fails-if-pre-backup-hook-fails");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let mut runtime = load_generation_runtime_info(&env, 1).unwrap().unwrap();
        runtime.services.get_mut("api").unwrap().state_config =
            Some(crate::storage::PersistedStateConfig {
                volume: "redis".into(),
                mount_path: "/data".into(),
                retention: PersistedVolumeRetention::Persistent,
                pre_backup_command: Some("exit 7".into()),
            });
        atomic_write(
            env.generation_dir(1).join("runtime.json"),
            format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()).as_bytes(),
        )
        .unwrap();
        docker.exec_results.push_back(Ok(ExecInContainerOutput {
            stdout: String::new(),
            stderr: "boom".into(),
            exit_code: 7,
        }));

        let err = create_backup(&root, &mut docker, "api", "production").unwrap_err();

        assert!(err.to_string().contains("pre_backup_command failed"));
        assert!(docker.helper_requests.is_empty());
    }

    #[test]
    fn backup_inspect_reports_hook_execution() {
        let root = test_root("backup-inspect-reports-hook-execution");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let mut runtime = load_generation_runtime_info(&env, 1).unwrap().unwrap();
        runtime.services.get_mut("api").unwrap().state_config =
            Some(crate::storage::PersistedStateConfig {
                volume: "redis".into(),
                mount_path: "/data".into(),
                retention: PersistedVolumeRetention::Persistent,
                pre_backup_command: Some("printf ok && printf warn >&2".into()),
            });
        atomic_write(
            env.generation_dir(1).join("runtime.json"),
            format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()).as_bytes(),
        )
        .unwrap();
        docker.exec_results.push_back(Ok(ExecInContainerOutput {
            stdout: "ok".into(),
            stderr: "warn".into(),
            exit_code: 0,
        }));

        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let inspected = inspect_backup(&root, &backup.backup_id).unwrap();

        assert_eq!(inspected.hooks.len(), 1);
        assert_eq!(inspected.hooks[0].container_name, "prod-api-gen-1");
        assert_eq!(
            inspected.hooks[0].pre_backup_command,
            "printf ok && printf warn >&2"
        );
        assert_eq!(inspected.hooks[0].stdout, "ok");
        assert_eq!(inspected.hooks[0].stderr, "warn");
        assert_eq!(inspected.hooks[0].exit_code, 0);
        assert!(inspected.hooks[0].started_at_unix.is_some());
        assert!(inspected.hooks[0].completed_at_unix.is_some());
    }

    #[test]
    fn redis_backup_hook_persists_dump_rdb() {
        let root = test_root("redis-backup-hook-persists-dump-rdb");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let mut runtime = load_generation_runtime_info(&env, 1).unwrap().unwrap();
        runtime.services.get_mut("api").unwrap().state_config =
            Some(crate::storage::PersistedStateConfig {
                volume: "redis".into(),
                mount_path: "/data".into(),
                retention: PersistedVolumeRetention::Persistent,
                pre_backup_command: Some("redis-cli SAVE".into()),
            });
        atomic_write(
            env.generation_dir(1).join("runtime.json"),
            format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()).as_bytes(),
        )
        .unwrap();
        docker.exec_results.push_back(Ok(ExecInContainerOutput {
            stdout: "OK".into(),
            stderr: String::new(),
            exit_code: 0,
        }));
        docker.exec_file_writes.push_back(vec![
            (
                root.join("volumes")
                    .join("redis-source")
                    .join("counter.txt"),
                b"1711".to_vec(),
            ),
            (
                root.join("volumes").join("redis-source").join("dump.rdb"),
                b"1711".to_vec(),
            ),
        ]);
        fs::write(
            root.join("volumes")
                .join("redis-source")
                .join("counter.txt"),
            "339",
        )
        .unwrap();

        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let inspected = inspect_backup(&root, &backup.backup_id).unwrap();
        assert!(
            inspected.volumes[0]
                .archive_files
                .iter()
                .any(|file| file.path == "dump.rdb")
        );
        assert_eq!(inspected.hooks[0].pre_backup_command, "redis-cli SAVE");
        assert_eq!(inspected.hooks[0].container_name, "prod-api-gen-1");
    }

    #[test]
    fn redis_restore_recovers_counter_at_backup_time() {
        let root = test_root("redis-restore-recovers-counter-at-backup-time");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let mut runtime = load_generation_runtime_info(&env, 1).unwrap().unwrap();
        runtime.services.get_mut("api").unwrap().state_config =
            Some(crate::storage::PersistedStateConfig {
                volume: "redis".into(),
                mount_path: "/data".into(),
                retention: PersistedVolumeRetention::Persistent,
                pre_backup_command: Some("redis-cli SAVE".into()),
            });
        atomic_write(
            env.generation_dir(1).join("runtime.json"),
            format!("{}\n", serde_json::to_string_pretty(&runtime).unwrap()).as_bytes(),
        )
        .unwrap();
        docker.exec_results.push_back(Ok(ExecInContainerOutput {
            stdout: "OK".into(),
            stderr: String::new(),
            exit_code: 0,
        }));
        docker.exec_file_writes.push_back(vec![
            (
                root.join("volumes")
                    .join("redis-source")
                    .join("counter.txt"),
                b"1711".to_vec(),
            ),
            (
                root.join("volumes").join("redis-source").join("dump.rdb"),
                b"1711".to_vec(),
            ),
        ]);
        fs::write(
            root.join("volumes")
                .join("redis-source")
                .join("counter.txt"),
            "339",
        )
        .unwrap();

        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        fs::write(
            root.join("volumes")
                .join("redis-source")
                .join("counter.txt"),
            "9999",
        )
        .unwrap();
        let mut routing = TestRoutingRuntime::default();
        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let restored_volume = docker
            .volume_inspections
            .keys()
            .find(|name| name.contains("restore-gen-2-vol-redis"))
            .unwrap()
            .clone();
        let restored_mountpoint = docker.volume_inspections[&restored_volume]
            .mountpoint
            .clone();
        assert_eq!(
            fs::read_to_string(restored_mountpoint.join("dump.rdb")).unwrap(),
            "1711"
        );
        assert_ne!(
            fs::read_to_string(restored_mountpoint.join("counter.txt")).unwrap(),
            "9999"
        );
    }

    #[test]
    fn restore_lineage_visible_in_diagnose() {
        let root = test_root("restore-lineage-visible-in-diagnose");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let diagnostics = load_environment_diagnostics(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "production",
        )
        .unwrap();

        let lineage = diagnostics
            .active_restore
            .expect("restore lineage should exist");
        assert_eq!(lineage.backup_id, backup.backup_id);
        assert_eq!(lineage.restored_generation, 2);
        assert_eq!(lineage.source_generation, Some(1));
        assert_eq!(lineage.source_deployment_id.as_deref(), Some("dep-1"));
        assert_eq!(lineage.hook_succeeded, None);
        assert_eq!(lineage.restored_volumes.len(), 1);
        assert_eq!(lineage.restored_volumes[0].volume_id, "redis");
        assert_eq!(
            lineage.restored_volumes[0]
                .restored_docker_volume_name
                .as_deref(),
            Some("forge-api-production-restore-gen-2-vol-redis")
        );
        assert!(!lineage.restored_volumes[0].archive_sha256.is_empty());
    }

    #[test]
    fn restored_service_reads_restored_volume_state() {
        let root = test_root("restored-service-reads-restored-volume-state");
        let mut docker = TestDockerRuntime::default();
        seed_multiservice_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        fs::write(
            root.join("volumes")
                .join("redis-source")
                .join("counter.txt"),
            "99",
        )
        .unwrap();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let runtime =
            load_generation_runtime_info(&EnvironmentPaths::new(&root, "api", "production"), 2)
                .unwrap()
                .unwrap();
        let redis_runtime = runtime.services.get("redis").unwrap();
        let redis_mount = redis_runtime.volume_mounts.first().unwrap();
        let restored_mountpoint = docker.volume_inspections[&redis_mount.docker_volume_name]
            .mountpoint
            .clone();
        assert_eq!(
            fs::read_to_string(restored_mountpoint.join("counter.txt")).unwrap(),
            "44"
        );
    }

    #[test]
    fn restore_removes_old_internal_service_alias_before_validation() {
        let root = test_root("restore-removes-old-internal-service-alias-before-validation");
        let event_log = multiservice_restore_event_log();
        let mut docker = TestDockerRuntime {
            event_log: Some(event_log.clone()),
            ..Default::default()
        };
        seed_multiservice_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime {
            event_log: Some(event_log.clone()),
            ..Default::default()
        };

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let events = event_log.lock().unwrap().clone();
        let stop_index = events
            .iter()
            .position(|event| event == "docker:stop:prod-api-redis-gen-1")
            .expect("source redis should be retired");
        let inspect_index = events
            .iter()
            .position(|event| event == "route:inspect:forge:api:production:api")
            .expect("restored route should be inspected");
        assert!(stop_index < inspect_index);
    }

    #[test]
    fn restored_api_reads_restored_redis_not_old_redis() {
        let root = test_root("restored-api-reads-restored-redis-not-old-redis");
        let mut docker = TestDockerRuntime::default();
        seed_multiservice_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        fs::write(
            root.join("volumes")
                .join("redis-source")
                .join("counter.txt"),
            "99",
        )
        .unwrap();
        let mut routing = TestRoutingRuntime::default();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let runtime =
            load_generation_runtime_info(&EnvironmentPaths::new(&root, "api", "production"), 2)
                .unwrap()
                .unwrap();
        let restored_mount = runtime
            .services
            .get("redis")
            .unwrap()
            .volume_mounts
            .first()
            .unwrap();
        let restored_mountpoint = docker.volume_inspections[&restored_mount.docker_volume_name]
            .mountpoint
            .clone();
        assert_eq!(
            fs::read_to_string(restored_mountpoint.join("counter.txt")).unwrap(),
            "44"
        );
        assert_eq!(
            fs::read_to_string(
                root.join("volumes")
                    .join("redis-source")
                    .join("counter.txt")
            )
            .unwrap(),
            "99"
        );
    }

    #[test]
    fn no_duplicate_service_aliases_after_restore() {
        let root = test_root("no-duplicate-service-aliases-after-restore");
        let mut docker = TestDockerRuntime::default();
        seed_multiservice_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let redis_container = docker
            .created_containers
            .iter()
            .find(|request| request.container_name == "prod-api-redis-gen-2")
            .expect("restored redis container should be created");
        assert_eq!(
            redis_container.network_aliases,
            vec!["redis".to_string(), "prod-api-redis-gen-2".to_string()]
        );
        assert_eq!(docker.stopped_containers, vec!["prod-api-redis-gen-1"]);
    }

    #[test]
    fn caddy_route_still_targets_restored_api_after_internal_retirement() {
        let root = test_root("caddy-route-still-targets-restored-api-after-internal-retirement");
        let mut docker = TestDockerRuntime::default();
        seed_multiservice_environment(&root, &mut docker);
        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let mut routing = TestRoutingRuntime::default();

        restore_backup(&root, &mut docker, &mut routing, &backup.backup_id).unwrap();

        let route = routing.inspect_route("forge:api:production:api").unwrap();
        assert_eq!(route.active_target, "172.19.0.20:3000");
        assert_eq!(docker.stopped_containers, vec!["prod-api-redis-gen-1"]);
    }

    #[test]
    fn backup_helper_failure_records_stderr() {
        let root = test_root("backup-helper-failure-records-stderr");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        docker.helper_results.push_back(Err(DockerRuntimeError::CommandFailed(
            "helper container failed for volume forge-api-production-vol-redis; stderr: tar: permission denied".into(),
        )));

        let err = create_backup(&root, &mut docker, "api", "production").unwrap_err();

        assert!(matches!(
            err,
            BackupError::Docker(DockerRuntimeError::CommandFailed(_))
        ));
        assert!(err.to_string().contains("stderr: tar: permission denied"));
    }

    #[test]
    fn backup_create_rejects_legacy_generation_before_creating_backup_dir() {
        let root = test_root("backup-create-rejects-legacy-generation");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let env = EnvironmentPaths::new(&root, "api", "production");
        fs::remove_file(env.generation_dir(1).join("runtime_env_snapshot.json")).unwrap();

        let err = create_backup(&root, &mut docker, "api", "production").unwrap_err();

        assert!(matches!(err, BackupError::Invalid(_)));
        let backup_root = backups_environment_root(&root, "api", "production");
        assert!(!backup_root.exists() || fs::read_dir(&backup_root).unwrap().next().is_none());
    }

    #[test]
    fn failed_backup_create_does_not_leave_metadata_less_dir() {
        let root = test_root("failed-backup-create-no-metadata-less-dir");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        docker
            .helper_results
            .push_back(Err(DockerRuntimeError::CommandFailed(
                "helper container failed".into(),
            )));

        let _ = create_backup(&root, &mut docker, "api", "production").unwrap_err();

        let backup_root = backups_environment_root(&root, "api", "production");
        assert!(!backup_root.exists() || fs::read_dir(&backup_root).unwrap().next().is_none());
    }

    #[test]
    fn backup_create_error_mentions_redeploy_required_for_missing_snapshot() {
        let root = test_root("backup-create-redeploy-required");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);
        let env = EnvironmentPaths::new(&root, "api", "production");
        fs::remove_file(env.generation_dir(1).join("runtime_env_snapshot.json")).unwrap();

        let err = create_backup(&root, &mut docker, "api", "production").unwrap_err();
        let message = err.to_string();

        assert!(
            message
                .contains("active generation lacks runtime env snapshot; redeploy before backup")
        );
        assert!(message.contains("project=api"));
        assert!(message.contains("environment=production"));
        assert!(message.contains("generation=1"));
    }

    #[test]
    fn backup_create_does_not_require_host_volume_permissions() {
        let root = test_root("backup-create-does-not-require-host-volume-permissions");
        let mut docker = TestDockerRuntime {
            fail_inspect_volume: true,
            ..Default::default()
        };
        seed_environment(&root, &mut docker);

        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();

        assert_eq!(backup.volumes.len(), 1);
        assert!(docker.inspect_volume_calls.is_empty());
        assert_eq!(docker.helper_requests.len(), 1);
    }

    #[test]
    fn backup_restore_rejects_missing_backup() {
        let root = test_root("missing-backup");
        let mut docker = TestDockerRuntime::default();
        let mut routing = TestRoutingRuntime::default();

        let err = restore_backup(&root, &mut docker, &mut routing, "backup-missing").unwrap_err();

        assert!(matches!(err, BackupError::NotFound(_)));
    }

    #[test]
    fn backup_metadata_redacts_secrets_plaintext() {
        let root = test_root("backup-redacts-secrets");
        let mut docker = TestDockerRuntime::default();
        seed_environment(&root, &mut docker);

        let backup = create_backup(&root, &mut docker, "api", "production").unwrap();
        let metadata = fs::read_to_string(
            backup_dir(&root, "api", "production", &backup.backup_id).join("metadata.json"),
        )
        .unwrap();

        assert!(!metadata.contains("postgres://supersecret"));
        assert!(metadata.contains("\"sealed_value\""));
    }
}
