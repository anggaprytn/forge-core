use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crate::process::run_command_with_timeout;
use sha2::{Digest, Sha256};

use crate::api::{
    BackupListResponse, BackupRecord, BackupRestoreResponse, BackupVolumeRecord, RestoreRecord,
};
use crate::events::EventRecord;
use crate::projects::ProjectRegistryStore;
use crate::queue::DeploymentRecord;
use crate::route_truth::resolve_route_target;
use crate::runtime::{
    ContainerInspection, CreateContainerRequest, CreateVolumeRequest, DockerRuntime,
    DockerRuntimeError, RouteInspection, RouteUpdateRequest, RoutingRuntime, VolumeMountRequest,
};
use crate::runtime_env::{RuntimeEnvMetadata, generated_forge_vars, restore_runtime_env};
use crate::status::derive_environment_domain;
use crate::storage::{
    DeploymentLifecycleState, DiagnosticsStore, EnvironmentPaths, EventStore, GenerationAllocator,
    GenerationHistoryRecord, LifecycleStore, PersistedActivationMode, PersistedBackupMetadata,
    PersistedBackupRestoreRecord, PersistedBackupVolumeRecord, PersistedBuildInfo,
    PersistedDeploymentLifecycle, PersistedPromotionSummary, PersistedResolvedRuntime,
    PersistedResolvedRuntimeEntry, PersistedRuntimeInfo, PersistedServiceRuntimeInfo,
    PersistedServiceState, PersistedSnapshotMetadata, PersistedValidationSummary,
    PersistedVolumeMount, PersistedVolumeRetention, PointerStore, RetentionStore,
    RuntimeHealthState, RuntimeState, RuntimeStateStore, SnapshotState, SnapshotWriter,
    StorageError, atomic_write, current_unix_timestamp, load_generation_build_info,
    load_generation_resolved_runtime, load_generation_runtime_info,
    load_generation_snapshot_metadata,
};

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
    let mut manifest = Vec::new();
    for mount in volume_mounts {
        let inspection = docker.inspect_volume(&mount.docker_volume_name)?;
        let archive_file = format!("{}-{}.tar.gz", mount.service_id, mount.volume_id);
        let archive_path = backup_dir.join("volumes").join(&archive_file);
        archive_directory(&inspection.mountpoint, &archive_path)?;
        let bytes = fs::read(&archive_path).map_err(|err| BackupError::Command(err.to_string()))?;
        manifest.push(PersistedBackupVolumeRecord {
            volume_id: mount.volume_id,
            docker_volume_name: mount.docker_volume_name,
            service_id: mount.service_id,
            mount_path: mount.mount_path,
            archive_file,
            archive_size_bytes: bytes.len() as u64,
            archive_sha256: hex::encode(Sha256::digest(bytes)),
        });
    }

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
        resolved_runtime: resolved,
        services: services.keys().cloned().collect(),
        volumes: manifest.clone(),
        restores: Vec::new(),
        warnings: vec![
            "backups are crash-consistent only".into(),
            "Forge does not coordinate database quiescing".into(),
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
    if root.exists() {
        for entry in fs::read_dir(root).map_err(|err| BackupError::Command(err.to_string()))? {
            let entry = entry.map_err(|err| BackupError::Command(err.to_string()))?;
            if !entry
                .file_type()
                .map_err(|err| BackupError::Command(err.to_string()))?
                .is_dir()
            {
                continue;
            }
            backups.push(api_backup_record(read_backup_metadata(&entry.path())?));
        }
    }
    backups.sort_by(|left, right| right.created_at_unix.cmp(&left.created_at_unix));
    Ok(BackupListResponse {
        project_id: project_id.into(),
        environment: environment.into(),
        backups,
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

    let source_services = runtime_services(&metadata.runtime_info);
    let service_count = source_services.len();
    let mut restored_services = BTreeMap::new();
    for (service_id, service) in &source_services {
        let container_name =
            generation_service_container_name(&record, generation, service_id, service_count);
        let volume_mounts = restore_volume_mounts(
            storage_root,
            docker,
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
                vec![service_id.clone()]
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
        })?;
        docker.start_container(&container_name)?;
        let inspection = docker.inspect_container(&container_name)?;
        validate_inspection(&inspection, &container_name)?;
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
    record: &GenerationHistoryRecord,
) -> Option<crate::api::RestoreLineage> {
    Some(crate::api::RestoreLineage {
        backup_id: record.restored_from_backup_id.clone()?,
        source_generation: record.restored_from_generation?,
        source_deployment_id: record.restored_from_deployment_id.clone(),
        restored_at_unix: record.restored_at_unix?,
    })
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

fn read_backup_metadata(path: &Path) -> Result<PersistedBackupMetadata, BackupError> {
    let raw = fs::read_to_string(path.join("metadata.json"))
        .map_err(|err| BackupError::Command(err.to_string()))?;
    serde_json::from_str(&raw).map_err(|err| BackupError::Invalid(err.to_string()))
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
    for project in fs::read_dir(&root).map_err(|err| BackupError::Command(err.to_string()))? {
        let project = project.map_err(|err| BackupError::Command(err.to_string()))?;
        for environment in
            fs::read_dir(project.path()).map_err(|err| BackupError::Command(err.to_string()))?
        {
            let environment = environment.map_err(|err| BackupError::Command(err.to_string()))?;
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

fn archive_directory(source: &Path, archive_path: &Path) -> Result<(), BackupError> {
    if let Some(parent) = archive_path.parent() {
        fs::create_dir_all(parent).map_err(|err| BackupError::Command(err.to_string()))?;
    }
    let output = run_command_with_timeout(
        Command::new("tar")
            .arg("-czf")
            .arg(archive_path)
            .arg("-C")
            .arg(source)
            .arg("."),
        Duration::from_secs(60),
    )
    .map_err(|err| BackupError::Command(err.to_string()))?;
    if !output.status.success() {
        return Err(BackupError::Command(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ));
    }
    Ok(())
}

fn extract_archive(archive_path: &Path, target: &Path) -> Result<(), BackupError> {
    fs::create_dir_all(target).map_err(|err| BackupError::Command(err.to_string()))?;
    let output = run_command_with_timeout(
        Command::new("tar")
            .arg("-xzf")
            .arg(archive_path)
            .arg("-C")
            .arg(target),
        Duration::from_secs(60),
    )
    .map_err(|err| BackupError::Command(err.to_string()))?;
    if !output.status.success() {
        return Err(BackupError::Command(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ));
    }
    Ok(())
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

fn restore_volume_mounts<D: DockerRuntime>(
    storage_root: &Path,
    docker: &mut D,
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
        let inspection = docker.inspect_volume(&volume_name)?;
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
        extract_archive(
            &backup_root.join("volumes").join(&backup.archive_file),
            &inspection.mountpoint,
        )?;
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

fn validate_inspection(
    inspection: &ContainerInspection,
    expected_container_name: &str,
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
    if inspection.restart_policy != "no" {
        return Err(BackupError::Invalid(
            "restart policy must remain disabled".into(),
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
    runtime
        .startup_order
        .iter()
        .find(|service_id| restored_services.contains_key(*service_id))
        .cloned()
        .unwrap_or_else(|| {
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
    use crate::runtime::{ManagedImage, ManagedVolume, RouteUpdateRequest, VolumeInspection};
    use crate::secrets::seal_value;
    use crate::storage::{
        PersistedResolvedRuntime, PersistedResolvedRuntimeEntry, PersistedRuntimeEnvSource,
        PersistedServiceState, PointerStore, SnapshotWriter,
    };

    #[derive(Default)]
    struct TestRoutingRuntime {
        routes: BTreeMap<String, RouteInspection>,
    }

    impl RoutingRuntime for TestRoutingRuntime {
        fn update_route(
            &mut self,
            request: RouteUpdateRequest,
        ) -> Result<(), crate::runtime::RoutingRuntimeError> {
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
        next_container_ip: VecDeque<String>,
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
                    restart_policy: "no".into(),
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
            self.volume_inspections
                .get(volume_name)
                .cloned()
                .ok_or_else(|| DockerRuntimeError::CommandFailed(volume_name.into()))
        }

        fn stop_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
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
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        PointerStore::new(env).swap_current(generation).unwrap();
        SeededEnvironment {
            root: root.to_path_buf(),
            original_persistent_volume: original_volume,
            original_mountpoint,
        }
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
    fn backup_inspect_reports_volume_manifest() {
        let root = test_root("backup-inspect-manifest");
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
    fn restore_preserves_stateful_data() {
        let root = test_root("restore-preserves-stateful-data");
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
