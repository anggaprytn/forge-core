use std::fmt::{Display, Formatter};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::api::ErrorResponse;
use crate::projects::ProjectRegistryStore;
use crate::queue::{PersistentQueue, QueueError};
use crate::runtime::{
    ContainerInspection, DockerRuntime, DockerRuntimeError, RouteInspection, RoutingRuntime,
    RoutingRuntimeError,
};
use crate::storage::{
    EnvironmentPaths, PersistedActivationMode, PersistedRouteTargetSource, PersistedRuntimeInfo,
    PointerStore, RuntimeStateStore, StorageError, load_generation_build_info,
    load_generation_runtime_info, load_generation_snapshot_metadata,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectEnvironmentStatus {
    pub project_id: String,
    pub environment: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_generation: Option<u64>,
    pub domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    pub container_running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_ip: Option<String>,
    pub route_active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_deployment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_at_unix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_started_at: Option<String>,
}

#[derive(Debug)]
pub enum ProjectStatusError {
    Storage(StorageError),
    Queue(QueueError),
    Routing(RoutingRuntimeError),
    Docker(DockerRuntimeError),
    ProjectLookup(String),
    ProjectNotFound,
    InvalidEnvironment,
}

impl Display for ProjectStatusError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "{err}"),
            Self::Queue(err) => write!(f, "{err}"),
            Self::Routing(err) => write!(f, "{err}"),
            Self::Docker(err) => write!(f, "{err}"),
            Self::ProjectLookup(message) => write!(f, "{message}"),
            Self::ProjectNotFound => write!(f, "project not found"),
            Self::InvalidEnvironment => {
                write!(
                    f,
                    "environment must be one of development, staging, production"
                )
            }
        }
    }
}

impl std::error::Error for ProjectStatusError {}

impl From<StorageError> for ProjectStatusError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<std::io::Error> for ProjectStatusError {
    fn from(value: std::io::Error) -> Self {
        Self::Storage(StorageError::Io(value))
    }
}

impl From<QueueError> for ProjectStatusError {
    fn from(value: QueueError) -> Self {
        Self::Queue(value)
    }
}

impl From<RoutingRuntimeError> for ProjectStatusError {
    fn from(value: RoutingRuntimeError) -> Self {
        Self::Routing(value)
    }
}

impl From<DockerRuntimeError> for ProjectStatusError {
    fn from(value: DockerRuntimeError) -> Self {
        Self::Docker(value)
    }
}

pub fn derive_environment_domain(base_domain: &str, environment: &str) -> String {
    match environment {
        "production" => base_domain.to_string(),
        "staging" => format!("staging-{base_domain}"),
        "development" => format!("development-{base_domain}"),
        other => format!("{other}-{base_domain}"),
    }
}

pub fn route_subtree_id(project_id: &str, environment: &str) -> String {
    format!("forge:{project_id}:{environment}")
}

pub fn project_status_error_response(
    err: ProjectStatusError,
) -> (axum::http::StatusCode, ErrorResponse) {
    match err {
        ProjectStatusError::ProjectNotFound => (
            axum::http::StatusCode::NOT_FOUND,
            ErrorResponse {
                code: "project_not_found".into(),
                message: "project not found".into(),
            },
        ),
        ProjectStatusError::InvalidEnvironment => (
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse {
                code: "invalid_environment".into(),
                message: "environment must be one of development, staging, production".into(),
            },
        ),
        other => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorResponse {
                code: "project_status_unavailable".into(),
                message: other.to_string(),
            },
        ),
    }
}

pub fn load_project_environment_status<D, R>(
    storage_root: &Path,
    queue: Option<&PersistentQueue>,
    docker: &mut D,
    routing: &mut R,
    project_id: &str,
    environment: &str,
) -> Result<ProjectEnvironmentStatus, ProjectStatusError>
where
    D: DockerRuntime,
    R: RoutingRuntime,
{
    if !matches!(environment, "development" | "staging" | "production") {
        return Err(ProjectStatusError::InvalidEnvironment);
    }

    let project = ProjectRegistryStore::new(storage_root)
        .get(project_id)
        .map_err(|err| {
            ProjectStatusError::ProjectLookup(format!(
                "project lookup failed for {project_id}: {err}"
            ))
        })?
        .ok_or(ProjectStatusError::ProjectNotFound)?;
    let domain = derive_environment_domain(&project.base_domain, environment);

    let env = EnvironmentPaths::new(storage_root, project_id, environment);
    env.ensure_exists()?;
    let current_generation = PointerStore::new(env.clone()).read_pointer("current")?;
    let runtime_state = RuntimeStateStore::new(env.clone()).load()?;
    let active_generation = current_generation.or(runtime_state.active_generation);
    let latest_generation = latest_generation(&env)?;

    let promoted_snapshot = current_generation
        .map(|generation| load_generation_snapshot_metadata(&env, generation))
        .transpose()?
        .flatten();
    let promoted_runtime = active_generation
        .map(|generation| load_generation_runtime_info(&env, generation))
        .transpose()?
        .flatten();
    let promoted_build = active_generation
        .map(|generation| load_generation_build_info(&env, generation))
        .transpose()?
        .flatten();

    let latest_snapshot = latest_generation
        .map(|generation| load_generation_snapshot_metadata(&env, generation))
        .transpose()?
        .flatten();
    let latest_build = latest_generation
        .map(|generation| load_generation_build_info(&env, generation))
        .transpose()?
        .flatten();

    let deploying = queue
        .map(|queue| queue.load_state())
        .transpose()?
        .is_some_and(|state| {
            state.active.as_ref().is_some_and(|record| {
                record.project_id == project_id && record.environment == environment
            }) || state
                .queued
                .iter()
                .any(|record| record.project_id == project_id && record.environment == environment)
        });

    let container_name = promoted_runtime
        .as_ref()
        .map(|runtime| runtime.container_name.clone());
    let container_inspection = inspect_promoted_container(docker, promoted_runtime.as_ref());
    let container_running = container_inspection
        .as_ref()
        .is_some_and(|inspection| inspection.running);
    let container_status = container_inspection
        .as_ref()
        .map(|inspection| inspection.state_status.clone());
    let container_started_at = container_inspection
        .as_ref()
        .and_then(|inspection| inspection.started_at.clone());
    let network_name =
        select_network_name(promoted_runtime.as_ref(), container_inspection.as_ref());
    let container_ip = network_name
        .as_deref()
        .and_then(|network| {
            container_inspection
                .as_ref()
                .and_then(|inspection| inspection.network_ips.get(network).cloned())
        })
        .or_else(|| {
            container_inspection
                .as_ref()
                .and_then(|inspection| inspection.network_ips.values().next().cloned())
        });
    let image_ref = container_inspection
        .as_ref()
        .map(|inspection| inspection.image_ref.clone())
        .or_else(|| promoted_build.as_ref().map(|build| build.image_ref.clone()));

    let route_details = inspect_route_status(
        routing,
        project_id,
        environment,
        &domain,
        promoted_runtime.as_ref(),
        container_inspection.as_ref(),
        network_name.as_deref(),
    );

    let route_active = route_details
        .as_ref()
        .and_then(|details| details.inspection.as_ref())
        .is_some();
    let route_matches = route_details
        .as_ref()
        .is_some_and(RouteStatusDetails::matches_truth);
    let route_required = promoted_runtime
        .as_ref()
        .and_then(|runtime| runtime.activation.as_ref())
        .is_some_and(|activation| matches!(activation, PersistedActivationMode::Http { .. }));
    let runtime_matches_promoted = current_generation == active_generation;
    let promoted_snapshot_healthy = promoted_snapshot
        .as_ref()
        .is_some_and(|snapshot| snapshot.state == "healthy");

    let status = if deploying {
        "deploying"
    } else if current_generation.is_some()
        && promoted_snapshot_healthy
        && container_running
        && runtime_matches_promoted
        && (!route_required || route_matches)
    {
        "healthy"
    } else if current_generation.is_none()
        && latest_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.state == "failed")
    {
        "failed"
    } else if current_generation.is_none()
        && active_generation.is_none()
        && latest_snapshot.is_none()
        && promoted_runtime.is_none()
        && promoted_build.is_none()
    {
        "missing"
    } else {
        "degraded"
    };

    Ok(ProjectEnvironmentStatus {
        project_id: project_id.to_string(),
        environment: environment.to_string(),
        status: status.into(),
        active_generation,
        domain,
        commit_sha: promoted_build
            .as_ref()
            .and_then(|build| build.commit_sha.clone())
            .or_else(|| {
                promoted_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.commit_sha.clone())
            }),
        source_ref: promoted_build
            .as_ref()
            .and_then(|build| build.source_ref.clone())
            .or_else(|| {
                promoted_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.source_ref.clone())
            }),
        container_name,
        container_running,
        container_status,
        network_name,
        container_ip,
        route_active,
        probe_path: promoted_runtime
            .as_ref()
            .and_then(|runtime| runtime.probe_path.clone()),
        image_ref,
        last_deployment_id: promoted_build
            .as_ref()
            .map(|build| build.deployment_id.clone())
            .or_else(|| {
                latest_build
                    .as_ref()
                    .map(|build| build.deployment_id.clone())
            }),
        deployed_at_unix: promoted_snapshot
            .as_ref()
            .map(|snapshot| snapshot.finalized_at_unix)
            .or_else(|| {
                latest_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.finalized_at_unix)
            }),
        container_started_at,
    })
}

fn latest_generation(env: &EnvironmentPaths) -> Result<Option<u64>, ProjectStatusError> {
    let generations_dir = env.generations_dir();
    if !generations_dir.exists() {
        return Ok(None);
    }

    let mut latest = None;
    for entry in fs::read_dir(generations_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(generation) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse().ok())
        else {
            continue;
        };
        if latest.is_none_or(|current| generation > current) {
            latest = Some(generation);
        }
    }
    Ok(latest)
}

fn inspect_promoted_container<D: DockerRuntime>(
    docker: &mut D,
    runtime: Option<&PersistedRuntimeInfo>,
) -> Option<ContainerInspection> {
    let container_name = runtime.map(|runtime| runtime.container_name.as_str())?;
    docker.inspect_container(container_name).ok()
}

fn select_network_name(
    runtime: Option<&PersistedRuntimeInfo>,
    inspection: Option<&ContainerInspection>,
) -> Option<String> {
    runtime
        .and_then(|runtime| runtime.network_name.clone())
        .or_else(|| inspection.and_then(|inspection| inspection.network_ips.keys().next().cloned()))
}

#[derive(Debug, Clone)]
struct RouteStatusDetails {
    inspection: Option<RouteInspection>,
    expected_target: Option<String>,
    expected_domain: String,
}

impl RouteStatusDetails {
    fn matches_truth(&self) -> bool {
        let Some(inspection) = &self.inspection else {
            return false;
        };
        let Some(expected_target) = self.expected_target.as_deref() else {
            return false;
        };
        inspection.active_target == expected_target
            && inspection.domain.as_deref() == Some(self.expected_domain.as_str())
    }
}

fn inspect_route_status<R: RoutingRuntime>(
    routing: &mut R,
    project_id: &str,
    environment: &str,
    domain: &str,
    runtime: Option<&PersistedRuntimeInfo>,
    container: Option<&ContainerInspection>,
    network_name: Option<&str>,
) -> Option<RouteStatusDetails> {
    let runtime = runtime?;
    let PersistedActivationMode::Http {
        internal_port,
        route_subtree_id: persisted_subtree_id,
        target_source,
    } = runtime.activation.as_ref()?
    else {
        return None;
    };
    let subtree_id = persisted_subtree_id
        .clone()
        .unwrap_or_else(|| route_subtree_id(project_id, environment));
    let inspection = routing.inspect_route(&subtree_id).ok();
    let expected_target = container.and_then(|container| {
        resolve_route_target(container, *internal_port, network_name, target_source)
    });
    Some(RouteStatusDetails {
        inspection,
        expected_target,
        expected_domain: domain.to_string(),
    })
}

fn resolve_route_target(
    container: &ContainerInspection,
    internal_port: u16,
    network_name: Option<&str>,
    target_source: &PersistedRouteTargetSource,
) -> Option<String> {
    match target_source {
        PersistedRouteTargetSource::ContainerIp => {
            let ip = network_name
                .and_then(|network| container.network_ips.get(network))
                .or_else(|| container.network_ips.values().next())?;
            Some(format!("{ip}:{internal_port}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crate::api::ProjectUpsertRequest;
    use crate::runtime::{
        BuildImageRequest, CreateContainerRequest, ManagedImage, RouteUpdateRequest,
    };
    use crate::storage::{
        PersistedRuntimeInfo, PointerStore, RuntimeHealthState, RuntimeState, RuntimeStateStore,
        SnapshotState, SnapshotWriter, atomic_write,
    };

    #[derive(Default)]
    struct StubDockerRuntime {
        inspection: Option<ContainerInspection>,
    }

    impl DockerRuntime for StubDockerRuntime {
        fn build_image(
            &mut self,
            request: BuildImageRequest,
        ) -> Result<String, DockerRuntimeError> {
            Ok(request.image_tag)
        }

        fn ensure_network(&mut self, _network_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn create_container(
            &mut self,
            request: CreateContainerRequest,
        ) -> Result<String, DockerRuntimeError> {
            Ok(request.container_name)
        }

        fn start_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
            Ok(())
        }

        fn inspect_container(
            &mut self,
            _container_name: &str,
        ) -> Result<ContainerInspection, DockerRuntimeError> {
            self.inspection.clone().ok_or_else(|| {
                DockerRuntimeError::CommandFailed("Error: No such object: container".into())
            })
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
            Ok(self.inspection.clone().into_iter().collect())
        }

        fn list_managed_images(&mut self) -> Result<Vec<ManagedImage>, DockerRuntimeError> {
            Ok(Vec::new())
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
    }

    #[derive(Default)]
    struct StubRoutingRuntime {
        inspection: Option<RouteInspection>,
    }

    impl RoutingRuntime for StubRoutingRuntime {
        fn update_route(
            &mut self,
            _request: RouteUpdateRequest,
        ) -> Result<(), RoutingRuntimeError> {
            Ok(())
        }

        fn inspect_route(
            &mut self,
            _subtree_id: &str,
        ) -> Result<RouteInspection, RoutingRuntimeError> {
            self.inspection
                .clone()
                .ok_or_else(|| RoutingRuntimeError::InspectionFailed("missing route".into()))
        }

        fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
            Ok(self.inspection.clone().into_iter().collect())
        }

        fn remove_route(&mut self, _subtree_id: &str) -> Result<(), RoutingRuntimeError> {
            Ok(())
        }
    }

    fn test_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "forge-status-tests-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn register_project(root: &Path, project_id: &str, base_domain: &str) {
        ProjectRegistryStore::new(root)
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some(project_id.into()),
                    repo_url: format!("https://github.com/example/{project_id}.git"),
                    default_branch: "main".into(),
                    base_domain: Some(base_domain.into()),
                },
                None,
            )
            .unwrap();
    }

    fn write_generation(root: &Path, generation: u64) {
        let env = EnvironmentPaths::new(root, "api", "staging");
        let writer = SnapshotWriter::new(env.clone(), generation).unwrap();
        writer
            .write_artifact(
                "build.json",
                &format!(
                    concat!(
                        "{{\n",
                        "  \"deployment_id\": \"dep-{}\",\n",
                        "  \"image_ref\": \"forge/api:staging-gen-{}\",\n",
                        "  \"source_ref\": \"main\",\n",
                        "  \"commit_sha\": \"340ac8108006d84dbf951d8c0bb04ecfaf0eccac\"\n",
                        "}}\n"
                    ),
                    generation, generation,
                ),
            )
            .unwrap();
        let runtime = serde_json::to_string_pretty(&PersistedRuntimeInfo {
            container_name: format!("staging-api-gen-{generation}"),
            running: true,
            network_name: Some("forge-managed".into()),
            probe_path: Some("/health".into()),
            activation: Some(PersistedActivationMode::Http {
                internal_port: 3000,
                route_subtree_id: Some("forge:api:staging".into()),
                target_source: PersistedRouteTargetSource::ContainerIp,
            }),
            environment_variables: BTreeMap::new(),
            source_ref: Some("main".into()),
            repo_url: None,
            commit_sha: Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac".into()),
            source_path: None,
        })
        .unwrap();
        writer
            .write_artifact("runtime.json", &format!("{runtime}\n"))
            .unwrap();
        writer
            .finalize("api", "staging", SnapshotState::Healthy)
            .unwrap();
        PointerStore::new(env.clone())
            .swap_current(generation)
            .unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(generation),
                health_state: RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 1,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "healthy".into(),
                last_error_code: None,
            })
            .unwrap();
    }

    fn healthy_container(generation: u64) -> ContainerInspection {
        ContainerInspection {
            container_name: format!("staging-api-gen-{generation}"),
            running: true,
            state_status: "running".into(),
            exit_code: Some(0),
            started_at: Some("2026-05-21T12:00:00Z".into()),
            image_ref: format!("forge/api:staging-gen-{generation}"),
            labels: BTreeMap::new(),
            network_ips: BTreeMap::from([("forge-managed".into(), "172.29.0.2".into())]),
            restart_policy: "no".into(),
        }
    }

    fn healthy_route() -> RouteInspection {
        RouteInspection {
            subtree_id: "forge:api:staging".into(),
            active_target: "172.29.0.2:3000".into(),
            domain: Some("staging-api.example.com".into()),
            activation_verified: true,
            verification_url: None,
            verification_host: None,
            verification_status_code: None,
            verification_response_body: None,
            health_checks_enabled: false,
        }
    }

    #[test]
    fn status_reports_promoted_generation_runtime() {
        let root = test_root("reports-promoted-generation-runtime");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "healthy");
        assert_eq!(status.active_generation, Some(7));
        assert_eq!(status.domain, "staging-api.example.com");
        assert_eq!(status.container_name.as_deref(), Some("staging-api-gen-7"));
        assert!(status.container_running);
        assert_eq!(status.network_name.as_deref(), Some("forge-managed"));
        assert_eq!(status.container_ip.as_deref(), Some("172.29.0.2"));
        assert!(status.route_active);
    }

    #[test]
    fn status_detects_missing_container() {
        let root = test_root("detects-missing-container");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut docker = StubDockerRuntime { inspection: None };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "degraded");
        assert!(!status.container_running);
    }

    #[test]
    fn status_detects_route_target_mismatch() {
        let root = test_root("detects-route-target-mismatch");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(RouteInspection {
                active_target: "172.29.0.55:3000".into(),
                ..healthy_route()
            }),
        };

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "degraded");
        assert!(status.route_active);
    }

    #[test]
    fn status_derives_environment_domain_correctly() {
        assert_eq!(
            derive_environment_domain("api.example.com", "production"),
            "api.example.com"
        );
        assert_eq!(
            derive_environment_domain("api.example.com", "staging"),
            "staging-api.example.com"
        );
        assert_eq!(
            derive_environment_domain("api.example.com", "development"),
            "development-api.example.com"
        );
    }

    #[test]
    fn status_reports_degraded_when_route_missing() {
        let root = test_root("reports-degraded-when-route-missing");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime { inspection: None };

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "degraded");
        assert!(!status.route_active);
    }

    #[test]
    fn status_json_matches_runtime_truth() {
        let root = test_root("json-matches-runtime-truth");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 7);

        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        atomic_write(
            root.join("queue").join("queued.db"),
            b"{\"deployment_id\":\"dep-8\",\"project_id\":\"api\",\"environment\":\"staging\"}\n",
        )
        .unwrap();
        atomic_write(root.join("queue").join("active.db"), b"\n").unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(7)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let status = load_project_environment_status(
            &root,
            Some(&queue),
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();
        let json = serde_json::to_value(&status).unwrap();

        assert_eq!(json["project_id"], "api");
        assert_eq!(json["environment"], "staging");
        assert_eq!(json["status"], "deploying");
        assert_eq!(json["active_generation"], 7);
        assert_eq!(json["container_running"], true);
        assert_eq!(json["route_active"], true);
        assert_eq!(json["image_ref"], "forge/api:staging-gen-7");
    }

    #[test]
    fn status_handles_missing_generation_gracefully() {
        let root = test_root("handles-missing-generation-gracefully");
        register_project(&root, "api", "api.example.com");

        let mut docker = StubDockerRuntime::default();
        let mut routing = StubRoutingRuntime::default();

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "missing");
        assert_eq!(status.active_generation, None);
        assert!(!status.container_running);
        assert!(!status.route_active);
    }

    #[test]
    fn status_reports_failed_without_healthy_promoted_generation() {
        let root = test_root("reports-failed-without-healthy-promoted-generation");
        register_project(&root, "api", "api.example.com");
        let env = EnvironmentPaths::new(&root, "api", "staging");
        SnapshotWriter::new(env.clone(), 3)
            .unwrap()
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-3\",\n  \"image_ref\": \"forge/api:staging-gen-3\"\n}\n",
            )
            .unwrap();
        SnapshotWriter::new(env, 3)
            .unwrap()
            .finalize("api", "staging", SnapshotState::Failed)
            .unwrap();

        let mut docker = StubDockerRuntime::default();
        let mut routing = StubRoutingRuntime::default();

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "failed");
        assert_eq!(status.last_deployment_id.as_deref(), Some("dep-3"));
    }

    #[test]
    fn status_after_rollback_reports_restored_generation() {
        let root = test_root("status-after-rollback-reports-restored-generation");
        register_project(&root, "api", "api.example.com");
        write_generation(&root, 1);
        write_generation(&root, 2);
        let env = EnvironmentPaths::new(&root, "api", "staging");
        PointerStore::new(env.clone()).swap_current(1).unwrap();
        RuntimeStateStore::new(env)
            .save(&RuntimeState {
                active_generation: Some(1),
                health_state: RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 1,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "rollback_completed".into(),
                last_error_code: None,
            })
            .unwrap();

        let mut docker = StubDockerRuntime {
            inspection: Some(healthy_container(1)),
        };
        let mut routing = StubRoutingRuntime {
            inspection: Some(healthy_route()),
        };

        let status = load_project_environment_status(
            &root,
            None,
            &mut docker,
            &mut routing,
            "api",
            "staging",
        )
        .unwrap();

        assert_eq!(status.status, "healthy");
        assert_eq!(status.active_generation, Some(1));
        assert_eq!(
            status.commit_sha.as_deref(),
            Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac")
        );
        assert_eq!(status.source_ref.as_deref(), Some("main"));
        assert_eq!(status.image_ref.as_deref(), Some("forge/api:staging-gen-1"));
        assert_eq!(status.last_deployment_id.as_deref(), Some("dep-1"));
    }
}
