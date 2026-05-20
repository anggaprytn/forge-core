#[path = "integration/common.rs"]
mod common;

use std::collections::BTreeMap;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use forge_core::deployments::{
    ActivationMode, DeploymentExecutor, ExecutionConfig, ValidationPolicy,
};
use forge_core::docker::{DockerCliRuntime, ProcessCommandRunner};
use forge_core::probes::DockerNetworkProbeRuntime;
use forge_core::queue::{DeploymentRecord, PersistentQueue};
use forge_core::runtime::{
    BuildImageRequest, CreateContainerRequest, DockerRuntime, RouteInspection, RouteUpdateRequest,
    RoutingRuntime, RoutingRuntimeError,
};
use forge_core::storage::{EnvironmentPaths, PointerStore};

fn forge_labels(project_id: &str, environment: &str, generation: u64) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("forge.managed".into(), "true".into()),
        ("forge.project_id".into(), project_id.into()),
        ("forge.environment".into(), environment.into()),
        ("forge.generation".into(), generation.to_string()),
    ])
}

#[test]
fn docker_integration_real_adapter_honors_runtime_invariants() {
    if !common::ensure_integration_enabled() || !common::ensure_docker_available() {
        return;
    }

    let runtime_root = common::runtime_root("docker-integration");
    let fixture = common::sample_http_app_fixture();
    let generation = 42_u64;
    let container_name = format!("prod-sample-http-gen-{generation}");
    let image_tag = format!("forge/sample-http:{generation}");
    let labels = forge_labels("sample-http", "production", generation);
    let mut docker = DockerCliRuntime::new(ProcessCommandRunner);

    let _guard = CleanupGuard {
        container_name: container_name.clone(),
    };

    let built_image = docker
        .build_image(BuildImageRequest {
            image_tag: image_tag.clone(),
            context_path: fixture.clone(),
            dockerfile_path: fixture.join("Dockerfile"),
            labels: labels.clone(),
        })
        .expect("sample image should build through the real docker adapter");

    assert!(
        !built_image.is_empty(),
        "real adapter should return a built image reference"
    );

    let created_container = docker
        .create_container(CreateContainerRequest {
            container_name: container_name.clone(),
            image_ref: built_image.clone(),
            labels: labels.clone(),
            environment: Default::default(),
            network_name: None,
        })
        .expect("generation-named container should be created");

    assert_eq!(created_container, container_name);

    docker
        .start_container(&container_name)
        .expect("container should start through the real adapter");

    let inspection = docker
        .inspect_container(&container_name)
        .expect("running container should be inspectable");

    assert!(inspection.running, "container should be running");
    assert_eq!(inspection.container_name, container_name);
    assert_eq!(inspection.restart_policy, "no");
    assert_eq!(inspection.image_ref, image_tag);
    assert_eq!(
        inspection.labels.get("forge.managed"),
        Some(&"true".to_string())
    );
    assert_eq!(
        inspection.labels.get("forge.project_id"),
        Some(&"sample-http".to_string())
    );
    assert_eq!(
        inspection.labels.get("forge.environment"),
        Some(&"production".to_string())
    );
    assert_eq!(
        inspection.labels.get("forge.generation"),
        Some(&generation.to_string())
    );
    assert!(
        inspection.network_ips.values().any(|ip| !ip.is_empty()),
        "real adapter should surface at least one container network IP"
    );

    docker
        .remove_container(&container_name)
        .expect("cleanup should remove the container");

    let artifact = runtime_root.join("docker_integration.ok");
    std::fs::write(&artifact, built_image).expect("integration artifact should be writable");
    assert!(artifact.exists());
}

#[test]
fn docker_integration_executor_validates_candidate_over_container_ip() {
    if !common::ensure_integration_enabled() || !common::ensure_docker_available() {
        return;
    }

    let runtime_root = common::runtime_root("docker-validation");
    let fixture = common::sample_http_app_fixture();
    let network_name = format!("forge-docker-validation-{}", unique_suffix());
    let container_name = "prod-api-gen-1".to_string();
    let _guard = ValidationCleanupGuard {
        container_name: container_name.clone(),
        network_name: network_name.clone(),
    };

    docker(&["network", "create", &network_name]).expect("docker network should be creatable");

    let queue = PersistentQueue::new(runtime_root.join("queue")).unwrap();
    queue
        .enqueue(DeploymentRecord {
            deployment_id: "dep-1".into(),
            project_id: "api".into(),
            environment: "production".into(),
            source_path: None,
            source_ref: None,
            repo_url: None,
            commit_sha: None,
        })
        .unwrap();

    let mut docker = DockerCliRuntime::new(ProcessCommandRunner);
    let mut probes = DockerNetworkProbeRuntime::new(network_name.clone(), 3000);
    let mut routing = NoopRoutingRuntime;

    let execution = DeploymentExecutor::new(
        &runtime_root,
        &queue,
        &mut docker,
        &mut probes,
        &mut routing,
        ValidationPolicy {
            tcp_required: true,
            http_health_path: Some("/health".into()),
            activation: ActivationMode::Direct,
        },
    )
    .with_execution_config(ExecutionConfig {
        context_path: fixture.clone(),
        dockerfile_path: fixture.join("Dockerfile"),
        network_name: Some(network_name.clone()),
    })
    .execute_next()
    .expect("deployment execution should succeed")
    .expect("queued deployment should execute");

    assert_eq!(execution.container_name, container_name);
    assert!(
        runtime_root
            .join("projects/api/environments/production/generations/1/snapshot.json")
            .exists()
    );
    assert_eq!(
        PointerStore::new(EnvironmentPaths::new(&runtime_root, "api", "production"))
            .read_pointer("current")
            .unwrap(),
        Some(1)
    );
}

struct CleanupGuard {
    container_name: String,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["stop", self.container_name.as_str()])
            .output();
        let _ = Command::new("docker")
            .args(["rm", "-f", self.container_name.as_str()])
            .output();
    }
}

struct ValidationCleanupGuard {
    container_name: String,
    network_name: String,
}

impl Drop for ValidationCleanupGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", self.container_name.as_str()])
            .output();
        let _ = Command::new("docker")
            .args(["network", "rm", self.network_name.as_str()])
            .output();
    }
}

struct NoopRoutingRuntime;

impl RoutingRuntime for NoopRoutingRuntime {
    fn update_route(&mut self, _request: RouteUpdateRequest) -> Result<(), RoutingRuntimeError> {
        Ok(())
    }

    fn inspect_route(&mut self, subtree_id: &str) -> Result<RouteInspection, RoutingRuntimeError> {
        Ok(RouteInspection {
            subtree_id: subtree_id.to_string(),
            active_target: String::new(),
            activation_verified: true,
            health_checks_enabled: false,
        })
    }

    fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError> {
        Ok(Vec::new())
    }

    fn remove_route(&mut self, _subtree_id: &str) -> Result<(), RoutingRuntimeError> {
        Ok(())
    }
}

fn docker(args: &[&str]) -> Result<(), String> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .map_err(|err| err.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
    }
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be valid")
        .as_nanos();
    format!("pid-{}-{nanos}", std::process::id())
}
