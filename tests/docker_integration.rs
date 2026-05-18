#[path = "integration/common.rs"]
mod common;

use std::collections::BTreeMap;
use std::process::Command;

use forge_core::docker::{DockerCliRuntime, ProcessCommandRunner};
use forge_core::runtime::{BuildImageRequest, CreateContainerRequest, DockerRuntime};

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

    docker
        .remove_container(&container_name)
        .expect("cleanup should remove the container");

    let artifact = runtime_root.join("docker_integration.ok");
    std::fs::write(&artifact, built_image).expect("integration artifact should be writable");
    assert!(artifact.exists());
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
