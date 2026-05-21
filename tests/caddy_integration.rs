#[path = "integration/common.rs"]
mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use forge_core::caddy::CaddyApiRuntime;
use forge_core::convergence::{ActiveTruth, ConvergenceEngine, TickInput};
use forge_core::deployments::{
    ActivationMode, DeploymentError, DeploymentExecutor, ValidationPolicy,
};
use forge_core::queue::{DeploymentRecord, PersistentQueue};
use forge_core::runtime::{
    BuildImageRequest, ContainerInspection, CreateContainerRequest, DockerRuntime,
    DockerRuntimeError, ProbeError, ProbeRuntime, RouteUpdateRequest, RoutingRuntime,
};
use forge_core::storage::{EnvironmentPaths, PointerStore, SnapshotState, SnapshotWriter};

#[test]
fn caddy_integration_forge_only_mutates_owned_caddy_subtree() {
    let _guard = integration_lock();
    let Some(mut harness) = CaddyHarness::start("owned-subtree") else {
        return;
    };
    harness.start_sample_app("prod-api-gen-41");

    let mut routing = harness.routing();
    let before = reqwest::blocking::get(harness.public_url("preserve"))
        .expect("preserve route should be reachable before forge route mutation");
    assert_eq!(before.status().as_u16(), 204);

    routing
        .update_route(RouteUpdateRequest {
            subtree_id: "forge:api:production".into(),
            target: "prod-api-gen-41:3000".into(),
            domain: None,
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .expect("forge-owned subtree should be mutable");
    routing
        .update_route(RouteUpdateRequest {
            subtree_id: "forge:api:production".into(),
            target: "prod-api-gen-41:3000".into(),
            domain: None,
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .expect("repeated subtree updates should be idempotent");

    let err = routing
        .update_route(RouteUpdateRequest {
            subtree_id: "external:preserve".into(),
            target: "prod-api-gen-41:3000".into(),
            domain: None,
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .expect_err("adapter must reject non-forge subtree mutation");
    assert!(
        err.to_string().contains("forge-owned"),
        "unexpected ownership error: {err}"
    );

    let after = reqwest::blocking::get(harness.public_url("preserve"))
        .expect("preserve route should remain reachable after forge route mutation");
    assert_eq!(after.status().as_u16(), 204);

    let ids = harness.route_ids();
    assert!(ids.contains("external:preserve"));
    assert!(ids.contains("forge:api:production"));
    assert_eq!(
        ids.iter()
            .filter(|id| id.as_str() == "forge:api:production")
            .count(),
        1,
        "idempotent updates must not duplicate route entries"
    );
}

#[test]
fn caddy_integration_route_targets_generation_named_container() {
    let _guard = integration_lock();
    let Some(mut harness) = CaddyHarness::start("route-target") else {
        return;
    };
    harness.start_sample_app("prod-api-gen-42");
    let mut routing = harness.routing();

    routing
        .update_route(RouteUpdateRequest {
            subtree_id: "forge:api:production".into(),
            target: "prod-api-gen-42:3000".into(),
            domain: None,
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .unwrap();

    let inspection = routing.inspect_route("forge:api:production").unwrap();
    assert_eq!(inspection.active_target, "prod-api-gen-42:3000");
}

#[test]
fn caddy_integration_route_activation_probe_succeeds() {
    let _guard = integration_lock();
    let Some(mut harness) = CaddyHarness::start("route-activation") else {
        return;
    };
    harness.start_sample_app("prod-api-gen-43");
    let mut routing = harness.routing();

    routing
        .update_route(RouteUpdateRequest {
            subtree_id: "forge:api:production".into(),
            target: "prod-api-gen-43:3000".into(),
            domain: None,
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .unwrap();

    let inspection = routing.inspect_route("forge:api:production").unwrap();
    assert!(inspection.activation_verified);
    assert!(!inspection.health_checks_enabled);

    let response = reqwest::blocking::get(harness.public_url("health"))
        .expect("public caddy probe should succeed");
    assert_eq!(response.status().as_u16(), 200);
}

#[test]
fn route_verification_sets_host_header() {
    let _guard = integration_lock();
    let Some(mut harness) = CaddyHarness::start("route-verification-host-header") else {
        return;
    };
    harness.start_sample_app("prod-api-gen-43");
    let mut routing = harness.routing();

    routing
        .update_route(RouteUpdateRequest {
            subtree_id: "forge:api:production".into(),
            target: "prod-api-gen-43:3000".into(),
            domain: Some("api.example.com".into()),
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .unwrap();

    let inspection = routing.inspect_route("forge:api:production").unwrap();
    assert!(inspection.activation_verified);
    assert_eq!(
        inspection.verification_host.as_deref(),
        Some("api.example.com")
    );

    let no_host = reqwest::blocking::get(harness.public_url("health"))
        .expect("request without host header should complete");
    assert_eq!(no_host.status().as_u16(), 404);

    let with_host = reqwest::blocking::Client::new()
        .get(harness.public_url("health"))
        .header(reqwest::header::HOST, "api.example.com")
        .send()
        .expect("request with host header should reach routed app");
    assert_eq!(with_host.status().as_u16(), 200);
}

#[test]
fn caddy_integration_active_route_overrides_ready_placeholder() {
    let _guard = integration_lock();
    let Some(mut harness) = CaddyHarness::start("ready-placeholder-override") else {
        return;
    };
    harness.start_sample_app("prod-api-gen-44");
    harness.install_ready_placeholder();
    let mut routing = harness.routing();

    routing
        .update_route(RouteUpdateRequest {
            subtree_id: "forge:api:production".into(),
            target: "prod-api-gen-44:3000".into(),
            domain: None,
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .unwrap();

    let inspection = routing.inspect_route("forge:api:production").unwrap();
    assert_eq!(inspection.active_target, "prod-api-gen-44:3000");

    let response = reqwest::blocking::get(harness.public_url("health"))
        .expect("public caddy probe should reach active generation");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response.text().unwrap(),
        "ok\n",
        "active application route must take precedence over forge:ready"
    );

    let route_order = harness.route_order();
    assert_eq!(
        route_order.last().map(String::as_str),
        Some("forge:ready"),
        "forge:ready must remain behind active forge routes"
    );
}

#[test]
fn host_matched_route_not_shadowed_by_legacy_unmatched_route() {
    let _guard = integration_lock();
    let Some(mut harness) = CaddyHarness::start("host-shadowing-order") else {
        return;
    };
    harness.start_sample_app("prod-api-gen-45");
    harness.start_sample_app("staging-api-gen-45");
    let mut routing = harness.routing();

    routing
        .update_route(RouteUpdateRequest {
            subtree_id: "forge:api:production".into(),
            target: "prod-api-gen-45:3000".into(),
            domain: None,
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .unwrap();
    routing
        .update_route(RouteUpdateRequest {
            subtree_id: "forge:api:staging".into(),
            target: "staging-api-gen-45:3000".into(),
            domain: Some("staging.example.com".into()),
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .unwrap();

    let inspection = routing.inspect_route("forge:api:staging").unwrap();
    assert!(inspection.activation_verified);
    assert_eq!(inspection.domain.as_deref(), Some("staging.example.com"));

    let route_order = harness.route_order();
    assert_eq!(
        route_order,
        vec![
            "external:preserve",
            "forge:api:staging",
            "forge:api:production"
        ]
    );
}

#[test]
fn caddy_integration_failed_route_activation_does_not_advance_current() {
    let _guard = integration_lock();
    let Some(mut harness) = CaddyHarness::start("activation-failure") else {
        return;
    };
    harness.start_sample_app("prod-api-gen-2");

    let root = harness.runtime_root.clone();
    let env = EnvironmentPaths::new(&root, "api", "production");
    setup_finalized_generation(&root, 1, "api", "production");
    PointerStore::new(env.clone()).swap_current(1).unwrap();
    let queue = PersistentQueue::new(root.join("queue")).unwrap();
    queue
        .enqueue(DeploymentRecord {
            deployment_id: "dep-2".into(),
            project_id: "api".into(),
            environment: "production".into(),
            source_path: None,
            source_ref: None,
            repo_url: None,
            commit_sha: None,
        })
        .unwrap();

    let mut docker = FakeDockerRuntime::running(["prod-api-gen-2"]);
    let mut probes = FixedProbeRuntime {
        tcp_ok: true,
        http_ok: true,
    };
    let mut routing = CaddyApiRuntime::new(
        harness.admin_base_url(),
        format!("http://127.0.0.1:{}", common::available_port()),
    );

    let result = DeploymentExecutor::new(
        &root,
        &queue,
        &mut docker,
        &mut probes,
        &mut routing,
        ValidationPolicy {
            tcp_required: true,
            http_health_path: Some("/health".into()),
            activation: ActivationMode::Http {
                internal_port: 3000,
            },
        },
    )
    .execute_next();

    assert!(matches!(
        result,
        Err(DeploymentError::ValidationFailed(_)) | Err(DeploymentError::Routing(_))
    ));
    assert_eq!(
        PointerStore::new(env).read_pointer("current").unwrap(),
        Some(1)
    );
}

#[test]
fn caddy_integration_route_rollback_restores_previous_generation() {
    let _guard = integration_lock();
    let Some(mut harness) = CaddyHarness::start("route-rollback") else {
        return;
    };
    harness.start_sample_app("prod-api-gen-1");
    harness.start_sample_app("prod-api-gen-2");

    let root = harness.runtime_root.clone();
    setup_finalized_generation(&root, 1, "api", "production");
    setup_finalized_generation(&root, 2, "api", "production");
    let env = EnvironmentPaths::new(&root, "api", "production");
    let pointers = PointerStore::new(env.clone());
    pointers.swap_current(1).unwrap();
    pointers.swap_current(2).unwrap();

    let mut routing = harness.routing();
    routing
        .update_route(RouteUpdateRequest {
            subtree_id: "forge:api:production".into(),
            target: "prod-api-gen-2:3000".into(),
            domain: None,
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .unwrap();

    let queue = PersistentQueue::new(root.join("queue")).unwrap();
    let mut docker = FakeDockerRuntime::running(["prod-api-gen-2"]);
    let mut probes = FixedProbeRuntime {
        tcp_ok: false,
        http_ok: true,
    };
    let mut engine = ConvergenceEngine::new(&root, &queue, &mut docker, &mut probes, &mut routing);

    for now in [100, 101, 102, 133] {
        let _ = engine.tick(TickInput {
            project_id: "api".into(),
            environment: "production".into(),
            now_unix: now,
            truth: ActiveTruth::HttpRouted {
                internal_port: 3000,
            },
            http_health_path: Some("/health".into()),
        });
    }

    let inspection = routing.inspect_route("forge:api:production").unwrap();
    assert_eq!(inspection.active_target, "172.30.0.11:3000");
    assert!(inspection.activation_verified);
    assert_eq!(
        PointerStore::new(env).read_pointer("current").unwrap(),
        Some(1)
    );
}

#[test]
fn caddy_integration_caddy_subtree_cleanup_removes_only_forge_routes() {
    let _guard = integration_lock();
    let Some(mut harness) = CaddyHarness::start("subtree-cleanup") else {
        return;
    };
    harness.start_sample_app("prod-api-gen-44");
    let mut routing = harness.routing();

    routing
        .update_route(RouteUpdateRequest {
            subtree_id: "forge:api:production".into(),
            target: "prod-api-gen-44:3000".into(),
            domain: None,
            health_checks_enabled: false,
            probe_path: Some("/health".into()),
        })
        .unwrap();
    routing.remove_route("forge:api:production").unwrap();

    let ids = harness.route_ids();
    assert!(ids.contains("external:preserve"));
    assert!(!ids.contains("forge:api:production"));

    let preserve = reqwest::blocking::get(harness.public_url("preserve"))
        .expect("preserve route should remain reachable after forge subtree cleanup");
    assert_eq!(preserve.status().as_u16(), 204);
}

struct CaddyHarness {
    runtime_root: PathBuf,
    network_name: String,
    caddy_container_name: String,
    sample_image_tag: String,
    sample_containers: Vec<String>,
    admin_port: u16,
    public_port: u16,
}

impl CaddyHarness {
    fn start(test_name: &str) -> Option<Self> {
        if !common::ensure_integration_enabled() || !common::ensure_docker_available() {
            return None;
        }

        let runtime_root = common::runtime_root("caddy-integration");
        let suffix = unique_suffix();
        let network_name = format!("forge-int-net-{test_name}-{suffix}");
        let caddy_container_name = format!("forge-int-caddy-{test_name}-{suffix}");
        let sample_image_tag = format!("forge/caddy-sample:{test_name}-{suffix}");
        let admin_port = common::available_port();
        let public_port = common::available_port();

        docker(&["network", "create", &network_name]).expect("docker network should be creatable");
        write_caddy_config(&runtime_root);
        docker(&[
            "build",
            "-t",
            &sample_image_tag,
            common::sample_http_app_fixture()
                .to_str()
                .expect("fixture path should be valid utf-8"),
        ])
        .expect("sample image should build for caddy integration");

        let config_path = runtime_root.join("caddy.json");
        docker(&[
            "run",
            "-d",
            "--name",
            &caddy_container_name,
            "--network",
            &network_name,
            "-p",
            &format!("127.0.0.1:{public_port}:8080"),
            "-p",
            &format!("127.0.0.1:{admin_port}:2019"),
            "-v",
            &format!(
                "{}:/etc/caddy/caddy.json:ro",
                config_path
                    .to_str()
                    .expect("config path should be valid utf-8")
            ),
            "caddy:2.8.4",
            "caddy",
            "run",
            "--config",
            "/etc/caddy/caddy.json",
        ])
        .expect("dockerized caddy should start");

        let harness = Self {
            runtime_root,
            network_name,
            caddy_container_name,
            sample_image_tag,
            sample_containers: Vec::new(),
            admin_port,
            public_port,
        };
        harness.wait_until_ready();
        Some(harness)
    }

    fn routing(&self) -> CaddyApiRuntime {
        CaddyApiRuntime::new(self.admin_base_url(), self.public_base_url())
    }

    fn admin_base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.admin_port)
    }

    fn public_base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.public_port)
    }

    fn public_url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.public_base_url(),
            path.trim_start_matches('/')
        )
    }

    fn start_sample_app(&mut self, container_name: &str) {
        docker(&[
            "run",
            "-d",
            "--name",
            container_name,
            "--network",
            &self.network_name,
            &self.sample_image_tag,
        ])
        .expect("sample app container should start on the caddy test network");
        self.sample_containers.push(container_name.to_string());
    }

    fn route_ids(&self) -> BTreeSet<String> {
        let routes = reqwest::blocking::get(format!(
            "{}/config/apps/http/servers/forge/routes",
            self.admin_base_url()
        ))
        .expect("caddy admin route listing should succeed")
        .json::<Vec<serde_json::Value>>()
        .expect("caddy routes should decode as json");

        routes
            .into_iter()
            .filter_map(|route| {
                route
                    .get("@id")
                    .and_then(|id| id.as_str())
                    .map(ToOwned::to_owned)
            })
            .collect()
    }

    fn route_order(&self) -> Vec<String> {
        let routes = reqwest::blocking::get(format!(
            "{}/config/apps/http/servers/forge/routes",
            self.admin_base_url()
        ))
        .expect("caddy admin route listing should succeed")
        .json::<Vec<serde_json::Value>>()
        .expect("caddy routes should decode as json");

        routes
            .into_iter()
            .filter_map(|route| {
                route
                    .get("@id")
                    .and_then(|id| id.as_str())
                    .map(ToOwned::to_owned)
            })
            .collect()
    }

    fn install_ready_placeholder(&self) {
        let mut config = reqwest::blocking::get(format!("{}/config/", self.admin_base_url()))
            .expect("caddy config inspection should succeed")
            .json::<serde_json::Value>()
            .expect("caddy config should decode as json");
        let routes = config["apps"]["http"]["servers"]["forge"]["routes"]
            .as_array_mut()
            .expect("forge routes should be an array");
        routes.push(serde_json::json!({
            "@id": "forge:ready",
            "terminal": true,
            "handle": [{
                "handler": "static_response",
                "status_code": 200,
                "body": "forge caddy ready"
            }]
        }));

        let client = reqwest::blocking::Client::new();
        let response = client
            .post(format!("{}/load", self.admin_base_url()))
            .json(&config)
            .send()
            .expect("caddy config load should succeed");
        assert!(
            response.status().is_success(),
            "ready placeholder install failed: {}",
            response.status()
        );
    }

    fn wait_until_ready(&self) {
        for _ in 0..40 {
            if let Ok(response) = reqwest::blocking::get(format!(
                "{}/config/apps/http/servers/forge/routes",
                self.admin_base_url()
            )) {
                if response.status().is_success() {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(250));
        }
        panic!("dockerized caddy did not become ready in time");
    }
}

impl Drop for CaddyHarness {
    fn drop(&mut self) {
        for name in &self.sample_containers {
            let _ = docker(&["rm", "-f", name]);
        }
        let _ = docker(&["rm", "-f", &self.caddy_container_name]);
        let _ = docker(&["network", "rm", &self.network_name]);
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

fn write_caddy_config(root: &Path) {
    let config = serde_json::json!({
        "admin": {
            "listen": ":2019"
        },
        "apps": {
            "http": {
                "servers": {
                    "forge": {
                        "listen": [":8080"],
                        "automatic_https": {
                            "disable": true
                        },
                        "routes": [{
                            "@id": "external:preserve",
                            "match": [{
                                "path": ["/preserve"]
                            }],
                            "handle": [{
                                "handler": "static_response",
                                "status_code": 204,
                                "body": "preserve"
                            }]
                        }]
                    }
                }
            }
        }
    });
    std::fs::write(
        root.join("caddy.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .expect("caddy config should be writable");
}

fn setup_finalized_generation(root: &Path, generation: u64, project_id: &str, environment: &str) {
    let env = EnvironmentPaths::new(root, project_id, environment);
    let writer = SnapshotWriter::new(env.clone(), generation).unwrap();
    writer
        .finalize(project_id, environment, SnapshotState::Healthy)
        .unwrap();
    std::fs::write(env.generation_counter(), format!("{generation}\n")).unwrap();
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be valid")
        .as_nanos();
    format!("pid-{}-{nanos}", std::process::id())
}

fn integration_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

#[derive(Default)]
struct FixedProbeRuntime {
    tcp_ok: bool,
    http_ok: bool,
}

impl ProbeRuntime for FixedProbeRuntime {
    fn probe_tcp(
        &mut self,
        _container_name: &str,
        _internal_port: u16,
    ) -> Result<bool, ProbeError> {
        Ok(self.tcp_ok)
    }

    fn probe_http(
        &mut self,
        _container_name: &str,
        _internal_port: u16,
        _path: &str,
    ) -> Result<bool, ProbeError> {
        Ok(self.http_ok)
    }
}

struct FakeDockerRuntime {
    running: BTreeSet<String>,
}

impl FakeDockerRuntime {
    fn running<const N: usize>(names: [&str; N]) -> Self {
        Self {
            running: names.into_iter().map(|name| name.to_string()).collect(),
        }
    }
}

impl DockerRuntime for FakeDockerRuntime {
    fn build_image(&mut self, request: BuildImageRequest) -> Result<String, DockerRuntimeError> {
        Ok(request.image_tag)
    }

    fn ensure_network(&mut self, _network_name: &str) -> Result<(), DockerRuntimeError> {
        Ok(())
    }

    fn create_container(
        &mut self,
        request: CreateContainerRequest,
    ) -> Result<String, DockerRuntimeError> {
        self.running.insert(request.container_name.clone());
        Ok(request.container_name)
    }

    fn start_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError> {
        self.running.insert(container_name.to_string());
        Ok(())
    }

    fn inspect_container(
        &mut self,
        container_name: &str,
    ) -> Result<ContainerInspection, DockerRuntimeError> {
        if !self.running.contains(container_name) {
            return Err(DockerRuntimeError::InvalidResponse(
                "missing container".into(),
            ));
        }
        Ok(ContainerInspection {
            container_name: container_name.to_string(),
            running: true,
            state_status: "running".into(),
            exit_code: Some(0),
            image_ref: "forge:test".into(),
            labels: fake_container_labels(container_name),
            network_ips: BTreeMap::from([("forge-test".into(), fake_container_ip(container_name))]),
            restart_policy: "no".into(),
        })
    }

    fn container_logs(
        &mut self,
        _container_name: &str,
        _tail_lines: usize,
    ) -> Result<String, DockerRuntimeError> {
        Ok(String::new())
    }

    fn list_managed_containers(&mut self) -> Result<Vec<ContainerInspection>, DockerRuntimeError> {
        Ok(self
            .running
            .iter()
            .map(|name| ContainerInspection {
                container_name: name.clone(),
                running: true,
                state_status: "running".into(),
                exit_code: Some(0),
                image_ref: "forge:test".into(),
                labels: fake_container_labels(name),
                network_ips: BTreeMap::from([("forge-test".into(), fake_container_ip(name))]),
                restart_policy: "no".into(),
            })
            .collect())
    }

    fn list_managed_images(
        &mut self,
    ) -> Result<Vec<forge_core::runtime::ManagedImage>, DockerRuntimeError> {
        Ok(Vec::new())
    }

    fn stop_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError> {
        self.running.remove(container_name);
        Ok(())
    }

    fn remove_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError> {
        self.running.remove(container_name);
        Ok(())
    }

    fn remove_image(&mut self, _image_ref: &str) -> Result<(), DockerRuntimeError> {
        Ok(())
    }
}

fn fake_container_labels(container_name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
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
    ])
}

fn fake_container_ip(container_name: &str) -> String {
    let generation = container_name
        .rsplit("-gen-")
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    format!("172.30.0.{}", generation + 10)
}
