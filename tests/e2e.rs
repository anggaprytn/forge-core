#[path = "integration/common.rs"]
mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use forge_core::api::DeploymentRequest;
use forge_core::caddy::CaddyApiRuntime;
use forge_core::config::DaemonConfig;
use forge_core::convergence::{ActiveDeploymentDecider, ActiveTruth, ConvergenceEngine, RecoveryOutcome, TickInput};
use forge_core::daemon::Daemon;
use forge_core::deployments::{ActivationMode, DeploymentExecutor, ExecutionConfig, ValidationPolicy};
use forge_core::docker::{DockerCliRuntime, ProcessCommandRunner};
use forge_core::http::{router, ControlPlane, HttpState, IdempotencyStore};
use forge_core::probes::DockerNetworkProbeRuntime;
use forge_core::queue::{DeploymentRecord, PersistentQueue};
use forge_core::runtime::{ContainerInspection, DockerRuntime, DockerRuntimeError, RoutingRuntime};
use forge_core::storage::{EnvironmentPaths, EventStore, PointerStore};
use reqwest::blocking::Client;
use reqwest::StatusCode;
use serde_json::Value;
use tokio::net::TcpListener;

#[test]
fn e2e_sample_app_deploys_public_route() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("sample-deploy") else {
        return;
    };

    let deployment_id = harness.enqueue_deploy();
    let execution = harness.execute_next_deployment().unwrap();

    assert_eq!(execution.deployment_id, deployment_id);
    assert_eq!(execution.generation, 1);

    let response = harness
        .http_client
        .get(harness.public_url("health"))
        .send()
        .expect("public route should be reachable");
    assert_eq!(response.status(), StatusCode::OK);

    let deployment = harness.get_deployment(&deployment_id);
    assert_eq!(deployment["state"], "healthy");
    assert_eq!(
        PointerStore::new(EnvironmentPaths::new(&harness.runtime_root, "api", "production"))
            .read_pointer("current")
            .unwrap(),
        Some(1)
    );
}

#[test]
fn e2e_events_visible_after_deploy() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("events-after-deploy") else {
        return;
    };

    let deployment_id = harness.enqueue_deploy();
    harness.execute_next_deployment().unwrap();

    let events = harness.get_events();
    let event_types = events["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["event_type"].as_str())
        .collect::<Vec<_>>();
    assert!(event_types.contains(&"DEPLOYMENT_STARTED"));
    assert!(event_types.contains(&"IMAGE_BUILT"));
    assert!(event_types.contains(&"CONTAINER_STARTED"));
    assert!(event_types.contains(&"VALIDATION_PASSED"));
    assert!(event_types.contains(&"SNAPSHOT_FINALIZED"));
    assert!(event_types.contains(&"GENERATION_PROMOTED"));

    let persisted = EventStore::list_all(&harness.runtime_root).unwrap();
    assert!(
        persisted
            .iter()
            .any(|event| event.deployment_id.as_deref() == Some(deployment_id.as_str()))
    );
}

#[test]
fn e2e_rollback_restores_previous_generation() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("rollback") else {
        return;
    };

    harness.enqueue_deploy();
    harness.execute_next_deployment().unwrap();

    harness.enqueue_deploy();
    let second = harness.execute_next_deployment().unwrap();
    assert_eq!(second.generation, 2);

    docker(&[
        "exec",
        second.container_name.as_str(),
        "sh",
        "-lc",
        "rm -f /www/health",
    ])
    .expect("active generation health endpoint should be removable for rollback test");

    harness.run_convergence_ticks(&[100, 101, 102, 133]).unwrap();

    let route = harness.routing.inspect_route("forge:api:production").unwrap();
    assert_eq!(route.active_target, "prod-api-gen-1:3000");
    assert!(route.activation_verified);
    assert_eq!(
        PointerStore::new(EnvironmentPaths::new(&harness.runtime_root, "api", "production"))
            .read_pointer("current")
            .unwrap(),
        Some(1)
    );
}

#[test]
fn e2e_daemon_restart_reconstructs_current_route() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("restart-reconstruct") else {
        return;
    };

    harness.enqueue_deploy();
    harness.execute_next_deployment().unwrap();

    let env = EnvironmentPaths::new(&harness.runtime_root, "api", "production");
    std::fs::write(env.current_pointer(), "\n").unwrap();

    harness.restart_api_server(AllowAllDecider(true));
    harness.run_convergence_ticks(&[200]).unwrap();

    let current = PointerStore::new(env).read_pointer("current").unwrap();
    assert_eq!(current, Some(1));
    let route = harness.routing.inspect_route("forge:api:production").unwrap();
    assert_eq!(route.active_target, "prod-api-gen-1:3000");
}

#[test]
fn e2e_restart_during_inflight_deploy_fails_or_recovers_deterministically() {
    let _guard = integration_lock();
    let Some(harness) = E2eHarness::start("restart-inflight") else {
        return;
    };

    let deployment_id = harness.enqueue_deploy();
    let queue = PersistentQueue::new(harness.runtime_root.join("queue")).unwrap();
    let active = queue.start_next().unwrap().unwrap();
    assert_eq!(active.deployment_id, deployment_id);

    let mut daemon = Daemon::new(
        harness.config.clone(),
        NoopDockerRuntime,
        NoopRoutingRuntime,
        AllowAllDecider(false),
    );
    daemon.start().unwrap();

    assert_eq!(
        daemon.last_recovery_outcome(),
        Some(&RecoveryOutcome::Failed(DeploymentRecord {
            deployment_id,
            project_id: "api".into(),
            environment: "production".into(),
        }))
    );
    assert!(PersistentQueue::new(harness.runtime_root.join("queue"))
        .unwrap()
        .load_state()
        .unwrap()
        .active
        .is_none());
}

struct E2eHarness {
    runtime_root: PathBuf,
    network_name: String,
    caddy_container_name: String,
    admin_port: u16,
    public_port: u16,
    api_port: u16,
    token: String,
    config: DaemonConfig,
    http_client: Client,
    api_threads: Vec<JoinHandle<()>>,
    routing: CaddyApiRuntime,
}

impl E2eHarness {
    fn start(test_name: &str) -> Option<Self> {
        if !common::ensure_integration_enabled() || !common::ensure_docker_available() {
            return None;
        }

        let runtime_root = common::runtime_root("e2e");
        let suffix = unique_suffix();
        let network_name = format!("forge-e2e-net-{test_name}-{suffix}");
        let caddy_container_name = format!("forge-e2e-caddy-{test_name}-{suffix}");
        let sample_image_tag = format!("forge/e2e-sample:{test_name}-{suffix}");
        let admin_port = common::available_port();
        let public_port = common::available_port();
        let api_port = common::available_port();
        let token = "test-token".to_string();

        docker(&["network", "create", &network_name]).expect("docker network should be creatable");
        write_caddy_config(&runtime_root);
        docker(&[
            "build",
            "-t",
            &sample_image_tag,
            common::sample_http_app_fixture().to_str().unwrap(),
        ])
        .expect("sample app fixture should build");
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
            &format!("{}:/etc/caddy/caddy.json:ro", runtime_root.join("caddy.json").display()),
            "caddy:2.8.4",
            "caddy",
            "run",
            "--config",
            "/etc/caddy/caddy.json",
        ])
        .expect("dockerized caddy should start");

        let config = DaemonConfig {
            storage_root: runtime_root.clone(),
            api_bind: format!("127.0.0.1:{api_port}"),
            bearer_token: token.clone(),
            sqlite_path: None,
        };

        let mut harness = Self {
            runtime_root,
            network_name,
            caddy_container_name,
            admin_port,
            public_port,
            api_port,
            token,
            config: config.clone(),
            http_client: Client::new(),
            api_threads: Vec::new(),
            routing: CaddyApiRuntime::new(
                format!("http://127.0.0.1:{admin_port}"),
                format!("http://127.0.0.1:{public_port}"),
            ),
        };

        harness.wait_for_caddy();
        harness.restart_api_server(AllowAllDecider(true));
        Some(harness)
    }

    fn restart_api_server<A: ActiveDeploymentDecider + Send + 'static>(&mut self, decider: A) {
        std::fs::create_dir_all(&self.runtime_root).unwrap();
        self.api_port = common::available_port();
        self.config.api_bind = format!("127.0.0.1:{}", self.api_port);

        let mut daemon = Daemon::new(self.config.clone(), NoopDockerRuntime, NoopRoutingRuntime, decider);
        daemon.start().unwrap();

        let state = HttpState::new(
            Arc::new(Mutex::new(Box::new(daemon) as Box<dyn ControlPlane>)),
            self.token.clone(),
            IdempotencyStore::new(self.runtime_root.join("idempotency")).unwrap(),
        );
        let app = router(state);
        self.api_threads.push(spawn_http_server(self.api_port, app));
        self.wait_for_api_ready();
    }

    fn enqueue_deploy(&self) -> String {
        let response = self
            .http_client
            .post(self.api_url("deployments"))
            .bearer_auth(&self.token)
            .json(&DeploymentRequest {
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
            })
            .send()
            .expect("deploy request should reach api");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let json = response.json::<Value>().unwrap();
        json["data"]["deployment_id"].as_str().unwrap().to_string()
    }

    fn execute_next_deployment(
        &mut self,
    ) -> Result<forge_core::deployments::DeploymentExecution, forge_core::deployments::DeploymentError> {
        let queue = PersistentQueue::new(self.runtime_root.join("queue")).unwrap();
        let mut docker = DockerCliRuntime::new(ProcessCommandRunner);
        let mut probes = DockerNetworkProbeRuntime::new(self.network_name.clone(), 3000);
        let mut routing = CaddyApiRuntime::new(self.admin_base_url(), self.public_base_url());

        DeploymentExecutor::new(
            &self.runtime_root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
            ValidationPolicy {
                tcp_required: true,
                http_health_path: Some("/health".into()),
                activation: ActivationMode::Http { internal_port: 3000 },
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: common::sample_http_app_fixture(),
            dockerfile_path: common::sample_http_app_fixture().join("Dockerfile"),
            network_name: Some(self.network_name.clone()),
        })
        .execute_next()
        .map(|value| value.expect("queued deployment should execute"))
    }

    fn get_events(&self) -> Value {
        let response = self
            .http_client
            .get(self.api_url("events"))
            .bearer_auth(&self.token)
            .send()
            .expect("events request should reach api");
        assert_eq!(response.status(), StatusCode::OK);
        let json = response.json::<Value>().unwrap();
        json["data"].clone()
    }

    fn get_deployment(&self, deployment_id: &str) -> Value {
        let response = self
            .http_client
            .get(self.api_url(&format!("deployments/{deployment_id}")))
            .bearer_auth(&self.token)
            .send()
            .expect("deployment status request should reach api");
        assert_eq!(response.status(), StatusCode::OK);
        let json = response.json::<Value>().unwrap();
        json["data"].clone()
    }

    fn run_convergence_ticks(&mut self, ticks: &[u64]) -> Result<(), Box<dyn std::error::Error>> {
        self.run_convergence_ticks_with_path(ticks, Some("/health"))
    }

    fn run_convergence_ticks_with_path(
        &mut self,
        ticks: &[u64],
        http_health_path: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let queue = PersistentQueue::new(self.runtime_root.join("queue"))?;
        let mut docker = DockerCliRuntime::new(ProcessCommandRunner);
        let mut probes = DockerNetworkProbeRuntime::new(self.network_name.clone(), 3000);
        let mut routing = CaddyApiRuntime::new(self.admin_base_url(), self.public_base_url());
        let mut engine = ConvergenceEngine::new(
            &self.runtime_root,
            &queue,
            &mut docker,
            &mut probes,
            &mut routing,
        );
        for now in ticks {
            let _ = engine.tick(TickInput {
                project_id: "api".into(),
                environment: "production".into(),
                now_unix: *now,
                truth: ActiveTruth::HttpRouted { internal_port: 3000 },
                http_health_path: http_health_path.map(|path| path.to_string()),
            })?;
        }
        Ok(())
    }

    fn admin_base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.admin_port)
    }

    fn public_base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.public_port)
    }

    fn public_url(&self, path: &str) -> String {
        format!("{}/{}", self.public_base_url(), path.trim_start_matches('/'))
    }

    fn api_url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}/{}", self.api_port, path.trim_start_matches('/'))
    }

    fn wait_for_caddy(&self) {
        for _ in 0..40 {
            if let Ok(response) = self
                .http_client
                .get(format!("{}/config/apps/http/servers/forge/routes", self.admin_base_url()))
                .send()
            {
                if response.status().is_success() {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(250));
        }
        panic!("caddy admin endpoint did not become ready");
    }

    fn wait_for_api_ready(&self) {
        for _ in 0..40 {
            if let Ok(response) = self.http_client.get(self.api_url("readyz")).send() {
                if response.status().is_success() {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
        panic!("api readyz did not become ready");
    }
}

impl Drop for E2eHarness {
    fn drop(&mut self) {
        let _ = cleanup_forge_containers();
        let _ = docker(&["rm", "-f", &self.caddy_container_name]);
        let _ = docker(&["network", "rm", &self.network_name]);
    }
}

fn spawn_http_server(port: u16, app: Router) -> JoinHandle<()> {
    thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().expect("api runtime should start");
        runtime.block_on(async move {
            let listener = TcpListener::bind(("127.0.0.1", port))
                .await
                .expect("api listener should bind");
            let _ = axum::serve(listener, app).await;
        });
    })
}

#[derive(Clone, Copy)]
struct AllowAllDecider(bool);

impl ActiveDeploymentDecider for AllowAllDecider {
    fn should_resume(&self, _deployment: &DeploymentRecord) -> bool {
        self.0
    }
}

#[derive(Default)]
struct NoopDockerRuntime;

impl DockerRuntime for NoopDockerRuntime {
    fn build_image(
        &mut self,
        request: forge_core::runtime::BuildImageRequest,
    ) -> Result<String, DockerRuntimeError> {
        Ok(request.image_tag)
    }

    fn create_container(
        &mut self,
        request: forge_core::runtime::CreateContainerRequest,
    ) -> Result<String, DockerRuntimeError> {
        Ok(request.container_name)
    }

    fn start_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
        Ok(())
    }

    fn inspect_container(
        &mut self,
        container_name: &str,
    ) -> Result<ContainerInspection, DockerRuntimeError> {
        Ok(ContainerInspection {
            container_name: container_name.into(),
            running: true,
            image_ref: "noop".into(),
            labels: Default::default(),
            restart_policy: "no".into(),
        })
    }

    fn stop_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
        Ok(())
    }

    fn remove_container(&mut self, _container_name: &str) -> Result<(), DockerRuntimeError> {
        Ok(())
    }
}

#[derive(Default)]
struct NoopRoutingRuntime;

impl RoutingRuntime for NoopRoutingRuntime {
    fn update_route(
        &mut self,
        _request: forge_core::runtime::RouteUpdateRequest,
    ) -> Result<(), forge_core::runtime::RoutingRuntimeError> {
        Ok(())
    }

    fn inspect_route(
        &mut self,
        subtree_id: &str,
    ) -> Result<forge_core::runtime::RouteInspection, forge_core::runtime::RoutingRuntimeError> {
        Ok(forge_core::runtime::RouteInspection {
            subtree_id: subtree_id.into(),
            active_target: String::new(),
            activation_verified: true,
            health_checks_enabled: false,
        })
    }

    fn remove_route(
        &mut self,
        _subtree_id: &str,
    ) -> Result<(), forge_core::runtime::RoutingRuntimeError> {
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

fn cleanup_forge_containers() -> Result<(), String> {
    let output = Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            "label=forge.managed=true",
        ])
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    let ids = String::from_utf8_lossy(&output.stdout);
    for id in ids.lines().filter(|line| !line.trim().is_empty()) {
        let _ = docker(&["rm", "-f", id.trim()]);
    }
    Ok(())
}

fn write_caddy_config(root: &Path) {
    let config = serde_json::json!({
        "admin": { "listen": ":2019" },
        "apps": {
            "http": {
                "servers": {
                    "forge": {
                        "listen": [":8080"],
                        "routes": []
                    }
                }
            }
        }
    });
    std::fs::write(root.join("caddy.json"), serde_json::to_vec_pretty(&config).unwrap()).unwrap();
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
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
