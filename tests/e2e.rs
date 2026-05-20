#[path = "integration/common.rs"]
mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use forge_core::api::{DeploymentRequest, ProjectUpsertRequest};
use forge_core::caddy::CaddyApiRuntime;
use forge_core::config::DaemonConfig;
use forge_core::convergence::{
    ActiveDeploymentDecider, ActiveTruth, ConvergenceEngine, RecoveryOutcome, TickInput,
};
use forge_core::daemon::Daemon;
use forge_core::deployments::{
    ActivationMode, DeploymentError, DeploymentExecutor, ExecutionConfig, ValidationPolicy,
};
use forge_core::docker::{DockerCliRuntime, ProcessCommandRunner};
use forge_core::github::GitHubWebhookConfig;
use forge_core::http::{ControlPlane, HttpState, IdempotencyStore, WebAuthState, router};
use forge_core::probes::DockerNetworkProbeRuntime;
use forge_core::projects::ProjectRegistryStore;
use forge_core::queue::{DeploymentRecord, PersistentQueue};
use forge_core::runtime::{
    BuildImageRequest, ContainerInspection, CreateContainerRequest, DockerRuntime,
    DockerRuntimeError, RouteUpdateRequest, RoutingRuntime,
};
use forge_core::secrets::SecretStore;
use forge_core::storage::{EnvironmentPaths, EventStore, PointerStore};
use hmac::{Hmac, Mac};
use reqwest::StatusCode;
use reqwest::blocking::Client;
use serde_json::Value;
use sha2::Sha256;
use tokio::net::TcpListener;

type HmacSha256 = Hmac<Sha256>;

#[test]
fn dogfood_sample_app_deploys_public_route() {
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
        PointerStore::new(EnvironmentPaths::new(
            &harness.runtime_root,
            "api",
            "production"
        ))
        .read_pointer("current")
        .unwrap(),
        Some(1)
    );
}

#[test]
fn dogfood_events_visible_after_deploy() {
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
fn dogfood_rollback_restores_previous_generation() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("rollback") else {
        return;
    };

    harness.enqueue_deploy_for_fixture(&common::sample_http_app_fixture());
    harness.execute_next_deployment().unwrap();

    harness.enqueue_deploy_for_fixture(&common::sample_http_app_fixture());
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

    harness
        .run_convergence_ticks(&[100, 101, 102, 133])
        .unwrap();

    let route = harness
        .routing
        .inspect_route("forge:api:production")
        .unwrap();
    assert_eq!(
        route.active_target,
        format!(
            "{}:3000",
            docker_container_ip("prod-api-gen-1", &harness.network_name)
        )
    );
    assert!(route.activation_verified);
    assert_eq!(
        PointerStore::new(EnvironmentPaths::new(
            &harness.runtime_root,
            "api",
            "production"
        ))
        .read_pointer("current")
        .unwrap(),
        Some(1)
    );
}

#[test]
fn dogfood_daemon_restart_reconstructs_current_route() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("restart-reconstruct") else {
        return;
    };

    harness.enqueue_deploy_for_fixture(&common::sample_http_app_fixture());
    harness.execute_next_deployment().unwrap();

    let env = EnvironmentPaths::new(&harness.runtime_root, "api", "production");
    std::fs::write(env.current_pointer(), "\n").unwrap();

    harness.restart_api_server(AllowAllDecider(true));
    harness.run_convergence_ticks(&[200]).unwrap();

    let current = PointerStore::new(env).read_pointer("current").unwrap();
    assert_eq!(current, Some(1));
    let route = harness
        .routing
        .inspect_route("forge:api:production")
        .unwrap();
    assert_eq!(
        route.active_target,
        format!(
            "{}:3000",
            docker_container_ip("prod-api-gen-1", &harness.network_name)
        )
    );
}

#[test]
fn dogfood_reboot_recovery_restores_container_and_route() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("reboot-recovery") else {
        return;
    };

    harness.enqueue_deploy_for_fixture(&common::sample_http_app_fixture());
    harness.execute_next_deployment().unwrap();

    docker(&["rm", "-f", "prod-api-gen-1"]).expect("active generation should be removable");
    harness
        .routing
        .remove_route("forge:api:production")
        .expect("managed route should be removable");
    harness.install_ready_placeholder();

    harness.restart_api_server(AllowAllDecider(true));

    assert!(docker_container_exists("prod-api-gen-1"));
    let route = harness
        .routing
        .inspect_route("forge:api:production")
        .unwrap();
    assert_eq!(
        route.active_target,
        format!(
            "{}:3000",
            docker_container_ip("prod-api-gen-1", &harness.network_name)
        )
    );
    let response = harness
        .http_client
        .get(harness.public_url("health"))
        .send()
        .expect("public route should be reachable after startup recovery");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().unwrap(),
        "ok\n",
        "startup recovery must override the forge:ready placeholder"
    );
}

#[test]
fn dogfood_finalized_runtime_persists_http_route_recovery_metadata() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("persist-runtime-route-metadata") else {
        return;
    };

    harness.enqueue_deploy_for_fixture(&common::sample_http_app_fixture());
    harness.execute_next_deployment().unwrap();

    let runtime: Value = serde_json::from_str(
        &fs::read_to_string(harness.generation_dir(1).join("runtime.json"))
            .expect("runtime.json should exist for finalized generation"),
    )
    .expect("runtime.json should be valid json");

    assert_eq!(runtime["network_name"], harness.network_name.as_str());
    assert_eq!(runtime["probe_path"], "/health");
    assert_eq!(runtime["activation"]["Http"]["internal_port"], 3000);
    assert_eq!(
        runtime["activation"]["Http"]["route_subtree_id"],
        "forge:api:production"
    );
    assert_eq!(
        runtime["activation"]["Http"]["target_source"],
        "ContainerIp"
    );
}

#[test]
fn dogfood_restart_during_inflight_deploy_fails_or_recovers_deterministically() {
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
            source_path: Some(common::sample_http_app_fixture()),
            source_ref: None,
            repo_url: None,
            commit_sha: None,
        }))
    );
    assert!(
        PersistentQueue::new(harness.runtime_root.join("queue"))
            .unwrap()
            .load_state()
            .unwrap()
            .active
            .is_none()
    );
}

#[test]
fn dogfood_bad_app_failed_health_does_not_promote_current() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("bad-app-no-promotion") else {
        return;
    };

    harness.enqueue_deploy_for_fixture(&common::bad_http_app_fixture());
    let result = harness.execute_next_deployment_for_fixture(&common::bad_http_app_fixture());

    assert!(matches!(
        result,
        Err(DeploymentError::ValidationFailed(
            "http health probe failed"
        ))
    ));
    let env = EnvironmentPaths::new(&harness.runtime_root, "api", "production");
    assert_eq!(
        PointerStore::new(env.clone())
            .read_pointer("current")
            .unwrap(),
        None
    );
    assert!(
        harness
            .routing
            .inspect_route("forge:api:production")
            .is_err()
    );
    assert!(!env.generation_dir(1).join("snapshot.json").exists());
}

#[test]
fn dogfood_bad_app_failed_generation_is_cleaned() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("bad-app-cleaned") else {
        return;
    };

    harness.enqueue_deploy_for_fixture(&common::bad_http_app_fixture());
    let _ = harness.execute_next_deployment_for_fixture(&common::bad_http_app_fixture());

    assert!(!docker_container_exists("prod-api-gen-1"));
    let cleanup = fs::read_to_string(harness.generation_dir(1).join("cleanup.json"))
        .expect("cleanup record should exist for failed generation");
    assert!(cleanup.contains("\"container_removed\": true"));
    assert!(cleanup.contains("\"tombstoned\": false"));
}

#[test]
fn dogfood_bad_app_diagnostics_are_visible() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("bad-app-diagnostics") else {
        return;
    };

    harness.enqueue_deploy_for_fixture(&common::bad_http_app_fixture());
    let _ = harness.execute_next_deployment_for_fixture(&common::bad_http_app_fixture());

    let diagnostics_dir = harness.generation_dir(1).join("diagnostics");
    let reason = fs::read_to_string(diagnostics_dir.join("failure_reason.log"))
        .expect("failure reason should be persisted");
    assert!(reason.contains("http health probe failed"));

    let summary = fs::read_to_string(diagnostics_dir.join("summary.json"))
        .expect("diagnostic summary should be persisted");
    assert!(summary.contains("\"failure_stage\": \"validation\""));
    assert!(summary.contains("\"failure_reason\": \"http health probe failed\""));
}

#[test]
fn dogfood_crash_during_deploy_recovers_without_orphan_container() {
    let _guard = integration_lock();
    let Some(harness) = E2eHarness::start("crash-during-deploy") else {
        return;
    };

    let deployment_id = "dep-crash-deploy".to_string();
    harness.stage_inflight_generation(&common::sample_http_app_fixture(), &deployment_id, 1, false);

    let mut daemon = Daemon::new(
        harness.config.clone(),
        DockerCliRuntime::new(ProcessCommandRunner),
        CaddyApiRuntime::new(harness.admin_base_url(), harness.public_base_url()),
        AllowAllDecider(false),
    );
    daemon.start().unwrap();

    assert_eq!(
        daemon.last_recovery_outcome(),
        Some(&RecoveryOutcome::Failed(DeploymentRecord {
            deployment_id,
            project_id: "api".into(),
            environment: "production".into(),
            source_path: None,
            source_ref: None,
            repo_url: None,
            commit_sha: None,
        }))
    );
    assert!(!docker_container_exists("prod-api-gen-1"));
}

#[test]
fn dogfood_crash_during_route_activation_recovers_without_orphan_route() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("crash-during-route") else {
        return;
    };

    let deployment_id = "dep-crash-route".to_string();
    harness.stage_inflight_generation(&common::sample_http_app_fixture(), &deployment_id, 1, true);

    let mut daemon = Daemon::new(
        harness.config.clone(),
        DockerCliRuntime::new(ProcessCommandRunner),
        CaddyApiRuntime::new(harness.admin_base_url(), harness.public_base_url()),
        AllowAllDecider(false),
    );
    daemon.start().unwrap();

    assert!(
        harness
            .routing
            .inspect_route("forge:api:production")
            .is_err()
    );
    let cleanup = fs::read_to_string(harness.generation_dir(1).join("cleanup.json"))
        .expect("startup cleanup should be recorded");
    assert!(cleanup.contains("\"route_removed\": true"));
}

#[test]
fn dogfood_github_webhook_push_enqueues_and_deploys() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("github-webhook") else {
        return;
    };

    let repo = harness.create_manifest_repo(
        "main",
        r#"{
  "forge_schema_version": 1,
  "project_id": "api",
  "repository": { "provider": "github" },
  "environments": {
    "development": { "branch": "dev" },
    "staging": { "branch": "release" },
    "production": { "branch": "main" }
  },
  "build": { "dockerfile_path": "./Dockerfile", "context_path": "." },
  "runtime": {
    "service_type": "http",
    "internal_port": 3000,
    "subdomain": "api",
    "resources": { "memory_limit_mb": 512, "cpu_shares": 1024 }
  },
  "health": {
    "tcp_required": true,
    "http": { "enabled": true, "path": "/health", "expected_status": [200], "timeout_ms": 5000 },
    "startup_grace_seconds": 30
  },
  "contract": { "version": 1, "spec": {} },
  "secrets": { "environment_variables": {} }
}"#,
    );
    let commit_sha = git_output(&repo, &["rev-parse", "HEAD"]);
    ProjectRegistryStore::new(&harness.runtime_root)
        .upsert(
            ProjectUpsertRequest {
                project_id: Some("api".into()),
                repo_url: repo.to_str().unwrap().into(),
                default_branch: "main".into(),
                base_domain: Some("api.example.com".into()),
            },
            None,
        )
        .unwrap();

    let response = harness.post_github_webhook(
        "delivery-1",
        "push",
        &format!(
            r#"{{
  "ref": "refs/heads/main",
  "after": "{commit_sha}",
  "repository": {{ "clone_url": "{}" }}
}}"#,
            repo.to_str().unwrap()
        ),
    );
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let json = response.json::<Value>().unwrap();
    assert_eq!(json["data"]["status"], "accepted");

    let deployment_id = json["data"]["deployment_id"].as_str().unwrap().to_string();
    let execution = harness.execute_next_deployment().unwrap();
    assert_eq!(execution.deployment_id, deployment_id);

    let deployment = harness.get_deployment(&deployment_id);
    assert_eq!(deployment["project_id"], "api");
    assert_eq!(deployment["environment"], "production");
}

#[test]
fn runtime_secret_is_injected_into_container() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("secret-injection") else {
        return;
    };

    harness.put_secret("DATABASE_URL", "postgres://alpha-secret-value");
    harness.enqueue_deploy_for_fixture(&common::secret_http_app_fixture());
    harness
        .execute_next_deployment_for_fixture(&common::secret_http_app_fixture())
        .unwrap();

    let response = harness
        .http_client
        .get(harness.public_url("secret-present"))
        .send()
        .expect("secret presence marker should be reachable");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().unwrap();
    assert_eq!(body.trim(), "present");
    assert!(!body.contains("postgres://alpha-secret-value"));
}

#[test]
fn secret_value_is_redacted_from_events() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("secret-redacted-events") else {
        return;
    };

    harness.put_secret("DATABASE_URL", "postgres://alpha-secret-value");
    harness.enqueue_deploy_for_fixture(&common::secret_http_app_fixture());
    harness
        .execute_next_deployment_for_fixture(&common::secret_http_app_fixture())
        .unwrap();

    let events = harness.get_events();
    let runtime_event = events["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["event_type"] == "RUNTIME_ENV_PREPARED")
        .expect("runtime env event should exist");
    let reason = runtime_event["reason"].as_str().unwrap();
    assert!(reason.contains("DATABASE_URL=[REDACTED]"));
    assert!(!reason.contains("postgres://alpha-secret-value"));
}

#[test]
fn secret_value_is_redacted_from_diagnostics() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("secret-redacted-diagnostics") else {
        return;
    };

    harness.put_secret("DATABASE_URL", "postgres://alpha-secret-value");
    harness.enqueue_deploy_for_fixture(&common::secret_http_bad_app_fixture());
    let result =
        harness.execute_next_deployment_for_fixture(&common::secret_http_bad_app_fixture());
    assert!(matches!(
        result,
        Err(DeploymentError::ValidationFailed(
            "http health probe failed"
        ))
    ));

    let summary = fs::read_to_string(harness.generation_dir(1).join("diagnostics/summary.json"))
        .expect("summary should be present");
    assert!(summary.contains("DATABASE_URL=[REDACTED]"));
    assert!(!summary.contains("postgres://alpha-secret-value"));
}

#[test]
fn logs_endpoint_redacts_secret_values() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("logs-redacted") else {
        return;
    };

    harness.put_secret("DATABASE_URL", "postgres://alpha-secret-value");
    let deployment_id = harness.enqueue_deploy_for_fixture(&common::secret_http_bad_app_fixture());
    let result =
        harness.execute_next_deployment_for_fixture(&common::secret_http_bad_app_fixture());
    assert!(matches!(
        result,
        Err(DeploymentError::ValidationFailed(
            "http health probe failed"
        ))
    ));

    let logs = harness.get_logs(&deployment_id);
    let rendered = logs["lines"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|line| line.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("DATABASE_URL=[REDACTED]"));
    assert!(!rendered.contains("postgres://alpha-secret-value"));
}

#[test]
fn failed_deploy_logs_preserve_diagnostic_context() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("logs-context") else {
        return;
    };

    let deployment_id = harness.enqueue_deploy_for_fixture(&common::bad_http_app_fixture());
    let result = harness.execute_next_deployment_for_fixture(&common::bad_http_app_fixture());
    assert!(matches!(
        result,
        Err(DeploymentError::ValidationFailed(
            "http health probe failed"
        ))
    ));

    let logs = harness.get_logs(&deployment_id);
    let lines = logs["lines"].as_array().unwrap();
    let rendered = lines
        .iter()
        .filter_map(|line| line.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("deployment started"));
    assert!(rendered.contains("container started"));
    assert!(rendered.contains("http health probe failed"));
}

#[test]
fn missing_required_secret_fails_before_container_start() {
    let _guard = integration_lock();
    let Some(mut harness) = E2eHarness::start("missing-required-secret") else {
        return;
    };

    harness.enqueue_deploy_for_fixture(&common::secret_http_app_fixture());
    let result = harness.execute_next_deployment_for_fixture(&common::secret_http_app_fixture());
    assert!(matches!(result, Err(DeploymentError::MissingSecret(_))));
    assert!(!docker_container_exists("prod-api-gen-1"));
    let reason = fs::read_to_string(
        harness
            .generation_dir(1)
            .join("diagnostics/failure_reason.log"),
    )
    .expect("failure reason should be present");
    assert!(reason.contains("missing required secret DATABASE_URL"));
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
        if !common::ensure_integration_enabled() {
            return None;
        }
        common::require_docker_available();
        ensure_test_master_key();
        cleanup_forge_containers().expect("forge containers should be cleaned between e2e runs");
        cleanup_forge_images().expect("forge images should be cleaned between e2e runs");
        cleanup_e2e_caddy_containers()
            .expect("e2e caddy containers should be cleaned between e2e runs");

        let runtime_root = common::runtime_root("e2e");
        let suffix = unique_suffix();
        let network_name = format!("forge-e2e-net-{test_name}-{suffix}");
        let caddy_container_name = format!("forge-e2e-caddy-{test_name}-{suffix}");
        let admin_port = common::available_port();
        let public_port = common::available_port();
        let api_port = common::available_port();
        let token = "test-token".to_string();

        docker(&["network", "create", &network_name]).expect("docker network should be creatable");
        write_caddy_config(&runtime_root);
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
                runtime_root.join("caddy.json").display()
            ),
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
            github_webhook_secret: Some("github-test-secret".into()),
            repository_cache_root: Some(runtime_root.join("repo-cache")),
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

        let mut daemon = Daemon::new(
            self.config.clone(),
            DockerCliRuntime::new(ProcessCommandRunner),
            CaddyApiRuntime::new(self.admin_base_url(), self.public_base_url()),
            decider,
        );
        daemon.start().unwrap();

        let state = HttpState::new(
            Arc::new(Mutex::new(Box::new(daemon) as Box<dyn ControlPlane>)),
            self.token.clone(),
            IdempotencyStore::new(self.runtime_root.join("idempotency")).unwrap(),
            self.github_webhook_state(),
            SecretStore::new(self.runtime_root.join("secrets")).unwrap(),
            ProjectRegistryStore::new(&self.runtime_root),
            WebAuthState::from_env(),
            None,
        );
        let app = router(state);
        self.api_threads.push(spawn_http_server(self.api_port, app));
        self.wait_for_api_ready();
    }

    fn enqueue_deploy(&self) -> String {
        self.enqueue_deploy_for_fixture(&common::sample_http_app_fixture())
    }

    fn enqueue_deploy_for_fixture(&self, fixture: &Path) -> String {
        let response = self
            .http_client
            .post(self.api_url("deployments"))
            .bearer_auth(&self.token)
            .json(&DeploymentRequest {
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: Some(fixture.to_path_buf()),
                source_ref: None,
            })
            .send()
            .expect("deploy request should reach api");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let json = response.json::<Value>().unwrap();
        json["data"]["deployment_id"].as_str().unwrap().to_string()
    }

    fn execute_next_deployment(
        &mut self,
    ) -> Result<
        forge_core::deployments::DeploymentExecution,
        forge_core::deployments::DeploymentError,
    > {
        self.execute_next_deployment_for_fixture(&common::sample_http_app_fixture())
    }

    fn execute_next_deployment_for_fixture(
        &mut self,
        fixture: &Path,
    ) -> Result<
        forge_core::deployments::DeploymentExecution,
        forge_core::deployments::DeploymentError,
    > {
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
                activation: ActivationMode::Http {
                    internal_port: 3000,
                },
            },
        )
        .with_execution_config(ExecutionConfig {
            context_path: fixture.to_path_buf(),
            dockerfile_path: fixture.join("Dockerfile"),
            network_name: Some(self.network_name.clone()),
        })
        .execute_next()
        .map(|value| value.expect("queued deployment should execute"))
    }

    fn generation_dir(&self, generation: u64) -> PathBuf {
        EnvironmentPaths::new(&self.runtime_root, "api", "production").generation_dir(generation)
    }

    fn github_webhook_state(&self) -> Option<forge_core::http::GitHubWebhookState> {
        Some(forge_core::http::GitHubWebhookState::new(
            GitHubWebhookConfig {
                secret: self
                    .config
                    .github_webhook_secret
                    .clone()
                    .expect("github webhook secret should be configured"),
                repository_cache_root: self
                    .config
                    .repository_cache_root
                    .clone()
                    .expect("repository cache root should be configured"),
            },
            forge_core::http::DeliveryStore::new(self.runtime_root.join("github-deliveries"))
                .unwrap(),
        ))
    }

    fn create_manifest_repo(&self, branch: &str, manifest: &str) -> PathBuf {
        let repo = self.runtime_root.join("webhook-repo");
        fs::create_dir_all(&repo).unwrap();
        git_in(&self.runtime_root, &["init", repo.to_str().unwrap()]);
        git_in(
            &self.runtime_root,
            &["-C", repo.to_str().unwrap(), "checkout", "-b", branch],
        );
        fs::write(repo.join("forge.project.json"), manifest).unwrap();
        fs::copy(
            common::sample_http_app_fixture().join("Dockerfile"),
            repo.join("Dockerfile"),
        )
        .unwrap();
        git_in(
            &self.runtime_root,
            &[
                "-C",
                repo.to_str().unwrap(),
                "add",
                "forge.project.json",
                "Dockerfile",
            ],
        );
        git_in(
            &self.runtime_root,
            &[
                "-C",
                repo.to_str().unwrap(),
                "-c",
                "user.name=Forge Test",
                "-c",
                "user.email=forge@example.com",
                "commit",
                "-m",
                "manifest",
            ],
        );
        repo
    }

    fn post_github_webhook(
        &self,
        delivery_id: &str,
        event: &str,
        body: &str,
    ) -> reqwest::blocking::Response {
        self.http_client
            .post(self.api_url("webhooks/github"))
            .header("x-github-delivery", delivery_id)
            .header("x-github-event", event)
            .header(
                "x-hub-signature-256",
                github_signature("github-test-secret", body.as_bytes()),
            )
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .expect("github webhook request should reach api")
    }

    fn put_secret(&self, key: &str, value: &str) {
        let response = self
            .http_client
            .post(self.api_url("secrets"))
            .bearer_auth(&self.token)
            .json(&serde_json::json!({
                "project_id": "api",
                "environment": "production",
                "key": key,
                "value": value,
            }))
            .send()
            .expect("secret write should reach api");
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    fn stage_inflight_generation(
        &self,
        fixture: &Path,
        deployment_id: &str,
        generation: u64,
        attach_route: bool,
    ) {
        let record = DeploymentRecord {
            deployment_id: deployment_id.into(),
            project_id: "api".into(),
            environment: "production".into(),
            source_path: None,
            source_ref: None,
            repo_url: None,
            commit_sha: None,
        };
        let env = EnvironmentPaths::new(&self.runtime_root, "api", "production");
        env.ensure_exists().unwrap();
        fs::create_dir_all(env.generation_dir(generation).join("diagnostics")).unwrap();
        fs::write(
            env.generation_dir(generation).join("build.json"),
            format!(
                "{{\n  \"deployment_id\": \"{}\",\n  \"image_ref\": \"forge/api:gen-{}\"\n}}\n",
                deployment_id, generation
            ),
        )
        .unwrap();
        fs::write(
            env.generation_dir(generation).join("runtime.json"),
            format!(
                "{{\n  \"container_name\": \"prod-api-gen-{}\",\n  \"running\": true\n}}\n",
                generation
            ),
        )
        .unwrap();

        let mut docker = DockerCliRuntime::new(ProcessCommandRunner);
        let image_ref = docker
            .build_image(BuildImageRequest {
                image_tag: format!("forge/e2e-staged:{}-{generation}", deployment_id),
                context_path: fixture.to_path_buf(),
                dockerfile_path: fixture.join("Dockerfile"),
                labels: forge_labels(&record, generation),
            })
            .unwrap();
        docker
            .create_container(CreateContainerRequest {
                container_name: format!("prod-api-gen-{generation}"),
                image_ref,
                labels: forge_labels(&record, generation),
                environment: Default::default(),
                network_name: Some(self.network_name.clone()),
            })
            .unwrap();
        docker
            .start_container(&format!("prod-api-gen-{generation}"))
            .unwrap();

        if attach_route {
            let mut routing = CaddyApiRuntime::new(self.admin_base_url(), self.public_base_url());
            routing
                .update_route(RouteUpdateRequest {
                    subtree_id: "forge:api:production".into(),
                    target: format!("prod-api-gen-{generation}:3000"),
                    domain: None,
                    health_checks_enabled: false,
                    probe_path: Some("/health".into()),
                })
                .unwrap();
        }

        let queue = PersistentQueue::new(self.runtime_root.join("queue")).unwrap();
        queue.enqueue(record).unwrap();
        let active = queue.start_next().unwrap().unwrap();
        assert_eq!(active.deployment_id, deployment_id);
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

    fn get_logs(&self, deployment_id: &str) -> Value {
        let response = self
            .http_client
            .get(self.api_url(&format!("logs/{deployment_id}")))
            .bearer_auth(&self.token)
            .send()
            .expect("deployment logs request should reach api");
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
                truth: ActiveTruth::HttpRouted {
                    internal_port: 3000,
                },
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
        format!(
            "{}/{}",
            self.public_base_url(),
            path.trim_start_matches('/')
        )
    }

    fn api_url(&self, path: &str) -> String {
        format!(
            "http://127.0.0.1:{}/{}",
            self.api_port,
            path.trim_start_matches('/')
        )
    }

    fn wait_for_caddy(&self) {
        for _ in 0..40 {
            if let Ok(response) = self
                .http_client
                .get(format!(
                    "{}/config/apps/http/servers/forge/routes",
                    self.admin_base_url()
                ))
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

    fn install_ready_placeholder(&self) {
        let mut config = self
            .http_client
            .get(format!("{}/config/", self.admin_base_url()))
            .send()
            .expect("caddy config inspection should succeed")
            .json::<Value>()
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

        let response = self
            .http_client
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

    fn ensure_network(&mut self, _network_name: &str) -> Result<(), DockerRuntimeError> {
        Ok(())
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
            state_status: "running".into(),
            exit_code: Some(0),
            image_ref: "noop".into(),
            labels: Default::default(),
            network_ips: Default::default(),
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
        Ok(Vec::new())
    }

    fn list_managed_images(
        &mut self,
    ) -> Result<Vec<forge_core::runtime::ManagedImage>, DockerRuntimeError> {
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
    ) -> Result<forge_core::runtime::RouteInspection, forge_core::runtime::RoutingRuntimeError>
    {
        Ok(forge_core::runtime::RouteInspection {
            subtree_id: subtree_id.into(),
            active_target: String::new(),
            domain: None,
            activation_verified: true,
            verification_url: None,
            verification_host: None,
            verification_status_code: None,
            verification_response_body: None,
            health_checks_enabled: false,
        })
    }

    fn list_managed_routes(
        &mut self,
    ) -> Result<Vec<forge_core::runtime::RouteInspection>, forge_core::runtime::RoutingRuntimeError>
    {
        Ok(Vec::new())
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

fn docker_container_ip(name: &str, network_name: &str) -> String {
    let output = Command::new("docker")
        .args(["inspect", name])
        .output()
        .expect("docker inspect should return container IP");
    assert!(
        output.status.success(),
        "docker inspect failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let inspections: Vec<Value> =
        serde_json::from_slice(&output.stdout).expect("docker inspect should return json");
    inspections[0]["NetworkSettings"]["Networks"][network_name]["IPAddress"]
        .as_str()
        .expect("container should have an IP on the test network")
        .to_string()
}

fn cleanup_forge_containers() -> Result<(), String> {
    let output = Command::new("docker")
        .args(["ps", "-aq", "--filter", "label=forge.managed=true"])
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

fn cleanup_forge_images() -> Result<(), String> {
    let output = Command::new("docker")
        .args(["images", "-q", "forge/api"])
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    let ids = String::from_utf8_lossy(&output.stdout);
    for id in ids.lines().filter(|line| !line.trim().is_empty()) {
        let _ = docker(&["rmi", "-f", id.trim()]);
    }
    Ok(())
}

fn cleanup_e2e_caddy_containers() -> Result<(), String> {
    let output = Command::new("docker")
        .args(["ps", "-aq", "--filter", "name=forge-e2e-caddy-"])
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
                        "automatic_https": {
                            "disable": true
                        },
                        "routes": []
                    }
                }
            }
        }
    });
    std::fs::write(
        root.join("caddy.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
}

fn docker_container_exists(name: &str) -> bool {
    Command::new("docker")
        .args(["inspect", name])
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn forge_labels(record: &DeploymentRecord, generation: u64) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("forge.managed".into(), "true".into()),
        ("forge.project_id".into(), record.project_id.clone()),
        ("forge.environment".into(), record.environment.clone()),
        ("forge.generation".into(), generation.to_string()),
        ("forge.deployment_id".into(), record.deployment_id.clone()),
    ])
}

fn github_signature(secret: &str, body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

fn ensure_test_master_key() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| unsafe {
        std::env::set_var(
            "FORGE_MASTER_KEY",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
        );
    });
}

fn git_in(cwd: &Path, args: &[&str]) {
    let output = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

fn git_output(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
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
