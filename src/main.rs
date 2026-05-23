use std::env;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io::ErrorKind;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime};

use forge_core::api::{
    BackupListResponse, BackupRecord, BackupRestoreResponse, CliLoginPollRequest,
    CliLoginPollResponse, CliLoginStartResponse, ClusterDiagnostics, DeploymentAccepted,
    DeploymentHistoryResponse, DeploymentLogs, DeploymentRequest, DeploymentStatus,
    EnvironmentDiagnostics, EnvironmentDiffResponse, EnvironmentVariableReport, ErrorResponse,
    EventList, MetricsResponse, ProjectList, ProjectRecord, ProjectUpsertRequest, ReadyzResponse,
    RestoreLineage, RetentionRole, SecretListResponse, SecretUnsetResponse, ServiceRuntimeStatus,
};
use forge_core::caddy::CaddyApiRuntime;
use forge_core::config::DaemonConfig;
use forge_core::convergence::ActiveDeploymentDecider;
use forge_core::convergence::garbage_collect;
use forge_core::daemon::{
    Daemon, DeploymentWorkerSettings, WorkerLeadership, run_deployment_worker_loop,
    run_heartbeat_loop, run_readyz_refresh_loop,
};
use forge_core::deployments::{
    ActivationMode, ExecutionConfig, FORGE_MANAGED_DOCKER_NETWORK, ValidationPolicy,
};
use forge_core::docker::{DockerCliRuntime, ProcessCommandRunner};
use forge_core::doctor::{DoctorOptions, run as run_doctor};
use forge_core::events::EventRecord;
use forge_core::github::GitHubWebhookConfig;
use forge_core::http::{
    ControlPlane, DeliveryStore, GitHubWebhookState, HttpState, IdempotencyStore, WebAuthState,
    router,
};
use forge_core::probes::DockerNetworkProbeRuntime;
use forge_core::projects::ProjectRegistryStore;
use forge_core::queue::PersistentQueue;
use forge_core::reconciliation::{
    ReconciliationIntentStatus, ReconciliationStore, ReplayOptions, intent_request_for_storage_root,
};
use forge_core::secrets::{SecretWriteRequest, SecretWriteResult};
use forge_core::status::ProjectEnvironmentStatus;
use forge_core::storage::{
    ClusterTopologyStore, LeaderLeaseStore, NodeMetadataStore, PersistedLeaderLease,
};
use reqwest::StatusCode;
use reqwest::blocking::{Client, RequestBuilder};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), CliError> {
    run_with_args(env::args().skip(1).collect(), run_daemon)
}

fn run_with_args<F>(args: Vec<String>, daemon_runner: F) -> Result<(), CliError>
where
    F: FnOnce(DaemonCommand) -> Result<(), CliError>,
{
    let parsed = ParsedArgs::parse(args)?;
    let control_plane_remote_client = parsed.remote_client_if_logged_in()?;
    let config_path_explicit = parsed.config_path_explicit;
    let api_credentials = if matches!(
        parsed.command,
        Command::Doctor { .. }
            | Command::Daemon(_)
            | Command::Bench { .. }
            | Command::Gc { .. }
            | Command::Init { .. }
            | Command::Login { .. }
            | Command::Logout
            | Command::WhoAmI
            | Command::ControlPlaneLeader { .. }
            | Command::ControlPlaneLease { .. }
            | Command::ControlPlaneReplayStatus { .. }
            | Command::ControlPlaneIntents { .. }
            | Command::ControlPlaneReplay { .. }
    ) {
        None
    } else {
        Some((parsed.base_url()?, parsed.token()?))
    };

    match parsed.command {
        Command::Doctor {
            config_path,
            caddy_admin_url,
            metrics_url,
        } => {
            let report = run_doctor(&DoctorOptions {
                config_path,
                caddy_admin_url,
                metrics_url,
            })
            .map_err(|err| CliError::Usage(err.to_string()))?;
            print!("{}", report.render());
            if report.has_errors() {
                return Err(CliError::Usage("doctor found failing checks".into()));
            }
        }
        Command::Daemon(command) => daemon_runner(command)?,
        Command::Init { force } => init_project_config(force)?,
        Command::Login { server_url } => run_login(server_url)?,
        Command::Logout => run_logout()?,
        Command::WhoAmI => run_whoami(&parsed)?,
        Command::ControlPlaneLeader { config_path } => run_control_plane_leader(
            control_plane_remote_client,
            config_path,
            config_path_explicit,
        )?,
        Command::ControlPlaneLease { config_path } => run_control_plane_lease(
            control_plane_remote_client,
            config_path,
            config_path_explicit,
        )?,
        Command::ControlPlaneReplayStatus { config_path } => {
            run_control_plane_replay_status(config_path)?
        }
        Command::ControlPlaneIntents { config_path } => run_control_plane_intents(config_path)?,
        Command::ControlPlaneReplay {
            config_path,
            caddy_admin_url,
            caddy_public_url,
            dry_run,
            resume,
        } => run_control_plane_replay(
            config_path,
            caddy_admin_url,
            caddy_public_url,
            dry_run,
            resume,
        )?,
        Command::Deploy {
            project_id,
            environment,
            source_path,
            source_ref,
        } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let accepted = client.post_deployment(DeploymentRequest {
                project_id,
                environment,
                intent: "deploy".into(),
                source_path,
                source_ref,
            })?;
            print_json(&accepted)?;
        }
        Command::Status { deployment_id } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let status = client.get_status(&deployment_id)?;
            print_json(&status)?;
        }
        Command::Logs {
            deployment_id,
            service,
            json,
        } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let logs = client.get_logs(&deployment_id, service.as_deref())?;
            if json {
                print_json(&logs)?;
            } else {
                print!("{}", render_deployment_logs(&logs));
            }
        }
        Command::ProjectStatus {
            project_id,
            environment,
            json,
        } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let status = client.get_project_environment_status(&project_id, &environment)?;
            if json {
                print_json(&status)?;
            } else {
                print!("{}", render_project_environment_status(&status));
            }
        }
        Command::Bench {
            ref target,
            samples,
        } => run_bench(&parsed, target, samples)?,
        Command::Diagnose {
            project_id,
            environment,
            json,
        } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let diagnostics =
                client.get_project_environment_diagnostics(&project_id, &environment)?;
            if json {
                print_json(&diagnostics)?;
            } else {
                print!("{}", render_environment_diagnostics(&diagnostics));
            }
        }
        Command::History {
            project_id,
            environment,
            json,
        } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let history = client.get_project_environment_history(&project_id, &environment)?;
            if json {
                print_json(&history)?;
            } else {
                print!("{}", render_deployment_history(&history));
            }
        }
        Command::Env {
            project_id,
            environment,
            json,
        } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let report = client.get_project_environment_env(&project_id, &environment)?;
            if json {
                print_json(&report)?;
            } else {
                print!("{}", render_environment_variables(&report));
            }
        }
        Command::EnvDiff {
            project_id,
            environment,
            from_generation,
            to_generation,
            json,
        } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let report = client.get_project_environment_env_diff(
                &project_id,
                &environment,
                from_generation,
                to_generation,
            )?;
            if json {
                print_json(&report)?;
            } else {
                print!("{}", render_environment_diff(&report));
            }
        }
        Command::Events => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let events = client.get_events()?;
            print_json(&events.events)?;
        }
        Command::Gc {
            config_path,
            caddy_admin_url,
            caddy_public_url,
            dry_run,
            json,
        } => run_gc_command(
            config_path,
            caddy_admin_url,
            caddy_public_url,
            dry_run,
            json,
        )?,
        Command::Rollback {
            project_id,
            environment,
        } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let accepted = client.post_deployment(DeploymentRequest {
                project_id,
                environment,
                intent: "rollback".into(),
                source_path: None,
                source_ref: None,
            })?;
            print_json(&accepted)?;
        }
        Command::BackupCreate {
            project_id,
            environment,
            json,
        } => {
            let (base_url, token) = api_credentials.unwrap();
            let client = ForgeClient::new(base_url, token);
            let backup = client.create_backup(&project_id, &environment)?;
            if json {
                print_json(&backup)?;
            } else {
                print!("{}", render_backup_record(&backup));
            }
        }
        Command::BackupList {
            project_id,
            environment,
            json,
        } => {
            let (base_url, token) = api_credentials.unwrap();
            let client = ForgeClient::new(base_url, token);
            let backups = client.list_backups(&project_id, &environment)?;
            if json {
                print_json(&backups)?;
            } else {
                print!("{}", render_backup_list(&backups));
            }
        }
        Command::BackupInspect { backup_id, json } => {
            let (base_url, token) = api_credentials.unwrap();
            let client = ForgeClient::new(base_url, token);
            let backup = client.inspect_backup(&backup_id)?;
            if json {
                print_json(&backup)?;
            } else {
                print!("{}", render_backup_record(&backup));
            }
        }
        Command::BackupRestore { backup_id, json } => {
            let (base_url, token) = api_credentials.unwrap();
            let client = ForgeClient::new(base_url, token);
            let restore = client.restore_backup(&backup_id)?;
            if json {
                print_json(&restore)?;
            } else {
                print!("{}", render_backup_restore(&restore));
            }
        }
        Command::ProjectAdd {
            project_id,
            repo_url,
            default_branch,
            base_domain,
        } => {
            let (base_url, token) = api_credentials.unwrap();
            let client = ForgeClient::new(base_url, token);
            let project = client.post_project(ProjectUpsertRequest {
                project_id,
                repo_url,
                default_branch,
                base_domain,
            })?;
            print_json(&project)?;
        }
        Command::ProjectList => {
            let (base_url, token) = api_credentials.unwrap();
            let client = ForgeClient::new(base_url, token);
            let projects = client.get_projects()?;
            print_json(&projects.projects)?;
        }
        Command::ProjectShow { project_id } => {
            let (base_url, token) = api_credentials.unwrap();
            let client = ForgeClient::new(base_url, token);
            let project = client.get_project(&project_id)?;
            print_json(&project)?;
        }
        Command::SecretsSet {
            project_id,
            environment,
            key,
            value,
        } => {
            let (base_url, token) = api_credentials.unwrap();
            let client = ForgeClient::new(base_url, token);
            let result = client.post_secret(SecretWriteRequest {
                project_id,
                environment,
                key,
                value,
            })?;
            print_json(&result)?;
        }
        Command::SecretsList {
            project_id,
            environment,
            json,
        } => {
            let (base_url, token) = api_credentials.unwrap();
            let client = ForgeClient::new(base_url, token);
            let result = client.get_secrets(&project_id, &environment)?;
            if json {
                print_json(&result)?;
            } else {
                print!("{}", render_secret_list(&result));
            }
        }
        Command::SecretsUnset {
            project_id,
            environment,
            key,
        } => {
            let (base_url, token) = api_credentials.unwrap();
            let client = ForgeClient::new(base_url, token);
            let result = client.delete_secret(&project_id, &environment, &key)?;
            print_json(&result)?;
        }
    }

    Ok(())
}

fn print_json<T: Serialize>(value: &T) -> Result<(), CliError> {
    let rendered =
        serde_json::to_string_pretty(value).map_err(|err| CliError::Usage(err.to_string()))?;
    println!("{rendered}");
    Ok(())
}

struct ForgeClient {
    http: Client,
    base_url: String,
    token: String,
}

impl ForgeClient {
    fn new(base_url: String, token: String) -> Self {
        Self {
            http: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            token,
        }
    }

    fn post_deployment(&self, request: DeploymentRequest) -> Result<DeploymentAccepted, CliError> {
        self.send_json(
            self.http
                .post(format!("{}/deployments", self.base_url))
                .json(&request),
        )
    }

    fn get_status(&self, deployment_id: &str) -> Result<DeploymentStatus, CliError> {
        self.send_json(
            self.http
                .get(format!("{}/deployments/{}", self.base_url, deployment_id)),
        )
    }

    fn get_events(&self) -> Result<EventList, CliError> {
        self.send_json(self.http.get(format!("{}/events", self.base_url)))
    }

    fn get_logs(
        &self,
        deployment_id: &str,
        service: Option<&str>,
    ) -> Result<DeploymentLogs, CliError> {
        let mut url = format!("{}/api/deployments/{deployment_id}/logs", self.base_url);
        if let Some(service) = service {
            url.push_str("?service=");
            url.push_str(service);
        }
        self.send_json(self.http.get(url))
    }

    fn get_project_environment_status(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<ProjectEnvironmentStatus, CliError> {
        self.send_json(self.http.get(format!(
            "{}/api/projects/{project_id}/environments/{environment}/status",
            self.base_url
        )))
    }

    fn get_project_environment_diagnostics(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<EnvironmentDiagnostics, CliError> {
        self.send_json(self.http.get(format!(
            "{}/api/projects/{project_id}/environments/{environment}/diagnostics",
            self.base_url
        )))
    }

    fn get_project_environment_history(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<DeploymentHistoryResponse, CliError> {
        self.send_json(self.http.get(format!(
            "{}/api/projects/{project_id}/environments/{environment}/history",
            self.base_url
        )))
    }

    fn get_project_environment_env(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<EnvironmentVariableReport, CliError> {
        self.send_json(self.http.get(format!(
            "{}/api/projects/{project_id}/environments/{environment}/env",
            self.base_url
        )))
    }

    fn get_project_environment_env_diff(
        &self,
        project_id: &str,
        environment: &str,
        from_generation: u64,
        to_generation: u64,
    ) -> Result<EnvironmentDiffResponse, CliError> {
        self.send_json(self.http.get(format!(
            "{}/api/projects/{project_id}/environments/{environment}/env/diff?generation={from_generation}&generation={to_generation}",
            self.base_url
        )))
    }

    fn create_backup(&self, project_id: &str, environment: &str) -> Result<BackupRecord, CliError> {
        self.send_json(self.http.post(format!(
            "{}/api/projects/{project_id}/environments/{environment}/backups",
            self.base_url
        )))
    }

    fn list_backups(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<BackupListResponse, CliError> {
        self.send_json(self.http.get(format!(
            "{}/api/projects/{project_id}/environments/{environment}/backups",
            self.base_url
        )))
    }

    fn inspect_backup(&self, backup_id: &str) -> Result<BackupRecord, CliError> {
        self.send_json(
            self.http
                .get(format!("{}/api/backups/{backup_id}", self.base_url)),
        )
    }

    fn restore_backup(&self, backup_id: &str) -> Result<BackupRestoreResponse, CliError> {
        self.send_json(
            self.http
                .post(format!("{}/api/backups/{backup_id}/restore", self.base_url)),
        )
    }

    fn post_secret(&self, request: SecretWriteRequest) -> Result<SecretWriteResult, CliError> {
        self.send_json(
            self.http
                .post(format!("{}/secrets", self.base_url))
                .json(&request),
        )
    }

    fn get_secrets(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<SecretListResponse, CliError> {
        self.send_json(self.http.get(format!(
            "{}/api/projects/{project_id}/environments/{environment}/secrets",
            self.base_url
        )))
    }

    fn delete_secret(
        &self,
        project_id: &str,
        environment: &str,
        key: &str,
    ) -> Result<SecretUnsetResponse, CliError> {
        self.send_json(self.http.delete(format!(
            "{}/api/projects/{project_id}/environments/{environment}/secrets/{key}",
            self.base_url
        )))
    }

    fn post_project(&self, request: ProjectUpsertRequest) -> Result<ProjectRecord, CliError> {
        self.send_json(
            self.http
                .post(format!("{}/api/projects", self.base_url))
                .json(&request),
        )
    }

    fn get_projects(&self) -> Result<ProjectList, CliError> {
        self.send_json(self.http.get(format!("{}/api/projects", self.base_url)))
    }

    fn get_project(&self, project_id: &str) -> Result<ProjectRecord, CliError> {
        self.send_json(
            self.http
                .get(format!("{}/api/projects/{}", self.base_url, project_id)),
        )
    }

    fn post_cli_login_start(&self) -> Result<CliLoginStartResponse, CliError> {
        self.send_json_without_auth(
            self.http
                .post(format!("{}/api/cli-login/start", self.base_url)),
        )
    }

    fn post_cli_login_poll(
        &self,
        request: CliLoginPollRequest,
    ) -> Result<CliLoginPollResponse, CliError> {
        self.send_json_without_auth(
            self.http
                .post(format!("{}/api/cli-login/poll", self.base_url))
                .json(&request),
        )
    }

    fn check_auth(&self) -> Result<bool, CliError> {
        let response = self
            .http
            .get(format!("{}/events", self.base_url))
            .bearer_auth(&self.token)
            .send()
            .map_err(|err| CliError::Http(err.to_string()))?;
        Ok(response.status().is_success())
    }

    fn get_metrics(&self) -> Result<MetricsResponse, CliError> {
        let request = self.http.get(format!("{}/metrics", self.base_url));
        let request = if self.token.is_empty() {
            request
        } else {
            request.bearer_auth(&self.token)
        };
        decode_raw_json(
            request
                .send()
                .map_err(|err| CliError::Http(err.to_string()))?,
        )
    }

    fn send_json<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T, CliError> {
        self.decode_response(
            request
                .bearer_auth(&self.token)
                .send()
                .map_err(|err| CliError::Http(err.to_string()))?,
        )
    }

    fn send_json_without_auth<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
    ) -> Result<T, CliError> {
        self.decode_response(
            request
                .send()
                .map_err(|err| CliError::Http(err.to_string()))?,
        )
    }

    fn decode_response<T: DeserializeOwned>(
        &self,
        response: reqwest::blocking::Response,
    ) -> Result<T, CliError> {
        let status = response.status();
        let body = response
            .bytes()
            .map_err(|err| CliError::Http(err.to_string()))?;
        let body_text = String::from_utf8_lossy(&body).into_owned();
        if status.is_success() {
            let envelope = serde_json::from_slice::<SuccessEnvelope<T>>(&body).map_err(|err| {
                CliError::Http(format!(
                    "error decoding response body: {err}; status: {}; body: {}",
                    status.as_u16(),
                    summarize_response_body(&body_text)
                ))
            })?;
            Ok(envelope.data)
        } else {
            let envelope = serde_json::from_slice::<ErrorEnvelope>(&body).map_err(|err| {
                CliError::Http(format!(
                    "error decoding error response body: {err}; status: {}; body: {}",
                    status.as_u16(),
                    summarize_response_body(&body_text)
                ))
            })?;
            Err(CliError::Api(
                status,
                ErrorResponse {
                    code: envelope.code,
                    message: envelope.message,
                },
            ))
        }
    }
}

fn run_bench(parsed: &ParsedArgs, target: &str, samples: usize) -> Result<(), CliError> {
    let base_url = parsed
        .resolved_server_url()?
        .ok_or_else(|| CliError::Usage("missing Forge URL: use --url or FORGE_URL".into()))?;
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|err| CliError::Http(err.to_string()))?;
    let path = match target {
        "readyz" => "/readyz",
        "leader" | "convergence" | "diagnostics" | "snapshots" => "/metrics",
        _ => {
            return Err(CliError::Usage(
                "bench target must be readyz, leader, convergence, diagnostics, or snapshots"
                    .into(),
            ));
        }
    };
    let url = format!("{}{}", base_url.trim_end_matches('/'), path);
    let throughput_started = std::time::Instant::now();
    let mut latencies_ms = Vec::with_capacity(samples);
    let mut last_readyz = None;
    let mut last_metrics = None;

    for _ in 0..samples {
        let started = std::time::Instant::now();
        let response = client
            .get(&url)
            .send()
            .map_err(|err| CliError::Http(err.to_string()))?;
        let latency_ms = started.elapsed().as_secs_f64() * 1_000.0;
        latencies_ms.push(latency_ms);
        match target {
            "readyz" => {
                last_readyz = Some(decode_raw_json::<ReadyzResponse>(response)?);
            }
            "leader" | "convergence" | "diagnostics" | "snapshots" => {
                last_metrics = Some(decode_raw_json::<MetricsResponse>(response)?);
            }
            _ => {}
        }
    }

    let elapsed_secs = throughput_started.elapsed().as_secs_f64();
    let throughput = if elapsed_secs > 0.0 {
        samples as f64 / elapsed_secs
    } else {
        samples as f64
    };
    latencies_ms.sort_by(|left, right| left.total_cmp(right));
    let min = *latencies_ms.first().unwrap_or(&0.0);
    let p50 = percentile(&latencies_ms, 0.50);
    let p95 = percentile(&latencies_ms, 0.95);
    let max = *latencies_ms.last().unwrap_or(&0.0);

    println!("target: {target}");
    println!("samples: {samples}");
    println!("latency_ms: min={min:.2} p50={p50:.2} p95={p95:.2} max={max:.2}");
    println!("throughput_rps: {:.2}", throughput);
    match target {
        "readyz" => {
            let readyz = last_readyz.expect("readyz benchmark should decode response");
            let metrics = decode_raw_json::<MetricsResponse>(
                client
                    .get(format!("{}/metrics", base_url.trim_end_matches('/')))
                    .send()
                    .map_err(|err| CliError::Http(err.to_string()))?,
            )?;
            println!("status: {}", readyz.status);
            println!("cache_age_ms: {}", metrics.readiness_cache_age_ms);
            println!("lock_wait_ms: n/a (cache-backed endpoint)");
            println!(
                "convergence_duration_ms: {}",
                metrics.convergence_loop_duration_ms
            );
        }
        "leader" | "convergence" | "diagnostics" | "snapshots" => {
            let metrics = last_metrics.expect("metrics benchmark should decode response");
            println!("cache_age_ms: {}", metrics.readiness_cache_age_ms);
            println!("lock_wait_ms: n/a (cache-backed endpoint)");
            println!(
                "convergence_duration_ms: {}",
                metrics.convergence_loop_duration_ms
            );
            println!("leader: {}", metrics.leader);
            println!("convergence_owner: {}", metrics.convergence_owner);
            println!("lease_epoch: {}", metrics.lease_epoch);
            println!("lease_age_ms: {}", metrics.lease_age_ms);
            println!("lease_expiry_ms: {}", metrics.lease_expiry_ms);
            println!("reconciliation_enabled: {}", metrics.reconciliation_enabled);
            println!("follower_mode: {}", metrics.follower_mode);
            println!(
                "docker_probe_latency_ms: {}",
                metrics.docker_probe_latency_ms
            );
            println!("caddy_probe_latency_ms: {}", metrics.caddy_probe_latency_ms);
            if target == "diagnostics" {
                println!("convergence_domains: {}", metrics.convergence_domains.len());
            }
            if target == "snapshots" {
                println!("snapshot_read_latency_ms: {}", metrics.readyz_latency_ms);
            }
        }
        _ => {}
    }
    Ok(())
}

fn decode_raw_json<T: DeserializeOwned>(
    response: reqwest::blocking::Response,
) -> Result<T, CliError> {
    let status = response.status();
    let body = response
        .bytes()
        .map_err(|err| CliError::Http(err.to_string()))?;
    let body_text = String::from_utf8_lossy(&body).into_owned();
    if status.is_success() {
        serde_json::from_slice::<T>(&body).map_err(|err| {
            CliError::Http(format!(
                "error decoding response body: {err}; status: {}; body: {}",
                status.as_u16(),
                summarize_response_body(&body_text)
            ))
        })
    } else {
        Err(CliError::Http(format!(
            "bench request failed with status {}: {}",
            status.as_u16(),
            summarize_response_body(&body_text)
        )))
    }
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    values[index]
}

fn summarize_response_body(body: &str) -> String {
    const MAX_LEN: usize = 600;
    let compact = body.trim().replace('\n', "\\n");
    if compact.len() <= MAX_LEN {
        compact
    } else {
        format!("{}...", &compact[..MAX_LEN])
    }
}

#[derive(Debug)]
enum CliError {
    Usage(String),
    Http(String),
    Api(StatusCode, ErrorResponse),
}

impl Display for CliError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(message) => write!(f, "{message}"),
            Self::Http(message) => write!(f, "{message}"),
            Self::Api(status, error) => {
                write!(f, "{} {}: {}", status.as_u16(), error.code, error.message)
            }
        }
    }
}

impl std::error::Error for CliError {}

#[derive(Debug)]
struct ParsedArgs {
    base_url: Option<String>,
    token: Option<String>,
    config_path_explicit: bool,
    command: Command,
}

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Doctor {
        config_path: PathBuf,
        caddy_admin_url: String,
        metrics_url: Option<String>,
    },
    Daemon(DaemonCommand),
    Init {
        force: bool,
    },
    Login {
        server_url: String,
    },
    Logout,
    WhoAmI,
    ControlPlaneLeader {
        config_path: PathBuf,
    },
    ControlPlaneLease {
        config_path: PathBuf,
    },
    ControlPlaneReplayStatus {
        config_path: PathBuf,
    },
    ControlPlaneIntents {
        config_path: PathBuf,
    },
    ControlPlaneReplay {
        config_path: PathBuf,
        caddy_admin_url: String,
        caddy_public_url: String,
        dry_run: bool,
        resume: bool,
    },
    Deploy {
        project_id: String,
        environment: String,
        source_path: Option<PathBuf>,
        source_ref: Option<String>,
    },
    Status {
        deployment_id: String,
    },
    Logs {
        deployment_id: String,
        service: Option<String>,
        json: bool,
    },
    ProjectStatus {
        project_id: String,
        environment: String,
        json: bool,
    },
    Bench {
        target: String,
        samples: usize,
    },
    Diagnose {
        project_id: String,
        environment: String,
        json: bool,
    },
    History {
        project_id: String,
        environment: String,
        json: bool,
    },
    Env {
        project_id: String,
        environment: String,
        json: bool,
    },
    EnvDiff {
        project_id: String,
        environment: String,
        from_generation: u64,
        to_generation: u64,
        json: bool,
    },
    Events,
    Gc {
        config_path: PathBuf,
        caddy_admin_url: String,
        caddy_public_url: String,
        dry_run: bool,
        json: bool,
    },
    Rollback {
        project_id: String,
        environment: String,
    },
    BackupCreate {
        project_id: String,
        environment: String,
        json: bool,
    },
    BackupList {
        project_id: String,
        environment: String,
        json: bool,
    },
    BackupInspect {
        backup_id: String,
        json: bool,
    },
    BackupRestore {
        backup_id: String,
        json: bool,
    },
    ProjectAdd {
        project_id: Option<String>,
        repo_url: String,
        default_branch: String,
        base_domain: Option<String>,
    },
    ProjectList,
    ProjectShow {
        project_id: String,
    },
    SecretsSet {
        project_id: String,
        environment: String,
        key: String,
        value: String,
    },
    SecretsList {
        project_id: String,
        environment: String,
        json: bool,
    },
    SecretsUnset {
        project_id: String,
        environment: String,
        key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DaemonCommand {
    config_path: PathBuf,
    caddy_admin_url: String,
    caddy_public_url: String,
}

impl ParsedArgs {
    fn parse(mut args: Vec<String>) -> Result<Self, CliError> {
        let mut base_url = None;
        let mut token = None;
        let mut config_path = None;
        let mut caddy_admin_url = None;
        let mut caddy_public_url = None;
        let mut metrics_url = None;
        let mut config_path_from_flag = false;

        loop {
            if args.first().map(String::as_str) == Some("--url") {
                if args.len() < 2 {
                    return Err(CliError::Usage("--url requires a value".into()));
                }
                base_url = Some(args[1].clone());
                args.drain(0..2);
                continue;
            }
            if args.first().map(String::as_str) == Some("--token") {
                if args.len() < 2 {
                    return Err(CliError::Usage("--token requires a value".into()));
                }
                token = Some(args[1].clone());
                args.drain(0..2);
                continue;
            }
            if args.first().map(String::as_str) == Some("--config") {
                if args.len() < 2 {
                    return Err(CliError::Usage("--config requires a value".into()));
                }
                config_path = Some(PathBuf::from(args[1].clone()));
                config_path_from_flag = true;
                args.drain(0..2);
                continue;
            }
            if args.first().map(String::as_str) == Some("--caddy-admin-url") {
                if args.len() < 2 {
                    return Err(CliError::Usage("--caddy-admin-url requires a value".into()));
                }
                caddy_admin_url = Some(args[1].clone());
                args.drain(0..2);
                continue;
            }
            if args.first().map(String::as_str) == Some("--caddy-public-url") {
                if args.len() < 2 {
                    return Err(CliError::Usage(
                        "--caddy-public-url requires a value".into(),
                    ));
                }
                caddy_public_url = Some(args[1].clone());
                args.drain(0..2);
                continue;
            }
            if args.first().map(String::as_str) == Some("--metrics-url") {
                if args.len() < 2 {
                    return Err(CliError::Usage("--metrics-url requires a value".into()));
                }
                metrics_url = Some(args[1].clone());
                args.drain(0..2);
                continue;
            }
            break;
        }

        let config_path_from_env = env::var("FORGE_CONFIG").ok().map(PathBuf::from);
        let command = parse_command(
            args,
            config_path
                .or_else(|| config_path_from_env.clone())
                .unwrap_or_else(|| PathBuf::from("forge.conf")),
            caddy_admin_url
                .or_else(|| env::var("FORGE_CADDY_ADMIN_URL").ok())
                .unwrap_or_else(|| "http://127.0.0.1:2019".into()),
            caddy_public_url
                .or_else(|| env::var("FORGE_CADDY_PUBLIC_URL").ok())
                .unwrap_or_else(|| "http://127.0.0.1".into()),
            metrics_url,
        )?;
        Ok(Self {
            base_url,
            token,
            config_path_explicit: config_path_from_flag || config_path_from_env.is_some(),
            command,
        })
    }

    fn base_url(&self) -> Result<String, CliError> {
        self.base_url
            .clone()
            .or_else(|| env::var("FORGE_URL").ok())
            .or_else(|| {
                load_saved_cli_config()
                    .ok()
                    .and_then(|config| config.server_url)
            })
            .ok_or_else(|| CliError::Usage("missing Forge URL: use --url or FORGE_URL".into()))
    }

    fn token(&self) -> Result<String, CliError> {
        self.token
            .clone()
            .or_else(|| env::var("FORGE_TOKEN").ok())
            .or_else(|| load_saved_cli_config().ok().and_then(|config| config.token))
            .ok_or_else(|| {
                CliError::Usage("missing Forge token: use --token or FORGE_TOKEN".into())
            })
    }

    fn resolved_server_url(&self) -> Result<Option<String>, CliError> {
        if let Some(value) = self.base_url.clone() {
            return Ok(Some(value));
        }
        if let Ok(value) = env::var("FORGE_URL") {
            return Ok(Some(value));
        }
        Ok(load_saved_cli_config()?.server_url)
    }

    fn resolved_token(&self) -> Result<(Option<String>, &'static str), CliError> {
        if let Some(value) = self.token.clone() {
            return Ok((Some(value), "flag"));
        }
        if let Ok(value) = env::var("FORGE_TOKEN") {
            return Ok((Some(value), "env"));
        }
        let config = load_saved_cli_config()?;
        Ok((config.token, "config"))
    }

    fn remote_client_if_logged_in(&self) -> Result<Option<ForgeClient>, CliError> {
        let Some(base_url) = self.resolved_server_url()? else {
            return Ok(None);
        };
        let (Some(token), _) = self.resolved_token()? else {
            return Ok(None);
        };
        Ok(Some(ForgeClient::new(base_url, token)))
    }
}

fn parse_command(
    args: Vec<String>,
    config_path: PathBuf,
    caddy_admin_url: String,
    caddy_public_url: String,
    metrics_url: Option<String>,
) -> Result<Command, CliError> {
    match args.as_slice() {
        [cmd] if cmd == "doctor" => Ok(Command::Doctor {
            config_path,
            caddy_admin_url,
            metrics_url,
        }),
        [cmd] if cmd == "daemon" => Ok(Command::Daemon(DaemonCommand {
            config_path,
            caddy_admin_url,
            caddy_public_url,
        })),
        [cmd] if cmd == "init" => Ok(Command::Init { force: false }),
        [cmd, flag] if cmd == "init" && flag == "--force" => Ok(Command::Init { force: true }),
        [cmd, server_url] if cmd == "login" => Ok(Command::Login {
            server_url: server_url.clone(),
        }),
        [cmd] if cmd == "logout" => Ok(Command::Logout),
        [cmd] if cmd == "whoami" => Ok(Command::WhoAmI),
        [group, action] if group == "control-plane" && action == "leader" => {
            Ok(Command::ControlPlaneLeader { config_path })
        }
        [group, action] if group == "control-plane" && action == "lease" => {
            Ok(Command::ControlPlaneLease { config_path })
        }
        [group, action] if group == "control-plane" && action == "replay-status" => {
            Ok(Command::ControlPlaneReplayStatus { config_path })
        }
        [group, action] if group == "control-plane" && action == "intents" => {
            Ok(Command::ControlPlaneIntents { config_path })
        }
        [group, action, flag]
            if group == "control-plane" && action == "replay" && flag == "--dry-run" =>
        {
            Ok(Command::ControlPlaneReplay {
                config_path,
                caddy_admin_url,
                caddy_public_url,
                dry_run: true,
                resume: false,
            })
        }
        [group, action, flag]
            if group == "control-plane" && action == "replay" && flag == "--resume" =>
        {
            Ok(Command::ControlPlaneReplay {
                config_path,
                caddy_admin_url,
                caddy_public_url,
                dry_run: false,
                resume: true,
            })
        }
        [cmd, rest @ ..] if cmd == "deploy" => parse_deploy_command(rest),
        [cmd, rest @ ..] if cmd == "bench" => parse_bench_command(rest),
        [cmd, rest @ ..] if cmd == "status" => parse_status_command(rest),
        [cmd, rest @ ..] if cmd == "logs" => parse_logs_command(rest),
        [cmd, rest @ ..] if cmd == "diagnose" => parse_diagnose_command(rest),
        [cmd, rest @ ..] if cmd == "history" || cmd == "deployments" => parse_history_command(rest),
        [cmd, action, rest @ ..] if cmd == "env" && action == "diff" => {
            parse_env_diff_command(rest)
        }
        [cmd, rest @ ..] if cmd == "env" => parse_env_command(rest),
        [cmd] if cmd == "events" => Ok(Command::Events),
        [cmd] if cmd == "gc" => Ok(Command::Gc {
            config_path,
            caddy_admin_url,
            caddy_public_url,
            dry_run: false,
            json: false,
        }),
        [cmd, rest @ ..] if cmd == "gc" => {
            parse_gc_command(rest, config_path, caddy_admin_url, caddy_public_url)
        }
        [cmd, project_id, environment] if cmd == "rollback" => Ok(Command::Rollback {
            project_id: project_id.clone(),
            environment: environment.clone(),
        }),
        [cmd, rest @ ..] if cmd == "backup" => parse_backup_command(rest),
        [group, action] if group == "project" && action == "list" => Ok(Command::ProjectList),
        [group, action, project_id] if group == "project" && action == "show" => {
            Ok(Command::ProjectShow {
                project_id: project_id.clone(),
            })
        }
        [group, action, rest @ ..] if group == "project" && action == "add" => {
            parse_project_add_command(rest)
        }
        [group, action, project_id, environment, key, value]
            if group == "secrets" && action == "set" =>
        {
            Ok(Command::SecretsSet {
                project_id: project_id.clone(),
                environment: environment.clone(),
                key: key.clone(),
                value: value.clone(),
            })
        }
        [group, action, rest @ ..] if group == "secrets" && action == "list" => {
            parse_secret_list_command(rest)
        }
        [group, action, project_id, environment, key]
            if group == "secrets" && action == "unset" =>
        {
            Ok(Command::SecretsUnset {
                project_id: project_id.clone(),
                environment: environment.clone(),
                key: key.clone(),
            })
        }
        _ => Err(CliError::Usage(usage())),
    }
}

fn usage() -> String {
    [
        "usage:",
        "  forge [--config PATH] [--caddy-admin-url URL] [--metrics-url URL] doctor",
        "  forge [--config PATH] [--caddy-admin-url URL] [--caddy-public-url URL] daemon",
        "  forge init [--force]",
        "  forge login <server_url>",
        "  forge logout",
        "  forge whoami",
        "  forge [--config PATH] control-plane leader",
        "  forge [--config PATH] control-plane lease",
        "  forge [--config PATH] control-plane replay-status",
        "  forge [--config PATH] control-plane intents",
        "  forge [--config PATH] [--caddy-admin-url URL] [--caddy-public-url URL] control-plane replay --dry-run",
        "  forge [--config PATH] [--caddy-admin-url URL] [--caddy-public-url URL] control-plane replay --resume",
        "  forge [--url URL] [--token TOKEN] deploy [--from PATH] [--ref REF] <project_id> <environment>",
        "  forge [--url URL] bench <readyz|leader|convergence|diagnostics|snapshots> [--samples N]",
        "  forge [--url URL] [--token TOKEN] status <deployment_id>",
        "  forge [--url URL] [--token TOKEN] logs [--json] [--service SERVICE] <deployment_id>",
        "  forge [--url URL] [--token TOKEN] status [--json] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] diagnose [--json] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] history [--json] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] deployments [--json] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] env [--json] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] env diff [--json] <project_id> <environment> --generation <from> --generation <to>",
        "  forge [--url URL] [--token TOKEN] events",
        "  forge [--config PATH] [--caddy-admin-url URL] [--caddy-public-url URL] gc [--dry-run] [--json]",
        "  forge [--url URL] [--token TOKEN] rollback <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] backup create [--json] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] backup list [--json] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] backup inspect [--json] <backup_id>",
        "  forge [--url URL] [--token TOKEN] backup restore [--json] <backup_id>",
        "  forge [--url URL] [--token TOKEN] project add [<project_id>] --repo <repo_url> [--branch <branch>] [--domain <base_domain>]",
        "  forge [--url URL] [--token TOKEN] project list",
        "  forge [--url URL] [--token TOKEN] project show <project_id>",
        "  forge [--url URL] [--token TOKEN] secrets list [--json] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] secrets set <project_id> <environment> <key> <value>",
        "  forge [--url URL] [--token TOKEN] secrets unset <project_id> <environment> <key>",
    ]
    .join("\n")
}

fn parse_history_command(args: &[String]) -> Result<Command, CliError> {
    match args {
        [project_id, environment] => Ok(Command::History {
            project_id: project_id.clone(),
            environment: environment.clone(),
            json: false,
        }),
        [flag, project_id, environment] if flag == "--json" => Ok(Command::History {
            project_id: project_id.clone(),
            environment: environment.clone(),
            json: true,
        }),
        _ => Err(CliError::Usage(usage())),
    }
}

fn parse_gc_command(
    args: &[String],
    config_path: PathBuf,
    caddy_admin_url: String,
    caddy_public_url: String,
) -> Result<Command, CliError> {
    let mut dry_run = false;
    let mut json = false;
    for value in args {
        match value.as_str() {
            "--dry-run" => dry_run = true,
            "--json" => json = true,
            _ => return Err(CliError::Usage(usage())),
        }
    }
    Ok(Command::Gc {
        config_path,
        caddy_admin_url,
        caddy_public_url,
        dry_run,
        json,
    })
}

fn parse_deploy_command(args: &[String]) -> Result<Command, CliError> {
    let mut source_path = None;
    let mut source_ref = None;
    let mut positionals = Vec::new();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--from" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage("deploy requires --from <path>".into()));
                };
                source_path = Some(PathBuf::from(value));
            }
            "--ref" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage("deploy requires --ref <ref>".into()));
                };
                source_ref = Some(value.clone());
            }
            value if value.starts_with("--") => return Err(CliError::Usage(usage())),
            value => positionals.push(value.to_string()),
        }
        index += 1;
    }

    if source_path.is_some() && source_ref.is_some() {
        return Err(CliError::Usage(
            "deploy accepts either --from <path> or --ref <ref>, not both".into(),
        ));
    }

    match positionals.as_slice() {
        [project_id, environment] => Ok(Command::Deploy {
            project_id: project_id.clone(),
            environment: environment.clone(),
            source_path,
            source_ref,
        }),
        _ => Err(CliError::Usage(usage())),
    }
}

fn parse_status_command(args: &[String]) -> Result<Command, CliError> {
    let mut json = false;
    let mut positionals = Vec::new();

    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            value if value.starts_with("--") => return Err(CliError::Usage(usage())),
            value => positionals.push(value.to_string()),
        }
    }

    match positionals.as_slice() {
        [deployment_id] if !json => Ok(Command::Status {
            deployment_id: deployment_id.clone(),
        }),
        [project_id, environment] => Ok(Command::ProjectStatus {
            project_id: project_id.clone(),
            environment: environment.clone(),
            json,
        }),
        _ => Err(CliError::Usage(usage())),
    }
}

fn parse_bench_command(args: &[String]) -> Result<Command, CliError> {
    let mut samples = 50usize;
    let mut positionals = Vec::new();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--samples" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage("bench requires --samples <count>".into()));
                };
                samples = value.parse::<usize>().map_err(|_| {
                    CliError::Usage("bench samples must be a positive integer".into())
                })?;
                if samples == 0 {
                    return Err(CliError::Usage(
                        "bench samples must be greater than zero".into(),
                    ));
                }
            }
            value if value.starts_with("--") => return Err(CliError::Usage(usage())),
            value => positionals.push(value.to_string()),
        }
        index += 1;
    }

    match positionals.as_slice() {
        [target]
            if target == "readyz"
                || target == "leader"
                || target == "convergence"
                || target == "diagnostics"
                || target == "snapshots" =>
        {
            Ok(Command::Bench {
                target: target.clone(),
                samples,
            })
        }
        _ => Err(CliError::Usage(usage())),
    }
}

fn parse_env_command(args: &[String]) -> Result<Command, CliError> {
    let mut json = false;
    let mut positionals = Vec::new();

    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            value if value.starts_with("--") => return Err(CliError::Usage(usage())),
            value => positionals.push(value.to_string()),
        }
    }

    match positionals.as_slice() {
        [project_id, environment] => Ok(Command::Env {
            project_id: project_id.clone(),
            environment: environment.clone(),
            json,
        }),
        _ => Err(CliError::Usage(usage())),
    }
}

fn parse_env_diff_command(args: &[String]) -> Result<Command, CliError> {
    let mut json = false;
    let mut generations = Vec::new();
    let mut positionals = Vec::new();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--generation" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage(
                        "env diff requires --generation <value>".into(),
                    ));
                };
                let generation = value.parse::<u64>().map_err(|_| {
                    CliError::Usage("env diff generation must be an integer".into())
                })?;
                generations.push(generation);
            }
            value if value.starts_with("--") => return Err(CliError::Usage(usage())),
            value => positionals.push(value.to_string()),
        }
        index += 1;
    }

    match (positionals.as_slice(), generations.as_slice()) {
        ([project_id, environment], [from_generation, to_generation]) => Ok(Command::EnvDiff {
            project_id: project_id.clone(),
            environment: environment.clone(),
            from_generation: *from_generation,
            to_generation: *to_generation,
            json,
        }),
        _ => Err(CliError::Usage(usage())),
    }
}

fn parse_secret_list_command(args: &[String]) -> Result<Command, CliError> {
    match args {
        [project_id, environment] => Ok(Command::SecretsList {
            project_id: project_id.clone(),
            environment: environment.clone(),
            json: false,
        }),
        [flag, project_id, environment] if flag == "--json" => Ok(Command::SecretsList {
            project_id: project_id.clone(),
            environment: environment.clone(),
            json: true,
        }),
        _ => Err(CliError::Usage(usage())),
    }
}

fn parse_logs_command(args: &[String]) -> Result<Command, CliError> {
    let mut json = false;
    let mut service = None;
    let mut positionals = Vec::new();

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => json = true,
            "--service" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage(
                        "logs requires --service <service_id>".into(),
                    ));
                };
                service = Some(value.clone());
            }
            value if value.starts_with("--") => return Err(CliError::Usage(usage())),
            value => positionals.push(value.to_string()),
        }
        index += 1;
    }

    match positionals.as_slice() {
        [deployment_id] => Ok(Command::Logs {
            deployment_id: deployment_id.clone(),
            service,
            json,
        }),
        _ => Err(CliError::Usage(usage())),
    }
}

fn parse_backup_command(args: &[String]) -> Result<Command, CliError> {
    let Some((action, rest)) = args.split_first() else {
        return Err(CliError::Usage("backup action required".into()));
    };
    let mut json = false;
    let mut positionals = Vec::new();
    for arg in rest {
        match arg.as_str() {
            "--json" => json = true,
            value if value.starts_with("--") => {
                return Err(CliError::Usage("invalid backup command".into()));
            }
            value => positionals.push(value.to_string()),
        }
    }
    match (action.as_str(), positionals.as_slice()) {
        ("create", [project_id, environment]) => Ok(Command::BackupCreate {
            project_id: project_id.clone(),
            environment: environment.clone(),
            json,
        }),
        ("list", [project_id, environment]) => Ok(Command::BackupList {
            project_id: project_id.clone(),
            environment: environment.clone(),
            json,
        }),
        ("inspect", [backup_id]) => Ok(Command::BackupInspect {
            backup_id: backup_id.clone(),
            json,
        }),
        ("restore", [backup_id]) => Ok(Command::BackupRestore {
            backup_id: backup_id.clone(),
            json,
        }),
        _ => Err(CliError::Usage("invalid backup command".into())),
    }
}

fn parse_diagnose_command(args: &[String]) -> Result<Command, CliError> {
    let mut json = false;
    let mut positionals = Vec::new();

    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            value if value.starts_with("--") => return Err(CliError::Usage(usage())),
            value => positionals.push(value.to_string()),
        }
    }

    match positionals.as_slice() {
        [project_id, environment] => Ok(Command::Diagnose {
            project_id: project_id.clone(),
            environment: environment.clone(),
            json,
        }),
        _ => Err(CliError::Usage(usage())),
    }
}

fn parse_project_add_command(args: &[String]) -> Result<Command, CliError> {
    if args.is_empty() {
        return Err(CliError::Usage(usage()));
    }

    let mut project_id = None;
    let mut repo_url = None;
    let mut default_branch = Some("main".to_string());
    let mut base_domain = None;
    let mut index = 0;
    if !args[index].starts_with("--") {
        project_id = Some(args[index].clone());
        index += 1;
    }
    while index < args.len() {
        match args[index].as_str() {
            "--repo" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage(
                        "project add requires --repo <repo_url>".into(),
                    ));
                };
                repo_url = Some(value.clone());
            }
            "--branch" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage(
                        "project add requires --branch <branch>".into(),
                    ));
                };
                default_branch = Some(value.clone());
            }
            "--domain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage(
                        "project add requires --domain <base_domain>".into(),
                    ));
                };
                base_domain = Some(value.clone());
            }
            _ => return Err(CliError::Usage(usage())),
        }
        index += 1;
    }

    let Some(repo_url) = repo_url else {
        return Err(CliError::Usage(
            "project add requires --repo <repo_url>".into(),
        ));
    };

    Ok(Command::ProjectAdd {
        project_id,
        repo_url,
        default_branch: default_branch.unwrap_or_else(|| "main".into()),
        base_domain,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct SuccessEnvelope<T> {
    data: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct ErrorEnvelope {
    code: String,
    message: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct _EventList {
    events: Vec<EventRecord>,
}

fn init_project_config(force: bool) -> Result<(), CliError> {
    let path = PathBuf::from("forge.yml");
    if !force && path.exists() {
        return Err(CliError::Usage(
            "forge.yml already exists; rerun with --force to overwrite".into(),
        ));
    }
    match fs::write(&path, default_init_config()) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::AlreadyExists => Err(CliError::Usage(
            "forge.yml already exists; rerun with --force to overwrite".into(),
        )),
        Err(err) => Err(CliError::Usage(err.to_string())),
    }
}

fn default_init_config() -> &'static str {
    concat!(
        "version: 1\n",
        "name: api\n",
        "type: web\n",
        "\n",
        "build:\n",
        "  dockerfile: Dockerfile\n",
        "  context: .\n",
        "\n",
        "runtime:\n",
        "  port: 3000\n",
        "  healthcheck:\n",
        "    path: /health\n",
        "    expected_status: 200\n",
        "\n",
        "invariants:\n",
        "  - name: health\n",
        "    path: /health\n",
        "    expect_status: 200\n",
    )
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SavedCliConfig {
    server_url: Option<String>,
    token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct WhoAmIOutput {
    server_url: Option<String>,
    token_source: String,
    authenticated: String,
}

fn render_services_section(services: &[ServiceRuntimeStatus], include_logs: bool) -> String {
    let mut output = String::new();
    output.push_str("Services:\n");
    for service in services {
        output.push_str(&format!("  {}\n", service.service_id));
        output.push_str(&format!("    role: {}\n", service.role));
        output.push_str(&format!(
            "    container: {}\n",
            service.container_name.as_deref().unwrap_or("unknown")
        ));
        output.push_str(&format!("    running: {}\n", service.running));
        if let Some(ip) = service.container_ip.as_deref() {
            output.push_str(&format!("    ip: {ip}\n"));
        }
        if let Some(port) = service.internal_port {
            output.push_str(&format!("    port: {port}\n"));
        }
        output.push_str(&format!(
            "    resources: cpu={} memory={}MB\n",
            service
                .runtime_policy
                .cpu_limit
                .as_deref()
                .unwrap_or("unlimited"),
            service
                .runtime_policy
                .memory_limit_mb
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unlimited".into())
        ));
        output.push_str(&format!(
            "    restart: {}{}\n",
            forge_core::storage::normalize_restart_policy_name(
                &service.runtime_policy.restart_policy,
            ),
            service
                .runtime_policy
                .max_retries
                .map(|value| format!(" (max_retries={value})"))
                .unwrap_or_default()
        ));
        output.push_str(&format!("    restart_count: {}\n", service.restart_count));
        if let Some(exit_code) = service.last_exit_code {
            output.push_str(&format!("    last_exit_code: {exit_code}\n"));
        }
        if let Some(usage) = service.runtime_usage.as_ref() {
            output.push_str(&format!(
                "    usage: cpu={}%% memory={}MB/{}MB\n",
                usage.cpu_percent.as_deref().unwrap_or("unknown"),
                usage
                    .memory_usage_mb
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".into()),
                usage
                    .memory_limit_mb
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".into())
            ));
        }
        output.push_str(&format!("    route: {}\n", service.route));
        output.push_str(&format!("    health: {}\n", service.health));
        if !service.depends_on.is_empty() {
            output.push_str(&format!(
                "    depends_on: {}\n",
                service.depends_on.join(", ")
            ));
        }
        if !service.dns_aliases.is_empty() {
            output.push_str(&format!(
                "    dns_aliases: {}\n",
                service.dns_aliases.join(", ")
            ));
        }
        if let Some(reason) = service.failure_reason.as_deref() {
            output.push_str(&format!("    failure_reason: {reason}\n"));
        }
        if !service.volumes.is_empty() {
            output.push_str("    volumes:\n");
            for volume in &service.volumes {
                output.push_str(&format!(
                    "      {} -> {} ({}, attached={})\n",
                    volume.docker_volume_name, volume.mount_path, volume.retention, volume.attached
                ));
                for warning in &volume.warnings {
                    output.push_str(&format!("      warning: {warning}\n"));
                }
            }
        }
        if include_logs {
            output.push_str("    logs_tail:\n");
            if service.logs_tail.is_empty() {
                output.push_str("      unavailable\n");
            } else {
                for line in &service.logs_tail {
                    output.push_str(&format!("      {line}\n"));
                }
            }
        }
        output.push('\n');
    }
    output
}

fn render_project_environment_status(status: &ProjectEnvironmentStatus) -> String {
    let mut output = String::new();
    output.push_str(&format!("Project: {}\n", status.project_id));
    output.push_str(&format!("Environment: {}\n", status.environment));
    output.push_str(&format!("Status: {}\n\n", status.status));
    output.push_str("Domain:\n");
    output.push_str(&format!("  https://{}\n\n", status.domain));
    output.push_str("Runtime:\n");
    output.push_str(&format!(
        "  Generation: {}\n",
        status
            .active_generation
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into())
    ));
    output.push_str(&format!(
        "  Commit: {}\n",
        status
            .commit_sha
            .as_deref()
            .map(|value| value.chars().take(16).collect::<String>())
            .unwrap_or_else(|| "unknown".into())
    ));
    output.push_str(&format!(
        "  Ref: {}\n",
        status.source_ref.as_deref().unwrap_or("unknown")
    ));
    output.push_str(&format!(
        "  Container: {}\n",
        status.container_name.as_deref().unwrap_or("unknown")
    ));
    output.push_str(&format!("  Running: {}\n", status.container_running));
    output.push_str(&format!(
        "  Resources: cpu={} memory={}MB restart={}{}\n",
        status
            .runtime_policy
            .cpu_limit
            .as_deref()
            .unwrap_or("unlimited"),
        status
            .runtime_policy
            .memory_limit_mb
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unlimited".into()),
        forge_core::storage::normalize_restart_policy_name(&status.runtime_policy.restart_policy),
        status
            .runtime_policy
            .max_retries
            .map(|value| format!(" (max_retries={value})"))
            .unwrap_or_default()
    ));
    output.push_str(&format!("  Restart Count: {}\n", status.restart_count));
    if let Some(usage) = status.runtime_usage.as_ref() {
        output.push_str(&format!(
            "  Usage Snapshot: cpu={}%% memory={}MB/{}MB\n",
            usage.cpu_percent.as_deref().unwrap_or("unknown"),
            usage
                .memory_usage_mb
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".into()),
            usage
                .memory_limit_mb
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".into())
        ));
    }
    output.push_str(&format!(
        "  Network: {}\n",
        status.network_name.as_deref().unwrap_or("unknown")
    ));
    output.push_str(&format!(
        "  IP: {}\n\n",
        status.container_ip.as_deref().unwrap_or("unknown")
    ));
    output.push_str("Routing:\n");
    output.push_str(&format!("  Route Active: {}\n", status.route_active));
    output.push_str(&format!(
        "  Probe Path: {}\n",
        status.probe_path.as_deref().unwrap_or("unknown")
    ));
    if let Some(state) = status.lifecycle_state.as_ref() {
        output.push_str(&format!("  Lifecycle State: {}\n", state.as_str()));
    }
    if status.lifecycle_state.is_some() || status.retention_role.is_some() {
        output.push_str(&format!(
            "  Status Label: {}\n",
            render_status_label(
                status.lifecycle_state.as_ref(),
                status.retention_role.as_ref()
            )
        ));
    }
    if let Some(retention_role) = status.retention_role.as_ref() {
        output.push_str(&format!("  Retention Role: {}\n", retention_role.as_str()));
    }
    if let Some(summary) = status.validation_summary.as_ref() {
        output.push_str(&format!(
            "  Validation Counters: tcp={}/{} http={}/{}\n",
            summary.tcp_consecutive_passes,
            summary.required_consecutive_passes,
            summary.http_consecutive_passes,
            summary.required_consecutive_passes
        ));
        output.push_str(&format!(
            "  Uptime Stability: {}s/{} stable_restarts={}\n",
            status
                .uptime_seconds
                .unwrap_or(summary.observed_uptime_seconds),
            summary.minimum_uptime_seconds,
            summary.restart_count_stable
        ));
    }
    if let Some(snapshot) = status.runtime_env_snapshot.as_ref() {
        output.push('\n');
        output.push_str("Runtime Env Snapshot:\n");
        output.push_str(&format!("  Generation: {}\n", snapshot.generation));
        output.push_str(&format!("  Deployment: {}\n", snapshot.deployment_id));
        output.push_str(&format!(
            "  Source Environment: {}\n",
            snapshot.source_environment
        ));
        output.push_str(&format!("  Keys: {}\n", snapshot.total_keys));
    } else if status.active_generation.is_some() {
        output.push('\n');
        output.push_str("Runtime Env Snapshot:\n");
        output.push_str("  legacy metadata unavailable\n");
    }
    if status.services.len() > 1 {
        output.push('\n');
        output.push_str(&render_services_section(&status.services, false));
    }
    output
}

fn render_deployment_logs(logs: &DeploymentLogs) -> String {
    let mut output = String::new();
    output.push_str(&format!("Deployment: {}\n", logs.deployment_id));
    output.push_str(&format!("Project: {}\n", logs.project_id));
    output.push_str(&format!("Environment: {}\n\n", logs.environment));
    output.push_str("Lifecycle:\n");
    if logs.lifecycle.is_empty() {
        output.push_str("  unavailable\n");
    } else {
        for line in &logs.lifecycle {
            output.push_str(&format!("  {line}\n"));
        }
    }
    output.push('\n');
    if logs.services.len() > 1 || logs.selected_service.is_some() {
        output.push_str("Service Logs:\n");
        for service in &logs.services {
            output.push_str(&format!("  {}\n", service.service_id));
            output.push_str(&format!("    role: {}\n", service.role));
            output.push_str(&format!(
                "    container: {}\n",
                service.container_name.as_deref().unwrap_or("unknown")
            ));
            if service.lines.is_empty() {
                output.push_str("    unavailable\n");
            } else {
                for line in &service.lines {
                    output.push_str(&format!("    {line}\n"));
                }
            }
        }
    } else {
        output.push_str("Container Logs:\n");
        if logs.container_logs.is_empty() {
            output.push_str("  unavailable\n");
        } else {
            for line in &logs.container_logs {
                output.push_str(&format!("  {line}\n"));
            }
        }
    }
    if let Some(summary) = logs.validation_failure_summary.as_deref() {
        output.push('\n');
        output.push_str("Validation Failure:\n");
        output.push_str(&format!("  {summary}\n"));
    }
    if let Some(source) = logs.diagnostics_source.as_deref() {
        output.push('\n');
        output.push_str("Diagnostics Source:\n");
        output.push_str(&format!("  {source}\n"));
    }
    output
}

fn render_status_label(
    lifecycle_state: Option<&forge_core::storage::DeploymentLifecycleState>,
    retention_role: Option<&RetentionRole>,
) -> &'static str {
    match retention_role {
        Some(RetentionRole::Current) => "active",
        Some(RetentionRole::RollbackTarget) => "rollback_target",
        Some(RetentionRole::GcEligible) => "gc_eligible",
        Some(RetentionRole::Retained) => match lifecycle_state {
            Some(forge_core::storage::DeploymentLifecycleState::Promoted) => "historical_promoted",
            Some(forge_core::storage::DeploymentLifecycleState::Failed) => "failed",
            Some(forge_core::storage::DeploymentLifecycleState::Rollback) => "rollback",
            Some(forge_core::storage::DeploymentLifecycleState::GcEligible) => "gc_eligible",
            _ => "historical",
        },
        None => match lifecycle_state {
            Some(forge_core::storage::DeploymentLifecycleState::Promoted) => "historical_promoted",
            Some(forge_core::storage::DeploymentLifecycleState::Failed) => "failed",
            Some(forge_core::storage::DeploymentLifecycleState::Rollback) => "rollback",
            Some(forge_core::storage::DeploymentLifecycleState::GcEligible) => "gc_eligible",
            _ => "historical",
        },
    }
}

fn render_environment_diagnostics(diagnostics: &EnvironmentDiagnostics) -> String {
    let mut output = String::new();
    output.push_str(&format!("Project: {}\n", diagnostics.project_id));
    output.push_str(&format!("Environment: {}\n", diagnostics.environment));
    output.push_str(&format!("Status: {}\n\n", diagnostics.status));
    output.push_str("Runtime Truth:\n");
    output.push_str(&format!(
        "  Active Generation: {}\n",
        diagnostics
            .active_generation
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into())
    ));
    output.push_str(&format!(
        "  Last Deployment: {}\n",
        diagnostics
            .last_deployment_id
            .as_deref()
            .unwrap_or("unknown")
    ));
    output.push_str(&format!(
        "  Container: {}\n",
        diagnostics
            .container
            .container_name
            .as_deref()
            .unwrap_or("unknown")
    ));
    output.push_str(&format!("  Running: {}\n", diagnostics.container.running));
    output.push_str(&format!(
        "  State: {}\n",
        diagnostics
            .container
            .state_status
            .as_deref()
            .unwrap_or("unknown")
    ));
    if let Some(policy) = diagnostics.container.runtime_policy.as_ref() {
        output.push_str(&format!(
            "  Runtime Policy: cpu={} memory={}MB restart={}{}\n",
            policy.cpu_limit.as_deref().unwrap_or("unlimited"),
            policy
                .memory_limit_mb
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unlimited".into()),
            forge_core::storage::normalize_restart_policy_name(&policy.restart_policy),
            policy
                .max_retries
                .map(|value| format!(" (max_retries={value})"))
                .unwrap_or_default()
        ));
    }
    if let Some(usage) = diagnostics.container.runtime_usage.as_ref() {
        output.push_str(&format!(
            "  Usage Snapshot: cpu={}%% memory={}MB/{}MB\n",
            usage.cpu_percent.as_deref().unwrap_or("unknown"),
            usage
                .memory_usage_mb
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".into()),
            usage
                .memory_limit_mb
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".into())
        ));
    }
    if let Some(termination) = diagnostics.container.termination.as_ref() {
        output.push_str(&format!(
            "  Termination: reason={} exit_code={} oom_killed={} restart_count={}\n",
            termination.reason.as_deref().unwrap_or("unknown"),
            termination
                .exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".into()),
            termination.oom_killed,
            termination.restart_count
        ));
    }
    output.push_str(&format!(
        "  Route Target: {}\n",
        diagnostics
            .route
            .current_target
            .as_deref()
            .unwrap_or("none")
    ));
    output.push_str(&format!(
        "  Expected Route Target: {}\n",
        diagnostics
            .route
            .expected_target
            .as_deref()
            .unwrap_or("none")
    ));
    output.push_str(&format!(
        "  Probe Target: {}\n",
        format_probe_target(diagnostics.probe_target.as_ref())
    ));
    if let Some(state) = diagnostics.active_lifecycle_state.as_ref() {
        output.push_str(&format!("  Lifecycle State: {}\n", state.as_str()));
    }
    if diagnostics.active_lifecycle_state.is_some() || diagnostics.retention_role.is_some() {
        output.push_str(&format!(
            "  Status Label: {}\n",
            render_status_label(
                diagnostics.active_lifecycle_state.as_ref(),
                diagnostics.retention_role.as_ref()
            )
        ));
    }
    if let Some(retention_role) = diagnostics.retention_role.as_ref() {
        output.push_str(&format!("  Retention Role: {}\n", retention_role.as_str()));
    }
    if let Some(summary) = diagnostics.validation_summary.as_ref() {
        output.push_str(&format!(
            "  Validation Counters: tcp={}/{} http={}/{}\n",
            summary.tcp_consecutive_passes,
            summary.required_consecutive_passes,
            summary.http_consecutive_passes,
            summary.required_consecutive_passes
        ));
        output.push_str(&format!(
            "  Uptime Stability: {}s/{} stable_restarts={}\n",
            summary.observed_uptime_seconds,
            summary.minimum_uptime_seconds,
            summary.restart_count_stable
        ));
    }
    if let Some(stage) = diagnostics.likely_failure_stage.as_deref() {
        output.push('\n');
        output.push_str("Likely Failure Stage:\n");
        output.push_str(&format!("  {stage}\n"));
    }
    if let Some(reason) = diagnostics.last_failed_transition.as_deref() {
        output.push('\n');
        output.push_str("Last Failed Transition:\n");
        output.push_str(&format!("  {reason}\n"));
    }
    if let Some(reason) = diagnostics.promotion_gate_reason.as_deref() {
        output.push('\n');
        output.push_str("Promotion Gate:\n");
        output.push_str(&format!("  {reason}\n"));
    }
    if let Some(summary) = diagnostics.warmup_failure_summary.as_deref() {
        output.push('\n');
        output.push_str("Warmup Failure Summary:\n");
        output.push_str(&format!("  {summary}\n"));
    }
    if diagnostics.restart_instability || diagnostics.probe_flapping {
        output.push('\n');
        output.push_str("Stability Signals:\n");
        output.push_str(&format!(
            "  restart_instability: {}\n  probe_flapping: {}\n",
            diagnostics.restart_instability, diagnostics.probe_flapping
        ));
    }
    if let Some(probe_stability) = diagnostics.probe_stability.as_ref() {
        output.push('\n');
        output.push_str("Probe Stability:\n");
        output.push_str(&format!(
            "  Probe Success Rate: {:.0}%\n",
            probe_stability.success_rate * 100.0
        ));
        output.push_str(&format!(
            "  Consecutive Success Streak: {}\n",
            probe_stability.consecutive_success_streak
        ));
        output.push_str(&format!(
            "  Recent Failure Count: {}\n",
            probe_stability.recent_failure_count
        ));
        output.push_str(&format!(
            "  Flapping Window Summary: {}\n",
            probe_stability.flapping_window_summary
        ));
    }
    if let Some(reason) = diagnostics.route.mismatch_reason.as_deref() {
        output.push('\n');
        output.push_str("Route Mismatch:\n");
        output.push_str(&format!("  {reason}\n"));
    }
    if !diagnostics.startup_order.is_empty() {
        output.push('\n');
        output.push_str("Dependency Graph:\n");
        output.push_str(&format!("  {}\n", diagnostics.startup_order.join(" -> ")));
    }
    if !diagnostics.services.is_empty() {
        output.push('\n');
        output.push_str(&render_services_section(&diagnostics.services, true));
    }
    if !diagnostics.orphaned_state_warnings.is_empty() {
        output.push('\n');
        output.push_str("State Warnings:\n");
        for warning in &diagnostics.orphaned_state_warnings {
            output.push_str(&format!("  {warning}\n"));
        }
    }
    if let Some(historical_repairs) = render_historical_repairs_section(diagnostics) {
        output.push_str(&historical_repairs);
    }
    output.push('\n');
    output.push_str("Recent Failures:\n");
    if diagnostics.recent_failures.is_empty() {
        output.push_str("  none\n");
    } else {
        for failure in &diagnostics.recent_failures {
            output.push_str(&format!(
                "  gen-{} {}{}: {}\n",
                failure.generation,
                failure.failure_stage,
                if failure.historical {
                    " [historical]"
                } else {
                    ""
                },
                failure.failure_reason
            ));
        }
    }
    if let Some(source) = diagnostics.diagnostics_source.as_deref() {
        output.push('\n');
        output.push_str("Diagnostics Source:\n");
        output.push_str(&format!("  {source}\n"));
    }
    if diagnostics.cluster.observed_nodes > 0
        || diagnostics.cluster.split_brain_suspected
        || diagnostics.cluster.lease_epoch_divergence
    {
        output.push('\n');
        output.push_str("Cluster:\n");
        output.push_str(&format!(
            "  observed_nodes: {}\n",
            diagnostics.cluster.observed_nodes
        ));
        output.push_str(&format!(
            "  active_reconcilers: {}\n",
            diagnostics.cluster.active_reconcilers
        ));
        output.push_str(&format!(
            "  cluster_size: {}\n",
            diagnostics.cluster.cluster_size
        ));
        output.push_str(&format!(
            "  local_role: {}\n",
            diagnostics.cluster.local_role
        ));
        output.push_str(&format!(
            "  heartbeat_age_ms: {}\n",
            diagnostics.cluster.heartbeat_age_ms
        ));
        output.push_str(&format!(
            "  split_brain_suspected: {}\n",
            diagnostics.cluster.split_brain_suspected
        ));
        output.push_str(&format!(
            "  lease_epoch_divergence: {}\n",
            diagnostics.cluster.lease_epoch_divergence
        ));
    }
    if let Some(snapshot) = diagnostics.runtime_env_snapshot.as_ref() {
        output.push('\n');
        output.push_str("Runtime Env Snapshot:\n");
        output.push_str(&format!("  Generation: {}\n", snapshot.generation));
        output.push_str(&format!("  Deployment: {}\n", snapshot.deployment_id));
        for (key, value) in &snapshot.generated_forge_vars {
            output.push_str(&format!("  {key}={value}\n"));
        }
    }
    output.push('\n');
    output.push_str("Secret Checks:\n");
    if diagnostics.missing_required_secrets.is_empty() {
        output.push_str("  Missing Required Secrets: none\n");
    } else {
        for key in &diagnostics.missing_required_secrets {
            output.push_str(&format!("  Missing Required Secret: {key}\n"));
        }
    }
    if let Some(drift) = diagnostics.env_drift.as_ref() {
        output.push_str(&format!(
            "  Env Drift: gen-{} -> gen-{} (added={}, removed={}, changed={}, secret_ref_changes={})\n",
            drift.from_generation,
            drift.to_generation,
            drift.added,
            drift.removed,
            drift.changed_values,
            drift.changed_secret_references
        ));
    } else {
        output.push_str("  Env Drift: none\n");
    }
    if diagnostics.recent_secret_mutations.is_empty() {
        output.push_str("  Recent Secret Mutations: none\n");
    } else {
        for mutation in &diagnostics.recent_secret_mutations {
            output.push_str(&format!(
                "  Secret Mutation: {} {} after gen-{} at {}\n",
                mutation.key,
                mutation.mutation,
                mutation.active_generation,
                mutation.updated_at_unix
            ));
        }
    }
    output.push('\n');
    output.push_str("Retention:\n");
    if let Some(restore) = diagnostics.active_restore.as_ref() {
        output.push_str(&format!(
            "  Active Restore: {}\n",
            render_restore_lineage(restore)
        ));
    } else {
        output.push_str("  Active Restore: none\n");
    }
    output.push_str(&format!(
        "  Rollback-safe Generation: {}\n",
        diagnostics
            .rollback_safe_generation
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".into())
    ));
    if diagnostics.retained_generations.is_empty() {
        output.push_str("  Retained Generations: none\n");
    } else {
        for generation in &diagnostics.retained_generations {
            output.push_str(&format!("  gen-{}", generation.generation));
            if generation.rollback_target {
                output.push_str(" [rollback-safe]");
            }
            if generation.restored_by_rollback {
                output.push_str(" [restored]");
            }
            output.push('\n');
        }
    }
    output.push('\n');
    output.push_str("Backup Restore Events:\n");
    if diagnostics.backup_restore_events.is_empty() {
        output.push_str("  none\n");
    } else {
        for event in &diagnostics.backup_restore_events {
            output.push_str(&format!("  {event}\n"));
        }
    }
    if !diagnostics.state_restore_warnings.is_empty() {
        output.push_str("State Restore Warnings:\n");
        for warning in &diagnostics.state_restore_warnings {
            output.push_str(&format!("  {warning}\n"));
        }
    }
    output.push('\n');
    output.push_str("Recent GC Actions:\n");
    if diagnostics.recent_gc_actions.is_empty() {
        output.push_str("  none\n");
    } else {
        for action in &diagnostics.recent_gc_actions {
            output.push_str(&format!(
                "  {} gen-{} {}: {}\n",
                action.timestamp_unix,
                action
                    .generation
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".into()),
                action.action,
                action.outcome
            ));
        }
    }
    output
}

fn render_restore_lineage(lineage: &RestoreLineage) -> String {
    let mut output = format!(
        "backup={} restored_generation={}",
        lineage.backup_id, lineage.restored_generation
    );
    if let Some(source_generation) = lineage.source_generation {
        output.push_str(&format!(" source_generation={source_generation}"));
    } else {
        output.push_str(" source=unknown");
    }
    if let Some(deployment_id) = lineage.source_deployment_id.as_deref() {
        output.push_str(&format!(" source_deployment={deployment_id}"));
    }
    if let Some(restored_at_unix) = lineage.restored_at_unix {
        output.push_str(&format!(" restored_at={restored_at_unix}"));
    }
    if let Some(hook_succeeded) = lineage.hook_succeeded {
        output.push_str(&format!(" hook_succeeded={hook_succeeded}"));
    }
    if !lineage.restored_volumes.is_empty() {
        let restored_volumes = lineage
            .restored_volumes
            .iter()
            .map(|volume| {
                format!(
                    "{}:{} {} checksum={} restored_volume={}",
                    volume.service_id,
                    volume.volume_id,
                    volume.mount_path,
                    volume.archive_sha256,
                    volume
                        .restored_docker_volume_name
                        .as_deref()
                        .unwrap_or("unknown")
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!(" restored_volumes=[{restored_volumes}]"));
    }
    output
}

fn normalize_historical_repair_line(line: &str) -> String {
    line.replace("restart_policy: \"\"", "restart_policy: no")
        .replace("restart_policy=\"\"", "restart_policy=no")
}

fn render_historical_repairs_section(diagnostics: &EnvironmentDiagnostics) -> Option<String> {
    if diagnostics.status == "healthy" {
        return None;
    }

    let entries = diagnostics
        .historical_policy_drift_repairs
        .iter()
        .chain(diagnostics.historical_volume_repair_events.iter())
        .map(|entry| normalize_historical_repair_line(entry))
        .take(3)
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return None;
    }

    let mut output = String::from("\nHistorical Repairs:\n");
    for entry in entries {
        output.push_str(&format!("  {entry}\n"));
    }
    Some(output)
}

fn render_backup_record(backup: &BackupRecord) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "Backup: {}\nProject: {}\nEnvironment: {}\nCreated At: {}\nSource Generation: {}\n",
        backup.backup_id,
        backup.project_id,
        backup.environment,
        backup.created_at_unix,
        backup.source_generation
    ));
    if let Some(deployment_id) = backup.source_deployment_id.as_deref() {
        output.push_str(&format!("Source Deployment: {deployment_id}\n"));
    }
    output.push_str("Services:\n");
    if backup.services.is_empty() {
        output.push_str("  none\n");
    } else {
        for service in &backup.services {
            output.push_str(&format!("  {service}\n"));
        }
    }
    output.push_str("Volumes:\n");
    if backup.volumes.is_empty() {
        output.push_str("  none\n");
    } else {
        for volume in &backup.volumes {
            output.push_str(&format!(
                "  {}:{} -> {} ({}, {} bytes, sha256={})\n",
                volume.service_id,
                volume.volume_id,
                volume.mount_path,
                volume.archive_file,
                volume.archive_size_bytes,
                volume.archive_sha256
            ));
            if volume.archive_files.is_empty() {
                output.push_str("    archive files: none\n");
            } else {
                for file in &volume.archive_files {
                    output.push_str(&format!(
                        "    archive file: {} ({} bytes, sha256={})\n",
                        file.path, file.size_bytes, file.sha256
                    ));
                }
            }
        }
    }
    output.push_str("Hooks:\n");
    if backup.hooks.is_empty() {
        output.push_str("  none\n");
    } else {
        for hook in &backup.hooks {
            output.push_str(&format!(
                "  {}:{} container={} command={} exit_code={} started_at={} completed_at={}\n",
                hook.service_id,
                hook.volume_id,
                hook.container_name,
                hook.pre_backup_command,
                hook.exit_code,
                hook.started_at_unix
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".into()),
                hook.completed_at_unix
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".into())
            ));
            if !hook.stdout.is_empty() {
                output.push_str(&format!("    stdout: {}\n", hook.stdout));
            }
            if !hook.stderr.is_empty() {
                output.push_str(&format!("    stderr: {}\n", hook.stderr));
            }
        }
    }
    output.push_str("Restores:\n");
    if backup.restores.is_empty() {
        output.push_str("  none\n");
    } else {
        for restore in &backup.restores {
            output.push_str(&format!(
                "  gen-{} {} at {} ({})\n",
                restore.restored_generation,
                restore.restored_deployment_id,
                restore.restored_at_unix,
                restore.status
            ));
        }
    }
    output
}

fn render_backup_list(backups: &BackupListResponse) -> String {
    let mut output = format!(
        "Project: {}\nEnvironment: {}\nBackups:\n",
        backups.project_id, backups.environment
    );
    if backups.backups.is_empty() {
        output.push_str("  none\n");
    } else {
        for backup in &backups.backups {
            output.push_str(&format!(
                "  {} gen-{} volumes={} restores={}\n",
                backup.backup_id,
                backup.source_generation,
                backup.volumes.len(),
                backup.restores.len()
            ));
        }
    }
    if !backups.warnings.is_empty() {
        output.push_str("Warnings:\n");
        for warning in &backups.warnings {
            output.push_str(&format!("  {warning}\n"));
        }
    }
    output
}

fn render_backup_restore(restore: &BackupRestoreResponse) -> String {
    format!(
        "Backup: {}\nRestored Generation: {}\nRestored Deployment: {}\nRestored At: {}\n",
        restore.backup_id,
        restore.restored_generation,
        restore.restored_deployment_id,
        restore.restored_at_unix
    )
}

fn render_environment_diff(diff: &EnvironmentDiffResponse) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "Project: {}\nEnvironment: {}\nGenerations: {} -> {}\n\n",
        diff.project_id, diff.environment, diff.from_generation, diff.to_generation
    ));
    for entry in &diff.added {
        output.push_str(&format!("+ {}={}\n", entry.key, entry.value));
    }
    for entry in &diff.removed {
        output.push_str(&format!("- {}", entry.key));
        if !entry.value.is_empty() {
            output.push_str(&format!("={}", entry.value));
        }
        output.push('\n');
    }
    for entry in &diff.changed_values {
        output.push_str(&format!("~ {}={}\n", entry.key, entry.after));
    }
    for entry in &diff.changed_secret_references {
        output.push_str(&format!("~ {}={}\n", entry.key, entry.after));
    }
    if diff.added.is_empty()
        && diff.removed.is_empty()
        && diff.changed_values.is_empty()
        && diff.changed_secret_references.is_empty()
    {
        output.push_str("No runtime env changes.\n");
    }
    output
}

fn render_deployment_history(history: &DeploymentHistoryResponse) -> String {
    let mut output = String::new();
    for entry in &history.entries {
        output.push_str(&format!("Generation {}\n", entry.generation));
        let status = render_status_label(
            entry.lifecycle_state.as_ref(),
            entry.retention_role.as_ref(),
        );
        output.push_str(&format!("  status: {status}\n"));
        if let Some(state) = entry.lifecycle_state.as_ref() {
            output.push_str(&format!("  lifecycle_state: {}\n", state.as_str()));
        }
        if let Some(retention_role) = entry.retention_role.as_ref() {
            output.push_str(&format!("  retention_role: {}\n", retention_role.as_str()));
        }
        if let Some(summary) = entry.validation_summary.as_ref() {
            output.push_str(&format!("  uptime: {}s\n", summary.observed_uptime_seconds));
            output.push_str(&format!(
                "  probes: {}/{} passed\n",
                summary.tcp_consecutive_passes.min(
                    summary
                        .http_consecutive_passes
                        .max(summary.required_consecutive_passes)
                ),
                summary.required_consecutive_passes
            ));
        }
        if let Some(commit_sha) = entry.commit_sha.as_deref() {
            output.push_str(&format!("  commit: {commit_sha}\n"));
        }
        if let Some(deployment_id) = entry.deployment_id.as_deref() {
            output.push_str(&format!("  deployment: {deployment_id}\n"));
        }
        if let Some(created_at) = entry.created_at_unix {
            output.push_str(&format!("  created: {created_at}\n"));
        }
        if let Some(finalized_state) = entry.finalized_state.as_deref() {
            output.push_str(&format!("  finalized: {finalized_state}\n"));
        }
        if let Some(promoted_at) = entry.promoted_at_unix {
            output.push_str(&format!("  promoted: {promoted_at}\n"));
        }
        if let Some(reason) = entry.transition_reason.as_deref() {
            output.push_str(&format!("  reason: {reason}\n"));
        }
        output.push_str(&format!(
            "  retained: {}\n",
            if entry.retained { "yes" } else { "no" }
        ));
        if entry.rollback_target {
            output.push_str("  rollback_target: true\n");
        }
        if entry.restored_by_rollback {
            output.push_str("  restored: true\n");
        }
        if entry.missing_artifacts {
            output.push_str("  missing_artifacts: true\n");
        }
        output.push('\n');
    }
    output
}

fn render_gc_report(
    report: &forge_core::convergence::GarbageCollectionReport,
    dry_run: bool,
) -> String {
    let mut output = String::new();
    if report.actions.is_empty() {
        output.push_str(if dry_run {
            "No GC actions would run.\n"
        } else {
            "No GC actions ran.\n"
        });
        return output;
    }
    for action in &report.actions {
        let heading = match action.subject_kind.as_deref() {
            Some("generation") => action
                .generation
                .map(|value| format!("Generation {value}"))
                .unwrap_or_else(|| "Generation".into()),
            Some("checkout") => "Checkout".into(),
            Some("image") => "Image".into(),
            Some("diagnostics") => "Diagnostics".into(),
            Some("runtime_snapshot") => "Runtime Snapshot".into(),
            Some("root") => "Root".into(),
            _ => format!("{} {}", action.project_id, action.environment),
        };
        output.push_str(&format!("{heading}\n"));
        if let Some(subject) = action.subject.as_deref() {
            match action.subject_kind.as_deref() {
                Some("checkout")
                | Some("image")
                | Some("diagnostics")
                | Some("runtime_snapshot")
                | Some("root") => {
                    output.push_str(&format!("  {subject}\n"));
                }
                _ => {}
            }
        }
        output.push_str(&format!("  action: {}\n", action.action));
        output.push_str(&format!("  reason: {}\n", action.reason));
        output.push_str(&format!("  outcome: {}\n", action.outcome));
        if !action.deleted.is_empty() {
            output.push_str("  deleted:\n");
            for entry in &action.deleted {
                output.push_str(&format!("    {entry}\n"));
            }
        }
        if !action.protected.is_empty() {
            output.push_str("  protected:\n");
            for entry in &action.protected {
                output.push_str(&format!("    {entry}\n"));
            }
        }
        output.push('\n');
    }
    output
}

fn render_environment_variables(report: &EnvironmentVariableReport) -> String {
    let mut output = String::new();
    for value in &report.values {
        output.push_str(&format!("{}={}\n", value.key, value.value));
    }
    output
}

fn render_secret_list(response: &SecretListResponse) -> String {
    let mut output = String::new();
    for secret in &response.secrets {
        output.push_str(&format!("{}={}\n", secret.key, secret.value));
        output.push_str(&format!("  created_at: {}\n", secret.created_at_unix));
        output.push_str(&format!("  updated_at: {}\n", secret.updated_at_unix));
        if secret.referenced_by_generations.is_empty() {
            output.push_str("  referenced_by_generations: none\n");
        } else {
            output.push_str(&format!(
                "  referenced_by_generations: {}\n",
                secret
                    .referenced_by_generations
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    if response.secrets.is_empty() {
        output.push_str("No secrets configured.\n");
    }
    output
}

fn format_probe_target(target: Option<&forge_core::api::ProbeTargetDiagnostics>) -> String {
    let Some(target) = target else {
        return "unknown".into();
    };
    let host = target.host.as_deref().unwrap_or("unknown");
    let port = target
        .port
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".into());
    match target.path.as_deref() {
        Some(path) => format!("{host}:{port}{path}"),
        None => format!("{host}:{port}"),
    }
}

fn run_login(server_url: String) -> Result<(), CliError> {
    let server_url = server_url.trim_end_matches('/').to_string();
    let client = ForgeClient::new(server_url.clone(), String::new());
    let start = client.post_cli_login_start()?;
    let approval_url = format!("{}/login/cli?code={}", server_url, start.code);

    println!("Approve this Forge CLI login in your browser:");
    println!("{approval_url}");
    let _ = try_open_browser(&approval_url);
    println!("Waiting for approval...");

    loop {
        let poll = client.post_cli_login_poll(CliLoginPollRequest {
            code: start.code.clone(),
        })?;
        match poll.status.as_str() {
            "pending" => thread::sleep(Duration::from_secs(start.poll_interval_seconds.max(1))),
            "approved" => {
                let token = poll.token.ok_or_else(|| {
                    CliError::Usage("cli login approval returned no token".into())
                })?;
                save_cli_config(&SavedCliConfig {
                    server_url: Some(server_url.clone()),
                    token: Some(token),
                })?;
                println!("Logged in to {server_url}");
                return Ok(());
            }
            "expired" => {
                return Err(CliError::Usage(
                    "cli login request expired before approval".into(),
                ));
            }
            other => {
                return Err(CliError::Usage(format!(
                    "unexpected cli login status: {other}"
                )));
            }
        }
    }
}

fn run_logout() -> Result<(), CliError> {
    let mut config = load_saved_cli_config()?;
    config.token = None;
    if config.server_url.is_none() {
        remove_saved_cli_config()?;
    } else {
        save_cli_config(&config)?;
    }
    println!("Removed saved Forge token.");
    Ok(())
}

fn run_whoami(parsed: &ParsedArgs) -> Result<(), CliError> {
    let server_url = parsed.resolved_server_url()?;
    let (token, token_source) = parsed.resolved_token()?;
    let authenticated = match (server_url.clone(), token.clone()) {
        (Some(url), Some(token)) => {
            let client = ForgeClient::new(url, token);
            match client.check_auth() {
                Ok(true) => "authenticated",
                Ok(false) => "unauthenticated",
                Err(_) => "unknown",
            }
        }
        _ => "missing_credentials",
    };

    print_json(&WhoAmIOutput {
        server_url,
        token_source: if token.is_some() {
            token_source.into()
        } else {
            "none".into()
        },
        authenticated: authenticated.into(),
    })
}

fn run_gc_command(
    config_path: PathBuf,
    caddy_admin_url: String,
    caddy_public_url: String,
    dry_run: bool,
    json: bool,
) -> Result<(), CliError> {
    let config = DaemonConfig::load_from_file(config_path)
        .map_err(|err| CliError::Usage(err.to_string()))?;
    require_local_leader(&config.storage_root)?;
    let queue = PersistentQueue::new(config.storage_root.join("queue"))
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let mut docker = DockerCliRuntime::new(ProcessCommandRunner);
    let mut routing = CaddyApiRuntime::new(caddy_admin_url, caddy_public_url);
    let reconciliation = ReconciliationStore::new(&config.storage_root);
    let gc_intent = (!dry_run)
        .then(|| {
            reconciliation.append_pending(intent_request_for_storage_root(
                &config.storage_root,
                "gc_action",
                "*",
                "*",
                None,
                "gc",
                "retention_reconciliation",
                std::collections::BTreeMap::new(),
            ))
        })
        .transpose()
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let report = garbage_collect(
        &config.storage_root,
        &queue,
        &mut docker,
        &mut routing,
        dry_run,
    )
    .map_err(|err| CliError::Usage(err.to_string()))?;
    if let Some(intent) = gc_intent.as_ref() {
        let _ = reconciliation
            .append_status(
                intent,
                ReconciliationIntentStatus::Applied,
                std::collections::BTreeMap::new(),
            )
            .map_err(|err| CliError::Usage(err.to_string()))?;
    }
    if json {
        print_json(&report)?;
    } else {
        print!("{}", render_gc_report(&report, dry_run));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct ControlPlaneLeaderView {
    local_node_id: String,
    leader_node_id: Option<String>,
    leader: bool,
    lease_epoch: u64,
    lease_age_ms: u64,
    lease_expiry_ms: u64,
    reconciliation_enabled: bool,
    follower_mode: bool,
    acquired_at_unix: Option<u64>,
    expires_at_unix: Option<u64>,
    last_heartbeat_unix: Option<u64>,
    cluster: ClusterDiagnostics,
}

#[derive(Debug, Serialize)]
struct ControlPlaneLeaseView {
    leader_node_id: Option<String>,
    lease_epoch: u64,
    lease_age_ms: u64,
    lease_expiry_ms: u64,
    acquired_at_unix: Option<u64>,
    expires_at_unix: Option<u64>,
    last_heartbeat_unix: Option<u64>,
    cluster: ClusterDiagnostics,
}

fn run_control_plane_leader(
    remote_client: Option<ForgeClient>,
    config_path: PathBuf,
    config_path_explicit: bool,
) -> Result<(), CliError> {
    let view = if let Some(client) = remote_client {
        control_plane_leader_view_from_metrics(&client.get_metrics()?)
    } else {
        let (local_node_id, lease, cluster) =
            load_local_control_plane_state(&config_path, config_path_explicit)?;
        control_plane_leader_view_from_storage(&local_node_id, lease.as_ref(), cluster)
    };
    print!("{}", render_control_plane_leader(&view));
    Ok(())
}

fn run_control_plane_lease(
    remote_client: Option<ForgeClient>,
    config_path: PathBuf,
    config_path_explicit: bool,
) -> Result<(), CliError> {
    let view = if let Some(client) = remote_client {
        control_plane_lease_view_from_metrics(&client.get_metrics()?)
    } else {
        let (_, lease, cluster) =
            load_local_control_plane_state(&config_path, config_path_explicit)?;
        control_plane_lease_view_from_storage(lease.as_ref(), cluster)
    };
    print!("{}", render_control_plane_lease(&view));
    Ok(())
}

fn run_control_plane_replay_status(config_path: PathBuf) -> Result<(), CliError> {
    let config = DaemonConfig::load_from_file(config_path)
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let store = ReconciliationStore::new(&config.storage_root);
    let cursor = store
        .load_cursor()
        .map_err(|err| CliError::Usage(err.to_string()))?
        .unwrap_or_default();
    print_json(&cursor)
}

fn run_control_plane_intents(config_path: PathBuf) -> Result<(), CliError> {
    let config = DaemonConfig::load_from_file(config_path)
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let intents = ReconciliationStore::new(&config.storage_root)
        .read_all()
        .map_err(|err| CliError::Usage(err.to_string()))?
        .intents;
    print_json(&intents)
}

fn run_control_plane_replay(
    config_path: PathBuf,
    caddy_admin_url: String,
    caddy_public_url: String,
    dry_run: bool,
    resume: bool,
) -> Result<(), CliError> {
    let config = DaemonConfig::load_from_file(config_path)
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let lease = require_local_leader(&config.storage_root)?;
    let local_node = NodeMetadataStore::new(&config.storage_root)
        .load()
        .map_err(|err| CliError::Usage(err.to_string()))?
        .ok_or_else(|| CliError::Usage("node metadata unavailable".into()))?;
    let mut routing = CaddyApiRuntime::new(caddy_admin_url, caddy_public_url);
    let replay = ReconciliationStore::new(&config.storage_root)
        .replay(
            &mut routing,
            &local_node.node_id,
            lease.lease_epoch,
            ReplayOptions { dry_run, resume },
        )
        .map_err(|err| CliError::Usage(err.to_string()))?;
    print_json(&replay.cursor)
}

fn control_plane_leader_view_from_metrics(metrics: &MetricsResponse) -> ControlPlaneLeaderView {
    ControlPlaneLeaderView {
        local_node_id: metrics
            .node
            .as_ref()
            .map(|value| value.node_id.clone())
            .unwrap_or_else(|| "unknown".into()),
        leader_node_id: (!metrics.convergence_owner.is_empty())
            .then(|| metrics.convergence_owner.clone()),
        leader: metrics.leader,
        lease_epoch: metrics.lease_epoch,
        lease_age_ms: metrics.lease_age_ms,
        lease_expiry_ms: metrics.lease_expiry_ms,
        reconciliation_enabled: metrics.reconciliation_enabled,
        follower_mode: metrics.follower_mode,
        acquired_at_unix: None,
        expires_at_unix: None,
        last_heartbeat_unix: None,
        cluster: metrics.cluster.clone(),
    }
}

fn control_plane_leader_view_from_storage(
    local_node_id: &str,
    lease: Option<&PersistedLeaderLease>,
    cluster: ClusterDiagnostics,
) -> ControlPlaneLeaderView {
    let now_unix = now_unix();
    ControlPlaneLeaderView {
        local_node_id: local_node_id.into(),
        leader_node_id: lease.map(|value| value.leader_node_id.clone()),
        leader: lease.is_some_and(|value| value.leader_node_id == local_node_id),
        lease_epoch: lease.map(|value| value.lease_epoch).unwrap_or(0),
        lease_age_ms: lease_age_ms(lease, now_unix),
        lease_expiry_ms: lease_expiry_ms(lease, now_unix),
        reconciliation_enabled: lease.is_some_and(|value| {
            value.leader_node_id == local_node_id && value.expires_at_unix > now_unix
        }),
        follower_mode: lease.is_some_and(|value| {
            value.leader_node_id != local_node_id && value.expires_at_unix > now_unix
        }),
        acquired_at_unix: lease.map(|value| value.acquired_at_unix),
        expires_at_unix: lease.map(|value| value.expires_at_unix),
        last_heartbeat_unix: lease.map(|value| value.last_heartbeat_unix),
        cluster,
    }
}

fn control_plane_lease_view_from_metrics(metrics: &MetricsResponse) -> ControlPlaneLeaseView {
    ControlPlaneLeaseView {
        leader_node_id: (!metrics.convergence_owner.is_empty())
            .then(|| metrics.convergence_owner.clone()),
        lease_epoch: metrics.lease_epoch,
        lease_age_ms: metrics.lease_age_ms,
        lease_expiry_ms: metrics.lease_expiry_ms,
        acquired_at_unix: None,
        expires_at_unix: None,
        last_heartbeat_unix: None,
        cluster: metrics.cluster.clone(),
    }
}

fn control_plane_lease_view_from_storage(
    lease: Option<&PersistedLeaderLease>,
    cluster: ClusterDiagnostics,
) -> ControlPlaneLeaseView {
    let now_unix = now_unix();
    ControlPlaneLeaseView {
        leader_node_id: lease.map(|value| value.leader_node_id.clone()),
        lease_epoch: lease.map(|value| value.lease_epoch).unwrap_or(0),
        lease_age_ms: lease_age_ms(lease, now_unix),
        lease_expiry_ms: lease_expiry_ms(lease, now_unix),
        acquired_at_unix: lease.map(|value| value.acquired_at_unix),
        expires_at_unix: lease.map(|value| value.expires_at_unix),
        last_heartbeat_unix: lease.map(|value| value.last_heartbeat_unix),
        cluster,
    }
}

fn render_control_plane_leader(view: &ControlPlaneLeaderView) -> String {
    let mut output = String::new();
    output.push_str(&format!("local_node_id: {}\n", view.local_node_id));
    output.push_str(&format!(
        "leader_node_id: {}\n",
        view.leader_node_id.as_deref().unwrap_or("none")
    ));
    output.push_str(&format!("leader: {}\n", view.leader));
    output.push_str(&format!("lease_epoch: {}\n", view.lease_epoch));
    output.push_str(&format!("lease_age_ms: {}\n", view.lease_age_ms));
    output.push_str(&format!("lease_expiry_ms: {}\n", view.lease_expiry_ms));
    output.push_str(&format!(
        "reconciliation_enabled: {}\n",
        view.reconciliation_enabled
    ));
    output.push_str(&format!("follower_mode: {}\n", view.follower_mode));
    if let Some(value) = view.acquired_at_unix {
        output.push_str(&format!("acquired_at_unix: {value}\n"));
    }
    if let Some(value) = view.expires_at_unix {
        output.push_str(&format!("expires_at_unix: {value}\n"));
    }
    if let Some(value) = view.last_heartbeat_unix {
        output.push_str(&format!("last_heartbeat_unix: {value}\n"));
    }
    output.push_str(&format!(
        "observed_nodes: {}\n",
        view.cluster.observed_nodes
    ));
    output.push_str(&format!(
        "active_reconcilers: {}\n",
        view.cluster.active_reconcilers
    ));
    output.push_str(&format!(
        "lease_epoch_divergence: {}\n",
        view.cluster.lease_epoch_divergence
    ));
    output.push_str(&format!(
        "split_brain_suspected: {}\n",
        view.cluster.split_brain_suspected
    ));
    output.push_str(&format!("cluster_size: {}\n", view.cluster.cluster_size));
    output.push_str(&format!("local_role: {}\n", view.cluster.local_role));
    output.push_str(&format!(
        "heartbeat_age_ms: {}\n",
        view.cluster.heartbeat_age_ms
    ));
    output
}

fn render_control_plane_lease(view: &ControlPlaneLeaseView) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "leader_node_id: {}\n",
        view.leader_node_id.as_deref().unwrap_or("none")
    ));
    output.push_str(&format!("lease_epoch: {}\n", view.lease_epoch));
    output.push_str(&format!("lease_age_ms: {}\n", view.lease_age_ms));
    output.push_str(&format!("lease_expiry_ms: {}\n", view.lease_expiry_ms));
    if let Some(value) = view.acquired_at_unix {
        output.push_str(&format!("acquired_at_unix: {value}\n"));
    }
    if let Some(value) = view.expires_at_unix {
        output.push_str(&format!("expires_at_unix: {value}\n"));
    }
    if let Some(value) = view.last_heartbeat_unix {
        output.push_str(&format!("last_heartbeat_unix: {value}\n"));
    }
    output.push_str(&format!(
        "observed_nodes: {}\n",
        view.cluster.observed_nodes
    ));
    output.push_str(&format!(
        "active_reconcilers: {}\n",
        view.cluster.active_reconcilers
    ));
    output.push_str(&format!(
        "lease_epoch_divergence: {}\n",
        view.cluster.lease_epoch_divergence
    ));
    output.push_str(&format!(
        "split_brain_suspected: {}\n",
        view.cluster.split_brain_suspected
    ));
    output.push_str(&format!("cluster_size: {}\n", view.cluster.cluster_size));
    output.push_str(&format!("local_role: {}\n", view.cluster.local_role));
    output.push_str(&format!(
        "heartbeat_age_ms: {}\n",
        view.cluster.heartbeat_age_ms
    ));
    output
}

fn load_local_control_plane_state(
    config_path: &Path,
    config_path_explicit: bool,
) -> Result<(String, Option<PersistedLeaderLease>, ClusterDiagnostics), CliError> {
    if !config_path_explicit && !config_path.exists() {
        return Err(CliError::Usage(
            "missing config path; use --config /etc/forge/forge.conf".into(),
        ));
    }
    let config = DaemonConfig::load_from_file(config_path)
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let local_node = NodeMetadataStore::new(&config.storage_root)
        .load()
        .map_err(|err| CliError::Usage(err.to_string()))?
        .ok_or_else(|| CliError::Usage("node metadata unavailable".into()))?;
    let lease = LeaderLeaseStore::new(&config.storage_root)
        .load()
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let nodes = ClusterTopologyStore::new(&config.storage_root)
        .load()
        .map_err(|err| CliError::Usage(err.to_string()))?
        .nodes;
    let now_unix = now_unix();
    let active_reconcilers = nodes.iter().filter(|node| node.active_reconciler).count();
    let mut epochs = nodes
        .iter()
        .filter_map(|node| (node.lease_epoch_seen > 0).then_some(node.lease_epoch_seen))
        .collect::<Vec<_>>();
    epochs.sort_unstable();
    epochs.dedup();
    let leader_claims = nodes
        .iter()
        .filter(|node| node.role == "leader" && node.active_reconciler)
        .count();
    let local_role = if lease.as_ref().is_some_and(|value| {
        value.leader_node_id == local_node.node_id && value.expires_at_unix > now_unix
    }) {
        "leader"
    } else if lease
        .as_ref()
        .is_some_and(|value| value.expires_at_unix > now_unix)
    {
        "follower"
    } else {
        "candidate"
    };
    let heartbeat_age_ms = nodes
        .iter()
        .find(|node| node.node_id == local_node.node_id)
        .map(|node| now_unix.saturating_sub(node.last_seen_unix) * 1_000)
        .unwrap_or(0);
    Ok((
        local_node.node_id,
        lease,
        ClusterDiagnostics {
            observed_nodes: nodes.len(),
            active_reconcilers,
            lease_epoch_divergence: epochs.len() > 1,
            split_brain_suspected: leader_claims > 1,
            cluster_size: nodes.len(),
            local_role: local_role.into(),
            heartbeat_age_ms,
            multiple_active_reconcilers: active_reconcilers > 1,
            ..ClusterDiagnostics::default()
        },
    ))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn lease_age_ms(lease: Option<&PersistedLeaderLease>, now_unix: u64) -> u64 {
    lease
        .map(|value| now_unix.saturating_sub(value.last_heartbeat_unix) * 1_000)
        .unwrap_or(0)
}

fn lease_expiry_ms(lease: Option<&PersistedLeaderLease>, now_unix: u64) -> u64 {
    lease
        .map(|value| value.expires_at_unix.saturating_sub(now_unix) * 1_000)
        .unwrap_or(0)
}

fn require_local_leader(storage_root: &Path) -> Result<PersistedLeaderLease, CliError> {
    let local_node = NodeMetadataStore::new(storage_root)
        .load()
        .map_err(|err| CliError::Usage(err.to_string()))?
        .ok_or_else(|| CliError::Usage("node metadata unavailable".into()))?;
    let lease = LeaderLeaseStore::new(storage_root)
        .load()
        .map_err(|err| CliError::Usage(err.to_string()))?
        .ok_or_else(|| CliError::Usage("leader lease unavailable".into()))?;
    let now_unix = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if lease.leader_node_id != local_node.node_id || lease.expires_at_unix <= now_unix {
        return Err(CliError::Usage(
            "local node is not the active control-plane leader".into(),
        ));
    }
    Ok(lease)
}

fn load_saved_cli_config() -> Result<SavedCliConfig, CliError> {
    let path = saved_cli_config_path()?;
    if !path.exists() {
        return Ok(SavedCliConfig::default());
    }
    let raw = fs::read_to_string(&path).map_err(|err| CliError::Usage(err.to_string()))?;
    parse_saved_cli_config(&raw)
}

fn parse_saved_cli_config(raw: &str) -> Result<SavedCliConfig, CliError> {
    let mut config = SavedCliConfig::default();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(CliError::Usage("invalid Forge CLI config".into()));
        };
        let key = key.trim();
        let value = parse_toml_string(value.trim())?;
        match key {
            "server_url" => config.server_url = Some(value),
            "token" => config.token = Some(value),
            _ => {}
        }
    }
    Ok(config)
}

fn parse_toml_string(value: &str) -> Result<String, CliError> {
    let Some(value) = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    else {
        return Err(CliError::Usage("invalid Forge CLI config".into()));
    };
    Ok(value.replace("\\\"", "\"").replace("\\\\", "\\"))
}

fn save_cli_config(config: &SavedCliConfig) -> Result<(), CliError> {
    if config.server_url.is_none() && config.token.is_none() {
        return remove_saved_cli_config();
    }
    let path = saved_cli_config_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| CliError::Usage("invalid Forge CLI config path".into()))?;
    fs::create_dir_all(parent).map_err(|err| CliError::Usage(err.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(parent, fs::Permissions::from_mode(0o700));
    }

    let mut rendered = String::new();
    if let Some(server_url) = &config.server_url {
        rendered.push_str("server_url = \"");
        rendered.push_str(&escape_toml_string(server_url));
        rendered.push_str("\"\n");
    }
    if let Some(token) = &config.token {
        rendered.push_str("token = \"");
        rendered.push_str(&escape_toml_string(token));
        rendered.push_str("\"\n");
    }
    write_private_file(&path, rendered.as_bytes())
}

fn remove_saved_cli_config() -> Result<(), CliError> {
    let path = saved_cli_config_path()?;
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(CliError::Usage(err.to_string())),
    }
}

fn saved_cli_config_path() -> Result<PathBuf, CliError> {
    if let Ok(config_home) = env::var("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(config_home).join("forge").join("config.toml"));
    }
    if let Ok(home) = env::var("HOME") {
        return Ok(PathBuf::from(home)
            .join(".config")
            .join("forge")
            .join("config.toml"));
    }
    Err(CliError::Usage(
        "missing HOME or XDG_CONFIG_HOME for Forge CLI config".into(),
    ))
}

fn escape_toml_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn write_private_file(path: &Path, contents: &[u8]) -> Result<(), CliError> {
    let tmp_path = path.with_extension("tmp");
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&tmp_path)
            .map_err(|err| CliError::Usage(err.to_string()))?
    };
    #[cfg(not(unix))]
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .map_err(|err| CliError::Usage(err.to_string()))?;
    file.write_all(contents)
        .and_then(|_| file.sync_all())
        .map_err(|err| CliError::Usage(err.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o600));
    }
    fs::rename(tmp_path, path).map_err(|err| CliError::Usage(err.to_string()))
}

fn try_open_browser(url: &str) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open").arg(url).spawn()?;
        return Ok(());
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        std::process::Command::new("xdg-open").arg(url).spawn()?;
        return Ok(());
    }
    #[cfg(windows)]
    {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", url])
            .spawn()?;
        return Ok(());
    }
    #[allow(unreachable_code)]
    Ok(())
}

fn run_daemon(command: DaemonCommand) -> Result<(), CliError> {
    let config = DaemonConfig::load_from_file(&command.config_path)
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let worker_caddy_admin_url = command.caddy_admin_url.clone();
    let worker_caddy_public_url = command.caddy_public_url.clone();
    let mut daemon = Daemon::new(
        config.clone(),
        DockerCliRuntime::new(ProcessCommandRunner),
        CaddyApiRuntime::new(command.caddy_admin_url, command.caddy_public_url),
        ResumeActiveDeployments,
    );
    daemon
        .start()
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let worker_leadership = WorkerLeadership {
        node_id: daemon.node_id().to_string(),
    };
    let control_plane_cache = Arc::new(RwLock::new(daemon.control_plane_snapshot()));
    let daemon = Arc::new(Mutex::new(Box::new(daemon) as Box<dyn ControlPlane>));
    let heartbeat_daemon = daemon.clone();
    thread::spawn(move || run_heartbeat_loop(heartbeat_daemon));
    let readyz_daemon = daemon.clone();
    let control_plane_cache_loop = control_plane_cache.clone();
    thread::spawn(move || run_readyz_refresh_loop(readyz_daemon, control_plane_cache_loop));
    let worker_queue = PersistentQueue::new(config.storage_root.join("queue"))
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let worker_settings = DeploymentWorkerSettings {
        validation: ValidationPolicy {
            tcp_required: true,
            http_health_path: Some("/health".into()),
            activation: ActivationMode::Http {
                internal_port: 3000,
            },
            ..ValidationPolicy::default()
        },
        execution: ExecutionConfig {
            context_path: PathBuf::from("."),
            dockerfile_path: PathBuf::from("./Dockerfile"),
            network_name: Some(FORGE_MANAGED_DOCKER_NETWORK.into()),
        },
        ..DeploymentWorkerSettings::default()
    };
    let worker_storage_root = config.storage_root.clone();
    thread::spawn(move || {
        run_deployment_worker_loop(
            worker_storage_root,
            worker_queue,
            DockerCliRuntime::new(ProcessCommandRunner),
            DockerNetworkProbeRuntime::new(FORGE_MANAGED_DOCKER_NETWORK, 3000),
            CaddyApiRuntime::new(worker_caddy_admin_url, worker_caddy_public_url),
            worker_settings,
            worker_leadership,
        )
    });

    let github_webhooks = build_github_webhook_state(&config)?;
    let state = HttpState::new(
        daemon,
        control_plane_cache,
        config.bearer_token.clone(),
        IdempotencyStore::new(config.storage_root.join("idempotency"))
            .map_err(|err| CliError::Usage(err.to_string()))?,
        github_webhooks,
        forge_core::secrets::SecretStore::new(config.storage_root.join("secrets"))
            .map_err(|err| CliError::Usage(err.to_string()))?,
        ProjectRegistryStore::new(&config.storage_root),
        WebAuthState::from_env(),
        forge_core::http::CliAuthState::from_env(config.storage_root.join("cli-logins"))
            .map_err(|err| CliError::Usage(err.to_string()))?,
    );
    let app = router(state);

    let runtime = tokio::runtime::Runtime::new().map_err(|err| CliError::Usage(err.to_string()))?;
    runtime.block_on(async move {
        let listener = TcpListener::bind(&config.api_bind)
            .await
            .map_err(|err| CliError::Usage(err.to_string()))?;
        axum::serve(listener, app)
            .await
            .map_err(|err| CliError::Usage(err.to_string()))
    })
}

fn build_github_webhook_state(
    config: &DaemonConfig,
) -> Result<Option<GitHubWebhookState>, CliError> {
    match (
        config.github_webhook_secret.clone(),
        config.repository_cache_root.clone(),
    ) {
        (Some(secret), Some(repository_cache_root)) => Ok(Some(GitHubWebhookState::new(
            GitHubWebhookConfig {
                secret,
                repository_cache_root,
            },
            DeliveryStore::new(config.storage_root.join("github-deliveries"))
                .map_err(|err| CliError::Usage(err.to_string()))?,
        ))),
        (Some(_), None) => Err(CliError::Usage(
            "github_webhook_secret requires repository_cache_root".into(),
        )),
        _ => Ok(None),
    }
}

#[derive(Debug, Clone, Copy)]
struct ResumeActiveDeployments;

impl ActiveDeploymentDecider for ResumeActiveDeployments {
    fn should_resume(&self, _deployment: &forge_core::queue::DeploymentRecord) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_core::storage::LeaseAcquireOutcome;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("forge-cli-{name}-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn diagnostics_fixture(status: &str) -> EnvironmentDiagnostics {
        EnvironmentDiagnostics {
            project_id: "api".into(),
            environment: "staging".into(),
            status: status.into(),
            active_generation: Some(7),
            last_deployment_id: None,
            container: forge_core::api::ContainerRuntimeDiagnostics {
                running: true,
                ..forge_core::api::ContainerRuntimeDiagnostics::default()
            },
            route: forge_core::api::RouteDiagnostics {
                matches_expected: true,
                ..forge_core::api::RouteDiagnostics::default()
            },
            probe_target: None,
            startup_order: Vec::new(),
            services: Vec::new(),
            recent_failures: Vec::new(),
            latest_validation_failure: None,
            latest_route_activation_failure: None,
            likely_failure_stage: None,
            diagnostics_source: None,
            runtime_env_snapshot: None,
            retained_generations: Vec::new(),
            rollback_safe_generation: None,
            recent_gc_actions: Vec::new(),
            missing_required_secrets: Vec::new(),
            env_drift: None,
            recent_secret_mutations: Vec::new(),
            orphaned_state_warnings: Vec::new(),
            volume_repair_events: Vec::new(),
            current_volume_repair_events: Vec::new(),
            historical_volume_repair_events: Vec::new(),
            active_lifecycle_state: None,
            retention_role: None,
            validation_summary: None,
            promotion_summary: None,
            last_failed_transition: None,
            promotion_gate_reason: None,
            warmup_failure_summary: None,
            restart_instability: false,
            probe_flapping: false,
            probe_stability: None,
            active_restore: None,
            state_restore_warnings: Vec::new(),
            backup_restore_events: Vec::new(),
            policy_drift_repairs: Vec::new(),
            current_policy_drift_repairs: Vec::new(),
            historical_policy_drift_repairs: Vec::new(),
            convergence_checkpoint: None,
            domain_summaries: Vec::new(),
            node: None,
            cluster: ClusterDiagnostics::default(),
        }
    }

    #[test]
    fn daemon_command_dispatches_to_launcher() {
        let mut launched = None;

        run_with_args(
            vec![
                "--config".into(),
                "/tmp/forge.conf".into(),
                "--caddy-admin-url".into(),
                "http://127.0.0.1:2019".into(),
                "--caddy-public-url".into(),
                "http://forge.local".into(),
                "daemon".into(),
            ],
            |command| {
                launched = Some(command);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(
            launched,
            Some(DaemonCommand {
                config_path: PathBuf::from("/tmp/forge.conf"),
                caddy_admin_url: "http://127.0.0.1:2019".into(),
                caddy_public_url: "http://forge.local".into(),
            })
        );
    }

    #[test]
    fn control_plane_leader_command_parses() {
        let parsed = ParsedArgs::parse(vec![
            "--config".into(),
            "/tmp/forge.conf".into(),
            "control-plane".into(),
            "leader".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::ControlPlaneLeader {
                config_path: PathBuf::from("/tmp/forge.conf"),
            }
        );
    }

    #[test]
    fn control_plane_lease_command_parses() {
        let parsed = ParsedArgs::parse(vec![
            "--config".into(),
            "/tmp/forge.conf".into(),
            "control-plane".into(),
            "lease".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::ControlPlaneLease {
                config_path: PathBuf::from("/tmp/forge.conf"),
            }
        );
    }

    #[test]
    fn deploy_command_accepts_from_before_positionals() {
        let parsed = ParsedArgs::parse(vec![
            "--url".into(),
            "http://127.0.0.1:8080".into(),
            "--token".into(),
            "token".into(),
            "deploy".into(),
            "--from".into(),
            "/srv/api".into(),
            "api".into(),
            "production".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::Deploy {
                project_id: "api".into(),
                environment: "production".into(),
                source_path: Some(PathBuf::from("/srv/api")),
                source_ref: None,
            }
        );
    }

    #[test]
    fn deploy_command_accepts_from_after_positionals() {
        let parsed = ParsedArgs::parse(vec![
            "--url".into(),
            "http://127.0.0.1:8080".into(),
            "--token".into(),
            "token".into(),
            "deploy".into(),
            "api".into(),
            "production".into(),
            "--from".into(),
            "/srv/api".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::Deploy {
                project_id: "api".into(),
                environment: "production".into(),
                source_path: Some(PathBuf::from("/srv/api")),
                source_ref: None,
            }
        );
    }

    #[test]
    fn deploy_command_accepts_ref() {
        let parsed = ParsedArgs::parse(vec![
            "--url".into(),
            "http://127.0.0.1:8080".into(),
            "--token".into(),
            "token".into(),
            "deploy".into(),
            "api".into(),
            "production".into(),
            "--ref".into(),
            "release-2026-05".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::Deploy {
                project_id: "api".into(),
                environment: "production".into(),
                source_path: None,
                source_ref: Some("release-2026-05".into()),
            }
        );
    }

    #[test]
    fn deploy_ref_and_from_are_mutually_exclusive() {
        let err = ParsedArgs::parse(vec![
            "--url".into(),
            "http://127.0.0.1:8080".into(),
            "--token".into(),
            "token".into(),
            "deploy".into(),
            "api".into(),
            "production".into(),
            "--from".into(),
            "/srv/api".into(),
            "--ref".into(),
            "main".into(),
        ])
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "deploy accepts either --from <path> or --ref <ref>, not both"
        );
    }

    #[test]
    fn gc_command_accepts_dry_run_json() {
        let parsed = ParsedArgs::parse(vec![
            "--config".into(),
            "/tmp/forge.conf".into(),
            "gc".into(),
            "--dry-run".into(),
            "--json".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::Gc {
                config_path: PathBuf::from("/tmp/forge.conf"),
                caddy_admin_url: "http://127.0.0.1:2019".into(),
                caddy_public_url: "http://127.0.0.1".into(),
                dry_run: true,
                json: true,
            }
        );
    }

    #[test]
    fn gc_rejected_on_follower() {
        let root = temp_root("gc-rejected-on-follower");
        let local_node = NodeMetadataStore::new(&root).load_or_create().unwrap();
        match LeaderLeaseStore::new(&root)
            .try_acquire_or_renew("other-node", 100, 5)
            .unwrap()
        {
            LeaseAcquireOutcome::Leader(_) => {}
            LeaseAcquireOutcome::Follower(_) => panic!("expected remote leader lease"),
        }

        let err = require_local_leader(&root).unwrap_err();
        assert_eq!(
            err.to_string(),
            "local node is not the active control-plane leader"
        );
        assert!(!local_node.node_id.is_empty());
    }

    #[test]
    fn bench_leader_command_parses() {
        let parsed = ParsedArgs::parse(vec![
            "--url".into(),
            "http://127.0.0.1:8080".into(),
            "bench".into(),
            "leader".into(),
            "--samples".into(),
            "5".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::Bench {
                target: "leader".into(),
                samples: 5,
            }
        );
    }

    #[test]
    fn status_renders_multiservice_services_section() {
        let rendered = render_project_environment_status(&ProjectEnvironmentStatus {
            project_id: "forge-multiservice-test".into(),
            environment: "staging".into(),
            status: "healthy".into(),
            active_generation: Some(1),
            domain: "staging-api.example.com".into(),
            commit_sha: None,
            source_ref: None,
            container_name: Some("staging-forge-multiservice-test-api-gen-1".into()),
            container_running: true,
            container_status: Some("running".into()),
            network_name: Some("forge-managed".into()),
            container_ip: Some("172.29.0.2".into()),
            route_active: true,
            probe_path: Some("/health".into()),
            image_ref: None,
            runtime_policy: forge_core::storage::PersistedRuntimePolicy {
                restart_policy: "no".into(),
                ..forge_core::storage::PersistedRuntimePolicy::default()
            },
            runtime_usage: None,
            termination: None,
            restart_count: 0,
            startup_order: vec!["api".into(), "worker".into()],
            services: vec![
                ServiceRuntimeStatus {
                    service_id: "api".into(),
                    role: "exposed".into(),
                    depends_on: Vec::new(),
                    dns_aliases: vec!["api".into()],
                    container_name: Some("staging-forge-multiservice-test-api-gen-1".into()),
                    image_ref: None,
                    running: true,
                    state_status: Some("running".into()),
                    lifecycle_state: None,
                    network_name: Some("forge-managed".into()),
                    container_ip: Some("172.29.0.2".into()),
                    internal_port: Some(3000),
                    probe_path: Some("/health".into()),
                    runtime_policy: forge_core::storage::PersistedRuntimePolicy {
                        restart_policy: "no".into(),
                        ..forge_core::storage::PersistedRuntimePolicy::default()
                    },
                    runtime_usage: None,
                    termination: None,
                    restart_count: 0,
                    last_exit_code: None,
                    route: "active".into(),
                    health: "healthy".into(),
                    failure_reason: None,
                    volumes: Vec::new(),
                    logs_tail: Vec::new(),
                },
                ServiceRuntimeStatus {
                    service_id: "worker".into(),
                    role: "internal".into(),
                    depends_on: vec!["api".into()],
                    dns_aliases: vec!["worker".into()],
                    container_name: Some("staging-forge-multiservice-test-worker-gen-1".into()),
                    image_ref: None,
                    running: true,
                    state_status: Some("running".into()),
                    lifecycle_state: None,
                    network_name: Some("forge-managed".into()),
                    container_ip: Some("172.29.0.3".into()),
                    internal_port: None,
                    probe_path: None,
                    runtime_policy: forge_core::storage::PersistedRuntimePolicy {
                        restart_policy: "no".into(),
                        ..forge_core::storage::PersistedRuntimePolicy::default()
                    },
                    runtime_usage: None,
                    termination: None,
                    restart_count: 0,
                    last_exit_code: None,
                    route: "none".into(),
                    health: "running".into(),
                    failure_reason: None,
                    volumes: Vec::new(),
                    logs_tail: Vec::new(),
                },
            ],
            last_deployment_id: None,
            deployed_at_unix: None,
            container_started_at: None,
            runtime_env_snapshot: None,
            lifecycle_state: None,
            retention_role: None,
            validation_summary: None,
            promotion_summary: None,
            uptime_seconds: None,
        });

        assert!(rendered.contains("Services:"));
        assert!(rendered.contains("role: exposed"));
        assert!(rendered.contains("route: active"));
        assert!(rendered.contains("worker"));
        assert!(rendered.contains("depends_on: api"));
        assert!(rendered.contains("health: running"));
    }

    #[test]
    fn logs_group_multiservice_service_logs_by_default() {
        let rendered = render_deployment_logs(&DeploymentLogs {
            deployment_id: "dep-1".into(),
            project_id: "forge-multiservice-test".into(),
            environment: "staging".into(),
            lines: vec!["generation promoted".into()],
            lifecycle: vec!["generation promoted".into()],
            container_logs: Vec::new(),
            services: vec![
                forge_core::api::ServiceLogGroup {
                    service_id: "api".into(),
                    role: "exposed".into(),
                    container_name: Some("staging-api-gen-1".into()),
                    lines: vec!["api log line".into()],
                },
                forge_core::api::ServiceLogGroup {
                    service_id: "worker".into(),
                    role: "internal".into(),
                    container_name: Some("staging-worker-gen-1".into()),
                    lines: vec!["worker log line".into()],
                },
            ],
            selected_service: None,
            validation_failure_summary: None,
            diagnostics_source: None,
        });

        assert!(rendered.contains("Service Logs:"));
        assert!(rendered.contains("api log line"));
        assert!(rendered.contains("worker log line"));
    }

    #[test]
    fn logs_command_accepts_service_selector() {
        let parsed = ParsedArgs::parse(vec![
            "--url".into(),
            "http://127.0.0.1:8080".into(),
            "--token".into(),
            "token".into(),
            "logs".into(),
            "--service".into(),
            "worker".into(),
            "dep-1".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::Logs {
                deployment_id: "dep-1".into(),
                service: Some("worker".into()),
                json: false,
            }
        );
    }

    #[test]
    fn backup_list_json_flag_after_args() {
        let parsed = ParsedArgs::parse(vec![
            "--url".into(),
            "http://127.0.0.1:8080".into(),
            "--token".into(),
            "token".into(),
            "backup".into(),
            "list".into(),
            "api".into(),
            "production".into(),
            "--json".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::BackupList {
                project_id: "api".into(),
                environment: "production".into(),
                json: true,
            }
        );
    }

    #[test]
    fn backup_inspect_cli_parses_backup_id() {
        let parsed = ParsedArgs::parse(vec![
            "--url".into(),
            "http://127.0.0.1:8080".into(),
            "--token".into(),
            "token".into(),
            "backup".into(),
            "inspect".into(),
            "backup-1".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::BackupInspect {
                backup_id: "backup-1".into(),
                json: false,
            }
        );
    }

    #[test]
    fn backup_inspect_json_flag_after_backup_id() {
        let parsed = ParsedArgs::parse(vec![
            "--url".into(),
            "http://127.0.0.1:8080".into(),
            "--token".into(),
            "token".into(),
            "backup".into(),
            "inspect".into(),
            "backup-1".into(),
            "--json".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::BackupInspect {
                backup_id: "backup-1".into(),
                json: true,
            }
        );
    }

    #[test]
    fn backup_restore_json_flag_after_backup_id() {
        let parsed = ParsedArgs::parse(vec![
            "--url".into(),
            "http://127.0.0.1:8080".into(),
            "--token".into(),
            "token".into(),
            "backup".into(),
            "restore".into(),
            "backup-1".into(),
            "--json".into(),
        ])
        .unwrap();

        assert_eq!(
            parsed.command,
            Command::BackupRestore {
                backup_id: "backup-1".into(),
                json: true,
            }
        );
    }

    #[test]
    fn diagnose_healthy_environment_hides_historical_policy_repairs() {
        let mut diagnostics = diagnostics_fixture("healthy");
        diagnostics.historical_policy_drift_repairs =
            vec!["historical gen-7: restart_policy: \"\"".into()];
        let rendered = render_environment_diagnostics(&diagnostics);

        assert!(!rendered.contains("Policy Drift Repairs:"));
        assert!(!rendered.contains("Historical Repairs:"));
    }

    #[test]
    fn diagnose_healthy_environment_hides_historical_volume_repairs() {
        let mut diagnostics = diagnostics_fixture("healthy");
        diagnostics.historical_volume_repair_events =
            vec!["historical gen-7: repaired empty volume set".into()];
        let rendered = render_environment_diagnostics(&diagnostics);

        assert!(!rendered.contains("Volume Repairs:"));
        assert!(!rendered.contains("Historical Repairs:"));
    }

    #[test]
    fn historical_repair_output_normalizes_restart_policy() {
        let mut diagnostics = diagnostics_fixture("degraded");
        diagnostics.historical_policy_drift_repairs =
            vec!["historical gen-7: restart_policy: \"\"".into()];
        let rendered = render_environment_diagnostics(&diagnostics);

        assert!(rendered.contains("Historical Repairs:"));
        assert!(rendered.contains("restart_policy: no"));
        assert!(!rendered.contains("restart_policy: \"\""));
    }
}
