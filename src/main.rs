use std::env;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use std::thread;

use forge_core::api::{
    DeploymentAccepted, DeploymentRequest, DeploymentStatus, ErrorResponse, EventList,
};
use forge_core::caddy::CaddyApiRuntime;
use forge_core::config::DaemonConfig;
use forge_core::convergence::ActiveDeploymentDecider;
use forge_core::daemon::{Daemon, DeploymentWorkerSettings, run_deployment_worker_loop};
use forge_core::deployments::{ActivationMode, ValidationPolicy};
use forge_core::docker::{DockerCliRuntime, ProcessCommandRunner};
use forge_core::doctor::{DoctorOptions, run as run_doctor};
use forge_core::events::EventRecord;
use forge_core::github::GitHubWebhookConfig;
use forge_core::http::{
    ControlPlane, DeliveryStore, GitHubWebhookState, HttpState, IdempotencyStore, router,
};
use forge_core::probes::DockerNetworkProbeRuntime;
use forge_core::queue::PersistentQueue;
use forge_core::secrets::{SecretWriteRequest, SecretWriteResult};
use reqwest::StatusCode;
use reqwest::blocking::{Client, RequestBuilder};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
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
    let api_credentials = if matches!(parsed.command, Command::Doctor { .. } | Command::Daemon(_)) {
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
        Command::Deploy {
            project_id,
            environment,
        } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let accepted = client.post_deployment(DeploymentRequest {
                project_id,
                environment,
                intent: "deploy".into(),
            })?;
            print_json(&accepted)?;
        }
        Command::Status { deployment_id } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let status = client.get_status(&deployment_id)?;
            print_json(&status)?;
        }
        Command::Events => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let events = client.get_events()?;
            print_json(&events.events)?;
        }
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
            })?;
            print_json(&accepted)?;
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

    fn post_secret(&self, request: SecretWriteRequest) -> Result<SecretWriteResult, CliError> {
        self.send_json(
            self.http
                .post(format!("{}/secrets", self.base_url))
                .json(&request),
        )
    }

    fn send_json<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T, CliError> {
        let response = request
            .bearer_auth(&self.token)
            .send()
            .map_err(|err| CliError::Http(err.to_string()))?;
        let status = response.status();
        if status.is_success() {
            let envelope = response
                .json::<SuccessEnvelope<T>>()
                .map_err(|err| CliError::Http(err.to_string()))?;
            Ok(envelope.data)
        } else {
            let envelope = response
                .json::<ErrorEnvelope>()
                .map_err(|err| CliError::Http(err.to_string()))?;
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
    command: Command,
}

#[derive(Debug)]
enum Command {
    Doctor {
        config_path: PathBuf,
        caddy_admin_url: String,
        metrics_url: Option<String>,
    },
    Daemon(DaemonCommand),
    Deploy {
        project_id: String,
        environment: String,
    },
    Status {
        deployment_id: String,
    },
    Events,
    Rollback {
        project_id: String,
        environment: String,
    },
    SecretsSet {
        project_id: String,
        environment: String,
        key: String,
        value: String,
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

        let command = parse_command(
            args,
            config_path
                .or_else(|| env::var("FORGE_CONFIG").ok().map(PathBuf::from))
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
            command,
        })
    }

    fn base_url(&self) -> Result<String, CliError> {
        self.base_url
            .clone()
            .or_else(|| env::var("FORGE_URL").ok())
            .ok_or_else(|| CliError::Usage("missing Forge URL: use --url or FORGE_URL".into()))
    }

    fn token(&self) -> Result<String, CliError> {
        self.token
            .clone()
            .or_else(|| env::var("FORGE_TOKEN").ok())
            .ok_or_else(|| {
                CliError::Usage("missing Forge token: use --token or FORGE_TOKEN".into())
            })
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
        [cmd, project_id, environment] if cmd == "deploy" => Ok(Command::Deploy {
            project_id: project_id.clone(),
            environment: environment.clone(),
        }),
        [cmd, deployment_id] if cmd == "status" => Ok(Command::Status {
            deployment_id: deployment_id.clone(),
        }),
        [cmd] if cmd == "events" => Ok(Command::Events),
        [cmd, project_id, environment] if cmd == "rollback" => Ok(Command::Rollback {
            project_id: project_id.clone(),
            environment: environment.clone(),
        }),
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
        _ => Err(CliError::Usage(usage())),
    }
}

fn usage() -> String {
    [
        "usage:",
        "  forge [--config PATH] [--caddy-admin-url URL] [--metrics-url URL] doctor",
        "  forge [--config PATH] [--caddy-admin-url URL] [--caddy-public-url URL] daemon",
        "  forge [--url URL] [--token TOKEN] deploy <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] status <deployment_id>",
        "  forge [--url URL] [--token TOKEN] events",
        "  forge [--url URL] [--token TOKEN] rollback <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] secrets set <project_id> <environment> <key> <value>",
    ]
    .join("\n")
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
    let worker_queue = PersistentQueue::new(config.storage_root.join("queue"))
        .map_err(|err| CliError::Usage(err.to_string()))?;
    let worker_settings = DeploymentWorkerSettings {
        validation: ValidationPolicy {
            tcp_required: true,
            http_health_path: Some("/health".into()),
            activation: ActivationMode::Http {
                internal_port: 3000,
            },
        },
        ..DeploymentWorkerSettings::default()
    };
    let worker_storage_root = config.storage_root.clone();
    thread::spawn(move || {
        run_deployment_worker_loop(
            worker_storage_root,
            worker_queue,
            DockerCliRuntime::new(ProcessCommandRunner),
            DockerNetworkProbeRuntime::new("bridge", 3000),
            CaddyApiRuntime::new(worker_caddy_admin_url, worker_caddy_public_url),
            worker_settings,
        )
    });

    let github_webhooks = build_github_webhook_state(&config)?;
    let state = HttpState::new(
        Arc::new(Mutex::new(Box::new(daemon) as Box<dyn ControlPlane>)),
        config.bearer_token.clone(),
        IdempotencyStore::new(config.storage_root.join("idempotency"))
            .map_err(|err| CliError::Usage(err.to_string()))?,
        github_webhooks,
        forge_core::secrets::SecretStore::new(config.storage_root.join("secrets"))
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
}
