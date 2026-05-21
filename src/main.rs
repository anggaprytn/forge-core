use std::env;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io::ErrorKind;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use forge_core::api::{
    CliLoginPollRequest, CliLoginPollResponse, CliLoginStartResponse, DeploymentAccepted,
    DeploymentLogs, DeploymentRequest, DeploymentStatus, EnvironmentDiagnostics, ErrorResponse,
    EventList, ProjectList, ProjectRecord, ProjectUpsertRequest,
};
use forge_core::caddy::CaddyApiRuntime;
use forge_core::config::DaemonConfig;
use forge_core::convergence::ActiveDeploymentDecider;
use forge_core::daemon::{Daemon, DeploymentWorkerSettings, run_deployment_worker_loop};
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
use forge_core::secrets::{SecretWriteRequest, SecretWriteResult};
use forge_core::status::ProjectEnvironmentStatus;
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
    let api_credentials = if matches!(
        parsed.command,
        Command::Doctor { .. }
            | Command::Daemon(_)
            | Command::Init { .. }
            | Command::Login { .. }
            | Command::Logout
            | Command::WhoAmI
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
            json,
        } => {
            let (base_url, token) = api_credentials.clone().unwrap();
            let client = ForgeClient::new(base_url, token);
            let logs = client.get_logs(&deployment_id)?;
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
                source_path: None,
                source_ref: None,
            })?;
            print_json(&accepted)?;
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

    fn get_logs(&self, deployment_id: &str) -> Result<DeploymentLogs, CliError> {
        self.send_json(self.http.get(format!(
            "{}/api/deployments/{deployment_id}/logs",
            self.base_url
        )))
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

    fn post_secret(&self, request: SecretWriteRequest) -> Result<SecretWriteResult, CliError> {
        self.send_json(
            self.http
                .post(format!("{}/secrets", self.base_url))
                .json(&request),
        )
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
        json: bool,
    },
    ProjectStatus {
        project_id: String,
        environment: String,
        json: bool,
    },
    Diagnose {
        project_id: String,
        environment: String,
        json: bool,
    },
    Events,
    Rollback {
        project_id: String,
        environment: String,
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
        [cmd, rest @ ..] if cmd == "deploy" => parse_deploy_command(rest),
        [cmd, rest @ ..] if cmd == "status" => parse_status_command(rest),
        [cmd, rest @ ..] if cmd == "logs" => parse_logs_command(rest),
        [cmd, rest @ ..] if cmd == "diagnose" => parse_diagnose_command(rest),
        [cmd] if cmd == "events" => Ok(Command::Events),
        [cmd, project_id, environment] if cmd == "rollback" => Ok(Command::Rollback {
            project_id: project_id.clone(),
            environment: environment.clone(),
        }),
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
        "  forge [--url URL] [--token TOKEN] deploy [--from PATH] [--ref REF] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] status <deployment_id>",
        "  forge [--url URL] [--token TOKEN] logs [--json] <deployment_id>",
        "  forge [--url URL] [--token TOKEN] status [--json] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] diagnose [--json] <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] events",
        "  forge [--url URL] [--token TOKEN] rollback <project_id> <environment>",
        "  forge [--url URL] [--token TOKEN] project add [<project_id>] --repo <repo_url> [--branch <branch>] [--domain <base_domain>]",
        "  forge [--url URL] [--token TOKEN] project list",
        "  forge [--url URL] [--token TOKEN] project show <project_id>",
        "  forge [--url URL] [--token TOKEN] secrets set <project_id> <environment> <key> <value>",
    ]
    .join("\n")
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

fn parse_logs_command(args: &[String]) -> Result<Command, CliError> {
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
        [deployment_id] => Ok(Command::Logs {
            deployment_id: deployment_id.clone(),
            json,
        }),
        _ => Err(CliError::Usage(usage())),
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
    output.push_str("Container Logs:\n");
    if logs.container_logs.is_empty() {
        output.push_str("  unavailable\n");
    } else {
        for line in &logs.container_logs {
            output.push_str(&format!("  {line}\n"));
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
    if let Some(stage) = diagnostics.likely_failure_stage.as_deref() {
        output.push('\n');
        output.push_str("Likely Failure Stage:\n");
        output.push_str(&format!("  {stage}\n"));
    }
    if let Some(reason) = diagnostics.route.mismatch_reason.as_deref() {
        output.push('\n');
        output.push_str("Route Mismatch:\n");
        output.push_str(&format!("  {reason}\n"));
    }
    output.push('\n');
    output.push_str("Recent Failures:\n");
    if diagnostics.recent_failures.is_empty() {
        output.push_str("  none\n");
    } else {
        for failure in &diagnostics.recent_failures {
            output.push_str(&format!(
                "  gen-{} {}: {}\n",
                failure.generation, failure.failure_stage, failure.failure_reason
            ));
        }
    }
    if let Some(source) = diagnostics.diagnostics_source.as_deref() {
        output.push('\n');
        output.push_str("Diagnostics Source:\n");
        output.push_str(&format!("  {source}\n"));
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
}
