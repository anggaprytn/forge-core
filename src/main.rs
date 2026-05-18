use std::env;
use std::fmt::{Display, Formatter};

use forge_core::api::{DeploymentAccepted, DeploymentRequest, DeploymentStatus, EventList, ErrorResponse};
use forge_core::events::EventRecord;
use forge_core::secrets::{SecretWriteRequest, SecretWriteResult};
use reqwest::blocking::{Client, RequestBuilder};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), CliError> {
    let parsed = ParsedArgs::parse(env::args().skip(1).collect())?;
    let client = ForgeClient::new(parsed.base_url, parsed.token);

    match parsed.command {
        Command::Deploy {
            project_id,
            environment,
        } => {
            let accepted = client.post_deployment(DeploymentRequest {
                project_id,
                environment,
                intent: "deploy".into(),
            })?;
            print_json(&accepted)?;
        }
        Command::Status { deployment_id } => {
            let status = client.get_status(&deployment_id)?;
            print_json(&status)?;
        }
        Command::Events => {
            let events = client.get_events()?;
            print_json(&events.events)?;
        }
        Command::Rollback {
            project_id,
            environment,
        } => {
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
        self.send_json(self.http.get(format!(
            "{}/deployments/{}",
            self.base_url, deployment_id
        )))
    }

    fn get_events(&self) -> Result<EventList, CliError> {
        self.send_json(self.http.get(format!("{}/events", self.base_url)))
    }

    fn post_secret(&self, request: SecretWriteRequest) -> Result<SecretWriteResult, CliError> {
        self.send_json(self.http.post(format!("{}/secrets", self.base_url)).json(&request))
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
            Err(CliError::Api(status, ErrorResponse {
                code: envelope.code,
                message: envelope.message,
            }))
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
            Self::Api(status, error) => write!(f, "{} {}: {}", status.as_u16(), error.code, error.message),
        }
    }
}

impl std::error::Error for CliError {}

#[derive(Debug)]
struct ParsedArgs {
    base_url: String,
    token: String,
    command: Command,
}

#[derive(Debug)]
enum Command {
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

impl ParsedArgs {
    fn parse(mut args: Vec<String>) -> Result<Self, CliError> {
        let mut base_url = None;
        let mut token = None;

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
            break;
        }

        let base_url = base_url
            .or_else(|| env::var("FORGE_URL").ok())
            .ok_or_else(|| CliError::Usage("missing Forge URL: use --url or FORGE_URL".into()))?;
        let token = token
            .or_else(|| env::var("FORGE_TOKEN").ok())
            .ok_or_else(|| CliError::Usage("missing Forge token: use --token or FORGE_TOKEN".into()))?;

        let command = parse_command(args)?;
        Ok(Self {
            base_url,
            token,
            command,
        })
    }
}

fn parse_command(args: Vec<String>) -> Result<Command, CliError> {
    match args.as_slice() {
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
