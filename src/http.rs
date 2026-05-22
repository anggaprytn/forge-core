use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Form, Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::Url;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::api::{
    CliLoginPollRequest, CliLoginPollResponse, CliLoginStartResponse, DeploymentAccepted,
    DeploymentHistoryResponse, DeploymentLogs, DeploymentRequest, DeploymentStatus,
    EnvironmentDiagnostics, EnvironmentDiffResponse, EnvironmentVariableReport, ErrorResponse,
    EventList, ProjectList, ProjectUpsertRequest, SecretListResponse, SecretUnsetResponse,
};
use crate::daemon::{Daemon, DaemonState};
use crate::github::{
    GitHubError, GitHubWebhookConfig, WebhookResolution, resolve_webhook, verify_signature,
};
use crate::metrics::render_prometheus;
use crate::projects::{ProjectRegistryStore, project_registry_error_response};
use crate::runtime::{DockerRuntime, RoutingRuntime};
use crate::secrets::{SecretError, SecretStore, SecretWriteRequest};
use crate::status::ProjectEnvironmentStatus;
use crate::storage::atomic_write;

const AUTHORIZATION: &str = "authorization";
const IDEMPOTENCY_KEY: &str = "idempotency-key";
const X_GITHUB_DELIVERY: &str = "x-github-delivery";
const X_GITHUB_EVENT: &str = "x-github-event";
const X_HUB_SIGNATURE_256: &str = "x-hub-signature-256";
const REQUEST_ID_HEADER: &str = "x-request-id";
const CORRELATION_ID_HEADER: &str = "x-correlation-id";
const WEB_INDEX_HTML: &str = include_str!("../web/index.html");
const WEB_LOGIN_HTML: &str = include_str!("../web/login.html");
const WEB_APP_HTML: &str = include_str!("../web/app.html");
const WEB_STYLES_CSS: &str = include_str!("../web/styles.css");
const WEB_APP_JS: &str = include_str!("../web/app.js");
const WEB_LOGIN_REQUIRED_ENV_VARS: [&str; 4] = [
    "FORGE_GITHUB_OAUTH_CLIENT_ID",
    "FORGE_GITHUB_OAUTH_CLIENT_SECRET",
    "FORGE_PUBLIC_URL",
    "FORGE_SESSION_SECRET",
];
const SESSION_COOKIE_NAME: &str = "forge_session";
const OAUTH_STATE_COOKIE_NAME: &str = "forge_oauth_state";
const GITHUB_AUTHORIZE_URL: &str = "https://github.com/login/oauth/authorize";
const GITHUB_ACCESS_TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
const GITHUB_USER_URL: &str = "https://api.github.com/user";
const CLI_LOGIN_TTL_SECONDS: u64 = 300;
const CLI_LOGIN_POLL_INTERVAL_SECONDS: u64 = 1;

pub trait ControlPlane: Send {
    fn is_ready(&self) -> bool;
    fn handle_post_deployments(
        &mut self,
        request: DeploymentRequest,
    ) -> Result<DeploymentAccepted, ErrorResponse>;
    fn get_deployment(
        &self,
        deployment_id: &str,
    ) -> Result<Option<DeploymentStatus>, ErrorResponse>;
    fn get_deployment_logs(
        &self,
        deployment_id: &str,
        service_id: Option<&str>,
    ) -> Result<DeploymentLogs, ErrorResponse>;
    fn list_events(&self) -> Result<EventList, ErrorResponse>;
    fn queue_depth(&self) -> Result<usize, ErrorResponse>;
    fn get_project_environment_status(
        &mut self,
        project_id: &str,
        environment: &str,
    ) -> Result<ProjectEnvironmentStatus, ErrorResponse>;
    fn get_project_environment_diagnostics(
        &mut self,
        project_id: &str,
        environment: &str,
    ) -> Result<EnvironmentDiagnostics, ErrorResponse>;
    fn get_project_environment_history(
        &mut self,
        project_id: &str,
        environment: &str,
    ) -> Result<DeploymentHistoryResponse, ErrorResponse>;
    fn get_project_environment_env(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<EnvironmentVariableReport, ErrorResponse>;
    fn get_project_environment_env_diff(
        &self,
        project_id: &str,
        environment: &str,
        from_generation: u64,
        to_generation: u64,
    ) -> Result<EnvironmentDiffResponse, ErrorResponse>;
}

impl<D, R, A> ControlPlane for Daemon<D, R, A>
where
    D: DockerRuntime + Send,
    R: RoutingRuntime + Send,
    A: crate::convergence::ActiveDeploymentDecider + Send,
{
    fn is_ready(&self) -> bool {
        self.state() == &DaemonState::Ready
    }

    fn handle_post_deployments(
        &mut self,
        request: DeploymentRequest,
    ) -> Result<DeploymentAccepted, ErrorResponse> {
        Daemon::handle_post_deployments(self, request)
    }

    fn get_deployment(
        &self,
        deployment_id: &str,
    ) -> Result<Option<DeploymentStatus>, ErrorResponse> {
        Daemon::get_deployment(self, deployment_id)
    }

    fn get_deployment_logs(
        &self,
        deployment_id: &str,
        service_id: Option<&str>,
    ) -> Result<DeploymentLogs, ErrorResponse> {
        Daemon::get_deployment_logs(self, deployment_id, service_id)
    }

    fn list_events(&self) -> Result<EventList, ErrorResponse> {
        Daemon::list_events(self)
    }

    fn queue_depth(&self) -> Result<usize, ErrorResponse> {
        Daemon::queue_depth(self)
    }

    fn get_project_environment_status(
        &mut self,
        project_id: &str,
        environment: &str,
    ) -> Result<ProjectEnvironmentStatus, ErrorResponse> {
        Daemon::get_project_environment_status(self, project_id, environment)
    }

    fn get_project_environment_diagnostics(
        &mut self,
        project_id: &str,
        environment: &str,
    ) -> Result<EnvironmentDiagnostics, ErrorResponse> {
        Daemon::get_project_environment_diagnostics(self, project_id, environment)
    }

    fn get_project_environment_history(
        &mut self,
        project_id: &str,
        environment: &str,
    ) -> Result<DeploymentHistoryResponse, ErrorResponse> {
        Daemon::get_project_environment_history(self, project_id, environment)
    }

    fn get_project_environment_env(
        &self,
        project_id: &str,
        environment: &str,
    ) -> Result<EnvironmentVariableReport, ErrorResponse> {
        Daemon::get_project_environment_env(self, project_id, environment)
    }

    fn get_project_environment_env_diff(
        &self,
        project_id: &str,
        environment: &str,
        from_generation: u64,
        to_generation: u64,
    ) -> Result<EnvironmentDiffResponse, ErrorResponse> {
        Daemon::get_project_environment_env_diff(
            self,
            project_id,
            environment,
            from_generation,
            to_generation,
        )
    }
}

#[derive(Clone)]
pub struct HttpState {
    daemon: Arc<Mutex<Box<dyn ControlPlane>>>,
    bearer_token: String,
    idempotency: IdempotencyStore,
    github_webhooks: Option<GitHubWebhookState>,
    secret_store: SecretStore,
    project_registry: ProjectRegistryStore,
    web_auth: WebAuthState,
    cli_auth: Option<CliAuthState>,
}

impl HttpState {
    pub fn new(
        daemon: Arc<Mutex<Box<dyn ControlPlane>>>,
        bearer_token: String,
        idempotency: IdempotencyStore,
        github_webhooks: Option<GitHubWebhookState>,
        secret_store: SecretStore,
        project_registry: ProjectRegistryStore,
        web_auth: WebAuthState,
        cli_auth: Option<CliAuthState>,
    ) -> Self {
        Self {
            daemon,
            bearer_token,
            idempotency,
            github_webhooks,
            secret_store,
            project_registry,
            web_auth,
            cli_auth,
        }
    }
}

#[derive(Clone)]
pub struct WebAuthState {
    config: Option<WebAuthConfig>,
    github_oauth: Arc<dyn GitHubOAuthProvider>,
}

impl WebAuthState {
    pub fn from_env() -> Self {
        Self {
            config: load_web_auth_config_from_env(),
            github_oauth: Arc::new(RealGitHubOAuthProvider),
        }
    }

    #[cfg(test)]
    fn unconfigured() -> Self {
        Self {
            config: None,
            github_oauth: Arc::new(MockGitHubOAuthProvider::default()),
        }
    }

    #[cfg(test)]
    fn configured_for_tests(login: &str, user_id: u64) -> Self {
        Self {
            config: Some(WebAuthConfig {
                public_url: "https://forge.example.com".into(),
                client_id: "test-client-id".into(),
                client_secret: "test-client-secret".into(),
                session_secret: "test-session-secret".into(),
                secure_cookies: true,
            }),
            github_oauth: Arc::new(MockGitHubOAuthProvider {
                login: login.into(),
                user_id,
                fail: false,
            }),
        }
    }
}

#[derive(Debug, Clone)]
struct WebAuthConfig {
    public_url: String,
    client_id: String,
    client_secret: String,
    session_secret: String,
    secure_cookies: bool,
}

impl WebAuthConfig {
    fn callback_url(&self) -> String {
        format!(
            "{}/oauth/github/callback",
            self.public_url.trim_end_matches('/')
        )
    }
}

trait GitHubOAuthProvider: Send + Sync {
    fn authenticate(&self, config: &WebAuthConfig, code: &str) -> Result<GitHubUser, String>;
}

struct RealGitHubOAuthProvider;

impl GitHubOAuthProvider for RealGitHubOAuthProvider {
    fn authenticate(&self, config: &WebAuthConfig, code: &str) -> Result<GitHubUser, String> {
        let client = reqwest::blocking::Client::builder()
            .user_agent("forge")
            .build()
            .map_err(|err| err.to_string())?;
        let callback_url = config.callback_url();
        let token = client
            .post(GITHUB_ACCESS_TOKEN_URL)
            .header(header::ACCEPT, "application/json")
            .form(&[
                ("client_id", config.client_id.as_str()),
                ("client_secret", config.client_secret.as_str()),
                ("code", code),
                ("redirect_uri", callback_url.as_str()),
            ])
            .send()
            .map_err(|err| err.to_string())?;
        if !token.status().is_success() {
            return Err(format!(
                "github token exchange failed with {}",
                token.status()
            ));
        }

        let token: GitHubAccessTokenResponse = token.json().map_err(|err| err.to_string())?;
        let user = client
            .get(GITHUB_USER_URL)
            .header(header::ACCEPT, "application/json")
            .bearer_auth(token.access_token)
            .send()
            .map_err(|err| err.to_string())?;
        if !user.status().is_success() {
            return Err(format!("github user lookup failed with {}", user.status()));
        }

        user.json().map_err(|err| err.to_string())
    }
}

#[cfg(test)]
#[derive(Default)]
struct MockGitHubOAuthProvider {
    login: String,
    user_id: u64,
    fail: bool,
}

#[cfg(test)]
impl GitHubOAuthProvider for MockGitHubOAuthProvider {
    fn authenticate(&self, _config: &WebAuthConfig, _code: &str) -> Result<GitHubUser, String> {
        if self.fail {
            return Err("mock oauth failure".into());
        }

        Ok(GitHubUser {
            login: self.login.clone(),
            id: self.user_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GitHubAccessTokenResponse {
    access_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GitHubUser {
    login: String,
    id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OAuthStateQuery {
    code: String,
    state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct OAuthStartQuery {
    next: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OAuthStateCookie {
    nonce: String,
    #[serde(default)]
    return_to: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SessionCookie {
    github_login: String,
    github_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CliTokenClaims {
    github_login: String,
    github_id: u64,
    issued_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CliLoginQuery {
    code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CliLoginApproveForm {
    code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct EnvDiffQuery {
    #[serde(default, rename = "generation")]
    generations: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CliLoginRecord {
    created_at_unix: u64,
    expires_at_unix: u64,
    approved_by_login: Option<String>,
    approved_by_id: Option<u64>,
    approved_at_unix: Option<u64>,
    consumed_at_unix: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliLoginStartRecord {
    code: String,
    expires_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CliLoginPollStatus {
    Pending,
    Approved(String),
    Expired,
}

#[derive(Clone)]
pub struct CliAuthState {
    token_secret: Arc<String>,
    requests: Arc<Mutex<CliLoginStore>>,
}

impl CliAuthState {
    pub fn from_env(root: impl AsRef<Path>) -> Result<Option<Self>, std::io::Error> {
        let Some(token_secret) = std::env::var("FORGE_CLI_TOKEN_SECRET").ok() else {
            return Ok(None);
        };
        Ok(Some(Self {
            token_secret: Arc::new(token_secret),
            requests: Arc::new(Mutex::new(CliLoginStore::new(root, CLI_LOGIN_TTL_SECONDS)?)),
        }))
    }

    #[cfg(test)]
    fn configured_for_tests(root: impl AsRef<Path>) -> Self {
        Self {
            token_secret: Arc::new("test-cli-token-secret".into()),
            requests: Arc::new(Mutex::new(
                CliLoginStore::new(root, CLI_LOGIN_TTL_SECONDS).unwrap(),
            )),
        }
    }

    fn start_request(&self) -> Result<CliLoginStartRecord, String> {
        self.requests
            .lock()
            .map_err(|_| "cli login state lock poisoned".to_string())?
            .create_request()
    }

    fn read_request(&self, code: &str) -> Result<Option<CliLoginRecord>, String> {
        self.requests
            .lock()
            .map_err(|_| "cli login state lock poisoned".to_string())?
            .read(code)
    }

    fn approve_request(&self, code: &str, session: &SessionCookie) -> Result<bool, String> {
        self.requests
            .lock()
            .map_err(|_| "cli login state lock poisoned".to_string())?
            .approve(code, session)
    }

    fn poll_request(&self, code: &str) -> Result<CliLoginPollStatus, String> {
        self.requests
            .lock()
            .map_err(|_| "cli login state lock poisoned".to_string())?
            .poll(code, self.token_secret.as_ref())
    }

    fn verify_token(&self, token: &str) -> bool {
        decode_cli_token(token, self.token_secret.as_ref()).is_some()
    }
}

#[derive(Debug, Clone)]
pub struct GitHubWebhookState {
    config: GitHubWebhookConfig,
    deliveries: DeliveryStore,
}

impl GitHubWebhookState {
    pub fn new(config: GitHubWebhookConfig, deliveries: DeliveryStore) -> Self {
        Self { config, deliveries }
    }
}

#[derive(Debug)]
pub enum HttpError {
    Unauthorized,
    InvalidHeader(&'static str),
    IdempotencyConflict,
    NotFound,
    BadRequest(ErrorResponse),
    Internal(String),
}

impl Display for HttpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized => write!(f, "unauthorized"),
            Self::InvalidHeader(name) => write!(f, "invalid header {name}"),
            Self::IdempotencyConflict => write!(f, "idempotency key conflict"),
            Self::NotFound => write!(f, "not found"),
            Self::BadRequest(err) => write!(f, "{}: {}", err.code, err.message),
            Self::Internal(err) => write!(f, "{err}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SuccessEnvelope<T> {
    request_id: String,
    correlation_id: String,
    data: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ErrorEnvelope {
    request_id: String,
    correlation_id: String,
    code: String,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HealthEnvelope {
    status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IdempotencyRecord {
    fingerprint: String,
    request_id: String,
    accepted: DeploymentAccepted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DeliveryRecord {
    request_id: String,
    result: WebhookResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WebhookResult {
    status: String,
    deployment_id: Option<String>,
    queue_position: Option<usize>,
    reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IdempotencyStore {
    root: PathBuf,
}

impl IdempotencyStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn read(&self, key: &str) -> Result<Option<IdempotencyRecord>, std::io::Error> {
        let path = self.path_for(key);
        if !path.exists() {
            return Ok(None);
        }
        let raw = std::fs::read_to_string(path)?;
        let record = serde_json::from_str(&raw)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
        Ok(Some(record))
    }

    fn write(&self, key: &str, record: &IdempotencyRecord) -> Result<(), std::io::Error> {
        let bytes = serde_json::to_vec(record)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
        atomic_write(self.path_for(key), &bytes)
            .map_err(|err| std::io::Error::other(err.to_string()))
    }

    fn path_for(&self, key: &str) -> PathBuf {
        let sanitized = key
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect::<String>();
        self.root.join(format!("{sanitized}.json"))
    }
}

#[derive(Debug, Clone)]
pub struct DeliveryStore {
    root: PathBuf,
}

impl DeliveryStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn read(&self, delivery_id: &str) -> Result<Option<DeliveryRecord>, std::io::Error> {
        let path = self.path_for(delivery_id);
        if !path.exists() {
            return Ok(None);
        }
        let raw = std::fs::read_to_string(path)?;
        let record = serde_json::from_str(&raw)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
        Ok(Some(record))
    }

    fn write(&self, delivery_id: &str, record: &DeliveryRecord) -> Result<(), std::io::Error> {
        let bytes = serde_json::to_vec(record)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
        atomic_write(self.path_for(delivery_id), &bytes)
            .map_err(|err| std::io::Error::other(err.to_string()))
    }

    fn path_for(&self, delivery_id: &str) -> PathBuf {
        let sanitized = delivery_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect::<String>();
        self.root.join(format!("{sanitized}.json"))
    }
}

#[derive(Debug, Clone)]
struct CliLoginStore {
    root: PathBuf,
    ttl_seconds: u64,
}

impl CliLoginStore {
    fn new(root: impl AsRef<Path>, ttl_seconds: u64) -> Result<Self, std::io::Error> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root, ttl_seconds })
    }

    fn create_request(&mut self) -> Result<CliLoginStartRecord, String> {
        let now = unix_now();
        let record = CliLoginRecord {
            created_at_unix: now,
            expires_at_unix: now + self.ttl_seconds,
            approved_by_login: None,
            approved_by_id: None,
            approved_at_unix: None,
            consumed_at_unix: None,
        };
        let mut attempts = 0usize;
        loop {
            let code = generate_cli_login_code();
            if self.read(&code)?.is_none() {
                self.write(&code, &record)?;
                return Ok(CliLoginStartRecord {
                    code,
                    expires_at_unix: record.expires_at_unix,
                });
            }
            attempts += 1;
            if attempts >= 8 {
                return Err("failed to allocate cli login code".into());
            }
        }
    }

    fn read(&self, code: &str) -> Result<Option<CliLoginRecord>, String> {
        let path = self.path_for(code);
        if !path.exists() {
            return Ok(None);
        }
        let raw = std::fs::read_to_string(path).map_err(|err| err.to_string())?;
        serde_json::from_str(&raw)
            .map(Some)
            .map_err(|err| err.to_string())
    }

    fn write(&self, code: &str, record: &CliLoginRecord) -> Result<(), String> {
        let bytes = serde_json::to_vec(record).map_err(|err| err.to_string())?;
        atomic_write(self.path_for(code), &bytes).map_err(|err| err.to_string())
    }

    fn approve(&mut self, code: &str, session: &SessionCookie) -> Result<bool, String> {
        let Some(mut record) = self.read(code)? else {
            return Ok(false);
        };
        if self.is_expired(&record) || record.consumed_at_unix.is_some() {
            return Ok(false);
        }
        record.approved_by_login = Some(session.github_login.clone());
        record.approved_by_id = Some(session.github_id);
        record.approved_at_unix = Some(unix_now());
        self.write(code, &record)?;
        Ok(true)
    }

    fn poll(&mut self, code: &str, token_secret: &str) -> Result<CliLoginPollStatus, String> {
        let Some(mut record) = self.read(code)? else {
            return Ok(CliLoginPollStatus::Expired);
        };
        if self.is_expired(&record) || record.consumed_at_unix.is_some() {
            return Ok(CliLoginPollStatus::Expired);
        }
        if let (Some(github_login), Some(github_id), Some(_)) = (
            record.approved_by_login.clone(),
            record.approved_by_id,
            record.approved_at_unix,
        ) {
            let token = encode_cli_token(
                &CliTokenClaims {
                    github_login,
                    github_id,
                    issued_at_unix: unix_now(),
                },
                token_secret,
            )?;
            record.consumed_at_unix = Some(unix_now());
            self.write(code, &record)?;
            return Ok(CliLoginPollStatus::Approved(token));
        }
        Ok(CliLoginPollStatus::Pending)
    }

    fn path_for(&self, code: &str) -> PathBuf {
        let sanitized = code
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect::<String>();
        self.root.join(format!("{sanitized}.json"))
    }

    fn is_expired(&self, record: &CliLoginRecord) -> bool {
        unix_now() >= record.expires_at_unix
    }
}

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/", get(get_root))
        .route("/login", get(get_login))
        .route("/styles.css", get(get_styles))
        .route("/app.js", get(get_app_js))
        .route(
            "/login/cli",
            get(get_login_cli).post(post_login_cli_approve),
        )
        .route("/oauth/github/start", get(get_oauth_github_start))
        .route("/oauth/github/callback", get(get_oauth_github_callback))
        .route("/app", get(get_app))
        .route("/logout", get(get_logout).post(post_logout))
        .route("/api/cli-login/start", post(post_cli_login_start))
        .route("/api/cli-login/poll", post(post_cli_login_poll))
        .route("/healthz", get(get_healthz))
        .route("/readyz", get(get_readyz))
        .route("/metrics", get(get_metrics))
        .route("/deployments", post(post_deployments))
        .route("/api/projects", post(post_projects).get(get_projects))
        .route("/api/projects/{project_id}", get(get_project))
        .route(
            "/api/projects/{project_id}/environments/{environment}/status",
            get(get_project_environment_status),
        )
        .route(
            "/api/projects/{project_id}/environments/{environment}/diagnostics",
            get(get_project_environment_diagnostics),
        )
        .route(
            "/api/projects/{project_id}/environments/{environment}/history",
            get(get_project_environment_history),
        )
        .route(
            "/api/projects/{project_id}/environments/{environment}/env",
            get(get_project_environment_env),
        )
        .route(
            "/api/projects/{project_id}/environments/{environment}/env/diff",
            get(get_project_environment_env_diff),
        )
        .route(
            "/api/projects/{project_id}/environments/{environment}/secrets",
            get(get_environment_secrets),
        )
        .route(
            "/api/projects/{project_id}/environments/{environment}/secrets/{key}",
            delete(delete_environment_secret),
        )
        .route("/secrets", post(post_secrets))
        .route("/webhooks/github", post(post_github_webhook))
        .route("/deployments/{id}", get(get_deployment))
        .route("/api/deployments/{id}/logs", get(get_logs))
        .route("/logs/{id}", get(get_logs))
        .route("/events", get(get_events))
        .with_state(state)
}

async fn get_root() -> Response {
    html_response(StatusCode::OK, WEB_INDEX_HTML)
}

async fn get_login(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    if let Some(config) = &state.web_auth.config {
        if read_session_cookie(&headers, config).is_some() {
            return redirect_response("/app");
        }
    }

    let body = if state.web_auth.config.is_some() {
        render_login_page(
            "<a class=\"button\" href=\"/oauth/github/start\">Continue with GitHub</a>",
            "<p class=\"page-note\">CLI and bearer-token API access remain available for automation.</p>",
        )
    } else {
        render_login_page(
            "<p class=\"status-block\">GitHub OAuth login is not configured yet.</p>",
            &format!(
                concat!(
                    "<p class=\"page-note\">Expected env vars:</p>",
                    "<ul class=\"env-list\">",
                    "<li>{}</li>",
                    "<li>{}</li>",
                    "<li>{}</li>",
                    "<li>{}</li>",
                    "</ul>"
                ),
                WEB_LOGIN_REQUIRED_ENV_VARS[0],
                WEB_LOGIN_REQUIRED_ENV_VARS[1],
                WEB_LOGIN_REQUIRED_ENV_VARS[2],
                WEB_LOGIN_REQUIRED_ENV_VARS[3],
            ),
        )
    };

    html_response(StatusCode::OK, body)
}

async fn get_styles() -> Response {
    asset_response(StatusCode::OK, "text/css; charset=utf-8", WEB_STYLES_CSS)
}

async fn get_app_js() -> Response {
    asset_response(
        StatusCode::OK,
        "application/javascript; charset=utf-8",
        WEB_APP_JS,
    )
}

async fn get_login_cli(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(query): Query<CliLoginQuery>,
) -> Response {
    let Some(code) = query.code else {
        return html_response(StatusCode::BAD_REQUEST, "missing cli login code");
    };
    let Some(cli_auth) = state.cli_auth.as_ref() else {
        return html_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "cli login approval is not configured",
        );
    };
    let Some(config) = state.web_auth.config.as_ref() else {
        return redirect_response("/login");
    };
    let return_to = format!("/login/cli?code={code}");
    let Some(record) = cli_auth.read_request(&code).ok().flatten() else {
        return html_response(StatusCode::GONE, "cli login request expired");
    };
    if unix_now() >= record.expires_at_unix {
        return html_response(StatusCode::GONE, "cli login request expired");
    }

    let continue_url = local_url_with_query("/oauth/github/start", &[("next", return_to.as_str())]);
    let body = if let Some(session) = read_session_cookie(&headers, config) {
        let login = escape_html(&session.github_login);
        format!(
            concat!(
                "<!doctype html>\n",
                "<html lang=\"en\">\n",
                "<head>\n",
                "  <meta charset=\"utf-8\">\n",
                "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n",
                "  <title>Approve Forge CLI</title>\n",
                "</head>\n",
                "<body>\n",
                "  <main>\n",
                "    <h1>Approve Forge CLI</h1>\n",
                "    <p>Signed in as {}.</p>\n",
                "    <p>This will issue a Forge CLI token to the waiting terminal.</p>\n",
                "    <form action=\"/login/cli\" method=\"post\">\n",
                "      <input type=\"hidden\" name=\"code\" value=\"{}\">\n",
                "      <button type=\"submit\">Approve CLI Access</button>\n",
                "    </form>\n",
                "  </main>\n",
                "</body>\n",
                "</html>\n",
            ),
            login,
            escape_html(&code)
        )
    } else {
        format!(
            concat!(
                "<!doctype html>\n",
                "<html lang=\"en\">\n",
                "<head>\n",
                "  <meta charset=\"utf-8\">\n",
                "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n",
                "  <title>Forge CLI Login</title>\n",
                "</head>\n",
                "<body>\n",
                "  <main>\n",
                "    <h1>Forge CLI Login</h1>\n",
                "    <p>Sign in with GitHub to approve CLI access for the waiting terminal.</p>\n",
                "    <p><a href=\"{}\">Continue with GitHub</a></p>\n",
                "  </main>\n",
                "</body>\n",
                "</html>\n",
            ),
            escape_html(&continue_url)
        )
    };

    html_response(StatusCode::OK, body)
}

async fn get_oauth_github_start(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(query): Query<OAuthStartQuery>,
) -> Response {
    let Some(config) = state.web_auth.config.as_ref() else {
        return redirect_response("/login");
    };
    if read_session_cookie(&headers, config).is_some() {
        let target = query
            .next
            .as_deref()
            .and_then(sanitize_return_to)
            .unwrap_or("/app");
        return redirect_response(target);
    }

    let nonce = generate_nonce();
    let authorize_url = match Url::parse_with_params(
        GITHUB_AUTHORIZE_URL,
        &[
            ("client_id", config.client_id.as_str()),
            ("redirect_uri", config.callback_url().as_str()),
            ("scope", "read:user"),
            ("state", nonce.as_str()),
        ],
    ) {
        Ok(url) => url.to_string(),
        Err(_) => return html_response(StatusCode::INTERNAL_SERVER_ERROR, "invalid oauth config"),
    };
    let state_cookie = match encode_signed_value(
        &OAuthStateCookie {
            nonce,
            return_to: query
                .next
                .and_then(|value| sanitize_return_to(&value).map(str::to_string)),
        },
        &config.session_secret,
    ) {
        Ok(cookie) => cookie,
        Err(_) => {
            return html_response(StatusCode::INTERNAL_SERVER_ERROR, "invalid oauth config");
        }
    };

    redirect_with_cookies(
        StatusCode::SEE_OTHER,
        &authorize_url,
        &[build_cookie(
            OAUTH_STATE_COOKIE_NAME,
            &state_cookie,
            config.secure_cookies,
            Some(600),
        )],
    )
}

async fn get_oauth_github_callback(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(query): Query<OAuthStateQuery>,
) -> Response {
    let Some(config) = state.web_auth.config.clone() else {
        return redirect_response("/login");
    };
    let clear_state_cookie = build_clear_cookie(OAUTH_STATE_COOKIE_NAME, config.secure_cookies);
    let Some(expected_state) = read_signed_cookie::<OAuthStateCookie>(
        &headers,
        OAUTH_STATE_COOKIE_NAME,
        &config.session_secret,
    ) else {
        return html_response_with_cookies(
            StatusCode::BAD_REQUEST,
            "invalid oauth state",
            &[clear_state_cookie],
        );
    };
    if !bool::from(
        expected_state
            .nonce
            .as_bytes()
            .ct_eq(query.state.as_bytes()),
    ) {
        return html_response_with_cookies(
            StatusCode::BAD_REQUEST,
            "invalid oauth state",
            &[clear_state_cookie],
        );
    }

    let provider = state.web_auth.github_oauth.clone();
    let code = query.code;
    let auth_config = config.clone();
    let user = match tokio::task::spawn_blocking(move || provider.authenticate(&auth_config, &code))
        .await
    {
        Ok(Ok(user)) => user,
        Ok(Err(_)) | Err(_) => {
            return html_response_with_cookies(
                StatusCode::BAD_GATEWAY,
                "github login failed",
                &[clear_state_cookie],
            );
        }
    };

    let session_cookie = match encode_signed_value(
        &SessionCookie {
            github_login: user.login,
            github_id: user.id,
        },
        &config.session_secret,
    ) {
        Ok(cookie) => cookie,
        Err(_) => {
            return html_response_with_cookies(
                StatusCode::INTERNAL_SERVER_ERROR,
                "session creation failed",
                &[clear_state_cookie],
            );
        }
    };

    redirect_with_cookies(
        StatusCode::SEE_OTHER,
        expected_state
            .return_to
            .as_deref()
            .and_then(sanitize_return_to)
            .unwrap_or("/app"),
        &[
            clear_state_cookie,
            build_cookie(
                SESSION_COOKIE_NAME,
                &session_cookie,
                config.secure_cookies,
                None,
            ),
        ],
    )
}

async fn post_login_cli_approve(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Form(form): Form<CliLoginApproveForm>,
) -> Response {
    let Some(cli_auth) = state.cli_auth.as_ref() else {
        return html_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "cli login approval is not configured",
        );
    };
    let Some(config) = state.web_auth.config.as_ref() else {
        return redirect_response("/login");
    };
    let Some(session) = read_session_cookie(&headers, config) else {
        return redirect_response(&format!("/login/cli?code={}", form.code));
    };

    match cli_auth.approve_request(&form.code, &session) {
        Ok(true) => html_response(
            StatusCode::OK,
            concat!(
                "<!doctype html>\n",
                "<html lang=\"en\">\n",
                "<head>\n",
                "  <meta charset=\"utf-8\">\n",
                "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n",
                "  <title>CLI Approved</title>\n",
                "</head>\n",
                "<body>\n",
                "  <main>\n",
                "    <h1>CLI Approved</h1>\n",
                "    <p>You can return to the terminal.</p>\n",
                "  </main>\n",
                "</body>\n",
                "</html>\n",
            ),
        ),
        Ok(false) => html_response(StatusCode::GONE, "cli login request expired"),
        Err(_) => html_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "cli login approval failed",
        ),
    }
}

async fn get_app(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    let Some(config) = state.web_auth.config.as_ref() else {
        return redirect_response("/login");
    };
    let Some(session) = read_session_cookie(&headers, config) else {
        return redirect_response("/login");
    };

    html_response(
        StatusCode::OK,
        WEB_APP_HTML.replace("__GITHUB_LOGIN__", &escape_html(&session.github_login)),
    )
}

async fn get_logout(State(state): State<HttpState>) -> Response {
    logout_response(&state)
}

async fn post_logout(State(state): State<HttpState>) -> Response {
    logout_response(&state)
}

async fn get_healthz() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(HealthEnvelope {
            status: "ok".into(),
        }),
    )
}

async fn post_cli_login_start(State(state): State<HttpState>) -> Response {
    let request_id = next_request_id();
    let Some(cli_auth) = state.cli_auth.as_ref() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &request_id,
            ErrorResponse {
                code: "cli_login_not_configured".into(),
                message: "cli login is not configured".into(),
            },
        );
    };
    if state.web_auth.config.is_none() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &request_id,
            ErrorResponse {
                code: "web_login_not_configured".into(),
                message: "web login is not configured".into(),
            },
        );
    }

    match cli_auth.start_request() {
        Ok(record) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: CliLoginStartResponse {
                    code: record.code,
                    expires_at_unix: record.expires_at_unix,
                    poll_interval_seconds: CLI_LOGIN_POLL_INTERVAL_SECONDS,
                },
            }),
        ),
        Err(err) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &request_id,
            ErrorResponse {
                code: "cli_login_start_failed".into(),
                message: err,
            },
        ),
    }
}

async fn post_cli_login_poll(
    State(state): State<HttpState>,
    Json(request): Json<CliLoginPollRequest>,
) -> Response {
    let request_id = next_request_id();
    let Some(cli_auth) = state.cli_auth.as_ref() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &request_id,
            ErrorResponse {
                code: "cli_login_not_configured".into(),
                message: "cli login is not configured".into(),
            },
        );
    };

    match cli_auth.poll_request(&request.code) {
        Ok(CliLoginPollStatus::Pending) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: CliLoginPollResponse {
                    status: "pending".into(),
                    token: None,
                },
            }),
        ),
        Ok(CliLoginPollStatus::Approved(token)) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: CliLoginPollResponse {
                    status: "approved".into(),
                    token: Some(token),
                },
            }),
        ),
        Ok(CliLoginPollStatus::Expired) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: CliLoginPollResponse {
                    status: "expired".into(),
                    token: None,
                },
            }),
        ),
        Err(err) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &request_id,
            ErrorResponse {
                code: "cli_login_poll_failed".into(),
                message: err,
            },
        ),
    }
}

async fn get_readyz(State(state): State<HttpState>) -> Response {
    let request_id = next_request_id();
    let ready = state
        .daemon
        .lock()
        .map(|daemon| daemon.is_ready())
        .unwrap_or(false);
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    json_response(
        status,
        &request_id,
        Json(HealthEnvelope {
            status: if ready {
                "ready".into()
            } else {
                "not_ready".into()
            },
        }),
    )
}

async fn get_metrics(State(state): State<HttpState>) -> Response {
    let request_id = next_request_id();
    let queue_depth = match state.daemon.lock() {
        Ok(daemon) => match daemon.queue_depth() {
            Ok(queue_depth) => queue_depth,
            Err(err) => return error_response(StatusCode::SERVICE_UNAVAILABLE, &request_id, err),
        },
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "daemon_lock_error".into(),
                    message: "daemon lock poisoned".into(),
                },
            );
        }
    };

    let mut response = (StatusCode::OK, render_prometheus(queue_depth)).into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(&request_id).unwrap(),
    );
    response.headers_mut().insert(
        CORRELATION_ID_HEADER,
        HeaderValue::from_str(&request_id).unwrap(),
    );
    response
}

async fn post_deployments(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<DeploymentRequest>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    let fingerprint = match serde_json::to_string(&request) {
        Ok(value) => value,
        Err(err) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                &request_id,
                ErrorResponse {
                    code: "invalid_request".into(),
                    message: err.to_string(),
                },
            );
        }
    };

    if let Some(key) = header_value(&headers, IDEMPOTENCY_KEY) {
        match state.idempotency.read(&key) {
            Ok(Some(record)) => {
                if record.fingerprint != fingerprint {
                    return error_response(
                        StatusCode::CONFLICT,
                        &request_id,
                        ErrorResponse {
                            code: "idempotency_conflict".into(),
                            message: "idempotency key already used with a different request".into(),
                        },
                    );
                }
                let envelope = SuccessEnvelope {
                    request_id: record.request_id.clone(),
                    correlation_id: record.request_id.clone(),
                    data: record.accepted,
                };
                return json_response(StatusCode::ACCEPTED, &record.request_id, Json(envelope));
            }
            Ok(None) => {}
            Err(err) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &request_id,
                    ErrorResponse {
                        code: "idempotency_store_error".into(),
                        message: err.to_string(),
                    },
                );
            }
        }
    }

    let accepted = {
        let mut daemon = match state.daemon.lock() {
            Ok(daemon) => daemon,
            Err(_) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &request_id,
                    ErrorResponse {
                        code: "daemon_lock_error".into(),
                        message: "daemon lock poisoned".into(),
                    },
                );
            }
        };
        match daemon.handle_post_deployments(request) {
            Ok(accepted) => accepted,
            Err(err) => {
                let status = if err.code == "daemon_not_ready" {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::BAD_REQUEST
                };
                return error_response(status, &request_id, err);
            }
        }
    };

    if let Some(key) = header_value(&headers, IDEMPOTENCY_KEY) {
        let record = IdempotencyRecord {
            fingerprint,
            request_id: request_id.clone(),
            accepted: accepted.clone(),
        };
        if let Err(err) = state.idempotency.write(&key, &record) {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "idempotency_store_error".into(),
                    message: err.to_string(),
                },
            );
        }
    }

    let envelope = SuccessEnvelope {
        request_id: request_id.clone(),
        correlation_id: request_id.clone(),
        data: accepted,
    };
    json_response(StatusCode::ACCEPTED, &request_id, Json(envelope))
}

async fn post_github_webhook(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = next_request_id();
    let Some(github) = state.github_webhooks.clone() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &request_id,
            ErrorResponse {
                code: "github_webhook_not_configured".into(),
                message: "github webhook integration is not configured".into(),
            },
        );
    };

    let Some(delivery_id) = header_value(&headers, X_GITHUB_DELIVERY) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            &request_id,
            ErrorResponse {
                code: "missing_github_delivery".into(),
                message: "missing x-github-delivery header".into(),
            },
        );
    };
    let Some(event) = header_value(&headers, X_GITHUB_EVENT) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            &request_id,
            ErrorResponse {
                code: "missing_github_event".into(),
                message: "missing x-github-event header".into(),
            },
        );
    };
    let Some(signature) = header_value(&headers, X_HUB_SIGNATURE_256) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            &request_id,
            ErrorResponse {
                code: "missing_github_signature".into(),
                message: "missing x-hub-signature-256 header".into(),
            },
        );
    };

    match github.deliveries.read(&delivery_id) {
        Ok(Some(record)) => {
            return json_response(
                StatusCode::ACCEPTED,
                &record.request_id,
                Json(SuccessEnvelope {
                    request_id: record.request_id.clone(),
                    correlation_id: record.request_id.clone(),
                    data: record.result,
                }),
            );
        }
        Ok(None) => {}
        Err(err) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "delivery_store_error".into(),
                    message: err.to_string(),
                },
            );
        }
    }

    if let Err(err) = verify_signature(&github.config.secret, &body, &signature) {
        return github_error_response(&request_id, err);
    }

    let result = match resolve_webhook(&github.config, &event, &body) {
        Ok(WebhookResolution::Ignore { reason }) => WebhookResult {
            status: "ignored".into(),
            deployment_id: None,
            queue_position: None,
            reason: Some(reason),
        },
        Ok(WebhookResolution::Enqueue(request)) => {
            let accepted = {
                let mut daemon = match state.daemon.lock() {
                    Ok(daemon) => daemon,
                    Err(_) => {
                        return error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            &request_id,
                            ErrorResponse {
                                code: "daemon_lock_error".into(),
                                message: "daemon lock poisoned".into(),
                            },
                        );
                    }
                };
                match daemon.handle_post_deployments(request) {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        let status = if err.code == "daemon_not_ready" {
                            StatusCode::SERVICE_UNAVAILABLE
                        } else {
                            StatusCode::BAD_REQUEST
                        };
                        return error_response(status, &request_id, err);
                    }
                }
            };
            WebhookResult {
                status: "accepted".into(),
                deployment_id: Some(accepted.deployment_id),
                queue_position: Some(accepted.queue_position),
                reason: None,
            }
        }
        Err(err) => return github_error_response(&request_id, err),
    };

    if let Err(err) = github.deliveries.write(
        &delivery_id,
        &DeliveryRecord {
            request_id: request_id.clone(),
            result: result.clone(),
        },
    ) {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &request_id,
            ErrorResponse {
                code: "delivery_store_error".into(),
                message: err.to_string(),
            },
        );
    }

    json_response(
        StatusCode::ACCEPTED,
        &request_id,
        Json(SuccessEnvelope {
            request_id: request_id.clone(),
            correlation_id: request_id.clone(),
            data: result,
        }),
    )
}

async fn post_secrets(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<SecretWriteRequest>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    match state.secret_store.write_environment_secret(&request) {
        Ok(result) => json_response(
            StatusCode::CREATED,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: result,
            }),
        ),
        Err(err) => secret_error_response(&request_id, err),
    }
}

async fn post_projects(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<ProjectUpsertRequest>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    let apps_domain = std::env::var("FORGE_APPS_DOMAIN").ok();
    match state
        .project_registry
        .upsert(request, apps_domain.as_deref())
    {
        Ok(project) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: project,
            }),
        ),
        Err(err) => {
            let (status, response) = project_registry_error_response(err);
            error_response(status, &request_id, response)
        }
    }
}

async fn get_projects(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    match state.project_registry.list() {
        Ok(projects) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: ProjectList { projects },
            }),
        ),
        Err(err) => {
            let (status, response) = project_registry_error_response(err);
            error_response(status, &request_id, response)
        }
    }
}

async fn get_project(
    State(state): State<HttpState>,
    headers: HeaderMap,
    AxumPath(project_id): AxumPath<String>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    match state.project_registry.get(&project_id) {
        Ok(Some(project)) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: project,
            }),
        ),
        Ok(None) => error_response(
            StatusCode::NOT_FOUND,
            &request_id,
            ErrorResponse {
                code: "project_not_found".into(),
                message: "project not found".into(),
            },
        ),
        Err(err) => {
            let (status, response) = project_registry_error_response(err);
            error_response(status, &request_id, response)
        }
    }
}

async fn get_project_environment_status(
    State(state): State<HttpState>,
    headers: HeaderMap,
    AxumPath((project_id, environment)): AxumPath<(String, String)>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    let mut daemon = match state.daemon.lock() {
        Ok(daemon) => daemon,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "daemon_lock_error".into(),
                    message: "daemon lock poisoned".into(),
                },
            );
        }
    };

    match daemon.get_project_environment_status(&project_id, &environment) {
        Ok(status) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: status,
            }),
        ),
        Err(err) => {
            let status = match err.code.as_str() {
                "project_not_found" => StatusCode::NOT_FOUND,
                "invalid_environment" => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            error_response(status, &request_id, err)
        }
    }
}

async fn get_deployment(
    State(state): State<HttpState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    let daemon = match state.daemon.lock() {
        Ok(daemon) => daemon,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "daemon_lock_error".into(),
                    message: "daemon lock poisoned".into(),
                },
            );
        }
    };

    match daemon.get_deployment(&id) {
        Ok(Some(status)) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: status,
            }),
        ),
        Ok(None) => error_response(
            StatusCode::NOT_FOUND,
            &request_id,
            ErrorResponse {
                code: "deployment_not_found".into(),
                message: "deployment not found".into(),
            },
        ),
        Err(err) => error_response(StatusCode::BAD_REQUEST, &request_id, err),
    }
}

async fn get_events(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    let daemon = match state.daemon.lock() {
        Ok(daemon) => daemon,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "daemon_lock_error".into(),
                    message: "daemon lock poisoned".into(),
                },
            );
        }
    };

    match daemon.list_events() {
        Ok(events) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: events,
            }),
        ),
        Err(err) => error_response(StatusCode::BAD_REQUEST, &request_id, err),
    }
}

async fn get_logs(
    State(state): State<HttpState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<DeploymentLogsQuery>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    let daemon = match state.daemon.lock() {
        Ok(daemon) => daemon,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "daemon_lock_error".into(),
                    message: "daemon lock poisoned".into(),
                },
            );
        }
    };

    match daemon.get_deployment_logs(&id, params.service.as_deref()) {
        Ok(logs) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: logs,
            }),
        ),
        Err(err) => {
            let status = match err.code.as_str() {
                "deployment_not_found" => StatusCode::NOT_FOUND,
                "service_not_found" => StatusCode::NOT_FOUND,
                _ => StatusCode::BAD_REQUEST,
            };
            error_response(status, &request_id, err)
        }
    }
}

#[derive(Debug, Deserialize)]
struct DeploymentLogsQuery {
    #[serde(default)]
    service: Option<String>,
}

async fn get_project_environment_diagnostics(
    State(state): State<HttpState>,
    headers: HeaderMap,
    AxumPath((project_id, environment)): AxumPath<(String, String)>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    let mut daemon = match state.daemon.lock() {
        Ok(daemon) => daemon,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "daemon_lock_error".into(),
                    message: "daemon lock poisoned".into(),
                },
            );
        }
    };

    match daemon.get_project_environment_diagnostics(&project_id, &environment) {
        Ok(diagnostics) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: diagnostics,
            }),
        ),
        Err(err) => error_response(StatusCode::BAD_REQUEST, &request_id, err),
    }
}

async fn get_project_environment_history(
    State(state): State<HttpState>,
    headers: HeaderMap,
    AxumPath((project_id, environment)): AxumPath<(String, String)>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    let mut daemon = match state.daemon.lock() {
        Ok(daemon) => daemon,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "daemon_lock_error".into(),
                    message: "daemon lock poisoned".into(),
                },
            );
        }
    };

    match daemon.get_project_environment_history(&project_id, &environment) {
        Ok(history) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: history,
            }),
        ),
        Err(err) => {
            let status = match err.code.as_str() {
                "project_not_found" => StatusCode::NOT_FOUND,
                "invalid_environment" => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            error_response(status, &request_id, err)
        }
    }
}

async fn get_project_environment_env(
    State(state): State<HttpState>,
    headers: HeaderMap,
    AxumPath((project_id, environment)): AxumPath<(String, String)>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    let daemon = match state.daemon.lock() {
        Ok(daemon) => daemon,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "daemon_lock_error".into(),
                    message: "daemon lock poisoned".into(),
                },
            );
        }
    };

    match daemon.get_project_environment_env(&project_id, &environment) {
        Ok(report) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: report,
            }),
        ),
        Err(err) => {
            let status = match err.code.as_str() {
                "project_not_found" | "runtime_env_snapshot_unavailable" => StatusCode::NOT_FOUND,
                "invalid_environment" => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            error_response(status, &request_id, err)
        }
    }
}

async fn get_project_environment_env_diff(
    State(state): State<HttpState>,
    headers: HeaderMap,
    AxumPath((project_id, environment)): AxumPath<(String, String)>,
    Query(query): Query<EnvDiffQuery>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }
    if query.generations.len() != 2 {
        return error_response(
            StatusCode::BAD_REQUEST,
            &request_id,
            ErrorResponse {
                code: "invalid_generation_query".into(),
                message: "exactly two generation query parameters are required".into(),
            },
        );
    }

    let daemon = match state.daemon.lock() {
        Ok(daemon) => daemon,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &request_id,
                ErrorResponse {
                    code: "daemon_lock_error".into(),
                    message: "daemon lock poisoned".into(),
                },
            );
        }
    };

    match daemon.get_project_environment_env_diff(
        &project_id,
        &environment,
        query.generations[0],
        query.generations[1],
    ) {
        Ok(diff) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: diff,
            }),
        ),
        Err(err) => {
            let status = match err.code.as_str() {
                "project_not_found" | "runtime_env_snapshot_unavailable" => StatusCode::NOT_FOUND,
                "invalid_environment" | "invalid_generation_query" => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            error_response(status, &request_id, err)
        }
    }
}

async fn get_environment_secrets(
    State(state): State<HttpState>,
    headers: HeaderMap,
    AxumPath((project_id, environment)): AxumPath<(String, String)>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    match state
        .secret_store
        .list_environment_secrets(&project_id, &environment)
    {
        Ok(secrets) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope::<SecretListResponse> {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: secrets,
            }),
        ),
        Err(err) => secret_error_response(&request_id, err),
    }
}

async fn delete_environment_secret(
    State(state): State<HttpState>,
    headers: HeaderMap,
    AxumPath((project_id, environment, key)): AxumPath<(String, String, String)>,
) -> Response {
    let request_id = next_request_id();
    if let Err(response) = ensure_authorized(&state, &headers, &request_id) {
        return response;
    }

    match state
        .secret_store
        .unset_environment_secret(&project_id, &environment, &key)
    {
        Ok(result) => json_response(
            StatusCode::OK,
            &request_id,
            Json(SuccessEnvelope::<SecretUnsetResponse> {
                request_id: request_id.clone(),
                correlation_id: request_id.clone(),
                data: result,
            }),
        ),
        Err(err) => secret_error_response(&request_id, err),
    }
}

fn ensure_authorized(
    state: &HttpState,
    headers: &HeaderMap,
    request_id: &str,
) -> Result<(), Response> {
    let Some(value) = header_value(headers, AUTHORIZATION) else {
        return Err(error_response(
            StatusCode::UNAUTHORIZED,
            request_id,
            ErrorResponse {
                code: "unauthorized".into(),
                message: "missing bearer token".into(),
            },
        ));
    };

    let expected = format!("Bearer {}", state.bearer_token);
    if value == expected {
        return Ok(());
    }

    let cli_authorized = value
        .strip_prefix("Bearer ")
        .and_then(|token| state.cli_auth.as_ref().map(|auth| auth.verify_token(token)))
        .unwrap_or(false);
    if cli_authorized {
        return Ok(());
    }

    Err(error_response(
        StatusCode::UNAUTHORIZED,
        request_id,
        ErrorResponse {
            code: "unauthorized".into(),
            message: "invalid bearer token".into(),
        },
    ))
}

fn github_error_response(request_id: &str, err: GitHubError) -> Response {
    match err {
        GitHubError::InvalidSignature => error_response(
            StatusCode::UNAUTHORIZED,
            request_id,
            ErrorResponse {
                code: "invalid_github_signature".into(),
                message: "invalid github signature".into(),
            },
        ),
        GitHubError::UnsupportedEvent(_) => error_response(
            StatusCode::BAD_REQUEST,
            request_id,
            ErrorResponse {
                code: "unsupported_github_event".into(),
                message: err.to_string(),
            },
        ),
        GitHubError::InvalidPayload(_) => error_response(
            StatusCode::BAD_REQUEST,
            request_id,
            ErrorResponse {
                code: "invalid_github_payload".into(),
                message: err.to_string(),
            },
        ),
        GitHubError::GitCommand(_) | GitHubError::Manifest(_) => error_response(
            StatusCode::BAD_REQUEST,
            request_id,
            ErrorResponse {
                code: "github_manifest_resolution_failed".into(),
                message: err.to_string(),
            },
        ),
    }
}

fn secret_error_response(request_id: &str, err: SecretError) -> Response {
    match err {
        SecretError::MissingMasterKey | SecretError::InvalidMasterKey => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            request_id,
            ErrorResponse {
                code: "secret_store_unavailable".into(),
                message: err.to_string(),
            },
        ),
        SecretError::InvalidRequest(_) => error_response(
            StatusCode::BAD_REQUEST,
            request_id,
            ErrorResponse {
                code: "invalid_secret_request".into(),
                message: err.to_string(),
            },
        ),
        SecretError::MissingSecret(_) => error_response(
            StatusCode::NOT_FOUND,
            request_id,
            ErrorResponse {
                code: "secret_not_found".into(),
                message: err.to_string(),
            },
        ),
        SecretError::Crypto(_) | SecretError::Io(_) => error_response(
            StatusCode::BAD_REQUEST,
            request_id,
            ErrorResponse {
                code: "secret_store_error".into(),
                message: err.to_string(),
            },
        ),
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
}

fn html_response(status: StatusCode, body: impl Into<String>) -> Response {
    asset_response(status, "text/html; charset=utf-8", body)
}

fn asset_response(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<String>,
) -> Response {
    let mut response = (status, body.into()).into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    response
}

fn html_response_with_cookies(
    status: StatusCode,
    body: impl Into<String>,
    cookies: &[String],
) -> Response {
    let mut response = html_response(status, body);
    for cookie in cookies {
        response
            .headers_mut()
            .append(header::SET_COOKIE, HeaderValue::from_str(cookie).unwrap());
    }
    response
}

fn redirect_response(location: &str) -> Response {
    redirect_with_cookies(StatusCode::SEE_OTHER, location, &[])
}

fn redirect_with_cookies(status: StatusCode, location: &str, cookies: &[String]) -> Response {
    let mut response = status.into_response();
    response
        .headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(location).unwrap());
    for cookie in cookies {
        response
            .headers_mut()
            .append(header::SET_COOKIE, HeaderValue::from_str(cookie).unwrap());
    }
    response
}

fn load_web_auth_config_from_env() -> Option<WebAuthConfig> {
    let client_id = std::env::var("FORGE_GITHUB_OAUTH_CLIENT_ID").ok()?;
    let client_secret = std::env::var("FORGE_GITHUB_OAUTH_CLIENT_SECRET").ok()?;
    let public_url = std::env::var("FORGE_PUBLIC_URL").ok()?;
    let session_secret = std::env::var("FORGE_SESSION_SECRET").ok()?;
    let secure_cookies = public_url.starts_with("https://");

    Some(WebAuthConfig {
        public_url,
        client_id,
        client_secret,
        session_secret,
        secure_cookies,
    })
}

fn generate_nonce() -> String {
    let bytes: [u8; 32] = rand::random();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn generate_cli_login_code() -> String {
    let bytes: [u8; 6] = rand::random();
    hex::encode(bytes)
}

fn encode_signed_value<T>(value: &T, secret: &str) -> Result<String, String>
where
    T: Serialize,
{
    let payload = serde_json::to_vec(value).map_err(|err| err.to_string())?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
    let signature = sign_value(secret, &payload)?;
    Ok(format!("{payload}.{signature}"))
}

fn sign_value(secret: &str, payload: &str) -> Result<String, String> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|err| err.to_string())?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn encode_cli_token(claims: &CliTokenClaims, secret: &str) -> Result<String, String> {
    let payload = serde_json::to_vec(claims).map_err(|err| err.to_string())?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
    let signature = sign_value(secret, &payload)?;
    Ok(format!("forge_cli.{payload}.{signature}"))
}

fn decode_cli_token(token: &str, secret: &str) -> Option<CliTokenClaims> {
    let raw = token.strip_prefix("forge_cli.")?;
    let (payload, signature) = raw.rsplit_once('.')?;
    let expected = sign_value(secret, payload).ok()?;
    if !bool::from(expected.as_bytes().ct_eq(signature.as_bytes())) {
        return None;
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn read_signed_cookie<T>(headers: &HeaderMap, name: &str, secret: &str) -> Option<T>
where
    T: DeserializeOwned,
{
    let raw = read_cookie(headers, name)?;
    let (payload, signature) = raw.rsplit_once('.')?;
    let expected = sign_value(secret, payload).ok()?;
    if !bool::from(expected.as_bytes().ct_eq(signature.as_bytes())) {
        return None;
    }

    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn read_session_cookie(headers: &HeaderMap, config: &WebAuthConfig) -> Option<SessionCookie> {
    read_signed_cookie(headers, SESSION_COOKIE_NAME, &config.session_secret)
}

fn read_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|cookie| cookie.split(';'))
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(cookie_name, cookie_value)| {
            if cookie_name == name {
                Some(cookie_value.to_string())
            } else {
                None
            }
        })
}

fn build_cookie(name: &str, value: &str, secure: bool, max_age_seconds: Option<u64>) -> String {
    let mut cookie = format!("{name}={value}; Path=/; HttpOnly; SameSite=Lax");
    if let Some(max_age_seconds) = max_age_seconds {
        cookie.push_str(&format!("; Max-Age={max_age_seconds}"));
    }
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

fn build_clear_cookie(name: &str, secure: bool) -> String {
    build_cookie(name, "", secure, Some(0))
}

fn logout_response(state: &HttpState) -> Response {
    let secure = state
        .web_auth
        .config
        .as_ref()
        .map(|config| config.secure_cookies)
        .unwrap_or(false);
    redirect_with_cookies(
        StatusCode::SEE_OTHER,
        "/login",
        &[
            build_clear_cookie(SESSION_COOKIE_NAME, secure),
            build_clear_cookie(OAUTH_STATE_COOKIE_NAME, secure),
        ],
    )
}

fn sanitize_return_to(value: &str) -> Option<&str> {
    if value.starts_with('/') && !value.starts_with("//") {
        Some(value)
    } else {
        None
    }
}

fn local_url_with_query(path: &str, params: &[(&str, &str)]) -> String {
    let base = format!("http://localhost{path}");
    match Url::parse_with_params(&base, params) {
        Ok(url) => {
            let mut rendered = url.path().to_string();
            if let Some(query) = url.query() {
                rendered.push('?');
                rendered.push_str(query);
            }
            rendered
        }
        Err(_) => path.to_string(),
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn render_login_page(primary: &str, secondary: &str) -> String {
    WEB_LOGIN_HTML
        .replace("__LOGIN_PRIMARY__", primary)
        .replace("__LOGIN_SECONDARY__", secondary)
}

fn json_response<T>(status: StatusCode, request_id: &str, body: Json<T>) -> Response
where
    T: Serialize,
{
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(request_id).unwrap(),
    );
    response.headers_mut().insert(
        CORRELATION_ID_HEADER,
        HeaderValue::from_str(request_id).unwrap(),
    );
    response
}

fn error_response(status: StatusCode, request_id: &str, error: ErrorResponse) -> Response {
    json_response(
        status,
        request_id,
        Json(ErrorEnvelope {
            request_id: request_id.to_string(),
            correlation_id: request_id.to_string(),
            code: error.code,
            message: error.message,
        }),
    )
}

fn next_request_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("req-{seq}")
}

#[cfg(test)]
fn test_root(name: &str) -> PathBuf {
    use std::fs;

    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let base = std::env::temp_dir().join(format!(
        "forge-core-tests-{name}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&base).unwrap();
    base
}

#[cfg(test)]
#[derive(Default)]
struct NoopDockerRuntime;

#[cfg(test)]
impl DockerRuntime for NoopDockerRuntime {
    fn build_image(
        &mut self,
        request: crate::runtime::BuildImageRequest,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        Ok(request.image_tag)
    }

    fn ensure_network(
        &mut self,
        _network_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }

    fn create_container(
        &mut self,
        request: crate::runtime::CreateContainerRequest,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        Ok(request.container_name)
    }

    fn start_container(
        &mut self,
        _container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }

    fn inspect_container(
        &mut self,
        container_name: &str,
    ) -> Result<crate::runtime::ContainerInspection, crate::runtime::DockerRuntimeError> {
        Ok(crate::runtime::ContainerInspection {
            container_name: container_name.to_string(),
            running: true,
            state_status: "running".into(),
            exit_code: Some(0),
            restart_count: 0,
            started_at: None,
            image_ref: "noop".into(),
            labels: Default::default(),
            network_ips: std::collections::BTreeMap::from([(
                "forge-managed".into(),
                "172.18.0.2".into(),
            )]),
            restart_policy: "no".into(),
        })
    }

    fn container_logs(
        &mut self,
        _container_name: &str,
        _tail_lines: usize,
    ) -> Result<String, crate::runtime::DockerRuntimeError> {
        Ok(String::new())
    }

    fn list_managed_containers(
        &mut self,
    ) -> Result<Vec<crate::runtime::ContainerInspection>, crate::runtime::DockerRuntimeError> {
        Ok(Vec::new())
    }

    fn list_managed_images(
        &mut self,
    ) -> Result<Vec<crate::runtime::ManagedImage>, crate::runtime::DockerRuntimeError> {
        Ok(Vec::new())
    }

    fn stop_container(
        &mut self,
        _container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }

    fn remove_container(
        &mut self,
        _container_name: &str,
    ) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }

    fn remove_image(&mut self, _image_ref: &str) -> Result<(), crate::runtime::DockerRuntimeError> {
        Ok(())
    }
}

#[cfg(test)]
#[derive(Default)]
struct NoopRoutingRuntime;

#[cfg(test)]
impl RoutingRuntime for NoopRoutingRuntime {
    fn update_route(
        &mut self,
        _request: crate::runtime::RouteUpdateRequest,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        Ok(())
    }

    fn inspect_route(
        &mut self,
        subtree_id: &str,
    ) -> Result<crate::runtime::RouteInspection, crate::runtime::RoutingRuntimeError> {
        let environment = subtree_id.rsplit(':').next().unwrap_or("production");
        let domain = match environment {
            "staging" => Some("staging-api.example.com".into()),
            "development" => Some("development-api.example.com".into()),
            _ => Some("api.example.com".into()),
        };
        Ok(crate::runtime::RouteInspection {
            subtree_id: subtree_id.to_string(),
            active_target: "172.18.0.2:3000".into(),
            domain,
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
    ) -> Result<Vec<crate::runtime::RouteInspection>, crate::runtime::RoutingRuntimeError> {
        Ok(Vec::new())
    }

    fn remove_route(
        &mut self,
        _subtree_id: &str,
    ) -> Result<(), crate::runtime::RoutingRuntimeError> {
        Ok(())
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct StaticDecider(bool);

#[cfg(test)]
impl crate::convergence::ActiveDeploymentDecider for StaticDecider {
    fn should_resume(&self, _deployment: &crate::queue::DeploymentRecord) -> bool {
        self.0
    }
}

#[cfg(test)]
fn build_state_with_root(ready: bool) -> (HttpState, PathBuf) {
    crate::metrics::reset_for_tests();
    let root = if ready {
        test_root("http-ready")
    } else {
        test_root("http-not-ready").join("missing")
    };
    let config = crate::config::DaemonConfig {
        storage_root: root.clone(),
        api_bind: "127.0.0.1:8080".into(),
        bearer_token: "test-token".into(),
        github_webhook_secret: None,
        repository_cache_root: None,
        sqlite_path: None,
    };
    let mut daemon = Daemon::new(
        config.clone(),
        NoopDockerRuntime,
        NoopRoutingRuntime,
        StaticDecider(true),
    );
    if ready {
        daemon.start().unwrap();
        seed_test_project(&root);
    }
    (
        HttpState::new(
            Arc::new(Mutex::new(Box::new(daemon))),
            config.bearer_token,
            IdempotencyStore::new(root.join("idempotency")).unwrap(),
            None,
            SecretStore::new(root.join("secrets")).unwrap(),
            ProjectRegistryStore::new(&root),
            WebAuthState::unconfigured(),
            None,
        ),
        root,
    )
}

#[cfg(test)]
fn build_state(ready: bool) -> HttpState {
    build_state_with_root(ready).0
}

#[cfg(test)]
fn seed_test_project(root: &Path) {
    use crate::api::ProjectUpsertRequest;

    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git_test(
        root,
        &["init", "--initial-branch=main", repo.to_str().unwrap()],
    );
    std::fs::write(repo.join("README.md"), "ok\n").unwrap();
    git_test(&repo, &["add", "README.md"]);
    git_test(&repo, &["commit", "-m", "initial"]);

    ProjectRegistryStore::new(root)
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
}

#[cfg(test)]
fn seed_project_status_runtime(root: &Path, generation: u64) {
    use crate::storage::{
        EnvironmentPaths, PersistedActivationMode, PersistedRouteTargetSource,
        PersistedRuntimeInfo, PointerStore, RuntimeHealthState, RuntimeState, RuntimeStateStore,
        SnapshotState, SnapshotWriter,
    };

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
        environment_variables: std::collections::BTreeMap::new(),
        source_ref: Some("main".into()),
        repo_url: None,
        commit_sha: Some("340ac8108006d84dbf951d8c0bb04ecfaf0eccac".into()),
        source_path: None,
        services: std::collections::BTreeMap::new(),
        startup_order: Vec::new(),
    })
    .unwrap();
    writer
        .write_artifact("runtime.json", &format!("{runtime}\n"))
        .unwrap();
    writer
        .write_artifact(
            "runtime_env_snapshot.json",
            &format!(
                concat!(
                    "{{\n",
                    "  \"snapshot_version\": 1,\n",
                    "  \"project_id\": \"api\",\n",
                    "  \"environment\": \"staging\",\n",
                    "  \"generation\": {generation},\n",
                    "  \"deployment_id\": \"dep-{generation}\",\n",
                    "  \"source_environment\": \"staging\",\n",
                    "  \"source_ref\": \"main\",\n",
                    "  \"commit_sha\": \"340ac8108006d84dbf951d8c0bb04ecfaf0eccac\",\n",
                    "  \"domain\": \"staging-api.example.com\",\n",
                    "  \"entries\": {{\n",
                    "    \"FORGE_PROJECT_ID\": {{ \"source\": \"forge_generated\", \"value\": \"api\", \"sensitive\": false, \"redacted\": false }}\n",
                    "  }}\n",
                    "}}\n"
                ),
                generation = generation,
            ),
        )
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

#[cfg(test)]
fn git_test(root: &Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .current_dir(root)
        .env("GIT_AUTHOR_NAME", "Forge Tests")
        .env("GIT_AUTHOR_EMAIL", "forge-tests@example.com")
        .env("GIT_COMMITTER_NAME", "Forge Tests")
        .env("GIT_COMMITTER_EMAIL", "forge-tests@example.com")
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(test)]
fn build_cli_login_state() -> HttpState {
    let (mut state, root) = build_state_with_root(true);
    state.web_auth = WebAuthState::configured_for_tests("octocat", 7);
    state.cli_auth = Some(CliAuthState::configured_for_tests(root.join("cli-logins")));
    state
}

#[cfg(test)]
pub mod http_requires_bearer_token {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn post_without_bearer_token_is_unauthorized() {
        let app = router(build_state(true));
        let request = Request::builder()
            .method(axum::http::Method::POST)
            .uri("/deployments")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"project_id":"api","environment":"production","intent":"deploy"}"#,
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}

#[cfg(test)]
pub mod root_serves_web_index {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn root_endpoint_returns_html() {
        let app = router(build_state(true));
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "text/html; charset=utf-8"
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Forge Runtime"));
        assert!(body.contains("/styles.css"));
    }
}

#[cfg(test)]
pub mod login_serves_web_login {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn login_endpoint_returns_html() {
        let app = router(build_state(true));
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/login")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "text/html; charset=utf-8"
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Forge Login"));
        assert!(body.contains("/styles.css"));
    }
}

#[cfg(test)]
pub mod login_endpoint_mentions_missing_oauth_config_when_unconfigured {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn login_endpoint_mentions_missing_oauth_config_when_unconfigured() {
        let app = router(build_state(true));
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/login")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains("GitHub OAuth login is not configured yet"));
        for key in WEB_LOGIN_REQUIRED_ENV_VARS {
            assert!(body.contains(key));
        }
    }
}

#[cfg(test)]
pub mod cli_login_start_creates_pending_request {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn cli_login_start_creates_pending_request() {
        let app = router(build_cli_login_state());
        let request = Request::builder()
            .method(axum::http::Method::POST)
            .uri("/api/cli-login/start")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["poll_interval_seconds"], 1);
        assert_eq!(json["data"]["code"].as_str().unwrap().len(), 12);
    }
}

#[cfg(test)]
pub mod login_endpoint_shows_continue_with_github_when_configured {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn login_endpoint_shows_continue_with_github_when_configured() {
        let mut state = build_state(true);
        state.web_auth = WebAuthState::configured_for_tests("octocat", 1);
        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/login")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains("Continue with GitHub"));
    }
}

#[cfg(test)]
pub mod oauth_start_redirects_to_github {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn oauth_start_redirects_to_github() {
        let mut state = build_state(true);
        state.web_auth = WebAuthState::configured_for_tests("octocat", 1);
        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/oauth/github/start")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert!(
            response
                .headers()
                .get(header::LOCATION)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(GITHUB_AUTHORIZE_URL)
        );
        assert!(
            response
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|value| {
                    value
                        .to_str()
                        .unwrap()
                        .starts_with(&format!("{OAUTH_STATE_COOKIE_NAME}="))
                })
        );
    }
}

#[cfg(test)]
pub mod login_cli_requires_session_to_approve {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn login_cli_requires_session_to_approve() {
        let app = router(build_cli_login_state());
        let start = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/api/cli-login/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(start.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let code = json["data"]["code"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/login/cli")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from(format!("code={code}")))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            &format!("/login/cli?code={code}")
        );
    }
}

#[cfg(test)]
pub mod login_cli_approve_marks_request_approved {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn login_cli_approve_marks_request_approved() {
        let state = build_cli_login_state();
        let config = state.web_auth.config.clone().unwrap();
        let session_cookie = encode_signed_value(
            &SessionCookie {
                github_login: "octocat".into(),
                github_id: 7,
            },
            &config.session_secret,
        )
        .unwrap();
        let app = router(state);
        let start = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/api/cli-login/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(start.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let code = json["data"]["code"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/login/cli")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .header(
                        header::COOKIE,
                        format!("{SESSION_COOKIE_NAME}={session_cookie}"),
                    )
                    .body(Body::from(format!("code={code}")))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[cfg(test)]
pub mod cli_login_poll_returns_pending_before_approval {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn cli_login_poll_returns_pending_before_approval() {
        let app = router(build_cli_login_state());
        let start = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/api/cli-login/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(start.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let code = json["data"]["code"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/api/cli-login/poll")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(r#"{{"code":"{code}"}}"#)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["status"], "pending");
        assert!(json["data"]["token"].is_null());
    }
}

#[cfg(test)]
pub mod cli_login_poll_returns_token_once_after_approval {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn cli_login_poll_returns_token_once_after_approval() {
        let state = build_cli_login_state();
        let config = state.web_auth.config.clone().unwrap();
        let session_cookie = encode_signed_value(
            &SessionCookie {
                github_login: "octocat".into(),
                github_id: 7,
            },
            &config.session_secret,
        )
        .unwrap();
        let app = router(state);
        let start = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/api/cli-login/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(start.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let code = json["data"]["code"].as_str().unwrap().to_string();

        let _approved = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/login/cli")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .header(
                        header::COOKIE,
                        format!("{SESSION_COOKIE_NAME}={session_cookie}"),
                    )
                    .body(Body::from(format!("code={code}")))
                    .unwrap(),
            )
            .await
            .unwrap();

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/api/cli-login/poll")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(r#"{{"code":"{code}"}}"#)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["data"]["status"], "approved");
        assert!(
            first_json["data"]["token"]
                .as_str()
                .unwrap()
                .starts_with("forge_cli.")
        );

        let second = app
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/api/cli-login/poll")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(r#"{{"code":"{code}"}}"#)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["data"]["status"], "expired");
        assert!(second_json["data"]["token"].is_null());
    }
}

#[cfg(test)]
pub mod oauth_callback_creates_session_cookie {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn oauth_callback_creates_session_cookie() {
        let mut state = build_state(true);
        state.web_auth = WebAuthState::configured_for_tests("octocat", 7);
        let config = state.web_auth.config.clone().unwrap();
        let app = router(state);
        let state_value = "test-state";
        let state_cookie = encode_signed_value(
            &OAuthStateCookie {
                nonce: state_value.into(),
                return_to: None,
            },
            &config.session_secret,
        )
        .unwrap();

        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri(format!(
                "/oauth/github/callback?code=test-code&state={state_value}"
            ))
            .header(
                header::COOKIE,
                format!("{OAUTH_STATE_COOKIE_NAME}={state_cookie}"),
            )
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/app");
        assert!(
            response
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|value| {
                    value
                        .to_str()
                        .unwrap()
                        .starts_with(&format!("{SESSION_COOKIE_NAME}="))
                })
        );
    }
}

#[cfg(test)]
pub mod app_requires_valid_session {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn app_requires_session() {
        let mut state = build_state(true);
        state.web_auth = WebAuthState::configured_for_tests("octocat", 7);
        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/app")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/login");
    }
}

#[cfg(test)]
pub mod static_assets_do_not_expose_secrets {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn static_assets_do_not_expose_secrets() {
        let app = router(build_cli_login_state());

        for path in ["/styles.css", "/app.js"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(axum::http::Method::GET)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(!body.contains("test-session-secret"));
            assert!(!body.contains("forge_session"));
            assert!(!body.contains("FORGE_SESSION_SECRET"));
        }
    }
}

#[cfg(test)]
pub mod app_page_preserves_auth_gate {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn app_page_preserves_auth_gate() {
        let state = build_cli_login_state();
        let config = state.web_auth.config.clone().unwrap();
        let session_cookie = encode_signed_value(
            &SessionCookie {
                github_login: "octocat".into(),
                github_id: 7,
            },
            &config.session_secret,
        )
        .unwrap();
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::GET)
                    .uri("/app")
                    .header(
                        header::COOKIE,
                        format!("{SESSION_COOKIE_NAME}={session_cookie}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Forge Control"));
        assert!(body.contains("octocat"));
        assert!(body.contains("/app.js"));
    }
}

#[cfg(test)]
pub mod logout_clears_session_cookie {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn logout_clears_session_cookie() {
        let mut state = build_state(true);
        state.web_auth = WebAuthState::configured_for_tests("octocat", 7);
        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::POST)
            .uri("/logout")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/login");
        assert!(
            response
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|value| {
                    let value = value.to_str().unwrap();
                    value.starts_with(&format!("{SESSION_COOKIE_NAME}="))
                        && value.contains("Max-Age=0")
                })
        );
    }
}

#[cfg(test)]
pub mod http_post_deployments_enqueues_job {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn valid_post_enqueues_job() {
        let app = router(build_state(true));
        let request = Request::builder()
            .method(axum::http::Method::POST)
            .uri("/deployments")
            .header("content-type", "application/json")
            .header("authorization", "Bearer test-token")
            .body(Body::from(
                r#"{"project_id":"api","environment":"production","intent":"deploy"}"#,
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
}

#[cfg(test)]
pub mod http_idempotency_key_replays_same_response {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn repeated_request_with_same_key_replays_same_deployment_id() {
        let app = router(build_state(true));
        let request = || {
            Request::builder()
                .method(axum::http::Method::POST)
                .uri("/deployments")
                .header("content-type", "application/json")
                .header("authorization", "Bearer test-token")
                .header("idempotency-key", "same-key")
                .body(Body::from(
                    r#"{"project_id":"api","environment":"production","intent":"deploy"}"#,
                ))
                .unwrap()
        };

        let first = app.clone().oneshot(request()).await.unwrap();
        let second = app.oneshot(request()).await.unwrap();

        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();

        assert_eq!(first_body, second_body);
    }
}

#[cfg(test)]
pub mod http_readyz_false_before_daemon_ready {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn readyz_returns_service_unavailable_before_ready() {
        let app = router(build_state(false));
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[cfg(test)]
pub mod http_error_response_is_machine_readable {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn unauthorized_response_contains_code_and_message() {
        let app = router(build_state(true));
        let request = Request::builder()
            .method(axum::http::Method::POST)
            .uri("/deployments")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"project_id":"api","environment":"production","intent":"deploy"}"#,
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("code").is_some());
        assert!(json.get("message").is_some());
        assert!(json.get("request_id").is_some());
    }
}

#[cfg(test)]
pub mod metrics_endpoint_exposes_prometheus_text {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn metrics_endpoint_exposes_prometheus_text() {
        let app = router(build_state(true));
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "text/plain; version=0.0.4"
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("forge_deployments_total "));
        assert!(body.contains("forge_deployments_failed_total "));
        assert!(body.contains("forge_deployments_rollback_total "));
        assert!(body.contains("forge_queue_depth 0"));
    }
}

#[cfg(test)]
pub mod metrics_report_queue_depth {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn metrics_report_queue_depth() {
        let app = router(build_state(true));
        let deploy_request = Request::builder()
            .method(axum::http::Method::POST)
            .uri("/deployments")
            .header("content-type", "application/json")
            .header("authorization", "Bearer test-token")
            .body(Body::from(
                r#"{"project_id":"api","environment":"production","intent":"deploy"}"#,
            ))
            .unwrap();
        let deploy_response = app.clone().oneshot(deploy_request).await.unwrap();
        assert_eq!(deploy_response.status(), StatusCode::ACCEPTED);

        let metrics_request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(metrics_request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("forge_queue_depth 1"));
    }
}

#[cfg(test)]
pub mod logs_endpoint_is_bounded {
    use super::*;
    use crate::events::EventRecord;
    use crate::storage::{DiagnosticsStore, EnvironmentPaths, EventStore};
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn logs_endpoint_is_bounded() {
        let (state, root) = build_state_with_root(true);
        let env = EnvironmentPaths::new(&root, "api", "production");
        let events = EventStore::new(env.clone(), 1);
        events
            .append(&EventRecord {
                timestamp_unix: 1,
                project_id: "api".into(),
                environment: "production".into(),
                generation: Some(1),
                deployment_id: Some("dep-logs-bounded".into()),
                event_type: "DEPLOYMENT_STARTED".into(),
                reason: None,
            })
            .unwrap();
        let diagnostics = DiagnosticsStore::new(env, 1);
        for idx in 0..200 {
            diagnostics
                .append_log_line(&format!("line-{idx}"), &[])
                .unwrap();
        }

        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/logs/dep-logs-bounded")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let lines = json["data"]["lines"].as_array().unwrap();
        assert!(lines.len() <= 64);
        assert_eq!(lines.last().unwrap(), "line-199");
        assert_ne!(lines.first().unwrap(), "line-0");
    }
}

#[cfg(test)]
pub mod deployment_diagnostics_endpoints {
    use super::*;
    use crate::storage::{DiagnosticsStore, EnvironmentPaths};
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use tower::util::ServiceExt;

    fn write_multiservice_logs_fixture(root: &std::path::Path, include_service_logs: bool) {
        let env = EnvironmentPaths::new(root, "api", "staging");
        let writer = crate::storage::SnapshotWriter::new(env.clone(), 1).unwrap();
        writer
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-ms-logs-1\",\n  \"image_ref\": \"forge/api:staging-gen-1\"\n}\n",
            )
            .unwrap();
        writer
            .write_artifact(
                "runtime.json",
                concat!(
                    "{\n",
                    "  \"container_name\": \"staging-api-gen-1\",\n",
                    "  \"running\": true,\n",
                    "  \"services\": {\n",
                    "    \"api\": {\n",
                    "      \"service_id\": \"api\",\n",
                    "      \"container_name\": \"staging-api-api-gen-1\",\n",
                    "      \"image_ref\": \"forge/api:staging-gen-1\",\n",
                    "      \"running\": true,\n",
                    "      \"externally_exposed\": true,\n",
                    "      \"activation\": {\"Http\": {\"internal_port\": 3000, \"route_subtree_id\": \"forge:api:staging:api\", \"target_source\": \"ContainerIp\"}}\n",
                    "    },\n",
                    "    \"worker\": {\n",
                    "      \"service_id\": \"worker\",\n",
                    "      \"container_name\": \"staging-api-worker-gen-1\",\n",
                    "      \"image_ref\": \"forge/worker:staging-gen-1\",\n",
                    "      \"running\": true,\n",
                    "      \"depends_on\": [\"api\"],\n",
                    "      \"activation\": \"Direct\"\n",
                    "    }\n",
                    "  },\n",
                    "  \"startup_order\": [\"api\", \"worker\"],\n",
                    "  \"activation\": {\"Http\": {\"internal_port\": 3000, \"route_subtree_id\": \"forge:api:staging\", \"target_source\": \"ContainerIp\"}},\n",
                    "  \"environment_variables\": {}\n",
                    "}\n"
                ),
            )
            .unwrap();
        writer
            .finalize("api", "staging", crate::storage::SnapshotState::Healthy)
            .unwrap();
        let diagnostics = DiagnosticsStore::new(env, 1);
        diagnostics
            .append_log_line("generation promoted", &[])
            .unwrap();
        if include_service_logs {
            diagnostics
                .write_artifact("services/api/container_logs_tail.log", "api ready\n", &[])
                .unwrap();
            diagnostics
                .write_artifact(
                    "services/worker/container_logs_tail.log",
                    "worker polling\n",
                    &[],
                )
                .unwrap();
        }
    }

    #[tokio::test]
    async fn logs_returns_persisted_deployment_log() {
        let (state, root) = build_state_with_root(true);
        let env = EnvironmentPaths::new(&root, "api", "staging");
        let writer = crate::storage::SnapshotWriter::new(env.clone(), 1).unwrap();
        writer
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-logs-1\",\n  \"image_ref\": \"forge/api:staging-gen-1\"\n}\n",
            )
            .unwrap();
        writer
            .finalize("api", "staging", crate::storage::SnapshotState::Healthy)
            .unwrap();
        let diagnostics = DiagnosticsStore::new(env, 1);
        diagnostics.append_log_line("image built", &[]).unwrap();
        diagnostics
            .append_log_line("generation promoted", &[])
            .unwrap();
        diagnostics
            .write_artifact(
                "container_logs_tail.log",
                "Server is running on 0.0.0.0:3000\n",
                &[],
            )
            .unwrap();

        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/api/deployments/dep-logs-1/logs")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["project_id"], "api");
        assert_eq!(json["data"]["environment"], "staging");
        assert_eq!(json["data"]["lifecycle"][0], "image built");
        assert_eq!(
            json["data"]["container_logs"][0],
            "Server is running on 0.0.0.0:3000"
        );
    }

    #[tokio::test]
    async fn logs_redacts_sensitive_values() {
        let (state, root) = build_state_with_root(true);
        let env = EnvironmentPaths::new(&root, "api", "staging");
        let writer = crate::storage::SnapshotWriter::new(env.clone(), 1).unwrap();
        writer
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-redacted-1\",\n  \"image_ref\": \"forge/api:staging-gen-1\"\n}\n",
            )
            .unwrap();
        writer
            .finalize("api", "staging", crate::storage::SnapshotState::Failed)
            .unwrap();
        let diagnostics = DiagnosticsStore::new(env, 1);
        diagnostics
            .append_log_line("Authorization: [REDACTED]", &[])
            .unwrap();
        diagnostics
            .write_artifact("container_logs_tail.log", "Bearer [REDACTED]\n", &[])
            .unwrap();

        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/api/deployments/dep-redacted-1/logs")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let rendered = json["data"].to_string();
        assert!(!rendered.contains("Authorization: secret"));
        assert!(!rendered.contains("Bearer token"));
        assert!(rendered.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn diagnostics_api_matches_persisted_artifacts() {
        let (state, root) = build_state_with_root(true);
        let env = EnvironmentPaths::new(&root, "api", "staging");
        let writer = crate::storage::SnapshotWriter::new(env.clone(), 1).unwrap();
        writer
            .write_artifact(
                "build.json",
                "{\n  \"deployment_id\": \"dep-diag-1\",\n  \"image_ref\": \"forge/api:staging-gen-1\",\n  \"source_ref\": \"main\"\n}\n",
            )
            .unwrap();
        writer
            .write_artifact(
                "runtime.json",
                "{\n  \"container_name\": \"staging-api-gen-1\",\n  \"running\": true,\n  \"network_name\": \"forge-managed\",\n  \"probe_path\": \"/health\",\n  \"activation\": {\"Http\": {\"internal_port\": 3000, \"route_subtree_id\": \"forge:api:staging\", \"target_source\": \"ContainerIp\"}},\n  \"environment_variables\": {}\n}\n",
            )
            .unwrap();
        writer
            .finalize("api", "staging", crate::storage::SnapshotState::Healthy)
            .unwrap();
        crate::storage::PointerStore::new(env.clone())
            .swap_current(1)
            .unwrap();
        crate::storage::RuntimeStateStore::new(env.clone())
            .save(&crate::storage::RuntimeState {
                active_generation: Some(1),
                health_state: crate::storage::RuntimeHealthState::Healthy,
                failed_probe_count: 0,
                successful_probe_count: 1,
                restart_attempted: false,
                degraded_since_unix: None,
                last_transition: "healthy".into(),
                last_error_code: None,
            })
            .unwrap();
        let diagnostics = DiagnosticsStore::new(env.clone(), 1);
        diagnostics
            .write_summary(&crate::storage::DiagnosticSummary {
                deployment_id: Some("dep-diag-1".into()),
                failure_stage: "validating_runtime".into(),
                failure_reason: "http health probe failed".into(),
                container_name: "staging-api-gen-1".into(),
                failed_service_name: None,
                probe_target_host: Some("172.18.0.2".into()),
                probe_target_port: Some(3000),
                probe_target_path: Some("/health".into()),
                cleanup_recorded: true,
                dependency_graph_summary: None,
                runtime_env_preview: Vec::new(),
            })
            .unwrap();
        diagnostics
            .write_artifact(
                "validation_failure.json",
                "{\n  \"probe_target\": {\"host\": \"172.18.0.2\", \"port\": 3000, \"path\": \"/health\"},\n  \"last_error\": \"http health probe returned unhealthy\"\n}\n",
                &[],
            )
            .unwrap();

        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/api/projects/api/environments/staging/diagnostics")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["project_id"], "api");
        assert_eq!(json["data"]["active_generation"], 1);
        assert_eq!(json["data"]["probe_target"]["path"], "/health");
        assert_eq!(
            json["data"]["recent_failures"][0]["failure_reason"],
            "http health probe failed"
        );
        assert_eq!(
            json["data"]["latest_validation_failure"]["last_error"],
            "http health probe returned unhealthy"
        );
    }

    #[tokio::test]
    async fn logs_grouped_multiservice_logs_by_default() {
        let (state, root) = build_state_with_root(true);
        write_multiservice_logs_fixture(&root, true);

        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/api/deployments/dep-ms-logs-1/logs")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["services"].as_array().unwrap().len(), 2);
        assert_eq!(json["data"]["services"][0]["service_id"], "api");
        assert_eq!(json["data"]["services"][0]["lines"][0], "api ready");
        assert_eq!(json["data"]["services"][1]["service_id"], "worker");
        assert_eq!(json["data"]["services"][1]["lines"][0], "worker polling");
    }

    #[tokio::test]
    async fn logs_service_filter_returns_only_requested_service() {
        let (state, root) = build_state_with_root(true);
        write_multiservice_logs_fixture(&root, true);

        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/api/deployments/dep-ms-logs-1/logs?service=worker")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["selected_service"], "worker");
        assert_eq!(json["data"]["services"].as_array().unwrap().len(), 1);
        assert_eq!(json["data"]["services"][0]["service_id"], "worker");
        assert_eq!(json["data"]["services"][0]["lines"][0], "worker polling");
    }

    #[tokio::test]
    async fn logs_invalid_service_returns_service_not_found() {
        let (state, root) = build_state_with_root(true);
        write_multiservice_logs_fixture(&root, true);

        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/api/deployments/dep-ms-logs-1/logs?service=cron")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "service_not_found");
    }

    #[tokio::test]
    async fn legacy_multiservice_generation_reports_service_logs_unavailable() {
        let (state, root) = build_state_with_root(true);
        write_multiservice_logs_fixture(&root, false);

        let app = router(state);
        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/api/deployments/dep-ms-logs-1/logs?service=api")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["data"]["services"][0]["lines"][0],
            "service logs unavailable for this generation"
        );
    }
}

#[cfg(test)]
pub mod project_registry_endpoints_round_trip {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn project_registry_endpoints_round_trip() {
        let app = router(build_state(true));
        let create = Request::builder()
            .method(axum::http::Method::POST)
            .uri("/api/projects")
            .header("content-type", "application/json")
            .header("authorization", "Bearer test-token")
            .body(Body::from(
                r#"{"project_id":"api","repo_url":"https://github.com/example/api.git","default_branch":"main","base_domain":"api.example.com"}"#,
            ))
            .unwrap();
        let create_response = app.clone().oneshot(create).await.unwrap();
        assert_eq!(create_response.status(), StatusCode::OK);

        let list = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/api/projects")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();
        let list_response = app.clone().oneshot(list).await.unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);
        let list_body = to_bytes(list_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list_json: Value = serde_json::from_slice(&list_body).unwrap();
        assert_eq!(list_json["data"]["projects"][0]["project_id"], "api");

        let show = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/api/projects/api")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();
        let show_response = app.oneshot(show).await.unwrap();
        assert_eq!(show_response.status(), StatusCode::OK);
        let show_body = to_bytes(show_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let show_json: Value = serde_json::from_slice(&show_body).unwrap();
        assert_eq!(show_json["data"]["base_domain"], "api.example.com");
        assert_eq!(show_json["data"]["domain_mode"], "explicit");
    }
}

#[cfg(test)]
pub mod project_environment_status_endpoint_reports_runtime_truth {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde_json::Value;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn project_environment_status_endpoint_reports_runtime_truth() {
        let (state, root) = build_state_with_root(true);
        seed_project_status_runtime(&root, 7);
        let app = router(state);

        let request = Request::builder()
            .method(axum::http::Method::GET)
            .uri("/api/projects/api/environments/staging/status")
            .header("authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["project_id"], "api");
        assert_eq!(json["data"]["environment"], "staging");
        assert_eq!(json["data"]["status"], "healthy");
        assert_eq!(json["data"]["active_generation"], 7);
        assert_eq!(json["data"]["domain"], "staging-api.example.com");
        assert_eq!(json["data"]["container_running"], true);
        assert_eq!(json["data"]["route_active"], true);
    }
}
