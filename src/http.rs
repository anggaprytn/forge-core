use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::api::{DeploymentAccepted, DeploymentRequest, DeploymentStatus, ErrorResponse, EventList};
use crate::daemon::{Daemon, DaemonState};
use crate::runtime::{DockerRuntime, RoutingRuntime};
use crate::storage::atomic_write;

const AUTHORIZATION: &str = "authorization";
const IDEMPOTENCY_KEY: &str = "idempotency-key";
const REQUEST_ID_HEADER: &str = "x-request-id";
const CORRELATION_ID_HEADER: &str = "x-correlation-id";

pub trait ControlPlane: Send {
    fn is_ready(&self) -> bool;
    fn handle_post_deployments(
        &mut self,
        request: DeploymentRequest,
    ) -> Result<DeploymentAccepted, ErrorResponse>;
    fn get_deployment(&self, deployment_id: &str) -> Result<Option<DeploymentStatus>, ErrorResponse>;
    fn list_events(&self) -> Result<EventList, ErrorResponse>;
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

    fn get_deployment(&self, deployment_id: &str) -> Result<Option<DeploymentStatus>, ErrorResponse> {
        Daemon::get_deployment(self, deployment_id)
    }

    fn list_events(&self) -> Result<EventList, ErrorResponse> {
        Daemon::list_events(self)
    }
}

#[derive(Clone)]
pub struct HttpState {
    daemon: Arc<Mutex<Box<dyn ControlPlane>>>,
    bearer_token: String,
    idempotency: IdempotencyStore,
}

impl HttpState {
    pub fn new(
        daemon: Arc<Mutex<Box<dyn ControlPlane>>>,
        bearer_token: String,
        idempotency: IdempotencyStore,
    ) -> Self {
        Self {
            daemon,
            bearer_token,
            idempotency,
        }
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

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
struct SuccessEnvelope<T> {
    request_id: String,
    correlation_id: String,
    data: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
struct ErrorEnvelope {
    request_id: String,
    correlation_id: String,
    code: String,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
struct HealthEnvelope {
    status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
struct IdempotencyRecord {
    fingerprint: String,
    request_id: String,
    accepted: DeploymentAccepted,
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

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/healthz", get(get_healthz))
        .route("/readyz", get(get_readyz))
        .route("/deployments", post(post_deployments))
        .route("/deployments/{id}", get(get_deployment))
        .route("/events", get(get_events))
        .with_state(state)
}

async fn get_healthz() -> impl IntoResponse {
    (StatusCode::OK, Json(HealthEnvelope { status: "ok".into() }))
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
            status: if ready { "ready".into() } else { "not_ready".into() },
        }),
    )
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

fn ensure_authorized(state: &HttpState, headers: &HeaderMap, request_id: &str) -> Result<(), Response> {
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
    if value != expected {
        return Err(error_response(
            StatusCode::UNAUTHORIZED,
            request_id,
            ErrorResponse {
                code: "unauthorized".into(),
                message: "invalid bearer token".into(),
            },
        ));
    }

    Ok(())
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
}

fn json_response<T>(status: StatusCode, request_id: &str, body: Json<T>) -> Response
where
    T: Serialize,
{
    let mut response = (status, body).into_response();
    response
        .headers_mut()
        .insert(REQUEST_ID_HEADER, HeaderValue::from_str(request_id).unwrap());
    response
        .headers_mut()
        .insert(CORRELATION_ID_HEADER, HeaderValue::from_str(request_id).unwrap());
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
            image_ref: "noop".into(),
            labels: Default::default(),
            restart_policy: "no".into(),
        })
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
        Ok(crate::runtime::RouteInspection {
            subtree_id: subtree_id.to_string(),
            active_target: String::new(),
            activation_verified: true,
            health_checks_enabled: false,
        })
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
fn build_state(ready: bool) -> HttpState {
    let root = if ready {
        test_root("http-ready")
    } else {
        test_root("http-not-ready").join("missing")
    };
    let config = crate::config::DaemonConfig {
        storage_root: root.clone(),
        api_bind: "127.0.0.1:8080".into(),
        bearer_token: "test-token".into(),
        sqlite_path: None,
    };
    let mut daemon = Daemon::new(config.clone(), NoopDockerRuntime, NoopRoutingRuntime, StaticDecider(true));
    if ready {
        daemon.start().unwrap();
    }
    HttpState::new(
        Arc::new(Mutex::new(Box::new(daemon))),
        config.bearer_token,
        IdempotencyStore::new(root.join("idempotency")).unwrap(),
    )
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
    use axum::body::{to_bytes, Body};
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
    use axum::body::{to_bytes, Body};
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
