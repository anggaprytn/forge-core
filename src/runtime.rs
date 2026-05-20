use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildImageRequest {
    pub image_tag: String,
    pub context_path: PathBuf,
    pub dockerfile_path: PathBuf,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateContainerRequest {
    pub container_name: String,
    pub image_ref: String,
    pub labels: BTreeMap<String, String>,
    pub environment: BTreeMap<String, String>,
    pub network_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerInspection {
    pub container_name: String,
    pub running: bool,
    pub state_status: String,
    pub exit_code: Option<i32>,
    pub image_ref: String,
    pub labels: BTreeMap<String, String>,
    pub network_ips: BTreeMap<String, String>,
    pub restart_policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedImage {
    pub image_ref: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DockerRuntimeError {
    CommandFailed(String),
    InvalidResponse(String),
}

impl Display for DockerRuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommandFailed(message) => write!(f, "{message}"),
            Self::InvalidResponse(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for DockerRuntimeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeError {
    Failed(String),
}

impl Display for ProbeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ProbeError {}

pub trait DockerRuntime {
    fn build_image(&mut self, request: BuildImageRequest) -> Result<String, DockerRuntimeError>;
    fn ensure_network(&mut self, network_name: &str) -> Result<(), DockerRuntimeError>;
    fn create_container(
        &mut self,
        request: CreateContainerRequest,
    ) -> Result<String, DockerRuntimeError>;
    fn start_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError>;
    fn inspect_container(
        &mut self,
        container_name: &str,
    ) -> Result<ContainerInspection, DockerRuntimeError>;
    fn container_logs(
        &mut self,
        container_name: &str,
        tail_lines: usize,
    ) -> Result<String, DockerRuntimeError>;
    fn list_managed_containers(&mut self) -> Result<Vec<ContainerInspection>, DockerRuntimeError>;
    fn list_managed_images(&mut self) -> Result<Vec<ManagedImage>, DockerRuntimeError>;
    fn stop_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError>;
    fn remove_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError>;
    fn remove_image(&mut self, image_ref: &str) -> Result<(), DockerRuntimeError>;
}

pub trait ProbeRuntime {
    fn probe_tcp(&mut self, container_name: &str, internal_port: u16) -> Result<bool, ProbeError>;
    fn probe_http(
        &mut self,
        container_name: &str,
        internal_port: u16,
        path: &str,
    ) -> Result<bool, ProbeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteUpdateRequest {
    pub subtree_id: String,
    pub target: String,
    pub health_checks_enabled: bool,
    pub probe_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteInspection {
    pub subtree_id: String,
    pub active_target: String,
    pub activation_verified: bool,
    pub health_checks_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingRuntimeError {
    UpdateFailed(String),
    InspectionFailed(String),
}

impl Display for RoutingRuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UpdateFailed(message) => write!(f, "{message}"),
            Self::InspectionFailed(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for RoutingRuntimeError {}

pub trait RoutingRuntime {
    fn update_route(&mut self, request: RouteUpdateRequest) -> Result<(), RoutingRuntimeError>;
    fn inspect_route(&mut self, subtree_id: &str) -> Result<RouteInspection, RoutingRuntimeError>;
    fn list_managed_routes(&mut self) -> Result<Vec<RouteInspection>, RoutingRuntimeError>;
    fn remove_route(&mut self, subtree_id: &str) -> Result<(), RoutingRuntimeError>;
}
