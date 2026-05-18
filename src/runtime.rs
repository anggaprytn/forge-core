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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerInspection {
    pub container_name: String,
    pub running: bool,
    pub image_ref: String,
    pub labels: BTreeMap<String, String>,
    pub restart_policy: String,
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

pub trait DockerRuntime {
    fn build_image(&mut self, request: BuildImageRequest) -> Result<String, DockerRuntimeError>;
    fn create_container(
        &mut self,
        request: CreateContainerRequest,
    ) -> Result<String, DockerRuntimeError>;
    fn start_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError>;
    fn inspect_container(
        &mut self,
        container_name: &str,
    ) -> Result<ContainerInspection, DockerRuntimeError>;
    fn stop_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError>;
    fn remove_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError>;
}

pub trait RoutingRuntime {}
