use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildImageRequest {
    pub image_tag: String,
    pub context_path: PathBuf,
    pub dockerfile_path: PathBuf,
    pub build_args: BTreeMap<String, String>,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateContainerRequest {
    pub container_name: String,
    pub image_ref: String,
    pub labels: BTreeMap<String, String>,
    pub environment: BTreeMap<String, String>,
    pub network_name: Option<String>,
    pub network_aliases: Vec<String>,
    pub volume_mounts: Vec<VolumeMountRequest>,
    pub command: Option<Vec<String>>,
    pub runtime_policy: ContainerRuntimePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeMountRequest {
    pub volume_name: String,
    pub mount_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateVolumeRequest {
    pub volume_name: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerVolumeMount {
    pub volume_name: String,
    pub mount_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerInspection {
    pub container_name: String,
    pub running: bool,
    pub state_status: String,
    pub exit_code: Option<i32>,
    pub restart_count: u64,
    pub started_at: Option<String>,
    pub image_ref: String,
    pub labels: BTreeMap<String, String>,
    pub network_ips: BTreeMap<String, String>,
    pub volume_mounts: Vec<ContainerVolumeMount>,
    pub restart_policy: String,
    pub restart_max_retries: Option<u64>,
    pub cpu_limit: Option<String>,
    pub memory_limit_mb: Option<u64>,
    pub oom_killed: bool,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub exit_signal: Option<i32>,
    pub termination_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContainerRuntimePolicy {
    pub cpu_limit: Option<String>,
    pub memory_limit_mb: Option<u64>,
    pub restart_policy: String,
    pub max_retries: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContainerUsageSnapshot {
    pub captured_at_unix: u64,
    pub cpu_percent: Option<String>,
    pub memory_usage_mb: Option<u64>,
    pub memory_limit_mb: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContainerTerminationInfo {
    pub oom_killed: bool,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub reason: Option<String>,
    pub stderr_tail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedImage {
    pub image_ref: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedVolume {
    pub volume_name: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeInspection {
    pub volume_name: String,
    pub mountpoint: PathBuf,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeArchiveMode {
    Backup,
    Restore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeArchiveHelperRequest {
    pub volume_name: String,
    pub archive_dir: PathBuf,
    pub archive_file: String,
    pub mode: VolumeArchiveMode,
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeArchiveHelperOutput {
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecInContainerRequest {
    pub container_name: String,
    pub command: Vec<String>,
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecInContainerOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
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
    fn ensure_volume(&mut self, request: CreateVolumeRequest) -> Result<(), DockerRuntimeError>;
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
    fn container_usage(
        &mut self,
        container_name: &str,
    ) -> Result<ContainerUsageSnapshot, DockerRuntimeError> {
        Err(DockerRuntimeError::CommandFailed(format!(
            "container usage not implemented for {container_name}"
        )))
    }
    fn list_managed_containers(&mut self) -> Result<Vec<ContainerInspection>, DockerRuntimeError>;
    fn list_managed_images(&mut self) -> Result<Vec<ManagedImage>, DockerRuntimeError>;
    fn list_managed_volumes(&mut self) -> Result<Vec<ManagedVolume>, DockerRuntimeError>;
    fn inspect_volume(
        &mut self,
        volume_name: &str,
    ) -> Result<VolumeInspection, DockerRuntimeError> {
        Err(DockerRuntimeError::CommandFailed(format!(
            "volume inspection not implemented for {volume_name}"
        )))
    }
    fn run_volume_archive_helper(
        &mut self,
        request: VolumeArchiveHelperRequest,
    ) -> Result<VolumeArchiveHelperOutput, DockerRuntimeError> {
        let direction = match request.mode {
            VolumeArchiveMode::Backup => "backup",
            VolumeArchiveMode::Restore => "restore",
        };
        let archive_dir = Path::new(&request.archive_dir).display();
        Err(DockerRuntimeError::CommandFailed(format!(
            "volume archive helper not implemented for {} {} in {}",
            direction, request.volume_name, archive_dir
        )))
    }
    fn exec_in_container(
        &mut self,
        request: ExecInContainerRequest,
    ) -> Result<ExecInContainerOutput, DockerRuntimeError> {
        Err(DockerRuntimeError::CommandFailed(format!(
            "container exec not implemented for {}",
            request.container_name
        )))
    }
    fn stop_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError>;
    fn remove_container(&mut self, container_name: &str) -> Result<(), DockerRuntimeError>;
    fn remove_image(&mut self, image_ref: &str) -> Result<(), DockerRuntimeError>;
    fn remove_volume(&mut self, volume_name: &str) -> Result<(), DockerRuntimeError>;
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
    pub domain: Option<String>,
    pub health_checks_enabled: bool,
    pub probe_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteInspection {
    pub subtree_id: String,
    pub active_target: String,
    pub domain: Option<String>,
    pub activation_verified: bool,
    pub verification_url: Option<String>,
    pub verification_host: Option<String>,
    pub verification_status_code: Option<u16>,
    pub verification_response_body: Option<String>,
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
