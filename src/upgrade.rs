use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::api::ForgeSchemaVersions;
use crate::config::DaemonConfig;
use crate::doctor::{
    DockerCliChecker, DoctorCheck, DoctorStatus, ReqwestHttpChecker, run_with_dependencies,
};
use crate::process::{CommandError, run_command_with_timeout};
use crate::storage::{atomic_write, current_unix_timestamp};

pub const MANIFEST_SCHEMA_VERSION: u64 = 1;
pub const STORAGE_COMPATIBILITY_VERSION: u64 = 1;
const DEFAULT_BINARY_PATH: &str = "/usr/local/bin/forge";
const DEFAULT_PREVIOUS_BINARY_PATH: &str = "/usr/local/bin/forge.previous";
const READYZ_TIMEOUT: Duration = Duration::from_secs(30);
const READYZ_POLL_INTERVAL: Duration = Duration::from_millis(500);
const DF_TIMEOUT: Duration = Duration::from_secs(5);
const TAR_TIMEOUT: Duration = Duration::from_secs(30);
const VERSION_COMMAND_TIMEOUT: Duration = Duration::from_secs(15);
const SYSTEMCTL_TIMEOUT: Duration = Duration::from_secs(30);
const SUDO_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum UpgradeError {
    Usage(String),
    Io(std::io::Error),
    InvalidArtifact(String),
    Command(String),
    Readyz(String),
}

impl Display for UpgradeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(message) => write!(f, "{message}"),
            Self::Io(err) => write!(f, "{err}"),
            Self::InvalidArtifact(message) => write!(f, "{message}"),
            Self::Command(message) => write!(f, "{message}"),
            Self::Readyz(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for UpgradeError {}

impl From<std::io::Error> for UpgradeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeEvent {
    pub timestamp_unix: u64,
    pub action: String,
    pub from_version: String,
    pub to_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_checksum: Option<String>,
    pub operator_source: String,
    pub result: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeCheckResult {
    pub status: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradePlanOutput {
    pub current_version: String,
    pub target_version: String,
    pub artifact_path: String,
    pub artifact_checksum: String,
    pub checks: Vec<UpgradeCheckResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BinaryVersionInfo {
    version: String,
    #[serde(default)]
    schema_versions: Option<ForgeSchemaVersions>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeApplyOutput {
    pub from_version: String,
    pub to_version: String,
    pub artifact_checksum: String,
    pub result: String,
}

#[derive(Debug, Clone)]
pub struct UpgradeOptions {
    pub config_path: PathBuf,
    pub caddy_admin_url: String,
    pub artifact_path: PathBuf,
    pub auto_rollback: bool,
}

pub fn plan(options: &UpgradeOptions) -> Result<UpgradePlanOutput, UpgradeError> {
    let config = load_config(&options.config_path)?;
    let current_version = inspect_binary_version(&current_binary_path())?;
    let artifact = inspect_artifact(&options.artifact_path)?;
    let mut checks = vec![
        check_file_readable(&options.config_path, "Config readable"),
        check_file_readable(
            &conventional_env_path(&options.config_path),
            "forge.env readable",
        ),
        check_disk_space(&config.storage_root),
    ];

    let mut docker = DockerCliChecker;
    let http = ReqwestHttpChecker::new();
    let report = run_with_dependencies(
        &config,
        &options.caddy_admin_url,
        None,
        true,
        Some(&options.config_path),
        &mut docker,
        &http,
    );
    checks.extend(report.checks.into_iter().map(map_doctor_check));
    checks.push(check_systemd_unit_exists());
    checks.extend(compatibility_checks(
        &current_version.schema_versions,
        &artifact.schema_versions,
    ));

    Ok(UpgradePlanOutput {
        current_version: current_version.version,
        target_version: artifact.version,
        artifact_path: options.artifact_path.display().to_string(),
        artifact_checksum: artifact.checksum,
        checks,
    })
}

pub fn apply(options: &UpgradeOptions) -> Result<UpgradeApplyOutput, UpgradeError> {
    let plan = plan(options)?;
    if plan.checks.iter().any(|check| check.status == "error") {
        return Err(UpgradeError::Usage(
            "upgrade plan failed; resolve checks before applying".into(),
        ));
    }
    let config = load_config(&options.config_path)?;
    let artifact = inspect_artifact(&options.artifact_path)?;

    systemctl(&["stop", "forge.service"])?;
    backup_current_binary(&current_binary_path(), &previous_binary_path())?;
    install_artifact_binary(&options.artifact_path, &current_binary_path())?;
    systemctl(&["start", "forge.service"])?;

    let result = match wait_readyz(&config) {
        Ok(()) => "ok".to_string(),
        Err(err) if options.auto_rollback => {
            restore_previous_binary(Path::new(&previous_binary_path()), &current_binary_path())?;
            systemctl(&["restart", "forge.service"])?;
            wait_readyz(&config).map_err(|rollback_err| {
                UpgradeError::Readyz(format!(
                    "upgrade failed readiness check ({err}); automatic rollback also failed ({rollback_err})"
                ))
            })?;
            "auto_rolled_back".into()
        }
        Err(err) => return Err(UpgradeError::Readyz(err)),
    };

    let output = UpgradeApplyOutput {
        from_version: plan.current_version.clone(),
        to_version: artifact.version.clone(),
        artifact_checksum: artifact.checksum.clone(),
        result: result.clone(),
    };
    write_journal(
        &config.storage_root,
        &UpgradeEvent {
            timestamp_unix: current_unix_timestamp(),
            action: "apply".into(),
            from_version: plan.current_version,
            to_version: artifact.version,
            artifact_checksum: Some(artifact.checksum),
            operator_source: operator_source("forge upgrade apply"),
            result,
            failure_reason: None,
        },
    )?;
    Ok(output)
}

pub fn rollback(config_path: &Path) -> Result<UpgradeApplyOutput, UpgradeError> {
    let config = load_config(config_path)?;
    let from_version = inspect_binary_version(&current_binary_path())?.version;
    let to_version = inspect_binary_version(Path::new(&previous_binary_path()))?.version;
    restore_previous_binary(Path::new(&previous_binary_path()), &current_binary_path())?;
    systemctl(&["restart", "forge.service"])?;
    wait_readyz(&config).map_err(UpgradeError::Readyz)?;
    write_journal(
        &config.storage_root,
        &UpgradeEvent {
            timestamp_unix: current_unix_timestamp(),
            action: "rollback".into(),
            from_version: from_version.clone(),
            to_version: to_version.clone(),
            artifact_checksum: None,
            operator_source: operator_source("forge upgrade rollback"),
            result: "ok".into(),
            failure_reason: None,
        },
    )?;
    Ok(UpgradeApplyOutput {
        from_version,
        to_version,
        artifact_checksum: "previous-binary".into(),
        result: "ok".into(),
    })
}

pub fn read_recent_events(storage_root: &Path, limit: usize) -> Vec<UpgradeEvent> {
    let path = upgrade_journal_path(storage_root);
    let Ok(raw) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut events = raw
        .lines()
        .filter_map(|line| serde_json::from_str::<UpgradeEvent>(line).ok())
        .collect::<Vec<_>>();
    if events.len() > limit {
        events.drain(0..events.len() - limit);
    }
    events
}

fn load_config(path: &Path) -> Result<DaemonConfig, UpgradeError> {
    DaemonConfig::load_from_file(path).map_err(|err| UpgradeError::Usage(err.to_string()))
}

fn map_doctor_check(check: DoctorCheck) -> UpgradeCheckResult {
    let status = match check.status {
        DoctorStatus::Ok => "ok",
        DoctorStatus::Warn => "warn",
        DoctorStatus::Error => "error",
    };
    UpgradeCheckResult {
        status: status.into(),
        message: check.message,
    }
}

fn check_file_readable(path: &Path, label: &str) -> UpgradeCheckResult {
    match File::open(path) {
        Ok(_) => UpgradeCheckResult {
            status: "ok".into(),
            message: label.into(),
        },
        Err(err) => UpgradeCheckResult {
            status: "error".into(),
            message: format!("{label}: {err}"),
        },
    }
}

fn check_disk_space(path: &Path) -> UpgradeCheckResult {
    let output = run_command_with_timeout(
        Command::new("df").args(["-Pk", &path.display().to_string()]),
        DF_TIMEOUT,
    );
    let Ok(output) = output else {
        return UpgradeCheckResult {
            status: "warn".into(),
            message: "Available disk space: unable to inspect".into(),
        };
    };
    if !output.status.success() {
        return UpgradeCheckResult {
            status: "warn".into(),
            message: "Available disk space: unable to inspect".into(),
        };
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let available_kb = stdout
        .lines()
        .nth(1)
        .and_then(|line| line.split_whitespace().nth(3))
        .and_then(|value| value.parse::<u64>().ok());
    match available_kb {
        Some(value) => UpgradeCheckResult {
            status: "ok".into(),
            message: format!("Available disk space: {value} KB"),
        },
        None => UpgradeCheckResult {
            status: "warn".into(),
            message: "Available disk space: unable to parse".into(),
        },
    }
}

fn check_systemd_unit_exists() -> UpgradeCheckResult {
    if Path::new("/etc/systemd/system/forge.service").exists()
        || Path::new("/lib/systemd/system/forge.service").exists()
    {
        UpgradeCheckResult {
            status: "ok".into(),
            message: "systemd unit exists".into(),
        }
    } else {
        UpgradeCheckResult {
            status: "warn".into(),
            message: "systemd unit exists: forge.service not found".into(),
        }
    }
}

fn compatibility_checks(
    current: &Option<ForgeSchemaVersions>,
    target: &Option<ForgeSchemaVersions>,
) -> Vec<UpgradeCheckResult> {
    let (Some(current), Some(target)) = (current.as_ref(), target.as_ref()) else {
        return vec![UpgradeCheckResult {
            status: "warn".into(),
            message: "Schema compatibility: unavailable from version output".into(),
        }];
    };
    let mut checks = Vec::new();
    if current.storage_compatibility == target.storage_compatibility {
        checks.push(UpgradeCheckResult {
            status: "ok".into(),
            message: format!("Storage compatibility: {}", target.storage_compatibility),
        });
    } else {
        checks.push(UpgradeCheckResult {
            status: "error".into(),
            message: format!(
                "Storage compatibility mismatch: current={} target={}",
                current.storage_compatibility, target.storage_compatibility
            ),
        });
    }
    for (label, current_value, target_value) in [
        (
            "Manifest schema compatibility",
            current.manifest_schema,
            target.manifest_schema,
        ),
        (
            "Snapshot schema compatibility",
            current.snapshot_schema,
            target.snapshot_schema,
        ),
        (
            "Checkpoint schema compatibility",
            current.checkpoint_schema,
            target.checkpoint_schema,
        ),
        (
            "Reconciliation log schema compatibility",
            current.reconciliation_log_schema,
            target.reconciliation_log_schema,
        ),
    ] {
        let (status, message) = if target_value >= current_value {
            (
                "ok",
                format!("{label}: current={current_value} target={target_value}"),
            )
        } else {
            (
                "error",
                format!("{label} mismatch: current={current_value} target={target_value}"),
            )
        };
        checks.push(UpgradeCheckResult {
            status: status.into(),
            message,
        });
    }
    checks
}

fn inspect_binary_version(binary_path: &Path) -> Result<BinaryVersionInfo, UpgradeError> {
    let output = run_command_with_timeout(
        Command::new(binary_path).arg("version"),
        VERSION_COMMAND_TIMEOUT,
    )
    .map_err(|err| command_error(err, format!("failed to execute {}", binary_path.display())))?;
    if !output.status.success() {
        return Err(UpgradeError::Command(format!(
            "failed to read version from {}",
            binary_path.display()
        )));
    }
    serde_json::from_slice(&output.stdout).map_err(|err| {
        UpgradeError::Command(format!(
            "failed to parse version output from {}: {err}",
            binary_path.display()
        ))
    })
}

#[derive(Debug, Clone)]
struct InspectedArtifact {
    version: String,
    checksum: String,
    schema_versions: Option<ForgeSchemaVersions>,
}

fn inspect_artifact(path: &Path) -> Result<InspectedArtifact, UpgradeError> {
    reject_world_writable(path)?;
    let checksum = sha256_file(path)?;
    verify_checksum_if_available(path, &checksum)?;
    let temp_root = unique_temp_dir("forge-upgrade-artifact");
    let _cleanup = TempDirGuard::new(&temp_root);
    fs::create_dir_all(&temp_root)?;
    unpack_artifact(path, &temp_root)?;
    let binary_path = temp_root.join("forge");
    if !binary_path.exists() {
        return Err(UpgradeError::InvalidArtifact(
            "artifact missing forge binary".into(),
        ));
    }
    let version = inspect_binary_version(&binary_path)?;
    Ok(InspectedArtifact {
        version: version.version,
        checksum,
        schema_versions: version.schema_versions,
    })
}

fn reject_world_writable(path: &Path) -> Result<(), UpgradeError> {
    let mode = fs::metadata(path)?.permissions().mode();
    if mode & 0o002 != 0 {
        return Err(UpgradeError::InvalidArtifact(format!(
            "refusing world-writable artifact: {}",
            path.display()
        )));
    }
    Ok(())
}

fn verify_checksum_if_available(path: &Path, actual_checksum: &str) -> Result<(), UpgradeError> {
    let checksum_path = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("checksums.txt");
    if !checksum_path.exists() {
        return Ok(());
    }
    let raw = fs::read_to_string(&checksum_path)?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            UpgradeError::InvalidArtifact(format!("invalid artifact path: {}", path.display()))
        })?;
    let expected = raw.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        let checksum = parts.next()?;
        let name = parts.next()?;
        (name.trim_start_matches('*') == file_name).then(|| checksum.to_string())
    });
    if let Some(expected) = expected
        && expected != actual_checksum
    {
        return Err(UpgradeError::InvalidArtifact(format!(
            "artifact checksum mismatch for {}",
            path.display()
        )));
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, UpgradeError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn unpack_artifact(path: &Path, destination: &Path) -> Result<(), UpgradeError> {
    let output = run_command_with_timeout(
        Command::new("tar")
            .args(["-xzf"])
            .arg(path)
            .args(["-C"])
            .arg(destination),
        TAR_TIMEOUT,
    )
    .map_err(|err| command_error(err, format!("failed to unpack artifact {}", path.display())))?;
    if !output.status.success() {
        return Err(UpgradeError::Command(format!(
            "failed to unpack artifact {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

fn install_artifact_binary(artifact_path: &Path, destination: &Path) -> Result<(), UpgradeError> {
    let temp_root = unique_temp_dir("forge-upgrade-install");
    let _cleanup = TempDirGuard::new(&temp_root);
    fs::create_dir_all(&temp_root)?;
    unpack_artifact(artifact_path, &temp_root)?;
    let binary_path = temp_root.join("forge");
    if !binary_path.exists() {
        return Err(UpgradeError::InvalidArtifact(
            "artifact missing forge binary".into(),
        ));
    }
    let contents = fs::read(&binary_path)?;
    write_binary_atomically(destination, &contents)
}

fn backup_current_binary(current: &Path, previous: &str) -> Result<(), UpgradeError> {
    if !current.exists() {
        return Ok(());
    }
    if path_requires_sudo(current) || path_requires_sudo(Path::new(previous)) {
        run_command(
            privileged_command("cp").arg(current).arg(previous),
            SUDO_TIMEOUT,
            format!(
                "failed to back up current binary {} -> {}",
                current.display(),
                previous
            ),
        )?;
        return Ok(());
    }
    fs::copy(current, previous)?;
    Ok(())
}

fn restore_previous_binary(previous: &Path, current: &Path) -> Result<(), UpgradeError> {
    let contents = fs::read(previous)?;
    write_binary_atomically(current, &contents)
}

fn wait_readyz(config: &DaemonConfig) -> Result<(), String> {
    let readyz_url = readyz_url(config);
    let client = reqwest::blocking::Client::new();
    let timeout = std::env::var("FORGE_UPGRADE_READYZ_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(READYZ_TIMEOUT);
    let poll_interval = std::env::var("FORGE_UPGRADE_READYZ_POLL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(READYZ_POLL_INTERVAL);
    let started = Instant::now();
    let mut attempts = 0_u64;
    let mut last_error = "no response".to_string();
    while started.elapsed() < timeout {
        attempts += 1;
        match client.get(&readyz_url).send() {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => last_error = format!("status {}", response.status()),
            Err(err) => last_error = err.to_string(),
        }
        thread::sleep(poll_interval);
    }
    Err(format!(
        "readyz failed at {readyz_url} after {}ms across {attempts} attempts: {last_error}",
        started.elapsed().as_millis()
    ))
}

fn readyz_url(config: &DaemonConfig) -> String {
    if let Ok(value) = std::env::var("FORGE_UPGRADE_READYZ_URL") {
        return value;
    }
    let port = config.api_bind.rsplit(':').next().unwrap_or("18080").trim();
    format!("http://127.0.0.1:{port}/readyz")
}

fn systemctl(args: &[&str]) -> Result<(), UpgradeError> {
    let binary = std::env::var("FORGE_SYSTEMCTL_BIN").unwrap_or_else(|_| "systemctl".into());
    let mut command = if std::env::var("FORGE_SYSTEMCTL_BIN").is_ok() || running_as_root() {
        Command::new(&binary)
    } else {
        privileged_command(&binary)
    };
    let output = run_command_with_timeout(command.args(args), SYSTEMCTL_TIMEOUT)
        .map_err(|err| command_error(err, format!("systemctl {} failed", args.join(" "))))?;
    if output.status.success() {
        return Ok(());
    }
    Err(UpgradeError::Command(format!(
        "systemctl {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

fn current_binary_path() -> PathBuf {
    std::env::var("FORGE_UPGRADE_BINARY_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_BINARY_PATH))
}

fn previous_binary_path() -> String {
    std::env::var("FORGE_UPGRADE_PREVIOUS_BINARY_PATH")
        .unwrap_or_else(|_| DEFAULT_PREVIOUS_BINARY_PATH.into())
}

fn operator_source(source: &str) -> String {
    let user = std::env::var("SUDO_USER")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "unknown".into());
    format!("{user}:{source}")
}

fn conventional_env_path(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .map(|parent| parent.join("forge.env"))
        .unwrap_or_else(|| PathBuf::from("forge.env"))
}

fn write_journal(storage_root: &Path, event: &UpgradeEvent) -> Result<(), UpgradeError> {
    let path = upgrade_journal_path(storage_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let line = serde_json::to_string(event).map_err(|err| {
        UpgradeError::Usage(format!("failed to serialize upgrade journal: {err}"))
    })?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn upgrade_journal_path(storage_root: &Path) -> PathBuf {
    storage_root.join("control_plane").join("upgrades.jsonl")
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ))
}

fn command_error(err: CommandError, prefix: String) -> UpgradeError {
    UpgradeError::Command(format!("{prefix}: {err}"))
}

fn run_command(
    command: &mut Command,
    timeout: Duration,
    prefix: String,
) -> Result<(), UpgradeError> {
    let output = run_command_with_timeout(command, timeout)
        .map_err(|err| command_error(err, prefix.clone()))?;
    if output.status.success() {
        return Ok(());
    }
    Err(UpgradeError::Command(format!(
        "{prefix}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

fn write_binary_atomically(destination: &Path, contents: &[u8]) -> Result<(), UpgradeError> {
    if path_requires_sudo(destination) {
        let temp_root = unique_temp_dir("forge-upgrade-sudo-install");
        let _cleanup = TempDirGuard::new(&temp_root);
        fs::create_dir_all(&temp_root)?;
        let source = temp_root.join("forge");
        fs::write(&source, contents)?;
        let target_tmp = destination.with_extension(format!("tmp.{}", current_unix_timestamp()));
        run_command(
            privileged_command("install")
                .args(["-m", "0755"])
                .arg(&source)
                .arg(&target_tmp),
            SUDO_TIMEOUT,
            format!("failed to stage binary at {}", target_tmp.display()),
        )?;
        return run_command(
            privileged_command("mv").arg(&target_tmp).arg(destination),
            SUDO_TIMEOUT,
            format!("failed to install binary at {}", destination.display()),
        );
    }
    atomic_write(destination, contents)
        .map_err(|err| UpgradeError::Io(storage_error_to_io(err)))?;
    let mut permissions = fs::metadata(destination)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(destination, permissions)?;
    Ok(())
}

fn privileged_command(program: &str) -> Command {
    if let Ok(binary) = std::env::var("FORGE_SUDO_BIN") {
        let mut command = Command::new(binary);
        command.arg(program);
        return command;
    }
    let mut command = Command::new("sudo");
    command.args(["-n", program]);
    command
}

fn path_requires_sudo(path: &Path) -> bool {
    if std::env::var("FORGE_UPGRADE_FORCE_SUDO").ok().as_deref() == Some("1") {
        return true;
    }
    !running_as_root() && path.starts_with("/usr/local/bin")
}

fn running_as_root() -> bool {
    let output = run_command_with_timeout(Command::new("id").arg("-u"), DF_TIMEOUT);
    let Ok(output) = output else {
        return false;
    };
    output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "0"
}

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn storage_error_to_io(err: crate::storage::StorageError) -> std::io::Error {
    match err {
        crate::storage::StorageError::Io(err) => err,
        other => std::io::Error::other(other.to_string()),
    }
}
