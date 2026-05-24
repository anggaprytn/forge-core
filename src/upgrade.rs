use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine;
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
const OPENSSL_TIMEOUT: Duration = Duration::from_secs(15);

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
    git_commit: Option<String>,
    #[serde(default)]
    git_dirty: Option<String>,
    #[serde(default)]
    build_timestamp: Option<String>,
    #[serde(default)]
    target_triple: Option<String>,
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
    pub release_tag: Option<String>,
    pub manifest_path: Option<PathBuf>,
    pub signature_path: Option<PathBuf>,
    pub allow_unsigned: bool,
    pub allow_dirty_artifact: bool,
    pub auto_rollback: bool,
}

pub fn plan(options: &UpgradeOptions) -> Result<UpgradePlanOutput, UpgradeError> {
    let config = load_config(&options.config_path)?;
    let resolved = resolve_upgrade_options(options)?;
    let current_version = inspect_binary_version(&current_binary_path())?;
    let artifact = inspect_artifact(&resolved.options.artifact_path, &config, &resolved.options)?;
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
        artifact_path: resolved.options.artifact_path.display().to_string(),
        artifact_checksum: artifact.checksum,
        checks,
    })
}

pub fn apply(options: &UpgradeOptions) -> Result<UpgradeApplyOutput, UpgradeError> {
    let config = load_config(&options.config_path)?;
    let resolved = resolve_upgrade_options(options)?;
    let current_version = inspect_binary_version(&current_binary_path())?;
    let artifact = inspect_artifact(&resolved.options.artifact_path, &config, &resolved.options)?;
    let mut checks = vec![
        check_file_readable(&resolved.options.config_path, "Config readable"),
        check_file_readable(
            &conventional_env_path(&resolved.options.config_path),
            "forge.env readable",
        ),
        check_disk_space(&config.storage_root),
    ];
    let mut docker = DockerCliChecker;
    let http = ReqwestHttpChecker::new();
    let report = run_with_dependencies(
        &config,
        &resolved.options.caddy_admin_url,
        None,
        true,
        Some(&resolved.options.config_path),
        &mut docker,
        &http,
    );
    checks.extend(report.checks.into_iter().map(map_doctor_check));
    checks.push(check_systemd_unit_exists());
    checks.extend(compatibility_checks(
        &current_version.schema_versions,
        &artifact.schema_versions,
    ));
    let plan = UpgradePlanOutput {
        current_version: current_version.version,
        target_version: artifact.version.clone(),
        artifact_path: resolved.options.artifact_path.display().to_string(),
        artifact_checksum: artifact.checksum.clone(),
        checks,
    };
    if plan.checks.iter().any(|check| check.status == "error") {
        return Err(UpgradeError::Usage(
            "upgrade plan failed; resolve checks before applying".into(),
        ));
    }

    systemctl(&["stop", "forge.service"])?;
    backup_current_binary(&current_binary_path(), &previous_binary_path())?;
    install_artifact_binary(&resolved.options.artifact_path, &current_binary_path())?;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReleaseManifest {
    version: String,
    git_commit: String,
    git_dirty: bool,
    build_timestamp: String,
    artifacts: Vec<ReleaseManifestArtifact>,
    schema_versions: ReleaseManifestSchemaVersions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReleaseManifestArtifact {
    name: String,
    target_triple: String,
    sha256: String,
    size_bytes: u64,
    created_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReleaseManifestSchemaVersions {
    manifest_schema: u64,
    snapshot_schema: u64,
    checkpoint_schema: u64,
    reconciliation_log_schema: u64,
    storage_compatibility_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GitHubRelease {
    assets: Vec<GitHubReleaseAsset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GitHubReleaseAsset {
    name: String,
    browser_download_url: String,
}

struct ResolvedUpgradeOptions {
    options: UpgradeOptions,
    _temp_dir: Option<TempDirGuard>,
}

impl ReleaseManifestSchemaVersions {
    fn to_forge_schema_versions(&self) -> ForgeSchemaVersions {
        ForgeSchemaVersions {
            manifest_schema: self.manifest_schema,
            snapshot_schema: self.snapshot_schema,
            checkpoint_schema: self.checkpoint_schema,
            reconciliation_log_schema: self.reconciliation_log_schema,
            storage_compatibility: self.storage_compatibility_version,
        }
    }
}

fn resolve_upgrade_options(
    options: &UpgradeOptions,
) -> Result<ResolvedUpgradeOptions, UpgradeError> {
    if let Some(tag) = options.release_tag.as_deref() {
        let temp_root = unique_temp_dir("forge-upgrade-release");
        fs::create_dir_all(&temp_root)?;
        let artifact_path = fetch_release_asset(tag, &temp_root)?;
        let mut resolved = options.clone();
        resolved.artifact_path = artifact_path;
        let manifest_path = temp_root.join("release-manifest.json");
        if manifest_path.exists() {
            resolved.manifest_path = Some(manifest_path);
        }
        let signature_path = temp_root.join("release-manifest.sig");
        resolved.signature_path = if signature_path.exists() {
            Some(signature_path)
        } else {
            None
        };
        return Ok(ResolvedUpgradeOptions {
            options: resolved,
            _temp_dir: Some(TempDirGuard::new(&temp_root)),
        });
    }

    if options.artifact_path.as_os_str().is_empty() {
        return Err(UpgradeError::Usage(
            "upgrade requires --artifact <path> or --release <tag>".into(),
        ));
    }

    Ok(ResolvedUpgradeOptions {
        options: options.clone(),
        _temp_dir: None,
    })
}

fn fetch_release_asset(tag: &str, destination: &Path) -> Result<PathBuf, UpgradeError> {
    let metadata = fetch_release_metadata(tag)?;
    let platform = current_release_platform()?;
    let artifact_name = metadata
        .assets
        .iter()
        .find(|asset| {
            asset.name.starts_with("forge-")
                && asset.name.ends_with(&format!("-{}.tar.gz", platform))
        })
        .map(|asset| asset.name.clone())
        .ok_or_else(|| {
            UpgradeError::Usage(format!(
                "release {tag} does not include a forge artifact for platform {platform}"
            ))
        })?;

    let artifact_path = destination.join(&artifact_name);
    download_release_file(
        metadata
            .assets
            .iter()
            .find(|asset| asset.name == artifact_name)
            .map(|asset| asset.browser_download_url.as_str())
            .unwrap_or_default(),
        &artifact_path,
    )?;
    for asset_name in [
        "release-manifest.json",
        "release-manifest.sig",
        "checksums.txt",
    ] {
        if let Some(asset) = metadata
            .assets
            .iter()
            .find(|asset| asset.name == asset_name)
        {
            download_release_file(&asset.browser_download_url, &destination.join(asset_name))?;
        }
    }
    Ok(artifact_path)
}

fn fetch_release_metadata(tag: &str) -> Result<GitHubRelease, UpgradeError> {
    let api_base = std::env::var("FORGE_RELEASE_API_BASE_URL")
        .unwrap_or_else(|_| "https://api.github.com".into());
    let repository = std::env::var("FORGE_RELEASE_REPOSITORY")
        .unwrap_or_else(|_| "anggaprytn/forge-core".into());
    let url = format!(
        "{}/repos/{}/releases/tags/{}",
        api_base.trim_end_matches('/'),
        repository,
        tag
    );
    let client = reqwest::blocking::Client::builder()
        .build()
        .map_err(|err| UpgradeError::Command(format!("failed to create HTTP client: {err}")))?;
    let response = client
        .get(&url)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "forge-upgrade")
        .send()
        .map_err(|err| {
            UpgradeError::Command(format!("failed to fetch GitHub release metadata: {err}"))
        })?;
    if !response.status().is_success() {
        return Err(UpgradeError::Usage(format!(
            "failed to fetch release metadata for tag {tag}: HTTP {}",
            response.status()
        )));
    }
    response
        .json::<GitHubRelease>()
        .map_err(|err| UpgradeError::Command(format!("failed to decode release metadata: {err}")))
}

fn download_release_file(url: &str, destination: &Path) -> Result<(), UpgradeError> {
    let client = reqwest::blocking::Client::builder()
        .build()
        .map_err(|err| UpgradeError::Command(format!("failed to create HTTP client: {err}")))?;
    let response = client
        .get(url)
        .header("Accept", "application/octet-stream")
        .header("User-Agent", "forge-upgrade")
        .send()
        .map_err(|err| UpgradeError::Command(format!("failed to download release asset: {err}")))?;
    if !response.status().is_success() {
        return Err(UpgradeError::Usage(format!(
            "failed to download release asset {}: HTTP {}",
            destination.display(),
            response.status()
        )));
    }
    let bytes = response.bytes().map_err(|err| {
        UpgradeError::Command(format!("failed to read release asset body: {err}"))
    })?;
    fs::write(destination, &bytes)?;
    Ok(())
}

fn current_release_platform() -> Result<&'static str, UpgradeError> {
    if let Ok(value) = std::env::var("FORGE_RELEASE_PLATFORM") {
        return match value.as_str() {
            "linux-amd64" => Ok("linux-amd64"),
            "darwin-arm64" => Ok("darwin-arm64"),
            other => Err(UpgradeError::Usage(format!(
                "unsupported release platform override {other}"
            ))),
        };
    }
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Ok("linux-amd64"),
        ("macos", "aarch64") => Ok("darwin-arm64"),
        (os, arch) => Err(UpgradeError::Usage(format!(
            "unsupported release platform {os}/{arch}"
        ))),
    }
}

fn inspect_artifact(
    path: &Path,
    config: &DaemonConfig,
    options: &UpgradeOptions,
) -> Result<InspectedArtifact, UpgradeError> {
    reject_world_writable(path)?;
    let checksum = sha256_file(path)?;
    let artifact_size = fs::metadata(path)?.len();
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
    let release_manifest = load_release_manifest(config, options)?;
    if let Some(release_manifest) = release_manifest.as_ref() {
        verify_manifest_for_artifact(
            release_manifest,
            path,
            artifact_size,
            &checksum,
            &version,
            options.allow_dirty_artifact,
        )?;
    }
    Ok(InspectedArtifact {
        version: version.version,
        checksum,
        schema_versions: release_manifest
            .map(|manifest| manifest.schema_versions.to_forge_schema_versions())
            .or(version.schema_versions),
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

fn load_release_manifest(
    config: &DaemonConfig,
    options: &UpgradeOptions,
) -> Result<Option<ReleaseManifest>, UpgradeError> {
    let public_key_path = release_public_key_path(config);
    let Some(manifest_path) = options.manifest_path.as_ref() else {
        if options.allow_unsigned {
            return Ok(None);
        }
        return Err(UpgradeError::Usage(
            "release manifest required; pass --manifest <path> or --allow-unsigned for development artifacts".into(),
        ));
    };

    reject_world_writable_named(manifest_path, "manifest")?;
    let manifest_raw = fs::read_to_string(manifest_path)?;
    let manifest = serde_json::from_str::<ReleaseManifest>(&manifest_raw).map_err(|err| {
        UpgradeError::InvalidArtifact(format!(
            "failed to parse release manifest {}: {err}",
            manifest_path.display()
        ))
    })?;

    if public_key_path.is_some() {
        let Some(signature_path) = options.signature_path.as_ref() else {
            return Err(UpgradeError::Usage(
                "release signature required when release public key is configured; pass --signature <path>".into(),
            ));
        };
        let public_key_path = public_key_path.unwrap();
        reject_world_writable_named(signature_path, "signature")?;
        verify_manifest_signature(manifest_path, signature_path, &public_key_path)?;
    } else if !options.allow_unsigned {
        return Err(UpgradeError::Usage(
            "release public key not configured; set release_public_key_path or FORGE_RELEASE_PUBLIC_KEY, or pass --allow-unsigned for development artifacts".into(),
        ));
    }

    Ok(Some(manifest))
}

fn release_public_key_path(config: &DaemonConfig) -> Option<PathBuf> {
    std::env::var_os("FORGE_RELEASE_PUBLIC_KEY")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| config.release_public_key_path.clone())
}

fn reject_world_writable_named(path: &Path, label: &str) -> Result<(), UpgradeError> {
    let mode = fs::metadata(path)?.permissions().mode();
    if mode & 0o002 != 0 {
        return Err(UpgradeError::InvalidArtifact(format!(
            "refusing world-writable {label}: {}",
            path.display()
        )));
    }
    Ok(())
}

fn verify_manifest_signature(
    manifest_path: &Path,
    signature_path: &Path,
    public_key_path: &Path,
) -> Result<(), UpgradeError> {
    let signature_raw = fs::read_to_string(signature_path)?;
    let signature = base64::engine::general_purpose::STANDARD
        .decode(signature_raw.split_whitespace().collect::<String>())
        .map_err(|err| {
            UpgradeError::InvalidArtifact(format!(
                "invalid release manifest signature {}: {err}",
                signature_path.display()
            ))
        })?;
    let temp_root = unique_temp_dir("forge-upgrade-signature");
    let _cleanup = TempDirGuard::new(&temp_root);
    fs::create_dir_all(&temp_root)?;
    let decoded_signature_path = temp_root.join("release-manifest.sig.bin");
    fs::write(&decoded_signature_path, signature)?;
    let output = run_command_with_timeout(
        Command::new("openssl")
            .args(["pkeyutl", "-verify", "-pubin", "-inkey"])
            .arg(public_key_path)
            .args(["-sigfile"])
            .arg(&decoded_signature_path)
            .args(["-rawin", "-in"])
            .arg(manifest_path),
        OPENSSL_TIMEOUT,
    )
    .map_err(|err| {
        command_error(
            err,
            format!(
                "failed to verify release manifest signature with {}",
                public_key_path.display()
            ),
        )
    })?;
    if !output.status.success() {
        return Err(UpgradeError::InvalidArtifact(format!(
            "release manifest signature verification failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

fn verify_manifest_for_artifact(
    manifest: &ReleaseManifest,
    artifact_path: &Path,
    artifact_size: u64,
    actual_checksum: &str,
    artifact_version: &BinaryVersionInfo,
    allow_dirty_artifact: bool,
) -> Result<(), UpgradeError> {
    if manifest.git_dirty && !allow_dirty_artifact {
        return Err(UpgradeError::InvalidArtifact(
            "refusing dirty release manifest; rerun with --allow-dirty-artifact only for emergency development overrides".into(),
        ));
    }

    let artifact_name = artifact_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            UpgradeError::InvalidArtifact(format!(
                "invalid artifact path: {}",
                artifact_path.display()
            ))
        })?;
    let manifest_artifact = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.name == artifact_name)
        .ok_or_else(|| {
            UpgradeError::InvalidArtifact(format!(
                "artifact not listed in release manifest: {}",
                artifact_path.display()
            ))
        })?;

    if manifest_artifact.sha256 != actual_checksum {
        return Err(UpgradeError::InvalidArtifact(format!(
            "artifact checksum mismatch for {}",
            artifact_path.display()
        )));
    }
    if manifest_artifact.size_bytes != artifact_size {
        return Err(UpgradeError::InvalidArtifact(format!(
            "artifact size mismatch for {}",
            artifact_path.display()
        )));
    }
    if artifact_version.version != manifest.version {
        return Err(UpgradeError::InvalidArtifact(format!(
            "artifact version mismatch: manifest={} artifact={}",
            manifest.version, artifact_version.version
        )));
    }
    if let Some(git_commit) = artifact_version
        .git_commit
        .as_deref()
        .filter(|value| *value != "unknown")
        && git_commit != manifest.git_commit
    {
        return Err(UpgradeError::InvalidArtifact(format!(
            "artifact git_commit mismatch: manifest={} artifact={git_commit}",
            manifest.git_commit
        )));
    }
    let manifest_git_dirty = if manifest.git_dirty { "true" } else { "false" };
    if let Some(git_dirty) = artifact_version
        .git_dirty
        .as_deref()
        .filter(|value| *value != "unknown")
        && git_dirty != manifest_git_dirty
    {
        return Err(UpgradeError::InvalidArtifact(format!(
            "artifact git_dirty mismatch: manifest={manifest_git_dirty} artifact={git_dirty}"
        )));
    }
    if let Some(target_triple) = artifact_version
        .target_triple
        .as_deref()
        .filter(|value| *value != "unknown")
        && target_triple != manifest_artifact.target_triple
    {
        return Err(UpgradeError::InvalidArtifact(format!(
            "artifact target_triple mismatch: manifest={} artifact={target_triple}",
            manifest_artifact.target_triple
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
    if path_requires_sudo(previous) || path_requires_sudo(current) {
        let target_tmp = current.with_extension(format!("tmp.{}", current_unix_timestamp()));
        run_command(
            privileged_command("install")
                .args(["-m", "0755"])
                .arg(previous)
                .arg(&target_tmp),
            SUDO_TIMEOUT,
            format!(
                "failed to stage rollback binary at {}",
                target_tmp.display()
            ),
        )?;
        return run_command(
            privileged_command("mv").arg(&target_tmp).arg(current),
            SUDO_TIMEOUT,
            format!("failed to restore rollback binary at {}", current.display()),
        );
    }
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
