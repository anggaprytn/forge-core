use std::fmt::{Display, Formatter};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use reqwest::blocking::Client;

use crate::config::{ConfigError, DaemonConfig};
use crate::reconciliation::{ReconciliationCursor, ReconciliationIntentEntry};
use crate::storage::{
    BACKUP_METADATA_VERSION, CHECKPOINT_SCHEMA_VERSION, ConvergenceCheckpointStore,
    EnvironmentPaths, PersistedBackupMetadata, PersistedEnvironmentCheckpoint,
    SNAPSHOT_SCHEMA_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DoctorStatus {
    Ok,
    Warn,
    Error,
}

impl DoctorStatus {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorCheck {
    pub status: DoctorStatus,
    pub message: String,
}

impl DoctorCheck {
    fn ok(message: impl Into<String>) -> Self {
        Self {
            status: DoctorStatus::Ok,
            message: message.into(),
        }
    }

    fn warn(message: impl Into<String>) -> Self {
        Self {
            status: DoctorStatus::Warn,
            message: message.into(),
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            status: DoctorStatus::Error,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorReport {
    pub checks: Vec<DoctorCheck>,
}

impl DoctorReport {
    pub fn has_errors(&self) -> bool {
        self.checks
            .iter()
            .any(|check| check.status == DoctorStatus::Error)
    }

    pub fn render(&self) -> String {
        let mut lines = self
            .checks
            .iter()
            .map(|check| format!("[{}] {}", check.status.label(), check.message))
            .collect::<Vec<_>>();
        lines.push(String::new());
        lines.join("\n")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorOptions {
    pub config_path: PathBuf,
    pub caddy_admin_url: String,
    pub metrics_url: Option<String>,
    pub upgrade: bool,
}

#[derive(Debug)]
pub enum DoctorError {
    MissingConfig { path: PathBuf, upgrade: bool },
    Config(ConfigError),
}

impl Display for DoctorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingConfig { path, upgrade } => {
                let label = if *upgrade {
                    "upgrade doctor failed"
                } else {
                    "doctor failed"
                };
                write!(
                    f,
                    "{label}:\n  missing config file: {}\n  fix: run with --config PATH or install Forge server config",
                    path.display()
                )
            }
            Self::Config(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for DoctorError {}

impl From<ConfigError> for DoctorError {
    fn from(value: ConfigError) -> Self {
        Self::Config(value)
    }
}

pub trait DockerReachability {
    fn check(&mut self) -> Result<(), String>;
}

pub trait HttpReachability {
    fn get_ok(&self, url: &str) -> Result<(), String>;
}

pub struct DockerCliChecker;

impl DockerReachability for DockerCliChecker {
    fn check(&mut self) -> Result<(), String> {
        let output = std::process::Command::new("docker")
            .args(["version", "--format", "{{.Server.Version}}"])
            .output()
            .map_err(|err| err.to_string())?;
        if output.status.success() {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
        }
    }
}

pub struct ReqwestHttpChecker {
    client: Client,
}

impl ReqwestHttpChecker {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }
}

impl HttpReachability for ReqwestHttpChecker {
    fn get_ok(&self, url: &str) -> Result<(), String> {
        let response = self.client.get(url).send().map_err(|err| err.to_string())?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!("status {}", response.status()))
        }
    }
}

pub fn run(options: &DoctorOptions) -> Result<DoctorReport, DoctorError> {
    if !options.config_path.exists() {
        return Err(DoctorError::MissingConfig {
            path: options.config_path.clone(),
            upgrade: options.upgrade,
        });
    }
    let config = DaemonConfig::load_from_file(&options.config_path)?;
    let metrics_url = options
        .metrics_url
        .clone()
        .unwrap_or_else(|| format!("http://{}/metrics", config.api_bind));
    let mut docker = DockerCliChecker;
    let http = ReqwestHttpChecker::new();
    Ok(run_with_dependencies(
        &config,
        &options.caddy_admin_url,
        Some(&metrics_url),
        options.upgrade,
        Some(&options.config_path),
        &mut docker,
        &http,
    ))
}

pub fn run_with_dependencies(
    config: &DaemonConfig,
    caddy_admin_url: &str,
    metrics_url: Option<&str>,
    upgrade: bool,
    config_path: Option<&Path>,
    docker: &mut dyn DockerReachability,
    http: &dyn HttpReachability,
) -> DoctorReport {
    let mut checks = Vec::new();

    match docker.check() {
        Ok(()) => checks.push(DoctorCheck::ok("Docker reachable")),
        Err(err) => checks.push(DoctorCheck::error(format!("Docker reachable: {err}"))),
    }

    let caddy_config_url = format!("{}/config/", caddy_admin_url.trim_end_matches('/'));
    match http.get_ok(&caddy_config_url) {
        Ok(()) => checks.push(DoctorCheck::ok("Caddy admin API reachable")),
        Err(err) => checks.push(DoctorCheck::error(format!(
            "Caddy admin API reachable: {err}"
        ))),
    }

    checks.push(check_storage_root_writable(&config.storage_root));
    checks.push(check_master_key(config_path));
    checks.push(check_exists(
        &config.storage_root.join("queue"),
        "Queue root exists",
    ));
    checks.push(check_exists(
        &config.storage_root.join("projects"),
        "Snapshot root exists",
    ));
    checks.push(check_api_token(&config.bearer_token));

    if let Some(metrics_url) = metrics_url {
        match http.get_ok(metrics_url) {
            Ok(()) => checks.push(DoctorCheck::ok("Metrics endpoint reachable")),
            Err(err) => checks.push(DoctorCheck::warn(format!(
                "Metrics endpoint reachable: {err}"
            ))),
        }
    }

    if upgrade {
        checks.extend(check_upgrade_readiness(config));
    }

    DoctorReport { checks }
}

fn check_upgrade_readiness(config: &DaemonConfig) -> Vec<DoctorCheck> {
    let mut checks = Vec::new();
    checks.push(check_storage_schema_readable(&config.storage_root));
    checks.push(check_checkpoint_schema_compatibility(&config.storage_root));
    checks.push(check_reconciliation_log_compatibility(&config.storage_root));
    checks.push(check_backup_metadata_compatibility(&config.storage_root));
    #[cfg(target_os = "linux")]
    checks.push(check_systemd_unit_sanity());
    checks
}

fn check_storage_root_writable(path: &Path) -> DoctorCheck {
    match fs::metadata(path) {
        Ok(metadata) => {
            if metadata.is_dir() && !metadata.permissions().readonly() {
                DoctorCheck::ok("Storage root writable")
            } else if !metadata.is_dir() {
                DoctorCheck::error("Storage root writable: configured path is not a directory")
            } else {
                DoctorCheck::error("Storage root writable: directory is read-only")
            }
        }
        Err(err) => DoctorCheck::error(format!("Storage root writable: {err}")),
    }
}

fn check_master_key(config_path: Option<&Path>) -> DoctorCheck {
    match resolve_master_key(config_path) {
        MasterKeyStatus::ValidEnv => DoctorCheck::ok("FORGE_MASTER_KEY configured"),
        MasterKeyStatus::ValidEnvFile(path) => DoctorCheck::ok(format!(
            "FORGE_MASTER_KEY configured via {}",
            path.display()
        )),
        MasterKeyStatus::InvalidEnv => DoctorCheck::warn("FORGE_MASTER_KEY invalid"),
        MasterKeyStatus::InvalidEnvFile(path) => {
            DoctorCheck::warn(format!("FORGE_MASTER_KEY invalid in {}", path.display()))
        }
        MasterKeyStatus::Missing => DoctorCheck::warn(
            "FORGE_MASTER_KEY missing from current process environment; daemon EnvironmentFile may still provide it",
        ),
    }
}

enum MasterKeyStatus {
    ValidEnv,
    ValidEnvFile(PathBuf),
    InvalidEnv,
    InvalidEnvFile(PathBuf),
    Missing,
}

fn resolve_master_key(config_path: Option<&Path>) -> MasterKeyStatus {
    match std::env::var("FORGE_MASTER_KEY") {
        Ok(value) if is_valid_master_key(&value) => MasterKeyStatus::ValidEnv,
        Ok(_) => MasterKeyStatus::InvalidEnv,
        Err(_) => match config_path.and_then(conventional_env_file_path) {
            Some(path) => match read_env_file_value(&path, "FORGE_MASTER_KEY") {
                Ok(Some(value)) if is_valid_master_key(&value) => {
                    MasterKeyStatus::ValidEnvFile(path)
                }
                Ok(Some(_)) => MasterKeyStatus::InvalidEnvFile(path),
                Ok(None) | Err(_) => MasterKeyStatus::Missing,
            },
            None => MasterKeyStatus::Missing,
        },
    }
}

fn conventional_env_file_path(config_path: &Path) -> Option<PathBuf> {
    config_path.parent().map(|parent| parent.join("forge.env"))
}

fn read_env_file_value(path: &Path, key: &str) -> Result<Option<String>, std::io::Error> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((entry_key, value)) = line.split_once('=') else {
            continue;
        };
        if entry_key.trim() != key {
            continue;
        }
        return Ok(Some(unquote_env_value(value.trim()).to_string()));
    }
    Ok(None)
}

fn unquote_env_value(value: &str) -> &str {
    if value.len() >= 2 {
        if (value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\''))
        {
            return &value[1..value.len() - 1];
        }
    }
    value
}

fn is_valid_master_key(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|c| c.is_ascii_hexdigit())
}

fn check_exists(path: &Path, label: &str) -> DoctorCheck {
    if path.exists() {
        DoctorCheck::ok(label)
    } else {
        DoctorCheck::warn(format!("{label}: missing"))
    }
}

fn check_api_token(token: &str) -> DoctorCheck {
    if token.trim().is_empty() {
        DoctorCheck::error("API token configured: missing bearer_token")
    } else {
        DoctorCheck::ok("API token configured")
    }
}

fn check_storage_schema_readable(storage_root: &Path) -> DoctorCheck {
    let projects_root = storage_root.join("projects");
    if !projects_root.exists() {
        return DoctorCheck::warn("Storage schema readability: no projects directory found");
    }
    match fs::read_dir(&projects_root) {
        Ok(_) => DoctorCheck::ok("Storage schema readability"),
        Err(err) => DoctorCheck::error(format!("Storage schema readability: {err}")),
    }
}

fn check_checkpoint_schema_compatibility(storage_root: &Path) -> DoctorCheck {
    match scan_checkpoint_schema(storage_root) {
        Ok(()) => DoctorCheck::ok("Checkpoint schema compatibility"),
        Err(err) => DoctorCheck::error(format!("Checkpoint schema compatibility: {err}")),
    }
}

fn check_reconciliation_log_compatibility(storage_root: &Path) -> DoctorCheck {
    let log_path = EnvironmentPaths::reconciliation_log_file(storage_root);
    if !log_path.exists() {
        return DoctorCheck::ok("Reconciliation log schema compatibility");
    }
    let raw = match fs::read_to_string(&log_path) {
        Ok(raw) => raw,
        Err(err) => {
            return DoctorCheck::error(format!("Reconciliation log schema compatibility: {err}"));
        }
    };
    for line in raw.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Err(err) = serde_json::from_str::<ReconciliationIntentEntry>(line) {
            return DoctorCheck::error(format!("Reconciliation log schema compatibility: {err}"));
        }
    }
    let cursor_path = storage_root
        .join("control_plane")
        .join("reconciliation_cursor.json");
    if cursor_path.exists()
        && serde_json::from_str::<ReconciliationCursor>(
            &fs::read_to_string(&cursor_path).unwrap_or_default(),
        )
        .is_err()
    {
        return DoctorCheck::error(
            "Reconciliation log schema compatibility: invalid reconciliation cursor",
        );
    }
    DoctorCheck::ok("Reconciliation log schema compatibility")
}

fn check_backup_metadata_compatibility(storage_root: &Path) -> DoctorCheck {
    match scan_backup_metadata(storage_root) {
        Ok(()) => DoctorCheck::ok("Backup metadata compatibility"),
        Err(err) => DoctorCheck::error(format!("Backup metadata compatibility: {err}")),
    }
}

#[cfg(target_os = "linux")]
fn check_systemd_unit_sanity() -> DoctorCheck {
    let candidates = [
        Path::new("/etc/systemd/system/forge.service"),
        Path::new("/lib/systemd/system/forge.service"),
    ];
    if let Some(path) = candidates.iter().find(|path| path.exists()) {
        match fs::read_to_string(path) {
            Ok(contents)
                if contents.contains("[Unit]")
                    && contents.contains("[Service]")
                    && contents.contains("ExecStart=") =>
            {
                DoctorCheck::ok("systemd unit sanity")
            }
            Ok(_) => {
                DoctorCheck::warn("systemd unit sanity: forge.service missing expected sections")
            }
            Err(err) => DoctorCheck::warn(format!("systemd unit sanity: {err}")),
        }
    } else {
        DoctorCheck::warn("systemd unit sanity: forge.service not found")
    }
}

fn scan_checkpoint_schema(storage_root: &Path) -> Result<(), String> {
    for env in discover_environments(storage_root)? {
        let store = ConvergenceCheckpointStore::new(env.clone());
        if let Some(checkpoint) = store.load().map_err(|err| err.to_string())? {
            validate_checkpoint(&checkpoint)?;
        }
    }
    Ok(())
}

fn validate_checkpoint(checkpoint: &PersistedEnvironmentCheckpoint) -> Result<(), String> {
    if checkpoint.snapshot_version != SNAPSHOT_SCHEMA_VERSION {
        return Err(format!(
            "unsupported checkpoint snapshot_version {}",
            checkpoint.snapshot_version
        ));
    }
    if checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION {
        return Err(format!(
            "unsupported checkpoint schema_version {}",
            checkpoint.schema_version
        ));
    }
    Ok(())
}

fn scan_backup_metadata(storage_root: &Path) -> Result<(), String> {
    for env in discover_environments(storage_root)? {
        let backups_root = env.root.join("backups");
        if !backups_root.exists() {
            continue;
        }
        for entry in fs::read_dir(&backups_root).map_err(|err| err.to_string())? {
            let entry = entry.map_err(|err| err.to_string())?;
            let path = entry.path().join("metadata.json");
            if !path.exists() {
                continue;
            }
            let metadata: PersistedBackupMetadata =
                serde_json::from_str(&fs::read_to_string(&path).map_err(|err| err.to_string())?)
                    .map_err(|err| err.to_string())?;
            if metadata.backup_version != BACKUP_METADATA_VERSION {
                return Err(format!(
                    "unsupported backup_version {} at {}",
                    metadata.backup_version,
                    path.display()
                ));
            }
        }
    }
    Ok(())
}

fn discover_environments(storage_root: &Path) -> Result<Vec<EnvironmentPaths>, String> {
    let projects_root = storage_root.join("projects");
    let mut environments = Vec::new();
    if !projects_root.exists() {
        return Ok(environments);
    }
    for project in fs::read_dir(&projects_root).map_err(|err| err.to_string())? {
        let project = project.map_err(|err| err.to_string())?;
        let environments_root = project.path().join("environments");
        if !environments_root.exists() {
            continue;
        }
        for environment in fs::read_dir(&environments_root).map_err(|err| err.to_string())? {
            let environment = environment.map_err(|err| err.to_string())?;
            let project_id = project.file_name().to_string_lossy().to_string();
            let environment_name = environment.file_name().to_string_lossy().to_string();
            environments.push(EnvironmentPaths::new(
                storage_root,
                &project_id,
                &environment_name,
            ));
        }
    }
    Ok(environments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct StubDocker {
        result: Result<(), String>,
    }

    impl DockerReachability for StubDocker {
        fn check(&mut self) -> Result<(), String> {
            self.result.clone()
        }
    }

    struct StubHttp {
        results: BTreeMap<String, Result<(), String>>,
    }

    impl HttpReachability for StubHttp {
        fn get_ok(&self, url: &str) -> Result<(), String> {
            self.results
                .get(url)
                .cloned()
                .unwrap_or_else(|| Err("connection refused".into()))
        }
    }

    fn test_root(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let base = std::env::temp_dir().join(format!(
            "forge-core-tests-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    fn test_config(root: &Path) -> DaemonConfig {
        DaemonConfig {
            storage_root: root.to_path_buf(),
            api_bind: "127.0.0.1:8080".into(),
            bearer_token: "test-token".into(),
            heartbeat_interval_ms: 1_000,
            startup_replay_max_duration_ms: 5_000,
            startup_replay_max_entries: 256,
            github_webhook_secret: None,
            repository_cache_root: None,
            sqlite_path: None,
        }
    }

    fn messages_with_status(report: &DoctorReport, status: DoctorStatus) -> BTreeSet<String> {
        report
            .checks
            .iter()
            .filter(|check| check.status == status)
            .map(|check| check.message.clone())
            .collect()
    }

    #[test]
    fn doctor_reports_docker_unavailable() {
        let root = test_root("doctor-docker-unavailable");
        fs::create_dir_all(root.join("queue")).unwrap();
        fs::create_dir_all(root.join("projects")).unwrap();
        let config = test_config(&root);
        let mut docker = StubDocker {
            result: Err("docker daemon unavailable".into()),
        };
        let http = StubHttp {
            results: BTreeMap::from([
                ("http://127.0.0.1:2019/config/".into(), Ok(())),
                ("http://127.0.0.1:8080/metrics".into(), Ok(())),
            ]),
        };

        let report = run_with_dependencies(
            &config,
            "http://127.0.0.1:2019",
            Some("http://127.0.0.1:8080/metrics"),
            false,
            None,
            &mut docker,
            &http,
        );

        assert!(
            messages_with_status(&report, DoctorStatus::Error)
                .contains("Docker reachable: docker daemon unavailable")
        );
    }

    #[test]
    fn doctor_reports_caddy_unavailable() {
        let root = test_root("doctor-caddy-unavailable");
        fs::create_dir_all(root.join("queue")).unwrap();
        fs::create_dir_all(root.join("projects")).unwrap();
        let config = test_config(&root);
        let mut docker = StubDocker { result: Ok(()) };
        let http = StubHttp {
            results: BTreeMap::from([(
                "http://127.0.0.1:2019/config/".into(),
                Err("connection refused".into()),
            )]),
        };

        let report = run_with_dependencies(
            &config,
            "http://127.0.0.1:2019",
            None,
            false,
            None,
            &mut docker,
            &http,
        );

        assert!(
            messages_with_status(&report, DoctorStatus::Error)
                .contains("Caddy admin API reachable: connection refused")
        );
    }

    #[test]
    fn doctor_reports_missing_master_key() {
        unsafe {
            std::env::remove_var("FORGE_MASTER_KEY");
        }
        let root = test_root("doctor-missing-master-key");
        fs::create_dir_all(root.join("queue")).unwrap();
        fs::create_dir_all(root.join("projects")).unwrap();
        let config = test_config(&root);
        let mut docker = StubDocker { result: Ok(()) };
        let http = StubHttp {
            results: BTreeMap::from([("http://127.0.0.1:2019/config/".into(), Ok(()))]),
        };

        let report = run_with_dependencies(
            &config,
            "http://127.0.0.1:2019",
            None,
            false,
            None,
            &mut docker,
            &http,
        );

        assert!(messages_with_status(&report, DoctorStatus::Warn).contains(
            "FORGE_MASTER_KEY missing from current process environment; daemon EnvironmentFile may still provide it"
        ));
    }

    #[test]
    fn doctor_upgrade_reports_schema_checks() {
        let root = test_root("doctor-upgrade");
        fs::create_dir_all(root.join("queue")).unwrap();
        fs::create_dir_all(
            root.join("projects")
                .join("api")
                .join("environments")
                .join("prod"),
        )
        .unwrap();
        let config = test_config(&root);
        let mut docker = StubDocker { result: Ok(()) };
        let http = StubHttp {
            results: BTreeMap::from([("http://127.0.0.1:2019/config/".into(), Ok(()))]),
        };

        let report = run_with_dependencies(
            &config,
            "http://127.0.0.1:2019",
            None,
            true,
            None,
            &mut docker,
            &http,
        );

        let ok = messages_with_status(&report, DoctorStatus::Ok);
        assert!(ok.contains("Storage schema readability"));
        assert!(ok.contains("Checkpoint schema compatibility"));
        assert!(ok.contains("Reconciliation log schema compatibility"));
        assert!(ok.contains("Backup metadata compatibility"));
    }

    #[test]
    fn doctor_upgrade_missing_config_reports_path_context() {
        let path = test_root("doctor-upgrade-missing-config").join("missing.conf");
        let err = run(&DoctorOptions {
            config_path: path.clone(),
            caddy_admin_url: "http://127.0.0.1:2019".into(),
            metrics_url: None,
            upgrade: true,
        })
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("upgrade doctor failed:"));
        assert!(message.contains(&format!("missing config file: {}", path.display())));
        assert!(message.contains("fix: run with --config PATH or install Forge server config"));
    }

    #[test]
    fn doctor_upgrade_never_returns_raw_os_error() {
        let err = run(&DoctorOptions {
            config_path: test_root("doctor-upgrade-raw-os-error").join("missing.conf"),
            caddy_admin_url: "http://127.0.0.1:2019".into(),
            metrics_url: None,
            upgrade: true,
        })
        .unwrap_err();

        assert!(!err.to_string().contains("os error 2"));
        assert!(!err.to_string().contains("No such file or directory"));
    }

    #[test]
    fn doctor_upgrade_reads_forge_env_file_for_master_key_if_available() {
        unsafe {
            std::env::remove_var("FORGE_MASTER_KEY");
        }
        let root = test_root("doctor-upgrade-reads-forge-env");
        fs::create_dir_all(root.join("queue")).unwrap();
        fs::create_dir_all(root.join("projects")).unwrap();
        let config_path = root.join("forge.conf");
        fs::write(
            &config_path,
            format!(
                "storage_root={}\napi_bind=127.0.0.1:8080\nbearer_token=test-token\n",
                root.display()
            ),
        )
        .unwrap();
        fs::write(
            root.join("forge.env"),
            "FORGE_MASTER_KEY=00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff\n",
        )
        .unwrap();

        let report = run(&DoctorOptions {
            config_path: config_path.clone(),
            caddy_admin_url: "http://127.0.0.1:9".into(),
            metrics_url: Some("http://127.0.0.1:9/metrics".into()),
            upgrade: true,
        })
        .unwrap();

        assert!(
            messages_with_status(&report, DoctorStatus::Ok).contains(&format!(
                "FORGE_MASTER_KEY configured via {}",
                root.join("forge.env").display()
            ))
        );
    }
}
