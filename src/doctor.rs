use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

use reqwest::blocking::Client;

use crate::config::{ConfigError, DaemonConfig};

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
}

#[derive(Debug)]
pub enum DoctorError {
    Config(ConfigError),
}

impl Display for DoctorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
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
        &mut docker,
        &http,
    ))
}

pub fn run_with_dependencies(
    config: &DaemonConfig,
    caddy_admin_url: &str,
    metrics_url: Option<&str>,
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
    checks.push(check_master_key());
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

    DoctorReport { checks }
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

fn check_master_key() -> DoctorCheck {
    match std::env::var("FORGE_MASTER_KEY") {
        Ok(value) if value.len() == 64 && value.chars().all(|c| c.is_ascii_hexdigit()) => {
            DoctorCheck::ok("FORGE_MASTER_KEY configured")
        }
        Ok(_) => DoctorCheck::warn("FORGE_MASTER_KEY invalid"),
        Err(_) => DoctorCheck::warn("FORGE_MASTER_KEY missing"),
    }
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
            &mut docker,
            &http,
        );

        assert!(messages_with_status(&report, DoctorStatus::Error)
            .contains("Docker reachable: docker daemon unavailable"));
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
            &mut docker,
            &http,
        );

        assert!(messages_with_status(&report, DoctorStatus::Error)
            .contains("Caddy admin API reachable: connection refused"));
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
            results: BTreeMap::from([(
                "http://127.0.0.1:2019/config/".into(),
                Ok(()),
            )]),
        };

        let report = run_with_dependencies(
            &config,
            "http://127.0.0.1:2019",
            None,
            &mut docker,
            &http,
        );

        assert!(messages_with_status(&report, DoctorStatus::Warn)
            .contains("FORGE_MASTER_KEY missing"));
    }
}
