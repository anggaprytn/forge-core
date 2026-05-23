use std::fmt::{Display, Formatter};
use std::fs;
use std::path::PathBuf;

use crate::config::DaemonConfig;
use crate::queue::PersistentQueue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapState {
    Ready,
    WaitingForStorage(PathBuf),
}

#[derive(Debug)]
pub enum BootstrapError {
    Io(std::io::Error),
}

impl Display for BootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for BootstrapError {}

impl From<std::io::Error> for BootstrapError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<crate::queue::QueueError> for BootstrapError {
    fn from(value: crate::queue::QueueError) -> Self {
        match value {
            crate::queue::QueueError::Io(err) => Self::Io(err),
            crate::queue::QueueError::CorruptState(path) => Self::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                path.display().to_string(),
            )),
            crate::queue::QueueError::ActiveDeploymentAlreadyRunning => {
                Self::Io(std::io::Error::other("active deployment already running"))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct BootstrapContext {
    pub config: DaemonConfig,
}

impl BootstrapContext {
    pub fn new(config: DaemonConfig) -> Self {
        Self { config }
    }

    pub fn initialize(&self) -> Result<BootstrapState, BootstrapError> {
        if !self.config.storage_root.exists() {
            return Ok(BootstrapState::WaitingForStorage(
                self.config.storage_root.clone(),
            ));
        }

        if !self.config.storage_root.is_dir() {
            return Ok(BootstrapState::WaitingForStorage(
                self.config.storage_root.clone(),
            ));
        }

        fs::create_dir_all(self.config.storage_root.join("projects"))?;
        fs::create_dir_all(self.config.storage_root.join("events"))?;
        fs::create_dir_all(self.config.storage_root.join("secrets"))?;
        fs::create_dir_all(self.config.storage_root.join("indexes"))?;
        fs::create_dir_all(self.config.storage_root.join("idempotency"))?;
        PersistentQueue::new(self.config.storage_root.join("queue"))?;
        Ok(BootstrapState::Ready)
    }
}

#[cfg(test)]
fn test_root(name: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

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
pub mod startup_blocks_until_storage_roots_are_valid {
    use super::*;

    #[test]
    fn missing_storage_root_keeps_bootstrap_waiting() {
        let root = test_root("bootstrap-missing-root").join("missing");
        let ctx = BootstrapContext::new(DaemonConfig {
            storage_root: root.clone(),
            api_bind: "127.0.0.1:8080".into(),
            bearer_token: "test-token".into(),
            heartbeat_interval_ms: 1_000,
            github_webhook_secret: None,
            repository_cache_root: None,
            sqlite_path: None,
        });

        let state = ctx.initialize().unwrap();
        assert_eq!(state, BootstrapState::WaitingForStorage(root));
    }

    #[test]
    fn valid_storage_root_initializes_without_sqlite() {
        let root = test_root("bootstrap-valid-root");
        let ctx = BootstrapContext::new(DaemonConfig {
            storage_root: root.clone(),
            api_bind: "127.0.0.1:8080".into(),
            bearer_token: "test-token".into(),
            heartbeat_interval_ms: 1_000,
            github_webhook_secret: None,
            repository_cache_root: None,
            sqlite_path: None,
        });

        let state = ctx.initialize().unwrap();
        assert_eq!(state, BootstrapState::Ready);
        assert!(root.join("queue").exists());
    }
}
