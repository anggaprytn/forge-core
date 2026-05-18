use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentRecord {
    pub deployment_id: String,
    pub project_id: String,
    pub environment: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueState {
    pub queued: VecDeque<DeploymentRecord>,
    pub active: Option<DeploymentRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentRecordWithState {
    pub record: DeploymentRecord,
    pub state: String,
}

#[derive(Debug)]
pub enum QueueError {
    Io(std::io::Error),
    CorruptState(PathBuf),
    ActiveDeploymentAlreadyRunning,
}

impl Display for QueueError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::CorruptState(path) => write!(f, "corrupt queue state at {}", path.display()),
            Self::ActiveDeploymentAlreadyRunning => write!(f, "active deployment already running"),
        }
    }
}

impl std::error::Error for QueueError {}

impl From<std::io::Error> for QueueError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<crate::storage::StorageError> for QueueError {
    fn from(value: crate::storage::StorageError) -> Self {
        match value {
            crate::storage::StorageError::Io(err) => Self::Io(err),
            _ => Self::CorruptState(PathBuf::from("queue")),
        }
    }
}

pub struct PersistentQueue {
    root: PathBuf,
}

impl PersistentQueue {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, QueueError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let queue = Self { root };
        if !queue.queued_file().exists() {
            queue.persist_queued(&VecDeque::new())?;
        }
        if !queue.active_file().exists() {
            crate::storage::atomic_write(queue.active_file(), b"\n")?;
        }
        Ok(queue)
    }

    pub fn enqueue(&self, record: DeploymentRecord) -> Result<(), QueueError> {
        let mut state = self.load_state()?;
        state.queued.push_back(record);
        self.persist_state(&state)
    }

    pub fn start_next(&self) -> Result<Option<DeploymentRecord>, QueueError> {
        let mut state = self.load_state()?;
        if state.active.is_some() {
            return Ok(None);
        }
        let next = state.queued.pop_front();
        state.active = next.clone();
        self.persist_state(&state)?;
        Ok(next)
    }

    pub fn complete_active(&self) -> Result<Option<DeploymentRecord>, QueueError> {
        let mut state = self.load_state()?;
        let completed = state.active.take();
        self.persist_state(&state)?;
        Ok(completed)
    }

    pub fn load_state(&self) -> Result<QueueState, QueueError> {
        let queued_raw = fs::read_to_string(self.queued_file())?;
        let active_raw = fs::read_to_string(self.active_file())?;

        let queued = queued_raw
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(parse_record)
            .collect::<Result<VecDeque<_>, _>>()?;

        let active = if active_raw.trim().is_empty() {
            None
        } else {
            Some(parse_record(active_raw.trim())?)
        };

        Ok(QueueState { queued, active })
    }

    pub fn queued_len(&self) -> Result<usize, QueueError> {
        Ok(self.load_state()?.queued.len())
    }

    pub fn find_deployment(
        &self,
        deployment_id: &str,
    ) -> Result<Option<DeploymentRecordWithState>, QueueError> {
        let state = self.load_state()?;
        if let Some(active) = state.active {
            if active.deployment_id == deployment_id {
                return Ok(Some(DeploymentRecordWithState {
                    record: active,
                    state: "active".into(),
                }));
            }
        }

        for queued in state.queued {
            if queued.deployment_id == deployment_id {
                return Ok(Some(DeploymentRecordWithState {
                    record: queued,
                    state: "queued".into(),
                }));
            }
        }

        Ok(None)
    }

    fn persist_state(&self, state: &QueueState) -> Result<(), QueueError> {
        self.persist_queued(&state.queued)?;
        let active_bytes = if let Some(active) = &state.active {
            format_record(active)
        } else {
            "\n".to_string()
        };
        crate::storage::atomic_write(self.active_file(), active_bytes.as_bytes())?;
        Ok(())
    }

    fn persist_queued(&self, queued: &VecDeque<DeploymentRecord>) -> Result<(), QueueError> {
        let mut serialized = String::new();
        for record in queued {
            serialized.push_str(&format_record(record));
            serialized.push('\n');
        }
        if serialized.is_empty() {
            serialized.push('\n');
        }
        crate::storage::atomic_write(self.queued_file(), serialized.as_bytes())?;
        Ok(())
    }

    fn queued_file(&self) -> PathBuf {
        self.root.join("queued.db")
    }

    fn active_file(&self) -> PathBuf {
        self.root.join("active.db")
    }
}

fn format_record(record: &DeploymentRecord) -> String {
    format!(
        "{}|{}|{}",
        record.deployment_id, record.project_id, record.environment
    )
}

fn parse_record(line: &str) -> Result<DeploymentRecord, QueueError> {
    let parts: Vec<&str> = line.split('|').collect();
    if parts.len() != 3 || parts.iter().any(|part| part.trim().is_empty()) {
        return Err(QueueError::CorruptState(PathBuf::from(line)));
    }
    Ok(DeploymentRecord {
        deployment_id: parts[0].to_string(),
        project_id: parts[1].to_string(),
        environment: parts[2].to_string(),
    })
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
pub mod only_one_active_deployment {
    use super::*;

    #[test]
    fn second_start_returns_none_while_one_is_active() {
        let root = test_root("queue-one-active");
        let queue = PersistentQueue::new(root.join("queue")).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "d1".into(),
                project_id: "api".into(),
                environment: "production".into(),
            })
            .unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "d2".into(),
                project_id: "api".into(),
                environment: "production".into(),
            })
            .unwrap();

        let first = queue.start_next().unwrap();
        let second = queue.start_next().unwrap();

        assert_eq!(first.unwrap().deployment_id, "d1");
        assert!(second.is_none());
        assert_eq!(queue.queued_len().unwrap(), 1);
    }
}

#[cfg(test)]
pub mod queued_deployments_survive_restart {
    use super::*;

    #[test]
    fn queue_state_is_reloaded_from_disk() {
        let root = test_root("queue-survives-restart");
        let queue_path = root.join("queue");
        let queue = PersistentQueue::new(&queue_path).unwrap();
        queue
            .enqueue(DeploymentRecord {
                deployment_id: "d1".into(),
                project_id: "api".into(),
                environment: "production".into(),
            })
            .unwrap();

        let restarted = PersistentQueue::new(&queue_path).unwrap();
        let state = restarted.load_state().unwrap();

        assert_eq!(state.queued.len(), 1);
        assert!(state.active.is_none());
        assert_eq!(state.queued[0].deployment_id, "d1");
    }
}
