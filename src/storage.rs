use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::events::{redact_text, EventRecord};
use serde::{Deserialize, Serialize};

const LOCK_RETRY_DELAY: Duration = Duration::from_millis(10);
const LOCK_RETRY_LIMIT: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotState {
    Healthy,
    Degraded,
    Failed,
    Stopped,
    Rollback,
}

impl SnapshotState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
            Self::Rollback => "rollback",
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    LockTimeout(PathBuf),
    InvalidPointer(PathBuf),
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::LockTimeout(path) => write!(f, "timed out acquiring lock at {}", path.display()),
            Self::InvalidPointer(path) => write!(f, "invalid pointer at {}", path.display()),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub enum RuntimeHealthState {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct RuntimeState {
    pub active_generation: Option<u64>,
    pub health_state: RuntimeHealthState,
    pub failed_probe_count: u32,
    pub successful_probe_count: u32,
    pub restart_attempted: bool,
    pub degraded_since_unix: Option<u64>,
    pub last_transition: String,
    pub last_error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct CleanupRecord {
    pub timestamp_unix: u64,
    pub failure_reason: String,
    pub container_name: Option<String>,
    pub route_subtree_id: Option<String>,
    pub container_removed: bool,
    pub route_removed: bool,
    pub tombstoned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct DiagnosticSummary {
    pub deployment_id: Option<String>,
    pub failure_stage: String,
    pub failure_reason: String,
    pub container_name: String,
    pub cleanup_recorded: bool,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            active_generation: None,
            health_state: RuntimeHealthState::Healthy,
            failed_probe_count: 0,
            successful_probe_count: 0,
            restart_attempted: false,
            degraded_since_unix: None,
            last_transition: "initialized".into(),
            last_error_code: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnvironmentPaths {
    pub root: PathBuf,
}

impl EnvironmentPaths {
    pub fn new(root: impl AsRef<Path>, project_id: &str, environment: &str) -> Self {
        Self {
            root: root
                .as_ref()
                .join("projects")
                .join(project_id)
                .join("environments")
                .join(environment),
        }
    }

    pub fn ensure_exists(&self) -> StorageResult<()> {
        fs::create_dir_all(self.generations_dir())?;
        self.ensure_pointer_file("current")?;
        self.ensure_pointer_file("previous")?;
        if !self.generation_counter().exists() {
            atomic_write(&self.generation_counter(), b"0\n")?;
        }
        Ok(())
    }

    pub fn generations_dir(&self) -> PathBuf {
        self.root.join("generations")
    }

    pub fn generation_dir(&self, generation: u64) -> PathBuf {
        self.generations_dir().join(generation.to_string())
    }

    pub fn generation_counter(&self) -> PathBuf {
        self.root.join("generation.counter")
    }

    pub fn current_pointer(&self) -> PathBuf {
        self.root.join("current")
    }

    pub fn previous_pointer(&self) -> PathBuf {
        self.root.join("previous")
    }

    pub fn runtime_state_file(&self) -> PathBuf {
        self.root.join("runtime_state.json")
    }

    fn ensure_pointer_file(&self, name: &str) -> StorageResult<()> {
        let path = self.root.join(name);
        if !path.exists() {
            atomic_write(&path, b"\n")?;
        }
        Ok(())
    }
}

pub struct GenerationAllocator {
    env: EnvironmentPaths,
}

impl GenerationAllocator {
    pub fn new(env: EnvironmentPaths) -> Self {
        Self { env }
    }

    pub fn allocate(&self) -> StorageResult<u64> {
        self.env.ensure_exists()?;
        let _guard = FileLock::acquire(self.env.generation_counter().with_extension("lock"))?;
        let current = fs::read_to_string(self.env.generation_counter())?;
        let next = current.trim().parse::<u64>().unwrap_or(0) + 1;
        atomic_write(self.env.generation_counter(), format!("{next}\n").as_bytes())?;
        Ok(next)
    }
}

pub struct SnapshotWriter {
    env: EnvironmentPaths,
    generation: u64,
}

impl SnapshotWriter {
    pub fn new(env: EnvironmentPaths, generation: u64) -> StorageResult<Self> {
        env.ensure_exists()?;
        fs::create_dir_all(env.generation_dir(generation).join("diagnostics"))?;
        Ok(Self { env, generation })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn generation_dir(&self) -> PathBuf {
        self.env.generation_dir(self.generation)
    }

    pub fn write_artifact(&self, name: &str, contents: &str) -> StorageResult<()> {
        atomic_write(self.generation_dir().join(name), contents.as_bytes())
    }

    pub fn finalize(&self, project_id: &str, environment: &str, state: SnapshotState) -> StorageResult<()> {
        let finalized_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let snapshot_json = format!(
            "{{\n  \"snapshot_version\": 1,\n  \"project_id\": \"{}\",\n  \"environment\": \"{}\",\n  \"generation\": {},\n  \"state\": \"{}\",\n  \"finalized_at_unix\": {}\n}}\n",
            project_id,
            environment,
            self.generation,
            state.as_str(),
            finalized_at,
        );
        atomic_write(self.generation_dir().join("snapshot.json"), snapshot_json.as_bytes())
    }
}

pub struct PointerStore {
    env: EnvironmentPaths,
}

pub struct RuntimeStateStore {
    env: EnvironmentPaths,
}

pub struct EventStore {
    env: EnvironmentPaths,
    generation: u64,
}

pub struct DiagnosticsStore {
    env: EnvironmentPaths,
    generation: u64,
}

impl RuntimeStateStore {
    pub fn new(env: EnvironmentPaths) -> Self {
        Self { env }
    }

    pub fn load(&self) -> StorageResult<RuntimeState> {
        self.env.ensure_exists()?;
        let path = self.env.runtime_state_file();
        if !path.exists() {
            return Ok(RuntimeState::default());
        }
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })
    }

    pub fn save(&self, state: &RuntimeState) -> StorageResult<()> {
        self.env.ensure_exists()?;
        let bytes = serde_json::to_vec_pretty(state).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        atomic_write(self.env.runtime_state_file(), &bytes)
    }
}

impl EventStore {
    pub fn new(env: EnvironmentPaths, generation: u64) -> Self {
        Self { env, generation }
    }

    pub fn append(&self, event: &EventRecord) -> StorageResult<()> {
        self.env.ensure_exists()?;
        let path = self.env.generation_dir(self.generation).join("events.jsonl");
        let mut existing = if path.exists() {
            fs::read_to_string(&path)?
        } else {
            String::new()
        };
        let line = serde_json::to_string(event).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        existing.push_str(&line);
        existing.push('\n');
        atomic_write(path, existing.as_bytes())
    }

    pub fn list_all(root: impl AsRef<Path>) -> StorageResult<Vec<EventRecord>> {
        let root = root.as_ref().join("projects");
        let mut events = Vec::new();
        if !root.exists() {
            return Ok(events);
        }
        for project in fs::read_dir(root)? {
            let project = project?;
            if !project.file_type()?.is_dir() {
                continue;
            }
            let envs = project.path().join("environments");
            if !envs.exists() {
                continue;
            }
            for env in fs::read_dir(envs)? {
                let env = env?;
                let generations = env.path().join("generations");
                if !generations.exists() {
                    continue;
                }
                for generation in fs::read_dir(generations)? {
                    let generation = generation?;
                    let path = generation.path().join("events.jsonl");
                    if !path.exists() {
                        continue;
                    }
                    let raw = fs::read_to_string(path)?;
                    for line in raw.lines() {
                        if line.trim().is_empty() {
                            continue;
                        }
                        let event = serde_json::from_str::<EventRecord>(line).map_err(|err| {
                            StorageError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                err.to_string(),
                            ))
                        })?;
                        events.push(event);
                    }
                }
            }
        }
        Ok(events)
    }
}

impl DiagnosticsStore {
    pub fn new(env: EnvironmentPaths, generation: u64) -> Self {
        Self { env, generation }
    }

    pub fn write_failure_reason(&self, reason: &str, secrets: &[String]) -> StorageResult<()> {
        self.env.ensure_exists()?;
        let path = self
            .env
            .generation_dir(self.generation)
            .join("diagnostics")
            .join("failure_reason.log");
        let redacted = redact_text(reason, secrets);
        let bounded = if redacted.len() > 4096 {
            redacted[..4096].to_string()
        } else {
            redacted
        };
        atomic_write(path, bounded.as_bytes())
    }

    pub fn write_summary(&self, summary: &DiagnosticSummary) -> StorageResult<()> {
        self.env.ensure_exists()?;
        let path = self
            .env
            .generation_dir(self.generation)
            .join("diagnostics")
            .join("summary.json");
        let bytes = serde_json::to_vec_pretty(summary).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        atomic_write(path, &bytes)
    }

    pub fn read_failure_reason(&self) -> StorageResult<Option<String>> {
        let path = self
            .env
            .generation_dir(self.generation)
            .join("diagnostics")
            .join("failure_reason.log");
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(fs::read_to_string(path)?))
    }
}

impl CleanupRecord {
    pub fn new(
        failure_reason: impl Into<String>,
        container_name: Option<String>,
        route_subtree_id: Option<String>,
        container_removed: bool,
        route_removed: bool,
        tombstoned: bool,
    ) -> Self {
        Self {
            timestamp_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            failure_reason: failure_reason.into(),
            container_name,
            route_subtree_id,
            container_removed,
            route_removed,
            tombstoned,
        }
    }
}

pub struct CleanupStore {
    env: EnvironmentPaths,
    generation: u64,
}

impl CleanupStore {
    pub fn new(env: EnvironmentPaths, generation: u64) -> Self {
        Self { env, generation }
    }

    pub fn write_record(&self, record: &CleanupRecord) -> StorageResult<()> {
        self.env.ensure_exists()?;
        let bytes = serde_json::to_vec_pretty(record).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })?;
        atomic_write(self.env.generation_dir(self.generation).join("cleanup.json"), &bytes)?;
        if record.tombstoned {
            atomic_write(
                self.env.generation_dir(self.generation).join("tombstone"),
                b"cleanup_incomplete\n",
            )?;
        }
        Ok(())
    }

    pub fn read_record(&self) -> StorageResult<Option<CleanupRecord>> {
        let path = self.env.generation_dir(self.generation).join("cleanup.json");
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw).map(Some).map_err(|err| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            ))
        })
    }
}

impl PointerStore {
    pub fn new(env: EnvironmentPaths) -> Self {
        Self { env }
    }

    pub fn swap_current(&self, generation: u64) -> StorageResult<()> {
        let generation_dir = self.env.generation_dir(generation);
        if !generation_dir.join("snapshot.json").exists() {
            return Err(StorageError::InvalidPointer(self.env.current_pointer()));
        }

        let current = self.read_pointer("current")?;
        if let Some(previous_generation) = current {
            atomic_write(
                self.env.previous_pointer(),
                format!("{previous_generation}\n").as_bytes(),
            )?;
        }

        atomic_write(self.env.current_pointer(), format!("{generation}\n").as_bytes())
    }

    pub fn read_pointer(&self, name: &str) -> StorageResult<Option<u64>> {
        let path = self.env.root.join(name);
        let raw = fs::read_to_string(path)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let value = trimmed
            .parse::<u64>()
            .map_err(|_| StorageError::InvalidPointer(self.env.root.join(name)))?;
        Ok(Some(value))
    }
}

struct FileLock {
    path: PathBuf,
}

impl FileLock {
    fn acquire(path: PathBuf) -> StorageResult<Self> {
        for _ in 0..LOCK_RETRY_LIMIT {
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(b"locked\n")?;
                    file.sync_all()?;
                    return Ok(Self { path });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    thread::sleep(LOCK_RETRY_DELAY);
                }
                Err(err) => return Err(StorageError::Io(err)),
            }
        }
        Err(StorageError::LockTimeout(path))
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) fn atomic_write(path: impl AsRef<Path>, contents: &[u8]) -> StorageResult<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let temp_name = format!(
        ".{}.tmp-{}-{}",
        path.file_name().and_then(|s| s.to_str()).unwrap_or("tmp"),
        std::process::id(),
        unique_suffix()
    );
    let temp_path = path.with_file_name(temp_name);

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;
        file.write_all(contents)?;
        file.sync_all()?;
    }

    fs::rename(&temp_path, path)?;
    sync_dir(path.parent().unwrap_or_else(|| Path::new(".")))?;
    Ok(())
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> StorageResult<()> {
    let file = File::open(path)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> StorageResult<()> {
    Ok(())
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
pub mod snapshot_atomicity {
    use super::*;

    #[test]
    fn snapshot_is_not_considered_finalized_until_snapshot_json_exists() {
        let root = test_root("snapshot-not-finalized");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let writer = SnapshotWriter::new(env.clone(), 1).unwrap();

        writer
            .write_artifact("desired_state.json", "{\n  \"ok\": true\n}\n")
            .unwrap();

        assert!(!writer.generation_dir().join("snapshot.json").exists());
        assert!(writer.generation_dir().join("desired_state.json").exists());
    }

    #[test]
    fn finalize_writes_snapshot_json_and_pointer_swap_requires_it() {
        let root = test_root("snapshot-finalize");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let writer = SnapshotWriter::new(env.clone(), 1).unwrap();
        let pointers = PointerStore::new(env.clone());

        assert!(pointers.swap_current(1).is_err());

        writer
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        pointers.swap_current(1).unwrap();

        assert!(writer.generation_dir().join("snapshot.json").exists());
        assert_eq!(pointers.read_pointer("current").unwrap(), Some(1));
    }
}

#[cfg(test)]
pub mod generation_allocator {
    use super::*;

    #[test]
    fn allocated_generations_are_monotonic_and_unique() {
        let root = test_root("generation-allocator");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let allocator = GenerationAllocator::new(env);

        let first = allocator.allocate().unwrap();
        let second = allocator.allocate().unwrap();
        let third = allocator.allocate().unwrap();

        assert_eq!((first, second, third), (1, 2, 3));
    }
}
