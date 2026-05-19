use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

use crate::storage::{EnvironmentPaths, PointerStore};

#[derive(Debug)]
pub enum InvariantError {
    CurrentPointerMissing,
    CurrentPointerDangling(PathBuf),
    DuplicateGeneration(PathBuf),
    Storage(crate::storage::StorageError),
    Io(std::io::Error),
}

impl Display for InvariantError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CurrentPointerMissing => write!(f, "current pointer missing"),
            Self::CurrentPointerDangling(path) => {
                write!(
                    f,
                    "current pointer references non-finalized generation at {}",
                    path.display()
                )
            }
            Self::DuplicateGeneration(path) => {
                write!(
                    f,
                    "duplicate or reused generation state detected at {}",
                    path.display()
                )
            }
            Self::Storage(err) => write!(f, "{err}"),
            Self::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for InvariantError {}

impl From<std::io::Error> for InvariantError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<crate::storage::StorageError> for InvariantError {
    fn from(value: crate::storage::StorageError) -> Self {
        Self::Storage(value)
    }
}

pub fn assert_current_pointer_valid(
    root: impl AsRef<Path>,
    project_id: &str,
    environment: &str,
) -> Result<(), InvariantError> {
    let env = EnvironmentPaths::new(root, project_id, environment);
    let pointer_store = PointerStore::new(env.clone());
    let current = pointer_store
        .read_pointer("current")?
        .ok_or(InvariantError::CurrentPointerMissing)?;
    let snapshot = env.generation_dir(current).join("snapshot.json");
    if !snapshot.exists() {
        return Err(InvariantError::CurrentPointerDangling(snapshot));
    }
    Ok(())
}

pub fn assert_generation_never_reused(
    root: impl AsRef<Path>,
    project_id: &str,
    environment: &str,
) -> Result<(), InvariantError> {
    let env = EnvironmentPaths::new(root, project_id, environment);
    env.ensure_exists()?;

    let mut generations = Vec::new();
    if env.generations_dir().exists() {
        for entry in fs::read_dir(env.generations_dir())? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Ok(value) = name.parse::<u64>() {
                generations.push(value);
            }
        }
    }

    generations.sort_unstable();
    generations.dedup();

    let counter = fs::read_to_string(env.generation_counter())?
        .trim()
        .parse::<u64>()
        .unwrap_or(0);

    if let Some(max_generation) = generations.last() {
        if *max_generation > counter {
            return Err(InvariantError::DuplicateGeneration(
                env.generation_dir(*max_generation),
            ));
        }
    }

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
pub mod current_pointer_valid {
    use super::*;
    use crate::storage::{PointerStore, SnapshotState, SnapshotWriter};

    #[test]
    fn finalized_snapshot_makes_current_pointer_valid() {
        let root = test_root("current-pointer-valid");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let writer = SnapshotWriter::new(env.clone(), 1).unwrap();
        writer
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();

        let pointers = PointerStore::new(env);
        pointers.swap_current(1).unwrap();

        assert!(assert_current_pointer_valid(&root, "api", "production").is_ok());
    }
}

#[cfg(test)]
pub mod generation_never_reused {
    use super::*;
    use crate::storage::{GenerationAllocator, SnapshotState, SnapshotWriter};

    #[test]
    fn allocated_generation_counter_never_moves_backward() {
        let root = test_root("generation-never-reused");
        let env = EnvironmentPaths::new(&root, "api", "production");
        let allocator = GenerationAllocator::new(env.clone());

        let first = allocator.allocate().unwrap();
        let second = allocator.allocate().unwrap();

        SnapshotWriter::new(env.clone(), first)
            .unwrap()
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();
        SnapshotWriter::new(env.clone(), second)
            .unwrap()
            .finalize("api", "production", SnapshotState::Healthy)
            .unwrap();

        assert!(assert_generation_never_reused(&root, "api", "production").is_ok());
    }
}
