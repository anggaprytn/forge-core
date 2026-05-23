use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::storage::atomic_write;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRecord {
    pub github_id: u64,
    pub github_login: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistrationDecision {
    Existing(UserRecord),
    Created(UserRecord),
    RegistrationClosed,
}

#[derive(Debug)]
pub enum UserStoreError {
    Io(std::io::Error),
    InvalidData(String),
}

impl Display for UserStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::InvalidData(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for UserStoreError {}

impl From<std::io::Error> for UserStoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone)]
pub struct UserStore {
    root: PathBuf,
}

impl UserStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn read_by_github_id(&self, github_id: u64) -> Result<Option<UserRecord>, UserStoreError> {
        let path = self.path_for(github_id);
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw)
            .map(Some)
            .map_err(|err| UserStoreError::InvalidData(format!("invalid user record: {err}")))
    }

    pub fn write_record(&self, record: &UserRecord) -> Result<(), UserStoreError> {
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|err| UserStoreError::InvalidData(err.to_string()))?;
        atomic_write(self.path_for(record.github_id), &bytes)
            .map_err(|err| UserStoreError::Io(std::io::Error::other(err.to_string())))
    }

    pub fn resolve_from_github(
        &self,
        github_id: u64,
        github_login: &str,
        allow_new_registration: bool,
    ) -> Result<RegistrationDecision, UserStoreError> {
        if let Some(mut record) = self.read_by_github_id(github_id)? {
            if record.github_login != github_login {
                record.github_login = github_login.to_string();
                record.updated_at_unix = unix_now();
                self.write_record(&record)?;
            }
            return Ok(RegistrationDecision::Existing(record));
        }

        if !allow_new_registration {
            return Ok(RegistrationDecision::RegistrationClosed);
        }

        let now = unix_now();
        let record = UserRecord {
            github_id,
            github_login: github_login.to_string(),
            created_at_unix: now,
            updated_at_unix: now,
        };
        self.write_record(&record)?;
        Ok(RegistrationDecision::Created(record))
    }

    pub fn user_exists(&self, github_id: u64) -> Result<bool, UserStoreError> {
        self.read_by_github_id(github_id)
            .map(|record| record.is_some())
    }

    fn path_for(&self, github_id: u64) -> PathBuf {
        self.root.join(format!("{github_id}.json"))
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(name: &str) -> PathBuf {
        let mut root = std::env::temp_dir();
        root.push(format!("forge-users-{}-{}", name, std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn resolve_existing_user_updates_login() {
        let store = UserStore::new(test_root("resolve-existing")).unwrap();
        let record = UserRecord {
            github_id: 7,
            github_login: "octocat".into(),
            created_at_unix: 1,
            updated_at_unix: 1,
        };
        store.write_record(&record).unwrap();

        let resolved = store
            .resolve_from_github(7, "renamed-octocat", false)
            .unwrap();

        let RegistrationDecision::Existing(updated) = resolved else {
            panic!("expected existing user");
        };
        assert_eq!(updated.github_login, "renamed-octocat");
        assert!(updated.updated_at_unix >= 1);
    }

    #[test]
    fn resolve_unknown_user_respects_registration_flag() {
        let store = UserStore::new(test_root("resolve-unknown")).unwrap();

        let closed = store.resolve_from_github(9, "new-user", false).unwrap();
        assert_eq!(closed, RegistrationDecision::RegistrationClosed);
        assert!(!store.user_exists(9).unwrap());

        let open = store.resolve_from_github(9, "new-user", true).unwrap();
        let RegistrationDecision::Created(record) = open else {
            panic!("expected created user");
        };
        assert_eq!(record.github_login, "new-user");
        assert!(store.user_exists(9).unwrap());
    }
}
