use crate::api::{ErrorResponse, ProjectRecord, ProjectUpsertRequest};
use crate::storage::{StorageError, atomic_write};
use serde_json::Error as JsonError;
use sha2::{Digest, Sha256};
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub enum ProjectRegistryError {
    Storage(StorageError),
    InvalidProjectId,
    InvalidRepoUrl(String),
    InvalidDefaultBranch,
    InvalidBaseDomain(String),
    MissingAppsDomain,
}

impl Display for ProjectRegistryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(err) => write!(f, "{err}"),
            Self::InvalidProjectId => write!(
                f,
                "project_id must use lowercase letters, digits, and hyphens only"
            ),
            Self::InvalidRepoUrl(message) => write!(f, "{message}"),
            Self::InvalidDefaultBranch => write!(f, "default_branch must not be empty"),
            Self::InvalidBaseDomain(message) => write!(f, "{message}"),
            Self::MissingAppsDomain => write!(
                f,
                "FORGE_APPS_DOMAIN is required when base_domain is not provided"
            ),
        }
    }
}

impl std::error::Error for ProjectRegistryError {}

impl From<StorageError> for ProjectRegistryError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<std::io::Error> for ProjectRegistryError {
    fn from(value: std::io::Error) -> Self {
        Self::Storage(StorageError::Io(value))
    }
}

impl From<JsonError> for ProjectRegistryError {
    fn from(value: JsonError) -> Self {
        Self::Storage(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            value.to_string(),
        )))
    }
}

#[derive(Debug, Clone)]
pub struct ProjectRegistryStore {
    root: PathBuf,
}

impl ProjectRegistryStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn upsert(
        &self,
        request: ProjectUpsertRequest,
        apps_domain: Option<&str>,
    ) -> Result<ProjectRecord, ProjectRegistryError> {
        let project_id = normalize_project_id(&request.project_id)?;
        let repo_url = normalize_repo_url(&request.repo_url)?;
        let default_branch = normalize_default_branch(&request.default_branch)?;
        let existing = self.get(&project_id)?;
        let created_at_unix = existing
            .as_ref()
            .map(|project| project.created_at_unix)
            .unwrap_or_else(unix_now);
        let (domain_mode, base_domain) = resolve_domain(
            &project_id,
            request.base_domain,
            existing.as_ref(),
            apps_domain,
        )?;

        if let Some(existing) = existing {
            if existing.repo_url == repo_url
                && existing.default_branch == default_branch
                && existing.base_domain == base_domain
                && existing.domain_mode == domain_mode
            {
                return Ok(existing);
            }

            let updated = ProjectRecord {
                project_id: project_id.clone(),
                repo_url,
                default_branch,
                base_domain,
                domain_mode,
                created_at_unix,
                updated_at_unix: unix_now(),
            };
            self.write(&updated)?;
            return Ok(updated);
        }

        let created = ProjectRecord {
            project_id: project_id.clone(),
            repo_url,
            default_branch,
            base_domain,
            domain_mode,
            created_at_unix,
            updated_at_unix: created_at_unix,
        };
        self.write(&created)?;
        Ok(created)
    }

    pub fn list(&self) -> Result<Vec<ProjectRecord>, ProjectRegistryError> {
        let root = self.projects_root();
        if !root.exists() {
            return Ok(Vec::new());
        }

        let mut projects: Vec<ProjectRecord> = Vec::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let path = entry.path().join("project.json");
            if !path.exists() {
                continue;
            }
            let raw = fs::read_to_string(path)?;
            projects.push(serde_json::from_str(&raw)?);
        }
        projects.sort_by(|left, right| left.project_id.cmp(&right.project_id));
        Ok(projects)
    }

    pub fn get(&self, project_id: &str) -> Result<Option<ProjectRecord>, ProjectRegistryError> {
        let project_id = normalize_project_id(project_id)?;
        let path = self.project_file(&project_id);
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path)?;
        Ok(Some(serde_json::from_str(&raw)?))
    }

    fn write(&self, project: &ProjectRecord) -> Result<(), ProjectRegistryError> {
        let payload = serde_json::to_vec_pretty(project)?;
        let mut payload = payload;
        payload.push(b'\n');
        atomic_write(self.project_file(&project.project_id), &payload)?;
        Ok(())
    }

    fn projects_root(&self) -> PathBuf {
        self.root.join("projects")
    }

    fn project_file(&self, project_id: &str) -> PathBuf {
        self.projects_root().join(project_id).join("project.json")
    }
}

pub fn project_registry_error_response(
    err: ProjectRegistryError,
) -> (axum::http::StatusCode, ErrorResponse) {
    match err {
        ProjectRegistryError::Storage(message) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ErrorResponse {
                code: "project_registry_unavailable".into(),
                message: message.to_string(),
            },
        ),
        ProjectRegistryError::InvalidProjectId => (
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse {
                code: "invalid_project_id".into(),
                message: "project_id must use lowercase letters, digits, and hyphens only".into(),
            },
        ),
        ProjectRegistryError::InvalidRepoUrl(message) => (
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse {
                code: "invalid_repo_url".into(),
                message,
            },
        ),
        ProjectRegistryError::InvalidDefaultBranch => (
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse {
                code: "invalid_default_branch".into(),
                message: "default_branch must not be empty".into(),
            },
        ),
        ProjectRegistryError::InvalidBaseDomain(message) => (
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse {
                code: "invalid_base_domain".into(),
                message,
            },
        ),
        ProjectRegistryError::MissingAppsDomain => (
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse {
                code: "apps_domain_required".into(),
                message: "FORGE_APPS_DOMAIN is required when base_domain is not provided".into(),
            },
        ),
    }
}

fn resolve_domain(
    project_id: &str,
    requested_base_domain: Option<String>,
    existing: Option<&ProjectRecord>,
    apps_domain: Option<&str>,
) -> Result<(String, String), ProjectRegistryError> {
    if let Some(base_domain) = requested_base_domain {
        let normalized = normalize_hostname(&base_domain)?;
        return Ok(("explicit".into(), normalized));
    }

    if let Some(existing) = existing {
        return Ok((existing.domain_mode.clone(), existing.base_domain.clone()));
    }

    let apps_domain = apps_domain.ok_or(ProjectRegistryError::MissingAppsDomain)?;
    let apps_domain = normalize_hostname(apps_domain)?;
    let shortid = generate_shortid(project_id);
    let generated = format!("{project_id}-{shortid}.{apps_domain}");
    let generated = normalize_hostname(&generated)?;
    Ok(("generated".into(), generated))
}

fn normalize_project_id(input: &str) -> Result<String, ProjectRegistryError> {
    let value = input.trim();
    if value.is_empty()
        || value.len() > 63
        || value.starts_with('-')
        || value.ends_with('-')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(ProjectRegistryError::InvalidProjectId);
    }
    Ok(value.to_string())
}

fn normalize_repo_url(input: &str) -> Result<String, ProjectRegistryError> {
    let value = input.trim();
    if value.is_empty() {
        return Err(ProjectRegistryError::InvalidRepoUrl(
            "repo_url must not be empty".into(),
        ));
    }

    if value.starts_with("http://") || value.starts_with("https://") {
        let parsed = reqwest::Url::parse(value).map_err(|err| {
            ProjectRegistryError::InvalidRepoUrl(format!("repo_url is invalid: {err}"))
        })?;
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(ProjectRegistryError::InvalidRepoUrl(
                "repo_url must not contain embedded credentials".into(),
            ));
        }
    }

    Ok(value.to_string())
}

fn normalize_default_branch(input: &str) -> Result<String, ProjectRegistryError> {
    let value = input.trim();
    if value.is_empty() {
        return Err(ProjectRegistryError::InvalidDefaultBranch);
    }
    Ok(value.to_string())
}

fn normalize_hostname(input: &str) -> Result<String, ProjectRegistryError> {
    let value = input.trim().to_ascii_lowercase();
    if value.is_empty() || value.len() > 253 || !value.contains('.') {
        return Err(ProjectRegistryError::InvalidBaseDomain(
            "base_domain must be a valid DNS hostname".into(),
        ));
    }

    for label in value.split('.') {
        if label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(ProjectRegistryError::InvalidBaseDomain(
                "base_domain must be a valid DNS hostname".into(),
            ));
        }
    }

    Ok(value)
}

fn generate_shortid(project_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(project_id.as_bytes());
    hasher.update(unix_now().to_string().as_bytes());
    hasher.update(std::process::id().to_string().as_bytes());
    hasher.update(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_string()
            .as_bytes(),
    );
    let digest = hasher.finalize();
    let mut shortid = String::with_capacity(8);
    for byte in &digest[..4] {
        shortid.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        shortid.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
    }
    shortid
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
fn test_root(name: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let base = std::env::temp_dir().join(format!(
        "forge-project-tests-{name}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&base).unwrap();
    base
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(project_id: &str) -> ProjectUpsertRequest {
        ProjectUpsertRequest {
            project_id: project_id.into(),
            repo_url: "https://github.com/example/api.git".into(),
            default_branch: "main".into(),
            base_domain: Some("api.example.com".into()),
        }
    }

    #[test]
    fn project_add_persists_registry_entry() {
        let root = test_root("persist");
        let store = ProjectRegistryStore::new(&root);

        let created = store.upsert(request("api"), None).unwrap();
        let loaded = store.get("api").unwrap().unwrap();

        assert_eq!(created, loaded);
    }

    #[test]
    fn project_add_is_idempotent() {
        let root = test_root("idempotent");
        let store = ProjectRegistryStore::new(&root);

        let first = store.upsert(request("api"), None).unwrap();
        let second = store.upsert(request("api"), None).unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn project_list_returns_registered_projects() {
        let root = test_root("list");
        let store = ProjectRegistryStore::new(&root);

        store.upsert(request("api"), None).unwrap();
        store.upsert(request("web"), None).unwrap();

        let projects = store.list().unwrap();
        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].project_id, "api");
        assert_eq!(projects[1].project_id, "web");
    }

    #[test]
    fn project_show_returns_project() {
        let root = test_root("show");
        let store = ProjectRegistryStore::new(&root);
        let expected = store.upsert(request("api"), None).unwrap();

        let actual = store.get("api").unwrap().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn project_add_rejects_invalid_project_id() {
        let root = test_root("invalid-project");
        let store = ProjectRegistryStore::new(&root);

        let err = store.upsert(request("Api"), None).unwrap_err();
        assert!(matches!(err, ProjectRegistryError::InvalidProjectId));
    }

    #[test]
    fn project_add_does_not_store_tokens() {
        let root = test_root("reject-token");
        let store = ProjectRegistryStore::new(&root);
        let mut request = request("api");
        request.repo_url = "https://token@github.com/example/api.git".into();

        let err = store.upsert(request, None).unwrap_err();
        assert!(matches!(err, ProjectRegistryError::InvalidRepoUrl(_)));
        assert!(store.get("api").unwrap().is_none());
    }

    #[test]
    fn project_add_generates_domain_when_omitted() {
        let root = test_root("generated-domain");
        let store = ProjectRegistryStore::new(&root);
        let mut request = request("api");
        request.base_domain = None;

        let created = store.upsert(request, Some("forge.example.com")).unwrap();
        assert_eq!(created.domain_mode, "generated");
        assert!(created.base_domain.starts_with("api-"));
        assert!(created.base_domain.ends_with(".forge.example.com"));
    }

    #[test]
    fn project_add_preserves_generated_domain_on_update() {
        let root = test_root("preserve-generated");
        let store = ProjectRegistryStore::new(&root);
        let mut request = request("api");
        request.base_domain = None;

        let created = store
            .upsert(request.clone(), Some("forge.example.com"))
            .unwrap();
        request.repo_url = "https://github.com/example/new-api.git".into();
        let updated = store.upsert(request, Some("forge.example.com")).unwrap();

        assert_eq!(updated.domain_mode, "generated");
        assert_eq!(updated.base_domain, created.base_domain);
        assert_eq!(updated.created_at_unix, created.created_at_unix);
    }

    #[test]
    fn project_add_uses_explicit_domain_when_provided() {
        let root = test_root("explicit-domain");
        let store = ProjectRegistryStore::new(&root);

        let created = store
            .upsert(request("api"), Some("forge.example.com"))
            .unwrap();
        assert_eq!(created.domain_mode, "explicit");
        assert_eq!(created.base_domain, "api.example.com");
    }

    #[test]
    fn project_add_requires_apps_domain_for_generated_domain() {
        let root = test_root("missing-apps-domain");
        let store = ProjectRegistryStore::new(&root);
        let mut request = request("api");
        request.base_domain = None;

        let err = store.upsert(request, None).unwrap_err();
        assert!(matches!(err, ProjectRegistryError::MissingAppsDomain));
    }
}
