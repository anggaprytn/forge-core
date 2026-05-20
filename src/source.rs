use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::projects::{ProjectRegistryError, ProjectRegistryStore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDeploymentSource {
    pub source_path: Option<PathBuf>,
    pub source_ref: Option<String>,
    pub repo_url: Option<String>,
    pub commit_sha: Option<String>,
}

#[derive(Debug)]
pub enum SourceResolverError {
    Io(std::io::Error),
    ProjectRegistry(ProjectRegistryError),
    ProjectNotFound(String),
    InvalidSourcePath(String),
    InvalidSourceRef,
    InvalidRepoUrl(String),
    GitCommand(String),
    CheckoutConflict {
        path: PathBuf,
        repo_url: String,
        source_ref: String,
        commit_sha: String,
    },
}

impl Display for SourceResolverError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::ProjectRegistry(err) => write!(f, "{err}"),
            Self::ProjectNotFound(project_id) => {
                write!(f, "project is not registered: {project_id}")
            }
            Self::InvalidSourcePath(message) => write!(f, "{message}"),
            Self::InvalidSourceRef => write!(f, "source_ref must not be empty"),
            Self::InvalidRepoUrl(message) => write!(f, "{message}"),
            Self::GitCommand(message) => write!(f, "{message}"),
            Self::CheckoutConflict {
                path,
                repo_url,
                source_ref,
                commit_sha,
            } => write!(
                f,
                "source checkout path already exists but does not match the requested commit: path={} repo={} ref={} sha={}",
                path.display(),
                repo_url,
                source_ref,
                commit_sha
            ),
        }
    }
}

impl std::error::Error for SourceResolverError {}

impl From<std::io::Error> for SourceResolverError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ProjectRegistryError> for SourceResolverError {
    fn from(value: ProjectRegistryError) -> Self {
        Self::ProjectRegistry(value)
    }
}

pub struct SourceResolver {
    storage_root: PathBuf,
    projects: ProjectRegistryStore,
}

impl SourceResolver {
    pub fn new(storage_root: impl AsRef<Path>) -> Self {
        let storage_root = storage_root.as_ref().to_path_buf();
        Self {
            projects: ProjectRegistryStore::new(&storage_root),
            storage_root,
        }
    }

    pub fn resolve(
        &self,
        project_id: &str,
        source_path: Option<&Path>,
        source_ref: Option<&str>,
    ) -> Result<ResolvedDeploymentSource, SourceResolverError> {
        if let Some(source_path) = source_path {
            return Ok(ResolvedDeploymentSource {
                source_path: Some(resolve_local_source_path(source_path)?),
                source_ref: None,
                repo_url: None,
                commit_sha: None,
            });
        }

        let requested_ref = normalize_source_ref(source_ref)?;
        let project = self
            .projects
            .get(project_id)?
            .ok_or_else(|| SourceResolverError::ProjectNotFound(project_id.to_string()))?;
        let repo_url = normalize_repo_url(&project.repo_url)?;
        let source_ref = requested_ref.unwrap_or(project.default_branch);
        let repository_path = self.repository_cache_path(project_id);
        prepare_repository(&repository_path, &repo_url)?;
        fetch_repository(&repository_path)?;
        let commit_sha = resolve_commit_sha(&repository_path, &source_ref)
            .map_err(|err| err.with_resolution_context(&repo_url, &source_ref, None))?;
        let source_path = ensure_checkout(
            &self.source_checkout_path(project_id, &commit_sha),
            &repository_path,
            &repo_url,
            &source_ref,
            &commit_sha,
        )
        .map_err(|err| err.with_resolution_context(&repo_url, &source_ref, Some(&commit_sha)))?;

        Ok(ResolvedDeploymentSource {
            source_path: Some(source_path),
            source_ref: Some(source_ref),
            repo_url: Some(repo_url),
            commit_sha: Some(commit_sha),
        })
    }

    fn repository_cache_path(&self, project_id: &str) -> PathBuf {
        self.storage_root.join("repositories").join(project_id)
    }

    fn source_checkout_path(&self, project_id: &str, commit_sha: &str) -> PathBuf {
        self.storage_root
            .join("source-checkouts")
            .join(project_id)
            .join(commit_sha)
    }
}

impl SourceResolverError {
    fn with_resolution_context(
        self,
        repo_url: &str,
        source_ref: &str,
        commit_sha: Option<&str>,
    ) -> Self {
        match self {
            Self::GitCommand(message) => Self::GitCommand(format_resolution_failure(
                repo_url, source_ref, commit_sha, &message,
            )),
            other => other,
        }
    }
}

fn resolve_local_source_path(source_path: &Path) -> Result<PathBuf, SourceResolverError> {
    let resolved = fs::canonicalize(source_path).map_err(|err| {
        SourceResolverError::InvalidSourcePath(format!(
            "source path `{}` is not accessible on the daemon host: {err}",
            source_path.display()
        ))
    })?;
    if !resolved.is_dir() {
        return Err(SourceResolverError::InvalidSourcePath(format!(
            "source path `{}` must be an existing directory",
            resolved.display()
        )));
    }
    Ok(resolved)
}

fn normalize_source_ref(source_ref: Option<&str>) -> Result<Option<String>, SourceResolverError> {
    let Some(source_ref) = source_ref else {
        return Ok(None);
    };
    let source_ref = source_ref.trim();
    if source_ref.is_empty() {
        return Err(SourceResolverError::InvalidSourceRef);
    }
    Ok(Some(source_ref.to_string()))
}

fn normalize_repo_url(repo_url: &str) -> Result<String, SourceResolverError> {
    let repo_url = repo_url.trim();
    if repo_url.is_empty() {
        return Err(SourceResolverError::InvalidRepoUrl(
            "repo_url must not be empty".into(),
        ));
    }
    if repo_url.starts_with("http://") || repo_url.starts_with("https://") {
        let parsed = reqwest::Url::parse(repo_url).map_err(|err| {
            SourceResolverError::InvalidRepoUrl(format!("repo_url is invalid: {err}"))
        })?;
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(SourceResolverError::InvalidRepoUrl(
                "repo_url must not contain embedded credentials".into(),
            ));
        }
    }
    Ok(repo_url.to_string())
}

fn prepare_repository(repository_path: &Path, repo_url: &str) -> Result<(), SourceResolverError> {
    fs::create_dir_all(
        repository_path
            .parent()
            .expect("repository cache path should have a parent"),
    )?;
    if repository_path.exists() {
        if !repository_path.is_dir() {
            return Err(SourceResolverError::InvalidSourcePath(format!(
                "repository cache path is not a directory: {}",
                repository_path.display()
            )));
        }
        git(
            repository_path.parent().unwrap_or_else(|| Path::new("/")),
            &[
                "-C",
                repository_path.to_str().unwrap_or_default(),
                "remote",
                "set-url",
                "origin",
                repo_url,
            ],
        )?;
        return Ok(());
    }

    git(
        repository_path.parent().unwrap_or_else(|| Path::new("/")),
        &[
            "clone",
            "--no-checkout",
            repo_url,
            repository_path.to_str().unwrap_or_default(),
        ],
    )
}

fn fetch_repository(repository_path: &Path) -> Result<(), SourceResolverError> {
    git(
        repository_path,
        &[
            "-C",
            repository_path.to_str().unwrap_or_default(),
            "fetch",
            "--prune",
            "--tags",
            "origin",
        ],
    )
}

fn resolve_commit_sha(
    repository_path: &Path,
    source_ref: &str,
) -> Result<String, SourceResolverError> {
    for candidate in ref_candidates(source_ref) {
        if let Some(commit_sha) = rev_parse(repository_path, &candidate)? {
            return Ok(commit_sha);
        }
    }

    git(
        repository_path,
        &[
            "-C",
            repository_path.to_str().unwrap_or_default(),
            "fetch",
            "--depth",
            "1",
            "origin",
            source_ref,
        ],
    )?;

    rev_parse(repository_path, "FETCH_HEAD^{commit}")?.ok_or_else(|| {
        SourceResolverError::GitCommand(format!("unable to resolve git ref `{source_ref}`"))
    })
}

fn ref_candidates(source_ref: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    if let Some(branch_name) = source_ref.strip_prefix("refs/heads/") {
        candidates.push(format!("refs/remotes/origin/{branch_name}^{{commit}}"));
    } else if source_ref.starts_with("refs/remotes/origin/") {
        candidates.push(format!("{source_ref}^{{commit}}"));
    } else if source_ref.starts_with("refs/tags/") {
        candidates.push(format!("{source_ref}^{{commit}}"));
    } else {
        candidates.push(format!("refs/remotes/origin/{source_ref}^{{commit}}"));
        candidates.push(format!("refs/tags/{source_ref}^{{commit}}"));
    }
    candidates.push(format!("{source_ref}^{{commit}}"));
    candidates
}

fn ensure_checkout(
    checkout_path: &Path,
    repository_path: &Path,
    repo_url: &str,
    source_ref: &str,
    commit_sha: &str,
) -> Result<PathBuf, SourceResolverError> {
    if checkout_path.exists() {
        let head = rev_parse(checkout_path, "HEAD^{commit}")?;
        if head.as_deref() == Some(commit_sha)
            && checkout_metadata_matches(checkout_path, repo_url, commit_sha)?
        {
            return Ok(checkout_path.to_path_buf());
        }
        return Err(SourceResolverError::CheckoutConflict {
            path: checkout_path.to_path_buf(),
            repo_url: repo_url.to_string(),
            source_ref: source_ref.to_string(),
            commit_sha: commit_sha.to_string(),
        });
    }

    fs::create_dir_all(
        checkout_path
            .parent()
            .expect("source checkout path should have a parent"),
    )?;
    git(
        repository_path,
        &[
            "-C",
            repository_path.to_str().unwrap_or_default(),
            "worktree",
            "add",
            "--detach",
            checkout_path.to_str().unwrap_or_default(),
            commit_sha,
        ],
    )?;
    write_checkout_metadata(checkout_path, repo_url, source_ref, commit_sha)?;
    Ok(checkout_path.to_path_buf())
}

#[derive(Debug, Serialize, Deserialize)]
struct CheckoutMetadata {
    repo_url: String,
    source_ref: String,
    commit_sha: String,
}

fn checkout_metadata_matches(
    checkout_path: &Path,
    repo_url: &str,
    commit_sha: &str,
) -> Result<bool, SourceResolverError> {
    let path = checkout_metadata_path(checkout_path);
    if !path.exists() {
        return Ok(false);
    }
    let raw = fs::read_to_string(path)?;
    let metadata: CheckoutMetadata = serde_json::from_str(&raw).map_err(|err| {
        SourceResolverError::GitCommand(format!(
            "invalid source checkout metadata in {}: {err}",
            checkout_path.display()
        ))
    })?;
    Ok(metadata.repo_url == repo_url && metadata.commit_sha == commit_sha)
}

fn write_checkout_metadata(
    checkout_path: &Path,
    repo_url: &str,
    source_ref: &str,
    commit_sha: &str,
) -> Result<(), SourceResolverError> {
    let metadata = CheckoutMetadata {
        repo_url: repo_url.to_string(),
        source_ref: source_ref.to_string(),
        commit_sha: commit_sha.to_string(),
    };
    let bytes = serde_json::to_vec_pretty(&metadata).map_err(|err| {
        SourceResolverError::GitCommand(format!(
            "failed to serialize source checkout metadata for {}: {err}",
            checkout_path.display()
        ))
    })?;
    fs::write(checkout_metadata_path(checkout_path), bytes)?;
    Ok(())
}

fn checkout_metadata_path(checkout_path: &Path) -> PathBuf {
    checkout_path.join(".forge-source.json")
}

fn format_resolution_failure(
    repo_url: &str,
    source_ref: &str,
    commit_sha: Option<&str>,
    message: &str,
) -> String {
    let sha = commit_sha.unwrap_or("unknown");
    format!("source resolution failed: repo={repo_url} ref={source_ref} sha={sha}: {message}")
}

fn rev_parse(
    repository_path: &Path,
    revision: &str,
) -> Result<Option<String>, SourceResolverError> {
    let output = Command::new("git")
        .current_dir(repository_path)
        .args([
            "-C",
            repository_path.to_str().unwrap_or_default(),
            "rev-parse",
            "--verify",
            "--quiet",
            revision,
        ])
        .output()
        .map_err(|err| SourceResolverError::GitCommand(err.to_string()))?;
    if output.status.success() {
        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if value.is_empty() {
            Ok(None)
        } else {
            Ok(Some(value))
        }
    } else {
        Ok(None)
    }
}

fn git(root: &Path, args: &[&str]) -> Result<(), SourceResolverError> {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .map_err(|err| SourceResolverError::GitCommand(err.to_string()))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(SourceResolverError::GitCommand(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ))
    }
}

#[cfg(test)]
fn test_root(name: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let base = std::env::temp_dir().join(format!(
        "forge-source-tests-{name}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&base).unwrap();
    base
}

#[cfg(test)]
fn git_test(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(root)
        .env("GIT_AUTHOR_NAME", "Forge Tests")
        .env("GIT_AUTHOR_EMAIL", "forge-tests@example.com")
        .env("GIT_COMMITTER_NAME", "Forge Tests")
        .env("GIT_COMMITTER_EMAIL", "forge-tests@example.com")
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[cfg(test)]
fn create_project_registry(storage_root: &Path, project_id: &str, repo_url: &str, branch: &str) {
    use crate::api::ProjectUpsertRequest;

    ProjectRegistryStore::new(storage_root)
        .upsert(
            ProjectUpsertRequest {
                project_id: Some(project_id.into()),
                repo_url: repo_url.into(),
                default_branch: branch.into(),
                base_domain: Some(format!("{project_id}.example.com")),
            },
            None,
        )
        .unwrap();
}

#[cfg(test)]
fn create_git_repo(root: &Path) -> (PathBuf, String) {
    let remote = root.join("remote");
    fs::create_dir_all(&remote).unwrap();
    git_test(
        root,
        &["init", "--initial-branch=main", remote.to_str().unwrap()],
    );
    fs::write(remote.join("README.md"), "v1\n").unwrap();
    git_test(&remote, &["add", "README.md"]);
    git_test(&remote, &["commit", "-m", "initial"]);
    let commit_sha = git_test(&remote, &["rev-parse", "HEAD"]);
    (remote, commit_sha)
}

#[cfg(test)]
fn commit_file(repo: &Path, path: &str, contents: &str, message: &str) -> String {
    fs::write(repo.join(path), contents).unwrap();
    git_test(repo, &["add", path]);
    git_test(repo, &["commit", "-m", message]);
    git_test(repo, &["rev-parse", "HEAD"])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deploy_by_ref_clones_repository() {
        let root = test_root("clone-repository");
        let (remote, _) = create_git_repo(&root);
        create_project_registry(&root, "api", remote.to_str().unwrap(), "main");

        let resolved = SourceResolver::new(&root)
            .resolve("api", None, Some("main"))
            .unwrap();

        assert!(root.join("repositories").join("api").exists());
        assert!(resolved.source_path.as_ref().unwrap().exists());
        assert_eq!(resolved.source_ref.as_deref(), Some("main"));
    }

    #[test]
    fn deploy_by_ref_reuses_cached_repository() {
        let root = test_root("reuse-repository");
        let (remote, _) = create_git_repo(&root);
        create_project_registry(&root, "api", remote.to_str().unwrap(), "main");
        let resolver = SourceResolver::new(&root);

        let first = resolver.resolve("api", None, Some("main")).unwrap();
        let cache = root.join("repositories").join("api");
        let git_dir = fs::metadata(cache.join(".git"))
            .unwrap()
            .modified()
            .unwrap();
        let second = resolver.resolve("api", None, Some("main")).unwrap();

        assert_eq!(first.source_path, second.source_path);
        assert!(
            fs::metadata(cache.join(".git"))
                .unwrap()
                .modified()
                .unwrap()
                >= git_dir
        );
    }

    #[test]
    fn deploy_by_ref_resolves_commit_sha() {
        let root = test_root("resolve-commit-sha");
        let (remote, commit_sha) = create_git_repo(&root);
        create_project_registry(&root, "api", remote.to_str().unwrap(), "main");

        let resolved = SourceResolver::new(&root)
            .resolve("api", None, Some("main"))
            .unwrap();

        assert_eq!(resolved.commit_sha.as_deref(), Some(commit_sha.as_str()));
    }

    #[test]
    fn deploy_by_ref_uses_default_branch_when_ref_omitted() {
        let root = test_root("default-branch");
        let (remote, commit_sha) = create_git_repo(&root);
        create_project_registry(&root, "api", remote.to_str().unwrap(), "main");

        let resolved = SourceResolver::new(&root)
            .resolve("api", None, None)
            .unwrap();

        assert_eq!(resolved.source_ref.as_deref(), Some("main"));
        assert_eq!(resolved.commit_sha.as_deref(), Some(commit_sha.as_str()));
    }

    #[test]
    fn deploy_by_ref_reuses_existing_checkout() {
        let root = test_root("reuse-checkout");
        let (remote, commit_sha) = create_git_repo(&root);
        create_project_registry(&root, "api", remote.to_str().unwrap(), "main");
        let resolver = SourceResolver::new(&root);

        let first = resolver.resolve("api", None, Some("main")).unwrap();
        let second = resolver.resolve("api", None, Some("main")).unwrap();

        assert_eq!(first.source_path, second.source_path);
        assert_eq!(second.commit_sha.as_deref(), Some(commit_sha.as_str()));
    }

    #[test]
    fn deploy_by_ref_fetches_updated_remote_branch() {
        let root = test_root("fetch-updated-remote-branch");
        let (remote, first_commit) = create_git_repo(&root);
        create_project_registry(&root, "api", remote.to_str().unwrap(), "main");
        let resolver = SourceResolver::new(&root);

        let first = resolver.resolve("api", None, Some("main")).unwrap();
        let second_commit = commit_file(&remote, "forge.yml", "version: 2\n", "update forge");
        let second = resolver.resolve("api", None, Some("main")).unwrap();

        assert_eq!(first.commit_sha.as_deref(), Some(first_commit.as_str()));
        assert_eq!(second.commit_sha.as_deref(), Some(second_commit.as_str()));
        assert_ne!(first.source_path, second.source_path);
        assert_eq!(
            fs::read_to_string(second.source_path.unwrap().join("forge.yml")).unwrap(),
            "version: 2\n"
        );
    }

    #[test]
    fn deploy_by_ref_does_not_reuse_stale_branch_checkout() {
        let root = test_root("stale-branch-checkout");
        let (remote, first_commit) = create_git_repo(&root);
        create_project_registry(&root, "api", remote.to_str().unwrap(), "main");
        let resolver = SourceResolver::new(&root);

        resolver.resolve("api", None, Some("main")).unwrap();
        let cached_repo = root.join("repositories").join("api");
        git_test(&cached_repo, &["checkout", "--detach", &first_commit]);
        git_test(&cached_repo, &["branch", "-f", "main", &first_commit]);

        let second_commit = commit_file(&remote, "forge.yml", "runtime: node\n", "move main");
        let resolved = resolver.resolve("api", None, Some("main")).unwrap();

        assert_eq!(resolved.commit_sha.as_deref(), Some(second_commit.as_str()));
        assert_eq!(
            fs::read_to_string(resolved.source_path.unwrap().join("forge.yml")).unwrap(),
            "runtime: node\n"
        );
    }

    #[test]
    fn source_checkout_contains_resolved_commit_contents() {
        let root = test_root("checkout-contains-resolved-commit");
        let (remote, _) = create_git_repo(&root);
        let forge_yml = "version: 1\nruntime:\n  port: 8080\n";
        let commit_sha = commit_file(&remote, "forge.yml", forge_yml, "add forge manifest");
        create_project_registry(&root, "api", remote.to_str().unwrap(), "main");

        let resolved = SourceResolver::new(&root)
            .resolve("api", None, Some("main"))
            .unwrap();
        let source_path = resolved.source_path.unwrap();

        assert_eq!(resolved.commit_sha.as_deref(), Some(commit_sha.as_str()));
        assert_eq!(
            fs::read_to_string(source_path.join("forge.yml")).unwrap(),
            forge_yml
        );

        let metadata_raw = fs::read_to_string(source_path.join(".forge-source.json")).unwrap();
        let metadata: CheckoutMetadata = serde_json::from_str(&metadata_raw).unwrap();
        assert_eq!(metadata.repo_url, remote.to_str().unwrap());
        assert_eq!(metadata.source_ref, "main");
        assert_eq!(metadata.commit_sha, resolved.commit_sha.unwrap());
    }
}
