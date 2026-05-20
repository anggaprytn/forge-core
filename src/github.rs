use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::api::DeploymentRequest;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubWebhookConfig {
    pub secret: String,
    pub repository_cache_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookResolution {
    Enqueue(DeploymentRequest),
    Ignore { reason: String },
}

#[derive(Debug)]
pub enum GitHubError {
    InvalidSignature,
    UnsupportedEvent(String),
    InvalidPayload(String),
    GitCommand(String),
    Manifest(String),
}

impl Display for GitHubError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSignature => write!(f, "invalid github signature"),
            Self::UnsupportedEvent(event) => write!(f, "unsupported github event {event}"),
            Self::InvalidPayload(message) => write!(f, "{message}"),
            Self::GitCommand(message) => write!(f, "{message}"),
            Self::Manifest(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for GitHubError {}

#[derive(Debug, Deserialize)]
struct GitHubPushPayload {
    #[serde(rename = "ref")]
    git_ref: String,
    after: String,
    deleted: Option<bool>,
    repository: GitHubRepository,
}

#[derive(Debug, Deserialize)]
struct GitHubRepository {
    clone_url: String,
}

#[derive(Debug, Deserialize)]
struct ForgeManifest {
    forge_schema_version: u64,
    project_id: String,
    repository: ForgeManifestRepository,
    environments: ForgeManifestEnvironments,
}

#[derive(Debug, Deserialize)]
struct ForgeManifestRepository {
    provider: String,
}

#[derive(Debug, Deserialize)]
struct ForgeManifestEnvironments {
    development: ForgeManifestEnvironment,
    staging: ForgeManifestEnvironment,
    production: ForgeManifestEnvironment,
}

#[derive(Debug, Deserialize)]
struct ForgeManifestEnvironment {
    branch: String,
}

pub fn verify_signature(secret: &str, body: &[u8], provided: &str) -> Result<(), GitHubError> {
    let Some(provided) = provided.strip_prefix("sha256=") else {
        return Err(GitHubError::InvalidSignature);
    };
    let provided = hex::decode(provided).map_err(|_| GitHubError::InvalidSignature)?;
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| GitHubError::InvalidSignature)?;
    mac.update(body);
    let expected = mac.finalize().into_bytes();
    if expected.as_slice().ct_eq(provided.as_slice()).into() {
        Ok(())
    } else {
        Err(GitHubError::InvalidSignature)
    }
}

pub fn resolve_webhook(
    config: &GitHubWebhookConfig,
    event: &str,
    body: &[u8],
) -> Result<WebhookResolution, GitHubError> {
    if event != "push" {
        return Err(GitHubError::UnsupportedEvent(event.to_string()));
    }

    let payload = serde_json::from_slice::<GitHubPushPayload>(body)
        .map_err(|err| GitHubError::InvalidPayload(err.to_string()))?;
    if payload.deleted.unwrap_or(false) || is_zero_commit(&payload.after) {
        return Ok(WebhookResolution::Ignore {
            reason: "deleted_ref".into(),
        });
    }

    let branch = payload
        .git_ref
        .strip_prefix("refs/heads/")
        .ok_or_else(|| GitHubError::InvalidPayload("webhook ref must be a branch".into()))?;

    let manifest = load_manifest_at_commit(
        &config.repository_cache_root,
        &payload.repository.clone_url,
        &payload.after,
    )?;
    if manifest.forge_schema_version != 1 {
        return Err(GitHubError::Manifest(
            "forge_schema_version must equal 1".into(),
        ));
    }
    if manifest.repository.provider != "github" {
        return Err(GitHubError::Manifest(
            "manifest repository provider must be github".into(),
        ));
    }

    let Some(environment) = branch_to_environment(&manifest, branch) else {
        return Ok(WebhookResolution::Ignore {
            reason: format!("branch_not_mapped:{branch}"),
        });
    };

    Ok(WebhookResolution::Enqueue(DeploymentRequest {
        project_id: manifest.project_id,
        environment: environment.into(),
        intent: "deploy".into(),
        source_path: None,
        source_ref: None,
    }))
}

fn branch_to_environment(manifest: &ForgeManifest, branch: &str) -> Option<&'static str> {
    let mut matched = Vec::new();
    if manifest.environments.development.branch == branch {
        matched.push("development");
    }
    if manifest.environments.staging.branch == branch {
        matched.push("staging");
    }
    if manifest.environments.production.branch == branch {
        matched.push("production");
    }
    if matched.len() == 1 {
        Some(matched[0])
    } else {
        None
    }
}

fn load_manifest_at_commit(
    repository_cache_root: &Path,
    clone_url: &str,
    commit_sha: &str,
) -> Result<ForgeManifest, GitHubError> {
    let repo_path = prepare_repository(repository_cache_root, clone_url)?;
    git(
        repository_cache_root,
        &[
            "-C",
            repo_path.to_str().unwrap_or_default(),
            "fetch",
            "--depth",
            "1",
            "origin",
            commit_sha,
        ],
    )?;
    let manifest_raw = git_output(
        repository_cache_root,
        &[
            "-C",
            repo_path.to_str().unwrap_or_default(),
            "show",
            &format!("{commit_sha}:forge.project.json"),
        ],
    )?;
    serde_json::from_str(&manifest_raw).map_err(|err| GitHubError::Manifest(err.to_string()))
}

fn prepare_repository(root: &Path, clone_url: &str) -> Result<PathBuf, GitHubError> {
    fs::create_dir_all(root).map_err(|err| GitHubError::GitCommand(err.to_string()))?;
    let repo_path = root.join(sanitize_repository_id(clone_url));
    if repo_path.exists() {
        git(
            root,
            &[
                "-C",
                repo_path.to_str().unwrap_or_default(),
                "remote",
                "set-url",
                "origin",
                clone_url,
            ],
        )?;
        return Ok(repo_path);
    }

    git(
        root,
        &[
            "clone",
            "--mirror",
            clone_url,
            repo_path.to_str().unwrap_or_default(),
        ],
    )?;
    Ok(repo_path)
}

fn git(root: &Path, args: &[&str]) -> Result<(), GitHubError> {
    git_output(root, args).map(|_| ())
}

fn git_output(root: &Path, args: &[&str]) -> Result<String, GitHubError> {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .map_err(|err| GitHubError::GitCommand(err.to_string()))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        Err(GitHubError::GitCommand(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ))
    }
}

fn sanitize_repository_id(clone_url: &str) -> String {
    clone_url
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect()
}

fn is_zero_commit(commit_sha: &str) -> bool {
    !commit_sha.is_empty() && commit_sha.chars().all(|ch| ch == '0')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn signature_verification_accepts_valid_digest() {
        let body = br#"{"ok":true}"#;
        let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
        mac.update(body);
        let digest = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
        assert!(verify_signature("secret", body, &digest).is_ok());
    }

    #[test]
    fn resolve_webhook_maps_branch_from_exact_commit_manifest() {
        let root = test_root("github-resolve");
        let repo = create_repo_with_manifest(
            &root,
            r#"{
  "forge_schema_version": 1,
  "project_id": "api",
  "repository": { "provider": "github" },
  "environments": {
    "development": { "branch": "dev" },
    "staging": { "branch": "release" },
    "production": { "branch": "main" }
  }
}"#,
            "main",
        );
        let commit_sha =
            git_output(&root, &["-C", repo.to_str().unwrap(), "rev-parse", "HEAD"]).unwrap();
        let payload = format!(
            r#"{{
  "ref": "refs/heads/main",
  "after": "{commit_sha}",
  "repository": {{ "clone_url": "{}" }}
}}"#,
            repo.to_str().unwrap()
        );
        let result = resolve_webhook(
            &GitHubWebhookConfig {
                secret: "secret".into(),
                repository_cache_root: root.join("cache"),
            },
            "push",
            payload.as_bytes(),
        )
        .unwrap();
        assert_eq!(
            result,
            WebhookResolution::Enqueue(DeploymentRequest {
                project_id: "api".into(),
                environment: "production".into(),
                intent: "deploy".into(),
                source_path: None,
                source_ref: None,
            })
        );
    }

    fn create_repo_with_manifest(root: &Path, manifest: &str, branch: &str) -> PathBuf {
        let repo = root.join("repo");
        fs::create_dir_all(&repo).unwrap();
        git(root, &["init", repo.to_str().unwrap()]).unwrap();
        git(
            root,
            &["-C", repo.to_str().unwrap(), "checkout", "-b", branch],
        )
        .unwrap();
        fs::write(repo.join("forge.project.json"), manifest).unwrap();
        git(
            root,
            &["-C", repo.to_str().unwrap(), "add", "forge.project.json"],
        )
        .unwrap();
        git(
            root,
            &[
                "-C",
                repo.to_str().unwrap(),
                "-c",
                "user.name=Forge Test",
                "-c",
                "user.email=forge@example.com",
                "commit",
                "-m",
                "manifest",
            ],
        )
        .unwrap();
        repo
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
}
