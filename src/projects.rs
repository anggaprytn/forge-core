use crate::api::{
    ErrorResponse, ProjectEnvironmentDetail, ProjectEnvironmentInventoryList,
    ProjectEnvironmentReadinessSummary, ProjectEnvironmentRestoreSummary,
    ProjectEnvironmentRollbackSummary, ProjectEnvironmentRuntimePolicySummary,
    ProjectEnvironmentServiceRuntimePolicySummary, ProjectEnvironmentServiceSummary,
    ProjectEnvironmentSummary, ProjectRecord, ProjectUpsertRequest,
};
use crate::backups::load_backup_restore_lineage;
use crate::status::derive_environment_domain;
use crate::storage::{
    ConvergenceCheckpointStore, EnvironmentPaths, GenerationHistoryRecord, PersistedActivationMode,
    PersistedRuntimeInfo, RetentionStore, StorageError, atomic_write, load_generation_build_info,
    load_generation_lifecycle, load_generation_runtime_info, load_generation_snapshot_metadata,
};
use crate::topology::runtime_with_primary_service;
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
    BaseDomainAlreadyInUse(String),
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
            Self::BaseDomainAlreadyInUse(base_domain) => {
                write!(
                    f,
                    "base_domain is already used by another project: {base_domain}"
                )
            }
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
    const GENERATED_DOMAIN_MAX_ATTEMPTS: usize = 4;

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
        let repo_url = normalize_repo_url(&request.repo_url)?;
        let project_id = resolve_project_id(request.project_id.as_deref(), &repo_url)?;
        let default_branch = normalize_default_branch(&request.default_branch)?;
        let existing = self.get(&project_id)?;
        let created_at_unix = existing
            .as_ref()
            .map(|project| project.created_at_unix)
            .unwrap_or_else(unix_now);
        let (domain_mode, base_domain) = resolve_domain(
            self,
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
                environments: Vec::new(),
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
            environments: Vec::new(),
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

    pub fn list_inventory(&self) -> Result<Vec<ProjectRecord>, ProjectRegistryError> {
        self.list()?
            .into_iter()
            .map(|project| self.decorate_project_inventory(project))
            .collect()
    }

    pub fn get_inventory(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectRecord>, ProjectRegistryError> {
        self.get(project_id)?
            .map(|project| self.decorate_project_inventory(project))
            .transpose()
    }

    pub fn get_environment_inventory(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectEnvironmentInventoryList>, ProjectRegistryError> {
        let Some(project) = self.get(project_id)? else {
            return Ok(None);
        };
        let environments = self
            .environment_names(&project.project_id)?
            .into_iter()
            .map(|environment| self.build_environment_detail(&project, &environment))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(ProjectEnvironmentInventoryList {
            project_id: project.project_id,
            environments,
        }))
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

    fn find_domain_owner(&self, base_domain: &str) -> Result<Option<String>, ProjectRegistryError> {
        Ok(self
            .list()?
            .into_iter()
            .find(|project| project.base_domain == base_domain)
            .map(|project| project.project_id))
    }

    fn decorate_project_inventory(
        &self,
        mut project: ProjectRecord,
    ) -> Result<ProjectRecord, ProjectRegistryError> {
        project.repo_url = sanitize_repo_url(&project.repo_url);
        project.environments = self
            .environment_names(&project.project_id)?
            .into_iter()
            .map(|environment| self.build_environment_summary(&project, &environment))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(project)
    }

    fn environment_names(&self, project_id: &str) -> Result<Vec<String>, ProjectRegistryError> {
        let root = self.projects_root().join(project_id).join("environments");
        if !root.exists() {
            return Ok(Vec::new());
        }

        let mut names = Vec::new();
        for environment in ["development", "staging", "production"] {
            if root.join(environment).is_dir() {
                names.push(environment.to_string());
            }
        }
        Ok(names)
    }

    fn build_environment_summary(
        &self,
        project: &ProjectRecord,
        environment: &str,
    ) -> Result<ProjectEnvironmentSummary, ProjectRegistryError> {
        let detail = self.build_environment_detail(project, environment)?;
        Ok(ProjectEnvironmentSummary {
            environment: detail.environment,
            current_generation: detail.current_generation,
            previous_generation: detail.previous_generation,
            route: detail.route,
            last_deployment_status: detail.last_deployment_status,
            readiness_summary: detail.readiness_summary,
        })
    }

    fn build_environment_detail(
        &self,
        project: &ProjectRecord,
        environment: &str,
    ) -> Result<ProjectEnvironmentDetail, ProjectRegistryError> {
        let env = EnvironmentPaths::new(&self.root, &project.project_id, environment);
        let current_generation = read_authoritative_generation(&env)?;
        let previous_generation = read_optional_pointer(env.previous_pointer())?;
        let latest_generation = latest_generation(&env)?;
        let selected_generation = current_generation
            .or(latest_generation)
            .or(previous_generation);
        let selected_snapshot = selected_generation
            .map(|generation| load_generation_snapshot_metadata(&env, generation))
            .transpose()?
            .flatten();
        let selected_build = selected_generation
            .map(|generation| load_generation_build_info(&env, generation))
            .transpose()?
            .flatten();
        let selected_lifecycle = selected_generation
            .map(|generation| load_generation_lifecycle(&env, generation))
            .transpose()?
            .flatten();
        let selected_runtime = selected_generation
            .map(|generation| load_generation_runtime_info(&env, generation))
            .transpose()?
            .flatten()
            .map(|runtime| runtime_with_primary_service(&runtime));
        let retention = RetentionStore::new(env.clone()).read()?;
        let history_record = selected_generation.and_then(|generation| {
            retention
                .generations
                .iter()
                .find(|record| record.generation == generation)
                .cloned()
        });
        let domain = derive_environment_domain(&project.base_domain, environment);
        let route = route_for_runtime(selected_runtime.as_ref(), &domain);
        let last_deployment_status = selected_lifecycle
            .as_ref()
            .map(|lifecycle| lifecycle.state.as_str().to_string())
            .or_else(|| {
                selected_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.state.clone())
            });
        let last_deployment_timestamp = selected_snapshot
            .as_ref()
            .map(|snapshot| snapshot.finalized_at_unix)
            .or_else(|| {
                selected_lifecycle
                    .as_ref()
                    .map(|lifecycle| lifecycle.entered_at_unix)
            })
            .or_else(|| {
                history_record
                    .as_ref()
                    .and_then(|record| record.finalized_at_unix)
            })
            .or_else(|| {
                history_record
                    .as_ref()
                    .and_then(|record| record.created_at_unix)
            });
        let restore_lineage = selected_generation.and_then(|generation| {
            let record = history_record
                .clone()
                .unwrap_or_else(|| GenerationHistoryRecord {
                    generation,
                    deployment_id: selected_build
                        .as_ref()
                        .map(|build| build.deployment_id.clone()),
                    finalized_at_unix: selected_snapshot
                        .as_ref()
                        .map(|snapshot| snapshot.finalized_at_unix),
                    finalized_state: selected_snapshot
                        .as_ref()
                        .map(|snapshot| snapshot.state.clone()),
                    ..GenerationHistoryRecord::default()
                });
            load_backup_restore_lineage(&self.root, &project.project_id, environment, &record).map(
                |restore| ProjectEnvironmentRestoreSummary {
                    backup_id: restore.backup_id,
                    source_generation: restore.source_generation,
                    source_deployment_id: restore.source_deployment_id,
                    restored_at_unix: restore.restored_at_unix,
                },
            )
        });
        let rollback_eligibility = Some(build_rollback_summary(
            previous_generation,
            selected_generation,
        ));
        let runtime_policy = selected_runtime.as_ref().map(build_runtime_policy_summary);
        let readiness_summary =
            ConvergenceCheckpointStore::new(env.clone())
                .load()?
                .map(|checkpoint| ProjectEnvironmentReadinessSummary {
                    health_state: runtime_health_state(&checkpoint.health_state),
                    active_generation: checkpoint.active_generation,
                    last_successful_convergence_unix: checkpoint.last_successful_convergence_unix,
                    reasons: checkpoint.readyz_reasons,
                });

        Ok(ProjectEnvironmentDetail {
            environment: environment.to_string(),
            current_generation,
            previous_generation,
            active_services: selected_runtime
                .as_ref()
                .map(|runtime| build_service_summaries(runtime, &domain))
                .unwrap_or_default(),
            route,
            last_deployment_status,
            last_deployment_timestamp,
            rollback_eligibility,
            restore_lineage,
            runtime_policy,
            readiness_summary,
        })
    }
}

fn build_service_summaries(
    runtime: &PersistedRuntimeInfo,
    domain: &str,
) -> Vec<ProjectEnvironmentServiceSummary> {
    runtime
        .services
        .iter()
        .map(|(service_id, service)| ProjectEnvironmentServiceSummary {
            service_id: service_id.clone(),
            role: if service.externally_exposed {
                "exposed".into()
            } else {
                "internal".into()
            },
            running: service.running,
            route: if service.externally_exposed {
                service_route(service.activation.as_ref(), domain)
            } else {
                None
            },
        })
        .collect()
}

fn build_runtime_policy_summary(
    runtime: &PersistedRuntimeInfo,
) -> ProjectEnvironmentRuntimePolicySummary {
    ProjectEnvironmentRuntimePolicySummary {
        restart_policy: runtime.runtime_policy.restart_policy.clone(),
        max_retries: runtime.runtime_policy.max_retries,
        cpu_limit: runtime.runtime_policy.cpu_limit.clone(),
        memory_limit_mb: runtime.runtime_policy.memory_limit_mb,
        services: runtime
            .services
            .iter()
            .map(
                |(service_id, service)| ProjectEnvironmentServiceRuntimePolicySummary {
                    service_id: service_id.clone(),
                    restart_policy: service.runtime_policy.restart_policy.clone(),
                    max_retries: service.runtime_policy.max_retries,
                    cpu_limit: service.runtime_policy.cpu_limit.clone(),
                    memory_limit_mb: service.runtime_policy.memory_limit_mb,
                },
            )
            .collect(),
    }
}

fn build_rollback_summary(
    previous_generation: Option<u64>,
    current_generation: Option<u64>,
) -> ProjectEnvironmentRollbackSummary {
    match previous_generation {
        Some(target_generation) => ProjectEnvironmentRollbackSummary {
            eligible: true,
            target_generation: Some(target_generation),
            reason: format!(
                "previous generation {target_generation} is available as a rollback target"
            ),
        },
        None if current_generation.is_some() => ProjectEnvironmentRollbackSummary {
            eligible: false,
            target_generation: None,
            reason: "no previous generation is retained for rollback".into(),
        },
        None => ProjectEnvironmentRollbackSummary {
            eligible: false,
            target_generation: None,
            reason: "no active generation is available yet".into(),
        },
    }
}

fn route_for_runtime(runtime: Option<&PersistedRuntimeInfo>, domain: &str) -> Option<String> {
    runtime.and_then(|runtime| service_route(runtime.activation.as_ref(), domain))
}

fn service_route(activation: Option<&PersistedActivationMode>, domain: &str) -> Option<String> {
    match activation {
        Some(PersistedActivationMode::Http { .. }) => Some(domain.to_string()),
        Some(PersistedActivationMode::Direct) | None => None,
    }
}

fn read_authoritative_generation(
    env: &EnvironmentPaths,
) -> Result<Option<u64>, ProjectRegistryError> {
    let promoted = read_optional_pointer(env.promoted_pointer())?;
    match promoted {
        Some(generation) => Ok(Some(generation)),
        None => read_optional_pointer(env.current_pointer()),
    }
}

fn read_optional_pointer(path: PathBuf) -> Result<Option<u64>, ProjectRegistryError> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&path)?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed
        .parse::<u64>()
        .map(Some)
        .map_err(|_| ProjectRegistryError::Storage(StorageError::InvalidPointer(path.clone())))
}

fn latest_generation(env: &EnvironmentPaths) -> Result<Option<u64>, ProjectRegistryError> {
    let generations_dir = env.generations_dir();
    if !generations_dir.exists() {
        return Ok(None);
    }

    let mut latest = None;
    for entry in fs::read_dir(generations_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(generation) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        if latest.is_none_or(|current| generation > current) {
            latest = Some(generation);
        }
    }
    Ok(latest)
}

fn runtime_health_state(state: &crate::storage::RuntimeHealthState) -> String {
    match state {
        crate::storage::RuntimeHealthState::Healthy => "healthy".into(),
        crate::storage::RuntimeHealthState::Degraded => "degraded".into(),
        crate::storage::RuntimeHealthState::Unavailable => "unavailable".into(),
    }
}

fn sanitize_repo_url(input: &str) -> String {
    if let Ok(mut parsed) = reqwest::Url::parse(input) {
        if !parsed.username().is_empty() || parsed.password().is_some() {
            let _ = parsed.set_username("***");
            let _ = parsed.set_password(None);
            return parsed.to_string();
        }
    }
    input.to_string()
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
        ProjectRegistryError::BaseDomainAlreadyInUse(base_domain) => (
            axum::http::StatusCode::BAD_REQUEST,
            ErrorResponse {
                code: "base_domain_conflict".into(),
                message: format!("base_domain is already used by another project: {base_domain}"),
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
    store: &ProjectRegistryStore,
    project_id: &str,
    requested_base_domain: Option<String>,
    existing: Option<&ProjectRecord>,
    apps_domain: Option<&str>,
) -> Result<(String, String), ProjectRegistryError> {
    if let Some(base_domain) = requested_base_domain {
        let normalized = normalize_hostname(&base_domain)?;
        ensure_domain_available(store, project_id, &normalized)?;
        return Ok(("explicit".into(), normalized));
    }

    if let Some(existing) = existing {
        return Ok((existing.domain_mode.clone(), existing.base_domain.clone()));
    }

    let apps_domain = apps_domain.ok_or(ProjectRegistryError::MissingAppsDomain)?;
    let apps_domain = normalize_hostname(apps_domain)?;
    let preferred = normalize_hostname(&format!("{project_id}.{apps_domain}"))?;
    if domain_available(store, project_id, &preferred)? {
        return Ok(("generated".into(), preferred));
    }

    for attempt in 0..ProjectRegistryStore::GENERATED_DOMAIN_MAX_ATTEMPTS {
        let shortid = generate_shortid(project_id, attempt);
        let generated = normalize_hostname(&format!("{project_id}-{shortid}.{apps_domain}"))?;
        if domain_available(store, project_id, &generated)? {
            return Ok(("generated".into(), generated));
        }
    }

    Err(ProjectRegistryError::BaseDomainAlreadyInUse(format!(
        "{project_id}.{apps_domain}"
    )))
}

fn domain_available(
    store: &ProjectRegistryStore,
    project_id: &str,
    base_domain: &str,
) -> Result<bool, ProjectRegistryError> {
    Ok(match store.find_domain_owner(base_domain)? {
        Some(owner) => owner == project_id,
        None => true,
    })
}

fn ensure_domain_available(
    store: &ProjectRegistryStore,
    project_id: &str,
    base_domain: &str,
) -> Result<(), ProjectRegistryError> {
    if domain_available(store, project_id, base_domain)? {
        return Ok(());
    }

    Err(ProjectRegistryError::BaseDomainAlreadyInUse(
        base_domain.to_string(),
    ))
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

fn resolve_project_id(
    requested_project_id: Option<&str>,
    repo_url: &str,
) -> Result<String, ProjectRegistryError> {
    match requested_project_id {
        Some(project_id) => normalize_project_id(project_id),
        None => infer_project_id_from_repo_url(repo_url),
    }
}

fn infer_project_id_from_repo_url(repo_url: &str) -> Result<String, ProjectRegistryError> {
    let trimmed = repo_url.trim().trim_end_matches('/');
    let Some(raw_basename) = trimmed
        .rsplit(['/', ':'])
        .next()
        .map(|segment| segment.strip_suffix(".git").unwrap_or(segment))
    else {
        return Err(ProjectRegistryError::InvalidRepoUrl(
            "repo_url must include a repository name".into(),
        ));
    };

    let inferred = normalize_inferred_project_id(raw_basename);
    if inferred.is_empty() {
        return Err(ProjectRegistryError::InvalidRepoUrl(
            "repo_url must include a usable repository name".into(),
        ));
    }

    normalize_project_id(&inferred).map_err(|_| {
        ProjectRegistryError::InvalidRepoUrl(
            "repo_url produced an invalid inferred project_id".into(),
        )
    })
}

fn normalize_inferred_project_id(input: &str) -> String {
    let mut normalized = String::with_capacity(input.len());
    let mut previous_was_hyphen = false;
    for ch in input.chars().flat_map(char::to_lowercase) {
        let next = if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            Some(ch)
        } else {
            Some('-')
        };
        if let Some(ch) = next {
            if ch == '-' {
                if previous_was_hyphen {
                    continue;
                }
                previous_was_hyphen = true;
            } else {
                previous_was_hyphen = false;
            }
            normalized.push(ch);
        }
    }
    normalized.trim_matches('-').to_string()
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

fn generate_shortid(project_id: &str, attempt: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(project_id.as_bytes());
    hasher.update(attempt.to_string().as_bytes());
    #[cfg(test)]
    if let Some(shortid) = take_test_shortid() {
        return shortid;
    }
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
fn test_shortids() -> &'static std::sync::Mutex<Vec<String>> {
    use std::sync::{Mutex, OnceLock};

    static SHORTIDS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    SHORTIDS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn take_test_shortid() -> Option<String> {
    test_shortids().lock().unwrap().pop()
}

#[cfg(test)]
fn set_test_shortids(shortids: &[&str]) {
    let mut values = test_shortids().lock().unwrap();
    values.clear();
    values.extend(shortids.iter().rev().map(|value| (*value).to_string()));
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
            project_id: Some(project_id.into()),
            repo_url: "https://github.com/example/api.git".into(),
            default_branch: "main".into(),
            base_domain: Some(format!("{project_id}.example.com")),
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
    fn project_add_generates_clean_domain_when_available() {
        let root = test_root("generated-clean-domain");
        let store = ProjectRegistryStore::new(&root);
        let mut request = request("api");
        request.base_domain = None;

        let created = store.upsert(request, Some("forge.example.com")).unwrap();
        assert_eq!(created.domain_mode, "generated");
        assert_eq!(created.base_domain, "api.forge.example.com");
    }

    #[test]
    fn project_add_generates_suffixed_domain_on_collision() {
        let root = test_root("generated-suffixed-domain");
        let store = ProjectRegistryStore::new(&root);
        let mut first = request("web");
        first.base_domain = Some("api.forge.example.com".into());
        store.upsert(first, Some("forge.example.com")).unwrap();

        let mut second = request("api");
        second.base_domain = None;
        set_test_shortids(&["abcd1234"]);

        let created = store.upsert(second, Some("forge.example.com")).unwrap();
        assert_eq!(created.domain_mode, "generated");
        assert_eq!(created.base_domain, "api-abcd1234.forge.example.com");
    }

    #[test]
    fn project_update_preserves_generated_domain() {
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
    fn project_add_rejects_explicit_domain_collision() {
        let root = test_root("explicit-domain-collision");
        let store = ProjectRegistryStore::new(&root);

        store
            .upsert(request("api"), Some("forge.example.com"))
            .unwrap();
        let mut request = request("web");
        request.base_domain = Some("api.example.com".into());

        let err = store
            .upsert(request, Some("forge.example.com"))
            .unwrap_err();
        assert!(matches!(
            err,
            ProjectRegistryError::BaseDomainAlreadyInUse(base_domain)
            if base_domain == "api.example.com"
        ));
    }

    #[test]
    fn project_add_preserves_explicit_domain_behavior() {
        let root = test_root("explicit-domain");
        let store = ProjectRegistryStore::new(&root);

        let created = store
            .upsert(request("api"), Some("forge.example.com"))
            .unwrap();
        assert_eq!(created.domain_mode, "explicit");
        assert_eq!(created.base_domain, "api.example.com");
    }

    #[test]
    fn project_add_fails_when_generated_suffix_attempts_are_exhausted() {
        let root = test_root("generated-domain-exhausted");
        let store = ProjectRegistryStore::new(&root);

        let mut existing = request("existing");
        existing.base_domain = Some("api.forge.example.com".into());
        store.upsert(existing, Some("forge.example.com")).unwrap();

        for suffix in ["aaaa0001", "aaaa0002", "aaaa0003", "aaaa0004"] {
            let mut taken = request(&format!("taken-{suffix}"));
            taken.base_domain = Some(format!("api-{suffix}.forge.example.com"));
            store.upsert(taken, Some("forge.example.com")).unwrap();
        }

        let mut request = request("api");
        request.base_domain = None;
        set_test_shortids(&["aaaa0001", "aaaa0002", "aaaa0003", "aaaa0004"]);

        let err = store
            .upsert(request, Some("forge.example.com"))
            .unwrap_err();
        assert!(matches!(
            err,
            ProjectRegistryError::BaseDomainAlreadyInUse(base_domain)
            if base_domain == "api.forge.example.com"
        ));
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

    #[test]
    fn project_add_infers_project_id_from_repo_url() {
        let root = test_root("infer-project-id");
        let store = ProjectRegistryStore::new(&root);
        let request = ProjectUpsertRequest {
            project_id: None,
            repo_url: "https://github.com/anggaprytn/forge-fullstack-api-test.git".into(),
            default_branch: "main".into(),
            base_domain: Some("forge-fullstack-api-test.example.com".into()),
        };

        let created = store.upsert(request, None).unwrap();
        assert_eq!(created.project_id, "forge-fullstack-api-test");
    }

    #[test]
    fn project_add_prefers_explicit_project_id() {
        let root = test_root("explicit-project-id");
        let store = ProjectRegistryStore::new(&root);
        let request = ProjectUpsertRequest {
            project_id: Some("custom-api".into()),
            repo_url: "https://github.com/anggaprytn/forge-fullstack-api-test.git".into(),
            default_branch: "main".into(),
            base_domain: Some("custom-api.example.com".into()),
        };

        let created = store.upsert(request, None).unwrap();
        assert_eq!(created.project_id, "custom-api");
    }

    #[test]
    fn inferred_project_id_is_normalized() {
        let root = test_root("normalized-inferred-project-id");
        let store = ProjectRegistryStore::new(&root);
        let request = ProjectUpsertRequest {
            project_id: None,
            repo_url: "https://github.com/example/Forge__Fullstack...API---Test.git".into(),
            default_branch: "main".into(),
            base_domain: Some("forge-fullstack-api-test.example.com".into()),
        };

        let created = store.upsert(request, None).unwrap();
        assert_eq!(created.project_id, "forge-fullstack-api-test");
    }

    #[test]
    fn generated_domain_prefers_clean_project_name() {
        let root = test_root("generated-domain-clean-project-name");
        let store = ProjectRegistryStore::new(&root);
        let request = ProjectUpsertRequest {
            project_id: None,
            repo_url: "https://github.com/anggaprytn/forge-fullstack-api-test.git".into(),
            default_branch: "main".into(),
            base_domain: None,
        };

        let created = store.upsert(request, Some("forge.example.com")).unwrap();
        assert_eq!(
            created.base_domain,
            "forge-fullstack-api-test.forge.example.com"
        );
    }

    #[test]
    fn generated_domain_adds_suffix_on_collision() {
        let root = test_root("generated-domain-collision");
        let store = ProjectRegistryStore::new(&root);
        store
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some("existing".into()),
                    repo_url: "https://github.com/example/first.git".into(),
                    default_branch: "main".into(),
                    base_domain: Some("forge-fullstack-api-test.forge.example.com".into()),
                },
                Some("forge.example.com"),
            )
            .unwrap();
        set_test_shortids(&["abcd1234"]);

        let created = store
            .upsert(
                ProjectUpsertRequest {
                    project_id: None,
                    repo_url: "https://github.com/anggaprytn/forge-fullstack-api-test.git".into(),
                    default_branch: "main".into(),
                    base_domain: None,
                },
                Some("forge.example.com"),
            )
            .unwrap();
        assert_eq!(
            created.base_domain,
            "forge-fullstack-api-test-abcd1234.forge.example.com"
        );
    }

    #[test]
    fn generated_domain_is_stable_after_updates() {
        let root = test_root("generated-domain-stable-updates");
        let store = ProjectRegistryStore::new(&root);
        let created = store
            .upsert(
                ProjectUpsertRequest {
                    project_id: None,
                    repo_url: "https://github.com/anggaprytn/forge-fullstack-api-test.git".into(),
                    default_branch: "main".into(),
                    base_domain: None,
                },
                Some("forge.example.com"),
            )
            .unwrap();

        let updated = store
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some(created.project_id.clone()),
                    repo_url: "https://github.com/anggaprytn/forge-fullstack-api-test-renamed.git"
                        .into(),
                    default_branch: "develop".into(),
                    base_domain: None,
                },
                Some("forge.example.com"),
            )
            .unwrap();

        assert_eq!(updated.project_id, created.project_id);
        assert_eq!(updated.base_domain, created.base_domain);
    }

    #[test]
    fn project_add_rejects_repo_urls_without_usable_inferred_project_name() {
        let root = test_root("invalid-inferred-project-id");
        let store = ProjectRegistryStore::new(&root);
        let request = ProjectUpsertRequest {
            project_id: None,
            repo_url: "https://github.com/example/---.git".into(),
            default_branch: "main".into(),
            base_domain: Some("invalid.example.com".into()),
        };

        let err = store.upsert(request, None).unwrap_err();
        assert!(matches!(err, ProjectRegistryError::InvalidRepoUrl(_)));
    }
}
