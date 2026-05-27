use std::fs;
use std::path::Path;

use serde_yaml::Value as YamlValue;

use crate::api::{
    ErrorResponse, WebDeployComposeSummary, WebDeployEnvPreviewSummary,
    WebDeployHealthcheckSummary, WebDeployManifestSummary, WebDeployPreviewRequest,
    WebDeployPreviewResponse, WebDeployRouteSummary,
};
use crate::compose::{detect_compose, preview_compose};
use crate::forge_yaml::load_optional_forge_yaml;
use crate::manifest::load_optional_manifest;
use crate::projects::ProjectRegistryStore;
use crate::runtime_env::is_sensitive_key;
use crate::secrets::SecretStore;
use crate::source::SourceResolver;
use crate::status::load_project_env_inventory_report;
use crate::storage::EnvStore;

pub fn validate_web_deploy_preview_request(
    request: &WebDeployPreviewRequest,
) -> Result<(), ErrorResponse> {
    if !matches!(
        request.environment.as_str(),
        "development" | "staging" | "production"
    ) {
        return Err(ErrorResponse {
            code: "invalid_environment".into(),
            message: "environment must be one of development, staging, production".into(),
        });
    }
    if request.git_ref.trim().is_empty() {
        return Err(ErrorResponse {
            code: "invalid_ref".into(),
            message: "ref must not be empty".into(),
        });
    }
    Ok(())
}

pub fn build_web_deploy_preview(
    storage_root: &Path,
    secret_store: &SecretStore,
    project_id: &str,
    request: &WebDeployPreviewRequest,
) -> Result<WebDeployPreviewResponse, ErrorResponse> {
    validate_web_deploy_preview_request(request)?;

    let project = ProjectRegistryStore::new(storage_root)
        .get(project_id)
        .map_err(|err| ErrorResponse {
            code: "project_lookup_failed".into(),
            message: err.to_string(),
        })?
        .ok_or_else(|| ErrorResponse {
            code: "project_not_found".into(),
            message: format!("project is not registered: {project_id}"),
        })?;

    if project.repo_url.trim().is_empty() {
        return Err(ErrorResponse {
            code: "repo_url_missing".into(),
            message: format!("project `{project_id}` does not have a registered repository"),
        });
    }

    let resolved = SourceResolver::new(storage_root)
        .resolve(project_id, None, Some(&request.git_ref))
        .map_err(|err| ErrorResponse {
            code: "source_resolution_failed".into(),
            message: err.to_string(),
        })?;
    let source_path = resolved.source_path.as_ref().ok_or_else(|| ErrorResponse {
        code: "source_resolution_failed".into(),
        message: "resolved source checkout is unavailable".into(),
    })?;

    let mut warnings = Vec::new();
    let mut errors = detect_preview_errors(source_path, project_id);
    let compose_detection = detect_compose(source_path).ok();

    let forge_yaml = match load_optional_forge_yaml(source_path, project_id) {
        Ok(Some(config)) => Some(config),
        Ok(None) => {
            errors.push("forge.yml is missing at the resolved ref".into());
            None
        }
        Err(err) => {
            errors.push(err.to_string());
            None
        }
    };

    let compose = compose_detection
        .as_ref()
        .and_then(|detection| detection.selected_file.as_ref().map(|path| (detection, path)))
        .map(|(detection, path)| {
            let compose_preview = preview_compose(path).ok();
            let compose_file = path
                .strip_prefix(source_path)
                .unwrap_or(path)
                .display()
                .to_string();
            WebDeployComposeSummary {
            detected: true,
            compose_file: Some(compose_file.clone()),
            services: detection.services.clone(),
            public_candidates: detection.public_candidates.clone(),
            internal_services: detection.internal_services.clone(),
            required_env_keys: compose_preview
                .as_ref()
                .map(|preview| preview.required_env_keys.clone())
                .unwrap_or_default(),
            unsupported_fields: compose_preview
                .as_ref()
                .map(|preview| preview.unsupported_fields.clone())
                .unwrap_or_default(),
            contract_copy: if forge_yaml.is_some() {
                "forge.yml is canonical. Compose also detected, but Forge will not use Compose automatically.".into()
            } else {
                "Compose file detected. Preview conversion and generate forge.yml. Deploy is blocked until forge.yml exists or a generated contract is confirmed.".into()
            },
            preview_command: format!("forge compose preview {compose_file}"),
            convert_command: format!("forge compose convert {compose_file} --out forge.yml"),
        }
        });
    if let Some(compose) = compose.as_ref() {
        if forge_yaml.is_some() {
            warnings.push(format!(
                "forge.yml will be used as the Forge contract. Compose file also detected at {}.",
                compose.compose_file.as_deref().unwrap_or("compose file")
            ));
        } else {
            warnings.push(format!(
                "Compose file detected at {}. Preview conversion and generate forge.yml before deploying.",
                compose.compose_file.as_deref().unwrap_or("compose file")
            ));
        }
    }

    let manifest = match load_optional_manifest(source_path) {
        Ok(value) => value,
        Err(err) => {
            errors.push(err.to_string());
            None
        }
    };

    let mut manifest_summary = WebDeployManifestSummary {
        name: project_id.to_string(),
        schema_version: 1,
        services: Vec::new(),
        exposed_services: Vec::new(),
        healthchecks: Vec::new(),
    };

    if let Some(forge_yaml) = forge_yaml.as_ref() {
        manifest_summary.services = forge_yaml.startup_order().to_vec();
        manifest_summary.exposed_services = forge_yaml
            .services()
            .values()
            .filter(|service| service.externally_exposed)
            .map(|service| service.service_id.clone())
            .collect();
        manifest_summary.healthchecks = forge_yaml
            .services()
            .values()
            .filter_map(|service| {
                service.validation.http_health_path.as_ref().map(|path| {
                    WebDeployHealthcheckSummary {
                        service_id: service.service_id.clone(),
                        path: path.clone(),
                        expected_status: 200,
                    }
                })
            })
            .collect();

        if manifest_summary.exposed_services.is_empty() {
            errors.push(
                "no exposed HTTP service found; web deploy requires a routed application service"
                    .into(),
            );
        }

        for service in forge_yaml.services().values() {
            if service.externally_exposed && service.validation.http_health_path.is_none() {
                errors.push(format!(
                    "service `{}` is exposed but missing runtime.healthcheck.path",
                    service.service_id
                ));
                errors.push(format!(
                    "fix: add services.{}.runtime.healthcheck.path",
                    service.service_id
                ));
            }
        }
    }

    let missing_required_secrets = detect_missing_required_secrets(
        secret_store,
        project_id,
        &request.environment,
        &forge_yaml,
        manifest.as_ref(),
    );
    if !missing_required_secrets.is_empty() {
        errors.push(format!(
            "missing required secrets detected: {}",
            missing_required_secrets.join(", ")
        ));
    }
    let compose_required_env_keys = compose
        .as_ref()
        .map(|compose| compose.required_env_keys.clone())
        .unwrap_or_default();
    let configured_required_keys = detect_configured_required_keys(
        storage_root,
        secret_store,
        project_id,
        &request.environment,
        &compose_required_env_keys,
    );
    for key in &compose_required_env_keys {
        if configured_required_keys.contains(key) {
            warnings.push(format!(
                "Required env key {key} is configured for next deployment."
            ));
        } else {
            warnings.push(format!(
                "Import {key} into Forge Env Manager before deploying."
            ));
        }
    }

    let pending_desired_env = load_project_env_inventory_report(
        storage_root,
        secret_store,
        project_id,
        Some(&request.environment),
    )
    .map(|inventory| {
        inventory.variables.iter().any(|variable| {
            variable
                .environments
                .get(&request.environment)
                .is_some_and(|cell| cell.pending_next_deploy)
        })
    })
    .unwrap_or(false);
    if pending_desired_env {
        warnings.push(format!(
            "pending desired env changes exist for `{}` and will apply on the next deployment",
            request.environment
        ));
    }

    Ok(WebDeployPreviewResponse {
        valid: errors.is_empty(),
        project_id: project.project_id.clone(),
        environment: request.environment.clone(),
        repo_url: project.repo_url.clone(),
        git_ref: request.git_ref.clone(),
        commit_sha: resolved.commit_sha,
        manifest: manifest_summary,
        route: WebDeployRouteSummary {
            domain: crate::status::derive_environment_domain(
                &project.base_domain,
                &request.environment,
            ),
        },
        env: WebDeployEnvPreviewSummary {
            pending_desired_env,
            source: "latest configured env store".into(),
            missing_required_secrets,
            configured_required_keys,
        },
        compose,
        warnings,
        errors,
    })
}

fn detect_configured_required_keys(
    storage_root: &Path,
    secret_store: &SecretStore,
    project_id: &str,
    environment: &str,
    required_keys: &[String],
) -> Vec<String> {
    let desired = EnvStore::new(storage_root)
        .load_desired_environment(project_id, environment)
        .ok()
        .flatten();
    let mut configured = required_keys
        .iter()
        .filter(|key| {
            secret_store.has_environment_secret(project_id, environment, key)
                || desired.as_ref().is_some_and(|config| {
                    config.entries.iter().any(|entry| entry.key == **key)
                        && !config.deleted_keys.iter().any(|entry| entry.key == **key)
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    configured.sort();
    configured.dedup();
    configured
}

fn detect_preview_errors(source_path: &Path, project_id: &str) -> Vec<String> {
    let path = source_path.join("forge.yml");
    if !path.exists() {
        return Vec::new();
    }

    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) => return vec![err.to_string()],
    };
    let yaml = match serde_yaml::from_str::<YamlValue>(&raw) {
        Ok(yaml) => yaml,
        Err(err) => return vec![format!("invalid forge.yml: {err}")],
    };
    let mut errors = Vec::new();

    let Some(root) = yaml.as_mapping() else {
        errors.push("invalid forge.yml: root document must be a mapping".into());
        return errors;
    };

    if let Some(name) = root
        .get(YamlValue::String("name".into()))
        .and_then(YamlValue::as_str)
        && name != project_id
    {
        errors.push(format!(
            "forge.yml name `{name}` does not match deployment project `{project_id}`"
        ));
        errors.push(format!(
            "fix: update forge.yml name to `{project_id}` and push"
        ));
    }

    if let Some(services) = root
        .get(YamlValue::String("services".into()))
        .and_then(YamlValue::as_mapping)
    {
        for (service_key, service_value) in services {
            let Some(service_id) = service_key.as_str() else {
                continue;
            };
            let Some(service) = service_value.as_mapping() else {
                continue;
            };
            if service.contains_key(YamlValue::String("type".into())) {
                errors.push(format!("services.{service_id}.type is not supported"));
            }
            if service.contains_key(YamlValue::String("image".into())) {
                errors.push(format!("services.{service_id}.image is not supported"));
                errors.push(format!("fix: use services.{service_id}.runtime.image"));
            }
            if let Some(runtime) = service
                .get(YamlValue::String("runtime".into()))
                .and_then(YamlValue::as_mapping)
                && runtime.contains_key(YamlValue::String("env".into()))
            {
                errors.push(format!(
                    "services.{service_id}.runtime.env is not supported"
                ));
                errors.push("fix: use Forge env manager or secrets set".into());
            }
        }
    }

    errors
}

fn detect_missing_required_secrets(
    secret_store: &SecretStore,
    project_id: &str,
    environment: &str,
    forge_yaml: &Option<crate::forge_yaml::ForgeYamlConfig>,
    manifest: Option<&crate::manifest::ProjectManifest>,
) -> Vec<String> {
    let mut missing = Vec::new();

    if let Some(manifest) = manifest {
        for reference in manifest.environment_variables.values() {
            if reference.scope == "environment"
                && !secret_store.has_environment_secret(project_id, environment, &reference.key)
            {
                missing.push(reference.key.clone());
            }
        }
    }

    if let Some(forge_yaml) = forge_yaml {
        for key in forge_yaml.environment().keys() {
            if is_sensitive_key(key)
                && !secret_store.has_environment_secret(project_id, environment, key)
            {
                missing.push(key.clone());
            }
        }
    }

    missing.sort();
    missing.dedup();
    missing
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ProjectUpsertRequest;
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn preview_catches_specific_unsupported_fields() {
        let root = test_root("preview-catches-unsupported-fields");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "services:\n",
                "  app:\n",
                "    type: web\n",
                "    runtime:\n",
                "      port: 3000\n",
                "      env:\n",
                "        MODE: dev\n",
                "      healthcheck:\n",
                "        path: /health\n",
                "        expected_status: 200\n",
                "  redis:\n",
                "    image: redis:7\n",
                "    runtime:\n",
                "      image: redis:7\n",
            ),
        )
        .unwrap();

        let errors = detect_preview_errors(&root, "forge-redis-fullstack-test");
        let rendered = errors.join("\n");
        assert!(rendered.contains(
            "forge.yml name `api` does not match deployment project `forge-redis-fullstack-test`"
        ));
        assert!(rendered.contains("services.app.type is not supported"));
        assert!(rendered.contains("services.app.runtime.env is not supported"));
        assert!(rendered.contains("services.redis.image is not supported"));
        assert!(rendered.contains("services.redis.runtime.image"));
    }

    #[test]
    fn preview_resolves_registered_ref_to_commit_sha() {
        let root = test_root("preview-resolves-ref");
        let (remote, _commit_sha) = create_git_repo(&root);
        fs::write(
            remote.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "build:\n",
                "  dockerfile: Dockerfile\n",
                "  context: .\n",
                "runtime:\n",
                "  port: 3000\n",
                "  healthcheck:\n",
                "    path: /health\n",
                "    expected_status: 200\n",
                "invariants:\n",
                "  - name: health\n",
                "    path: /health\n",
                "    expect_status: 200\n"
            ),
        )
        .unwrap();
        git_test(&remote, &["add", "forge.yml"]);
        git_test(&remote, &["commit", "-m", "add forge manifest"]);
        let commit_sha = git_test(&remote, &["rev-parse", "HEAD"]);
        ProjectRegistryStore::new(&root)
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some("api".into()),
                    repo_url: remote.to_string_lossy().to_string(),
                    default_branch: "main".into(),
                    base_domain: Some("api.example.com".into()),
                },
                None,
            )
            .unwrap();
        let secret_store = SecretStore::new(root.join("secrets")).unwrap();

        let preview = build_web_deploy_preview(
            &root,
            &secret_store,
            "api",
            &WebDeployPreviewRequest {
                environment: "production".into(),
                git_ref: "main".into(),
            },
        )
        .unwrap();

        assert!(preview.valid);
        assert_eq!(preview.commit_sha.as_deref(), Some(commit_sha.as_str()));
        assert_eq!(preview.git_ref, "main");
        assert_eq!(preview.repo_url, remote.to_string_lossy());
        assert_eq!(preview.manifest.exposed_services, vec!["api"]);
    }

    #[test]
    fn preview_reports_compose_detection_when_forge_yml_is_missing() {
        let root = test_root("preview-detects-compose");
        let (remote, _commit_sha) = create_git_repo(&root);
        fs::write(
            remote.join("docker-compose.yml"),
            concat!(
                "services:\n",
                "  app:\n",
                "    build: .\n",
                "    ports:\n",
                "      - \"3000:3000\"\n",
                "    environment:\n",
                "      REDIS_URL: redis://redis:6379\n",
                "  redis:\n",
                "    image: redis:alpine\n",
            ),
        )
        .unwrap();
        git_test(&remote, &["add", "docker-compose.yml"]);
        git_test(&remote, &["commit", "-m", "add compose"]);
        ProjectRegistryStore::new(&root)
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some("api".into()),
                    repo_url: remote.to_string_lossy().to_string(),
                    default_branch: "main".into(),
                    base_domain: Some("api.example.com".into()),
                },
                None,
            )
            .unwrap();
        let secret_store = SecretStore::new(root.join("secrets")).unwrap();

        let preview = build_web_deploy_preview(
            &root,
            &secret_store,
            "api",
            &WebDeployPreviewRequest {
                environment: "production".into(),
                git_ref: "main".into(),
            },
        )
        .unwrap();

        assert!(!preview.valid);
        assert!(
            preview
                .errors
                .iter()
                .any(|error| error.contains("forge.yml is missing"))
        );
        let compose = preview.compose.as_ref().expect("compose summary");
        assert!(compose.detected);
        assert_eq!(compose.compose_file.as_deref(), Some("docker-compose.yml"));
        assert_eq!(compose.services, vec!["app", "redis"]);
        assert_eq!(compose.public_candidates, vec!["app"]);
        assert_eq!(compose.internal_services, vec!["redis"]);
        assert_eq!(compose.required_env_keys, vec!["REDIS_URL"]);
        assert!(compose.contract_copy.contains("Compose file detected"));
        assert!(compose.contract_copy.contains("Deploy is blocked"));
        assert!(compose.preview_command.contains("forge compose preview"));
        assert!(compose.convert_command.contains("forge compose convert"));
        assert!(
            preview
                .warnings
                .iter()
                .any(|warning| warning
                    == "Import REDIS_URL into Forge Env Manager before deploying.")
        );
        let rendered = serde_json::to_string(&preview).unwrap();
        assert!(!rendered.contains("redis://redis:6379"));
    }

    #[test]
    fn preview_reports_configured_required_env_key_for_next_deployment() {
        let root = test_root("preview-compose-required-env-configured");
        let (remote, _commit_sha) = create_git_repo(&root);
        fs::write(
            remote.join("docker-compose.yml"),
            concat!(
                "services:\n",
                "  app:\n",
                "    build: .\n",
                "    ports:\n",
                "      - \"3000:3000\"\n",
                "    environment:\n",
                "      REDIS_URL: redis://redis:6379\n",
                "  redis:\n",
                "    image: redis:7-alpine\n",
            ),
        )
        .unwrap();
        git_test(&remote, &["add", "docker-compose.yml"]);
        git_test(&remote, &["commit", "-m", "add compose"]);
        ProjectRegistryStore::new(&root)
            .upsert(
                ProjectUpsertRequest {
                    project_id: Some("api".into()),
                    repo_url: remote.to_string_lossy().to_string(),
                    default_branch: "main".into(),
                    base_domain: Some("api.example.com".into()),
                },
                None,
            )
            .unwrap();
        unsafe {
            std::env::set_var(
                "FORGE_MASTER_KEY",
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            );
        }
        crate::storage::EnvStore::new(&root)
            .write_desired_environment(&crate::storage::PersistedDesiredEnvConfig {
                snapshot_version: 1,
                project_id: "api".into(),
                environment: "production".into(),
                env_store_revision: 1,
                updated_at_unix: 1,
                updated_by: None,
                entries: vec![crate::storage::PersistedDesiredEnvEntry {
                    key: "REDIS_URL".into(),
                    normalized_key: "redis_url".into(),
                    sealed_value: crate::secrets::seal_value("redis://redis:6379").unwrap(),
                }],
                deleted_keys: Vec::new(),
            })
            .unwrap();
        let secret_store = SecretStore::new(root.join("secrets")).unwrap();

        let preview = build_web_deploy_preview(
            &root,
            &secret_store,
            "api",
            &WebDeployPreviewRequest {
                environment: "production".into(),
                git_ref: "main".into(),
            },
        )
        .unwrap();

        assert_eq!(preview.env.configured_required_keys, vec!["REDIS_URL"]);
        assert!(
            preview.warnings.iter().any(|warning| warning
                == "Required env key REDIS_URL is configured for next deployment.")
        );
        let rendered = serde_json::to_string(&preview).unwrap();
        assert!(!rendered.contains("redis://redis:6379"));
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

    fn git_test(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .unwrap();
        if !output.status.success() {
            panic!(
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }
}
