use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::deployments::{ActivationMode, ExecutionConfig, ValidationPolicy};

#[derive(Debug)]
pub enum ForgeYamlError {
    Io(std::io::Error),
    Invalid(String),
}

impl Display for ForgeYamlError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Invalid(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ForgeYamlError {}

impl From<std::io::Error> for ForgeYamlError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeYamlConfig {
    execution: ExecutionConfig,
    validation: ValidationPolicy,
    validation_timeout_ms: Option<u64>,
}

impl ForgeYamlConfig {
    pub fn execution(&self) -> &ExecutionConfig {
        &self.execution
    }

    pub fn validation(&self) -> &ValidationPolicy {
        &self.validation
    }

    pub fn validation_timeout_ms(&self) -> Option<u64> {
        self.validation_timeout_ms
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawForgeYaml {
    version: u64,
    name: String,
    #[serde(rename = "type")]
    app_type: String,
    build: RawBuildConfig,
    runtime: RawRuntimeConfig,
    invariants: Vec<RawInvariant>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBuildConfig {
    dockerfile: PathBuf,
    context: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRuntimeConfig {
    port: u16,
    healthcheck: RawHealthcheckConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHealthcheckConfig {
    path: String,
    expected_status: u16,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInvariant {
    name: String,
    path: String,
    expect_status: u16,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

pub fn load_optional_forge_yaml(
    root: &Path,
    expected_project_id: &str,
) -> Result<Option<ForgeYamlConfig>, ForgeYamlError> {
    let path = root.join("forge.yml");
    if !path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(path)?;
    let parsed: RawForgeYaml = serde_yaml::from_str(&raw)
        .map_err(|err| ForgeYamlError::Invalid(format!("invalid forge.yml: {err}")))?;
    Ok(Some(parsed.validate(root, expected_project_id)?))
}

impl RawForgeYaml {
    fn validate(
        self,
        root: &Path,
        expected_project_id: &str,
    ) -> Result<ForgeYamlConfig, ForgeYamlError> {
        if self.version != 1 {
            return Err(ForgeYamlError::Invalid(
                "forge.yml version must equal 1".into(),
            ));
        }
        if self.name != expected_project_id {
            return Err(ForgeYamlError::Invalid(format!(
                "forge.yml name `{}` does not match deployment project `{expected_project_id}`",
                self.name
            )));
        }
        if self.app_type != "web" {
            return Err(ForgeYamlError::Invalid(format!(
                "unsupported forge.yml app type `{}`; only `web` is supported",
                self.app_type
            )));
        }
        if self.build.context.as_os_str().is_empty() {
            return Err(ForgeYamlError::Invalid(
                "forge.yml build.context is required".into(),
            ));
        }
        if self.build.context.is_absolute() {
            return Err(ForgeYamlError::Invalid(
                "forge.yml build.context must be relative to the project root".into(),
            ));
        }
        if self.build.dockerfile.as_os_str().is_empty() {
            return Err(ForgeYamlError::Invalid(
                "forge.yml build.dockerfile is required".into(),
            ));
        }
        if self.build.dockerfile.is_absolute() {
            return Err(ForgeYamlError::Invalid(
                "forge.yml build.dockerfile must be relative to the project root".into(),
            ));
        }
        if self.runtime.port == 0 {
            return Err(ForgeYamlError::Invalid(
                "forge.yml runtime.port must be a valid TCP port".into(),
            ));
        }
        if !self.runtime.healthcheck.path.starts_with('/') {
            return Err(ForgeYamlError::Invalid(
                "forge.yml runtime.healthcheck.path must start with `/`".into(),
            ));
        }
        if self.runtime.healthcheck.expected_status != 200 {
            return Err(ForgeYamlError::Invalid(
                "forge.yml runtime.healthcheck.expected_status must equal 200".into(),
            ));
        }
        if self.invariants.len() != 1 {
            return Err(ForgeYamlError::Invalid(
                "forge.yml invariants must contain exactly one entry in v1".into(),
            ));
        }

        let invariant = &self.invariants[0];
        if invariant.name.trim().is_empty() {
            return Err(ForgeYamlError::Invalid(
                "forge.yml invariants[0].name is required".into(),
            ));
        }
        if invariant.path != self.runtime.healthcheck.path
            || invariant.expect_status != self.runtime.healthcheck.expected_status
        {
            return Err(ForgeYamlError::Invalid(
                "forge.yml invariants[0] must match runtime.healthcheck".into(),
            ));
        }

        Ok(ForgeYamlConfig {
            execution: ExecutionConfig {
                context_path: root.join(self.build.context),
                dockerfile_path: root.join(self.build.dockerfile),
                network_name: None,
            },
            validation: ValidationPolicy {
                tcp_required: true,
                http_health_path: Some(self.runtime.healthcheck.path),
                activation: ActivationMode::Http {
                    internal_port: self.runtime.port,
                },
            },
            validation_timeout_ms: invariant.timeout_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsupported_fields() {
        let root = test_root("forge-yaml-unknown-field");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "extra: true\n",
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
                "    expect_status: 200\n",
            ),
        )
        .unwrap();

        let err = load_optional_forge_yaml(&root, "api").unwrap_err();
        assert!(err.to_string().contains("unknown field `extra`"));
    }

    #[test]
    fn rejects_unsupported_type() {
        let root = test_root("forge-yaml-type");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: worker\n",
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
                "    expect_status: 200\n",
            ),
        )
        .unwrap();

        let err = load_optional_forge_yaml(&root, "api").unwrap_err();
        assert!(err.to_string().contains("only `web` is supported"));
    }

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
}
