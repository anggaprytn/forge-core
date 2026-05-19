use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::Path;

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectManifest {
    pub project_id: String,
    pub environment_variables: BTreeMap<String, SecretReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretReference {
    pub scope: String,
    pub key: String,
    pub sensitive: bool,
}

#[derive(Debug)]
pub enum ManifestError {
    Io(std::io::Error),
    Invalid(String),
}

impl Display for ManifestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Invalid(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ManifestError {}

impl From<std::io::Error> for ManifestError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub fn load_optional_manifest(root: &Path) -> Result<Option<ProjectManifest>, ManifestError> {
    let path = root.join("forge.project.json");
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(path)?;
    let json: Value =
        serde_json::from_str(&raw).map_err(|err| ManifestError::Invalid(err.to_string()))?;
    parse_manifest(json).map(Some)
}

fn parse_manifest(json: Value) -> Result<ProjectManifest, ManifestError> {
    let object = json
        .as_object()
        .ok_or_else(|| ManifestError::Invalid("manifest must be a JSON object".into()))?;
    let project_id = object
        .get("project_id")
        .and_then(Value::as_str)
        .ok_or_else(|| ManifestError::Invalid("manifest project_id is required".into()))?
        .to_string();
    let secret_root = object
        .get("secrets")
        .and_then(Value::as_object)
        .and_then(|value| value.get("environment_variables"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let mut environment_variables = BTreeMap::new();
    for (env_name, value) in secret_root {
        let reference = value.as_object().ok_or_else(|| {
            ManifestError::Invalid(format!("secret reference for {env_name} must be an object"))
        })?;
        for key in reference.keys() {
            if !matches!(key.as_str(), "scope" | "key" | "sensitive") {
                return Err(ManifestError::Invalid(format!(
                    "secret values must not be loaded from manifest for {env_name}"
                )));
            }
        }
        let scope = reference
            .get("scope")
            .and_then(Value::as_str)
            .ok_or_else(|| ManifestError::Invalid(format!("secret scope missing for {env_name}")))?
            .to_string();
        let key = reference
            .get("key")
            .and_then(Value::as_str)
            .ok_or_else(|| ManifestError::Invalid(format!("secret key missing for {env_name}")))?
            .to_string();
        let sensitive = reference
            .get("sensitive")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        environment_variables.insert(
            env_name,
            SecretReference {
                scope,
                key,
                sensitive,
            },
        );
    }

    Ok(ProjectManifest {
        project_id,
        environment_variables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn secret_values_are_never_loaded_from_manifest() {
        let root = test_root("manifest-secret-inline");
        fs::write(
            root.join("forge.project.json"),
            r#"{
  "project_id": "api",
  "secrets": {
    "environment_variables": {
      "DATABASE_URL": {
        "scope": "environment",
        "key": "DATABASE_URL",
        "value": "postgres://secret-inline"
      }
    }
  }
}"#,
        )
        .unwrap();

        let err = load_optional_manifest(&root).unwrap_err();
        assert!(
            err.to_string()
                .contains("secret values must not be loaded from manifest")
        );
    }

    fn test_root(name: &str) -> std::path::PathBuf {
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
