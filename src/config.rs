use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    pub storage_root: PathBuf,
    pub api_bind: String,
    pub sqlite_path: Option<PathBuf>,
}

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    MissingKey(&'static str),
    InvalidLine(String),
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::MissingKey(key) => write!(f, "missing required config key {key}"),
            Self::InvalidLine(line) => write!(f, "invalid config line: {line}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl DaemonConfig {
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path)?;
        Self::load_from_str(&contents)
    }

    pub fn load_from_str(contents: &str) -> Result<Self, ConfigError> {
        let mut values = HashMap::new();
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                return Err(ConfigError::InvalidLine(line.to_string()));
            };
            values.insert(key.trim().to_string(), value.trim().to_string());
        }

        let storage_root = values
            .get("storage_root")
            .ok_or(ConfigError::MissingKey("storage_root"))?;
        let api_bind = values
            .get("api_bind")
            .ok_or(ConfigError::MissingKey("api_bind"))?;
        let sqlite_path = values.get("sqlite_path").map(PathBuf::from);

        Ok(Self {
            storage_root: PathBuf::from(storage_root),
            api_bind: api_bind.clone(),
            sqlite_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_minimal_config() {
        let config = DaemonConfig::load_from_str(
            "storage_root=/tmp/forge\napi_bind=127.0.0.1:8080\n",
        )
        .unwrap();

        assert_eq!(config.storage_root, PathBuf::from("/tmp/forge"));
        assert_eq!(config.api_bind, "127.0.0.1:8080");
        assert_eq!(config.sqlite_path, None);
    }
}
