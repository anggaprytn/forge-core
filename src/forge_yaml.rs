use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::deployments::{ActivationMode, ExecutionConfig, ValidationPolicy};
use crate::storage::PersistedVolumeRetention;

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
    default_service_build: Option<ForgeBuildConfig>,
    validation: ValidationPolicy,
    validation_timeout_ms: Option<u64>,
    environment: BTreeMap<String, String>,
    services: BTreeMap<String, ForgeServiceConfig>,
    startup_order: Vec<String>,
}

impl ForgeYamlConfig {
    pub fn execution(&self) -> &ExecutionConfig {
        &self.execution
    }

    pub fn default_service_build(&self) -> Option<&ForgeBuildConfig> {
        self.default_service_build.as_ref()
    }

    pub fn validation(&self) -> &ValidationPolicy {
        &self.validation
    }

    pub fn validation_timeout_ms(&self) -> Option<u64> {
        self.validation_timeout_ms
    }

    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    pub fn services(&self) -> &BTreeMap<String, ForgeServiceConfig> {
        &self.services
    }

    pub fn startup_order(&self) -> &[String] {
        &self.startup_order
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeBuildConfig {
    pub context_path: PathBuf,
    pub dockerfile_path: PathBuf,
    pub build_args: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeServiceConfig {
    pub service_id: String,
    pub build: Option<ForgeBuildConfig>,
    pub image: Option<String>,
    pub command: Option<String>,
    pub runtime_policy: ForgeRuntimePolicy,
    pub state: Option<ForgeStateConfig>,
    pub depends_on: Vec<String>,
    pub validation: ValidationPolicy,
    pub required_for_promotion: bool,
    pub externally_exposed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ForgeRuntimePolicy {
    pub cpu_limit: Option<String>,
    pub memory_limit_mb: Option<u64>,
    pub restart_policy: String,
    pub max_retries: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeStateConfig {
    pub volume: String,
    pub mount_path: String,
    pub retention: PersistedVolumeRetention,
    pub pre_backup_command: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawForgeYaml {
    #[serde(default)]
    version: Option<u64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(rename = "type")]
    #[serde(default)]
    app_type: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    build: Option<RawBuildConfig>,
    #[serde(default)]
    runtime: Option<RawRuntimeConfig>,
    #[serde(default)]
    invariants: Vec<RawInvariant>,
    #[serde(default)]
    services: BTreeMap<String, RawServiceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBuildConfig {
    dockerfile: PathBuf,
    context: PathBuf,
    #[serde(default)]
    args: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawRuntimeConfig {
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    healthcheck: Option<RawHealthcheckConfig>,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    cpu: Option<RawCpuConfig>,
    #[serde(default)]
    memory: Option<RawMemoryConfig>,
    #[serde(default)]
    restart: Option<RawRestartConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCpuConfig {
    limit: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMemoryConfig {
    limit_mb: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRestartConfig {
    policy: String,
    #[serde(default)]
    max_retries: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServiceConfig {
    #[serde(default)]
    build: Option<RawBuildConfig>,
    runtime: RawRuntimeConfig,
    #[serde(default)]
    state: Option<RawStateConfig>,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    expose: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStateConfig {
    #[serde(default)]
    volume: Option<String>,
    #[serde(default)]
    mount_path: Option<String>,
    #[serde(default)]
    retention: Option<String>,
    #[serde(default)]
    pre_backup_command: Option<String>,
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
        if self.services.is_empty() {
            return self.validate_legacy(root, expected_project_id);
        }
        self.validate_multi_service(root, expected_project_id)
    }

    fn validate_legacy(
        self,
        root: &Path,
        expected_project_id: &str,
    ) -> Result<ForgeYamlConfig, ForgeYamlError> {
        let version = self
            .version
            .ok_or_else(|| ForgeYamlError::Invalid("forge.yml version must equal 1".into()))?;
        if version != 1 {
            return Err(ForgeYamlError::Invalid(
                "forge.yml version must equal 1".into(),
            ));
        }
        let name = self
            .name
            .ok_or_else(|| ForgeYamlError::Invalid("forge.yml name is required".into()))?;
        if name != expected_project_id {
            return Err(ForgeYamlError::Invalid(format!(
                "forge.yml name `{name}` does not match deployment project `{expected_project_id}`"
            )));
        }
        let app_type = self
            .app_type
            .ok_or_else(|| ForgeYamlError::Invalid("forge.yml type is required".into()))?;
        if app_type != "web" {
            return Err(ForgeYamlError::Invalid(format!(
                "unsupported forge.yml app type `{app_type}`; only `web` is supported"
            )));
        }
        let build = self
            .build
            .ok_or_else(|| ForgeYamlError::Invalid("forge.yml build is required".into()))?;
        validate_build(&build)?;
        let runtime = self
            .runtime
            .ok_or_else(|| ForgeYamlError::Invalid("forge.yml runtime is required".into()))?;
        let port = runtime.port.ok_or_else(|| {
            ForgeYamlError::Invalid("forge.yml runtime.port must be a valid TCP port".into())
        })?;
        if port == 0 {
            return Err(ForgeYamlError::Invalid(
                "forge.yml runtime.port must be a valid TCP port".into(),
            ));
        }
        let healthcheck = runtime.healthcheck.clone().ok_or_else(|| {
            ForgeYamlError::Invalid("forge.yml runtime.healthcheck is required".into())
        })?;
        validate_healthcheck(&healthcheck)?;
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
        if invariant.path != healthcheck.path
            || invariant.expect_status != healthcheck.expected_status
        {
            return Err(ForgeYamlError::Invalid(
                "forge.yml invariants[0] must match runtime.healthcheck".into(),
            ));
        }

        let validation = validation_for_runtime(&runtime, Some(healthcheck.path.clone()))?;
        let service_id = expected_project_id.to_string();
        let service = ForgeServiceConfig {
            service_id: service_id.clone(),
            build: None,
            image: None,
            command: runtime.command.clone(),
            runtime_policy: runtime_policy_for_runtime(&runtime)?,
            state: None,
            depends_on: Vec::new(),
            validation: validation.clone(),
            required_for_promotion: true,
            externally_exposed: matches!(validation.activation, ActivationMode::Http { .. }),
        };
        let context_path = root.join(build.context.clone());
        let dockerfile_path = root.join(build.dockerfile.clone());

        Ok(ForgeYamlConfig {
            execution: ExecutionConfig {
                context_path: context_path.clone(),
                dockerfile_path: dockerfile_path.clone(),
                network_name: None,
            },
            default_service_build: Some(ForgeBuildConfig {
                context_path,
                dockerfile_path,
                build_args: build.args,
            }),
            validation,
            validation_timeout_ms: invariant.timeout_ms,
            environment: self.env,
            services: BTreeMap::from([(service_id.clone(), service)]),
            startup_order: vec![service_id],
        })
    }

    fn validate_multi_service(
        self,
        root: &Path,
        expected_project_id: &str,
    ) -> Result<ForgeYamlConfig, ForgeYamlError> {
        if let Some(name) = self.name.as_deref() {
            if name != expected_project_id {
                return Err(ForgeYamlError::Invalid(format!(
                    "forge.yml name `{name}` does not match deployment project `{expected_project_id}`"
                )));
            }
        }
        if let Some(version) = self.version {
            if version != 1 {
                return Err(ForgeYamlError::Invalid(
                    "forge.yml version must equal 1".into(),
                ));
            }
        }
        if let Some(app_type) = self.app_type.as_deref() {
            if app_type != "web" {
                return Err(ForgeYamlError::Invalid(format!(
                    "unsupported forge.yml app type `{app_type}`; only `web` is supported"
                )));
            }
        }

        let build = self.build.clone();
        if let Some(build) = build.as_ref() {
            validate_build(build)?;
        }
        for (service_id, service) in &self.services {
            if let Some(service_build) = service.build.as_ref() {
                validate_build(service_build)?;
            }
            if build.is_none() && service.build.is_none() && service.runtime.image.is_none() {
                return Err(ForgeYamlError::Invalid(format!(
                    "service `{service_id}` requires either services.{service_id}.build, runtime.image, or root forge.yml build"
                )));
            }
            if let Some(state) = service.state.as_ref() {
                validate_service_state(service_id, state)?;
            }
        }
        let mut seen_volume_ids = BTreeMap::new();
        for (service_id, service) in &self.services {
            let Some(state) = service.state.as_ref() else {
                continue;
            };
            if let Some(existing) = seen_volume_ids.insert(state.volume.clone(), service_id.clone())
            {
                return Err(ForgeYamlError::Invalid(format!(
                    "state volume `{}` is declared by both service `{existing}` and service `{service_id}`",
                    state.volume.as_deref().unwrap_or_default()
                )));
            }
        }

        let startup_order = topological_service_order(&self.services)?;
        let mut services = BTreeMap::new();
        for service_id in &startup_order {
            let raw = self.services.get(service_id).expect("topology validated");
            let validation = validation_for_runtime(&raw.runtime, None)?;
            let externally_exposed = raw
                .expose
                .unwrap_or(matches!(validation.activation, ActivationMode::Http { .. }));
            let depends_on = service_depends_on(raw);
            services.insert(
                service_id.clone(),
                ForgeServiceConfig {
                    service_id: service_id.clone(),
                    build: raw.build.clone().map(|build| ForgeBuildConfig {
                        context_path: root.join(build.context),
                        dockerfile_path: root.join(build.dockerfile),
                        build_args: build.args,
                    }),
                    image: raw.runtime.image.clone(),
                    command: raw.runtime.command.clone(),
                    runtime_policy: runtime_policy_for_runtime(&raw.runtime)?,
                    state: raw.state.as_ref().map(|state| ForgeStateConfig {
                        volume: state.volume.clone().expect("state validated"),
                        mount_path: state.mount_path.clone().expect("state validated"),
                        retention: parse_state_retention(service_id, state)
                            .expect("state validated"),
                        pre_backup_command: state.pre_backup_command.clone(),
                    }),
                    depends_on,
                    validation,
                    required_for_promotion: true,
                    externally_exposed,
                },
            );
        }

        let primary_service = services
            .values()
            .find(|service| service.externally_exposed)
            .or_else(|| services.values().next())
            .ok_or_else(|| ForgeYamlError::Invalid("forge.yml services cannot be empty".into()))?;
        let default_service_build = build.map(|build| ForgeBuildConfig {
            context_path: root.join(build.context.clone()),
            dockerfile_path: root.join(build.dockerfile.clone()),
            build_args: build.args.clone(),
        });
        let execution = default_service_build
            .as_ref()
            .map(|build| ExecutionConfig {
                context_path: build.context_path.clone(),
                dockerfile_path: build.dockerfile_path.clone(),
                network_name: None,
            })
            .unwrap_or_default();

        Ok(ForgeYamlConfig {
            execution,
            default_service_build,
            validation: primary_service.validation.clone(),
            validation_timeout_ms: self.invariants.first().and_then(|value| value.timeout_ms),
            environment: self.env,
            services,
            startup_order,
        })
    }
}

fn validate_build(build: &RawBuildConfig) -> Result<(), ForgeYamlError> {
    if build.context.as_os_str().is_empty() {
        return Err(ForgeYamlError::Invalid(
            "forge.yml build.context is required".into(),
        ));
    }
    if build.context.is_absolute() {
        return Err(ForgeYamlError::Invalid(
            "forge.yml build.context must be relative to the project root".into(),
        ));
    }
    if build.dockerfile.as_os_str().is_empty() {
        return Err(ForgeYamlError::Invalid(
            "forge.yml build.dockerfile is required".into(),
        ));
    }
    if build.dockerfile.is_absolute() {
        return Err(ForgeYamlError::Invalid(
            "forge.yml build.dockerfile must be relative to the project root".into(),
        ));
    }
    Ok(())
}

fn validate_healthcheck(healthcheck: &RawHealthcheckConfig) -> Result<(), ForgeYamlError> {
    if !healthcheck.path.starts_with('/') {
        return Err(ForgeYamlError::Invalid(
            "forge.yml runtime.healthcheck.path must start with `/`".into(),
        ));
    }
    if healthcheck.expected_status != 200 {
        return Err(ForgeYamlError::Invalid(
            "forge.yml runtime.healthcheck.expected_status must equal 200".into(),
        ));
    }
    Ok(())
}

fn validate_service_state(service_id: &str, state: &RawStateConfig) -> Result<(), ForgeYamlError> {
    if state
        .volume
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        return Err(ForgeYamlError::Invalid(format!(
            "service `{service_id}` state.volume is required"
        )));
    }
    let mount_path = state.mount_path.as_deref().unwrap_or_default();
    if mount_path.trim().is_empty() {
        return Err(ForgeYamlError::Invalid(format!(
            "service `{service_id}` state.mount_path is required"
        )));
    }
    if !mount_path.starts_with('/') {
        return Err(ForgeYamlError::Invalid(format!(
            "service `{service_id}` state.mount_path must start with `/`"
        )));
    }
    parse_state_retention(service_id, state)?;
    Ok(())
}

fn parse_state_retention(
    service_id: &str,
    state: &RawStateConfig,
) -> Result<PersistedVolumeRetention, ForgeYamlError> {
    match state.retention.as_deref() {
        Some("persistent") => Ok(PersistedVolumeRetention::Persistent),
        Some("ephemeral") => Ok(PersistedVolumeRetention::Ephemeral),
        None | Some("") => Err(ForgeYamlError::Invalid(format!(
            "service `{service_id}` state.retention is required"
        ))),
        Some(_) => Err(ForgeYamlError::Invalid(format!(
            "service `{service_id}` state.retention must be one of `persistent`, `ephemeral`"
        ))),
    }
}

fn validation_for_runtime(
    runtime: &RawRuntimeConfig,
    fallback_health_path: Option<String>,
) -> Result<ValidationPolicy, ForgeYamlError> {
    if let Some(port) = runtime.port {
        if port == 0 {
            return Err(ForgeYamlError::Invalid(
                "forge.yml runtime.port must be a valid TCP port".into(),
            ));
        }
    }
    if let Some(healthcheck) = runtime.healthcheck.as_ref() {
        validate_healthcheck(healthcheck)?;
    }
    let activation = match runtime.port {
        Some(port) => ActivationMode::Http {
            internal_port: port,
        },
        None => ActivationMode::Direct,
    };
    Ok(ValidationPolicy {
        tcp_required: runtime.port.is_some(),
        http_health_path: runtime
            .healthcheck
            .as_ref()
            .map(|value| value.path.clone())
            .or(fallback_health_path),
        activation,
        ..ValidationPolicy::default()
    })
}

fn runtime_policy_for_runtime(
    runtime: &RawRuntimeConfig,
) -> Result<ForgeRuntimePolicy, ForgeYamlError> {
    let cpu_limit = runtime
        .cpu
        .as_ref()
        .map(|cpu| validate_cpu_limit(&cpu.limit))
        .transpose()?;
    let memory_limit_mb = runtime
        .memory
        .as_ref()
        .map(|memory| {
            if memory.limit_mb == 0 {
                Err(ForgeYamlError::Invalid(
                    "forge.yml runtime.memory.limit_mb must be greater than 0".into(),
                ))
            } else {
                Ok(memory.limit_mb)
            }
        })
        .transpose()?;
    let (restart_policy, max_retries) = match runtime.restart.as_ref() {
        Some(restart) => validate_restart_policy(restart)?,
        None => ("no".into(), None),
    };
    Ok(ForgeRuntimePolicy {
        cpu_limit,
        memory_limit_mb,
        restart_policy,
        max_retries,
    })
}

fn validate_cpu_limit(value: &str) -> Result<String, ForgeYamlError> {
    let parsed = value.parse::<f64>().map_err(|_| {
        ForgeYamlError::Invalid("forge.yml runtime.cpu.limit must be a positive number".into())
    })?;
    if parsed <= 0.0 {
        return Err(ForgeYamlError::Invalid(
            "forge.yml runtime.cpu.limit must be a positive number".into(),
        ));
    }
    Ok(value.to_string())
}

fn validate_restart_policy(
    restart: &RawRestartConfig,
) -> Result<(String, Option<u64>), ForgeYamlError> {
    match restart.policy.as_str() {
        "always" | "unless-stopped" | "no" => Ok((restart.policy.clone(), None)),
        "on-failure" => Ok((restart.policy.clone(), restart.max_retries)),
        other => Err(ForgeYamlError::Invalid(format!(
            "forge.yml runtime.restart.policy `{other}` must be one of always, on-failure, unless-stopped, no"
        ))),
    }
}

fn topological_service_order(
    services: &BTreeMap<String, RawServiceConfig>,
) -> Result<Vec<String>, ForgeYamlError> {
    let mut pending = services
        .iter()
        .map(|(service_id, config)| (service_id.clone(), service_depends_on(config)))
        .collect::<BTreeMap<_, _>>();
    for (service_id, depends_on) in &pending {
        for dependency in depends_on {
            if dependency == service_id {
                return Err(ForgeYamlError::Invalid(format!(
                    "service `{service_id}` cannot depend on itself"
                )));
            }
            if !services.contains_key(dependency) {
                return Err(ForgeYamlError::Invalid(format!(
                    "service `{service_id}` depends on unknown service `{dependency}`"
                )));
            }
        }
    }

    let mut order = Vec::new();
    while !pending.is_empty() {
        let ready = pending
            .iter()
            .filter(|(_, deps)| deps.iter().all(|dep| order.contains(dep)))
            .map(|(service_id, _)| service_id.clone())
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err(ForgeYamlError::Invalid(
                "service dependency graph contains a cycle".into(),
            ));
        }
        for service_id in ready {
            pending.remove(&service_id);
            order.push(service_id);
        }
    }
    Ok(order)
}

fn service_depends_on(service: &RawServiceConfig) -> Vec<String> {
    if service.depends_on.is_empty() {
        service.runtime.depends_on.clone()
    } else {
        service.depends_on.clone()
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

    #[test]
    fn multiservice_manifest_accepts_build_and_runtime() {
        let root = test_root("multiservice-manifest-accepts-build-and-runtime");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "version: 1\n",
                "name: api\n",
                "type: web\n",
                "services:\n",
                "  api:\n",
                "    build:\n",
                "      dockerfile: Dockerfile\n",
                "      context: .\n",
                "    runtime:\n",
                "      port: 3000\n",
                "      healthcheck:\n",
                "        path: /health\n",
                "        expected_status: 200\n",
                "    expose: true\n",
                "  worker:\n",
                "    build:\n",
                "      dockerfile: Dockerfile.worker\n",
                "      context: .\n",
                "    runtime:\n",
                "      command: node worker.js\n",
                "    depends_on:\n",
                "      - api\n",
                "    expose: false\n",
            ),
        )
        .unwrap();

        let config = load_optional_forge_yaml(&root, "api").unwrap().unwrap();
        assert_eq!(
            config.startup_order(),
            &["api".to_string(), "worker".to_string()]
        );
        assert_eq!(
            config.services()["api"]
                .build
                .as_ref()
                .unwrap()
                .dockerfile_path,
            root.join("Dockerfile")
        );
        assert_eq!(
            config.services()["worker"]
                .build
                .as_ref()
                .unwrap()
                .dockerfile_path,
            root.join("Dockerfile.worker")
        );
        assert_eq!(
            config.services()["worker"].depends_on,
            vec!["api".to_string()]
        );
        assert!(config.services()["api"].externally_exposed);
        assert!(!config.services()["worker"].externally_exposed);
    }

    #[test]
    fn service_state_schema_accepts_persistent_volume() {
        let root = test_root("service-state-schema-accepts-persistent-volume");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "services:\n",
                "  redis:\n",
                "    runtime:\n",
                "      image: redis:7\n",
                "    state:\n",
                "      volume: redis-data\n",
                "      mount_path: /data\n",
                "      retention: persistent\n",
                "    expose: false\n",
            ),
        )
        .unwrap();

        let config = load_optional_forge_yaml(&root, "api").unwrap().unwrap();
        let state = config.services()["redis"].state.as_ref().unwrap();
        assert_eq!(state.volume, "redis-data");
        assert_eq!(state.mount_path, "/data");
        assert_eq!(state.retention, PersistedVolumeRetention::Persistent);
    }

    #[test]
    fn service_state_schema_accepts_ephemeral_volume() {
        let root = test_root("service-state-schema-accepts-ephemeral-volume");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "services:\n",
                "  redis:\n",
                "    runtime:\n",
                "      image: redis:7\n",
                "    state:\n",
                "      volume: redis-data\n",
                "      mount_path: /data\n",
                "      retention: ephemeral\n",
                "    expose: false\n",
            ),
        )
        .unwrap();

        let config = load_optional_forge_yaml(&root, "api").unwrap().unwrap();
        assert_eq!(
            config.services()["redis"].state.as_ref().unwrap().retention,
            PersistedVolumeRetention::Ephemeral
        );
    }

    #[test]
    fn service_state_schema_rejects_missing_volume() {
        let root = test_root("service-state-schema-rejects-missing-volume");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "services:\n",
                "  redis:\n",
                "    runtime:\n",
                "      image: redis:7\n",
                "    state:\n",
                "      mount_path: /data\n",
                "      retention: persistent\n",
                "    expose: false\n",
            ),
        )
        .unwrap();

        let err = load_optional_forge_yaml(&root, "api").unwrap_err();
        assert!(
            err.to_string()
                .contains("service `redis` state.volume is required")
        );
    }

    #[test]
    fn service_state_schema_rejects_invalid_retention() {
        let root = test_root("service-state-schema-rejects-invalid-retention");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "services:\n",
                "  redis:\n",
                "    runtime:\n",
                "      image: redis:7\n",
                "    state:\n",
                "      volume: redis-data\n",
                "      mount_path: /data\n",
                "      retention: durable\n",
                "    expose: false\n",
            ),
        )
        .unwrap();

        let err = load_optional_forge_yaml(&root, "api").unwrap_err();
        assert!(
            err.to_string().contains(
                "service `redis` state.retention must be one of `persistent`, `ephemeral`"
            )
        );
    }

    #[test]
    fn live_stateful_manifest_parses_successfully() {
        let root = test_root("live-stateful-manifest-parses-successfully");
        fs::write(
            root.join("forge.yml"),
            concat!(
                "services:\n",
                "  redis:\n",
                "    runtime:\n",
                "      image: redis:7\n",
                "    state:\n",
                "      volume: redis-data\n",
                "      mount_path: /data\n",
                "      retention: persistent\n",
                "    expose: false\n",
            ),
        )
        .unwrap();

        let config = load_optional_forge_yaml(&root, "forge-stateful-test")
            .unwrap()
            .unwrap();
        let redis = &config.services()["redis"];
        assert_eq!(redis.image.as_deref(), Some("redis:7"));
        assert!(!redis.externally_exposed);
        assert!(redis.state.is_some());
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
