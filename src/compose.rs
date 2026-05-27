use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_yaml::{Mapping, Value};

use crate::forge_yaml::{ForgeYamlError, load_optional_forge_yaml};

const COMPOSE_FILE_NAMES: [&str; 3] = ["docker-compose.yml", "compose.yml", "compose.yaml"];
const HTTP_LIKE_PORTS: [u16; 12] = [
    80, 3000, 4000, 5000, 5173, 8000, 8080, 8081, 8088, 8888, 9000, 3001,
];
const KNOWN_INTERNAL_IMAGES: [&str; 6] =
    ["redis", "postgres", "mysql", "mariadb", "mongo", "rabbitmq"];

#[derive(Debug)]
pub enum ComposeError {
    Io(std::io::Error),
    Invalid(String),
}

impl Display for ComposeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Invalid(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ComposeError {}

impl From<std::io::Error> for ComposeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeDetection {
    pub search_root: PathBuf,
    pub detected_files: Vec<PathBuf>,
    pub selected_file: Option<PathBuf>,
    pub services: Vec<String>,
    pub public_candidates: Vec<String>,
    pub internal_services: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposePreview {
    pub compose_file: PathBuf,
    pub project_name: String,
    pub services: Vec<ComposeServicePreview>,
    pub public_candidates: Vec<String>,
    pub internal_services: Vec<String>,
    pub required_env_keys: Vec<String>,
    pub unsupported_fields: Vec<String>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub generated_forge_yaml: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeServicePreview {
    pub service_id: String,
    pub build: Option<ComposeBuildPreview>,
    pub image: Option<String>,
    pub ports: Vec<ComposePortPreview>,
    pub depends_on: Vec<String>,
    pub environment_keys: Vec<String>,
    pub healthcheck: Option<String>,
    pub command: Option<String>,
    pub restart: Option<String>,
    pub classification: ServiceClassification,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeBuildPreview {
    pub context: String,
    pub dockerfile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposePortPreview {
    pub raw: String,
    pub host_port: Option<u16>,
    pub container_port: u16,
    pub protocol: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceClassification {
    PublicCandidate,
    Internal,
    Ambiguous,
}

pub fn detect_compose(root: &Path) -> Result<ComposeDetection, ComposeError> {
    let (search_root, detected_files) = detect_compose_files(root)?;
    let mut warnings = Vec::new();
    if detected_files.len() > 1 {
        warnings.push(format!(
            "multiple Compose files detected: {}",
            detected_files
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let selected_file = detected_files.first().cloned();
    let (services, public_candidates, internal_services) =
        if let Some(path) = selected_file.as_ref() {
            let analysis = analyze_compose(path)?;
            (
                analysis
                    .services
                    .iter()
                    .map(|service| service.service_id.clone())
                    .collect(),
                analysis.public_candidates,
                analysis.internal_services,
            )
        } else {
            (Vec::new(), Vec::new(), Vec::new())
        };
    Ok(ComposeDetection {
        search_root,
        detected_files,
        selected_file,
        services,
        public_candidates,
        internal_services,
        warnings,
    })
}

pub fn preview_compose(path: &Path) -> Result<ComposePreview, ComposeError> {
    let analysis = analyze_compose(path)?;
    let generated_forge_yaml = if analysis.errors.is_empty() {
        Some(render_generated_forge_yaml(&analysis)?)
    } else {
        None
    };
    Ok(ComposePreview {
        compose_file: analysis.compose_file,
        project_name: analysis.project_name,
        services: analysis.services,
        public_candidates: analysis.public_candidates,
        internal_services: analysis.internal_services,
        required_env_keys: analysis.required_env_keys,
        unsupported_fields: analysis.unsupported_fields,
        warnings: analysis.warnings,
        errors: analysis.errors,
        generated_forge_yaml,
    })
}

pub fn convert_compose(
    path: &Path,
    out_path: Option<&Path>,
    force: bool,
) -> Result<String, ComposeError> {
    let preview = preview_compose(path)?;
    if !preview.errors.is_empty() {
        return Err(ComposeError::Invalid(format!(
            "compose conversion failed:\n{}",
            preview.errors.join("\n")
        )));
    }
    let rendered = preview.generated_forge_yaml.clone().ok_or_else(|| {
        ComposeError::Invalid("compose conversion did not produce forge.yml".into())
    })?;
    validate_generated_forge_yaml(&rendered, &preview.project_name)?;
    if let Some(out_path) = out_path {
        if out_path.exists() && !force {
            return Err(ComposeError::Invalid(format!(
                "{} already exists; rerun with --force to overwrite",
                out_path.display()
            )));
        }
        fs::write(out_path, &rendered)?;
    }
    Ok(rendered)
}

pub fn explain_compose(path: &Path) -> Result<String, ComposeError> {
    let preview = preview_compose(path)?;
    let mut lines = Vec::new();
    lines.push(format!("Compose input: {}", preview.compose_file.display()));
    lines.push(
        "Compose is input only. Forge keeps forge.yml as the canonical runtime contract.".into(),
    );
    lines.push(format!(
        "Derived Forge project name: {}",
        preview.project_name
    ));
    if preview.public_candidates.is_empty() {
        lines.push("No single public HTTP service could be inferred.".into());
    } else {
        lines.push(format!(
            "Public service candidates: {}",
            preview.public_candidates.join(", ")
        ));
    }
    if !preview.internal_services.is_empty() {
        lines.push(format!(
            "Internal services: {}",
            preview.internal_services.join(", ")
        ));
    }
    if !preview.required_env_keys.is_empty() {
        lines.push(format!(
            "Required env keys: {}",
            preview.required_env_keys.join(", ")
        ));
    }
    lines.push(
        "Environment values are never copied into forge.yml. Import keys into Forge Env Manager."
            .into(),
    );
    if !preview.unsupported_fields.is_empty() {
        lines.push(format!(
            "Unsupported Compose fields are dropped with warnings: {}",
            preview.unsupported_fields.join(", ")
        ));
    }
    if !preview.errors.is_empty() {
        lines.push(format!(
            "Blocking conversion errors: {}",
            preview.errors.join(" | ")
        ));
    }
    Ok(lines.join("\n"))
}

pub fn compose_file_names() -> &'static [&'static str] {
    &COMPOSE_FILE_NAMES
}

#[derive(Debug, Clone)]
struct ComposeAnalysis {
    compose_file: PathBuf,
    project_name: String,
    services: Vec<ComposeServicePreview>,
    public_candidates: Vec<String>,
    internal_services: Vec<String>,
    required_env_keys: Vec<String>,
    unsupported_fields: Vec<String>,
    warnings: Vec<String>,
    errors: Vec<String>,
}

fn detect_compose_files(root: &Path) -> Result<(PathBuf, Vec<PathBuf>), ComposeError> {
    if root.is_file() {
        let file_name = root
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| {
                ComposeError::Invalid(format!("unsupported Compose file path {}", root.display()))
            })?;
        if COMPOSE_FILE_NAMES.contains(&file_name) {
            return Ok((
                root.parent()
                    .unwrap_or_else(|| Path::new("."))
                    .to_path_buf(),
                vec![root.to_path_buf()],
            ));
        }
        return Err(ComposeError::Invalid(format!(
            "unsupported Compose file name `{file_name}`; expected one of {}",
            COMPOSE_FILE_NAMES.join(", ")
        )));
    }

    let detected = COMPOSE_FILE_NAMES
        .iter()
        .map(|name| root.join(name))
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    Ok((root.to_path_buf(), detected))
}

fn analyze_compose(path: &Path) -> Result<ComposeAnalysis, ComposeError> {
    let raw = fs::read_to_string(path)?;
    let yaml = serde_yaml::from_str::<Value>(&raw)
        .map_err(|err| ComposeError::Invalid(format!("invalid Compose YAML: {err}")))?;
    let root = yaml
        .as_mapping()
        .ok_or_else(|| ComposeError::Invalid("compose file root must be a mapping".into()))?;
    let services_value = root
        .get(Value::String("services".into()))
        .ok_or_else(|| ComposeError::Invalid("compose file must define services".into()))?;
    let services_mapping = services_value
        .as_mapping()
        .ok_or_else(|| ComposeError::Invalid("compose services must be a mapping".into()))?;
    if services_mapping.is_empty() {
        return Err(ComposeError::Invalid(
            "compose services must not be empty".into(),
        ));
    }

    let project_name = derive_project_name(path, root);
    let mut services = Vec::new();
    let mut public_candidates = Vec::new();
    let mut internal_services = Vec::new();
    let mut required_env_keys = BTreeSet::new();
    let mut warnings = Vec::new();
    let mut errors = Vec::new();
    let mut unsupported_fields = BTreeSet::new();

    collect_root_unsupported_fields(root, &mut unsupported_fields);

    for (service_key, service_value) in services_mapping {
        let Some(service_id) = service_key.as_str() else {
            return Err(ComposeError::Invalid("service ids must be strings".into()));
        };
        let service = parse_service(
            service_id,
            service_value,
            &mut warnings,
            &mut unsupported_fields,
        )?;
        if matches!(
            service.classification,
            ServiceClassification::PublicCandidate
        ) && service.healthcheck.is_none()
        {
            warnings.push(format!(
                "service `{service_id}` has no convertible HTTP healthcheck. Consider adding /health manually in forge.yml after conversion."
            ));
        }
        match service.classification {
            ServiceClassification::PublicCandidate => {
                public_candidates.push(service.service_id.clone())
            }
            ServiceClassification::Internal => internal_services.push(service.service_id.clone()),
            ServiceClassification::Ambiguous => {}
        }
        for key in &service.environment_keys {
            required_env_keys.insert(key.clone());
        }
        services.push(service);
    }

    if public_candidates.len() > 1 {
        errors.push(format!(
            "multiple public service candidates detected: {}. Compose import will not guess which service should be routed. Explicit selection is required in a future version.",
            public_candidates.join(", ")
        ));
    } else if public_candidates.is_empty() {
        errors.push("no public HTTP service candidate detected. Forge currently requires one routed web service.".into());
    }

    Ok(ComposeAnalysis {
        compose_file: path.to_path_buf(),
        project_name,
        services,
        public_candidates,
        internal_services,
        required_env_keys: required_env_keys.into_iter().collect(),
        unsupported_fields: unsupported_fields.into_iter().collect(),
        warnings,
        errors,
    })
}

fn parse_service(
    service_id: &str,
    value: &Value,
    warnings: &mut Vec<String>,
    unsupported_fields: &mut BTreeSet<String>,
) -> Result<ComposeServicePreview, ComposeError> {
    let mapping = value.as_mapping().ok_or_else(|| {
        ComposeError::Invalid(format!("service `{service_id}` must be a mapping"))
    })?;

    collect_service_unsupported_fields(service_id, mapping, unsupported_fields);

    let build = parse_build(
        service_id,
        mapping.get(Value::String("build".into())),
        warnings,
        unsupported_fields,
    )?;
    let image = mapping
        .get(Value::String("image".into()))
        .and_then(Value::as_str)
        .map(str::to_string);
    let ports = parse_ports(
        service_id,
        mapping.get(Value::String("ports".into())),
        warnings,
    )?;
    let depends_on = parse_depends_on(
        service_id,
        mapping.get(Value::String("depends_on".into())),
        warnings,
    )?;
    let (environment_keys, environment_warnings) =
        parse_environment(service_id, mapping.get(Value::String("environment".into())))?;
    warnings.extend(environment_warnings);
    let (healthcheck, healthcheck_warnings) =
        parse_healthcheck(service_id, mapping.get(Value::String("healthcheck".into())))?;
    warnings.extend(healthcheck_warnings);
    let command = parse_string_or_list(mapping.get(Value::String("command".into())))
        .map_err(|message| ComposeError::Invalid(format!("service `{service_id}` {message}")))?;
    let restart = mapping
        .get(Value::String("restart".into()))
        .map(parse_restart_value)
        .transpose()?
        .flatten();

    let classification = classify_service(service_id, image.as_deref(), &ports);

    Ok(ComposeServicePreview {
        service_id: service_id.to_string(),
        build,
        image,
        ports,
        depends_on,
        environment_keys,
        healthcheck,
        command,
        restart,
        classification,
    })
}

fn parse_build(
    service_id: &str,
    value: Option<&Value>,
    warnings: &mut Vec<String>,
    unsupported_fields: &mut BTreeSet<String>,
) -> Result<Option<ComposeBuildPreview>, ComposeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Some(context) = value.as_str() {
        return Ok(Some(ComposeBuildPreview {
            context: context.to_string(),
            dockerfile: None,
        }));
    }
    let mapping = value.as_mapping().ok_or_else(|| {
        ComposeError::Invalid(format!(
            "service `{service_id}` build must be a string or mapping"
        ))
    })?;
    for key in mapping.keys().filter_map(Value::as_str) {
        if !matches!(key, "context" | "dockerfile") {
            unsupported_fields.insert(format!("services.{service_id}.build.{key}"));
        }
    }
    let context = mapping
        .get(Value::String("context".into()))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ComposeError::Invalid(format!("service `{service_id}` build.context is required"))
        })?;
    let dockerfile = mapping
        .get(Value::String("dockerfile".into()))
        .and_then(Value::as_str)
        .map(str::to_string);
    if dockerfile.is_none() {
        warnings.push(format!(
            "service `{service_id}` uses build.context without build.dockerfile. Forge will default to Dockerfile."
        ));
    }
    Ok(Some(ComposeBuildPreview {
        context: context.to_string(),
        dockerfile,
    }))
}

fn parse_ports(
    service_id: &str,
    value: Option<&Value>,
    warnings: &mut Vec<String>,
) -> Result<Vec<ComposePortPreview>, ComposeError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let sequence = value.as_sequence().ok_or_else(|| {
        ComposeError::Invalid(format!("service `{service_id}` ports must be a list"))
    })?;
    let mut ports = Vec::new();
    for entry in sequence {
        let raw = entry.as_str().ok_or_else(|| {
            ComposeError::Invalid(format!(
                "service `{service_id}` port entries must be strings"
            ))
        })?;
        if let Some(port) = parse_port_mapping(raw) {
            if let Some(host_port) = port.host_port
                && host_port != port.container_port
            {
                warnings.push(format!(
                    "service `{service_id}` host port {host_port} is ignored. Forge routes by domain and will use container port {}.",
                    port.container_port
                ));
            }
            ports.push(port);
        } else {
            warnings.push(format!(
                "service `{service_id}` port mapping `{raw}` is not supported. Supported forms: \"3000:3000\" and \"3000\"."
            ));
        }
    }
    Ok(ports)
}

fn parse_port_mapping(raw: &str) -> Option<ComposePortPreview> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.contains('/') || trimmed.matches(':').count() > 1 {
        return None;
    }
    if let Ok(container_port) = trimmed.parse::<u16>() {
        return Some(ComposePortPreview {
            raw: trimmed.to_string(),
            host_port: None,
            container_port,
            protocol: "tcp".into(),
        });
    }
    let mut parts = trimmed.split(':');
    let host_port = parts.next()?.parse::<u16>().ok()?;
    let container_port = parts.next()?.parse::<u16>().ok()?;
    Some(ComposePortPreview {
        raw: trimmed.to_string(),
        host_port: Some(host_port),
        container_port,
        protocol: "tcp".into(),
    })
}

fn parse_depends_on(
    service_id: &str,
    value: Option<&Value>,
    warnings: &mut Vec<String>,
) -> Result<Vec<String>, ComposeError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if let Some(sequence) = value.as_sequence() {
        return sequence
            .iter()
            .map(|entry| {
                entry.as_str().map(str::to_string).ok_or_else(|| {
                    ComposeError::Invalid(format!(
                        "service `{service_id}` depends_on entries must be strings"
                    ))
                })
            })
            .collect();
    }
    if let Some(mapping) = value.as_mapping() {
        let mut depends_on = Vec::new();
        for (dependency, config) in mapping {
            let dependency = dependency.as_str().ok_or_else(|| {
                ComposeError::Invalid(format!(
                    "service `{service_id}` depends_on keys must be strings"
                ))
            })?;
            depends_on.push(dependency.to_string());
            if let Some(config_mapping) = config.as_mapping()
                && config_mapping.contains_key(Value::String("condition".into()))
            {
                warnings.push(format!(
                    "service `{service_id}` depends_on condition for `{dependency}` is not supported. Forge will preserve dependency ordering only."
                ));
            }
        }
        return Ok(depends_on);
    }
    Err(ComposeError::Invalid(format!(
        "service `{service_id}` depends_on must be a list or mapping"
    )))
}

fn parse_environment(
    service_id: &str,
    value: Option<&Value>,
) -> Result<(Vec<String>, Vec<String>), ComposeError> {
    let Some(value) = value else {
        return Ok((Vec::new(), Vec::new()));
    };
    let mut keys = BTreeSet::new();
    let mut warnings = Vec::new();
    match value {
        Value::Mapping(mapping) => {
            for key in mapping.keys().filter_map(Value::as_str) {
                keys.insert(key.to_string());
            }
        }
        Value::Sequence(sequence) => {
            for entry in sequence {
                let raw = entry.as_str().ok_or_else(|| {
                    ComposeError::Invalid(format!(
                        "service `{service_id}` environment entries must be strings"
                    ))
                })?;
                let key = raw.split('=').next().unwrap_or_default().trim();
                if key.is_empty() {
                    return Err(ComposeError::Invalid(format!(
                        "service `{service_id}` environment entry `{raw}` is invalid"
                    )));
                }
                keys.insert(key.to_string());
            }
        }
        _ => {
            return Err(ComposeError::Invalid(format!(
                "service `{service_id}` environment must be a mapping or list"
            )));
        }
    }
    if !keys.is_empty() {
        if keys.contains("REDIS_URL") {
            warnings.push("Import REDIS_URL into Forge Env Manager before deploying.".into());
        }
        warnings.push(format!(
            "service `{service_id}` defines environment variables. Import these keys into Forge Env Manager: {}",
            keys.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    Ok((keys.into_iter().collect(), warnings))
}

fn parse_healthcheck(
    service_id: &str,
    value: Option<&Value>,
) -> Result<(Option<String>, Vec<String>), ComposeError> {
    let Some(value) = value else {
        return Ok((None, Vec::new()));
    };
    let mapping = value.as_mapping().ok_or_else(|| {
        ComposeError::Invalid(format!(
            "service `{service_id}` healthcheck must be a mapping"
        ))
    })?;
    let mut warnings = Vec::new();
    let test = mapping.get(Value::String("test".into())).ok_or_else(|| {
        ComposeError::Invalid(format!(
            "service `{service_id}` healthcheck.test is required"
        ))
    })?;
    let command = parse_healthcheck_test(test).ok_or_else(|| {
        ComposeError::Invalid(format!(
            "service `{service_id}` healthcheck.test must be a string or list"
        ))
    })?;
    if let Some(path) = extract_http_health_path(&command) {
        return Ok((Some(path), warnings));
    }
    warnings.push(format!(
        "service `{service_id}` Compose healthcheck could not be converted. Add runtime.healthcheck.path manually."
    ));
    Ok((None, warnings))
}

fn parse_healthcheck_test(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_string());
    }
    value
        .as_sequence()
        .map(|parts| {
            parts
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.trim().is_empty())
}

fn parse_string_or_list(value: Option<&Value>) -> Result<Option<String>, &'static str> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Some(text) = value.as_str() {
        return Ok(Some(text.to_string()));
    }
    let Some(sequence) = value.as_sequence() else {
        return Err("command must be a string or list");
    };
    let mut parts = Vec::new();
    for entry in sequence {
        let text = entry
            .as_str()
            .ok_or("command list entries must be strings")?;
        parts.push(shell_quote(text));
    }
    Ok(Some(parts.join(" ")))
}

fn parse_restart_value(value: &Value) -> Result<Option<String>, ComposeError> {
    let raw = value
        .as_str()
        .ok_or_else(|| ComposeError::Invalid("restart must be a string".into()))?;
    Ok(Some(raw.to_string()))
}

fn classify_service(
    service_id: &str,
    image: Option<&str>,
    ports: &[ComposePortPreview],
) -> ServiceClassification {
    if is_known_internal_image(image) {
        return ServiceClassification::Internal;
    }
    if has_http_like_port(ports) {
        return ServiceClassification::PublicCandidate;
    }
    if image.is_some() {
        return ServiceClassification::Internal;
    }
    let _ = service_id;
    ServiceClassification::Ambiguous
}

fn is_known_internal_image(image: Option<&str>) -> bool {
    image.is_some_and(|image| {
        let lower = image.to_ascii_lowercase();
        KNOWN_INTERNAL_IMAGES.iter().any(|needle| {
            lower == *needle
                || lower.starts_with(&format!("{needle}:"))
                || lower.contains(&format!("/{needle}:"))
                || lower.ends_with(&format!("/{needle}"))
        })
    })
}

fn has_http_like_port(ports: &[ComposePortPreview]) -> bool {
    ports
        .iter()
        .any(|port| HTTP_LIKE_PORTS.contains(&port.container_port))
}

fn choose_http_port(service: &ComposeServicePreview, warnings: &mut Vec<String>) -> Option<u16> {
    let http_ports = service
        .ports
        .iter()
        .filter(|port| HTTP_LIKE_PORTS.contains(&port.container_port))
        .collect::<Vec<_>>();
    if http_ports.len() > 1 {
        warnings.push(format!(
            "service `{}` exposes multiple HTTP-like ports. Forge will use container port {}.",
            service.service_id, http_ports[0].container_port
        ));
    }
    http_ports.first().map(|port| port.container_port)
}

fn render_generated_forge_yaml(analysis: &ComposeAnalysis) -> Result<String, ComposeError> {
    let public_service_id =
        analysis.public_candidates.first().cloned().ok_or_else(|| {
            ComposeError::Invalid("no public HTTP service candidate detected".into())
        })?;
    let mut warnings = analysis.warnings.clone();
    let mut services = BTreeMap::new();
    for service in &analysis.services {
        let expose = service.service_id == public_service_id;
        let runtime_port = if expose {
            choose_http_port(service, &mut warnings)
        } else {
            None
        };
        let healthcheck = if expose {
            match (service.healthcheck.as_ref(), runtime_port) {
                (Some(path), Some(_)) => Some(OutputHealthcheck {
                    path: path.clone(),
                    expected_status: 200,
                }),
                (None, Some(_)) => None,
                _ => None,
            }
        } else {
            None
        };
        let build = service.build.as_ref().map(|build| OutputBuild {
            context: build.context.clone(),
            dockerfile: build
                .dockerfile
                .clone()
                .unwrap_or_else(|| "Dockerfile".into()),
        });
        let restart = service
            .restart
            .as_deref()
            .map(map_restart_policy)
            .transpose()?;
        let runtime = OutputRuntime {
            port: runtime_port,
            image: service.image.clone(),
            command: service.command.clone(),
            healthcheck,
            restart,
        };
        services.insert(
            service.service_id.clone(),
            OutputService {
                build,
                runtime,
                depends_on: service.depends_on.clone(),
                expose,
            },
        );
    }
    let document = OutputForgeYaml {
        version: 1,
        name: analysis.project_name.clone(),
        app_type: "web".into(),
        services,
    };
    let rendered = serde_yaml::to_string(&document)
        .map_err(|err| ComposeError::Invalid(format!("failed to render forge.yml: {err}")))?;
    let _ = warnings;
    Ok(rendered)
}

fn map_restart_policy(raw: &str) -> Result<OutputRestart, ComposeError> {
    let lower = raw.trim().to_ascii_lowercase();
    if lower == "always" || lower == "unless-stopped" || lower == "no" {
        return Ok(OutputRestart {
            policy: lower,
            max_retries: None,
        });
    }
    if let Some(value) = lower.strip_prefix("on-failure") {
        let max_retries = value
            .strip_prefix(':')
            .and_then(|value| value.parse::<u64>().ok());
        return Ok(OutputRestart {
            policy: "on-failure".into(),
            max_retries,
        });
    }
    Err(ComposeError::Invalid(format!(
        "unsupported restart policy `{raw}`"
    )))
}

fn validate_generated_forge_yaml(
    rendered: &str,
    expected_project_id: &str,
) -> Result<(), ComposeError> {
    let temp_root = std::env::temp_dir().join(format!(
        "forge-compose-validate-{}-{}",
        expected_project_id,
        std::process::id()
    ));
    fs::create_dir_all(&temp_root)?;
    fs::write(temp_root.join("forge.yml"), rendered)?;
    let result = load_optional_forge_yaml(&temp_root, expected_project_id);
    let _ = fs::remove_file(temp_root.join("forge.yml"));
    let _ = fs::remove_dir(&temp_root);
    match result {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(ComposeError::Invalid(
            "generated forge.yml was not found during validation".into(),
        )),
        Err(ForgeYamlError::Io(err)) => Err(ComposeError::Io(err)),
        Err(ForgeYamlError::Invalid(message)) => Err(ComposeError::Invalid(message)),
    }
}

fn extract_http_health_path(command: &str) -> Option<String> {
    for prefix in ["http://localhost", "http://127.0.0.1", "http://0.0.0.0"] {
        if let Some(index) = command.find(prefix) {
            let mut url = &command[index + prefix.len()..];
            if url.starts_with(':') {
                let offset = url.find('/').unwrap_or(url.len());
                url = &url[offset..];
            }
            let end = url.find(char::is_whitespace).unwrap_or(url.len());
            let path = url[..end].trim_end_matches(|ch| matches!(ch, '\'' | '"' | ')' | ','));
            if path.starts_with('/') {
                return Some(path.to_string());
            }
        }
    }
    None
}

fn collect_root_unsupported_fields(root: &Mapping, unsupported_fields: &mut BTreeSet<String>) {
    for key in root.keys().filter_map(Value::as_str) {
        match key {
            "version" | "name" | "services" => {}
            "profiles" | "deploy" | "secrets" | "configs" | "extends" | "network_mode" => {
                unsupported_fields.insert(key.to_string());
            }
            other if other.starts_with("x-") => {
                unsupported_fields.insert(other.to_string());
            }
            _ => {}
        }
    }
}

fn collect_service_unsupported_fields(
    service_id: &str,
    mapping: &Mapping,
    unsupported_fields: &mut BTreeSet<String>,
) {
    for key in mapping.keys().filter_map(Value::as_str) {
        match key {
            "build" | "image" | "ports" | "environment" | "depends_on" | "healthcheck"
            | "command" | "restart" => {}
            "profiles" | "deploy" | "network_mode" | "privileged" | "cap_add" | "cap_drop"
            | "extends" => {
                unsupported_fields.insert(format!("services.{service_id}.{key}"));
            }
            "volumes" => {
                unsupported_fields.insert(format!(
                    "services.{service_id}.volumes (host bind volumes are not imported)"
                ));
            }
            "networks" => {
                unsupported_fields.insert(format!("services.{service_id}.networks"));
            }
            "secrets" => {
                unsupported_fields.insert(format!("services.{service_id}.secrets"));
            }
            "configs" => {
                unsupported_fields.insert(format!("services.{service_id}.configs"));
            }
            other if other.starts_with("x-") => {
                unsupported_fields.insert(format!("services.{service_id}.{other}"));
            }
            _ => {}
        }
    }
}

fn derive_project_name(path: &Path, root: &Mapping) -> String {
    root.get(Value::String("name".into()))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(sanitize_project_name)
        .or_else(|| {
            path.parent()
                .and_then(Path::file_name)
                .and_then(|value| value.to_str())
                .map(sanitize_project_name)
        })
        .unwrap_or_else(|| "app".into())
}

fn sanitize_project_name(value: &str) -> String {
    let mut sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while sanitized.contains("--") {
        sanitized = sanitized.replace("--", "-");
    }
    sanitized.trim_matches('-').to_string()
}

fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '-' | '_' | ':'))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[derive(Debug, Clone, Serialize)]
struct OutputForgeYaml {
    version: u64,
    name: String,
    #[serde(rename = "type")]
    app_type: String,
    services: BTreeMap<String, OutputService>,
}

#[derive(Debug, Clone, Serialize)]
struct OutputService {
    #[serde(skip_serializing_if = "Option::is_none")]
    build: Option<OutputBuild>,
    runtime: OutputRuntime,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    depends_on: Vec<String>,
    expose: bool,
}

#[derive(Debug, Clone, Serialize)]
struct OutputBuild {
    context: String,
    dockerfile: String,
}

#[derive(Debug, Clone, Serialize)]
struct OutputRuntime {
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    healthcheck: Option<OutputHealthcheck>,
    #[serde(skip_serializing_if = "Option::is_none")]
    restart: Option<OutputRestart>,
}

#[derive(Debug, Clone, Serialize)]
struct OutputHealthcheck {
    path: String,
    expected_status: u16,
}

#[derive(Debug, Clone, Serialize)]
struct OutputRestart {
    policy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_retries: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn detects_docker_compose_file() {
        let root = test_root("compose-detect-docker-compose");
        fs::write(root.join("docker-compose.yml"), compose_app_redis_fixture()).unwrap();
        let detection = detect_compose(&root).unwrap();
        assert_eq!(
            detection
                .selected_file
                .unwrap()
                .file_name()
                .unwrap()
                .to_str(),
            Some("docker-compose.yml")
        );
        assert_eq!(detection.services, vec!["app", "redis"]);
        assert_eq!(detection.public_candidates, vec!["app"]);
        assert_eq!(detection.internal_services, vec!["redis"]);
    }

    #[test]
    fn detects_compose_yaml_names() {
        for name in ["compose.yml", "compose.yaml"] {
            let root = test_root(&format!("compose-detect-{name}"));
            fs::write(root.join(name), compose_app_redis_fixture()).unwrap();
            let detection = detect_compose(&root).unwrap();
            assert_eq!(
                detection
                    .selected_file
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str(),
                Some(name)
            );
        }
    }

    #[test]
    fn preview_classifies_app_and_redis_and_redacts_env_values() {
        let root = test_root("compose-preview-app-redis");
        let compose_path = root.join("docker-compose.yml");
        fs::write(&compose_path, compose_app_redis_fixture()).unwrap();

        let preview = preview_compose(&compose_path).unwrap();
        assert!(preview.errors.is_empty(), "{preview:#?}");
        assert_eq!(preview.public_candidates, vec!["app"]);
        assert_eq!(preview.internal_services, vec!["redis"]);
        assert_eq!(
            preview.project_name,
            sanitize_project_name(root.file_name().unwrap().to_str().unwrap())
        );
        assert!(
            preview
                .warnings
                .iter()
                .any(|warning| warning.contains("Import these keys into Forge Env Manager"))
        );
        let rendered = preview.generated_forge_yaml.clone().unwrap();
        assert!(rendered.contains(&format!("name: {}", preview.project_name)));
        assert!(rendered.contains("app:"));
        assert!(rendered.contains("expose: true"));
        assert!(rendered.contains("redis:"));
        assert!(rendered.contains("image: redis:alpine"));
        assert!(rendered.contains("expose: false"));
        assert!(rendered.contains("path: /health"));
        assert!(!rendered.contains("super-secret"));
        assert!(!rendered.contains("redis://redis:6379"));
    }

    #[test]
    fn warns_on_host_port_and_unsupported_fields() {
        let root = test_root("compose-preview-warnings");
        let compose_path = root.join("docker-compose.yml");
        fs::write(
            &compose_path,
            concat!(
                "services:\n",
                "  app:\n",
                "    build: .\n",
                "    ports:\n",
                "      - \"8080:3000\"\n",
                "    deploy:\n",
                "      replicas: 2\n",
                "    environment:\n",
                "      SECRET_TOKEN: super-secret\n",
            ),
        )
        .unwrap();

        let preview = preview_compose(&compose_path).unwrap();
        assert!(
            preview
                .warnings
                .iter()
                .any(|warning| warning.contains("host port 8080 is ignored"))
        );
        assert!(
            preview
                .unsupported_fields
                .iter()
                .any(|field| field.contains("services.app.deploy"))
        );
        assert!(!format!("{preview:#?}").contains("super-secret"));
    }

    #[test]
    fn warns_when_multiple_public_candidates_exist() {
        let root = test_root("compose-preview-multiple-public");
        let compose_path = root.join("docker-compose.yml");
        fs::write(
            &compose_path,
            concat!(
                "services:\n",
                "  app:\n",
                "    build: .\n",
                "    ports:\n",
                "      - \"3000:3000\"\n",
                "  admin:\n",
                "    build: .\n",
                "    ports:\n",
                "      - \"8080:8080\"\n",
            ),
        )
        .unwrap();

        let preview = preview_compose(&compose_path).unwrap();
        assert!(
            preview
                .errors
                .iter()
                .any(|error| error.contains("multiple public service candidates"))
        );
        assert!(preview.generated_forge_yaml.is_none());
    }

    fn compose_app_redis_fixture() -> &'static str {
        concat!(
            "services:\n",
            "  app:\n",
            "    build:\n",
            "      context: .\n",
            "      dockerfile: Dockerfile\n",
            "    ports:\n",
            "      - \"3000:3000\"\n",
            "    depends_on:\n",
            "      - redis\n",
            "    environment:\n",
            "      REDIS_URL: redis://redis:6379\n",
            "      SECRET_TOKEN: super-secret\n",
            "    healthcheck:\n",
            "      test: [\"CMD\", \"curl\", \"-f\", \"http://localhost:3000/health\"]\n",
            "    restart: unless-stopped\n",
            "  redis:\n",
            "    image: redis:alpine\n",
            "    ports:\n",
            "      - \"6379\"\n",
        )
    }

    fn test_root(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "forge-compose-tests-{}-{}-{}",
            name,
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }
}
