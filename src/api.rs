use crate::events::EventRecord;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentRequest {
    pub project_id: String,
    pub environment: String,
    pub intent: String,
    #[serde(default)]
    pub source_path: Option<PathBuf>,
    #[serde(default)]
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentAccepted {
    pub deployment_id: String,
    pub queue_position: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentStatus {
    pub deployment_id: String,
    pub project_id: String,
    pub environment: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentLogs {
    pub deployment_id: String,
    pub lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventList {
    pub events: Vec<EventRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectUpsertRequest {
    #[serde(default)]
    pub project_id: Option<String>,
    pub repo_url: String,
    pub default_branch: String,
    #[serde(default)]
    pub base_domain: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub project_id: String,
    pub repo_url: String,
    pub default_branch: String,
    pub base_domain: String,
    pub domain_mode: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectList {
    pub projects: Vec<ProjectRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliLoginStartResponse {
    pub code: String,
    pub expires_at_unix: u64,
    pub poll_interval_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliLoginPollRequest {
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliLoginPollResponse {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

pub fn validate_deployment_request(request: &DeploymentRequest) -> Result<(), ErrorResponse> {
    if request.project_id.is_empty() {
        return Err(ErrorResponse {
            code: "invalid_project_id".into(),
            message: "project_id must not be empty".into(),
        });
    }

    if !matches!(
        request.environment.as_str(),
        "development" | "staging" | "production"
    ) {
        return Err(ErrorResponse {
            code: "invalid_environment".into(),
            message: "environment must be one of development, staging, production".into(),
        });
    }

    if !matches!(request.intent.as_str(), "deploy" | "redeploy" | "rollback") {
        return Err(ErrorResponse {
            code: "invalid_intent".into(),
            message: "intent must be one of deploy, redeploy, rollback".into(),
        });
    }

    if request
        .source_path
        .as_ref()
        .is_some_and(|path| path.as_os_str().is_empty())
    {
        return Err(ErrorResponse {
            code: "invalid_source_path".into(),
            message: "source_path must not be empty".into(),
        });
    }

    if request
        .source_ref
        .as_ref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(ErrorResponse {
            code: "invalid_source_ref".into(),
            message: "source_ref must not be empty".into(),
        });
    }

    if request.source_path.is_some() && request.source_ref.is_some() {
        return Err(ErrorResponse {
            code: "invalid_source".into(),
            message: "source_path and source_ref are mutually exclusive".into(),
        });
    }

    if request.intent == "rollback"
        && (request.source_path.is_some() || request.source_ref.is_some())
    {
        return Err(ErrorResponse {
            code: "invalid_source".into(),
            message: "source_path and source_ref are only supported for deploy intents".into(),
        });
    }

    Ok(())
}
