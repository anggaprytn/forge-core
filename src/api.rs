use serde::{Deserialize, Serialize};
use crate::events::EventRecord;

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct DeploymentRequest {
    pub project_id: String,
    pub environment: String,
    pub intent: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct DeploymentAccepted {
    pub deployment_id: String,
    pub queue_position: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct DeploymentStatus {
    pub deployment_id: String,
    pub project_id: String,
    pub environment: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct DeploymentLogs {
    pub deployment_id: String,
    pub lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct EventList {
    pub events: Vec<EventRecord>,
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

    Ok(())
}
