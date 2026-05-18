#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentRequest {
    pub project_id: String,
    pub environment: String,
    pub intent: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentAccepted {
    pub deployment_id: String,
    pub queue_position: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}
