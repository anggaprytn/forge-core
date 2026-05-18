use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct EventRecord {
    pub timestamp_unix: u64,
    pub project_id: String,
    pub environment: String,
    pub generation: Option<u64>,
    pub deployment_id: Option<String>,
    pub event_type: String,
    pub reason: Option<String>,
}

pub fn redact_text(input: &str, secrets: &[String]) -> String {
    let mut result = input.to_string();
    for secret in secrets {
        if secret.len() >= 8 {
            result = result.replace(secret, "[REDACTED]");
        }
    }
    result
}

#[cfg(test)]
pub mod redaction_runs_before_event_or_log_delivery {
    use super::*;

    #[test]
    fn long_secrets_are_redacted_before_delivery() {
        let output = redact_text(
            "password=supersecretvalue token=1234",
            &["supersecretvalue".into(), "1234".into()],
        );
        assert!(output.contains("password=[REDACTED]"));
        assert!(output.contains("token=1234"));
    }
}
