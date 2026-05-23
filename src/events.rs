use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    let mut result = redact_sensitive_headers(input);
    result = redact_sensitive_assignments(&result);
    for secret in secrets {
        if secret.len() >= 8 {
            result = result.replace(secret, "[REDACTED]");
        }
    }
    result = redact_bearer_tokens(&result);
    result
}

fn redact_sensitive_headers(input: &str) -> String {
    input
        .lines()
        .map(|line| {
            if let Some((name, _)) = line.split_once(':') {
                if name.trim().eq_ignore_ascii_case("authorization") {
                    return format!("{name}: [REDACTED]");
                }
            }
            line.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn redact_sensitive_assignments(input: &str) -> String {
    input
        .lines()
        .map(|line| {
            let mut rendered = line
                .split_whitespace()
                .map(|part| {
                    if let Some((key, value)) = part.split_once('=') {
                        if is_sensitive_key(key.trim()) && value.trim().len() >= 8 {
                            return format!("{key}=[REDACTED]");
                        }
                    }
                    part.to_string()
                })
                .collect::<Vec<_>>()
                .join(" ");
            if rendered != line {
                return rendered;
            }
            if let Some((key, value)) = line.split_once(':') {
                let trimmed = key.trim().trim_matches('"');
                if is_sensitive_key(trimmed) && value.trim().trim_matches('"').len() >= 8 {
                    return format!("{key}: [REDACTED]");
                }
            }
            rendered = line.to_string();
            rendered
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn redact_bearer_tokens(input: &str) -> String {
    input
        .lines()
        .map(redact_bearer_token_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn redact_bearer_token_line(line: &str) -> String {
    let mut parts = line.split_whitespace().peekable();
    let mut rendered = Vec::new();
    while let Some(part) = parts.next() {
        rendered.push(part.to_string());
        if part.eq_ignore_ascii_case("bearer") && parts.peek().is_some() {
            rendered.push("[REDACTED]".into());
            let _ = parts.next();
        }
    }
    rendered.join(" ")
}

fn is_sensitive_key(key: &str) -> bool {
    let uppercase = key.to_ascii_uppercase();
    [
        "FORGE_MASTER_KEY",
        "FORGE_CLI_TOKEN_SECRET",
        "FORGE_GITHUB_OAUTH_CLIENT_SECRET",
        "BEARER_TOKEN",
        "AUTHORIZATION",
        "GITHUB_TOKEN",
        "SECRET",
        "TOKEN",
        "PASSWORD",
        "SESSION",
        "OAUTH",
    ]
    .iter()
    .any(|needle| uppercase.contains(needle))
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

    #[test]
    fn auth_logs_redact_authorization_header() {
        let output = redact_text("Authorization: Bearer secret-token-value", &[]);
        assert_eq!(output, "Authorization: [REDACTED]");
    }

    #[test]
    fn sensitive_environment_assignments_are_redacted() {
        let output = redact_text(
            concat!(
                "FORGE_MASTER_KEY=abc123456789\n",
                "FORGE_CLI_TOKEN_SECRET=def123456789\n",
                "FORGE_GITHUB_OAUTH_CLIENT_SECRET=ghi123456789\n",
                "bearer_token=jkl123456789\n",
                "APP_SECRET=mno123456789\n",
                "GITHUB_TOKEN=pqr123456789\n"
            ),
            &[],
        );
        assert!(!output.contains("abc123456789"));
        assert!(!output.contains("def123456789"));
        assert!(!output.contains("ghi123456789"));
        assert!(!output.contains("jkl123456789"));
        assert!(!output.contains("mno123456789"));
        assert!(!output.contains("pqr123456789"));
    }
}
