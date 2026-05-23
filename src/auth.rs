use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::storage::atomic_write;

pub const CLI_TOKEN_FORMAT_VERSION: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliTokenClaims {
    #[serde(default)]
    pub version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    pub github_login: String,
    pub github_id: u64,
    pub issued_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenIssueRequest {
    pub name: String,
    pub github_login: String,
    pub github_id: u64,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenRecord {
    pub token_id: String,
    pub name: String,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<u64>,
    pub github_login: String,
    pub source: String,
    pub token_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenIssueResult {
    pub plaintext_token: String,
    pub record: TokenRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    pub auth_source: AuthSource,
    pub github_login: Option<String>,
    pub github_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthSource {
    BootstrapToken,
    CliToken,
    LegacyCliToken,
}

#[derive(Debug)]
pub enum TokenStoreError {
    Io(std::io::Error),
    InvalidData(String),
    MissingCurrentSecret,
    TokenNotFound(String),
}

impl Display for TokenStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::InvalidData(err) => write!(f, "{err}"),
            Self::MissingCurrentSecret => {
                write!(f, "FORGE_CLI_TOKEN_SECRET_CURRENT is not configured")
            }
            Self::TokenNotFound(token_id) => write!(f, "token {token_id} not found"),
        }
    }
}

impl std::error::Error for TokenStoreError {}

impl From<std::io::Error> for TokenStoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone)]
pub struct CliTokenVerifier {
    current_secret: String,
    previous_secret: Option<String>,
    store: CliTokenStore,
}

impl CliTokenVerifier {
    pub fn from_env(root: impl AsRef<Path>) -> Result<Option<Self>, TokenStoreError> {
        let current_secret = current_cli_token_secret();
        let previous_secret = previous_cli_token_secret();
        let Some(current_secret) = current_secret else {
            if previous_secret.is_none() {
                return Ok(None);
            }
            return Err(TokenStoreError::MissingCurrentSecret);
        };
        Ok(Some(Self {
            current_secret,
            previous_secret,
            store: CliTokenStore::new(root)?,
        }))
    }

    #[cfg(test)]
    pub fn configured_for_tests(root: impl AsRef<Path>) -> Self {
        Self {
            current_secret: "test-cli-token-secret".into(),
            previous_secret: None,
            store: CliTokenStore::new(root).unwrap(),
        }
    }

    #[cfg(test)]
    pub fn configured_for_tests_with_previous(
        root: impl AsRef<Path>,
        current_secret: &str,
        previous_secret: Option<&str>,
    ) -> Self {
        Self {
            current_secret: current_secret.into(),
            previous_secret: previous_secret.map(str::to_string),
            store: CliTokenStore::new(root).unwrap(),
        }
    }

    pub fn issue_token(
        &self,
        request: TokenIssueRequest,
    ) -> Result<TokenIssueResult, TokenStoreError> {
        let token_id = generate_token_id();
        let claims = CliTokenClaims {
            version: CLI_TOKEN_FORMAT_VERSION,
            token_id: Some(token_id.clone()),
            github_login: request.github_login.clone(),
            github_id: request.github_id,
            issued_at_unix: unix_now(),
        };
        let plaintext_token = encode_cli_token(&claims, &self.current_secret)
            .map_err(TokenStoreError::InvalidData)?;
        let record = TokenRecord {
            token_id,
            name: request.name,
            created_at: unix_now(),
            last_used_at: None,
            revoked_at: None,
            github_login: request.github_login,
            source: request.source,
            token_hash: hash_token(&plaintext_token),
        };
        self.store.write_record(&record)?;
        Ok(TokenIssueResult {
            plaintext_token,
            record,
        })
    }

    pub fn verify_token(
        &self,
        token: &str,
    ) -> Result<Option<AuthenticatedPrincipal>, TokenStoreError> {
        let Some(claims) = self.decode_with_any_secret(token) else {
            return Ok(None);
        };

        if let Some(token_id) = claims.token_id.clone() {
            let Some(mut record) = self.store.read_record(&token_id)? else {
                return Ok(None);
            };
            if record.revoked_at.is_some() || record.token_hash != hash_token(token) {
                return Ok(None);
            }
            record.last_used_at = Some(unix_now());
            self.store.write_record(&record)?;
            return Ok(Some(AuthenticatedPrincipal {
                auth_source: AuthSource::CliToken,
                github_login: Some(record.github_login),
                github_id: Some(claims.github_id),
            }));
        }

        Ok(Some(AuthenticatedPrincipal {
            auth_source: AuthSource::LegacyCliToken,
            github_login: Some(claims.github_login),
            github_id: Some(claims.github_id),
        }))
    }

    pub fn list_tokens(&self) -> Result<Vec<TokenRecord>, TokenStoreError> {
        self.store.list_records()
    }

    pub fn revoke_token(&self, token_id: &str) -> Result<TokenRecord, TokenStoreError> {
        let Some(mut record) = self.store.read_record(token_id)? else {
            return Err(TokenStoreError::TokenNotFound(token_id.into()));
        };
        if record.revoked_at.is_none() {
            record.revoked_at = Some(unix_now());
            self.store.write_record(&record)?;
        }
        Ok(record)
    }

    fn decode_with_any_secret(&self, token: &str) -> Option<CliTokenClaims> {
        decode_cli_token(token, &self.current_secret).or_else(|| {
            self.previous_secret
                .as_ref()
                .and_then(|secret| decode_cli_token(token, secret))
        })
    }
}

#[derive(Debug, Clone)]
pub struct CliTokenStore {
    root: PathBuf,
}

impl CliTokenStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn read_record(&self, token_id: &str) -> Result<Option<TokenRecord>, TokenStoreError> {
        let path = self.path_for(token_id);
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw).map(Some).map_err(|err| {
            TokenStoreError::InvalidData(format!("invalid cli token metadata: {err}"))
        })
    }

    pub fn write_record(&self, record: &TokenRecord) -> Result<(), TokenStoreError> {
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|err| TokenStoreError::InvalidData(err.to_string()))?;
        atomic_write(self.path_for(&record.token_id), &bytes)
            .map_err(|err| TokenStoreError::Io(std::io::Error::other(err.to_string())))
    }

    pub fn list_records(&self) -> Result<Vec<TokenRecord>, TokenStoreError> {
        let mut records = Vec::new();
        if !self.root.exists() {
            return Ok(records);
        }
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let raw = fs::read_to_string(entry.path())?;
            let record: TokenRecord = serde_json::from_str(&raw).map_err(|err| {
                TokenStoreError::InvalidData(format!("invalid cli token metadata: {err}"))
            })?;
            records.push(record);
        }
        records.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        Ok(records)
    }

    fn path_for(&self, token_id: &str) -> PathBuf {
        self.root.join(format!("{token_id}.json"))
    }
}

pub fn current_cli_token_secret() -> Option<String> {
    std::env::var("FORGE_CLI_TOKEN_SECRET_CURRENT")
        .ok()
        .or_else(|| std::env::var("FORGE_CLI_TOKEN_SECRET").ok())
}

pub fn previous_cli_token_secret() -> Option<String> {
    std::env::var("FORGE_CLI_TOKEN_SECRET_PREVIOUS").ok()
}

pub fn encode_cli_token(claims: &CliTokenClaims, secret: &str) -> Result<String, String> {
    let payload = serde_json::to_vec(claims).map_err(|err| err.to_string())?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
    let signature = sign_value(secret, &payload)?;
    Ok(format!("forge_cli.{payload}.{signature}"))
}

pub fn decode_cli_token(token: &str, secret: &str) -> Option<CliTokenClaims> {
    let raw = token.strip_prefix("forge_cli.")?;
    let (payload, signature) = raw.rsplit_once('.')?;
    let expected = sign_value(secret, payload).ok()?;
    if !bool::from(expected.as_bytes().ct_eq(signature.as_bytes())) {
        return None;
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let mut claims: CliTokenClaims = serde_json::from_slice(&bytes).ok()?;
    if claims.version == 0 {
        claims.version = 1;
    }
    Some(claims)
}

fn sign_value(secret: &str, payload: &str) -> Result<String, String> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|err| err.to_string())?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn hash_token(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn generate_token_id() -> String {
    format!("tok-{}", hex::encode(rand::random::<[u8; 8]>()))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_root(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let base = std::env::temp_dir().join(format!(
            "forge-auth-tests-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    #[test]
    fn token_signed_with_previous_secret_still_verifies() {
        let root = test_root("previous-secret-verifies");
        let verifier = CliTokenVerifier::configured_for_tests_with_previous(
            &root,
            "current-secret",
            Some("previous-secret"),
        );
        let legacy = encode_cli_token(
            &CliTokenClaims {
                version: CLI_TOKEN_FORMAT_VERSION,
                token_id: None,
                github_login: "octocat".into(),
                github_id: 7,
                issued_at_unix: 1,
            },
            "previous-secret",
        )
        .unwrap();

        let principal = verifier.verify_token(&legacy).unwrap().unwrap();
        assert_eq!(principal.github_login.as_deref(), Some("octocat"));
    }

    #[test]
    fn new_token_uses_current_secret() {
        let root = test_root("new-token-current-secret");
        let verifier = CliTokenVerifier::configured_for_tests_with_previous(
            &root,
            "current-secret",
            Some("previous-secret"),
        );
        let issued = verifier
            .issue_token(TokenIssueRequest {
                name: "laptop".into(),
                github_login: "octocat".into(),
                github_id: 7,
                source: "token_create".into(),
            })
            .unwrap();

        assert!(decode_cli_token(&issued.plaintext_token, "current-secret").is_some());
        assert!(decode_cli_token(&issued.plaintext_token, "previous-secret").is_none());
    }

    #[test]
    fn removing_previous_secret_invalidates_old_tokens() {
        let root = test_root("remove-previous-secret");
        let legacy = encode_cli_token(
            &CliTokenClaims {
                version: 1,
                token_id: None,
                github_login: "octocat".into(),
                github_id: 7,
                issued_at_unix: 1,
            },
            "previous-secret",
        )
        .unwrap();

        let verifier =
            CliTokenVerifier::configured_for_tests_with_previous(&root, "current-secret", None);
        assert!(verifier.verify_token(&legacy).unwrap().is_none());
    }
}
