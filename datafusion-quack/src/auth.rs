//! Authentication and authorization.

use std::fmt::Debug;

use async_trait::async_trait;
use subtle::ConstantTimeEq;

use crate::error::ClientError;

/// What a client sent in its CONNECTION request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionRequest {
    /// The token (`auth_string`).
    pub auth_string: String,
    /// The client's DuckDB version, e.g. `v2.0.0`.
    pub client_version: String,
    /// The client's platform, e.g. `osx_arm64` or `quack-rust`.
    pub client_platform: String,
}

/// A session, as hooks and the [`AuthProvider`] see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionInfo {
    /// The session's connection id.
    pub connection_id: String,
    /// The client's DuckDB version.
    pub client_version: String,
    /// The client's platform.
    pub client_platform: String,
}

impl SessionInfo {
    /// Whether the client is DuckDB itself, which reports its version. Other clients,
    /// such as the Rust `quack_protocol` client, leave it empty.
    pub fn is_duckdb_client(&self) -> bool {
        !self.client_version.is_empty()
    }
}

/// Decides who may connect, and what they may run.
#[async_trait]
pub trait AuthProvider: Send + Sync + Debug {
    /// Accepts or refuses a new session.
    async fn authenticate(&self, request: &ConnectionRequest) -> Result<(), ClientError>;

    /// Accepts or refuses one query of a session (DuckDB's
    /// `quack_authorization_function`). Accepts everything by default.
    async fn authorize(&self, session: &SessionInfo, sql: &str) -> Result<(), ClientError> {
        let _ = (session, sql);
        Ok(())
    }
}

/// The default [`AuthProvider`]: a fixed token, compared in constant time.
///
/// With no token, every client is accepted.
#[derive(Clone)]
pub struct TokenAuth {
    token: Option<String>,
}

impl Debug for TokenAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenAuth")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl TokenAuth {
    /// Requires `token`, or accepts everyone when it is `None`.
    pub fn new(token: Option<String>) -> Self {
        Self { token }
    }
}

#[async_trait]
impl AuthProvider for TokenAuth {
    async fn authenticate(&self, request: &ConnectionRequest) -> Result<(), ClientError> {
        let Some(token) = &self.token else {
            return Ok(());
        };
        if bool::from(token.as_bytes().ct_eq(request.auth_string.as_bytes())) {
            Ok(())
        } else {
            Err(ClientError::permission("Authentication failed"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(token: &str) -> ConnectionRequest {
        ConnectionRequest {
            auth_string: token.into(),
            client_version: String::new(),
            client_platform: String::new(),
        }
    }

    #[tokio::test]
    async fn tokens_must_match() {
        let auth = TokenAuth::new(Some("s3cret".into()));
        assert!(auth.authenticate(&request("s3cret")).await.is_ok());
        assert!(auth.authenticate(&request("s3cre")).await.is_err());
        assert!(auth.authenticate(&request("")).await.is_err());
        assert!(!format!("{auth:?}").contains("s3cret"));
    }

    #[tokio::test]
    async fn no_token_accepts_everyone() {
        let auth = TokenAuth::new(None);
        assert!(auth.authenticate(&request("anything")).await.is_ok());
    }
}
