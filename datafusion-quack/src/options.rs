//! Server configuration.

use std::path::PathBuf;
use std::time::Duration;

/// The port DuckDB's `quack_serve` listens on by default.
pub const DEFAULT_PORT: u16 = 9494;

/// Settings for [`serve`](crate::serve) and [`QuackServer`](crate::QuackServer).
///
/// Every limit has a safe default (listed on each method); `0` turns a limit off
/// where a method says so.
///
/// ```
/// use std::time::Duration;
/// use datafusion_quack::ServerOptions;
///
/// let options = ServerOptions::new()
///     .with_port(9494)
///     .with_token("s3cret-token")
///     .with_max_sessions(64)
///     .with_result_ttl(Duration::from_secs(60));
/// assert_eq!(options.port(), 9494);
/// ```
#[derive(Clone, Debug)]
pub struct ServerOptions {
    host: String,
    port: u16,
    token: Option<String>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    max_sessions: usize,
    heartbeat_max: Duration,
    result_ttl: Duration,
    inline_rows: u64,
    max_request_bytes: usize,
    max_inflight_batches: u64,
    batch_target_bytes: usize,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: DEFAULT_PORT,
            token: None,
            tls_cert: None,
            tls_key: None,
            max_sessions: 1024,
            heartbeat_max: Duration::from_secs(300),
            result_ttl: Duration::from_secs(300),
            inline_rows: 24_576,
            max_request_bytes: 64 * 1024 * 1024,
            max_inflight_batches: 64,
            batch_target_bytes: 1024 * 1024,
        }
    }
}

impl ServerOptions {
    /// The defaults: `127.0.0.1:9494`, no token, no TLS.
    pub fn new() -> Self {
        Self::default()
    }

    /// The address to listen on. Default `127.0.0.1`.
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// The port to listen on. Default [`DEFAULT_PORT`] (9494); `0` picks a free port.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// The token clients must present (DuckDB: `CREATE SECRET (TYPE quack, TOKEN '…')`).
    ///
    /// Without a token, and without a custom [`AuthProvider`](crate::AuthProvider), any
    /// client may connect.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Serve HTTPS with this PEM certificate chain and private key.
    pub fn with_tls(mut self, cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        self.tls_cert = Some(cert.into());
        self.tls_key = Some(key.into());
        self
    }

    /// The most sessions open at once; further connection requests are refused. Default 1024;
    /// `0` is unlimited.
    pub fn with_max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = max_sessions;
        self
    }

    /// The longest heartbeat timeout a client may ask for; longer requests are lowered to
    /// it. Default 300 s.
    pub fn with_heartbeat_max(mut self, heartbeat_max: Duration) -> Self {
        self.heartbeat_max = heartbeat_max;
        self
    }

    /// How long an unread result stays open after its last FETCH. Default 300 s; `0`
    /// keeps results until their session ends.
    pub fn with_result_ttl(mut self, result_ttl: Duration) -> Self {
        self.result_ttl = result_ttl;
        self
    }

    /// How many rows a PREPARE response carries before the client must FETCH, unless the
    /// client asks for a number. Default 24576, as DuckDB.
    pub fn with_inline_rows(mut self, inline_rows: u64) -> Self {
        self.inline_rows = inline_rows;
        self
    }

    /// The largest request body accepted. Default 64 MiB.
    pub fn with_max_request_bytes(mut self, max_request_bytes: usize) -> Self {
        self.max_request_bytes = max_request_bytes;
        self
    }

    /// How far ahead of its last acknowledged batch a client may FETCH. Results are held
    /// until acknowledged, so this bounds a session's result memory to about
    /// `max_inflight_batches × batch_target_bytes`. Default 64.
    ///
    /// Held batches also count against the session's DataFusion memory pool, so a
    /// memory limit on the `SessionContext`'s runtime bounds them across all sessions.
    pub fn with_max_inflight_batches(mut self, max_inflight_batches: u64) -> Self {
        self.max_inflight_batches = max_inflight_batches.max(1);
        self
    }

    /// The size a FETCH batch is filled to before it is sent. Default 1 MiB.
    pub fn with_batch_target_bytes(mut self, batch_target_bytes: usize) -> Self {
        self.batch_target_bytes = batch_target_bytes.max(1);
        self
    }

    /// See [`with_host`](Self::with_host).
    pub fn host(&self) -> &str {
        &self.host
    }

    /// See [`with_port`](Self::with_port).
    pub fn port(&self) -> u16 {
        self.port
    }

    /// See [`with_token`](Self::with_token).
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// See [`with_tls`](Self::with_tls).
    pub fn tls(&self) -> Option<(&PathBuf, &PathBuf)> {
        self.tls_cert.as_ref().zip(self.tls_key.as_ref())
    }

    /// See [`with_max_sessions`](Self::with_max_sessions).
    pub fn max_sessions(&self) -> usize {
        self.max_sessions
    }

    /// See [`with_heartbeat_max`](Self::with_heartbeat_max).
    pub fn heartbeat_max(&self) -> Duration {
        self.heartbeat_max
    }

    /// See [`with_result_ttl`](Self::with_result_ttl).
    pub fn result_ttl(&self) -> Duration {
        self.result_ttl
    }

    /// See [`with_inline_rows`](Self::with_inline_rows).
    pub fn inline_rows(&self) -> u64 {
        self.inline_rows
    }

    /// See [`with_max_request_bytes`](Self::with_max_request_bytes).
    pub fn max_request_bytes(&self) -> usize {
        self.max_request_bytes
    }

    /// See [`with_max_inflight_batches`](Self::with_max_inflight_batches).
    pub fn max_inflight_batches(&self) -> u64 {
        self.max_inflight_batches
    }

    /// See [`with_batch_target_bytes`](Self::with_batch_target_bytes).
    pub fn batch_target_bytes(&self) -> usize {
        self.batch_target_bytes
    }
}
