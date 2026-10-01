//! Serve a DataFusion [`SessionContext`] over DuckDB's [Quack protocol].
//!
//! Quack is DuckDB's client–server protocol: DuckDB message frames over HTTP
//! `POST /quack`, with results as DuckDB `DataChunk`s. With this crate, DuckDB
//! (`ATTACH 'quack:host:port'`), the Rust `quack_protocol` client, and the DataFusion
//! Quack table provider can all query DataFusion.
//!
//! ```no_run
//! use std::sync::Arc;
//! use datafusion::prelude::SessionContext;
//! use datafusion_quack::{ServerOptions, serve};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = SessionContext::new();
//! ctx.sql("CREATE TABLE t AS VALUES (1, 'one'), (2, 'two')").await?;
//! serve(Arc::new(ctx), &ServerOptions::new().with_token("s3cret-token")).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Then, in DuckDB:
//!
//! ```sql
//! CREATE SECRET (TYPE quack, TOKEN 's3cret-token');
//! ATTACH 'quack:localhost:9494' AS df;
//! FROM df.public.t;
//! ```
//!
//! Each client session gets its own `SessionContext`, made by a
//! [`SessionContextProvider`]. The default shares the base context's catalogs and adds
//! DuckDB compatibility: DuckDB's SQL dialect, the `duckdb_tables()` family of catalog
//! functions, `information_schema` with DuckDB type names, and case-insensitive table
//! names (see [`datafusion_quack_catalog`]).
//!
//! [Quack protocol]: https://duckdb.org/docs/current/quack/overview

mod auth;
mod cursor;
mod dispatch;
mod error;
mod hooks;
mod http;
mod options;
mod session;

use std::future::Future;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use tokio::net::TcpListener;

pub use auth::{AuthProvider, ConnectionRequest, SessionInfo, TokenAuth};
pub use datafusion_quack_catalog;
pub use error::{ClientError, ExceptionType, ServerError};
pub use hooks::{QueryHook, QueryOutput, TransactionHook};
pub use options::{DEFAULT_PORT, ServerOptions};
pub use session::{SessionContextProvider, SharedSessionContextProvider};

use dispatch::Dispatcher;
use session::SessionStore;

/// Serves `ctx` until the process stops.
pub async fn serve(ctx: Arc<SessionContext>, options: &ServerOptions) -> Result<(), ServerError> {
    QuackServer::new(ctx).with_options(options.clone()).serve().await
}

/// Serves `ctx`, answering statements with `hooks` before DataFusion.
pub async fn serve_with_hooks(
    ctx: Arc<SessionContext>,
    options: &ServerOptions,
    hooks: Vec<Arc<dyn QueryHook>>,
) -> Result<(), ServerError> {
    QuackServer::new(ctx)
        .with_options(options.clone())
        .with_hooks(hooks)
        .serve()
        .await
}

/// Serves `ctx` on a listener the caller bound, e.g. to port 0 in a test.
pub async fn serve_with_listener(
    listener: TcpListener,
    ctx: Arc<SessionContext>,
    options: &ServerOptions,
    hooks: Vec<Arc<dyn QueryHook>>,
) -> Result<(), ServerError> {
    QuackServer::new(ctx)
        .with_options(options.clone())
        .with_hooks(hooks)
        .serve_with_listener(listener)
        .await
}

/// A Quack server, configured piece by piece.
///
/// ```no_run
/// use std::sync::Arc;
/// use datafusion::prelude::SessionContext;
/// use datafusion_quack::{QuackServer, ServerOptions, TokenAuth};
///
/// # async fn run() -> Result<(), datafusion_quack::ServerError> {
/// let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
/// QuackServer::new(Arc::new(SessionContext::new()))
///     .with_auth_provider(Arc::new(TokenAuth::new(Some("s3cret-token".into()))))
///     .serve_with_shutdown(listener, async { /* wait for a signal */ })
///     .await
/// # }
/// ```
#[derive(Debug)]
pub struct QuackServer {
    options: ServerOptions,
    provider: Arc<dyn SessionContextProvider>,
    auth: Option<Arc<dyn AuthProvider>>,
    hooks: Vec<Arc<dyn QueryHook>>,
}

impl QuackServer {
    /// A server whose sessions share `ctx`'s catalogs (see
    /// [`SharedSessionContextProvider`]).
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self::with_session_context_provider(Arc::new(SharedSessionContextProvider::new(ctx)))
    }

    /// A server whose sessions get their contexts from `provider`.
    pub fn with_session_context_provider(provider: Arc<dyn SessionContextProvider>) -> Self {
        Self {
            options: ServerOptions::default(),
            provider,
            auth: None,
            hooks: Vec::new(),
        }
    }

    /// Replaces the options.
    pub fn with_options(mut self, options: ServerOptions) -> Self {
        self.options = options;
        self
    }

    /// Decides who may connect and what they may run. The default is a [`TokenAuth`]
    /// with the options' token.
    pub fn with_auth_provider(mut self, auth: Arc<dyn AuthProvider>) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Adds hooks, which run after the built-in [`TransactionHook`].
    pub fn with_hooks(mut self, hooks: Vec<Arc<dyn QueryHook>>) -> Self {
        self.hooks.extend(hooks);
        self
    }

    fn dispatcher(self) -> Arc<Dispatcher> {
        let auth = self
            .auth
            .unwrap_or_else(|| Arc::new(TokenAuth::new(self.options.token().map(String::from))));
        if self.options.token().is_none() {
            tracing::warn!("no token is set: every client may connect");
        }
        let mut hooks: Vec<Arc<dyn QueryHook>> = vec![Arc::new(TransactionHook)];
        hooks.extend(self.hooks);
        Arc::new(Dispatcher {
            sessions: SessionStore::new(self.options.max_sessions()),
            options: self.options,
            auth,
            provider: self.provider,
            hooks,
        })
    }

    /// Binds the options' host and port, and serves until the process stops.
    pub async fn serve(self) -> Result<(), ServerError> {
        let listener =
            TcpListener::bind((self.options.host().to_string(), self.options.port())).await?;
        self.serve_with_listener(listener).await
    }

    /// Serves on `listener` until the process stops.
    pub async fn serve_with_listener(self, listener: TcpListener) -> Result<(), ServerError> {
        self.serve_with_shutdown(listener, std::future::pending()).await
    }

    /// Serves on `listener` until `shutdown` resolves.
    pub async fn serve_with_shutdown(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ServerError> {
        if let Ok(address) = listener.local_addr() {
            tracing::info!(%address, "serving the Quack protocol");
        }
        http::run(listener, self.dispatcher(), shutdown).await
    }
}
