//! The HTTP endpoint: `GET /`, `OPTIONS /quack` and `POST /quack`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::dispatch::{Dispatcher, error_response};
use crate::error::{ClientError, ServerError};

/// The body `GET /` returns, as DuckDB's server does.
pub(crate) const BANNER: &str =
    "This is a DuckDB Quack RPC endpoint. Use ATTACH 'quack:...' to connect here.\n";

const CONTENT_TYPE_DUCKDB: &str = "application/vnd.duckdb";

pub(crate) fn router(dispatcher: Arc<Dispatcher>) -> Router {
    Router::new()
        .route("/", get(|| async { BANNER }))
        .route(
            "/quack",
            axum::routing::post(post_quack).options(options_quack),
        )
        .with_state(dispatcher)
}

async fn options_quack() -> Response {
    (
        StatusCode::NO_CONTENT,
        [
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
            (header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS"),
            (header::ACCESS_CONTROL_ALLOW_HEADERS, "*"),
        ],
    )
        .into_response()
}

async fn post_quack(State(dispatcher): State<Arc<Dispatcher>>, body: Body) -> Response {
    let limit = dispatcher.options.max_request_bytes();
    let response = match axum::body::to_bytes(body, limit).await {
        Ok(body) => dispatcher.handle(&body).await,
        Err(_) => error_response(&ClientError::invalid_input(format!(
            "Request body is larger than the server's limit of {limit} bytes"
        ))),
    };
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, CONTENT_TYPE_DUCKDB),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        response,
    )
        .into_response()
}

/// Serves `router` on `listener` until `shutdown` resolves, over TLS when the options
/// name a certificate. A task ends expired sessions and stale results meanwhile.
pub(crate) async fn run(
    listener: TcpListener,
    dispatcher: Arc<Dispatcher>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServerError> {
    let tls = match dispatcher.options.tls() {
        Some((cert, key)) => Some(tls_acceptor(cert, key)?),
        None => None,
    };
    // after anything that can fail, so an early return can't leave it running
    let reaper = tokio::spawn(reap(Arc::clone(&dispatcher)));
    let app = router(Arc::clone(&dispatcher));
    let result = match tls {
        None => axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(ServerError::from),
        Some(acceptor) => serve_tls(listener, app, acceptor, shutdown).await,
    };
    reaper.abort();
    result
}

async fn reap(dispatcher: Arc<Dispatcher>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        dispatcher.sessions.sweep(dispatcher.options.result_ttl());
    }
}

/// How long a client may take over the TLS handshake before its socket is closed.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Serves HTTPS until `shutdown` resolves, then stops accepting and waits for open
/// connections to finish their requests, as the plain-HTTP path does.
async fn serve_tls(
    listener: TcpListener,
    app: Router,
    acceptor: TlsAcceptor,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServerError> {
    let graceful = GracefulShutdown::new();
    let builder = auto::Builder::new(TokioExecutor::new());
    tokio::pin!(shutdown);
    loop {
        let (stream, peer) = tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::warn!(%error, "accept failed");
                    continue;
                }
            },
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        let builder = builder.clone();
        // taken now, so shutdown also waits for connections still in the handshake
        let watcher = graceful.watcher();
        tokio::spawn(async move {
            let stream =
                match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(error)) => {
                        tracing::debug!(%peer, %error, "TLS handshake failed");
                        return;
                    }
                    Err(_) => {
                        tracing::debug!(%peer, "TLS handshake timed out");
                        return;
                    }
                };
            let connection = builder
                .serve_connection(TokioIo::new(stream), TowerToHyperService::new(app))
                .into_owned();
            if let Err(error) = watcher.watch(connection).await {
                tracing::debug!(%peer, %error, "connection ended with an error");
            }
        });
    }
    drop(listener);
    graceful.shutdown().await;
    Ok(())
}

fn tls_acceptor(cert: &std::path::Path, key: &std::path::Path) -> Result<TlsAcceptor, ServerError> {
    let tls_error = |what: &str, path: &std::path::Path, error: &dyn std::fmt::Display| {
        ServerError::Tls(format!("{what} {}: {error}", path.display()))
    };
    let certs = CertificateDer::pem_file_iter(cert)
        .map_err(|e| tls_error("cannot read", cert, &e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| tls_error("cannot read", cert, &e))?;
    if certs.is_empty() {
        return Err(tls_error("no certificate in", cert, &"empty"));
    }
    let key = PrivateKeyDer::from_pem_file(key).map_err(|e| tls_error("cannot read", key, &e))?;

    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| ServerError::Tls(e.to_string()))?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|e| ServerError::Tls(e.to_string()))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}
