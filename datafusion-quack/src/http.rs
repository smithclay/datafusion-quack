//! The HTTP endpoint: `GET /`, `OPTIONS /quack` and `POST /quack`.

use std::future::Future;
use std::io::BufReader;
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
use hyper_util::service::TowerToHyperService;
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
    let reaper = tokio::spawn(reap(Arc::clone(&dispatcher)));
    let tls = match dispatcher.options.tls() {
        Some((cert, key)) => Some(tls_acceptor(cert, key)?),
        None => None,
    };
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

async fn serve_tls(
    listener: TcpListener,
    app: Router,
    acceptor: TlsAcceptor,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServerError> {
    tokio::pin!(shutdown);
    loop {
        let (stream, peer) = tokio::select! {
            () = &mut shutdown => return Ok(()),
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
        tokio::spawn(async move {
            let stream = match acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::debug!(%peer, %error, "TLS handshake failed");
                    return;
                }
            };
            let service = TowerToHyperService::new(app);
            if let Err(error) = auto::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(%peer, %error, "connection ended with an error");
            }
        });
    }
}

fn tls_acceptor(cert: &std::path::Path, key: &std::path::Path) -> Result<TlsAcceptor, ServerError> {
    let tls_error = |what: &str, path: &std::path::Path, error: &dyn std::fmt::Display| {
        ServerError::Tls(format!("{what} {}: {error}", path.display()))
    };
    let certs = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(cert).map_err(|e| tls_error("cannot open", cert, &e))?,
    ))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| tls_error("cannot read", cert, &e))?;
    if certs.is_empty() {
        return Err(tls_error("no certificate in", cert, &"empty"));
    }
    let key = rustls_pemfile::private_key(&mut BufReader::new(
        std::fs::File::open(key).map_err(|e| tls_error("cannot open", key, &e))?,
    ))
    .map_err(|e| tls_error("cannot read", key, &e))?
    .ok_or_else(|| tls_error("no private key in", key, &"empty"))?;

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
