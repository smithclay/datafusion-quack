//! Serving over TLS: shutdown waits for requests in flight.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement;
use datafusion_quack::{ClientError, QuackServer, QueryHook, QueryOutput, SessionInfo};
use quack_protocol::server::{
    HugeIntParts, MessageHeader, MessageType, QuackMessage, decode_request, encode_response,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

mod common;
use common::*;

/// Answers `SELECT 'slow'` after a second.
#[derive(Debug)]
struct Slow;

#[async_trait]
impl QueryHook for Slow {
    async fn handle(
        &self,
        statement: &Statement,
        _ctx: &SessionContext,
        _session: &SessionInfo,
    ) -> Option<Result<QueryOutput, ClientError>> {
        if !statement.to_string().contains("'slow'") {
            return None;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        Some(Ok(QueryOutput::Success))
    }
}

/// POSTs `body` to `/quack` on a kept-alive HTTP/1.1 connection; returns the body.
async fn post(stream: &mut TlsStream<tokio::net::TcpStream>, body: &[u8]) -> Vec<u8> {
    let head = format!(
        "POST /quack HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    // read the head, then exactly Content-Length bytes
    let mut response = Vec::new();
    let mut byte = [0u8; 1];
    while !response.ends_with(b"\r\n\r\n") {
        assert_eq!(
            stream.read(&mut byte).await.unwrap(),
            1,
            "connection closed"
        );
        response.push(byte[0]);
    }
    let head = String::from_utf8(response).unwrap();
    let length: usize = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .expect("a Content-Length");
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await.unwrap();
    body
}

#[tokio::test]
async fn shutdown_waits_for_a_request_in_flight() {
    let dir = std::env::temp_dir().join(format!("quack-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(
        QuackServer::new(Arc::new(SessionContext::new()))
            .with_options(options().with_tls(&cert, &key))
            .with_hooks(vec![Arc::new(Slow)])
            .serve_with_shutdown(listener, async {
                let _ = stopped.await;
            }),
    );

    let mut roots = rustls::RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut stream = TlsConnector::from(Arc::new(config))
        .connect("localhost".try_into().unwrap(), tcp)
        .await
        .unwrap();

    let connect = encode_response(&QuackMessage::ConnectionRequest {
        header: MessageHeader::new(MessageType::ConnectionRequest),
        auth_string: Some(TOKEN.into()),
        client_duckdb_version: None,
        client_platform: None,
        min_supported_quack_version: 3,
        max_supported_quack_version: 3,
        client_id: None,
        heartbeat_timeout_seconds: 60,
    })
    .unwrap();
    let id = connection_id(&post(&mut stream, &connect).await);
    let prepare = encode_response(&QuackMessage::PrepareRequest {
        header: MessageHeader::new(MessageType::PrepareRequest).with_connection(&id),
        sql: "SELECT 'slow'".into(),
        query_uuid: Some(HugeIntParts { upper: 0, lower: 1 }),
        inline_rows: None,
    })
    .unwrap();

    let in_flight = tokio::spawn(async move { post(&mut stream, &prepare).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !server.is_finished(),
        "the server waits for the request in flight"
    );

    let response = in_flight.await.unwrap();
    assert!(matches!(
        decode_request(&response).unwrap(),
        QuackMessage::PrepareResponse { .. }
    ));
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("the server stops once the connection is idle")
        .unwrap()
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
