//! Hostile input (gate G4): no request body crashes the server or goes unanswered.
//!
//! Every malformed body gets HTTP 200 with a well-formed ERROR_RESPONSE, and the
//! server keeps serving. The proptest mutates real requests: bit flips, cuts and
//! splices of the golden fixtures.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use proptest::prelude::*;
use quack_protocol::server::{
    BinaryWriter, HugeIntParts, MessageHeader, MessageType, QuackMessage, decode_request,
    encode_header, encode_response,
};

mod common;
use common::*;

async fn post(server: &TestServer, body: Vec<u8>) -> QuackMessage {
    let bytes = post_bytes(&server.url, body).await;
    let message = decode_request(&bytes).expect("a well-formed response");
    // the dispatcher turns a panic into this error; any panic is a bug
    if let QuackMessage::ErrorResponse { message, .. } = &message {
        assert_ne!(
            message, "the server failed while handling the request",
            "a panic"
        );
    }
    message
}

fn error_message(message: QuackMessage) -> String {
    match message {
        QuackMessage::ErrorResponse { message, .. } => message,
        other => panic!("expected ERROR_RESPONSE, got {other:?}"),
    }
}

fn connect() -> Vec<u8> {
    encode_response(&QuackMessage::ConnectionRequest {
        header: MessageHeader::new(MessageType::ConnectionRequest),
        auth_string: Some(TOKEN.into()),
        client_duckdb_version: None,
        client_platform: None,
        min_supported_quack_version: 3,
        max_supported_quack_version: 3,
        client_id: None,
        heartbeat_timeout_seconds: 60,
    })
    .unwrap()
}

fn prepare(connection_id: &str, sql: &str) -> Vec<u8> {
    encode_response(&QuackMessage::PrepareRequest {
        header: MessageHeader::new(MessageType::PrepareRequest).with_connection(connection_id),
        sql: sql.into(),
        query_uuid: Some(HugeIntParts { upper: 0, lower: 1 }),
        inline_rows: Some(0),
    })
    .unwrap()
}

async fn open_session(server: &TestServer) -> String {
    connection_id(&post_bytes(&server.url, connect()).await)
}

/// A header for `message_type` on `connection_id`, then `body` as raw bytes.
fn raw(message_type: MessageType, connection_id: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut header = MessageHeader::new(message_type);
    header.connection_id = connection_id.map(String::from);
    let mut writer = BinaryWriter::new();
    encode_header(&mut writer, &header).unwrap();
    writer.write_bytes(body).unwrap();
    writer.into_bytes()
}

#[tokio::test]
async fn malformed_bodies_get_error_responses() {
    let server = TestServer::start(SessionContext::new(), options()).await;
    let id = open_session(&server).await;

    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("empty body", vec![]),
        ("one byte", vec![0x01]),
        ("truncated ULEB", vec![0x01, 0x00, 0xff, 0xff, 0xff, 0xff]),
        ("ULEB too long", [vec![0x01, 0x00], vec![0x80; 32]].concat()),
        ("unknown message type", vec![0x01, 0x00, 0x63, 0xff, 0xff]),
        (
            "huge string length",
            raw(MessageType::PrepareRequest, Some(&id), &{
                let mut w = BinaryWriter::new();
                w.write_field_id(1).unwrap();
                w.write_uleb(u64::MAX).unwrap();
                w.into_bytes()
            }),
        ),
        (
            "unknown field id",
            raw(
                MessageType::PrepareRequest,
                Some(&id),
                &[0x4d, 0x00, 0x00, 0xff, 0xff],
            ),
        ),
        (
            "missing required field",
            raw(MessageType::FetchRequest, Some(&id), &[0xff, 0xff]),
        ),
        (
            "invalid UTF-8 in SQL",
            raw(MessageType::PrepareRequest, Some(&id), &{
                let mut w = BinaryWriter::new();
                w.write_field(1, |w| w.write_string_bytes(&[0xff, 0xfe]))
                    .unwrap();
                w.write_field(2, |w| {
                    w.write_huge_int_parts(HugeIntParts { upper: 0, lower: 1 })
                })
                .unwrap();
                w.write_field_id(0xffff).unwrap();
                w.into_bytes()
            }),
        ),
        (
            "trailing bytes",
            [prepare(&id, "SELECT 1"), vec![0xde, 0xad]].concat(),
        ),
        ("wrong connection id", prepare("not-a-session", "SELECT 1")),
        (
            "no connection id",
            raw(MessageType::HeartbeatRequest, None, &[0xff, 0xff]),
        ),
        (
            "server-to-client message",
            raw(MessageType::FetchResponse, Some(&id), &[0xff, 0xff]),
        ),
        (
            "SEND_DATA is not supported",
            raw(MessageType::SendDataRequest, Some(&id), &[0xff, 0xff]),
        ),
    ];
    for (name, body) in cases {
        let message = error_message(post(&server, body).await);
        assert!(!message.is_empty(), "{name}");
    }
    // the session still works
    assert!(matches!(
        post(&server, prepare(&id, "SELECT 1")).await,
        QuackMessage::PrepareResponse { .. }
    ));
}

#[tokio::test]
async fn oversized_bodies_are_refused() {
    let server = TestServer::start(
        SessionContext::new(),
        options().with_max_request_bytes(1024),
    )
    .await;
    let id = open_session(&server).await;
    let sql = format!("SELECT '{}'", "x".repeat(4096));
    let message = error_message(post(&server, prepare(&id, &sql)).await);
    assert!(message.contains("larger than"), "{message}");
}

#[tokio::test]
async fn fetching_far_past_the_ack_is_refused() {
    let server = TestServer::start(
        SessionContext::new(),
        options()
            .with_max_inflight_batches(4)
            .with_batch_target_bytes(1),
    )
    .await;
    let id = open_session(&server).await;
    let QuackMessage::PrepareResponse {
        needs_more_fetch, ..
    } = post(&server, prepare(&id, "SELECT * FROM range(100000)")).await
    else {
        panic!("prepare failed");
    };
    assert!(needs_more_fetch);
    let fetch = |batch_index: u64| {
        encode_response(&QuackMessage::FetchRequest {
            header: MessageHeader::new(MessageType::FetchRequest).with_connection(&id),
            result_uuid: HugeIntParts { upper: 0, lower: 1 },
            batch_index: Some(batch_index),
            ack_index: Some(0),
        })
        .unwrap()
    };
    let message = error_message(post(&server, fetch(1_000_000)).await);
    assert!(message.contains("past the last acknowledged"), "{message}");
    // a FETCH for the wrong result
    let wrong = encode_response(&QuackMessage::FetchRequest {
        header: MessageHeader::new(MessageType::FetchRequest).with_connection(&id),
        result_uuid: HugeIntParts { upper: 9, lower: 9 },
        batch_index: Some(1),
        ack_index: Some(0),
    })
    .unwrap();
    assert_eq!(
        error_message(post(&server, wrong).await),
        "Result has been closed"
    );
    assert!(matches!(
        post(&server, fetch(1)).await,
        QuackMessage::FetchResponse { .. }
    ));
}

#[tokio::test]
async fn cancel_stops_a_result_and_fetch_reports_it() {
    let server =
        TestServer::start(SessionContext::new(), options().with_batch_target_bytes(1)).await;
    let id = open_session(&server).await;
    post(&server, prepare(&id, "SELECT * FROM range(100000)")).await;
    let cancel = encode_response(&QuackMessage::CancelRequest {
        header: MessageHeader::new(MessageType::CancelRequest).with_connection(&id),
        query_uuid: HugeIntParts { upper: 0, lower: 0 },
    })
    .unwrap();
    assert!(matches!(
        post(&server, cancel).await,
        QuackMessage::SuccessResponse { .. }
    ));
    let fetch = encode_response(&QuackMessage::FetchRequest {
        header: MessageHeader::new(MessageType::FetchRequest).with_connection(&id),
        result_uuid: HugeIntParts { upper: 0, lower: 1 },
        batch_index: Some(1),
        ack_index: Some(0),
    })
    .unwrap();
    let message = error_message(post(&server, fetch).await);
    assert!(message.contains("Interrupted"), "{message}");
}

fn golden_requests() -> Vec<Vec<u8>> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../testdata/wire/golden");
    let mut requests: Vec<Vec<u8>> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "req"))
        .map(|path| std::fs::read(path).unwrap())
        .collect();
    requests.sort();
    requests
}

#[derive(Debug, Clone)]
enum Mutation {
    Flip(usize, u8),
    Truncate(usize),
    Insert(usize, Vec<u8>),
    Splice(usize, usize),
}

fn mutate(mut body: Vec<u8>, mutations: &[Mutation], other: &[u8]) -> Vec<u8> {
    for mutation in mutations {
        let len = body.len().max(1);
        match mutation {
            Mutation::Flip(at, bits) => {
                if let Some(byte) = body.get_mut(at % len) {
                    *byte ^= bits;
                }
            }
            Mutation::Truncate(at) => body.truncate(at % len),
            Mutation::Insert(at, bytes) => {
                let at = (at % len).min(body.len());
                body.splice(at..at, bytes.iter().copied());
            }
            Mutation::Splice(at, from) => {
                let at = (at % len).min(body.len());
                let from = from % other.len().max(1);
                body.truncate(at);
                body.extend_from_slice(other.get(from..).unwrap_or_default());
            }
        }
    }
    body
}

fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        (any::<usize>(), 1u8..).prop_map(|(at, bits)| Mutation::Flip(at, bits)),
        any::<usize>().prop_map(Mutation::Truncate),
        (any::<usize>(), prop::collection::vec(any::<u8>(), 1..12))
            .prop_map(|(at, bytes)| Mutation::Insert(at, bytes)),
        (any::<usize>(), any::<usize>()).prop_map(|(at, from)| Mutation::Splice(at, from)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// The per-PR fuzz smoke run: mutated real requests (`cargo fuzz` goes further).
    #[test]
    fn mutated_requests_get_well_formed_responses(
        base in 0usize..64,
        splice_from in 0usize..64,
        mutations in prop::collection::vec(mutation(), 1..4),
    ) {
        static RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> =
            std::sync::LazyLock::new(|| tokio::runtime::Runtime::new().unwrap());
        static SERVER: tokio::sync::OnceCell<TestServer> = tokio::sync::OnceCell::const_new();
        RUNTIME.block_on(async {
            let server = SERVER
                .get_or_init(|| TestServer::start(SessionContext::new(), options()))
                .await;
            let requests = golden_requests();
            let body = mutate(
                requests[base % requests.len()].clone(),
                &mutations,
                &requests[splice_from % requests.len()],
            );
            // post() checks for HTTP 200 and a decodable response
            post(server, body).await;
        });
    }
}

#[tokio::test]
async fn banner_and_cors_preflight() {
    let server = TestServer::start(SessionContext::new(), options()).await;
    let base = server.url.trim_end_matches("/quack").to_string();
    let banner = reqwest::get(&base).await.unwrap();
    assert_eq!(banner.status(), 200);
    assert!(
        banner
            .text()
            .await
            .unwrap()
            .contains("DuckDB Quack RPC endpoint")
    );

    let preflight = reqwest::Client::new()
        .request(reqwest::Method::OPTIONS, &server.url)
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), 204);
    assert_eq!(preflight.headers()["access-control-allow-origin"], "*");
}

#[tokio::test]
async fn sessions_without_heartbeats_expire() {
    let server = TestServer::start(SessionContext::new(), options()).await;
    let connect = encode_response(&QuackMessage::ConnectionRequest {
        header: MessageHeader::new(MessageType::ConnectionRequest),
        auth_string: Some(TOKEN.into()),
        client_duckdb_version: None,
        client_platform: None,
        min_supported_quack_version: 3,
        max_supported_quack_version: 3,
        client_id: None,
        heartbeat_timeout_seconds: 1,
    })
    .unwrap();
    let QuackMessage::ConnectionResponse { header, .. } = post(&server, connect).await else {
        panic!("not connected");
    };
    let id = header.connection_id.unwrap();
    // no heartbeat for longer than the lease: the session is gone
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    let message = error_message(post(&server, prepare(&id, "SELECT 1")).await);
    assert!(
        message == "Invalid connection id" || message == "Connection heartbeat lease expired",
        "{message}"
    );
}

fn prepare_with(connection_id: &str, uuid: u64, sql: &str, inline_rows: u64) -> Vec<u8> {
    encode_response(&QuackMessage::PrepareRequest {
        header: MessageHeader::new(MessageType::PrepareRequest).with_connection(connection_id),
        sql: sql.into(),
        query_uuid: Some(HugeIntParts {
            upper: 0,
            lower: uuid,
        }),
        inline_rows: Some(inline_rows),
    })
    .unwrap()
}

fn cancel_request(connection_id: &str, uuid: u64) -> Vec<u8> {
    encode_response(&QuackMessage::CancelRequest {
        header: MessageHeader::new(MessageType::CancelRequest).with_connection(connection_id),
        query_uuid: HugeIntParts {
            upper: 0,
            lower: uuid,
        },
    })
    .unwrap()
}

fn fetch_request(connection_id: &str, uuid: u64, batch_index: u64) -> Vec<u8> {
    encode_response(&QuackMessage::FetchRequest {
        header: MessageHeader::new(MessageType::FetchRequest).with_connection(connection_id),
        result_uuid: HugeIntParts {
            upper: 0,
            lower: uuid,
        },
        batch_index: Some(batch_index),
        ack_index: Some(batch_index - 1),
    })
    .unwrap()
}

#[tokio::test]
async fn a_stale_cancel_leaves_the_running_query_alone() {
    let server =
        TestServer::start(SessionContext::new(), options().with_batch_target_bytes(1)).await;
    let id = open_session(&server).await;
    post(
        &server,
        prepare_with(&id, 1, "SELECT * FROM range(100000)", 0),
    )
    .await;
    post(
        &server,
        prepare_with(&id, 2, "SELECT * FROM range(100000)", 0),
    )
    .await;
    // a late CANCEL of query 1 must not stop query 2
    let message = error_message(post(&server, cancel_request(&id, 1)).await);
    assert!(message.contains("different query"), "{message}");
    assert!(matches!(
        post(&server, fetch_request(&id, 2, 1)).await,
        QuackMessage::FetchResponse { .. }
    ));
}

#[tokio::test]
async fn cancel_stops_a_long_ddl_statement() {
    let server = TestServer::start(SessionContext::new(), options()).await;
    let id = open_session(&server).await;
    let prepare = tokio::spawn({
        let url = server.url.clone();
        let body = prepare_with(
            &id,
            1,
            "CREATE TABLE big AS SELECT value AS v FROM range(10000000000)",
            0,
        );
        async move { post_bytes(&url, body).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let cancelled = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        post(&server, cancel_request(&id, 0)),
    )
    .await
    .expect("CANCEL answers while the DDL runs");
    assert!(matches!(cancelled, QuackMessage::SuccessResponse { .. }));
    let prepared = tokio::time::timeout(std::time::Duration::from_secs(10), prepare)
        .await
        .expect("the DDL stops")
        .unwrap();
    let message = error_message(decode_request(&prepared).unwrap());
    assert!(message.contains("Interrupted"), "{message}");
}

#[tokio::test]
async fn clients_cannot_raise_the_inline_row_limit() {
    let server = TestServer::start(
        SessionContext::new(),
        options().with_inline_rows(10).with_batch_target_bytes(1),
    )
    .await;
    let id = open_session(&server).await;
    let QuackMessage::PrepareResponse {
        needs_more_fetch,
        results,
        ..
    } = post(
        &server,
        prepare_with(&id, 1, "SELECT * FROM range(100000)", u64::MAX),
    )
    .await
    else {
        panic!("prepare failed");
    };
    assert!(needs_more_fetch);
    assert!(results.iter().map(|c| c.row_count).sum::<usize>() < 100_000);
}

#[tokio::test]
async fn a_fetch_of_the_last_result_answers_while_a_new_statement_runs() {
    let server =
        TestServer::start(SessionContext::new(), options().with_batch_target_bytes(1)).await;
    let id = open_session(&server).await;
    post(
        &server,
        prepare_with(&id, 1, "SELECT * FROM range(100000)", 0),
    )
    .await;
    let prepare = tokio::spawn({
        let url = server.url.clone();
        let body = prepare_with(
            &id,
            2,
            "CREATE TABLE big AS SELECT value AS v FROM range(10000000000)",
            0,
        );
        async move { post_bytes(&url, body).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    // query 2 replaced query 1: the FETCH is refused at once, not after query 2
    let fetched = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        post(&server, fetch_request(&id, 1, 1)),
    )
    .await
    .expect("FETCH answers while the next statement runs");
    let message = error_message(fetched);
    assert!(message.contains("closed"), "{message}");
    post(&server, cancel_request(&id, 2)).await;
    prepare.await.unwrap();
}

#[tokio::test]
async fn refused_statements_are_permission_errors() {
    let read_only = datafusion::prelude::SQLOptions::new().with_allow_ddl(false);
    let server =
        TestServer::start(SessionContext::new(), options().with_sql_options(read_only)).await;
    let id = open_session(&server).await;
    match post(&server, prepare(&id, "CREATE TABLE t (i INT)")).await {
        QuackMessage::ErrorResponse {
            exception_type,
            message,
            ..
        } => {
            assert_eq!(exception_type.as_deref(), Some("Permission"), "{message}");
        }
        other => panic!("expected ERROR_RESPONSE, got {other:?}"),
    }
}
