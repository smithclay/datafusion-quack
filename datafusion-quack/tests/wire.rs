//! Wire conformance (gate G2): our responses against DuckDB 2.0's, byte for byte.
//!
//! `testdata/wire/golden/` holds requests and DuckDB 2.0 `quack_serve`'s responses to
//! them, for a table both servers create from `setup.sql`. The test sends the same
//! requests to an in-process server and requires the same bytes back.
//!
//! Documented differences, compared by meaning rather than bytes:
//!
//! - CONNECTION_RESPONSE: the connection id is random, and the server reports its own
//!   version and platform.
//! - ERROR_RESPONSE: the message and exception type come from DataFusion
//!   (`Binder Error: table '…' not found`, where DuckDB says
//!   `Catalog Error: Table with name … does not exist!`).
//!
//! To capture the fixtures again, run a DuckDB 2.0 server
//! (`CALL quack_serve('quack:127.0.0.1:9494', token => 'golden-token')`) and
//! `QUACK_GOLDEN_URI=http://127.0.0.1:9494/quack cargo test -p datafusion-quack --test wire -- --ignored`.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::path::PathBuf;

use datafusion::prelude::{SessionConfig, SessionContext};
use quack_protocol::server::{
    HugeIntParts, MessageHeader, MessageType, QuackMessage, decode_request, encode_response,
};

mod common;
use common::*;

const GOLDEN_TOKEN: &str = "golden-token";

/// The statements each fixture prepares, in order.
const QUERIES: &[&str] = &[
    "SELECT * FROM golden ORDER BY i",
    "SELECT i, s FROM golden WHERE i > 1 ORDER BY i",
    "SELECT * FROM golden WHERE i > 100",
    "SELECT * FROM no_such_table",
    "BEGIN TRANSACTION",
    "SELECT n, n * 2 AS twice, 'n' || n AS label FROM golden_big ORDER BY n",
];

/// The index of the query whose ERROR_RESPONSE differs (see the module docs).
const ERROR_QUERY: usize = 3;

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../testdata/wire/golden")
}

fn setup_sql() -> Vec<String> {
    std::fs::read_to_string(dir().join("setup.sql"))
        .unwrap()
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

fn connect_request() -> Vec<u8> {
    encode_response(&QuackMessage::ConnectionRequest {
        header: MessageHeader::new(MessageType::ConnectionRequest).with_client_query_id(1),
        auth_string: Some(GOLDEN_TOKEN.into()),
        client_duckdb_version: Some("v2.0.0-golden".into()),
        client_platform: Some("golden".into()),
        min_supported_quack_version: 3,
        max_supported_quack_version: 3,
        client_id: None,
        heartbeat_timeout_seconds: 60,
    })
    .unwrap()
}

fn prepare_request(connection_id: &str, index: usize, sql: &str) -> Vec<u8> {
    encode_response(&QuackMessage::PrepareRequest {
        header: MessageHeader::new(MessageType::PrepareRequest)
            .with_connection(connection_id)
            .with_client_query_id(2),
        sql: sql.to_string(),
        query_uuid: Some(HugeIntParts {
            upper: 0,
            lower: index as u64 + 1,
        }),
        inline_rows: None,
    })
    .unwrap()
}

/// The request with its connection id replaced, so a fixture replays on any server.
fn with_connection(request: &[u8], connection_id: &str) -> Vec<u8> {
    let mut message = decode_request(request).unwrap();
    let header = match &mut message {
        QuackMessage::PrepareRequest { header, .. } | QuackMessage::Disconnect { header } => header,
        other => panic!("unexpected fixture request {other:?}"),
    };
    header.connection_id = Some(connection_id.to_string());
    encode_response(&message).unwrap()
}

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(dir().join(name)).unwrap_or_else(|e| panic!("{name}: {e}"))
}

#[tokio::test]
async fn responses_match_duckdb_byte_for_byte() {
    // the DuckDB dialect, as the server parses SQL
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().set_str("datafusion.sql_parser.dialect", "duckdb"),
    );
    for statement in setup_sql() {
        ctx.sql(&statement).await.unwrap().collect().await.unwrap();
    }
    let server = TestServer::start(ctx, options().with_token(GOLDEN_TOKEN)).await;

    let connected = post_bytes(&server.url, fixture("connect.req")).await;
    let ours = decode_request(&connected).unwrap();
    let QuackMessage::ConnectionResponse {
        quack_version,
        heartbeat_timeout_seconds,
        ..
    } = &ours
    else {
        panic!("{ours:?}");
    };
    let QuackMessage::ConnectionResponse {
        quack_version: duck_version,
        heartbeat_timeout_seconds: duck_heartbeat,
        ..
    } = decode_request(&fixture("connect.resp")).unwrap()
    else {
        panic!("connect.resp is not a CONNECTION_RESPONSE");
    };
    assert_eq!(
        (quack_version, heartbeat_timeout_seconds),
        (&duck_version, &duck_heartbeat)
    );
    let id = connection_id(&connected);

    for (index, sql) in QUERIES.iter().enumerate() {
        let request = with_connection(&fixture(&format!("q{}.req", index + 1)), &id);
        let response = post_bytes(&server.url, request).await;
        let expected = fixture(&format!("q{}.resp", index + 1));
        if index == ERROR_QUERY {
            assert!(matches!(
                decode_request(&response).unwrap(),
                QuackMessage::ErrorResponse { .. }
            ));
            assert!(matches!(
                decode_request(&expected).unwrap(),
                QuackMessage::ErrorResponse { .. }
            ));
            continue;
        }
        assert_eq!(
            response,
            expected,
            "{sql}: ours {:?}\nDuckDB {:?}",
            decode_request(&response),
            decode_request(&expected)
        );
    }

    let request = with_connection(&fixture("disconnect.req"), &id);
    assert_eq!(
        post_bytes(&server.url, request).await,
        fixture("disconnect.resp")
    );
}

#[test]
fn fixtures_round_trip_through_the_codec() {
    for entry in std::fs::read_dir(dir()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "req" || e == "resp") {
            let bytes = std::fs::read(&path).unwrap();
            let message =
                decode_request(&bytes).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            assert_eq!(
                encode_response(&message).unwrap(),
                bytes,
                "{}",
                path.display()
            );
        }
    }
}

/// Records the fixtures from a DuckDB 2.0 server (see the module docs).
#[tokio::test]
#[ignore = "needs a DuckDB 2.0 quack_serve; set QUACK_GOLDEN_URI"]
async fn capture_fixtures_from_duckdb() {
    let url = std::env::var("QUACK_GOLDEN_URI").expect("QUACK_GOLDEN_URI");
    let write = |name: &str, bytes: &[u8]| std::fs::write(dir().join(name), bytes).unwrap();

    let request = connect_request();
    let response = post_bytes(&url, request.clone()).await;
    write("connect.req", &request);
    write("connect.resp", &response);
    let id = connection_id(&response);

    for (index, statement) in setup_sql().iter().enumerate() {
        let request = prepare_request(&id, 100 + index, statement);
        let response = decode_request(&post_bytes(&url, request).await).unwrap();
        assert!(
            !matches!(response, QuackMessage::ErrorResponse { .. }),
            "{statement}: {response:?}"
        );
    }
    for (index, sql) in QUERIES.iter().enumerate() {
        let request = prepare_request(&id, index, sql);
        let response = post_bytes(&url, request.clone()).await;
        write(&format!("q{}.req", index + 1), &request);
        write(&format!("q{}.resp", index + 1), &response);
    }
    let request = encode_response(&QuackMessage::Disconnect {
        header: MessageHeader::new(MessageType::DisconnectMessage).with_connection(id),
    })
    .unwrap();
    let response = post_bytes(&url, request.clone()).await;
    write("disconnect.req", &request);
    write("disconnect.resp", &response);
}
