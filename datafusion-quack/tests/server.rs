//! The server against the `quack_protocol` Rust client (milestone M3).

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use datafusion_quack::QuackServer;
use futures::TryStreamExt;
use quack_protocol::{QuackClient, QuackClientOptions, Value, rows_from_chunk};

mod common;
use common::*;

fn client_options(token: &str) -> QuackClientOptions {
    QuackClientOptions {
        auth_token: Some(token.to_string()),
        ..Default::default()
    }
}

async fn connect(server: &TestServer) -> QuackClient {
    QuackClient::connect(&server.uri, client_options(TOKEN))
        .await
        .expect("connects")
}

async fn values(client: &QuackClient, sql: &str) -> Vec<Vec<Value>> {
    let (_, chunks) = client.query(sql, None).await.expect(sql).into_chunks();
    let chunks: Vec<_> = chunks.try_collect().await.expect(sql);
    chunks
        .iter()
        .flat_map(|chunk| rows_from_chunk(chunk).expect("rows"))
        .map(|row| row.into_values().collect())
        .collect()
}

#[tokio::test]
async fn selects_a_million_rows_across_many_batches() {
    let server = TestServer::start(
        SessionContext::new(),
        options().with_batch_target_bytes(64 * 1024).with_inline_rows(5000),
    )
    .await;
    let client = connect(&server).await;
    let (columns, chunks) = client
        .query("SELECT value AS v FROM range(1000000)", None)
        .await
        .unwrap()
        .into_chunks();
    assert_eq!(columns.len(), 1);
    let chunks: Vec<_> = chunks.try_collect().await.unwrap();
    let mut expected = 0i64;
    for chunk in &chunks {
        for value in chunk.column_values(0).unwrap() {
            assert_eq!(value, &Value::Int(expected));
            expected += 1;
        }
    }
    assert_eq!(expected, 1_000_000);
    // the next query on the session still works
    assert_eq!(values(&client, "SELECT 41 + 1").await, [[Value::Int(42)]]);
}

#[tokio::test]
async fn bad_sql_is_an_error_and_the_session_survives() {
    let server = TestServer::start(SessionContext::new(), options()).await;
    let client = connect(&server).await;
    let error = client.query("SELEC 1", None).await.err().expect("an error");
    assert!(error.to_string().contains("Expected"), "{error}");
    let error = client
        .query("SELECT * FROM no_such_table", None)
        .await
        .err()
        .expect("an error");
    assert!(error.to_string().contains("no_such_table"), "{error}");
    assert!(!error.is_connection_fatal());
    assert_eq!(values(&client, "SELECT 1").await, [[Value::Int(1)]]);
}

#[tokio::test]
async fn a_bad_token_is_rejected() {
    let server = TestServer::start(SessionContext::new(), options()).await;
    let error = QuackClient::connect(&server.uri, client_options("wrong-token"))
        .await
        .err()
        .expect("refused");
    assert!(error.to_string().contains("Authentication failed"), "{error}");
}

#[tokio::test]
async fn heartbeats_keep_a_session_alive_past_its_timeout() {
    let server = TestServer::start(SessionContext::new(), options()).await;
    let client = QuackClient::connect(
        &server.uri,
        QuackClientOptions {
            heartbeat_timeout: Some(Duration::from_secs(1)),
            ..client_options(TOKEN)
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(3500)).await;
    assert_eq!(values(&client, "SELECT 'still here'").await, [[Value::String("still here".into())]]);
}

#[tokio::test]
async fn ddl_dml_and_transactions() {
    let server = TestServer::start(SessionContext::new(), options()).await;
    let client = connect(&server).await;
    client.execute("BEGIN TRANSACTION", None).await.unwrap();
    client
        .execute("CREATE TABLE t (id INT, name VARCHAR)", None)
        .await
        .unwrap();
    client
        .execute("INSERT INTO t VALUES (1, 'a'), (2, NULL)", None)
        .await
        .unwrap();
    client.execute("COMMIT", None).await.unwrap();
    assert_eq!(
        values(&client, "SELECT id, name FROM t ORDER BY id").await,
        [
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Int(2), Value::Null]
        ]
    );
    // another session sees the table
    let other = connect(&server).await;
    assert_eq!(values(&other, "SELECT count(*) FROM t").await, [[Value::Int(2)]]);
}

#[tokio::test]
async fn several_statements_return_the_last_result() {
    let server = TestServer::start(SessionContext::new(), options()).await;
    let client = connect(&server).await;
    assert_eq!(
        values(&client, "CREATE TABLE s AS SELECT 7 AS x; SELECT x FROM s").await,
        [[Value::Int(7)]]
    );
}

#[tokio::test]
async fn sessions_run_queries_concurrently() {
    let server = TestServer::start(SessionContext::new(), options()).await;
    let mut tasks = Vec::new();
    for i in 0..8i64 {
        let uri = server.uri.clone();
        tasks.push(tokio::spawn(async move {
            let client = QuackClient::connect(&uri, client_options(TOKEN)).await.unwrap();
            let rows = values(&client, &format!("SELECT sum(value) + {i} FROM range(100000)")).await;
            assert_eq!(rows, [[Value::Int(4_999_950_000 + i)]]);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn shutdown_stops_the_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(
        QuackServer::new(Arc::new(SessionContext::new()))
            .with_options(options())
            .serve_with_shutdown(listener, async {
                let _ = stopped.await;
            }),
    );
    let uri = format!("quack:{address}");
    QuackClient::connect(&uri, client_options(TOKEN)).await.unwrap();
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
