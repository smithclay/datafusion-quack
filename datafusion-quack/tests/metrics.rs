//! The `metrics` feature: what a recorder sees after a short session.

#![cfg(feature = "metrics")]
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use metrics_util::{CompositeKey, MetricKind};
use quack_protocol::{QuackClient, QuackClientOptions};

mod common;
use common::*;

/// The value of the metric `name` with `labels` in `snapshot`.
fn value<'a>(
    snapshot: &'a [(CompositeKey, DebugValue)],
    kind: MetricKind,
    name: &str,
    labels: &[(&str, &str)],
) -> Option<&'a DebugValue> {
    snapshot
        .iter()
        .find(|(key, _)| {
            key.kind() == kind
                && key.key().name() == name
                && labels.iter().all(|(k, v)| {
                    key.key()
                        .labels()
                        .any(|label| label.key() == *k && label.value() == *v)
                })
        })
        .map(|(_, value)| value)
}

#[tokio::test]
async fn a_session_is_counted() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();

    let server = TestServer::start(
        SessionContext::new(),
        options().with_batch_target_bytes(1024).with_inline_rows(10),
    )
    .await;
    let client = QuackClient::connect(
        &server.uri,
        QuackClientOptions {
            auth_token: Some(TOKEN.into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (_, chunks) = client
        .query("SELECT * FROM range(100000)", None)
        .await
        .unwrap()
        .into_chunks();
    let _: Vec<_> = chunks.try_collect().await.unwrap();
    assert!(client.query("SELEC 1", None).await.is_err());

    client.disconnect().await.unwrap();

    // one snapshot: taking one resets the recorder's values
    let snapshot: Vec<_> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(key, _, _, value)| (key, value))
        .collect();
    let counter =
        |name, labels: &[(&str, &str)]| match value(&snapshot, MetricKind::Counter, name, labels) {
            Some(DebugValue::Counter(n)) => *n,
            other => panic!("{name} {labels:?}: {other:?}"),
        };
    let gauge = |name| match value(&snapshot, MetricKind::Gauge, name, &[]) {
        Some(DebugValue::Gauge(n)) => n.into_inner(),
        other => panic!("{name}: {other:?}"),
    };
    assert_eq!(
        counter("quack_requests_total", &[("message", "connection")]),
        1
    );
    assert!(counter("quack_requests_total", &[("message", "fetch")]) > 1);
    assert_eq!(counter("quack_statements_total", &[("outcome", "ok")]), 1);
    assert_eq!(
        counter("quack_statements_total", &[("outcome", "error")]),
        1
    );
    assert_eq!(counter("quack_errors_total", &[("exception", "Parser")]), 1);
    assert!(counter("quack_response_bytes_total", &[]) > 100_000);
    assert_eq!(
        counter("quack_sessions_closed_total", &[("reason", "disconnect")]),
        1
    );
    assert_eq!(gauge("quack_sessions"), 0.0);
    // the result's batches were acknowledged, or released with the session
    assert_eq!(gauge("quack_result_bytes_held"), 0.0);
}
