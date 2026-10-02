//! Metrics, through the [`metrics`](https://docs.rs/metrics) facade (the `metrics`
//! feature). The application installs a recorder, e.g. a Prometheus exporter; without
//! one, or without the feature, these calls do nothing.
//!
//! | Metric | Type | Labels | What |
//! |---|---|---|---|
//! | `quack_requests_total` | counter | `message` | requests, by message type |
//! | `quack_errors_total` | counter | `exception` | error responses, by DuckDB exception type |
//! | `quack_sessions` | gauge | | open sessions |
//! | `quack_sessions_closed_total` | counter | `reason` | sessions ended: `disconnect` or `expired` |
//! | `quack_statements_total` | counter | `outcome` | statements prepared: `ok`, `error` or `cancelled` |
//! | `quack_prepare_seconds` | histogram | | PREPARE time, to the first response |
//! | `quack_results_expired_total` | counter | | results closed unread after `result_ttl` |
//! | `quack_result_bytes_held` | gauge | | encoded batches held until the client acknowledges them |
//! | `quack_response_bytes_total` | counter | | response bytes sent |

use std::time::Duration;

use quack_protocol::server::MessageType;

use crate::error::{ClientError, ExceptionType};

pub(crate) fn request(message: MessageType) {
    #[cfg(feature = "metrics")]
    metrics::counter!("quack_requests_total", "message" => message_name(message)).increment(1);
    #[cfg(not(feature = "metrics"))]
    let _ = message;
}

pub(crate) fn error(error: &ClientError) {
    #[cfg(feature = "metrics")]
    metrics::counter!("quack_errors_total", "exception" => error.exception_type.name())
        .increment(1);
    #[cfg(not(feature = "metrics"))]
    let _ = error;
}

pub(crate) fn sessions(open: usize) {
    #[cfg(feature = "metrics")]
    metrics::gauge!("quack_sessions").set(open as f64);
    #[cfg(not(feature = "metrics"))]
    let _ = open;
}

/// Why a session ended.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SessionEnd {
    Disconnect,
    Expired,
}

pub(crate) fn session_closed(reason: SessionEnd) {
    #[cfg(feature = "metrics")]
    metrics::counter!("quack_sessions_closed_total", "reason" => match reason {
        SessionEnd::Disconnect => "disconnect",
        SessionEnd::Expired => "expired",
    })
    .increment(1);
    #[cfg(not(feature = "metrics"))]
    let _ = reason;
}

pub(crate) fn statement(elapsed: Duration, result: Result<(), &ClientError>) {
    #[cfg(feature = "metrics")]
    {
        let outcome = match result {
            Ok(()) => "ok",
            Err(error) if error.exception_type == ExceptionType::Interrupt => "cancelled",
            Err(_) => "error",
        };
        metrics::counter!("quack_statements_total", "outcome" => outcome).increment(1);
        metrics::histogram!("quack_prepare_seconds").record(elapsed.as_secs_f64());
    }
    #[cfg(not(feature = "metrics"))]
    let _ = (elapsed, result, ExceptionType::Interrupt);
}

pub(crate) fn result_expired() {
    #[cfg(feature = "metrics")]
    metrics::counter!("quack_results_expired_total").increment(1);
}

/// `bytes` more (or, negative, fewer) result bytes held for clients.
pub(crate) fn held_bytes(bytes: f64) {
    #[cfg(feature = "metrics")]
    metrics::gauge!("quack_result_bytes_held").increment(bytes);
    #[cfg(not(feature = "metrics"))]
    let _ = bytes;
}

pub(crate) fn response_bytes(bytes: usize) {
    #[cfg(feature = "metrics")]
    metrics::counter!("quack_response_bytes_total").increment(bytes as u64);
    #[cfg(not(feature = "metrics"))]
    let _ = bytes;
}

#[cfg(feature = "metrics")]
fn message_name(message: MessageType) -> &'static str {
    match message {
        MessageType::ConnectionRequest => "connection",
        MessageType::PrepareRequest => "prepare",
        MessageType::FetchRequest => "fetch",
        MessageType::CancelRequest => "cancel",
        MessageType::HeartbeatRequest => "heartbeat",
        MessageType::Acknowledgement => "acknowledgement",
        MessageType::DisconnectMessage => "disconnect",
        _ => "other",
    }
}
