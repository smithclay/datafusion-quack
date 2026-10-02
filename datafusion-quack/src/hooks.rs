//! Query hooks: statements the server answers itself, before DataFusion plans them.

use std::fmt::Debug;

use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement;

use crate::auth::SessionInfo;
use crate::error::ClientError;

/// What a statement produced.
#[non_exhaustive]
pub enum QueryOutput {
    /// Rows. The stream's schema is the result's schema.
    Rows(SendableRecordBatchStream),
    /// No rows: DDL, a transaction statement, `SET`. Sent as DuckDB sends it, a single
    /// BOOLEAN column named `Success` with no rows.
    Success,
}

impl Debug for QueryOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rows(stream) => f.debug_tuple("Rows").field(&stream.schema()).finish(),
            Self::Success => f.write_str("Success"),
        }
    }
}

/// Answers some statements instead of DataFusion.
///
/// Hooks run in order; the first that returns `Some` answers the statement. Extend
/// the server through hooks (and DataFusion planner extensions) rather than by
/// rewriting SQL text.
#[async_trait]
pub trait QueryHook: Send + Sync + Debug {
    /// Handles `statement`, or returns `None` to leave it to the next hook.
    async fn handle(
        &self,
        statement: &Statement,
        ctx: &SessionContext,
        session: &SessionInfo,
    ) -> Option<Result<QueryOutput, ClientError>>;
}
