//! Query hooks: statements the server answers itself, before DataFusion plans them.

use std::fmt::Debug;

use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast;

use crate::auth::SessionInfo;
use crate::error::ClientError;

/// What a statement produced.
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

/// Accepts `BEGIN`, `COMMIT` and `ROLLBACK` as no-ops.
///
/// DuckDB's `ATTACH` wraps every remote query in `BEGIN TRANSACTION … COMMIT`.
/// DataFusion has no transactions, so each statement stands alone.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransactionHook;

#[async_trait]
impl QueryHook for TransactionHook {
    async fn handle(
        &self,
        statement: &Statement,
        _ctx: &SessionContext,
        _session: &SessionInfo,
    ) -> Option<Result<QueryOutput, ClientError>> {
        let Statement::Statement(statement) = statement else {
            return None;
        };
        match statement.as_ref() {
            ast::Statement::StartTransaction { .. }
            | ast::Statement::Commit { .. }
            | ast::Statement::Rollback { .. } => Some(Ok(QueryOutput::Success)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::DFParserBuilder;
    use datafusion::sql::sqlparser::dialect::DuckDbDialect;

    use super::*;

    fn parse(sql: &str) -> Statement {
        DFParserBuilder::new(sql)
            .with_dialect(&DuckDbDialect {})
            .build()
            .unwrap()
            .parse_statements()
            .unwrap()
            .pop_front()
            .unwrap()
    }

    fn session() -> SessionInfo {
        SessionInfo {
            connection_id: "c".into(),
            client_version: String::new(),
            client_platform: String::new(),
        }
    }

    #[tokio::test]
    async fn transactions_are_no_ops() {
        let ctx = SessionContext::new();
        for sql in [
            "BEGIN TRANSACTION",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "START TRANSACTION",
        ] {
            let output = TransactionHook
                .handle(&parse(sql), &ctx, &session())
                .await
                .unwrap_or_else(|| panic!("{sql} not handled"))
                .unwrap();
            assert!(matches!(output, QueryOutput::Success), "{sql}");
        }
        assert!(
            TransactionHook
                .handle(&parse("SELECT 1"), &ctx, &session())
                .await
                .is_none()
        );
    }
}
