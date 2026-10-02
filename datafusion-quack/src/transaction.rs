//! Transaction statements, answered honestly.
//!
//! DuckDB's `ATTACH` wraps every remote query in `BEGIN TRANSACTION … COMMIT`.
//! DataFusion has no transactions: each statement takes effect as it runs. So `BEGIN`
//! and `COMMIT` are accepted, and `ROLLBACK` succeeds only when there is nothing to
//! undo. After a write it fails, saying the writes were kept, rather than reporting a
//! rollback that didn't happen.

use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast;

use crate::error::{ClientError, ExceptionType};

/// A transaction statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Control {
    Begin,
    Commit,
    Rollback,
}

impl Control {
    pub(crate) fn of(statement: &Statement) -> Option<Self> {
        let Statement::Statement(statement) = statement else {
            return None;
        };
        match statement.as_ref() {
            ast::Statement::StartTransaction { .. } => Some(Self::Begin),
            ast::Statement::Commit { .. } => Some(Self::Commit),
            ast::Statement::Rollback { .. } => Some(Self::Rollback),
            _ => None,
        }
    }
}

/// A session's transaction, as far as DataFusion can keep one.
#[derive(Debug, Default)]
pub(crate) struct Transaction {
    open: bool,
    /// A statement since `BEGIN` may have changed data.
    wrote: bool,
}

impl Transaction {
    /// Applies a transaction statement, with DuckDB's errors for misplaced ones.
    pub(crate) fn apply(&mut self, control: Control) -> Result<(), ClientError> {
        let error = |message: &str| ClientError::new(ExceptionType::TransactionContext, message);
        let was_open = std::mem::replace(&mut self.open, control == Control::Begin);
        let wrote = std::mem::take(&mut self.wrote);
        match control {
            Control::Begin if was_open => {
                self.wrote = wrote;
                Err(error("cannot start a transaction within a transaction"))
            }
            Control::Commit if !was_open => Err(error("cannot commit - no transaction is active")),
            Control::Rollback if !was_open => {
                Err(error("cannot rollback - no transaction is active"))
            }
            Control::Rollback if wrote => Err(error(
                "cannot rollback: this server has no transactions, so the writes since BEGIN \
                 were applied as they ran and are kept. The transaction is closed.",
            )),
            _ => Ok(()),
        }
    }

    /// Notes a statement about to run inside the transaction.
    pub(crate) fn run(&mut self, statement: &Statement) {
        if self.open && may_write(statement) {
            self.wrote = true;
        }
    }
}

/// Whether `statement` may change data or the catalog. Anything not known to be
/// read-only counts as a write.
fn may_write(statement: &Statement) -> bool {
    match statement {
        Statement::Statement(statement) => !matches!(
            statement.as_ref(),
            ast::Statement::Query(_)
                | ast::Statement::Set(_)
                | ast::Statement::ShowTables { .. }
                | ast::Statement::ShowColumns { .. }
                | ast::Statement::ShowVariable { .. }
                | ast::Statement::ShowVariables { .. }
                | ast::Statement::ShowFunctions { .. }
                | ast::Statement::ShowCreate { .. }
                | ast::Statement::ShowSchemas { .. }
                | ast::Statement::ShowDatabases { .. }
                | ast::Statement::ExplainTable { .. }
                | ast::Statement::Use(_)
        ),
        Statement::Explain(explain) => explain.options.analyze && may_write(&explain.statement),
        Statement::Reset(_) => false,
        Statement::CreateExternalTable(_) | Statement::CopyTo(_) => true,
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

    /// Runs `sqls` through a transaction, returning each control statement's result.
    fn run(sqls: &[&str]) -> Vec<Result<(), ClientError>> {
        let mut transaction = Transaction::default();
        let mut results = Vec::new();
        for sql in sqls {
            let statement = parse(sql);
            match Control::of(&statement) {
                Some(control) => results.push(transaction.apply(control)),
                None => transaction.run(&statement),
            }
        }
        results
    }

    #[test]
    fn transaction_statements_are_recognized() {
        for (sql, control) in [
            ("BEGIN TRANSACTION", Control::Begin),
            ("BEGIN", Control::Begin),
            ("START TRANSACTION", Control::Begin),
            ("COMMIT", Control::Commit),
            ("ROLLBACK", Control::Rollback),
        ] {
            assert_eq!(Control::of(&parse(sql)), Some(control), "{sql}");
        }
        assert_eq!(Control::of(&parse("SELECT 1")), None);
    }

    #[test]
    fn reads_commit_and_roll_back() {
        assert!(
            run(&[
                "BEGIN", "SELECT 1", "COMMIT", "BEGIN", "SELECT 1", "ROLLBACK"
            ])
            .iter()
            .all(Result::is_ok)
        );
    }

    #[test]
    fn a_rollback_after_a_write_says_the_write_was_kept() {
        let results = run(&["BEGIN", "INSERT INTO t VALUES (1)", "ROLLBACK"]);
        let error = results[1].as_ref().unwrap_err();
        assert_eq!(error.exception_type, ExceptionType::TransactionContext);
        assert!(error.message.contains("are kept"), "{error}");
        // the transaction is closed either way, and a write outside one doesn't count
        let results = run(&[
            "BEGIN",
            "CREATE TABLE t (i INT)",
            "ROLLBACK",
            "INSERT INTO t VALUES (1)",
            "BEGIN",
            "ROLLBACK",
        ]);
        assert!(results[1].is_err() && results[2].is_ok() && results[3].is_ok());
    }

    #[test]
    fn misplaced_statements_get_duckdbs_errors() {
        let results = run(&["COMMIT", "ROLLBACK", "BEGIN", "BEGIN", "COMMIT"]);
        let messages: Vec<_> = results
            .iter()
            .map(|r| r.as_ref().err().map(|e| e.message.as_str()))
            .collect();
        assert_eq!(
            messages,
            [
                Some("cannot commit - no transaction is active"),
                Some("cannot rollback - no transaction is active"),
                None,
                Some("cannot start a transaction within a transaction"),
                None,
            ]
        );
    }

    #[test]
    fn explain_analyze_of_a_write_is_a_write() {
        assert!(may_write(&parse(
            "EXPLAIN ANALYZE INSERT INTO t VALUES (1)"
        )));
        assert!(!may_write(&parse("EXPLAIN INSERT INTO t VALUES (1)")));
        assert!(!may_write(&parse("SELECT 1")));
        assert!(may_write(&parse("COPY t TO 'x.parquet'")));
    }
}
