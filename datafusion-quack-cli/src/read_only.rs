//! Tables read from files, which clients may query but not change.
//!
//! The command serves files read in place. Writing to one would change the user's
//! data (a directory table gains new files), and a single-file table can't take
//! writes at all. So every write is refused, saying how to make a writable copy.

use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::{Constraints, DFSchemaRef, Statistics, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::logical_expr::dml::{InsertOp, MergeIntoClause};
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

/// A file-backed table that refuses INSERT, UPDATE, DELETE, TRUNCATE and MERGE.
#[derive(Debug)]
pub(crate) struct ReadOnlyFile {
    name: String,
    path: String,
    inner: Arc<dyn TableProvider>,
}

impl ReadOnlyFile {
    pub(crate) fn new(name: &str, path: &str, inner: Arc<dyn TableProvider>) -> Self {
        Self {
            name: name.to_string(),
            path: path.to_string(),
            inner,
        }
    }

    fn refuse<T>(&self, what: &str) -> Result<T> {
        let (name, path) = (&self.name, &self.path);
        plan_err!(
            "{what} is not supported on table {name}: it is read from the file {path}, which \
             this server does not change. Copy it into memory to change it: CREATE TABLE \
             my_{name} AS SELECT * FROM {name}"
        )
    }
}

#[async_trait]
impl TableProvider for ReadOnlyFile {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        self.inner.scan_with_args(state, args).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.refuse("INSERT")
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.refuse("DELETE")
    }

    async fn update(
        &self,
        _state: &dyn Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.refuse("UPDATE")
    }

    async fn truncate(&self, _state: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        self.refuse("TRUNCATE")
    }

    async fn merge_into(
        &self,
        _state: &dyn Session,
        _source: Arc<dyn ExecutionPlan>,
        _merge_schema: DFSchemaRef,
        _on: Expr,
        _clauses: Vec<MergeIntoClause>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.refuse("MERGE")
    }
}
