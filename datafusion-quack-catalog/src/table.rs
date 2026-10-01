//! A table whose rows are computed when it is scanned.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures::future::BoxFuture;

type Compute = Arc<dyn Fn() -> BoxFuture<'static, Result<RecordBatch>> + Send + Sync>;

/// A read-only table computed at scan time, e.g. from the catalog.
pub(crate) struct ComputedTable {
    name: &'static str,
    schema: SchemaRef,
    compute: Compute,
}

impl Debug for ComputedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputedTable")
            .field("name", &self.name)
            .finish()
    }
}

impl ComputedTable {
    pub(crate) fn new(
        name: &'static str,
        schema: SchemaRef,
        compute: impl Fn() -> BoxFuture<'static, Result<RecordBatch>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name,
            schema,
            compute: Arc::new(compute),
        }
    }
}

#[async_trait]
impl TableProvider for ComputedTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch = (self.compute)().await?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?)
    }
}
