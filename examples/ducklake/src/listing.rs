//! Lists a DuckLake catalog for DuckDB's `ATTACH` in a few metadata queries.
//!
//! Without it quack opens every table to read its columns, and opening a DuckLake
//! table reads its columns, statistics and partitioning: a dozen metadata queries per
//! table, twice per `ATTACH`.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion_ducklake::types::build_arrow_schema;
use datafusion_ducklake::DuckLakeCatalog;
use datafusion_quack::datafusion_quack_catalog::{CatalogListing, ListedTable};

/// Lists `catalog`: its tables from three bulk metadata queries, its views by opening
/// them, since a view's columns come from planning its SQL.
#[derive(Debug)]
pub struct DuckLakeListing {
    catalog: Arc<DuckLakeCatalog>,
}

impl DuckLakeListing {
    pub fn new(catalog: Arc<DuckLakeCatalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait::async_trait]
impl CatalogListing for DuckLakeListing {
    async fn tables(&self) -> Result<Vec<ListedTable>> {
        let provider = self.catalog.provider();
        let snapshot = provider.get_current_snapshot().map_err(external)?;
        let mut columns = BTreeMap::<i64, Vec<_>>::new();
        for column in provider.list_all_columns(snapshot).map_err(external)? {
            columns
                .entry(column.table_id)
                .or_default()
                .push(column.column);
        }

        let mut listed = Vec::new();
        for table in provider.list_all_tables(snapshot).map_err(external)? {
            let table_columns = columns.remove(&table.table.table_id).unwrap_or_default();
            listed.push(ListedTable {
                schema: table.schema_name,
                name: table.table.table_name,
                columns: Arc::new(build_arrow_schema(&table_columns).map_err(external)?),
                table_type: TableType::Base,
                definition: None,
            });
        }
        for view in provider.list_all_views(snapshot).map_err(external)? {
            let Some(schema) = self.catalog.schema(&view.schema_name) else {
                continue;
            };
            let Some(opened) = schema.table(&view.view.view_name).await? else {
                continue;
            };
            listed.push(ListedTable {
                schema: view.schema_name,
                name: view.view.view_name,
                columns: opened.schema(),
                table_type: opened.table_type(),
                definition: opened.get_table_definition().map(str::to_string),
            });
        }
        Ok(listed)
    }
}

fn external(error: datafusion_ducklake::DuckLakeError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
