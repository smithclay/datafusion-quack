//! Catalogs that bring their own `information_schema`, and catalogs that are listed
//! instead of walked.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, TableProvider,
};
use datafusion::common::exec_err;
use datafusion::datasource::{MemTable, TableType};
use datafusion::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_quack_catalog::{
    CatalogListing, CatalogListings, ListedTable, duckdb_session_state,
};

fn columns() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn mem_table() -> Arc<dyn TableProvider> {
    Arc::new(MemTable::try_new(columns(), vec![vec![]]).unwrap())
}

fn session(config: SessionConfig, catalog: Arc<dyn CatalogProvider>) -> SessionContext {
    let ctx = SessionContext::new_with_config(config);
    ctx.register_catalog("lake", catalog);
    SessionContext::new_with_state(duckdb_session_state(ctx.state()).unwrap())
}

/// The first column of `sql`'s rows, as strings.
async fn strings(ctx: &SessionContext, sql: &str) -> Result<Vec<String>> {
    let batches: Vec<RecordBatch> = ctx.sql(sql).await?.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        let column = arrow::compute::cast(batch.column(0), &DataType::Utf8)?;
        values.extend(
            column
                .as_string::<i32>()
                .iter()
                .map(|v| v.unwrap_or_default().to_string()),
        );
    }
    Ok(values)
}

#[tokio::test]
async fn duckdb_information_schema_replaces_a_catalogs_own() -> Result<()> {
    let catalog = MemoryCatalogProvider::new();
    let main = MemorySchemaProvider::new();
    main.register_table("t".into(), mem_table())?;
    catalog.register_schema("main", Arc::new(main))?;
    // the catalog's own metadata tables, as a DuckLake catalog has
    let own = MemorySchemaProvider::new();
    own.register_table("files".into(), mem_table())?;
    own.register_table("columns".into(), mem_table())?;
    catalog.register_schema("information_schema", Arc::new(own))?;
    let ctx = session(SessionConfig::new(), Arc::new(catalog));

    let types = strings(
        &ctx,
        "SELECT data_type FROM lake.information_schema.columns WHERE table_name = 't'",
    )
    .await?;
    assert_eq!(types, ["INTEGER"]);
    let schemas = strings(
        &ctx,
        "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'lake'",
    )
    .await?;
    assert_eq!(schemas, ["main"]);
    let tables = strings(
        &ctx,
        "SELECT table_name FROM duckdb_tables() UNION ALL SELECT view_name FROM duckdb_views()",
    )
    .await?;
    assert_eq!(tables, ["t"]);
    Ok(())
}

/// A schema whose tables can be named but not opened.
#[derive(Debug)]
struct Unopenable;

#[async_trait]
impl SchemaProvider for Unopenable {
    fn table_names(&self) -> Vec<String> {
        vec!["t".into(), "v".into()]
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("opened {name}")
    }

    fn table_exist(&self, name: &str) -> bool {
        name == "t" || name == "v"
    }
}

#[derive(Debug)]
struct Listing;

#[async_trait]
impl CatalogListing for Listing {
    async fn tables(&self) -> Result<Vec<ListedTable>> {
        let listed = |name: &str, table_type, definition: Option<&str>| ListedTable {
            schema: "main".into(),
            name: name.into(),
            columns: columns(),
            table_type,
            definition: definition.map(str::to_string),
        };
        Ok(vec![
            listed(
                "v",
                TableType::View,
                Some("CREATE VIEW main.v AS SELECT 1 AS id;"),
            ),
            listed("t", TableType::Base, None),
            // listed tables pass through the same filter as walked ones
            ListedTable {
                schema: "information_schema".into(),
                ..listed("files", TableType::View, None)
            },
        ])
    }
}

fn unopenable_catalog() -> Arc<dyn CatalogProvider> {
    let catalog = MemoryCatalogProvider::new();
    catalog
        .register_schema("main", Arc::new(Unopenable))
        .unwrap();
    Arc::new(catalog)
}

#[tokio::test]
async fn a_listed_catalog_is_described_without_opening_its_tables() -> Result<()> {
    let listings = CatalogListings::new().with("lake", Arc::new(Listing));
    let ctx = session(
        SessionConfig::new().with_extension(Arc::new(listings)),
        unopenable_catalog(),
    );

    let tables = strings(&ctx, "SELECT sql FROM duckdb_tables()").await?;
    assert_eq!(tables, ["CREATE TABLE t(id INTEGER NOT NULL);"]);
    let views = strings(&ctx, "SELECT sql FROM duckdb_views()").await?;
    assert_eq!(views, ["CREATE VIEW main.v AS SELECT 1 AS id;"]);
    let columns = strings(
        &ctx,
        "SELECT table_name || '.' || column_name FROM lake.information_schema.columns ORDER BY 1",
    )
    .await?;
    assert_eq!(columns, ["t.id", "v.id"]);
    Ok(())
}

#[tokio::test]
async fn an_unlisted_catalog_is_walked() {
    let ctx = session(SessionConfig::new(), unopenable_catalog());
    let error = strings(&ctx, "SELECT sql FROM duckdb_tables()")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("opened t"), "{error}");
}
