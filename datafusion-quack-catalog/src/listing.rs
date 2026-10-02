//! Listing a catalog's tables without opening each one.
//!
//! The catalog functions and `information_schema.columns` need every table's columns.
//! Without a listing they get them by opening each table with `SchemaProvider::table`,
//! which for a catalog whose tables are costly to open (one that reads metadata from a
//! database, say) means a round trip or more per table on every `ATTACH`.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableType;
use datafusion::error::Result;

/// A table or view, as the catalog functions describe it.
#[derive(Debug, Clone)]
pub struct ListedTable {
    /// The schema it is in.
    pub schema: String,
    /// Its name.
    pub name: String,
    /// Its columns: the schema its `TableProvider` would report.
    pub columns: SchemaRef,
    /// `Base`, `View` or `Temporary`.
    pub table_type: TableType,
    /// A view's `CREATE VIEW` statement, if known.
    pub definition: Option<String>,
}

/// Lists every table and view of one catalog, in place of opening each.
///
/// What it lists must match what the catalog's schemas return: the same names, and
/// for each the columns its `TableProvider` reports.
#[async_trait]
pub trait CatalogListing: Debug + Send + Sync {
    /// Every table and view in the catalog, in any order.
    async fn tables(&self) -> Result<Vec<ListedTable>>;
}

/// [`CatalogListing`]s by catalog name. Add it to the `SessionConfig` as an extension;
/// catalogs without a listing are walked table by table.
///
/// ```
/// # use std::sync::Arc;
/// # use datafusion::prelude::SessionConfig;
/// # use datafusion_quack_catalog::CatalogListings;
/// let config = SessionConfig::new().with_extension(Arc::new(CatalogListings::new()));
/// ```
#[derive(Debug, Default)]
pub struct CatalogListings {
    listings: HashMap<String, Arc<dyn CatalogListing>>,
}

impl CatalogListings {
    /// No listings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lists catalog `catalog` with `listing`.
    pub fn with(mut self, catalog: impl Into<String>, listing: Arc<dyn CatalogListing>) -> Self {
        self.listings.insert(catalog.into(), listing);
        self
    }

    /// The listing of catalog `catalog`, if it has one.
    pub fn get(&self, catalog: &str) -> Option<&Arc<dyn CatalogListing>> {
        self.listings.get(catalog)
    }
}
