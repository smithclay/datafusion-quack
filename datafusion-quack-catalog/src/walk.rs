//! Walks every catalog, schema and table a session can see.

use std::sync::Arc;

use datafusion::catalog::{CatalogProviderList, TableProvider};
use datafusion::error::Result;

/// A table, with where it lives.
pub(crate) struct TableEntry {
    pub(crate) catalog: String,
    pub(crate) schema: String,
    pub(crate) name: String,
    pub(crate) provider: Arc<dyn TableProvider>,
}

/// Every `(catalog, schema)` pair, in name order.
pub(crate) fn schemas(list: &dyn CatalogProviderList) -> Vec<(String, String)> {
    let mut result = Vec::new();
    let mut catalogs = list.catalog_names();
    catalogs.sort();
    for catalog_name in catalogs {
        let Some(catalog) = list.catalog(&catalog_name) else {
            continue;
        };
        let mut schema_names = catalog.schema_names();
        schema_names.sort();
        for schema in schema_names {
            result.push((catalog_name.clone(), schema));
        }
    }
    result
}

/// Every table, in name order.
pub(crate) async fn tables(list: &dyn CatalogProviderList) -> Result<Vec<TableEntry>> {
    let mut result = Vec::new();
    for (catalog_name, schema_name) in schemas(list) {
        let Some(schema) = list
            .catalog(&catalog_name)
            .and_then(|catalog| catalog.schema(&schema_name))
        else {
            continue;
        };
        let mut table_names = schema.table_names();
        table_names.sort();
        for name in table_names {
            if let Some(provider) = schema.table(&name).await? {
                result.push(TableEntry {
                    catalog: catalog_name.clone(),
                    schema: schema_name.clone(),
                    name,
                    provider,
                });
            }
        }
    }
    Ok(result)
}
