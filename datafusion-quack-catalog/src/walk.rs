//! Walks every catalog, schema and table a session can see.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::datasource::TableType;
use datafusion::error::Result;

use crate::information_schema::INFORMATION_SCHEMA;
use crate::listing::CatalogListings;

/// A table, with where it lives.
pub(crate) struct TableEntry {
    pub(crate) catalog: String,
    pub(crate) schema: String,
    pub(crate) name: String,
    pub(crate) columns: SchemaRef,
    pub(crate) table_type: TableType,
    pub(crate) definition: Option<String>,
}

/// Every `(catalog, schema)` pair, in name order.
pub(crate) fn schemas(list: &dyn CatalogProviderList) -> Vec<(String, String)> {
    let mut result = Vec::new();
    for (catalog_name, catalog) in catalogs(list) {
        for schema in schema_names(catalog.as_ref()) {
            result.push((catalog_name.clone(), schema));
        }
    }
    result
}

/// Every table, in name order. A catalog with a listing in `listings` is listed;
/// the others are walked, opening each table.
pub(crate) async fn tables(
    list: &dyn CatalogProviderList,
    listings: Option<&CatalogListings>,
) -> Result<Vec<TableEntry>> {
    let mut result = Vec::new();
    for (catalog_name, catalog) in catalogs(list) {
        if let Some(listing) = listings.and_then(|listings| listings.get(&catalog_name)) {
            let mut listed: Vec<TableEntry> = listing
                .tables()
                .await?
                .into_iter()
                .filter(|table| !is_information_schema(&table.schema))
                .map(|table| TableEntry {
                    catalog: catalog_name.clone(),
                    schema: table.schema,
                    name: table.name,
                    columns: table.columns,
                    table_type: table.table_type,
                    definition: table.definition,
                })
                .collect();
            listed.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
            result.extend(listed);
            continue;
        }
        for schema_name in schema_names(catalog.as_ref()) {
            let Some(schema) = catalog.schema(&schema_name) else {
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
                        columns: provider.schema(),
                        table_type: provider.table_type(),
                        definition: provider.get_table_definition().map(str::to_string),
                    });
                }
            }
        }
    }
    Ok(result)
}

/// Every catalog, in name order.
fn catalogs(list: &dyn CatalogProviderList) -> Vec<(String, Arc<dyn CatalogProvider>)> {
    let mut names = list.catalog_names();
    names.sort();
    names
        .into_iter()
        .filter_map(|name| list.catalog(&name).map(|catalog| (name, catalog)))
        .collect()
}

/// A catalog's schemas, in name order, without an `information_schema` of its own:
/// every catalog gets DuckDB's (see [`crate::information_schema`]), which describes
/// the others rather than being one of them.
fn schema_names(catalog: &dyn CatalogProvider) -> Vec<String> {
    let mut names: Vec<String> = catalog
        .schema_names()
        .into_iter()
        .filter(|name| !is_information_schema(name))
        .collect();
    names.sort();
    names
}

fn is_information_schema(name: &str) -> bool {
    name.eq_ignore_ascii_case(INFORMATION_SCHEMA)
}
