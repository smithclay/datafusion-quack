//! `information_schema` with DuckDB type names.
//!
//! DataFusion's own `information_schema` names types the Arrow way (`Int64`,
//! `Decimal128(10, 2)`). DuckDB clients read `information_schema.columns.data_type`
//! as a DuckDB type name (`BIGINT`, `DECIMAL(10,2)`), so `columns` is replaced; every
//! other table is DataFusion's.

use std::sync::Arc;

use arrow::array::{RecordBatch, StringBuilder, UInt64Builder, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::information_schema::InformationSchemaProvider;
use datafusion::catalog::{CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::error::Result;

use crate::table::ComputedTable;
use crate::types::{column_type_name, numeric_precision};
use crate::walk;

/// The schema name.
pub(crate) const INFORMATION_SCHEMA: &str = "information_schema";

/// `information_schema`, as DuckDB clients expect it.
#[derive(Debug)]
pub struct DuckDbInformationSchema {
    inner: InformationSchemaProvider,
    list: Arc<dyn CatalogProviderList>,
}

impl DuckDbInformationSchema {
    /// The `information_schema` of every catalog in `list`.
    pub fn new(list: Arc<dyn CatalogProviderList>) -> Self {
        Self {
            inner: InformationSchemaProvider::new(Arc::clone(&list)),
            list,
        }
    }
}

#[async_trait]
impl SchemaProvider for DuckDbInformationSchema {
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if name.eq_ignore_ascii_case("columns") {
            return Ok(Some(Arc::new(columns_table(Arc::clone(&self.list)))));
        }
        let name = crate::names::resolve(name, &self.inner.table_names())
            .unwrap_or_else(|| name.to_string());
        self.inner.table(&name).await
    }

    fn table_exist(&self, name: &str) -> bool {
        crate::names::resolve(name, &self.inner.table_names()).is_some()
    }
}

fn columns_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::UInt64, false),
        Field::new("column_default", DataType::Utf8, true),
        Field::new("is_nullable", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("character_maximum_length", DataType::UInt64, true),
        Field::new("character_octet_length", DataType::UInt64, true),
        Field::new("numeric_precision", DataType::UInt64, true),
        Field::new("numeric_precision_radix", DataType::UInt64, true),
        Field::new("numeric_scale", DataType::UInt64, true),
        Field::new("datetime_precision", DataType::UInt64, true),
        Field::new("interval_type", DataType::Utf8, true),
    ]))
}

fn columns_table(list: Arc<dyn CatalogProviderList>) -> ComputedTable {
    ComputedTable::new(
        "information_schema.columns",
        columns_schema(),
        move |schema| {
            let list = Arc::clone(&list);
            Box::pin(async move { columns(list.as_ref(), schema).await })
        },
    )
}

async fn columns(list: &dyn CatalogProviderList, schema: SchemaRef) -> Result<RecordBatch> {
    let mut catalogs = StringBuilder::new();
    let mut schemas = StringBuilder::new();
    let mut tables = StringBuilder::new();
    let mut names = StringBuilder::new();
    let mut positions = UInt64Builder::new();
    let mut nullable = StringBuilder::new();
    let mut types = StringBuilder::new();
    let mut precisions = UInt64Builder::new();
    let mut radixes = UInt64Builder::new();
    let mut scales = UInt64Builder::new();

    let mut rows = 0;
    for table in walk::tables(list).await? {
        for (index, field) in table.provider.schema().fields().iter().enumerate() {
            catalogs.append_value(&table.catalog);
            schemas.append_value(&table.schema);
            tables.append_value(&table.name);
            names.append_value(field.name());
            positions.append_value(index as u64 + 1);
            rows += 1;
            nullable.append_value(if field.is_nullable() { "YES" } else { "NO" });
            types.append_value(column_type_name(field));
            let (precision, radix, scale) = numeric_precision(field.data_type());
            precisions.append_option(precision);
            radixes.append_option(radix);
            scales.append_option(scale);
        }
    }
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(catalogs.finish()),
            Arc::new(schemas.finish()),
            Arc::new(tables.finish()),
            Arc::new(names.finish()),
            Arc::new(positions.finish()),
            new_null_array(&DataType::Utf8, rows),
            Arc::new(nullable.finish()),
            Arc::new(types.finish()),
            new_null_array(&DataType::UInt64, rows),
            new_null_array(&DataType::UInt64, rows),
            Arc::new(precisions.finish()),
            Arc::new(radixes.finish()),
            Arc::new(scales.finish()),
            new_null_array(&DataType::UInt64, rows),
            new_null_array(&DataType::Utf8, rows),
        ],
    )?)
}
