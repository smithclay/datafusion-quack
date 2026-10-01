//! DuckDB's catalog table functions: `duckdb_databases()`, `duckdb_schemas()`,
//! `duckdb_tables()`, `duckdb_views()` and `duckdb_columns()`.
//!
//! DuckDB's `ATTACH` reads the remote catalog through them: `duckdb_schemas()` for
//! the schema tree, and `duckdb_tables()`' `sql` column, a `CREATE TABLE` statement
//! the client parses to learn each table's columns.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{
    CatalogProviderList, TableFunctionArgs, TableFunctionImpl, TableProvider,
};
use datafusion::common::{ScalarValue, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::SessionState;

use crate::table::ComputedTable;
use crate::types::{column_type_name, create_table_sql, numeric_precision, qualified_name};
use crate::walk;

/// Object ids for catalog entries, stable for the life of a session.
///
/// DuckDB joins its catalog functions on oids (`duckdb_tables().schema_oid =
/// duckdb_schemas().oid`), so one registry serves all of a session's functions.
#[derive(Debug, Default)]
pub struct OidRegistry {
    oids: Mutex<HashMap<Vec<String>, i64>>,
}

impl OidRegistry {
    /// The oid of the entry at `path`, e.g. `["table", catalog, schema, name]`.
    pub fn oid(&self, path: &[&str]) -> i64 {
        let mut oids = self.oids.lock().unwrap_or_else(PoisonError::into_inner);
        let next = 1000 + oids.len() as i64;
        *oids
            .entry(path.iter().map(|s| s.to_string()).collect())
            .or_insert(next)
    }
}

/// Which catalog function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogFunction {
    /// `duckdb_databases()`: one row per DataFusion catalog.
    Databases,
    /// `duckdb_schemas()`: one row per schema.
    Schemas,
    /// `duckdb_tables()`: one row per base or temporary table.
    Tables,
    /// `duckdb_views()`: one row per view.
    Views,
    /// `duckdb_columns()`: one row per column of a table or view.
    Columns,
}

impl CatalogFunction {
    /// Every catalog function.
    pub const ALL: [Self; 5] = [
        Self::Databases,
        Self::Schemas,
        Self::Tables,
        Self::Views,
        Self::Columns,
    ];

    /// The function's SQL name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Databases => "duckdb_databases",
            Self::Schemas => "duckdb_schemas",
            Self::Tables => "duckdb_tables",
            Self::Views => "duckdb_views",
            Self::Columns => "duckdb_columns",
        }
    }

    fn schema(self) -> SchemaRef {
        let utf8 = |name: &str, nullable: bool| Field::new(name, DataType::Utf8, nullable);
        let int64 = |name: &str, nullable: bool| Field::new(name, DataType::Int64, nullable);
        let boolean = |name: &str| Field::new(name, DataType::Boolean, false);
        let located = |kind: &str| {
            vec![
                utf8("database_name", false),
                int64("database_oid", false),
                utf8("schema_name", false),
                int64("schema_oid", false),
                utf8(&format!("{kind}_name"), false),
                int64(&format!("{kind}_oid"), false),
            ]
        };
        let fields = match self {
            Self::Databases => vec![
                utf8("database_name", false),
                int64("database_oid", false),
                utf8("path", true),
                utf8("comment", true),
                boolean("internal"),
                utf8("type", false),
                boolean("readonly"),
            ],
            Self::Schemas => vec![
                int64("oid", false),
                utf8("database_name", false),
                int64("database_oid", false),
                utf8("schema_name", false),
                utf8("comment", true),
                boolean("internal"),
                utf8("sql", true),
                int64("parent_schema_oid", true),
            ],
            Self::Tables => [
                located("table"),
                vec![
                    utf8("comment", true),
                    boolean("internal"),
                    boolean("temporary"),
                    boolean("has_primary_key"),
                    int64("estimated_size", true),
                    int64("column_count", false),
                    int64("index_count", false),
                    int64("check_constraint_count", false),
                    utf8("sql", true),
                ],
            ]
            .concat(),
            Self::Views => [
                located("view"),
                vec![
                    utf8("comment", true),
                    boolean("internal"),
                    boolean("temporary"),
                    int64("column_count", false),
                    utf8("sql", true),
                ],
            ]
            .concat(),
            Self::Columns => [
                located("table"),
                vec![
                    utf8("column_name", false),
                    Field::new("column_index", DataType::Int32, false),
                    utf8("comment", true),
                    boolean("internal"),
                    utf8("column_default", true),
                    boolean("is_nullable"),
                    utf8("data_type", false),
                    int64("data_type_id", true),
                    Field::new("character_maximum_length", DataType::Int32, true),
                    Field::new("numeric_precision", DataType::Int32, true),
                    Field::new("numeric_precision_radix", DataType::Int32, true),
                    Field::new("numeric_scale", DataType::Int32, true),
                ],
            ]
            .concat(),
        };
        Arc::new(Schema::new(fields))
    }
}

/// A DuckDB catalog function, over the session's catalogs.
#[derive(Debug)]
pub struct DuckDbCatalogFunction {
    function: CatalogFunction,
    oids: Arc<OidRegistry>,
}

impl DuckDbCatalogFunction {
    /// `function`, numbering entries with `oids`.
    pub fn new(function: CatalogFunction, oids: Arc<OidRegistry>) -> Self {
        Self { function, oids }
    }
}

impl TableFunctionImpl for DuckDbCatalogFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        if !args.exprs().is_empty() {
            return plan_err!("{}() takes no arguments", self.function.name());
        }
        let Some(state) = args.session().as_any().downcast_ref::<SessionState>() else {
            return plan_err!("{}() needs a SessionState", self.function.name());
        };
        let list = Arc::clone(state.catalog_list());
        let function = self.function;
        let oids = Arc::clone(&self.oids);
        let schema = function.schema();
        let batch_schema = Arc::clone(&schema);
        Ok(Arc::new(ComputedTable::new(
            function.name(),
            schema,
            move || {
                let list = Arc::clone(&list);
                let oids = Arc::clone(&oids);
                let schema = Arc::clone(&batch_schema);
                Box::pin(async move { compute(function, list.as_ref(), &oids, schema).await })
            },
        )))
    }
}

/// Rows of scalar values, turned into a batch column by column.
struct Rows {
    schema: SchemaRef,
    rows: Vec<Vec<ScalarValue>>,
}

impl Rows {
    fn push(&mut self, row: Vec<ScalarValue>) {
        self.rows.push(row);
    }

    fn finish(self) -> Result<RecordBatch> {
        let columns = (0..self.schema.fields().len())
            .map(|index| {
                let field = self.schema.field(index);
                if self.rows.is_empty() {
                    return Ok(arrow::array::new_empty_array(field.data_type()));
                }
                ScalarValue::iter_to_array(self.rows.iter().map(|row| row[index].clone()))
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        RecordBatch::try_new(self.schema, columns).map_err(DataFusionError::from)
    }
}

fn utf8(value: &str) -> ScalarValue {
    ScalarValue::Utf8(Some(value.to_string()))
}

fn null_utf8() -> ScalarValue {
    ScalarValue::Utf8(None)
}

fn int64(value: i64) -> ScalarValue {
    ScalarValue::Int64(Some(value))
}

fn int32(value: Option<u64>) -> ScalarValue {
    ScalarValue::Int32(value.and_then(|v| i32::try_from(v).ok()))
}

fn boolean(value: bool) -> ScalarValue {
    ScalarValue::Boolean(Some(value))
}

async fn compute(
    function: CatalogFunction,
    list: &dyn CatalogProviderList,
    oids: &OidRegistry,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut rows = Rows {
        schema,
        rows: Vec::new(),
    };
    let database_oid = |catalog: &str| oids.oid(&["database", catalog]);
    let schema_oid = |catalog: &str, schema: &str| oids.oid(&["schema", catalog, schema]);
    let table_oid =
        |catalog: &str, schema: &str, table: &str| oids.oid(&["table", catalog, schema, table]);

    match function {
        CatalogFunction::Databases => {
            let mut catalogs = list.catalog_names();
            catalogs.sort();
            for catalog in catalogs {
                rows.push(vec![
                    utf8(&catalog),
                    int64(database_oid(&catalog)),
                    null_utf8(),
                    null_utf8(),
                    boolean(false),
                    utf8("datafusion"),
                    boolean(false),
                ]);
            }
        }
        CatalogFunction::Schemas => {
            for (catalog, schema) in walk::schemas(list) {
                rows.push(vec![
                    int64(schema_oid(&catalog, &schema)),
                    utf8(&catalog),
                    int64(database_oid(&catalog)),
                    utf8(&schema),
                    null_utf8(),
                    boolean(false),
                    utf8(&format!(
                        "CREATE SCHEMA {};",
                        arrow_quack::quote_identifier(&schema)
                    )),
                    ScalarValue::Int64(None),
                ]);
            }
        }
        CatalogFunction::Tables | CatalogFunction::Views => {
            for table in walk::tables(list).await? {
                let table_type = table.provider.table_type();
                let is_view = table_type == TableType::View;
                if is_view != (function == CatalogFunction::Views) {
                    continue;
                }
                let table_schema = table.provider.schema();
                let located = vec![
                    utf8(&table.catalog),
                    int64(database_oid(&table.catalog)),
                    utf8(&table.schema),
                    int64(schema_oid(&table.catalog, &table.schema)),
                    utf8(&table.name),
                    int64(table_oid(&table.catalog, &table.schema, &table.name)),
                ];
                let column_count = int64(table_schema.fields().len() as i64);
                if is_view {
                    let sql = table.provider.get_table_definition().map_or_else(
                        || {
                            format!(
                                "CREATE VIEW {} AS SELECT * FROM {};",
                                qualified_name(&table.schema, &table.name),
                                qualified_name(&table.schema, &table.name)
                            )
                        },
                        str::to_string,
                    );
                    rows.push(
                        [
                            located,
                            vec![
                                null_utf8(),
                                boolean(false),
                                boolean(false),
                                column_count,
                                utf8(&sql),
                            ],
                        ]
                        .concat(),
                    );
                    continue;
                }
                let Some(sql) = create_table_sql(&table.schema, &table.name, &table_schema) else {
                    tracing::warn!(
                        catalog = %table.catalog,
                        schema = %table.schema,
                        table = %table.name,
                        "duckdb_tables() leaves out a table with a column DuckDB has no type for"
                    );
                    continue;
                };
                rows.push(
                    [
                        located,
                        vec![
                            null_utf8(),
                            boolean(false),
                            boolean(table_type == TableType::Temporary),
                            boolean(false),
                            ScalarValue::Int64(None),
                            column_count,
                            int64(0),
                            int64(0),
                            utf8(&sql),
                        ],
                    ]
                    .concat(),
                );
            }
        }
        CatalogFunction::Columns => {
            for table in walk::tables(list).await? {
                for (index, field) in table.provider.schema().fields().iter().enumerate() {
                    let (precision, radix, scale) = numeric_precision(field.data_type());
                    let type_id = arrow_quack::arrow_to_logical_type(field.data_type())
                        .ok()
                        .map(|t| t.id as i64);
                    rows.push(vec![
                        utf8(&table.catalog),
                        int64(database_oid(&table.catalog)),
                        utf8(&table.schema),
                        int64(schema_oid(&table.catalog, &table.schema)),
                        utf8(&table.name),
                        int64(table_oid(&table.catalog, &table.schema, &table.name)),
                        utf8(field.name()),
                        ScalarValue::Int32(Some(index as i32 + 1)),
                        null_utf8(),
                        boolean(false),
                        null_utf8(),
                        boolean(field.is_nullable()),
                        utf8(&column_type_name(field)),
                        ScalarValue::Int64(type_id),
                        ScalarValue::Int32(None),
                        int32(precision),
                        int32(radix),
                        int32(scale),
                    ]);
                }
            }
        }
    }
    rows.finish()
}
