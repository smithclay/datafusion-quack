//! Columns described the DuckDB way.

use arrow::datatypes::{DataType, Field, Schema};
use arrow_quack::{duckdb_type_name, quote_identifier};

/// The DuckDB type name of a column. A type DuckDB has no name for keeps Arrow's,
/// so a client can still list the column and report why it can't read it.
pub(crate) fn column_type_name(field: &Field) -> String {
    duckdb_type_name(field).unwrap_or_else(|_| field.data_type().to_string())
}

/// DuckDB's `numeric_precision`, `numeric_precision_radix` and `numeric_scale`.
pub(crate) fn numeric_precision(data_type: &DataType) -> (Option<u64>, Option<u64>, Option<u64>) {
    let binary = |bits: u64| (Some(bits), Some(2), Some(0));
    match data_type {
        DataType::Int8 | DataType::UInt8 => binary(8),
        DataType::Int16 | DataType::UInt16 => binary(16),
        DataType::Int32 | DataType::UInt32 => binary(32),
        DataType::Int64 | DataType::UInt64 => binary(64),
        DataType::Float16 | DataType::Float32 => (Some(24), Some(2), None),
        DataType::Float64 => (Some(53), Some(2), None),
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s) => {
            (Some(u64::from(*p)), Some(10), u64::try_from(*s).ok())
        }
        DataType::Dictionary(_, value) => numeric_precision(value),
        _ => (None, None, None),
    }
}

/// The `CREATE TABLE` statement DuckDB's `duckdb_tables()` shows for a table, e.g.
/// `CREATE TABLE t(id BIGINT NOT NULL, "name" VARCHAR);`. A table in the `main`
/// schema is unqualified, as in DuckDB.
///
/// `None` when a column has a type DuckDB has no name for: a client couldn't parse
/// the statement.
pub(crate) fn create_table_sql(
    schema_name: &str,
    table_name: &str,
    schema: &Schema,
) -> Option<String> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let type_name = duckdb_type_name(field).ok()?;
            let not_null = if field.is_nullable() { "" } else { " NOT NULL" };
            Some(format!(
                "{} {type_name}{not_null}",
                quote_identifier(field.name())
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(format!(
        "CREATE TABLE {}({});",
        qualified_name(schema_name, table_name),
        columns.join(", ")
    ))
}

/// `name` in `schema`, qualified unless the schema is `main`.
pub(crate) fn qualified_name(schema_name: &str, name: &str) -> String {
    if schema_name == "main" {
        quote_identifier(name)
    } else {
        format!(
            "{}.{}",
            quote_identifier(schema_name),
            quote_identifier(name)
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

    use super::*;

    #[test]
    fn create_table_sql_looks_like_duckdb() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Decimal128(21, 1), true),
        ]);
        assert_eq!(
            create_table_sql("main", "t", &schema).unwrap(),
            "CREATE TABLE t(id BIGINT NOT NULL, \"name\" VARCHAR, score DECIMAL(21,1));"
        );
        assert_eq!(
            create_table_sql(
                "s2",
                "U",
                &Schema::new(vec![Field::new("x", DataType::Int32, true)])
            )
            .unwrap(),
            "CREATE TABLE s2.\"U\"(x INTEGER);"
        );
        let unsupported = Schema::new(vec![Field::new("d", DataType::Decimal256(50, 0), true)]);
        assert_eq!(create_table_sql("main", "t", &unsupported), None);
    }

    #[test]
    fn unsupported_types_keep_their_arrow_name() {
        let field = Field::new("d", DataType::Decimal256(50, 0), true);
        assert_eq!(column_type_name(&field), "Decimal256(50, 0)");
    }
}
