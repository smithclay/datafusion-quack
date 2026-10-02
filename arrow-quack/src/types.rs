//! Arrow types as DuckDB logical types, and their DuckDB names.

use arrow::datatypes::{DataType, Field, TimeUnit};
use quack_protocol::{ChildType, LogicalType, LogicalTypes};

use crate::{Error, Result};

/// The widest DECIMAL DuckDB has.
const MAX_DECIMAL_WIDTH: u8 = 38;

/// The DuckDB logical type that values of `data_type` are encoded as.
///
/// | Arrow | DuckDB |
/// |---|---|
/// | `Null` | `INTEGER` (every value NULL) |
/// | `Boolean` | `BOOLEAN` |
/// | `Int8`..`Int64`, `UInt8`..`UInt64` | `TINYINT`..`BIGINT`, `UTINYINT`..`UBIGINT` |
/// | `Float16`, `Float32`, `Float64` | `FLOAT`, `FLOAT`, `DOUBLE` |
/// | `Decimal32/64/128(p, s)`, `p <= 38`, `s >= 0` | `DECIMAL(p, s)` |
/// | `Utf8`, `LargeUtf8`, `Utf8View` | `VARCHAR` |
/// | `Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary` | `BLOB` |
/// | `Date32`, `Date64` | `DATE` |
/// | `Time32(_)`, `Time64(µs)` | `TIME` |
/// | `Time64(ns)` | `TIME_NS` |
/// | `Timestamp(s/ms/µs/ns)` | `TIMESTAMP_S`/`TIMESTAMP_MS`/`TIMESTAMP`/`TIMESTAMP_NS` |
/// | `Timestamp(_, Some(tz))` | `TIMESTAMP WITH TIME ZONE` (microseconds) |
/// | `Interval(_)`, `Duration(_)` | `INTERVAL` |
/// | `List`, `LargeList` | `LIST` |
/// | `FixedSizeList(n)` | `ARRAY[n]` |
/// | `Struct` | `STRUCT` |
/// | `Map` | `MAP` |
/// | `Dictionary(_, v)` | the type of `v` |
///
/// A timestamp with a time zone, and a nanosecond interval or duration, keep
/// microseconds: DuckDB has no finer unit for them.
pub fn arrow_to_logical_type(data_type: &DataType) -> Result<LogicalType> {
    Ok(match data_type {
        DataType::Null => LogicalTypes::integer(),
        DataType::Boolean => LogicalTypes::boolean(),
        DataType::Int8 => LogicalTypes::tinyint(),
        DataType::Int16 => LogicalTypes::smallint(),
        DataType::Int32 => LogicalTypes::integer(),
        DataType::Int64 => LogicalTypes::bigint(),
        DataType::UInt8 => LogicalTypes::utinyint(),
        DataType::UInt16 => LogicalTypes::usmallint(),
        DataType::UInt32 => LogicalTypes::uinteger(),
        DataType::UInt64 => LogicalTypes::ubigint(),
        DataType::Float16 | DataType::Float32 => LogicalTypes::float(),
        DataType::Float64 => LogicalTypes::double(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => {
            if *precision == 0 || *precision > MAX_DECIMAL_WIDTH {
                return Err(Error::unsupported(
                    data_type,
                    format!("DuckDB DECIMAL width is 1 to {MAX_DECIMAL_WIDTH}"),
                ));
            }
            let scale = u8::try_from(*scale)
                .ok()
                .filter(|scale| scale <= precision)
                .ok_or_else(|| {
                    Error::unsupported(data_type, "DuckDB DECIMAL scale is 0 to the width")
                })?;
            LogicalTypes::decimal(u64::from(*precision), u64::from(scale))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => LogicalTypes::varchar(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => LogicalTypes::blob(),
        DataType::Date32 | DataType::Date64 => LogicalTypes::date(),
        DataType::Time32(_) | DataType::Time64(TimeUnit::Microsecond) => LogicalTypes::time(),
        DataType::Time64(TimeUnit::Nanosecond) => LogicalTypes::time_ns(),
        DataType::Time64(_) => {
            return Err(Error::unsupported(data_type, "Time64 is in µs or ns"));
        }
        DataType::Timestamp(_, Some(_)) => LogicalTypes::timestamp_tz(),
        DataType::Timestamp(TimeUnit::Second, None) => LogicalTypes::timestamp_seconds(),
        DataType::Timestamp(TimeUnit::Millisecond, None) => LogicalTypes::timestamp_millis(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => LogicalTypes::timestamp(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => LogicalTypes::timestamp_nanos(),
        DataType::Interval(_) | DataType::Duration(_) => LogicalTypes::interval(),
        DataType::List(field) | DataType::LargeList(field) => {
            LogicalTypes::list(arrow_to_logical_type(field.data_type())?)
        }
        DataType::FixedSizeList(field, size) => {
            let size = u64::try_from(*size)
                .ok()
                .filter(|size| *size > 0)
                .ok_or_else(|| Error::unsupported(data_type, "ARRAY size must be positive"))?;
            LogicalTypes::array(arrow_to_logical_type(field.data_type())?, size)
        }
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(Error::unsupported(data_type, "a STRUCT needs a field"));
            }
            LogicalTypes::r#struct(
                fields
                    .iter()
                    .map(|field| {
                        Ok(ChildType {
                            name: field.name().clone(),
                            logical_type: arrow_to_logical_type(field.data_type())?,
                        })
                    })
                    .collect::<Result<_>>()?,
            )
        }
        DataType::Map(field, _) => {
            let (key, value) = map_entry_types(data_type, field)?;
            LogicalTypes::map(arrow_to_logical_type(key)?, arrow_to_logical_type(value)?)
        }
        DataType::Dictionary(_, value) => arrow_to_logical_type(value)?,
        DataType::Decimal256(..)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Union(..)
        | DataType::RunEndEncoded(..) => {
            return Err(Error::unsupported(data_type, "no DuckDB counterpart"));
        }
    })
}

/// The key and value types of a `Map`'s entries.
pub(crate) fn map_entry_types<'a>(
    data_type: &DataType,
    entries: &'a Field,
) -> Result<(&'a DataType, &'a DataType)> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => {
            Ok((fields[0].data_type(), fields[1].data_type()))
        }
        _ => Err(Error::unsupported(
            data_type,
            "a Map's entries must be a struct of a key and a value",
        )),
    }
}

/// The DuckDB name of `field`'s type, as `information_schema.columns.data_type` and
/// `CREATE TABLE` show it: `DECIMAL(10,2)`, `TIMESTAMP_MS`, `TIMESTAMP WITH TIME ZONE`,
/// `INTEGER[]`, `STRUCT(a INTEGER, "b c" VARCHAR)`.
pub fn duckdb_type_name(field: &Field) -> Result<String> {
    type_name(field.data_type())
}

fn type_name(data_type: &DataType) -> Result<String> {
    // validate first, so the name and the encoder agree on what is supported
    arrow_to_logical_type(data_type)?;
    Ok(match data_type {
        DataType::Null | DataType::Int32 => "INTEGER".into(),
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8 => "TINYINT".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::UInt8 => "UTINYINT".into(),
        DataType::UInt16 => "USMALLINT".into(),
        DataType::UInt32 => "UINTEGER".into(),
        DataType::UInt64 => "UBIGINT".into(),
        DataType::Float16 | DataType::Float32 => "FLOAT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => format!("DECIMAL({precision},{scale})"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR".into(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BLOB".into(),
        DataType::Date32 | DataType::Date64 => "DATE".into(),
        DataType::Time64(TimeUnit::Nanosecond) => "TIME_NS".into(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".into(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP WITH TIME ZONE".into(),
        DataType::Timestamp(TimeUnit::Second, None) => "TIMESTAMP_S".into(),
        DataType::Timestamp(TimeUnit::Millisecond, None) => "TIMESTAMP_MS".into(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "TIMESTAMP_NS".into(),
        DataType::Interval(_) | DataType::Duration(_) => "INTERVAL".into(),
        DataType::List(field) | DataType::LargeList(field) => {
            format!("{}[]", type_name(field.data_type())?)
        }
        DataType::FixedSizeList(field, size) => {
            format!("{}[{size}]", type_name(field.data_type())?)
        }
        DataType::Struct(fields) => {
            let children = fields
                .iter()
                .map(|field| {
                    Ok(format!(
                        "{} {}",
                        quote_identifier(field.name()),
                        type_name(field.data_type())?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            format!("STRUCT({})", children.join(", "))
        }
        DataType::Map(field, _) => {
            let (key, value) = map_entry_types(data_type, field)?;
            format!("MAP({}, {})", type_name(key)?, type_name(value)?)
        }
        DataType::Dictionary(_, value) => type_name(value)?,
        DataType::Decimal256(..)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Union(..)
        | DataType::RunEndEncoded(..) => {
            return Err(Error::unsupported(data_type, "no DuckDB counterpart"));
        }
    })
}

/// Quotes `identifier` for DuckDB SQL when it isn't a plain lowercase name.
///
/// DuckDB folds unquoted names to lowercase, so any name with uppercase or other
/// characters, and every SQL keyword DuckDB reserves, is quoted.
pub fn quote_identifier(identifier: &str) -> String {
    let plain = identifier
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && identifier
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !is_keyword(identifier);
    if plain {
        identifier.to_string()
    } else {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }
}

/// Keywords that DuckDB can't take as a column or struct field name unquoted (its
/// `reserved` and `type_function` keywords, as of v2.0), plus the names DuckDB itself
/// quotes in `CREATE TABLE` (`"name"`, `"types"`). Sorted.
const KEYWORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "anti",
    "any",
    "array",
    "as",
    "asc",
    "asof",
    "asymmetric",
    "at",
    "authorization",
    "binary",
    "both",
    "by",
    "case",
    "cast",
    "check",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "default",
    "deferrable",
    "desc",
    "describe",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "glob",
    "grant",
    "group",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "lambda",
    "lateral",
    "leading",
    "left",
    "like",
    "limit",
    "map",
    "name",
    "natural",
    "not",
    "notnull",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "outer",
    "overlaps",
    "pivot",
    "pivot_longer",
    "pivot_wider",
    "placing",
    "positional",
    "primary",
    "qualify",
    "references",
    "returning",
    "right",
    "select",
    "semi",
    "show",
    "similar",
    "some",
    "struct",
    "summarize",
    "symmetric",
    "table",
    "tablesample",
    "then",
    "to",
    "trailing",
    "true",
    "type",
    "types",
    "union",
    "unique",
    "unpack",
    "unpivot",
    "using",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
];

fn is_keyword(identifier: &str) -> bool {
    KEYWORDS.binary_search(&identifier).is_ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Fields;
    use quack_protocol::LogicalTypeId;

    use super::*;

    fn name(data_type: DataType) -> String {
        duckdb_type_name(&Field::new("c", data_type, true)).unwrap()
    }

    #[test]
    fn keywords_are_sorted_for_the_binary_search() {
        assert!(KEYWORDS.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(is_keyword("select") && is_keyword("name") && !is_keyword("id"));
        // type_function keywords: a column named `inner` breaks DuckDB's ATTACH
        assert!(is_keyword("inner") && is_keyword("left") && is_keyword("like"));
    }

    #[test]
    fn names_match_duckdb() {
        assert_eq!(name(DataType::Int32), "INTEGER");
        assert_eq!(name(DataType::Decimal128(10, 2)), "DECIMAL(10,2)");
        assert_eq!(
            name(DataType::Timestamp(TimeUnit::Millisecond, None)),
            "TIMESTAMP_MS"
        );
        assert_eq!(
            name(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some("UTC".into())
            )),
            "TIMESTAMP WITH TIME ZONE"
        );
        assert_eq!(name(DataType::Time64(TimeUnit::Nanosecond)), "TIME_NS");
        assert_eq!(
            name(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                true
            )))),
            "INTEGER[]"
        );
        assert_eq!(
            name(DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Utf8, true)),
                3
            )),
            "VARCHAR[3]"
        );
        assert_eq!(
            name(DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("B c", DataType::Utf8, true),
            ]))),
            "STRUCT(a INTEGER, \"B c\" VARCHAR)"
        );
        assert_eq!(
            name(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Int64, true),
                    ])),
                    false
                )),
                false
            )),
            "MAP(VARCHAR, BIGINT)"
        );
    }

    #[test]
    fn logical_types() {
        assert_eq!(
            arrow_to_logical_type(&DataType::Utf8View).unwrap().id,
            LogicalTypeId::Varchar
        );
        assert_eq!(
            arrow_to_logical_type(&DataType::Dictionary(
                Box::new(DataType::Int16),
                Box::new(DataType::LargeUtf8)
            ))
            .unwrap()
            .id,
            LogicalTypeId::Varchar
        );
    }

    #[test]
    fn unsupported_types_are_errors() {
        for data_type in [
            DataType::Decimal256(40, 0),
            DataType::Decimal128(10, -2),
            DataType::Time64(TimeUnit::Second),
            DataType::Struct(Fields::empty()),
        ] {
            assert!(
                matches!(
                    arrow_to_logical_type(&data_type),
                    Err(Error::Unsupported { .. })
                ),
                "{data_type}"
            );
            assert!(name_result(data_type).is_err());
        }
    }

    fn name_result(data_type: DataType) -> Result<String> {
        duckdb_type_name(&Field::new("c", data_type, true))
    }

    #[test]
    fn identifiers_are_quoted_like_duckdb() {
        assert_eq!(quote_identifier("id"), "id");
        assert_eq!(quote_identifier("name"), "\"name\"");
        assert_eq!(quote_identifier("Mixed"), "\"Mixed\"");
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_identifier("1a"), "\"1a\"");
    }
}
