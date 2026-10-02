//! Functions DuckDB clients call that DataFusion lacks or defines differently.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{ScalarValue, exec_err, plan_err};
use datafusion::error::Result;
use datafusion::functions_aggregate::count::count_all;
use datafusion::logical_expr::function::{
    AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs,
};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// `current_database()`: the session's default catalog.
pub fn current_database_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SessionName::Database)
}

/// `current_schema()`: the session's default schema.
pub fn current_schema_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SessionName::Schema)
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum SessionName {
    Database,
    Schema,
}

impl ScalarUDFImpl for SessionName {
    fn name(&self) -> &str {
        match self {
            Self::Database => "current_database",
            Self::Schema => "current_schema",
        }
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::nullary(Volatility::Stable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let catalog = &args.config_options.catalog;
        let name = match self {
            Self::Database => catalog.default_catalog.clone(),
            Self::Schema => catalog.default_schema.clone(),
        };
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))))
    }
}

/// DuckDB's `length`: characters in a string, or elements in a list.
///
/// DataFusion's `length` takes only strings; DuckDB's `ATTACH` sorts schemas by
/// `length(schema_path)`, a list.
pub fn length_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Length {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Length {
    signature: Signature,
}

impl ScalarUDFImpl for Length {
    fn name(&self) -> &str {
        "length"
    }

    fn aliases(&self) -> &[String] {
        static ALIASES: std::sync::LazyLock<Vec<String>> =
            std::sync::LazyLock::new(|| vec!["len".to_string()]);
        &ALIASES
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(
                DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Null
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(..)
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView,
            ) => Ok(DataType::Int64),
            Some(DataType::Dictionary(_, value)) => self.return_type(&[value.as_ref().clone()]),
            other => plan_err!("length() takes a string or a list, not {other:?}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!("length() takes one argument");
        };
        let array = arg.to_array(args.number_rows)?;
        let lengths = lengths(&array)?;
        Ok(match arg {
            ColumnarValue::Scalar(_) => {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&lengths, 0)?)
            }
            ColumnarValue::Array(_) => ColumnarValue::Array(lengths),
        })
    }
}

fn lengths(array: &ArrayRef) -> Result<ArrayRef> {
    // DuckDB counts characters; arrow's length kernel counts bytes
    let chars = |value: Option<&str>| value.map(|v| v.chars().count() as i64);
    Ok(match array.data_type() {
        DataType::Null => Arc::new(Int64Array::new_null(array.len())),
        DataType::Utf8 => Arc::new(
            array
                .as_string::<i32>()
                .iter()
                .map(chars)
                .collect::<Int64Array>(),
        ),
        DataType::LargeUtf8 => Arc::new(
            array
                .as_string::<i64>()
                .iter()
                .map(chars)
                .collect::<Int64Array>(),
        ),
        DataType::Utf8View => Arc::new(
            array
                .as_string_view()
                .iter()
                .map(chars)
                .collect::<Int64Array>(),
        ),
        DataType::Dictionary(_, value) if value.is_string() => {
            lengths(&arrow::compute::cast(array, value)?)?
        }
        // bytes of a BLOB, elements of a list
        _ => arrow::compute::cast(
            &arrow::compute::kernels::length::length(array)?,
            &DataType::Int64,
        )?,
    })
}

/// DuckDB's `count_star()`, the name DuckDB gives `count(*)` when it sends a pushed
/// down aggregate. It simplifies to `count(*)` before it runs.
pub fn count_star_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(CountStar {
        signature: Signature::new(TypeSignature::Nullary, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CountStar {
    signature: Signature,
}

impl AggregateUDFImpl for CountStar {
    fn name(&self) -> &str {
        "count_star"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}_count", args.name),
            DataType::Int64,
            false,
        ))])
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        exec_err!("count_star() runs as count(*); enable the simplify_expressions optimizer rule")
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        Some(Box::new(|function, _info| {
            let mut count = count_all();
            if let datafusion::logical_expr::Expr::AggregateFunction(ref mut counted) = count {
                counted.params.filter = function.params.filter;
                counted.params.distinct = function.params.distinct;
            }
            Ok(count)
        }))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ListArray, StringArray};
    use arrow::datatypes::Int32Type;

    use super::*;

    #[test]
    fn length_counts_characters_and_list_elements() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("héllo"), None, Some("")]));
        assert_eq!(
            lengths(&strings).unwrap().as_ref(),
            &Int64Array::from(vec![Some(5), None, Some(0)]) as &dyn Array
        );
        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ]));
        assert_eq!(
            lengths(&lists).unwrap().as_ref(),
            &Int64Array::from(vec![Some(2), None]) as &dyn Array
        );
    }
}
