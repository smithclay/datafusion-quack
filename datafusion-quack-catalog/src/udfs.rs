//! Functions DuckDB clients call that DataFusion lacks or defines differently.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, Int64Builder};
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
    fn collect(len: usize, mut value: impl FnMut(usize) -> Option<i64>) -> ArrayRef {
        let mut builder = Int64Builder::with_capacity(len);
        for index in 0..len {
            builder.append_option(value(index));
        }
        Arc::new(builder.finish())
    }
    let valid = |index: usize| array.is_valid(index);
    let len = array.len();
    Ok(match array.data_type() {
        DataType::Null => Arc::new(Int64Array::new_null(len)),
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            collect(len, |i| valid(i).then(|| a.value(i).chars().count() as i64))
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            collect(len, |i| valid(i).then(|| a.value(i).chars().count() as i64))
        }
        DataType::Utf8View => {
            let a = array.as_string_view();
            collect(len, |i| valid(i).then(|| a.value(i).chars().count() as i64))
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            collect(len, |i| valid(i).then(|| a.value(i).len() as i64))
        }
        DataType::LargeBinary => {
            let a = array.as_binary::<i64>();
            collect(len, |i| valid(i).then(|| a.value(i).len() as i64))
        }
        DataType::BinaryView => {
            let a = array.as_binary_view();
            collect(len, |i| valid(i).then(|| a.value(i).len() as i64))
        }
        DataType::List(_) => {
            let a = array.as_list::<i32>();
            collect(len, |i| valid(i).then(|| a.value_length(i) as i64))
        }
        DataType::LargeList(_) => {
            let a = array.as_list::<i64>();
            collect(len, |i| valid(i).then(|| a.value_length(i)))
        }
        DataType::FixedSizeList(_, size) => collect(len, |i| valid(i).then_some(*size as i64)),
        DataType::Dictionary(_, value) => {
            let values = arrow::compute::cast(array, value)?;
            return lengths(&values);
        }
        other => return exec_err!("length() takes a string or a list, not {other}"),
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
