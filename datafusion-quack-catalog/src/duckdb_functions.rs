//! Functions as DuckDB names them in the SQL it pushes down.
//!
//! DuckDB's `ATTACH` sends a query's bound expressions back as SQL, and some come out
//! as calls to DuckDB's own functions, qualified with DuckDB's system catalog:
//! `INTERVAL 1 DAY` becomes
//! `"system".main."add"("system".main.to_days(1), "system".main.to_microseconds(0))`.
//!
//! This module answers those names: every function is also registered as
//! `"system".main.<name>`, and DuckDB's arithmetic functions (`add`, `subtract`,
//! `multiply`, `divide`, `mod`) and interval constructors (`to_days`,
//! `to_microseconds`, ...) are provided where DataFusion has none.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use arrow::array::{ArrayRef, AsArray, IntervalMonthDayNanoBuilder};
use arrow::compute::kernels::numeric;
use arrow::datatypes::{DataType, Float64Type, IntervalMonthDayNano, IntervalUnit};
use datafusion::common::{ScalarValue, exec_err, plan_err};
use datafusion::error::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion::logical_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::physical_expr_common::datum::apply;

/// The prefixes DuckDB qualifies its functions with. The function name itself is
/// quoted when DuckDB considers it a keyword (`"add"`), so both spellings are kept.
fn system_aliases(name: &str) -> [String; 2] {
    [
        format!("\"system\".main.{name}"),
        format!("\"system\".main.\"{name}\""),
    ]
}

/// DataFusion's function aliases must be `'static`. Each distinct alias is leaked
/// once per process, so the memory is bounded by the number of function names.
fn intern(alias: String) -> &'static str {
    static INTERNED: LazyLock<Mutex<HashSet<&'static str>>> =
        LazyLock::new(|| Mutex::new(HashSet::new()));
    let mut interned = INTERNED.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(existing) = interned.get(alias.as_str()) {
        return existing;
    }
    let leaked: &'static str = Box::leak(alias.into_boxed_str());
    interned.insert(leaked);
    leaked
}

/// Registers the DuckDB arithmetic and interval functions DataFusion lacks, then
/// makes every function also answer to `"system".main.<name>`.
pub(crate) fn register(state: &mut SessionState) -> Result<()> {
    for udf in [
        OperatorFunction::udf("add", Operator::Plus),
        OperatorFunction::udf("subtract", Operator::Minus),
        OperatorFunction::udf("multiply", Operator::Multiply),
        OperatorFunction::udf("divide", Operator::Divide),
        OperatorFunction::udf("mod", Operator::Modulo),
    ]
    .into_iter()
    .chain(
        IntervalConstructor::all()
            .into_iter()
            .map(ScalarUDF::new_from_impl),
    ) {
        if state.udf(udf.name()).is_err() {
            state.register_udf(Arc::new(udf))?;
        }
    }
    register_system_aliases(state)
}

fn register_system_aliases(state: &mut SessionState) -> Result<()> {
    let scalars: HashMap<String, Arc<ScalarUDF>> = state
        .scalar_functions()
        .values()
        .map(|udf| (udf.name().to_string(), Arc::clone(udf)))
        .collect();
    for (name, udf) in scalars {
        let aliases = system_aliases(&name).map(intern);
        state.register_udf(Arc::new(udf.as_ref().clone().with_aliases(aliases)))?;
    }
    let aggregates: HashMap<String, _> = state
        .aggregate_functions()
        .values()
        .map(|udaf| (udaf.name().to_string(), Arc::clone(udaf)))
        .collect();
    for (name, udaf) in aggregates {
        let aliases = system_aliases(&name).map(intern);
        state.register_udaf(Arc::new(udaf.as_ref().clone().with_aliases(aliases)))?;
    }
    let windows: HashMap<String, _> = state
        .window_functions()
        .values()
        .map(|udwf| (udwf.name().to_string(), Arc::clone(udwf)))
        .collect();
    for (name, udwf) in windows {
        let aliases = system_aliases(&name).map(intern);
        state.register_udwf(Arc::new(udwf.as_ref().clone().with_aliases(aliases)))?;
    }
    Ok(())
}

/// A binary operator as a function, e.g. DuckDB's `add(a, b)` for `a + b`. Arguments
/// are coerced as the operator coerces them.
#[derive(Debug, PartialEq, Eq, Hash)]
struct OperatorFunction {
    name: &'static str,
    op: Operator,
    signature: Signature,
}

impl OperatorFunction {
    fn udf(name: &'static str, op: Operator) -> ScalarUDF {
        ScalarUDF::new_from_impl(Self {
            name,
            op,
            signature: Signature::user_defined(Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for OperatorFunction {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = arg_types else {
            return plan_err!("{}() takes two arguments", self.name);
        };
        let (left, right) = BinaryTypeCoercer::new(left, &self.op, right).get_input_types()?;
        Ok(vec![left, right])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [left, right] = arg_types else {
            return plan_err!("{}() takes two arguments", self.name);
        };
        BinaryTypeCoercer::new(left, &self.op, right).get_result_type()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return exec_err!("{}() takes two arguments", self.name);
        };
        let kernel = match self.op {
            Operator::Plus => numeric::add,
            Operator::Minus => numeric::sub,
            Operator::Multiply => numeric::mul,
            Operator::Divide => numeric::div,
            Operator::Modulo => numeric::rem,
            other => return exec_err!("{other} is not an arithmetic operator"),
        };
        apply(left, right, kernel)
    }
}

/// DuckDB's `to_years(n)` .. `to_microseconds(n)`: an interval of `n` units.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IntervalConstructor {
    name: &'static str,
    field: IntervalField,
    /// Units of `field` in one of the function's units.
    factor: u64,
    signature: Signature,
}

/// The interval field a unit counts in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum IntervalField {
    Months,
    Days,
    Micros,
}

impl IntervalConstructor {
    fn all() -> Vec<Self> {
        use IntervalField::*;
        [
            ("to_millennia", Months, 12_000),
            ("to_centuries", Months, 1_200),
            ("to_decades", Months, 120),
            ("to_years", Months, 12),
            ("to_quarters", Months, 3),
            ("to_months", Months, 1),
            ("to_weeks", Days, 7),
            ("to_days", Days, 1),
            ("to_hours", Micros, 3_600_000_000),
            ("to_minutes", Micros, 60_000_000),
            ("to_seconds", Micros, 1_000_000),
            ("to_milliseconds", Micros, 1_000),
            ("to_microseconds", Micros, 1),
        ]
        .into_iter()
        .map(|(name, field, factor)| Self {
            name,
            field,
            factor,
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        })
        .collect()
    }

    /// The interval of `n` units. Months and days must come out whole; microseconds
    /// round, so fractional seconds keep their microseconds, as DuckDB's do.
    fn interval(&self, n: f64) -> Option<IntervalMonthDayNano> {
        let units = n * self.factor as f64;
        let whole = (units.fract() == 0.0 && units.abs() < 2e9).then_some(units as i32);
        Some(match self.field {
            IntervalField::Months => IntervalMonthDayNano::new(whole?, 0, 0),
            IntervalField::Days => IntervalMonthDayNano::new(0, whole?, 0),
            IntervalField::Micros => {
                let micros = units.round();
                (micros.abs() < 9.2e15).then_some(())?;
                IntervalMonthDayNano::new(0, 0, micros as i64 * 1_000)
            }
        })
    }
}

impl ScalarUDFImpl for IntervalConstructor {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(t) if t.is_numeric() || t.is_null() => {
                Ok(DataType::Interval(IntervalUnit::MonthDayNano))
            }
            other => plan_err!("{}() takes a number, not {other:?}", self.name),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!("{}() takes one argument", self.name);
        };
        let scalar = matches!(arg, ColumnarValue::Scalar(_));
        let array = arg.to_array(if scalar { 1 } else { args.number_rows })?;
        let values = arrow::compute::cast(&array, &DataType::Float64)?;
        let values = values.as_primitive::<Float64Type>();
        let mut builder = IntervalMonthDayNanoBuilder::with_capacity(values.len());
        for value in values.iter() {
            match value {
                None => builder.append_null(),
                Some(n) => match self.interval(n) {
                    Some(interval) => builder.append_value(interval),
                    None => return exec_err!("{}({n}) is out of range", self.name),
                },
            }
        }
        let result: ArrayRef = Arc::new(builder.finish());
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?)
        } else {
            ColumnarValue::Array(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use super::*;

    async fn value(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        arrow::util::display::array_value_to_string(batches[0].column(0), 0).unwrap()
    }

    #[tokio::test]
    async fn duckdb_interval_sql_plans() {
        let mut state = SessionContext::new().state();
        register(&mut state).unwrap();
        let ctx = SessionContext::new_with_state(state);
        // as DuckDB renders INTERVAL 1 DAY
        assert_eq!(
            value(
                &ctx,
                r#"SELECT DATE '2024-01-01' + "system".main."add"("system".main."add"("system".main.to_months(0), "system".main.to_days(1)), "system".main.to_microseconds(CAST(0 AS BIGINT)))"#
            )
            .await,
            "2024-01-02"
        );
        assert_eq!(
            value(&ctx, "SELECT to_seconds(1.5) = INTERVAL '1.5 seconds'").await,
            "true"
        );
        assert_eq!(
            value(&ctx, r#"SELECT "system".main."multiply"(3, 4)"#).await,
            "12"
        );
        assert_eq!(value(&ctx, r#"SELECT "system".main.upper('x')"#).await, "X");
        assert_eq!(
            value(
                &ctx,
                r#"SELECT "system".main.sum(x) FROM (VALUES (1), (2)) t(x)"#
            )
            .await,
            "3"
        );
    }

    #[test]
    fn intervals_of_each_unit() {
        let c = |name: &str| {
            IntervalConstructor::all()
                .into_iter()
                .find(|c| c.name == name)
                .unwrap()
        };
        assert_eq!(
            c("to_years").interval(2.0),
            Some(IntervalMonthDayNano::new(24, 0, 0))
        );
        assert_eq!(
            c("to_weeks").interval(1.0),
            Some(IntervalMonthDayNano::new(0, 7, 0))
        );
        assert_eq!(
            c("to_milliseconds").interval(1.5),
            Some(IntervalMonthDayNano::new(0, 0, 1_500_000))
        );
        assert_eq!(c("to_days").interval(1.5), None);
    }
}
