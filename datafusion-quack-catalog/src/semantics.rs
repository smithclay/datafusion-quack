//! DuckDB's result types, for sessions of DuckDB clients.
//!
//! DuckDB's `ATTACH` pushes whole queries to the server and shows the results as
//! they come back, so they should have the values and types DuckDB itself would
//! give. Where DuckDB and DataFusion differ, a planner extension and two aggregate
//! overrides apply DuckDB's rules:
//!
//! | Expression | DataFusion | DuckDB (applied here) |
//! |---|---|---|
//! | integer or decimal `/` integer or decimal | integer / decimal | `DOUBLE` |
//! | `DATE ± INTERVAL` | `DATE` | `TIMESTAMP` |
//! | `DATE ± integer` | error | `DATE` (days) |
//! | `DATE - DATE` | interval | `BIGINT` (days) |
//! | integer `//` integer | not supported | integer, truncated toward zero |
//! | `avg(DECIMAL)` | `DECIMAL(p+4, s+4)` | `DOUBLE` |
//!
//! These are DuckDB's semantics, not DataFusion's, so they belong only in sessions
//! of DuckDB clients. A DataFusion client (the Quack table provider) plans with
//! DataFusion's rules and expects its pushed-down SQL to keep them.
//!
//! One rule applies to every session ([`wide_sum_udaf`]): `sum` of integers returns
//! `DECIMAL(38,0)`, as DuckDB returns `HUGEINT`, rather than wrapping on overflow. A
//! client that expects a narrower type (the table provider casts results to its plan's
//! types) then gets an error instead of a wrong sum.

use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::error::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, ExprSchemable, GroupsAccumulator, Operator,
    ReversedUDAF, Signature, Volatility, binary_expr, cast,
};
use datafusion::sql::sqlparser::ast::BinaryOperator;

/// `state`, with DuckDB's result types (see the module docs).
pub fn duckdb_client_semantics(state: SessionState) -> Result<SessionState> {
    let mut planners: Vec<Arc<dyn ExprPlanner>> = vec![Arc::new(DuckDbExprPlanner::new(&state))];
    planners.extend(state.expr_planners().iter().cloned());
    let mut state = SessionStateBuilder::new_from_existing(state)
        .with_expr_planners(planners)
        .build();
    // DuckDB averages a DECIMAL as a DOUBLE
    if let Some(avg) = state.aggregate_functions().get("avg").cloned() {
        state.register_udaf(Arc::new(coerced(avg, |t| {
            t.is_decimal().then_some(DataType::Float64)
        })))?;
    }
    Ok(state)
}

/// `sum` whose integer arguments are summed as `DECIMAL(38,0)`, so the sum can't wrap.
pub fn wide_sum_udaf(sum: Arc<AggregateUDF>) -> AggregateUDF {
    coerced(sum, |t| {
        t.is_integer().then_some(DataType::Decimal128(38, 0))
    })
}

fn coerced(inner: Arc<AggregateUDF>, coerce: fn(&DataType) -> Option<DataType>) -> AggregateUDF {
    AggregateUDF::new_from_impl(Coerced {
        inner,
        signature: Signature::user_defined(Volatility::Immutable),
        coerce,
    })
}

/// Plans `/` and date arithmetic with DuckDB's result types.
#[derive(Debug)]
pub struct DuckDbExprPlanner {
    to_days: Option<Arc<datafusion::logical_expr::ScalarUDF>>,
}

impl DuckDbExprPlanner {
    /// A planner using `state`'s `to_days`.
    pub fn new(state: &SessionState) -> Self {
        Self {
            to_days: state.scalar_functions().get("to_days").cloned(),
        }
    }

    fn days(&self, days: Expr) -> Option<Expr> {
        let to_days = self.to_days.as_ref()?;
        Some(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(to_days),
            vec![days],
        )))
    }
}

fn is_exact_number(data_type: &DataType) -> bool {
    data_type.is_integer() || data_type.is_decimal() || data_type.is_null()
}

const TIMESTAMP: DataType = DataType::Timestamp(TimeUnit::Microsecond, None);

impl ExprPlanner for DuckDbExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let (Ok(left), Ok(right)) = (expr.left.get_type(schema), expr.right.get_type(schema))
        else {
            return Ok(PlannerResult::Original(expr));
        };
        let op = match expr.op {
            BinaryOperator::Plus => Operator::Plus,
            BinaryOperator::Minus => Operator::Minus,
            BinaryOperator::Divide => Operator::Divide,
            // DuckDB's `//` truncates toward zero, as DataFusion's integer division does
            BinaryOperator::DuckIntegerDivide if left.is_integer() && right.is_integer() => {
                return Ok(PlannerResult::Planned(binary_expr(
                    expr.left,
                    Operator::Divide,
                    expr.right,
                )));
            }
            _ => return Ok(PlannerResult::Original(expr)),
        };
        let interval = |t: &DataType| matches!(t, DataType::Interval(_));
        let planned = match (op, &left, &right) {
            (Operator::Divide, l, r) if is_exact_number(l) && is_exact_number(r) => binary_expr(
                cast(expr.left.clone(), DataType::Float64),
                op,
                cast(expr.right.clone(), DataType::Float64),
            ),
            (Operator::Plus | Operator::Minus, DataType::Date32, r) if interval(r) => {
                binary_expr(cast(expr.left.clone(), TIMESTAMP), op, expr.right.clone())
            }
            (Operator::Plus, l, DataType::Date32) if interval(l) => {
                binary_expr(expr.left.clone(), op, cast(expr.right.clone(), TIMESTAMP))
            }
            (Operator::Plus | Operator::Minus, DataType::Date32, r) if r.is_integer() => {
                let Some(days) = self.days(expr.right.clone()) else {
                    return Ok(PlannerResult::Original(expr));
                };
                binary_expr(expr.left.clone(), op, days)
            }
            (Operator::Plus, l, DataType::Date32) if l.is_integer() => {
                let Some(days) = self.days(expr.left.clone()) else {
                    return Ok(PlannerResult::Original(expr));
                };
                binary_expr(expr.right.clone(), op, days)
            }
            (Operator::Minus, DataType::Date32, DataType::Date32) => {
                let days = |e: Expr| cast(cast(e, DataType::Int32), DataType::Int64);
                binary_expr(days(expr.left.clone()), op, days(expr.right.clone()))
            }
            _ => return Ok(PlannerResult::Original(expr)),
        };
        Ok(PlannerResult::Planned(planned))
    }
}

/// An aggregate that coerces some argument types before its inner function sees them,
/// e.g. DuckDB's `avg`, which averages a DECIMAL as a DOUBLE.
#[derive(Debug)]
struct Coerced {
    inner: Arc<AggregateUDF>,
    signature: Signature,
    coerce: fn(&DataType) -> Option<DataType>,
}

impl PartialEq for Coerced {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner && std::ptr::fn_addr_eq(self.coerce, other.coerce)
    }
}

impl Eq for Coerced {}

impl std::hash::Hash for Coerced {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl AggregateUDFImpl for Coerced {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let coerced: Vec<DataType> = arg_types
            .iter()
            .map(|t| (self.coerce)(t).unwrap_or_else(|| t.clone()))
            .collect();
        datafusion::logical_expr::type_coercion::functions::fields_with_udf(
            &coerced
                .iter()
                .map(|t| Arc::new(arrow::datatypes::Field::new("arg", t.clone(), true)))
                .collect::<Vec<FieldRef>>(),
            self.inner.as_ref(),
        )
        .map(|fields| fields.iter().map(|f| f.data_type().clone()).collect())
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        self.inner.return_field(arg_fields)
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        self.inner.create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.create_sliding_accumulator(args)
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn default_value(&self, data_type: &DataType) -> Result<datafusion::common::ScalarValue> {
        self.inner.default_value(data_type)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use super::*;

    async fn typed(ctx: &SessionContext, sql: &str) -> (String, String) {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let column = batches[0].column(0);
        (
            column.data_type().to_string(),
            arrow::util::display::array_value_to_string(column, 0).unwrap(),
        )
    }

    async fn duckdb_ctx() -> SessionContext {
        let state = crate::duckdb_session_state(SessionContext::new().state()).unwrap();
        SessionContext::new_with_state(duckdb_client_semantics(state).unwrap())
    }

    #[tokio::test]
    async fn results_have_duckdb_types() {
        let ctx = duckdb_ctx().await;
        assert_eq!(
            typed(&ctx, "SELECT 5 / 2").await,
            ("Float64".into(), "2.5".into())
        );
        assert_eq!(
            typed(
                &ctx,
                "SELECT CAST(1.5 AS DECIMAL(10,2)) / CAST(2 AS DECIMAL(10,2))"
            )
            .await,
            ("Float64".into(), "0.75".into())
        );
        assert_eq!(typed(&ctx, "SELECT 7 // 2").await.1, "3");
        assert_eq!(
            typed(&ctx, "SELECT avg(x) FROM (VALUES (CAST(1.5 AS DECIMAL(10,2))), (CAST(2 AS DECIMAL(10,2)))) t(x)").await,
            ("Float64".into(), "1.75".into())
        );
        assert_eq!(
            typed(
                &ctx,
                "SELECT sum(x) FROM (VALUES (9223372036854775807), (9223372036854775807)) t(x)"
            )
            .await,
            ("Decimal128(38, 0)".into(), "18446744073709551614".into())
        );
        assert_eq!(
            typed(&ctx, "SELECT DATE '2024-01-01' + INTERVAL '1' DAY").await,
            ("Timestamp(µs)".into(), "2024-01-02T00:00:00".into())
        );
        assert_eq!(
            typed(&ctx, "SELECT DATE '2024-01-01' + 31").await,
            ("Date32".into(), "2024-02-01".into())
        );
        assert_eq!(
            typed(&ctx, "SELECT DATE '2024-03-01' - DATE '2024-01-01'").await,
            ("Int64".into(), "60".into())
        );
        // other aggregates and operators are untouched
        assert_eq!(typed(&ctx, "SELECT 5 * 2").await.0, "Int64");
        assert_eq!(
            typed(&ctx, "SELECT avg(x) FROM (VALUES (1), (2)) t(x)")
                .await
                .1,
            "1.5"
        );
    }
}
