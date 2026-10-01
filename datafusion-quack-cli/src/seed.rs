//! Fixture tables the server can preload (`--seed`).
//!
//! `provider-fixtures` holds the tables the `datafusion-table-providers` Quack suite
//! reads in seeded mode (`QUACK_SEED_MODE=1`): the same rows its tests create on a
//! DuckDB server, without the DuckDB-only types (HUGEINT, ENUM, collations, UUID,
//! VARIANT, ...).

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion::common::TableReference;

/// A named set of fixtures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Seed {
    /// The tables of the provider suite's seeded mode.
    ProviderFixtures,
}

/// Registers `seed`'s tables in `ctx`'s default schema.
pub async fn load(ctx: &SessionContext, seed: Seed) -> Result<()> {
    match seed {
        Seed::ProviderFixtures => provider_fixtures(ctx).await,
    }
}

fn register(ctx: &SessionContext, name: &str, batch: RecordBatch) -> Result<()> {
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    // bare, so a mixed-case name keeps its case
    ctx.register_table(TableReference::bare(name), Arc::new(table))?;
    Ok(())
}

fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}

async fn provider_fixtures(ctx: &SessionContext) -> Result<()> {
    register(ctx, "quack_types", types()?)?;
    register(ctx, "quack_nullability", nullability()?)?;
    register(ctx, "quack_pushdown", pushdown()?)?;

    let ids = |n: i64| Arc::new(Int64Array::from_iter_values(0..n)) as ArrayRef;
    register(
        ctx,
        "quack_orders",
        batch(
            vec![
                Field::new("id", DataType::Int64, true),
                Field::new("customer_id", DataType::Int64, true),
                Field::new("amount", DataType::Int64, true),
            ],
            vec![
                ids(9),
                Arc::new(Int64Array::from_iter_values((0..9).map(|x| x % 3))),
                Arc::new(Int64Array::from_iter_values((0..9).map(|x| x * 10))),
            ],
        )?,
    )?;
    register(
        ctx,
        "quack_customers",
        batch(
            vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
            ],
            vec![ids(3), Arc::new(StringArray::from(vec!["ann", "bo", "cy"]))],
        )?,
    )?;
    let k = |n: i64| batch(vec![Field::new("k", DataType::Int64, true)], vec![ids(n)]);
    register(ctx, "quack_runtime_filters", k(1000)?)?;
    register(ctx, "quack_exhaust", k(1_000_000)?)?;
    register(
        ctx,
        "quack_overflow",
        batch(
            vec![Field::new("v", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![i64::MAX, i64::MAX]))],
        )?,
    )?;
    register(
        ctx,
        "MixedCase_seed",
        batch(
            vec![Field::new("id", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![7]))],
        )?,
    )?;
    ctx.sql(r#"CREATE VIEW quack_view AS SELECT id FROM "MixedCase_seed""#)
        .await?
        .collect()
        .await?;
    register(
        ctx,
        "quack_ext_a",
        batch(vec![Field::new("id", DataType::Int64, true)], vec![ids(3)])?,
    )?;
    register(
        ctx,
        "quack_ext_b",
        batch(
            vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
            ],
            vec![ids(3), Arc::new(StringArray::from(vec!["n0", "n1", "n2"]))],
        )?,
    )?;
    register(
        ctx,
        "quack_no_runtime",
        batch(
            vec![Field::new("answer", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![42]))],
        )?,
    )?;
    Ok(())
}

/// One row of every type of the MVP type set, then a row of NULLs.
fn types() -> Result<RecordBatch> {
    let item = |data_type: DataType| Arc::new(Field::new("item", data_type, true));
    let struct_fields = Fields::from(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let both = Some(NullBuffer::from(vec![true, false]));

    let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    map.keys().append_value("k1");
    map.values().append_value(1);
    map.keys().append_value("k2");
    map.values().append_null();
    map.append(true)?;
    map.append(false)?;

    let columns: Vec<(&str, ArrayRef)> = vec![
        ("c_bool", Arc::new(BooleanArray::from(vec![Some(true), None]))),
        ("c_i8", Arc::new(Int8Array::from(vec![Some(i8::MIN), None]))),
        ("c_i16", Arc::new(Int16Array::from(vec![Some(i16::MIN), None]))),
        ("c_i32", Arc::new(Int32Array::from(vec![Some(i32::MIN), None]))),
        ("c_i64", Arc::new(Int64Array::from(vec![Some(i64::MIN), None]))),
        ("c_u8", Arc::new(UInt8Array::from(vec![Some(u8::MAX), None]))),
        ("c_u16", Arc::new(UInt16Array::from(vec![Some(u16::MAX), None]))),
        ("c_u32", Arc::new(UInt32Array::from(vec![Some(u32::MAX), None]))),
        ("c_u64", Arc::new(UInt64Array::from(vec![Some(u64::MAX), None]))),
        ("c_f32", Arc::new(Float32Array::from(vec![Some(1.5), None]))),
        ("c_f64", Arc::new(Float64Array::from(vec![Some(-2.25), None]))),
        (
            "c_dec4",
            Arc::new(Decimal128Array::from(vec![Some(1234), None]).with_precision_and_scale(4, 2)?),
        ),
        (
            "c_dec18",
            Arc::new(
                Decimal128Array::from(vec![Some(-123_456_789_012_345_678), None])
                    .with_precision_and_scale(18, 3)?,
            ),
        ),
        (
            "c_dec38",
            Arc::new(
                Decimal128Array::from(vec![Some(12_345_678_901_234_567_890_123_456_780_123_456_789), None])
                    .with_precision_and_scale(38, 10)?,
            ),
        ),
        ("c_varchar", Arc::new(StringArray::from(vec![Some("héllo 'q'"), None]))),
        (
            "c_blob",
            Arc::new(BinaryArray::from(vec![Some(&[0u8, 255][..]), None])),
        ),
        ("c_date", Arc::new(Date32Array::from(vec![Some(1), None]))),
        (
            "c_time",
            Arc::new(Time64MicrosecondArray::from(vec![Some(86_399_999_999), None])),
        ),
        (
            "c_time_ns",
            Arc::new(Time64NanosecondArray::from(vec![Some(3_723_123_456_789), None])),
        ),
        (
            "c_ts",
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_709_210_096_789_012), None])),
        ),
        (
            "c_ts_s",
            Arc::new(TimestampSecondArray::from(vec![Some(1_709_210_096), None])),
        ),
        (
            "c_ts_ms",
            Arc::new(TimestampMillisecondArray::from(vec![Some(1_709_210_096_789), None])),
        ),
        (
            "c_ts_ns",
            Arc::new(TimestampNanosecondArray::from(vec![Some(1_709_210_096_123_456_789), None])),
        ),
        (
            "c_tstz",
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1_709_192_096_000_000), None])
                    .with_timezone("UTC"),
            ),
        ),
        (
            "c_interval",
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                Some(IntervalMonthDayNano::new(14, 3, 14_706_000_007_000)),
                None,
            ])),
        ),
        (
            "c_list",
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), None, Some(3)]),
                None,
            ])),
        ),
        (
            "c_list_str",
            Arc::new(ListArray::new(
                item(DataType::Utf8),
                OffsetBuffer::from_lengths([2, 0]),
                Arc::new(StringArray::from(vec![Some("x"), None])),
                both.clone(),
            )),
        ),
        (
            "c_struct",
            Arc::new(StructArray::new(
                struct_fields,
                vec![
                    Arc::new(Int32Array::from(vec![Some(1), None])),
                    Arc::new(StringArray::from(vec![Some("z"), None])),
                ],
                both,
            )),
        ),
        ("c_map", Arc::new(map.finish())),
        (
            "c_array",
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![Some(vec![Some(7), Some(8), Some(9)]), None],
                3,
            )),
        ),
    ];
    let fields = columns
        .iter()
        .map(|(name, column)| Field::new(*name, column.data_type().clone(), true))
        .collect();
    batch(fields, columns.into_iter().map(|(_, column)| column).collect())
}

fn nullability() -> Result<RecordBatch> {
    batch(
        vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), false),
        ],
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec![None, Some("n")])),
            Arc::new(Decimal128Array::from(vec![125, 250]).with_precision_and_scale(10, 2)?),
        ],
    )
}

/// The provider's pushdown rows, without the ENUM and HUGEINT columns.
fn pushdown() -> Result<RecordBatch> {
    const DAY: i64 = 86_400;
    let date = |days: i32| Some(days);
    // 2024-01-01, 2024-06-30, 2024-12-31, 1999-01-01 as days and seconds since the epoch
    let (jan, jun, dec, y99) = (19_723, 19_904, 20_088, 10_592);
    let secs = |days: i64| days * DAY;
    batch(
        vec![
            Field::new("id", DataType::Int32, true),
            Field::new("i", DataType::Int32, true),
            Field::new("u", DataType::UInt64, true),
            Field::new("dec", DataType::Decimal128(10, 2), true),
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new("ts_ns", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "tstz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("b", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, true),
        ],
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None, Some(5)])),
            Arc::new(UInt64Array::from(vec![Some(1), Some(u64::MAX), Some(5), None, Some(0)])),
            Arc::new(
                Decimal128Array::from(vec![Some(150), Some(-225), Some(0), None, Some(9999)])
                    .with_precision_and_scale(10, 2)?,
            ),
            Arc::new(Date32Array::from(vec![
                date(jan),
                date(jun),
                date(dec),
                None,
                date(y99),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(secs(jan as i64) * 1_000_000),
                Some((secs(jun as i64) + 12 * 3600) * 1_000_000 + 500_000),
                Some((secs(dec as i64) + DAY - 1) * 1_000_000 + 999_999),
                None,
                Some(secs(y99 as i64) * 1_000_000),
            ])),
            Arc::new(TimestampSecondArray::from(vec![
                Some(secs(jan as i64)),
                Some(secs(jun as i64) + 12 * 3600),
                Some(secs(dec as i64) + DAY - 1),
                None,
                Some(secs(y99 as i64)),
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(secs(jan as i64) * 1_000_000_000 + 1),
                Some((secs(jun as i64) + 12 * 3600) * 1_000_000_000 + 123_456_789),
                Some((secs(dec as i64) + DAY - 1) * 1_000_000_000 + 999_999_999),
                None,
                Some(secs(y99 as i64) * 1_000_000_000),
            ])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    Some(secs(jan as i64) * 1_000_000),
                    Some((secs(jun as i64) + 10 * 3600) * 1_000_000),
                    Some((secs(dec as i64) + DAY - 1) * 1_000_000),
                    None,
                    Some(secs(y99 as i64) * 1_000_000),
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(false),
            ])),
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some("X"),
                Some("y"),
                None,
                Some("Xylophone"),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(-0.0),
                Some(0.0),
                None,
                Some(2.5),
            ])),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn provider_fixtures_load() {
        let ctx = SessionContext::new();
        load(&ctx, Seed::ProviderFixtures).await.unwrap();
        let count = ctx
            .sql("SELECT count(*) FROM quack_exhaust")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(count[0].num_rows(), 1);
        let types = ctx.table("quack_types").await.unwrap().collect().await.unwrap();
        assert_eq!(types[0].num_rows(), 2);
    }
}
