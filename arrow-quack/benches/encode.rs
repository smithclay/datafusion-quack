//! Encoder throughput, to track the cost of the Arrow to DataChunk path.
#![allow(missing_docs, clippy::expect_used)]

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

fn batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            Arc::new(Float64Array::from_iter(
                (0..rows).map(|i| (i % 7 != 0).then_some(i as f64 * 1.5)),
            )),
            Arc::new(StringArray::from_iter(
                (0..rows).map(|i| (i % 5 != 0).then(|| format!("name_{i}"))),
            )),
        ],
    )
    .expect("batch")
}

fn encode(c: &mut Criterion) {
    let batch = batch(8192);
    let mut group = c.benchmark_group("encode_record_batch");
    group.throughput(Throughput::Elements(batch.num_rows() as u64));
    group.bench_function("int64_float64_utf8_8192", |b| {
        b.iter(|| arrow_quack::encode_record_batch(&batch).expect("encode"))
    });
    group.finish();
}

criterion_group!(benches, encode);
criterion_main!(benches);
