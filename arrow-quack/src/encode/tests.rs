//! Round trips through `quack_protocol`'s decoder (gate G4).
//!
//! Every test encodes Arrow arrays, decodes the bytes with the client's
//! `DataChunk` decoder, and compares the decoded values with the values an
//! independent conversion reads from the Arrow arrays.

use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::*;
use indexmap::IndexMap;
use proptest::prelude::*;
use quack_protocol::server::{BinaryReader, decode_data_chunk};
use quack_protocol::{
    DataChunk, DateValue, DecimalValue, IntervalValue, TimeUnit as QTimeUnit, TimeValue,
    TimestampUnit, TimestampValue, Value,
};

use super::*;

fn decode(chunk: &EncodedChunk) -> DataChunk {
    let mut reader = BinaryReader::new(&chunk.bytes);
    let decoded = decode_data_chunk(&mut reader).expect("decodes");
    reader.assert_eof().expect("no trailing bytes");
    assert_eq!(decoded.row_count, chunk.rows);
    decoded
}

/// Encodes `batch`, decodes every chunk, and returns each column's values.
fn round_trip(batch: &RecordBatch) -> Vec<Vec<Value>> {
    let chunks = encode_record_batch(batch).expect("encodes");
    let mut columns = vec![Vec::new(); batch.num_columns()];
    for chunk in &chunks {
        assert!(chunk.rows <= STANDARD_VECTOR_SIZE);
        let decoded = decode(chunk);
        for (index, column) in columns.iter_mut().enumerate() {
            column.extend_from_slice(decoded.column_values(index).expect("column"));
        }
    }
    columns
}

fn expected(array: &dyn Array) -> Vec<Value> {
    (0..array.len()).map(|row| value(array, row)).collect()
}

/// The value DuckDB holds for `array[row]`, read independently of the encoder.
fn value(array: &dyn Array, row: usize) -> Value {
    if array.data_type() == &DataType::Null || array.is_null(row) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => Value::Bool(array.as_boolean().value(row)),
        DataType::Int8 => Value::Int(array.as_primitive::<Int8Type>().value(row).into()),
        DataType::Int16 => Value::Int(array.as_primitive::<Int16Type>().value(row).into()),
        DataType::Int32 => Value::Int(array.as_primitive::<Int32Type>().value(row).into()),
        DataType::Int64 => Value::Int(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => Value::UInt(array.as_primitive::<UInt8Type>().value(row).into()),
        DataType::UInt16 => Value::UInt(array.as_primitive::<UInt16Type>().value(row).into()),
        DataType::UInt32 => Value::UInt(array.as_primitive::<UInt32Type>().value(row).into()),
        DataType::UInt64 => Value::UInt(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => Value::Float(array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => Value::Double(array.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal128(p, s) => Value::Decimal(DecimalValue {
            value: array.as_primitive::<Decimal128Type>().value(row),
            width: u64::from(*p),
            scale: *s as u64,
        }),
        DataType::Utf8 => Value::String(array.as_string::<i32>().value(row).into()),
        DataType::LargeUtf8 => Value::String(array.as_string::<i64>().value(row).into()),
        DataType::Utf8View => Value::String(array.as_string_view().value(row).into()),
        DataType::Binary => Value::Bytes(array.as_binary::<i32>().value(row).into()),
        DataType::BinaryView => Value::Bytes(array.as_binary_view().value(row).into()),
        DataType::Date32 => Value::Date(DateValue {
            days: array.as_primitive::<Date32Type>().value(row),
        }),
        DataType::Time64(TimeUnit::Microsecond) => Value::Time(TimeValue {
            unit: QTimeUnit::Micros,
            value: array.as_primitive::<Time64MicrosecondType>().value(row),
        }),
        DataType::Timestamp(unit, tz) => {
            let raw = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
                TimeUnit::Millisecond => {
                    array.as_primitive::<TimestampMillisecondType>().value(row)
                }
                TimeUnit::Microsecond => {
                    array.as_primitive::<TimestampMicrosecondType>().value(row)
                }
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
            };
            if tz.is_some() {
                let micros = match unit {
                    TimeUnit::Second => raw * 1_000_000,
                    TimeUnit::Millisecond => raw * 1_000,
                    TimeUnit::Microsecond => raw,
                    TimeUnit::Nanosecond => raw.div_euclid(1_000),
                };
                Value::Timestamp(TimestampValue {
                    unit: TimestampUnit::Micros,
                    value: micros,
                    timezone_utc: true,
                })
            } else {
                Value::Timestamp(TimestampValue {
                    unit: match unit {
                        TimeUnit::Second => TimestampUnit::Seconds,
                        TimeUnit::Millisecond => TimestampUnit::Millis,
                        TimeUnit::Microsecond => TimestampUnit::Micros,
                        TimeUnit::Nanosecond => TimestampUnit::Nanos,
                    },
                    value: raw,
                    timezone_utc: false,
                })
            }
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let v = array.as_primitive::<IntervalMonthDayNanoType>().value(row);
            Value::Interval(IntervalValue {
                months: v.months,
                days: v.days,
                micros: v.nanoseconds / 1_000,
            })
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>().value(row);
            Value::List(expected(&list))
        }
        DataType::FixedSizeList(..) => {
            let list = array.as_fixed_size_list().value(row);
            Value::List(expected(&list))
        }
        DataType::Struct(fields) => {
            let array = array.as_struct();
            Value::Struct(
                fields
                    .iter()
                    .zip(array.columns())
                    .map(|(f, c)| (f.name().clone(), value(c, row)))
                    .collect::<IndexMap<_, _>>(),
            )
        }
        DataType::Map(..) => {
            let entries = array.as_map().value(row);
            Value::List(
                (0..entries.len())
                    .map(|i| {
                        Value::Struct(IndexMap::from([
                            ("key".to_string(), value(entries.column(0), i)),
                            ("value".to_string(), value(entries.column(1), i)),
                        ]))
                    })
                    .collect(),
            )
        }
        other => panic!("no expected-value rule for {other}"),
    }
}

fn batch_of(columns: Vec<ArrayRef>) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch")
}

fn assert_round_trips(columns: Vec<ArrayRef>) {
    let batch = batch_of(columns);
    let actual = round_trip(&batch);
    for (index, column) in batch.columns().iter().enumerate() {
        assert_eq!(actual[index], expected(column), "column {index} ({})", column.data_type());
    }
}

// ---- strategies -------------------------------------------------------------

fn opt<T: std::fmt::Debug + Clone>(
    strategy: impl Strategy<Value = T>,
    len: usize,
) -> impl Strategy<Value = Vec<Option<T>>> {
    prop::collection::vec(prop::option::weighted(0.8, strategy), len)
}

fn utf8() -> impl Strategy<Value = String> {
    prop_oneof![Just(String::new()), "[a-zA-Z0-9 ]{0,12}", "\\PC{0,6}"]
}

fn column(len: usize) -> BoxedStrategy<ArrayRef> {
    prop_oneof![
        opt(any::<bool>(), len).prop_map(|v| Arc::new(BooleanArray::from(v)) as ArrayRef),
        opt(any::<i8>(), len).prop_map(|v| Arc::new(Int8Array::from(v)) as ArrayRef),
        opt(any::<i16>(), len).prop_map(|v| Arc::new(Int16Array::from(v)) as ArrayRef),
        opt(any::<i32>(), len).prop_map(|v| Arc::new(Int32Array::from(v)) as ArrayRef),
        opt(any::<i64>(), len).prop_map(|v| Arc::new(Int64Array::from(v)) as ArrayRef),
        opt(any::<u8>(), len).prop_map(|v| Arc::new(UInt8Array::from(v)) as ArrayRef),
        opt(any::<u16>(), len).prop_map(|v| Arc::new(UInt16Array::from(v)) as ArrayRef),
        opt(any::<u32>(), len).prop_map(|v| Arc::new(UInt32Array::from(v)) as ArrayRef),
        opt(any::<u64>(), len).prop_map(|v| Arc::new(UInt64Array::from(v)) as ArrayRef),
        opt(any::<f32>(), len).prop_map(|v| Arc::new(Float32Array::from(v)) as ArrayRef),
        opt(any::<f64>(), len).prop_map(|v| Arc::new(Float64Array::from(v)) as ArrayRef),
        (opt(-9_999i128..=9_999, len), Just(4u8), 0i8..=4).prop_map(decimal),
        (opt(-999_999_999i128..=999_999_999, len), Just(9u8), 0i8..=9).prop_map(decimal),
        (opt(-(10i128.pow(18) - 1)..10i128.pow(18), len), Just(18u8), 0i8..=18).prop_map(decimal),
        (opt(-(10i128.pow(38) - 1)..10i128.pow(38), len), Just(38u8), 0i8..=38).prop_map(decimal),
        opt(utf8(), len).prop_map(|v| Arc::new(StringArray::from(v)) as ArrayRef),
        opt(utf8(), len).prop_map(|v| Arc::new(LargeStringArray::from(v)) as ArrayRef),
        opt(utf8(), len).prop_map(|v| Arc::new(StringViewArray::from(v)) as ArrayRef),
        opt(prop::collection::vec(any::<u8>(), 0..20), len).prop_map(|v| {
            Arc::new(BinaryArray::from_iter(v.iter().map(|b| b.as_deref()))) as ArrayRef
        }),
        opt(any::<i32>(), len).prop_map(|v| Arc::new(Date32Array::from(v)) as ArrayRef),
        opt(0i64..86_400_000_000, len)
            .prop_map(|v| Arc::new(Time64MicrosecondArray::from(v)) as ArrayRef),
        opt(any::<i32>().prop_map(i64::from), len)
            .prop_map(|v| Arc::new(TimestampSecondArray::from(v)) as ArrayRef),
        opt(any::<i64>(), len)
            .prop_map(|v| Arc::new(TimestampMillisecondArray::from(v)) as ArrayRef),
        opt(any::<i64>(), len)
            .prop_map(|v| Arc::new(TimestampMicrosecondArray::from(v)) as ArrayRef),
        opt(any::<i64>(), len)
            .prop_map(|v| Arc::new(TimestampNanosecondArray::from(v)) as ArrayRef),
        opt(-(1i64 << 40)..(1i64 << 40), len).prop_map(|v| {
            Arc::new(TimestampMillisecondArray::from(v).with_timezone("+05:00")) as ArrayRef
        }),
        opt(any::<i64>(), len).prop_map(|v| {
            Arc::new(TimestampNanosecondArray::from(v).with_timezone("UTC")) as ArrayRef
        }),
        opt((any::<i32>(), any::<i32>(), any::<i64>()), len).prop_map(|v| {
            Arc::new(IntervalMonthDayNanoArray::from(
                v.into_iter()
                    .map(|o| o.map(|(m, d, n)| IntervalMonthDayNano::new(m, d, n)))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        }),
        opt(prop::collection::vec(prop::option::of(any::<i32>()), 0..5), len).prop_map(|v| {
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(v)) as ArrayRef
        }),
        opt(prop::collection::vec(prop::option::of(any::<i16>()), 3), len).prop_map(|v| {
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int16Type, _, _>(v, 3))
                as ArrayRef
        }),
        (opt(any::<i32>(), len), opt(utf8(), len), prop::collection::vec(any::<bool>(), len))
            .prop_map(|(a, b, valid)| {
                let fields = Fields::from(vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b c", DataType::Utf8, true),
                ]);
                Arc::new(StructArray::new(
                    fields,
                    vec![Arc::new(Int32Array::from(a)), Arc::new(StringArray::from(b))],
                    Some(NullBuffer::from(valid)),
                )) as ArrayRef
            }),
    ]
    .boxed()
}

fn decimal((values, precision, scale): (Vec<Option<i128>>, u8, i8)) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .expect("valid decimal"),
    )
}

fn batch_strategy() -> impl Strategy<Value = (Vec<ArrayRef>, usize, usize)> {
    (1usize..2500)
        .prop_flat_map(|len| {
            (
                prop::collection::vec(column(len), 1..4),
                0..len,
                Just(len),
            )
        })
        .prop_flat_map(|(columns, offset, len)| (Just(columns), Just(offset), 0..=len - offset))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    fn arrow_to_datachunk_to_values_is_identity((columns, _, _) in batch_strategy()) {
        assert_round_trips(columns);
    }

    #[test]
    fn sliced_arrays_round_trip((columns, offset, length) in batch_strategy()) {
        let sliced = columns.iter().map(|c| c.slice(offset, length)).collect();
        assert_round_trips(sliced);
    }
}

// ---- edge cases -------------------------------------------------------------

#[test]
fn batches_split_at_the_standard_vector_size() {
    let batch = batch_of(vec![Arc::new(Int64Array::from_iter_values(0..5000))]);
    let chunks = encode_record_batch(&batch).unwrap();
    assert_eq!(
        chunks.iter().map(|c| c.rows).collect::<Vec<_>>(),
        [2048, 2048, 904]
    );
    assert_eq!(
        round_trip(&batch)[0],
        (0..5000).map(Value::Int).collect::<Vec<_>>()
    );
}

#[test]
fn empty_batches_encode_to_no_chunks() {
    let batch = batch_of(vec![Arc::new(Int64Array::from(Vec::<i64>::new()))]);
    assert!(encode_record_batch(&batch).unwrap().is_empty());
}

#[test]
fn sliced_list_with_offsets_round_trips() {
    // datafusion-postgres #419: a sliced array's values start at its offset
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
        Some(vec![]),
        Some(vec![Some(4), None, Some(5)]),
    ]);
    let strings = StringArray::from(vec![Some("a"), None, Some("ccc"), Some(""), Some("e")]);
    assert_round_trips(vec![Arc::new(list.slice(2, 3)), Arc::new(strings.slice(1, 4).slice(0, 3))]);
}

#[test]
fn null_list_rows_with_non_empty_ranges_round_trip() {
    // Arrow lets a NULL list row cover child values; DuckDB must see it as empty.
    let values = Int32Array::from(vec![1, 2, 3, 4]);
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0, 2, 4].into()),
        Arc::new(values),
        Some(NullBuffer::from(vec![false, true])),
    );
    assert_eq!(
        round_trip(&batch_of(vec![Arc::new(list)]))[0],
        vec![Value::Null, Value::List(vec![Value::Int(3), Value::Int(4)])]
    );
}

#[test]
fn nested_lists_structs_and_maps_round_trip() {
    let inner = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1)]),
        Some(vec![Some(2), Some(3)]),
        None,
    ]);
    let outer = ListArray::new(
        Arc::new(Field::new("item", inner.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 2, 3].into()),
        Arc::new(inner),
        None,
    );
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    map.keys().append_value("k1");
    map.values().append_value(1);
    map.keys().append_value("k2");
    map.values().append_null();
    map.append(true).unwrap();
    map.append(false).unwrap();
    let map = map.finish();
    assert_round_trips(vec![Arc::new(outer), Arc::new(map)]);
}

#[test]
fn dictionaries_are_unpacked() {
    let dictionary: DictionaryArray<Int16Type> =
        vec![Some("x"), None, Some("y"), Some("x")].into_iter().collect();
    let batch = batch_of(vec![Arc::new(dictionary)]);
    assert_eq!(
        round_trip(&batch)[0],
        vec![
            Value::String("x".into()),
            Value::Null,
            Value::String("y".into()),
            Value::String("x".into())
        ]
    );
}

#[test]
fn null_arrays_are_null_integers() {
    let batch = batch_of(vec![Arc::new(NullArray::new(3))]);
    assert_eq!(round_trip(&batch)[0], vec![Value::Null; 3]);
}

#[test]
fn other_temporal_types_round_trip() {
    let batch = batch_of(vec![
        Arc::new(Date64Array::from(vec![Some(86_400_000 * 3), Some(-1), None])),
        Arc::new(Time32SecondArray::from(vec![Some(1), Some(2), None])),
        Arc::new(Time64NanosecondArray::from(vec![Some(5), Some(6), None])),
        Arc::new(IntervalYearMonthArray::from(vec![Some(14), Some(-1), None])),
        Arc::new(DurationMillisecondArray::from(vec![Some(1500), Some(-2), None])),
    ]);
    let values = round_trip(&batch);
    assert_eq!(
        values[0],
        [Value::Date(DateValue { days: 3 }), Value::Date(DateValue { days: -1 }), Value::Null]
    );
    assert_eq!(
        values[1][0],
        Value::Time(TimeValue {
            unit: QTimeUnit::Micros,
            value: 1_000_000
        })
    );
    assert_eq!(
        values[2][1],
        Value::Time(TimeValue {
            unit: QTimeUnit::Nanos,
            value: 6
        })
    );
    assert_eq!(
        values[3][0],
        Value::Interval(IntervalValue {
            months: 14,
            days: 0,
            micros: 0
        })
    );
    assert_eq!(
        values[4][0],
        Value::Interval(IntervalValue {
            months: 0,
            days: 0,
            micros: 1_500_000
        })
    );
}

#[test]
fn unsupported_types_are_errors_not_panics() {
    let union = UnionArray::try_new(
        UnionFields::try_new(vec![0], vec![Field::new("a", DataType::Int32, true)]).unwrap(),
        vec![0i8, 0].into(),
        None,
        vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
    )
    .unwrap();
    let decimal256 = Decimal256Array::from(vec![Some(i256::from(1))])
        .with_precision_and_scale(40, 0)
        .unwrap();
    for column in [Arc::new(union) as ArrayRef, Arc::new(decimal256)] {
        let batch = batch_of(vec![column]);
        assert!(matches!(
            encode_record_batch(&batch),
            Err(Error::Unsupported { .. })
        ));
    }
}

#[test]
fn decimal_values_wider_than_their_precision_are_errors() {
    // Arrow doesn't validate precision on construction; DuckDB's narrow storage can't hold it
    let decimal = Decimal128Array::from(vec![Some(123_456)])
        .with_precision_and_scale(4, 0)
        .unwrap();
    assert!(encode_record_batch(&batch_of(vec![Arc::new(decimal)])).is_err());
}
