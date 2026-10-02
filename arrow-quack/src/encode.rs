//! `RecordBatch` to DuckDB `DataChunk` encoding.
//!
//! A chunk is the object DuckDB's `DataChunk::Serialize` writes: the row count
//! (field 100), the column types (101) and one vector object per column (102).
//! Vectors are written flat, as `Vector::Serialize` does for storage version 2.0:
//!
//! - 100 `has_validity_mask`, 101 the mask (one bit per row, in 64-bit words)
//! - fixed-size types: 102 the values, `count * width` bytes, little endian
//! - strings: 107 data length, 108 one `u32` length per row, 109 the bytes
//! - STRUCT: 103 a list of child vectors
//! - LIST/MAP: 104 child count, 105 `(offset, length)` entries, 106 child vector
//! - ARRAY: 103 the array size, 104 child vector

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, FixedSizeListArray, OffsetSizeTrait, PrimitiveArray,
    StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type,
    Decimal128Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use quack_protocol::server::{
    BinaryWriter, encode_logical_type, validity_mask_size, write_string_vector_data,
};

use crate::types::arrow_to_logical_type;
use crate::{Error, Result};

/// The most rows DuckDB puts in a `DataChunk`.
pub const STANDARD_VECTOR_SIZE: usize = 2048;

const MILLIS_PER_DAY: i64 = 86_400_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// One encoded DuckDB `DataChunk`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct EncodedChunk {
    /// The serialized `DataChunk` object.
    pub bytes: Vec<u8>,
    /// Its row count.
    pub rows: usize,
}

impl AsRef<[u8]> for EncodedChunk {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Encodes `batch` as DuckDB `DataChunk`s of at most [`STANDARD_VECTOR_SIZE`] rows.
///
/// An empty batch, or one with no columns, encodes to no chunks: DuckDB never sends
/// an empty chunk. Sliced arrays are encoded from their offset.
pub fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<EncodedChunk>> {
    if batch.num_rows() == 0 || batch.num_columns() == 0 {
        return Ok(Vec::new());
    }
    let types = batch
        .schema()
        .fields()
        .iter()
        .map(|field| arrow_to_logical_type(field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    // unpack dictionaries once, rather than for every chunk
    let columns = batch
        .columns()
        .iter()
        .map(normalize)
        .collect::<Result<Vec<_>>>()?;

    let mut chunks = Vec::with_capacity(batch.num_rows().div_ceil(STANDARD_VECTOR_SIZE));
    let mut offset = 0;
    while offset < batch.num_rows() {
        let rows = STANDARD_VECTOR_SIZE.min(batch.num_rows() - offset);
        let mut writer = BinaryWriter::with_capacity(estimate_size(&columns, rows));
        writer.write_object(|chunk| {
            chunk.write_field(100, |chunk| chunk.write_uleb(rows as u64))?;
            chunk.write_field(101, |chunk| {
                chunk.write_list(&types, |chunk, logical_type, _| {
                    encode_logical_type(chunk, logical_type)
                })
            })?;
            chunk.write_field(102, |chunk| {
                chunk.write_uleb(columns.len() as u64)?;
                for column in &columns {
                    let column = column.slice(offset, rows);
                    write_vector(chunk, &column).map_err(protocol_error)?;
                }
                Ok(())
            })
        })?;
        chunks.push(EncodedChunk {
            bytes: writer.into_bytes(),
            rows,
        });
        offset += rows;
    }
    Ok(chunks)
}

/// Replaces dictionary-encoded arrays (at any depth) with their values.
fn normalize(array: &ArrayRef) -> Result<ArrayRef> {
    Ok(match array.data_type() {
        DataType::Dictionary(_, value) => cast(array, value)?,
        DataType::Struct(_) => {
            let array = array.as_struct();
            let columns = array
                .columns()
                .iter()
                .map(normalize)
                .collect::<Result<Vec<_>>>()?;
            let fields = array
                .fields()
                .iter()
                .zip(&columns)
                .map(|(field, column)| {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(column.data_type().clone()),
                    )
                })
                .collect::<Vec<_>>();
            Arc::new(StructArray::try_new(
                fields.into(),
                columns,
                array.nulls().cloned(),
            )?)
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _)
            if contains_dictionary(field.data_type()) =>
        {
            cast(array, &without_dictionaries(array.data_type()))?
        }
        _ => Arc::clone(array),
    })
}

fn contains_dictionary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(..) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_dictionary(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| contains_dictionary(f.data_type())),
        _ => false,
    }
}

fn without_dictionaries(data_type: &DataType) -> DataType {
    let field = |field: &arrow::datatypes::FieldRef| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(without_dictionaries(field.data_type())),
        )
    };
    match data_type {
        DataType::Dictionary(_, value) => without_dictionaries(value),
        DataType::List(f) => DataType::List(field(f)),
        DataType::LargeList(f) => DataType::LargeList(field(f)),
        DataType::FixedSizeList(f, size) => DataType::FixedSizeList(field(f), *size),
        DataType::Map(f, sorted) => DataType::Map(field(f), *sorted),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        other => other.clone(),
    }
}

fn estimate_size(columns: &[ArrayRef], rows: usize) -> usize {
    let per_row: usize = columns
        .iter()
        .map(|column| column.data_type().primitive_width().unwrap_or(16))
        .sum();
    256 + per_row * rows
}

fn protocol_error(error: Error) -> quack_protocol::QuackError {
    match error {
        Error::Protocol(error) => error,
        other => quack_protocol::QuackError::UnsupportedType(other.to_string()),
    }
}

/// Writes one vector object for every row of `array`.
fn write_vector(writer: &mut BinaryWriter, array: &dyn Array) -> Result<()> {
    writer.write_object(|vector| write_vector_body(vector, array).map_err(protocol_error))?;
    Ok(())
}

fn write_vector_body(writer: &mut BinaryWriter, array: &dyn Array) -> Result<()> {
    let count = array.len();
    let nulls = array.logical_nulls().filter(|nulls| nulls.null_count() > 0);
    write_validity(writer, nulls.as_ref(), count)?;
    let valid = |index: usize| nulls.as_ref().is_none_or(|nulls| nulls.is_valid(index));

    match array.data_type() {
        DataType::Null => write_fixed(writer, count, &i32::null_bytes(), |_, _| Ok(()))?,
        DataType::Boolean => {
            // BOOLEAN is stored as int8, so a NULL is 0x80
            let values: &BooleanArray = array.as_boolean();
            write_fixed(writer, count, &i8::null_bytes(), |data, index| {
                if valid(index) {
                    data.push(u8::from(values.value(index)));
                }
                Ok(())
            })?
        }
        DataType::Int8 => write_primitive::<Int8Type, _>(writer, array, &valid, |v| v)?,
        DataType::Int16 => write_primitive::<Int16Type, _>(writer, array, &valid, |v| v)?,
        DataType::Int32 => write_primitive::<Int32Type, _>(writer, array, &valid, |v| v)?,
        DataType::Int64 => write_primitive::<Int64Type, _>(writer, array, &valid, |v| v)?,
        DataType::UInt8 => write_primitive::<UInt8Type, _>(writer, array, &valid, |v| v)?,
        DataType::UInt16 => write_primitive::<UInt16Type, _>(writer, array, &valid, |v| v)?,
        DataType::UInt32 => write_primitive::<UInt32Type, _>(writer, array, &valid, |v| v)?,
        DataType::UInt64 => write_primitive::<UInt64Type, _>(writer, array, &valid, |v| v)?,
        DataType::Float16 => write_primitive::<Float16Type, _>(writer, array, &valid, f32::from)?,
        DataType::Float32 => write_primitive::<Float32Type, _>(writer, array, &valid, |v| v)?,
        DataType::Float64 => write_primitive::<Float64Type, _>(writer, array, &valid, |v| v)?,
        DataType::Decimal32(precision, _) => {
            write_decimal::<Decimal32Type>(writer, array, *precision, &valid, i128::from)?
        }
        DataType::Decimal64(precision, _) => {
            write_decimal::<Decimal64Type>(writer, array, *precision, &valid, i128::from)?
        }
        DataType::Decimal128(precision, _) => {
            write_decimal::<Decimal128Type>(writer, array, *precision, &valid, |v| v)?
        }
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            write_strings(writer, count, &valid, |i| a.value(i).as_bytes())?
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            write_strings(writer, count, &valid, |i| a.value(i).as_bytes())?
        }
        DataType::Utf8View => {
            let a = array.as_string_view();
            write_strings(writer, count, &valid, |i| a.value(i).as_bytes())?
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            write_strings(writer, count, &valid, |i| a.value(i))?
        }
        DataType::LargeBinary => {
            let a = array.as_binary::<i64>();
            write_strings(writer, count, &valid, |i| a.value(i))?
        }
        DataType::BinaryView => {
            let a = array.as_binary_view();
            write_strings(writer, count, &valid, |i| a.value(i))?
        }
        DataType::FixedSizeBinary(_) => {
            let a = array.as_fixed_size_binary();
            write_strings(writer, count, &valid, |i| a.value(i))?
        }
        DataType::Date32 => write_primitive::<Date32Type, _>(writer, array, &valid, |v| v)?,
        DataType::Date64 => write_primitive::<Date64Type, _>(writer, array, &valid, |v| {
            i32::try_from(v.div_euclid(MILLIS_PER_DAY)).unwrap_or(i32::MAX)
        })?,
        DataType::Time32(TimeUnit::Second) => {
            write_primitive::<Time32SecondType, _>(writer, array, &valid, |v| {
                i64::from(v) * 1_000_000
            })?
        }
        DataType::Time32(_) => {
            write_primitive::<Time32MillisecondType, _>(writer, array, &valid, |v| {
                i64::from(v) * 1_000
            })?
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            write_primitive::<Time64NanosecondType, _>(writer, array, &valid, |v| v)?
        }
        DataType::Time64(_) => {
            write_primitive::<Time64MicrosecondType, _>(writer, array, &valid, |v| v)?
        }
        DataType::Timestamp(unit, timezone) => {
            // DuckDB has one TIMESTAMPTZ, in microseconds
            let tz = timezone.is_some();
            match unit {
                TimeUnit::Second => {
                    write_primitive::<TimestampSecondType, _>(writer, array, &valid, |v| {
                        if tz { v.saturating_mul(1_000_000) } else { v }
                    })?
                }
                TimeUnit::Millisecond => {
                    write_primitive::<TimestampMillisecondType, _>(writer, array, &valid, |v| {
                        if tz { v.saturating_mul(1_000) } else { v }
                    })?
                }
                TimeUnit::Microsecond => {
                    write_primitive::<TimestampMicrosecondType, _>(writer, array, &valid, |v| v)?
                }
                TimeUnit::Nanosecond => {
                    write_primitive::<TimestampNanosecondType, _>(writer, array, &valid, |v| {
                        if tz { v.div_euclid(1_000) } else { v }
                    })?
                }
            }
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let a = array.as_primitive::<IntervalYearMonthType>();
            write_intervals(writer, count, &valid, |i| (a.value(i), 0, 0))?
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let a = array.as_primitive::<IntervalDayTimeType>();
            write_intervals(writer, count, &valid, |i| {
                let v = a.value(i);
                (0, v.days, i64::from(v.milliseconds) * 1_000)
            })?
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let a = array.as_primitive::<IntervalMonthDayNanoType>();
            write_intervals(writer, count, &valid, |i| {
                let v = a.value(i);
                (v.months, v.days, v.nanoseconds / 1_000)
            })?
        }
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => {
                write_durations::<DurationSecondType>(writer, array, &valid, |v| {
                    v.saturating_mul(1_000_000)
                })?
            }
            TimeUnit::Millisecond => {
                write_durations::<DurationMillisecondType>(writer, array, &valid, |v| {
                    v.saturating_mul(1_000)
                })?
            }
            TimeUnit::Microsecond => {
                write_durations::<DurationMicrosecondType>(writer, array, &valid, |v| v)?
            }
            TimeUnit::Nanosecond => {
                write_durations::<DurationNanosecondType>(writer, array, &valid, |v| v / 1_000)?
            }
        },
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            write_list_body(writer, list.value_offsets(), list.values(), &valid)?
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            write_list_body(writer, list.value_offsets(), list.values(), &valid)?
        }
        DataType::Map(..) => {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            write_list_body(writer, map.value_offsets(), &entries, &valid)?
        }
        DataType::FixedSizeList(_, size) => {
            let list: &FixedSizeListArray = array.as_fixed_size_list();
            let size = *size as usize;
            writer.write_field(103, |writer| writer.write_uleb(size as u64))?;
            let child = list.values().slice(list.offset() * size, count * size);
            writer.write_field(104, |writer| {
                write_vector(writer, &child).map_err(protocol_error)
            })?;
        }
        DataType::Struct(_) => {
            let array: &StructArray = array.as_struct();
            writer.write_field(103, |writer| {
                writer.write_uleb(array.num_columns() as u64)?;
                for child in array.columns() {
                    write_vector(writer, child).map_err(protocol_error)?;
                }
                Ok(())
            })?;
        }
        other => return Err(Error::unsupported(other, "no DuckDB counterpart")),
    }
    Ok(())
}

fn write_validity(
    writer: &mut BinaryWriter,
    nulls: Option<&NullBuffer>,
    count: usize,
) -> Result<()> {
    let Some(nulls) = nulls else {
        writer.write_field(100, |writer| writer.write_bool(false))?;
        return Ok(());
    };
    writer.write_field(100, |writer| writer.write_bool(true))?;
    // as DuckDB's ValidityMask: all valid, padding included, then the NULLs cleared
    let mut mask = vec![0xffu8; validity_mask_size(count)];
    for (index, valid) in nulls.iter().enumerate() {
        if !valid {
            mask[index / 8] &= !(1 << (index % 8));
        }
    }
    writer.write_field(101, |writer| writer.write_blob(&mask))?;
    Ok(())
}

/// Writes field 102 with `count` fixed-width values; `write` appends one value. A row
/// `write` skips (a NULL) gets `null`, the value DuckDB stores in a NULL slot
/// (`NullValue<T>()`): the minimum of a signed type, 0 of an unsigned one, NaN.
fn write_fixed(
    writer: &mut BinaryWriter,
    count: usize,
    null: &[u8],
    mut write: impl FnMut(&mut Vec<u8>, usize) -> Result<()>,
) -> Result<()> {
    let mut data = Vec::with_capacity(count * null.len());
    for index in 0..count {
        let before = data.len();
        write(&mut data, index)?;
        if data.len() == before {
            data.extend_from_slice(null);
        }
    }
    writer.write_field(102, |writer| writer.write_blob(&data))?;
    Ok(())
}

/// A little-endian fixed-width value, and DuckDB's NULL slot for its type.
trait Fixed: Copy {
    const NULL: Self;
    fn put(self, data: &mut Vec<u8>);
    fn null_bytes() -> Vec<u8> {
        let mut data = Vec::new();
        Self::NULL.put(&mut data);
        data
    }
}

macro_rules! fixed {
    ($($t:ty => $null:expr),*) => {$(
        impl Fixed for $t {
            const NULL: Self = $null;
            fn put(self, data: &mut Vec<u8>) {
                data.extend_from_slice(&self.to_le_bytes());
            }
        }
    )*};
}
fixed!(
    i8 => i8::MIN, i16 => i16::MIN, i32 => i32::MIN, i64 => i64::MIN,
    // hugeint_t's NullValue: lower 0, upper i64::MIN
    i128 => i128::MIN,
    u8 => 0, u16 => 0, u32 => 0, u64 => 0,
    f32 => f32::NAN, f64 => f64::NAN
);

/// An INTERVAL NULL slot: every field at its minimum.
fn interval_null() -> Vec<u8> {
    let mut data = Vec::with_capacity(16);
    write_interval_value(&mut data, i32::MIN, i32::MIN, i64::MIN);
    data
}

fn write_primitive<T: ArrowPrimitiveType, F: Fixed>(
    writer: &mut BinaryWriter,
    array: &dyn Array,
    valid: &dyn Fn(usize) -> bool,
    convert: impl Fn(T::Native) -> F,
) -> Result<()> {
    let values: &PrimitiveArray<T> = array.as_primitive();
    write_fixed(writer, values.len(), &F::null_bytes(), |data, index| {
        if valid(index) {
            convert(values.value(index)).put(data);
        }
        Ok(())
    })
}

/// Decimals use the narrowest integer DuckDB stores their width in.
fn write_decimal<T: ArrowPrimitiveType>(
    writer: &mut BinaryWriter,
    array: &dyn Array,
    precision: u8,
    valid: &dyn Fn(usize) -> bool,
    widen: impl Fn(T::Native) -> i128,
) -> Result<()> {
    let values: &PrimitiveArray<T> = array.as_primitive();
    let width = match precision {
        1..=4 => 2,
        5..=9 => 4,
        10..=18 => 8,
        _ => 16,
    };
    let null = match width {
        2 => i16::null_bytes(),
        4 => i32::null_bytes(),
        8 => i64::null_bytes(),
        _ => i128::null_bytes(),
    };
    write_fixed(writer, values.len(), &null, |data, index| {
        if !valid(index) {
            return Ok(());
        }
        let value = widen(values.value(index));
        let out_of_range = || {
            Error::unsupported(
                array.data_type(),
                format!("value {value} does not fit DECIMAL({precision})"),
            )
        };
        match width {
            2 => i16::try_from(value).map_err(|_| out_of_range())?.put(data),
            4 => i32::try_from(value).map_err(|_| out_of_range())?.put(data),
            8 => i64::try_from(value).map_err(|_| out_of_range())?.put(data),
            _ => value.put(data),
        }
        Ok(())
    })
}

fn write_strings<'a>(
    writer: &mut BinaryWriter,
    count: usize,
    valid: &dyn Fn(usize) -> bool,
    value: impl Fn(usize) -> &'a [u8],
) -> Result<()> {
    let mut lengths = Vec::with_capacity(count * 4);
    let mut bytes = Vec::new();
    for index in 0..count {
        if valid(index) {
            let value = value(index);
            let length = u32::try_from(value.len())
                .map_err(|_| Error::unsupported(&DataType::Utf8, "a value is longer than 4 GiB"))?;
            lengths.extend_from_slice(&length.to_le_bytes());
            bytes.extend_from_slice(value);
        } else {
            lengths.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    write_string_vector_data(writer, &lengths, &bytes)?;
    Ok(())
}

fn write_interval_value(data: &mut Vec<u8>, months: i32, days: i32, micros: i64) {
    months.put(data);
    days.put(data);
    micros.put(data);
}

/// Writes INTERVALs; `value` gives a row's (months, days, microseconds).
fn write_intervals(
    writer: &mut BinaryWriter,
    count: usize,
    valid: &dyn Fn(usize) -> bool,
    value: impl Fn(usize) -> (i32, i32, i64),
) -> Result<()> {
    write_fixed(writer, count, &interval_null(), |data, index| {
        if valid(index) {
            let (months, days, micros) = value(index);
            write_interval_value(data, months, days, micros);
        }
        Ok(())
    })
}

/// Writes durations as INTERVALs of whole days and the rest, as DuckDB's
/// `Interval::FromMicro`; `micros` converts a value to microseconds.
fn write_durations<T: ArrowPrimitiveType<Native = i64>>(
    writer: &mut BinaryWriter,
    array: &dyn Array,
    valid: &dyn Fn(usize) -> bool,
    micros: impl Fn(i64) -> i64,
) -> Result<()> {
    let values = array.as_primitive::<T>();
    write_intervals(writer, values.len(), valid, |i| {
        let micros = micros(values.value(i));
        let days = i32::try_from(micros / MICROS_PER_DAY).unwrap_or(i32::MAX);
        (0, days, micros % MICROS_PER_DAY)
    })
}

/// Writes a LIST or MAP vector's fields 104 to 106 for rows whose child ranges are
/// `offsets` (one more than the rows), rebased to the first. A NULL row is the empty
/// range at 0, as DuckDB writes it.
fn write_list_body<O: OffsetSizeTrait>(
    writer: &mut BinaryWriter,
    offsets: &[O],
    values: &ArrayRef,
    valid: &dyn Fn(usize) -> bool,
) -> Result<()> {
    let count = offsets.len() - 1;
    let offset = |index: usize| offsets[index].as_usize();
    let base = offset(0);
    writer.write_field(104, |writer| {
        writer.write_uleb((offset(count) - base) as u64)
    })?;
    writer.write_field(105, |writer| {
        writer.write_uleb(count as u64)?;
        for index in 0..count {
            let (start, length) = if valid(index) {
                (offset(index) - base, offset(index + 1) - offset(index))
            } else {
                (0, 0)
            };
            writer.write_object(|entry| {
                entry.write_field(100, |entry| entry.write_uleb(start as u64))?;
                entry.write_field(101, |entry| entry.write_uleb(length as u64))
            })?;
        }
        Ok(())
    })?;
    let child = values.slice(base, offset(count) - base);
    writer.write_field(106, |writer| {
        write_vector(writer, &child).map_err(protocol_error)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests;
