//! Arrow to DuckDB encoding for the [Quack protocol].
//!
//! A Quack server sends query results as DuckDB `DataChunk`s. This crate maps Arrow
//! types to DuckDB logical types ([`arrow_to_logical_type`]), renders the DuckDB
//! name of a type ([`duckdb_type_name`]), and encodes a [`RecordBatch`] as
//! `DataChunk`s of at most [`STANDARD_VECTOR_SIZE`] rows ([`encode_record_batch`]).
//!
//! The encoder works column by column, straight from the Arrow buffers, and writes
//! string vectors in DuckDB's storage version 2.0 layout, as a DuckDB 2.0 server does.
//!
//! Types without a DuckDB counterpart are an [`Error::Unsupported`], never a panic.
//!
//! [Quack protocol]: https://duckdb.org/docs/current/quack/overview
//! [`RecordBatch`]: arrow::record_batch::RecordBatch

mod encode;
mod types;

pub use encode::{EncodedChunk, STANDARD_VECTOR_SIZE, encode_record_batch};
pub use quack_protocol::{LogicalType, LogicalTypeId};
pub use types::{arrow_to_logical_type, duckdb_type_name, quote_identifier};

use arrow::datatypes::DataType;

/// An error from this crate.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The Arrow type has no DuckDB counterpart.
    #[error("unsupported Arrow type {data_type}: {reason}")]
    Unsupported {
        /// The type that can't be encoded.
        data_type: DataType,
        /// Why.
        reason: String,
    },
    /// Arrow failed, e.g. when a dictionary was unpacked.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// The wire codec failed.
    #[error(transparent)]
    Protocol(#[from] quack_protocol::QuackError),
}

impl Error {
    pub(crate) fn unsupported(data_type: &DataType, reason: impl Into<String>) -> Self {
        Self::Unsupported {
            data_type: data_type.clone(),
            reason: reason.into(),
        }
    }
}

/// A result with this crate's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
