//! Errors, and how they reach a client.
//!
//! Every failure a client causes becomes an ERROR_RESPONSE (HTTP 200) carrying a
//! message and a DuckDB exception type name. DuckDB prints them as
//! `"<type> Error: <message>"`.

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;

/// A DuckDB exception type, as DuckDB names it on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExceptionType {
    /// `Binder`: an unknown column, or a plan that doesn't type-check.
    Binder,
    /// `Catalog`: an unknown table or schema.
    Catalog,
    /// `Conversion`: a cast failed.
    Conversion,
    /// `Divide by Zero`.
    DivideByZero,
    /// `INTERNAL`: a bug in the server. DuckDB clients keep their database usable.
    Internal,
    /// `INTERRUPT`: the query was cancelled.
    Interrupt,
    /// `Invalid Input`: everything else a client did wrong.
    InvalidInput,
    /// `IO`.
    Io,
    /// `Not implemented`.
    NotImplemented,
    /// `Out of Memory`.
    OutOfMemory,
    /// `Out of Range`.
    OutOfRange,
    /// `Parser`: the SQL doesn't parse.
    Parser,
    /// `Permission`: authentication or authorization failed.
    Permission,
    /// `Settings`: a bad configuration option.
    Settings,
    /// `TransactionContext`: a transaction statement out of place.
    TransactionContext,
}

impl ExceptionType {
    /// The name DuckDB uses on the wire (`Exception::ExceptionTypeToString`).
    pub fn name(self) -> &'static str {
        match self {
            Self::Binder => "Binder",
            Self::Catalog => "Catalog",
            Self::Conversion => "Conversion",
            Self::DivideByZero => "Divide by Zero",
            Self::Internal => "INTERNAL",
            Self::Interrupt => "INTERRUPT",
            Self::InvalidInput => "Invalid Input",
            Self::Io => "IO",
            Self::NotImplemented => "Not implemented",
            Self::OutOfMemory => "Out of Memory",
            Self::OutOfRange => "Out of Range",
            Self::Parser => "Parser",
            Self::Permission => "Permission",
            Self::Settings => "Settings",
            Self::TransactionContext => "TransactionContext",
        }
    }
}

/// An error sent to a client as an ERROR_RESPONSE.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{} Error: {message}", exception_type.name())]
#[non_exhaustive]
pub struct ClientError {
    /// The DuckDB exception type.
    pub exception_type: ExceptionType,
    /// The message, without the type prefix.
    pub message: String,
}

impl ClientError {
    /// An error of `exception_type`.
    pub fn new(exception_type: ExceptionType, message: impl Into<String>) -> Self {
        Self {
            exception_type,
            message: message.into(),
        }
    }

    /// An `Invalid Input` error, the type DuckDB's server uses for protocol errors.
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(ExceptionType::InvalidInput, message)
    }

    /// A `Permission` error.
    pub fn permission(message: impl Into<String>) -> Self {
        Self::new(ExceptionType::Permission, message)
    }

    /// Prefixes the message with what it is about.
    pub fn with_context(mut self, context: &str) -> Self {
        self.message = format!("{context}: {}", self.message);
        self
    }

    /// The query was interrupted, in the words DuckDB clients look for.
    pub fn interrupted(reason: &str) -> Self {
        Self::new(ExceptionType::Interrupt, format!("Interrupted: {reason}"))
    }

    /// The query was cancelled.
    pub fn cancelled() -> Self {
        Self::interrupted("query was cancelled")
    }
}

impl From<DataFusionError> for ClientError {
    fn from(error: DataFusionError) -> Self {
        let message = error.message().into_owned();
        let exception_type = match error.find_root() {
            DataFusionError::SQL(..) => ExceptionType::Parser,
            DataFusionError::SchemaError(..) | DataFusionError::Plan(_) => ExceptionType::Binder,
            DataFusionError::NotImplemented(_) => ExceptionType::NotImplemented,
            DataFusionError::Internal(_) => ExceptionType::Internal,
            DataFusionError::ResourcesExhausted(_) => ExceptionType::OutOfMemory,
            DataFusionError::Configuration(_) => ExceptionType::Settings,
            DataFusionError::IoError(_) | DataFusionError::ObjectStore(_) => ExceptionType::Io,
            DataFusionError::ArrowError(arrow, _) => arrow_exception_type(arrow),
            _ => ExceptionType::InvalidInput,
        };
        Self {
            exception_type,
            message,
        }
    }
}

impl From<arrow_quack::Error> for ClientError {
    fn from(error: arrow_quack::Error) -> Self {
        let exception_type = match &error {
            arrow_quack::Error::Unsupported { .. } => ExceptionType::NotImplemented,
            arrow_quack::Error::Arrow(arrow) => arrow_exception_type(arrow),
            _ => ExceptionType::Internal,
        };
        Self::new(exception_type, error.to_string())
    }
}

impl From<quack_protocol::QuackError> for ClientError {
    fn from(error: quack_protocol::QuackError) -> Self {
        Self::new(ExceptionType::Internal, error.to_string())
    }
}

fn arrow_exception_type(error: &ArrowError) -> ExceptionType {
    match error {
        ArrowError::DivideByZero => ExceptionType::DivideByZero,
        ArrowError::CastError(_) | ArrowError::ParseError(_) => ExceptionType::Conversion,
        ArrowError::ArithmeticOverflow(_) | ArrowError::ComputeError(_) => {
            ExceptionType::OutOfRange
        }
        ArrowError::MemoryError(_) => ExceptionType::OutOfMemory,
        ArrowError::IoError(..) => ExceptionType::Io,
        _ => ExceptionType::InvalidInput,
    }
}

/// An error that stops the server from starting or running.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServerError {
    /// Binding or serving the socket failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The TLS certificate or key could not be used.
    #[error("TLS configuration error: {0}")]
    Tls(String),
    /// DataFusion failed while setting up a session.
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datafusion_errors_get_duckdb_types() {
        let plan = ClientError::from(DataFusionError::Plan("table 't' not found".into()));
        assert_eq!(plan.exception_type, ExceptionType::Binder);
        assert_eq!(plan.message, "table 't' not found");
        assert_eq!(plan.to_string(), "Binder Error: table 't' not found");

        let wrapped = DataFusionError::Context(
            "while planning".into(),
            Box::new(DataFusionError::ArrowError(
                Box::new(ArrowError::DivideByZero),
                None,
            )),
        );
        assert_eq!(
            ClientError::from(wrapped).exception_type,
            ExceptionType::DivideByZero
        );
    }

    #[test]
    fn interrupts_use_the_text_duckdb_clients_look_for() {
        let error = ClientError::interrupted("query was cancelled");
        assert_eq!(error.exception_type.name(), "INTERRUPT");
        assert!(error.message.starts_with("Interrupted"));
    }
}
