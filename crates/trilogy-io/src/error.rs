use std::fmt::Display;

/// Distinct from 1/2 so a consumer can tell "the source's own logic failed"
/// from "the launcher failed". Matches `trilogy.io.errors.SCRIPT_ERROR_EXIT_CODE`.
pub const SCRIPT_ERROR_EXIT_CODE: u8 = 65;

pub const ERROR_PREFIX: &str = "trilogy-io-error: ";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Contract(String),
    #[error("{0}")]
    Adapter(String),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Whatever the source function itself returned.
    #[error("{0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    pub fn contract(message: impl Display) -> Self {
        Error::Contract(message.to_string())
    }

    pub fn adapter(message: impl Display) -> Self {
        Error::Adapter(message.to_string())
    }

    /// The name a consumer sees in the structured error line.
    pub fn kind(&self) -> &'static str {
        match self {
            Error::Contract(_) => "ContractError",
            Error::Adapter(_) => "AdapterError",
            Error::Arrow(_) => "ArrowError",
            Error::Parquet(_) => "ParquetError",
            Error::Io(_) => "IoError",
            Error::Json(_) => "JsonError",
            Error::Source(_) => "SourceError",
        }
    }

    /// Transient by nature, so the consumer may retry rather than fail the query.
    pub fn retryable(&self) -> bool {
        match self {
            Error::Io(e) => matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::WouldBlock
            ),
            _ => false,
        }
    }
}
