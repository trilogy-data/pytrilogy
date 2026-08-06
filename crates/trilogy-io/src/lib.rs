//! Make a Rust program a Trilogy data source.
//!
//! ```no_run
//! use arrow::array::RecordBatch;
//! use trilogy_io::{Result, SourceRequest};
//!
//! fn landmarks(request: &SourceRequest) -> Result<Vec<RecordBatch>> {
//!     let _ = request.limit;
//!     Ok(vec![])
//! }
//!
//! fn main() -> std::process::ExitCode {
//!     trilogy_io::run(landmarks)
//! }
//! ```
//!
//! The program gets `--limit`, `--filter`, `--columns`, `--order-by`, `--format`,
//! `--output` and `--describe` for free, and writes an Arrow IPC stream to stdout. That
//! command-line surface -- not this library -- is the actual contract, and it
//! is byte-for-byte the one `trilogy.io` implements in python, so Trilogy does
//! not care which language a source is written in.
//!
//! Anything the source does not claim through [`Source::pushdown`] is enforced
//! on its output stream, so the contract holds whether or not the author does
//! anything about it. Claiming a field is an optimization, not a correctness
//! requirement -- an API-backed source claims `Filters` so it can push the
//! predicate to the API instead of scanning and discarding.

use std::io::Write;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::datatypes::{Schema, SchemaRef};
use clap::Parser;

pub mod adapters;
pub mod cli;
pub mod contract;
pub mod describe;
pub mod error;
pub mod sinks;
pub mod transform;

pub use adapters::{BatchReader, IntoBatchReader};
pub use cli::{Cli, Invocation};
pub use contract::{effective_pushdown, Field, Filter, Op, Sort, SourceRequest, CONTRACT_VERSION};
pub use error::{Error, Result, ERROR_PREFIX, SCRIPT_ERROR_EXIT_CODE};
pub use sinks::Format;

pub const METADATA_PREFIX: &str = "trilogy.";

/// A source function plus what it claims about itself.
pub struct Source<F> {
    function: F,
    pushdown: Vec<Field>,
    schema: Option<SchemaRef>,
    watermark: Option<String>,
}

/// Wrap a function so it can be run as a data source.
pub fn source<F, T>(function: F) -> Source<F>
where
    F: FnOnce(&SourceRequest) -> T,
    T: IntoBatchReader,
{
    Source {
        function,
        pushdown: Vec::new(),
        schema: None,
        watermark: None,
    }
}

impl<F, T> Source<F>
where
    F: FnOnce(&SourceRequest) -> T,
    T: IntoBatchReader,
{
    /// Contract fields this source honors itself. Anything omitted is enforced
    /// on the output stream instead.
    pub fn pushdown(mut self, fields: &[Field]) -> Self {
        self.pushdown = fields.to_vec();
        self
    }

    /// The columns this source produces, so it can still describe itself when
    /// it returns no rows.
    pub fn schema(mut self, schema: impl Into<SchemaRef>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// High-water value reached, reported to the consumer as stream metadata.
    pub fn watermark(mut self, watermark: impl Into<String>) -> Self {
        self.watermark = Some(watermark.into());
        self
    }

    /// Build the reader for an already-parsed invocation, applying the request.
    pub fn resolve(self, invocation: &Invocation) -> Result<BatchReader> {
        let pushdown = effective_pushdown(&self.pushdown, &invocation.request);
        let handed = invocation.request.withheld(&pushdown);
        let reader = (self.function)(&handed).into_batch_reader(self.schema)?;
        let reader = transform::apply(reader, &invocation.request, &pushdown)?;
        Ok(stamp(reader, &pushdown, self.watermark.as_deref()))
    }

    /// Parse the process arguments and run. Returns a process exit code.
    pub fn run(self) -> ExitCode {
        let program = std::env::args().next().unwrap_or_else(|| "source".into());
        match self.execute(&program) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                report(&e);
                ExitCode::from(SCRIPT_ERROR_EXIT_CODE)
            }
        }
    }

    fn execute(self, program: &str) -> Result<()> {
        let invocation = Cli::parse().into_invocation()?;
        if invocation.describe {
            let pushdown = self.pushdown.clone();
            // Deliberately the unnarrowed request: describe reports what the
            // source produces, not what this particular invocation would return.
            let plain = Invocation {
                request: SourceRequest::default(),
                ..invocation
            };
            let reader = self.resolve(&plain)?;
            let payload = describe::payload(&reader.schema(), &pushdown, program);
            println!("{}", serde_json::to_string_pretty(&payload)?);
            return Ok(());
        }
        let format = invocation.format;
        let output = invocation.output.clone();
        sinks::write(self.resolve(&invocation)?, format, output.as_deref())?;
        Ok(())
    }
}

/// Run `function` as a data source, claiming no pushdown.
pub fn run<F, T>(function: F) -> ExitCode
where
    F: FnOnce(&SourceRequest) -> T,
    T: IntoBatchReader,
{
    source(function).run()
}

/// Attach sideband metadata to the stream's schema.
///
/// Schema-level key-value metadata rides inside the IPC stream and survives a
/// parquet staging hop, so the consumer reads it off the schema with no extra
/// plumbing. Only facts known before the first batch belong here -- the schema
/// is written first, so row counts cannot go in it.
fn stamp(reader: BatchReader, pushdown: &[Field], watermark: Option<&str>) -> BatchReader {
    let mut metadata = reader.schema().metadata().clone();
    metadata.insert(
        format!("{METADATA_PREFIX}contract"),
        CONTRACT_VERSION.to_string(),
    );
    metadata.insert(
        format!("{METADATA_PREFIX}pushdown"),
        pushdown
            .iter()
            .map(|f| f.as_str())
            .collect::<Vec<_>>()
            .join(","),
    );
    if let Some(watermark) = watermark {
        metadata.insert(format!("{METADATA_PREFIX}watermark"), watermark.to_string());
    }
    let schema: SchemaRef = Arc::new(Schema::new_with_metadata(
        reader.schema().fields().clone(),
        metadata,
    ));
    Box::new(Restamped {
        inner: reader,
        schema,
    })
}

struct Restamped {
    inner: BatchReader,
    schema: SchemaRef,
}

impl Iterator for Restamped {
    type Item = std::result::Result<arrow::array::RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.inner.next()?;
        Some(batch.and_then(|b| {
            arrow::array::RecordBatch::try_new(self.schema.clone(), b.columns().to_vec())
        }))
    }
}

impl RecordBatchReader for Restamped {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Machine-readable line first, then the detail a human needs.
fn report(error: &Error) {
    let line = serde_json::json!({
        "type": error.kind(),
        "message": error.to_string(),
        "contract": CONTRACT_VERSION,
        "retryable": error.retryable(),
    });
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "{ERROR_PREFIX}{line}");
    let _ = writeln!(stderr, "{error}");
    let _ = stderr.flush();
}
