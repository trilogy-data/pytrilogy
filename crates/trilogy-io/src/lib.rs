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

use std::collections::BTreeMap;
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
#[cfg(feature = "vgi")]
pub mod vgi;

pub use adapters::{BatchReader, IntoBatchReader};
pub use cli::{Cli, Invocation};
pub use contract::{effective_pushdown, Field, Filter, Op, Sort, SourceRequest, CONTRACT_VERSION};
pub use error::{Error, Result, ERROR_PREFIX, SCRIPT_ERROR_EXIT_CODE};
pub use sinks::Format;

pub const METADATA_PREFIX: &str = "trilogy.";

/// Serve the source as a pooled VGI worker instead of writing one stream.
/// Matches `trilogy.io.errors.VGI_FLAG`.
pub const VGI_FLAG: &str = "--vgi";

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
    F: Fn(&SourceRequest) -> T + Send + Sync + 'static,
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
    F: Fn(&SourceRequest) -> T + Send + Sync + 'static,
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
    pub fn resolve(&self, invocation: &Invocation) -> Result<BatchReader> {
        let pushdown = effective_pushdown(&self.pushdown, &invocation.request);
        let handed = invocation.request.withheld(&pushdown);
        let declared = self.schema.clone();
        let reader = (self.function)(&handed).into_batch_reader(self.schema.clone())?;
        let reader = adapters::conform_reader(reader, declared)?;
        let reader = transform::apply(reader, &invocation.request, &pushdown)?;
        Ok(stamp(reader, &pushdown, self.watermark.as_deref()))
    }

    /// Parse the process arguments and run. Returns a process exit code.
    ///
    /// With `--vgi`, serves the source as a pooled VGI worker instead (needs
    /// the `vgi` feature; without it the flag is refused, and a consumer falls
    /// back to running the source once per scan).
    pub fn run(self) -> ExitCode {
        Program::Single(Box::new(self)).run()
    }
}

/// A source with its function's type erased, so several can share a program.
pub(crate) trait Erased: Send + Sync {
    fn read(&self, invocation: &Invocation) -> Result<BatchReader>;
    fn claims(&self) -> &[Field];
}

impl<F, T> Erased for Source<F>
where
    F: Fn(&SourceRequest) -> T + Send + Sync + 'static,
    T: IntoBatchReader,
{
    fn read(&self, invocation: &Invocation) -> Result<BatchReader> {
        self.resolve(invocation)
    }

    fn claims(&self) -> &[Field] {
        &self.pushdown
    }
}

/// Several named sources served by one program, selected with `--source`.
#[derive(Default)]
pub struct Sources {
    entries: BTreeMap<String, Box<dyn Erased>>,
}

/// Start a program that serves several sources.
pub fn sources() -> Sources {
    Sources::default()
}

impl Sources {
    /// Register `source` as `name`. Panics on a name that is not an identifier
    /// (`[A-Za-z_][A-Za-z0-9_]*`) or is already registered.
    pub fn add<F, T>(mut self, name: &str, source: Source<F>) -> Self
    where
        F: Fn(&SourceRequest) -> T + Send + Sync + 'static,
        T: IntoBatchReader,
    {
        assert!(
            is_identifier(name),
            "source name {name:?} is not an identifier"
        );
        assert!(
            !self.entries.contains_key(name),
            "source {name:?} is registered twice"
        );
        self.entries.insert(name.to_string(), Box::new(source));
        self
    }

    /// Parse the process arguments and run the selected source, or serve them
    /// all under `--vgi`. Returns a process exit code.
    pub fn run(self) -> ExitCode {
        Program::Several(self.entries).run()
    }
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// What a program serves: one unnamed source, or several by name.
pub(crate) enum Program {
    Single(Box<dyn Erased>),
    Several(BTreeMap<String, Box<dyn Erased>>),
}

impl Program {
    /// The source `--source` names; a contract failure listing what is served.
    pub(crate) fn select(&self, source: Option<&str>) -> Result<&dyn Erased> {
        match (self, source) {
            (Program::Single(only), None) => Ok(only.as_ref()),
            (Program::Single(_), Some(name)) => Err(Error::contract(format!(
                "unknown source '{name}': this program serves a single source"
            ))),
            (Program::Several(entries), None) => Err(Error::contract(format!(
                "this program serves several sources; choose one with --source. \
                 Available sources: {}",
                available(entries)
            ))),
            (Program::Several(entries), Some(name)) => {
                entries.get(name).map(|e| e.as_ref()).ok_or_else(|| {
                    Error::contract(format!(
                        "unknown source '{name}'. Available sources: {}",
                        available(entries)
                    ))
                })
            }
        }
    }

    /// The selected source's reader for an already-parsed invocation.
    #[cfg(feature = "vgi")]
    pub(crate) fn resolve(&self, invocation: &Invocation) -> Result<BatchReader> {
        self.select(invocation.source.as_deref())?.read(invocation)
    }

    fn run(self) -> ExitCode {
        if std::env::args().any(|arg| arg == VGI_FLAG) {
            return self.serve();
        }
        let program = std::env::args().next().unwrap_or_else(|| "source".into());
        match self.execute(&program) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                report(&e);
                ExitCode::from(SCRIPT_ERROR_EXIT_CODE)
            }
        }
    }

    #[cfg(feature = "vgi")]
    fn serve(self) -> ExitCode {
        vgi::serve(self)
    }

    #[cfg(not(feature = "vgi"))]
    fn serve(self) -> ExitCode {
        eprintln!("{VGI_FLAG} needs trilogy-io built with the `vgi` feature");
        ExitCode::from(2)
    }

    fn execute(&self, program: &str) -> Result<()> {
        let invocation = Cli::parse().into_invocation()?;
        let selected = self.select(invocation.source.as_deref())?;
        if invocation.describe {
            // Deliberately the unnarrowed request: describe reports what the
            // source produces, not what this particular invocation would return.
            let plain = Invocation {
                request: SourceRequest::default(),
                format: invocation.format,
                output: None,
                describe: true,
                source: invocation.source.clone(),
            };
            let reader = selected.read(&plain)?;
            let payload = describe::payload(
                &reader.schema(),
                selected.claims(),
                program,
                invocation.source.as_deref(),
            );
            println!("{}", serde_json::to_string_pretty(&payload)?);
            return Ok(());
        }
        let format = invocation.format;
        let output = invocation.output.clone();
        sinks::write(selected.read(&invocation)?, format, output.as_deref())?;
        Ok(())
    }
}

fn available(entries: &BTreeMap<String, Box<dyn Erased>>) -> String {
    entries.keys().cloned().collect::<Vec<_>>().join(", ")
}

/// Run `function` as a data source, claiming no pushdown.
pub fn run<F, T>(function: F) -> ExitCode
where
    F: Fn(&SourceRequest) -> T + Send + Sync + 'static,
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
pub(crate) fn error_report(error: &Error) -> String {
    let line = serde_json::json!({
        "type": error.kind(),
        "message": error.to_string(),
        "contract": CONTRACT_VERSION,
        "retryable": error.retryable(),
    });
    format!("{ERROR_PREFIX}{line}\n{error}\n")
}

fn report(error: &Error) {
    let mut stderr = std::io::stderr().lock();
    let _ = stderr.write_all(error_report(error).as_bytes());
    let _ = stderr.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty(_: &SourceRequest) -> Result<Vec<arrow::array::RecordBatch>> {
        Ok(vec![])
    }

    fn several() -> Program {
        Program::Several(
            sources()
                .add("b", source(empty))
                .add("a", source(empty))
                .entries,
        )
    }

    fn message(program: &Program, name: Option<&str>) -> String {
        program
            .select(name)
            .err()
            .expect("selection fails")
            .to_string()
    }

    #[test]
    fn several_sources_need_a_known_name() {
        let program = several();
        assert!(program.select(Some("a")).is_ok());
        assert_eq!(
            message(&program, None),
            "this program serves several sources; choose one with --source. \
             Available sources: a, b"
        );
        assert_eq!(
            message(&program, Some("x")),
            "unknown source 'x'. Available sources: a, b"
        );
        assert_eq!(
            program.select(Some("x")).err().unwrap().kind(),
            "ContractError"
        );
    }

    #[test]
    fn a_single_source_refuses_a_name() {
        let program = Program::Single(Box::new(source(empty)));
        assert!(program.select(None).is_ok());
        assert_eq!(
            message(&program, Some("x")),
            "unknown source 'x': this program serves a single source"
        );
    }

    #[test]
    fn names_are_identifiers() {
        assert!(is_identifier("process"));
        assert!(is_identifier("_a1"));
        assert!(!is_identifier("1a"));
        assert!(!is_identifier("a-b"));
        assert!(!is_identifier(""));
    }

    #[test]
    #[should_panic(expected = "registered twice")]
    fn a_duplicate_name_panics() {
        let _ = sources().add("a", source(empty)).add("a", source(empty));
    }
}
