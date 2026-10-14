//! Serve a source as a pooled VGI worker: `Source::run` under `--vgi`.
//!
//! The twin of `trilogy/io/vgi.py`. DuckDB (haybarn, with the `vgi` extension)
//! attaches the program once and calls `trilogy_source.main.source(args)` per
//! scan, so the function runs in a warm process. `args` is the command line the
//! source would otherwise be launched with, parsed by the same [`Cli`]: the
//! contract does not fork, it is just delivered per call.

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::{Schema, SchemaRef};
use clap::Parser;
use vgi::catalog::CatalogModel;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
use vgi_rpc::{OutputCollector, RpcError};

use crate::adapters::{BatchReader, IntoBatchReader};
use crate::cli::{Cli, Invocation};
use crate::contract::SourceRequest;
use crate::error::{Error, Result, SCRIPT_ERROR_EXIT_CODE};
use crate::{error_report, Source};

/// Matches `trilogy.io.vgi.CATALOG_NAME` / `FUNCTION_NAME`.
pub const CATALOG_NAME: &str = "trilogy_source";
pub const FUNCTION_NAME: &str = "source";
/// Matches `trilogy.io.errors.VGI_FAILURE_MARKER`.
pub const VGI_FAILURE_MARKER: &str = "trilogy-script-failure";

/// The error DuckDB relays, carrying what a one-shot run would have exited with.
fn failure(error: &Error) -> RpcError {
    let payload = serde_json::json!({
        "script": std::env::args().next().unwrap_or_default(),
        "return_code": SCRIPT_ERROR_EXIT_CODE,
        "stderr": error_report(error),
    });
    RpcError::runtime_error(format!("{VGI_FAILURE_MARKER} {payload}"))
}

fn invocation(args: &str) -> Result<Invocation> {
    let argv = shlex::split(args)
        .ok_or_else(|| Error::contract(format!("unparseable source arguments: {args:?}")))?;
    let cli = Cli::try_parse_from(std::iter::once("source".to_string()).chain(argv))
        .map_err(|e| Error::contract(e.to_string()))?;
    cli.into_invocation()
}

fn plain(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(schema.fields().clone()))
}

struct Served<F> {
    source: Source<F>,
    // Readers resolved at bind (which needs the schema), claimed by the scan
    // that follows; a list per args because one query may scan a request twice.
    pending: Mutex<HashMap<String, Vec<BatchReader>>>,
}

impl<F, T> Served<F>
where
    F: Fn(&SourceRequest) -> T + Send + Sync + 'static,
    T: IntoBatchReader,
{
    fn resolve(&self, args: &str) -> std::result::Result<BatchReader, RpcError> {
        invocation(args)
            .and_then(|invocation| self.source.resolve(&invocation))
            .map_err(|e| failure(&e))
    }

    fn args(arguments: &vgi::arguments::Arguments) -> String {
        arguments.const_str(0).unwrap_or_default()
    }
}

impl<F, T> TableFunction for Served<F>
where
    F: Fn(&SourceRequest) -> T + Send + Sync + 'static,
    T: IntoBatchReader,
{
    fn name(&self) -> &str {
        FUNCTION_NAME
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Run this trilogy source with a contract command line".to_string(),
            projection_pushdown: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "args",
            0,
            "varchar",
            "The source's command line, e.g. '--limit 5'",
        )]
    }

    fn on_bind(&self, params: &BindParams) -> vgi_rpc::Result<BindResponse> {
        let args = Self::args(&params.arguments);
        let reader = self.resolve(&args)?;
        let output_schema = plain(&reader.schema());
        self.pending
            .lock()
            .unwrap()
            .entry(args)
            .or_default()
            .push(reader);
        Ok(BindResponse {
            output_schema,
            opaque_data: Vec::new(),
        })
    }

    fn producer(&self, params: &ProcessParams) -> vgi_rpc::Result<Box<dyn TableProducer>> {
        let args = Self::args(&params.arguments);
        let claimed = self
            .pending
            .lock()
            .unwrap()
            .get_mut(&args)
            .filter(|readers| !readers.is_empty())
            .map(|readers| readers.remove(0));
        // Bind ran in another pooled process when nothing is waiting here.
        let reader = match claimed {
            Some(reader) => reader,
            None => self.resolve(&args)?,
        };
        let schema = plain(&reader.schema());
        Ok(Box::new(Scan { reader, schema }))
    }
}

struct Scan {
    reader: BatchReader,
    schema: SchemaRef,
}

impl TableProducer for Scan {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> vgi_rpc::Result<Option<RecordBatch>> {
        match self.reader.next() {
            None => Ok(None),
            Some(batch) => batch
                .and_then(|b| RecordBatch::try_new(self.schema.clone(), b.columns().to_vec()))
                .map(Some)
                .map_err(|e| failure(&Error::from(e))),
        }
    }

    fn resume_supported(&self) -> bool {
        // Served over stdio; HTTP continuation does not apply.
        false
    }
}

pub(crate) fn serve<F, T>(source: Source<F>) -> ExitCode
where
    F: Fn(&SourceRequest) -> T + Send + Sync + 'static,
    T: IntoBatchReader,
{
    let mut worker = vgi::Worker::new();
    worker.register_table(Served {
        source,
        pending: Mutex::new(HashMap::new()),
    });
    worker.set_catalog(CatalogModel {
        name: CATALOG_NAME.to_string(),
        ..Default::default()
    });
    worker.run();
    ExitCode::SUCCESS
}
