//! The command line is the contract.
//!
//! Flag names, exit codes, the `--describe` payload and the metadata keys are
//! what makes a Rust source and a python source interchangeable to Trilogy, so
//! these must stay identical to `trilogy/io/runner.py`.

use std::collections::BTreeMap;

use clap::Parser;

use crate::contract::{Filter, Sort, SourceRequest};
use crate::error::{Error, Result};
use crate::sinks::Format;

#[derive(Debug, Parser)]
#[command(about = "A trilogy data source. Writes Arrow IPC to stdout.")]
pub struct Cli {
    /// Maximum rows to emit.
    #[arg(long)]
    pub limit: Option<usize>,

    /// Comma-separated columns to project.
    #[arg(long)]
    pub columns: Option<String>,

    /// Row predicate, repeatable (e.g. --filter 'state in ["CA"]').
    #[arg(long = "filter", value_name = "'col op value'")]
    pub filters: Vec<String>,

    /// Comma-separated sort keys, e.g. 'score:desc,id'.
    #[arg(long = "order-by")]
    pub order_by: Option<String>,

    /// Watermark low bound.
    #[arg(long)]
    pub since: Option<String>,

    /// Partition selector, repeatable.
    #[arg(long, value_name = "KEY=VALUE")]
    pub partition: Vec<String>,

    #[arg(long = "format", value_enum, default_value_t = Format::Arrow)]
    pub format: Format,

    /// Destination URI; default stdout.
    #[arg(long)]
    pub output: Option<String>,

    /// Print this source's schema and pushdown support as JSON.
    #[arg(long)]
    pub describe: bool,
}

#[derive(Debug)]
pub struct Invocation {
    pub request: SourceRequest,
    pub format: Format,
    pub output: Option<String>,
    pub describe: bool,
}

impl Cli {
    pub fn into_invocation(self) -> Result<Invocation> {
        let mut partition = BTreeMap::new();
        for pair in &self.partition {
            let (key, value) = pair.split_once('=').ok_or_else(|| {
                Error::contract(format!("--partition expects KEY=VALUE, got {pair:?}"))
            })?;
            partition.insert(key.trim().to_string(), value.trim().to_string());
        }
        Ok(Invocation {
            request: SourceRequest {
                limit: self.limit,
                columns: self.columns.as_deref().map(split_columns),
                filters: self
                    .filters
                    .iter()
                    .map(|f| Filter::parse(f))
                    .collect::<Result<Vec<_>>>()?,
                order_by: self
                    .order_by
                    .as_deref()
                    .map(split_columns)
                    .unwrap_or_default()
                    .iter()
                    .map(|s| Sort::parse(s))
                    .collect::<Result<Vec<_>>>()?,
                since: self.since,
                partition,
            },
            format: self.format,
            output: self.output,
            describe: self.describe,
        })
    }
}

fn split_columns(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Invocation {
        let mut argv = vec!["source"];
        argv.extend_from_slice(args);
        Cli::parse_from(argv).into_invocation().unwrap()
    }

    #[test]
    fn builds_a_request() {
        let invocation = parse(&[
            "--limit",
            "5",
            "--columns",
            "i, state",
            "--filter",
            "state = CA",
            "--since",
            "2026-01-01",
            "--partition",
            "day=2026-01-01",
        ]);
        assert_eq!(invocation.request.limit, Some(5));
        assert_eq!(
            invocation.request.columns,
            Some(vec!["i".to_string(), "state".to_string()])
        );
        assert_eq!(invocation.request.filters.len(), 1);
        assert_eq!(invocation.request.since.as_deref(), Some("2026-01-01"));
        assert_eq!(invocation.request.partition["day"], "2026-01-01");
    }

    #[test]
    fn defaults_to_arrow_on_stdout() {
        let invocation = parse(&[]);
        assert_eq!(invocation.format, Format::Arrow);
        assert!(invocation.output.is_none());
        assert!(!invocation.describe);
        assert_eq!(invocation.request, SourceRequest::default());
    }

    #[test]
    fn rejects_a_partition_without_a_value() {
        let cli = Cli::parse_from(["source", "--partition", "day"]);
        assert!(cli.into_invocation().is_err());
    }
}
