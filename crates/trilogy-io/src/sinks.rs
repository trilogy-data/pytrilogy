//! Write an Arrow stream out in the format the caller asked for.
//!
//! Batches are written as they arrive, so a source is never fully buffered.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use arrow::array::RecordBatchReader;
use clap::ValueEnum;

use crate::adapters::BatchReader;
use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum Format {
    #[default]
    Arrow,
    Parquet,
    Csv,
    Json,
}

impl Format {
    pub fn as_str(&self) -> &'static str {
        match self {
            Format::Arrow => "arrow",
            Format::Parquet => "parquet",
            Format::Csv => "csv",
            Format::Json => "json",
        }
    }
}

pub fn write(reader: BatchReader, format: Format, output: Option<&str>) -> Result<usize> {
    match output {
        Some(path) => {
            if let Some(parent) = Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            let sink = BufWriter::new(File::create(path)?);
            write_to(reader, format, sink)
        }
        // `Stdout` rather than `StdoutLock`: parquet's writer needs `Send`, and
        // the lock guard is not.
        None => write_to(reader, format, BufWriter::new(std::io::stdout())),
    }
}

pub fn write_to<W: Write + Send>(reader: BatchReader, format: Format, sink: W) -> Result<usize> {
    match format {
        Format::Arrow => write_arrow(reader, sink),
        Format::Parquet => write_parquet(reader, sink),
        Format::Csv => write_csv(reader, sink),
        Format::Json => write_json(reader, sink),
    }
}

fn write_arrow<W: Write + Send>(reader: BatchReader, sink: W) -> Result<usize> {
    let mut rows = 0;
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(sink, &reader.schema())?;
    for batch in reader {
        let batch = batch?;
        writer.write(&batch)?;
        rows += batch.num_rows();
    }
    writer.finish()?;
    Ok(rows)
}

fn write_parquet<W: Write + Send>(reader: BatchReader, sink: W) -> Result<usize> {
    let mut rows = 0;
    let mut writer = parquet::arrow::ArrowWriter::try_new(sink, reader.schema(), None)?;
    for batch in reader {
        let batch = batch?;
        writer.write(&batch)?;
        rows += batch.num_rows();
    }
    writer.close()?;
    Ok(rows)
}

fn write_csv<W: Write + Send>(reader: BatchReader, sink: W) -> Result<usize> {
    let mut rows = 0;
    // Quote non-numeric fields, matching pyarrow's `needed` style. pyarrow
    // always quotes the header regardless, so these bytes only line up with the
    // python implementation at this setting (tests/io/test_conformance.py).
    let mut writer = arrow::csv::WriterBuilder::new()
        .with_quote_style(arrow::csv::writer::QuoteStyle::NonNumeric)
        .build(sink);
    for batch in reader {
        let batch = batch?;
        writer.write(&batch)?;
        rows += batch.num_rows();
    }
    Ok(rows)
}

fn write_json<W: Write + Send>(reader: BatchReader, sink: W) -> Result<usize> {
    let mut rows = 0;
    let mut writer = arrow::json::LineDelimitedWriter::new(sink);
    for batch in reader {
        let batch = batch?;
        writer.write(&batch)?;
        rows += batch.num_rows();
    }
    writer.finish()?;
    Ok(rows)
}
