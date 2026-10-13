//! `--limit N` rows of (id, value, label), streamed as Arrow IPC on stdout.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use trilogy_io::{BatchReader, Result, SourceRequest};

const BATCH: usize = 65536;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

fn batch() -> RecordBatch {
    let ids: Vec<i64> = (0..BATCH as i64).collect();
    let values: Vec<f64> = ids.iter().map(|i| *i as f64 * 0.5).collect();
    let labels: Vec<String> = ids.iter().map(|i| format!("label_{}", i % 1000)).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(labels)),
        ],
    )
    .expect("valid batch")
}

fn rows(request: &SourceRequest) -> Result<BatchReader> {
    let full = batch();
    let total = request.limit.unwrap_or(1000) as usize;
    let batches =
        (0..total.div_ceil(BATCH)).map(move |i| Ok(full.slice(0, BATCH.min(total - i * BATCH))));
    Ok(Box::new(RecordBatchIterator::new(batches, schema())))
}

fn main() -> std::process::ExitCode {
    trilogy_io::source(rows)
        .pushdown(&[trilogy_io::Field::Limit])
        .schema(schema())
        .run()
}
