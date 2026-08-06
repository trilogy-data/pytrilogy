//! A data source in Rust, and the fixture the cross-language conformance test
//! compares against its python twin (`tests/io/conformance/landmarks.py`).
//!
//! Run it: `cargo run --example landmarks -- --limit 4 --filter 'state = CA' --format csv`

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use trilogy_io::{Result, SourceRequest};

const STATES: [&str; 4] = ["CA", "NY", "TX", "WA"];
const TOTAL: i64 = 100;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
    ]))
}

/// Claims `Limit`, so it stops generating early -- but only when there is no
/// filter left for the wrapper to apply, which `effective_pushdown` decides.
fn landmarks(request: &SourceRequest) -> Result<Vec<RecordBatch>> {
    let count = request.limit.map(|l| l as i64).unwrap_or(TOTAL).min(TOTAL);
    let ids: Vec<i64> = (0..count).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("landmark-{i}")).collect();
    let states: Vec<&str> = ids.iter().map(|i| STATES[(i % 4) as usize]).collect();
    Ok(vec![RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(states)),
        ],
    )?])
}

fn main() -> std::process::ExitCode {
    trilogy_io::source(landmarks)
        .pushdown(&[trilogy_io::Field::Limit])
        .schema(schema())
        .run()
}
