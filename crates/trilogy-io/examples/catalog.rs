//! Several sources in one program, and the fixture the cross-language
//! conformance test compares against its python twin
//! (`tests/io/conformance/catalog.py`).
//!
//! Run it: `cargo run --example catalog -- --source states --format csv`

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use trilogy_io::{Result, SourceRequest};

const STATES: [&str; 4] = ["CA", "NY", "TX", "WA"];
const TOTAL: i64 = 100;

fn landmarks_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
    ]))
}

fn states_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("state", DataType::Utf8, false),
        Field::new("position", DataType::Int64, false),
    ]))
}

/// The same source as the `landmarks` example.
fn landmarks(request: &SourceRequest) -> Result<Vec<RecordBatch>> {
    let count = request.limit.map(|l| l as i64).unwrap_or(TOTAL).min(TOTAL);
    let ids: Vec<i64> = (0..count).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("landmark-{i}")).collect();
    let states: Vec<&str> = ids.iter().map(|i| STATES[(i % 4) as usize]).collect();
    Ok(vec![RecordBatch::try_new(
        landmarks_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(states)),
        ],
    )?])
}

fn states(_request: &SourceRequest) -> Result<Vec<RecordBatch>> {
    Ok(vec![RecordBatch::try_new(
        states_schema(),
        vec![
            Arc::new(StringArray::from(STATES.to_vec())),
            Arc::new(Int64Array::from(
                (0..STATES.len() as i64).collect::<Vec<_>>(),
            )),
        ],
    )?])
}

fn main() -> std::process::ExitCode {
    trilogy_io::sources()
        .add(
            "landmarks",
            trilogy_io::source(landmarks)
                .pushdown(&[trilogy_io::Field::Limit])
                .schema(landmarks_schema()),
        )
        .add("states", trilogy_io::source(states).schema(states_schema()))
        .run()
}
