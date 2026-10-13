//! The same rows as `source`, served as `resident.main.src(rows)` by a VGI
//! worker that DuckDB pools: no process starts per query.

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::catalog::CatalogModel;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
use vgi_rpc::{OutputCollector, Result};

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

struct Rows {
    full: RecordBatch,
    left: usize,
}

impl TableProducer for Rows {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.left == 0 {
            return Ok(None);
        }
        let n = self.left.min(BATCH);
        self.left -= n;
        Ok(Some(self.full.slice(0, n)))
    }

    fn resume_supported(&self) -> bool {
        // Only matters for HTTP continuation; this worker runs over stdio.
        false
    }
}

struct Src;

impl TableFunction for Src {
    fn name(&self) -> &str {
        "src"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "benchmark rows".to_string(),
            projection_pushdown: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("rows", 0, "int64", "row count")]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema(),
            opaque_data: Vec::new(),
        })
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let left = params.arguments.const_i64(0).unwrap_or(0).max(0) as usize;
        Ok(Box::new(Rows {
            full: batch(),
            left,
        }))
    }
}

fn main() {
    let mut worker = vgi::Worker::new();
    worker.register_table(Src);
    worker.set_catalog(CatalogModel {
        name: "resident".to_string(),
        ..Default::default()
    });
    worker.run();
}
