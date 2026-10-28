//! A deterministic forecast table function served over HTTP for the vgi_http
//! tests: `forecast_daily(latitude, longitude, days := 3)`.
//!
//! Run it: `cargo run -- --http --port 8123`

use std::sync::Arc;

use arrow::array::{Date32Array, Float64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use vgi::catalog::CatalogModel;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
use vgi_rpc::{OutputCollector, RpcError};

/// 2030-01-01 as days since the epoch.
const FIRST_DAY: i32 = 21915;
const DEFAULT_DAYS: i64 = 3;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("time", DataType::Date32, false),
        Field::new("temperature_2m_max", DataType::Float64, false),
        Field::new("temperature_2m_min", DataType::Float64, false),
    ]))
}

struct ForecastDaily;

impl TableFunction for ForecastDaily {
    fn name(&self) -> &str {
        "forecast_daily"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description:
                "Daily forecast for a coordinate: max = latitude + day, min = longitude - day"
                    .to_string(),
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("latitude", 0, "double", "Latitude"),
            ArgSpec::const_arg("longitude", 1, "double", "Longitude"),
            ArgSpec::const_arg("days", -1, "int64", "Days to forecast"),
        ]
    }

    fn on_bind(&self, _params: &BindParams) -> vgi_rpc::Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema(),
            opaque_data: Vec::new(),
        })
    }

    fn producer(&self, params: &ProcessParams) -> vgi_rpc::Result<Box<dyn TableProducer>> {
        let args = &params.arguments;
        let (Some(latitude), Some(longitude)) = (args.const_f64(0), args.const_f64(1)) else {
            return Err(RpcError::runtime_error(
                "latitude and longitude are required",
            ));
        };
        let days = args.named_i64("days").unwrap_or(DEFAULT_DAYS) as i32;
        let offsets: Vec<f64> = (0..days).map(f64::from).collect();
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Date32Array::from_iter_values(
                    (0..days).map(|d| FIRST_DAY + d),
                )),
                Arc::new(Float64Array::from_iter_values(
                    offsets.iter().map(|d| latitude + d),
                )),
                Arc::new(Float64Array::from_iter_values(
                    offsets.iter().map(|d| longitude - d),
                )),
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let batch = batch
            .project(&project(&schema(), &params.output_schema))
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Box::new(Once(Some(batch))))
    }
}

fn project(full: &Schema, wanted: &Schema) -> Vec<usize> {
    wanted
        .fields()
        .iter()
        .filter_map(|f| full.index_of(f.name()).ok())
        .collect()
}

/// One batch, so the HTTP transport needs no continuation.
struct Once(Option<RecordBatch>);

impl TableProducer for Once {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> vgi_rpc::Result<Option<RecordBatch>> {
        Ok(self.0.take())
    }

    fn resume_supported(&self) -> bool {
        false
    }
}

fn main() {
    let mut worker = vgi::Worker::new();
    worker.register_table(ForecastDaily);
    worker.set_catalog(CatalogModel {
        name: "weather".to_string(),
        ..Default::default()
    });
    worker.run();
}
