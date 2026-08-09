//! `--describe`: what this source produces, without producing it.

use arrow::datatypes::{DataType, Schema};
use serde::Serialize;

use crate::contract::{Field, CONTRACT_VERSION};

#[derive(Debug, Serialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Serialize)]
pub struct Payload {
    pub contract: u32,
    pub schema: Vec<Column>,
    pub pushdown: Vec<String>,
    pub datasource: String,
}

pub fn trilogy_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "bool",
        DataType::Int8 | DataType::Int16 | DataType::Int32 => "int",
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => "int",
        DataType::Int64 | DataType::UInt64 => "bigint",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "float",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "numeric",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Timestamp(_, Some(_)) => "timestamp",
        DataType::Timestamp(_, None) => "datetime",
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "bytes",
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => "array",
        DataType::Struct(_) => "struct",
        DataType::Map(_, _) => "map",
        DataType::Null => "null",
        _ => "string",
    }
}

fn identifier(program: &str) -> String {
    let stem = Path::new(program)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "source".to_string());
    stem.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

use std::path::Path;

pub fn datasource_stub(schema: &Schema, program: &str) -> String {
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("    {}: {}", f.name(), f.name()))
        .collect();
    let grain = schema
        .fields()
        .first()
        .map(|f| f.name().as_str())
        .unwrap_or("");
    format!(
        "datasource {}(\n{}\n)\ngrain ({})\nfile `{}`;",
        identifier(program),
        columns.join(",\n"),
        grain,
        program
    )
}

pub fn payload(schema: &Schema, pushdown: &[Field], program: &str) -> Payload {
    Payload {
        contract: CONTRACT_VERSION,
        schema: schema
            .fields()
            .iter()
            .map(|f| Column {
                name: f.name().clone(),
                data_type: trilogy_type(f.data_type()).to_string(),
                nullable: f.is_nullable(),
            })
            .collect(),
        pushdown: pushdown.iter().map(|f| f.as_str().to_string()).collect(),
        datasource: datasource_stub(schema, program),
    }
}
