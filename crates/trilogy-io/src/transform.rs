//! Enforce the contract fields a source did not take ownership of.
//!
//! Lazy: a `Transform` wraps the inner reader and narrows each batch as it goes,
//! and stops pulling once a limit is satisfied rather than draining the source.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchIterator, RecordBatchReader, Scalar,
};
use arrow::compute::kernels::cmp;
use arrow::compute::{
    cast, concat_batches, filter_record_batch, lexsort_to_indices, like, or, take, SortColumn,
    SortOptions,
};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::error::ArrowError;
use serde_json::Value;

use crate::adapters::BatchReader;
use crate::contract::{Field, Filter, Op, Sort, SourceRequest};
use crate::error::{Error, Result};

pub fn apply(
    reader: BatchReader,
    request: &SourceRequest,
    pushdown: &[Field],
) -> Result<BatchReader> {
    let owned = |field: Field| pushdown.contains(&field);
    let filters: Vec<Filter> = if owned(Field::Filters) {
        Vec::new()
    } else {
        request.filters.clone()
    };
    let columns = if owned(Field::Columns) {
        None
    } else {
        request.columns.clone()
    };
    let limit = if owned(Field::Limit) {
        None
    } else {
        request.limit
    };
    let order_by: Vec<Sort> = if owned(Field::OrderBy) {
        Vec::new()
    } else {
        request.order_by.clone()
    };

    let input_schema = reader.schema();
    if !filters.is_empty() {
        let names: Vec<&str> = filters.iter().map(|f| f.column.as_str()).collect();
        validate(&input_schema, &names, "filter on")?;
    }

    let indices = match &columns {
        Some(names) => {
            let refs: Vec<&str> = names.iter().map(String::as_str).collect();
            validate(&input_schema, &refs, "project")?;
            Some(
                refs.iter()
                    .map(|name| input_schema.index_of(name).unwrap())
                    .collect::<Vec<_>>(),
            )
        }
        None => None,
    };

    if !order_by.is_empty() {
        let names: Vec<&str> = order_by.iter().map(|s| s.column.as_str()).collect();
        validate(&input_schema, &names, "sort by")?;
    }

    if filters.is_empty() && indices.is_none() && limit.is_none() && order_by.is_empty() {
        return Ok(reader);
    }

    let schema = match &indices {
        Some(indices) => Arc::new(Schema::new_with_metadata(
            indices
                .iter()
                .map(|i| input_schema.field(*i).clone())
                .collect::<Vec<_>>(),
            input_schema.metadata().clone(),
        )),
        None => input_schema.clone(),
    };

    if !order_by.is_empty() {
        // Sorting cannot stream: the last batch can hold the first row. This is
        // why a source that can order itself should claim `OrderBy`.
        return sorted_reader(
            reader,
            &filters,
            &order_by,
            indices.as_deref(),
            limit,
            schema,
        );
    }

    Ok(Box::new(Transform {
        inner: reader,
        schema,
        filters,
        indices,
        limit,
        emitted: 0,
        done: false,
    }))
}

/// Filter, then sort, then limit, then project -- in that order.
fn sorted_reader(
    reader: BatchReader,
    filters: &[Filter],
    order_by: &[Sort],
    indices: Option<&[usize]>,
    limit: Option<usize>,
    schema: SchemaRef,
) -> Result<BatchReader> {
    let input_schema = reader.schema();
    let mut collected: Vec<RecordBatch> = Vec::new();
    for batch in reader {
        let mut batch = batch?;
        for predicate in filters {
            let mask = mask(&batch, predicate)?;
            batch = filter_record_batch(&batch, &mask)?;
        }
        if batch.num_rows() > 0 {
            collected.push(batch);
        }
    }
    let combined = concat_batches(&input_schema, &collected)?;
    let columns: Vec<SortColumn> = order_by
        .iter()
        .map(|sort| {
            Ok(SortColumn {
                values: combined
                    .column(input_schema.index_of(&sort.column)?)
                    .clone(),
                // nulls last in both directions, matching pyarrow's `at_end`.
                options: Some(SortOptions {
                    descending: sort.descending,
                    nulls_first: false,
                }),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let order = lexsort_to_indices(&columns, limit)?;
    let sorted: Vec<arrow::array::ArrayRef> = combined
        .columns()
        .iter()
        .map(|column| take(column, &order, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut batch = RecordBatch::try_new(input_schema, sorted)?;
    if let Some(indices) = indices {
        batch = batch.project(indices)?;
    }
    Ok(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch)),
        schema,
    )))
}

fn validate(schema: &Schema, columns: &[&str], verb: &str) -> Result<()> {
    let missing: Vec<&str> = columns
        .iter()
        .copied()
        .filter(|name| schema.index_of(name).is_err())
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let available: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    Err(Error::contract(format!(
        "Cannot {verb} {}: not produced by this source. Available columns: {}.",
        missing.join(", "),
        available.join(", ")
    )))
}

struct Transform {
    inner: BatchReader,
    schema: SchemaRef,
    filters: Vec<Filter>,
    indices: Option<Vec<usize>>,
    limit: Option<usize>,
    emitted: usize,
    done: bool,
}

impl Transform {
    fn narrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut batch = batch;
        for predicate in &self.filters {
            let mask = mask(&batch, predicate)?;
            batch = filter_record_batch(&batch, &mask)?;
        }
        if let Some(indices) = &self.indices {
            batch = batch.project(indices)?;
        }
        if let Some(limit) = self.limit {
            let remaining = limit - self.emitted;
            if batch.num_rows() > remaining {
                batch = batch.slice(0, remaining);
            }
            self.emitted += batch.num_rows();
            if self.emitted >= limit {
                self.done = true;
            }
        }
        Ok(batch)
    }
}

impl Iterator for Transform {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            let batch = match self.inner.next()? {
                Ok(batch) => batch,
                Err(e) => return Some(Err(e)),
            };
            match self.narrow(batch) {
                Err(e) => return Some(Err(ArrowError::ComputeError(e.to_string()))),
                Ok(batch) if batch.num_rows() > 0 => return Some(Ok(batch)),
                Ok(_) => continue,
            }
        }
        None
    }
}

impl RecordBatchReader for Transform {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn mask(batch: &RecordBatch, predicate: &Filter) -> Result<BooleanArray> {
    let index = batch.schema().index_of(&predicate.column)?;
    let column = batch.column(index);
    match predicate.op {
        Op::In | Op::NotIn => {
            let values = match &predicate.value {
                Value::Array(items) => items.clone(),
                single => vec![single.clone()],
            };
            let mut acc: Option<BooleanArray> = None;
            for value in values {
                let hit = cmp::eq(column, &scalar(&value, column.data_type())?)?;
                acc = Some(match acc {
                    Some(previous) => or(&previous, &hit)?,
                    None => hit,
                });
            }
            let hits = acc.unwrap_or_else(|| BooleanArray::from(vec![false; batch.num_rows()]));
            Ok(if predicate.op == Op::NotIn {
                arrow::compute::not(&hits)?
            } else {
                hits
            })
        }
        Op::Like => {
            let pattern = match &predicate.value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let column = cast(column, &DataType::Utf8)?;
            Ok(like(
                &column,
                &scalar(&Value::String(pattern), &DataType::Utf8)?,
            )?)
        }
        op => {
            let value = scalar(&predicate.value, column.data_type())?;
            Ok(match op {
                Op::Eq => cmp::eq(column, &value)?,
                Op::NotEq => cmp::neq(column, &value)?,
                Op::Lt => cmp::lt(column, &value)?,
                Op::LtEq => cmp::lt_eq(column, &value)?,
                Op::Gt => cmp::gt(column, &value)?,
                Op::GtEq => cmp::gt_eq(column, &value)?,
                _ => unreachable!("handled above"),
            })
        }
    }
}

/// A one-element array of the column's type, built from the JSON value.
///
/// Going through a natural Arrow type and casting keeps this to one path for
/// every column type -- dates and timestamps included -- instead of a match arm
/// per pairing.
fn scalar(value: &Value, target: &DataType) -> Result<Scalar<ArrayRef>> {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};

    let natural: ArrayRef = match value {
        Value::Bool(v) => Arc::new(BooleanArray::from(vec![*v])),
        Value::Number(n) if n.is_i64() => Arc::new(Int64Array::from(vec![n.as_i64().unwrap()])),
        Value::Number(n) => Arc::new(Float64Array::from(vec![n.as_f64().unwrap_or_default()])),
        Value::String(s) => Arc::new(StringArray::from(vec![s.clone()])),
        Value::Null => Arc::new(StringArray::from(vec![None::<String>])),
        other => Arc::new(StringArray::from(vec![other.to_string()])),
    };
    let cast_to = if natural.data_type() == target {
        None
    } else {
        Some(target)
    };
    let array = match cast_to {
        Some(target) => cast(&natural, target).map_err(|_| {
            Error::contract(format!(
                "Filter value {value} is not comparable with a {target} column"
            ))
        })?,
        None => natural,
    };
    Ok(Scalar::new(array))
}
