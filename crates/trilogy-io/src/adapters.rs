//! Turn whatever a source returned into an Arrow batch reader.
//!
//! The Rust analogue of the python adapter registry: a trait bound rather than
//! a probe list, since the type is known at compile time. Streaming inputs stay
//! streaming -- nothing here collects a reader into memory.

use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use crate::error::{Error, Result};

pub type BatchReader = Box<dyn RecordBatchReader + Send>;

pub trait IntoBatchReader {
    /// `schema` is the source's declared schema, if it has one. It is the only
    /// thing that can describe an empty result.
    fn into_batch_reader(self, schema: Option<SchemaRef>) -> Result<BatchReader>;
}

fn empty(schema: Option<SchemaRef>) -> Result<BatchReader> {
    let schema = schema.ok_or_else(|| {
        Error::adapter(
            "Cannot infer a schema from an empty result. Set .schema() so the \
             source still describes its columns when it has no rows.",
        )
    })?;
    Ok(Box::new(RecordBatchIterator::new(
        std::iter::empty(),
        schema,
    )))
}

impl IntoBatchReader for BatchReader {
    fn into_batch_reader(self, _schema: Option<SchemaRef>) -> Result<BatchReader> {
        Ok(self)
    }
}

impl IntoBatchReader for RecordBatch {
    fn into_batch_reader(self, _schema: Option<SchemaRef>) -> Result<BatchReader> {
        let schema = self.schema();
        Ok(Box::new(RecordBatchIterator::new(
            std::iter::once(Ok(self)),
            schema,
        )))
    }
}

impl IntoBatchReader for Vec<RecordBatch> {
    fn into_batch_reader(self, schema: Option<SchemaRef>) -> Result<BatchReader> {
        let Some(first) = self.first() else {
            return empty(schema);
        };
        let schema = schema.unwrap_or_else(|| first.schema());
        Ok(Box::new(RecordBatchIterator::new(
            self.into_iter().map(Ok),
            schema,
        )))
    }
}

impl IntoBatchReader for Option<RecordBatch> {
    fn into_batch_reader(self, schema: Option<SchemaRef>) -> Result<BatchReader> {
        match self {
            Some(batch) => batch.into_batch_reader(schema),
            None => empty(schema),
        }
    }
}

/// So a fallible source can be handed over directly, without unwrapping first.
impl<T, E> IntoBatchReader for std::result::Result<T, E>
where
    T: IntoBatchReader,
    E: std::error::Error + Send + Sync + 'static,
{
    fn into_batch_reader(self, schema: Option<SchemaRef>) -> Result<BatchReader> {
        match self {
            Ok(value) => value.into_batch_reader(schema),
            Err(e) => Err(Error::Source(Box::new(e))),
        }
    }
}

/// The Rust side of the same C stream interface python reads through
/// `__arrow_c_stream__` -- polars and datafusion both hand one over.
impl IntoBatchReader for FFI_ArrowArrayStream {
    fn into_batch_reader(self, _schema: Option<SchemaRef>) -> Result<BatchReader> {
        Ok(Box::new(ArrowArrayStreamReader::try_new(self)?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    fn count(reader: BatchReader) -> usize {
        reader.map(|b| b.unwrap().num_rows()).sum()
    }

    #[test]
    fn converts_the_supported_shapes() {
        assert_eq!(count(batch().into_batch_reader(None).unwrap()), 3);
        assert_eq!(
            count(vec![batch(), batch()].into_batch_reader(None).unwrap()),
            6
        );
        assert_eq!(count(Some(batch()).into_batch_reader(None).unwrap()), 3);
    }

    #[test]
    fn a_fallible_source_passes_through() {
        let ok: std::result::Result<RecordBatch, std::io::Error> = Ok(batch());
        assert_eq!(count(ok.into_batch_reader(None).unwrap()), 3);

        let failed: std::result::Result<RecordBatch, std::io::Error> =
            Err(std::io::Error::other("nope"));
        assert!(failed.into_batch_reader(None).is_err());
    }

    #[test]
    fn an_empty_result_needs_a_declared_schema() {
        assert!(Vec::<RecordBatch>::new().into_batch_reader(None).is_err());
        let schema = batch().schema();
        assert_eq!(
            count(
                Vec::<RecordBatch>::new()
                    .into_batch_reader(Some(schema))
                    .unwrap()
            ),
            0
        );
    }
}
