//! Turn whatever a source returned into an Arrow batch reader.
//!
//! The Rust analogue of the python adapter registry: a trait bound rather than
//! a probe list, since the type is known at compile time. Streaming inputs stay
//! streaming -- nothing here collects a reader into memory.
//!
//! A declared schema wins over whatever the source produced, whichever impl
//! handled it -- see [`conform_reader`], which `resolve` applies centrally.

use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use crate::error::{Error, Result};

pub type BatchReader = Box<dyn RecordBatchReader + Send>;

pub trait IntoBatchReader {
    /// `schema` is the source's declared schema, if it has one. Impls only need
    /// it to describe an empty result; [`conform_reader`] enforces it on the
    /// rows themselves, so ignoring it here is safe.
    fn into_batch_reader(self, schema: Option<SchemaRef>) -> Result<BatchReader>;
}

/// Make a declared schema authoritative over whatever the source produced.
///
/// Applied at one seam rather than in each impl, because most of them carry a
/// schema of their own and would otherwise silently win: a source returning a
/// `RecordBatch` and one returning `Vec<RecordBatch>` must answer `--describe`
/// the same way they fill the stream. The declared schema also fixes column
/// order and drops what it does not name, so it is a projection as much as a
/// cast. Mirrors `trilogy.io.adapters.to_reader`.
pub fn conform_reader(reader: BatchReader, schema: Option<SchemaRef>) -> Result<BatchReader> {
    let Some(schema) = schema else {
        return Ok(reader);
    };
    let produced = reader.schema();
    if produced == schema {
        return Ok(reader);
    }
    let missing: Vec<&str> = schema
        .fields()
        .iter()
        .filter(|field| produced.field_with_name(field.name()).is_err())
        .map(|field| field.name().as_str())
        .collect();
    if !missing.is_empty() {
        let names: Vec<&str> = produced
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        return Err(Error::adapter(format!(
            "Source produced columns [{}], which do not cover its declared \
             schema: {} missing.",
            names.join(", "),
            missing.join(", ")
        )));
    }
    Ok(Box::new(Conformed {
        inner: reader,
        schema,
    }))
}

fn conform(batch: RecordBatch, schema: &SchemaRef) -> std::result::Result<RecordBatch, ArrowError> {
    if batch.schema() == *schema {
        return Ok(batch);
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let column = batch.column_by_name(field.name()).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "batch is missing declared column {}",
                    field.name()
                ))
            })?;
            arrow::compute::cast(column, field.data_type())
        })
        .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
    // `try_new` rejects a 0-row batch built from no columns, so an empty schema
    // has to keep the row count it came in with.
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

struct Conformed {
    inner: BatchReader,
    schema: SchemaRef,
}

impl Iterator for Conformed {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.inner.next()?;
        Some(batch.and_then(|b| conform(b, &self.schema)))
    }
}

impl RecordBatchReader for Conformed {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
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

    use arrow::array::{Int32Array, Int64Array, StringArray};
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

    // --- a declared schema is authoritative ---------------------------------

    fn declared() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]))
    }

    /// Deliberately the wrong types, wrong order, and one column too many.
    fn divergent() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("extra", DataType::Int64, true),
            Field::new("i", DataType::Int32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![9, 9])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap()
    }

    fn conformed(batch: RecordBatch) -> BatchReader {
        let reader = batch.into_batch_reader(None).unwrap();
        conform_reader(reader, Some(declared())).unwrap()
    }

    #[test]
    fn a_declared_schema_casts_reorders_and_narrows() {
        let mut reader = conformed(divergent());
        assert_eq!(reader.schema(), declared());
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.schema(), declared());
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );
    }

    #[test]
    fn a_matching_schema_is_not_rewrapped() {
        let reader = batch().into_batch_reader(None).unwrap();
        let schema = reader.schema();
        assert_eq!(count(conform_reader(reader, Some(schema)).unwrap()), 3);
    }

    #[test]
    fn no_declared_schema_leaves_the_source_alone() {
        let reader = divergent().into_batch_reader(None).unwrap();
        let produced = reader.schema();
        assert_eq!(conform_reader(reader, None).unwrap().schema(), produced);
    }

    #[test]
    fn a_column_the_declared_schema_names_must_exist() {
        let reader = batch().into_batch_reader(None).unwrap();
        let Err(error) = conform_reader(reader, Some(declared())) else {
            panic!("a declared column the source omits must be an error");
        };
        assert!(error.to_string().contains("s missing"), "{error}");
    }

    #[test]
    fn conforming_preserves_an_empty_batch() {
        let source = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let empty =
            RecordBatch::try_new(source, vec![Arc::new(Int32Array::from(Vec::<i32>::new()))])
                .unwrap();
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]));
        let batch = conform(empty, &target).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), target);
    }

    #[test]
    fn a_declared_schema_reaches_a_vec_of_batches() {
        let reader = vec![divergent(), divergent()]
            .into_batch_reader(None)
            .unwrap();
        let reader = conform_reader(reader, Some(declared())).unwrap();
        assert_eq!(reader.schema(), declared());
        assert_eq!(count(reader), 4);
    }
}
