import pyarrow as pa
import sys


def emit_arrow(table: pa.Table) -> None:
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)


def emit_arrow_batches(batches: Iterator[pa.RecordBatch], schema: pa.Schema) -> None:
    with pa.ipc.new_stream(sys.stdout.buffer, schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
