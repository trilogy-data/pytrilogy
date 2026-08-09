"""Legacy direct-to-stdout writers.

Kept because they ship in the published wheel. New code should use
:func:`trilogy.io.run`, which routes through :mod:`trilogy.io.sinks` and picks
up the format and output options.
"""

from collections.abc import Iterator

import pyarrow as pa

from trilogy.io.sinks import Format, write


def emit_arrow(table: pa.Table) -> None:
    write(table.to_reader(), Format.ARROW)


def emit_arrow_batches(batches: Iterator[pa.RecordBatch], schema: pa.Schema) -> None:
    write(pa.RecordBatchReader.from_batches(schema, batches), Format.ARROW)
