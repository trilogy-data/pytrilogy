"""The pre-`run()` writers, kept because they ship in the published wheel."""

import pyarrow as pa

from trilogy.io.arrow import emit_arrow, emit_arrow_batches

TABLE = pa.Table.from_pylist([{"i": i} for i in range(4)])


def read(stdout: bytes) -> pa.Table:
    with pa.ipc.open_stream(stdout) as reader:
        return reader.read_all()


def test_emit_arrow_writes_a_stream_to_stdout(capsysbinary):
    emit_arrow(TABLE)
    assert read(capsysbinary.readouterr().out) == TABLE


def test_emit_arrow_batches_writes_the_batches_it_is_given(capsysbinary):
    emit_arrow_batches(iter(TABLE.to_batches()), TABLE.schema)
    assert read(capsysbinary.readouterr().out) == TABLE
