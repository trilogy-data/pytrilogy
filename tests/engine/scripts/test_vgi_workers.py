"""The VGI workers' own logic, called in-process.

The engine tests reach these classes only through worker processes DuckDB
launches; here each failure path is driven directly, with stand-in params.
"""

import sys
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

from tests.vgi_support import require_vgi
from trilogy.dialect.duckdb import vgi_failure

require_vgi()

from trilogy.dialect import duckdb_vgi
from trilogy.io import vgi as io_vgi

SCHEMA = pa.schema([("id", pa.int64()), ("name", pa.string())])
TABLE = pa.table({"id": [1, 2], "name": ["a", "b"]}, schema=SCHEMA)

STREAM = """
import sys, pyarrow as pa
table = pa.table({"id": [1, 2], "name": ["a", "b"]})
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as w:
    w.write_table(table)
"""
STREAM_THEN_EXIT = (
    STREAM + "\nsys.stdout.flush()\nsys.stderr.write('late failure')\nsys.exit(3)\n"
)
BATCH_THEN_GARBAGE = """
import sys, pyarrow as pa
schema = pa.schema([("id", pa.int64())])
writer = pa.ipc.new_stream(sys.stdout.buffer, schema)
writer.write_batch(pa.record_batch([pa.array([1])], schema=schema))
sys.stdout.buffer.write(b"\\x01" * 64)
sys.stdout.buffer.flush()
"""
LOCKED_ONCE = """
import sys, pathlib
marker = pathlib.Path(sys.argv[1])
if not marker.exists():
    marker.write_text("x")
    sys.stderr.write("error: failed to acquire lock on the uv cache")
    sys.exit(2)
""" + STREAM


class Out:
    def __init__(self):
        self.batches: list[pa.RecordBatch] = []
        self.finished = False

    def emit(self, batch: pa.RecordBatch) -> None:
        self.batches.append(batch)

    def finish(self) -> None:
        self.finished = True


def params(path: str, args: str = "", schema: pa.Schema = SCHEMA):
    return SimpleNamespace(
        args=SimpleNamespace(path=path, args=args), output_schema=schema
    )


@pytest.fixture
def script(monkeypatch):
    """Run ``code`` (the "path") with ``args`` under this interpreter."""
    monkeypatch.setattr(
        duckdb_vgi,
        "build_script_command",
        lambda code, args: [sys.executable, "-c", code, *args.split()],
    )
    monkeypatch.setattr(duckdb_vgi, "retry_delay", lambda attempt: 0)


def drain(cls, p) -> Out:
    state = cls.initial_state(p)
    out = Out()
    while not out.finished:
        cls.process(p, state, out)
    return out


def test_bridge_streams_and_projects(script):
    p = params(STREAM)
    assert duckdb_vgi.Script.on_bind(p).output_schema == SCHEMA
    out = drain(duckdb_vgi.Script, params(STREAM, schema=SCHEMA.remove(1)))
    assert pa.Table.from_batches(out.batches).to_pylist() == [{"id": 1}, {"id": 2}]


def test_bridge_starts_its_own_run_when_bind_was_elsewhere(script):
    out = drain(duckdb_vgi.Script, params(STREAM))
    assert pa.Table.from_batches(out.batches) == TABLE


def test_bridge_retries_a_transient_uv_failure(script, tmp_path: Path):
    marker = tmp_path / "locked"
    run = duckdb_vgi.start(LOCKED_ONCE, str(marker))
    assert run.reader.read_all() == TABLE
    assert run.close() == 0


def test_bridge_reports_a_script_that_fails_before_streaming(script):
    code = "import sys; sys.stderr.write('ValueError: bad config'); sys.exit(1)"
    with pytest.raises(RuntimeError) as raised:
        duckdb_vgi.start(code, "")
    error = vgi_failure(str(raised.value))
    assert error is not None
    assert (error.return_code, error.stderr) == (1, "ValueError: bad config")


def test_bridge_reports_an_exit_after_the_stream(script):
    with pytest.raises(RuntimeError) as raised:
        drain(duckdb_vgi.Script, params(STREAM_THEN_EXIT))
    error = vgi_failure(str(raised.value))
    assert error is not None
    assert (error.return_code, error.stderr) == (3, "late failure")


def test_bridge_reports_a_corrupt_stream(script):
    p = params(BATCH_THEN_GARBAGE, schema=pa.schema([("id", pa.int64())]))
    with pytest.raises(RuntimeError) as raised:
        drain(duckdb_vgi.Script, p)
    assert vgi_failure(str(raised.value)) is not None


def test_bridge_failure_without_stderr_names_the_cause():
    error = vgi_failure(str(duckdb_vgi._failure("/s.py", 1, "", ValueError("boom"))))
    assert error is not None
    assert error.stderr == "boom"


def test_bridge_reaps_binds_that_never_scanned(script, monkeypatch):
    duckdb_vgi.Script.on_bind(params(STREAM))
    monkeypatch.setattr(duckdb_vgi, "_ORPHAN_SECONDS", -1.0)
    duckdb_vgi._reap_orphans()
    assert not any(duckdb_vgi._PENDING.values())


def test_resident_projects_to_the_requested_columns():
    batch = pa.RecordBatch.from_pydict({"id": [1], "name": ["a"]})
    projected = io_vgi._project(batch, pa.schema([("name", pa.string())]))
    assert projected.to_pylist() == [{"name": "a"}]


def test_resident_reports_a_failure_to_start_a_scan(monkeypatch):
    def broken(args: str) -> pa.RecordBatchReader:
        raise ConnectionError("upstream down")

    monkeypatch.setattr(io_vgi.Source, "resolver", staticmethod(broken), raising=False)
    with pytest.raises(RuntimeError) as raised:
        io_vgi.Source.initial_state(params("", "--limit 1"))
    error = vgi_failure(str(raised.value))
    assert error is not None
    assert error.reported == {
        "type": "ConnectionError",
        "message": "upstream down",
        "contract": 1,
        "retryable": True,
    }


def test_resident_reports_a_failure_mid_scan():
    class Broken:
        def read_next_batch(self):
            raise ValueError("bad row")

    io_vgi._ACTIVE["scan"] = Broken()
    with pytest.raises(RuntimeError) as raised:
        io_vgi.Source.process(params(""), io_vgi.SourceState(scan="scan"), Out())
    assert "scan" not in io_vgi._ACTIVE
    error = vgi_failure(str(raised.value))
    assert error is not None
    assert error.reported is not None
    assert error.reported["message"] == "bad row"


def test_resident_rejects_unparseable_arguments():
    with pytest.raises(ValueError, match="invalid source arguments"):
        io_vgi.resolve_args(lambda: TABLE, None, None, "--no-such-flag")


def test_resident_resolves_like_a_one_shot_run():
    reader = io_vgi.resolve_args(lambda: TABLE, None, None, "--limit 1")
    assert reader.read_all().to_pylist() == [{"id": 1, "name": "a"}]


def test_haybarn_rollback_swallows_only_the_idle_case():
    import haybarn

    from trilogy.dialect.haybarn_engine import HaybarnDialect

    class Connection:
        def __init__(self, message: str):
            self.message = message

        def rollback(self):
            raise haybarn.TransactionException(self.message)

    dialect = HaybarnDialect()
    dialect.do_rollback(Connection("cannot rollback - no transaction is active"))
    with pytest.raises(haybarn.TransactionException):
        dialect.do_rollback(Connection("Conflict on update"))
