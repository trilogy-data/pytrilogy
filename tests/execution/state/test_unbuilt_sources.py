"""A remote output that has never been built is stale, so a refresh builds it
rather than reading through it (#963).

Parse cannot stat a remote address and assumes it exists, so the planner may
route a sibling through it; only staleness puts it in the plan, where pending
assets are hidden from each other's builds.
"""

import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import pytest

from trilogy import Dialects
from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import Address
from trilogy.core.models.environment import Environment
from trilogy.execution.state.state_store import (
    create_refresh_plan,
    execute_refresh_plan,
)
from trilogy.execution.state.watermarks import is_missing_file

MODEL = """key id int;
property id.a string;
property id.b string;

root datasource left_rows (
    id: id,
    a: a,
)
grain (id)
query '''select 1 as id, 'x' as a''';

root datasource right_rows (
    id: id,
    b: b,
)
grain (id)
query '''select 1 as id, 'y' as b''';

datasource wide (
    id,
    a,
    b,
)
grain (id)
file `{wide_read}`:`{wide_write}`;

datasource target (
    id,
    a,
    b,
)
grain (id)
file `{target}`;
"""


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass


@pytest.fixture
def served(tmp_path):
    root = tmp_path / "served"
    root.mkdir()
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(_QuietHandler, directory=str(root))
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield root, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


def _executor(tmp_path, served):
    root, url = served
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.parse_text(
        MODEL.format(
            wide_read=f"{url}/wide.parquet",
            wide_write=(root / "wide.parquet").as_posix(),
            target=(tmp_path / "target.parquet").as_posix(),
        )
    )
    return executor


def test_unbuilt_remote_sibling_is_planned(tmp_path, served):
    executor = _executor(tmp_path, served)

    reasons = {
        a.datasource_id: a.reason for a in create_refresh_plan(executor).refresh_assets
    }

    assert reasons == {"wide": "file not found", "target": "file not found"}


def test_fresh_model_first_refresh_builds(tmp_path, served):
    executor = _executor(tmp_path, served)

    result = execute_refresh_plan(executor, create_refresh_plan(executor))

    assert result.refreshed_count == 2
    rows = executor.execute_raw_sql(
        f"select id, a, b from '{(tmp_path / 'target.parquet').as_posix()}'"
    ).fetchall()
    assert rows == [(1, "x", "y")]
    assert (served[0] / "wide.parquet").exists()


def test_built_remote_sibling_is_not_missing(tmp_path, served):
    executor = _executor(tmp_path, served)
    execute_refresh_plan(executor, create_refresh_plan(executor))

    assert not is_missing_file(executor.environment.datasources["wide"], executor)


def test_unreachable_remote_file_still_raises(tmp_path, served):
    executor = _executor(tmp_path, served)
    ds = executor.environment.datasources["wide"]
    ds.address = Address(
        location="https://example.com/wide.parquet",
        type=AddressType.PARQUET,
    )
    with (
        patch.object(
            type(executor.generator),
            "get_file_columns",
            side_effect=RuntimeError("HTTP 403 Forbidden"),
        ),
        pytest.raises(RuntimeError),
    ):
        is_missing_file(ds, executor)


def test_corrupt_remote_file_is_not_missing(tmp_path, served):
    executor = _executor(tmp_path, served)
    (served[0] / "wide.parquet").write_bytes(b"<html>not parquet</html>")

    assert not is_missing_file(executor.environment.datasources["wide"], executor)


def test_dialect_without_file_schema_is_not_missing(tmp_path, served):
    executor = _executor(tmp_path, served)
    with patch.object(type(executor.generator), "get_file_columns", return_value=None):
        assert not is_missing_file(executor.environment.datasources["wide"], executor)
