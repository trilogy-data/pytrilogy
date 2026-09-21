"""An incremental refresh of a file-backed datasource keeps the file's rows.

A file has no INSERT: a persist to one is rewritten whole, which is right for
an OVERWRITE and destroys the asset on an APPEND. The refresh that a scoped
(incremental or sliced) rebuild produces is an APPEND precisely because it
writes only part of the table, so the rows already in the file have to be part
of what is written back.
"""

import duckdb
import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.execution.state.state_store import refresh_stale_assets

MODEL = """key ev_id int;
property ev_id.ev_ts datetime;

root datasource src (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{src}`;

datasource target (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{target}`
incremental by ev_ts;
"""

PARTITIONED = """key ev_id int;
property ev_id.ev_ts datetime;
property ev_id.ev_day date;

root datasource src (
    ev_id: ev_id,
    ev_ts: ev_ts,
    ev_day: ev_day
)
grain (ev_id)
query '''
select 1 as ev_id, timestamp '2024-01-10 12:00:00' as ev_ts, date '2024-01-10' as ev_day
union all
select 2 as ev_id, timestamp '2024-01-15 12:00:00' as ev_ts, date '2024-01-15' as ev_day
''';

datasource target (
    ev_id: ev_id,
    ev_ts: ev_ts,
    ev_day: ev_day
)
grain (ev_id)
file `{target}`
incremental by ev_ts
partition by ev_day;
"""

SOURCE_ROWS = """
SELECT 1 AS ev_id, TIMESTAMP '2024-01-10 12:00:00' AS ev_ts
UNION ALL SELECT 2, TIMESTAMP '2024-01-15 12:00:00'
"""


def _write(path, select: str) -> None:
    con = duckdb.connect()
    try:
        con.execute(f"COPY ({select}) TO '{path.as_posix()}' (FORMAT PARQUET)")
    finally:
        con.close()


def _rows(path) -> list[tuple]:
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT * FROM read_parquet('{path.as_posix()}') ORDER BY 1"
        ).fetchall()
    finally:
        con.close()


def _csv_rows(path) -> list[tuple]:
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT * FROM read_csv('{path.as_posix()}') ORDER BY 1"
        ).fetchall()
    finally:
        con.close()


def _executor(tmp_path, model: str, **paths):
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.parse_text(model.format(**paths))
    return executor


@pytest.fixture
def behind(tmp_path):
    """A target holding the first of the root's two rows."""
    src = tmp_path / "src.parquet"
    target = tmp_path / "target.parquet"
    _write(src, SOURCE_ROWS)
    _write(target, f"SELECT * FROM ({SOURCE_ROWS}) WHERE ev_id = 1")
    return src, target


def test_incremental_refresh_keeps_the_rows_already_in_the_file(tmp_path, behind):
    src, target = behind
    executor = _executor(tmp_path, MODEL, src=src.as_posix(), target=target.as_posix())

    assert refresh_stale_assets(executor).refreshed_count == 1

    assert [r[0] for r in _rows(target)] == [1, 2]


def test_a_second_refresh_neither_duplicates_nor_drops(tmp_path, behind):
    src, target = behind
    executor = _executor(tmp_path, MODEL, src=src.as_posix(), target=target.as_posix())
    refresh_stale_assets(executor)

    again = _executor(tmp_path, MODEL, src=src.as_posix(), target=target.as_posix())
    assert refresh_stale_assets(again).refreshed_count == 0

    assert [r[0] for r in _rows(target)] == [1, 2]


def test_a_missing_target_is_built_from_scratch(tmp_path):
    src = tmp_path / "src.parquet"
    target = tmp_path / "target.parquet"
    _write(src, SOURCE_ROWS)
    executor = _executor(tmp_path, MODEL, src=src.as_posix(), target=target.as_posix())

    assert refresh_stale_assets(executor).refreshed_count == 1

    assert [r[0] for r in _rows(target)] == [1, 2]


def test_an_authored_append_to_a_missing_file_writes_only_its_rows(tmp_path):
    src = tmp_path / "src.parquet"
    target = tmp_path / "target.parquet"
    _write(src, SOURCE_ROWS)
    executor = _executor(tmp_path, MODEL, src=src.as_posix(), target=target.as_posix())

    executor.execute_text(
        "append into target from where ev_id = 2 select ev_id, ev_ts;"
    )

    assert [r[0] for r in _rows(target)] == [2]


def test_a_csv_target_keeps_its_rows_too(tmp_path):
    src = tmp_path / "src.parquet"
    target = tmp_path / "target.csv"
    _write(src, SOURCE_ROWS)
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT * FROM ({SOURCE_ROWS}) WHERE ev_id = 1) "
        f"TO '{target.as_posix()}' (FORMAT CSV, HEADER)"
    )
    con.close()
    executor = _executor(tmp_path, MODEL, src=src.as_posix(), target=target.as_posix())

    assert refresh_stale_assets(executor).refreshed_count == 1

    assert [r[0] for r in _csv_rows(target)] == [1, 2]


def test_a_reordered_target_is_unioned_by_name(tmp_path, behind):
    """The file's column order is whatever the last write left it, not the
    select's, so the two sides cannot be matched positionally."""
    src, target = behind
    _write(target, "SELECT TIMESTAMP '2024-01-10 12:00:00' AS ev_ts, 1 AS ev_id")
    executor = _executor(tmp_path, MODEL, src=src.as_posix(), target=target.as_posix())

    refresh_stale_assets(executor)

    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT ev_id, ev_ts FROM read_parquet('{target.as_posix()}') ORDER BY 1"
        ).fetchall()
    finally:
        con.close()
    assert [r[0] for r in rows] == [1, 2]


def test_a_partitioned_file_target_refuses_the_append(tmp_path):
    target = tmp_path / "target.parquet"
    _write(
        target,
        "SELECT 1 AS ev_id, TIMESTAMP '2024-01-10 12:00:00' AS ev_ts,"
        " DATE '2024-01-10' AS ev_day",
    )
    executor = _executor(tmp_path, PARTITIONED, target=target.as_posix())

    with pytest.raises(Exception, match="partitioned file target"):
        refresh_stale_assets(executor)
