"""A file-backed datasource whose file does not carry the columns it declares is
stale, exactly as a table-backed one is.

The watermark cannot see this: adding a column to a model changes what the
asset must contain while every root it reads stays where it was, so a
watermark-fresh file would never be rebuilt and every read of the new column
would fail at execution.
"""

from unittest.mock import patch

import duckdb
import pytest

from trilogy import Dialects
from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import Address
from trilogy.core.models.environment import Environment
from trilogy.dialect.base import BaseDialect
from trilogy.execution.state.state_store import (
    create_refresh_plan,
    refresh_stale_assets,
)
from trilogy.execution.state.watermarks import schema_mismatch

MODEL = """key id int;
property id.a string;
property id.b string;
property <*>.source_through datetime;

root datasource source_rows (
    id: id,
    a: a,
    b: b,
)
grain (id)
query '''select 1 as id, 'x' as a, 'y' as b''';

root datasource source_time (
    source_through: source_through,
)
query '''select TIMESTAMP '2026-01-01 00:00:00' as source_through''';

datasource target (
    id,
    a,
    {extra}source_through,
)
grain (id)
file `{target}`
freshness by source_through;
"""


def _executor(tmp_path, target: str, extra: str = ""):
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.parse_text(MODEL.format(target=target, extra=extra))
    return executor


def _columns(path) -> list[str]:
    con = duckdb.connect()
    try:
        return [
            r[0]
            for r in con.execute(
                f"DESCRIBE SELECT * FROM '{path.as_posix()}'"
            ).fetchall()
        ]
    finally:
        con.close()


def _built(tmp_path, name: str):
    target = tmp_path / name
    executor = _executor(tmp_path, target.as_posix())
    assert refresh_stale_assets(executor).refreshed_count == 1
    assert _columns(target) == ["id", "a", "source_through"]
    return target


@pytest.mark.parametrize("name", ["target.parquet", "target.csv"])
def test_a_file_missing_a_declared_column_is_stale(tmp_path, name):
    target = _built(tmp_path, name)

    executor = _executor(tmp_path, target.as_posix(), extra="b,\n    ")
    plan = create_refresh_plan(executor)

    assert [a.datasource_id for a in plan.stale_assets] == ["target"]
    assert plan.stale_assets[0].reason == "schema changed: missing column b"
    assert not plan.stale_assets[0].filters.keys

    assert refresh_stale_assets(executor).refreshed_count == 1
    assert _columns(target) == ["id", "a", "b", "source_through"]


def test_a_file_carrying_an_undeclared_column_is_stale(tmp_path):
    target = tmp_path / "target.parquet"
    executor = _executor(tmp_path, target.as_posix(), extra="b,\n    ")
    assert refresh_stale_assets(executor).refreshed_count == 1

    executor = _executor(tmp_path, target.as_posix())
    plan = create_refresh_plan(executor)

    assert [a.datasource_id for a in plan.stale_assets] == ["target"]
    assert plan.stale_assets[0].reason == "schema changed: undeclared column b"


def test_a_file_matching_its_declaration_is_fresh(tmp_path):
    target = _built(tmp_path, "target.parquet")

    assert (
        create_refresh_plan(_executor(tmp_path, target.as_posix())).stale_assets == []
    )


def test_a_csv_is_judged_by_name_not_sniffed_type(tmp_path):
    """DuckDB infers CSV column types from the values, so a string column
    holding digits reads back as an integer; that is not a schema change."""
    target = tmp_path / "target.csv"
    con = duckdb.connect()
    con.execute(
        "COPY (SELECT 1 AS id, '7' AS a,"
        " TIMESTAMP '2026-01-01 00:00:00' AS source_through)"
        f" TO '{target.as_posix()}' (FORMAT CSV, HEADER)"
    )
    con.close()

    assert (
        create_refresh_plan(_executor(tmp_path, target.as_posix())).stale_assets == []
    )


def test_a_missing_file_is_reported_missing_not_mismatched(tmp_path):
    target = tmp_path / "target.parquet"

    plan = create_refresh_plan(_executor(tmp_path, target.as_posix()))

    assert [a.reason for a in plan.stale_assets] == ["file not found"]


def test_an_unexpected_describe_error_is_not_absorbed(tmp_path):
    """Only a missing or corrupt file yields no schema; a broken engine must
    not be rendered as a clean bill of health."""
    target = _built(tmp_path, "target.parquet")
    executor = _executor(tmp_path, target.as_posix())

    with patch.object(
        type(executor.generator), "get_file_columns", side_effect=RuntimeError("boom")
    ), pytest.raises(RuntimeError, match="boom"):
        create_refresh_plan(executor)


def test_a_dialect_that_cannot_describe_a_file_declines():
    address = Address(location="x.parquet", type=AddressType.PARQUET)

    assert BaseDialect().get_file_columns(None, address) is None


TYPED_MODEL = """key id int;
property id.x float;
property id.ts datetime;

datasource target (id, x, ts)
file `{target}`;
"""


def _typed_drift(tmp_path, select: str) -> str | None:
    target = tmp_path / "target.parquet"
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.parse_text(TYPED_MODEL.format(target=target.as_posix()))
    executor.execute_raw_sql(
        f"COPY ({select}) TO '{target.as_posix()}' (FORMAT PARQUET)"
    )
    return schema_mismatch(executor.environment.datasources["target"], executor)


@pytest.mark.parametrize(
    "select",
    [
        "SELECT 1 AS id, 1.5::DOUBLE AS x, TIMESTAMP '2026-01-01' AS ts",
        (
            "SELECT 1::BIGINT AS id, 1.5::FLOAT AS x,"
            " TIMESTAMPTZ '2026-01-01 00:00:00+00' AS ts"
        ),
    ],
)
def test_a_parquet_stored_at_another_width_is_not_drift(tmp_path, select):
    assert _typed_drift(tmp_path, select) is None


def test_a_parquet_column_of_another_kind_is_drift(tmp_path):
    drift = _typed_drift(
        tmp_path, "SELECT 1 AS id, '1.5' AS x, TIMESTAMP '2026-01-01' AS ts"
    )

    assert drift == "type of x (string vs float)"
