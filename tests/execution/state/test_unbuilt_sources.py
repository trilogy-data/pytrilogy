"""A refresh never reads through a sibling output that has not been built yet
(issue #963): the planner falls back to the roots, and says so plainly when
nothing else can answer."""

from dataclasses import replace
from unittest.mock import patch

import pytest

from trilogy import Dialects
from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import Address
from trilogy.core.models.environment import Environment
from trilogy.execution.state.build_sources import (
    UnbuiltSourceError,
    hide_unbuilt_sources,
)
from trilogy.execution.state.state_store import (
    RefreshAssetError,
    StaleAsset,
    create_refresh_plan,
    execute_refresh_plan,
)
from trilogy.execution.state.watermarks import is_missing_file

ROOTS = """key id int;
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
"""

SIBLINGS = """
datasource wide (
    id,
    a,
    b,
)
grain (id)
file `{wide}`;

datasource target (
    id,
    a,
    b,
)
grain (id)
file `{target}`;
"""


def _executor(tmp_path, model: str, unbuilt: list[str]):
    """Parse with ``unbuilt`` present, then remove them: the planner's view of
    a remote output, which parse cannot stat and assumes exists."""
    for name in unbuilt:
        (tmp_path / name).touch()
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.parse_text(model)
    for name in unbuilt:
        (tmp_path / name).unlink()
    return executor


def _sibling_executor(tmp_path):
    return _executor(
        tmp_path,
        ROOTS
        + SIBLINGS.format(
            wide=(tmp_path / "wide.parquet").as_posix(),
            target=(tmp_path / "target.parquet").as_posix(),
        ),
        ["wide.parquet", "target.parquet"],
    )


def _only(plan, ds_id: str):
    asset = StaleAsset(datasource_id=ds_id, reason="test", explicit=True)
    return replace(plan, stale_assets=[], forced_assets=[asset])


def test_planner_prefers_the_sibling_when_it_exists(tmp_path):
    executor = _sibling_executor(tmp_path)
    target = executor.environment.datasources["target"]
    assert hide_unbuilt_sources(executor, target, set()) == {"wide"}


def test_target_builds_from_roots_while_sibling_unbuilt(tmp_path):
    executor = _sibling_executor(tmp_path)
    plan = _only(create_refresh_plan(executor), "target")

    result = execute_refresh_plan(executor, plan)

    assert result.refreshed_count == 1
    assert (tmp_path / "target.parquet").exists()
    assert not (tmp_path / "wide.parquet").exists()
    rows = executor.execute_raw_sql(
        f"select id, a, b from '{(tmp_path / 'target.parquet').as_posix()}'"
    ).fetchall()
    assert rows == [(1, "x", "y")]


def test_built_sibling_stays_eligible(tmp_path):
    executor = _sibling_executor(tmp_path)
    execute_refresh_plan(executor, _only(create_refresh_plan(executor), "wide"))
    target = executor.environment.datasources["target"]
    assert hide_unbuilt_sources(executor, target, set()) == set()


def test_unbuilt_only_source_reports_clearly(tmp_path):
    model = """key id int;
property id.a string;

root datasource ids (
    id: id,
)
grain (id)
query '''select 1 as id''';

datasource upstream (
    id,
    a,
)
grain (id)
file `{upstream}`;

datasource target (
    id,
    a,
)
grain (id)
file `{target}`;
""".format(
        upstream=(tmp_path / "upstream.parquet").as_posix(),
        target=(tmp_path / "target.parquet").as_posix(),
    )
    executor = _executor(tmp_path, model, ["upstream.parquet", "target.parquet"])
    plan = _only(create_refresh_plan(executor), "target")

    with pytest.raises(RefreshAssetError) as exc_info:
        execute_refresh_plan(executor, plan)

    assert isinstance(exc_info.value.original, UnbuiltSourceError)
    assert "not built yet: upstream" in str(exc_info.value)


def test_missing_remote_file_is_missing(tmp_path):
    executor = _sibling_executor(tmp_path)
    ds = executor.environment.datasources["wide"]
    ds.address = Address(
        location="https://example.com/wide.parquet",
        type=AddressType.PARQUET,
    )
    with patch.object(
        type(executor.generator),
        "get_file_columns",
        side_effect=RuntimeError("HTTP HEAD error (HTTP 404 Not Found)"),
    ):
        assert is_missing_file(ds, executor)


def test_unreachable_remote_file_still_raises(tmp_path):
    executor = _sibling_executor(tmp_path)
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
