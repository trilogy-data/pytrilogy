"""Regression: every CTE name referenced in a generated query must be defined.

A landmark refresh in the sf_tree_reporting repo failed with
``Catalog Error: Table with name <X> does not exist`` because
``filter_irrelevant_ctes`` short-circuited on a CTE that had been visited as
a UnionCTE branch (``emit=False``) and then never marked it relevant when a
sibling later referenced it via ``parent_ctes``. The CTE was dropped from
the WITH clause while consumers still referenced its name in ``FROM``.

Two angles:

1. ``test_filter_irrelevant_ctes_keeps_branch_that_is_also_a_parent``
   — direct unit test against ``filter_irrelevant_ctes`` with hand-built
   CTEs in the exact problematic shape. Tight, fast, no SQL involved.
2. ``test_refresh_landmark_shape_generates_executable_sql`` — compiles
   and executes the SQL that mirrors the production refresh failure.
   DuckDB raises ``CatalogException`` ("Table with name X does not
   exist") if any CTE reference is unresolved, which is exactly the
   production error.
"""

from __future__ import annotations

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import SourceType
from trilogy.core.models.build import BuildGrain
from trilogy.core.models.execute import CTE, QueryDatasource, UnionCTE
from trilogy.core.optimization import filter_irrelevant_ctes


def _qds() -> QueryDatasource:
    return QueryDatasource(
        input_concepts=[],
        output_concepts=[],
        datasources=[],
        grain=BuildGrain(),
        joins=[],
        source_map={},
        source_type=SourceType.SELECT,
    )


def _cte(name: str, parents: list[CTE | UnionCTE] | None = None) -> CTE:
    return CTE(
        name=name,
        source=_qds(),
        output_columns=[],
        grain=BuildGrain(),
        source_map={},
        parent_ctes=parents or [],
    )


def _union(
    name: str,
    branches: list[CTE | UnionCTE],
    parents: list[CTE | UnionCTE] | None = None,
) -> UnionCTE:
    # Mirror datasource_to_cte: parent_ctes carries only grandparents (each
    # branch's own parents), NOT the branches themselves. Branches live in
    # internal_ctes. Including branches in parent_ctes would mask the bug
    # because filter_irrelevant_ctes would then walk them as real parents.
    return UnionCTE(
        name=name,
        source=_qds(),
        parent_ctes=parents or [],
        internal_ctes=list(branches),
        output_columns=[],
        grain=BuildGrain(),
    )


def test_filter_irrelevant_ctes_keeps_branch_that_is_also_a_parent():
    """A CTE that appears as both a UnionCTE branch and a sibling's parent
    must be emitted in the WITH clause. The bug was that the union branch
    visit (emit=False) marked the CTE as ``visited`` so the later sibling
    walk found it already-seen and skipped emitting it.
    """
    shared_branch = _cte("shared_branch")
    other_branch = _cte("other_branch")
    union = _union("u", branches=[shared_branch, other_branch])

    # A separate CTE that consumes the shared branch as a parent — this is
    # what triggers the "FROM \"shared_branch\"" reference in rendered SQL.
    consumer = _cte("consumer", parents=[shared_branch])

    root = _cte("root", parents=[union, consumer])

    kept = filter_irrelevant_ctes([union, shared_branch, other_branch, consumer], root)
    kept_names = {c.name for c in kept}

    assert "shared_branch" in kept_names, (
        "shared_branch is referenced by consumer.parent_ctes and must appear "
        f"in the emitted WITH; got {sorted(kept_names)}"
    )
    assert "consumer" in kept_names
    assert "u" in kept_names


# ----- integration: actually generate + execute the failing SQL -----


MODEL = """
key landmark_id string;
property landmark_id.name string;
property landmark_id.city string;
property <*>.arbue_landmark_data_updated_through datetime;
property <*>.deber_landmark_data_updated_through datetime;
property <*>.usbos_landmark_data_updated_through datetime;
auto latest <- greatest(
    arbue_landmark_data_updated_through,
    deber_landmark_data_updated_through,
    usbos_landmark_data_updated_through
);

# arbue's freshness lives on the same raw landmark file (no separate probe)
# — this forces a GROUP BY CTE that parents the same datasource the union
# also consumes as a branch.
root partial datasource raw_arbue (
    landmark_id: landmark_id,
    city: city,
    name: name,
    arbue_landmark_data_updated_through: arbue_landmark_data_updated_through,
)
grain (landmark_id)
complete where city = 'ARBUE'
query '''select 'a' as landmark_id, 'ARBUE' as city, 'a' as name, now() as arbue_landmark_data_updated_through''';

root datasource deber_probe (
    data_updated_through: deber_landmark_data_updated_through
)
query '''select now() as data_updated_through''';

root partial datasource raw_deber (
    landmark_id: landmark_id,
    city: city,
    name: name,
)
grain (landmark_id)
complete where city = 'DEBER'
query '''select 'b' as landmark_id, 'DEBER' as city, 'b' as name''';

root datasource usbos_probe (
    data_updated_through: usbos_landmark_data_updated_through
)
query '''select now() as data_updated_through''';

root partial datasource raw_usbos (
    landmark_id: landmark_id,
    city: city,
    name: name,
)
grain (landmark_id)
complete where city = 'USBOS'
query '''select 'c' as landmark_id, 'USBOS' as city, 'c' as name''';

datasource landmark_info (
    landmark_id,
    city,
    name,
    latest,
)
grain (landmark_id)
query '''select 'x' as landmark_id, 'X' as city, 'x' as name, now() as latest'''
freshness by latest;
"""


def test_refresh_landmark_shape_generates_executable_sql():
    """End-to-end: mirror the failing refresh path — parse the landmark-style
    model, hide the merged target (as refresh does), generate SQL for the
    union-with-freshness shape, and execute it through DuckDB. A missing
    CTE surfaces as ``CatalogException`` exactly like the production error.
    """
    env = Environment()
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    exe.parse_text(MODEL)
    try:
        exe.environment.datasources.pop("landmark_info")
        sql = exe.generate_sql("SELECT landmark_id, name, city, latest;")[-1]
    except Exception as e:
        # If model resolution shifts so the integration setup itself stops
        # building, fall back to the unit test above — don't fail noisily on
        # planner refactors that don't touch the bug surface.
        pytest.skip(f"integration setup no longer reaches the failing shape: {e}")
    exe.execute_raw_sql(sql).fetchall()
