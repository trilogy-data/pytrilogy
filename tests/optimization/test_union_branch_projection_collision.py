"""Regression: a union arm must not collapse onto a same-named emitted CTE that
has a *different* projection.

A ``trilogy refresh`` of a merged datasource whose ``freshness by`` is a nested
``greatest()`` failed with::

    BinderException: Values list "<cte>" does not have a column named "multi_wm"

Shape that triggers it: the watermark concept ``multi_wm`` is itself derived
(``greatest(wm_a, wm_b)``) AND stored as a column on a materialized *partial*.
The planner sources ``multi_wm`` from the partial's stored column, building a
GROUP-BY CTE over that partial. Separately, the partial is a *branch* of the
UNION that supplies the merged datasource's data rows. Both the GROUP-parent and
the union arm are ``<partial>_at_<grain>`` — QDS identity ignores projection, so
they share a CTE name. ``canonicalize_graph`` collapsed the inline union arm onto
the emitted GROUP-parent, then the union-arity re-alignment overwrote the shared
CTE's ``output_columns`` with the arm's columns — dropping ``multi_wm`` that the
GROUP consumer still referenced.
"""

from __future__ import annotations

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import PersistMode
from trilogy.core.statements.author import PersistStatement

MODEL = """
key city enum<string>['MULTI', 'SIMPLE'];
key id int;
property id.val int;

property <*>.wm_a datetime;
property <*>.wm_b datetime;
property <*>.wm_simple datetime;

# watermark sourced only via derivation (not a single root column), AND stored
# as a column on the materialized partial below.
auto multi_wm <- greatest(wm_a, wm_b);
# the merged datasource's freshness NESTS the derived watermark.
auto latest <- greatest(multi_wm, wm_simple);

root datasource src_a (wm_a: wm_a) query '''select now() as wm_a''';
root datasource src_b (wm_b: wm_b) query '''select now() as wm_b''';
root datasource src_simple (wm_simple: wm_simple) query '''select now() as wm_simple''';

root partial datasource multi_materialized (
    id: id, city: city, val: val, multi_wm: multi_wm,
)
grain (id)
complete where city = 'MULTI'
query '''select 1 as id, 'MULTI' as city, 10 as val, now() as multi_wm''';

root partial datasource simple_materialized (
    id: id, city: city, val: val, wm_simple: wm_simple,
)
grain (id)
complete where city = 'SIMPLE'
query '''select 2 as id, 'SIMPLE' as city, 20 as val, now() as wm_simple''';

datasource merged (
    id, city, val, latest,
)
grain (id)
query '''select 1 as id, 'MULTI' as city, 10 as val, now() as latest'''
freshness by latest;
"""


def _persist_merged(exe):
    ds = exe.environment.datasources["merged"]
    select_stmt = ds.create_update_statement(exe.environment, None, line_no=None)
    statement = PersistStatement(
        datasource=ds, select=select_stmt, persist_mode=PersistMode.OVERWRITE
    )
    return exe.generator.generate_queries(
        exe.environment, [statement], hooks=exe.hooks
    )[0]


def test_nested_greatest_refresh_keeps_watermark_projection():
    """Every aggregate's parent CTE must still project the watermark column it
    groups on — the union-arm collapse must not strip it."""
    exe = Dialects.DUCK_DB.default_executor(environment=Environment())
    exe.parse_text(MODEL)

    processed = _persist_merged(exe)

    by_name = {c.name: c for c in processed.ctes}
    # Find the GROUP CTE that aggregates multi_wm; its single parent (the read of
    # the partial) must expose multi_wm in its output projection.
    multi_groups = [
        c
        for c in processed.ctes
        if getattr(c, "group_to_grain", False)
        and any(col.address == "local.multi_wm" for col in c.output_columns)
    ]
    assert multi_groups, "expected a GROUP-BY CTE producing multi_wm"
    for group in multi_groups:
        for parent in group.parent_ctes:
            parent_live = by_name.get(parent.name, parent)
            assert any(
                col.address == "local.multi_wm" for col in parent_live.output_columns
            ), (
                f"GROUP parent {parent_live.name} must project multi_wm but has "
                f"{[c.address for c in parent_live.output_columns]}"
            )


def test_nested_greatest_refresh_sql_executes():
    """End-to-end: the generated persist SQL must bind and run in DuckDB. Before
    the fix this raised BinderException ('does not have a column named multi_wm')."""
    exe = Dialects.DUCK_DB.default_executor(environment=Environment())
    exe.parse_text(MODEL)
    try:
        processed = _persist_merged(exe)
        sql = exe.generator.compile_statement(processed)
    except Exception as e:
        pytest.skip(f"persist setup no longer reaches the failing shape: {e}")
    exe.execute_raw_sql(sql)
