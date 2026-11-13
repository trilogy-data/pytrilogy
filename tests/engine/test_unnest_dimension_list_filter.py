"""Rows filtered by the unnest of a dimension's list, reached through a fact.

`event_id` and the unnested `group_region` are related only by `events`
binding `group_id` beside `event_id`, and `group_regions` being keyed on
`group_id`. The FINAL read the event rows from the root scan and the elements
from the unnest of them, then paired the two on nothing (`ON 1=1`), so every
event passed any element filter. Found by the fuzzer's `merged_axis_pin`
family (`formal/Trilogy/Binding.lean`)."""

import pytest

from tests.engine.test_grain_pin_unnest_merge_filter import MODEL as TREES
from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException

EVENTS = """
key group_id int;
key event_id int;
property group_id.group_regions list<int>;
auto group_region <- unnest(group_regions);
datasource events (eid: event_id, gid: group_id) grain (event_id)
query '''select 1 eid, 1 gid union all select 2, 2''';
root datasource lists (gid: group_id, regions: ?group_regions) grain (group_id)
query '''select 1 gid, [0, 1] regions union all select 2, [2] union all select 3, [1]''';
"""

MERGED = EVENTS + """
key region_id int;
merge group_region into region_id;
root datasource regions (rid: region_id) grain (region_id)
query '''select 0 rid union all select 1 union all select 2''';
"""

CASES = [
    (TREES, "select tree_id where ecoregion_id = 2;", [("t1",), ("t2",)]),
    (TREES, "select tree_id where tree_eco_region = 2;", [("t1",), ("t2",)]),
    (
        TREES,
        "select tree_id, species where ecoregion_id = 2;",
        [("t1", "oak"), ("t2", "pine")],
    ),
    (
        TREES,
        "select tree_id, ecoregion_id where ecoregion_id = 2;",
        [("t1", 2), ("t2", 2)],
    ),
    (TREES, "select species where ecoregion_id = 2;", [("oak",), ("pine",)]),
    (TREES, "select tree_id, latitude where ecoregion_id = 1;", [("t1", 1.0)]),
    (EVENTS, "select group_id, event_id where group_region = 1;", [(1, 1)]),
    (EVENTS, "select event_id where group_region = 1;", [(1,)]),
    (EVENTS, "select event_id, group_region where group_region = 1;", [(1, 1)]),
    (EVENTS, "select event_id, group_region;", [(1, 0), (1, 1), (2, 2)]),
    (MERGED, "select group_id, event_id where group_region = 1;", [(1, 1)]),
    (MERGED, "select event_id where group_region = 1;", [(1,)]),
    (MERGED, "select event_id, group_region where group_region = 1;", [(1, 1)]),
    (MERGED, "select group_id, event_id where region_id = 1;", [(1, 1)]),
    (MERGED, "select event_id where region_id = 1;", [(1,)]),
    (MERGED, "select group_id * 10 as g, event_id where region_id = 1;", [(10, 1)]),
]


@pytest.mark.parametrize("model, query, expected", CASES)
def test_element_filter_keeps_the_rows_it_matches(model, query, expected):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    sql = executor.generate_sql(query)[-1]
    assert "on 1=1" not in sql, sql
    rows = sorted(tuple(r) for r in executor.execute_raw_sql(sql).fetchall())
    assert rows == sorted(expected)


def test_correlated_unnest_cross_join_is_a_planner_error(monkeypatch):
    """An unnest carrying its parent's `group_id` is no literal `unnest([..])`:
    joined keyless it fans out, so the guard names it rather than shipping it."""
    from trilogy.core.processing.v4_helper import join_plan

    monkeypatch.setattr(join_plan, "_delivered_keys", lambda *_: frozenset())
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MERGED)
    with pytest.raises(UnresolvableQueryException, match="keyless join"):
        executor.generate_sql("select group_id, event_id where region_id = 1;")


def test_an_element_filter_pairs_on_the_element_and_its_row(monkeypatch):
    from trilogy.core.processing.v4_helper import strategy_builder

    pairs = []
    join = strategy_builder._join_element_filters

    def record(node, filters, environment):
        merged = join(node, filters, environment)
        sides = merged.planned_pairs.sides
        for spoke in merged.parents[1:]:
            held = {o.address for o in spoke.output_concepts}
            pairs.append(
                merged.planned_pairs.between(sides[id(node)], sides[id(spoke)])
                & held
            )
        return merged

    monkeypatch.setattr(strategy_builder, "_join_element_filters", record)
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(EVENTS)
    query = "select event_id, group_region where group_region = 1;"
    rows = executor.execute_raw_sql(executor.generate_sql(query)[-1]).fetchall()
    assert [tuple(r) for r in rows] == [(1, 1)]
    assert pairs == [{"local.group_region", "local.group_regions"}]
