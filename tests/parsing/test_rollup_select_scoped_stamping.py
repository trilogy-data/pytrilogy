"""A SELECT-level `by rollup/cube/grouping sets` spec is a property of that one
select. It used to be stamped in place on shared AggregateWrapper objects — the
environment definition of `auto total <- sum(v)`, or the env-registered alias
of `sum(v) as t2` — so the spec leaked across statement boundaries: a later
plain select silently rendered ROLLUP (wrong rows), a later different-key
rollup kept the stale keys (parse error), and re-parsing the same text found
nothing left to stamp ("requires at least one aggregate"). Stamps now land on
select-local clones (`select.local_concepts` + `grouping_stamped_locals`) and
the shared definitions stay pristine.
"""

import pytest

from trilogy import Dialects
from trilogy.core.enums import AggregateGroupingMode

MODEL = """
key id int;
property id.g1 string?;
property id.g2 string?;
property id.v int;

datasource t (id: id, g1: g1, g2: g2, v: v)
grain (id)
query '''
select 1 id, 'a' g1, 'x' g2, 10 v
union all select 2, 'a', 'y', 20
union all select 3, 'b', 'x', 5
union all select 4, null, null, 7
''';

auto total <- sum(v);
auto tnp <- sum(v);
auto tesp <- sum(v * 2);
auto ratio <- tnp / tesp;
"""

ROLLUP_G1 = "select g1, total by rollup (g1);"


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(MODEL)
    return exec


def _rollups(sql: str) -> int:
    return sql.upper().count("ROLLUP")


def test_reparse_same_rollup_query(executor):
    first = executor.generate_sql(ROLLUP_G1)[0]
    second = executor.generate_sql(ROLLUP_G1)[0]
    assert first == second
    assert _rollups(second) == 1
    rows = sorted(executor.execute_raw_sql(second).fetchall(), key=str)
    assert rows == [("a", 30), ("b", 5), (None, 42), (None, 7)]


def test_env_definition_stays_unstamped(executor):
    executor.generate_sql(ROLLUP_G1)
    wrapper = executor.environment.concepts["total"].lineage
    assert wrapper.grouping == AggregateGroupingMode.STANDARD
    assert wrapper.by == []


def test_plain_select_after_rollup_select(executor):
    executor.generate_sql(ROLLUP_G1)
    sql = executor.generate_sql("select g1, g2, total;")[0]
    assert _rollups(sql) == 0
    assert len(executor.execute_raw_sql(sql).fetchall()) == 4


def test_two_rollup_selects_different_keys_one_script(executor):
    sqls = executor.generate_sql(
        "select g1, total by rollup (g1);\nselect g2, total by rollup (g2);"
    )
    assert _rollups(sqls[0]) == 1
    assert _rollups(sqls[1]) == 1
    rows = sorted(executor.execute_raw_sql(sqls[1]).fetchall(), key=str)
    assert rows == [("x", 15), ("y", 20), (None, 42), (None, 7)]


def test_rollup_then_plain_one_script(executor):
    sqls = executor.generate_sql(f"{ROLLUP_G1}\nselect g1, g2, total;")
    assert _rollups(sqls[0]) == 1
    assert _rollups(sqls[1]) == 0
    assert len(executor.execute_raw_sql(sqls[1]).fetchall()) == 4


def test_alias_defined_aggregate_not_stamped_in_env(executor):
    sqls = executor.generate_sql(
        "select g1, sum(v) as t2 by rollup (g1);\nselect g1, g2, t2;"
    )
    assert _rollups(sqls[0]) == 1
    assert _rollups(sqls[1]) == 0
    rows = sorted(executor.execute_raw_sql(sqls[0]).fetchall(), key=str)
    assert rows == [("a", 30), ("b", 5), (None, 42), (None, 7)]


def test_transitive_derived_measure_stamps_select_locally(executor):
    sql = executor.generate_sql("select g1, ratio by rollup (g1);")[0]
    assert _rollups(sql) == 1
    assert len(executor.execute_raw_sql(sql).fetchall()) == 4
    later = executor.generate_sql("select g1, g2, ratio;")[0]
    assert _rollups(later) == 0
