import duckdb

from trilogy import Dialects
from trilogy.core.models.build import BuildAggregateWrapper

ROWS = """
select 1 id, 'a' g1, 'x' g2, 10 v
union all select 2, 'a', 'y', 20
union all select 3, 'b', 'x', 5
union all select 4, null, null, 7
"""

FIXTURE = f"""
key id int;
property id.g1 string?;
property id.g2 string?;
property id.v int;
datasource t (id: id, g1: g1, g2: g2, v: v)
grain (id)
query '''{ROWS}''';
"""


def _executor():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(FIXTURE)
    return executor


def _oracle(group_by: str) -> list[tuple]:
    rows = duckdb.sql(
        f"select g1, g2, sum(v), grouping(g1), grouping(g2) from ({ROWS}) "
        f"group by {group_by}"
    ).fetchall()
    return sorted(rows, key=repr)


def test_arm_aggregates_carry_no_grouping_mode():
    assert "grouping" not in BuildAggregateWrapper.__dataclass_fields__
    assert "grouping_sets" not in BuildAggregateWrapper.__dataclass_fields__


def test_two_arms_of_one_statement_each_run_their_own_pass():
    body = "(select g1, g2, sum(v) as t, grouping(g1) as f1, grouping(g2) as f2)"
    rows = _executor().execute_text(f"""
with r as rollup({body}, g1, g2);
with c as cube({body}, g1, g2);
select *union(
    (select r.g1, r.g2, r.t, r.f1, r.f2, 'r' as src),
    (select c.g1, c.g2, c.t, c.f1, c.f2, 'c' as src)
) -> (k1, k2, tot, h1, h2, src);
""")[-1].fetchall()
    by_source: dict[str, list[tuple]] = {"r": [], "c": []}
    for row in rows:
        by_source[row[5]].append(tuple(row[:5]))
    assert sorted(by_source["r"], key=repr) == _oracle("rollup (g1, g2)")
    assert sorted(by_source["c"], key=repr) == _oracle("cube (g1, g2)")


def test_an_aggregate_pinned_at_the_pass_keys_is_computed_in_the_pass():
    rows = (
        _executor()
        .execute_text(
            "select *rollup((select g1, sum(v) as t, sum(v) by g1 as p, "
            "grouping(g1) as f1), g1);"
        )[-1]
        .fetchall()
    )
    assert sorted(map(tuple, rows), key=repr) == [
        ("a", 30, 30, 0),
        ("b", 5, 5, 0),
        (None, 42, 42, 1),
        (None, 7, 7, 0),
    ]


def test_an_aggregate_at_another_grain_stays_its_own_group():
    rows = (
        _executor()
        .execute_text(
            "select *rollup((select g1, sum(v) as t, max(v) by id as top), g1);"
        )[-1]
        .fetchall()
    )
    assert sorted(map(tuple, rows), key=repr) == [
        ("a", 30, 10),
        ("a", 30, 20),
        ("b", 5, 5),
        (None, 42, 7),
        (None, 7, 7),
    ]
