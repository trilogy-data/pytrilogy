"""Global (`by *`) aggregates beside authored scoped joins — the q23 family.

An abstract-grain (`by *`) aggregate is a single-row value broadcast onto every
output row through a keyless FULL join. The q23 collapse: MergeNode's
scoped-join key-exposure injection (the q59 key-drop fix) force-injected a join
key into the abstract-grain GroupNode, re-graining it to that key; the group's
CTE stopped grouping (`group_to_grain` false) and the renderer's grain-match
collapse dropped the aggregate function entirely — every row's "global max"
silently became its own value, FULL-joined back per key.

Sweeps join form (none / subset / union) x aggregate (max / count) x aggregate
source (union anchor / non-anchor rowset output / plain model concepts) x
position (select / having). Expected values are computed from the fact rows by
a python oracle; the broadcast cells also pin that the aggregate function
survives into the SQL text.
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

# (rid, cust, amount, yr)
FACT_ROWS: list[tuple[int, int, int, int]] = [
    (1, 1, 10, 2000),
    (2, 1, 20, 2001),
    (3, 2, 50, 2001),
    (4, 2, 40, 2000),
    (5, 3, 7, 2000),
]

# (a_id, a_val) / (b_id, b_val) — plain-concept sides, keys 1/2 vs 2/3
A_ROWS: list[tuple[int, int]] = [(1, 10), (2, 30)]
B_ROWS: list[tuple[int, int]] = [(2, 5), (3, 8)]

MODEL = """
key rid int;
property rid.cust int;
property rid.amount int;
property rid.yr int;
datasource fact (r: rid, c: cust, a: amount, y: yr) grain (rid)
query '''{fact}''';

with all_totals as
select cust, sum(amount) as total_all;

with recent_totals as
where yr = 2001
select cust, sum(amount) as total_recent;

key a_id int;
property a_id.a_val int;
datasource side_a (i: a_id, v: a_val) grain (a_id)
query '''{side_a}''';

key b_id int;
property b_id.b_val int;
datasource side_b (i: b_id, v: b_val) grain (b_id)
query '''{side_b}''';
"""


def _rows_sql(rows: list[tuple], cols: tuple[str, ...]) -> str:
    return " union all ".join(
        "select " + ", ".join(f"{v} {c}" for v, c in zip(r, cols)) for r in rows
    )


def _totals(year: int | None = None) -> dict[int, int]:
    out: dict[int, int] = {}
    for _, cust, amount, yr in FACT_ROWS:
        if year is None or yr == year:
            out[cust] = out.get(cust, 0) + amount
    return out


ALL_TOTALS = _totals()
RECENT_TOTALS = _totals(2001)
MAX_ALL = max(ALL_TOTALS.values())
MAX_RECENT = max(RECENT_TOTALS.values())
COUNT_RECENT = len(RECENT_TOTALS)


def _joined(m: int | None) -> list[tuple]:
    """FULL join of the two per-cust totals, plus the broadcast global `m`."""
    return sort_rows(
        [
            (cust, ALL_TOTALS.get(cust), RECENT_TOTALS.get(cust), m)
            for cust in set(ALL_TOTALS) | set(RECENT_TOTALS)
        ]
    )


def _run(tmp_path: Path, query: str, require_aggregate: str | None) -> list[tuple]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    model = MODEL.format(
        fact=_rows_sql(FACT_ROWS, ("r", "c", "a", "y")),
        side_a=_rows_sql(A_ROWS, ("i", "v")),
        side_b=_rows_sql(B_ROWS, ("i", "v")),
    )
    statements = engine.parse_text(model + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    if require_aggregate:
        assert (
            require_aggregate in sql.lower()
        ), f"global aggregate collapsed out of the SQL entirely:\n{sql}"
    rows = [
        tuple(int(x) if x is not None else None for x in r)
        for r in engine.execute_raw_sql(sql).fetchall()
    ]
    return sort_rows(rows)


def test_no_join_max_rowset(tmp_path: Path):
    rows = _run(
        tmp_path,
        "select recent_totals.cust, recent_totals.total_recent,"
        " max(recent_totals.total_recent) by * as m;",
        require_aggregate="max(",
    )
    want = [(cust, total, MAX_RECENT) for cust, total in RECENT_TOTALS.items()]
    assert rows == sort_rows(want)


def test_subset_join_max_rowset(tmp_path: Path):
    rows = _run(
        tmp_path,
        "select all_totals.cust, all_totals.total_all, recent_totals.total_recent,"
        " max(recent_totals.total_recent) by * as m "
        "subset join recent_totals.cust = all_totals.cust;",
        require_aggregate="max(",
    )
    assert rows == _joined(MAX_RECENT)


_GLOBAL_ORACLE = {
    "max": max(RECENT_TOTALS.values()),
    "count": len(RECENT_TOTALS),
    "sum": sum(RECENT_TOTALS.values()),
    "avg": sum(RECENT_TOTALS.values()) // len(RECENT_TOTALS),
}


@pytest.mark.parametrize("agg", ["max", "count", "sum", "avg"])
def test_union_join_global_agg_nonanchor_rowset(tmp_path: Path, agg: str):
    rows = _run(
        tmp_path,
        "select all_totals.cust, all_totals.total_all, recent_totals.total_recent,"
        f" {agg}(recent_totals.total_recent) by * as m "
        "union join all_totals.cust = recent_totals.cust;",
        require_aggregate=f"{agg}(",
    )
    assert rows == _joined(_GLOBAL_ORACLE[agg])


def test_union_join_max_anchor_rowset(tmp_path: Path):
    rows = _run(
        tmp_path,
        "select all_totals.cust, all_totals.total_all, recent_totals.total_recent,"
        " max(all_totals.total_all) by * as m "
        "union join all_totals.cust = recent_totals.cust;",
        require_aggregate="max(",
    )
    assert rows == _joined(MAX_ALL)


def test_union_join_count_rowset(tmp_path: Path):
    rows = _run(
        tmp_path,
        "select all_totals.cust, all_totals.total_all, recent_totals.total_recent,"
        " count(recent_totals.total_recent) by * as m "
        "union join all_totals.cust = recent_totals.cust;",
        require_aggregate="count(",
    )
    assert rows == _joined(COUNT_RECENT)


def test_union_join_max_in_having(tmp_path: Path):
    rows = _run(
        tmp_path,
        "select all_totals.cust, all_totals.total_all, recent_totals.total_recent "
        "union join all_totals.cust = recent_totals.cust "
        "having recent_totals.total_recent = max(recent_totals.total_recent) by *;",
        require_aggregate="max(",
    )
    want = [
        (cust, ALL_TOTALS.get(cust), total)
        for cust, total in RECENT_TOTALS.items()
        if total == MAX_RECENT
    ]
    assert rows == sort_rows(want)


def test_union_join_max_plain_concepts(tmp_path: Path):
    rows = _run(
        tmp_path,
        "select a_id, a_val, b_val, max(a_val) by * as m " "union join a_id = b_id;",
        require_aggregate="max(",
    )
    a, b = dict(A_ROWS), dict(B_ROWS)
    max_a = max(a.values())
    want = [(k, a.get(k), b.get(k), max_a) for k in set(a) | set(b)]
    assert rows == sort_rows(want)


def test_misgrained_broadcast_is_loud_not_silent(tmp_path: Path, monkeypatch):
    """Backstop for the render layer: if planner misbehavior re-grains a
    single-row aggregate again (here: the injection guard disabled), SQL
    generation must fail loudly rather than emit the identity collapse."""
    from trilogy.core.processing.nodes import merge_node

    monkeypatch.setattr(
        merge_node, "_abstract_output_grain", lambda parent, environment: False
    )
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    model = MODEL.format(
        fact=_rows_sql(FACT_ROWS, ("r", "c", "a", "y")),
        side_a=_rows_sql(A_ROWS, ("i", "v")),
        side_b=_rows_sql(B_ROWS, ("i", "v")),
    )
    statements = engine.parse_text(
        model
        + "select all_totals.cust, all_totals.total_all, recent_totals.total_recent,"
        " max(recent_totals.total_recent) by * as m "
        "union join all_totals.cust = recent_totals.cust;"
    )
    with pytest.raises(ValueError, match=r"aggregate local\.m rendered in CTE"):
        engine.generate_sql(statements[-1])
