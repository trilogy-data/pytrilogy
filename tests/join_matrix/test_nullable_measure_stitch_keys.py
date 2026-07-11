"""Nullable rowset measures must stay payload — never ordinary stitch-join keys.

The q59 wrong-result shape: two aggregate rowsets at grain (store, week), each
projecting filtered sums (`sum(sales ? dow = N)`), related ONLY by an authored
`union join` on store and an offset week. Discovery leaves one side's bare
rowset node in the stack and re-sources it through the other side's enrichment;
loop completion then stitches the two materializations on EVERY shared visible
concept — promoting the filtered sums into INNER JOIN equality predicates.

A filtered sum is NULL for any group with no matching rows. `NULL = NULL` is
not true, so the stitch silently drops exactly the rows whose measure payload
is legitimately NULL (q59 lost week 5279 for all six stores).

Contract: internal stitch joins are keyed by grain/key concepts; a measure may
only remain a join key when it is the sole connection, and then only with
null-safe equality.
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.enums import Modifier, Purpose
from trilogy.core.models.environment import Environment

# (rid, store, week, year, dow, sales)
# store 10: only dow=2 rows both years -> sun and mon sums NULL on both sides
# store 20: dow 0 and 1 rows both years -> all measures populated (control)
# store 30: dow 0 rows only both years -> sun populated, mon NULL both sides
FACT_ROWS: list[tuple[int, int, int, int, int, int]] = [
    (1, 10, 1, 2001, 2, 5),
    (2, 10, 53, 2002, 2, 7),
    (3, 20, 1, 2001, 0, 11),
    (4, 20, 1, 2001, 1, 13),
    (5, 20, 53, 2002, 0, 17),
    (6, 20, 53, 2002, 1, 19),
    (7, 30, 1, 2001, 0, 23),
    (8, 30, 53, 2002, 0, 29),
]

MODEL = """
key rid int;
property rid.store int;
property rid.week int;
property rid.year int;
property rid.dow int;
property rid.sales int;
datasource fact (r: rid, s: store, w: week, y: year, d: dow, v: sales) grain (rid)
query '''{source}''';

with this_year as
where year = 2001
select
    store,
    week,
    sum(sales ? dow = 0) as sun,
    sum(sales ? dow = 1) as mon
;

with next_year as
where year = 2002
select
    store,
    week,
    sum(sales ? dow = 0) as sun,
    sum(sales ? dow = 1) as mon
;
"""

QUERY = """
select
    this_year.store as store_code,
    this_year.week as wk,
    case when this_year.sun is not null and next_year.sun is not null and next_year.sun != 0
        then this_year.sun / next_year.sun end as sun_ratio,
    case when this_year.mon is not null and next_year.mon is not null and next_year.mon != 0
        then this_year.mon / next_year.mon end as mon_ratio
union join this_year.store = next_year.store
union join this_year.week = next_year.week - 52
where this_year.store is not null and next_year.store is not null
order by store_code asc, wk asc
;
"""


def _source_sql() -> str:
    return " union all ".join(
        f"select {r} r, {s} s, {w} w, {y} y, {d} d, {v} v"
        for r, s, w, y, d, v in FACT_ROWS
    )


def _aggregate(year: int) -> dict[tuple[int, int], dict[int, int]]:
    out: dict[tuple[int, int], dict[int, int]] = {}
    for _, store, week, row_year, dow, sales in FACT_ROWS:
        if row_year == year:
            sums = out.setdefault((store, week), {})
            sums[dow] = sums.get(dow, 0) + sales
    return out


def _expected() -> list[tuple]:
    ty, ny = _aggregate(2001), _aggregate(2002)
    rows = []
    for (store, week), ty_sums in ty.items():
        ny_sums = ny.get((store, week + 52))
        if ny_sums is None:
            continue
        ratios = []
        for dow in (0, 1):
            t, n = ty_sums.get(dow), ny_sums.get(dow)
            ratios.append(t / n if t is not None and n is not None and n != 0 else None)
        rows.append((store, week, *ratios))
    return sort_rows(rows)


def _engine(tmp_path: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )


def _parse(engine: Executor):
    text = MODEL.format(source=_source_sql()) + QUERY
    return engine.parse_text(text)


def test_nullable_measure_rows_survive_stitch(tmp_path: Path):
    engine = _engine(tmp_path)
    statements = _parse(engine)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])
    expected = _expected()
    assert len(rows) == len(expected), f"{rows} != {expected}\n{sql}"
    for got, want in zip(rows, expected):
        assert got[:2] == want[:2], f"{rows} != {expected}\n{sql}"
        for g, w in zip(got[2:], want[2:]):
            if w is None:
                assert g is None, f"{rows} != {expected}\n{sql}"
            else:
                assert g == pytest.approx(w), f"{rows} != {expected}\n{sql}"


def test_no_ordinary_equality_on_measures(tmp_path: Path):
    engine = _engine(tmp_path)
    statements = _parse(engine)
    offenders = []
    for cte in statements[-1].ctes:
        for join in cte.joins or []:
            for pair in getattr(join, "concept_pairs", None) or []:
                if (
                    pair.left.purpose == Purpose.METRIC
                    or pair.right.purpose == Purpose.METRIC
                ) and Modifier.NULLABLE not in pair.modifiers:
                    offenders.append(f"{cte.name}: {pair.left} = {pair.right}")
    assert not offenders, offenders
