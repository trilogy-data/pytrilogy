"""Rowset joins + an outer WHERE on a BASE-model concept the rowsets don't output.

The q02 shape: two rowsets aggregating the same fact, scoped-joined (optionally
on a derived key), with a final `where <base concept>` (e.g. `s.date.year =
2001`). Two failure families guarded here:

1. RECURSION — a derived join key + a base-model WHERE arg sent each rowset's
   enrichment through the OTHER rowset carrying that arg, unbounded
   (bug_q02_derived_rowset_union_join_base_where_recursion.md). The derived-key
   enrichment must only bundle concepts the cross-rowset relation can resolve;
   base-model residue belongs to the outer loop.
2. SILENT FILTER DROP — an all-rowset node stack passed condition validation
   vacuously through the independent-scope exemption with NO node applying the
   filter, returning unfiltered rows.

The CONTRACT for every cell: valid SQL with the filter actually applied, or a
clean trilogy error — never a RecursionError, an uncaught exception, a sentinel,
or rows identical to the unfiltered baseline.
"""

import sys

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import (
    DisconnectedConceptsException,
    InvalidSyntaxException,
    UndefinedConceptException,
    UnresolvableQueryException,
)
from trilogy.core.models.environment import Environment

CLEAN = (
    DisconnectedConceptsException,
    UnresolvableQueryException,
    InvalidSyntaxException,
    UndefinedConceptException,
)

# weekly totals over WEB+CATALOG: {1: 12, 2: 11, 54: 17, 55: 23}
MODEL = """
key rid int;
property rid.week int;
property rid.year int;
property rid.channel string;
property rid.sales float;
datasource fact (r: rid, w: week, y: year, c: channel, v: sales) grain (rid)
query '''
select 1 r, 1 w, 2001 y, 'WEB' c, 5.0 v union all
select 2 r, 1 w, 2001 y, 'CATALOG' c, 7.0 v union all
select 3 r, 2 w, 2001 y, 'WEB' c, 11.0 v union all
select 4 r, 54 w, 2002 y, 'WEB' c, 17.0 v union all
select 5 r, 54 w, 2002 y, 'STORE' c, 19.0 v union all
select 6 r, 55 w, 2002 y, 'CATALOG' c, 23.0 v
''';
with cur_period as
where channel in ('WEB','CATALOG')
select week as wk, sum(sales) as amt;
with fut_period as
where channel in ('WEB','CATALOG')
select week as wk, sum(sales) as amt;
"""

PROJ = "select cur_period.wk, cur_period.amt / fut_period.amt as r"
KEYS = {
    "plain": "cur_period.wk = fut_period.wk",
    "derived": "cur_period.wk + 53 = fut_period.wk",
    "composite": "cur_period.wk + 53 = fut_period.wk and cur_period.wk = fut_period.wk",
}
WHERES = {
    "none": "",
    "base": "where year = 2001\n",
    "rowset": "where cur_period.wk < 50\n",
}
JOINS = ["union", "subset"]


def _run(tail: str):
    eng = Dialects.DUCK_DB.default_executor(environment=Environment())
    sql = eng.generate_sql(MODEL + tail)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = eng.execute_raw_sql(sql).fetchall()
    return sorted(
        (tuple(r) for r in rows), key=lambda x: tuple((v is None, v) for v in x)
    )


def _cases():
    return [
        pytest.param(
            f"{where}{PROJ}\n{jt} join {key};", id=f"{jt}-{key_name}-{where_name}"
        )
        for key_name, key in KEYS.items()
        for where_name, where in WHERES.items()
        for jt in JOINS
    ]


@pytest.mark.parametrize("tail", _cases())
def test_rowset_join_base_where_is_safe(tail: str):
    prev = sys.getrecursionlimit()
    sys.setrecursionlimit(3000)
    try:
        _run(tail)
    except RecursionError:
        pytest.fail(f"RecursionError (unguarded planner cycle) on: {tail}")
    except CLEAN:
        return
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"uncaught {type(exc).__name__} on {tail}: {exc}")
    finally:
        sys.setrecursionlimit(prev)


def _filter_effective(tail_filtered: str, tail_baseline: str):
    """A base-model WHERE must never be silently dropped: if the query plans,
    its rows must differ from the unfiltered baseline; a clean error is also
    acceptable (the model declares no join between the rowset outputs and the
    filter column)."""
    baseline = _run(tail_baseline)
    try:
        rows = _run(tail_filtered)
    except CLEAN:
        return None
    assert rows != baseline, f"base-model WHERE silently dropped on: {tail_filtered}"
    return rows


@pytest.mark.parametrize("jt", JOINS)
@pytest.mark.parametrize("key_name", list(KEYS))
def test_rowset_join_base_where_not_dropped(jt: str, key_name: str):
    prev = sys.getrecursionlimit()
    sys.setrecursionlimit(3000)
    try:
        _filter_effective(
            f"where year = 2001\n{PROJ}\n{jt} join {KEYS[key_name]};",
            f"{PROJ}\n{jt} join {KEYS[key_name]};",
        )
    finally:
        sys.setrecursionlimit(prev)


def test_single_rowset_base_where_not_dropped():
    rows = _filter_effective(
        "where year = 2001\nselect cur_period.wk, cur_period.amt;",
        "select cur_period.wk, cur_period.amt;",
    )
    if rows is not None:
        assert rows == [(1, 12.0), (2, 11.0)]


def test_union_derived_rowset_where_control():
    # D3 in the bug report matrix: WHERE on a rowset output stays supported.
    rows = _run(
        f"where cur_period.wk < 50\n{PROJ}\nunion join cur_period.wk + 53 = fut_period.wk;"
    )
    assert rows == [(1, 12.0 / 17.0), (2, 11.0 / 23.0)]


def test_union_derived_no_where_control():
    # D in the bug report matrix: matched pairs ratio, unmatched sides null.
    rows = _run(f"{PROJ}\nunion join cur_period.wk + 53 = fut_period.wk;")
    matched = [r for r in rows if r[1] is not None]
    assert matched == [(1, 12.0 / 17.0), (2, 11.0 / 23.0)]
    assert all(r[1] is None for r in rows if r not in matched)
