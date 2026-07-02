"""Shape matrix for scoped joins between two rowsets on a DERIVED-expression key.

Sweeps join type x which-side-the-derived-expr-is-on x projection. The CONTRACT
(what this guards) is that every shape is SAFE: ``generate_sql`` either returns
valid SQL (no ``INVALID_REFERENCE_BUG`` sentinel) or raises a clean trilogy error
— NEVER a ``RecursionError`` or an uncaught exception. The derived-rowset-join +
left-anchor fixes are easy to make brittle here; this flushes regressions.

Cells that currently resolve are also checked for CORRECTNESS (exact rows, correct
join type) on a fixed dataset. Cells that clean-error are KNOWN build-level
limitations (e.g. both keys derived, or FULL on a one-sided derived key): the
scoped-merge collapse substitutes one rowset's key into the other's derivation, so
the collapsed side can't be materialized locally. A clean error is acceptable; a
recursion/sentinel/wrong-result is not. See
evals/tpcds_agent/bug_left_derived_rowset_join_recursion.md and
bug_derived_rowset_join_key_reaggregate_disconnect.md.
"""

import sys
import tempfile
from pathlib import Path

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

SALES = """key sid int;
property sid.period int;
property sid.region int;
property sid.amt float;
datasource sales (sid: sid, p: period, r: region, a: amt) grain (sid)
query '''
select 1 sid, 1 p, 10 r, 100.0 a union all
select 2 sid, 1 p, 10 r, 50.0 a union all
select 3 sid, 54 p, 10 r, 30.0 a union all
select 4 sid, 54 p, 10 r, 7.0 a union all
select 5 sid, 2 p, 10 r, 20.0 a''';
"""
BASE1 = """import sales as s;
rowset agg <- select s.period, sum(s.amt) as tot;
rowset fut <- select s.period, sum(s.amt) as tot;
"""
BASE2 = """import sales as s;
rowset agg <- select s.period, s.region, sum(s.amt) as tot;
rowset fut <- select s.period, s.region, sum(s.amt) as tot;
"""

PROJ = {
    "reagg_both": "select agg.period, sum(agg.tot) / sum(fut.tot) as r {join};",
    "plain": "select agg.period, agg.tot, fut.tot {join};",
    "reagg_other": "select agg.period, agg.tot, sum(fut.tot) as ft {join};",
}
SINGLE_KEYS = {
    "plainEq": "agg.period = fut.period",
    "deriv_anchor": "agg.period + 53 = fut.period",
    "deriv_other": "agg.period = fut.period + 53",
    "deriv_both": "agg.period + 1 = fut.period + 2",
    "mult_anchor": "agg.period * 2 = fut.period",
}
COMPOSITE_KEYS = {
    "comp_mixed": "agg.period + 53 = fut.period and agg.region = fut.region",
    "comp_deriv": "agg.period + 53 = fut.period and agg.region + 1 = fut.region",
}
JOINS = ["left", "full"]


def _build_sql(base: str, tail: str) -> str:
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sales.preql").write_text(SALES)
        eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
        return eng.generate_sql(base + tail)[-1]


def _cases():
    out = []
    for base, keys in ((BASE1, SINGLE_KEYS), (BASE2, COMPOSITE_KEYS)):
        for proj, tmpl in PROJ.items():
            for key, expr in keys.items():
                for jt in JOINS:
                    tail = tmpl.format(join=f"{jt} join {expr}")
                    out.append(pytest.param(base, tail, id=f"{proj}-{key}-{jt}"))
    return out


@pytest.mark.parametrize("base,tail", _cases())
def test_scoped_derived_rowset_join_is_safe(base: str, tail: str):
    """Never a RecursionError, an uncaught exception, or a sentinel — only valid
    SQL or a clean trilogy error."""
    prev = sys.getrecursionlimit()
    sys.setrecursionlimit(3000)
    try:
        sql = _build_sql(base, tail)
    except RecursionError:
        pytest.fail(f"RecursionError (unguarded planner cycle) on: {tail}")
    except CLEAN:
        return
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"uncaught {type(exc).__name__} on {tail}: {exc}")
    finally:
        sys.setrecursionlimit(prev)
    assert "INVALID_REFERENCE_BUG" not in sql, f"sentinel leaked on: {tail}"


def _rows(base: str, tail: str):
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sales.preql").write_text(SALES)
        eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
        sql = eng.generate_sql(base + tail)[-1]
        jt = (
            "FULL"
            if "FULL JOIN" in sql
            else (
                "LEFT"
                if "LEFT OUTER JOIN" in sql
                else "INNER" if "INNER JOIN" in sql else "?"
            )
        )
        rows = sorted(
            (tuple(r) for r in eng.execute_text(base + tail)[0].fetchall()),
            key=lambda x: (x[0] is None, x[0]),
        )
        return jt, rows


# agg/fut by period = {1: 150, 2: 20, 54: 37}
@pytest.mark.parametrize(
    "key,jt,exp_join,exp_rows",
    [
        # derived on the anchor: fut.period = agg.period + 53
        (
            "agg.period + 53 = fut.period",
            "left",
            "LEFT",
            [(1, 150 / 37), (2, None), (54, None)],
        ),
        # derived on the looked-up side: agg.period = fut.period + 53
        (
            "agg.period = fut.period + 53",
            "left",
            "LEFT",
            [(1, None), (2, None), (54, 37 / 150)],
        ),
        # multiplicative: fut.period = agg.period * 2
        (
            "agg.period * 2 = fut.period",
            "left",
            "LEFT",
            [(1, 150 / 20), (2, None), (54, None)],
        ),
    ],
)
def test_scoped_derived_rowset_join_correct(key, jt, exp_join, exp_rows):
    got_join, rows = _rows(
        BASE1, f"select agg.period, sum(agg.tot) / sum(fut.tot) as r {jt} join {key};"
    )
    assert got_join == exp_join
    assert rows == exp_rows


def test_composite_both_plain_left_join_stays_left():
    """Control: a both-plain composite LEFT key keeps LEFT directionality."""
    got_join, _ = _rows(
        BASE2,
        "select agg.period, sum(agg.tot) / sum(fut.tot) as r "
        "left join agg.period = fut.period and agg.region = fut.region;",
    )
    assert got_join == "LEFT"


def test_composite_mixed_key_left_join_should_not_widen():
    # KNOWN-BROKEN: composite scoped LEFT join with a MIXED derived+plain key
    # splits into two join levels and widens the plain half to FULL -> spurious
    # right-only rows. Pre-existing q78-family bug. See
    # evals/tpcds_agent/bug_composite_mixed_key_scoped_left_join_widens_full.md
    got_join, rows = _rows(
        BASE2,
        "select agg.period, sum(agg.tot) / sum(fut.tot) as r "
        "left join agg.period + 53 = fut.period and agg.region = fut.region;",
    )
    # Should be a LEFT join with NO right-only (NULL-period) rows.
    assert got_join == "LEFT"
    assert all(r[0] is not None for r in rows)


# --- shared-canonical composite (plain-eq co-key + derived key) --------------
# q59 shape: a parent rowset (weekly_store) with two year-filtered CHILD rowsets
# (this_year / next_year), both projecting the parent key (store_id). The two
# child store_id concepts collapse to ONE canonical (via the parent pseudonym),
# so when the scoped join carries a DERIVED co-key, the planner treats store_id
# as "already unified", builds the this_year branch WITHOUT store_id, and the
# inferred join keys on the derived week key ALONE -> cross-store fan-out.
# NOTE: a bare `property row_id.k` synthetic model of this shape RecursionErrors
# (a separate failure, see bug_left_derived_rowset_join_recursion.md), so this
# guard is built against a grain-keyed / conformed-dimension model.
CONFORMED = """key rid int;
property rid.store int;
property rid.week_seq int;
property rid.year int;
property rid.sales_price float;
datasource sales (rid: rid, s: store, w: week_seq, y: year, sp: sales_price) grain (rid)
query '''
select 1 rid, 1 s, 1 w, 2001 y, 100.0 sp union all
select 2 rid, 1 s, 1 w, 2001 y, 50.0 sp union all
select 3 rid, 2 s, 1 w, 2001 y, 30.0 sp union all
select 4 rid, 1 s, 53 w, 2002 y, 7.0 sp union all
select 5 rid, 2 s, 53 w, 2002 y, 20.0 sp union all
select 6 rid, 1 s, 2 w, 2001 y, 200.0 sp union all
select 7 rid, 1 s, 54 w, 2002 y, 40.0 sp''';
"""
Q59 = """import sales as ss;
with weekly_store as
where ss.year = 2001 or ss.year = 2002
select ss.store as store_id, ss.week_seq as week_seq, ss.year as year,
       sum(ss.sales_price) as tot ;
with this_year as
where weekly_store.year = 2001
select weekly_store.store_id, weekly_store.week_seq, weekly_store.tot ;
with next_year as
where weekly_store.year = 2002
select weekly_store.store_id, weekly_store.week_seq, weekly_store.tot ;
left join this_year.store_id = next_year.store_id
  and next_year.week_seq = this_year.week_seq + 52
select this_year.store_id, this_year.week_seq,
       this_year.tot / next_year.tot as ratio
order by this_year.store_id, this_year.week_seq;
"""


def _q59_rows():
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sales.preql").write_text(CONFORMED)
        eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
        prev = sys.getrecursionlimit()
        sys.setrecursionlimit(4000)
        try:
            return [tuple(r) for r in eng.execute_text(Q59)[0].fetchall()]
        finally:
            sys.setrecursionlimit(prev)


def test_q59_shared_canonical_composite_left_join_no_fanout():
    # KNOWN-BROKEN: shared-canonical composite scoped LEFT join (plain-eq co-key
    # whose two sides collapse to ONE canonical via a common parent rowset, + a
    # derived key) drops the equality co-key -- the this_year branch is built
    # without store_id so the inferred join carries only the derived week key ->
    # cross-store fan-out. See
    # evals/tpcds_agent/bug_q59_composite_derived_left_join_drops_equality_key_fanout.md
    rows = _q59_rows()
    # store_id, week_seq, ratio. Correct = one row per (store, week_seq), no
    # cross-store fan-out: (1,1,150/7), (2,1,30/20), (1,2,200/40).
    keys = [(r[0], r[1]) for r in rows]
    assert len(keys) == len(set(keys)), f"cross-store fan-out: {rows}"
    assert sorted(keys) == [(1, 1), (1, 2), (2, 1)]
