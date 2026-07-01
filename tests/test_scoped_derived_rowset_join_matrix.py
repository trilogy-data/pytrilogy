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
JOINS = ["inner", "left", "full"]


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
        ("agg.period + 53 = fut.period", "inner", "INNER", [(1, 150 / 37)]),
        (
            "agg.period + 53 = fut.period",
            "left",
            "LEFT",
            [(1, 150 / 37), (2, None), (54, None)],
        ),
        # derived on the looked-up side: agg.period = fut.period + 53
        ("agg.period = fut.period + 53", "inner", "INNER", [(54, 37 / 150)]),
        (
            "agg.period = fut.period + 53",
            "left",
            "LEFT",
            [(1, None), (2, None), (54, 37 / 150)],
        ),
        # multiplicative: fut.period = agg.period * 2
        ("agg.period * 2 = fut.period", "inner", "INNER", [(1, 150 / 20)]),
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


CO_KEY_SALES = """key sid int;
property sid.store int;
property sid.period int;
property sid.yr int;
property sid.amt float;
datasource sales (sid: sid, s: store, p: period, y: yr, a: amt) grain (sid)
query '''
select 1 sid, 1 s, 1 p, 1 y, 10.0 a union all
select 2 sid, 2 s, 1 p, 1 y, 100.0 a union all
select 3 sid, 1 s, 11 p, 2 y, 20.0 a union all
select 4 sid, 2 s, 11 p, 2 y, 200.0 a''';
"""
CO_KEY_HEAD = """import sales as s;
rowset a <- where s.yr = 1 select s.store, s.period, sum(s.amt) as tot;
rowset b <- where s.yr = 2 select s.store, s.period, sum(s.amt) as tot;
select a.store, a.period, a.tot / b.tot as r
"""


def _co_key_rows(join: str):
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sales.preql").write_text(CO_KEY_SALES)
        eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
        text = CO_KEY_HEAD + join + ";"
        sql = eng.generate_sql(text)[-1]
        store_in_join = any(
            "store" in ln.lower()
            for ln in sql.splitlines()
            if " join " in ln.lower() and " on " in ln.lower()
        )
        rows = sorted(tuple(r) for r in eng.execute_text(text)[0].fetchall())
        return store_in_join, rows


def test_composite_mixed_key_inner_join_keeps_equality_co_key():
    """A cross-rowset INNER join ANDing a plain-equality key with a derived key
    (`a.store = b.store and a.period + 10 = b.period`) must keep BOTH keys. The
    equality co-key was silently dropped, fanning each store's ratio across every
    store. Correct: one row per store (each store's own next-period ratio)."""
    store_in_join, rows = _co_key_rows(
        "inner join a.store = b.store and a.period + 10 = b.period"
    )
    assert store_in_join, "equality co-key `a.store = b.store` dropped from the join"
    assert rows == [(1, 1, 0.5), (2, 1, 0.5)]


@pytest.mark.xfail(
    reason="composite scoped LEFT join with a MIXED derived+plain key splits into "
    "two join levels and widens the plain half to FULL -> spurious right-only rows. "
    "Pre-existing q78-family bug. See "
    "evals/tpcds_agent/bug_composite_mixed_key_scoped_left_join_widens_full.md",
    strict=True,
)
def test_composite_mixed_key_left_join_should_not_widen():
    got_join, rows = _rows(
        BASE2,
        "select agg.period, sum(agg.tot) / sum(fut.tot) as r "
        "left join agg.period + 53 = fut.period and agg.region = fut.region;",
    )
    # Should be a LEFT join with NO right-only (NULL-period) rows.
    assert got_join == "LEFT"
    assert all(r[0] is not None for r in rows)
