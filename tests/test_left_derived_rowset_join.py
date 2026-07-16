"""A subset join on a DERIVED-expression key between two re-aggregated
rowsets must NOT recurse — it resolves or raises a clean trilogy error.

The derived-rowset-join enrichment (`_producible_derived_join_keys` /
`_enrich_via_derived_join_key` in node_generators/rowset_node.py) materializes the
derived key locally and sources the OTHER side's key to pull the other rowset.
That works for INNER (the other rowset's key keeps its identity), but a subset
join SUBSTITUTES the optional side's key onto the anchor's derived key —
so the "other side" re-routes into the anchor rowset and the enrichment loops.

Regression for evals/tpcds_agent/bug_left_derived_rowset_join_recursion.md (a
regression introduced by the derived-rowset-join spike). INNER stays resolvable;
the subset form must raise a clean error rather than a bare RecursionError.
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SALES = """key sid int;
property sid.period int;
property sid.amt float;
datasource sales (sid: sid, p: period, a: amt) grain (sid) address sales_tbl;
"""

BASE = """import sales as s;
rowset agg <- select s.period, sum(s.amt) as tot;
rowset fut <- select s.period, sum(s.amt) as tot;
"""


def _sql(tail: str) -> str:
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sales.preql").write_text(SALES)
        eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
        return eng.generate_sql(BASE + tail)[-1]


def test_left_derived_rowset_join_does_not_recurse():
    # Was an uncaught RecursionError; the LEFT-anchor substitution makes the
    # other side derive from THIS rowset, so the derived-key enrichment declines
    # and the standard left-anchor path resolves it as a real LEFT OUTER JOIN.
    sql = _sql(
        "select agg.period, sum(agg.tot) / sum(fut.tot) as r "
        "subset join fut.period = agg.period + 53;"
    )
    assert "INVALID_REFERENCE_BUG" not in sql
    assert "LEFT OUTER JOIN" in sql


def test_left_plain_equality_rowset_join_still_resolves():
    sql = _sql(
        "select agg.period, sum(agg.tot) / sum(fut.tot) as r "
        "subset join fut.period = agg.period;"
    )
    assert "INVALID_REFERENCE_BUG" not in sql
