"""Repro: a HAVING filter on the subset (partial) rowset's measure, in a
cross-model two-rowset subset join, renders corrupt SQL — `generate_sql` raises
`ValueError: Invalid reference string found in query` (an INVALID_REFERENCE_BUG
sentinel leaks into the rendered SQL).

Minimal trigger (single join key is enough):

    with agg_a as select fa.a_item as it, sum(fa.a_qty) as qa;   -- model fa
    with agg_b as select fb.b_item as it, sum(fb.b_qty) as qb;   -- model fb
    select agg_a.it, agg_a.qa, coalesce(agg_b.qb, 0) as qb
    subset join agg_b.it = agg_a.it
    having coalesce(agg_b.qb, 0) > 0          -- <-- filtering the partial side here breaks it

Control that DOES work (so the trigger is specifically HAVING-on-the-partial-measure
under an outer join):
- the same subset join WITHOUT the having clause renders fine.

Surfaced on enriched TPC-DS q78 ("keep store rows where combined other-channel
quantity > 0"): the natural decomposition is per-channel aggregate rowsets,
LEFT-joined to keep all store rows, then filtered on the other-channel total. The
agent hit this, got a silently-wrong / corrupt result, and thrashed (~2.5M tokens).
The single-key / one-other-channel form here is the reduced core.

Related: `bug_outer_scoped_join_two_rowset_measures.md` (the projection-only outer
two-rowset case, FIXED) — this was the HAVING-on-partial-measure extension.

FIXED: a HAVING scalar-derived expression (e.g. `coalesce(x, 0)`) is now rewritten
to reference its matching SELECT alias (`_substitute_having_derived` in
`select_finalize.py`), so the renderer reads the materialized column instead of
re-deriving the inner (absent) argument at the outer scope.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

FA = """key a_id int;
property a_id.a_item int;
property a_id.a_qty float;
datasource fa (id: a_id, it: a_item, q: a_qty)
grain (a_id) address fa_tbl;
"""

FB = """key b_id int;
property b_id.b_item int;
property b_id.b_qty float;
datasource fb (id: b_id, it: b_item, q: b_qty)
grain (b_id) address fb_tbl;
"""

_HEAD = """
import fa as fa;
import fb as fb;
with agg_a as select fa.a_item as it, sum(fa.a_qty) as qa;
with agg_b as select fb.b_item as it, sum(fb.b_qty) as qb;
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "fa.preql").write_text(FA)
    (tmp_path / "fb.preql").write_text(FB)
    return tmp_path


def _gen(models: Path, body: str) -> str:
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    return eng.generate_sql(body)[-1]


def test_outer_rowset_left_join_no_having(models: Path):
    # Control: subset join of two cross-model rowsets renders fine without the filter.
    sql = _gen(
        models,
        _HEAD + """
select agg_a.it, agg_a.qa, coalesce(agg_b.qb, 0) as qb
subset join agg_b.it = agg_a.it
limit 10;
""",
    )
    assert sql and "INVALID_REFERENCE_BUG" not in sql


def test_outer_rowset_left_join_having_on_partial_measure(models: Path):
    # Filtering the partial (subset) side's coalesce-derived measure in HAVING now
    # resolves to the materialized SELECT alias instead of re-deriving the inner
    # (absent) argument at the outer scope. subset-join semantics are preserved.
    sql = _gen(
        models,
        _HEAD + """
select agg_a.it, agg_a.qa, coalesce(agg_b.qb, 0) as qb
subset join agg_b.it = agg_a.it
having coalesce(agg_b.qb, 0) > 0
limit 10;
""",
    )
    assert sql and "INVALID_REFERENCE_BUG" not in sql
    assert "LEFT OUTER JOIN" in sql
