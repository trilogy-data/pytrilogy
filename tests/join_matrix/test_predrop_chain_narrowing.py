"""Pre-drop chain-context pins (handoff_narrowing_soundness_residuals task 1,
closed 2026-07-03 as unreachable).

Geometry: an EARLIER dropping join in a FROM chain (a WHERE-forced INNER /
directional upgrade, incl. cross-CTE null-rejection) precedes a join that
directional narrowing collapses on a key the drop touched. The worry was that
`_complete_values` judges the superset side IN ISOLATION, blind to chain
drops.

Why these cells PASS (the invariant, verified here as row-identity against
the fully un-optimized plan): every producer of a chain-earlier dropping join
is either (a) proof-based — equivalence/completeness/narrowing proofs are
self-contained and drop nothing on honest data; (b) declaration-trusted —
lying declarations are author error, the ruled semantics; or (c) WHERE-based —
and the guards' proof is precisely that rows NULL at the rejected address
never survive any consumer output. Every row a later narrowing could drop is
padded on the sup-side chain, hence NULL at that address, hence already dead
under the same WHERE; NULL-key rows that null-safe-match instead acquire
partners and are not dropped (`_null_extended_before` covers the extension
geometry). These cells fail loudly if a future pass breaks the entailment —
e.g. by relocating or deleting a condition after join upgrades consumed it.

Also pins task 3 (sup-side pseudonym `~` stamps, closed as unreachable):
build substitution re-addresses a merged member's partial stamp onto the
rendered pair address, so `_own_coverage_partial`'s exact-address check sees
it and the merged-member cell keeps its preserving FULL rows.
"""

import pytest

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.parsing.parse_engine_v2 import clear_parse_cache

# b complete for k; a and c are genuine-`~` subsets ({1,2} and {3}).
MODEL = """
key k int;
property k.b_val int;
datasource bsrc (k: k, v: b_val) grain (k)
query '''select 1 k, 10 v union all select 2, 20 union all select 3, 30''';

property k.a_val int;
datasource asrc (k: ~k, av: a_val) grain (k)
query '''select 1 k, 100 av union all select 2, 200''';

property k.c_val int;
datasource csrc (k: ~k, cv: c_val) grain (k)
query '''select 3 k, 999 cv''';
"""

# nullable-key variant: NULL is a valid member; null-safe equality must pair
# the NULL groups, and narrowing must not drop them.
MODEL_NULLABLE = """
key k int;
property k.b_val int;
datasource bsrc (k: k, v: b_val) grain (k)
query '''select 1 k, 10 v union all select 2, 20 union all select 3, 30''';

property k.a_val int;
datasource asrc (k: ~?k, av: a_val) grain (k)
query '''select 1 k, 100 av union all select 2, 200 union all select cast(null as int), 500''';

property k.c_val int;
datasource csrc (k: ~?k, cv: c_val) grain (k)
query '''select 3 k, 999 cv union all select cast(null as int), 777''';
"""

# task 3: dsup binds the merged member k2 with `~`; the stamp must surface at
# the rendered pair address so completeness is never overclaimed for dsup.
MODEL_MERGED_MEMBER = """
key k int;
property k.fact_val int;
datasource fact (kk: ~k, w: fact_val) grain (k)
query '''select 1 kk, 11 w union all select 2, 22''';

key k2 int;
property k2.sup_val int;
datasource dsup (kk: ~k2, v: sup_val) grain (k2)
query '''select 1 kk, 111 v''';

property k.dim_val int;
datasource dim (kk: k, d: dim_val) grain (k)
query '''select 1 kk, 1000 d union all select 2, 2000 union all select 3, 3000''';

merge k2 into k;
"""

_JOIN_OPT_FLAGS = (
    "upgrade_outer_key_set_equivalence",
    "upgrade_condition_joins",
)


def _run(model: str, query: str, optimize: bool) -> list[tuple]:
    for flag in _JOIN_OPT_FLAGS:
        setattr(CONFIG.optimizations, flag, optimize)
    clear_parse_cache()
    try:
        engine = Dialects.DUCK_DB.default_executor()
        engine.parse_text(model)
        sql = engine.generate_sql(query)[-1]
        assert "INVALID_REFERENCE_BUG" not in sql, sql
        rows = [tuple(r) for r in engine.execute_raw_sql(sql).fetchall()]
        return sorted(rows, key=lambda r: tuple((x is None, x) for x in r))
    finally:
        for flag in _JOIN_OPT_FLAGS:
            setattr(CONFIG.optimizations, flag, True)
        clear_parse_cache()


def _assert_cell(model: str, query: str, expected: list[tuple]) -> None:
    optimized = _run(model, query, optimize=True)
    baseline = _run(model, query, optimize=False)
    assert optimized == baseline, (optimized, baseline)
    assert optimized == expected, optimized


CELLS = [
    (
        "same_cte_where",
        MODEL,
        "select k, b_val, a_val, c_val where a_val > 0;",
        [(1, 10, 100, None), (2, 20, 200, None)],
    ),
    (
        "where_arg_not_selected",
        MODEL,
        "select k, b_val, c_val where a_val > 0;",
        [(1, 10, None), (2, 20, None)],
    ),
    (
        "cross_cte_where_via_rowset",
        MODEL,
        "with agg as select k, b_val, a_val, c_val;\n"
        "select agg.k, agg.b_val, agg.c_val where agg.a_val > 0;",
        [(1, 10, None), (2, 20, None)],
    ),
    (
        "aggregate_consumer",
        MODEL,
        "select k, b_val, sum(c_val) -> tc where a_val > 0;",
        [(1, 10, None), (2, 20, None)],
    ),
    (
        "no_where_keeps_full",
        MODEL,
        "select k, b_val, a_val, c_val;",
        [(1, 10, 100, None), (2, 20, 200, None), (3, 30, None, 999)],
    ),
    (
        "nullable_same_cte_where",
        MODEL_NULLABLE,
        "select k, b_val, a_val, c_val where a_val > 0;",
        [(1, 10, 100, None), (2, 20, 200, None), (None, None, 500, 777)],
    ),
    (
        "nullable_where_arg_not_selected",
        MODEL_NULLABLE,
        "select k, b_val, c_val where a_val > 0;",
        [(1, 10, None), (2, 20, None), (None, None, 777)],
    ),
    (
        "nullable_no_where_keeps_full",
        MODEL_NULLABLE,
        "select k, b_val, a_val, c_val;",
        [
            (1, 10, 100, None),
            (2, 20, 200, None),
            (3, 30, None, 999),
            (None, None, 500, 777),
        ],
    ),
]


@pytest.mark.parametrize(
    "model,query,expected",
    [pytest.param(m, q, e, id=name) for name, m, q, e in CELLS],
)
def test_predrop_chain_cell(model: str, query: str, expected: list[tuple]):
    _assert_cell(model, query, expected)


def test_merged_member_partial_stamp_blocks_completeness():
    """dsup carries `~` on merged member k2 ({1} of k's domain {1,2,3}); the
    fact side carries `~` on k ({1,2}). Neither side may be claimed complete,
    so fact-exclusive k=2 keeps its row (preserving joins survive narrowing)."""
    _assert_cell(
        MODEL_MERGED_MEMBER,
        "select k, dim_val, fact_val, sup_val;",
        [
            (1, 1000, 11, 111),
            (2, 2000, 22, None),
            (3, 3000, None, None),
        ],
    )
