"""Composite-key join of a plain rowset onto a `union(...)`-derived rowset.

The q78 shape: a per-channel `store_agg` rowset `union join`ed onto a
`union(...) -> (...)` rowset that stacks the other two channels, related by a
COMPOSITE key (item, cust). Discovery used to report the two sides as
disconnected — the `union(...)`-derived rowset's `_combined_*` output concepts
never received the join edge from the `union join a.k = combined.k` clauses, so
the store component and the union-output component stayed split
(DisconnectedConceptsException) even though a join per key was declared. Same
edge-registration gap fixed for q14/q59; this pins the union-rowset case.
"""

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects
from trilogy.core.models.environment import Environment

# (rid, channel, item, cust, qty)
FACT_ROWS = [
    (1, "store", 1, 1, 5),
    (2, "store", 2, 1, 7),
    (3, "web", 1, 1, 3),
    (4, "catalog", 1, 1, 2),
    (5, "catalog", 2, 1, 4),
]

MODEL = """
key rid int;
property rid.channel string;
property rid.item int;
property rid.cust int;
property rid.qty int;
datasource fact (r: rid, c: channel, i: item, u: cust, q: qty) grain (rid)
query '''{source}''';

rowset store_agg <- where channel = 'store'
select item as item, cust as cust, sum(qty) as store_qty;

with combined_other as union(
    (where channel = 'web' select item as item, cust as cust, sum(qty) as qty),
    (where channel = 'catalog' select item as item, cust as cust, sum(qty) as qty)
) -> (item, cust, qty);

select
    store_agg.item,
    store_agg.cust,
    store_agg.store_qty,
    combined_other.qty as other_qty,
union join store_agg.item = combined_other.item
union join store_agg.cust = combined_other.cust
order by store_agg.item asc;
"""


def _model() -> str:
    source = " union all ".join(
        f"select {r} r, '{c}' c, {i} i, {u} u, {q} q" for r, c, i, u, q in FACT_ROWS
    )
    return MODEL.format(source=source)


def test_union_rowset_composite_join_not_disconnected():
    """Must GENERATE (no false-disconnect) and return the composite-key-joined
    rows: store_agg (item,cust)->store_qty joined to web+catalog totals."""
    exec_ = Dialects.DUCK_DB.default_executor(environment=Environment())
    query = _model()
    # generate_sql must not raise DisconnectedConceptsException (the regression)
    sql = exec_.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = [tuple(r) for r in exec_.execute_raw_sql(sql).fetchall()]
    # `union(...)` STACKS the two arms (UNION ALL), so the other-channel side
    # keeps one row per arm; the composite join relates each to store_agg.
    # (item, cust, store_qty, other_qty):
    #   item 1: store 5, joined to web 3 AND catalog 2 -> two rows
    #   item 2: store 7, joined to catalog 4           -> one row
    # The point of the guard: all three land in ONE connected query (no
    # DisconnectedConceptsException) with both sides' measures populated.
    assert sort_rows(rows) == sort_rows(
        [(1, 1, 5, 3), (1, 1, 5, 2), (2, 1, 7, 4)]
    ), rows
