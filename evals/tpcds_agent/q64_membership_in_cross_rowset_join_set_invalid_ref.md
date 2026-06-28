# Bug: a cross-rowset-JOIN rowset whose own WHERE compares its operands → uncaught `INVALID_REFERENCE_BUG` crash (q64)

**Status:** FIXED 2026-06-27. Root cause was NOT in membership/existence sourcing — the
cross-rowset-join rowset `qual` crashes even when **selected directly** (no membership). In
`gen_rowset_node`'s cross-rowset branch (rowset_node.py), the merge was sourced from
`[concept] + local_optional + condition_targets`, where `condition_targets` *excludes* predicate
operands already present in the rowset being processed (`have`). But the merge is sourced anew and
does NOT inherit that rowset's outputs, so an operand living in it (here `item_sales.s_amt`, the
measure being compared) was never produced by the merge — yet the predicate referencing it was
applied → dangling CTE. Fix: source the merge against EVERY `conditions.row_arguments` (via
`unique(... )`), and guard with `_condition_operands_resolved(...)` so the predicate is only applied
when the merge actually produces all operands (else fall through, never emit the sentinel).
Tests: `tests/test_cross_rowset_join_rowset_as_set.py` (direct select + membership set).

(historical) Same `INVALID_REFERENCE_BUG` dangling-CTE
family as the (FIXED) q2 cross-rowset-membership-existence bug, but a case that fix does NOT cover.
**Surfaced by:** TPC-DS q64 (run `20260627-131753`) — the run's top token sink: **2.77M tokens / 60
iterations / 8 errors**, of which **3× `Unexpected error: Invalid reference string found in query`**
(uncaught `ValueError` → no actionable message → agent loops). High-token query → framework bug, as
usual.
**Severity:** HIGH — uncaught crash; SQL renders the `INVALID_REFERENCE_BUG` sentinel where a CTE
alias belongs (dialect/base.py:247 guard at :2386).

## Trigger (minimal, isolated)

A `where k in QUAL.col` membership where the set `QUAL` is itself a rowset built from a **scoped join
of two other rowsets**:

```trilogy
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

with catalog_item_sales as
  select cs.item.id as cat_item_id, sum(cs.ext_list_price) as cat_ext_list_price;
with catalog_item_refunds as
  select cr.item.id as ref_item_id, sum(coalesce(cr.refunded_cash,0)) as cat_refund_total;

# the existence SET is a CROSS-ROWSET JOIN of the two rowsets above:
with qual as
  select catalog_item_sales.cat_item_id as q_item_id
  left join catalog_item_sales.cat_item_id = catalog_item_refunds.ref_item_id
  where catalog_item_sales.cat_ext_list_price > 2 * coalesce(catalog_item_refunds.cat_refund_total, 0);

where ss.item.id in qual.q_item_id          -- membership in the cross-rowset-join set
select ss.item.text_id as id, sum(ss.ext_list_price) as lp;
-- ValueError: Invalid reference string found in query
```

## Bisection

| `qual` (the membership set) | result |
|---|---|
| a **single** rowset (`select ... where cat_ext_list_price > 1000`) | **OK** |
| a **cross-rowset JOIN** rowset (`left join ... where ...`, as above) | **ERR** (sentinel) |

**Correction (matrix-tested):** membership is NOT required — `select qual.q_item` directly *also*
crashes. The real trigger is the **cross-rowset-join rowset whose own WHERE compares its operand
rowsets**; membership is just one consumer. (A matrix also found `out_B` — output taken from the
*refund* rowset — happened not to crash pre-fix because the merge anchored on that side; the bug is
sensitive to which operand the merge anchors on.)

Nothing else from the original q64 is needed — no `physical_returns` join, no dimension columns, no
marital filter, no scoped join on the consuming side. Just **membership in a set whose producer is a
scoped-join of two rowsets**. The set's producer CTE is never materialized → the existence subselect
renders `in (select INVALID_REFERENCE_BUG."q_item_id" from INVALID_REFERENCE_BUG ...)`.

## Likely fix area

Directly adjacent to the FIXED q2 bug
([[project_q2_expr_join_filtered_div_membership_invalid_ref]]): `gen_rowset_node`'s cross-rowset
branch now calls `append_existence_check` to source a membership's existence set — but that sources a
*plain* set. Here the existence set (`qual`) is itself a **cross-rowset-join rowset**, so sourcing it
must recurse into ITS operand rowsets (catalog_item_sales ⋈ catalog_item_refunds) and materialize
their producer CTEs. Inspect how `append_existence_check` / existence sourcing builds the parent for a
membership RHS that resolves to a scoped-join rowset (vs a simple filtered rowset). At minimum it must
become a clean error, not an uncaught `ValueError`. Same dangling-CTE family as
[[project_q64_nested_membership_two_source_agg_invalid_ref]] and
[[project_q14_having_rollup_vs_scalar_invalid_ref]].

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-131753/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('q64.preql').read())   # ValueError: Invalid reference string found in query
```
