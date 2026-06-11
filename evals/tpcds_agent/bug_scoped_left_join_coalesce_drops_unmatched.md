# Bug: scoped `left join` with only coalesce-wrapped partial output renders INNER → drops unmatched left rows

**Status:** FIXED 2026-06-11 (found same day, during the q76/q77 multiselect→`union(...)` conversion).

## Fix

Root cause was *not* a non-null proof demoting the join (the bug-report hypothesis):
the partial flag never propagated at all. The LEFT-join target key (`rrs.r_store`) is a
**ROWSET-derived** concept, so `Factory._is_binding_keyed` returns True → it's excluded
from `scoped_merge_sources` → `scoped_partial_derived` (= `scoped_merge_sources &
scoped_partial_sources`) is empty → `get_node_joins` never marks it partial. The other
partiality path, `_scoped_join_targets` in `rowset_node.py`, only bridges rowsets that
**share a base dimension** (the q44 self-join shape); two distinct rowsets joined
explicitly (sales↔returns) fall through both, so the join resolved as INNER.

Fix (`trilogy/core/processing/node_generators/rowset_node.py` `gen_rowset_node`): after
`set_output_concepts`, mark any of the rowset's own output concepts whose address is in
`environment.scoped_partial_sources` as partial. The two rowset keys canonicalize into one
class in `build_canonical_address_map`, so `_collect_deep_partial_addresses` →
`canon_node` lands the partial on the shared connecting key and `get_join_type` emits
LEFT. coalesce-only references no longer demote it.

q77 consequence: making the store/web sales↔returns joins correctly LEFT surfaced 2 extra
rows (NULL-store-id / NULL-web-page-id sales groups, masked before by the INNER drop). The
real TPC-DS q77 inner-joins `store_sales`/`web_sales` to the store/web_page dimension
(`ss_store_sk = s_store_sk`), excluding NULL ids. Mirrored that in `query77.preql` by
adding `and ss.store.id is not null` / `and ws.web_page.id is not null`. q77 now renders 2
LEFT OUTER JOINs and matches `PRAGMA tpcds(77)` (44 rows) at sf=1.

Tests: `tests/engine/test_duckdb_rowset.py::test_scoped_left_join_coalesce_keeps_unmatched`
(deterministic repro), plus `test_seventy_seven` still green.

---

**Original report (found 2026-06-11, during the q76/q77 multiselect→`union(...)` conversion).**
**Severity:** medium-high — silently wrong results (a LEFT join drops rows it must keep). Currently
masked in the tpc_ds suite because at sf=0.01 q77's store/web joins have **0 unmatched rows**, so
INNER == LEFT and `PRAGMA tpcds(77)` still matches. Any data with a left row lacking a right match
would diverge.

## Symptom

A query-scoped `left join a = b` where the partial (right) side is referenced **only** through a
non-null wrapper (`coalesce(partial, 0)`) is built as an `INNER JOIN`, dropping every left row with
no right match — instead of keeping it with the coalesced default.

## Deterministic reproduction (no checked-in model needed)

```python
from trilogy import Dialects
FIX = """
key store_id int;
key return_store_id int;
property store_id.sale_amt int;
property return_store_id.return_amt int;
datasource sales (store_id: store_id, sale_amt: sale_amt) grain (store_id)
query '''select 1 as store_id, 100 as sale_amt union all select 2 as store_id, 200 as sale_amt''';
datasource returns (return_store_id: return_store_id, return_amt: return_amt) grain (return_store_id)
query '''select 1 as return_store_id, 10 as return_amt''';
rowset srs <- select store_id as s_store, sum(sale_amt) as s_amt;
rowset rrs <- select return_store_id as r_store, sum(return_amt) as r_amt;
"""
ex = Dialects.DUCK_DB.default_executor()
ex.execute_text(FIX)
q = '''
left join srs.s_store = rrs.r_store
select srs.s_store -> k, srs.s_amt - coalesce(rrs.r_amt, 0) -> net order by k asc;
'''
print(ex.generate_sql(q)[-1])            # -> INNER JOIN on s_store = r_store
print([tuple(r) for r in ex.execute_text(q)[0].fetchall()])
# GOT:      [(1, 90)]            (store 2 dropped)
# EXPECTED: [(1, 90), (2, 200)]  (store 2 kept; coalesce(NULL,0)=0 -> 200-0)
```

Returns covers only store 1, so store 2 is unmatched on the left. A correct `left join` keeps store
2 with `r_amt` NULL → `coalesce(NULL,0)=0` → `net = 200`. The build emits INNER and drops it.

## Minimization

- **Not** the `join_upgrade` optimizer. Disabling both `CONFIG.optimizations.upgrade_condition_joins`
  and `upgrade_outer_key_set_equivalence` still renders INNER and still drops the row. The LEFT-ness
  is lost in the **base scoped-join build** (`trilogy/core/models/build.py` `scoped_joins` /
  `scoped_partial_sources` handling), not a later rewrite.
- **Not** union/arm-specific. A plain top-level select with the same `left join` (above) reproduces
  it. The union-arm path (`SelectLineage.scoped_joins` → `gen_union_select_node`) applies the arm's
  join exactly like the outer form, so it inherits this behavior.
- Trigger appears to be: the **only** output column sourced from the partial side is wrapped in a
  non-null function (`coalesce`), so no *nullable* output survives from the partial side, and the
  build concludes the join can be INNER. The LEFT key itself (`srs.s_store`, on the kept side) is
  still projected, so the left rows *should* survive regardless.

## Likely root cause area

`build.py`: `scoped_merge_sources` / `scoped_partial_sources` (the LEFT side that should bind
partial) + how `find_nullable_concepts` / the join-type resolution decide LEFT vs INNER. The
non-null proof from `coalesce(partial, k)` is being allowed to demote the join, but `coalesce` over a
partial column is the canonical *keep-left-and-default* idiom and must NOT force LEFT→INNER — the
same principle as the CASE/raw-flag non-null gate in `project_join_upgrade_raw_flag_false`
(`_opaque_binding_addresses`), but here it must apply in the scoped-join build itself, not just the
optimizer.

## Provenance

Found converting `query77.preql` (3-channel UNION ALL multiselect) to the relational `union(...)`
TVF. Both the original multiselect form AND the union form render the store/web sales↔returns joins
as INNER (verified identical), so this is pre-existing and shared. q77 still matches `PRAGMA
tpcds(77)` only because the sf=0.01 generated data has no store/web with sales but no returns
(`select count(*) from (select distinct ss_store_sk from store_sales where ss_store_sk not in
(select sr_store_sk from store_returns ...))` → 0). See [[project_tvf_union_aggregate_arms]].
