# Handoff: shared-parent DISTINCT dedup fused away in the rowset `join` form (q75)

**Status:** OPEN. Failing test: `tests/test_rowset_derived_twice_join_bugs.py::test_shared_parent_dedup_fusion`
(`xfail(strict=True)` — delete the marker when it passes).

## One-line
Two rowsets that each aggregate a **shared DISTINCT-grain parent rowset** and are
then joined produce *wrong numbers* in the `join` form: the parent's dedup
(GROUP BY at its full select grain) gets **fused into the child aggregate**, so
rows that should have collapsed are summed twice. The equivalent `merge`+`align`
multiselect materializes the dedup and is correct.

## Context
This is the follow-on to the self-referential-key grain fix (2026-06-08, see
`handoff_q75_derived_twice_rowset_join.md` RESOLVED section + memory
`project_rowset_derived_twice_self_key`). That fix made the *structure* of a
shared-parent join correct (keys align, no `FULL JOIN 1=1`). This bug is the
remaining reason q75 itself can't convert: q75's `deduped` rowset exists for
UNION-DISTINCT semantics, and the join form loses that dedup.

## Minimal repro (self-contained, in the failing test)
```trilogy
import sales as sales;                      -- sales(sale_id, item, year, qty) + returns(sale_id, ret)
auto net <- sales.qty - coalesce(sales.ret, 0);
rowset dedup <- where sales.year in (2001, 2002)
    select sales.item.brand, sales.year, net;     -- DISTINCT grain = (brand, year, net)

-- reference (correct): multiselect materializes `dedup`, then sums it
rowset yp <- where dedup.sales.year = 2002 select dedup.sales.item.brand as brand_c, sum(dedup.net) as c_sum
  merge where dedup.sales.year = 2001 select dedup.sales.item.brand as brand_p, sum(dedup.net) as p_sum
  align brand: brand_c, brand_p;

-- buggy (join): two rowsets each sum `dedup`, joined on brand
rowset c <- where dedup.sales.year = 2002 select dedup.sales.item.brand as brand, sum(dedup.net) as c_sum;
rowset p <- where dedup.sales.year = 2001 select dedup.sales.item.brand as brand, sum(dedup.net) as p_sum;
inner join c.brand = p.brand select c.brand, c.c_sum, p.p_sum;
```
Data: `(brand10, 2001)` has two rows with the same `net=5` (qty5/ret0 and
qty6/ret1) that the `(brand, year, net)` dedup must collapse to one.
- **multiselect (reference):** `[(10,7,5),(20,9,3)]` ✅
- **join (bug):** `[(10,7,10),(20,9,3)]` ❌ — brand10 `p_sum` is `5+5=10`.

`net` must be a **derived** measure spanning a join (sales→returns). If you sum a
plain same-table column the dedup grain expands to the raw columns and both forms
agree (so the bug doesn't show) — the derived-measure-over-a-join is load-bearing
for the repro, mirroring q75's `cnt_per_row = quantity - coalesce(return_quantity,0)`.

## Diagnosis (where to look)
Inspect the two generated SQLs (`engine.generate_sql(text)[-1]`):
- **multiselect:** emits a dedup CTE — `SELECT net, brand, year ... GROUP BY <full select grain>` — then a second CTE `SELECT brand, sum(net) ... GROUP BY brand`. Two grouping levels.
- **join:** emits a single `SELECT brand, sum(net) ... GROUP BY brand` straight over the joined base rows. The dedup CTE is gone — the child aggregate was pushed down through the parent's DISTINCT grain.

So the parent rowset's grain (which includes `net`/the dedup columns) is **not
preserved** when its consumer is an aggregate in the single-SELECT join path; the
aggregate-pushdown fuses the two GROUP BYs into one. The multiselect path keeps
them separate because each arm is its own select that forces `dedup` to materialize.

Likely areas: the rowset-node + aggregate fusion in the single-select planner
(`trilogy/core/processing/node_generators/` rowset/group nodes, and the
aggregate-pushdown / `group_if_required`-style logic). The fix must keep a
DISTINCT-grain rowset materialized when a coarser aggregate consumes it (don't
push the child GROUP BY past the parent's grain). Compare how the multiselect arm
path materializes it and mirror that for the join/single-select path.

## Validation
1. `test_shared_parent_dedup_fusion` passes (both forms == `[(10,7,5),(20,9,3)]`); remove its `xfail`.
2. Then convert `tests/modeling/tpc_ds_duckdb/query75.preql` from merge+align to the
   two-rowset `inner join` form (curr/prev each `sum(deduped.cnt_per_row)` /
   `sum(deduped.amt_per_row)` by the 4 item-attr keys, joined on those 4 keys,
   keeping the HAVING ratio + ordering) and confirm `test_seventy_five` still
   matches `PRAGMA tpcds(75)`. A ready-to-finish join skeleton is the body I tested;
   it was structurally correct but returned `prev_yr_cnt` 5820 vs the reference 5804
   purely because of this dedup fusion.

## Pitfalls
- Don't trust "it ran" — the join form returns WRONG NUMBERS, not an error. Diff
  against the multiselect / PRAGMA.
- Concurrent agents share the working tree; never mutate git state.
- This is NOT the nested-aggregate limitation (`sum(sum(...))`, xfail
  `yoy_shared_parent_nested_agg`): here the inner op is a DISTINCT, the merge
  reference is correct, and only the join form is wrong.
