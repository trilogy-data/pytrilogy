# q14 — grainless BASIC-over-aggregates scalar in HAVING is misrouted (silent 0 rows / binder error)

Run: `evals/tpcds_agent/results/20260705-200535` — q14 burned **2,014,440 tokens / 48 calls, SCORED FAIL**.

## Symptom

The agent repeatedly wrote the natural single-select form of q14:

```trilogy
auto overall_avg_sale <- sum(sales.quantity*sales.list_price ? sales.date.year between 1999 and 2001)
    / greatest(sum(1 ? sales.date.year between 1999 and 2001),1);
...
where sales.date.year=2001 and sales.date.month_of_year=11 and combo_key in qualifying_combos.key
select sales.channel as channel, sales.item.brand_id as brand_id, ...,
  sum(sales.quantity*sales.list_price) as total_sales,
having total_sales > overall_avg_sale
by rollup (channel, brand_id, class_id, category_id) ...
```

This **runs cleanly, exit 0, and returns 0 rows** (agent_log msg 30). The correct answer is 100 rows.
The prompt's "an empty result can be right" hint then convinced the agent its empty output was valid — the framework gave it no error to react to, so it churned and submitted a wrong (empty-ish) file. The final `workspace/query14.preql` is broken/empty-result.

The canonical answer is NOT empty: `tests/modeling/tpc_ds_duckdb/query14.preql` **builds and matches the reference exactly (100/100) on the current engine** (verified via generate_sql + reference multiset compare). It avoids the bug two ways: (a) it computes the average as a `rowset avg_sales <- select avg(...) as average_sales` (a RowsetItem scalar), and (b) it does the `having ... > avg_sales.average_sales` gate at leaf grain inside a *separate* rowset `l0_filtered`, then rolls up in a plain downstream select.

## Minimal repro (against results/20260705-200535/workspace db)

```trilogy
import raw.all_sales as sales;
auto gavg <- sum(sales.quantity*sales.list_price ? sales.date.year between 1999 and 2001)
           / greatest(sum(1 ? sales.date.year between 1999 and 2001),1);
where sales.date.year=2001 and sales.date.month_of_year=11
select sales.channel as channel,
  sum(sales.quantity*sales.list_price) as total_sales,
having total_sales > gavg
order by channel asc nulls first limit 100;
```
→ **0 rows** (should be 3: CATALOG/STORE/WEB). No rollup, no membership filter needed.

## Trigger matrix (toggle one ingredient; expected ~3 channel rows or ~100 w/ subtotals)

| case | membership `in rs` | `by rollup` | HAVING threshold expression | result | verdict |
|---|---|---|---|---|---|
| A | no | yes | **rowset** scalar `avg_sales.average_sales` | 100 rows +subtotals | PASS |
| E | yes | no | **rowset** scalar | 100 rows | PASS |
| C | yes | yes | **literal** `100000` | 100 rows +subtotals | PASS |
| D | yes | yes | (no having) | 100 rows | PASS |
| F | no | no | **auto/BASIC ratio** `sum(x?c)/greatest(sum(1?c),1)` | **0 rows** | FAIL (silent) |
| G | yes | no | auto/BASIC ratio | **0 rows** | FAIL (silent) |
| AGENT | yes | yes | auto/BASIC ratio | **0 rows** | FAIL (silent) |
| F3 | no | no | auto/BASIC ratio, **same population** `sum(x)/greatest(sum(1),1)` | **DuckDB BinderException** (`sales_channel must appear in GROUP BY`) | FAIL (hard error) |
| F_min | no | no | single grainless `avg(x)` | 3 rows but wrong population (inlined into HAVING over the Nov-2001 subset, not global) | WRONG (separate, returns rows) |

Isolated trigger = **HAVING a group aggregate against a grainless BASIC-over-aggregate(s) `auto` scalar** (arithmetic wrapping ≥1 aggregate). Rollup and the membership filter are both irrelevant. A single bare `avg()` inlines (F_min, wrong but returns rows); wrapping ≥1 aggregate in a BASIC (division, `+`, `/2`) forces a separate materialized scalar node that is then **INNER-JOINed on the select's grain key** (`sales_channel`) instead of broadcast — nonsensical for a grand-total scalar. Cross-population (`? 1999-2001`) → silent 0 rows; same-population → hard binder error.

## Root cause

Two-layer causal chain (READ-ONLY, not fixed):

**Deep root — `trilogy/core/models/author.py:1503-1532` `calculate_granularity`.**
A BASIC concept whose `grain.components` is empty (a grand-total scalar) is mis-tagged `Granularity.MULTI_ROW`. Verified: `sum(x)` / `sum(x?c)` are correctly `SINGLE_ROW` (grain empty), but `auto g <- sbare/2`, `sbare/greatest(sbare,1)`, `sbare+sbare`, and the q14 `gavg` all come back `MULTI_ROW`. Two near-misses in the function:
- line 1526-1529 tests `all(x.granularity==SINGLE_ROW for x in lineage.concept_arguments)`, but `concept_arguments` **flattens through the aggregate sub-expressions to the ROOT leaf columns** (`sales.quantity`, `sales.list_price`, `sales.date.year` — all `MULTI_ROW`) → False.
- line 1530 tests `grain.components == {ALL_ROWS_CONCEPT}`, but a grand-total BASIC's grain is the **empty set** `set()`, not `{ALL_ROWS_CONCEPT}` → False.

So it falls to line 1532 → `MULTI_ROW`.

**Proximate root — `trilogy/parsing/v2/select_finalize.py:1646-1651` `_promote_having_aggregates_to_outputs`.**
The gate that decides whether a HAVING scalar ref is promoted to a hidden broadcast output (correct) vs left to the finer-dim semijoin rewrite (wrong) is:
```
grain_determined = named.is_aggregate
  or named.derivation in (AGGREGATE, WINDOW)
  or named.granularity == Granularity.SINGLE_ROW
  or _is_single_row_rowset_scalar(named, context.concepts)
```
For `gavg`: `is_aggregate=False`, `derivation=BASIC`, `granularity=MULTI_ROW` (from the deep bug), and `_is_single_row_rowset_scalar` (author of the *prior* fix) only recognizes `RowsetItem`-sourced scalars or BASICs recursively composed of them — its BASIC branch recurses `concept_arguments`, which again flatten to ROOT leaves, so it returns False. → `grain_determined=False` → the grainless scalar is treated as a finer *dimension* and routed into `_rewrite_having_finer_dims_to_membership`, which materializes it with a spurious `sales_channel` grain (INNER JOIN `on juicy.sales_channel = vacuous.sales_channel`) and computes the HAVING as a wrong-grain/wrong-population semijoin → empty membership set → 0 rows (or a groupless-column binder error in the same-population form).

Fix direction (not applied): make `calculate_granularity` return `SINGLE_ROW` for a BASIC whose grain has no components (or whose direct — not flattened — arguments are all SINGLE_ROW/constant); the gate then promotes it and it broadcasts via `on 1=1` like the rowset-scalar form already does.

## Classification: KNOWN REOPENED RESIDUAL (same family, broader sub-form)

Same root-cause family as `project_rollup_having_crossrowset_drops_subtotals.md` (RC1: grainless HAVING scalar mis-tagged non-single-row → skipped by the promotion gate → misrouted into the finer-dim semijoin). MEMORY flags that entry "PARTIALLY FIXED / REOPENED" for an `auto`-wrapped scalar; this failure is the **same causal chain but a broader sub-form**: the `auto` here wraps *plain non-rowset aggregates* (never touches a rowset), so the landed `_is_single_row_rowset_scalar` predicate (RowsetItem-only) does not cover it. Also related: `project_crossrowset_inner_join_grainless_scalar.md` (grainless scalar INNER-joined instead of broadcast).

- NOT a regression: canonical query14.preql (rowset-scalar + split-rowset having) still builds and matches reference 100/100; the landed rowset-scalar fix (case A/E) still holds.
- NOT fully new: identical root-cause chain and memory family; only the scalar's authored form differs (BASIC-over-plain-aggregates vs BASIC-over-rowset-output).
