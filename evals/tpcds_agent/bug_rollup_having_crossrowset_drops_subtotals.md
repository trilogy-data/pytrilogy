# BUG: `by rollup` + `having <agg> > <cross-rowset scalar>` silently drops ALL subtotal/grand-total rows

**Classification:** framework bug — **SILENT wrong results**. A `by rollup (...)` select whose
`having` compares an aggregate to a value from a *separate rowset* (a cross-rowset/cross-source
scalar) emits the leaf-grain rows correctly but drops **every** rollup subtotal and grand-total
row. The query "runs fine"; the rollup the author asked for just isn't there.

## SIBLING (still OPEN 2026-07-01): the WHERE-path drops the grand total (different mechanism)

`test_rollup_where_crossrowset_preserves_grand_total` fails — a DISTINCT bug from the HAVING one,
NOT fixed by the `_is_single_row_rowset_scalar` recursion (verified: original + fixed gate both
drop it). Realistic for q14: the agent falls back to `where` when it botches HAVING clause order.

Root cause (from generated SQL): the WHERE predicate requiring the cross-rowset join is applied
**TWICE**:
1. leaf-grain CTE `thoughtful` (pre-rollup) — correct: `... INNER JOIN highfalutin on 1=1 WHERE cheerful.ch_total_sales > overall_avg_sale`, drops the failing channels.
2. the outermost SELECT (post-rollup) re-applies the SAME predicate: `... INNER JOIN highfalutin on 1=1 WHERE questionable.ch_total_sales > overall_avg_sale`. But `questionable` re-fetches `ch_total_sales` via `cooperative LEFT OUTER JOIN thoughtful ON cooperative.ch_channel = thoughtful.ch_channel`, and the ROLLUP grand-total row has `ch_channel = NULL` → the join misses → `ch_total_sales = NULL` → `NULL > scalar` → grand total dropped.

Fix direction (planner/SQL-gen layer, NOT the parse-time gate): a cross-rowset-join WHERE that is
already satisfied pre-rollup must not be re-applied above the rollup (it's redundant), or the
post-rollup re-fetch must not null the grand-total row's leaf field. Locus: where a join-requiring
WHERE condition is attached at both the leaf node and the outermost select (`query_processor.py` /
dialect rendering), around the `cooperative`→`questionable` LEFT-join re-fetch + outer WHERE
duplication. Owned by the rollup-fix author (adjacent code); the HAVING-path recursion fix does not
reach it.

---

**Status:** PARTIALLY FIXED — REOPENED 2026-07-01. The 2026-06-30 fix handles the scalar
referenced **directly** (`having <agg> > overall_avg.avg_val`) and its test
(`test_rollup_having_crossrowset_preserves_subtotals`) passes. But it MISSES the scalar referenced
through an **`auto`/BASIC indirection** — the natural form the q14 agent used. See "REOPENED" below.

## REOPENED 2026-07-01 — `auto`-wrapped cross-rowset scalar still drops subtotals

Minimal A/B on the SAME model (`_ROLLUP_HAVING_CROSSROWSET_MODEL`), only the HAVING operand differs:

```trilogy
rowset ov <- select (sum(samt)+sum(wamt))/(count(sid)+count(wid)) as v;
# ... union by_channel(channel, brand, total_sales) ...

# A) DIRECT rowset-output ref  -> 5 rows, 3 subtotals  ✓ (covered by existing test)
having sum(by_channel.total_sales) > ov.v            by rollup (by_channel.channel, by_channel.brand)

# B) AUTO indirection          -> 2 rows, 0 subtotals ✗ SUBTOTALS DROPPED (not covered)
auto avg_val <- ov.v;
having sum(by_channel.total_sales) > avg_val         by rollup (by_channel.channel, by_channel.brand)
```

`(B)` is exactly what TPC-DS q14 does — `auto overall_avg_sale <- overall_stats.total_value /
overall_stats.total_count; ... having sum(...) > overall_avg_sale`. It is the dominant driver of the
q14 4.15M-token sink (run 20260701-033309): every framework obstacle q14 hit is fixed EXCEPT this,
so the agent's natural formulation still silently loses the rollup and it can't converge.

**Root cause of the gap:** the fix's `_is_single_row_rowset_scalar` gate matches a HAVING operand
that IS a `RowsetItem` (single-row rowset output). An `auto`/derived concept wrapping that output
(`avg_val <- ov.v`, or `ov.tv / ov.tc`) has `derivation=BASIC`, so the gate returns False → the
predicate is again treated as a finer-dimension membership → CASE-key-nulling → subtotals dropped.

**Fix direction (extends the landed fix):** make `_is_single_row_rowset_scalar` see THROUGH BASIC
indirection — a concept whose lineage transitively resolves to only single-row rowset scalars (and
literals/constants) is itself a single-row scalar and must broadcast, not route to the semijoin.
Recurse over `concept_arguments` of a BASIC concept.

**Additional failing unit test to add** (currently FAILS):
`tests/engine/test_duckdb.py::test_rollup_having_auto_wrapped_crossrowset_scalar_preserves_subtotals`
— identical to the passing test but with `auto avg_val <- overall_avg.avg_val;` and
`having sum(by_channel.total_sales) > avg_val`; assert the same 5-row result (subtotals present).

---

**(original 2026-06-30 fix notes below — still valid for the direct-reference case)**

**Impact:** THE driver of the TPC-DS **q14** token sink (3.76M prompt tokens, run 20260701-013044).
The agent got a rollup result with no subtotal rows, correctly noticed "still no nulls! the rollup
rows are not appearing," hypothesized the HAVING was to blame, and thrashed without finding the
cause. (q14's reference output is a rollup — the missing subtotals make it unscoreable.)

## Reproduction (against the eval workspace)

Two saved files (run each with the scoring engine over `.../workspace/_worker_0`):

- `evals/tpcds_agent/repro_rollup_having_crossrowset_drops_subtotals.preql` — the q14 shape WITH the
  `having sum(total_sales) > overall_avg.avg_sale_value` → **0 null-key (subtotal) rows**.
- `evals/tpcds_agent/repro_rollup_having_crossrowset_CONTROL_nohaving.preql` — identical minus the
  `having` line → **32 null-key rows in the first 100** (rollup subtotals present).

Toggle result (only difference is the one `having` line):

| variant | null-key rollup rows |
|---|---|
| WITH `having <agg> > overall_avg.avg_sale_value` | **0** (subtotals gone) |
| WITHOUT that `having` | 32 (subtotals present) |

`ROLLUP` IS present in the generated SQL in both cases — DuckDB generates the subtotal rows; the
HAVING rewrite then eliminates them.

## Root cause (from the generated SQL)

The HAVING comparison against the cross-rowset scalar is not applied as a post-aggregation row
filter. Instead it is rewritten into a **CASE that NULLs out each rollup key** when the predicate
fails, and those CASE-wrapped keys feed the `ROLLUP`:

```sql
-- final CTE (abridged):
CASE WHEN "h"."_virt_agg_sum_..." > "h"."overall_avg_avg_sale_value"
     THEN "h"."nov2001_by_channel_brand_id"    ELSE NULL END,   -- and the same for channel/class/category
...
GROUP BY ROLLUP (3, 1, 4, 2)
```

This is the HAVING-post-aggregation-filter machinery (the "finer-dim / non-output ref → CASE /
semijoin" rewrite; see memory `project_having_post_aggregation_non_output_refs`,
`select_finalize.py::_rewrite_having_finer_dims_to_membership`). It is **fundamentally incompatible
with ROLLUP**: ROLLUP already uses NULL keys to denote subtotal/grand-total rows, so nulling keys
via CASE conflates "filtered out" with "subtotal," and the genuine subtotal rows are lost. A plain
post-aggregation `HAVING <agg> > <scalar>` (filter rows, keep NULL-key rollup rows) would be correct.

Same family as `project_q05_rollup_sibling_join_upgraded_to_inner` (rollup NULL-key rows dropped by
a downstream operation) and `project_q70_grouping_in_where` (grouping/rollup vs a pre/post-group
construct mismatch).

## Scope / trigger (what's confirmed vs open)

Confirmed necessary ingredients (from toggling q14): `by rollup` + a `having` whose RHS is a
**cross-rowset scalar** (here `overall_avg.avg_sale_value`, a separate rowset derived from the base
facts, not the rolled-up `nov` union).

Not sufficient alone (these did NOT trigger the CASE rewrite; subtotals survived):
- plain fact `by rollup` + `having sum > <same-source rowset scalar>` — rollup rows kept.
- `by rollup` + `having sum > <literal>` / own-aggregate — rollup rows kept.

So the rewrite fires on the specific combination in q14 (rollup keys are **rowset/union outputs**
and the HAVING scalar is a **different** rowset). Minimal isolation is the remaining task — start
from the two workspace repro files and strip the union/qualifying-combos layers one at a time,
watching for `CASE WHEN ... THEN <rollup_key> ELSE NULL` to appear in the generated SQL.

## Fix direction

In the HAVING rewrite (`trilogy/parsing/v2/select_finalize.py::_rewrite_having_finer_dims_to_membership`, the finer-dim/non-output → CASE/semijoin path): when
the select's grain mode is ROLLUP / CUBE / GROUPING_SETS, do **not** apply the CASE-key-nulling
rewrite. Route the predicate to a genuine post-aggregation `HAVING` (or an outer wrapper that
filters rows) so subtotal/grand-total NULL-key rows are preserved and only leaf rows failing the
predicate are dropped. Guard the rewrite on `not select.grain rollup/cube/grouping_sets`, mirroring
the other rollup-interaction guards.

## Guardrails (must not regress)

- Plain `by rollup` (no having) must keep subtotal rows (control file above).
- `having` post-aggregation filters on non-rollup selects must still push down (the feature this
  rewrite implements — `project_having_post_aggregation_non_output_refs`).
- q05 rollup-subtotal preservation (`test_duckdb.py::test_rollup_sibling_column_join_preserves_subtotals`).
- q70 grouping-in-where/having guards.

## Actual fix (2026-06-30)

The CASE-null was a downstream symptom of a **mis-routed HAVING predicate**, driven by two defects
in `trilogy/parsing/v2/select_finalize.py`. Both fixes live in `_promote_having_aggregates_to_outputs`:

1. **Cross-rowset scalar not promoted.** After aggregate promotion the HAVING is
   `_virt_agg_sum > overall_avg.avg_val`. `overall_avg.avg_val` (a ratio-of-aggregates rowset
   output) is classified `derivation=ROWSET`, `granularity=MULTI_ROW`, `is_aggregate=False` —
   mis-tagged, because its source rowset select is grainless (one row). The scalar-promotion gate
   only accepted `is_aggregate` / `AGGREGATE|WINDOW` / `SINGLE_ROW`, so it skipped the scalar → it
   never became a hidden output → not in `allowed_addresses` → `needs_membership` treated it as a
   *finer dimension* and routed the whole predicate into the grain-key CASE-nulling semijoin. Fix:
   new `_is_single_row_rowset_scalar` (a `RowsetItem` over a non-union `SelectLineage` with empty
   `grain.components`) added to the gate.

2. **Bare HAVING aggregate not matched to the rollup projection.** With (1) fixed the membership no
   longer fires, but the planner then split the projected `total_sales` (`GROUP BY ROLLUP`) from the
   HAVING-gate aggregate (plain `GROUP BY`) and joined them back — the gate node had no NULL-key
   rows, so subtotals still vanished. Cause: `_propagate_select_grouping` stamps the rollup keys onto
   each projection aggregate's `by`, so projected `total_sales` sig `(SUM,(x,),(brand,channel))` ≠
   bare HAVING `sum(x)` sig `(SUM,(x,),())` → promotion minted a fresh plain-grain `_virt_agg_sum`.
   Fix: `rollup_alias_by_bare_sig` maps each projection whose `by` equals the `select.grouping` keys
   to its output ref under the bare-`by` signature; a matching bare HAVING aggregate is substituted
   onto that projected alias (the working `having total_sales > scalar` form) instead of minted.

Result SQL: one `GROUP BY ROLLUP` CTE + top-level `WHERE total_sales > overall_avg_avg_val`
(scalar broadcast via `on 1=1`) — identical to the plain-fact case. Both rollup and non-rollup now
correct. Guardrails held (rollup-sibling, all having/rollup/grouping tests, 152 tpc-ds modeling).

## File pointers

- `trilogy/parsing/v2/select_finalize.py:1203` — `_rewrite_having_finer_dims_to_membership` / the HAVING→CASE post-aggregation rewrite (add the rollup/cube/grouping_sets guard here). Rendering side: `trilogy/dialect/base.py:963`.
- Repro: `evals/tpcds_agent/repro_rollup_having_crossrowset_drops_subtotals.preql` (+ `_CONTROL_nohaving.preql`).
- Related memory: `project_q05_rollup_sibling_join_upgraded_to_inner`, `project_having_post_aggregation_non_output_refs`, `project_q70_grouping_in_where_groupless_binder`.
