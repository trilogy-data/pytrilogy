# BUG: `by rollup` + `having <agg> > <cross-rowset scalar>` silently drops ALL subtotal/grand-total rows

**Classification:** framework bug — **SILENT wrong results**. A `by rollup (...)` select whose
`having` compares an aggregate to a value from a *separate rowset* (a cross-rowset/cross-source
scalar) emits the leaf-grain rows correctly but drops **every** rollup subtotal and grand-total
row. The query "runs fine"; the rollup the author asked for just isn't there.

**Status:** confirmed + root-caused to the mechanism (generated SQL), reproduced against the eval
workspace with a clean HAVING on/off toggle. Minimal synthetic not yet isolated (see Scope). NOT fixed.

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

## File pointers

- `trilogy/parsing/v2/select_finalize.py:1203` — `_rewrite_having_finer_dims_to_membership` / the HAVING→CASE post-aggregation rewrite (add the rollup/cube/grouping_sets guard here). Rendering side: `trilogy/dialect/base.py:963`.
- Repro: `evals/tpcds_agent/repro_rollup_having_crossrowset_drops_subtotals.preql` (+ `_CONTROL_nohaving.preql`).
- Related memory: `project_q05_rollup_sibling_join_upgraded_to_inner`, `project_having_post_aggregation_non_output_refs`, `project_q70_grouping_in_where_groupless_binder`.
