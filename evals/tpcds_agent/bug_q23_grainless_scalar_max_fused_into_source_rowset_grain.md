# q23 — grainless `auto` scalar `max(rowset.col)` silently fused into source rowset's grain (filter degenerates to `x > 0.5x`)

> **SUPERSEDED 2026-07-06 — the fix below was REVERTED.** The `_degenerate_aggregate_cograin`
> carveout was a language misunderstanding: per owner-stated intent, a bare (no `by`) aggregate is
> grain-polymorphic and co-grains to the consuming grain, so the degenerate `x > 0.5*max(x)`
> tautology is the *correct* application of the rule — a global reduction must be authored as
> `max(x) by *` or a grained select output. The carveout made WHERE deviate from HAVING. See
> `bug_q23_bare_max_sibling_rowset_having_tautology.md` for the consistent-principles resolution.

> **FIXED 2026-07-05 (reverted, see above).** Root cause was upstream of node sourcing: `_build_select_lineage`'s
> `where_factory` co-grains every bare (no `by`) WHERE aggregate to the select grain
> (`Factory.aggregate_grain`, HAVING-like, documented). When the aggregate's input is a rowset
> column already AT that grain, the co-grain is degenerate — one row per group — so the render-time
> grain-match collapse turns `max(x)` into `x` (the "fusion" observed). Fix:
> `Factory._degenerate_aggregate_cograin` (trilogy/core/models/build.py) — in WHERE/filter
> co-grain contexts only, a bare aggregate whose inputs are all rowset-derived (recursing through
> BASIC wraps) and functionally determined by the co-grain resolves at empty grain instead, i.e. a
> global SINGLE_ROW scalar, broadcast via the existing single-row cross-join path. Cases A/A2 and
> the inline form now produce the rowset-wrapped (B) plan. Non-rowset degenerate WHERE aggregates
> keep documented co-grain semantics (guarded by
> `tests/test_derived_concepts.py::test_where_aggregate_on_grouped_select_executes`).
> Guards: `tests/test_where_scalar_aggregate_degenerate_cograin.py`.
> NEW DISTINCT BUG found while guarding: a NON-degenerate bare WHERE aggregate over a rowset column
> (rowset finer than select grain) is silently dropped entirely (no WHERE/HAVING emitted) —
> pre-existing, strict-xfail'd in the same test file; see
> `bug_nondegenerate_rowset_where_aggregate_silently_dropped.md`.

**Target:** q23, run `evals/tpcds_agent/results/20260705-200535`. 655,137 tokens / 21 calls / SCORE=FAIL.
**Class:** FRAMEWORK bug, SILENT wrong results (query runs exit 0, returns 100 rows / 1505 full; correct = 4).
**Regression/residual/new:** NEW distinct bug. Same broad family as the OPEN cross-rowset grainless-scalar
entries (`project_rollup_having_crossrowset_drops_subtotals`, `project_crossrowset_inner_join_grainless_scalar`)
but a different mechanism and not covered by the two prior q23 fixes (`filter_only_rowset_output_vs_concept`,
`bare_aggregate_in_filter_content_grain`). Canonical `query23.preql` builds and returns the correct 4 rows on
the current engine — the engine is healthy for the *rowset-wrapped* form; only the `auto`-scalar form breaks.

## Symptom
The agent modeled the "best customers" gate exactly as the business reads:
```
rowset sct <- where ... select ss.customer.id as cust_id, sum(ss.quantity*ss.sales_price) as total_spent,;
auto max_total <- max(sct.total_spent);                              -- grainless scalar
rowset best_customers <- where sct.total_spent > 0.5 * max_total     -- gate vs the scalar
                         select sct.cust_id as cust_id,;
```
`best_customers` returns **76,412** rows (every customer) instead of **750**. The gate never filters.

## Minimal repro + trigger matrix
Model: `sct` = per-customer store totals (2000–2003). Ground truth (raw SQL): 76412 customers, max=236266.51,
**750** exceed `0.5*max`.

| Case | Form | rows | correct? |
|------|------|------|----------|
| A | `auto m <- max(sct.total_spent)`; `where sct.total_spent > 0.5*m` (sibling rowset) | **76412** | WRONG (all) |
| B | `rowset mx <- select max(sct.total_spent) as cmax,`; `where sct.total_spent > 0.5*mx.cmax` | 750 | correct |
| C | `auto m <- max(sct.total_spent)`; `select m;` (scalar alone) | 236266.51 | correct |
| D | `where sct.total_spent > 100000.0` (literal threshold control) | 2049 | correct |

Trigger = **all three**: (1) the max is an `auto`/BASIC grainless scalar (not a `rowset ... select max(...)`);
(2) it is consumed in a sibling rowset's WHERE; (3) the *other* operand of that WHERE is the SAME source
rowset's per-row column. Wrapping the max in its own `rowset` (B) fixes it; so does any threshold that does not
co-reference the source rowset (D). Selected alone (C) the scalar is correct.

## Generated SQL (the smoking gun)
For case A the planner emits:
```sql
cooperative as (                       -- this is the max_total node
  SELECT thoughtful.store_customer_totals_cust_id,
         thoughtful.store_customer_totals_total_spent as "max_total"   -- NO max(), NO collapse
  FROM thoughtful),
questionable as (                      -- best_customers
  SELECT thoughtful.cust_id as best_customers_cust_id
  FROM cooperative INNER JOIN thoughtful
       ON cooperative.cust_id = thoughtful.cust_id                     -- joined back per customer
  WHERE thoughtful.total_spent > 0.5 * cooperative.max_total           -- == total_spent > 0.5*total_spent
  GROUP BY 1)
```
`max_total` is materialized as a per-customer passthrough of `total_spent` and joined back on `cust_id`, so for
every row `max_total == that row's own total_spent`. The predicate collapses to `x > 0.5x`, true for all
positive totals ⇒ all 76,412 customers pass. The `max()` aggregation is dropped entirely.

## Root cause
The concept is classified correctly — `local.m.granularity == Granularity.SINGLE_ROW` at BOTH author and build
level (`Concept.calculate_granularity`, `trilogy/core/models/author.py:1503-1532`; AGGREGATE with empty/abstract
grain ⇒ SINGLE_ROW). So the classification is *not* the bug.

The defect is in node sourcing: a SINGLE_ROW aggregate whose input is a rowset column, when co-required in a
WHERE alongside that same rowset's per-row column, is **fused into the keyed (per-`cust_id`) source node instead
of being isolated as an independent single-row scan that the final node cross-joins/broadcasts.** Once fused at
`cust_id` grain, `max(total_spent)` over one row per group = identity, so the aggregation silently vanishes.

The single-row isolation that *should* apply lives in `trilogy/core/processing/v4_helper/group_rules.py:299-418`
(`Granularity.SINGLE_ROW` roots are pulled into their own `single_row` bucket and cross-joined). It is not
reached here because `m` never surfaces as an independent root: the filter-sourcing path co-sources every WHERE
argument of `best_customers` from the shared `sct` subtree (same area exercised by the prior q23 fixes —
`concept_strategies_v3.py` `initialize_loop_context` required_filters, `build.py` `_build_filter_where`,
and the SINGLE_ROW handling in `discovery_utility.py:414-543`), so the grainless max is absorbed into the
per-`cust_id` group rather than split out. The correct plan is the one the `rowset`-wrapped form (B) produces:
`max` in its own node, broadcast onto the gate.

Fix direction (NOT applied): when a `Granularity.SINGLE_ROW` aggregate sourced from a rowset column is required
by a filter that also references a per-row column of the *same* rowset, force the aggregate onto its own
single-row node (cross-join/broadcast) instead of fusing it into the keyed source group.

## Token-sink assessment
The agent's confusion was genuine and framework-driven: its `auto max_total <- max(...)` gate is idiomatic and
runs clean, but silently returns everyone, so the agent had no error to react to and churned on interpretation.
Two secondary friction points seen in the log were legit/clear, not framework bugs: `property max_total <- max(...)`
→ clear "purpose METRIC does not match PROPERTY, default to auto"; and `rowset x as where...` (wrong `<-` operator)
→ Syntax [104] (message is somewhat misleading — it blames "definitions after where/select" rather than the
`as` vs `<-` operator, minor guidance defect). The primary, score-determining defect is the silent max-fusion above.
