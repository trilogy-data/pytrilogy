# q05 token sink — `by rollup (…)` SILENTLY DROPPED when the SELECT has no fresh aggregate

**Target:** q05 in `evals/tpcds_agent/results/20260703-212501/` — **1,640,409 tokens, FAILED**
(100 cand rows vs 100 ref rows, "result set differs from reference"). Trilogy v0.3.288.

## Symptom (what the agent saw)

q05 = channel roll-up (store/catalog/web) with `rollup(channel, entity_id)` →
per-entity leaves + per-channel subtotals + a grand total, sorted nulls-first.

The agent authored the natural Trilogy shape: two `where … select …` rowsets
(`sale_agg`, `return_agg`), each pre-aggregating with `sum(…)`, combined with a
`union join`, then a final SELECT that PASSES THROUGH the pre-aggregated columns
(`coalesce(sale_agg.ext_sales, 0) as total_ext_sales`) with a
`by rollup (sale_agg.channel, sale_agg.entity_text_id)` clause.

The submitted query runs clean and returns 100 rows — but **every row is a leaf
(catalog channel); there are ZERO subtotal/grand-total rows.** The `by rollup`
clause was silently discarded. The agent noticed and could not explain it,
churning for hundreds of thousands of tokens:

> "all 442 rows have non-null channel_label and entity_id. That means the rollup
> subtotals aren't showing?" (conv. line 4688)
> "the column stats say non_null=442 for entity_id but there are 442 total rows.
> So ALL have non-null entity_id. This means the `case … concat(…)` returns
> non-null even on rollup rows…" (line 5884)

It never found the real cause (the rollup never made it into the SQL) and shipped
a passthrough form that fails.

## Root cause — one construct, reproducible with NO union join

`by rollup (…)` is materialized ONLY by stamping the grouping mode onto
**un-grouped projection aggregates**. When the SELECT projects pre-aggregated
values with NO fresh aggregate function, the spec attaches to nothing and is
dropped — **no ROLLUP/GROUPING SETS in the SQL, no error, wrong rows.**

`trilogy/parsing/v2/select_finalize.py`:
- `_propagate_select_grouping` (L995) — for a non-None `spec`, loops select items
  and calls `_collect_ungrouped_aggregates_deep`, stamping `wrapper.grouping =
  spec.mode` / `.grouping_sets` onto each hit (L1042-1045). **If the loop finds
  zero un-grouped aggregates, it stamps nothing and returns — the rollup spec is
  discarded with no diagnostic.**
- `_is_ungrouped_aggregate` (L929) — requires a bare `AggregateWrapper` with
  `not node.by`. A passthrough of a rowset/`auto` measure (already summed at its
  own grain) is not one, so it is never collected.

Downstream, `query_processor.py` builds a plain GROUP BY (no `rollup_concepts`),
so DuckDB gets `GROUP BY 1,2,…` with no subtotal rows.

## Trigger matrix (current working tree, workspace `20260703-212501`)

| # | Construct | `by rollup` in SQL? |
|---|-----------|:---:|
| M1 | base-model `select … sum(x) by rollup (a,b)` | **YES** (canonical q05 works) |
| M2 | `union join` two rowsets, PASSTHROUGH `coalesce(sale_agg.sales,0)` + rollup | **NO** — dropped |
| M3 | `union join` two rowsets, passthrough, no rollup | n/a |
| M4 | SINGLE rowset, PASSTHROUGH `coalesce(sale_agg.sales,0)` + rollup | **NO** — dropped (executes: 442 rows, **0 subtotal rows**) |
| M5 | SINGLE rowset, FRESH `sum(sale_agg.sales)` + rollup | **YES** |
| M6 | `union join` two rowsets, FRESH `sum(sale_agg.sales)` + rollup | **YES** |
| M7 | `auto tot <- sum(x) by …`, referenced as passthrough + rollup (no rowset/join) | **NO** — dropped |

Key result: the drop is **not** union-join-related. It is governed solely by
whether the final SELECT contains an **un-grouped aggregate**. Passthrough of an
already-aggregated rowset/`auto` column (M2/M4/M7) → dropped; wrapping the same
column in a fresh `sum(…)` (M5/M6) → preserved. The canonical `query05.preql`
dodges it because its `@windowed(…)` macro expands to fresh `sum(…)` in the final
SELECT (and uses no rowset), so M1 keeps the rollup and yields the reference rows
(verified: 100 rows incl. `(None,None,…)` grand total and `('catalog channel',
None,…)` subtotal, returns-only entities like `catalog_pageAAAAAAAAAABBAAAA`
present with 0 sales / 326.05 returns).

## Classification

- **PRIMARY: framework bug (silent wrong-result), the token-sink driver.** An
  explicit `by rollup (…)` clause is silently discarded when the projection has
  no un-grouped aggregate. It should EITHER roll up the passthrough measure
  (re-aggregate the pre-summed column) OR raise a clear error
  (`"by rollup requires an aggregate measure in the SELECT; a passthrough of a
  pre-aggregated column cannot be rolled up — wrap it in sum(…)"`). Silently
  emitting a plain GROUP BY with no subtotal rows and no error is what burned
  1.6M tokens: the agent saw the rows, saw no subtotals, and had no signal that
  the clause it wrote was ignored. The inconsistency (M5/M6 keep it, M2/M4/M7
  drop it) made it un-diagnosable.
- **Secondary / authoring (not the sink):** the submitted query also derives
  labels + `total_returns` from the SALE arm only (`sale_agg.*`), so returns-only
  entities and the correct return totals are wrong — the previously-diagnosed
  label/returns-coalesce authoring defect. Real, but subordinate to the rollup
  drop (which alone guarantees failure).
- **Not the cause:** no `union join` optimizer crash here (distinct from q59's
  `value_set_join_upgrade.py` regression); the `union join` itself resolves.

## Fix options (do NOT implement — READ-ONLY task)

- **Framework (preferred):** in `_propagate_select_grouping`
  (`select_finalize.py` L995), when `spec is not None` but zero un-grouped
  aggregates were stamped, raise a clear `InvalidSyntaxException` (as above), OR
  extend rollup materialization to re-aggregate passthrough measures at the
  rollup grain. Add a join-matrix / rollup cell: "`by rollup` over a passthrough
  of a pre-aggregated rowset/`auto` column."
- **Guidance (interim, prevents the sink):** the model/question should state that
  `by rollup (…)` requires the rolled measures to be **fresh aggregates in the
  final SELECT** — combine the rowsets and write `sum(sale_agg.sales)` (M6), not
  `coalesce(sale_agg.sales, 0)` passthrough. The agent-info rollup docs
  (conv. lines 654-683, 2824-2862) show only base-model `sum(x) by rollup (…)`;
  they never warn that passthrough silently no-ops.
