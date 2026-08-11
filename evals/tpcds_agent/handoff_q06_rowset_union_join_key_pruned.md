# Handoff: `grain(...)` in a filtered aggregate drops a scoped-join key

**Still open as of 2026-08-10**, reconfirmed after rebasing onto
`830fc4329` ([Feat]: Hierarchical `then where` staged filters, #633) and
rebuilding the rust parser. This is independent of aggregate-datasource
selection: the failing query selects only the ordinary `store_sales` and item
datasources.

> **Root cause narrowed — see "Minimal trigger" below.** The original
> hypothesis in this document (the hidden-concept pass prunes the rowset's
> key) is **disproven**: the key can be present in the CTE and the join still
> degrades. The actual trigger is the `grain(...)` wrapper on a filtered
> aggregate's key.
>
> **Rebuild the rust parser before investigating.** A stale
> `_preql_import_resolver` .pyd additionally swallows the rowset's `WHERE`
> clause, which looks like a second defect but is not one. Run
> `.venv\Scripts\python.exe -m maturin develop --release` from the repo root.

## Summary

The q06 agent authored a category-average rowset and explicitly joined it back
to sales by category:

```preql
rowset cat_avg <-
where item.category is not null
select
    item.category as category,
    avg(item.current_price) as cat_avg_price;

select ...
union join ss.item.category = cat_avg.category;
```

Trilogy correctly groups the average CTE by item category, but prunes the
category projection before rendering the later join. The generated join then
loses its predicate entirely:

```sql
quizzical AS (
    SELECT avg(item.I_CURRENT_PRICE) AS cat_avg_price
    FROM item
    WHERE item.I_CATEGORY IS NOT NULL
    GROUP BY item.I_CATEGORY
),
concerned AS (
    ...
    FROM quizzical
    FULL JOIN yummy ON 1=1
)
```

Every qualifying sale is consequently compared with every category average.
On the SF=1 evaluation database this returns 51 states/state rows rather than
the reference's 46.

## Standalone reproduction

The reproduction uses the checked-in enriched semantic model and only
generates SQL:

```powershell
.venv\Scripts\python.exe evals\tpcds_agent\repro_q06_rowset_union_join_key_pruned.py
```

It asserts that the category-average grouping exists while the downstream
`FULL JOIN` has degraded to `ON 1=1`.

## Aggregate-path control

The original failing artifact is:

```text
evals/tpcds_agent/results/20260810-200843_enriched_aggregates/workspace/query06.preql
```

Its generated SQL references `store_sales`, not
`agg_store_sales_daily`. Running that exact program against the
`enriched_noise` workspace, which has no aggregate datasource bindings,
reproduces the same 51 rows. The failure is therefore not caused by aggregate
substitution.

The independently authored noise candidate avoids the defect by expressing
the category qualification as membership in a keyed item rowset; it returns
the correct 46 rows.

## Fresh-run confirmation

The renamed-table/cumulative-noise run reproduced the same defect through a
second authoring shape:

```text
evals/tpcds_agent/results/20260810-211903_enriched_noise/workspace/query06.preql
```

That candidate defines `auto cat_avg <- avg(item_dedup.cp) by
item_dedup.category` and relies on the grouped key to relate the category
average to each sale. Generated SQL again loses the key and emits `RIGHT OUTER
JOIN ... ON 1=1`, returning 51 rows. It reads `fact_store_sales` directly and
does not select a `fact_agg_*` datasource.

The independently authored `enriched_aggregates` candidate in the same run
uses an explicit keyed `subset join`; its SQL retains the category predicate
and returns the correct 46 rows. This fresh A/B is additional evidence that
aggregate materialization is not causal: candidate shape determines whether
the optimizer hits the key-liveness defect.

## Minimal trigger

```powershell
.venv\Scripts\python.exe evals\tpcds_agent\repro_q06_min_grain_scoped_join.py
```

Two legs differing by **one token**. Both use a root-import `item` rowset
grouped by category and a `union join ss.item.category = cat_avg.category`:

| filtered count key | rendered join |
| --- | --- |
| `count(ss.item.sk ? ...)` | `... on "highfalutin"."cat_avg_category" is not distinct from "ss_item_items"."I_CATEGORY"` |
| `count(grain(ss.item.sk) ? ...)` | `... on 1=1` |

The `grain(...)` wrapper alone flips it. Compositeness is not required — a
single-key `grain(ss.item.sk)` already fails, so the composite
`grain(ss.item.sk, ss.ticket_number)` in q06 is incidental.

## What is ruled out

Each of these was tested against the rebased tree and does **not** cause it:

- **Key pruning / `HideUnusedConcepts`.** Adding `cat_avg.category` to the
  outer projection keeps the alias alive through the CTE chain
  (`"quizzical"."cat_avg_category"` is projected into the join CTE) and the
  join is *still* `on 1=1`. The key being absent is a symptom, not the cause.
- **The `is not null` presence rewrite.** Dropping
  `ss.item.category is not null` from the count predicate — so no
  `_virt_presence_*` marker is generated — does not restore the join.
- **Cross-namespace keys.** An `item`-namespace rowset joined to
  `ss.item.category` renders correctly without the `grain()` wrapper.
- **`union join` specifically.** `subset join` degrades identically, so this
  is not a `union`/coalescing-join rendering path issue.
- **Aggregate datasource substitution** (already covered above).

## Likely root area

Scoped-join key propagation through the grain-scoped filtered-aggregate
lowering. The `grain(...)` spec appears to rebuild the aggregate's source node
at the declared grain without carrying the scoped join's key requirement into
that node's output set, so by the time the join is rendered neither side
exposes the key. Inspect the filtered-aggregate/`grain()` path in
`trilogy/core/processing/v4_node_generators/` and how scoped-join keys are
registered as required outputs.

## Desired behavior

`cat_avg.category` must remain projected from the grouped rowset and the final
SQL must join it to `ss.item.category`. If a required scoped-join key cannot be
rendered, generation should fail with a semantic error rather than silently
weakening the predicate to `ON 1=1`.

Regression coverage should assert both that the category key survives the
average CTE and that no predicate-bearing scoped join becomes an unconditional
join after optimization.
