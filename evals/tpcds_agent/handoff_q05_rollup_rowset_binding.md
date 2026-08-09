# Handoff: q05 rollup over a joined rowset emits an ungrouped binding column

## Summary

TPC-DS q05 exposed a planner/rendering bug when a query:

1. aggregates two rowsets at `(channel, entity)` grain;
2. combines them with scoped `union join` on both keys;
3. computes normalized channel/entity labels directly in the final select; and
4. applies `by rollup (channel)` to the computed channel alias.

Trilogy generates an aggregate CTE grouped only by the normalized channel but
also projects a backing join key such as `r_a_channel`. DuckDB rejects the SQL:

```text
Binder Error: column "r_a_channel" must appear in the GROUP BY clause or must
be part of an aggregate function.
```

The failure is immediate during DuckDB binding; it is not a runaway query.

## Standalone reproduction

The inspector uses the checked-in enriched TPC-DS semantic model to generate
the failing SQL, but deliberately does not execute it:

```powershell
.venv\Scripts\python.exe evals\tpcds_agent\repro_q05_rollup_rowset_binding.py
```

It prints the offending rollup CTE and ends with:

```text
REPRODUCED: rollup CTE projects a backing channel outside its GROUP BY
```

The script is safe and bounded: it only parses the model and generates SQL.
The adjacent `.preql` file is the exact failing intermediate query.

## Reduced Trilogy shape

```preql
rowset s <- where f.sale_entity is not null
select f.channel as channel, f.sale_entity as entity, sum(f.sales) as sales;

rowset r <- where f.return_entity is not null
select f.channel as channel, f.return_entity as entity, sum(f.returns) as returns;

select
    case coalesce(s.channel, r.channel) ... end as channel,
    concat(..., coalesce(s.entity, r.entity)) as entity,
    sum(coalesce(s.sales, 0)) as sales,
    sum(coalesce(r.returns, 0)) as returns
union join s.channel = r.channel
union join s.entity = r.entity
by rollup (channel);
```

## Control

Including the complete selected leaf grain avoids the invalid binding in the
original run:

```preql
by rollup (channel, entity);
```

That is also the semantically correct q05 authoring because it requests entity
leaves, channel subtotals, and a grand total. However, the original form should
either compile to valid SQL according to Trilogy's grain semantics or fail with
a clear semantic error before reaching DuckDB. It must not emit invalid SQL.

## Original artifact

- Run: `evals/tpcds_agent/results/20260806-210222`
- Failure: `agent_log.q05.jsonl`, tool result at `2026-08-06T21:17:36Z`
- Final corrected candidate: `workspace/query05.preql`

The original generated SQL created an aggregate CTE that resembled:

```sql
SELECT
    protective.channel AS channel,
    protective.r_a_channel AS r_a_channel,
    protective.r_entity AS r_entity,
    sum(...) AS returns
FROM protective
GROUP BY ROLLUP (1)
```

`r_a_channel` and `r_entity` are carried for a later stitch join but are neither
grouped nor aggregated. The outer query then FULL JOINs that aggregate CTE back
to the pre-rollup `protective` CTE using those leaked keys.

## Likely root area

Inspect rollup grain construction and rowset reattachment/stitch planning:

- A post-aggregate stitch requests the joined rowset's backing keys even though
  the rollup removed them from the aggregate grain.
- The renderer consequently projects those keys through the aggregate CTE
  without adding them to `GROUP BY`.
- The normalized `coalesce` outputs may retain lineage to both union-joined key
  members, causing the planner to reattach the pre-rollup rowset unnecessarily.

## Desired behavior

Choose and test one explicit contract:

1. If non-rollup output dimensions remain implicit leaf-grain dimensions, keep
   them in the leaf grouping and null them only at appropriate rollup levels.
2. If `by rollup(channel)` intentionally excludes `entity`, reject selecting
   `entity` with a concise Trilogy semantic error.

In either case, generated aggregate SQL must never project non-grouped backing
keys solely to support a later internal stitch.

## Resolution (2026-08-06)

Contract chosen: **(1)** — the already-documented one. `_apply_grouping_to_passthroughs`
in `trilogy/parsing/v2/select_finalize.py` states it explicitly: a plain dim at
another grain is "fetched through a join-back at the leaf grain (NULL on subtotal
rows)". `by rollup (channel)` selecting `entity` is legal; it groups at `channel`
and broadcasts each level over the leaf `entity` rows. No new semantic error.

Two independent defects blocked that:

1. `MergeNode._inject_scoped_join_key_exposure` surfaced the coalescing
   (`union join`) key group's members on whichever parent could reach them —
   including the ROLLUP node, whose GROUP BY is the wrapper's `by` list verbatim.
   `r_a_channel`/`r_entity` became bare ungrouped projections. Fixed by skipping
   parents that render a non-standard GROUP BY (`_renders_nonstandard_grouping`),
   alongside the existing abstract-grain skip.
2. With those keys gone the stitch paired on the rollup key itself, and
   `get_join_type` returns INNER when both sides are nullable — silently dropping
   the grand-total row. A grouping-set NULL is padding, not a value, so it can
   never find a partner on a side that does not pad the same key. `get_node_joins`
   now tracks rollup-padded keys per source (`rollup_padded_addresses`) and
   `get_join_type` preserves toward the padded side.

`repro_q05_rollup_rowset_binding.py` now raises its own "the known ungrouped
backing key was not generated" assertion — that is the fixed state.

## Regression coverage

Use the standalone query shape as an execution test and assert:

- the failing form either executes or raises the chosen clean semantic error;
- no DuckDB `BinderException` escapes;
- generated aggregate CTEs project only grouped or aggregated columns;
- `rollup(channel, entity)` continues to execute and returns leaf, subtotal,
  and grand-total rows;
- unmatched sales-only and returns-only entities survive the `union join`.

Landed as `tests/engine/test_duckdb_rollup_rowset_binding.py` (all five points)
plus a new `grouping_placement` family in the differential fuzzer
(`local_scripts/fuzzer`, 24 cases over two seeds) that varies rollup placement
against DuckDB oracles. Only the coalescing-key cases in that family fail
without the fixes — ordinary rollup placement was already correct.
