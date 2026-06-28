# bug q14 — INVALID_REFERENCE_BUG in ORDER BY (REGRESSION)

## REGRESSION BANNER
**Was a clean author-time error, now an uncaught sentinel crash.**
- Offending commit: **`9dc18944` "more_results"** (first of the recent fix chain; direct
  child of last-good `058e5cc9` "Post-Join Bug Bash").
- At `058e5cc9` the exact failing q14 construct produced a *clean* `InvalidSyntaxException`
  ("HAVING references 'local.overall_avg_sale', which is not in the SELECT projection").
- At `9dc18944` (and every commit after: `4659d503`/`8ef13bd3` remove_hidden_concepts,
  `10468b3a` more_fixes, `68e8dd60` more_work) the same construct now *builds* and then
  emits `INVALID_REFERENCE_BUG` in the rendered ORDER BY → `ValueError: Invalid reference
  string found in query` → harness "Unexpected error", ~2.64M tokens burned on q14.

## CANONICAL STATUS
**The canonical `tests/modeling/tpc_ds_duckdb/query14.preql` did NOT regress** — it still
generates valid SQL on the current engine (6525 chars, no sentinel). The canonical uses a
rowset/INTERSECT formulation (`cross_tuples` / `avg_sales` / `l0_filtered`, final
`by rollup ()` ordering by the *aliases*). The regression is reachable only via the agent's
alternate formulation (single `by rollup(channel,brand,class,category)` with `grouping()`
CASE projections, a HAVING vs a `by *` scalar average, and **ORDER BY by the raw dimension
concept paths**).

## SYMPTOM (failing SQL tail)
```sql
SELECT "friendly"."channel" ..., "friendly"."brand_id" ..., ...
FROM "friendly" INNER JOIN "concerned" on 1=1
WHERE "friendly"."_virt_agg_sum_5574077946046721" > "concerned"."overall_avg_sale"
ORDER BY
     'CATALOG'  asc nulls first,            -- raw all_sales.channel mis-resolved to a union-arm literal
    INVALID_REFERENCE_BUG asc nulls first,  -- all_sales.item.brand_id
    INVALID_REFERENCE_BUG asc nulls first,  -- all_sales.item.class_id
    INVALID_REFERENCE_BUG asc nulls first   -- all_sales.item.category_id
LIMIT (100), this should never occur. Please create an issue to report this.
```
The ORDER BY terms are the **raw rollup grouping keys** (`all_sales.channel`,
`all_sales.item.brand_id`, …). The final CTE (`friendly`) outputs only the
`grouping()`-CASE *aliases* (`channel`, `brand_id`, …) — the raw keys were dropped across
the FULL-JOIN rollup-vs-aggregate restructuring and are absent from `friendly`, so each
ORDER-BY raw key dead-ends. (The first key happened to resolve to a union-arm constant
`'CATALOG'`, the rest to the bare sentinel — both wrong.)

## MINIMAL REPRO
Requires all four ingredients: `by rollup` + `grouping()`-CASE projection +
HAVING vs a `by *` scalar + **ORDER BY by the raw concept path**. Dropping any one passes.

```preql
# model: evals/tpcds_agent/results/20260628-175514/workspace  (or any all_sales model)
import raw.all_sales as all_sales;
auto overall_avg_sale <- avg(all_sales.quantity * all_sales.list_price) by *;
select
    case when grouping(all_sales.channel) = 1 then null else all_sales.channel end as channel,
    case when grouping(all_sales.item.brand_id) = 1 then null else all_sales.item.brand_id end as brand_id,
    sum(all_sales.quantity * all_sales.list_price) as total_sales
having sum(all_sales.quantity * all_sales.list_price) > overall_avg_sale
by rollup (all_sales.channel, all_sales.item.brand_id)
order by all_sales.channel asc nulls first, all_sales.item.brand_id asc nulls first
limit 100;
```
Command (read-only generate_sql):
```python
from pathlib import Path
from trilogy.core.models.environment import Environment
from trilogy.dialect.enums import Dialects
env = Environment(working_path=Path("evals/tpcds_agent/results/20260628-175514/workspace"))
sql = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(open("repro.preql").read())[-1]
print("SENTINEL" in_sql := "INVALID_REFERENCE_BUG" in sql)
```
Peel-back results (current engine):
- drop the grouping() CASE (plain raw projection) → OK
- drop the HAVING `> avg by *` → OK
- **ORDER BY the aliases** (`order by channel, brand_id`) instead of raw paths → **OK** (this
  is the workaround the agent eventually stumbled onto; the on-disk workspace file uses
  aliases and now generates 11023 chars cleanly).

## ROOT CAUSE
1. **Newly-reachable build path (the regression trigger)** — `9dc18944` added auto-
   materialization of a HAVING comparison against a `by *` scalar aggregate as a hidden
   SELECT output (`_select_aggregate_outputs` / the HAVING-materialize block,
   `trilogy/parsing/v2/select_finalize.py:326` and ~1126–1176). Before this commit the
   query was rejected at author time; now it proceeds to build. The HAVING-vs-scalar forces
   the rollup into a multi-CTE shape: a `grouping()`-CASE rollup CTE (`sparkling`) FULL-JOINed
   to a re-aggregated grain CTE (`scrawny`) → `friendly`, whose outputs are the coalesced
   CASE aliases only.
2. **Unredirected ORDER BY raw key (the actual defect)** — the ORDER BY still references the
   *raw* rollup keys. The only redirect that points HAVING/ORDER BY source refs at a SELECT
   alias, `_rewrite_aliased_source_refs` / `_alias_rename_map`
   (`trilogy/parsing/v2/select_finalize.py:705,733`), **intentionally handles pure renames
   only** (`X as Y`), not `case … grouping(X) … as Y`. So the raw key is never rewritten to
   the alias, and (unlike the `built_membership` grain-key path at lines 1322–1342) it is not
   carried as a hidden output. At render time `render_order_item`
   (`trilogy/dialect/base.py:881`) finds the raw concept absent from `cte.output_columns`,
   falls through to `render_expr`, whose `safe_get_cte_value` lookup misses the final CTE's
   `source_map` → `INVALID_REFERENCE_STRING` (sentinel emitted ~`trilogy/dialect/base.py:1385`).

## FAMILY RELATION
Same sentinel + same render path as the existing INVALID_REFERENCE reports
(bug_q38/q64/q75/q23): a concept that is *needed late* (here an ORDER-BY term) dead-ends in
the final CTE because `source_map` propagation dropped it across a join/regroup boundary.
Mechanistically closest to the **order-by-carry** family (MEMORY q5/q83: order-by handle
points past a materializing boundary). New facet: the boundary is a `grouping()`-CASE rollup
projection (alias ≠ raw key address) combined with the FULL-JOIN restructuring newly induced
by the `9dc18944` HAVING-scalar materialization. Suggested fix direction: extend the
ORDER-BY → alias redirect (or the hidden-grain-key carry at 1322–1342) to cover a rollup key
that is projected only through a `grouping()`-CASE wrapper, OR carry the raw rollup keys as
hidden outputs of `friendly`. NOT fixed here (read-only diagnosis).
```
```
