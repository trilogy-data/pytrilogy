# Bug: `union(...)` TVF crashes rendering — "Could not find upstream map for multiselect"

**Status:** FIXED 2026-06-12. Root cause was TWO bugs, both fixed:

1. **Order-by union column grouped away (the crash).** The outer aggregate
   groups the union output to the relabel grain (`channel_name`/`entity_id`), so
   the `sort_ch`/`sort_id` columns the ORDER BY references survive nowhere
   downstream and the renderer falls into `find_source` with no mapping. Fix:
   `_carry_order_by_concepts` in `query_processor.py` pulls a `union(...)`/
   multiselect ORDER BY column (walking through its rowset handle) into the query
   grain + a hidden output, so a *single* group node keeps it (it is always an
   alias-source of a selected transform, hence 1:1 with a grain key — no extra
   row). Plain order-by concepts are untouched (they already render from their
   source_map CTE), so no plan churn for non-union queries.
2. **Dangling union-arm reference (latent, exposed by the carry's regroup).**
   Carrying the column produced an extra arm-group layer (`concerned`/`busy`),
   which `CollapseSingleParent` merged into its parent — repointing the deduped
   arm instance but NOT the divergent copy living in the union's `internal_ctes`,
   leaving `protective` rendering `FROM <merged-away-cte>` → DuckDB
   `CatalogException`. Fix: `UnionCTE.replace_dependency` now also repoints each
   internal arm whose own base is the replaced CTE.

Also: `BuildMultiSelectLineage.find_source` now raises a `RuntimeError`
("Internal planner error: …, order by the projected output column instead")
instead of a bare `SyntaxError`, so any residual unmapped shape is no longer
mislabeled as user syntax feedback.

Regression test: `tests/engine/test_duckdb_rowset.py::
test_tvf_union_order_by_grouped_away_column`.

---

**(original report below)**

**Status:** OPEN (found 2026-06-12, full-99 TPC-DS rebaseline, enriched q05).
**Severity: HIGH.** (1) It crashes the engine with an *unhandled* error mislabeled
as a "Syntax error", and (2) the crashing construct is the **recommended** one —
the agent followed current guidance and used `union(...)` (relational UNION TVF)
instead of a multi-select. A guided-correct query should never hard-crash.

## Symptom

`trilogy run query05.preql` → exit 1:

```
{"event": "error",
 "message": "Syntax error: Could not find upstream map for multiselect
   local.sort_ch@Grain<all_sales.channel_dim_text_id,all_sales.return_channel_dim_text_id,all_sales.sales_channel>
   | ... on cte (CTE(name='waggish', source=all_sales.catalog_dim_return_unified_..._union_..._union_...))"}
```

It is NOT a syntax error — the query parses fine. It's an internal planner
failure raised as a bare `SyntaxError` during SQL rendering.

## Repro (deterministic)

Query: `evals/_repros/q05_union_upstream_map_crash.preql` (also inline below).
A `union(...)` of two asymmetric arms over the unified `all_sales` model, whose
6-column output is consumed by an OUTER aggregate that re-references the union's
`sort_ch`/`sort_id` columns in a CASE relabel + `order by`.

```python
import sys; sys.path.insert(0, 'evals'); from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260612-133004_enriched/workspace')  # any raw-model ws
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
body = Path('evals/_repros/q05_union_upstream_map_crash.preql').read_text()
eng.generate_sql(body)        # -> SyntaxError: Could not find upstream map for multiselect ...
```

The crashing query:

```sql
import raw.all_sales as all_sales;

with base as union(
  (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
     and all_sales.channel_dim_id is not null
   select all_sales.sales_channel as sort_ch, all_sales.channel_dim_text_id as sort_id,
     sum(all_sales.ext_sales_price) as gross_sales, 0.0 as ret_amount,
     sum(all_sales.net_profit) as sales_np, 0.0 as ret_nl),
  (where all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date
     and all_sales.return_channel_dim_id is not null
   select all_sales.sales_channel as sort_ch, all_sales.return_channel_dim_text_id as sort_id,
     0.0 as gross_sales, sum(all_sales.return_amount) as ret_amount,
     0.0 as sales_np, sum(all_sales.return_net_loss) as ret_nl)
) -> (sort_ch, sort_id, gross_sales, ret_amount, sales_np, ret_nl);

select
  case base.sort_ch when 'STORE' then 'store channel' when 'CATALOG' then 'catalog channel'
       when 'WEB' then 'web channel' end as channel_name,
  case base.sort_ch when 'STORE' then concat('store', base.sort_id) ... end as entity_id,
  coalesce(sum(base.gross_sales), 0) as total_gross_sales,
  coalesce(sum(base.ret_amount), 0) as total_returns,
  coalesce(sum(base.sales_np), 0) - coalesce(sum(base.ret_nl), 0) as net_profit
order by base.sort_ch, base.sort_id
limit 100;
```

## Raise site

`trilogy/core/models/build.py:1909`, `BuildMultiSelectLineage.find_source`
(inherited by `BuildUnionSelectLineage`, defined just below at ~1914):

```python
def find_source(self, concept, cte):
    for x in self.align.items:
        if concept.name == x.alias:
            for c in x.concepts:
                if c.address in cte.output_lcl:
                    return c
    raise SyntaxError(f"Could not find upstream map for multiselect {concept} on cte ({cte})")
```

Rendering the outer aggregate's reference to `base.sort_ch` recurses
`render_concept_sql` -> `_render_concept_sql` -> `find_source` (`dialect/base.py`
1025/1040). For the union output `sort_ch`, no align-item concept's `address` is
present in the union CTE's `output_lcl`, so it falls through to the raise. (The
traceback is ~1.2MB of `render_concept_sql` self-recursion before the raise — the
renderer walks the lineage chain deeply, which is itself worth a look.)

## Minimization notes (for the fixer)

The FULL 6-column query reproduces every time. Simpler forms do NOT — so start
from the full repro, don't assume the reductions below are equivalent:
- 3-column union (`sort_ch, sort_id, gross_sales`) consumed by passthrough /
  re-aggregate / CASE+group: all OK.
- 3-column asymmetric arms (`0.0` vs `sum`) + outer re-aggregate / CASE: OK.
The trigger needs the full shape — multiple asymmetric measure columns
(`0.0` constants aligned against `sum(...)` across arms) AND the outer
CASE-relabel + re-aggregate that re-references `sort_ch`/`sort_id`. Likely the
union CTE's `output_lcl` doesn't carry the alias `sort_ch`/`sort_id` map the
outer select needs, or carries a mangled per-arm address instead of the aligned
output address (`find_source` matches on `x.alias` + `c.address ∈ output_lcl`).

## Two things to fix

1. **The plan should render.** A `union(...)` output re-consumed by an outer
   CASE/aggregate is valid; `find_source` must resolve the union output concept
   against the union CTE. Look at how `BuildUnionSelectLineage` populates
   `output_lcl` / the align map vs what the renderer asks for.
2. **Never crash like this.** Even if some shape is genuinely unsupported, raising
   a bare `SyntaxError` mislabeled "Syntax error" is wrong — it should be an
   internal-error class with an actionable message, not a planner crash surfaced
   to the user as bogus syntax feedback.

## Context

- Related TVF/union machinery: `project_tvf_union_implementation`,
  `project_tvf_union_aggregate_arms`, `BuildUnionSelectLineage`,
  `Derivation.TVF_UNION`, `UnionNode`.
- Found during full-99 rebaseline `results/20260612-180707_enriched` (q05). The
  run was NOT polluted (no in-flight source edits); this is a genuine result.
