# q05 — global aggregate over a 2-level rowset chain does not collapse (SILENT wrong numbers + row fan-out)

**Status:** OPEN. Reproduced on the eval workspace
`evals/tpcds_agent/results/20260628-042638_enriched/workspace/` (has `trilogy.toml` +
`tpcds.duckdb`). Surfaced in that run's q05 (FAILED). Sibling of the OPEN
`q05_union_measure_broadcast_bug.md` (same union/rowset grain family, different symptom).

## Symptom

When a `union(...)` rowset feeds a **second** rowset that only re-projects/relabels its
measure, a downstream aggregate over that measure loses its grain:

- a **global** `sum(formatted.gross)` returns **62,237 rows instead of 1** (it does not
  collapse to the query's abstract grain);
- in the agent's actual q05 the chain `union → formatted-relabel → by rollup` over-counts the
  **WEB** channel: subtotal **26.57M** vs correct **19.64M**, inflating the grand total
  **112,458,734 → 119,387,633**. WEB is the only SCD dimension here — `web_site` has up to 3
  surrogate keys (`channel_dim_id`) per business id (`channel_dim_text_id`), so the lost-grain
  measure fans out across the surrogate versions. STORE/CATALOG (non-fanning) stay correct.

No error is raised — the number is silently wrong.

## Minimal repro

Run from the workspace above:

```bash
.venv/Scripts/trilogy.exe run --all-rows repro_min.preql
```

```trilogy
import raw.all_sales as s;

with combined as union(
  (where s.date.date between '2000-08-23'::date and '2000-09-06'::date and s.channel_dim_id is not null
   select s.channel as channel, s.channel_dim_text_id as ent, s.ext_sales_price as gross),
  (where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_id is not null
   select s.channel as channel, s.return_channel_dim_text_id as ent, 0::float as gross)
) -> (channel, ent, gross);

# (A) single-level: global sum over the union rowset -> 1 row (CORRECT)
select sum(combined.gross) as total_gross_A;

# passthrough/relabel rowset that only re-projects the measure
with formatted as select concat('x', combined.ent) as ent_fmt, combined.gross;

# (B) global sum over the 2-level chain -> should be 1 row; BUG: many rows
select sum(formatted.gross) as total_gross_B;
```

Observed: statement (A) `row_count: 1` (correct collapse). Statement (B)
`row_count: 62237`. The ONLY difference is the interposed `formatted` passthrough rowset —
same data, same measure. (In the agent's fuller q05 `formatted`, which carried more grain
columns, the same bare global sum returned 861 rows; the fan-out magnitude depends on the
relabel rowset's grain, but the non-collapse is constant.)

Contrast that pins it: `sum(combined.gross)` over the **single** union rowset is correct
(1 row); add one passthrough rowset layer and the same global aggregate stops collapsing.

## Root-cause hypothesis (suspects, NOT a fix)

The measure `formatted.gross` is a 1:1 passthrough of the union output `combined.gross`. For
grain purposes its identity should resolve back to the union's (abstract) row grain so a
global aggregate grounds at abstract grain and collapses to one row. Two suspects:

- `trilogy/core/models/author.py:1249-1257` (`get_select_grain_and_keys`): a `RowsetItem`
  output returns `self.grain` (its own grain) and "must not inherit the consuming select's
  grain." When the rowset is itself *built on another rowset* (`formatted` over the `combined`
  union), the outer rowset's measure carries the inner union's row identity through a second
  layer; the consuming global `select sum(...)` then never re-grounds it to abstract grain, so
  the planner keeps the rowset's per-row grain and emits one row per underlying row.
- `trilogy/core/processing/grain_utility.py:102-125` (`concept_source_address` /
  `rowset_source_grain`): the BASIC-ALIAS recursion unwraps a single rename to its rowset
  source, but `formatted.gross` reaches the union output through the `formatted` rowset's
  ROWSET derivation (line 103-106 returns `lineage.content.address`), i.e. only ONE rowset hop
  is followed. A measure two rowset layers deep likely resolves to `formatted`'s row grain
  rather than the union's abstract grain — so it looks like real grain and forces per-row output.

Plan-shape check: `trilogy run repro_min.preql --debug-file <f>` — the aggregate's GroupNode
sits over the `formatted` RowsetNode over the `combined` UnionNode; the global `sum` does not
get an abstract (groupless) GroupNode the way statement (A) does.

## Relation to existing bugs / memory

- **`evals/tpcds_agent/q05_union_measure_broadcast_bug.md` (OPEN)** — same union/rowset
  grain-resolution family. That file: single-level line-grain union directly aggregated; a
  per-arm measure broadcasts at channel grain onto every entity. This file: a **second** rowset
  layer (relabel passthrough) makes even a **global** aggregate fail to collapse and fan out on
  SCD keys. Likely the same underlying defect (union output's grain identity not propagated
  through downstream rowset/aggregate re-grounding); worth fixing together.
- **`q5_rowset_over_union_rowset_downstream_recursion.md`** /
  memory `project_q5_rowset_over_union_rowset_recursion.md` — identical structural shape
  (a rowset built over a union rowset) but that one manifests as a planner RecursionError when
  order-by/CASE walks past the materializing rowset; here the same shape silently mis-grains a
  global aggregate. The `get_select_grain_and_keys` RowsetItem branch (author.py:1249) was the
  fix site there too.
- **`q05_union_derived_output_recursion_bug.md`** / memory
  `project_union_derived_output_recursion_bug.md` — abstract-grain union output inheriting the
  consuming select's grain → cycle; the inverse failure mode of the same grain-inheritance seam.
- **`q54_group_by_rowset_aggregate_output_count_cross_join.md`** — another SILENT wrong-result
  in the rowset-aggregate-grain family (re-grained rowset output read as scalar).

## Workaround (works today)

Do the relabeling inline in the final aggregating select (no second rowset), or pre-aggregate
each side to entity grain in separate rowsets and combine with a scoped `full join` + `coalesce`
(the agent's eventual path in `q05_union_measure_broadcast_bug.md`). The canonical
`tests/modeling/tpc_ds_duckdb/query05.preql` avoids the whole family by doing one pass over
`all_sales` with a `windowed(...)` macro and no rowsets.
