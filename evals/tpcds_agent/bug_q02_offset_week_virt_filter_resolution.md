# q02 — `_virt_filter` UnresolvableQueryException when filtering a rowset/aggregate output by an out-of-grain concept

Run: `evals/tpcds_agent/results/20260628-174846` (q02 enriched leg, ~1.23M tokens / 40 turns, FAILED).

## Symptom

`trilogy run query02.preql` raised, twice, with different hashes:

```
Resolution error in query02.preql: Could not resolve connections for query with output
['local._virt_filter_wk_5614907096583429<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
```

Run→write mapping (verified from `agent_log.q02.jsonl`): the two failures are `run` at log line 43 (file = write **40**) and `run` at line 91 (file = write **88**). No `DisconnectedConceptsException` occurred in this run; the only other error was a parse slip (`rowset ... as` instead of `<-`, write 34). The agent's offset-join attempts (`inner join weekly_sales.wk + 53 = future_sales.wk`, writes 46–82) generated SQL fine — they returned wrong/empty data, not a resolution error.

## The failing construct

Both failures share one shape: **a filtered membership / FILTER concept where a ROWSET (aggregate) output column is filtered by a base-model concept that is NOT in the rowset's grain.**

```trilogy
rowset all_weekly_sales <- where s.channel in ('WEB','CATALOG')
select s.date.week_seq as wk, s.date.day_of_week as dow, sum(s.sales_price*s.quantity) as sales;

select all_weekly_sales.wk, ...
having
    all_weekly_sales.wk in (all_weekly_sales.wk ? s.date.year = 2001);   -- <-- fails
```

`all_weekly_sales.wk ? s.date.year = 2001` mints a FILTER-derivation virtual concept `_virt_filter_wk_<hash>` (minted at `trilogy/parsing/common.py:1472`, `_virt_filter_{content.name}_{hash}`). Its filtered concept is the rowset output `wk`; its condition row-argument is `s.date.year`, a base concept the rowset aggregated away. The two live in subgraphs the resolver cannot co-source, so it cannot build a node producing `_virt_filter_wk`.

## Minimal repro

Model dir: `tests/modeling/tpc_ds_duckdb` (uses `all_sales`). `generate_sql` only — no execution needed.

```python
from pathlib import Path
from trilogy import Environment, Dialects
env = Environment(working_path=Path("tests/modeling/tpc_ds_duckdb"))
env.parse("import all_sales as sales;")
text = """
rowset r <- where sales.channel in ('WEB','CATALOG')
select sales.date.week_seq as wk, sales.date.day_of_week as dow, sum(sales.ext_sales_price) as amt;
select r.wk, r.amt
having r.wk in (r.wk ? sales.date.year = 2001);
"""
Dialects.DUCK_DB.default_executor(environment=env).generate_sql(text)
# -> UnresolvableQueryException: Could not resolve connections for query with output
#    ['local._virt_filter_wk_8797659759950179<Purpose.PROPERTY>Derivation.FILTER>'] ...
```

Discriminating variants (same env):

| Variant | Result |
|---|---|
| `having r.wk in (r.wk ? sales.date.year = 2001)` — filter rowset col by **out-of-grain base** concept | **FAIL** (`_virt_filter_wk`) |
| `having r.wk in (r.wk ? r.dow = 0)` — filter rowset col by an **in-grain rowset** col | OK |
| `auto rel <- r.wk ? sales.date.year = 2001; ... having r.wk in rel` — **named**, not inline | **FAIL** (identical) |
| `where ... and sales.date.week_seq in (sales.date.week_seq ? sales.date.year=2001) select ...` — filter **base** concept (no rowset) | OK |

Key findings: it is the **rowset-output-column ⨉ out-of-grain-condition** combination that fails, not the membership idiom itself; **naming the filter via `auto` does not help** (rules out the inline-vs-named hypothesis from `project_q02_*`); filtering a plain **base** concept by a sibling base concept is fine (this is the canonical idiom, below).

## Classification: (b) inherent limitation + needs-clearer / earlier error — NOT a planner bug

The query as written is genuinely unsatisfiable: a rowset materializes a CTE at grain `(wk, dow)` and discards `s.date.year`; `r.wk` is an alias that severs lineage back to `sales.date.week_seq`, so there is no join path from the rowset output to `date.year`. The resolver correctly returns "no node". So this is not a constructible-but-wrongly-rejected query — it is the resolver right that you cannot filter an aggregate's output by a column the aggregate threw away.

The real defect is the **opaque error**. It names an internal hashed virtual concept (`_virt_filter_wk_8797659759950179<Derivation.FILTER>`) and says only "Could not resolve connections," giving the agent zero signal that the cause is "the filter condition `s.date.year` is not part of the grain of `r.wk` — add it to the rowset grain, or filter the base concept instead." That opacity is what drove the 40-turn / 1.23M-token churn.

The canonical query (`tests/modeling/tpc_ds_duckdb/query02.preql`) sidesteps it entirely by filtering the **base** concept, named:
```trilogy
auto relevent_week_seq <- sales.date.week_seq ? sales.date.year in (2001,2002);
where sales.channel in ('WEB','CATALOG') and sales.date.week_seq in relevent_week_seq
```
and expresses the 53-week offset with a window (`lead(amt, 53) over (order by sales.date.week_seq)`) over a per-`week_seq` aggregate — not a self-join. The agent itself eventually found the equivalent fix: add `s.date.year as yr` to the rowset grain and filter the in-grain `yr` (`having all_weekly_sales.yr = 2001`, write 109 / final workspace file) — which resolves.

## Root cause (file:line)

- Mint site: `trilogy/parsing/common.py:1472` — `_virt_filter_{content.name}_{hash}` for `<concept> ? <cond>`.
- Resolution failure / emission: `trilogy/core/processing/concept_strategies_v3.py:632-662` — `source_query_concepts` calls `search_concepts` for the FILTER concept's mandatory set (filtered concept `r.wk` + condition row-arg `s.date.year`); `validate_stack` never reaches `ValidationResult.COMPLETE` because the rowset-output node exposes no join path to `date.year`, so `search_concepts` returns `None`. `disconnected_components` does not split into >1 group here (the rowset output and date.year aren't both free base nodes — `r.wk` is a rowset-bound alias), so it falls through past the `DisconnectedConceptsException` branch (line 654) to `raise UnresolvableQueryException` at line 659. The duplicate emission point is `trilogy/core/query_processor.py:679`.
- Why no clean message: the resolver reports the auto-generated virtual concept address rather than diagnosing that a FILTER concept's condition row-argument is unreachable from the filtered concept's grain.

## Relation to priors

- `project_join_expression_keys` ("Unblocks q02 `join a.k+53=b.k`"): confirmed **unblocked** — the agent's `inner join weekly_sales.wk + 53 = future_sales.wk` attempts parsed and generated SQL (they failed on results, not resolution). This `_virt_filter` failure is a **separate, distinct construct** (filtered membership in HAVING over a rowset output), not the offset-join gap resurfacing.
- `project_q02_week_over_week_offset_idiom_gap` ("real gap = offset-key join + window-pre-filter friction"): this is the **window-pre-filter friction** manifesting concretely — the agent's natural way to restrict the rowset to 2001 weeks (`rowset.wk ? base.year=2001`) is exactly the unresolvable shape.

## Suggested direction (do NOT fix here)

Detect, at resolution time, a FILTER concept whose condition row-arguments are unreachable from the filtered concept's grain (rowset/aggregate output) and raise an author-facing error naming the offending condition concept (e.g. "`s.date.year` is not available at the grain of rowset output `all_weekly_sales.wk`; add it to the rowset's select, or filter the base concept `s.date.week_seq` instead") rather than the opaque `_virt_filter_wk_<hash>` "could not resolve connections". An agent-info note that out-of-grain filters on rowset outputs are unsupported would also have short-circuited the churn.
