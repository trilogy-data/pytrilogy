# q02 — `_virt_filter` resolution failures when filtering a rowset/aggregate output (UnresolvableQueryException AND, new, DisconnectedConceptsException)

Run: `evals/tpcds_agent/results/20260628-174846` (q02 enriched leg, ~1.23M tokens / 40 turns, FAILED).

> **UPDATE 2026-06-28 (run `evals/tpcds_agent/results/20260628-194910`, q02 ~779k tokens):** the same
> `_virt_filter`-over-rowset weakness resurfaced in a **second manifestation** — a
> `DisconnectedConceptsException` (not the `UnresolvableQueryException` documented below) when filtered
> aggregates `sum(x ? cond)` are projected over **both sides of a scoped self-join of two rowsets**. See
> the new section "Manifestation 2" at the bottom. (The same run also hit a *separate, unrelated*
> false-ambiguity bug in rowset self-join leaf-shorthand — written up independently in
> `bug_q02_self_join_ambiguous_week_side.md`, not part of this `_virt_filter` family.)

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

### FIXED 2026-06-28 (Manifestation 1 — clearer error only; the query stays unsatisfiable by design)

`diagnose_unreachable_rowset_filter(output_concepts)` (`trilogy/core/processing/discovery_utility.py`) runs when discovery returns no node, just before the two generic emission points (`source_query_concepts` in `concept_strategies_v3.py` and the v4 dead-end in `query_processor.py`). It looks for a `Derivation.FILTER` output whose `BuildFilterItem.content` is a rowset output (`Derivation.ROWSET` / `BuildRowsetItem`) and whose `where.row_arguments` contains an address NOT in the rowset's `derived_concepts`. When found it raises:

> Cannot filter rowset output `r.wk` by `sales.date.year`: the filter condition references concept(s) not available at the grain of rowset `r` (its outputs are {`r.amt`, `r.dow`, `r.wk`}). A rowset is materialized at its own grain, so a column it aggregated away cannot filter its outputs. Either add the condition concept(s) to the rowset `r`'s select, or filter the underlying base concept before/inside the rowset instead.

Fires for both the inline (`having r.wk in (r.wk ? sales.date.year=2001)`) and named-`auto` forms. Because it only runs on an already-failing path (root is None), it's purely a message refiner — no behavior change. The in-grain case (`? r.dow=0`) and base-concept filters still resolve. **Manifestation 2 is unchanged**: its condition (`cur.dw`) IS in-grain, so the diagnosis correctly does not fire and it keeps its existing `DisconnectedConceptsException` named-subgraph message. Tests: `test_non_benchmark_queries.py::test_q02_filter_rowset_output_by_out_of_grain_concept_clean_error` (+ `_by_in_grain_concept_still_resolves` guard).

---

## Manifestation 2 — DisconnectedConceptsException: filtered aggregate `sum(x ? cond)` over both sides of a scoped self-join of two rowsets (added 2026-06-28)

Run `evals/tpcds_agent/results/20260628-194910`, `agent_log.q02.jsonl` writes at log lines 41 and 47
→ errors at lines 45 and 51. The agent built two per-`(week_seq, day_of_week)` rowsets (`cur`, `nxt`),
scoped-self-joined them on the 53-week offset (`cur.wk + 53 = nxt.wk`, `cur.dw = nxt.dw`), then pivoted
each day-of-week by projecting **filtered aggregates over both joined sides**:

```trilogy
select
    cur.wk as week_seq,
    round(sum(cur.amt ? cur.dw = 0) / sum(nxt.amt ? nxt.dw = 0), 2) as sunday,
    ...                               -- monday..saturday, same shape
inner join cur.wk + 53 = nxt.wk
inner join cur.dw = nxt.dw
where cur.wk in weeks_in_2001;
```

```
Resolution error: Discovery error: cannot merge all concepts into one connected query.
The requested concepts split into 7 disconnected subgraphs:
{cur.wk, nxt.amt}; {local._virt_filter_amt_<hash>}; ...   (one isolated _virt_filter_amt per day)
```

Each `sum(<rowset output> ? <cond>)` mints a `_virt_filter_amt_<hash>` FILTER concept that the resolver
cannot co-source with the join graph `{cur.wk, nxt.amt}`, so every pivot column splits into its own
isolated subgraph → `DisconnectedConceptsException` (NOT the `UnresolvableQueryException` of
Manifestation 1).

### Trigger matrix (`generate_sql`, eval workspace `raw.all_sales`)

| variant | result |
|---|---|
| **single** rowset, projected `sum(cur.amt ? cur.dw = 0)` (no self-join) | **OK** (len ~1664) |
| self-join, filtered aggregate over **cur only** (`nxt` leg pruned, unused) | **OK** (len ~1664) |
| self-join, filtered aggregate over **both** sides `sum(cur.amt ? ...) / sum(nxt.amt ? ...)` | **DisconnectedConcepts** (`_virt_filter_amt`) |
| self-join, **`case when`** pivot `sum(case when cur.dw=0 then cur.amt else null end)` (both sides) | **OK** (len ~1262) |

Key: the failure needs the **scoped self-join + filtered aggregate over the SECOND (joined) side**.
A single rowset filtered aggregate resolves fine (so the condition `cur.dw` being in-grain is not the
issue here — unlike Manifestation 1, which is about an *out-of-grain* condition). The `case when`
pivot is the working idiom and the agent eventually reached it.

### Why same family

Same `_virt_filter_*` minting machinery and the same underlying weakness — a FILTER concept over a
rowset/aggregate output that the resolver cannot attach to the surrounding (here, join) graph. The
surface differs: Manifestation 1 = filtered membership/aggregate in HAVING over a single rowset output
by an out-of-grain base concept → `UnresolvableQueryException`; Manifestation 2 = filtered aggregate in
the SELECT projection over the far side of a scoped self-join → `DisconnectedConceptsException`. Both
would benefit from (a) the FILTER concept carrying the join/rowset lineage of its filtered operand, or
(b) a clear author-facing error pointing at `case when` as the supported pivot idiom.
