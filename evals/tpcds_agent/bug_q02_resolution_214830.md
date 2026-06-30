# q02 token sink (run 20260629-214830, 1.55M tokens) — framework bug analysis

Model: deepseek-chat. Agent log: `results/20260629-214830/agent_log.q02.jsonl` (135 events).
Final workspace file `workspace/query02.preql` DOES run (agent recovered at write [124]); the
1.55M tokens were burned thrashing against two framework resolution bugs before stumbling on the
exact phrasing that dodges them.

q02 = per week_seq, ratio of each day-of-week's WEB+CATALOG sales to the SAME day-of-week 53 weeks
later. Canonical solves this with a single `lead(amt, 53) over (order by week_seq)` window — NO
self-join. The agent instead built a two-rowset self-relation (`cur` vs `nxt`) joined at
`cur.ws + 53 = nxt.ws`, which is where it hit the framework walls.

## Canonical builds: YES
`tests/modeling/tpc_ds_duckdb/query02.preql` parses, resolves, and `generate_sql` emits 1 statement
(4632 chars, window/LEAD present). No self-join. Confirmed against current HEAD.

## Error inventory (from log)
| event | error | construct |
|---|---|---|
| [20] | `Discovery error: couldn't source ... {d.day_of_week, d.week_seq, local.in_2001}` | early design: imported `date_dim d` separately from sales (agent dropped it) — not pursued here |
| [44] | Syntax [102] subquery refused | agent tried SQL `in (select ...)` — friendly error, working as intended |
| [53]/[89] | Syntax [213]/[212] `by <grain>` placement | friendly errors, working as intended |
| [65] | `Undefined concept: next.week_seq` | agent referenced a rowset (`next`) it never declared — agent error, clean message |
| **[80]** | **`Ambiguous reference 'joined.day_of_week': matches ['joined.cur.day_of_week','joined.nxt.day_of_week']`** | **FRAMEWORK BUG #1** |
| **[95]** | **`cannot merge all concepts ... 8 disconnected subgraphs: {cur...week_seq, nxt.sales}; {local._virt_filter_sales_*}; ...`** | **FRAMEWORK BUG #2** |
| **[122]** | **`Ambiguous reference 'ratios.dow': matches ['ratios.cur.dow','ratios.nxt.dow']`** | **FRAMEWORK BUG #1 again** |

Both [80] and [122] are the same bug (the `joined`/`ratios` rowset name differs only in spelling).

---

## FRAMEWORK BUG #1 — rowset self-join join-key leaks as a phantom output, ONLY inside `def` macro bodies

> **STATUS 2026-06-29: FIXED.** A scoped-join condition's other-side column
> (`ratios.nxt.dow`) is now recorded at COLLECT_SYMBOLS in a static
> `rowset_join_key_leaks` registry (a rowset-output address that appears ONLY in
> a `JOIN_CLAUSE` literal, never in the SELECT list). `_collapse_alias_matches`'s
> `not real` branch (the `def`-body case where neither candidate has committed)
> drops registered leaks when a non-leak candidate remains, so the genuine
> `ratios.cur.dow` resolves unambiguously. Producer: `statement_plans.py`
> `collect_symbols` + `symbols.find_join_clause_literals`. Consumer:
> `semantic_state.py _collapse_alias_matches`. Registry on
> `environment.py EnvironmentConceptDict`. Tests:
> `test_rowset_output_shorthand.py::test_self_join_phantom_join_key_not_ambiguous_inside_def`
> (+ leak-registry isolation test).


### Minimal repro (fails on current HEAD)
```trilogy
import raw.all_sales as s;
rowset cur <- where s.channel in ('WEB','CATALOG')
  select s.date.week_seq as ws, s.date.day_of_week as dow, s.date.year, sum(s.net_paid) as sales;
rowset nxt <- where s.channel in ('WEB','CATALOG')
  select s.date.week_seq as ws, s.date.day_of_week as dow, s.date.year, sum(s.net_paid) as sales;
rowset ratios <- where cur.year = 2001
  select cur.ws, cur.dow, round(cur.sales / nxt.sales, 2) as r
  inner join cur.ws + 53 = nxt.ws
  inner join cur.dow = nxt.dow;        -- nxt.dow appears ONLY in a join condition

def pv(d) -> sum(ratios.r ? ratios.dow = d) by ratios.ws;   -- ratios.dow inside a def
select ratios.ws as wk, @pv(0) as sun;
-- ERROR: Ambiguous reference 'ratios.dow': matches ['ratios.cur.dow','ratios.nxt.dow']
```
`nxt.dow` is never a *projected output* of `ratios` (only `cur.dow` is) — it leaks from the join
condition `cur.dow = nxt.dow` into namespace `ratios`, so the leaf-shorthand `ratios.dow` finds two
candidates.

### Trigger matrix (rowset outputs unaliased `cur.dow`; reference is `ratios.dow`)
| downstream reference site | result |
|---|---|
| `select ratios.dow` (plain) | **OK** |
| `select sum(ratios.r ? ratios.dow = 0) by ratios.ws` (inline filtered agg) | **OK** |
| `def pv(d) -> sum(ratios.r ? ratios.dow = d) by ratios.ws` (filtered agg in macro) | **FAILS** |
| `def pE() -> sum(ratios.r ? ratios.dow = 0) by ratios.ws` (no param, in macro) | **FAILS** |
| `def pF(d) -> ratios.dow + d` (trivial expr in macro) | **FAILS** |
| `def pG(d) -> sum(ratios.r) by ratios.dow` (dow as grain in macro) | **FAILS** |
| aliased outputs `cur.dow as dow` + the failing `def` (the agent's [124] fix) | **OK** |

So: the ambiguity fires for **any** reference to `ratios.dow` from **inside a `def` body**, and is
avoided by either (a) referencing it in a normal select/expr, or (b) explicitly aliasing the rowset
output (`cur.dow as dow`).

### Root cause — `trilogy/parsing/v2/semantic_state.py`
Fires at `_resolve_rowset_suffix` line **626-631** (confirmed via traceback — NOT the parallel
`environment.py:_try_resolve_namespace_suffix:322`). The collapse that is *supposed* to drop the
phantom is `_collapse_alias_matches` (lines **544-579**); its docstring even cites this exact q02
case (`a scoped self-join references the OTHER source's same-named column as a join key
(joined.nxt.wk)`). The collapse works by anchoring on a **real/pending** match:

```python
# semantic_state.py ~562-571
real = [a for a in matches if _existing_concept(a) is a real Concept]
forward = [...]
if not real:
    return matches          # <-- line 570-571: gives up, returns ALL candidates
```

Runtime instrumentation of `_collapse_alias_matches(['ratios.nxt.dow','ratios.cur.dow'])`:

| reference site | `ratios.cur.dow` | `ratios.nxt.dow` | outcome |
|---|---|---|---|
| plain `select` (works) | existing=**Concept**, pending=**True** | existing=None, pending=False | `real=[cur.dow]` → phantom `nxt.dow` dropped (not pending) → unambiguous |
| inside `def` (fails) | existing=**None**, pending=**False** | existing=None, pending=False | `not real` → **returns both** → false ambiguity |

The `def` macro body resolves `ratios.dow` at a phase where the genuine output `ratios.cur.dow` has
**not yet been committed** as a real/pending concept (both candidates are bare forward refs), so the
`if not real: return matches` guard short-circuits before the phantom join-key can be filtered out.
The direct-select path works only because by its resolution time `ratios.cur.dow` is already real+
pending, letting the existing q02 fix drop the non-pending `ratios.nxt.dow`.

### Prior handoff status: PARTIALLY FIXED → **RECURRING**
The prior `bug_q02_self_join_ambiguous_week_side.md` fix landed (`_collapse_alias_matches`, docstring
references `joined.nxt.wk`) and covers direct/inline references. It does **not** cover references
originating inside a `def` macro body, where neither candidate is yet real — so the same false
"Ambiguous reference" still fires. This is the dominant current obstacle (hit at both [80] and [122]).

---

## FRAMEWORK BUG #2 — filtered-aggregate `_virt_filter` concepts orphaned from a scoped-join component

> **STATUS 2026-06-29: REFRAMED + PARTIALLY FIXED.** Investigation showed the
> `_virt_filter` "orphan" is NOT what blocked the agent — it was a *misleading
> error symptom*. The true root cause of the orphan subgraph is `_island_rowsets`
> (discovery_utility.py): islanding severs EVERY edge crossing a rowset boundary,
> including the legitimate edge from a downstream consumer (a filtered aggregate
> `sum(cur.sales ? ...)`) to the rowset's *declared output* — the exact
> "concept DERIVED from [a rowset output]" false-positive the islanding docstring
> already warns about. **FIX**: after islanding reconnects a rowset's outputs to
> its hub, also reconnect each external `g`-successor (downstream consumer) of an
> output to the hub; only UPSTREAM navigation (base concepts) stays severed.
> Grain-only `by`-key consumers are skipped (else two unrelated models bridge
> through an aggregate's grouping key — guarded by
> `test_cross_cte_aggregate_grain_only_bridge_raises`). This corrects the
> connectivity checker and removes the confusing `{_virt_filter_...}` subgraph
> from the error. Tests: `test_disconnected_subgraphs.py::test_filter_over_scoped_join_rowset_measure_not_islanded`.
>
> **What actually unblocked q02**: BUG #1's fix alone. The natural `ratios` form
> (scoped join INSIDE a rowset, then a filtered aggregate over its single output —
> all 7 `@pv(0..6)` day buckets) now resolves end-to-end. Added as the
> `filtered_aggregate_over_scoped_join_rowset` SUCCESS cell of the rowset
> generation matrix.
>
> **Residual limitation (NOT a connectivity bug)**: grouping ONE rowset's measure
> by ANOTHER rowset's key through an *outer* scoped join
> (`sum(nxt.sales ? ...) by cur.week_seq` with the join at the outer select) is
> genuinely unexpressible — the declared join lives at the outer select, not
> inside the aggregate's grain. This is the report's literal Bug #2 repro. It now
> resolves to a clean `DisconnectedConceptsException` (do the join inside a rowset
> first, i.e. the `ratios` form). Guarded as the
> `cross_rowset_grouped_aggregate_offset_join_clean_error` CLEAN-ERROR matrix cell.
> Making it actually compute (threading the outer scoped join into the inner
> aggregate's grain resolution) is a separate, larger planner change.


### Minimal repro (fails on current HEAD)
```trilogy
import raw.all_sales as s;
rowset cur <- where s.channel in ('WEB','CATALOG')
  select s.date.week_seq, s.date.day_of_week, sum(s.net_paid) as sales;
rowset nxt <- where s.channel in ('WEB','CATALOG')
  select s.date.week_seq, s.date.day_of_week, sum(s.net_paid) as sales;
def ratio_dow(d) -> round(
  sum(cur.sales ? cur.day_of_week = d) by cur.week_seq
  / sum(nxt.sales ? nxt.day_of_week = d) by cur.week_seq, 2);
select cur.week_seq, @ratio_dow(0) as sun
  inner join cur.week_seq + 53 = nxt.week_seq
  inner join cur.day_of_week = nxt.day_of_week;
-- ERROR: cannot merge all concepts ... disconnected subgraphs:
--        {cur...week_seq, nxt.sales}; {local._virt_filter_sales_<hash>}; ...
```

### Trigger matrix
| variant | result |
|---|---|
| single filtered agg over ONE rowset, no `nxt` (`sum(cur.sales ? ...) by cur.week_seq`) | **OK** |
| two rowsets, filtered aggs, **with** scoped join (above) | **FAILS** — orphan `_virt_filter` singletons |
| two rowsets, filtered aggs, **without** any join | FAILS — but here genuinely unjoinable (correct) |

The scoped join DOES connect the raw rowset outputs (`{cur.week_seq, nxt.sales}` land in one
subgraph), but each filtered-aggregate virtual concept `_virt_filter_sales_<hash>` (one per
`x ? cond`, hence 7+ in the full q02) stays a **singleton subgraph** — the scoped join never pulls
the filter-derived measures into the connected component.

### Root cause / file:line
- `_virt_filter_<hash>` minted in `trilogy/core/models/build.py:126` (`get_filter_*_name`,
  `VIRTUAL_CONCEPT_PREFIX` from `constants.py:14`) for every `x ? cond` filtered aggregate.
- The merge that reports the failure: `trilogy/core/processing/discovery_utility.py:875-877`
  (`cannot merge all concepts ... disconnected subgraphs`); the orphan `_virt_*` subgraph surfacing
  is at discovery_utility.py:998. The gap is that a `_virt_filter` concept whose filter predicate is
  on a scoped-join rowset column is not associated with that rowset's connected component during
  Steiner/merge discovery.

### Prior handoff status: **RECURRING**
Matches the family described by the (named-but-absent) `bug_q02_offset_week_virt_filter_resolution.md`
— the offset/filtered self-relation still explodes into per-`_virt_filter` disconnected subgraphs.

---

## Summary of prior-bug status
- `bug_q02_self_join_ambiguous_week_side.md` (ambiguous join-key leak): **PARTIALLY FIXED / RECURRING** —
  fixed for direct refs (`_collapse_alias_matches`), still fires from inside `def` macro bodies.
- `bug_q02_offset_week_virt_filter_resolution.md` (offset `_virt_filter` disconnect): **RECURRING**.

Note: neither named handoff `.md` file currently exists on disk under `evals/tpcds_agent/`; their
content survives only as the `_collapse_alias_matches` docstring (q02 / `joined.nxt.wk`) and the
`discovery_utility` `_virt_*` orphan handling.

## Verdict
>500k tokens ⇒ framework bug: CONFIRMED. The agent's natural rowset self-join phrasing is blocked by
Bug #1 (ambiguity in `def` bodies) and Bug #2 (`_virt_filter` orphaning). It only escaped by
discovering the precise dodge (explicit `cur.dow as dow` output aliases + a `sum(...) by ratios.ws`
pivot that keeps the agg outside a problematic shape). Recommend the framework either (a) teach the
agent toward the `lead(amt, 53)` window idiom for offset self-relations, and/or (b) fix Bug #1 by
making the `def`-body resolution defer until rowset outputs are committed (or have
`_collapse_alias_matches` drop a non-pending, non-output join-key leak even when `not real`).
