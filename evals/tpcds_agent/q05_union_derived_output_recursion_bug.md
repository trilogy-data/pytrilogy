# Bug: `generate_sql` RecursionError — derived concept over a `union(...)` output column (q05)

**Status:** FIXED 2026-06-25. Root cause was a **cyclic grain on union outputs**, not the
`_upstream_concepts` walk the original analysis below blamed. Regression test:
`tests/engine/test_duckdb_rowset.py::test_tvf_union_derived_concept_over_output_aggregated_no_recursion`.
**Surfaced by:** TPC-DS q05 enriched eval, run `20260625-150049` (expr-join tree). Graded
`exhausted` — the agent retried the recursing query until it ran out of iterations.
**Severity:** HIGH. A valid-parsing query crashed the planner with an opaque interpreter error.

## Actual root cause (corrected)

The recursion was **not** in `_upstream_concepts` (that walk maxes out at depth 5 here). It is the
planner's node-generation loop: `_generate_aggregate_node → gen_group_node → _resolve_parent_sources
→ source_concepts → generate_node → _generate_aggregate_node …` re-entering for the SAME aggregate
`total` forever.

`total = sum(combined.gsales) by combined.ch, combined.eid`. The union output `combined.gsales`
(derivation `ROWSET`) is created at parse time with an **abstract** grain (union stack has no
narrower key than the full row). At build time, `Concept.get_select_grain_and_keys`
(`author.py:1223`) made an abstract-grain concept **inherit the consuming select's grain** — which
here is `{local.total}`. So `combined.gsales.grain == {local.total}` while `total.grain ==
{combined.ch, combined.eid}`: a cycle. `resolve_function_parent_concepts(total)` →
`_walk_aggregate_grain_inputs(combined.gsales)` (ROWSET branch) returns `gsales.grain.components ==
{local.total}`, so `total` becomes its own parent → the search loop re-plans `total` endlessly.

This is the "false grain association" class: a concept's grain points at an aggregate derived from
that very concept.

## Fix

`Concept.get_select_grain_and_keys` (`trilogy/core/models/author.py`): a `RowsetItem`-lineage
concept now keeps **its own** grain instead of inheriting the select grain. For a union that grain
is abstract, which is correct (the `UnionNode` is a UNION ALL stack; a concrete grain would also
wrongly inject a dedup `GROUP BY (cols)` — verified: it collapsed two identical `(k, v)` rows and
mis-summed). The originally-proposed parse-time grain change in `rowset_semantics.py` was tried and
**rejected** for exactly that over-grouping reason.

---

## Original (incorrect) analysis — kept for the record

## Symptom

```
RecursionError: maximum recursion depth exceeded
```

Stack (innermost frames):

```
concept_strategies_v3.py:105  search_concepts
concept_strategies_v3.py:515  _search_concepts -> get_loop_iteration_targets
discovery_utility.py:946      get_loop_iteration_targets -> get_priority_concept
discovery_utility.py:497      get_priority_concept:  all_upstream |= get_upstream_concepts(c)
discovery_utility.py:311      get_upstream_concepts -> _upstream_concepts(base, nested, {})
discovery_utility.py:329      _upstream_concepts:  for x in base.lineage.concept_arguments  <-- recurses unbounded
```

## Minimal repro / trigger isolation

The full failing query is `results/20260625-150049/workspace/query05.preql` (a `union(...)` of a
sales arm and a returns arm, then derived `case` concepts `channel_type`/`entity_id` over the
union outputs, projected with `by rollup` aggregates).

Toggling pieces against the run DB isolates the trigger to **a derived concept over a `union(...)`
output column, projected alongside an aggregate over union columns**:

```trilogy
import raw.all_sales as all_sales;
with combined as union(
  (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
   select all_sales.channel as ch, all_sales.channel_dim_text_id as eid, all_sales.ext_sales_price as gsales),
  (where all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date
   select all_sales.channel as ch, all_sales.return_channel_dim_text_id as eid, 0::float as gsales)
) -> (ch, eid, gsales);
```

| Variant | Result |
|---|---|
| A: `auto et <- case combined.ch ...` + `sum(combined.gsales) by rollup combined.ch, combined.eid` | **RECURSE** |
| B: select `combined.ch, combined.eid, sum(...) by rollup ...` (NO derived concept) | OK |
| C: `auto et <- case combined.ch ...` + `sum(...) by combined.ch, combined.eid` (plain `by`, no rollup) | **RECURSE** |
| D: `auto et <- concat('p', combined.eid)` + `sum(...) by rollup ...` | **RECURSE** |

So: **rollup is not required** (C recurses with a plain `by`); selecting the union columns
directly is fine (B); the trigger is the **derived `auto` concept (`case`/`concat`) reading a
`union(...)` output column**, then projected with an aggregate. Smallest reproducer is variant C.

## Root cause

`_upstream_concepts` (`discovery_utility.py:314`) memoizes a node **only after** fully expanding
its lineage:

```python
def _upstream_concepts(base, nested, cache):
    if nested:
        memoized = cache.get(id(base))
        if memoized is not None:
            return memoized           # breaks DIAMONDS (already-completed nodes)
    upstream = set()
    if nested: upstream.add(base.address)
    if base.lineage:
        for x in base.lineage.concept_arguments:
            ...
            upstream |= _upstream_concepts(x, True, cache)   # recurses
    if nested:
        cache[id(base)] = upstream    # memo written AFTER the loop
    return upstream
```

The memo guards diamonds but has **no "currently-visiting" guard**. A node still in-progress is
not yet in `cache`, so if the lineage contains a **cycle** (a concept reachable from itself
through the union-output / derived-concept chain), the walk re-enters the same node forever →
stack overflow. The derived-over-union-output shape (variants A/C/D) introduces exactly such a
lineage cycle; selecting the raw union columns (B) does not.

## Suggested fix (two layers)

1. **Make the walk cycle-safe (defense in depth):** add an in-progress/visiting set so
   `_upstream_concepts` returns a partial/empty set on re-entry instead of recursing. Even if a
   real cycle exists, the planner should never stack-overflow.
2. **Eliminate the actual cycle (the real bug):** the derived concept over a `union(...)` output
   column should resolve its lineage to the union's `derived_concepts` (as the ROWSET branch at
   `:335-337` already does for rowsets) rather than forming a self-referential edge. Determine
   why `case combined.ch` / `concat(combined.eid)` produces a cyclic lineage when
   `combined.ch`/`combined.eid` alone do not.

## Relation to prior notes

- `project_union_output_alias_self_reference` — a `union(...)` output aliased to its own aligned
  name produced a 2-cycle RecursionError, fixed in `function_to_concept` by rejecting an ALIAS
  whose source resolves to its own address. This q05 case is the **same family** (union-output
  lineage cycle → RecursionError) but via a *derived* concept (`case`/`concat`), not a direct
  self-alias — so that guard does not catch it.
- `project_derived_over_rowset_disconnected_bug` (q66) — a derived/arithmetic expr over a *rowset*
  output column produced a `DisconnectedConceptsException` (fixed). Same area (derived over a
  combined-source output column); the **union** analogue here fails differently (recursion).
- See also `evals/tpcds_agent/recursion_bug_handoff.md` (the pre-existing cross-model-aggregate
  recursion bug) — likely the same missing-cycle-guard in `_upstream_concepts`.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260625-150049/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('repro.preql').read())   # RecursionError
```
