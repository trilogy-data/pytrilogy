# Bug: `union(...)` output under ROLLUP + cross-model HAVING → RecursionError (crash)

**Status:** CRASH FIXED 2026-06-12 (clean error now); full build still blocked by
the sibling `_virt_msl_*` union-lineage bug (see below).
**Severity: HIGH** — a `RecursionError` blows the Python stack (crash class, like
the q05 union crash). Reconfirmed reproducing on the current engine; a
prior "cross-model-aggregate recursion" fix (`recursion_bug_handoff.md`) does NOT
cover this `union(...)`-TVF shape.

## ROOT CAUSE (2026-06-12)

A **self-referential output alias**, not a planner cycle. The rowset-with form
`with channel_data as union(...) -> (channel, ...)` registers the union's aligned
output `channel` in the **bare local namespace** (`local.channel`); the rowset
re-exposes it as `channel_data.channel` with lineage `Rowset(content=local.channel)`.
The outer select then writes `channel_data.channel as channel`, which **redefines**
`local.channel` as `alias(channel_data.channel)`. Now `local.channel` →
`channel_data.channel` → `local.channel`: a 2-cycle.

Build-time recursion guards can't catch it — planning each union arm calls
`get_query_node` → `materialize_for_select`, which rebuilds the *whole*
environment under a **fresh factory stack**, so the per-factory `_building`
in-progress set never sees the repeat; it just recurses (fresh arm-plan → rebuild
env → rebuild `channel_data.channel` → re-plan union → …) until the Python stack
blows.

## FIX (crash → clean error)

`trilogy/parsing/common.py`: `function_to_concept` now rejects an `ALIAS`
(rename) whose target address the aliased source already resolves back to
(through a rowset wrapper or a further rename) — helper `_alias_target_cycles`.
Raises `InvalidSyntaxException` ("...refers back to itself. Use a distinct output
name e.g. 'channel_out'."). Regression test:
`tests/engine/test_duckdb_rowset.py::test_tvf_union_output_alias_self_reference_errors`.

Renaming the outer aliases away from the union's aligned names (`as out_channel`)
clears the collision but then hits the **separate** `_virt_msl_*`
`UndefinedConceptException` (the same union-rollup-HAVING lineage bug noted below /
shared with q05 `bug_union_tvf_upstream_map_crash.md`) — so the query still does
not fully build. That deeper resolution is the remaining open work.

## Symptom

`trilogy run` / `generate_sql` raises:

```
RecursionError: Recursion error building concept channel_data.channel
  with grain Grain<Abstract> and lineage <Rowset<channel_data...>>
```

`channel_data.channel` is the union's first output column (the per-arm constant
`'store'`/`'catalog'`/`'web'`). Building it recurses without termination.

## Repro (deterministic)

`evals/_repros/q14_union_rowset_recursion.preql`:

```python
import sys; sys.path.insert(0, 'evals'); from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260612-133004_ingest/workspace')   # any raw-model ws
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(Path('evals/_repros/q14_union_rowset_recursion.preql').read_text())
# -> RecursionError: Recursion error building concept channel_data.channel ...
```

## Shape that triggers it

1. `merge ss.item.item_sk into ~cs.item.item_sk` (+ ws) — share `item` across the
   three channel facts.
2. `with channel_data as union( arm_store, arm_catalog, arm_web )` — each arm is a
   per-channel aggregate that emits a **constant channel literal** (`'store' as ch`)
   plus the item brand/class/category keys and `sum`/`count` measures; aligned to
   `-> (channel, brand_id, class_id, category_id, total_sales, total_number_sales)`.
3. Outer select consumes the union and `sum(...) by rollup channel, brand_id,
   class_id, category_id`, with `having total_sales > overall_avg_sale` where
   `overall_avg_sale` is a **global cross-model** aggregate over the same three
   facts (projected hidden via `--overall_avg_sale`).

The recursion is in concept-build for the union output `channel` under this
ROLLUP + cross-model-HAVING combination.

## Same family as the other union/multiselect failures

This is one of a cluster the rebaseline surfaced on the `union(...)` TVF:
- `bug_union_tvf_upstream_map_crash.md` — q05: `find_source` "Could not find
  upstream map for multiselect" (SyntaxError) rendering a union output re-consumed
  by an outer CASE/aggregate.
- q14 reductions of THIS query also hit `UndefinedConceptException: Concept
  '_virt_msl_… not found in environment` — another union/multiselect-lineage
  failure mode.

Likely a shared root cause in how `BuildUnionSelectLineage` / multiselect lineage
resolves its output concepts when consumed by an outer aggregate (rollup) and/or a
cross-model HAVING. Worth fixing together.

## Minimization notes

Could NOT reduce to a clean minimal case (same finicky behavior as q05): a 2-arm
`union(...)` + rollup is fine; adding the global-avg HAVING flips it to the
`_virt_msl_*` undefined error, not the recursion. The recursion needs the full
3-arm + 4-key-rollup + merge + cross-model-HAVING shape — use the saved fixture.

## Two things to fix

1. **Never crash.** A `RecursionError` must never surface — at minimum bound the
   concept-build recursion and raise a clean, actionable error.
2. **The plan should build.** This is a valid query (per-channel union, rollup,
   filter vs a global average). Resolve the union output concept (`channel`) under
   rollup + cross-model HAVING without recursing.

## Context

Found in run `results/20260612-180707_ingest` (q14), NOT polluted by in-flight
edits. Related machinery: `BuildUnionSelectLineage`, `Derivation.TVF_UNION`,
`UnionNode`, `build.py` concept-build; prior (insufficient) fix
`recursion_bug_handoff.md`.
