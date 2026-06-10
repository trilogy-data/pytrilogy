# v4 C1/C4 — derived-merge-key fan-out: FIXED (v4-scoped, 2026-06-09)

Parity case (passing, in `cases/`):
`local_scripts/v4_evals/cases/merge_into_key_fanout.preql`.

## Symptom

A `merge X into ~Y` join worked in v4 only when the merge key was a directly-bound
column; it broke when the key was DERIVED (`r_last <- split(full_name)[2]`). v4
emitted `LEFT OUTER JOIN ... on 1=1` (cross-join fan-out — demo test_merge_basic
26730 vs 17) + `INVALID_REFERENCE_BUG as full_name`.

## Root cause (two coupled bugs)

`SELECT p_name, full_name` over `passengers`(p_id) + `rich`(full_name), merged on
`p_last ≡ r_last`. Both roots fall into one ROOT group; `plan_source` bridges the
two datasources.

1. **Graph `.datasources` registry desync.** The Steiner walk
   (`determine_induced_minimal_nodes`) reaches `full_name` via the merge key's
   reverse-lineage, so `ds~rich` isn't in the minimal tree; the post-pass re-adds
   the edge but `ReferenceGraph.subgraph` already rebuilt `.datasources` from the
   Steiner nodes, so `ds~rich` is a graph NODE but not in the registry.
   `_datasource_nodes_for_bridge` iterates the registry → never scans rich.
2. **`.address` vs `.canonical_address` mismatch.** A derived concept's graph
   node uses its `.canonical_address` (`_virt_func_*`), but `bridge_addresses` is
   keyed by `.address`, so the derived merge key isn't collected onto its scan →
   no shared key → ON 1=1.

## The fix — v4-scoped, in `source_planning.py` only (v3 untouched)

Earlier attempt mutated the shared Steiner helper `determine_induced_minimal_nodes`;
that reached the recursive connector's internal graphs and broke
`test_recursive_enrichment` (correct→crash). The landed fix lives entirely in the
v4 bridge materialization, on the bridge's PRIVATE graph copy:

- `_datasource_nodes_for_bridge`: before the scan loop, re-point any `ds~` graph
  node missing from `plan.graph.datasources` from the full source graph — GATED
  two ways: (a) **gap-only** — register only if the source uniquely provides a
  bridge concept no registered datasource covers (else over-sources a
  union/semijoin bridge → regressed `partial_union_bridge_semijoin`); (b) stand
  down entirely when the bridge involves a **non-BASIC merge** (recursive /
  aggregate origin in `alias_origin_lookup`), since that key is supplied by
  `_derived_connector_nodes` (else strands recursive enrichment).
- `_local_concept_nodes_for_datasource`: also match a neighbour by
  `canonical_concepts[address].address`, restricted to a **BASIC-derived merge
  key** (carries pseudonyms, derivation BASIC) — so the inline-computable key
  lands on its scan while recursive keys and plain derived concepts (window/cast)
  are left alone.

Result: both scans derive their key (`split(...)`) and join `INNER JOIN ... ON
p_last = r_last`. Repro v3=2, v4=2.

## Validation

- Parity cases: 21 passed (repro promoted to `cases/`); `partial_union_bridge_semijoin`
  clean; `test_recursive_enrichment`, `test_rank_by` pass.
- v3: untouched (only `source_planning.py` changed — v3 uses the Steiner helper,
  which is back at baseline).
- Wide v4 sweep: wins = repro + gcat2 `test_refresh` + gcat `test_no_duplicates`
  (removed from known_failing). No correct→crash regressions.

## Cross-namespace merge render bug — also FIXED (inline-datasource guard)

The bridge fix advanced CROSS-NAMESPACE merges (`merge rich_info.last_name into
~passenger.last_name`) from a silent ON 1=1 cross-join to a real keyed join, which
then surfaced a SEPARATE downstream bug in the **inline-datasource optimization**:
`DatasourceCTE.consumer_column` `assert alias is not None` (execute.py:1423). The
merge key on the far side is DERIVED (`split(rich_info.full_name)`), so when the
optimizer folds the `rich_info` raw table into the consumer, the join's ON clause
references a `passenger_last_name` column the raw table doesn't have. (Confirmed
by toggling `CONFIG.optimizations.datasource_inlining = False` → renders correctly:
`split(rich_info.Name)[-1] as passenger_last_name` on the rich side.)

Fix (INLINE-PRESERVING — keeps the natural single-table query): the projection
path already renders cross-namespace merge keys correctly — `render_concept_sql`
tries the concept's PSEUDONYMS as render candidates, so `passenger.last_name`
falls back to `rich_info.last_name` (lineage `split(full_name)`). The join-key
path didn't. Two changes route it through the same derivation:
- `trilogy/core/models/execute.py` `DatasourceCTE.consumer_column`: when the
  concept isn't a raw column, find a pseudonym among `self.output_columns` whose
  own lineage IS local to this datasource (`rich_info.last_name <-
  split(rich_info.full_name)`) and return that lineage.
- `trilogy/dialect/common.py` `render_join_concept`: render the returned
  expression (`BuildFunction`/`BuildAggregateWrapper`) rather than the merged
  concept (whose lineage points at the other namespace).
Result: `... INNER JOIN quizzical ON STRING_SPLIT("rich_info"."Name",' ')[-1] =
"quizzical"."passenger_last_name"` with `rich_info` inlined directly — no extra
CTE. Affects v3 + v4 (shared render path). (An earlier guard in
`inline_datasource.py` that DECLINED the fold was reverted in favour of this,
since inlining yields more natural SQL.)

Cleared by this: `test_cast_merge`, hackernews `test_adhoc06`, demo `test_merge`,
`test_merge_basic`, `test_demo_merge_rowset_e2e` (all removed from known_failing).

## C4 — unresolvable query raised ValueError, not UnresolvableQueryException — FIXED

ncaa `adhoc02` is a NEGATIVE test: it selects `team.color` + `sum(game_tall.win)`
from two UNCONNECTED namespaces (no join key), so it should raise
`UnresolvableQueryException` (v3 does). v4 instead built a degenerate node — a
parent-less `ConstantNode`/`GroupNode` whose outputs (`team.color`, ...) had no
source — and only failed at the final render with `ValueError: Invalid reference`.

Fix: `strategy_builder.build_strategy_node` — after assembling the final node,
`_has_unsourced_leaf` walks the tree; if any parent-less node with no
`.datasource` outputs a ROOT-derivation concept (a base column that MUST come
from a datasource), the plan is unresolvable → return None →
`UnresolvableQueryException`. ROOT-derivation is the right discriminator:
unnest-of-literal / constant leaves output only derived concepts, so they're left
alone (an earlier granularity-based check wrongly flagged the 5
`*_unnest_*` parity cases). v4-only (strategy_builder); v3 unaffected.

Still failing (SEPARATE bug): demo `test_demo_e2e` (NoDatasourceException —
passenger.cabin, the C3 persist-of-unnest reuse: after the raw source is deleted,
`split_cabin` should come from the persisted table but v4 re-derives it from the
deleted `cabin`; `_materialized_root_addresses` deliberately excludes UNNEST).
