# v4 P0 crash briefs

Seven genuine crashes (exceptions) surfaced by the v4-default sweep
(`TRILOGY_V4_DISCOVERY=1`). All pass under v3. Each brief is self-contained for a
fresh agent. Three earlier-suspected "crashes" (`bound_conversion_existence_presto`,
`aggregate_of_aggregate`, `rowset_alias_name_collision`, `in_subselect_with_inlined_datasource`)
turned out to be SQL-shape assertion diffs, not exceptions — they're tracked as
structure regressions in `tests/v4_known_failing.py`, not here.

| brief | test(s) | exception | status |
| --- | --- | --- | --- |
| [01](01_window_over_partition_invalid_ref.md) | `test_rank_by` | `ValueError: Invalid reference string` (INVALID_REFERENCE_BUG) | ✅ FIXED |
| [02](02_recursive_enrichment_invalid_ref.md) | `test_recursive_enrichment` | `ValueError: Invalid reference string` | ✅ FIXED |
| [03](03_subselect_correlation_invalid_ref.md) | `test_subselect_closest_warehouse` | `ValueError: Invalid reference string` | ✅ FIXED |
| [04](04_struct_in_array_unresolvable.md) | `test_struct_in_array_parsing`, `test_struct_in_array_item_access` | `UnresolvableQueryException` | ✅ FIXED |
| [05](05_rowset_window_unresolvable.md) | `test_rowset_shape` | `UnresolvableQueryException` | ✅ FIXED |
| [06](06_refresh_ambiguous_join.md) | `test_refresh_multiple_aggregate_persists_with_shared_count` | `AmbiguousRelationshipResolutionException` → later `UnresolvableQueryException` | ✅ FIXED |

**Remaining:** none — all six briefs are fixed.

Brief 02's fix: `_datasource_nodes_for_bridge` (`v4_helper/source_planning.py`) only
materialized real `ds~` datasource nodes and silently dropped the *derived*
connector the bridge identified (a recursive `recurse_edge` merged into a dimension
key — its lineage lives in `alias_origin_lookup`, not as a `ds~` node), leaving the
concept it provides unsourced (INVALID_REFERENCE). New `_derived_connector_nodes`
plans each needed connector's true origin (generic over any merged derivation, not
just RECURSIVE) and hands it back as a parent for `_merge_component_sources` to join
on the pseudonym equivalence. Two obstacles handled: (1) infinite re-entry — planning
a connector recurses to source its own inputs whose bridge re-routes through the same
connector; guarded via `V4History.connectors_in_progress`. (2) The connector must
emit its grain keys (`id`), not just the join key — else the consumer's column is
grouped away.

Brief 06's final fix: `_bridge_plan` (`v4_helper/source_planning.py`) treated a
bridged set that is a strict *subset* of the requested concepts (single-row/abstract
concepts like `data_through` are excluded from the bridge search) as if the bridge
had added a connector, routing the rest through one partial datasource and dropping
the complete-key merge. Now it only prefers the bridge when it adds concepts NOT in
the request (a real connector) or a union.

## Shared context: the `INVALID_REFERENCE_BUG` sentinel (briefs 01–03)

`BASE_INVALID = "INVALID_REFERENCE_BUG"` (`trilogy/dialect/base.py:236`) is emitted
by the renderer when a concept referenced by a CTE is not exposed as a column by
that CTE's source. `compile_statement` then hard-fails in strict mode:

```python
# trilogy/dialect/base.py:2314
if CONFIG.strict_mode and BASE_INVALID in final:
    raise ValueError(f"Invalid reference string found in query: {final}, ...")
```

So the ValueError is only a *guard* — the real bug is always upstream: a v4 node
did not pull a needed parent concept into the consuming node's `input_concepts`
(or didn't add the enrichment join that would source it). Briefs 01–03 are three
different node generators hitting this same class; they may share a root in
`parent_outputs_needed` / enrichment-join insertion, so coordinate if taken in
parallel.

## Workflow for each brief

1. Reproduce with the given command (`--runxfail` defeats the registry's xfail).
2. Confirm v3 passes (drop `TRILOGY_V4_DISCOVERY=1`).
3. Fix in the v4 path only (`trilogy/core/processing/v4_*`); v3 is the oracle.
4. Definition of done: test passes under **both** engines; remove its entry from
   `tests/v4_known_failing.py`; run the test's whole group under
   `TRILOGY_V4_DISCOVERY=1` to confirm no new XPASS/fail; `ruff`/`black`/`mypy trilogy`.
5. If the bug distills to a small standalone program, drop a `.preql` in
   `local_scripts/v4_evals/failing_cases/` (auto-tracked) → it moves to `cases/`
   once fixed.
