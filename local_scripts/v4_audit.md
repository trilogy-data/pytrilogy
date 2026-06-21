# v4 compatibility audit (last refreshed 2026-06-21)

This file is the current handoff for v4 discovery work. Long fixed-case notes
from the 2026-06-11 through 2026-06-20 sweeps were removed because they were
stale and obscured the remaining work. The authoritative skip list is
`tests/v4_known_failing.py`; reclassify with `python local_scripts/v4_classify.py`
when changing planner behavior.

## Current tracked state

`tests/v4_known_failing.py` currently tracks 48 non-strict xfail entries:

| bucket | n | meaning |
| --- | ---: | --- |
| INLINE | 11 | SQL/CTE shape differs from v3, rows expected to match |
| RESULT | 2 | known wrong-row regressions with distilled repro coverage |
| MODELING | 17 | modeling sweep diffs still needing per-test classification |
| TPCDS_SIZE | 18 | TPC-DS rows match, SQL exceeds v3-tuned size ceilings |

Known result-regression entries:

- `tests/stdlib/test_report.py::test_top_x_by_metric`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_seven_two`

High-priority unclassified modeling entries from the previous audit, still worth
checking before cosmetic shape work:

- `tests/engine/test_duckdb.py::test_composite_rollup_aggregate_keeps_group_by`
- `tests/modeling/geography/test_landmark_updates.py::test_exact_match_resolution`
- `tests/modeling/geography/test_landmark_updates.py::test_exact_match_with_parenthetical_extra_filter`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_sixty_eight`

## Phase boundary contract

The intended v4 path is:

1. Source concept demand: `concept_graph.py` walks mandatory output concepts and
   condition inputs back to root concepts, preserving row-vs-existence dataflow
   with typed edges. This phase should not pick concrete datasources or build
   `StrategyNode`s.
2. Group concepts: `group_graph.py` applies per-derivation grouping rules,
   injects conditions at groups, computes group IO, and appends the FINAL sink.
   This phase should decide which concepts can be sourced together, but should
   not render or materialize query nodes.
3. Materialize groups: `strategy_builder.py` walks the group DAG and dispatches
   each group to `v4_node_generators`. ROOT groups call `source_planning.py` for
   datasource selection, bridge planning, partial completion, and pinned rollup
   sourcing. This phase may project/dedup parents to preserve grain, but should
   not re-partition the conceptual groups.
4. Zip final query: `_assemble_final_node` merges the minimum built contributors
   that cover the mandatory outputs, applies final-only filters, hides join keys
   that were carried only for assembly, and dedups to the requested output grain.
   Optimization happens after this planner returns a normal strategy node.

## Separation audit notes

No obvious phase-breaking runtime issue was found in the current code read. The
notable sharp edges are:

- `source_planning.py` is the right home for concrete datasource selection. Its
  bridge and partial-completion helpers are sourcing concerns, not grouping
  concerns.
- `_regraft_group_sources` in `group_graph.py` is topology-only: it may add a
  better existing parent edge or a synthetic dimension ROOT bucket, but it must
  not call `plan_source`, inspect datasources, or build `StrategyNode`s.
- `_project_dimension_parents_to_group_grain`, `_wrap_for_grain`, and
  `_group_to_grain_if_required` in `strategy_builder.py` are materialization
  safeguards. They protect cardinality while assembling nodes; they should not
  become alternative grouping rules.
- Existence edges must stay side-channel-only. They order subselect sources but
  should not be treated as row-stream JOIN parents.
- Rowset, recursive, aggregate, window, and group-to concepts remain row-shape
  barriers. Fixes should preserve them as materialized boundaries rather than
  decomposing their roots into another phase.

## Next cleanup loop

1. Re-run `local_scripts/v4_classify.py` under `TRILOGY_V4_DISCOVERY=1`.
2. Promote any current xpasses that pass in isolation.
3. Split `_MODELING` entries into `_RESULT`, `_INLINE`, or `_TPCDS_SIZE`.
4. Tackle the two known `_RESULT` entries before the shape backlog.
