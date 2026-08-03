# RESOLVED 2026-07-27 (s41): both defects were in the `_connect`-removal
# rewrite, fixed in network_search.py.
#
# 1. Labelable-satisfier STALL (q5/q8, and the s40 order-dependent
#    test_where_clause_inputs): satisfiers were FIRST hops off the source, but
#    the discharge test needs the whole in-cover chain. An intermediate that
#    binds no terminal (q5's supplier) mints no labelable of its own once
#    chosen, so every satisfier was already in the cover, the state stranded,
#    and the enumeration emitted ZERO covers -> gen_root's fallback path (hides
#    grain keys on an intermediate merge) crashed downstream.
#    Fix: `_label_chain_state` returns the walk FRONTIER as satisfiers.
# 2. Row-partial chain hops (enum arm-spanning x3): the labeling walk extended
#    THROUGH row-partial candidates (enum arms, `partial datasource`), so a
#    channel-A-only table discharged labeling for all channels and the FK
#    union was reduced away. Fix: `_row_complete` — a row-partial candidate
#    may terminate a chain it fully binds, never extend one (applies to the
#    walk, the frontier, and `_functional_reach`).
#
# gcat test_aggregate_optimization was a third, separate item: the
# model_ambiguity validator correctly flagged the test's stale inline
# `fuel_aggregates` redeclaration (missing `stage_name: vehicle.stage.name`,
# which the real model binds); the test now co-locates it.
#
# Original handoff kept below for the record.

# Handoff: TPC-H q5/q8 (+ enum arm-spanning x3) failures — in-flight
# network_search rewrite, NOT the ambiguity work

## Current, definitive facts (2026-07-26 evening, working tree on
## v4_more_parity_work_three with the in-progress `_connect` rewrite)

These fail IN ISOLATION on the current tree:

- `tests/modeling/tpc_h/test_tpch_queries.py::test_five`
  `ValueError: Invalid input concepts to node! ['part.supplier.nation.id'] are
  missing non-hidden parent nodes; have {...} and hidden
  {'part.supplier.nation.id'}` (`trilogy/core/processing/nodes/base_node.py:283`)
  — also renders as `Could not render the query: Missing source reference to
  part.supplier.nation.region.name; ...` depending on route.
- `tests/modeling/tpc_h/test_tpch_queries.py::test_eight` — same
  `Invalid input concepts to node!` family.
- `tests/engine/test_enum_unions.py::test_enum_union_arm_spanning_multiple_sources_{row_grain,aggregated,in_tvf}`
  — wrong RESULTS (assertion on fetched rows), the "per-enum source search
  must recognize a mergeable set (ret_c |x| sale_c)" shape.

## Attribution (A/B'd, all on the same tree state)

The model-ambiguity landing (`trilogy/core/processing/model_ambiguity.py` +
its two integration edits) is EXONERATED for every one of these:

1. `validate_relation_paths` call toggled off in
   `source_planning._search_concepts_for_bridge` -> identical failures.
2. `search_sources` winner selection temporarily reverted to the original
   non-dominated + sort -> identical failures.
3. With BOTH disabled simultaneously -> q5/q8 still fail in isolation.

What changed besides the ambiguity work: `network_search.py` was rewritten
mid-session (working tree, uncommitted) — `_connect` became a pure
connectivity CHECK (bridge fabrication removed, `MAX_BRIDGE_ADDITIONS`
deleted), with multi-hop functional chains moved to a `labelable` obligation
via a new `_functional_reach`. The removed fabrication's own comment (earlier
tree state) warned: "a check-only version broke gcat (`test_array_agg`) and
`test_multi_join_assignments`, so fabrication IS load-bearing for suite
shapes outside the corpus." q5's symptom (a connector key
`part.supplier.nation.id` planned but landing hidden/unsourced) and the enum
arm-spanning mergeable-set shape both fit that removal.

## The earlier "order dependence" theory — DOWNGRADED to unconfirmed

Earlier today q5 appeared to pass alone but fail after the tpc_ds battery in
one process. That evidence is CONFOUNDED: the tree was being edited
concurrently between runs, and on the final tree q5 fails alone. Before
chasing cross-module state, re-run the alone-vs-after-battery A/B on a FIXED
tree. If it still reproduces there, prior suspects worth checking:
module-level caches in `trilogy/core/processing/**`, CONFIG mutations not
restored by battery tests, and the tpch module-scoped `engine` fixture
swapping `engine.environment` per test (stale id()-keyed plan state).

## Suggested next steps

1. Re-run `pytest tests/modeling/tpc_h/test_tpch_queries.py -q` at the
   `simplified_network` commit (0e3609640) vs the current working tree to
   bracket exactly which `_connect`/`_functional_reach` delta broke q5/q8.
2. Design the fabrication replacement against these five tests + gcat
   `test_array_agg` + `test_multi_join_assignments` (the shapes the old
   comment named), not against the TPC-DS corpus alone.
3. Only after that lands, re-test the order-dependence hypothesis (step in
   the section above).
