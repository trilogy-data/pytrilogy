# Handoff: port coalescing-axis + per-side probe materialization to v4

> **STATUS 2026-07-12 (session 2): LARGELY DONE.** 13/23 family cells + 20
> collateral fixed; v4 sweep 177 → 145 with 0 new failures (v3 5403/0). See
> the session-2 entry at the top of `v4_audit.md` for the mechanism map and
> the ~10 residual rowset-flavored cells (cast/concat derived keys, bare
> rowset-axis projection, filtered_rowset_anchor, mixed anchors, subset
> property null test) folded into the next rowset-pair key-carry session.

Target for the first post-#598 parity session. Scope: one mechanism, worth
~20–25 correctness failures out of the ~177 tracked in `v4_audit.md` (07-12
section). Do this before the rowset key-carry family — it is fully
root-caused and the fix surface is known.

## The failing families (all wrong ROWS, not shape)

```bash
TRILOGY_V4_DISCOVERY=1 pytest \
  tests/join_matrix/test_coalescing_presence_matrix.py \
  tests/join_matrix/test_subset_presence_probe.py \
  tests/join_matrix/test_root_presence_probe.py \
  tests/join_matrix/test_filtered_rowset_anchor.py -q
```

Canonical repro (q97 shape, `test_union_presence_counts`): two fact tables,
`union join s_cust = c_cust`, presence CASE counts per channel. Expected
(1, 1, 1); v4 returns **(0, 0, 2)** — it plans the ENTIRE query from one
member's scan (catalog only), so the axis has 2 rows instead of the 3-row
union, and both per-side probes render identically as
`coalesce("catalog_fact"."c")` (never NULL).

## Root cause (verified 2026-07-12)

Two v3 mechanisms in `node_generators/presence_probe.py` have no v4
counterpart in the group graph:

1. **`gen_coalescing_axis_node`**: a `full`/`union` key group's unified axis
   is the union of member domains — it must materialize as a FULL merge of
   EVERY member's own side, coalesced. v4's walker sees the canonical key as
   an ordinary ROOT sourced from whichever single scan scores best.
   v3 triggers this only at the two sites that are ABOUT the axis: a bare
   axis projection, and a presence probe's key (an `is null` answer lives on
   the complement side). Do NOT make it a blanket invariant — a query touching
   only one side's own attributes stays single-sourced (v3 docstring covers
   this).
2. **Per-side probe pinning through the group graph**: the simple ROOT-member
   cells were fixed in #598 (`_datasource_renders_probe` in
   `v4_helper/source_planning.py`) — but that gate only bites when the plan
   goes through the BRIDGE (`_datasource_nodes_for_bridge`). In the q97 shape
   the group graph collapses everything into one root group and no bridge is
   built, so the gate never runs. The probe atoms need to form their OWN
   groups pinned to `member_binding_datasources(member, env)` BEFORE group
   fusion, with the coalescing axis as the join spine.

## Landed groundwork you can build on (#598)

- `_datasource_renders_probe` (source_planning.py): side-identity gate;
  reuse its resolution pattern — **graph nodes carry the probe's
  `_virt_func_coalesce_<hash>` canonical address, NOT `_virt_presence_*`;
  always resolve via `environment.canonical_concepts[address]` before any
  prefix check.**
- Probe `canonical_name` is now the probe's own name
  (`Factory._build_concept`, gated on PRESENCE_PROBE_PREFIX), so
  canonical-space passes can no longer fuse the two sides of one key group.
- Side-identity helpers in `node_generators/presence_probe.py`:
  `is_presence_probe`, `probe_member_address` (name-hash → member),
  `member_binding_datasources` (column `origin_address` match; off-grain
  FK-carrier bindings first — see its docstring for the q84 dimension-vs-fact
  subtlety, locked by `test_subset_member_dimension_and_fact_bindings_probe_fact`).

## Where to intervene in v4

The group pipeline is `build_concept_graph` → `build_group_graph` →
`build_strategy_node` (concept_strategies_v4.`_build_from_graph`). The axis
and probe decisions are GROUPING decisions, so they belong in the first two
stages, not in the generators (`gen_basic` receives already-decided parents —
too late; verified dead end).

Suggested shape: during concept-graph build, when a node's resolved concept
is a presence probe (or a bare projection of a coalescing axis —
`coalescing_axis_group(address, env)` in presence_probe.py tells you), tag it
so `build_group_graph` buckets it per member side with the member's binding
datasource pinned, and inject the axis group (FULL merge of member sides) as
the spine they hang off. The v3 target SQL for every cell is cheap to print —
run the repro with `CONFIG.use_v4_discovery = False` and mimic that shape
(per-side CTE + probe column, FULL-joined on the axis, null tests against the
per-side columns).

`_conditions_prove_non_null` carve-out matters for parity: when the statement
WHERE proves the probe non-null, v3 SKIPS the complement side and renders an
INNER join (see `test_subset_side_not_null_intersects`'s v3 plan). Without it
you'll pass rows but regress TPC-DS size ceilings.

## Verification protocol (non-negotiable, from prior burns)

- Execute ROWS for every touched cell; never trust shape-only green
  (memory: "cosmetic bucket was mostly real regressions").
- Run suspect tests in ISOLATION as well as in-suite (contamination masks).
- Full gates before calling it done, NEVER concurrently (duckdb hard-crashed
  the interpreter under two parallel TPC-DS suites):
  ```bash
  pytest tests -m "not adventureworks_execution" --ignore=tests/engine/test_clickhouse_server.py -q
  TRILOGY_V4_DISCOVERY=1 pytest tests -m "not adventureworks_execution" --ignore=tests/engine/test_clickhouse_server.py -q
  ```
  Baselines at #598 merge: v3 **0 relevant fails / 5402 passed**; v4 **177
  failed / 5154 passed**. Any v4 count above 177, or any v3 fail, is a
  regression you introduced. TPC-H q22 and
  `test_root_filter_can_use_aggregate_side_parent` are the canaries this
  area's changes broke last time — run them early.
- `ruff check . --fix && mypy trilogy && black .` at the end.

## After this family

Next sessions in priority order (details in `v4_audit.md` 07-12 section):
rowset-pair scoped-join key-carry (~30, consumption/two_source matrices);
mixed aggregate+row `?` filter CASE re-render (4, needs multi-parent
`_filter_guaranteed_by_sole_parent` or bare-content rewrite); cograin
crashers (2); materialized-aggregate bridge fan-out (4); narrowing through
LIMITed chains; TPC-DS q14/q64/q81.
