# q05: drop the dead sales-fact anchor on return-only projections over `all_sales`

Companion to `handoff_q05_returns_arm_full_sales_join_perf.md` (the symptom: a return-only
arm joins the whole `store_sales` fact → 375 s/arm → agent timeout). **Implemented and green.**

## What it does

A query over the unified `all_sales` model that references only return-side concepts kept
`store_sales` as the grain anchor — it uniquely supplied `item.id`/`order_id` as *complete*
keys, so the planner joined the 2.88 M-row sales fact and `RIGHT OUTER JOIN`ed the returns
onto it, contributing no selected column. The fix recognizes that a WHERE condition on a
returns-only field (`return_channel_dim_id is not null`) is an **implicit `complete where`**:
it proves every surviving row is a returns row, so `store_returns`'s `~` (intrinsic-partial)
grain keys already cover the whole result population and the sales anchor is dead weight.

We do **not** elide the key from the request (it's legitimately needed to source the measures
at `(item, order, channel)` grain). We recognize `store_returns` as a *satisfactory / complete
-for-this-query source* for those keys; the redundant `store_sales` then falls out of source
selection via the ordinary subset rule.

## Root cause (v3 planner — the live path; `use_v4_discovery=False`)

Grain `(item.id, order_id, channel)` has two same-grain sibling facts: `store_sales_unified`
(keys **complete**) and `store_returns_unified` (keys are `~item.id`/`~order_id`,
`column_level_partial`). A `~` key means the source covers only a subset of the key's
universe (return rows), so it's judged an *incomplete* provider and `store_sales` gets pulled
in to anchor the full grain. Two places enforce that judgment, and both had to learn the
membership exception:

1. **Source selection** — `resolve_subgraphs` compares *non-partial* requested concepts; the
   `~` keys are excluded from `store_returns`'s non-partial set, so `store_sales` isn't seen
   as a subset and survives as the FROM anchor.
2. **Node output** — even once `store_returns` is chosen, `create_datasource_node` stamps
   `~order_id` **partial** on the emitted scan, so the discovery loop sees the key come back
   partial and **re-sources it** from a sibling (the double-source that produced the residual
   join in the `test_right_only_padded_column_proof_still_upgrades` shape).

## The fix — one completeness proof, two call sites

`membership_complete_grain_keys(ds, datasources, conditions)` in `source_scoring.py` returns
the `~` grain keys of `ds` that the WHERE proves complete for this result. Non-empty only when
**all** hold (so every other query is untouched):

- The keys are `ds`'s **`~` grain keys** (`column_level_partial ∩ grain`) — not an arbitrary
  partial attribute. *(Guards a real dimension like `stores.store_id` beside a `~store_id`
  fact from being wrongly completed — the `rowset_outer_addition` regression.)*
- A **same-grain sibling supplies those keys non-structurally** (there is an anchor we'd
  otherwise pull in). With no such sibling the key is genuinely partial and stays partial.
  *(Guards the single-source case — `test_column_level_partial_survives_condition_implies`.)*
- **Membership proof:** `condition_proves_non_null(WHERE)` names a concept `ds` provides that
  its same-grain siblings do NOT. Every surviving row is then a `ds` row — sibling rows are
  filtered out — so the keys cover the whole population. *(Without this the base join stays
  FULL and sibling-only rows form a NULL group; dropping the sibling would change the result.
  This is the correctness gate, not a heuristic.)*

Wired into both partiality surfaces:

- `get_graph_partial_nodes` / `get_graph_partial_canonical` subtract the completed keys →
  `store_sales` is dropped by the existing subset rule (uniformly, every sub-graph).
- `create_datasource_node` subtracts them from `partial_concepts` (only when `partial_is_full`
  is False; a satisfied `complete where` already lifts everything) → the key comes back
  **complete**, so the discovery loop doesn't re-source it. This is what deletes the residual
  join — no output-grain plumbing, no per-shape guard.

## Why there is no "output-grain gate"

An earlier draft added a third gate ("don't fire when a completed key is a query output")
because a bare-key-output query (`select order_id, count(item_id) where _ret_order = 1`) grew
a `LEFT OUTER JOIN`. The real cause was narrower: `order_id` came back **partial** from the
returns scan, so discovery sourced it a *second* time (once with the condition → anchor
dropped, once condition-free as a filter base → anchor kept), and the two diverged. Fixing
partiality at the **node output** (call site 2) makes the key come back complete the first
time, so it's never re-sourced. The double-source — and the whole need for an output-grain
signal — disappears. That query now plans as a single `SELECT o, count(i) FROM returns WHERE
ro = 1 GROUP BY o`.

## Validation

- q05 return-only arm (run workspace `results/20260707-151529`): `store_sales` scan gone →
  `FROM store_returns INNER JOIN date LEFT JOIN store`. **0.06 s vs 375 s**, 6 rows,
  byte-identical to a hand-written `store_returns`-only ground truth.
- `where _ret_order = 1 select order_id, count(item_id)` → single `returns` scan, no join,
  rows `{(1,1)}`.
- Suites green: `tests/{core/processing,generators,optimization,engine}`,
  `test_partial_datasource.py`, `complex/test_dataset_merge.py` (**1268 passed**); full
  `tests/modeling/` incl. `tpc_ds_duckdb` (**pass**). ruff / mypy / black clean.

## Scope / non-goals

- **Per-channel arms only.** q05's arms filter to one channel → single-channel non-union
  sources (`store_returns_unified`). A channel-less return-only aggregate routes through the
  `BuildUnionDatasource` path (`_structural_partial_concepts` returns `[]` for unions today) —
  **not** covered; follow-up.
- **Correctness is by result-preservation:** the completed keys cover the whole result
  population (membership proof), the dropped sibling contributes no selected column, and its
  join can't change cardinality — output is identical to today's (slow) plan.
- q05 itself was already handled at the node-output layer by the pre-existing `partial_is_full`
  (its `store_returns` has `complete where channel='STORE'`); the source-selection change is
  what drops the anchor. The node-output change matters for sources **without** a `complete
  where` (the `_ret_order` shape).
- v4 planner (`use_v4_discovery=True`, off in evals): analogous seam is
  `v4_helper/source_planning.py` (`_datasource_nodes_for_bridge` / `_datasource_grain_concept_nodes`).
  Out of scope for the v3 fix; note for parity.

## Tests to add before landing (regression-first)

New `tests/engine/test_duckdb_return_only_anchor_elision.py`, each asserting SQL shape + rows:

1. **Fires** — return-only aggregate over a two-fact unified model (`~` keys on returns, a
   return dim, filter `return_dim_id is not null`, `sum(return_amount)` by the dim): no `sales`
   scan / no OUTER join to it; rows == returns-only ground truth. (direct q05 guard)
2. **Doesn't fire — sales measure referenced** (`+ sum(ext_sales_price)`): sales scan present.
3. **Doesn't fire — no membership proof** (`sum(return_amount) where channel='STORE'`, no
   returns-exclusive not-null): anchor retained (FULL-join NULL-group behavior preserved).
4. **Bare-key output collapses to one scan** (`select order_id, count(item_id) where
   _ret_order = 1`): single fact scan, no OUTER join. (generalizes
   `test_right_only_padded_column_proof_still_upgrades`, kept green)
5. **Dimension not dropped** (`~store_id` fact beside a complete `stores` dim): dim survives.
   (the `rowset_outer_addition` shape)

Plus a unit test for `membership_complete_grain_keys` covering each negative gate (non-grain
key, no sibling, no membership proof) → empty set. Full sweep + `ruff/mypy/black` before land.

Acceptance: q05 scores **pass** and the return-only arm runs in a few seconds (also unblocks
`project_float32_union_placeholder_drift_no_double_type`).
