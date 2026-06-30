# BUG: derived-expression join key between two rowsets fails to resolve when the non-anchor measure is re-aggregated

**Classification:** framework bug (planner limitation). LOUD — raises `DisconnectedConceptsException`, not silent wrong rows. Drives heavy agent thrash on TPC-DS **q02** (1.4M–2M tokens/run; pass_rate ~0.6).

**Status:** FIXED 2026-06-30. The rowset enrichment now materializes a
derived-expression scoped join key (`a.grp + 1`) off the rowset's own output and
merges over its pseudonym. R1/R5 now resolve correctly; R2–R6 unchanged. See
`trilogy/core/processing/node_generators/rowset_node.py`
(`_producible_derived_join_keys` / `_enrich_via_derived_join_key`),
`tests/generators/test_rowset_derived_join_key_enrichment.py`, and the
`cross_rowset_grouped_aggregate_offset_join` cell in
`tests/test_rowset_generation_matrix.py`.

---

## Symptom

The language *should* execute this, and does not:

```trilogy
import orders as orders;
with a as select orders.grp as grp, sum(orders.amt) as tot where orders.status = 1;
with b as select orders.grp as grp, sum(orders.amt) as tot where orders.status = 2;

select a.grp, sum(a.tot) as at, sum(b.tot) as bt
inner join a.grp + 1 = b.grp;
```

```
DisconnectedConceptsException: Discovery error: couldn't source all these concepts
into one query; you may need a join or merge to relate them across models.
Sourced individually but not joinable from model: {a.grp, b.tot}
```

Two rowsets joined on a **derived-expression equality key** (`a.grp + 1 = b.grp`), where the
final select **re-aggregates** the non-anchor rowset's measure (`sum(b.tot)`) to the anchor's
output grain (`a.grp`). This is a legitimate "compare period N to period N+k" shape.

## Minimal self-contained repro

No eval workspace needed — uses an in-memory model. Run:

```
.venv/Scripts/python.exe evals/tpcds_agent/repro_derived_rowset_join.py
```

(script committed alongside this file). It builds the `orders` model from
`tests/test_rowset_offset_join_contract.py` and runs the trigger matrix below.

## Trigger matrix (the diagnostic part)

Model: `orders(oid, status, amt, grp)`. Two rowsets `a`,`b` each `sum(orders.amt) as tot`
grouped by `orders.grp`, filtered to different statuses.

| # | final select | join clause | result |
|---|---|---|---|
| R1 | `a.grp, sum(a.tot), sum(b.tot)` | `a.grp + 1 = b.grp` | ❌ disconnect |
| R2 | `a.grp, a.tot, b.tot` (plain, no re-agg) | `a.grp + 1 = b.grp` | ✅ |
| R3 | `a.grp, sum(a.tot), sum(b.tot)` | `a.grp = b.grp + 1` (derived on **b** side) | ✅ |
| R4 | `a.grp, b.grp, sum(a.tot), sum(b.tot)` (**keep b.grp**) | `a.grp + 1 = b.grp` | ✅ |
| R5 | `a.grp, a.tot, sum(b.tot)` (only b re-agg) | `a.grp + 1 = b.grp` | ❌ disconnect |
| R6 | `a.grp, sum(a.tot), sum(b.tot)` | `a.grp = b.grp` (**plain equality**) | ✅ |

**Failure requires ALL of:** (a) the non-anchor rowset's measure is re-aggregated to the
anchor's grain (R2 plain-projection passes), AND (b) the join key's derived expression sits on
the **anchor / output-grain** side (R3 flips it → passes), AND (c) the join is a
derived expression, not plain equality (R6 passes). Materializing the non-anchor key as a live
output (R4) sidesteps it.

The existing passing contract `tests/test_rowset_offset_join_contract.py` is exactly R2 (plain
projection) — which is why the derived-offset join was believed covered. Re-aggregation is the
uncovered case.

## TPC-DS q02 (the real-world driver)

q02 asks for per-week-day sales ratios vs the next year (`week_seq + 53`). Agents reach for the
SQL self-join idiom — two grouped CTEs `curr`/`future` joined on
`curr.week_seq + 53 = future.week_seq and curr.day_of_week = future.day_of_week`, with filtered
ratio aggregates `sum(curr.sales_amt ? curr.day_of_week=dow) / sum(future.sales_amt ? future.day_of_week=dow)`.
This is the same shape: derived key on the anchor side + re-aggregated non-anchor measure.
q02 has the extra wrinkle that its only *direct-equality* bridge (`day_of_week`) is consumed
**only inside the filtered aggregate**, so it is not a live bridging node either → even the
orientation flip (`curr.week_seq = future.week_seq - 53`) does NOT save q02 (verified).

The canonical hand-authored answer (`tests/modeling/tpc_ds_duckdb/query02.preql`) avoids the
whole thing with a window lag — `lead(amt, 53) over (order by week_seq)` — which resolves
cleanly. That is the idiomatic Trilogy form, but the engine should still resolve the self-join
form rather than dead-ending the agent.

## Root cause

The error message comes from the **diagnostic**, but the real failure is upstream: in
`source_query_concepts` (`trilogy/core/processing/concept_strategies_v3.py:650-704`),
`search_concepts(...)` returns `None` (no plan found). Only THEN is
`disconnected_components(...)` called to format a friendly error.

Note there are **two** raise sites for this exception. The repro's traceback fires from the
*inner* one — `get_priority_concept` (`discovery_utility.py:522`), reached while
`gen_enrichment_node` (`node_generators/common.py:524`) tries to source the aggregate's parent +
join keys for the GROUP node. That confirms the failure is in **aggregate-grain parent
sourcing across the explicit join** (`gen_group_node` → `_resolve_parent_sources` →
`_source_parent_concepts`), not in the top-level diagnostic.

Two cooperating issues:

1. **Planner (the real failure).** When `sum(b.tot)` must be produced at output grain `a.grp`,
   the planner needs a concept *on the b side* whose value equals `a.grp` so it can group b's
   measure by it. The authored join derives the key on the **anchor** side
   (`a.grp + 1`, a `_virt_func_add` keyed off `a.grp`); the b side carries only raw `b.grp`.
   The planner cannot invert `a.grp + 1 = b.grp` into "group b by (b.grp − 1) = a.grp", so it
   can't source `b.tot` at the `a.grp` grain → no plan. When the derived expr is on the b side
   (R3) the b side *does* carry a column equal to `a.grp`, so grouping works; when `b.grp` is a
   live output (R4) the grain is `{a.grp, b.grp}` and no inversion is needed.
   Look at how the scoped join key concept is built/required (`join_resolution.py`,
   `build.py`/`build_environment.py` scoped-join-key registries) and how `search_concepts`
   sources an aggregate's grain parents across an explicit join.

2. **Diagnostic (why the error says "disconnected").** `_island_rowsets`
   (`trilogy/core/processing/discovery_utility.py:588-666`) severs all cross-rowset edges and
   reconnects across rowsets **only via pseudonyms** (step 3, lines 658-666). A direct-equality
   join key (`a.grp = b.grp`) yields a pseudonym/merge → an edge; a **derived-expression** key
   (`a.grp + 1 = b.grp`) lowers to a `_virt_func_add` and produces **no pseudonym → no edge**.
   So the rowsets look disconnected. Additionally the grain-only-consumer skip
   (lines 650-655) drops the edge for a measure reached only as an aggregate's grouping parent.

A fix likely needs (1) the planner to treat a derived-expression equality join key as
bidirectional when sourcing the non-anchor measure at the anchor grain (or to materialize the
inverted key on the non-anchor side), and (2) `_island_rowsets` to add a connectivity edge
between two rowsets whenever an explicit join clause references concepts from both (even when
the key is a derived expression), so the diagnostic stops false-reporting disconnection.

## Guardrails (must not regress)

- `tests/test_rowset_offset_join_contract.py` — derived offset join, plain projection (R2). The
  join must stay offset-only, never welded on the shared base key.
- Scoped-join correctness memories: q29 (nullable inner-join not widened to full), q78
  (left/inner multi-partial anchor), q21 (HAVING finer-aggregate fan-out). See `scoped_inner_join_keys`,
  `scoped_left_anchor_keys`, `scoped_full_join_keys` registries.
- R2–R6 above must keep passing; only R1/R5 should flip to ✅.
- A genuinely unrelatable cross-rowset query must still raise a clean DisconnectedConcepts.

## Agent workarounds (until fixed)

- Idiomatic: window lag — `lead(metric, k) over (order by seq)` — no self-join (canonical q02).
- Put the derived expression on the looked-up side: `a.grp = b.grp + 1` not `a.grp + 1 = b.grp`
  (works for the simple case; NOT sufficient for q02's filter-hidden bridge).
- Keep the non-anchor join key as a live output (`select ..., b.grp, ...`).

## File pointers

- `trilogy/core/processing/concept_strategies_v3.py:650` — `source_query_concepts` (failure + diagnostic).
- `trilogy/core/processing/discovery_utility.py:588` — `_island_rowsets` (pseudonym-only cross-rowset reconnect).
- `trilogy/core/processing/join_resolution.py` — scoped join key resolution.
- `tests/test_rowset_offset_join_contract.py` — the passing-R2 contract / model to extend.
- `tests/modeling/tpc_ds_duckdb/query02.preql` — canonical window form.
