# Handoff: why the compiler never selects `fact_agg_*` — and how to fix it

> **RESOLVED 2026-08-12.** Fixes 1-3 below landed (plus two latent bugs the work
> exposed). Regression coverage: `tests/discovery/test_aggregate_rollup_matching.py`
> — every cell asserts both "the summary IS used" and "the answer still matches
> the raw fact", because this is an optimization that otherwise degrades
> silently. Gates: tpc_ds + tpc_h corpora 132/132 byte-identical, full suite
> green. See §Outcome at the end. Item 4 (size-aware tie-break) and item 5
> (sum-linearity, per-measure non-null counts) are still open.

**2026-08-12.** Full record of the datasource-selection investigation summarized
in [`handoff_noise_crossover.md`](handoff_noise_crossover.md) §4. Verified
against the preserved run workspace
`results/20260811-145002_enriched_aggregates/workspace/` (probes confirmed the
working tree, Trilogy 0.3.324).

**Design intent (decided 2026-08-12):** the aggregate bindings stay
underscore-hidden from agent exploration. That is the point of the
experiment — aggregates are private implementation details the compiler is
free to use. The fix therefore has to land in the **engine's matching**, not
in exposing named metrics to the agent (that workaround is recorded below for
completeness, but it is not the goal).

## TL;DR

Across 40 saved enriched candidates (two runs), zero compiled to a
`fact_agg_*` datasource. Classification:

- **Not (a) a binding defect.** The bindings are fully functional: a query
  that names the bound concept (`ss._warehouse_sum_sales_price`) compiles to
  a correct `SUM(sum_sales_price) … GROUP BY date_sk` scan of
  `fact_agg_store_sales_daily`, including coarser-grain rollup and dimension
  joins through the grain keys.
- **(b) the planner never considers the aggregate** for agent-authored inline
  aggregates (`sum(ss.sales_price) as total_sales`) — the dominant failure,
  hit by every candidate.
- **(c) a tie-break loss** for pure key-existence subqueries, where the
  aggregate IS enumerated but scores worse than the raw fact.
- **(d) principled non-matches** for 6 of the 19 candidates (row-level
  filters, measure products, count-distinct grains) — correctly not servable.

## Repro matrix

Four-way split in the eval workspace:

| Query | Compiles to |
|---|---|
| `select ss.sale_date.sk, sum(ss.sales_price) as total_sales` | raw `fact_store_sales` |
| `select ss.sale_date.sk, ss._warehouse_sum_sales_price` | **`fact_agg_store_sales_daily`**, correct rollup |
| inline `sum(...)` at the aggregate's exact 8-key grain | raw (even exact grain fails) |
| all 18 compilable saved candidates (q08 needs a parameter) | raw facts, zero agg hits |

Synthetic namespace matrix (small model: `orders` fact + `agg_by_customer`
summary binding a named `total_amount <- sum(amount)`):

| Case | Chosen |
|---|---|
| same-namespace, named metric, exact or coarser grain | **agg** |
| same-namespace, inline alias, exact grain | **agg** (canonical-hash bridge works) |
| same-namespace, inline alias, coarser grain | raw |
| imported namespace (`import m as o`), inline alias, exact grain | raw |
| imported namespace, named metric | **agg** |

The eval hits all three failing conditions at once — inline aliases +
imported namespaces (`ss.`/`cs.`/`ws.`) + coarser-than-agg target grains —
but any one alone suffices to miss.

## Mechanism (code paths)

1. **The capable matcher exists but is unreachable.**
   `trilogy/core/processing/aggregate_rollup.py:34-61`
   (`_aggregate_signature` / `_datasource_has_matching_additive_aggregate`)
   matches by *(operator, sorted canonical arg addresses)*. Probed directly,
   `local.total_sales` and the bound `ss._warehouse_sum_sales_price` hash to
   the same `(SUM, ('ss.sales_price',))`, and `get_additive_rollup_concepts`
   (same file, :192-251) returns the query concept as rollable from
   `_warehouse_store_sales_daily`.

2. **The rollup entry gate is address-based and runs first.**
   `trilogy/core/processing/concept_strategies_v4.py:320-321`:
   `if concept.address not in {c.address for c in ds.output_concepts}:
   continue`. An agent-authored alias (`local.total_ext_sales_price`) is never
   an output address of a `_warehouse_*` datasource, so the ROLLUP
   root-marking branch never fires. The comment at :306-308 documents the
   address assumption as intentional ("a finer-grain table binds the same
   named concept").

3. **The canonical bridge fails on namespace prefix + grain-abstract
   registration.** `canonical_address = f"{namespace}.{canonical_name}"`
   (`trilogy/core/models/build.py:1336`); the hash *content*
   (`_canonical_str_for_hash`, build.py:4723-4743) is namespace-safe (args
   fully qualified), but the prefix is the authoring namespace: the query
   alias canonicalizes to `local._virt_agg_sum_<hash>` while the ds column is
   `ss._virt_agg_sum_<hash>` — identical hash, different prefix, no match.
   Separately, `materialized_canonical_concepts`
   (`trilogy/core/models/build_environment.py:261-295`) intersects ds-column
   canonicals with *environment-concept* canonicals; the environment-side
   `_warehouse_sum_*` concepts are grain-**abstract** while the ds-bound
   copies are grain-**pinned**, so the set contains zero virt-agg entries and
   the EXACT branch (concept_strategies_v4.py:292) can never fire for any
   aggregate in this workspace.

4. **Root-marking alone is insufficient (verified by monkeypatch A/B).**
   Patching `_materialized_root_addresses` to add signature-matched
   aggregates fired correctly (`sig-match ds=_warehouse_store_sales_daily,
   rolled=['local.total_sales']`) but the plan still scanned raw: group-graph
   coverage is address-keyed
   (`datasource_columns = frozenset(c.address …)`,
   concept_strategies_v4.py:355-358) and source planning's
   `renders_materialized_canonical` requires the ds to physically bind the
   requested concept's canonical address
   (`trilogy/core/processing/v4_helper/source_planning.py:993-999`). Every
   downstream layer must see the aggregate as *binding the queried concept*.

5. **Existence subqueries (c): enumerated but out-scored.** For q10-style
   `customer.sk in (fact filtered by date)`, both raw and agg bind the needed
   keys by address, so the agg is a candidate — but `score_datasource_node`
   (`trilogy/core/processing/v4_helper/source_scoring.py:351-376`) ranks by
   `(mat_score, grain_score, …)` where `grain_score` counts grain components
   *not requested*: the raw fact's 2-key grain beats the aggregate's 8-key
   grain, and `get_materialization_score` (:168) knows address *type*, not
   table size. No notion of row count/cost exists anywhere in selection.

## Servable-candidate ceiling (19 saved candidates; there is no query15)

- **Strictly servable** as-is, had matching worked (pure sum/count of bound
  measures, filters/groupings reachable from the 8 grain keys): **9** —
  q01 (store_returns agg), q02, q03, q06, q08, q10 (existence-only), q12,
  q19, q20.
- **+2 with a linearity rewrite** (`sum(a−b+c) → sum_a−sum_b+sum_c`, all
  columns bound): q04, q11 → **11**.
- **Partial / near-miss**: q05 (sales arms servable; web-returns
  channel-attribution exceeds the agg), q07 (avg-blocked — aggs lack
  per-measure non-null counts, `avg = sum / count(non-null)`) → theoretical
  ceiling **~12-13**.
- **Principled non-matches (d): 6** — q09, q13 (row-level measure-value band
  filters), q14 (`sum(quantity*list_price)` measure product + order-grain
  counts), q16 (order-number-grain rowsets / count_distinct), q17 (stddev +
  ticket-level sale↔return pairing), q18 (avgs + `group() by order_number`).

So roughly **half to two-thirds** of real candidates are aggregate-servable;
the observed 0/40 is entirely the matching gap, not workload shape. This is
also the ceiling for the new `agg used n/20` funnel metric once matching works.

## Fix design (engine, ranked; no code changed yet)

Target invariant: **an agent-authored inline aggregate that is
lineage-equivalent to a hidden binding compiles to the aggregate table**,
with the agent never learning the binding exists.

1. **Namespace-free canonical identity for `_virt_*` concepts.** Compare
   `canonical_name` (the lineage+grain hash — already namespace-safe in
   content) instead of `canonical_address`, or register virt canonicals under
   a prefix-free key. Fixes the imported-namespace EXACT case outright and is
   the smallest change with the widest coverage.
2. **Grain-pinned canonical registration.** Populate
   `materialized_canonical_concepts` from the ds-column concepts' own
   (grain-pinned) canonicals rather than intersecting with abstract-grain
   environment concepts (`build_environment.py:269-282`). Required for the
   EXACT branch to ever see aggregate bindings.
3. **Alias-as-pseudonym rewrite at rollup root-marking.** Extend the gate at
   `concept_strategies_v4.py:320` from address membership to the
   `aggregate_rollup` signature match — and when it fires, register the query
   alias as a **pseudonym of the bound concept address** via the existing
   alias/pseudonym machinery, so every downstream address-keyed layer
   (group-graph coverage, `renders_materialized_canonical`, rendering) sees
   the aggregate as binding the requested concept. This is the
   teach-one-layer alternative the monkeypatch proved necessary; without it,
   fixes 1-2 only cover exact-grain queries.
4. **Size-aware tie-break** for the existence subclass: a materialization
   preference or declared row-count hint in `score_datasource_node`, so an
   8-key aggregate can beat a 2-key raw fact when both merely provide keys.
   Independent of 1-3 and lower priority — it changes plans for existing
   models, so it needs a corpus A/B with the byte-diff harness.
5. **Optional follow-ups** once 1-3 land: sum-linearity rewrite (+2 queries),
   per-measure non-null counts in the agg DDL to unlock `avg` (+1).

Guardrails when implementing: `tests/join_matrix` gets cells first
(established rule), corpus A/B byte-diff to prove zero plan changes outside
aggregate-bound models, and count rule firings — a green suite says nothing
about a matcher that never fires.

*(Recorded for completeness: exposing the metrics as named concepts makes
everything work today with zero engine changes — the min2 repro proves it —
but it surrenders the hidden-implementation-detail claim, so it is explicitly
not the chosen path.)*

## Outcome (2026-08-12)

The target invariant holds: an inline `sum(ss.price) as total` compiles to the
summary table, in any namespace, at exact / coarser / grand-total grain, with
the binding never exposed to the agent.

What actually shipped, against the plan above:

1. **Namespace-free canonical identity** — `canonical_address_for`
   (`models/build.py`): a `_virt_*` canonical name is a lineage hash over
   fully-qualified args, so it is already globally unique and the authoring
   namespace must not re-partition it. `canonical_address_grain` follows, so
   nested virt args unify too. Fixed the imported-namespace EXACT case.
2. **Grain-pinned registration was NOT needed.** The diagnosis was right that
   `materialized_canonical_concepts` holds no virt-agg entries at *abstract*
   grain, but the environment is rebuilt per select, so the query's own
   grain-pinned alias is registered and the EXACT branch does fire.
3. **Signature matching, not pseudonyms.** The alias-as-pseudonym rewrite was
   not required. Instead the *lineage signature* — `(operator, sorted canonical
   arg addresses)`, now `BuildConcept.additive_aggregate_signature` — replaced
   the address gate at each address-keyed layer the monkeypatch predicted:
   - `_materialized_root_addresses` rollup gate (`concept_strategies_v4.py`)
   - reference-graph candidacy (`env_processor.additive_rollup_edges`) — without
     this the summary is never even a candidate source
   - the v4 bridge (`source_planning._datasource_rolls_up_to`), plus a
     canonical-keyed bridge test: an alias and an identically-derived named
     metric collide in `canonical_concepts` and the winner is arbitrary
   - aggregate pruning (`graph_models.prune_sources_for_aggregates`)
   - scan outputs / inputs (`datasource_nodes.create_datasource_node`)
   - column resolution (`BuildDatasource.rollup_column_for` /
     `aggregate_column_for`), which is what makes the renderer able to emit
     `sum("summary"."total_x")` for a column it shares no name with. The old
     "the renderer can't emit it" justification for the address gates is gone.

Two latent bugs surfaced (both could mis-plan for *named* metrics too, they were
just unreachable behind the address gates):

- `get_additive_rollup_concepts` never checked the datasource could supply the
  **target** grain — only that its own dropped grain was safe. A customer-grain
  summary was therefore a legal rollup source for a per-product query. Fixed by
  `_addresses_reachable` (shared with `_conditions_supported`).
- `create_datasource_node` dropped an aggregate whose target grain is spelled as
  a *property* of the table's grain key (`carrier.name` over a `carrier.code`
  summary) — same metric, third distinct canonical.

Measured effects beyond the repro matrix: `tests/discovery` now serves
`sum(order_value) ... where order_date > X` from the customer/date summary
(filtered pre-aggregation, SUM-rolled) instead of the raw orders scan, and the
README quickstart's `count(id) by carrier.name` now reads
`flight_count_by_carrier` — identical numbers to the named metric.

**Known remaining gap**: only a *bare* aggregate is matched. `sum(x) + 0` (and
therefore `sum(a) - sum(b)`) demands the enclosing BASIC and the search never
looks inside its lineage — the same thing item 5's linearity rewrite would need.
Pinned by `test_aggregate_inside_an_expression_is_a_known_gap`.

## Verification harness

Scratch probes from the investigation (session scratchpad, repo untouched):
`compile_probe.py` (parse a query file against the workspace env, print
selected physical tables), `min1-4.preql` (the repro matrix),
`probe_env.py`/`probe_env2.py` (canonical-address probes), `patch_probe.py`
(root-marking monkeypatch A/B), `ns_probe.py` (namespace matrix). Rebuild as
needed — each is ~20 lines against
`results/20260811-145002_enriched_aggregates/workspace/`. The eval-side
acceptance metric already exists: `used_aggregate` in `report.json` /
`agg used` in the funnel, which reads compiled datasource selection without
exposing anything to the agent.
