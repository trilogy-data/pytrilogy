# v4 aggregate-source-selection — ALL FIXED ✅ (handoff)

> **2026-06-08: all 6 remaining tests are now fixed.** Sub-problems 1–4 below are
> all resolved; the `_AGG_SOURCE` bucket in `tests/v4_known_failing.py` is empty.
> Fixes live in `v4_helper/source_planning.py` (`_plan_finer_filter_rollup`,
> `_plan_complete_where_source`, `_single_source_covers_completely` bridge guard)
> and `v4_helper/group_rules.py` (`partition_roots` SINGLE_ROW split). The
> sections below are retained for context.


> **Context (2026-06-08).** The original `_AGG_SOURCE` bucket (37 tests, see
> `agg_source_reuse_brief.md`) is down to 2. Exact-grain match, additive
> grand-total/coarser rollup (unfiltered), and partial-key dimension upgrade
> (unfiltered) all land. This pass added **group-level filtered rollup** and
> fixed `test_dimension_filter_with_aggregate`; see "What landed" below.
>
> **UPDATE (2026-06-08): sub-problems 1 and 2 below are now FIXED.** Both pin a
> datasource at the top of `plan_source` (`v4_helper/source_planning.py`):
> sub-problem 1 → `_plan_finer_filter_rollup`, sub-problem 2 →
> `_plan_complete_where_source`. The finer-filter gate in
> `_materialized_root_addresses` was removed (the `get_additive_rollup_concepts`
> conditions check already guards it). **Sub-problem 3 is also FIXED** — in
> `group_rules.partition_roots`, SINGLE_ROW roots are split into their own bucket
> so two grand-totals from different sources cross-join instead of forming one
> unsourceable bucket. Remaining: sub-problem 4.
>
> The 1 left is NOT a localized patch — it's a real v4 source-selection
> capability. The section is self-contained for a fresh agent.

## Shared model

Most repros use `tests/discovery/test_aggregate_resolution_coverage.py`'s
`SALES_MODEL` (base `orders` at `order_id` grain + pre-aggregated summary tables
`agg_by_customer`, `agg_by_date`, `agg_by_customer_date` binding
`order_count <- count(order_id)` / `total_amount <- sum(amount)` at their grains;
`customers`/`products` dim tables). `aggregate_testing.preql` and the flight
models in `test_aggregate_handling.py` / `test_primary_source_aggregate_fallback.py`
are variants. `SALES_MODEL_PRIMARY_ONLY` strips the summaries — the empirical
oracle: agg path must produce identical rows to the base path.

Reproduce any one with the env var + `--runxfail`:
```
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest --runxfail <nodeid>
```
v3 (drop the env var) is the correctness oracle for all of them.

## What landed this pass (don't redo)

`trilogy/core/processing/aggregate_rollup.py`:
- `filter_finer_row_args(conditions, target_grain, concepts)` — row-arg filter
  concepts NOT constant within a target-grain group (single-row / a grain
  component / a property whose keys ⊆ grain are EXCLUDED; everything finer is
  returned).
- `filter_is_group_level(...)` = `not filter_finer_row_args(...)`.

`trilogy/core/processing/concept_strategies_v4.py::_materialized_root_addresses`
rollup branch: the old gate was `if where is not None: continue` (blocked ALL
filtered rollups). Now `if not filter_is_group_level(where, target_grain, ...):
continue` — a **group-level** filter (e.g. `WHERE region` at `region` grain,
`region` functionally determined by `customer_id`) selects whole groups, so a
post-join filter on a coarser/exact aggregate is correct; only **finer** filters
are declined here.

**Why not just remove the gate:** doing so made
`test_aggregate_path_matches_primary_path[WHERE order_date … customer_id,
order_count]` return WRONG rows — source-planning sourced `order_count` from
`agg_by_customer` (the coarser exact table, can't express `order_date`) and
joined the *unfiltered* per-customer count to a separately-filtered customer
list. The group-level check is the correctness guard.

---

## Sub-problem 1 — finer-filter pre-aggregation rollup (3 tests) — ✅ FIXED 2026-06-08

> Fixed via `_plan_finer_filter_rollup` in `v4_helper/source_planning.py` +
> removal of the finer-filter gate in `_materialized_root_addresses`. The
> analysis below is retained for context.


`test_filter_on_grain_not_in_select`, `test_partial_key_upgrade_with_filter`
(both in `test_aggregate_resolution_coverage.py`),
`test_partial_precomputed_uses_aggregate_with_grain_filter`
(`test_primary_source_aggregate_fallback.py`).

**Query shape:** `WHERE order_date >= '2024-01-16' SELECT customer_id, order_count;`
Target grain `customer_id`; filter on `order_date`, a column FINER than the
target. The only correct source is `agg_by_customer_date` (grain
`customer_id, order_date`): push the filter pre-aggregation, then SUM-roll to
`customer_id`.

**v3 oracle SQL** (verified):
```sql
quizzical AS (SELECT customer_id, sum(order_count) AS order_count
              FROM agg_by_customer_date
              WHERE order_date >= date '2024-01-16' GROUP BY 1),
wakeful  AS (SELECT customer_id FROM customers)
SELECT wakeful.customer_id, quizzical.order_count
FROM wakeful INNER JOIN quizzical ON wakeful.customer_id = quizzical.customer_id
```
The `customers` INNER join completes the partial key (see below). Rows:
`[(101,1),(102,1),(103,1)]`.

**Why v4 gets it wrong today:** `order_count` is correctly identified as a
SUM-rollup candidate (signature match in `get_additive_rollup_concepts`), but
source-planning picks `agg_by_customer` (coarser, exact `customer` grain, binds
`order_count`) and applies the `order_date` filter on a SEPARATE `orders` scan
joined back — decoupling filter from aggregate → unfiltered count per customer.

**Three dead ends already explored (don't repeat):**

1. **Augment the demanded grain** (add `order_date` to `mandatory_list`, plan as
   exact match, then SUM-roll). FAILS: the aggregate's `canonical_address`
   encodes its `by` grain — `count(order_id) by customer` =
   `_virt_agg_count_1670…` ≠ `count(order_id) by customer,date` =
   `_virt_agg_count_1718…`. The requested concept is the customer-grain one;
   `agg_by_customer_date` binds the customer-date one, so exact-match (which keys
   on `canonical_address`) never fires at the augmented grain. `with_grain()`
   does NOT recompute `canonical_name`, so you can't cheaply re-grain it.

2. **`plan_source(outputs=[customer_id, order_count], conditions=order_date)`** —
   hoping the conditions-in-outputs rule forces a single source carrying all
   three. FAILS: the bridge planner still returns a MergeNode
   (`agg_by_customer` + `orders`-for-the-filter + `customers`), i.e. the same
   decoupling.

3. **Pinned single scan** — `plan_pinned_datasource_scan(agg_by_customer_date,
   [customer_id, order_count, order_date], where)` + `_group_to_grain_if_required`.
   Produces the CORRECT filtered + SUM-rolled CTE (matches `quizzical` above).
   But `agg_by_customer_date`'s `customer_id` is declared `~customer_id`
   (partial), so the top-level completeness check in
   `query_processor._get_query_node_v4` (`partial_requested`) raises
   `UnresolvableQueryException: … could only be resolved from partial sources`.
   A single pinned scan can't supply the INNER dim-table join that completes the
   key. NOTE: scan with the *registered* query concepts (matched to ds columns by
   ADDRESS), not `ds.output_concepts` — the latter's grain-specific canonical
   isn't in `environment.canonical_concepts` → KeyError.

**Why it's hard:** the correct plan is `scan(finer summary, filtered) INNER JOIN
dim(complete key) → SUM-roll`. The unfiltered partial-key upgrade
(`test_partial_key_upgrade_via_dimension_table`, PASSES) already builds the
`agg INNER JOIN customers → SUM-roll` shape — the gap is ONLY that under a finer
filter it sources the aggregate from `agg_by_customer` and decouples the filter,
instead of sourcing from `agg_by_customer_date` with the filter inline.

**Recommended direction:** make the existing materialized-root SUM-rollup path
prefer the filter-compatible finer table as the aggregate's source. Two angles:
- In `_materialized_root_addresses`, when a finer filter exists, pin the rollup
  to a datasource that (a) holds the target grain keys, (b) `_conditions_supported`,
  (c) `get_additive_rollup_concepts` covers all requested aggregates — and thread
  that choice into source-planning so the aggregate's group sources from THAT
  table with the filter, while the dim-upgrade join still runs. The thread is the
  missing piece: `materialized_roots` is a `frozenset[str]` of addresses with no
  way to pin a source.
- OR: make condition placement (`v4_helper/condition_placement.py`) able to land
  a finer filter on the rollup aggregate's group by having that group demand the
  filter columns at the finer (source) grain — so `_reachable_input` includes
  `order_date` and the atom places there, forcing `agg_by_customer_date`.

Helpers already present and correct: `get_additive_rollup_concepts` (validates the
dropped grain is functionally safe), `_group_to_grain_if_required` (the SUM-roll;
see `strategy_builder.py:1099`, `rollup_concepts`). `filter_finer_row_args`
identifies the finer columns.

**Done when:** all 3 pass under v4 AND v3, rows match, and
`test_aggregate_path_matches_primary_path[WHERE order_date …]` (the equivalence
guard, currently passing) stays correct.

---

## Sub-problem 2 — partial/`complete where` datasource match under filter (1 test) — ✅ FIXED 2026-06-08

> Fixed via `_plan_complete_where_source` in `v4_helper/source_planning.py`
> (pins the partial datasource whose `non_partial_for` is implied by the WHERE).
> The analysis below is retained for context.


`test_aggregates_comprehensive.py::test_high_value_customer_filter`.

**Query:** `WHERE customer_revenue > 100 SELECT customer_id, customer_revenue2,
customer_order_count;` (`customer_revenue2`/`customer_order_count` are
`sum(order_value)`/`count(order_id) by customer_id`). Model has:
```
partial datasource high_value_customers (customer_id, customer_revenue, customer_order_count)
grain (customer_id) complete where customer_revenue > 100  -- pre-filtered table
```
Expected: the planner picks `high_value_customers` because the query's `WHERE
customer_revenue > 100` MATCHES the datasource's `complete where` (a `non_partial_for`
condition), making the partial table "complete" for this query.

**Why hard:** v4 needs to recognise that an outer filter equal to (or implied by)
a partial datasource's `non_partial_for.conditional` upgrades that datasource to
complete for the query — v3's partial/complete-source machinery. v4's
`_conditions_supported` / source-policy `accept_partial` handle partials but
don't do this "filter satisfies the completeness predicate" match. Look at how
v3's `gen_select_node` / datasource selection consumes `non_partial_for` and
`condition_implies` (`trilogy/core/processing/condition_utility.py`).

This is filter-on-an-AGGREGATE (`customer_revenue > 100` is a HAVING-style
predicate at the target grain — group-level, NOT the finer-filter case). The
sibling `test_high_value_customer_filter_two` (filter written as
`sum(order_value) > 100`) is `@pytest.mark.skip` (needs canonical-type complete
detection) — out of scope.

**Done when:** picks `high_value_customers`, passes v4 + v3.

---

## Sub-problem 3 — grand-total signature match across namespaces (1 test) — ✅ FIXED 2026-06-08

> Fixed in `group_rules.partition_roots`: SINGLE_ROW roots are pulled out of the
> reach/union/bailout and given their own `grp:root:root:∅:single_row` bucket,
> so a grand-total precomputed source and a different-source grand-total
> cross-join at FINAL instead of forming one unsourceable root bucket. The
> analysis below is retained for context.


`test_aggregate_handling.py::test_combine_grand_total_with_joined_namespace_count`.

**Query:** `SELECT flight_count, count(carrier_name) AS named_carriers;` — a
grand-total `flight_count` (count over the local key, served by precomputed
`flight_count_total` grain-`<*>` table) combined in ONE select with a count over
a joined-namespace property (`carrier_name` from the `carrier` dim). Both are
grand totals (no group by).

Expected: `flight_count` resolves from `flight_count_total` (don't recompute
`count(id)` over raw `flight`); `named_carriers` from `carrier`; both in one SQL.

**Why hard:** the docstring names the root: "the resolver pruned every aggregate
datasource because `with_materialized_source` strips lineage, so signature-based
matching failed." In v4, when two grand-total aggregates from different
namespaces coexist, the abstract-grain (`<*>`) precomputed source for
`flight_count` isn't being matched/kept. Likely interacts with the single-row /
abstract-grain handling in `source_planning._resolve_bridge_graph`
(`Granularity.SINGLE_ROW` concepts are excluded from the bridge search) and the
grand-total branch of `get_additive_rollup_concepts` (`not target_grain.components`).
Trace why `flight_count_total` isn't selected as the source for `flight_count`
when `named_carriers` is also requested (isolate: it likely works for
`flight_count` alone — confirm, then find what the second aggregate perturbs).

**Done when:** both aggregates present, `flight_count_total` used, v4 + v3 pass.

---

## Sub-problem 4 — prefer summary's dimension column for recomputed non-additive (1 test) — ✅ FIXED 2026-06-08

> Fixed via `_single_source_covers_completely` guard in `_bridge_plan`
> (`source_planning.py`): when one datasource binds every requested concept as a
> non-partial output, the bridge bails so `_direct_source`'s grain-aware scoring
> picks the exact-grain summary instead of the bridge routing dims through base
> via a spurious `id` connector. The analysis below is retained for context.


`test_aggregate_handling.py::test_partial_aggregate_rollup_rejects_unsupported_aggregates`.

Two asserts; the FIRST already passes (an `avg_distance` query correctly does NOT
reuse the finer summary — non-additive). The FAILING one:

**Query:** `SELECT origin_code, flight_date, distinct_destinations;`
(`distinct_destinations <- count_distinct(destination_code)`) at grain
`(origin_code, flight_date)`. Summary `flight_count_by_source_dest_date` is at
`(origin_code, destination_code, flight_date)` and HAS `destination_code` as a
grain key.

Expected: recompute `count(distinct flight_count_by_source_dest_date.destination_code)`
grouped to `(origin_code, flight_date)` — i.e. SCAN THE SUMMARY for its
`destination_code` column (it's finer and carries the value), NOT base `flight`.
The summary's own precomputed `distinct_destinations` column must NOT be used
(can't roll up a distinct count by sum). v4 currently scans base `flight`.

**Why hard:** pure source-preference — when a non-additive aggregate must be
recomputed, v4 should still prefer a finer summary table that carries the needed
dimension column over the base fact (cheaper, and v3 does it). This is the
"source-preference for non-additive" item (#3) in `agg_source_reuse_brief.md`.
The rollup machinery deliberately refuses to reuse the precomputed
non-additive column (correct); the gap is that it then falls all the way back to
base instead of scanning the summary's grain columns. Likely a source-cost /
candidate-ranking tweak in `source_planning` so a finer summary covering the
dimension wins over base when the aggregate is recomputed either way.

**Done when:** `count(distinct "flight_count_by_source_dest_date"."destination_code")`
appears, the precomputed `distinct_destinations` column is not used, v4 + v3 pass.

---

## Workflow / definition of done (all)

1. Fix in the v4 path only (`trilogy/core/processing/v4_*`); v3 is the oracle.
2. Test passes under BOTH engines; remove its entry from `tests/v4_known_failing.py`.
3. Run the test's whole group under `TRILOGY_V4_DISCOVERY=1` for no new XPASS/fail,
   then a broad sweep (`discovery optimization complex persistence engine modeling`,
   `-m "not adventureworks_execution"`; the ~82 clickhouse.cloud errors are
   environmental). `ruff check . --fix; mypy trilogy; black .`.
4. Prefer fixing at the source-selection layer generically — sub-problems 1, 3,
   4 are all "v4 picks base / a coarser table instead of the right summary," so a
   shared improvement to candidate selection may clear several at once.
