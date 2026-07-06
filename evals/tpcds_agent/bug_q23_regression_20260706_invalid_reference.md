# q23 token explosion (3.7M) — INVALID_REFERENCE_BUG + Ambiguous-reference: CAUSATION = PRE-EXISTING (NOT a regression from the 4 candidate changes)

Run: `evals/tpcds_agent/results/20260706-023449` (q23 FAIL, 3,695,645 tokens).
Prior runs: 655k (20260705-200535) → 581k (20260706-001731) → **3.7M** (this run). All FAIL.

## Symptoms (two distinct framework defects, both hard failures)

The agent's q23 uses a chain of rowsets: `cust_totals` (per-customer `sum(ss.quantity*ss.sales_price)`),
then an outer rowset/select that filters customers whose total exceeds `0.5 * max(total)`, then a
catalog+web `union(...)`. Two independent framework bugs fire depending on how the agent phrases the
"total vs 0.5*max" filter:

### Bug 1 — `INVALID_REFERENCE_BUG<Missing source reference to ss.quantity / ss.sales_price>`
Preql (from jsonl write idx 31, → run result idx 35):
```
rowset cust_totals <- ... select ss.customer.id as cust_id, sum(ss.quantity*ss.sales_price) as cust_total;
rowset best_cust <-
select cust_totals.cust_id as cust_id, max(cust_totals.cust_total) as overall_max,
having cust_totals.cust_total > 0.5 * overall_max;
```
Generated CTE (`macho`) sources only the rowset outputs (`cust_id`, `overall_max`) yet its WHERE still
carries the raw base lineage of the rowset measure:
```
WHERE INVALID_REFERENCE_BUG<Missing source reference to ss.quantity>
    * INVALID_REFERENCE_BUG<Missing source reference to ss.sales_price> > 0.5 * "..."."_best_cust_overall_max"
```
→ `ValueError: Could not render the query`. INVALID_REFERENCE_BUG is a hard sentinel the engine must never emit.

### Bug 2 — `BinderException: Ambiguous reference to table "<cte>" (duplicate alias)`
Preql (write idx 40 / 70, → results 44 / 74). When the agent instead materializes the comparison as
one parent rowset and filters it:
```
rowset best_cust_filter <- select cust_totals.cust_id as cust_id, cust_totals.cust_total as cust_total, max_val.overall_max as overall_max;
... where best_cust_filter.cust_total > 0.5 * best_cust_filter.overall_max select best_cust_filter.cust_id;
```
The filter CTE self-joins its single parent to itself **without aliasing**:
```
FROM "friendly" INNER JOIN "friendly" on "friendly"."..cust_id" is not distinct from "friendly"."..cust_id"
```
→ DuckDB BinderException at execute. (Renders; fails at bind.)

## Minimal repro (deterministic, generate_sql only, no full eval)
Harness: `make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')` over `.../20260706-023449/workspace`.
Also reproduces against the **canonical committed model** `tests/modeling/tpc_ds_duckdb/store_sales.preql`
(so not workspace- or is_returned-specific).

Bug 1 minimal:
```
import raw.store_sales as ss;
rowset cust_totals <- where ss.date.year>=2000
  select ss.customer.id as cust_id, sum(ss.quantity*ss.sales_price) as cust_total;
rowset best_cust <- select cust_totals.cust_id as cust_id, max(cust_totals.cust_total) as overall_max,
  having cust_totals.cust_total > 0.5 * overall_max;
select best_cust.cust_id;
```
Bug 2 minimal (outer rowset NOT required — plain select reproduces):
```
... (cust_totals + max_val + best_cust_filter rowsets as above) ...
where best_cust_filter.cust_total > 0.5 * best_cust_filter.overall_max
select best_cust_filter.cust_id as cust_id;
```

## Trigger matrix (Bug 1)
| variant | result |
|---|---|
| outer **rowset** selects `max(inner.measure)` + HAVING refs raw inner aggregate-measure | **INVALID_REFERENCE_BUG** |
| same but **plain select** (no outer `rowset` wrapper) | OK |
| select `measure` + `max(measure)` together, no HAVING | OK |
| HAVING `measure > max(measure)` (no named `overall_max` output) | OK |
| HAVING `measure > literal` | OK |
| aggregate is `sum(measure)` instead of `max(measure)` | OK |
| inner rowset measure is a plain column (not an aggregate expr) | OK |

Trigger = outer **rowset** + a named aggregate output over the inner rowset's aggregate-measure + a HAVING
that references that same inner aggregate-measure raw.

Trigger matrix (Bug 2): WHERE/filter comparing **two columns of the same parent rowset**
(`a.x > k*a.y`) → spurious unaliased self-join. Single-column filter (`a.x > literal`) is OK.

## CAUSATION VERDICT: PRE-EXISTING — none of the 4 candidate changes is responsible

Proven, not asserted:

1. **Feature-absence.** Both minimal repros contain **no `union`, no `CAST`, no `decimal`, no `double`** in
   the generated SQL (verified programmatically). So `union_arm_cast_target` and the `DataType.DOUBLE`/
   `decimal` cast-alias changes are off the codepath. `find_nullable_concepts` `.get()` guard is defensive
   (cannot create errors). q23 does not use `is_returned`; store_sales rework irrelevant (repro uses only
   `quantity`, `sales_price`, `customer.id`, `date.year`).

2. **A/B revert of the only in-window planner change.** The sole commit that landed **between** the 581k
   run (20260706-001731 = 2026-07-06 00:17 UTC) and the 3.7M run (023449 = 02:34 UTC) is
   `60f628897 more_fixes` (2026-07-05 21:57 -0400 = 2026-07-06 01:57 UTC). Its one substantive planner
   change is a rewrite of `_resolve_condition_disposition` in `trilogy/core/processing/discovery_utility.py`
   (adds `_rowset_sourced` + rowset-remainder injection). Monkeypatch-reverting that function to its
   `9214ca660` (parent) body and re-running **both** minimal repros: **both still fail identically**
   (INVALID + Ambiguous). Therefore this commit did not introduce either bug. (`DataType.DOUBLE` was that
   commit's only base.py edit — 1 line — confirming the cast machinery is unrelated.)

3. **Why 581k → 3.7M then?** Not a framework delta — the **agent wrote different preql**. These two shapes
   (rowset HAVING over another rowset's aggregate-measure; WHERE comparing two columns of one parent rowset)
   are in the long-documented cross-rowset HAVING/scalar family (see MEMORY: `rollup_having_crossrowset`,
   `crossrowset_inner_join_grainless_scalar`, `q23_bare_aggregate_in_filter`). The agent only stumbled into
   these exact forms this run, then thrashed against the hard sentinels for 3.7M tokens.

4. **Canonical guard intact.** `pytest -k twenty_three` → 1 passed. The framework limitation is only hit by
   these specific author shapes, not the canonical query23.

## Root cause (file:line)
- **Bug 1 render symptom:** `trilogy/dialect/base.py:1335` — `render_expr` emits
  `INVALID_REFERENCE_STRING("Missing source reference to {c.address}")` when a concept has no `source_map`
  entry. **Planner root:** when an outer rowset's HAVING/WHERE references a rowset aggregate-measure
  (`cust_totals.cust_total`, derivation ROWSET, lineage `sum(ss.quantity*ss.sales_price)`), the condition
  is carried into the outer node with its **base lineage** (`ss.quantity`, `ss.sales_price`) rather than
  bound to the precomputed rowset output column; the outer node's source_map exposes only rowset outputs,
  so the base concepts are unsourced. Condition-sourcing decision lives in
  `discovery_utility.py::_resolve_condition_disposition` (~line 1040) / `get_inputs_that_require_pushdown`,
  but reverting the recent edit does not fix it — the measure→base-lineage expansion in the condition is
  upstream and pre-existing.
- **Bug 2:** spurious unaliased self-join generated in `trilogy/core/processing/join_resolution.py`
  (`get_node_joins`) when a filter node's condition draws two columns from a single upstream rowset CTE:
  the shared-canonical-key inference emits `INNER JOIN <cte> ... is not distinct from` against the same CTE
  with no distinct alias.

## Recommendation
Do NOT attribute to the 20260706 changes. File as pre-existing cross-rowset condition-sourcing defects:
(1) bind a rowset aggregate-measure referenced in an outer HAVING/WHERE to its rowset output column instead
of re-expanding its base lineage; (2) suppress / alias the degenerate self-join when both comparison
operands source from one parent CTE. Both should also raise a clean planner error rather than emitting the
INVALID_REFERENCE sentinel / invalid SQL, to stop agent token blow-ups.
