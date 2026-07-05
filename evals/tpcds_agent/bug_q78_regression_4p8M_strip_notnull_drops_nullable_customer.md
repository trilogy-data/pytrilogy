# q78 — 4.86M-token exhaustion is agent thrashing on a SILENT wrong-results bug: `StripRedundantNotNull` drops a genuinely-meaningful `<customer> IS NOT NULL`

Run: `evals/tpcds_agent/results/20260705-142435` (q78). Burned **4,860,621 tokens and
EXHAUSTED its 75-iteration budget** (`crash.q78.txt`: "Agent exhausted 75 iterations
without returning control"). This is ~7x the prior documented ~655k
(`bug_q78_residual_churn_001912.md`).

## Verdict: NOT a regression of the prior residual, NOT a slow-plan hang — it is agent thrashing on a NEW silent bug

The prior 655k run failed on a **FULL-join** cartesian timeout (chained-composite-key
Defect A + constant-key `1=1` Defect B). This run never used a FULL join. The agent
picked a completely different, now-compilable formulation: a 3-arm `union(...)` rowset of
the channels feeding a grouped **filtered-aggregate** select (`sum(x ? channel='store')`
vs `!= 'store'`). Every `trilogy run` completes in ~400ms — there is **no hang / no slow
plan**. The churn is pure reformulation thrashing driven by a silent wrong result: the
agent filters `customer.id is not null` in every arm, yet the output has `cust_id = NULL`
rows (final run, msg 156: `[2000,12467,null,...]`, `[2000,15743,null,...]`). It spends the
whole budget trying to reconcile "why are there null customers when I filtered them out"
(explicitly noted msg 75, msg 149, msg 153).

## Symptom (minimal, deterministic, read-only via `generate_sql`)

The final `workspace/query78.preql` (a 3-arm union + filtered aggregates) generates arm
CTE WHERE clauses that have **silently dropped the `<customer> IS NOT NULL` predicate**:

```
uneven (store):   WHERE D_YEAR = 2000 and SR_RETURN_TIME_SK IS NOT NULL = False
young  (web):     WHERE D_YEAR = 2000 and (WR_ORDER_NUMBER is not null) = False
cheerful (catalog): WHERE D_YEAR = 2000 and CR_RETURN_AMOUNT is null
```

None of them has `SS_CUSTOMER_SK / WS_BILL_CUSTOMER_SK / CS_BILL_CUSTOMER_SK is not null`,
even though the source authored `... and ss.customer.id is not null and ...` in each arm.
Null-customer sales rows therefore survive → the 2 output rows both have `cust_id = NULL`.

## Trigger matrix (customer `is not null` filters surviving in generated SQL)

Base = 3-arm `union(...)` of store/web/catalog, each arm `where <year>=2000 and
<customer> is not null and <not-returned>`; final grouped select over the union.

| variant | final select | customer filters kept |
|---|---|---|
| single non-union select, `is not null` on output key | — | 3/3 (fine) |
| 2-arm union, one measure (`sum(qty)`) | plain sum | 2/2 (fine) |
| 3-arm union, plain `sum(qty)` | 1 measure | 3/3 (fine) |
| 3-arm union, `sum(qty?=store)`,`sum(qty?!=store)` | **1 measure col (qty)** | 3/3 (fine) |
| 3-arm union, `sum(qty?..)` + `sum(wc?..)` | **2 measure cols** | **0/3 (BUG)** |
| 3-arm union, 6 filtered aggs (qty+wc+sp) | 3 measure cols | **0/3 (BUG)** |
| full agent query78.preql | 6 filtered aggs + ratio + having + order | **0/3 (BUG)** |

**Trigger:** the final aggregate consuming **more than one** union measure column (qty AND
wc/sp). One measure → filters kept; two+ → all three dropped. `having`, `ratio`, `order
by`, and arm count are NOT the trigger.

## Root cause (file:line)

`trilogy/core/optimizations/strip_redundant_not_null.py:44-73`,
`StripRedundantNotNull.optimize`, the drop guard at **lines 57-63**:

```python
if (concept is not None
    and concept.derivation == Derivation.ROOT
    and is_scalar_condition(atom)
    and not concept.equivalent_addresses.isdisjoint(output)   # in this CTE's outputs
    and concept.equivalent_addresses.isdisjoint(nullable)):   # NOT in nullable_concepts
    dropped = True   # <-- drops `customer IS NOT NULL`
```

The pass treats `cte.nullable_concepts` as a **complete** record of "can this be NULL
here", and strips any `IS NOT NULL` on a ROOT output concept absent from it. But
`nullable_concepts` only records **outer-join padding** nullability — it does **not** carry
the concept's **intrinsic source-column** nullability. `customer.id` here is a plain
base-table column (`SS_CUSTOMER_SK` etc.) that is genuinely NULLable at source and is not
padded by any outer join, so it is (wrongly) absent from `nullable_concepts`. The
`IS NOT NULL` on it is meaningful, not tautological, and stripping it is unsound. The
docstring's claim (lines 8-12) that with the join path known "the decision is exact" is
false for base columns that are nullable in their own table.

Instrumentation at the strip point (all three arm CTEs):
`concept=<...>.customer.id  derivation=ROOT  in_output=True  in_nullable_set=False → will_drop=True`.

Confirmed causal: disabling ONLY `CONFIG.optimizations.strip_redundant_not_null` restores
all 3 filters (0→3) and the null-customer rows disappear; toggling every other pass
(`predicate_pushdown`, `simplify_null_safe_joins`, `merge_aggregate`, `union_dim_pushdown`)
changes nothing. With the whole optimizer off the filters are also present.

### Why the "≥2 measures" trigger

`nullable_concepts` for the union-arm scan CTE is **unstable** across the two shapes: with
one consumed measure, `customer` IS in the arm's `nullable_concepts` (so `will_drop=False`,
guard kept); adding a second measure produces the two-pass filtered-aggregate GROUP BY
shape (the recent q17 `dialect/base.py` two-pass-aggregate work is what makes this union
form compile), and in that shape the arm CTE's `nullable_concepts` omits `customer` →
`will_drop=True`. So the multi-measure path both (a) newly compiles and (b) exposes the
latent `StripRedundantNotNull` soundness hole. The pass was added 2026-06-05 (PR #574
"Agentic Optimization"); the prior q78 run never hit it because it used a FULL join, not
this union+aggregate form.

## Secondary observations (NOT the token sink; noted for completeness)

- **Store arm join asymmetry:** `ss.is_returned = false` renders as `INNER JOIN
  store_returns ... WHERE SR_RETURN_TIME_SK IS NOT NULL = False`, whereas web/catalog
  render as `LEFT OUTER JOIN ... WHERE (WR_ORDER_NUMBER is not null) = False`. The store
  INNER-join-to-returns form looks semantically wrong for "never returned" and may drop
  non-returned store rows; worth a separate look (model-level `is_returned` definition for
  store_sales).
- **Item keyed through the returns table:** web/catalog arms render
  `it = coalesce(WR_ITEM_SK, WS_ITEM_SK)` / `coalesce(CR_ITEM_SK, CS_ITEM_SK)` (the q64
  coalescing-key machinery in `rowset_node.py`). Benign here because the arm filters
  returns-is-null so the coalesce reduces to the sale item, but it is the same transitive
  key path.

## Classification

**(b) Agent thrashing driven by a (a) silent framework codegen BUG.** No error, no hang;
each plan compiles and runs fast but returns semantically wrong rows (null customers that
the authored filter explicitly excludes). The bug is a soundness hole in
`StripRedundantNotNull`: it strips an `IS NOT NULL` on a genuinely-nullable base column
because `nullable_concepts` (outer-join-padding only) does not record intrinsic source
nullability. Severity high — silently wrong results with no signal are exactly what denies
the agent a correction cue and burns the full iteration budget. Do NOT fix (per task).

## STATUS: FIXED 2026-07-05

Root cause refined: `nullable_concepts` DOES record intrinsic binding-level (`?`)
nullability at construction, but build-time condition refinement
(`StrategyNode._refine_nullable_for_conditions`, plus the `proven_non_null` gates in
`datasource_nodes.py` and `group_node.py`) removes a concept the node's own WHERE
null-rejects — so the authored `customer IS NOT NULL` erased its own evidence, and
`StripRedundantNotNull` read the absence as "never nullable" and stripped it (circular).
The ≥2-measure trigger just changes which node carries condition+output together.

Fix: `strip_redundant_not_null.py` now corroborates a candidate drop against
`_unfiltered_nullable_addresses(cte.source)` — a walk of the CTE's source tree collecting
intrinsic nullability at `BuildDatasource` leaves plus outer-join padding at every level
(`find_nullable_concepts` per QDS). A guard on a column that is nullable at ground truth
absent filtering is never stripped; genuinely non-null-at-source strips still fire.
Guards: `tests/optimization/test_multihop_not_null_preserved.py::
test_union_multi_measure_keeps_binding_nullable_fk_guard` (+ single-measure control).
Verified against this run's workspace: all 3 arm predicates restored.
