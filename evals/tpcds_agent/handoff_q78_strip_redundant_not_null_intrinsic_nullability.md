# HANDOFF — q78: `StripRedundantNotNull` drops a meaningful `IS NOT NULL` on an intrinsically-nullable base column (silent null leak)

**Status:** ✅ FIXED 2026-07-05 — see the STATUS section in
`bug_q78_regression_4p8M_strip_notnull_drops_nullable_customer.md`. The pass now
corroborates drops against a ground-truth source-tree nullability walk
(`_unfiltered_nullable_addresses`); the deeper mechanism was condition-refinement
(`_refine_nullable_for_conditions` + proven_non_null gates) erasing the filtered
column from `nullable_concepts`, making the pass strip the very filter that proved
non-nullness. Guards in `tests/optimization/test_multihop_not_null_preserved.py`.
The q94 sibling (`join_upgrade.py`) remains OPEN.
**Full diagnosis:** `evals/tpcds_agent/bug_q78_regression_4p8M_strip_notnull_drops_nullable_customer.md`
**Classification:** REAL framework bug, **SILENT** (wrong rows, no error → agent thrashed to a
4.86M-token exhaustion). Optimizer nullability-model soundness hole.
**Related:** sibling to the q94 `join_upgrade.py` bug — both are optimizer passes reasoning from an
incomplete nullability model. See
`handoff_q94_join_upgrade_leftjoin_to_inner_on_shared_key.md`. **Fix/review the two together.**

## Symptom
The agent authored `... and <channel>.customer.id is not null ...` in every arm of a 3-arm
`union(...)`, yet the output rows have `cust_id = NULL`. Every generated arm CTE has **silently
dropped the `<customer> IS NOT NULL` predicate**:
```
store   WHERE D_YEAR=2000 and SR_RETURN_TIME_SK IS NOT NULL = False      -- no SS_CUSTOMER_SK is not null
web     WHERE D_YEAR=2000 and (WR_ORDER_NUMBER is not null) = False      -- no WS_BILL_CUSTOMER_SK is not null
catalog WHERE D_YEAR=2000 and CR_RETURN_AMOUNT is null                   -- no CS_BILL_CUSTOMER_SK is not null
```
Null-customer sales rows survive → wrong result. No error, no hang (every `run` ~400ms) — the
agent burned its whole 75-iteration budget trying to reconcile "why are there null customers when I
filtered them out." (This is NOT the prior q78 FULL-join timeout bug — different, newer construct.)

## Root cause
`trilogy/core/optimizations/strip_redundant_not_null.py:44-73`, `StripRedundantNotNull.optimize`,
drop guard at **:57-63**:
```python
if (concept is not None
    and concept.derivation == Derivation.ROOT
    and is_scalar_condition(atom)
    and not concept.equivalent_addresses.isdisjoint(output)   # in this CTE's outputs
    and concept.equivalent_addresses.isdisjoint(nullable)):   # NOT in nullable_concepts
    dropped = True   # drops `customer IS NOT NULL`
```
The pass treats `cte.nullable_concepts` as a **complete** record of "can this be NULL here" and
strips any `IS NOT NULL` on a ROOT output concept absent from it. But `nullable_concepts` only
records **outer-join padding** nullability — it does **not** carry a concept's **intrinsic
source-column** nullability. `customer.id` is a plain base-table column (`SS_CUSTOMER_SK`, etc.)
that is genuinely NULLable at source and is not padded by any outer join, so it is (wrongly) absent
from `nullable_concepts`. The `IS NOT NULL` is meaningful, not tautological — stripping it is
unsound. (The docstring's claim at :8-12 that "with the join path known the decision is exact" is
false for base columns nullable in their own table.)

Instrumentation at the strip point, all 3 arm CTEs:
`concept=<...>.customer.id  derivation=ROOT  in_output=True  in_nullable_set=False → will_drop=True`.

## Trigger matrix (customer `is not null` filters surviving in generated SQL)
| variant | customer filters kept |
|---|---|
| single non-union select, `is not null` on output key | 3/3 ✅ |
| 3-arm union, plain `sum(qty)` (one measure) | 3/3 ✅ |
| 3-arm union, `sum(qty?=store)` + `sum(qty?!=store)` (one measure COLUMN) | 3/3 ✅ |
| **3-arm union, `sum(qty?..)` + `sum(wc?..)` (two measure columns)** | **0/3 ❌** |
| full agent query78.preql (6 filtered aggs + ratio + having + order) | **0/3 ❌** |

**Trigger:** the final aggregate consuming **>1 union measure column** produces the two-pass
filtered-aggregate GROUP BY shape, in which the arm CTE's `nullable_concepts` omits `customer` →
the strip fires. `having`/`ratio`/`order by`/arm-count are NOT the trigger.

**Change link (not a regression, an exposure):** the q17 two-pass-aggregate work
(`trilogy/dialect/base.py`) is what makes this union+filtered-aggregate form *compile*, newly
reaching this latent `StripRedundantNotNull` hole (pass added 2026-06-05, PR #574). The old q78 run
dodged it via a FULL join.

**Toggle proof:** disabling ONLY `CONFIG.optimizations.strip_redundant_not_null` restores all 3
filters (0→3) and the null-customer rows disappear; toggling every other pass changes nothing.

## Fix direction
`StripRedundantNotNull` may only strip `<c> IS NOT NULL` when `c` is **provably non-null here** —
which requires BOTH (a) not outer-join-padded (current `nullable_concepts` check) AND (b) not
intrinsically nullable at its source column. Consult the concept's source-column nullability (the
same intrinsic-nullability signal the scan/BASIC layer already stamps elsewhere — grep for where
base-column nullability is recorded) and treat an intrinsically-nullable ROOT column as nullable
even when absent from `nullable_concepts`. Only strip when the column is known-non-null at source
(e.g. a NOT NULL / key column) and unpadded.

Investigate secondarily why `nullable_concepts` for the arm CTE is **unstable** across the 1-vs-2
measure shapes (customer present with one measure, absent with two) — the two-pass GROUP BY path
should carry the same nullability set as the one-pass path. Fixing that instability may be the
cleaner root fix; the intrinsic-nullability guard above is the correctness backstop.

## Test to add
DuckDB codegen+execute test (near the optimization / null-handling tests): the two-measure 3-arm
union shape must keep all `<customer> IS NOT NULL` predicates and return zero null-customer rows.
Assert the one-measure shape still strips nothing it shouldn't, and that a genuinely-redundant
`IS NOT NULL` on a NOT-NULL key column IS still stripped (don't regress the optimization).

## Acceptance criteria
- Two-measure 3-arm union keeps the customer filters (3/3); no null-customer rows.
- One-measure and single-select shapes unchanged.
- A redundant `IS NOT NULL` on a genuinely non-null column is still stripped (regression guard).
- No regression in the optimization suite / `tests/join_matrix/`.
- `ruff check . --fix && mypy trilogy && black .` clean.

## Do NOT
- Do NOT disable `StripRedundantNotNull` wholesale — it's a valid optimization; make it consult
  intrinsic source nullability so it only strips genuinely-redundant predicates.

## Secondary (flagged, NOT in scope — separate look)
- Store arm renders `INNER JOIN store_returns … WHERE SR_RETURN_TIME_SK IS NOT NULL = False` for
  `is_returned=false`, vs LEFT for web/catalog — may drop non-returned store rows (model-level
  `is_returned` definition for `store_sales`).
- web/catalog arms render item as `coalesce(WR_ITEM_SK, WS_ITEM_SK)` (the q64 coalescing-key path);
  benign here but the same transitive-key machinery.
