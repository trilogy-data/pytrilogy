# BUG: q64 "Unexpected error" token sink (2.03M) — KeyError leaked from find_nullable_concepts

> **FIXED 2026-07-06.** `trilogy/core/processing/utility.py:200-201` now guards the
> `datasource_map` lookup with `.get()` + None-skip (matching its outer-join siblings at
> 171-178) instead of a hard subscript. A synthetic self-join-key pseudonym `right_datasource`
> absent from the map no longer KeyErrors. NOT caused by the is_returned model change or the
> DOUBLE/decimal type work — a pre-existing latent bug exposed by #594's
> `_unfiltered_nullable_addresses` walk. Deterministic repro now builds (17862 rows). Guard:
> `tests/nodes/utility/test_nullability.py::test_find_nullable_concepts_missing_right_datasource_no_crash`.

**Run:** `evals/tpcds_agent/results/20260706-015417` (q64: 835k → 2,027,624 tok).
**Classification:** FRAMEWORK bug (unhandled `KeyError` swallowed into an opaque
"Unexpected error", leaking an internal concept address). Not wrong results — a crash
the agent could not diagnose, so it thrashed for ~50 iterations.

## Symptom
`trilogy run query64.preql` exits 1 with (5× in the conversation):
```
Unexpected error in query64.preql: 'ss.customer_demographic.customer_demographics_at_ss_customer_demographic_id'
```
The quoted string is `str(KeyError(<addr>))`. The address is a synthetic join-key
pseudonym datasource ("customer_demographics AT ss.customer_demographic.id"). A sibling
form of the same crash surfaces `'ss.customer.customers_at_ss_customer_id'` — same bug,
different self-join key. The generic CLI catch-all `handle_execution_exception`
(`trilogy/scripts/common.py`, via `run.py:218`) stringifies the uncaught exception.

## Root cause — file:line
`trilogy/core/processing/utility.py:201` in `find_nullable_concepts`:
```python
if is_on_nullable_condition:
    nullable_datasources.add(datasource_map[right_id])   # <-- KeyError
```
`right_id = join.right_datasource.identifier`. When a join's right operand is a
synthetic pseudonym/property-key datasource that is NOT registered in that qds's
`datasources` list, `right_id` is absent from `datasource_map` and the direct `[]`
subscript raises `KeyError`. The same function already guards the identical lookup
defensively with `.get()` at lines **171** and **176** — line 201 is an inconsistent
hard subscript. (A defensive `.get()` + `None` skip is the shape of the fix; not applied per instructions.)

**Why it started firing now (caller surface, not the model/type changes):** the q78 fix
(committed in `[Feat]: Agent Optimization (#594)`, 2026-07-05) added
`_unfiltered_nullable_addresses` in `trilogy/core/optimizations/strip_redundant_not_null.py:57`,
which walks the whole source tree and calls `find_nullable_concepts(qds.source_map,
qds.datasources, qds.joins)` on **every** nested QueryDatasource (line 75). This newly
exercises `find_nullable_concepts` on intermediate qds whose joins reference synthetic
self-join-key datasources — exactly the demographic cross-CDEMO self-join. Both framework
files are committed and clean; neither was touched by the is_returned or DOUBLE/decimal work.

`StripRedundantNotNull.optimize` only calls this path when a condition atom is a
`<ROOT-column> IS NOT NULL` whose column is in outputs and not already known-nullable
(lines 104-113) — hence the `is not null` is a required trigger ingredient.

## Minimal repro (against `.../20260706-015417/workspace`, also repro's on canonical model)
```trilogy
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
with cat_list_price as select cs.item.id as item_id, sum(cs.ext_list_price) as total_ext_list_price;
with cat_refund as select cr.item.id as item_id, sum(coalesce(cr.refunded_cash,0)) as total_refund;
where ss.customer.demographics.marital_status != ss.customer_demographic.marital_status
    and ss.ticket_number is not null
select ss.item.id as item_id, count(ss.line_item) as sale_line_count,
subset join cat_list_price.item_id = ss.item.id
subset join cat_refund.item_id = ss.item.id
where cat_list_price.total_ext_list_price > 2 * cat_refund.total_refund;
```
Reproduces the KeyError against BOTH the run workspace and the canonical
`tests/modeling/tpc_ds_duckdb` model (identical bug, address `ss.customer.customers_at_ss_customer_id`).

## Trigger matrix (all four required)
| Ingredient | Present → | Removed → |
|---|---|---|
| cross-CDEMO `!=` self-join (`ss.customer.demographics.marital_status != ss.customer_demographic.marital_status`) | KeyError | OK (M2, no `!=`) |
| a `<ROOT col> IS NOT NULL` atom (drives StripRedundantNotNull) | KeyError | OK ("P3 without is not null") |
| the two catalog-aggregate `subset join`s + cross-rowset agg-compare WHERE (`total_ext_list_price > 2*total_refund`) | KeyError | OK (Q1/Q2, single join or no agg-compare) |
| — | | |
Notes: store_returns / `is_returned` / decimal-double are NOT in the minimal repro.
Single-`!=`-only, single-subset-join, and no-`is not null` variants all build fine.
The catalog aggregate joins + the demographic self-join together are what put the
synthetic pseudonym-key datasource on a join's `right` inside a nested qds.

## CAUSATION VERDICT: (c) pre-existing framework bug
- **NOT (a) is_returned model change.** The minimal repro references neither `is_returned`,
  `_returned_ticket`, `_returned_order_number`, nor `store_returns`/`catalog_returns.is_returned`.
  The canonical `query64.preql` DOES use the new `ss.is_returned` (line 28) and
  `test_sixty_four` PASSES (2 passed) on the current engine. Reverting is_returned cannot
  affect the repro (it touches none of the demographic/catalog columns involved).
- **NOT (b) DOUBLE/decimal type change.** No decimal/double cast anywhere in the repro;
  `trilogy/core/datatype.py`/`enums.py` are clean and not in the traceback.
- **(c) confirmed.** The defect lives entirely in committed framework code
  (`utility.py:201` hard subscript; newly reached via the #594/q78
  `_unfiltered_nullable_addresses` walk). The 2.03M "regression" is a *hit-rate* change,
  not a code regression: this run's agent authored the cross-CDEMO `!=` inside the inner
  row filter (the very construct the canonical query deliberately keeps OUT of
  customer_demographics join planning — see the comment at `query64.preql:19-25`), so it
  tripped the latent crash; the two prior q64 runs never wrote that shape.

## Fix guidance (NOT applied)
Make `utility.py:201` defensive like its siblings:
`ds = datasource_map.get(right_id); if ds is not None: nullable_datasources.add(ds)`.
That converts the crash into the correct no-op (a synthetic key datasource has no
base-table nullability to contribute). Optionally also give the CLI catch-all a clearer
label for internal `KeyError`s (same class as the RecursionError relabel in
`scripts/common.py`). Reverting the is_returned or type change does NOT fix it.
