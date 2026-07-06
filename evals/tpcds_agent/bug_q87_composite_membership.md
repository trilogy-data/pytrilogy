# q87 — composite (multi-column tuple) membership is unusable (2 framework bugs)

Run: `evals/tpcds_agent/results/20260706-135542_enriched` — q87 enriched leg, **443k tokens, FAILED**.
Two distinct `Unexpected error` crashes from `generate_sql` (not clean authored parse errors) → both framework bugs.

## Symptom / business shape
TPC-DS q87 = count customers, identified by the composite `(last_name, first_name, sale_date)`,
present in store_sales but NOT catalog_sales and NOT web_sales in a date window (except / anti-join).
The natural Trilogy spelling is composite anti-membership on a 3-col tuple:
`(a,b,c) not in (set.a, set.b, set.c)`. Both spellings the agent tried crashed the engine.

## The two error triggers (from `agent_log.q87.conversation.txt`)

### Trigger 1 — `Tuple elements have incompatible types STRING and DATE`
Agent wrote (msg 21) a 3-col heterogeneous tuple (STRING last_name, STRING first_name, **DATE** sale_date):
```
where (store_tuples.store_last_name, store_tuples.store_first_name, store_tuples.store_sale_date)
    not in (catalog_tuples.cat_last_name, catalog_tuples.cat_first_name, catalog_tuples.cat_sale_date)
```
Raised at parse/hydrate time as `HydrationError`.

### Trigger 2 — `composite membership operands must be concepts`
Agent's workaround (msg 27): cast the DATE element to `::string` so all three positions are STRING:
```
where (store_tuples.store_last_name, store_tuples.store_first_name, store_tuples.store_sale_date::string)
    not in (catalog_tuples.cat_last_name, catalog_tuples.cat_first_name, catalog_tuples.cat_sale_date::string)
```
Raised at render time as `AssertionError`. The agent was stuck: removing the cast → Trigger 1; adding it → Trigger 2.

## Minimal repro (harness: workspace engine)
```python
import sys; sys.path.insert(0,'evals'); from pathlib import Path; from common import scoring
ws=Path('evals/tpcds_agent/results/20260706-135542_enriched/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(body)   # bodies below (2 rowsets: store_tuples, catalog_tuples)
```

## Trigger matrix
| case | tuple | RHS operands | result |
|------|-------|--------------|--------|
| 2-col, same types (STRING,STRING), `not in` | homogeneous | bare concepts | **OK** |
| 2-col, same types, `in` | homogeneous | bare concepts | **OK** |
| **2-col heterogeneous (STRING,DATE)** | mixed | bare concepts | **HydrationError: Tuple elements have incompatible types STRING and DATE** |
| **3-col heterogeneous (STRING,STRING,DATE)** | mixed | bare concepts | **same HydrationError** (Trigger 1) |
| **2-col homogeneous, RHS has a `::string` cast** | homogeneous | expr (cast) | **AssertionError: composite membership operands must be concepts** |
| **3-col, all `::string` cast** | homogeneous | expr (cast) | **same AssertionError** (Trigger 2) |
| single-col `concat(...) not in concat(...)` | n/a (scalar) | expr | **OK** (the workaround the agent finally shipped) |

Findings: Bug 1 is triggered by **heterogeneous element types**, independent of arity (a 2-col STRING,DATE fails too).
Bug 2 is triggered by **any expression operand** in the row tuple (cast/concat), independent of arity/homogeneity.
`in` vs `not in` makes no difference. RHS being a rowset column is the shape in both.

## Root cause

### Bug 1 — premature, whole-tuple type-merge on a row tuple
`expr_tuple` (`trilogy/parsing/v2/rules/subselect_rules.py:30-37`) builds the `TupleWrapper` for **every** tuple and
calls `reduce_tuple_element_datatypes([...])`, which merges all element datatypes to one representative type and
raises `ValueError("Tuple elements have incompatible types STRING and DATE")`
at `trilogy/core/models/core.py:532-533`.

That whole-tuple merge is correct only for a **scalar-vs-value-list** membership (`x in (1,2,3)` — one common type).
It is wrong for a **composite (row) membership** `(a,b,c) in (...)`, whose positions are compared independently and
may legitimately differ in type. The classification into a row tuple happens later in
`rewrite_composite_membership` (`trilogy/parsing/common.py:506`, called from `subselect_rules.py:76`), and the
**correct per-position** compatibility check already exists in `SubselectComparison` validation
(`trilogy/core/models/author.py:683-698`, zip over `left_elems`/`right_elems`) — but the parse-time merge fails
first, so that code is never reached. The comment at `subselect_rules.py:27-29` explicitly (and incorrectly) claims
this pairwise-merge is intended "for both literal value tuples and composite-membership tuples."

### Bug 2 — composite render path rejects expression operands
`render_composite_membership` (`trilogy/dialect/base.py:1545-1548`) asserts each RHS operand `rc` is a `BuildConcept`:
```python
assert isinstance(rc, BuildConcept), "composite membership operands must be concepts"
```
A `cat_sale_date::string` cast (or any concat/expr) is a `BuildFunction`, so the assert fires.
The **single-column** membership path handles expression RHS via `_render_expression_membership_subselect`
(`base.py:1560`), but the composite path has no equivalent — it only knows how to source a bare concept column.

## Does the canonical avoid composite `in`? — YES
`tests/modeling/tpc_ds_duckdb/query87.preql` builds cleanly on the current engine (1 statement) and **never uses
composite membership**. It computes three per-tuple aggregate counts (`store_in_window`, `catalog_in_window`,
`web_in_window`) as `sum(CASE WHEN channel=... AND date in window THEN 1 ELSE 0 END)` grouped by
`(last_name, first_name, date.date)`, then `sum(CASE WHEN store>0 AND catalog=0 AND web=0 THEN 1 END)`.
The agent's own final shipped `workspace/query87.preql` sidesteps the bugs a different way: single-column
`concat(last||first||date::string)` keys with scalar `not in` (the "OK" matrix row) — but that leg still failed
scoring (443k tokens burned thrashing between the two crashes before landing the concat form).

## Classification
- **Bug 1** (`Tuple elements have incompatible types`): real framework bug. Composite/row membership must NOT require
  a single merged element type; the whole-tuple merge in `expr_tuple` should be skipped for row tuples (or the
  membership tuple should not be type-merged at all — the per-position check in `author.py` is the correct gate).
- **Bug 2** (`composite membership operands must be concepts`): real framework bug. The composite render path must
  accept expression operands (cast/concat), mirroring `_render_expression_membership_subselect` on the scalar path,
  instead of asserting bare-concept operands.
- Not a guidance defect and not agent error: the agent's spelling is the natural one; both spellings it tried are
  legitimate and crash the engine with unhandled exceptions.

DO NOT FIX (read-only investigation).

## RESOLUTION (2026-07-06) — both bugs FIXED

- **Bug 1**: `expr_tuple` no longer fails on a mixed-type tuple — on merge failure it falls back to
  `DataType.UNKNOWN` and defers validation to `SubselectComparison._validate_types` (per-position for
  row tuples; a new per-element-vs-left check for scalar value lists keeps `x in (1, 'a')` an error).
- **Bug 2**: `render_composite_membership` accepts expression operands: inner concepts resolve via
  `_resolve_existence_column` (pinning the existence FROM + null guards) and the expression renders
  through a scoped `_existence_ref_overrides` substitution (plain `render_expr` resolution can't see
  `existence_source_map` and emitted INVALID_ALIAS in pushed-down CTE copies). Literal positions work;
  multi-source or inlined-datasource expression operands raise clear errors instead of asserting.
- Guards: `tests/engine/test_duckdb_tuple_membership.py` (heterogeneous in/not-in, cast operands,
  literal position, scalar strictness preserved).
- Verified on this run's workspace: both agent spellings (Trigger 1 and Trigger 2 bodies) now
  generate + execute, returning 45689 — identical to the agent's shipped concat-key form. The gap to
  `PRAGMA tpcds(87)` (47298) is `NOT IN` vs `EXCEPT` NULL semantics on NULL name components, shared
  by the concat spelling — authored-query semantics, not a rendering defect.
