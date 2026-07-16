# Handoff — scope diagnostics hide authored equality and render a transitive join equality

**Status:** FIXED 2026-07-15 (the "Preferred" API shape)  

## Resolution

`input_row_filters` now reports the AUTHORED WHERE predicate; when a scoped-join
group rewrote an endpoint, the planner's form appears under a separate
`normalized_input_row_filters`, and the join equivalences under `scoped_joins`
(rendered in authored surface form — `union join a = b`, subset operands
un-swapped). Both new fields are omitted when there is no divergence, so the
control case (no scoped join) is byte-identical to before.

Implementation:
- `BuildSelectLineage` now carries `authored_where_clause` (the pre-
  normalization WHERE) — diagnostics-only, never read by planning
  (`trilogy/core/models/build.py`).
- `_Extractor.extract` renders row filters from the authored conjuncts, pairing
  them 1:1 with the build conjuncts (which still drive cross-row classification);
  a length-mismatch guard degrades safely to the normalized spelling.
- `process_query` passes `statement.join_clauses` (authored `SelectJoin`s) into
  `extract_derived_value_scopes`; rendering undoes the subset operand swap.
- Agent guidance in `trilogy/scripts/agent.py` explains the new fields.
- Tests: `tests/test_scope_diagnostics.py` — the four asserted behaviors plus
  the three-member-group endpoint-invariance case.

---

**Original status:** OPEN presentation/diagnostics bug  
**Run:** `results/explore_namespaced_top10_restart_20260715-091146_enriched`, q17  
**Impact:** The query executes correctly, but `derived_value_scopes` makes an
authored row filter appear missing and presents a transitive scoped-join
equivalence as though it were the effective input filter. The agent spent about
23k additional prompt tokens repeatedly proving that its condition still worked.

## Symptom

The authored query contains this ordinary row filter:

```trilogy
where ss.return_customer.sk = ss.customer.sk
```

and these separate scoped-join declarations:

```trilogy
union join ss.customer.sk = cs.billing_customer.sk
union join ss.item.sk = cs.item.sk
```

For every aggregate, `derived_value_scopes.input_row_filters` omits the authored
filter and instead reports:

```json
[
  "ss.date.year = 2001",
  "ss.return_customer.sk = cs.billing_customer.sk",
  "ss.return_date.year in (2001, 2002)",
  "ss.is_returned = True",
  "cs.sold_date.year in (2001, 2002)"
]
```

The agent correctly noticed that
`ss.return_customer.sk = ss.customer.sk` disappeared. It then alternated between
assuming the filter was silently dropped and assuming role-playing made it
implicit, repeatedly rewriting/running the query and comparing counts.

## Semantics are intact

Removing the authored equality changes the aggregate count from 23 to 85 in the
agent's probe. The filter is therefore not dropped from execution.

Generated SQL contains both relationships, though normalized:

```sql
INNER JOIN catalog_sales cs
  ON ss.SS_CUSTOMER_SK IS NOT DISTINCT FROM cs.CS_BILL_CUSTOMER_SK
 AND ss.SS_ITEM_SK = cs.CS_ITEM_SK

WHERE sr.SR_CUSTOMER_SK =
      coalesce(cs.CS_BILL_CUSTOMER_SK, ss.SS_CUSTOMER_SK)
```

For the inner matched population this enforces the authored
`return_customer = customer` condition. The problem is the diagnostic spelling
and provenance, not query execution.

## Minimal reproduction

Use the current TPC-DS model:

```trilogy
import store_sales as ss;
import catalog_sales as cs;

where
    ss.date.year = 2001
    and ss.return_customer.sk = ss.customer.sk
    and cs.sold_date.year in (2001, 2002)
select
    count(ss.quantity) as store_qty_count
union join ss.customer.sk = cs.billing_customer.sk
union join ss.item.sk = cs.item.sk;
```

Expected diagnostic behavior: the report must make it unambiguous that the
authored input restriction `ss.return_customer.sk = ss.customer.sk` is applied.
Join declarations must not silently replace it in a field called
`input_row_filters`.

Actual behavior: the filter is rendered as
`ss.return_customer.sk = cs.billing_customer.sk`, and the authored spelling is
absent.

Control: remove both `union join` declarations (and the catalog reference). The
diagnostic then reports `ss.return_customer.sk = ss.customer.sk` as expected.

## Root cause

`trilogy/core/scope_diagnostics.py::_Extractor.extract` deliberately reads the
post-normalization `BuildSelectLineage.where_clause` and renders every ordinary
conjunct into `input_row_filters`:

```python
where_conjuncts = _conjuncts(where.conditional) if where else []
row_filters = [render_scope_expr(c) for c in where_conjuncts ...]
```

By this stage, `BuildFactory` has canonicalized scoped-join key groups. Equality
normalization has replaced the customer endpoint with the join group's
representative, so the extractor sees only the transitive condition. The build
lineage passed through `build_lineage_sink` has no scoped-join/provenance field
that lets diagnostics distinguish:

1. authored WHERE predicates;
2. scoped-join domain declarations; and
3. normalized/transitive conditions used by planning.

The author `SelectLineage` still has `where_clause` and `scoped_joins`, but
`query_processor.process_query` calls `extract_derived_value_scopes` only with
the normalized build lineage and environment.

Relevant loci:

- `trilogy/core/query_processor.py`, where `build_lineage_sink[0]` is passed to
  `extract_derived_value_scopes`;
- `trilogy/core/scope_diagnostics.py::_Extractor.extract`;
- scoped-key canonical substitution in `trilogy/core/models/build.py`.

## Required behavior

Preserve the specification's goal of reporting effective scope, but do not
erase condition provenance. Any of these API shapes would be acceptable:

### Preferred

Keep `input_row_filters` as authored row-restricting predicates and add a
separate normalized field when it materially differs:

```json
"input_row_filters": [
  "ss.return_customer.sk = ss.customer.sk"
],
"normalized_input_row_filters": [
  "ss.return_customer.sk = coalesce(cs.billing_customer.sk, ss.customer.sk)"
],
"scoped_joins": [
  "union join ss.customer.sk = cs.billing_customer.sk",
  "union join ss.item.sk = cs.item.sk"
]
```

### Minimum viable

Continue reporting the normalized condition, but render the actual effective
coalesce expression and attach an `authored_as` value. Do not present the
transitive `return_customer = billing_customer` equality alone: that spelling
looks like a different business filter.

## Tests

Add an end-to-end scope diagnostic test using two fact models and:

```text
where return_customer = customer
union join customer = billing_customer
```

Assert all of the following:

1. the authored equality remains visible;
2. scoped joins are distinguishable from row filters;
3. the normalized/effective form, if exposed, matches generated SQL semantics;
4. removing the scoped join restores the same authored-filter presentation;
5. a three-member join group does not arbitrarily change the displayed
   business predicate based on canonical endpoint selection.

Do not fix this by merely special-casing q17 addresses. The issue applies to any
WHERE equality whose endpoint participates in a scoped-join equivalence group.

