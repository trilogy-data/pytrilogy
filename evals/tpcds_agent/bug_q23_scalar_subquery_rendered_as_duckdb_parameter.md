# Bug: q23 scalar subquery rendered as a DuckDB parameter

**FIXED 2026-08-15.** Scalar `(select ...)` subqueries now lower correctly in
select-output and assignment position. See "Fix" below. The re-verification that
motivated it is kept for the record.

---

**Re-verified OPEN 2026-08-15 — symptom changed, root cause unchanged, severity WORSE.**

The titular symptom is gone: no `$1` placeholder, no `SubqueryItem` reaches the
DuckDB driver, no `NotImplementedException`. The `SubqueryItem` build dispatch
added in 4dcac11ea (`trilogy/core/models/build.py:4081`) unwraps the item to its
single rowset output before SQL generation.

But the root cause this report named — *"the renderer/expression
parameterization path mistakes the hydrated `SubqueryItem` for a literal"* — is
still live, one layer earlier. `unwrap_transformation`
(`trilogy/parsing/common.py:135-141`) has no `SubqueryItem` branch, so a
subquery in select-output position falls through to the `CONSTANT` wrapper:

```text
SelectItem(content=ConceptTransform(
    function=constant(<Subquery: ref:_subquery_1_8.val_min>), output=local.m))
```

That is the same misclassification, and it is the branch the `Comparison` case
at `common.py:126` was added to escape. The rowset lineage is lost, so the
subquery's `where` and grouping never reach SQL.

Predicate placement (`having`/`where`) and membership are correct — those
materialize the synthetic rowset as its own CTE. Only the select-output
placement is broken.

### Current behavior by placement (2026-08-15, tree at 88a6a7c61)

| Shape | Result |
| --- | --- |
| `auto m <- (select max(rs.total));` | clean `HydrationError` (acceptable fallback) |
| `select (select min(rs.val)) as m;` | `ValueError` render sentinel: `Missing source reference to local.val` |
| `select (select min(rs.val)) as m, sum(val) as grand;` | **silently wrong result** |
| `select name, (select min(rs.val)) as m;` | `ValueError` render sentinel: global aggregate at keyed grain |
| `select ... having total = (select max(rs.total));` | correct |
| `select ... where val > (select max(val)/2 -> half);` | correct |
| `select min(rs.val) as m, sum(val) as grand;` (named control) | correct |

The third row is a regression in severity relative to the original report: a
crash became a wrong answer. Repro on a three-row model where the rowset is
filtered so its scalar differs from the outer scope:

```preql
rowset f1 <- select id, val where val >= 20;
select (select min(f1.val)) as m, sum(val) as grand;
```

Expected `(20, 60)`; actual `(10, 60)`. The rowset's `where val >= 20` is
dropped entirely and `min` is re-resolved against the outer scope:

```sql
WITH quizzical as (SELECT "rows"."val" as "val" FROM ...)
SELECT min("quizzical"."val") as "m", sum("quizzical"."val") as "grand"
FROM "quizzical"
```

The named control (`select min(f1.val) as m, sum(val) as grand;`) returns
`(20, 60)` correctly, isolating the defect to the inline `(select ...)` form.

A second shape flattens the subquery into a nested aggregate that DuckDB
rejects — `select (select max(rs.total)) as m, sum(val) as grand;` over an
aggregate rowset emits `max(sum("quizzical"."val"))` and binder-errors.

### Existing coverage gap

`tests/engine/test_duckdb_subquery.py` exercises only `having`, `where`, and
membership placements — every passing case is a predicate. There is no
select-output case, which is why this survived.

## Fix (2026-08-15)

Three changes, all author-layer:

1. `trilogy/parsing/common.py` — `unwrap_transformation` grows a `SubqueryItem`
   branch before the `CONSTANT` fallthrough. It returns
   `Function(ALIAS, arguments=[subquery_item])`, taking datatype and purpose
   from the referenced rowset concept.

   The item is aliased *whole* rather than unwrapped to `.content`. Both lower
   identically (build's `SubqueryItem` dispatch reduces the item to the rowset
   concept), but unwrapping to `.content` destroys render fidelity: the
   renderer needs the `SubqueryItem` to reproduce the inline `(select ...)`
   form, and without it the round-trip emits the synthetic
   `_subquery_1_8.val_min` — a name that does not reparse. The renderer already
   passes ALIAS through to its argument, so the wrap costs nothing.

2. `trilogy/parsing/common.py` — `_grain_contribution`'s ALIAS recursion read
   `arguments[0].address` behind a `# type: ignore`, which `AttributeError`s on
   a non-address-bearing argument. It now narrows with `isinstance` and follows
   a `SubqueryItem` to its rowset output concept. The new
   non-`Concept`/`ConceptRef` fallthrough only reaches inputs that previously
   crashed.

3. `trilogy/parsing/v2/rules/concept_rules.py` — `concept_derivation` routes a
   `SubqueryItem` source through the same `unwrap_transformation`, so
   `auto x <- (select ...)` and `select (select ...) as x` behave identically.
   This turns the report's original shape from a clean-but-unnecessary
   `HydrationError` into working code.

### Verification

Every row of the placement table above now returns the correct result,
including the two that previously errored and the one that was silently wrong.
The round-trip renders as `(select min(rs.val) as val_min)` with no
`_subquery_` leak and re-executes to the same rows.

The corpus footprint is zero: no `.preql` under `tests/` uses an inline
`(select ...)` outside `in`/`not in`, so the pre-fix/post-fix A/B rests
entirely on the new tests, each of which was confirmed failing beforehand.

### Regression tests added

In `tests/engine/test_duckdb_subquery.py`, all on both parser backends:

- `test_scalar_subquery_as_select_output` — six placements: scalar alone,
  scalar plus a companion aggregate, scalar beside a grouping dimension,
  scalar over an aggregate rowset (the shape that emitted the nested
  `min(max(val))`), subquery over a base concept, and the `auto` assignment
  form. The rowset is filtered so a dropped filter changes the answer.
- `test_scalar_subquery_select_output_matches_named` — inline form equals the
  equivalent named-rowset reference.
- `test_scalar_subquery_select_output_is_not_a_literal` — lineage is not a
  `CONSTANT` function, and the SQL contains no `$1` and no `Subquery`.
- `test_scalar_subquery_select_output_round_trips` — renders back to the inline
  form and reparses.

## Summary

A fresh ten-run enriched Q23 baseline exposed a framework defect distinct from
the previously fixed multi-column-subquery validation issue. A supported scalar
subquery reaches SQL generation as a bound Python `SubqueryItem` value. DuckDB
cannot transform that Python object into a logical type and raises an unhandled
`NotImplementedException`.

This is generated-SQL/framework failure, not an authored syntax error.

## Artifacts

- Run: `evals/tpcds_agent/results/repeat_q23_20260714-003601_enriched`
- Failing trajectory: `agent_log.q23.r09.jsonl`
- Prior, fixed issue: `bug_q23_supported_subselect_unexpected_error.md`
- Canonical: `tests/modeling/tpc_ds_duckdb/query23.preql`

The ten-run baseline passed only 1/10, averaged 1.02M prompt tokens, and had one
2.57M-token exhausted trajectory. The exception below occurred in repetition
9.

## Error

```text
Unexpected error in stdin: (_duckdb.NotImplementedException)
Not implemented Error: Unable to transform python value of type
'<class 'trilogy.core.models.author.SubqueryItem'>' to DuckDB LogicalType
```

The generated SQL contains an ordinary placeholder instead of rendered
subquery SQL:

```sql
quizzical as (
    SELECT
        $1 as "max_alltime"
)
```

The bound parameter is:

```text
(<Subquery: ref:_subquery_31_5.alltime_total_max>,)
```

## Failing semantic shape

The agent was calculating a scalar maximum over a customer-level aggregate and
referencing that scalar alongside another aggregate. In simplified form:

```preql
rowset alltime <-
select
    sales.billing_customer.sk,
    sum(sales.quantity * sales.sales_price) as total
;

auto max_alltime <- (select max(alltime.total));

select
    max_alltime,
    another_scalar
;
```

The exact failing candidate should be extracted from the last stdin diagnostic
call in `agent_log.q23.r09.jsonl` when minimizing.

## Expected behavior

The scalar subquery should be rendered as a SQL subquery or materialized CTE
reference. No `SubqueryItem` object may enter the database-driver parameter
list.

If the authored placement is unsupported, Trilogy must reject it before SQL
execution with a structured syntax/resolution error and source location. It
must never emit a Python semantic-model object as a database parameter.

## Relationship to the prior Q23 fix

The prior report concerned a multi-column subquery and is fixed: Trilogy now
returns an actionable one-column validation error. This new failure occurs
after a subquery has been accepted and hydrated. The renderer/expression
parameterization path mistakes the hydrated `SubqueryItem` for a literal.

## Investigation

Steps 1-4 are done as of 2026-08-15; the remainder stands.

1. ~~Minimize the failing expression.~~ Minimal repro is in the
   re-verification table above; no eval log needed.
2. ~~Trace `SubqueryItem` through hydration and rendering.~~ Select outputs
   reach `unwrap_transformation` via
   `trilogy/parsing/v2/rules/select_statement_rules.py:399` (aliased) and
   `:462` (anonymous).
3. ~~Find the branch that classifies it as a literal.~~
   `trilogy/parsing/common.py:135-141`, the `else` fallthrough to the
   `FunctionType.CONSTANT` wrapper. `SubqueryItem` is absent from the
   pass-through tuple at `:121-125` and from the `Comparison` escape at `:126`.
4. ~~Compare placements.~~ See the table above: predicate and membership
   placements are correct, `auto` errors cleanly, select-output is broken.
5. ~~Decide the fix shape.~~ Alias the `SubqueryItem` from
   `unwrap_transformation`; build's existing dispatch then materializes the
   rowset as its own CTE, the same path predicate placement already took. See
   "Fix" above.
6. ~~Ensure nested scalar subqueries and multiple subqueries in one select
   render once without entering `compiled_parameters`.~~ Verified: two distinct
   subqueries in one select each get their own CTE, two textually identical
   ones collapse to a single CTE with two projections, and a nested
   `(select min((select max(rs.val))))` resolves. All covered by
   `test_scalar_subquery_multiple_and_nested`.

## Regression coverage

Add DuckDB execution tests asserting that:

- a one-column scalar subquery can be assigned and selected;
- a scalar aggregate over a rowset can be combined with another scalar;
- generated SQL contains subquery SQL or a valid CTE reference, not `$1` for
  the subquery;
- a subquery over a *filtered* rowset keeps that filter — the
  `(20, 60)` vs `(10, 60)` case above, which no current test would catch;
- the inline `(select ...)` form and the equivalent named-rowset reference
  return identical rows;
- the parameter list contains only database-supported literal values; and
- no `SubqueryItem` reaches the dialect driver.

