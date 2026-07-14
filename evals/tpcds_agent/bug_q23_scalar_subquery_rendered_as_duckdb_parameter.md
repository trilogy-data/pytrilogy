# Bug: q23 scalar subquery rendered as a DuckDB parameter

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

1. Extract and minimize the exact scalar-subquery expression from repetition
   9.
2. Trace `SubqueryItem` through hydration, expression rendering, and parameter
   collection.
3. Find the branch that classifies it as a literal/bind parameter.
4. Compare a scalar subquery used in a predicate, an `auto`, and a selected
   output.
5. Test a scalar derived from a plain rowset value versus an aggregate over a
   rowset value.
6. Ensure nested scalar subqueries and references shared across multiple output
   expressions render once without entering `compiled_parameters`.

## Regression coverage

Add DuckDB execution tests asserting that:

- a one-column scalar subquery can be assigned and selected;
- a scalar aggregate over a rowset can be combined with another scalar;
- generated SQL contains subquery SQL or a valid CTE reference, not `$1` for
  the subquery;
- the parameter list contains only database-supported literal values; and
- no `SubqueryItem` reaches the dialect driver.

