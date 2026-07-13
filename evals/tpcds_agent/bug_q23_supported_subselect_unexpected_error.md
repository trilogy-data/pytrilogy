# Bug: q23 supported subselect reaches `Unexpected error`

**Status: FIXED 2026-07-12.** `handle_execution_exception` now classifies
`HydrationError` as `Syntax error` and surfaces the diagnostic's source
location (`trilogy/scripts/common.py`); the raise site's message now includes
the one-column correction (`trilogy/parsing/v2/rules/rowset_rules.py`).
Output is now:

```text
Syntax error in stdin: a `(select ...)` subquery used as a scalar value or
membership set must select exactly one column; project only the key/value
consumed by the outer expression (line 2, column 23)
```

Regression coverage: `tests/engine/test_duckdb_subquery.py` (one-column
scalar/membership/rowset-qualified succeed; multi-column raises a structured
error with location, both backends) and
`tests/scripts/test_trilogy.py::test_multi_column_subquery_reported_as_syntax_error`
(CLI labels it `Syntax error` with location, never `Unexpected error`).
The agent-guide contradiction was already resolved in `trilogy/ai/constants.py`
(one-column expression subqueries documented as supported).

## Summary

The enriched q23 trajectory used Trilogy's supported `(select ...)` subquery
form while building membership and scalar filters. A multi-column projection
reached an unhandled validation path and returned:

```text
Unexpected error in stdin: a `(select ...)` subquery must select exactly one column
```

This is a framework bug. The construct is recognized as a subquery and the
one-column constraint is known, but the violation is surfaced as an unexpected
internal error instead of an authored validation error with a source location.

The default agent guide simultaneously said "No subselects", which is stale now
that one-column scalar/membership subqueries are supported. That contradiction
contributed to 2.75M tokens of recovery attempts.

## Artifacts

- Run: `evals/tpcds_agent/results/20260712-204357_enriched`
- Trajectory: `agent_log.q23.jsonl` / `agent_log.q23.conversation.txt`
- Candidate: `workspace/query23.preql`
- Reference: `tests/modeling/tpc_ds_duckdb/query23.preql`

The query ultimately failed scoring with 52 candidate rows versus 4 reference
rows.

## Trigger

The failure is produced by a parenthesized subquery in expression position when
its select list contains more than one output. The validator already has enough
information to state the rule, but the exception escapes through the generic
unexpected-error handler.

Representative invalid shape:

```preql
where x.key in (
    select candidate.key, candidate.payload
)
select x.key;
```

The supported form is a one-column projection:

```preql
where x.key in (
    select candidate.key
)
select x.key;
```

## Expected behavior

Reject the multi-column form before planning with a normal syntax/validation
diagnostic that includes the source span and a correction such as:

```text
Subqueries used as scalar values or membership sets must select exactly one
column; project only the key/value consumed by the outer expression.
```

Never label an understood authored constraint as `Unexpected error`.

## Likely fix area

Find the validation path that checks subquery projection cardinality. Convert
its generic exception/assertion into the same structured authored-error type
used by undefined concepts, invalid aggregate placement, and unsupported SQL
syntax. Ensure the CLI preserves the location and suggestion.

Also keep the agent documentation aligned with the parser: one-column
expression subqueries are supported; arbitrary SQL `FROM (SELECT ...)` table
subqueries are not implied by that support.

## Regression coverage

Add parser/validation tests for:

1. one-column `in (select ...)` succeeds;
2. one-column scalar `(select ...)` succeeds where scalar subqueries are legal;
3. two-column subquery fails with a structured authored error;
4. the message includes a source location and never contains `Unexpected error`;
5. rowset-qualified concepts remain resolvable inside the supported form.

