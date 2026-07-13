# Specification: derived-value scope diagnostics

**Status:** MVP implemented (`trilogy/core/scope_diagnostics.py`; CLI `run
--scope`; JSON/agent default-on). **Audience:** query planner, CLI, and
agent-tool owners.

## Summary

Add an optional, factual scope report to query results. The report describes
the effective input population and grain of every aggregate and window value
used by a query. It also distinguishes filters applied before a value is
computed from filters applied after it is computed.

The first version does not warn about suspicious scopes or attempt to infer
author intent. Its purpose is to make the planner's interpretation observable
so a person or agent can compare it with the business question.

Example:

```text
Derived value scopes

customer_return_total
  kind: aggregate
  expression: sum(web_returns.return_amount)
  input filters: return_date.year = 2002
  group by: return_customer.sk, return_location.state

state_average
  kind: aggregate
  expression: avg(customer_return_total)
  input filters: return_date.year = 2002
  input grain: return_customer.sk, return_location.state
  group by: return_location.state

previous_month_total
  kind: window
  expression: lag(monthly_total)
  input filters: sold_month between 1998-12 and 2000-01
  partition by: category, brand, call_center
  order by: year, month
  output filters: year = 1999
```

## Motivation

Many otherwise valid-looking queries fail because a condition is evaluated at
the wrong stage:

- an outer restriction leaks into the population of an average;
- boundary months are removed before `lag` or `lead` is evaluated;
- a filter intended for aggregate inputs is applied only to aggregate outputs;
- a nested aggregate is evaluated at a different grain than expected.

The result table alone rarely exposes the cause. Generated SQL exposes it, but
is too large and indirect for routine agent diagnosis. A compact scope report
should answer two questions for each cross-row computation:

1. Which rows were available when this value was computed?
2. At what grain, partition, and ordering was it computed?

## Goals

1. Report the planner's **effective** scope, not merely nearby authored syntax.
2. Cover aggregates and numbering/navigation windows.
3. Preserve the distinction between computation input filters and later output
   filters.
4. Produce stable, compact text suitable for humans and language-model agents.
5. Provide a structured representation so terminal, JSON, and agent outputs do
   not independently reconstruct scope semantics.
6. Avoid additional database queries in the initial implementation.

## Non-goals for v1

- Inferring whether a scope is correct for the user's intent.
- Emitting warnings or recommendations.
- Reporting row counts at every planner boundary.
- Explaining ordinary row-scalar expressions.
- Dumping the complete planner graph or generated SQL.
- Guaranteeing authored spelling; normalized expressions are acceptable.

## User-facing behavior

### CLI

Add an opt-in flag:

```powershell
trilogy run query.preql --scope
```

Normal results render first. If the executed statement contains a reportable
derived value, render `Derived value scopes` immediately after its result
table. A file containing several selects receives one scope block per select,
using the existing statement numbering.

Without `--scope`, interactive CLI output is unchanged.

### Agent query tools

Trilogy agent `run`/`run_file` results should include the scope block by
default. The agent is the primary consumer: it can compare the factual scope
against the question before accepting a query that merely runs cleanly.

If output size must be bounded, retain the scope report ahead of low-value
middle result rows. Scope entries should be subject to a separate generous
entry limit rather than disappearing through ordinary text truncation.

### JSON

JSON mode should add a `derived_value_scopes` field to each query result. An
empty list is valid. Existing fields and result rows remain unchanged.

## Reported values

Report every unique aggregate or window computation that:

- is selected directly;
- is referenced by a selected scalar expression;
- is used by a `where`, `having`, or `order by` expression; or
- feeds another reported aggregate or window through a named value or rowset.

Do not emit a separate entry for aliases that resolve to the same planned
computation. Prefer the user-authored alias as the display name. Otherwise use
the selected concept address, followed by a deterministic generated name only
when no stable authored name exists.

Rowset-derived intermediate values are represented through `input values` and
`input grain`; they do not need standalone entries unless they themselves are
aggregates or windows.

## Scope model

Introduce a dialect-independent diagnostic model associated with the processed
query, conceptually:

```python
class DerivedValueScope:
    name: str
    kind: Literal["aggregate", "window"]
    expression: str
    input_filters: list[str]
    input_grain: list[str]
    group_by: list[str]
    partition_by: list[str]
    order_by: list[ScopeOrder]
    output_filters: list[str]
    input_values: list[str]
```

The final implementation must use the project's normal typed model style; the
shape above defines the contract, not a required class location.

### `input_filters`

Conditions that restrict rows before the aggregate or window is evaluated.
This must reflect effective planner placement after normalization, pushdown,
rowset expansion, and WHERE dual-scope handling.

Repeated equivalent filters should be deduplicated. Render `none` when the
computation sees the unrestricted available population.

### `input_grain`

The grain of values entering the computation when that information is useful
and known. It is especially important for nested aggregates: an average of
per-customer totals should state that its input grain includes the customer,
even though its own `group by` does not.

For a direct aggregate over fact rows, omit an unhelpful implementation-level
fact grain rather than printing dozens of physical keys.

### `group_by`

For aggregates, the semantic grouping keys. Render `*` for a scalar aggregate.
This is distinct from incidental keys carried by a datasource or internal
stitch.

### `partition_by` and `order_by`

For windows, report the semantic partition and ordered sequence, including
direction and null ordering when explicitly or effectively defined.

### `output_filters`

Conditions that restrict the rows carrying the already-computed value. This
includes a final-year filter applied after a window and qualifying conditions
applied to an aggregate result.

Only include a condition here when the planner can establish that it is outside
the computation boundary. Do not classify an ambiguous condition by textual
proximity.

### `input_values`

Names of directly consumed derived values, useful for nested computations. For
example, `state_average` consumes `customer_return_total`. Omit the field when
the computation consumes only root or row-scalar values.

## Rendering format

Use a line-oriented format rather than a table. Scopes have different fields,
and long conditions are easier to read when wrapped beneath labels.

Fields appear in this stable order:

1. `kind`
2. `expression`
3. `input values`
4. `input filters`
5. `input grain`
6. `group by` for aggregates
7. `partition by` and `order by` for windows
8. `output filters`

Omit fields that are inapplicable, except `input filters`, which should render
`none` when empty. Preserve authored concept names where possible, but render
normalized conditions so semantically identical plans produce equivalent
diagnostics.

Do not include ANSI styling in the underlying serialized form. Terminal
rendering may style headings and labels.

## Extraction architecture

Scope extraction belongs after author statements have been built and scope
normalization has run, but before dialect SQL rendering erases semantic
boundaries. It must operate on the same build/processed objects used to create
the executable query.

Recommended separation:

1. A core extractor walks reachable selected/filtering/ordering concepts and
   creates typed `DerivedValueScope` records.
2. The processed query carries those records as diagnostic metadata, or exposes
   enough stable build metadata for a single shared extractor.
3. CLI and agent integrations serialize/render those records; they do not walk
   concept lineage themselves.

Do not derive scope by parsing generated SQL. SQL CTE boundaries and planner
stitches are implementation artifacts and may no longer map cleanly to the
semantic computation.

The extractor must be cycle-safe and deduplicate by planned computation
identity, not display name.

## Important semantic cases

### Scoped aggregate followed by an outer filter

```preql
with customer_totals as
where returns.date.year = 2002
select
    returns.customer.sk,
    returns.location.state,
    sum(returns.amount) as customer_total
;

with state_norms as
select
    customer_totals.state,
    avg(customer_totals.customer_total) as state_average
;
```

If the final query filters current customer state to `GA`, the diagnostic for
`state_average` must not include that restriction unless it actually entered
the average's planned input.

### Window boundary population

If December 1998 and January 2000 exist only to supply boundaries for windows,
the window should report the full boundary range under `input filters` and the
final `year = 1999` condition under `output filters`.

### Same expression in WHERE and SELECT

WHERE dual-scope normalization may create distinct planned computations from
one authored-looking expression. If their effective scopes differ, emit two
entries with disambiguated names such as `(filter scope)` and `(output scope)`.
Do not merge solely because expression text matches.

### Rollups

For an aggregate using rollup, preserve the authored rollup hierarchy in
`group by`. Do not expand it into every generated grouping-set arm. A window
over the rollup output should report the rollup output as its input grain and
its own partition/order independently.

### Filtered aggregate arguments

For `sum(amount ? condition)`, report the condition as part of the aggregate's
input scope or expression consistently. Prefer `input filters` when the
condition controls row membership for the value; it must not be confused with
a statement-level filter affecting peer values.

## Failure behavior

Diagnostics are observational and must never prevent query execution. If scope
extraction cannot fully describe a value:

- emit the known fields;
- represent an unknown field explicitly in structured output;
- optionally render `unknown` in text; and
- record a debug log entry.

Do not silently label an unknown scope as `none` or unrestricted.

## Acceptance criteria

1. `trilogy run query.preql --scope` prints normal results followed by one
   deterministic scope block per query statement.
2. Agent run results include the same block without requiring authored query
   changes.
3. JSON mode exposes the same typed information without parsing text.
4. A nested group-average test reports both the inner grain and outer grouping.
5. A lag/lead boundary test distinguishes the window input range from the final
   output-year restriction.
6. A WHERE dual-scope test reports two computations when their effective scopes
   differ.
7. Inline, aliased, and environment-named spellings with the same effective
   plan produce equivalent scope records apart from preferred display names.
8. Rollup diagnostics preserve the semantic hierarchy rather than listing SQL
   implementation arms.
9. Enabling diagnostics executes no extra database queries and does not change
   generated SQL or query results.
10. Existing CLI output remains unchanged when diagnostics are disabled.

## Suggested test matrix

- scalar aggregate grouped by `*`;
- aggregate by one and several keys;
- filtered aggregate argument versus statement WHERE;
- nested average of grouped totals;
- rank with partition and explicit null ordering;
- lag/lead with boundary rows removed only after the window;
- window incorrectly scoped by an inner filter, proving diagnostics report the
  actual bad plan rather than the desired plan;
- WHERE dual-scope aggregate and window cases;
- rowset alias chains and repeated consumption of one derived value;
- rollup aggregate followed by a window;
- hidden aggregate used only by HAVING or ORDER BY;
- several selects in one file;
- text, JSON, and agent-tool rendering.

## Follow-up possibilities

After the factual report is stable, separate proposals may add execution
cardinalities or rule-based warnings. Those features should consume this scope
model rather than changing its semantics. Intent inference is explicitly not a
prerequisite for this diagnostic to be useful.
