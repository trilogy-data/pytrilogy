# Handoff: scope diagnostics dependency coverage and computation-role clarity

**Date:** 2026-07-13. **Status:** implemented 2026-07-13 (issues 1-3;
count-grain reports the counted argument's own grain rather than the
discovery-dependent row identity, which lineage extraction cannot know —
see `_record_aggregate`). Pending: A/B rerun. **Area:**
`trilogy/core/scope_diagnostics.py`, diagnostic serialization/rendering, and
tests in `tests/test_scope_diagnostics.py`.

## Post-implementation validation blocker (2026-07-13) — RESOLVED same day

Both blocker items are fixed: author traversal now switches to the owning
statement's name/filters/grain at every rowset/subquery boundary (anonymous
`_subquery_*` addresses redirect to their true owner, or report nothing rather
than something wrong), and the count-identity fact moved to a distinct
`argument_grain` field (option 2; row grain is discovery-dependent and not
reported). Regression: `test_having_subselect_benchmark_owned_by_its_rowset`,
`test_count_family_reports_argument_grain`.

## Original blocker report

The first implementation correctly fixes q81 role clarity and q95 membership
rowset coverage, but it is **not ready to rebaseline**. Running the historical
q14 treatment-1 candidate through the current extractor produces an inaccurate
scope for the scalar average rowset consumed from another rowset's HAVING.

Authored structure:

```preql
with overall_avg as
where sales.date.year between 1999 and 2001
select avg(sales.quantity * sales.list_price) as overall_avg_val;

with filtered_groups as
where sales.date.year = 2001 and sales.date.month_of_year = 11
select ... sum(...) as total_sales
having total_sales > (select overall_avg.overall_avg_val);
```

Actual diagnostic:

```json
{
  "name": "filtered_groups._overall_avg_overall_avg_val",
  "expression": "avg(sales.quantity * sales.list_price)",
  "role": "upstream",
  "input_row_filters": [
    "sales.date.year = 2001",
    "sales.date.month_of_year = 11",
    "... qualifying tuple membership ..."
  ],
  "group_by": ["channel", "brand_id", "category_id", "class_id"]
}
```

Every important field is wrong except the expression. It must instead be:

```json
{
  "name": "overall_avg.overall_avg_val",
  "expression": "avg(sales.quantity * sales.list_price)",
  "role": "upstream",
  "input_row_filters": ["sales.date.year between 1999 and 2001"],
  "group_by": ["*"]
}
```

Likely cause: author-side dependency traversal reaches the subselect's aggregate
while retaining the consuming `filtered_groups` rowset name, filters, and grain.
A subselect/rowset boundary must switch to the referenced statement's own name
and context before visiting its computations. Do not propagate the consumer's
`input_row_filters` or `select.grain` across that boundary.

The existing `test_scalar_benchmark_rowset_reported_from_having` is too shallow:
the final statement directly consumes the benchmark. Add the exact nested
shape above: final rollup consumes `filtered_groups`; `filtered_groups.HAVING`
consumes `overall_avg` through an expression subselect. Assert the average is
owned by `overall_avg`, has its 1999–2001 filter, and is scalar.

### Count-grain label is also not yet the requested fact grain

For q14's wrong `count(sales.order_id)`, current output says:

```json
"input_grain": ["sales.order_id"]
```

That is the counted argument's identity, not the aggregate's input-row grain.
The relevant fact grain is `(channel, order_id, item.sk)`. Calling the former
`input_grain` can make the wrong count look correct. Either:

1. report the actual input relation grain; or
2. rename this field to `argument_grain` and separately report row grain.

Add a regression where `order_id` repeats across multiple item lines. Assert
the diagnostic distinguishes the counted argument grain from the fact input
grain and makes no correctness claim.

## Summary

The first TPC-DS A/B evaluation of derived-value scopes improved pass rate from
27/40 (67.5%) to 30/40 (75.0%). It produced direct successful corrections for
window-boundary mistakes, especially q47 and q57.

An audit of mixed/negative q14, q81, and q95 trajectories found no evidence
that the extractor invents false scopes. It did find two systemic shortcomings:

1. Derived computations reached through subqueries and membership rowsets are
   missing, so agents claim scopes validate logic that the report never showed.
2. WHERE dual-scope entries expose accurate facts but label them ambiguously
   enough that agents repeatedly rationalize an incorrect benchmark as correct.

These are diagnostic defects, not query-engine semantic defects. Do not change
WHERE dual-scope behavior as part of this work.

## A/B artifacts

Runs:

```text
evals/tpcds_agent/results/scope_ab_20260713-123400_baseline_1..5
evals/tpcds_agent/results/scope_ab_20260713-123400_treatment_1..5
```

Primary audited logs:

```text
treatment_1/agent_log.q14.jsonl
treatment_1/agent_log.q95.jsonl
treatment_4/agent_log.q81.jsonl
treatment_5/agent_log.q81.jsonl
```

Final candidates are under each run's `workspace/queryNN.preql`.

## Issue 1: upstream rowset/subquery computations are omitted

### q14: scalar benchmark and tuple qualification disappear

The query has three independent computation stages:

1. `qualifying_tuples.channel_count` over 1999–2001;
2. `overall_avg.overall_avg_val` over 1999–2001;
3. November 2001 leaf totals filtered against the average, then rolled up.

The treatment-1 report includes the leaf and rollup totals, but omits both
upstream stages. The leaf qualification renders as:

```text
_filtered_groups_total_sales
  > <Subquery: ref:_subquery_37_19.overall_avg.overall_avg_val>
```

The opaque subquery token does not state that the average sees 1999–2001 rather
than November 2001. `qualifying_tuples.channel_count` is also absent, so the
report cannot establish that tuple eligibility sees all three channels in the
required period.

This contradicts the spec's reported-values rule: include a derived value when
it feeds another reported aggregate/window through a named value or rowset.

### q95: eligibility aggregates disappear behind membership

The candidate defines:

```preql
with eligible_orders as
select
    ws.order_number,
    count_distinct(ws.warehouse.sk) as warehouse_count,
    bool_or(ws.is_returned) as has_return
having
    warehouse_count > 1
    and has_return = true;
```

The final aggregate filters on:

```preql
ws.order_number in eligible_orders.order_number
```

The report contains only the final `count_distinct(order_number)`, shipping-cost
sum, and profit sum. It omits `warehouse_count` and `has_return` entirely.

Agents nevertheless say things like:

> The scopes confirm the eligible_orders rowset is computed first with no
> report constraints.

The scopes do not confirm that; the agent is rereading its authored query.

### Required behavior

Reachability must traverse semantic dependencies introduced by:

- scalar expression subqueries;
- membership against a rowset or subquery;
- tuple membership;
- named rowset outputs consumed as filters;
- aggregates/windows feeding HAVING conditions inside those rowsets.

For q14, the report should contain separate entries equivalent to:

```text
overall_avg.overall_avg_val
  kind: aggregate
  expression: avg(quantity * list_price)
  input row filters: date.year between 1999 and 2001
  group by: *

qualifying_tuples.channel_count
  kind: aggregate
  expression: count_distinct(channel)
  input row filters: date.year between 1999 and 2001
  group by: brand_id, class_id, category_id
  output row filters: channel_count = 3
```

For q95, include:

```text
eligible_orders.warehouse_count
  expression: count_distinct(warehouse.sk)
  input row filters: NONE — unrestricted population
  group by: order_number
  output row filters: warehouse_count > 1 and has_return = true

eligible_orders.has_return
  expression: bool_or(is_returned)
  input row filters: NONE — unrestricted population
  group by: order_number
  output row filters: warehouse_count > 1 and has_return = true
```

The final aggregate may continue to show membership as an input filter, but it
should reference the named eligibility computation rather than leave its
definition unreported.

## Issue 2: WHERE dual-scope labels are materially misleading

### q81 failure shape

The business requirement is:

- total returns per `(customer, return state)` for year 2000;
- average those totals by return state over the complete benchmark population;
- compare totals to 1.2 times the average;
- only afterward restrict reported customers to current home state GA.

Treatment-4 authored the total without binding year 2000 into the aggregate:

```preql
auto cust_state_total <- sum(cs.return_amount_inc_tax)
    by cs.return_customer.sk, cs.return_address.state;

where
    cs.return_date.year = 2000
    and cs.return_address.state is not null
    and cust_state_total > 1.2 * state_avg
    and cs.return_customer.address.state = 'GA'
```

The report accurately exposes two planned values:

```json
{
  "name": "cust_state_total (filter scope)",
  "expression": "sum(cs.return_amount_inc_tax)",
  "group_by": ["return_address.state", "return_customer.sk"]
}
```

There is no `input_row_filters` field: this WHERE-gate computation is unrestricted
and therefore wrong for the requested year-2000 benchmark.

It also emits:

```json
{
  "name": "cust_state_total (output scope)",
  "input_row_filters": [
    "return_date.year = 2000",
    "return_address.state is not null",
    "sum(...) > 1.2 * avg(sum(...))",
    "return_customer.address.state = 'GA'"
  ]
}
```

The agent alternately reads the first entry correctly, then treats the second
entry as proof that the benchmark saw year 2000 and GA did not contaminate it.
Treatment-5 repeats this with only `is_returned = true` bound into the filter
scope; year 2000 remains absent, but the agent accepts the query.

### Why the current labels fail

`(filter scope)` and `(output scope)` describe internal address splitting, not
the semantic role a user needs to reason about. Both entries use the label
`input_row_filters`, even though they answer different questions:

- value used to decide row admission in WHERE;
- value recomputed over rows admitted to the SELECT output.

The output-scope list also contains the aggregate comparison itself alongside
ordinary row predicates. That makes a recursive aggregate gate look like a
normal source-population restriction.

Omitting an empty `input_row_filters` key compounds the problem. The agent prompt
says absence means unrestricted, but agents do not reliably make that
inference.

### Required behavior

Give each planned computation an explicit role field. Suggested structured
values:

```json
"role": "where_gate"
"role": "selected_output"
```

Suggested display names:

```text
cust_state_total — used by WHERE comparison
cust_state_total — selected output after row admission
```

Do not rely on a suffix embedded in `name` as the only role signal.

Always serialize `input_row_filters`, including an empty list:

```json
"input_row_filters": []
```

Render the empty case loudly:

```text
input row filters: NONE — unrestricted population
```

Separate input row filters from conditions that admit already-computed values.
The current `output_row_filters` field is appropriate for the latter; an aggregate
comparison must not also appear indistinguishably among ordinary input row filters
unless it truly becomes part of that computation's effective input and the
boundary is made explicit.

At minimum, the q81 failed shape should make this contrast unavoidable:

```text
cust_state_total — used by WHERE comparison
  input row filters: NONE — unrestricted population
  group by: return state, customer sk

cust_state_total — selected output after row admission
  input row filters: year = 2000, return state recorded, home state = GA
  admitted by: cust_state_total > 1.2 * state_average
```

The report remains factual. This does not require an intent-based warning.

## Issue 3: direct aggregate grain is useful for count diagnosis

Most q14 treatment failures were not filter-placement errors. They used
`count(order_id)` or `count(sale_line_item_counter)` where the benchmark needs
a sale-line count. Trilogy `count(k)` counts distinct non-null values, and a
constant/repeated counter is not a line identity.

The report accurately shows expressions such as:

```text
filtered_groups.line_item_count
  expression: count(sales.order_id)
```

It does not show the direct input grain. The spec currently permits omitting a
fact grain when it is unhelpful; here it is load-bearing because the requested
line identity is `(channel, order/ticket, item sk)`.

Do not turn this feature into a general warning engine. Instead, include a
compact direct `input_grain` for count/count-distinct computations when known.
That gives the agent factual evidence to compare the counted expression with
the available row identity.

This is lower priority than Issues 1 and 2 because it did not create a false
scope report.

## q95 role-selection misses are not scope inaccuracies

The failing q95 candidates generally used:

```preql
ws.ship_customer.address.state = 'IL'
```

instead of the sale's recorded shipping address:

```preql
ws.ship_address.state = 'IL'
```

The diagnostic faithfully displays the authored wrong concept. Aggregate input
scope cannot decide which business role the user intended. Do not add
intent-based role warnings as part of this handoff.

The missing eligibility scopes remain a real coverage issue, but they would not
by themselves have corrected these specific q95 results.

## Implementation direction

1. Build the initial reachable set from selected, WHERE, HAVING, and ORDER BY
   computations as today.
2. Expand reachability through subquery statements and rowset membership
   dependencies before extracting entries.
3. Deduplicate using planned computation identity, not expression text or alias.
4. Preserve authored rowset-qualified names when available.
5. Add a typed computation role to `DerivedValueScope`; do not encode the role
   only in `name`.
6. Make serialization schema-stable: applicable list fields should serialize as
   empty lists rather than disappearing when empty.
7. Render computation role and unrestricted input explicitly.
8. Keep extraction best-effort and observational; failures must not block query
   execution.

Do not parse generated SQL to recover these relationships. The needed edges
exist in author/build/processed query structures before dialect rendering.

## Regression tests

### Dependency traversal

Add focused unit tests for:

1. Scalar aggregate rowset referenced through an expression subquery.
2. Aggregate rowset used through single-column membership.
3. Two aggregate values in an eligibility rowset used through membership.
4. Tuple membership where the source rowset has an aggregate HAVING gate.
5. Nested rowset chain: reported aggregate → membership rowset → aggregate
   benchmark rowset.

Assert every upstream aggregate appears once, with its own filters and grain.
Assert no opaque `<Subquery: ...>` is the only representation of a reported
dependency.

### Dual-scope rendering

Use a small `(entity, partition, segment, amount)` dataset:

```text
entity  partition  segment  amount
1       A          keep     20
2       A          other    100
```

Define a partitioned benchmark in WHERE and select the same aggregate while
filtering output to `segment = 'keep'`.

Assert:

- two records exist when their effective computations differ;
- one has `role == "where_gate"`;
- one has `role == "selected_output"`;
- empty `input_row_filters` serializes as `[]`;
- text renders `NONE — unrestricted population`;
- the aggregate comparison is displayed as an admission/output condition, not
  silently mixed with ordinary source filters;
- inline, aliased, and environment-named spellings retain equivalent records.

### Count input grain

At line grain `(order, item)`, compare diagnostics for:

- `count(order)`;
- `count(item)`;
- `count(line_flag)` where the flag is constant/repeated;
- `sum(line_flag)`.

Assert the diagnostic includes the same semantic input grain for each and does
not itself claim which expression is correct.

## Acceptance criteria

1. q14 reports the independent overall average and channel-count tuple
   qualification, including their 1999–2001 filters.
2. q95 reports warehouse-count and has-return eligibility computations with
   unrestricted inputs.
3. q81's unrestricted WHERE-gate aggregate is explicit in both JSON and text.
4. q81's two computations have typed, human-readable roles.
5. Empty list fields are stable in JSON rather than omitted.
6. Existing scope values and generated query results are unchanged.
7. No intent-based warnings are introduced.
8. Existing scope diagnostic tests plus the new dependency/role matrix pass.

After implementation, rerun the same eight-query 5× A/B. The key behavioral
checks are fewer q81 rationalizations, continued q47/q57 window lift, and agent
reasoning that cites actual q14/q95 upstream entries rather than claiming an
absent scope was verified.
