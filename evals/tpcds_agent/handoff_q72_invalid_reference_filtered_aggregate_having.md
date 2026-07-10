# Handoff: q72 filtered aggregate + bare HAVING emits INVALID_REFERENCE_BUG

## Severity

Fatal framework bug. A query accepted by parsing/finalization reaches rendering
with a planned concept that has no source CTE. The renderer emits an
`INVALID_REFERENCE_BUG` sentinel and the CLI reports an unexpected `ValueError`.

This occurred during the live q72 replay in:

```text
evals/tpcds_agent/results/20260709-105517_enriched/agent_log.q72.jsonl
```

at approximately `2026-07-10T17:11:29Z` (`query72_check8.preql`).

## Reproduction

Against the enriched TPC-DS model:

```preql
import raw.catalog_sales as cs;
import raw.inventory as inv;

where
    cs.sold_date.year = 1999
    and cs.bill_household_demographic.buy_potential = '>10000'
    and cs.billing_customer_demographic.marital_status = 'D'
    and cs.days_to_ship > 5
    and inv.warehouse.sk is not null
    and inv.quantity_on_hand < cs.quantity

subset join cs.item.sk = inv.item.sk
subset join cs.sold_date.week_seq = inv.date.week_seq

select
    cs.item.desc,
    inv.warehouse.name,
    cs.sold_date.week_seq,
    count(cs.order_number) as total_orders,
    count(cs.order_number ? cs.promotion.sk is not null) as with_promo_orders
having
    cs.promotion.sk is null
limit 100;
```

The important combination is:

1. `cs.promotion.sk` is an input to a selected filtered aggregate.
2. The same raw concept is referenced by a bare finer-grain HAVING predicate.
3. The output grain does not project `cs.promotion.sk`.
4. The query also contains a scoped multi-source join.

## Actual failure

The inner grouped CTE correctly materializes the selected filtered aggregate as
a CASE expression:

```sql
CASE WHEN catalog_sales.CS_PROMO_SK IS NOT NULL
     THEN catalog_sales.CS_ORDER_NUMBER
     ELSE NULL
END AS _virt_filter_order_number_...
```

It does **not** project raw `CS_PROMO_SK`. The outer SELECT then renders:

```sql
WHERE
    INVALID_REFERENCE_BUG<Missing source reference to cs.promotion.sk> IS NULL
```

CLI error:

```text
Could not render the query: Missing source reference to cs.promotion.sk.
A planned reference has no backing source CTE...
```

This is not merely malformed SQL returned to the database; it is the framework's
internal sentinel escaping through an accepted query.

## Expected contract

The repository explicitly defines bare finer-grain HAVING references as a
post-aggregation semijoin:

```text
tests/test_having_resolution.py
```

In particular, `test_bare_dim_in_having_resolves_as_semijoin` requires a
non-output HAVING dimension to compile without `INVALID_REFERENCE_BUG` and to
render using an existence/membership subquery.

Therefore this query should either:

1. resolve `promotion.sk is null` as the documented semijoin over the SELECT
   grain, preserving the selected aggregates; or
2. if this precise combination is unsupported, fail during semantic validation
   with a clear authored-query error.

It must never reach rendering with a missing source reference.

## Trigger observations

| Variant | Result |
|---|---|
| Filtered count uses `promotion.sk`; bare HAVING also uses raw `promotion.sk`; raw concept not selected | `INVALID_REFERENCE_BUG` |
| Raw `promotion.sk` projected in the SELECT | No missing-reference sentinel |
| HAVING filters the selected aggregate alias instead | No sentinel (different semantics) |
| Move promotion predicate to WHERE | No sentinel, but changes the population feeding every aggregate |

The failure is not caused by the promotion column being absent from the root
datasource: the generated inner CASE reads `CS_PROMO_SK` successfully. It is
lost between grouped materialization and outer HAVING rendering.

## Likely root cause

HAVING finalization is intended to rewrite a finer non-output dimension to a
grain-key membership in:

```text
trilogy/parsing/v2/select_finalize.py:1680
    _rewrite_having_finer_dims_to_membership
```

The rewrite decides whether a HAVING reference is already allowed using the
SELECT's allowed addresses. Because `promotion.sk` is nested inside the selected
filtered aggregate, it is apparently treated as available/projected even though
only the CASE-derived virtual concept survives the group CTE. The membership
rewrite is skipped, and later validation also accepts the raw reference.

The group/query planner then materializes only the filtered aggregate input and
does not carry raw `promotion.sk`. Rendering finally fails at:

```text
trilogy/dialect/base.py:1401
```

where `safe_get_cte_value` cannot resolve the planned reference and creates the
sentinel.

Relevant downstream existence wiring:

```text
trilogy/core/processing/concept_strategies_v3.py:54
    append_existence_check
trilogy/core/query_processor.py:716,892
```

## Full-fix requirements

1. Distinguish a raw concept being an **operand** of a selected expression from
   that raw concept being a materialized SELECT output.
2. Rewrite the bare HAVING leaf to the documented semijoin even when the same
   concept appears inside a filtered aggregate.
3. Ensure the semijoin receives the original query WHERE and all scoped-join
   inputs, as required by the q44 boundary documented in
   `append_existence_check`.
4. Add a pre-render invariant: every planned condition reference must resolve
   to a source CTE. Raise a structured internal compiler error before dialect
   rendering if it does not.
5. Never expose `INVALID_REFERENCE_BUG` text through normal CLI execution.

## Regression tests

Extend `tests/test_having_resolution.py` with:

- one root datasource version;
- the two-datasource scoped composite-join version above;
- `is null` and `is not null` variants;
- filtered `count`, `sum`, and `count_distinct` variants;
- assertions that generated SQL contains a semijoin/membership;
- assertions that `INVALID_REFERENCE_BUG` is absent;
- execution against a small oracle fixture.

Also assert that projecting the raw HAVING dimension and filtering an aggregate
alias remain valid controls.

