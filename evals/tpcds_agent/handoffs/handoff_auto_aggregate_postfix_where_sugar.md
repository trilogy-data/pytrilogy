# Handoff — support postfix `where` on derived aggregate concepts

## Summary

Extend Trilogy so an aggregate concept definition can express its input-row
filter after the aggregate:

```trilogy
auto overall_avg_sales <- avg(ss.quantity * ss.list_price)
    where ss.date.year between 1999 and 2001;
```

This should be syntax sugar for the existing filtered-aggregate form:

```trilogy
auto overall_avg_sales <- avg(
    (ss.quantity * ss.list_price) ? ss.date.year between 1999 and 2001
);
```

The verbose spelling is natural, especially when the aggregate expression or
predicate is long. There is no need to force authors to introduce a one-column
rowset merely to scope a reusable aggregate.

Discovered in the latest TPC-DS q14 trajectory. The agent wrote the intended
input population correctly but hit an operator-oriented parse error at `where`.

## Current symptom

```trilogy
import raw.store_sales as ss;

auto overall_avg_sales <- avg(
    ss.quantity * ss.list_price
) where ss.date.year between 1999 and 2001;
```

Current error:

```text
Parse error:
  --> 5:3
   |
 5 | ) where ss.date.year between 1999 and 2001;
   |   ^---
   = expected ... aggregate_over, or window_sql_over
```

The parser accepts the aggregate and optional `by`/window tails, then has no
legal derivation tail beginning with `where`.

## Required semantics

The postfix predicate filters rows **before** the aggregate is computed. It is
not a predicate on the completed aggregate value and is not equivalent to a
query-level `HAVING` clause.

For an aggregate `A(expr)` and predicate `p`:

```trilogy
auto x <- A(expr) where p;
```

must lower to the same author/build representation and generated SQL as:

```trilogy
auto x <- A(expr ? p);
```

In particular, preserve the aggregate's ordinary null behavior. For example:

```trilogy
auto x <- avg(quantity * list_price) where year = 2001;
```

must exclude rows outside 2001, while `avg` continues to ignore null products
inside 2001. It must not inject `coalesce`, zeros, or a count-changing CASE
fallback.

The sugar should work with an explicit aggregate grain as well:

```trilogy
auto yearly_avg <- avg(amount) by customer.id where date.year = 2001;
```

This must be equivalent to:

```trilogy
auto yearly_avg <- avg(amount ? date.year = 2001) by customer.id;
```

An aggregate without `by` must remain responsive to the consuming query grain;
the postfix filter must not accidentally freeze it at a scalar grain.

## Scope

Start narrowly with derived aggregate concepts:

```trilogy
auto|metric|property name <- <aggregate> [by ...] where <conditional>;
```

Supporting postfix `where` on arbitrary scalar or window expressions is not
required by this handoff. Reject or retain existing behavior for ambiguous
non-aggregate forms rather than silently assigning them new semantics.

All built-in aggregate functions accepted by `aggregate_functions` should be
covered, including `count`, `count_distinct`, `sum`, `avg`, `min`, `max`,
`stddev`, `variance`, `array_agg`, `bool_and`, `bool_or`, and `any`.

## Likely implementation areas

Both parser backends must accept the syntax:

- `trilogy/parsing/trilogy.lark`: `concept_derivation` currently ends at
  `conditional`; add an unambiguous optional postfix aggregate-filter clause.
- `trilogy/scripts/dependency/src/trilogy.pest`: mirror the grammar change so
  Rust and Python parsing remain behaviorally identical.
- `trilogy/parsing/v2/concept_syntax.py` and
  `trilogy/parsing/v2/rules/concept_rules.py`: hydrate the optional predicate
  and lower it into the aggregate's input expression as a `FilterItem` with a
  `WhereClause`.
- The legacy parser transformer/rules need the equivalent lowering if they do
  not share the v2 hydration path.
- `trilogy/parsing/render.py`: render the accepted spelling consistently and
  ensure parse/render/parse retains the filter.

Do not implement this by wrapping the completed `AggregateWrapper` in a generic
filter without verifying planner semantics. The desired representation is the
same one produced by `avg(expr ? predicate)`: the predicate scopes the
aggregate argument/input rows.

Prefer a distinct syntax node such as `derivation_where` over relying on child
positions or raw token inspection. This will keep metadata parsing and future
derivation tails stable.

## Regression coverage

Add parser, semantic, SQL-generation, and execution tests for:

1. The minimal `avg(x) where year = 2001` example parses in both backends.
2. Postfix `where` and `avg(x ? predicate)` produce identical rows.
3. Generated SQL applies the predicate before aggregation.
4. Null aggregate inputs remain null/ignored according to the aggregate; no
   implicit zero substitution occurs.
5. `count(grain(order_id, item_id)) where predicate` matches the existing
   filtered-count form.
6. `count_distinct`, `sum`, `min`, and `max` accept the same spelling (a parser
   parameterization is sufficient where semantics share the same path).
7. An explicit `by` grain followed by `where` parses and matches the `?` form.
8. A responsive aggregate without `by` still inherits its consuming grain.
9. Compound predicates (`and`, `or`, `between`, `is null`) bind wholly to the
   postfix filter.
10. Existing concept derivations, query-level `where`, aggregate `by`, window
    `over`, and filtered aggregates using `?` remain unchanged.
11. Parse/render/parse preserves the predicate and its meaning.
12. Invalid postfix `where` on a non-aggregate derivation receives a targeted
    authored error if the grammar can recognize it, rather than being silently
    reinterpreted.

## Acceptance example

Given rows:

| year | quantity | list_price |
|---:|---:|---:|
| 2001 | 2 | 10 |
| 2001 | null | 50 |
| 2000 | 100 | 100 |

this definition:

```trilogy
auto avg_2001 <- avg(quantity * list_price) where year = 2001;
select avg_2001;
```

must return `20`, exactly like:

```trilogy
auto avg_2001 <- avg((quantity * list_price) ? year = 2001);
select avg_2001;
```

The 2000 row is outside the input population and the null 2001 product is
ignored by `avg`.

## Non-goals

- Do not change the meaning of `?`.
- Do not make postfix `where` a completed-value/HAVING filter.
- Do not add implicit `coalesce` behavior.
- Do not change rowset or select-level `where` syntax.
- Do not special-case TPC-DS models or q14.
