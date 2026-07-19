# Handoff — valid tuple `IN (select ...)` rejects multi-column subquery

## Symptom

TPC-DS q14 attempted a standard row-wise tuple membership test:

```trilogy
where
    (ss.item.brand_id, ss.item.class_id, ss.item.category_id)
    in (
        select
            common_tuples.brand_id,
            common_tuples.class_id,
            common_tuples.category_id
    )
select ...;
```

Trilogy rejects it with:

```text
a `(select ...)` subquery used as a scalar value or membership set must select
exactly one column; project only the key/value consumed by the outer expression
```

That restriction is correct for scalar subqueries and scalar membership, but
incorrect when the left operand is a tuple with the same arity as the subquery
projection.

## Expected behavior

Allow a multi-column `(select ...)` on the right of `IN` or `NOT IN` when the
left side is a tuple of exactly the same arity:

```trilogy
(a, b, c) in (select rs.a, rs.b, rs.c)
(a, b, c) not in (select rs.a, rs.b, rs.c)
```

These must be row-wise membership comparisons. A left tuple matches only when
one projected subquery row matches all positions together.

Do **not** lower this into independent predicates such as:

```trilogy
a in (select rs.a)
and b in (select rs.b)
and c in (select rs.c)
```

That admits cross-pairs assembled from different subquery rows and changes the
meaning.

Continue rejecting:

```trilogy
a in (select rs.a, rs.b)             # scalar vs two columns
(a, b) in (select rs.a)              # arity 2 vs 1
(a, b) in (select rs.a, rs.b, rs.c)  # arity 2 vs 3
```

The error for mismatched arity should report both sides, for example:

```text
Tuple membership arity mismatch: left side has 2 fields but the subquery
selects 3 fields.
```

## Root cause

`trilogy/parsing/v2/rules/rowset_rules.py::scalar_subquery` validates the
subquery before its enclosing comparison is known:

```python
if not isinstance(select, SelectStatement) or len(select.output_components) != 1:
    raise fail(... "must select exactly one column" ...)
```

It then returns one `SubqueryItem`, so the outer expression hydrator never gets
an opportunity to compare tuple arities or construct correlated row-wise
membership.

The grammar already permits `expr array_comparison scalar_subquery`; this is a
hydration/modeling restriction, not fundamentally a parse ambiguity.

## Implementation guidance

Make multi-output subquery hydration aware of its use as the RHS of a
membership comparison. Reasonable approaches include:

1. hydrate `left operator scalar_subquery` as a specialized comparison path,
   finalizing the subquery select and validating its outputs against the left
   tuple before creating the comparison; or
2. return an intermediate subquery-result object carrying every projected
   concept, then let the enclosing comparison convert it to a scalar
   `SubqueryItem` or a row tuple after context and arity are known.

Likely touch points:

- `trilogy/parsing/v2/rules/rowset_rules.py::scalar_subquery`
- `trilogy/parsing/v2/rules/expression_rules.py`, where
  `SubselectComparison` is constructed
- author/build comparison models if the existing tuple-membership RHS cannot
  retain a projected rowset with multiple correlated outputs
- SQL generation for tuple `IN` / `NOT IN`
- rendering/round-trip support for the authored form

Reuse the existing row-tuple membership machinery where possible. Named-rowset
tuple membership already has the desired correlation semantics:

```trilogy
(a, b, c) in (rs.a, rs.b, rs.c)
```

The inline-select form should lower to the same semantic shape, with all RHS
concepts tied to one anonymous rowset.

Do not simply remove the single-column check globally. A multi-column subquery
is still invalid in scalar arithmetic, scalar comparison, function arguments,
and scalar `IN` contexts.

## Null semantics

Preserve Trilogy's existing authored tuple-membership identity semantics,
including its current handling of null tuple components. Inline-select tuple
membership should behave identically to membership against the equivalent
named rowset; it should not accidentally inherit a database-specific SQL
three-valued-logic difference.

## Regression coverage

Add parser and execution tests for:

1. `(a, b) in (select rs.a, rs.b)` parses and executes.
2. Three-column tuple membership, matching the q14 construct, works.
3. `NOT IN` is the exact complement under Trilogy's existing tuple/null rules.
4. Correlation is preserved: RHS rows `(1, 2)` and `(3, 4)` must not match
   left row `(1, 4)`.
5. Inline-select tuple membership returns the same rows as the equivalent
   named-rowset tuple membership.
6. Nullable tuple components match the named-rowset behavior.
7. Scalar `x in (select rs.x)` remains valid.
8. Scalar vs multi-column RHS is rejected with an arity-specific error.
9. Tuple vs too-few and too-many RHS columns are rejected with both arities.
10. A multi-column subquery used as a scalar value remains rejected.
11. Aliased and computed select outputs work positionally.
12. Parse/render/parse preserves the inline tuple-subquery form.
13. Both parser backends either support the construct consistently or the
    implementation documents why the restriction exists only after shared CST
    construction; do not leave backend-dependent behavior.

## Minimal execution fixture

```trilogy
key id int;
property id.a int;
property id.b int;

datasource values (
    id: id,
    a: a,
    b: b
)
grain (id)
query '''select 1 id, 1 a, 2 b union all select 2, 3, 4''';

rowset pairs <- select a, b;

# Must return true for (1, 2), false for (1, 4).
select
    (1, 2) in (select pairs.a, pairs.b) as present,
    (1, 4) in (select pairs.a, pairs.b) as cross_pair_absent;
```

## Source artifact

The failure came from the q14 trajectory at roughly 5.0M prompt tokens. The
rejected production query used three fields on each side:

```trilogy
(ss.item.brand_id, ss.item.class_id, ss.item.category_id)
in (select common_tuples.brand_id,
           common_tuples.class_id,
           common_tuples.category_id)
```

This was a valid expression of the intended tuple membership and should not
have forced the agent to restructure the query.
