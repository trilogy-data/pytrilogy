# Handoff: q67 inline window aggregate loses ROLLUP row identity

## Summary

There is a recurring silent wrong-result bug when a window over a `ROLLUP`
orders by an inline aggregate expression that is equivalent to a selected
aggregate:

```preql
select
    ...,
    sum(v) as total,
    rank(g1, g2) over (partition by g1 order by sum(v) desc) as r
by rollup (g1, g2);
```

The selected `sum(v) as total` and the inline window-order `sum(v)` are planned
as separate aggregate concepts. The window branch carries the hidden aggregate
but not `total`; the final assembly recovers `total` with a null-safe join on
the visible rollup dimensions. Those dimensions do not uniquely identify a
grouping-set row: a source NULL and a NULL injected by ROLLUP are visibly
identical. The join therefore fans out subtotal/grand-total rows and associates
aggregate values with ranks from other grouping levels.

TPC-DS q67 produced 100 rows on both sides but a different first page. The
candidate repeated the all-null row with sales `104996.99` under ranks 1, 2, 3,
4, and 72; the reference contains it once, at rank 72.

This is a gap in the existing rollup/window identity fix, not a new conceptual
class. The full fix should make inline-equivalent and alias-referenced window
aggregates share the same grouping output and preserve grouping-set identity
through every planner path.

## q67 trigger

Candidate artifact:

```text
evals/tpcds_agent/results/20260709-105517_enriched/workspace/query67.preql
```

Relevant shape:

```preql
select
    ss.item.category as category,
    ss.item.class as class,
    ss.item.brand_name as brand,
    ss.item.product_name as product_name,
    ss.date.year as year,
    ss.date.quarter as quarter_of_year,
    ss.date.month_of_year as month_of_year,
    ss.store.id as store_code,
    sum(coalesce(ss.sales_price, 0) * coalesce(ss.quantity, 0)) as summed_sales,
    rank(category, class, brand, product_name, year, quarter_of_year,
         month_of_year, store_code)
        over (
            partition by category
            order by sum(coalesce(ss.sales_price, 0) * coalesce(ss.quantity, 0)) desc
        ) as within_category_rank
by rollup (category, class, brand, product_name, year, quarter_of_year,
           month_of_year, store_code)
having within_category_rank <= 100;
```

The canonical workaround references the selected aggregate alias:

```preql
sum(coalesce(ss.sales_price * ss.quantity, 0)) as sumsales,
rank() over (partition by ss.item.category order by sumsales desc) as rk,
by rollup ();
```

## Generated SQL symptom

The candidate generates one ROLLUP CTE containing both a displayed and hidden
copy of the same aggregate:

```sql
sum(...) AS _virt_agg_sum_1829515134007898,
sum(...) AS summed_sales
```

The window CTE keeps `_virt_agg_sum_...` and the visible dimensions, but drops
`summed_sales`. Final assembly joins the window rows back to the rollup CTE on
all eight visible dimensions:

```sql
FROM uneven
INNER JOIN cooperative
  ON uneven.brand IS NOT DISTINCT FROM cooperative.brand
 AND uneven.category IS NOT DISTINCT FROM cooperative.category
 AND uneven.class IS NOT DISTINCT FROM cooperative.class
 ...
 AND uneven.year IS NOT DISTINCT FROM cooperative.year
```

No grouping-set identity participates in this join. Multiple ROLLUP rows with
the same visible null tuple match each other many-to-many.

## Minimal reproduction

```preql
key id int;
property id.g1 string?;
property id.g2 string?;
property id.v int;

datasource t (id: id, g1: g1, g2: g2, v: v)
grain (id)
query '''
select 1 id, 'a' g1, 'x' g2, 10 v
union all select 2, 'a', 'y', 20
union all select 3, 'b', 'x', 5
union all select 4, null, null, 7
''';

auto total <- sum(v);

select
    g1,
    g2,
    total,
    rank(g1, g2) over (
        partition by g1
        order by sum(v) desc
    ) as r
by rollup (g1, g2)
order by g1 nulls first, g2 nulls first, total nulls first, r nulls first;
```

Current result has 14 rows instead of the correct 8. The bad all-null portion
is:

```text
(NULL, NULL,  7, 1) x2
(NULL, NULL,  7, 2) x4
(NULL, NULL, 42, 1) x1
(NULL, NULL, 42, 2) x2
```

The correct grouping-set rows are:

```text
(NULL, NULL,  7, 2)  # leaf with data NULLs
(NULL, NULL,  7, 2)  # g1 subtotal for the NULL group
(NULL, NULL, 42, 1)  # grand total
```

The first two rows are intentionally visibly identical outputs but are distinct
grouping-set rows. They must not cross-match during internal assembly.

## Trigger matrix

Using the same four-row fixture:

| Window ordering | ROLLUP CTEs | Assembly joins | Rows | Result |
|---|---:|---:|---:|---|
| `order by total desc` | 1 | 0 | 8 | Correct |
| `order by sum(v) desc` | 1 | 1 | 14 | Wrong fanout |

Without the source row containing actual NULL grouping keys, the inline form
can appear correct by accident, although it still emits the unnecessary join.
That is why the regression fixture must contain data NULLs and must assert SQL
shape as well as row values.

## Existing partial fixes

This family already has targeted machinery and tests:

- `trilogy/core/processing/node_generators/window_node.py:32`
  `resolve_window_parent_concepts` carries aggregate grain keys through a
  window to avoid non-unique join-backs.
- `trilogy/core/processing/node_generators/window_node.py:128-141` threads
  explicit grouping-identity concepts through the window parent.
- `trilogy/core/processing/v4_helper/group_graph.py:1045`
  `_widen_window_grain_to_grouping_parent` widens a window from partition grain
  to its rollup parent's emitted grain.
- `tests/engine/test_duckdb.py:959`
  `test_window_over_rollup_preserves_grouping_rows` covers the alias form.
- `tests/engine/test_duckdb_rollup_grouping_identity.py` covers explicit
  `grouping()` level columns and asserts a single ROLLUP with no stitch join.

Those tests pass because the window orders by the selected aggregate concept
(`total`/`gross_margin`). They do not cover a structurally equivalent aggregate
spelled inline in the window ordering.

## Likely root cause

The inline aggregate becomes a hidden `_virt_agg_*` concept distinct from the
selected aggregate alias. Window planning treats the selected aggregate as a
non-equivalent optional output:

- `trilogy/core/processing/node_generators/window_node.py:84`
  `gen_window_node`
- `trilogy/core/processing/node_generators/window_node.py:143`
  computes `non_equivalent_optional`
- `trilogy/core/processing/node_generators/window_node.py:233`
  calls `gen_enrichment_node`, producing the lossy join-back

Aggregate co-sourcing/equivalence is recognized in group planning around
`trilogy/core/processing/node_generators/group_node.py:192-243`, but the
equivalence does not survive or is not consulted when the window's inline
aggregate and the selected alias are assembled.

The current protection focuses on carrying grain keys. That is necessary but
not sufficient for non-standard grouping: the visible key values are not a
unique row identity across grouping sets. Either the equivalent aggregates
must co-source so no rejoin occurs, or any unavoidable rejoin must also carry
the per-dimension `GROUPING(...)` bits / grouping ID.

## Full-fix requirements

1. **Canonicalize equivalent aggregates.** A selected aggregate and an
   identical inline aggregate used by a window should resolve to one materialized
   grouping output, regardless of aliasing or syntactic placement.
2. **Keep selected sibling measures in the window parent/output.** Do not enrich
   them afterward when they share the same non-standard grouping pass.
3. **Treat grouping-set identity as grain.** Any internal join between outputs
   of a ROLLUP/CUBE/GROUPING SETS pass must include hidden grouping identity,
   not only nullable displayed dimensions.
4. **Cover both planner paths.** The v3 window/group/enrichment path and v4
   group-graph widening/co-sourcing path should produce the same one-pass shape.
5. **Do not paper over with authored `grouping()` columns.** Identity must be
   internal and automatic even when the user does not request hierarchy-level
   outputs.

Preferred SQL shape is a single grouped/windowed pipeline with no join back to
the ROLLUP output, matching the alias form and hand-written SQL oracle.

## Regression tests

Add the minimal fixture above next to
`test_window_over_rollup_preserves_grouping_rows` and pin:

```python
assert sql.upper().count("ROLLUP") == 1
assert " JOIN " not in sql.upper()
assert got == oracle_rows
assert len(got) == 8
```

Test at least these variants:

1. Selected `sum(v) as total`; window orders by inline `sum(v)`.
2. Selected `sum(coalesce(a, 0) * coalesce(b, 0))`; window repeats that exact
   composite expression (q67 shape).
3. Window uses multi-key `rank(g1, g2)` and a coarser partition.
4. Source contains actual NULLs in every rollup dimension.
5. ROLLUP, CUBE, and explicit GROUPING SETS.
6. Two different selected measures to ensure only truly equivalent aggregates
   coalesce.

Also add q67's candidate form as a model-level regression and compare all rows
to a single-statement DuckDB oracle, not only the limited first 100.

## Scope note

The earlier q67 diagnosis in `diag_q67_enriched_20260706.md` concerned a
different candidate and correctly found model/question issues. The July 9
candidate analyzed here uses the requested `sales_price * quantity` measure and
known-store filter; its wrong rows are the planner fanout described above.
