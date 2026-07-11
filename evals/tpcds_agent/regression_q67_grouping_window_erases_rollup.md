# Regression: grouping-derived window partition erases ROLLUP

## RESOLUTION (2026-07-10)

Not reproducible at any committed state (b76e8fbf9, 8bd33d1bc, b0302b74a all
pass `test_window_partition_by_grouping_level_over_rollup`). The observed
failure matches the transient state of an in-flight uncommitted rework in the
shared working tree: the select-scoped grouping-stamp fix (for the cross-
statement stamp-leak family) briefly had its stamped clone clobbered by the
finalize SELECT loop, which produced exactly this symptom — zero ROLLUP tokens,
leaf rows only. The completed rework (select-local stamp clones +
`grouping_stamped_locals` riding `as_lineage`) restores the single-ROLLUP shape
and all report variants pass:

- report minimal repro: 1 ROLLUP, 0 joins, 6 rows
- partition by `grouping(g1)` directly / CASE over grouping / named bucket:
  1 ROLLUP, 0 joins (new coverage in
  `tests/engine/test_duckdb_rollup_window_inline_aggregate.py::test_grouping_derived_partition_keeps_rollup`)
- cross-statement stamp-leak family:
  `tests/parsing/test_rollup_select_scoped_stamping.py`

Lesson for shared-tree regression reports: check whether the failure reproduces
at HEAD before attributing it to a landed change — another session's
uncommitted WIP is indistinguishable from a regression in the working tree.

## Summary

The post-q67 rollup/window fixes removed the previous stitch-join fanout, but a
new silent wrong-result regression now drops the non-standard grouping itself.

When a window partitions by a concept derived from `grouping(...)`, a query
authored with `by rollup (...)` generates a plain `GROUP BY` and returns only
leaf rows. Subtotals and the grand total disappear without an error.

This is not limited to TPC-DS q67: the repository's existing regression test
`test_window_partition_by_grouping_level_over_rollup` now fails.

## q67 symptom

Latest replay artifact:

```text
evals/tpcds_agent/results/20260709-105517_enriched/workspace/query67.preql
```

Relevant shape:

```preql
auto sum_sales <- sum(coalesce(ss.sales_price, 0) * coalesce(ss.quantity, 0));
auto g_cat <- grouping(ss.item.category);
auto cat_group <- case
    when g_cat = 1 then '~grand_total~'
    else ss.item.category
end;
auto rnk <- rank(
    ss.item.category,
    ss.item.class,
    ss.item.brand_name,
    ss.item.product_name,
    ss.date.year,
    ss.date.quarter,
    ss.date.month_of_year,
    ss.store.id
) over (partition by cat_group order by sum_sales desc);

select
    ss.item.category,
    ss.item.class,
    ss.item.brand_name,
    ss.item.product_name,
    ss.date.year,
    ss.date.quarter,
    ss.date.month_of_year,
    ss.store.id,
    sum_sales,
    rnk
where ss.date.year = 2000 and ss.store.id is not null
by rollup (
    ss.item.category,
    ss.item.class,
    ss.item.brand_name,
    ss.item.product_name,
    ss.date.year,
    ss.date.quarter,
    ss.date.month_of_year,
    ss.store.id
)
having rnk <= 100;
```

The generated aggregate CTE contains:

```sql
grouping(item.I_CATEGORY) AS g_cat,
sum(...) AS sum_sales
...
GROUP BY
    month, quarter, year, brand, category, class, product_name, store
```

There is no `ROLLUP`, `CUBE`, or `GROUPING SETS` token anywhere in the generated
SQL. `grouping(category)` evaluates as zero for every ordinary group, so the
window sees only leaf rows.

Candidate row zero remains a leaf:

```text
(NULL, NULL, NULL, NULL, 2000, 3, 8, <store>, 8684.62, 75)
```

The reference begins with a genuine rolled-up row:

```text
(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 104996.99, 72)
```

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
''';

auto total <- sum(v);
auto gg <- grouping(g1);
auto bucket <- case when gg = 1 then '~total~' else g1 end;
auto r <- rank(g1, g2) over (
    partition by bucket
    order by total desc
);

select g1, g2, total, r
by rollup (g1, g2);
```

Expected six rows:

```text
(NULL, NULL, 35, ...)
('a', NULL, 30, ...)
('b', NULL, 5, ...)
('a', 'x', 10, ...)
('a', 'y', 20, ...)
('b', 'x', 5, ...)
```

Actual result contains only the three leaves. Generated SQL has zero `ROLLUP`
tokens.

The failure is unchanged by clause placement or a simple WHERE predicate:

| Variant | ROLLUP tokens | Rows |
|---|---:|---:|
| No WHERE | 0 | 3 |
| WHERE before SELECT | 0 | 3 |
| WHERE after SELECT | 0 | 3 |
| Partition directly by `grouping(g1)` | 0 | 3 |
| Partition by CASE derived from `grouping(g1)` | 0 | 3 |

## Existing regression now failing

Command:

```text
.venv/Scripts/python.exe -m pytest \
  tests/engine/test_duckdb.py::test_window_partition_by_grouping_level_over_rollup \
  tests/engine/test_duckdb_rollup_grouping_identity.py -q
```

Result:

```text
F..  1 failed, 2 passed
```

Failure:

```text
test_window_partition_by_grouping_level_over_rollup
actual:   3 leaf rows
expected: 6 leaf/subtotal/total rows
```

The two tests in `test_duckdb_rollup_grouping_identity.py` still pass. Their
measure is a composite derived aggregate (`sum(np) / sum(esp)`) and their window
partition includes explicit level/parent concepts. The failing existing test and
q67 use a direct aggregate measure. This difference is useful for locating the
bucket/co-sourcing regression.

## Relationship to the previous q67 bug

The prior handoff is:

```text
evals/tpcds_agent/handoff_q67_inline_window_aggregate_rollup_identity.md
```

That bug generated a real ROLLUP, then rejoined window and aggregate outputs on
nullable visible dimensions, causing many-to-many duplication.

The current candidate avoids the old trigger by ordering the window with the
selected aggregate alias. There is no stitch join fanout. Instead, the grouping
mode is downgraded before rendering and the ROLLUP never exists.

These are distinct failure modes:

| Failure | ROLLUP emitted? | Stitch join? | Symptom |
|---|---|---|---|
| Previous q67 | Yes | Yes | Duplicated/mismatched ranks and sums |
| Current regression | No | No | Only leaf rows; subtotals/total missing |

## Likely root area

The v4 group graph contains explicit machinery intended to preserve this shape:

```text
trilogy/core/processing/v4_helper/group_graph.py:318
    _fold_rollup_key_dims
trilogy/core/processing/v4_helper/group_graph.py:1045
    _widen_window_grain_to_grouping_parent
```

`_fold_rollup_key_dims` identifies non-standard grouping through concept
`grouping_mode`. In the failing direct-aggregate shape, the aggregate/grouping
concept bucket reaches rendering with standard grouping even though the authored
aggregate was stamped by `by rollup`. The downstream window widening cannot
restore a grouping mode that has already been lost.

Investigate where the direct aggregate, `grouping()` output, and grouping-derived
partition concept are bucketed/co-sourced. Compare the group facts and native
grain against the passing composite-measure test.

Also inspect the v3 group/window path because the public engine must preserve the
same contract regardless of planner selection:

```text
trilogy/core/processing/node_generators/group_node.py
trilogy/core/processing/node_generators/window_node.py
```

## Required fix

1. Preserve `AggregateGroupingMode.ROLLUP` from select finalization through
   group bucketing and rendering when `grouping()` feeds a downstream window.
2. Ensure folding/co-sourcing a `grouping()`-derived BASIC concept cannot replace
   or normalize the parent aggregate's non-standard grouping mode to standard.
3. Assert that every selected `grouping()` expression is rendered in a query
   whose GROUP BY is actually ROLLUP/CUBE/GROUPING SETS.
4. If that invariant fails internally, raise before rendering rather than
   silently evaluating `grouping()` over ordinary GROUP BY.
5. Preserve the previous no-stitch-join identity fix; restoring ROLLUP must not
   reintroduce the earlier fanout.

## Regression coverage

The already-failing
`test_window_partition_by_grouping_level_over_rollup` should be the first gate.
Add SQL-shape assertions:

```python
assert sql.upper().count("ROLLUP") == 1
assert len(rows) == 6
```

Add variants for:

- direct aggregate versus composite derived aggregate;
- partition directly by `grouping(g1)`;
- partition by CASE over `grouping(g1)`;
- selected versus hidden grouping-level concept;
- ROLLUP, CUBE, and explicit GROUPING SETS;
- real NULL values in grouping columns;
- q67's eight-level rollup with rank/HAVING.

For all variants assert both the single grouped SQL shape and equality with a
hand-written DuckDB oracle.
