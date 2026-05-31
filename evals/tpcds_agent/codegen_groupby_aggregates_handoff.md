# Codegen bug handoff: "GROUP BY clause cannot contain aggregates" on where-on-aggregate + ratio-of-aggregates

## Summary

A query that (a) filters on a derived aggregate in `where` AND (b) selects an
**arithmetic combination of two aggregates** (e.g. a ratio `a / b`) at the same
grain generates SQL that puts the aggregate expressions in the `GROUP BY`, which
DuckDB rejects:

```
(_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!
```

The Trilogy parses and resolves fine — this is a **SQL-generation** bug, not a
modeling error. Surfaced in the TPC-DS agent eval (q04, `20260530-194955`, the
multi-channel customer year-total ratio query).

## Minimal, self-contained repro

```trilogy
key id int;
property id.g string;
property id.v int;
datasource t (id, g, v)
  grain (id)
  query '''select 1 as id, 'a' as g, 10 as v
           union all select 2, 'a', 20
           union all select 3, 'b', 5''';

auto a <- sum(v ? v > 5) by g;
auto b <- sum(v ? v > 0) by g;
auto r <- a / b;

where a > 0
select g, r;
```

`trilogy run repro.preql duckdb` → `GROUP BY clause cannot contain aggregates`.

### Exactly what narrows it (tested, current code)

| query | result |
|---|---|
| `where a > 0 select g, a, b` (where-on-agg, raw aggregates, no ratio) | ✅ runs |
| `select g, r` (ratio, no where-on-agg) | ✅ runs |
| **`where a > 0 select g, r`** (where-on-agg **and** ratio) | ❌ GROUP BY error |

So **both** are required: a `where` predicate on a derived aggregate AND a
selected expression that combines aggregates (`a / b`). Either alone is fine.

## Where to look

- The where-on-aggregate predicate (`where a > 0`, semantically a HAVING) plus
  the ratio node appear to force the inner aggregates into the GROUP BY of the
  generated query instead of being computed in an aggregating CTE and referenced
  by alias.
- Likely in the query processor's grouping/CTE construction when an output
  column is an arithmetic expression over aggregates and a post-aggregate filter
  is present — the ratio's aggregate children should be materialized in the
  aggregate CTE, and the GROUP BY should list only the grain key (`g`), never the
  aggregate expressions.
- Compare the generated SQL for the ✅ and ❌ rows above to see where the
  aggregate leaks into GROUP BY.

## Related

- The same q04 family also triggers the cross-model recursion bug
  (`recursion_bug_handoff.md`) on a different draft. Different failure, likely
  adjacent grain/aggregate-resolution code.
- A sibling DuckDB binder error from the same eval, `column "channel" must
  appear in the GROUP BY clause` (q05), is probably the same root area (grouping
  set built incorrectly for a constant/aggregate mix) — worth checking together.

- Observed on `trilogy 0.3.275`, DuckDB dialect.
