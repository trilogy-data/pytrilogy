# Codegen bug handoff: "Parameter not supported in ORDER BY clause" when ordering by a constant column

## Summary

Ordering by a **constant-valued select column** in a query whose plan is built
from **multiple aggregate CTEs** (metrics at different grains, joined) renders
the constant as a **bound parameter** in the generated SQL and orders by it:

```sql
ORDER BY
    $1 asc,                       -- <- the constant `'store' as channel`
    "cooperative"."outlet" asc nulls first
```

DuckDB rejects this:

```
(_duckdb.Error) Parameter not supported in ORDER BY clause
```

The Trilogy is valid — this is a **SQL-generation** bug. Surfaced in the TPC-DS
agent eval (q05, `20260530-201159`), the multi-channel rollup that selects
`'store' as channel, ... order by channel`.

## Minimal, self-contained repro

```trilogy
key id int;
property id.sid string;
property id.iid string;
property id.v int;
datasource t (id, sid, iid, v)
  grain (id)
  query '''select 1 as id, 's1' as sid, 'i1' as iid, 10 as v
           union all select 2, 's1', 'i2', 20
           union all select 3, 's2', 'i1', 5''';

auto by_store <- sum(v) by sid;
auto by_item  <- sum(v) by iid;      -- second metric at a DIFFERENT grain

select 'store' as channel, sid, by_store, by_item
order by channel asc, sid asc nulls first;
```

`trilogy run repro.preql duckdb` → `Parameter not supported in ORDER BY clause`.

### What narrows it (tested, current code)

- **Constant select column that is ordered by** is necessary: `'store' as
  channel ... order by channel`.
- **A multi-CTE plan** is necessary: with a single aggregate grain the constant
  is inlined and `order by channel` works fine (`select 'store' as channel,
  sum(v) by sid as total order by channel` runs). Adding a second metric at a
  *different* grain (`by_item`) forces codegen to build two aggregate CTEs and
  join them — and in that shape the constant is bound as `$1` and leaks into the
  outer `ORDER BY`. The real q05 hits this via its 6 cross-channel merges.

## Where to look

- Constant/literal rendering in the SQL generator: a literal select column
  should be **inlined** in `ORDER BY` (or the ORDER BY should reference the
  output column by alias/position), never emitted as a bind parameter.
- The leak appears only in the join-of-CTEs path, so check how constants are
  carried through CTE boundaries vs. the single-CTE path that inlines correctly.
- A safe general fix: when an ORDER BY term resolves to a constant, drop it from
  the ORDER BY (ordering by a constant is a no-op) or inline the literal.

- Observed on `trilogy 0.3.275`, DuckDB dialect.
