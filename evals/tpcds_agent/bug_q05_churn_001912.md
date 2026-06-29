# q05 churn / silent failure — run 20260629-001912 (809k tokens, failed, no exit_code:1 sentinel)

## TL;DR
q05's submitted query **ran clean (exit 0) and produced numerically-correct
leaf rows**, but returned the **wrong 100-row window**: it returned 100 catalog
leaf rows and **silently dropped the rollup grand-total + per-channel subtotal
rows** that the reference surfaces first. So the eval diff failed even though
every aggregate value was right.

The obstacle is **not a codegen/resolution bug** — `by rollup (...)` + plain
`NULLS FIRST` ordering reproduces the reference EXACTLY (proof below). The
obstacle is the bundled **rollup syntax example** (`trilogy/ai/syntax_examples.py:534-573`),
which teaches `order by _level asc, ...` (leaves-first). Under `LIMIT 100` with
>100 leaves, leaves-first ordering pushes the subtotal/grand-total rows past the
limit window and silently drops them — the exact opposite of a "nulls first"
(totals-first) requirement. This is the task's own hypothesis #5: *"an
order-by/limit interaction dropping rollup rows."*

## What the agent did
- Used a correct `union(...)` of a sales arm + a returns arm, then
  `by rollup (channel, entity)` — semantically sound; leaf values match the
  reference to the cent.
- Copied the rollup example's ordering verbatim: it projected
  `--grouping(channel)+grouping(entity) as _level` and wrote
  `order by _level asc, combined.channel asc nulls first, combined.entity asc nulls first limit 100`.
- It never tried a plain nulls-first order; it went straight to the example's
  `_level asc` idiom (msg 38 tried `grouping()` directly in ORDER BY → correct
  framework error "ORDER BY contains aggregate", msg 44 switched to the `_level`
  hidden column per the example). Submitted believing 835 entities + correct
  numbers = correct (msg 50).

## Minimal repro (workspace model + tpcds.duckdb)
Same query, only the ORDER BY differs. 835 leaf entities exist.

Agent's `order by _level asc, channel nulls first, entity nulls first limit 100`:
```
catalog channel | catalog_pageAAAAAAAAAAABAAAA | 163753.93 | 4645.17 | -36282.28
catalog channel | catalog_pageAAAAAAAAAABBAAAA |      0.00 |  326.05 |   -191.31
... 100 catalog LEAF rows; NO grand total, NO subtotals ...
```
Plain `order by channel asc nulls first, entity asc nulls first limit 100`
(no `_level`):
```
<null>          | <null>                       | 112458734.69 | 3255243.12 | -31584085.44   <- grand total
catalog channel | <null>                       |  38544639.26 | 1083573.98 |  -4396139.07   <- subtotal
catalog channel | catalog_pageAAAAAAAAAAABAAAA |    163753.93 |    4645.17 |    -36282.28
...
```
Reference SQL (`tests/modeling/tpc_ds_duckdb/query05.sql`) first rows:
```
(None, None, 112458734.70, 3255243.12, -31584085.44)   grand total
('catalog channel', None, 38544639.28, 1083573.98, -4396139.07)  subtotal
('catalog channel','catalog_pageAAAAAAAAAAABAAAA',163753.93,4645.17,-36282.28)
```
=> the plain-nulls-first trilogy output is **row-for-row identical** to the
reference. The `_level asc` output is a disjoint row set → diff fail.

## Root cause (file:line)
- `trilogy/ai/syntax_examples.py:534-573` — the `rollup` example body:
  - line 560: `--grouping(enroll.department) + grouping(enroll.course) as _level`
  - line 562: `order by _level asc, department asc nulls first, course asc nulls first`
  - lines 567-569 NOTE: *"SORT BY LEVEL (leaves, then subtotals, then grand total)"*
  - line 563: `limit 100`
  The example itself is a footgun: any rollup with >100 leaves + this idiom
  drops its own subtotal/grand-total rows under the limit. There is no caveat
  that leaves-first + LIMIT hides the rollup rows, and no example of the common
  "totals first" (`order by dim nulls first`) form that a "nulls first" spec
  wants.
- Codegen verified correct: `by rollup` grouping sets, NULL subtotal markers,
  and `NULLS FIRST` sorting all behave exactly like the SQL reference. No fix
  needed in the planner/dialect.

## Classification
Misleading bundled guidance (syntax example) → silent wrong-result via an
order-by/limit interaction that drops rollup total rows. NOT a codegen or
resolution bug. Distinct from the already-filed explore token-bloat
(handoff_explore_conformed_dimension_dedup.md), which explains the *size* of the
run; this explains why it *failed* rather than merely being expensive.

## Suggested direction (not applied)
In the `rollup` example, lead with the totals-first form
(`order by department asc nulls first, course asc nulls first` — no `_level`),
and demote `_level asc` to a NOTE that explicitly warns: with `LIMIT`, sorting
leaves-first hides the subtotal/grand-total rows; put rolled-up (NULL) rows
first when the spec says "nulls first".
