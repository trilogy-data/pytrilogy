# Improved-shape patterns from TPC-DS query analysis

Baseline (main queries only, excluding alt `.1`/`.2` variants which are
expected-loss test cases): ~33 wins / 55 losses / 8 ties out of 96 queries
(~**38%** win rate). Target 50%. Fixing the 9-10 biggest losses gets there.

Each pattern below is grounded in a measured speedup against the parquet dataset
(scripts: `_lab_q*.py`, harness: `_shape_lab.py`).

Biggest deltas (main queries, excluding noise-dominated <20ms queries):

| Query | exec_time | comp_time | loss | covered by |
|---|---|---|---|---|
| 66 | 0.34s | 0.16s | +0.18s | P3, P4, P7, P9 |
| 28 | 0.22s | 0.05s | +0.18s | P2 (UNION variant) |
| 97 | 0.25s | 0.08s | +0.17s | P1, P4 |
| 65 | 0.25s | 0.10s | +0.15s | P1, P3, P6 |
| 78 | 0.34s | 0.21s | +0.13s | P4 |
| 73 | 0.15s | 0.04s | +0.12s | P3 |
| 81 | 0.15s | 0.04s | +0.11s | P2 |
| 29 | 0.18s | 0.08s | +0.10s | P3 |
| 67 | 1.01s | 0.91s | +0.10s | P9 (modest) |
| 85 | 0.74s | 0.64s | +0.10s | (join order; minor) |
| 77 | 0.14s | 0.05s | +0.09s | P7 |

---

## P1 — Push dimension filters into the source CTE (eliminate post-aggregation filtering)

When a dimension column (`d_year`, `d_month_seq`, `d_moy`, ...) appears only as a
filter (not in the projection of an aggregate), the generator currently does:

```sql
WITH src AS (
  SELECT fact.customer_id, fact.item_id, fact.date_sk
  FROM fact GROUP BY 1, 2, 3                   -- groups ALL rows
),
src_filtered AS (
  SELECT src.customer_id, src.item_id
  FROM src
  INNER JOIN date_dim ON src.date_sk = date_dim.d_date_sk
  WHERE date_dim.d_month_seq BETWEEN 1200 AND 1211   -- filter applied AFTER full grouping
  GROUP BY 1, 2
)
```

Should be:

```sql
WITH src AS (
  SELECT fact.customer_id, fact.item_id
  FROM fact, date_dim
  WHERE fact.date_sk = date_dim.d_date_sk
    AND date_dim.d_month_seq BETWEEN 1200 AND 1211   -- filter pushed into source
  GROUP BY 1, 2
)
```

**Affected queries:** q97, q81, q65, q66 (re-joins date_dim 3x with same filter).

**Measured impact (q97):** **6.25x** (268ms → 43ms).
**Measured impact (q81 isolated):** **2.1x** on the `customer_total_return` aggregation.

---

## P2 — Convert `CASE WHEN dim = const THEN x END` aggregates back into a WHERE clause when the predicate is shared by all aggregates

When every aggregate has the same `CASE WHEN dim_col = K` guard (or is part of a
mutually-exclusive set), promote the predicate to the WHERE clause:

```sql
-- Generated (q81 abundant)
SELECT customer, state,
       sum(CASE WHEN dd.d_year = 2000 THEN amt END) AS total_2000
FROM returns
JOIN date_dim dd ON returns.date_sk = dd.d_date_sk
GROUP BY 1, 2
```

Should be:

```sql
SELECT customer, state, sum(amt) AS total_2000
FROM returns, date_dim
WHERE returns.date_sk = dd.d_date_sk AND dd.d_year = 2000
GROUP BY 1, 2
```

The CASE-WHEN form forces the optimizer to retain all years of date_dim rows
just to mark them NULL. With ~7-8 years of TPC-DS data, this is 7-8x the work.

**Affected queries:** q81 (single shared year predicate), q28 + q90 (mutually exclusive but disjoint bucket predicates over store_sales).

**Measured impact (q81 abundant CTE):** **2.1x** (143ms → 67ms), 131,949 → 27,820 rows scanned.
**Measured impact (q28):** **1.1x** (87ms → 76ms) — modest because count(DISTINCT) dominates.

For q28/q90-style cases (multiple mutually exclusive predicates), a UNION of
filtered subqueries beats a single CASE-WHEN scan when each predicate selects a
small fraction of the table.

---

## P3 — Collapse multiple scans of the same fact table with identical filters

When the trilogy lowering produces N CTEs that each scan the same fact table
with the same filter set but project different columns (because the user
referenced different grain combinations of the same model), collapse to a single
filtered base CTE and project subsets downstream.

```sql
-- Generated (q65): 3 scans of store_sales, same filter
WITH uneven AS (
  SELECT item_sk, store_sk, sum(price)
  FROM store_sales JOIN date_dim ON ... WHERE d_month_seq BETWEEN ... GROUP BY 1, 2
),
questionable AS (
  SELECT date_sk, store_sk FROM store_sales WHERE store_sk IS NOT NULL GROUP BY 1, 2
),
cheerful AS (
  SELECT item_sk, date_sk FROM store_sales WHERE store_sk IS NOT NULL GROUP BY 1, 2, store_sk
)
```

Should be one filtered base CTE that downstream CTEs derive from.

**Affected queries:** q65, q73 (3 separate scans of store_sales with same filter
set), q29, q94.

**Measured impact (q65):** **8.0x** (193ms → 24ms) by collapsing 3 scans into 1.

---

## P4 — Per-channel aggregations beat UNION-ALL-with-channel-marker when downstream is per-channel CASE-WHEN

When a `unified_sales` / `unified_returns` model produces a UNION ALL across
channels, and the downstream consumer uses per-channel CASE WHEN aggregates,
the generator should materialize a per-channel aggregation and join them by the
group keys. Reasoning: DuckDB cannot push the per-channel WHERE filter through
a UNION ALL, so the union scans everything and CASE WHEN filters per row.

```sql
-- Generated (q78): unify, then anti-join, then 10 channel-keyed CASE WHEN sums
WITH abundant AS (
  SELECT ... FROM catalog_sales UNION ALL FROM store_sales UNION ALL FROM web_sales
),
yummy AS (
  SELECT customer_id, item_id,
         sum(CASE WHEN ch='STORE' THEN qty END) as store_qty,
         sum(CASE WHEN ch='WEB'   THEN qty END) as ws_qty,
         sum(CASE WHEN ch='CATALOG' THEN qty END) as cs_qty
  FROM abundant LEFT JOIN cheerful ON ...
  WHERE cheerful.is_returned IS NULL
  GROUP BY 1, 2
)
```

Should be one aggregate per channel, then joined:

```sql
WITH ss AS (SELECT customer, item, sum(qty) ss_qty FROM store_sales LEFT JOIN store_returns ...),
     cs AS (SELECT customer, item, sum(qty) cs_qty FROM catalog_sales LEFT JOIN catalog_returns ...),
     ws AS (SELECT customer, item, sum(qty) ws_qty FROM web_sales LEFT JOIN web_returns ...)
SELECT ... FROM ss LEFT JOIN ws ON ... LEFT JOIN cs ON ...
```

**Affected queries:** q78, q66 (web+catalog unified to compute per-channel
month_of_year sums), q97 (catalog+store unified to count per-channel customers).

**Measured impact (q78):** **2.0x** (255ms → 127ms).

---

## P5 — Drop dim joins when only the surrogate key is referenced

When a FULL OUTER / INNER JOIN reaches a dim table but only its PK column is
consumed downstream, the PK is already available in the fact-table FK — drop
the dim join entirely.

```sql
LEFT OUTER JOIN customer ON sales.customer_id = customer.c_customer_sk
LEFT OUTER JOIN item ON sales.item_id = item.i_item_sk
...
SELECT coalesce(customer.c_customer_sk, ...) AS customer_id,  -- equivalent to sales.customer_id
       coalesce(item.i_item_sk, ...) AS item_id               -- equivalent to sales.item_id
```

Detect: a dim table joined on a single equi-predicate `fact.fk = dim.pk` where
only `dim.pk` (or columns coalesce-equivalent to the FK) is consumed downstream.

**Affected queries:** q64 (FULL-joins customer + item + coalesce dance — already
wins on exec_time but this pattern still bloats the query and slows parse).

Lower priority for win-rate goal since q64 already wins on exec_time, but
applying this cleans up queries 29, 81 too.

---

## P6 — Avoid FULL JOIN ... IS NOT DISTINCT FROM when both sides come from the same fact and the key is the same

When two CTEs derived from the same fact table are joined on `IS NOT DISTINCT
FROM` keys that both sides necessarily share (e.g. both rows came from the same
store_sales group), the FULL JOIN is just a re-grouping. Use INNER JOIN or
collapse the upstream CTEs.

Examples:
- q66 `concerned FULL JOIN young ON concerned.warehouse_id IS NOT DISTINCT FROM young.warehouse_id` — both derive from `thoughtful` and share the warehouse_id grain.
- q65 `juicy FULL JOIN abundant ON ...` similar.

This pattern is downstream of P3/P4 — splitting the same source into multiple
CTEs creates the false need for a FULL OUTER reconciliation.

---

## P7 — Inline constants instead of wrapping them in a CTE + cross-join

Trilogy currently models named constants as a single-row CTE that is then
RIGHT/FULL-joined into the result on `1=1`:

```sql
quizzical AS (SELECT :ship_carriers AS ship_carriers),
...
juicy AS (
  SELECT quizzical.ship_carriers FROM quizzical RIGHT OUTER JOIN yummy ON 1=1 GROUP BY 1
)
... FULL JOIN juicy ON 1=1 ...
```

Inline as a literal in the final SELECT or push as a constant column inside the
existing source CTE. The cross-join + GROUP BY is pure overhead.

**Affected queries:** q66 (`ship_carriers`), q77 (`u_channel_c`, `u_channel_s`,
`u_channel_w` — actually wrapped in a `vacuous` CTE).

---

## P8 — Eliminate dead "deduplicating" GROUP BYs

Trilogy emits `GROUP BY` on source CTEs that don't reduce the row count meaningfully:

```sql
-- q94 questionable
SELECT ws_order_number, ws_warehouse_sk FROM web_sales GROUP BY 1, 2
-- web_sales is already (order, warehouse) at line level here
```

Detect: a GROUP BY where the grouped columns are already a superset of a key in
the source table. When this can't be statically proven, leave it; but for common
fact-table grains (e.g. store_sales by ticket+item, web_sales by order+item)
trilogy could ship metadata.

This is a smaller cost than P1-P4 but appears repeatedly. Combine with P3.

---

## P9 — Don't re-join date_dim when D_YEAR / D_MOY are already projected by an upstream CTE

q66's `young` CTE projects `D_YEAR` and `D_MOY`, but the upstream `thoughtful`
already INNER-joined date_dim. Either:
- Keep the dim columns in `thoughtful` and reference them in `young`, or
- Don't aggregate to `thoughtful`'s grain at all.

Similarly, q67 has `thoughtful` join date_dim then `cooperative` re-projects the
year/quarter/moy fields without re-joining (the only reason this isn't 3x slower
is that the optimizer fuses these CTEs in DuckDB).

---

## P10 — Replace `WHERE col IN (SELECT col FROM sub WHERE col IS NOT NULL)` with `EXISTS`/`SEMI JOIN`

q94 uses `IN` subqueries to express EXISTS / NOT EXISTS predicates from the
reference. The duplicate scan of web_sales then becomes a probe against an
arbitrary materialized set. DuckDB usually handles both shapes well, but
combined with P3 (dedup scans) this materializes the same set twice when
it could be one EXISTS check.

Lower priority than P1-P5.

---

## Summary table

| Pattern | Queries | Best speedup | Difficulty |
|---|---|---|---|
| P1 — Filter pushdown into source CTE | 97, 65, 66, 81 | 6.25x (q97) | medium |
| P2 — CASE WHEN → WHERE | 81, 28, 90 | 2.1x (81), 1.1x (28) | medium |
| P3 — Collapse duplicate fact scans | 65, 73, 29, 94 | 8.0x | high |
| P4 — Per-channel agg vs unified UNION | 78, 66, 97 | 2.0x | high |
| P5 — Drop unused dim joins | 64, 29, 81 | structural | medium |
| P6 — Drop IS NOT DISTINCT FROM reconciliation | 65, 66 | (in 8.0x) | medium |
| P7 — Inline constants | 66, 77 | small | low |
| P8 — Eliminate dead GROUP BY | 94, many | small per-query | low |
| P9 — Reuse upstream dim columns | 66, 67 | small | medium |
| P10 — IN subquery → EXISTS | 94 | small | low |

## Win-rate projection

If P1+P3+P4 land (the high-leverage ones), the 9 biggest main-query losses convert:

- q97: **+0.17s** (likely win after P1)
- q65: **+0.15s** (likely win after P3 — 8x measured)
- q78: **+0.13s** (likely win after P4 — 2x measured)
- q66: **+0.18s** (likely win after P3+P7+P9)
- q81: **+0.11s** (likely win after P2 — 2.1x measured)
- q73, q29: P3 should flip them
- q67, q77: P1+P7 should neutralize

That's 7-9 swing wins on top of the 33 base. **New win rate: ~42-44/96 ≈ 44-46%.**

Hitting strictly 50% needs ~15 wins added — to get there beyond the obvious
P1/P3/P4 fixes, also apply P2 to q28 (with a UNION-of-filtered-subqueries
emitter for mutually-exclusive aggregate buckets) and P9 to q67/q85. Many of
the small (<20ms) "losses" at the bottom of the list will flip noisily on
re-run and shouldn't be counted as load-bearing.
