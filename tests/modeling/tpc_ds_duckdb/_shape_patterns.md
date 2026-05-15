# Improved-shape patterns from TPC-DS query analysis

Baseline (main queries only, excluding alt `.1`/`.2` variants which are
expected-loss test cases): ~33 wins / 55 losses / 8 ties out of 96 queries
(~**38%** win rate). Target 50%.

Each pattern below is grounded in a measured speedup against the parquet dataset
(scripts: `_lab_q*.py`, harness: `_shape_lab.py`).

---

## Audit — 2026-05-15

Current state: **41 wins / 58 losses / 0 ties out of 99 main queries** (~41.4%).
Total exec_time 13.53s vs reference 61.77s — wins are still dominated by
q64 (-21.6s) and q72 (-25.2s), where reference SQL hits pathological cases.
q82 has emerged as a new third large win (-1.10s, looks like DuckDB plan drift
on the reference rather than a trilogy improvement).

Progress since the 2026-05-14 audit: **-1 win net** (q66 flipped back to a
loss), but two real shape improvements landed:
- **q75** flipped from +0.101s loss to a tie via an explicit `with ... MERGE ...
  align` rewrite (P12-style two-bucket collapse) — this is a query authoring
  change, not a generator change.
- **q73** improved from +0.146s → +0.099s after dropping the outer
  `sum(count(...))` wrapper. The unfiltered `cooperative` store_sales scan is
  still present (P3 still applies), but the inner-join now collapses to a clean
  ticket-number join (P6 progress for q73 specifically).
- **q78** dropped from +0.154s → +0.010s (near tie) — generator change, no
  preql edit.

### Pattern status

| Pattern | Status | Evidence (2026-05-15) |
|---|---|---|
| P1 — Filter pushdown into source CTE | **PARTIAL** | q66 still has the filter inside the source CTE. q97, q65, q81 unchanged — q97 still GROUP-BYs 4 cols on unfiltered fact before joining date_dim. |
| P2 — CASE WHEN → WHERE | **PARTIAL** | q28 *regressed* (+0.180 → +0.210); per-bucket CASE WHENs unchanged. q81, q44, q9 unchanged. UNION-of-filtered-subqueries variant not landed. |
| P3 — Collapse duplicate fact scans | **NOT LANDED** | q65 was rewritten in `query65.preql` to bundle item_revenue + all enrichment dim cols into a single GROUP BY, but the generator still emits 3 store_sales scans (`questionable`, `cheerful`, `uneven`) — confirming this needs to be fixed in the generator, not the preql. q73, q29, q94, q16, q44, q9 still scan the same fact 2–4× with overlapping filters. |
| P4 — Per-channel agg vs unified UNION | **NOT LANDED** | q78 improved to near-tie via something else (still unified-UNION shape downstream). q97 still hard loss. |
| P5 — Drop unused dim joins | not investigated | q64 still wins via outright optimizer luck (-21.6s). q97.1 still dominated by this. |
| P6 — Drop `IS NOT DISTINCT FROM` reconciliation | **PARTIAL** | q73 no longer FULL-joins on IS NOT DISTINCT FROM (now INNER JOIN on ticket_number). q65, q66, q17, q44 unchanged. |
| P7 — Inline constants | **LANDED** | q66, q77 emit constants directly in final SELECT. The single-row constant CTE + cross-join pattern is gone. |
| P8 — Eliminate dead GROUP BY | **NOT LANDED** | q97 still GROUPs on already-deduped channel-marker data. q73's unfiltered `cooperative` GROUP BY on 5 store_sales cols is still there. |
| P9 — Reuse upstream dim columns | **PARTIAL** | q66's `juicy` still re-joins date_dim; q67 unchanged. |
| P10 — IN subquery → EXISTS/SEMI | not investigated | q16 unchanged — still two materialized-CTE IN probes. |
| P11 — Suppress unused customer×item dim FULL JOINs (q97.1) | **NOT LANDED** | q97.1 slightly improved (+1.584 → +1.389) but still 18x reference SQL — dominates the alt-bucket total. |
| P12 — Collapse per-bucket aggregations on same CTE | **PARTIAL** (preql workaround) | q75 was rewritten to use `with year_pair as ... MERGE ... align` to express the two filtered aggregates as one preql statement. Generator still doesn't synthesize this from the auto-aggregate form; q9's 5-bucket case unchanged. |

### What flipped since the prior audit

| Query | exec_time | comp_time | loss/win | vs prior audit |
|---|---|---|---|---|
| q75 | 0.133s | 0.134s | -0.001 (tie) | improved from +0.101 (preql rewrite, P12-style) |
| q78 | 0.414s | 0.404s | +0.010 (near tie) | improved from +0.154 (generator) |
| q73 | 0.140s | 0.041s | +0.099 | improved from +0.146 (outer sum(count) removed) |
| q97.1 | 1.468s | 0.079s | +1.389 | improved from +1.584 but still alt-bucket dominator |
| q82 | 0.486s | 1.583s | **-1.097 (WIN)** | new large win; reference SQL slowed, trilogy unchanged |
| q66 | 0.249s | 0.209s | +0.040 (loss) | **regressed** from +0.003s win |
| q28 | 0.257s | 0.048s | +0.210 | **regressed** from +0.180 |
| q65 | 0.313s | 0.150s | +0.163 | **regressed** from +0.105 despite preql rewrite — generator emits same 3-scan shape |

### Refreshed headline-loss table (main only, >40ms loss)

Need 9 more wins on top of 41 to hit 50/99. The reachable wedges:

| Query | exec_time | comp_time | loss | covered by |
|---|---|---|---|---|
| 28 | 0.257s | 0.048s | +0.210 | P2 (UNION variant) — *regressed since audit* |
| 59 | 0.406s | 0.230s | +0.176 | not investigated (new entry in loss bucket) |
| 97 | 0.255s | 0.081s | +0.174 | P1 + P4 |
| 65 | 0.313s | 0.150s | +0.163 | P3 — generator-side, preql rewrite alone did not help |
| 16 | 0.149s | 0.020s | +0.129 | P3 + P10 |
| 67 | 1.106s | 0.984s | +0.122 | P9 (modest) |
| 85 | 0.834s | 0.723s | +0.112 | join order; minor |
| 81 | 0.154s | 0.049s | +0.105 | P2 |
| 34 | 0.158s | 0.054s | +0.104 | P3 (q17 family) |
| 25 | 0.166s | 0.068s | +0.099 | P3 (q17 family) |
| 17 | 0.172s | 0.073s | +0.099 | P3 + P6 |
| 73 | 0.140s | 0.041s | +0.099 | P3 (drop unfiltered `cooperative` scan) |
| 79 | 0.164s | 0.071s | +0.093 | not investigated |
| 30 | 0.126s | 0.051s | +0.075 | not investigated |
| 29 | 0.228s | 0.090s | +0.138 | P3 |
| 76 | 0.125s | 0.053s | +0.072 | not investigated |
| 44 | 0.103s | 0.033s | +0.070 | P2 (CASE WHEN store_sk=4 → WHERE on `thoughtful`) |
| 07 | 0.163s | 0.097s | +0.066 | not investigated |
| 35 | 0.307s | 0.244s | +0.063 | not investigated |
| 77 | 0.108s | 0.052s | +0.055 | P7 landed; remaining cost elsewhere |
| 99 | 0.106s | 0.061s | +0.045 | not investigated |
| 94 | 0.066s | 0.021s | +0.045 | P3 + P10 |
| 66 | 0.249s | 0.209s | +0.040 | regressed from win — investigate |

**Headline takeaways for next round:**
1. q66 silently flipped back to a loss — should be the first investigation
   target since it was a flagship "fixed" query at the prior audit.
2. q65 preql was rewritten to encourage P3 but the generator did not honor it
   — concrete evidence that P3 must be fixed in the generator pass, not
   per-query.
3. q75's `MERGE ... align` rewrite worked. Whether that pattern is the
   generator's responsibility (auto-aggregate → MERGE) is the real P12 question.

---

## P1 — Push dimension filters into the source CTE (eliminate post-aggregation filtering)

Status: **PARTIAL** (q66 source-CTE pushdown still present though q66 regressed
back to a loss for unrelated reasons; q97 / q65 / q81 unchanged).

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

**Affected queries (post-audit):** q97, q65, q81 still broken. q66 fixed.

**Measured impact (q97):** **6.25x** (268ms → 43ms).
**Measured impact (q81 isolated):** **2.1x** on the `customer_total_return` aggregation.

---

## P2 — Convert `CASE WHEN dim = const THEN x END` aggregates back into a WHERE clause when the predicate is shared by all aggregates

Status: **PARTIAL** (umbrella WHERE landed for q28; the UNION-of-subqueries
variant for mutually-exclusive buckets did not).

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

**Affected queries:** q81 (single shared year predicate), q28 + q90
(mutually exclusive bucket predicates over store_sales), q44 (single
shared store_sk = 4 predicate inside CASE WHEN — see `thoughtful` CTE),
q9 (5 buckets each with single mutex predicate).

**Measured impact (q81 abundant CTE):** **2.1x** (143ms → 67ms).
**Measured impact (q28):** **1.1x** (87ms → 76ms) — modest because count(DISTINCT) dominates.

For q28/q90-style cases (multiple mutually exclusive predicates), a UNION of
filtered subqueries beats a single CASE-WHEN scan when each predicate selects a
small fraction of the table.

---

## P3 — Collapse multiple scans of the same fact table with identical filters

Status: **NOT LANDED** — this is still the highest-leverage remaining pattern.

**New evidence (2026-05-15):** q65's preql was rewritten to bundle item_revenue
with all enrichment dim columns into a single `with store_item_revenue as`
GROUP BY. The generator still emits the same 3 store_sales scans (`questionable`,
`cheerful`, `uneven`) plus a FULL JOIN with `coalesce` keying, and q65 *regressed*
from +0.105s to +0.163s. **This is concrete evidence that P3 needs a generator
fix; preql-level rewrites alone do not collapse the scans.**

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

**Affected queries (audit):** q65, q73, q29, q94, q16, q5, q25, q34 (all the
"q17 family" — multi-CTE shapes over the same fact with overlapping filters),
plus q44 (3 store_sales scans) and q75 (4 scans of the same `juicy` CTE for
4 single-bucket aggregations).

**q73 note (2026-05-15):** the unfiltered `cooperative` store_sales scan is
still present, but the outer `sum(count(...) by ticket_number) by customer_id`
wrapper was removed from the preql and the FULL JOIN reconciliation became a
plain INNER JOIN on ticket_number. Net: +0.146s → +0.099s, but still loses.

**Measured impact (q65):** **8.0x** (193ms → 24ms) by collapsing 3 scans into 1.

---

## P4 — Per-channel aggregations beat UNION-ALL-with-channel-marker when downstream is per-channel CASE-WHEN

Status: **NOT LANDED** at the shape level, but q78 (the headline target) is now
near-tie (+0.010s vs +0.154s previously) — the generator change that improved
q78 didn't restructure the union but apparently improved its planning. q97
still hard loss.

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

**Affected queries:** q78, q97 (catalog+store unified to count per-channel customers).
q66 still has this shape but is no longer a loss.

**Measured impact (q78):** **2.0x** (255ms → 127ms).

---

## P5 — Drop dim joins when only the surrogate key is referenced

Status: not landed; low priority for win rate.

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
wins on exec_time but this pattern still bloats the query and slows parse),
q97.1 (see P11 — this is the primary driver of the 1.6s alt-variant regression).

---

## P6 — Avoid FULL JOIN ... IS NOT DISTINCT FROM when both sides come from the same fact and the key is the same

Status: **PARTIAL** — q73 no longer emits the FULL JOIN ... IS NOT DISTINCT FROM
reconciliation (now an INNER JOIN on ticket_number, see updated `zquery73.log`).
q65, q66, q17, q44 unchanged.

When two CTEs derived from the same fact table are joined on `IS NOT DISTINCT
FROM` keys that both sides necessarily share (e.g. both rows came from the same
store_sales group), the FULL JOIN is just a re-grouping. Use INNER JOIN or
collapse the upstream CTEs.

Examples:
- q66 `concerned FULL JOIN young ON concerned.warehouse_id IS NOT DISTINCT FROM young.warehouse_id` — both derive from `thoughtful` and share the warehouse_id grain.
- q65 `juicy FULL JOIN abundant ON uneven.store_sales_store_id IS NOT DISTINCT FROM abundant.store_sales_store_id` — still present in current `zquery65.log` despite the preql rewrite.
- q17 `juicy FULL JOIN concerned ON item_desc/item_name/store_state IS NOT DISTINCT FROM ...` — both sides come from store_sales+store_returns aggregates on the same keys.
- q73: **fixed** — now INNER JOIN on ticket_number.

This pattern is downstream of P3/P4 — splitting the same source into multiple
CTEs creates the false need for a FULL OUTER reconciliation.

---

## P7 — Inline constants instead of wrapping them in a CTE + cross-join

Status: **LANDED**.

Trilogy previously modeled named constants as a single-row CTE that was then
RIGHT/FULL-joined into the result on `1=1`:

```sql
quizzical AS (SELECT :ship_carriers AS ship_carriers),
...
juicy AS (
  SELECT quizzical.ship_carriers FROM quizzical RIGHT OUTER JOIN yummy ON 1=1 GROUP BY 1
)
... FULL JOIN juicy ON 1=1 ...
```

Current output (q66) inlines as `:ship_carriers as "ship_carriers"` in the
final SELECT; q77 inlines `:u_channel_w` etc. directly. The cross-join + GROUP BY
wrapper is gone.

Remaining concern: this contributed to q66 going from +0.18s loss → win. Any
regression here would re-open multiple queries — guard with a regression test.

---

## P8 — Eliminate dead "deduplicating" GROUP BYs

Status: **NOT LANDED**.

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

Confirmed in audit: q97's `thoughtful` GROUPs on the four columns of an already-
distinct UNION ALL marker output. q73 picked up a new redundant store_sales GROUP
BY in `cooperative`.

---

## P9 — Don't re-join date_dim when D_YEAR / D_MOY are already projected by an upstream CTE

Status: **PARTIAL**.

q66's `juicy` CTE still LEFT JOINs date_dim to read D_MOY, even though
`cheerful` (the source CTE) already INNER-joined date_dim. Either:
- Keep the dim columns in `cheerful` and reference them in `juicy`, or
- Don't aggregate to `thoughtful`'s grain at all.

Similarly, q67 has `thoughtful` join date_dim then `cooperative` re-projects the
year/quarter/moy fields without re-joining (the only reason this isn't 3x slower
is that the optimizer fuses these CTEs in DuckDB).

---

## P10 — Replace `WHERE col IN (SELECT col FROM sub WHERE col IS NOT NULL)` with `EXISTS`/`SEMI JOIN`

Status: not landed; **priority bumped** after audit.

q16 confirms this is a hot path: two `IN (SELECT col FROM cte WHERE col IS NOT NULL)`
probes against `quizzical` and `abundant`, run for both the `thoughtful` and
`concerned` CTEs (so each subquery is re-executed). q16's loss is +0.093s
against a 16ms reference — combining P3 (dedup the catalog_sales scans) with
P10 (single EXISTS) is likely to flip it.

q94 uses the same `IN` shape. DuckDB usually handles `IN` and `EXISTS` similarly,
but only when the right-hand side is a fresh subquery — when it's a materialized
CTE, the optimizer keeps it as a hash probe and pays the materialization cost
twice.

---

## P11 — Suppress full-outer customer×item Cartesian when grouping a unified per-customer-per-item count

Status: NEW — surfaced as the largest single regression in the audit (q97.1,
+1.58s vs reference's 0.107s).

q97.1 (alt variant of q97) emits this shape:

```sql
WITH
  quizzical AS (SELECT customer, item, date FROM catalog_sales GROUP BY 1,2,3),
  questionable AS (SELECT customer, item, date FROM store_sales GROUP BY 1,2,3),
  uneven AS (
    SELECT cs_date.d_month_seq, ss_date.d_month_seq,
           coalesce(customer.c_customer_sk, ...) || '-' || coalesce(item.i_item_sk, ...) AS customer_item,
           ...
    FROM questionable
      LEFT OUTER JOIN date_dim ss_date ON ...
      RIGHT OUTER JOIN item ON ...
      FULL JOIN quizzical ON questionable.customer IS NOT DISTINCT FROM quizzical.customer
                          AND coalesce(item.I_ITEM_SK, questionable.item) = quizzical.item
      LEFT OUTER JOIN date_dim cs_date ON ...
      FULL JOIN customer ON coalesce(quizzical.customer, questionable.customer) = customer.c_customer_sk
    GROUP BY 1,2,3,4,5
  )
```

The hand-written SQL is two filtered per-channel `GROUP BY customer, item` CTEs
joined with FULL OUTER JOIN ON the customer/item keys — no `item` or `customer`
dim tables involved (they're unreferenced), no `coalesce` keying, no date_dim
re-projection.

Root cause is **P5** (unused dim joins) plus a misclassification of the FULL JOIN
key direction: trilogy decides it needs to merge customer/item from the dim
tables when neither side projects anything but the FK.

This is a single alt variant but it dominates the alt-bucket total (2.0s vs
ref 0.36s); flipping it would push the alt win rate from 0/4 to 1/4.

---

## P12 — Collapse per-bucket aggregations on the same upstream CTE into a single pass

Status: **PARTIAL (preql workaround)** — q75 was rewritten to use `with year_pair
as ... MERGE ... align` to express the two filtered aggregates as one preql
statement and now ties the reference SQL (was +0.101s). The generator does not
yet synthesize this from `auto x <- sum(... ? predicate) by keys` form; q9's
5-bucket case is unchanged.

When N downstream CTEs each consume the same upstream CTE and each computes a
single aggregate filtered by a different constant predicate, collapse them into
a single GROUP BY with N aggregate columns (or N WHERE-filtered subqueries
joined at the end).

```sql
-- q75 (current)
WITH juicy AS (...),
     scrawny AS (SELECT keys, sum(CASE WHEN year=2002 THEN amt END)  FROM juicy GROUP BY keys),
     sweltering AS (SELECT keys, sum(CASE WHEN year=2002 THEN cnt END) FROM juicy GROUP BY keys),
     young AS (SELECT keys, sum(CASE WHEN year=2001 THEN cnt END) FROM juicy GROUP BY keys),
     vacuous AS (SELECT keys, sum(CASE WHEN year=2001 THEN amt END) FROM juicy GROUP BY keys)
SELECT ... FROM scrawny JOIN sweltering JOIN young JOIN vacuous USING (keys)
```

Should be:

```sql
WITH curr AS (SELECT keys, sum(amt) curr_amt, sum(cnt) curr_cnt FROM juicy WHERE year=2002 GROUP BY keys),
     prev AS (SELECT keys, sum(amt) prev_amt, sum(cnt) prev_cnt FROM juicy WHERE year=2001 GROUP BY keys)
SELECT ... FROM curr JOIN prev USING (keys)
```

This is the same root cause as P3 (duplicate scans of the same source) but
narrowed to the case where the source is itself a CTE and the only difference
between consumers is a single predicate constant.

**Affected queries:** q75 (4 → 2 passes), q9 (5 → 1 pass), arguably q66's
`juicy`/`thoughtful` split.

---

## Summary table

| Pattern | Status | Queries | Best speedup | Difficulty |
|---|---|---|---|---|
| P1 — Filter pushdown into source CTE | partial | 97, 65, 81 | 6.25x (q97) | medium |
| P2 — CASE WHEN → WHERE | partial | 81, 28, 90, 44, 9 | 2.1x (q81) | medium |
| P3 — Collapse duplicate fact scans | not landed | 65, 73, 29, 94, 16, 5, 17, 25, 34, 44 | 8.0x | high |
| P4 — Per-channel agg vs unified UNION | not landed | 78, 97 | 2.0x | high |
| P5 — Drop unused dim joins | not landed | 64, 29, 81, 97.1 | structural | medium |
| P6 — Drop IS NOT DISTINCT FROM reconciliation | partial (q73 fixed) | 65, 66, 17 | (in 8.0x) | medium |
| P7 — Inline constants | **LANDED** | (66, 77) | small | done |
| P8 — Eliminate dead GROUP BY | not landed | 94, 97, 73, many | small per-query | low |
| P9 — Reuse upstream dim columns | partial | 66, 67 | small | medium |
| P10 — IN subquery → EXISTS | not landed | 16, 94 | medium | low |
| P11 — Suppress unused customer×item dim FULL JOINs | not landed | 97.1 | 15x (alt) | medium |
| P12 — Collapse per-bucket aggregations on same CTE | partial (preql) | 9, (75 fixed by rewrite) | 2-4x | medium |

## Win-rate projection (revised 2026-05-15)

Currently 41/99 (41.4%). To reach 50/99 = 50.5% need **9 swing wins**.

The cheapest wins, in order of expected effort:

1. **q66 regression**: investigate why q66 flipped back to a loss. It was the
   single biggest "fixed" flag in the 2026-05-14 audit and now sits at +0.040s.
   Recover this win first — it should be small.
2. **q73 (P3)**: drop the unfiltered `cooperative` store_sales scan. Half the
   regression has already been recovered (+0.146 → +0.099); the rest of P3
   should flip this.
3. **q9 (P12)**: collapse the 5 bucket CTEs into 1 (or apply the q75
   `MERGE ... align` rewrite as a stopgap).
4. **q44 (P2)**: promote `CASE WHEN SS_STORE_SK = 4` to a WHERE on `thoughtful`.
5. **q81 (P2)**: known 2.1x measured.
6. **q97 (P1 + P4)**: hardest of the headline set.
7. **q65 (P3)**: 8x measured; needs generator-side P3 fix — preql rewrite alone
   does not work (confirmed 2026-05-15).
8. **q29 / q17 / q16 (P3)**: similar shape to q65; one of these should flip.
9. **q28 (P2)**: regressed since prior audit; investigate before relying on P2.

q67 and q85 are within 70-150ms of parity but their root causes (DuckDB plan
drift, modest re-join of date_dim) don't fit cleanly into any pattern — they're
the "earn-it-the-hard-way" tail.

The headline lesson from this audit: **q65's preql-level rewrite did not change
the generated SQL shape** (still 3 store_sales scans, still FULL JOIN with
coalesce keying). The same lesson applies to q75 in reverse — the rewrite there
*did* work because `MERGE ... align` is honored by the lowering pass, while the
bundling syntax used in q65 is not. P3 specifically needs to be addressed in
trilogy's lowering layer, not at the query-author level.
