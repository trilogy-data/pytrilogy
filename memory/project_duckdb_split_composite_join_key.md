---
name: project_duckdb_split_composite_join_key
description: FIXED 0.3.513 (#1027) — DuckDB inlines a single-use CTE and joins on ONE key of a composite join when the CTE assembled its keys from several parents; near-cartesian intermediate, OOM. Barrier = MATERIALIZED (trilogy/dialect/join_barriers.py)
metadata:
  type: project
---

FIXED 0.3.513. tree_reporting `{city}_tree_predictions` went from ~1 s to out of memory between 0.3.498 and 0.3.511 with identical per-CTE row counts.

Mechanism (measured on Amsterdam, DuckDB 2.0.0, `EXPLAIN (FORMAT JSON)`): the final statement joins the tree stream to the per-cell density table on `city AND density_cell`. The left CTE is used once, so DuckDB inlines it; inside, `city` comes off one parent and `density_cell` off another (joined on `tree_id`). DuckDB then sees two separate edges and joined on `city = city` alone, one distinct value: 337,417 x 45,171 rows, estimated at 213,287, with `density_cell` applied at a later join.

NOT the cause, despite #1027's title: the watermark `LEFT OUTER` -> `INNER JOIN on 1=1`. Restoring LEFT on 0.3.512's SQL still ran out of memory. The trigger between versions was the reordered genus lookup chain, which only moved DuckDB's cardinality estimate of an upstream CTE (832k -> 337k) and with it the join order. Any plan change can tip it; the emitted shape is the hazard.

Fix: `split_key_barriers` marks a CTE `MATERIALIZED` (DuckDB only, `DuckDBDialect.generate_ctes`) when a join reads 2+ keys off it that trace to more than one relation once every plain-select CTE under it is inlined. A relation is a REFERENCE, not a table: the same CTE read twice is two relations (first attempt keyed origins by name and missed exactly this case). Aggregates, limits, unions, scans and earlier barriers end the trace.

Also fixed in the same change (the semantic half of #1027, from 0.3.502): `SideFacts.one_row` typed a keyless join INNER for any SINGLE_ROW column, including a datasource-stored `property <*>.x`, whose table can be empty — every row vanished. `_computes_one_row` keeps INNER for constants / `by *` totals / expressions over them only.

Debugging recipe that found it: compile both versions, run each SQL hunk of the diff alone under `SET memory_limit` + `SET max_temp_directory_size`, then read the join conditions and estimates in the JSON plan. `count(*)` per CTE proves nothing here: every CTE was fine alone.

Open: a join whose left keys come off two CTEs of the SAME select (`a.k1 = c.k1 AND b.k2 = c.k2`) is the same shape with nothing to materialize; not addressed. The standalone repro is exactly that shape, so it is reachable.

Standalone repro (DuckDB 2.0.0 fails; 1.4.1 and 1.2.2 pick the good order; a TABLE instead of Parquet is fine on every version — no distinct-count stats on Parquet, and 2.0.0 breaks the tie toward the one-value key):

```sql
SET memory_limit='1GB'; SET max_temp_directory_size='1GB';
COPY (SELECT i AS tree_id, 'ams' AS city, i % 45000 AS cell FROM range(337417) t(i)) TO 'trees.parquet' (FORMAT parquet);
WITH per_cell AS (SELECT city, cell, count(*) AS n FROM 'trees.parquet' GROUP BY 1, 2)
SELECT count(*) FROM 'trees.parquet' t
INNER JOIN 'trees.parquet' c ON t.tree_id = c.tree_id
INNER JOIN per_cell ON t.city = per_cell.city AND c.cell = per_cell.cell;
```

Upstream DuckDB issue drafted, not filed as of 2026-11-17.
