# Q80 — Investigation Notes

Reference: `https://github.com/duckdb/duckdb/blob/main/extension/tpcds/dsdgen/queries/80.sql`

## Status

`test_eighty` passes. Output matches the reference exactly (100/100 rows).

## Reference query shape

```
WITH ssr AS (sales LEFT JOIN store_returns + dims, GROUP BY s_store_id),
     csr AS (sales LEFT JOIN catalog_returns + dims, GROUP BY cp_catalog_page_id),
     wsr AS (sales LEFT JOIN web_returns + dims, GROUP BY web_site_id)
SELECT channel, id, sum(sales), sum(returns_), sum(profit)
FROM (UNION ALL of the three CTEs with channel labels)
GROUP BY ROLLUP (channel, id)
ORDER BY channel NULLS FIRST, id NULLS FIRST
LIMIT 100;
```

Key points:
1. Each per-channel CTE **inner-joins** all dims (`s_store_id`, `cp_catalog_page_id`,
   `web_site_id`). A sale whose channel-dim FK is NULL or doesn't match a dim row is
   *excluded* — load-bearing for correctness against TPC-DS data.
2. The three CTEs are `UNION ALL`ed.
3. `ROLLUP(channel, id)` produces detail / per-channel total / grand total rows.

## Model

`tests/modeling/tpc_ds_duckdb/unified_sales.preql` exposes:
- `key sales_channel enum<string>['WEB','CATALOG','STORE']`
- `key channel_dim_id int` and `properties <sales_channel, channel_dim_id>(channel_dim_text_id string)`
- `key order_id int` and `properties <sales_channel, order_id, item.id>(...)` — sales metrics
  plus nullable `return_amount` / `return_net_loss`.

Nine partial datasources, each `complete where sales_channel = 'X'`:
- 3× sales: `web/catalog/store_sales_unified` → grain `(sales_channel, order_id, item.id)`
- 3× returns: `web/catalog/store_returns_unified` (use `~order_id, ~item.id` loose) → same grain
- 3× channel dim: `web/catalog/store_dim_unified` → grain `(sales_channel, channel_dim_id)`

## Diagnostic findings

The early hypothesis that the cascade levels diverged was wrong; per-CTE probing showed
all three rolled up to identical per-channel totals. Two real bugs were found:

### Bug 1 — model-side: orphan FKs survived dim LEFT OUTER JOIN

Trilogy's plan emits `LEFT OUTER JOIN premium` for the dim, while the reference's
`WHERE cs_catalog_page_sk = cp_catalog_page_sk` is effectively an INNER JOIN. TPC-DS
catalog_sales has 7185 rows with NULL `cs_catalog_page_sk`; **2 of those survive all
the q80 filters** (date / item / promotion) and contributed exactly:

- catalog returns delta: 575.60
- catalog profit delta: -4597.82
- catalog sales delta: 0 (NULL `ext_sales_price` on the orphan rows)

**Fix (preql):** added `AND sales.channel_dim_text_id is not null` to each level's
WHERE clause. This promotes the dim join to `INNER JOIN` in the planner output.

### Bug 2 — planner: cascade FULL JOIN row-collapse

After the dim-filter fix, all data values matched but the `(NULL, NULL)` grand-total
row was missing. The cascade align rendering produced:

```sql
FROM puzzled                                     -- L1 (channel, id, sums)
    FULL JOIN premium ON puzzled.channel ⩻ premium.channel AND puzzled.id ⩻ premium.id
    FULL JOIN busy    ON puzzled.channel ⩻ busy.channel    AND puzzled.id ⩻ busy.id
```

After the first FULL JOIN, premium-only rows have `puzzled.channel = NULL` and
`puzzled.id = NULL`. The second JOIN's condition `busy.x IS NOT DISTINCT FROM puzzled.x`
where both are NULL evaluates TRUE, so `busy` is **absorbed into the per-channel rows**
instead of surfacing as its own grand-total row.

**Fix (planner):** in `extra_align_joins`
(`trilogy/core/processing/node_generators/multiselect_node.py`), build the chain
explicitly with `concept_pairs` that bind the joining-in CTE to **every prior** parent.
The existing `_build_joinkeys` grouping then renders this as
`coalesce(prior1, prior2, ...) = leveln`, which correctly excludes the busy row from
matching premium-only rows (because `'catalog' = NULL → NULL`, not TRUE).

Resulting JOIN:
```sql
FULL JOIN premium ON puzzled.channel ⩻ premium.channel AND puzzled.id ⩻ premium.id
FULL JOIN busy ON coalesce(puzzled.channel, premium.channel) = busy.channel
              AND coalesce(puzzled.id,      premium.id)      = busy.id
```

## Audit of uncommitted framework changes

| Change | Verdict | Reason |
|---|---|---|
| `safe_get_cte_value` (`trilogy/dialect/base.py`) — RawColumnExpr / FUNCTION_ITEMS handling | **Kept** | Without it, `_best_enum_union`'s parallel partitionings reach `safe_quote` with a `RawColumnExpr` and crash with `'RawColumnExpr' object has no attribute 'startswith'`. Real fix for the `raw(''' 'CATALOG' ''')` literal-column path. |
| `QueryDatasource.__add__` — force_group widening | **Reverted** | Was masking the cascade FULL JOIN row-collapse bug. With the planner fix in place, the original strict equality check works fine; all 394 modeling/generators/optimization tests pass. |
| `_best_enum_union` — multi-result return shape | **Kept** | Without it, q80's planner hangs (the parallel sales/returns/dim partitionings, all keyed by the same channel enum, can't be expressed as separate union datasources). The maximal-overlap filter prevents "mixed" combos like `(2 sales, 1 dim)` from drowning out the pure partitionings. |

## Things that do not (yet) work

### Direct merge without rowset wrapping

Replacing `with q80_results as ... ; SELECT q80_results.x ...` with a direct multiselect
that has `ORDER BY` / `LIMIT` on the merge itself surfaces all per-level columns
(`channel_a`, `sales_b`, etc.) on top of the aligned/derived ones — 20 output columns
instead of 5. The rowset wrapping is acting as a projection layer.

Marking the per-level columns hidden with `--` produces invalid SQL: the planner emits
`INVALID_REFERENCE_BUG_<Missing source reference to sales.ext_sales_price>` placeholders
because the parent CTEs (`puzzled`, `premium`, `busy`) drop the hidden columns, and the
outer SELECT's aggregations have no source CTE to read from.

This is a separate planner gap. Not blocking q80; the rowset-wrapped form works.
