# Q80 — Investigation Notes

Reference: `https://github.com/duckdb/duckdb/blob/main/extension/tpcds/dsdgen/queries/80.sql`

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
   *excluded* — this is load-bearing for correctness against TPC-DS data.
2. The three CTEs are `UNION ALL`ed.
3. `ROLLUP(channel, id)` produces detail / per-channel total / grand total rows.

## Model shape

`tests/modeling/tpc_ds_duckdb/unified_sales.preql` exposes:
- `key sales_channel enum<string>['WEB','CATALOG','STORE']`
- `key channel_dim_id int` and `properties <sales_channel, channel_dim_id>(channel_dim_text_id string)`
- `key order_id int` and `properties <sales_channel, order_id, item.id>(...)` — sales metrics
  plus nullable `return_amount` / `return_net_loss`.

Nine partial datasources, each `complete where sales_channel = 'X'`:
- 3× sales: `web/catalog/store_sales_unified` → grain `(sales_channel, order_id, item.id)`
- 3× returns: `web/catalog/store_returns_unified` (use `~order_id, ~item.id` loose) → same grain
- 3× channel dim: `web/catalog/store_dim_unified` → grain `(sales_channel, channel_dim_id)`

## Current status

`query80.preql` runs end-to-end. All per-row data values match the reference, including
detail rows and per-channel totals. **One** structural row is missing from the output: the
`(NULL, NULL)` grand-total row. This is a real planner bug in trilogy's MERGE+align
cascade rendering — see "Outstanding bug" below.

## Framework changes that landed (uncommitted)

These were added during the q80 push. Their actual necessity is being re-audited.

1. **`safe_get_cte_value` (`trilogy/dialect/base.py`)** — both single-source and multi-source
   paths now route through a `_format` helper that recognizes `RawColumnExpr` and
   `FUNCTION_ITEMS` from `cte.get_alias`, returning the raw text directly. Previously a
   `RawColumnExpr` reaching `safe_quote` raised `'RawColumnExpr' object has no attribute
   'startswith'`. **Likely load-bearing**: the `raw(''' 'CATALOG' ''')` literal column in
   the partial datasources surfaces as a `RawColumnExpr` that travels through CTE column
   resolution.

2. **`QueryDatasource.__add__` (`trilogy/core/models/execute.py`)** — relaxed the strict
   `force_group` equality check. Previously merging a returns-grouped QDS with a
   sales-grouped QDS at the same grain raised `'can only merge two datasources if the
   force_group flag is the same'`. **Suspicious**: two QDSes with the same name should not
   have different `force_group` semantics. Needs revert + repro to find the real cause.

3. **`_best_enum_union` (`trilogy/core/processing/node_generators/select_helpers/
   datasource_injection.py`)** — now returns `list[list[BuildDatasource]]` (one per
   distinct concept-overlap signature) instead of a single combo. **Likely load-bearing**:
   the parallel sales / returns / dim partitionings are all keyed by the same
   `sales_channel` enum, and need to coexist as separate union datasources. The
   maximal-overlap filter prevents "mixed" combos like `(2 sales, 1 dim)` from drowning
   out pure partitionings.

The 26 unit tests in `tests/generators/test_datasource_scoring.py` were updated for the
new `_best_enum_union` shape. All other unit tests still pass.

## Diagnostic findings (2026-05-04)

The earlier version of this doc claimed cascade levels diverged and that joining to the
dim mysteriously changed sums. **Both claims were wrong.** Per-CTE probing of the actual
generated SQL showed:

### Claim 1 (REFUTED): "Levels diverge — level 1 joins dim, levels 2/3 don't"

All three trilogy cascade CTEs agree perfectly when probed:

| | catalog | store | web |
|---|---|---|---|
| level 1 (`puzzled`) | 218899.93 | 363697.50 | 115697.04 |
| level 2 (`kaput`) | 218899.93 | 363697.50 | 115697.04 |
| row-level (`late`) | 218899.93 | 363697.50 | 115697.04 |

The earlier "kaput.returns_b for catalog channel = 575.60" was a misread of the diff
between trilogy and the reference; the actual `kaput` value is 218899.93. There is **no
divergence between levels** to fix.

### Claim 2 (REFUTED): "Joining to the dim changes the result"

A LEFT OUTER JOIN to a per-key-unique dim doesn't change `sum()` — it just adds nullable
columns. Both `puzzled` (with the dim joined) and `late` (without) produce the same row
sums, confirming this.

### Actual root cause of the 575.60 delta

Trilogy's plan emits `LEFT OUTER JOIN premium` for the dim. The reference query's `WHERE
cs_catalog_page_sk = cp_catalog_page_sk` is effectively an INNER JOIN that drops sales
whose dim FK doesn't match. TPC-DS catalog_sales contains 7185 rows with NULL
`cs_catalog_page_sk`; **2 of those rows survive all the q80 filters** (date / item /
promotion). Their contribution is exactly:

- catalog returns delta: 575.60
- catalog profit delta: -4597.82
- catalog sales delta: 0 (these rows have NULL `ext_sales_price`)

Same situation in store: 53047.78 of returns drops out when the dim is enforced.

### Fix (applied to `query80.preql`)

Adding `AND sales.channel_dim_text_id is not null` to the WHERE clauses promotes trilogy's
plan to `INNER JOIN thoughtful` for the dim. After the fix:

| | reference | trilogy |
|---|---|---|
| catalog total | (4655990.73, 218324.33, -510980.92) | (4655990.73, 218324.33, -510980.92) ✓ |
| store total | (6738789.72, 358581.74, -2862388.33) | (6738789.72, 358581.74, -2862388.33) ✓ |
| web total | (2165333.43, 115697.04, -224178.15) | (2165333.43, 115697.04, -224178.15) ✓ |

All detail rows and all per-channel totals now match exactly.

## Outstanding bug — cascade FULL JOIN row-collapse

After the dim-filter fix, the data values are correct but the `(NULL, NULL)` grand-total
row never appears in the final output. The cascade align rendering produces:

```sql
FROM puzzled                                              -- L1: (channel, id, sums)
    FULL JOIN premium ON puzzled.channel  IS NOT DISTINCT FROM premium.channel
                     AND puzzled.id       IS NOT DISTINCT FROM premium.id
    FULL JOIN busy    ON puzzled.channel  IS NOT DISTINCT FROM busy.channel
                     AND puzzled.id       IS NOT DISTINCT FROM busy.id
```

where `premium` rows have `(channel='catalog channel', id=NULL)` and `busy` is the single
`(NULL, NULL)` grand-total row.

After the first `FULL JOIN`, premium-only rows have `puzzled.channel = NULL` and
`puzzled.id = NULL` (no left-side match). The second `FULL JOIN` to `busy` then finds that
`busy.channel = NULL IS NOT DISTINCT FROM puzzled.channel = NULL` is TRUE, so `busy` is
**absorbed into each per-channel row** instead of surfacing as its own row. Reproduced
minimally:

```sql
-- with the trilogy-emitted condition: 3 rows, no grand total
-- with `coalesce(puzzled.col, premium.col) IS NOT DISTINCT FROM busy.col`: 4 rows incl. grand total
```

### Fix shape

The level-N join in a cascade align needs its `ON` condition to match against
`coalesce(level1.col, level2.col, ..., level(N-1).col)`, not just `level1.col`. Code
locations:

- `trilogy/core/processing/node_generators/multiselect_node.py::extra_align_joins` —
  emits the pairwise `NodeJoin`s; today they're rendered as a chain that always uses the
  first node's column reference.
- `trilogy/core/models/execute.py::BaseJoin` (or its renderer in `trilogy/dialect/`) —
  this is where the `ON` condition gets serialized; would need a "coalesce against prior
  joined nodes" mode.

Whether to fix the planner or skip the test is the next decision.
