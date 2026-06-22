# Trilogy failure analysis — 20260622-174304

- Run `20260622-174304` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1712 | failed: 237 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 152 | 64% |
| `syntax-parse` | 47 | 20% |
| `cli-misuse` | 16 | 7% |
| `syntax-missing-alias` | 10 | 4% |
| `undefined-concept` | 7 | 3% |
| `file-not-found` | 4 | 2% |
| `join-resolution` | 1 | 0% |

## Detail

### `other`

- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales select catalog_sales.sold_date.year, sum(catalog_sales.ext_sales_price) as cat_sales, sum(web_sales.ext_sales_price) as web_sales order by 1;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {catalog_sales.sold_date.year, local.cat_sales}; {local.web_sales}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: HAVING references 'sv01.store_val_2001', 'cv01.cat_val_2001', 'wv01.web_val_2001', 'cv02.cat_val_2002', 'sv02.store_val_2002', 'wv02.web_val_2002', which are not in the SELECT projection (line 35). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --sv01.store_val_2001, --cv01.cat_val_2001, --wv01.web_val_2001, --cv02.cat_val_2002, --sv02.store_val_2002, --wv02.web_val_2002
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  Resolution error in query04.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cv01.cat_val_2001, cv02.cat_val_2002, local.customer_id, local.first_name, local.last_name, local.preferred_cust_flag, sv01.store_val_2001, sv02.store_val_2002, wv01.web_val_2001, wv02.web_val_2002}
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: HAVING references 'local.cat_val_2002', 'local.cat_val_2001', 'local.store_val_2002', 'local.store_val_2001', 'local.web_val_2002', 'local.web_val_2001', which are not in the SELECT projection (line 18). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.cat_val_2002, --local.cat_val_2001, --local.store_val_2002, --local.store_val_2001, --local.web_val_2002, --local.web_val_2001
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Resolution error in query05.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {returns_part.chan, returns_part.dim_text_id, returns_part.return_amount, returns_part.return_net_loss, sales_part.dim_text_id, sales_part.gross_sales, sales_part.sales_net_profit}
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: (_duckdb.BinderException) Binder Error: column "sales_channel" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(sales_channel)" if the exact value of "sales_channel" is not important.

  LINE 179:     "friendly"."sales_channel" asc,
                ^
  [SQL:
  WITH
  uneven as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
      "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
      "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "sales_return_date_id",
      "sales_catalog_returns_unified"."CR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "catalog_returns" as "sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
      "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
      "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
      "sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "sales_return_date_id",
      "sales_store_returns_unified"."SR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "store_returns" as "sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
      "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
      "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
      "sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "sales_return_date_id",
      "sales_web_returns_unified"."WR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "web_returns" as "sales_web_returns_unified"),
  juicy as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
      "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_returns_unified"."CR_CATALOG_PAGE_SK" as "sales_return_channel_dim_id"
  FROM
      "catalog_returns" as "sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
      "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
      "sales_store_returns_unified"."SR_STORE_SK" as "sales_return_channel_dim_id"
  FROM
      "store_returns" as "sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
      "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_return_channel_dim_id"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  young as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "sales_channel_dim_id",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
      "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
      "sales_catalog_sales_unified"."CS_NET_PROFIT" as "sales_net_profit",
      "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_STORE_SK" as "sales_channel_dim_id",
      "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
      "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
      "sales_store_sales_unified"."SS_NET_PROFIT" as "sales_net_profit",
      "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id"
  FROM
      "store_sales" as "sales_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_channel_dim_id",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
      "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
      "sales_web_sales_unified"."WS_NET_PROFIT" as "sales_net_profit",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  cheerful as (
  SELECT
      "sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "sales_channel_dim_id",
      "sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "sales_channel_dim_text_id"
  FROM
      "catalog_page" as "sales_catalog_dim_unified"
  UNION ALL
  SELECT
      "sales_store_dim_unified"."S_STORE_SK" as "sales_channel_dim_id",
      "sales_store_dim_unified"."S_STORE_ID" as "sales_channel_dim_text_id"
  FROM
      "store" as "sales_store_dim_unified"
  UNION ALL
  SELECT
      "sales_web_dim_unified"."web_site_sk" as "sales_channel_dim_id",
      "sales_web_dim_unified"."web_site_id" as "sales_channel_dim_text_id"
  FROM
      "web_site" as "sales_web_dim_unified"),
  sweltering as (
  SELECT
      "young"."sales_channel" as "sales_channel",
      "young"."sales_channel_dim_id" as "sales_channel_dim_id",
      CASE WHEN cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "young"."sales_channel_dim_id" is not null THEN "young"."sales_ext_sales_price" ELSE NULL END as "sale_amt",
      CASE WHEN cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "young"."sales_channel_dim_id" is not null THEN "young"."sales_net_profit" ELSE NULL END as "sale_prof",
      CASE WHEN cast("sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "juicy"."sales_return_channel_dim_id" is not null THEN "uneven"."sales_return_amount" ELSE NULL END as "ret_amt",
      CASE WHEN cast("sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "juicy"."sales_return_channel_dim_id" is not null THEN "uneven"."sales_return_net_loss" ELSE NULL END as "ret_loss"
  FROM
      "young"
      LEFT OUTER JOIN "date_dim" as "sales_date_date" on "young"."sales_date_id" = "sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "juicy" on "young"."sales_channel" = "juicy"."sales_channel" AND "young"."sales_item_id" = "juicy"."sales_item_id" AND "young"."sales_order_id" = "juicy"."sales_order_id"
      LEFT OUTER JOIN "uneven" on "young"."sales_channel" = "uneven"."sales_channel" AND "young"."sales_item_id" = "uneven"."sales_item_id" AND "young"."sales_order_id" = "uneven"."sales_order_id"
      LEFT OUTER JOIN "date_dim" as "sales_return_date_date" on "uneven"."sales_return_date_id" = "sales_return_date_date"."D_DATE_SK"
  WHERE
      ( cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "young"."sales_channel_dim_id" is not null ) or ( cast("sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "juicy"."sales_return_channel_dim_id" is not null )

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      "young"."sales_item_id",
      "young"."sales_order_id"),
  thoughtful as (
  SELECT
      "cheerful"."sales_channel_dim_id" as "sales_channel_dim_id",
      "cheerful"."sales_channel_dim_text_id" as "sales_channel_dim_text_id"
  FROM
      "cheerful"
  GROUP BY
      1,
      2),
  friendly as (
  SELECT
      "sweltering"."sales_channel" as "sales_channel",
      "sweltering"."sales_channel_dim_id" as "sales_channel_dim_id",
      sum("sweltering"."ret_amt") as "_virt_agg_sum_9135821917307218",
      sum("sweltering"."ret_loss") as "_virt_agg_sum_2471052009056623",
      sum("sweltering"."sale_amt") as "_virt_agg_sum_1147765332253556",
      sum("sweltering"."sale_prof") as "_virt_agg_sum_88772860406151"
  FROM
      "sweltering"
  GROUP BY
      ROLLUP (1, 2))
  SELECT
      CASE
  	WHEN "friendly"."sales_channel" = 'STORE' THEN 'store channel'
  	WHEN "friendly"."sales_channel" = 'CATALOG' THEN 'catalog channel'
  	WHEN "friendly"."sales_channel" = 'WEB' THEN 'web channel'
  	END as "channel_type",
      CASE
  	WHEN "friendly"."sales_channel" = 'STORE' THEN ('store' || "thoughtful"."sales_channel_dim_text_id")
  	WHEN "friendly"."sales_channel" = 'CATALOG' THEN ('catalog_page' || "thoughtful"."sales_channel_dim_text_id")
  	WHEN "friendly"."sales_channel" = 'WEB' THEN ('web_site' || "thoughtful"."sales_channel_dim_text_id")
  	END as "entity_identifier",
      coalesce("friendly"."_virt_agg_sum_1147765332253556",0) as "total_gross_sales",
      coalesce("friendly"."_virt_agg_sum_9135821917307218",0) as "total_returns",
      coalesce("friendly"."_virt_agg_sum_88772860406151",0) - coalesce("friendly"."_virt_agg_sum_2471052009056623",0) as "net_profit"
  FROM
      "friendly"
      INNER JOIN "thoughtful" on "friendly"."sales_channel_dim_id" = "thoughtful"."sales_channel_dim_id"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      "friendly"."sales_channel" asc,
      "thoughtful"."sales_channel_dim_id" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: (_duckdb.BinderException) Binder Error: column "sales_channel" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(sales_channel)" if the exact value of "sales_channel" is not important.

  LINE 175:     "friendly"."sales_channel" asc,
                ^
  [SQL:
  WITH
  uneven as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
      "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
      "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "sales_return_date_id",
      "sales_catalog_returns_unified"."CR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "catalog_returns" as "sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
      "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
      "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
      "sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "sales_return_date_id",
      "sales_store_returns_unified"."SR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "store_returns" as "sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
      "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
      "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
      "sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "sales_return_date_id",
      "sales_web_returns_unified"."WR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "web_returns" as "sales_web_returns_unified"),
  juicy as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
      "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_returns_unified"."CR_CATALOG_PAGE_SK" as "sales_return_channel_dim_id"
  FROM
      "catalog_returns" as "sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
      "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
      "sales_store_returns_unified"."SR_STORE_SK" as "sales_return_channel_dim_id"
  FROM
      "store_returns" as "sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
      "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_return_channel_dim_id"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  young as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "sales_channel_dim_id",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
      "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
      "sales_catalog_sales_unified"."CS_NET_PROFIT" as "sales_net_profit",
      "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_STORE_SK" as "sales_channel_dim_id",
      "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
      "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
      "sales_store_sales_unified"."SS_NET_PROFIT" as "sales_net_profit",
      "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id"
  FROM
      "store_sales" as "sales_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_channel_dim_id",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
      "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
      "sales_web_sales_unified"."WS_NET_PROFIT" as "sales_net_profit",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  cheerful as (
  SELECT
      "sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "sales_channel_dim_id",
      "sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "sales_channel_dim_text_id"
  FROM
      "catalog_page" as "sales_catalog_dim_unified"
  UNION ALL
  SELECT
      "sales_store_dim_unified"."S_STORE_SK" as "sales_channel_dim_id",
      "sales_store_dim_unified"."S_STORE_ID" as "sales_channel_dim_text_id"
  FROM
      "store" as "sales_store_dim_unified"
  UNION ALL
  SELECT
      "sales_web_dim_unified"."web_site_sk" as "sales_channel_dim_id",
      "sales_web_dim_unified"."web_site_id" as "sales_channel_dim_text_id"
  FROM
      "web_site" as "sales_web_dim_unified"),
  sweltering as (
  SELECT
      "young"."sales_channel" as "sales_channel",
      "young"."sales_channel_dim_id" as "sales_channel_dim_id",
      CASE WHEN cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "young"."sales_channel_dim_id" is not null THEN "young"."sales_ext_sales_price" ELSE NULL END as "sale_amt",
      CASE WHEN cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "young"."sales_channel_dim_id" is not null THEN "young"."sales_net_profit" ELSE NULL END as "sale_prof",
      CASE WHEN cast("sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "juicy"."sales_return_channel_dim_id" is not null THEN "uneven"."sales_return_amount" ELSE NULL END as "ret_amt",
      CASE WHEN cast("sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "juicy"."sales_return_channel_dim_id" is not null THEN "uneven"."sales_return_net_loss" ELSE NULL END as "ret_loss"
  FROM
      "young"
      LEFT OUTER JOIN "date_dim" as "sales_date_date" on "young"."sales_date_id" = "sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "juicy" on "young"."sales_channel" = "juicy"."sales_channel" AND "young"."sales_item_id" = "juicy"."sales_item_id" AND "young"."sales_order_id" = "juicy"."sales_order_id"
      LEFT OUTER JOIN "uneven" on "young"."sales_channel" = "uneven"."sales_channel" AND "young"."sales_item_id" = "uneven"."sales_item_id" AND "young"."sales_order_id" = "uneven"."sales_order_id"
      LEFT OUTER JOIN "date_dim" as "sales_return_date_date" on "uneven"."sales_return_date_id" = "sales_return_date_date"."D_DATE_SK"
  WHERE
      ( cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "young"."sales_channel_dim_id" is not null ) or ( cast("sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "juicy"."sales_return_channel_dim_id" is not null )

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      "young"."sales_item_id",
      "young"."sales_order_id"),
  thoughtful as (
  SELECT
      "cheerful"."sales_channel_dim_id" as "sales_channel_dim_id",
      "cheerful"."sales_channel_dim_text_id" as "sales_channel_dim_text_id"
  FROM
      "cheerful"
  GROUP BY
      1,
      2),
  friendly as (
  SELECT
      "sweltering"."sales_channel" as "sales_channel",
      "sweltering"."sales_channel_dim_id" as "sales_channel_dim_id",
      sum("sweltering"."ret_amt") as "_virt_agg_sum_9135821917307218",
      sum("sweltering"."ret_loss") as "_virt_agg_sum_2471052009056623",
      sum("sweltering"."sale_amt") as "_virt_agg_sum_1147765332253556",
      sum("sweltering"."sale_prof") as "_virt_agg_sum_88772860406151"
  FROM
      "sweltering"
  GROUP BY
      ROLLUP (1, 2))
  SELECT
      CASE
  	WHEN "friendly"."sales_channel" = 'STORE' THEN 'store channel'
  	WHEN "friendly"."sales_channel" = 'CATALOG' THEN 'catalog channel'
  	WHEN "friendly"."sales_channel" = 'WEB' THEN 'web channel'
  	END as "channel_type",
      "thoughtful"."sales_channel_dim_text_id" as "entity_identifier",
      coalesce("friendly"."_virt_agg_sum_1147765332253556",0) as "total_gross_sales",
      coalesce("friendly"."_virt_agg_sum_9135821917307218",0) as "total_returns",
      coalesce("friendly"."_virt_agg_sum_88772860406151",0) - coalesce("friendly"."_virt_agg_sum_2471052009056623",0) as "net_profit"
  FROM
      "friendly"
      INNER JOIN "thoughtful" on "friendly"."sales_channel_dim_id" = "thoughtful"."sales_channel_dim_id"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      "friendly"."sales_channel" asc,
      "thoughtful"."sales_channel_dim_id" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql`

  ```text
  Resolution error in query05.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {returns_agg.chan, returns_agg.dim_text, returns_agg.ra, returns_agg.rl, sales_agg.dim_text, sales_agg.gs, sales_agg.sp}
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cat_avg.avg_price}; {local.line_item_count, local.state, sales.customer.id, sales.date.month_of_year, sales.date.year, sales.item.category, sales.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: Undefined concept: item_cat_prices.category. Suggestions: ['item_cat_prices.sales.item.category', 'sales.item.category', 'item_cat_prices.sales.item.current_price']
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: Undefined concept: cat_avg.category. Suggestions: ['cat_avg.item_dim.category', 'item_dim.category', 'sales.item.category', 'cat_avg.avg_price']
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: HAVING references 'sales.item.current_price', 'cat_avg.avg_price', which are not in the SELECT projection (line 12). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --sales.item.current_price, --cat_avg.avg_price
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query07.preql`

  ```text
  Syntax error in query07.preql: Comparison `ss.customer_demographic.gender = 'Male'` can never match enum field 'ss.customer_demographic.gender', which contains only these values: 'M', 'F'. It is always false and should be removed.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.preferred_cust_flag. Suggestions: ['sales.customer.preferred_cust_flag', 'sales.return_customer.preferred_cust_flag']
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unexpected error in query08.preql: (_duckdb.BinderException) Binder Error: Cannot compare values of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is required

  LINE 29: ..." ) > 10 THEN "cooperative"."cust_address_zip" ELSE NULL END in (select questionable."zip_list" from questionable where...
                                                                           ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "cust_address_customer_address"."CA_ZIP" as "cust_address_zip",
      count(CASE WHEN "cust_customers"."C_PREFERRED_CUST_FLAG" = 'Y' THEN "cust_customers"."C_CUSTOMER_SK" ELSE NULL END) as "_virt_agg_count_4120572373512085"
  FROM
      "customer" as "cust_customers"
      INNER JOIN "customer_address" as "cust_address_customer_address" on "cust_customers"."C_CURRENT_ADDR_SK" = "cust_address_customer_address"."CA_ADDRESS_SK"
  GROUP BY
      1),
  questionable as (
  SELECT
      STRING_SPLIT( $1 , ',' ) as "zip_list"
  ),
  cooperative as (
  SELECT
      "cust_address_customer_address"."CA_ZIP" as "cust_address_zip",
      "wakeful"."_virt_agg_count_4120572373512085" as "_virt_agg_count_4120572373512085"
  FROM
      "wakeful"
      INNER JOIN "customer_address" as "cust_address_customer_address" on "wakeful"."cust_address_zip" = "cust_address_customer_address"."CA_ZIP"),
  abundant as (
  SELECT
      CASE WHEN ( "cooperative"."_virt_agg_count_4120572373512085" ) > 10 THEN "cooperative"."cust_address_zip" ELSE NULL END as "qualifying_zip"
  FROM
      "cooperative"
  WHERE
      CASE WHEN ( "cooperative"."_virt_agg_count_4120572373512085" ) > 10 THEN "cooperative"."cust_address_zip" ELSE NULL END in (select questionable."zip_list" from questionable where questionable."zip_list" is not null)
  ),
  uneven as (
  SELECT
      SUBSTRING("abundant"."qualifying_zip",1,2) as "qualifying_prefix"
  FROM
      "abundant"
  GROUP BY
      1),
  young as (
  SELECT
      "sales_store_sales"."SS_NET_PROFIT" as "sales_net_profit",
      "sales_store_store"."S_STORE_NAME" as "sales_store_name"
  FROM
      "store_sales" as "sales_store_sales"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_store_sales"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "sales_store_store" on "sales_store_sales"."SS_STORE_SK" = "sales_store_store"."S_STORE_SK"
  WHERE
      "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_QOY" = 2 and SUBSTRING("sales_store_store"."S_ZIP",1,2) in (select uneven."qualifying_prefix" from uneven where uneven."qualifying_prefix" is not null)

  GROUP BY
      1,
      2,
      "sales_store_sales"."SS_ITEM_SK",
      "sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "young"."sales_store_name" as "sales_store_name",
      sum("young"."sales_net_profit") as "total_net_profit"
  FROM
      "young"
  GROUP BY
      1
  ORDER BY
      "young"."sales_store_name" asc
  LIMIT (100)]
  [parameters: ('24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,28577,55565,17183,54601,67897,22752 ... (2101 characters truncated) ... 26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576',)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 32, col 5, in ORDER BY); did you mean: all_sales.billing_customer.first_name, all_sales.ship_customer.first_name, all_sales.purchasing_customer.first_name?
    - last_name (line 33, col 5, in ORDER BY); did you mean: all_sales.billing_customer.last_name, all_sales.ship_customer.last_name, all_sales.purchasing_customer.last_name?
    - preferred_cust_flag (line 34, col 5, in ORDER BY); did you mean: all_sales.billing_customer.preferred_cust_flag, all_sales.ship_customer.preferred_cust_flag, all_sales.purchasing_customer.preferred_cust_flag?
  ```
- `trilogy run query12.preql`

  ```text
  Syntax error in query12.preql: 5 undefined concept references; fix all before re-running:
    - item_totals.category (line 20, col 5, in SELECT); did you mean: item_totals.ws.item.category, ws.item.category, item_totals.class, item_totals.item_code?
    - item_totals.class (line 21, col 5, in SELECT); did you mean: item_totals.ws.item.class, ws.item.class, item_totals.category, item_totals.item_code?
    - item_totals.current_price (line 22, col 5, in SELECT); did you mean: item_totals.ws.item.current_price, ws.item.current_price, item_totals.category?
    - item_totals.category (line 26, col 5, in ORDER BY); did you mean: item_totals.ws.item.category, ws.item.category, item_totals.class, item_totals.item_code?
    - item_totals.class (line 27, col 5, in ORDER BY); did you mean: item_totals.ws.item.class, ws.item.class, item_totals.category, item_totals.item_code?
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 6 undefined concept references; fix all before re-running:
    - sales.customer_demographics.marital_status (line 8, col 6, in WHERE); did you mean: sales.customer.demographics.marital_status, sales.return_customer.demographics.marital_status, sales.customer_demographic.marital_status?
    - sales.customer_demographics.education_status (line 9, col 11, in WHERE); did you mean: sales.customer.demographics.education_status, sales.return_customer.demographics.education_status, sales.customer_demographic.education_status?
    - sales.customer_demographics.marital_status (line 13, col 6, in WHERE); did you mean: sales.customer.demographics.marital_status, sales.return_customer.demographics.marital_status, sales.customer_demographic.marital_status?
    - sales.customer_demographics.education_status (line 14, col 11, in WHERE); did you mean: sales.customer.demographics.education_status, sales.return_customer.demographics.education_status, sales.customer_demographic.education_status?
    - sales.customer_demographics.marital_status (line 18, col 6, in WHERE); did you mean: sales.customer.demographics.marital_status, sales.return_customer.demographics.marital_status, sales.customer_demographic.marital_status?
    - sales.customer_demographics.education_status (line 19, col 11, in WHERE); did you mean: sales.customer.demographics.education_status, sales.return_customer.demographics.education_status, sales.customer_demographic.education_status?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'overall_avg.avg_sale_value', which is not in the SELECT projection (line 21). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --overall_avg.avg_sale_value
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query16.preql`

  ```text
  Syntax error in query16.preql: Undefined concept: multi_warehouse_orders.order_number (line 17, col 28, in WHERE). Suggestions: ['multi_warehouse_orders.cs.order_number', 'cs.order_number', 'cr.sales.order_number', 'cr.order_number', 'multi_warehouse_orders.warehouse_count', '_multi_warehouse_orders_warehouse_count']
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(local.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.country) as g order by g desc`.
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(local.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.country) as g order by g desc`.
  ```
- `trilogy run query18.preql`

  ```text
  Unexpected error in query18.preql: (_duckdb.BinderException) Binder Error: column "cs_item_text_id" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(cs_item_text_id)" if the exact value of "cs_item_text_id" is not important.

  LINE 80:     "abundant"."cs_item_text_id" as "cs_item_text_id",
               ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "cs_catalog_sales"."CS_BILL_CDEMO_SK" as "cs_bill_customer_demographic_id",
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" as "cs_bill_customer_id",
      "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
      "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sold_date_id"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  GROUP BY
      1,
      2,
      3,
      4),
  yummy as (
  SELECT
      "cs_bill_customer_address_customer_address"."CA_COUNTRY" as "cs_bill_customer_address_country",
      "cs_bill_customer_address_customer_address"."CA_COUNTY" as "cs_bill_customer_address_county",
      "cs_bill_customer_address_customer_address"."CA_STATE" as "cs_bill_customer_address_state",
      "cs_bill_customer_customers"."C_BIRTH_YEAR" as "cs_bill_customer_birth_year",
      "cs_bill_customer_demographic_customer_demographics"."CD_DEP_COUNT" as "cs_bill_customer_demographic_dependent_count",
      "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
      "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
      "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "cs_bill_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_bill_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "cs_bill_customer_address_customer_address" on "cs_bill_customer_customers"."C_CURRENT_ADDR_SK" = "cs_bill_customer_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 1998 and "cs_bill_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_bill_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_bill_customer_customers"."C_BIRTH_MONTH" in (1,2,6,8,9,12) and "cs_bill_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')
  ),
  abundant as (
  SELECT
      "cs_bill_customer_address_customer_address"."CA_COUNTRY" as "cs_bill_customer_address_country",
      "cs_bill_customer_address_customer_address"."CA_COUNTY" as "cs_bill_customer_address_county",
      "cs_bill_customer_address_customer_address"."CA_STATE" as "cs_bill_customer_address_state",
      "cs_item_items"."I_ITEM_ID" as "cs_item_text_id"
  FROM
      "thoughtful"
      INNER JOIN "item" as "cs_item_items" on "thoughtful"."cs_item_id" = "cs_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "thoughtful"."cs_sold_date_id" = "cs_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "cs_bill_customer_customers" on "thoughtful"."cs_bill_customer_id" = "cs_bill_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "cs_bill_customer_address_customer_address" on "cs_bill_customer_customers"."C_CURRENT_ADDR_SK" = "cs_bill_customer_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "thoughtful"."cs_bill_customer_demographic_id" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 1998 and "cs_bill_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_bill_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_bill_customer_customers"."C_BIRTH_MONTH" in (1,2,6,8,9,12) and "cs_bill_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')

  GROUP BY
      1,
      2,
      3,
      4,
      "cs_bill_customer_address_customer_address"."CA_ADDRESS_SK"),
  juicy as (
  SELECT
      "yummy"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
      "yummy"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
      "yummy"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
      avg("yummy"."cs_bill_customer_birth_year") as "avg_birth_year",
      avg("yummy"."cs_bill_customer_demographic_dependent_count") as "avg_dependent_count",
      avg("yummy"."cs_coupon_amt") as "avg_coupon_amount",
      avg("yummy"."cs_list_price") as "avg_list_price",
      avg("yummy"."cs_net_profit") as "avg_net_profit",
      avg("yummy"."cs_quantity") as "avg_ticket_quantity",
      avg("yummy"."cs_sales_price") as "avg_sales_price"
  FROM
      "yummy"
  GROUP BY
      ROLLUP (1, 3, 2)),
  uneven as (
  SELECT
      "abundant"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
      "abundant"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
      "abundant"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
      "abundant"."cs_item_text_id" as "cs_item_text_id",
      grouping("abundant"."cs_bill_customer_address_country") as "g_country",
      grouping("abundant"."cs_bill_customer_address_county") as "g_county",
      grouping("abundant"."cs_bill_customer_address_state") as "g_state"
  FROM
      "abundant"
  GROUP BY
      ROLLUP (1, 3, 2))
  SELECT
      "uneven"."cs_item_text_id" as "item_code",
      coalesce("juicy"."cs_bill_customer_address_country","uneven"."cs_bill_customer_address_country") as "country",
      coalesce("juicy"."cs_bill_customer_address_state","uneven"."cs_bill_customer_address_state") as "state",
      coalesce("juicy"."cs_bill_customer_address_county","uneven"."cs_bill_customer_address_county") as "county",
      "juicy"."avg_ticket_quantity" as "avg_ticket_quantity",
      "juicy"."avg_list_price" as "avg_list_price",
      "juicy"."avg_coupon_amount" as "avg_coupon_amount",
      "juicy"."avg_sales_price" as "avg_sales_price",
      "juicy"."avg_net_profit" as "avg_net_profit",
      "juicy"."avg_birth_year" as "avg_birth_year",
      "juicy"."avg_dependent_count" as "avg_dependent_count"
  FROM
      "uneven"
      INNER JOIN "juicy" on "uneven"."cs_bill_customer_address_country" is not distinct from "juicy"."cs_bill_customer_address_country" AND "uneven"."cs_bill_customer_address_county" is not distinct from "juicy"."cs_bill_customer_address_county" AND "uneven"."cs_bill_customer_address_state" is not distinct from "juicy"."cs_bill_customer_address_state"
  ORDER BY
      "uneven"."g_country" asc,
      "uneven"."g_state" asc,
      "uneven"."g_county" asc,
      "country" asc nulls first,
      "state" asc nulls first,
      "county" asc nulls first,
      "item_code" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query18.preql`

  ```text
  Unexpected error in query18.preql: (_duckdb.BinderException) Binder Error: column "cs_item_text_id" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(cs_item_text_id)" if the exact value of "cs_item_text_id" is not important.

  LINE 80:     "abundant"."cs_item_text_id" as "cs_item_text_id",
               ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "cs_catalog_sales"."CS_BILL_CDEMO_SK" as "cs_bill_customer_demographic_id",
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" as "cs_bill_customer_id",
      "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
      "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sold_date_id"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  GROUP BY
      1,
      2,
      3,
      4),
  yummy as (
  SELECT
      "cs_bill_customer_address_customer_address"."CA_COUNTRY" as "cs_bill_customer_address_country",
      "cs_bill_customer_address_customer_address"."CA_COUNTY" as "cs_bill_customer_address_county",
      "cs_bill_customer_address_customer_address"."CA_STATE" as "cs_bill_customer_address_state",
      "cs_bill_customer_customers"."C_BIRTH_YEAR" as "cs_bill_customer_birth_year",
      "cs_bill_customer_demographic_customer_demographics"."CD_DEP_COUNT" as "cs_bill_customer_demographic_dependent_count",
      "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
      "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
      "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "cs_bill_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_bill_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "cs_bill_customer_address_customer_address" on "cs_bill_customer_customers"."C_CURRENT_ADDR_SK" = "cs_bill_customer_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 1998 and "cs_bill_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_bill_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_bill_customer_customers"."C_BIRTH_MONTH" in (1,2,6,8,9,12) and "cs_bill_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')
  ),
  abundant as (
  SELECT
      "cs_bill_customer_address_customer_address"."CA_COUNTRY" as "cs_bill_customer_address_country",
      "cs_bill_customer_address_customer_address"."CA_COUNTY" as "cs_bill_customer_address_county",
      "cs_bill_customer_address_customer_address"."CA_STATE" as "cs_bill_customer_address_state",
      "cs_item_items"."I_ITEM_ID" as "cs_item_text_id"
  FROM
      "thoughtful"
      INNER JOIN "item" as "cs_item_items" on "thoughtful"."cs_item_id" = "cs_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "thoughtful"."cs_sold_date_id" = "cs_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "cs_bill_customer_customers" on "thoughtful"."cs_bill_customer_id" = "cs_bill_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "cs_bill_customer_address_customer_address" on "cs_bill_customer_customers"."C_CURRENT_ADDR_SK" = "cs_bill_customer_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "thoughtful"."cs_bill_customer_demographic_id" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 1998 and "cs_bill_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_bill_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_bill_customer_customers"."C_BIRTH_MONTH" in (1,2,6,8,9,12) and "cs_bill_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')

  GROUP BY
      1,
      2,
      3,
      4,
      "cs_bill_customer_address_customer_address"."CA_ADDRESS_SK"),
  juicy as (
  SELECT
      "yummy"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
      "yummy"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
      "yummy"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
      avg("yummy"."cs_bill_customer_birth_year") as "avg_birth_year",
      avg("yummy"."cs_bill_customer_demographic_dependent_count") as "avg_dependent_count",
      avg("yummy"."cs_coupon_amt") as "avg_coupon_amount",
      avg("yummy"."cs_list_price") as "avg_list_price",
      avg("yummy"."cs_net_profit") as "avg_net_profit",
      avg("yummy"."cs_quantity") as "avg_ticket_quantity",
      avg("yummy"."cs_sales_price") as "avg_sales_price"
  FROM
      "yummy"
  GROUP BY
      ROLLUP (1, 3, 2)),
  uneven as (
  SELECT
      "abundant"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
      "abundant"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
      "abundant"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
      "abundant"."cs_item_text_id" as "cs_item_text_id",
      grouping("abundant"."cs_bill_customer_address_country") as "g_country",
      grouping("abundant"."cs_bill_customer_address_county") as "g_county",
      grouping("abundant"."cs_bill_customer_address_state") as "g_state"
  FROM
      "abundant"
  GROUP BY
      ROLLUP (1, 3, 2))
  SELECT
      "uneven"."cs_item_text_id" as "item_code",
      coalesce("juicy"."cs_bill_customer_address_country","uneven"."cs_bill_customer_address_country") as "country",
      coalesce("juicy"."cs_bill_customer_address_state","uneven"."cs_bill_customer_address_state") as "state",
      coalesce("juicy"."cs_bill_customer_address_county","uneven"."cs_bill_customer_address_county") as "county",
      "uneven"."g_country" as "g_country",
      "uneven"."g_state" as "g_state",
      "uneven"."g_county" as "g_county",
      "juicy"."avg_ticket_quantity" as "avg_ticket_quantity",
      "juicy"."avg_list_price" as "avg_list_price",
      "juicy"."avg_coupon_amount" as "avg_coupon_amount",
      "juicy"."avg_sales_price" as "avg_sales_price",
      "juicy"."avg_net_profit" as "avg_net_profit",
      "juicy"."avg_birth_year" as "avg_birth_year",
      "juicy"."avg_dependent_count" as "avg_dependent_count"
  FROM
      "uneven"
      INNER JOIN "juicy" on "uneven"."cs_bill_customer_address_country" is not distinct from "juicy"."cs_bill_customer_address_country" AND "uneven"."cs_bill_customer_address_county" is not distinct from "juicy"."cs_bill_customer_address_county" AND "uneven"."cs_bill_customer_address_state" is not distinct from "juicy"."cs_bill_customer_address_state"
  ORDER BY
      "uneven"."g_country" asc,
      "uneven"."g_state" asc,
      "uneven"."g_county" asc,
      "country" asc nulls first,
      "state" asc nulls first,
      "county" asc nulls first,
      "item_code" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query20.preql`

  ```text
  Resolution error in query20.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cs.sold_date.date, local.pct_of_class, local.total_ext_sales_price}; {i.category, i.class, i.current_price, local.description, local.item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query22.preql`

  ```text
  Resolution error in query22.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.year}; {item.brand_name, item.category, item.class, item.product_name}; {local.avg_on_hand}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Undefined concept: ps.sold_date.year. Suggestions: ['cs.date.year', 'cs.ship_date.year', 'cs.sold_date.year', 'cs.ship_customer.first_sales_date.year', 'cs.ship_customer.first_shipto_date.year', 'cs.bill_customer.first_sales_date.year']
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Output column 'last_name' aliases 'combined.last_name', which is itself the 'last_name' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'last_name_out').
  ```
- `trilogy run query24.preql`

  ```text
  Syntax error in query24.preql: 6 undefined concept references; fix all before re-running:
    - stage2.cust_last_name (line 40, col 5, in SELECT); did you mean: stage2.stage1.cust_last_name, stage1.cust_last_name, stage2.cust_first_name, _stage1_cust_last_name?
    - stage2.cust_first_name (line 41, col 5, in SELECT); did you mean: stage2.stage1.cust_first_name, stage1.cust_first_name, _stage1_cust_first_name, stage2.cust_last_name?
    - stage2.store_name (line 42, col 5, in SELECT); did you mean: stage2.stage1.store_name, stage1.store_name, _stage1_store_name?
    - stage2.cust_last_name (line 44, col 10, in ORDER BY); did you mean: stage2.stage1.cust_last_name, stage1.cust_last_name, stage2.cust_first_name, _stage1_cust_last_name?
    - stage2.cust_first_name (line 44, col 33, in ORDER BY); did you mean: stage2.stage1.cust_first_name, stage1.cust_first_name, _stage1_cust_first_name, stage2.cust_last_name?
    - stage2.store_name (line 44, col 57, in ORDER BY); did you mean: stage2.stage1.store_name, stage1.store_name, _stage1_store_name?
  ```
- `trilogy run query24.preql`

  ```text
  Syntax error in query24.preql: 6 undefined concept references; fix all before re-running:
    - stage2.cust_last_name (line 40, col 5, in SELECT); did you mean: stage2.stage1.cust_last_name, stage1.cust_last_name, stage2.cust_first_name, _stage1_cust_last_name?
    - stage2.cust_first_name (line 41, col 5, in SELECT); did you mean: stage2.stage1.cust_first_name, stage1.cust_first_name, _stage1_cust_first_name, stage2.cust_last_name?
    - stage2.store_name (line 42, col 5, in SELECT); did you mean: stage2.stage1.store_name, stage1.store_name, _stage1_store_name?
    - stage2.cust_last_name (line 44, col 10, in ORDER BY); did you mean: stage2.stage1.cust_last_name, stage1.cust_last_name, stage2.cust_first_name, _stage1_cust_last_name?
    - stage2.cust_first_name (line 44, col 33, in ORDER BY); did you mean: stage2.stage1.cust_first_name, stage1.cust_first_name, _stage1_cust_first_name, stage2.cust_last_name?
    - stage2.store_name (line 44, col 57, in ORDER BY); did you mean: stage2.stage1.store_name, stage1.store_name, _stage1_store_name?
  ```
- `trilogy run query24.preql`

  ```text
  Syntax error in query24.preql: HAVING references 'local.peach_subtotal', 'local.avg_stage1', which are not in the SELECT projection (line 27). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.peach_subtotal, --local.avg_stage1
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query26.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query27.preql`

  ```text
  Resolution error in query27.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 4 disconnected subgraphs: {date.year}; {item.text_id}; {local.avg_coupon_amt, local.avg_list_price, local.avg_ticket_qty, local.avg_unit_price, ss.customer_demographic.education_status, ss.customer_demographic.gender, ss.customer_demographic.marital_status}; {local.group_indicator, store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Undefined concept: local.catalog_pairs (line 18, col 39, in WHERE). Suggestions: ['cs.catalog_page.id', 'catalog_pairs.cat_item_id', 'catalog_pairs.cat_cust_id']
  ```
- `trilogy run query30.preql duckdb`

  ```text
  Syntax error in query30.preql: 11 undefined concept references; fix all before re-running:
    - salutation (line 30, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.salutation, wr.web_sales.ship_customer.salutation, wr.web_sales.return_customer.salutation, wr.web_sales.return_refund_customer.salutation, wr.billing_customer.salutation, wr.refunded_customer.salutation?
    - first_name (line 31, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.first_name, wr.web_sales.ship_customer.first_name, wr.web_sales.return_customer.first_name, wr.web_sales.return_refund_customer.first_name, wr.billing_customer.first_name, wr.refunded_customer.first_name?
    - last_name (line 32, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.last_name, wr.web_sales.ship_customer.last_name, wr.web_sales.return_customer.last_name, wr.web_sales.return_refund_customer.last_name, wr.billing_customer.last_name, wr.refunded_customer.last_name?
    - preferred_cust_flag (line 33, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.preferred_cust_flag, wr.web_sales.ship_customer.preferred_cust_flag, wr.web_sales.return_customer.preferred_cust_flag, wr.web_sales.return_refund_customer.preferred_cust_flag, wr.billing_customer.preferred_cust_flag, wr.refunded_customer.preferred_cust_flag?
    - birth_day (line 34, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_day, wr.web_sales.ship_customer.birth_day, wr.web_sales.return_customer.birth_day, wr.web_sales.return_refund_customer.birth_day, wr.billing_customer.birth_day, wr.refunded_customer.birth_day?
    - birth_month (line 35, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_month, wr.web_sales.ship_customer.birth_month, wr.web_sales.return_customer.birth_month, wr.web_sales.return_refund_customer.birth_month, wr.billing_customer.birth_month, wr.refunded_customer.birth_month?
    - birth_year (line 36, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_year, wr.web_sales.ship_customer.birth_year, wr.web_sales.return_customer.birth_year, wr.web_sales.return_refund_customer.birth_year, wr.billing_customer.birth_year, wr.refunded_customer.birth_year?
    - birth_country (line 37, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_country, wr.web_sales.ship_customer.birth_country, wr.web_sales.return_customer.birth_country, wr.web_sales.return_refund_customer.birth_country, wr.billing_customer.birth_country, wr.refunded_customer.birth_country?
    - login (line 38, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.login, wr.web_sales.ship_customer.login, wr.web_sales.return_customer.login, wr.web_sales.return_refund_customer.login, wr.billing_customer.login, wr.refunded_customer.login?
    - email_address (line 39, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.email_address, wr.web_sales.ship_customer.email_address, wr.web_sales.return_customer.email_address, wr.web_sales.return_refund_customer.email_address, wr.billing_customer.email_address, wr.refunded_customer.email_address?
    - last_review_date (line 40, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.last_review_date, wr.web_sales.ship_customer.last_review_date, wr.web_sales.return_customer.last_review_date, wr.web_sales.return_refund_customer.last_review_date, wr.billing_customer.last_review_date, wr.refunded_customer.last_review_date?
  ```
- `trilogy run query30.preql duckdb`

  ```text
  Syntax error in query30.preql: HAVING references 'local.state_avg_amt', 'wr.billing_customer.address.state', which are not in the SELECT projection (line 11). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.state_avg_amt, --wr.billing_customer.address.state
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query33.preql`

  ```text
  Syntax error in query33.preql: Undefined concept: item.category. Suggestions: ['all_sales.item.category', 'all_sales.item.category_id']
  ```
- `trilogy run query35.preql duckdb`

  ```text
  Syntax error in query35.preql: 3 undefined concept references; fix all before re-running:
    - store_customers.id (line 31, col 34, in WHERE); did you mean: store_customers.store_sales.customer.id, store_sales.item.id, store_sales.date.id, store_sales.return_date.id, store_sales.time.id, store_sales.return_time.id?
    - web_customers.id (line 32, col 35, in WHERE); did you mean: web_customers.web_sales.billing_customer.id, store_sales.item.id, store_sales.date.id, store_sales.return_date.id, store_sales.time.id, store_sales.return_time.id?
    - catalog_customers.id (line 32, col 82, in WHERE); did you mean: catalog_customers.catalog_sales.ship_customer.id, store_sales.item.id, store_sales.date.id, store_sales.return_date.id, store_sales.time.id, store_sales.return_time.id?
  ```
- `trilogy run query36.preql`

  ```text
  Resolution error in query36.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.year}; {item.category, item.class, local.gross_margin_ratio, local.hierarchy_level, local.rnk}; {store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query39.preql`

  ```text
  Syntax error in query39.preql: Undefined concept: warehouse.id. Suggestions: ['inv.warehouse.id', 'inv.item.id', 'inv.date.id', 'id', 'inv.warehouse.city', 'inv.warehouse.text_id']
  ```
- `trilogy run query39.preql`

  ```text
  Syntax error in query39.preql: Undefined concept: qualifying.mo. Suggestions: ['qualifying.monthly_stats.mo', 'monthly_stats.mo', 'qualifying.cv', '_qualifying_cv', 'qualifying.monthly_stats.m']
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Invalid reference string found in query:
  WITH
  wakeful as (
  SELECT
      "inv_date_date"."D_MOY" as "inv_date_month_of_year",
      "inv_warehouse_inventory"."inv_item_sk" as "inv_item_id",
      "inv_warehouse_inventory"."inv_quantity_on_hand" as "inv_quantity_on_hand",
      "inv_warehouse_inventory"."inv_warehouse_sk" as "inv_warehouse_id"
  FROM
      "inventory" as "inv_warehouse_inventory"
      INNER JOIN "date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
  WHERE
      "inv_date_date"."D_YEAR" = 2001
  ),
  cooperative as (
  SELECT
      "wakeful"."inv_date_month_of_year" as "monthly_stats_mo",
      "wakeful"."inv_item_id" as "monthly_stats_item_id",
      "wakeful"."inv_warehouse_id" as "monthly_stats_wh_id",
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END as "_jan_cv",
      avg("wakeful"."inv_quantity_on_hand") as "monthly_stats_m"
  FROM
      "wakeful"
  WHERE
      "wakeful"."inv_date_month_of_year" = 1

  GROUP BY
      1,
      2,
      3),
  cheerful as (
  SELECT
      "wakeful"."inv_date_month_of_year" as "monthly_stats_mo",
      "wakeful"."inv_item_id" as "monthly_stats_item_id",
      "wakeful"."inv_warehouse_id" as "monthly_stats_wh_id",
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END as "_feb_cv",
      avg("wakeful"."inv_quantity_on_hand") as "monthly_stats_m"
  FROM
      "wakeful"
  WHERE
      "wakeful"."inv_date_month_of_year" = 2

  GROUP BY
      1,
      2,
      3),
  questionable as (
  SELECT
      "cooperative"."_jan_cv" as "jan_cv",
      "cooperative"."monthly_stats_item_id" as "jan_monthly_stats_item_id",
      "cooperative"."monthly_stats_m" as "jan_monthly_stats_m",
      "cooperative"."monthly_stats_mo" as "jan_monthly_stats_mo",
      "cooperative"."monthly_stats_wh_id" as "jan_monthly_stats_wh_id"
  FROM
      "cooperative"
  WHERE
      ( "cooperative"."monthly_stats_m" = 0 and stddev_samp(INVALID_REFERENCE_BUG) is not null ) or ( "cooperative"."monthly_stats_m" != 0 and ( stddev_samp(INVALID_REFERENCE_BUG) / "cooperative"."monthly_stats_m" ) > 1 )
  ),
  thoughtful as (
  SELECT
      "cheerful"."_feb_cv" as "feb_cv",
      "cheerful"."monthly_stats_item_id" as "feb_monthly_stats_item_id",
      "cheerful"."monthly_stats_m" as "feb_monthly_stats_m",
      "cheerful"."monthly_stats_mo" as "feb_monthly_stats_mo",
      "cheerful"."monthly_stats_wh_id" as "feb_monthly_stats_wh_id"
  FROM
      "cheerful"
  WHERE
      ( "cheerful"."monthly_stats_m" = 0 and stddev_samp(INVALID_REFERENCE_BUG) is not null ) or ( "cheerful"."monthly_stats_m" != 0 and ( stddev_samp(INVALID_REFERENCE_BUG) / "cheerful"."monthly_stats_m" ) > 1 )
  )
  SELECT
      "thoughtful"."feb_monthly_stats_wh_id" as "warehouse_surrogate_id_jan",
      "thoughtful"."feb_monthly_stats_item_id" as "item_jan",
      "questionable"."jan_monthly_stats_mo" as "month_of_year_jan",
      "questionable"."jan_monthly_stats_m" as "mean_jan",
      "questionable"."jan_cv" as "coefficient_of_variation_jan",
      "thoughtful"."feb_monthly_stats_wh_id" as "warehouse_surrogate_id_feb",
      "thoughtful"."feb_monthly_stats_item_id" as "item_feb",
      "thoughtful"."feb_monthly_stats_mo" as "month_of_year_feb",
      "thoughtful"."feb_monthly_stats_m" as "mean_feb",
      "thoughtful"."feb_cv" as "coefficient_of_variation_feb"
  FROM
      "questionable"
      INNER JOIN "thoughtful" on "questionable"."jan_monthly_stats_item_id" = "thoughtful"."feb_monthly_stats_item_id" AND "questionable"."jan_monthly_stats_wh_id" = "thoughtful"."feb_monthly_stats_wh_id"
  ORDER BY
      "warehouse_surrogate_id_jan" asc nulls first,
      "item_jan" asc nulls first,
      "month_of_year_jan" asc nulls first,
      "mean_jan" asc nulls first,
      "coefficient_of_variation_jan" asc nulls first,
      "month_of_year_feb" asc nulls first,
      "mean_feb" asc nulls first,
      "coefficient_of_variation_feb" asc nulls first, this should never occur. Please create an issue to report this.
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file exists query42.preql`

  ```text
  {
    "event": "exists",
    "path": "query42.preql",
    "exists": false
  }
  ```
- `trilogy run query43.preql`

  ```text
  Syntax error in query43.preql: Undefined concept: date.day_name. Suggestions: ['ps.date.day_name', 'ps.store.date.day_name', 'ps.return_store.date.day_name', 'ps.return_date.day_name', 'ps.customer.first_sales_date.day_name', 'ps.customer.first_shipto_date.day_name']
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: ranked_items.product_name. Suggestions: ['ranked_items.sales.item.product_name', 'sales.item.product_name', 'ranked_items.avg_net_profit']
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: ranked_items.sales_item_product_name. Suggestions: ['ranked_items.sales.item.product_name', 'sales.item.product_name', 'ranked_items.avg_net_profit']
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: qualifying_items.avg_net_profit. Suggestions: ['qualifying_items.ranked_items.avg_net_profit', 'ranked_items.avg_net_profit', '_ranked_items_avg_net_profit']
  ```
- `trilogy run --import raw/physical_sales:sales with ranked_items as where sales.store.id = 1 select sales.item.product_name as pn, avg(sales.net_profit) as av….avg_net_profit > 0 select ranked_items.pn as pn, ranked_items.avg_net_profit as avg_np; select qualifying_items.pn, qualifying_items.avg_np limit 3;`

  ```text
  Unexpected error in stdin: (_duckdb.BinderException) Binder Error: WHERE clause cannot contain aggregates!

  LINE 24:     avg("wakeful"."sales_net_profit") > 0
               ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "sales_item_items"."I_PRODUCT_NAME" as "sales_item_product_name",
      "sales_store_sales"."SS_NET_PROFIT" as "sales_net_profit"
  FROM
      "store_sales" as "sales_store_sales"
      INNER JOIN "item" as "sales_item_items" on "sales_store_sales"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_store_sales"."SS_STORE_SK" = 1

  GROUP BY
      1,
      2,
      "sales_item_items"."I_ITEM_SK",
      "sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "wakeful"."sales_item_product_name" as "qualifying_items_pn",
      avg("wakeful"."sales_net_profit") as "qualifying_items_avg_np"
  FROM
      "wakeful"
  WHERE
      avg("wakeful"."sales_net_profit") > 0

  GROUP BY
      1
  LIMIT (3)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.null_addr_threshold', which is not in the SELECT projection (line 9). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.null_addr_threshold
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw/physical_sales:sales select sales.item.product_name as pn, avg(sales.net_profit ? sales.store.id = 1) as avg_np where sales.store.id = 1 having avg(sales.net_profit) > -738.04 order by avg_np asc limit 20;`

  ```text
  Unexpected error in stdin: Invalid reference string found in query:
  WITH
  wakeful as (
  SELECT
      "sales_item_items"."I_PRODUCT_NAME" as "sales_item_product_name",
      "sales_store_sales"."SS_NET_PROFIT" as "_virt_filter_net_profit_5864984486180729"
  FROM
      "store_sales" as "sales_store_sales"
      INNER JOIN "item" as "sales_item_items" on "sales_store_sales"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
  WHERE
      "sales_store_sales"."SS_STORE_SK" = 1

  GROUP BY
      1,
      2,
      "sales_item_items"."I_ITEM_SK",
      "sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "wakeful"."sales_item_product_name" as "pn",
      avg("wakeful"."_virt_filter_net_profit_5864984486180729") as "avg_np"
  FROM
      "wakeful"
  GROUP BY
      1
  HAVING
      avg(INVALID_REFERENCE_BUG) > -738.04

  ORDER BY
      "avg_np" asc
  LIMIT (20), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.null_addr_threshold', which is not in the SELECT projection (line 9). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.null_addr_threshold
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw/physical_sales:sales auto null_addr_threshold <- 0.9 * avg(sales.net_profit ? sales.store.id = 1 and sales.sale_address.id is null) …ked_items.rank_asc, ranked_items.rank_desc
where ranked_items.rank_asc <= 10 or ranked_items.rank_desc <= 10
order by ranked_items.rank_asc
limit 20;`

  ```text
  Syntax error in stdin: Undefined concept: ranked_items.avg_net_profit (line 22, col 25, in SELECT). Suggestions: ['ranked_items.items_at_store.avg_net_profit', 'items_at_store.avg_net_profit', '_items_at_store_avg_net_profit']
  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query45.preql`

  ```text
  Syntax error in query45.preql: Undefined concept: item.id. Suggestions: ['ws.item.id', 'ws.date.id', 'ws.ship_date.id', 'ws.return_date.id', 'ws.time.id', 'ws.return_time.id']
  ```
- `trilogy run query47.preql`

  ```text
  Syntax error in query47.preql: Undefined concept: monthly_totals.category. Suggestions: ['monthly_totals.s.item.category', 's.item.category', 'monthly_totals.s.date.year', 'monthly_totals.s.store.name']
  ```
- `trilogy run query47.preql duck_db`

  ```text
  Syntax error in query47.preql: Undefined concept: monthly_totals.category. Suggestions: ['monthly_totals.s.item.category', 's.item.category', 'monthly_totals.s.date.year', 'monthly_totals.s.store.name']
  ```
- `trilogy run query47.preql duck_db`

  ```text
  Syntax error in query47.preql: 16 undefined concept references; fix all before re-running:
    - annual_avgs.s.item.category (line 40, col 5, in SELECT); did you mean: annual_avgs.monthly_totals.s.item.category, s.item.category, monthly_totals.s.item.category, annual_avgs.s.date.year?
    - annual_avgs.s.item.brand_name (line 41, col 5, in SELECT); did you mean: annual_avgs.monthly_totals.s.item.brand_name, s.item.brand_name, monthly_totals.s.item.brand_name, annual_avgs.s.store.name, annual_avgs.s.store.company_name?
    - annual_avgs.s.store.name (line 42, col 5, in SELECT); did you mean: annual_avgs.monthly_totals.s.store.name, s.store.name, s.return_store.name, monthly_totals.s.store.name, annual_avgs.s.store.company_name, annual_avgs.s.item.brand_name?
    - annual_avgs.s.store.company_name (line 43, col 5, in SELECT); did you mean: annual_avgs.monthly_totals.s.store.company_name, s.store.company_name, s.return_store.company_name, monthly_totals.s.store.company_name, annual_avgs.s.store.name, annual_avgs.s.item.brand_name?
    - annual_avgs.s.date.year (line 44, col 5, in SELECT); did you mean: annual_avgs.monthly_totals.s.date.year, s.date.year, s.return_date.year, s.customer.first_sales_date.year, s.customer.first_shipto_date.year, s.return_customer.first_sales_date.year?
    - annual_avgs.s.date.month_of_year (line 45, col 5, in SELECT); did you mean: annual_avgs.monthly_totals.s.date.month_of_year, s.date.month_of_year, s.return_date.month_of_year, s.customer.first_sales_date.month_of_year, s.customer.first_shipto_date.month_of_year, s.return_customer.first_sales_date.month_of_year?
    - annual_avgs.month_total (line 47, col 5, in SELECT); did you mean: annual_avgs.monthly_totals.month_total, monthly_totals.month_total, annual_avgs.avg_monthly_sales, annual_avgs.monthly_totals.s.date.year?
    - annual_avgs.month_total (line 39, in SELECT); did you mean: annual_avgs.monthly_totals.month_total, monthly_totals.month_total, annual_avgs.avg_monthly_sales, annual_avgs.monthly_totals.s.date.year?
    - annual_avgs.month_total (line 39, in ORDER BY); did you mean: annual_avgs.monthly_totals.month_total, monthly_totals.month_total, annual_avgs.avg_monthly_sales, annual_avgs.monthly_totals.s.date.year?
    - annual_avgs.s.item.category (line 62, col 5, in ORDER BY); did you mean: annual_avgs.monthly_totals.s.item.category, s.item.category, monthly_totals.s.item.category, annual_avgs.s.date.year?
    - annual_avgs.s.item.brand_name (line 63, col 5, in ORDER BY); did you mean: annual_avgs.monthly_totals.s.item.brand_name, s.item.brand_name, monthly_totals.s.item.brand_name, annual_avgs.s.store.name, annual_avgs.s.store.company_name?
    - annual_avgs.s.store.name (line 64, col 5, in ORDER BY); did you mean: annual_avgs.monthly_totals.s.store.name, s.store.name, s.return_store.name, monthly_totals.s.store.name, annual_avgs.s.store.company_name, annual_avgs.s.item.brand_name?
    - annual_avgs.s.store.company_name (line 65, col 5, in ORDER BY); did you mean: annual_avgs.monthly_totals.s.store.company_name, s.store.company_name, s.return_store.company_name, monthly_totals.s.store.company_name, annual_avgs.s.store.name, annual_avgs.s.item.brand_name?
    - annual_avgs.s.date.year (line 66, col 5, in ORDER BY); did you mean: annual_avgs.monthly_totals.s.date.year, s.date.year, s.return_date.year, s.customer.first_sales_date.year, s.customer.first_shipto_date.year, s.return_customer.first_sales_date.year?
    - annual_avgs.s.date.month_of_year (line 67, col 5, in ORDER BY); did you mean: annual_avgs.monthly_totals.s.date.month_of_year, s.date.month_of_year, s.return_date.month_of_year, s.customer.first_sales_date.month_of_year, s.customer.first_shipto_date.month_of_year, s.return_customer.first_sales_date.month_of_year?
    - annual_avgs.month_total (line 69, col 5, in ORDER BY); did you mean: annual_avgs.monthly_totals.month_total, monthly_totals.month_total, annual_avgs.avg_monthly_sales, annual_avgs.monthly_totals.s.date.year?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query50.preql`

  ```text
  Syntax error in query50.preql: 2 undefined concept references; fix all before re-running:
    - return_date.year (line 3, col 7, in WHERE); did you mean: ss.return_date.year, ss.date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.return_customer.first_sales_date.year, ss.return_customer.first_shipto_date.year?
    - return_date.month_of_year (line 4, col 7, in WHERE); did you mean: ss.return_date.month_of_year, ss.date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year, ss.return_customer.first_shipto_date.month_of_year?
  ```
- `trilogy run query51.preql`

  ```text
  Syntax error in query51.preql: Undefined concept: web_running.item_id. Suggestions: ['web_running.web_daily.item_id', 'web_daily.item_id', 'store_daily.item_id', 'store_running.store_daily.item_id']
  ```
- `trilogy run query51.preql`

  ```text
  Syntax error in query51.preql: Undefined concept: store_running.sale_date.
  ```
- `trilogy run query51.preql`

  ```text
  Syntax error in query51.preql: 8 undefined concept references; fix all before re-running:
    - combined.item_id (line 33, col 5, in SELECT); did you mean: combined.web_daily.item_id, web_daily.item_id, store_daily.item_id, combined.store_price, combined.web_price?
    - combined.sale_date (line 34, col 5, in SELECT); did you mean: combined.web_daily.sale_date, web_daily.sale_date, store_daily.sale_date, combined.store_price, combined.web_daily.item_id?
    - combined.sale_date (line 35, col 74, in SELECT); did you mean: combined.web_daily.sale_date, web_daily.sale_date, store_daily.sale_date, combined.store_price, combined.web_daily.item_id?
    - combined.sale_date (line 36, col 76, in SELECT); did you mean: combined.web_daily.sale_date, web_daily.sale_date, store_daily.sale_date, combined.store_price, combined.web_daily.item_id?
    - combined.sale_date (line 37, col 74, in SELECT); did you mean: combined.web_daily.sale_date, web_daily.sale_date, store_daily.sale_date, combined.store_price, combined.web_daily.item_id?
    - combined.sale_date (line 38, col 76, in SELECT); did you mean: combined.web_daily.sale_date, web_daily.sale_date, store_daily.sale_date, combined.store_price, combined.web_daily.item_id?
    - combined.item_id (line 41, col 10, in ORDER BY); did you mean: combined.web_daily.item_id, web_daily.item_id, store_daily.item_id, combined.store_price, combined.web_price?
    - combined.sale_date (line 41, col 44, in ORDER BY); did you mean: combined.web_daily.sale_date, web_daily.sale_date, store_daily.sale_date, combined.store_price, combined.web_daily.item_id?
  ```
- `trilogy run query51.preql`

  ```text
  Resolution error in query51.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {store_daily.daily_price, web_daily.daily_price, web_daily.item_id, web_daily.sale_date}
  ```
- `trilogy run query51.preql`

  ```text
  Syntax error in query51.preql: Undefined concept: web_running.item_id. Suggestions: ['web_running.web_daily.item_id', 'web_daily.item_id', 'store_daily.item_id', 'store_running.store_daily.item_id']
  ```
- `trilogy run query51.preql`

  ```text
  Syntax error in query51.preql: Undefined concept: web_daily. Suggestions: ['web_daily.item_id', '_web_daily_item_id', 'web_daily.sale_date']
  ```
- `trilogy run query51.preql`

  ```text
  Unexpected error in query51.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_sales_unified"."CS_SALES_PRICE" as "all_sales_sales_price",
      cast("all_sales_date_date"."D_DATE" as date) as "all_sales_date_date"
  FROM
      "catalog_sales" as "all_sales_catalog_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      (  'CATALOG'  = 'WEB' or  'CATALOG'  = 'STORE' ) and year(cast("all_sales_date_date"."D_DATE" as date)) = 2000

  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_sales_unified"."SS_ITEM_SK" as "all_sales_item_id",
      "all_sales_store_sales_unified"."SS_SALES_PRICE" as "all_sales_sales_price",
      cast("all_sales_date_date"."D_DATE" as date) as "all_sales_date_date"
  FROM
      "store_sales" as "all_sales_store_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_store_sales_unified"."SS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      (  'STORE'  = 'WEB' or  'STORE'  = 'STORE' ) and year(cast("all_sales_date_date"."D_DATE" as date)) = 2000

  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_ITEM_SK" as "all_sales_item_id",
      "all_sales_web_sales_unified"."WS_SALES_PRICE" as "all_sales_sales_price",
      cast("all_sales_date_date"."D_DATE" as date) as "all_sales_date_date"
  FROM
      "web_sales" as "all_sales_web_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_web_sales_unified"."WS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      (  'WEB'  = 'WEB' or  'WEB'  = 'STORE' ) and year(cast("all_sales_date_date"."D_DATE" as date)) = 2000
  ),
  questionable as (
  SELECT
      "all_sales_item_items"."I_ITEM_ID" as "all_sales_item_text_id",
      "cheerful"."all_sales_channel" as "all_sales_channel",
      "cheerful"."all_sales_date_date" as "all_sales_date_date",
      "cheerful"."all_sales_sales_price" as "all_sales_sales_price"
  FROM
      "cheerful"
      INNER JOIN "item" as "all_sales_item_items" on "cheerful"."all_sales_item_id" = "all_sales_item_items"."I_ITEM_SK"
  WHERE
      ( "cheerful"."all_sales_channel" = 'WEB' or "cheerful"."all_sales_channel" = 'STORE' )
  ),
  abundant as (
  SELECT
      "questionable"."all_sales_date_date" as "all_sales_date_date",
      "questionable"."all_sales_item_text_id" as "all_sales_item_text_id",
      CASE WHEN "questionable"."all_sales_channel" = 'STORE' THEN "questionable"."all_sales_sales_price" ELSE NULL END as "_virt_filter_sales_price_2327061012734831",
      CASE WHEN "questionable"."all_sales_channel" = 'WEB' THEN "questionable"."all_sales_sales_price" ELSE NULL END as "_virt_filter_sales_price_3173439316457841"
  FROM
      "questionable"),
  uneven as (
  SELECT
      "abundant"."_virt_filter_sales_price_2327061012734831" as "_virt_filter_sales_price_2327061012734831",
      "abundant"."_virt_filter_sales_price_3173439316457841" as "_virt_filter_sales_price_3173439316457841",
      "abundant"."all_sales_date_date" as "all_sales_date_date",
      "abundant"."all_sales_item_text_id" as "all_sales_item_text_id",
      sum("abundant"."_virt_filter_sales_price_2327061012734831") over (partition by "abundant"."all_sales_item_text_id" order by "abundant"."all_sales_date_date" asc ) as "store_rt",
      sum("abundant"."_virt_filter_sales_price_3173439316457841") over (partition by "abundant"."all_sales_item_text_id" order by "abundant"."all_sales_date_date" asc ) as "web_rt"
  FROM
      "abundant"),
  vacuous as (
  SELECT
      "uneven"."all_sales_date_date" as "all_sales_date_date",
      "uneven"."all_sales_item_text_id" as "all_sales_item_text_id",
      "uneven"."web_rt" as "web_rt"
  FROM
      "uneven"
  GROUP BY
      1,
      2,
      3,
      "uneven"."_virt_filter_sales_price_3173439316457841"),
  yummy as (
  SELECT
      "uneven"."all_sales_date_date" as "all_sales_date_date",
      "uneven"."all_sales_item_text_id" as "all_sales_item_text_id",
      "uneven"."store_rt" as "store_rt"
  FROM
      "uneven"
  GROUP BY
      1,
      2,
      3,
      "uneven"."_virt_filter_sales_price_2327061012734831"),
  concerned as (
  SELECT
      "vacuous"."all_sales_date_date" as "all_sales_date_date",
      "vacuous"."all_sales_item_text_id" as "all_sales_item_text_id",
      max("vacuous"."web_rt") as "_virt_agg_max_9443155110582121",
      max("vacuous"."web_rt") as "web_running_total"
  FROM
      "vacuous"
  GROUP BY
      1,
      2),
  juicy as (
  SELECT
      "yummy"."all_sales_date_date" as "all_sales_date_date",
      "yummy"."all_sales_item_text_id" as "all_sales_item_text_id",
      max("yummy"."store_rt") as "_virt_agg_max_7361222774745484",
      max("yummy"."store_rt") as "store_running_total"
  FROM
      "yummy"
  GROUP BY
      1,
      2),
  young as (
  SELECT
      "juicy"."all_sales_date_date" as "all_sales_date_date",
      "juicy"."all_sales_item_text_id" as "all_sales_item_text_id",
      max("concerned"."_virt_agg_max_9443155110582121") over (partition by "juicy"."all_sales_item_text_id" order by "juicy"."all_sales_date_date" asc ) as "web_running_max",
      max("juicy"."_virt_agg_max_7361222774745484") over (partition by "juicy"."all_sales_item_text_id" order by "juicy"."all_sales_date_date" asc ) as "store_running_max"
  FROM
      "concerned"
      INNER JOIN "juicy" on "concerned"."all_sales_date_date" = "juicy"."all_sales_date_date" AND "concerned"."all_sales_item_text_id" = "juicy"."all_sales_item_text_id"),
  abhorrent as (
  SELECT
      "concerned"."web_running_total" as "web_running_total",
      "young"."all_sales_date_date" as "all_sales_date_date",
      "young"."all_sales_item_text_id" as "all_sales_item_text_id",
      "young"."store_running_max" as "store_running_max",
      "young"."web_running_max" as "web_running_max"
  FROM
      "concerned"
      INNER JOIN "young" on "concerned"."all_sales_date_date" = "young"."all_sales_date_date" AND "concerned"."all_sales_item_text_id" = "young"."all_sales_item_text_id")
  SELECT
      "juicy"."all_sales_item_text_id" as "item_id",
      "juicy"."all_sales_date_date" as "sale_date",
      "abhorrent"."web_running_total" as "web_running_total",
      "juicy"."store_running_total" as "store_running_total",
      "abhorrent"."web_running_max" as "web_running_max",
      "abhorrent"."store_running_max" as "store_running_max"
  FROM
      "abhorrent"
      INNER JOIN "juicy" on "abhorrent"."all_sales_date_date" = "juicy"."all_sales_date_date" AND "abhorrent"."all_sales_item_text_id" = "juicy"."all_sales_item_text_id"
  QUALIFY
      max(sum(CASE WHEN  'CATALOG'  = 'WEB' THEN INVALID_REFERENCE_BUG ELSE NULL END) over (partition by "juicy"."all_sales_item_text_id" order by "juicy"."all_sales_date_date" asc )) over (partition by "juicy"."all_sales_item_text_id" order by "juicy"."all_sales_date_date" asc ) > max(sum(CASE WHEN  'CATALOG'  = 'STORE' THEN INVALID_REFERENCE_BUG ELSE NULL END) over (partition by "juicy"."all_sales_item_text_id" order by "juicy"."all_sales_date_date" asc )) over (partition by "juicy"."all_sales_item_text_id" order by "juicy"."all_sales_date_date" asc )

  ORDER BY
      "item_id" asc nulls first,
      "sale_date" asc nulls first
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query53.preql`

  ```text
  Syntax error in query53.preql: Undefined concept: manufacturer_quarter_totals.manufacturer_id (line 32, col 3, in SELECT). Suggestions: ['manufacturer_quarter_totals.item.manufacturer_id', 'physical_sales.item.manufacturer_id', 'item.manufacturer_id', 'manufacturer_quarter_totals.date.quarter', 'manufacturer_quarter_totals.per_quarter_total']
  ```
- `trilogy run query53.preql`

  ```text
  Syntax error in query53.preql: Undefined concept: manufacturer_avg.item.manufacturer_id. Suggestions: ['manufacturer_avg.manufacturer_quarter_totals.item.manufacturer_id', 'physical_sales.item.manufacturer_id', 'item.manufacturer_id', 'manufacturer_quarter_totals.item.manufacturer_id']
  ```
- `trilogy run query53.preql`

  ```text
  Resolution error in query53.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.quarter, date.year}; {item.brand_name, item.category, item.class, item.manufacturer_id, local._manufacturer_quarter_totals_per_quarter_total}; {store.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query53.preql`

  ```text
  Resolution error in query53.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.quarter, date.year}; {item.brand_name, item.category, item.class, item.manufacturer_id, local._manufacturer_quarter_totals_per_quarter_total}; {store.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query53.preql`

  ```text
  Resolution error in query53.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 4 disconnected subgraphs: {date.quarter, date.year}; {item.brand_name, item.category, item.class, item.manufacturer_id}; {local.per_quarter_total}; {store.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.physical_sales:ps select count(ps.customer.id) as cnt where cs.sold_date.month_of_year = 12 and cs.sol…88 and 1190 and ps.store.county = ps.customer.address.county and ps.store.state = ps.customer.address.state and ps.customer.id = cs.bill_customer.id;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cs.bill_customer.id, cs.item.category, cs.item.class, cs.sold_date.month_of_year, cs.sold_date.year}; {ps.customer.address.county, ps.customer.address.state, ps.customer.id, ps.date.month_seq, ps.store.county, ps.store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw.physical_sales:ps --import raw.web_sales:ws select count(ps.customer.id) as cnt where ws.date.month_of_year = 12 and ws.date.year = …and 1190 and ps.store.county = ps.customer.address.county and ps.store.state = ps.customer.address.state and ps.customer.id = ws.billing_customer.id;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {ps.customer.address.county, ps.customer.address.state, ps.customer.id, ps.date.month_seq, ps.store.county, ps.store.state}; {ws.billing_customer.id, ws.date.month_of_year, ws.date.year, ws.item.category, ws.item.class}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query57.preql`

  ```text
  Syntax error in query57.preql: Duplicate select output for local.avg_monthly_sales; Line: 18
  ```
- `trilogy run query57.preql`

  ```text
  Unexpected error in query57.preql: Invalid reference string found in query:
  WITH
  thoughtful as (
  SELECT
      "cs_call_center_call_center"."CC_NAME" as "monthly_data_call_center_name",
      "cs_item_items"."I_BRAND" as "monthly_data_item_brand",
      "cs_item_items"."I_CATEGORY" as "monthly_data_item_category",
      "cs_sold_date_date"."D_MOY" as "monthly_data_mo",
      "cs_sold_date_date"."D_YEAR" as "monthly_data_yr",
      sum("cs_catalog_sales"."CS_SALES_PRICE") as "monthly_data_monthly_total"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "call_center" as "cs_call_center_call_center" on "cs_catalog_sales"."CS_CALL_CENTER_SK" = "cs_call_center_call_center"."CC_CALL_CENTER_SK"
  WHERE
      ( "cs_sold_date_date"."D_YEAR" = 1998 and "cs_sold_date_date"."D_MOY" = 12 ) or "cs_sold_date_date"."D_YEAR" = 1999 or ( "cs_sold_date_date"."D_YEAR" = 2000 and "cs_sold_date_date"."D_MOY" = 1 )

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  abundant as (
  SELECT
      "thoughtful"."monthly_data_call_center_name" as "monthly_data_call_center_name",
      "thoughtful"."monthly_data_item_brand" as "monthly_data_item_brand",
      "thoughtful"."monthly_data_item_category" as "monthly_data_item_category",
      "thoughtful"."monthly_data_yr" as "monthly_data_yr",
      avg("thoughtful"."monthly_data_monthly_total") as "avg_monthly_sales"
  FROM
      "thoughtful"
  GROUP BY
      1,
      2,
      3,
      4),
  questionable as (
  SELECT
      "thoughtful"."monthly_data_call_center_name" as "monthly_data_call_center_name",
      "thoughtful"."monthly_data_item_brand" as "monthly_data_item_brand",
      "thoughtful"."monthly_data_item_category" as "monthly_data_item_category",
      "thoughtful"."monthly_data_mo" as "monthly_data_mo",
      "thoughtful"."monthly_data_yr" as "monthly_data_yr",
      lag("thoughtful"."monthly_data_monthly_total", 1) over (partition by "thoughtful"."monthly_data_item_category","thoughtful"."monthly_data_item_brand","thoughtful"."monthly_data_call_center_name" order by "thoughtful"."monthly_data_yr" asc,"thoughtful"."monthly_data_mo" asc ) as "prev_month_total",
      lead("thoughtful"."monthly_data_monthly_total", 1) over (partition by "thoughtful"."monthly_data_item_category","thoughtful"."monthly_data_item_brand","thoughtful"."monthly_data_call_center_name" order by "thoughtful"."monthly_data_yr" asc,"thoughtful"."monthly_data_mo" asc ) as "next_month_total"
  FROM
      "thoughtful")
  SELECT
      "questionable"."monthly_data_item_category" as "category",
      "questionable"."monthly_data_item_brand" as "brand",
      "questionable"."monthly_data_call_center_name" as "call_center",
      "questionable"."monthly_data_yr" as "year",
      "questionable"."monthly_data_mo" as "month_of_year",
      "abundant"."avg_monthly_sales" as "avg_monthly_sales",
      "questionable"."prev_month_total" as "prev_month_total",
      "questionable"."next_month_total" as "next_month_total"
  FROM
      "questionable"
      INNER JOIN "abundant" on "questionable"."monthly_data_call_center_name" = "abundant"."monthly_data_call_center_name" AND "questionable"."monthly_data_item_brand" = "abundant"."monthly_data_item_brand" AND "questionable"."monthly_data_item_category" = "abundant"."monthly_data_item_category" AND "questionable"."monthly_data_yr" = "abundant"."monthly_data_yr"
  WHERE
      "questionable"."monthly_data_yr" = 1999 and "abundant"."avg_monthly_sales" > 0 and abs(INVALID_REFERENCE_BUG - "abundant"."avg_monthly_sales") / "abundant"."avg_monthly_sales" > 0.1

  ORDER BY
      ( INVALID_REFERENCE_BUG - "abundant"."avg_monthly_sales" ) asc nulls first,
      "category" asc,
      "brand" asc,
      "call_center" asc,
      "year" asc,
      "month_of_year" asc,
      INVALID_REFERENCE_BUG asc,
      "questionable"."prev_month_total" asc nulls first,
      "questionable"."next_month_total" asc nulls first
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query57.preql`

  ```text
  Unexpected error in query57.preql: Invalid reference string found in query:
  WITH
  thoughtful as (
  SELECT
      "cs_call_center_call_center"."CC_NAME" as "monthly_data_call_center_name",
      "cs_item_items"."I_BRAND" as "monthly_data_item_brand",
      "cs_item_items"."I_CATEGORY" as "monthly_data_item_category",
      "cs_sold_date_date"."D_MOY" as "monthly_data_mo",
      "cs_sold_date_date"."D_YEAR" as "monthly_data_yr",
      sum("cs_catalog_sales"."CS_SALES_PRICE") as "monthly_data_monthly_total"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "call_center" as "cs_call_center_call_center" on "cs_catalog_sales"."CS_CALL_CENTER_SK" = "cs_call_center_call_center"."CC_CALL_CENTER_SK"
  WHERE
      ( "cs_sold_date_date"."D_YEAR" = 1998 and "cs_sold_date_date"."D_MOY" = 12 ) or "cs_sold_date_date"."D_YEAR" = 1999 or ( "cs_sold_date_date"."D_YEAR" = 2000 and "cs_sold_date_date"."D_MOY" = 1 )

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  abundant as (
  SELECT
      "thoughtful"."monthly_data_call_center_name" as "monthly_data_call_center_name",
      "thoughtful"."monthly_data_item_brand" as "monthly_data_item_brand",
      "thoughtful"."monthly_data_item_category" as "monthly_data_item_category",
      "thoughtful"."monthly_data_yr" as "monthly_data_yr",
      avg("thoughtful"."monthly_data_monthly_total") as "avg_monthly_sales"
  FROM
      "thoughtful"
  GROUP BY
      1,
      2,
      3,
      4),
  questionable as (
  SELECT
      "thoughtful"."monthly_data_call_center_name" as "monthly_data_call_center_name",
      "thoughtful"."monthly_data_item_brand" as "monthly_data_item_brand",
      "thoughtful"."monthly_data_item_category" as "monthly_data_item_category",
      "thoughtful"."monthly_data_mo" as "monthly_data_mo",
      "thoughtful"."monthly_data_yr" as "monthly_data_yr",
      lag("thoughtful"."monthly_data_monthly_total", 1) over (partition by "thoughtful"."monthly_data_item_category","thoughtful"."monthly_data_item_brand","thoughtful"."monthly_data_call_center_name" order by "thoughtful"."monthly_data_yr" asc,"thoughtful"."monthly_data_mo" asc ) as "prev_month_total",
      lead("thoughtful"."monthly_data_monthly_total", 1) over (partition by "thoughtful"."monthly_data_item_category","thoughtful"."monthly_data_item_brand","thoughtful"."monthly_data_call_center_name" order by "thoughtful"."monthly_data_yr" asc,"thoughtful"."monthly_data_mo" asc ) as "next_month_total"
  FROM
      "thoughtful")
  SELECT
      "questionable"."monthly_data_item_category" as "category",
      "questionable"."monthly_data_item_brand" as "brand",
      "questionable"."monthly_data_call_center_name" as "call_center",
      "questionable"."monthly_data_yr" as "year",
      "questionable"."monthly_data_mo" as "month_of_year",
      "abundant"."avg_monthly_sales" as "avg_monthly_sales",
      "questionable"."prev_month_total" as "prev_month_total",
      "questionable"."next_month_total" as "next_month_total"
  FROM
      "questionable"
      INNER JOIN "abundant" on "questionable"."monthly_data_call_center_name" = "abundant"."monthly_data_call_center_name" AND "questionable"."monthly_data_item_brand" = "abundant"."monthly_data_item_brand" AND "questionable"."monthly_data_item_category" = "abundant"."monthly_data_item_category" AND "questionable"."monthly_data_yr" = "abundant"."monthly_data_yr"
  WHERE
      "questionable"."monthly_data_yr" = 1999 and "abundant"."avg_monthly_sales" > 0 and abs(INVALID_REFERENCE_BUG - "abundant"."avg_monthly_sales") / "abundant"."avg_monthly_sales" > 0.1

  ORDER BY
      ( INVALID_REFERENCE_BUG - "abundant"."avg_monthly_sales" ) asc nulls first,
      "category" asc,
      "brand" asc,
      "call_center" asc,
      "year" asc,
      "month_of_year" asc,
      INVALID_REFERENCE_BUG asc,
      "questionable"."prev_month_total" asc nulls first,
      "questionable"."next_month_total" asc nulls first
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query58.preql`

  ```text
  Syntax error in query58.preql: Output column 'store_total' aliases 'local.store_total', which is itself the 'store_total' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'store_total_out').
  ```
- `trilogy run query58.preql`

  ```text
  Syntax error in query58.preql: Undefined concept: item.text_id. Suggestions: ['store_sales.item.text_id', 'catalog_sales.item.text_id', 'web_sales.item.text_id', 'store_sales.date.text_id', 'store_sales.return_date.text_id', 'store_sales.time.text_id']
  ```
- `trilogy run query58.preql`

  ```text
  Resolution error in query58.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {local.catalog_total, local.store_total, local.web_total, store_sales.item.text_id}
  ```
- `trilogy run query58.preql`

  ```text
  Syntax error in query58.preql: 2 undefined concept references; fix all before re-running:
    - combined.item_code (line 46, col 5, in SELECT); did you mean: combined.store_totals.item_code, store_totals.item_code, catalog_totals.item_code, web_totals.item_code, combined.web_tot, combined.store_tot?
    - combined.item_code (line 54, col 10, in ORDER BY); did you mean: combined.store_totals.item_code, store_totals.item_code, catalog_totals.item_code, web_totals.item_code, combined.web_tot, combined.store_tot?
  ```
- `trilogy run query58_test.preql`

  ```text
  Resolution error in query58_test.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {catalog_totals.total}; {store_totals.item_code, store_totals.total}; {web_totals.total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query58_test.preql`

  ```text
  Resolution error in query58_test.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {catalog_totals.total}; {store_totals.item_code, store_totals.total}; {web_totals.total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query59.preql`

  ```text
  Syntax error in query59.preql: Undefined concept: date.day_of_week. Suggestions: ['store_sales.date.day_of_week', 'store_sales.store.date.day_of_week', 'store_sales.return_store.date.day_of_week', 'store_sales.return_date.day_of_week', 'store_sales.customer.first_sales_date.day_of_week', 'store_sales.customer.first_shipto_date.day_of_week']
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {weekly_2001.friday, weekly_2001.monday, weekly_2001.saturday, weekly_2001.store_code, weekly_2001.store_name, weekly_2001.sunday, weekly_2001.this_year_week_seq, weekly_2001.thursday, weekly_2001.tuesday, weekly_2001.wednesday, weekly_2002.friday, weekly_2002.monday, weekly_2002.saturday, weekly_2002.sunday, weekly_2002.thursday, weekly_2002.tuesday, weekly_2002.wednesday}
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {weekly_2001.friday, weekly_2001.monday, weekly_2001.saturday, weekly_2001.store_code, weekly_2001.store_name, weekly_2001.sunday, weekly_2001.this_year_week_seq, weekly_2001.thursday, weekly_2001.tuesday, weekly_2001.wednesday, weekly_2002.friday, weekly_2002.monday, weekly_2002.saturday, weekly_2002.sunday, weekly_2002.thursday, weekly_2002.tuesday, weekly_2002.wednesday}
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {weekly_2001.friday, weekly_2001.monday, weekly_2001.saturday, weekly_2001.store_code, weekly_2001.store_name, weekly_2001.sunday, weekly_2001.this_year_week_seq, weekly_2001.thursday, weekly_2001.tuesday, weekly_2001.wednesday, weekly_2002.friday, weekly_2002.monday, weekly_2002.saturday, weekly_2002.sunday, weekly_2002.thursday, weekly_2002.tuesday, weekly_2002.wednesday}
  ```
- `trilogy run query59.preql`

  ```text
  Syntax error in query59.preql: HAVING references 'store_sales.date.year', which is not in the SELECT projection (line 9). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --store_sales.date.year
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {weekly_2001.friday, weekly_2001.monday, weekly_2001.saturday, weekly_2001.store_code, weekly_2001.store_name, weekly_2001.sunday, weekly_2001.this_yr_ws, weekly_2001.thursday, weekly_2001.tuesday, weekly_2001.wednesday, weekly_2002.friday, weekly_2002.monday, weekly_2002.saturday, weekly_2002.sunday, weekly_2002.thursday, weekly_2002.tuesday, weekly_2002.wednesday}
  ```
- `trilogy file read query60.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query62.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs.item.id, local.cat_ext_list_price_by_item, local.cat_rfnd_by_item}
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'cat_agg.ext_list_price_sum', 'cat_agg.refund_sum', which are not in the SELECT projection (line 18). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --cat_agg.ext_list_price_sum, --cat_agg.refund_sum
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: cat_qualifying_items.item_id (line 25, col 8, in SELECT). Suggestions: ['cat_qualifying_items.cat_agg.item_id', 'cat_agg.item_id', 'cat_qualifying_items.cat_agg.refund_sum']
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {local._cat_agg_ext_list_price_sum, local._cat_agg_item_id}; {local._cat_agg_refund_sum}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'local.cat_ext_list', 'local.cat_refund', which are not in the SELECT projection (line 12). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.cat_ext_list, --local.cat_refund
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cs.item.id, local.cat_ext_list}; {local.cat_refund}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: cr.item. Suggestions: ['cr.sales.item.id', 'cr.sales.item.text_id', 'cr.sales.item.product_name', 'cr.sales.item.brand_id', 'cr.sales.item.brand_name', 'cr.sales.item.manufacturer_id']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: cr.sales.item. Suggestions: ['cr.sales.item.id', 'cr.sales.item.text_id', 'cr.sales.item.product_name', 'cr.sales.item.brand_id', 'cr.sales.item.brand_name', 'cr.sales.item.manufacturer_id']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: agg_sales_2000.item_id. Suggestions: ['cat_qualifying.item_id', 'agg_sales.item_id', '_agg_sales_item_id', 'agg_sales.store_zip']
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ps.store.id', which is not in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ps.store.id order by ps.store.id asc`.
  ```
- `trilogy file read query65.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 32 column 45 (char 2698). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query71.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query74.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `grouping(s.channel)` (line 15), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(s.channel) as g order by g desc`.
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: Undefined concept: date.year. Suggestions: ['ss.date.year', 'ss.store.date.year', 'ss.return_store.date.year', 'ws.date.year', 'cs.date.year', 'ss.return_date.year']
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_agg.cat_qty, catalog_agg.cat_sp, catalog_agg.cat_wc, store_agg.cust_id, store_agg.item_id, store_agg.store_qty, store_agg.store_sp, store_agg.store_wc, store_agg.yr, web_agg.web_qty, web_agg.web_sp, web_agg.web_wc}
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query81.preql`

  ```text
  Syntax error in query81.preql: 14 undefined concept references; fix all before re-running:
    - local.salutation (line 32, col 25, in ORDER BY); did you mean: cr.sales.ship_customer.salutation, cr.sales.bill_customer.salutation, cr.billing_customer.salutation, cr.refunded_customer.salutation?
    - local.first_name (line 32, col 37, in ORDER BY); did you mean: cr.sales.ship_customer.first_name, cr.sales.bill_customer.first_name, cr.billing_customer.first_name, cr.refunded_customer.first_name, last_name, street_name?
    - local.last_name (line 32, col 49, in ORDER BY); did you mean: cr.sales.ship_customer.last_name, cr.sales.bill_customer.last_name, cr.billing_customer.last_name, cr.refunded_customer.last_name, first_name, street_name?
    - local.street_number (line 32, col 60, in ORDER BY); did you mean: cr.sales.ship_customer.address.street_number, cr.sales.bill_customer.address.street_number, cr.sales.customer_address.street_number, cr.sales.bill_address.street_number, cr.billing_customer.address.street_number, cr.refunded_customer.address.street_number?
    - local.street_name (line 32, col 75, in ORDER BY); did you mean: cr.sales.ship_customer.address.street_name, cr.sales.bill_customer.address.street_name, cr.sales.customer_address.street_name, cr.sales.bill_address.street_name, cr.billing_customer.address.street_name, cr.refunded_customer.address.street_name?
    - local.street_type (line 32, col 88, in ORDER BY); did you mean: cr.sales.ship_customer.address.street_type, cr.sales.bill_customer.address.street_type, cr.sales.customer_address.street_type, cr.sales.bill_address.street_type, cr.billing_customer.address.street_type, cr.refunded_customer.address.street_type?
    - local.suite_number (line 32, col 101, in ORDER BY); did you mean: cr.sales.ship_customer.address.suite_number, cr.sales.bill_customer.address.suite_number, cr.sales.customer_address.suite_number, cr.sales.bill_address.suite_number, cr.billing_customer.address.suite_number, cr.refunded_customer.address.suite_number?
    - local.city (line 32, col 115, in ORDER BY); did you mean: cr.sales.ship_customer.address.city, cr.sales.bill_customer.address.city, cr.sales.warehouse.city, cr.sales.customer_address.city, cr.sales.bill_address.city, cr.billing_customer.address.city?
    - local.county (line 32, col 121, in ORDER BY); did you mean: cr.sales.ship_customer.address.county, cr.sales.bill_customer.address.county, cr.sales.call_center.county, cr.sales.warehouse.county, cr.sales.customer_address.county, cr.sales.bill_address.county?
    - local.state (line 32, col 129, in ORDER BY); did you mean: cr.sales.ship_customer.address.state, cr.sales.bill_customer.address.state, cr.sales.warehouse.state, cr.sales.customer_address.state, cr.sales.bill_address.state, cr.billing_customer.address.state?
    - local.zip (line 32, col 136, in ORDER BY); did you mean: cr.sales.ship_customer.address.zip, cr.sales.bill_customer.address.zip, cr.sales.customer_address.zip, cr.sales.bill_address.zip, cr.billing_customer.address.zip, cr.refunded_customer.address.zip?
    - local.country (line 32, col 141, in ORDER BY); did you mean: cr.sales.ship_customer.address.country, cr.sales.bill_customer.address.country, cr.sales.warehouse.country, cr.sales.customer_address.country, cr.sales.bill_address.country, cr.billing_customer.address.country?
    - local.gmt_offset (line 32, col 150, in ORDER BY); did you mean: cr.sales.ship_customer.address.gmt_offset, cr.sales.bill_customer.address.gmt_offset, cr.sales.customer_address.gmt_offset, cr.sales.bill_address.gmt_offset, cr.billing_customer.address.gmt_offset, cr.refunded_customer.address.gmt_offset?
    - local.location_type (line 32, col 162, in ORDER BY); did you mean: cr.sales.ship_customer.address.location_type, cr.sales.bill_customer.address.location_type, cr.sales.customer_address.location_type, cr.sales.bill_address.location_type, cr.billing_customer.address.location_type, cr.refunded_customer.address.location_type?
  ```
- `trilogy run query81.preql`

  ```text
  Syntax error in query81.preql: HAVING references 'local.state_avg', which is not in the SELECT projection (line 12). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.state_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {inventory.date.date, inventory.quantity_on_hand}; {item.current_price, item.manufacturer_id, local.current_price, local.description, local.in_store_sales, local.item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: HAVING references 'local.store_rows', 'local.catalog_rows', 'local.web_rows', which are not in the SELECT projection (line 16). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.store_rows, --local.catalog_rows, --local.web_rows
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: HAVING references 'local.store_rows', 'local.catalog_rows', 'local.web_rows', which are not in the SELECT projection (line 11). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.store_rows, --local.catalog_rows, --local.web_rows
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error in query86.preql: (_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups

  LINE 28:     grouping("thoughtful"."rollup_data_item_category") as "_vir...
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "ws_item_items"."I_CATEGORY" as "ws_item_category",
      "ws_item_items"."I_CLASS" as "ws_item_class",
      "ws_web_sales"."WS_NET_PAID" as "ws_net_paid"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ws_item_items" on "ws_web_sales"."WS_ITEM_SK" = "ws_item_items"."I_ITEM_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2000
  ),
  thoughtful as (
  SELECT
      "cheerful"."ws_item_category" as "rollup_data_item_category",
      "cheerful"."ws_item_class" as "rollup_data_item_class",
      sum("cheerful"."ws_net_paid") as "rollup_data_total_net_paid"
  FROM
      "cheerful"
  GROUP BY
      ROLLUP (1, 2)),
  cooperative as (
  SELECT
      "thoughtful"."rollup_data_item_category" as "rollup_data_item_category",
      "thoughtful"."rollup_data_item_class" as "rollup_data_item_class",
      grouping("thoughtful"."rollup_data_item_category") as "_virt_agg_grouping_9556673619439574",
      grouping("thoughtful"."rollup_data_item_class") as "_virt_agg_grouping_5286880506231037"
  FROM
      "thoughtful"),
  questionable as (
  SELECT
      "thoughtful"."rollup_data_item_category" as "rollup_data_item_category",
      "thoughtful"."rollup_data_item_class" as "rollup_data_item_class",
      "thoughtful"."rollup_data_total_net_paid" as "rollup_data_total_net_paid",
      rank() over (partition by CASE
  	WHEN "cooperative"."_virt_agg_grouping_5286880506231037" = 0 THEN "thoughtful"."rollup_data_item_category"
  	WHEN "cooperative"."_virt_agg_grouping_9556673619439574" = 1 THEN 'grand_total'
  	ELSE 'all_categories'
  	END order by "thoughtful"."rollup_data_total_net_paid" desc ) as "within_parent_rank"
  FROM
      "cooperative"
      INNER JOIN "thoughtful" on "cooperative"."rollup_data_item_category" = "thoughtful"."rollup_data_item_category" AND "cooperative"."rollup_data_item_class" = "thoughtful"."rollup_data_item_class")
  SELECT
      "cooperative"."rollup_data_item_category" as "rollup_data_item_category",
      "cooperative"."rollup_data_item_class" as "rollup_data_item_class",
      "questionable"."rollup_data_total_net_paid" as "rollup_data_total_net_paid",
      "cooperative"."_virt_agg_grouping_9556673619439574" + "cooperative"."_virt_agg_grouping_5286880506231037" as "hierarchy_level",
      "questionable"."within_parent_rank" as "within_parent_rank"
  FROM
      "questionable"
      INNER JOIN "cooperative" on "questionable"."rollup_data_item_category" = "cooperative"."rollup_data_item_category" AND "questionable"."rollup_data_item_class" = "cooperative"."rollup_data_item_class"
  ORDER BY
      "hierarchy_level" desc nulls first,
      "cooperative"."rollup_data_item_category" asc nulls first,
      "questionable"."within_parent_rank" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query94.preql`

  ```text
  Syntax error in query94.preql: HAVING references 'ws.warehouse.id', 'ws.is_returned', which are not in the SELECT projection (line 10). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ws.warehouse.id, --ws.is_returned
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query94.preql`

  ```text
  Syntax error in query94.preql: Undefined concept: qualifying_orders.order_number (line 21, col 26, in WHERE). Suggestions: ['qualifying_orders.ws.order_number', 'ws.order_number', 'qualifying_orders.return_cnt', 'qualifying_orders.warehouse_cnt']
  ```
- `trilogy run query95.preql`

  ```text
  Syntax error in query95.preql: Undefined concept: qualified_orders.order_number (line 23, col 7, in WHERE). Suggestions: ['qualified_orders.ws.order_number', 'ws.order_number', 'qualified_orders.warehouse_count', 'qualified_orders.has_return']
  ```
- `trilogy run query97.preql`

  ```text
  Resolution error in query97.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs.bill_customer.id, cs.item.id, local.in_catalog, local.in_store, ss.customer.id, ss.item.id}
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query98.preql`

  ```text
  Syntax error in query98.preql: 2 undefined concept references; fix all before re-running:
    - item.category (line 3, col 7, in WHERE); did you mean: store_sales.item.category, category, store_sales.item.category_id?
    - date.date (line 4, col 7, in WHERE); did you mean: store_sales.date.date, store_sales.store.date.date, store_sales.return_store.date.date, store_sales.return_date.date, store_sales.customer.first_sales_date.date, store_sales.customer.first_shipto_date.date?
  ```
- `trilogy file read query98.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

# Stack catalog and web sales with …Only week sequences that occur in 2001
    combined.wk in (select raw.date.week_seq where raw.date.year = 2001)
order by combined.wk asc nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...ur in 2001     combined.wk in ??? (select raw.date.week_seq wher...

  Write stats: received 2158 chars / 2158 bytes; tail: …' raw.date.year = 2001)\\norder by combined.wk asc nulls first;'.
  ```
- `trilogy run --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales select catalog_sales.sold_date.year, sum(catalog_sales.ext_sales_price) as cat_sales, sum(web_sales.ext_sales_price) as web_sales group by 1 order by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ext_sales_price) as web_sales ??? group by 1 order by 1;
  ```
- `trilogy file write query04.preql --content import raw/all_sales as s;

auto line_value <- s.ext_list_price - s.ext_wholesale_cost - s.ext_discount_amount + s…1.cat_val_2001) > (wv02.web_val_2002 / wv01.web_val_2001)
order by customer_id asc, first_name asc, last_name asc, preferred_cust_flag asc
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as s;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as s;  auto line...

  Write stats: received 1952 chars / 1952 bytes; tail: …'_name asc, last_name asc, preferred_cust_flag asc\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Channel name mapping
auto channel_name <- case(
    sales.channel = 'STORE', 'st…te between '2000-08-23'::date and '2000-09-06'::date and sales.return_channel_dim_id is not null)
order by channel_type, entity_identifier
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:2
    |
  8 | );
    |  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or fcase_simple_when
  Location:
  ...annel = 'WEB', 'web channel' ) ??? ;  # Entity identifier: prefix...

  Write stats: received 1982 chars / 1982 bytes; tail: …'ot null)\\norder by channel_type, entity_identifier\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Sales aggregation: by channel and channel_dim_id
with sales_agg as
where sales.d…  end as entity_id_sales
from
    full join sales_agg.channel = returns_agg.chan
    and sales_agg.channel_dim_id = returns_agg.ret_dim_id
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...d)     end as entity_id_sales ??? from     full join sales_agg.c...

  Write stats: received 1527 chars / 1527 bytes; tail: …'sales_agg.channel_dim_id = returns_agg.ret_dim_id\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Filtered values for sales
auto sale_amt <- sales.ext_sales_price ? sales.date.da…s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and sales.return_channel_dim_id is not null)
order by ch_type, ent_id
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:1
     |
  27 | by rollup ch_type, ent_id
     | ^---
     |
     = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...m(ret_loss), 0) as net_profit ??? by rollup ch_type, ent_id wher...

  Write stats: received 1681 chars / 1681 bytes; tail: …'nnel_dim_id is not null)\\norder by ch_type, ent_id\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Filtered values for sales
auto sale_amt <- sales.ext_sales_price ? sales.date.da…s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and sales.return_channel_dim_id is not null)
order by ch_type, ent_id
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:43
     |
  21 | def r(metric) -> coalesce(sum(metric), 0) by rollup ch_type, ent_id;
     |                                           ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...) -> coalesce(sum(metric), 0) ??? by rollup ch_type, ent_id;  se...

  Write stats: received 1669 chars / 1669 bytes; tail: …'nnel_dim_id is not null)\\norder by ch_type, ent_id\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Filtered values
auto sale_amt <- sales.ext_sales_price ? sales.date.date between…te between '2000-08-23'::date and '2000-09-06'::date and sales.return_channel_dim_id is not null)
order by channel_type, entity_identifier
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:1
     |
  23 | by rollup (case
     | ^---
     |
     = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...m(ret_loss), 0) as net_profit ??? by rollup (case         when s...

  Write stats: received 2123 chars / 2123 bytes; tail: …'ot null)\\norder by channel_type, entity_identifier\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Filtered values
auto sale_amt <- sales.ext_sales_price ? sales.date.date between…ot null)
    or
    (sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date and sales.return_channel_dim_id is not null)
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:43
    |
  9 | def r(metric) -> coalesce(sum(metric), 0) by rollup (case
    |                                           ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...) -> coalesce(sum(metric), 0) ??? by rollup (case     when sales...

  Write stats: received 1310 chars / 1310 bytes; tail: …'date and sales.return_channel_dim_id is not null)\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Filtered values
auto sale_amt <- sales.ext_sales_price ? sales.date.date between…ot null)
    or
    (sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date and sales.return_channel_dim_id is not null)
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:40
    |
  9 | def r(metric) -> sum(metric) by rollup (case
    |                                        ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, OVER_COMPONENT_REF, or IDENTIFIER
  Location:
  ...ric) -> sum(metric) by rollup ??? (case     when sales.channel =...

  Write stats: received 1349 chars / 1349 bytes; tail: …'date and sales.return_channel_dim_id is not null)\\nlimit 100;'.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as sales;
import raw.customer as cust;

parameter zips string;

# Turn the comma-separat…1, 2) in qualifying_prefix_set.prefix
select
    sales.store.name,
    sum(sales.net_profit) as total_net_profit
order by sales.store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset param_zips as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...set of individual zip strings ??? rowset param_zips as select

  Write stats: received 1095 chars / 1095 bytes; tail: …'it) as total_net_profit\\norder by sales.store.name\\nlimit 100;'.
  ```
- `trilogy file write query10.preql --content import raw.physical_sales as store_sales;

# Customers whose current home address is in the specified counties
# a…dependent_count,
  store_sales.customer.demographics.employed_dependent_count,
  store_sales.customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:20
     |
  11 |   and month_of_year(store_sales.date.date) in (1, 2, 3, 4)
     |                    ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...te) = 2002   and month_of_year ??? (store_sales.date.date) in (1,...

  Write stats: received 2389 chars / 2389 bytes; tail: …'les.customer.demographics.college_dependent_count\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Step 1: Define the set of (brand_id, class_id, category_id) that appear
# in…ales.channel nulls first, all_sales.item.brand_id nulls first, all_sales.item.class_id nulls first, all_sales.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:75
    |
  8 | auto multi_channel_bcc <- all_sales.item.brand_id ? channels_per_combo = 3, all_sales.item.class_id ? channels_per_combo = 3, all_sales.item.category_id ? channels_per_combo = 3;
    |                                                                           ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...nd_id ? channels_per_combo = 3 ??? , all_sales.item.class_id ? ch...

  Write stats: received 1299 chars / 1299 bytes; tail: …'lls first, all_sales.item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Stage 1: Find (brand_id, class_id, category_id) combos that appear
# in all …nt
having total_sales > overall_avg_sale
order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset multi_channel_items as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...TALOG, WEB) during 1999-2001. ??? rowset multi_channel_items as

  Write stats: received 1515 chars / 1515 bytes; tail: …'st, class_id nulls first, category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Stage 1: Find (brand_id, class_id, category_id) combos that appear
# in all …nt
having total_sales > overall_avg_sale
order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:20
     |
  20 |   and month_of_year(all_sales.date.date) = 11
     |                    ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...te) = 2001   and month_of_year ??? (all_sales.date.date) = 11   a...

  Write stats: received 1515 chars / 1515 bytes; tail: …'st, class_id nulls first, category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Stage 1: Find (brand_id, class_id, category_id) combos that appear
# in all …nt
having total_sales > overall_avg_sale
order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:11
     |
  30 |     count(*) as sale_count
     |           ^---
     |
     = expected access_chain
  Location:
  ...ce) as total_sales,     count( ??? *) as sale_count having total_...

  Write stats: received 1509 chars / 1509 bytes; tail: …'st, class_id nulls first, category_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/catalog_store_returns:csr select csr.store_sale_date.quarter_name, csr.store_sale_date.year, count(csr.ticket_number) as cnt group by 1,2 order by 1,2 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...unt(csr.ticket_number) as cnt ??? group by 1,2 order by 1,2 limi...
  ```
- `trilogy file write query23.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.physical_sales as ps;
import raw.item as it…,
    sum(combined.line_total) as total_sales
order by
    last_name nulls first,
    first_name nulls first,
    total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...st_totals.total_spent > 0.5 * ??? (select max_total from max_cus...

  Write stats: received 2965 chars / 2965 bytes; tail: …'st_name nulls first,\\n    total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.physical_sales as ps;
import raw.item as it…,
    sum(combined.line_total) as total_sales
order by
    last_name nulls first,
    first_name nulls first,
    total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 80:25
     |
  80 | with combined as union((cat_sales), (web_sales_arm)) -> (last_name, first_name, line_total, cust_id);
     |                         ^---
     |
     = expected select_statement
  Location:
  ...tomer with combined as union(( ??? cat_sales), (web_sales_arm)) -...

  Write stats: received 2788 chars / 2788 bytes; tail: …'st_name nulls first,\\n    total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.physical_sales:ps --import raw.item:item select substring(ps.item.desc, 1, 30) as desc_prefix, count(ps.line_item) as pair_count where ps.date.year between 2000 and 2003 group by desc_prefix having pair_count > 4 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...te.year between 2000 and 2003 ??? group by desc_prefix having pa...
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.bill_customer.id as cid, count(cs.line_item) as cnt where cs.sold_date.year = 2000 and cs.sold_date.month_of_year = 2 group by cid order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...s.sold_date.month_of_year = 2 ??? group by cid order by cnt desc...
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.physical_sales:ps select cs.bill_customer.id as cid, count(cs.line_item) as cnt where cs.sold_date.yea…tomer.id from ps where ps.date.year between 2000 and 2003 and ps.customer.id is not null having sum(ps.quantity * ps.sales_price) > 118133) limit 10;`

  ```text
  Syntax error in stdin: Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
   2 and cs.bill_customer.id in ??? (select ps.customer.id from ps...
  ```
- `trilogy file write query24.preql --content import raw.physical_sales as sales;
import raw.physical_returns as returns;

# Stage 1: For matched sales-returns …,
    stage2.store_name
having stage2.reagg_sum > 0.05 * avg_all_subtotals
order by stage2.cust_last_name, stage2.cust_first_name, stage2.store_name;`

  ```text
  refused to write 'query24.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset stage1 as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...et_paid per detailed grouping ??? rowset stage1 as where sales.s...

  Write stats: received 1561 chars / 1561 bytes; tail: …'2.cust_last_name, stage2.cust_first_name, stage2.store_name;'.
  ```
- `trilogy file write query25.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as cat_sales;

# Aggregate catalog sales to (cu…join store_agg.item_id = catalog_agg.item_id
order by store_agg.item_code, store_agg.item_desc, store_agg.store_code, store_agg.store_name
limit 100;`

  ```text
  refused to write 'query25.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset catalog_agg as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   item) grain for Apr-Oct 2001 ??? rowset catalog_agg as where ca...

  Write stats: received 1501 chars / 1501 bytes; tail: …'_desc, store_agg.store_code, store_agg.store_name\\nlimit 100;'.
  ```
- `trilogy file write query29.preql --content import raw.catalog_sales as cs;

where cs.sold_date.year = 1999
select
    cs.bill_customer.id as cust_id,
    cs.item.id as item_id,
    sum(cs.quantity) as cat_qty
group by cust_id, item_id
limit 100;`

  ```text
  refused to write 'query29.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
    sum(cs.quantity) as cat_qty ??? group by cust_id, item_id limi...

  Write stats: received 202 chars / 202 bytes; tail: …'cs.quantity) as cat_qty\\ngroup by cust_id, item_id\\nlimit 100;'.
  ```
- `trilogy file write query36.preql --content import raw.physical_sales as sales;
import raw.item as item;
import raw.date as date;
import raw.store as store;

… hierarchy_level,
    rnk
order by hierarchy_level desc nulls first,
         item.category asc nulls first,
         rnk asc nulls first
limit 100;
`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...sales_price) at rollup levels ??? auto total_net_profit <- sum(s...

  Write stats: received 1415 chars / 1415 bytes; tail: …'ry asc nulls first,\\n         rnk asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query51.preql --content import raw.all_sales as all_sales;

# Approach: compute per-item-date running totals of unit price
# for web and s…store.channel,
    daily_web_store.daily_price
order by daily_web_store.item_id asc nulls first, daily_web_store.sale_date asc nulls first
limit 20;
`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...s.sales_price) as daily_price ??? group by all_sales.item.text_i...

  Write stats: received 799 chars / 799 bytes; tail: …' first, daily_web_store.sale_date asc nulls first\\nlimit 20;\\n'.
  ```
- `trilogy file write query53.preql --content # Step 1: Define the per-(manufacturer, quarter) totals as a rowset
with manufacturer_quarter_totals as
import raw…_avg.avg_quarterly_sales asc,
  manufacturer_quarter_totals.per_quarter_total asc,
  manufacturer_quarter_totals.item.manufacturer_id asc
limit 100;
`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:1
    |
  3 | import raw.physical_sales as physical_sales;
    | ^---
    |
    = expected select_statement or tvf_union_invocation
  Location:
  ...anufacturer_quarter_totals as ??? import raw.physical_sales as p...

  Write stats: received 2097 chars / 2097 bytes; tail: …'facturer_quarter_totals.item.manufacturer_id asc\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.date:date select date.year, date.month_of_year, date.month_seq where date.year in (1999) and date.month_of_year in (1,2,3) group by date.year, date.month_of_year, date.month_seq order by date.month_seq limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...date.month_of_year in (1,2,3) ??? group by date.year, date.month...
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
import raw.physical_sales as store_s…stomer.id in all_buyers.customer_id
order by segment asc, customer_count asc, segment_times_50 asc
  nulls first, nulls first, nulls first
limit 100;`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 39:22
     |
  39 |   nulls first, nulls first, nulls first
     |                      ^---
     |
     = expected limit, ORDER_DIRECTION_LA, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...s_50 asc   nulls first, nulls ??? first, nulls first limit 100;

  Write stats: received 1576 chars / 1576 bytes; tail: …'es_50 asc\\n  nulls first, nulls first, nulls first\\nlimit 100;'.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
import raw.physical_sales as store_s…_id) as customer_count,
  segment * 50 as segment_times_50
group by segment
order by segment asc, customer_count asc, segment_times_50 asc
limit 100;`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ment * 50 as segment_times_50 ??? group by segment order by segm...

  Write stats: received 1634 chars / 1634 bytes; tail: …'ent asc, customer_count asc, segment_times_50 asc\\nlimit 100;'.
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;

# Monthly per-unit price totals per (category, brand, call_center)
with monthly_d…asc,
    month_of_year asc,
    monthly_data.monthly_total asc,
    prev_month_total asc nulls first,
    next_month_total asc nulls first
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:37
     |
  25 |     --monthly_data.monthly_total as --monthly_total_hidden,
     |                                     ^---
     |
     = expected IDENTIFIER
  Location:
  ...monthly_data.monthly_total as ??? --monthly_total_hidden,     la...

  Write stats: received 1862 chars / 1862 bytes; tail: …'nulls first,\\n    next_month_total asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query59.preql --content import raw.physical_sales as store_sales;

# Sum of unit price per day-of-week, by store and calendar week
def wk_…   weekly_2001.store_name asc nulls first,
    weekly_2001.store_code asc nulls first,
    weekly_2001.this_year_week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 58:40
     |
  58 |   and weekly_2001.this_year_week_seq = (weekly_2002.next_year_week_seq - 52)
     |                                        ^---
     |
     = expected IDENTIFIER
  Location:
  ...kly_2001.this_year_week_seq = ??? (weekly_2002.next_year_week_se...

  Write stats: received 2754 chars / 2754 bytes; tail: …'  weekly_2001.this_year_week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query59.preql --content import raw.physical_sales as store_sales;

# Sum of unit price per day-of-week, by store and calendar week
def wk_…   weekly_2001.store_name asc nulls first,
    weekly_2001.store_code asc nulls first,
    weekly_2001.this_year_week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 58:75
     |
  58 | left join weekly_2001.this_year_week_seq = weekly_2002.next_year_week_seq - 52
     |                                                                           ^---
     |
     = expected limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...eekly_2002.next_year_week_seq ??? - 52 order by     weekly_2001....

  Write stats: received 2804 chars / 2804 bytes; tail: …'  weekly_2001.this_year_week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.store.name as name, store_sales.date.week_seq as ws, count(1) as cnt where store_sales.date.year = 2001 group by name, ws order by name, ws limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   store_sales.date.year = 2001 ??? group by name, ws order by nam...
  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.date.year as yr, min(store_sales.date.week_seq) as min_ws, max(store_sales.date.week_seq) as max_ws group by yr order by yr limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ales.date.week_seq) as max_ws ??? group by yr order by yr limit
  ```
- `trilogy file write query64.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.physical_returns as sr;
import raw.ite…m
;

# Items meeting catalog condition
auto cat_qualifying <- cs.item.id ? ext_list_price_sum > 2 * refund_sum by cat_agg;
select cs.item.id
limit 5;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:73
     |
  17 | auto cat_qualifying <- cs.item.id ? ext_list_price_sum > 2 * refund_sum by cat_agg;
     |                                                                         ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...st_price_sum > 2 * refund_sum ??? by cat_agg; select cs.item.id

  Write stats: received 687 chars / 687 bytes; tail: …'_sum > 2 * refund_sum by cat_agg;\\nselect cs.item.id\\nlimit 5;'.
  ```
- `trilogy file write query64.preql`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:1
     |
  30 | ;
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ....demographics.marital_status
   ??? ;

   # Year 1999 aggregated
   r...

  Write stats: received 3879 chars / 3879 bytes; tail: …'tal_wholesale_cost, y2000.total_wholesale_cost\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query69.preql --content import raw/physical_sales as store_sales;
import raw/catalog_sales as cat_sales;
import raw/web_sales as web;

# C…er.demographics.education_status,
  store_sales.customer.demographics.purchase_estimate,
  store_sales.customer.demographics.credit_rating
limit 100;`

  ```text
  refused to write 'query69.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/physical_sales as store_sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/physical_sales as store_sa...

  Write stats: received 1350 chars / 1350 bytes; tail: …'  store_sales.customer.demographics.credit_rating\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as store_sales;

# Step 1: Filter to year 2000 for store sales
auto profit_2000 <- store…e_county,
    rolled_profit as total_net_profit,
    hlevel,
    rnk
order by hlevel desc, store_sales.store.state asc nulls last, rnk asc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:84
     |
  30 | auto parent_key <- case when hlevel = 0 then store_sales.store.state else cast(null, 'string') end;
     |                                                                                    ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...les.store.state else cast(null ??? , 'string') end; auto rnk <- r...

  Write stats: received 1881 chars / 1881 bytes; tail: …', store_sales.store.state asc nulls last, rnk asc\\nlimit 100;'.
  ```
- `trilogy file write query78.preql --content import raw.physical_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Store channel: nev…her_sp
inner join store_sales.yr = web_sales_nr.yr
  and store_sales.item_id = web_sales_nr.item_id
  and store_sales.cust_id = web_sales_nr.cust_id
`

  ```text
  refused to write 'query78.preql': not syntactically valid Trilogy.

  Parse error:
    --> 53:80
     |
  53 |     round(store_sales.store_qty::numeric / store_sales.other_qty::numeric, 2)) as ratio,
     |                                                                                ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or fcase_simple_when
  Location:
  ...sales.other_qty::numeric, 2)) ??? as ratio,   store_sales.store_...

  Write stats: received 1835 chars / 1835 bytes; tail: …'nr.item_id\\n  and store_sales.cust_id = web_sales_nr.cust_id\\n'.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, any_value(all_sales.channel_dim_text_id) as example, count(all_sales.channel_dim_text_id) as cnt group by all_sales.channel limit 10;`

  ```text
  Syntax error in stdin:  --> 2:36
    |
  2 | select all_sales.channel, any_value(all_sales.channel_dim_text_id) as example, count(all_sales.channel_dim_text_id) as cnt group by all_sales.channel limit 10;
    |                                    ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...t all_sales.channel, any_value ??? (all_sales.channel_dim_text_id...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, concat('store', all_sales.channel_dim_text_id) as outlet, sum(all_sales.ext_sales_pric…d all_sales.item.current_price > 50 and all_sales.promotion.channel_tv = 'N' and all_sales.outlet_id is not null group by all_sales.channel limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...l_sales.outlet_id is not null ??? group by all_sales.channel lim...
  ```
- `trilogy file write query83.preql --escapes -c import raw.all_sales as s;

where s.is_returned = true
  and s.return_date.week_seq in (5244, 5257, 5264)

auto…three_channel_avg
having store_rows > 0
   and catalog_rows > 0
   and web_rows > 0
order by s.item.text_id asc, store_qty asc nulls first
limit 100;`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...ek_seq in (5244, 5257, 5264)  ??? auto store_qty <- sum(s.return...

  Write stats: received 1424 chars / 1424 bytes; tail: …' by s.item.text_id asc, store_qty asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

# Web sales in year 2000, total net paid by item category and class with rollup
def t… ) as within_parent_rank
order by
    hierarchy_level desc nulls first,
    item_category nulls first,
    within_parent_rank nulls first
limit 100;
`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | def total_net_paid_by_cat_class -> sum(ws.net_paid) by rollup ws.item.category, ws.item.class;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ategory and class with rollup ??? def total_net_paid_by_cat_clas...

  Write stats: received 925 chars / 925 bytes; tail: …' nulls first,\\n    within_parent_rank nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;

# Store sales: unique (customer, item) combos wh…g > 0 then 1 else 0 end) as in_both
full join store_pairs.cust_id = catalog_pairs.cust_id and store_pairs.item_id = catalog_pairs.item_id
limit 100;
`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...00 and customer is identified ??? rowset store_pairs as where ye...

  Write stats: received 1409 chars / 1409 bytes; tail: …' and store_pairs.item_id = catalog_pairs.item_id\\nlimit 100;\\n'.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;

# Store sales: unique (customer, item) combos wh… null then 1 else 0 end) as in_both
full join store_pairs.cust_id = catalog_pairs.cust_id and store_pairs.item_id = catalog_pairs.item_id
limit 100;
`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...00 and customer is identified ??? rowset store_pairs as where ye...

  Write stats: received 1034 chars / 1034 bytes; tail: …' and store_pairs.item_id = catalog_pairs.item_id\\nlimit 100;\\n'.
  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run --format json query47.preql`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run query53.preql --format rich`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run -i raw.item:item select distinct item.brand_name limit 20;`

  ```text
  'raw.item:item' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.item:item duck_db select distinct item.brand_name limit 20;`

  ```text
  'select distinct item.brand_name limit 20;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/sales/preql/auto/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales/preql/auto/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/return_reason.preql`

  ```text
  Invalid value for 'PATH': File 'raw/return_reason.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Filtered values for sales
auto sale_amt <- sales.ext_sales_price ? sales.date.da…s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and sales.return_channel_dim_id is not null)
order by ch_type, ent_id
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `coalesce(sum(sale_amt), 0) by rollup ch_type as coalesce_sum_sale_amt_0_by_rollup_ch_typ`
  Location:
     coalesce(sum(sale_amt), 0) ??? by rollup ch_type, ent_id as t...

  Write stats: received 1707 chars / 1707 bytes; tail: …'nnel_dim_id is not null)\\norder by ch_type, ent_id\\nlimit 100;'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Define the eight attribute profiles as a derived concept
auto profile_items <- item.te…turer_id between 1 and 500
  and item.manufact in target_manuf
select
  distinct item.product_name as product_name
order by
  product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct item.product_name as distinct_item_product_name`
  Location:
  ...arget_manuf select   distinct ??? item.product_name as product_n...

  Write stats: received 1577 chars / 1577 bytes; tail: …'duct_name as product_name\\norder by\\n  product_name\\nlimit 100;'.
  ```
- `trilogy run --import raw/physical_sales:sales select sales.store.id, count(sales.ticket_number) limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(sales.ticket_number) as ticket_number_count`
  Location:
  ...d, count(sales.ticket_number) ??? limit 5;
  ```
- `trilogy file write query64.preql -e -c import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Merge item conce…t_qualifying.item_id
  cat_qualifying.item_id as item_id
having cat_qualifying.item_id in store_return_items
;

select target_items.item_id
limit 20;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `cat_qualifying.item_id
    cat_qualifying.item_id as cat_qualifying_item_id_cat_qualifying_it`
  Location:
     --cat_qualifying.item_id   ??? cat_qualifying.item_id as item...

  Write stats: received 984 chars / 984 bytes; tail: …' store_return_items\\n;\\n\\nselect target_items.item_id\\nlimit 20;'.
  ```
- `trilogy file write query64.preql -e -c import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Merge item conce…t_qualifying.item_id
  cat_qualifying.item_id as item_id
having cat_qualifying.item_id in store_return_items
;

select target_items.item_id
limit 20;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `cat_qualifying.item_id
    cat_qualifying.item_id as cat_qualifying_item_id_cat_qualifying_it`
  Location:
     --cat_qualifying.item_id   ??? cat_qualifying.item_id as item...

  Write stats: received 984 chars / 984 bytes; tail: …' store_return_items\\n;\\n\\nselect target_items.item_id\\nlimit 20;'.
  ```
- `trilogy file write query66.preql --content import raw.all_sales as s;

# Compute the per-month aggregates for qualifying sales
with monthly_data as
where yea…oalesce(monthly_data.monthly_net, 0) as monthly_net
order by monthly_data.warehouse_name asc nulls first, monthly_data.yr asc nulls first
limit 100;
`

  ```text
  refused to write 'query66.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct
    monthly_data.warehouse_name as distinct_monthly_data_warehouse_name`
  Location:
  ...se_spine as select distinct   ??? monthly_data.warehouse_name,

  Write stats: received 2019 chars / 2019 bytes; tail: …'asc nulls first, monthly_data.yr asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;

# Compute per-line net quantity and net amount, deduplicating at the line leve…    compare.qty - compare.prev_qty as qty_diff,
    compare.amt - compare.prev_amt as amt_diff
order by
    qty_diff asc,
    amt_diff asc
limit 100;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct
      all_sales.date.year as distinct_all_sales_date_year`
  Location:
  ...01, 2002) select distinct     ??? all_sales.date.year as year,

  Write stats: received 2167 chars / 2167 bytes; tail: …'_diff\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

# Channel labels
auto store_label <- 'store channel';
auto catalog_label <- 'catalog c…l) + grouping(s.outlet_id) as grouping_level
order by
  grouping_level asc,
  channel nulls first,
  outlet_id nulls first,
  returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sales_amt by rollup s.channel as sales_amt_by_rollup_s_channel`
  Location:
  ..._id as outlet_id,   sales_amt ??? by rollup s.channel, s.outlet_...

  Write stats: received 1735 chars / 1735 bytes; tail: …'ls first,\\n  outlet_id nulls first,\\n  returns desc\\nlimit 100;'.
  ```
- `trilogy file write query77.preql -e -c import raw.all_sales as s;

# Channel labels
auto store_label <- 'store channel';
auto catalog_label <- 'catalog chann…et_id as profit
order by
  grouping(s.channel) + grouping(s.outlet_id) asc,
  channel nulls first,
  outlet_id nulls first,
  returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `coalesce(sum(s.return_amount ? s.return_date.date between '2000-08-23'::date and '2000-09-22'::date), 0) by rollup s.channel as coalesce_sum_s_return_amount_s_return_da`
  Location:
  ...e and '2000-09-22'::date), 0) ??? by rollup s.channel, s.outlet_...

  Write stats: received 1254 chars / 1254 bytes; tail: …'ls first,\\n  outlet_id nulls first,\\n  returns desc\\nlimit 100;'.
  ```
- `trilogy file write query79.preql --content import raw.physical_sales as sales;

where
  sales.date.day_name = 'Monday'
  and sales.date.year in (1999, 2000, …stomer.first_name asc nulls first,
  store_city asc nulls first,
  total_net_profit asc nulls first,
  sales.ticket_number asc nulls first
limit 100;`

  ```text
  refused to write 'query79.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sale_address fields for grouping (not displayed in final output but needed for correct grain)
    --sales.sale_address.id as sale_address_fields_for_grouping_not_dis`
  Location:
  ...ket_number,   -- sale_address ??? fields for grouping (not displ...

  Write stats: received 906 chars / 906 bytes; tail: …'ulls first,\\n  sales.ticket_number asc nulls first\\nlimit 100;'.
  ```

### `undefined-concept`

- `trilogy explore query06.preql`

  ```text
  Failed to parse query06.preql: (UndefinedConceptException(...), "Undefined concept: item_cat_prices.category. Suggestions: ['item_cat_prices.sales.item.category', 'sales.item.category', 'item_cat_prices.sales.item.current_price']")
  ```
- `trilogy explore query12.preql`

  ```text
  Failed to parse query12.preql: (UndefinedConceptException(...), '5 undefined concept references; fix all before re-running:\n  - item_totals.category (line 20, col 5, in SELECT); did you mean: item_totals.ws.item.category, ws.item.category, item_totals.class, item_totals.item_code?\n  - item_totals.class (line 21, col 5, in SELECT); did you mean: item_totals.ws.item.class, ws.item.class, item_totals.category, item_totals.item_code?\n  - item_totals.current_price (line 22, col 5, in SELECT); did you mean: item_totals.ws.item.current_price, ws.item.current_price, item_totals.category?\n  - item_totals.category (line 26, col 5, in ORDER BY); did you mean: item_totals.ws.item.category, ws.item.category, item_totals.class, item_totals.item_code?\n  - item_totals.class (line 27, col 5, in ORDER BY); did you mean: item_totals.ws.item.class, ws.item.class, item_totals.category, item_totals.item_code?')
  ```
- `trilogy explore query24.preql`

  ```text
  Failed to parse query24.preql: (UndefinedConceptException(...), '6 undefined concept references; fix all before re-running:\n  - stage2.cust_last_name (line 40, col 5, in SELECT); did you mean: stage2.stage1.cust_last_name, stage1.cust_last_name, stage2.cust_first_name, _stage1_cust_last_name?\n  - stage2.cust_first_name (line 41, col 5, in SELECT); did you mean: stage2.stage1.cust_first_name, stage1.cust_first_name, _stage1_cust_first_name, stage2.cust_last_name?\n  - stage2.store_name (line 42, col 5, in SELECT); did you mean: stage2.stage1.store_name, stage1.store_name, _stage1_store_name?\n  - stage2.cust_last_name (line 44, col 10, in ORDER BY); did you mean: stage2.stage1.cust_last_name, stage1.cust_last_name, stage2.cust_first_name, _stage1_cust_last_name?\n  - stage2.cust_first_name (line 44, col 33, in ORDER BY); did you mean: stage2.stage1.cust_first_name, stage1.cust_first_name, _stage1_cust_first_name, stage2.cust_last_name?\n  - stage2.store_name (line 44, col 57, in ORDER BY); did you mean: stage2.stage1.store_name, stage1.store_name, _stage1_store_name?')
  ```
- `trilogy explore query35.preql`

  ```text
  Failed to parse query35.preql: (UndefinedConceptException(...), '3 undefined concept references; fix all before re-running:\n  - store_customers.id (line 31, col 34, in WHERE); did you mean: store_customers.store_sales.customer.id, store_sales.item.id, store_sales.date.id, store_sales.return_date.id, store_sales.time.id, store_sales.return_time.id?\n  - web_customers.id (line 32, col 35, in WHERE); did you mean: web_customers.web_sales.billing_customer.id, store_sales.item.id, store_sales.date.id, store_sales.return_date.id, store_sales.time.id, store_sales.return_time.id?\n  - catalog_customers.id (line 32, col 82, in WHERE); did you mean: catalog_customers.catalog_sales.ship_customer.id, store_sales.item.id, store_sales.date.id, store_sales.return_date.id, store_sales.time.id, store_sales.return_time.id?')
  ```
- `trilogy explore query39.preql`

  ```text
  Failed to parse query39.preql: (UndefinedConceptException(...), "Undefined concept: qualifying.mo. Suggestions: ['qualifying.monthly_stats.mo', 'monthly_stats.mo', 'qualifying.cv', '_qualifying_cv', 'qualifying.monthly_stats.m']")
  ```
- `trilogy explore query53.preql`

  ```text
  Failed to parse query53.preql: (UndefinedConceptException(...), "Undefined concept: manufacturer_quarter_totals.manufacturer_id (line 32, col 3, in SELECT). Suggestions: ['manufacturer_quarter_totals.item.manufacturer_id', 'physical_sales.item.manufacturer_id', 'item.manufacturer_id', 'manufacturer_quarter_totals.date.quarter', 'manufacturer_quarter_totals.per_quarter_total']")
  ```
- `trilogy explore query95.preql`

  ```text
  Failed to parse query95.preql: (UndefinedConceptException(...), "Undefined concept: qualified_orders.order_number (line 23, col 7, in WHERE). Suggestions: ['qualified_orders.ws.order_number', 'ws.order_number', 'qualified_orders.warehouse_count', 'qualified_orders.has_return']")
  ```

### `file-not-found`

- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Join key `jan.qualifying.monthly_stats.wh_id` does not exist
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Join key `jan.monthly_stats.wh_id` does not exist
  ```
- `trilogy run query59.preql`

  ```text
  Unexpected error in query59.preql: Join key `weekly_2001.name` does not exist
  ```
- `trilogy explore query59.preql`

  ```text
  Failed to parse query59.preql: Join key `weekly_2001.name` does not exist
  ```

### `join-resolution`

- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Could not resolve connections for query with output ['local.state<Purpose.PROPERTY>Derivation.BASIC>', 'local.line_item_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
