# Trilogy failure analysis — 20260703-151512

- Run `20260703-151512` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 283 | failed: 42 (15%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 30 | 71% |
| `syntax-parse` | 10 | 24% |
| `cli-misuse` | 1 | 2% |
| `join-resolution` | 1 | 2% |

## Detail

### `other`

- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: (_duckdb.BinderException) Binder Error: column "returns_agg_s_return_channel_dim_id" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(returns_agg_s_return_channel_dim_id)" if the exact value of "returns_agg_s_return_channel_dim_id" is not important.

  LINE 256:     "rambunctious"."returns_agg_s_return_channel_dim_id" as...
                ^
  [SQL:
  WITH
  busy as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "s_channel_dim_id",
      "s_catalog_sales_unified"."CS_SOLD_DATE_SK" as "s_date_id",
      "s_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "s_ext_sales_price",
      "s_catalog_sales_unified"."CS_NET_PROFIT" as "s_net_profit"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
  WHERE
      "s_catalog_sales_unified"."CS_CATALOG_PAGE_SK" is not null

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_STORE_SK" as "s_channel_dim_id",
      "s_store_sales_unified"."SS_SOLD_DATE_SK" as "s_date_id",
      "s_store_sales_unified"."SS_EXT_SALES_PRICE" as "s_ext_sales_price",
      "s_store_sales_unified"."SS_NET_PROFIT" as "s_net_profit"
  FROM
      "store_sales" as "s_store_sales_unified"
  WHERE
      "s_store_sales_unified"."SS_STORE_SK" is not null

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_WEB_SITE_SK" as "s_channel_dim_id",
      "s_web_sales_unified"."WS_SOLD_DATE_SK" as "s_date_id",
      "s_web_sales_unified"."WS_EXT_SALES_PRICE" as "s_ext_sales_price",
      "s_web_sales_unified"."WS_NET_PROFIT" as "s_net_profit"
  FROM
      "web_sales" as "s_web_sales_unified"
  WHERE
      "s_web_sales_unified"."WS_WEB_SITE_SK" is not null
  ),
  scrawny as (
  SELECT
      "s_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "s_channel_dim_id",
      "s_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "s_channel_dim_text_id"
  FROM
      "catalog_page" as "s_catalog_dim_unified"
  WHERE
      "s_catalog_dim_unified"."CP_CATALOG_PAGE_SK" is not null

  UNION ALL
  SELECT
      "s_store_dim_unified"."S_STORE_SK" as "s_channel_dim_id",
      "s_store_dim_unified"."S_STORE_ID" as "s_channel_dim_text_id"
  FROM
      "store" as "s_store_dim_unified"
  WHERE
      "s_store_dim_unified"."S_STORE_SK" is not null

  UNION ALL
  SELECT
      "s_web_dim_unified"."web_site_sk" as "s_channel_dim_id",
      "s_web_dim_unified"."web_site_id" as "s_channel_dim_text_id"
  FROM
      "web_site" as "s_web_dim_unified"
  WHERE
      "s_web_dim_unified"."web_site_sk" is not null
  ),
  uneven as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_returns_unified"."CR_ITEM_SK" as "s_item_id",
      "s_catalog_returns_unified"."CR_ORDER_NUMBER" as "s_order_id",
      "s_catalog_returns_unified"."CR_RETURN_AMOUNT" as "s_return_amount",
      "s_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "s_return_date_id",
      "s_catalog_returns_unified"."CR_NET_LOSS" as "s_return_net_loss"
  FROM
      "catalog_returns" as "s_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_returns_unified"."SR_ITEM_SK" as "s_item_id",
      "s_store_returns_unified"."SR_TICKET_NUMBER" as "s_order_id",
      "s_store_returns_unified"."SR_RETURN_AMT" as "s_return_amount",
      "s_store_returns_unified"."SR_RETURNED_DATE_SK" as "s_return_date_id",
      "s_store_returns_unified"."SR_NET_LOSS" as "s_return_net_loss"
  FROM
      "store_returns" as "s_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_returns_unified"."WR_ITEM_SK" as "s_item_id",
      "s_web_returns_unified"."WR_ORDER_NUMBER" as "s_order_id",
      "s_web_returns_unified"."WR_RETURN_AMT" as "s_return_amount",
      "s_web_returns_unified"."WR_RETURNED_DATE_SK" as "s_return_date_id",
      "s_web_returns_unified"."WR_NET_LOSS" as "s_return_net_loss"
  FROM
      "web_returns" as "s_web_returns_unified"),
  juicy as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_returns_unified"."CR_ITEM_SK" as "s_item_id",
      "s_catalog_returns_unified"."CR_ORDER_NUMBER" as "s_order_id",
      "s_catalog_returns_unified"."CR_CATALOG_PAGE_SK" as "s_return_channel_dim_id"
  FROM
      "catalog_returns" as "s_catalog_returns_unified"
  WHERE
      "s_catalog_returns_unified"."CR_CATALOG_PAGE_SK" is not null

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_returns_unified"."SR_ITEM_SK" as "s_item_id",
      "s_store_returns_unified"."SR_TICKET_NUMBER" as "s_order_id",
      "s_store_returns_unified"."SR_STORE_SK" as "s_return_channel_dim_id"
  FROM
      "store_returns" as "s_store_returns_unified"
  WHERE
      "s_store_returns_unified"."SR_STORE_SK" is not null

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_id",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id",
      "s_web_sales_unified"."WS_WEB_SITE_SK" as "s_return_channel_dim_id"
  FROM
      "web_sales" as "s_web_sales_unified"
  WHERE
      "s_web_sales_unified"."WS_WEB_SITE_SK" is not null
  ),
  cheerful as (
  SELECT
      "s_catalog_dim_return_unified"."CP_CATALOG_PAGE_SK" as "s_return_channel_dim_id",
      "s_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" as "s_return_channel_dim_text_id"
  FROM
      "catalog_page" as "s_catalog_dim_return_unified"
  WHERE
      "s_catalog_dim_return_unified"."CP_CATALOG_PAGE_SK" is not null

  UNION ALL
  SELECT
      "s_store_dim_return_unified"."S_STORE_SK" as "s_return_channel_dim_id",
      "s_store_dim_return_unified"."S_STORE_ID" as "s_return_channel_dim_text_id"
  FROM
      "store" as "s_store_dim_return_unified"
  WHERE
      "s_store_dim_return_unified"."S_STORE_SK" is not null

  UNION ALL
  SELECT
      "s_web_dim_return_unified"."web_site_sk" as "s_return_channel_dim_id",
      "s_web_dim_return_unified"."web_site_id" as "s_return_channel_dim_text_id"
  FROM
      "web_site" as "s_web_dim_return_unified"
  WHERE
      "s_web_dim_return_unified"."web_site_sk" is not null
  ),
  protective as (
  SELECT
      "busy"."s_channel" as "s_channel",
      "busy"."s_channel_dim_id" as "s_channel_dim_id",
      sum(coalesce("busy"."s_ext_sales_price",0)) as "_sales_agg_ext_sales",
      sum(coalesce("busy"."s_net_profit",0)) as "_sales_agg_net_profit"
  FROM
      "busy"
      INNER JOIN "date_dim" as "s_date_date" on "busy"."s_date_id" = "s_date_date"."D_DATE_SK"
  WHERE
      cast("s_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

  GROUP BY
      1,
      2),
  friendly as (
  SELECT
      "scrawny"."s_channel_dim_id" as "s_channel_dim_id",
      "scrawny"."s_channel_dim_text_id" as "s_channel_dim_text_id"
  FROM
      "scrawny"
  GROUP BY
      1,
      2),
  concerned as (
  SELECT
      "juicy"."s_return_channel_dim_id" as "s_return_channel_dim_id",
      coalesce("juicy"."s_channel","uneven"."s_channel") as "s_channel",
      sum(coalesce("uneven"."s_return_amount",0)) as "_returns_agg_ret_amount",
      sum(coalesce("uneven"."s_return_net_loss",0)) as "_returns_agg_ret_net_loss"
  FROM
      "juicy"
      INNER JOIN "uneven" on "juicy"."s_channel" = "uneven"."s_channel" AND "juicy"."s_item_id" = "uneven"."s_item_id" AND "juicy"."s_order_id" = "uneven"."s_order_id"
      INNER JOIN "date_dim" as "s_return_date_date" on "uneven"."s_return_date_id" = "s_return_date_date"."D_DATE_SK"
  WHERE
      cast("s_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "juicy"."s_return_channel_dim_id" is not null

  GROUP BY
      1,
      2),
  thoughtful as (
  SELECT
      "cheerful"."s_return_channel_dim_id" as "s_return_channel_dim_id"
  FROM
      "cheerful"
  GROUP BY
      1,
      "cheerful"."s_return_channel_dim_text_id"),
  puzzled as (
  SELECT
      "friendly"."s_channel_dim_id" as "s_channel_dim_id",
      "friendly"."s_channel_dim_text_id" as "s_channel_dim_text_id",
      "protective"."_sales_agg_ext_sales" as "_sales_agg_ext_sales",
      "protective"."_sales_agg_net_profit" as "_sales_agg_net_profit",
      "protective"."s_channel" as "s_channel"
  FROM
      "protective"
      INNER JOIN "friendly" on "protective"."s_channel_dim_id" = "friendly"."s_channel_dim_id"),
  sparkling as (
  SELECT
      "concerned"."_returns_agg_ret_amount" as "_returns_agg_ret_amount",
      "concerned"."_returns_agg_ret_net_loss" as "_returns_agg_ret_net_loss",
      "concerned"."s_channel" as "s_channel",
      "thoughtful"."s_return_channel_dim_id" as "s_return_channel_dim_id"
  FROM
      "concerned"
      INNER JOIN "thoughtful" on "concerned"."s_return_channel_dim_id" = "thoughtful"."s_return_channel_dim_id"),
  waggish as (
  SELECT
      "puzzled"."_sales_agg_ext_sales" as "sales_agg_ext_sales",
      "puzzled"."_sales_agg_net_profit" as "sales_agg_net_profit",
      "puzzled"."s_channel" as "sales_agg_s_channel",
      "puzzled"."s_channel_dim_id" as "sales_agg_s_channel_dim_id",
      "puzzled"."s_channel_dim_text_id" as "sales_agg_s_channel_dim_text_id"
  FROM
      "puzzled"),
  abhorrent as (
  SELECT
      "sparkling"."_returns_agg_ret_amount" as "returns_agg_ret_amount",
      "sparkling"."_returns_agg_ret_net_loss" as "returns_agg_ret_net_loss",
      "sparkling"."s_channel" as "returns_agg_s_channel",
      "sparkling"."s_return_channel_dim_id" as "returns_agg_s_return_channel_dim_id"
  FROM
      "sparkling"),
  rambunctious as (
  SELECT
      "abhorrent"."returns_agg_ret_amount" as "returns_agg_ret_amount",
      "abhorrent"."returns_agg_ret_net_loss" as "returns_agg_ret_net_loss",
      "waggish"."sales_agg_ext_sales" as "sales_agg_ext_sales",
      "waggish"."sales_agg_net_profit" as "sales_agg_net_profit",
      "waggish"."sales_agg_s_channel_dim_text_id" as "sales_agg_s_channel_dim_text_id",
      coalesce("abhorrent"."returns_agg_s_channel","waggish"."sales_agg_s_channel") as "returns_agg_s_channel",
      coalesce("abhorrent"."returns_agg_s_channel","waggish"."sales_agg_s_channel") as "sales_agg_s_channel",
      coalesce("abhorrent"."returns_agg_s_return_channel_dim_id","waggish"."sales_agg_s_channel_dim_id") as "returns_agg_s_return_channel_dim_id",
      coalesce("abhorrent"."returns_agg_s_return_channel_dim_id","waggish"."sales_agg_s_channel_dim_id") as "sales_agg_s_channel_dim_id"
  FROM
      "abhorrent"
      FULL JOIN "waggish" on "abhorrent"."returns_agg_s_channel" = "waggish"."sales_agg_s_channel" AND "abhorrent"."returns_agg_s_return_channel_dim_id" = "waggish"."sales_agg_s_channel_dim_id"),
  puffy as (
  SELECT
      "rambunctious"."returns_agg_s_return_channel_dim_id" as "returns_agg_s_return_channel_dim_id",
      "rambunctious"."sales_agg_s_channel" as "sales_agg_s_channel",
      grouping("rambunctious"."sales_agg_s_channel") as "_virt_agg_grouping_2335873445943412",
      grouping("rambunctious"."sales_agg_s_channel_dim_id") as "_virt_agg_grouping_2679570584196302",
      sum("rambunctious"."returns_agg_ret_amount") as "_virt_agg_sum_5684104003332447",
      sum("rambunctious"."returns_agg_ret_net_loss") as "_virt_agg_sum_1006578281048606",
      sum("rambunctious"."sales_agg_ext_sales") as "_virt_agg_sum_4786848671953554",
      sum("rambunctious"."sales_agg_net_profit") as "_virt_agg_sum_1043362569701221"
  FROM
      "rambunctious"
  GROUP BY
      ROLLUP (2, "rambunctious"."sales_agg_s_channel_dim_id"))
  SELECT
      CASE
  	WHEN "puffy"."_virt_agg_grouping_2335873445943412" = 1 THEN null
  	ELSE CASE
  	coalesce("puffy"."sales_agg_s_channel","rambunctious"."sales_agg_s_channel")
  	WHEN 'STORE' THEN 'store channel'
  	WHEN 'CATALOG' THEN 'catalog channel'
  	WHEN 'WEB' THEN 'web channel'
  	END
  	END as "channel_type",
      CASE
  	WHEN "puffy"."_virt_agg_grouping_2335873445943412" = 1 or "puffy"."_virt_agg_grouping_2679570584196302" = 1 THEN null
  	ELSE CASE
  	coalesce("puffy"."sales_agg_s_channel","rambunctious"."sales_agg_s_channel")
  	WHEN 'STORE' THEN ('store' || "rambunctious"."sales_agg_s_channel_dim_text_id")
  	WHEN 'CATALOG' THEN ('catalog_page' || "rambunctious"."sales_agg_s_channel_dim_text_id")
  	WHEN 'WEB' THEN ('web_site' || "rambunctious"."sales_agg_s_channel_dim_text_id")
  	END
  	END as "entity_id",
      coalesce("puffy"."_virt_agg_sum_4786848671953554",0) as "total_ext_sales",
      coalesce("puffy"."_virt_agg_sum_5684104003332447",0) as "total_returns",
      coalesce("puffy"."_virt_agg_sum_1043362569701221",0) - coalesce("puffy"."_virt_agg_sum_1006578281048606",0) as "net_profit"
  FROM
      "puffy"
      FULL JOIN "rambunctious" on "puffy"."returns_agg_s_return_channel_dim_id" is not distinct from "rambunctious"."returns_agg_s_return_channel_dim_id" AND "puffy"."sales_agg_s_channel" is not distinct from "rambunctious"."sales_agg_s_channel"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      "channel_type" asc nulls first,
      "entity_id" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query05.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: 2 undefined concept references; fix all before re-running:
    - customer_totals.cust_id (line 29, col 31, in SELECT); did you mean: customer_totals.cust_total, cs.customer_address.text_id, cs.customer_address.id?
    - customer_totals.cust_total (line 30, col 11, in WHERE); did you mean: customer_totals.cust_id, max_cust_total?
  ```
- `trilogy run test_count_years.preql`

  ```text
  Unexpected error in test_count_years.preql: _pair_side_fully_matches() takes 7 positional arguments but 8 were given
  ```
- `trilogy run test_count_years.preql`

  ```text
  Unexpected error in test_count_years.preql: _pair_side_fully_matches() takes 7 positional arguments but 8 were given
  ```
- `trilogy run test_count_years.preql`

  ```text
  Unexpected error in test_count_years.preql: 'UpgradeOuterFromKeySetEquivalence' object has no attribute 'subset_binding_sources'
  ```
- `trilogy run test_direct_join.preql`

  ```text
  Unexpected error in test_direct_join.preql: 'UpgradeOuterFromKeySetEquivalence' object has no attribute 'subset_binding_sources'
  ```
- `trilogy run test_direct_join2.preql`

  ```text
  Unexpected error in test_direct_join2.preql: 'UpgradeOuterFromKeySetEquivalence' object has no attribute 'subset_binding_sources'
  ```
- `trilogy run test_check_dup2.preql`

  ```text
  Syntax error in test_check_dup2.preql: Undefined concept: local.code (line 31, col 8, in SELECT). Suggestions: ['t1.code', 't2.code', '_t2_code']
  ```
- `trilogy run test_check_dup2.preql`

  ```text
  Syntax error in test_check_dup2.preql: Ambiguous reference 'joined.code': matches ['joined.t1.code', 'joined.t2.code']. Qualify the full path to disambiguate.
  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 28). The requested concepts split into 2 disconnected subgraphs: {cs.item.id, cat_ext_list_by_item}; {cat_refund_by_item}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Ambiguous reference 'ss_with_sr.year': matches ['ss_with_sr.ss.customer.first_sales_date.year', 'ss_with_sr.ss.customer.first_shipto_date.year', 'ss_with_sr.ss.date.year']. Qualify the full path to disambiguate.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: 14 undefined concept references; fix all before re-running:
    - ss_agg.ss_item_product_name (line 40, col 5, in SELECT); did you mean: ss_agg.ss.item.product_name, ss.item.product_name, sr.item.product_name?
    - ss_agg.ss_store_name (line 41, col 5, in SELECT); did you mean: ss_agg.ss.store.name, ss_agg.ss_store_zip, ss.store.name?
    - ss_agg.ss_store_zip (line 42, col 5, in SELECT); did you mean: ss_agg.ss.store.zip, ss_agg.ss_store_name, ss_agg.ss_customer_address_zip?
    - ss_agg.ss_sale_address_street_number (line 43, col 5, in SELECT); did you mean: ss_agg.ss.sale_address.street_number, ss_agg.ss_sale_address_street_name, ss_agg.ss_customer_address_street_number?
    - ss_agg.ss_sale_address_street_name (line 44, col 5, in SELECT); did you mean: ss_agg.ss_sale_address_street_number, ss_agg.ss.sale_address.street_name, ss_agg.ss_customer_address_street_name?
    - ss_agg.ss_sale_address_city (line 45, col 5, in SELECT); did you mean: ss_agg.ss.sale_address.city, ss_agg.ss_sale_address_zip, ss_agg.ss_customer_address_city?
    - ss_agg.ss_sale_address_zip (line 46, col 5, in SELECT); did you mean: ss_agg.ss.sale_address.zip, ss_agg.ss_sale_address_city, ss_agg.ss_customer_address_zip?
    - ss_agg.ss_customer_address_street_number (line 47, col 5, in SELECT); did you mean: ss_agg.ss_customer_address_street_name, ss_agg.ss.customer.address.street_number, ss_agg.ss_sale_address_street_number?
    - ss_agg.ss_customer_address_street_name (line 48, col 5, in SELECT); did you mean: ss_agg.ss_customer_address_street_number, ss_agg.ss.customer.address.street_name, ss_agg.ss_sale_address_street_name?
    - ss_agg.ss_customer_address_city (line 49, col 5, in SELECT); did you mean: ss_agg.ss_customer_address_zip, ss_agg.ss.customer.address.city, ss_agg.ss_sale_address_city?
    - ss_agg.ss_customer_address_zip (line 50, col 5, in SELECT); did you mean: ss_agg.ss_customer_address_city, ss_agg.ss.customer.address.zip, ss_agg.ss_sale_address_zip?
    - ss_agg.ss_date_year (line 51, col 5, in SELECT); did you mean: ss_agg.ss.date.year, ss_agg.ss_store_name, ss.date.year?
    - ss_agg.ss_customer_first_sales_date_year (line 52, col 5, in SELECT); did you mean: ss_agg.ss.customer.first_sales_date.year, ss_agg.ss_customer_first_shipto_date_year, ss.customer.first_sales_date.year?
    - ss_agg.ss_customer_first_shipto_date_year (line 53, col 5, in SELECT); did you mean: ss_agg.ss.customer.first_shipto_date.year, ss_agg.ss_customer_first_sales_date_year, ss.customer.first_shipto_date.year?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {yr1999.ss_with_sr.coupon_amt_sum, yr1999.ss_with_sr.list_price_sum, yr1999.ss_with_sr.sale_lines, yr1999.ss_with_sr.ss.customer.address.city, yr1999.ss_with_sr.ss.customer.address.street_name, yr1999.ss_with_sr.ss.customer.address.street_number, yr1999.ss_with_sr.ss.customer.address.zip, yr1999.ss_with_sr.ss.item.id, yr1999.ss_with_sr.ss.item.product_name, yr1999.ss_with_sr.ss.sale_address.city, yr1999.ss_with_sr.ss.sale_address.street_name, yr1999.ss_with_sr.ss.sale_address.street_number, yr1999.ss_with_sr.ss.sale_address.zip, yr1999.ss_with_sr.ss.store.name, yr1999.ss_with_sr.ss.store.zip, yr1999.ss_with_sr.wholesale_cost_sum, yr2000.ss_with_sr.coupon_amt_sum, yr2000.ss_with_sr.list_price_sum, yr2000.ss_with_sr.sale_lines, yr2000.ss_with_sr.wholesale_cost_sum}
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 69). The requested concepts split into 2 disconnected subgraphs: {yr1999.coupon_amt_sum, yr1999.list_price_sum, yr1999.sale_lines, yr1999.ss.customer.address.city, yr1999.ss.customer.address.street_name, yr1999.ss.customer.address.street_number, yr1999.ss.customer.address.zip, yr1999.ss.item.id, yr1999.ss.item.product_name, yr1999.ss.sale_address.city, yr1999.ss.sale_address.street_name, yr1999.ss.sale_address.street_number, yr1999.ss.sale_address.zip, yr1999.ss.store.name, yr1999.ss.store.zip, yr1999.wholesale_cost_sum}; {yr2000.coupon_amt_sum, yr2000.list_price_sum, yr2000.sale_lines, yr2000.wholesale_cost_sum}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {agg00.base_agg.coupon_amt_sum, agg00.base_agg.list_price_sum, agg00.base_agg.sale_lines, agg00.base_agg.wholesale_cost_sum, agg99.base_agg.coupon_amt_sum, agg99.base_agg.list_price_sum, agg99.base_agg.sale_lines, agg99.base_agg.ss.customer.address.city, agg99.base_agg.ss.customer.address.street_name, agg99.base_agg.ss.customer.address.street_number, agg99.base_agg.ss.customer.address.zip, agg99.base_agg.ss.item.id, agg99.base_agg.ss.item.product_name, agg99.base_agg.ss.sale_address.city, agg99.base_agg.ss.sale_address.street_name, agg99.base_agg.ss.sale_address.street_number, agg99.base_agg.ss.sale_address.zip, agg99.base_agg.ss.store.name, agg99.base_agg.ss.store.zip, agg99.base_agg.wholesale_cost_sum}
  ```
- `trilogy run query64.preql`

  ```text
  {
    "event": "statement_result",
    "index": 0,
    "total": 1,
    "duration_ms": 21.658,
    "success": false,
    "error": "Could not render the query: Missing source reference to ss.item.id. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).\n\nFull SQL with sentinel(s):\n\nWITH \nwakeful as (\nSELECT\n    \"cr_catalog_returns\".\"CR_ITEM_SK\" as \"qualifying_item_ids\"\nFROM\n    \"catalog_sales\" as \"cr_sales_catalog_sales\"\n    INNER
  …
    \"waggish\".\"paired_base_agg_ss_item_product_name\" asc,\n    \"waggish\".\"paired_base_agg_ss_store_name\" asc,\n    \"waggish\".\"paired_next_sale_lines\" asc,\n    \"waggish\".\"paired_base_agg_wholesale_cost_sum\" asc,\n    \"waggish\".\"paired_next_wholesale_cost_sum\" asc",
    "error_type": "ValueError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 22.012,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query67.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `grouping(local._channel)` (line 10), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local._channel) as g order by g desc`.
  ```
- `trilogy file read query77.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query87.preql`

  ```text
  Resolution error in query87.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 18). The requested concepts split into 3 disconnected subgraphs: {in_catalog}; {in_store}; {in_web}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query87.preql`

  ```text
  Unexpected error in query87.preql: Invalid input concepts to node! ['store_combos.store.customer.last_name'] are missing non-hidden parent nodes; have {'catalog_combos.catalog.bill_customer.first_name', 'web_combos.web.billing_customer.last_name', 'catalog_combos.catalog.date.date', 'web_combos.web.billing_customer.first_name', 'web_combos.web.date.date', 'catalog_combos.catalog.bill_customer.last_name'} and hidden set() from root {'catalog_combos.catalog.date.date', 'catalog_combos.catalog.bill_customer.last_name', 'catalog_combos.catalog.bill_customer.first_name'}
  ```
- `trilogy file read query87.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query59.preql --content import raw.store_sales as ss;

auto sun <- sum(ss.sales_price ? ss.date.day_of_week = 0) by ss.store.id, ss.date.w…ar.week_seq = next_year.week_seq - 52
order by
    store_name nulls first,
    store_code nulls first,
    this_year_week_seq nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 56:5
     |
  56 |     on this_year.text_id = next_year.text_id
     |     ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...oin this_year = next_year     ??? on this_year.text_id = next_ye...

  Write stats: received 2640 chars / 2640 bytes; tail: …' nulls first,\\n    this_year_week_seq nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.id, ss.store.name, ss.store.text_id, count(1) as cnt by ss.store.id;`

  ```text
  Syntax error in stdin:  --> 2:70
    |
  2 | select ss.store.id, ss.store.name, ss.store.text_id, count(1) as cnt by ss.store.id;
    |                                                                      ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...tore.text_id, count(1) as cnt ??? by ss.store.id;
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.text_id, ss.store.name, sum(ss.sales_price) as total_units by ss.store.text_id limit 10;`

  ```text
  Syntax error in stdin:  --> 2:76
    |
  2 | select ss.store.text_id, ss.store.name, sum(ss.sales_price) as total_units by ss.store.text_id limit 10;
    |                                                                            ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...s.sales_price) as total_units ??? by ss.store.text_id limit 10;
  ```
- `trilogy file write test_grain.preql --content import raw.store_sales as ss;

with weekly_totals as
select
    ss.store.name as store_name,
    ss.store.text_…   weekly_totals.store_code,
    weekly_totals.week,
    count(1) as cnt
by weekly_totals.store_code, weekly_totals.week
order by cnt desc
limit 20;
`

  ```text
  refused to write 'test_grain.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:1
     |
  16 | by weekly_totals.store_code, weekly_totals.week
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...als.week,     count(1) as cnt ??? by weekly_totals.store_code, w...

  Write stats: received 384 chars / 384 bytes; tail: …'.store_code, weekly_totals.week\\norder by cnt desc\\nlimit 20;\\n'.
  ```
- `trilogy file write test_year_filter.preql --content import raw.store_sales as ss;

auto total_price_by_text_id <- sum(ss.sales_price) by ss.store.text_id, ss…001
select
    ss.store.text_id as code,
    ss.date.week_seq as week,
    total_price_by_text_id as total
;

select count(1) as cnt from this_year;
`

  ```text
  refused to write 'test_year_filter.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...tal ;  select count(1) as cnt ??? from this_year;

  Write stats: received 325 chars / 325 bytes; tail: …'_text_id as total\\n;\\n\\nselect count(1) as cnt from this_year;\\n'.
  ```
- `trilogy file write test_dup_check.preql --content import raw.store_sales as ss;

auto total_price_by_text_id <- sum(ss.sales_price) by ss.store.text_id, ss.d…by code, week
;

select combo_counts.code, combo_counts.week, combo_counts.rcnt
where combo_counts.rcnt > 1
order by combo_counts.rcnt desc
limit 5;
`

  ```text
  refused to write 'test_dup_check.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:1
     |
  19 | by code, week
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
     week,     count(1) as rcnt ??? by code, week ;  select combo_...

  Write stats: received 563 chars / 563 bytes; tail: …'bo_counts.rcnt > 1\\norder by combo_counts.rcnt desc\\nlimit 5;\\n'.
  ```
- `trilogy file write test_count_years.preql --content import raw.store_sales as ss;

auto total_price_by_text_id <- sum(ss.sales_price) by ss.store.text_id, ss…week_seq as week,
    total_price_by_text_id as total
;

select count(1) as this_count from this_year;
select count(1) as next_count from next_year;
`

  ```text
  refused to write 'test_count_years.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...select count(1) as this_count ??? from this_year; select count(1...

  Write stats: received 528 chars / 528 bytes; tail: …'om this_year;\\nselect count(1) as next_count from next_year;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r…lines,
    sum(ss.ext_wholesale_cost) as wholesale_cost_sum,
    sum(ss.ext_list_price) as list_price_sum,
    sum(ss.coupon_amt) as coupon_amt_sum
;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:134
     |
  14 | auto cum_refund_amt <- coalesce(sum(cr.refunded_cash), 0) + coalesce(sum(cr.reversed_charge), 0) + coalesce(sum(cr.store_credit), 0) as total_refund;
     |                                                                                                                                      ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...esce(sum(cr.store_credit), 0) ??? as total_refund;  # Items wher...

  Write stats: received 1934 chars / 1934 bytes; tail: …'s list_price_sum,\\n    sum(ss.coupon_amt) as coupon_amt_sum\\n;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_returns as cr;
import raw.store_returns as sr;

# --- Catalog qua…9.base_agg.ss.item.product_name,
    agg99.base_agg.ss.store.name,
    agg00.sale_lines,
    agg99.wholesale_cost_sum,
    agg00.wholesale_cost_sum
;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
     --> 102:9
      |
  102 |     1999,
      |         ^---
      |
      = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...customer.address.zip,     1999 ??? ,     agg99.sale_lines,     ag...

  Write stats: received 3941 chars / 3941 bytes; tail: …'    agg99.wholesale_cost_sum,\\n    agg00.wholesale_cost_sum\\n;'.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as ss;

auto null_category <- ss.item.category is null;
auto cat_group <- case when null_ca…month_of_year asc nulls first,
         ss.store.text_id asc nulls first,
         sum_sales asc nulls first,
         rnk asc nulls first
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 35:1
     |
  35 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...th_of_year, ss.store.text_id) ??? having     rnk <= 100 order by...

  Hint: HAVING must come *before* the `by rollup/cube/grouping sets` clause in Trilogy (the reverse of SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> having <cond> by rollup (<keys>) order by <cols> limit <n>;

  Write stats: received 1717 chars / 1717 bytes; tail: …'les asc nulls first,\\n         rnk asc nulls first\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.items_with_store_return<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
