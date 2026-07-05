# Trilogy failure analysis — 20260705-142435

- Run `20260705-142435` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1200 | failed: 99 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 54 | 55% |
| `syntax-parse` | 37 | 37% |
| `syntax-missing-alias` | 5 | 5% |
| `cli-misuse` | 3 | 3% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel)` (line 26), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel) as g order by g desc`.
  ```
- `trilogy run query10.preql`

  ```text
  Syntax error in query10.preql: 8 undefined concept references; fix all before re-running:
    - gender (line 39, col 3, in ORDER BY); did you mean: store.customer.demographics.gender, store.return_customer.demographics.gender, store.customer_demographic.gender, web.billing_customer.demographics.gender, web.ship_customer.demographics.gender, web.return_customer.demographics.gender?
    - marital_status (line 40, col 3, in ORDER BY); did you mean: store.customer.demographics.marital_status, store.return_customer.demographics.marital_status, store.customer_demographic.marital_status, web.billing_customer.demographics.marital_status, web.ship_customer.demographics.marital_status, web.return_customer.demographics.marital_status?
    - education_status (line 41, col 3, in ORDER BY); did you mean: store.customer.demographics.education_status, store.return_customer.demographics.education_status, store.customer_demographic.education_status, web.billing_customer.demographics.education_status, web.ship_customer.demographics.education_status, web.return_customer.demographics.education_status?
    - purchase_estimate (line 42, col 3, in ORDER BY); did you mean: store.customer.demographics.purchase_estimate, store.return_customer.demographics.purchase_estimate, store.customer_demographic.purchase_estimate, web.billing_customer.demographics.purchase_estimate, web.ship_customer.demographics.purchase_estimate, web.return_customer.demographics.purchase_estimate?
    - credit_rating (line 43, col 3, in ORDER BY); did you mean: store.customer.demographics.credit_rating, store.return_customer.demographics.credit_rating, store.customer_demographic.credit_rating, web.billing_customer.demographics.credit_rating, web.ship_customer.demographics.credit_rating, web.return_customer.demographics.credit_rating?
    - dependent_count (line 44, col 3, in ORDER BY); did you mean: store.customer.demographics.dependent_count, store.customer.household_demographic.dependent_count, store.return_customer.demographics.dependent_count, store.return_customer.household_demographic.dependent_count, store.customer_demographic.dependent_count, store.household_demographic.dependent_count?
    - employed_dependent_count (line 45, col 3, in ORDER BY); did you mean: store.customer.demographics.employed_dependent_count, store.return_customer.demographics.employed_dependent_count, store.customer_demographic.employed_dependent_count, web.billing_customer.demographics.employed_dependent_count, web.ship_customer.demographics.employed_dependent_count, web.return_customer.demographics.employed_dependent_count?
    - college_dependent_count (line 46, col 3, in ORDER BY); did you mean: store.customer.demographics.college_dependent_count, store.return_customer.demographics.college_dependent_count, store.customer_demographic.college_dependent_count, web.billing_customer.demographics.college_dependent_count, web.ship_customer.demographics.college_dependent_count, web.return_customer.demographics.college_dependent_count?
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 32). The requested concepts split into 2 disconnected subgraphs: {_level, brand_id, category_id, class_id, total_count, total_sales, nov_2001_bcc.channel}; {overall_avg}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: (_duckdb.BinderException) Binder Error: column "all_channel_bcc_category_id" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(all_channel_bcc_category_id)" if the exact value of "all_channel_bcc_category_id" is not important.

  LINE 154:     "late"."all_channel_bcc_category_id" as "all_channel_bcc_ca...
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_SOLD_DATE_SK" as "s_date_id",
      "s_catalog_sales_unified"."CS_ITEM_SK" as "s_item_id",
      "s_catalog_sales_unified"."CS_LIST_PRICE" as "s_list_price",
      "s_catalog_sales_unified"."CS_QUANTITY" as "s_quantity",
       1  as "s_sale_line_item_counter"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_SOLD_DATE_SK" as "s_date_id",
      "s_store_sales_unified"."SS_ITEM_SK" as "s_item_id",
      "s_store_sales_unified"."SS_LIST_PRICE" as "s_list_price",
      "s_store_sales_unified"."SS_QUANTITY" as "s_quantity",
       1  as "s_sale_line_item_counter"
  FROM
      "store_sales" as "s_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_SOLD_DATE_SK" as "s_date_id",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_id",
      "s_web_sales_unified"."WS_LIST_PRICE" as "s_list_price",
      "s_web_sales_unified"."WS_QUANTITY" as "s_quantity",
       1  as "s_sale_line_item_counter"
  FROM
      "web_sales" as "s_web_sales_unified"),
  cooperative as (
  SELECT
      avg("cheerful"."s_quantity" * "cheerful"."s_list_price") as "_overall_stats_overall_avg"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_id" = "s_date_date"."D_DATE_SK"
  WHERE
      "s_date_date"."D_YEAR" BETWEEN 1999 AND 2001
  ),
  yummy as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_id" = "s_date_date"."D_DATE_SK"
      INNER JOIN "item" as "s_item_items" on "cheerful"."s_item_id" = "s_item_items"."I_ITEM_SK"
  WHERE
      "s_date_date"."D_YEAR" BETWEEN 1999 AND 2001

  GROUP BY
      1,
      2,
      3,
      4),
  sparkling as (
  SELECT
      "cheerful"."s_channel" as "_nov_2001_bcc_channel",
      "s_item_items"."I_BRAND_ID" as "_nov_2001_bcc_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "_nov_2001_bcc_category_id",
      "s_item_items"."I_CLASS_ID" as "_nov_2001_bcc_class_id",
      count("cheerful"."s_sale_line_item_counter") as "_nov_2001_bcc_sale_count",
      sum("cheerful"."s_quantity" * "cheerful"."s_list_price") as "_nov_2001_bcc_total_sales"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_id" = "s_date_date"."D_DATE_SK"
      INNER JOIN "item" as "s_item_items" on "cheerful"."s_item_id" = "s_item_items"."I_ITEM_SK"
  WHERE
      "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 11

  GROUP BY
      1,
      2,
      3,
      4),
  abundant as (
  SELECT
      "cooperative"."_overall_stats_overall_avg" as "overall_stats_overall_avg"
  FROM
      "cooperative"),
  juicy as (
  SELECT
      "yummy"."s_item_brand_id" as "_all_channel_bcc_brand_id",
      "yummy"."s_item_category_id" as "_all_channel_bcc_category_id",
      "yummy"."s_item_class_id" as "_all_channel_bcc_class_id"
  FROM
      "yummy"
  GROUP BY
      1,
      2,
      3
  HAVING
      count(distinct "yummy"."s_channel") = 3
  ),
  sweltering as (
  SELECT
      "sparkling"."_nov_2001_bcc_brand_id" as "nov_2001_bcc_brand_id",
      "sparkling"."_nov_2001_bcc_category_id" as "nov_2001_bcc_category_id",
      "sparkling"."_nov_2001_bcc_channel" as "nov_2001_bcc_channel",
      "sparkling"."_nov_2001_bcc_class_id" as "nov_2001_bcc_class_id",
      "sparkling"."_nov_2001_bcc_sale_count" as "nov_2001_bcc_sale_count",
      "sparkling"."_nov_2001_bcc_total_sales" as "nov_2001_bcc_total_sales"
  FROM
      "sparkling"),
  vacuous as (
  SELECT
      "juicy"."_all_channel_bcc_brand_id" as "_all_channel_bcc_brand_id",
      "juicy"."_all_channel_bcc_category_id" as "_all_channel_bcc_category_id",
      "juicy"."_all_channel_bcc_class_id" as "_all_channel_bcc_class_id"
  FROM
      "juicy"),
  concerned as (
  SELECT
      "vacuous"."_all_channel_bcc_brand_id" as "all_channel_bcc_brand_id",
      "vacuous"."_all_channel_bcc_category_id" as "all_channel_bcc_category_id",
      "vacuous"."_all_channel_bcc_class_id" as "all_channel_bcc_class_id"
  FROM
      "vacuous"),
  late as (
  SELECT
      "sweltering"."nov_2001_bcc_channel" as "nov_2001_bcc_channel",
      "sweltering"."nov_2001_bcc_sale_count" as "nov_2001_bcc_sale_count",
      "sweltering"."nov_2001_bcc_total_sales" as "nov_2001_bcc_total_sales",
      coalesce("concerned"."all_channel_bcc_brand_id","sweltering"."nov_2001_bcc_brand_id") as "all_channel_bcc_brand_id",
      coalesce("concerned"."all_channel_bcc_brand_id","sweltering"."nov_2001_bcc_brand_id") as "nov_2001_bcc_brand_id",
      coalesce("concerned"."all_channel_bcc_category_id","sweltering"."nov_2001_bcc_category_id") as "all_channel_bcc_category_id",
      coalesce("concerned"."all_channel_bcc_category_id","sweltering"."nov_2001_bcc_category_id") as "nov_2001_bcc_category_id",
      coalesce("concerned"."all_channel_bcc_class_id","sweltering"."nov_2001_bcc_class_id") as "all_channel_bcc_class_id",
      coalesce("concerned"."all_channel_bcc_class_id","sweltering"."nov_2001_bcc_class_id") as "nov_2001_bcc_class_id"
  FROM
      "concerned"
      LEFT OUTER JOIN "sweltering" on "concerned"."all_channel_bcc_brand_id" = "sweltering"."nov_2001_bcc_brand_id" AND "concerned"."all_channel_bcc_category_id" is not distinct from "sweltering"."nov_2001_bcc_category_id" AND "concerned"."all_channel_bcc_class_id" is not distinct from "sweltering"."nov_2001_bcc_class_id"
  WHERE
      coalesce("concerned"."all_channel_bcc_brand_id","sweltering"."nov_2001_bcc_brand_id") is not null and coalesce("concerned"."all_channel_bcc_class_id","sweltering"."nov_2001_bcc_class_id") is not null and coalesce("concerned"."all_channel_bcc_category_id","sweltering"."nov_2001_bcc_category_id") is not null
  ),
  young as (
  SELECT
      "abundant"."overall_stats_overall_avg" as "overall_stats_overall_avg",
      "concerned"."all_channel_bcc_brand_id" as "all_channel_bcc_brand_id",
      "concerned"."all_channel_bcc_category_id" as "all_channel_bcc_category_id",
      "concerned"."all_channel_bcc_class_id" as "all_channel_bcc_class_id"
  FROM
      "concerned"
      INNER JOIN "abundant" on 1=1
  WHERE
      "concerned"."all_channel_bcc_brand_id" is not null and "concerned"."all_channel_bcc_class_id" is not null and "concerned"."all_channel_bcc_category_id" is not null
  ),
  macho as (
  SELECT
      "late"."all_channel_bcc_category_id" as "all_channel_bcc_category_id",
      "late"."all_channel_bcc_class_id" as "all_channel_bcc_class_id",
      "late"."nov_2001_bcc_brand_id" as "nov_2001_bcc_brand_id",
      "late"."nov_2001_bcc_channel" as "nov_2001_bcc_channel",
      grouping("late"."nov_2001_bcc_brand_id") as "_virt_agg_grouping_4113029141657478",
      grouping("late"."nov_2001_bcc_category_id") as "_virt_agg_grouping_9304296554823461",
      grouping("late"."nov_2001_bcc_channel") as "_virt_agg_grouping_1552900560272233",
      grouping("late"."nov_2001_bcc_class_id") as "_virt_agg_grouping_4124217819479783",
      sum("late"."nov_2001_bcc_sale_count") as "total_count",
      sum("late"."nov_2001_bcc_total_sales") as "total_sales"
  FROM
      "late"
  WHERE
      "late"."all_channel_bcc_brand_id" is not null and "late"."all_channel_bcc_class_id" is not null and "late"."all_channel_bcc_category_id" is not null

  GROUP BY
      ROLLUP (4, 3, "late"."nov_2001_bcc_class_id", "late"."nov_2001_bcc_category_id")),
  scrawny as (
  SELECT
      "late"."nov_2001_bcc_category_id" as "nov_2001_bcc_category_id",
      "late"."nov_2001_bcc_class_id" as "nov_2001_bcc_class_id",
      "macho"."_virt_agg_grouping_1552900560272233" as "_virt_agg_grouping_1552900560272233",
      "macho"."_virt_agg_grouping_4113029141657478" as "_virt_agg_grouping_4113029141657478",
      "macho"."_virt_agg_grouping_4124217819479783" as "_virt_agg_grouping_4124217819479783",
      "macho"."_virt_agg_grouping_9304296554823461" as "_virt_agg_grouping_9304296554823461",
      "macho"."total_count" as "total_count",
      "macho"."total_sales" as "total_sales",
      coalesce("late"."all_channel_bcc_category_id","macho"."all_channel_bcc_category_id") as "all_channel_bcc_category_id",
      coalesce("late"."all_channel_bcc_class_id","macho"."all_channel_bcc_class_id") as "all_channel_bcc_class_id",
      coalesce("late"."nov_2001_bcc_brand_id","macho"."nov_2001_bcc_brand_id") as "nov_2001_bcc_brand_id",
      coalesce("late"."nov_2001_bcc_channel","macho"."nov_2001_bcc_channel") as "nov_2001_bcc_channel"
  FROM
      "macho"
      INNER JOIN "late" on "macho"."all_channel_bcc_category_id" = "late"."all_channel_bcc_category_id" AND "macho"."all_channel_bcc_class_id" = "late"."all_channel_bcc_class_id" AND "macho"."nov_2001_bcc_brand_id" = "late"."nov_2001_bcc_brand_id" AND "macho"."nov_2001_bcc_channel" is not distinct from "late"."nov_2001_bcc_channel"
  WHERE
      "late"."all_channel_bcc_brand_id" is not null and coalesce("late"."all_channel_bcc_class_id","macho"."all_channel_bcc_class_id") is not null and coalesce("late"."all_channel_bcc_category_id","macho"."all_channel_bcc_category_id") is not null
  ),
  friendly as (
  SELECT
      "scrawny"."nov_2001_bcc_category_id" as "nov_2001_bcc_category_id",
      "scrawny"."nov_2001_bcc_channel" as "channel",
      "scrawny"."nov_2001_bcc_class_id" as "nov_2001_bcc_class_id",
      "scrawny"."total_count" as "total_count",
      "scrawny"."total_sales" as "total_sales",
      ( ( "scrawny"."_virt_agg_grouping_1552900560272233" + "scrawny"."_virt_agg_grouping_4113029141657478" ) + "scrawny"."_virt_agg_grouping_4124217819479783" ) + "scrawny"."_virt_agg_grouping_9304296554823461" as "_level",
      CASE
  	WHEN "scrawny"."_virt_agg_grouping_4113029141657478" = 1 THEN null
  	ELSE coalesce("concerned"."all_channel_bcc_brand_id","scrawny"."nov_2001_bcc_brand_id")
  	END as "brand_id",
      CASE
  	WHEN "scrawny"."_virt_agg_grouping_4124217819479783" = 1 THEN null
  	ELSE "scrawny"."nov_2001_bcc_class_id"
  	END as "class_id",
      CASE
  	WHEN "scrawny"."_virt_agg_grouping_9304296554823461" = 1 THEN null
  	ELSE "scrawny"."nov_2001_bcc_category_id"
  	END as "category_id",
      coalesce("concerned"."all_channel_bcc_brand_id","scrawny"."nov_2001_bcc_brand_id") as "all_channel_bcc_brand_id",
      coalesce("concerned"."all_channel_bcc_brand_id","scrawny"."nov_2001_bcc_brand_id") as "nov_2001_bcc_brand_id",
      coalesce("concerned"."all_channel_bcc_category_id","scrawny"."all_channel_bcc_category_id") as "all_channel_bcc_category_id",
      coalesce("concerned"."all_channel_bcc_class_id","scrawny"."all_channel_bcc_class_id") as "all_channel_bcc_class_id"
  FROM
      "concerned"
      INNER JOIN "scrawny" on "concerned"."all_channel_bcc_brand_id" = "scrawny"."nov_2001_bcc_brand_id" AND "concerned"."all_channel_bcc_category_id" = "scrawny"."all_channel_bcc_category_id" AND "concerned"."all_channel_bcc_class_id" = "scrawny"."all_channel_bcc_class_id"
  WHERE
      coalesce("concerned"."all_channel_bcc_brand_id","scrawny"."nov_2001_bcc_brand_id") is not null and coalesce("concerned"."all_channel_bcc_class_id","scrawny"."all_channel_bcc_class_id") is not null and coalesce("concerned"."all_channel_bcc_category_id","scrawny"."all_channel_bcc_category_id") is not null
  )
  SELECT
      "friendly"."channel" as "channel",
      "friendly"."brand_id" as "brand_id",
      "friendly"."class_id" as "class_id",
      "friendly"."category_id" as "category_id",
      "friendly"."total_sales" as "total_sales",
      "friendly"."total_count" as "total_count"
  FROM
      "young"
      INNER JOIN "friendly" on "young"."all_channel_bcc_brand_id" = "friendly"."nov_2001_bcc_brand_id" AND "young"."all_channel_bcc_category_id" = "friendly"."nov_2001_bcc_category_id" AND "young"."all_channel_bcc_class_id" = "friendly"."nov_2001_bcc_class_id"
  WHERE
      "friendly"."total_sales" > "young"."overall_stats_overall_avg"

  ORDER BY
      "friendly"."_level" asc,
      "friendly"."channel" asc nulls first,
      "friendly"."brand_id" asc nulls first,
      "friendly"."class_id" asc nulls first,
      "friendly"."category_id" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query38.preql`

  ```text
  Resolution error in query38.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 16). The requested concepts split into 3 disconnected subgraphs: {in_catalog}; {in_store, ss.customer.first_name, ss.customer.last_name, ss.date.date}; {in_web}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query38.preql`

  ```text
  Syntax error in query38.preql: Undefined concept: _channel_count. Suggestions: ['combo_channel_count', 'distinct_channel_count']
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: A column tuple ('ref:ss.date.year', 'ref:ss.item.id', 'ref:ss.customer.id') can only be tested for membership against a tuple of the same arity, e.g. `(a, b) in (set.a, set.b)`.
  ```
- `trilogy file read query78.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Undefined concept: wr.item.text_id. Suggestions: ['wr.web_sales.item.text_id', 'sr.store.date.text_id', 'sr.store.text_id', 'sr.item.text_id', 'sr.return_date.text_id', 'sr.time.text_id']
  ```
- `trilogy run --import raw.web_sales:ws with base_orders as where ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date and ws.ship_address.state = 'IL' and ws.web_site.company_name = 'pri' select ws.order_number; select count(base_orders.ws_order_number) as cnt; duckdb`

  ```text
  Syntax error in stdin: Undefined concept: base_orders.ws_order_number (line 2, in SELECT). Suggestions: ['base_orders.ws.order_number', 'ws.order_number']
  ```
- `trilogy file read query94.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query31.preql`

  ```text
  Resolution error in query31.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {local.ss_q1_ext_sales, local.ss_q2_ext_sales, local.ss_q3_ext_sales, local.ws_q1_ext_sales, local.ws_q2_ext_sales, local.ws_q3_ext_sales, ss.sale_address.county, ws.bill_address.county}
  ```
- `trilogy run query31.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 2098 (char 2097). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {best_performers.items_above_threshold.ss.item.product_name, best_performers.rank, worst_performers.items_above_threshold.ss.item.product_name}
  ```
- `trilogy run query44.preql`

  ```text
  Unexpected error in query44.preql: (_duckdb.BinderException) Binder Error: column "ss_item_id" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(ss_item_id)" if the exact value of "ss_item_id" is not important.

  LINE 23: ..."."threshold" and "questionable"."ss_store_id" = 1 THEN "questionable"."ss_item_id" ELSE NULL END as "_virt_filter_...
                                                                      ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      avg(CASE WHEN "ss_store_sales"."SS_ADDR_SK" is null THEN "ss_store_sales"."SS_NET_PROFIT" ELSE NULL END) as "null_addr_avg_net_profit"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1),
  questionable as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
      "thoughtful"."null_addr_avg_net_profit" * 0.9 as "threshold"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "thoughtful" on "ss_store_sales"."SS_ITEM_SK" = "thoughtful"."ss_item_id"),
  abundant as (
  SELECT
      "questionable"."ss_store_id" as "ss_store_id",
      CASE WHEN avg("questionable"."ss_net_profit") > "questionable"."threshold" and "questionable"."ss_store_id" = 1 THEN "questionable"."ss_item_id" ELSE NULL END as "_virt_filter_id_7691558876961924"
  FROM
      "questionable"
  GROUP BY
      1,
      "questionable"."threshold"
  HAVING
      avg("questionable"."ss_net_profit") > "questionable"."threshold"
  ),
  uneven as (
  SELECT
      "abundant"."_virt_filter_id_7691558876961924" as "_virt_filter_id_7691558876961924"
  FROM
      "abundant"
  WHERE
      "abundant"."ss_store_id" = 1
  ),
  highfalutin as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      avg("ss_store_sales"."SS_NET_PROFIT") as "avg_net_profit_per_item"
  FROM
      "store_sales" as "ss_store_sales"
  WHERE
      "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ITEM_SK" in (select uneven."_virt_filter_id_7691558876961924" from uneven where uneven."_virt_filter_id_7691558876961924" is not null)

  GROUP BY
      1),
  yummy as (
  SELECT
      "highfalutin"."avg_net_profit_per_item" as "avg_net_profit_per_item",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name"
  FROM
      "highfalutin"
      INNER JOIN "item" as "ss_item_items" on "highfalutin"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_item_items"."I_ITEM_SK" in (select uneven."_virt_filter_id_7691558876961924" from uneven where uneven."_virt_filter_id_7691558876961924" is not null)
  ),
  juicy as (
  SELECT
      "yummy"."ss_item_product_name" as "qualifying_items_ss_item_product_name",
      dense_rank() over (order by "yummy"."avg_net_profit_per_item" asc ) as "asc_rank",
      dense_rank() over (order by "yummy"."avg_net_profit_per_item" desc ) as "desc_rank"
  FROM
      "yummy"),
  concerned as (
  SELECT
      "juicy"."asc_rank" as "ranked_asc_rank",
      "juicy"."desc_rank" as "ranked_desc_rank",
      "juicy"."qualifying_items_ss_item_product_name" as "ranked_qualifying_items_ss_item_product_name"
  FROM
      "juicy"),
  abhorrent as (
  SELECT
      "concerned"."ranked_desc_rank" as "_worst_pair_rank",
      "concerned"."ranked_qualifying_items_ss_item_product_name" as "_worst_worst_product_name"
  FROM
      "concerned"
  WHERE
      "concerned"."ranked_desc_rank" <= 10

  GROUP BY
      1,
      2),
  young as (
  SELECT
      "concerned"."ranked_asc_rank" as "_best_pair_rank",
      "concerned"."ranked_qualifying_items_ss_item_product_name" as "_best_best_product_name"
  FROM
      "concerned"
  WHERE
      "concerned"."ranked_asc_rank" <= 10

  GROUP BY
      1,
      2),
  sweltering as (
  SELECT
      "abhorrent"."_worst_pair_rank" as "worst_pair_rank",
      "abhorrent"."_worst_worst_product_name" as "worst_worst_product_name"
  FROM
      "abhorrent"),
  sparkling as (
  SELECT
      "young"."_best_best_product_name" as "best_best_product_name",
      "young"."_best_pair_rank" as "best_pair_rank"
  FROM
      "young")
  SELECT
      coalesce("sparkling"."best_pair_rank","sweltering"."worst_pair_rank") as "rank",
      "sparkling"."best_best_product_name" as "best_performer_product_name",
      "sweltering"."worst_worst_product_name" as "worst_performer_product_name"
  FROM
      "sweltering"
      FULL JOIN "sparkling" on "sweltering"."worst_pair_rank" = "sparkling"."best_pair_rank"
  ORDER BY
      "rank" asc,
      "best_performer_product_name" desc,
      "worst_performer_product_name" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query49.preql`

  ```text
  Syntax error in query49.preql: Output column 'return_ratio' renames 'local.return_ratio' back to the name of an existing concept 'return_ratio' (defined at line 3) that 'local.return_ratio' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'return_ratio_out').
  ```
- `trilogy run query49.preql`

  ```text
  Unexpected error in query49.preql: (_duckdb.BinderException) Binder Error: column "sales_channel" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(sales_channel)" if the exact value of "sales_channel" is not important.

  LINE 120:     LOWER("vacuous"."sales_channel")  asc nulls first,
                      ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
      "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
      "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity"
  FROM
      "catalog_returns" as "sales_catalog_returns_unified"
  WHERE
      "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" > 10000

  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
      "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
      "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
      "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity"
  FROM
      "store_returns" as "sales_store_returns_unified"
  WHERE
      "sales_store_returns_unified"."SR_RETURN_AMT" > 10000

  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
      "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
      "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
      "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity"
  FROM
      "web_returns" as "sales_web_returns_unified"
  WHERE
      "sales_web_returns_unified"."WR_RETURN_AMT" > 10000
  ),
  abundant as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
      "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
      "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
  WHERE
      "sales_catalog_sales_unified"."CS_NET_PROFIT" > 1 and "sales_catalog_sales_unified"."CS_NET_PAID" > 0 and "sales_catalog_sales_unified"."CS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12

  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
      "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
      "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
      "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity"
  FROM
      "store_sales" as "sales_store_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
  WHERE
      "sales_store_sales_unified"."SS_NET_PROFIT" > 1 and "sales_store_sales_unified"."SS_NET_PAID" > 0 and "sales_store_sales_unified"."SS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12

  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
      "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
      "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity"
  FROM
      "web_sales" as "sales_web_sales_unified"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
  WHERE
      "sales_web_sales_unified"."WS_NET_PROFIT" > 1 and "sales_web_sales_unified"."WS_NET_PAID" > 0 and "sales_web_sales_unified"."WS_QUANTITY" > 0 and "sales_date_date"."D_YEAR" = 2001 and "sales_date_date"."D_MOY" = 12
  ),
  yummy as (
  SELECT
      "abundant"."sales_item_id" as "sales_item_id",
      "cheerful"."sales_channel" as "sales_channel",
      sum("abundant"."sales_net_paid") as "_virt_agg_sum_2775739316613904",
      sum("abundant"."sales_quantity") as "_virt_agg_sum_6186806241713450",
      sum("cheerful"."sales_return_amount") as "_virt_agg_sum_8056058741317745",
      sum(coalesce("cheerful"."sales_return_quantity",0)) as "_virt_agg_sum_4057596625194597"
  FROM
      "abundant"
      INNER JOIN "cheerful" on "abundant"."sales_channel" = "cheerful"."sales_channel" AND "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id"
  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "yummy"."_virt_agg_sum_4057596625194597" / "yummy"."_virt_agg_sum_6186806241713450" as "return_ratio",
      "yummy"."sales_channel" as "sales_channel",
      "yummy"."sales_item_id" as "item",
      LOWER("yummy"."sales_channel")  as "channel",
      rank() over (partition by "yummy"."sales_channel" order by "yummy"."_virt_agg_sum_4057596625194597" / "yummy"."_virt_agg_sum_6186806241713450" asc nulls first ) as "return_rank",
      rank() over (partition by "yummy"."sales_channel" order by "yummy"."_virt_agg_sum_8056058741317745" / "yummy"."_virt_agg_sum_2775739316613904" asc nulls first ) as "currency_rank"
  FROM
      "yummy")
  SELECT
      "vacuous"."channel" as "channel",
      "vacuous"."item" as "item",
      "vacuous"."return_ratio" as "return_ratio",
      "vacuous"."return_rank" as "return_rank",
      "vacuous"."currency_rank" as "currency_rank"
  FROM
      "vacuous"
  WHERE
      "vacuous"."return_rank" <= 10 or "vacuous"."currency_rank" <= 10

  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      LOWER("vacuous"."sales_channel")  asc nulls first,
      "vacuous"."return_rank" asc nulls first,
      "vacuous"."currency_rank" asc nulls first,
      "vacuous"."item" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query57.preql`

  ```text
  Syntax error in query57.preql: Comparison `cs.sold_date.month_of_year between 1 and 12` matches every value of enum field 'cs.sold_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'store_sales.store.id', which is not in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.store.id order by store_sales.store.id asc`.
  ```
- `trilogy file read query65.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

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
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: Comparison `cs.bill_customer_demographic.gender = 'Female'` can never match enum field 'cs.bill_customer_demographic.gender', which contains only these values: 'M', 'F'. It is always false and should be removed.
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(local.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.country) as g order by g desc`.
  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query21.preql`

  ```text
  Unexpected error in query21.preql: (_duckdb.BinderException) Binder Error: Values list "questionable" does not have a column named "before_qoh"

  LINE 43:     "questionable"."before_qoh" as "before_total",
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "inv_item_items"."I_CURRENT_PRICE" as "inv_item_current_price",
      "inv_item_items"."I_ITEM_ID" as "inv_item_text_id",
      "inv_warehouse_inventory"."inv_quantity_on_hand" as "inv_quantity_on_hand",
      "inv_warehouse_inventory"."inv_warehouse_sk" as "inv_warehouse_id",
      cast("inv_date_date"."D_DATE" as date) as "inv_date_date"
  FROM
      "inventory" as "inv_warehouse_inventory"
      INNER JOIN "date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
      INNER JOIN "item" as "inv_item_items" on "inv_warehouse_inventory"."inv_item_sk" = "inv_item_items"."I_ITEM_SK"),
  thoughtful as (
  SELECT
      "cheerful"."inv_item_text_id" as "inv_item_text_id",
      "cheerful"."inv_warehouse_id" as "inv_warehouse_id",
      sum(CASE WHEN "cheerful"."inv_date_date" >= date '2000-02-10' and "cheerful"."inv_date_date" < date '2000-03-11' THEN "cheerful"."inv_quantity_on_hand" ELSE NULL END) as "before_qoh",
      sum(CASE WHEN "cheerful"."inv_date_date" >= date '2000-03-11' and "cheerful"."inv_date_date" <= date '2000-04-10' THEN "cheerful"."inv_quantity_on_hand" ELSE NULL END) as "after_qoh"
  FROM
      "cheerful"
  GROUP BY
      1,
      2),
  questionable as (
  SELECT
      "cheerful"."inv_item_text_id" as "inv_item_text_id",
      "cheerful"."inv_warehouse_id" as "inv_warehouse_id"
  FROM
      "cheerful"
      INNER JOIN "thoughtful" on "cheerful"."inv_item_text_id" = "thoughtful"."inv_item_text_id" AND "cheerful"."inv_warehouse_id" is not distinct from "thoughtful"."inv_warehouse_id"
  WHERE
      "cheerful"."inv_item_current_price" BETWEEN 0.99 AND 1.49 and ( "thoughtful"."after_qoh" * 1.0 ) / nullif("thoughtful"."before_qoh",0) is not null and ( "thoughtful"."after_qoh" * 1.0 ) / nullif("thoughtful"."before_qoh",0) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0

  GROUP BY
      1,
      2,
      "thoughtful"."after_qoh",
      "thoughtful"."before_qoh")
  SELECT
      "inv_warehouse_warehouse"."w_warehouse_name" as "warehouse_name",
      "questionable"."inv_item_text_id" as "item_code",
      "questionable"."before_qoh" as "before_total",
      "questionable"."after_qoh" as "after_total"
  FROM
      "questionable"
      INNER JOIN "warehouse" as "inv_warehouse_warehouse" on "questionable"."inv_warehouse_id" = "inv_warehouse_warehouse"."w_warehouse_sk"
  ORDER BY
      "warehouse_name" asc nulls first,
      "item_code" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query26.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query30.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query34.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {i.current_price, i.id, i.manufacturer_id, current_price, description, item_code}; {inv.date.date, inv.quantity_on_hand}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query45.preql`

  ```text
  Syntax error in query45.preql: Undefined concept: item.id. Suggestions: ['web.item.id', 'web.date.id', 'web.ship_date.id', 'web.return_date.id', 'web.time.id', 'web.return_time.id']
  ```
- `trilogy file read query51.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query53.preql duckdb`

  ```text
  Syntax error in query53.preql: Undefined concept: date.year. Suggestions: ['ss.date.year', 'ss.store.date.year', 'ss.return_store.date.year', 'ss.return_date.year', 'ss.customer.first_sales_date.year', 'ss.customer.first_shipto_date.year']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query61.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Ambiguous reference 'ss_agg.year': matches ['ss_agg.ss.customer.first_sales_date.year', 'ss_agg.ss.customer.first_shipto_date.year', 'ss_agg.ss.date.year']. Qualify the full path to disambiguate.
  ```
- `trilogy file read query66.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query74.preql duckdb tpcds.duckdb`

  ```text
  Syntax error in query74.preql: Undefined concept: web_2002.web_2001_total (line 61, in WHERE). Suggestions: ['web_2001.web_2001_total', 'web_2002.web_2002_total', '_web_2002_web_2002_total']
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `grouping(s.channel)` (line 5), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(s.channel) as g order by g desc`.
  ```
- `trilogy file read query77.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 9). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {description, item_code, ss.item.current_price, ss.item.id, ss.item.manufacturer_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query86.preql`

  ```text
  Syntax error in query86.preql: All case expressions must have the same output datatype, got {<DataType.STRING: 'string'>, <DataType.INTEGER: 'int'>} from {'0': <DataType.INTEGER: 'int'>, 'ref:ws.item.category': EnumType(type=<DataType.STRING: 'string'>, values=['Books', 'Children', 'Electronics', 'Home', 'Jewelry', 'Men', 'Music', 'Shoes', 'Sports', 'Women'])}
  ```
- `trilogy file read query86.preql`

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

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id where all_sales.channel_dim_text_id is not null group by all_sales.channel, all_sales.channel_dim_text_id order by all_sales.channel limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...annel_dim_text_id is not null ??? group by all_sales.channel, al...
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Find (brand, class, category) combos that appear in all 3 channels in 1999-2…ategory_id)
order by _level asc, channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:39
    |
  4 | auto qualifying_bcc <- s.item.brand_id, s.item.class_id, s.item.category_id
    |                                       ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...lifying_bcc <- s.item.brand_id ??? , s.item.class_id, s.item.cate...

  Write stats: received 1946 chars / 1946 bytes; tail: …'_id asc nulls first, category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Find BCC combos present in all 3 channels in 1999-2001
rowset all_channel_bc…ategory_id)
order by _level asc, channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...st_price) as total_sales,     ??? count(*) as sale_count having

  Write stats: received 2056 chars / 2056 bytes; tail: …'_id asc nulls first, category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Find BCC combos present in all 3 channels in 1999-2001
rowset all_channel_bc…ategory_id)
order by _level asc, channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 47:48
     |
  47 | having sum(nov_2001_bcc.total_sales) > (select * from overall_stats).overall_avg
     |                                                ^---
     |
     = expected select_item
  Location:
  ...01_bcc.total_sales) > (select ??? * from overall_stats).overall_...

  Write stats: received 2062 chars / 2062 bytes; tail: …'_id asc nulls first, category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Rowset: store sales (… = catalog_combos.sale_date = web_combos.sale_date
where
    catalog_combos.last_name is not null
    and web_combos.last_name is not null
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...aring in all three select     ??? count(*) as cnt union join sto...

  Write stats: received 1449 chars / 1449 bytes; tail: …'not null\\n    and web_combos.last_name is not null\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.year, count(ss.customer.id) as cnt group by ss.date.year order by ss.date.year limit 100;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   count(ss.customer.id) as cnt ??? group by ss.date.year order by...
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.year, count(ss.customer.id) as cnt by ss.date.year order by ss.date.year limit 100;`

  ```text
  Syntax error in stdin:  --> 2:51
    |
  2 | select ss.date.year, count(ss.customer.id) as cnt by ss.date.year order by ss.date.year limit 100;
    |                                                   ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   count(ss.customer.id) as cnt ??? by ss.date.year order by ss.da...
  ```
- `trilogy run --import raw.all_sales:all where all.date.year = 2000 and all.purchasing_customer.id is not null select count(all.purchasing_customer.id) as cnt, all.channel by all.channel;`

  ```text
  Syntax error in stdin: Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...tomer.id) as cnt, all.channel ??? by all.channel;
  ```
- `trilogy run --import raw.all_sales:all where all.date.year = 2000 and all.purchasing_customer.id is not null select all.channel, count(all.purchasing_customer.id) as cnt by all.channel;`

  ```text
  Syntax error in stdin:  --> 2:132
    |
  2 | where all.date.year = 2000 and all.purchasing_customer.id is not null select all.channel, count(all.purchasing_customer.id) as cnt by all.channel;
    |                                                                                                                                    ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...urchasing_customer.id) as cnt ??? by all.channel;
  ```
- `trilogy file write query38.preql --content import raw.all_sales as all;

# Per (last_name, first_name, date, channel) - count rows as presence indicator
auto…count > 0;

# Count of combos appearing in all three channels
select
    count(distinct_channel_count ? distinct_channel_count = 3) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
   <- count(combo_channel_count ??? by all.purchasing_customer.las...

  Write stats: received 734 chars / 734 bytes; tail: …'hannel_count ? distinct_channel_count = 3) as cnt\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/web_sales:ws select count(ss.line_item) as cnt where ss.date.year=2000 and ss.customer.id is not null an…d, ss.customer.id) in (ws.item.id, ws.billing_customer.id) where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned = false;`

  ```text
  Syntax error in stdin:  --> 3:190
    |
  3 | select count(ss.line_item) as cnt where ss.date.year=2000 and ss.customer.id is not null and ss.is_returned = false and (ss.item.id, ss.customer.id) in (ws.item.id, ws.billing_customer.id) where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned = false;
    |                                                                                                                                                                                              ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...m.id, ws.billing_customer.id) ??? where ws.date.year=2000 and ws...
  ```
- `trilogy run --import raw/store_sales:ss select count(ss.line_item) as cnt where ss.date.year=2000 and ss.customer.id is not null and ss.is_returned = false a…illing_customer.id::string) as k from raw/web_sales as ws where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned = false);`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ing_customer.id::string) as k ??? from raw/web_sales as ws where...
  ```
- `trilogy run --import raw/web_sales:ws --import raw/catalog_sales:cs select concat(ws.item.id::string,'-',ws.billing_customer.id::string) as k, count(ws.line_item) as cnt where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned = false group by k order by cnt desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ll and ws.is_returned = false ??? group by k order by cnt desc l...
  ```
- `trilogy run --import raw/store_sales:ss select count(ss.customer.id) as c where ss.date.year=2000 and ss.customer.id is not null and ss.is_returned = false a…n (select ws.billing_customer.id from raw/web_sales as ws where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned = false);`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...select ws.billing_customer.id ??? from raw/web_sales as ws where...
  ```
- `trilogy run --import raw/store_sales:ss select ss.item.id as iid, ss.customer.id as cid, count(ss.line_item) as cnt where ss.date.year=2000 and ss.is_returned = false and ss.item.id in (12467, 15743) group by ss.item.id, ss.customer.id;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   ss.item.id in (12467, 15743) ??? group by ss.item.id, ss.custom...
  ```
- `trilogy run --import raw/store_sales:ss select ss.item.id as iid, ss.customer.id as cid, sum(ss.quantity) as sq where ss.date.year=2000 and ss.customer.id is not null and ss.is_returned = false group by ss.item.id having sum(ss.quantity) = 12;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ll and ss.is_returned = false ??? group by ss.item.id having sum...
  ```
- `trilogy file write query06.preql --content import raw.store_sales as ss;

# average item current price by category (each item once, not weighted by volume)
a…s.customer.address.state
having line_item_count >= 10
order by line_item_count asc nulls first, ss.customer.address.state asc nulls first
limit 100;
`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...stomer.address.id is not null ??? group by ss.customer.address.s...

  Write stats: received 769 chars / 769 bytes; tail: …'first, ss.customer.address.state asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query27.preql -e -c import raw.store_sales as ss;

where
  ss.date.year = 2002
  and ss.store.state = 'TN'
  and ss.customer_demographic.m…avg_unit_price
by rollup (ss.item.text_id, ss.store.state)
order by grp_indicator, ss.item.text_id nulls first, ss.store.state nulls first
limit 100;`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:27
     |
  12 |   grouping(ss.item.text_id, ss.store.state) as grp_indicator,
     |                           ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...te,   grouping(ss.item.text_id ??? , ss.store.state) as grp_indic...

  Write stats: received 651 chars / 651 bytes; tail: …'m.text_id nulls first, ss.store.state nulls first\\nlimit 100;'.
  ```
- `trilogy file write query31.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Compute per-county store sales by quarter in 2000
aut…ver_q2,
    ss_q3_q2 as store_q3_over_q2
union join ss_county = ws_county
having
    ws_q2_q1 > ss_q2_q1
    and ws_q3_q2 > ss_q3_q2
order by county;`

  ```text
  refused to write 'query31.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...0 and ws_q2 > 0 and ws_q3 > 0 ??? auto ws_q2_q1 <- ws_q2 / ws_q1...

  Write stats: received 1449 chars / 1449 bytes; tail: …'2_q1 > ss_q2_q1\\n    and ws_q3_q2 > ss_q3_q2\\norder by county;'.
  ```
- `trilogy file write query40.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Filter to items with current_price between 0.…efunded_cash, 0) ? cs.sold_date.date >= '2000-03-11'::date) as after_20000311
order by
    cs.warehouse.state asc,
    cs.item.text_id asc
limit 100;`

  ```text
  refused to write 'query40.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:21
     |
  12 | subset join cs = cr on cs.order_number = cr.order_number, cs.item.id = cr.item.id
     |                     ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...not null  subset join cs = cr ??? on cs.order_number = cr.order_...

  Write stats: received 884 chars / 884 bytes; tail: …'  cs.warehouse.state asc,\\n    cs.item.text_id asc\\nlimit 100;'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Eight attribute profiles (category, color, units, size):
# 1. Books / tan / Oz / N/A
#…m.manufacturer_id between 1 and 500
  and item.manufact in profile_manufs
select distinct
  item.product_name
order by
  item.product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...fact in profile_manufs select ??? distinct   item.product_name o...

  Write stats: received 1716 chars / 1716 bytes; tail: …'\\n  item.product_name\\norder by\\n  item.product_name\\nlimit 100;'.
  ```
- `trilogy file write query70.preql -e -c import raw.store_sales as ss;

where ss.date.year = 2000
select
    ss.store.state,
    ss.store.county,
    sum(ss.ne…    hierarchy_level,
    rnk
order by
    hierarchy_level desc,
    case when hierarchy_level = 0 then ss.store.state end asc,
    rnk asc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:37
    |
  9 |         when grouping(ss.store.state, ss.store.county) = 0 then 2
    |                                     ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
    when grouping(ss.store.state ??? , ss.store.county) = 0 then 2

  Write stats: received 885 chars / 885 bytes; tail: …'evel = 0 then ss.store.state end asc,\\n    rnk asc\\nlimit 100;'.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store sales: annual v…y
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;
`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset store_2001 as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...by customer for 2001 and 2002 ??? rowset store_2001 as where ss....

  Write stats: received 2892 chars / 2892 bytes; tail: …'s first,\\n    preferred_cust_flag asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query09.preql`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ..._81_100 end as bucket_81_100
   ??? by *;


  Write stats: received 2154 chars / 2154 bytes; tail: …'isc_81_100 else avg_net_81_100 end as bucket_81_100\r\\nby *;\r\\n'.
  ```
- `trilogy file write query09.preql`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:1
     |
  20 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...1 and 100) as avg_net_81_100
   ??? by *;

   select
       case when...

  Write stats: received 1846 chars / 1846 bytes; tail: …'100 else aggregated.avg_net_81_100 end as bucket_81_100\r\\n;\r\\n'.
  ```
- `trilogy file write query09.preql`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:1
     |
  33 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...81_100_val) as bucket_81_100
   ??? by *;


  Write stats: received 1917 chars / 1917 bytes; tail: …'61_80,\r\\n    sum(bucket_81_100_val) as bucket_81_100\r\\nby *;\r\\n'.
  ```
- `trilogy file write query28.preql --content import raw.store_sales as store_sales;

# Bucket 1: quantity 0-5, AND (list_price 8-18 OR coupon_amt 459-1459 OR w…as bucket5_unique_list_price,
  @b6_avg as bucket6_avg_list_price,
  @b6_cnt as bucket6_count_list_price,
  @b6_cnt_d as bucket6_unique_list_price,
;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
     --> 118:3
      |
  118 |   @b1_avg as bucket1_avg_list_price,
      |   ^---
      |
      = expected select_item
  Location:
  ...between 7 and 27));  select   ??? @b1_avg as bucket1_avg_list_pr...

  Write stats: received 5740 chars / 5740 bytes; tail: …'ount_list_price,\\n  @b6_cnt_d as bucket6_unique_list_price,\\n;'.
  ```
- `trilogy file write query28.preql`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
     --> 118:3
      |
  118 |   @b1_avg as bucket1_avg_list_price,
      |   ^---
      |
      = expected select_item
  Location:
  ...ween 7 and 27));

   select
     ??? @b1_avg as bucket1_avg_list_pr...

  Write stats: received 5875 chars / 5875 bytes; tail: …'nt_list_price,\r\\n  @b6_cnt_d as bucket6_unique_list_price,\r\\n;'.
  ```
- `trilogy file write query36.preql --content import raw.store_sales as ss;

# Hierarchy-level markers: 0 = leaf (category + class), 1 = category subtotal, 2 = ….class)
where ss.store.state = 'TN' and ss.date.year = 2001
order by level desc nulls first, category asc nulls first, rnk asc nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:1
     |
  25 | where ss.store.state = 'TN' and ss.date.year = 2001
     | ^---
     |
     = expected limit or order_by
  Location:
  ...item.category, ss.item.class) ??? where ss.store.state = 'TN' an...

  Write stats: received 999 chars / 999 bytes; tail: …'st, category asc nulls first, rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query51.preql --content import raw.web_sales as web;
import raw.store_sales as store;

# Per-item, per-day running total of unit price fro…running.sale_date = store_running.sale_date
having web_running_max > store_running_max
order by item asc nulls first, date asc nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset web_running as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...e from web sales in year 2000 ??? rowset web_running as where we...

  Write stats: received 1595 chars / 1595 bytes; tail: …'der by item asc nulls first, date asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query53.preql --content import raw.store_sales as ss;

# Filter to year 2000, recorded store, and matching item profiles
where date.year =…elect
    item.manufacturer_id,
    sum(ss.sales_price) as quarterly_total
      by item.manufacturer_id, date.quarter
      as manuf_quarter_total;
`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:7
     |
  18 |       by item.manufacturer_id, date.quarter
     |       ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ice) as quarterly_total       ??? by item.manufacturer_id, date....

  Write stats: received 809 chars / 809 bytes; tail: …'manufacturer_id, date.quarter\\n      as manuf_quarter_total;\\n'.
  ```
- `trilogy file write query61.preql --content import raw/store_sales as sales;
import raw/date as d;
import raw/item as i;
import raw/store as s;
import raw/pro…ct <- promo_total / overall_total * 100.0;

select
    promo_total,
    overall_total,
    pct
order by
    promo_total,
    overall_total
limit 100;`

  ```text
  refused to write 'query61.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as sales; impo...

  Write stats: received 984 chars / 986 bytes; tail: …'  pct\\norder by\\n    promo_total,\\n    overall_total\\nlimit 100;'.
  ```
- `trilogy file write query61.preql --content import raw.store_sales as sales;

# Filter: November 1998
filter sales.date.year = 1998;
filter sales.date.month_o…_price) as promo_total,
    sum(sales.ext_sales_price) as overall_total,
    sum(sales.ext_sales_price) / sum(sales.ext_sales_price) * 100.0 as pct
;`

  ```text
  refused to write 'query61.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | filter sales.date.year = 1998;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...les;  # Filter: November 1998 ??? filter sales.date.year = 1998;...

  Write stats: received 554 chars / 554 bytes; tail: …'t_sales_price) / sum(sales.ext_sales_price) * 100.0 as pct\\n;'.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as ss;

# Sum of ext_sales_price (sales_price * quantity, null treated as 0 via coalesce)
a…,
    ss.date.month_of_year asc nulls first,
    ss.store.text_id asc nulls first,
    sum_sales asc nulls first,
    rnk asc nulls first
limit 100;
`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 47:1
     |
  47 | having rnk <= 100
     | ^---
     |
     = expected limit or order_by
  Location:
  ...th_of_year, ss.store.text_id) ??? having rnk <= 100 order by

  Hint: HAVING must come *before* the `by rollup/cube/grouping sets` clause in Trilogy (the reverse of SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> having <cond> by rollup (<keys>) order by <cols> limit <n>;

  Write stats: received 2482 chars / 2482 bytes; tail: …'m_sales asc nulls first,\\n    rnk asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Deduplicated per-line records for Books items
with line_data as
where sales.item… null
    and y2001.qty is not null
    and y2001.qty > 0
    and y2002.qty / y2001.qty < 0.9
order by
    qty_diff asc,
    amt_diff asc
limit 100;
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | rowset y2001 as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...acturer_id;  # Year 2001 rows ??? rowset y2001 as where line_dat...

  Write stats: received 1925 chars / 1925 bytes; tail: …' 0.9\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query85.preql --content import raw/web_returns as web_returns;

# For web sales sold in year 2000 that were returned
# Match web return to…funded_cash) as avg_refunded_cash,
  avg(web_returns.fee) as avg_fee
order by
  reason_desc_20,
  avg_qty,
  avg_refunded_cash,
  avg_fee
limit 100;
`

  ```text
  refused to write 'query85.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/web_returns as web_returns;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/web_returns as web_returns...

  Write stats: received 2460 chars / 2460 bytes; tail: …'sc_20,\\n  avg_qty,\\n  avg_refunded_cash,\\n  avg_fee\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.item:item select distinct item.class where item.category in ('Men','Jewelry','Women') and item.class in ('birdal','dresses','shirts');`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.class where item...
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales:ss --import raw/web_sales:ws select count(ss.line_item) where ss.date.year = 2000 and ss.customer.id is not null and ss.…,'-',ws.item.id::string,'-',ws.billing_customer.id::string) where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned=false);`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.line_item) as line_item_count`
  Location:
  ...s; select count(ss.line_item) ??? where ss.date.year = 2000 and
  ```
- `trilogy run --import raw/store_sales:ss select count(ss.customer.id) where ss.date.year=2000 and ss.customer.id is not null and ss.is_returned = false and ss…n (select ws.billing_customer.id from raw/web_sales as ws where ws.date.year=2000 and ws.billing_customer.id is not null and ws.is_returned = false);`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.customer.id) as id_count`
  Location:
   select count(ss.customer.id) ??? where ss.date.year=2000 and ss...
  ```
- `trilogy run --import raw.web_sales:ws select ws.ship_date.date, ws.ship_address.state, ws.web_site.company_name, count(ws.line_item) limit 5; duckdb`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ws.line_item) as line_item_count`
  Location:
  ...any_name, count(ws.line_item) ??? limit 5;
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, count(all_sales.order_id) where all_sales.date.week_seq = 5218 group by all_sales.channel;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(all_sales.order_id) as order_id_count`
  Location:
  ...el, count(all_sales.order_id) ??? where all_sales.date.week_seq
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.item.category, count(store_sales.ticket_number) limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(store_sales.ticket_number) as ticket_number_count`
  Location:
  ...nt(store_sales.ticket_number) ??? limit 5;
  ```

### `cli-misuse`

- `trilogy explore raw/sold_date.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sold_date.preql' does not exist.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
