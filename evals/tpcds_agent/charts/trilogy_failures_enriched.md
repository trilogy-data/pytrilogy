# Trilogy failure analysis — 20260707-151529

- Run `20260707-151529` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1124 | failed: 105 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 75 | 71% |
| `syntax-parse` | 27 | 26% |
| `cli-misuse` | 1 | 1% |
| `join-resolution` | 1 | 1% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy file read query01.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query04.preql duckdb`

  ```text
  Resolution error in query04.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 63). The requested concepts split into 6 disconnected subgraphs: {ca_2001.cid, ca_2001.val}; {ca_2002.cid, ca_2002.val}; {st_2001.cid, st_2001.val}; {st_2002.cid, st_2002.fn, st_2002.ln, st_2002.pcf, st_2002.val}; {we_2001.cid, we_2001.val}; {we_2002.cid, we_2002.val}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel_label)` (line 78), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_label) as g order by g desc`.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.address.zip. Suggestions: ['ss.customer.address.zip', 'ss.customer.address.id', 'ss.customer.address.city', 'ss.return_customer.address.zip', 'ss.store.zip', 'ss.return_store.zip']
  ```
- `trilogy run query10.preql`

  ```text
  Syntax error in query10.preql: Comparison `store_sales.date.month_of_year >= 1` matches every value of enum field 'store_sales.date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 37). The requested concepts split into 2 disconnected subgraphs: {billing_customer_code, store_2001.store_rev_2001, store_2002.store_rev_2002, web_2001.web_rev_2001, web_2002.web_rev_2002, ws.billing_customer.first_name, ws.billing_customer.last_name}; {ss.customer.preferred_cust_flag}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(local.channel)` (line 19), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel) as g order by g desc`.
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_SOLD_DATE_SK" as "s_date_id",
      "s_catalog_sales_unified"."CS_ITEM_SK" as "s_item_id",
      "s_catalog_sales_unified"."CS_ORDER_NUMBER" as "s_order_id",
      "s_catalog_sales_unified"."CS_EXT_LIST_PRICE" as "s_ext_list_price"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_SOLD_DATE_SK" as "s_date_id",
      "s_store_sales_unified"."SS_ITEM_SK" as "s_item_id",
      "s_store_sales_unified"."SS_TICKET_NUMBER" as "s_order_id",
      "s_store_sales_unified"."SS_EXT_LIST_PRICE" as "s_ext_list_price"
  FROM
      "store_sales" as "s_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_SOLD_DATE_SK" as "s_date_id",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_id",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id",
      "s_web_sales_unified"."WS_EXT_LIST_PRICE" as "s_ext_list_price"
  FROM
      "web_sales" as "s_web_sales_unified"),
  questionable as (
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
  abundant as (
  SELECT
      "questionable"."s_item_brand_id" as "_qualifying_combos_brand_id",
      "questionable"."s_item_category_id" as "_qualifying_combos_category_id",
      "questionable"."s_item_class_id" as "_qualifying_combos_class_id"
  FROM
      "questionable"
  GROUP BY
      1,
      2,
      3
  HAVING
      count(distinct "questionable"."s_channel") = 3
  ),
  uneven as (
  SELECT
      "abundant"."_qualifying_combos_brand_id" as "_qualifying_combos_brand_id",
      "abundant"."_qualifying_combos_category_id" as "_qualifying_combos_category_id",
      "abundant"."_qualifying_combos_class_id" as "_qualifying_combos_class_id"
  FROM
      "abundant"),
  yummy as (
  SELECT
      "uneven"."_qualifying_combos_brand_id" as "qualifying_combos_brand_id",
      "uneven"."_qualifying_combos_category_id" as "qualifying_combos_category_id",
      "uneven"."_qualifying_combos_class_id" as "qualifying_combos_class_id"
  FROM
      "uneven"),
  concerned as (
  SELECT
      "yummy"."qualifying_combos_class_id" as "qualifying_combos_class_id"
  FROM
      "yummy"
  GROUP BY
      1),
  vacuous as (
  SELECT
      "yummy"."qualifying_combos_category_id" as "qualifying_combos_category_id"
  FROM
      "yummy"
  GROUP BY
      1),
  juicy as (
  SELECT
      "yummy"."qualifying_combos_brand_id" as "qualifying_combos_brand_id"
  FROM
      "yummy"
  GROUP BY
      1),
  friendly as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "cheerful"."s_order_id" as "s_order_id",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_id" = "s_date_date"."D_DATE_SK"
      INNER JOIN "item" as "s_item_items" on "cheerful"."s_item_id" = "s_item_items"."I_ITEM_SK"
  WHERE
      "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 11 and "s_item_items"."I_BRAND_ID" in (select juicy."qualifying_combos_brand_id" from juicy where juicy."qualifying_combos_brand_id" is not null) and "s_item_items"."I_CLASS_ID" in (select concerned."qualifying_combos_class_id" from concerned where concerned."qualifying_combos_class_id" is not null) and "s_item_items"."I_CATEGORY_ID" in (select vacuous."qualifying_combos_category_id" from vacuous where vacuous."qualifying_combos_category_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  sparkling as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "cheerful"."s_ext_list_price" as "s_ext_list_price",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_id" = "s_date_date"."D_DATE_SK"
      INNER JOIN "item" as "s_item_items" on "cheerful"."s_item_id" = "s_item_items"."I_ITEM_SK"
  WHERE
      "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 11 and "s_item_items"."I_BRAND_ID" in (select juicy."qualifying_combos_brand_id" from juicy where juicy."qualifying_combos_brand_id" is not null) and "s_item_items"."I_CLASS_ID" in (select concerned."qualifying_combos_class_id" from concerned where concerned."qualifying_combos_class_id" is not null) and "s_item_items"."I_CATEGORY_ID" in (select vacuous."qualifying_combos_category_id" from vacuous where vacuous."qualifying_combos_category_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      "cheerful"."s_item_id",
      "cheerful"."s_order_id"),
  young as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "s_date_date"."D_YEAR" as "s_date_year",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_id" = "s_date_date"."D_DATE_SK"
      INNER JOIN "item" as "s_item_items" on "cheerful"."s_item_id" = "s_item_items"."I_ITEM_SK"
  WHERE
      "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 11 and "s_item_items"."I_BRAND_ID" in (select juicy."qualifying_combos_brand_id" from juicy where juicy."qualifying_combos_brand_id" is not null) and "s_item_items"."I_CLASS_ID" in (select concerned."qualifying_combos_class_id" from concerned where concerned."qualifying_combos_class_id" is not null) and "s_item_items"."I_CATEGORY_ID" in (select vacuous."qualifying_combos_category_id" from vacuous where vacuous."qualifying_combos_category_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  kaput as (
  SELECT
      "friendly"."s_channel" as "s_channel",
      "friendly"."s_item_brand_id" as "s_item_brand_id",
      "friendly"."s_item_category_id" as "s_item_category_id",
      "friendly"."s_item_class_id" as "s_item_class_id",
      count("friendly"."s_order_id") as "_filtered_channel_groups_channel_count"
  FROM
      "friendly"
  GROUP BY
      1,
      2,
      3,
      4),
  abhorrent as (
  SELECT
      "sparkling"."s_channel" as "s_channel",
      "sparkling"."s_item_brand_id" as "s_item_brand_id",
      "sparkling"."s_item_category_id" as "s_item_category_id",
      "sparkling"."s_item_class_id" as "s_item_class_id",
      avg("sparkling"."s_ext_list_price") as "_virt_agg_avg_3301353096098894",
      sum("sparkling"."s_ext_list_price") as "_filtered_channel_groups_channel_total"
  FROM
      "sparkling"
  GROUP BY
      1,
      2,
      3,
      4),
  sweltering as (
  SELECT
      "abhorrent"."_virt_agg_avg_3301353096098894" as "_virt_agg_avg_3301353096098894",
      "young"."s_channel" as "s_channel",
      "young"."s_date_year" as "s_date_year",
      "young"."s_item_brand_id" as "s_item_brand_id",
      "young"."s_item_category_id" as "s_item_category_id",
      "young"."s_item_class_id" as "s_item_class_id"
  FROM
      "young"
      INNER JOIN "abhorrent" on "young"."s_channel" = "abhorrent"."s_channel" AND "young"."s_item_brand_id" is not distinct from "abhorrent"."s_item_brand_id" AND "young"."s_item_category_id" is not distinct from "abhorrent"."s_item_category_id" AND "young"."s_item_class_id" is not distinct from "abhorrent"."s_item_class_id"),
  late as (
  SELECT
      "sweltering"."s_channel" as "s_channel",
      "sweltering"."s_item_brand_id" as "s_item_brand_id",
      "sweltering"."s_item_category_id" as "s_item_category_id",
      "sweltering"."s_item_class_id" as "s_item_class_id",
      CASE WHEN "sweltering"."s_date_year" BETWEEN 1999 AND 2001 THEN "sweltering"."_virt_agg_avg_3301353096098894" ELSE NULL END as "overall_avg_sale"
  FROM
      "sweltering"),
  macho as (
  SELECT
      "late"."overall_avg_sale" as "overall_avg_sale",
      "late"."s_channel" as "s_channel",
      "late"."s_item_brand_id" as "s_item_brand_id",
      "late"."s_item_category_id" as "s_item_category_id",
      "late"."s_item_class_id" as "s_item_class_id"
  FROM
      "late"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  scrawny as (
  SELECT
      "abhorrent"."_filtered_channel_groups_channel_total" as "_filtered_channel_groups_channel_total",
      "macho"."overall_avg_sale" as "overall_avg_sale",
      "macho"."s_channel" as "s_channel",
      "macho"."s_item_brand_id" as "s_item_brand_id",
      "macho"."s_item_category_id" as "s_item_category_id",
      "macho"."s_item_class_id" as "s_item_class_id"
  FROM
      "macho"
      INNER JOIN "abhorrent" on "macho"."s_channel" = "abhorrent"."s_channel" AND "macho"."s_item_brand_id" is not distinct from "abhorrent"."s_item_brand_id" AND "macho"."s_item_category_id" is not distinct from "abhorrent"."s_item_category_id" AND "macho"."s_item_class_id" is not distinct from "abhorrent"."s_item_class_id"
  WHERE
      "abhorrent"."_filtered_channel_groups_channel_total" > "macho"."overall_avg_sale"
  ),
  divergent as (
  SELECT
      "scrawny"."_filtered_channel_groups_channel_total" as "_filtered_channel_groups_channel_total",
      "scrawny"."s_channel" as "_filtered_channel_groups_channel",
      "scrawny"."s_item_brand_id" as "_filtered_channel_groups_brand_id",
      "scrawny"."s_item_category_id" as "_filtered_channel_groups_category_id",
      "scrawny"."s_item_class_id" as "_filtered_channel_groups_class_id",
      coalesce("kaput"."_filtered_channel_groups_channel_count",0) as "_filtered_channel_groups_channel_count"
  FROM
      "scrawny"
      INNER JOIN "kaput" on "scrawny"."s_channel" = "kaput"."s_channel" AND "scrawny"."s_item_brand_id" is not distinct from "kaput"."s_item_brand_id" AND "scrawny"."s_item_category_id" is not distinct from "kaput"."s_item_category_id" AND "scrawny"."s_item_class_id" is not distinct from "kaput"."s_item_class_id"
  WHERE
      "scrawny"."_filtered_channel_groups_channel_total" > "scrawny"."overall_avg_sale"
  ),
  busy as (
  SELECT
      "divergent"."_filtered_channel_groups_brand_id" as "filtered_channel_groups_brand_id",
      "divergent"."_filtered_channel_groups_category_id" as "filtered_channel_groups_category_id",
      "divergent"."_filtered_channel_groups_channel" as "filtered_channel_groups_channel",
      "divergent"."_filtered_channel_groups_channel_count" as "filtered_channel_groups_channel_count",
      "divergent"."_filtered_channel_groups_channel_total" as "filtered_channel_groups_channel_total",
      "divergent"."_filtered_channel_groups_class_id" as "filtered_channel_groups_class_id"
  FROM
      "divergent"),
  charming as (
  SELECT
      "busy"."filtered_channel_groups_brand_id" as "filtered_channel_groups_brand_id",
      "busy"."filtered_channel_groups_category_id" as "filtered_channel_groups_category_id",
      "busy"."filtered_channel_groups_channel" as "filtered_channel_groups_channel",
      "busy"."filtered_channel_groups_channel_count" as "filtered_channel_groups_channel_count",
      "busy"."filtered_channel_groups_channel_total" as "filtered_channel_groups_channel_total",
      "busy"."filtered_channel_groups_class_id" as "filtered_channel_groups_class_id"
  FROM
      "busy"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6)
  SELECT
      "charming"."filtered_channel_groups_channel" as "channel",
      "charming"."filtered_channel_groups_brand_id" as "brand_id",
      "charming"."filtered_channel_groups_class_id" as "class_id",
      "charming"."filtered_channel_groups_category_id" as "category_id",
      sum("charming"."filtered_channel_groups_channel_total") as "total_sales",
      sum("charming"."filtered_channel_groups_channel_count") as "total_number_of_sales"
  FROM
      "charming"
  GROUP BY
      ROLLUP (1, 2, 3, 4)
  ORDER BY
      MIN(grouping("charming"."filtered_channel_groups_channel")) asc nulls first,
      MIN(grouping("charming"."filtered_channel_groups_brand_id")) asc nulls first,
      MIN(grouping("charming"."filtered_channel_groups_class_id")) asc nulls first,
      MIN(grouping("charming"."filtered_channel_groups_category_id")) asc nulls first,
      "channel" asc nulls first,
      "brand_id" asc nulls first,
      "class_id" asc nulls first,
      "category_id" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query25.preql`

  ```text
  Resolution error in query25.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs_qualified.customer_id, local.store_sale_net_profit, ss_with_returns.item_code, ss_with_returns.item_description, ss_with_returns.store_code, ss_with_returns.store_name}
  ```
- `trilogy run query26.preql`

  ```text
  Resolution error in query26.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cs.billing_customer_demographic.education_status, cs.billing_customer_demographic.gender, cs.billing_customer_demographic.marital_status, cs.promotion.channel_email, cs.promotion.channel_event, cs.sold_date.year, avg_coupon_amt, avg_list_price, avg_quantity, avg_sales_price}; {item.text_id}.
    - `item.text_id` is disconnected — did you mean `cs.item.text_id`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `cs.item.text_id`) instead of importing a second, disconnected copy.
  ```
- `trilogy file read query27.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query28.preql`

  ```text
  Syntax error in query28.preql: Undefined concept: list_price. Suggestions: ['s.list_price', 's.ext_list_price', 's.sales_price']
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Comparison `sr.return_date.month_of_year <= 12` matches every value of enum field 'sr.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query29.preql`

  ```text
  Resolution error in query29.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs_agg.cs_customer_id, local.store_sale_qty, ss_sr_combined.matched_ss.ss_item_code, ss_sr_combined.matched_ss.ss_item_desc, ss_sr_combined.matched_ss.ss_store_code, ss_sr_combined.matched_ss.ss_store_name}
  ```
- `trilogy run query29.preql`

  ```text
  Resolution error in query29.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_by_cust_item.cat_cust_id, local.store_sale_qty, store_combined.item_code, store_combined.item_desc, store_combined.store_code, store_combined.store_name}
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 2 disconnected subgraphs: {cs.item.current_price, cs.item.id, cs.item.manufacturer_id, current_price, description, item_code}; {inv.date.date, inv.quantity_on_hand}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe item`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query48.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 48 column 3 (char 1933). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query56.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query56.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query56.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query56.preql`

  ```text
  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query56.preql duckdb`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query56.preql duckdb tpcds.duckdb?mode=read_only`

  ```text
  Configuration error: Unknown DuckDB connection parameters: tpcds.duckdb?mode. Valid parameters: path, enable_python_datasources, enable_gcs, enable_spatial, gcs_cache_bust
  ```
- `trilogy run query56.preql duckdb --config trilogy.toml`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query56.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query56.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query56.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query56.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query58.preql`

  ```text
  Syntax error in query58.preql: Undefined concept: st.item_code. Suggestions: ['ct.item_code', 'store_totals.item_code', 'web_totals.item_code', 'catalog_totals.item_code']
  ```
- `trilogy run query59.preql`

  ```text
  Syntax error in query59.preql: Undefined concept: date.day_of_week. Suggestions: ['sales.date.day_of_week', 'sales.store.date.day_of_week', 'sales.return_store.date.day_of_week', 'sales.return_date.day_of_week', 'sales.customer.first_sales_date.day_of_week', 'sales.customer.first_shipto_date.day_of_week']
  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 20). The requested concepts split into 2 disconnected subgraphs: {catalog_item.cat_ext_list_price, catalog_item.cat_refund_amount, cust_city, cust_street_name, cust_street_number, cust_zip, first_sales_year, first_ship_year, item_id, line_count, product_name, sale_city, sale_street_name, sale_street_number, sale_year, sale_zip, store_name, store_zip, total_coupon_amt, total_list_price, total_wholesale_cost, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.is_returned}; {item.color, item.current_price}.
    - `item.color` is disconnected — did you mean `ss.item.color`? (connected to the other concepts)
    - `item.current_price` is disconnected — did you mean `ss.item.current_price`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.item.color`) instead of importing a second, disconnected copy.
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
  ```
- `trilogy file read query65.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query67.preql`

  ```text
  Syntax error in query67.preql: Undefined concept: rank (line 38, col 5, in ORDER BY). Suggestions: ['brand']
  ```
- `trilogy run query69.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query69.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260707-151529\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 162428)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query69.preql`

  ```text
  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy file read query71.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query73.preql`

  ```text
  Syntax error in query73.preql: ORDER BY references 'ss.customer.id', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.id order by ss.customer.id asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 42 column 12 (char 1500). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query76_test.preql`

  ```text
  Syntax error in query76_test.preql: union(...) requires at least two relational arms.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 80 column 3 (char 3076). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query81.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.id, item.manufacturer_id, current_price, item_code, item_description}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 9). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.id, item.manufacturer_id, current_price, item_code, item_description}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_returns:sr --import raw/item:item select item.text_id, sr.item.id, sr.return_quantity, sr.return_date.week_seq limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {item.text_id}; {sr.item.id, sr.return_date.week_seq, sr.return_quantity}.
    - `item.text_id` is disconnected — did you mean `sr.item.text_id`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `sr.item.text_id`) instead of importing a second, disconnected copy.
  ```
- `trilogy run --import raw/web_returns:wr select wr.item.text_id, wr.return_date.week_seq, wr.return_quantity limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: wr.item.text_id (line 2, col 8, in SELECT). Suggestions: ['wr.web_sales.item.text_id', 'wr.time.text_id', 'wr.store.text_id', 'wr.store.date.text_id', 'wr.web_sales.date.text_id', 'wr.web_sales.ship_date.text_id']
  ```
- `trilogy run --import raw/web_returns:wr select wr.web_sales.item.text_id as text_id, sum(wr.return_quantity) as web_qty, count(wr.item.id) as web_rows where wr.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Syntax error in stdin: Undefined concept: wr.item.id (line 2, in SELECT). Suggestions: ['wr.web_sales.item.id', 'wr.time.id', 'wr.store.id', 'wr.store.date.id', 'wr.web_sales.date.id', 'wr.web_sales.ship_date.id']
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query85.preql`

  ```text
  Syntax error in query85.preql: Comparison `wr.web_sales.return_customer.demographics.marital_status = 'Married'` can never match enum field 'wr.web_sales.return_customer.demographics.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query91.preql`

  ```text
  Syntax error in query91.preql: Comparison `cr.billing_customer.demographics.marital_status = 'Married'` can never match enum field 'cr.billing_customer.demographics.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read query91.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store_sales select ss.customer.id where ss.return_reason.desc = 'reason 28' and ss.customer.id is null limit 5;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - ss.customer.id (line 2, col 8, in SELECT); did you mean: store_sales.customer.id, store_sales.customer.text_id, store_sales.customer.login, store_sales.item.id, store_sales.date.id, store_sales.return_date.id?
    - ss.return_reason.desc (line 2, col 29, in WHERE); did you mean: store_sales.return_reason.desc, store_sales.return_reason.id, store_sales.return_reason.text_id, store_sales.item.desc?
    - ss.customer.id (line 2, col 69, in WHERE); did you mean: store_sales.customer.id, store_sales.customer.text_id, store_sales.customer.login, store_sales.item.id, store_sales.date.id, store_sales.return_date.id?
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query94.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query98.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel as ch, count(*) as cnt where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date group by ch;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...lect all_sales.channel as ch, ??? count(*) as cnt where all_sale...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel as ch, count(all_sales.order_id) as cnt where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date group by ch;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...::date and '2000-09-06'::date ??? group by ch;
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel as ch, count(*) as cnt where all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date group by ch;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...lect all_sales.channel as ch, ??? count(*) as cnt where all_sale...
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: Find (brand_id, class_id, category_id) combos present in all 3 channels duri… asc nulls first,
    channel asc nulls first,
    brand_id asc nulls first,
    class_id asc nulls first,
    category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:1
     |
  33 | by rollup (s.channel, s.item.brand_id, s.item.class_id, s.item.category_id)
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...otal_sales > overall_avg_sale ??? by rollup (s.channel, s.item.b...

  Hint: the `by rollup/cube/grouping sets` clause must come *before* HAVING in Trilogy (same order as SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> by rollup (<keys>) having <cond> order by <cols> limit <n>;

  Write stats: received 1624 chars / 1624 bytes; tail: …' asc nulls first,\\n    category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query20.preql --content import raw/catalog_sales as cs;

where cs.item.category in ('Sports', 'Books', 'Home')
  and cs.sold_date.date bet…ion by cs.item.class) as pct_of_class
order by
    cs.item.category,
    cs.item.class,
    cs.item.id,
    cs.item.desc,
    pct_of_class
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  wher...

  Write stats: received 580 chars / 580 bytes; tail: …'   cs.item.id,\\n    cs.item.desc,\\n    pct_of_class\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# ---- Frequent items -…ales.last_name asc nulls first,
    combined_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100
having
    total_sales > 0
;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:26
     |
  14 |     count(ss.item.text_id, substring(ss.item.desc, 1, 30), ss.date.date) > 4
     |                          ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ving     count(ss.item.text_id ??? , substring(ss.item.desc, 1, 3...

  Write stats: received 2667 chars / 2667 bytes; tail: …'sales asc nulls first\\nlimit 100\\nhaving\\n    total_sales > 0\\n;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# ---- Step 1: Frequent…ales.last_name asc nulls first,
    combined_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100
having
    total_sales > 0
;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:68
    |
  8 | auto cnt_items <- sum(ss.row_counter) by ss.item.text_id, substring(ss.item.desc, 1, 30), ss.date.date;
    |                                                                    ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
   by ss.item.text_id, substring ??? (ss.item.desc, 1, 30), ss.date...

  Write stats: received 2477 chars / 2477 bytes; tail: …'sales asc nulls first\\nlimit 100\\nhaving\\n    total_sales > 0\\n;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# ---- Step 1: Frequent…ales.last_name asc nulls first,
    combined_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100
having
    total_sales > 0
;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:57
    |
  8 | auto cnt_items <- sum(ss.row_counter) by ss.item.text_id, (substring(ss.item.desc, 1, 30)), ss.date.date;
    |                                                         ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...ow_counter) by ss.item.text_id ??? , (substring(ss.item.desc, 1,

  Write stats: received 2479 chars / 2479 bytes; tail: …'sales asc nulls first\\nlimit 100\\nhaving\\n    total_sales > 0\\n;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# ---- Step 1: Frequent…ales.last_name asc nulls first,
    combined_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100
having
    total_sales > 0
;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 66:1
     |
  66 | order by
     | ^---
     |
     = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...s.sale_amount) as total_sales ??? order by     combined_sales.la...

  Write stats: received 2431 chars / 2431 bytes; tail: …'sales asc nulls first\\nlimit 100\\nhaving\\n    total_sales > 0\\n;'.
  ```
- `trilogy file write query28.preql --content import raw.store_sales as s;

# Bucket 1: qty 0-5 AND (list_price 8-18 OR coupon_amt 459-1459 OR wholesale_cost 57… bucket6_avg_list_price,
    count(b6_list_price) as bucket6_count_list_price,
    count_distinct(b6_list_price) as bucket6_unique_list_price
by *
;
`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
    --> 40:1
     |
  40 | by *
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   as bucket6_unique_list_price ??? by * ;

  Write stats: received 2694 chars / 2694 bytes; tail: …'distinct(b6_list_price) as bucket6_unique_list_price\\nby *\\n;\\n'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items whose manufacturer_id is between 1 and 500 inclusive
# and whose manufacturer te…ast one other item
# matching any of the eight attribute profiles

select distinct item.product_name as product_name
order by product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ht attribute profiles  select ??? distinct item.product_name as

  Write stats: received 284 chars / 284 bytes; tail: …'roduct_name as product_name\\norder by product_name\\nlimit 100;'.
  ```
- `trilogy run --import raw.web_sales:ws select distinct ws.billing_customer.address.zip where substring(ws.billing_customer.address.zip, 1, 5) in ('85669','86197','88274','83405','86475','85392','85460','80348','81792') limit 5;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...t raw.web_sales as ws; select ??? distinct ws.billing_customer.a...
  ```
- `trilogy file write query47.preql --content import raw.store_sales as ss;

# Compute monthly totals of unit price (sales_price) per (category, brand, store, c….name,
  mt.company_name,
  mt.year,
  mt.month_of_year,
  avg_monthly_sales,
  this_month_total,
  prior_month_total,
  next_month_total
limit 100;
`

  ```text
  refused to write 'query47.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...onth_seq) as next_month_total ??? from monthly_totals as mt wher...

  Write stats: received 1444 chars / 1444 bytes; tail: …'h_total,\\n  prior_month_total,\\n  next_month_total\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/date select date.week_seq where date.date = '2000-01-03'::date select date.week_seq;`

  ```text
  Syntax error in stdin:  --> 2:59
    |
  2 | select date.week_seq where date.date = '2000-01-03'::date select date.week_seq;
    |                                                           ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ate.date = '2000-01-03'::date ??? select date.week_seq;
  ```
- `trilogy file write query66.preql --content # Query 66: Web and catalog sales in 2001
# Time window: seconds 30838-59638, shipped DHL/BARIAN, with warehouse

…les_per_sqft,
    coalesce(sum(combined.net_amt), 0) as monthly_net
order by
    combined.wh_name nulls first,
    combined.yr nulls first
limit 100;`

  ```text
  refused to write 'query66.preql': not syntactically valid Trilogy.

  Parse error:
    --> 48:6
     |
  48 |     (catalog_sales_arm),
     |      ^---
     |
     = expected select_statement
  Location:
   with combined as union(     ( ??? catalog_sales_arm),     (web_s...

  Write stats: received 2047 chars / 2047 bytes; tail: …'.wh_name nulls first,\\n    combined.yr nulls first\\nlimit 100;'.
  ```
- `trilogy file write query69.preql --content import raw.customer as customer;
import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import …ng.gender,
    qualifying.marital_status,
    qualifying.education_status,
    qualifying.purchase_estimate,
    qualifying.credit_rating
limit 100
;`

  ```text
  refused to write 'query69.preql': not syntactically valid Trilogy.

  Parse error:
    --> 45:6
     |
  45 |     (target_customers),
     |      ^---
     |
     = expected select_statement
  Location:
  ...th qualifying as except(     ( ??? target_customers),     (target...

  Write stats: received 1988 chars / 1988 bytes; tail: …'.purchase_estimate,\\n    qualifying.credit_rating\\nlimit 100\\n;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.state, sum(ss.net_profit) as net_profit where ss.date.year = 2000 and ss.store.id is not null group by ss.store.state order by net_profit desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...0 and ss.store.id is not null ??? group by ss.store.state order
  ```
- `trilogy file write query74.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Rowset: per-customer store sales totals by year
with …code as customer_code,
  store_cust.first_name as first_name,
  store_cust.last_name as last_name
order by
  customer_code asc nulls first
limit 100;`

  ```text
  refused to write 'query74.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:1
     |
  26 | where
     | ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...ust_code = web_cust.cust_code ??? where   store_cust.store_2001

  Write stats: received 1141 chars / 1141 bytes; tail: …'ast_name\\norder by\\n  customer_code asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…nulls first,
         combined.year asc nulls first,
         combined.quarter asc nulls first,
         combined.category asc nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
    --> 36:128
     |
  36 | ) -> (channel string, missing_ref string, year int, quarter int, category string, line_count int, total_ext_sales_price numeric)
     |                                                                                                                                ^---
     |
     = expected concept_nullable_modifier or metadata
  Location:
   total_ext_sales_price numeric ??? ) order by combined.channel as...

  Write stats: received 1577 chars / 1577 bytes; tail: …'first,\\n         combined.category asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…nulls first,
         combined.year asc nulls first,
         combined.quarter asc nulls first,
         combined.category asc nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
    --> 36:87
     |
  36 | ) -> (channel, missing_ref, year, quarter, category, line_count, total_ext_sales_price)
     |                                                                                       ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...e_count, total_ext_sales_price ??? ) order by combined.channel as...

  Write stats: received 1536 chars / 1536 bytes; tail: …'first,\\n         combined.category asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…nulls first,
         combined.year asc nulls first,
         combined.quarter asc nulls first,
         combined.category asc nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
    --> 36:87
     |
  36 | ) -> (channel, missing_ref, year, quarter, category, line_count, total_ext_sales_price)
     |                                                                                       ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...e_count, total_ext_sales_price ??? ) order by combined.channel as...

  Write stats: received 1539 chars / 1539 bytes; tail: …'first,\\n         combined.category asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…ng_ref asc nulls first,
         combined.yr asc nulls first,
         combined.qtr asc nulls first,
         combined.cat asc nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
    --> 36:70
     |
  36 | ) -> (channel, missing_ref, yr, qtr, cat, line_count, total_ext_price)
     |                                                                      ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...t, line_count, total_ext_price ??? ) order by combined.channel as...

  Write stats: received 1460 chars / 1460 bytes; tail: …'ulls first,\\n         combined.cat asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query76_test.preql --content import raw.store_sales as ss;

with combined as union(
    (where ss.date.id is not null and ss.store.id is n…s missing_ref,
         ss.date.year as yr,
    )
) -> (channel, missing_ref, yr)
select combined.channel, combined.missing_ref, combined.yr
limit 5;`

  ```text
  refused to write 'query76_test.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
   -> (channel, missing_ref, yr) ??? select combined.channel, comb...

  Write stats: received 327 chars / 327 bytes; tail: …'combined.channel, combined.missing_ref, combined.yr\\nlimit 5;'.
  ```
- `trilogy file write query78.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Store sales: year 200…desc,
    store_agg.ss_sales_price desc,
    other_qty_val asc,
    other_wholesale_val asc,
    other_sales_price_val asc,
    ratio_val
limit 100;
`

  ```text
  refused to write 'query78.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...ombined other-channel qty > 0 ??? def other_qty_val <- coalesce(...

  Write stats: received 2679 chars / 2679 bytes; tail: …'sc,\\n    other_sales_price_val asc,\\n    ratio_val\\nlimit 100;\\n'.
  ```
- `trilogy file write query85.preql`

  ```text
  refused to write 'query85.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/web_returns as wr;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/web_returns as wr;

   # Fi...

  Write stats: received 2763 chars / 2763 bytes; tail: …'avg_quantity,\r\\n  avg_refunded_cash,\r\\n  avg_fee\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query92.preql --content import raw/web_sales as ws;

# Filter to items from manufacturer 350
auto man_350 <- ws.item.manufacturer_id = 350…t ? (man_350 and in_window and ws.ext_discount_amount > 1.3 * avg_discount_by_item)) as total_ext_discount
order by
    total_ext_discount
limit 100;`

  ```text
  refused to write 'query92.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/web_sales as ws;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/web_sales as ws;  # Filter...

  Write stats: received 646 chars / 646 bytes; tail: …'otal_ext_discount\\norder by\\n    total_ext_discount\\nlimit 100;'.
  ```
- `trilogy file write query98.preql --content import raw.store_sales as store_sales;

where
  store_sales.item.category in {'Sports', 'Books', 'Home'}
  and sto… by
  category asc nulls first,
  class asc nulls first,
  item_code asc nulls first,
  description asc nulls first,
  pct_of_class asc nulls first
;`

  ```text
  refused to write 'query98.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:33
    |
  4 |   store_sales.item.category in {'Sports', 'Books', 'Home'}
    |                                 ^---
    |
    = expected null_lit or MULTILINE_STRING
  Location:
  ...store_sales.item.category in { ??? 'Sports', 'Books', 'Home'}   a...

  Write stats: received 761 chars / 761 bytes; tail: …'escription asc nulls first,\\n  pct_of_class asc nulls first\\n;'.
  ```

### `cli-misuse`

- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run query27.preql`

  ```text
  Resolution error in query27.preql: Could not resolve connections for query with output ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>', 'local.state<Purpose.PROPERTY>Derivation.BASIC>', 'local.group_indicator<Purpose.METRIC>Derivation.BASIC>', 'local.avg_ticket_quantity<Purpose.METRIC>Derivation.AGGREGATE>', 'local.avg_list_price<Purpose.METRIC>Derivation.AGGREGATE>', 'local.avg_coupon_amt<Purpose.METRIC>Derivation.AGGREGATE>', 'local.avg_unit_price<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `type-error`

- `trilogy run query32.preql`

  ```text
  Syntax error in query32.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
