# Trilogy failure analysis — 20260708-030809

- Run `20260708-030808_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1273 | failed: 100 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 62 | 62% |
| `syntax-parse` | 26 | 26% |
| `cli-misuse` | 7 | 7% |
| `syntax-missing-alias` | 3 | 3% |
| `undefined-concept` | 1 | 1% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 2 disconnected subgraphs: {cat_avg_price}; {qualifying_line_items, state, ss.customer.address.id, ss.customer.address.state, ss.date.month_of_year, ss.date.year, ss.item.category}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {cat_avg_price}; {qualifying_line_items, state, ss.customer.address.id, ss.customer.address.state, ss.date.month_of_year, ss.date.year, ss.item.category}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: Undefined concept: i.
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: Undefined concept: item. Suggestions: ['item.id', 'item.size', 'item.desc']
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 14). The requested concepts split into 2 disconnected subgraphs: {cat_avg}; {qualifying_line_items, state, ss.customer.address.id, ss.customer.address.state, ss.date.month_of_year, ss.date.year, ss.item.category}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query06_debug2.preql`

  ```text
  Unexpected error in query06_debug2.preql: (_duckdb.BinderException) Binder Error: Referenced table "ss_item_items" not found!
  Candidate tables: "item_items"

  LINE 50:     coalesce("item_items"."I_ITEM_SK","ss_item_items"."id","thoughtful"."ss_item_id")),
                                                 ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      coalesce("ss_customer_customers"."C_CUSTOMER_SK","ss_store_sales"."SS_CUSTOMER_SK") as "ss_customer_id",
      coalesce("ss_date_date"."D_DATE_SK","ss_store_sales"."SS_SOLD_DATE_SK") as "ss_date_id",
      coalesce("ss_item_items"."I_ITEM_SK","ss_store_sales"."SS_ITEM_SK") as "ss_item_id"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001 and "ss_date_date"."D_MOY" = 1 and "ss_customer_customers"."C_CURRENT_ADDR_SK" is not null and "ss_item_items"."I_CATEGORY" is not null

  GROUP BY
      1,
      2,
      3),
  uneven as (
  SELECT
      "item_items"."I_CATEGORY" as "item_category",
      "ss_item_items"."I_CATEGORY" as "ss_item_category",
      "ss_item_items"."I_CURRENT_PRICE" as "ss_item_current_price",
      coalesce("item_items"."I_ITEM_SK","ss_item_items"."I_ITEM_SK","thoughtful"."ss_item_id") as "ss_item_id"
  FROM
      "thoughtful"
      INNER JOIN "date_dim" as "ss_date_date" on "thoughtful"."ss_date_id" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "ss_customer_customers" on "thoughtful"."ss_customer_id" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "item" as "ss_item_items" on "thoughtful"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "item" as "item_items" on "thoughtful"."ss_item_id" = "item_items"."I_ITEM_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001 and "ss_date_date"."D_MOY" = 1 and "ss_item_items"."I_CATEGORY" is not null and "ss_customer_customers"."C_CURRENT_ADDR_SK" is not null

  GROUP BY
      1,
      2,
      3,
      4),
  questionable as (
  SELECT
      "item_items"."I_CATEGORY" as "item_category",
      "item_items"."I_CURRENT_PRICE" as "item_current_price"
  FROM
      "thoughtful"
      LEFT OUTER JOIN "item" as "item_items" on "thoughtful"."ss_item_id" = "item_items"."I_ITEM_SK"
  GROUP BY
      1,
      2,
      coalesce("item_items"."I_ITEM_SK","ss_item_items"."id","thoughtful"."ss_item_id")),
  abundant as (
  SELECT
      "questionable"."item_category" as "item_category",
      avg("questionable"."item_current_price") as "cat_avg_price"
  FROM
      "questionable"
  GROUP BY
      1)
  SELECT
      "uneven"."ss_item_id" as "ss_item_id",
      "uneven"."ss_item_category" as "ss_item_category",
      "uneven"."ss_item_current_price" as "ss_item_current_price",
      "abundant"."cat_avg_price" as "cat_avg_price",
      "uneven"."ss_item_current_price" / "abundant"."cat_avg_price" as "ratio"
  FROM
      "uneven"
      INNER JOIN "abundant" on "uneven"."item_category" is not distinct from "abundant"."item_category"
  ORDER BY
      "ratio" desc
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: Undefined concept: item. Suggestions: ['item.id', 'item.size', 'item.desc']
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query07.preql`

  ```text
  Syntax error in query07.preql: Comparison `ss.customer_demographic.gender = 'Male'` can never match enum field 'ss.customer_demographic.gender', which contains only these values: 'M', 'F'. It is always false and should be removed.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 61 column 3 (char 1755). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 37). The requested concepts split into 2 disconnected subgraphs: {customer_id, store_2001.revenue, store_2002.revenue, web_2001.revenue, web_2002.revenue}; {first_name, last_name, preferred_cust_flag}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(local.channel)` (line 18), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel) as g order by g desc`.
  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: Could not render the query: Missing source reference to store.item.desc; Missing source reference to store.date._date_string. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  thoughtful as (
  SELECT
      "store_date_date"."D_DATE_SK" as "store_date_id"
  FROM
      "date_dim" as "store_date_date"
  WHERE
      "store_date_date"."D_YEAR" BETWEEN 2000 AND 2003
  ),
  concerned as (
  SELECT
      sum("store_store_sales"."SS_QUANTITY" * CASE WHEN "store_date_date"."D_YEAR" BETWEEN 2000 AND 2003 THEN "store_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_virt_filter_4022858797657622"
  FROM
      "store_sales" as "store_store_sales"
      LEFT OUTER JOIN "date_dim" as "store_date_date" on "store_store_sales"."SS_SOLD_DATE_SK" = "store_date_date"."D_DATE_SK"
  WHERE
      "store_store_sales"."SS_CUSTOMER_SK" is not null
  ),
  late as (
  SELECT
      "store_store_sales"."SS_CUSTOMER_SK" as "_best_customers_customer_id",
      sum("store_store_sales"."SS_QUANTITY" * "store_store_sales"."SS_SALES_PRICE") as "customer_alltime_alltime_total"
  FROM
      "store_sales" as "store_store_sales"
  WHERE
      "store_store_sales"."SS_CUSTOMER_SK" is not null

  GROUP BY
      1),
  abundant as (
  SELECT
      "store_item_items"."I_ITEM_ID" as "_frequent_items_item_id"
  FROM
      "store_sales" as "store_store_sales"
      INNER JOIN "thoughtful" on "store_store_sales"."SS_SOLD_DATE_SK" = "thoughtful"."store_date_id"
      INNER JOIN "item" as "store_item_items" on "store_store_sales"."SS_ITEM_SK" = "store_item_items"."I_ITEM_SK"
  GROUP BY
      1),
  abhorrent as (
  SELECT
      max("concerned"."_virt_filter_4022858797657622") as "_max_short_max_val"
  FROM
      "concerned"),
  yummy as (
  SELECT
      "abundant"."_frequent_items_item_id" as "_frequent_items_item_id"
  FROM
      "abundant"
  WHERE
      CASE WHEN (((("abundant"."_frequent_items_item_id" || '-') || SUBSTRING(INVALID_REFERENCE_BUG<Missing source reference to store.item.desc>,1,30)) || '-') || cast(cast(INVALID_REFERENCE_BUG<Missing source reference to store.date._date_string> as date) as string)) IS NOT NULL THEN 1 ELSE 0 END > 4
  ),
  sweltering as (
  SELECT
      "abhorrent"."_max_short_max_val" as "max_short_max_val"
  FROM
      "abhorrent"),
  juicy as (
  SELECT
      "yummy"."_frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "yummy"),
  friendly as (
  SELECT
      "late"."_best_customers_customer_id" as "best_customers_customer_id"
  FROM
      "late"
      INNER JOIN "sweltering" on 1=1
  WHERE
      "late"."customer_alltime_alltime_total" > 0.5 * "sweltering"."max_short_max_val"

  GROUP BY
      1),
  puffy as (
  SELECT
      "web_billing_customer_customers"."C_FIRST_NAME" as "web_billing_customer_first_name",
      "web_billing_customer_customers"."C_LAST_NAME" as "web_billing_customer_last_name",
      "web_web_sales"."WS_LIST_PRICE" as "web_list_price",
      "web_web_sales"."WS_QUANTITY" as "web_quantity"
  FROM
      "web_sales" as "web_web_sales"
      INNER JOIN "date_dim" as "web_date_date" on "web_web_sales"."WS_SOLD_DATE_SK" = "web_date_date"."D_DATE_SK"
      INNER JOIN "item" as "web_item_items" on "web_web_sales"."WS_ITEM_SK" = "web_item_items"."I_ITEM_SK"
      INNER JOIN "customer" as "web_billing_customer_customers" on "web_web_sales"."WS_BILL_CUSTOMER_SK" = "web_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "web_date_date"."D_YEAR" = 2000 and "web_date_date"."D_MOY" = 2 and "web_web_sales"."WS_BILL_CUSTOMER_SK" is not null and "web_web_sales"."WS_BILL_CUSTOMER_SK" in (select friendly."best_customers_customer_id" from friendly where friendly."best_customers_customer_id" is not null) and "web_item_items"."I_ITEM_ID" in (select juicy."frequent_items_item_id" from juicy where juicy."frequent_items_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "web_item_items"."I_ITEM_SK",
      "web_web_sales"."WS_ORDER_NUMBER"),
  busy as (
  SELECT
      "cat_billing_customer_customers"."C_FIRST_NAME" as "cat_billing_customer_first_name",
      "cat_billing_customer_customers"."C_LAST_NAME" as "cat_billing_customer_last_name",
      "cat_catalog_sales"."CS_LIST_PRICE" as "cat_list_price",
      "cat_catalog_sales"."CS_QUANTITY" as "cat_quantity"
  FROM
      "catalog_sales" as "cat_catalog_sales"
      INNER JOIN "date_dim" as "cat_date_date" on "cat_catalog_sales"."CS_SOLD_DATE_SK" = "cat_date_date"."D_DATE_SK"
      INNER JOIN "item" as "cat_item_items" on "cat_catalog_sales"."CS_ITEM_SK" = "cat_item_items"."I_ITEM_SK"
      INNER JOIN "customer" as "cat_billing_customer_customers" on "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cat_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cat_date_date"."D_YEAR" = 2000 and "cat_date_date"."D_MOY" = 2 and "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" is not null and "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select friendly."best_customers_customer_id" from friendly where friendly."best_customers_customer_id" is not null) and "cat_item_items"."I_ITEM_ID" in (select juicy."frequent_items_item_id" from juicy where juicy."frequent_items_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cat_catalog_sales"."CS_ITEM_SK",
      "cat_catalog_sales"."CS_ORDER_NUMBER"),
  hard as (
  SELECT
      "puffy"."web_billing_customer_first_name" as "___tvf_arm_1_first_name",
      "puffy"."web_billing_customer_last_name" as "___tvf_arm_1_last_name",
      sum("puffy"."web_quantity" * "puffy"."web_list_price") as "___tvf_arm_1_total_sales"
  FROM
      "puffy"
  GROUP BY
      1,
      2),
  charming as (
  SELECT
      "busy"."cat_billing_customer_first_name" as "___tvf_arm_0_first_name",
      "busy"."cat_billing_customer_last_name" as "___tvf_arm_0_last_name",
      sum("busy"."cat_quantity" * "busy"."cat_list_price") as "___tvf_arm_0_total_sales"
  FROM
      "busy"
  GROUP BY
      1,
      2),
  yellow as (
  SELECT
      "charming"."___tvf_arm_0_last_name" as "_combined_last_name",
      "charming"."___tvf_arm_0_first_name" as "_combined_first_name",
      "charming"."___tvf_arm_0_total_sales" as "_combined_total_sales"
  FROM
      "charming"
  UNION ALL
  SELECT
      "hard"."___tvf_arm_1_last_name" as "_combined_last_name",
      "hard"."___tvf_arm_1_first_name" as "_combined_first_name",
      "hard"."___tvf_arm_1_total_sales" as "_combined_total_sales"
  FROM
      "hard"),
  resonant as (
  SELECT
      "yellow"."_combined_first_name" as "_combined_first_name",
      "yellow"."_combined_last_name" as "_combined_last_name",
      "yellow"."_combined_total_sales" as "_combined_total_sales"
  FROM
      "yellow")
  SELECT
      "resonant"."_combined_last_name" as "combined_last_name",
      "resonant"."_combined_first_name" as "combined_first_name",
      "resonant"."_combined_total_sales" as "combined_total_sales"
  FROM
      "resonant"
  WHERE
      "resonant"."_combined_total_sales" > 0

  ORDER BY
      coalesce("resonant"."_combined_last_name",'') asc,
      coalesce("resonant"."_combined_first_name",'') asc,
      "combined_total_sales" asc nulls first
  LIMIT (100)
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: Could not render the query: Missing source reference to store.item.desc; Missing source reference to store.date._date_string. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  abhorrent as (
  SELECT
      sum("store_store_sales"."SS_QUANTITY" * CASE WHEN "store_date_date"."D_YEAR" BETWEEN 2000 AND 2003 THEN "store_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_virt_filter_4022858797657622"
  FROM
      "store_sales" as "store_store_sales"
      LEFT OUTER JOIN "date_dim" as "store_date_date" on "store_store_sales"."SS_SOLD_DATE_SK" = "store_date_date"."D_DATE_SK"
  WHERE
      "store_store_sales"."SS_CUSTOMER_SK" is not null
  ),
  abundant as (
  SELECT
      "store_store_sales"."SS_ITEM_SK" as "store_item_id",
      "store_store_sales"."SS_SOLD_DATE_SK" as "store_date_id"
  FROM
      "store_sales" as "store_store_sales"
  GROUP BY
      1,
      2),
  friendly as (
  SELECT
      "store_store_sales"."SS_CUSTOMER_SK" as "_best_customers_customer_id",
      sum("store_store_sales"."SS_QUANTITY" * "store_store_sales"."SS_SALES_PRICE") as "customer_alltime_alltime_total"
  FROM
      "store_sales" as "store_store_sales"
  WHERE
      "store_store_sales"."SS_CUSTOMER_SK" is not null

  GROUP BY
      1),
  macho as (
  SELECT
      max("abhorrent"."_virt_filter_4022858797657622") as "_max_short_max_val"
  FROM
      "abhorrent"),
  uneven as (
  SELECT
      "store_item_items"."I_ITEM_ID" as "store_item_text_id",
      SUBSTRING("store_item_items"."I_ITEM_DESC",1,30) as "_triple_counts_desc_prefix",
      cast("store_date_date"."D_DATE" as date) as "store_date_date"
  FROM
      "abundant"
      INNER JOIN "date_dim" as "store_date_date" on "abundant"."store_date_id" = "store_date_date"."D_DATE_SK"
      INNER JOIN "item" as "store_item_items" on "abundant"."store_item_id" = "store_item_items"."I_ITEM_SK"
  WHERE
      "store_date_date"."D_YEAR" BETWEEN 2000 AND 2003

  GROUP BY
      1,
      2,
      3,
      "store_item_items"."I_ITEM_SK"),
  scrawny as (
  SELECT
      "macho"."_max_short_max_val" as "max_short_max_val"
  FROM
      "macho"),
  yummy as (
  SELECT
      "uneven"."store_item_text_id" as "_triple_counts_item_id"
  FROM
      "uneven"
  GROUP BY
      1,
      "uneven"."_triple_counts_desc_prefix",
      "uneven"."store_date_date"),
  busy as (
  SELECT
      "friendly"."_best_customers_customer_id" as "best_customers_customer_id"
  FROM
      "friendly"
      INNER JOIN "scrawny" on 1=1
  WHERE
      "friendly"."customer_alltime_alltime_total" > 0.5 * "scrawny"."max_short_max_val"

  GROUP BY
      1),
  juicy as (
  SELECT
      "yummy"."_triple_counts_item_id" as "_frequent_items_item_id"
  FROM
      "yummy"
  GROUP BY
      1),
  concerned as (
  SELECT
      "juicy"."_frequent_items_item_id" as "_frequent_items_item_id"
  FROM
      "juicy"
  WHERE
      CASE WHEN (((("juicy"."_frequent_items_item_id" || '-') || SUBSTRING(INVALID_REFERENCE_BUG<Missing source reference to store.item.desc>,1,30)) || '-') || cast(cast(INVALID_REFERENCE_BUG<Missing source reference to store.date._date_string> as date) as string)) IS NOT NULL THEN 1 ELSE 0 END > 4
  ),
  young as (
  SELECT
      "concerned"."_frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "concerned"),
  sparkling as (
  SELECT
      "young"."frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "young"
  GROUP BY
      1),
  yellow as (
  SELECT
      "web_billing_customer_customers"."C_FIRST_NAME" as "web_billing_customer_first_name",
      "web_billing_customer_customers"."C_LAST_NAME" as "web_billing_customer_last_name",
      "web_web_sales"."WS_LIST_PRICE" as "web_list_price",
      "web_web_sales"."WS_QUANTITY" as "web_quantity"
  FROM
      "web_sales" as "web_web_sales"
      INNER JOIN "date_dim" as "web_date_date" on "web_web_sales"."WS_SOLD_DATE_SK" = "web_date_date"."D_DATE_SK"
      INNER JOIN "item" as "web_item_items" on "web_web_sales"."WS_ITEM_SK" = "web_item_items"."I_ITEM_SK"
      INNER JOIN "customer" as "web_billing_customer_customers" on "web_web_sales"."WS_BILL_CUSTOMER_SK" = "web_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "web_date_date"."D_YEAR" = 2000 and "web_date_date"."D_MOY" = 2 and "web_web_sales"."WS_BILL_CUSTOMER_SK" is not null and "web_web_sales"."WS_BILL_CUSTOMER_SK" in (select busy."best_customers_customer_id" from busy where busy."best_customers_customer_id" is not null) and "web_item_items"."I_ITEM_ID" in (select sparkling."frequent_items_item_id" from sparkling where sparkling."frequent_items_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "web_item_items"."I_ITEM_SK",
      "web_web_sales"."WS_ORDER_NUMBER"),
  premium as (
  SELECT
      "cat_billing_customer_customers"."C_FIRST_NAME" as "cat_billing_customer_first_name",
      "cat_billing_customer_customers"."C_LAST_NAME" as "cat_billing_customer_last_name",
      "cat_catalog_sales"."CS_LIST_PRICE" as "cat_list_price",
      "cat_catalog_sales"."CS_QUANTITY" as "cat_quantity"
  FROM
      "catalog_sales" as "cat_catalog_sales"
      INNER JOIN "date_dim" as "cat_date_date" on "cat_catalog_sales"."CS_SOLD_DATE_SK" = "cat_date_date"."D_DATE_SK"
      INNER JOIN "item" as "cat_item_items" on "cat_catalog_sales"."CS_ITEM_SK" = "cat_item_items"."I_ITEM_SK"
      INNER JOIN "customer" as "cat_billing_customer_customers" on "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cat_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cat_date_date"."D_YEAR" = 2000 and "cat_date_date"."D_MOY" = 2 and "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" is not null and "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select busy."best_customers_customer_id" from busy where busy."best_customers_customer_id" is not null) and "cat_item_items"."I_ITEM_ID" in (select sparkling."frequent_items_item_id" from sparkling where sparkling."frequent_items_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cat_catalog_sales"."CS_ITEM_SK",
      "cat_catalog_sales"."CS_ORDER_NUMBER"),
  resonant as (
  SELECT
      "yellow"."web_billing_customer_first_name" as "___tvf_arm_1_first_name",
      "yellow"."web_billing_customer_last_name" as "___tvf_arm_1_last_name",
      sum("yellow"."web_quantity" * "yellow"."web_list_price") as "___tvf_arm_1_total_sales"
  FROM
      "yellow"
  GROUP BY
      1,
      2),
  puzzled as (
  SELECT
      "premium"."cat_billing_customer_first_name" as "___tvf_arm_0_first_name",
      "premium"."cat_billing_customer_last_name" as "___tvf_arm_0_last_name",
      sum("premium"."cat_quantity" * "premium"."cat_list_price") as "___tvf_arm_0_total_sales"
  FROM
      "premium"
  GROUP BY
      1,
      2),
  courageous as (
  SELECT
      "puzzled"."___tvf_arm_0_last_name" as "_combined_last_name",
      "puzzled"."___tvf_arm_0_first_name" as "_combined_first_name",
      "puzzled"."___tvf_arm_0_total_sales" as "_combined_total_sales"
  FROM
      "puzzled"
  UNION ALL
  SELECT
      "resonant"."___tvf_arm_1_last_name" as "_combined_last_name",
      "resonant"."___tvf_arm_1_first_name" as "_combined_first_name",
      "resonant"."___tvf_arm_1_total_sales" as "_combined_total_sales"
  FROM
      "resonant"),
  vast as (
  SELECT
      "courageous"."_combined_first_name" as "_combined_first_name",
      "courageous"."_combined_last_name" as "_combined_last_name",
      "courageous"."_combined_total_sales" as "_combined_total_sales"
  FROM
      "courageous")
  SELECT
      "vast"."_combined_last_name" as "combined_last_name",
      "vast"."_combined_first_name" as "combined_first_name",
      "vast"."_combined_total_sales" as "combined_total_sales"
  FROM
      "vast"
  WHERE
      "vast"."_combined_total_sales" > 0

  ORDER BY
      coalesce("vast"."_combined_last_name",'') asc,
      coalesce("vast"."_combined_first_name",'') asc,
      "combined_total_sales" asc nulls first
  LIMIT (100)
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: Could not render the query: Missing source reference to store.item.desc; Missing source reference to store.date._date_string. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  sparkling as (
  SELECT
      sum("store_store_sales"."SS_QUANTITY" * CASE WHEN "store_date_date"."D_YEAR" BETWEEN 2000 AND 2003 THEN "store_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_virt_filter_4022858797657622"
  FROM
      "store_sales" as "store_store_sales"
      LEFT OUTER JOIN "date_dim" as "store_date_date" on "store_store_sales"."SS_SOLD_DATE_SK" = "store_date_date"."D_DATE_SK"
  WHERE
      "store_store_sales"."SS_CUSTOMER_SK" is not null
  ),
  abundant as (
  SELECT
      "store_store_sales"."SS_ITEM_SK" as "store_item_id",
      "store_store_sales"."SS_SOLD_DATE_SK" as "store_date_id"
  FROM
      "store_sales" as "store_store_sales"
  GROUP BY
      1,
      2),
  scrawny as (
  SELECT
      "store_store_sales"."SS_CUSTOMER_SK" as "_best_customers_customer_id",
      sum("store_store_sales"."SS_QUANTITY" * "store_store_sales"."SS_SALES_PRICE") as "customer_alltime_alltime_total"
  FROM
      "store_sales" as "store_store_sales"
  WHERE
      "store_store_sales"."SS_CUSTOMER_SK" is not null

  GROUP BY
      1),
  late as (
  SELECT
      max("sparkling"."_virt_filter_4022858797657622") as "_max_short_max_val"
  FROM
      "sparkling"),
  uneven as (
  SELECT
      "store_item_items"."I_ITEM_ID" as "_triple_base_item_id"
  FROM
      "abundant"
      INNER JOIN "date_dim" as "store_date_date" on "abundant"."store_date_id" = "store_date_date"."D_DATE_SK"
      INNER JOIN "item" as "store_item_items" on "abundant"."store_item_id" = "store_item_items"."I_ITEM_SK"
  WHERE
      "store_date_date"."D_YEAR" BETWEEN 2000 AND 2003

  GROUP BY
      1,
      "store_item_items"."I_ITEM_DESC",
      SUBSTRING("store_item_items"."I_ITEM_DESC",1,30),
      cast("store_date_date"."D_DATE" as date)),
  macho as (
  SELECT
      "late"."_max_short_max_val" as "max_short_max_val"
  FROM
      "late"),
  yummy as (
  SELECT
      "uneven"."_triple_base_item_id" as "_frequent_items_item_id"
  FROM
      "uneven"
  GROUP BY
      1),
  divergent as (
  SELECT
      "scrawny"."_best_customers_customer_id" as "best_customers_customer_id"
  FROM
      "scrawny"
      INNER JOIN "macho" on 1=1
  WHERE
      "scrawny"."customer_alltime_alltime_total" > 0.5 * "macho"."max_short_max_val"

  GROUP BY
      1),
  vacuous as (
  SELECT
      "yummy"."_frequent_items_item_id" as "_frequent_items_item_id"
  FROM
      "yummy"
  WHERE
      CASE WHEN (((("yummy"."_frequent_items_item_id" || '-') || SUBSTRING(INVALID_REFERENCE_BUG<Missing source reference to store.item.desc>,1,30)) || '-') || cast(cast(INVALID_REFERENCE_BUG<Missing source reference to store.date._date_string> as date) as string)) IS NOT NULL THEN 1 ELSE 0 END > 4
  ),
  concerned as (
  SELECT
      "vacuous"."_frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "vacuous"),
  young as (
  SELECT
      "concerned"."frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "concerned"
  GROUP BY
      1),
  sedate as (
  SELECT
      "web_billing_customer_customers"."C_FIRST_NAME" as "web_billing_customer_first_name",
      "web_billing_customer_customers"."C_LAST_NAME" as "web_billing_customer_last_name",
      "web_web_sales"."WS_LIST_PRICE" as "web_list_price",
      "web_web_sales"."WS_QUANTITY" as "web_quantity"
  FROM
      "web_sales" as "web_web_sales"
      INNER JOIN "date_dim" as "web_date_date" on "web_web_sales"."WS_SOLD_DATE_SK" = "web_date_date"."D_DATE_SK"
      INNER JOIN "item" as "web_item_items" on "web_web_sales"."WS_ITEM_SK" = "web_item_items"."I_ITEM_SK"
      INNER JOIN "customer" as "web_billing_customer_customers" on "web_web_sales"."WS_BILL_CUSTOMER_SK" = "web_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "web_date_date"."D_YEAR" = 2000 and "web_date_date"."D_MOY" = 2 and "web_web_sales"."WS_BILL_CUSTOMER_SK" is not null and "web_web_sales"."WS_BILL_CUSTOMER_SK" in (select divergent."best_customers_customer_id" from divergent where divergent."best_customers_customer_id" is not null) and "web_item_items"."I_ITEM_ID" in (select young."frequent_items_item_id" from young where young."frequent_items_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "web_item_items"."I_ITEM_SK",
      "web_web_sales"."WS_ORDER_NUMBER"),
  protective as (
  SELECT
      "cat_billing_customer_customers"."C_FIRST_NAME" as "cat_billing_customer_first_name",
      "cat_billing_customer_customers"."C_LAST_NAME" as "cat_billing_customer_last_name",
      "cat_catalog_sales"."CS_LIST_PRICE" as "cat_list_price",
      "cat_catalog_sales"."CS_QUANTITY" as "cat_quantity"
  FROM
      "catalog_sales" as "cat_catalog_sales"
      INNER JOIN "date_dim" as "cat_date_date" on "cat_catalog_sales"."CS_SOLD_DATE_SK" = "cat_date_date"."D_DATE_SK"
      INNER JOIN "item" as "cat_item_items" on "cat_catalog_sales"."CS_ITEM_SK" = "cat_item_items"."I_ITEM_SK"
      INNER JOIN "customer" as "cat_billing_customer_customers" on "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cat_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cat_date_date"."D_YEAR" = 2000 and "cat_date_date"."D_MOY" = 2 and "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" is not null and "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select divergent."best_customers_customer_id" from divergent where divergent."best_customers_customer_id" is not null) and "cat_item_items"."I_ITEM_ID" in (select young."frequent_items_item_id" from young where young."frequent_items_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cat_catalog_sales"."CS_ITEM_SK",
      "cat_catalog_sales"."CS_ORDER_NUMBER"),
  yellow as (
  SELECT
      "sedate"."web_billing_customer_first_name" as "___tvf_arm_1_first_name",
      "sedate"."web_billing_customer_last_name" as "___tvf_arm_1_last_name",
      sum("sedate"."web_quantity" * "sedate"."web_list_price") as "___tvf_arm_1_total_sales"
  FROM
      "sedate"
  GROUP BY
      1,
      2),
  premium as (
  SELECT
      "protective"."cat_billing_customer_first_name" as "___tvf_arm_0_first_name",
      "protective"."cat_billing_customer_last_name" as "___tvf_arm_0_last_name",
      sum("protective"."cat_quantity" * "protective"."cat_list_price") as "___tvf_arm_0_total_sales"
  FROM
      "protective"
  GROUP BY
      1,
      2),
  dapper as (
  SELECT
      "premium"."___tvf_arm_0_last_name" as "_combined_last_name",
      "premium"."___tvf_arm_0_first_name" as "_combined_first_name",
      "premium"."___tvf_arm_0_total_sales" as "_combined_total_sales"
  FROM
      "premium"
  UNION ALL
  SELECT
      "yellow"."___tvf_arm_1_last_name" as "_combined_last_name",
      "yellow"."___tvf_arm_1_first_name" as "_combined_first_name",
      "yellow"."___tvf_arm_1_total_sales" as "_combined_total_sales"
  FROM
      "yellow"),
  courageous as (
  SELECT
      "dapper"."_combined_first_name" as "_combined_first_name",
      "dapper"."_combined_last_name" as "_combined_last_name",
      "dapper"."_combined_total_sales" as "_combined_total_sales"
  FROM
      "dapper")
  SELECT
      "courageous"."_combined_last_name" as "combined_last_name",
      "courageous"."_combined_first_name" as "combined_first_name",
      "courageous"."_combined_total_sales" as "combined_total_sales"
  FROM
      "courageous"
  WHERE
      "courageous"."_combined_total_sales" > 0

  ORDER BY
      coalesce("courageous"."_combined_last_name",'') asc,
      coalesce("courageous"."_combined_first_name",'') asc,
      "combined_total_sales" asc nulls first
  LIMIT (100)
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: Could not render the query: Missing source reference to store.item.desc; Missing source reference to store.date._date_string. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  thoughtful as (
  SELECT
      "store_date_date"."D_DATE_SK" as "store_date_id"
  FROM
      "date_dim" as "store_date_date"
  WHERE
      "store_date_date"."D_YEAR" BETWEEN 2000 AND 2003
  ),
  concerned as (
  SELECT
      sum("store_store_sales"."SS_QUANTITY" * CASE WHEN "store_date_date"."D_YEAR" BETWEEN 2000 AND 2003 THEN "store_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_virt_filter_4022858797657622"
  FROM
      "store_sales" as "store_store_sales"
      LEFT OUTER JOIN "date_dim" as "store_date_date" on "store_store_sales"."SS_SOLD_DATE_SK" = "store_date_date"."D_DATE_SK"
  WHERE
      "store_store_sales"."SS_CUSTOMER_SK" is not null
  ),
  late as (
  SELECT
      "store_store_sales"."SS_CUSTOMER_SK" as "store_customer_id",
      sum("store_store_sales"."SS_QUANTITY" * "store_store_sales"."SS_SALES_PRICE") as "_virt_agg_sum_4119990290395352"
  FROM
      "store_sales" as "store_store_sales"
  WHERE
      "store_store_sales"."SS_CUSTOMER_SK" is not null

  GROUP BY
      1),
  abundant as (
  SELECT
      "store_item_items"."I_ITEM_ID" as "_frequent_items_item_id"
  FROM
      "store_sales" as "store_store_sales"
      INNER JOIN "thoughtful" on "store_store_sales"."SS_SOLD_DATE_SK" = "thoughtful"."store_date_id"
      INNER JOIN "item" as "store_item_items" on "store_store_sales"."SS_ITEM_SK" = "store_item_items"."I_ITEM_SK"
  GROUP BY
      1),
  abhorrent as (
  SELECT
      max("concerned"."_virt_filter_4022858797657622") as "_max_short_val_max_val"
  FROM
      "concerned"),
  yummy as (
  SELECT
      "abundant"."_frequent_items_item_id" as "_frequent_items_item_id"
  FROM
      "abundant"
  WHERE
      CASE WHEN (((("abundant"."_frequent_items_item_id" || '-') || SUBSTRING(INVALID_REFERENCE_BUG<Missing source reference to store.item.desc>,1,30)) || '-') || cast(cast(INVALID_REFERENCE_BUG<Missing source reference to store.date._date_string> as date) as string)) IS NOT NULL THEN 1 ELSE 0 END > 4
  ),
  sweltering as (
  SELECT
      "abhorrent"."_max_short_val_max_val" as "max_short_val_max_val"
  FROM
      "abhorrent"),
  juicy as (
  SELECT
      "yummy"."_frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "yummy"),
  macho as (
  SELECT
      "late"."store_customer_id" as "_best_customers_customer_id"
  FROM
      "late"
      INNER JOIN "sweltering" on 1=1
  WHERE
      "late"."_virt_agg_sum_4119990290395352" > 0.5 * "sweltering"."max_short_val_max_val"
  ),
  scrawny as (
  SELECT
      "macho"."_best_customers_customer_id" as "best_customers_customer_id"
  FROM
      "macho"),
  puzzled as (
  SELECT
      "web_billing_customer_customers"."C_FIRST_NAME" as "web_billing_customer_first_name",
      "web_billing_customer_customers"."C_LAST_NAME" as "web_billing_customer_last_name",
      "web_web_sales"."WS_LIST_PRICE" as "web_list_price",
      "web_web_sales"."WS_QUANTITY" as "web_quantity"
  FROM
      "web_sales" as "web_web_sales"
      INNER JOIN "date_dim" as "web_date_date" on "web_web_sales"."WS_SOLD_DATE_SK" = "web_date_date"."D_DATE_SK"
      INNER JOIN "item" as "web_item_items" on "web_web_sales"."WS_ITEM_SK" = "web_item_items"."I_ITEM_SK"
      INNER JOIN "customer" as "web_billing_customer_customers" on "web_web_sales"."WS_BILL_CUSTOMER_SK" = "web_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "web_date_date"."D_YEAR" = 2000 and "web_date_date"."D_MOY" = 2 and "web_web_sales"."WS_BILL_CUSTOMER_SK" is not null and "web_web_sales"."WS_BILL_CUSTOMER_SK" in (select scrawny."best_customers_customer_id" from scrawny where scrawny."best_customers_customer_id" is not null) and "web_item_items"."I_ITEM_ID" in (select juicy."frequent_items_item_id" from juicy where juicy."frequent_items_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "web_item_items"."I_ITEM_SK",
      "web_web_sales"."WS_ORDER_NUMBER"),
  friendly as (
  SELECT
      "cat_billing_customer_customers"."C_FIRST_NAME" as "cat_billing_customer_first_name",
      "cat_billing_customer_customers"."C_LAST_NAME" as "cat_billing_customer_last_name",
      "cat_catalog_sales"."CS_LIST_PRICE" as "cat_list_price",
      "cat_catalog_sales"."CS_QUANTITY" as "cat_quantity"
  FROM
      "catalog_sales" as "cat_catalog_sales"
      INNER JOIN "date_dim" as "cat_date_date" on "cat_catalog_sales"."CS_SOLD_DATE_SK" = "cat_date_date"."D_DATE_SK"
      INNER JOIN "item" as "cat_item_items" on "cat_catalog_sales"."CS_ITEM_SK" = "cat_item_items"."I_ITEM_SK"
      INNER JOIN "customer" as "cat_billing_customer_customers" on "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cat_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cat_date_date"."D_YEAR" = 2000 and "cat_date_date"."D_MOY" = 2 and "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" is not null and "cat_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select scrawny."best_customers_customer_id" from scrawny where scrawny."best_customers_customer_id" is not null) and "cat_item_items"."I_ITEM_ID" in (select juicy."frequent_items_item_id" from juicy where juicy."frequent_items_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cat_catalog_sales"."CS_ITEM_SK",
      "cat_catalog_sales"."CS_ORDER_NUMBER"),
  waggish as (
  SELECT
      "puzzled"."web_billing_customer_first_name" as "___tvf_arm_1_first_name",
      "puzzled"."web_billing_customer_last_name" as "___tvf_arm_1_last_name",
      sum("puzzled"."web_quantity" * "puzzled"."web_list_price") as "___tvf_arm_1_total_sales"
  FROM
      "puzzled"
  GROUP BY
      1,
      2),
  kaput as (
  SELECT
      "friendly"."cat_billing_customer_first_name" as "___tvf_arm_0_first_name",
      "friendly"."cat_billing_customer_last_name" as "___tvf_arm_0_last_name",
      sum("friendly"."cat_quantity" * "friendly"."cat_list_price") as "___tvf_arm_0_total_sales"
  FROM
      "friendly"
  GROUP BY
      1,
      2),
  puffy as (
  SELECT
      "kaput"."___tvf_arm_0_last_name" as "_combined_last_name",
      "kaput"."___tvf_arm_0_first_name" as "_combined_first_name",
      "kaput"."___tvf_arm_0_total_sales" as "_combined_total_sales"
  FROM
      "kaput"
  UNION ALL
  SELECT
      "waggish"."___tvf_arm_1_last_name" as "_combined_last_name",
      "waggish"."___tvf_arm_1_first_name" as "_combined_first_name",
      "waggish"."___tvf_arm_1_total_sales" as "_combined_total_sales"
  FROM
      "waggish"),
  hard as (
  SELECT
      "puffy"."_combined_first_name" as "_combined_first_name",
      "puffy"."_combined_last_name" as "_combined_last_name",
      "puffy"."_combined_total_sales" as "_combined_total_sales"
  FROM
      "puffy")
  SELECT
      "hard"."_combined_last_name" as "combined_last_name",
      "hard"."_combined_first_name" as "combined_first_name",
      "hard"."_combined_total_sales" as "combined_total_sales"
  FROM
      "hard"
  WHERE
      "hard"."_combined_total_sales" > 0

  ORDER BY
      coalesce("hard"."_combined_last_name",'') asc,
      coalesce("hard"."_combined_first_name",'') asc,
      "combined_total_sales" asc nulls first
  LIMIT (100)
  ```
- `trilogy run --import raw.store_sales:store select store.customer.id as cid, sum(store.quantity * store.sales_price) as total where store.customer.id in (sele…te.year = 2000 and cat.date.month_of_year = 2) and store.customer.id is not null having sum(store.quantity * store.sales_price) > 118133.26 limit 10;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - cat.billing_customer.id (line 2, col 118, in SELECT); did you mean: store.customer.id, store.return_customer.id, store.item.id, store.date.id, store.return_date.id, store.time.id?
    - cat.date.year (line 2, col 148, in WHERE); did you mean: cat.date.month_of_year, store.date.year, store.store.date.year, store.return_date.year, store.customer.first_sales_date.year, store.customer.first_shipto_date.year?
    - cat.date.month_of_year (line 2, col 173, in WHERE); did you mean: cat.date.year, store.date.month_of_year, store.store.date.month_of_year, store.return_date.month_of_year, store.customer.first_sales_date.month_of_year, store.customer.first_shipto_date.month_of_year?
  ```
- `trilogy run --import raw.store_sales:store --import raw.catalog_sales:cat --import raw.web_sales:web with best as where store.customer.id is not null select ….date.month_of_year = 2 and cat.billing_customer.id is not null and cat.billing_customer.id in best.cid select count(cat.billing_customer.id) as cnt;`

  ```text
  Syntax error in stdin: Nothing was executed: parsed 5 definition statement(s) (3 imports, 2 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run --import raw.store_sales:store --import raw.catalog_sales:cat with best as where store.customer.id is not null select store.customer.id as cid ha…r between 2000 and 2003 having count_distinct(store.item.text_id || '-' || substring(store.item.desc, 1, 30) || '-' || store.date.date::string) > 4);`

  ```text
  Unexpected error in stdin: a `(select ...)` subquery must select exactly one column
  ```
- `trilogy file read query24.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query31.preql`

  ```text
  Syntax error in query31.preql: 4 undefined concept references; fix all before re-running:
    - web_pivot.web_q2_over_q1 (line 49, col 5, in WHERE); did you mean: web_pivot.web_q3_over_q2, web_pivot.web_q2, web_pivot.web_q1, web_q2_over_q1?
    - store_pivot.store_q2_over_q1 (line 49, col 32, in WHERE); did you mean: store_pivot.store_q3_over_q2, store_pivot.store_q2, store_pivot.store_q1, store_q2_over_q1?
    - web_pivot.web_q3_over_q2 (line 50, col 9, in WHERE); did you mean: web_pivot.web_q2_over_q1, web_pivot.web_q3, web_pivot.web_q2, web_q3_over_q2?
    - store_pivot.store_q3_over_q2 (line 50, col 36, in WHERE); did you mean: store_pivot.store_q2_over_q1, store_pivot.store_q3, store_pivot.store_q2, store_q3_over_q2?
  ```
- `trilogy run query36.preql`

  ```text
  Syntax error in query36.preql: Undefined concept: base.store_sales_item_category (line 32, col 5, in ORDER BY). Suggestions: ['base.store_sales.item.category', 'base.store_sales_item_class', 'base.store_sales.item.class', 'store_sales.item.category']
  ```
- `trilogy file read query36.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {feb.cv, feb.monthly_stats.mn, feb.monthly_stats.mo, jan.cv, jan.monthly_stats.item_id, jan.monthly_stats.mn, jan.monthly_stats.mo, jan.monthly_stats.wh_id}
  ```
- `trilogy file read query39.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 2 disconnected subgraphs: {_item_profits_avg_net_profit, ss.store.id}; {_item_profits_item_id, _item_profits_prod_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 17). The requested concepts split into 2 disconnected subgraphs: {item_profits.item_id}; {ss.item.id, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query44.preql`

  ```text
  Unexpected error in query44.preql: Cannot join `item_profits.item_id` to itself (`item_profits.item_id` resolves to the same key `item_profits.item_id`), which degenerates to `1=1`. Join distinct keys (e.g. separate rowset outputs or distinct expressions).
  ```
- `trilogy file read query44.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 26). The requested concepts split into 2 disconnected subgraphs: {_customer_store_totals_cust_id, _customer_store_totals_total_price, store_sales.customer.address.county, store_sales.customer.address.state, store_sales.customer.id, store_sales.date.month_seq, store_sales.store.county, store_sales.store.state}; {dec1998_month_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query54.preql`

  ```text
  Unexpected error in query54.preql: a `(select ...)` subquery must select exactly one column
  ```
- `trilogy run query57.preql`

  ```text
  Syntax error in query57.preql: Undefined concept: mt.category. Suggestions: ['cs.item.category', 'cs.item.category_id', 'monthly_totals.category']
  ```
- `trilogy run query58.preql`

  ```text
  Syntax error in query58.preql: Undefined concept: date.week_seq.
  ```
- `trilogy file read query61.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
  ```
- `trilogy file read query68.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query71.preql`

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
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 81 column 4 (char 1986). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query77.preql`

  ```text
  Unexpected error in query77.preql: (_duckdb.BinderException) Binder Error: GROUPING child "s_channel" must be a grouping column

  LINE 109: 	WHEN grouping("juicy"."s_channel") = 1 THEN null
                 ^
  [SQL:
  WITH
  cheerful as (
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
  abundant as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_SOLD_DATE_SK" as "s_date_id",
      "s_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "s_ext_sales_price",
      "s_catalog_sales_unified"."CS_ITEM_SK" as "s_item_id",
      "s_catalog_sales_unified"."CS_NET_PROFIT" as "s_net_profit",
      "s_catalog_sales_unified"."CS_ORDER_NUMBER" as "s_order_id",
      "s_catalog_sales_unified"."CS_CALL_CENTER_SK" as "s_outlet_id"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
  WHERE
      "s_catalog_sales_unified"."CS_CALL_CENTER_SK" is not null

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_SOLD_DATE_SK" as "s_date_id",
      "s_store_sales_unified"."SS_EXT_SALES_PRICE" as "s_ext_sales_price",
      "s_store_sales_unified"."SS_ITEM_SK" as "s_item_id",
      "s_store_sales_unified"."SS_NET_PROFIT" as "s_net_profit",
      "s_store_sales_unified"."SS_TICKET_NUMBER" as "s_order_id",
      "s_store_sales_unified"."SS_STORE_SK" as "s_outlet_id"
  FROM
      "store_sales" as "s_store_sales_unified"
  WHERE
      "s_store_sales_unified"."SS_STORE_SK" is not null

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_SOLD_DATE_SK" as "s_date_id",
      "s_web_sales_unified"."WS_EXT_SALES_PRICE" as "s_ext_sales_price",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_id",
      "s_web_sales_unified"."WS_NET_PROFIT" as "s_net_profit",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id",
      "s_web_sales_unified"."WS_WEB_PAGE_SK" as "s_outlet_id"
  FROM
      "web_sales" as "s_web_sales_unified"
  WHERE
      "s_web_sales_unified"."WS_WEB_PAGE_SK" is not null
  ),
  juicy as (
  SELECT
      "abundant"."s_outlet_id" as "s_outlet_id",
      CASE
  	WHEN coalesce("abundant"."s_channel","cheerful"."s_channel") = 'STORE' THEN 'store channel'
  	WHEN coalesce("abundant"."s_channel","cheerful"."s_channel") = 'CATALOG' THEN 'catalog channel'
  	WHEN coalesce("abundant"."s_channel","cheerful"."s_channel") = 'WEB' THEN 'web channel'
  	END as "channel_label",
      CASE WHEN cast("s_date_date"."D_DATE" as date) >= date '2000-08-23' and cast("s_date_date"."D_DATE" as date) <= date '2000-09-22' THEN "abundant"."s_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_5300101624022709",
      CASE WHEN cast("s_date_date"."D_DATE" as date) >= date '2000-08-23' and cast("s_date_date"."D_DATE" as date) <= date '2000-09-22' THEN "abundant"."s_net_profit" ELSE NULL END as "_virt_filter_net_profit_680025349974531",
      CASE WHEN cast("s_return_date_date"."D_DATE" as date) >= date '2000-08-23' and cast("s_return_date_date"."D_DATE" as date) <= date '2000-09-22' THEN "cheerful"."s_return_amount" ELSE NULL END as "_virt_filter_return_amount_812246143086209",
      CASE WHEN cast("s_return_date_date"."D_DATE" as date) >= date '2000-08-23' and cast("s_return_date_date"."D_DATE" as date) <= date '2000-09-22' THEN "cheerful"."s_return_net_loss" ELSE NULL END as "_virt_filter_return_net_loss_4624054266166043",
      coalesce("abundant"."s_channel","cheerful"."s_channel") as "s_channel"
  FROM
      "abundant"
      LEFT OUTER JOIN "cheerful" on "abundant"."s_channel" = "cheerful"."s_channel" AND "abundant"."s_item_id" = "cheerful"."s_item_id" AND "abundant"."s_order_id" = "cheerful"."s_order_id"
      LEFT OUTER JOIN "date_dim" as "s_date_date" on "abundant"."s_date_id" = "s_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "date_dim" as "s_return_date_date" on "cheerful"."s_return_date_id" = "s_return_date_date"."D_DATE_SK"
  WHERE
      "abundant"."s_outlet_id" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      coalesce("abundant"."s_item_id","cheerful"."s_item_id"),
      coalesce("abundant"."s_order_id","cheerful"."s_order_id")),
  sparkling as (
  SELECT
      CASE
  	WHEN grouping("juicy"."s_channel") = 1 THEN null
  	ELSE "juicy"."channel_label"
  	END as "channel",
      CASE
  	WHEN grouping("juicy"."s_outlet_id") = 1 THEN null
  	ELSE "juicy"."s_outlet_id"
  	END as "outlet",
      coalesce(sum("juicy"."_virt_filter_ext_sales_price_5300101624022709"),0) as "sales",
      coalesce(sum("juicy"."_virt_filter_net_profit_680025349974531"),0) - coalesce(sum("juicy"."_virt_filter_return_net_loss_4624054266166043"),0) as "profit",
      coalesce(sum("juicy"."_virt_filter_return_amount_812246143086209"),0) as "returns"
  FROM
      "juicy"
  GROUP BY
      ROLLUP ("juicy"."channel_label", "juicy"."s_outlet_id"))
  SELECT
      "sparkling"."channel" as "channel",
      "sparkling"."outlet" as "outlet",
      "sparkling"."sales" as "sales",
      "sparkling"."returns" as "returns",
      "sparkling"."profit" as "profit"
  FROM
      "sparkling"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      "sparkling"."channel" asc nulls first,
      "sparkling"."outlet" asc nulls first,
      "sparkling"."returns" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {inventory.date.date, inventory.quantity_on_hand}; {item.current_price, item.id, item.manufacturer_id, description, item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Undefined concept: store_qty (line 47, col 25, in ORDER BY). Suggestions: ['store_agg.store_qty', 'wr.store.city', 'sr.store.city']
  ```
- `trilogy file read query83.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query86.preql`

  ```text
  Syntax error in query86.preql: ORDER BY contains aggregate `grouping(ws.item.category)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(ws.item.category) as g order by g desc`.
  ```
- `trilogy file read query89.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query90.preql`

  ```text
  Syntax error in query90.preql: Undefined concept: ship_household_demographic.dependent_count. Suggestions: ['web_sales.ship_household_demographic.dependent_count', 'web_sales.bill_household_demographic.dependent_count', 'web_sales.ship_customer.household_demographic.dependent_count', 'web_sales.billing_customer.demographics.dependent_count', 'web_sales.billing_customer.household_demographic.dependent_count', 'web_sales.ship_customer.demographics.dependent_count']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:store_sales select return_reason.desc limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: return_reason.desc (line 2, col 8, in SELECT). Suggestions: ['store_sales.return_reason.desc', 'store_sales.return_reason.id', 'store_sales.item.desc']
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw.store_sales:ss select ss.date.year, ss.date.month_of_year, count(ss.line_item) as cnt group by 1,2 order by 1,2 limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...r, count(ss.line_item) as cnt ??? group by 1,2 order by 1,2 limi...
  ```
- `trilogy file write query06.preql --content import raw.item as item;
import raw.store_sales as ss;

merge item.id into ~ss.item.id;

# Per-item price and cate…items
having
    ss.item.current_price > 1.2 * cat_avg_price
order by
    qualifying_line_items asc nulls first,
    state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:40
     |
  17 | subset join ss.item.id = item_prices.id;
     |                                        ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...in ss.item.id = item_prices.id ??? ;  where     ss.date.year = 20...

  Write stats: received 808 chars / 808 bytes; tail: …'_items asc nulls first,\\n    state asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select distinct ss.customer_demographic.gender, ss.customer_demographic.marital_status, ss.customer_demographic.education_status limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...raw.store_sales as ss; select ??? distinct ss.customer_demograph...
  ```
- `trilogy run --import raw.store_sales:store select store.item.text_id as item_id, count_distinct(store.item.text_id || '-' || substring(store.item.desc, 1, 30…ore.item.text_id having count_distinct(store.item.text_id || '-' || substring(store.item.desc, 1, 30) || '-' || store.date.date::string) > 4 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...te.year between 2000 and 2003 ??? group by store.item.text_id ha...
  ```
- `trilogy run --import raw.store_sales:store select max(tt) as m from (select store.customer.id as cid, sum(store.quantity * store.sales_price ? store.date.year between 2000 and 2003) as tt where store.customer.id is not null);`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...as store; select max(tt) as m ??? from (select store.customer.id...
  ```
- `trilogy run --import raw.catalog_sales:cat --import raw.web_sales:web select count(*) as cnt where cat.date.year = 2000 and cat.date.month_of_year = 2 and cat.billing_customer.id = 9901 and cat.item.text_id in (select 'AAAAAAAAIOJAAAAA');`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
   raw.web_sales as web; select ??? count(*) as cnt where cat.date...
  ```
- `trilogy run --import raw.store_sales:store --import raw.catalog_sales:cat sub union join store.customer.id = cat.billing_customer.id where store.date.year be…(store.quantity * store.sales_price) > 0.5 * 236266.51 select store.customer.id, cat.billing_customer.id, sum(cat.quantity * cat.list_price) limit 5;`

  ```text
  Syntax error in stdin:  --> 3:1
    |
  3 | sub union join store.customer.id = cat.billing_customer.id where store.date.year between 2000 and 2003 and cat.date.year = 2000 and cat.date.month_of_year = 2 and store.customer.id is not null and cat.billing_customer.id is not null having count_distinct(store.item.text_id || '-' || substring(store.item.desc, 1, 30) || '-' || store.date.date::string) > 4 and sum(store.quantity * store.sales_price) > 0.5 * 236266.51 select store.customer.id, cat.billing_customer.id, sum(cat.quantity * cat.list_price) limit 5;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ort raw.catalog_sales as cat; ??? sub union join store.customer....
  ```
- `trilogy file write query26.preql --content import raw/catalog_sales as cs;

where
  cs.sold_date.year = 2000
  and cs.billing_customer_demographic.gender = '…rice) as avg_list_price,
    avg(cs.coupon_amt) as avg_coupon_amt,
    avg(cs.sales_price) as avg_sales_price
order by
    cs.item.text_id
limit 100;`

  ```text
  refused to write 'query26.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  wher...

  Write stats: received 547 chars / 547 bytes; tail: …') as avg_sales_price\\norder by\\n    cs.item.text_id\\nlimit 100;'.
  ```
- `trilogy file write query36.preql --content import raw.store_sales as store_sales;

# For store sales in 2001 at stores in TN
where store_sales.date.year = 20….item.class)
order by
    hierarchy_level desc nulls first,
    store_sales.item.category nulls first,
    within_parent_rank(nulls first)
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:23
     |
  16 |     within_parent_rank(nulls first)
     |                       ^---
     |
     = expected limit, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   first,     within_parent_rank ??? (nulls first) limit 100;

  Write stats: received 686 chars / 686 bytes; tail: …' nulls first,\\n    within_parent_rank(nulls first)\\nlimit 100;'.
  ```
- `trilogy file write query37.preql`

  ```text
  refused to write 'query37.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
   in inv_items.text_id
   select ??? distinct
       cs.item.text_id

  Write stats: received 722 chars / 722 bytes; tail: …'\\n    cs.item.current_price\r\\norder by item_code\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query39.preql --content import raw.inventory as inv;

# Aggregate inventory by warehouse, item, month-of-year for year 2001
with monthly_s…first,
    jan_mn asc nulls first,
    jan_cv asc nulls first,
    feb_mo_h asc nulls first,
    feb_mn asc nulls first,
    feb_cv asc nulls first
;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...or same warehouse+item select ??? distinct     paired.wh_id,

  Write stats: received 1414 chars / 1414 bytes; tail: …'st,\\n    feb_mn asc nulls first,\\n    feb_cv asc nulls first\\n;'.
  ```
- `trilogy file write query43.preql --content import raw/store_sales as sales;

# Pivot macro: sum unit price (sales_price) for a given day name
def dow_sum(d) …day') as thursday,
    @dow_sum('Friday') as friday,
    @dow_sum('Saturday') as saturday,
order by
    store_name asc,
    store_code asc
limit 100;`

  ```text
  refused to write 'query43.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as sales;  # P...

  Write stats: received 631 chars / 631 bytes; tail: …',\\norder by\\n    store_name asc,\\n    store_code asc\\nlimit 100;'.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;
import raw.item as item;

# Threshold: 90% of avg net profit on sales at store 1 whe…name as best_performer,
    worst.product_name as worst_performer
order by
    rank asc,
    best_performer desc,
    worst_performer desc
limit 100;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...ies <- avg_profit > threshold ??? by item.id;  # Best performers...

  Write stats: received 1256 chars / 1256 bytes; tail: …'    best_performer desc,\\n    worst_performer desc\\nlimit 100;'.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;

# Auto concepts for the threshold and avg profit per item at store 1
# Threshold: 9…k <= 10 or worst_rank <= 10)
select
    best_rank as best_rnk,
    worst_rank as worst_rnk,
    item_name as prod_name,
    item_avg as avg_profit
;
`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:68
     |
  12 | auto qualifies <- avg(ss.net_profit ? ss.store.id = 1) > threshold by ss.item.id;
     |                                                                    ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   ss.store.id = 1) > threshold ??? by ss.item.id;  # Product name...

  Write stats: received 1141 chars / 1141 bytes; tail: …'k,\\n    item_name as prod_name,\\n    item_avg as avg_profit\\n;\\n'.
  ```
- `trilogy run --import raw.address:addr select distinct substring(addr.zip, 1, 5) as zip5 where substring(addr.zip, 1, 5) in ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792') limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...t raw.address as addr; select ??? distinct substring(addr.zip, 1...
  ```
- `trilogy file write query49.preql --content import raw.all_sales as s;

# Qualifying transactions: Dec 2001, net_profit > 1, net_paid > 0, quantity > 0, and r…
order by
    s.channel asc nulls first,
    return_rank asc nulls first,
    currency_rank asc nulls first,
    s.item.id asc nulls first
limit 100;`

  ```text
  refused to write 'query49.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:58
     |
  17 |     sum(s.return_amount) / nullif(sum(s.net_paid), 0) as --currency_ratio,
     |                                                          ^---
     |
     = expected IDENTIFIER
  Location:
  ...nullif(sum(s.net_paid), 0) as ??? --currency_ratio,     rank(s.i...

  Write stats: received 983 chars / 983 bytes; tail: …'nk asc nulls first,\\n    s.item.id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query49.preql --content import raw.all_sales as s;

# Qualifying transactions: Dec 2001, net_profit > 1, net_paid > 0, quantity > 0, and r…
order by
    s.channel asc nulls first,
    return_rank asc nulls first,
    currency_rank asc nulls first,
    s.item.id asc nulls first
limit 100;`

  ```text
  refused to write 'query49.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:73
     |
  16 |     sum(s.return_amount) / nullif(sum(s.net_paid), 0) as currency_ratio --,
     |                                                                         ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t_paid), 0) as currency_ratio ??? --,     rank(s.item.id) over (...

  Write stats: received 962 chars / 962 bytes; tail: …'nk asc nulls first,\\n    s.item.id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query53.preql --content import raw.store_sales as ss;

# Filter: year 2000, store recorded
where 
  ss.date.year = 2000
  and ss.store.id …f_year) totals.
# We need to compute the average quarterly sales per manufacturer_id,
# then filter rows where avg > 0 and |total - avg| / avg > 0.1
`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:5
     |
  20 |     by ss.date.quarter as quarter_of_year
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...s_price) as quarter_total     ??? by ss.date.quarter as quarter_...

  Write stats: received 973 chars / 973 bytes; tail: …'hen filter rows where avg > 0 and |total - avg| / avg > 0.1\\n'.
  ```
- `trilogy file write query56.preql --content import raw.all_sales as sales;

where 
    sales.date.year = 2001 
    and sales.date.month_of_year = 2
    and sa…  sales.item.text_id as item_code,
    sum(sales.ext_sales_price) as total_sales
order by
    total_sales, item_code
both with nulls first
limit 100;`

  ```text
  refused to write 'query56.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | both with nulls first
     | ^---
     |
     = expected limit, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...by     total_sales, item_code ??? both with nulls first limit 10...

  Write stats: received 368 chars / 368 bytes; tail: …'\\n    total_sales, item_code\\nboth with nulls first\\nlimit 100;'.
  ```
- `trilogy file write query63.preql --content import raw.store_sales as ss;

# Filter to year 2000 and the two item profile groups, with a recorded store
auto f…tal - avg_monthly_sales) / avg_monthly_sales > 0.1
order by
    ss.item.manager_id asc,
    avg_monthly_sales asc,
    per_month_total asc
limit 100;`

  ```text
  refused to write 'query63.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:33
    |
  4 | auto filtered <- ss.sales_price &
    |                                 ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...to filtered <- ss.sales_price ??? &   ss.date.year = 2000 and

  Write stats: received 1309 chars / 1309 bytes; tail: …'   avg_monthly_sales asc,\\n    per_month_total asc\\nlimit 100;'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as ss;

# First compute total net profit per state for year 2000 (to find top 5 states)
aut…nk_within_parent
by rollup (ss.store.state, ss.store.county)
order by
    hierarchy_level desc,
    ss.store.state,
    rank_within_parent
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:28
     |
  17 |     grouping(ss.store.state, ss.store.county) as hierarchy_level,
     |                            ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t,     grouping(ss.store.state ??? , ss.store.county) as hierarch...

  Write stats: received 1113 chars / 1113 bytes; tail: …' desc,\\n    ss.store.state,\\n    rank_within_parent\\nlimit 100;'.
  ```
- `trilogy file write query75.preql -e -c import raw.all_sales as s;

# Dedup: remove duplicate lines at the (item, channel, order_id) grain
# Compute net metri… join yr2001.manufacturer_id = yr2002.manufacturer_id
where yr2002.total_qty / yr2001.total_qty < 0.9
order by qty_diff asc, amt_diff asc
limit 100
;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | dedup_lines <-
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   Compute net metrics per line ??? dedup_lines <-  select     s.i...

  Write stats: received 2113 chars / 2113 bytes; tail: …'al_qty < 0.9\\norder by qty_diff asc, amt_diff asc\\nlimit 100\\n;'.
  ```
- `trilogy file write query83.preql --content import raw.store_returns as sr;
import raw.catalog_returns as cr;
import raw.web_returns as wr;

# Week sequences …agg.item_code = catalog_agg.item_code
union join store_agg.item_code = web_agg.item_code
order by item_code asc, store_qty asc nulls first
limit 100;`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
    --> 45:1
     |
  45 | union join store_agg.item_code = catalog_agg.item_code
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
   web_agg.web_rows is not null ??? union join store_agg.item_code...

  Write stats: received 1446 chars / 1446 bytes; tail: …'order by item_code asc, store_qty asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query84.preql --content import raw.store_returns as sr;
import raw.customer as c;

# Join store_returns customer_demographic with any cust…demographic.income_band.lower_bound >= 38128
  and c.household_demographic.income_band.upper_bound <= 88128
order by c.text_id nulls first
limit 100;`

  ```text
  refused to write 'query84.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:28
     |
  10 |     sr.item.id as _item_id --,
     |                            ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...e,     sr.item.id as _item_id ??? --,     sr.ticket_number as _t...

  Write stats: received 814 chars / 814 bytes; tail: …'per_bound <= 88128\\norder by c.text_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.web_sales:ws select distinct ws.item.category, count(ws.item.id) as cnt where ws.date.year = 2000 group by rollup (ws.item.category) limit 100;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...t raw.web_sales as ws; select ??? distinct ws.item.category, cou...
  ```
- `trilogy file write query94.preql --content import raw.web_sales as ws;

# Web sales: ship date between 1999-02-01 and 1999-04-02,
# shipped to IL, web site c…as order_count,
    sum(ws.ext_ship_cost) as total_extended_ship_cost,
    sum(ws.net_profit) as total_net_profit
order by
    order_count
limit 100;`

  ```text
  refused to write 'query94.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...and whether any return exists ??? auto warehouse_count <- count_...

  Write stats: received 918 chars / 918 bytes; tail: …'fit) as total_net_profit\\norder by\\n    order_count\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy trilogy file write query06.preql --content import raw.store_sales as ss;
import raw.item as item;

# Compute unweighted per-category average price fr…em.id
having
    ss.item.current_price > 1.2 * cat_avg_price
order by
    qualifying_line_items asc nulls first,
    state asc nulls first
limit 100;`

  ```text
  No such command 'trilogy'.
  ```
- `trilogy explore raw/sale_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sale_address.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy file write --content import raw.catalog_sales as cs;
import raw.inventory as inv;

# Items in inventory with quantity 100-500 on dates 2000-02-01 to …ms.text_id
select distinct
    cs.item.text_id as item_code,
    cs.item.desc as description,
    cs.item.current_price
order by item_code
limit 100;`

  ```text
  Missing argument 'PATH'.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore`

  ```text
  Missing argument 'PATH'.
  ```
- `trilogy explore raw/sales/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales/sales.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.date, count(1) where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_id is not null limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(1) as count`
  Location:
  ...all_sales.date.date, count(1) ??? where all_sales.date.date betw...
  ```
- `trilogy run --import raw.store_sales:store --import raw.catalog_sales:cat --import raw.web_sales:web select store.date.year, count(store.item.text_id) where store.date.year between 2000 and 2003 limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(store.item.text_id) as text_id_count`
  Location:
  ...ar, count(store.item.text_id) ??? where store.date.year between
  ```
- `trilogy file write query49.preql --content import raw.all_sales as s;

where
    s.date.year = 2001
    and s.date.month_of_year = 12
    and s.net_profit > …
order by
    s.channel asc nulls first,
    return_rank asc nulls first,
    currency_rank asc nulls first,
    s.item.id asc nulls first
limit 100;`

  ```text
  refused to write 'query49.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sum(s.return_amount) / nullif(sum(s.net_paid), 0) as net_paid_sum`
  Location:
  ...) / nullif(sum(s.net_paid), 0) ??? ,     rank(s.item.id) over (pa...

  Write stats: received 836 chars / 836 bytes; tail: …'nk asc nulls first,\\n    s.item.id asc nulls first\\nlimit 100;'.
  ```

### `undefined-concept`

- `trilogy explore query36.preql`

  ```text
  Failed to parse query36.preql: (UndefinedConceptException(...), "Undefined concept: base.store_sales_item_category (line 32, col 5, in ORDER BY). Suggestions: ['base.store_sales.item.category', 'base.store_sales_item_class', 'base.store_sales.item.class', 'store_sales.item.category']")
  ```

### `type-error`

- `trilogy run query92.preql`

  ```text
  Syntax error in query92.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
