# Trilogy failure analysis — 20260628-042639

- Run `20260628-042638_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1329 | failed: 113 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 79 | 70% |
| `syntax-parse` | 27 | 24% |
| `cli-misuse` | 4 | 4% |
| `join-resolution` | 2 | 2% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: daily_sales.next_amt. Suggestions: ['daily_sales.amt', '_daily_sales_amt', 'daily_sales.ws']
  ```
- `trilogy run query04.preql duckdb`

  ```text
  Syntax error in query04.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 50, col 5, in ORDER BY); did you mean: store.customer.first_name, store.return_customer.first_name, catalog.ship_customer.first_name, catalog.bill_customer.first_name, web.billing_customer.first_name, web.ship_customer.first_name?
    - last_name (line 51, col 5, in ORDER BY); did you mean: store.customer.last_name, store.return_customer.last_name, catalog.ship_customer.last_name, catalog.bill_customer.last_name, web.billing_customer.last_name, web.ship_customer.last_name?
    - preferred_cust_flag (line 52, col 5, in ORDER BY); did you mean: store.customer.preferred_cust_flag, store.return_customer.preferred_cust_flag, catalog.ship_customer.preferred_cust_flag, catalog.bill_customer.preferred_cust_flag, web.billing_customer.preferred_cust_flag, web.ship_customer.preferred_cust_flag?
  ```
- `trilogy file read query04.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.ch)` (line 40), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.ch) as g order by g desc`.
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.preferred_cust_flag. Suggestions: ['store_sales.customer.preferred_cust_flag', 'store_sales.return_customer.preferred_cust_flag']
  ```
- `trilogy file read query08.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query10.preql`

  ```text
  Resolution error in query10.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 22). The requested concepts split into 2 disconnected subgraphs: {customer.demographics.college_dependent_count, customer.demographics.credit_rating, customer.demographics.dependent_count, customer.demographics.education_status, customer.demographics.employed_dependent_count, customer.demographics.gender, customer.demographics.marital_status, customer.demographics.purchase_estimate, local.customer_count, local.customer_count2, local.customer_count3, local.customer_count4, local.customer_count5, local.customer_count6, local.has_catalog_sale_2002, local.has_demographics, local.has_web_sale_2002, local.in_county}; {local.has_store_sale_2002}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(local.channel)` (line 19), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel) as g order by g desc`.
  ```
- `trilogy run query15.preql`

  ```text
  Syntax error in query15.preql: Cannot compare STRING and ArrayType<INTEGER> with operator ComparisonOperator.IN in SubselectComparison(left=substring(ref:cs.bill_customer.address.zip,1,5), right=(85669, 86197, 88274, 83405, 86475, 85392, 85460, 80348, 81792), operator=<ComparisonOperator.IN: 'in'>)
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query21.preql`

  ```text
  Resolution error in query21.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 10). The requested concepts split into 3 disconnected subgraphs: {item.current_price, local.item_code}; {local.after_total, local.before_total}; {local.warehouse_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 20 column 14 (char 649). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Undefined concept: local.customer_total. Suggestions: ['customer.text_id', 'customer.birth_month', 'customer.login']
  ```
- `trilogy run query23.preql`

  ```text
  Resolution error in query23.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 10). The requested concepts split into 2 disconnected subgraphs: {local._frequent_items_desc_prefix, local._frequent_items_item_id}; {local._frequent_items_pair_count, local._frequent_items_sold_date, store_sales.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: (_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!

  LINE 61:     CASE WHEN sum("abundant"."store_sales_quantity" * "abundant"."store_s...
                         ^
  [SQL:
  WITH
  sweltering as (
  SELECT
      "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" BETWEEN 2000 AND 2003

  GROUP BY
      1,
      cast("store_sales_date_date"."D_DATE" as date)
  HAVING
      count((cast("store_sales_store_sales"."SS_TICKET_NUMBER" as string) || '-' || cast("store_sales_store_sales"."SS_ITEM_SK" as string))) > 4
  ),
  abundant as (
  SELECT
      "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
      "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
      "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity",
      "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price"
  FROM
      "store_sales" as "store_sales_store_sales"
      LEFT OUTER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"),
  scrawny as (
  SELECT
      "sweltering"."store_sales_item_id" as "_frequent_items_item_id"
  FROM
      "sweltering"
      INNER JOIN "item" as "store_sales_item_items" on "sweltering"."store_sales_item_id" = "store_sales_item_items"."I_ITEM_SK"),
  uneven as (
  SELECT
      sum("abundant"."store_sales_quantity" * CASE WHEN "abundant"."store_sales_date_year" BETWEEN 2000 AND 2003 and "abundant"."store_sales_customer_id" is not null THEN "abundant"."store_sales_sales_price" ELSE NULL END) as "all_customer_totals"
  FROM
      "abundant"
  GROUP BY
      "abundant"."store_sales_customer_id"),
  friendly as (
  SELECT
      "scrawny"."_frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "scrawny"),
  juicy as (
  SELECT
      max("uneven"."all_customer_totals") * 0.5 as "best_customer_threshold"
  FROM
      "uneven"),
  kaput as (
  SELECT
      "friendly"."frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "friendly"
  GROUP BY
      1),
  vacuous as (
  SELECT
      "abundant"."store_sales_customer_id" as "store_sales_customer_id",
      "abundant"."store_sales_date_year" as "store_sales_date_year",
      CASE WHEN sum("abundant"."store_sales_quantity" * "abundant"."store_sales_sales_price") > "juicy"."best_customer_threshold" and "abundant"."store_sales_date_year" BETWEEN 2000 AND 2003 and "abundant"."store_sales_customer_id" is not null THEN "abundant"."store_sales_customer_id" ELSE NULL END as "_virt_filter_id_390034287534358"
  FROM
      "juicy"
      LEFT OUTER JOIN "abundant" on 1=1
  GROUP BY
      1,
      2,
      3,
      "juicy"."best_customer_threshold"
  HAVING
      sum("abundant"."store_sales_quantity" * "abundant"."store_sales_sales_price") > "juicy"."best_customer_threshold"
  ),
  young as (
  SELECT
      "vacuous"."_virt_filter_id_390034287534358" as "_virt_filter_id_390034287534358"
  FROM
      "vacuous"
  WHERE
      "vacuous"."store_sales_date_year" BETWEEN 2000 AND 2003 and "vacuous"."store_sales_customer_id" is not null
  ),
  cooperative as (
  SELECT
      "store_sales_store_sales"."SS_CUSTOMER_SK" as "_best_customers_cust_id"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" BETWEEN 2000 AND 2003 and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null and "store_sales_store_sales"."SS_CUSTOMER_SK" in (select young."_virt_filter_id_390034287534358" from young where young."_virt_filter_id_390034287534358" is not null)

  GROUP BY
      1),
  sparkling as (
  SELECT
      "cooperative"."_best_customers_cust_id" as "_best_customers_cust_id"
  FROM
      "cooperative"),
  abhorrent as (
  SELECT
      "sparkling"."_best_customers_cust_id" as "best_customers_cust_id"
  FROM
      "sparkling"),
  waggish as (
  SELECT
      "web_sales_billing_customer_customers"."C_FIRST_NAME" as "web_sales_billing_customer_first_name",
      "web_sales_billing_customer_customers"."C_LAST_NAME" as "web_sales_billing_customer_last_name",
      "web_sales_web_sales"."WS_LIST_PRICE" as "web_sales_list_price",
      "web_sales_web_sales"."WS_QUANTITY" as "web_sales_quantity"
  FROM
      "web_sales" as "web_sales_web_sales"
      INNER JOIN "date_dim" as "web_sales_date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "web_sales_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "web_sales_billing_customer_customers" on "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" = "web_sales_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "web_sales_date_date"."D_YEAR" = 2000 and "web_sales_date_date"."D_MOY" = 2 and "web_sales_web_sales"."WS_ITEM_SK" in (select kaput."frequent_items_item_id" from kaput where kaput."frequent_items_item_id" is not null) and "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" in (select abhorrent."best_customers_cust_id" from abhorrent where abhorrent."best_customers_cust_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "web_sales_web_sales"."WS_ITEM_SK",
      "web_sales_web_sales"."WS_ORDER_NUMBER"),
  divergent as (
  SELECT
      "catalog_sales_bill_customer_customers"."C_FIRST_NAME" as "catalog_sales_bill_customer_first_name",
      "catalog_sales_bill_customer_customers"."C_LAST_NAME" as "catalog_sales_bill_customer_last_name",
      "catalog_sales_catalog_sales"."CS_LIST_PRICE" as "catalog_sales_list_price",
      "catalog_sales_catalog_sales"."CS_QUANTITY" as "catalog_sales_quantity"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
      INNER JOIN "date_dim" as "catalog_sales_sold_date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sales_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "catalog_sales_bill_customer_customers" on "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" = "catalog_sales_bill_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "catalog_sales_sold_date_date"."D_YEAR" = 2000 and "catalog_sales_sold_date_date"."D_MOY" = 2 and "catalog_sales_catalog_sales"."CS_ITEM_SK" in (select kaput."frequent_items_item_id" from kaput where kaput."frequent_items_item_id" is not null) and "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select abhorrent."best_customers_cust_id" from abhorrent where abhorrent."best_customers_cust_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "catalog_sales_catalog_sales"."CS_ITEM_SK",
      "catalog_sales_catalog_sales"."CS_ORDER_NUMBER"),
  rambunctious as (
  SELECT
      "waggish"."web_sales_billing_customer_first_name" as "___tvf_arm_1_first_name",
      "waggish"."web_sales_billing_customer_last_name" as "___tvf_arm_1_last_name",
      sum("waggish"."web_sales_quantity" * "waggish"."web_sales_list_price") as "___tvf_arm_1_total_sales"
  FROM
      "waggish"
  GROUP BY
      1,
      2),
  busy as (
  SELECT
      "divergent"."catalog_sales_bill_customer_first_name" as "___tvf_arm_0_first_name",
      "divergent"."catalog_sales_bill_customer_last_name" as "___tvf_arm_0_last_name",
      sum("divergent"."catalog_sales_quantity" * "divergent"."catalog_sales_list_price") as "___tvf_arm_0_total_sales"
  FROM
      "divergent"
  GROUP BY
      1,
      2),
  hard as (
  SELECT
      "busy"."___tvf_arm_0_last_name" as "_combined_last_name",
      "busy"."___tvf_arm_0_first_name" as "_combined_first_name",
      "busy"."___tvf_arm_0_total_sales" as "_combined_total_sales"
  FROM
      "busy"
  UNION ALL
  SELECT
      "rambunctious"."___tvf_arm_1_last_name" as "_combined_last_name",
      "rambunctious"."___tvf_arm_1_first_name" as "_combined_first_name",
      "rambunctious"."___tvf_arm_1_total_sales" as "_combined_total_sales"
  FROM
      "rambunctious"),
  sedate as (
  SELECT
      "hard"."_combined_first_name" as "_combined_first_name",
      "hard"."_combined_last_name" as "_combined_last_name",
      "hard"."_combined_total_sales" as "_combined_total_sales"
  FROM
      "hard"),
  yellow as (
  SELECT
      "sedate"."_combined_first_name" as "combined_first_name",
      "sedate"."_combined_last_name" as "combined_last_name",
      "sedate"."_combined_total_sales" as "combined_total_sales"
  FROM
      "sedate")
  SELECT
      "yellow"."combined_last_name" as "combined_last_name",
      "yellow"."combined_first_name" as "combined_first_name",
      "yellow"."combined_total_sales" as "combined_total_sales"
  FROM
      "yellow"
  GROUP BY
      1,
      2,
      3
  ORDER BY
      "yellow"."combined_last_name" asc,
      "yellow"."combined_first_name" asc,
      "yellow"."combined_total_sales" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: (_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!

  LINE 77:     CASE WHEN sum("abundant"."store_sales_quantity" * "abundant"."store_s...
                         ^
  [SQL:
  WITH
  sweltering as (
  SELECT
      "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" BETWEEN 2000 AND 2003

  GROUP BY
      1,
      cast("store_sales_date_date"."D_DATE" as date)
  HAVING
      count((cast("store_sales_store_sales"."SS_TICKET_NUMBER" as string) || '-' || cast("store_sales_store_sales"."SS_ITEM_SK" as string))) > 4
  ),
  cooperative as (
  SELECT
      "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
      "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity",
      "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" BETWEEN 2000 AND 2003 and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null
  ),
  abundant as (
  SELECT
      "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
      "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
      "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity",
      "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price"
  FROM
      "store_sales" as "store_sales_store_sales"
      LEFT OUTER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"),
  scrawny as (
  SELECT
      "sweltering"."store_sales_item_id" as "_frequent_items_item_id"
  FROM
      "sweltering"
      INNER JOIN "item" as "store_sales_item_items" on "sweltering"."store_sales_item_id" = "store_sales_item_items"."I_ITEM_SK"),
  uneven as (
  SELECT
      sum("cooperative"."store_sales_quantity" * "cooperative"."store_sales_sales_price") as "_best_threshold_customer_total"
  FROM
      "cooperative"
  GROUP BY
      "cooperative"."store_sales_customer_id"),
  friendly as (
  SELECT
      "scrawny"."_frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "scrawny"),
  yummy as (
  SELECT
      "uneven"."_best_threshold_customer_total" as "best_threshold_customer_total"
  FROM
      "uneven"),
  kaput as (
  SELECT
      "friendly"."frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "friendly"
  GROUP BY
      1),
  juicy as (
  SELECT
      max("yummy"."best_threshold_customer_total") * 0.5 as "best_customer_threshold"
  FROM
      "yummy"),
  vacuous as (
  SELECT
      "abundant"."store_sales_customer_id" as "store_sales_customer_id",
      "abundant"."store_sales_date_year" as "store_sales_date_year",
      CASE WHEN sum("abundant"."store_sales_quantity" * "abundant"."store_sales_sales_price") > "juicy"."best_customer_threshold" and "abundant"."store_sales_date_year" BETWEEN 2000 AND 2003 and "abundant"."store_sales_customer_id" is not null THEN "abundant"."store_sales_customer_id" ELSE NULL END as "_virt_filter_id_390034287534358"
  FROM
      "juicy"
      LEFT OUTER JOIN "abundant" on 1=1
  GROUP BY
      1,
      2,
      3,
      "juicy"."best_customer_threshold"
  HAVING
      sum("abundant"."store_sales_quantity" * "abundant"."store_sales_sales_price") > "juicy"."best_customer_threshold"
  ),
  young as (
  SELECT
      "vacuous"."_virt_filter_id_390034287534358" as "_virt_filter_id_390034287534358"
  FROM
      "vacuous"
  WHERE
      "vacuous"."store_sales_date_year" BETWEEN 2000 AND 2003 and "vacuous"."store_sales_customer_id" is not null
  ),
  questionable as (
  SELECT
      "cooperative"."store_sales_customer_id" as "_best_customers_cust_id"
  FROM
      "cooperative"
  WHERE
      "cooperative"."store_sales_customer_id" in (select young."_virt_filter_id_390034287534358" from young where young."_virt_filter_id_390034287534358" is not null)

  GROUP BY
      1),
  sparkling as (
  SELECT
      "questionable"."_best_customers_cust_id" as "_best_customers_cust_id"
  FROM
      "questionable"),
  abhorrent as (
  SELECT
      "sparkling"."_best_customers_cust_id" as "best_customers_cust_id"
  FROM
      "sparkling"),
  waggish as (
  SELECT
      "web_sales_billing_customer_customers"."C_FIRST_NAME" as "web_sales_billing_customer_first_name",
      "web_sales_billing_customer_customers"."C_LAST_NAME" as "web_sales_billing_customer_last_name",
      "web_sales_web_sales"."WS_LIST_PRICE" as "web_sales_list_price",
      "web_sales_web_sales"."WS_QUANTITY" as "web_sales_quantity"
  FROM
      "web_sales" as "web_sales_web_sales"
      INNER JOIN "date_dim" as "web_sales_date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "web_sales_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "web_sales_billing_customer_customers" on "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" = "web_sales_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "web_sales_date_date"."D_YEAR" = 2000 and "web_sales_date_date"."D_MOY" = 2 and "web_sales_web_sales"."WS_ITEM_SK" in (select kaput."frequent_items_item_id" from kaput where kaput."frequent_items_item_id" is not null) and "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" in (select abhorrent."best_customers_cust_id" from abhorrent where abhorrent."best_customers_cust_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "web_sales_web_sales"."WS_ITEM_SK",
      "web_sales_web_sales"."WS_ORDER_NUMBER"),
  divergent as (
  SELECT
      "catalog_sales_bill_customer_customers"."C_FIRST_NAME" as "catalog_sales_bill_customer_first_name",
      "catalog_sales_bill_customer_customers"."C_LAST_NAME" as "catalog_sales_bill_customer_last_name",
      "catalog_sales_catalog_sales"."CS_LIST_PRICE" as "catalog_sales_list_price",
      "catalog_sales_catalog_sales"."CS_QUANTITY" as "catalog_sales_quantity"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
      INNER JOIN "date_dim" as "catalog_sales_sold_date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sales_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "catalog_sales_bill_customer_customers" on "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" = "catalog_sales_bill_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "catalog_sales_sold_date_date"."D_YEAR" = 2000 and "catalog_sales_sold_date_date"."D_MOY" = 2 and "catalog_sales_catalog_sales"."CS_ITEM_SK" in (select kaput."frequent_items_item_id" from kaput where kaput."frequent_items_item_id" is not null) and "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select abhorrent."best_customers_cust_id" from abhorrent where abhorrent."best_customers_cust_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "catalog_sales_catalog_sales"."CS_ITEM_SK",
      "catalog_sales_catalog_sales"."CS_ORDER_NUMBER"),
  rambunctious as (
  SELECT
      "waggish"."web_sales_billing_customer_first_name" as "___tvf_arm_1_first_name",
      "waggish"."web_sales_billing_customer_last_name" as "___tvf_arm_1_last_name",
      sum("waggish"."web_sales_quantity" * "waggish"."web_sales_list_price") as "___tvf_arm_1_total_sales"
  FROM
      "waggish"
  GROUP BY
      1,
      2),
  busy as (
  SELECT
      "divergent"."catalog_sales_bill_customer_first_name" as "___tvf_arm_0_first_name",
      "divergent"."catalog_sales_bill_customer_last_name" as "___tvf_arm_0_last_name",
      sum("divergent"."catalog_sales_quantity" * "divergent"."catalog_sales_list_price") as "___tvf_arm_0_total_sales"
  FROM
      "divergent"
  GROUP BY
      1,
      2),
  hard as (
  SELECT
      "busy"."___tvf_arm_0_last_name" as "_combined_last_name",
      "busy"."___tvf_arm_0_first_name" as "_combined_first_name",
      "busy"."___tvf_arm_0_total_sales" as "_combined_total_sales"
  FROM
      "busy"
  UNION ALL
  SELECT
      "rambunctious"."___tvf_arm_1_last_name" as "_combined_last_name",
      "rambunctious"."___tvf_arm_1_first_name" as "_combined_first_name",
      "rambunctious"."___tvf_arm_1_total_sales" as "_combined_total_sales"
  FROM
      "rambunctious"),
  sedate as (
  SELECT
      "hard"."_combined_first_name" as "_combined_first_name",
      "hard"."_combined_last_name" as "_combined_last_name",
      "hard"."_combined_total_sales" as "_combined_total_sales"
  FROM
      "hard"),
  yellow as (
  SELECT
      "sedate"."_combined_first_name" as "combined_first_name",
      "sedate"."_combined_last_name" as "combined_last_name",
      "sedate"."_combined_total_sales" as "combined_total_sales"
  FROM
      "sedate")
  SELECT
      "yellow"."combined_last_name" as "combined_last_name",
      "yellow"."combined_first_name" as "combined_first_name",
      "yellow"."combined_total_sales" as "combined_total_sales"
  FROM
      "yellow"
  GROUP BY
      1,
      2,
      3
  ORDER BY
      "yellow"."combined_last_name" asc,
      "yellow"."combined_first_name" asc,
      "yellow"."combined_total_sales" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: (_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!

  LINE 77:     CASE WHEN sum("abundant"."store_sales_quantity" * "abundant"."store_s...
                         ^
  [SQL:
  WITH
  sweltering as (
  SELECT
      "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" BETWEEN 2000 AND 2003

  GROUP BY
      1,
      cast("store_sales_date_date"."D_DATE" as date)
  HAVING
      count((cast("store_sales_store_sales"."SS_TICKET_NUMBER" as string) || '-' || cast("store_sales_store_sales"."SS_ITEM_SK" as string))) > 4
  ),
  cooperative as (
  SELECT
      "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
      "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity",
      "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" BETWEEN 2000 AND 2003 and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null
  ),
  abundant as (
  SELECT
      "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
      "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
      "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity",
      "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price"
  FROM
      "store_sales" as "store_sales_store_sales"
      LEFT OUTER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"),
  scrawny as (
  SELECT
      "sweltering"."store_sales_item_id" as "_frequent_items_item_id"
  FROM
      "sweltering"
      INNER JOIN "item" as "store_sales_item_items" on "sweltering"."store_sales_item_id" = "store_sales_item_items"."I_ITEM_SK"),
  uneven as (
  SELECT
      sum("cooperative"."store_sales_quantity" * "cooperative"."store_sales_sales_price") as "_all_store_customer_totals_customer_total"
  FROM
      "cooperative"
  GROUP BY
      "cooperative"."store_sales_customer_id"),
  friendly as (
  SELECT
      "scrawny"."_frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "scrawny"),
  yummy as (
  SELECT
      "uneven"."_all_store_customer_totals_customer_total" as "all_store_customer_totals_customer_total"
  FROM
      "uneven"),
  kaput as (
  SELECT
      "friendly"."frequent_items_item_id" as "frequent_items_item_id"
  FROM
      "friendly"
  GROUP BY
      1),
  juicy as (
  SELECT
      max("yummy"."all_store_customer_totals_customer_total") * 0.5 as "best_threshold"
  FROM
      "yummy"),
  vacuous as (
  SELECT
      "abundant"."store_sales_customer_id" as "store_sales_customer_id",
      "abundant"."store_sales_date_year" as "store_sales_date_year",
      CASE WHEN sum("abundant"."store_sales_quantity" * "abundant"."store_sales_sales_price") > "juicy"."best_threshold" and "abundant"."store_sales_date_year" BETWEEN 2000 AND 2003 and "abundant"."store_sales_customer_id" is not null THEN "abundant"."store_sales_customer_id" ELSE NULL END as "_virt_filter_id_6328804181652567"
  FROM
      "juicy"
      LEFT OUTER JOIN "abundant" on 1=1
  GROUP BY
      1,
      2,
      3,
      "juicy"."best_threshold"
  HAVING
      sum("abundant"."store_sales_quantity" * "abundant"."store_sales_sales_price") > "juicy"."best_threshold"
  ),
  young as (
  SELECT
      "vacuous"."_virt_filter_id_6328804181652567" as "_virt_filter_id_6328804181652567"
  FROM
      "vacuous"
  WHERE
      "vacuous"."store_sales_date_year" BETWEEN 2000 AND 2003 and "vacuous"."store_sales_customer_id" is not null
  ),
  questionable as (
  SELECT
      "cooperative"."store_sales_customer_id" as "_best_customers_cust_id"
  FROM
      "cooperative"
  WHERE
      "cooperative"."store_sales_customer_id" in (select young."_virt_filter_id_6328804181652567" from young where young."_virt_filter_id_6328804181652567" is not null)

  GROUP BY
      1),
  sparkling as (
  SELECT
      "questionable"."_best_customers_cust_id" as "_best_customers_cust_id"
  FROM
      "questionable"),
  abhorrent as (
  SELECT
      "sparkling"."_best_customers_cust_id" as "best_customers_cust_id"
  FROM
      "sparkling"),
  waggish as (
  SELECT
      "web_sales_billing_customer_customers"."C_FIRST_NAME" as "web_sales_billing_customer_first_name",
      "web_sales_billing_customer_customers"."C_LAST_NAME" as "web_sales_billing_customer_last_name",
      "web_sales_web_sales"."WS_LIST_PRICE" as "web_sales_list_price",
      "web_sales_web_sales"."WS_QUANTITY" as "web_sales_quantity"
  FROM
      "web_sales" as "web_sales_web_sales"
      INNER JOIN "date_dim" as "web_sales_date_date" on "web_sales_web_sales"."WS_SOLD_DATE_SK" = "web_sales_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "web_sales_billing_customer_customers" on "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" = "web_sales_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "web_sales_date_date"."D_YEAR" = 2000 and "web_sales_date_date"."D_MOY" = 2 and "web_sales_web_sales"."WS_ITEM_SK" in (select kaput."frequent_items_item_id" from kaput where kaput."frequent_items_item_id" is not null) and "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" in (select abhorrent."best_customers_cust_id" from abhorrent where abhorrent."best_customers_cust_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "web_sales_web_sales"."WS_ITEM_SK",
      "web_sales_web_sales"."WS_ORDER_NUMBER"),
  divergent as (
  SELECT
      "catalog_sales_bill_customer_customers"."C_FIRST_NAME" as "catalog_sales_bill_customer_first_name",
      "catalog_sales_bill_customer_customers"."C_LAST_NAME" as "catalog_sales_bill_customer_last_name",
      "catalog_sales_catalog_sales"."CS_LIST_PRICE" as "catalog_sales_list_price",
      "catalog_sales_catalog_sales"."CS_QUANTITY" as "catalog_sales_quantity"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
      INNER JOIN "date_dim" as "catalog_sales_sold_date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sales_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "catalog_sales_bill_customer_customers" on "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" = "catalog_sales_bill_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "catalog_sales_sold_date_date"."D_YEAR" = 2000 and "catalog_sales_sold_date_date"."D_MOY" = 2 and "catalog_sales_catalog_sales"."CS_ITEM_SK" in (select kaput."frequent_items_item_id" from kaput where kaput."frequent_items_item_id" is not null) and "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select abhorrent."best_customers_cust_id" from abhorrent where abhorrent."best_customers_cust_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "catalog_sales_catalog_sales"."CS_ITEM_SK",
      "catalog_sales_catalog_sales"."CS_ORDER_NUMBER"),
  rambunctious as (
  SELECT
      "waggish"."web_sales_billing_customer_first_name" as "___tvf_arm_1_first_name",
      "waggish"."web_sales_billing_customer_last_name" as "___tvf_arm_1_last_name",
      sum("waggish"."web_sales_quantity" * "waggish"."web_sales_list_price") as "___tvf_arm_1_total_sales"
  FROM
      "waggish"
  GROUP BY
      1,
      2),
  busy as (
  SELECT
      "divergent"."catalog_sales_bill_customer_first_name" as "___tvf_arm_0_first_name",
      "divergent"."catalog_sales_bill_customer_last_name" as "___tvf_arm_0_last_name",
      sum("divergent"."catalog_sales_quantity" * "divergent"."catalog_sales_list_price") as "___tvf_arm_0_total_sales"
  FROM
      "divergent"
  GROUP BY
      1,
      2),
  hard as (
  SELECT
      "busy"."___tvf_arm_0_last_name" as "_combined_last_name",
      "busy"."___tvf_arm_0_first_name" as "_combined_first_name",
      "busy"."___tvf_arm_0_total_sales" as "_combined_total_sales"
  FROM
      "busy"
  UNION ALL
  SELECT
      "rambunctious"."___tvf_arm_1_last_name" as "_combined_last_name",
      "rambunctious"."___tvf_arm_1_first_name" as "_combined_first_name",
      "rambunctious"."___tvf_arm_1_total_sales" as "_combined_total_sales"
  FROM
      "rambunctious"),
  sedate as (
  SELECT
      "hard"."_combined_first_name" as "_combined_first_name",
      "hard"."_combined_last_name" as "_combined_last_name",
      "hard"."_combined_total_sales" as "_combined_total_sales"
  FROM
      "hard"),
  yellow as (
  SELECT
      "sedate"."_combined_first_name" as "combined_first_name",
      "sedate"."_combined_last_name" as "combined_last_name",
      "sedate"."_combined_total_sales" as "combined_total_sales"
  FROM
      "sedate")
  SELECT
      "yellow"."combined_last_name" as "combined_last_name",
      "yellow"."combined_first_name" as "combined_first_name",
      "yellow"."combined_total_sales" as "combined_total_sales"
  FROM
      "yellow"
  GROUP BY
      1,
      2,
      3
  ORDER BY
      "yellow"."combined_last_name" asc,
      "yellow"."combined_first_name" asc,
      "yellow"."combined_total_sales" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query24.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query27.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query29.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: 11 undefined concept references; fix all before re-running:
    - salutation (line 29, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.salutation, web_returns.web_sales.ship_customer.salutation, web_returns.web_sales.return_customer.salutation, web_returns.web_sales.return_refund_customer.salutation, web_returns.billing_customer.salutation, web_returns.refunded_customer.salutation?
    - first_name (line 30, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.first_name, web_returns.web_sales.ship_customer.first_name, web_returns.web_sales.return_customer.first_name, web_returns.web_sales.return_refund_customer.first_name, web_returns.billing_customer.first_name, web_returns.refunded_customer.first_name?
    - last_name (line 31, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.last_name, web_returns.web_sales.ship_customer.last_name, web_returns.web_sales.return_customer.last_name, web_returns.web_sales.return_refund_customer.last_name, web_returns.billing_customer.last_name, web_returns.refunded_customer.last_name?
    - preferred_cust_flag (line 32, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.preferred_cust_flag, web_returns.web_sales.ship_customer.preferred_cust_flag, web_returns.web_sales.return_customer.preferred_cust_flag, web_returns.web_sales.return_refund_customer.preferred_cust_flag, web_returns.billing_customer.preferred_cust_flag, web_returns.refunded_customer.preferred_cust_flag?
    - birth_day (line 33, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_day, web_returns.web_sales.ship_customer.birth_day, web_returns.web_sales.return_customer.birth_day, web_returns.web_sales.return_refund_customer.birth_day, web_returns.billing_customer.birth_day, web_returns.refunded_customer.birth_day?
    - birth_month (line 34, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_month, web_returns.web_sales.ship_customer.birth_month, web_returns.web_sales.return_customer.birth_month, web_returns.web_sales.return_refund_customer.birth_month, web_returns.billing_customer.birth_month, web_returns.refunded_customer.birth_month?
    - birth_year (line 35, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_year, web_returns.web_sales.ship_customer.birth_year, web_returns.web_sales.return_customer.birth_year, web_returns.web_sales.return_refund_customer.birth_year, web_returns.billing_customer.birth_year, web_returns.refunded_customer.birth_year?
    - birth_country (line 36, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_country, web_returns.web_sales.ship_customer.birth_country, web_returns.web_sales.return_customer.birth_country, web_returns.web_sales.return_refund_customer.birth_country, web_returns.billing_customer.birth_country, web_returns.refunded_customer.birth_country?
    - login (line 37, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.login, web_returns.web_sales.ship_customer.login, web_returns.web_sales.return_customer.login, web_returns.web_sales.return_refund_customer.login, web_returns.billing_customer.login, web_returns.refunded_customer.login?
    - email_address (line 38, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.email_address, web_returns.web_sales.ship_customer.email_address, web_returns.web_sales.return_customer.email_address, web_returns.web_sales.return_refund_customer.email_address, web_returns.billing_customer.email_address, web_returns.refunded_customer.email_address?
    - last_review_date (line 39, col 5, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.last_review_date, web_returns.web_sales.ship_customer.last_review_date, web_returns.web_sales.return_customer.last_review_date, web_returns.web_sales.return_refund_customer.last_review_date, web_returns.billing_customer.last_review_date, web_returns.refunded_customer.last_review_date?
  ```
- `trilogy run query31.preql duckdb`

  ```text
  Syntax error in query31.preql: HAVING references 'web_pivot.web_q2_over_q1', 'store_pivot.store_q2_over_q1', 'web_pivot.web_q3_over_q2', 'store_pivot.store_q3_over_q2', which are not defined (line 41). Check for a typo or import the relevant concept.
  ```
- `trilogy run query38.preql`

  ```text
  Unexpected error in query38.preql: Invalid reference string found in query:
  WITH
  highfalutin as (
  SELECT
      "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" as "catalog_sales_bill_customer_id",
      "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" as "catalog_sales_sold_date_id"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
  GROUP BY
      1,
      2),
  thoughtful as (
  SELECT
      "catalog_sales_bill_customer_customers"."C_FIRST_NAME" as "_catalog_combos_first_name",
      "catalog_sales_bill_customer_customers"."C_LAST_NAME" as "_catalog_combos_last_name",
      cast("catalog_sales_sold_date_date"."D_DATE" as date) as "_catalog_combos_sale_date"
  FROM
      "highfalutin"
      INNER JOIN "date_dim" as "catalog_sales_sold_date_date" on "highfalutin"."catalog_sales_sold_date_id" = "catalog_sales_sold_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "catalog_sales_bill_customer_customers" on "highfalutin"."catalog_sales_bill_customer_id" = "catalog_sales_bill_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "catalog_sales_sold_date_date"."D_YEAR" = 2000

  GROUP BY
      1,
      2,
      3),
  cooperative as (
  SELECT
      INVALID_REFERENCE_BUG as "store_and_catalog_last_name"
  FROM
      "thoughtful"
  WHERE
      "thoughtful"."_catalog_combos_last_name" = "thoughtful"."_catalog_combos_last_name" and "thoughtful"."_catalog_combos_first_name" = "thoughtful"."_catalog_combos_first_name" and "thoughtful"."_catalog_combos_sale_date" = "thoughtful"."_catalog_combos_sale_date"

  GROUP BY
      1)
  SELECT
      count("cooperative"."store_and_catalog_last_name") as "unique_count"
  FROM
      "cooperative"
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query38.preql`

  ```text
  Syntax error in query38.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 35). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy run query39.preql`

  ```text
  Syntax error in query39.preql: Undefined concept: jan.warehouse_id. Suggestions: ['agg_data.warehouse_id', 'qualified.agg_data.warehouse_id', 'feb.warehouse_id', 'jan_warehouse_id', 'inv.warehouse.id']
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 4 disconnected subgraphs: {d.year, local._agg_month}; {local._agg_item_id}; {local._agg_mean_qty, local._agg_std_qty}; {local._agg_warehouse_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query41.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query43.preql`

  ```text
  Syntax error in query43.preql: Undefined concept: date.day_of_week. Suggestions: ['ss.date.day_of_week', 'ss.store.date.day_of_week', 'ss.return_store.date.day_of_week', 'ss.return_date.day_of_week', 'ss.customer.first_sales_date.day_of_week', 'ss.customer.first_shipto_date.day_of_week']
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 46). The requested concepts split into 2 disconnected subgraphs: {best.product_name, best.rnk}; {worst.product_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query48.preql`

  ```text
  Syntax error in query48.preql: Comparison `store_sales.customer_demographic.marital_status = 'Married'` can never match enum field 'store_sales.customer_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read query50.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query51.preql`

  ```text
  Syntax error in query51.preql: None
  ```
- `trilogy run query58.preql`

  ```text
  Syntax error in query58.preql: Undefined concept: local.catalog_totals. Suggestions: ['catalog_totals.item_code', '_catalog_totals_item_code', 'catalog_pct_of_avg']
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 35). The requested concepts split into 2 disconnected subgraphs: {next_year.fri, next_year.mon, next_year.sat, next_year.store_code, next_year.store_name, next_year.sun, next_year.thu, next_year.tue, next_year.wed, next_year.wk_seq}; {this_year.fri, this_year.mon, this_year.sat, this_year.store_code, this_year.store_name, this_year.sun, this_year.thu, this_year.tue, this_year.wed, this_year.wk_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query61.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query63.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 20). The requested concepts split into 2 disconnected subgraphs: {cs.item.text_id, local.coupon_amt_sum, local.cust_city, local.cust_street_name, local.cust_street_number, local.cust_zip, local.first_sales_year, local.first_shipto_year, local.line_count, local.list_price_sum, local.sale_city, local.sale_street_name, local.sale_street_number, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.wholesale_cost_sum, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.item.color, ss.item.current_price, ss.item.id, ss.item.product_name, ss.ticket_number}; {sr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {cs.item.text_id, local._cat_qualifying_items_cat_list_sum}; {local._cat_qualifying_items_cat_refund_sum}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: ss_1999.sale_year. Suggestions: ['ss_1999.first_sales_year', 'ss_1999.sale_city', 'ss_1999.sale_zip']
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
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
- `trilogy file read query71.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query74.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Output column 'prev_qty' renames 'local.prev_qty' back to the name of an existing concept 'prev_qty' (defined at line 30) that 'local.prev_qty' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'prev_qty_out').
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: agg.deduped.year. Suggestions: ['all_sales.date.year', 'all_sales.return_date.year', 'all_sales.billing_customer.first_sales_date.year', 'all_sales.billing_customer.first_shipto_date.year', 'all_sales.ship_customer.first_sales_date.year', 'all_sales.ship_customer.first_shipto_date.year']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: deduped.all_sales.date.year. Suggestions: ['all_sales.date.year', 'all_sales.return_date.year', 'all_sales.billing_customer.first_sales_date.year', 'all_sales.billing_customer.first_shipto_date.year', 'all_sales.ship_customer.first_sales_date.year', 'all_sales.ship_customer.first_shipto_date.year']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: agg.deduped.year. Suggestions: ['all_sales.date.year', 'all_sales.return_date.year', 'all_sales.billing_customer.first_sales_date.year', 'all_sales.billing_customer.first_shipto_date.year', 'all_sales.ship_customer.first_sales_date.year', 'all_sales.ship_customer.first_shipto_date.year']
  ```
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: ORDER BY references 'local._virt_agg_sum_8929275053401891', which is not in the SELECT projection (line 5). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local._virt_agg_sum_8929275053401891 order by local._virt_agg_sum_8929275053401891 asc`.
  ```
- `trilogy run query75.preql`

  ```text
  Unexpected error in query75.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_returns_unified"."CR_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "all_sales_return_amount",
      "all_sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "all_sales_return_quantity"
  FROM
      "catalog_returns" as "all_sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_returns_unified"."SR_ITEM_SK" as "all_sales_item_id",
      "all_sales_store_returns_unified"."SR_TICKET_NUMBER" as "all_sales_order_id",
      "all_sales_store_returns_unified"."SR_RETURN_AMT" as "all_sales_return_amount",
      "all_sales_store_returns_unified"."SR_RETURN_QUANTITY" as "all_sales_return_quantity"
  FROM
      "store_returns" as "all_sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_returns_unified"."WR_ITEM_SK" as "all_sales_item_id",
      "all_sales_web_returns_unified"."WR_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_web_returns_unified"."WR_RETURN_AMT" as "all_sales_return_amount",
      "all_sales_web_returns_unified"."WR_RETURN_QUANTITY" as "all_sales_return_quantity"
  FROM
      "web_returns" as "all_sales_web_returns_unified"),
  abundant as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_catalog_sales_unified"."CS_QUANTITY" as "all_sales_quantity",
      "all_sales_date_date"."D_YEAR" as "all_sales_date_year"
  FROM
      "catalog_sales" as "all_sales_catalog_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "all_sales_item_items" on "all_sales_catalog_sales_unified"."CS_ITEM_SK" = "all_sales_item_items"."I_ITEM_SK"
  WHERE
      "all_sales_date_date"."D_YEAR" in (2001,2002) and "all_sales_item_items"."I_CATEGORY" = 'Books'

  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_store_sales_unified"."SS_ITEM_SK" as "all_sales_item_id",
      "all_sales_store_sales_unified"."SS_TICKET_NUMBER" as "all_sales_order_id",
      "all_sales_store_sales_unified"."SS_QUANTITY" as "all_sales_quantity",
      "all_sales_date_date"."D_YEAR" as "all_sales_date_year"
  FROM
      "store_sales" as "all_sales_store_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_store_sales_unified"."SS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "all_sales_item_items" on "all_sales_store_sales_unified"."SS_ITEM_SK" = "all_sales_item_items"."I_ITEM_SK"
  WHERE
      "all_sales_date_date"."D_YEAR" in (2001,2002) and "all_sales_item_items"."I_CATEGORY" = 'Books'

  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_web_sales_unified"."WS_ITEM_SK" as "all_sales_item_id",
      "all_sales_web_sales_unified"."WS_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_web_sales_unified"."WS_QUANTITY" as "all_sales_quantity",
      "all_sales_date_date"."D_YEAR" as "all_sales_date_year"
  FROM
      "web_sales" as "all_sales_web_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_web_sales_unified"."WS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "all_sales_item_items" on "all_sales_web_sales_unified"."WS_ITEM_SK" = "all_sales_item_items"."I_ITEM_SK"
  WHERE
      "all_sales_date_date"."D_YEAR" in (2001,2002) and "all_sales_item_items"."I_CATEGORY" = 'Books'
  ),
  juicy as (
  SELECT
      "abundant"."all_sales_date_year" as "_agg_yr",
      "all_sales_item_items"."I_BRAND_ID" as "_agg_brand_id",
      "all_sales_item_items"."I_CATEGORY_ID" as "_agg_category_id",
      "all_sales_item_items"."I_CLASS_ID" as "_agg_class_id",
      "all_sales_item_items"."I_MANUFACT_ID" as "_agg_manufacturer_id",
      sum(coalesce("abundant"."all_sales_ext_sales_price",0) - coalesce("cheerful"."all_sales_return_amount",0)) as "_agg_ta",
      sum(coalesce("abundant"."all_sales_quantity",0) - coalesce("cheerful"."all_sales_return_quantity",0)) as "_agg_tq"
  FROM
      "abundant"
      INNER JOIN "item" as "all_sales_item_items" on "abundant"."all_sales_item_id" = "all_sales_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "cheerful" on "abundant"."all_sales_channel" = "cheerful"."all_sales_channel" AND "abundant"."all_sales_item_id" = "cheerful"."all_sales_item_id" AND "abundant"."all_sales_order_id" = "cheerful"."all_sales_order_id"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  concerned as (
  SELECT
      "juicy"."_agg_brand_id" as "agg_brand_id",
      "juicy"."_agg_category_id" as "agg_category_id",
      "juicy"."_agg_class_id" as "agg_class_id",
      "juicy"."_agg_manufacturer_id" as "agg_manufacturer_id",
      "juicy"."_agg_ta" as "agg_ta",
      "juicy"."_agg_tq" as "agg_tq",
      "juicy"."_agg_yr" as "agg_yr"
  FROM
      "juicy"),
  sweltering as (
  SELECT
      "concerned"."agg_brand_id" as "_virt_filter_brand_id_2488385400655448",
      "concerned"."agg_category_id" as "_virt_filter_category_id_8894165433708692",
      "concerned"."agg_class_id" as "_virt_filter_class_id_4476623903564469",
      "concerned"."agg_manufacturer_id" as "_virt_filter_manufacturer_id_750977489592690",
      "concerned"."agg_yr" as "_virt_filter_yr_5966650411026037"
  FROM
      "concerned"
  WHERE
      "concerned"."agg_yr" = 2002
  ),
  young as (
  SELECT
      "concerned"."agg_brand_id" as "agg_brand_id",
      "concerned"."agg_category_id" as "agg_category_id",
      "concerned"."agg_class_id" as "agg_class_id",
      "concerned"."agg_manufacturer_id" as "agg_manufacturer_id",
      "concerned"."agg_ta" as "agg_ta",
      "concerned"."agg_tq" as "agg_tq",
      lag("concerned"."agg_ta", 1) over (partition by "concerned"."agg_brand_id","concerned"."agg_class_id","concerned"."agg_category_id","concerned"."agg_manufacturer_id" order by "concerned"."agg_yr" asc ) as "_virt_window_lag_4654692358013776",
      lag("concerned"."agg_ta", 1) over (partition by "concerned"."agg_brand_id","concerned"."agg_class_id","concerned"."agg_category_id","concerned"."agg_manufacturer_id" order by "concerned"."agg_yr" asc ) as "prev_amt",
      lag("concerned"."agg_tq", 1) over (partition by "concerned"."agg_brand_id","concerned"."agg_class_id","concerned"."agg_category_id","concerned"."agg_manufacturer_id" order by "concerned"."agg_yr" asc ) as "_virt_window_lag_9882665409836596",
      lag("concerned"."agg_tq", 1) over (partition by "concerned"."agg_brand_id","concerned"."agg_class_id","concerned"."agg_category_id","concerned"."agg_manufacturer_id" order by "concerned"."agg_yr" asc ) as "prev_qty"
  FROM
      "concerned"),
  sparkling as (
  SELECT
      "young"."agg_brand_id" as "brand_id",
      "young"."agg_category_id" as "category_id",
      "young"."agg_class_id" as "class_id",
      "young"."agg_manufacturer_id" as "manufacturer_id",
      "young"."agg_ta" - "young"."_virt_window_lag_4654692358013776" as "amt_diff",
      "young"."agg_ta" as "curr_amt",
      "young"."agg_tq" - "young"."_virt_window_lag_9882665409836596" as "qty_diff",
      "young"."agg_tq" as "curr_qty",
      "young"."prev_amt" as "prev_amt",
      "young"."prev_qty" as "prev_qty",
      2001 as "prev_year",
      2002 as "curr_year"
  FROM
      "young"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      "young"."_virt_window_lag_4654692358013776",
      "young"."_virt_window_lag_9882665409836596"),
  abhorrent as (
  SELECT
      "sparkling"."amt_diff" as "amt_diff",
      "sparkling"."brand_id" as "brand_id",
      "sparkling"."category_id" as "category_id",
      "sparkling"."class_id" as "class_id",
      "sparkling"."curr_amt" as "curr_amt",
      "sparkling"."curr_qty" as "curr_qty",
      "sparkling"."curr_year" as "curr_year",
      "sparkling"."manufacturer_id" as "manufacturer_id",
      "sparkling"."prev_amt" as "prev_amt",
      "sparkling"."prev_qty" as "prev_qty",
      "sparkling"."prev_year" as "prev_year",
      "sparkling"."qty_diff" as "qty_diff"
  FROM
      "sparkling"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12)
  SELECT
      "abhorrent"."prev_year" as "prev_year",
      "abhorrent"."curr_year" as "curr_year",
      "abhorrent"."brand_id" as "brand_id",
      "abhorrent"."class_id" as "class_id",
      "abhorrent"."category_id" as "category_id",
      "abhorrent"."manufacturer_id" as "manufacturer_id",
      "abhorrent"."prev_qty" as "prev_qty",
      "abhorrent"."prev_amt" as "prev_amt",
      "abhorrent"."curr_qty" as "curr_qty",
      "abhorrent"."curr_amt" as "curr_amt",
      "abhorrent"."qty_diff" as "qty_diff",
      "abhorrent"."amt_diff" as "amt_diff"
  FROM
      "abhorrent"
  WHERE
      (INVALID_REFERENCE_BUG, INVALID_REFERENCE_BUG, INVALID_REFERENCE_BUG, INVALID_REFERENCE_BUG, INVALID_REFERENCE_BUG) in (select sweltering."_virt_filter_brand_id_2488385400655448", sweltering."_virt_filter_category_id_8894165433708692", sweltering."_virt_filter_class_id_4476623903564469", sweltering."_virt_filter_manufacturer_id_750977489592690", sweltering."_virt_filter_yr_5966650411026037" from sweltering where sweltering."_virt_filter_brand_id_2488385400655448" is not null and sweltering."_virt_filter_category_id_8894165433708692" is not null and sweltering."_virt_filter_class_id_4476623903564469" is not null and sweltering."_virt_filter_manufacturer_id_750977489592690" is not null and sweltering."_virt_filter_yr_5966650411026037" is not null) and "abhorrent"."prev_qty" is not null and "abhorrent"."prev_qty" > 0 and ( "abhorrent"."curr_qty" / "abhorrent"."prev_qty" ) < 0.9

  ORDER BY
      "abhorrent"."qty_diff" asc,
      "abhorrent"."amt_diff" asc
  LIMIT (100), this should never occur. Please create an issue to report this.
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
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `grouping(local.channel)` (line 6), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel) as g order by g desc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 105 column 12 (char 3219). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 50). The requested concepts split into 3 disconnected subgraphs: {catalog_agg.catalog_qty, catalog_agg.catalog_sales_price, catalog_agg.catalog_wholesale_cost}; {store_agg.customer_id, store_agg.item_id, store_agg.store_qty, store_agg.store_sales_price, store_agg.store_wholesale_cost, store_agg.year}; {web_agg.customer_id, web_agg.item_id, web_agg.web_qty, web_agg.web_sales_price, web_agg.web_wholesale_cost, web_agg.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: Ambiguous reference 'store_web.year': matches ['store_web.store_agg.year', 'store_web.web_agg.year']. Qualify the full path to disambiguate.
  ```
- `trilogy file read query78.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 38 column 12 (char 1387). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw.web_returns:wr -- select wr.item.text_id, sum(wr.return_quantity) as web_qty, count(wr.web_sales.order_number) as web_rows where wr.return_date.week_seq in (5244,5257,5264) order by wr.item.text_id limit 5;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - wr.item.text_id (line 2, col 8, in SELECT); did you mean: wr.web_sales.item.text_id, wr.store.date.text_id, wr.store.text_id, wr.web_sales.date.text_id, wr.web_sales.ship_date.text_id, wr.web_sales.return_date.text_id?
    - wr.item.text_id (line 2, col 165, in ORDER BY); did you mean: wr.web_sales.item.text_id, wr.store.date.text_id, wr.store.text_id, wr.web_sales.date.text_id, wr.web_sales.ship_date.text_id, wr.web_sales.return_date.text_id?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query94.preql`

  ```text
  Syntax error in query94.preql: Undefined concept: warehouse.id. Suggestions: ['ws.warehouse.id', 'ws.item.id', 'ws.date.id', 'ws.ship_date.id', 'ws.return_date.id', 'ws.time.id']
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs where ss.date.year = 2000 and ss.customer.id is not null and cs.date.year = 2000 and cs.bill_customer.id is not null select count(ss.customer.id) as ss_cnt, count(cs.bill_customer.id) as cs_cnt limit 100;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cs.bill_customer.id, cs.date.year}; {ss.customer.id, ss.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query97.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query99.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Combined web + catalog sales by (week_seq, day_of_week)
# Use a window to get ne…t null  # Only week_seqs that have a 53-week-ahead value
  and with_next.ws between 5270 and 5322  # Only 2001 week sequences
order by with_next.ws;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...n_ratio(ws_val) -> round(     ??? (select with_next.amt where wi...

  Write stats: received 2771 chars / 2771 bytes; tail: …'and 5322  # Only 2001 week sequences\\norder by with_next.ws;\\n'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_id, sum(all_sales.ext_sales_price) as gross_sales by rollup (all_sales.channel, all_sales.channel_dim_id) where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date limit 30;`

  ```text
  Syntax error in stdin:  --> 2:155
    |
  2 | select all_sales.channel, all_sales.channel_dim_id, sum(all_sales.ext_sales_price) as gross_sales by rollup (all_sales.channel, all_sales.channel_dim_id) where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date limit 30;
    |                                                                                                                                                           ^---
    |
    = expected limit or order_by
  Location:
  ...el, all_sales.channel_dim_id) ??? where all_sales.date.date betw...
  ```
- `trilogy run --import raw.all_sales:all_sales with test_data as (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date select all_sales.…ct test_data.channel, test_data.channel_dim_id, sum(test_data.gross_sales) as total by rollup (test_data.channel, test_data.channel_dim_id) limit 10;`

  ```text
  Syntax error in stdin:  --> 2:19
    |
  2 | with test_data as (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date select all_sales.channel, all_sales.channel_dim_id, sum(all_sales.ext_sales_price) as gross_sales) select test_data.channel, test_data.channel_dim_id, sum(test_data.gross_sales) as total by rollup (test_data.channel, test_data.channel_dim_id) limit 10;
    |                   ^---
    |
    = expected select_statement or tvf_union_invocation
  Location:
   all_sales; with test_data as ??? (where all_sales.date.date bet...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.channel_dim_id, sum(all_sales.ext_sales_price) as gross_sales by rollup (all_sales.channel, all_sales.channel_dim_id) where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date limit 5;`

  ```text
  Syntax error in stdin:  --> 2:155
    |
  2 | select all_sales.channel, all_sales.channel_dim_id, sum(all_sales.ext_sales_price) as gross_sales by rollup (all_sales.channel, all_sales.channel_dim_id) where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date limit 5;
    |                                                                                                                                                           ^---
    |
    = expected limit or order_by
  Location:
  ...el, all_sales.channel_dim_id) ??? where all_sales.date.date betw...
  ```
- `trilogy run --import raw.all_sales:all_sales 
with sales_arm as
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
  and all_sales.c…tity_id) as g_eid
by rollup (combined.channel, combined.entity_id)
where grouping(combined.channel) > 0 or grouping(combined.entity_id) > 0
limit 20;`

  ```text
  Syntax error in stdin:   --> 37:1
     |
  37 | where grouping(combined.channel) > 0 or grouping(combined.entity_id) > 0
     | ^---
     |
     = expected limit or order_by
  Location:
  ....channel, combined.entity_id) ??? where grouping(combined.channe...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id, all_sales.channel, count(all_sales.sale_line_item_counter) as cnt where all_sales.date.year between 1999 and 2001 group by 1,2,3,4 order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...te.year between 1999 and 2001 ??? group by 1,2,3,4 order by cnt
  ```
- `trilogy file write query16.preql --content import raw/catalog_sales as cs;

where
  cs.ship_date.date between '2002-02-01'::date and '2002-04-02'::date
  and… as order_count,
  sum(cs.ext_ship_cost) as total_extended_ship_cost,
  sum(cs.net_profit) as total_net_profit
order by
  order_count desc
limit 100;`

  ```text
  refused to write 'query16.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  wher...

  Write stats: received 513 chars / 513 bytes; tail: …') as total_net_profit\\norder by\\n  order_count desc\\nlimit 100;'.
  ```
- `trilogy file write query19.preql --content import raw.store_sales as store_sales;

where
  store_sales.item.manager_id = 8
  and year(store_sales.date.date) …ore_sales.ext_sales_price) as total_extended_sales_price
order by
  total_extended_sales_price desc,
  brand_id asc,
  manufacturer_id asc
limit 100;`

  ```text
  refused to write 'query19.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:20
    |
  6 |   and month_of_year(store_sales.date.date) = 11
    |                    ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...te) = 1998   and month_of_year ??? (store_sales.date.date) = 11

  Write stats: received 622 chars / 622 bytes; tail: …'price desc,\\n  brand_id asc,\\n  manufacturer_id asc\\nlimit 100;'.
  ```
- `trilogy file write query23.preql -e -c import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
i…ame,
    combined.first_name,
    combined.total_sales,
order by combined.last_name, combined.first_name, combined.total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:38
     |
  15 |     --count(store_sales.ticket_number, store_sales.item.id) as pair_count,
     |                                      ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ount(store_sales.ticket_number ??? , store_sales.item.id) as pair...

  Write stats: received 2533 chars / 2533 bytes; tail: …'ined.first_name, combined.total_sales nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql -e -c import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

…name,
    combined.first_name,
    combined.total_sales
order by combined.last_name, combined.first_name, combined.total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...total having customer_total > ??? (select max_total.best_custome...

  Write stats: received 2599 chars / 2599 bytes; tail: …'ined.first_name, combined.total_sales nulls first\\nlimit 100;'.
  ```
- `trilogy file write query24.preql -e -c import raw.store_sales as ss;

where ss.store.market = 8
  and ss.customer.address.zip = ss.store.zip
  and lower(ss.c…_totals
having
    peach_sum > 0.05 * avg_all_sub_totals
order by
    ss.customer.last_name,
    ss.customer.first_name,
    ss.store.name
limit 100;`

  ```text
  refused to write 'query24.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
     and ss.is_returned = true  ??? auto sub_total <- sum(ss.net_p...

  Write stats: received 796 chars / 796 bytes; tail: …'me,\\n    ss.customer.first_name,\\n    ss.store.name\\nlimit 100;'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Eight detailed attribute profiles: (category, color, units, size)
auto profile1 <- (it…item.manufacturer_id between 1 and 500
  and manufacturer_with_shared
select distinct
    item.product_name
order by
    item.product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...nufacturer_with_shared select ??? distinct     item.product_name...

  Write stats: received 1641 chars / 1641 bytes; tail: …' item.product_name\\norder by\\n    item.product_name\\nlimit 100;'.
  ```
- `trilogy file write query53.preql --content import raw.store_sales as store_sales;

# Step 1: per-quarter totals per manufacturer
with per_qtr as
where store_…ce,
    --per_qtr.avg_q as avg_quarterly_sales
    --avg_q
order by
    --avg_q,
    per_qtr.total_unit_price,
    per_qtr.manufacturer_id
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:5
     |
  29 |     --avg_q
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ..._q as avg_quarterly_sales     ??? --avg_q order by     --avg_q,

  Write stats: received 1260 chars / 1260 bytes; tail: …'qtr.total_unit_price,\\n    per_qtr.manufacturer_id\\nlimit 100;'.
  ```
- `trilogy file write query54.preql --content import raw/store_sales as ss;
import raw/catalog_sales as cs;
import raw/web_sales as ws;
import raw/customer as c…,
    segment * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as ss; import

  Write stats: received 1634 chars / 1634 bytes; tail: …'_count asc nulls first,\\n    segment_times_50 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query54.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.customer as c…,
    segment * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:29
     |
  19 | auto qual_custs <- cat_cust union web_cust;
     |                             ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...r auto qual_custs <- cat_cust ??? union web_cust;  # For each qu...

  Write stats: received 1515 chars / 1515 bytes; tail: …'_count asc nulls first,\\n    segment_times_50 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Rowset: items w…mt) as coupon_amt_sum
order by
    ss.item.product_name,
    ss.store.name,
    line_count,
    wholesale_cost_sum,
    wholesale_cost_sum
limit 100;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset cat_qualifying_items as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...2 * catalog cumulative refund ??? rowset cat_qualifying_items as...

  Write stats: received 1788 chars / 1788 bytes; tail: …'t,\\n    wholesale_cost_sum,\\n    wholesale_cost_sum\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales select warehouse.name, warehouse.square_feet, warehouse.city, warehouse.county, warehouse.state, warehouse.country, date.y…e.time between 30838 and 59638 and ship_mode.carrier in ('DHL','BARIAN') and warehouse.id is not null group by 1,2,3,4,5,6,7,8 order by 1,7 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   and warehouse.id is not null ??? group by 1,2,3,4,5,6,7,8 order...
  ```
- `trilogy file write query75.preql --content import raw.all_sales as all_sales;

# Filter to Books category, compute per-line net metrics
# Deduplicate at the … prev_amt as amt_diff,
having
    prev_qty > 0
    and (agg.total_qty * 1.0 / prev_qty) < 0.9
order by
    qty_diff asc,
    amt_diff asc
limit 100;
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset deduped as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ry_id, manufacturer_id) grain ??? rowset deduped as where all_sa...

  Write stats: received 1797 chars / 1797 bytes; tail: …' 0.9\\norder by\\n    qty_diff asc,\\n    amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:all_sales where all_sales.item.category = 'Books' select all_sales.item.category_id, count(*) as cnt, sum(all_sales.quantity) as tot_qty, all_sales.date.year as yr group by all_sales.date.year, all_sales.item.category_id order by yr asc;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...t all_sales.item.category_id, ??? count(*) as cnt, sum(all_sales...
  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…mbined.total_ext_sales_price
order by
  combined.channel,
  combined.missing_ref,
  combined.year,
  combined.quarter,
  combined.category
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` — a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ???  select   combined.channel,

  Write stats: received 1447 chars / 1447 bytes; tail: …'ned.year,\\n  combined.quarter,\\n  combined.category\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Question 77: sales, returns, and profit per channel and per outlet for 2000-…ollup (channel, all_sales.outlet_id)
order by
  --sort_level asc,
  channel nulls first,
  all_sales.outlet_id nulls first,
  returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:4
     |
  23 |   --sort_level asc,
     |    ^---
     |
     = expected access_chain
  Location:
  ..._sales.outlet_id) order by   - ??? -sort_level asc,   channel nul...

  Write stats: received 1240 chars / 1240 bytes; tail: …'  all_sales.outlet_id nulls first,\\n  returns desc\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_returns:str -- select str.item.text_id, sum(str.return_quantity) as store_qty, count(str.ticket_number) as store_rows where str.return_date.week_seq in (5244,5257,5264) group by str.item.text_id order by str.item.text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....week_seq in (5244,5257,5264) ??? group by str.item.text_id orde...
  ```
- `trilogy run --import raw.item:item select distinct item.category, item.class;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword — a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.category, item.c...
  ```
- `trilogy run --import raw.store_sales:sales select sales.return_reason.desc, count(sales.line_item) as cnt where sales.return_reason.desc is not null group by sales.return_reason.desc order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eturn_reason.desc is not null ??? group by sales.return_reason.d...
  ```
- `trilogy run --import raw.store_sales:ss where ss.date.year = 2000 select count(ss.customer.id) as cnt, count(ss.item.id) as item_cnt, count(ss.customer.id, ss.item.id) as combo_cnt limit 10;`

  ```text
  Syntax error in stdin:  --> 2:115
    |
  2 | where ss.date.year = 2000 select count(ss.customer.id) as cnt, count(ss.item.id) as item_cnt, count(ss.customer.id, ss.item.id) as combo_cnt limit 10;
    |                                                                                                                   ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...item_cnt, count(ss.customer.id ??? , ss.item.id) as combo_cnt lim...
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.store_sales:ss where ss.date.year = 2000 and (ss.customer.id, ss.item.id) not in (select cs.bill_customer.id, cs.item.id where cs.date.year = 2000) select ss.customer.id, ss.item.id limit 10;`

  ```text
  Syntax error in stdin: Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...stomer.id, ss.item.id) not in ??? (select cs.bill_customer.id, c...
  ```
- `trilogy file write test_nulls2.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Unique (customer, item) combos from store sal… 1 else 0 end) as both_null
full join store_combos.cust_id = catalog_combos.cust_id
    and store_combos.item_id = catalog_combos.item_id
limit 100;
`

  ```text
  refused to write 'test_nulls2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...ne_item) as cnt ;  select     ??? count(*) as total_rows,     su...

  Write stats: received 1137 chars / 1137 bytes; tail: …'nd store_combos.item_id = catalog_combos.item_id\\nlimit 100;\\n'.
  ```

### `cli-misuse`

- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy syntax example staged-membership`

  ```text
  No such command 'syntax'.
  ```
- `trilogy run query76.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/ship_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/ship_address.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run query10.preql`

  ```text
  Resolution error in query10.preql: Could not resolve connections for query with output ['customer.demographics.gender<Purpose.PROPERTY>Derivation.ROOT>', 'customer.demographics.marital_status<Purpose.PROPERTY>Derivation.ROOT>', 'customer.demographics.education_status<Purpose.PROPERTY>Derivation.ROOT>', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>', 'customer.demographics.purchase_estimate<Purpose.PROPERTY>Derivation.ROOT>', 'local.customer_count2<Purpose.METRIC>Derivation.AGGREGATE>', 'customer.demographics.credit_rating<Purpose.PROPERTY>Derivation.ROOT>', 'local.customer_count3<Purpose.METRIC>Derivation.AGGREGATE>', 'customer.demographics.dependent_count<Purpose.PROPERTY>Derivation.ROOT>', 'local.customer_count4<Purpose.METRIC>Derivation.AGGREGATE>', 'customer.demographics.employed_dependent_count<Purpose.PROPERTY>Derivation.ROOT>', 'local.customer_count5<Purpose.METRIC>Derivation.AGGREGATE>', 'customer.demographics.college_dependent_count<Purpose.PROPERTY>Derivation.ROOT>', 'local.customer_count6<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.good_item<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```

### `type-error`

- `trilogy run --import raw.date:date select date.date where date.date between '2000-08-23' and '2000-09-06' order by date.date limit 20;`

  ```text
  Syntax error in stdin: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
