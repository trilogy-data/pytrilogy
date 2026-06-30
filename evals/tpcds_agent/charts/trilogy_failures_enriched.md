# Trilogy failure analysis — 20260629-214830

- Run `20260629-214830` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1274 | failed: 105 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 74 | 70% |
| `syntax-parse` | 27 | 26% |
| `join-resolution` | 2 | 2% |
| `syntax-missing-alias` | 2 | 2% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {d.day_of_week, d.week_seq, local.in_2001}
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: next.week_seq. Suggestions: ['s.date.week_seq', 's.return_date.week_seq', 's.billing_customer.first_sales_date.week_seq', 's.billing_customer.first_shipto_date.week_seq', 's.ship_customer.first_sales_date.week_seq', 's.ship_customer.first_shipto_date.week_seq']
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Ambiguous reference 'joined.day_of_week': matches ['joined.cur.day_of_week', 'joined.nxt.day_of_week']. Qualify the full path to disambiguate.
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 25). The requested concepts split into 8 disconnected subgraphs: {cur.s.date.week_seq, nxt.sales}; {local._virt_filter_sales_1052951538548851}; {local._virt_filter_sales_1109219073415122}; {local._virt_filter_sales_4141270223950682}; {local._virt_filter_sales_4506010473411463}; {local._virt_filter_sales_4680731694186725}; {local._virt_filter_sales_8084014703438785}; {local._virt_filter_sales_9110547926739904}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Ambiguous reference 'ratios.dow': matches ['ratios.cur.dow', 'ratios.nxt.dow']. Qualify the full path to disambiguate.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 80 column 2 (char 2185). Re-issue the call with valid JSON arguments.
  ```
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
  Resolution error in query10.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 13). The requested concepts split into 2 disconnected subgraphs: {c.address.county, c.demographics.college_dependent_count, c.demographics.credit_rating, c.demographics.dependent_count, c.demographics.education_status, c.demographics.employed_dependent_count, c.demographics.gender, c.demographics.id, c.demographics.marital_status, c.demographics.purchase_estimate, c.id}; {ss.date.month_of_year, ss.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query10.preql`

  ```text
  Unexpected error in query10.preql: Could not render the query: Missing source reference to c.id. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  concerned as (
  SELECT
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" as "ws_billing_customer_id"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2002 and "ws_date_date"."D_MOY" BETWEEN 1 AND 4

  GROUP BY
      1),
  questionable as (
  SELECT
      "cs_catalog_sales"."CS_SHIP_CUSTOMER_SK" as "cs_ship_customer_id"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 2002 and "cs_sold_date_date"."D_MOY" BETWEEN 1 AND 4

  GROUP BY
      1),
  cooperative as (
  SELECT
      "c_demographics_customer_demographics"."CD_CREDIT_RATING" as "c_demographics_credit_rating",
      "c_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "c_demographics_college_dependent_count",
      "c_demographics_customer_demographics"."CD_DEP_COUNT" as "c_demographics_dependent_count",
      "c_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "c_demographics_employed_dependent_count",
      "c_demographics_customer_demographics"."CD_EDUCATION_STATUS" as "c_demographics_education_status",
      "c_demographics_customer_demographics"."CD_GENDER" as "c_demographics_gender",
      "c_demographics_customer_demographics"."CD_MARITAL_STATUS" as "c_demographics_marital_status",
      "c_demographics_customer_demographics"."CD_PURCHASE_ESTIMATE" as "c_demographics_purchase_estimate"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "c_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "c_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "c_address_customer_address" on "c_customers"."C_CURRENT_ADDR_SK" = "c_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "c_demographics_customer_demographics" on "c_customers"."C_CURRENT_CDEMO_SK" = "c_demographics_customer_demographics"."CD_DEMO_SK"
  WHERE
      "c_address_customer_address"."CA_COUNTY" in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County') and "ss_date_date"."D_YEAR" = 2002 and "ss_date_date"."D_MOY" BETWEEN 1 AND 4 and "c_customers"."C_CURRENT_CDEMO_SK" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      "ss_store_sales"."SS_CUSTOMER_SK"),
  abhorrent as (
  SELECT
      "concerned"."ws_billing_customer_id" as "web_customers_ws_billing_customer_id"
  FROM
      "concerned"),
  juicy as (
  SELECT
      "questionable"."cs_ship_customer_id" as "catalog_customers_cs_ship_customer_id"
  FROM
      "questionable"),
  sweltering as (
  SELECT
      "cooperative"."c_demographics_college_dependent_count" as "store_customers_c_demographics_college_dependent_count",
      "cooperative"."c_demographics_credit_rating" as "store_customers_c_demographics_credit_rating",
      "cooperative"."c_demographics_dependent_count" as "store_customers_c_demographics_dependent_count",
      "cooperative"."c_demographics_education_status" as "store_customers_c_demographics_education_status",
      "cooperative"."c_demographics_employed_dependent_count" as "store_customers_c_demographics_employed_dependent_count",
      "cooperative"."c_demographics_gender" as "store_customers_c_demographics_gender",
      "cooperative"."c_demographics_marital_status" as "store_customers_c_demographics_marital_status",
      "cooperative"."c_demographics_purchase_estimate" as "store_customers_c_demographics_purchase_estimate",
      INVALID_REFERENCE_BUG<Missing source reference to c.id> as "store_customers_c_id"
  FROM
      "cooperative"
  WHERE
      INVALID_REFERENCE_BUG<Missing source reference to c.id> in (select abhorrent."web_customers_ws_billing_customer_id" from abhorrent where abhorrent."web_customers_ws_billing_customer_id" is not null) or INVALID_REFERENCE_BUG<Missing source reference to c.id> in (select juicy."catalog_customers_cs_ship_customer_id" from juicy where juicy."catalog_customers_cs_ship_customer_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9)
  SELECT
      "sweltering"."store_customers_c_demographics_gender" as "store_customers_c_demographics_gender",
      "sweltering"."store_customers_c_demographics_marital_status" as "store_customers_c_demographics_marital_status",
      "sweltering"."store_customers_c_demographics_education_status" as "store_customers_c_demographics_education_status",
      count("sweltering"."store_customers_c_id") as "cust_count",
      "sweltering"."store_customers_c_demographics_purchase_estimate" as "store_customers_c_demographics_purchase_estimate",
      count("sweltering"."store_customers_c_id") as "cust_count2",
      "sweltering"."store_customers_c_demographics_credit_rating" as "store_customers_c_demographics_credit_rating",
      count("sweltering"."store_customers_c_id") as "cust_count3",
      "sweltering"."store_customers_c_demographics_dependent_count" as "store_customers_c_demographics_dependent_count",
      count("sweltering"."store_customers_c_id") as "cust_count4",
      "sweltering"."store_customers_c_demographics_employed_dependent_count" as "store_customers_c_demographics_employed_dependent_count",
      count("sweltering"."store_customers_c_id") as "cust_count5",
      "sweltering"."store_customers_c_demographics_college_dependent_count" as "store_customers_c_demographics_college_dependent_count",
      count("sweltering"."store_customers_c_id") as "cust_count6"
  FROM
      "sweltering"
  GROUP BY
      1,
      2,
      3,
      5,
      7,
      9,
      11,
      13
  ORDER BY
      "sweltering"."store_customers_c_demographics_gender" asc,
      "sweltering"."store_customers_c_demographics_marital_status" asc,
      "sweltering"."store_customers_c_demographics_education_status" asc,
      "sweltering"."store_customers_c_demographics_purchase_estimate" asc,
      "sweltering"."store_customers_c_demographics_credit_rating" asc,
      "sweltering"."store_customers_c_demographics_dependent_count" asc,
      "sweltering"."store_customers_c_demographics_employed_dependent_count" asc,
      "sweltering"."store_customers_c_demographics_college_dependent_count" asc
  LIMIT (100)
  ```
- `trilogy run query10.preql`

  ```text
  Resolution error in query10.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 12). The requested concepts split into 4 disconnected subgraphs: {c.address.county, c.demographics.college_dependent_count, c.demographics.credit_rating, c.demographics.dependent_count, c.demographics.education_status, c.demographics.employed_dependent_count, c.demographics.gender, c.demographics.id, c.demographics.marital_status, c.demographics.purchase_estimate, c.id, local.cust_count, local.cust_count2, local.cust_count3, local.cust_count4, local.cust_count5, local.cust_count6}; {local.has_catalog_sale}; {local.has_web_sale}; {ss.customer.id, ss.date.month_of_year, ss.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query10.preql`

  ```text
  Unexpected error in query10.preql: Could not render the query: Missing source reference to c.id. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  concerned as (
  SELECT
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" as "ws_billing_customer_id"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2002 and "ws_date_date"."D_MOY" BETWEEN 1 AND 4

  GROUP BY
      1),
  questionable as (
  SELECT
      "cs_catalog_sales"."CS_SHIP_CUSTOMER_SK" as "cs_ship_customer_id"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 2002 and "cs_sold_date_date"."D_MOY" BETWEEN 1 AND 4

  GROUP BY
      1),
  cooperative as (
  SELECT
      "c_demographics_customer_demographics"."CD_CREDIT_RATING" as "c_demographics_credit_rating",
      "c_demographics_customer_demographics"."CD_DEP_COLLEGE_COUNT" as "c_demographics_college_dependent_count",
      "c_demographics_customer_demographics"."CD_DEP_COUNT" as "c_demographics_dependent_count",
      "c_demographics_customer_demographics"."CD_DEP_EMPLOYED_COUNT" as "c_demographics_employed_dependent_count",
      "c_demographics_customer_demographics"."CD_EDUCATION_STATUS" as "c_demographics_education_status",
      "c_demographics_customer_demographics"."CD_GENDER" as "c_demographics_gender",
      "c_demographics_customer_demographics"."CD_MARITAL_STATUS" as "c_demographics_marital_status",
      "c_demographics_customer_demographics"."CD_PURCHASE_ESTIMATE" as "c_demographics_purchase_estimate"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "c_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "c_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "c_address_customer_address" on "c_customers"."C_CURRENT_ADDR_SK" = "c_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "c_demographics_customer_demographics" on "c_customers"."C_CURRENT_CDEMO_SK" = "c_demographics_customer_demographics"."CD_DEMO_SK"
  WHERE
      "c_address_customer_address"."CA_COUNTY" in ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County') and "ss_date_date"."D_YEAR" = 2002 and "ss_date_date"."D_MOY" BETWEEN 1 AND 4 and "c_customers"."C_CURRENT_CDEMO_SK" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      "ss_store_sales"."SS_CUSTOMER_SK"),
  abhorrent as (
  SELECT
      "concerned"."ws_billing_customer_id" as "web_custs_ws_billing_customer_id"
  FROM
      "concerned"),
  juicy as (
  SELECT
      "questionable"."cs_ship_customer_id" as "cat_custs_cs_ship_customer_id"
  FROM
      "questionable"),
  sweltering as (
  SELECT
      "cooperative"."c_demographics_college_dependent_count" as "store_custs_c_demographics_college_dependent_count",
      "cooperative"."c_demographics_credit_rating" as "store_custs_c_demographics_credit_rating",
      "cooperative"."c_demographics_dependent_count" as "store_custs_c_demographics_dependent_count",
      "cooperative"."c_demographics_education_status" as "store_custs_c_demographics_education_status",
      "cooperative"."c_demographics_employed_dependent_count" as "store_custs_c_demographics_employed_dependent_count",
      "cooperative"."c_demographics_gender" as "store_custs_c_demographics_gender",
      "cooperative"."c_demographics_marital_status" as "store_custs_c_demographics_marital_status",
      "cooperative"."c_demographics_purchase_estimate" as "store_custs_c_demographics_purchase_estimate",
      INVALID_REFERENCE_BUG<Missing source reference to c.id> as "store_custs_c_id"
  FROM
      "cooperative"
  WHERE
      INVALID_REFERENCE_BUG<Missing source reference to c.id> in (select abhorrent."web_custs_ws_billing_customer_id" from abhorrent where abhorrent."web_custs_ws_billing_customer_id" is not null) or INVALID_REFERENCE_BUG<Missing source reference to c.id> in (select juicy."cat_custs_cs_ship_customer_id" from juicy where juicy."cat_custs_cs_ship_customer_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9)
  SELECT
      "sweltering"."store_custs_c_demographics_gender" as "store_custs_c_demographics_gender",
      "sweltering"."store_custs_c_demographics_marital_status" as "store_custs_c_demographics_marital_status",
      "sweltering"."store_custs_c_demographics_education_status" as "store_custs_c_demographics_education_status",
      count("sweltering"."store_custs_c_id") as "cust_count",
      "sweltering"."store_custs_c_demographics_purchase_estimate" as "store_custs_c_demographics_purchase_estimate",
      count("sweltering"."store_custs_c_id") as "cust_count2",
      "sweltering"."store_custs_c_demographics_credit_rating" as "store_custs_c_demographics_credit_rating",
      count("sweltering"."store_custs_c_id") as "cust_count3",
      "sweltering"."store_custs_c_demographics_dependent_count" as "store_custs_c_demographics_dependent_count",
      count("sweltering"."store_custs_c_id") as "cust_count4",
      "sweltering"."store_custs_c_demographics_employed_dependent_count" as "store_custs_c_demographics_employed_dependent_count",
      count("sweltering"."store_custs_c_id") as "cust_count5",
      "sweltering"."store_custs_c_demographics_college_dependent_count" as "store_custs_c_demographics_college_dependent_count",
      count("sweltering"."store_custs_c_id") as "cust_count6"
  FROM
      "sweltering"
  GROUP BY
      1,
      2,
      3,
      5,
      7,
      9,
      11,
      13
  ORDER BY
      "sweltering"."store_custs_c_demographics_gender" asc,
      "sweltering"."store_custs_c_demographics_marital_status" asc,
      "sweltering"."store_custs_c_demographics_education_status" asc,
      "sweltering"."store_custs_c_demographics_purchase_estimate" asc,
      "sweltering"."store_custs_c_demographics_credit_rating" asc,
      "sweltering"."store_custs_c_demographics_dependent_count" asc,
      "sweltering"."store_custs_c_demographics_employed_dependent_count" asc,
      "sweltering"."store_custs_c_demographics_college_dependent_count" asc
  LIMIT (100)
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 2 undefined concept references; fix all before re-running:
    - first_name (line 52, col 5, in ORDER BY); did you mean: ss.customer.first_name, ss.return_customer.first_name, ws.billing_customer.first_name, ws.ship_customer.first_name, ws.return_customer.first_name, ws.return_refund_customer.first_name?
    - last_name (line 53, col 5, in ORDER BY); did you mean: ss.customer.last_name, ss.return_customer.last_name, ws.billing_customer.last_name, ws.ship_customer.last_name, ws.return_customer.last_name, ws.return_refund_customer.last_name?
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 2 undefined concept references; fix all before re-running:
    - first_name (line 52, col 5, in ORDER BY); did you mean: ss.customer.first_name, ss.return_customer.first_name, ws.billing_customer.first_name, ws.ship_customer.first_name, ws.return_customer.first_name, ws.return_refund_customer.first_name?
    - last_name (line 53, col 5, in ORDER BY); did you mean: ss.customer.last_name, ss.return_customer.last_name, ws.billing_customer.last_name, ws.ship_customer.last_name, ws.return_customer.last_name, ws.return_refund_customer.last_name?
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
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
  Unexpected error in query21.preql: (_duckdb.BinderException) Binder Error: Values list "questionable" does not have a column named "before_qty"

  LINE 43:     "questionable"."before_qty" as "before_total",
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
      sum(CASE WHEN "cheerful"."inv_date_date" >= date '2000-02-10' and "cheerful"."inv_date_date" < date '2000-03-11' THEN "cheerful"."inv_quantity_on_hand" ELSE NULL END) as "before_qty",
      sum(CASE WHEN "cheerful"."inv_date_date" >= date '2000-03-11' and "cheerful"."inv_date_date" <= date '2000-04-10' THEN "cheerful"."inv_quantity_on_hand" ELSE NULL END) as "after_qty"
  FROM
      "cheerful"
  GROUP BY
      1,
      2),
  questionable as (
  SELECT
      coalesce("cheerful"."inv_item_text_id","thoughtful"."inv_item_text_id") as "inv_item_text_id",
      coalesce("cheerful"."inv_warehouse_id","thoughtful"."inv_warehouse_id") as "inv_warehouse_id"
  FROM
      "cheerful"
      LEFT OUTER JOIN "thoughtful" on "cheerful"."inv_item_text_id" = "thoughtful"."inv_item_text_id" AND "cheerful"."inv_warehouse_id" is not distinct from "thoughtful"."inv_warehouse_id"
  WHERE
      "cheerful"."inv_item_current_price" BETWEEN 0.99 AND 1.49 and "thoughtful"."after_qty" / nullif("thoughtful"."before_qty",0) is not null and "thoughtful"."after_qty" / nullif("thoughtful"."before_qty",0) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0

  GROUP BY
      1,
      2,
      "thoughtful"."after_qty",
      "thoughtful"."before_qty")
  SELECT
      "inv_warehouse_warehouse"."w_warehouse_name" as "warehouse_name",
      "questionable"."inv_item_text_id" as "item_code",
      "questionable"."before_qty" as "before_total",
      "questionable"."after_qty" as "after_total"
  FROM
      "questionable"
      INNER JOIN "warehouse" as "inv_warehouse_warehouse" on "questionable"."inv_warehouse_id" = "inv_warehouse_warehouse"."w_warehouse_sk"
  ORDER BY
      "warehouse_name" asc nulls first,
      "item_code" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query22.preql`

  ```text
  Syntax error in query22.preql: 12 undefined concept references; fix all before re-running:
    - item.product_name (line 6, col 5, in SELECT); did you mean: inv.item.product_name, item.brand_name, inv.item.brand_name?
    - item.brand_name (line 7, col 5, in SELECT); did you mean: inv.item.brand_name, item.product_name, inv.item.brand_id?
    - item.class (line 8, col 5, in SELECT); did you mean: inv.item.class, inv.item.class_id, item.category?
    - item.category (line 9, col 5, in SELECT); did you mean: inv.item.category, inv.item.category_id, item.class?
    - item.product_name (line 4, in SELECT); did you mean: inv.item.product_name, item.brand_name, inv.item.brand_name?
    - item.brand_name (line 4, in SELECT); did you mean: inv.item.brand_name, item.product_name, inv.item.brand_id?
    - item.class (line 4, in SELECT); did you mean: inv.item.class, inv.item.class_id, item.category?
    - item.category (line 4, in SELECT); did you mean: inv.item.category, inv.item.category_id, item.class?
    - item.product_name (line 13, col 5, in ORDER BY); did you mean: inv.item.product_name, item.brand_name, inv.item.brand_name?
    - item.brand_name (line 14, col 5, in ORDER BY); did you mean: inv.item.brand_name, item.product_name, inv.item.brand_id?
    - item.class (line 15, col 5, in ORDER BY); did you mean: inv.item.class, inv.item.class_id, item.category?
    - item.category (line 16, col 5, in ORDER BY); did you mean: inv.item.category, inv.item.category_id, item.class?
  ```
- `trilogy run query22.preql`

  ```text
  Resolution error in query22.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {d.year}; {inv.item.brand_name, inv.item.category, inv.item.class, inv.item.product_name, local.avg_quantity_on_hand}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'RowsetNode<customer_totals.cust_id,customer_totals.lifetime_total>': None} and need customer_totals.lifetime_total > multiply(0.5,local.max_customer_total@Grain<customer_totals.cust_id>|customer_totals.lifetime_total > multiply(0.5,local.max_customer_total@Grain<Abstract>))
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'RowsetNode<customer_totals.cust_id,customer_totals.lifetime_total>': None} and need customer_totals.lifetime_total > multiply(0.5,local.max_customer_total@Grain<Abstract>)
  ```
- `trilogy file read test_freq.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run test_combined.preql`

  ```text
  Syntax error in test_combined.preql: Have {'RowsetNode<customer_totals.cust_id,customer_totals.lifetime_total>': None} and need customer_totals.lifetime_total > local.threshold
  ```
- `trilogy run test_combined2.preql`

  ```text
  Syntax error in test_combined2.preql: Have {'RowsetNode<cust_tots.cust_id,cust_tots.total>': None} and need cust_tots.total > local.the_threshold
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query27.preql`

  ```text
  Syntax error in query27.preql: ORDER BY contains aggregate `grouping(local.item_code)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.item_code) as g order by g desc`.
  ```
- `trilogy file read query27.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query29.preql duckdb`

  ```text
  Syntax error in query29.preql: Comparison `ss.return_date.month_of_year <= 12` matches every value of enum field 'ss.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 44 column 12 (char 1685). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: 11 undefined concept references; fix all before re-running:
    - salutation (line 31, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.salutation, wr.web_sales.ship_customer.salutation, wr.web_sales.return_customer.salutation, wr.web_sales.return_refund_customer.salutation, wr.billing_customer.salutation, wr.refunded_customer.salutation?
    - first_name (line 32, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.first_name, wr.web_sales.ship_customer.first_name, wr.web_sales.return_customer.first_name, wr.web_sales.return_refund_customer.first_name, wr.billing_customer.first_name, wr.refunded_customer.first_name?
    - last_name (line 33, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.last_name, wr.web_sales.ship_customer.last_name, wr.web_sales.return_customer.last_name, wr.web_sales.return_refund_customer.last_name, wr.billing_customer.last_name, wr.refunded_customer.last_name?
    - preferred_cust_flag (line 34, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.preferred_cust_flag, wr.web_sales.ship_customer.preferred_cust_flag, wr.web_sales.return_customer.preferred_cust_flag, wr.web_sales.return_refund_customer.preferred_cust_flag, wr.billing_customer.preferred_cust_flag, wr.refunded_customer.preferred_cust_flag?
    - birth_day (line 35, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_day, wr.web_sales.ship_customer.birth_day, wr.web_sales.return_customer.birth_day, wr.web_sales.return_refund_customer.birth_day, wr.billing_customer.birth_day, wr.refunded_customer.birth_day?
    - birth_month (line 36, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_month, wr.web_sales.ship_customer.birth_month, wr.web_sales.return_customer.birth_month, wr.web_sales.return_refund_customer.birth_month, wr.billing_customer.birth_month, wr.refunded_customer.birth_month?
    - birth_year (line 37, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_year, wr.web_sales.ship_customer.birth_year, wr.web_sales.return_customer.birth_year, wr.web_sales.return_refund_customer.birth_year, wr.billing_customer.birth_year, wr.refunded_customer.birth_year?
    - birth_country (line 38, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.birth_country, wr.web_sales.ship_customer.birth_country, wr.web_sales.return_customer.birth_country, wr.web_sales.return_refund_customer.birth_country, wr.billing_customer.birth_country, wr.refunded_customer.birth_country?
    - login (line 39, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.login, wr.web_sales.ship_customer.login, wr.web_sales.return_customer.login, wr.web_sales.return_refund_customer.login, wr.billing_customer.login, wr.refunded_customer.login?
    - email_address (line 40, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.email_address, wr.web_sales.ship_customer.email_address, wr.web_sales.return_customer.email_address, wr.web_sales.return_refund_customer.email_address, wr.billing_customer.email_address, wr.refunded_customer.email_address?
    - last_review_date (line 41, col 5, in ORDER BY); did you mean: wr.web_sales.billing_customer.last_review_date, wr.web_sales.ship_customer.last_review_date, wr.web_sales.return_customer.last_review_date, wr.web_sales.return_refund_customer.last_review_date, wr.billing_customer.last_review_date, wr.refunded_customer.last_review_date?
  ```
- `trilogy file read query30.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query31.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query33.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query39.preql`

  ```text
  Syntax error in query39.preql: Undefined concept: jan.wh_id. Suggestions: ['monthly_stats.wh_id', 'qualified.monthly_stats.wh_id', 'feb.wh_id', 'jan.item_id', 'jan_wh']
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query47.preql`

  ```text
  Syntax error in query47.preql: Output column 'avg_monthly_sales' renames 'local.avg_monthly_sales' back to the name of an existing concept 'avg_monthly_sales' (defined at line 7) that 'local.avg_monthly_sales' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'avg_monthly_sales_out').
  ```
- `trilogy file read query50.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query57.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 31 (char 30). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {next_year.pivoted.fri_price, next_year.pivoted.mon_price, next_year.pivoted.sat_price, next_year.pivoted.sun_price, next_year.pivoted.thu_price, next_year.pivoted.tue_price, next_year.pivoted.wed_price, next_year.pivoted.weekly_sales.ss.date.week_seq, next_year.pivoted.weekly_sales.ss.store.id, this_year.pivoted.fri_price, this_year.pivoted.mon_price, this_year.pivoted.sat_price, this_year.pivoted.sun_price, this_year.pivoted.thu_price, this_year.pivoted.tue_price, this_year.pivoted.wed_price, this_year.pivoted.weekly_sales.ss.date.week_seq, this_year.pivoted.weekly_sales.ss.store.id, this_year.pivoted.weekly_sales.ss.store.name, this_year.pivoted.weekly_sales.ss.store.text_id}
  ```
- `trilogy run query59.preql`

  ```text
  Syntax error in query59.preql: Undefined concept: next_year.id. Suggestions: ['ss.item.id', 'ss.date.id', 'ss.return_date.id', 'ss.time.id', 'ss.return_time.id', 'ss.customer.demographics.id']
  ```
- `trilogy file read query59.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 56 column 3 (char 2843). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query65.preql duckdb`

  ```text
  Syntax error in query65.preql: Undefined concept: ss.store. Suggestions: ['ss.store.date.id', 'ss.store.date.text_id', 'ss.store.date._date_string', 'ss.store.date.date', 'ss.store.date.year', 'ss.store.date.day_of_week']
  ```
- `trilogy run query65.preql duckdb`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 6). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query72.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql duckdb`

  ```text
  Unexpected error in query75.preql: Join chain repeats source `yr2002` (keys `yr2002.brand_id` and `yr2002.class_id`). A `=` chain joins ONE key across distinct sources; join a composite key with `and` or separate clauses (e.g. `a.k1 = b.k1 and a.k2 = b.k2`).
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
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: ORDER BY contains aggregate `grouping(sales.channel)` (line 4), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(sales.channel) as g order by g desc`.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query79.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql duckdb`

  ```text
  Syntax error in query80.preql: Undefined concept: catalog_sales.return_net_loss. Suggestions: ['store_sales.return_net_loss', 'catalog_sales.return_amount', 'catalog_sales.order_number']
  ```
- `trilogy run query80.preql duckdb`

  ```text
  Syntax error in query80.preql: ORDER BY contains aggregate `grouping(combined.channel_label)` (line 41), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_label) as g order by g desc`.
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query82.preql`

  ```text
  Syntax error in query82.preql: Undefined concept: raw.store_sales.item.id (line 17, col 24, in WHERE). Suggestions: ['inv.warehouse.id', 'inv.item.id', 'inv.date.id']
  ```
- `trilogy run query83.preql duckdb`

  ```text
  Syntax error in query83.preql: Undefined concept: return_date.week_seq. Suggestions: ['sr.return_date.week_seq', 'wr.web_sales.return_date.week_seq', 'wr.return_date.week_seq', 'sr.store.date.week_seq', 'sr.billing_customer.first_sales_date.week_seq', 'sr.billing_customer.first_shipto_date.week_seq']
  ```
- `trilogy run query83.preql duckdb`

  ```text
  Syntax error in query83.preql: Undefined concept: cr.return_date.week_seq. Suggestions: ['sr.store.date.week_seq', 'sr.return_date.week_seq', 'sr.billing_customer.first_sales_date.week_seq', 'sr.billing_customer.first_shipto_date.week_seq', 'cr.sales.date.week_seq', 'cr.sales.ship_date.week_seq']
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query85.preql`

  ```text
  Syntax error in query85.preql: Comparison `wr.returning_demographic.marital_status = 'Married'` can never match enum field 'wr.returning_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query93.preql`

  ```text
  Syntax error in query93.preql: Output column 'customer_id' renames 'local.customer_id' back to the name of an existing concept 'customer_id' (defined at line 20) that 'local.customer_id' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'customer_id_out').
  ```
- `trilogy run query94.preql`

  ```text
  Syntax error in query94.preql: Cannot compare BOOL (sum(<Filter: ref:ws.is_returned where ref:ws.is_returned = True>)<abstract>) and INTEGER (0) of different types with operator = in sum(<Filter: ref:ws.is_returned where ref:ws.is_returned = True>)<abstract> = 0
  ```
- `trilogy file exists query96.preql`

  ```text
  {
    "event": "exists",
    "path": "query96.preql",
    "exists": false
  }
  ```
- `trilogy file exists query96.preql`

  ```text
  {
    "event": "exists",
    "path": "query96.preql",
    "exists": false
  }
  ```
- `trilogy file read query96.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as s;

# First compute sales per week_seq and day_of_week for WEB + CATALOG
with weekly_sales…of_week = weekly_sales.day_of_week), 2) as ratio
inner join weekly_sales.week_seq + 53 = weekly_sales_next.week_seq
    = weekly_sales.day_of_week
;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...here weekly_sales.week_seq in ??? (select s.date.week_seq where

  Write stats: received 738 chars / 738 bytes; tail: …'weekly_sales_next.week_seq\\n    = weekly_sales.day_of_week\\n;\\n'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Total sales per (week_seq, day_of_week) for WEB + CATALOG
auto wd_sales <- sum(s.net…o(3) as wednesday,
    @day_ratio(4) as thursday,
    @day_ratio(5) as friday,
    @day_ratio(6) as saturday,
order by s.date.week_seq nulls first
;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
   s.date.day_of_week = dow, 2) ??? by s.date.week_seq;  # Week se...

  Write stats: received 906 chars / 906 bytes; tail: …'atio(6) as saturday,\\norder by s.date.week_seq nulls first\\n;\\n'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Rowset: current year (WEB + CATALOG) sales per (week_seq, day_of_week)
rowset cur <-…tio_dow(6) as saturday,
inner join cur.week_seq + 53 = nxt.week_seq
inner join cur.day_of_week = nxt.day_of_week
order by cur.week_seq nulls first
;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate - write `coalesce(sum(x) by store.id, 0)` - or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...nxt.day_of_week = d), 2)      ??? by cur.week_seq;  select     c...

  Write stats: received 998 chars / 998 bytes; tail: …'_week = nxt.day_of_week\\norder by cur.week_seq nulls first\\n;\\n'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Rowset: current year (WEB + CATALOG) sales and next-year sales, per (week_seq, day_o…tio(3) as wednesday,
    @piv_ratio(4) as thursday,
    @piv_ratio(5) as friday,
    @piv_ratio(6) as saturday,
order by base.week_seq nulls first
;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset base as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ..., per (week_seq, day_of_week) ??? rowset base as where s.channel...

  Write stats: received 849 chars / 849 bytes; tail: …'_ratio(6) as saturday,\\norder by base.week_seq nulls first\\n;\\n'.
  ```
- `trilogy run --import raw.all_sales:s select s.channel, s.channel_dim_id, s.channel_dim_text_id, count(s.item.id) as cnt where s.date.date between '2000-08-23…te and s.channel_dim_id is not null and s.channel = 'CATALOG' group by s.channel, s.channel_dim_id, s.channel_dim_text_id order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ull and s.channel = 'CATALOG' ??? group by s.channel, s.channel_...
  ```
- `trilogy run --import raw.store:s select s.text_id, s.name from raw.store as s order by s.text_id asc limit 5;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...s s; select s.text_id, s.name ??? from raw.store as s order by s...
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Step 1: Find (brand_id, class_id, category_id) combos that appear in all three c….item.category_id) asc,
    channel asc nulls first, brand_id asc nulls first, 
    class_id asc nulls first, category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:1
     |
  32 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...s_id, sales.item.category_id) ??? having      (grouping(sales.ch...

  Write stats: received 3423 chars / 3423 bytes; tail: …'_id asc nulls first, category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Step 1: Find (brand_id, class_id, category_id) combos that appear in all three c…asc nulls first,
    channel asc nulls first,
    brand_id asc nulls first,
    class_id asc nulls first,
    category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:1
     |
  32 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...s_id, sales.item.category_id) ??? having     (         grouping(...

  Write stats: received 2629 chars / 2629 bytes; tail: …'asc nulls first,\\n    category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Step 1: Find (brand_id, class_id, category_id) combos that appear in all three c…asc nulls first,
    channel asc nulls first,
    brand_id asc nulls first,
    class_id asc nulls first,
    category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:1
     |
  24 | having
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...sales.date.month_of_year = 11 ??? having     (         grouping(...

  Write stats: received 2656 chars / 2656 bytes; tail: …'asc nulls first,\\n    category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:sales select sales.channel, count(sales.order_id) as cnt by sales.channel;`

  ```text
  Syntax error in stdin:  --> 2:52
    |
  2 | select sales.channel, count(sales.order_id) as cnt by sales.channel;
    |                                                    ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   count(sales.order_id) as cnt ??? by sales.channel;
  ```
- `trilogy file write query14_test3.preql --content import raw.all_sales as sales;

where sales.date.year = 2001
  and sales.date.month_of_year = 11
select
    …les.channel) asc nulls first,
    grouping(sales.item.brand_id) asc nulls first,
    channel asc nulls first,
    brand_id asc nulls first
limit 20;
`

  ```text
  refused to write 'query14_test3.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...channel, sales.item.brand_id) ??? having      (grouping(sales.ch...

  Write stats: received 695 chars / 695 bytes; tail: …'nel asc nulls first,\\n    brand_id asc nulls first\\nlimit 20;\\n'.
  ```
- `trilogy run --import raw.store_sales:store select store.date.year, count(store.ticket_number) as cnt group by 1 order by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...t(store.ticket_number) as cnt ??? group by 1 order by 1;
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.ticket_number, sum(store_sales.quantity) as ticket_qty group by store_sales.ticket_number order by ticket_qty desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sales.quantity) as ticket_qty ??? group by store_sales.ticket_nu...
  ```
- `trilogy file write query29.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Count store_sales rows vs catalog_sales rows that… cs.quantity as qty;

select
    count(*) as row_count
inner join store_count.cid = catalog_count.cid
inner join store_count.iid = catalog_count.iid;`

  ```text
  refused to write 'query29.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ....quantity as qty;  select     ??? count(*) as row_count inner jo...

  Write stats: received 732 chars / 732 bytes; tail: …'og_count.cid\\ninner join store_count.iid = catalog_count.iid;'.
  ```
- `trilogy file write query37.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

# Items in catalog sales, filtered by price, manufac…ween '2000-02-01'::date and '2000-04-01'::date
            and inv.quantity_on_hand between 100 and 500
    )
order by
    cs.item.text_id
limit 100;`

  ```text
  refused to write 'query37.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries - joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...8)     and cs.item.text_id in ??? (         select inv.item.text...

  Write stats: received 615 chars / 615 bytes; tail: …'en 100 and 500\\n    )\\norder by\\n    cs.item.text_id\\nlimit 100;'.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Unique (last_name, fi…st_name = web_tuples.last_name
   and store_tuples.first_name = web_tuples.first_name
   and store_tuples.sale_date = web_tuples.sale_date
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...n all three fields select     ??? count(*) as cnt inner join sto...

  Write stats: received 1464 chars / 1464 bytes; tail: …'and store_tuples.sale_date = web_tuples.sale_date\\nlimit 100;'.
  ```
- `trilogy file write query53.preql --content import raw.store_sales as sales;

auto filtered_sales <-
    sales.sales_price,
    sales.item.manufacturer_id,
  …sales) / avg_quarterly_sales > 0.1
order by
    avg_quarterly_sales asc,
    per_quarter_total asc,
    filtered_sales.manufacturer_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:22
    |
  4 |     sales.sales_price,
    |                      ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...sales <-     sales.sales_price ??? ,     sales.item.manufacturer_...

  Write stats: received 1418 chars / 1418 bytes; tail: …'total asc,\\n    filtered_sales.manufacturer_id asc\\nlimit 100;'.
  ```
- `trilogy file write query67.preql`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...icators for rollup hierarchy
   ??? auto g_class <- grouping(store...

  Write stats: received 2361 chars / 2361 bytes; tail: …'first,\r\\n    within_category_rank asc nulls first\r\\nlimit 100;'.
  ```
- `trilogy file write query67.preql`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 42:1
     |
  42 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ..., store_sales.store.text_id)
   ??? having
       rnk <= 100
   order

  Write stats: received 2361 chars / 2361 bytes; tail: …'first,\r\\n    within_category_rank asc nulls first\r\\nlimit 100;'.
  ```
- `trilogy file write query67.preql`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
    --> 42:1
     |
  42 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ..., store_sales.store.text_id)
   ??? having
       rnk <= 100
   order

  Write stats: received 2361 chars / 2361 bytes; tail: …'first,\r\\n    within_category_rank asc nulls first\r\\nlimit 100;'.
  ```
- `trilogy file write query72.preql --content import raw/catalog_sales as cs;
import raw/inventory as inv;

select
    cs.item.desc as item_description,
    inv…total_orders desc nulls first,
    item_description asc nulls first,
    warehouse_name asc nulls first,
    week_sequence asc nulls first
limit 100;`

  ```text
  refused to write 'query72.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs; impor...

  Write stats: received 868 chars / 868 bytes; tail: …'sc nulls first,\\n    week_sequence asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…ned.missing_ref_label asc nulls first,
    combined.yr asc nulls first,
    combined.qtr asc nulls first,
    combined.cat asc nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...yr, qtr, cat, total_ext_price) ???  select     combined.channel,...

  Write stats: received 1612 chars / 1612 bytes; tail: …'asc nulls first,\\n    combined.cat asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…tlet_id) = 1
order by grouping(combined.channel_label) + grouping(combined.outlet_id) asc, channel asc nulls first, outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
    --> 47:1
     |
  47 | having combined.outlet_id is not null or grouping(combined.outlet_id) = 1
     | ^---
     |
     = expected limit or order_by
  Location:
  ...el_label, combined.outlet_id) ??? having combined.outlet_id is n...

  Write stats: received 2290 chars / 2290 bytes; tail: …', channel asc nulls first, outlet asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…tlet_id) = 1
order by grouping(combined.channel_label) + grouping(combined.outlet_id) asc, channel asc nulls first, outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
    --> 47:1
     |
  47 | having combined.outlet_id is not null or grouping(combined.outlet_id) = 1
     | ^---
     |
     = expected limit or order_by
  Location:
  ...el_label, combined.outlet_id) ??? having combined.outlet_id is n...

  Write stats: received 2290 chars / 2290 bytes; tail: …', channel asc nulls first, outlet asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Distinct (la… appearing in either catalog or web
select
    count(store_key ? store_key not in catalog_key and store_key not in web_key) as unique_combinations
;
`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | rowset store_combos as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...mbos from store_sales in 2000 ??? rowset store_combos as where s...

  Write stats: received 1471 chars / 1471 bytes; tail: …'_key and store_key not in web_key) as unique_combinations\\n;\\n'.
  ```
- `trilogy run --format json --import raw.store_sales:ss select ss.customer.id, ss.return_reason.desc, count(ss.ticket_number) as cnt where ss.return_reason.desc = 'reason 28' and ss.customer.id is null group by ss.customer.id, ss.return_reason.desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...8' and ss.customer.id is null ??? group by ss.customer.id, ss.re...
  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Stack store-sales pairs and catalog-sales pairs w…e = 0 and in_catalog > 0 then 1 else 0 end) as catalog_only,
    sum(case when in_store > 0 and in_catalog > 0 then 1 else 0 end) as both,
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...customer_id, item_id, channel) ???  # Presence flags per (custom...

  Write stats: received 1084 chars / 1084 bytes; tail: …' 0 and in_catalog > 0 then 1 else 0 end) as both,\\nlimit 100;'.
  ```

### `join-resolution`

- `trilogy run test_combined.preql`

  ```text
  Resolution error in test_combined.preql: Could not resolve connections for query with output ['local.frequent_desc_prefix<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
- `trilogy run query47.preql`

  ```text
  Resolution error in query47.preql: Could not resolve connections for query with output ['item.category<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand_name<Purpose.PROPERTY>Derivation.ROOT>', 'store.name<Purpose.PROPERTY>Derivation.ROOT>', 'store.company_name<Purpose.PROPERTY>Derivation.ROOT>', 'date.year<Purpose.PROPERTY>Derivation.ROOT>', 'date.month_of_year<Purpose.PROPERTY>Derivation.ROOT>', 'local.avg_monthly_sales<Purpose.METRIC>Derivation.AGGREGATE>', 'local.monthly_unit_price<Purpose.METRIC>Derivation.AGGREGATE>', 'local.prior_month_total<Purpose.PROPERTY>Derivation.WINDOW>', 'local.next_month_total<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```

### `syntax-missing-alias`

- `trilogy file write query88.preql --content import raw.store_sales as ss;

# Filter to store named 'ese'
where ss.store.name = 'ese'

# Filter households wher…0 (hour=12, minute>=0 and minute<30)
    count(ss.line_item ? ss.time.hour = 12 and ss.time.minute >= 0 and ss.time.minute < 30) as count_1200_1230
;`

  ```text
  refused to write 'query88.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `Bucket 1: 8:30-9:00 (hour=8, minute>=30) or (hour=9, minute<0)
      count(ss.line_item ? (ss.time.hour = 8 and ss.time.minute >= 30) or (ss.time.hour = 9 and ss.time.minute < 0)) as bucket_1_8_30_9_00_hour_8_minute_30_or_h`
  Location:
   2)   )  select     -- Bucket ??? 1: 8:30-9:00 (hour=8, minute>=...

  Write stats: received 2247 chars / 2247 bytes; tail: …'me.minute >= 0 and ss.time.minute < 30) as count_1200_1230\\n;'.
  ```
- `trilogy file write query88.preql --content import raw.store_sales as ss;

# Filter to store named 'ese'
where ss.store.name = 'ese'

# Filter households wher…time.minute < 0) as count_1130_1200,
    count(ss.line_item ? ss.time.hour = 12 and ss.time.minute >= 0 and ss.time.minute < 30) as count_1200_1230
;`

  ```text
  refused to write 'query88.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.line_item ? ss.time.hour = 9 and ss.time.minute >= 30) or (ss.time.hour = 10 and ss.time.minute < 0) as minute_count`
  Location:
  ...= 9 and ss.time.minute >= 30) ??? or (ss.time.hour = 10 and ss.t...

  Write stats: received 1613 chars / 1613 bytes; tail: …'me.minute >= 0 and ss.time.minute < 30) as count_1200_1230\\n;'.
  ```
