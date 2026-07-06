# Trilogy failure analysis — 20260706-023449

- Run `20260706-023449` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1173 | failed: 101 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 60 | 59% |
| `syntax-parse` | 29 | 29% |
| `cli-misuse` | 6 | 6% |
| `syntax-missing-alias` | 3 | 3% |
| `file-not-found` | 2 | 2% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel)` (line 30), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel) as g order by g desc`.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Resolution error in query08.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 17). The requested concepts split into 2 disconnected subgraphs: {total_net_profit, ss.date.quarter, ss.date.year}; {st.name, st.zip}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(sales.channel)` (line 22), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(sales.channel) as g order by g desc`.
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: Could not render the query: Missing source reference to ss.quantity; Missing source reference to ss.sales_price. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  sparkling as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "_cust_totals_cust_id",
      sum("ss_store_sales"."SS_QUANTITY" * "ss_store_sales"."SS_SALES_PRICE") as "_cust_totals_cust_total"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
  WHERE
      "ss_date_date"."D_YEAR" >= 2000 and "ss_date_date"."D_YEAR" <= 2003 and "ss_store_sales"."SS_CUSTOMER_SK" is not null

  GROUP BY
      1),
  abundant as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2),
  sweltering as (
  SELECT
      "sparkling"."_cust_totals_cust_id" as "_best_cust_cust_id",
      "sparkling"."_cust_totals_cust_total" as "_best_cust_overall_max"
  FROM
      "sparkling"),
  uneven as (
  SELECT
      "ss_item_items"."I_ITEM_SK" as "ss_item_id",
      SUBSTRING("ss_item_items"."I_ITEM_DESC",1,30) as "_freq_prefix_prefix"
  FROM
      "abundant"
      INNER JOIN "date_dim" as "ss_date_date" on "abundant"."ss_date_id" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ss_item_items" on "abundant"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_date_date"."D_YEAR" >= 2000 and "ss_date_date"."D_YEAR" <= 2003

  GROUP BY
      1,
      2),
  macho as (
  SELECT
      "sweltering"."_best_cust_cust_id" as "_best_cust_cust_id"
  FROM
      "sweltering"
  WHERE
      INVALID_REFERENCE_BUG<Missing source reference to ss.quantity> * INVALID_REFERENCE_BUG<Missing source reference to ss.sales_price> > 0.5 * "sweltering"."_best_cust_overall_max"
  ),
  yummy as (
  SELECT
      "uneven"."_freq_prefix_prefix" as "_freq_prefix_prefix"
  FROM
      "uneven"
  GROUP BY
      1
  HAVING
      count("uneven"."ss_item_id") > 4
  ),
  scrawny as (
  SELECT
      "macho"."_best_cust_cust_id" as "best_cust_cust_id"
  FROM
      "macho"),
  juicy as (
  SELECT
      "yummy"."_freq_prefix_prefix" as "_freq_prefix_prefix"
  FROM
      "yummy"),
  friendly as (
  SELECT
      "scrawny"."best_cust_cust_id" as "best_cust_cust_id"
  FROM
      "scrawny"
  GROUP BY
      1),
  vacuous as (
  SELECT
      "juicy"."_freq_prefix_prefix" as "freq_prefix_prefix"
  FROM
      "juicy"),
  concerned as (
  SELECT
      "ss_item_items"."I_ITEM_SK" as "frequent_item_id"
  FROM
      "item" as "ss_item_items"
  WHERE
      SUBSTRING("ss_item_items"."I_ITEM_DESC",1,30) in (select vacuous."freq_prefix_prefix" from vacuous where vacuous."freq_prefix_prefix" is not null)

  GROUP BY
      1),
  protective as (
  SELECT
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" as "ws_billing_customer_id",
      "ws_web_sales"."WS_LIST_PRICE" as "ws_list_price",
      "ws_web_sales"."WS_QUANTITY" as "ws_quantity",
      "ws_web_sales"."WS_SOLD_DATE_SK" as "ws_date_id"
  FROM
      "web_sales" as "ws_web_sales"
  WHERE
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" in (select friendly."best_cust_cust_id" from friendly where friendly."best_cust_cust_id" is not null) and "ws_web_sales"."WS_ITEM_SK" in (select concerned."frequent_item_id" from concerned where concerned."frequent_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "ws_web_sales"."WS_ITEM_SK"),
  highfalutin as (
  SELECT
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" as "cs_bill_customer_id",
      "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_date_id"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  WHERE
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select friendly."best_cust_cust_id" from friendly where friendly."best_cust_cust_id" is not null) and "cs_catalog_sales"."CS_ITEM_SK" in (select concerned."frequent_item_id" from concerned where concerned."frequent_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cs_catalog_sales"."CS_ITEM_SK"),
  puzzled as (
  SELECT
      "protective"."ws_quantity" * "protective"."ws_list_price" as "___tvf_arm_1_line_amt",
      "ws_billing_customer_customers"."C_FIRST_NAME" as "___tvf_arm_1_first_name",
      "ws_billing_customer_customers"."C_LAST_NAME" as "___tvf_arm_1_last_name"
  FROM
      "protective"
      INNER JOIN "date_dim" as "ws_date_date" on "protective"."ws_date_id" = "ws_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "ws_billing_customer_customers" on "protective"."ws_billing_customer_id" = "ws_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2000 and "ws_date_date"."D_MOY" = 2

  GROUP BY
      1,
      2,
      3),
  kaput as (
  SELECT
      "cs_bill_customer_customers"."C_FIRST_NAME" as "___tvf_arm_0_first_name",
      "cs_bill_customer_customers"."C_LAST_NAME" as "___tvf_arm_0_last_name",
      "highfalutin"."cs_quantity" * "highfalutin"."cs_list_price" as "___tvf_arm_0_line_amt"
  FROM
      "highfalutin"
      INNER JOIN "date_dim" as "cs_date_date" on "highfalutin"."cs_date_id" = "cs_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "cs_bill_customer_customers" on "highfalutin"."cs_bill_customer_id" = "cs_bill_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cs_date_date"."D_YEAR" = 2000 and "cs_date_date"."D_MOY" = 2

  GROUP BY
      1,
      2,
      3),
  rambunctious as (
  SELECT
      "kaput"."___tvf_arm_0_last_name" as "_feb2000_last_name",
      "kaput"."___tvf_arm_0_first_name" as "_feb2000_first_name",
      "kaput"."___tvf_arm_0_line_amt" as "_feb2000_line_amt"
  FROM
      "kaput"
  UNION ALL
  SELECT
      "puzzled"."___tvf_arm_1_last_name" as "_feb2000_last_name",
      "puzzled"."___tvf_arm_1_first_name" as "_feb2000_first_name",
      "puzzled"."___tvf_arm_1_line_amt" as "_feb2000_line_amt"
  FROM
      "puzzled")
  SELECT
      "rambunctious"."_feb2000_last_name" as "feb2000_last_name",
      "rambunctious"."_feb2000_first_name" as "feb2000_first_name",
      sum("rambunctious"."_feb2000_line_amt") as "total_sales"
  FROM
      "rambunctious"
  GROUP BY
      1,
      2
  ORDER BY
      "feb2000_last_name" asc,
      "feb2000_first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: (_duckdb.BinderException) Binder Error: Ambiguous reference to table "friendly" (duplicate alias "friendly", explicitly alias one of the tables using "AS my_alias")
  [SQL:
  WITH
  sparkling as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "_best_cust_filter_cust_id",
      sum("ss_store_sales"."SS_QUANTITY" * "ss_store_sales"."SS_SALES_PRICE") as "_best_cust_filter_cust_total",
      sum("ss_store_sales"."SS_QUANTITY" * "ss_store_sales"."SS_SALES_PRICE") as "cust_totals_cust_total"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
  WHERE
      "ss_date_date"."D_YEAR" >= 2000 and "ss_date_date"."D_YEAR" <= 2003 and "ss_store_sales"."SS_CUSTOMER_SK" is not null

  GROUP BY
      1),
  abundant as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2),
  late as (
  SELECT
      max("sparkling"."cust_totals_cust_total") as "_best_cust_filter_overall_max"
  FROM
      "sparkling"),
  uneven as (
  SELECT
      "ss_item_items"."I_ITEM_SK" as "ss_item_id",
      SUBSTRING("ss_item_items"."I_ITEM_DESC",1,30) as "_freq_prefix_prefix"
  FROM
      "abundant"
      INNER JOIN "date_dim" as "ss_date_date" on "abundant"."ss_date_id" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ss_item_items" on "abundant"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_date_date"."D_YEAR" >= 2000 and "ss_date_date"."D_YEAR" <= 2003

  GROUP BY
      1,
      2),
  scrawny as (
  SELECT
      "late"."_best_cust_filter_overall_max" as "_best_cust_filter_overall_max",
      "sparkling"."_best_cust_filter_cust_id" as "_best_cust_filter_cust_id",
      "sparkling"."_best_cust_filter_cust_total" as "_best_cust_filter_cust_total"
  FROM
      "sparkling"
      FULL JOIN "late" on 1=1),
  yummy as (
  SELECT
      "uneven"."_freq_prefix_prefix" as "_freq_prefix_prefix"
  FROM
      "uneven"
  GROUP BY
      1
  HAVING
      count("uneven"."ss_item_id") > 4
  ),
  friendly as (
  SELECT
      "scrawny"."_best_cust_filter_cust_id" as "best_cust_filter_cust_id",
      "scrawny"."_best_cust_filter_cust_total" as "best_cust_filter_cust_total",
      "scrawny"."_best_cust_filter_overall_max" as "best_cust_filter_overall_max"
  FROM
      "scrawny"),
  juicy as (
  SELECT
      "yummy"."_freq_prefix_prefix" as "_freq_prefix_prefix"
  FROM
      "yummy"),
  kaput as (
  SELECT
      "friendly"."best_cust_filter_cust_id" as "best_cust_cust_id"
  FROM
      "friendly"
      INNER JOIN "friendly" on "friendly"."best_cust_filter_cust_id" = "friendly"."best_cust_filter_cust_id"
  WHERE
      "friendly"."best_cust_filter_cust_total" > 0.5 * "friendly"."best_cust_filter_overall_max"

  GROUP BY
      1),
  vacuous as (
  SELECT
      "juicy"."_freq_prefix_prefix" as "freq_prefix_prefix"
  FROM
      "juicy"),
  concerned as (
  SELECT
      "ss_item_items"."I_ITEM_SK" as "frequent_item_id"
  FROM
      "item" as "ss_item_items"
  WHERE
      SUBSTRING("ss_item_items"."I_ITEM_DESC",1,30) in (select vacuous."freq_prefix_prefix" from vacuous where vacuous."freq_prefix_prefix" is not null)

  GROUP BY
      1),
  rambunctious as (
  SELECT
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" as "ws_billing_customer_id",
      "ws_web_sales"."WS_LIST_PRICE" as "ws_list_price",
      "ws_web_sales"."WS_QUANTITY" as "ws_quantity",
      "ws_web_sales"."WS_SOLD_DATE_SK" as "ws_date_id"
  FROM
      "web_sales" as "ws_web_sales"
  WHERE
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" in (select kaput."best_cust_cust_id" from kaput where kaput."best_cust_cust_id" is not null) and "ws_web_sales"."WS_ITEM_SK" in (select concerned."frequent_item_id" from concerned where concerned."frequent_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "ws_web_sales"."WS_ITEM_SK"),
  highfalutin as (
  SELECT
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" as "cs_bill_customer_id",
      "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_date_id"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  WHERE
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select kaput."best_cust_cust_id" from kaput where kaput."best_cust_cust_id" is not null) and "cs_catalog_sales"."CS_ITEM_SK" in (select concerned."frequent_item_id" from concerned where concerned."frequent_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cs_catalog_sales"."CS_ITEM_SK"),
  hard as (
  SELECT
      "rambunctious"."ws_quantity" * "rambunctious"."ws_list_price" as "___tvf_arm_1_line_amt",
      "ws_billing_customer_customers"."C_FIRST_NAME" as "___tvf_arm_1_first_name",
      "ws_billing_customer_customers"."C_LAST_NAME" as "___tvf_arm_1_last_name"
  FROM
      "rambunctious"
      INNER JOIN "date_dim" as "ws_date_date" on "rambunctious"."ws_date_id" = "ws_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "ws_billing_customer_customers" on "rambunctious"."ws_billing_customer_id" = "ws_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2000 and "ws_date_date"."D_MOY" = 2

  GROUP BY
      1,
      2,
      3),
  protective as (
  SELECT
      "cs_bill_customer_customers"."C_FIRST_NAME" as "___tvf_arm_0_first_name",
      "cs_bill_customer_customers"."C_LAST_NAME" as "___tvf_arm_0_last_name",
      "highfalutin"."cs_quantity" * "highfalutin"."cs_list_price" as "___tvf_arm_0_line_amt"
  FROM
      "highfalutin"
      INNER JOIN "date_dim" as "cs_date_date" on "highfalutin"."cs_date_id" = "cs_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "cs_bill_customer_customers" on "highfalutin"."cs_bill_customer_id" = "cs_bill_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cs_date_date"."D_YEAR" = 2000 and "cs_date_date"."D_MOY" = 2

  GROUP BY
      1,
      2,
      3),
  yellow as (
  SELECT
      "protective"."___tvf_arm_0_last_name" as "_feb2000_last_name",
      "protective"."___tvf_arm_0_first_name" as "_feb2000_first_name",
      "protective"."___tvf_arm_0_line_amt" as "_feb2000_line_amt"
  FROM
      "protective"
  UNION ALL
  SELECT
      "hard"."___tvf_arm_1_last_name" as "_feb2000_last_name",
      "hard"."___tvf_arm_1_first_name" as "_feb2000_first_name",
      "hard"."___tvf_arm_1_line_amt" as "_feb2000_line_amt"
  FROM
      "hard")
  SELECT
      "yellow"."_feb2000_last_name" as "feb2000_last_name",
      "yellow"."_feb2000_first_name" as "feb2000_first_name",
      sum("yellow"."_feb2000_line_amt") as "total_sales"
  FROM
      "yellow"
  GROUP BY
      1,
      2
  ORDER BY
      "feb2000_last_name" asc,
      "feb2000_first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: (_duckdb.BinderException) Binder Error: Ambiguous reference to table "friendly" (duplicate alias "friendly", explicitly alias one of the tables using "AS my_alias")
  [SQL:
  WITH
  sparkling as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "_best_with_max_cust_id",
      sum("ss_store_sales"."SS_QUANTITY" * "ss_store_sales"."SS_SALES_PRICE") as "_best_with_max_cust_total",
      sum("ss_store_sales"."SS_QUANTITY" * "ss_store_sales"."SS_SALES_PRICE") as "cust_totals_cust_total"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
  WHERE
      "ss_date_date"."D_YEAR" >= 2000 and "ss_date_date"."D_YEAR" <= 2003 and "ss_store_sales"."SS_CUSTOMER_SK" is not null

  GROUP BY
      1),
  abundant as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2),
  late as (
  SELECT
      max("sparkling"."cust_totals_cust_total") as "_best_with_max_global_max"
  FROM
      "sparkling"),
  uneven as (
  SELECT
      "ss_item_items"."I_ITEM_SK" as "ss_item_id",
      SUBSTRING("ss_item_items"."I_ITEM_DESC",1,30) as "_freq_prefix_prefix"
  FROM
      "abundant"
      INNER JOIN "date_dim" as "ss_date_date" on "abundant"."ss_date_id" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ss_item_items" on "abundant"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_date_date"."D_YEAR" >= 2000 and "ss_date_date"."D_YEAR" <= 2003

  GROUP BY
      1,
      2),
  scrawny as (
  SELECT
      "late"."_best_with_max_global_max" as "_best_with_max_global_max",
      "sparkling"."_best_with_max_cust_id" as "_best_with_max_cust_id",
      "sparkling"."_best_with_max_cust_total" as "_best_with_max_cust_total"
  FROM
      "sparkling"
      FULL JOIN "late" on 1=1),
  yummy as (
  SELECT
      "uneven"."_freq_prefix_prefix" as "_freq_prefix_prefix"
  FROM
      "uneven"
  GROUP BY
      1
  HAVING
      count("uneven"."ss_item_id") > 4
  ),
  friendly as (
  SELECT
      "scrawny"."_best_with_max_cust_id" as "best_with_max_cust_id",
      "scrawny"."_best_with_max_cust_total" as "best_with_max_cust_total",
      "scrawny"."_best_with_max_global_max" as "best_with_max_global_max"
  FROM
      "scrawny"),
  juicy as (
  SELECT
      "yummy"."_freq_prefix_prefix" as "_freq_prefix_prefix"
  FROM
      "yummy"),
  kaput as (
  SELECT
      "friendly"."best_with_max_cust_id" as "best_cust_cust_id"
  FROM
      "friendly"
      INNER JOIN "friendly" on "friendly"."best_with_max_cust_id" = "friendly"."best_with_max_cust_id"
  WHERE
      "friendly"."best_with_max_cust_total" > 0.5 * "friendly"."best_with_max_global_max"

  GROUP BY
      1),
  vacuous as (
  SELECT
      "juicy"."_freq_prefix_prefix" as "freq_prefix_prefix"
  FROM
      "juicy"),
  concerned as (
  SELECT
      "ss_item_items"."I_ITEM_SK" as "frequent_item_id"
  FROM
      "item" as "ss_item_items"
  WHERE
      SUBSTRING("ss_item_items"."I_ITEM_DESC",1,30) in (select vacuous."freq_prefix_prefix" from vacuous where vacuous."freq_prefix_prefix" is not null)

  GROUP BY
      1),
  rambunctious as (
  SELECT
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" as "ws_billing_customer_id",
      "ws_web_sales"."WS_LIST_PRICE" as "ws_list_price",
      "ws_web_sales"."WS_QUANTITY" as "ws_quantity",
      "ws_web_sales"."WS_SOLD_DATE_SK" as "ws_date_id"
  FROM
      "web_sales" as "ws_web_sales"
  WHERE
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" in (select kaput."best_cust_cust_id" from kaput where kaput."best_cust_cust_id" is not null) and "ws_web_sales"."WS_ITEM_SK" in (select concerned."frequent_item_id" from concerned where concerned."frequent_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "ws_web_sales"."WS_ITEM_SK"),
  highfalutin as (
  SELECT
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" as "cs_bill_customer_id",
      "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_date_id"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  WHERE
      "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select kaput."best_cust_cust_id" from kaput where kaput."best_cust_cust_id" is not null) and "cs_catalog_sales"."CS_ITEM_SK" in (select concerned."frequent_item_id" from concerned where concerned."frequent_item_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      "cs_catalog_sales"."CS_ITEM_SK"),
  hard as (
  SELECT
      "rambunctious"."ws_quantity" * "rambunctious"."ws_list_price" as "___tvf_arm_1_line_amt",
      "ws_billing_customer_customers"."C_FIRST_NAME" as "___tvf_arm_1_first_name",
      "ws_billing_customer_customers"."C_LAST_NAME" as "___tvf_arm_1_last_name"
  FROM
      "rambunctious"
      INNER JOIN "date_dim" as "ws_date_date" on "rambunctious"."ws_date_id" = "ws_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "ws_billing_customer_customers" on "rambunctious"."ws_billing_customer_id" = "ws_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2000 and "ws_date_date"."D_MOY" = 2

  GROUP BY
      1,
      2,
      3),
  protective as (
  SELECT
      "cs_bill_customer_customers"."C_FIRST_NAME" as "___tvf_arm_0_first_name",
      "cs_bill_customer_customers"."C_LAST_NAME" as "___tvf_arm_0_last_name",
      "highfalutin"."cs_quantity" * "highfalutin"."cs_list_price" as "___tvf_arm_0_line_amt"
  FROM
      "highfalutin"
      INNER JOIN "date_dim" as "cs_date_date" on "highfalutin"."cs_date_id" = "cs_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "cs_bill_customer_customers" on "highfalutin"."cs_bill_customer_id" = "cs_bill_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cs_date_date"."D_YEAR" = 2000 and "cs_date_date"."D_MOY" = 2

  GROUP BY
      1,
      2,
      3),
  yellow as (
  SELECT
      "protective"."___tvf_arm_0_last_name" as "_feb2000_last_name",
      "protective"."___tvf_arm_0_first_name" as "_feb2000_first_name",
      "protective"."___tvf_arm_0_line_amt" as "_feb2000_line_amt"
  FROM
      "protective"
  UNION ALL
  SELECT
      "hard"."___tvf_arm_1_last_name" as "_feb2000_last_name",
      "hard"."___tvf_arm_1_first_name" as "_feb2000_first_name",
      "hard"."___tvf_arm_1_line_amt" as "_feb2000_line_amt"
  FROM
      "hard")
  SELECT
      "yellow"."_feb2000_last_name" as "feb2000_last_name",
      "yellow"."_feb2000_first_name" as "feb2000_first_name",
      sum("yellow"."_feb2000_line_amt") as "total_sales"
  FROM
      "yellow"
  GROUP BY
      1,
      2
  ORDER BY
      "feb2000_last_name" asc,
      "feb2000_first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Comparison `sr.return_date.month_of_year <= 12` matches every value of enum field 'sr.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
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
      avg(CASE WHEN "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ADDR_SK" is null THEN "ss_store_sales"."SS_NET_PROFIT" ELSE NULL END) as "avg_null_addr_profit"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1),
  questionable as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      "ss_store_sales"."SS_NET_PROFIT" as "ss_net_profit",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
      "thoughtful"."avg_null_addr_profit" * 0.9 as "threshold"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "thoughtful" on "ss_store_sales"."SS_ITEM_SK" = "thoughtful"."ss_item_id"),
  abundant as (
  SELECT
      "questionable"."ss_store_id" as "ss_store_id",
      CASE WHEN avg("questionable"."ss_net_profit") > "questionable"."threshold" and "questionable"."ss_store_id" = 1 THEN "questionable"."ss_item_id" ELSE NULL END as "_virt_filter_id_5033071990553107"
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
      "abundant"."_virt_filter_id_5033071990553107" as "_virt_filter_id_5033071990553107"
  FROM
      "abundant"
  WHERE
      "abundant"."ss_store_id" = 1
  ),
  highfalutin as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      avg("ss_store_sales"."SS_NET_PROFIT") as "_items_ranked_avg_profit"
  FROM
      "store_sales" as "ss_store_sales"
  WHERE
      "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ITEM_SK" in (select uneven."_virt_filter_id_5033071990553107" from uneven where uneven."_virt_filter_id_5033071990553107" is not null)

  GROUP BY
      1),
  yummy as (
  SELECT
      "highfalutin"."_items_ranked_avg_profit" as "_items_ranked_avg_profit",
      "ss_item_items"."I_ITEM_SK" as "ss_item_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name"
  FROM
      "highfalutin"
      INNER JOIN "item" as "ss_item_items" on "highfalutin"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_item_items"."I_ITEM_SK" in (select uneven."_virt_filter_id_5033071990553107" from uneven where uneven."_virt_filter_id_5033071990553107" is not null)
  ),
  juicy as (
  SELECT
      "yummy"."_items_ranked_avg_profit" as "items_ranked_avg_profit",
      "yummy"."ss_item_id" as "items_ranked_ss_item_id",
      "yummy"."ss_item_product_name" as "items_ranked_ss_item_product_name"
  FROM
      "yummy"),
  vacuous as (
  SELECT
      "juicy"."items_ranked_ss_item_id" as "items_ranked_ss_item_id",
      rank() over (order by "juicy"."items_ranked_avg_profit" asc ) as "_best10_rnk",
      rank() over (order by "juicy"."items_ranked_avg_profit" desc ) as "_worst10_rnk"
  FROM
      "juicy"),
  concerned as (
  SELECT
      "juicy"."items_ranked_ss_item_product_name" as "items_ranked_ss_item_product_name",
      "vacuous"."_best10_rnk" as "_best10_rnk",
      "vacuous"."_worst10_rnk" as "_worst10_rnk"
  FROM
      "vacuous"
      INNER JOIN "juicy" on "vacuous"."items_ranked_ss_item_id" = "juicy"."items_ranked_ss_item_id"),
  young as (
  SELECT
      "concerned"."_best10_rnk" as "_best10_rnk",
      "concerned"."_worst10_rnk" as "_worst10_rnk",
      "concerned"."items_ranked_ss_item_product_name" as "items_ranked_ss_item_product_name"
  FROM
      "concerned"
  LIMIT (10)),
  abhorrent as (
  SELECT
      "young"."_worst10_rnk" as "worst10_rnk",
      "young"."items_ranked_ss_item_product_name" as "worst10_items_ranked_ss_item_product_name"
  FROM
      "young"),
  sparkling as (
  SELECT
      "young"."_best10_rnk" as "best10_rnk",
      "young"."items_ranked_ss_item_product_name" as "best10_items_ranked_ss_item_product_name"
  FROM
      "young")
  SELECT
      coalesce("abhorrent"."worst10_rnk","sparkling"."best10_rnk") as "rank",
      "sparkling"."best10_items_ranked_ss_item_product_name" as "best_performer",
      "abhorrent"."worst10_items_ranked_ss_item_product_name" as "worst_performer"
  FROM
      "abhorrent"
      FULL JOIN "sparkling" on "abhorrent"."worst10_rnk" = "sparkling"."best10_rnk"
  GROUP BY
      1,
      2,
      3
  ORDER BY
      "rank" asc,
      "best_performer" desc,
      "worst_performer" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query44.preql`

  ```text
  Unexpected error in query44.preql: (_duckdb.BinderException) Binder Error: column "SS_ITEM_SK" must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(SS_ITEM_SK)" if the exact value of "SS_ITEM_SK" is not important.

  LINE 11: ..."."threshold" and "ss_store_sales"."SS_STORE_SK" = 1 THEN "ss_store_sales"."SS_ITEM_SK" ELSE NULL END as "_virt_filte...
                                                                        ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      avg(CASE WHEN "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ADDR_SK" is null THEN "ss_store_sales"."SS_NET_PROFIT" ELSE NULL END) * 0.9 as "threshold"
  FROM
      "store_sales" as "ss_store_sales"),
  questionable as (
  SELECT
      "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
      CASE WHEN avg("ss_store_sales"."SS_NET_PROFIT") > "thoughtful"."threshold" and "ss_store_sales"."SS_STORE_SK" = 1 THEN "ss_store_sales"."SS_ITEM_SK" ELSE NULL END as "_virt_filter_id_5033071990553107"
  FROM
      "thoughtful"
      LEFT OUTER JOIN "store_sales" as "ss_store_sales" on 1=1
  GROUP BY
      1,
      "thoughtful"."threshold"
  HAVING
      avg("ss_store_sales"."SS_NET_PROFIT") > "thoughtful"."threshold"
  ),
  uneven as (
  SELECT
      "questionable"."_virt_filter_id_5033071990553107" as "_virt_filter_id_5033071990553107"
  FROM
      "questionable"
  WHERE
      "questionable"."ss_store_id" = 1
  ),
  highfalutin as (
  SELECT
      "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
      avg("ss_store_sales"."SS_NET_PROFIT") as "_items_ranked_avg_profit"
  FROM
      "store_sales" as "ss_store_sales"
  WHERE
      "ss_store_sales"."SS_STORE_SK" = 1 and "ss_store_sales"."SS_ITEM_SK" in (select uneven."_virt_filter_id_5033071990553107" from uneven where uneven."_virt_filter_id_5033071990553107" is not null)

  GROUP BY
      1),
  yummy as (
  SELECT
      "highfalutin"."_items_ranked_avg_profit" as "_items_ranked_avg_profit",
      "ss_item_items"."I_ITEM_SK" as "ss_item_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name"
  FROM
      "highfalutin"
      INNER JOIN "item" as "ss_item_items" on "highfalutin"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_item_items"."I_ITEM_SK" in (select uneven."_virt_filter_id_5033071990553107" from uneven where uneven."_virt_filter_id_5033071990553107" is not null)
  ),
  juicy as (
  SELECT
      "yummy"."_items_ranked_avg_profit" as "items_ranked_avg_profit",
      "yummy"."ss_item_id" as "items_ranked_ss_item_id",
      "yummy"."ss_item_product_name" as "items_ranked_ss_item_product_name"
  FROM
      "yummy"),
  vacuous as (
  SELECT
      "juicy"."items_ranked_ss_item_id" as "items_ranked_ss_item_id",
      rank() over (order by "juicy"."items_ranked_avg_profit" asc ) as "_best10_rnk",
      rank() over (order by "juicy"."items_ranked_avg_profit" desc ) as "_worst10_rnk"
  FROM
      "juicy"),
  concerned as (
  SELECT
      "juicy"."items_ranked_ss_item_product_name" as "items_ranked_ss_item_product_name",
      "vacuous"."_best10_rnk" as "_best10_rnk",
      "vacuous"."_worst10_rnk" as "_worst10_rnk"
  FROM
      "vacuous"
      INNER JOIN "juicy" on "vacuous"."items_ranked_ss_item_id" = "juicy"."items_ranked_ss_item_id"),
  young as (
  SELECT
      "concerned"."_best10_rnk" as "_best10_rnk",
      "concerned"."_worst10_rnk" as "_worst10_rnk",
      "concerned"."items_ranked_ss_item_product_name" as "items_ranked_ss_item_product_name"
  FROM
      "concerned"
  LIMIT (10)),
  abhorrent as (
  SELECT
      "young"."_worst10_rnk" as "worst10_rnk",
      "young"."items_ranked_ss_item_product_name" as "worst10_items_ranked_ss_item_product_name"
  FROM
      "young"),
  sparkling as (
  SELECT
      "young"."_best10_rnk" as "best10_rnk",
      "young"."items_ranked_ss_item_product_name" as "best10_items_ranked_ss_item_product_name"
  FROM
      "young")
  SELECT
      coalesce("abhorrent"."worst10_rnk","sparkling"."best10_rnk") as "rank",
      "sparkling"."best10_items_ranked_ss_item_product_name" as "best_performer",
      "abhorrent"."worst10_items_ranked_ss_item_product_name" as "worst_performer"
  FROM
      "abhorrent"
      FULL JOIN "sparkling" on "abhorrent"."worst10_rnk" = "sparkling"."best10_rnk"
  GROUP BY
      1,
      2,
      3
  ORDER BY
      "rank" asc,
      "best_performer" desc,
      "worst_performer" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write -c import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_sales as ss;
import raw.item as item;

# Item-level:…ext_list > 2 * cum_refund
;

select qualifying_items.item_text_id, qualifying_items.cum_ext_list, qualifying_items.cum_refund
limit 10; query64.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query64.preql duckdb`

  ```text
  Syntax error in query64.preql: Undefined concept: local.cum_ext_list. Suggestions: ['item_cs_extlist.cum_ext_list', 'cs.ext_list_price']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query04.preql duckdb`

  ```text
  Syntax error in query04.preql: Nothing was executed: parsed 7 definition statement(s) (4 rowsets, 3 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query04.preql duckdb`

  ```text
  Syntax error in query04.preql: Nothing was executed: parsed 10 definition statement(s) (7 rowsets, 3 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query04.preql duckdb`

  ```text
  Unexpected error in query04.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 6 undefined concept references; fix all before re-running:
    - store_sales.customer_demographics.marital_status (line 7, col 8, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 8, col 12, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
    - store_sales.customer_demographics.marital_status (line 12, col 8, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 13, col 12, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
    - store_sales.customer_demographics.marital_status (line 17, col 8, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 18, col 12, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
  ```
- `trilogy run query16.preql`

  ```text
  Syntax error in query16.preql: Undefined concept: raw.catalog_returns.order_number (line 16, col 30, in WHERE). Suggestions: ['cs.order_number', 'multi_warehouse.cs.order_number']
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(cs.bill_customer.address.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(cs.bill_customer.address.country) as g order by g desc`.
  ```
- `trilogy run query20.preql`

  ```text
  Syntax error in query20.preql: 4 undefined concept references; fix all before re-running:
    - item_category (line 28, col 5, in ORDER BY); did you mean: item_totals.item_category, item.category, item.category_id, cs.item.category?
    - item_class (line 29, col 5, in ORDER BY); did you mean: item_totals.item_class, item.class, item.class_id, cs.item.class?
    - item_code (line 30, col 5, in ORDER BY); did you mean: item_totals.item_code, item.desc, item.color, item.id?
    - item_desc (line 31, col 5, in ORDER BY); did you mean: item_totals.item_desc, item.desc, cs.item.desc, item.id?
  ```
- `trilogy file read query25.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query26.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query27.preql`

  ```text
  Syntax error in query27.preql: Comparison `ss.customer_demographic.marital_status = 'Single'` can never match enum field 'ss.customer_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy run query31.preql`

  ```text
  Resolution error in query31.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 2 disconnected subgraphs: {store_by_county_q.county, store_by_county_q.quarter, store_by_county_q.store_ext_total}; {web_by_county_q.county}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query31.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query32.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query37.preql`

  ```text
  Syntax error in query37.preql: 5 undefined concept references; fix all before re-running:
    - item.current_price (line 14, col 3, in SELECT); did you mean: cs.item.current_price, inv.item.current_price, item.text_id?
    - item.current_price (line 6, col 3, in WHERE); did you mean: cs.item.current_price, inv.item.current_price, item.text_id?
    - item.manufacturer_id (line 7, col 7, in WHERE); did you mean: cs.item.manufacturer_id, inv.item.manufacturer_id, cs.item.manager_id?
    - item.id (line 10, col 7, in WHERE); did you mean: cs.item.id, inv.item.id, cs.date.id, cs.ship_date.id, cs.sold_date.id, cs.time.id?
    - item.text_id (line 16, col 3, in ORDER BY); did you mean: cs.item.text_id, inv.item.text_id, cs.date.text_id, cs.ship_date.text_id, cs.sold_date.text_id, cs.time.text_id?
  ```
- `trilogy file read query37.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs select count(ss.customer.last_name) as cnt where ss.date.year = 2000 and cs.sold_date.y…stomer.last_name = cs.bill_customer.last_name and ss.customer.first_name = cs.bill_customer.first_name and ss.date.date = cs.sold_date.date limit 10;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cs.bill_customer.first_name, cs.bill_customer.id, cs.bill_customer.last_name, cs.sold_date.date, cs.sold_date.year}; {ss.customer.first_name, ss.customer.id, ss.customer.last_name, ss.date.date, ss.date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales select date.day_of_week limit 100;`

  ```text
  Syntax error in stdin: Undefined concept: date.day_of_week (line 2, col 8, in SELECT). Suggestions: ['store_sales.date.day_of_week', 'store_sales.store.date.day_of_week', 'store_sales.return_store.date.day_of_week', 'store_sales.return_date.day_of_week', 'store_sales.customer.first_sales_date.day_of_week', 'store_sales.customer.first_shipto_date.day_of_week']
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query50.preql`

  ```text
  Syntax error in query50.preql: Undefined concept: ss.return_date. Suggestions: ['ss.return_date.id', 'ss.return_date.text_id', 'ss.return_date._date_string', 'ss.return_date.date', 'ss.return_date.year', 'ss.return_date.day_of_week']
  ```
- `trilogy file read query52.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query53.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query57.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query62.preql`

  ```text
  Syntax error in query62.preql: 5 undefined concept references; fix all before re-running:
    - warehouse.name (line 4, in SELECT); did you mean: web.warehouse.name, web.web_site.name, web_site.name, warehouse.id, warehouse_name_prefix?
    - ship_date.year (line 5, col 3, in WHERE); did you mean: web.ship_date.year, web.date.year, web.return_date.year, web.billing_customer.first_sales_date.year, web.billing_customer.first_shipto_date.year, web.ship_customer.first_sales_date.year?
    - warehouse.id (line 6, col 7, in WHERE); did you mean: web.warehouse.id, web.item.id, web.date.id, web.ship_date.id, web.return_date.id, web.time.id?
    - ship_mode.id (line 7, col 7, in WHERE); did you mean: web.ship_mode.id, web.item.id, web.date.id, web.ship_date.id, web.return_date.id, web.time.id?
    - web_site.id (line 8, col 7, in WHERE); did you mean: web.web_site.id, web.item.id, web.date.id, web.ship_date.id, web.return_date.id, web.time.id?
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 6). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
  ```
- `trilogy file read query69.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query72.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query74.preql`

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
- `trilogy run query77.preql duckdb`

  ```text
  Syntax error in query77.preql: Undefined concept: s.channel_label (line 12, in SELECT). Suggestions: ['channel_label', 's.channel', 's.channel_dim_id']
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {ss.item.current_price, ss.item.desc, ss.item.manufacturer_id, ss.item.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.text_id, description, item_code, price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.text_id, description, item_code, price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query82.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query84.preql`

  ```text
  Unexpected error in query84.preql: Cannot join `ret.ticket_number` to itself (`ret.ticket_number` resolves to the same key `ret.ticket_number`), which degenerates to `1=1`. Join distinct keys (e.g. separate rowset outputs or distinct expressions).
  ```
- `trilogy run query84.preql`

  ```text
  Resolution error in query84.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cust.address.city, cust.demographics.id, cust.household_demographic.income_band.lower_bound, cust.household_demographic.income_band.upper_bound, customer_code, full_name}; {ret.customer_demographic.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query85.preql duckdb`

  ```text
  Syntax error in query85.preql: Comparison `wr.returning_demographic.marital_status = 'Married'` can never match enum field 'wr.returning_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read query87.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query95.preql`

  ```text
  Syntax error in query95.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 9). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy run query97.preql`

  ```text
  Syntax error in query97.preql: Undefined concept: in_store.
  ```
- `trilogy file read query97.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;

# Combine web and catalog sales per date
with web_b…es / dow_with_next.next_year_sales, 2) as ratio_sun,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
where
    ...
order by
    dow_with_next.week_seq asc
;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 51:5
     |
  51 |     ?,
     |     ^---
     |
     = expected select_item, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...r_sales, 2) as ratio_sun,     ??? ?,     ?,     ?,     ?,     ?,...

  Write stats: received 1452 chars / 1452 bytes; tail: …'   ?\\nwhere\\n    ...\\norder by\\n    dow_with_next.week_seq asc\\n;'.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web;
import raw.catalog_sales as cat;
import raw.date as d;

# Web sales by date
with web_…ratio_fri,
    round(sum(agg.tot ? agg.dow = 6) / sum(agg.fut ? agg.dow = 6), 2) as ratio_sat
where
    agg.ws in wk2001.ws
order by
    agg.ws asc
;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...as ws where     d.year = 2001 ??? group by d.week_seq ;  # Now p...

  Write stats: received 1726 chars / 1726 bytes; tail: …'_sat\\nwhere\\n    agg.ws in wk2001.ws\\norder by\\n    agg.ws asc\\n;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

with combined as union(
    # Sales arm: channel_dim_id is the sale entity
    (where …sort_entity
by rollup (combined.channel, combined.entity_id)
order by combined.channel asc nulls first, combined.entity_id asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 51:6
     |
  51 |     --     when grouping(combined.entity_id) = 1 then 'zz' || case combined.channel
     |      ^---
     |
     = expected access_chain
  Location:
  ....channel) = 1 then 'zzz'     - ??? -     when grouping(combined.e...

  Write stats: received 3052 chars / 3052 bytes; tail: …'c nulls first, combined.entity_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:sales select sales.channel, count(sales.sale_line_item_counter) as cnt where sales.date.year between 1999 and 2001 and sales.item.brand_id is null and sales.item.class_id is null and sales.item.category_id is null group by sales.channel;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ales.item.category_id is null ??? group by sales.channel;
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequent item…st_name,
    sum(combined.line_amt) as total_sales,
order by combined.last_name asc, combined.first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...select ss2.item.id            ??? from raw.store_sales as ss2

  Write stats: received 2616 chars / 2616 bytes; tail: …'ined.first_name asc, total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequent item….first_name,
    sum(feb2000.line_amt) as total_sales
order by feb2000.last_name asc, feb2000.first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ct max_cust_total.overall_max ??? from max_cust_total) ;  # Step...

  Write stats: received 2355 chars / 2355 bytes; tail: …'2000.first_name asc, total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Step 1: Frequent item….first_name,
    sum(feb2000.line_amt) as total_sales
order by feb2000.last_name asc, feb2000.first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...cust_total > 0.5 * (select mx ??? from max_total) ;  # Step 4: C...

  Write stats: received 2035 chars / 2035 bytes; tail: …'2000.first_name asc, total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.desc, substring(ss.item.desc, 1, 30) as prefix, count(ss.item.id) as cnt where ss.date.year >= 2000 and ss.date.year <= 2003 group by ss.item.desc, prefix having cnt > 4 order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...2000 and ss.date.year <= 2003 ??? group by ss.item.desc, prefix
  ```
- `trilogy run --import raw/store_sales:ss select max(total) by * as overall_max from (select ss.customer.id as cust_id, sum(ss.quantity * ss.sales_price) as total where ss.date.year >= 2000 and ss.date.year <= 2003 and ss.customer.id is not null);`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ax(total) by * as overall_max ??? from (select ss.customer.id as...
  ```
- `trilogy run --import raw/catalog_sales:cs select cs.bill_customer.id as cid, count(cs.bill_customer.id) as cnt where cs.date.year = 2000 and cs.date.month_of_year = 2 and cs.bill_customer.id is not null group by cid order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....bill_customer.id is not null ??? group by cid order by cnt desc...
  ```
- `trilogy file write test6.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

rowset freq_prefix <-
where ss.date.year >= 2000 and …also frequent
select distinct cs.item.id as iid
where cs.date.year = 2000 and cs.date.month_of_year = 2
  and cs.item.id in frequent_items.item_id
;
`

  ```text
  refused to write 'test6.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...that are also frequent select ??? distinct cs.item.id as iid whe...

  Write stats: received 616 chars / 616 bytes; tail: …'th_of_year = 2\\n  and cs.item.id in frequent_items.item_id\\n;\\n'.
  ```
- `trilogy file write query64.preql`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset item_cs_extlist as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ist_price from catalog_sales
   ??? rowset item_cs_extlist as
   sel...

  Write stats: received 997 chars / 997 bytes; tail: …'g_items.cum_ext_list, qualifying_items.cum_refund\r\\nlimit 10;'.
  ```
- `trilogy file write query01.preql --content import raw.store_returns as sr;

auto cust_store_returns <- sum(sr.return_amount) by sr.billing_customer.id, sr.st…d
    --cust_store_returns,
    --store_avg_return
having
    cust_store_returns > 1.2 * store_avg_return
order by
    billing_customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:5
     |
  11 |     --cust_store_returns,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...id as billing_customer_id     ??? --cust_store_returns,     --st...

  Write stats: received 464 chars / 464 bytes; tail: …'store_avg_return\\norder by\\n    billing_customer_id\\nlimit 100;'.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store: annual value p…f_flag,
union join store_annual.cust_id = catalog_annual.cust_id = web_annual.cust_id
union join store_annual.yr = catalog_annual.yr = web_annual.yr
`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...log_annual.yr = web_annual.yr ???

  Write stats: received 1360 chars / 1360 bytes; tail: …'on join store_annual.yr = catalog_annual.yr = web_annual.yr\\n'.
  ```
- `trilogy file write query13.preql --content import raw.store_sales as store_sales;

# Year 2001
where store_sales.date.year = 2001

# Demographic-and-price cl…ext_sales_price,
  avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,
  sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost
;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | where (
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...+ state/net_profit conditions ??? where (   # Combination 1: mar...

  Write stats: received 1945 chars / 1945 bytes; tail: …'tore_sales.ext_wholesale_cost) as total_ext_wholesale_cost\\n;'.
  ```
- `trilogy file write query16.preql --content import raw.catalog_sales as cs;

# Orders that ship from more than one warehouse
auto multi_warehouse_orders <- cs…orders,
    sum(cs.ext_ship_cost) as total_extended_ship_cost,
    sum(cs.net_profit) as total_net_profit
order by
    unique_orders desc
limit 100;
`

  ```text
  refused to write 'query16.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:41
    |
  7 | auto returned_orders <- cs.order_number where cs.order_number in raw.catalog_returns.order_number;
    |                                         ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ned_orders <- cs.order_number ??? where cs.order_number in raw.c...

  Write stats: received 785 chars / 785 bytes; tail: …'total_net_profit\\norder by\\n    unique_orders desc\\nlimit 100;\\n'.
  ```
- `trilogy file write query21.preql --content import raw/inventory as inv;

# Before window: 2000-02-10 <= date < 2000-03-11
auto inv_before <- sum(inv.quantity…fore >= 2.0/3.0
  and inv_after / inv_before <= 3.0/2.0
order by
  inv.warehouse.name asc nulls first,
  inv.item.text_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/inventory as inv;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/inventory as inv;  # Befor...

  Write stats: received 864 chars / 864 bytes; tail: …' nulls first,\\n  inv.item.text_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query26.preql --content import raw/catalog_sales as cs;

where cs.sold_date.year = 2000
  and cs.bill_customer_demographic.gender = 'M'
  …rice) as avg_list_price,
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

  Write stats: received 549 chars / 549 bytes; tail: …') as avg_sales_price\\norder by\\n    cs.item.text_id\\nlimit 100;'.
  ```
- `trilogy file write query28.preql --content import raw.store_sales as store_sales;

# Bucket 1: ticket quantity 0 to 5, and (list price 8-18 OR coupon 459-145…list_price_5,
    bucket5.unique_list_price_5,
    bucket6.avg_list_price_6,
    bucket6.count_list_price_6,
    bucket6.unique_list_price_6
limit 1;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:1
     |
  13 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...price) as unique_list_price_1 ??? by *;  # Bucket 2: ticket quan...

  Write stats: received 3726 chars / 3726 bytes; tail: …'count_list_price_6,\\n    bucket6.unique_list_price_6\\nlimit 1;'.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store sales tuples: c…ame
union join ss_tuples.sale_date = ws_tuples.sale_date
where
    cs_tuples.last_name is not null
    and ws_tuples.last_name is not null
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...aring in all three select     ??? count(*) as combo_count union

  Write stats: received 1545 chars / 1545 bytes; tail: …' not null\\n    and ws_tuples.last_name is not null\\nlimit 100;'.
  ```
- `trilogy run --import raw.web_sales:ws select distinct substring(ws.billing_customer.address.zip, 1, 5) as zip_prefix where substring(ws.billing_customer.address.zip, 1, 5) in ('85669','86197','88274','83405','86475','85392','85460','80348','81792') limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...t raw.web_sales as ws; select ??? distinct substring(ws.billing_...
  ```
- `trilogy run --import raw/store_sales:store_sales select distinct date.day_of_week order by 1 limit 100;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ..._sales as store_sales; select ??? distinct date.day_of_week orde...
  ```
- `trilogy file write query47.preql --content import raw.store_sales as ss;

# Monthly total of unit price per (category, brand, store, company, year, month)
au…c,
    ss.date.year asc,
    ss.date.month_of_year asc,
    avg_monthly asc,
    monthly_total asc,
    prev_total asc,
    next_total asc
limit 100;`

  ```text
  refused to write 'query47.preql': not syntactically valid Trilogy.

  Parse error:
    --> 36:1
     |
  36 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
     and next_total is not null ??? select     ss.item.category,

  Write stats: received 2334 chars / 2334 bytes; tail: …'total asc,\\n    prev_total asc,\\n    next_total asc\\nlimit 100;'.
  ```
- `trilogy file write query51.preql --content import raw.web_sales as ws;
import raw.store_sales as ss;

# Per-item, per-day: sum of sales_price (unit price) fo…rows between unbounded preceding and current row) as store_running_max
order by combined.item asc nulls first, combined.dt asc nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
    --> 38:80
     |
  38 |     sum(coalesce(combined.web_unit_price, 0)) over (partition by combined.item order by combined.dt asc rows between unbounded preceding and current row) as web_running_total,
     |                                                                                ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...r (partition by combined.item ??? order by combined.dt asc rows

  Write stats: received 1984 chars / 1984 bytes; tail: …'item asc nulls first, combined.dt asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss where ss.customer.id = (select ss2.customer.id from raw.store_sales as ss2 where ss2.date.month_seq between 1188 and … and ss2.item.category = 'Women' limit 1) and ss.date.month_seq between 1188 and 1190 select ss.customer.id, sum(ss.ext_sales_price) as tot limit 10;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ....id = (select ss2.customer.id ??? from raw.store_sales as ss2 wh...
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Define a macro to sum sales_price for a given day-of-week (0=Sunday .. 6=Saturday…not null
order by this_yr.store_name asc nulls first,
         this_yr.store_code asc nulls first,
         this_yr.wk_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset this_yr as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   data, pivoted by day-of-week ??? rowset this_yr as where ss.dat...

  Write stats: received 2079 chars / 2079 bytes; tail: …'ls first,\\n         this_yr.wk_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query65.preql --content import raw.store_sales as ss;

auto revenue_per_store_item <- sum(ss.sales_price) by ss.store.id, ss.item.id;
auto…aving revenue_per_store_item <= 0.1 * store_avg_revenue
order by ss.store.name, ss.item.desc, ss.store.id, ss.item.id
    with nulls first
limit 100;`

  ```text
  refused to write 'query65.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:5
     |
  17 |     with nulls first
     |     ^---
     |
     = expected limit, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..., ss.store.id, ss.item.id     ??? with nulls first limit 100;

  Write stats: received 534 chars / 534 bytes; tail: …'esc, ss.store.id, ss.item.id\\n    with nulls first\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

with combined …tal_ext_sales_price
order by
  combined.channel,
  combined.missing_ref_label,
  combined.yr,
  combined.qtr,
  combined.cat
  nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ???  select   combined.channel,

  Write stats: received 1494 chars / 1494 bytes; tail: …'.yr,\\n  combined.qtr,\\n  combined.cat\\n  nulls first\\nlimit 100;'.
  ```
- `trilogy file write query82.preql --content import raw.store_sales as ss;
import raw.inventory as inv;
import raw.item as item;

subset join item.text_id = ss…07-24'::date
select
    item.text_id as item_code,
    item.desc as description,
    item.current_price as price
order by
    item.text_id
limit 100;`

  ```text
  refused to write 'query82.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | where
    | ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...m.text_id = inv.item.text_id  ??? where     item.current_price b...

  Write stats: received 530 chars / 530 bytes; tail: …'.current_price as price\\norder by\\n    item.text_id\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy explore raw/customer_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_demographics.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy explore ./raw/sales/sales.preql`

  ```text
  Invalid value for 'PATH': File './raw/sales/sales.preql' does not exist.
  ```
- `trilogy explore raw/customer_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_demographics.preql' does not exist.
  ```
- `trilogy explore raw/household_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/household_demographics.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/catalog_sales:cs select count(cs.bill_customer.id) where cs.date.year = 2000 and cs.date.month_of_year = 2 and cs.bill_customer.id is not null;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cs.bill_customer.id) as id_count`
  Location:
  ...ct count(cs.bill_customer.id) ??? where cs.date.year = 2000 and
  ```
- `trilogy run --import raw.catalog_returns:cr select count(cr.order_number), count(cr.order_number ? cr.order_number is not null) as non_null, count(cr.order_number ? cr.order_number is null) as null_count;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cr.order_number) as order_number_count`
  Location:
   select count(cr.order_number) ??? , count(cr.order_number ? cr.o...
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, sales.date.month_of_year, count(sales.order_id), sum(sales.return_amount) where sales.return_amount > 10000 group by sales.date.year, sales.date.month_of_year order by sales.date.year, sales.date.month_of_year;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(sales.order_id) as order_id_count`
  Location:
  ...of_year, count(sales.order_id) ??? , sum(sales.return_amount) whe...
  ```

### `file-not-found`

- `trilogy run --import raw/catalog_sales:cs --import raw/store_sales:ss select count(cs.bill_customer.id) as cnt where cs.date.year = 2000 and cs.date.month_of…ss.customer.id where ss.date.year >= 2000 and ss.date.year <= 2003 and ss.customer.id is not null having sum(ss.quantity * ss.sales_price) > 118133);`

  ```text
  Input 'raw/store_sales:ss' does not exist.
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/store_sales:ss select cs.item.id as iid, count(cs.item.id) as cnt where cs.date.year = 2000 and cs.dat…s.item.id where ss.date.year >= 2000 and ss.date.year <= 2003 and substring(ss.item.desc, 1, 30) in ('S','D','H','R','P','O','T','C','Co')) limit 10;`

  ```text
  Input 'raw/store_sales:ss' does not exist.
  ```

### `type-error`

- `trilogy run query38.preql`

  ```text
  Type error in query38.preql: Invalid argument type 'DATE' passed into CONCAT function in position 2 from concept: ss_tuples.ss.date.date. Valid: 'STRING'.
  ```
