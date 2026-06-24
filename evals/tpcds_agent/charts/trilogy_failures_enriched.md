# Trilogy failure analysis — 20260624-133456

- Run `20260624-133456` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1562 | failed: 185 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 110 | 59% |
| `syntax-parse` | 45 | 24% |
| `cli-misuse` | 9 | 5% |
| `syntax-missing-alias` | 8 | 4% |
| `join-resolution` | 8 | 4% |
| `undefined-concept` | 3 | 2% |
| `file-not-found` | 1 | 1% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: _virt_filter_dow_sum_1387793287534767.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 82 column 3 (char 2483). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: Undefined concept: sv.
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: HAVING references 's01.val', 'c01.val', 'w01.val', 'c02.val', 's02.val', 'w02.val', which are not in the SELECT projection (line 57). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --s01.val, --c01.val, --w01.val, --c02.val, --s02.val, --w02.val
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 63 (char 62). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel_type)` (line 36), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_type) as g order by g desc`.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel_type)` (line 36), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_type) as g order by g desc`.
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: 4 undefined concept references; fix all before re-running:
    - local.line_item (line 7, in SELECT); did you mean: ss.line_item, line_item_count, ss.line_item_count?
    - item.current_price (line 12, col 7, in WHERE); did you mean: ss.item.current_price?
    - item.category (line 13, col 7, in WHERE); did you mean: ss.item.category, ss.item.category_id, ss.item.color?
    - customer.address.id (line 14, col 7, in WHERE); did you mean: ss.customer.address.id, ss.item.id, ss.date.id, ss.return_date.id, ss.time.id, ss.return_time.id?
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.preferred_cust_flag. Suggestions: ['ss.customer.preferred_cust_flag', 'ss.return_customer.preferred_cust_flag', 'ss.customer.first_name']
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unexpected error in query08.preql: (_duckdb.BinderException) Binder Error: Cannot compare values of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is required

  LINE 41:     "ss_customer_address_customer_address"."CA_ZIP" in (select quizzical."zips_array" from quizzical where quiz...
                                                               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "ss_customer_address_customer_address"."CA_ZIP" as "ss_customer_address_zip",
      count(CASE WHEN "ss_customer_customers"."C_PREFERRED_CUST_FLAG" = 'Y' THEN "ss_customer_customers"."C_CUSTOMER_SK" ELSE NULL END) as "_virt_agg_count_1049178494825001"
  FROM
      "customer" as "ss_customer_customers"
      INNER JOIN "customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
  GROUP BY
      1),
  quizzical as (
  SELECT
      STRING_SPLIT( $1 , ',' ) as "zips_array"
  ),
  questionable as (
  SELECT
      "cheerful"."_virt_agg_count_1049178494825001" as "_virt_agg_count_1049178494825001",
      "ss_customer_address_customer_address"."CA_ZIP" as "ss_customer_address_zip"
  FROM
      "cheerful"
      INNER JOIN "customer_address" as "ss_customer_address_customer_address" on "cheerful"."ss_customer_address_zip" = "ss_customer_address_customer_address"."CA_ZIP"),
  abundant as (
  SELECT
      CASE WHEN ( "questionable"."_virt_agg_count_1049178494825001" ) > 10 THEN "questionable"."ss_customer_address_zip" ELSE NULL END as "zip_with_many_preferred"
  FROM
      "questionable"),
  uneven as (
  SELECT
      "abundant"."zip_with_many_preferred" as "zip_with_many_preferred"
  FROM
      "abundant"
  GROUP BY
      1),
  yummy as (
  SELECT
      "ss_customer_address_customer_address"."CA_ZIP" as "qualifying_zip"
  FROM
      "customer_address" as "ss_customer_address_customer_address"
  WHERE
      "ss_customer_address_customer_address"."CA_ZIP" in (select quizzical."zips_array" from quizzical where quizzical."zips_array" is not null) and "ss_customer_address_customer_address"."CA_ZIP" in (select uneven."zip_with_many_preferred" from uneven where uneven."zip_with_many_preferred" is not null)
  ),
  juicy as (
  SELECT
      SUBSTRING("yummy"."qualifying_zip",1,2) as "_virt_func_substring_1010830310934997"
  FROM
      "yummy"
  GROUP BY
      1)
  SELECT
      "ss_store_store"."S_STORE_NAME" as "store_name",
      sum("ss_store_sales"."SS_NET_PROFIT") as "total_net_profit"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 1998 and "ss_date_date"."D_QOY" = 2 and SUBSTRING("ss_store_store"."S_ZIP",1,2) in (select juicy."_virt_func_substring_1010830310934997" from juicy where juicy."_virt_func_substring_1010830310934997" is not null)

  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]
  [parameters: ('24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,28577,55565,17183,54601,67897,22752 ... (2101 characters truncated) ... 26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576',)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query09.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 73 column 12 (char 2882). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Syntax error in query11.preql: HAVING references 'wr02.w_rev', 'wr01.w_rev', 'sr02.s_rev', 'sr01.s_rev', which are not in the SELECT projection (line 42). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --wr02.w_rev, --wr01.w_rev, --sr02.s_rev, --sr01.s_rev
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Unexpected error in query11.preql: (_duckdb.BinderException) Binder Error: Values list "juicy" does not have a column named "c_id"

  LINE 119:     coalesce("abhorrent"."sr02_cust_id","juicy"."c_id") as "sr02_cust_id"
                                                    ^
  [SQL:
  WITH
  late as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "_sr01_cust_id",
      sum("ss_store_sales"."SS_EXT_LIST_PRICE" - "ss_store_sales"."SS_EXT_DISCOUNT_AMT") as "_sr01_s_rev"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2001

  GROUP BY
      1),
  young as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "_sr02_cust_id",
      sum("ss_store_sales"."SS_EXT_LIST_PRICE" - "ss_store_sales"."SS_EXT_DISCOUNT_AMT") as "_sr02_s_rev"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 2002

  GROUP BY
      1),
  abundant as (
  SELECT
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" as "_wr01_cust_id",
      sum("ws_web_sales"."WS_EXT_LIST_PRICE" - "ws_web_sales"."WS_EXT_DISCOUNT_AMT") as "_wr01_w_rev"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2001

  GROUP BY
      1),
  cheerful as (
  SELECT
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" as "_wr02_cust_id",
      sum("ws_web_sales"."WS_EXT_LIST_PRICE" - "ws_web_sales"."WS_EXT_DISCOUNT_AMT") as "_wr02_w_rev"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
  WHERE
      "ws_date_date"."D_YEAR" = 2002

  GROUP BY
      1),
  quizzical as (
  SELECT
      "c_customers"."C_CUSTOMER_ID" as "c_text_id",
      "c_customers"."C_CUSTOMER_SK" as "c_id",
      "c_customers"."C_FIRST_NAME" as "c_first_name",
      "c_customers"."C_LAST_NAME" as "c_last_name",
      "c_customers"."C_PREFERRED_CUST_FLAG" as "c_preferred_cust_flag"
  FROM
      "customer" as "c_customers"),
  scrawny as (
  SELECT
      "late"."_sr01_cust_id" as "sr01_cust_id",
      "late"."_sr01_s_rev" as "sr01_s_rev"
  FROM
      "late"),
  abhorrent as (
  SELECT
      "young"."_sr02_cust_id" as "sr02_cust_id",
      "young"."_sr02_s_rev" as "sr02_s_rev"
  FROM
      "young"),
  yummy as (
  SELECT
      "abundant"."_wr01_cust_id" as "wr01_cust_id",
      "abundant"."_wr01_w_rev" as "wr01_w_rev"
  FROM
      "abundant"),
  cooperative as (
  SELECT
      "cheerful"."_wr02_cust_id" as "wr02_cust_id",
      "cheerful"."_wr02_w_rev" as "wr02_w_rev"
  FROM
      "cheerful"),
  questionable as (
  SELECT
      "cooperative"."wr02_w_rev" as "wr02_w_rev",
      "quizzical"."c_first_name" as "c_first_name",
      "quizzical"."c_last_name" as "c_last_name",
      "quizzical"."c_preferred_cust_flag" as "c_preferred_cust_flag",
      "quizzical"."c_text_id" as "c_text_id",
      coalesce("cooperative"."wr02_cust_id","quizzical"."c_id") as "wr02_cust_id"
  FROM
      "quizzical"
      LEFT OUTER JOIN "cooperative" on "quizzical"."c_id" = "cooperative"."wr02_cust_id"),
  juicy as (
  SELECT
      "questionable"."c_first_name" as "c_first_name",
      "questionable"."c_last_name" as "c_last_name",
      "questionable"."c_preferred_cust_flag" as "c_preferred_cust_flag",
      "questionable"."c_text_id" as "c_text_id",
      "questionable"."wr02_w_rev" as "wr02_w_rev",
      "yummy"."wr01_cust_id" as "wr01_cust_id",
      "yummy"."wr01_w_rev" as "wr01_w_rev"
  FROM
      "yummy"
      LEFT OUTER JOIN "questionable" on "yummy"."wr01_cust_id" = "questionable"."wr02_cust_id"
  WHERE
      "yummy"."wr01_w_rev" > 0
  ),
  sweltering as (
  SELECT
      "abhorrent"."sr02_s_rev" as "sr02_s_rev",
      "juicy"."c_first_name" as "c_first_name",
      "juicy"."c_last_name" as "c_last_name",
      "juicy"."c_preferred_cust_flag" as "c_preferred_cust_flag",
      "juicy"."c_text_id" as "c_text_id",
      "juicy"."wr01_w_rev" as "wr01_w_rev",
      "juicy"."wr02_w_rev" as "wr02_w_rev",
      coalesce("abhorrent"."sr02_cust_id","juicy"."c_id") as "sr02_cust_id"
  FROM
      "juicy"
      LEFT OUTER JOIN "abhorrent" on "juicy"."wr01_cust_id" = "abhorrent"."sr02_cust_id"
  WHERE
      "juicy"."wr01_w_rev" > 0
  )
  SELECT
      "sweltering"."c_text_id" as "customer_code",
      "sweltering"."c_first_name" as "c_first_name",
      "sweltering"."c_last_name" as "c_last_name",
      "sweltering"."c_preferred_cust_flag" as "c_preferred_cust_flag"
  FROM
      "sweltering"
      INNER JOIN "scrawny" on "sweltering"."sr02_cust_id" = "scrawny"."sr01_cust_id"
  WHERE
      "scrawny"."sr01_s_rev" > 0 and (coalesce("sweltering"."wr02_w_rev",0) - "sweltering"."wr01_w_rev") / "sweltering"."wr01_w_rev" > (coalesce("sweltering"."sr02_s_rev",0) - "scrawny"."sr01_s_rev") / "scrawny"."sr01_s_rev"

  ORDER BY
      "customer_code" asc nulls first,
      "sweltering"."c_first_name" asc nulls first,
      "sweltering"."c_last_name" asc nulls first,
      "sweltering"."c_preferred_cust_flag" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query12.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 6 undefined concept references; fix all before re-running:
    - store_sales.customer_demographics.marital_status (line 10, col 4, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 11, col 8, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
    - store_sales.customer_demographics.marital_status (line 16, col 4, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 17, col 8, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
    - store_sales.customer_demographics.marital_status (line 22, col 4, in WHERE); did you mean: store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status, store_sales.customer_demographic.marital_status?
    - store_sales.customer_demographics.education_status (line 23, col 8, in WHERE); did you mean: store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status, store_sales.customer_demographic.education_status?
  ```
- `trilogy run query14.preql duckdb`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 21). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql duckdb`

  ```text
  Syntax error in query14.preql: 8 undefined concept references; fix all before re-running:
    - nov2001.channel (line 36, col 5, in SELECT); did you mean: nov2001.sales.channel, sales.channel, nov2001.category_id, nov2001.class_id?
    - nov2001.brand_id (line 37, col 5, in SELECT); did you mean: nov2001.sales.item.brand_id, sales.item.brand_id, nov2001.class_id, nov2001.category_id?
    - nov2001.class_id (line 38, col 5, in SELECT); did you mean: nov2001.sales.item.class_id, sales.item.class_id, nov2001.brand_id, nov2001.category_id?
    - nov2001.category_id (line 39, col 5, in SELECT); did you mean: nov2001.sales.item.category_id, sales.item.category_id, nov2001.class_id, nov2001.brand_id?
    - nov2001.channel (line 42, col 10, in ORDER BY); did you mean: nov2001.sales.channel, sales.channel, nov2001.category_id, nov2001.class_id?
    - nov2001.brand_id (line 43, col 3, in ORDER BY); did you mean: nov2001.sales.item.brand_id, sales.item.brand_id, nov2001.class_id, nov2001.category_id?
    - nov2001.class_id (line 44, col 3, in ORDER BY); did you mean: nov2001.sales.item.class_id, sales.item.class_id, nov2001.brand_id, nov2001.category_id?
    - nov2001.category_id (line 45, col 3, in ORDER BY); did you mean: nov2001.sales.item.category_id, sales.item.category_id, nov2001.class_id, nov2001.brand_id?
  ```
- `trilogy run query14.preql duckdb`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 21). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query16.preql`

  ```text
  Syntax error in query16.preql: Undefined concept: multi_warehouse_orders.order_number (line 18, col 28, in WHERE). Suggestions: ['multi_warehouse_orders.cs.order_number', 'cs.order_number', 'cr.sales.order_number', 'cr.order_number', 'multi_warehouse_orders.warehouse_count', '_multi_warehouse_orders_warehouse_count']
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query19.preql`

  ```text
  Syntax error in query19.preql: 11 undefined concept references; fix all before re-running:
    - item.brand_id (line 9, col 3, in SELECT); did you mean: store_sales.item.brand_id, item.brand_name, item.manager_id?
    - item.brand_name (line 10, col 3, in SELECT); did you mean: store_sales.item.brand_name, item.brand_id, item.manager_id?
    - item.manufacturer_id (line 11, col 3, in SELECT); did you mean: store_sales.item.manufacturer_id, item.manager_id, item.manufact?
    - item.manufact (line 12, col 3, in SELECT); did you mean: store_sales.item.manufact, item.manufacturer_id, item.manager_id?
    - item.manager_id (line 4, col 3, in WHERE); did you mean: store_sales.item.manager_id, item.manufacturer_id, item.brand_id?
    - date.year (line 5, col 7, in WHERE); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, store_sales.return_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.first_shipto_date.year?
    - date.month_of_year (line 6, col 7, in WHERE); did you mean: store_sales.date.month_of_year, store_sales.store.date.month_of_year, store_sales.return_store.date.month_of_year, store_sales.return_date.month_of_year, store_sales.customer.first_sales_date.month_of_year, store_sales.customer.first_shipto_date.month_of_year?
    - customer.address.zip (line 3, in WHERE); did you mean: store_sales.customer.address.zip, store_sales.return_customer.address.zip, store_sales.store.zip, store_sales.return_store.zip, store_sales.sale_address.zip, store.zip?
    - store.zip (line 3, in WHERE); did you mean: store_sales.store.zip, store_sales.customer.address.zip, store_sales.return_customer.address.zip, store_sales.return_store.zip, store_sales.sale_address.zip, customer.address.zip?
    - item.brand_id (line 16, col 3, in ORDER BY); did you mean: store_sales.item.brand_id, item.brand_name, item.manager_id?
    - item.manufacturer_id (line 17, col 3, in ORDER BY); did you mean: store_sales.item.manufacturer_id, item.manager_id, item.manufact?
  ```
- `trilogy file read query21.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Undefined concept: item.desc. Suggestions: ['store_sales.item.desc', 'catalog_sales.item.desc', 'web_sales.item.desc', 'store_sales.return_reason.desc']
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'store_sales.item.desc', which is not in the SELECT projection (line 24). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --store_sales.item.desc
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run test_cust.preql`

  ```text
  Unexpected error in test_cust.preql: (_duckdb.BinderException) Binder Error: Values list "uneven" does not have a column named "cust_store_rev"

  LINE 47:     "uneven"."cust_store_rev" as "cust_store_rev"
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
      "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
      "store_sales_store_sales"."SS_EXT_SALES_PRICE" as "store_sales_ext_sales_price",
      "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity"
  FROM
      "store_sales" as "store_sales_store_sales"
      LEFT OUTER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"),
  thoughtful as (
  SELECT
      "cheerful"."store_sales_customer_id" as "store_sales_customer_id",
      sum("cheerful"."store_sales_quantity" * CASE WHEN "cheerful"."store_sales_date_year" BETWEEN 2000 AND 2003 and "cheerful"."store_sales_customer_id" is not null THEN "cheerful"."store_sales_ext_sales_price" ELSE NULL END) as "cust_store_rev"
  FROM
      "cheerful"
  GROUP BY
      1),
  abundant as (
  SELECT
      "thoughtful"."cust_store_rev" as "cust_store_rev",
      coalesce("cheerful"."store_sales_customer_id","thoughtful"."store_sales_customer_id") as "store_sales_customer_id"
  FROM
      "cheerful"
      FULL JOIN "thoughtful" on "cheerful"."store_sales_customer_id" is not distinct from "thoughtful"."store_sales_customer_id"),
  questionable as (
  SELECT
      max("thoughtful"."cust_store_rev") as "_virt_agg_max_1802502615656440"
  FROM
      "thoughtful"),
  uneven as (
  SELECT
      "abundant"."store_sales_customer_id" as "store_sales_customer_id"
  FROM
      "questionable"
      FULL JOIN "abundant" on 1=1
  WHERE
      ("abundant"."cust_store_rev" > 0.5 * "questionable"."_virt_agg_max_1802502615656440") = True

  GROUP BY
      1,
      "abundant"."cust_store_rev")
  SELECT
      "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
      "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
      "uneven"."cust_store_rev" as "cust_store_rev"
  FROM
      "uneven"
      INNER JOIN "customer" as "store_sales_customer_customers" on "uneven"."store_sales_customer_id" = "store_sales_customer_customers"."C_CUSTOMER_SK"
  LIMIT (20)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query24.preql`

  ```text
  Syntax error in query24.preql: 6 undefined concept references; fix all before re-running:
    - stage1.last_name (line 31, col 3, in SELECT); did you mean: stage1.ss.customer.last_name, ss.customer.last_name, ss.return_customer.last_name, pr.billing_customer.last_name, stage1.first_name, stage1.name?
    - stage1.first_name (line 32, col 3, in SELECT); did you mean: stage1.ss.customer.first_name, ss.customer.first_name, ss.return_customer.first_name, pr.billing_customer.first_name, stage1.last_name, stage1.name?
    - stage1.name (line 33, col 3, in SELECT); did you mean: stage1.ss.store.name, ss.store.name, ss.return_store.name, pr.store.name, stage1.last_name, stage1.first_name?
    - stage1.last_name (line 36, col 10, in ORDER BY); did you mean: stage1.ss.customer.last_name, ss.customer.last_name, ss.return_customer.last_name, pr.billing_customer.last_name, stage1.first_name, stage1.name?
    - stage1.first_name (line 36, col 28, in ORDER BY); did you mean: stage1.ss.customer.first_name, ss.customer.first_name, ss.return_customer.first_name, pr.billing_customer.first_name, stage1.last_name, stage1.name?
    - stage1.name (line 36, col 47, in ORDER BY); did you mean: stage1.ss.store.name, ss.store.name, ss.return_store.name, pr.store.name, stage1.last_name, stage1.first_name?
  ```
- `trilogy run query24.preql`

  ```text
  Syntax error in query24.preql: HAVING references 'local.avg_subtotal', which is not in the SELECT projection (line 30). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.avg_subtotal
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: HAVING references 'local.state_avg', which is not in the SELECT projection (line 9). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.state_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query31.preql`

  ```text
  Syntax error in query31.preql: 2 undefined concept references; fix all before re-running:
    - st.county (line 24, col 5, in SELECT); did you mean: ss.customer.address.county, ss.return_customer.address.county, ss.store.county, ss.return_store.county, ss.sale_address.county, ws.billing_customer.address.county?
    - local.county (line 38, col 10, in ORDER BY); did you mean: ss.customer.address.county, ss.return_customer.address.county, ss.store.county, ss.return_store.county, ss.sale_address.county, ws.billing_customer.address.county?
  ```
- `trilogy run query31.preql`

  ```text
  Syntax error in query31.preql: 2 undefined concept references; fix all before re-running:
    - st.county (line 24, col 5, in SELECT); did you mean: ss.customer.address.county, ss.return_customer.address.county, ss.store.county, ss.return_store.county, ss.sale_address.county, ws.billing_customer.address.county?
    - local.county (line 40, col 10, in ORDER BY); did you mean: ss.customer.address.county, ss.return_customer.address.county, ss.store.county, ss.return_store.county, ss.sale_address.county, ws.billing_customer.address.county?
  ```
- `trilogy run query34.preql`

  ```text
  Syntax error in query34.preql: 10 undefined concept references; fix all before re-running:
    - ticket_lines.last_name (line 35, col 3, in SELECT); did you mean: ticket_lines.store_sales.customer.last_name, store_sales.customer.last_name, store_sales.return_customer.last_name, ticket_lines.first_name, ticket_lines.ticket_number?
    - ticket_lines.first_name (line 36, col 3, in SELECT); did you mean: ticket_lines.store_sales.customer.first_name, store_sales.customer.first_name, store_sales.return_customer.first_name, ticket_lines.last_name, ticket_lines.ticket_number?
    - ticket_lines.salutation (line 37, col 3, in SELECT); did you mean: ticket_lines.store_sales.customer.salutation, store_sales.customer.salutation, store_sales.return_customer.salutation, ticket_lines.last_name, ticket_lines.ticket_number?
    - ticket_lines.preferred_cust_flag (line 38, col 3, in SELECT); did you mean: ticket_lines.store_sales.customer.preferred_cust_flag, store_sales.customer.preferred_cust_flag, store_sales.return_customer.preferred_cust_flag, ticket_lines.first_name?
    - ticket_lines.ticket_number (line 39, col 3, in SELECT); did you mean: ticket_lines.store_sales.ticket_number, store_sales.ticket_number, ticket_lines.first_name?
    - ticket_lines.last_name (line 42, col 3, in ORDER BY); did you mean: ticket_lines.store_sales.customer.last_name, store_sales.customer.last_name, store_sales.return_customer.last_name, ticket_lines.first_name, ticket_lines.ticket_number?
    - ticket_lines.first_name (line 43, col 3, in ORDER BY); did you mean: ticket_lines.store_sales.customer.first_name, store_sales.customer.first_name, store_sales.return_customer.first_name, ticket_lines.last_name, ticket_lines.ticket_number?
    - ticket_lines.salutation (line 44, col 3, in ORDER BY); did you mean: ticket_lines.store_sales.customer.salutation, store_sales.customer.salutation, store_sales.return_customer.salutation, ticket_lines.last_name, ticket_lines.ticket_number?
    - ticket_lines.preferred_cust_flag (line 45, col 3, in ORDER BY); did you mean: ticket_lines.store_sales.customer.preferred_cust_flag, store_sales.customer.preferred_cust_flag, store_sales.return_customer.preferred_cust_flag, ticket_lines.first_name?
    - ticket_lines.ticket_number (line 46, col 3, in ORDER BY); did you mean: ticket_lines.store_sales.ticket_number, store_sales.ticket_number, ticket_lines.first_name?
  ```
- `trilogy file read test_rollup.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.manufacturer_id, item.text_id, local.current_price, local.description, local.item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query39.preql`

  ```text
  Syntax error in query39.preql: Undefined concept: date.year. Suggestions: ['inv.date.year']
  ```
- `trilogy run query39.preql`

  ```text
  Syntax error in query39.preql: Undefined concept: qualifying.mon. Suggestions: ['qualifying.monthly_stats.mon', 'monthly_stats.mon', 'qualifying.cv', '_qualifying_cv']
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {feb.avg_qoh, feb.cv, feb.mon, jan.avg_qoh, jan.cv, jan.i_id, jan.mon, jan.w_id}
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query41.preql`

  ```text
  Syntax error in query41.preql: Undefined concept: profile_items.manufact (line 21, col 26, in WHERE). Suggestions: ['profile_items.item.manufact', 'item.manufact', 'item.manufacturer_id']
  ```
- `trilogy file read query43.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: ranked_items.product_name. Suggestions: ['ranked_items.store_sales.item.product_name', 'store_sales.item.product_name', 'ranked_items.avg_profit']
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.threshold', which is not in the SELECT projection (line 7). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.threshold
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: 2 undefined concept references; fix all before re-running:
    - best_name (line 35, col 20, in ORDER BY); did you mean: best.best_name, _best_best_name?
    - worst_name (line 35, col 36, in ORDER BY); did you mean: worst.worst_name, _worst_worst_name, worst.rank_no?
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'store_sales.net_profit', 'store_sales.store.id', 'store_sales.sale_address.id', '__preql_internal.all_rows', which are not in the SELECT projection (line 6). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --store_sales.net_profit, --store_sales.store.id, --store_sales.sale_address.id, --__preql_internal.all_rows
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query46.preql`

  ```text
  Syntax error in query46.preql: 11 undefined concept references; fix all before re-running:
    - local.ticket_number (line 14, col 3, in SELECT); did you mean: store_sales.ticket_number?
    - local.coupon_amt (line 3, in SELECT); did you mean: store_sales.coupon_amt, total_coupon_amt?
    - local.net_profit (line 3, in SELECT); did you mean: store_sales.net_profit, total_net_profit?
    - date.day_of_week (line 4, col 3, in WHERE); did you mean: store_sales.date.day_of_week, store_sales.store.date.day_of_week, store_sales.return_store.date.day_of_week, store_sales.return_date.day_of_week, store_sales.customer.first_sales_date.day_of_week, store_sales.customer.first_shipto_date.day_of_week?
    - date.year (line 5, col 7, in WHERE); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, store_sales.return_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.first_shipto_date.year?
    - store.city (line 6, col 7, in WHERE); did you mean: store_sales.store.city, store_sales.customer.address.city, store_sales.return_customer.address.city, store_sales.return_store.city, store_sales.sale_address.city, customer.address.city?
    - household_demographic.dependent_count (line 7, col 8, in WHERE); did you mean: store_sales.customer.household_demographic.dependent_count, store_sales.return_customer.household_demographic.dependent_count, store_sales.household_demographic.dependent_count, store_sales.customer.demographics.dependent_count, store_sales.return_customer.demographics.dependent_count, store_sales.customer_demographic.dependent_count?
    - household_demographic.vehicle_count (line 7, col 53, in WHERE); did you mean: store_sales.customer.household_demographic.vehicle_count, store_sales.return_customer.household_demographic.vehicle_count, store_sales.household_demographic.vehicle_count, household_demographic.dependent_count?
    - customer.address.city (line 8, col 7, in WHERE); did you mean: store_sales.customer.address.city, store_sales.return_customer.address.city, store_sales.store.city, store_sales.return_store.city, store_sales.sale_address.city, store.city?
    - sale_address.city (line 8, col 32, in WHERE); did you mean: store_sales.sale_address.city, store_sales.customer.address.city, store_sales.return_customer.address.city, store_sales.store.city, store_sales.return_store.city, store.city?
    - ticket_number (line 22, col 3, in ORDER BY); did you mean: store_sales.ticket_number, ticket_number?
  ```
- `trilogy run query47.preql`

  ```text
  Syntax error in query47.preql: 12 undefined concept references; fix all before re-running:
    - monthly_totals.category (line 20, col 5, in SELECT); did you mean: monthly_totals.ss.item.category, ss.item.category, monthly_totals.name, monthly_totals.year?
    - monthly_totals.brand_name (line 21, col 5, in SELECT); did you mean: monthly_totals.ss.item.brand_name, ss.item.brand_name, monthly_totals.name, monthly_totals.company_name?
    - monthly_totals.name (line 22, col 5, in SELECT); did you mean: monthly_totals.ss.store.name, ss.store.name, ss.return_store.name, monthly_totals.brand_name, monthly_totals.year, monthly_totals.company_name?
    - monthly_totals.company_name (line 23, col 5, in SELECT); did you mean: monthly_totals.ss.store.company_name, ss.store.company_name, ss.return_store.company_name, monthly_totals.brand_name, monthly_totals.name?
    - monthly_totals.year (line 24, col 5, in SELECT); did you mean: monthly_totals.ss.date.year, ss.date.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.return_customer.first_sales_date.year?
    - monthly_totals.month_of_year (line 25, col 5, in SELECT); did you mean: monthly_totals.ss.date.month_of_year, ss.date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year?
    - monthly_totals.category (line 34, col 10, in ORDER BY); did you mean: monthly_totals.ss.item.category, ss.item.category, monthly_totals.name, monthly_totals.year?
    - monthly_totals.brand_name (line 35, col 10, in ORDER BY); did you mean: monthly_totals.ss.item.brand_name, ss.item.brand_name, monthly_totals.name, monthly_totals.company_name?
    - monthly_totals.name (line 36, col 10, in ORDER BY); did you mean: monthly_totals.ss.store.name, ss.store.name, ss.return_store.name, monthly_totals.brand_name, monthly_totals.year, monthly_totals.company_name?
    - monthly_totals.company_name (line 37, col 10, in ORDER BY); did you mean: monthly_totals.ss.store.company_name, ss.store.company_name, ss.return_store.company_name, monthly_totals.brand_name, monthly_totals.name?
    - monthly_totals.year (line 38, col 10, in ORDER BY); did you mean: monthly_totals.ss.date.year, ss.date.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.return_customer.first_sales_date.year?
    - monthly_totals.month_of_year (line 39, col 10, in ORDER BY); did you mean: monthly_totals.ss.date.month_of_year, ss.date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year?
  ```
- `trilogy file read query48.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query53.preql`

  ```text
  Syntax error in query53.preql: Undefined concept: store_sales.sales_price. Suggestions: ['sales_price', 'ext_sales_price']
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {dec98.ms}; {local._cust_totals_cust_id, local._cust_totals_total_spent, ss.customer.address.county, ss.customer.address.state, ss.customer.id, ss.date.month_seq, ss.store.county, ss.store.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query57.preql`

  ```text
  Resolution error in query57.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {cc.name}; {cs.sold_date.month_of_year, cs.sold_date.year, local.avg_monthly_sales, local.monthly_total}; {item.brand_name, item.category}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query57.preql`

  ```text
  Syntax error in query57.preql: Output column 'prior_month_total' aliases 'local.prior_month_total', which is itself the 'prior_month_total' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'prior_month_total_out').
  ```
- `trilogy file read test_auto.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_rollup.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read test_store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query61.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {local.cr_refund_total}; {local.cs_ext_list_price_total}; {local.sale_lines, local.total_coupon_amt, local.total_list_price, local.total_wholesale_cost, ss.customer.address.city, ss.customer.address.street_name, ss.customer.address.street_number, ss.customer.address.zip, ss.customer.demographics.marital_status, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.customer_demographic.marital_status, ss.date.year, ss.is_returned, ss.item.color, ss.item.current_price, ss.item.product_name, ss.sale_address.city, ss.sale_address.street_name, ss.sale_address.street_number, ss.sale_address.zip, ss.store.name, ss.store.zip}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: agg_sales.product_name (line 50, col 3, in SELECT). Suggestions: ['agg_sales.ss.item.product_name', 'ss.item.product_name', 'cs.item.product_name', 'cr.sales.item.product_name', 'cr.item.product_name', 'agg_sales.store_name']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: agg_1999.product_name (line 76, col 3, in SELECT). Suggestions: ['agg_1999.ss.item.product_name', 'ss.item.product_name', 'cs.item.product_name', 'cr.sales.item.product_name', 'cr.item.product_name', 'agg_2000.ss.item.product_name']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: agg_1999.ss.item.id (line 28, col 8, in SELECT). Suggestions: ['ss.item.id', 'ss.date.id', 'ss.return_date.id', 'ss.time.id', 'ss.return_time.id', 'ss.customer.demographics.id']
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: HAVING references 'ss.store.id', which is not in the SELECT projection (line 4). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ss.store.id
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: HAVING clause aggregate `avg(local.per_item_revenue) by local.store_id` is not in the SELECT projection (line 4). HAVING can only filter on off-grain or nested aggregates that are also computed in the SELECT. Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --avg(local.per_item_revenue) by local.store_id`; (b) move the filter to WHERE — for an aggregate condition on a non-output grain, write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy file read query65.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: 2 undefined concept references; fix all before re-running:
    - store.state (line 21, col 5, in SELECT); did you mean: ss.store.state, ss.customer.address.state, ss.return_customer.address.state, ss.return_store.state, ss.sale_address.state, ss.store.name?
    - date.year (line 19, col 7, in WHERE); did you mean: ss.date.year, ss.store.date.year, ss.return_store.date.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year?
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: Multiple where clauses are not supported
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: Undefined concept: top_states.state (line 31, col 25, in WHERE). Suggestions: ['top_states.ss.store.state', 'ss.customer.address.state', 'ss.return_customer.address.state', 'ss.store.state', 'ss.return_store.state', 'ss.sale_address.state']
  ```
- `trilogy file read query71.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query72.preql`

  ```text
  Unexpected error in query72.preql: Multiple where clauses are not supported
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: yearly.brand_id. Suggestions: ['yearly.deduped.brand_id', 'sales.item.brand_id', 'deduped.brand_id']
  ```
- `trilogy file read query75.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: 8 undefined concept references; fix all before re-running:
    - agg.brand_id (line 33, col 5, in SELECT); did you mean: agg.deduped.brand_id, sales.item.brand_id, deduped.brand_id, agg.class_id?
    - agg.class_id (line 34, col 5, in SELECT); did you mean: agg.deduped.class_id, sales.item.class_id, deduped.class_id, agg.category_id, agg.brand_id?
    - agg.category_id (line 35, col 5, in SELECT); did you mean: agg.deduped.category_id, sales.item.category_id, deduped.category_id?
    - agg.manufacturer_id (line 36, col 5, in SELECT); did you mean: agg.deduped.manufacturer_id, sales.item.manufacturer_id, deduped.manufacturer_id?
    - agg.yr (line 37, col 120, in SELECT); did you mean: agg.deduped.yr, deduped.yr?
    - agg.yr (line 39, col 136, in SELECT); did you mean: agg.deduped.yr, deduped.yr?
    - agg.yr (line 40, col 136, in SELECT); did you mean: agg.deduped.yr, deduped.yr?
    - agg.yr (line 29, col 7, in WHERE); did you mean: agg.deduped.yr, deduped.yr?
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: 6 undefined concept references; fix all before re-running:
    - agg.deduped.total_qty (line 28, in SELECT); did you mean: agg.total_qty, agg.deduped.total_amt, agg.deduped.yr?
    - agg.deduped.yr (line 35, col 160, in SELECT); did you mean: deduped.yr, agg.yr, agg.deduped.brand_id, agg.deduped.total_qty?
    - agg.deduped.yr (line 37, col 184, in SELECT); did you mean: deduped.yr, agg.yr, agg.deduped.brand_id, agg.deduped.total_qty?
    - agg.deduped.total_amt (line 28, in SELECT); did you mean: agg.total_amt, agg.deduped.total_qty, deduped.net_amt?
    - agg.deduped.yr (line 38, col 184, in SELECT); did you mean: deduped.yr, agg.yr, agg.deduped.brand_id, agg.deduped.total_qty?
    - agg.deduped.yr (line 39, col 7, in WHERE); did you mean: deduped.yr, agg.yr, agg.deduped.brand_id, agg.deduped.total_qty?
  ```
- `trilogy run _test2.preql`

  ```text
  Syntax error in _test2.preql: ORDER BY references 'agg.cid', which is not in the SELECT projection (line 26). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --agg.cid order by agg.cid asc`.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: HAVING references 'agg.yr', which is not in the SELECT projection (line 29). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --agg.yr
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query77.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 78 column 12 (char 2628). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query79.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:sales --import raw.store:store where sales.date.date between '2000-08-23'::date and '2000-09-22'::date and sales.item.curr…= 'N' and sales.channel = 'STORE' and sales.channel_dim_id is not null select sales.channel_dim_id, store.text_id, sales.channel_dim_text_id limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {sales.channel, sales.channel_dim_id, sales.channel_dim_text_id, sales.date.date, sales.item.current_price, sales.promotion.channel_tv}; {store.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw.store:store select store.text_id order by store.id limit 10;`

  ```text
  Syntax error in stdin: ORDER BY references 'store.id', which is not in the SELECT projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store.id order by store.id asc`.
  ```
- `trilogy run query81.preql`

  ```text
  Syntax error in query81.preql: ORDER BY contains aggregate `sum(cr.return_amt_inc_tax)` (line 10), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; order by the SELECT alias `total_return_amt_inc_tax` instead (`order by total_return_amt_inc_tax desc`).
  ```
- `trilogy run query82.preql`

  ```text
  Resolution error in query82.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {inv.date.date, inv.quantity_on_hand}; {item.current_price, item.id, item.manufacturer_id, local.description, local.item_code}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Undefined concept: web_ret.item.text_id. Suggestions: ['web_ret.web_sales.item.text_id', 'store_ret.item.text_id', 'store_ret.date.text_id', 'store_ret.return_date.text_id', 'store_ret.time.text_id', 'store_ret.return_time.text_id']
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error in query86.preql: Recursion error building concept local.within_parent_rank with grain Grain<ws.item.id,ws.order_number> and lineage case(WHEN add(grouping(ref:ws.item.category)<['ref:ws.item.category', 'ref:ws.item.class']>,grouping(ref:ws.item.class)<['ref:ws.item.category', 'ref:ws.item.class']>) = 2 THEN 1,WHEN add(grouping(ref:ws.item.category)<['ref:ws.item.category', 'ref:ws.item.class']>,grouping(ref:ws.item.class)<['ref:ws.item.category', 'ref:ws.item.class']>) = 1 THEN rank([ref:ws.item.category]) over [add(grouping(ref:ws.item.category)<abstract>,grouping(ref:ws.item.class)<abstract>)] order [OrderItem(expr=AggregateWrapper(function=sum(ref:ws.net_paid), by=[ref:ws.item.category], grouping=<AggregateGroupingMode.STANDARD: 'standard'>, grouping_sets=[]), order=<Ordering.DESCENDING: 'desc'>)],ELSE rank([ref:ws.item.class]) over [ref:ws.item.category] order [OrderItem(expr=AggregateWrapper(function=sum(ref:ws.net_paid), by=[], grouping=<AggregateGroupingMode.STANDARD: 'standard'>, grouping_sets=[]), order=<Ordering.DESCENDING: 'desc'>)]). This is likely due to a circular reference.
  ```
- `trilogy file read query86.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query89.preql duckdb`

  ```text
  Syntax error in query89.preql: 12 undefined concept references; fix all before re-running:
    - monthly_totals.category (line 25, col 5, in SELECT); did you mean: monthly_totals.ss.item.category, ss.item.category, monthly_totals.name, monthly_totals.class?
    - monthly_totals.class (line 26, col 5, in SELECT); did you mean: monthly_totals.ss.item.class, ss.item.class, monthly_totals.name, monthly_totals.category?
    - monthly_totals.brand_name (line 27, col 5, in SELECT); did you mean: monthly_totals.ss.item.brand_name, ss.item.brand_name, monthly_totals.name, monthly_totals.company_name?
    - monthly_totals.name (line 28, col 5, in SELECT); did you mean: monthly_totals.ss.store.name, ss.store.name, ss.return_store.name, monthly_totals.brand_name, monthly_totals.company_name, monthly_totals.class?
    - monthly_totals.company_name (line 29, col 5, in SELECT); did you mean: monthly_totals.ss.store.company_name, ss.store.company_name, ss.return_store.company_name, monthly_totals.brand_name, monthly_totals.name?
    - monthly_totals.month_of_year (line 30, col 5, in SELECT); did you mean: monthly_totals.ss.date.month_of_year, ss.date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year?
    - monthly_totals.name (line 35, col 5, in ORDER BY); did you mean: monthly_totals.ss.store.name, ss.store.name, ss.return_store.name, monthly_totals.brand_name, monthly_totals.company_name, monthly_totals.class?
    - monthly_totals.category (line 36, col 5, in ORDER BY); did you mean: monthly_totals.ss.item.category, ss.item.category, monthly_totals.name, monthly_totals.class?
    - monthly_totals.class (line 37, col 5, in ORDER BY); did you mean: monthly_totals.ss.item.class, ss.item.class, monthly_totals.name, monthly_totals.category?
    - monthly_totals.brand_name (line 38, col 5, in ORDER BY); did you mean: monthly_totals.ss.item.brand_name, ss.item.brand_name, monthly_totals.name, monthly_totals.company_name?
    - monthly_totals.company_name (line 39, col 5, in ORDER BY); did you mean: monthly_totals.ss.store.company_name, ss.store.company_name, ss.return_store.company_name, monthly_totals.brand_name, monthly_totals.name?
    - monthly_totals.month_of_year (line 40, col 5, in ORDER BY); did you mean: monthly_totals.ss.date.month_of_year, ss.date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year?
  ```
- `trilogy run query90.preql duckdb`

  ```text
  Syntax error in query90.preql: Output column 'ratio' aliases 'local.ratio', which is itself the 'ratio' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'ratio_out').
  ```
- `trilogy file read query92.preql`

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
- `trilogy run query94.preql`

  ```text
  Syntax error in query94.preql: 6 undefined concept references; fix all before re-running:
    - candidate_lines.order_number (line 17, col 3, in SELECT); did you mean: candidate_lines.ws.order_number, ws.order_number, candidate_lines.warehouse_id, candidate_lines.is_returned?
    - candidate_lines.order_number (line 16, in SELECT); did you mean: candidate_lines.ws.order_number, ws.order_number, candidate_lines.warehouse_id, candidate_lines.is_returned?
    - candidate_lines.order_number (line 20, col 51, in WHERE); did you mean: candidate_lines.ws.order_number, ws.order_number, candidate_lines.warehouse_id, candidate_lines.is_returned?
    - candidate_lines.is_returned (line 16, in WHERE); did you mean: candidate_lines.ws.is_returned, ws.is_returned, candidate_lines.warehouse_id, _candidate_lines_warehouse_id?
    - candidate_lines.is_returned (line 21, col 43, in WHERE); did you mean: candidate_lines.ws.is_returned, ws.is_returned, candidate_lines.warehouse_id, _candidate_lines_warehouse_id?
    - candidate_lines.order_number (line 21, col 82, in WHERE); did you mean: candidate_lines.ws.order_number, ws.order_number, candidate_lines.warehouse_id, candidate_lines.is_returned?
  ```
- `trilogy run query95.preql`

  ```text
  Syntax error in query95.preql: HAVING references 'ws.warehouse.id', 'ws.is_returned', which are not in the SELECT projection (line 3). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ws.warehouse.id, --ws.is_returned
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query95.preql`

  ```text
  Syntax error in query95.preql: HAVING references 'ws.warehouse.id', 'ws.is_returned', which are not in the SELECT projection (line 4). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ws.warehouse.id, --ws.is_returned
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query95.preql`

  ```text
  Syntax error in query95.preql: 3 undefined concept references; fix all before re-running:
    - qualifying_orders.order_number (line 19, in SELECT); did you mean: qualifying_orders.ws.order_number, ws.order_number, qualifying_orders.net_profit, qualifying_orders.ws.warehouse.id?
    - qualifying_orders.ext_ship_cost (line 19, in SELECT); did you mean: qualifying_orders.ws.ext_ship_cost, ws.ext_ship_cost, qualifying_orders.net_profit, qualifying_orders.ws.net_profit?
    - qualifying_orders.net_profit (line 19, in SELECT); did you mean: qualifying_orders.ws.net_profit, ws.net_profit, qualifying_orders.ext_ship_cost, qualifying_orders.ws.ext_ship_cost?
  ```
- `trilogy file read query95.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query97.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query99.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query01.preql --content import raw/physical_returns as ret;

# Total returns per billing customer per store
auto cust_store_returns <- sum…  --cust_store_returns,
    --store_avg_returns,
having cust_store_returns > 1.2 * store_avg_returns
order by ret.billing_customer.text_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/physical_returns as ret;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/physical_returns as ret;

  Write stats: received 506 chars / 506 bytes; tail: …'avg_returns\\norder by ret.billing_customer.text_id\\nlimit 100;'.
  ```
- `trilogy file write query01.preql --content import raw.physical_returns as ret;

# Filter: returns to physical stores in Tennessee in 2000
where ret.store.sta…  --cust_store_returns,
    --store_avg_returns,
having cust_store_returns > 1.2 * store_avg_returns
order by ret.billing_customer.text_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...er billing customer per store ??? auto cust_store_returns <- sum...

  Write stats: received 628 chars / 628 bytes; tail: …'avg_returns\\norder by ret.billing_customer.text_id\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.date.year, min(all_sales.date.week_seq) as min_ws, max(all_sales.date.week_seq) as max_ws group by all_sales.date.year order by all_sales.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ales.date.week_seq) as max_ws ??? group by all_sales.date.year o...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.date.week_seq, all_sales.date.year, count(all_sales.order_id) as cnt where all_sales.date.week_seq between 5269 and 5272 group by all_sales.date.week_seq, all_sales.date.year order by all_sales.date.week_seq, all_sales.date.year`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eek_seq between 5269 and 5272 ??? group by all_sales.date.week_s...
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# WEB and CATALOG sales only: sum ext_sales_price by week_seq and day_of_week
…01.week_seq + 53 = sales_2002.week_seq
  and sales_2001.day_of_week = sales_2002.day_of_week
order by sales_2001.week_seq asc nulls first
limit 500;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:12
     |
  30 | inner join sales_2001.week_seq + 53 = sales_2002.week_seq
     |            ^---
     |
     = expected join_group
  Location:
  ...w_sum, 2) as ratio inner join ??? sales_2001.week_seq + 53 = sal...

  Write stats: received 1145 chars / 1145 bytes; tail: …'eek\\norder by sales_2001.week_seq asc nulls first\\nlimit 500;\\n'.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# WEB and CATALOG sales: sum ext_sales_price by week_seq and day_of_week
# acr…y_of_week = 6) / nullif(sum(ratios.val_2002 ? ratios.day_of_week = 6), 0), 2) as saturday_ratio,
order by ratios.week_seq asc nulls first
limit 500;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 57:14
     |
  57 |              case(all_weekly_dow.day_of_week = 0, 52, true, 53)
     |              ^---
     |
     = expected INT_LITERAL_PART
  Location:
  ...ly_dow.dow_sum,               ??? case(all_weekly_dow.day_of_wee...

  Write stats: received 3220 chars / 3220 bytes; tail: …'_ratio,\\norder by ratios.week_seq asc nulls first\\nlimit 500;\\n'.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.date.week_seq, all_sales.date.year from raw.all_sales where all_sales.channel in ('WEB','CATALOG') and all_sales.date.day_of_week=2 and all_sales.date.week_seq between 5269 and 5325 order by all_sales.date.week_seq`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...week_seq, all_sales.date.year ??? from raw.all_sales where all_s...
  ```
- `trilogy file write query04.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Store channel: annual…ue.first_name,
    store_value.last_name,
    store_cust_2002.pref_flag
inner join store_value = catalog_value
    = web_value
    = store_cust_2002
`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...b_value     = store_cust_2002 ???

  Write stats: received 1432 chars / 1432 bytes; tail: …'value = catalog_value\\n    = web_value\\n    = store_cust_2002\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

with combined as union(
  (where s.date.date between '2000-08-23'::date and '2000-09-0…otal
order by
  grouping(combined.channel_type) + grouping(combined.entity_id) desc,
  combined.channel_type asc,
  combined.entity_id asc
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 36:52
     |
  36 | def rollup_agg(metric) -> coalesce(sum(metric), 0) by rollup combined.channel_type, combined.entity_id;
     |                                                    ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...) -> coalesce(sum(metric), 0) ??? by rollup combined.channel_typ...

  Write stats: received 1911 chars / 1911 bytes; tail: …'mbined.channel_type asc,\\n  combined.entity_id asc\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:s where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_id is not null select s.channel, count(s.row_one) as sales_rows, count(s.is_returned) as ret_rows, count(*) as total_rows limit 5;`

  ```text
  Syntax error in stdin:  --> 2:206
    |
  2 | where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_id is not null select s.channel, count(s.row_one) as sales_rows, count(s.is_returned) as ret_rows, count(*) as total_rows limit 5;
    |                                                                                                                                                                                                              ^---
    |
    = expected access_chain
  Location:
  ..._returned) as ret_rows, count( ??? *) as total_rows limit 5;
  ```
- `trilogy run --import raw.all_sales:s where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_id is not null selec…unt) as ret_amt, sum(s.return_net_loss) as ret_loss, sum(s.ext_sales_price) as gross_sales group by s.channel, s.return_channel_dim_text_id limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...t_sales_price) as gross_sales ??? group by s.channel, s.return_c...
  ```
- `trilogy run --import raw.all_sales:s with sales_part as (where s.date.date between '2000-08-23'::date and '2000-09-06'::date and s.channel_dim_id is not null…t sales_part.ch, sales_part.ent, sum(sales_part.gs) as gs, sum(sales_part.ret) as ret, sum(sales_part.np) as np, sum(sales_part.rnl) as rnl limit 10;`

  ```text
  Syntax error in stdin:  --> 2:20
    |
  2 | with sales_part as (where s.date.date between '2000-08-23'::date and '2000-09-06'::date and s.channel_dim_id is not null select case when s.channel = 'STORE' then 'store channel' when s.channel = 'CATALOG' then 'catalog channel' when s.channel = 'WEB' then 'web channel' end as ch, concat(case when s.channel = 'STORE' then 'store' when s.channel = 'CATALOG' then 'catalog_page' when s.channel = 'WEB' then 'web_site' end, s.channel_dim_text_id) as ent, s.ext_sales_price as gs, 0::float as ret, s.net_profit as np, 0::float as rnl) select sales_part.ch, sales_part.ent, sum(sales_part.gs) as gs, sum(sales_part.ret) as ret, sum(sales_part.np) as np, sum(sales_part.rnl) as rnl limit 10;
    |                    ^---
    |
    = expected select_statement or tvf_union_invocation
  Location:
  ...ales as s; with sales_part as ??? (where s.date.date between '20...
  ```
- `trilogy file write test_auto2.preql --content import raw.all_sales as s;

auto sale_gross <- sum(s.ext_sales_price ? (s.date.date between '2000-08-23'::date … s.return_channel_dim_id);

select
  s.channel,
  coalesce(s.channel_dim_text_id, s.return_channel_dim_text_id) as entity_id,
  sale_gross
limit 10;
`

  ```text
  refused to write 'test_auto2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ....channel_dim_id is not null)) ??? by coalesce(s.channel_dim_id,

  Write stats: received 347 chars / 347 bytes; tail: …'n_channel_dim_text_id) as entity_id,\\n  sale_gross\\nlimit 10;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

with sales_agg as
where s.date.date between '2000-08-23'::date and '2000-09-06'::date
…ale_ch = ret_agg.ret_ch
full join sales_agg.sale_entity = ret_agg.ret_entity
order by
  g1 + g2 desc,
  channel_type asc,
  entity_id asc
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 39:56
     |
  39 |   coalesce(sum(sales_agg.sale_gross) by rollup coalesce(sales_agg.sale_ch, ret_agg.ret_ch), coalesce(sales_agg.sale_entity, ret_agg.ret_entity), 0) as total_gross_sales,
     |                                                        ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...sale_gross) by rollup coalesce ??? (sales_agg.sale_ch, ret_agg.re...

  Write stats: received 2412 chars / 2412 bytes; tail: …'1 + g2 desc,\\n  channel_type asc,\\n  entity_id asc\\nlimit 100;\\n'.
  ```
- `trilogy file write test_rollup2.preql --content import raw.all_sales as s;

with sales_agg as
where s.date.date between '2000-08-23'::date and '2000-09-06'::…d, 1 - sum(combined.loss) by rollup combined.ch_type, combined.ent_id, 1 as net_profit_total
order by
  combined.ch_type asc,
  combined.ent_id asc;
`

  ```text
  refused to write 'test_rollup2.preql': not syntactically valid Trilogy.

  Parse error:
    --> 48:66
     |
  48 |   sum(combined.gross) by rollup combined.ch_type, combined.ent_id, 1 as total_gross_sales,
     |                                                                  ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...bined.ch_type, combined.ent_id ??? , 1 as total_gross_sales,   su...

  Write stats: received 2099 chars / 2099 bytes; tail: …'tal\\norder by\\n  combined.ch_type asc,\\n  combined.ent_id asc;\\n'.
  ```
- `trilogy file write query11.preql --content # Query 11: Among customers active in both store and web channels in 2001,
# find those whose web revenue grew fas…tomer.first_name asc nulls first,
         ss.customer.last_name asc nulls first,
         ss.customer.preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
    --> 86:9
     |
  86 |     end as store_growth,
     |         ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  .../ combined.s_rev_2001     end ??? as store_growth,     case when...

  Write stats: received 2433 chars / 2433 bytes; tail: …'  ss.customer.preferred_cust_flag asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query11.preql --content # Query 11: Among customers active in both store and web channels in 2001,
# find those whose web revenue grew fas…tomer.first_name asc nulls first,
         ss.customer.last_name asc nulls first,
         ss.customer.preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
    --> 37:27
     |
  37 | inner join ss.date.year = 2002
     |                           ^---
     |
     = expected IDENTIFIER
  Location:
  ..._id inner join ss.date.year = ??? 2002 order by customer_code as...

  Write stats: received 1369 chars / 1369 bytes; tail: …'  ss.customer.preferred_cust_flag asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query11.preql --content # Query 11: Among customers active in both store and web channels in 2001,
# find those whose web revenue grew fas…c nulls first,
         c.first_name asc nulls first,
         c.last_name asc nulls first,
         c.preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
    --> 60:9
     |
  60 |     end as store_growth,
     |         ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...1.s_rev) / sr01.s_rev     end ??? as store_growth,     case when...

  Write stats: received 1922 chars / 1922 bytes; tail: …'t,\\n         c.preferred_cust_flag asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query11.preql --content # Query 11: Among customers active in both store and web channels in 2001,
# find those whose web revenue grew fas…c nulls first,
         c.first_name asc nulls first,
         c.last_name asc nulls first,
         c.preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
    --> 53:85
     |
  53 |     case when sr01.s_rev = 0 then 0 else (sr02.s_rev - sr01.s_rev) / sr01.s_rev end as store_growth
     |                                                                                     ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   sr01.s_rev) / sr01.s_rev end ??? as store_growth     and case w...

  Write stats: received 1720 chars / 1720 bytes; tail: …'t,\\n         c.preferred_cust_flag asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Combo key for membership tests
auto combo_key <- concat(sales.item.brand_id::str…nt
order by nov2001.channel nulls first,
  nov2001.brand_id nulls first,
  nov2001.class_id nulls first,
  nov2001.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 31:11
     |
  31 |     count(*) as sale_count
     |           ^---
     |
     = expected access_chain
  Location:
  ...ce) as total_sales,     count( ??? *) as sale_count having total_...

  Write stats: received 1571 chars / 1571 bytes; tail: …'id nulls first,\\n  nov2001.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Combo key for membership tests
auto combo_key <- concat(sales.item.brand_id::str…

# Just count distinct channels for info
select
    nov2001.sales.channel,
    count(*) as cnt
order by nov2001.sales.channel nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 37:11
     |
  37 |     count(*) as cnt
     |           ^---
     |
     = expected access_chain
  Location:
  ...2001.sales.channel,     count( ??? *) as cnt order by nov2001.sal...

  Write stats: received 1322 chars / 1322 bytes; tail: …'as cnt\\norder by nov2001.sales.channel nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.quarter_name, count(ss.ticket_number) as cnt where ss.date.quarter_name in ('2001Q1','2001Q2','2001Q3') group by ss.date.quarter_name limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   ('2001Q1','2001Q2','2001Q3') ??? group by ss.date.quarter_name
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale….total) as total_sales
order by
  combined.last_name asc nulls first,
  combined.first_name asc nulls first,
  total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:85
    |
  7 | auto freq_item_key <- substring(item.desc, 1, 30) ? (count(store_sales.ticket_number, store_sales.date.id) by substring(item.desc, 1, 30)) > 4;
    |                                                                                     ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ount(store_sales.ticket_number ??? , store_sales.date.id) by subs...

  Write stats: received 2209 chars / 2213 bytes; tail: …'me asc nulls first,\\n  total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query27.preql --content import raw.store_sales as store_sales;

def rollup_tkt_qty(metric) -> sum(metric) by rollup store_sales.item.text_…(store_sales.sales_price) as avg_unit_price
order by
  store_sales.item.text_id asc nulls first,
  store_sales.store.state asc nulls first
limit 100;`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | def rollup_cnt_tkt -> count(store_sales.ticket_number) by rollup store_sales.item.text_id, store_sales.store.state;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ..._id, store_sales.store.state; ??? def rollup_cnt_tkt -> count(st...

  Write stats: received 1163 chars / 1163 bytes; tail: …' first,\\n  store_sales.store.state asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query36.preql --content import raw.store_sales as ss;

# Filter: store sales in year 2001 at stores in Tennessee
where ss.date.year = 2001…y hlevel desc nulls first,
         case when hlevel = 0 then ss.item.category else null end asc nulls first,
         rnk asc nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...egory, and grand total levels ??? auto g_cat <- grouping(ss.item...

  Write stats: received 1353 chars / 1353 bytes; tail: …'end asc nulls first,\\n         rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query43.preql --content import raw.store_sales as store_sales;

# Filtered aggregates per day-of-week (0=Sunday..6=Saturday)
def sun_sum(p…sum() as thursday,
    @fri_sum() as friday,
    @sat_sum() as saturday
order by
    store_sales.store.name,
    store_sales.store.text_id
limit 100
`

  ```text
  refused to write 'query43.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:1
     |
  27 | limit 100
     | ^---
     |
     = expected ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
      store_sales.store.text_id ??? limit 100

  Write stats: received 1090 chars / 1090 bytes; tail: …'e_sales.store.name,\\n    store_sales.store.text_id\\nlimit 100\\n'.
  ```
- `trilogy file write query43.preql -e -c import raw.store_sales as store_sales;

def sun_sum() -> sum(store_sales.sales_price ? store_sales.date.day_of_week = …sum() as thursday,
    @fri_sum() as friday,
    @sat_sum() as saturday
order by
    store_sales.store.name,
    store_sales.store.text_id
limit 100
`

  ```text
  refused to write 'query43.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:13
    |
  3 | def sun_sum() -> sum(store_sales.sales_price ? store_sales.date.day_of_week = 0);
    |             ^---
    |
    = expected IDENTIFIER
  Location:
   as store_sales;  def sun_sum( ??? ) -> sum(store_sales.sales_pri...

  Write stats: received 1022 chars / 1022 bytes; tail: …'e_sales.store.name,\\n    store_sales.store.text_id\\nlimit 100\\n'.
  ```
- `trilogy file write query43.preql -e -c import raw.store_sales as store_sales;

auto sunday_sum <- sum(store_sales.sales_price ? store_sales.date.day_of_week …sum as thursday,
    friday_sum as friday,
    saturday_sum as saturday
order by
    store_sales.store.name,
    store_sales.store.text_id
limit 100
`

  ```text
  refused to write 'query43.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:1
     |
  26 | limit 100
     | ^---
     |
     = expected ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
      store_sales.store.text_id ??? limit 100

  Write stats: received 1052 chars / 1052 bytes; tail: …'e_sales.store.name,\\n    store_sales.store.text_id\\nlimit 100\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.month_seq, ss.date.year, ss.date.month_of_year, count(ss.line_item) as cnt where ss.date.year = 1998 group by ss.date.month_seq, ss.date.year, ss.date.month_of_year order by ss.date.month_seq limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...cnt where ss.date.year = 1998 ??? group by ss.date.month_seq, ss...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.item.text_id, sum(all_sales.ext_sales_price) as store_total where all_sales.date.week_seq = 5218 and all_sales.channel = 'STORE' group by all_sales.item.text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...d all_sales.channel = 'STORE' ??? group by all_sales.item.text_i...
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

def sun_sum -> sum(ss.sales_price ? ss.date.day_of_week = 0);
def mon_sum -> sum(ss…ext_year.store_name
order by store_name asc nulls first,
         store_code asc nulls first,
         this_year_week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:1
    |
  3 | def sun_sum -> sum(ss.sales_price ? ss.date.day_of_week = 0);
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...mport raw.store_sales as ss;  ??? def sun_sum -> sum(ss.sales_pr...

  Write stats: received 2862 chars / 2862 bytes; tail: …'rst,\\n         this_year_week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

def sun_sum -> sum(ss.sales_price ? ss.date.day_of_week = 0);

select ss.store.name as store_name limit 5;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:1
    |
  3 | def sun_sum -> sum(ss.sales_price ? ss.date.day_of_week = 0);
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...mport raw.store_sales as ss;  ??? def sun_sum -> sum(ss.sales_pr...

  Write stats: received 137 chars / 137 bytes; tail: …'y_of_week = 0);\\n\\nselect ss.store.name as store_name limit 5;'.
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# 2001 weekly data per store
with this_year as
where ss.date.year = 2001
select
   …ext_year.store_code
order by store_name asc nulls first,
         store_code asc nulls first,
         this_year_week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 54:51
     |
  54 |     left join this_year.wk_seq = next_year.wk_seq - 52
     |                                                   ^---
     |
     = expected limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...ear.wk_seq = next_year.wk_seq ??? - 52     and this_year.store_c...

  Write stats: received 2900 chars / 2900 bytes; tail: …'rst,\\n         this_year_week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;

# Deduplicate per-line net values at the (year, brand, class, category, manufactur…as curr_qty,
    totals.total_qty - prev_qty as qty_diff,
    totals.total_amt - prev_amt as amt_diff
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:37
     |
  17 | auto totals <- sum(deduped.net_qty) as total_qty, sum(deduped.net_amt) as total_amt
     |                                     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over
  Location:
  ...otals <- sum(deduped.net_qty) ??? as total_qty, sum(deduped.net_...

  Write stats: received 1490 chars / 1490 bytes; tail: …' as amt_diff\\norder by qty_diff asc, amt_diff asc\\nlimit 100;\\n'.
  ```
- `trilogy file write _test2.preql --content import raw.all_sales as sales;

rowset deduped <-
where sales.item.category = 'Books'
select
    year(sales.date.da…otal_qty,
    sum(deduped.net_amt) as total_amt
;

select agg.yr, agg.bid, count(*) as cnt
where agg.yr in (2001, 2002)
order by agg.yr asc
limit 10;`

  ```text
  refused to write '_test2.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:31
     |
  26 | select agg.yr, agg.bid, count(*) as cnt
     |                               ^---
     |
     = expected access_chain
  Location:
  ...select agg.yr, agg.bid, count( ??? *) as cnt where agg.yr in (200...

  Write stats: received 754 chars / 754 bytes; tail: …'t\\nwhere agg.yr in (2001, 2002)\\norder by agg.yr asc\\nlimit 10;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…al_ext_sales_price
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
    --> 39:87
     |
  39 | ) -> (channel, missing_ref, year, quarter, category, line_count, total_ext_sales_price)
     |                                                                                       ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...e_count, total_ext_sales_price ??? )  select     combined.channel...

  Write stats: received 1870 chars / 1870 bytes; tail: …'year,\\n    combined.quarter,\\n    combined.category\\nlimit 100;'.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as s;

# Channel labels
auto channel_label <- case
    when s.channel = 'STORE' then 'store c…d period_end)
    ) as profit_amt
order by channel_label asc nulls first,
         s.outlet_id asc nulls first,
         returns_amt desc
limit 100;
`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...here s.outlet_id is not null  ??? def rollup_channel_outlet(metr...

  Write stats: received 1128 chars / 1128 bytes; tail: …'et_id asc nulls first,\\n         returns_amt desc\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:sales where sales.date.date between '2000-08-23'::date and '2000-09-22'::date and sales.item.current_price > 50 and sales.promotion.channel_tv = 'N' and sales.channel_dim_id is not null select sales.channel, count(sales.row_one) as cnt group by sales.channel limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..., count(sales.row_one) as cnt ??? group by sales.channel limit 2...
  ```
- `trilogy run --import raw.all_sales:sales where sales.date.date between '2000-08-23'::date and '2000-09-22'::date and sales.item.current_price > 50 and sales.…returns, sum(sales.net_profit) - sum(sales.return_net_loss) as profit group by sales.channel_dim_text_id order by sales.channel_dim_text_id limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...es.return_net_loss) as profit ??? group by sales.channel_dim_tex...
  ```
- `trilogy file write query81.preql --content import raw.catalog_returns as cr;

# Year 2000 filter for date
where cr.date.year = 2000

# Per (returning custome…ess.country,
    cr.billing_customer.address.gmt_offset,
    cr.billing_customer.address.location_type,
    sum(cr.return_amt_inc_tax) asc
limit 100;`

  ```text
  refused to write 'query81.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...l return amount including tax ??? auto cust_state_total <- sum(c...

  Write stats: received 1873 chars / 1873 bytes; tail: …'location_type,\\n    sum(cr.return_amt_inc_tax) asc\\nlimit 100;'.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

# Web sales in year 2000, rolled up by item category and class
where ws.date.year = 2…  ) as within_parent_rank
order by
    hierarchy_level desc nulls first,
    item.category nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:30
    |
  9 |     grouping(ws.item.category, ws.item.class) as hierarchy_level,
    |                              ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
       grouping(ws.item.category ??? , ws.item.class) as hierarchy_...

  Write stats: received 702 chars / 702 bytes; tail: …'y nulls first,\\n    within_parent_rank nulls first\\nlimit 100;'.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

# Web sales in year 2000, rolled up by item category and class
where ws.date.year = 2…    within_parent_rank
order by
    hierarchy_level desc nulls first,
    ws.item.category nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...ss where ws.date.year = 2000  ??? auto total_net_paid <- sum(ws....

  Write stats: received 1007 chars / 1007 bytes; tail: …'y nulls first,\\n    within_parent_rank nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:ss select ss.customer.id, sum(case when ss.quantity is null or ss.sales_price is null then null when ss.return_quantity …se ss.quantity * ss.sales_price end) as adj where ss.return_reason.desc = 'reason 28' group by ss.customer.id having adj is not null limit 10; duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...urn_reason.desc = 'reason 28' ??? group by ss.customer.id having...
  ```
- `trilogy run --import raw/store_sales:ss select ss.customer.id as customer_id, sum(case when ss.quantity is null or ss.sales_price is null then null when ss.r…ice end) as adj where ss.return_reason.desc = 'reason 28' group by ss.customer.id order by adj nulls first, customer_id nulls first limit 100; duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...urn_reason.desc = 'reason 28' ??? group by ss.customer.id order
  ```
- `trilogy file write query95.preql --content import raw/web_sales as ws;

where
    ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
    and…ofit) as total_net_profit
having
    count_distinct(ws.warehouse.id) > 1
    and bool_or(ws.is_returned) = true
order by
    unique_orders
limit 100;`

  ```text
  refused to write 'query95.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/web_sales as ws;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/web_sales as ws;  where

  Write stats: received 467 chars / 467 bytes; tail: …'ws.is_returned) = true\\norder by\\n    unique_orders\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy run query23.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/customer_demographics.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_demographics.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy run - --import raw.all_sales:sales select sales.ext_sales_price, sales.return_amount, sales.quantity, sales.return_quantity limit 5;`

  ```text
  'select sales.ext_sales_price, sales.return_amount, sales.quantity, sales.return_quantity limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run query77.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy run query99.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:s where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_id is not null selec…when s.channel = 'WEB' then 'web_site' end, s.return_channel_dim_text_id, sum(s.return_amount) as total_returns order by total_returns desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `case when s.channel = 'STORE' then 'store' when s.channel = 'CATALOG' then 'catalog_page' when s.channel = 'WEB' then 'web_site' end as case_when_s_channel_store_then_store_whe`
  Location:
  ...el = 'WEB' then 'web_site' end ??? , s.return_channel_dim_text_id...
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Combo key for membership tests
auto combo_key <- concat(sales.item.brand_id::str…total_sales > overall_avg;

# Check distinct channels
select distinct
    nov2001.sales.channel
order by nov2001.sales.channel nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct
      nov2001.sales.channel as distinct_nov2001_sales_channel`
  Location:
   channels select distinct     ??? nov2001.sales.channel order by...

  Write stats: received 1283 chars / 1283 bytes; tail: …'hannel\\norder by nov2001.sales.channel nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.date:date select distinct date.quarter_name limit 20;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct date.quarter_name as distinct_date_quarter_name`
  Location:
  ...date as date; select distinct ??? date.quarter_name limit 20;
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any of the 8 detailed attribute profiles
with profile_items as
select
 …oduct_name
where
    item.manufacturer_id between 1 and 500
    and item.manufact in profile_items.manufact
order by
    item.product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct
      item.product_name as distinct_item_product_name`
  Location:
  ...ile items select distinct     ??? item.product_name where     it...

  Write stats: received 1235 chars / 1235 bytes; tail: …'ile_items.manufact\\norder by\\n    item.product_name\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.item.category, cs.item.class, count(cs.line_item) where cs.sold_date.year = 1998 and cs.sold_date.month_of_year = 12 and cs.item.category = 'Women' and cs.item.class = 'maternity' limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cs.line_item) as line_item_count`
  Location:
  ...em.class, count(cs.line_item) ??? where cs.sold_date.year = 1998...
  ```
- `trilogy file write _test2.preql --content import raw.all_sales as sales;

rowset deduped <-
where sales.item.category = 'Books'
select
    year(sales.date.da…, 1) over (partition by agg.bid, agg.cid, agg.catid, agg.mid order by agg.yr asc) is not null
order by agg.bid, agg.cid, agg.catid, agg.mid
limit 10;`

  ```text
  refused to write '_test2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `agg.total_amt::float as agg_total_amt_float`
  Location:
  ..._qty,     agg.total_amt::float ??? ,     lag(agg.total_qty, 1) ov...

  Write stats: received 1197 chars / 1197 bytes; tail: …'null\\norder by agg.bid, agg.cid, agg.catid, agg.mid\\nlimit 10;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as sales;

# Date range: 2000-08-23 through 2000-09-22
# Item: current_price > 50
# Promotion…_loss), 0)) by rollup channel_label, outlet_identifier as profit
order by channel_label asc nulls first, outlet_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `coalesce(sum(sales.return_amount), 0) by rollup channel_label as coalesce_sum_sales_return_amount_0_by_ro`
  Location:
  ...(sum(sales.return_amount), 0) ??? by rollup channel_label, outle...

  Write stats: received 1282 chars / 1282 bytes; tail: …'sc nulls first, outlet_identifier asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as sales;

# Date range: 2000-08-23 through 2000-09-22
# Item: current_price > 50
# Promotion…t_loss), 0) by rollup channel_label, outlet_identifier as profit
order by channel_label asc nulls first, outlet_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `sum(sales.net_profit) - coalesce(sum(sales.return_net_loss), 0) by rollup channel_label as sum_sales_net_profit_coalesce_sum_sales_`
  Location:
  ...um(sales.return_net_loss), 0) ??? by rollup channel_label, outle...

  Write stats: received 1267 chars / 1267 bytes; tail: …'sc nulls first, outlet_identifier asc nulls first\\nlimit 100;'.
  ```

### `join-resolution`

- `trilogy run query11.preql duckdb`

  ```text
  Resolution error in query11.preql: Could not resolve connections for query with output ['local.customer_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>', 'ss.customer.first_name<Purpose.PROPERTY>Derivation.ROOT>', 'ss.customer.last_name<Purpose.PROPERTY>Derivation.ROOT>', 'ss.customer.preferred_cust_flag<Purpose.PROPERTY>Derivation.ROOT>', 'local.s_2001<Purpose.METRIC>Derivation.BASIC>', 'local.s_2002<Purpose.METRIC>Derivation.BASIC>', 'local.w_2001<Purpose.METRIC>Derivation.BASIC>', 'local.w_2002<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Could not resolve connections for query with output ['local.inventory_items<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Could not resolve connections for query with output ['local.customer_id<Purpose.KEY>Derivation.BASIC>', 'local.total_price<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query57.preql`

  ```text
  Resolution error in query57.preql: Could not resolve connections for query with output ['local._monthly_data_category<Purpose.PROPERTY>Derivation.BASIC>', 'local._monthly_data_brand<Purpose.PROPERTY>Derivation.BASIC>', 'local._monthly_data_call_center<Purpose.PROPERTY>Derivation.BASIC>', 'local._monthly_data_year<Purpose.PROPERTY>Derivation.BASIC>', 'local._monthly_data_month_of_year<Purpose.PROPERTY>Derivation.BASIC>', 'local._monthly_data_total<Purpose.METRIC>Derivation.BASIC>', 'local._monthly_data_avg_monthly_sales<Purpose.METRIC>Derivation.BASIC>', 'local.seq_num<Purpose.PROPERTY>Derivation.WINDOW>', 'local.prev_total<Purpose.PROPERTY>Derivation.WINDOW>', 'local.next_total<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run query57.preql`

  ```text
  Resolution error in query57.preql: Could not resolve connections for query with output ['local.category<Purpose.PROPERTY>Derivation.BASIC>', 'local.brand<Purpose.PROPERTY>Derivation.BASIC>', 'local.call_center<Purpose.PROPERTY>Derivation.BASIC>', 'local.year<Purpose.PROPERTY>Derivation.BASIC>', 'local.month_of_year<Purpose.PROPERTY>Derivation.BASIC>', 'local.total<Purpose.METRIC>Derivation.BASIC>', 'local.avg_monthly_sales<Purpose.METRIC>Derivation.BASIC>', 'local.signed_diff<Purpose.PROPERTY>Derivation.BASIC>', 'local.prior_month_total<Purpose.PROPERTY>Derivation.BASIC>', 'local.next_month_total<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query57.preql`

  ```text
  Resolution error in query57.preql: Could not resolve connections for query with output ['local.category<Purpose.PROPERTY>Derivation.BASIC>', 'local.brand<Purpose.PROPERTY>Derivation.BASIC>', 'local.call_center<Purpose.PROPERTY>Derivation.BASIC>', 'local.year<Purpose.PROPERTY>Derivation.BASIC>', 'local.month_of_year<Purpose.PROPERTY>Derivation.BASIC>', 'local.total<Purpose.METRIC>Derivation.AGGREGATE>', 'local.avg_monthly_sales<Purpose.METRIC>Derivation.AGGREGATE>', 'local.seq_num<Purpose.PROPERTY>Derivation.WINDOW>', 'local.prior_month_total<Purpose.METRIC>Derivation.WINDOW>', 'local.next_month_total<Purpose.METRIC>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run query57.preql`

  ```text
  Resolution error in query57.preql: Could not resolve connections for query with output ['item.category<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand_name<Purpose.PROPERTY>Derivation.ROOT>', 'cc.name<Purpose.PROPERTY>Derivation.ROOT>', 'cs.sold_date.year<Purpose.PROPERTY>Derivation.ROOT>', 'cs.sold_date.month_of_year<Purpose.PROPERTY>Derivation.ROOT>', 'local.monthly_total<Purpose.METRIC>Derivation.AGGREGATE>', 'local.avg_monthly_sales<Purpose.METRIC>Derivation.AGGREGATE>', 'local.seq_num<Purpose.PROPERTY>Derivation.WINDOW>', 'local.prior_month_total<Purpose.PROPERTY>Derivation.WINDOW>', 'local.next_month_total<Purpose.PROPERTY>Derivation.WINDOW>', 'local.signed_diff<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query87.preql`

  ```text
  Resolution error in query87.preql: Could not resolve connections for query with output ['local.unique_name_date_combinations<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `undefined-concept`

- `trilogy explore query44.preql`

  ```text
  Failed to parse query44.preql: (UndefinedConceptException(...), "Undefined concept: ranked_items.product_name. Suggestions: ['ranked_items.store_sales.item.product_name', 'store_sales.item.product_name', 'ranked_items.avg_profit']")
  ```
- `trilogy explore query47.preql`

  ```text
  Failed to parse query47.preql: (UndefinedConceptException(...), '12 undefined concept references; fix all before re-running:\n  - monthly_totals.category (line 20, col 5, in SELECT); did you mean: monthly_totals.ss.item.category, ss.item.category, monthly_totals.name, monthly_totals.year?\n  - monthly_totals.brand_name (line 21, col 5, in SELECT); did you mean: monthly_totals.ss.item.brand_name, ss.item.brand_name, monthly_totals.name, monthly_totals.company_name?\n  - monthly_totals.name (line 22, col 5, in SELECT); did you mean: monthly_totals.ss.store.name, ss.store.name, ss.return_store.name, monthly_totals.brand_name, monthly_totals.year, monthly_totals.company_name?\n  - monthly_totals.company_name (line 23, col 5, in SELECT); did you mean: monthly_totals.ss.store.company_name, ss.store.company_name, ss.return_store.company_name, monthly_totals.brand_name, monthly_totals.name?\n  - monthly_totals.year (line 24, col 5, in SELECT); did you mean: monthly_totals.ss.date.year, ss.date.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.return_customer.first_sales_date.year?\n  - monthly_totals.month_of_year (line 25, col 5, in SELECT); did you mean: monthly_totals.ss.date.month_of_year, ss.date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year?\n  - monthly_totals.category (line 34, col 10, in ORDER BY); did you mean: monthly_totals.ss.item.category, ss.item.category, monthly_totals.name, monthly_totals.year?\n  - monthly_totals.brand_name (line 35, col 10, in ORDER BY); did you mean: monthly_totals.ss.item.brand_name, ss.item.brand_name, monthly_totals.name, monthly_totals.company_name?\n  - monthly_totals.name (line 36, col 10, in ORDER BY); did you mean: monthly_totals.ss.store.name, ss.store.name, ss.return_store.name, monthly_totals.brand_name, monthly_totals.year, monthly_totals.company_name?\n  - monthly_totals.company_name (line 37, col 10, in ORDER BY); did you mean: monthly_totals.ss.store.company_name, ss.store.company_name, ss.return_store.company_name, monthly_totals.brand_name, monthly_totals.name?\n  - monthly_totals.year (line 38, col 10, in ORDER BY); did you mean: monthly_totals.ss.date.year, ss.date.year, ss.return_date.year, ss.customer.first_sales_date.year, ss.customer.first_shipto_date.year, ss.return_customer.first_sales_date.year?\n  - monthly_totals.month_of_year (line 39, col 10, in ORDER BY); did you mean: monthly_totals.ss.date.month_of_year, ss.date.month_of_year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year, ss.customer.first_shipto_date.month_of_year, ss.return_customer.first_sales_date.month_of_year?')
  ```
- `trilogy explore query64.preql --regex agg_sales`

  ```text
  Failed to parse query64.preql: (UndefinedConceptException(...), "Undefined concept: agg_sales.product_name (line 50, col 3, in SELECT). Suggestions: ['agg_sales.ss.item.product_name', 'ss.item.product_name', 'cs.item.product_name', 'cr.sales.item.product_name', 'cr.item.product_name', 'agg_sales.store_name']")
  ```

### `file-not-found`

- `trilogy run query56.preql`

  ```text
  Unexpected error in query56.preql: Join key `qualifying_items.text_id` does not exist
  ```

### `type-error`

- `trilogy run query90.preql duckdb`

  ```text
  Unexpected error in query90.preql: Invalid argument type 'INTEGER' passed into HOUR function in position 1 from concept: ws.time.time. Valid: 'DATE', 'DATETIME', 'STRING', 'TIMESTAMP'.
  ```
