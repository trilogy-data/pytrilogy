# Trilogy failure analysis — 20260622-125347

- Run `20260622-125347` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1586 | failed: 255 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 167 | 65% |
| `syntax-parse` | 50 | 20% |
| `cli-misuse` | 17 | 7% |
| `syntax-missing-alias` | 9 | 4% |
| `file-not-found` | 6 | 2% |
| `join-resolution` | 5 | 2% |
| `undefined-concept` | 1 | 0% |

## Detail

### `other`

- `trilogy file read query01.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: d.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY references 'local.lvl', which is not in the SELECT projection (line 59). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.lvl order by local.lvl asc`.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Output column 'lvl' aliases 'local.lvl', which is itself the 'lvl' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'lvl_out').
  ```
- `trilogy run --import raw.all_sales:s select s.channel, s.channel_dim_text_id, s.return_channel_dim_text_id, s.return_amount, s.return_net_loss, s.return_date.date where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_id is not null limit 20;`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in query08.preql: Undefined concept: customer.preferred_cust_flag. Suggestions: ['sales.customer.preferred_cust_flag', 'sales.return_customer.preferred_cust_flag']
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unexpected error in query08.preql: (_duckdb.BinderException) Binder Error: Cannot compare values of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is required

  LINE 41:     "sales_customer_address_customer_address"."CA_ZIP" in (select quizzical."zips_list" from quizzical where quizz...
                                                                  ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "sales_customer_address_customer_address"."CA_ZIP" as "sales_customer_address_zip",
      count(CASE WHEN "sales_customer_customers"."C_PREFERRED_CUST_FLAG" = 'Y' THEN "sales_customer_customers"."C_CUSTOMER_SK" ELSE NULL END) as "preferred_zip_counts"
  FROM
      "customer" as "sales_customer_customers"
      INNER JOIN "customer_address" as "sales_customer_address_customer_address" on "sales_customer_customers"."C_CURRENT_ADDR_SK" = "sales_customer_address_customer_address"."CA_ADDRESS_SK"
  GROUP BY
      1),
  quizzical as (
  SELECT
      STRING_SPLIT( $1 , ',' ) as "zips_list"
  ),
  questionable as (
  SELECT
      "cheerful"."preferred_zip_counts" as "preferred_zip_counts",
      "sales_customer_address_customer_address"."CA_ZIP" as "sales_customer_address_zip"
  FROM
      "cheerful"
      INNER JOIN "customer_address" as "sales_customer_address_customer_address" on "cheerful"."sales_customer_address_zip" = "sales_customer_address_customer_address"."CA_ZIP"),
  abundant as (
  SELECT
      CASE WHEN "questionable"."preferred_zip_counts" > 10 THEN "questionable"."sales_customer_address_zip" ELSE NULL END as "qualified_preferred_zips"
  FROM
      "questionable"),
  uneven as (
  SELECT
      "abundant"."qualified_preferred_zips" as "qualified_preferred_zips"
  FROM
      "abundant"
  GROUP BY
      1),
  yummy as (
  SELECT
      "sales_customer_address_customer_address"."CA_ZIP" as "qualifying_zips"
  FROM
      "customer_address" as "sales_customer_address_customer_address"
  WHERE
      "sales_customer_address_customer_address"."CA_ZIP" in (select quizzical."zips_list" from quizzical where quizzical."zips_list" is not null) and "sales_customer_address_customer_address"."CA_ZIP" in (select uneven."qualified_preferred_zips" from uneven where uneven."qualified_preferred_zips" is not null)
  ),
  juicy as (
  SELECT
      SUBSTRING("yummy"."qualifying_zips",1,2) as "qualifying_prefixes"
  FROM
      "yummy"
  GROUP BY
      1)
  SELECT
      "sales_store_store"."S_STORE_NAME" as "store_name",
      sum("sales_store_sales"."SS_NET_PROFIT") as "total_net_profit"
  FROM
      "store_sales" as "sales_store_sales"
      INNER JOIN "date_dim" as "sales_date_date" on "sales_store_sales"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "sales_store_store" on "sales_store_sales"."SS_STORE_SK" = "sales_store_store"."S_STORE_SK"
  WHERE
      "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_QOY" = 2 and SUBSTRING("sales_store_store"."S_ZIP",1,2) in (select juicy."qualifying_prefixes" from juicy where juicy."qualifying_prefixes" is not null)

  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]
  [parameters: ('24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,28577,55565,17183,54601,67897,22752 ... (2101 characters truncated) ... 26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576',)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query08.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Syntax error in query11.preql: HAVING references 'all_sales.channel', which is not in the SELECT projection (line 14). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --all_sales.channel
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Resolution error in query11.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {all_sales.billing_customer.id, all_sales.billing_customer.preferred_cust_flag, all_sales.channel, all_sales.date.year, both_2001.cust_id}
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Syntax error in query11.preql: Undefined concept: WEB.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Syntax error in query11.preql: Undefined concept: STORE.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Syntax error in query11.preql: Undefined concept: STORE.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Syntax error in query11.preql: Undefined concept: STORE.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Syntax error in query11.preql: Undefined concept: all_sales.row_counter. Suggestions: ['all_sales.row_one', 'all_sales.coupon_amt', 'all_sales.return_amount']
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Unexpected error in query11.preql: (_duckdb.BinderException) Binder Error: WHERE clause cannot contain aggregates!

  LINE 41:     (sum(CASE WHEN "cheerful"."all_sales_channel" = 'STORE' and...
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "all_sales_billing_customer_id",
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_EXT_DISCOUNT_AMT" as "all_sales_ext_discount_amount",
      "all_sales_catalog_sales_unified"."CS_EXT_LIST_PRICE" as "all_sales_ext_list_price",
       1  as "all_sales_row_one"
  FROM
      "catalog_sales" as "all_sales_catalog_sales_unified"
  UNION ALL
  SELECT
      "all_sales_store_sales_unified"."SS_CUSTOMER_SK" as "all_sales_billing_customer_id",
       'STORE'  as "all_sales_channel",
      "all_sales_store_sales_unified"."SS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_store_sales_unified"."SS_EXT_DISCOUNT_AMT" as "all_sales_ext_discount_amount",
      "all_sales_store_sales_unified"."SS_EXT_LIST_PRICE" as "all_sales_ext_list_price",
       1  as "all_sales_row_one"
  FROM
      "store_sales" as "all_sales_store_sales_unified"
  UNION ALL
  SELECT
      "all_sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "all_sales_billing_customer_id",
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_web_sales_unified"."WS_EXT_DISCOUNT_AMT" as "all_sales_ext_discount_amount",
      "all_sales_web_sales_unified"."WS_EXT_LIST_PRICE" as "all_sales_ext_list_price",
       1  as "all_sales_row_one"
  FROM
      "web_sales" as "all_sales_web_sales_unified")
  SELECT
      "cheerful"."all_sales_billing_customer_id" as "cid",
      sum(CASE WHEN "cheerful"."all_sales_channel" = 'STORE' and "all_sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."all_sales_ext_list_price" - coalesce("cheerful"."all_sales_ext_discount_amount",0.0) ELSE NULL END) as "s2001",
      sum(CASE WHEN "cheerful"."all_sales_channel" = 'WEB' and "all_sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."all_sales_ext_list_price" - coalesce("cheerful"."all_sales_ext_discount_amount",0.0) ELSE NULL END) as "w2001"
  FROM
      "cheerful"
      LEFT OUTER JOIN "date_dim" as "all_sales_date_date" on "cheerful"."all_sales_date_id" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      (sum(CASE WHEN "cheerful"."all_sales_channel" = 'STORE' and "all_sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."all_sales_row_one" ELSE NULL END) > 0) = True and (sum(CASE WHEN "cheerful"."all_sales_channel" = 'WEB' and "all_sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."all_sales_row_one" ELSE NULL END) > 0) = True

  GROUP BY
      1
  HAVING
      sum(CASE WHEN "cheerful"."all_sales_channel" = 'STORE' and "all_sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."all_sales_ext_list_price" - coalesce("cheerful"."all_sales_ext_discount_amount",0.0) ELSE NULL END) > 0.0 and sum(CASE WHEN "cheerful"."all_sales_channel" = 'WEB' and "all_sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."all_sales_ext_list_price" - coalesce("cheerful"."all_sales_ext_discount_amount",0.0) ELSE NULL END) > 0.0

  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query12.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 22). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query17.preql --import raw.date:date select distinct date.quarter_name, date.year, date.quarter limit 20;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy run query18.preql duckdb`

  ```text
  Syntax error in query18.preql: Comparison `cs.bill_customer_demographic.gender = 'Female'` can never match enum field 'cs.bill_customer_demographic.gender', which contains only these values: 'M', 'F'. It is always false and should be removed.
  ```
- `trilogy file read query21.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'customer_totals.cust_total', 'local.max_total', which are not in the SELECT projection (line 31). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --customer_totals.cust_total, --local.max_total
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query27.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query30.preql`

  ```text
  Unexpected error in query30.preql: (_duckdb.BinderException) Binder Error: ORDER term out of range - should be between 1 and 12

  LINE 96:     13 asc
               ^
  [SQL:
  WITH
  cooperative as (
  SELECT
      "wr_billing_customer_customers"."C_CUSTOMER_ID" as "wr_billing_customer_text_id",
      "wr_billing_customer_customers"."C_CUSTOMER_SK" as "wr_billing_customer_id",
      "wr_return_address_customer_address"."CA_STATE" as "wr_return_address_state",
      sum("wr_web_returns"."WR_RETURN_AMT") as "cust_state_ret_amt"
  FROM
      "web_returns" as "wr_web_returns"
      INNER JOIN "date_dim" as "wr_return_date_date" on "wr_web_returns"."WR_RETURNED_DATE_SK" = "wr_return_date_date"."D_DATE_SK"
      INNER JOIN "customer_address" as "wr_return_address_customer_address" on "wr_web_returns"."WR_RETURNING_ADDR_SK" = "wr_return_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer" as "wr_billing_customer_customers" on "wr_web_returns"."WR_RETURNING_CUSTOMER_SK" = "wr_billing_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "wr_billing_customer_address_customer_address" on "wr_billing_customer_customers"."C_CURRENT_ADDR_SK" = "wr_billing_customer_address_customer_address"."CA_ADDRESS_SK"
  WHERE
      year(cast("wr_return_date_date"."D_DATE" as date)) = 2002 and "wr_billing_customer_address_customer_address"."CA_STATE" = 'GA'

  GROUP BY
      1,
      2,
      3),
  uneven as (
  SELECT
      "cooperative"."cust_state_ret_amt" as "cust_state_ret_amt",
      "cooperative"."wr_billing_customer_text_id" as "wr_billing_customer_text_id",
      "cooperative"."wr_return_address_state" as "wr_return_address_state",
      "wr_billing_customer_customers"."C_BIRTH_COUNTRY" as "wr_billing_customer_birth_country",
      "wr_billing_customer_customers"."C_BIRTH_DAY" as "wr_billing_customer_birth_day",
      "wr_billing_customer_customers"."C_BIRTH_MONTH" as "wr_billing_customer_birth_month",
      "wr_billing_customer_customers"."C_BIRTH_YEAR" as "wr_billing_customer_birth_year",
      "wr_billing_customer_customers"."C_EMAIL_ADDRESS" as "wr_billing_customer_email_address",
      "wr_billing_customer_customers"."C_FIRST_NAME" as "wr_billing_customer_first_name",
      "wr_billing_customer_customers"."C_LAST_NAME" as "wr_billing_customer_last_name",
      "wr_billing_customer_customers"."C_LAST_REVIEW_DATE_SK" as "wr_billing_customer_last_review_date",
      "wr_billing_customer_customers"."C_LOGIN" as "wr_billing_customer_login",
      "wr_billing_customer_customers"."C_PREFERRED_CUST_FLAG" as "wr_billing_customer_preferred_cust_flag",
      "wr_billing_customer_customers"."C_SALUTATION" as "wr_billing_customer_salutation"
  FROM
      "cooperative"
      INNER JOIN "customer" as "wr_billing_customer_customers" on "cooperative"."wr_billing_customer_id" = "wr_billing_customer_customers"."C_CUSTOMER_SK"),
  abundant as (
  SELECT
      "cooperative"."wr_return_address_state" as "wr_return_address_state",
      avg("cooperative"."cust_state_ret_amt") as "state_avg_ret_amt"
  FROM
      "cooperative"
  GROUP BY
      1)
  SELECT
      "uneven"."wr_billing_customer_text_id" as "customer_code",
      "uneven"."wr_billing_customer_salutation" as "wr_billing_customer_salutation",
      "uneven"."wr_billing_customer_first_name" as "wr_billing_customer_first_name",
      "uneven"."wr_billing_customer_last_name" as "wr_billing_customer_last_name",
      "uneven"."wr_billing_customer_preferred_cust_flag" as "wr_billing_customer_preferred_cust_flag",
      "uneven"."wr_billing_customer_birth_day" as "wr_billing_customer_birth_day",
      "uneven"."wr_billing_customer_birth_month" as "wr_billing_customer_birth_month",
      "uneven"."wr_billing_customer_birth_year" as "wr_billing_customer_birth_year",
      "uneven"."wr_billing_customer_birth_country" as "wr_billing_customer_birth_country",
      "uneven"."wr_billing_customer_login" as "wr_billing_customer_login",
      "uneven"."wr_billing_customer_email_address" as "wr_billing_customer_email_address",
      "uneven"."wr_billing_customer_last_review_date" as "wr_billing_customer_last_review_date"
  FROM
      "uneven"
      RIGHT OUTER JOIN "abundant" on "uneven"."wr_return_address_state" is not distinct from "abundant"."wr_return_address_state"
  WHERE
      "uneven"."cust_state_ret_amt" > 1.2 * "abundant"."state_avg_ret_amt"

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
      12,
      "abundant"."state_avg_ret_amt",
      "uneven"."cust_state_ret_amt"
  ORDER BY
      1 asc,
      2 asc,
      3 asc,
      4 asc,
      5 asc,
      6 asc,
      7 asc,
      8 asc,
      9 asc,
      10 asc,
      11 asc,
      12 asc,
      13 asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: HAVING references 'local.state_avg_ret_amt', which is not in the SELECT projection (line 9). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.state_avg_ret_amt
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query31.preql`

  ```text
  Syntax error in query31.preql: Undefined concept: combined.store_ext_sales. Suggestions: ['combined.store_by_q.store_ext_sales', 'store_by_q.store_ext_sales', 'combined.web_by_q.web_ext_sales']
  ```
- `trilogy run query31.preql`

  ```text
  Resolution error in query31.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {store_by_q.store_ext_sales}; {web_by_q.county}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query31.preql`

  ```text
  Syntax error in query31.preql: WHERE clause aggregate `sum(<Filter: ref:county_data.web_ext_sales where ref:county_data.q = 2>)` is also computed in the SELECT (as `web_q2`); aggregate filters must use the HAVING clause - e.g. `having web_q2 > ...`; Line: 34
  ```
- `trilogy run query31.preql`

  ```text
  Syntax error in query31.preql: Duplicate select output for local.store_q1; Line: 34
  ```
- `trilogy run query31.preql`

  ```text
  Resolution error in query31.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {store_by_q.county, store_by_q.q, store_by_q.store_ext_sales, web_by_q.web_ext_sales}
  ```
- `trilogy run query31.preql`

  ```text
  Resolution error in query31.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {store_by_q.store_ext_sales, web_by_q.county}
  ```
- `trilogy run query31.preql`

  ```text
  Resolution error in query31.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {store_totals.county, store_totals.ext_sales, store_totals.q, web_totals.ext_sales}
  ```
- `trilogy run query31.preql`

  ```text
  Syntax error in query31.preql: Output column 'county' aliases 'combined.county', which is itself the 'county' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'county_out').
  ```
- `trilogy file read query31.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query36.preql`

  ```text
  Syntax error in query36.preql: ORDER BY references 'local.sort_category', which is not in the SELECT projection (line 27). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.sort_category order by local.sort_category asc`.
  ```
- `trilogy file read query38.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query39.preql`

  ```text
  Syntax error in query39.preql: Undefined concept: qualifying.mo. Suggestions: ['qualifying.agg_2001.mo', 'agg_2001.mo', 'qualifying.cv', '_qualifying_cv']
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: (_duckdb.BinderException) Binder Error: WHERE clause cannot contain aggregates!

  LINE 29: 	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
                ^
  [SQL:
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
  thoughtful as (
  SELECT
      "wakeful"."inv_date_month_of_year" as "jan_mo",
      "wakeful"."inv_item_id" as "jan_item_id",
      "wakeful"."inv_warehouse_id" as "jan_w_id",
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END as "jan_cv",
      avg("wakeful"."inv_quantity_on_hand") as "jan_mn"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END > 1 and "wakeful"."inv_date_month_of_year" = 1

  GROUP BY
      1,
      2,
      3),
  cheerful as (
  SELECT
      "wakeful"."inv_date_month_of_year" as "feb_mo",
      "wakeful"."inv_item_id" as "feb_item_id",
      "wakeful"."inv_warehouse_id" as "feb_w_id",
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END as "feb_cv",
      avg("wakeful"."inv_quantity_on_hand") as "feb_mn"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END > 1 and "wakeful"."inv_date_month_of_year" = 2

  GROUP BY
      1,
      2,
      3)
  SELECT
      "thoughtful"."jan_w_id" as "jan_w_id",
      "thoughtful"."jan_item_id" as "jan_item_id",
      "thoughtful"."jan_mo" as "jan_mo",
      "thoughtful"."jan_mn" as "jan_mn",
      "thoughtful"."jan_cv" as "jan_cv",
      "cheerful"."feb_w_id" as "feb_w_id",
      "cheerful"."feb_item_id" as "feb_item_id",
      "cheerful"."feb_mo" as "feb_mo",
      "cheerful"."feb_mn" as "feb_mn",
      "cheerful"."feb_cv" as "feb_cv"
  FROM
      "thoughtful"
      INNER JOIN "cheerful" on "thoughtful"."jan_item_id" = "cheerful"."feb_item_id" AND "thoughtful"."jan_w_id" = "cheerful"."feb_w_id"
  ORDER BY
      "cheerful"."feb_w_id" asc nulls first,
      "cheerful"."feb_item_id" asc nulls first,
      "jan_mo" asc nulls first,
      "jan_mn" asc nulls first,
      "jan_cv" asc nulls first,
      "feb_mo" asc nulls first,
      "feb_mn" asc nulls first,
      "feb_cv" asc nulls first]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Invalid reference string found in query:
  WITH
  wakeful as (
  SELECT
      "inv_date_date"."D_MOY" as "_monthly_mo",
      "inv_warehouse_inventory"."inv_item_sk" as "_monthly_item_id",
      "inv_warehouse_inventory"."inv_warehouse_sk" as "_monthly_w_id",
      avg("inv_warehouse_inventory"."inv_quantity_on_hand") as "_monthly_mn",
      stddev_samp("inv_warehouse_inventory"."inv_quantity_on_hand") as "_monthly_sd"
  FROM
      "inventory" as "inv_warehouse_inventory"
      INNER JOIN "date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
  WHERE
      "inv_date_date"."D_YEAR" = 2001

  GROUP BY
      1,
      2,
      3),
  questionable as (
  SELECT
      "wakeful"."_monthly_item_id" as "monthly_item_id",
      "wakeful"."_monthly_mn" as "monthly_mn",
      "wakeful"."_monthly_mo" as "monthly_mo",
      "wakeful"."_monthly_sd" as "monthly_sd",
      "wakeful"."_monthly_w_id" as "monthly_w_id"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN INVALID_REFERENCE_BUG = 0 THEN null
  	ELSE stddev_samp(INVALID_REFERENCE_BUG) / INVALID_REFERENCE_BUG
  	END > 1 and "wakeful"."_monthly_mo" = 2
  ),
  thoughtful as (
  SELECT
      "wakeful"."_monthly_item_id" as "monthly_item_id",
      "wakeful"."_monthly_mn" as "monthly_mn",
      "wakeful"."_monthly_mo" as "monthly_mo",
      "wakeful"."_monthly_sd" as "monthly_sd",
      "wakeful"."_monthly_w_id" as "monthly_w_id"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN INVALID_REFERENCE_BUG = 0 THEN null
  	ELSE stddev_samp(INVALID_REFERENCE_BUG) / INVALID_REFERENCE_BUG
  	END > 1 and "wakeful"."_monthly_mo" = 1
  ),
  abundant as (
  SELECT
      "questionable"."monthly_item_id" as "feb_item_id",
      "questionable"."monthly_mn" as "feb_mn",
      "questionable"."monthly_mo" as "feb_mo",
      "questionable"."monthly_w_id" as "feb_w_id",
      CASE
  	WHEN "questionable"."monthly_mn" = 0 THEN null
  	ELSE "questionable"."monthly_sd" / "questionable"."monthly_mn"
  	END as "feb_cv"
  FROM
      "questionable"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  cooperative as (
  SELECT
      "thoughtful"."monthly_item_id" as "jan_item_id",
      "thoughtful"."monthly_mn" as "jan_mn",
      "thoughtful"."monthly_mo" as "jan_mo",
      "thoughtful"."monthly_w_id" as "jan_w_id",
      CASE
  	WHEN "thoughtful"."monthly_mn" = 0 THEN null
  	ELSE "thoughtful"."monthly_sd" / "thoughtful"."monthly_mn"
  	END as "jan_cv"
  FROM
      "thoughtful"
  GROUP BY
      1,
      2,
      3,
      4,
      5)
  SELECT
      "cooperative"."jan_w_id" as "jan_w_id",
      "cooperative"."jan_item_id" as "jan_item_id",
      "cooperative"."jan_mo" as "jan_mo",
      "cooperative"."jan_mn" as "jan_mn",
      "cooperative"."jan_cv" as "jan_cv",
      "abundant"."feb_w_id" as "feb_w_id",
      "abundant"."feb_item_id" as "feb_item_id",
      "abundant"."feb_mo" as "feb_mo",
      "abundant"."feb_mn" as "feb_mn",
      "abundant"."feb_cv" as "feb_cv"
  FROM
      "abundant"
      INNER JOIN "cooperative" on "abundant"."feb_item_id" = "cooperative"."jan_item_id" AND "abundant"."feb_w_id" = "cooperative"."jan_w_id"
  ORDER BY
      "abundant"."feb_w_id" asc nulls first,
      "abundant"."feb_item_id" asc nulls first,
      "jan_mo" asc nulls first,
      "jan_mn" asc nulls first,
      "jan_cv" asc nulls first,
      "feb_mo" asc nulls first,
      "feb_mn" asc nulls first,
      "feb_cv" asc nulls first, this should never occur. Please create an issue to report this.
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: (_duckdb.BinderException) Binder Error: WHERE clause cannot contain aggregates!

  LINE 29: 	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
                ^
  [SQL:
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
  thoughtful as (
  SELECT
      "wakeful"."inv_date_month_of_year" as "jan_mo",
      "wakeful"."inv_item_id" as "jan_item_id",
      "wakeful"."inv_warehouse_id" as "jan_w_id",
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END as "jan_cv",
      avg("wakeful"."inv_quantity_on_hand") as "jan_mn"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END > 1 and "wakeful"."inv_date_month_of_year" = 1

  GROUP BY
      1,
      2,
      3),
  cheerful as (
  SELECT
      "wakeful"."inv_date_month_of_year" as "feb_mo",
      "wakeful"."inv_item_id" as "feb_item_id",
      "wakeful"."inv_warehouse_id" as "feb_w_id",
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END as "feb_cv",
      avg("wakeful"."inv_quantity_on_hand") as "feb_mn"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END > 1 and "wakeful"."inv_date_month_of_year" = 2

  GROUP BY
      1,
      2,
      3)
  SELECT
      "thoughtful"."jan_w_id" as "jan_w_id",
      "thoughtful"."jan_item_id" as "jan_item_id",
      "thoughtful"."jan_mo" as "jan_mo",
      "thoughtful"."jan_mn" as "jan_mn",
      "thoughtful"."jan_cv" as "jan_cv",
      "cheerful"."feb_w_id" as "feb_w_id",
      "cheerful"."feb_item_id" as "feb_item_id",
      "cheerful"."feb_mo" as "feb_mo",
      "cheerful"."feb_mn" as "feb_mn",
      "cheerful"."feb_cv" as "feb_cv"
  FROM
      "thoughtful"
      INNER JOIN "cheerful" on "thoughtful"."jan_item_id" = "cheerful"."feb_item_id" AND "thoughtful"."jan_w_id" = "cheerful"."feb_w_id"
  ORDER BY
      "cheerful"."feb_w_id" asc nulls first,
      "cheerful"."feb_item_id" asc nulls first,
      "jan_mo" asc nulls first,
      "jan_mn" asc nulls first,
      "jan_cv" asc nulls first,
      "feb_mo" asc nulls first,
      "feb_mn" asc nulls first,
      "feb_cv" asc nulls first]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Invalid reference string found in query:
  WITH
  wakeful as (
  SELECT
      "inv_date_date"."D_MOY" as "_qualifying_mo",
      "inv_warehouse_inventory"."inv_item_sk" as "_qualifying_item_id",
      "inv_warehouse_inventory"."inv_warehouse_sk" as "_qualifying_w_id",
      CASE
  	WHEN avg("inv_warehouse_inventory"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("inv_warehouse_inventory"."inv_quantity_on_hand") / avg("inv_warehouse_inventory"."inv_quantity_on_hand")
  	END as "_qualifying_cv",
      avg("inv_warehouse_inventory"."inv_quantity_on_hand") as "_qualifying_mn"
  FROM
      "inventory" as "inv_warehouse_inventory"
      INNER JOIN "date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
  WHERE
      "inv_date_date"."D_YEAR" = 2001

  GROUP BY
      1,
      2,
      3),
  questionable as (
  SELECT
      "wakeful"."_qualifying_cv" as "qualifying_cv",
      "wakeful"."_qualifying_item_id" as "qualifying_item_id",
      "wakeful"."_qualifying_mn" as "qualifying_mn",
      "wakeful"."_qualifying_mo" as "qualifying_mo",
      "wakeful"."_qualifying_w_id" as "qualifying_w_id"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN "wakeful"."_qualifying_mn" = 0 THEN null
  	ELSE stddev_samp(INVALID_REFERENCE_BUG) / "wakeful"."_qualifying_mn"
  	END > 1 and "wakeful"."_qualifying_mo" = 2
  ),
  thoughtful as (
  SELECT
      "wakeful"."_qualifying_cv" as "qualifying_cv",
      "wakeful"."_qualifying_item_id" as "qualifying_item_id",
      "wakeful"."_qualifying_mn" as "qualifying_mn",
      "wakeful"."_qualifying_mo" as "qualifying_mo",
      "wakeful"."_qualifying_w_id" as "qualifying_w_id"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN "wakeful"."_qualifying_mn" = 0 THEN null
  	ELSE stddev_samp(INVALID_REFERENCE_BUG) / "wakeful"."_qualifying_mn"
  	END > 1 and "wakeful"."_qualifying_mo" = 1
  ),
  abundant as (
  SELECT
      "questionable"."qualifying_cv" as "feb_cv",
      "questionable"."qualifying_item_id" as "feb_item_id",
      "questionable"."qualifying_mn" as "feb_mn",
      "questionable"."qualifying_mo" as "feb_mo",
      "questionable"."qualifying_w_id" as "feb_w_id"
  FROM
      "questionable"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  cooperative as (
  SELECT
      "thoughtful"."qualifying_cv" as "jan_cv",
      "thoughtful"."qualifying_item_id" as "jan_item_id",
      "thoughtful"."qualifying_mn" as "jan_mn",
      "thoughtful"."qualifying_mo" as "jan_mo",
      "thoughtful"."qualifying_w_id" as "jan_w_id"
  FROM
      "thoughtful"
  GROUP BY
      1,
      2,
      3,
      4,
      5)
  SELECT
      "cooperative"."jan_w_id" as "jan_w_id",
      "cooperative"."jan_item_id" as "jan_item_id",
      "cooperative"."jan_mo" as "jan_mo",
      "cooperative"."jan_mn" as "jan_mn",
      "cooperative"."jan_cv" as "jan_cv",
      "abundant"."feb_w_id" as "feb_w_id",
      "abundant"."feb_item_id" as "feb_item_id",
      "abundant"."feb_mo" as "feb_mo",
      "abundant"."feb_mn" as "feb_mn",
      "abundant"."feb_cv" as "feb_cv"
  FROM
      "abundant"
      INNER JOIN "cooperative" on "abundant"."feb_item_id" = "cooperative"."jan_item_id" AND "abundant"."feb_w_id" = "cooperative"."jan_w_id"
  ORDER BY
      "abundant"."feb_w_id" asc nulls first,
      "abundant"."feb_item_id" asc nulls first,
      "jan_mo" asc nulls first,
      "jan_mn" asc nulls first,
      "jan_cv" asc nulls first,
      "feb_mo" asc nulls first,
      "feb_mn" asc nulls first,
      "feb_cv" asc nulls first, this should never occur. Please create an issue to report this.
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Invalid reference string found in query:
  WITH
  wakeful as (
  SELECT
      "inv_date_date"."D_MOY" as "_qualifying_mo",
      "inv_warehouse_inventory"."inv_item_sk" as "_qualifying_item_id",
      "inv_warehouse_inventory"."inv_warehouse_sk" as "_qualifying_w_id",
      avg("inv_warehouse_inventory"."inv_quantity_on_hand") as "_qualifying_mn",
      stddev_samp("inv_warehouse_inventory"."inv_quantity_on_hand") as "_qualifying_sd"
  FROM
      "inventory" as "inv_warehouse_inventory"
      INNER JOIN "date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
  WHERE
      "inv_date_date"."D_YEAR" = 2001

  GROUP BY
      1,
      2,
      3),
  questionable as (
  SELECT
      "wakeful"."_qualifying_item_id" as "qualifying_item_id",
      "wakeful"."_qualifying_mn" as "qualifying_mn",
      "wakeful"."_qualifying_mo" as "qualifying_mo",
      "wakeful"."_qualifying_sd" as "qualifying_sd",
      "wakeful"."_qualifying_w_id" as "qualifying_w_id"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN INVALID_REFERENCE_BUG = 0 THEN null
  	ELSE stddev_samp(INVALID_REFERENCE_BUG) / INVALID_REFERENCE_BUG
  	END > 1 and "wakeful"."_qualifying_mo" = 1
  ),
  thoughtful as (
  SELECT
      "wakeful"."_qualifying_item_id" as "qualifying_item_id",
      "wakeful"."_qualifying_mn" as "qualifying_mn",
      "wakeful"."_qualifying_mo" as "qualifying_mo",
      "wakeful"."_qualifying_sd" as "qualifying_sd",
      "wakeful"."_qualifying_w_id" as "qualifying_w_id"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN INVALID_REFERENCE_BUG = 0 THEN null
  	ELSE stddev_samp(INVALID_REFERENCE_BUG) / INVALID_REFERENCE_BUG
  	END > 1 and "wakeful"."_qualifying_mo" = 2
  ),
  abundant as (
  SELECT
      "questionable"."qualifying_item_id" as "jan_item_id",
      "questionable"."qualifying_mn" as "jan_mn",
      "questionable"."qualifying_mo" as "jan_mo",
      "questionable"."qualifying_w_id" as "jan_w_id",
      CASE
  	WHEN "questionable"."qualifying_mn" = 0 THEN null
  	ELSE "questionable"."qualifying_sd" / "questionable"."qualifying_mn"
  	END as "jan_cv"
  FROM
      "questionable"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  cooperative as (
  SELECT
      "thoughtful"."qualifying_item_id" as "feb_item_id",
      "thoughtful"."qualifying_mn" as "feb_mn",
      "thoughtful"."qualifying_mo" as "feb_mo",
      "thoughtful"."qualifying_w_id" as "feb_w_id",
      CASE
  	WHEN "thoughtful"."qualifying_mn" = 0 THEN null
  	ELSE "thoughtful"."qualifying_sd" / "thoughtful"."qualifying_mn"
  	END as "feb_cv"
  FROM
      "thoughtful"
  GROUP BY
      1,
      2,
      3,
      4,
      5)
  SELECT
      "abundant"."jan_w_id" as "jan_w_id",
      "abundant"."jan_item_id" as "jan_item_id",
      "abundant"."jan_mo" as "jan_mo",
      "abundant"."jan_mn" as "jan_mn",
      "abundant"."jan_cv" as "jan_cv",
      "cooperative"."feb_w_id" as "feb_w_id",
      "cooperative"."feb_item_id" as "feb_item_id",
      "cooperative"."feb_mo" as "feb_mo",
      "cooperative"."feb_mn" as "feb_mn",
      "cooperative"."feb_cv" as "feb_cv"
  FROM
      "abundant"
      INNER JOIN "cooperative" on "abundant"."jan_item_id" = "cooperative"."feb_item_id" AND "abundant"."jan_w_id" = "cooperative"."feb_w_id"
  ORDER BY
      "cooperative"."feb_w_id" asc nulls first,
      "cooperative"."feb_item_id" asc nulls first,
      "jan_mo" asc nulls first,
      "jan_mn" asc nulls first,
      "jan_cv" asc nulls first,
      "feb_mo" asc nulls first,
      "feb_mn" asc nulls first,
      "feb_cv" asc nulls first, this should never occur. Please create an issue to report this.
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Invalid reference string found in query:
  WITH
  wakeful as (
  SELECT
      "inv_date_date"."D_MOY" as "_qualifying_mo",
      "inv_warehouse_inventory"."inv_item_sk" as "_qualifying_item_id",
      "inv_warehouse_inventory"."inv_warehouse_sk" as "_qualifying_w_id",
      avg("inv_warehouse_inventory"."inv_quantity_on_hand") as "_qualifying_mn",
      stddev_samp("inv_warehouse_inventory"."inv_quantity_on_hand") as "_qualifying_sd"
  FROM
      "inventory" as "inv_warehouse_inventory"
      INNER JOIN "date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
  WHERE
      "inv_date_date"."D_YEAR" = 2001

  GROUP BY
      1,
      2,
      3),
  questionable as (
  SELECT
      "wakeful"."_qualifying_item_id" as "qualifying_item_id",
      "wakeful"."_qualifying_mn" as "qualifying_mn",
      "wakeful"."_qualifying_mo" as "qualifying_mo",
      "wakeful"."_qualifying_sd" as "qualifying_sd",
      "wakeful"."_qualifying_w_id" as "qualifying_w_id"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN INVALID_REFERENCE_BUG = 0 THEN null
  	ELSE stddev_samp(INVALID_REFERENCE_BUG) / INVALID_REFERENCE_BUG
  	END > 1 and "wakeful"."_qualifying_mo" = 1
  ),
  thoughtful as (
  SELECT
      "wakeful"."_qualifying_item_id" as "qualifying_item_id",
      "wakeful"."_qualifying_mn" as "qualifying_mn",
      "wakeful"."_qualifying_mo" as "qualifying_mo",
      "wakeful"."_qualifying_sd" as "qualifying_sd",
      "wakeful"."_qualifying_w_id" as "qualifying_w_id"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN INVALID_REFERENCE_BUG = 0 THEN null
  	ELSE stddev_samp(INVALID_REFERENCE_BUG) / INVALID_REFERENCE_BUG
  	END > 1 and "wakeful"."_qualifying_mo" = 2
  ),
  abundant as (
  SELECT
      "questionable"."qualifying_item_id" as "jan_item_id",
      "questionable"."qualifying_mn" as "jan_mn",
      "questionable"."qualifying_mo" as "jan_mo",
      "questionable"."qualifying_w_id" as "jan_w_id",
      CASE
  	WHEN "questionable"."qualifying_mn" = 0 THEN null
  	ELSE "questionable"."qualifying_sd" / "questionable"."qualifying_mn"
  	END as "jan_cv"
  FROM
      "questionable"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      "questionable"."qualifying_sd"),
  cooperative as (
  SELECT
      "thoughtful"."qualifying_item_id" as "feb_item_id",
      "thoughtful"."qualifying_mn" as "feb_mn",
      "thoughtful"."qualifying_mo" as "feb_mo",
      "thoughtful"."qualifying_w_id" as "feb_w_id",
      CASE
  	WHEN "thoughtful"."qualifying_mn" = 0 THEN null
  	ELSE "thoughtful"."qualifying_sd" / "thoughtful"."qualifying_mn"
  	END as "feb_cv"
  FROM
      "thoughtful"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      "thoughtful"."qualifying_sd")
  SELECT
      "abundant"."jan_w_id" as "jan_w_id",
      "abundant"."jan_item_id" as "jan_item_id",
      "abundant"."jan_mo" as "jan_mo",
      "abundant"."jan_mn" as "jan_mn",
      "abundant"."jan_cv" as "jan_cv",
      "cooperative"."feb_w_id" as "feb_w_id",
      "cooperative"."feb_item_id" as "feb_item_id",
      "cooperative"."feb_mo" as "feb_mo",
      "cooperative"."feb_mn" as "feb_mn",
      "cooperative"."feb_cv" as "feb_cv"
  FROM
      "abundant"
      INNER JOIN "cooperative" on "abundant"."jan_item_id" = "cooperative"."feb_item_id" AND "abundant"."jan_w_id" = "cooperative"."feb_w_id"
  ORDER BY
      "cooperative"."feb_w_id" asc nulls first,
      "cooperative"."feb_item_id" asc nulls first,
      "jan_mo" asc nulls first,
      "jan_mn" asc nulls first,
      "jan_cv" asc nulls first,
      "feb_mo" asc nulls first,
      "feb_mn" asc nulls first,
      "feb_cv" asc nulls first, this should never occur. Please create an issue to report this.
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: (_duckdb.BinderException) Binder Error: WHERE clause cannot contain aggregates!

  LINE 29: 	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
                ^
  [SQL:
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
  thoughtful as (
  SELECT
      "wakeful"."inv_date_month_of_year" as "jan_data_mo",
      "wakeful"."inv_item_id" as "jan_data_item_id",
      "wakeful"."inv_warehouse_id" as "jan_data_w_id",
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END as "jan_data_cv",
      avg("wakeful"."inv_quantity_on_hand") as "jan_data_mn"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END > 1 and "wakeful"."inv_date_month_of_year" = 1

  GROUP BY
      1,
      2,
      3),
  cheerful as (
  SELECT
      "wakeful"."inv_date_month_of_year" as "feb_data_mo",
      "wakeful"."inv_item_id" as "feb_data_item_id",
      "wakeful"."inv_warehouse_id" as "feb_data_w_id",
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END as "feb_data_cv",
      avg("wakeful"."inv_quantity_on_hand") as "feb_data_mn"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN avg("wakeful"."inv_quantity_on_hand") = 0 THEN null
  	ELSE stddev_samp("wakeful"."inv_quantity_on_hand") / avg("wakeful"."inv_quantity_on_hand")
  	END > 1 and "wakeful"."inv_date_month_of_year" = 2

  GROUP BY
      1,
      2,
      3)
  SELECT
      "thoughtful"."jan_data_w_id" as "jan_data_w_id",
      "thoughtful"."jan_data_item_id" as "jan_data_item_id",
      "thoughtful"."jan_data_mo" as "jan_mo",
      "thoughtful"."jan_data_mn" as "jan_mn",
      "thoughtful"."jan_data_cv" as "jan_cv",
      "cheerful"."feb_data_w_id" as "feb_w_id",
      "cheerful"."feb_data_item_id" as "feb_item_id",
      "cheerful"."feb_data_mo" as "feb_mo",
      "cheerful"."feb_data_mn" as "feb_mn",
      "cheerful"."feb_data_cv" as "feb_cv"
  FROM
      "thoughtful"
      INNER JOIN "cheerful" on "thoughtful"."jan_data_item_id" = "cheerful"."feb_data_item_id" AND "thoughtful"."jan_data_w_id" = "cheerful"."feb_data_w_id"
  ORDER BY
      "cheerful"."feb_data_w_id" asc nulls first,
      "cheerful"."feb_data_item_id" asc nulls first,
      "jan_mo" asc nulls first,
      "jan_mn" asc nulls first,
      "jan_cv" asc nulls first,
      "feb_mo" asc nulls first,
      "feb_mn" asc nulls first,
      "feb_cv" asc nulls first]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Invalid reference string found in query:
  WITH
  wakeful as (
  SELECT
      "inv_date_date"."D_MOY" as "_monthly_mo",
      "inv_warehouse_inventory"."inv_item_sk" as "_monthly_item_id",
      "inv_warehouse_inventory"."inv_warehouse_sk" as "_monthly_w_id",
      avg("inv_warehouse_inventory"."inv_quantity_on_hand") as "_monthly_mn",
      stddev_samp("inv_warehouse_inventory"."inv_quantity_on_hand") as "_monthly_sd"
  FROM
      "inventory" as "inv_warehouse_inventory"
      INNER JOIN "date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
  WHERE
      "inv_date_date"."D_YEAR" = 2001

  GROUP BY
      1,
      2,
      3),
  questionable as (
  SELECT
      "wakeful"."_monthly_item_id" as "monthly_item_id",
      "wakeful"."_monthly_mn" as "monthly_mn",
      "wakeful"."_monthly_mo" as "monthly_mo",
      "wakeful"."_monthly_sd" as "monthly_sd",
      "wakeful"."_monthly_w_id" as "monthly_w_id"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN INVALID_REFERENCE_BUG = 0 THEN null
  	ELSE stddev_samp(INVALID_REFERENCE_BUG) / INVALID_REFERENCE_BUG
  	END > 1 and "wakeful"."_monthly_mo" = 2
  ),
  thoughtful as (
  SELECT
      "wakeful"."_monthly_item_id" as "monthly_item_id",
      "wakeful"."_monthly_mn" as "monthly_mn",
      "wakeful"."_monthly_mo" as "monthly_mo",
      "wakeful"."_monthly_sd" as "monthly_sd",
      "wakeful"."_monthly_w_id" as "monthly_w_id"
  FROM
      "wakeful"
  WHERE
      CASE
  	WHEN INVALID_REFERENCE_BUG = 0 THEN null
  	ELSE stddev_samp(INVALID_REFERENCE_BUG) / INVALID_REFERENCE_BUG
  	END > 1 and "wakeful"."_monthly_mo" = 1
  ),
  abundant as (
  SELECT
      "questionable"."monthly_item_id" as "feb_item_id",
      "questionable"."monthly_mn" as "feb_mn",
      "questionable"."monthly_mo" as "feb_mo",
      "questionable"."monthly_w_id" as "feb_w_id",
      CASE
  	WHEN "questionable"."monthly_mn" = 0 THEN null
  	ELSE "questionable"."monthly_sd" / "questionable"."monthly_mn"
  	END as "feb_cv"
  FROM
      "questionable"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  cooperative as (
  SELECT
      "thoughtful"."monthly_item_id" as "jan_item_id",
      "thoughtful"."monthly_mn" as "jan_mn",
      "thoughtful"."monthly_mo" as "jan_mo",
      "thoughtful"."monthly_w_id" as "jan_w_id",
      CASE
  	WHEN "thoughtful"."monthly_mn" = 0 THEN null
  	ELSE "thoughtful"."monthly_sd" / "thoughtful"."monthly_mn"
  	END as "jan_cv"
  FROM
      "thoughtful"
  GROUP BY
      1,
      2,
      3,
      4,
      5)
  SELECT
      "cooperative"."jan_w_id" as "jan_w_id",
      "cooperative"."jan_item_id" as "jan_item_id",
      "cooperative"."jan_mo" as "jan_mo",
      "cooperative"."jan_mn" as "jan_mn",
      "cooperative"."jan_cv" as "jan_cv",
      "abundant"."feb_w_id" as "feb_w_id",
      "abundant"."feb_item_id" as "feb_item_id",
      "abundant"."feb_mo" as "feb_mo",
      "abundant"."feb_mn" as "feb_mn",
      "abundant"."feb_cv" as "feb_cv"
  FROM
      "abundant"
      INNER JOIN "cooperative" on "abundant"."feb_item_id" = "cooperative"."jan_item_id" AND "abundant"."feb_w_id" = "cooperative"."jan_w_id"
  ORDER BY
      "abundant"."feb_w_id" asc nulls first,
      "abundant"."feb_item_id" asc nulls first,
      "jan_mo" asc nulls first,
      "jan_mn" asc nulls first,
      "jan_cv" asc nulls first,
      "feb_mo" asc nulls first,
      "feb_mn" asc nulls first,
      "feb_cv" asc nulls first, this should never occur. Please create an issue to report this.
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: HAVING references 'local.threshold', which is not in the SELECT projection (line 11). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.threshold
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query45.preql`

  ```text
  Syntax error in query45.preql: Undefined concept: item.id. Suggestions: ['ws.item.id', 'ws.date.id', 'ws.ship_date.id', 'ws.return_date.id', 'ws.time.id', 'ws.return_time.id']
  ```
- `trilogy run query46.preql`

  ```text
  Syntax error in query46.preql: 7 undefined concept references; fix all before re-running:
    - date.day_of_week (line 3, col 7, in WHERE); did you mean: store_sales.date.day_of_week, store_sales.store.date.day_of_week, store_sales.return_store.date.day_of_week, store_sales.return_date.day_of_week, store_sales.customer.first_sales_date.day_of_week, store_sales.customer.first_shipto_date.day_of_week?
    - date.year (line 4, col 7, in WHERE); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, store_sales.return_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.first_shipto_date.year?
    - store.city (line 5, col 7, in WHERE); did you mean: store_sales.store.city, store_sales.customer.address.city, store_sales.return_customer.address.city, store_sales.return_store.city, store_sales.sale_address.city, customer.address.city?
    - household_demographic.dependent_count (line 6, col 8, in WHERE); did you mean: store_sales.customer.household_demographic.dependent_count, store_sales.return_customer.household_demographic.dependent_count, store_sales.household_demographic.dependent_count, store_sales.customer.demographics.dependent_count, store_sales.return_customer.demographics.dependent_count, store_sales.customer_demographic.dependent_count?
    - household_demographic.vehicle_count (line 6, col 53, in WHERE); did you mean: store_sales.customer.household_demographic.vehicle_count, store_sales.return_customer.household_demographic.vehicle_count, store_sales.household_demographic.vehicle_count, household_demographic.dependent_count?
    - customer.address.city (line 7, col 7, in WHERE); did you mean: store_sales.customer.address.city, store_sales.return_customer.address.city, store_sales.store.city, store_sales.return_store.city, store_sales.sale_address.city, store.city?
    - sale_address.city (line 7, col 32, in WHERE); did you mean: store_sales.sale_address.city, store_sales.customer.address.city, store_sales.return_customer.address.city, store_sales.store.city, store_sales.return_store.city, store.city?
  ```
- `trilogy file read query48.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query50.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 69 column 12 (char 2891). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query51.preql`

  ```text
  Syntax error in query51.preql: 2 undefined concept references; fix all before re-running:
    - web_running.item_id (line 19, col 5, in SELECT); did you mean: web_running.web_daily.item_id, web_daily.item_id, web_running.sale_date?
    - web_running.sale_date (line 20, col 5, in SELECT); did you mean: web_running.web_daily.sale_date, web_daily.sale_date, web_running.web_daily.item_id?
  ```
- `trilogy run query51.preql`

  ```text
  Resolution error in query51.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {local.item_id, local.sale_date, store_rt.running_total, web_rt.running_total}
  ```
- `trilogy file read query51.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query53.preql`

  ```text
  Resolution error in query53.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.year, local.quarter}; {item.brand_name, item.category, item.class, local.manufacturer_id}; {local.total_unit_price, ss.store.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query53.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query56.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query57.preql`

  ```text
  Syntax error in query57.preql: Output column 'avg_monthly_sales' aliases 'local.avg_monthly_sales', which is itself the 'avg_monthly_sales' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'avg_monthly_sales_out').
  ```
- `trilogy run query59.preql duckdb`

  ```text
  Syntax error in query59.preql: Undefined concept: date.day_of_week. Suggestions: ['ps.date.day_of_week', 'ps.store.date.day_of_week', 'ps.return_store.date.day_of_week', 'ps.return_date.day_of_week', 'ps.customer.first_sales_date.day_of_week', 'ps.customer.first_shipto_date.day_of_week']
  ```
- `trilogy run query59.preql duckdb`

  ```text
  Resolution error in query59.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {next_year.fri_total, next_year.mon_total, next_year.sat_total, next_year.store_code, next_year.store_name, next_year.sun_total, next_year.thu_total, next_year.tue_total, next_year.wed_total, next_year.ws, this_year.fri_total, this_year.mon_total, this_year.sat_total, this_year.store_code, this_year.store_name, this_year.sun_total, this_year.thu_total, this_year.tue_total, this_year.wed_total, this_year.ws}
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query60.preql`

  ```text
  Unexpected error in query60.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy run query60.preql`

  ```text
  Syntax error in query60.preql: Undefined concept: item.category. Suggestions: ['all_sales.item.category', 'all_sales.item.category_id']
  ```
- `trilogy file read query62.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cat_sales.item.id, local.cat_list_by_item, local.cat_refund_by_item}
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: qualifying_cat_items.item_id (line 29, col 33, in WHERE). Suggestions: ['qualifying_cat_items.cat_sales_by_item.item_id', 'cat_sales_by_item.item_id', 'cat_returns_by_item.item_id', 'item_id', 'qualifying_cat_items.cat_returns_by_item.total_cat_refund', 'qualifying_cat_items.cat_sales_by_item.total_cat_list_price']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: 3 undefined concept references; fix all before re-running:
    - qualifying_cat_items.item_id (line 28, col 5, in SELECT); did you mean: qualifying_cat_items.cat_sales_by_item.item_id, cat_sales_by_item.item_id, cat_returns_by_item.item_id, qualifying_cat_items.total_cat_refund, qualifying_cat_items.total_cat_list_price?
    - qualifying_cat_items.total_cat_list_price (line 29, col 5, in SELECT); did you mean: qualifying_cat_items.cat_sales_by_item.total_cat_list_price, cat_sales_by_item.total_cat_list_price, qualifying_cat_items.total_cat_refund?
    - qualifying_cat_items.total_cat_refund (line 30, col 5, in SELECT); did you mean: qualifying_cat_items.cat_returns_by_item.total_cat_refund, cat_returns_by_item.total_cat_refund, qualifying_cat_items.total_cat_list_price, qualifying_cat_items.item_id?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cat_returns_by_item.total_cat_refund}; {cat_sales_by_item.item_id, cat_sales_by_item.total_cat_list_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cat_sales.item.text_id, local._cat_joined_total_cat_list_price}; {local._cat_joined_total_cat_refund}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'cat_returns.sales.ext_list_price', 'cat_returns.refunded_cash', 'cat_returns.reversed_charge', 'cat_returns.store_credit', which are not in the SELECT projection (line 6). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --cat_returns.sales.ext_list_price, --cat_returns.refunded_cash, --cat_returns.reversed_charge, --cat_returns.store_credit
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'cat_returns.sales.ext_list_price', 'cat_returns.refunded_cash', 'cat_returns.reversed_charge', 'cat_returns.store_credit', which are not in the SELECT projection (line 6). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --cat_returns.sales.ext_list_price, --cat_returns.refunded_cash, --cat_returns.reversed_charge, --cat_returns.store_credit
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'cat_returns.sales.ext_list_price', 'cat_returns.refunded_cash', 'cat_returns.reversed_charge', 'cat_returns.store_credit', which are not in the SELECT projection (line 6). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --cat_returns.sales.ext_list_price, --cat_returns.refunded_cash, --cat_returns.reversed_charge, --cat_returns.store_credit
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'store_sales.store.id', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.store.id order by store_sales.store.id asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query66.preql`

  ```text
  Unexpected error in query66.preql: Recursion error building concept local.monthly_sales with grain Grain<all_sales.channel,all_sales.item.id,all_sales.order_id> and lineage case(WHEN ref:all_sales.channel = WEB THEN sum(multiply(ref:all_sales.quantity,ref:all_sales.ext_sales_price))<abstract>,ELSE sum(multiply(ref:all_sales.quantity,ref:all_sales.sales_price))<abstract>). This is likely due to a circular reference.
  ```
- `trilogy file read query69.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query70.preql`

  ```text
  Resolution error in query70.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {local.g_county, local.g_state, store.county, store.state}; still unresolved: {local.total_net_profit}
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: 3 undefined concept references; fix all before re-running:
    - rolled.state (line 21, col 5, in SELECT); did you mean: rolled.sale.store.state, sale.customer.address.state, sale.return_customer.address.state, sale.store.state, sale.return_store.state, sale.sale_address.state?
    - rolled.county (line 22, col 5, in SELECT); did you mean: rolled.sale.store.county, sale.customer.address.county, sale.return_customer.address.county, sale.store.county, sale.return_store.county, sale.sale_address.county?
    - rolled.state (line 26, col 34, in ORDER BY); did you mean: rolled.sale.store.state, sale.customer.address.state, sale.return_customer.address.state, sale.store.state, sale.return_store.state, sale.sale_address.state?
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "sale_store_sales"."SS_SOLD_DATE_SK" as "sale_date_id",
      "sale_store_sales"."SS_STORE_SK" as "sale_store_id"
  FROM
      "store_sales" as "sale_store_sales"
  GROUP BY
      1,
      2),
  questionable as (
  SELECT
      "sale_store_store"."S_STATE" as "sale_store_state",
      sum("sale_store_sales"."SS_NET_PROFIT") as "_virt_agg_sum_807075557008438"
  FROM
      "store_sales" as "sale_store_sales"
      LEFT OUTER JOIN "store" as "sale_store_store" on "sale_store_sales"."SS_STORE_SK" = "sale_store_store"."S_STORE_SK"
  GROUP BY
      1),
  cheerful as (
  SELECT
      "sale_store_sales"."SS_NET_PROFIT" as "sale_net_profit",
      "sale_store_store"."S_COUNTY" as "sale_store_county",
      "sale_store_store"."S_STATE" as "sale_store_state"
  FROM
      "store_sales" as "sale_store_sales"
      INNER JOIN "date_dim" as "sale_date_date" on "sale_store_sales"."SS_SOLD_DATE_SK" = "sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "sale_store_store" on "sale_store_sales"."SS_STORE_SK" = "sale_store_store"."S_STORE_SK"
  WHERE
      "sale_date_date"."D_YEAR" = 2000
  ),
  vacuous as (
  SELECT
      "sale_date_date"."D_YEAR" as "sale_date_year",
      "sale_store_store"."S_STATE" as "sale_store_state"
  FROM
      "thoughtful"
      LEFT OUTER JOIN "date_dim" as "sale_date_date" on "thoughtful"."sale_date_id" = "sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "sale_store_store" on "thoughtful"."sale_store_id" = "sale_store_store"."S_STORE_SK"
  GROUP BY
      1,
      2),
  cooperative as (
  SELECT
      "sale_date_date"."D_YEAR" as "sale_date_year",
      "sale_store_store"."S_STATE" as "sale_store_state",
      "thoughtful"."sale_store_id" as "sale_store_id"
  FROM
      "thoughtful"
      LEFT OUTER JOIN "date_dim" as "sale_date_date" on "thoughtful"."sale_date_id" = "sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "sale_store_store" on "thoughtful"."sale_store_id" = "sale_store_store"."S_STORE_SK"
  GROUP BY
      1,
      2,
      3),
  concerned as (
  SELECT
      "questionable"."_virt_agg_sum_807075557008438" as "_virt_agg_sum_807075557008438",
      "vacuous"."sale_date_year" as "sale_date_year",
      coalesce("questionable"."sale_store_state","vacuous"."sale_store_state") as "sale_store_state"
  FROM
      "vacuous"
      INNER JOIN "questionable" on "vacuous"."sale_store_state" is not distinct from "questionable"."sale_store_state"),
  uneven as (
  SELECT
      "cooperative"."sale_date_year" as "sale_date_year",
      "cooperative"."sale_store_id" as "sale_store_id",
      "questionable"."_virt_agg_sum_807075557008438" as "_virt_agg_sum_807075557008438",
      coalesce("cooperative"."sale_store_state","questionable"."sale_store_state") as "sale_store_state"
  FROM
      "cooperative"
      FULL JOIN "questionable" on "cooperative"."sale_store_state" is not distinct from "questionable"."sale_store_state"),
  young as (
  SELECT
      "concerned"."sale_store_state" as "sale_store_state",
      CASE WHEN "concerned"."sale_date_year" = 2000 and "concerned"."sale_store_state" is not null THEN "concerned"."_virt_agg_sum_807075557008438" ELSE NULL END as "state_profit"
  FROM
      "concerned"),
  yummy as (
  SELECT
      "uneven"."sale_store_id" as "sale_store_id",
      "uneven"."sale_store_state" as "sale_store_state",
      CASE WHEN "uneven"."sale_date_year" = 2000 and "uneven"."sale_store_state" is not null THEN "uneven"."_virt_agg_sum_807075557008438" ELSE NULL END as "state_profit"
  FROM
      "uneven"),
  sparkling as (
  SELECT
      "young"."sale_store_state" as "sale_store_state",
      "young"."state_profit" as "state_profit"
  FROM
      "young"
  GROUP BY
      1,
      2),
  juicy as (
  SELECT
      "yummy"."sale_store_state" as "sale_store_state",
      "yummy"."state_profit" as "state_profit"
  FROM
      "yummy"
  GROUP BY
      1,
      2,
      "yummy"."sale_store_id"),
  abhorrent as (
  SELECT
      "sparkling"."sale_store_state" as "sale_store_state",
      "sparkling"."state_profit" as "state_profit",
      rank() over (order by "sparkling"."state_profit" desc ) as "state_rank"
  FROM
      "sparkling"),
  sweltering as (
  SELECT
      "juicy"."sale_store_state" as "top_state"
  FROM
      "abhorrent"
      INNER JOIN "juicy" on "abhorrent"."sale_store_state" = "juicy"."sale_store_state" AND "abhorrent"."state_profit" = "juicy"."state_profit"
  WHERE
      "abhorrent"."state_rank" <= 5

  GROUP BY
      1),
  late as (
  SELECT
      "cheerful"."sale_store_county" as "rolled_county",
      "cheerful"."sale_store_state" as "rolled_state",
      CASE
  	WHEN grouping("cheerful"."sale_store_county") = 0 THEN "cheerful"."sale_store_state"
  	ELSE null
  	END as "parent",
      grouping("cheerful"."sale_store_state") + grouping("cheerful"."sale_store_county") as "level",
      sum("cheerful"."sale_net_profit") as "rolled_total_net_profit"
  FROM
      "cheerful"
  WHERE
      ( "cheerful"."sale_store_state" in (select sweltering."top_state" from sweltering where sweltering."top_state" is not null) and "cheerful"."sale_store_state" is not null ) or grouping("cheerful"."sale_store_state") + grouping("cheerful"."sale_store_county") = 2

  GROUP BY
      ROLLUP (2, 1)),
  macho as (
  SELECT
      "late"."level" as "level",
      "late"."rolled_county" as "rolled_county",
      "late"."rolled_state" as "rolled_state",
      "late"."rolled_total_net_profit" as "rolled_total_net_profit",
      rank() over (partition by "late"."level","late"."parent" order by "late"."rolled_total_net_profit" desc ) as "rnk"
  FROM
      "late")
  SELECT
      "macho"."rolled_state" as "rolled_state",
      "macho"."rolled_county" as "rolled_county",
      "macho"."rolled_total_net_profit" as "rolled_total_net_profit",
      "macho"."level" as "level",
      "macho"."rnk" as "rnk"
  FROM
      "macho"
  ORDER BY
      "macho"."level" desc nulls first,
      "macho"."rolled_state" asc nulls first,
      "macho"."rnk" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  cheerful as (
  SELECT
      "sale_store_store"."S_STATE" as "state_totals_state",
      sum("sale_store_sales"."SS_NET_PROFIT") as "state_totals_total_net_profit"
  FROM
      "store_sales" as "sale_store_sales"
      INNER JOIN "date_dim" as "sale_date_date" on "sale_store_sales"."SS_SOLD_DATE_SK" = "sale_date_date"."D_DATE_SK"
      INNER JOIN "store" as "sale_store_store" on "sale_store_sales"."SS_STORE_SK" = "sale_store_store"."S_STORE_SK"
  WHERE
      "sale_date_date"."D_YEAR" = 2000 and "sale_store_store"."S_STATE" is not null

  GROUP BY
      1),
  abundant as (
  SELECT
      "sale_store_sales"."SS_NET_PROFIT" as "sale_net_profit",
      "sale_store_store"."S_COUNTY" as "sale_store_county",
      "sale_store_store"."S_STATE" as "sale_store_state"
  FROM
      "store_sales" as "sale_store_sales"
      INNER JOIN "date_dim" as "sale_date_date" on "sale_store_sales"."SS_SOLD_DATE_SK" = "sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "sale_store_store" on "sale_store_sales"."SS_STORE_SK" = "sale_store_store"."S_STORE_SK"
  WHERE
      "sale_date_date"."D_YEAR" = 2000
  ),
  cooperative as (
  SELECT
      "cheerful"."state_totals_state" as "state_totals_state",
      rank() over (order by "cheerful"."state_totals_total_net_profit" desc ) as "state_rank"
  FROM
      "cheerful"),
  questionable as (
  SELECT
      "cooperative"."state_totals_state" as "top5_state"
  FROM
      "cooperative"
  WHERE
      "cooperative"."state_rank" <= 5
  ),
  uneven as (
  SELECT
      "abundant"."sale_store_county" as "rolled_county",
      "abundant"."sale_store_state" as "rolled_state",
      CASE
  	WHEN grouping("abundant"."sale_store_county") = 0 THEN "abundant"."sale_store_state"
  	ELSE null
  	END as "parent",
      grouping("abundant"."sale_store_state") + grouping("abundant"."sale_store_county") as "level",
      sum("abundant"."sale_net_profit") as "rolled_total_net_profit"
  FROM
      "abundant"
  WHERE
      "abundant"."sale_store_state" in (select questionable."top5_state" from questionable where questionable."top5_state" is not null) or grouping("abundant"."sale_store_state") + grouping("abundant"."sale_store_county") = 2

  GROUP BY
      ROLLUP (2, 1)),
  yummy as (
  SELECT
      "uneven"."level" as "level",
      "uneven"."rolled_county" as "rolled_county",
      "uneven"."rolled_state" as "rolled_state",
      "uneven"."rolled_total_net_profit" as "rolled_total_net_profit",
      rank() over (partition by "uneven"."level","uneven"."parent" order by "uneven"."rolled_total_net_profit" desc ) as "rnk"
  FROM
      "uneven")
  SELECT
      "yummy"."rolled_state" as "rolled_state",
      "yummy"."rolled_county" as "rolled_county",
      "yummy"."rolled_total_net_profit" as "rolled_total_net_profit",
      "yummy"."level" as "level",
      "yummy"."rnk" as "rnk"
  FROM
      "yummy"
  ORDER BY
      "yummy"."level" desc nulls first,
      "yummy"."rolled_state" asc nulls first,
      "yummy"."rnk" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: Undefined concept: ranked_states.state. Suggestions: ['ranked_states.state_totals.state', 'sale.customer.address.state', 'sale.return_customer.address.state', 'sale.store.state', 'sale.return_store.state', 'sale.sale_address.state']
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  questionable as (
  SELECT
      "sale_store_store"."S_STATE" as "sale_store_state",
      sum("sale_store_sales"."SS_NET_PROFIT") as "_virt_agg_sum_807075557008438"
  FROM
      "store_sales" as "sale_store_sales"
      LEFT OUTER JOIN "store" as "sale_store_store" on "sale_store_sales"."SS_STORE_SK" = "sale_store_store"."S_STORE_SK"
  GROUP BY
      1),
  thoughtful as (
  SELECT
      "sale_store_sales"."SS_SOLD_DATE_SK" as "sale_date_id",
      "sale_store_sales"."SS_STORE_SK" as "sale_store_id"
  FROM
      "store_sales" as "sale_store_sales"
  GROUP BY
      1,
      2),
  cheerful as (
  SELECT
      "sale_store_sales"."SS_NET_PROFIT" as "sale_net_profit",
      "sale_store_store"."S_COUNTY" as "sale_store_county",
      "sale_store_store"."S_STATE" as "sale_store_state"
  FROM
      "store_sales" as "sale_store_sales"
      INNER JOIN "date_dim" as "sale_date_date" on "sale_store_sales"."SS_SOLD_DATE_SK" = "sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "sale_store_store" on "sale_store_sales"."SS_STORE_SK" = "sale_store_store"."S_STORE_SK"
  WHERE
      "sale_date_date"."D_YEAR" = 2000
  ),
  uneven as (
  SELECT
      "questionable"."sale_store_state" as "sale_store_state",
      rank() over (order by "questionable"."_virt_agg_sum_807075557008438" desc ) as "_virt_window_rank_2073068781919543"
  FROM
      "questionable"),
  cooperative as (
  SELECT
      "sale_date_date"."D_YEAR" as "sale_date_year",
      "sale_store_store"."S_STATE" as "sale_store_state"
  FROM
      "thoughtful"
      INNER JOIN "date_dim" as "sale_date_date" on "thoughtful"."sale_date_id" = "sale_date_date"."D_DATE_SK"
      INNER JOIN "store" as "sale_store_store" on "thoughtful"."sale_store_id" = "sale_store_store"."S_STORE_SK"
  WHERE
      "sale_date_date"."D_YEAR" = 2000 and "sale_store_store"."S_STATE" is not null

  GROUP BY
      1,
      2,
      "thoughtful"."sale_store_id"),
  yummy as (
  SELECT
      CASE WHEN "cooperative"."sale_date_year" = 2000 and "cooperative"."sale_store_state" is not null and "uneven"."_virt_window_rank_2073068781919543" <= 5 THEN "cooperative"."sale_store_state" ELSE NULL END as "top5_states"
  FROM
      "cooperative"
      INNER JOIN "uneven" on "cooperative"."sale_store_state" = "uneven"."sale_store_state"
  WHERE
      "cooperative"."sale_store_state" is not null and "uneven"."_virt_window_rank_2073068781919543" <= 5

  GROUP BY
      1),
  concerned as (
  SELECT
      "cheerful"."sale_store_county" as "hierarchy_sale_store_county",
      "cheerful"."sale_store_state" as "hierarchy_sale_store_state",
      CASE
  	WHEN grouping("cheerful"."sale_store_county") = 0 THEN "cheerful"."sale_store_state"
  	ELSE null
  	END as "hierarchy_parent",
      grouping("cheerful"."sale_store_county") as "hierarchy_g_county",
      grouping("cheerful"."sale_store_state") as "hierarchy_g_state"
  FROM
      "cheerful"
  WHERE
      "cheerful"."sale_store_state" in (select yummy."top5_states" from yummy where yummy."top5_states" is not null) or ( grouping("cheerful"."sale_store_state") = 1 and grouping("cheerful"."sale_store_county") = 1 )

  GROUP BY
      ROLLUP (2, 1)),
  juicy as (
  SELECT
      "cheerful"."sale_store_county" as "hierarchy_sale_store_county",
      "cheerful"."sale_store_state" as "hierarchy_sale_store_state",
      CASE
  	WHEN grouping("cheerful"."sale_store_county") = 0 THEN "cheerful"."sale_store_state"
  	ELSE null
  	END as "hierarchy_parent",
      grouping("cheerful"."sale_store_state") + grouping("cheerful"."sale_store_county") as "_virt_func_add_4217210344466260",
      sum("cheerful"."sale_net_profit") as "hierarchy_total_net_profit"
  FROM
      "cheerful"
  WHERE
      "cheerful"."sale_store_state" in (select yummy."top5_states" from yummy where yummy."top5_states" is not null) or ( grouping("cheerful"."sale_store_state") = 1 and grouping("cheerful"."sale_store_county") = 1 )

  GROUP BY
      ROLLUP (2, 1)),
  vacuous as (
  SELECT
      "juicy"."hierarchy_parent" as "hierarchy_parent",
      "juicy"."hierarchy_sale_store_county" as "hierarchy_sale_store_county",
      "juicy"."hierarchy_sale_store_state" as "hierarchy_sale_store_state",
      "juicy"."hierarchy_total_net_profit" as "hierarchy_total_net_profit",
      rank() over (partition by "juicy"."_virt_func_add_4217210344466260","juicy"."hierarchy_parent" order by "juicy"."hierarchy_total_net_profit" desc ) as "rnk"
  FROM
      "juicy")
  SELECT
      "vacuous"."hierarchy_sale_store_state" as "state",
      "vacuous"."hierarchy_sale_store_county" as "county",
      "vacuous"."hierarchy_total_net_profit" as "hierarchy_total_net_profit",
      "concerned"."hierarchy_g_state" + "concerned"."hierarchy_g_county" as "level",
      "vacuous"."rnk" as "rnk"
  FROM
      "concerned"
      INNER JOIN "vacuous" on "concerned"."hierarchy_parent" = "vacuous"."hierarchy_parent" AND "concerned"."hierarchy_sale_store_county" = "vacuous"."hierarchy_sale_store_county" AND "concerned"."hierarchy_sale_store_state" = "vacuous"."hierarchy_sale_store_state"
  ORDER BY
      "level" desc nulls first,
      "state" asc nulls first,
      "vacuous"."rnk" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: Concept '_virt_filter_state_800974178045047' not found in environment.
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "sale_store_sales"."SS_SOLD_DATE_SK" as "sale_date_id",
      "sale_store_sales"."SS_STORE_SK" as "sale_store_id"
  FROM
      "store_sales" as "sale_store_sales"
  GROUP BY
      1,
      2),
  questionable as (
  SELECT
      "sale_store_store"."S_STATE" as "sale_store_state",
      sum("sale_store_sales"."SS_NET_PROFIT") as "_virt_agg_sum_807075557008438"
  FROM
      "store_sales" as "sale_store_sales"
      LEFT OUTER JOIN "store" as "sale_store_store" on "sale_store_sales"."SS_STORE_SK" = "sale_store_store"."S_STORE_SK"
  GROUP BY
      1),
  cheerful as (
  SELECT
      "sale_store_sales"."SS_NET_PROFIT" as "sale_net_profit",
      "sale_store_store"."S_COUNTY" as "sale_store_county",
      "sale_store_store"."S_STATE" as "sale_store_state"
  FROM
      "store_sales" as "sale_store_sales"
      INNER JOIN "date_dim" as "sale_date_date" on "sale_store_sales"."SS_SOLD_DATE_SK" = "sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "sale_store_store" on "sale_store_sales"."SS_STORE_SK" = "sale_store_store"."S_STORE_SK"
  WHERE
      "sale_date_date"."D_YEAR" = 2000
  ),
  vacuous as (
  SELECT
      "sale_date_date"."D_YEAR" as "sale_date_year",
      "sale_store_store"."S_STATE" as "sale_store_state"
  FROM
      "thoughtful"
      LEFT OUTER JOIN "date_dim" as "sale_date_date" on "thoughtful"."sale_date_id" = "sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "sale_store_store" on "thoughtful"."sale_store_id" = "sale_store_store"."S_STORE_SK"
  GROUP BY
      1,
      2),
  cooperative as (
  SELECT
      "sale_date_date"."D_YEAR" as "sale_date_year",
      "sale_store_store"."S_STATE" as "sale_store_state",
      "thoughtful"."sale_store_id" as "sale_store_id"
  FROM
      "thoughtful"
      LEFT OUTER JOIN "date_dim" as "sale_date_date" on "thoughtful"."sale_date_id" = "sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "sale_store_store" on "thoughtful"."sale_store_id" = "sale_store_store"."S_STORE_SK"
  GROUP BY
      1,
      2,
      3),
  concerned as (
  SELECT
      "questionable"."_virt_agg_sum_807075557008438" as "_virt_agg_sum_807075557008438",
      "vacuous"."sale_date_year" as "sale_date_year",
      coalesce("questionable"."sale_store_state","vacuous"."sale_store_state") as "sale_store_state"
  FROM
      "vacuous"
      INNER JOIN "questionable" on "vacuous"."sale_store_state" is not distinct from "questionable"."sale_store_state"),
  uneven as (
  SELECT
      "cooperative"."sale_date_year" as "sale_date_year",
      "cooperative"."sale_store_id" as "sale_store_id",
      "questionable"."_virt_agg_sum_807075557008438" as "_virt_agg_sum_807075557008438",
      coalesce("cooperative"."sale_store_state","questionable"."sale_store_state") as "sale_store_state"
  FROM
      "cooperative"
      FULL JOIN "questionable" on "cooperative"."sale_store_state" is not distinct from "questionable"."sale_store_state"),
  young as (
  SELECT
      "concerned"."sale_store_state" as "sale_store_state",
      CASE WHEN "concerned"."sale_date_year" = 2000 and "concerned"."sale_store_state" is not null THEN "concerned"."_virt_agg_sum_807075557008438" ELSE NULL END as "state_profit"
  FROM
      "concerned"),
  yummy as (
  SELECT
      "uneven"."sale_store_id" as "sale_store_id",
      "uneven"."sale_store_state" as "sale_store_state",
      CASE WHEN "uneven"."sale_date_year" = 2000 and "uneven"."sale_store_state" is not null THEN "uneven"."_virt_agg_sum_807075557008438" ELSE NULL END as "state_profit"
  FROM
      "uneven"),
  sparkling as (
  SELECT
      "young"."sale_store_state" as "sale_store_state",
      "young"."state_profit" as "state_profit"
  FROM
      "young"
  GROUP BY
      1,
      2),
  juicy as (
  SELECT
      "yummy"."sale_store_state" as "sale_store_state",
      "yummy"."state_profit" as "state_profit"
  FROM
      "yummy"
  GROUP BY
      1,
      2,
      "yummy"."sale_store_id"),
  abhorrent as (
  SELECT
      "sparkling"."sale_store_state" as "sale_store_state",
      "sparkling"."state_profit" as "state_profit",
      rank() over (order by "sparkling"."state_profit" desc ) as "state_rank"
  FROM
      "sparkling"),
  sweltering as (
  SELECT
      "juicy"."sale_store_state" as "top5_state"
  FROM
      "abhorrent"
      INNER JOIN "juicy" on "abhorrent"."sale_store_state" = "juicy"."sale_store_state" AND "abhorrent"."state_profit" = "juicy"."state_profit"
  WHERE
      "abhorrent"."state_rank" <= 5

  GROUP BY
      1),
  scrawny as (
  SELECT
      "cheerful"."sale_store_county" as "rolled_county",
      "cheerful"."sale_store_state" as "rolled_state",
      grouping("cheerful"."sale_store_county") as "rolled_g_county",
      grouping("cheerful"."sale_store_state") as "rolled_g_state"
  FROM
      "cheerful"
  WHERE
      "cheerful"."sale_store_state" in (select sweltering."top5_state" from sweltering where sweltering."top5_state" is not null) or ( grouping("cheerful"."sale_store_state") = 1 and grouping("cheerful"."sale_store_county") = 1 )

  GROUP BY
      ROLLUP (2, 1)),
  late as (
  SELECT
      "cheerful"."sale_store_county" as "rolled_county",
      "cheerful"."sale_store_state" as "rolled_state",
      CASE
  	WHEN grouping("cheerful"."sale_store_county") = 0 THEN "cheerful"."sale_store_state"
  	ELSE null
  	END as "_virt_func_case_5533437872336584",
      grouping("cheerful"."sale_store_state") + grouping("cheerful"."sale_store_county") as "_virt_paren_5589105324586112",
      sum("cheerful"."sale_net_profit") as "rolled_total_net_profit"
  FROM
      "cheerful"
  WHERE
      "cheerful"."sale_store_state" in (select sweltering."top5_state" from sweltering where sweltering."top5_state" is not null) or ( grouping("cheerful"."sale_store_state") = 1 and grouping("cheerful"."sale_store_county") = 1 )

  GROUP BY
      ROLLUP (2, 1)),
  macho as (
  SELECT
      "late"."rolled_county" as "rolled_county",
      "late"."rolled_state" as "rolled_state",
      "late"."rolled_total_net_profit" as "rolled_total_net_profit",
      rank() over (partition by "late"."_virt_paren_5589105324586112","late"."_virt_func_case_5533437872336584" order by "late"."rolled_total_net_profit" desc ) as "rnk"
  FROM
      "late")
  SELECT
      "macho"."rolled_state" as "rolled_state",
      "macho"."rolled_county" as "rolled_county",
      "macho"."rolled_total_net_profit" as "rolled_total_net_profit",
      "scrawny"."rolled_g_state" + "scrawny"."rolled_g_county" as "level",
      "macho"."rnk" as "rnk"
  FROM
      "scrawny"
      INNER JOIN "macho" on "scrawny"."rolled_county" = "macho"."rolled_county" AND "scrawny"."rolled_state" = "macho"."rolled_state"
  ORDER BY
      "level" desc nulls first,
      "macho"."rolled_state" asc nulls first,
      "macho"."rnk" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: 5 undefined concept references; fix all before re-running:
    - ranked.state (line 41, col 5, in SELECT); did you mean: ranked.rolled.state, sale.customer.address.state, sale.return_customer.address.state, sale.store.state, sale.return_store.state, sale.sale_address.state?
    - ranked.county (line 42, col 5, in SELECT); did you mean: ranked.rolled.county, sale.customer.address.county, sale.return_customer.address.county, sale.store.county, sale.return_store.county, sale.sale_address.county?
    - ranked.total_net_profit (line 43, col 5, in SELECT); did you mean: ranked.rolled.total_net_profit, rolled.total_net_profit, _rolled_total_net_profit?
    - ranked.state (line 47, col 6, in WHERE); did you mean: ranked.rolled.state, sale.customer.address.state, sale.return_customer.address.state, sale.store.state, sale.return_store.state, sale.sale_address.state?
    - ranked.state (line 49, col 41, in ORDER BY); did you mean: ranked.rolled.state, sale.customer.address.state, sale.return_customer.address.state, sale.store.state, sale.return_store.state, sale.sale_address.state?
  ```
- `trilogy run query70.preql`

  ```text
  Resolution error in query70.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {rolled.county, rolled.state, state_totals.net_profit, state_totals.state}
  ```
- `trilogy file read query70.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query72.preql`

  ```text
  Syntax error in query72.preql: HAVING references 'inv.quantity_on_hand', 'cs.quantity', which are not in the SELECT projection (line 6). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --inv.quantity_on_hand, --cs.quantity
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query72.preql`

  ```text
  Resolution error in query72.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs.bill_customer_demographic.marital_status, cs.bill_household_demographic.buy_potential, cs.days_to_ship, cs.quantity, cs.sold_date.year, inv.date.week_seq, inv.quantity_on_hand}
  ```
- `trilogy run query72.preql`

  ```text
  Resolution error in query72.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {cs.bill_customer_demographic.marital_status, cs.bill_household_demographic.buy_potential, cs.days_to_ship, cs.quantity, cs.sold_date.week_seq, cs.sold_date.year, inv.quantity_on_hand}
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query72.preql`

  ```text
  Syntax error in query72.preql: HAVING references 'sales_filtered.sale_qty', which is not in the SELECT projection (line 21). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --sales_filtered.sale_qty
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query72.preql`

  ```text
  Syntax error in query72.preql: Undefined concept: sales_filtered.order_number (line 32, in SELECT). Suggestions: ['sales_filtered.cs.order_number', 'cs.order_number', 'sales_filtered.item_id', 'sales_filtered.week_seq']
  ```
- `trilogy run query72.preql`

  ```text
  Resolution error in query72.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {local._inv_filtered_item_id, local._inv_filtered_qty_on_hand, local._inv_filtered_warehouse_name, local._inv_filtered_week_seq}; {sales_filtered.sale_qty}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query72.preql`

  ```text
  Syntax error in query72.preql: HAVING references 'inv.quantity_on_hand', 'cs.quantity', which are not in the SELECT projection (line 8). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --inv.quantity_on_hand, --cs.quantity
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query72.preql`

  ```text
  Syntax error in query72.preql: Undefined concept: cs.item. Suggestions: ['cs.item.id', 'cs.item.text_id', 'cs.item.product_name', 'cs.item.brand_id', 'cs.item.brand_name', 'cs.item.manufacturer_id']
  ```
- `trilogy run query72.preql`

  ```text
  Syntax error in query72.preql: Undefined concept: cs.sold_date. Suggestions: ['cs.sold_date.id', 'cs.sold_date.text_id', 'cs.sold_date._date_string', 'cs.sold_date.date', 'cs.sold_date.year', 'cs.sold_date.day_of_week']
  ```
- `trilogy run query74.preql duckdb`

  ```text
  Syntax error in query74.preql: Have {'GroupNode<store_2001.customer_code,store_2001.first_name,store_2001.last_name...4 more>': sales.channel = STORE and sales.date.year = 2001} and need store_2001.store_total_2001 > 0 and web_2001.web_total_2001 > 0 and divide(web_2002.web_total_2002@Grain<web_2002.customer_code,web_2002.first_name,web_2002.last_name>,web_2001.web_total_2001@Grain<web_2001.customer_code,web_2001.first_name,web_2001.last_name>) > divide(store_2002.store_total_2002@Grain<store_2002.customer_code,store_2002.first_name,store_2002.last_name>,store_2001.store_total_2001@Grain<store_2001.customer_code,store_2001.first_name,store_2001.last_name>)
  ```
- `trilogy run query74.preql duckdb`

  ```text
  Syntax error in query74.preql: HAVING references 'sales.net_paid', 'sales.channel', 'sales.date.year', which are not in the SELECT projection (line 4). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --sales.net_paid, --sales.channel, --sales.date.year
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query74_test.preql duckdb`

  ```text
  Syntax error in query74_test.preql: Undefined concept: sales.line_item (line 3, in SELECT). Suggestions: ['sales.item.id', 'sales.net_profit', 'sales.sales_price']
  ```
- `trilogy run query74.preql duckdb`

  ```text
  Syntax error in query74.preql: HAVING references 'sales.net_paid', 'sales.channel', 'sales.date.year', which are not in the SELECT projection (line 3). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --sales.net_paid, --sales.channel, --sales.date.year
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query74_test3.preql duckdb`

  ```text
  Syntax error in query74_test3.preql: HAVING references 'sales.net_paid', 'sales.channel', 'sales.date.year', which are not in the SELECT projection (line 3). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --sales.net_paid, --sales.channel, --sales.date.year
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query74.preql duckdb`

  ```text
  Syntax error in query74.preql: HAVING references 'totals.store_2001', 'totals.web_2001', 'totals.web_2002', 'totals.store_2002', which are not in the SELECT projection (line 14). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --totals.store_2001, --totals.web_2001, --totals.web_2002, --totals.store_2002
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query74.preql duckdb`

  ```text
  Unexpected error in query74.preql: (_duckdb.BinderException) Binder Error: WHERE clause cannot contain aggregates!

  LINE 36:     sum(CASE WHEN ( "cheerful"."sales_channel" = 'STORE' and...
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
      "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
      "sales_catalog_sales_unified"."CS_SHIP_CUSTOMER_SK" as "sales_purchasing_customer_id"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
      "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
      "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_purchasing_customer_id"
  FROM
      "store_sales" as "sales_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
      "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
      "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_purchasing_customer_id"
  FROM
      "web_sales" as "sales_web_sales_unified")
  SELECT
      "sales_purchasing_customer_customers"."C_CUSTOMER_ID" as "totals_customer_code",
      "sales_purchasing_customer_customers"."C_FIRST_NAME" as "totals_first_name",
      "sales_purchasing_customer_customers"."C_LAST_NAME" as "totals_last_name"
  FROM
      "cheerful"
      LEFT OUTER JOIN "date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "sales_purchasing_customer_customers" on "cheerful"."sales_purchasing_customer_id" = "sales_purchasing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      sum(CASE WHEN ( "cheerful"."sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 ) THEN "cheerful"."sales_net_paid" ELSE NULL END) > 0 and sum(CASE WHEN ( "cheerful"."sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 ) THEN "cheerful"."sales_net_paid" ELSE NULL END) > 0 and sum(CASE WHEN ( "cheerful"."sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2002 ) THEN "cheerful"."sales_net_paid" ELSE NULL END) / nullif(sum(CASE WHEN ( "cheerful"."sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 ) THEN "cheerful"."sales_net_paid" ELSE NULL END),0) > sum(CASE WHEN ( "cheerful"."sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2002 ) THEN "cheerful"."sales_net_paid" ELSE NULL END) / nullif(sum(CASE WHEN ( "cheerful"."sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 ) THEN "cheerful"."sales_net_paid" ELSE NULL END),0)

  GROUP BY
      1,
      2,
      3
  ORDER BY
      "totals_customer_code" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query74.preql duckdb`

  ```text
  Syntax error in query74.preql: HAVING references 'sales.net_paid', 'sales.channel', 'sales.date.year', which are not in the SELECT projection (line 3). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --sales.net_paid, --sales.channel, --sales.date.year
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
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
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 54 column 12 (char 2961). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_agg.cat_qty, catalog_agg.cat_sp, catalog_agg.cat_wc, catalog_agg.cust_id, catalog_agg.item_id, catalog_agg.yr, web_agg.cust_id, web_agg.item_id, web_agg.web_qty, web_agg.web_sp, web_agg.web_wc, web_agg.yr}
  ```
- `trilogy run query78.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file read raw/physical_sales.preql`

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
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.date:date select date.date, date.week_seq where date.date in ('2000-06-30','2000-09-27','2000-11-17') order by date.date;`

  ```text
  Syntax error in stdin: Cannot compare DATE and ArrayType<STRING> with operator ComparisonOperator.IN in SubselectComparison(left=ref:date.date, right=('2000-06-30', '2000-09-27', '2000-11-17'), operator=<ComparisonOperator.IN: 'in'>)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 54 (char 53). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 62 column 12 (char 2438). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Undefined concept: web.item.text_id. Suggestions: ['web.web_sales.item.text_id', 'store.item.text_id', 'store.date.text_id', 'store.return_date.text_id', 'store.time.text_id', 'store.return_time.text_id']
  ```
- `trilogy run query83.preql`

  ```text
  Resolution error in query83.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_returns_agg.catalog_qty, store_returns.item_code, store_returns.store_qty, web_returns_agg.web_qty}
  ```
- `trilogy file read query83.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query84.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query84.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query84.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query84.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file read query84.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query84.preql duckdb`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query84.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query84.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql duckdb`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql --config trilogy.toml`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy unit query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql duckdb tpcds.duckdb`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file read query86.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query86.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query87.preql`

  ```text
  Resolution error in query87.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {local.catalog_present}; {local.store_present}; {local.web_present}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query88.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query88.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260622-125347\workspace\_worker_1\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 68320)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy file read query89.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query91.preql`

  ```text
  Syntax error in query91.preql: 8 undefined concept references; fix all before re-running:
    - date.month_of_year (line 3, col 7, in WHERE); did you mean: catalog_returns.sales.date.month_of_year, catalog_returns.date.month_of_year, catalog_returns.sales.ship_date.month_of_year, catalog_returns.sales.sold_date.month_of_year, catalog_returns.sales.ship_customer.first_sales_date.month_of_year, catalog_returns.sales.ship_customer.first_shipto_date.month_of_year?
    - date.year (line 4, col 7, in WHERE); did you mean: catalog_returns.sales.date.year, catalog_returns.date.year, catalog_returns.sales.ship_date.year, catalog_returns.sales.sold_date.year, catalog_returns.sales.ship_customer.first_sales_date.year, catalog_returns.sales.ship_customer.first_shipto_date.year?
    - billing_customer.address.gmt_offset (line 5, col 7, in WHERE); did you mean: catalog_returns.billing_customer.address.gmt_offset, catalog_returns.sales.ship_customer.address.gmt_offset, catalog_returns.sales.bill_customer.address.gmt_offset, catalog_returns.sales.customer_address.gmt_offset, catalog_returns.sales.bill_address.gmt_offset, catalog_returns.refunded_customer.address.gmt_offset?
    - billing_customer.household_demographic.buy_potential (line 6, col 7, in WHERE); did you mean: catalog_returns.billing_customer.household_demographic.buy_potential, catalog_returns.sales.ship_customer.household_demographic.buy_potential, catalog_returns.sales.bill_customer.household_demographic.buy_potential, catalog_returns.sales.bill_household_demographic.buy_potential, catalog_returns.refunded_customer.household_demographic.buy_potential?
    - billing_customer.demographics.marital_status (line 8, col 6, in WHERE); did you mean: catalog_returns.billing_customer.demographics.marital_status, catalog_returns.sales.ship_customer.demographics.marital_status, catalog_returns.sales.bill_customer.demographics.marital_status, catalog_returns.sales.bill_customer_demographic.marital_status, catalog_returns.refunded_customer.demographics.marital_status, marital_status?
    - billing_customer.demographics.education_status (line 8, col 67, in WHERE); did you mean: catalog_returns.billing_customer.demographics.education_status, catalog_returns.sales.ship_customer.demographics.education_status, catalog_returns.sales.bill_customer.demographics.education_status, catalog_returns.sales.bill_customer_demographic.education_status, catalog_returns.refunded_customer.demographics.education_status, education_status?
    - billing_customer.demographics.marital_status (line 10, col 6, in WHERE); did you mean: catalog_returns.billing_customer.demographics.marital_status, catalog_returns.sales.ship_customer.demographics.marital_status, catalog_returns.sales.bill_customer.demographics.marital_status, catalog_returns.sales.bill_customer_demographic.marital_status, catalog_returns.refunded_customer.demographics.marital_status, marital_status?
    - billing_customer.demographics.education_status (line 10, col 67, in WHERE); did you mean: catalog_returns.billing_customer.demographics.education_status, catalog_returns.sales.ship_customer.demographics.education_status, catalog_returns.sales.bill_customer.demographics.education_status, catalog_returns.sales.bill_customer_demographic.education_status, catalog_returns.refunded_customer.demographics.education_status, education_status?
  ```
- `trilogy run query91.preql`

  ```text
  Syntax error in query91.preql: Comparison `catalog_returns.billing_customer.demographics.marital_status = 'Married'` can never match enum field 'catalog_returns.billing_customer.demographics.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query94.preql`

  ```text
  Syntax error in query94.preql: Undefined concept: order_analysis.order_number (line 19, in SELECT). Suggestions: ['order_analysis.ws.order_number', 'ws.order_number', 'order_analysis.total_lines']
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as s;

# Build weekly day-of-week sums for web and catalog sales only
# We need sums by (week…as week_seq,
    s.date.day_of_week as dow,
    sum(s.ext_sales_price) as dow_sales
;

select
    week_seq,
    dow_sales
from weekly_dow
limit 100;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...t     week_seq,     dow_sales ??? from weekly_dow limit 100;

  Write stats: received 406 chars / 406 bytes; tail: …'lect\\n    week_seq,\\n    dow_sales\\nfrom weekly_dow\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:s where s.channel in ('WEB', 'CATALOG') select s.date.year, count(*) as cnt order by s.date.year;`

  ```text
  Syntax error in stdin:  --> 2:65
    |
  2 | where s.channel in ('WEB', 'CATALOG') select s.date.year, count(*) as cnt order by s.date.year;
    |                                                                 ^---
    |
    = expected access_chain
  Location:
  ...G') select s.date.year, count( ??? *) as cnt order by s.date.year...
  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Compute per-week-sum by day_of_week for web+catalog sales
rowset weekly_sums <-
wher…kly_sums.dow_sales,
    lead(weekly_sums.dow_sales, 53) over (partition by weekly_sums.dow order by weekly_sums.week_seq) as next_yr_sales
limit 20;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...where weekly_sums.week_seq in ??? (     select s.date.week_seq

  Write stats: received 710 chars / 710 bytes; tail: …'w order by weekly_sums.week_seq) as next_yr_sales\\nlimit 20;\\n'.
  ```
- `trilogy run --import raw.all_sales:s where s.channel in ('WEB','CATALOG') and s.date.day_of_week = 1 and s.date.week_seq between 5270 and 5325 select s.date.week_seq, count(*) as cnt order by s.date.week_seq;`

  ```text
  Syntax error in stdin:  --> 2:137
    |
  2 | where s.channel in ('WEB','CATALOG') and s.date.day_of_week = 1 and s.date.week_seq between 5270 and 5325 select s.date.week_seq, count(*) as cnt order by s.date.week_seq;
    |                                                                                                                                         ^---
    |
    = expected access_chain
  Location:
  ...select s.date.week_seq, count( ??? *) as cnt order by s.date.week...
  ```
- `trilogy file write query04.preql --content import raw.all_sales as sales;

# Annual value per customer per channel per year
# Value = ext_list_price - ext_wh…ag_2002_store as preferred_cust_flag
order by
  customer_id asc,
  first_name asc,
  last_name asc,
  preferred_cust_flag asc
limit 100 nulls first;
`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
    --> 72:1
     |
  72 | order by
     | ^---
     |
     = expected metadata, where, having, or JOIN_TYPE
  Location:
  ..._store as preferred_cust_flag ??? order by   customer_id asc,

  Write stats: received 4004 chars / 4004 bytes; tail: …'_name asc,\\n  preferred_cust_flag asc\\nlimit 100 nulls first;\\n'.
  ```
- `trilogy file write query11.preql --content import raw.all_sales as all_sales;

# Computed revenue per line: ext_list_price - ext_discount_amount
auto line_re…id as cust_id
group by all_sales.billing_customer.id
having
  bool_or(all_sales.channel = 'STORE') and bool_or(all_sales.channel = 'WEB')
limit 1000;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...illing_customer.id as cust_id ??? group by all_sales.billing_cus...

  Write stats: received 868 chars / 868 bytes; tail: …" 'STORE') and bool_or(all_sales.channel = 'WEB')\\nlimit 1000;".
  ```
- `trilogy file write query11.preql --content import raw.all_sales as all_sales;

# Computed revenue per line: ext_list_price - ext_discount_amount
auto line_re…= 'STORE') as had_store_2001,
  bool_or(all_sales.channel = 'WEB') as had_web_2001
having
  had_store_2001 = true and had_web_2001 = true;

select 1;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:9
     |
  23 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...had_web_2001 = true;  select 1 ??? ;

  Write stats: received 923 chars / 923 bytes; tail: …'\\n  had_store_2001 = true and had_web_2001 = true;\\n\\nselect 1;'.
  ```
- `trilogy file write query11.preql --content import raw.all_sales as all_sales;

# Revenue per line: ext_list_price - ext_discount_amount
auto line_revenue <- …les.billing_customer.first_name as first_name,
  all_sales.billing_customer.last_name as last_name,
  pref_flag_2002 as preferred_cust_flag
limit 10;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:3
     |
  18 |   and bool_or(all_sales.channel = 'WEB' ? all_sales.date.year = 2001) by all_sales.billing_customer.id = true
     |   ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ....billing_customer.id = true   ??? and bool_or(all_sales.channel

  Write stats: received 1923 chars / 1923 bytes; tail: …'last_name,\\n  pref_flag_2002 as preferred_cust_flag\\nlimit 10;'.
  ```
- `trilogy file write query11.preql --content import raw.all_sales as all_sales;

# Revenue per line: ext_list_price - ext_discount_amount
auto line_revenue <- …2001
auto store_rev_2001 <- sum(line_revenue ? all_sales.channel = STORE and all_sales.date.year = 2001) by all_sales.billing_customer.id;

select 1;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:9
    |
  9 | select 1;
    |         ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...billing_customer.id;  select 1 ??? ;

  Write stats: received 368 chars / 368 bytes; tail: …'te.year = 2001) by all_sales.billing_customer.id;\\n\\nselect 1;'.
  ```
- `trilogy file write query11.preql --content import raw.all_sales as all_sales;

auto line_revenue <- all_sales.ext_list_price - coalesce(all_sales.ext_discoun…
  billing_customer_code asc nulls first,
  first_name asc nulls first,
  last_name asc nulls first,
  preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...mer.id;  # --- Main query --- ??? inner join both_2001.cust_id =...

  Write stats: received 1886 chars / 1886 bytes; tail: …'ulls first,\\n  preferred_cust_flag asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.sold_date.year, count(cs.order_number) as cnt, cs.bill_customer.address.country, cs.bill_customer.address…s.bill_customer.birth_month in (1,2,6,8,9,12) and cs.bill_customer.address.state in ('MS','IN','ND','OK','NM','VA') group by 1,3,4,5 limit 20; duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...MS','IN','ND','OK','NM','VA') ??? group by 1,3,4,5 limit 20;
  ```
- `trilogy file write query21.preql --content import raw.inventory as inv;

# Before window: 2000-02-10 (inclusive) up to but not including 2000-03-11
# After w…before_total * 2 / 3
    and after_total <= before_total * 3 / 2
order by
    warehouse_name asc nulls first,
    item_code asc nulls first
limit 100`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:1
     |
  19 | order by
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...total <= before_total * 3 / 2 ??? order by     warehouse_name as...

  Write stats: received 806 chars / 806 bytes; tail: …'ame asc nulls first,\\n    item_code asc nulls first\\nlimit 100'.
  ```
- `trilogy file write query21.preql --content import raw.inventory as inv;

# Before window: 2000-02-10 (inclusive) up to but not including 2000-03-11
# After w…otal * 2.0 / 3.0
    and after_total <= before_total * 3.0 / 2.0
order by
    warehouse_name asc nulls first,
    item_code asc nulls first
limit 100`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:1
     |
  19 | order by
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...l <= before_total * 3.0 / 2.0 ??? order by     warehouse_name as...

  Write stats: received 814 chars / 814 bytes; tail: …'ame asc nulls first,\\n    item_code asc nulls first\\nlimit 100'.
  ```
- `trilogy file write query21.preql --content import raw.inventory as inv;

# Before window: 2000-02-10 (inclusive) up to but not including 2000-03-11
# After w…/ 3.0)
    and (after_total * 1.0) <= (before_total * 3.0 / 2.0)
order by
    warehouse_name asc nulls first,
    item_code asc nulls first
limit 100`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:1
     |
  19 | order by
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...<= (before_total * 3.0 / 2.0) ??? order by     warehouse_name as...

  Write stats: received 834 chars / 834 bytes; tail: …'ame asc nulls first,\\n    item_code asc nulls first\\nlimit 100'.
  ```
- `trilogy file write query21.preql --content import raw.inventory as inv;

where inv.item.current_price between 0.99 and 1.49
  and inv.date.date >= '2000-02-1…and ? inv.date.date < '2000-03-11'::date) as before_total,
    sum(inv.quantity_on_hand ? inv.date.date >= '2000-03-11'::date) as after_total
limit 5`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:1
     |
  11 | limit 5
     | ^---
     |
     = expected metadata, order_by, where, having, or JOIN_TYPE
  Location:
  ...-03-11'::date) as after_total ??? limit 5

  Write stats: received 423 chars / 423 bytes; tail: …" inv.date.date >= '2000-03-11'::date) as after_total\\nlimit 5".
  ```
- `trilogy file write query21.preql --content import raw.inventory as inv;

# Filtered inventory sums by warehouse and item
auto before_oh <- sum(inv.quantity_o…elect
    inv.warehouse.name as warehouse_name,
    inv.item.text_id as item_code,
    before_oh as before_total,
    after_oh as after_total
limit 5`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | limit 5
     | ^---
     |
     = expected metadata, order_by, where, having, or JOIN_TYPE
  Location:
  ...,     after_oh as after_total ??? limit 5

  Write stats: received 608 chars / 608 bytes; tail: …'fore_oh as before_total,\\n    after_oh as after_total\\nlimit 5'.
  ```
- `trilogy file write query21.preql --content import raw.inventory as inv;

where inv.item.current_price between 0.99 and 1.49
  and inv.date.date >= '2000-02-1…00-04-10'::date
select
    inv.warehouse.name as warehouse_name,
    inv.item.text_id as item_code,
    sum(inv.quantity_on_hand) as total_oh
limit 5`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:1
     |
  10 | limit 5
     | ^---
     |
     = expected metadata, order_by, where, having, or JOIN_TYPE
  Location:
  ...quantity_on_hand) as total_oh ??? limit 5

  Write stats: received 298 chars / 298 bytes; tail: …'item_code,\\n    sum(inv.quantity_on_hand) as total_oh\\nlimit 5'.
  ```
- `trilogy file write query21.preql --content import raw.inventory as inv;
select inv.warehouse.name as warehouse_name limit 5`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
   --> 2:45
    |
  2 | select inv.warehouse.name as warehouse_name limit 5
    |                                             ^---
    |
    = expected metadata, order_by, where, having, or JOIN_TYPE
  Location:
  ...ehouse.name as warehouse_name ??? limit 5

  Write stats: received 80 chars / 80 bytes; tail: …' as inv;\\nselect inv.warehouse.name as warehouse_name limit 5'.
  ```
- `trilogy file write query21.preql`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
   --> 2:45
    |
  2 | select inv.warehouse.name as warehouse_name limit 5
    |                                             ^---
    |
    = expected metadata, order_by, where, having, or JOIN_TYPE
  Location:
  ...ehouse.name as warehouse_name ??? limit 5

  Write stats: received 81 chars / 81 bytes; tail: …'as inv;\r\\nselect inv.warehouse.name as warehouse_name limit 5'.
  ```
- `trilogy file write query21.preql`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
   --> 2:69
    |
  2 | select inv.warehouse.name as warehouse_name order by warehouse_name limit 5
    |                                                                     ^---
    |
    = expected ORDER_DIRECTION_LA, ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._name order by warehouse_name ??? limit 5

  Write stats: received 105 chars / 105 bytes; tail: …'house.name as warehouse_name order by warehouse_name limit 5'.
  ```
- `trilogy file write query21.preql`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...e_name order by warehouse_name ???

  Write stats: received 97 chars / 97 bytes; tail: …'inv.warehouse.name as warehouse_name order by warehouse_name'.
  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# ---- Step…rder by combined_sales.last_name asc nulls first,
         combined_sales.first_name asc nulls first,
         total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...y the first 30 chars of desc. ??? rowset freq_items as where sto...

  Write stats: received 2587 chars / 2587 bytes; tail: …'nulls first,\\n         total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_store_returns:csr select csr.store_sale_date.year, count(csr.ticket_number) as cnt group by csr.store_sale_date.year order by csr.store_sale_date.year limit 5; duck_db tpcds.duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...unt(csr.ticket_number) as cnt ??? group by csr.store_sale_date.y...
  ```
- `trilogy file write query27.preql --content import raw.physical_sales as store_sales;

where
  store_sales.date.year = 2002
  and store_sales.store.state = 'T…p store_sales.item.text_id, store_sales.store.state
order by
  store_sales.item.text_id nulls first,
  store_sales.store.state nulls first
limit 100;`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:3
     |
  22 |   by rollup store_sales.item.text_id, store_sales.store.state
     |   ^---
     |
     = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...es_price) as avg_unit_price   ??? by rollup store_sales.item.tex...

  Write stats: received 892 chars / 892 bytes; tail: …'ulls first,\\n  store_sales.store.state nulls first\\nlimit 100;'.
  ```
- `trilogy file write query31.preql --content import raw.physical_sales as store;
import raw.web_sales as web;

# Use union to combine store and web channel dat…erently.

# Actually the union approach stacks rows, but the question wants store and web
# side by side. Let me use a different approach.

select 1;`

  ```text
  refused to write 'query31.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:9
     |
  33 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   different approach.  select 1 ??? ;

  Write stats: received 1249 chars / 1249 bytes; tail: …'\\n# side by side. Let me use a different approach.\\n\\nselect 1;'.
  ```
- `trilogy file write query32.preql --content import raw.catalog_sales as cs;

# Filter to manufacturer 48 within date window
where cs.sold_date.date between '2…g(cs.discount_amount) by cs.item.id;

select
    sum(cs.discount_amount ? cs.discount_amount > 1.3 * item_avg_discount) as total_discount
limit 100;
`

  ```text
  refused to write 'query32.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...l catalog sales in the window ??? auto item_avg_discount <- avg(...

  Write stats: received 434 chars / 434 bytes; tail: …'unt > 1.3 * item_avg_discount) as total_discount\\nlimit 100;\\n'.
  ```
- `trilogy file write query34.preql --content import raw/physical_sales as ss;

where 
    ss.date.year in (1999, 2000, 2001)
    and (ss.date.day_of_month betw… nulls first,
    ss.customer.salutation asc nulls first,
    ss.customer.preferred_cust_flag desc nulls first,
    ss.ticket_number asc nulls first;`

  ```text
  refused to write 'query34.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/physical_sales as ss;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/physical_sales as ss;  whe...

  Write stats: received 962 chars / 962 bytes; tail: …'flag desc nulls first,\\n    ss.ticket_number asc nulls first;'.
  ```
- `trilogy file write query37.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

where
  cs.item.current_price between 68 and 98
  an…00
  )
select
  cs.item.text_id as item_code,
  cs.item.desc as description,
  cs.item.current_price as current_price
order by
  item_code
limit 100;`

  ```text
  refused to write 'query37.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...808)   and cs.item.text_id in ??? (     select inv.item.text_id

  Write stats: received 516 chars / 516 bytes; tail: …'rrent_price as current_price\\norder by\\n  item_code\\nlimit 100;'.
  ```
- `trilogy file write query38.preql -c import raw.all_sales as all_sales;

# Get unique (last_name, first_name, sale_date) combos for store sales in 2000
with s…e = catalog_tuples.first_name = web_tuples.first_name
inner join store_tuples.sale_date = catalog_tuples.sale_date = web_tuples.sale_date
limit 100;
`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:11
     |
  32 |     count(*) as unique_combination_count
     |           ^---
     |
     = expected access_chain
  Location:
  ...ree channels select     count( ??? *) as unique_combination_count...

  Write stats: received 1372 chars / 1372 bytes; tail: …' catalog_tuples.sale_date = web_tuples.sale_date\\nlimit 100;\\n'.
  ```
- `trilogy file write query43.preql --content import raw.physical_sales as sales;

# Pivot: one filtered aggregate per day of week summing the unit price
def su…s as thursday,
    @friday_sales as friday,
    @saturday_sales as saturday
order by
    sales.store.name asc,
    sales.store.text_id asc
limit 100;`

  ```text
  refused to write 'query43.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | def sunday_sales   -> sum(sales.sales_price ? sales.date.day_name = 'Sunday');
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...f week summing the unit price ??? def sunday_sales   -> sum(sale...

  Write stats: received 1079 chars / 1079 bytes; tail: …'sales.store.name asc,\\n    sales.store.text_id asc\\nlimit 100;'.
  ```
- `trilogy file write query45.preql --content import raw.web_sales as ws;

# Define the set of item text_ids that match the given item ids
auto target_item_text…address.city,
  sum(ws.sales_price) as total_web_unit_price
order by
  ws.billing_customer.address.zip,
  ws.billing_customer.address.city
limit 100;`

  ```text
  refused to write 'query45.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:28
    |
  7 | auto target_zips <- '85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792';
    |                            ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ck auto target_zips <- '85669' ??? , '86197', '88274', '83405', '...

  Write stats: received 739 chars / 739 bytes; tail: …'r.address.zip,\\n  ws.billing_customer.address.city\\nlimit 100;'.
  ```
- `trilogy file write query53.preql --content import raw.physical_sales as ss;
import raw.item as item;
import raw.date as date;

# Filter to year 2000 store sa…manufacturer_id) as avg_quarterly_sales
  sum(ss.sales_price) as total_unit_price
having
  total_unit_price > 0
order by
  total_unit_price
limit 10;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:3
     |
  21 |   --, avg(sum(ss.sales_price) by item.manufacturer_id) as avg_quarterly_sales
     |   ^---
     |
     = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ..._price) as total_unit_price   ??? --, avg(sum(ss.sales_price) by...

  Write stats: received 1013 chars / 1013 bytes; tail: …'  total_unit_price > 0\\norder by\\n  total_unit_price\\nlimit 10;'.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.physical_sales as ps;
import raw.date as d;…ice / 50)::int * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:34
     |
  17 | auto target_customers <- distinct(union((select cat_customers), (select web_customers)) -> (cust_id int));
     |                                  ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...o target_customers <- distinct ??? (union((select cat_customers),...

  Write stats: received 1721 chars / 1721 bytes; tail: …'r_count asc nulls first,\\n    segment_times_50 asc\\nlimit 100;'.
  ```
- `trilogy run --import raw.item:item --import raw.all_sales:all_sales select item.text_id as item_code, sum(all_sales.ext_sales_price) as total where item.colo…and all_sales.bill_address.gmt_offset = -5 left join item.id = all_sales.item.id order by total asc nulls first, item_code asc nulls first limit 100;`

  ```text
  Syntax error in stdin:  --> 3:239
    |
  3 | select item.text_id as item_code, sum(all_sales.ext_sales_price) as total where item.color in ('slate','blanched','burnished') and all_sales.date.year = 2001 and all_sales.date.month_of_year = 2 and all_sales.bill_address.gmt_offset = -5 left join item.id = all_sales.item.id order by total asc nulls first, item_code asc nulls first limit 100;
    |                                                                                                                                                                                                                                               ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ....bill_address.gmt_offset = -5 ??? left join item.id = all_sales....
  ```
- `trilogy run --import raw.item:item --import raw.all_sales:all_sales select item.text_id as item_code, sum(all_sales.ext_sales_price) as total where item.colo…and all_sales.bill_address.gmt_offset = -5 left join item.id = all_sales.item.id order by total asc nulls first, item_code asc nulls first limit 100;`

  ```text
  Syntax error in stdin:  --> 3:239
    |
  3 | select item.text_id as item_code, sum(all_sales.ext_sales_price) as total where item.color in ('slate','blanched','burnished') and all_sales.date.year = 2001 and all_sales.date.month_of_year = 2 and all_sales.bill_address.gmt_offset = -5 left join item.id = all_sales.item.id order by total asc nulls first, item_code asc nulls first limit 100;
    |                                                                                                                                                                                                                                               ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ....bill_address.gmt_offset = -5 ??? left join item.id = all_sales....
  ```
- `trilogy run --import raw.item:item --import raw.all_sales:all_sales rowset qualifying_items <- select item.text_id as item_code where item.color in ('slate',…ss.gmt_offset = -5 left join qualifying_items.item_code = all_sales.item.text_id order by total asc nulls first, item_code asc nulls first limit 100;`

  ```text
  Syntax error in stdin:  --> 3:316
    |
  3 | rowset qualifying_items <- select item.text_id as item_code where item.color in ('slate','blanched','burnished'); select qualifying_items.item_code as item_code, sum(all_sales.ext_sales_price) as total where all_sales.date.year = 2001 and all_sales.date.month_of_year = 2 and all_sales.bill_address.gmt_offset = -5 left join qualifying_items.item_code = all_sales.item.text_id order by total asc nulls first, item_code asc nulls first limit 100;
    |                                                                                                                                                                                                                                                                                                                            ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ....bill_address.gmt_offset = -5 ??? left join qualifying_items.ite...
  ```
- `trilogy run --import raw/date:date select min(date.week_seq) as min_ws, max(date.week_seq) as max_ws, date.year where date.year in (2001, 2002) group by date.year; duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ere date.year in (2001, 2002) ??? group by date.year;
  ```
- `trilogy file write query59.preql --content import raw.physical_sales as ps;

# Define filtered aggregates for each day of week
# day_of_week: 0=Sunday, 1=Mon…t_year.store_code
order by this_year.store_name nulls first,
         this_year.store_code nulls first,
         this_year.ws nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | rowset this_year as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...);  # 2001 weekly data rowset ??? rowset this_year as where ps.d...

  Write stats: received 3189 chars / 3189 bytes; tail: …'e nulls first,\\n         this_year.ws nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query59.preql --content import raw.physical_sales as ps;

# Define filtered aggregates for each day of week
# day_of_week: 0=Sunday, 1=Mon…t_year.store_code
order by this_year.store_name nulls first,
         this_year.store_code nulls first,
         this_year.ws nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 78:40
     |
  78 | inner join this_year.ws = next_year.ws - 52
     |                                        ^---
     |
     = expected limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...n this_year.ws = next_year.ws ??? - 52 inner join this_year.stor...

  Write stats: received 3171 chars / 3171 bytes; tail: …'e nulls first,\\n         this_year.ws nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/physical_sales:ps select count(ps.store.name) as cnt where ps.date.year = 2001 group by ps.store.name, ps.date.week_seq; duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...cnt where ps.date.year = 2001 ??? group by ps.store.name, ps.dat...
  ```
- `trilogy file write query64.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as cat_sales;
import raw.catalog_returns as cat…m.item_id
having
    cat_sales_by_item.total_cat_list_price > 2 * cat_returns_by_item.total_cat_refund;

select * from qualifying_cat_items
limit 10;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:8
     |
  27 | select * from qualifying_cat_items
     |        ^---
     |
     = expected select_item
  Location:
  ...tem.total_cat_refund;  select ??? * from qualifying_cat_items li...

  Write stats: received 1005 chars / 1005 bytes; tail: …'al_cat_refund;\\n\\nselect * from qualifying_cat_items\\nlimit 10;'.
  ```
- `trilogy file write query64.preql --content import raw.physical_sales as store_sales;
import raw.catalog_returns as cat_returns;

# Per-item catalog condition…esce(cat_returns.store_credit, 0)) as total_cat_refund
having
    total_cat_list > 2 * total_cat_refund;

select * from cat_qualifying_items
limit 5;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:8
     |
  13 | select * from cat_qualifying_items
     |        ^---
     |
     = expected select_item
  Location:
  ...2 * total_cat_refund;  select ??? * from cat_qualifying_items li...

  Write stats: received 581 chars / 581 bytes; tail: …'tal_cat_refund;\\n\\nselect * from cat_qualifying_items\\nlimit 5;'.
  ```
- `trilogy file write query65.preql --content import raw.physical_sales as store_sales;

# For store sales in year 1998, compute per-item revenue (sum of unit p…ore_name asc nulls first,
    item_desc asc nulls first,
    store_sales.store.id asc nulls first,
    store_sales.item.id asc nulls first
limit 100;`

  ```text
  refused to write 'query65.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:5
     |
  17 |     --store_sales.store.id
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...e.id as store_avg_revenue     ??? --store_sales.store.id order b...

  Write stats: received 908 chars / 908 bytes; tail: …'ls first,\\n    store_sales.item.id asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store:store select store.state, count(store.id) as cnt group by store.state order by cnt desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...state, count(store.id) as cnt ??? group by store.state order by
  ```
- `trilogy file write query72.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

# filter catalog sales to 1999, matching household/b…rst,
  sales_filtered.item_desc asc nulls first,
  inv_filtered.warehouse_name asc nulls first,
  sales_filtered.week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'query72.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset sales_filtered as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ntial/marital status/shipping ??? rowset sales_filtered as where...

  Write stats: received 1535 chars / 1535 bytes; tail: …'first,\\n  sales_filtered.week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query74.preql -e -c import raw.all_sales as sales;

# For each customer, compute the 4 channel-year totals
select
    sales.purchasing_cus…ate.year = 2002)) / sum(sales.net_paid ? (sales.channel = 'STORE' and sales.date.year = 2001))
order by
    customer_code asc nulls first
limit 100
;`

  ```text
  refused to write 'query74.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:5
    |
  8 |     --sum(sales.net_paid ? (sales.channel = 'STORE' and sales.date.year = 2001)) as store_2001,
    |     ^---
    |
    = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...er.last_name as last_name     ??? --sum(sales.net_paid ? (sales....

  Write stats: received 1181 chars / 1181 bytes; tail: …'001))\\norder by\\n    customer_code asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy file write query75.preql -e -c import raw.all_sales as s;
import raw.item as i;

# Filter to Books category and compute per-line net metrics
rowset b…d,
    --sum(yr_agg.total_qty ? yr_agg.yr = 2001) as prev_qty,
    --sum(yr_agg.total_qty ? yr_agg.yr = 2002) as curr_qty
left join yr_agg.yr = 2001
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 42:23
     |
  42 | left join yr_agg.yr = 2001
     |                       ^---
     |
     = expected IDENTIFIER
  Location:
  ...urr_qty left join yr_agg.yr = ??? 2001

  Write stats: received 1154 chars / 1154 bytes; tail: …' ? yr_agg.yr = 2002) as curr_qty\\nleft join yr_agg.yr = 2001\\n'.
  ```
- `trilogy run --import raw.physical_sales:sales select sales.customer.id as cid, sum(case when sales.quantity is not null and sales.sales_price is not null the… end end) as adj where sales.return_reason.desc = 'reason 28' group by sales.customer.id order by adj asc nulls first, cid asc nulls first limit 100;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...urn_reason.desc = 'reason 28' ??? group by sales.customer.id ord...
  ```
- `trilogy run --import raw.physical_sales:sales select count(sales.customer.id) as cnt where sales.return_reason.desc = 'reason 28' group by sum(case when sale…null then (sales.quantity - sales.return_quantity) * sales.sales_price else sales.quantity * sales.sales_price end) is null having grouping(cnt) = 0;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...urn_reason.desc = 'reason 28' ??? group by sum(case when sales.r...
  ```
- `trilogy run --import raw.physical_sales:sales select sales.customer.id as cid, sum(case when sales.return_quantity is not null then (sales.quantity - sales.r…les.quantity * sales.sales_price end) as adj where sales.return_reason.desc = 'reason 28' order by adj asc nulls first, cid asc limit 100 offset 100;`

  ```text
  Syntax error in stdin:  --> 2:245
    |
  2 | select sales.customer.id as cid, sum(case when sales.return_quantity is not null then (sales.quantity - sales.return_quantity) * sales.sales_price else sales.quantity * sales.sales_price end) as adj where sales.return_reason.desc = 'reason 28' order by adj asc nulls first, cid asc limit 100 offset 100;
    |                                                                                                                                                                                                                                                     ^---
    |
    = expected having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...urn_reason.desc = 'reason 28' ??? order by adj asc nulls first,
  ```

### `cli-misuse`

- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy run - --import raw.catalog_store_returns:csr select csr.store_sale_date.year, csr.store_sale_date.month_of_year, count(csr.ticket_number) as cnt limit 5;`

  ```text
  'select csr.store_sale_date.year, csr.store_sale_date.month_of_year, count(csr.ticket_number) as cnt limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy run --format json query40.preql`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql --regex store_sales --regex item --regex date --regex category`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run --explain --import raw/date:date select min(date.week_seq), max(date.week_seq), date.year where date.year in (2001, 2002) group by date.year;`

  ```text
  'select min(date.week_seq), max(date.week_seq), date.year where date.year in (2001, 2002) group by date.year;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run query76.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy run - --import raw.physical_sales:physical_sales select count(physical_sales.line_item) as cnt where physical_sales.store.id is null;`

  ```text
  'select count(physical_sales.line_item) as cnt where physical_sales.store.id is null;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run --format json query83.preql`

  ```text
  'json' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run query86.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:s select s.channel, count(s.order_id) where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_id is not null limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(s.order_id) as order_id_count`
  Location:
   s.channel, count(s.order_id) ??? where s.return_date.date betwe...
  ```
- `trilogy run --import raw.all_sales:s select distinct s.return_channel_dim_text_id as rc_id where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_id is not null and s.channel = 'CATALOG' limit 20;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct s.return_channel_dim_text_id as distinct_s_return_channel_dim_text_id`
  Location:
  ...l_sales as s; select distinct ??? s.return_channel_dim_text_id a...
  ```
- `trilogy run --import raw.date:date select distinct date.quarter_name, date.year, date.quarter limit 20;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct date.quarter_name as distinct_date_quarter_name`
  Location:
  ...date as date; select distinct ??? date.quarter_name, date.year,
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching at least one of the eight detailed attribute profiles
auto profile_item…t distinct item.product_name
where item.manufacturer_id between 1 and 500
  and item.manufact in profile_manufs
order by item.product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct item.product_name as distinct_item_product_name`
  Location:
  ...le_manufs set select distinct ??? item.product_name where item.m...

  Write stats: received 1355 chars / 1355 bytes; tail: …'fact in profile_manufs\\norder by item.product_name\\nlimit 100;'.
  ```
- `trilogy run --import raw.item:item select distinct item.brand_name limit 30;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct item.brand_name as distinct_item_brand_name`
  Location:
  ...item as item; select distinct ??? item.brand_name limit 30;
  ```
- `trilogy file write query53.preql --content import raw.physical_sales as ss;

# Filter to year 2000, recorded store, and the two item profiles
where ss.date.y…urer_id as manufacturer_id,
  ss.date.quarter as quarter,
  --hide the quarter total for HAVING
  --sum(ss.sales_price) as total_unit_price
limit 10;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hide the quarter total for as hide_the_quarter_total_for`
  Location:
  ....quarter as quarter,   --hide ??? the quarter total for HAVING

  Write stats: received 859 chars / 859 bytes; tail: …'HAVING\\n  --sum(ss.sales_price) as total_unit_price\\nlimit 10;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.item.color;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct all_sales.item.color as distinct_all_sales_item_color`
  Location:
  ...as all_sales; select distinct ??? all_sales.item.color;
  ```
- `trilogy run --import raw.item:item --import raw.all_sales:all_sales rowset qualifying_items <- select distinct item.text_id as item_code where item.color in …ss.gmt_offset = -5 left join qualifying_items.item_code = all_sales.item.text_id order by total asc nulls first, item_code asc nulls first limit 100;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct item.text_id as distinct_item_text_id`
  Location:
  ...ying_items <- select distinct ??? item.text_id as item_code wher...
  ```
- `trilogy run --import raw/date:date select min(date.week_seq), max(date.week_seq), date.year where date.year in (2001, 2002) group by date.year; duckdb`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `min(date.week_seq) as week_seq_min`
  Location:
  ...ate; select min(date.week_seq) ??? , max(date.week_seq), date.yea...
  ```

### `file-not-found`

- `trilogy run query39.preql`

  ```text
  Unexpected error in query39.preql: Join key `jan.w_id` does not exist
  ```
- `trilogy run query51.preql`

  ```text
  Unexpected error in query51.preql: Join key `web_running.item_id` does not exist
  ```
- `trilogy explore query51.preql`

  ```text
  Failed to parse query51.preql: Join key `web_running.item_id` does not exist
  ```
- `trilogy run query51.preql`

  ```text
  Unexpected error in query51.preql: Join key `web_running.item_id` does not exist
  ```
- `trilogy run query51.preql`

  ```text
  Unexpected error in query51.preql: Join key `web_running.item_id` does not exist
  ```
- `trilogy explore query51.preql --regex web_running`

  ```text
  Failed to parse query51.preql: Join key `web_running.item_id` does not exist
  ```

### `join-resolution`

- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Could not resolve connections for query with output ['weekly_dow_sums.week_seq<Purpose.PROPERTY>Derivation.ROWSET>', 'local.sunday_ratio<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query35.preql`

  ```text
  Resolution error in query35.preql: Could not resolve connections for query with output ['local.state<Purpose.PROPERTY>Derivation.BASIC>', 'local.gender<Purpose.PROPERTY>Derivation.BASIC>', 'local.marital_status<Purpose.PROPERTY>Derivation.BASIC>', 'local.dependent_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.employed_dependent_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.college_dependent_count<Purpose.PROPERTY>Derivation.BASIC>', 'local.row_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.min_dep_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.max_dep_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.avg_dep_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.min_emp_dep_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.max_emp_dep_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.avg_emp_dep_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.min_col_dep_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.max_col_dep_count<Purpose.METRIC>Derivation.AGGREGATE>', 'local.avg_col_dep_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query36.preql`

  ```text
  Resolution error in query36.preql: Could not resolve connections for query with output ['local.gross_margin<Purpose.PROPERTY>Derivation.BASIC>', 'item.category<Purpose.PROPERTY>Derivation.ROOT>', 'item.class<Purpose.PROPERTY>Derivation.ROOT>', 'local.hierarchy_level<Purpose.PROPERTY>Derivation.BASIC>', 'local.rnk<Purpose.PROPERTY>Derivation.WINDOW>'] from current model.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.qualifying_item_ids<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Could not resolve connections for query with output ['local._books_lines_channel<Purpose.KEY>Derivation.BASIC>', 'local._books_lines_order_id<Purpose.KEY>Derivation.BASIC>', 'local._books_lines_item_id<Purpose.KEY>Derivation.BASIC>', 'local._books_lines_brand_id<Purpose.PROPERTY>Derivation.BASIC>', 'local._books_lines_class_id<Purpose.PROPERTY>Derivation.BASIC>', 'local._books_lines_category_id<Purpose.PROPERTY>Derivation.BASIC>', 'local._books_lines_manufacturer_id<Purpose.PROPERTY>Derivation.BASIC>', 'local._books_lines_net_qty<Purpose.PROPERTY>Derivation.BASIC>', 'local._books_lines_net_amt<Purpose.PROPERTY>Derivation.BASIC>', 'local._books_lines_yr<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```

### `undefined-concept`

- `trilogy explore query70.preql`

  ```text
  Failed to parse query70.preql: (UndefinedConceptException(...), "Undefined concept: ranked_states.state. Suggestions: ['ranked_states.state_totals.state', 'sale.customer.address.state', 'sale.return_customer.address.state', 'sale.store.state', 'sale.return_store.state', 'sale.sale_address.state']")
  ```
