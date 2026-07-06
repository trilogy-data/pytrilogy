# Trilogy failure analysis — 20260706-222300

- Run `20260706-222300` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 723 | failed: 87 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 52 | 60% |
| `syntax-parse` | 30 | 34% |
| `cli-misuse` | 2 | 2% |
| `syntax-missing-alias` | 2 | 2% |
| `join-resolution` | 1 | 1% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Aggregate concept local.sun_ratio cannot reference itself. If defining a new concept in a select, use a new name.
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: SELECT output 'local.channel_label' is defined by an expression that references 'local.channel_label' itself (line 26). This is a recursive self-reference: an alias cannot redefine a name its own calculation reads. Rename the output to a distinct name (e.g. `... as channel_label_out`).
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: SELECT output 'local.channel_label' is defined by an expression that references 'local.channel_label' itself (line 26). This is a recursive self-reference: an alias cannot redefine a name its own calculation reads. Rename the output to a distinct name (e.g. `... as channel_label_out`).
  ```
- `trilogy file read query06.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: date.year. Suggestions: ['ss.date.year', 'ss.store.date.year', 'ss.return_store.date.year', 'ws.date.year', 'ws.ship_date.year', 'ss.return_date.year']
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 28). The requested concepts split into 2 disconnected subgraphs: {store_rev_2001, store_rev_2002}; {web_rev_2001, web_rev_2002, ws.billing_customer.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 22). The requested concepts split into 2 disconnected subgraphs: {ss_rev_01, ss_rev_02}; {ws_rev_01, ws_rev_02, ws.billing_customer.id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(sales.channel)` (line 17), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(sales.channel) as g order by g desc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query16.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(local.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.country) as g order by g desc`.
  ```
- `trilogy run query18.preql`

  ```text
  Unexpected error in query18.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  cooperative as (
  SELECT
      "cs_billing_customer_address_customer_address"."CA_COUNTRY" as "country",
      "cs_billing_customer_address_customer_address"."CA_COUNTY" as "county",
      "cs_billing_customer_address_customer_address"."CA_STATE" as "state",
      "cs_billing_customer_customers"."C_BIRTH_YEAR" as "cs_billing_customer_birth_year",
      "cs_billing_customer_demographic_customer_demographics"."CD_DEP_COUNT" as "cs_billing_customer_demographic_dependent_count",
      "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
      "cs_catalog_sales"."CS_ITEM_SK" as "item_code",
      "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
      "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "cs_billing_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_billing_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "cs_billing_customer_address_customer_address" on "cs_billing_customer_customers"."C_CURRENT_ADDR_SK" = "cs_billing_customer_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "cs_billing_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_billing_customer_demographic_customer_demographics"."CD_DEMO_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 1998 and "cs_billing_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_billing_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_billing_customer_customers"."C_BIRTH_MONTH" in (1,2,6,8,9,12) and "cs_billing_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')
  )
  SELECT
      "cooperative"."item_code" as "item_code",
      "cooperative"."country" as "country",
      "cooperative"."state" as "state",
      "cooperative"."county" as "county",
      avg("cooperative"."cs_quantity") as "avg_ticket_quantity",
      avg("cooperative"."cs_list_price") as "avg_per_line_list_price",
      avg("cooperative"."cs_coupon_amt") as "avg_per_line_coupon_amt",
      avg("cooperative"."cs_sales_price") as "avg_per_line_sales_price",
      avg("cooperative"."cs_net_profit") as "avg_per_line_net_profit",
      avg("cooperative"."cs_billing_customer_birth_year") as "avg_customer_birth_year",
      avg("cooperative"."cs_billing_customer_demographic_dependent_count") as "avg_dependent_count"
  FROM
      "cooperative"
  GROUP BY
      ROLLUP (2, 3, 4, 1)
  ORDER BY
      "cooperative"."country" asc nulls first,
      "cooperative"."state" asc nulls first,
      "cooperative"."county" asc nulls first,
      "cooperative"."item_code" asc nulls first,
      MIN(grouping("cooperative"."country")) asc,
      MIN(grouping("cooperative"."state")) asc,
      MIN(grouping("cooperative"."county")) asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Nothing was executed: parsed 5 definition statement(s) (2 concepts, 2 imports, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query23.preql --import raw.all_sales:sales --import raw.store_sales:ss select sales.item.id limit 5;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query24.preql`

  ```text
  Unexpected error in query24.preql: Unsupported datatype NUMBER for parameter zero.
  ```
- `trilogy file read query24.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: Undefined concept: web_returns.billing_customer. Suggestions: ['web_returns.web_sales.billing_customer.demographics.id', 'web_returns.web_sales.billing_customer.demographics.gender', 'web_returns.web_sales.billing_customer.demographics.marital_status', 'web_returns.web_sales.billing_customer.demographics.education_status', 'web_returns.web_sales.billing_customer.demographics.purchase_estimate', 'web_returns.web_sales.billing_customer.demographics.credit_rating']
  ```
- `trilogy run --import raw.web_returns:web_returns select billing_customer.id limit 3;`

  ```text
  Syntax error in stdin: Undefined concept: billing_customer.id (line 2, col 8, in SELECT). Suggestions: ['web_returns.web_sales.billing_customer.demographics.id', 'web_returns.web_sales.billing_customer.household_demographic.income_band.id', 'web_returns.web_sales.billing_customer.household_demographic.id', 'web_returns.web_sales.billing_customer.address.id', 'web_returns.web_sales.billing_customer.first_sales_date.id', 'web_returns.web_sales.billing_customer.first_shipto_date.id']
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.web_returns:web_returns select web_returns.web_sales.return_address.state limit 3;`

  ```text
  Syntax error in stdin: Undefined concept: web_returns.web_sales.return_address.state (line 2, col 8, in SELECT). Suggestions: ['web_returns.web_sales.return_customer.address.state', 'web_returns.web_sales.ship_address.state', 'web_returns.web_sales.bill_address.state', 'web_returns.store.state', 'web_returns.web_sales.billing_customer.address.state', 'web_returns.web_sales.ship_customer.address.state']
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: Undefined concept: web_returns.web_sales.billing_customer. Suggestions: ['web_returns.web_sales.billing_customer.demographics.id', 'web_returns.web_sales.billing_customer.demographics.gender', 'web_returns.web_sales.billing_customer.demographics.marital_status', 'web_returns.web_sales.billing_customer.demographics.education_status', 'web_returns.web_sales.billing_customer.demographics.purchase_estimate', 'web_returns.web_sales.billing_customer.demographics.credit_rating']
  ```
- `trilogy run query30.preql`

  ```text
  Syntax error in query30.preql: 11 undefined concept references; fix all before re-running:
    - salutation (line 25, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.salutation, web_returns.web_sales.ship_customer.salutation, web_returns.web_sales.return_customer.salutation, web_returns.web_sales.return_refund_customer.salutation, web_returns.billing_customer.salutation, web_returns.refunded_customer.salutation?
    - first_name (line 26, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.first_name, web_returns.web_sales.ship_customer.first_name, web_returns.web_sales.return_customer.first_name, web_returns.web_sales.return_refund_customer.first_name, web_returns.billing_customer.first_name, web_returns.refunded_customer.first_name?
    - last_name (line 27, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.last_name, web_returns.web_sales.ship_customer.last_name, web_returns.web_sales.return_customer.last_name, web_returns.web_sales.return_refund_customer.last_name, web_returns.billing_customer.last_name, web_returns.refunded_customer.last_name?
    - preferred_cust_flag (line 28, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.preferred_cust_flag, web_returns.web_sales.ship_customer.preferred_cust_flag, web_returns.web_sales.return_customer.preferred_cust_flag, web_returns.web_sales.return_refund_customer.preferred_cust_flag, web_returns.billing_customer.preferred_cust_flag, web_returns.refunded_customer.preferred_cust_flag?
    - birth_day (line 29, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_day, web_returns.web_sales.ship_customer.birth_day, web_returns.web_sales.return_customer.birth_day, web_returns.web_sales.return_refund_customer.birth_day, web_returns.billing_customer.birth_day, web_returns.refunded_customer.birth_day?
    - birth_month (line 30, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_month, web_returns.web_sales.ship_customer.birth_month, web_returns.web_sales.return_customer.birth_month, web_returns.web_sales.return_refund_customer.birth_month, web_returns.billing_customer.birth_month, web_returns.refunded_customer.birth_month?
    - birth_year (line 31, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_year, web_returns.web_sales.ship_customer.birth_year, web_returns.web_sales.return_customer.birth_year, web_returns.web_sales.return_refund_customer.birth_year, web_returns.billing_customer.birth_year, web_returns.refunded_customer.birth_year?
    - birth_country (line 32, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.birth_country, web_returns.web_sales.ship_customer.birth_country, web_returns.web_sales.return_customer.birth_country, web_returns.web_sales.return_refund_customer.birth_country, web_returns.billing_customer.birth_country, web_returns.refunded_customer.birth_country?
    - login (line 33, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.login, web_returns.web_sales.ship_customer.login, web_returns.web_sales.return_customer.login, web_returns.web_sales.return_refund_customer.login, web_returns.billing_customer.login, web_returns.refunded_customer.login?
    - email_address (line 34, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.email_address, web_returns.web_sales.ship_customer.email_address, web_returns.web_sales.return_customer.email_address, web_returns.web_sales.return_refund_customer.email_address, web_returns.billing_customer.email_address, web_returns.refunded_customer.email_address?
    - last_review_date (line 35, col 10, in ORDER BY); did you mean: web_returns.web_sales.billing_customer.last_review_date, web_returns.web_sales.ship_customer.last_review_date, web_returns.web_sales.return_customer.last_review_date, web_returns.web_sales.return_refund_customer.last_review_date, web_returns.billing_customer.last_review_date, web_returns.refunded_customer.last_review_date?
  ```
- `trilogy file read query30.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query54.preql`

  ```text
  Syntax error in query54.preql: 2 undefined concept references; fix all before re-running:
    - store_totals.store_total (line 53, in SELECT); did you mean: store_totals.cust_id, store_total?
    - store_totals.cust_id (line 53, in SELECT); did you mean: store_totals.store_total, store_total, cust_id?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 3088 (char 3087). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query59.preql`

  ```text
  Resolution error in query59.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 16). The requested concepts split into 2 disconnected subgraphs: {_weekly_friday_price, _weekly_monday_price, _weekly_saturday_price, _weekly_store_code, _weekly_store_name, _weekly_sunday_price, _weekly_thursday_price, _weekly_tuesday_price, _weekly_wednesday_price}; {_weekly_week_seq, _weekly_year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql duckdb`

  ```text
  Syntax error in query64.preql: Undefined concept: y1999.agg_sales.sale_year. Suggestions: ['agg_sales.sale_line_count', 'agg_sales.wholesale_sum', 'store_lines.sale_year', 'matched_lines.store_lines.sale_year', 'agg_sales.matched_lines.store_lines.sale_year']
  ```
- `trilogy run query64.preql duckdb`

  ```text
  Syntax error in query64.preql: Undefined concept: y1999.agg_sales.sale_year. Suggestions: ['agg_sales.sale_line_count', 'agg_sales.wholesale_sum', 'store_lines.sale_year', 'matched_lines.store_lines.sale_year', 'agg_sales.matched_lines.store_lines.sale_year']
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query74.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run raw/all_sales.preql duckdb --param limit=5`

  ```text
  Syntax error in raw\all_sales.preql: Nothing was executed: parsed 31 definition statement(s) (12 datasources, 11 imports, 5 concepts, 3 propertys) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```
- `trilogy run --import raw/store_returns:sr --import raw/catalog_returns:cr --import raw/web_returns:wr select count(sr.item.id) as sr_count, count(cr.item.id)…e sr.return_date.week_seq in (5244, 5257, 5264) and cr.date.week_seq in (5244, 5257, 5264) and wr.return_date.week_seq in (5244, 5257, 5264) limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 3 disconnected subgraphs: {cr.date.week_seq}; {sr.return_date.week_seq}; {wr.return_date.week_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw/all_sales:s 
# Store returns
with store_ret as
where s.return_date.week_seq in (5244, 5257, 5264) and s.is_returned = true and s.cha… store_ret.store_rows > 0
  and catalog_ret.catalog_rows > 0
  and web_ret.web_rows > 0
order by store_ret.item_code, store_ret.store_qty
limit 100;
`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 33). The requested concepts split into 3 disconnected subgraphs: {catalog_ret.catalog_qty, catalog_ret.catalog_rows}; {store_ret.item_code, store_ret.store_qty, store_ret.store_rows}; {web_ret.web_qty, web_ret.web_rows}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query84.preql`

  ```text
  Traceback (most recent call last):
    File "<frozen runpy>", line 189, in _run_module_as_main
    File "<frozen runpy>", line 112, in _get_module_details
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\__init__.py", line 4, in <module>
      from trilogy.executor import Executor
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\executor.py", line 61, in <module>
      from trilogy.core.validation.common import (
          ValidationTest,
      )
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\core\validation\common.py", line 5, in <module>
      from trilogy.authoring import (
      ...<4 lines>...
      )
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\authoring\__init__.py", line 11, in <module>
      from trilogy.core.functions import FunctionFactory
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\core\functions.py", line 1196, in <module>
      raise InvalidSyntaxException(
          f"Function enum value {k} not in creation registry"
      )
  trilogy.core.exceptions.InvalidSyntaxException: Function enum value FunctionType.CONCAT_STRICT not in creation registry
  ```
- `trilogy file read query87.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/time.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:s select s.date.week_seq, s.date.year, min(s.date.day_of_week) as min_dow group by 1,2 having s.date.week_seq in (5323, 5324) order by s.date.week_seq;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ....date.day_of_week) as min_dow ??? group by 1,2 having s.date.wee...
  ```
- `trilogy file write query02.preql --content import raw.all_sales as s;

# Rowset: daily sales for web and catalog only, across ALL years
rowset daily_sales_da…   sum(ratio ? daily_sales_data.dow = 5) as fri_ratio,
       sum(ratio ? daily_sales_data.dow = 6) as sat_ratio
order by daily_sales_data.week_seq;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:1
     |
  20 | daily_sales_data.dow = daily_sales_data.dow
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...: filter to 2001 weeks, pivot ??? daily_sales_data.dow = daily_s...

  Write stats: received 1215 chars / 1215 bytes; tail: …'a.dow = 6) as sat_ratio\\norder by daily_sales_data.week_seq;\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as s;

# Combine sales and return entities via union arms, then rollup
with combined as union…s), 0) as net_profit
by rollup (combined.channel, combined.entity_id)
order by channel_label asc nulls first, entity_label asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...rn_amt, sale_profit, ret_loss) ???  select     case         when...

  Write stats: received 2000 chars / 2000 bytes; tail: …'bel asc nulls first, entity_label asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query06.preql --content import raw.store_sales as ss;

# Distinct item prices per category (one row per item.id)
auto item_price_by_cat <-…m) as line_item_count
having
    line_item_count >= 10
order by line_item_count asc nulls first, ss.customer.address.state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ..._cat <- ss.item.current_price ??? by ss.item.id, ss.item.categor...

  Write stats: received 698 chars / 698 bytes; tail: …' first, ss.customer.address.state asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Find (brand_id, class_id, category_id) combos appearing in all 3 channels during… nulls first,
    sales.item.brand_id asc nulls first,
    sales.item.class_id asc nulls first,
    sales.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset qualifying_combos as intersect(
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...l 3 channels during 1999-2001 ??? rowset qualifying_combos as in...

  Hint: the `by rollup/cube/grouping sets` clause must come *before* HAVING in Trilogy (same order as SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> by rollup (<keys>) having <cond> order by <cols> limit <n>;

  Write stats: received 1804 chars / 1804 bytes; tail: …'first,\\n    sales.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Find (brand_id, class_id, category_id) combos appearing in all 3 channels during… nulls first,
    sales.item.brand_id asc nulls first,
    sales.item.class_id asc nulls first,
    sales.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 28:1
     |
  28 | by rollup (sales.channel, sales.item.brand_id, sales.item.class_id, sales.item.category_id)
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ing total_sales > overall_avg ??? by rollup (sales.channel, sale...

  Hint: the `by rollup/cube/grouping sets` clause must come *before* HAVING in Trilogy (same order as SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> by rollup (<keys>) having <cond> order by <cols> limit <n>;

  Write stats: received 1802 chars / 1802 bytes; tail: …'first,\\n    sales.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Find (brand_id, class_id, category_id) combos appearing in all 3 channels during… nulls first,
    sales.item.brand_id asc nulls first,
    sales.item.class_id asc nulls first,
    sales.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:1
     |
  33 | by rollup (sales.channel, sales.item.brand_id, sales.item.class_id, sales.item.category_id)
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...verall_avg_rowset.overall_avg ??? by rollup (sales.channel, sale...

  Hint: the `by rollup/cube/grouping sets` clause must come *before* HAVING in Trilogy (same order as SQL's `GROUP BY ... HAVING`). Reorder to:
      select <cols> by rollup (<keys>) having <cond> order by <cols> limit <n>;

  Write stats: received 1879 chars / 1879 bytes; tail: …'first,\\n    sales.item.category_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query16.preql --content import raw/catalog_sales as cs;

# Orders that ship from multiple warehouses (at least 2 distinct warehouses)
auto…der_number,
    sum(cs.ext_ship_cost) as total_ext_ship_cost,
    sum(cs.net_profit) as total_net_profit
order by
    cs.order_number desc
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
  import ??? raw/catalog_sales as cs;  # Or...

  Write stats: received 772 chars / 772 bytes; tail: …'otal_net_profit\\norder by\\n    cs.order_number desc\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.all_sales as sales;
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales …roup by sales.item.id, first30_desc, sales.date.date
  having count(sales.sale_line_item_counter) > 4
);

# Hmm, let me rethink and write more simply`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:3
     |
  14 |   select sales.item.id
     |   ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...in (2000, 2001, 2002, 2003)   ??? select sales.item.id   group b...

  Write stats: received 699 chars / 699 bytes; tail: …'counter) > 4\\n);\\n\\n# Hmm, let me rethink and write more simply'.
  ```
- `trilogy file write query23.preql -e -c import raw.all_sales as sales;

# Frequent items: items where count of (item.id, first30(desc), sold date) > 4
with fr… 1, 30) as desc_prefix,
    sales.date.date as sold_date,
    count(*) as cnt
having count(*) > 4
;

select frequent_items_triples.item_id
limit 10;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...s.date.date as sold_date,     ??? count(*) as cnt having count(*...

  Write stats: received 408 chars / 408 bytes; tail: …'t(*) > 4\\n;\\n\\nselect frequent_items_triples.item_id\\nlimit 10;\\n'.
  ```
- `trilogy file write query23.preql -e -c import raw.all_sales as sales;
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as w…ct
    sales.item.id as item_id,
    sales.item.desc as item_desc,
    sales.date.date as sold_date,
    triple_count as cnt
where is_freq
limit 20;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...sales.sale_line_item_counter) ??? by sales.item.id, substring(sa...

  Write stats: received 667 chars / 667 bytes; tail: …' sold_date,\\n    triple_count as cnt\\nwhere is_freq\\nlimit 20;\\n'.
  ```
- `trilogy file write query23.preql -e -c import raw.all_sales as sales;
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as w…ct
    sales.item.id as item_id,
    sales.item.desc as item_desc,
    sales.date.date as sold_date,
    triple_count as cnt
where is_freq
limit 20;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...sales.sale_line_item_counter) ??? by sales.item.id, (substring(s...

  Write stats: received 669 chars / 669 bytes; tail: …' sold_date,\\n    triple_count as cnt\\nwhere is_freq\\nlimit 20;\\n'.
  ```
- `trilogy file write query41.preql`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | ;
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...oss' and item.size = 'N/A');
   ??? ;

   # Get manufacturer text v...

  Write stats: received 1644 chars / 1644 bytes; tail: …'ile_manufacts\r\\norder by\r\\n    item.product_name\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query41.preql`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...one profile item
   select
       ??? distinct item.product_name
   wh...

  Write stats: received 1439 chars / 1439 bytes; tail: …'ile_manufacts\r\\norder by\r\\n    item.product_name\r\\nlimit 100;\r\\n'.
  ```
- `trilogy run --import raw/store_sales:ss select count(ss.customer.id) as cnt where ss.date.month_seq between 1188 and 1190 and ss.customer.id in (select cs.bi…r.id from raw/web_sales.preql as ws where ws.item.category='Women' and ws.item.class='maternity' and ws.date.year=1998 and ws.date.month_of_year=12);`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...select cs.billing_customer.id ??? from raw/catalog_sales.preql a...
  ```
- `trilogy run --import raw.date:date select distinct date.day_name, date.day_of_week order by date.day_of_week;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.date as date; select ??? distinct date.day_name, date.d...
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r…
    y1999.product_name asc,
    y1999.store_name asc,
    y2000.cnt_2000 asc,
    y1999.wholesale_1999 asc,
    y2000.wholesale_2000 asc
limit 100;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...g_sales: total ext_list_price ??? rowset cat_sales_item as selec...

  Write stats: received 7130 chars / 7130 bytes; tail: …'wholesale_1999 asc,\\n    y2000.wholesale_2000 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r…
    y1999.product_name asc,
    y1999.store_name asc,
    y2000.cnt_2000 asc,
    y1999.wholesale_1999 asc,
    y2000.wholesale_2000 asc
limit 100;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset cat_sales_item as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...g_sales: total ext_list_price ??? rowset cat_sales_item as selec...

  Write stats: received 6090 chars / 6090 bytes; tail: …'wholesale_1999 asc,\\n    y2000.wholesale_2000 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as ss;

# Top 5 states by store net profit in year 2000, considering only known stores
with…llup (ss.store.state, ss.store.county)
having hierarchy_level >= 0
order by
    hierarchy_level desc,
    ss.store.state asc,
    rnk asc
limit 100
;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:28
     |
  22 |     grouping(ss.store.state, ss.store.county) as hierarchy_level,
     |                            ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t,     grouping(ss.store.state ??? , ss.store.county) as hierarch...

  Write stats: received 1086 chars / 1086 bytes; tail: …'_level desc,\\n    ss.store.state asc,\\n    rnk asc\\nlimit 100\\n;'.
  ```
- `trilogy run --import raw/all_sales:all select all.date.year, count(*) as cnt where all.item.category = 'Books' limit 10;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...as all; select all.date.year, ??? count(*) as cnt where all.item...
  ```
- `trilogy run --import raw/all_sales:all select all.date.year, count(all.order_id) as cnt where all.item.category = 'Books' group by all.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...e all.item.category = 'Books' ??? group by all.date.year;
  ```
- `trilogy file write query75.preql --content import raw/all_sales as all;

# Deduplicated line-level records for Books items, with net values
with lines as
whe…yr2002.sum_qty is not null
    and cast(yr2002.sum_qty as float) / cast(yr2001.sum_qty as float) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as all;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as all;  # Dedup...

  Write stats: received 1859 chars / 1859 bytes; tail: …' float) < 0.9\\norder by qty_diff asc, amt_diff asc\\nlimit 100;'.
  ```
- `trilogy file write query76.preql -e -c import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as catalog;

with combined as u…ined.missing_ref asc nulls first,
  combined.year asc nulls first,
  combined.quarter asc nulls first,
  combined.category asc nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ???  select   combined.channel,

  Write stats: received 1709 chars / 1709 bytes; tail: …' nulls first,\\n  combined.category asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…eturns,
  combined.profit
by rollup (combined.channel, combined.outlet)
order by combined.channel nulls first, combined.outlet nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...turns numeric, profit numeric) ???  select   combined.channel,

  Write stats: received 2071 chars / 2071 bytes; tail: …'.channel nulls first, combined.outlet nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/date:date select date.week_seq, min(date.date) as week_start, max(date.date) as week_end where date.week_seq in (5244, 5257, 5264) group by date.week_seq order by date.week_seq;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eek_seq in (5244, 5257, 5264) ??? group by date.week_seq order b...
  ```
- `trilogy run --import raw/store_returns:sr select count(*) as cnt where sr.return_date.week_seq in (5244, 5257, 5264);`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...w.store_returns as sr; select ??? count(*) as cnt where sr.retur...
  ```
- `trilogy file write query83.preql --content import raw/all_sales as s;

# Store channel returns, aggregated at item level
with store_ret as
where s.channel = …atalog_ret.catalog_row_count > 0
  and web_ret.web_row_count > 0
order by store_ret.item_code nulls first, store_ret.store_qty nulls first
limit 100;`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as s;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as s;  # Store c...

  Write stats: received 2224 chars / 2224 bytes; tail: …'code nulls first, store_ret.store_qty nulls first\\nlimit 100;'.
  ```
- `trilogy file write query84.preql --content import raw.customer as c;
import raw.store_returns as sr;

# Join on demographic match — the return's point-in-tim… as customer_code,
  concat(coalesce(c.last_name, ''), ', ', coalesce(c.first_name, '')) as full_name
order by
  customer_code nulls first
limit 100;`

  ```text
  refused to write 'query84.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:1
    |
  9 | where
    | ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...r: customer lives in Edgewood ??? where    c.address.city = 'Edg...

  Write stats: received 614 chars / 616 bytes; tail: …'as full_name\\norder by\\n  customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Derive the (last_name…t_name,
    ss.customer.first_name as first_name,
    ss.date.date as sale_date
;

select
    count(ss_only.last_name) as unique_combination_count
;
`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:29
    |
  7 | auto ss_key <- concat(isnull(ss.customer.last_name, '<NULL>'), '|',
    |                             ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...0 auto ss_key <- concat(isnull ??? (ss.customer.last_name, '<NULL...

  Write stats: received 1162 chars / 1162 bytes; tail: …'\\n    count(ss_only.last_name) as unique_combination_count\\n;\\n'.
  ```
- `trilogy file write query87_test.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

select
    count_d…lling_customer.id
subset join ss.customer.id = ws.billing_customer.id
subset join ss.date.id = cs.sold_date.id
subset join ss.date.id = ws.date.id
;
`

  ```text
  refused to write 'query87_test.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:1
     |
  25 | subset join ss.customer.id = cs.billing_customer.id
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...0     and ws.date.year = 2000 ??? subset join ss.customer.id = c...

  Write stats: received 1014 chars / 1014 bytes; tail: …'.id = cs.sold_date.id\\nsubset join ss.date.id = ws.date.id\\n;\\n'.
  ```

### `cli-misuse`

- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy trilogy agent-info syntax example scoped-join`

  ```text
  No such command 'trilogy'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.store_sales:store_sales select store_sales.date.year, min(store_sales.date.week_seq), max(store_sales.date.week_seq) where store_sales.date.year in (2001, 2002) order by 1;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `min(store_sales.date.week_seq) as week_seq_min`
  Location:
  ...min(store_sales.date.week_seq) ??? , max(store_sales.date.week_se...
  ```
- `trilogy file write test.preql --content import raw.store_sales as ss;

rowset base <-
select
    ss.item.id as item_id,
    ss.date.year as yr
;

rowset filtered <-
where base.yr = 1999
select
    base.item_id
;

select count(filtered.item_id);
`

  ```text
  refused to write 'test.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(filtered.item_id) as item_id_count`
  Location:
  ...select count(filtered.item_id) ??? ;

  Write stats: received 205 chars / 205 bytes; tail: …'\\nselect\\n    base.item_id\\n;\\n\\nselect count(filtered.item_id);\\n'.
  ```

### `join-resolution`

- `trilogy run --import raw/catalog_sales:cs --import raw/web_sales:ws select count(cs.billing_customer.id) as cat_cust, count(ws.billing_customer.id) as web_cust where cs.item.category='Women' and cs.item.class='maternity' and cs.date.year=1998 and cs.date.month_of_year=12;`

  ```text
  Resolution error in stdin: Could not resolve connections for query with output ['local.cat_cust<Purpose.METRIC>Derivation.AGGREGATE>', 'local.web_cust<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
