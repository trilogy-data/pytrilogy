# Trilogy failure analysis — 20260708-135137

- Run `20260708-135136_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1290 | failed: 148 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 96 | 65% |
| `syntax-parse` | 38 | 26% |
| `syntax-missing-alias` | 4 | 3% |
| `cli-misuse` | 3 | 2% |
| `type-error` | 2 | 1% |
| `join-resolution` | 2 | 1% |
| `file-not-found` | 2 | 1% |
| `undefined-concept` | 1 | 1% |

## Detail

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 121 column 3 (char 3311). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query04.preql`

  ```text
  Syntax error in query04.preql: 15 undefined concept references; fix all before re-running:
    - local.s01_val (line 76, col 5, in WHERE); did you mean: w01_val, s02_val, c01_val, s01.s01_val?
    - local.s02_val (line 77, col 9, in WHERE); did you mean: w02_val, s01_val, c02_val, s02.s02_val?
    - local.c01_val (line 78, col 9, in WHERE); did you mean: w01_val, s01_val, c02_val, c01.c01_val?
    - local.c02_val (line 79, col 9, in WHERE); did you mean: w02_val, s02_val, c01_val, c02.c02_val?
    - local.w01_val (line 80, col 9, in WHERE); did you mean: w02_val, s01_val, c01_val, w01.w01_val?
    - local.w02_val (line 81, col 9, in WHERE); did you mean: w01_val, s02_val, c02_val, w02.w02_val?
    - local.s01_val (line 83, col 9, in WHERE); did you mean: w01_val, s02_val, c01_val, s01.s01_val?
    - local.c01_val (line 84, col 9, in WHERE); did you mean: w01_val, s01_val, c02_val, c01.c01_val?
    - local.w01_val (line 85, col 9, in WHERE); did you mean: w02_val, s01_val, c01_val, w01.w01_val?
    - local.s02_val (line 63, in WHERE); did you mean: w02_val, s01_val, c02_val, s02.s02_val?
    - local.s01_val (line 63, in WHERE); did you mean: w01_val, s02_val, c01_val, s01.s01_val?
    - local.c02_val (line 63, in WHERE); did you mean: w02_val, s02_val, c01_val, c02.c02_val?
    - local.c01_val (line 63, in WHERE); did you mean: w01_val, s01_val, c02_val, c01.c01_val?
    - local.w02_val (line 63, in WHERE); did you mean: w01_val, s02_val, c02_val, w02.w02_val?
    - local.w01_val (line 63, in WHERE); did you mean: w02_val, s01_val, c01_val, w01.w01_val?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id limit 5;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - all_sales.channel (line 2, col 8, in SELECT); did you mean: all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id, channel?
    - all_sales.channel_dim_text_id (line 2, col 27, in SELECT); did you mean: all_sales.return_channel_dim_text_id, all_sales.channel, channel_dim_text_id?
    - all_sales.return_channel_dim_text_id (line 2, col 58, in SELECT); did you mean: all_sales.channel_dim_text_id, all_sales.channel, return_channel_dim_text_id, return_channel_dim_id?
  ```
- `trilogy run --import raw/all_sales select s.date.date, s.return_date.date limit 5;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - s.date.date (line 2, col 8, in SELECT); did you mean: s.return_date.date, date.date, date.quarter, return_date.date, billing_customer.first_sales_date.date, billing_customer.first_shipto_date.date?
    - s.return_date.date (line 2, col 21, in SELECT); did you mean: s.date.date, return_date.date, return_date.id, return_date.quarter, date.date, billing_customer.first_sales_date.date?
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: SELECT output 'local.net_profit' is defined by an expression that references 'local.net_profit' itself (line 22). This is a recursive self-reference: an alias cannot redefine a name its own calculation reads. Rename the output to a distinct name (e.g. `... as net_profit_out`).
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: SELECT output 'local.net_profit' is defined by an expression that references 'local.net_profit' itself (line 22). This is a recursive self-reference: an alias cannot redefine a name its own calculation reads. Rename the output to a distinct name (e.g. `... as net_profit_out`).
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {cat_avg_price}; {line_item_count, store_sales.customer.address.sk, store_sales.customer.address.state, store_sales.date.month_of_year, store_sales.date.year, store_sales.item.category, store_sales.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: Undefined concept: qualifying_items.line_item (line 19, in SELECT). Suggestions: ['qualifying_items.state', 'qualifying_items.store_sales.item.sk', 'qualifying_items.store_sales.item.category', 'store_sales.line_item']
  ```
- `trilogy run query09.preql`

  ```text
  Syntax error in query09.preql: Undefined concept: local.by (line 33, in SELECT).
  ```
- `trilogy run query10.preql`

  ```text
  Syntax error in query10.preql: Duplicate select output for local.customer_count; Line: 28
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: date.year. Suggestions: ['store.date.year', 'web.date.year', 'store.store.date.year', 'store.return_store.date.year', 'web.ship_date.year', 'store.return_date.year']
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 53, col 5, in ORDER BY); did you mean: cust.first_name, store.customer.first_name, store.return_customer.first_name, web.billing_customer.first_name, web.ship_customer.first_name, web.return_customer.first_name?
    - last_name (line 54, col 5, in ORDER BY); did you mean: cust.last_name, cust.full_name, store.customer.last_name, store.return_customer.last_name, web.billing_customer.last_name, web.ship_customer.last_name?
    - preferred_cust_flag (line 55, col 5, in ORDER BY); did you mean: cust.preferred_cust_flag, store.customer.preferred_cust_flag, web.ship_customer.preferred_cust_flag, store.return_customer.preferred_cust_flag, web.billing_customer.preferred_cust_flag, web.return_customer.preferred_cust_flag?
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 53, col 5, in ORDER BY); did you mean: cust.first_name, store.customer.first_name, store.return_customer.first_name, web.billing_customer.first_name, web.ship_customer.first_name, web.return_customer.first_name?
    - last_name (line 54, col 5, in ORDER BY); did you mean: cust.last_name, cust.full_name, store.customer.last_name, store.return_customer.last_name, web.billing_customer.last_name, web.ship_customer.last_name?
    - preferred_cust_flag (line 55, col 5, in ORDER BY); did you mean: cust.preferred_cust_flag, store.customer.preferred_cust_flag, web.ship_customer.preferred_cust_flag, store.return_customer.preferred_cust_flag, web.billing_customer.preferred_cust_flag, web.return_customer.preferred_cust_flag?
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 55, col 5, in ORDER BY); did you mean: store_rev.first_name, _store_rev_first_name, store.customer.first_name, store.return_customer.first_name, web.billing_customer.first_name, web.ship_customer.first_name?
    - last_name (line 56, col 5, in ORDER BY); did you mean: store_rev.last_name, _store_rev_last_name, store.customer.last_name, store.return_customer.last_name, web.billing_customer.last_name, web.ship_customer.last_name?
    - preferred_cust_flag (line 57, col 5, in ORDER BY); did you mean: store_rev.preferred_cust_flag, _store_rev_preferred_cust_flag, store.customer.preferred_cust_flag, store.return_customer.preferred_cust_flag, web.billing_customer.preferred_cust_flag, web.ship_customer.preferred_cust_flag?
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 37). The requested concepts split into 2 disconnected subgraphs: {combined.cust_id, combined.store_2001, combined.store_2002, combined.web_2001, combined.web_2002}; {store_rev.cust_id, store_rev.first_name, store_rev.last_name, store_rev.preferred_cust_flag}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query12.preql`

  ```text
  Syntax error in query12.preql: 2 undefined concept references; fix all before re-running:
    - local.category (line 17, col 5, in ORDER BY); did you mean: ws.item.category?
    - local.class (line 18, col 5, in ORDER BY); did you mean: ws.item.class?
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 6 undefined concept references; fix all before re-running:
    - store_sales.customer_demographics.marital_status (line 9, col 8, in WHERE); did you mean: store_sales.customer_demographic.marital_status, store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status?
    - store_sales.customer_demographics.education_status (line 9, col 67, in WHERE); did you mean: store_sales.customer_demographic.education_status, store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status?
    - store_sales.customer_demographics.marital_status (line 10, col 11, in WHERE); did you mean: store_sales.customer_demographic.marital_status, store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status?
    - store_sales.customer_demographics.education_status (line 10, col 70, in WHERE); did you mean: store_sales.customer_demographic.education_status, store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status?
    - store_sales.customer_demographics.marital_status (line 11, col 11, in WHERE); did you mean: store_sales.customer_demographic.marital_status, store_sales.customer.demographics.marital_status, store_sales.return_customer.demographics.marital_status?
    - store_sales.customer_demographics.education_status (line 11, col 70, in WHERE); did you mean: store_sales.customer_demographic.education_status, store_sales.customer.demographics.education_status, store_sales.return_customer.demographics.education_status?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Nothing was executed: parsed 8 definition statement(s) (4 imports, 4 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Nothing was executed: parsed 9 definition statement(s) (5 rowsets, 3 imports, 1 concept) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 82). The requested concepts split into 2 disconnected subgraphs: {channel_groups.brand_id, channel_groups.category_id, channel_groups.channel, channel_groups.class_id, channel_groups.sale_count, channel_groups.total_sales}; {_virt_agg_sum_3128605219700023, _virt_agg_sum_4241845383485580, _virt_agg_sum_4564182496557734, _virt_agg_sum_5875394234911310, _virt_agg_sum_6054272814551056, _virt_agg_sum_7410683025691259}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query15.preql`

  ```text
  Syntax error in query15.preql: Comparison `cs.sold_date.quarter = 'Q2'` can never match enum field 'cs.sold_date.quarter', which contains only these values: 1, 2, 3, 4. It is always false and should be removed.
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query18.preql duckdb`

  ```text
  Syntax error in query18.preql: ORDER BY contains aggregate `grouping(cs.billing_customer.address.country)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(cs.billing_customer.address.country) as g order by g desc`.
  ```
- `trilogy run query18.preql duckdb`

  ```text
  Unexpected error in query18.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  questionable as (
  SELECT
      "cs_billing_customer_address_customer_address"."CA_COUNTRY" as "cs_billing_customer_address_country",
      "cs_billing_customer_address_customer_address"."CA_COUNTY" as "cs_billing_customer_address_county",
      "cs_billing_customer_address_customer_address"."CA_STATE" as "cs_billing_customer_address_state",
      "cs_billing_customer_customers"."C_BIRTH_YEAR" as "cs_billing_customer_birth_year",
      "cs_billing_customer_demographic_customer_demographics"."CD_DEP_COUNT" as "cs_billing_customer_demographic_dependent_count",
      "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
      "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
      "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price",
      "cs_item_items"."I_ITEM_ID" as "cs_item_id"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "cs_billing_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_billing_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "cs_billing_customer_address_customer_address" on "cs_billing_customer_customers"."C_CURRENT_ADDR_SK" = "cs_billing_customer_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "cs_billing_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_billing_customer_demographic_customer_demographics"."CD_DEMO_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 1998 and "cs_billing_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_billing_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_billing_customer_customers"."C_BIRTH_MONTH" in (1,2,6,8,9,12) and "cs_billing_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')
  )
  SELECT
      "questionable"."cs_item_id" as "item_code",
      "questionable"."cs_billing_customer_address_country" as "country",
      "questionable"."cs_billing_customer_address_state" as "state",
      "questionable"."cs_billing_customer_address_county" as "county",
      avg("questionable"."cs_quantity") as "avg_quantity",
      avg("questionable"."cs_list_price") as "avg_list_price",
      avg("questionable"."cs_coupon_amt") as "avg_coupon_amt",
      avg("questionable"."cs_sales_price") as "avg_sales_price",
      avg("questionable"."cs_net_profit") as "avg_net_profit",
      avg("questionable"."cs_billing_customer_birth_year") as "avg_birth_year",
      avg("questionable"."cs_billing_customer_demographic_dependent_count") as "avg_dependent_count"
  FROM
      "questionable"
  GROUP BY
      ROLLUP (2, 3, 4, 1)
  ORDER BY
      "country" asc,
      "state" asc,
      "county" asc,
      "item_code" asc,
      MIN(grouping("questionable"."cs_billing_customer_address_country")) asc,
      MIN(grouping("questionable"."cs_billing_customer_address_state")) asc,
      MIN(grouping("questionable"."cs_billing_customer_address_county")) asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query18.preql duckdb`

  ```text
  Unexpected error in query18.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  questionable as (
  SELECT
      "cs_billing_customer_address_customer_address"."CA_COUNTRY" as "cs_billing_customer_address_country",
      "cs_billing_customer_address_customer_address"."CA_COUNTY" as "cs_billing_customer_address_county",
      "cs_billing_customer_address_customer_address"."CA_STATE" as "cs_billing_customer_address_state",
      "cs_billing_customer_customers"."C_BIRTH_YEAR" as "cs_billing_customer_birth_year",
      "cs_billing_customer_demographic_customer_demographics"."CD_DEP_COUNT" as "cs_billing_customer_demographic_dependent_count",
      "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
      "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
      "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
      "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
      "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price",
      "cs_item_items"."I_ITEM_ID" as "cs_item_id"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      INNER JOIN "customer" as "cs_billing_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_billing_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_address" as "cs_billing_customer_address_customer_address" on "cs_billing_customer_customers"."C_CURRENT_ADDR_SK" = "cs_billing_customer_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "cs_billing_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_billing_customer_demographic_customer_demographics"."CD_DEMO_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" = 1998 and "cs_billing_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_billing_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_billing_customer_customers"."C_BIRTH_MONTH" in (1,2,6,8,9,12) and "cs_billing_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')
  )
  SELECT
      "questionable"."cs_item_id" as "item_code",
      "questionable"."cs_billing_customer_address_country" as "country",
      "questionable"."cs_billing_customer_address_state" as "state",
      "questionable"."cs_billing_customer_address_county" as "county",
      avg("questionable"."cs_quantity") as "avg_quantity",
      avg("questionable"."cs_list_price") as "avg_list_price",
      avg("questionable"."cs_coupon_amt") as "avg_coupon_amt",
      avg("questionable"."cs_sales_price") as "avg_sales_price",
      avg("questionable"."cs_net_profit") as "avg_net_profit",
      avg("questionable"."cs_billing_customer_birth_year") as "avg_birth_year",
      avg("questionable"."cs_billing_customer_demographic_dependent_count") as "avg_dependent_count"
  FROM
      "questionable"
  GROUP BY
      ROLLUP (2, 3, 4, 1)
  ORDER BY
      "country" asc,
      "state" asc,
      "county" asc,
      "item_code" asc,
      MIN(grouping_id("questionable"."cs_billing_customer_address_country","questionable"."cs_billing_customer_address_state","questionable"."cs_billing_customer_address_county")) asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query20.preql`

  ```text
  Syntax error in query20.preql: Output column 'pct_of_class' renames 'local.pct_of_class' back to the name of an existing concept 'pct_of_class' (defined at line 10) that 'local.pct_of_class' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'pct_of_class_out').
  ```
- `trilogy run query21.preql`

  ```text
  Resolution error in query21.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 3 disconnected subgraphs: {item.current_price, item_code}; {after_total, before_total}; {warehouse_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query22.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query25.preql`

  ```text
  Syntax error in query25.preql: 4 undefined concept references; fix all before re-running:
    - local.item_id (line 42, col 10, in ORDER BY); did you mean: item_desc, ss.item.id, cs.item.id, ss_filtered.item_id, cs_filtered.item_id?
    - local.item_desc (line 42, col 19, in ORDER BY); did you mean: ss.item.desc, cs.item.desc, item_id, ss_filtered.item_desc?
    - local.store_code (line 42, col 30, in ORDER BY); did you mean: store_name, ss_filtered.store_code, _ss_filtered_store_code?
    - local.store_name (line 42, col 42, in ORDER BY); did you mean: ss.store.name, store_code, ss.store.street_name, ss_filtered.store_name?
  ```
- `trilogy run query29.preql`

  ```text
  Resolution error in query29.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['cs.item.id', 'cs.sold_date.year', 'ss.date.month_of_year', 'ss.date.year', 'ss.item.desc', 'ss.item.sk', 'ss.quantity', 'ss.return_date.month_of_year', 'ss.return_date.year', 'ss.return_quantity', 'ss.store.id', 'ss.store.name', 'ss.ticket_number', 'cs.sold_date.sk', 'ss.date.sk', 'ss.return_date.sk', 'ss.store.sk'], have {'cs.item.sk'} or {'cs.billing_customer.sk', 'ss.customer.sk', 'cs.billing_customer.id'}. Different paths are is: [{'cs.item.sk'}, {'cs.billing_customer.sk', 'ss.customer.sk', 'cs.billing_customer.id'}]")
  ```
- `trilogy run query31.preql`

  ```text
  Resolution error in query31.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 23). The requested concepts split into 2 disconnected subgraphs: {store_by_county_q.ss.date.quarter, store_by_county_q.ss.sale_address.county, store_by_county_q.store_ext_price}; {web_by_county_q.web_ext_price, web_by_county_q.ws.bill_address.county, web_by_county_q.ws.date.quarter}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cs.item.current_price, cs.item.manufacturer_id, cs.item.sk, current_price, item_code, item_description}; {inv.date.date, inv.item.sk, inv.quantity_on_hand}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {feb.qualifying.cv, feb.qualifying.stats.item_id, feb.qualifying.stats.mn, feb.qualifying.stats.mo, feb.qualifying.stats.warehouse_sk, jan.qualifying.cv, jan.qualifying.stats.item_id, jan.qualifying.stats.mn, jan.qualifying.stats.mo, jan.qualifying.stats.warehouse_sk}
  ```
- `trilogy file read query39.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql --import raw.store:store select store.sk, store.id, store.name where store.sk = 1 limit 5;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: 2 undefined concept references; fix all before re-running:
    - best_name (line 43, col 5, in ORDER BY); did you mean: best_10.best_name, _best_10_best_name, best_10.rnk?
    - worst_name (line 44, col 5, in ORDER BY); did you mean: worst_10.worst_name, _worst_10_worst_name, worst_10.rnk?
  ```
- `trilogy run query44.preql`

  ```text
  Resolution error in query44.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 36). The requested concepts split into 2 disconnected subgraphs: {best_10.rnk, best_10.ss.item.product_name}; {worst_10.rnk, worst_10.ss.item.product_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query46.preql`

  ```text
  Syntax error in query46.preql: 15 undefined concept references; fix all before re-running:
    - customer.last_name (line 10, col 3, in SELECT); did you mean: store_sales.customer.last_name, customer.first_name, store_sales.return_customer.last_name?
    - customer.first_name (line 11, col 3, in SELECT); did you mean: store_sales.customer.first_name, customer.last_name, store_sales.return_customer.first_name?
    - local.ticket_number (line 14, col 3, in SELECT); did you mean: store_sales.ticket_number?
    - local.coupon_amt (line 3, in SELECT); did you mean: total_coupon_amt, store_sales.coupon_amt?
    - local.net_profit (line 3, in SELECT); did you mean: total_net_profit, store_sales.net_profit?
    - date.day_name (line 4, col 3, in WHERE); did you mean: store_sales.date.day_name, store_sales.store.date.day_name, store_sales.return_store.date.day_name, date.year, store_sales.return_date.day_name, store_sales.customer.first_sales_date.day_name?
    - date.year (line 5, col 7, in WHERE); did you mean: store_sales.date.year, store_sales.store.date.year, store_sales.return_store.date.year, store_sales.return_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.first_shipto_date.year?
    - store.city (line 6, col 7, in WHERE); did you mean: store_sales.store.city, customer_city, customer.address.city, sale_city, store_sales.customer.address.city, store_sales.return_customer.address.city?
    - sale_household_demographic.dependent_count (line 7, col 8, in WHERE); did you mean: store_sales.sale_household_demographic.dependent_count, sale_household_demographic.vehicle_count, store_sales.customer.household_demographic.dependent_count, store_sales.customer.demographics.dependent_count, store_sales.return_customer.demographics.dependent_count, store_sales.return_customer.household_demographic.dependent_count?
    - sale_household_demographic.vehicle_count (line 7, col 58, in WHERE); did you mean: store_sales.sale_household_demographic.vehicle_count, sale_household_demographic.dependent_count, store_sales.customer.household_demographic.vehicle_count, store_sales.return_customer.household_demographic.vehicle_count?
    - customer.address.city (line 8, col 7, in WHERE); did you mean: store_sales.customer.address.city, customer.last_name, sale_address.city, store_sales.customer.address.county, store_sales.return_customer.address.city, store_sales.store.city?
    - sale_address.city (line 8, col 32, in WHERE); did you mean: store_sales.sale_address.city, customer.address.city, sale_city, store_sales.customer.address.city, store_sales.return_customer.address.city, store_sales.store.city?
    - customer.last_name (line 18, col 3, in ORDER BY); did you mean: store_sales.customer.last_name, customer.first_name, store_sales.return_customer.last_name?
    - customer.first_name (line 19, col 3, in ORDER BY); did you mean: store_sales.customer.first_name, customer.last_name, store_sales.return_customer.first_name?
    - local.ticket_number (line 22, col 3, in ORDER BY); did you mean: store_sales.ticket_number?
  ```
- `trilogy file read query46.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query49.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file write query53.preql -e -c`

  ```text
  --escapes only applies to an inline `--content <value>`; stdin already supports real newlines, so drop `-e` when piping content.
  ```
- `trilogy run query54.preql`

  ```text
  Syntax error in query54.preql: Undefined concept: web_sales.sold_date.month_seq. Suggestions: ['web_sales.date.month_seq', 'web_sales.ship_date.month_seq', 'web_sales.return_date.month_seq', 'catalog_sales.sold_date.month_seq', 'store_sales.date.month_seq', 'store_sales.return_date.month_seq']
  ```
- `trilogy run query57.preql`

  ```text
  Unexpected error in query57.preql: Join chain repeats source `monthly_totals` (keys `monthly_totals.cs.item.category` and `monthly_totals.cs.item.brand_name`). A `=` chain joins ONE key across distinct sources; join a composite key with `and` or separate clauses (e.g. `a.k1 = b.k1 and a.k2 = b.k2`).
  ```
- `trilogy file read query57.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run raw/test_week.preql`

  ```text
  Unexpected error in raw\test_week.preql: Unable to import 'raw\raw\all_sales.preql': [Errno 2] No such file or directory: 'raw\\raw\\all_sales.preql'. Did you mean: all_sales?
  ```
- `trilogy run raw/test_week.preql`

  ```text
  Unexpected error in raw\test_week.preql: Unable to import 'raw\raw\all_sales.preql': [Errno 2] No such file or directory: 'raw\\raw\\all_sales.preql'. Did you mean: all_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting value: line 1 column 59 (char 58). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query58.preql`

  ```text
  Unexpected error in query58.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy run query58.preql`

  ```text
  Syntax error in query58.preql: Output column 'store_total' renames 'local.store_total' back to the name of an existing concept 'store_total' (defined at line 6) that 'local.store_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'store_total_out').
  ```
- `trilogy run query58.preql`

  ```text
  Syntax error in query58.preql: Output column 'store_total' renames 'local.store_total' back to the name of an existing concept 'store_total' (defined at line 4) that 'local.store_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'store_total_out').
  ```
- `trilogy run query59.preql`

  ```text
  Syntax error in query59.preql: Undefined concept: local.this_year. Suggestions: ['this_year.wed', 'this_year.tue', 'this_year.thu']
  ```
- `trilogy file write --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Catalog: per-item cumulative …    sum(ss.wholesale_cost) as wholesale_cost_sum,
    sum(ss.list_price) as list_price_sum,
    sum(ss.coupon_amt) as coupon_amt_sum
;
 query64.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Compute per-ite…in agg_sales2 as agg_sales2
    where agg_sales2.ss_date.year = 2000
    and agg_sales2.sale_line_count <= agg_sales.sale_line_count
;
 query64.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 76 column 3 (char 2751). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: agg_sales.ss_date.year. Suggestions: ['agg_sales.ss.date.year', 'agg_sales.ss.item.sk', 'agg_sales.ss.store.name', 'cr.sales.sold_date.year', 'cr.sales.ship_date.year', 'ss.date.year']
  ```
- `trilogy run query64.preql`

  ```text
  Unexpected error in query64.preql: Could not find CTE for datasource ss.item.items_join_cr.catalog_returns_at_cr_item_sk_cr_order_number_join_cs.catalog_sales_at_cr_item_sk_cr_order_number_at_cr_item_sk_cr_order_number_grouped_by_cr.item.sk_local._cat_items_item_sk_at_cr_item_sk_at_cat_items_item_sk_grouped_by_local.qualifying_item_sk_at_local_qualifying_item_sk_filtered_by_2734787394810424_at_ss_item_sk_filtered_by_8610126078781587; have {'ss.item.items', 'cr.catalog_returns_at_cr_item_sk_cr_order_number_join_cs.catalog_sales_at_cr_item_sk_cr_order_number_at_cr_item_sk_cr_order_number_grouped_by_cr.item.sk_local._cat_items_item_sk_at_cr_item_sk_at_cat_items_item_sk_grouped_by_local.qualifying_item_sk_at_local_qualifying_item_sk_filtered_by_2734787394810424_join_ss.customer.address.customer_address_at_ss_customer_address_sk_join_ss.customer.customers_at_ss_customer_sk_join_ss.customer.demographics.customer_demographics_at_ss_customer_demographics_sk_join_ss.customer.first_sales_date.date_at_ss_customer_first_sales_date_sk_join_ss.customer.first_shipto_date.date_at_ss_customer_first_shipto_date_sk_join_ss.customer_demographic.customer_demographics_at_ss_customer_demographic_sk_join_ss.date.date_at_ss_date_sk_join_ss.item.items_at_ss_item_sk_join_ss.sale_address.customer_address_at_ss_sale_address_sk_join_ss.store.store_at_ss_store_sk_join_ss.store_returns_at_ss_item_sk_ss_ticket_number_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_grouped_by_ss.coupon_amt_ss.customer.address.city_ss.customer.address.street_name_ss.customer.address.street_number_ss.customer.address.zip_ss.customer.first_sales_date.year_ss.customer.first_shipto_date.year_ss.date.year_ss.item.sk_ss.line_item_ss.list_price_ss.sale_address.city_ss.sale_address.street_name_ss.sale_address.street_number_ss.sale_address.zip_ss.store.name_ss.store.zip_ss.ticket_number_ss.wholesale_cost_at_ss_item_sk_ss_ticket_number_filtered_by_9961812627629563_grouped_by_ss.customer.address.city_ss.customer.address.street_name_ss.customer.address.street_number_ss.customer.address.zip_ss.customer.first_sales_date.year_ss.customer.first_shipto_date.year_ss.date.year_ss.item.sk_ss.sale_address.city_ss.sale_address.street_name_ss.sale_address.street_number_ss.sale_address.zip_ss.store.name_ss.store.zip_at_ss_customer_address_city_ss_customer_address_street_name_ss_customer_address_street_number_ss_customer_address_zip_ss_customer_first_sales_date_year_ss_customer_first_shipto_date_year_ss_date_year_ss_item_sk_ss_sale_address_city_ss_sale_address_street_name_ss_sale_address_street_number_ss_sale_address_zip_ss_store_name_ss_store_zip', 'cr.catalog_returns_at_cr_item_sk_cr_order_number_join_cs.catalog_sales_at_cr_item_sk_cr_order_number_at_cr_item_sk_cr_order_number_grouped_by_cr.item.sk_local._cat_items_item_sk_at_cr_item_sk_at_cat_items_item_sk_grouped_by_local.qualifying_item_sk_at_local_qualifying_item_sk_filtered_by_2734787394810424_join_ss.item.items_at_ss_item_sk_filtered_by_8610126078781587'}
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Nothing was executed: parsed 6 definition statement(s) (3 imports, 2 rowsets, 1 concept) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.item.id', which is not in the SELECT projection (line 12). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.item.id order by ss.item.id asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query69.preql`

  ```text
  Syntax error in query69.preql: 11 undefined concept references; fix all before re-running:
    - customer.demographics.gender (line 22, col 3, in SELECT); did you mean: ss.customer.demographics.gender, customer.demographics.credit_rating, customer.demographics.education_status, customer.demographics.marital_status, ss.customer_demographic.gender, ws.ship_customer.demographics.gender?
    - customer.demographics.marital_status (line 23, col 3, in SELECT); did you mean: ss.customer.demographics.marital_status, customer.demographics.education_status, customer.demographics.credit_rating, customer.demographics.purchase_estimate, ss.customer_demographic.marital_status, ws.ship_customer.demographics.marital_status?
    - customer.demographics.education_status (line 24, col 3, in SELECT); did you mean: ss.customer.demographics.education_status, customer.demographics.marital_status, customer.demographics.purchase_estimate, customer.demographics.credit_rating, ss.customer_demographic.education_status, ws.ship_customer.demographics.education_status?
    - customer.demographics.purchase_estimate (line 25, col 3, in SELECT); did you mean: ss.customer.demographics.purchase_estimate, customer.demographics.credit_rating, customer.demographics.gender, customer.demographics.marital_status, ss.customer_demographic.purchase_estimate, ws.ship_customer.demographics.purchase_estimate?
    - customer.demographics.credit_rating (line 26, col 3, in SELECT); did you mean: ss.customer.demographics.credit_rating, customer.demographics.marital_status, customer.demographics.education_status, customer.demographics.gender, ss.customer_demographic.credit_rating, ws.ship_customer.demographics.credit_rating?
    - customer.address.state (line 10, col 7, in WHERE); did you mean: ss.customer.address.state, cs.customer_address.state, ws.ship_customer.address.state, ss.return_customer.address.state, ss.store.state, ss.return_store.state?
    - customer.demographics.gender (line 31, col 3, in ORDER BY); did you mean: ss.customer.demographics.gender, customer.demographics.credit_rating, customer.demographics.education_status, customer.demographics.marital_status, ss.customer_demographic.gender, ws.ship_customer.demographics.gender?
    - customer.demographics.marital_status (line 32, col 3, in ORDER BY); did you mean: ss.customer.demographics.marital_status, customer.demographics.education_status, customer.demographics.credit_rating, customer.demographics.purchase_estimate, ss.customer_demographic.marital_status, ws.ship_customer.demographics.marital_status?
    - customer.demographics.education_status (line 33, col 3, in ORDER BY); did you mean: ss.customer.demographics.education_status, customer.demographics.marital_status, customer.demographics.purchase_estimate, customer.demographics.credit_rating, ss.customer_demographic.education_status, ws.ship_customer.demographics.education_status?
    - customer.demographics.purchase_estimate (line 34, col 3, in ORDER BY); did you mean: ss.customer.demographics.purchase_estimate, customer.demographics.credit_rating, customer.demographics.gender, customer.demographics.marital_status, ss.customer_demographic.purchase_estimate, ws.ship_customer.demographics.purchase_estimate?
    - customer.demographics.credit_rating (line 35, col 3, in ORDER BY); did you mean: ss.customer.demographics.credit_rating, customer.demographics.marital_status, customer.demographics.education_status, customer.demographics.gender, ss.customer_demographic.credit_rating, ws.ship_customer.demographics.credit_rating?
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: ORDER BY references 'local.g_county', which is not in the SELECT projection (line 27). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.g_county order by local.g_county asc`.
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: ORDER BY references 'local.g_county', which is not in the SELECT projection (line 27). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.g_county order by local.g_county asc`.
  ```
- `trilogy run query71.preql`

  ```text
  Resolution error in query71.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {item.manager_id, brand_id, brand_name}; {hour, minute, total_ext_sales_price, sales.date.month_of_year, sales.date.year, sales.time.meal_time}.
    - `item.manager_id` is disconnected — did you mean `sales.item.manager_id`? (connected to the other concepts)
    - `local.brand_id` is disconnected — did you mean `sales.item.brand_id`? (connected to the other concepts)
    - `local.brand_name` is disconnected — did you mean `sales.item.brand_name`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `sales.item.manager_id`) instead of importing a second, disconnected copy.
  ```
- `trilogy run query72.preql`

  ```text
  Resolution error in query72.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 33). The requested concepts split into 2 disconnected subgraphs: {matching_inventory.inv_week, matching_inventory.item_sk, matching_inventory.qoh, matching_inventory.wh_name}; {qualifying_sales.item_desc, qualifying_sales.item_sk, qualifying_sales.order_num, qualifying_sales.promo_sk, qualifying_sales.sale_qty, qualifying_sales.sale_week}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query72.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query74.preql`

  ```text
  Resolution error in query74.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 46). The requested concepts split into 2 disconnected subgraphs: {c.first_name, c.last_name}; {customer_code, pivoted.ss_2001, pivoted.ss_2002, pivoted.ws_2001, pivoted.ws_2002}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

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
- `trilogy file write --content # Query 78: Multi-channel never-returned sales analysis

import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.ca…asc,
    combined_other.sprice asc,
    round(store_agg.store_qty::numeric / nullif(combined_other.qty::numeric, 0), 2) asc
limit 100;
 query78.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: ORDER BY references 'store_agg.store_wcost', a measure at a finer grain (Grain<store_agg.cust_id,store_agg.item_sk,store_agg.yr>) than the select grain (Grain<combined_other.cust_id,combined_other.item_sk,combined_other.qty,combined_other.yr,local.other_sprice,local.other_wcost>|store_agg.store_qty is not MagicConstants.NULL and combined_other.qty > 0); it has no single value per output row. Project it (prefix with `--` to keep it out of the rows) and order by that alias instead.
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 72). The requested concepts split into 2 disconnected subgraphs: {_combined_other_cust_id, _combined_other_item_sk, _combined_other_qty, _combined_other_sprice, _combined_other_wcost, _combined_other_yr}; {store_agg.cust_id, store_agg.item_sk, store_agg.store_qty, store_agg.store_unit_sprice, store_agg.store_unit_wcost, store_agg.yr}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 72). The requested concepts split into 2 disconnected subgraphs: {_combined_other_cust_id, _combined_other_item_sk, _combined_other_qty, _combined_other_sprice, _combined_other_wcost, _combined_other_yr}; {store_agg.cust_id, store_agg.item_sk, store_agg.store_qty, store_agg.store_unit_sprice, store_agg.store_unit_wcost, store_agg.yr}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 70). The requested concepts split into 2 disconnected subgraphs: {_combined_other_cust_id, _combined_other_item_sk, _combined_other_qty, _combined_other_sprice, _combined_other_wcost, _combined_other_yr}; {store_agg.cust_id, store_agg.item_sk, store_agg.store_qty, store_agg.store_unit_sprice, store_agg.store_unit_wcost, store_agg.yr}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 70). The requested concepts split into 2 disconnected subgraphs: {_combined_other_cust_id, _combined_other_item_sk, _combined_other_qty, _combined_other_sprice, _combined_other_wcost, _combined_other_yr}; {store_agg.cust_id, store_agg.item_sk, store_agg.store_qty, store_agg.store_unit_sprice, store_agg.store_unit_wcost, store_agg.yr}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: Undefined concept: cat_agg.yr. Suggestions: ['cat_agg.qty', 'cat_agg.sprice', 'cat_agg.wcost', 'catalog_agg.yr', 'store_agg.yr', 'web_agg.yr']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Nothing was executed: parsed 6 definition statement(s) (3 imports, 3 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: Cannot use is not with non-null or boolean value <Filter: MagicConstants.NULL where bool(1) = True>
  ```
- `trilogy run query84.preql`

  ```text
  Resolution error in query84.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 2 disconnected subgraphs: {c.address.city, c.demographics.sk, c.household_demographic.income_band.lower_bound, c.household_demographic.income_band.upper_bound, customer_code, full_name}; {sr.customer_demographic.sk, sr.item.sk, sr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query90.preql`

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
- `trilogy run raw/store_sales.preql duckdb --format json`

  ```text
  Syntax error in raw\store_sales.preql: Nothing was executed: parsed 23 definition statement(s) (15 imports, 5 concepts, 2 datasources, 1 property) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query93.preql duckdb --format json`

  ```text
  Syntax error in query93.preql: 2 undefined concept references; fix all before re-running:
    - return_reason.desc (line 3, col 8, in SELECT); did you mean: ss.return_reason.desc, return_reason.id, ss.return_reason.sk, ss.item.desc?
    - return_reason.id (line 3, col 28, in SELECT); did you mean: ss.return_reason.id, return_reason.desc, ss.return_reason.desc, ss.item.id, ss.date.id, ss.return_date.id?
  ```
- `trilogy file read query93.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query95.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 42 column 12 (char 1267). Re-issue the call with valid JSON arguments.
  ```

### `syntax-parse`

- `trilogy file write query04.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…
    store_cust_info.fname as First_Name,
    store_cust_info.lname as Last_Name,
    store_cust_info.pref_flag as Preferred_Customer_Flag
 --escapes`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...rowsets on customer ID select ??? distinct     s01.cust_id as Cu...

  Write stats: received 2272 chars / 2272 bytes; tail: …'e,\\n    store_cust_info.pref_flag as Preferred_Customer_Flag\\n'.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as ss;

# Bucket 1: quantity 1-20
# Bucket 2: quantity 21-40
# Bucket 3: quantity 41-60
# B… end;

select
    col1 as bucket_1_20,
    col2 as bucket_21_40,
    col3 as bucket_41_60,
    col4 as bucket_61_80,
    col5 as bucket_81_100
by *
;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...80,     col5 as bucket_81_100 ??? by * ;

  Write stats: received 2045 chars / 2045 bytes; tail: …',\\n    col4 as bucket_61_80,\\n    col5 as bucket_81_100\\nby *\\n;'.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as ss;

# count of lines per bucket
auto cnt_1_20 <- count(ss.line_item ? ss.quantity betwe…0 as bucket2_avg_paid,
    avg_paid_41_60 as bucket3_avg_paid,
    avg_paid_61_80 as bucket4_avg_paid,
    avg_paid_81_100 as bucket5_avg_paid
by *
;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...id_81_100 as bucket5_avg_paid ??? by * ;

  Write stats: received 1774 chars / 1774 bytes; tail: …'et4_avg_paid,\\n    avg_paid_81_100 as bucket5_avg_paid\\nby *\\n;'.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as ss;

select
    case when count(ss.line_item ? ss.quantity between 1 and 20) > 74129
   …ext_discount_amount ? ss.quantity between 81 and 100)
         else avg(ss.net_paid ? ss.quantity between 81 and 100)
    end as bucket_81_100
by *
;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
    --> 28:1
     |
  28 | by *
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...100)     end as bucket_81_100 ??? by * ;

  Write stats: received 1245 chars / 1245 bytes; tail: …'quantity between 81 and 100)\\n    end as bucket_81_100\\nby *\\n;'.
  ```
- `trilogy file write query16.preql --content import raw.catalog_sales as cs;

# Find orders that ship from more than one warehouse
with multi_warehouse_orders …as unique_orders,
    sum(cs.ext_ship_cost) as total_ext_ship_cost,
    sum(cs.net_profit) as total_net_profit
order by unique_orders desc
limit 100;`

  ```text
  refused to write 'query16.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:33
    |
  7 |     count(cs.item.sk) as dummy  -- just need to select something with order_number
    |                                 ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
    count(cs.item.sk) as dummy  ??? -- just need to select somethi...

  Write stats: received 1013 chars / 1013 bytes; tail: …') as total_net_profit\\norder by unique_orders desc\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.all_sales as all_sales;
import raw.store_sales as store_sales;

# Step 1: Frequent items
# count of (it…ving
    total_sales > 0
order by
    cw_sales.last_name nulls first,
    cw_sales.first_name nulls first,
    cw_sales.total nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 37:6
     |
  37 |     (import raw.catalog_sales as cs;
     |      ^---
     |
     = expected select_statement
  Location:
   with cw_sales as union(     ( ??? import raw.catalog_sales as cs...

  Write stats: received 2525 chars / 2525 bytes; tail: …'name nulls first,\\n    cw_sales.total nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year, count(all_sales.sale_line_item_counter) as cnt where all_sales.date.year between 2000 and 2003 group by all_sales.date.year order by all_sales.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...te.year between 2000 and 2003 ??? group by all_sales.date.year o...
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.id, sum(ss.quantity * ss.sales_price) as total_alltime where ss.customer.id is not null group by ss.customer.id order by total_alltime desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...re ss.customer.id is not null ??? group by ss.customer.id order
  ```
- `trilogy file write query25.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

# Filter store sales: Apr 2001 sales with returns i…omer_id = cs_filtered.customer_id
union join ss_filtered.item_id = cs_filtered.item_id
order by item_id, item_desc, store_code, store_name
limit 100;`

  ```text
  refused to write 'query25.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset ss_filtered as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   with returns in Apr-Oct 2001 ??? rowset ss_filtered as where

  Write stats: received 1431 chars / 1431 bytes; tail: …'der by item_id, item_desc, store_code, store_name\\nlimit 100;'.
  ```
- `trilogy file write query26.preql --content import raw/catalog_sales as cs;

where
    cs.sold_date.year = 2000
    and cs.billing_customer_demographic.gender…ist_price) as avg_list_price,
    avg(cs.coupon_amt) as avg_coupon_amt,
    avg(cs.sales_price) as avg_sales_price
order by
    cs.item.id
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

  Write stats: received 565 chars / 565 bytes; tail: …'price) as avg_sales_price\\norder by\\n    cs.item.id\\nlimit 100;'.
  ```
- `trilogy file write query27.preql --content import raw.store_sales as sales;

# For store sales in 2002 at stores in TN
# where recorded point-of-sale custome…s.item.id, sales.store.state) as grouping_indicator
by rollup (sales.item.id, sales.store.state)
order by sales.item.id, sales.store.state
limit 100;`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:27
     |
  19 |     grouping(sales.item.id, sales.store.state) as grouping_indicator
     |                           ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ce,     grouping(sales.item.id ??? , sales.store.state) as groupi...

  Write stats: received 806 chars / 806 bytes; tail: …'.state)\\norder by sales.item.id, sales.store.state\\nlimit 100;'.
  ```
- `trilogy file write query36.preql --content import raw.store_sales as ss;

# Hierarchy level markers
auto g_class <- grouping(ss.item.class);
auto g_category …'TN'
order by level desc nulls first,
         case when level = 0 then ss.item.category end asc nulls first,
         rnk asc nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:1
     |
  30 | where ss.date.year = 2001
     | ^---
     |
     = expected limit, order_by, or having
  Location:
  ...item.category, ss.item.class) ??? where ss.date.year = 2001   an...

  Write stats: received 1189 chars / 1189 bytes; tail: …'end asc nulls first,\\n         rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query38.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with all_three as intersect… as first_name,
      ws.date.date as sale_date
  )
) -> (last_name string?, first_name string?, sale_date date)

select
  count(*) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:60
     |
  27 | ) -> (last_name string?, first_name string?, sale_date date)
     |                                                            ^---
     |
     = expected concept_nullable_modifier or metadata
  Location:
  ...t_name string?, sale_date date ??? )  select   count(*) as cnt li...

  Write stats: received 871 chars / 871 bytes; tail: …'tring?, sale_date date)\\n\\nselect\\n  count(*) as cnt\\nlimit 100;'.
  ```
- `trilogy file write query38.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with all_three as intersect…first_name,
      ws.date.date as sale_date
  )
) -> (last_name string?, first_name string?, sale_date_raw date)

select
  count(*) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:64
     |
  27 | ) -> (last_name string?, first_name string?, sale_date_raw date)
     |                                                                ^---
     |
     = expected concept_nullable_modifier or metadata
  Location:
  ...me string?, sale_date_raw date ??? )  select   count(*) as cnt li...

  Write stats: received 875 chars / 875 bytes; tail: …'g?, sale_date_raw date)\\n\\nselect\\n  count(*) as cnt\\nlimit 100;'.
  ```
- `trilogy file write query38.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with all_three as intersect… first_name,
      ws.date.date as sale_date
  )
) -> (last_name string?, first_name string?, sale_date string?)

select
  count(*) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:63
     |
  27 | ) -> (last_name string?, first_name string?, sale_date string?)
     |                                                               ^---
     |
     = expected metadata
  Location:
  ...ame string?, sale_date string? ??? )  select   count(*) as cnt li...

  Write stats: received 874 chars / 874 bytes; tail: …'ng?, sale_date string?)\\n\\nselect\\n  count(*) as cnt\\nlimit 100;'.
  ```
- `trilogy file write query38.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with all_three as intersect… as first_name,
      ws.date.date as sale_date
  )
) -> (last_name string, first_name string, sale_date string)

select
  count(*) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:60
     |
  27 | ) -> (last_name string, first_name string, sale_date string)
     |                                                            ^---
     |
     = expected concept_nullable_modifier or metadata
  Location:
  ..._name string, sale_date string ??? )  select   count(*) as cnt li...

  Write stats: received 871 chars / 871 bytes; tail: …'ring, sale_date string)\\n\\nselect\\n  count(*) as cnt\\nlimit 100;'.
  ```
- `trilogy file write query38.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with all_three as intersect…g_customer.first_name as first_name,
      ws.date.date as sale_date
  )
) -> (last_name, first_name, sale_date)

select
  count(*) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:39
     |
  27 | ) -> (last_name, first_name, sale_date)
     |                                       ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...st_name, first_name, sale_date ??? )  select   count(*) as cnt li...

  Write stats: received 850 chars / 850 bytes; tail: …' first_name, sale_date)\\n\\nselect\\n  count(*) as cnt\\nlimit 100;'.
  ```
- `trilogy file write test38.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;

with test as intersect(
  (
    where ss.customer.sk is …me,
      cs.sold_date.date as sale_date
  )
) -> (last_name, first_name, sale_date)

select test.last_name, test.first_name, test.sale_date
limit 3;`

  ```text
  refused to write 'test38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...t_name, first_name, sale_date) ???  select test.last_name, test....

  Write stats: received 622 chars / 622 bytes; tail: …'ect test.last_name, test.first_name, test.sale_date\\nlimit 3;'.
  ```
- `trilogy file write query38.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with all_three as intersect…ng_customer.first_name as first_name,
      ws.date.date as sale_date
  )
) -> (last_name, first_name, sale_date);

select count(*) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...rst_name, sale_date);  select ??? count(*) as cnt limit 100;

  Write stats: received 849 chars / 849 bytes; tail: …', first_name, sale_date);\\n\\nselect count(*) as cnt\\nlimit 100;'.
  ```
- `trilogy file write query43.preql --content import raw.store_sales as s;

# Seven day-of-week columns: sum of unit price (sales_price) per day
# Filter: year …m as wednesday,
    @thursday_sum as thursday,
    @friday_sum as friday,
    @saturday_sum as saturday
order by s.store.name, s.store.id
limit 100;
`

  ```text
  refused to write 'query43.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:1
    |
  6 | def sunday_sum   <- sum(s.sales_price ? s.date.day_name = 'Sunday');
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...0, stores with GMT offset -5  ??? def sunday_sum   <- sum(s.sale...

  Write stats: received 985 chars / 985 bytes; tail: …'um as saturday\\norder by s.store.name, s.store.id\\nlimit 100;\\n'.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as ss;

# Filter to store with surrogate id = 1
where ss.store.sk = 1

# First compute thre…ofit) desc) as rank_desc
having
    avg_net_profit > 0.9 * avg(ss.net_profit ? ss.sale_address.sk is null)
order by
    item_avg_profit asc
limit 20;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...where sale_address sk is null ??? auto threshold <- 0.9 * avg(ss...

  Write stats: received 1029 chars / 1029 bytes; tail: …'dress.sk is null)\\norder by\\n    item_avg_profit asc\\nlimit 20;'.
  ```
- `trilogy file write query51.preql --content import raw.web_sales as ws;
import raw.store_sales as ss;

# Web sales: per-item, per-day unit prices in year 2000…rder by coalesce(web_running.sale_date, store_running.sale_date) asc)
order by
    item_key asc nulls first,
    sale_date asc nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset web_daily_raw as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...-day unit prices in year 2000 ??? rowset web_daily_raw as where

  Write stats: received 2325 chars / 2325 bytes; tail: …'ey asc nulls first,\\n    sale_date asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query53.preql`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens - write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any function call, cast, or other expression needs them.
  Location:
  ...1: Compute per-quarter totals ??? by manufacturer
   with per_quar...

  Write stats: received 1557 chars / 1557 bytes; tail: …'arter.q_total asc,\r\\n    per_quarter.man_id asc\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query53.preql`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | with per_quarter as
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...nd ss.store.id is not null

   ??? with per_quarter as
   select


  Write stats: received 1290 chars / 1290 bytes; tail: …'arter.q_total asc,\r\\n    per_quarter.man_id asc\r\\nlimit 100;\r\\n'.
  ```
- `trilogy run --import raw.item:item select distinct item.brand_name where item.brand_name like '%scholar%' limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.brand_name where...
  ```
- `trilogy file write query53.preql -e -c import raw.store_sales as ss;

where ss.date.year = 2000
  and (
    (ss.item.category in ('Books', 'Children', 'Elect…anufacturer_avgs.avg_q_sales > 0.1
order by
    manufacturer_avgs.avg_q_sales asc,
    per_quarter.q_total asc,
    per_quarter.man_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | with per_quarter as
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
   and ss.store.id is not null  ??? with per_quarter as select

  Write stats: received 1250 chars / 1250 bytes; tail: …'r_quarter.q_total asc,\\n    per_quarter.man_id asc\\nlimit 100;'.
  ```
- `trilogy file write query53.preql --content import raw.store_sales as ss;

where ss.date.year = 2000
  and (
    (ss.item.category in ('Books', 'Children', 'E…anufacturer_avgs.avg_q_sales > 0.1
order by
    manufacturer_avgs.avg_q_sales asc,
    per_quarter.q_total asc,
    per_quarter.man_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | with per_quarter as
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
   and ss.store.id is not null  ??? with per_quarter as select

  Write stats: received 1250 chars / 1250 bytes; tail: …'r_quarter.q_total asc,\\n    per_quarter.man_id asc\\nlimit 100;'.
  ```
- `trilogy file write query54.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…ce / 50)::int * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 34:1
     |
  34 | union
     | ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...alog_buyers.c_id is not null  ??? union  select     web_buyers.c...

  Write stats: received 2114 chars / 2114 bytes; tail: …'_count asc nulls first,\\n    segment_times_50 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query57.preql -e -c import raw/catalog_sales as cs;

# Step 1: Monthly totals per (category, brand, call_center, year, month)
with monthly… sequenced.call_center_name,
    sequenced.year,
    sequenced.month_of_year,
    sequenced.avg_monthly_sales,
    sequenced.monthly_total
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/catalog_sales as cs;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/catalog_sales as cs;  # St...

  Write stats: received 2745 chars / 2745 bytes; tail: …'ed.avg_monthly_sales,\\n    sequenced.monthly_total\\nlimit 100;'.
  ```
- `trilogy run --import raw.item:item select distinct item.brand_name limit 50;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.brand_name limit...
  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…nt,
    combined.total_ext_sales_price
order by combined.channel, combined.missing_ref, combined.year, combined.quarter, combined.category
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:6
    |
  7 |     (import raw.store_sales as ss;
    |      ^---
    |
    = expected select_statement
  Location:
   reference (ss_store_sk)     ( ??? import raw.store_sales as ss;

  Write stats: received 2039 chars / 2039 bytes; tail: …'ombined.year, combined.quarter, combined.category\\nlimit 100;'.
  ```
- `trilogy file write query76.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined as union(…ined.total_ext_sales_price
order by combined.channel, combined.missing_ref, combined.year, combined.quarter, combined.category nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..._count, total_ext_sales_price) ???  select     combined.channel,...

  Write stats: received 1576 chars / 1576 bytes; tail: …', combined.quarter, combined.category nulls first\\nlimit 100;'.
  ```
- `trilogy file write query83.preql --content import raw.store_returns as sr;
import raw.catalog_returns as cr;
import raw.web_returns as wr;

# Get store retur…eb_sales.item.id as item_code,
    sum(wr.return_quantity ? wr.return_quantity is not null) as web_qty,
    count(wr.web_sales.item.sk) as web_rows
;`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:38
     |
  11 |     count(sr.item.sk) as store_rows  -- count of return rows regardless of quantity
     |                                      ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...nt(sr.item.sk) as store_rows  ??? -- count of return rows regard...

  Write stats: received 1017 chars / 1017 bytes; tail: …'l) as web_qty,\\n    count(wr.web_sales.item.sk) as web_rows\\n;'.
  ```
- `trilogy file write query84.preql --content import raw.store_returns as sr;
import raw.customer as c;

# Customer code and full name
# For each store return, …(c.last_name, ', ', c.first_name) as full_name
    --sr.ticket_number as --,
    --sr.item.sk as --
order by
    customer_code nulls first
limit 100;`

  ```text
  refused to write 'query84.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:5
     |
  18 |     --sr.ticket_number as --,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ....first_name) as full_name     ??? --sr.ticket_number as --,

  Write stats: received 768 chars / 768 bytes; tail: …'m.sk as --\\norder by\\n    customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write query87.preql --content # Q87: Count store-sale (last_name, first_name, sale_date) combos in 2000 not found in catalog or web sales

impor…
;

with only_store as
except((store_combos), (catalog_combos), (web_combos)) -> (ln, fn, sd)
select
    count(only_store.ln) as unique_combo_count
;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
    --> 38:9
     |
  38 | except((store_combos), (catalog_combos), (web_combos)) -> (ln, fn, sd)
     |         ^---
     |
     = expected select_statement
  Location:
  ...;  with only_store as except(( ??? store_combos), (catalog_combos...

  Write stats: received 951 chars / 951 bytes; tail: …' sd)\\nselect\\n    count(only_store.ln) as unique_combo_count\\n;'.
  ```
- `trilogy file write query89.preql --content import raw.store_sales as ss;

# Per-unit sale price = sales_price
The monthly total of unit sale prices for each …s.store.name,
  ss.item.category,
  ss.item.class,
  ss.item.brand_name,
  ss.store.company_name,
  ss.date.month_of_year,
  total,
  avg
limit 100;
`

  ```text
  refused to write 'query89.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | The monthly total of unit sale prices for each group
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...unit sale price = sales_price ??? The monthly total of unit sale...

  Write stats: received 1281 chars / 1281 bytes; tail: …'ny_name,\\n  ss.date.month_of_year,\\n  total,\\n  avg\\nlimit 100;\\n'.
  ```
- `trilogy file write query91.preql --content import raw.catalog_returns as cr;

where
    cr.date.year = 1998
    and cr.date.month_of_year = 11
    and cr.bil…ss
    -- cr.billing_customer.demographics.marital_status,
    -- cr.billing_customer.demographics.education_status
order by
    total_net_loss desc;`

  ```text
  refused to write 'query91.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:5
     |
  18 |     -- cr.billing_customer.demographics.marital_status,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t_loss) as total_net_loss     ??? -- cr.billing_customer.demogra...

  Write stats: received 807 chars / 807 bytes; tail: …'ographics.education_status\\norder by\\n    total_net_loss desc;'.
  ```
- `trilogy file write query93.preql --content import raw.store_sales as ss;

where ss.return_reason.desc = 'reason 28'
select
    ss.customer.id as customer_id,…es_price
        end
    ) as total_adjusted_amount
order by total_adjusted_amount asc nulls first, customer_id asc nulls first
limit 100
offset 250;`

  ```text
  refused to write 'query93.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | order by total_adjusted_amount asc nulls first, customer_id asc nulls first
     | ^---
     |
     = expected metadata, where, having, select_grouping, or JOIN_TYPE
  Location:
     ) as total_adjusted_amount ??? order by total_adjusted_amount...

  Write stats: received 428 chars / 428 bytes; tail: …'lls first, customer_id asc nulls first\\nlimit 100\\noffset 250;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year, count(all_sales.sale_line_item_counter) where all_sales.date.year between 2000 and 2003 group by all_sales.date.year order by all_sales.date.year;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(all_sales.sale_line_item_counter) as sale_line_item_counter_count`
  Location:
  ...sales.sale_line_item_counter) ??? where all_sales.date.year betw...
  ```
- `trilogy file write query27.preql --content import raw.store_sales as sales;

# For store sales in 2002 at stores in TN
# where recorded point-of-sale custome…s.item.id, sales.store.state) as grouping_indicator
by rollup (sales.item.id, sales.store.state)
order by sales.item.id, sales.store.state
limit 100;`

  ```text
  refused to write 'query27.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `average ticket quantity: avg quantity per ticket
      avg(sales.quantity * 1.0) as average_ticket_quantity_avg_quantity_per`
  Location:
  ...s.store.state,     -- average ??? ticket quantity: avg quantity

  Write stats: received 961 chars / 961 bytes; tail: …'.state)\\norder by sales.item.id, sales.store.state\\nlimit 100;'.
  ```
- `trilogy run --import raw.catalog_sales:sale select count(sale.order_number) where sale.sold_date.year = 1999 and sale.bill_household_demographic.buy_potential = '>10000' and sale.billing_customer_demographic.marital_status = 'D' and sale.days_to_ship > 5 and sale.promotion.sk is null limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(sale.order_number) as order_number_count`
  Location:
  ...lect count(sale.order_number) ??? where sale.sold_date.year = 19...
  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

# Filter to year 2000 sold date
where ws.date.year = 2000

select
    ws.item.categor…tegory, ws.item.class)
order by hierarchy_level desc nulls first,
         ws.item.category asc nulls first,
         rnk asc nulls first
limit 100;
`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `parent for ranking within category: only for detail (level 0) rows
      rank(ws.item.category, ws.item.class) over (
          partition by grouping(ws.item.class) + grouping(ws.item.category),
                       case when grouping(ws.item.class) = 0 then ws.item.category else null end
          order by sum(ws.net_paid) desc
      ) as parent_for_ranking_within_category_only_`
  Location:
  ...hierarchy_level,     --parent ??? for ranking within category: o...

  Write stats: received 767 chars / 767 bytes; tail: …'ry asc nulls first,\\n         rnk asc nulls first\\nlimit 100;\\n'.
  ```

### `cli-misuse`

- `trilogy run - --import raw.store_sales:ss select count(ss.line_item ? ss.quantity between 1 and 20) as c1, count(ss.line_item ? ss.quantity between 21 and 40…tity between 41 and 60) as c3, count(ss.line_item ? ss.quantity between 61 and 80) as c4, count(ss.line_item ? ss.quantity between 81 and 100) as c5;`

  ```text
  'select count(ss.line_item ? ss.quantity between 1 and 20) as c1, count(ss.line_item ? ss.quantity between 21 and 40) as c2, count(ss.line_item ? ss.quantity between 41 and 60) as c3, count(ss.line_item ? ss.quantity between 61 and 80) as c4, count(ss.line_item ? ss.quantity between 81 and 100) as c5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```

### `type-error`

- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
- `trilogy run query77.preql`

  ```text
  Syntax error in query77.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `join-resolution`

- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Could not resolve connections for query with output ['local._overall_stats_total_all<Purpose.METRIC>Derivation.BASIC>', 'local._overall_stats_count_all<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Could not resolve connections for query with output ['local._overall_stats_total_all<Purpose.METRIC>Derivation.BASIC>', 'local._overall_stats_count_all<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```

### `file-not-found`

- `trilogy run query14-overall-avg.preql`

  ```text
  Input 'query14-overall-avg.preql' does not exist.
  ```
- `trilogy run query95.preql`

  ```text
  Input 'query95.preql' does not exist.
  ```

### `undefined-concept`

- `trilogy explore query64.preql --regex agg_sales`

  ```text
  Failed to parse query64.preql: (UndefinedConceptException(...), "Undefined concept: agg_sales.ss_date.year. Suggestions: ['agg_sales.ss.date.year', 'agg_sales.ss.item.sk', 'agg_sales.ss.store.name', 'cr.sales.sold_date.year', 'cr.sales.ship_date.year', 'ss.date.year']")
  ```
