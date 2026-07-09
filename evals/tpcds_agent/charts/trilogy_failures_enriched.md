# Trilogy failure analysis — 20260709-105518

- Run `20260709-105517_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1435 | failed: 168 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 95 | 57% |
| `syntax-parse` | 52 | 31% |
| `syntax-missing-alias` | 8 | 5% |
| `cli-misuse` | 8 | 5% |
| `type-error` | 4 | 2% |
| `join-resolution` | 1 | 1% |

## Detail

### `other`

- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin: Comparison `sales.channel in ('STORE', 'CATALOG', 'WEB')` matches every value of enum field 'sales.channel', which contains only these values: 'WEB', 'CATALOG', 'STORE'. It is always true and should be removed.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/web_returns:wr select wr.store.id, wr.web_sales.web_site.id, count(1) as cnt, sum(wr.return_amount) as tot_ret where wr.return_date.date between '2000-08-23'::date and '2000-09-06'::date and wr.store.id is not null limit 5;`

  ```text
  Unexpected error in stdin: (_duckdb.BinderException) Binder Error: Table "wr_web_returns" does not have a column named "WR_STORE_SK"

  Candidate bindings: : "wr_item_sk"

  LINE 10:     INNER JOIN "store" as "wr_store_store" on "wr_web_returns"."WR_STORE_SK" = "wr_store_store"."S_STORE_SK...
                                                         ^
  [SQL:
  WITH
  cooperative as (
  SELECT
      "wr_store_store"."S_STORE_ID" as "wr_store_id",
      "wr_web_sales_web_site_web_site"."web_site_id" as "wr_web_sales_web_site_id",
      sum("wr_web_returns"."WR_RETURN_AMT") as "tot_ret"
  FROM
      "web_returns" as "wr_web_returns"
      INNER JOIN "store" as "wr_store_store" on "wr_web_returns"."WR_STORE_SK" = "wr_store_store"."S_STORE_SK"
      INNER JOIN "date_dim" as "wr_return_date_date" on "wr_web_returns"."WR_RETURNED_DATE_SK" = "wr_return_date_date"."D_DATE_SK"
      INNER JOIN "web_sales" as "wr_web_sales_web_sales" on "wr_web_returns"."WR_ITEM_SK" = "wr_web_sales_web_sales"."WS_ITEM_SK" AND "wr_web_returns"."WR_ORDER_NUMBER" = "wr_web_sales_web_sales"."WS_ORDER_NUMBER"
      LEFT OUTER JOIN "web_site" as "wr_web_sales_web_site_web_site" on "wr_web_sales_web_sales"."WS_WEB_SITE_SK" = "wr_web_sales_web_site_web_site"."web_site_sk"
  WHERE
      cast("wr_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

  GROUP BY
      1,
      2),
  abundant as (
  SELECT
      "cooperative"."wr_store_id" as "wr_store_id",
      "cooperative"."wr_web_sales_web_site_id" as "wr_web_sales_web_site_id",
      CASE WHEN 1 IS NOT NULL THEN 1 ELSE 0 END as "cnt"
  FROM
      "cooperative")
  SELECT
      "cooperative"."wr_store_id" as "wr_store_id",
      "cooperative"."wr_web_sales_web_site_id" as "wr_web_sales_web_site_id",
      "abundant"."cnt" as "cnt",
      "cooperative"."tot_ret" as "tot_ret"
  FROM
      "abundant"
      INNER JOIN "cooperative" on "abundant"."wr_store_id" = "cooperative"."wr_store_id" AND "abundant"."wr_web_sales_web_site_id" is not distinct from "cooperative"."wr_web_sales_web_site_id"
  LIMIT (5)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query06.preql`

  ```text
  Syntax error in query06.preql: 3 undefined concept references; fix all before re-running:
    - item.category (line 11, col 9, in WHERE); did you mean: sales.item.category, sales.item.category_id?
    - sales.customer.address (line 12, col 9, in WHERE); did you mean: sales.customer.address.sk, sales.customer.address.id, sales.customer.address.street_number, sales.customer.address.street_name, sales.customer.address.street_type, sales.customer.address.suite_number?
    - item.current_price (line 13, col 9, in WHERE); did you mean: sales.item.current_price?
  ```
- `trilogy run query06.preql`

  ```text
  Resolution error in query06.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 18). The requested concepts split into 2 disconnected subgraphs: {category_avg.avg_price, category_avg.item_prices.sales.item.category}; {qualifying_count, sales.customer.address.sk, sales.customer.address.state, sales.date.month_of_year, sales.date.year, sales.item.category, sales.item.current_price}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query10.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 30, col 5, in ORDER BY); did you mean: store.customer.first_name, store.return_customer.first_name, web.billing_customer.first_name, web.ship_customer.first_name, web.return_customer.first_name, web.return_refund_customer.first_name?
    - last_name (line 31, col 5, in ORDER BY); did you mean: store.customer.last_name, store.return_customer.last_name, web.billing_customer.last_name, web.ship_customer.last_name, web.return_customer.last_name, web.return_refund_customer.last_name?
    - preferred_cust_flag (line 32, col 5, in ORDER BY); did you mean: store.customer.preferred_cust_flag, web.ship_customer.preferred_cust_flag, web.return_customer.preferred_cust_flag, store.return_customer.preferred_cust_flag, web.billing_customer.preferred_cust_flag, web.return_refund_customer.preferred_cust_flag?
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: 6 undefined concept references; fix all before re-running:
    - ss.customer_demographics.marital_status (line 11, col 6, in WHERE); did you mean: ss.customer_demographic.marital_status, ss.customer.demographics.marital_status, ss.return_customer.demographics.marital_status?
    - ss.customer_demographics.education_status (line 12, col 11, in WHERE); did you mean: ss.customer_demographic.education_status, ss.customer.demographics.education_status, ss.return_customer.demographics.education_status?
    - ss.customer_demographics.marital_status (line 16, col 6, in WHERE); did you mean: ss.customer_demographic.marital_status, ss.customer.demographics.marital_status, ss.return_customer.demographics.marital_status?
    - ss.customer_demographics.education_status (line 17, col 11, in WHERE); did you mean: ss.customer_demographic.education_status, ss.customer.demographics.education_status, ss.return_customer.demographics.education_status?
    - ss.customer_demographics.marital_status (line 21, col 6, in WHERE); did you mean: ss.customer_demographic.marital_status, ss.customer.demographics.marital_status, ss.return_customer.demographics.marital_status?
    - ss.customer_demographics.education_status (line 22, col 11, in WHERE); did you mean: ss.customer_demographic.education_status, ss.customer.demographics.education_status, ss.return_customer.demographics.education_status?
  ```
- `trilogy run query13.preql`

  ```text
  Syntax error in query13.preql: Comparison `ss.customer_demographic.marital_status = 'Married'` can never match enum field 'ss.customer_demographic.marital_status', which contains only these values: 'M', 'S', 'D', 'W', 'U'. It is always false and should be removed.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query16.preql`

  ```text
  Syntax error in query16.preql: `cs.order_number in cs.order_number` compares a concept to itself — cs.order_number and qualified_orders.cs.order_number are merged into one concept, so it cannot filter one model by another. To filter by values present in a related model, compare two distinct (unmerged) concepts (reference the other model's key directly, or drop the `merge`).
  ```
- `trilogy file read query17.preql`

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
      ROLLUP (1, 2, 3, 4)
  ORDER BY
      "country" asc nulls first,
      "state" asc nulls first,
      "county" asc nulls first,
      "item_code" asc nulls first,
      MIN(grouping("questionable"."cs_billing_customer_address_country")) asc nulls first,
      MIN(grouping("questionable"."cs_billing_customer_address_state")) asc nulls first,
      MIN(grouping("questionable"."cs_billing_customer_address_county")) asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query18.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query21.preql`

  ```text
  Syntax error in query21.preql: Cannot compare DATE (ref:inv.date.date) and STRING (2000-02-10) of different types with operator >= in ref:inv.date.date >= 2000-02-10
  ```
- `trilogy run query23.preql`

  ```text
  Resolution error in query23.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 46). The requested concepts split into 2 disconnected subgraphs: {cust_2000_2003.cust_id_03, cust_2000_2003.total_2000_2003}; {cust_alltime.cust_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run test_best2.preql`

  ```text
  Resolution error in test_best2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 31). The requested concepts split into 2 disconnected subgraphs: {cust_2000_2003.cust_id_03}; {cust_alltime.alltime_total, cust_alltime.cust_first_name, cust_alltime.cust_id, cust_alltime.cust_last_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Comparison `store_returns.return_date.month_of_year <= 12` matches every value of enum field 'store_returns.return_date.month_of_year', which contains only these values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. It is always true and should be removed.
  ```
- `trilogy run query29.preql`

  ```text
  Resolution error in query29.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['catalog_sales.item.id', 'catalog_sales.sold_date.year', 'store_returns.item.sk', 'store_returns.return_date.month_of_year', 'store_returns.return_date.year', 'store_returns.return_quantity', 'store_returns.ticket_number', 'store_sales.date.month_of_year', 'store_sales.date.year', 'store_sales.item.desc', 'store_sales.quantity', 'store_sales.store.id', 'store_sales.store.name', 'catalog_sales.sold_date.sk', 'store_returns.return_date.sk', 'store_sales.date.sk', 'store_sales.store.sk'], have {'catalog_sales.item.sk'} or {'store_returns.billing_customer.sk', 'catalog_sales.billing_customer.id', 'catalog_sales.billing_customer.sk'}. Different paths are is: [{'catalog_sales.item.sk'}, {'store_returns.billing_customer.sk', 'catalog_sales.billing_customer.id', 'catalog_sales.billing_customer.sk'}]")
  ```
- `trilogy run query29.preql`

  ```text
  Resolution error in query29.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['catalog_sales.item.id', 'catalog_sales.sold_date.year', 'store_returns.item.sk', 'store_returns.return_date.month_of_year', 'store_returns.return_date.year', 'store_returns.return_quantity', 'store_returns.ticket_number', 'store_sales.date.month_of_year', 'store_sales.date.year', 'store_sales.item.desc', 'store_sales.quantity', 'store_sales.store.id', 'store_sales.store.name', 'catalog_sales.sold_date.sk', 'store_returns.return_date.sk', 'store_sales.date.sk', 'store_sales.store.sk'], have {'catalog_sales.item.sk'} or {'catalog_sales.billing_customer.id', 'store_returns.billing_customer.sk', 'catalog_sales.billing_customer.sk'}. Different paths are is: [{'catalog_sales.item.sk'}, {'catalog_sales.billing_customer.id', 'store_returns.billing_customer.sk', 'catalog_sales.billing_customer.sk'}]")
  ```
- `trilogy run query29_debug2.preql`

  ```text
  Resolution error in query29_debug2.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['cs.sold_date.year', 'sr.item.sk', 'sr.return_date.month_of_year', 'sr.return_date.year', 'sr.return_quantity', 'sr.ticket_number', 'ss.date.month_of_year', 'ss.date.year', 'cs.sold_date.sk', 'sr.return_date.sk', 'ss.date.sk'], have {'cs.item.id', 'cs.item.sk'} or {'cs.billing_customer.id', 'cs.billing_customer.sk', 'sr.billing_customer.sk'}. Different paths are is: [{'cs.item.id', 'cs.item.sk'}, {'cs.billing_customer.id', 'cs.billing_customer.sk', 'sr.billing_customer.sk'}]")
  ```
- `trilogy run query29.preql`

  ```text
  Resolution error in query29.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 42). The requested concepts split into 2 disconnected subgraphs: {cs_pairs.cs_qty, cs_pairs.customer_id, cs_pairs.item_id}; {sr_filtered.sr_qty, ss_filtered.customer_id, ss_filtered.item_desc, ss_filtered.item_id, ss_filtered.store_id, ss_filtered.store_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query29_verify7.preql`

  ```text
  Syntax error in query29_verify7.preql: Nothing was executed: parsed 3 definition statement(s) (2 imports, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query29.preql`

  ```text
  Referenced table \"questionable\" not found!\nCandidate tables: \"concerned\"\n\nLINE 247:     \"questionable\".\"cs_agg_cid\" as \"cs_agg_cid\",\n              ^\n[SQL: \nWITH \ncharming as (\nSELECT\n    \"ss_store_sales\".\"SS_CUSTOMER_SK\" as \"ss_customer_sk\",\n    \"ss_store_sales\".\"SS_SOLD_DATE_SK\" as \"ss_date_sk\",\n    \"ss_store_sales\".\"SS_STORE_SK\" as \"ss_store_sk\",\n    \"ss_store_sales\".\"SS_TICKET_NUMBER\" as \"ss_ticket_number\"\nFROM\n    \"store_sales\" as \"ss_store_sales\"\nGROUP BY\n    1,\n    2,\n    3,\n    4),\nmacho as (\nSELECT\n    \"ss_store_sales\".\"SS
  …
  \"puffy\".\"ss_store_name\" is not distinct from \"yellow\".\"ss_store_name\"\nGROUP BY\n    1,\n    2,\n    3,\n    4,\n    5,\n    6,\n    7\nORDER BY \n    1 asc,\n    2 asc,\n    3 asc,\n    4 asc\nLIMIT (100)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 18.12,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query29.preql`

  ```text
  Unexpected error in query29.preql: (_duckdb.BinderException) Binder Error: Referenced table "questionable" not found!
  Candidate tables: "kaput"

  LINE 266:     "questionable"."cs_agg_cid" as "cs_agg_cid",
                ^
  [SQL:
  WITH
  protective as (
  SELECT
      "sr_billing_customer_customers"."C_CUSTOMER_ID" as "_sr_data_cid",
      "sr_store_returns"."SR_ITEM_SK" as "_sr_data_item_sk",
      "sr_store_returns"."SR_RETURN_QUANTITY" as "_sr_data_sr_qty",
      "sr_store_returns"."SR_TICKET_NUMBER" as "sr_ticket_number"
  FROM
      "store_returns" as "sr_store_returns"
      INNER JOIN "date_dim" as "sr_return_date_date" on "sr_store_returns"."SR_RETURNED_DATE_SK" = "sr_return_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "sr_billing_customer_customers" on "sr_store_returns"."SR_CUSTOMER_SK" = "sr_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "sr_return_date_date"."D_YEAR" = 1999 and "sr_return_date_date"."D_MOY" >= 9
  ),
  concerned as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_sk",
      "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_sk",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_sk",
      "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2,
      3,
      4),
  sweltering as (
  SELECT
      "ss_store_sales"."SS_CUSTOMER_SK" as "ss_customer_sk",
      "ss_store_sales"."SS_QUANTITY" as "ss_quantity",
      "ss_store_sales"."SS_STORE_SK" as "ss_store_sk",
      "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
  FROM
      "store_sales" as "ss_store_sales"
  GROUP BY
      1,
      2,
      3,
      4),
  thoughtful as (
  SELECT
      "cs_billing_customer_customers"."C_CUSTOMER_ID" as "_cs_agg_cid",
      "cs_item_items"."I_ITEM_ID" as "_cs_agg_iid",
      sum("cs_catalog_sales"."CS_QUANTITY") as "_cs_agg_cs_qty"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "cs_billing_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "cs_sold_date_date"."D_YEAR" in (1999,2000,2001)

  GROUP BY
      1,
      2),
  sparkling as (
  SELECT
      "ss_item_items"."I_ITEM_DESC" as "ss_item_desc",
      "ss_item_items"."I_ITEM_ID" as "ss_item_id",
      "ss_item_items"."I_ITEM_SK" as "ss_item_sk"
  FROM
      "item" as "ss_item_items"),
  premium as (
  SELECT
      "protective"."_sr_data_cid" as "sr_data_cid",
      "protective"."_sr_data_item_sk" as "sr_data_item_sk",
      "protective"."_sr_data_sr_qty" as "sr_data_sr_qty",
      "protective"."sr_ticket_number" as "sr_data_sr_ticket_number"
  FROM
      "protective"),
  young as (
  SELECT
      "concerned"."ss_ticket_number" as "ss_ticket_number",
      "ss_customer_customers"."C_CUSTOMER_ID" as "ss_customer_id",
      "ss_date_date"."D_MOY" as "ss_date_month_of_year",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_store_store"."S_STORE_ID" as "ss_store_id",
      "ss_store_store"."S_STORE_NAME" as "ss_store_name"
  FROM
      "concerned"
      INNER JOIN "date_dim" as "ss_date_date" on "concerned"."ss_date_sk" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "concerned"."ss_store_sk" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "customer" as "ss_customer_customers" on "concerned"."ss_customer_sk" = "ss_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "ss_date_date"."D_YEAR" = 1999 and "ss_date_date"."D_MOY" = 9
  ),
  late as (
  SELECT
      "ss_customer_customers"."C_CUSTOMER_ID" as "ss_customer_id",
      "ss_store_store"."S_STORE_ID" as "ss_store_id",
      "ss_store_store"."S_STORE_NAME" as "ss_store_name",
      "sweltering"."ss_quantity" as "ss_quantity",
      "sweltering"."ss_ticket_number" as "ss_ticket_number"
  FROM
      "sweltering"
      LEFT OUTER JOIN "store" as "ss_store_store" on "sweltering"."ss_store_sk" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "customer" as "ss_customer_customers" on "sweltering"."ss_customer_sk" = "ss_customer_customers"."C_CUSTOMER_SK"),
  questionable as (
  SELECT
      "thoughtful"."_cs_agg_cid" as "cs_agg_cid",
      "thoughtful"."_cs_agg_cs_qty" as "cs_agg_cs_qty",
      "thoughtful"."_cs_agg_iid" as "cs_agg_iid"
  FROM
      "thoughtful"),
  abundant as (
  SELECT
      "questionable"."cs_agg_cid" as "cs_agg_cid",
      "questionable"."cs_agg_iid" as "cs_agg_iid"
  FROM
      "questionable"),
  abhorrent as (
  SELECT
      "sparkling"."ss_item_desc" as "ss_item_desc",
      "sparkling"."ss_item_sk" as "ss_item_sk",
      "young"."ss_date_month_of_year" as "ss_date_month_of_year",
      "young"."ss_date_year" as "ss_date_year",
      "young"."ss_store_id" as "ss_store_id",
      "young"."ss_store_name" as "ss_store_name",
      "young"."ss_ticket_number" as "ss_ticket_number",
      coalesce("abundant"."cs_agg_cid","young"."ss_customer_id") as "cs_agg_cid",
      coalesce("abundant"."cs_agg_cid","young"."ss_customer_id") as "ss_customer_id",
      coalesce("abundant"."cs_agg_iid","sparkling"."ss_item_id") as "cs_agg_iid",
      coalesce("abundant"."cs_agg_iid","sparkling"."ss_item_id") as "ss_item_id"
  FROM
      "young"
      LEFT OUTER JOIN "abundant" on "young"."ss_customer_id" = "abundant"."cs_agg_cid"
      LEFT OUTER JOIN "sparkling" on "abundant"."cs_agg_iid" = "sparkling"."ss_item_id"
  WHERE
      "young"."ss_date_year" = 1999 and "young"."ss_date_month_of_year" = 9
  ),
  macho as (
  SELECT
      "late"."ss_quantity" as "ss_quantity",
      "late"."ss_store_id" as "ss_store_id",
      "late"."ss_store_name" as "ss_store_name",
      "late"."ss_ticket_number" as "ss_ticket_number",
      "sparkling"."ss_item_desc" as "ss_item_desc",
      "sparkling"."ss_item_sk" as "ss_item_sk",
      coalesce("abundant"."cs_agg_cid","late"."ss_customer_id") as "cs_agg_cid",
      coalesce("abundant"."cs_agg_cid","late"."ss_customer_id") as "ss_customer_id",
      coalesce("abundant"."cs_agg_iid","sparkling"."ss_item_id") as "cs_agg_iid",
      coalesce("abundant"."cs_agg_iid","sparkling"."ss_item_id") as "ss_item_id"
  FROM
      "late"
      FULL JOIN "abundant" on "late"."ss_customer_id" = "abundant"."cs_agg_cid"
      FULL JOIN "sparkling" on "abundant"."cs_agg_iid" = "sparkling"."ss_item_id"),
  waggish as (
  SELECT
      "abhorrent"."cs_agg_iid" as "cs_agg_iid",
      "abhorrent"."ss_customer_id" as "ss_customer_id",
      "abhorrent"."ss_item_desc" as "ss_item_desc",
      "abhorrent"."ss_item_id" as "ss_item_id",
      "abhorrent"."ss_store_id" as "ss_store_id",
      "abhorrent"."ss_store_name" as "ss_store_name",
      "premium"."sr_data_sr_qty" as "sr_data_sr_qty",
      coalesce("abhorrent"."cs_agg_cid","premium"."sr_data_cid") as "cs_agg_cid",
      coalesce("abhorrent"."ss_item_sk","premium"."sr_data_item_sk") as "sr_data_item_sk",
      coalesce("abhorrent"."ss_item_sk","premium"."sr_data_item_sk") as "ss_item_sk",
      coalesce("abhorrent"."ss_ticket_number","premium"."sr_data_sr_ticket_number") as "sr_data_sr_ticket_number",
      coalesce("abhorrent"."ss_ticket_number","premium"."sr_data_sr_ticket_number") as "ss_ticket_number"
  FROM
      "abhorrent"
      INNER JOIN "premium" on "abhorrent"."ss_customer_id" = "premium"."sr_data_cid" AND "abhorrent"."ss_item_sk" = "premium"."sr_data_item_sk" AND "abhorrent"."ss_ticket_number" = "premium"."sr_data_sr_ticket_number"
  WHERE
      "abhorrent"."ss_date_year" = 1999 and "abhorrent"."ss_date_month_of_year" = 9 and "premium"."sr_data_sr_qty" is not null
  ),
  scrawny as (
  SELECT
      "macho"."ss_item_sk" as "ss_item_sk",
      "macho"."ss_ticket_number" as "ss_ticket_number",
      coalesce("macho"."cs_agg_iid","macho"."ss_item_id","questionable"."cs_agg_iid") as "cs_agg_iid",
      coalesce("macho"."cs_agg_iid","macho"."ss_item_id","questionable"."cs_agg_iid") as "ss_item_id",
      sum("macho"."ss_quantity") as "store_ticket_quantity"
  FROM
      "macho"
      FULL JOIN "questionable" on "macho"."ss_customer_id" = "questionable"."cs_agg_cid" AND "macho"."ss_item_id" = "questionable"."cs_agg_iid"
  GROUP BY
      1,
      2,
      3,
      "macho"."ss_item_desc",
      "macho"."ss_store_id",
      "macho"."ss_store_name"),
  rambunctious as (
  SELECT
      "questionable"."cs_agg_cid" as "cs_agg_cid",
      "questionable"."cs_agg_cs_qty" as "cs_agg_cs_qty",
      "questionable"."cs_agg_iid" as "cs_agg_iid",
      "waggish"."sr_data_item_sk" as "sr_data_item_sk",
      "waggish"."sr_data_sr_qty" as "sr_data_sr_qty",
      "waggish"."sr_data_sr_ticket_number" as "sr_data_sr_ticket_number",
      "waggish"."ss_item_desc" as "ss_item_desc",
      "waggish"."ss_item_id" as "ss_item_id",
      "waggish"."ss_store_id" as "ss_store_id",
      "waggish"."ss_store_name" as "ss_store_name"
  FROM
      "waggish"
      INNER JOIN "questionable" on "waggish"."ss_customer_id" = "questionable"."cs_agg_cid" AND "waggish"."ss_item_id" = "questionable"."cs_agg_iid"
  WHERE
      ("waggish"."ss_customer_id", "waggish"."ss_item_id") in (select questionable."cs_agg_cid", questionable."cs_agg_iid" from questionable where questionable."cs_agg_cid" is not null and questionable."cs_agg_iid" is not null)
  ),
  kaput as (
  SELECT
      "abhorrent"."cs_agg_cid" as "cs_agg_cid",
      "abhorrent"."ss_customer_id" as "ss_customer_id",
      "abhorrent"."ss_item_desc" as "ss_item_desc",
      "abhorrent"."ss_store_id" as "ss_store_id",
      "abhorrent"."ss_store_name" as "ss_store_name",
      "scrawny"."store_ticket_quantity" as "store_ticket_quantity",
      coalesce("abhorrent"."cs_agg_iid","scrawny"."cs_agg_iid") as "cs_agg_iid",
      coalesce("abhorrent"."ss_item_id","scrawny"."ss_item_id") as "ss_item_id",
      coalesce("abhorrent"."ss_item_sk","scrawny"."ss_item_sk") as "ss_item_sk",
      coalesce("abhorrent"."ss_ticket_number","scrawny"."ss_ticket_number") as "ss_ticket_number"
  FROM
      "abhorrent"
      LEFT OUTER JOIN "scrawny" on "abhorrent"."ss_item_id" is not distinct from "scrawny"."ss_item_id" AND "abhorrent"."ss_item_sk" is not distinct from "scrawny"."ss_item_sk" AND "abhorrent"."ss_ticket_number" is not distinct from "scrawny"."ss_ticket_number"
  WHERE
      "abhorrent"."ss_date_year" = 1999 and "abhorrent"."ss_date_month_of_year" = 9
  ),
  sedate as (
  SELECT
      "rambunctious"."cs_agg_cid" as "cs_agg_cid",
      "rambunctious"."cs_agg_iid" as "cs_agg_iid",
      "rambunctious"."ss_item_desc" as "ss_item_desc",
      "rambunctious"."ss_item_id" as "ss_item_id",
      "rambunctious"."ss_store_id" as "ss_store_id",
      "rambunctious"."ss_store_name" as "ss_store_name",
      sum("rambunctious"."cs_agg_cs_qty") as "catalog_ticket_quantity"
  FROM
      "rambunctious"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6),
  puffy as (
  SELECT
      "rambunctious"."cs_agg_iid" as "cs_agg_iid",
      "rambunctious"."cs_agg_iid" as "ss_item_id",
      "rambunctious"."sr_data_item_sk" as "sr_data_item_sk",
      "rambunctious"."sr_data_sr_ticket_number" as "sr_data_sr_ticket_number",
      "rambunctious"."ss_store_id" as "ss_store_id",
      "rambunctious"."ss_store_name" as "ss_store_name",
      sum("rambunctious"."sr_data_sr_qty") as "store_return_quantity"
  FROM
      "rambunctious"
  GROUP BY
      2,
      3,
      4,
      5,
      6,
      "rambunctious"."ss_item_desc"),
  puzzled as (
  SELECT
      "kaput"."ss_customer_id" as "ss_customer_id",
      "kaput"."ss_item_desc" as "ss_item_desc",
      "kaput"."ss_item_id" as "ss_item_id",
      "kaput"."ss_store_id" as "ss_store_id",
      "kaput"."ss_store_name" as "ss_store_name",
      "kaput"."store_ticket_quantity" as "store_ticket_quantity",
      "questionable"."cs_agg_cid" as "cs_agg_cid",
      "questionable"."cs_agg_iid" as "cs_agg_iid",
      coalesce("kaput"."cs_agg_cid","premium"."sr_data_cid") as "sr_data_cid",
      coalesce("kaput"."ss_item_sk","premium"."sr_data_item_sk") as "sr_data_item_sk",
      coalesce("kaput"."ss_item_sk","premium"."sr_data_item_sk") as "ss_item_sk",
      coalesce("kaput"."ss_ticket_number","premium"."sr_data_sr_ticket_number") as "sr_data_sr_ticket_number",
      coalesce("kaput"."ss_ticket_number","premium"."sr_data_sr_ticket_number") as "ss_ticket_number"
  FROM
      "kaput"
      INNER JOIN "premium" on "kaput"."ss_customer_id" = "premium"."sr_data_cid" AND "kaput"."ss_item_sk" = "premium"."sr_data_item_sk" AND "kaput"."ss_ticket_number" = "premium"."sr_data_sr_ticket_number"
  WHERE
      ("kaput"."ss_customer_id", "kaput"."ss_item_id") in (select questionable."cs_agg_cid", questionable."cs_agg_iid" from questionable where questionable."cs_agg_cid" is not null and questionable."cs_agg_iid" is not null) and "premium"."sr_data_sr_qty" is not null
  ),
  hard as (
  SELECT
      "puffy"."store_return_quantity" as "store_return_quantity",
      "puzzled"."cs_agg_cid" as "cs_agg_cid",
      "puzzled"."ss_customer_id" as "ss_customer_id",
      "puzzled"."ss_item_desc" as "ss_item_desc",
      "puzzled"."ss_item_id" as "ss_item_id",
      "puzzled"."ss_store_id" as "ss_store_id",
      "puzzled"."ss_store_name" as "ss_store_name",
      "puzzled"."store_ticket_quantity" as "store_ticket_quantity",
      coalesce("puffy"."cs_agg_iid","puzzled"."cs_agg_iid") as "cs_agg_iid"
  FROM
      "puzzled"
      LEFT OUTER JOIN "puffy" on "puzzled"."sr_data_sr_ticket_number" is not distinct from "puffy"."sr_data_sr_ticket_number" AND "puzzled"."ss_item_id" = "puffy"."ss_item_id" AND "puzzled"."ss_item_sk" is not distinct from "puffy"."sr_data_item_sk" AND "puzzled"."ss_store_id" is not distinct from "puffy"."ss_store_id" AND "puzzled"."ss_store_name" is not distinct from "puffy"."ss_store_name")
  SELECT
      coalesce("hard"."ss_item_id","sedate"."ss_item_id") as "item_code",
      coalesce("hard"."ss_item_desc","sedate"."ss_item_desc") as "item_description",
      coalesce("hard"."ss_store_id","sedate"."ss_store_id") as "store_code",
      coalesce("hard"."ss_store_name","sedate"."ss_store_name") as "store_name",
      "hard"."store_ticket_quantity" as "store_ticket_quantity",
      "hard"."store_return_quantity" as "store_return_quantity",
      "sedate"."catalog_ticket_quantity" as "catalog_ticket_quantity"
  FROM
      "hard"
      FULL JOIN "sedate" on "hard"."ss_customer_id" = "sedate"."cs_agg_cid" AND "hard"."ss_item_desc" is not distinct from "sedate"."ss_item_desc" AND "hard"."ss_item_id" = "sedate"."ss_item_id" AND "hard"."ss_store_id" is not distinct from "sedate"."ss_store_id" AND "hard"."ss_store_name" is not distinct from "sedate"."ss_store_name"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7
  ORDER BY
      1 asc,
      2 asc,
      3 asc,
      4 asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query29.preql`

  ```text
  Resolution error in query29.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['cs.billing_customer.id', 'cs.item.id', 'sr.item.sk', 'sr.return_quantity', 'sr.ticket_number', 'ss.date.month_of_year', 'ss.date.year', 'ss.item.desc', 'ss.quantity', 'ss.store.id', 'ss.store.name', 'cs.billing_customer.sk', 'ss.date.sk', 'ss.store.sk'], have {'sr.billing_customer.sk'} or {'cs.item.sk'} or {'ss.customer.sk'}. Different paths are is: [{'sr.billing_customer.sk'}, {'cs.item.sk'}, set()]")
  ```
- `trilogy run query29.preql`

  ```text
  Syntax error in query29.preql: Undefined concept: sr_data.sr_qty (line 25, in SELECT). Suggestions: ['sr_data.cid', 'sr_data.item_sk', 'cs_agg.cs_qty']
  ```
- `trilogy run query29.preql`

  ```text
  Referenced table \"questionable\" not found!\nCandidate tables: \"concerned\"\n\nLINE 244:     \"questionable\".\"cs_agg_cid\" as \"cs_agg_cid\",\n              ^\n[SQL: \nWITH \ncharming as (\nSELECT\n    \"ss_store_sales\".\"SS_CUSTOMER_SK\" as \"ss_customer_sk\",\n    \"ss_store_sales\".\"SS_SOLD_DATE_SK\" as \"ss_date_sk\",\n    \"ss_store_sales\".\"SS_STORE_SK\" as \"ss_store_sk\",\n    \"ss_store_sales\".\"SS_TICKET_NUMBER\" as \"ss_ticket_number\"\nFROM\n    \"store_sales\" as \"ss_store_sales\"\nGROUP BY\n    1,\n    2,\n    3,\n    4),\nmacho as (\nSELECT\n    \"ss_store_sales\".\"SS
  …
  \"puffy\".\"ss_store_name\" is not distinct from \"yellow\".\"ss_store_name\"\nGROUP BY\n    1,\n    2,\n    3,\n    4,\n    5,\n    6,\n    7\nORDER BY \n    1 asc,\n    2 asc,\n    3 asc,\n    4 asc\nLIMIT (100)]\n(Background on this error at: https://sqlalche.me/e/20/f405)",
    "error_type": "ProgrammingError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 19.39,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query35.preql`

  ```text
  Resolution error in query35.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 32). The requested concepts split into 2 disconnected subgraphs: {avg_col_dep, avg_dep, avg_emp_dep, college_dependent_count, dependent_count, employed_dependent_count, gender, marital_status, max_col_dep, max_dep, max_emp_dep, min_col_dep, min_dep, min_emp_dep, state}; {customer_count, customer_count2, customer_count3, store_custs.cust_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query37.preql`

  ```text
  Resolution error in query37.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {catalog_sales.item.current_price, catalog_sales.item.id, catalog_sales.item.manufacturer_id, current_price, description, item_code}; {inventory.date.date, inventory.quantity_on_hand}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query39.preql`

  ```text
  Resolution error in query39.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 3 disconnected subgraphs: {inv.date.year, _monthly_stats_mean_qoh, _monthly_stats_month_of_year, _monthly_stats_stddev_qoh}; {_monthly_stats_item_id}; {_monthly_stats_warehouse_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query40.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: store.sk. Suggestions: ['ss.store.sk', 'ss.store.date.sk', 'ss.time.sk', 'ss.item.sk', 'ss.date.sk', 'ss.return_date.sk']
  ```
- `trilogy run query44.preql`

  ```text
  Syntax error in query44.preql: Undefined concept: qualified_items.product_name. Suggestions: ['qualified_items.item_product_name', 'qualified_items.profit_threshold', 'qualified_items.item_avg_profit', 'item_product_name', 'ss.item.product_name']
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run query45.preql`

  ```text
  Syntax error in query45.preql: Undefined concept: ws.date (line 6, in WHERE). Suggestions: ['ws.date.sk', 'ws.date.id', 'ws.date._date_string', 'ws.date.date', 'ws.date.year', 'ws.date.day_of_week']
  ```
- `trilogy file read query45.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query47.preql`

  ```text
  Syntax error in query47.preql: Undefined concept: monthly_totals.ss.store.name (line 76, col 5, in ORDER BY). Suggestions: ['monthly_totals.store_name', 'monthly_totals.ss.store.company_name', 'monthly_totals.ss.item.brand_name', '_monthly_totals_store_name', 'ss.store.name', 'ss.return_store.name']
  ```
- `trilogy run query47.preql`

  ```text
  Syntax error in query47.preql: Undefined concept: monthly_totals.ss.store.name. Suggestions: ['monthly_totals.store_name', 'monthly_totals.ss.store.company_name', 'monthly_totals.ss.item.brand_name', '_monthly_totals_store_name', 'ss.store.name', 'ss.return_store.name']
  ```
- `trilogy file read query50.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query29_verify9.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 28). The requested concepts split into 3 disconnected subgraphs: {c.address.county, c.address.state}; {customer_id, is_qualifying, total_ext_sales_price, ss.date.month_seq}; {st.county, st.state}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 78 column 3 (char 2524). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 31). The requested concepts split into 2 disconnected subgraphs: {cust_city, cust_first_sales_year, cust_first_ship_year, cust_street_name, cust_street_number, cust_zip, sale_lines, sale_year, sale_zip, total_coupon, total_list, total_wholesale, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.is_returned, ss.item.color, ss.item.current_price, ss.item.id, ss.item.product_name, ss.item.sk, ss.sale_address.city, ss.sale_address.street_name, ss.sale_address.street_number, ss.store.name, ss.store.zip}; {item_cat_ok}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Unexpected error in query64.preql: (_duckdb.BinderException) Binder Error: Cannot compare values of type VARCHAR and BIGINT in IN/ANY/ALL clause - an explicit cast is required

  LINE 77: ...phics"."CD_MARITAL_STATUS" and "ss_item_items"."I_ITEM_ID" in (select questionable."catalog_qualifying_items_item_sk...
                                                                         ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "cs_catalog_sales"."CS_ITEM_SK" as "_catalog_sales_by_item_item_sk",
      sum("cs_catalog_sales"."CS_EXT_LIST_PRICE") as "_catalog_sales_by_item_total_ext_list"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  GROUP BY
      1),
  quizzical as (
  SELECT
      "cr_catalog_returns"."CR_ITEM_SK" as "_catalog_returns_by_item_item_sk",
      sum(( coalesce("cr_catalog_returns"."CR_REFUNDED_CASH",0) + coalesce("cr_catalog_returns"."CR_REVERSED_CHARGE",0) ) + coalesce("cr_catalog_returns"."CR_STORE_CREDIT",0)) as "_catalog_returns_by_item_total_refund"
  FROM
      "catalog_returns" as "cr_catalog_returns"
  GROUP BY
      1),
  cooperative as (
  SELECT
      "cheerful"."_catalog_sales_by_item_item_sk" as "catalog_sales_by_item_item_sk",
      "cheerful"."_catalog_sales_by_item_total_ext_list" as "catalog_sales_by_item_total_ext_list"
  FROM
      "cheerful"),
  wakeful as (
  SELECT
      "quizzical"."_catalog_returns_by_item_item_sk" as "catalog_returns_by_item_item_sk",
      "quizzical"."_catalog_returns_by_item_total_refund" as "catalog_returns_by_item_total_refund"
  FROM
      "quizzical"),
  questionable as (
  SELECT
      coalesce("cooperative"."catalog_sales_by_item_item_sk","wakeful"."catalog_returns_by_item_item_sk") as "catalog_qualifying_items_item_sk"
  FROM
      "cooperative"
      LEFT OUTER JOIN "wakeful" on "cooperative"."catalog_sales_by_item_item_sk" = "wakeful"."catalog_returns_by_item_item_sk"
  WHERE
      "cooperative"."catalog_sales_by_item_total_ext_list" > 2 * coalesce("wakeful"."catalog_returns_by_item_total_refund",0)

  GROUP BY
      1),
  kaput as (
  SELECT
      "ss_customer_address_customer_address"."CA_CITY" as "ss_customer_address_city",
      "ss_customer_address_customer_address"."CA_STREET_NAME" as "ss_customer_address_street_name",
      "ss_customer_address_customer_address"."CA_STREET_NUMBER" as "ss_customer_address_street_number",
      "ss_customer_address_customer_address"."CA_ZIP" as "ss_customer_address_zip",
      "ss_customer_first_sales_date_date"."D_YEAR" as "ss_customer_first_sales_date_year",
      "ss_customer_first_shipto_date_date"."D_YEAR" as "ss_customer_first_shipto_date_year",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_item_items"."I_ITEM_ID" as "ss_item_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
      "ss_sale_address_customer_address"."CA_CITY" as "ss_sale_address_city",
      "ss_sale_address_customer_address"."CA_STREET_NAME" as "ss_sale_address_street_name",
      "ss_sale_address_customer_address"."CA_STREET_NUMBER" as "ss_sale_address_street_number",
      "ss_sale_address_customer_address"."CA_ZIP" as "ss_sale_address_zip",
      "ss_store_sales"."SS_COUPON_AMT" as "ss_coupon_amt",
      "ss_store_sales"."SS_LIST_PRICE" as "ss_list_price",
      "ss_store_sales"."SS_WHOLESALE_COST" as "ss_wholesale_cost",
      "ss_store_store"."S_STORE_NAME" as "ss_store_name",
      "ss_store_store"."S_ZIP" as "ss_store_zip",
      CONCAT(cast(coalesce("ss_store_returns"."SR_TICKET_NUMBER","ss_store_sales"."SS_TICKET_NUMBER") as string), '-', cast(coalesce("ss_item_items"."I_ITEM_SK","ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK") as string)) as "ss_line_item"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      INNER JOIN "customer_demographics" as "ss_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_customer_demographic_customer_demographics"."CD_DEMO_SK"
      INNER JOIN "customer_demographics" as "ss_customer_demographics_customer_demographics" on "ss_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_customer_demographics_customer_demographics"."CD_DEMO_SK"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      LEFT OUTER JOIN "store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
      LEFT OUTER JOIN "customer_address" as "ss_sale_address_customer_address" on "ss_store_sales"."SS_ADDR_SK" = "ss_sale_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_sales_date_date" on "ss_customer_customers"."C_FIRST_SALES_DATE_SK" = "ss_customer_first_sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_shipto_date_date" on "ss_customer_customers"."C_FIRST_SHIPTO_DATE_SK" = "ss_customer_first_shipto_date_date"."D_DATE_SK"
  WHERE
      "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium') and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 and ("ss_store_returns"."SR_TICKET_NUMBER" is not null) and "ss_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" != "ss_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" and "ss_item_items"."I_ITEM_ID" in (select questionable."catalog_qualifying_items_item_sk" from questionable where questionable."catalog_qualifying_items_item_sk" is not null)

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
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      coalesce("ss_item_items"."I_ITEM_SK","ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK"),
      coalesce("ss_store_returns"."SR_TICKET_NUMBER","ss_store_sales"."SS_TICKET_NUMBER"))
  SELECT
      "kaput"."ss_item_product_name" as "ss_item_product_name",
      "kaput"."ss_item_id" as "ss_item_id",
      "kaput"."ss_store_name" as "ss_store_name",
      "kaput"."ss_store_zip" as "ss_store_zip",
      "kaput"."ss_sale_address_street_number" as "ss_sale_address_street_number",
      "kaput"."ss_sale_address_street_name" as "ss_sale_address_street_name",
      "kaput"."ss_sale_address_city" as "ss_sale_address_city",
      "kaput"."ss_sale_address_zip" as "sale_zip",
      "kaput"."ss_customer_address_street_number" as "cust_street_number",
      "kaput"."ss_customer_address_street_name" as "cust_street_name",
      "kaput"."ss_customer_address_city" as "cust_city",
      "kaput"."ss_customer_address_zip" as "cust_zip",
      "kaput"."ss_date_year" as "sale_year",
      "kaput"."ss_customer_first_sales_date_year" as "cust_first_sales_year",
      "kaput"."ss_customer_first_shipto_date_year" as "cust_first_ship_year",
      count("kaput"."ss_line_item") as "sale_lines",
      sum("kaput"."ss_wholesale_cost") as "total_wholesale",
      sum("kaput"."ss_list_price") as "total_list",
      sum("kaput"."ss_coupon_amt") as "total_coupon"
  FROM
      "kaput"
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
      13,
      14,
      15
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query65.preql`

  ```text
  Syntax error in query65.preql: ORDER BY references 'ss.store.id', which is not in the SELECT projection (line 10). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.store.id order by ss.store.id asc`.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query66.preql`

  ```text
  Import error in query66.preql: Unable to import '.\all_sales.preql': [Errno 2] No such file or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy file read query68.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales select top_states.state, top_states.state_profit where store_sales.date.year = 2000 and store_sales.store.sk is not null and store_sales.store.state in top_states.state limit 10;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - top_states.state (line 2, col 8, in SELECT); did you mean: top_states.state_profit, store_sales.customer.address.state, store_sales.return_customer.address.state, store_sales.store.state, store_sales.return_store.state, store_sales.sale_address.state?
    - top_states.state_profit (line 2, col 26, in SELECT); did you mean: top_states.state, store_sales.net_profit?
    - top_states.state (line 2, col 153, in WHERE); did you mean: top_states.state_profit, store_sales.customer.address.state, store_sales.return_customer.address.state, store_sales.store.state, store_sales.return_store.state, store_sales.sale_address.state?
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: (_duckdb.BinderException) Binder Error: aggregate function calls cannot be nested

  LINE 34:     MIN(sum("cheerful"."store_sales_net_profit")) desc
                   ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_store_sales"."SS_NET_PROFIT" as "store_sales_net_profit",
      "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_store_sales"."SS_STORE_SK" is not null
  ),
  abundant as (
  SELECT
      "cheerful"."store_sales_store_county" as "store_sales_store_county",
      "cheerful"."store_sales_store_state" as "store_sales_store_state",
      grouping("cheerful"."store_sales_store_county") as "_rolled_g_county",
      grouping("cheerful"."store_sales_store_state") as "_rolled_g_state",
      sum("cheerful"."store_sales_net_profit") as "_rolled_total_net_profit"
  FROM
      "cheerful"
  GROUP BY
      ROLLUP (2, 1)),
  thoughtful as (
  SELECT
      "cheerful"."store_sales_store_state" as "_top_states_state"
  FROM
      "cheerful"
  GROUP BY
      1
  ORDER BY
      MIN(sum("cheerful"."store_sales_net_profit")) desc
  LIMIT (5)),
  questionable as (
  SELECT
      "thoughtful"."_top_states_state" as "top_states_state"
  FROM
      "thoughtful"),
  uneven as (
  SELECT
      "abundant"."_rolled_total_net_profit" as "rolled_total_net_profit",
      "abundant"."store_sales_store_county" as "rolled_store_sales_store_county",
      "abundant"."store_sales_store_state" as "rolled_store_sales_store_state",
      ( "abundant"."_rolled_g_state" * 2 ) + "abundant"."_rolled_g_county" as "level",
      CASE
  	WHEN "abundant"."_rolled_g_county" = 0 THEN "abundant"."store_sales_store_state"
  	ELSE null
  	END as "parent"
  FROM
      "abundant"
  WHERE
      "abundant"."store_sales_store_state" in (select questionable."top_states_state" from questionable where questionable."top_states_state" is not null) or "abundant"."_rolled_g_state" = 1
  ),
  vacuous as (
  SELECT
      "uneven"."parent" as "parent",
      "uneven"."rolled_store_sales_store_county" as "rolled_store_sales_store_county",
      "uneven"."rolled_store_sales_store_state" as "rolled_store_sales_store_state"
  FROM
      "uneven"),
  yummy as (
  SELECT
      "uneven"."level" as "level",
      "uneven"."parent" as "parent",
      "uneven"."rolled_store_sales_store_county" as "rolled_store_sales_store_county",
      "uneven"."rolled_store_sales_store_state" as "rolled_store_sales_store_state",
      "uneven"."rolled_total_net_profit" as "rolled_total_net_profit"
  FROM
      "uneven"),
  juicy as (
  SELECT
      "yummy"."level" as "level",
      "yummy"."parent" as "parent",
      "yummy"."rolled_store_sales_store_county" as "rolled_store_sales_store_county",
      "yummy"."rolled_store_sales_store_state" as "rolled_store_sales_store_state",
      "yummy"."rolled_total_net_profit" as "rolled_total_net_profit",
      rank() over (partition by "yummy"."level","yummy"."parent" order by "yummy"."rolled_total_net_profit" desc ) as "rnk"
  FROM
      "yummy")
  SELECT
      "juicy"."rolled_store_sales_store_state" as "rolled_store_sales_store_state",
      "juicy"."rolled_store_sales_store_county" as "rolled_store_sales_store_county",
      "juicy"."rolled_total_net_profit" as "rolled_total_net_profit",
      "juicy"."level" as "level",
      "juicy"."rnk" as "rnk"
  FROM
      "vacuous"
      INNER JOIN "juicy" on "vacuous"."parent" is not distinct from "juicy"."parent" AND "vacuous"."rolled_store_sales_store_county" = "juicy"."rolled_store_sales_store_county" AND "vacuous"."rolled_store_sales_store_state" = "juicy"."rolled_store_sales_store_state"
  ORDER BY
      "juicy"."level" desc,
      "juicy"."rolled_store_sales_store_state" asc nulls first,
      "juicy"."rnk" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: (_duckdb.BinderException) Binder Error: aggregate function calls cannot be nested

  LINE 54:     MIN(sum("cheerful"."store_sales_net_profit")) desc
                   ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_store_sales"."SS_NET_PROFIT" as "store_sales_net_profit",
      "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_store_sales"."SS_STORE_SK" is not null
  ),
  juicy as (
  SELECT
      "cheerful"."store_sales_store_county" as "store_sales_store_county",
      "cheerful"."store_sales_store_state" as "store_sales_store_state",
      sum("cheerful"."store_sales_net_profit") as "total_net_profit"
  FROM
      "cheerful"
  GROUP BY
      ROLLUP (2, 1)),
  abundant as (
  SELECT
      "cheerful"."store_sales_store_county" as "store_sales_store_county",
      "cheerful"."store_sales_store_state" as "store_sales_store_state",
      CASE
  	WHEN CASE
  	WHEN grouping("cheerful"."store_sales_store_state") = 1 and grouping("cheerful"."store_sales_store_county") = 1 THEN 2
  	WHEN grouping("cheerful"."store_sales_store_county") = 1 THEN 1
  	ELSE 0
  	END = 0 THEN "cheerful"."store_sales_store_state"
  	ELSE null
  	END as "parent",
      CASE
  	WHEN grouping("cheerful"."store_sales_store_state") = 1 and grouping("cheerful"."store_sales_store_county") = 1 THEN 2
  	WHEN grouping("cheerful"."store_sales_store_county") = 1 THEN 1
  	ELSE 0
  	END as "level",
      sum("cheerful"."store_sales_net_profit") as "_virt_agg_sum_5987678758911430"
  FROM
      "cheerful"
  GROUP BY
      ROLLUP (2, 1)),
  thoughtful as (
  SELECT
      "cheerful"."store_sales_store_state" as "_top_states_state"
  FROM
      "cheerful"
  GROUP BY
      1
  ORDER BY
      MIN(sum("cheerful"."store_sales_net_profit")) desc
  LIMIT (5)),
  uneven as (
  SELECT
      "abundant"."level" as "level",
      "abundant"."store_sales_store_county" as "store_sales_store_county",
      "abundant"."store_sales_store_state" as "store_sales_store_state",
      rank() over (partition by "abundant"."level","abundant"."parent" order by "abundant"."_virt_agg_sum_5987678758911430" desc ) as "rnk"
  FROM
      "abundant"),
  questionable as (
  SELECT
      "thoughtful"."_top_states_state" as "top_states_state"
  FROM
      "thoughtful"),
  yummy as (
  SELECT
      "uneven"."level" as "level",
      "uneven"."rnk" as "rnk",
      "uneven"."store_sales_store_county" as "store_sales_store_county",
      "uneven"."store_sales_store_state" as "store_sales_store_state"
  FROM
      "uneven"
  GROUP BY
      1,
      2,
      3,
      4)
  SELECT
      "yummy"."store_sales_store_state" as "store_sales_store_state",
      "yummy"."store_sales_store_county" as "store_sales_store_county",
      "juicy"."total_net_profit" as "total_net_profit",
      "yummy"."level" as "level",
      "yummy"."rnk" as "rnk"
  FROM
      "juicy"
      INNER JOIN "yummy" on "juicy"."store_sales_store_county" is not distinct from "yummy"."store_sales_store_county" AND "juicy"."store_sales_store_state" is not distinct from "yummy"."store_sales_store_state"
  WHERE
      ( "yummy"."level" = 2 ) or ( "yummy"."level" = 1 ) or ( "yummy"."store_sales_store_state" in (select questionable."top_states_state" from questionable where questionable."top_states_state" is not null) )

  ORDER BY
      "yummy"."level" desc,
      "yummy"."store_sales_store_state" asc nulls first,
      "yummy"."rnk" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query70.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query72.preql`

  ```text
  Syntax error in query72.preql: Nothing was executed: parsed 4 definition statement(s) (2 imports, 2 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run query72.preql`

  ```text
  Resolution error in query72.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {inv.quantity_on_hand, inv.warehouse.name, sales_by_item_week.qualifying_sales.sale_week_seq, sales_by_item_week.total_qty}
  ```
- `trilogy run query72.preql`

  ```text
  Resolution error in query72.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 27). The requested concepts split into 2 disconnected subgraphs: {inv.warehouse.name}; {sales_by_item_week.qualifying_sales.item_desc, sales_by_item_week.qualifying_sales.item_sk, sales_by_item_week.qualifying_sales.sale_week_seq, sales_by_item_week.total_qty}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query72.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy database describe warehouse`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
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
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: ORDER BY references 'store_agg.st_wholesale', a measure at a finer grain (Grain<store_agg.cust_id,store_agg.item_id,store_agg.yr>) than the select grain (Grain<cat_agg.cust_id,cat_agg.item_id,cat_agg.yr,local.other_channel_quantity,local.other_channel_sales_price,local.other_channel_wholesale,local.ratio>|store_agg.st_qty is not MagicConstants.NULL and add(coalesce(web_agg.w_qty@Grain<web_agg.cust_id,web_agg.item_id,web_agg.yr>,0),coalesce(cat_agg.c_qty@Grain<cat_agg.cust_id,cat_agg.item_id,cat_agg.yr>,0)) > 0); it has no single value per output row. Project it (prefix with `--` to keep it out of the rows) and order by that alias instead.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.channel, all_sales.channel_dim_text_id limit 20;`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260709-105517_enriched\workspace\_worker_0\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 138188)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.channel, all_sales.channel_dim_text_id limit 20;`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260709-105517_enriched\workspace\_worker_0\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 138188)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.channel, all_sales.channel_dim_text_id limit 20;`

  ```text
  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy run query80.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260709-105517_enriched\workspace\_worker_0\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 138188)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query80.preql`

  ```text
  Unexpected error: (_duckdb.IOException) IO Error: Cannot open file "C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\20260709-105517_enriched\workspace\_worker_0\tpcds.duckdb": The process cannot access the file because it is being used by another process.

  File is already open in
  C:\Program Files\Python313\python.exe (PID 138188)
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query80.preql`

  ```text
  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy file read trilogy.toml`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file delete tpcds.duckdb.tmp`

  ```text
  tpcds.duckdb.tmp is a directory; pass recursive=True to remove it.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: `by rollup (…)` requires at least one aggregate (or re-aggregable pre-aggregated measure) in the select to group; found none.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: ORDER BY contains aggregate `grouping(combined.channel_label)` (line 44), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_label) as g order by g desc`.
  ```
- `trilogy run query80.preql`

  ```text
  Unexpected error in query80.preql: (_duckdb.BinderException) Binder Error: GROUPING function is not supported here
  [SQL:
  WITH
  questionable as (
  SELECT
      "cs_catalog_returns_inline"."CR_NET_LOSS" as "___tvf_arm_1_return_net_loss",
      "cs_catalog_returns_inline"."CR_RETURN_AMOUNT" as "___tvf_arm_1_return_amount",
      "cs_catalog_sales"."CS_EXT_SALES_PRICE" as "___tvf_arm_1_ext_sales_price",
      "cs_catalog_sales"."CS_NET_PROFIT" as "___tvf_arm_1_net_profit",
      $1 as "___tvf_arm_1_channel_label",
      CONCAT('catalog_page', "cs_catalog_page_catalog_page"."CP_CATALOG_PAGE_ID") as "___tvf_arm_1_outlet_identifier"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
      INNER JOIN "catalog_page" as "cs_catalog_page_catalog_page" on "cs_catalog_sales"."CS_CATALOG_PAGE_SK" = "cs_catalog_page_catalog_page"."CP_CATALOG_PAGE_SK"
      INNER JOIN "item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "catalog_returns" as "cs_catalog_returns_inline" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_catalog_returns_inline"."CR_ITEM_SK" AND "cs_catalog_sales"."CS_ORDER_NUMBER" = "cs_catalog_returns_inline"."CR_ORDER_NUMBER"
      LEFT OUTER JOIN "promotion" as "cs_promotion_promotion" on "cs_catalog_sales"."CS_PROMO_SK" = "cs_promotion_promotion"."P_PROMO_SK"
  WHERE
      cast("cs_sold_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' and "cs_item_items"."I_CURRENT_PRICE" > 50 and ( "cs_promotion_promotion"."P_CHANNEL_TV" = 'N' or "cs_promotion_promotion"."P_CHANNEL_TV" is null ) and "cs_catalog_page_catalog_page"."CP_CATALOG_PAGE_ID" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      6),
  sparkling as (
  SELECT
      "ss_store_returns"."SR_NET_LOSS" as "___tvf_arm_0_return_net_loss",
      "ss_store_returns"."SR_RETURN_AMT" as "___tvf_arm_0_return_amount",
      "ss_store_sales"."SS_EXT_SALES_PRICE" as "___tvf_arm_0_ext_sales_price",
      "ss_store_sales"."SS_NET_PROFIT" as "___tvf_arm_0_net_profit",
      $2 as "___tvf_arm_0_channel_label",
      CONCAT('store', "ss_store_store"."S_STORE_ID") as "___tvf_arm_0_outlet_identifier"
  FROM
      "store_sales" as "ss_store_sales"
      INNER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      INNER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "promotion" as "ss_promotion_promotion" on "ss_store_sales"."SS_PROMO_SK" = "ss_promotion_promotion"."P_PROMO_SK"
      LEFT OUTER JOIN "store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
  WHERE
      cast("ss_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' and "ss_item_items"."I_CURRENT_PRICE" > 50 and ( "ss_promotion_promotion"."P_CHANNEL_TV" = 'N' or "ss_promotion_promotion"."P_CHANNEL_TV" is null ) and "ss_store_store"."S_STORE_ID" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      6),
  divergent as (
  SELECT
      "ws_web_returns"."WR_NET_LOSS" as "___tvf_arm_2_return_net_loss",
      "ws_web_returns"."WR_RETURN_AMT" as "___tvf_arm_2_return_amount",
      "ws_web_sales"."WS_EXT_SALES_PRICE" as "___tvf_arm_2_ext_sales_price",
      "ws_web_sales"."WS_NET_PROFIT" as "___tvf_arm_2_net_profit",
      $3 as "___tvf_arm_2_channel_label",
      CONCAT('web_site', "ws_web_site_web_site"."web_site_id") as "___tvf_arm_2_outlet_identifier"
  FROM
      "web_sales" as "ws_web_sales"
      INNER JOIN "date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
      INNER JOIN "web_site" as "ws_web_site_web_site" on "ws_web_sales"."WS_WEB_SITE_SK" = "ws_web_site_web_site"."web_site_sk"
      INNER JOIN "item" as "ws_item_items" on "ws_web_sales"."WS_ITEM_SK" = "ws_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "web_returns" as "ws_web_returns" on "ws_web_sales"."WS_ITEM_SK" = "ws_web_returns"."WR_ITEM_SK" AND "ws_web_sales"."WS_ORDER_NUMBER" = "ws_web_returns"."WR_ORDER_NUMBER"
      LEFT OUTER JOIN "promotion" as "ws_promotion_promotion" on "ws_web_sales"."WS_PROMO_SK" = "ws_promotion_promotion"."P_PROMO_SK"
  WHERE
      cast("ws_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-22' and "ws_item_items"."I_CURRENT_PRICE" > 50 and ( "ws_promotion_promotion"."P_CHANNEL_TV" = 'N' or "ws_promotion_promotion"."P_CHANNEL_TV" is null ) and "ws_web_site_web_site"."web_site_id" is not null

  GROUP BY
      1,
      2,
      3,
      4,
      6),
  charming as (
  SELECT
      "questionable"."___tvf_arm_1_channel_label" as "_combined_channel_label",
      "questionable"."___tvf_arm_1_outlet_identifier" as "_combined_outlet_identifier",
      "questionable"."___tvf_arm_1_net_profit" as "_combined_net_profit",
      "questionable"."___tvf_arm_1_return_net_loss" as "_combined_return_net_loss",
      "questionable"."___tvf_arm_1_return_amount" as "_combined_return_amount",
      "questionable"."___tvf_arm_1_ext_sales_price" as "_combined_ext_sales_price"
  FROM
      "questionable"
  UNION ALL
  SELECT
      "sparkling"."___tvf_arm_0_channel_label" as "_combined_channel_label",
      "sparkling"."___tvf_arm_0_outlet_identifier" as "_combined_outlet_identifier",
      "sparkling"."___tvf_arm_0_net_profit" as "_combined_net_profit",
      "sparkling"."___tvf_arm_0_return_net_loss" as "_combined_return_net_loss",
      "sparkling"."___tvf_arm_0_return_amount" as "_combined_return_amount",
      "sparkling"."___tvf_arm_0_ext_sales_price" as "_combined_ext_sales_price"
  FROM
      "sparkling"
  UNION ALL
  SELECT
      "divergent"."___tvf_arm_2_channel_label" as "_combined_channel_label",
      "divergent"."___tvf_arm_2_outlet_identifier" as "_combined_outlet_identifier",
      "divergent"."___tvf_arm_2_net_profit" as "_combined_net_profit",
      "divergent"."___tvf_arm_2_return_net_loss" as "_combined_return_net_loss",
      "divergent"."___tvf_arm_2_return_amount" as "_combined_return_amount",
      "divergent"."___tvf_arm_2_ext_sales_price" as "_combined_ext_sales_price"
  FROM
      "divergent"),
  protective as (
  SELECT
      "charming"."_combined_channel_label" as "_combined_channel_label",
      "charming"."_combined_ext_sales_price" as "_combined_ext_sales_price",
      "charming"."_combined_net_profit" as "_combined_net_profit",
      "charming"."_combined_outlet_identifier" as "_combined_outlet_identifier",
      "charming"."_combined_return_amount" as "_combined_return_amount",
      "charming"."_combined_return_net_loss" as "_combined_return_net_loss"
  FROM
      "charming"),
  premium as (
  SELECT
      "protective"."_combined_channel_label" as "combined_channel_label",
      "protective"."_combined_ext_sales_price" as "combined_ext_sales_price",
      "protective"."_combined_net_profit" as "combined_net_profit",
      "protective"."_combined_outlet_identifier" as "combined_outlet_identifier",
      "protective"."_combined_return_amount" as "combined_return_amount",
      "protective"."_combined_return_net_loss" as "combined_return_net_loss"
  FROM
      "protective")
  SELECT
      "premium"."combined_channel_label" as "combined_channel_label",
      "premium"."combined_outlet_identifier" as "combined_outlet_identifier",
      sum("premium"."combined_ext_sales_price") as "sales",
      coalesce(sum("premium"."combined_return_amount"),0) as "returns",
      sum("premium"."combined_net_profit") - coalesce(sum("premium"."combined_return_net_loss"),0) as "profit"
  FROM
      "premium"
  GROUP BY
      ROLLUP (1, 2)
  ORDER BY
      MIN(grouping("premium"."combined_channel_label")) desc,
      "premium"."combined_channel_label" asc nulls first,
      MIN(grouping("premium"."combined_outlet_identifier")) desc,
      "premium"."combined_outlet_identifier" asc nulls first
  LIMIT (100)]
  [parameters: ('catalog channel', 'store channel', 'web channel')]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query82.preql`

  ```text
  Syntax error in query82.preql: Undefined concept: inventory.date.date.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 46 column 12 (char 1291). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw.store_returns:sr --import raw.catalog_returns:cr select sr.return_date.week_seq, cr.date.week_seq limit 3;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cr.date.week_seq}; {sr.return_date.week_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query84.preql`

  ```text
  Resolution error in query84.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {c.address.city, c.demographics.sk, c.household_demographic.income_band.lower_bound, c.household_demographic.income_band.upper_bound, customer_code, customer_full_name}; {sr.customer_demographic.sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query89.preql`

  ```text
  Syntax error in query89.preql: 17 undefined concept references; fix all before re-running:
    - item.category (line 13, col 5, in SELECT); did you mean: ss.item.category, item.class, ss.item.category_id, ss.item.color?
    - item.class (line 14, col 5, in SELECT); did you mean: ss.item.class, item.category, ss.item.class_id, ss.item.color?
    - item.brand_name (line 15, col 5, in SELECT); did you mean: ss.item.brand_name, ss.item.brand_id, ss.item.product_name?
    - store.name (line 16, col 5, in SELECT); did you mean: ss.store.name, store.company_name, ss.store.street_name, ss.return_store.name?
    - store.company_name (line 17, col 5, in SELECT); did you mean: ss.store.company_name, store.name, ss.return_store.company_name, ss.store.company_id?
    - date.month_of_year (line 18, col 5, in SELECT); did you mean: ss.date.month_of_year, ss.store.date.month_of_year, ss.return_store.date.month_of_year, date.year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year?
    - date.year (line 7, col 5, in WHERE); did you mean: ss.date.year, ss.store.date.year, ss.return_store.date.year, date.month_of_year, ss.return_date.year, ss.customer.first_sales_date.year?
    - item.category (line 9, col 10, in WHERE); did you mean: ss.item.category, item.class, ss.item.category_id, ss.item.color?
    - item.class (line 9, col 64, in WHERE); did you mean: ss.item.class, item.category, ss.item.class_id, ss.item.color?
    - item.category (line 10, col 13, in WHERE); did you mean: ss.item.category, item.class, ss.item.category_id, ss.item.color?
    - item.class (line 10, col 60, in WHERE); did you mean: ss.item.class, item.category, ss.item.class_id, ss.item.color?
    - store.name (line 23, col 41, in ORDER BY); did you mean: ss.store.name, store.company_name, ss.store.street_name, ss.return_store.name?
    - item.category (line 23, col 53, in ORDER BY); did you mean: ss.item.category, item.class, ss.item.category_id, ss.item.color?
    - item.class (line 23, col 68, in ORDER BY); did you mean: ss.item.class, item.category, ss.item.class_id, ss.item.color?
    - item.brand_name (line 23, col 80, in ORDER BY); did you mean: ss.item.brand_name, ss.item.brand_id, ss.item.product_name?
    - store.company_name (line 23, col 97, in ORDER BY); did you mean: ss.store.company_name, store.name, ss.return_store.company_name, ss.store.company_id?
    - date.month_of_year (line 23, col 117, in ORDER BY); did you mean: ss.date.month_of_year, ss.store.date.month_of_year, ss.return_store.date.month_of_year, date.year, ss.return_date.month_of_year, ss.customer.first_sales_date.month_of_year?
  ```
- `trilogy file read query91.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset daily_sales as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...y_of_week (web+catalog only)
   ??? rowset daily_sales as
   where s...

  Write stats: received 1324 chars / 1324 bytes; tail: …'53 = fut.ws\r\\nand cur.dow = fut.dow\r\\norder by cur.ws asc\r\\n;\r\\n'.
  ```
- `trilogy run --import raw/date:d select d.year, min(d.week_seq) as min_ws, max(d.week_seq) as max_ws group by d.year order by d.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ws, max(d.week_seq) as max_ws ??? group by d.year order by d.yea...
  ```
- `trilogy run --import raw/all_sales:sales where sales.channel in ('WEB','CATALOG') select sales.date.year, count(distinct sales.date.week_seq) as num_weeks, sum(sales.ext_sales_price) as total_sales group by sales.date.year order by sales.date.year;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...t_sales_price) as total_sales ??? group by sales.date.year order...
  ```
- `trilogy run --import raw/all_sales:all_sales select distinct all_sales.channel, all_sales.channel_dim_text_id limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ll_sales as all_sales; select ??? distinct all_sales.channel, al...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, count(1) as cnt where all_sales.date.date between '2000-08-23' and '2000-09-06' group by 1,2 order by 1,2 limit 30;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...'2000-08-23' and '2000-09-06' ??? group by 1,2 order by 1,2 limi...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.return_channel_dim_text_id, count(1) as cnt where all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.is_returned is not null group by 1 order by 2 desc limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sales.is_returned is not null ??? group by 1 order by 2 desc lim...
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, count(1) as cnt where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date group by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...::date and '2000-09-06'::date ??? group by 1;
  ```
- `trilogy file write query08.preql --content import raw.store_sales as ss;

# Declare the zips parameter
parameter zips string;

# Split zips into an array
aut…ed_customer_zips.zip in zips_array)
select
    ss.store.name as store_name,
    sum(ss.net_profit) as total_net_profit
order by store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:10
     |
  27 |          where preferred_customer_zips.preferred_count > 10
     |          ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...mer_zips.zip, 1, 2)           ??? where preferred_customer_zips....

  Write stats: received 1164 chars / 1164 bytes; tail: …'t_profit) as total_net_profit\\norder by store_name\\nlimit 100;'.
  ```
- `trilogy run --import raw.customer:customer select customer.address.county, count(customer.sk) as cnt where customer.address.county in ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County') group by customer.address.county;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...a County', 'La Porte County') ??? group by customer.address.coun...
  ```
- `trilogy run --import raw.all_sales:s where s.date.year between 1999 and 2001 select s.item.brand_id, s.item.class_id, s.item.category_id, count_distinct(s.channel) as n_channels group by s.item.brand_id, s.item.class_id, s.item.category_id having n_channels = 3 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...inct(s.channel) as n_channels ??? group by s.item.brand_id, s.it...
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Stage 1: Find (brand, class, category) combos present in all 3 channels during 1999-…
order by leaf_data.channel nulls first, leaf_data.brand_id nulls first, leaf_data.class_id nulls first, leaf_data.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ct overall_avg.avg_sale_value ??? from overall_avg) ;  # Stage 4...

  Write stats: received 1661 chars / 1661 bytes; tail: …'id nulls first, leaf_data.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:s select s.channel, count(*) as cnt, sum(s.ext_list_price) as total where s.date.year = 2001 and s.date.month_of_year = 11 group by s.channel;`

  ```text
  Syntax error in stdin: Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...sales as s; select s.channel, ??? count(*) as cnt, sum(s.ext_lis...
  ```
- `trilogy run --import raw.all_sales:s select s.channel, sum(s.ext_list_price) as total where s.date.year = 2001 and s.date.month_of_year = 11 and (s.item.bran…ear between 1999 and 2001 group by s2.item.brand_id, s2.item.class_id, s2.item.category_id having count_distinct(s2.channel) = 3) group by s.channel;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...class_id, s2.item.category_id ??? from raw.all_sales as s2 where...
  ```
- `trilogy file write query21.preql --content import raw/inventory.preql as inv;

# Filter to items with current_price between 0.99 and 1.49
# Before window: 20…and (after_total / before_total) between (2.0/3.0) and (3.0/2.0)
order by
    inv.warehouse.name nulls first,
    inv.item.id nulls first
limit 100;
`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/inventory.preql as inv;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/inventory.preql as inv;  #...

  Write stats: received 709 chars / 709 bytes; tail: …'se.name nulls first,\\n    inv.item.id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…
    sum(combined_sales.line_total) as total_sales
having
    total_sales > 0
order by
    last_name, first_name, total_sales nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:30
     |
  16 |     count(store_sales.item.id, substring(store_sales.item.desc, 1, 30), store_sales.date.date) > 4
     |                              ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
       count(store_sales.item.id ??? , substring(store_sales.item.d...

  Write stats: received 3447 chars / 3447 bytes; tail: …'  last_name, first_name, total_sales nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.last_name, ss.customer.first_name, ss.store.name, ss.customer.address.state, ss.store.state, ss.it…s.store.zip and lower(ss.customer.birth_country) != lower(ss.customer.address.country) group by 1,2,3,4,5,6,7,8,9,10 order by ss.item.color limit 50;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...(ss.customer.address.country) ??? group by 1,2,3,4,5,6,7,8,9,10
  ```
- `trilogy run --import raw.store_sales:ss select avg(sub_total) as avg_all from (select sum(ss.net_paid) as sub_total where ss.store.market = 8 and ss.is_returned = True and ss.customer.address.zip = ss.store.zip and lower(ss.customer.birth_country) != lower(ss.customer.address.country) group by 1 limit 10);`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ect avg(sub_total) as avg_all ??? from (select sum(ss.net_paid)
  ```
- `trilogy file write query28.preql --content import raw.store_sales as ss;

# Bucket 1: quantity 0-5, (list_price 8-18 OR coupon_amt 459-1459 OR wholesale_cost…price,
    bucket_6_count_list_price as bucket_6_count_list_price,
    bucket_6_count_distinct_list_price as bucket_6_count_distinct_list_price
by *;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | bucket_1_avg_list_price <- avg(ss.list_price ? ss.quantity between 0 and 5 and (ss.list_price between 8 and 18 or ss.coupon_amt between 459 and 1459 or ss.wholesale_cost between 57 and 77));
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...1459 OR wholesale_cost 57-77) ??? bucket_1_avg_list_price <- avg...

  Write stats: received 5554 chars / 5554 bytes; tail: …'tinct_list_price as bucket_6_count_distinct_list_price\\nby *;'.
  ```
- `trilogy file write query28.preql --content import raw.store_sales as ss;

select
    avg(ss.list_price ? ss.quantity between 0 and 5 and (ss.list_price betwe…ween 26 and 30 and (ss.list_price between 154 and 164 or ss.coupon_amt between 7326 and 8326 or ss.wholesale_cost between 7 and 27)) as b6_uniq
by *;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
    --> 22:1
     |
  22 | by *;
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...between 7 and 27)) as b6_uniq ??? by *;

  Write stats: received 3504 chars / 3504 bytes; tail: …'326 or ss.wholesale_cost between 7 and 27)) as b6_uniq\\nby *;'.
  ```
- `trilogy file write query29_verify4.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

with ss_sr …s.item.id as iid
;

select distinct
  ss_sr.cid as customer_id,
  ss_sr.iid as item_id
where (ss_sr.cid, ss_sr.iid) in (cs_pairs.cid, cs_pairs.iid)
;`

  ```text
  refused to write 'query29_verify4.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
    cs.item.id as iid ;  select ??? distinct   ss_sr.cid as custom...

  Write stats: received 673 chars / 673 bytes; tail: …'ere (ss_sr.cid, ss_sr.iid) in (cs_pairs.cid, cs_pairs.iid)\\n;'.
  ```
- `trilogy file write query29_verify8.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

# Check: wh…         where cs.sold_date.year in (1999, 2000, 2001))
select
  ss.customer.id as cid,
  ss.item.id as iid
;

select count(ss_cs_cross.cid) as cnt
;`

  ```text
  refused to write 'query29_verify8.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...omer.id, cs.item.id           ??? from catalog_sales cs

  Write stats: received 602 chars / 602 bytes; tail: …' ss.item.id as iid\\n;\\n\\nselect count(ss_cs_cross.cid) as cnt\\n;'.
  ```
- `trilogy file write query29_verify9.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

# Catalog s…omer.id as cid,
  ss.item.id as iid,
  ss.item.desc as item_desc,
  ss.store.id as store_id,
  ss.store.name as store_name,
  ss.quantity as ss_qty
;`

  ```text
  refused to write 'query29_verify9.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...sr.billing_customer.id        ??? from raw.store_returns sr

  Write stats: received 965 chars / 965 bytes; tail: …'id,\\n  ss.store.name as store_name,\\n  ss.quantity as ss_qty\\n;'.
  ```
- `trilogy file write query29.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

# Rowset A: qualify…ity,
  sum(a.sr_qty) as store_return_quantity,
  sum(b.cs_qty) as catalog_ticket_quantity
union join a.item_id = b.iid
order by 1, 2, 3, 4
limit 100;`

  ```text
  refused to write 'query29.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...omer.id, cs.item.id           ??? from raw.catalog_sales cs

  Write stats: received 1452 chars / 1452 bytes; tail: …'\\nunion join a.item_id = b.iid\\norder by 1, 2, 3, 4\\nlimit 100;'.
  ```
- `trilogy file write query29.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

# Catalog sales (cu…m_description,
  ss.store.id as store_code,
  ss.store.name as store_name,
  sum(ss.quantity) as store_ticket_quantity
order by 1, 2, 3, 4
limit 100;`

  ```text
  refused to write 'query29.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...tem_sk, sr_data.cid           ??? from sr_data           where s...

  Write stats: received 1193 chars / 1193 bytes; tail: …'ity) as store_ticket_quantity\\norder by 1, 2, 3, 4\\nlimit 100;'.
  ```
- `trilogy file write query36.preql --content import raw.store_sales as ss;

# Year 2001, stores in TN
where ss.date.year = 2001
  and ss.store.state = 'TN'

# …ierarchy_level is not null
order by hierarchy_level desc nulls first, ss.item.category asc nulls first, within_parent_rank asc nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...= 'TN'  # Pre-define concepts ??? auto g_cat <- grouping(ss.item...

  Write stats: received 1171 chars / 1171 bytes; tail: …'c nulls first, within_parent_rank asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Find (last_name, firs…ws.date.year = 2000
        and ws.billing_customer.id is not null
    )
) -> (last_name, first_name, sale_date)
;

select count(*) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...st_name, sale_date) ;  select ??? count(*) as cnt limit 100;

  Write stats: received 975 chars / 975 bytes; tail: …' first_name, sale_date)\\n;\\n\\nselect count(*) as cnt\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.date.year, count(ss.ticket_number) as cnt group by 1 order by 1;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ount(ss.ticket_number) as cnt ??? group by 1 order by 1;
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs --import raw.web_sales:ws select ss.customer.last_name, ss.customer.first_name, ss.date…rst_name, ws.date.date as sale_date where ws.date.year = 2000 and ws.billing_customer.id is not null) -> (last_name, first_name, sale_date)) limit 5;`

  ```text
  Syntax error in stdin:  --> 4:216
    |
  4 | select ss.customer.last_name, ss.customer.first_name, ss.date.date as sale_date where ss.date.year = 2000 and ss.customer.id is not null and (ss.customer.last_name, ss.customer.first_name, ss.date.date) in intersect((select cs.billing_customer.last_name, cs.billing_customer.first_name, cs.date.date as sale_date where cs.date.year = 2000 and cs.billing_customer.id is not null), (select ws.billing_customer.last_name, ws.billing_customer.first_name, ws.date.date as sale_date where ws.date.year = 2000 and ws.billing_customer.id is not null) -> (last_name, first_name, sale_date)) limit 5;
    |                                                                                                                                                                                                                        ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...me, ss.date.date) in intersect ??? ((select cs.billing_customer.l...
  ```
- `trilogy file write query39.preql --content import raw.inventory as inv;
import raw.date as d;
import raw.warehouse as w;
import raw.item as i;

# Compute agg…jan_stats.cv asc nulls first,
    feb_stats.month_of_year asc nulls first,
    feb_stats.mean_qoh asc nulls first,
    feb_stats.cv asc nulls first
;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset monthly_stats as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   month_of_year) for year 2001 ??? rowset monthly_stats as where

  Write stats: received 1892 chars / 1892 bytes; tail: …'mean_qoh asc nulls first,\\n    feb_stats.cv asc nulls first\\n;'.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any of the 8 attribute profiles
auto profile_manuf <- item.manufact ? (…urer_id between 1 and 500
  and item.manufact in profile_manuf
select distinct
  item.product_name as product_name
order by
  product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ufact in profile_manuf select ??? distinct   item.product_name a...

  Write stats: received 1121 chars / 1121 bytes; tail: …'duct_name as product_name\\norder by\\n  product_name\\nlimit 100;'.
  ```
- `trilogy file write query51.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Per-item, per-date aggregated unit price sums for sto…g combined.web_running_max > combined.store_running_max
order by combined.item_sk asc nulls first,
    combined.sale_date asc nulls first
limit 100;
`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_agg as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...it price sums for store sales ??? rowset store_agg as where ss.d...

  Write stats: received 2074 chars / 2074 bytes; tail: …'ls first,\\n    combined.sale_date asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.store_sales as ss;
import raw.customer as c….month_of_year = 12;

# Combined set of qualifying customer IDs
auto qualifying_customers <- distinct(concat(catalog_customers, '_', web_customers))？`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:38
     |
  17 | auto qualifying_customers <- distinct(concat(catalog_customers, '_', web_customers))？
     |                                      ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...alifying_customers <- distinct ??? (concat(catalog_customers, '_'...

  Write stats: received 736 chars / 738 bytes; tail: …" <- distinct(concat(catalog_customers, '_', web_customers))？".
  ```
- `trilogy run --import raw/catalog_sales:cs select cs.billing_customer.id as cust_id limit 5 where cs.item.category = 'Women' and cs.item.class = 'maternity' and cs.sold_date.year = 1998 and cs.sold_date.month_of_year = 12;`

  ```text
  Syntax error in stdin:  --> 2:42
    |
  2 | select cs.billing_customer.id as cust_id limit 5 where cs.item.category = 'Women' and cs.item.class = 'maternity' and cs.sold_date.year = 1998 and cs.sold_date.month_of_year = 12;
    |                                          ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...illing_customer.id as cust_id ??? limit 5 where cs.item.category...
  ```
- `trilogy run --import raw/store_sales:ss count(ss.customer.id) as cnt where ss.date.month_seq between 1188 and 1190 and ss.store.county = ss.customer.address.county and ss.store.state = ss.customer.address.state limit 5;`

  ```text
  Syntax error in stdin:  --> 2:1
    |
  2 | count(ss.customer.id) as cnt where ss.date.month_seq between 1188 and 1190 and ss.store.county = ss.customer.address.county and ss.store.state = ss.customer.address.state limit 5;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  import raw.store_sales as ss; ??? count(ss.customer.id) as cnt w...
  ```
- `trilogy run --import raw/all_sales:sales select sales.date.date, sales.channel, count(sales.sale_line_item_counter) as cnt where sales.date.week_seq = 5218 group by sales.date.date, sales.channel order by sales.channel, sales.date.date limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...re sales.date.week_seq = 5218 ??? group by sales.date.date, sale...
  ```
- `trilogy run --import raw/all_sales:sales select sales.item.id, count(sales.sale_line_item_counter) as cnt where sales.channel = 'STORE' and sales.date.week_seq = 5218 group by sales.item.id having cnt > 1 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nd sales.date.week_seq = 5218 ??? group by sales.item.id having
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_r…ice > 2 * catalog refund
auto item_cat_condition <- cs.item.sk in (select cs.item.sk where cat_ext_list_by_item > 2 * cat_refund_by_item);

select 1;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:9
     |
  19 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...cat_refund_by_item);  select 1 ??? ;

  Write stats: received 874 chars / 874 bytes; tail: …'e cat_ext_list_by_item > 2 * cat_refund_by_item);\\n\\nselect 1;'.
  ```
- `trilogy file write query64.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.store_sales as ss;

with catalog_sale…_item) as sale_lines,
  sum(ss.wholesale_cost) as total_wholesale,
  sum(ss.list_price) as total_list,
  sum(ss.coupon_amt) as total_coupon
limit 10;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 32:52
     |
  32 |   and ss.item.id in (catalog_item_combined.item_sk where catalog_item_combined.total_ext_list > 2 * coalesce(catalog_item_combined.total_refund, 0))
     |                                                    ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...catalog_item_combined.item_sk ??? where catalog_item_combined.to...

  Write stats: received 1826 chars / 1826 bytes; tail: …'s total_list,\\n  sum(ss.coupon_amt) as total_coupon\\nlimit 10;'.
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.store.state, sum(store_sales.net_profit) as total_np where store_sales.date.year = 2000 and store_sales.store.sk is not null group by store_sales.store.state order by total_np desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...re_sales.store.sk is not null ??? group by store_sales.store.sta...
  ```
- `trilogy run --import raw/store:store select store.state, count(store.sk) as cnt where store.id is not null group by store.state order by cnt desc limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nt where store.id is not null ??? group by store.state order by
  ```
- `trilogy file write query72.preql --content import raw.warehouse as w;
select distinct w.name limit 100;`

  ```text
  refused to write 'query72.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...rt raw.warehouse as w; select ??? distinct w.name limit 100;

  Write stats: received 60 chars / 60 bytes; tail: …'import raw.warehouse as w;\\nselect distinct w.name limit 100;'.
  ```
- `trilogy run --import raw/all_sales:sales select sales.date.year, sales.channel, sales.item.category, count(sales.order_id) as cnt, sum(sales.quantity) as total_qty, sum(sales.ext_sales_price) as total_amt where sales.item.category = 'Books' group by 1,2,3 order by 1,2,3 limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sales.item.category = 'Books' ??? group by 1,2,3 order by 1,2,3
  ```
- `trilogy run --import raw/all_sales:sales select sales.item.id, sales.order_id, count(sales.channel) as num_channels where sales.item.category = 'Books' and sales.date.year = 2001 group by 1,2 having count(sales.channel) > 1 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...s' and sales.date.year = 2001 ??? group by 1,2 having count(sale...
  ```
- `trilogy file write query78.preql --content import raw.store_sales as store;
import raw.web_sales as web;
import raw.catalog_sales as cat;

# Store sales neve…re_wholesale desc, --store_sp desc,
    other_channel_quantity asc, other_channel_wholesale asc, other_channel_sales_price asc, ratio asc
limit 100;
`

  ```text
  refused to write 'query78.preql': not syntactically valid Trilogy.

  Parse error:
    --> 55:31
     |
  55 |     store_agg.st_wholesale as --store_wholesale,
     |                               ^---
     |
     = expected IDENTIFIER
  Location:
      store_agg.st_wholesale as ??? --store_wholesale,     store_a...

  Write stats: received 2876 chars / 2876 bytes; tail: …'le asc, other_channel_sales_price asc, ratio asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Define per-channel su…l_label) desc,
  combined.channel_label nulls first,
  grouping(combined.outlet_identifier) desc,
  combined.outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
    --> 51:4
     |
  51 |   (store_arm),
     |    ^---
     |
     = expected select_statement
  Location:
  ...ms with combined as union(   ( ??? store_arm),   (catalog_arm),

  Write stats: received 2178 chars / 2178 bytes; tail: …'r) desc,\\n  combined.outlet_identifier nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with combined as union(…l) desc,
    combined.channel_label nulls first,
    grouping(combined.outlet_identifier) desc,
    combined.outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...ifier, profit, returns, sales) ???  select     combined.channel_...

  Write stats: received 2143 chars / 2143 bytes; tail: …' desc,\\n    combined.outlet_identifier nulls first\\nlimit 100;'.
  ```
- `trilogy file write query83.preql -e -c import raw/store_returns as sr;
import raw/catalog_returns as cr;
import raw/web_returns as wr;

# Store returns: per ….item_code
where
    sa.store_rows > 0
    and ca.catalog_rows > 0
    and wa.web_rows > 0
order by sa.item_code, sa.store_qty nulls first
limit 100;`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_returns as sr;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_returns as sr; impor...

  Write stats: received 1217 chars / 1217 bytes; tail: …'0\\norder by sa.item_code, sa.store_qty nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_returns:sr select sr.item.id, sum(sr.return_quantity) as qty, count(sr.ticket_number) as cnt where sr.return_date.week_seq in (5244, 5257, 5264) group by sr.item.id having cnt > 0 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...eek_seq in (5244, 5257, 5264) ??? group by sr.item.id having cnt...
  ```
- `trilogy file write query84.preql --content import raw.store_returns as sr;
import raw.customer as c;

# Find customer demographic matches: any customer whose…lesce(c.first_name, '')) as customer_full_name
  --sr.ticket_number as tkt,
  --sr.item.sk as item_sk
order by
  customer_code nulls first
limit 100;`

  ```text
  refused to write 'query84.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:3
     |
  16 |   --sr.ticket_number as tkt,
     |   ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
   '')) as customer_full_name   ??? --sr.ticket_number as tkt,   -...

  Write stats: received 716 chars / 716 bytes; tail: …'k as item_sk\\norder by\\n  customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as ws;

# Filter for year 2000
where ws.date.year = 2000

# Hierarchy level: 0 = category+cla…    rnk
by rollup (ws.item.category, ws.item.class)
order by level desc nulls first, ws.item.category asc nulls first, rnk asc nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block - they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...ory subtotal, 2 = grand total ??? auto g_class <- grouping(ws.it...

  Write stats: received 960 chars / 960 bytes; tail: …'tem.category asc nulls first, rnk asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query90.preql --content import raw.web_sales as ws;

where
    ws.ship_household_demographic.dependent_count = 6
    and ws.web_page.char_…n 8 and 9)::numeric(12,4),
        count(ws.line_item ? hour(ws.time.time) between 19 and 20)::numeric(12,4)
    ) as ratio
order by ratio
limit 100;`

  ```text
  refused to write 'query90.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:16
     |
  10 |     safe_divide(
     |                ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, select_grouping, or JOIN_TYPE
  Location:
  ...evening_count,     safe_divide ??? (         count(ws.line_item ?...

  Write stats: received 530 chars / 530 bytes; tail: …' 20)::numeric(12,4)\\n    ) as ratio\\norder by ratio\\nlimit 100;'.
  ```
- `trilogy file write query91.preql --content import raw.catalog_returns as cr;

where 
    cr.date.month_of_year = 11
    and cr.date.year = 1998
    and cr.bi…    cr.billing_customer.demographics.education_status as --education_status,
    sum(cr.net_loss) as total_net_loss
order by
    total_net_loss desc;`

  ```text
  refused to write 'query91.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:56
     |
  16 |     cr.billing_customer.demographics.marital_status as --marital_status,
     |                                                        ^---
     |
     = expected IDENTIFIER
  Location:
  ...emographics.marital_status as ??? --marital_status,     cr.billi...

  Write stats: received 884 chars / 884 bytes; tail: …'et_loss) as total_net_loss\\norder by\\n    total_net_loss desc;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.customer:customer select customer.address.county, count(customer.sk) where customer.address.county in ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County') group by customer.address.county;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(customer.sk) as sk_count`
  Location:
  ...ss.county, count(customer.sk) ??? where customer.address.county
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.market, count(ss.ticket_number) where ss.store.market = 8 and ss.is_returned = True limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.ticket_number) as ticket_number_count`
  Location:
  ...rket, count(ss.ticket_number) ??? where ss.store.market = 8 and
  ```
- `trilogy file write query47.preql --content import raw.store_sales as ss;

# monthly total of unit prices per category/brand/store/company
with monthly_totals…f_year asc,
    avg_monthly_sales asc,
    monthly_total asc,
    prior_month_total asc nulls first,
    next_month_total asc nulls first
limit 100
;`

  ```text
  refused to write 'query47.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `diff_from_avg used for ordering only as diff_from_avg_used_for_ordering_only`
  Location:
  ...h_total,     -- diff_from_avg ??? used for ordering only, not re...

  Write stats: received 2709 chars / 2709 bytes; tail: …'ulls first,\\n    next_month_total asc nulls first\\nlimit 100\\n;'.
  ```
- `trilogy run --import raw/catalog_sales:cs select count(cs.billing_customer.id) where cs.item.category = 'Women' and cs.item.class = 'maternity' and cs.sold_date.year = 1998 and cs.sold_date.month_of_year = 12;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cs.billing_customer.id) as id_count`
  Location:
  ...count(cs.billing_customer.id) ??? where cs.item.category = 'Wome...
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Per-item catalo…k as item_sk,
  sum(coalesce(cr.refunded_cash,0) + coalesce(cr.reversed_charge,0) + coalesce(cr.store_credit,0)) as total_refund
;

select 1 limit 1;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `1 as 1`
  Location:
  ...) as total_refund ;  select 1 ??? limit 1;

  Write stats: received 1174 chars / 1174 bytes; tail: …'sce(cr.store_credit,0)) as total_refund\\n;\\n\\nselect 1 limit 1;'.
  ```
- `trilogy file write query64.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.item as item;

with catalog_sales_by_…k as item_sk,
  sum(coalesce(cr.refunded_cash,0) + coalesce(cr.reversed_charge,0) + coalesce(cr.store_credit,0)) as total_refund
;

select 1 limit 1;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `1 as 1`
  Location:
  ...) as total_refund ;  select 1 ??? limit 1;

  Write stats: received 399 chars / 399 bytes; tail: …'sce(cr.store_credit,0)) as total_refund\\n;\\n\\nselect 1 limit 1;'.
  ```
- `trilogy run --import raw/all_sales:sales select sales.channel, sales.item.category, count(sales.order_id) limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(sales.order_id) as order_id_count`
  Location:
  ...tegory, count(sales.order_id) ??? limit 5;
  ```
- `trilogy run --import raw.store_sales:store --import raw.web_sales:web --import raw.catalog_sales:cat select store.date.year, count(store.quantity) where store.date.year = 2000 and store.customer.id is not null limit 5;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(store.quantity) as quantity_count`
  Location:
  ...e.year, count(store.quantity) ??? where store.date.year = 2000 a...
  ```

### `cli-misuse`

- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy run -i raw.store_sales:store_sales select count(store_sales.item.id), store_sales.item.id, substring(store_sales.item.desc,1,30) as desc_prefix, store_sales.date.date where store_sales.date.year in (2000,2001,2002,2003) limit 5;`

  ```text
  'raw.store_sales:store_sales' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sales.preql' does not exist.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/store_sales.preql --repr json`

  ```text
  No such option: --repr (Possible options: --help, --regex)
  ```
- `trilogy explore raw`

  ```text
  Invalid value for 'PATH': File 'raw' is a directory.
  ```
- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
- `trilogy explore raw/sale_household_demographic.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sale_household_demographic.preql' does not exist.
  ```

### `type-error`

- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.ext_sales_price, all_sales.net_profit, all_sa…eturn_net_loss, all_sales.is_returned where all_sales.date.date between '2000-08-23' and '2000-09-06' and all_sales.is_returned is not null limit 10;`

  ```text
  Syntax error in stdin: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
- `trilogy run query08.preql duckdb --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,840…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Type error in query08.preql: Invalid argument type 'ArrayType<STRING>' passed into TRIM function in position 1 from concept: local.zips_list. Valid: 'STRING'.
  ```
- `trilogy run query20.preql`

  ```text
  Syntax error in query20.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
- `trilogy run query94.preql`

  ```text
  Syntax error in query94.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `join-resolution`

- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Could not resolve connections for query with output ['local.customer_id<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>', 'local.total_ext_sales_price<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
