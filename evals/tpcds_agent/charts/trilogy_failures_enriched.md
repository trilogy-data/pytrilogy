# Trilogy failure analysis — 20260627-164436

- Run `20260627-164436` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 221 | failed: 38 (17%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 29 | 76% |
| `join-resolution` | 4 | 11% |
| `syntax-parse` | 2 | 5% |
| `syntax-missing-alias` | 2 | 5% |
| `undefined-concept` | 1 | 3% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 25). The requested concepts split into 7 disconnected subgraphs: {cur_sales.ws, nxt_sales.sales}; {local._virt_filter_sales_5091272609522265}; {local._virt_filter_sales_5579278383917571}; {local._virt_filter_sales_6003313945349525}; {local._virt_filter_sales_7905328803953891}; {local._virt_filter_sales_9490218559732084}; {local._virt_filter_sales_9760385907245714}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: date.year. Suggestions: ['store.date.year', 'store.store.date.year', 'store.return_store.date.year', 'web.date.year', 'store.return_date.year', 'store.customer.first_sales_date.year']
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 2 undefined concept references; fix all before re-running:
    - first_name (line 60, col 5, in ORDER BY); did you mean: store.customer.first_name, store.return_customer.first_name, web.billing_customer.first_name, web.ship_customer.first_name, web.return_customer.first_name, web.return_refund_customer.first_name?
    - last_name (line 61, col 5, in ORDER BY); did you mean: store.customer.last_name, store.return_customer.last_name, web.billing_customer.last_name, web.ship_customer.last_name, web.return_customer.last_name, web.return_refund_customer.last_name?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: 3 undefined concept references; fix all before re-running:
    - valid_bcc.item.brand_id (line 13, col 26, in WHERE); did you mean: s.item.brand_id, brand_id, valid_bcc.item.class_id, valid_bcc.item.category_id?
    - valid_bcc.item.class_id (line 14, col 26, in WHERE); did you mean: s.item.class_id, class_id, valid_bcc.item.brand_id, valid_bcc.item.category_id?
    - valid_bcc.item.category_id (line 15, col 29, in WHERE); did you mean: s.item.category_id, category_id, valid_bcc.item.class_id, valid_bcc.item.brand_id?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 19). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: ORDER BY contains aggregate `grouping(s.channel)` (line 19), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(s.channel) as g order by g desc`.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'cust_tots.lifetime_total', 'local.global_max_total', which are not in the SELECT projection (line 30). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --cust_tots.lifetime_total, --local.global_max_total
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Unexpected error in query23.preql: Invalid reference string found in query:
  WITH
  cooperative as (
  SELECT
      "store_sales_date_date"."D_DATE_SK" as "store_sales_date_id",
      cast("store_sales_date_date"."D_DATE" as date) as "store_sales_date_date"
  FROM
      "date_dim" as "store_sales_date_date"
  WHERE
      "store_sales_date_date"."D_YEAR" BETWEEN 2000 AND 2003
  ),
  young as (
  SELECT
      "store_sales_store_sales"."SS_CUSTOMER_SK" as "_cust_tots_cust_id"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" BETWEEN 2000 AND 2003 and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null

  GROUP BY
      1),
  uneven as (
  SELECT
      "cooperative"."store_sales_date_date" as "store_sales_date_date",
      "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id",
      SUBSTRING("store_sales_item_items"."I_ITEM_DESC",1,30) as "_freq_items_desc_prefix"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "cooperative" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "cooperative"."store_sales_date_id"
      INNER JOIN "item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
  GROUP BY
      1,
      2,
      3,
      "cooperative"."store_sales_date_id"),
  abhorrent as (
  SELECT
      "young"."_cust_tots_cust_id" as "cust_tots_cust_id",
      INVALID_REFERENCE_BUG * INVALID_REFERENCE_BUG as "cust_tots_lifetime_total"
  FROM
      "young"),
  yummy as (
  SELECT
      "uneven"."_freq_items_desc_prefix" as "_freq_items_desc_prefix"
  FROM
      "uneven"
  GROUP BY
      1
  HAVING
      count(distinct (cast("uneven"."store_sales_item_id" as string) || '-' || cast("uneven"."store_sales_date_date" as string))) > 4
  ),
  sweltering as (
  SELECT
      max("abhorrent"."cust_tots_lifetime_total") as "global_max_total"
  FROM
      "abhorrent"),
  juicy as (
  SELECT
      "yummy"."_freq_items_desc_prefix" as "_freq_items_desc_prefix"
  FROM
      "yummy"),
  late as (
  SELECT
      "abhorrent"."cust_tots_cust_id" as "cust_tots_cust_id"
  FROM
      "abhorrent"
      INNER JOIN "sweltering" on 1=1
  WHERE
      "abhorrent"."cust_tots_lifetime_total" > 0.5 * "sweltering"."global_max_total"
  ),
  vacuous as (
  SELECT
      "juicy"."_freq_items_desc_prefix" as "freq_items_desc_prefix"
  FROM
      "juicy"),
  macho as (
  SELECT
      "late"."cust_tots_cust_id" as "best_cust_cust_tots_cust_id"
  FROM
      "late"),
  charming as (
  SELECT
      "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" as "web_sales_billing_customer_id",
      "web_sales_web_sales"."WS_ITEM_SK" as "web_sales_item_id",
      "web_sales_web_sales"."WS_LIST_PRICE" as "web_sales_list_price",
      "web_sales_web_sales"."WS_QUANTITY" as "web_sales_quantity",
      "web_sales_web_sales"."WS_SOLD_DATE_SK" as "web_sales_date_id"
  FROM
      "web_sales" as "web_sales_web_sales"
  WHERE
      "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" in (select macho."best_cust_cust_tots_cust_id" from macho where macho."best_cust_cust_tots_cust_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  highfalutin as (
  SELECT
      "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" as "catalog_sales_bill_customer_id",
      "catalog_sales_catalog_sales"."CS_ITEM_SK" as "catalog_sales_item_id",
      "catalog_sales_catalog_sales"."CS_LIST_PRICE" as "catalog_sales_list_price",
      "catalog_sales_catalog_sales"."CS_QUANTITY" as "catalog_sales_quantity",
      "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" as "catalog_sales_sold_date_id"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
  WHERE
      "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" in (select macho."best_cust_cust_tots_cust_id" from macho where macho."best_cust_cust_tots_cust_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      5),
  premium as (
  SELECT
      "charming"."web_sales_quantity" * "charming"."web_sales_list_price" as "___tvf_arm_1_sv",
      "web_sales_billing_customer_customers"."C_FIRST_NAME" as "___tvf_arm_1_fn",
      "web_sales_billing_customer_customers"."C_LAST_NAME" as "___tvf_arm_1_ln"
  FROM
      "charming"
      INNER JOIN "date_dim" as "web_sales_date_date" on "charming"."web_sales_date_id" = "web_sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "web_sales_item_items" on "charming"."web_sales_item_id" = "web_sales_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "customer" as "web_sales_billing_customer_customers" on "charming"."web_sales_billing_customer_id" = "web_sales_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "web_sales_date_date"."D_YEAR" = 2000 and "web_sales_date_date"."D_MOY" = 2 and SUBSTRING("web_sales_item_items"."I_ITEM_DESC",1,30) in (select vacuous."freq_items_desc_prefix" from vacuous where vacuous."freq_items_desc_prefix" is not null)

  GROUP BY
      1,
      2,
      3),
  scrawny as (
  SELECT
      "catalog_sales_bill_customer_customers"."C_FIRST_NAME" as "___tvf_arm_0_fn",
      "catalog_sales_bill_customer_customers"."C_LAST_NAME" as "___tvf_arm_0_ln",
      "highfalutin"."catalog_sales_quantity" * "highfalutin"."catalog_sales_list_price" as "___tvf_arm_0_sv"
  FROM
      "highfalutin"
      INNER JOIN "item" as "catalog_sales_item_items" on "highfalutin"."catalog_sales_item_id" = "catalog_sales_item_items"."I_ITEM_SK"
      INNER JOIN "date_dim" as "catalog_sales_sold_date_date" on "highfalutin"."catalog_sales_sold_date_id" = "catalog_sales_sold_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "customer" as "catalog_sales_bill_customer_customers" on "highfalutin"."catalog_sales_bill_customer_id" = "catalog_sales_bill_customer_customers"."C_CUSTOMER_SK"
  WHERE
      "catalog_sales_sold_date_date"."D_YEAR" = 2000 and "catalog_sales_sold_date_date"."D_MOY" = 2 and SUBSTRING("catalog_sales_item_items"."I_ITEM_DESC",1,30) in (select vacuous."freq_items_desc_prefix" from vacuous where vacuous."freq_items_desc_prefix" is not null)

  GROUP BY
      1,
      2,
      3),
  waggish as (
  SELECT
      "scrawny"."___tvf_arm_0_ln" as "_combined_ln",
      "scrawny"."___tvf_arm_0_fn" as "_combined_fn",
      "scrawny"."___tvf_arm_0_sv" as "_combined_sv"
  FROM
      "scrawny"
  UNION ALL
  SELECT
      "premium"."___tvf_arm_1_ln" as "_combined_ln",
      "premium"."___tvf_arm_1_fn" as "_combined_fn",
      "premium"."___tvf_arm_1_sv" as "_combined_sv"
  FROM
      "premium")
  SELECT
      "waggish"."_combined_ln" as "last_name",
      "waggish"."_combined_fn" as "first_name",
      sum("waggish"."_combined_sv") as "total_sales"
  FROM
      "waggish"
  GROUP BY
      1,
      2
  ORDER BY
      "last_name" asc nulls first,
      "first_name" asc nulls first,
      "total_sales" asc nulls first
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'local.store_customer_total', 'local.global_max_total', which are not in the SELECT projection (line 25). To make them available, you may add them to  the SELECT. Prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.store_customer_total, --local.global_max_total
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 40). The requested concepts split into 3 disconnected subgraphs: {customer.address.county, customer.address.state, customer.id}; {store.county, store.state}; {store_sales.date.month_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query54.preql`

  ```text
  Resolution error in query54.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 41). The requested concepts split into 2 disconnected subgraphs: {store.county, store.state}; {store_sales.customer.address.county, store_sales.customer.address.state, store_sales.customer.id, store_sales.date.month_seq}.
    - `store.county` is disconnected — did you mean `store_sales.store.county`? (connected to the other concepts)
    - `store.state` is disconnected — did you mean `store_sales.store.state`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `store_sales.store.county`) instead of importing a second, disconnected copy.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 132 column 3 (char 4320). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 5277 (char 5276). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 75 column 17 (char 3052). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: store_returns.item.id. Suggestions: ['store_sales.item.id', 'store_sales.date.id', 'store_sales.return_date.id', 'store_sales.time.id', 'store_sales.return_time.id', 'store_sales.customer.demographics.id']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Ambiguous reference 'y1999.product_name': matches ['y1999.base.product_name', 'y1999.base.store_sales.item.product_name']. Qualify the full path to disambiguate.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 11). The requested concepts split into 2 disconnected subgraphs: {catalog_sales.item.color, catalog_sales.item.current_price, catalog_sales.item.id}; {local._virt_agg_sum_441397383881491}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 11). The requested concepts split into 2 disconnected subgraphs: {catalog_sales.item.color, catalog_sales.item.current_price, catalog_sales.item.id}; {local._virt_agg_sum_441397383881491}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Ambiguous reference 'yearly.year': matches ['yearly.deduped.sales.date.year', 'yearly.deduped.year']. Qualify the full path to disambiguate.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Ambiguous reference 'yearly.brand_id': matches ['yearly.deduped.brand_id', 'yearly.deduped.sales.item.brand_id']. Qualify the full path to disambiguate.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: ORDER BY contains aggregate `grouping(local.channel_label)` (line 3), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel_label) as g order by g desc`.
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.qualifying_item<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.item_qualifies<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.item_qualifies<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.qual_item<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```

### `syntax-parse`

- `trilogy file write query11.preql --content import raw.store_sales as store;
import raw.web_sales as web;

# Store revenue by customer by year
rowset store_re…ing_customer_code asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_rev as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...e revenue by customer by year ??? rowset store_rev as where date...

  Write stats: received 2163 chars / 2163 bytes; tail: …'s first,\\n    preferred_cust_flag asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Step 1: overall average sale value (quantity * list_price) across all channels 1999-…      channel asc nulls first,
         brand_id asc nulls first,
         class_id asc nulls first,
         category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | rowset valid_bcc as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...stage this coarse-grained set ??? rowset valid_bcc as where year...

  Write stats: received 1676 chars / 1676 bytes; tail: …'ulls first,\\n         category_id asc nulls first\\nlimit 100;\\n'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws select count(cs.bill_customer.id) where cs.sold_date.year=1998 and cs.sold_date.month_of_year=12 and cs.item.category='Women' and cs.item.class='maternity';`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cs.bill_customer.id) as id_count`
  Location:
  ...ct count(cs.bill_customer.id) ??? where cs.sold_date.year=1998 a...
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws select count(ws.billing_customer.id) where ws.date.year=1998 and ws.date.month_of_year=12 and ws.item.category='Women' and ws.item.class='maternity';`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ws.billing_customer.id) as id_count`
  Location:
  ...count(ws.billing_customer.id) ??? where ws.date.year=1998 and ws...
  ```

### `undefined-concept`

- `trilogy explore query64.preql`

  ```text
  Failed to parse query64.preql: (UndefinedConceptException(...), "Ambiguous reference 'y1999.product_name': matches ['y1999.base.product_name', 'y1999.base.store_sales.item.product_name']. Qualify the full path to disambiguate.")
  ```
