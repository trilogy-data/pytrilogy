# Trilogy failure analysis — 20260813-035250

- Run `20260813-035250_enriched_confusable` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 296 | failed: 14 (5%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 6 | 43% |
| `other` | 3 | 21% |
| `syntax-parse` | 3 | 21% |
| `undefined-concept` | 1 | 7% |
| `cli-misuse` | 1 | 7% |

## Detail

### `disabled-tool`

- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `other`

- `trilogy file write probe_i.preql --run-and-delete`

  ```text
  Unexpected error in probe_i.preql: (_duckdb.ParserException) Parser Error: zero-length delimited identifier at or near """"

  LINE 88: ...ng_customer_id_at_web_billing_customer_id_at_web_ch_cid']"."","macho"."web_ch_cid","sparkling"."customer_id") asc nulls...
                                                                         ^
  [SQL:
  WITH
  macho as (
  SELECT
      "web_billing_customer_customers"."C_CUSTOMER_ID" as "web_ch_cid",
      sum(CASE WHEN "web_sale_date_date"."D_YEAR" = 2001 THEN ( ( "web_web_sales"."WS_EXT_LIST_PRICE" - "web_web_sales"."WS_EXT_WHOLESALE_COST" ) - "web_web_sales"."WS_EXT_DISCOUNT_AMT" ) + "web_web_sales"."WS_EXT_SALES_PRICE" ELSE NULL END) as "web_ch_wv01",
      sum(CASE WHEN "web_sale_date_date"."D_YEAR" = 2002 THEN ( ( "web_web_sales"."WS_EXT_LIST_PRICE" - "web_web_sales"."WS_EXT_WHOLESALE_COST" ) - "web_web_sales"."WS_EXT_DISCOUNT_AMT" ) + "web_web_sales"."WS_EXT_SALES_PRICE" ELSE NULL END) as "web_ch_wv02"
  FROM
      "fact_web_sales" as "web_web_sales"
      INNER JOIN "dim_date_dim" as "web_sale_date_date" on "web_web_sales"."WS_SOLD_DATE_SK" = "web_sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "dim_customer" as "web_billing_customer_customers" on "web_web_sales"."WS_BILL_CUSTOMER_SK" = "web_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      ("web_sale_date_date"."D_YEAR" is not null and "web_sale_date_date"."D_YEAR" in (2001,2002))

  GROUP BY
      1),
  juicy as (
  SELECT
      "store_customer_customers"."C_CUSTOMER_ID" as "store_ch_cid",
      "store_customer_customers"."C_FIRST_NAME" as "store_ch_fn",
      "store_customer_customers"."C_LAST_NAME" as "store_ch_ln",
      "store_customer_customers"."C_PREFERRED_CUST_FLAG" as "store_ch_pf",
      sum(CASE WHEN "store_sale_date_date"."D_YEAR" = 2001 THEN ( ( "store_store_sales"."SS_EXT_LIST_PRICE" - "store_store_sales"."SS_EXT_WHOLESALE_COST" ) - "store_store_sales"."SS_EXT_DISCOUNT_AMT" ) + "store_store_sales"."SS_EXT_SALES_PRICE" ELSE NULL END) as "store_ch_sv01",
      sum(CASE WHEN "store_sale_date_date"."D_YEAR" = 2002 THEN ( ( "store_store_sales"."SS_EXT_LIST_PRICE" - "store_store_sales"."SS_EXT_WHOLESALE_COST" ) - "store_store_sales"."SS_EXT_DISCOUNT_AMT" ) + "store_store_sales"."SS_EXT_SALES_PRICE" ELSE NULL END) as "store_ch_sv02"
  FROM
      "fact_store_sales" as "store_store_sales"
      INNER JOIN "dim_date_dim" as "store_sale_date_date" on "store_store_sales"."SS_SOLD_DATE_SK" = "store_sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "dim_customer" as "store_customer_customers" on "store_store_sales"."SS_CUSTOMER_SK" = "store_customer_customers"."C_CUSTOMER_SK"
  WHERE
      ("store_sale_date_date"."D_YEAR" is not null and "store_sale_date_date"."D_YEAR" in (2001,2002))

  GROUP BY
      1,
      2,
      3,
      4),
  sparkling as (
  SELECT
      "juicy"."store_ch_cid" as "customer_id",
      "juicy"."store_ch_fn" as "first_name",
      "juicy"."store_ch_ln" as "last_name",
      "juicy"."store_ch_pf" as "preferred_cust_flag",
      "juicy"."store_ch_sv01" as "store_ch_sv01",
      "juicy"."store_ch_sv02" as "store_ch_sv02"
  FROM
      "juicy"
  WHERE
      "juicy"."store_ch_sv01" > 0 and "juicy"."store_ch_sv02" > 0
  ),
  cheerful as (
  SELECT
      "catalog_billing_customer_customers"."C_CUSTOMER_ID" as "catalog_ch_cid",
      sum(CASE WHEN "catalog_sale_date_date"."D_YEAR" = 2001 THEN ( ( "catalog_catalog_sales"."CS_EXT_LIST_PRICE" - "catalog_catalog_sales"."CS_EXT_WHOLESALE_COST" ) - "catalog_catalog_sales"."CS_EXT_DISCOUNT_AMT" ) + "catalog_catalog_sales"."CS_EXT_SALES_PRICE" ELSE NULL END) as "catalog_ch_cv01",
      sum(CASE WHEN "catalog_sale_date_date"."D_YEAR" = 2002 THEN ( ( "catalog_catalog_sales"."CS_EXT_LIST_PRICE" - "catalog_catalog_sales"."CS_EXT_WHOLESALE_COST" ) - "catalog_catalog_sales"."CS_EXT_DISCOUNT_AMT" ) + "catalog_catalog_sales"."CS_EXT_SALES_PRICE" ELSE NULL END) as "catalog_ch_cv02"
  FROM
      "fact_catalog_sales" as "catalog_catalog_sales"
      INNER JOIN "dim_date_dim" as "catalog_sale_date_date" on "catalog_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sale_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "dim_customer" as "catalog_billing_customer_customers" on "catalog_catalog_sales"."CS_BILL_CUSTOMER_SK" = "catalog_billing_customer_customers"."C_CUSTOMER_SK"
  WHERE
      ("catalog_sale_date_date"."D_YEAR" is not null and "catalog_sale_date_date"."D_YEAR" in (2001,2002))

  GROUP BY
      1)
  SELECT
      coalesce("cheerful"."customer_id","macho"."web_ch_cid","sparkling"."customer_id") as "customer_id",
      "sparkling"."first_name" as "first_name",
      "sparkling"."last_name" as "last_name",
      "sparkling"."preferred_cust_flag" as "preferred_cust_flag"
  FROM
      "sparkling"
      INNER JOIN "macho" on "sparkling"."customer_id" is not distinct from "macho"."web_ch_cid"
      INNER JOIN "cheerful" on "macho"."web_ch_cid" is not distinct from "cheerful"."catalog_ch_cid" AND "sparkling"."customer_id" is not distinct from "cheerful"."catalog_ch_cid"
  WHERE
      "sparkling"."store_ch_sv01" > 0 and "cheerful"."catalog_ch_cv01" > 0 and "macho"."web_ch_wv01" > 0 and "sparkling"."store_ch_sv02" > 0 and "cheerful"."catalog_ch_cv02" > 0 and "macho"."web_ch_wv02" > 0 and ( "cheerful"."catalog_ch_cv02" / nullif("cheerful"."catalog_ch_cv01",0) ) > ( "sparkling"."store_ch_sv02" / nullif("sparkling"."store_ch_sv01",0) ) and ( "cheerful"."catalog_ch_cv02" / nullif("cheerful"."catalog_ch_cv01",0) ) > ( "macho"."web_ch_wv02" / nullif("macho"."web_ch_wv01",0) )

  GROUP BY
      1,
      2,
      3,
      4,
      "cheerful"."catalog_ch_cv01",
      "cheerful"."catalog_ch_cv02",
      "macho"."web_ch_wv01",
      "macho"."web_ch_wv02",
      "sparkling"."store_ch_sv01",
      "sparkling"."store_ch_sv02"
  ORDER BY
      coalesce("cheerful"."INVALID_ALIAS: [MODELS_EXECUTE] Concept local"."customer_id@Grain<Abstract> not found on catalog"."billing_customer"."customers_at_catalog_billing_customer_sk_join_catalog"."catalog_sales_at_catalog_item_sk_catalog_order_number_join_catalog"."sale_date"."date_at_catalog_sale_date_sk_at_catalog_item_sk_catalog_order_number_filtered_by_2606441257082357_grouped_by_catalog"."billing_customer"."id_at_catalog_billing_customer_id_at_catalog_billing_customer_id_at_catalog_ch_cid_join_store"."customer"."customers_at_store_customer_sk_join_store"."sale_date"."date_at_store_sale_date_sk_join_store"."store_sales_at_store_item_sk_store_ticket_number_at_store_item_sk_store_ticket_number_filtered_by_5302012938685798_grouped_by_store"."customer"."first_name_store"."customer"."id_store"."customer"."last_name_store"."customer"."preferred_cust_flag_at_store_customer_first_name_store_customer_id_store_customer_last_name_store_customer_preferred_cust_flag_at_store_customer_first_name_store_customer_id_store_customer_last_name_store_customer_preferred_cust_flag_at_store_ch_cid_store_ch_fn_store_ch_ln_store_ch_pf_at_local_customer_id_store_ch_cid_store_ch_fn_store_ch_ln_store_ch_pf_join_web"."billing_customer"."customers_at_web_billing_customer_sk_join_web"."sale_date"."date_at_web_sale_date_sk_join_web"."web_sales_at_web_item_sk_web_order_number_at_web_item_sk_web_order_number_filtered_by_8090534549351428_grouped_by_web"."billing_customer"."id_at_web_billing_customer_id_at_web_billing_customer_id_at_web_ch_cid_grouped_by_local"."customer_id_local"."first_name_local"."last_name_local"."preferred_cust_flag_at_catalog_ch_cv01_catalog_ch_cv02_local_customer_id_local_first_name_local_last_name_local_preferred_cust_flag_web_ch_wv01_web_ch_wv02; have ['local"."customer_id@Grain<catalog_ch"."cv01,catalog_ch"."cv02,local"."customer_id,local"."first_name,local"."last_name,local"."preferred_cust_flag,web_ch"."wv01,web_ch"."wv02>', 'local"."first_name@Grain<catalog_ch"."cv01,catalog_ch"."cv02,local"."customer_id,local"."first_name,local"."last_name,local"."preferred_cust_flag,web_ch"."wv01,web_ch"."wv02>', 'local"."last_name@Grain<catalog_ch"."cv01,catalog_ch"."cv02,local"."customer_id,local"."first_name,local"."last_name,local"."preferred_cust_flag,web_ch"."wv01,web_ch"."wv02>', 'local"."preferred_cust_flag@Grain<catalog_ch"."cv01,catalog_ch"."cv02,local"."customer_id,local"."first_name,local"."last_name,local"."preferred_cust_flag,web_ch"."wv01,web_ch"."wv02>', 'store_ch"."sv01@Grain<catalog_ch"."cv01,catalog_ch"."cv02,local"."customer_id,local"."first_name,local"."last_name,local"."preferred_cust_flag,web_ch"."wv01,web_ch"."wv02>', 'store_ch"."sv02@Grain<catalog_ch"."cv01,catalog_ch"."cv02,local"."customer_id,local"."first_name,local"."last_name,local"."preferred_cust_flag,web_ch"."wv01,web_ch"."wv02>', 'catalog_ch"."cv01@Grain<catalog_ch"."cv01,catalog_ch"."cv02,local"."customer_id,local"."first_name,local"."last_name,local"."preferred_cust_flag,web_ch"."wv01,web_ch"."wv02>', 'catalog_ch"."cv02@Grain<catalog_ch"."cv01,catalog_ch"."cv02,local"."customer_id,local"."first_name,local"."last_name,local"."preferred_cust_flag,web_ch"."wv01,web_ch"."wv02>', 'web_ch"."wv01@Grain<catalog_ch"."cv01,catalog_ch"."cv02,local"."customer_id,local"."first_name,local"."last_name,local"."preferred_cust_flag,web_ch"."wv01,web_ch"."wv02>', 'web_ch"."wv02@Grain<catalog_ch"."cv01,catalog_ch"."cv02,local"."customer_id,local"."first_name,local"."last_name,local"."preferred_cust_flag,web_ch"."wv01,web_ch"."wv02>'] from ['catalog"."billing_customer"."customers_at_catalog_billing_customer_sk_join_catalog"."catalog_sales_at_catalog_item_sk_catalog_order_number_join_catalog"."sale_date"."date_at_catalog_sale_date_sk_at_catalog_item_sk_catalog_order_number_filtered_by_2606441257082357_grouped_by_catalog"."billing_customer"."id_at_catalog_billing_customer_id_at_catalog_billing_customer_id_at_catalog_ch_cid', 'store"."customer"."customers_at_store_customer_sk_join_store"."sale_date"."date_at_store_sale_date_sk_join_store"."store_sales_at_store_item_sk_store_ticket_number_at_store_item_sk_store_ticket_number_filtered_by_5302012938685798_grouped_by_store"."customer"."first_name_store"."customer"."id_store"."customer"."last_name_store"."customer"."preferred_cust_flag_at_store_customer_first_name_store_customer_id_store_customer_last_name_store_customer_preferred_cust_flag_at_store_customer_first_name_store_customer_id_store_customer_last_name_store_customer_preferred_cust_flag_at_store_ch_cid_store_ch_fn_store_ch_ln_store_ch_pf_at_local_customer_id_store_ch_cid_store_ch_fn_store_ch_ln_store_ch_pf', 'web"."billing_customer"."customers_at_web_billing_customer_sk_join_web"."sale_date"."date_at_web_sale_date_sk_join_web"."web_sales_at_web_item_sk_web_order_number_at_web_item_sk_web_order_number_filtered_by_8090534549351428_grouped_by_web"."billing_customer"."id_at_web_billing_customer_id_at_web_billing_customer_id_at_web_ch_cid']"."","macho"."web_ch_cid","sparkling"."customer_id") asc nulls first,
      "sparkling"."first_name" asc nulls first,
      "sparkling"."last_name" asc nulls first,
      "sparkling"."preferred_cust_flag" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  [v4] group-graph lineage cycle, skipping concept-set pass: [('grp:filter:d*:it.category:sig:8a0c0fb09101', 'grp:aggregate:d0:it.category:input:it.sk'), ('grp:aggregate:d0:it.category:input:it.sk', 'grp:filter:d*:it.category:sig:8a0c0fb09101')]
  [v4] group-graph lineage cycle, skipping concept-set pass: [('grp:filter:d*:it.category:sig:8a0c0fb09101', 'grp:aggregate:d0:it.category:input:it.sk'), ('grp:aggregate:d0:it.category:input:it.sk', 'grp:filter:d*:it.category:sig:8a0c0fb09101')]
  ```
- `trilogy file write probe_pop.preql --run-and-delete`

  ```text
  Unexpected error in probe_pop.preql: (_duckdb.BinderException) Binder Error: aggregate function calls cannot be nested

  LINE 28:     (count(max(CASE WHEN ("cs_catalog_returns"."CR_ORDER_NUMBER" is...
                      ^
  [SQL:
  WITH
  questionable as (
  SELECT
      "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
      "cs_catalog_sales"."CS_WAREHOUSE_SK" as "cs_warehouse_sk"
  FROM
      "fact_catalog_sales" as "cs_catalog_sales"
  GROUP BY
      1,
      2),
  abundant as (
  SELECT
      "questionable"."cs_order_number" as "cs_order_number",
      (count(distinct "questionable"."cs_warehouse_sk") > 1) as "multi_warehouse"
  FROM
      "questionable"
  GROUP BY
      1),
  vacuous as (
  SELECT
      "abundant"."cs_order_number" as "cs_order_number",
      CASE WHEN "abundant"."multi_warehouse" = True THEN "abundant"."cs_order_number" ELSE NULL END as "_virt_filter_order_number_7120941346010623"
  FROM
      "abundant"),
  wakeful as (
  SELECT
      (count(max(CASE WHEN ("cs_catalog_returns"."CR_ORDER_NUMBER" is not null) = True THEN coalesce("cs_catalog_returns"."CR_ORDER_NUMBER","cs_catalog_sales"."CS_ORDER_NUMBER") ELSE NULL END)) = 0) as "no_catalog_return",
      coalesce("cs_catalog_returns"."CR_ORDER_NUMBER","cs_catalog_sales"."CS_ORDER_NUMBER") as "cs_order_number"
  FROM
      "fact_catalog_sales" as "cs_catalog_sales"
      LEFT OUTER JOIN "fact_catalog_returns" as "cs_catalog_returns" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_catalog_returns"."CR_ITEM_SK" AND "cs_catalog_sales"."CS_ORDER_NUMBER" = "cs_catalog_returns"."CR_ORDER_NUMBER"
  GROUP BY
      2),
  yummy as (
  SELECT
      CASE WHEN ( "abundant"."multi_warehouse" and "wakeful"."no_catalog_return" ) THEN coalesce("abundant"."cs_order_number","wakeful"."cs_order_number") ELSE NULL END as "_virt_filter_order_number_9284513565393913",
      coalesce("abundant"."cs_order_number","wakeful"."cs_order_number") as "cs_order_number"
  FROM
      "abundant"
      FULL JOIN "wakeful" on "abundant"."cs_order_number" = "wakeful"."cs_order_number"),
  cooperative as (
  SELECT
      "wakeful"."cs_order_number" as "cs_order_number",
      CASE WHEN "wakeful"."no_catalog_return" = True THEN "wakeful"."cs_order_number" ELSE NULL END as "_virt_filter_order_number_6056746762473999"
  FROM
      "wakeful")
  SELECT
      count("vacuous"."_virt_filter_order_number_7120941346010623") as "multi_wh_orders",
      count("cooperative"."_virt_filter_order_number_6056746762473999") as "no_ret_orders",
      count("yummy"."_virt_filter_order_number_9284513565393913") as "both_orders"
  FROM
      "yummy"
      FULL JOIN "vacuous" on "yummy"."cs_order_number" = "vacuous"."cs_order_number"
      FULL JOIN "cooperative" on coalesce("yummy"."cs_order_number", "vacuous"."cs_order_number") is not distinct from "cooperative"."cs_order_number"]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `syntax-parse`

- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:15
    |
  4 | with multi as (
    |               ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
   within an id?
   with multi as ??? (
     select
       i.id as item_...
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  refused to write 'probe6.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:1
     |
  11 | union
     | ^---
     |
     = expected join_clause, metadata, limit, order_by, where, having, or select_grouping
  Location:
  ...year is null) as n_null_year
   ??? union
   select
     'CATALOG' as
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   csr.customer.sk is not null
   ??? group by 1
   order by csr.custo...
  ```

### `undefined-concept`

- `trilogy file write probe19.preql --run-and-delete`

  ```text
  Syntax error in probe19.preql: Undefined concept: _virt_func_hash_6919905492247878.
  ```

### `cli-misuse`

- `trilogy explore raw/catalog_store_returns.preql --show grain`

  ```text
  Invalid value for '--show': 'grain' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```
