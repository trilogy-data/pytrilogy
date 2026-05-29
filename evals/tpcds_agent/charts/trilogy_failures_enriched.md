# Trilogy failure analysis — 20260529-191522

- Run `20260529-191521_enriched` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 3031 | failed: 399 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 146 | 37% |
| `syntax-parse` | 116 | 29% |
| `cli-misuse` | 44 | 11% |
| `join-resolution` | 44 | 11% |
| `syntax-missing-alias` | 35 | 9% |
| `undefined-concept` | 11 | 3% |
| `type-error` | 3 | 1% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.catalog_2002', which is not in the
  SELECT projection (line 22). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.catalog_2002`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.catalog_2002', which is not in the
  SELECT projection (line 23). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.catalog_2002`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.catalog_2002', which is not in the
  SELECT projection (line 23). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.catalog_2002`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "all_sales_sales_channel" must appear in the GROUP BY clause or must be part of
  an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(all_sales_sales_channel)"
  if the exact value of "all_sales_sales_channel" is not important.

  LINE 131:     "abhorrent"."all_sales_sales_channel" as "channel",
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as
  "all_sales_channel_dim_id",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as
  "all_sales_channel_dim_text_id",

  …
  "abhorrent"."total_returns" as "total_returns",
      sum("abhorrent"."all_sales_net_profit") -
  sum("abhorrent"."all_sales_return_net_loss") as "profit",
      "abhorrent"."g_channel" as "g_channel",
      "abhorrent"."g_outlet" as "g_outlet"
  FROM
      "abhorrent"
  ORDER BY
      ( CASE
          WHEN "abhorrent"."g_channel" = 1 THEN 0
          ELSE 1
          END ) asc,
      ( CASE
          WHEN "abhorrent"."g_outlet" = 1 THEN 0
          ELSE 1
          END ) asc,
      "channel" asc nulls first,
      "abhorrent"."outlet" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "all_sales_sales_channel" must appear in the GROUP BY clause or must be part of
  an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(all_sales_sales_channel)"
  if the exact value of "all_sales_sales_channel" is not important.

  LINE 131:     "abhorrent"."all_sales_sales_channel" as "channel",
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as
  "all_sales_channel_dim_id",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as
  "all_sales_channel_dim_text_id",

  …
  _sales_channel" as "channel",
      "abhorrent"."outlet" as "outlet",
      "abhorrent"."total_sales" as "total_sales",
      "abhorrent"."total_returns" as "total_returns",
      sum("abhorrent"."all_sales_net_profit") -
  sum("abhorrent"."all_sales_return_net_loss") as "profit",
      "abhorrent"."g_channel" as "g_channel",
      "abhorrent"."g_outlet" as "g_outlet"
  FROM
      "abhorrent"
  ORDER BY
      "abhorrent"."g_channel" asc,
      "abhorrent"."g_outlet" asc,
      "channel" asc nulls first,
      "abhorrent"."outlet" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "all_sales_sales_channel" must appear in the GROUP BY clause or must be part of
  an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(all_sales_sales_channel)"
  if the exact value of "all_sales_sales_channel" is not important.

  LINE 127:     "abhorrent"."all_sales_sales_channel" as "channel",
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as
  "all_sales_channel_dim_id",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as
  "all_sales_channel_dim_text_id",

  …
  oung"."all_sales_channel_dim_text_id" AND
  "sparkling"."all_sales_sales_channel" = "young"."all_sales_sales_channel")
  SELECT
      "abhorrent"."all_sales_sales_channel" as "channel",
      "abhorrent"."outlet" as "outlet",
      "abhorrent"."total_sales" as "total_sales",
      "abhorrent"."total_returns" as "total_returns",
      sum("abhorrent"."all_sales_net_profit") -
  sum("abhorrent"."all_sales_return_net_loss") as "profit"
  FROM
      "abhorrent"
  ORDER BY
      "channel" asc nulls first,
      "abhorrent"."outlet" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.BinderException) Binder Error: column "total_sales"
  must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(total_sales)" if the
  exact value of "total_sales" is not important.

  LINE 131:     "abhorrent"."total_sales" as "total_sales",
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as
  "all_sales_channel_dim_id",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as
  "all_sales_channel_dim_text_id",
       'CATALOG'  as "all_sales_sales_channel"

  …
  es" as "total_sales",
      "abhorrent"."total_returns" as "total_returns",
      sum("abhorrent"."all_sales_net_profit") -
  sum("abhorrent"."all_sales_return_net_loss") as "profit",
      "abhorrent"."all_sales_sales_channel" as "channel",
      "abhorrent"."outlet" as "outlet",
      "abhorrent"."g_channel" as "g_channel",
      "abhorrent"."g_outlet" as "g_outlet"
  FROM
      "abhorrent"
  ORDER BY
      "abhorrent"."g_channel" asc,
      "abhorrent"."g_outlet" asc,
      "channel" asc nulls first,
      "abhorrent"."outlet" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.BinderException) Binder Error: column "total_sales"
  must appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(total_sales)" if the
  exact value of "total_sales" is not important.

  LINE 125:     "abhorrent"."total_sales" as "total_sales",
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as
  "all_sales_channel_dim_id",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as
  "all_sales_channel_dim_text_id",
       'CATALOG'  as "all_sales_sales_channel"

  …
  return_net_loss"
  FROM
      "sparkling"
      FULL JOIN "young" on "sparkling"."all_sales_channel_dim_text_id" is not
  distinct from "young"."all_sales_channel_dim_text_id" AND
  "sparkling"."all_sales_sales_channel" = "young"."all_sales_sales_channel")
  SELECT
      "abhorrent"."total_sales" as "total_sales",
      "abhorrent"."total_returns" as "total_returns",
      sum("abhorrent"."all_sales_net_profit") -
  sum("abhorrent"."all_sales_return_net_loss") as "profit"
  FROM
      "abhorrent"
  ORDER BY
      "abhorrent"."total_sales" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql`

  ```text
  Recursion error building concept local.profit with grain
  Grain<all_sales.item.id,all_sales.order_id,all_sales.sales_channel> and lineage
  subtract(sum(ref:all_sales.net_profit)<abstract>,sum(ref:all_sales.return_net_l
  oss)<['ref:all_sales.channel_dim_text_id', 'ref:all_sales.sales_channel']>).
  This is likely due to a circular reference.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 74 (char 73). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.ConversionException) Conversion Error: Type VARCHAR
  with value '31904' can't be cast to the destination type VARCHAR[] when casting
  from source column s_zip

  LINE 29: ..."."D_YEAR" = 1998 and "store_sales_date_date"."D_QOY" = 2 and
  "store_sales_store_store"."S_ZIP" in (STRING_SPLIT( $1 ...
                                                                            ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "store_sales_billing_customer_address_customer_address"."CA_ZIP" as
  "store_sales_billing_customer_address_zip"
  FROM
      "customer_address" as
  "store_sales_billing_customer_address_cu
  …
      "store_sales_date_date"."D_YEAR" = 1998 and "store_sales_date_date"."D_QOY"
  = 2 and "store_sales_store_store"."S_ZIP" in (STRING_SPLIT( $1 , ',' ))
  )
  SELECT
      "abundant"."store_sales_store_name" as "store_sales_store_name",
      sum("abundant"."store_sales_net_profit") as "total_net_profit"
  FROM
      "abundant"
      INNER JOIN "wakeful" on
  "abundant"."store_sales_billing_customer_address_zip" =
  "wakeful"."store_sales_billing_customer_address_zip"
  GROUP BY
      1
  ORDER BY
      "abundant"."store_sales_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/9h9h)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 94 (char 93). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw/physical_sales:ps --import raw/web_sales:ws select ps.billing_customer.text_id, sum(ps.ext_list_price - ps.ext_discount_amount ? ps.…_discount_amount ? ws.date.year=2001) as web_rev_2001, sum(ws.ext_sales_price - ws.ext_discount_amount ? ws.date.year=2002) as web_rev_2002 limit 10;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_rev_2001', 'local.web_rev_2002', 'local.web_rev_2001'}
  out of  with found {'local.store_rev_2001', 'ps.billing_customer.text_id',
  'local.store_rev_2002'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.avg_sale_value_all', which is not in
  the SELECT projection (line 22). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.avg_sale_value_all`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.avg_sale_value_all', which is not in
  the SELECT projection (line 17). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.avg_sale_value_all`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 205: ..."."total_sales" > CASE WHEN INVALID_REFERENCE_BUG_<Missing source
  reference to all_sales.date.year> BETWEEN 1999 AND...
                                                                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_sales_unified"."CS_ORDER_NUMBER" as
  "all_sales_order_id",
       1  as "all_sales_row_one",
       'CATALOG
  …
  REFERENCE_BUG_<Missing source reference to all_sales.list_price> ELSE
  NULL END / CASE WHEN CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference
  to all_sales.date.year> BETWEEN 1999 AND 2001 THEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.order_id> ELSE
  NULL END IS NOT NULL THEN 1 ELSE 0 END or ( "scrawny"."g_channel" = 1 )

  ORDER BY
      "scrawny"."g_channel" asc nulls first,
      "scrawny"."g_brand" asc nulls first,
      "scrawny"."g_class" asc nulls first,
      "scrawny"."g_cat" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query14.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 179: ..."."total_sales" > CASE WHEN INVALID_REFERENCE_BUG_<Missing source
  reference to all_sales.date.year> BETWEEN 1999 AND...
                                                                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_sales_unified"."CS_ORDER_NUMBER" as
  "all_sales_order_id",
       1  as "all_sales_row_one",
       'CATALOG
  …
  ty> *
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.list_price> ELSE
  NULL END / CASE WHEN CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference
  to all_sales.date.year> BETWEEN 1999 AND 2001 THEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.order_id> ELSE
  NULL END IS NOT NULL THEN 1 ELSE 0 END or ( "late"."g_channel" = 1 )

  ORDER BY
      "late"."g_channel" asc nulls first,
      "late"."g_brand" asc nulls first,
      "late"."g_class" asc nulls first,
      "late"."g_cat" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'all_sales.date.year', which is not in the
  SELECT projection (line 13). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --all_sales.date.year`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query16.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run raw/catalog_sales.preql --import raw/catalog_sales:catalog_sales select catalog_sales.item.text_id, catalog_sales.item.desc, catalog_sales.item.category, catalog_sales.item.class, catalog_sales.item.current_price, catalog_sales.ext_sales_price, catalog_sales.sold_date.date limit 3;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 100 (char 99). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 55 (char 54). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query31.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_sales_q1', 'local.web_sales_q1'} out of  with found
  {'local.store_sales_q1', 'local.store_sales_q3', 'local.web_sales_q3',
  'local.web_sales_q1', 'local.store_sales_q2', 'web_sales.bill_address.county',
  'local.web_sales_q2', 'store_sales.sale_address.county'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 4 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run query31.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.web_sales_q1', 'local.store_sales_q1'} out of  with found
  {'local.web_sales_q1', 'local.store_sales_q1', 'local.web_sales_q2',
  'local.store_sales_q2', 'web_sales.bill_address.county', 'local.web_sales_q3',
  'local.store_sales_q3', 'store_sales.sale_address.county'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query36.preql --all-rows --displayed-rows 200`

  ```text
  Recursion error building concept local.gross_margin with
  grain Grain<physical_sales.item.id,physical_sales.ticket_number> and lineage
  divide(sum(ref:physical_sales.net_profit)<abstract>,sum(ref:physical_sales.ext_
  sales_price)<abstract>). This is likely due to a circular reference.
  ```
- `trilogy run query36.preql --all-rows --displayed-rows 200`

  ```text
  Recursion error building concept local.gross_margin with
  grain Grain<physical_sales.item.id,physical_sales.ticket_number> and lineage
  divide(sum(ref:physical_sales.net_profit)<abstract>,sum(ref:physical_sales.ext_
  sales_price)<abstract>). This is likely due to a circular reference.
  ```
- `trilogy run query36.preql --all-rows --displayed-rows 200`

  ```text
  Recursion error building concept local.gross_margin with
  grain Grain<physical_sales.item.id,physical_sales.ticket_number> and lineage
  divide(sum(ref:physical_sales.net_profit)<abstract>,sum(ref:physical_sales.ext_
  sales_price)<abstract>). This is likely due to a circular reference.
  ```
- `trilogy run query36.preql --all-rows --displayed-rows 200`

  ```text
  Recursion error building concept local.g_cat with grain
  Grain<local.within_parent_rank,physical_sales.item.category,physical_sales.item
  .class> and lineage
  grouping(ref:physical_sales.item.category)<['ref:physical_sales.item.category',
  'ref:physical_sales.item.class', 'ref:local.within_parent_rank']>. This is
  likely due to a circular reference.
  ```
- `trilogy run query37.preql duckdb`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy database describe warehouse_inventory`

  ```text
  Table 'warehouse_inventory' not found, or it has no columns.
  ```
- `trilogy run query40.preql`

  ```text
  Unknown cast target 'usd': expected a data type or a
  registered trait.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 79 (char 78). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 79 (char 78). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 79 (char 78). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 79 (char 78). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 87 (char 86). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 87 (char 86). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query47.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 79 (char 78). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 90 (char 89). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 91 (char 90). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 90 (char 89). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query51.preql`

  ```text
  All arguments to coalesce must be of the same type, have
  {<DataType.INTEGER: 'int'>, TraitDataType(type=NumericType(precision=15,
  scale=2), traits=['usd'])} for [ref:local.store_run_total,
  sum(ref:local._virt_6338137402595305, offset=None) over  order
  [OrderItem(expr=ref:store.date.date, order=<Ordering.ASCENDING: 'asc'>)]]
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 90 (char 89). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query51.preql`

  ```text
  (_duckdb.BinderException) Binder Error: WHERE clause cannot
  contain window functions!

  LINE 130:     max("sparkling"."_virt_func_coalesce_8709395116109855")...
                ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "web_web_sales"."WS_ITEM_SK" as "web_item_id",
      "web_web_sales"."WS_SOLD_DATE_SK" as "web_date_id"
  FROM
      "web_sales" as "web_web_sales"
  GROUP BY
      1,
      2),
  uneven as (
  SELECT
      "web_web_sales"."WS_ITEM_SK" as "web_item_id",
      cast("web_date_date"."D_DATE" as date) as "web_date_date",
      sum("web_web_sales"."WS_EXT_SALES_PRICE") as "web_daily"
  FROM
      "web_sales" a
  …
  g"."store_date_date","young"."store_date_date"),coal
  esce("sparkling"."web_date_date","young"."web_date_date")) asc ) >
  max("sparkling"."_virt_func_coalesce_774149616310123") over (partition by
  "sparkling"."_virt_func_coalesce_4935530997754253" order by
  coalesce(coalesce("sparkling"."store_date_date","young"."store_date_date"),coal
  esce("sparkling"."web_date_date","young"."web_date_date")) asc )

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6
  ORDER BY
      "item_id" asc nulls first,
      "sale_date" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query54.preql`

  ```text
  Recursion error building concept local.segment with grain
  Grain<physical_sales.billing_customer.id> and lineage
  round(divide(ref:local.store_sales_by_customer,50),0). This is likely due to a
  circular reference.
  ```
- `trilogy run query54.preql`

  ```text
  Recursion error building concept local.segment with grain
  Grain<physical_sales.billing_customer.id> and lineage
  round(divide(ref:local.store_sales_by_customer,50),0). This is likely due to a
  circular reference.
  ```
- `trilogy run query54.preql`

  ```text
  Recursion error building concept local.segment with grain
  Grain<physical_sales.billing_customer.id> and lineage
  round(divide(ref:local.store_sales_by_customer,50),0). This is likely due to a
  circular reference.
  ```
- `trilogy run query54.preql`

  ```text
  Recursion error building concept local.segment with grain
  Grain<physical_sales.billing_customer.id> and lineage
  round(divide(ref:local.store_sales_by_customer,50),0). This is likely due to a
  circular reference.
  ```
- `trilogy run query54.preql`

  ```text
  Recursion error building concept local.segment with grain
  Grain<physical_sales.billing_customer.id> and lineage
  round(divide(ref:local.store_sales_total,50),0). This is likely due to a
  circular reference.
  ```
- `trilogy run query54.preql`

  ```text
  Recursion error building concept local.customer_count with
  grain Grain<local.store_sales_total> and lineage
  count(ref:physical_sales.billing_customer.id)<['ref:local.store_sales_total']>.
  This is likely due to a circular reference.
  ```
- `trilogy run query54.preql`

  ```text
  Recursion error building concept local.store_sales_total with
  grain Grain<physical_sales.billing_customer.id> and lineage sum(<Filter:
  ref:physical_sales.ext_sales_price where ref:physical_sales.date.month_seq >=
  add(ref:local.dec1998_month_seq,1) and ref:physical_sales.date.month_seq <=
  add(ref:local.dec1998_month_seq,3) and ref:physical_sales.store.county =
  ref:physical_sales.billing_customer.address.county and
  ref:physical_sales.store.state =
  ref:physical_sales.billing_customer.address.state>)<['ref:physical_sales.billin
  g_customer.id']>. This is likely due to a circular reference.
  ```
- `trilogy run query54.preql`

  ```text
  Recursion error building concept local.segment with grain
  Grain<physical_sales.billing_customer.id> and lineage
  round(divide(ref:local.store_total_by_cust,50),0). This is likely due to a
  circular reference.
  ```
- `trilogy run query54.preql`

  ```text
  Recursion error building concept local.customer_count with
  grain Grain<local.store_total_by_cust> and lineage
  count(ref:physical_sales.billing_customer.id)<['ref:local.store_total_by_cust']
  >. This is likely due to a circular reference.
  ```
- `trilogy run query54.preql`

  ```text
  Recursion error building concept local.raw_segment with grain
  Grain<physical_sales.billing_customer.id> and lineage
  divide(ref:local.store_total_by_cust,50). This is likely due to a circular
  reference.
  ```
- `trilogy run -e --import raw.item:item --import raw.catalog_sales:catalog_sales duck_db select item.class_id, catalog_sales.item.id limit 5;`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing
  env file: --import
  ```
- `trilogy run -e raw.item:item raw.catalog_sales:catalog_sales duck_db select item.class_id, catalog_sales.item.id limit 5;`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing
  env file: raw.item:item
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.sold_date.year, catalog_sales.sold_date.month_of_year, sum(catalog_sales.ext_sales_price) as monthly_total where catalog_sales.sold_date.year in (1998,1999,2000) order by catalog_sales.sold_date.month_seq limit 20;`

  ```text
  ORDER BY references 'catalog_sales.sold_date.month_seq',
  which is not in the SELECT projection (line 2). Add it to SELECT to sort by it
  — prefix with `--` to keep it out of the output rows, e.g. `select ...,
  --catalog_sales.sold_date.month_seq order by catalog_sales.sold_date.month_seq
  asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 82 (char 81). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query57.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run query57.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 91:     INVALID_REFERENCE_BUG_<Missing source reference to
  cs.call_center.name> asc,
                                              ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "cs_call_center_call_center"."CC_NAME" as "cs_call_center_name",
      "cs_item_items"."I_BRAND" as "cs_item_brand_name",
      "cs_item_items"."I_CATEGORY" as "cs_item_category",
      "cs_sold_date_date"."D_MOY" as "cs_sold_date_month_of_year",
      "cs_sold_date_date"."D_YEAR" as "cs_sold_date_year",
      sum(CASE WHEN "cs_sold_date_date"."D_YEAR" =
  …
  _month_total",
      "yummy"."next_month_total" as "next_month_total"
  FROM
      "yummy"
  WHERE
      abs("yummy"."monthly_total" - "yummy"."avg_monthly_by_year") /
  "yummy"."avg_monthly_by_year" > 0.1

  ORDER BY
      ( "yummy"."monthly_total" - "yummy"."avg_monthly_by_year" ) asc nulls
  first,
      "yummy"."cs_item_category" asc,
      "yummy"."cs_item_brand_name" asc,
      INVALID_REFERENCE_BUG_<Missing source reference to cs.call_center.name>
  asc,
      "yummy"."cs_sold_date_year" asc,
      "yummy"."cs_sold_date_month_of_year" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query57.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query57.preql`

  ```text
  HAVING references 'local.rel_deviation', which is not in the
  SELECT projection (line 19). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.rel_deviation`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query57.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.item.id, sum(all_sales.ext_sales_price ? all_sales.sales_channel = 'STORE') as store_total, sum…WEB') as web_total where all_sales.date.week_seq = 5218 and store_total is not null and catalog_total is not null and web_total is not null limit 20;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.store_total) in the same statement where clause; move to the HAVING
  clause instead; Line: 2
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 83 (char 82). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw/physical_sales:ps --import raw/catalog_sales:cs --import raw/catalog_returns:cr select ps.item.id as iid, count(ps.row_counter) as s…ales_cnt, count(cs.row_counter) as catalog_sales_cnt, sum(cr.refunded_cash) + sum(cr.reversed_charge) + sum(cr.store_credit) as total_refund limit 5;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.catalog_sales_cnt', 'local._virt_agg_sum_8884450178666315',
  'local._virt_agg_sum_1615300313462831', 'local._virt_agg_sum_9109582128025445',
  'local.store_sales_cnt'} out of  with found {'local.store_sales_cnt',
  'ps.item.id'}
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr -e merge cs.item.id into ~cr.sales.item.id; select cs.item.id as iid, sum(cs.ext_li…coalesce(cr.refunded_cash,0) + coalesce(cr.reversed_charge,0) + coalesce(cr.store_credit,0)) as cum_refund_amt, count(cs.row_counter) as cnt limit 5;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr --import raw/item:item merge cs.item.id into ~cr.sales.item.id; select cs.item.id a…fund_amt where sum(cs.ext_list_price) > 2 * sum(coalesce(cr.refunded_cash,0) + coalesce(cr.reversed_charge,0) + coalesce(cr.store_credit,0)) limit 5;`

  ```text
  WHERE clause aggregate `sum(cs.ext_list_price)` is also
  computed in the SELECT (as `cum_ext_list_price`); aggregate filters must use
  the HAVING clause - e.g. `having cum_ext_list_price > ...`; Line: 4
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query65.preql`

  ```text
  HAVING references 'local.store_avg_revenue', which is not in
  the SELECT projection (line 9). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.store_avg_revenue`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query65.preql`

  ```text
  ORDER BY references 'physical_sales.store.id', which is not
  in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --physical_sales.store.id order by physical_sales.store.id asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query67.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 93:     INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.store.text_id> asc nulls...
                                              ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "store_sales_date_date"."D_MOY" as "store_sales_date_month_of_year",
      "store_sales_date_date"."D_QOY" as "store_sales_date_quarter",
      "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
      "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
      "store_sales_item_items"."I_CATEGORY" as "store_sales_i
  …
  class" asc nulls first,
      "abundant"."store_sales_item_brand_name" asc nulls first,
      "abundant"."store_sales_item_product_name" asc nulls first,
      "abundant"."store_sales_date_year" asc nulls first,
      "abundant"."store_sales_date_quarter" asc nulls first,
      "abundant"."store_sales_date_month_of_year" asc nulls first,
      INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.store.text_id> asc nulls first,
      "abundant"."summed_sales" asc nulls first,
      "abundant"."within_cat_rank" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query67.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 90:     INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.store.text_id> asc nulls...
                                              ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "store_sales_date_date"."D_MOY" as "store_sales_date_month_of_year",
      "store_sales_date_date"."D_QOY" as "store_sales_date_quarter",
      "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
      "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
      "store_sales_item_items"."I_CATEGORY" as "store_sales_i
  …
  class" asc nulls first,
      "abundant"."store_sales_item_brand_name" asc nulls first,
      "abundant"."store_sales_item_product_name" asc nulls first,
      "abundant"."store_sales_date_year" asc nulls first,
      "abundant"."store_sales_date_quarter" asc nulls first,
      "abundant"."store_sales_date_month_of_year" asc nulls first,
      INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.store.text_id> asc nulls first,
      "abundant"."summed_sales" asc nulls first,
      "abundant"."within_cat_rank" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 87 (char 86). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query75.preql`

  ```text
  Cannot reference an aggregate derived in the select
  (local.prev_qty) in the same statement where clause; move to the HAVING clause
  instead; Line: 25
  ```
- `trilogy run query75.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query75.preql`

  ```text
  Cannot reference an aggregate derived in the select
  (local.prev_qty) in the same statement where clause; move to the HAVING clause
  instead; Line: 20
  ```
- `trilogy run query75.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query75.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query75.preql`

  ```text
  Cannot reference an aggregate derived in the select
  (local.prev_qty) in the same statement where clause; move to the HAVING clause
  instead; Line: 18
  ```
- `trilogy run query75.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query75.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query76.preql`

  ```text
  Aggregate concept local.line_count cannot reference itself.
  If defining a new concept in a select, use a new name.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.item.id as item_id, sum(all_sales.return_quantity ? all_sales.sales_channel = 'STORE' and all_s…7, 5264)) as web_qty where store_qty > 0 and catalog_qty > 0 and web_qty > 0 order by all_sales.item.id nulls first, store_qty nulls first limit 100;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.store_qty) in the same statement where clause; move to the HAVING clause
  instead; Line: 2
  ```
- `trilogy run --import raw.all_sales:all_sales auto store_qty <- sum(all_sales.return_quantity ? all_sales.sales_channel = 'STORE' and all_sales.return_date.we…y,
    coalesce(store_qty, 0) + coalesce(catalog_qty, 0) + coalesce(web_qty, 0) as total_qty
where store_qty > 0
order by all_sales.item.id
limit 20;`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY clause
  cannot contain aggregates!

  LINE 75:     ( coalesce("yummy"."store_qty",0) + coalesce(sum(CASE WHEN
  "yummy"."all_sales_sales_channel" = 'CATALOG...
                                                            ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_returns_unified"."CR_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_returns_unified"."CR_ORDER_NUMBER" as
  "all_sales_order_id",
      "all_sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as
  "all_sales_return_date_id",
      "all_sales_catalog_returns_unified"."CR_
  …
  WEB' and
  "yummy"."all_sales_return_date_week_seq" in (5244,5257,5264) THEN
  "yummy"."all_sales_return_quantity" ELSE NULL END),0) as "total_qty"
  FROM
      "yummy"
      LEFT OUTER JOIN "cooperative" on "yummy"."all_sales_item_id" =
  "cooperative"."all_sales_item_id" AND "yummy"."all_sales_order_id" =
  "cooperative"."all_sales_order_id" AND "yummy"."all_sales_sales_channel" =
  "cooperative"."all_sales_sales_channel"
  WHERE
      "yummy"."store_qty" > 0

  GROUP BY
      1,
      2,
      4
  ORDER BY
      "yummy"."all_sales_item_id" asc
  LIMIT (20)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy database describe customer_address --schema tpcds`

  ```text
  Table 'customer_address' not found, or it has no columns.
  ```
- `trilogy run --debug -e import raw.physical_returns as store_returns; select count(store_returns.ticket_number) limit 10;`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing
  env file: import raw.physical_returns as store_returns;
  ```
- `trilogy run --import raw.web_returns:wsr select wsr.web_sales.date.year, count(wsr.web_sales.order_number) as cnt order by year limit 20;`

  ```text
  ORDER BY references 'local.year', which is not in the SELECT
  projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep
  it out of the output rows, e.g. `select ..., --local.year order by local.year
  asc`.
  ```
- `trilogy run --import raw.web_returns:wsr select wsr.web_sales.date.year, count(wsr.web_sales.order_number) as cnt order by year,cnt limit 20;`

  ```text
  ORDER BY references 'local.year', which is not in the SELECT
  projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep
  it out of the output rows, e.g. `select ..., --local.year order by local.year
  asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 63 (char 62). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 88 (char 87). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 92 (char 91). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query97.preql`

  ```text
  Property local._virt_filter_2860582759452170 missing keys in
  lookup
  ```

### `syntax-parse`

- `trilogy file write query01.preql --content import physical_sales as store_sales;

# For returns to stores in Tennessee in 2000, find customers whose total re… store_sales.return_customer.text_id as c_customer_id
having
    customer_store_return > 1.2 * store_avg_return
order by
    c_customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 11, column 1.
  Expected one of:
          * SELECT

  Location:
  ...er store: total return amount ??? auto customer_store_return <-

  Write stats: received 799 chars / 799 bytes; tail: …'1.2 *
  store_avg_return\\norder by\\n    c_customer_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql --content import physical_sales as store_sales;

# For returns to stores in Tennessee in 2000, find customers whose total re… store_sales.return_customer.text_id as c_customer_id
having
    customer_store_return > 1.2 * store_avg_return
order by
    c_customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 11, column 1.
  Expected one of:
          * SELECT

  Location:
  ...er store: total return amount ??? auto customer_store_return <-

  Write stats: received 799 chars / 799 bytes; tail: …'1.2 *
  store_avg_return\\norder by\\n    c_customer_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql --content import physical_sales as store_sales;

where store_sales.return_store.state = 'TN'
  and store_sales.return_date.y… store_sales.return_customer.text_id as c_customer_id
having
    customer_store_return > 1.2 * store_avg_return
order by
    c_customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 6, column 1.
  Expected one of:
          * SELECT

  Location:
  ...ales.return_date.year = 2000  ??? auto customer_store_return <-

  Write stats: received 496 chars / 496 bytes; tail: …'1.2 *
  store_avg_return\\norder by\\n    c_customer_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql -e -c import physical_sales as store_sales;

where store_sales.return_store.state = 'TN'
  and store_sales.return_date.year … store_sales.return_customer.text_id as c_customer_id
having
    customer_store_return > 1.2 * store_avg_return
order by
    c_customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 6, column 1.
  Expected one of:
          * SELECT

  Location:
  ...ales.return_date.year = 2000  ??? auto customer_store_return <-

  Write stats: received 496 chars / 496 bytes; tail: …'1.2 *
  store_avg_return\\norder by\\n    c_customer_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql -e -c import physical_sales as store_sales;
# For returns to stores in Tennessee in 2000, find customers whose total returns… store_sales.return_customer.text_id as c_customer_id
having
    customer_store_return > 1.2 * store_avg_return
order by
    c_customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 8, column 1.
  Expected one of:
          * SELECT

  Location:
  ...er store: total return amount ??? auto customer_store_return <-

  Write stats: received 794 chars / 794 bytes; tail: …'1.2 *
  store_avg_return\\norder by\\n    c_customer_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql -e -c import physical_sales;
# For returns to stores in Tennessee in 2000, find customers whose total returns for a store
# …ysical_sales.return_customer.text_id as c_customer_id
having
    customer_store_return > 1.2 * store_avg_return
order by
    c_customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 8, column 1.
  Expected one of:
          * SELECT

  Location:
  ...er store: total return amount ??? auto customer_store_return <-

  Write stats: received 800 chars / 800 bytes; tail: …'1.2 *
  store_avg_return\\norder by\\n    c_customer_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/physical_sales:store_sales # quick check
where store_sales.return_store.state = 'TN'
  and store_sales.return_date.year = 2000
auto …) by store_sales.return_store.id;
select store_sales.return_customer.text_id as c_customer_id
having csr > 1.2 * sar
order by c_customer_id
limit 10;`

  ```text
  --> 5:1
    |
  5 | auto csr <- sum(store_sales.return_amount) by
  store_sales.return_customer.id, store_sales.return_store.id;
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...sales.return_date.year = 2000 ??? auto csr <- sum(store_sales.re...
  ```
- `trilogy file write query04.preql --content import raw/physical_sales.preql as store;
import raw/catalog_sales.preql as catalog;
import raw/web_sales.preql as…st_flag
order by
    customer_code nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;`

  ```text
  …
  talog_ratio > store_ratio\n  and
  catalog_ratio > web_ratio\n\nselect\n    store.billing_customer.text_id as
  customer_code,\n    store.billing_customer.first_name as first_name,\n
  store.billing_customer.last_name as last_name,\n
  store.billing_customer.preferred_cust_flag as preferred_cust_flag\norder by\n
  customer_code nulls first,\n    first_name nulls first,\n    last_name nulls
  first,\n    preferred_cust_flag nulls first\nlimit 100;") at line 1, column 11.
  Expected one of:
          * IMPORT_DOT
          * _TERMINATOR
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /physical_sales.preql as store...

  Write stats: received 2563 chars / 2563 bytes; tail: …' nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Merge the…st_flag
order by
    customer_code nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 29, column 1.
  Expected one of:
          * SELECT

  Location:
  ...uaranteed by the where above) ??? auto catalog_ratio <- catalog_...

  Write stats: received 2545 chars / 2545 bytes; tail: …' nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Merge the…st_flag
order by
    customer_code nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
  ..., which is already guaranteed ??? by the where above) where cata...

  Write stats: received 2382 chars / 2382 bytes; tail: …' nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Merge the…st_flag
order by
    customer_code nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
  ..., which is already guaranteed ??? by the where above) where (cat...

  Write stats: received 2386 chars / 2386 bytes; tail: …' nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --escapes -e -c import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Mer…st_flag
order by
    customer_code nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 24, column 1.
  Expected one of:
          * SELECT

  Location:
  ...og_2001 > 0 and web_2001 > 0  ??? where (catalog_2002 / catalog_...

  Write stats: received 2135 chars / 2135 bytes; tail: …' nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --escapes -e -c import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Mer…b_ratio
order by
    customer_code nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 26, column 1.
  Expected one of:
          * SELECT

  Location:
   2001 > 0 already guaranteed) ??? auto catalog_ratio <- catalog_...

  Write stats: received 2357 chars / 2357 bytes; tail: …' nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --escapes -e -c import raw.physical_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Mer…st_flag
order by
    customer_code nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 24, column 1.
  Expected one of:
          * SELECT

  Location:
  ...og_2001 > 0 and web_2001 > 0  ??? where (catalog_2002 / catalog_...

  Write stats: received 2132 chars / 2132 bytes; tail: …' nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Date range: 2000-08-23 to 2000-09-06 inclusive
where all_sales.date.date bet…sales as sales,
    group_returns as returns,
    group_profit as profit
order by
    channel asc nulls first,
    outlet asc nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...rofit (net profit - net loss) ??? auto group_sales <- sum(all_sa...

  Write stats: received 889 chars / 889 bytes; tail: …'nnel asc nulls first,\\n
  outlet asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

# Date range: 2000-08-23 to 2000-09-06 inclusive
where all_sales.date.date between…_sales as sales,
    group_returns as returns,
    group_profit as profit
order by
    channel asc nulls first,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...rofit (net profit - net loss) ??? auto group_sales <- sum(all_sa...

  Write stats: received 888 chars / 888 bytes; tail: …'annel asc nulls first,\\n
  outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Date range: 2000-08-23 to 2000-09-06 inclusive
where all_sales.date.date bet…_sales as sales,
    group_returns as returns,
    group_profit as profit
order by
    channel asc nulls first,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...rofit (net profit - net loss) ??? auto group_sales <- sum(all_sa...

  Write stats: received 888 chars / 888 bytes; tail: …'annel asc nulls first,\\n
  outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date

#…ales.channel_dim_text_id as g_outlet
order by
    g_channel asc,
    g_outlet asc,
    channel asc nulls first,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...p once, referenced by aliases ??? def rollup_agg(metric) -> sum(...

  Write stats: received 1008 chars / 1008 bytes; tail: …'annel asc nulls
  first,\\n    outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...easures with a single rollup ??? def rollup_agg(metric) -> sum(...

  Write stats: received 975 chars / 975 bytes; tail: …'nel asc nulls first,\r\\n
  outlet asc nulls first\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import physical_sales as store_sales;

parameter zips string;

# Preferred customers with current address ZIP > 10…ect matching_zips)

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('HAVING', 'having') at line 6, column 137.
  Expected one of:
          * _TERMINATOR

  Location:
   billing_customer.address.zip ??? having count_distinct(billing_...

  Write stats: received 1052 chars / 1052 bytes; tail: …'
  total_net_profit\\norder by store_sales.store.name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Step 1: Find ZIP codes (5-digit) where >10 pr…param_zips, 1, 2))

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('HAVING', 'having') at line 9, column 141.
  Expected one of:
          * _TERMINATOR

  Location:
   billing_customer.address.zip ??? having count_distinct(billing_...

  Write stats: received 1340 chars / 1340 bytes; tail: …'
  total_net_profit\\norder by store_sales.store.name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Build the full set of qualifying ZIP prefixes… = param_first_two

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('HAVING', 'having') at line 11, column 177.
  Expected one of:
          * _TERMINATOR

  Location:
  ....billing_customer.address.zip ??? having count_distinct(store_sa...

  Write stats: received 1711 chars / 1711 bytes; tail: …'
  total_net_profit\\norder by store_sales.store.name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.physical_sales as store_sales;

# Bucket definitions: ticket-quantity ranges [1-20, 21-40, 41-60, 61-80…1_40_value as bucket_21_40,
    bucket_41_60_value as bucket_41_60,
    bucket_61_80_value as bucket_61_80,
    bucket_81_100_value as bucket_81_100;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 16, column 2.
  Expected one of:
          * WHEN

  Location:
  ...isc,     bucket_1_20_avg_net ) ??? ;  auto bucket_21_40_lines <-

  Write stats: received 2679 chars / 2679 bytes; tail: …'e as bucket_61_80,\\n
  bucket_81_100_value as bucket_81_100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.physical_sales as store_sales;

# Bucket definitions: ticket-quantity ranges [1-20, 21-40, 41-60, 61-80…1_40_value as bucket_21_40,
    bucket_41_60_value as bucket_41_60,
    bucket_61_80_value as bucket_61_80,
    bucket_81_100_value as bucket_81_100;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'bucket_1_20_lines ') at line 11,
  column 10.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ..._1_20_value <- case(     when ??? bucket_1_20_lines > 74129 then...

  Write stats: received 2621 chars / 2621 bytes; tail: …'e as bucket_61_80,\\n
  bucket_81_100_value as bucket_81_100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.physical_sales as store_sales;

auto bucket_1_20_lines <- count(store_sales.row_counter ? store_sales.q…1_40_value as bucket_21_40,
    bucket_41_60_value as bucket_41_60,
    bucket_61_80_value as bucket_61_80,
    bucket_81_100_value as bucket_81_100;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 6, column 101.
  Expected one of:
          * WHEN

  Location:
  ...avg_disc, bucket_1_20_avg_net) ??? ;  auto bucket_21_40_lines <-

  Write stats: received 2317 chars / 2317 bytes; tail: …'e as bucket_61_80,\\n
  bucket_81_100_value as bucket_81_100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.physical_sales as store_sales;

-- bucket line counts to check against thresholds
auto l1 <- count(stor… ? l5 > 165306, n5);

select
    c1 as bucket_01_20,
    c2 as bucket_21_40,
    c3 as bucket_41_60,
    c4 as bucket_61_80,
    c5 as bucket_81_100;`

  ```text
  …
  IMPORT
          * PERSIST_MODE
          * _DEF_TABLE
          * SELF_IMPORT
          * SELECT
          * PROPERTY
          * PARSE_COMMENT
          * MOCK
          * WITH
          * DATASOURCE_PARTIAL
          * PARAMETER
          * DEF
          * FROM
          * COPY
          * "merge"i
          * _PROPERTIES
          * AUTO
          * UNIQUE
          * PARAM
          * VALIDATE
          * RAW_SQL
          * WHERE
          * PUBLISH_ACTION
          * DATASOURCE
          * SHORTHAND_MODIFIER
          * $END
          * CHART
          * ROWSET
          * DATASOURCE_ROOT
  Previous tokens: [Token('LINE_SEPARATOR', '\n\n')]

  Location:
  ...ysical_sales as store_sales;  ??? -- bucket line counts to check...

  Write stats: received 1837 chars / 1837 bytes; tail: …'cket_41_60,\\n    c4 as
  bucket_61_80,\\n    c5 as bucket_81_100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09_debug2.preql --content import raw.physical_sales as store_sales;
auto l1 <- count(store_sales.row_counter ? store_sales.quantity b…   bucket_1 as bucket_01_20,
    bucket_2 as bucket_21_40,
    bucket_3 as bucket_41_60,
    bucket_4 as bucket_61_80,
    bucket_5 as bucket_81_100;`

  ```text
  refused to write 'query09_debug2.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 6, column 39.
  Expected one of:
          * WHEN

  Location:
  ...ket_1 <- case(l1 > t1, d1, n1) ??? ; auto l2 <- count(store_sales...

  Write stats: received 1756 chars / 1756 bytes; tail: …'    bucket_4 as
  bucket_61_80,\\n    bucket_5 as bucket_81_100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09_debug2.preql --content import raw.physical_sales as store_sales;
auto l1 <- count(store_sales.row_counter ? store_sales.quantity b…   bucket_1 as bucket_01_20,
    bucket_2 as bucket_21_40,
    bucket_3 as bucket_41_60,
    bucket_4 as bucket_61_80,
    bucket_5 as bucket_81_100;`

  ```text
  refused to write 'query09_debug2.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'l1 ') at line 6, column 28.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ...9; auto bucket_1 <- case(when ??? l1 > t1 then d1 else n1); auto...

  Write stats: received 1821 chars / 1821 bytes; tail: …'    bucket_4 as
  bucket_61_80,\\n    bucket_5 as bucket_81_100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09_v2.preql --content import raw.physical_sales as store_sales;

-- Count lines per bucket
auto l1 <- count(store_sales.row_counter ?…e(d5 ? l5 > t5, n5);

select
    c1 as bucket_01_20,
    c2 as bucket_21_40,
    c3 as bucket_41_60,
    c4 as bucket_61_80,
    c5 as bucket_81_100;`

  ```text
  …
  TER
          * _DEF_TABLE
          * PROPERTY
          * PURPOSE
          * SHOW
          * COPY
          * WITH
          * _PROPERTIES
          * IMPORT
          * DATASOURCE_ROOT
          * SELECT
          * CREATE
          * MOCK
          * PARAM
          * UNIQUE
          * DEF
          * TYPE
          * ROWSET
          * DATASOURCE_PARTIAL
          * AUTO
          * PARSE_COMMENT
          * RAW_SQL
          * PUBLISH_ACTION
          * VALIDATE
          * $END
          * FROM
          * "merge"i
          * WHERE
          * PERSIST_MODE
          * SHORTHAND_MODIFIER
  Previous tokens: [Token('LINE_SEPARATOR', '\n\n')]

  Location:
  ...ysical_sales as store_sales;  ??? -- Count lines per bucket auto...

  Write stats: received 1917 chars / 1917 bytes; tail: …'cket_41_60,\\n    c4 as
  bucket_61_80,\\n    c5 as bucket_81_100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09_v4.preql --content import raw.physical_sales as store_sales;

auto r1 <- count(store_sales.row_counter ? store_sales.quantity betw… d3 else n3) as bucket_41_60,
    case(when r4 > 10097 then d4 else n4) as bucket_61_80,
    case(when r5 > 165306 then d5 else n5) as bucket_81_100;`

  ```text
  refused to write 'query09_v4.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'r1 ') at line 22, column 15.
  Expected one of:
          * COMMA
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ...d 100);  select     case(when ??? r1 > 74129 then d1 else n1) as...

  Write stats: received 1615 chars / 1615 bytes; tail: …'    case(when r5 >
  165306 then d5 else n5) as bucket_81_100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09_v4.preql --content import raw.physical_sales as store_sales;
auto r1 <- count(store_sales.row_counter ? store_sales.quantity betwe…et_21_40,
    case(r3 > 56580, d3, n3) as bucket_41_60,
    case(r4 > 10097, d4, n4) as bucket_61_80,
    case(r5 > 165306, d5, n5) as bucket_81_100;`

  ```text
  refused to write 'query09_v4.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 18, column 30.
  Expected one of:
          * WHEN

  Location:
       case(r1 > 74129, d1, n1) ??? as bucket_01_20,     case(r2 >...

  Write stats: received 1546 chars / 1546 bytes; tail: …'ucket_61_80,\\n
  case(r5 > 165306, d5, n5) as bucket_81_100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;
import raw.item as item;

# Compute per-line sale value = quantity * list_price…g_channel = 1)
order by
    g_channel asc nulls first,
    g_brand asc nulls first,
    g_class asc nulls first,
    g_cat asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '-- hide from output but
  available for HAVING\nhaving total_sales > avg_sale_value_all or (g_channel =
  1)\norder by\n    g_channel asc nulls first,\n    g_brand asc nulls first,\n
  g_class asc nulls first,\n    g_cat asc nulls first\nlimit 100;') at line 35,
  column 42.
  Expected one of:
          * MERGE
          * _TERMINATOR
          * WHERE
          * LIMIT
          * METADATA
          * ORDER
          * COMMA
          * HAVING
  Previous tokens: [Token('IDENTIFIER', 'overall_avg')]

  Location:
  ...ale_value_all as overall_avg  ??? -- hide from output but availa...

  Write stats: received 2960 chars / 2960 bytes; tail: …'_class asc nulls
  first,\\n    g_cat asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql -e -c import raw.catalog_sales as catalog_sales;

# Filter: catalog sales in Q2 2001
where catalog_sales.sold_date.year = 20…omer.address.zip as zip_code,
    sum(catalog_sales.sales_price) as total_sales_price
group by zip_code
order by zip_code asc nulls first
limit 100;
`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'group ') at line 20, column 1.
  Expected one of:
          * MERGE
          * WHERE
          * METADATA
          * HAVING
          * ORDER
          * _TERMINATOR
          * COMMA
          * LIMIT
  Previous tokens: [Token('IDENTIFIER', 'total_sales_price')]

  Location:
  ...s_price) as total_sales_price ??? group by zip_code order by zip...

  Write stats: received 731 chars / 731 bytes; tail: …'up by zip_code\\norder by
  zip_code asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query21.preql --content import raw.inventory as inventory;
import raw.item as item;
import raw.warehouse as warehouse;

# Reference date
l…qty as before_window_total,
    after_qty as after_window_total
order by
    warehouse.name asc nulls first,
    item_code asc nulls first
limit 100;`

  ```text
  …
      * CHART
          * UNIQUE
          * PARAMETER
          * SHORTHAND_MODIFIER
          * COPY
          * WITH
          * SELF_IMPORT
          * DATASOURCE_ROOT
          * SELECT
          * "merge"i
          * $END
          * _PROPERTIES
          * SHOW
          * MOCK
          * AUTO
          * TYPE
          * DEF
          * DATASOURCE_PARTIAL
          * CREATE
          * WHERE
          * VALIDATE
          * PARAM
          * IMPORT
          * PARSE_COMMENT
          * RAW_SQL
          * ROWSET
          * DATASOURCE
          * PROPERTY
          * PURPOSE
  Previous tokens: [Token('PARSE_COMMENT', '# Reference date\n')]

  Location:
   warehouse;  # Reference date ??? let ref_date = '2000-03-11'::d...

  Write stats: received 925 chars / 925 bytes; tail: …'me asc nulls first,\\n
  item_code asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query21.preql --content import raw.inventory as inventory;

# Reference date: 2000-03-11
# Before window: [2000-02-10, 2000-03-11)  i.e. d…ore_window_total,
    after_qty as after_window_total
order by
    inventory.warehouse.name asc nulls first,
    item_code asc nulls first
limit 100;`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('__ANON_19', '1.49')]

  Location:
  ..._price between 0.99 and 1.49  ??? auto before_qty <- sum(invento...

  Write stats: received 927 chars / 927 bytes; tail: …'me asc nulls first,\\n
  item_code asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…e pair count per item
auto item_date_sale_count <- count_distinct(store_sales.date.date ? store_sales.date.year between 2000 and 2003) by (item.id);
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 34, column 32.
  Expected one of:
          * _TERMINATOR

  Location:
  ...uto frequent_items <- item.id ??? where count_distinct(store_sal...

  Write stats: received 2159 chars / 2159 bytes; tail: …' store_sales.date.year
  between 2000 and 2003) by (item.id);\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query30.preql --content import raw.web_sales as web_sales;
import raw.customer as customer;

# Per-customer total web-return amount in 200…_review_date,
    cust_total_return
order by 1 asc, 2 asc, 3 asc, 4 asc, 5 asc, 6 asc, 7 asc, 8 asc, 9 asc, 10 asc, 11 asc, 12 asc, 13 asc
limit 100;`

  ```text
  refused to write 'query30.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'asc') at line 29, column 12.
  Expected one of:
          * MERGE
          * LIMIT
          * _TERMINATOR

  Location:
   cust_total_return order by 1 ??? asc, 2 asc, 3 asc, 4 asc, 5 as...

  Write stats: received 1399 chars / 1399 bytes; tail: …'asc, 8 asc, 9 asc, 10
  asc, 11 asc, 12 asc, 13 asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql --content import raw.physical_sales as physical_sales;

where physical_sales.date.year = 2001
and physical_sales.store.state… desc nulls first,
    case when hierarchy_level = 0 then physical_sales.item.category end nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 18, column 10.
  Expected one of:
          * MERGE
          * LIMIT
          * _TERMINATOR

  Location:
  ...el desc nulls first,     case ??? when hierarchy_level = 0 then

  Write stats: received 1083 chars / 1083 bytes; tail: …'d nulls first,\\n
  within_parent_rank nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql --content import raw.physical_sales as physical_sales;

where physical_sales.date.year = 2001
and physical_sales.store.state…first,
    case when hierarchy_level = 0 then physical_sales.item.category end nulls first,
    g_cat nulls first,
    g_class nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 15, column 10.
  Expected one of:
          * _TERMINATOR
          * LIMIT
          * MERGE

  Location:
  ...el desc nulls first,     case ??? when hierarchy_level = 0 then

  Write stats: received 618 chars / 618 bytes; tail: …'t,\\n    g_cat nulls
  first,\\n    g_class nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql --content import raw.physical_sales as physical_sales;

where physical_sales.date.year = 2001
and physical_sales.store.state…y_level
order by
    hierarchy_level desc,
    case when hierarchy_level = 0 then physical_sales.item.category end,
    g_cat,
    g_class
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 15, column 10.
  Expected one of:
          * _TERMINATOR
          * LIMIT
          * MERGE

  Location:
  ...ierarchy_level desc,     case ??? when hierarchy_level = 0 then

  Write stats: received 570 chars / 570 bytes; tail: …'l_sales.item.category
  end,\\n    g_cat,\\n    g_class\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql --content import raw.physical_sales as physical_sales;

where physical_sales.date.year = 2001
and physical_sales.store.state…nt_rank
order by
    hierarchy_level desc,
    case when hierarchy_level = 0 then physical_sales.item.category end,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 16, column 10.
  Expected one of:
          * MERGE
          * LIMIT
          * _TERMINATOR

  Location:
  ...ierarchy_level desc,     case ??? when hierarchy_level = 0 then

  Write stats: received 735 chars / 735 bytes; tail: …'l_sales.item.category
  end,\\n    within_parent_rank\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql --content import raw.physical_sales as physical_sales;

where physical_sales.date.year = 2001
and physical_sales.store.state…ng(physical_sales.item.class) as g_class,
    g_cat + g_class as hierarchy_level
order by
    hierarchy_level desc,
    g_cat,
    g_class
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...cal_sales.store.state = 'TN'  ??? auto net_profit_s <- sum(physi...

  Write stats: received 748 chars / 748 bytes; tail: …'\\n    hierarchy_level
  desc,\\n    g_cat,\\n    g_class\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql --content import raw.physical_sales as physical_sales;

where physical_sales.date.year = 2001
and physical_sales.store.state…ng(physical_sales.item.class) as g_class,
    g_cat + g_class as hierarchy_level
order by
    hierarchy_level desc,
    g_cat,
    g_class
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...cal_sales.store.state = 'TN'  ??? auto net_profit_s <- sum(physi...

  Write stats: received 748 chars / 748 bytes; tail: …'\\n    hierarchy_level
  desc,\\n    g_cat,\\n    g_class\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.item:item select item.manufacturer_id as mfr_id, count(item.id) as cnt group by mfr_id order by mfr_id limit 20;`

  ```text
  --> 2:62
    |
  2 | select item.manufacturer_id as mfr_id, count(item.id) as cnt group by
  mfr_id order by mfr_id limit 20;
    |                                                              ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...mfr_id, count(item.id) as cnt ??? group by mfr_id order by mfr_i...
  ```
- `trilogy file write query38.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…s.billing_customer.first_name,
    web_sales.date.date
) where web_sales.date.year = 2000;

select
    count_distinct(store_combos) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 18, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
  ...,     store_sales.date.date ) ??? where store_sales.date.year =

  Write stats: received 1179 chars / 1179 bytes; tail: …';\\n\\nselect\\n
  count_distinct(store_combos) as cnt\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query39.preql --content import raw.inventory as inventory;
import raw.date as date;
import raw.item as item;
import raw.warehouse as wareh…c nulls first,
    jan_cv asc nulls first,
    feb_month_of_year asc nulls first,
    feb_mean asc nulls first,
    feb_cv asc nulls first
limit 100;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 16, column 1.
  Expected one of:
          * SELECT

  Location:
  ...nuary and February separately ??? auto jan_mean <- avg(inventory...

  Write stats: received 1766 chars / 1766 bytes; tail: …'_mean asc nulls
  first,\\n    feb_cv asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query39.preql --content import raw.inventory as inventory;
import raw.date as date;
import raw.item as item;
import raw.warehouse as wareh…c nulls first,
    jan_cv asc nulls first,
    feb_month_of_year asc nulls first,
    feb_mean asc nulls first,
    feb_cv asc nulls first
limit 100;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 10, column 1.
  Expected one of:
          * SELECT

  Location:
  ...rehouse, item, month_of_year) ??? auto inv_mean <- avg(inventory...

  Write stats: received 1842 chars / 1842 bytes; tail: …'_mean asc nulls
  first,\\n    feb_cv asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query39.preql --content import raw.inventory as inventory;
import raw.date as date;
import raw.item as item;
import raw.warehouse as wareh…c nulls first,
    jan_cv asc nulls first,
    feb_month_of_year asc nulls first,
    feb_mean asc nulls first,
    feb_cv asc nulls first
limit 100;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 10, column 1.
  Expected one of:
          * SELECT

  Location:
  ...rehouse, item, month_of_year) ??? auto inv_mean <- avg(inventory...

  Write stats: received 1830 chars / 1830 bytes; tail: …'_mean asc nulls
  first,\\n    feb_cv asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.inventory:inventory --import raw.date:date --import raw.item:item --import raw.warehouse:warehouse select inventory.warehouse.id as …c nulls first, jan_mean asc nulls first, jan_cv asc nulls first, feb_moy asc nulls first, feb_mean asc nulls first, feb_cv asc nulls first limit 100;`

  ```text
  …
  ate.date) = 2) /
  nullif(avg(inventory.quantity_on_hand ? month(inventory.date.date) = 2), 0) as
  feb_cv where inventory.date.year = 2001 having
  stddev(inventory.quantity_on_hand) / nullif(avg(inventory.quantity_on_hand), 0)
  by inventory.warehouse.id, inventory.item.id, month(inventory.date.date) > 1
  order by w_id asc nulls first, i_id asc nulls first, jan_moy asc nulls first,
  jan_mean asc nulls first, jan_cv asc nulls first, feb_moy asc nulls first,
  feb_mean asc nulls first, feb_cv asc nulls first limit 100;
    |
  ^---
    |
    = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ventory.quantity_on_hand), 0) ??? by inventory.warehouse.id, inv...
  ```
- `trilogy file write query39.preql --content import raw.inventory as inventory;
import raw.date as date;
import raw.item as item;
import raw.warehouse as wareh…  warehouse asc nulls first,
    item asc nulls first,
    month_of_year asc nulls first,
    mean asc nulls first,
    cv asc nulls first
limit 200;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 9, column 94.
  Expected one of:
          * _TERMINATOR

  Location:
  ...arehouse.id, inventory.item.id ??? , (month(inventory.date.date))...

  Write stats: received 1044 chars / 1044 bytes; tail: …'\\n    mean asc nulls
  first,\\n    cv asc nulls first\\nlimit 200;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query39.preql --content import raw.inventory as inventory;
import raw.date as date;
import raw.item as item;
import raw.warehouse as wareh…  warehouse asc nulls first,
    item asc nulls first,
    month_of_year asc nulls first,
    mean asc nulls first,
    cv asc nulls first
limit 200;`

  ```text
  …
  nv_cv <- case when
  inv_mean = 0 then null else inv_stddev / inv_mean end;\n\nwhere
  inventory.date.year = 2001\n\nselect\n    inventory.warehouse.id as
  warehouse,\n    inventory.item.id as item,\n    month(inventory.date.date) as
  month_of_year,\n    inv_mean as mean,\n    inv_cv as cv\nhaving\n    inv_cv >
  1\norder by\n    warehouse asc nulls first,\n    item asc nulls first,\n
  month_of_year asc nulls first,\n    mean asc nulls first,\n    cv asc nulls
  first\nlimit 200;') at line 9, column 101.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('__ANON_11', ', month')]

  Location:
  ...e.id, inventory.item.id, month ??? (inventory.date.date); auto in...

  Write stats: received 1040 chars / 1040 bytes; tail: …'\\n    mean asc nulls
  first,\\n    cv asc nulls first\\nlimit 200;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# First: find the manufacturer text values (manufact) shared by at least one other item …
select item.product_name
where item.manufacturer_id between 738 and 778
  and item.manufact in shared_manufact
order by item.product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 18, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
  ...to profile_match <- item.id   ??? where item.manufacturer_id bet...

  Write stats: received 2646 chars / 2646 bytes; tail: …'act in
  shared_manufact\\norder by item.product_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# First: find the manufacturer text values (manufact) shared by at least one other item …
select item.product_name
where item.manufacturer_id between 738 and 778
  and item.manufact in shared_manufact
order by item.product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 18, column 3.
  Expected one of:
          * RPAR

  Location:
  ...file_match <- count(item.id   ??? where item.manufacturer_id bet...

  Write stats: received 2465 chars / 2465 bytes; tail: …'act in
  shared_manufact\\norder by item.product_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any profile, with manufacturer_id in range
auto profile_match <- count(…
select item.product_name
where item.manufacturer_id between 738 and 778
  and item.manufact in shared_manufact
order by item.product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 18, column 39.
  Expected one of:
          * _TERMINATOR

  Location:
  ...red_manufact <- item.manufact ??? where profile_match >= 2;  # F...

  Write stats: received 1720 chars / 1720 bytes; tail: …'act in
  shared_manufact\\norder by item.product_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Count profile-matching items per manufacturer text field (manufact)
# Items matching a…
select item.product_name
where item.manufacturer_id between 738 and 778
  and item.manufact in shared_manufact
order by item.product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 19, column 39.
  Expected one of:
          * _TERMINATOR

  Location:
  ...red_manufact <- item.manufact ??? where count(item.id ? profile_...

  Write stats: received 1778 chars / 1778 bytes; tail: …'act in
  shared_manufact\\norder by item.product_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.item:item select item.manufact, count(item.id) as cnt where item.manufacturer_id between 738 and 778 group by item.manufact having cnt >= 2 order by cnt desc limit 20;`

  ```text
  --> 2:92
    |
  2 | select item.manufact, count(item.id) as cnt where item.manufacturer_id
  between 738 and 778 group by item.manufact having cnt >= 2 order by cnt desc
  limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...cturer_id between 738 and 778 ??? group by item.manufact having
  ```
- `trilogy run --import raw.item:item select item.category, item.color, item.units, item.size, item.manufact, count(item.id) as cnt where item.manufacturer_id between 738 and 778 group by item.category, item.color, item.units, item.size, item.manufact order by cnt desc limit 20;`

  ```text
  --> 2:142
    |
  2 | select item.category, item.color, item.units, item.size, item.manufact,
  count(item.id) as cnt where item.manufacturer_id between 738 and 778 group by
  item.category, item.color, item.units, item.size, item.manufact order by cnt
  desc limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...cturer_id between 738 and 778 ??? group by item.category, item.c...
  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.item.manager_id, sum(store_sales.ext_sales_price) as total where store_sales.date.year = 2000 and store_sales.date.month_of_year = 11 group by store_sales.item.manager_id order by store_sales.item.manager_id asc limit 100;`

  ```text
  --> 2:154
    |
  2 | select store_sales.item.manager_id, sum(store_sales.ext_sales_price) as
  total where store_sales.date.year = 2000 and store_sales.date.month_of_year =
  11 group by store_sales.item.manager_id order by store_sales.item.manager_id
  asc limit 100;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...sales.date.month_of_year = 11 ??? group by store_sales.item.mana...
  ```
- `trilogy run --import raw.physical_sales:store_sales select count(distinct store_sales.item.category_id) as num_cats where store_sales.date.year = 2000 and store_sales.date.month_of_year = 11 and store_sales.item.manager_id = 1;`

  ```text
  --> 2:23
    |
  2 | select count(distinct store_sales.item.category_id) as num_cats where
  store_sales.date.year = 2000 and store_sales.date.month_of_year = 11 and
  store_sales.item.manager_id = 1;
    |                       ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._sales; select count(distinct ??? store_sales.item.category_id)
  ```
- `trilogy run --import raw.physical_sales:physical_sales select physical_sales.store.name, physical_sales.store.text_id, sum(physical_sales.sales_price ? physi… physical_sales.store.gmt_offset = -5 and physical_sales.date.year = 2000 group by physical_sales.store.name, physical_sales.store.text_id limit 100;`

  ```text
  --> 2:228
    |
  2 | select physical_sales.store.name, physical_sales.store.text_id,
  sum(physical_sales.sales_price ? physical_sales.date.day_name = 'Sunday') as
  sunday where physical_sales.store.gmt_offset = -5 and physical_sales.date.year
  = 2000 group by physical_sales.store.name, physical_sales.store.text_id limit
  100;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ysical_sales.date.year = 2000 ??? group by physical_sales.store....
  ```
- `trilogy run --import raw/physical_sales:ss select ss.item.product_name, avg(ss.net_profit) as avg_profit where ss.store.id = 1 group by ss.item.product_name limit 5;`

  ```text
  --> 2:85
    |
  2 | select ss.item.product_name, avg(ss.net_profit) as avg_profit where
  ss.store.id = 1 group by ss.item.product_name limit 5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._profit where ss.store.id = 1 ??? group by ss.item.product_name
  ```
- `trilogy run --import raw/physical_sales:ss select ss.item.id, avg(ss.net_profit) as avg_profit where ss.store.id = 1 group by ss.item.id limit 5;`

  ```text
  --> 2:75
    |
  2 | select ss.item.id, avg(ss.net_profit) as avg_profit where ss.store.id = 1
  group by ss.item.id limit 5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._profit where ss.store.id = 1 ??? group by ss.item.id limit 5;
  ```
- `trilogy run --import raw/physical_sales:ss select ss.store.id, count(distinct ss.item.id) as cnt where ss.store.id = 4 limit 5;`

  ```text
  --> 2:36
    |
  2 | select ss.store.id, count(distinct ss.item.id) as cnt where ss.store.id = 4
  limit 5;
    |                                    ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t ss.store.id, count(distinct ??? ss.item.id) as cnt where ss.st...
  ```
- `trilogy file write query45.preql`

  ```text
  refused to write 'query45.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 21, column 1.
  Expected one of:
          * SELECT

  Location:
  ...the item group identifier.  ??? auto is_zip_match <-    subst...

  Write stats: received 1766 chars / 1766 bytes; tail: …'\\n
  web_sales.billing_customer.address.city asc\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query47.preql`

  ```text
  refused to write 'query47.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
  ...vg_monthly_sales > 0.1 order ??? by     this_month_total - avg...

  Write stats: received 2218 chars / 2218 bytes; tail: …'\r\\n
  prior_month_total,\r\\n    next_month_total\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query47.preql`

  ```text
  refused to write 'query47.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
  ...vg_monthly_sales > 0.1 order ??? by     this_month_total - avg...

  Write stats: received 2131 chars / 2131 bytes; tail: …'\r\\n
  prior_month_total,\r\\n    next_month_total\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query49.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;
import raw.physical_sales as store_s…return_amount) / sum(web_sales.net_paid) as return_currency_ratio
order by channel, return_qty_ratio asc, return_currency_ratio asc, item
limit 100;
`

  ```text
  refused to write 'query49.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 15, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
   web_sales.net_profit > 0     ??? and web_sales.net_paid > 0

  Write stats: received 1102 chars / 1102 bytes; tail: …'n_qty_ratio asc,
  return_currency_ratio asc, item\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query51.preql --content import raw.physical_sales as store;
import raw.web_sales as web;
import raw.date as date;

merge store.date.id int…_total,
    c_store_max as store_running_max,
    c_web_max as web_running_max
order by item_id asc nulls first, sale_date asc nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 15, column 1.
  Expected one of:
          * SELECT

  Location:
  ...tals (cumulative across days) ??? auto store_rt <- sum(store_dai...

  Write stats: received 1644 chars / 1644 bytes; tail: …'tem_id asc nulls first,
  sale_date asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query51.preql --content import raw.physical_sales as store;
import raw.web_sales as web;
import raw.date as date;

merge store.date.id int…_total,
    c_store_max as store_running_max,
    c_web_max as web_running_max
order by item_id asc nulls first, sale_date asc nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'store_rt') at line 15, column 1.
  Expected one of:
          * SELECT

  Location:
  ...tals (cumulative across days) ??? store_rt <- sum(store_daily) o...

  Write stats: received 1491 chars / 1491 bytes; tail: …'tem_id asc nulls first,
  sale_date asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query53.preql --content import raw.physical_sales as store_sales;

# Restrict to year 2000 and the two profile groups
where store_sales.da…tal - avg_qtr_sales) / avg_qtr_sales > 0.1
order by
    avg_qtr_sales asc,
    mfg_qtr_total asc,
    store_sales.item.manufacturer_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 16, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...er (manufacturer_id, quarter) ??? auto mfg_qtr_total <- sum(stor...

  Write stats: received 1340 chars / 1340 bytes; tail: …'tal asc,\\n
  store_sales.item.manufacturer_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query53.preql --content import raw.physical_sales as store_sales;

where store_sales.date.year = 2000
  and (
    (store_sales.item.catego…tal - avg_qtr_sales) / avg_qtr_sales > 0.1
order by
    avg_qtr_sales asc,
    mfg_qtr_total asc,
    store_sales.item.manufacturer_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 12, column 4.
  Expected one of:
          * SELECT

  Location:
  ...o #1', 'importoamalg #1'))   ) ??? ;  auto mfg_qtr_total <- sum(s...

  Write stats: received 1174 chars / 1174 bytes; tail: …'tal asc,\\n
  store_sales.item.manufacturer_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query53.preql --content import raw.physical_sales as store_sales;

auto profile_filtered <- 
  store_sales.date.year = 2000
  and (
    (s…tal - avg_qtr_sales) / avg_qtr_sales > 0.1
order by
    avg_qtr_sales asc,
    mfg_qtr_total asc,
    store_sales.item.manufacturer_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 5, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
  ...tore_sales.date.year = 2000   ??? and (     (store_sales.item.ca...

  Write stats: received 1215 chars / 1215 bytes; tail: …'tal asc,\\n
  store_sales.item.manufacturer_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query53.preql --content import raw.physical_sales as store_sales;

auto year_2000 <- store_sales.date.year = 2000;
auto profile_a <- store…tal - avg_qtr_sales) / avg_qtr_sales > 0.1
order by
    avg_qtr_sales asc,
    mfg_qtr_total asc,
    store_sales.item.manufacturer_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 5, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
   'Children', 'Electronics')   ??? and store_sales.item.class in

  Write stats: received 1265 chars / 1265 bytes; tail: …'tal asc,\\n
  store_sales.item.manufacturer_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query53.preql --content import raw.physical_sales as store_sales;

auto profile_filtered <- store_sales.item.category in ('Books', 'Childr…tal - avg_qtr_sales) / avg_qtr_sales > 0.1
order by
    avg_qtr_sales asc,
    mfg_qtr_total asc,
    store_sales.item.manufacturer_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 3, column 92.
  Expected one of:
          * _TERMINATOR

  Location:
  ...', 'Children', 'Electronics') ??? and store_sales.item.class in

  Write stats: received 1168 chars / 1168 bytes; tail: …'tal asc,\\n
  store_sales.item.manufacturer_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
import raw.physical_sales as physica… customer_count,
  segment * 50 as segment_times_50
order by segment asc nulls first, customer_count asc nulls first, segment_times_50 asc
limit 100;`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union ') at line 17, column 3.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...m.class ilike '%maternity%'   ??? union web_sales.billing_custom...

  Write stats: received 1872 chars / 1872 bytes; tail: …'tomer_count asc nulls
  first, segment_times_50 asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run -q --import raw.item:item duck_db select color, id, class_id limit 5;`

  ```text
  --> 2:1
    |
  2 | -q;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  import raw.item as item; ??? -q;
  ```
- `trilogy run -q --import raw.item:item --import raw.catalog_sales:catalog_sales duck_db select item.class_id, item.id limit 5;`

  ```text
  --> 3:1
    |
  3 | -q;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...talog_sales as catalog_sales; ??? -q;
  ```
- `trilogy file write query57.preql --content import catalog_sales as cs;

# Step 1: monthly price totals per (category, brand, call_center, month_seq, year, mo… nulls first,
    cs.item.category,
    cs.item.brand_name,
    cs.call_center.name,
    cs.sold_date.year,
    cs.sold_date.month_of_year
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 13, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
   year), average monthly total ??? auto avg_monthly_by_year <- av...

  Write stats: received 2311 chars / 2311 bytes; tail: …'cs.sold_date.year,\\n
  cs.sold_date.month_of_year\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query59.preql --content import raw.physical_sales as store_sales;

# Step 1: For each (store, calendar week), sum sales_price split into 7…te.week_seq, store_sales.date.day_of_week;

select
    store_sales.store.name,
    store_sales.store.text_id,
    store_sales.date.week_seq,
    ...
`

  ```text
  …
  TYPE_SQL_NAVIGATION
          * _MAP_KEYS
          * BOOL
          * _ARRAY_FILTER
          * MULTIPLY
          * FALSE
          * WHERE
          * _REPLACE
          * _ARRAY_SORT
          * /(group)\s+(*)/i
          * _WEEK
          * LEAST
          * LPAR
          * _HEX
          * _YEAR
          * DIVIDE
          * _REGEXP_CONTAINS
          * _GEO_X
          * _SUM
          * _MAP_VALUES
          * _TIMESTAMP
          * "@"
          * NULLIF
          * _FORMAT_TIME
          * _ILIKE
          * CURRENT_TIMESTAMP
          * _REGEXP_EXTRACT
          * _STRUCT
          * _SUBSTRING
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...tore_sales.date.week_seq,     ??? ...

  Write stats: received 859 chars / 859 bytes; tail: …'sales.store.text_id,\\n
  store_sales.date.week_seq,\\n    ...\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query60.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…ct
    item.text_id as item_code,
    store_total + catalog_total + web_total as combined_total
order by item_code asc, combined_total asc
limit 100;`

  ```text
  refused to write 'query60.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 17, column 26.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._ids ) by store_sales.item.id ??? as store_by_item;  # September...

  Write stats: received 1934 chars / 1934 bytes; tail: …'_total\\norder by
  item_code asc, combined_total asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query60.preql`

  ```text
  refused to write 'query60.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'from ') at line 6, column 1.
  Expected one of:
          * MERGE
          * COMMA
          * WHERE
          * METADATA
          * LIMIT
          * _TERMINATOR
          * ORDER
          * HAVING
  Previous tokens: [Token('IDENTIFIER', 'item_code')]

  Location:
     item.text_id as item_code ??? from sales order by item_code...

  Write stats: received 141 chars / 141 bytes; tail: …'_id as item_code\r\\nfrom
  sales\r\\norder by item_code\r\\nlimit 5;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/physical_sales.preql:store_sales select store_sales.item.brand_name where lower(store_sales.item.brand_name) like '%scholaramalg%' group by store_sales.item.brand_name limit 20;`

  ```text
  --> 2:99
    |
  2 | select store_sales.item.brand_name where lower(store_sales.item.brand_name)
  like '%scholaramalg%' group by store_sales.item.brand_name limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or
  MULTIPLY_DIVIDE_PERCENT
  Location:
  ...d_name) like '%scholaramalg%' ??? group by store_sales.item.bran...
  ```
- `trilogy run --import raw/customer:customer insert into raw.customer as c; select c.id, c.demographics.marital_status, c.address.street_number, c.address.street_name, c.address.city, c.address.zip limit 5;`

  ```text
  --> 2:1
    |
  2 | insert into raw.customer as c; select c.id, c.demographics.marital_status,
  c.address.street_number, c.address.street_name, c.address.city, c.address.zip
  limit 5;
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...ort raw.customer as customer; ??? insert into raw.customer as c;...
  ```
- `trilogy file write query66.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Define auto fields for catalog monthly aggreg…10_npq as net_tot_m10,
    catalog.m11_npq as net_tot_m11,
    catalog.m12_npq as net_tot_m12
order by catalog.warehouse.name nulls first
limit 100;
`

  ```text
  …
  OPY
          * SELECT
          * PARAM
          * TYPE
          * DATASOURCE
          * PARAMETER
          * "merge"i
          * PERSIST_MODE
          * WHERE
          * AUTO
          * CREATE
          * ROWSET
          * DEF
          * IMPORT
          * VALIDATE
          * SELF_IMPORT
          * _DEF_TABLE
          * WITH
          * DATASOURCE_PARTIAL
          * MOCK
          * DATASOURCE_ROOT
          * _PROPERTIES
          * PUBLISH_ACTION
          * PARSE_COMMENT
          * SHORTHAND_MODIFIER
          * SHOW
          * $END
          * FROM
          * RAW_SQL
  Previous tokens: [Token('PARSE_COMMENT', '# month 1\n')]

  Location:
   monthly aggregates # month 1 ??? catalog.m1_esq <- sum(catalog....

  Write stats: received 6314 chars / 6314 bytes; tail: …'_m12\\norder by
  catalog.warehouse.name nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query67.preql --content import raw.physical_sales as store_sales;

# Compute summed sales: coalesce(sales_price,0) * coalesce(quantity,0)
…first,
    store_sales.date.month_of_year asc nulls first,
    store_sales.store.text_id asc nulls first,
    summed_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'as ') at line 8, column 17.
  Expected one of:
          * LPAR
  Previous tokens: [Token('IDENTIFIER', 'rollup_keys')]

  Location:
  ...ultiple times def rollup_keys ??? as rollup     store_sales.item...

  Write stats: received 1481 chars / 1481 bytes; tail: …'asc nulls first,\\n
  summed_sales asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query70_check4.preql --content import raw.physical_sales as store_sales;

# Check distinct states from store_sales for year 2000
select
  … sum(store_sales.net_profit) as total_profit
where store_sales.date.year = 2000
group by store_sales.store.state
order by total_profit desc
limit 20;`

  ```text
  refused to write 'query70_check4.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_17', 'group by') at line 8, column 1.
  Expected one of:
          * LIMIT
          * HAVING
          * MERGE
          * _TERMINATOR
          * ORDER

  Location:
   store_sales.date.year = 2000 ??? group by store_sales.store.sta...

  Write stats: received 286 chars / 286 bytes; tail:
  …'store_sales.store.state\\norder by total_profit desc\\nlimit 20;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query74.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Net paid from store sales, year 20…ling_customer.first_name as first_name,
    store_sales.billing_customer.last_name as last_name
order by
    customer_code asc nulls first
limit 100;`

  ```text
  refused to write 'query74.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('MERGE', 'merge') at line 20, column 1.
  Expected one of:
          * SELECT

  Location:
  ...store and web sales customers ??? merge raw.customer.id into ~st...

  Write stats: received 1361 chars / 1361 bytes; tail: …'t_name\\norder by\\n
  customer_code asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.sales_channel, sum(all_sales.ext_sales_price) as sales group by rollup all_sales.sales_channel;`

  ```text
  --> 2:73
    |
  2 | select all_sales.sales_channel, sum(all_sales.ext_sales_price) as sales
  group by rollup all_sales.sales_channel;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...les.ext_sales_price) as sales ??? group by rollup all_sales.sale...
  ```
- `trilogy run --import raw.all_sales:all_sales --all-rows --displayed-rows 100 select all_sales.sales_channel as channel, all_sales.channel_dim_id as outlet, s…l_sales.date.date between '2000-08-23'::date and '2000-09-22'::date order by channel asc nulls first, outlet asc nulls first, returns desc limit 100;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...ales.channel_dim_id as profit ??? from profit - sum(all_sales.re...
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

# Sales amount: use ext_sales_price (line-extended sales price).
# Returns amo…ng(out_ident) by rollup all_sales.sales_channel, out_ident as g_outlet
order by
    channel nulls first,
    outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 15, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 1426 chars / 1426 bytes; tail: …'el nulls first,\\n
  outlet_identifier nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

auto out_ident <- case(
    all_sales.sales_channel = 'STORE',
    concat('sto…ng(out_ident) by rollup all_sales.sales_channel, out_ident as g_outlet
order by
    channel nulls first,
    outlet_identifier nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 10, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 1177 chars / 1177 bytes; tail: …'el nulls first,\\n
  outlet_identifier nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

auto out_ident <- case(
    all_sales.sales_channel = 'STORE',
    concat('sto…  grouping(out_ident) by rollup all_sales.sales_channel, out_ident as g_outlet
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 10, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 1169 chars / 1169 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

auto out_ident <- case(
    all_sales.sales_channel = 'STORE',
    concat('sto…  grouping(out_ident) by rollup all_sales.sales_channel, out_ident as g_outlet
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 1132 chars / 1132 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

auto out_ident <- case(
    all_sales.sales_channel = 'STORE',
    concat('sto…  grouping(out_ident) by rollup all_sales.sales_channel, out_ident as g_outlet
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 1132 chars / 1132 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

auto out_ident <- case(
    all_sales.sales_channel = 'STORE',
    concat('sto…  grouping(out_ident) by rollup all_sales.sales_channel, out_ident as g_outlet
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 1132 chars / 1132 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.


  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

auto out_ident <- case(
    all_sales.sales_channel = 'STORE',
    concat('sto…   grouping(out_ident) by rollup all_sales.sales_channel, out_ident as g_outlet
order by channel nulls first, outlet_identifier nulls first limit 100`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 1131 chars / 1131 bytes; tail: …'channel nulls first,
  outlet_identifier nulls first limit 100'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

auto out_ident <- case(
    all_sales.sales_channel = 'STORE',
    concat('sto…les.return_net_loss, 0)) by rollup all_sales.sales_channel, out_ident as profit
order by channel nulls first, outlet_identifier nulls first limit 100`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 952 chars / 952 bytes; tail: …'channel nulls first,
  outlet_identifier nulls first limit 100'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  …nel_dim_text_id as outlet_raw,
    sum(all_sales.ext_sales_price) by rollup all_sales.sales_channel, all_sales.channel_dim_text_id as sales
limit 100`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a
  semicolon `;`.
  Location:
  ...dim_text_id as sales limit 100 ???

  Write stats: received 409 chars / 409 bytes; tail: …'es_channel,
  all_sales.channel_dim_text_id as sales\\nlimit 100'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

auto out_ident <- case(
    all_sales.sales_channel = 'STORE',
    concat('sto…es.return_net_loss, 0)) by rollup all_sales.sales_channel, out_ident as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ;  where all_sales.date.date b...

  Write stats: received 953 chars / 953 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

auto out_ident <- case(
    all_sales.sales_channel = 'STORE',
    concat('sto…es.return_net_loss, 0)) by rollup all_sales.sales_channel, out_ident as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 8, column 55.
  Expected one of:
          * WHEN

  Location:
  ...ll_sales.channel_dim_text_id)) ??? ;  where all_sales.date.date b...

  Write stats: received 952 chars / 952 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

auto outlet_identifier <- case(
    all_sales.sales_channel = 'STORE',
    con…n_net_loss, 0)) by rollup all_sales.sales_channel, outlet_identifier as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 8, column 55.
  Expected one of:
          * WHEN

  Location:
  ...ll_sales.channel_dim_text_id)) ??? ;  where all_sales.date.date b...

  Write stats: received 971 chars / 971 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  …'CATALOG','catalog_page','web_site'),all_sales.channel_dim_text_id)) as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 14, column 24.
  Expected one of:
          * WHEN

  Location:
  ...page',             'web_site') ??? ,         all_sales.channel_di...

  Write stats: received 1309 chars / 1309 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  …'CATALOG','catalog_page','web_site'),all_sales.channel_dim_text_id)) as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 8, column 117.
  Expected one of:
          * WHEN

  Location:
  ...OG','catalog_page','web_site') ??? , all_sales.channel_dim_text_i...

  Write stats: received 1223 chars / 1223 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  …'CATALOG','catalog_page','web_site'),all_sales.channel_dim_text_id)) as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 8, column 118.
  Expected one of:
          * WHEN

  Location:
  ...OG','catalog_page','web_site') ??? , all_sales.channel_dim_text_i...

  Write stats: received 1224 chars / 1224 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  … 'catalog_page' else 'web_site' end),all_sales.channel_dim_text_id)) as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('THEN', 'then') at line 8, column 49.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...l_sales.sales_channel='STORE' ??? then 'store' when all_sales.sa...

  Write stats: received 1319 chars / 1319 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  …'CATALOG','catalog_page','web_site'),all_sales.channel_dim_text_id)) as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 8, column 117.
  Expected one of:
          * WHEN

  Location:
  ...OG','catalog_page','web_site') ??? , all_sales.channel_dim_text_i...

  Write stats: received 1223 chars / 1223 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query83.preql --content import raw.all_sales as all_sales;

# Find the week_seq values for the three target dates
# 2000-06-30, 2000-09-27…re_return_qty + catalog_return_qty + web_return_qty) / 3 as three_channel_avg
order by item_code nulls first, store_return_qty nulls first
limit 100;`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...sales.return_date.week_seq in ??? (     select all_sales.return_...

  Write stats: received 2093 chars / 2093 bytes; tail: …'em_code nulls first,
  store_return_qty nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.return_date.week_seq, count(all_sales.item.id) as cnt where all_sales.return_date.week_seq in (5244, 5257, 5264) group by all_sales.return_date.week_seq;`

  ```text
  --> 2:131
    |
  2 | select all_sales.return_date.week_seq, count(all_sales.item.id) as cnt
  where all_sales.return_date.week_seq in (5244, 5257, 5264) group by
  all_sales.return_date.week_seq;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...eek_seq in (5244, 5257, 5264) ??? group by all_sales.return_date...
  ```
- `trilogy run --import raw.all_sales:all_sales select sum(all_sales.return_quantity ? all_sales.sales_channel = 'STORE' and all_sales.return_date.week_seq in (5244, 5257, 5264)) as store_qty by all_sales.item.id order by all_sales.item.id limit 20;`

  ```text
  --> 2:145
    |
  2 | select sum(all_sales.return_quantity ? all_sales.sales_channel = 'STORE'
  and all_sales.return_date.week_seq in (5244, 5257, 5264)) as store_qty by
  all_sales.item.id order by all_sales.item.id limit 20;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...44, 5257, 5264)) as store_qty ??? by all_sales.item.id order by
  ```
- `trilogy run --import raw.web_returns:wsr select wsr.web_sales.date.year, count(wsr.web_sales.order_number) as cnt group by year order by year limit 10;`

  ```text
  --> 2:74
    |
  2 | select wsr.web_sales.date.year, count(wsr.web_sales.order_number) as cnt
  group by year order by year limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...eb_sales.order_number) as cnt ??? group by year order by year li...
  ```
- `trilogy run --import raw.web_sales as web_sales select count(web_sales.order_number) by web_sales.bill_household_demographic.dependent_count;`

  ```text
  --> 2:93
    |
  2 | select count(web_sales.order_number) by
  web_sales.bill_household_demographic.dependent_count;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...ld_demographic.dependent_count ??? ;
  ```
- `trilogy file write query92.preql --content import raw.web_sales as web_sales;

auto web_sales.item.manufacturer_id;
auto web_sales.date.date;
auto web_sales.…iscount_by_item
select
  sum(web_sales.ext_discount_amount) as total_extended_discount_amount
order by total_extended_discount_amount desc
limit 100;`

  ```text
  refused to write 'query92.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [203]: Missing assignment operator '<-' and expression in derivation.
  Write `auto X <- <expression>;` (also valid: `metric`, `property`, `rowset`).
  Example: `auto orders_per_customer <- count(orders.id) by customer.id;`.
  Location:
  ...web_sales.item.manufacturer_id ??? ; auto web_sales.date.date; au...

  Write stats: received 605 chars / 605 bytes; tail: …'ount\\norder by
  total_extended_discount_amount desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query92.preql -e -c import raw.web_sales as web_sales;

where web_sales.item.manufacturer_id = 350
  and web_sales.date.date between '2000-01-27'::date and '2000-04-26'::date
select
  count(*) as cnt
limit 100;
`

  ```text
  …
          * _YEAR
          * _MINUTE
          * _BOOL_OR
          * _UNNEST
          * DBLQUOTE
          * CASE
          * _DATE_DIFF
          * _ANY
          * _WEEK
          * CONCAT
          * _STRPOS
          * _ARRAY_SORT
          * _RTRIM
          * _HOUR
          * /\-?[0-9]*\.[0-9]+/
          * QUOTE
          * DATETIME
          * _GEO_CENTROID
          * MOD
          * _GROUP
          * WINDOW_TYPE_LEGACY
          * BOOL
          * _DAY_NAME
          * _AVG
          * _GEO_TRANSFORM
          * INT_LITERAL_PART
          * _DAY_OF_WEEK
          * _HEX
          * _ILIKE
  Previous tokens: [Token('_COUNT', 'count(')]

  Location:
  ...0-04-26'::date select   count( ??? *) as cnt limit 100;

  Write stats: received 191 chars / 191 bytes; tail: …" and
  '2000-04-26'::date\\nselect\\n  count(*) as cnt\\nlimit 100;\\n".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/reason:r select r.* limit 10;`

  ```text
  --> 2:12
    |
  2 | select r.* limit 10;
    |            ^---
    |
    = expected access_chain
  Location:
  ...t raw.reason as r; select r.* ??? limit 10;
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# store sales: unique (custo…er.id as customer_id, store_sales.item.id as item_id where store_sales.date.year = 2000;
# ERROR: there's no store_sales.date directly - let me check`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'distinct ') at line 5, column 28.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...00 auto store_pairs <- select ??? distinct store_sales.billing_c...

  Write stats: received 358 chars / 358 bytes; tail: …"# ERROR: there's no
  store_sales.date directly - let me check".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run - duckdb --import raw/catalog_sales:catalog_sales select catalog_sales.days_to_ship, count(catalog_sales.order_number) as total_orders, count(cat…een 61 and 90) as b_61_90 where catalog_sales.ship_date.year = 2000 group by catalog_sales.days_to_ship order by catalog_sales.days_to_ship limit 20;`

  ```text
  --> 2:393
    |
  2 | select catalog_sales.days_to_ship, count(catalog_sales.order_number) as
  total_orders, count(catalog_sales.order_number ? catalog_sales.days_to_ship <=
  30) as le_30, count(catalog_sales.order_number ? catalog_sales.days_to_ship
  between 31 and 60) as b_31_60, count(catalog_sales.order_number ?
  catalog_sales.days_to_ship between 61 and 90) as b_61_90 where
  catalog_sales.ship_date.year = 2000 group by catalog_sales.days_to_ship order
  by catalog_sales.days_to_ship limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...g_sales.ship_date.year = 2000 ??? group by catalog_sales.days_to...
  ```

### `cli-misuse`

- `trilogy run - --import raw/physical_sales:store_sales select store_sales.return_store.state, store_sales.return_date.year limit 5;`

  ```text
  'select store_sales.return_store.state, store_sales.return_date.year limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/catalog_sales.precl --regex order_number|item\.id`

  ```text
  Invalid value for 'PATH': File 'raw/catalog_sales.precl' does not exist.
  ```
- `trilogy explore raw/physical_sales.preql --regex ticket_number net_profit sales_price wholesale_cost quantity ext_sales_price ext_wholesale_cost`

  ```text
  Got unexpected extra arguments (net_profit sales_price wholesale_cost quantity ext_sales_price ext_wholesale_cost)
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql --regex county --show concepts`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql --regex date|quarter|year --show concepts`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy read_file raw/catalog_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy run - --import raw.catalog_sales:catalog_sales select count(catalog_sales.item.id) as cnt;`

  ```text
  'select count(catalog_sales.item.id) as cnt;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql --regex net_profit --regex extended_sales_price --regex category --regex class --regex store`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy read_file raw/web_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/web_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql --regex item --regex address --regex date --regex gmtoffset --regex gmt_offset`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run -q -e --import raw.item:item select color, id, class_id limit 5;`

  ```text
  'raw.item:item' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy db list`

  ```text
  No such command 'db'.
  ```
- `trilogy explore raw/store_sales.preql --regex item --regex date --regex address --regex store --regex gmt --regex sep --regex 1998 --regex extended`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr merge cs.item.id into ~cr.sales.item.id; select cs.item.id as iid, sum(cs.ext_list_…coalesce(cr.refunded_cash,0) + coalesce(cr.reversed_charge,0) + coalesce(cr.store_credit,0)) as cum_refund_amt, count(cs.row_counter) as cnt limit 5;`

  ```text
  'select cs.item.id as iid, sum(cs.ext_list_price) as cum_ext_list_price, sum(coalesce(cr.refunded_cash,0) + coalesce(cr.reversed_charge,0) + coalesce(cr.store_credit,0)) as cum_refund_amt, count(cs.row_counter) as cnt limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql --regex store --regex date --regex net_profit`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/web_sales.prekl`

  ```text
  Invalid value for 'PATH': File 'raw/web_sales.prekl' does not exist.
  ```
- `trilogy explore raw/store_sales.preql --regex store_ref|store.s_ store_sales.store ss_store_sk year quarter`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/web_sales.preql --regex ship|customer|c_customer web_sales.customer ws_ship_customer year quarter`

  ```text
  Got unexpected extra arguments (web_sales.customer ws_ship_customer year quarter)
  ```
- `trilogy explore raw/catalog_sales.preql --regex ship|address|ca_address catalog_sales.customer_address cs_ship_addr year quarter`

  ```text
  Got unexpected extra arguments (catalog_sales.customer_address cs_ship_addr year quarter)
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy read_file raw/item.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/store_returns.preql --show all`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy read_file raw/physical_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/catalog_store_returns.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy run - --import raw/catalog_sales:catalog_sales select catalog_sales.warehouse.name, catalog_sales.ship_mode.type, catalog_sales.call_center.name, catalog_sales.days_to_ship, catalog_sales.order_number limit 5;`

  ```text
  'select catalog_sales.warehouse.name, catalog_sales.ship_mode.type, catalog_sales.call_center.name, catalog_sales.days_to_ship, catalog_sales.order_number limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `join-resolution`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Could not resolve connections for query with output
  ['store_sales.store.name<Purpose.PROPERTY>Derivation.ROOT>',
  'local.total_net_profit<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['customer.demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.marital_status<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.education_status<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.purchase_estimate<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.credit_rating<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.dependent_count<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.employed_dependent_count<Purpose.PROPERTY>Derivation.ROO
  T>',
  'customer.demographics.college_dependent_count<Purpose.PROPERTY>Derivation.ROOT
  >', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['customer.demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.marital_status<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.education_status<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.purchase_estimate<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.credit_rating<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.dependent_count<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.employed_dependent_count<Purpose.PROPERTY>Derivation.ROO
  T>',
  'customer.demographics.college_dependent_count<Purpose.PROPERTY>Derivation.ROOT
  >', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run --import raw.physical_sales:physical_sales --import raw.customer:customer select customer.demographics.gender, customer.demographics.marital_stat… cnt where customer.address.county in ('Rush County') and physical_sales.date.year = 2002 and physical_sales.date.month_of_year in (1,2,3,4) limit 5;`

  ```text
  Could not resolve connections for query with output
  ['customer.demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.demographics.marital_status<Purpose.PROPERTY>Derivation.ROOT>',
  'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.physical_sales:physical_sales --import raw.customer:customer select customer.demographics.gender, count(customer.id) as cnt where customer.address.county in ('Rush County') and physical_sales.date.year = 2002 and physical_sales.date.month_of_year in (1,2,3,4) limit 5;`

  ```text
  Could not resolve connections for query with output
  ['customer.demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.physical_sales:physical_sales --import raw.customer:customer select customer.demographics.gender, physical_sales.date.year, count(cu… cnt where customer.address.county in ('Rush County') and physical_sales.date.year = 2002 and physical_sales.date.month_of_year in (1,2,3,4) limit 5;`

  ```text
  Could not resolve connections for query with output
  ['customer.demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'physical_sales.date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.physical_sales:physical_sales --import raw.customer:customer select physical_sales.date.year, count(customer.id) as cnt where customer.address.county in ('Rush County') and physical_sales.date.year = 2002 and physical_sales.date.month_of_year in (1,2,3,4) limit 5;`

  ```text
  Could not resolve connections for query with output
  ['physical_sales.date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.physical_sales:physical_sales --import raw.customer:customer select physical_sales.date.year, count(customer.id) as cnt where physical_sales.date.year = 2002 and physical_sales.date.month_of_year in (1,2,3,4) limit 5;`

  ```text
  Could not resolve connections for query with output
  ['physical_sales.date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.physical_sales:physical_sales --import raw.customer:customer select physical_sales.billing_customer.id, count(customer.id) as cnt where physical_sales.date.year = 2002 and physical_sales.date.month_of_year in (1,2,3,4) limit 5;`

  ```text
  Could not resolve connections for query with output
  ['physical_sales.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query16.preql`

  ```text
  No datasource exists for root concept
  returns.had_return@Grain<returns.item.id,returns.order_number>, and no
  resolvable pseudonyms found from set(). This query is unresolvable from your
  environment. Check your datasources and imports to make sure this concept is
  bound.
  ```
- `trilogy run query21.preql`

  ```text
  Could not resolve connections for query with output
  ['warehouse.name<Purpose.PROPERTY>Derivation.ROOT>',
  'local.item_code<Purpose.KEY>Derivation.BASIC>',
  'local.before_window_total<Purpose.METRIC>Derivation.BASIC>',
  'local.after_window_total<Purpose.METRIC>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query22.preql`

  ```text
  Could not resolve connections for query with output
  ['item.product_name<Purpose.PROPERTY>Derivation.ROOT>',
  'item.brand_name<Purpose.PROPERTY>Derivation.ROOT>',
  'item.class<Purpose.PROPERTY>Derivation.ROOT>',
  'item.category<Purpose.PROPERTY>Derivation.ROOT>',
  'local.avg_qty_on_hand<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query25.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_description<Purpose.PROPERTY>Derivation.BASIC>',
  'local.store_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.store_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.store_sale_net_profit<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.store_return_net_loss<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.catalog_sale_net_profit<Purpose.METRIC>Derivation.AGGREGATE>'] from
  current model.
  ```
- `trilogy run query25.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_description<Purpose.PROPERTY>Derivation.BASIC>',
  'local.store_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.store_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.store_sale_net_profit<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.store_return_net_loss<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.catalog_sale_net_profit<Purpose.METRIC>Derivation.AGGREGATE>'] from
  current model.
  ```
- `trilogy run query25.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_description<Purpose.PROPERTY>Derivation.BASIC>',
  'local.store_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.store_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.store_sale_net_profit_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.store_return_net_loss_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.catalog_sale_net_profit_sum<Purpose.METRIC>Derivation.AGGREGATE>'] from
  current model.
  ```
- `trilogy run query25.preql --debug`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_description<Purpose.PROPERTY>Derivation.BASIC>',
  'local.store_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.store_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.store_sale_net_profit_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.store_return_net_loss_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.catalog_sale_net_profit_sum<Purpose.METRIC>Derivation.AGGREGATE>'] from
  current model.
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users
  …
  t model."
      )
  trilogy.core.exceptions.UnresolvableQueryException: Could not resolve
  connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_description<Purpose.PROPERTY>Derivation.BASIC>',
  'local.store_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.store_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.store_sale_net_profit_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.store_return_net_loss_sum<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.catalog_sale_net_profit_sum<Purpose.METRIC>Derivation.AGGREGATE>'] from
  current model.
  ```
- `trilogy run query37.preql duckdb`

  ```text
  Could not resolve connections for query with output
  ['item.text_id<Purpose.PROPERTY>Derivation.ROOT>',
  'item.desc<Purpose.PROPERTY>Derivation.ROOT>',
  'item.current_price<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw.catalog_sales:catalog_sales --import raw.item:item --import raw.inventory:inventory --import raw.date:date select item.text_id, item…ntory.quantity_on_hand between 100 and 500 and inventory.date.date between '2000-02-01'::date and '2000-04-01'::date order by item.text_id limit 100;`

  ```text
  Could not resolve connections for query with output
  ['item.text_id<Purpose.PROPERTY>Derivation.ROOT>',
  'item.desc<Purpose.PROPERTY>Derivation.ROOT>',
  'item.current_price<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run query39.preql`

  ```text
  Could not resolve connections for query with output
  ['local.warehouse<Purpose.KEY>Derivation.BASIC>',
  'local.item<Purpose.KEY>Derivation.BASIC>',
  'local.jan_month_of_year<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.jan_mean<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.jan_cv<Purpose.PROPERTY>Derivation.BASIC>',
  'local.feb_month_of_year<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.feb_mean<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.feb_cv<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query39.preql`

  ```text
  Could not resolve connections for query with output
  ['local.warehouse<Purpose.KEY>Derivation.BASIC>',
  'local.item<Purpose.KEY>Derivation.BASIC>',
  'local.jan_month_of_year<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.jan_mean<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.jan_cv<Purpose.PROPERTY>Derivation.BASIC>',
  'local.feb_month_of_year<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.feb_mean<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.feb_cv<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query39.preql`

  ```text
  Could not resolve connections for query with output
  ['local.warehouse<Purpose.KEY>Derivation.BASIC>',
  'local.item<Purpose.KEY>Derivation.BASIC>',
  'local.jan_month_of_year<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.jan_mean<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.jan_cv<Purpose.PROPERTY>Derivation.BASIC>',
  'local.feb_month_of_year<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.feb_mean<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.feb_cv<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.inventory:inventory --import raw.date:date --import raw.item:item --import raw.warehouse:warehouse select warehouse.id, item.id, 1 as jan_moy limit 5;`

  ```text
  Could not resolve connections for query with output
  ['warehouse.id<Purpose.KEY>Derivation.ROOT>',
  'item.id<Purpose.KEY>Derivation.ROOT>',
  'local.jan_moy<Purpose.CONSTANT>Derivation.CONSTANT>'] from current model.
  ```
- `trilogy run --import raw.inventory:inventory --import raw.date:date --import raw.item:item --import raw.warehouse:warehouse select inventory.warehouse.id, inventory.item.id, date.month_of_year as moy, avg(inventory.quantity_on_hand) as mean_val, stddev(inventory.quantity_on_hand) as stddev_val limit 5;`

  ```text
  Could not resolve connections for query with output
  ['inventory.warehouse.id<Purpose.KEY>Derivation.ROOT>',
  'inventory.item.id<Purpose.KEY>Derivation.ROOT>',
  'local.moy<Purpose.PROPERTY>Derivation.BASIC>',
  'local.mean_val<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.stddev_val<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.inventory:inventory --import raw.date:date --import raw.item:item --import raw.warehouse:warehouse select inventory.warehouse.id as …yr = 2001 and cv > 1 order by w_id asc nulls first, i_id asc nulls first, moy asc nulls first, mean_val asc nulls first, cv asc nulls first limit 10;`

  ```text
  Could not resolve connections for query with output
  ['local.w_id<Purpose.KEY>Derivation.BASIC>',
  'local.i_id<Purpose.KEY>Derivation.BASIC>',
  'local.yr<Purpose.PROPERTY>Derivation.BASIC>',
  'local.moy<Purpose.PROPERTY>Derivation.BASIC>',
  'local.mean_val<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.stddev_val<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cv<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.inventory:inventory --import raw.date:date --import raw.item:item --import raw.warehouse:warehouse select inventory.warehouse.id as …ear = 2001 having cv > 1 order by w_id asc nulls first, i_id asc nulls first, moy asc nulls first, mean asc nulls first, cv asc nulls first limit 10;`

  ```text
  Could not resolve connections for query with output
  ['local.w_id<Purpose.KEY>Derivation.BASIC>',
  'local.i_id<Purpose.KEY>Derivation.BASIC>',
  'local.moy<Purpose.PROPERTY>Derivation.BASIC>',
  'local.mean<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cv<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query40.preql`

  ```text
  Could not resolve connections for query with output
  ['cs.warehouse.state<Purpose.PROPERTY>Derivation.ROOT>',
  'local.item_code<Purpose.KEY>Derivation.BASIC>',
  'local.total_before<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_after<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query54.preql`

  ```text
  Could not resolve connections for query with output
  ['physical_sales.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'local.total_store_price<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query54.preql`

  ```text
  Could not resolve connections for query with output
  ['local.cust_id<Purpose.KEY>Derivation.BASIC>',
  'local.store_total_by_cust<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.segment<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query54.preql`

  ```text
  Could not resolve connections for query with output
  ['local.cust_id<Purpose.KEY>Derivation.BASIC>',
  'local.total_store_price<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query56.preql`

  ```text
  Could not resolve connections for query with output
  ['local.total_ext_sales<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query60.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.combined_total<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query60.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.combined_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query60.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.combined_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query60.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.combined_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query60.preql --debug`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.combined_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\scripts\parallel_executio
  n.py", line 578, in run_single_script_execution
      queries = exec.parse_text(
          text, root=base if isinstance(base, Path) else None
      )
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\executor.py", line
  700, in parse_text
      return list(self.pars
  …
  ines>...
          history=history,
          ^^^^^^^^^^^^^^^^
      )
      ^
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\core\processing\concept_s
  trategies_v3.py", line 517, in source_query_concepts
      raise UnresolvableQueryException(
          f"Could not resolve connections for query with output {error_strings}
  from current model."
      )
  trilogy.core.exceptions.UnresolvableQueryException: Could not resolve
  connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.combined_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query60.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.combined_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query60.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.test_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query72.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_description<Purpose.PROPERTY>Derivation.BASIC>',
  'local.warehouse_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.sold_week<Purpose.PROPERTY>Derivation.BASIC>',
  'local.no_promo_lines<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.promo_lines<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_lines<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query74.preql duckdb`

  ```text
  Could not resolve connections for query with output
  ['local.customer_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.item.id, all_sales.sales_channel, sum(all_sales.return_quantity) as ret_qty where all_sales.return_date.week_seq in (5244, 5257, 5264) order by all_sales.item.id, all_sales.sales_channel limit 20;`

  ```text
  Query is unresolvable: no complete sources found for output
  concepts {'all_sales.item.id'}. These concepts could only be resolved from
  partial sources.
  ```
- `trilogy run query84.preql`

  ```text
  Could not resolve connections for query with output
  ['local.customer_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.full_name<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query84.preql`

  ```text
  Could not resolve connections for query with output
  ['local.customer_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.full_name<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query84.preql --debug`

  ```text
  Could not resolve connections for query with output
  ['local.customer_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.full_name<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\scripts\parallel_executio
  n.py", line 578, in run_single_script_execution
      queries = exec.parse_text(
          text, root=base if isinstance(base, Path) else None
      )
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\executor.py", line
  700, in parse_text
      return list(self.p
  …
  s>...
          history=history,
          ^^^^^^^^^^^^^^^^
      )
      ^
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\core\processing\concept_s
  trategies_v3.py", line 517, in source_query_concepts
      raise UnresolvableQueryException(
          f"Could not resolve connections for query with output {error_strings}
  from current model."
      )
  trilogy.core.exceptions.UnresolvableQueryException: Could not resolve
  connections for query with output
  ['local.customer_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.full_name<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.web_returns:web_returns select web_sales.is_returned, web_sales.date.year, web_sales.sales_price, web_sales.net_profit, web_sales.quantity, web_returns.refunded_cash, web_returns.fee, web_returns.reason.desc limit 5;`

  ```text
  Could not resolve connections for query with output
  ['web_sales.is_returned<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.sales_price<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.net_profit<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'web_returns.refunded_cash<Purpose.PROPERTY>Derivation.ROOT>',
  'web_returns.fee<Purpose.PROPERTY>Derivation.ROOT>',
  'web_returns.reason.desc<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/physical_sales:store_sales select store_sales.return_store.state, store_sales.return_date.year, sum(store_sales.return_amount) by store_sales.return_customer.id, store_sales.return_store.id, store_sales.return_store.state, store_sales.return_date.year limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales.return_date.year ??? limit 10;
  ```
- `trilogy run --import raw.physical_sales:physical_sales --import raw.web_sales:web_sales --import raw.catalog_sales:catalog_sales --import raw.customer:customer select customer.demographics.gender, customer.demographics.marital_status, count(customer.id) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...al_status, count(customer.id) ??? limit 5;
  ```
- `trilogy run --import raw/catalog_store_returns:csr select csr.item.id, csr.store.id, csr.store.name, csr.item.desc, sum(csr.store_quantity), sum(csr.store_re…r >= 9 and csr.store_return_date.month_of_year <= 12 and csr.store_return_date.year = 1999 and csr.catalog_date.year in (1999, 2000, 2001) limit 100;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ....desc, sum(csr.store_quantity) ??? , sum(csr.store_return_quantit...
  ```
- `trilogy run --import raw.item:item select distinct item.manufacturer_id order by item.manufacturer_id limit 10; duck_db`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.manufacturer_id order by
  ```
- `trilogy run --import raw.catalog_sales:catalog_sales --import raw.inventory:inventory --import raw.date:date select count(inventory.quantity_on_hand) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...t(inventory.quantity_on_hand) ??? limit 10;
  ```
- `trilogy run --import raw.item:item select distinct item.manufacturer_id as mfr_id limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.manufacturer_id as mfr_id...
  ```
- `trilogy run --import raw.all_sales:sales select sales.sales_channel, count(sales.order_id) where year(sales.date.date)=2000 group by sales.sales_channel limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...hannel, count(sales.order_id) ??? where year(sales.date.date)=20...
  ```
- `trilogy file write query41.preql --content import raw.item as item; where item.manufacturer_id between 738 and 778 select distinct item.product_name order by item.product_name limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ct distinct item.product_name ??? order by item.product_name lim...

  Write stats: received 143 chars / 143 bytes; tail: …'inct item.product_name
  order by item.product_name limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.physical_sales:physical_sales select store.name, store.text_id, store.gmt_offset, count(1) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...d, store.gmt_offset, count(1) ??? limit 10;
  ```
- `trilogy run --import raw/physical_sales:ss select ss.store.id, count(ss.ticket_number), avg(ss.net_profit) as avg_profit where ss.store.id = 4 limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...re.id, count(ss.ticket_number) ??? , avg(ss.net_profit) as avg_pr...
  ```
- `trilogy run --import raw/physical_sales:ss select distinct ss.store.id limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ..._sales as ss; select distinct ??? ss.store.id limit 20;
  ```
- `trilogy run import raw.physical_sales as store_sales; select distinct store_sales.billing_customer.demographics.education_status limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? store_sales.billing_customer.d...
  ```
- `trilogy run --import raw.physical_sales:store_sales select distinct store_sales.billing_customer.demographics.education_status limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? store_sales.billing_customer.d...
  ```
- `trilogy run --import raw.physical_sales:store_sales select distinct store_sales.billing_customer.demographics.education_status limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? store_sales.billing_customer.d...
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.sold_date.year, count(catalog_sales.order_number) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...t(catalog_sales.order_number) ??? limit 5;
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.sales_channel, all_sales.item.id, sum(all_sales.ext_sales_price) where all_sales.date.week_seq = 5218 group by all_sales.sales_channel, all_sales.item.id limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...um(all_sales.ext_sales_price) ??? where all_sales.date.week_seq
  ```
- `trilogy run --import raw/physical_sales.preql:store_sales select distinct store_sales.item.brand_name from store_sales where lower(store_sales.item.brand_name) like '%scholaramalg%' limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? store_sales.item.brand_name fr...
  ```
- `trilogy run --import raw/physical_sales:physical_sales --import raw/catalog_sales:catalog_sales --import raw/catalog_returns:catalog_returns --import raw/customer:customer select physical_sales.item.id, count(physical_sales.row_counter) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...t(physical_sales.row_counter) ??? limit 5;
  ```
- `trilogy file write query66.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;

# Catalog sales: compute monthly totals of (ext…ales_tot_11,
    cat_ext_sales_qty by catalog.sold_date.month_of_year as cat_ext_sales_tot_12
order by catalog.warehouse.name nulls first
limit 100;
`

  ```text
  refused to write 'query66.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...nnel,     cat_ext_sales_qty by ??? catalog.sold_date.month_of_ye...

  Write stats: received 2210 chars / 2210 bytes; tail: …'t_12\\norder by
  catalog.warehouse.name nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.physical_sales:store_sales select max(store_sales.date.year), min(store_sales.date.year), count(store_sales.ticket_number) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ect max(store_sales.date.year) ??? , min(store_sales.date.year),
  ```
- `trilogy run --import raw.physical_sales:store_sales select distinct store_sales.store.county as co where store_sales.date.year in (1999,2000,2001) and store_sales.date.day_of_month in (1,2) and store_sales.store.county is not null order by co limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? store_sales.store.county as co...
  ```
- `trilogy run --import raw.physical_sales:store_sales select distinct store_sales.billing_customer.household_demographic.buy_potential as bp, store_sales.billing_customer.household_demographic.vehicle_count as vc order by bp limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? store_sales.billing_customer.h...
  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Date range
where all_sales.date.date between '2000-08-23'::date and '2000-09…les.sales_channel, all_sales.channel_dim_id as g_outlet
order by
    channel asc nulls first,
    outlet asc nulls first,
    returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ..._sales.ext_sales_price), 0) by ??? rollup all_sales.sales_channe...

  Write stats: received 925 chars / 925 bytes; tail: …'rst,\\n    outlet asc
  nulls first,\\n    returns desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.sales_channel, sum(all_sales.ext_sales_price) by rollup all_sales.sales_channel limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ollup all_sales.sales_channel ??? limit 5;
  ```
- `trilogy run --import raw.all_sales:all_sales --all-rows --displayed-rows 100 select all_sales.sales_channel as channel, all_sales.channel_dim_id as outlet, c…ollup all_sales.sales_channel, all_sales.channel_dim_id as g_outlet order by channel asc nulls first, outlet asc nulls first, returns desc limit 100;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ll_sales.ext_sales_price), 0) ??? by rollup all_sales.sales_chan...
  ```
- `trilogy run --import raw.all_sales:all_sales --all-rows --displayed-rows 100 select all_sales.sales_channel as channel, all_sales.channel_dim_id as outlet, c…ollup all_sales.sales_channel, all_sales.channel_dim_id as g_outlet order by channel asc nulls first, outlet asc nulls first, returns desc limit 100;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ll_sales.ext_sales_price), 0) ??? by rollup all_sales.sales_chan...
  ```
- `trilogy run --import raw.all_sales:all_sales --all-rows --displayed-rows 100 select all_sales.sales_channel as channel, all_sales.channel_dim_id as outlet, s…ollup all_sales.sales_channel, all_sales.channel_dim_id as g_outlet order by channel asc nulls first, outlet asc nulls first, returns desc limit 100;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ll_sales.return_net_loss), 0) ??? by rollup all_sales.sales_chan...
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  …catalog_page' else 'web_site' end) || all_sales.channel_dim_text_id) as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ollup all_sales.sales_channel, ??? ((case when all_sales.sales_c...

  Write stats: received 1338 chars / 1338 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  …'catalog_page' else 'web_site' end || all_sales.channel_dim_text_id) as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ollup all_sales.sales_channel, ??? (case when all_sales.sales_ch...

  Write stats: received 1330 chars / 1330 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  … 'catalog_page' else 'web_site' end || all_sales.channel_dim_text_id as profit
order by channel nulls first, outlet_identifier nulls first limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...sales.sales_channel, case when ??? all_sales.sales_channel = 'ST...

  Write stats: received 1324 chars / 1324 bytes; tail: …'hannel nulls first,
  outlet_identifier nulls first limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.sales_channel;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...as all_sales; select distinct ??? all_sales.sales_channel;
  ```
- `trilogy run --debug --import raw.physical_returns:store_returns select count(store_returns.ticket_number) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...(store_returns.ticket_number) ??? limit 10;
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\pest_backend.p
  y", line 321, in parse_pest
      tree = parse_trilogy_syntax_tuple(text)
  ValueError:  --> 2:43
    |
  2 | select count(store_returns.ticket_number) limit 10;
    |                                           ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over

  The above exception was the direct cause of the following exception:

  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\scripts\parallel_executio
  n.py", line 578, in run_single_script_execution
      queries = exec.parse_text(
          text, root=base if isinstance(base, Path) else None
      )
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\executor.py", line
  700, in parse_text
      r
  …
  ```
- `trilogy run --import raw.web_sales as web_sales select web_sales.time.hour, count(web_sales.order_number) where web_sales.bill_household_demographic.dependent_count = 6 and web_sales.web_page.char_count between 5000 and 5200;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...count(web_sales.order_number) ??? where web_sales.bill_household...
  ```
- `trilogy run --import raw/physical_sales:ps select distinct ps.return_reason.desc limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ..._sales as ps; select distinct ??? ps.return_reason.desc limit 20...
  ```
- `trilogy run --import raw/physical_sales:ps select ps.return_reason.id, ps.return_reason.desc, count(ps.ticket_number) where ps.return_reason.id is not null group by 1,2 limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...desc, count(ps.ticket_number) ??? where ps.return_reason.id is n...
  ```

### `undefined-concept`

- `trilogy run --import raw/physical_sales:store_sales select store_sales.return_customer.text_id as c_customer_id,
    avg(cust_store_ret) by store_sales.return_store.id as store_avg
where store_sales.return_store.state = 'TN'
  and store_sales.return_date.year = 2000
limit 5;`

  ```text
  (UndefinedConceptException(...), 'line: 2: Undefined concept:
  local.cust_store_ret.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  billing_customer.id. Suggestions: ['store_sales.billing_customer.id',
  'store_sales.billing_customer.text_id', 'store_sales.billing_customer.login']")
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), 'line: 13: Undefined
  concept: local.avg_sale_value_all.')
  ```
- `trilogy run query30.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  salutation. Suggestions: ['customer.salutation']")
  ```
- `trilogy run query33.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.category. Suggestions: ['all_sales.item.category',
  'all_sales.item.category_id']")
  ```
- `trilogy run query66.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web.sold_date.month_of_year. Suggestions: ['web.date.month_of_year',
  'web.ship_date.month_of_year', 'catalog.sold_date.month_of_year']")
  ```
- `trilogy run query70_check7.preql`

  ```text
  (UndefinedConceptException(...), "line: 4: Undefined concept:
  sales.store.state. Suggestions: ['sales.warehouse.state', 'sales.time.time',
  'sales.bill_address.state']")
  ```
- `trilogy run select store_sales.billing_customer.last_name as ln, count(store_sales.ticket_number) as cnt where store_sales.date.year in (1999,2000,2001) and …stomer.household_demographic.buy_potential in ('Unknown','>10000') and store_sales.billing_customer.household_demographic.vehicle_count > 0 limit 10;`

  ```text
  (UndefinedConceptException(...), 'line: 1: Undefined concept:
  store_sales.ticket_number.')
  ```
- `trilogy run query74.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  raw.customer.id. Suggestions: ['web_sales.ship_customer.id',
  'store_sales.return_customer.id', 'store_sales.billing_customer.id']")
  ```
- `trilogy run query76.preql`

  ```text
  (UndefinedConceptException(...), 'line: 12: Undefined
  concept: local.channel.')
  ```
- `trilogy run --import raw/physical_sales:ps select billing_customer.id, billing_customer.full_name limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  billing_customer.id. Suggestions: ['ps.billing_customer.id',
  'ps.billing_customer.text_id', 'ps.billing_customer.login']")
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.param_zips_list. Valid:
  'STRING'.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type ArrayType<STRING>' passed into
  SUBSTRING function from function SPLIT in position 1. Valid: 'STRING'
  ```
- `trilogy run --import raw.web_returns:wsr select year(wsr.web_sales.date.year) as sale_year, count(wsr.web_sales.order_number) as cnt order by sale_year limit 20;`

  ```text
  Invalid argument type 'Trait<INTEGER, ['year']>' passed into
  YEAR function in position 1 from concept: wsr.web_sales.date.year. Valid:
  'DATE', 'DATETIME', 'TIMESTAMP'.
  ```
