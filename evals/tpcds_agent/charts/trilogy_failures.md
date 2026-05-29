# Trilogy failure analysis — 20260529-191523

- Run `20260529-191521_base` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 3304 | failed: 448 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 172 | 38% |
| `syntax-parse` | 128 | 29% |
| `syntax-missing-alias` | 45 | 10% |
| `undefined-concept` | 34 | 8% |
| `cli-misuse` | 33 | 7% |
| `join-resolution` | 28 | 6% |
| `type-error` | 5 | 1% |
| `file-not-found` | 3 | 1% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text
  HAVING references 'local.cust_store_return', which is not in
  the SELECT projection (line 10). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.cust_store_return`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_agg_sum_6502742331444045', 'local.sales',
  'local._virt_agg_sum_9615521547794243', 'local.returns'} out of  with found
  {'local.sales', 'local.outlet'}
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.store.store_id, sum(store_sales.ext_sales_price) as sales, sum(store_returns.return_amt) as returns limit 10;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.returns', 'local.sales'} out of  with found
  {'store_sales.store.store_id', 'local.sales'}
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.store.store_id, sum(store_sales.ext_sales_price) as sales, sum(store_returns.return_amt) as returns limit 10;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.sales', 'local.returns'} out of  with found {'local.sales',
  'store_sales.store.store_id'}
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.store.store_id, sum(store_sales.ext_sales_price) as sales, sum(store_sales.net_profit) as profit, sum(store_returns.return_amt) as returns, sum(store_returns.net_loss) as loss limit 10;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.loss', 'local.profit', 'local.returns'} out of  with found
  {'local.sales', 'store_sales.store.store_id', 'local.profit'}
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.store.store_id, sum(store_sales.ext_sales_price) as sales, sum(store_sales.net_profit) as profit, sum(store_returns.return_amt) as returns limit 10;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.profit', 'local.returns'} out of  with found
  {'store_sales.store.store_id', 'local.profit', 'local.sales'}
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.store.store_id, sum(ss.ext_sales_price) as sales, sum(ss.net_profit) as profit, sum(sr.return_amt) as returns limit 10;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.returns', 'local.profit'} out of  with found
  {'ss.store.store_id', 'local.profit', 'local.sales'}
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_agg_sum_9615521547794243', 'local.total_sales',
  'local._virt_agg_sum_6502742331444045'} out of  with found
  {'local.total_sales', 'local.outlet'}
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.store.store_sk, concat('store_', coalesce(store_… as total_sales, sum(store_sales.net_profit) as profit, sum(store_returns.return_amt) as total_returns, sum(store_returns.net_loss) as loss limit 10;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.profit', 'local.total_returns', 'local.loss', 'local.outlet'}
  out of  with found {'local.profit', 'local.total_sales',
  'store_sales.store.store_sk'}
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.store.store_sk, concat('store_', coalesce(ss.store.store_id, 'NULL')) as outlet, sum(ss.ext_sales_price) as total_sales, sum(sr.return_amt) as total_returns, sum(ss.net_profit) - sum(sr.net_loss) as total_profit limit 10;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_agg_sum_2856259540536641',
  'local._virt_agg_sum_5237463246240634', 'local.total_returns'} out of  with
  found {'local._virt_agg_sum_2856259540536641', 'local.total_sales',
  'ss.store.store_sk'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 123 (char 122). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Cannot compare values
  of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is
  required

  LINE 32: ..."."store_sales_customer_customer_address_zip" ELSE NULL END in
  (select quizzical."zips_param" from quizzical where quiz...
                                                                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip",
      count("store_sales_customer_customer"."c_customer_sk") as "pref_zip_count"
  FROM
      "c
  …
  "store_sales_store_store"."s_zip",1,2) in (select
  questionable."matched_zip_prefixes" from questionable where
  questionable."matched_zip_prefixes" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "vacuous"."store_sales_store_store_name" as "store_sales_store_store_name",
      sum("vacuous"."store_sales_net_profit") as "total_net_profit"
  FROM
      "vacuous"
  GROUP BY
      1
  ORDER BY
      "vacuous"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --debug -e SELECT 1 as test;`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing
  env file: SELECT 1 as test;
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.web_sales:web_sales --import raw.customer:customer merge customer.customer_sk into ~web_sales.b…e - web_sales.ext_discount_amt ? web_sales.sold_date.year=2001) by web_sales.bill_customer.customer_sk as wr2001 where sr2001>0 and wr2001>0 limit 5;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.sr2001) in the same statement where clause; move to the HAVING clause
  instead; Line: 4
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.customer_id, store_sales.customer.preferred_cust_flag, store_sales.date_dim.year….ext_discount_amt) by store_sales.customer.customer_sk, store_sales.date_dim.year as rev where store_sales.date_dim.year = 2002 and rev > 0 limit 10;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.rev) in the same statement where clause; move to the HAVING clause
  instead; Line: 2
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 67 (char 66). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.ss_val_all', which is not in the
  SELECT projection (line 27). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ..., --local.ss_val_all`;
  (b) move the filter to WHERE — for an aggregate condition on a non-output
  grain, write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 223:     sum(INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.quantity> * CASE WHEN INVAL...
                                                   ^
  [SQL:
  WITH
  sparkling as (
  SELECT
      "web_sales_web_sales"."ws_item_sk" as "store_sales_item_item_sk",
      "web_sales_web_sales"."ws_sold_date_sk" as "web_sales_sold_date_date_sk"
  FROM
      "web_sales" as "web_sales_web_sales"
  GROUP BY
      1,
      2),
  quizzical as (
  SELECT
      "catalog_sales_catalog_sales"."cs_item_sk" as "store_sales_item_item_sk",
      "cat
  …
  Missing source reference to
  store_sales.quantity> * CASE WHEN INVALID_REFERENCE_BUG_<Missing source
  reference to store_sales.date_dim.year> = 2001 and
  INVALID_REFERENCE_BUG_<Missing source reference to store_sales.date_dim.moy> =
  11 THEN INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.list_price> ELSE NULL END) > avg("puffy"."ss_val_all")

  ORDER BY
      "channel" asc nulls first,
      "puffy"."brand_id" asc nulls first,
      "puffy"."class_id" asc nulls first,
      "puffy"."category_id" asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query14.preql`

  ```text
  Statement 1 failed: (_duckdb.Error) Parameter not supported in ORDER BY clause
  [SQL:
  WITH
  sparkling as (
  SELECT
      "web_sales_web_sales"."ws_item_sk" as "store_sales_item_item_sk",
      "web_sales_web_sales"."ws_sold_date_sk" as "web_sales_sold_date_date_sk"
  FROM
      "web_sales" as "web_sales_web_sales"
  GROUP BY
      1,
      2),
  quizzical as (
  SELECT
      "catalog_sales_catalog_sales"."cs_item_sk" as "store_sales_item_item_sk",
      "catalog_sales_catalog_sales"."cs_sold_date_sk" as
  "catalog_sales_sold_date_date_sk"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
  GROUP BY
      1,

  …
  s_num"
  FROM
      "courageous"
      RIGHT OUTER JOIN "hard" on "courageous"."brand_id" is not distinct from
  "hard"."brand_id" AND "courageous"."category_id" is not distinct from
  "hard"."category_id" AND "courageous"."class_id" is not distinct from
  "hard"."class_id"
  ORDER BY
      $1 asc nulls first,
      coalesce("courageous"."brand_id","hard"."brand_id") asc nulls first,
      coalesce("courageous"."class_id","hard"."class_id") asc nulls first,
      coalesce("courageous"."category_id","hard"."category_id") asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/dbapi)
  ```
- `trilogy run query14.preql`

  ```text
  Statement 1 failed: (_duckdb.Error) Parameter not supported in ORDER BY clause
  [SQL:
  WITH
  sparkling as (
  SELECT
      "web_sales_web_sales"."ws_item_sk" as "store_sales_item_item_sk",
      "web_sales_web_sales"."ws_sold_date_sk" as "web_sales_sold_date_date_sk"
  FROM
      "web_sales" as "web_sales_web_sales"
  GROUP BY
      1,
      2),
  quizzical as (
  SELECT
      "catalog_sales_catalog_sales"."cs_item_sk" as "store_sales_item_item_sk",
      "catalog_sales_catalog_sales"."cs_sold_date_sk" as
  "catalog_sales_sold_date_date_sk"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
  GROUP BY
      1,

  …
  s_num"
  FROM
      "courageous"
      RIGHT OUTER JOIN "hard" on "courageous"."brand_id" is not distinct from
  "hard"."brand_id" AND "courageous"."category_id" is not distinct from
  "hard"."category_id" AND "courageous"."class_id" is not distinct from
  "hard"."class_id"
  ORDER BY
      $1 asc nulls first,
      coalesce("courageous"."brand_id","hard"."brand_id") asc nulls first,
      coalesce("courageous"."class_id","hard"."class_id") asc nulls first,
      coalesce("courageous"."category_id","hard"."category_id") asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/dbapi)
  ```
- `trilogy run query14.preql`

  ```text
  Statement 1 failed: (_duckdb.Error) Parameter not supported in ORDER BY clause
  [SQL:
  WITH
  sparkling as (
  SELECT
      "web_sales_web_sales"."ws_item_sk" as "store_sales_item_item_sk",
      "web_sales_web_sales"."ws_sold_date_sk" as "web_sales_sold_date_date_sk"
  FROM
      "web_sales" as "web_sales_web_sales"
  GROUP BY
      1,
      2),
  quizzical as (
  SELECT
      "catalog_sales_catalog_sales"."cs_item_sk" as "store_sales_item_item_sk",
      "catalog_sales_catalog_sales"."cs_sold_date_sk" as
  "catalog_sales_sold_date_date_sk"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
  GROUP BY
      1,

  …
  s_num"
  FROM
      "courageous"
      RIGHT OUTER JOIN "hard" on "courageous"."brand_id" is not distinct from
  "hard"."brand_id" AND "courageous"."category_id" is not distinct from
  "hard"."category_id" AND "courageous"."class_id" is not distinct from
  "hard"."class_id"
  ORDER BY
      $1 asc nulls first,
      coalesce("courageous"."brand_id","hard"."brand_id") asc nulls first,
      coalesce("courageous"."class_id","hard"."class_id") asc nulls first,
      coalesce("courageous"."category_id","hard"."category_id") asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/dbapi)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 80 (char 79). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query20.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 79 (char 78). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query21.preql`

  ```text
  Unable to import '.\inventory.preql': [Errno 2] No such file
  or directory: '.\\inventory.preql'. Did you mean: raw.inventory?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query23.preql`

  ```text
  HAVING references 'local.item_pair_count', which is not in
  the SELECT projection (line 8). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.item_pair_count`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  HAVING references 'local.item_pair_count', which is not in
  the SELECT projection (line 17). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.item_pair_count`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy file write --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as catalog_sales;

# …,
    sum(catalog_sales.net_profit) as catalog_sales_net_profit
order by item_code, item_description, store_code, store_name
limit 100; query25.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 81 (char 80). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 84 (char 83). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query31.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 65:     CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to
  web_sales.sold_date.year> = 2000 and...
                                                        ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "store_sales_customer_customer_address_customer_address"."ca_county" as
  "store_sales_customer_customer_address_county",
      "store_sales_date_dim_date_dim"."d_qoy" as "store_sales_date_dim_qoy",
      "store_sales_date_dim_date_dim"."d_year" as "store_sales_date_dim_year",
      "store_sales_store_sales"."ss_ext_sa
  …
  2000 and INVALID_REFERENCE_BUG_<Missing source
  reference to store_sales.date_dim.qoy> = 3 THEN INVALID_REFERENCE_BUG_<Missing
  source reference to store_sales.ext_sales_price> ELSE NULL END / nullif(CASE
  WHEN INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.date_dim.year> = 2000 and INVALID_REFERENCE_BUG_<Missing source
  reference to store_sales.date_dim.qoy> = 2 THEN INVALID_REFERENCE_BUG_<Missing
  source reference to store_sales.ext_sales_price> ELSE NULL END,0) is not null

  ORDER BY
      "abhorrent"."county" asc]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query32.preql --import raw.catalog_sales:catalog_sales select catalog_sales.item.manufact_id, catalog_sales.sold_date.date limit 10;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy unit query34.preql`

  ```text
  Mocking not implemented for datatype BIGINT
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run --import raw.store_sales:store_sales select grouping(store_sales.item.category) + grouping(store_sales.item.class) as hl, sum(store_sales.net_pro…), 0) asc) as rnk where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN' order by hl desc nulls first, gm asc nulls first limit 30`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING function is
  not supported here
  [SQL:
  WITH
  questionable as (
  SELECT
      "store_sales_store_sales"."ss_item_sk" as "store_sales_item_item_sk",
      "store_sales_store_sales"."ss_sold_date_sk" as
  "store_sales_date_dim_date_sk",
      "store_sales_store_sales"."ss_store_sk" as "store_sales_store_store_sk"
  FROM
      "store_sales" as "store_sales_store_sales"
  GROUP BY
      1,
      2,
      3),
  thoughtful as (
  SELECT
      "store_sales_item_item"."i_category" as "store_sales_item_category",
      "store_sales_item_item"."i_class" as "store_sales_item_class",

  …
  es_item_class
  ") as "store_sales_item_class",
      "yummy"."rnk" as "rnk"
  FROM
      "yummy"
      FULL JOIN "vacuous" on "yummy"."_virt_func_case_3712811191885438" is not
  distinct from "vacuous"."_virt_func_case_3712811191885438" AND
  "yummy"."store_sales_item_category" is not distinct from
  "vacuous"."store_sales_item_category" AND "yummy"."store_sales_item_class" is
  not distinct from "vacuous"."store_sales_item_class"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      "hl" desc nulls first,
      "gm" asc nulls first
  LIMIT (30)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw.store_sales:store_sales select grouping(store_sales.item.category) + grouping(store_sales.item.class) as hl, sum(store_sales.net_pro…), 0) asc) as rnk where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN' order by hl desc nulls first, gm asc nulls first limit 30`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING function is
  not supported here
  [SQL:
  WITH
  questionable as (
  SELECT
      "store_sales_store_sales"."ss_item_sk" as "store_sales_item_item_sk",
      "store_sales_store_sales"."ss_sold_date_sk" as
  "store_sales_date_dim_date_sk",
      "store_sales_store_sales"."ss_store_sk" as "store_sales_store_store_sk"
  FROM
      "store_sales" as "store_sales_store_sales"
  GROUP BY
      1,
      2,
      3),
  thoughtful as (
  SELECT
      "store_sales_item_item"."i_category" as "store_sales_item_category",
      "store_sales_item_item"."i_class" as "store_sales_item_class",

  …
  ass
  ") as "store_sales_item_class",
      "yummy"."rnk" as "rnk"
  FROM
      "yummy"
      FULL JOIN "vacuous" on "yummy"."_virt_func_case_3778886985794125" is not
  distinct from "vacuous"."_virt_func_case_3778886985794125" AND
  "yummy"."store_sales_item_category" is not distinct from
  "vacuous"."store_sales_item_category" AND "yummy"."store_sales_item_class" is
  not distinct from "vacuous"."store_sales_item_class"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      "vacuous"."hl" desc nulls first,
      "gm" asc nulls first
  LIMIT (30)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query37.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run --import raw.item:item select item.item_id, item.item_desc, item.current_price, item.manufact_id, item.current_price where item.current_price bet… 68 and 98 and (item.manufact_id = 677 or item.manufact_id = 940 or item.manufact_id = 694 or item.manufact_id = 808) order by item.item_id limit 20;`

  ```text
  Duplicate select output for item.current_price; Line: 2
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 88 (char 87). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 171 (char 170). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 84 (char 83). Re-issue the call with valid JSON arguments.
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
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
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
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 98 (char 97). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 4 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 5 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy run query39.preql`

  ```text
  Unable to import '.\inventory.preql': [Errno 2] No such file
  or directory: '.\\inventory.preql'. Did you mean: raw.inventory?
  ```
- `trilogy run query39.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query39.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Values list "yummy"
  does not have a column named "qty_mean"

  LINE 93:     "yummy"."qty_mean" as "qty_mean",
               ^
  [SQL:
  WITH
  uneven as (
  SELECT
      "inventory_date_dim_date_dim"."d_moy" as "inventory_date_dim_moy",
      "inventory_inventory"."inv_item_sk" as "inventory_item_item_sk",
      "inventory_inventory"."inv_warehouse_sk" as
  "inventory_warehouse_warehouse_sk"
  FROM
      "date_dim" as "inventory_date_dim_date_dim"
      LEFT OUTER JOIN "inventory" as "inventory_inventory" on
  "inventory_date_dim_date_dim"."d_date_sk" = "inventory_inventory"
  …
  ory_date_dim_moy" =
  "abundant"."inventory_date_dim_moy" AND "yummy"."inventory_item_item_sk" =
  "abundant"."inventory_item_item_sk" AND
  "yummy"."inventory_warehouse_warehouse_sk" =
  "abundant"."inventory_warehouse_warehouse_sk"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      "abundant"."inventory_warehouse_warehouse_id" asc nulls first,
      "abundant"."inventory_item_item_id" asc nulls first,
      "yummy"."inventory_date_dim_moy" asc nulls first,
      "yummy"."qty_mean" asc nulls first,
      "cv" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query39.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Values list "uneven"
  does not have a column named "qty_mean"

  LINE 91:     "uneven"."qty_mean" as "qty_mean",
               ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "inventory_date_dim_date_dim"."d_moy" as "inventory_date_dim_moy",
      "inventory_date_dim_date_dim"."d_year" as "inventory_date_dim_year",
      "inventory_inventory"."inv_item_sk" as "inventory_item_item_sk",
      "inventory_inventory"."inv_quantity_on_hand" as
  "inventory_quantity_on_hand",
      "inventory_inventory"."inv_warehouse_sk" as
  "inventory_warehouse_warehouse_sk"
  FROM
      "date_dim"
  …
  date_dim_moy" =
  "abundant"."inventory_date_dim_moy" AND "uneven"."inventory_item_item_sk" =
  "abundant"."inventory_item_item_sk" AND
  "uneven"."inventory_warehouse_warehouse_sk" =
  "abundant"."inventory_warehouse_warehouse_sk"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      "abundant"."inventory_warehouse_warehouse_id" asc nulls first,
      "abundant"."inventory_item_item_id" asc nulls first,
      "uneven"."inventory_date_dim_moy" asc nulls first,
      "uneven"."qty_mean" asc nulls first,
      "cv" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query39.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query39.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Values list
  "cooperative" does not have a column named "qty_mean"

  LINE 55:     "cooperative"."qty_mean" as "qty_mean",
               ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "inventory_date_dim_date_dim"."d_moy" as "inventory_date_dim_moy",
      "inventory_inventory"."inv_item_sk" as "inventory_item_item_sk",
      "inventory_inventory"."inv_warehouse_sk" as
  "inventory_warehouse_warehouse_sk"
  FROM
      "date_dim" as "inventory_date_dim_date_dim"
      LEFT OUTER JOIN "inventory" as "inventory_inventory" on
  "inventory_date_dim_date_dim"."d_date_sk" = "invento
  …
  ry_item_item" on
  "cooperative"."inventory_item_item_sk" = "inventory_item_item"."i_item_sk"
      INNER JOIN "warehouse" as "inventory_warehouse_warehouse" on
  "cooperative"."inventory_warehouse_warehouse_sk" =
  "inventory_warehouse_warehouse"."w_warehouse_sk"
  ORDER BY
      "inventory_warehouse_warehouse"."w_warehouse_id" asc nulls first,
      "inventory_item_item"."i_item_id" asc nulls first,
      "cooperative"."inventory_date_dim_moy" asc nulls first,
      "cooperative"."qty_mean" asc nulls first,
      "cv" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw.item:item select item.manufact as man, count(item.item_sk) over (partition by item.manufact) as man_cnt, item.product_name as pn where item.manufact_id between 738 and 778 and man_cnt >= 2 order by pn limit 100`

  ```text
  Cannot reference an aggregate derived in the select
  (local.man_cnt) in the same statement where clause; move to the HAVING clause
  instead; Line: 2
  ```
- `trilogy run --import raw.item:item select item.category as cat, item.color as col, item.units as un, item.size as sz where item.manufact_id between 738 and 778 order by pn limit 5`

  ```text
  ORDER BY references 'local.pn', which is not in the SELECT
  projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep
  it out of the output rows, e.g. `select ..., --local.pn order by local.pn asc`.
  ```
- `trilogy run query44.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run query44.preql`

  ```text
  Value 4 is not valid for enum field
  'store_sales.store.store_sk'. Allowed values: 1.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.store_sk as store_id, count(store_sales.ticket_number) as cnt having store_id=4;`

  ```text
  Value 4 is not valid for enum field 'local.store_id'. Allowed
  values: 1.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query47.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 88: ...    "uneven"."year" = 1999 and INVALID_REFERENCE_BUG_<Missing
  source reference to store_sales.sales_price> > 0 and abs...
                                                                            ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "store_sales_date_dim_date_dim"."d_moy" as "store_sales_date_dim_moy",
      "store_sales_date_dim_date_dim"."d_year" as "store_sales_date_dim_year",
      "store_sales_item_item"."i_brand" as "store_sales_item_brand",
      "store_sales_item_item"."i_category" as "store_sales_i
  …
  rice> > 0 and abs("uneven"."this_month_total" -
  INVALID_REFERENCE_BUG_<Missing source reference to store_sales.sales_price>) /
  INVALID_REFERENCE_BUG_<Missing source reference to store_sales.sales_price> >
  0.1

  ORDER BY
      ( "uneven"."this_month_total" - INVALID_REFERENCE_BUG_<Missing source
  reference to store_sales.sales_price> ) asc,
      "uneven"."category" asc,
      "uneven"."brand" asc,
      "uneven"."store_name" asc,
      "uneven"."store_company_name" asc,
      "uneven"."year" asc,
      "uneven"."month_of_year" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/store_returns:store_returns select store_sales.store.store_name, count(store_sales.ticket_number) as sales_count, count(store_returns.ticket_number) as return_count limit 5;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.return_count', 'local.sales_count'} out of  with found
  {'store_sales.store.store_name', 'local.sales_count'}
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/store_returns:store_returns select store_sales.store.store_name, store_sales.date_dim.date as sale_date, store_returns.date_dim.date as return_date, date_diff(store_returns.date_dim.date, store_sales.date_dim.date, DAY) as elapsed limit 5;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.elapsed', 'local.sale_date', 'local.return_date'} out of
  with found {'store_sales.date_dim.date', 'local.sale_date',
  'store_sales.store.store_name'}
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/store_returns:store_returns select store_sales.store.store_name, store_sales.date_dim.date as sale_date, store_returns.date_dim.date as return_date limit 5;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.return_date', 'local.sale_date'} out of  with found
  {'store_sales.store.store_name', 'store_sales.date_dim.date',
  'local.sale_date'}
  ```
- `trilogy run query51.preql`

  ```text
  Cannot bind non-singleton concept local.store_rt
  (Granularity.MULTI_ROW, lineage alias(ref:local.store_running_total)) to a
  parameter.
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.store_sales:store_sales select web_sales.item.item_sk, web_sales.sold_date.date, web_sales.item.item_sk, web_sales.sold_date.year limit 5;`

  ```text
  Duplicate select output for web_sales.item.item_sk; Line: 3
  ```
- `trilogy run - duckdb --import raw.web_sales:web_sales --import raw.store_sales:store_sales`

  ```text
  Duplicate select output for web_sales.item.item_sk; Line: 8
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read query53.preql`

  ```text
  No such file: query53.preql
  ```
- `trilogy run --import store_sales select store_sales.item.manager_id, store_sales.item.brand, count(store_sales.ticket_number) as cnt limit 3;`

  ```text
  Unable to import
  'C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\2026052
  9-191521_base\workspace\_worker_0\store_sales.preql': [Errno 2] No such file or
  directory:
  'C:\\Users\\ethan\\coding_projects\\pytrilogy_two\\evals\\tpcds_agent\\results\
  \20260529-191521_base\\workspace\\_worker_0\\store_sales.preql'. Did you mean:
  raw.store_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 61 (char 60). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 1629 (char 1628). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 86 (char 85). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs; stdin import raw.catalog_sales as cs;

auto monthly_total <- sum(cs.net_paid) by c…thly_sales) asc nulls first,
    cs.item.category,
    cs.item.brand,
    cs.call_center.name,
    cs.sold_date.year,
    cs.sold_date.moy
limit 100;`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 3 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run query57.preql`

  ```text
  HAVING references 'local.rel_dev', which is not in the SELECT
  projection (line 5). Fix one of: (a) add it to SELECT — prefix with `--` to
  keep it out of the output rows, e.g. `select ..., --local.rel_dev`; (b) move
  the filter to WHERE — for an aggregate condition on a non-output grain, write
  the aggregate inline as `agg(x) by grain` directly in WHERE.
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
      "cs_call_center_call_center"."cc_name" as "cs_call_center_name",
      "cs_item_item"."i_brand" as "cs_item_brand",
      "cs_item_item"."i_category" as "cs_item_category",
      "cs_sold_date_date_dim"."d_moy" as "cs_sold_date_moy",
      "cs_sold_date_date_dim"."d_year" as "cs_sold_date_year",
      sum("cs_catalog_sales"."cs_net_paid") as "monthly_tota
  …
  l",
      "uneven"."prev_month_total" as "prev_month_total",
      "uneven"."next_month_total" as "next_month_total"
  FROM
      "uneven"
  WHERE
      "uneven"."avg_monthly_sales" > 0 and "uneven"."rel_dev" > 0.1

  ORDER BY
      ( "uneven"."this_month_total" - "uneven"."avg_monthly_sales" ) asc nulls
  first,
      "uneven"."cs_item_category" asc,
      "uneven"."cs_item_brand" asc,
      INVALID_REFERENCE_BUG_<Missing source reference to cs.call_center.name>
  asc,
      "uneven"."cs_sold_date_year" asc,
      "uneven"."cs_sold_date_moy" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs --import raw/web_sales:ws select ss.item.item_id, sum(ss.ext_sales_price) as ss_total, sum(cs.ext_sales_price) as cs_total, sum(ws.ext_sales_price) as ws_total limit 5;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.cs_total', 'local.ss_total', 'local.ws_total'} out of  with
  found {'local.ss_total', 'ss.item.item_id'}
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs select ss.item.item_id, sum(ss.ext_sales_price) as ss_total, sum(cs.ext_sales_price) as cs_total limit 5;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ss_total', 'local.cs_total'} out of  with found
  {'local.ss_total', 'ss.item.item_id'}
  ```
- `trilogy run --import raw/catalog_sales:cs --import raw/web_sales:ws select cs.item.item_id, sum(cs.ext_sales_price) as cs_total, sum(ws.ext_sales_price) as ws_total limit 5;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ws_total', 'local.cs_total'} out of  with found
  {'cs.item.item_id', 'local.cs_total'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 76 (char 75). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 69 (char 68). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 74 (char 73). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query59.preql`

  ```text
  GROUP BY clause
  cannot contain aggregates!

  LINE 52: ...  CASE WHEN "cheerful"."store_sales_date_dim_year" = 2001 THEN
  sum("cheerful"."_virt_filter_ext_sales_price_9906239080929290...
                                                                             ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "store_sales_date_dim_date_dim"."d_dow" as "store_sales_date_dim_dow",
      "store_sales_date_dim_date_dim"."d_week_seq" as
  "store_sales_date_dim_week_seq",
      "store_sales_date_dim_date_dim"."d_year" as "store_sales_date_dim_year",
      "store_sales_store_sales"."ss_ext_sales_price" as
  "store_sal
  …
  on "resonant"."store_sales_date_dim_week_seq" =
  "puzzled"."store_sales_date_dim_week_seq" AND
  "resonant"."store_sales_store_store_sk" is not distinct from
  "puzzled"."store_sales_store_store_sk"
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
      10
  ORDER BY
      "resonant"."store_sales_store_store_name" asc nulls first,
      "resonant"."store_sales_store_store_id" asc nulls first,
      coalesce("puzzled"."store_sales_date_dim_week_seq","resonant"."store_sales_
  date_dim_week_seq") asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query60.preql`

  ```text
  Have
  {'MergeNode<store_sales.customer_address.gmt_offset,store_sales.date_dim.moy,st
  ore_sales.date_dim.year...4 more>': None} and need store_sales.date_dim.year =
  1998 and store_sales.date_dim.moy = 9 and
  store_sales.customer_address.gmt_offset = -5 and store_sales.item.item_id =
  local.music_item_ids
  ```
- `trilogy run query61.preql`

  ```text
  Value 'Y' is not valid for enum field
  'store_sales.promotion.channel_email'. Allowed values: 'N'.
  ```
- `trilogy run query61.preql`

  ```text
  Value 'Y' is not valid for enum field
  'store_sales.promotion.channel_email'. Allowed values: 'N'.
  ```
- `trilogy run query61.preql`

  ```text
  Value 'Y' is not valid for enum field
  'store_sales.promotion.channel_email'. Allowed values: 'N'.
  ```
- `trilogy run query61.preql`

  ```text
  Value 'Y' is not valid for enum field
  'store_sales.promotion.channel_email'. Allowed values: 'N'.
  ```
- `trilogy run query61.preql`

  ```text
  Value 'Y' is not valid for enum field
  'store_sales.promotion.channel_email'. Allowed values: 'N'.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query64.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 95 (char 94). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 95 (char 94). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw/store_sales.preql:store_sales --import raw/store.preql:store select store.store_name, store_sales.item.item_desc, sum(store_sales.sa…store_sales.date_dim.year = 1998 order by store.store_name, store_sales.item.item_desc, store.store_sk, store_sales.item.item_sk nulls first limit 5;`

  ```text
  ORDER BY references 'store.store_sk', which is not in the
  SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--`
  to keep it out of the output rows, e.g. `select ..., --store.store_sk order by
  store.store_sk asc`.
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.store.store_name, store_sales.item.item_desc, sum(store_sales.sales_price) as reven…sales.item.item_sk) by store_sales.store.store_sk order by store_sales.store.store_name nulls first, store_sales.item.item_desc nulls first limit 10;`

  ```text
  WHERE clause aggregate `sum(store_sales.sales_price)` is also
  computed in the SELECT (as `revenue`); aggregate filters must use the HAVING
  clause - e.g. `having revenue > ...`; Line: 2
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.store.store_name, store_sales.item.item_desc, sum(store_sales.sales_price) as reven… 0.1 * avg(revenue) by store_sales.store.store_sk order by store_sales.store.store_name nulls first, store_sales.item.item_desc nulls first limit 10;`

  ```text
  HAVING references 'store_sales.store.store_sk', which is not
  in the SELECT projection (line 2). Fix one of: (a) add it to SELECT — prefix
  with `--` to keep it out of the output rows, e.g. `select ...,
  --store_sales.store.store_sk`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.store.store_name, store_sales.item.item_desc, sum(store_sales.sales_price) as reven… 0.1 * avg(revenue) by store_sales.store.store_sk order by store_sales.store.store_name nulls first, store_sales.item.item_desc nulls first limit 10;`

  ```text
  HAVING clause aggregate `avg(local.revenue) by
  store_sales.store.store_sk` is not in the SELECT projection (line 2). HAVING
  can only filter on off-grain or nested aggregates that are also computed in the
  SELECT. Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of
  the output rows, e.g. `select ..., --avg(local.revenue) by
  store_sales.store.store_sk`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.store.store_name, store_sales.item.item_desc, sum(store_sales.sales_price) as reven…ore_name nulls first, store_sales.item.item_desc nulls first, store_sales.store.store_sk nulls first, store_sales.item.item_sk nulls first limit 100;`

  ```text
  ORDER BY references 'store_sales.item.item_sk', which is not
  in the SELECT projection (line 2). Add it to SELECT to sort by it — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --store_sales.item.item_sk order by store_sales.item.item_sk asc`.
  ```
- `trilogy run --import raw/store_sales.prejl:store_sales select store_sales.store.store_name, store_sales.item.item_desc, sum(store_sales.sales_price) as reven….item.brand where store_sales.date_dim.year = 1998 order by store_sales.store.store_name nulls first, store_sales.item.item_desc nulls first limit 5;`

  ```text
  Unable to import
  'C:\Users\ethan\coding_projects\pytrilogy_two\evals\tpcds_agent\results\2026052
  9-191521_base\workspace\_worker_1\raw\store_sales\prejl.preql': [Errno 2] No
  such file or directory:
  'C:\\Users\\ethan\\coding_projects\\pytrilogy_two\\evals\\tpcds_agent\\results\
  \20260529-191521_base\\workspace\\_worker_1\\raw\\store_sales\\prejl.preql'.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query66.preql`

  ```text
  Duplicate select output for
  web_sales.warehouse.warehouse_sq_ft; Line: 5
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query67.preql duckdb`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 89:     INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.date_dim.qoy> asc nulls...
                                              ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "store_sales_date_dim_date_dim"."d_moy" as "store_sales_date_dim_moy",
      "store_sales_date_dim_date_dim"."d_qoy" as "store_sales_date_dim_qoy",
      "store_sales_date_dim_date_dim"."d_year" as "store_sales_date_dim_year",
      "store_sales_item_item"."i_brand" as "store_sales_item_brand",
      "store_sales_item_item"."i_category" as
  …
  store_sales_item_brand" asc nulls first,
      "abundant"."store_sales_item_product_name" asc nulls first,
      "abundant"."store_sales_date_dim_year" asc nulls first,
      INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.date_dim.qoy> asc nulls first,
      INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.date_dim.moy> asc nulls first,
      INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.store.store_id> asc nulls first,
      "abundant"."sales_raw" asc,
      "abundant"."cat_rank" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query68.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run query69.preql`

  ```text
  Have
  {'GroupNode<local._virt_agg_count_2140972817556308,store_sales.customer.custome
  r_sk>': None,
  'MergeNode<store_sales.customer.customer_address.state,store_sales.customer.cus
  tomer_demographics.credit_rating,store_sales.customer.customer_demographics.edu
  cation_status...6 more>': None} and need
  BuildSubselectComparison(left=store_sales.customer.customer_address.state@Grain
  <store_sales.customer.customer_address.address_sk>, right=('KY', 'GA', 'NM'),
  operator=<ComparisonOperator.IN: 'in'>) and store_sales.date_dim.year = 2001
  and store_sales.date_dim.moy@Grain<store_sales.date_dim.date_sk> between 4 and
  6 and local._virt_agg_count_2140972817556308 >= 1 and
  local._virt_agg_count_8149832695417059 = 0 and
  local._virt_agg_count_6838249166744657 = 0
  ```
- `trilogy run query69.preql`

  ```text
  Have
  {'GroupNode<local.store_sale_2001_q2,store_sales.customer.customer_sk>': None,
  'MergeNode<store_sales.customer.customer_address.state,store_sales.customer.cus
  tomer_demographics.credit_rating,store_sales.customer.customer_demographics.edu
  cation_status...4 more>': None} and need
  BuildSubselectComparison(left=store_sales.customer.customer_address.state@Grain
  <store_sales.customer.customer_address.address_sk>, right=('KY', 'GA', 'NM'),
  operator=<ComparisonOperator.IN: 'in'>) and
  coalesce(local.store_sale_2001_q2@Grain<store_sales.customer.customer_sk>,0) >=
  1 and coalesce(local.web_sale_2001@Grain<store_sales.customer.customer_sk>,0) =
  0 and
  coalesce(local.catalog_sale_2001@Grain<store_sales.customer.customer_sk>,0) = 0
  ```
- `trilogy run -e --debug query72.preql`

  ```text
  Some scripts failed during execution.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

select
  '…annel asc, missing_reference_label asc, year asc nulls first, quarter_of_year asc nulls first, item_category asc nulls first
limit 100; query76.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

select
  '…annel asc, missing_reference_label asc, year asc nulls first, quarter_of_year asc nulls first, item_category asc nulls first
limit 100; query76.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy file write --content import raw.store_sales as store_sales;
select
  'store' as channel,
  'store reference' as missing_reference_label,
  store_sale…annel asc, missing_reference_label asc, year asc nulls first, quarter_of_year asc nulls first, item_category asc nulls first
limit 100; query76.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 99 (char 98). Re-issue the call with valid JSON arguments.
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
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 61 (char 60). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 69 (char 68). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 83 (char 82). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 75 (char 74). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy run query81.preql`

  ```text
  ORDER BY references 'local.salutation', which is not in the
  SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--`
  to keep it out of the output rows, e.g. `select ..., --local.salutation order
  by local.salutation asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
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
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.

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
- `trilogy run query83.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 12: ...."d_week_seq" in (min(CASE WHEN INVALID_REFERENCE_BUG_<Missing
  source reference to store_returns.date_dim.date> = date...
                                                                             ^
  [SQL:
  WITH
  young as (
  SELECT
      "store_returns_item_item"."i_item_sk" as "store_returns_item_item_sk",
      "web_returns_web_returns"."wr_return_quantity" as
  "_virt_filter_return_quantity_1721815486941896"
  FROM
      "item" as "store_returns_item_item"
      LEFT OUTER JOIN "web_returns" as "web_returns_web_retu
  …
  ) * 100 as
  "web_pct",
      (( "sweltering"."store_qty" + "cooperative"."catalog_qty" ) +
  "sweltering"."web_qty") / 3 as "avg_qty"
  FROM
      "cooperative"
      INNER JOIN "sweltering" on "cooperative"."store_returns_item_item_sk" =
  "sweltering"."store_returns_item_item_sk"
  WHERE
      "sweltering"."store_qty" > 0 and "sweltering"."web_qty" > 0

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8
  ORDER BY
      "sweltering"."store_returns_item_item_id" asc nulls first,
      "sweltering"."store_qty" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query83.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 20: ...."d_week_seq" in (min(CASE WHEN INVALID_REFERENCE_BUG_<Missing
  source reference to date_dim.date> = date '2000-06-30' THEN...
                                                                             ^
  [SQL:
  WITH
  vacuous as (
  SELECT
      "store_returns_item_item"."i_item_sk" as "store_returns_item_item_sk",
      sum("web_returns_web_returns"."wr_return_quantity") as "web_qty"
  FROM
      "item" as "store_returns_item_item"
      LEFT OUTER JOIN "web_returns" as "web_returns_web_returns" on
  "store_returns_item
  …
  ty") ) / 3 ) * 100 as
  "web_pct",
      (( "sparkling"."store_qty" + "thoughtful"."catalog_qty" ) +
  "sparkling"."web_qty") / 3 as "avg_qty"
  FROM
      "thoughtful"
      INNER JOIN "sparkling" on "thoughtful"."store_returns_item_item_sk" =
  "sparkling"."store_returns_item_item_sk"
  WHERE
      "sparkling"."store_qty" > 0 and "sparkling"."web_qty" > 0

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8
  ORDER BY
      "sparkling"."store_returns_item_item_id" asc nulls first,
      "sparkling"."store_qty" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query84.preql`

  ```text
  Unable to import '.\store_returns.preql': [Errno 2] No such
  file or directory: '.\\store_returns.preql'. Did you mean: raw.store_returns?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 82 (char 81). Re-issue the call with valid JSON arguments.
  ```
- `trilogy unit query86.preql`

  ```text
  Mocking not implemented for datatype BIGINT
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query87.preql`

  ```text
  HAVING references 'local.has_catalog', which is not in the
  SELECT projection (line 31). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ..., --local.has_catalog`;
  (b) move the filter to WHERE — for an aggregate condition on a non-output
  grain, write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy run query89.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 128:     ( INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.sales_price> - (INVALID_REF...
                                                 ^
  [SQL:
  WITH
  abundant as (
  SELECT
      "store_sales_store_sales"."ss_item_sk" as "store_sales_item_item_sk",
      "store_sales_store_sales"."ss_sold_date_sk" as
  "store_sales_date_dim_date_sk",
      "store_sales_store_sales"."ss_store_sk" as "store_sales_store_store_sk"
  FROM
      "store_sales" as "store_sales_store_sales"
  GROUP BY
      1,
      2,
      3),
  thoughtful as
  …
  _sales.sales_price> / 12) ) asc,
      "juicy"."store_sales_store_store_name" asc,
      "juicy"."store_sales_item_category" asc,
      "juicy"."store_sales_item_class" asc,
      "juicy"."store_sales_item_brand" asc,
      "juicy"."store_sales_store_company_name" asc,
      INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.date_dim.moy> asc,
      INVALID_REFERENCE_BUG_<Missing source reference to store_sales.sales_price>
  asc,
      ( INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.sales_price> / 12 ) asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query89.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 71:     ( INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.sales_price> - INVALID_REFE...
                                                ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "store_sales_date_dim_date_dim"."d_moy" as "store_sales_date_dim_moy",
      "store_sales_item_item"."i_brand" as "store_sales_item_brand",
      "store_sales_item_item"."i_category" as "store_sales_item_category",
      "store_sales_item_item"."i_class" as "store_sales_item_class",
      "store_sales_store_store"."s_company_name" a
  …
  ID_REFERENCE_BUG_<Missing source reference to
  store_sales.sales_price> ) asc,
      "abundant"."store_sales_store_store_name" asc,
      "abundant"."store_sales_item_category" asc,
      "abundant"."store_sales_item_class" asc,
      "abundant"."store_sales_item_brand" asc,
      "abundant"."store_sales_store_company_name" asc,
      "abundant"."month_of_year" asc,
      INVALID_REFERENCE_BUG_<Missing source reference to store_sales.sales_price>
  asc,
      INVALID_REFERENCE_BUG_<Missing source reference to store_sales.sales_price>
  asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query89.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 68:     INVALID_REFERENCE_BUG_<Missing source reference to
  store_sales.sales_price> = 0 or abs...
                                              ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "store_sales_date_dim_date_dim"."d_moy" as "store_sales_date_dim_moy",
      "store_sales_item_item"."i_brand" as "store_sales_item_brand",
      "store_sales_item_item"."i_category" as "store_sales_item_category",
      "store_sales_item_item"."i_class" as "store_sales_item_class",
      "store_sales_store_store"."s_company_name" as
  "stor
  …
  ales_price>
  = 0 or abs(( "abundant"."diff_from_avg" ) / INVALID_REFERENCE_BUG_<Missing
  source reference to store_sales.sales_price>) > 0.1

  ORDER BY
      "abundant"."diff_from_avg" asc,
      "abundant"."store_sales_store_store_name" asc,
      "abundant"."store_sales_item_category" asc,
      "abundant"."store_sales_item_class" asc,
      "abundant"."store_sales_item_brand" asc,
      "abundant"."store_sales_store_company_name" asc,
      "abundant"."month_of_year" asc,
      "abundant"."total" asc,
      "abundant"."monthly_avg" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw.catalog_returns:catalog_returns select catalog_returns.refunded_customer.customer_demographics.marital_status, catalog_returns.refun…hics.marital_status, catalog_returns.refunded_customer.customer_demographics.education_status) in (('M','Unknown'),('W','Advanced Degree')) limit 20;`

  ```text
  Tuple must have same type for all elements
  ```
- `trilogy run --import raw/reason select sk, desc where desc = 'reason 28' limit 20;`

  ```text
  Value 'reason 28' is not valid for enum field 'local.desc'.
  Allowed values: 'Did not get it on time', 'Package was damaged', 'Stopped
  working'.
  ```
- `trilogy run query97.preql duckdb`

  ```text
  Duplicate select output for
  catalog_sales.bill_customer.customer_sk; Line: 7
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.store_returns as store_returns;

# Filter: returns to stores in Tennessee in year 2000
# Find customers…tore_returns.customer.customer_id
order by
    store_returns.customer.customer_id
having
    cust_store_return > 1.2 * avg_return_by_store
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('HAVING', 'having') at line 17, column 1.
  Expected one of:
          * LIMIT
          * _TERMINATOR
          * MERGE

  Location:
  ..._returns.customer.customer_id ??? having     cust_store_return >...

  Write stats: received 656 chars / 656 bytes; tail: …'    cust_store_return >
  1.2 * avg_return_by_store\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql --content import raw.store_returns as store_returns;

# Filter: returns to stores in Tennessee in year 2000
# Find customers…tore_returns.customer.customer_id
order by
    store_returns.customer.customer_id
limit 100
having
    cust_store_return > 1.2 * avg_return_by_store;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'having\n    ') at line 18, column
  1.
  Expected one of:
          * MERGE
          * _TERMINATOR
  Previous tokens: [Token('__ANON_10', '100')]

  Location:
  ...ustomer.customer_id limit 100 ??? having     cust_store_return >...

  Write stats: received 656 chars / 656 bytes; tail: …'00\\nhaving\\n
  cust_store_return > 1.2 * avg_return_by_store;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql --content import raw.store_returns as store_returns;

# Filter: returns to stores in Tennessee in year 2000
# Find customers…tore_returns.customer.customer_id
order by
    store_returns.customer.customer_id
having
    cust_store_return > 1.2 * avg_return_by_store
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('HAVING', 'having') at line 17, column 1.
  Expected one of:
          * MERGE
          * _TERMINATOR
          * LIMIT

  Location:
  ..._returns.customer.customer_id ??? having     cust_store_return >...

  Write stats: received 656 chars / 656 bytes; tail: …'    cust_store_return >
  1.2 * avg_return_by_store\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

# Merge sold_date concepts so catal…1, 0) as thu_ratio,
    fri_2002 / nullif(fri_2001, 0) as fri_ratio,
    sat_2002 / nullif(sat_2001, 0) as sat_ratio
order by week_seq asc
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 15, column 42.
  Expected one of:
          * _TERMINATOR

  Location:
  ...sales <- combined_daily_sales ??? by catalog_sales.sold_date.dat...

  Write stats: received 2982 chars / 2982 bytes; tail: …'f(sat_2001, 0) as
  sat_ratio\\norder by week_seq asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 92, column 32.
  Expected one of:
          * SELECT

  Location:
   and catalog_ratio > web_ratio ??? ;  select     store_sales.c...

  Write stats: received 4888 chars / 4888 bytes; tail: …' first,\r\\n
  preferred_cust_flag asc nulls first\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 50, column 32.
  Expected one of:
          * SELECT

  Location:
   and catalog_ratio > web_ratio ??? ;  select     store_sales.c...

  Write stats: received 2617 chars / 2617 bytes; tail: …' first,\r\\n
  preferred_cust_flag asc nulls first\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run - duckdb --import raw.store_sales:store_sales`

  ```text
  --> 2:82
    |
  2 | select store_sales.customer.customer_id, count(store_sales.ticket_number)
  as cnt by store_sales.customer.customer_sk limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...e_sales.ticket_number) as cnt ??? by store_sales.customer.custom...
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…turns.return_amt) as returns,
    sum(store_sales.net_profit) - sum(store_returns.net_loss) as profit
order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 12, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
   (matched by item and ticket) ??? auto store_sales_sum <- sum(st...

  Write stats: received 1058 chars / 1058 bytes; tail: …'s) as profit\\norder by
  channel, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…profit - wr_loss as total_profit
    where web_sales.sold_date.date between start_date and end_date
)
order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 15, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
   store_sales.store.store_sk   ??? where store_sales.date_dim.dat...

  Write stats: received 3810 chars / 3810 bytes; tail: …'d end_date\\n)\\norder
  by channel, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --debug --param zips=39 --import raw.store_sales:store_sales select substring(store_sales.store.zip,1,2) as pref, count(store_sales.customer.customer_sk ? store_sales.customer.preferred_cust_flag = 'Y') by substring(store_sales.customer.customer_address.zip,1,2) as cnt_by_pref_prefix limit 10;`

  ```text
  …
   ??? by substring(store_sales.custo...
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\pest_backend.p
  y", line 321, in parse_pest
      tree = parse_trilogy_syntax_tuple(text)
  ValueError:  --> 2:155
    |
  2 | select substring(store_sales.store.zip,1,2) as pref,
  count(store_sales.customer.customer_sk ?
  store_sales.customer.preferred_cust_flag = 'Y') by
  substring(store_sales.customer.customer_address.zip,1,2) as cnt_by_pref_prefix
  limit 10;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF

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

  …
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.customer as customer;

merge …g as preferred_cust_flag
order by customer_id nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 16, column 35.
  Expected one of:
          * SELECT

  Location:
  ...d web_revenue_2001 is not null ??? ;  select     store_sales.cust...

  Write stats: received 1303 chars / 1303 bytes; tail: …'ame nulls first,
  preferred_cust_flag nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.customer_sk …as preferred_cust_flag
order by customer_code nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 14, column 23.
  Expected one of:
          * SELECT

  Location:
  ...001 > 0   and web_rev_2001 > 0 ??? ;  # Growth ratios: treat zero...

  Write stats: received 1607 chars / 1607 bytes; tail: …'ame nulls first,
  preferred_cust_flag nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.customer_sk …as preferred_cust_flag
order by customer_code nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 14, column 1.
  Expected one of:
          * SELECT

  Location:
  ...1 > 0   and web_rev_2001 > 0  ??? auto store_growth <- coalesce(...

  Write stats: received 1408 chars / 1408 bytes; tail: …'ame nulls first,
  preferred_cust_flag nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.customer_sk …as preferred_cust_flag
order by customer_code nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 13, column 1.
  Expected one of:
          * SELECT

  Location:
  ...001 > 0 and web_rev_2001 > 0  ??? auto store_growth <- coalesce(...

  Write stats: received 1406 chars / 1406 bytes; tail: …'ame nulls first,
  preferred_cust_flag nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

…   count(ws_nov_cnt) as total_sales_num
order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n') at line 64, column 1.
  Expected one of:
          * _TERMINATOR
          * MERGE
          * ORDER
          * COMMA
          * LIMIT
          * METADATA
          * HAVING
          * WHERE
  Previous tokens: [Token('IDENTIFIER', 'total_sales_num')]

  Location:
  ...s_nov_cnt) as total_sales_num ??? union # Catalog sales Nov 2001...

  Write stats: received 4372 chars / 4372 bytes; tail: …'t, class_id nulls first,
  category_id nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql -e -c import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

…s, count(ws_nov_cnt) as total_sales_num
order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n') at line 40, column 1.
  Expected one of:
          * HAVING
          * MERGE
          * ORDER
          * LIMIT
          * _TERMINATOR
          * WHERE
          * COMMA
          * METADATA
  Previous tokens: [Token('IDENTIFIER', 'total_sales_num')]

  Location:
  ...s_nov_cnt) as total_sales_num ??? union where cs_ct_items > 0 an...

  Write stats: received 3359 chars / 3359 bytes; tail: …'t, class_id nulls first,
  category_id nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import catalog_sales as cs;

# Filter to items in Sports/Books/Home categories sold between 1999-02-22 and 1999-03….class asc nulls first,
    cs.item.item_id asc nulls first,
    cs.item.item_desc asc nulls first,
    pct_of_class_total asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
   price per item (sum by item) ??? auto item_total_ext_sales_pric...

  Write stats: received 1003 chars / 1003 bytes; tail: …'lls first,\\n
  pct_of_class_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import catalog_sales as cs;

# Filter to items in Sports/Books/Home categories sold between 1999-02-22 and 1999-03….class asc nulls first,
    cs.item.item_id asc nulls first,
    cs.item.item_desc asc nulls first,
    pct_of_class_total asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
   price per item (sum by item) ??? auto item_total_ext_sales_pric...

  Write stats: received 1003 chars / 1003 bytes; tail: …'lls first,\\n
  pct_of_class_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import catalog_sales as cs;

# Filter to items in Sports/Books/Home categories sold between 1999-02-22 and 1999-03….class asc nulls first,
    cs.item.item_id asc nulls first,
    cs.item.item_desc asc nulls first,
    pct_of_class_total asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
   price per item (sum by item) ??? auto item_total_ext_sales_pric...

  Write stats: received 1003 chars / 1003 bytes; tail: …'lls first,\\n
  pct_of_class_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql -e -c import catalog_sales as cs;

# Filter to items in Sports/Books/Home categories sold between 1999-02-22 and 1999-03-24
….class asc nulls first,
    cs.item.item_id asc nulls first,
    cs.item.item_desc asc nulls first,
    pct_of_class_total asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
   price per item (sum by item) ??? auto item_total_ext_sales_pric...

  Write stats: received 1003 chars / 1003 bytes; tail: …'lls first,\\n
  pct_of_class_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import raw.catalog_sales as cs;

# Filter to items in Sports/Books/Home categories sold between 1999-02-22 and 199….class asc nulls first,
    cs.item.item_id asc nulls first,
    cs.item.item_desc asc nulls first,
    pct_of_class_total asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...l catalog sales of that item) ??? auto item_total <- sum(cs.ext_...

  Write stats: received 1030 chars / 1030 bytes; tail: …'lls first,\\n
  pct_of_class_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query21.preql --content import inventory as inv;

# Restrict to items whose current price is between 0.99 and 1.49
where inv.item.current_…after_qty / before_qty::numeric <= 3.0/2.0
order by
    inv.warehouse.warehouse_name asc nulls first,
    inv.item.item_id asc nulls first
limit 100;`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('__ANON_19', '1.49')]

  Location:
  ...but not including 2000-03-11) ??? auto before_qty <- sum(inv.qua...

  Write stats: received 1072 chars / 1072 bytes; tail: …'nulls first,\\n
  inv.item.item_id asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query21.preql --content import inventory as inv;
where inv.item.current_price between 0.99 and 1.49
auto before_qty <- sum(inv.quantity_on…after_qty / before_qty::numeric <= 3.0/2.0
order by
    inv.warehouse.warehouse_name asc nulls first,
    inv.item.item_id asc nulls first
limit 100;`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 3, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('__ANON_19', '1.49')]

  Location:
  ...t_price between 0.99 and 1.49 ??? auto before_qty <- sum(inv.qua...

  Write stats: received 663 chars / 663 bytes; tail: …'nulls first,\\n
  inv.item.item_id asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…web' as channel,
    sum(web_sales.quantity * web_sales.list_price) as total_sales
order by last_name, first_name, total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
  ...<- store_sales.item.item_sk   ??? where store_sales.date_dim.yea...

  Write stats: received 1928 chars / 1928 bytes; tail: …'by last_name,
  first_name, total_sales nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…st_name,
    sum(catalog_sales.quantity * catalog_sales.list_price) as total_sales
order by last_name, first_name, total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('HAVING', 'having') at line 7, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
  ...<- store_sales.item.item_sk   ??? having count(store_sales.ticke...

  Write stats: received 1415 chars / 1415 bytes; tail: …'by last_name,
  first_name, total_sales nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…tore_sales.customer.customer_sk,
    sum(store_sales.quantity * store_sales.sales_price) as customer_lifetime_total
order by customer_lifetime_total;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
  ...<- store_sales.item.item_sk   ??? where store_sales.date_dim.yea...

  Write stats: received 717 chars / 717 bytes; tail: …'as
  customer_lifetime_total\\norder by customer_lifetime_total;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…sk, store_sales.date_dim.date_sk) where store_sales.date_dim.year between 2000 and 2003;

select store_sales.item.item_sk
having item_sale_dates > 4;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 121.
  Expected one of:
          * _TERMINATOR

  Location:
  ...store_sales.date_dim.date_sk) ??? where store_sales.date_dim.yea...

  Write stats: received 450 chars / 450 bytes; tail: …'\\nselect
  store_sales.item.item_sk\\nhaving item_sale_dates > 4;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…s.date_dim.date_sk) by (substring(store_sales.item.item_desc,1,30), store_sales.item.item_sk) where store_sales.date_dim.year between 2000 and 2003;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 6, column 135.
  Expected one of:
          * _TERMINATOR

  Location:
  ...0), store_sales.item.item_sk) ??? where store_sales.date_dim.yea...

  Write stats: received 388 chars / 388 bytes; tail: …'_sk) where
  store_sales.date_dim.year between 2000 and 2003;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…s.date_dim.date_sk) by (store_sales.item.item_sk, substring(store_sales.item.item_desc,1,30)) where store_sales.date_dim.year between 2000 and 2003;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 130.
  Expected one of:
          * _TERMINATOR

  Location:
  ...e_sales.item.item_desc,1,30)) ??? where store_sales.date_dim.yea...

  Write stats: received 431 chars / 431 bytes; tail: …'30)) where
  store_sales.date_dim.year between 2000 and 2003;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…_desc, 1, 30));

auto frequent_item_sk <- store_sales.item.item_sk
where store_sales.date_dim.year between 2000 and 2003
having item_pair_count > 4;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 1.
  Expected one of:
          * _TERMINATOR

  Location:
  ...k <- store_sales.item.item_sk ??? where store_sales.date_dim.yea...

  Write stats: received 467 chars / 467 bytes; tail: …'_dim.year between 2000 and
  2003\\nhaving item_pair_count > 4;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…esc, 1, 30)) where store_sales.date_dim.year between 2000 and 2003;

auto frequent_item_sk <- store_sales.item.item_sk
  having item_pair_count > 4;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 6, column 137.
  Expected one of:
          * _TERMINATOR

  Location:
  ...sales.item.item_desc, 1, 30)) ??? where store_sales.date_dim.yea...

  Write stats: received 469 chars / 469 bytes; tail: …'k <-
  store_sales.item.item_sk\\n  having item_pair_count > 4;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…list_price) as total_sales
order by catalog_sales.bill_customer.last_name, catalog_sales.bill_customer.first_name, total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 112.
  Expected one of:
          * _TERMINATOR

  Location:
  ...re_sales.customer.customer_sk ??? where store_sales.date_dim.yea...

  Write stats: received 1379 chars / 1379 bytes; tail:
  …'bill_customer.first_name, total_sales nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…e store_sales.date_dim.year between 2000 and 2003
select store_sales.item.item_sk as freq_item_sk,
    --item_pair_count
having item_pair_count > 4;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 112.
  Expected one of:
          * _TERMINATOR

  Location:
  ...re_sales.customer.customer_sk ??? where store_sales.date_dim.yea...

  Write stats: received 821 chars / 821 bytes; tail: …'_item_sk,\\n
  --item_pair_count\\nhaving item_pair_count > 4;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…equent items
select store_sales.item.item_sk,
    --item_pair_count
where store_sales.date_dim.year between 2000 and 2003
having item_pair_count > 4;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 112.
  Expected one of:
          * _TERMINATOR

  Location:
  ...re_sales.customer.customer_sk ??? where store_sales.date_dim.yea...

  Write stats: received 789 chars / 789 bytes; tail: …'e_dim.year between 2000
  and 2003\\nhaving item_pair_count > 4;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…list_price) as total_sales
order by catalog_sales.bill_customer.last_name, catalog_sales.bill_customer.first_name, total_sales nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...catalog_sales.item.item_sk in ??? (select store_sales.item.item_...

  Write stats: received 1370 chars / 1370 bytes; tail:
  …'bill_customer.first_name, total_sales nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query28.preql --content import raw.store_sales as store_sales;

# Bucket 1: ticket quantity 0 to 5, and (list_price 8-18 OR coupon_amt 459…ket6_avg_list_price,
    b6_list_price_count as bucket6_list_price_count,
    b6_unique_list_price_count as bucket6_unique_list_price_count
limit 1;
`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 5, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...<- store_sales.list_price     ??? where store_sales.quantity bet...

  Write stats: received 4167 chars / 4167 bytes; tail: …'ist_price_count as
  bucket6_unique_list_price_count\\nlimit 1;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query28.preql -e -c import raw.store_sales as store_sales;

auto b1_rows <- store_sales.list_price
    where store_sales.quantity between …cket6_avg_list_price,
    b6_list_price_count as bucket6_list_price_count,
    b6_unique_list_price_count as bucket6_unique_list_price_count
limit 1;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 4, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...<- store_sales.list_price     ??? where store_sales.quantity bet...

  Write stats: received 3508 chars / 3508 bytes; tail: …'list_price_count as
  bucket6_unique_list_price_count\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query28.preql --content import raw.store_sales as store_sales;
auto b1_rows <- store_sales.list_price where store_sales.quantity between 0…ce as bucket6_avg_list_price,
b6_list_price_count as bucket6_list_price_count,
b6_unique_list_price_count as bucket6_unique_list_price_count
limit 1;`

  ```text
  refused to write 'query28.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 2, column 40.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ows <- store_sales.list_price ??? where store_sales.quantity bet...

  Write stats: received 3231 chars / 3231 bytes; tail: …'list_price_count as
  bucket6_unique_list_price_count\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select count(*),min(date_dim.date),max(date_dim.date)`

  ```text
  --> 2:14
    |
  2 | select count(*),min(date_dim.date),max(date_dim.date);
    |              ^---
    |
    = expected access_chain
  Location:
   as store_sales; select count( ??? *),min(date_dim.date),max(date...
  ```
- `trilogy file write query32.preql --content import raw.catalog_sales as catalog_sales;

# Filter: manufacturer 977, date window 2000-01-27 to 2000-04-26
where…em_sk;

# Total ext_discount_amt for lines exceeding 1.3x the item's average
select sum(catalog_sales.ext_discount_amt) as total_discount
limit 100;
`

  ```text
  refused to write 'query32.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...r item across the same window ??? auto avg_discount_per_item <-

  Write stats: received 547 chars / 547 bytes; tail:
  …'atalog_sales.ext_discount_amt) as total_discount\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query32.preql --content import raw.catalog_sales as catalog_sales;

where catalog_sales.item.manufact_id = 977
  and catalog_sales.sold_da…les.item.item_sk;

select sum(catalog_sales.ext_discount_amt ? catalog_sales.ext_discount_amt > 1.3 * avg_item_discount) as total_discount
limit 100;`

  ```text
  refused to write 'query32.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-04-26'::date  ??? auto avg_item_discount <- avg(...

  Write stats: received 397 chars / 397 bytes; tail: …'_amt > 1.3 *
  avg_item_discount) as total_discount\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run -`

  ```text
  --> 4:1
    |
  4 | auto avg_item_discount <- avg(catalog_sales.ext_discount_amt) by
  catalog_sales.item.item_sk;
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...::date and '2000-04-26'::date ??? auto avg_item_discount <- avg(...
  ```
- `trilogy file write query34.preql -e -c import raw.store_sales as store_sales;

# Compute line items per ticket (by customer per ticket)
auto line_items_per_t…r.salutation asc nulls first,
    store_sales.customer.preferred_cust_flag desc nulls first,
    store_sales.ticket_number asc nulls first
limit 200;`

  ```text
  refused to write 'query34.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 7, column 137.
  Expected one of:
          * _TERMINATOR

  Location:
  ...phics.vehicle_count::numeric) ??? by store_sales.customer.custom...

  Write stats: received 1435 chars / 1435 bytes; tail: …'st,\\n
  store_sales.ticket_number asc nulls first\\nlimit 200;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query34.preql -e -c import raw.store_sales as store_sales;

auto line_items_per_ticket <- count(store_sales.ticket_number) by store_sales.…r.salutation asc nulls first,
    store_sales.customer.preferred_cust_flag desc nulls first,
    store_sales.ticket_number asc nulls first
limit 200;`

  ```text
  refused to write 'query34.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 5, column 161.
  Expected one of:
          * _TERMINATOR

  Location:
  ...phics.vehicle_count::numeric) ??? by store_sales.customer.custom...

  Write stats: received 1349 chars / 1349 bytes; tail: …'st,\\n
  store_sales.ticket_number asc nulls first\\nlimit 200;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql --content import raw.store_sales as store_sales;

# Filter: store sales in year 2001 at stores in TN
where store_sales.date_…nt_rank
order by
    h_level desc nulls first,
    case when h_level = 0 then category end nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...gory, class), (category), ()  ??? auto gross_margin <- sum(store...

  Write stats: received 2028 chars / 2028 bytes; tail: …'d nulls first,\\n
  within_parent_rank nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql --content import raw.store_sales as store_sales;

where store_sales.date_dim.year = 2001
  and store_sales.store.state = 'TN…nt_rank
order by
    h_level desc nulls first,
    case when h_level = 0 then category end nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...ore_sales.store.state = 'TN'  ??? auto g_cat <- grouping(store_s...

  Write stats: received 1130 chars / 1130 bytes; tail: …'d nulls first,\\n
  within_parent_rank nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql -e -c import raw.store_sales as store_sales;

where store_sales.date_dim.year = 2001
  and store_sales.store.state = 'TN'

a…nt_rank
order by
    h_level desc nulls first,
    case when h_level = 0 then category end nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...ore_sales.store.state = 'TN'  ??? auto g_cat <- grouping(store_s...

  Write stats: received 1130 chars / 1130 bytes; tail: …'d nulls first,\\n
  within_parent_rank nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql -e -c import raw.store_sales as store_sales;

where store_sales.date_dim.year = 2001
  and store_sales.store.state = 'TN'

a…nt_rank
order by
    h_level desc nulls first,
    case when h_level = 0 then category end nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...ore_sales.store.state = 'TN'  ??? auto g_cat <- grouping(store_s...

  Write stats: received 1130 chars / 1130 bytes; tail: …'d nulls first,\\n
  within_parent_rank nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql -e -c import raw.store_sales as store_sales;

where store_sales.date_dim.year = 2001
  and store_sales.store.state = 'TN'

a…t_rank
order by
    h_level desc nulls first,
    case when h_level = 0 then category end nulls first,
    within_parent_rank nulls first
limit 100;
`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...ore_sales.store.state = 'TN'  ??? auto g_cat <- grouping(store_s...

  Write stats: received 1131 chars / 1131 bytes; tail: …' nulls first,\\n
  within_parent_rank nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run query36.preql`

  ```text
  --> 6:1
    |
  6 | auto g_cat <- grouping(store_sales.item.category) by rollup
  store_sales.item.category, store_sales.item.class;
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ore_sales.store.state = 'TN'  ??? auto g_cat <- grouping(store_s...
  ```
- `trilogy run query36.preql`

  ```text
  --> 6:1
    |
  6 | auto gross_margin <- sum(store_sales.net_profit) /
  nullif(sum(store_sales.ext_sales_price), 0) by rollup
  store_sales.item.category, store_sales.item.class;
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ore_sales.store.state = 'TN'  ??? auto gross_margin <- sum(store...
  ```
- `trilogy run --import raw.item:item select item.manufact_id, count(item.item_sk) as cnt where item.current_price between 68 and 98 group by item.manufact_id order by item.manufact_id limit 20;`

  ```text
  --> 2:96
    |
  2 | select item.manufact_id, count(item.item_sk) as cnt where
  item.current_price between 68 and 98 group by item.manufact_id order by
  item.manufact_id limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...rrent_price between 68 and 98 ??? group by item.manufact_id orde...
  ```
- `trilogy run --import raw.item:item select max(item.manufact_id) as mx, min(item.manufact_id) as mn from item;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ..., min(item.manufact_id) as mn ??? from item;
  ```
- `trilogy run --import raw.item:item select item.manufact_id, count(item.item_sk) as cnt where item.manufact_id in (677,940,694,808) group by item.manufact_id order by item.manufact_id limit 20;`

  ```text
  --> 2:97
    |
  2 | select item.manufact_id, count(item.item_sk) as cnt where item.manufact_id
  in (677,940,694,808) group by item.manufact_id order by item.manufact_id limit
  20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ufact_id in (677,940,694,808) ??? group by item.manufact_id orde...
  ```
- `trilogy file write query38.preql --content import raw.catalog_sales as catalog_sales;
import raw.store_sales as store_sales;
import raw.web_sales as web_sale… 2000

select count_distinct(
  store_sales.customer.last_name,
  store_sales.customer.first_name,
  store_sales.date_dim.date
) as result
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 15, column 33.
  Expected one of:
          * RPAR

  Location:
  ...store_sales.customer.last_name ??? ,   store_sales.customer.first...

  Write stats: received 693 chars / 693 bytes; tail: …'rst_name,\\n
  store_sales.date_dim.date\\n) as result\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query38.preql --content import raw.catalog_sales as catalog_sales;
import raw.store_sales as store_sales;
import raw.web_sales as web_sale…e.year = 2000

select count_distinct(store_sales.customer.last_name, store_sales.customer.first_name, store_sales.date_dim.date) as result
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 14, column 53.
  Expected one of:
          * RPAR

  Location:
  ...store_sales.customer.last_name ??? , store_sales.customer.first_n...

  Write stats: received 685 chars / 685 bytes; tail: …'.first_name,
  store_sales.date_dim.date) as result\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query38.preql --content import raw.catalog_sales as catalog_sales;
import raw.store_sales as store_sales;
import raw.web_sales as web_sale…ame || '|' || store_sales.customer.first_name || '|' || store_sales.date_dim.date::string);

select count_distinct(customer_key) as result
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 14, column 1.
  Expected one of:
          * SELECT

  Location:
  ..._sales.sold_date.year = 2000  ??? auto customer_key <- (store_sa...

  Write stats: received 749 chars / 749 bytes; tail: …');\\n\\nselect
  count_distinct(customer_key) as result\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query38.preql --content import raw.catalog_sales as catalog_sales;
import raw.store_sales as store_sales;
import raw.web_sales as web_sale…sale_date
where web_sales.sold_date.year = 2000;

merge cs_pairs into ~ss_pairs;
merge ws_pairs into ~ss_pairs;

select count(*) as result
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'store_sales.customer.last_name ')
  at line 6, column 3.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...s;  auto ss_pairs <- select   ??? store_sales.customer.last_name...

  Write stats: received 847 chars / 847 bytes; tail: …'_pairs into
  ~ss_pairs;\\n\\nselect count(*) as result\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.item:item select item.size as sz group by item.size`

  ```text
  --> 2:24
    |
  2 | select item.size as sz group by item.size;
    |                        ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
   item; select item.size as sz ??? group by item.size;
  ```
- `trilogy file write query44.preql --content import store_sales as store_sales;

# Average net profit per item at store 4
# (store.store_sk is an enum, and sto…est.product_name as best_performer,
    item.product_name as worst_performer
limit 100;

# Wait - this approach needs rethinking. Let me reconsider.
`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'item.item_sk') at line 25,
  column 5.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...o ranked_items <-  select     ??? item.item_sk,     item.product...

  Write stats: received 1874 chars / 1874 bytes; tail: …'# Wait - this approach
  needs rethinking. Let me reconsider.\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query45.preql --content import raw.web_sales as web_sales;

where web_sales.sold_date.year = 2001
  and web_sales.sold_date.qoy = 2
  and …ice) as total_sales_price
order by
  web_sales.bill_customer.customer_address.zip asc,
  web_sales.bill_customer.customer_address.city asc
limit 100;`

  ```text
  refused to write 'query45.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
   or web_sales.item.item_sk in ??? (       select web_sales.item....

  Write stats: received 683 chars / 683 bytes; tail:
  …'web_sales.bill_customer.customer_address.city asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.item:item select item.item_id where item.manufact_id in (select item.manufact_id where item.item_sk in (2,3,5,7,11,13,17,19,23,29));`

  ```text
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside
  parens)? Trilogy does not support subqueries — joins are auto-resolved from
  dotted paths. To filter on a value that lives on a related dimension, reference
  its dot-path directly. Example: instead of `where ss.store_id in (select
  store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ..._id where item.manufact_id in ??? (select item.manufact_id where...
  ```
- `trilogy file write query47.preql -e -c import raw.store_sales as store_sales;

# Monthly price total per (category, brand, store, company)
auto monthly_total…al - avg_monthly asc,
    category asc,
    brand asc,
    store_name asc,
    store_company_name asc,
    year asc,
    month_of_year asc
limit 100;`

  ```text
  refused to write 'query47.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
  ...ly) / avg_monthly > 0.1 order ??? by     this_month_total - avg_...

  Write stats: received 2308 chars / 2308 bytes; tail: …'any_name asc,\\n    year
  asc,\\n    month_of_year asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query48.preql -e -c import raw.store_sales as store_sales;

# Profile 1: Married, '4 yr Degree', sales_price 100-150, billing address US i…ere store_sales.date_dim.year = 2000
  and (profile1_match or profile2_match or profile3_match)

select sum(store_sales.quantity) as total_quantity;
`

  ```text
  refused to write 'query48.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 6, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...hics.marital_status = 'M'     ??? and store_sales.customer.custo...

  Write stats: received 1843 chars / 1843 bytes; tail: …'match)\\n\\nselect
  sum(store_sales.quantity) as total_quantity;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query48.preql -e -c import raw.store_sales as store_sales;

auto profile1_match <- store_sales.customer.customer_demographics.marital_stat…here store_sales.date_dim.year = 2000
  and (profile1_match or profile2_match or profile3_match)
select sum(store_sales.quantity) as total_quantity;
`

  ```text
  refused to write 'query48.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 4, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...hics.marital_status = 'M'     ??? and store_sales.customer.custo...

  Write stats: received 1452 chars / 1452 bytes; tail: …'_match)\\nselect
  sum(store_sales.quantity) as total_quantity;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query48.preql --content import raw.store_sales as store_sales;

auto profile1_match <- store_sales.customer.customer_demographics.marital_…where store_sales.date_dim.year = 2000
  and (profile1_match or profile2_match or profile3_match)
select sum(store_sales.quantity) as total_quantity;`

  ```text
  refused to write 'query48.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 4, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...hics.marital_status = 'M'     ??? and store_sales.customer.custo...

  Write stats: received 1451 chars / 1451 bytes; tail: …'3_match)\\nselect
  sum(store_sales.quantity) as total_quantity;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query48.preql --content import raw.store_sales as store_sales;

auto profile1_match <- store_sales.customer.customer_demographics.marital_…where store_sales.date_dim.year = 2000
  and (profile1_match or profile2_match or profile3_match)
select sum(store_sales.quantity) as total_quantity;`

  ```text
  refused to write 'query48.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 3, column 88.
  Expected one of:
          * _TERMINATOR

  Location:
  ...graphics.marital_status = 'M' ??? and store_sales.customer.custo...

  Write stats: received 1391 chars / 1391 bytes; tail: …'3_match)\\nselect
  sum(store_sales.quantity) as total_quantity;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query48.preql --content import raw.store_sales as store_sales;

auto profile1_match <- store_sales.customer.customer_demographics.marital_…where store_sales.date_dim.year = 2000
  and (profile1_match or profile2_match or profile3_match)
select sum(store_sales.quantity) as total_quantity;`

  ```text
  refused to write 'query48.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 3, column 88.
  Expected one of:
          * _TERMINATOR

  Location:
  ...graphics.marital_status = 'M' ??? and store_sales.customer.custo...

  Write stats: received 1389 chars / 1389 bytes; tail: …'3_match)\\nselect
  sum(store_sales.quantity) as total_quantity;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query51.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Merge the item dimension so both fact…te,
    null as web_rt,
    store_running_total as store_rt
where store_sales.date_dim.year = 2000
order by item_sk, sale_date nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'union') at line 27, column 1.
  Expected one of:
          * _TERMINATOR
          * MERGE
          * ORDER
          * LIMIT
          * HAVING

  Location:
  ...b_sales.sold_date.year = 2000 ??? union select     store_sales.i...

  Write stats: received 1568 chars / 1568 bytes; tail: …'ar = 2000\\norder by
  item_sk, sale_date nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query51.preql -e -c import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Merge the item dimension so both fact tab…as item_sk,
    store_sales.date_dim.date as sale_date,
    0 as web_rt,
    store_running_total as store_rt
where store_sales.date_dim.year = 2000;
`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'web_sales.item.item_sk ') at line
  22, column 5.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...s auto combined <- select     ??? web_sales.item.item_sk as item...

  Write stats: received 1538 chars / 1538 bytes; tail: …'g_total as
  store_rt\\nwhere store_sales.date_dim.year = 2000;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run - duckdb --import raw.web_sales:web_sales --import raw.store_sales:store_sales`

  ```text
  --> 8:124
    |
  8 | auto web_running_max <- max(web_running_total) over (partition by
  web_sales.item.item_sk order by web_sales.sold_date.date rows between unbounded
  preceding and current row);
    |
  ^---
    |
    = expected ORDER_DIRECTION_LA, ORDERING_DIRECTION, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...r by web_sales.sold_date.date ??? rows between unbounded precedi...
  ```
- `trilogy file write query53.preql -e -c import raw.store_sales as store_sales;

# Profile 1: Books/Children/Electronics categories, specific classes and brand…y_total) / avg_quarterly_total > 0.1
order by
    avg_quarterly_total asc,
    per_quarter_total asc,
    store_sales.item.manufact_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 19, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...grouped by (manufact_id, qoy) ??? auto per_quarter_total <- sum(...

  Write stats: received 1502 chars / 1502 bytes; tail: …'r_total asc,\\n
  store_sales.item.manufact_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query53.preql -e -c import raw.store_sales as store_sales;

# Profile 1: Books/Children/Electronics categories, specific classes and brand…y_total) / avg_quarterly_total > 0.1
order by
    avg_quarterly_total asc,
    per_quarter_total asc,
    store_sales.item.manufact_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 19, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...grouped by (manufact_id, qoy) ??? auto per_quarter_total <- sum(...

  Write stats: received 1502 chars / 1502 bytes; tail: …'r_total asc,\\n
  store_sales.item.manufact_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run -`

  ```text
  --> 1:9
    |
  1 | select 1;
    |         ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  select 1 ??? ;
  ```
- `trilogy run -`

  ```text
  --> 2:24
    |
  2 | auto item_group_ids <- select item.color, item.item_id where item.color in
  ('slate', 'blanched', 'burnished');
    |                        ^---
    |
    = expected access_chain
  Location:
   item; auto item_group_ids <- ??? select item.color, item.item_i...
  ```
- `trilogy run -`

  ```text
  --> 6:1
    |
  6 | group by item_group
    | ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...em.item_sk) as items_in_group ??? group by item_group order by i...
  ```
- `trilogy run -`

  ```text
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside
  parens)? Trilogy does not support subqueries — joins are auto-resolved from
  dotted paths. To filter on a value that lives on a related dimension, reference
  its dot-path directly. Example: instead of `where ss.store_id in (select
  store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
   union in Trilogy select union ??? (   select store_sales.item.it...
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;

# Monthly catalog-sales net_paid totals per (category, brand, call_center, year, …thly_sales) asc nulls first,
    cs.item.category,
    cs.item.brand,
    cs.call_center.name,
    cs.sold_date.year,
    cs.sold_date.moy
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 13, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...ry, brand, call_center, year) ??? auto avg_per_yr <- avg(monthly...

  Write stats: received 1876 chars / 1876 bytes; tail: …'name,\\n
  cs.sold_date.year,\\n    cs.sold_date.moy\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;

auto monthly_total <- sum(cs.net_paid) by cs.item.category, cs.item.brand, cs.cal…thly_sales) asc nulls first,
    cs.item.category,
    cs.item.brand,
    cs.call_center.name,
    cs.sold_date.year,
    cs.sold_date.moy
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...00 and cs.sold_date.moy = 1)  ??? auto avg_per_yr <- avg(monthly...

  Write stats: received 1337 chars / 1337 bytes; tail: …'name,\\n
  cs.sold_date.year,\\n    cs.sold_date.moy\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;

# Monthly totals per (category, brand, call_center, year, month)
auto monthly_tot…thly_sales) asc nulls first,
    cs.item.category,
    cs.item.brand,
    cs.call_center.name,
    cs.sold_date.year,
    cs.sold_date.moy
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 12, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...hin (cat, brand, call_center) ??? auto avg_per_yr <- avg(monthly...

  Write stats: received 2111 chars / 2111 bytes; tail: …'name,\\n
  cs.sold_date.year,\\n    cs.sold_date.moy\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query57.preql --escapes --content import raw.catalog_sales as cs;

auto monthly_total <- sum(cs.net_paid) by cs.item.category, cs.item.bra…thly_sales) asc nulls first,
    cs.item.category,
    cs.item.brand,
    cs.call_center.name,
    cs.sold_date.year,
    cs.sold_date.moy
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...00 and cs.sold_date.moy = 1)  ??? auto avg_per_yr <- avg(monthly...

  Write stats: received 1338 chars / 1338 bytes; tail: …'name,\\n
  cs.sold_date.year,\\n    cs.sold_date.moy\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;
# Monthly totals per (category, brand, call_center, year, month)
auto monthly_tota…onth_total - avg_monthly_sales) asc nulls first, cs.item.category, cs.item.brand, cs.call_center.name, cs.sold_date.year, cs.sold_date.moy
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...hin (cat, brand, call_center) ??? auto avg_per_yr <- avg(monthly...

  Write stats: received 1476 chars / 1476 bytes; tail: …'_center.name,
  cs.sold_date.year, cs.sold_date.moy\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;
auto monthly_total <- sum(cs.net_paid) by cs.item.category, cs.item.brand, cs.call…onth_total - avg_monthly_sales) asc nulls first, cs.item.category, cs.item.brand, cs.call_center.name, cs.sold_date.year, cs.sold_date.moy
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...000 and cs.sold_date.moy = 1) ??? auto avg_per_yr <- avg(monthly...

  Write stats: received 1269 chars / 1269 bytes; tail: …'_center.name,
  cs.sold_date.year, cs.sold_date.moy\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;

auto monthly_total <- sum(cs.net_paid) by cs.item.category, cs.item.brand, cs.cal…onth_total - avg_monthly_sales) asc nulls first, cs.item.category, cs.item.brand, cs.call_center.name, cs.sold_date.year, cs.sold_date.moy
limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...00 and cs.sold_date.moy = 1)  ??? auto avg_per_yr <- avg(monthly...

  Write stats: received 1268 chars / 1268 bytes; tail: …'_center.name,
  cs.sold_date.year, cs.sold_date.moy\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as cs;
auto monthly_total <- sum(cs.net_paid) by cs.item.category, cs.item.brand, cs.call…onth_total - avg_monthly_sales) asc nulls first, cs.item.category, cs.item.brand, cs.call_center.name, cs.sold_date.year, cs.sold_date.moy limit 100;`

  ```text
  refused to write 'query57.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 4, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...000 and cs.sold_date.moy = 1) ??? auto avg_per_yr <- avg(monthly...

  Write stats: received 1263 chars / 1263 bytes; tail: …'_center.name,
  cs.sold_date.year, cs.sold_date.moy limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query58.preql --content import raw.item as i;
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

m…alog_pct,
  web_total,
  web_total / avg_3way * 100 as web_pct,
  avg_3way
order by i.item_id asc nulls first, store_total asc nulls first
limit 100;`

  ```text
  refused to write 'query58.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 24, column 68.
  Expected one of:
          * SELECT

  Location:
  ..._total and 1.1 * catalog_total ??? ;  select   i.item_id,   store...

  Write stats: received 1375 chars / 1375 bytes; tail: …'m_id asc nulls first,
  store_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query58.preql --content import raw.item as i;
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

m…og_pct,
  web_total,
  (web_total / avg_3way) * 100 as web_pct,
  avg_3way
order by i.item_id asc nulls first, store_total asc nulls first
limit 100;`

  ```text
  refused to write 'query58.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 24, column 68.
  Expected one of:
          * SELECT

  Location:
  ..._total and 1.1 * catalog_total ??? ;  select   i.item_id,   store...

  Write stats: received 1381 chars / 1381 bytes; tail: …'m_id asc nulls first,
  store_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query58.preql -e -c import raw.item as i;
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

merge…og_pct,
  web_total,
  (web_total / avg_3way) * 100 as web_pct,
  avg_3way
order by i.item_id asc nulls first, store_total asc nulls first
limit 100;`

  ```text
  refused to write 'query58.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 24, column 68.
  Expected one of:
          * SELECT

  Location:
  ..._total and 1.1 * catalog_total ??? ;  select   i.item_id,   store...

  Write stats: received 1381 chars / 1381 bytes; tail: …'m_id asc nulls first,
  store_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.store_name, store_sales.store.store_id, store_sales.date_dim.week_seq, store_sales.date_dim.year, sum(store_sales.ext_sales_price) as total_sales where store_sales.date_dim.year in (2001,2002) group by 1,2,3,4 order by 1,2,3 limit 20;`

  ```text
  --> 2:219
    |
  2 | select store_sales.store.store_name, store_sales.store.store_id,
  store_sales.date_dim.week_seq, store_sales.date_dim.year,
  sum(store_sales.ext_sales_price) as total_sales where store_sales.date_dim.year
  in (2001,2002) group by 1,2,3,4 order by 1,2,3 limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ....date_dim.year in (2001,2002) ??? group by 1,2,3,4 order by 1,2,...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.store_name, store_sales.store.store_id, store_sales.date_dim.week_seq, store_sales.date_dim.year, sum(store_sales.ext_sales_price) as total_sales where store_sales.date_dim.year in (2001,2002) order by 1,2,3 limit 30 offset 50;`

  ```text
  --> 2:234
    |
  2 | select store_sales.store.store_name, store_sales.store.store_id,
  store_sales.date_dim.week_seq, store_sales.date_dim.year,
  sum(store_sales.ext_sales_price) as total_sales where store_sales.date_dim.year
  in (2001,2002) order by 1,2,3 limit 30 offset 50;
    |
  ^---
    |
    = expected ORDERING_DIRECTION, dot_tail, bracket_tail, dcolon_tail,
  COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...in (2001,2002) order by 1,2,3 ??? limit 30 offset 50;
  ```
- `trilogy file write query60.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale… store_sales.item.item_id as item_code,
    sum(store_sales.ext_sales_price) as total
group by item_code
order by item_code asc, total asc
limit 100;`

  ```text
  refused to write 'query60.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...tore_sales.item.item_id = any ??? (select music_item_id_set)  se...

  Write stats: received 1124 chars / 1124 bytes; tail: …'up by item_code\\norder
  by item_code asc, total asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query64.preql -e -c import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as catalog_returns;
import raw.store_sales as st… and item.current_price between 65 and 74;

select
  qualifying_item.product_name,
  store_sales.store.store_name,
  store_sales.store.zip
limit 100;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 30.
  Expected one of:
          * _TERMINATOR

  Location:
   auto qualifying_item <- item ??? where item.color in ('purple',...

  Write stats: received 495 chars / 495 bytes; tail:
  …'e_sales.store.store_name,\\n  store_sales.store.zip\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query66.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

# Time dimension seconds: time_dim.…ty) by (web_sales.sold_date.moy) as ws_m11,
    sum(web_sales.ext_sales_price * web_sales.quantity) by (web_sales.sold_date.moy) as ws_m12
limit 100;`

  ```text
  refused to write 'query66.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_SUM', 'sum(') at line 22, column 5.
  Expected one of:
          * WHERE
          * HAVING
          * _TERMINATOR
          * LIMIT
          * ORDER
          * COMMA
          * MERGE
          * METADATA
  Previous tokens: [Token('IDENTIFIER', 'carrier')]

  Location:
    'DHL,BARIAN' as carrier     ??? sum(web_sales.ext_sales_price

  Write stats: received 2040 chars / 2040 bytes; tail: …'.quantity) by
  (web_sales.sold_date.moy) as ws_m12\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# For store sales whose sold date falls in the year 2000, report total net…    final_rank as within_parent_rank
having hierarchy_level in (0, 1)
order by
    hierarchy_level desc,
    state,
    within_parent_rank
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 14, column 1.
  Expected one of:
          * SELECT

  Location:
   metrics at all rollup levels ??? auto profit_by_geo <- sum(stor...

  Write stats: received 2419 chars / 2419 bytes; tail: …'chy_level desc,\\n
  state,\\n    within_parent_rank\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale….item.item_sk, web_sales.order_number);

auto web_manufact_id <- web_sales.item.manufact_id
    by (web_sales.item.item_sk, web_sales.order_number);
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 18, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...turns.return_quantity, 0)     ??? by (store_sales.item.item_sk,

  Write stats: received 4140 chars / 4140 bytes; tail: …'id\\n    by
  (web_sales.item.item_sk, web_sales.order_number);\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…sales.item.item_sk, web_sales.order_number);
auto web_manufact_id <- web_sales.item.manufact_id by (web_sales.item.item_sk, web_sales.order_number);
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 13, column 102.
  Expected one of:
          * _TERMINATOR

  Location:
  ...e_returns.return_quantity, 0) ??? by (store_sales.item.item_sk,

  Write stats: received 3090 chars / 3090 bytes; tail: …'act_id by
  (web_sales.item.item_sk, web_sales.order_number);\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select coalesce(store_sales.quantity, 0) - coalesce(store_returns.return_quantity, 0) as net_qty by (store_sales.item.item_sk, store_sales.ticket_number);`

  ```text
  --> 3:98
    |
  3 | select coalesce(store_sales.quantity, 0) -
  coalesce(store_returns.return_quantity, 0) as net_qty by
  (store_sales.item.item_sk, store_sales.ticket_number);
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...eturn_quantity, 0) as net_qty ??? by (store_sales.item.item_sk,
  ```
- `trilogy file write query76.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…l

order by channel asc, missing_reference_label asc, year asc nulls first, quarter_of_year asc nulls first, item_category asc nulls first
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n\n') at line 24, column 1.
  Expected one of:
          * MERGE
          * ORDER
          * _TERMINATOR
          * LIMIT
          * HAVING
  Previous tokens: [Token('NULL', 'null')]

  Location:
  ...sales.store.store_sk is null  ??? union  select     'web' as cha...

  Write stats: received 1874 chars / 1874 bytes; tail: …'ar asc nulls first,
  item_category asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\r\n\r\n') at line 55, column
  1.
  Expected one of:
          * METADATA
          * LIMIT
          * MERGE
          * HAVING
          * COMMA
          * ORDER
          * _TERMINATOR
          * WHERE
  Previous tokens: [Token('IDENTIFIER', 'profit')]

  Location:
  ...s,     s_profit as profit  ??? union  where catalog_sales.s...

  Write stats: received 3591 chars / 3591 bytes; tail: …'order by channel nulls
  first, outlet nulls first\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\r\n') at line 42, column 1.
  Expected one of:
          * MERGE
          * METADATA
          * _TERMINATOR
          * LIMIT
          * HAVING
          * WHERE
          * ORDER
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'profit')]

  Location:
  ...rns,     s_profit as profit ??? union where catalog_sales.sol...

  Write stats: received 2732 chars / 2732 bytes; tail: …'order by channel nulls
  first, outlet nulls first\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\r\n') at line 42, column 1.
  Expected one of:
          * LIMIT
          * WHERE
          * METADATA
          * _TERMINATOR
          * COMMA
          * ORDER
          * HAVING
          * MERGE
  Previous tokens: [Token('IDENTIFIER', 'profit')]

  Location:
  ...rns,     s_profit as profit ??? union where cs.sold_date.date...

  Write stats: received 2212 chars / 2212 bytes; tail: …'order by channel nulls
  first, outlet nulls first\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query81.preql --content import raw.catalog_returns as catalog_returns;

# Define the per-(customer, state) total return amount including t…eet_type,
    suite_number,
    city,
    county,
    state,
    zip,
    country,
    gmt_offset,
    location_type,
    cust_state_total
limit 100;`

  ```text
  refused to write 'query81.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 7, column 192.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._returns.returning_addr.state ??? as state_avg;  where catalog_r...

  Write stats: received 1951 chars / 1951 bytes; tail: …'t_offset,\\n
  location_type,\\n    cust_state_total\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run /* want current_price between 62 and 92 */ select item.item_id where item.current_price between 62 and 92 --import raw.item:item limit 5;`

  ```text
  --> 1:108
    |
  1 | /* want current_price between 62 and 92 */ select item.item_id where
  item.current_price between 62 and 92 --import raw.item:item limit 5;
    |
  ^---
    |
    = expected access_chain
  Location:
  ...rent_price between 62 and 92 - ??? -import raw.item:item limit 5;...
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.sold_date.year = 2000

# Step 1: declare the rollup aggregates…c) as rank_within_parent
order by
    level desc nulls first,
    web_sales.item.category nulls first,
    rank_within_parent nulls first
limit 100;
`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 6, column 1.
  Expected one of:
          * SELECT

  Location:
  ...p aggregates as auto concepts ??? auto total_net_paid <- sum(web...

  Write stats: received 903 chars / 903 bytes; tail: …' nulls first,\\n
  rank_within_parent nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

# Define rollup aggregates as auto concepts so they can be reused
# in both SE…c) as rank_within_parent
order by
    level desc nulls first,
    web_sales.item.category nulls first,
    rank_within_parent nulls first
limit 100;
`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 7, column 1.
  Expected one of:
          * SELECT

  Location:
  ..._sales.sold_date.year = 2000  ??? auto total_net_paid <- sum(web...

  Write stats: received 868 chars / 868 bytes; tail: …' nulls first,\\n
  rank_within_parent nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…0
);

where store_sales.date_dim.year = 2000
select count_distinct(store_cust_date) as customer_count
having has_catalog = false and has_web = false;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 11, column 140.
  Expected one of:
          * _TERMINATOR

  Location:
  ...les.date_dim.date as string)) ??? by store_sales.customer.custom...

  Write stats: received 1560 chars / 1560 bytes; tail: …'stomer_count\\nhaving
  has_catalog = false and has_web = false;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query89.preql --content import raw.store_sales as store_sales;

# Filter criteria per the question:
# Items in ('Books','Electronics','Spo…ass asc,
    store_sales.item.brand asc,
    store_sales.store.company_name asc,
    month_of_year asc,
    total asc,
    monthly_avg asc
limit 100;`

  ```text
  refused to write 'query89.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 16, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...e company name, month of year ??? auto monthly_total <- sum(stor...

  Write stats: received 2137 chars / 2137 bytes; tail: …'h_of_year asc,\\n
  total asc,\\n    monthly_avg asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query89.preql --content import raw.store_sales as store_sales;

# Filter criteria per the question:
# Items in ('Books','Electronics','Spo…ass asc,
    store_sales.item.brand asc,
    store_sales.store.company_name asc,
    month_of_year asc,
    total asc,
    monthly_avg asc
limit 100;`

  ```text
  refused to write 'query89.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 16, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...e company name, month of year ??? auto monthly_total <- sum(stor...

  Write stats: received 1966 chars / 1966 bytes; tail: …'h_of_year asc,\\n
  total asc,\\n    monthly_avg asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query89.preql --content import raw.store_sales as store_sales;

where store_sales.date_dim.year = 1999
  and (
    (store_sales.item.categ…ass asc,
    store_sales.item.brand asc,
    store_sales.store.company_name asc,
    month_of_year asc,
    total asc,
    monthly_avg asc
limit 100;`

  ```text
  refused to write 'query89.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 10, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...ts','birdal','dresses'))   )  ??? auto monthly_total <- sum(stor...

  Write stats: received 1444 chars / 1444 bytes; tail: …'h_of_year asc,\\n
  total asc,\\n    monthly_avg asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query89.preql --content import raw.store_sales as store_sales;

where store_sales.date_dim.year = 1999
  and (
    (store_sales.item.categ…asc,
    store_sales.store.company_name asc,
    store_sales.date_dim.moy asc,
    monthly_total asc,
    (yearly_total_by_group / 12) asc
limit 100;`

  ```text
  refused to write 'query89.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 10, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...ts','birdal','dresses'))   )  ??? auto monthly_total <- sum(stor...

  Write stats: received 1513 chars / 1513 bytes; tail: …'y_total asc,\\n
  (yearly_total_by_group / 12) asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run select max(wp_char_count) as max_cc, min(wp_char_count) as min_cc from web_page;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
   min(wp_char_count) as min_cc ??? from web_page;
  ```
- `trilogy run --import raw/household_demographics:hd select hd.dep_count, count(hd.demo_sk) as cnt group by hd.dep_count order by hd.dep_count;`

  ```text
  --> 2:47
    |
  2 | select hd.dep_count, count(hd.demo_sk) as cnt group by hd.dep_count order
  by hd.dep_count;
    |                                               ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...unt, count(hd.demo_sk) as cnt ??? group by hd.dep_count order by...
  ```
- `trilogy run --import raw.catalog_returns:catalog_returns select catalog_returns.date_dim.year, catalog_returns.date_dim.moy, count(catalog_returns.order_number) as num_returns group by all limit 20;`

  ```text
  --> 2:120
    |
  2 | select catalog_returns.date_dim.year, catalog_returns.date_dim.moy,
  count(catalog_returns.order_number) as num_returns group by all limit 20;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ....order_number) as num_returns ??? group by all limit 20;
  ```
- `trilogy run --import raw.catalog_returns:catalog_returns select catalog_returns.refunded_customer.customer_demographics.marital_status, catalog_returns.refun…gmt_offset = -7 and catalog_returns.refunded_customer.household_demographics.buy_potential like 'Unknown%' group by all order by total_net_loss desc;`

  ```text
  --> 2:560
    |
  2 | select
  catalog_returns.refunded_customer.customer_demographics.marital_status,
  catalog_returns.refunded_customer.customer_demographics.education_status,
  catalog_returns.call_center.call_center_id, catalog_returns.call_center.name,
  catalog_returns.call_center.manager, sum(catalog_returns.net_loss) as
  total_net_loss where catalog_returns.date_dim.year = 1998 and
  catalog_returns.date_dim.moy = 11 and
  catalog_returns.refunded_customer.customer_address.gmt_offset = -7 and
  catalog_returns.refunded_customer.household_demographics.buy_potential like
  'Unknown%' group by all order by total_net_loss desc;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or
  MULTIPLY_DIVIDE_PERCENT
  Location:
  ...buy_potential like 'Unknown%' ??? group by all order by total_ne...
  ```
- `trilogy run --import raw.web_sales:web_sales select web_sales.item.manufact_id, count(web_sales.item.item_sk) from web_sales limit 5;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...count(web_sales.item.item_sk) ??? from web_sales limit 5;
  ```
- `trilogy run --import raw.web_sales:web_sales select web_sales.item.manufact_id, count(web_sales.item.item_sk) as cnt where web_sales.sold_date.date between '2000-01-01'::date and '2000-04-30'::date group by web_sales.item.manufact_id having web_sales.item.manufact_id=350 limit 5;`

  ```text
  --> 2:154
    |
  2 | select web_sales.item.manufact_id, count(web_sales.item.item_sk) as cnt
  where web_sales.sold_date.date between '2000-01-01'::date and
  '2000-04-30'::date group by web_sales.item.manufact_id having
  web_sales.item.manufact_id=350 limit 5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...::date and '2000-04-30'::date ??? group by web_sales.item.manufa...
  ```
- `trilogy run --import raw/reason select * limit 100;`

  ```text
  --> 2:8
    |
  2 | select * limit 100;
    |        ^---
    |
    = expected select_item
  Location:
  import raw.reason; select ??? * limit 100;
  ```
- `trilogy run select sk, desc from raw.reason limit 20;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  select sk, desc ??? from raw.reason limit 20;
  ```
- `trilogy run select r_reason_desc from reason limit 100;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  select r_reason_desc ??? from reason limit 100;
  ```
- `trilogy run --import raw/store_returns select sk, desc from reason limit 10;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...tore_returns; select sk, desc ??? from reason limit 10;
  ```
- `trilogy run select r_reason_desc from reason limit 100;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  select r_reason_desc ??? from reason limit 100;
  ```
- `trilogy file write query97.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

merge store_sales.customer.cust…count(catalog_pairs.c ? store_pairs.c is null) as only_catalog_count,
    count(store_pairs.c ? catalog_pairs.c is not null) as both_count
limit 100;`

  ```text
  …
  catalog_pairs <- group by
  (catalog_sales.bill_customer.customer_sk, catalog_sales.item.item_sk)\n
  select distinct catalog_sales.bill_customer.customer_sk as c,
  catalog_sales.item.item_sk as i\n    where catalog_sales.sold_date.year =
  2000;\n\nselect\n    count(store_pairs.c ? catalog_pairs.c is null) as
  only_store_count,\n    count(catalog_pairs.c ? store_pairs.c is null) as
  only_catalog_count,\n    count(store_pairs.c ? catalog_pairs.c is not null) as
  both_count\nlimit 100;') at line 8, column 30.
  Expected one of:
          * "BY"i
  Previous tokens: [Token('__ANON_17', 'group by')]

  Location:
   auto store_pairs <- group by ??? (store_sales.customer.customer...

  Write stats: received 1074 chars / 1074 bytes; tail: …'rs.c ? catalog_pairs.c
  is not null) as both_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

merge store_sales.customer.cust… as only_store,
    count(catalog_set.c ? store_set.c is null) as only_catalog,
    count(store_set.c ? catalog_set.c is not null) as both
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...n year 2000 auto store_set <- ??? (select     store_sales.custom...

  Write stats: received 926 chars / 926 bytes; tail: …'(store_set.c ?
  catalog_set.c is not null) as both\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

merge store_sales.customer.cust…is null) as only_store,
    count(cat_set_c ? store_set_c is null) as only_catalog,
    count(store_set_c ? cat_set_c is not null) as both
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 8, column 54.
  Expected one of:
          * _TERMINATOR

  Location:
  ...re_sales.customer.customer_sk ??? by (store_sales.customer.custo...

  Write stats: received 1215 chars / 1215 bytes; tail: …'ount(store_set_c ?
  cat_set_c is not null) as both\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

merge store_sales.customer.cust…ull) as only_store,
    count(cs_pair_key ? ss_pair_key is null) as only_catalog,
    count(ss_pair_key ? cs_pair_key is not null) as both
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 47.
  Expected one of:
          * _TERMINATOR

  Location:
  ...re_sales.customer.customer_sk ??? where store_sales.date_dim.yea...

  Write stats: received 981 chars / 981 bytes; tail: …'nt(ss_pair_key ?
  cs_pair_key is not null) as both\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

merge store_sales.customer.cust…ate.year = 2000;

select
    count(store_only_cnt) as only_store,
    count(catalog_only_cnt) as only_catalog,
    count(both_cnt) as both
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 178.
  Expected one of:
          * _TERMINATOR

  Location:
  ...sk, store_sales.item.item_sk) ??? where store_sales.date_dim.yea...

  Write stats: received 1282 chars / 1282 bytes; tail: …'cnt) as only_catalog,\\n
  count(both_cnt) as both\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

merge store_sales.customer.cust…r ? cs_pair is null) as only_store,
    count(cs_pair ? ss_pair is null) as only_catalog,
    count(ss_pair ? cs_pair is not null) as both
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 88.
  Expected one of:
          * _TERMINATOR

  Location:
  ...sk, store_sales.item.item_sk) ??? where store_sales.date_dim.yea...

  Write stats: received 818 chars / 818 bytes; tail: …'\\n    count(ss_pair ?
  cs_pair is not null) as both\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

merge store_sales.customer.cust…mer_sk, store_sales.item.item_sk ? catalog_sales.bill_customer.customer_sk is not null and catalog_sales.item.item_sk is not null) as both
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER',
  'store_sales.customer.customer_sk') at line 14, column 20.
  Expected one of:
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'distinct')]

  Location:
  ...y_catalog,     count(distinct ??? store_sales.customer.customer_...

  Write stats: received 916 chars / 916 bytes; tail: …'d
  catalog_sales.item.item_sk is not null) as both\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `syntax-missing-alias`

- `trilogy run - duckdb --import raw.store_sales:store_sales`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...re_sales.customer.customer_sk ??? limit 10;
  ```
- `trilogy run - duckdb --import raw.store_sales:store_sales`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...nt(store_sales.ticket_number) ??? limit 10;
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.date, sum(store_sales.ext_sales_price) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...(store_sales.ext_sales_price) ??? limit 10;
  ```
- `trilogy run --import raw.store_sales:store_sales select count(ticket_number),min(date_dim.date),max(date_dim.date) limit 5`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...s; select count(ticket_number) ??? ,min(date_dim.date),max(date_d...
  ```
- `trilogy run -`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ufact_id, count(item.item_sk) ??? limit 100;
  ```
- `trilogy run -`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.manufact_id, count(item.i...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.item.category, store_sales.item.class, sum(store_sales.net_profit) / nullif(sum(store_sal…llup store_sales.item.category, store_sales.item.class as g_class where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN' limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...re_sales.ext_sales_price), 0) ??? by rollup store_sales.item.cat...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.item.category, store_sales.item.class, sum(store_sales.net_profit) / nullif(sum(store_sal…llup store_sales.item.category, store_sales.item.class as g_class where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN' limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...re_sales.ext_sales_price), 0) ??? by rollup store_sales.item.cat...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.item.category, store_sales.item.class, sum(store_sales.net_profit) / nullif(sum(store_sal…ollup store_sales.item.category, store_sales.item.class as g_class where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN' limit 20`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...re_sales.ext_sales_price), 0) ??? by rollup store_sales.item.cat...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.item.category, store_sales.item.class, sum(store_sales.net_profit) / nullif(sum(store_sal… by rollup store_sales.item.category, store_sales.item.class as gm where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN' limit 20`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...re_sales.ext_sales_price), 0) ??? by rollup store_sales.item.cat...
  ```
- `trilogy run --import raw.store_sales:store_sales select sum(store_sales.net_profit) / nullif(sum(store_sales.ext_sales_price), 0) by rollup store_sales.item.…tem.class as gm, store_sales.item.category, store_sales.item.class where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN' limit 20`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...re_sales.ext_sales_price), 0) ??? by rollup store_sales.item.cat...
  ```
- `trilogy run --import raw.store_sales:store_sales select sum(store_sales.net_profit) / nullif(sum(store_sales.ext_sales_price), 0) by store_sales.item.category as gm, store_sales.item.category where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN' limit 10`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...re_sales.ext_sales_price), 0) ??? by store_sales.item.category a...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.item.category, store_sales.item.class, sum(store_sales.net_profit) / nullif(sum(store_sal… by rollup store_sales.item.category, store_sales.item.class as gm where store_sales.date_dim.year = 2001 and store_sales.store.state = 'TN' limit 20`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...re_sales.ext_sales_price), 0) ??? by rollup store_sales.item.cat...
  ```
- `trilogy run --import raw.item:item select item.manufact_id, count(item.item_sk) where item.current_price between 68 and 98 order by item.manufact_id limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ufact_id, count(item.item_sk) ??? where item.current_price betwe...
  ```
- `trilogy run --import raw.item:item select distinct item.manufact_id order by item.manufact_id limit 30;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.manufact_id order by item...
  ```
- `trilogy file write query39.preql --content import raw.inventory as inventory;

select
    inventory.warehouse.warehouse_id,
    inventory.item.item_id,
    i…)
order by
    inventory.warehouse.warehouse_id nulls first,
    inventory.item.item_id nulls first,
    inventory.date_dim.moy nulls first
limit 20;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ..._dim.moy nulls first limit 20; ???

  Write stats: received 509 chars / 509 bytes; tail: …'ulls first,\\n
  inventory.date_dim.moy nulls first\\nlimit 20;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.item:item select distinct item.size`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.size;
  ```
- `trilogy run --import raw.item:item select distinct item.size`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.size;
  ```
- `trilogy run --import raw.item:item select distinct item.size as distinct_size`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.size as distinct_size;
  ```
- `trilogy run --import raw.item:item select distinct item.size as sz`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.size as sz;
  ```
- `trilogy run --import raw.item:item select distinct item.color as col limit 20`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.color as col limit 20;
  ```
- `trilogy run --import raw.item:item select distinct item.manufact as man limit 30`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.manufact as man limit 30;...
  ```
- `trilogy run --import raw.item:item select distinct item.size as sz`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.size as sz;
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct store_sales.store.store_sk;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? store_sales.store.store_sk;
  ```
- `trilogy run --import raw.item:item select distinct item.manufact, item.manufact_id;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...item as item; select distinct ??? item.manufact, item.manufact_i...
  ```
- `trilogy run --import raw.store:store select distinct store.city from store;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ore as store; select distinct ??? store.city from store;
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct store_sales.store.city limit 100;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? store_sales.store.city limit 1...
  ```
- `trilogy run --import raw.store_sales:store_sales - --all-rows`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...nt(store_sales.ticket_number) ??? where store_sales.customer.cus...
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/store_returns:store_returns select store_sales.store.store_name, count(store_sales.ticket_number), count(store_returns.ticket_number) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...unt(store_sales.ticket_number) ??? , count(store_returns.ticket_n...
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.item.manager_id, store_sales.item.brand, count(store_sales.ticket_number) limit 3;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...nt(store_sales.ticket_number) ??? limit 3;
  ```
- `trilogy run -`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...roup 'AAAAA'? select distinct ??? substring(store_sales.item.ite...
  ```
- `trilogy run -`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...r position 5. select distinct ??? substring(item.item_id, 6, 2)
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/catalog_sales:catalog_sales --import raw/web_sales:web_sales select store_sales.item.item_id, sum(store_sales.ext_sales_price) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...(store_sales.ext_sales_price) ??? limit 5;
  ```
- `trilogy file write query67.preql --content import raw.store_sales as store_sales;

# Compute sum of (coalesce(sales_price, 0) * coalesce(quantity, 0)) as the…asc nulls first,
    store_sales.date_dim.moy asc nulls first,
    store_sales.store.store_id asc nulls first,
    sales asc,
    rank asc
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ore_code,     sales_dollars by ??? rollup         store_sales.it...

  Write stats: received 1365 chars / 1365 bytes; tail: …'e_id asc nulls first,\\n
  sales asc,\\n    rank asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as store_sales;

# Compute sum of (coalesce(sales_price, 0) * coalesce(quantity, 0)) as the…,
        store_sales.date_dim.year,
        store_sales.date_dim.qoy,
        store_sales.date_dim.moy,
        store_sales.store.store_id as sales;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ore_code,     sales_dollars by ??? rollup         store_sales.it...

  Write stats: received 984 chars / 984 bytes; tail: …'s.date_dim.moy,\\n
  store_sales.store.store_id as sales;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query67.preql -e -c import raw.store_sales as store_sales;

auto sales_dollars <- sum(coalesce(store_sales.sales_price::numeric, 0) * coal…,
        store_sales.date_dim.year,
        store_sales.date_dim.qoy,
        store_sales.date_dim.moy,
        store_sales.store.store_id as sales;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ore_code,     sales_dollars by ??? rollup         store_sales.it...

  Write stats: received 814 chars / 814 bytes; tail: …'s.date_dim.moy,\\n
  store_sales.store.store_id as sales;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as store_sales;

auto sales_dollars <- sum(coalesce(store_sales.sales_price::numeric, 0) * …d, store_sales.item.product_name, store_sales.date_dim.year, store_sales.date_dim.qoy, store_sales.date_dim.moy, store_sales.store.store_id as sales;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ore_code,     sales_dollars by ??? rollup store_sales.item.categ...

  Write stats: received 750 chars / 750 bytes; tail: …'ore_sales.date_dim.moy,
  store_sales.store.store_id as sales;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as store_sales;

auto sales_dollars <- sum(coalesce(store_sales.sales_price::numeric, 0) * …nd, store_sales.item.product_name, store_sales.date_dim.year, store_sales.date_dim.qoy, store_sales.date_dim.moy, store_sales.store.store_id as sales`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
   coalesce(sales_dollars, 0) by ??? rollup store_sales.item.categ...

  Write stats: received 762 chars / 762 bytes; tail: …'tore_sales.date_dim.moy,
  store_sales.store.store_id as sales'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct store_sales.store.state;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? store_sales.store.state;
  ```
- `trilogy run --import raw.inventory:inventory select distinct inventory.item.manufact_id order by 1 limit 100;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...as inventory; select distinct ??? inventory.item.manufact_id ord...
  ```
- `trilogy run --debug --import raw.date_dim:date_dim --import raw.web_sales:web_sales select date_dim.year, count(web_sales.order_number) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...count(web_sales.order_number) ??? limit 5;
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\pest_backend.p
  y", line 321, in parse_pest
      tree = parse_trilogy_syntax_tuple(text)
  ValueError:  --> 3:53
    |
  3 | select date_dim.year, count(web_sales.order_number) limit 5;
    |                                                     ^---
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
- `trilogy run select distinct wp_char_count from web_page order by wp_char_count;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  select distinct ??? wp_char_count from web_page or...
  ```
- `trilogy run --import raw.web_sales:web_sales select web_sales.item.manufact_id, count(web_sales.item.item_sk) where web_sales.item.manufact_id = 350 limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...count(web_sales.item.item_sk) ??? where web_sales.item.manufact_...
  ```
- `trilogy run --import raw/store_returns:returns select returns.reason.desc, count(returns.ticket_number) where returns.reason.desc = 'Package was damaged';`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   count(returns.ticket_number) ??? where returns.reason.desc = 'P...
  ```
- `trilogy run --import raw.catalog_sales:catalog_sales select count(1) by catalog_sales.order_number limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...by catalog_sales.order_number ??? limit 5;
  ```

### `undefined-concept`

- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  raw.date_dim.')
  ```
- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date_dim. Suggestions: ['date_dim.qoy', 'date_dim.moy', 'date_dim.dow']")
  ```
- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.sold_date. Suggestions: ['catalog_sales.sold_date.qoy',
  'catalog_sales.sold_date.moy', 'catalog_sales.sold_date.dow']")
  ```
- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.sold_date. Suggestions: ['catalog_sales.sold_date.qoy',
  'catalog_sales.sold_date.moy', 'catalog_sales.sold_date.dow']")
  ```
- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.sold_date. Suggestions: ['catalog_sales.sold_date.qoy',
  'catalog_sales.sold_date.moy', 'catalog_sales.sold_date.dow']")
  ```
- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  sold_date.date_sk. Suggestions: ['web_sales.sold_date.date_sk',
  'catalog_sales.sold_date.date_sk', 'web_sales.sold_date.date']")
  ```
- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.sold_date. Suggestions: ['catalog_sales.sold_date.qoy',
  'catalog_sales.sold_date.moy', 'catalog_sales.sold_date.dow']")
  ```
- `trilogy run query04.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query06.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.category. Suggestions: ['store_sales.item.category',
  'store_sales.item.category_id']")
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  customer.preferred_cust_flag. Suggestions:
  ['store_sales.customer.preferred_cust_flag']")
  ```
- `trilogy run query16.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_returns.order_number. Suggestions: ['catalog_sales.order_number',
  'catalog_sales.warehouse.street_number',
  'catalog_sales.warehouse.suite_number']")
  ```
- `trilogy run query17.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_returns.item.id. Suggestions: ['store_returns.item.size',
  'store_returns.item.item_id', 'store_returns.item.units']")
  ```
- `trilogy run query18.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.item.code. Suggestions: ['catalog_sales.item.color',
  'catalog_sales.item.size', 'catalog_sales.item.container']")
  ```
- `trilogy run --import raw.store_sales:store_sales select count(ticket_number) as cnt, min(date_dim.date) as min_date, max(date_dim.date) as max_date limit 5`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  local.ticket_number. Suggestions: ['store_sales.ticket_number']")
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales merge catalog_sales.item.item_sk into ~store_sales.item.item_sk; me…turns.return_quantity) as total_store_return_qty, sum(catalog_sales.quantity) as total_catalog_sale_qty order by store_sales.item.item_id asc limit 5`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  store_returns.return_quantity. Suggestions: ['store_sales.quantity']")
  ```
- `trilogy run query30.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer_return_amount.')
  ```
- `trilogy run query33.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.manufact_id. Suggestions: ['manufact_id', 'web_sales.item.manufact_id',
  'store_sales.item.manufact_id']")
  ```
- `trilogy run query33.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.manufact_id. Suggestions: ['manufact_id', 'web_sales.item.manufact_id',
  'store_sales.item.manufact_id']")
  ```
- `trilogy run query59.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  store_name.')
  ```
- `trilogy run query60.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_sales.item.in_music_group. Suggestions: ['store_sales.item.size',
  'store_sales.item.item_sk', 'store_sales.item.item_id']")
  ```
- `trilogy run query60.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_sales.item.in_music_group. Suggestions: ['store_sales.item.size',
  'store_sales.item.item_sk', 'store_sales.item.item_id']")
  ```
- `trilogy run query60.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  item.in_music_group.')
  ```
- `trilogy run query60.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_sales.item.in_music_group. Suggestions: ['store_sales.item.size',
  'store_sales.item.item_sk', 'store_sales.item.item_id']")
  ```
- `trilogy run query71.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.item.id. Suggestions: ['catalog_sales.item.size',
  'catalog_sales.item.item_id', 'catalog_sales.item.units']")
  ```
- `trilogy run --import raw.store_sales:store_sales select quantity, item.item_sk limit 10;`

  ```text
  (UndefinedConceptException(...), 'line: 2: Undefined concept:
  local.quantity.')
  ```
- `trilogy run query80.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_returns.item.sk. Suggestions: ['store_returns.item.size',
  'store_returns.item.item_sk', 'store_returns.item.units']")
  ```
- `trilogy run query80.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_returns.return_amt. Suggestions: ['catalog_returns.return_amount',
  'catalog_returns.return_tax', 'catalog_returns.return_quantity']")
  ```
- `trilogy run query81.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  salutation.')
  ```
- `trilogy run query83.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date_dim.date. Suggestions: ['web_returns.date_dim.date',
  'store_returns.date_dim.date', 'web_returns.date_dim.date_sk']")
  ```
- `trilogy run query83.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_returns.item. Suggestions: ['catalog_returns.item.size',
  'catalog_returns.fee', 'catalog_returns.item.units']")
  ```
- `trilogy run query83.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_returns.date_dim. Suggestions: ['catalog_returns.date_dim.qoy',
  'catalog_returns.date_dim.moy', 'catalog_returns.date_dim.dow']")
  ```
- `trilogy run --import raw/reason select reason.sk, reason.desc limit 20;`

  ```text
  (UndefinedConceptException(...), 'line: 2: Undefined concept:
  reason.sk.')
  ```
- `trilogy run --import raw/store_returns select store_returns.customer.customer_sk, store_returns.store_sk, store_returns.reason.sk, item.item_sk, ticket_number, return_quantity limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  store_returns.customer.customer_sk. Suggestions: ['customer.customer_sk',
  'customer.customer_id', 'customer.customer_address.zip']")
  ```
- `trilogy run --import raw/store_returns select desc where desc = 'Stopped working' limit 10;`

  ```text
  (UndefinedConceptException(...), 'line: 2: Undefined concept:
  local.desc.')
  ```

### `cli-misuse`

- `trilogy run - > query03.preql > tmp.preql`

  ```text
  '>' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.store_sales:store_sales duckdb select store_sales.customer.customer_id, count(store_sales.ticket_number) by store_sales.customer.customer_sk limit 10;`

  ```text
  'select store_sales.customer.customer_id, count(store_sales.ticket_number) by store_sales.customer.customer_sk limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.store_sales:store_sales duckdb select store_sales.customer.customer_id, count(store_sales.ticket_number) by store_sales.customer.customer_sk limit 10;`

  ```text
  'select store_sales.customer.customer_id, count(store_sales.ticket_number) by store_sales.customer.customer_sk limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy read_file raw/store_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/store_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/web_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/customer.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/web_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/item.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy help`

  ```text
  No such command 'help'.
  ```
- `trilogy debug-file query13.preql`

  ```text
  No such command 'debug-file'.
  ```
- `trilogy explore raw/catalog_sales.preql --regex bill_addr\. sold_date\. sales_price bill_customer\.customer_address`

  ```text
  Got unexpected extra arguments (sold_date\. sales_price bill_customer\.customer_address)
  ```
- `trilogy explore raw/sold_date.preql`

  ```text
  Invalid value for 'PATH': File 'raw/sold_date.preql' does not exist.
  ```
- `trilogy explore raw/web_sales.precl --regex sold_date\.`

  ```text
  Invalid value for 'PATH': File 'raw/web_sales.precl' does not exist.
  ```
- `trilogy explore raw/store_sales.preql --regex ^date_dim\.(date)$ --concepts`

  ```text
  No such option: --concepts
  ```
- `trilogy explore raw/store_sales.preql --regex customer\.(first_name|last_name) --concepts`

  ```text
  No such option: --concepts
  ```
- `trilogy database run select distinct s_store_sk from store order by s_store_sk`

  ```text
  No such command 'run'.
  ```
- `trilogy sql select distinct s_city from store;`

  ```text
  No such command 'sql'.
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.store_sales:store_sales merge store_sales.item.item_sk into ~web_sales.item.item_sk; select web_sales.item.item_sk, web_sales.sold_date.date, web_sales.sold_date.year limit 5;`

  ```text
  'select web_sales.item.item_sk, web_sales.sold_date.date, web_sales.sold_date.year limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.store_sales:store_sales duckdb - select web_sales.item.item_sk, web_sales.sold_date.date, web_sales.sold_date.year limit 5;`

  ```text
  '-' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy read_file trilogy.toml`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/store_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs --import raw/web_sales:ws --import raw/item:i merge i.item_sk into ~ss.item.item_sk; me…tem.item_sk; select i.item_id, sum(ss.ext_sales_price) as ss_total, sum(cs.ext_sales_price) as cs_total, sum(ws.ext_sales_price) as ws_total limit 5;`

  ```text
  'merge i.item_sk into ~cs.item.item_sk;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run - --import raw.store_sales:store_sales select store_sales.item.category, sales_dollars by rollup store_sales.item.category as sales;`

  ```text
  'select store_sales.item.category, sales_dollars by rollup store_sales.item.category as sales;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql --regex store .state .county .country`

  ```text
  Got unexpected extra arguments (.state .county .country)
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns merge store_returns.item.item_sk into ~store_sales.item.item_sk; me…~store_sales.ticket_number; select store_sales.quantity, store_returns.return_quantity, store_sales.item.item_sk, store_sales.ticket_number limit 10;`

  ```text
  'merge store_returns.ticket_number into ~store_sales.ticket_number;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns auto net_qty <- store_sales.quantity - store_returns.return_quantity by (store_sales.item.item_sk, store_sales.ticket_number); select net_qty limit 5;`

  ```text
  'select net_qty limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns auto net_qty <- store_sales.quantity - store_returns.return_quantity; select net_qty limit 5;`

  ```text
  'select net_qty limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns --import raw.item:item merge item.item_sk into ~store_sales.item.it…k; merge item.item_sk into ~store_returns.item.item_sk; select store_sales.quantity, store_returns.return_quantity, store_sales.item.item_sk limit 5;`

  ```text
  'merge item.item_sk into ~store_returns.item.item_sk;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy read_file raw/date_dim.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/customer.preql -show concepts`

  ```text
  No such option: -s
  ```
- `trilogy explore raw/web_returns.preql --regex \.item \.reason`

  ```text
  Got unexpected extra argument (\.reason)
  ```
- `trilogy read_file raw/catalog_sales.preql`

  ```text
  No such command 'read_file'.
  ```

### `join-resolution`

- `trilogy run query05.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.outlet<Purpose.PROPERTY>Derivation.BASIC>',
  'local.sales<Purpose.METRIC>Derivation.BASIC>',
  'local.returns<Purpose.METRIC>Derivation.BASIC>',
  'local.profit<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['store_sales.customer.customer_demographics.gender<Purpose.PROPERTY>Derivation
  .ROOT>',
  'store_sales.customer.customer_demographics.marital_status<Purpose.PROPERTY>Der
  ivation.ROOT>',
  'store_sales.customer.customer_demographics.education_status<Purpose.PROPERTY>D
  erivation.ROOT>',
  'store_sales.customer.customer_demographics.purchase_estimate<Purpose.PROPERTY>
  Derivation.ROOT>',
  'store_sales.customer.customer_demographics.credit_rating<Purpose.PROPERTY>Deri
  vation.ROOT>',
  'store_sales.customer.customer_demographics.dep_count<Purpose.PR
  …

  'store_sales.customer.customer_demographics.dep_employed_count<Purpose.PROPERTY
  >Derivation.ROOT>',
  'store_sales.customer.customer_demographics.dep_college_count<Purpose.PROPERTY>
  Derivation.ROOT>',
  'local.customer_count1<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count2<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count3<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count4<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count5<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count6<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['store_sales.customer.customer_demographics.gender<Purpose.PROPERTY>Derivation
  .ROOT>',
  'store_sales.customer.customer_demographics.marital_status<Purpose.PROPERTY>Der
  ivation.ROOT>',
  'store_sales.customer.customer_demographics.education_status<Purpose.PROPERTY>D
  erivation.ROOT>',
  'store_sales.customer.customer_demographics.purchase_estimate<Purpose.PROPERTY>
  Derivation.ROOT>',
  'store_sales.customer.customer_demographics.credit_rating<Purpose.PROPERTY>Deri
  vation.ROOT>',
  'store_sales.customer.customer_demographics.dep_count<Purpose.PR
  …

  'store_sales.customer.customer_demographics.dep_employed_count<Purpose.PROPERTY
  >Derivation.ROOT>',
  'store_sales.customer.customer_demographics.dep_college_count<Purpose.PROPERTY>
  Derivation.ROOT>',
  'local.customer_count1<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count2<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count3<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count4<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count5<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count6<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['store_sales.customer.customer_demographics.gender<Purpose.PROPERTY>Derivation
  .ROOT>',
  'store_sales.customer.customer_demographics.marital_status<Purpose.PROPERTY>Der
  ivation.ROOT>',
  'store_sales.customer.customer_demographics.education_status<Purpose.PROPERTY>D
  erivation.ROOT>',
  'store_sales.customer.customer_demographics.purchase_estimate<Purpose.PROPERTY>
  Derivation.ROOT>',
  'store_sales.customer.customer_demographics.credit_rating<Purpose.PROPERTY>Deri
  vation.ROOT>',
  'store_sales.customer.customer_demographics.dep_count<Purpose.PR
  …

  'store_sales.customer.customer_demographics.dep_employed_count<Purpose.PROPERTY
  >Derivation.ROOT>',
  'store_sales.customer.customer_demographics.dep_college_count<Purpose.PROPERTY>
  Derivation.ROOT>',
  'local.customer_count1<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count2<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count3<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count4<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count5<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count6<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query11.preql`

  ```text
  Could not resolve connections for query with output
  ['local.customer_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.preferred_cust_flag<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.web_sales:web_sales --import raw.customer:customer merge customer.customer_sk into ~web_sales.b… - web_sales.ext_discount_amt ? web_sales.sold_date.year=2001) by web_sales.bill_customer.customer_sk as wr2001 having sr2001>0 and wr2001>0 limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.customer.customer_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.sr2001<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.wr2001<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.item.item_id, count(store_sales.quantity) as sq,…re store_sales.date_dim.fy_year=2001 and store_sales.date_dim.qoy=1 and store_returns.date_dim.fy_year=2001 and store_returns.date_dim.qoy=1 limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.sq<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.rq<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store:store --import raw.store_sales:store_sales select store.store_sk as sid, count(store_sales.ticket_number) as cnt;`

  ```text
  Could not resolve connections for query with output
  ['local.sid<Purpose.KEY>Derivation.BASIC>',
  'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run -`

  ```text
  Could not resolve connections for query with output
  ['store_sales.item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.ss_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ws_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs select ss.item.item_id, sum(ss.ext_sales_price) as ss_total, sum(cs.ext_sales_price) as cs_total where ss.date_dim.week_seq = cs.sold_date.week_seq limit 5;`

  ```text
  Could not resolve connections for query with output
  ['ss.item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.ss_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/catalog_sales:cs --import raw/web_sales:ws select ss.item.item_id, sum(ss.ext_sales_price) as ss_total, …tal, sum(ws.ext_sales_price) as ws_total where ss.date_dim.week_seq = cs.sold_date.week_seq and ss.date_dim.week_seq = ws.sold_date.week_seq limit 5;`

  ```text
  Could not resolve connections for query with output
  ['ss.item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.ss_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ws_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw/store_sales.preql:store_sales --import raw/store.preql:store select store.store_name, store_sales.item.item_desc, sum(store_sales.sa…store_sales.date_dim.year = 1998 order by store.store_name, store_sales.item.item_desc, store.store_sk, store_sales.item.item_sk nulls first limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store.store_name<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.item.item_desc<Purpose.PROPERTY>Derivation.ROOT>',
  'local.revenue<Purpose.METRIC>Derivation.AGGREGATE>',
  'store_sales.item.current_price<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.item.wholesale_cost<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.item.brand<Purpose.PROPERTY>Derivation.ROOT>',
  'store.store_sk<Purpose.KEY>Derivation.ROOT>',
  'store_sales.item.item_sk<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw/store_sales.preql:store_sales --import raw/item.preql:item select store_sales.item.item_desc, item.current_price limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.item.item_desc<Purpose.PROPERTY>Derivation.ROOT>',
  'item.current_price<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run query66.preql`

  ```text
  Could not resolve connections for query with output
  ['web_sales.warehouse.warehouse_name<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.warehouse.warehouse_sq_ft<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.warehouse.city<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.warehouse.county<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.warehouse.state<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.warehouse.country<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.sold_date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'local.carrier_string<Purpose.CONSTANT>Derivation.CONSTANT>'] from current
  model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select coalesce(store_sales.quantity, 0) - coalesce(store_returns.return_quantity, 0) as net_qty, store_sales.item.item_sk limit 10;`

  ```text
  Could not resolve connections for query with output
  ['local.net_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'store_sales.item.item_sk<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.quantity, store_returns.return_quantity, store_sales.item.item_sk limit 10;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_returns.return_quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.item.item_sk<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.quantity, store_returns.return_quantity, store_sales.item.item_sk limit 10;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_returns.return_quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.item.item_sk<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.quantity, store_returns.return_quantity, store_sales.item.item_sk, store_sales.ticket_number limit 10;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_returns.return_quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.item.item_sk<Purpose.KEY>Derivation.ROOT>',
  'store_sales.ticket_number<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns --import raw.item:item select store_sales.quantity, store_returns.return_quantity, store_sales.item.item_sk, store_sales.ticket_number limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_returns.return_quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.item.item_sk<Purpose.KEY>Derivation.ROOT>',
  'store_sales.ticket_number<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.quantity, sr.return_quantity, ss.item.item_sk, ss.ticket_number limit 5;`

  ```text
  Could not resolve connections for query with output
  ['ss.quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'sr.return_quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'ss.item.item_sk<Purpose.KEY>Derivation.ROOT>',
  'ss.ticket_number<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.item.category, store_sales.quantity, store_returns.return_quantity limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.item.category<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_returns.return_quantity<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.item.category, store_sales.quantity, store_returns.return_quantity, store_sales.ticket_number limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.item.category<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_returns.return_quantity<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.ticket_number<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run query76.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.missing_reference_label<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.year<Purpose.PROPERTY>Derivation.BASIC>',
  'local.quarter_of_year<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_category<Purpose.PROPERTY>Derivation.BASIC>',
  'local.line_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_ext_sales_price<Purpose.METRIC>Derivation.AGGREGATE>'] from
  current model.
  ```
- `trilogy run query76.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.missing_reference_label<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.year<Purpose.PROPERTY>Derivation.BASIC>',
  'local.quarter_of_year<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_category<Purpose.PROPERTY>Derivation.BASIC>',
  'local.line_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_ext_sales_price<Purpose.METRIC>Derivation.AGGREGATE>'] from
  current model.
  ```
- `trilogy run query83.preql`

  ```text
  Could not resolve connections for query with output
  ['store_returns.item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.store_return_qty<Purpose.METRIC>Derivation.BASIC>',
  'local.store_pct<Purpose.PROPERTY>Derivation.BASIC>',
  'local.catalog_return_qty<Purpose.METRIC>Derivation.BASIC>',
  'local.catalog_pct<Purpose.PROPERTY>Derivation.BASIC>',
  'local.web_return_qty<Purpose.METRIC>Derivation.BASIC>',
  'local.web_pct<Purpose.PROPERTY>Derivation.BASIC>',
  'local.avg_qty<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.date_dim:date_dim --import raw.web_sales:web_sales select date_dim.year, count(web_sales.order_number) as cnt limit 5;`

  ```text
  Could not resolve connections for query with output
  ['date_dim.year<Purpose.PROPERTY>Derivation.ROOT>',
  'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.web_returns:web_returns --import raw.web_sales:web_sales select count(web_sales.order_number) as ws_cnt, count(web_returns.order_number) as wr_cnt where web_sales.sold_date.year = 2000 limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.ws_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.wr_cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.web_returns:web_returns --import raw.web_sales:web_sales select count(web_sales.order_number) as cnt where web_sales.sold_date.year = 2000 and web_sales.sales_price between 100 and 150 and web_returns.refunded_cdemo.marital_status = 'M' limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.zips_param. Valid:
  'STRING'.
  ```
- `trilogy run query38.preql duckdb`

  ```text
  Invalid argument type 'DATE' passed into CONCAT function in
  position 2 from concept: store_sales.date_dim.date. Valid: 'STRING'.
  ```
- `trilogy run -`

  ```text
  Invalid argument type 'DATE' passed into SUBTRACT function in
  position 1 from concept: ws.sold_date.date. Valid: 'BIGINT', 'FLOAT',
  'INTEGER', 'NUMBER', 'NUMERIC'.
  ```
- `trilogy run query72.preql`

  ```text
  Invalid argument type 'DATE' passed into SUBTRACT function in
  position 1 from concept: catalog_sales.ship_date.date. Valid: 'BIGINT',
  'FLOAT', 'INTEGER', 'NUMBER', 'NUMERIC'.
  ```
- `trilogy run query87.preql`

  ```text
  Invalid argument type 'DATE' passed into CONCAT function in
  position 3 from concept: store_sales.date_dim.date. Valid: 'STRING'.
  ```

### `file-not-found`

- `trilogy run --import raw.store_sales:store_sales testquery.preql duckdb`

  ```text
  Input 'testquery.preql' does not exist.
  ```
- `trilogy run query11.preql`

  ```text
  Input 'query11.preql' does not exist.
  ```
- `trilogy run query38.preql duckdb tpcds.duckdb`

  ```text
  Input 'query38.preql' does not exist.
  ```
