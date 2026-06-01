# Trilogy failure analysis — 20260601-190402

- Run `20260601-190401_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 298 | failed: 54 (18%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 33 | 61% |
| `undefined-concept` | 9 | 17% |
| `syntax-parse` | 7 | 13% |
| `join-resolution` | 4 | 7% |
| `syntax-missing-alias` | 1 | 2% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text
  HAVING references 'local.cust_store_total',
  'local.store_avg', which are not in the SELECT projection (line 7). Add them to
  SELECT, each prefixed with `--` so they stay out of the output rows — keep your
  HAVING as-is:
      select <your existing columns>, --local.cust_store_total, --local.store_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ws.sold_date.date_sk; select cs.sold_date.date_sk, ws.sold_date.date_sk limit 3;`

  ```text
  Duplicate select output for ws.sold_date.date_sk; Line: 3
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ws.sold_date.date_sk;
auto cat_daily <- sum(cs.ext_sales_…un_ratio, mon_ratio, tue_ratio, wed_ratio, thu_ratio, fri_ratio, sat_ratio having cs.sold_date.year=2001 order by cs.sold_date.week_seq asc limit 60;`

  ```text
  HAVING references 'cs.sold_date.year', which is not in the
  SELECT projection (line 16). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --cs.sold_date.year
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.cs_yt_2002', 'local.cs_yt_2001',
  'local.ss_yt_2002', 'local.ss_yt_2001', 'local.ws_yt_2002', 'local.ws_yt_2001',
  which are not in the SELECT projection (line 36). Add them to SELECT, each
  prefixed with `--` so they stay out of the output rows — keep your HAVING
  as-is:
      select <your existing columns>, --local.cs_yt_2002, --local.cs_yt_2001,
  --local.ss_yt_2002, --local.ss_yt_2001, --local.ws_yt_2002, --local.ws_yt_2001
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.cs_yt_2002', 'local.cs_yt_2001',
  'local.ss_yt_2002', 'local.ss_yt_2001', 'local.ws_yt_2002', 'local.ws_yt_2001',
  which are not in the SELECT projection (line 33). Add them to SELECT, each
  prefixed with `--` so they stay out of the output rows — keep your HAVING
  as-is:
      select <your existing columns>, --local.cs_yt_2002, --local.cs_yt_2001,
  --local.ss_yt_2002, --local.ss_yt_2001, --local.ws_yt_2002, --local.ws_yt_2001
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ss_yt_2001', 'local.ws_yt_2001', 'local.cs_yt_2001'} out of
  with found {'ws.ext_wholesale_cost', 'ws.item.item_sk', 'local.cs_yt_2001',
  'ws.ext_discount_amt', 'ws.bill_customer.customer_sk', 'ws.ext_sales_price',
  'local.ss_yt_2001', 'ws.ext_list_price', 'ss.customer.customer_sk',
  'ws.order_number', 'cs.bill_customer.customer_sk', 'local.ws_yt_2001',
  'ws.sold_date.year'}
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY term out of
  range - should be between 1 and 1
  [SQL:
  WITH
  young as (
  SELECT
      "ws_web_sales"."ws_bill_customer_sk" as "ws_bill_customer_customer_sk",
      "ws_web_sales"."ws_sold_date_sk" as "ws_sold_date_date_sk"
  FROM
      "web_sales" as "ws_web_sales"
  GROUP BY
      1,
      2),
  highfalutin as (
  SELECT
      "cs_catalog_sales"."cs_bill_customer_sk" as "cs_bill_customer_customer_sk",
      "cs_catalog_sales"."cs_sold_date_sk" as "cs_sold_date_date_sk"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  GROUP BY
      1,
      2),
  yummy as (
  SELECT
      coales
  …
  "."ss_customer_customer_sk" =
  "scrawny"."ss_customer_customer_sk")
  SELECT
      "divergent"."ss_customer_customer_id" as "customer_code",
      "divergent"."ss_customer_first_name" as "ss_customer_first_name",
      "divergent"."ss_customer_last_name" as "ss_customer_last_name",
      coalesce("divergent"."pref_flag",'N') as "preferred_cust_flag"
  FROM
      "divergent"
      FULL JOIN "late" on "divergent"."ss_customer_customer_sk" is not distinct
  from "late"."ss_customer_customer_sk"
  ORDER BY
      "customer_code" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query04.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY term out of
  range - should be between 1 and 1
  [SQL:
  WITH
  sparkling as (
  SELECT
      "ws_web_sales"."ws_bill_customer_sk" as "ws_bill_customer_customer_sk",
      "ws_web_sales"."ws_ext_discount_amt" as "ws_ext_discount_amt",
      "ws_web_sales"."ws_ext_list_price" as "ws_ext_list_price",
      "ws_web_sales"."ws_ext_sales_price" as "ws_ext_sales_price",
      "ws_web_sales"."ws_ext_wholesale_cost" as "ws_ext_wholesale_cost",
      "ws_web_sales"."ws_sold_date_sk" as "ws_sold_date_date_sk"
  FROM
      "web_sales" as "ws_web_sales"
  GROUP BY
      1,
      2,

  …
  irst_name",
      "vacuous"."ss_customer_last_name" as "ss_customer_last_name",
      coalesce(CASE WHEN "vacuous"."ss_date_dim_year" = 2002 THEN
  "vacuous"."ss_customer_preferred_cust_flag" ELSE NULL END,'N') as
  "preferred_cust_flag"
  FROM
      "vacuous"
  GROUP BY
      1,
      2,
      3,
      4
  HAVING
      ( sum((( ( "vacuous"."ss_ext_list_price" -
  "vacuous"."ss_ext_wholesale_cost" ) - "vacuous"."ss_ext_discount_amt" ) +
  "vacuous"."ss_ext_sales_price") / CASE WHEN "vacuous"."ss_date_dim_year" = 2001
  THEN 2 ELSE NULL END) ) > 0

  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_filter_preferred_cust_flag_4235709056100541',
  'local._virt_filter_1907433764434820', 'local._virt_agg_sum_7038398333195918',
  'local._virt_agg_sum_8405755467471446', 'local._virt_agg_sum_855141700733479'}
  out of  with found {'local._virt_filter_preferred_cust_flag_4235709056100541',
  'ss.customer.first_name', 'local._virt_filter_1907433764434820',
  'ss.customer.last_name', 'ss.ext_list_price', 'ss.ext_sales_price',
  'ss.ext_wholesale_cost', 'ss.customer.customer_sk', 'ss.ext_discount_amt',
  'ss.customer.customer_id'}
  ```
- `trilogy run query04.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.pref_flag_2002', 'local.ws_yt_2002', 'local.ws_yt_2001',
  'local.ss_yt_2001'} out of  with found {'ss.customer.customer_id',
  'ss.customer.first_name', 'ss.customer.customer_sk', 'local.ss_yt_2001',
  'local.pref_flag_2002', 'ss.customer.last_name'}
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ss_2001', 'local.ss_2002', 'local.cs_2002', 'local.ws_2002',
  'local.ws_2001'} out of  with found {'local.ss_2001',
  'ws.bill_customer.customer_sk', 'local.ss_2002',
  'cs.bill_customer.customer_sk', 'local.cs_2002', 'local.ws_2002',
  'ss.customer.customer_sk', 'local.ws_2001'}
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_loss', 'local.store_profit'} out of  with found
  {'local.store_return_amount', 'local.store_sales_amount', 'local.store_loss',
  'store_returns.store.store_id', 'store_sales.store.store_id',
  'local.store_profit'}
  ```
- `trilogy run query06.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 13: ..."."param_zip_list" in (select INVALID_REFERENCE_BUG_<Missing source
  reference to local.pref_cust_zip_codes>."pref_cust_z...
                                                                           ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      unnest(STRING_SPLIT( $1 , ',' )) as "param_zip_list"
  ),
  highfalutin as (
  SELECT
      SUBSTRING("quizzical"."param_zip_list",1,2) as
  "_virt_func_substring_9221055437369198"
  FROM
      "quizzical"
  WHERE
      "quizzical"."param_zip_list" in (select INVALID_REFERENCE_BUG_<Missing
  …
  ighfalutin."_virt_func_substring_9221055437369198" from highfalutin where
  highfalutin."_virt_func_substring_9221055437369198" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "questionable"."store_sales_store_store_name" as
  "store_sales_store_store_name",
      sum("questionable"."store_sales_net_profit") as "total_net_profit"
  FROM
      "questionable"
  GROUP BY
      1
  ORDER BY
      "questionable"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY clause
  cannot contain aggregates!

  LINE 6:     SUBSTRING(CASE WHEN (
  count("store_sales_customer_customer"."c_customer_id") ...
                                    ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip",
      SUBSTRING(CASE WHEN (
  count("store_sales_customer_customer"."c_customer_id") ) > 10 and
  CONTAINS(LOWER((',' || $1 || ',')), LOWER((',' ||
  "store_sales_customer_customer_address_customer_address"."ca_zip" || ',')))
  THEN "stor
  …
  tore"."s_zip",1,2) in (select
  thoughtful."_virt_func_substring_9221055437369198" from thoughtful where
  thoughtful."_virt_func_substring_9221055437369198" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "yummy"."store_sales_store_store_name" as "store_sales_store_store_name",
      sum("yummy"."store_sales_net_profit") as "total_net_profit"
  FROM
      "yummy"
  GROUP BY
      1
  ORDER BY
      "yummy"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "render"

  LINE 23:     CASE WHEN ( INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_count_2439913080765832...
                                                         ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_customer_customer"."c_customer_id" as
  "_virt_filter_customer_id_9584118987298370",
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip"
  FROM
      "customer_address" as
  "store_sales_customer_customer_address_customer_address"
      INNER
  …
  e"."s_zip",1,2) in (select
  cooperative."_virt_func_substring_9221055437369198" from cooperative where
  cooperative."_virt_func_substring_9221055437369198" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "juicy"."store_sales_store_store_name" as "store_sales_store_store_name",
      sum("juicy"."store_sales_net_profit") as "total_net_profit"
  FROM
      "juicy"
  GROUP BY
      1
  ORDER BY
      "juicy"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "render"

  LINE 23:     CASE WHEN ( INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_count_2439913080765832...
                                                         ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_customer_customer"."c_customer_id" as
  "_virt_filter_customer_id_9584118987298370",
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip"
  FROM
      "customer_address" as
  "store_sales_customer_customer_address_customer_address"
      INNER
  …
  sales_store_store"."s_zip",1,2) in (select
  cooperative."_virt_filter_1501940812236188" from cooperative where
  cooperative."_virt_filter_1501940812236188" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "yummy"."store_sales_store_store_name" as "store_sales_store_store_name",
      sum("yummy"."store_sales_net_profit") as "total_net_profit"
  FROM
      "yummy"
  GROUP BY
      1
  ORDER BY
      "yummy"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: WHERE clause cannot
  contain aggregates!

  LINE 23: ... CONTAINS(LOWER((',' || $1 || ',')), LOWER((',' || CASE WHEN (
  count("wakeful"."_virt_filter_customer_id_9584118987298370...
                                                                             ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "store_sales_customer_customer"."c_customer_id" as
  "_virt_filter_customer_id_9584118987298370",
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip"
  FROM
      "customer_address" as
  "store_sales_custo
  …
  tore_store"."s_zip",1,2) in (select
  cheerful."_virt_func_substring_9221055437369198" from cheerful where
  cheerful."_virt_func_substring_9221055437369198" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "yummy"."store_sales_store_store_name" as "store_sales_store_store_name",
      sum("yummy"."store_sales_net_profit") as "total_net_profit"
  FROM
      "yummy"
  GROUP BY
      1
  ORDER BY
      "yummy"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "quizzical" not found!
  Candidate tables: "store_sales_store_sales"

  LINE 39: ...TRING("store_sales_store_store"."s_zip",1,2) in
  (SUBSTRING("quizzical"."_virt_unnest_3575542089691502",1,2))
                                                                         ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      SUBSTRING("store_sales_customer_customer_address_customer_address"."ca_zip"
  ,1,2) as "_virt_func_substring_1167221594554121"
  FROM
      "customer_address" as
  "store_sales_customer_customer_address_customer_address"
      INNER JOIN "customer"
  …
  irt_func_substring_1167221594554121" is not null) and
  SUBSTRING("store_sales_store_store"."s_zip",1,2) in
  (SUBSTRING("quizzical"."_virt_unnest_3575542089691502",1,2))

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "juicy"."store_sales_store_store_name" as "store_sales_store_store_name",
      sum("juicy"."store_sales_net_profit") as "total_net_profit"
  FROM
      "juicy"
  GROUP BY
      1
  ORDER BY
      "juicy"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY clause
  cannot contain aggregates!

  LINE 26:     CASE WHEN (
  count("cheerful"."_virt_filter_customer_id_9584118987298370...
                           ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_customer_customer"."c_customer_id" as
  "_virt_filter_customer_id_9584118987298370",
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip"
  FROM
      "customer_address" as
  "store_sales_customer_customer_address_customer_address"
      INNER JOIN "customer" as "store_sales_customer_custo
  …
  "."ca_zip" in
  (select quizzical."_virt_func_unnest_3575542089691502" from quizzical where
  quizzical."_virt_func_unnest_3575542089691502" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "concerned"."store_sales_store_store_name" as
  "store_sales_store_store_name",
      sum("concerned"."store_sales_net_profit") as "total_net_profit"
  FROM
      "concerned"
  GROUP BY
      1
  ORDER BY
      "concerned"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY clause
  cannot contain aggregates!

  LINE 23:     SUBSTRING(CASE WHEN (
  count("cheerful"."_virt_filter_customer_id_9584118987298370...
                                     ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      unnest(STRING_SPLIT( $1 , ',' )) as "param_zips"
  ),
  cheerful as (
  SELECT
      "store_sales_customer_customer"."c_customer_id" as
  "_virt_filter_customer_id_9584118987298370",
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip"
  FROM
      "customer_address" as
  "store_sales_cu
  …
  tore"."s_zip",1,2) in (select
  thoughtful."_virt_func_substring_9221055437369198" from thoughtful where
  thoughtful."_virt_func_substring_9221055437369198" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "juicy"."store_sales_store_store_name" as "store_sales_store_store_name",
      sum("juicy"."store_sales_net_profit") as "total_net_profit"
  FROM
      "juicy"
  GROUP BY
      1
  ORDER BY
      "juicy"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY clause
  cannot contain aggregates!

  LINE 24:     CASE WHEN (
  count("cheerful"."_virt_filter_customer_id_9584118987298370...
                           ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      unnest(STRING_SPLIT( $1 , ',' )) as "param_zips"
  ),
  cheerful as (
  SELECT
      "store_sales_customer_customer"."c_customer_id" as
  "_virt_filter_customer_id_9584118987298370",
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "param_only_zips"
  FROM
      "customer_address" as
  "store_sales_customer_customer_address_customer_address"

  …
  aram_zips" from quizzical where quizzical."param_zips" is
  not null) and "store_sales_customer_customer"."c_preferred_cust_flag" = 'Y'

  GROUP BY
      1,
      2
  HAVING
      ( count("store_sales_customer_customer"."c_customer_id") ) > 10
  )
  SELECT
      CASE WHEN ( count("cheerful"."_virt_filter_customer_id_9584118987298370") )
  > 10 THEN "cheerful"."param_only_zips" ELSE NULL END as "qualifying_zips"
  FROM
      "cheerful"
  GROUP BY
      1,
      "cheerful"."_virt_filter_customer_id_9584118987298370",
      "cheerful"."param_only_zips"
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `undefined-concept`

- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['ss.customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['ss.customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['c.first_name', 'c.last_name',
  'ss.customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['ss.customer.first_name']")
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr merge sr.item.item_sk into ~ss.item.item_sk; merge sr.ticket_number into ~ss.ticket_number; select ss.sr.return_amt limit 3;`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  ss.sr.return_amt. Suggestions: ['sr.return_amt', 'sr.return_tax',
  'sr.return_quantity']")
  ```
- `trilogy run query06.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.current_price. Suggestions: ['store_sales.item.current_price',
  'store_sales.promotion.item.current_price']")
  ```
- `trilogy run query06.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  customer.customer_address.state. Suggestions:
  ['store_sales.customer.customer_address.state',
  'store_sales.customer_address.state',
  'store_sales.customer.customer_address.city']")
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  store_sales.customer.preferred_cust_flag.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.preferred_cust_flag.')
  ```

### `syntax-parse`

- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ws.sold_date.date_sk; auto cat_daily <- sum(cs.ext_sales_…s.sold_date.dow, daily_total as val, lag(daily_total, 53) over (partition by cs.sold_date.dow order by cs.sold_date.week_seq) as val_53_ago limit 20;`

  ```text
  --> 3:310
    |
  3 | merge cs.sold_date.date_sk into ws.sold_date.date_sk; auto cat_daily <-
  sum(cs.ext_sales_price) by cs.sold_date.date_sk; auto web_daily <-
  sum(ws.ext_sales_price) by ws.sold_date.date_sk; auto daily_total <-
  coalesce(cat_daily,0) + coalesce(web_daily,0); where cs.sold_date.year=2001 or
  cs.sold_date.year=2002; select cs.sold_date.year, cs.sold_date.week_seq,
  cs.sold_date.dow, daily_total as val, lag(daily_total, 53) over (partition by
  cs.sold_date.dow order by cs.sold_date.week_seq) as val_53_ago limit 20;
    |
  ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...2001 or cs.sold_date.year=2002 ??? ; select cs.sold_date.year, cs...
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r… total_returns,
    sum(st_prof) - sum(st_loss) by rollup channel, outlet as total_profit
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 19, column 124.
  Expected one of:
          * _TERMINATOR

  Location:
  ...e and '2000-09-06'::date), 0) ??? by ss.store.store_id; auto st_...

  Write stats: received 2794 chars / 2794 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r… total_returns,
    sum(st_prof) - sum(st_loss) by rollup channel, outlet as total_profit
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 19, column 124.
  Expected one of:
          * _TERMINATOR

  Location:
  ...e and '2000-09-06'::date), 0) ??? by ss.store.store_id; auto st_...

  Write stats: received 2794 chars / 2794 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  ```
- `trilogy run select case(1, 1, 'store channel', 2, 'catalog channel', 'web channel') as test;`

  ```text
  --> 1:73
    |
  1 | select case(1, 1, 'store channel', 2, 'catalog channel', 'web channel') as
  test;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or fcase_simple_when
  Location:
  ...alog channel', 'web channel') ??? as test;
  ```
- `trilogy run --import raw.store_sales:ss --import raw.item:item select item.item_id, sum(ss.ext_sales_price) as s by ss.store.store_id limit 2;`

  ```text
  --> 3:51
    |
  3 | select item.item_id, sum(ss.ext_sales_price) as s by ss.store.store_id
  limit 2;
    |                                                   ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
   sum(ss.ext_sales_price) as s ??? by ss.store.store_id limit 2;
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…, 0) as total_returns,
    coalesce(st_net, 0) - coalesce(st_netloss, 0) as total_profit
order by channel nulls first, outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 43, column 7.
  Expected one of:
          * WHEN

  Location:
  ...nel',         'unknown'     ) ??? as channel,     concat('store'...

  Write stats: received 2752 chars / 2752 bytes; tail: …'order by channel nulls
  first, outlet nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query08.preql --content parameter zips string;
import raw.store_sales as store_sales;

# First filter: customer_address ZIPs that appear i…ring(qualifying_zips, 1, 2)
group by store_sales.store.store_name, store_sales.store.zip, zip_prefix
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it.
  Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate
  at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g.
  `sum(sales.amount) by sales.store.id`).
  Location:
  ...string(qualifying_zips, 1, 2) ??? group by store_sales.store.sto...

  Write stats: received 1046 chars / 1046 bytes; tail: …' zip_prefix\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  ```

### `join-resolution`

- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws select cs.sold_date.date_sk, ws.sold_date.date_sk limit 3;`

  ```text
  Could not resolve connections for query with output
  ['cs.sold_date.date_sk<Purpose.KEY>Derivation.ROOT>',
  'ws.sold_date.date_sk<Purpose.KEY>Derivation.ROOT>'] from current model. The
  output draws on models that are not connected in the current graph: cs (needed
  by cs.sold_date.date_sk); ws (needed by ws.sold_date.date_sk). If these should
  be related, bridge their keys with a merge, e.g. `merge cs.<key> into
  ~ws.<key>;`.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.catalog_sales:cs select ss.store.store_id, cs.catalog_page.catalog_page_id, sum(ss.ext_sales_price) as s, sum(cs.ext_sales_price) as c limit 3;`

  ```text
  Could not resolve connections for query with output
  ['ss.store.store_id<Purpose.PROPERTY>Derivation.ROOT>',
  'cs.catalog_page.catalog_page_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.s<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.c<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. The output
  draws on models that are not connected in the current graph: cs (needed by
  cs.catalog_page.catalog_page_id, local.c, local.s); ss (needed by local.c,
  local.s, ss.store.store_id). If these should be related, bridge their keys with
  a merge, e.g. `merge cs.<key> into ~ss.<key>;`.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Could not resolve connections for query with output
  ['local._virt_func_substring_9221055437369198<Purpose.PROPERTY>Derivation.BASIC
  >'] from current model.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Could not resolve connections for query with output
  ['local._virt_func_substring_9221055437369198<Purpose.PROPERTY>Derivation.BASIC
  >'] from current model. The output draws on models that are not connected in
  the current graph: customer (needed by
  local._virt_func_substring_9221055437369198); customer_address (needed by
  local._virt_func_substring_9221055437369198). If these should be related,
  bridge their keys with a merge, e.g. `merge customer.<key> into
  ~customer_address.<key>;`.
  ```

### `syntax-missing-alias`

- `trilogy file write query08.preql --content parameter zips string;
import raw.store_sales as store_sales;

# First filter: customer_address ZIPs that appear i…ss.zip
    ? store_sales.customer.customer_address.zip in param_zips;

# Check what params are matched
select distinct
    param_only_zips
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct
      param_only_zips as distinct_param_only_zips`
  Location:
   distinct     param_only_zips ??? limit 100;

  Write stats: received 388 chars / 388 bytes; tail: …'s are matched\\nselect
  distinct\\n    param_only_zips\\nlimit 100;'.
  ```
