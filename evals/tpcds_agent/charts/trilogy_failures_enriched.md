# Trilogy failure analysis — 20260530-040508

- Run `20260530-040508_enriched` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 873 | failed: 143 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 73 | 51% |
| `syntax-parse` | 31 | 22% |
| `join-resolution` | 14 | 10% |
| `syntax-missing-alias` | 10 | 7% |
| `undefined-concept` | 10 | 7% |
| `cli-misuse` | 4 | 3% |
| `type-error` | 1 | 1% |

## Detail

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 87 (char 86). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 936 (char 935). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query04.preql`

  ```text
  Have
  {'GroupNode<local.catalog_2001,store_sales.billing_customer.id>': None,
  'GroupNode<local.store_2001,store_sales.billing_customer.id>': None,
  'GroupNode<local.web_2001,web_sales.billing_customer.id>': None,
  'GroupNode<store_sales.billing_customer.first_name,store_sales.billing_customer
  .last_name,store_sales.billing_customer.preferred_cust_flag...1 more>': None}
  and need local.store_2001 > 0 and local.catalog_2001 > 0 and local.web_2001 > 0
  and local.catalog_ratio > local.store_ratio and local.catalog_ratio >
  local.web_ratio
  ```
- `trilogy run query04.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.catalog_yt', 'local.web_yt', 'local.store_yt'} out of  with
  found {'local.store_yt', 'web_sales.date.year', 'local.catalog_yt',
  'catalog_sales.sold_date.year', 'catalog_sales.bill_customer.id',
  'web_sales.billing_customer.id', 'store_sales.billing_customer.id',
  'local.web_yt', 'store_sales.date.year'}
  ```
- `trilogy run query04.preql`

  ```text
  Have
  {'GroupNode<local.c_2001,store_sales.billing_customer.id>': None,
  'GroupNode<local.s_2001,store_sales.billing_customer.id>': None,
  'GroupNode<local.w_2001,web_sales.billing_customer.id>': None,
  'GroupNode<store_sales.billing_customer.first_name,store_sales.billing_customer
  .last_name,store_sales.billing_customer.preferred_cust_flag...1 more>': None}
  and need local.s_2001 > 0 and local.c_2001 > 0 and local.w_2001 > 0 and
  local.c_ratio > local.s_ratio and local.c_ratio > local.w_ratio
  ```
- `trilogy run query04.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.c_2001', 'local.w_2001', 'local.s_2001'} out of  with found
  {'web_sales.ext_sales_price', 'local.s_2001', 'web_sales.billing_customer.id',
  'local.c_2001', 'store_sales.billing_customer.id',
  'web_sales.item.wholesale_cost', 'web_sales.ext_discount_amount',
  'web_sales.item.id', 'local.w_2001', 'web_sales.quantity',
  'web_sales.list_price', 'web_sales.date.year', 'web_sales.order_number',
  'catalog_sales.bill_customer.id'}
  ```
- `trilogy run query04.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.s_2001', 'local.w_2001', 'local.c_2001'} out of  with found
  {'local.s_2001', 'local.w_2001', 'web_sales.list_price',
  'web_sales.ext_discount_amount', 'web_sales.order_number',
  'catalog_sales.bill_customer.id', 'web_sales.date.year', 'web_sales.quantity',
  'web_sales.item.wholesale_cost', 'web_sales.ext_sales_price',
  'web_sales.item.id', 'local.c_2001', 'web_sales.billing_customer.id',
  'store_sales.billing_customer.id'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql`

  ```text
  Unable to import '.\all_sales.preql': [Errno 2] No such file
  or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy file write query05.preql --content None`

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run --import raw.all_sales:all_sales --all-rows --displayed-rows 200 select
    all_sales.sales_channel,
    concat(
        case all_sales.sales_cha…all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by
    all_sales.sales_channel asc,
    outlet asc nulls first
limit 100;`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "all_sales_sales_channel" must appear in the GROUP BY clause or must be part of
  an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(all_sales_sales_channel)"
  if the exact value of "all_sales_sales_channel" is not important.

  LINE 138:     "sweltering"."all_sales_sales_channel" as "all_sales_sales_...
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as
  "all_sales_channel_dim_id",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as
  "all_sales_channel_dim_t
  …
  sales_sales_channel" = "abhorrent"."all_sales_sales_channel")
  SELECT
      "sweltering"."all_sales_sales_channel" as "all_sales_sales_channel",
      "sweltering"."outlet" as "outlet",
      "sweltering"."total_sales" as "total_sales",
      sum("sweltering"."all_sales_return_amount") as "total_returns",
      sum("sweltering"."all_sales_net_profit") -
  sum("sweltering"."all_sales_return_net_loss") as "profit"
  FROM
      "sweltering"
  ORDER BY
      "sweltering"."all_sales_sales_channel" asc,
      "sweltering"."outlet" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write query05.preql --content None`

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run query05.preql --all-rows --displayed-rows 200`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "all_sales_sales_channel" must appear in the GROUP BY clause or must be part of
  an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(all_sales_sales_channel)"
  if the exact value of "all_sales_sales_channel" is not important.

  LINE 138:     "sweltering"."all_sales_sales_channel" as "all_sales_sales_...
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as
  "all_sales_channel_dim_id",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as
  "all_sales_channel_dim_t
  …
  sales_sales_channel" = "abhorrent"."all_sales_sales_channel")
  SELECT
      "sweltering"."all_sales_sales_channel" as "all_sales_sales_channel",
      "sweltering"."outlet" as "outlet",
      "sweltering"."total_sales" as "total_sales",
      sum("sweltering"."all_sales_return_amount") as "total_returns",
      sum("sweltering"."all_sales_net_profit") -
  sum("sweltering"."all_sales_return_net_loss") as "profit"
  FROM
      "sweltering"
  ORDER BY
      "sweltering"."all_sales_sales_channel" asc,
      "sweltering"."outlet" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql --all-rows --displayed-rows 200`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "all_sales_sales_channel" must appear in the GROUP BY clause or must be part of
  an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(all_sales_sales_channel)"
  if the exact value of "all_sales_sales_channel" is not important.

  LINE 138:     "sweltering"."all_sales_sales_channel" as "all_sales_sales_...
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as
  "all_sales_channel_dim_id",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as
  "all_sales_channel_dim_t
  …
  sales_sales_channel" = "abhorrent"."all_sales_sales_channel")
  SELECT
      "sweltering"."all_sales_sales_channel" as "all_sales_sales_channel",
      "sweltering"."outlet" as "outlet",
      "sweltering"."total_sales" as "total_sales",
      sum("sweltering"."all_sales_return_amount") as "total_returns",
      sum("sweltering"."all_sales_net_profit") -
  sum("sweltering"."all_sales_return_net_loss") as "profit"
  FROM
      "sweltering"
  ORDER BY
      "sweltering"."all_sales_sales_channel" asc,
      "sweltering"."outlet" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Have
  {'MergeNode<store_sales.item.id,store_sales.net_profit,store_sales.store.date.q
  uarter...3 more>': None} and need store_sales.store.date.year = 1998 and
  store_sales.store.date.quarter = 2 and local.param_match@Grain<store.id> and
  local.pref_match@Grain<customer.address.id,store.id>
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Have
  {'MergeNode<store_sales.item.id,store_sales.net_profit,store_sales.store.date.q
  uarter...3 more>': None} and need store_sales.store.date.year = 1998 and
  store_sales.store.date.quarter = 2 and local.param_match@Grain<store.id> and
  local.pref_match@Grain<customer.address.id,store.id>
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Have
  {'MergeNode<store_sales.item.id,store_sales.net_profit,store_sales.store.date.q
  uarter...3 more>': None} and need store_sales.store.date.year = 1998 and
  store_sales.store.date.quarter = 2 and local.param_match@Grain<store.id> and
  local.pref_match@Grain<customer.address.id,store.id>
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Have
  {'MergeNode<store_sales.item.id,store_sales.net_profit,store_sales.store.date.q
  uarter...3 more>': None} and need store_sales.store.date.year = 1998 and
  store_sales.store.date.quarter = 2 and local.param_match@Grain<store.id> and
  local.pref_match@Grain<customer.address.id,store.id>
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Table
  "store_sales_store_store" does not have a column named "S_CLOSED_DATE"

  Candidate bindings: : "s_closed_date_sk"

  LINE 30: ...    INNER JOIN "date_dim" as "store_sales_store_date_date" on
  "store_sales_store_store"."S_CLOSED_DATE" = "store_sales_st...
                                                                            ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "customer_address_customer_address"."CA_ZIP" as "customer_address_zip",
      count("customer_customers"."C_CUSTOMER_SK") as "pref_zip_count"
  FROM
      "customer_address" as "customer_addres
  …
  XP_MATCHES($1,('(^|,)' ||
  SUBSTRING("store_sales_store_store"."S_ZIP",1,2) || '\\d{3}($|,)')) and
  SUBSTRING("store_sales_store_store"."S_ZIP",1,2) in
  (SUBSTRING("thoughtful"."_virt_filter_zip_4720087241414976",1,2))

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "juicy"."store_sales_store_name" as "store_name",
      sum("juicy"."store_sales_net_profit") as "total_net_profit"
  FROM
      "juicy"
  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "thoughtful" not found!
  Candidate tables: "store_sales_date_date"

  LINE 32: ...TRING("store_sales_store_store"."S_ZIP",1,2) in
  (SUBSTRING("thoughtful"."_virt_filter_zip_4720087241414976",1,2))
                                                                         ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "customer_address_customer_address"."CA_ZIP" as "customer_address_zip",
      count("customer_customers"."C_CUSTOMER_SK") as "pref_zip_count"
  FROM
      "customer_address" as "customer_address_customer_address"
      INNER JOIN "customer" as "
  …
  XP_MATCHES($1,('(^|,)' ||
  SUBSTRING("store_sales_store_store"."S_ZIP",1,2) || '\\d{3}($|,)')) and
  SUBSTRING("store_sales_store_store"."S_ZIP",1,2) in
  (SUBSTRING("thoughtful"."_virt_filter_zip_4720087241414976",1,2))

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "juicy"."store_sales_store_name" as "store_name",
      sum("juicy"."store_sales_net_profit") as "total_net_profit"
  FROM
      "juicy"
  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "thoughtful" not found!
  Candidate tables: "store_sales_date_date"

  LINE 32: ...TRING("store_sales_store_store"."S_ZIP",1,2) in
  (SUBSTRING("thoughtful"."_virt_filter_zip_4720087241414976",1,2)) and...
                                                                         ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "customer_address_customer_address"."CA_ZIP" as "customer_address_zip",
      count("customer_customers"."C_CUSTOMER_SK") as "pref_zip_count"
  FROM
      "customer_address" as "customer_address_customer_address"
      INNER JOIN "custome
  …
  TRING("store_sales_store_store"."S_ZIP",1,2) in
  (SUBSTRING("thoughtful"."_virt_filter_zip_4720087241414976",1,2)) and
  REGEXP_MATCHES($1,('(^|,)' || SUBSTRING("store_sales_store_store"."S_ZIP",1,2)
  || '\\d{3}($|,)'))

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "juicy"."store_sales_store_name" as "store_name",
      sum("juicy"."store_sales_net_profit") as "total_net_profit"
  FROM
      "juicy"
  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "cooperative" not found!
  Candidate tables: "store_store"

  LINE 29:     SUBSTRING("store_store"."S_ZIP",1,2) in
  (SUBSTRING("cooperative"."_virt_filter_zip_4720087241414976",1,2))
                                                                  ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "customer_address_customer_address"."CA_ZIP" as "customer_address_zip",
      count("customer_customers"."C_CUSTOMER_SK") as "pref_zip_count"
  FROM
      "customer_address" as "customer_address_customer_address"
      INNER JOIN "customer" as "customer_customers" o
  …
  ilter_zip_4720087241414976"
  FROM
      "cheerful"
      INNER JOIN "customer_address" as "customer_address_customer_address" on
  "cheerful"."customer_address_zip" =
  "customer_address_customer_address"."CA_ZIP"
  GROUP BY
      1)
  SELECT
      SUBSTRING("store_store"."S_ZIP",1,2) as "prefix",
      count("store_store"."S_STORE_SK") as "cnt"
  FROM
      "store" as "store_store"
  WHERE
      SUBSTRING("store_store"."S_ZIP",1,2) in
  (SUBSTRING("cooperative"."_virt_filter_zip_4720087241414976",1,2))

  GROUP BY
      1
  ORDER BY
      "prefix" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query08.preql --param zips=24128`

  ```text
  Have
  {'MergeNode<store_sales.date.quarter,store_sales.date.year,store_sales.item.id.
  ..3 more>': None} and need store_sales.date.year = 1998 and
  store_sales.date.quarter = 2 and
  regexp_contains(:local_zips,concat((^|,),local.store_prefix@Grain<store.id>,\\d
  {3}($|,))) and
  BuildSubselectComparison(left=local.store_prefix@Grain<store.id>,
  right=(substring(local._virt_filter_zip_4720087241414976@Grain<customer.address
  .id>,1,2)), operator=<ComparisonOperator.IN: 'in'>)
  ```
- `trilogy run query08.preql --param zips=24128`

  ```text
  Have
  {'MergeNode<store_sales.date.quarter,store_sales.date.year,store_sales.item.id.
  ..3 more>': None} and need store_sales.date.year = 1998 and
  store_sales.date.quarter = 2 and local.param_zip_check@Grain<store.id> and
  local.pref_zip_check@Grain<customer.address.id,store.id>
  ```
- `trilogy run query08.preql --param zips=24128`

  ```text
  Have
  {'MergeNode<store_sales.date.quarter,store_sales.date.year,store_sales.item.id.
  ..3 more>': None} and need store_sales.date.year = 1998 and
  store_sales.date.quarter = 2 and local.param_zip_check@Grain<store.id> and
  local.pref_zip_check@Grain<customer.address.id,store.id>
  ```
- `trilogy run -`

  ```text
  Cannot reference an aggregate derived in the select
  (local.qty_per_ticket) in the same statement where clause; move to the HAVING
  clause instead; Line: 2
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 100 (char 99). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 80 (char 79). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local._virt_agg_count_89820483538232 >= 1 and
  (local._virt_agg_count_6124933137961963 >= 1 or
  local._virt_agg_count_8306150851820695 >= 1)
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local._virt_agg_count_89820483538232 >= 1 and
  (local._virt_agg_count_6124933137961963 >= 1 or
  local._virt_agg_count_8306150851820695 >= 1)
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local._virt_agg_count_89820483538232 >= 1 and
  (local._virt_agg_count_6124933137961963 >= 1 or
  local._virt_agg_count_8306150851820695 >= 1)
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local._virt_agg_count_2903771304087651 >= 1 and
  (local._virt_agg_count_6365333489879973 >= 1 or
  local._virt_agg_count_4211921610073389 >= 1)
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local._virt_agg_count_2903771304087651 >= 1 and
  (local._virt_agg_count_6365333489879973 >= 1 or
  local._virt_agg_count_4211921610073389 >= 1)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 251 (char 250). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 81 (char 80). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_avg_sale', which is not in
  the SELECT projection (line 7). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.overall_avg_sale`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_avg_sale', which is not in
  the SELECT projection (line 8). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.overall_avg_sale`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 106: ...sales_quantity" * CASE WHEN INVALID_REFERENCE_BUG_<Missing source
  reference to all_sales.date.year> BETWEEN 1999 AND...
                                                                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
       'CATALOG'  as "all_sales_sales_channel",
      "all_sales_catalog_sales_unified"."CS_LIST_PRICE" as
  "all_sales_list_price",
  …
  "g_class",
      grouping("vacuous"."all_sales_item_category_id") as "g_category"
  FROM
      "vacuous"
  GROUP BY
      ROLLUP (1, 2, 3, 4)
  HAVING
      "total_sales" > avg("vacuous"."all_sales_quantity" * CASE WHEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.date.year> BETWEEN
  1999 AND 2001 THEN INVALID_REFERENCE_BUG_<Missing source reference to
  all_sales.list_price> ELSE NULL END)

  ORDER BY
      "channel" asc nulls first,
      "brand" asc nulls first,
      "class" asc nulls first,
      "category" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.over_all_avg', which is not in the
  SELECT projection (line 7). Fix one of: (a) add it to SELECT — prefix with `--`
  to keep it out of the output rows, e.g. `select ..., --local.over_all_avg`; (b)
  move the filter to WHERE — for an aggregate condition on a non-output grain,
  write the aggregate inline as `agg(x) by grain` directly in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 115: ...sales_quantity" * CASE WHEN INVALID_REFERENCE_BUG_<Missing source
  reference to all_sales.date.year> BETWEEN 1999 AND...
                                                                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
       'CATALOG'  as "all_sales_sales_channel",
      "all_sales_catalog_sales_unified"."CS_LIST_PRICE" as
  "all_sales_list_price",
  …
  sum("juicy"."all_sales_quantity" * CASE WHEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.date.year> BETWEEN
  1999 AND 2001 THEN INVALID_REFERENCE_BUG_<Missing source reference to
  all_sales.list_price> ELSE NULL END) / count(CASE WHEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.date.year> BETWEEN
  1999 AND 2001 THEN "juicy"."all_sales_item_id" ELSE NULL END)

  ORDER BY
      "channel" asc nulls first,
      "brand" asc nulls first,
      "class" asc nulls first,
      "category" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query14.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 115: ...sales_quantity" * CASE WHEN INVALID_REFERENCE_BUG_<Missing source
  reference to all_sales.date.year> BETWEEN 1999 AND...
                                                                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
       'CATALOG'  as "all_sales_sales_channel",
      "all_sales_catalog_sales_unified"."CS_LIST_PRICE" as
  "all_sales_list_price",
  …
  "."all_sales_quantity" * CASE WHEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.date.year> BETWEEN
  1999 AND 2001 THEN INVALID_REFERENCE_BUG_<Missing source reference to
  all_sales.list_price> ELSE NULL END) / nullif(count(CASE WHEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.date.year> BETWEEN
  1999 AND 2001 THEN "juicy"."all_sales_item_id" ELSE NULL END),0)

  ORDER BY
      "channel" asc nulls first,
      "brand" asc nulls first,
      "class" asc nulls first,
      "category" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query14.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 115: ...sales_quantity" * CASE WHEN INVALID_REFERENCE_BUG_<Missing source
  reference to all_sales.date.year> BETWEEN 1999 AND...
                                                                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
       'CATALOG'  as "all_sales_sales_channel",
      "all_sales_catalog_sales_unified"."CS_LIST_PRICE" as
  "all_sales_list_price",
  …
  um("juicy"."all_sales_quantity" *
  CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to
  all_sales.date.year> BETWEEN 1999 AND 2001 THEN INVALID_REFERENCE_BUG_<Missing
  source reference to all_sales.list_price> ELSE NULL END) / count(CASE WHEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.date.year> BETWEEN
  1999 AND 2001 THEN "juicy"."all_sales_item_id" ELSE NULL END)

  ORDER BY
      "channel" asc nulls first,
      "brand" asc nulls first,
      "class" asc nulls first,
      "category" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query14.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 115: ...sales_quantity" * CASE WHEN INVALID_REFERENCE_BUG_<Missing source
  reference to all_sales.date.year> BETWEEN 1999 AND...
                                                                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
       'CATALOG'  as "all_sales_sales_channel",
      "all_sales_catalog_sales_unified"."CS_LIST_PRICE" as
  "all_sales_list_price",
  …
  sum("juicy"."all_sales_quantity" * CASE WHEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.date.year> BETWEEN
  1999 AND 2001 THEN INVALID_REFERENCE_BUG_<Missing source reference to
  all_sales.list_price> ELSE NULL END) / count(CASE WHEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.date.year> BETWEEN
  1999 AND 2001 THEN "juicy"."all_sales_item_id" ELSE NULL END)

  ORDER BY
      "channel" asc nulls first,
      "brand" asc nulls first,
      "class" asc nulls first,
      "category" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_sum_sales', which is not in
  the SELECT projection (line 8). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.overall_sum_sales`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 115: ...sales_quantity" * CASE WHEN INVALID_REFERENCE_BUG_<Missing source
  reference to all_sales.date.year> BETWEEN 1999 AND...
                                                                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
       'CATALOG'  as "all_sales_sales_channel",
      "all_sales_catalog_sales_unified"."CS_LIST_PRICE" as
  "all_sales_list_price",
  …
  sum("juicy"."all_sales_quantity" * CASE WHEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.date.year> BETWEEN
  1999 AND 2001 THEN INVALID_REFERENCE_BUG_<Missing source reference to
  all_sales.list_price> ELSE NULL END) / count(CASE WHEN
  INVALID_REFERENCE_BUG_<Missing source reference to all_sales.date.year> BETWEEN
  1999 AND 2001 THEN "juicy"."all_sales_item_id" ELSE NULL END)

  ORDER BY
      "channel" asc nulls first,
      "brand" asc nulls first,
      "class" asc nulls first,
      "category" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query14.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "<"

  LINE 187: ...l_sales" > "scrawny"."overall_sum" / INVALID_REFERENCE_BUG_<Cannot
  render aggregate local._virt_agg_count_639529659470...
                                                                          ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_sales_unified"."CS_ORDER_NUMBER" as
  "all_sales_order_id",
       'CATALOG'  as "all_sales_sales_channel",

  …
  wny"."overall_sum" /
  INVALID_REFERENCE_BUG_<Cannot render aggregate
  local._virt_agg_count_6395296594708827 in CTE friendly: source_map miss and CTE
  grain Grain<local.brand,local.category,local.channel,local.class> != aggregate
  by-grain <['all_sales.item.brand_id', 'all_sales.item.category_id',
  'all_sales.item.class_id', 'all_sales.sales_channel']>>

  ORDER BY
      "scrawny"."channel" asc nulls first,
      "scrawny"."brand" asc nulls first,
      "scrawny"."class" asc nulls first,
      "scrawny"."category" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 86 (char 85). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 95 (char 94). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 99 (char 98). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 95 (char 94). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 4 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
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
  trilogy error: 'args' must be a list of strings.
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
  trilogy error: 'args' must be a list of strings.
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
  trilogy error: 'args' must be a list of strings.
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
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 90 (char 89). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```

### `syntax-parse`

- `trilogy file write query01.preql`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 13, column 51.
  Expected one of:
          * SELECT

  Location:
  ...urns > 1.2 * avg_store_returns ??? ;  select   physical_sales....

  Write stats: received 617 chars / 617 bytes; tail: …'der by\r\\n
  physical_sales.return_customer.text_id\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql --escapes -c import raw.physical_sales as physical_sales;

# total returns per customer per store
auto cust_store_returns <-…tore_returns > 1.2 * avg_store_returns;

select
  physical_sales.return_customer.text_id
order by
  physical_sales.return_customer.text_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 13, column 51.
  Expected one of:
          * SELECT

  Location:
  ...urns > 1.2 * avg_store_returns ??? ;  select   physical_sales.ret...

  Write stats: received 599 chars / 599 bytes; tail: …'order by\\n
  physical_sales.return_customer.text_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Restrict to web and catalog only (no store)
where all_sales.sales_channel in…y - let me try a simpler approach: use window functions (lag)
# to shift 2002 values back 53*7 days = 371 days worth of week_seqs

select 1;
limit 1;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 4, column 52.
  Expected one of:
          * SELECT

  Location:
  ..._channel in ('WEB', 'CATALOG') ??? ;  # Daily ext_sales_price per...

  Write stats: received 3202 chars / 3202 bytes; tail: …' 53*7 days = 371 days
  worth of week_seqs\\n\\nselect 1;\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

auto s2001 <- sum(all_sales.ext_sales_price ? all_sales.sales_channel in ('WEB… as sun, mon_2002 as mon, tue_2002 as tue, wed_2002 as wed, thu_2002 as thu, fri_2002 as fri, sat_2002 as sat, 2002 as yr)
);

select 1 as x limit 1;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
   auto all_weeks <- union(     ??? (select all_sales.date.week_se...

  Write stats: received 2328 chars / 2328 bytes; tail: …'fri, sat_2002 as sat,
  2002 as yr)\\n);\\n\\nselect 1 as x limit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…red_cust_flag as c_pref
order by c_code asc nulls first, c_first_name asc nulls first, c_last_name asc nulls first, c_pref asc nulls first
limit 100;`

  ```text
  …
   * SELF_IMPORT
          * SHOW
          * AUTO
          * DATASOURCE_PARTIAL
          * CREATE
          * DATASOURCE
          * PROPERTY
          * UNIQUE
          * FROM
          * VALIDATE
          * PERSIST_MODE
          * PURPOSE
          * $END
          * CHART
          * WITH
          * PARSE_COMMENT
          * SHORTHAND_MODIFIER
          * DEF
          * COPY
          * _PROPERTIES
          * SELECT
          * _DEF_TABLE
          * MOCK
          * PUBLISH_ACTION
          * RAW_SQL
          * IMPORT
          * "merge"i
          * PARAM
          * DATASOURCE_ROOT
  Previous tokens: [Token('LINE_SEPARATOR', '\n\n')]

  Location:
  ...b_sales.billing_customer.id;  ??? having c_2001 > 0 and s_2001 >...

  Write stats: received 2486 chars / 2486 bytes; tail: …'last_name asc nulls
  first, c_pref asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.web_sales:web_sales select web_sales.list_price, web_sales.ext_sales_price, web_sales.ext_discount_amount, web_sales.item.wholesale_cost from web_sales limit 5;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...web_sales.item.wholesale_cost ??? from web_sales limit 5;
  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.billing_customer.text_id, store_sales.date.year, sum(((store_sales.ext_list_price - st…sales.ext_wholesale_cost - coalesce(store_sales.ext_discount_amount, 0)) + store_sales.ext_sales_price) / 2) as year_total from store_sales limit 10;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...les_price) / 2) as year_total ??? from store_sales limit 10;
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Filter to date range on the sale date 2000-08-23 to 2000-09-06 inclusive
whe…ales.sales_channel)) as g_outlet
order by
    g_channel asc,
    g_outlet asc,
    all_sales.sales_channel asc,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...xpression as a reusable macro ??? def outlet_label(x) -> concat(...

  Write stats: received 1302 chars / 1304 bytes; tail: …'les.sales_channel
  asc,\\n    outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Filter to date range on the sale date 2000-08-23 to 2000-09-06 inclusive
whe…hannel, outlet_label as g_outlet
order by
    g_channel asc,
    g_outlet asc,
    all_sales.sales_channel asc,
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
  ...we can reference it in rollup ??? auto outlet_label <- concat(

  Write stats: received 1086 chars / 1086 bytes; tail: …'les.sales_channel
  asc,\\n    outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:all_sales --all-rows --displayed-rows 200 select
    all_sales.sales_channel,
    concat(
        case all_sales.sales_cha…all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by
    all_sales.sales_channel asc,
    outlet asc nulls first
limit 100;`

  ```text
  --> 13:69
     |
  13 |     sum(all_sales.ext_sales_price) by rollup all_sales.sales_channel,
  (concat(
     |                                                                     ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...rollup all_sales.sales_channel ??? , (concat(         case all_sa...
  ```
- `trilogy file write query06.preql --content import raw.physical_sales as store_sales;

# Average current price per item category
auto avg_price_by_category <-…tomer.address.state as state,
    count_distinct(store_sales.billing_customer.id) as customer_count
order by customer_count asc, state asc
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
     and store_sales.item.id in ??? (select expensive_items) selec...

  Write stats: received 769 chars / 769 bytes; tail: …'omer_count\\norder by
  customer_count asc, state asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.customer as customer;
import raw.store as store;
import raw.physical_sales as s….quarter = 2

select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 30, column 57.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._zip_prefix in param_prefixes ??? and store_zip_prefix in pref_p...

  Write stats: received 1407 chars / 1407 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.customer as customer;
import raw.store as store;
import raw.physical_sales as s…f_prefix_arr

select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_OR', 'or') at line 41, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
  ...g(param_zip_list[1], 1, 2)    ??? or store_zip_prefix = substrin...

  Write stats: received 3650 chars / 3650 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.customer as customer;
import raw.store as store;
import raw.physical_sales as s…ref_prefixes

select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'x ') at line 16, column 66.
  Expected one of:
          * "@"
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...ct(array_transform(param_arr, ??? x -> substring(x, 1, 2)));  #

  Write stats: received 1102 chars / 1102 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.customer as customer;
import raw.store as store;
import raw.physical_sales as s…atching_pref

select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'x ') at line 69, column 47.
  Expected one of:
          * "@"
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...y_transform(split(zips, ','), ??? x -> case when substring(x, 1,...

  Write stats: received 3388 chars / 3388 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.customer as customer;
import raw.store as store;
import raw.physical_sales as s…d{3}($|,)'))

select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  …
  MPORT
          * SELF_IMPORT
          * PUBLISH_ACTION
          * PARSE_COMMENT
          * CHART
          * UNIQUE
          * DATASOURCE_PARTIAL
          * FROM
          * PARAMETER
          * PURPOSE
          * DEF
          * PERSIST_MODE
          * WHERE
          * DATASOURCE
          * CREATE
          * COPY
          * PROPERTY
          * $END
          * "merge"i
          * RAW_SQL
          * _PROPERTIES
          * SHOW
          * ROWSET
          * TYPE
          * LINE_SEPARATOR
          * SHORTHAND_MODIFIER
          * AUTO
          * PARAM
          * MOCK
          * WITH
  Previous tokens: [Token('_TERMINATOR', ';')]

  Location:
  ...ing(split(zips, ','), 1, 2);  ??? -- array, but we need to unnes...

  Write stats: received 1300 chars / 1300 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
impo…  customer.demographics.dependent_count,
  customer.demographics.employed_dependent_count,
  customer.demographics.college_dependent_count
limit 100;`

  ```text
  …
  d_dependent_count,\n
  customer.demographics.college_dependent_count,\n  count(customer.id) as
  customer_count\norder by\n  customer.demographics.gender,\n
  customer.demographics.marital_status,\n
  customer.demographics.education_status,\n
  customer.demographics.purchase_estimate,\n
  customer.demographics.credit_rating,\n
  customer.demographics.dependent_count,\n
  customer.demographics.employed_dependent_count,\n
  customer.demographics.college_dependent_count\nlimit 100;') at line 10, column
  91.
  Expected one of:
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'month_of_year')]

  Location:
  ...date) = 2002 and month_of_year ??? (store_sales.date.date) in (1,...

  Write stats: received 1589 chars / 1589 bytes; tail: …',\\n
  customer.demographics.college_dependent_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.customer:customer --import raw.physical_sales:store_sales --import raw.web_sales:web_sales --import raw.catalog_sales:catalog_sales select customer.address.county, count(customer.id) as cnt group by customer.address.county order by cnt desc limit 5;`

  ```text
  --> 5:59
    |
  5 | select customer.address.county, count(customer.id) as cnt group by
  customer.address.county order by cnt desc limit 5;
    |                                                           ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...ty, count(customer.id) as cnt ??? group by customer.address.coun...
  ```
- `trilogy file write query11.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Revenue = sum of (ext_list_price -…erred_cust_flag
order by billing_customer_code nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 15, column 59.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ales.billing_customer.text_id ??? by store_sales.billing_custome...

  Write stats: received 1849 chars / 1849 bytes; tail: …'name nulls first,
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql -e -c import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Revenue = sum of (ext_list_price - ext…erred_cust_flag
order by billing_customer_code nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 15, column 59.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ales.billing_customer.text_id ??? by store_sales.billing_custome...

  Write stats: received 1849 chars / 1849 bytes; tail: …'name nulls first,
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql -e -c import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Revenue = sum of (ext_list_price - ext…erred_cust_flag
order by billing_customer_code nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 15, column 59.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ales.billing_customer.text_id ??? by store_sales.billing_custome...

  Write stats: received 1849 chars / 1849 bytes; tail: …'name nulls first,
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.physical_sales:store_sales select store_sales.billing_customer.demographics.marital_status as ms, store_sales.billing_customer.demog…) and store_sales.billing_customer.demographics.education_status in ('Advanced Degree', 'College', '2 yr Degree') group by ms, es, sp, hhdc limit 50;`

  ```text
  --> 2:512
    |
  2 | select store_sales.billing_customer.demographics.marital_status as ms,
  store_sales.billing_customer.demographics.education_status as es,
  store_sales.sales_price as sp,
  store_sales.billing_customer.household_demographic.dependent_count as hhdc,
  count(store_sales.ticket_number) as cnt where store_sales.date.year = 2001 and
  store_sales.billing_customer.demographics.marital_status in ('M', 'S', 'W') and
  store_sales.billing_customer.demographics.education_status in ('Advanced
  Degree', 'College', '2 yr Degree') group by ms, es, sp, hhdc limit 50;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...e', 'College', '2 yr Degree') ??? group by ms, es, sp, hhdc limi...
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Step 1: Find items whose (brand, class, category) combination appears in all…otal_sales > overall_avg_sale_val
order by
    channel nulls first,
    brand nulls first,
    class nulls first,
    category nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 27, column 1.
  Expected one of:
          * SELECT

  Location:
  ..._present = 3  # November 2001 ??? auto is_nov2001 <- all_sales.d...

  Write stats: received 2030 chars / 2030 bytes; tail: …',\\n    class nulls
  first,\\n    category nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as all_sales;

auto channels_present <- count_distinct(all_sales.sales_channel ? all_sales.date.y…year between 1999 and 2001) by all_sales.item.id)
order by channel nulls first, brand nulls first, class nulls first, category nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...er_sales having total_sales > ??? (select avg(all_sales.quantity...

  Write stats: received 915 chars / 915 bytes; tail: …'ls first, class nulls
  first, category nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as all_sales;

auto channels_present <- count_distinct(all_sales.sales_channel ? all_sales.date.y…ng
    total_sales > over_all_avg
order by
    channel nulls first,
    brand nulls first,
    class nulls first,
    category nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 7, column 1.
  Expected one of:
          * SELECT

  Location:
    where channels_present = 3  ??? auto over_all_avg <- avg(all_s...

  Write stats: received 1272 chars / 1272 bytes; tail: …',\\n    class nulls
  first,\\n    category nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as all_sales;

auto channels_present <- count_distinct(all_sales.sales_channel ? all_sales.date.y…ng
    total_sales > over_all_avg
order by
    channel nulls first,
    brand nulls first,
    class nulls first,
    category nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 9, column 1.
  Expected one of:
          * SELECT

  Location:
    where channels_present = 3  ??? auto nov_2001_sales_value <- a...

  Write stats: received 1257 chars / 1257 bytes; tail: …',\\n    class nulls
  first,\\n    category nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as all_sales;

auto channels_present <- count_distinct(all_sales.sales_channel ? all_sales.date.y…les > overall_sum / overall_count
order by
    channel nulls first,
    brand nulls first,
    class nulls first,
    category nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_SUM', 'sum(') at line 15, column 5.
  Expected one of:
          * MERGE
          * _TERMINATOR
          * COMMA
          * ORDER
          * LIMIT
          * HAVING
          * WHERE
          * METADATA
  Previous tokens: [Token('IDENTIFIER', 'over_all_avg')]

  Location:
  ...and 2001) as over_all_avg     ??? sum(all_sales.quantity * all_s...

  Write stats: received 1381 chars / 1381 bytes; tail: …',\\n    class nulls
  first,\\n    category nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as all_sales;

auto channels_present <- count_distinct(all_sales.sales_channel ? all_sales.date.y….date.year between 1999 and 2001)
order by
    channel nulls first,
    brand nulls first,
    class nulls first,
    category nulls first
limit 100;`

  ```text
  …
  UM
          * _ARRAY_TRANSFORM
          * FALSE
          * _DATE_ADD
          * _CEIL
          * MULTIPLY
          * _FLOOR
          * _MAP_VALUES
          * MOD
          * NULL
          * _UNNEST
          * _GEO_Y
          * _MAP_KEYS
          * _LIKE
          * _GEO_CENTROID
          * _RANDOM
          * _GENERATE_ARRAY
          * _DATE_SUB
          * _DATE_SPINE
          * _TRIM
          * _ATTR_ACCESS
          * CAST
          * _SUBSELECT
          * _SUBSTRING
          * _GEO_TRANSFORM
          * _REPLACE
          * _REGEXP_CONTAINS
          * LEN
          * _CONTAINS
  Previous tokens: [Token('PLUS_OR_MINUS', '-')]

  Location:
       --overall_sum_sales     - ??? -overall_cnt_sales having

  Write stats: received 1249 chars / 1249 bytes; tail: …',\\n    class nulls
  first,\\n    category nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as all_sales;

auto channels_present <- count_distinct(all_sales.sales_channel ? all_sales.date.y…date.year between 1999 and 2001))
order by
    channel nulls first,
    brand nulls first,
    class nulls first,
    category nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...ales having     total_sales > ??? (select sum(all_sales.quantity...

  Write stats: received 1209 chars / 1209 bytes; tail: …',\\n    class nulls
  first,\\n    category nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as all_sales;

auto channels_present <- count_distinct(all_sales.sales_channel ? all_sales.date.y…ing
    total_sales > overall_avg
order by
    channel nulls first,
    brand nulls first,
    class nulls first,
    category nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 7, column 1.
  Expected one of:
          * SELECT

  Location:
    where channels_present = 3  ??? auto overall_avg <- sum(all_sa...

  Write stats: received 1253 chars / 1253 bytes; tail: …',\\n    class nulls
  first,\\n    category nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query19.preql --content import raw/physical_sales.preql as store_sales;`

  ```text
  refused to write 'query19.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '/physical_sales.preql as
  store_sales;') at line 1, column 11.
  Expected one of:
          * _TERMINATOR
          * "as"
          * IMPORT_DOT
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /physical_sales.preql as store...

  Write stats: received 47 chars / 47 bytes; tail: …'import
  raw/physical_sales.preql as store_sales;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `join-resolution`

- `trilogy run query04.preql`

  ```text
  Could not resolve connections for query with output
  ['local.c_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.c_first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.c_last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.c_pref<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query04.preql`

  ```text
  Could not resolve connections for query with output
  ['local.c_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.c_first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.c_last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.c_pref<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.physical_sales:store_sales --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales select store_sales.date.year, catalog_sales.sold_date.year, web_sales.date.year limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'catalog_sales.sold_date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.date.year<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw.physical_sales:store_sales --import raw.catalog_sales:catalog_sales select store_sales.date.year, catalog_sales.sold_date.year limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'catalog_sales.sold_date.year<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```
- `trilogy run --import raw.physical_sales:store_sales --import raw.web_sales:web_sales select store_sales.date.year, web_sales.date.year limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'web_sales.date.year<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run query08.preql --param zips=24128`

  ```text
  Could not resolve connections for query with output
  ['local.sp<Purpose.PROPERTY>Derivation.BASIC>',
  'local.vp<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.customer:customer --import raw.physical_sales:store_sales where customer.address.county in ('Rush County', 'Toole County', 'Jefferso…, 'Dona Ana County', 'La Porte County') select customer.id, customer.address.county, count(store_sales.ticket_number) by customer.id as cnt limit 10;`

  ```text
  Could not resolve connections for query with output
  ['customer.id<Purpose.KEY>Derivation.ROOT>',
  'customer.address.county<Purpose.PROPERTY>Derivation.ROOT>',
  'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.customer:customer --import raw.physical_sales:store_sales select customer.id, customer.address.county, count(store_sales.ticket_number) by customer.id as cnt limit 10;`

  ```text
  Could not resolve connections for query with output
  ['customer.id<Purpose.KEY>Derivation.ROOT>',
  'customer.address.county<Purpose.PROPERTY>Derivation.ROOT>',
  'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.customer:customer --import raw.physical_sales:store_sales select count(store_sales.ticket_number) by customer.id as cnt, customer.address.county limit 10;`

  ```text
  Could not resolve connections for query with output
  ['local.cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'customer.address.county<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```
- `trilogy run query11.preql`

  ```text
  Could not resolve connections for query with output
  ['local.billing_customer_code<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.preferred_cust_flag<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query16.preql`

  ```text
  No datasource exists for root concept
  catalog_sales.warehouse_id@Grain<catalog_sales.warehouse_id>, and no resolvable
  pseudonyms found from set(). This query is unresolvable from your environment.
  Check your datasources and imports to make sure this concept is bound.
  ```
- `trilogy run query16.preql`

  ```text
  No datasource exists for root concept
  catalog_sales.warehouse_id@Grain<catalog_sales.warehouse_id>, and no resolvable
  pseudonyms found from set(). This query is unresolvable from your environment.
  Check your datasources and imports to make sure this concept is bound.
  ```
- `trilogy run --import raw/physical_sales:ps --import raw/physical_returns:pr select ps.billing_customer.id, ps.item.id, pr.return_date.quarter_name, ps.date.quarter_name, pr.ticket_number limit 5;`

  ```text
  Could not resolve connections for query with output
  ['ps.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'ps.item.id<Purpose.KEY>Derivation.ROOT>',
  'pr.return_date.quarter_name<Purpose.PROPERTY>Derivation.ROOT>',
  'ps.date.quarter_name<Purpose.PROPERTY>Derivation.ROOT>',
  'pr.ticket_number<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run --import raw/physical_sales:ps --import raw/physical_returns:pr --import raw/catalog_sales:cs select ps.billing_customer.id, ps.item.id, pr.ticket_number, ps.date.quarter_name, pr.return_date.quarter_name, cs.sold_date.quarter_name limit 5;`

  ```text
  Could not resolve connections for query with output
  ['ps.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'ps.item.id<Purpose.KEY>Derivation.ROOT>',
  'pr.ticket_number<Purpose.KEY>Derivation.ROOT>',
  'ps.date.quarter_name<Purpose.PROPERTY>Derivation.ROOT>',
  'pr.return_date.quarter_name<Purpose.PROPERTY>Derivation.ROOT>',
  'cs.sold_date.quarter_name<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```

### `syntax-missing-alias`

- `trilogy file write query02.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

merge catalog_sales.item.id into ~w…catalog_sales.sold_date.year + sum(daily_sales ? web_sales.date.day_of_week = 6) by web_sales.date.week_seq, web_sales.date.year;

select 1;
limit 1;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...eb_sales.date.year;  select 1; ??? limit 1;

  Write stats: received 2166 chars / 2166 bytes; tail: …'ales.date.week_seq,
  web_sales.date.year;\\n\\nselect 1;\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Restrict to web and catalog only (no store)
# Filter is applied in the aggre…hifted by 53 weeks to 2001 week_seq.
#
# In Trilogy I can compute the ratio inline by defining the 2002 values at the 2001 grain.

select 1;
limit 1;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
   at the 2001 grain.  select 1; ??? limit 1;

  Write stats: received 2784 chars / 2784 bytes; tail: …'ining the 2002 values at
  the 2001 grain.\\n\\nselect 1;\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Filter to date range on the sale date 2000-08-23 to 2000-09-06 inclusive
whe…l_dim_text_id
    )) as g_outlet
order by
    g_channel asc,
    g_outlet asc,
    all_sales.sales_channel asc,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ollup all_sales.sales_channel, ??? (concat(         case all_sal...

  Write stats: received 2174 chars / 2174 bytes; tail: …'les.sales_channel
  asc,\\n    outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Filter to date range on the sale date 2000-08-23 to 2000-09-06 inclusive
whe…l_dim_text_id
    )) as g_outlet
order by
    g_channel asc,
    g_outlet asc,
    all_sales.sales_channel asc,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ollup all_sales.sales_channel, ??? (concat(         case all_sal...

  Write stats: received 2154 chars / 2154 bytes; tail: …'les.sales_channel
  asc,\\n    outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.customer as customer;
import raw.store as store;

# Check what store zip prefix…ring(store.zip, 1, 2);

select
    distinct pref_zip_prefixes as pref_prefix,
    count(pref_zip_prefixes) as cnt
order by pref_prefix asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
     distinct pref_zip_prefixes ??? as pref_prefix,     count(pref...

  Write stats: received 496 chars / 496 bytes; tail: …'ref_zip_prefixes) as
  cnt\\norder by pref_prefix asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.customer as customer;
import raw.store as store;

auto pref_zip_count <- count(…Y') by customer.address.zip;

select
    distinct customer.address.zip as zip,
    pref_zip_count as cnt
order by customer.address.zip asc
limit 200;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...distinct customer.address.zip ??? as zip,     pref_zip_count as

  Write stats: received 309 chars / 309 bytes; tail: …'ip_count as cnt\\norder by
  customer.address.zip asc\\nlimit 200;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.customer as customer;
import raw.store as store;
import raw.physical_sales as s… (pref_zip_count > 10);
auto pref_prefixes <- substring(pref_zips, 1, 2);

select
    distinct pref_prefixes as prefix
order by prefix asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...ct     distinct pref_prefixes ??? as prefix order by prefix asc

  Write stats: received 421 chars / 421 bytes; tail: …'tinct pref_prefixes as
  prefix\\norder by prefix asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.physical_sales:store_sales select count(store_sales.row_counter), count(store_sales.ticket_number), avg(store_sales.quantity), max(store_sales.quantity), min(store_sales.quantity);`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...count(store_sales.row_counter) ??? , count(store_sales.ticket_num...
  ```
- `trilogy run --import raw.physical_sales:store_sales select distinct store_sales.billing_customer.demographics.education_status limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? store_sales.billing_customer.d...
  ```
- `trilogy run --import raw/physical_sales select physical_sales.date.quarter_name, count(physical_sales.ticket_number) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...physical_sales.ticket_number) ??? limit 5;
  ```

### `undefined-concept`

- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web_sales.ext_list_price. Suggestions: ['web_sales.list_price',
  'store_sales.ext_list_price', 'web_sales.ext_sales_price']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query06.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.current_price. Suggestions: ['store_sales.item.current_price']")
  ```
- `trilogy run query08.preql --param zips=24128`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_zip_prefix. Suggestions: ['store_prefix', 'store.zip', 'pref_prefixes']")
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web_sales.ext_list_price. Suggestions: ['web_sales.list_price',
  'store_sales.ext_list_price', 'web_sales.ext_sales_price']")
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.catalog_sales.bill_customer.address.zip, all_sales.catalog_sales.bill_customer.address.state, all_sales.catalog_sales.sold_date.year, all_sales.catalog_sales.sold_date.quarter limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  all_sales.catalog_sales.bill_customer.address.zip. Suggestions:
  ['all_sales.billing_customer.address.zip',
  'all_sales.ship_customer.address.zip',
  'all_sales.billing_customer.address.id']")
  ```
- `trilogy run --import raw/physical_sales select physical_sales.date.quarter_name, count(physical_sales.ticket_number) as cnt limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  physical_sales.date.quarter_name. Suggestions: ['store.date.quarter_name',
  'date.quarter_name', 'return_store.date.quarter_name']")
  ```
- `trilogy run --import raw/physical_sales select physical_sales.store.date.quarter_name, count(physical_sales.ticket_number) as cnt limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  physical_sales.store.date.quarter_name. Suggestions:
  ['store.date.quarter_name', 'return_store.date.quarter_name',
  'store.date.quarter']")
  ```
- `trilogy run --import raw/physical_sales select physical_sales.store.state, count(physical_sales.ticket_number) as cnt limit 5;`

  ```text
  (UndefinedConceptException(...), 'line: 2: Undefined concept:
  physical_sales.store.state.')
  ```

### `cli-misuse`

- `trilogy trilogy run query04.preql`

  ```text
  No such command 'trilogy'.
  ```
- `trilogy explore raw/physical_sales.preql --regex promotion\.channel_email promotion\.channel_event`

  ```text
  Got unexpected extra argument (promotion\.channel_event)
  ```
- `trilogy read_file raw/physical_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/store_sales.preql --regex store --regex address --regex zip --regex z i p --regex zip_code`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.param_zips. Valid:
  'STRING'.
  ```
