# Trilogy failure analysis — 20260531-020357

- Run `20260531-020356_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 376 | failed: 34 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 12 | 35% |
| `syntax-parse` | 12 | 35% |
| `undefined-concept` | 5 | 15% |
| `syntax-missing-alias` | 2 | 6% |
| `join-resolution` | 1 | 3% |
| `type-error` | 1 | 3% |
| `cli-misuse` | 1 | 3% |

## Detail

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 98 (char 97). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 82 (char 81). Re-issue the call with valid JSON arguments.
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
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 85 (char 84). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw.all_sales:all_sales select
    sum(((all_sales.ext_list_price - all_sales.ext_wholesale_cost - all_sales.ext_discount_amount + all_s…EB' and all_sales.date.year = 2001) as w2001,
    count(all_sales.billing_customer.id) as cust_count
    where s2001 > 0 and c2001 > 0 and w2001 > 0;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.s2001) in the same statement where clause; move to the HAVING clause
  instead; Line: 2
  ```
- `trilogy run --import raw.all_sales:all_sales select
  sum(1.0 ? all_sales.sales_channel = 'STORE' and all_sales.date.year = 2001 and all_sales.billing_custom… all_sales.billing_customer.id as web_flag,
  count(all_sales.billing_customer.id) as cnt where store_flag > 0 and catalog_flag > 0 and web_flag > 0;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.store_flag) in the same statement where clause; move to the HAVING
  clause instead; Line: 2
  ```
- `trilogy run test_debug3.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUP BY clause
  cannot contain aggregates!

  LINE 70:     sum(CASE WHEN "cooperative"."all_sales_sales_channel" =...
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as
  "all_sales_billing_customer_id",
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_EXT_DISCOUNT_AMT" as
  "all_sales_ext_discount_amount",
      "all_sales_catalog_sales_unified"."CS_EXT_LIST_PRICE" as
  "all_sales_ext_list_price",
      "all_sales_catalog_sales_
  …
  e"."all_sales_date_year" = 2002 THEN (( (
  "cooperative"."all_sales_ext_list_price" -
  "cooperative"."all_sales_ext_wholesale_cost" ) -
  "cooperative"."all_sales_ext_discount_amount" ) +
  "cooperative"."all_sales_ext_sales_price") / 2.0 ELSE NULL END) /
  "questionable"."w_2001" as "w_ratio"
  FROM
      "questionable"
      LEFT OUTER JOIN "cooperative" on
  "questionable"."all_sales_billing_customer_id" is not distinct from
  "cooperative"."all_sales_billing_customer_id"
  GROUP BY
      1,
      2,
      3,
      4,
      8,
      9,
      10
  LIMIT (30)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 85 (char 84). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.BinderException) Binder Error: column "channel" must
  appear in the GROUP BY clause or must be part of an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(channel)" if the exact
  value of "channel" is not important.

  LINE 131:     "abhorrent"."channel" as "channel",
                ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "sales_channel_dim_id",
      "sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as
  "sales_channel_dim_text_id",
       'CATALOG'  as "sales_sales_channel"
  FROM
      "catalog_page" as "sales_catalog_
  …
  OIN "young" on "sparkling"."channel" is not distinct from
  "young"."channel" AND "sparkling"."outlet" is not distinct from
  "young"."outlet")
  SELECT
      "abhorrent"."channel" as "channel",
      "abhorrent"."outlet" as "outlet",
      "abhorrent"."total_sales" as "total_sales",
      "abhorrent"."total_returns" as "total_returns",
      sum("abhorrent"."sales_net_profit") -
  sum("abhorrent"."sales_return_net_loss") as "profit"
  FROM
      "abhorrent"
  ORDER BY
      "abhorrent"."channel" asc,
      "abhorrent"."outlet" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run qdebug4.preql`

  ```text
  This script requires parameter "zips" to be set in
  environment.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
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
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 90 (char 89). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 99 (char 98). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 99 (char 98). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql duckdb`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query14.preql duckdb`

  ```text
  HAVING references 'local.overall_avg_sale', which is not in
  the SELECT projection (line 15). Add it to SELECT, each prefixed with `--` so
  it stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql duckdb --displayed-rows 200`

  ```text
  HAVING references 'local.overall_avg_sale', which is not in
  the SELECT projection (line 12). Add it to SELECT, each prefixed with `--` so
  it stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 101 (char 100). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 95 (char 94). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```

### `syntax-parse`

- `trilogy file write ~query02.preql --content Let's think through this query step by step.`

  ```text
  …
  CHART
          * SHORTHAND_MODIFIER
          * ROWSET
          * AUTO
          * PURPOSE
          * CREATE
          * PUBLISH_ACTION
          * PARAMETER
          * PERSIST_MODE
          * DEF
          * SELF_IMPORT
          * COPY
          * $END
          * DATASOURCE_ROOT
          * DATASOURCE_PARTIAL
          * SHOW
          * IMPORT
          * DATASOURCE
          * PROPERTY
          * _DEF_TABLE
          * _PROPERTIES
          * FROM
          * SELECT
          * MOCK
          * PARSE_COMMENT
          * RAW_SQL
          * TYPE
          * WHERE
          * UNIQUE
          * WITH
          * PARAM
  Previous tokens: [None]

  Location:
   ??? Let's think through this query...

  Write stats: received 44 chars / 44 bytes; tail: …"Let's think through this
  query step by step.".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;
import raw.reason as reason;

# Bucket ticket quantities into ranges and co…t_paid end) as bucket_61_80,
    (case when bucket5_count > 165306 then bucket5_avg_discount else bucket5_avg_net_paid end) as bucket_81_100
limit 1;`

  ```text
  …
       * DATASOURCE
          * PARAMETER
          * $END
          * PERSIST_MODE
          * ROWSET
          * PROPERTY
          * WITH
          * VALIDATE
          * MOCK
          * DEF
          * PURPOSE
          * WHERE
          * TYPE
          * PARSE_COMMENT
          * UNIQUE
          * AUTO
          * RAW_SQL
          * SELECT
          * DATASOURCE_ROOT
          * "merge"i
          * DATASOURCE_PARTIAL
          * SHORTHAND_MODIFIER
          * FROM
          * CHART
          * CREATE
          * PUBLISH_ACTION
          * IMPORT
  Previous tokens: [Token('PARSE_COMMENT', '# Count of line items per quantity
  bucket\n')]

  Location:
  ...ine items per quantity bucket ??? bucket1_count = count(store_sa...

  Write stats: received 2742 chars / 2742 bytes; tail: …'unt else
  bucket5_avg_net_paid end) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw/store_sales as store_sales;

# For store sales in the year 2001, return the average ticket quantity,
# …_sales_price,
    avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;`

  ```text
  …
  ', 'NM', 'KY')\n   and
  store_sales.net_profit between 150 and 300)\n  or\n
  (store_sales.sale_address.state in ('VA', 'TX', 'MS')\n   and
  store_sales.net_profit between 50 and 250)\n)\n\nselect\n
  avg(store_sales.quantity) as avg_ticket_qty,\n
  avg(store_sales.ext_sales_price) as avg_ext_sales_price,\n
  avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,\n
  sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;") at line 1,
  column 11.
  Expected one of:
          * _TERMINATOR
          * IMPORT_DOT
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /store_sales as store_sales;

  Write stats: received 2199 chars / 2199 bytes; tail:
  …'store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw/store_sales as store_sales;

where store_sales.date.year = 2001
and (
  (store_sales.customer_demograph…_sales_price,
    avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;`

  ```text
  …
  OR', 'NM',
  'KY')\n   and store_sales.net_profit between 150 and 300)\n  or\n
  (store_sales.sale_address.state in ('VA', 'TX', 'MS')\n   and
  store_sales.net_profit between 50 and 250)\n)\nselect\n
  avg(store_sales.quantity) as avg_ticket_qty,\n
  avg(store_sales.ext_sales_price) as avg_ext_sales_price,\n
  avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,\n
  sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;") at line 1,
  column 11.
  Expected one of:
          * _TERMINATOR
          * IMPORT_DOT
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /store_sales as store_sales;

  Write stats: received 1460 chars / 1460 bytes; tail:
  …'store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw/store_sales as store_sales;

where store_sales.date.year = 2001
and (
  (store_sales.customer_demograph…_sales_price,
    avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;`

  ```text
  …
  OR', 'NM',
  'KY')\n   and store_sales.net_profit between 150 and 300)\n  or\n
  (store_sales.sale_address.state in ('VA', 'TX', 'MS')\n   and
  store_sales.net_profit between 50 and 250)\n)\nselect\n
  avg(store_sales.quantity) as avg_ticket_qty,\n
  avg(store_sales.ext_sales_price) as avg_ext_sales_price,\n
  avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,\n
  sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;") at line 1,
  column 11.
  Expected one of:
          * _TERMINATOR
          * IMPORT_DOT
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /store_sales as store_sales;

  Write stats: received 1460 chars / 1460 bytes; tail:
  …'store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query16.preql --content import raw.catalog_sales as catalog_sales;

# Restrict to catalog sales with ship date in range, shipping to GA,
#…alog_sales.ext_ship_cost) as total_extended_ship_cost,
    sum(catalog_sales.net_profit) as total_net_profit
order by distinct_order_count
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
   warehouse # We filter orders ??? by this condition using inline...

  Write stats: received 975 chars / 975 bytes; tail: …'as
  total_net_profit\\norder by distinct_order_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `other`

- `trilogy run --import raw/store_sales:store_sales select store_sales.return_store.id as store_id, avg(sum(store_sales.return_amount)) by store_sales.return_cu…stomer_return where store_sales.return_store.state='TN' and store_sales.return_date.year=2000 and store_sales.return_customer.id is not null limit 5;`

  ```text
  HAVING references 'local.cust_store_return',
  'local.store_avg_return', which are not in the SELECT projection (line 9). Add
  them to SELECT, each prefixed with `--` so they stay out of the output rows —
  keep your HAVING as-is:
      select <your existing columns>, --local.cust_store_return,
  --local.store_avg_return
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, sum(all_sales.ext_sales_price ? all_sales.date.day_of_week = 0 and all_sales.sal…') and all_sales.date.year = 2002) by (all_sales.date.week_seq - 53) as sun_2002 where sun_2002 is not null order by all_sales.date.week_seq limit 5;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.sun_2002) in the same statement where clause; move to the HAVING clause
  instead; Line: 2
  ```
- `trilogy run --import raw.all_sales:all_sales auto sun_2001 <- sum(all_sales.ext_sales_price ? all_sales.date.day_of_week = 0 and all_sales.sales_channel in (…ll_sales.date.week_seq) as wk_seq, sun_2001, (all_sales.date.week_seq - 53) as wk2002, sun_2002 having sun_2001 is not null order by wk_seq limit 10;`

  ```text
  Parenthetical with non-supported content
  ref:all_sales.date.week_seq (<class 'trilogy.core.models.author.ConceptRef'>)
  not yet supported
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.catalog_2002', 'local.catalog_2001',
  'local.store_2002', 'local.store_2001', 'local.web_2002', 'local.web_2001',
  which are not in the SELECT projection (line 19). Add them to SELECT, each
  prefixed with `--` so they stay out of the output rows — keep your HAVING
  as-is:
      select <your existing columns>, --local.catalog_2002, --local.catalog_2001,
  --local.store_2002, --local.store_2001, --local.web_2002, --local.web_2001
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local._virt_agg_count_8851570471982349 >= 1 and
  (BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6721737319307331@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>) or
  BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6022827501633367@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>))
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local.has_store_sale_2002_q1 >= 1 and
  (BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6721737319307331@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>) or
  BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6022827501633367@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>))
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'MergeNode<customer.address.county,customer.demographics.college_dependent_cou
  nt,customer.demographics.credit_rating...7 more>': None} and need
  BuildSubselectComparison(left=customer.address.county@Grain<customer.address.id
  >, right=('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County',
  'La Porte County'), operator=<ComparisonOperator.IN: 'in'>) and
  local.has_store_sale_2002_q1 >= 1 and
  (BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6721737319307331@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>) or
  BuildSubselectComparison(left=customer.id@Grain<customer.id>,
  right=(local._virt_filter_id_6022827501633367@Grain<Abstract>),
  operator=<ComparisonOperator.IN: 'in'>))
  ```
- `trilogy run query04.preql`

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 31 column 12 (char 1091). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.num_channels',
  'local.overall_avg_sale', which are not in the SELECT projection (line 20). Add
  them to SELECT, each prefixed with `--` so they stay out of the output rows —
  keep your HAVING as-is:
      select <your existing columns>, --local.num_channels,
  --local.overall_avg_sale
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_avg_sale', which is not in
  the SELECT projection (line 14). Add it to SELECT, each prefixed with `--` so
  it stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query15.preql`

  ```text
  Value 'Q2' is not valid for enum field
  'catalog_sales.sold_date.quarter'. Allowed values: 1, 2, 3, 4.
  ```
- `trilogy run query18.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year, min(all_sales.date.week_seq) as min_ws, max(all_sales.date.week_seq) as max_ws where all_sales.sales_channel in ('WEB','CATALOG') group by all_sales.date.year order by all_sales.date.year limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._channel in ('WEB','CATALOG') ??? group by all_sales.date.year o...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year, count(all_sales.date.week_seq) as num_weeks where all_sales.sales_channel in ('WEB','CATALOG') group by all_sales.date.year order by all_sales.date.year limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ..._channel in ('WEB','CATALOG') ??? group by all_sales.date.year o...
  ```
- `trilogy run --import raw.all_sales:all_sales auto sales_2001 <- sum(all_sales.ext_sales_price ? all_sales.sales_channel in ('WEB','CATALOG') and all_sales.da…_2002_shifted having sales_2001 is not null and sales_2002_shifted is not null order by all_sales.date.week_seq, all_sales.date.day_of_week limit 15;`

  ```text
  Syntax [211]: Expression in `by` clause must be wrapped in
  parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work
  without parens, but any function call, cast, or other expression needs them.
  Location:
  ...d all_sales.date.year = 2002) ??? by (all_sales.date.week_seq -
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

auto outlet <- case
    when all_sales.sales_channel = 'STORE'
        then co…ice, all_sales.return_amount, sum(all_sales.net_profit) - sum(all_sales.return_net_loss))
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 14, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? def rollup_metrics(metric_sale...

  Write stats: received 1032 chars / 1032 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
impo…stomer.demographics.dependent_count,
    customer.demographics.employed_dependent_count,
    customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_OR', 'or') at line 7, column 68.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...ddress.county = 'Rush County' ??? or customer.address.county = '...

  Write stats: received 2238 chars / 2238 bytes; tail: …'
  customer.demographics.college_dependent_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
impo…stomer.demographics.dependent_count,
    customer.demographics.employed_dependent_count,
    customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('RPAR', ')') at line 12, column 30.
  Expected one of:
          * WHEN

  Location:
                              0) ??? ) by customer.id;  # At least

  Write stats: received 2022 chars / 2022 bytes; tail: …'
  customer.demographics.college_dependent_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
impo…stomer.demographics.dependent_count,
    customer.demographics.employed_dependent_count,
    customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'customer.address.county ') at line
  7, column 33.
  Expected one of:
          * COMMA
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ...to in_county <- max(case(when ??? customer.address.county = 'Rus...

  Write stats: received 2088 chars / 2088 bytes; tail: …'
  customer.demographics.college_dependent_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
impo…stomer.demographics.dependent_count,
    customer.demographics.employed_dependent_count,
    customer.demographics.college_dependent_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('RPAR', ')') at line 7, column 253.
  Expected one of:
          * WHEN

  Location:
  ...ss.county = 'La Porte County') ??? ) by customer.id;  # At least

  Write stats: received 2265 chars / 2265 bytes; tail: …'
  customer.demographics.college_dependent_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Step 1: Find which (brand, class, category) combos appear in all 3 channels ….brand_id nulls first,
    g_class asc,
    all_sales.item.class_id nulls first,
    g_cat asc,
    all_sales.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [203]: Missing assignment operator '<-' and expression in derivation.
  Write `auto X <- <expression>;` (also valid: `metric`, `property`, `rowset`).
  Example: `auto orders_per_customer <- count(orders.id) by customer.id;`.
  Location:
  ...all 3 channels auto valid_bcc ??? = all_sales.item.brand_id, all...

  Write stats: received 3494 chars / 3494 bytes; tail: …'t asc,\\n
  all_sales.item.category_id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Overall average sale value (qty * list_price) across all 3 channels in 1999-…nulls first,
    all_sales.item.brand_id nulls first,
    all_sales.item.class_id nulls first,
    all_sales.item.category_id nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 39, column 44.
  Expected one of:
          * SELECT
  Previous tokens: [Token('__ANON_19', '1.49')]

  Location:
  ...ems <- all_sales.item.brand_id ??? , all_sales.item.class_id, all...

  Write stats: received 4386 chars / 4386 bytes; tail: …'irst,\\n
  all_sales.item.category_id nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as all_sales;

# Sale value = quantity * list price
auto sale_value <- all_sales.quantity * a…nd_id nulls first,
    --g_class asc,
    all_sales.item.class_id nulls first,
    --g_cat asc,
    all_sales.item.category_id nulls first
limit 100;`

  ```text
  …
          * _MAP_VALUES
          * _MONTH_NAME
          * _VARIANCE
          * _GEO_X
          * CURRENT_DATETIME
          * _ILIKE
          * FILTER
          * _GEO_TRANSFORM
          * _UPPER
          * /add\(/
          * _REGEXP_EXTRACT
          * "@"
          * _SUBSTRING
          * _SUBSELECT
          * _GEO_CENTROID
          * _REGEXP_REPLACE
          * _CEIL
          * _STDDEV
          * _DATE_DIFF
          * LBRACE
          * SUBTRACT
          * _DATE_SPINE
          * _PARSE_TIME
          * _STRUCT
          * _DAY_OF_WEEK
          * _STRPOS
          * _ARRAY_TRANSFORM
  Previous tokens: [Token('__ANON_7', 'by')]

  Location:
  ...overall_avg_sale order by     ??? --g_channel asc,     all_sales...

  Write stats: received 2109 chars / 2109 bytes; tail: …'t asc,\\n
  all_sales.item.category_id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query18.preql --content import catalog_sales as catalog_sales;

# Filter conditions
where catalog_sales.sold_date.year = 1998
  and catalo…ress.state asc nulls first,
    catalog_sales.bill_customer.address.county asc nulls first,
    catalog_sales.item.text_id asc nulls first
limit 100;`

  ```text
  refused to write 'query18.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 12, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...code), country, state, county ??? def avg_rollup(metric) -> avg(...

  Write stats: received 1659 chars / 1659 bytes; tail: …'t,\\n
  catalog_sales.item.text_id asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year, count(all_sales.order_id) order by all_sales.date.year;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ar, count(all_sales.order_id) ??? order by all_sales.date.year;
  ```
- `trilogy file write qdebug.preql --content import raw.physical_sales as store_sales;

# Check store ZIP prefixes
select distinct substring(store_sales.store.zip, 1, 2) as zip_prefix,
       count(store_sales.store.id) as store_count
order by zip_prefix
limit 50;`

  ```text
  refused to write 'qdebug.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...xes select distinct substring( ??? store_sales.store.zip, 1, 2) a...

  Write stats: received 219 chars / 219 bytes; tail: …'sales.store.id) as
  store_count\\norder by zip_prefix\\nlimit 50;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.physical_sales:ps select count(ps.ticket_number) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...elect count(ps.ticket_number) ??? limit 10;
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.ship_date.date, count(cs.order_number) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ....date, count(cs.order_number) ??? limit 10;
  ```
- `trilogy run --import raw.call_center:cc select cc.county, count(cc.id) limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...elect cc.county, count(cc.id) ??? limit 20;
  ```
- `trilogy run --import raw.catalog_sales:cs select count(cs.order_number) where cs.sold_date.year=2001 and cs.sold_date.quarter in (1,2,3) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...select count(cs.order_number) ??? where cs.sold_date.year=2001 a...
  ```

### `undefined-concept`

- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_zip_prefix. Suggestions: ['store_prefix', 'store.zip', 'pref_prefixes']")
  ```
- `trilogy run query10.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.ship_customer.id. Suggestions: ['catalog_sales.bill_customer.id',
  'catalog_sales.ship_mode.id', 'catalog_sales.ship_date.id']")
  ```
- `trilogy run query17.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  ss.sale_date.quarter_name. Suggestions: ['ss.date.quarter_name',
  'cs.sold_date.quarter_name', 'ss.store.date.quarter_name']")
  ```
- `trilogy run query18.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.bill_demographics.gender. Suggestions:
  ['catalog_sales.bill_customer.demographics.gender',
  'catalog_sales.bill_customer_demographic.gender',
  'catalog_sales.billing_customer.demographics.gender']")
  ```
- `trilogy run query20.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query19.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.brand_id. Suggestions: ['store_sales.item.brand_id']")
  ```
- `trilogy run query19.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date.year. Suggestions: ['store_sales.date.year']")
  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy run --import raw.physical_sales:ss --import raw.catalog_sales:cs merge ss.item.id into ~cs.item.id; merge ss.billing_customer.id into ~cs.bill_custom…and cs.sold_date.year=2001 and cs.sold_date.quarter in (1,2,3) and ss.billing_customer.id = cs.bill_customer.id and ss.item.id = cs.item.id limit 10;`

  ```text
  'merge ss.billing_customer.id into ~cs.bill_customer.id;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.physical_sales:ss --import raw.catalog_sales:cs merge ss.item.id into ~cs.item.id; merge ss.billing_customer.id into ~cs.bill_custom… sc, cs.bill_customer.id as cc where ss.date.year=2001 and ss.date.quarter=1 and cs.sold_date.year=2001 and cs.sold_date.quarter in (1,2,3) limit 10;`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.sales_channel:sc select distinct sc.sales_channel;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...hannel as sc; select distinct ??? sc.sales_channel;
  ```
- `trilogy run --import raw.sales_channel:sc select distinct sc.sales_channel as sc;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...hannel as sc; select distinct ??? sc.sales_channel as sc;
  ```
- `trilogy run --import raw.all_sales:s select distinct s.sales_channel as chan;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...l_sales as s; select distinct ??? s.sales_channel as chan;
  ```
- `trilogy run --import raw.all_sales select distinct all_sales.sales_channel as chan;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...aw.all_sales; select distinct ??? all_sales.sales_channel as cha...
  ```

### `syntax-missing-alias`

- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date

s…         then concat('web_site_', all_sales.channel_dim_text_id)
    end) as total_profit
order by channel nulls first, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `(case
          when all_sales.sales_channel = 'STORE'
              then concat('store_', all_sales.channel_dim_text_id)
          when all_sales.sales_channel = 'CATALOG'
              then concat('catalog_page_', all_sales.channel_dim_text_id)
          when all_sales.sales_channel = 'WEB'
              then concat('web_site_', all_sales.channel_dim_text_id)
      end) as case_when_all_sales_sales_channel_store_`
  Location:
  ...ollup all_sales.sales_channel, ??? (case         when all_sales....

  Write stats: received 1962 chars / 1962 bytes; tail: …'\\norder by channel
  nulls first, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write test_channels.preql --content import raw.all_sales as all_sales;

where all_sales.date.year = 2001 and all_sales.date.month_of_year = 11
select distinct all_sales.sales_channel
limit 10;`

  ```text
  refused to write 'test_channels.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct all_sales.sales_channel as
  distinct_all_sales_sales_channel`
  Location:
  ...tinct all_sales.sales_channel ??? limit 10;

  Write stats: received 156 chars / 156 bytes; tail: …'_year = 11\\nselect
  distinct all_sales.sales_channel\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `join-resolution`

- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year, min(all_sales.date.d_week_seq1) as min_dws, max(all_sales.date.d_week_seq1) as max_d…week_seq) as max_ws where all_sales.sales_channel in ('WEB','CATALOG') and all_sales.date.year in (2001, 2002) order by all_sales.date.year limit 10;`

  ```text
  No datasource exists for root concept
  all_sales.date.d_week_seq1@Grain<all_sales.date.id>, and no resolvable
  pseudonyms found from set(). This query is unresolvable from your environment.
  Check your datasources and imports to make sure this concept is bound.
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type ArrayType<STRING>' passed into
  SUBSTRING function from function SPLIT in position 1. Valid: 'STRING'
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.intersecting_zips. Valid:
  'STRING'.
  ```

### `cli-misuse`

- `trilogy explore raw/physical_sales.preql --show imports,datasources`

  ```text
  Invalid value for '--show': 'imports,datasources' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```
