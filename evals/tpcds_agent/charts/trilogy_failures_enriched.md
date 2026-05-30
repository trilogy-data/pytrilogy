# Trilogy failure analysis — 20260530-031442

- Run `20260530-031441_enriched` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 694 | failed: 66 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 30 | 45% |
| `syntax-parse` | 22 | 33% |
| `cli-misuse` | 8 | 12% |
| `undefined-concept` | 3 | 5% |
| `type-error` | 2 | 3% |
| `syntax-missing-alias` | 1 | 2% |

## Detail

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 81 (char 80). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 100 (char 99). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 94 (char 93). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy run query01.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 64 (char 63). Re-issue the call with valid JSON arguments.
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
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 85 (char 84). Re-issue the call with valid JSON arguments.
  ```
- `trilogy explore raw/physical_sales.preql --regex ^(date\.|?date\.)`

  ```text
  Invalid --regex pattern '^(date\\.|?date\\.)': nothing to repeat at position 9
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "highfalutin" not found!
  Candidate tables: "store_sales_date_date"

  LINE 54: ...TRING("store_sales_store_store"."S_ZIP",1,2) in
  (SUBSTRING("highfalutin"."_virt_unnest_4565099077345551",1,2)) and...
                                                                         ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      STRING_SPLIT( $1 , ',' ) as "param_zips"
  ),
  questionable as (
  SELECT
      "store_sales_billing_customer_customers"."C_CURRENT_ADDR_SK" as
  "store_sales_billing_customer_address_id",
      "store_sales_billing_customer_customers"."C_P
  …
  "store_sales_store_store"."S_ZIP",1,2) in
  (SUBSTRING("highfalutin"."_virt_unnest_4565099077345551",1,2)) and
  SUBSTRING("store_sales_store_store"."S_ZIP",1,2) in
  (SUBSTRING("uneven"."preferred_zips",1,2))

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "sparkling"."store_sales_store_name" as "store_name",
      sum("sparkling"."store_sales_net_profit") as "total_net_profit"
  FROM
      "sparkling"
  GROUP BY
      1
  ORDER BY
      "store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 82 (char 81). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw/physical_sales.preql:physical_sales select count(physical_sales.row_counter ? (sum(physical_sales.quantity) by physical_sales.ticket_number) between 21 and 40) as cnt;`

  ```text
  (_duckdb.BinderException) Binder Error: aggregate function
  calls cannot be nested

  LINE 2:     count(CASE WHEN ( sum("physical_sales_store_sales"."SS_QUANTITY") )
  BETWEEN...
                                ^
  [SQL: SELECT
      count(CASE WHEN ( sum("physical_sales_store_sales"."SS_QUANTITY") ) BETWEEN
  21 AND 40 THEN 1 ELSE NULL END) as "cnt"
  FROM
      "store_sales" as "physical_sales_store_sales"]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 1383 (char 1382). Re-issue the call with valid JSON arguments.
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
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 55 (char 54). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 81 (char 80). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy run query14.preql duckdb`

  ```text
  Unable to import '.\all_sales.preql': [Errno 2] No such file
  or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy run query14.preql duckdb`

  ```text
  HAVING references 'local.avg_sale_all', which is not in the
  SELECT projection (line 35). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.avg_sale_all`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query16.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run query16.preql`

  ```text
  Have
  {'GroupNode<catalog_sales.order_number,local._virt_agg_count_3804749603066067>'
  : None} and need catalog_sales.ship_date.date@Grain<catalog_sales.ship_date.id>
  between constant(2002-02-01) and constant(2002-04-02) and
  catalog_sales.customer_address.state = GA and catalog_sales.call_center.county
  = Williamson County and local._virt_agg_count_3804749603066067 > 1 and
  local._virt_agg_count_5414203329674744 = 0
  ```
- `trilogy run query19.preql`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```

### `syntax-parse`

- `trilogy file write query04.preql --content import raw.all_sales as all_sales;

# For each billing customer and each year, compute a 'year total' separately
#…flag
order by
  customer_code asc nulls first,
  first_name asc nulls first,
  last_name asc nulls first,
  preferred_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 11, column 109.
  Expected one of:
          * _TERMINATOR

  Location:
  ...all_sales.billing_customer.id ??? where all_sales.date.year = 20...

  Write stats: received 2410 chars / 2410 bytes; tail: …'asc nulls first,\\n
  preferred_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.all_sales as all_sales;

# The core per-row calculation: ((ext_list_price - ext_wholesale_cost - ext_di…flag
order by
  customer_code asc nulls first,
  first_name asc nulls first,
  last_name asc nulls first,
  preferred_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
  ...l_sales.billing_customer.id   ??? where all_sales.date.year = 20...

  Write stats: received 2182 chars / 2182 bytes; tail: …'asc nulls first,\\n
  preferred_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.all_sales as all_sales;
# row_calc: per-row value = ((ext_list_price - ext_wholesale_cost - ext_discoun…eferred_flag
order by customer_code asc nulls first, first_name asc nulls first, last_name asc nulls first, preferred_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 6, column 96.
  Expected one of:
          * _TERMINATOR

  Location:
  ...all_sales.billing_customer.id ??? where all_sales.date.year = 20...

  Write stats: received 1836 chars / 1836 bytes; tail: …'e asc nulls first,
  preferred_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.all_sales as all_sales;
auto row_calc <- ((all_sales.ext_list_price - all_sales.ext_wholesale_cost - al…t_sales_price) / 2;

auto s01 <- sum(row_calc ? all_sales.sales_channel = 'STORE') by all_sales.billing_customer.id where all_sales.date.year = 2001;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 4, column 96.
  Expected one of:
          * _TERMINATOR

  Location:
  ...all_sales.billing_customer.id ??? where all_sales.date.year = 20...

  Write stats: received 306 chars / 306 bytes; tail: …'_sales.billing_customer.id
  where all_sales.date.year = 2001;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.all_sales as all_sales;
auto row_calc <- ((all_sales.ext_list_price - all_sales.ext_wholesale_cost - al…eferred_flag
order by customer_code asc nulls first, first_name asc nulls first, last_name asc nulls first, preferred_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 12, column 69.
  Expected one of:
          * SELECT

  Location:
  ...01 > 0 and c01 > 0 and w01 > 0 ??? ; auto sr <- s02 / s01; auto c...

  Write stats: received 1293 chars / 1293 bytes; tail: …'e asc nulls first,
  preferred_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.all_sales as all_sales;
auto row_calc <- ((all_sales.ext_list_price - all_sales.ext_wholesale_cost - al…flag
order by
  customer_code asc nulls first,
  first_name asc nulls first,
  last_name asc nulls first,
  preferred_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 17, column 1.
  Expected one of:
          * SELECT

  Location:
     and c01 > 0   and w01 > 0  ??? auto sr <- s02 / s01; auto cr

  Write stats: received 1307 chars / 1307 bytes; tail: …'asc nulls first,\\n
  preferred_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Build outlet label prefixed with channel kind
case
  when all_sales.sales_ch…et) by rollup all_sales.sales_channel, outlet as g_outlet
order by g_channel, g_outlet nulls first, channel nulls first, outlet nulls first
limit 100`

  ```text
  …
  RTY
          * CHART
          * "merge"i
          * _PROPERTIES
          * PARSE_COMMENT
          * UNIQUE
          * SHORTHAND_MODIFIER
          * WHERE
          * ROWSET
          * PURPOSE
          * CREATE
          * SELECT
          * DATASOURCE_PARTIAL
          * WITH
          * DATASOURCE_ROOT
          * AUTO
          * PARAM
          * DEF
          * TYPE
          * VALIDATE
          * SHOW
          * FROM
          * PUBLISH_ACTION
          * IMPORT
          * PARAMETER
          * MOCK
          * $END
          * COPY
  Previous tokens: [Token('PARSE_COMMENT', '# Build outlet label prefixed with
  channel kind\n')]

  Location:
  ...el prefixed with channel kind ??? case   when all_sales.sales_ch...

  Write stats: received 1263 chars / 1263 bytes; tail: …'lls first, channel nulls
  first, outlet nulls first\\nlimit 100'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

# Build outlet label prefixed with channel kind
case
  when all_sales.sales_channe…et) by rollup all_sales.sales_channel, outlet as g_outlet
order by g_channel, g_outlet nulls first, channel nulls first, outlet nulls first
limit 100`

  ```text
  …
          * FROM
          * WHERE
          * CREATE
          * TYPE
          * MOCK
          * CHART
          * SELF_IMPORT
          * COPY
          * _DEF_TABLE
          * AUTO
          * "merge"i
          * RAW_SQL
          * SHORTHAND_MODIFIER
          * _PROPERTIES
          * PROPERTY
          * PURPOSE
          * WITH
          * UNIQUE
          * PARSE_COMMENT
          * SHOW
          * VALIDATE
          * $END
          * DATASOURCE_ROOT
          * PARAM
          * SELECT
          * DATASOURCE
          * PUBLISH_ACTION
  Previous tokens: [Token('PARSE_COMMENT', '# Build outlet label prefixed with
  channel kind\n')]

  Location:
  ...el prefixed with channel kind ??? case   when all_sales.sales_ch...

  Write stats: received 1263 chars / 1263 bytes; tail: …'lls first, channel nulls
  first, outlet nulls first\\nlimit 100'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
select
…tlet as g_channel,
    grouping(outlet) by rollup all_sales.sales_channel, outlet as g_outlet
order by g_channel, g_outlet, channel, outlet
limit 100`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a
  semicolon `;`.
  Location:
  ...let, channel, outlet limit 100 ???

  Write stats: received 1164 chars / 1164 bytes; tail: …'tlet\\norder by
  g_channel, g_outlet, channel, outlet\\nlimit 100'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Step 1: ZIP codes from the parameter where mo…rred_prefixes
  )
select
  store_sales.store.name as store_name,
  sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...ore_sales.store.zip, 1, 2) in ??? (     select param_prefixes

  Write stats: received 1042 chars / 1042 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/physical_sales.preql:physical_sales select sum(physical_sales.quantity) as ticket_qty, count(physical_sales.row_counter) as line_items, avg(physical_sales.ext_discount_amount) as avg_disc, avg(physical_sales.net_paid) as avg_paid by physical_sales.ticket_number limit 10;`

  ```text
  --> 2:195
    |
  2 | select sum(physical_sales.quantity) as ticket_qty,
  count(physical_sales.row_counter) as line_items,
  avg(physical_sales.ext_discount_amount) as avg_disc,
  avg(physical_sales.net_paid) as avg_paid by physical_sales.ticket_number limit
  10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...l_sales.net_paid) as avg_paid ??? by physical_sales.ticket_numbe...
  ```
- `trilogy run --import raw/physical_sales.preql:physical_sales select count(physical_sales.row_counter ? sum(physical_sales.quantity) by physical_sales.ticket_number between 1 and 20) by 1 as bucket_1_20_line_items limit 10;`

  ```text
  --> 2:125
    |
  2 | select count(physical_sales.row_counter ? sum(physical_sales.quantity) by
  physical_sales.ticket_number between 1 and 20) by 1 as bucket_1_20_line_items
  limit 10;
    |
  ^---
    |
    = expected aggregate_all, aggregate_rollup, aggregate_cube,
  aggregate_grouping_sets, aggregate_paren_by, or IDENTIFIER
  Location:
  ...t_number between 1 and 20) by ??? 1 as bucket_1_20_line_items li...
  ```
- `trilogy run --import raw/physical_sales.preql:physical_sales auto ticket_qty <- sum(physical_sales.quantity) by physical_sales.ticket_number; auto in_bucket(t) -> ticket_qty ? ticket_qty between t[1] and t[2]; auto bucket_line_items(t) <- count(in_bucket(t)); select bucket_line_items([1,20]) as cnt_bucket1;`

  ```text
  --> 2:87
    |
  2 | auto ticket_qty <- sum(physical_sales.quantity) by
  physical_sales.ticket_number; auto in_bucket(t) -> ticket_qty ? ticket_qty
  between t[1] and t[2]; auto bucket_line_items(t) <- count(in_bucket(t)); select
  bucket_line_items([1,20]) as cnt_bucket1;
    |
  ^---
    |
    = expected prop_ident or prop_ident_wildcard
  Location:
  ...cal_sales.ticket_number; auto ??? in_bucket(t) -> ticket_qty ? t...
  ```
- `trilogy file write query12.preql --content import raw.web_sales as web_sales;

# Filter to items in Sports, Books, or Home categories, sold between 1999-02-2…t_sales as pct_of_class_total
order by
    category asc,
    class asc,
    item_code asc,
    description asc,
    pct_of_class_total asc
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...nded web sales price per item ??? auto item_total_ext_sales <- s...

  Write stats: received 1000 chars / 1000 bytes; tail: …',\\n    description
  asc,\\n    pct_of_class_total asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as web_sales;

# Filter: web sales of items in Sports/Books/Home, sold between 1999-02-22 and…t_sales as pct_of_class_total
order by
    category asc,
    class asc,
    item_code asc,
    description asc,
    pct_of_class_total asc
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...e per item (item-level grain) ??? auto item_total_ext_sales <- s...

  Write stats: received 1010 chars / 1010 bytes; tail: …',\\n    description
  asc,\\n    pct_of_class_total asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as web_sales;

where web_sales.item.category in ('Sports', 'Books', 'Home')
  and web_sales.d…em.desc as description,
    web_sales.item.category as category,
    web_sales.item.class as class,
    web_sales.item.current_price as current_price`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a
  semicolon `;`.
  Location:
  ...current_price as current_price ???

  Write stats: received 387 chars / 387 bytes; tail: …' as class,\\n
  web_sales.item.current_price as current_price'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as web_sales;

where web_sales.item.category in ('Sports', 'Books', 'Home')
  and web_sales.d…t_sales as pct_of_class_total
order by
    category asc,
    class asc,
    item_code asc,
    description asc,
    pct_of_class_total asc
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '1999-03-24'::date  ??? auto item_total_ext_sales <- s...

  Write stats: received 787 chars / 787 bytes; tail: …',\\n    description
  asc,\\n    pct_of_class_total asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as web_sales;

where web_sales.item.category in ('Sports', 'Books', 'Home')
  and web_sales.d…t_sales as pct_of_class_total
order by
    category asc,
    class asc,
    item_code asc,
    description asc,
    pct_of_class_total asc
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
   numerator at the item grain) ??? auto item_total_ext_sales <- s...

  Write stats: received 943 chars / 943 bytes; tail: …',\\n    description
  asc,\\n    pct_of_class_total asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as web_sales;

# Filter conditions: item category and date range
where web_sales.item.categor…t_sales as pct_of_class_total
order by
    category asc,
    class asc,
    item_code asc,
    description asc,
    pct_of_class_total asc
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...ass level for the denominator ??? auto class_ext_sales <- sum(we...

  Write stats: received 834 chars / 834 bytes; tail: …',\\n    description
  asc,\\n    pct_of_class_total asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.sales_channel, count(all_sales.item.id) as cnt, count_distinct(all_sales.date.year) as yrs, count_distinct(all_sales.item.id) as items group by all_sales.sales_channel order by all_sales.sales_channel; duckdb`

  ```text
  --> 2:153
    |
  2 | select all_sales.sales_channel, count(all_sales.item.id) as cnt,
  count_distinct(all_sales.date.year) as yrs, count_distinct(all_sales.item.id)
  as items group by all_sales.sales_channel order by all_sales.sales_channel;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...t(all_sales.item.id) as items ??? group by all_sales.sales_chann...
  ```
- `trilogy file write query20.preql --content import raw.catalog_sales as cs;

# Filter: items in Sports, Books, or Home categories, sold between 1999-02-22 and…c nulls first,
    class asc nulls first,
    item_code asc nulls first,
    description asc nulls first,
    pct_of_class asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...extended sales price per item ??? auto item_total_ext_sales <- s...

  Write stats: received 1001 chars / 1001 bytes; tail: …'asc nulls first,\\n
  pct_of_class asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql -e -c import raw.catalog_sales as cs;
where cs.item.category in ('Sports', 'Books', 'Home')
  and cs.sold_date.date between …c nulls first,
    class asc nulls first,
    item_code asc nulls first,
    description asc nulls first,
    pct_of_class asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 4, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...::date and '1999-03-24'::date ??? auto item_total_ext_sales <- s...

  Write stats: received 761 chars / 761 bytes; tail: …'asc nulls first,\\n
  pct_of_class asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `cli-misuse`

- `trilogy explore raw/physical_sales.preql --regex promotion customer_demographic date item`

  ```text
  Got unexpected extra arguments (customer_demographic date item)
  ```
- `trilogy read_file raw/physical_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/physical_sales.preql --show datasources,imports`

  ```text
  Invalid value for '--show': 'datasources,imports' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```
- `trilogy explore raw/web_sales.precl --regex net_paid|ext_discount`

  ```text
  Invalid value for 'PATH': File 'raw/web_sales.precl' does not exist.
  ```
- `trilogy explore raw/all_sales.precl --regex store\.|date\.year`

  ```text
  Invalid value for 'PATH': File 'raw/all_sales.precl' does not exist.
  ```
- `trilogy explore raw/store_sales.preql --regex store_sales\.(ss_|date_dim|customer|addr|demo|hdemo|item|store|promo) --show all`

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

### `undefined-concept`

- `trilogy run query06.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.current_price. Suggestions: ['store_sales.item.current_price']")
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  billing_customer.preferred_cust_flag. Suggestions:
  ['store_sales.billing_customer.preferred_cust_flag',
  'store_sales.return_customer.preferred_cust_flag',
  'store_sales.billing_customer.first_name']")
  ```
- `trilogy run query20.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  category.')
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.param_zips. Valid:
  'STRING'.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.param_zips. Valid:
  'STRING'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/physical_sales.preql:physical_sales auto ticket_qty <- sum(physical_sales.quantity) by physical_sales.ticket_number; select count(ticket_qty ? ticket_qty between 100 and 200);`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...icket_qty between 100 and 200) ??? ;
  ```
