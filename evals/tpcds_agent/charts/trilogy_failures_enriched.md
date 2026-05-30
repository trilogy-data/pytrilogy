# Trilogy failure analysis — 20260530-194955

- Run `20260530-194955` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 470 | failed: 54 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 26 | 48% |
| `syntax-parse` | 14 | 26% |
| `syntax-missing-alias` | 6 | 11% |
| `undefined-concept` | 4 | 7% |
| `cli-misuse` | 3 | 6% |
| `join-resolution` | 1 | 2% |

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

- `trilogy run --import raw/date:date select date.year, min(date.week_seq) as min_ws, max(date.week_seq) as max_ws group by date.year order by date.year;`

  ```text
  --> 2:78
    |
  2 | select date.year, min(date.week_seq) as min_ws, max(date.week_seq) as
  max_ws group by date.year order by date.year;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
   max(date.week_seq) as max_ws ??? group by date.year order by da...
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Web and catalog sales only
where all_sales.sales_channel in ('WEB', 'CATALOG…hu,
    coalesce(fri_ratio, 0) as fri,
    coalesce(sat_ratio, 0) as sat
where all_sales.date.year = 2001
order by week_seq asc nulls first
limit 60;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...eek (0=Sunday ... 6=Saturday) ??? auto ws_sun_2001 <- sum(all_sa...

  Write stats: received 3062 chars / 3062 bytes; tail: …'date.year = 2001\\norder
  by week_seq asc nulls first\\nlimit 60;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Web and catalog sales only (channel filter applies globally to all aggregate…as wed,
    thu_ratio as thu,
    fri_ratio as fri,
    sat_ratio as sat
where all_sales.date.year = 2001
order by week_seq asc nulls first
limit 60;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...eek (0=Sunday ... 6=Saturday) ??? auto ws_sun_2001 <- sum(all_sa...

  Write stats: received 3023 chars / 3023 bytes; tail: …'date.year = 2001\\norder
  by week_seq asc nulls first\\nlimit 60;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.date.year, all_sales.date.day_of_week, all_sales.sales_channel, sum(all_sales.ext_sales_price) … group by all_sales.date.year, all_sales.date.day_of_week, all_sales.sales_channel order by all_sales.date.year, all_sales.date.day_of_week limit 30;`

  ```text
  --> 2:214
    |
  2 | select all_sales.date.year, all_sales.date.day_of_week,
  all_sales.sales_channel, sum(all_sales.ext_sales_price) as total where
  all_sales.date.year in (2001, 2002) and all_sales.sales_channel in ('WEB',
  'CATALOG') group by all_sales.date.year, all_sales.date.day_of_week,
  all_sales.sales_channel order by all_sales.date.year,
  all_sales.date.day_of_week limit 30;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...channel in ('WEB', 'CATALOG') ??? group by all_sales.date.year,
  ```
- `trilogy run --import raw.all_sales:all_sales select
  count(distinct all_sales.billing_customer.id) as cust_count
where all_sales.date.year = 2001
  and all_sales.sales_channel in ('STORE', 'CATALOG', 'WEB');`

  ```text
  --> 3:18
    |
  3 |   count(distinct all_sales.billing_customer.id) as cust_count
    |                  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ales; select   count(distinct ??? all_sales.billing_customer.id)...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.billing_customer.id where all_sales.date.year = 2001 group by all_sales.billing_customer.id having count_distinct(all_sales.sales_channel) = 3 limit 5;`

  ```text
  --> 2:71
    |
  2 | select all_sales.billing_customer.id where all_sales.date.year = 2001 group
  by all_sales.billing_customer.id having count_distinct(all_sales.sales_channel)
  = 3 limit 5;
    |                                                                       ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...re all_sales.date.year = 2001 ??? group by all_sales.billing_cus...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

where sales.date.date between '2000-08-23'::date and '2000-09-06'::date

auto outl…up channel, outlet as total_returns,
    sum(sales.net_profit) - sum(sales.return_net_loss) as profit
order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 5, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? auto outlet = case     when sa...

  Write stats: received 731 chars / 731 bytes; tail: …'s) as profit\\norder by
  channel, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

auto outlet = case
    when sales.sales_channel = 'STORE' then concat('store_', sa…up channel, outlet as total_returns,
    sum(sales.net_profit) - sum(sales.return_net_loss) as profit
order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [203]: Missing assignment operator '<-' and expression in derivation.
  Write `auto X <- <expression>;` (also valid: `metric`, `property`, `rowset`).
  Example: `auto orders_per_customer <- count(orders.id) by customer.id;`.
  Location:
  ..._sales as sales;  auto outlet ??? = case     when sales.sales_ch...

  Write stats: received 731 chars / 731 bytes; tail: …'s) as profit\\norder by
  channel, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query06.preql --content import raw.physical_sales as store_sales;

# Average current price of items within the same category (across all i…state,
    count(store_sales.billing_customer.id) as customer_count
order by
    customer_count asc nulls first,
    state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 7, column 74.
  Expected one of:
          * _TERMINATOR

  Location:
  ...> 1.2 * avg_price_by_category ??? by item.id;  where     store_s...

  Write stats: received 689 chars / 689 bytes; tail: …'_count asc nulls first,\\n
  state asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Step 1: ZIP codes from the param list (first … pref_zip_prefixes

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.name
limit 100;`

  ```text
  …
   pref_zip_prefixes <-
  array_distinct(array_transform(\n    array_agg(customer.address.zip ?
  preferred_zip > 10),\n    (x) -> substring(x, 1, 2)\n));\n\nwhere
  store_sales.date.year = 1998\n  and store_sales.date.quarter = 2\n  and
  substring(store_sales.store.zip, 1, 2) in param_zip_prefixes\n  and
  substring(store_sales.store.zip, 1, 2) in pref_zip_prefixes\n\nselect\n
  store_sales.store.name,\n    sum(store_sales.net_profit) as
  total_net_profit\norder by store_sales.store.name\nlimit 100;") at line 6,
  column 77.
  Expected one of:
          * "@"
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...y_transform(split(zips, ','), ??? (x) -> substring(x, 1, 2)));

  Write stats: received 956 chars / 956 bytes; tail: …' total_net_profit\\norder
  by store_sales.store.name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.physical_sales as store;
import raw.web_sales as web;
import raw.customer as c;

# Merge billing custom…order by
    billing_customer_code nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 22, column 1.
  Expected one of:
          * SELECT

  Location:
  ...ro denominator as zero ratio) ??? auto store_growth <- case when...

  Write stats: received 1667 chars / 1667 bytes; tail: …' nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.warehouse:w select w.id as wid, w.county as county, count(w.id) as cnt group by w.id, w.county limit 20;`

  ```text
  --> 2:60
    |
  2 | select w.id as wid, w.county as county, count(w.id) as cnt group by w.id,
  w.county limit 20;
    |                                                            ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...as county, count(w.id) as cnt ??? group by w.id, w.county limit
  ```
- `trilogy run --import raw.catalog_sales:cs select cs.bill_customer.id as cid, cs.item.id as iid, count(cs.order_number) as cnt where cs.sold_date.year=2001 an…ate.quarter in (1,2,3) and cs.bill_customer.id in (7678, 3035, 6104, 4221, 2579) group by cs.bill_customer.id, cs.item.id order by cid, iid limit 20;`

  ```text
  --> 2:208
    |
  2 | select cs.bill_customer.id as cid, cs.item.id as iid,
  count(cs.order_number) as cnt where cs.sold_date.year=2001 and
  cs.sold_date.quarter in (1,2,3) and cs.bill_customer.id in (7678, 3035, 6104,
  4221, 2579) group by cs.bill_customer.id, cs.item.id order by cid, iid limit
  20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...7678, 3035, 6104, 4221, 2579) ??? group by cs.bill_customer.id,
  ```
- `trilogy file write query20.preql --content import raw.catalog_sales as catalog_sales;

# Filter to qualifying catalog sales: items in Sports/Books/Home categ…ls first,
    catalog_sales.item.id asc nulls first,
    catalog_sales.item.desc asc nulls first,
    percentage_of_class asc nulls first
limit 100;
`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...m across all qualifying sales ??? auto item_ext_sales <- sum(cat...

  Write stats: received 1173 chars / 1173 bytes; tail: …'s first,\\n
  percentage_of_class asc nulls first\\nlimit 100;\\n'.
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

- `trilogy run query06.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.current_price. Suggestions: ['store_sales.item.current_price']")
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  customer.preferred_cust_flag. Suggestions:
  ['store_sales.return_customer.preferred_cust_flag',
  'store_sales.billing_customer.preferred_cust_flag']")
  ```
- `trilogy run query10.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.ship_customer.id. Suggestions: ['catalog_sales.bill_customer.id',
  'catalog_sales.ship_mode.id', 'catalog_sales.ship_date.id']")
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['c.first_name', 'c.last_name']")
  ```

### `cli-misuse`

- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy run --import raw.physical_sales:ss --import raw.catalog_sales:cs merge ss.item.id into ~cs.item.id; merge ss.billing_customer.id into ~cs.bill_custom…and cs.sold_date.year=2001 and cs.sold_date.quarter in (1,2,3) and ss.billing_customer.id = cs.bill_customer.id and ss.item.id = cs.item.id limit 10;`

  ```text
  'merge ss.billing_customer.id into ~cs.bill_customer.id;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.physical_sales:ss --import raw.catalog_sales:cs merge ss.item.id into ~cs.item.id; merge ss.billing_customer.id into ~cs.bill_custom… sc, cs.bill_customer.id as cc where ss.date.year=2001 and ss.date.quarter=1 and cs.sold_date.year=2001 and cs.sold_date.quarter in (1,2,3) limit 10;`

  ```text
  'merge ss.billing_customer.id into ~cs.bill_customer.id;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `join-resolution`

- `trilogy run --import raw.physical_sales:ss --import raw.catalog_sales:cs select ss.billing_customer.id, ss.item.id, cs.bill_customer.id, cs.item.id where ss.…and cs.sold_date.year=2001 and cs.sold_date.quarter in (1,2,3) and ss.billing_customer.id = cs.bill_customer.id and ss.item.id = cs.item.id limit 10;`

  ```text
  Could not resolve connections for query with output
  ['ss.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'ss.item.id<Purpose.KEY>Derivation.ROOT>',
  'cs.bill_customer.id<Purpose.KEY>Derivation.ROOT>',
  'cs.item.id<Purpose.KEY>Derivation.ROOT>'] from current model.
  ```
