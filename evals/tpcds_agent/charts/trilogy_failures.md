# Trilogy failure analysis — 20260530-040508

- Run `20260530-040508_base` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 854 | failed: 115 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 45 | 39% |
| `other` | 25 | 22% |
| `syntax-missing-alias` | 23 | 20% |
| `undefined-concept` | 9 | 8% |
| `cli-misuse` | 8 | 7% |
| `join-resolution` | 5 | 4% |

## Detail

### `syntax-parse`

- `trilogy run - --import raw.date_dim:dd`

  ```text
  --> 2:135
    |
  2 | select dd.year, min(dd.week_seq) as min_ws, max(dd.week_seq) as max_ws,
  count(dd.date_sk) as days where dd.year in (2000, 2001, 2002) group by dd.year
  order by dd.year;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...dd.year in (2000, 2001, 2002) ??? group by dd.year order by dd.y...
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

# Merge the fact tables through a s…d_date.dow = 5) as fri,
    sum(ws_2001 ? web_sales.sold_date.dow = 6) as sat
where web_sales.sold_date.year = 2001
order by wk_seq asc nulls first;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 11, column 37.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ales_by_ws_dow <- daily_sales ??? by web_sales.sold_date.week_se...

  Write stats: received 1333 chars / 1333 bytes; tail: …'ales.sold_date.year =
  2001\\norder by wk_seq asc nulls first;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

merge catalog_sales.sold_date.date_…ld_date.dow = 5) as fri,
    sum(ws_2001 ? web_sales.sold_date.dow = 6) as sat
where web_sales.sold_date.year = 2001
order by wk_seq asc nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 9, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._by_ws_dow <- daily_sales     ??? by (web_sales.sold_date.week_s...

  Write stats: received 984 chars / 984 bytes; tail: …'sales.sold_date.year =
  2001\\norder by wk_seq asc nulls first;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

merge catalog_sales.sold_date.date_…price);

# Sales grouped by week and day-of-week
auto sales_by_ws_dow <- daily_sales
    by (web_sales.sold_date.week_seq, web_sales.sold_date.dow);
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 11, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._by_ws_dow <- daily_sales     ??? by (web_sales.sold_date.week_s...

  Write stats: received 424 chars / 424 bytes; tail: …'by
  (web_sales.sold_date.week_seq, web_sales.sold_date.dow);\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

merge catalog_sales.sold_date.date_…y
auto sales_by_ws_dow_2001 <- daily_sales
    by (web_sales.sold_date.week_seq, web_sales.sold_date.dow)
    where web_sales.sold_date.year = 2001;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 11, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...s_dow_2001 <- daily_sales     ??? by (web_sales.sold_date.week_s...

  Write stats: received 490 chars / 490 bytes; tail: …'s.sold_date.dow)\\n
  where web_sales.sold_date.year = 2001;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…ales,
    sum(web_returns_amt) as total_returns,
    sum(web_profit) as profit
having channel = 'Web'
order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n') at line 45, column 1.
  Expected one of:
          * ORDER
          * MERGE
          * _TERMINATOR
          * LIMIT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...ofit having channel = 'Store' ??? union select     channel_name

  Write stats: received 3612 chars / 3612 bytes; tail: …"nnel = 'Web'\\norder by
  channel, outlet nulls first\\nlimit 100;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…date and '2000-09-06'::date) - sum(web_returns.net_loss ? web_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as web_profit;`

  ```text
  …
  T
          * PARAMETER
          * _PROPERTIES
          * WHERE
          * _DEF_TABLE
          * RAW_SQL
          * PARSE_COMMENT
          * TYPE
          * "merge"i
          * WITH
          * PURPOSE
          * IMPORT
          * SELECT
          * CREATE
          * MOCK
          * DATASOURCE_ROOT
          * PROPERTY
          * AUTO
          * FROM
          * UNIQUE
          * PERSIST_MODE
          * SHORTHAND_MODIFIER
          * ROWSET
          * DATASOURCE_PARTIAL
          * SHOW
          * DEF
          * VALIDATE
          * PUBLISH_ACTION
          * PARAM
  Previous tokens: [Token('PARSE_COMMENT', '# Store channel\n')]

  Location:
  ...ers, combined # Store channel ??? store_sales.store.store_id, 'S...

  Write stats: received 2749 chars / 2749 bytes; tail: …"en '2000-08-23'::date
  and '2000-09-06'::date) as web_profit;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…ns.net_loss ? web_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date)) as profit
order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n') at line 25, column 1.
  Expected one of:
          * WHERE
          * METADATA
          * _TERMINATOR
          * LIMIT
          * MERGE
          * HAVING
          * ORDER
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'profit')]

  Location:
  ...2000-09-06'::date)) as profit ??? union select     'Catalog' as

  Write stats: received 2630 chars / 2630 bytes; tail: …')) as profit\\norder by
  channel, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…l.channel, web_channel.outlet, web_channel.total_sales, web_channel.total_returns, web_channel.profit
order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [203]: Missing assignment operator '<-' and expression in derivation.
  Write `auto X <- <expression>;` (also valid: `metric`, `property`, `rowset`).
  Example: `auto orders_per_customer <- count(orders.id) by customer.id;`.
  Location:
   channel rowset store_channel ??? (     channel string,     outl...

  Write stats: received 3483 chars / 3483 bytes; tail: …'annel.profit\\norder by
  channel, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.promotion.channel_email, store_sales.promotion.channel_event, count(store_sales.ticket_number) as cnt where store_sales.date_dim.year = 2000 group by 1,2;`

  ```text
  --> 2:161
    |
  2 | select store_sales.promotion.channel_email,
  store_sales.promotion.channel_event, count(store_sales.ticket_number) as cnt
  where store_sales.date_dim.year = 2000 group by 1,2;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...re_sales.date_dim.year = 2000 ??? group by 1,2;
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.store_sales as store_sales;
import raw.customer as customer;
import raw.custome…fixes)

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...s <- customer_address.zip     ??? where customer.preferred_cust_...

  Write stats: received 1430 chars / 1430 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.store_sales as store_sales;
import raw.customer as customer;
import raw.custome…fixes)

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...s <- customer_address.zip     ??? where customer.preferred_cust_...

  Write stats: received 1430 chars / 1430 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;
import raw.store_sales as store_sales;
import raw.customer as customer;
import raw.customer…ching_prefixes)
select store_sales.store.store_name as store_name, sum(store_sales.net_profit) as total_net_profit order by store_name asc limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 5, column 50.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._zips <- customer_address.zip ??? where customer.preferred_cust_...

  Write stats: received 868 chars / 868 bytes; tail: …'ofit) as total_net_profit
  order by store_name asc limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select '24' in (select substring(unnest(split('24128,76232,65084', ',')), 1, 2)) as check; duckdb`

  ```text
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside
  parens)? Trilogy does not support subqueries — joins are auto-resolved from
  dotted paths. To filter on a value that lives on a related dimension, reference
  its dot-path directly. Example: instead of `where ss.store_id in (select
  store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...s store_sales; select '24' in ??? (select substring(unnest(split...
  ```
- `trilogy run --import raw.store_sales:store_sales select substring(store_sales.store.zip, 1, 2) as f2, store_sales.store.store_name as sn, sum(store_sales.net_profit) as profit where store_sales.date_dim.year = 1998 and store_sales.date_dim.qoy = 2 group by sn order by sn asc limit 100; duckdb`

  ```text
  --> 2:200
    |
  2 | select substring(store_sales.store.zip, 1, 2) as f2,
  store_sales.store.store_name as sn, sum(store_sales.net_profit) as profit where
  store_sales.date_dim.year = 1998 and store_sales.date_dim.qoy = 2 group by sn
  order by sn asc limit 100;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   store_sales.date_dim.qoy = 2 ??? group by sn order by sn asc li...
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.store_sales as store_sales;

# Preferred customer ZIPs (>10 preferred customers… '67')

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_SUBSTRING', 'substring(') at line 6, column 41.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'distinct')]

  Location:
  ...cust_zip_prefixes <- distinct ??? substring(store_sales.customer...

  Write stats: received 1914 chars / 1914 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_SUBSTRING', 'substring(') at line 6, column 41.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'distinct')]

  Location:
  ...cust_zip_prefixes <- distinct ??? substring(store_sales.customer...

  Write stats: received 1244 chars / 1244 bytes; tail: …') as
  total_net_profit\r\\norder by store_name asc\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...mer.customer_address.zip     ??? where store_sales.customer.pre...

  Write stats: received 1607 chars / 1607 bytes; tail: …') as
  total_net_profit\r\\norder by store_name asc\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 6, column 66.
  Expected one of:
          * _TERMINATOR

  Location:
  ...customer.customer_address.zip ??? where store_sales.customer.pre...

  Write stats: received 1454 chars / 1454 bytes; tail: …'it) as total_net_profit
  order by store_name asc limit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 6, column 107.
  Expected one of:
          * _TERMINATOR

  Location:
  ...customer.customer_address.zip ??? where store_sales.customer.pre...

  Write stats: received 1018 chars / 1018 bytes; tail: …') as
  total_net_profit\r\\norder by store_name asc\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 66.
  Expected one of:
          * _TERMINATOR

  Location:
  ...customer.customer_address.zip ??? where pref_zip_cnt > 10; auto...

  Write stats: received 820 chars / 820 bytes; tail: …') as
  total_net_profit\r\\norder by store_name asc\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales auto pref_zip_cnt <- count(store_sales.customer.customer_sk ? store_sales.customer.preferred_cust_flag = 'Y'…tore_sales.customer.customer_address.zip; auto gt10 <- store_sales.customer.customer_address.zip where pref_zip_cnt > 10; select gt10 limit 5; duckdb`

  ```text
  --> 2:212
    |
  2 | auto pref_zip_cnt <- count(store_sales.customer.customer_sk ?
  store_sales.customer.preferred_cust_flag = 'Y') by
  store_sales.customer.customer_address.zip; auto gt10 <-
  store_sales.customer.customer_address.zip where pref_zip_cnt > 10; select gt10
  limit 5;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...customer.customer_address.zip ??? where pref_zip_cnt > 10; selec...
  ```
- `trilogy run --import raw.store_sales:store_sales auto pref_zip_cnt <- count(store_sales.customer.customer_sk ? store_sales.customer.preferred_cust_flag = 'Y'…dress.zip; auto pref_pfx <- distinct substring(store_sales.customer.customer_address.zip ? pref_zip_cnt > 10, 1, 2); select pref_pfx limit 10; duckdb`

  ```text
  --> 2:183
    |
  2 | auto pref_zip_cnt <- count(store_sales.customer.customer_sk ?
  store_sales.customer.preferred_cust_flag = 'Y') by
  store_sales.customer.customer_address.zip; auto pref_pfx <- distinct
  substring(store_sales.customer.customer_address.zip ? pref_zip_cnt > 10, 1, 2);
  select pref_pfx limit 10;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ip; auto pref_pfx <- distinct ??? substring(store_sales.customer...
  ```
- `trilogy run --import raw.store_sales:store_sales select count(store_sales.ticket_number) by sum(store_sales.quantity) by store_sales.ticket_number as bucket limit 5;`

  ```text
  Syntax [211]: Expression in `by` clause must be wrapped in
  parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work
  without parens, but any function call, cast, or other expression needs them.
  Location:
  ...nt(store_sales.ticket_number) ??? by sum(store_sales.quantity) b...
  ```
- `trilogy run --import raw.store_sales:store_sales select bucket, cnt from (auto ticket_qty <- sum(store_sales.quantity) by store_sales.ticket_number;
auto buc…et_number) as cnt, avg(store_sales.ext_discount_amt) as avg_ext_discount_amt, avg(store_sales.net_paid) as avg_net_paid) where bucket in (1,2,3,4,5);`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...ore_sales; select bucket, cnt ??? from (auto ticket_qty <- sum(s...
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.customer_sk …    customer_code asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 19, column 1.
  Expected one of:
          * SELECT

  Location:
   (zero denominator = 0 ratio) ??? auto ss_growth <- ss_rev_2002

  Write stats: received 1657 chars / 1657 bytes; tail: …'s first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.customer_sk …    customer_code asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 16, column 1.
  Expected one of:
          * SELECT

  Location:
   (zero denominator = 0 ratio) ??? auto ss_growth <- ss_rev_2002

  Write stats: received 1472 chars / 1472 bytes; tail: …'s first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 16, column 1.
  Expected one of:
          * SELECT

  Location:
  ...(zero denominator = 0 ratio) ??? auto ss_growth <- ss_rev_2002

  Write stats: received 1501 chars / 1501 bytes; tail: …' first,\r\\n
  preferred_cust_flag asc nulls first\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 14, column 1.
  Expected one of:
          * SELECT

  Location:
  ...01 > 0 and ws_rev_2001 > 0  ??? auto ss_growth <- ss_rev_2002

  Write stats: received 1374 chars / 1374 bytes; tail: …' first,\r\\n
  preferred_cust_flag asc nulls first\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.customer_demographics.marital_status, store_sales.customer.customer_demographics…raphics.dep_count, store_sales.sales_price, count(store_sales.ticket_number) as cnt where store_sales.date_dim.year = 2001 group by 1,2,3,4 limit 20;`

  ```text
  --> 2:287
    |
  2 | select store_sales.customer.customer_demographics.marital_status,
  store_sales.customer.customer_demographics.education_status,
  store_sales.customer.household_demographics.dep_count, store_sales.sales_price,
  count(store_sales.ticket_number) as cnt where store_sales.date_dim.year = 2001
  group by 1,2,3,4 limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...re_sales.date_dim.year = 2001 ??? group by 1,2,3,4 limit 20;
  ```
- `trilogy run --import raw/store_sales:s select s.item.brand_id as brand, s.item.class_id as class, s.item.category_id as cat, s.date_dim.year as yr, count(s.quantity) as cnt where s.date_dim.year between 1999 and 2001 group by brand, class, cat, yr limit 5;`

  ```text
  --> 2:179
    |
  2 | select s.item.brand_id as brand, s.item.class_id as class,
  s.item.category_id as cat, s.date_dim.year as yr, count(s.quantity) as cnt
  where s.date_dim.year between 1999 and 2001 group by brand, class, cat, yr
  limit 5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...im.year between 1999 and 2001 ??? group by brand, class, cat, yr...
  ```
- `trilogy run --import raw/store_sales:s --import raw/catalog_sales:c --import raw/web_sales:w select s.item.brand_id as brand_id, s.item.class_id as class_id, s.item.category_id as cat_id, sum(s.quantity * s.list_price) as s_val from s where s.date_dim.year between 1999 and 2001 limit 5;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...tity * s.list_price) as s_val ??? from s where s.date_dim.year b...
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.date_dim.month_seq as mseq, min(store_sales.date_dim.year) as min_yr, max(store_sales.date_dim.year) as max_yr where store_sales.date_dim.year between 1999 and 2001 group by mseq order by mseq limit 20;`

  ```text
  --> 2:185
    |
  2 | select store_sales.date_dim.month_seq as mseq,
  min(store_sales.date_dim.year) as min_yr, max(store_sales.date_dim.year) as
  max_yr where store_sales.date_dim.year between 1999 and 2001 group by mseq
  order by mseq limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...im.year between 1999 and 2001 ??? group by mseq order by mseq li...
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.date_dim.month_seq as mseq from store_sales limit 5;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...es.date_dim.month_seq as mseq ??? from store_sales limit 5;
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.date_dim.month_seq as mseq, min(store_sales.date_dim.year) as min_yr where store_sales.date_dim.year = 2001 group by mseq order by mseq limit 5;`

  ```text
  --> 2:128
    |
  2 | select store_sales.date_dim.month_seq as mseq,
  min(store_sales.date_dim.year) as min_yr where store_sales.date_dim.year = 2001
  group by mseq order by mseq limit 5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...re_sales.date_dim.year = 2001 ??? group by mseq order by mseq li...
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/catalog_sales:catalog_sales --import raw/web_sales:web_sales select store_sales.item.brand as b…sum(store_sales.quantity * store_sales.list_price) as total where store_sales.date_dim.year between 1999 and 2001 group by brand, class, cat limit 5;`

  ```text
  --> 4:222
    |
  4 | select store_sales.item.brand as brand, store_sales.item.class as class,
  store_sales.item.category as cat, sum(store_sales.quantity *
  store_sales.list_price) as total where store_sales.date_dim.year between 1999
  and 2001 group by brand, class, cat limit 5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...im.year between 1999 and 2001 ??? group by brand, class, cat lim...
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as catalog_sales;

where
    catalog_sales.sold_date.year = 2001
    and catalog_sales.so…elect
    catalog_sales.bill_addr.zip as zip_code,
    catalog_sales.sales_price as per_line_sales_price
order by
    zip_code nulls first
limit 100;`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'regexp ') at line 7, column 37.
  Expected one of:
          * RPAR
          * LOGICAL_AND
          * LOGICAL_OR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'catalog_sales.bill_addr.zip')]

  Location:
    catalog_sales.bill_addr.zip ??? regexp '^(85669|86197|88274|83...

  Write stats: received 509 chars / 509 bytes; tail: …'ine_sales_price\\norder
  by\\n    zip_code nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as catalog_sales;

where
    catalog_sales.sold_date.year = 2001
    and catalog_sales.so…addr.zip as zip_code,
    sum(catalog_sales.sales_price) as total_per_line_sales_price
group by zip_code
order by
    zip_code nulls first
limit 100;`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'group ') at line 15, column 1.
  Expected one of:
          * _TERMINATOR
          * WHERE
          * ORDER
          * MERGE
          * HAVING
          * COMMA
          * LIMIT
          * METADATA
  Previous tokens: [Token('IDENTIFIER', 'total_per_line_sales_price')]

  Location:
  ...as total_per_line_sales_price ??? group by zip_code order by

  Write stats: received 846 chars / 846 bytes; tail: …'oup by zip_code\\norder
  by\\n    zip_code nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write debug10.preql --content import raw.catalog_sales as cs;
# Check: does the warehouse_sk appear in TPC-DS catalog_sales table?
# In TPC-DS, catalog_sales has a warehouse_sk column.
# Let's look at the datasource to see what columns map to what
read_file raw/catalog_sales.preql`

  ```text
  …
   DATASOURCE_ROOT
          * "merge"i
          * PARAMETER
          * TYPE
          * PERSIST_MODE
          * DATASOURCE
          * DATASOURCE_PARTIAL
          * AUTO
          * SHOW
          * COPY
          * _DEF_TABLE
          * PURPOSE
          * UNIQUE
          * FROM
          * CREATE
          * WHERE
          * PARSE_COMMENT
          * ROWSET
          * _PROPERTIES
          * DEF
          * IMPORT
          * PARAM
          * VALIDATE
          * CHART
          * $END
          * RAW_SQL
          * SELECT
  Previous tokens: [Token('PARSE_COMMENT', "# Let's look at the datasource to see
  what columns map to what\n")]

  Location:
   see what columns map to what ??? read_file raw/catalog_sales.pr...

  Write stats: received 251 chars / 251 bytes; tail: …'e what columns map to
  what\\nread_file raw/catalog_sales.preql'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write debug19.preql --content import raw.catalog_sales as cs;
# Both 11952 and 12669 have matching returns! So after the anti-join, both get eli…count(distinct cs.order_number) as unique_orders,
    sum(cs.ext_ship_cost) as total_ship_cost,
    sum(cs.net_profit) as total_net_profit
limit 100;`

  ```text
  refused to write 'debug19.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'cs.order_number') at line 20,
  column 20.
  Expected one of:
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'distinct')]

  Location:
  ...= 0 select     count(distinct ??? cs.order_number) as unique_ord...

  Write stats: received 1058 chars / 1058 bytes; tail: …'_cost,\\n
  sum(cs.net_profit) as total_net_profit\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:ss select count(ss.ticket_number) as cnt, count(distinct ss.item.item_id) as items, count(distinct ss.store.store_sk) as stores limit 5;`

  ```text
  --> 2:55
    |
  2 | select count(ss.ticket_number) as cnt, count(distinct ss.item.item_id) as
  items, count(distinct ss.store.store_sk) as stores limit 5;
    |                                                       ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...umber) as cnt, count(distinct ??? ss.item.item_id) as items, cou...
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select count(ss.ticket_number) as ss_cnt, count(sr.ticket_number) as sr_cnt, count(distinct ss.item.item_id) as items limit 5;`

  ```text
  --> 3:93
    |
  3 | select count(ss.ticket_number) as ss_cnt, count(sr.ticket_number) as
  sr_cnt, count(distinct ss.item.item_id) as items limit 5;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...er) as sr_cnt, count(distinct ??? ss.item.item_id) as items limi...
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr --import raw.catalog_sales:cs merge sr.item.item_sk into ~ss.item.item_sk; merge sr.cus…count(ss.ticket_number) as ss_cnt, count(sr.ticket_number) as sr_cnt, count(cs.quantity) as cs_cnt, count(distinct ss.item.item_id) as items limit 5;`

  ```text
  --> 4:340
    |
  4 | merge sr.item.item_sk into ~ss.item.item_sk; merge sr.customer.customer_sk
  into ~ss.customer.customer_sk; merge cs.item.item_sk into ~ss.item.item_sk;
  merge cs.bill_customer.customer_sk into ~ss.customer.customer_sk; select
  count(ss.ticket_number) as ss_cnt, count(sr.ticket_number) as sr_cnt,
  count(cs.quantity) as cs_cnt, count(distinct ss.item.item_id) as items limit 5;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ty) as cs_cnt, count(distinct ??? ss.item.item_id) as items limi...
  ```
- `trilogy run --import raw.catalog_sales:cs select count(cs.quantity) as cnt, cs.sold_date.fy_year as yr, cs.sold_date.qoy as q, count(distinct cs.item.item_sk) as items_distinct, count(cs.item.item_sk) as items_raw limit 10;`

  ```text
  --> 2:101
    |
  2 | select count(cs.quantity) as cnt, cs.sold_date.fy_year as yr,
  cs.sold_date.qoy as q, count(distinct cs.item.item_sk) as items_distinct,
  count(cs.item.item_sk) as items_raw limit 10;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...date.qoy as q, count(distinct ??? cs.item.item_sk) as items_dist...
  ```
- `trilogy file write query18.preql --content import raw.catalog_sales as catalog_sales;

# For catalog sales in 1998, restricted to:
# - billing customer-demog…es.bill_customer.customer_address.county nulls first,
    catalog_sales.item.item_id nulls first,
    g_country,
    g_state,
    g_county
limit 100;`

  ```text
  refused to write 'query18.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 15, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...S','IN','ND','OK','NM','VA')  ??? def avg_rollup(metric) -> avg(...

  Write stats: received 2755 chars / 2755 bytes; tail: …'s first,\\n
  g_country,\\n    g_state,\\n    g_county\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `other`

- `trilogy run query01.preql`

  ```text
  HAVING references 'local.cust_store_returns', which is not in
  the SELECT projection (line 9). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.cust_store_returns`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 86 (char 85). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query02.preql`

  ```text
  Recursion error building concept local.sun_ratio with grain
  Grain<Abstract> and lineage
  divide(ref:local.sun_sales_2002,ref:local.sun_sales_2001). This is likely due
  to a circular reference.
  ```
- `trilogy run query02.preql`

  ```text
  Recursion error building concept local.sun with grain
  Grain<Abstract> and lineage
  divide(ref:local.sun_sales_02,ref:local.sun_sales_01). This is likely due to a
  circular reference.
  ```
- `trilogy run - --import raw.catalog_sales:cs --import raw.web_sales:ws`

  ```text
  Recursion error building concept local.sun_02 with grain
  Grain<local.sun_01,local.sun_02,ws.sold_date.week_seq>|ref:ws.sold_date.week_se
  q between 5270 and 5275 and lineage <Filter: ref:local.sales_by where
  ref:ws.sold_date.year = 2002 and ref:ws.sold_date.dow = 0>. This is likely due
  to a circular reference.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query04.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.cs_2001', 'local.ws_2001', 'local.first_name',
  'local.preferred_flag', 'local.customer_code', 'local.last_name',
  'local.ss_2001'} out of  with found {'local.cs_2001',
  'catalog_sales.bill_customer.customer_sk', 'local.ws_2002', 'local.ss_2002',
  'local.ws_2001', 'local.ss_2001', 'web_sales.bill_customer.customer_sk',
  'local.cs_2002', 'store_sales.customer.customer_sk'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 94 (char 93). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.store.store_id, sum(store_sales.ext_sales_price … '2000-09-06'::date) - sum(store_returns.net_loss ? store_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as profit limit 5;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_agg_sum_9444807640129606',
  'local._virt_agg_sum_9914007459767470', 'local.total_returns'} out of  with
  found {'local._virt_agg_sum_9914007459767470', 'local.total_sales',
  'store_sales.store.store_id'}
  ```
- `trilogy file write --escapes --content parameter zips string;

import raw.store_sales as store_sales;

# Preferred customer ZIPs (>10 preferred customers)
au…
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100; query08.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run --import raw.store_sales:store_sales select sum(store_sales.quantity) by store_sales.ticket_number as ticket_qty, count(store_sales.ticket_number…er as avg_disc, avg(store_sales.net_paid) by store_sales.ticket_number as avg_net where ticket_qty between 1 and 100 order by ticket_qty asc limit 5;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.ticket_qty) in the same statement where clause; move to the HAVING
  clause instead; Line: 2
  ```
- `trilogy run --import raw.store_sales:store_sales auto ticket_qty <- sum(store_sales.quantity) by store_sales.ticket_number;
auto line_items <- count(store_sa… avg(store_sales.net_paid) by store_sales.ticket_number;
select avg_disc, avg_net where ticket_qty between 1 and 100 order by ticket_qty asc limit 5;`

  ```text
  ORDER BY references 'local.ticket_qty', which is not in the
  SELECT projection (line 6). Add it to SELECT to sort by it — prefix with `--`
  to keep it out of the output rows, e.g. `select ..., --local.ticket_qty order
  by local.ticket_qty asc`.
  ```
- `trilogy run --import raw.store_sales:store_sales auto ticket_qty <- sum(store_sales.quantity) by store_sales.ticket_number;
auto bucket <- case when ticket_q… 4 when ticket_qty between 81 and 100 then 5 end;
select bucket, count(store_sales.ticket_number) as tickets_in_bucket, --ticket_qty order by bucket;`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "aggregate"

  LINE 19:     INVALID_REFERENCE_BUG_<Cannot render aggregate
  local.tickets_in_bucket in CTE wakeful: source_map...
                                                    ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      "store_sales_store_sales"."ss_ticket_number" as
  "store_sales_ticket_number",
      sum("store_sales_store_sales"."ss_quantity") as "ticket_qty"
  FROM
      "store_sales" as "store_sales_store_sales"
  GROUP BY
      1)
  SELECT
      CASE
          WHEN "quizzical"."ticket_qty" BETWEEN 1 AND 20 THEN 1
          WHEN "quizzical"."ticket_qty" BETWEEN 21 AND 40 THEN 2
          WHEN "quizzical"."ticket_qty" BETWEEN 41 AND 60 THEN 3
          WHEN "quizzical"."ticket_qty" BETWEEN 61 AND 80 THEN 4
          WHEN "quizzical"."ticket_qty" BETWEEN 81 AND 100 THEN 5
          END as "bucket",
      INVALID_REFERENCE_BUG_<Cannot render aggregate local.tickets_in_bucket in
  CTE wakeful: source_map miss and CTE grain Grain<local.bucket,local.ticket_qty>
  != aggregate by-grain <['local.ticket_qty']>> as "tickets_in_bucket"
  FROM
      "quizzical"
  ORDER BY
      "bucket" asc]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw.store_sales:store_sales auto ticket_qty <- sum(store_sales.quantity) by store_sales.ticket_number;
auto bucket <- case when ticket_q…then 4 when ticket_qty between 81 and 100 then 5 end;
auto cnt <- count(store_sales.ticket_number) by ticket_qty;
select bucket, cnt order by bucket;`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "aggregate"

  LINE 20:     INVALID_REFERENCE_BUG_<Cannot render aggregate local.cnt in CTE
  wakeful: source_map miss and...
                                                    ^
  [SQL:
  WITH
  quizzical as (
  SELECT
      "store_sales_store_sales"."ss_ticket_number" as
  "store_sales_ticket_number",
      sum("store_sales_store_sales"."ss_quantity") as "ticket_qty"
  FROM
      "store_sales" as "store_sales_store_sales"
  GROUP BY
      1),
  wakeful as (
  SELECT
      CASE
          WHEN "quizzical"."ticket_qty" BETWEEN 1 AND 20 THEN 1
          WHEN "qui
  …
  AND 60 THEN 3
          WHEN "quizzical"."ticket_qty" BETWEEN 61 AND 80 THEN 4
          WHEN "quizzical"."ticket_qty" BETWEEN 81 AND 100 THEN 5
          END as "bucket",
      INVALID_REFERENCE_BUG_<Cannot render aggregate local.cnt in CTE wakeful:
  source_map miss and CTE grain Grain<local.bucket,local.ticket_qty> != aggregate
  by-grain <['local.ticket_qty']>> as "cnt"
  FROM
      "quizzical")
  SELECT
      "wakeful"."bucket" as "bucket",
      "wakeful"."cnt" as "cnt"
  FROM
      "wakeful"
  GROUP BY
      1,
      2
  ORDER BY
      "wakeful"."bucket" asc]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 501 (char 500). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query10.preql`

  ```text
  Duplicate select output for local.customer_count; Line: 8
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/catalog_sales:catalog_sales --import raw/web_sales:web_sales select store_sales.item.brand_id a…_val, count(catalog_sales.quantity) as cs_cnt, sum(web_sales.quantity * web_sales.list_price) as ws_val, count(web_sales.quantity) as ws_cnt limit 5;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ws_val', 'local.ws_cnt', 'local.cs_val', 'local.cs_cnt',
  'local.ss_cnt'} out of  with found {'store_sales.item.category_id',
  'store_sales.item.class_id', 'store_sales.item.brand_id', 'local.ss_val',
  'local.ss_cnt'}
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/catalog_sales:catalog_sales --import raw/web_sales:web_sales select store_sales.item.brand_id a…_id as catid, sum(store_sales.quantity * store_sales.list_price) as ss_val, sum(catalog_sales.quantity * catalog_sales.list_price) as cs_val limit 5;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ss_val', 'local.cs_val'} out of  with found
  {'store_sales.item.brand_id', 'local.ss_val', 'store_sales.item.category_id',
  'store_sales.item.class_id'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 101 (char 100). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr --import raw.catalog_sales:cs merge sr.item.item_sk into ~ss.item.item_sk; merge sr.cus…ustomer.customer_sk as csk, ss.item.item_sk as isk, ss.ticket_number as tn, count(sr.ticket_number) as sr_cnt, count(cs.quantity) as cs_cnt limit 10;`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "ss_item_item" not found!
  Candidate tables: "ss_date_dim_date_dim", "ss_customer_customer",
  "sr_date_dim_date_dim", "ss_store_sales", "sr_store_returns"

  LINE 60:
  ...falutin"."ss_item_item_sk","sr_store_returns"."sr_item_sk","ss_item_item"."i
  tem_sk","ss_store_sales"."ss_item_sk")...
                                                                         ^
  [SQL:
  WITH
  cooperative as (
  SELECT
      "sr_store_returns"."sr_customer_sk" as "ss_customer_customer_sk",
      "sr_store_returns"."sr_item_sk" as "ss_item_item_sk",
      coalesce("sr
  …
  rned"."ss_ticket_number")
  SELECT
      "young"."ss_customer_customer_sk" as "csk",
      coalesce("abhorrent"."ss_item_item_sk","young"."ss_item_item_sk") as "isk",
      coalesce("abhorrent"."ss_ticket_number","young"."ss_ticket_number") as
  "tn",
      coalesce("young"."sr_cnt",0) as "sr_cnt",
      coalesce("abhorrent"."cs_cnt",0) as "cs_cnt"
  FROM
      "young"
      FULL JOIN "abhorrent" on "young"."ss_item_item_sk" =
  "abhorrent"."ss_item_item_sk" AND "young"."ss_ticket_number" is not distinct
  from "abhorrent"."ss_ticket_number"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query19.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```

### `syntax-missing-alias`

- `trilogy run - --import raw.date_dim:dd`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...as dd; select min(dd.week_seq) ??? , max(dd.week_seq), min(dd.yea...
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns --import raw.catalog_sales:catalog_sales --import raw.catalog_retur…ns --import raw.web_sales:web_sales --import raw.web_returns:web_returns select store_sales.store.store_id, sum(store_sales.ext_sales_price) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...(store_sales.ext_sales_price) ??? limit 5;
  ```
- `trilogy run --import raw.catalog_returns:catalog_returns select catalog_returns.catalog_page.catalog_page_sk, sum(catalog_returns.net_loss) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...sum(catalog_returns.net_loss) ??? limit 5;
  ```
- `trilogy run --import raw.store_sales:store_sales select substring('31904', 1, 2); duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...elect substring('31904', 1, 2) ??? ;
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct substring('24128', 1, 2) as f2, unnest(split('24128,76232,65084', ',')) as zip; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? substring('24128', 1, 2) as f2...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct substring(unnest(split('24128,76232,65084', ',')), 1, 2) as f2; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? substring(unnest(split('24128,...
  ```
- `trilogy run --import raw.store_sales:store_sales SELECT distinct substring(unnest(split('24128,76232,65084', ',')), 1, 2) as f2; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; SELECT distinct ??? substring(unnest(split('24128,...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct substring(store_sales.customer.customer_address.zip, 1, 2) as f2 where store_sales.customer.preferred_cust_flag = 'Y' and count(store_sales.customer.customer_sk) by store_sales.customer.customer_address.zip > 10; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? substring(store_sales.customer...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct substring(store_sales.customer.customer_address.zip, 1, 2) as f2 where store_sales.customer.preferred_cust_flag = 'Y' and count(store_sales.customer.customer_sk) by store_sales.customer.customer_address.zip > 10; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? substring(store_sales.customer...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct substring(unnest(split('24128,76232,65084', ',')), 1, 2) as f2; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? substring(unnest(split('24128,...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct substring(unnest(split('24128,76232,65084', ',')), 1, 2) as f2; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? substring(unnest(split('24128,...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct unnest(split('24128,76232,65084', ',')) as zip; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? unnest(split('24128,76232,6508...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct substring(store_sales.customer.customer_address.zip, 1, 2) as pfx where store_sales.customer.preferred_cust_flag = 'Y' and count(store_sales.customer.customer_sk) by store_sales.customer.customer_address.zip > 10; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? substring(store_sales.customer...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct substring(store_sales.customer.customer_address.zip, 1, 2) as pfx where store_sales.customer.preferred_cust_flag = 'Y' and count(store_sales.customer.customer_sk) by store_sales.customer.customer_address.zip > 10; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? substring(store_sales.customer...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct substring(store_sales.customer.customer_address.zip, 1, 2) as pfx where store_sales.customer.preferred_cust_flag = 'Y' and count(store_sales.customer.customer_sk) by store_sales.customer.customer_address.zip > 10; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? substring(store_sales.customer...
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct substring(store_sales.store.zip, 1, 2) as pfx; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   store_sales; select distinct ??? substring(store_sales.store.zi...
  ```
- `trilogy run --import raw.store_sales:store_sales auto pref_zip_cnt <- count(store_sales.customer.customer_sk ? store_sales.customer.preferred_cust_flag = 'Y'…ustomer_address.zip; auto gt10 <- store_sales.customer.customer_address.zip ? pref_zip_cnt > 10; select distinct substring(gt10, 1, 2) as pfx; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...zip_cnt > 10; select distinct ??? substring(gt10, 1, 2) as pfx;
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.quantity, store_sales.sales_price, store_sales.ext_sales_price, store_sales.ext_wholesale…on_status = 'Advanced Degree' AND store_sales.sales_price between 100 and 150 AND store_sales.customer.household_demographics.dep_count = 3 limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ress.state IN ('VA','TX','MS') ??? , store_sales.customer.custome...
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/catalog_sales:catalog_sales --import raw/web_sales:web_sales select store_sales.date_dim.year, store_sales.date_dim.month_seq, count(store_sales.quantity) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ..., count(store_sales.quantity) ??? limit 5;
  ```
- `trilogy file write debug8.preql --content import raw.catalog_sales as cs;
# Check: warehouse_sk has NULLs - does the condition count_distinct(...)>1 work wit…inct counts NULL as a distinct value

# First, just get all distinct warehouse_sk values
select distinct cs.warehouse.warehouse_sk as wh_sk
limit 10;`

  ```text
  refused to write 'debug8.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...nct cs.warehouse.warehouse_sk ??? as wh_sk limit 10;

  Write stats: received 877 chars / 877 bytes; tail: …'select distinct
  cs.warehouse.warehouse_sk as wh_sk\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write debug15.preql --content import raw.catalog_sales as cs;
# There are orders with NULL warehouse_sk in the full dataset
# But in the filtere…
# warehouse_sk values. Let's just check what warehouse_sk values exist in the FULL dataset
select distinct cs.warehouse.warehouse_sk as wh
limit 10;`

  ```text
  refused to write 'debug15.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...nct cs.warehouse.warehouse_sk ??? as wh limit 10;

  Write stats: received 1329 chars / 1329 bytes; tail: …'et\\nselect distinct
  cs.warehouse.warehouse_sk as wh\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr --import raw.catalog_sales:cs select count(ss.ticket_number), count(sr.ticket_number), count(cs.order_number), count(distinct ss.item.item_id), count(distinct ss.store.store_sk) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...select count(ss.ticket_number) ??? , count(sr.ticket_number), cou...
  ```
- `trilogy file write query18.preql --content import raw.catalog_sales as catalog_sales;

# For catalog sales in 1998, restricted to:
# - billing customer-demog…es.bill_customer.customer_address.county nulls first,
    catalog_sales.item.item_id nulls first,
    g_country,
    g_state,
    g_county
limit 100;`

  ```text
  refused to write 'query18.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...es.quantity)::numeric(12,2) by ??? rollup item_code, catalog_sal...

  Write stats: received 3214 chars / 3214 bytes; tail: …'s first,\\n
  g_country,\\n    g_state,\\n    g_county\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `undefined-concept`

- `trilogy run query03.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date_dim.moy. Suggestions: ['store_sales.date_dim.moy',
  'store_sales.date_dim.qoy']")
  ```
- `trilogy run --import raw.web_returns:web_returns select web_returns.web_site.site_id, sum(web_returns.return_amt ? web_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as total_returns limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  web_returns.web_site.site_id. Suggestions: ['web_returns.item.item_id',
  'web_returns.item.item_desc', 'web_returns.item.size']")
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns merge store_returns.item.id into ~store_sales.item.id; merge store_… '2000-09-06'::date) - sum(store_returns.net_loss ? store_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as profit limit 5;`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_returns.item.id. Suggestions: ['store_returns.item.size',
  'store_returns.item.item_id', 'store_returns.item.units']")
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
  store_sales.item.is_overpriced. Suggestions: ['store_sales.item.size',
  'store_sales.item.item_id', 'store_sales.list_price']")
  ```
- `trilogy run --import raw.store_sales:store_sales select count(customer.customer_address.zip) as cnt where customer.preferred_cust_flag = 'Y' and count(customer.customer_sk) by customer.customer_address.zip > 10; duckdb`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  customer.customer_address.zip. Suggestions:
  ['store_sales.customer.customer_address.zip',
  'store_sales.customer_address.zip',
  'store_sales.customer.customer_address.city']")
  ```
- `trilogy run --import raw.store_sales:store_sales select count(ticket_number) as all_tickets;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  local.ticket_number. Suggestions: ['store_sales.ticket_number']")
  ```
- `trilogy run query19.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.manager_id. Suggestions: ['store_sales.item.manager_id']")
  ```
- `trilogy run -`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  category.')
  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.precl`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.precl' does not exist.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns merge store_returns.item.id into ~store_sales.item.id; merge store_… '2000-09-06'::date) - sum(store_returns.net_loss ? store_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as profit limit 5;`

  ```text
  'merge store_returns.ticket_number into ~store_sales.ticket_number;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run catalog_sales --import raw.catalog_sales:catalog_sales --import raw.catalog_returns:catalog_returns merge catalog_returns.item.item_sk into ~cata…09-06'::date) - sum(catalog_returns.net_loss ? catalog_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date)) as profit limit 100;`

  ```text
  'merge catalog_returns.item.item_sk into ~catalog_sales.item.item_sk; merge catalog_returns.order_number into ~catalog_sales.order_number; select 'Catalog' as channel, concat('catalog_page_', catalog_sales.catalog_page.catalog_page_id) as outlet, sum(catalog_sales.ext_sales_price ? catalog_sales.sold_date.date between '2000-08-23'::date and '2000-09-06'::date) as total_sales, sum(catalog_returns.return_amount ? catalog_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as total_returns, (sum(catalog_sales.net_profit ? catalog_sales.sold_date.date between '2000-08-23'::date and '2000-09-06'::date) - sum(catalog_returns.net_loss ? catalog_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date)) as profit limit 100;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.precl --show imports`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.precl' does not exist.
  ```
- `trilogy file write --escapes --content parameter zips string;

import raw.store_sales as store_sales;
import raw.customer as customer;
import raw.customer_ad…fixes)

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  Missing argument 'PATH'.
  ```
- `trilogy read_file raw/household_demographics.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/database_relationship.preql`

  ```text
  Invalid value for 'PATH': File 'raw/database_relationship.preql' does not exist.
  ```
- `trilogy read_file raw/catalog_sales.preql`

  ```text
  No such command 'read_file'.
  ```

### `join-resolution`

- `trilogy run --import raw/store_sales:store_sales --import raw/catalog_sales:catalog_sales --import raw/web_sales:web_sales select sum(store_sales.quantity * …sales.quantity) + count(catalog_sales.quantity) + count(web_sales.quantity) as cnt_all where store_sales.date_dim.year between 1999 and 2001 limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.total_all<Purpose.METRIC>Derivation.BASIC>',
  'local.cnt_all<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr --import raw.catalog_sales:cs merge sr.item.item_sk into ~ss.item.item_sk; merge sr.cus…01 and cs.sold_date.qoy in (1,2,3) select count(ss.ticket_number) as ss_cnt, count(sr.ticket_number) as sr_cnt, count(cs.quantity) as cs_cnt limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.ss_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.sr_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr --import raw.catalog_sales:cs merge sr.item.item_sk into ~ss.item.item_sk; merge sr.cus…01 and cs.sold_date.qoy in (1,2,3) select count(ss.ticket_number) as ss_cnt, count(sr.ticket_number) as sr_cnt, count(cs.quantity) as cs_cnt limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.ss_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.sr_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr --import raw.catalog_sales:cs merge sr.item.item_sk into ~ss.item.item_sk; merge sr.cus…fy_year=2001 and ss.date_dim.qoy=1 select count(ss.ticket_number) as ss_cnt, count(sr.ticket_number) as sr_cnt, count(cs.quantity) as cs_cnt limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.ss_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.sr_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr --import raw.catalog_sales:cs merge sr.item.item_sk into ~ss.item.item_sk; merge sr.cus…001 and sr.date_dim.qoy in (1,2,3) select count(ss.ticket_number) as ss_cnt, count(sr.ticket_number) as sr_cnt, count(cs.quantity) as cs_cnt limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.ss_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.sr_cnt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
