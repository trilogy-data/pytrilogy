# Trilogy failure analysis — 20260530-201159

- Run `20260530-201159` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 550 | failed: 85 (15%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 34 | 40% |
| `other` | 30 | 35% |
| `undefined-concept` | 10 | 12% |
| `syntax-missing-alias` | 6 | 7% |
| `join-resolution` | 3 | 4% |
| `cli-misuse` | 1 | 1% |
| `type-error` | 1 | 1% |

## Detail

### `syntax-parse`

- `trilogy run --import raw.web_sales:ws --import raw.catalog_sales:cs merge ws.sold_date.date_sk into ~cs.sold_date.date_sk;

auto daily_2001 <- sum(ws.ext_sal… ws.sold_date.dow, daily_2001, daily_2002
where ws.sold_date.week_seq between 5270 and 5272
order by ws.sold_date.week_seq asc, ws.sold_date.dow asc;`

  ```text
  Syntax [211]: Expression in `by` clause must be wrapped in
  parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work
  without parens, but any function call, cast, or other expression needs them.
  Location:
  ...ice ? cs.sold_date.year=2002) ??? by (ws.sold_date.week_seq - 53...
  ```
- `trilogy run --import raw.web_sales:ws --import raw.catalog_sales:cs merge ws.sold_date.date_sk into ~cs.sold_date.date_sk;

auto daily_2001 <- sum(ws.ext_sal… ws.sold_date.dow, daily_2001, daily_2002
where ws.sold_date.week_seq between 5270 and 5272
order by ws.sold_date.week_seq asc, ws.sold_date.dow asc;`

  ```text
  Syntax [211]: Expression in `by` clause must be wrapped in
  parens — write `by (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work
  without parens, but any function call, cast, or other expression needs them.
  Location:
  ...ice ? cs.sold_date.year=2002) ??? by (ws.sold_date.week_seq - 53...
  ```
- `trilogy file write query04.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…d as customer_code,
    store_sales.customer.first_name,
    store_sales.customer.last_name,
    store_sales.customer.preferred_cust_flag
limit 100;
`

  ```text
  …
  t to customers who have positive year
  totals in 2001 across all three channels\nwhere\n    store_year_total ?
  store_sales.date_dim.year = 2001 > 0\n    and catalog_year_total ?
  catalog_sales.sold_date.year = 2001 > 0\n    and web_year_total ?
  web_sales.sold_date.year = 2001 > 0\n\nselect\n
  store_sales.customer.customer_id as customer_code,\n
  store_sales.customer.first_name,\n    store_sales.customer.last_name,\n
  store_sales.customer.preferred_cust_flag\nlimit 100;\n') at line 15, column 19.
  Expected one of:
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'chan_value')]

  Location:
  ...r_total <-      sum(chan_value ??? (store_sales.ext_list_price, s...

  Write stats: received 1778 chars / 1778 bytes; tail: …'me,\\n
  store_sales.customer.preferred_cust_flag\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…utlet) by rollup all_rows.channel, all_rows.outlet as g_outlet
order by 
    all_rows.channel nulls first,
    all_rows.outlet nulls first
limit 100;`

  ```text
  …
  outlet as total_sales,\n
  sum(all_rows.total_returns) by rollup all_rows.channel, all_rows.outlet as
  total_returns,\n    sum(all_rows.total_profit) by rollup all_rows.channel,
  all_rows.outlet as total_profit,\n    grouping(all_rows.channel) by rollup
  all_rows.channel, all_rows.outlet as g_channel,\n    grouping(all_rows.outlet)
  by rollup all_rows.channel, all_rows.outlet as g_outlet\norder by \n
  all_rows.channel nulls first,\n    all_rows.outlet nulls first\nlimit 100;") at
  line 40, column 5.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...auto store_row <- select      ??? 'store' as channel,     store_...

  Write stats: received 4133 chars / 4133 bytes; tail: …'nnel nulls first,\\n
  all_rows.outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run select union(select 'a' as x, 1 as y, 2 as z, 3 as w, select 'b' as x, 4 as y, 5 as z, 6 as w);`

  ```text
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside
  parens)? Trilogy does not support subqueries — joins are auto-resolved from
  dotted paths. To filter on a value that lives on a related dimension, reference
  its dot-path directly. Example: instead of `where ss.store_id in (select
  store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  select union ??? (select 'a' as x, 1 as y, 2 as...
  ```
- `trilogy run --import raw.store_returns:store_returns select 'store' as channel, concat('store_', store_returns.store.store_id) as outlet, 0 as total_sales, s…otal_returns, -sum(store_returns.net_loss ? store_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as total_profit limit 100;`

  ```text
  --> 2:232
    |
  2 | select 'store' as channel, concat('store_', store_returns.store.store_id)
  as outlet, 0 as total_sales, sum(store_returns.return_amt ?
  store_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date)
  as total_returns, -sum(store_returns.net_loss ? store_returns.date_dim.date
  between '2000-08-23'::date and '2000-09-06'::date) as total_profit limit 100;
    |
  ^---
    |
    = expected select_item, limit, order_by, where, or having
  Location:
  ...-06'::date) as total_returns, ??? -sum(store_returns.net_loss ?
  ```
- `trilogy run --import raw.store_sales:store_sales select concat('x_', store_sales.store.store_id) as outlet, sum(store_sales.ext_sales_price) by rollup concat('x_', store_sales.store.store_id) as total_sales limit 10;`

  ```text
  --> 2:109
    |
  2 | select concat('x_', store_sales.store.store_id) as outlet,
  sum(store_sales.ext_sales_price) by rollup concat('x_',
  store_sales.store.store_id) as total_sales limit 10;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ..._sales_price) by rollup concat ??? ('x_', store_sales.store.store...
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Step 1: Declare the parameter as a list of ZIP codes
auto zips_list <- split(zips, ',');…ts > 10
  )
select
  store_sales.store.store_name,
  sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 8, column 128.
  Expected one of:
          * _TERMINATOR

  Location:
  ...customer.customer_address.zip ??? as pref_cust_count;  # Step 3:...

  Write stats: received 1465 chars / 1465 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.store_sales as store_sales;

# Auto: count preferred customers per ZIP
auto pre…ip_prefixes
select
  store_sales.store.store_name,
  sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 86.
  Expected one of:
          * _TERMINATOR

  Location:
  ...r.customer_address.zip, 1, 2) ??? where pref_cust_by_zip > 10;

  Write stats: received 903 chars / 903 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.year, count(store_sales.ticket_number) as cnt where store_sales.date_dim.year = 1998 group by store_sales.date_dim.year limit 5;`

  ```text
  --> 2:114
    |
  2 | select store_sales.date_dim.year, count(store_sales.ticket_number) as cnt
  where store_sales.date_dim.year = 1998 group by store_sales.date_dim.year limit
  5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...re_sales.date_dim.year = 1998 ??? group by store_sales.date_dim....
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Total quantity per ticket (sum of line quantities per ticket_number)
aut…_80 > 10097, avg_disc_61_80, avg_net_61_80) as bucket_61_80,
    case(cnt_81_100 > 165306, avg_disc_81_100, avg_net_81_100) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 28, column 60.
  Expected one of:
          * WHEN

  Location:
  ...vg_disc_01_20, avg_net_01_20) ??? as bucket_1_20,     case(cnt_2...

  Write stats: received 1950 chars / 1950 bytes; tail: …', avg_disc_81_100,
  avg_net_81_100) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Total quantity per ticket
auto ticket_qty <- sum(store_sales.quantity) b…1_40_val as bucket_21_40,
    bucket_41_60_val as bucket_41_60,
    bucket_61_80_val as bucket_61_80,
    bucket_81_100_val as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 29, column 79.
  Expected one of:
          * WHEN

  Location:
  ...avg_disc_01_20, avg_net_01_20) ??? ; auto bucket_21_40_val <- cas...

  Write stats: received 2233 chars / 2233 bytes; tail: …'ucket_61_80,\\n
  bucket_81_100_val as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Total quantity per ticket
auto ticket_qty <- sum(store_sales.quantity) b…1_40_val as bucket_21_40,
    bucket_41_60_val as bucket_41_60,
    bucket_61_80_val as bucket_61_80,
    bucket_81_100_val as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'cnt_01_20 ') at line 29, column 35.
  Expected one of:
          * COMMA
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
   bucket_1_20_val <- case(when ??? cnt_01_20 > 74129 then avg_dis...

  Write stats: received 2311 chars / 2311 bytes; tail: …'ucket_61_80,\\n
  bucket_81_100_val as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Total quantity per ticket
auto ticket_qty <- sum(store_sales.quantity) b…_80 > 10097, avg_disc_61_80, avg_net_61_80) as bucket_61_80,
    case(cnt_81_100 > 165306, avg_disc_81_100, avg_net_81_100) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 28, column 60.
  Expected one of:
          * WHEN

  Location:
  ...vg_disc_01_20, avg_net_01_20) ??? as bucket_1_20,     case(cnt_2...

  Write stats: received 1907 chars / 1907 bytes; tail: …', avg_disc_81_100,
  avg_net_81_100) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;
select
    case(when store_sales.quantity > 5 then store_sales.ext_discount_amt else store_sales.net_paid end) as test_col
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'store_sales.quantity ') at line 3,
  column 15.
  Expected one of:
          * COMMA
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ...e_sales; select     case(when ??? store_sales.quantity > 5 then

  Write stats: received 170 chars / 170 bytes; tail: …'ount_amt else
  store_sales.net_paid end) as test_col\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;
select
    case(store_sales.quantity > 5, store_sales.ext_discount_amt, store_sales.net_paid) as test_col
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 3, column 88.
  Expected one of:
          * WHEN

  Location:
  ...nt_amt, store_sales.net_paid) ??? as test_col limit 1;

  Write stats: received 153 chars / 153 bytes; tail: …'ext_discount_amt,
  store_sales.net_paid) as test_col\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;
select
    case(store_sales.quantity, when store_sales.quantity > 5, store_sales.ext_discount_amt, store_sales.net_paid) as test_col
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'store_sales.quantity ') at line 3,
  column 37.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ...se(store_sales.quantity, when ??? store_sales.quantity > 5, stor...

  Write stats: received 180 chars / 180 bytes; tail: …'ext_discount_amt,
  store_sales.net_paid) as test_col\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;
select
    case(when store_sales then store_sales.quantity > 5, store_sales.ext_discount_amt, store_sales.net_paid) as test_col
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'store_sales ') at line 3, column
  15.
  Expected one of:
          * RPAR
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'when')]

  Location:
  ...e_sales; select     case(when ??? store_sales then store_sales.q...

  Write stats: received 175 chars / 175 bytes; tail: …'ext_discount_amt,
  store_sales.net_paid) as test_col\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 21, column 31.
  Expected one of:
          * SELECT

  Location:
    and web_rev_2001 is not null ??? ;  # Growth rates: treat zer...

  Write stats: received 1948 chars / 1948 bytes; tail: …'ls first,\r\\n
  preferred_cust_flag nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 23, column 46.
  Expected one of:
          * SELECT

  Location:
  ..._2001 > 0 and web_rev_2001 > 0 ??? ;  # Web revenue grew faster...

  Write stats: received 1763 chars / 1763 bytes; tail: …'ls first,\r\\n
  preferred_cust_flag nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 24, column 76.
  Expected one of:
          * SELECT

  Location:
   and web_growth > store_growth ??? ;  select     store_sales.c...

  Write stats: received 1762 chars / 1762 bytes; tail: …'ls first,\r\\n
  preferred_cust_flag nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 19, column 76.
  Expected one of:
          * SELECT

  Location:
   and web_growth > store_growth ??? ;  select     store_sales.c...

  Write stats: received 1442 chars / 1442 bytes; tail: …'ls first,\r\\n
  preferred_cust_flag nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 16, column 46.
  Expected one of:
          * SELECT

  Location:
  ..._2001 > 0 and web_rev_2001 > 0 ??? ;  select     store_sales.c...

  Write stats: received 1302 chars / 1302 bytes; tail: …'ls first,\r\\n
  preferred_cust_flag nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 25.
  Expected one of:
          * SELECT

  Location:
  ...k;  where store_rev_2001 > 0 ??? ;  select     store_sales.c...

  Write stats: received 441 chars / 441 bytes; tail: …'es.customer.customer_id as
  billing_customer_code\r\\nlimit 5;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 15, column 39.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._faster <- store_rev_2001 > 0 ??? and web_rev_2001 > 0     and

  Write stats: received 1502 chars / 1502 bytes; tail: …'ls first,\r\\n
  preferred_cust_flag nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as ws;

# Filter to relevant categories and date range
where ws.item.category in ('Sports', '…class as pct_of_class_sales
order by
    ws.item.category,
    ws.item.class,
    ws.item.item_id,
    ws.item.item_desc,
    pct_of_class
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...em total extended sales price ??? auto item_total_ext <- sum(ws....

  Write stats: received 840 chars / 840 bytes; tail: …'.item_id,\\n
  ws.item.item_desc,\\n    pct_of_class\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as ws;

# Filter to relevant categories and date range
where ws.item.category in ('Sports', '…class as pct_of_class_sales
order by
    ws.item.category,
    ws.item.class,
    ws.item.item_id,
    ws.item.item_desc,
    pct_of_class
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...em total extended sales price ??? auto item_total_ext <- sum(ws....

  Write stats: received 840 chars / 840 bytes; tail: …'.item_id,\\n
  ws.item.item_desc,\\n    pct_of_class\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as ws;

# Filter to relevant categories and date range by using sold_date.date
where ws.item.…class as pct_of_class_sales
order by
    ws.item.category,
    ws.item.class,
    ws.item.item_id,
    ws.item.item_desc,
    pct_of_class
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...em total extended sales price ??? auto item_total_ext <- sum(ws....

  Write stats: received 894 chars / 894 bytes; tail: …'.item_id,\\n
  ws.item.item_desc,\\n    pct_of_class\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw.store_sales as store_sales;

where store_sales.date_dim.year = 2001
and store_sales.sales_price between 100 and 150
limit 10
select
  store_sales.sales_price
limit 5;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LIMIT', 'limit') at line 5, column 1.
  Expected one of:
          * SELECT

  Location:
  ...les_price between 100 and 150 ??? limit 10 select   store_sales....

  Write stats: received 177 chars / 177 bytes; tail: …'0 and 150\\nlimit
  10\\nselect\\n  store_sales.sales_price\\nlimit 5;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\r\n\r\n') at line 61, column
  1.
  Expected one of:
          * ORDER
          * _TERMINATOR
          * LIMIT
          * MERGE
          * HAVING
  Previous tokens: [Token('IDENTIFIER', 'overall_avg_sale')]

  Location:
  ...v_sales > overall_avg_sale  ??? union  select     'Catalog

  Write stats: received 5134 chars / 5134 bytes; tail: …' class_id nulls first,
  category_id nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql`

  ```text
  …
  el, brand_id, class_id, category_id as
  g_ch,\r\n    grouping(brand_id) by rollup channel, brand_id, class_id,
  category_id as g_br,\r\n    grouping(class_id) by rollup channel, brand_id,
  class_id, category_id as g_cl,\r\n    grouping(category_id) by rollup channel,
  brand_id, class_id, category_id as g_ca,\r\n    g_ch + g_br + g_cl + g_ca as
  level\r\norder by level asc, channel nulls first, brand_id nulls first,
  class_id nulls first, category_id nulls first\r\nlimit 100;\r\n") at line 43,
  column 5.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
   auto ss_data <- select     ??? 'Store Sales' as channel,

  Write stats: received 4708 chars / 4708 bytes; tail: …' class_id nulls first,
  category_id nulls first\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql`

  ```text
  …
  \n    ss.item.class_id,\r\n    ss.item.category_id,\r\n
  sum(ss.quantity * ss.list_price ? ss.date_dim.year = 2001 and ss.date_dim.moy =
  11) as nov_sales,\r\n    count(ss.ticket_number ? ss.date_dim.year = 2001 and
  ss.date_dim.moy = 11) as nov_cnt\r\nwhere ss.date_dim.year = 2001 and
  ss.date_dim.moy = 11\r\ngroup by channel, ss.item.brand_id, ss.item.class_id,
  ss.item.category_id\r\norder by channel, ss.item.brand_id, ss.item.class_id,
  ss.item.category_id\r\nlimit 100;\r\n") at line 27, column 5.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...ory) auto ss1 <- select     ??? 'Store Sales' as channel,

  Write stats: received 1841 chars / 1841 bytes; tail: …'rand_id,
  ss.item.class_id, ss.item.category_id\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write test_union.preql`

  ```text
  refused to write 'test_union.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...s ss;  auto ss_data <- union ??? (     select 'Store Sales' as...

  Write stats: received 323 chars / 323 bytes; tail: …'here ss_nov_sale >
  0\r\\n);\r\\n\r\\nselect * from ss_data limit 5;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.catalog_sales:cs select max(warehouse_count) as max_warehouses from (select count_distinct(cs.warehouse.warehouse_sk) as warehouse_count by cs.order_number);`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...ouse_count) as max_warehouses ??? from (select count_distinct(cs...
  ```

### `other`

- `trilogy run query01.preql`

  ```text
  HAVING references 'local.cust_store_total',
  'local.store_avg', which are not in the SELECT projection (line 6). Add them to
  SELECT, each prefixed with `--` so they stay out of the output rows — keep your
  HAVING as-is:
      select <your existing columns>, --local.cust_store_total, --local.store_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw.date_dim:date_dim select date_dim.week_seq, date_dim.year order by date_dim.date asc limit 20;`

  ```text
  ORDER BY references 'date_dim.date', which is not in the
  SELECT projection (line 2). Add it to SELECT to sort by it — prefix with `--`
  to keep it out of the output rows, e.g. `select ..., --date_dim.date order by
  date_dim.date asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw.web_sales:ws --import raw.catalog_sales:cs merge ws.sold_date.date_sk into ~cs.sold_date.date_sk;

select ws.sold_date.week_seq,
   …old_date.week_seq - 53), ws.sold_date.dow) as mon_02
where ws.sold_date.week_seq between 5270 and 5272 and ws.sold_date.dow between 0 and 1
limit 20;`

  ```text
  Recursion error building concept local.sun_02 with grain
  Grain<cs.net_paid_inc_ship_tax,cs.sold_date.date_sk,ws.net_paid_inc_ship> and
  lineage add(sum(<Filter: ref:ws.ext_sales_price where ref:ws.sold_date.dow = 0
  and ref:ws.sold_date.year = 2002>)<abstract>,sum(<Filter:
  ref:cs.ext_sales_price where ref:cs.sold_date.dow = 0 and ref:cs.sold_date.year
  = 2002>)<['ref:local._virt_paren_5901083230450481', 'ref:ws.sold_date.dow']>).
  This is likely due to a circular reference.
  ```
- `trilogy run --import raw.web_sales:ws --import raw.catalog_sales:cs merge ws.sold_date.date_sk into ~cs.sold_date.date_sk;

auto ws_01 <- ws.sold_date.week_s… + sum(cs.ext_sales_price ? cs.sold_date.dow=1) by (ws_02 - 53);

select ws_01, mon_01, mon_02
having ws_01 between 5270 and 5272
order by ws_01 asc;`

  ```text
  Recursion error building concept local.mon_01 with grain
  Grain<ws.sold_date.date_sk> and lineage add(sum(<Filter: ref:ws.ext_sales_price
  where ref:ws.sold_date.dow = 1>)<abstract>,sum(<Filter: ref:cs.ext_sales_price
  where ref:cs.sold_date.dow = 1>)<['ref:local.ws_01']>). This is likely due to a
  circular reference.
  ```
- `trilogy run query04.preql duckdb`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql duckdb`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql duckdb`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql duckdb`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.Error) Parameter not supported in ORDER BY clause
  [SQL:
  WITH
  cooperative as (
  SELECT
      ('store_' || "store_sales_store_store"."s_store_id") as "outlet",
      $1 as "channel",
      sum("store_returns_store_returns"."sr_return_amt") as "total_returns",
      sum("store_sales_store_sales"."ss_ext_sales_price") as "total_sales",
      sum("store_sales_store_sales"."ss_net_profit") -
  sum("store_returns_store_returns"."sr_net_loss") as "total_profit"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "store_returns" as "store_returns_store_returns" on
  "store_sales_store_sal
  …
  tore_sales_date_dim_date_dim"."d_date" BETWEEN date
  '2000-08-23' AND date '2000-09-06'

  GROUP BY
      1,
      "store_sales_store_store"."s_store_id")
  SELECT
      "cooperative"."channel" as "channel",
      "cooperative"."outlet" as "outlet",
      "cooperative"."total_sales" as "total_sales",
      "cooperative"."total_returns" as "total_returns",
      "cooperative"."total_profit" as "total_profit"
  FROM
      "cooperative"
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      $1 asc,
      "cooperative"."outlet" asc nulls first
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/dbapi)
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_returns_amt', 'local.store_sales_amt'} out of  with
  found {'local.store_sales_profit', 'store_sales.store.store_id',
  'store_returns.store.store_id', 'local.store_returns_amt',
  'local.store_sales_amt', 'local.store_returns_loss'}
  ```
- `trilogy run query05.preql`

  ```text
  (_duckdb.NotImplementedException) Not implemented Error:
  Unable to transform python value of type '<enum 'MagicConstants'>' to DuckDB
  LogicalType
  [SQL:
  WITH
  cool as (
  SELECT
      "wr_web_returns"."wr_return_amt" as
  "_virt_filter_return_amt_2222098767211036"
  FROM
      "item" as "wr_item_item"
      LEFT OUTER JOIN "web_returns" as "wr_web_returns" on
  "wr_item_item"."i_item_sk" = "wr_web_returns"."wr_item_sk"
      RIGHT OUTER JOIN "date_dim" as "wr_date_dim_date_dim" on
  "wr_web_returns"."wr_returned_date_sk" = "wr_date_dim_date_dim"."d_date_sk"
  WHERE
      "wr_date_dim_date_dim"."d_date" BETWEE
  …
    sum("cool"."_virt_filter_return_amt_2222098767211036") as "total_returns"
  FROM
      "cool"),
  vast as (
  SELECT
      0 - sum("courageous"."_virt_filter_net_loss_5606254344611810") as
  "total_profit"
  FROM
      "courageous")
  SELECT
      "quizzical"."channel" as "channel",
      "quizzical"."outlet" as "outlet",
      "quizzical"."total_sales" as "total_sales",
      "elated"."total_returns" as "total_returns",
      "vast"."total_profit" as "total_profit"
  FROM
      "quizzical"
      FULL JOIN "vast" on 1=1
      FULL JOIN "elated" on 1=1
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/tw8g)
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.InvalidInputException) Invalid Input Error: missing
  ): (^|,)(31
  [SQL:
  WITH
  wakeful as (
  SELECT
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip"
  FROM
      "customer_address" as
  "store_sales_customer_customer_address_customer_address"
      INNER JOIN "customer" as "store_sales_customer_customer" on
  "store_sales_customer_customer_address_customer_address"."ca_address_sk" =
  "store_sales_customer_customer"."c_current_addr_sk"
  WHERE
      "store_sales_customer_customer"."c_preferred_cust_flag" = 'Y'

  GROUP BY
      1
  HAVI
  …
  _address"."ca_zip",1,2
  ) and REGEXP_MATCHES($1,('(^|,)(' ||
  SUBSTRING("store_sales_store_store"."s_zip",1,2)))
  )
  SELECT
      "abundant"."store_sales_store_store_name" as
  "store_sales_store_store_name",
      sum("abundant"."store_sales_net_profit") as "total_net_profit"
  FROM
      "abundant"
      INNER JOIN "wakeful" on
  "abundant"."store_sales_customer_customer_address_zip" is not distinct from
  "wakeful"."store_sales_customer_customer_address_zip"
  GROUP BY
      1
  ORDER BY
      "abundant"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query11.preql`

  ```text
  HAVING references 'local.store_rev_2001',
  'local.web_rev_2001', which are not in the SELECT projection (line 15). Add
  them to SELECT, each prefixed with `--` so they stay out of the output rows —
  keep your HAVING as-is:
      select <your existing columns>, --local.store_rev_2001,
  --local.web_rev_2001
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query14.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query14.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query14.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query14.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query14.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_avg', which is not in the
  SELECT projection (line 22). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 84 (char 83). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 84 (char 83). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 84 (char 83). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```

### `undefined-concept`

- `trilogy run query04.preql duckdb`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query04.preql duckdb`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query04.preql duckdb`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query05.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web_returns.web_site.site_id. Suggestions: ['web_sales.web_site.site_id',
  'web_returns.item.item_id', 'web_sales.web_site.site_sk']")
  ```
- `trilogy run query06.preql duckdb`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.current_price. Suggestions: ['store_sales.item.current_price',
  'store_sales.promotion.item.current_price']")
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
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  brand_id. Suggestions: ['ws.item.brand_id', 'ss.item.brand_id',
  'cs.item.brand_id']")
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.date_dim:date_dim select date_dim.week_seq, date_dim.year, date_dim.date limit 20 order by date_dim.date asc;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   date_dim.year, date_dim.date ??? limit 20 order by date_dim.dat...
  ```
- `trilogy file write query02.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;

merge ws.sold_date.date_sk into ~cs.sold_date.date_sk…ek_seq,
    coalesce(s02_daily / nullif(s01_daily, 0), 0) by (s02_week = ws.sold_date.week_seq + 53, s02_dow = ws.sold_date.dow) as ratio
limit 100;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
   / nullif(s01_daily, 0), 0) by ??? (s02_week = ws.sold_date.week...

  Write stats: received 897 chars / 897 bytes; tail: …'k_seq + 53, s02_dow =
  ws.sold_date.dow) as ratio\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;

merge ws.sold_date.date_sk into ~cs.sold_date.date_sk…
    coalesce(s02_sat, 0) / nullif(coalesce(s01_sat, 0), 0) by ws.sold_date.week_seq + 53 as sat_ratio
order by ws.sold_date.week_seq asc
limit 100;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...if(coalesce(s01_sun, 0), 0) by ??? ws.sold_date.week_seq + 53 as...

  Write stats: received 3803 chars / 3803 bytes; tail: …' as sat_ratio\\norder by
  ws.sold_date.week_seq asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;

# Store sales per outlet with rollup
select
    'store' as channel,
    co…06'::date) as total_returns,
    0 - sum(wr.net_loss ? wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as total_profit
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...te) as total_profit limit 100; ???

  Write stats: received 3101 chars / 3101 bytes; tail: …"23'::date and
  '2000-09-06'::date) as total_profit\\nlimit 100;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.year, store_sales.date_dim.qoy, count(store_sales.ticket_number) where store_sales.date_dim.year = 1998 and store_sales.date_dim.qoy = 2 limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...nt(store_sales.ticket_number) ??? where store_sales.date_dim.yea...
  ```
- `trilogy run --import raw.store_sales:ss select count(ss.ticket_number) where ss.date_dim.fy_year=2001 and ss.date_dim.qoy=1;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...elect count(ss.ticket_number) ??? where ss.date_dim.fy_year=2001...
  ```

### `join-resolution`

- `trilogy run --import raw.web_sales:ws --import raw.catalog_sales:cs select ws.sold_date.year as yr, sum(ws.ext_sales_price) + sum(cs.ext_sales_price) as total where ws.sold_date.year=2001 and cs.sold_date.year=2001 limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.yr<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns select store_sales.store.store_id, sum(store_sales.ext_sales_price)…m.date between '2000-08-23'::date and '2000-09-06'::date and store_returns.date_dim.date between '2000-08-23'::date and '2000-09-06'::date limit 100;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.store.store_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select count(ss.ticket_number) as cnt where ss.date_dim.fy_year=2001 and ss.date_dim.qoy=1 and sr.date_dim.fy_year=2001 and sr.date_dim.qoy in (1,2,3) limit 10;`

  ```text
  Could not resolve connections for query with output
  ['local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `cli-misuse`

- `trilogy run --import raw.web_sales:ws --import raw.web_returns:wr merge wr.item.item_sk into ~ws.item.item_sk; merge wr.order_number into ~ws.order_number; s…-23'::date and '2000-09-06'::date) - sum(wr.net_loss ? wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as total_profit limit 100;`

  ```text
  'select 'web' as channel, concat('web_site_', ws.web_site.site_id) as outlet, sum(ws.ext_sales_price ? ws.sold_date.date between '2000-08-23'::date and '2000-09-06'::date) as total_sales, sum(wr.return_amt ? wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as total_returns, sum(ws.net_profit ? ws.sold_date.date between '2000-08-23'::date and '2000-09-06'::date) - sum(wr.net_loss ? wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as total_profit limit 100;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.input_zips_list. Valid:
  'STRING'.
  ```
