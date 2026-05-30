# Trilogy failure analysis — 20260530-031442

- Run `20260530-031441_base` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 730 | failed: 81 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 30 | 37% |
| `other` | 23 | 28% |
| `undefined-concept` | 12 | 15% |
| `join-resolution` | 11 | 14% |
| `syntax-missing-alias` | 2 | 2% |
| `cli-misuse` | 2 | 2% |
| `file-not-found` | 1 | 1% |

## Detail

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.store_returns as store_returns;

# Filter: returns in 2000 at stores in Tennessee
where store_returns.d…eturn_amt) by store_returns.store.store_sk;

select
    store_returns.customer.customer_id
order by
    store_returns.customer.customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...amount per customer per store ??? auto cust_store_return_amt <-

  Write stats: received 599 chars / 599 bytes; tail: …'d\\norder by\\n
  store_returns.customer.customer_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql -e -c import raw.store_returns as store_returns;

# Filter: returns in 2000 at stores in Tennessee
where store_returns.date_…eturn_amt) by store_returns.store.store_sk;

select
    store_returns.customer.customer_id
order by
    store_returns.customer.customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...amount per customer per store ??? auto cust_store_return_amt <-

  Write stats: received 599 chars / 599 bytes; tail: …'d\\norder by\\n
  store_returns.customer.customer_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_returns:store_returns select store_returns.customer.customer_id, store_returns.store.store_id, sum(store_returns.return_amt) a…returns.store.state = 'TN' and store_returns.date_dim.year = 2000 group by store_returns.customer.customer_id, store_returns.store.store_id limit 10;`

  ```text
  --> 2:198
    |
  2 | select store_returns.customer.customer_id, store_returns.store.store_id,
  sum(store_returns.return_amt) as total_return where store_returns.store.state =
  'TN' and store_returns.date_dim.year = 2000 group by
  store_returns.customer.customer_id, store_returns.store.store_id limit 10;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ..._returns.date_dim.year = 2000 ??? group by store_returns.custome...
  ```
- `trilogy file write query01.preql -e -c import raw.store_returns as store_returns;

# Filter: returns in 2000 at stores in Tennessee
where store_returns.date_…urns.customer.customer_id
where
    cust_store_return_amt > 1.2 * avg_cust_return_by_store
order by
    store_returns.customer.customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...amount per customer per store ??? auto cust_store_return_amt <-

  Write stats: received 727 chars / 727 bytes; tail: …'e\\norder by\\n
  store_returns.customer.customer_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql --content import raw.store_returns as store_returns;
where store_returns.date_dim.year = 2000
  and store_returns.store.stat…urns.customer.customer_id
where
    cust_store_return_amt > 1.2 * avg_cust_return_by_store
order by
    store_returns.customer.customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 4, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...re_returns.store.state = 'TN' ??? auto cust_store_return_amt <-

  Write stats: received 514 chars / 514 bytes; tail: …'e\\norder by\\n
  store_returns.customer.customer_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Step 1: Split the zips parameter into an array of 5-digit ZIP strings
auto zips_array <-…oy = 2

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by
    store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 9, column 83.
  Expected one of:
          * _TERMINATOR

  Location:
  ...customer_sk) by preferred_zip ??? as zip_pref_count;  where

  Write stats: received 697 chars / 697 bytes; tail: …'ofit) as
  total_net_profit\\norder by\\n    store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Ticket-quantity buckets
# bucket 1: 1-20
# bucket 2: 21-40
# bucket 3: 4…
    sum(bucket_2_val) as b2_21_40,
    sum(bucket_3_val) as b3_41_60,
    sum(bucket_4_val) as b4_61_80,
    sum(bucket_5_val) as b5_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 16, column 2.
  Expected one of:
          * WHEN

  Location:
  ...antity between 81 and 100, 5 ) ??? ;  # count of line items (rows...

  Write stats: received 1640 chars / 1640 bytes; tail: …'al) as b4_61_80,\\n
  sum(bucket_5_val) as b5_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

auto qty_bucket <- case(
    store_sales.quantity between 1 and 20, 1,
   …y_bucket = 4) as b4_61_80,
    max(case(bucket_line_count > 165306, bucket_avg_discount, bucket_avg_net_paid) ? qty_bucket = 5) as b5_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 2.
  Expected one of:
          * WHEN

  Location:
  ...antity between 81 and 100, 5 ) ??? ;  auto bucket_line_count <- c...

  Write stats: received 1105 chars / 1105 bytes; tail: …'bucket_avg_net_paid) ?
  qty_bucket = 5) as b5_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Compute per-ticket quantity by summing the quantity per ticket
auto tick…60,
    case(cnt > 10097, avg_disc, avg_paid) ? bucket = 4 as b4_61_80,
    case(cnt > 165306, avg_disc, avg_paid) ? bucket = 5 as b5_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 13, column 2.
  Expected one of:
          * WHEN

  Location:
  ...et_qty between 81 and 100, 5 ) ??? ;  # Per-bucket metrics: line-...

  Write stats: received 1022 chars / 1022 bytes; tail: …'5306, avg_disc,
  avg_paid) ? bucket = 5 as b5_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Per-ticket quantity sum
auto ticket_qty <- sum(store_sales.quantity) by …se(cnt > 10097, avg_disc, avg_paid) ? item_bucket = 4 as b4_61_80,
    case(cnt > 165306, avg_disc, avg_paid) ? item_bucket = 5 as b5_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 8, column 21.
  Expected one of:
          * RPAR
          * COMMA

  Location:
   <- case(     ticket_qty >= 1 ??? and ticket_qty <= 20, 1,     t...

  Write stats: received 1261 chars / 1261 bytes; tail: …' avg_disc, avg_paid) ?
  item_bucket = 5 as b5_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Per-ticket quantity sum
auto ticket_qty <- sum(store_sales.quantity) by …se(cnt > 10097, avg_disc, avg_paid) ? item_bucket = 4 as b4_61_80,
    case(cnt > 165306, avg_disc, avg_paid) ? item_bucket = 5 as b5_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 13, column 2.
  Expected one of:
          * WHEN

  Location:
  ...et_qty between 81 and 100, 5 ) ??? ;  # Now re-express at line-it...

  Write stats: received 1216 chars / 1216 bytes; tail: …' avg_disc, avg_paid) ?
  item_bucket = 5 as b5_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select case(1=1, 1, 2) as test;`

  ```text
  --> 2:24
    |
  2 | select case(1=1, 1, 2) as test;
    |                        ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or fcase_simple_when
  Location:
  ...sales; select case(1=1, 1, 2) ??? as test;
  ```
- `trilogy run --import raw.store_sales:store_sales select simple_case(1=1, 1, 2) as test;`

  ```text
  --> 2:19
    |
  2 | select simple_case(1=1, 1, 2) as test;
    |                   ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...tore_sales; select simple_case ??? (1=1, 1, 2) as test;
  ```
- `trilogy run --import raw.store_sales:store_sales select sum(store_sales.quantity) as ticket_qty
by store_sales.ticket_number
limit 10;`

  ```text
  --> 3:1
    |
  3 | by store_sales.ticket_number
    | ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...sales.quantity) as ticket_qty ??? by store_sales.ticket_number l...
  ```
- `trilogy run --import raw.store_sales:store_sales select
    sum(store_sales.quantity) as ticket_qty
    by store_sales.ticket_number
limit 10;`

  ```text
  --> 4:5
    |
  4 |     by store_sales.ticket_number
    |     ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...s.quantity) as ticket_qty     ??? by store_sales.ticket_number l...
  ```
- `trilogy run --import raw.store_sales:store_sales select
    sum(store_sales.quantity) as ticket_qty
    by (store_sales.ticket_number)
limit 10;`

  ```text
  --> 4:5
    |
  4 |     by (store_sales.ticket_number)
    |     ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...s.quantity) as ticket_qty     ??? by (store_sales.ticket_number)...
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Per-ticket total quantity (sum across all items on that ticket)
auto tic…
    count(store_sales.ticket_number) as cnt,
    avg(store_sales.ext_discount_amt) as avg_disc,
    avg(store_sales.net_paid) as avg_paid
limit 100;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 7, column 36.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ticket_qty_line <- ticket_qty ??? by store_sales.ticket_number,

  Write stats: received 817 chars / 817 bytes; tail: …'g_disc,\\n
  avg(store_sales.net_paid) as avg_paid\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Per-ticket total quantity
auto ticket_qty <- sum(store_sales.quantity) b…t) as b4_61_80
    where bucket = 4,
    case(ticket_cnt > 165306, avg_disc_by_bucket, avg_paid_by_bucket) as b5_81_100
    where bucket = 5
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 13, column 2.
  Expected one of:
          * WHEN

  Location:
  ...et_qty between 81 and 100, 5 ) ??? ;  # Count of tickets per buck...

  Write stats: received 1373 chars / 1373 bytes; tail: …'g_paid_by_bucket) as
  b5_81_100\\n    where bucket = 5\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Per-ticket total quantity
auto ticket_qty <- sum(store_sales.quantity) b…by_bucket) as b4_61_80 where bucket = 4,
    case(ticket_cnt > 165306, avg_disc_by_bucket, avg_paid_by_bucket) as b5_81_100 where bucket = 5
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 13, column 2.
  Expected one of:
          * WHEN

  Location:
  ...et_qty between 81 and 100, 5 ) ??? ;  # Count of tickets per buck...

  Write stats: received 1353 chars / 1353 bytes; tail: …', avg_paid_by_bucket) as
  b5_81_100 where bucket = 5\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Customers who have sales in BOTH stor…rder by
    billing_customer_code nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 19, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ....customer.customer_sk > 0     ??? and count(web_sales.order_numb...

  Write stats: received 2269 chars / 2269 bytes; tail: …'nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

auto store_rev_2001 <- sum(store_sales.…rder by
    billing_customer_code nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 16, column 36.
  Expected one of:
          * _TERMINATOR

  Location:
  ...active_both <- has_store_2001 ??? and has_web_2001;  where activ...

  Write stats: received 1821 chars / 1821 bytes; tail: …'nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…_inc_ship) by (web_sales.item.brand, web_sales.item.class_id, web_sales.item.category_id)
    where web_sales.sold_date.year between 1999 and 2001;

`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 17, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...e_sales.item.category_id)     ??? where store_sales.date_dim.yea...

  Write stats: received 1293 chars / 1293 bytes; tail: …'\\n    where
  web_sales.sold_date.year between 1999 and 2001;\\n\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales select store_sales.item.brand, sto…, store_sales.item.category_id, count(store_sales.ticket_number) as cnt where store_sales.date_dim.year between 1999 and 2001 group by 1,2,3 limit 5;`

  ```text
  --> 4:183
    |
  4 | select store_sales.item.brand, store_sales.item.class_id,
  store_sales.item.category_id, count(store_sales.ticket_number) as cnt where
  store_sales.date_dim.year between 1999 and 2001 group by 1,2,3 limit 5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...im.year between 1999 and 2001 ??? group by 1,2,3 limit 5;
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…istinct channels it appears in
auto combo_channels <- count(1) by (store_sales.item.brand, store_sales.item.class_id, store_sales.item.category_id);
`

  ```text
  …

          * _DATE_ADD
          * _WEEK
          * _HOUR
          * FILTER
          * _ILIKE
          * INT_LITERAL_PART
          * _REGEXP_REPLACE
          * LBRACE
          * _DAY_NAME
          * _PARSE_TIME
          * GREATEST
          * _DATE_DIFF
          * _ATTR_ACCESS
          * _BOOL_AND
          * CURRENT_DATE
          * _VARIANCE
          * GROUPING
          * _GEO_Y
          * COALESCE
          * MULTILINE_STRING
          * _GEO_CENTROID
          * _ARRAY_SUM
          * _QUARTER
          * _DATE_SUB
          * FALSE
          * _DATE
          * _MAP_KEYS
          * DIVIDE
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...ar between 1999 and 2001) by ( ??? )     and avg(catalog_sales.qu...

  Write stats: received 2175 chars / 2175 bytes; tail: …',
  store_sales.item.class_id, store_sales.item.category_id);\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…, item_class_id, item_category_id) ? web_sales.sold_date.year between 1999 and 2001) > 0

select item_brand, item_class_id, item_category_id
limit 5;`

  ```text
  …
   _UNNEST
          * _DATE_PART
          * /\-?[0-9]*\.[0-9]+/
          * _ARRAY_SUM
          * TRUE
          * DIVIDE
          * _ARRAY_TRANSFORM
          * WINDOW_TYPE_SQL_NUMBERING
          * COALESCE
          * COUNT_DISTINCT
          * _PARSE_TIME
          * LEAST
          * LSQB
          * _ARRAY_SORT
          * FALSE
          * _COUNT
          * _DATE_ADD
          * _DAY_NAME
          * BOOL
          * _LIKE
          * CONDITION_NOT
          * NULLIF
          * _LOG
          * _SPLIT
          * _GROUP
          * _RANDOM
          * "@"
          * _BOOL_AND
          * _BOOL_OR
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...l_amt <- sum(ss_sale_val) by ( ??? ) where store_sales.date_dim.y...

  Write stats: received 2183 chars / 2183 bytes; tail: …'\\nselect item_brand,
  item_class_id, item_category_id\\nlimit 5;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale… ();

select
    store_sales.item.brand as brand,
    store_sales.item.class_id as class_id,
    store_sales.item.category_id as category_id
limit 5;`

  ```text
  …
  _TRUNC
          * _MIN
          * _GENERATE_ARRAY
          * _COUNT
          * _GEO_TRANSFORM
          * LEN
          * WINDOW_TYPE_SQL_NAVIGATION
          * FILTER
          * GROUPING_ID
          * _LOWER
          * /(group)\s+(*)/i
          * _LTRIM
          * "@"
          * _DAY
          * CONCAT
          * CURRENT_DATE
          * _HOUR
          * _ARRAY_FILTER
          * _RTRIM
          * _ARRAY_SORT
          * WINDOW_TYPE_SQL_NUMBERING
          * /add\(/
          * DATETIME
          * WINDOW_TYPE_LEGACY
          * /\-?[0-9]*\.[0-9]+/
          * _PARSE_TIME
          * LBRACE
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...ar between 1999 and 2001) by ( ??? );  select     store_sales.ite...

  Write stats: received 730 chars / 730 bytes; tail: …'id,\\n
  store_sales.item.category_id as category_id\\nlimit 5;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…s total_sales,
    count(store_sales.ticket_number) as total_count
where store_sales.date_dim.year = 2001 and store_sales.date_dim.moy = 11
limit 10;`

  ```text
  …
  EXP_EXTRACT
          * _GEO_DISTANCE
          * _GEO_POINT
          * _UNNEST
          * _ARRAY_SORT
          * _GEO_X
          * _DAY_NAME
          * _GEO_CENTROID
          * _HEX
          * LPAR
          * CASE
          * _BOOL_AND
          * _TIMESTAMP
          * LEN
          * _DATE_DIFF
          * _CEIL
          * LEAST
          * COUNT_DISTINCT
          * _RTRIM
          * _TRIM
          * _BOOL_OR
          * _LTRIM
          * _AVG
          * _DATE_TRUNC
          * "@"
          * _REGEXP_CONTAINS
          * _LOWER
          * CONDITION_NOT
          * _REPLACE
          * DIVIDE
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...* store_sales.list_price) by ( ??? ) where store_sales.date_dim.y...

  Write stats: received 1060 chars / 1060 bytes; tail: …'_dim.year = 2001 and
  store_sales.date_dim.moy = 11\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query17.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…uantity) / avg(catalog_sales.quantity) as cs_qty_cv
order by item_code nulls first, item_description nulls first, store_state nulls first
limit 100;
`

  ```text
  refused to write 'query17.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('MERGE', 'merge') at line 11, column 1.
  Expected one of:
          * SELECT

  Location:
  ...urn date in 2001Q1, Q2, or Q3 ??? merge store_returns.item.item_...

  Write stats: received 2081 chars / 2081 bytes; tail: …'description nulls first,
  store_state nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query17.preql -e -c import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as catalog_…uantity) / avg(catalog_sales.quantity) as cs_qty_cv
order by item_code nulls first, item_description nulls first, store_state nulls first
limit 100;
`

  ```text
  refused to write 'query17.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('MERGE', 'merge') at line 11, column 1.
  Expected one of:
          * SELECT

  Location:
  ...urn date in 2001Q1, Q2, or Q3 ??? merge store_returns.item.item_...

  Write stats: received 2081 chars / 2081 bytes; tail: …'description nulls first,
  store_state nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.date_dim:date_dim select date_dim.fy_year, date_dim.fy_quarter_seq, date_dim.quarter_name, date_dim.date, count(date_dim.date_sk) as…m.fy_quarter_seq in (1,2,3) group by date_dim.fy_year, date_dim.fy_quarter_seq, date_dim.quarter_name, date_dim.date order by date_dim.date limit 10;`

  ```text
  --> 2:189
    |
  2 | select date_dim.fy_year, date_dim.fy_quarter_seq, date_dim.quarter_name,
  date_dim.date, count(date_dim.date_sk) as cnt where date_dim.fy_year = 2001 and
  date_dim.fy_quarter_seq in (1,2,3) group by date_dim.fy_year,
  date_dim.fy_quarter_seq, date_dim.quarter_name, date_dim.date order by
  date_dim.date limit 10;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...dim.fy_quarter_seq in (1,2,3) ??? group by date_dim.fy_year, dat...
  ```

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 92 (char 91). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 84 (char 83). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 87 (char 86). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.year_total_catalog', which is not in
  the SELECT projection (line 19). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.year_total_catalog`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run - --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run - --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query07.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 98 (char 97). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 103 (char 102). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
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
- `trilogy run --import raw.store_sales:store_sales select case when 1=1 then 1 else 2 end as test;`

  ```text
  'int' object has no attribute 'concept_arguments'
  ```
- `trilogy run --import raw.store_sales:store_sales select
    sum(store_sales.quantity) by store_sales.ticket_number as ticket_qty
where ticket_qty between 1 and 20 or ticket_qty between 21 and 40 or ticket_qty between 41 and 60 or ticket_qty between 61 and 80 or ticket_qty between 81 and 100
limit 5;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.ticket_qty) in the same statement where clause; move to the HAVING
  clause instead; Line: 2
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query11.preql`

  ```text
  HAVING references 'local.web_rev_2002', which is not in the
  SELECT projection (line 13). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.web_rev_2002`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 4987 (char 4986). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 3570 (char 3569). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run - duckdb --import raw/catalog_sales.preql:catalog_sales --import raw/catalog_returns.preql:catalog_returns`

  ```text
  Duplicate select output for catalog_sales.order_number; Line:
  6
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```

### `undefined-concept`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.customer_sk.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.preferred_cust_flag.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.preferred_cust_flag.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.preferred_cust_flag.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'line: 5: Undefined concept:
  local.net_profit.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'line: 5: Undefined concept:
  local.net_profit.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'line: 5: Undefined concept:
  local.net_profit.')
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
- `trilogy explore query11.preql`

  ```text
  Failed to parse query11.preql: (UndefinedConceptException(...), 'Undefined
  concept: first_name.')
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```

### `join-resolution`

- `trilogy run query02.preql`

  ```text
  Could not resolve connections for query with output
  ['local.week_seq<Purpose.PROPERTY>Derivation.BASIC>',
  'local.sunday<Purpose.METRIC>Derivation.BASIC>',
  'local.monday<Purpose.METRIC>Derivation.BASIC>',
  'local.tuesday<Purpose.METRIC>Derivation.BASIC>',
  'local.wednesday<Purpose.METRIC>Derivation.BASIC>',
  'local.thursday<Purpose.METRIC>Derivation.BASIC>',
  'local.friday<Purpose.METRIC>Derivation.BASIC>',
  'local.saturday<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['customer_demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'customer_demographics.marital_status<Purpose.PROPERTY>Derivation.ROOT>',
  'customer_demographics.education_status<Purpose.PROPERTY>Derivation.ROOT>',
  'customer_demographics.purchase_estimate<Purpose.PROPERTY>Derivation.ROOT>',
  'customer_demographics.credit_rating<Purpose.PROPERTY>Derivation.ROOT>',
  'customer_demographics.dep_count<Purpose.PROPERTY>Derivation.ROOT>',
  'customer_demographics.dep_employed_count<Purpose.PROPERTY>Derivation.ROOT>',
  'customer_demographics.dep_college_count<Purpose.PROPERTY>Derivation.ROOT>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query11.preql`

  ```text
  Could not resolve connections for query with output
  ['local.billing_customer_code<Purpose.PROPERTY>Derivation.BASIC>',
  'customer.first_name<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.last_name<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.preferred_cust_flag<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```
- `trilogy run query11.preql`

  ```text
  Could not resolve connections for query with output
  ['local.billing_customer_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.preferred_cust_flag<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query11.preql`

  ```text
  Could not resolve connections for query with output
  ['local.billing_customer_code<Purpose.PROPERTY>Derivation.BASIC>',
  'customer.first_name<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.last_name<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.preferred_cust_flag<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales select avg(store_sales.quantity * …ate_dim.year between 1999 and 2001 and catalog_sales.sold_date.year between 1999 and 2001 and web_sales.sold_date.year between 1999 and 2001 limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.overall_avg_sale<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales select avg(store_sales.quantity * store_sales.list_price) as overall_avg where catalog_sales.sold_date.year between 1999 and 2001 limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.overall_avg<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales select store_sales.item.brand as b…and store_sales.date_dim.moy = 11 and catalog_sales.sold_date.year between 1999 and 2001 and web_sales.sold_date.year between 1999 and 2001 limit 10;`

  ```text
  Could not resolve connections for query with output
  ['local.brand<Purpose.PROPERTY>Derivation.BASIC>',
  'local.class_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.category_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales select store_sales.item.brand as b…atalog_sales.sold_date.year = 2001 and catalog_sales.sold_date.moy = 11 and web_sales.sold_date.year = 2001 and web_sales.sold_date.moy = 11 limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.brand<Purpose.PROPERTY>Derivation.BASIC>',
  'local.class_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.category_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.ss_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ss_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ws_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ws_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales select store_sales.item.brand as brand, store_sales.item.class_id a…sales, count(catalog_sales.net_paid_inc_ship_tax) as cs_count where catalog_sales.sold_date.year = 2001 and catalog_sales.sold_date.moy = 11 limit 5;`

  ```text
  Could not resolve connections for query with output
  ['local.brand<Purpose.PROPERTY>Derivation.BASIC>',
  'local.class_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.category_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.cs_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_count<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run - duckdb --import raw/catalog_sales.preql:catalog_sales --import raw/catalog_returns.preql:catalog_returns`

  ```text
  Could not resolve connections for query with output
  ['catalog_sales.order_number<Purpose.PROPERTY>Derivation.ROOT>',
  'catalog_returns.order_number<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.store_returns:store_returns select store_returns.date_dim.year, store_returns.store.state, count(store_returns.ticket_number) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...(store_returns.ticket_number) ??? limit 10;
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.zip, count(store_sales.item.item_sk) order by 2 desc limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...unt(store_sales.item.item_sk) ??? order by 2 desc limit 5;
  ```

### `cli-misuse`

- `trilogy read_file raw/store_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/catalog_sales.preql --show all --regex call_center .county`

  ```text
  Got unexpected extra argument (.county)
  ```

### `file-not-found`

- `trilogy run query17.preql`

  ```text
  Input 'query17.preql' does not exist.
  ```
