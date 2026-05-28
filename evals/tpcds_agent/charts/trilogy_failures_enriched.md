# Trilogy failure analysis — 20260528-133742

- Run `20260528-133742` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 565 | failed: 54 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 31 | 57% |
| `other` | 11 | 20% |
| `cli-misuse` | 4 | 7% |
| `syntax-missing-alias` | 4 | 7% |
| `undefined-concept` | 2 | 4% |
| `join-resolution` | 1 | 2% |
| `file-not-found` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy file write query02.preql --content # Query 2: Combine web_sales and catalog_sales into a single stream of (sold date, extended sales price).
# For ea…nd merge facts.
# Actually, in Trilogy, we can't do UNION of two fact models easily without a shared
# unified model. Let's try a different approach.`

  ```text
  …
  q, cats.date.day_of_week) + \n
  sum(cats.ext_sales_price ? cats.date.year = 2002) by (cats.date.week_seq,
  cats.date.day_of_week);\n\n# Hmm, that won't work as just a sum of separate
  aggregations. Let me think differently.\n\n# We need a combined stream. Let's
  use the date dimension directly and merge facts.\n# Actually, in Trilogy, we
  can't do UNION of two fact models easily without a shared\n# unified model.
  Let's try a different approach.") at line 10, column 11.
  Expected one of:
          * IMPORT_DOT
          * "as"
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
   with nulls first.  import raw ??? /web_sales.preql as webs; impo...

  Write stats: received 1775 chars / 1775 bytes; tail: …"ut a shared\\n# unified
  model. Let's try a different approach.".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw/unified_sales.preql as sales;

# Per week_seq, year, split sales into seven day-of-week columns
# day_o…(wk_fri_by_2002, 0), 2) as fri_ratio,
    round(coalesce(wk_sat, 0) * 1.0 / nullif(wk_sat_by_2002, 0), 2) as sat_ratio
order by sales.date.week_seq;
`

  ```text
  …
  , 2) as mon_ratio,\n
  round(coalesce(wk_tue, 0) * 1.0 / nullif(wk_tue_by_2002, 0), 2) as tue_ratio,\n
  round(coalesce(wk_wed, 0) * 1.0 / nullif(wk_wed_by_2002, 0), 2) as wed_ratio,\n
  round(coalesce(wk_thu, 0) * 1.0 / nullif(wk_thu_by_2002, 0), 2) as thu_ratio,\n
  round(coalesce(wk_fri, 0) * 1.0 / nullif(wk_fri_by_2002, 0), 2) as fri_ratio,\n
  round(coalesce(wk_sat, 0) * 1.0 / nullif(wk_sat_by_2002, 0), 2) as
  sat_ratio\norder by sales.date.week_seq;\n') at line 1, column 11.
  Expected one of:
          * _TERMINATOR
          * IMPORT_DOT
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /unified_sales.preql as sales;...

  Write stats: received 1914 chars / 1914 bytes; tail: …'_by_2002, 0), 2) as
  sat_ratio\\norder by sales.date.week_seq;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw/unified_sales.preql as sales;

# Per-day totals per (week_seq, year) - combining WEB + CATALOG (not STO…es.date.week_seq, sales.date.year);
auto wk_sat <- sum(sales.ext_sales_price ? sales.date.day_of_week = 6) by (sales.date.week_seq, sales.date.year);`

  ```text
  …

  sum(sales.ext_sales_price ? sales.date.day_of_week = 3) by
  (sales.date.week_seq, sales.date.year);\nauto wk_thu <-
  sum(sales.ext_sales_price ? sales.date.day_of_week = 4) by
  (sales.date.week_seq, sales.date.year);\nauto wk_fri <-
  sum(sales.ext_sales_price ? sales.date.day_of_week = 5) by
  (sales.date.week_seq, sales.date.year);\nauto wk_sat <-
  sum(sales.ext_sales_price ? sales.date.day_of_week = 6) by
  (sales.date.week_seq, sales.date.year);') at line 1, column 11.
  Expected one of:
          * "as"
          * _TERMINATOR
          * IMPORT_DOT
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /unified_sales.preql as sales;...

  Write stats: received 966 chars / 966 bytes; tail: …'.day_of_week = 6) by
  (sales.date.week_seq, sales.date.year);'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.unified_sales as sales;

# Per-day totals per (week_seq, year) - combining WEB + CATALOG (not STORE)
# …= 6 and sales.date.year = 2002) by (sales.date.week_seq) where sales.date.week_seq = wk_2001.ws + 53), 2) as sat_ratio
order by sales.date.week_seq;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 18, column 148.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...002) by (sales.date.week_seq) ??? where sales.date.week_seq = wk...

  Write stats: received 2650 chars / 2650 bytes; tail: …'001.ws + 53), 2) as
  sat_ratio\\norder by sales.date.week_seq;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query06.preql --content import raw.store_sales as store_sales;

# Restrict to store sales in January 2001
where month_of_year(store_sales.…ales.customer.id) as customer_count
having
    customer_count >= 10
order by
    customer_count asc nulls first,
    state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '(store_sales.date.date) = 1 and
  year(store_sales.date.date) = 2001\n\n# Find items whose current_price > 1.2 *
  avg(current_price) in same category\n# For each customer who bought such an
  item, count them per state\nselect\n    store_sales.customer.address.state as
  state,\n    count(store_sales.customer.id) as customer_count\nhaving\n
  customer_count >= 10\norder by\n    customer_count asc nulls first,\n    state
  asc nulls first\nlimit 100;') at line 4, column 20.
  Expected one of:
          * SELECT
  Previous tokens: [Token('IDENTIFIER', 'month_of_year')]

  Location:
  ...nuary 2001 where month_of_year ??? (store_sales.date.date) = 1 an...

  Write stats: received 534 chars / 534 bytes; tail: …'_count asc nulls first,\\n
  state asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;

where store_sales.return_reason.id = 1

select
    case(
        count(store_s…store_sales.quantity between 81 and 100),
        avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)
    ) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 10, column 7.
  Expected one of:
          * WHEN

  Location:
  ...ntity between 1 and 20)     ) ??? as bucket_1_20,     case(

  Write stats: received 1526 chars / 1526 bytes; tail: …'quantity between 81 and
  100)\\n    ) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;

where store_sales.return_reason.id = 1

select
    case(
        count(store_s…_sales.quantity between 81 and 100)
    ,
        avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)
    ) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 12, column 7.
  Expected one of:
          * WHEN

  Location:
  ...ntity between 1 and 20)     ) ??? as bucket_1_20,     case(

  Write stats: received 1576 chars / 1576 bytes; tail: …'quantity between 81 and
  100)\\n    ) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;

where store_sales.return_reason.id = 1

select
    case(
        count(store_s…e_sales.quantity between 81 and 100)
        else avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)
    ) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 8, column 9.
  Expected one of:
          * RPAR
          * COMMA

  Location:
  ...een 1 and 20) > 74129         ??? when avg(store_sales.ext_disco...

  Write stats: received 1566 chars / 1566 bytes; tail: …'quantity between 81 and
  100)\\n    ) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;

where store_sales.return_reason.id = 1

select
    case(
        count(store_s…nt ? store_sales.quantity between 1 and 20)
        , avg(store_sales.net_paid ? store_sales.quantity between 1 and 20)
    ) as bucket_1_20
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 10, column 7.
  Expected one of:
          * WHEN

  Location:
  ...ntity between 1 and 20)     ) ??? as bucket_1_20 limit 1;

  Write stats: received 378 chars / 378 bytes; tail: …'les.quantity between 1 and
  20)\\n    ) as bucket_1_20\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;

where store_sales.return_reason.id = 1

select
    case(
        count(store_s…nt ? store_sales.quantity between 1 and 20)
        , avg(store_sales.net_paid ? store_sales.quantity between 1 and 20)
    ) as bucket_1_20
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 10, column 7.
  Expected one of:
          * WHEN

  Location:
  ...ntity between 1 and 20)     ) ??? as bucket_1_20 limit 1;

  Write stats: received 378 chars / 378 bytes; tail: …'les.quantity between 1 and
  20)\\n    ) as bucket_1_20\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;
where store_sales.return_reason.id = 1

auto cnt_1_20 <- count(store_sales.…tity between 81 and 100);

select
    coalesce(cnt_1_20, 0) as c1,
    coalesce(avg_disc_1_20, 0) as d1,
    coalesce(avg_net_1_20, 0) as n1
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 4, column 1.
  Expected one of:
          * SELECT

  Location:
  ...e_sales.return_reason.id = 1  ??? auto cnt_1_20 <- count(store_s...

  Write stats: received 1627 chars / 1627 bytes; tail: …'1_20, 0) as d1,\\n
  coalesce(avg_net_1_20, 0) as n1\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…  purchase_estimate asc,
    credit_rating asc,
    dependent_count asc,
    employed_dependent_count asc,
    college_dependent_count asc
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...and 4     and (         exists ??? (select web_sales.order_number...

  Write stats: received 2606 chars / 2606 bytes; tail: …'endent_count asc,\\n
  college_dependent_count asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Merge customer concept so both domain…y 
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 22, column 69.
  Expected one of:
          * WHEN

  Location:
  ...0, 0, store_2002 / store_2001) ??? ;  # Web ratio 2002/2001, trea...

  Write stats: received 2034 chars / 2034 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.id into ~web…y 
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 12, column 69.
  Expected one of:
          * WHEN

  Location:
  ...0, 0, store_2002 / store_2001) ??? ; auto web_ratio <- case(web_2...

  Write stats: received 1306 chars / 1306 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.id into ~web…y 
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER', 'order') at line 21, column 1.
  Expected one of:
          * WHEN

  Location:
  ..., 1, coalesce(store_2001, 0)) ??? order by      customer_id asc

  Write stats: received 1321 chars / 1321 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.id into ~web…y 
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 11, column 71.
  Expected one of:
          * WHEN

  Location:
   0.0, store_2002 / store_2001) ??? ; auto web_ratio <- case(web_2...

  Write stats: received 1309 chars / 1309 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.id into ~web…y 
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 11, column 108.
  Expected one of:
          * WHEN

  Location:
  ...0, 1, coalesce(store_2001, 0)) ??? ; auto web_ratio <- coalesce(w...

  Write stats: received 1383 chars / 1383 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.id into ~web…y 
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '(coalesce(web_2001, 0) = 0,
  true, 1, coalesce(web_2001, 0)) > coalesce(store_2002, 0) /
  simple_case(coalesce(store_2001, 0) = 0, true, 1, coalesce(store_2001,
  0))\norder by \n    customer_id asc nulls first,\n    first_name asc nulls
  first,\n    last_name asc nulls first,\n    preferred_cust_flag asc nulls
  first\nlimit 100;') at line 19, column 44.
  Expected one of:
          * ORDER
          * HAVING
          * _TERMINATOR
          * LIMIT
          * MERGE
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...sce(web_2002, 0) / simple_case ??? (coalesce(web_2001, 0) = 0, tr...

  Write stats: received 1346 chars / 1346 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.id into ~web…y 
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  …
  store_sales.customer.id as customer_id,\n    store_sales.customer.first_name as
  first_name,\n    store_sales.customer.last_name as last_name,\n
  store_sales.customer.preferred_cust_flag as preferred_cust_flag\nwhere \n
  coalesce(store_2001, 0) > 0\n    and coalesce(web_2001, 0) > 0\n    and
  web_ratio > store_ratio\norder by \n    customer_id asc nulls first,\n
  first_name asc nulls first,\n    last_name asc nulls first,\n
  preferred_cust_flag asc nulls first\nlimit 100;') at line 11, column 49.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'if')]

  Location:
  ...- coalesce(store_2002, 0) / if ??? (store_2001 = 0, 1, coalesce(s...

  Write stats: received 1353 chars / 1353 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query11.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

merge store_sales.customer.id into ~web…y 
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 16, column 93.
  Expected one of:
          * WHEN

  Location:
  ..., 1, coalesce(store_2001, 0)) ??? as store_ratio,     coalesce(w...

  Write stats: received 1384 chars / 1384 bytes; tail: …'ls first,\\n
  preferred_cust_flag asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/store_sales:store_sales select year(store_sales.date.date) as yr, count(store_sales.ticket_number) as cnt group by yr order by yr limit 10;`

  ```text
  --> 2:83
    |
  2 | select year(store_sales.date.date) as yr, count(store_sales.ticket_number)
  as cnt group by yr order by yr limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...e_sales.ticket_number) as cnt ??? group by yr order by yr limit
  ```
- `trilogy file write query16.preql --content import raw/catalog_sales as catalog_sales;

# Filter conditions
where
    catalog_sales.ship_date.date between '20…_sales.ext_ship_cost) as total_extended_ship_cost,
    sum(catalog_sales.net_profit) as total_net_profit
order by
    distinct_order_count
limit 100;`

  ```text
  …
  ss.state = 'GA'\n    and
  catalog_sales.call_center.county = 'Williamson County'\n    and
  catalog_sales.return_amount is null\n\n# Count distinct order numbers, total
  extended ship cost, total net profit\nselect\n
  count(catalog_sales.order_number) as distinct_order_count,\n
  sum(catalog_sales.ext_ship_cost) as total_extended_ship_cost,\n
  sum(catalog_sales.net_profit) as total_net_profit\norder by\n
  distinct_order_count\nlimit 100;") at line 1, column 11.
  Expected one of:
          * IMPORT_DOT
          * "as"
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /catalog_sales as catalog_sale...

  Write stats: received 622 chars / 622 bytes; tail: …'otal_net_profit\\norder
  by\\n    distinct_order_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query16.preql --content import raw/catalog_sales as catalog_sales;

where
    catalog_sales.ship_date.date between '2002-02-01'::date and …_sales.ext_ship_cost) as total_extended_ship_cost,
    sum(catalog_sales.net_profit) as total_net_profit
order by
    distinct_order_count
limit 100;`

  ```text
  …
  02-02-01'::date and '2002-04-02'::date\n    and
  catalog_sales.customer_address.state = 'GA'\n    and
  catalog_sales.call_center.county = 'Williamson County'\n    and
  catalog_sales.return_amount is null\n\nselect\n
  count(catalog_sales.order_number) as distinct_order_count,\n
  sum(catalog_sales.ext_ship_cost) as total_extended_ship_cost,\n
  sum(catalog_sales.net_profit) as total_net_profit\norder by\n
  distinct_order_count\nlimit 100;") at line 1, column 11.
  Expected one of:
          * IMPORT_DOT
          * _TERMINATOR
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /catalog_sales as catalog_sale...

  Write stats: received 527 chars / 527 bytes; tail: …'otal_net_profit\\norder
  by\\n    distinct_order_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.item.id, catalog_sales.item.desc, catalog_sales.item.category, catalog_sales.item.c…og_sales.item.current_price order by catalog_sales.item.category, catalog_sales.item.class, catalog_sales.item.id, catalog_sales.item.desc limit 100;`

  ```text
  --> 2:358
    |
  2 | select catalog_sales.item.id, catalog_sales.item.desc,
  catalog_sales.item.category, catalog_sales.item.class,
  catalog_sales.item.current_price, sum(catalog_sales.ext_sales_price) as
  item_total_ext_sales_price where catalog_sales.sold_date.date between
  '1999-02-22'::date and '1999-03-24'::date and catalog_sales.item.category in
  ('Sports', 'Books', 'Home') group by catalog_sales.item.id,
  catalog_sales.item.desc, catalog_sales.item.category, catalog_sales.item.class,
  catalog_sales.item.current_price order by catalog_sales.item.category,
  catalog_sales.item.class, catalog_sales.item.id, catalog_sales.item.desc limit
  100;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...n ('Sports', 'Books', 'Home') ??? group by catalog_sales.item.id...
  ```
- `trilogy file write query20.preql --content import raw/catalog_sales as catalog_sales;

where catalog_sales.sold_date.date between '1999-02-22'::date and '199…les.item.category,
    catalog_sales.item.class,
    catalog_sales.item.id,
    catalog_sales.item.desc,
    pct_of_class_ext_sales_price
limit 100;
`

  ```text
  …
  em.class,\n    catalog_sales.item.current_price,\n
  sum(catalog_sales.ext_sales_price) as item_total_ext_sales_price,\n
  (sum(catalog_sales.ext_sales_price) / sum(catalog_sales.ext_sales_price) by
  catalog_sales.item.class) * 100 as pct_of_class_ext_sales_price\norder by\n
  catalog_sales.item.category,\n    catalog_sales.item.class,\n
  catalog_sales.item.id,\n    catalog_sales.item.desc,\n
  pct_of_class_ext_sales_price\nlimit 100;\n") at line 1, column 11.
  Expected one of:
          * IMPORT_DOT
          * "as"
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /catalog_sales as catalog_sale...

  Write stats: received 744 chars / 744 bytes; tail: …'ales.item.desc,\\n
  pct_of_class_ext_sales_price\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --escapes --content import raw/catalog_sales as catalog_sales;
where catalog_sales.sold_date.date between '1999-02-22'::date…ales.item.category,
    catalog_sales.item.class,
    catalog_sales.item.id,
    catalog_sales.item.desc,
    pct_of_class_ext_sales_price
limit 100;`

  ```text
  …
  item.class,\n    catalog_sales.item.current_price,\n
  sum(catalog_sales.ext_sales_price) as item_total_ext_sales_price,\n
  (sum(catalog_sales.ext_sales_price) / sum(catalog_sales.ext_sales_price) by
  catalog_sales.item.class) * 100 as pct_of_class_ext_sales_price\norder by\n
  catalog_sales.item.category,\n    catalog_sales.item.class,\n
  catalog_sales.item.id,\n    catalog_sales.item.desc,\n
  pct_of_class_ext_sales_price\nlimit 100;") at line 1, column 11.
  Expected one of:
          * "as"
          * IMPORT_DOT
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /catalog_sales as catalog_sale...

  Write stats: received 742 chars / 742 bytes; tail: …'sales.item.desc,\\n
  pct_of_class_ext_sales_price\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import raw/catalog_sales as catalog_sales;
where catalog_sales.sold_date.date between '1999-02-22'::date and '1999…ales.item.category,
    catalog_sales.item.class,
    catalog_sales.item.id,
    catalog_sales.item.desc,
    pct_of_class_ext_sales_price
limit 100;`

  ```text
  …
  item.class,\n    catalog_sales.item.current_price,\n
  sum(catalog_sales.ext_sales_price) as item_total_ext_sales_price,\n
  (sum(catalog_sales.ext_sales_price) / sum(catalog_sales.ext_sales_price) by
  catalog_sales.item.class) * 100 as pct_of_class_ext_sales_price\norder by\n
  catalog_sales.item.category,\n    catalog_sales.item.class,\n
  catalog_sales.item.id,\n    catalog_sales.item.desc,\n
  pct_of_class_ext_sales_price\nlimit 100;") at line 1, column 11.
  Expected one of:
          * _TERMINATOR
          * IMPORT_DOT
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /catalog_sales as catalog_sale...

  Write stats: received 742 chars / 742 bytes; tail: …'sales.item.desc,\\n
  pct_of_class_ext_sales_price\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --escapes --content import raw/catalog_sales as catalog_sales;
where catalog_sales.sold_date.date between '1999-02-22'::date…ales.item.category,
    catalog_sales.item.class,
    catalog_sales.item.id,
    catalog_sales.item.desc,
    pct_of_class_ext_sales_price
limit 100;`

  ```text
  …
  item.class,\n    catalog_sales.item.current_price,\n
  sum(catalog_sales.ext_sales_price) as item_total_ext_sales_price,\n
  (sum(catalog_sales.ext_sales_price) / sum(catalog_sales.ext_sales_price) by
  catalog_sales.item.class) * 100 as pct_of_class_ext_sales_price\norder by\n
  catalog_sales.item.category,\n    catalog_sales.item.class,\n
  catalog_sales.item.id,\n    catalog_sales.item.desc,\n
  pct_of_class_ext_sales_price\nlimit 100;") at line 1, column 11.
  Expected one of:
          * _TERMINATOR
          * "as"
          * IMPORT_DOT
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /catalog_sales as catalog_sale...

  Write stats: received 742 chars / 742 bytes; tail: …'sales.item.desc,\\n
  pct_of_class_ext_sales_price\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import raw/catalog_sales as catalog_sales;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '/catalog_sales as
  catalog_sales;') at line 1, column 11.
  Expected one of:
          * IMPORT_DOT
          * _TERMINATOR
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /catalog_sales as catalog_sale...

  Write stats: received 42 chars / 42 bytes; tail: …'import raw/catalog_sales as
  catalog_sales;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/inventory:inv select inv.item.category, inv.item.class, inv.item.brand_name, inv.item.product_name, avg(inv.quantity_on_hand) as avg…category, inv.item.class, inv.item.brand_name, inv.item.product_name) order by g4 desc, g3 desc, g2 desc, g1 desc, avg_qoh asc nulls first limit 100;`

  ```text
  --> 2:151
    |
  2 | select inv.item.category, inv.item.class, inv.item.brand_name,
  inv.item.product_name, avg(inv.quantity_on_hand) as avg_qoh where inv.date.year
  = 2000 group by rollup (inv.item.category, inv.item.class, inv.item.brand_name,
  inv.item.product_name) order by g4 desc, g3 desc, g2 desc, g1 desc, avg_qoh asc
  nulls first limit 100;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...oh where inv.date.year = 2000 ??? group by rollup (inv.item.cate...
  ```
- `trilogy run --all-rows --import raw/inventory:inv select avg(inv.quantity_on_hand) as avg_qoh, group(inv.item.product_name, inv.item.brand_name, inv.item.class, inv.item.category) as g where inv.date.year = 2000;`

  ```text
  --> 2:73
    |
  2 | select avg(inv.quantity_on_hand) as avg_qoh, group(inv.item.product_name,
  inv.item.brand_name, inv.item.class, inv.item.category) as g where
  inv.date.year = 2000;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...h, group(inv.item.product_name ??? , inv.item.brand_name, inv.ite...
  ```

### `other`

- `trilogy run query01.preql`

  ```text
  Unable to import '.\store_returns.preql': [Errno 2] No such
  file or directory: '.\\store_returns.preql'. Did you mean: raw.store_returns?
  ```
- `trilogy run query03.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 2 column 94 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run -e duckdb query09.preql --debug`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing
  env file: duckdb
  ```
- `trilogy run query11.preql`

  ```text
  All arguments to coalesce must be of the same type, have
  {<DataType.FLOAT: 'float'>, <DataType.INTEGER: 'int'>} for
  ```
- `trilogy run query11.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query11.preql --debug`

  ```text
  maximum recursion depth exceeded
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
      return list(self.parse_text_generator(command, persist=persist, root=root))
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\executor.py", line
  733, in parse_text_gene
  …
  turn self.__repr__()
             ~~~~~~~~~~~~~^^
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\core\models\build.py",
  line 704, in __repr__
      return f"{str(self.left)} {self.operator.value} {str(self.right)}"
                ~~~^^^^^^^^^^^
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\core\models\build.py",
  line 893, in __str__
      return self.__repr__()
             ~~~~~~~~~~~~~^^
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\core\models\build.py",
  line 885, in __repr__
      left = str(self.left)
  RecursionError: maximum recursion depth exceeded
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 33 (char 32). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 35 (char 34). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw/inventory:inv select avg(inv.quantity_on_hand) by (inv.item.category, inv.item.class, inv.item.brand_name, inv.item.product_name) as… g2, grouping(inv.item.product_name) as g1 where inv.date.year = 2000 order by g4 desc, g3 desc, g2 desc, g1 desc, avg_qoh asc nulls first limit 100;`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING child
  "COALESCE(cheerful.inv_item_category, questionable.inv_item_category)" must be
  a grouping column

  LINE 52:     grouping(coalesce("cheerful"."inv_item_category","questionable...
               ^
  [SQL:
  WITH
  cooperative as (
  SELECT
      "inv_warehouse_inventory"."inv_date_sk" as "inv_date_id",
      "inv_warehouse_inventory"."inv_item_sk" as "inv_item_id"
  FROM
      "inventory" as "inv_warehouse_inventory"
  GROUP BY
      1,
      2),
  cheerful as (
  SELECT
      "inv_item_items"."I_BRAND" as "inv_item_brand_name",
      "inv_item_items"."I_CATEGORY" as "
  …
  ame")) as "g1"
  FROM
      "cheerful"
      FULL JOIN "questionable" on "cheerful"."inv_item_brand_name" =
  "questionable"."inv_item_brand_name" AND "cheerful"."inv_item_category" is not
  distinct from "questionable"."inv_item_category" AND
  "cheerful"."inv_item_class" is not distinct from
  "questionable"."inv_item_class" AND "cheerful"."inv_item_product_name" =
  "questionable"."inv_item_product_name"
  GROUP BY
      1
  ORDER BY
      "g4" desc,
      "g3" desc,
      "g2" desc,
      "g1" desc,
      "cheerful"."avg_qoh" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --all-rows --import raw/inventory:inv select coalesce(inv.item.product_name, 'ALL PRODUCTS') as product_name, coalesce(inv.item.brand_name, 'ALL … g2, grouping(inv.item.product_name) as g1 where inv.date.year = 2000 order by g4 desc, g3 desc, g2 desc, g1 desc, avg_qoh asc nulls first limit 100;`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING child
  "inv_item_brand_name" must be a grouping column

  LINE 64:     grouping("questionable"."inv_item_brand_name") as "g2",
               ^
  [SQL:
  WITH
  cooperative as (
  SELECT
      "inv_warehouse_inventory"."inv_date_sk" as "inv_date_id",
      "inv_warehouse_inventory"."inv_item_sk" as "inv_item_id"
  FROM
      "inventory" as "inv_warehouse_inventory"
  GROUP BY
      1,
      2),
  cheerful as (
  SELECT
      avg("inv_warehouse_inventory"."inv_quantity_on_hand") as "avg_qoh",
      coalesce("inv_item_items"."I_BRAND",'ALL BRANDS') as "brand",
      coalesce("inv
  …
  s "avg_qoh",
      "abundant"."g4" as "g4",
      "abundant"."g3" as "g3",
      "abundant"."g2" as "g2",
      "abundant"."g1" as "g1"
  FROM
      "abundant"
      INNER JOIN "cheerful" on "abundant"."brand" = "cheerful"."brand" AND
  "abundant"."category" = "cheerful"."category" AND "abundant"."class" =
  "cheerful"."class" AND "abundant"."product_name" = "cheerful"."product_name"
  ORDER BY
      "abundant"."g4" desc,
      "abundant"."g3" desc,
      "abundant"."g2" desc,
      "abundant"."g1" desc,
      "cheerful"."avg_qoh" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `cli-misuse`

- `trilogy trilogy explore raw/web_sales.preql --show datasources --grep date`

  ```text
  No such command 'trilogy'.
  ```
- `trilogy read_file raw/web_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy trilogy run query13.preql`

  ```text
  No such command 'trilogy'.
  ```
- `trilogy read_file raw/catalog_sales.preql`

  ```text
  No such command 'read_file'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.item.category, avg(store_sales.item.current_price) as avg_price, count(store_sales.item.id) where store_sales.date.month_of_year=1 and store_sales.date.year=2001 limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...e, count(store_sales.item.id) ??? where store_sales.date.month_o...
  ```
- `trilogy run --import raw/item.preql:item select item.category, avg(item.current_price) as avg_price, count(item.id) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
   as avg_price, count(item.id) ??? limit 5;
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;

where store_sales.return_reason.id = 1

select
    simple_case(
        count(…store_sales.quantity between 81 and 100),
        avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)
    ) as bucket_81_100
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
     ) as bucket_81_100 limit 1; ???

  Write stats: received 1561 chars / 1561 bytes; tail: …'quantity between 81 and
  100)\\n    ) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.item.category, sum(catalog_sales.ext_sales_price) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...atalog_sales.ext_sales_price) ??? limit 5;
  ```

### `undefined-concept`

- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web_sales.ext_list_price. Suggestions: ['web_sales.list_price',
  'store_sales.ext_list_price', 'web_sales.ext_sales_price']")
  ```
- `trilogy run --import raw/store_sales:store_sales select year(store_sales.date.date) as yr, count(ticket_number) as cnt order by yr limit 10;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  local.ticket_number. Suggestions: ['store_sales.ticket_number']")
  ```

### `join-resolution`

- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.warehouse_id, catalog_sales.order_number limit 10;`

  ```text
  No datasource exists for root concept
  catalog_sales.warehouse_id@Grain<catalog_sales.warehouse_id>, and no resolvable
  pseudonyms found from set(). This query is unresolvable from your environment.
  Check your datasources and imports to make sure this concept is bound.
  ```

### `file-not-found`

- `trilogy run query20.preql`

  ```text
  Input 'query20.preql' does not exist.
  ```
