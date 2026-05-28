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
- `trilogy `

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
  refused to write 'query05.preql': not syntactically valid Trilogy.

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
  refused to write 'query14.preql': not syntactically valid Trilogy.

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
          * _TERMINATOR

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

### `undefined-concept`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  billing_customer.preferred_cust_flag. Suggestions:
  ['store_sales.billing_customer.preferred_cust_flag',
  'store_sales.return_customer.preferred_cust_flag',
  'store_sales.billing_customer.first_name']")
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
  category.')
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
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.intersecting_zips. Valid:
  'STRING'.
  ```

### `cli-misuse`

- `trilogy explore raw/physical_sales.preql --show imports,datasources`

  ```text
  Invalid value for '--show': 'imports,datasources' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```
