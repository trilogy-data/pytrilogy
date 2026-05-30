# Trilogy failure analysis — 20260531-020357

- Run `20260531-020356_base` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 441 | failed: 57 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 24 | 42% |
| `other` | 14 | 25% |
| `join-resolution` | 8 | 14% |
| `syntax-missing-alias` | 5 | 9% |
| `undefined-concept` | 4 | 7% |
| `cli-misuse` | 1 | 2% |
| `file-not-found` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.store_returns as store_returns;

# Merge sold_date dimension between web_sales and cat…_sales,
    coalesce(fri_sales, 0) as fri_sales,
    coalesce(sat_sales, 0) as sat_sales
order by ws.sold_date.year, ws.sold_date.week_seq
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 9, column 71.
  Expected one of:
          * _TERMINATOR

  Location:
  ...alesce(cs.ext_sales_price, 0) ??? as combined_ext_sales_price;

  Write stats: received 1732 chars / 1732 bytes; tail: …'order by
  ws.sold_date.year, ws.sold_date.week_seq\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Merge sold_date dimension between web_sales and cat…_sales,
    coalesce(fri_sales, 0) as fri_sales,
    coalesce(sat_sales, 0) as sat_sales
order by ws.sold_date.year, ws.sold_date.week_seq
limit 100;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 11, column 79.
  Expected one of:
          * _TERMINATOR

  Location:
  ...te.week_seq, ws.sold_date.year ??? , (ws.sold_date.dow = 0); auto...

  Write stats: received 1543 chars / 1543 bytes; tail: …'order by
  ws.sold_date.year, ws.sold_date.week_seq\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;
import raw.date_dim as dd;

merge ws.sold_date.date_sk…_fri, 0) / nullif(coalesce(s01_fri, 0), 0) as fri_ratio,
    coalesce(s02_sat, 0) / nullif(coalesce(s01_sat, 0), 0) as sat_ratio
order by aligned_ws;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 11, column 87.
  Expected one of:
          * WHEN

  Location:
  ...year = 2002, dd.week_seq - 53) ??? ;  # 2001 sales by aligned_ws

  Write stats: received 2215 chars / 2215 bytes; tail: …'f(coalesce(s01_sat, 0),
  0) as sat_ratio\\norder by aligned_ws;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;
import raw.date_dim as dd;

merge ws.sold_date.date_sk…_fri, 0) / nullif(coalesce(s01_fri, 0), 0) as fri_ratio,
    coalesce(s02_sat, 0) / nullif(coalesce(s01_sat, 0), 0) as sat_ratio
order by aligned_ws;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 12, column 38.
  Expected one of:
          * WHEN

  Location:
  ...year = 2002, dd.week_seq - 53) ??? ;  auto s01_sun <- sum(daily_s...

  Write stats: received 2081 chars / 2081 bytes; tail: …'f(coalesce(s01_sat, 0),
  0) as sat_ratio\\norder by aligned_ws;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write _check_syntax.preql --content import raw.date_dim as dd;
auto test <- case(
    dd.year = 2001 when dd.week_seq,
    dd.year = 2002 when dd.week_seq - 53);
select test, dd.year, dd.week_seq
where dd.year in (2001, 2002)
limit 5;`

  ```text
  refused to write '_check_syntax.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 3, column 20.
  Expected one of:
          * RPAR
          * COMMA

  Location:
  ...t <- case(     dd.year = 2001 ??? when dd.week_seq,     dd.year

  Write stats: received 198 chars / 198 bytes; tail: …' dd.year,
  dd.week_seq\\nwhere dd.year in (2001, 2002)\\nlimit 5;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write _check_syntax.preql --content import raw.date_dim as dd;
auto test <- case(dd.year = 2001, dd.week_seq, dd.year = 2002, dd.week_seq - 53);
select test, dd.year, dd.week_seq
where dd.year in (2001, 2002)
limit 5;`

  ```text
  refused to write '_check_syntax.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 2, column 81.
  Expected one of:
          * WHEN

  Location:
  ...year = 2002, dd.week_seq - 53) ??? ; select test, dd.year, dd.wee...

  Write stats: received 181 chars / 181 bytes; tail: …' dd.year,
  dd.week_seq\\nwhere dd.year in (2001, 2002)\\nlimit 5;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write _check_syntax.preql --content import raw.date_dim as dd;
auto test <- simple_case(dd.year, 2001, dd.week_seq, 2002, dd.week_seq - 53);
select test, dd.year, dd.week_seq
where dd.year in (2001, 2002)
limit 5;`

  ```text
  refused to write '_check_syntax.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '(dd.year, 2001, dd.week_seq,
  2002, dd.week_seq - 53);\nselect test, dd.year, dd.week_seq\nwhere dd.year in
  (2001, 2002)\nlimit 5;') at line 2, column 25.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...s dd; auto test <- simple_case ??? (dd.year, 2001, dd.week_seq, 2...

  Write stats: received 177 chars / 177 bytes; tail: …' dd.year,
  dd.week_seq\\nwhere dd.year in (2001, 2002)\\nlimit 5;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…date and '2000-09-06'::date
  and wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date

order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 17, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...'::date  # Per-outlet metrics ??? auto store_sales_val <- sum(ss...

  Write stats: received 2926 chars / 2926 bytes; tail: …"9-06'::date\\n\\norder
  by channel, outlet nulls first\\nlimit 100;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…ate
  and wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date
group by channel, outlet
order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it.
  Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate
  at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g.
  `sum(sales.amount) by sales.store.id`).
  Location:
  ...::date and '2000-09-06'::date ??? group by channel, outlet union...

  Write stats: received 1455 chars / 1455 bytes; tail: …'nnel, outlet\\norder by
  channel, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…2000-08-23'::date and '2000-09-06'::date) - sum(sr.net_loss ? sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as profit
limit 10;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 15, column 3.
  Expected one of:
          * _TERMINATOR

  Location:
   store_sales.store.store_sk   ??? where store_sales.date_dim.dat...

  Write stats: received 3810 chars / 3810 bytes; tail: …'d end_date\\n)\\norder
  by channel, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --debug --param zips=39 --import raw.store_sales:store_sales select substring(store_sales.store.zip,1,2) as pref, count(store_sales.customer.customer_sk ? store_sales.customer.preferred_cust_flag = 'Y') by substring(store_sales.customer.customer_address.zip,1,2) as cnt_by_pref_prefix limit 10;`

  ```text
  …
     concat('store_',
  ss.store.store_id) as outlet,\n    sum(ss.ext_sales_price ? ss.date_dim.date
  between '2000-08-23'::date and '2000-09-06'::date) as total_sales,\n
  sum(sr.return_amt ? sr.date_dim.date between '2000-08-23'::date and
  '2000-09-06'::date) as total_returns,\n    sum(ss.net_profit ? ss.date_dim.date
  between '2000-08-23'::date and '2000-09-06'::date) - sum(sr.net_loss ?
  sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) as
  profit\nlimit 10;") at line 19, column 123.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'concat')]

  Location:
   '2000-09-06'::date) by concat ??? ('store_', ss.store.store_id);...

  Write stats: received 1638 chars / 1638 bytes; tail: …"000-08-23'::date and
  '2000-09-06'::date) as profit\\nlimit 10;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.item as item;
import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as…oncat('store_', ss.store.store_id) as outlet,
    ss_sales as total_sales,
    ss_returns as total_returns,
    ss_prof - sr_loss as profit
limit 10;`

  ```text
  …
   ss_prof <- sum(ss.net_profit ? ss.date_dim.date
  between '2000-08-23'::date and '2000-09-06'::date) by concat('store_',
  ss.store.store_id);\nauto sr_loss <- sum(sr.net_loss ? sr.date_dim.date between
  '2000-08-23'::date and '2000-09-06'::date) by concat('store_',
  sr.store.store_id);\n\nselect\n    'store' as channel,\n    concat('store_',
  ss.store.store_id) as outlet,\n    ss_sales as total_sales,\n    ss_returns as
  total_returns,\n    ss_prof - sr_loss as profit\nlimit 10;") at line 19, column
  120.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'concat')]

  Location:
   '2000-09-06'::date) by concat ??? ('store_', ss.store.store_id);...

  Write stats: received 1294 chars / 1294 bytes; tail: …' as total_returns,\\n
  ss_prof - sr_loss as profit\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

merge sr.item.item_sk into ~ss.item.item_sk;
merge ….net_loss ? sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) by rollup (concat('store_', sr.store.store_id)) as net_los
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('CONCAT', 'concat') at line 10, column 109.
  Expected one of:
          * RPAR
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...2000-09-06'::date) by rollup ( ??? concat('store_', ss.store.stor...

  Write stats: received 869 chars / 869 bytes; tail: …" (concat('store_',
  sr.store.store_id)) as net_los\\nlimit 100;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Count preferred customers per ZIP code
auto pref_cust_zip_count <- count(customer.custom…ix_arr

select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 87.
  Expected one of:
          * RPAR

  Location:
  ...r.customer_address.zip, 1, 2) ??? where pref_cust_zip_count > 10...

  Write stats: received 841 chars / 841 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Preferred customer ZIPs with count > 10
# Count preferred customers per ZIP
auto pref_cu…efixes

select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 79.
  Expected one of:
          * _TERMINATOR

  Location:
  ...r.customer_address.zip, 1, 2) ??? where pref_cust_zip_count > 10...

  Write stats: received 787 chars / 787 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.store_sales as store_sales;

parameter zips string;

# Count preferred customers per ZIP code
auto pref…,|$)')

select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 12, column 84.
  Expected one of:
          * _TERMINATOR

  Location:
  ...r.customer_address.zip, 1, 2) ??? where pref_cust_zip_count > 10...

  Write stats: received 1092 chars / 1092 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as customer;
import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sale…r.customer_demographics.dep_count,
  customer.customer_demographics.dep_employed_count,
  customer.customer_demographics.dep_college_count
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 11, column 3.
  Expected one of:
          * SELECT

  Location:
   in ss.customer.customer_sk   ??? where ss.date_dim.year = 2002

  Write stats: received 1882 chars / 1882 bytes; tail: …'
  customer.customer_demographics.dep_college_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query10.preql --content import raw.customer as c;
import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs…dit_rating,
  c.customer_demographics.dep_count,
  c.customer_demographics.dep_employed_count,
  c.customer_demographics.dep_college_count
limit 100;`

  ```text
  refused to write 'query10.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 46.
  Expected one of:
          * SELECT

  Location:
  ...sk in ss.customer.customer_sk ??? where ss.date_dim.year = 2002

  Write stats: received 1593 chars / 1593 bytes; tail: …'ount,\\n
  c.customer_demographics.dep_college_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as web_sales;

# Filter: items in 'Sports', 'Books', or 'Home' categories
# Sold between 1999…/ class_total * 100 as pct_of_class
order by
    store_returns.customer.customer_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...em total extended sales price ??? auto item_total <- sum(web_sal...

  Write stats: received 959 chars / 959 bytes; tail: …'de asc,\\n    description
  asc,\\n    pct_of_class asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql -e -c import raw.store_returns as store_returns;

where web_sales.item.category in ('Sports', 'Books', 'Home')
  and web_sales.s…/ class_total * 100 as pct_of_class
order by
    category asc,
    class asc,
    item_code asc,
    description asc,
    pct_of_class asc
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '1999-03-24'::date  ??? auto item_total <- sum(web_sal...

  Write stats: received 759 chars / 759 bytes; tail: …'de asc,\\n    description
  asc,\\n    pct_of_class asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as web_sales;

where web_sales.item.category in ('Sports', 'Books', 'Home')
  and web_sales.s…/ class_total * 100 as pct_of_class
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
  ...e.date <= '1999-03-24'::date  ??? auto item_total <- sum(web_sal...

  Write stats: received 784 chars / 784 bytes; tail: …'de asc,\\n    description
  asc,\\n    pct_of_class asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.item as item;
import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import …store_sales.item.brand_id asc nulls first,
    store_sales.item.class_id asc nulls first,
    store_sales.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 17, column 48.
  Expected one of:
          * SELECT

  Location:
  ...o <- store_sales.item.brand_id ??? , store_sales.item.class_id, s...

  Write stats: received 3002 chars / 3002 bytes; tail: …'\\n
  store_sales.item.category_id asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.fy_year, store_sales.date_dim.qoy, store_sales.date_dim.quarter_name, count(stor…year = 2001 group by store_sales.date_dim.fy_year, store_sales.date_dim.qoy, store_sales.date_dim.quarter_name order by store_sales.date_dim.qoy asc;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sales.date_dim.fy_year = 2001 ??? group by store_sales.date_dim....
  ```
- `trilogy file write query18.preql --content import raw.catalog_sales as catalog_sales;

# Filter: year 1998, billing demographic filters, birth month, home ad…tomer_address.county asc nulls first,
    catalog_sales.item.item_id asc nulls first,
    g_country asc,
    g_state asc,
    g_county asc
limit 100;`

  ```text
  refused to write 'query18.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 11, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...'VA')  # Roll up aggregations ??? def by_geo_item(metric) -> avg...

  Write stats: received 2757 chars / 2757 bytes; tail: …' g_country asc,\\n
  g_state asc,\\n    g_county asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import raw.catalog_sales as catalog_sales;

# Filter: items in Sports, Books, or Home categories, sold between 199…first,
    catalog_sales.item.item_id asc nulls first,
    catalog_sales.item.item_desc asc nulls first,
    pct_of_class asc nulls first
limit 100;
`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...em total extended sales price ??? auto item_ext_sales <- sum(cat...

  Write stats: received 1197 chars / 1197 bytes; tail: …'sc nulls first,\\n
  pct_of_class asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `other`

- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.outlet', 'local._virt_filter_net_loss_5060387553640372'} out
  of  with found {'local._virt_filter_net_loss_5060387553640372',
  'sr.item.item_sk', 'sr.ticket_number',
  'local._virt_filter_return_amt_1494936058274708'}
  ```
- `trilogy run query07.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Have
  {'GroupNode<c.customer_address.zip,local.pref_cust_zip_count>': None,
  'MergeNode<store_sales.date_dim.qoy,store_sales.date_dim.year,store_sales.item.
  item_sk...4 more>': None} and need store_sales.date_dim.year = 1998 and
  store_sales.date_dim.qoy = 2 and local.pref_cust_zip_count > 10 and
  BuildSubselectComparison(left=substring(store_sales.store.zip@Grain<store_sales
  .store.store_sk>,1,2),
  right=local._virt_func_substring_846777743695943@Grain<c.customer_address.addre
  ss_sk>, operator=<ComparisonOperator.IN: 'in'>) and
  regexp_contains(:local_zips,concat(concat((^|,),local.store_zip_prefix@Grain<st
  ore_sales.store.store_sk>),[0-9]{3}(,|$)))
  ```
- `trilogy run query09.preql`

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 45 column 12 (char 2540). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query10.preql`

  ```text
  HAVING references 'ss.customer.customer_sk',
  'ss.date_dim.year', 'ss.date_dim.moy', 'ws.bill_customer.customer_sk',
  'ws.sold_date.year', 'cs.ship_customer.customer_sk', 'cs.sold_date.year', which
  are not in the SELECT projection (line 12). Add them to SELECT, each prefixed
  with `--` so they stay out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --ss.customer.customer_sk,
  --ss.date_dim.year, --ss.date_dim.moy, --ws.bill_customer.customer_sk,
  --ws.sold_date.year, --cs.ship_customer.customer_sk, --cs.sold_date.year
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query10.preql`

  ```text
  HAVING references 'ws.bill_customer.customer_sk',
  'cs.ship_customer.customer_sk', which are not in the SELECT projection (line
  7). Add them to SELECT, each prefixed with `--` so they stay out of the output
  rows — keep your HAVING as-is:
      select <your existing columns>, --ws.bill_customer.customer_sk,
  --cs.ship_customer.customer_sk
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query10.preql`

  ```text
  HAVING references 'local.has_store_sale',
  'local.has_web_sale', 'local.has_catalog_sale', which are not in the SELECT
  projection (line 16). Add them to SELECT, each prefixed with `--` so they stay
  out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.has_store_sale,
  --local.has_web_sale, --local.has_catalog_sale
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query10.preql`

  ```text
  HAVING references 'ws.sold_date.year', 'cs.sold_date.year',
  which are not in the SELECT projection (line 10). Add them to SELECT, each
  prefixed with `--` so they stay out of the output rows — keep your HAVING
  as-is:
      select <your existing columns>, --ws.sold_date.year, --cs.sold_date.year
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query10.preql`

  ```text
  HAVING clause aggregate `count(<Filter: ref:ws.sold_date.year
  where ref:ws.sold_date.year = 2002>) by ss.customer.customer_sk` is not in the
  SELECT projection (line 10). HAVING can only filter on off-grain or nested
  aggregates that are also computed in the SELECT. Fix one of: (a) add it to
  SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ...,
  --count(<Filter: ref:ws.sold_date.year where ref:ws.sold_date.year = 2002>) by
  ss.customer.customer_sk`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  HAVING references 'local.ws_2002_rev', 'local.ws_2001_rev',
  'local.ss_2002_rev', 'local.ss_2001_rev', which are not in the SELECT
  projection (line 16). Add them to SELECT, each prefixed with `--` so they stay
  out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.ws_2002_rev, --local.ws_2001_rev,
  --local.ss_2002_rev, --local.ss_2001_rev
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_avg_sale_value', which is
  not in the SELECT projection (line 44). Add it to SELECT, each prefixed with
  `--` so it stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale_value
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_avg', which is not in the
  SELECT projection (line 46). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_avg', which is not in the
  SELECT projection (line 84). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```

### `syntax-parse`

- `trilogy run query05.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.outlet<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query06.preql`

  ```text
  Could not resolve connections for query with output
  ['local.state<Purpose.PROPERTY>Derivation.BASIC>',
  'local.line_item_count<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['ss.customer.customer_demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'ss.customer.customer_demographics.marital_status<Purpose.PROPERTY>Derivation.R
  OOT>',
  'ss.customer.customer_demographics.education_status<Purpose.PROPERTY>Derivation
  .ROOT>',
  'ss.customer.customer_demographics.purchase_estimate<Purpose.PROPERTY>Derivatio
  n.ROOT>',
  'ss.customer.customer_demographics.credit_rating<Purpose.PROPERTY>Derivation.RO
  OT>',
  'ss.customer.customer_demographics.dep_count<Purpose.PROPERTY>Derivation.ROOT>'
  ,
  'ss.customer.customer_demographics.dep_employed_count<Purpose.PROPERTY>Derivati
  on.ROOT>',
  'ss.customer.customer_demographics.dep_college_count<Purpose.PROPERTY>Derivatio
  n.ROOT>', 'local.customer_count_1<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_2<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_3<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_4<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_5<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_6<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['c.customer_demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'c.customer_demographics.marital_status<Purpose.PROPERTY>Derivation.ROOT>',
  'c.customer_demographics.education_status<Purpose.PROPERTY>Derivation.ROOT>',
  'c.customer_demographics.purchase_estimate<Purpose.PROPERTY>Derivation.ROOT>',
  'c.customer_demographics.credit_rating<Purpose.PROPERTY>Derivation.ROOT>',
  'c.customer_demographics.dep_count<Purpose.PROPERTY>Derivation.ROOT>',
  'c.customer_demographics.dep_employed_count<Purpose.PROPERTY>Derivation.ROOT>',
  'c.customer_
  …
  .ROOT>',
  'local.has_store_sale<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.has_web_sale<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.has_catalog_sale<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_1<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_2<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_3<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_4<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_5<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_6<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query11.preql duckdb`

  ```text
  Could not resolve connections for query with output
  ['local.customer_code<Purpose.PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.preferred_cust_flag<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select count(ss.ticket_number) as cnt where ss.date_dim.fy_year = 2001 and ss.date_dim.qoy = 1 and sr.date_dim.fy_year = 2001 and sr.date_dim.qoy in (1,2,3);`

  ```text
  Could not resolve connections for query with output
  ['local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr --import raw.catalog_sales:cs merge ss.item.item_sk into sr.item.item_sk; merge ss.cust…1 and sr.date_dim.qoy in (1, 2, 3) and ss.item.item_sk in cs.item.item_sk and cs.sold_date.fy_year = 2001 and cs.sold_date.qoy in (1, 2, 3) limit 10;`

  ```text
  Could not resolve connections for query with output
  ['ss.item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'ss.item.item_desc<Purpose.PROPERTY>Derivation.ROOT>',
  'ss.store.state<Purpose.PROPERTY>Derivation.ROOT>',
  'local.ss_qty<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.sr_qty<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr --import raw.catalog_sales:cs merge ss.item.item_sk into sr.item.item_sk; merge ss.cust….item.item_sk and cs.sold_date.fy_year = 2001 and cs.sold_date.qoy in (1, 2, 3) and ss.customer.customer_sk in cs.bill_customer.customer_sk limit 10;`

  ```text
  Could not resolve connections for query with output
  ['ss.item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'ss.item.item_desc<Purpose.PROPERTY>Derivation.ROOT>',
  'ss.store.state<Purpose.PROPERTY>Derivation.ROOT>',
  'local.ss_qty<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.sr_qty<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cs_qty<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `syntax-missing-alias`

- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

merge sr.item.item_sk into ~ss.item.item_sk;
merge …sr.net_loss ? sr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date) by rollup concat('store_', sr.store.store_id) as net_los
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `sum(sr.net_loss ? sr.date_dim.date between '2000-08-23'::date
  and '2000-09-06'::date) by rollup concat('store_', sr.store.store_id) as
  store_id_sum`
  Location:
  ...tore_id) as net_los limit 100; ???

  Write stats: received 861 chars / 861 bytes; tail: …"up concat('store_',
  sr.store.store_id) as net_los\\nlimit 100;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/customer_demographics:cd select distinct cd.education_status;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct cd.education_status as
  distinct_cd_education_status`
  Location:
  ...aphics as cd; select distinct ??? cd.education_status;
  ```
- `trilogy run --import raw/customer_demographics:cd select distinct cd.education_status as edu_status;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct cd.education_status as
  distinct_cd_education_status`
  Location:
  ...aphics as cd; select distinct ??? cd.education_status as edu_sta...
  ```
- `trilogy file write query11.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;

# Merge customer keys across the two fact models
merge …1 else ss_2001_rev end

order by customer_code nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `Growth rates
      -- (ss_2002_rev - ss_2001_rev) / nullif(ss_2001_rev, 0) as
  growth_rates_ss_2002_rev_ss_2001_rev_nul`
  Location:
  ..._rev,     -- Growth rates     ??? -- (ss_2002_rev - ss_2001_rev)...

  Write stats: received 1743 chars / 1743 bytes; tail: …'name nulls first,
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.item as item;
import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import …store_sales.item.brand_id asc nulls first,
    store_sales.item.class_id asc nulls first,
    store_sales.item.category_id asc nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `ss_nov_sales by rollup store_sales.item.brand_id as
  ss_nov_sales_by_rollup_store_sales_item_`
  Location:
  ...S' as ch1,     ss_nov_sales by ??? rollup store_sales.item.brand...

  Write stats: received 5520 chars / 5520 bytes; tail: …'\\n
  store_sales.item.category_id asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `file-not-found`

- `trilogy run query06.preql`

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
  store_sales.customer.preferred_cust_flag.')
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.store_returns:store_returns --import raw.catalog_sales:catalog_sales --import raw.catalog_retur…ns --import raw.web_sales:web_sales --import raw.web_returns:web_returns select store_sales.store.store_id, sum(store_sales.ext_sales_price) limit 5;`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_sales.date_dim.month. Suggestions: ['store_sales.date_dim.month_seq',
  'store_sales.date_dim.moy', 'store_sales.date_dim.qoy']")
  ```

### `cli-misuse`

- `trilogy run - --import raw.date_dim:date_dim select date_dim.dow, date_dim.day_name, date_dim.date limit 7;`

  ```text
  'select date_dim.dow, date_dim.day_name, date_dim.date limit 7;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```

### `file-not-found`

- `trilogy run query10.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  ws_bill_customer_customer does not exist!
  Did you mean "customer"?

  LINE 10: ... ws_bill_customer_customer."ws_bill_customer_customer_sk" from
  ws_bill_customer_customer where ws_bill_customer_customer...
                                                                             ^
  [SQL:
  WITH
  cooperative as (
  SELECT
      "ss_store_sales"."ss_customer_sk" as "ss_customer_customer_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_dim_date_sk"
  FROM
      "store_sales" as "ss_store_sales"
  WHERE
      ( "ss_store_sales"."ss_customer_sk"
  …
  er_customer_demographics_gender" asc,
      "uneven"."ss_customer_customer_demographics_marital_status" asc,
      "uneven"."ss_customer_customer_demographics_education_status" asc,
      "uneven"."ss_customer_customer_demographics_purchase_estimate" asc,
      "uneven"."ss_customer_customer_demographics_credit_rating" asc,
      "uneven"."ss_customer_customer_demographics_dep_count" asc,
      "uneven"."ss_customer_customer_demographics_dep_employed_count" asc,
      "uneven"."ss_customer_customer_demographics_dep_college_count" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
