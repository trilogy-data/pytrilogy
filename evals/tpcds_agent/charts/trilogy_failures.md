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
  refused to write 'query09.preql': not syntactically valid Trilogy.

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
    category asc,
    class asc,
    item_code asc,
    description asc,
    pct_of_class asc
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...em total extended sales price ??? auto item_total <- sum(web_sal...

  Write stats: received 959 chars / 959 bytes; tail: …'de asc,\\n    description
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
    category asc,
    class asc,
    item_code asc,
    description asc,
    pct_of_class asc
limit 100;`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

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
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 17, column 48.
  Expected one of:
          * _TERMINATOR

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
- `trilogy `

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
  (UndefinedConceptException(...), "Undefined concept:
  item.current_price. Suggestions: ['store_sales.item.current_price',
  'store_sales.promotion.item.current_price']")
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
- `trilogy run query19.preql`

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
