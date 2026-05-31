# Trilogy failure analysis — 20260531-035152

- Run `20260531-035151_base` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 2243 | failed: 221 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 78 | 35% |
| `other` | 63 | 29% |
| `undefined-concept` | 33 | 15% |
| `syntax-missing-alias` | 19 | 9% |
| `join-resolution` | 17 | 8% |
| `file-not-found` | 8 | 4% |
| `type-error` | 2 | 1% |
| `cli-misuse` | 1 | 0% |

## Detail

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.store_returns as store_returns;

# Merge sold_date dimension between web_sales and cat…_sales,
    coalesce(fri_sales, 0) as fri_sales,
    coalesce(sat_sales, 0) as sat_sales
order by ws.sold_date.year, ws.sold_date.week_seq
limit 100;`

  ```text
  refused to write 'query02_check.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it.
  Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate
  at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g.
  `sum(sales.amount) by sales.store.id`).
  Location:
  ...ws, count(dd.date_sk) as days ??? group by dd.year having dd.yea...

  Write stats: received 200 chars / 200 bytes; tail: …'r\\nhaving dd.year between
  2000 and 2003\\norder by dd.year asc;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

# Merge sold_date dimensions so bot…te.week_seq
    where web_sales.sold_date.dow = 0 and web_sales.sold_date.year = 2001;

select web_sales.sold_date.week_seq as wk, sun_2001
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

  Location:
  ..._sales.sold_date.week_seq     ??? where web_sales.sold_date.dow

  Write stats: received 567 chars / 567 bytes; tail: …'elect
  web_sales.sold_date.week_seq as wk, sun_2001\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02_check.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;
merge catalog_sales.sold_date.…ly_sales,
    prev_yr_sales
where web_sales.sold_date.year = 2002 and web_sales.sold_date.dow = 0
order by web_sales.sold_date.week_seq asc
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
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...old_date.year in (2001, 2002) ??? auto daily_sales <- sum(web_sa...

  Write stats: received 855 chars / 855 bytes; tail: …'.dow = 0\\norder by
  web_sales.sold_date.week_seq asc\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

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
  ...ld_date.year in (2001, 2002)  ??? auto prev_yr_sales <- lag(dail...

  Write stats: received 1717 chars / 1717 bytes; tail: …'_ratio\\norder by
  week_sequence_2001 asc nulls first\\nlimit 60;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…t_flag
order by customer_code asc nulls first, first_name asc nulls first, last_name asc nulls first, preferred_cust_flag asc nulls first
limit 100;
`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 28, column 22.
  Expected one of:
          * SELECT

  Location:
  ...2001 > 0   and web_yt_2001 > 0 ??? ;  # Ratios (only when 2001 >

  Write stats: received 2567 chars / 2567 bytes; tail: …'nulls first,
  preferred_cust_flag asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…01 > 0
order by customer_code asc nulls first, first_name asc nulls first, last_name asc nulls first, preferred_cust_flag asc nulls first
limit 100;
`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 11, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('IDENTIFIER', 'web_sales.bill_customer.customer_id')]

  Location:
  ...customer per-channel per-year ??? auto store_yt_2001 <- sum((sto...

  Write stats: received 1262 chars / 1262 bytes; tail: …'nulls first,
  preferred_cust_flag asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

merge store_returns.ticket_numb…re,
    @sales_rollup_s(store_sales.net_profit) - @sales_rollup_s(store_returns.net_loss) as profit_store
order by ch, outlet nulls first
limit 100;
`

  ```text
  …
  sales.store.store_id);\n\nwhere store_sales.date_dim.date between
  '2000-08-23'::date and '2000-09-06'::date\nselect\n    'store' as ch,\n
  concat('store_', store_sales.store.store_id) as outlet,\n
  @sales_rollup_s(store_sales.ext_sales_price) as sales_store,\n
  @sales_rollup_s(store_returns.return_amt) as returns_store,\n
  @sales_rollup_s(store_sales.net_profit) -
  @sales_rollup_s(store_returns.net_loss) as profit_store\norder by ch, outlet
  nulls first\nlimit 100;\n") at line 8, column 59.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'concat')]

  Location:
  ...> sum(metric) by rollup concat ??? ('store_', store_sales.store.s...

  Write stats: received 781 chars / 781 bytes; tail: …' as profit_store\\norder
  by ch, outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

merge store_returns.ticket_numb…rns.return_amt) as returns_web,
    sum(web_sales.net_profit) - sum(web_returns.net_loss) as profit_web
order by ch3, outlet3 nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 10, column 52.
  Expected one of:
          * SELECT

  Location:
  ...um(metric) by rollup st_outlet ??? , 'store' ;  where store_sales...

  Write stats: received 2085 chars / 2085 bytes; tail: …' as profit_web\\norder
  by ch3, outlet3 nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

merge store_returns.ticket_numb… returns_s,
    @rollup_store(store_sales.net_profit) - @rollup_store(store_returns.net_loss) as profit_s
order by ch, outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "'store', st_outlet;\n\nselect\n
  'store' as ch,\n    st_outlet as outlet,\n
  @rollup_store(store_sales.ext_sales_price) as sales_s,\n
  @rollup_store(store_returns.return_amt) as returns_s,\n
  @rollup_store(store_sales.net_profit) - @rollup_store(store_returns.net_loss)
  as profit_s\norder by ch, outlet nulls first\nlimit 100;\n") at line 12, column
  119.
  Expected one of:
          * LPAR
          * IDENTIFIER
  Previous tokens: [Token('ROLLUP', 'rollup')]

  Location:
  ...'2000-09-06'::date) by rollup ??? 'store', st_outlet;  select

  Write stats: received 878 chars / 878 bytes; tail: …'oss) as profit_s\\norder
  by ch, outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.store_sales as store_sales;

# Preferred customers per customer address ZIP
aut…10
  )

select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 20, column 7.
  Expected one of:
          * RPAR
          * COMMA

  Location:
  ...omer_address.zip, 1, 2)       ??? where store_sales.customer.cus...

  Write stats: received 1045 chars / 1045 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.store_sales as store_sales;

# Count preferred customers per customer address Z…prefix

select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 11, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...stomer_address.zip, 1, 2)     ??? where store_sales.customer.cus...

  Write stats: received 843 chars / 843 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

import raw.store_sales as store_sales;

# Count preferred customers per customer address Z…prefix

select
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "(split(zips, ','),
  store_sales.customer.customer_address.zip);\n\nwhere store_sales.date_dim.year
  = 1998\n  and store_sales.date_dim.qoy = 2\n  and
  substring(store_sales.store.zip, 1, 2) in valid_zip_prefix\n\nselect\n
  store_sales.store.store_name,\n    sum(store_sales.net_profit) as
  total_net_profit\norder by store_sales.store.store_name\nlimit 100;") at line
  13, column 23.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'array_contains')]

  Location:
  ...ip > 10     and array_contains ??? (split(zips, ','), store_sales...

  Write stats: received 881 chars / 881 bytes; tail: …'_net_profit\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.store_name, store_sales.store.zip, sum(store_sales.net_profit) as total_profit wher….year = 1998 and store_sales.date_dim.qoy = 2 and store_sales.store.store_name = 'eing' group by store_sales.store.store_name, store_sales.store.zip;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...les.store.store_name = 'eing' ??? group by store_sales.store.sto...
  ```
- `trilogy file write query12.preql --content import raw.web_sales as ws;

# Filter: items in Sports, Books, or Home categories, sold between dates
where ws.ite…class

order by
    ws.item.category asc,
    ws.item.class asc,
    ws.item.item_id asc,
    ws.item.item_desc asc,
    pct_of_class asc
limit 100;
`

  ```text
  refused to write 'query12.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...extended sales price per item ??? auto item_total_ws <- sum(ws.e...

  Write stats: received 832 chars / 832 bytes; tail: …'\\n    ws.item.item_desc
  asc,\\n    pct_of_class asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…t
having total_sales > overall_avg_sale

order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n\n') at line 65, column 1.
  Expected one of:
          * _TERMINATOR
          * ORDER
          * MERGE
          * LIMIT
  Previous tokens: [Token('IDENTIFIER', 'overall_avg_sale')]

  Location:
  ...tal_sales > overall_avg_sale  ??? union  select     'Catalog Sal...

  Write stats: received 4277 chars / 4277 bytes; tail: …'st, class_id nulls
  first, category_id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…
g_lev = 0
order by store_sales.item.brand_id nulls first, store_sales.item.class_id nulls first, store_sales.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'g_lev ') at line 51, column 1.
  Expected one of:
          * ORDER
          * LIMIT
          * MERGE
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'overall_avg')]

  Location:
  ...ing total_sales > overall_avg ??? g_lev = 0 order by store_sales...

  Write stats: received 3137 chars / 3137 bytes; tail: …'s first,
  store_sales.item.category_id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import raw.catalog_sales as catalog_sales;

where catalog_sales.item.category in ('Sports', 'Books', 'Home')
  and…   pct_of_class as pct_of_class_sales
order by category asc, class asc, item_code asc, description asc, pct_of_class_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '1999-03-24'::date  ??? auto item_total_ext_price <- s...

  Write stats: received 925 chars / 925 bytes; tail: …'scription asc,
  pct_of_class_sales asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/inventory:inv select inv.warehouse.warehouse_name, inv.item.item_id, sum(inv.quantity_on_hand) as total_qoh where inv.date_dim.date_sk between 2451585 and 2451614 group by 1,2 limit 10; duckdb`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...k between 2451585 and 2451614 ??? group by 1,2 limit 10;
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item keys acros…st_name, cs.bill_customer.first_name
order by cs.bill_customer.last_name asc, cs.bill_customer.first_name asc, total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 18, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...1, 30), ss.date_dim.date)     ??? where ss.date_dim.year between...

  Write stats: received 2089 chars / 2089 bytes; tail: …'tomer.first_name asc,
  total_sales asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item keys acros…? cust_total > 0.5 * max_cust_total)
order by cs.bill_customer.last_name asc, cs.bill_customer.first_name asc, total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 17, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...refix, ss.date_dim.date)      ??? where ss.date_dim.year between...

  Write stats: received 1661 chars / 1661 bytes; tail: …'tomer.first_name asc,
  total_sales asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item keys acros…customer.customer_sk in (best_custs)
order by cs.bill_customer.last_name asc, cs.bill_customer.first_name asc, total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 22, column 32.
  Expected one of:
          * _TERMINATOR

  Location:
  ...uto freq_items <- desc_prefix ??? where ss.date_dim.year between...

  Write stats: received 1860 chars / 1860 bytes; tail: …'tomer.first_name asc,
  total_sales asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item keys acros…_customer.customer_sk in (best_cust)
order by cs.bill_customer.last_name asc, cs.bill_customer.first_name asc, total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 20, column 33.
  Expected one of:
          * _TERMINATOR

  Location:
  ...to freq_prefix <- desc_prefix ??? where sold_date_count > 4;  #

  Write stats: received 1633 chars / 1633 bytes; tail: …'tomer.first_name asc,
  total_sales asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item keys acros…_dim.year between 2000 and 2003)
  )
order by cs.bill_customer.last_name asc, cs.bill_customer.first_name asc, total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...(cs.item.item_desc, 1, 30) in ??? (     select substring(ss.item...

  Write stats: received 1263 chars / 1263 bytes; tail: …'tomer.first_name asc,
  total_sales asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Compute frequent desc…_customer.customer_sk in (best_cust)
order by cs.bill_customer.last_name asc, cs.bill_customer.first_name asc, total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...s.item.item_desc, 1, 30)      ??? where count(ss.ticket_number ?...

  Write stats: received 1120 chars / 1120 bytes; tail: …'tomer.first_name asc,
  total_sales asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql --content import raw.store_sales as store_sales;

where store_sales.date_dim.year = 2001
  and store_sales.store.state = 'TN…(store_sales.item.class)    by rollup store_sales.item.category, store_sales.item.class as g_class,
    g_cat + g_class as hierarchy_level
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...for numerator and denominator ??? def rollup_profit(metric) -> s...

  Write stats: received 818 chars / 818 bytes; tail: …'s g_class,\\n    g_cat +
  g_class as hierarchy_level\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query36.preql --content import raw.store_sales as store_sales;

where store_sales.date_dim.year = 2001
  and store_sales.store.state = 'TN…vel desc nulls first,
    case when hierarchy_level = 0 then store_sales.item.category end nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 18, column 10.
  Expected one of:
          * LIMIT
          * MERGE
          * _TERMINATOR

  Location:
  ...el desc nulls first,     case ??? when hierarchy_level = 0 then

  Write stats: received 1122 chars / 1122 bytes; tail: …'d nulls first,\\n
  within_parent_rank nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…
    where web_sales.sold_date.year = 2000;

where ss_combo in cs_combo
  and ss_combo in ws_combo

select count_distinct(ss_combo) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 6, column 135.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._sales.date_dim.date::string) ??? by store_sales.customer.last_n...

  Write stats: received 1138 chars / 1138 bytes; tail: …' ws_combo\\n\\nselect
  count_distinct(ss_combo) as cnt\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query38.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…ate;

where store_sales.date_dim.year = 2000
  and ss_combo in cs_combo
  and ss_combo in ws_combo

select count_distinct(ss_combo) as cnt
limit 100;`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 6, column 135.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._sales.date_dim.date::string) ??? by store_sales.customer.last_n...

  Write stats: received 1029 chars / 1029 bytes; tail: …' ws_combo\\n\\nselect
  count_distinct(ss_combo) as cnt\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/catalog_sales:catalog_sales select concat(store_sales.customer.last_name, '|', store_sales.cust…catalog_sales.bill_customer.last_name, '|', catalog_sales.bill_customer.first_name, '|', catalog_sales.sold_date.date::string) group by key limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...sales.sold_date.date::string) ??? group by key limit 10;
  ```
- `trilogy run - --import raw.inventory:inv duckdb`

  ```text
  --> 3:1
    |
  3 | auto avg_qty <- avg(inv.quantity_on_hand) by inv.warehouse.warehouse_sk,
  inv.item.item_sk, inv.date_dim.moy;
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...here inv.date_dim.year = 2001 ??? auto avg_qty <- avg(inv.quanti...
  ```
- `trilogy file write query44.preql --content import raw.store_sales as store_sales;

# Average net profit by item for store_sk = 1
auto item_avg_net_profit <- …m.product_name as best_name
    --store_sales.item.product_name ? desc_rank = position as worst_name
order by position asc, best_name desc
limit 100;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '--store_sales.item.product_name
  ? desc_rank = position as worst_name\norder by position asc, best_name
  desc\nlimit 100;') at line 26, column 5.
  Expected one of:
          * COMMA
          * HAVING
          * _TERMINATOR
          * LIMIT
          * ORDER
          * MERGE
          * WHERE
          * METADATA
  Previous tokens: [Token('IDENTIFIER', 'best_name')]

  Location:
  ...product_name as best_name     ??? --store_sales.item.product_nam...

  Write stats: received 1201 chars / 1201 bytes; tail: …' worst_name\\norder by
  position asc, best_name desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as store_sales;

# ============================================================
# Core calc…me;

where position is not null

select
    position,
    best_name,
    worst_name
order by position asc, best_name desc, worst_name desc
limit 100;`

  ```text
  …
  nto ~position;\n\n# Now
  position has both best and worst items connected through it\n# For each
  position, best_name = product_name of item with that asc_rank\n# worst_name =
  product_name of item with that desc_rank\n\nauto best_name <-
  store_sales.item.product_name;\nauto worst_name <-
  store_sales.item.product_name;\n\nwhere position is not null\n\nselect\n
  position,\n    best_name,\n    worst_name\norder by position asc, best_name
  desc, worst_name desc\nlimit 100;") at line 27, column 16.
  Expected one of:
          * INTO
  Previous tokens: [Token('WILDCARD_IDENTIFIER', 'asc_rank')]

  Location:
  ...ion = asc_rank merge asc_rank ??? ? item_avg_net_profit > thresh...

  Write stats: received 2046 chars / 2046 bytes; tail: …' by position asc,
  best_name desc, worst_name desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as store_sales;
import raw.store as store;

# =============================================…ere item_avg_net_profit > threshold and position is not null

select
    position,
    store_sales.item.product_name
order by position asc
limit 100;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '? item_avg_net_profit >
  threshold and asc_rank <= 10 into ~position;\n\nwhere item_avg_net_profit >
  threshold and position is not null\n\nselect\n    position,\n
  store_sales.item.product_name\norder by position asc\nlimit 100;') at line 24,
  column 16.
  Expected one of:
          * INTO
  Previous tokens: [Token('WILDCARD_IDENTIFIER', 'asc_rank')]

  Location:
  ...============== merge asc_rank ??? ? item_avg_net_profit > thresh...

  Write stats: received 1411 chars / 1411 bytes; tail:
  …'ore_sales.item.product_name\\norder by position asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as store_sales;

# ============================================================
# Core calc…here item_avg_net_profit > threshold

select
    asc_rank,
    desc_rank,
    store_sales.item.product_name
ordering by asc_rank, desc_rank
limit 20;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'ing ') at line 21, column 6.
  Expected one of:
          * "BY"i
  Previous tokens: [Token('ORDER', 'order')]

  Location:
  ..._sales.item.product_name order ??? ing by asc_rank, desc_rank lim...

  Write stats: received 911 chars / 911 bytes; tail:
  …'.item.product_name\\nordering by asc_rank, desc_rank\\nlimit 20;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as store_sales;

# ============================================================
# Core calc… best_performer_name,
    worst_name_at_pos as worst_performer_name
order by rank asc, best_performer_name desc, worst_performer_name desc
limit 100;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 24, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...product_name) by position     ??? where item_avg_net_profit > th...

  Write stats: received 1627 chars / 1627 bytes; tail: …'st_performer_name desc,
  worst_performer_name desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query49.preql --content import raw.web_sales as ws;
import raw.web_returns as wr;
import raw.catalog_sales as cs;
import raw.catalog_retur…nd store_net_paid > 0 and store_ret_qty > 0 and store_ret_amt > 0
order by channel, return_qty_rank, return_currency_rank, ss.item.item_id
limit 100;`

  ```text
  refused to write 'query49.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('MULTIPLY_DIVIDE_PERCENT', '/') at line 44, column 52.
  Expected one of:
          * RPAR

  Location:
  ...d) over (order by web_ret_qty ??? / web_sold_qty asc) as return_...

  Write stats: received 3774 chars / 3774 bytes; tail: …'n_qty_rank,
  return_currency_rank, ss.item.item_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query50.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

# Merge linking keys so we can match sales to retur…en 91 and 120);
auto bucket_gt_120      <- count(ss.ticket_number ? order_elapsed_days > 120);

where sr.date_dim.year = 2001 and sr.date_dim.moy = 8`

  ```text
  refused to write 'query50.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('$END', '') at line 18, column 53.
  Expected one of:
          * SELECT

  Location:
   = 2001 and sr.date_dim.moy = ??? 8

  Write stats: received 882 chars / 882 bytes; tail: …'120);\\n\\nwhere
  sr.date_dim.year = 2001 and sr.date_dim.moy = 8'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.item_sk as sk, ss.date_dim.date as d, ss.date_dim.year as yr, sum(ss.sales_price) as total by ss.item.item_sk, ss.date_dim.date, ss.date_dim.year having yr = 2000 order by 1,2 limit 10;`

  ```text
  --> 2:107
    |
  2 | select ss.item.item_sk as sk, ss.date_dim.date as d, ss.date_dim.year as
  yr, sum(ss.sales_price) as total by ss.item.item_sk, ss.date_dim.date,
  ss.date_dim.year having yr = 2000 order by 1,2 limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
   sum(ss.sales_price) as total ??? by ss.item.item_sk, ss.date_di...
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
import raw.store_sales as store_sale…count,
    segment * 50 as segment_times_50
order by
    segment asc nulls first,
    cust_count asc nulls first,
    segment_times_50 asc
limit 100;`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...bill_customer.customer_sk     ??? where catalog_sales.sold_date....

  Write stats: received 1982 chars / 1982 bytes; tail: …'t_count asc nulls
  first,\\n    segment_times_50 asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;
import raw.store_sales as store_sale…count,
    segment * 50 as segment_times_50
order by
    segment asc nulls first,
    cust_count asc nulls first,
    segment_times_50 asc
limit 100;`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 30, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ales.customer.customer_sk     ??? where store_sales.customer.cus...

  Write stats: received 1810 chars / 1810 bytes; tail: …'t_count asc nulls
  first,\\n    segment_times_50 asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query57.preql --content import raw.catalog_sales as catalog_sales;

# Monthly total sales
# Filter to Dec 1998, all of 1999, and Jan 2000
…g_sales.item.brand asc,
    catalog_sales.call_center.name asc,
    catalog_sales.sold_date.year asc,
    catalog_sales.sold_date.moy asc
limit 100;
`

  ```text
  …
  _month_total,\n
  seq_num\nhaving catalog_sales.sold_date.year = 1999\n   and avg_monthly_sales >
  0\n   and abs(monthly_total - avg_monthly_sales) / avg_monthly_sales >
  0.1\norder by (monthly_total - avg_monthly_sales) asc nulls first,\n
  catalog_sales.item.category asc,\n    catalog_sales.item.brand asc,\n
  catalog_sales.call_center.name asc,\n    catalog_sales.sold_date.year asc,\n
  catalog_sales.sold_date.moy asc\nlimit 100;\n') at line 34, column 5.
  Expected one of:
          * ORDER
          * "PARTITION"i
          * RPAR
  Previous tokens: [Token('_WINDOW_OVER_PAREN', 'over (')]

  Location:
  ...(monthly_total, 1) over (     ??? --    partition by catalog_sal...

  Write stats: received 2340 chars / 2340 bytes; tail: …'te.year asc,\\n
  catalog_sales.sold_date.moy asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query58.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…g_total between 0.9 * web_total and 1.1 * web_total
order by
    store_sales.item.item_id asc nulls first,
    store_total asc nulls first
limit 100;`

  ```text
  refused to write 'query58.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 19, column 57.
  Expected one of:
          * SELECT

  Location:
  ...m_id in web_sales.item.item_id ??? ;  select     store_sales.item...

  Write stats: received 1633 chars / 1633 bytes; tail: …' asc nulls first,\\n
  store_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query59.preql --content import raw.store_sales as store_sales;

# Per (store, week_seq) for 2001 - day of week sums
auto this_sun <- sum(s…ar = 2002
  )
select
    store_sales.store.store_name,
    store_sales.store.store_id,
    store_sales.date_dim.week_seq as this_week_seq
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...les.date_dim.week_seq + 52 in ??? (       select store_sales.dat...

  Write stats: received 2265 chars / 2265 bytes; tail: …'
  store_sales.date_dim.week_seq as this_week_seq\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query59.preql --content import raw.store_sales as store_sales;

# For 'this year' (2001) - day-of-week sums keyed by (store_sk, week_seq)
… this_sat / nullif(next_sat, 0) as sat_ratio
order by store_name asc nulls first, store_id asc nulls first, this_week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 15, column 160.
  Expected one of:
          * _TERMINATOR

  Location:
   by store_sales.store.store_sk ??? , (store_sales.date_dim.week_s...

  Write stats: received 3673 chars / 3673 bytes; tail: …'d asc nulls first,
  this_week_seq asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query63.preql`

  ```text
  refused to write 'query63.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 16, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...sales price for each manager ??? auto per_month_total <- sum(st...

  Write stats: received 1346 chars / 1346 bytes; tail: …'vg_monthly_sales
  asc,\r\\n    per_month_total asc\r\\nlimit 100;\r\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query64.preql --content # Query 64: TPC-DS style query
# Identify items sold in store sales meeting catalog price/refund condition

import…lesale_cost) as total_wholesale_cost,
  sum(store_sales.list_price) as total_list_price,
  sum(store_sales.coupon_amt) as total_coupon_amt
limit 100;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 35, column 94.
  Expected one of:
          * _TERMINATOR

  Location:
   store_returns.ticket_number) ??? and (item.item_sk in store_ret...

  Write stats: received 3578 chars / 3578 bytes; tail: …'
  sum(store_sales.coupon_amt) as total_coupon_amt\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query64.preql --content # Query 64: TPC-DS style query with self-pairing

import raw.item as item;
import raw.store_sales as store_sales;
…by base_1999.product_name, base_1999.store_name, base_2000.sale_line_count, base_1999.total_wholesale_cost, base_2000.total_wholesale_cost
limit 100;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'item.product_name') at line 22,
  column 3.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...99 auto base_1999 <- select   ??? item.product_name,   store_sal...

  Write stats: received 4618 chars / 4618 bytes; tail: …'al_wholesale_cost,
  base_2000.total_wholesale_cost\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query64.preql --content # Query 64: TPC-DS style query with self-pairing

import raw.item as item;
import raw.store_sales as store_sales;
…2000,
  lp_2000,
  ca_2000
where cnt_2000 <= cnt_1999
order by item.product_name, store_sales.store.store_name, cnt_2000, ws_1999, ws_2000
limit 100;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 30, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('IDENTIFIER',
  'store_sales.customer.customer_demographics.marital_status')]

  Location:
   - computed by inline filters ??? auto cnt_1999 <- count(store_s...

  Write stats: received 6918 chars / 6918 bytes; tail: …'ales.store.store_name,
  cnt_2000, ws_1999, ws_2000\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query67.preql --content import raw.store_sales as store_sales;

# Treating null as 0 for sales_price and quantity
# Sum of (per-line sales…te_dim.moy asc nulls first,
    store_sales.store.store_id asc nulls first,
    summed_sales asc nulls first,
    cat_rank asc nulls first
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 33, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...der by summed_sales desc)     ??? as cat_rank;  where store_sale...

  Write stats: received 2202 chars / 2202 bytes; tail: …'les asc nulls first,\\n
  cat_rank asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# Filter to year 2000
where store_sales.date_dim.year = 2000

# Define tot…        else 0
    end as parent_rank
having state_rank <= 5
order by
    level desc,
    store_sales.store.state asc,
    parent_rank asc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 7, column 1.
  Expected one of:
          * SELECT

  Location:
  ...state for top-5 determination ??? auto state_profit <- sum(store...

  Write stats: received 1053 chars / 1053 bytes; tail: …' store_sales.store.state
  asc,\\n    parent_rank asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.store.state as s, count(store_sales.ticket_number) as cnt group by s order by cnt desc limit 20; duckdb`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...e_sales.ticket_number) as cnt ??? group by s order by cnt desc l...
  ```
- `trilogy file write query70.preql --content import raw.store_sales as store_sales;

# Define total net profit per state for top-5 determination
auto state_pro…_sales.store.county as g_county,
    g_state + g_county as level
order by
    level desc,
    store_sales.store.state asc,
    profit desc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...nd store_sales.store.state in ??? (     select store_sales.store...

  Write stats: received 1101 chars / 1101 bytes; tail: …'\\n
  store_sales.store.state asc,\\n    profit desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…amt) as total_amt
group by year, brand_id, class_id, category_id, manufact_id
order by year, brand_id, class_id, category_id, manufact_id
limit 1000;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it.
  Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate
  at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g.
  `sum(sales.amount) by sales.store.id`).
  Location:
   sum(ss_net_amt) as total_amt ??? group by year, brand_id, class...

  Write stats: received 1802 chars / 1802 bytes; tail: …'ar, brand_id, class_id,
  category_id, manufact_id\\nlimit 1000;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…
    sum(ss_net_qty_line) as total_qty,
    sum(ss_net_amt_line) as total_amt
order by year, brand_id, class_id, category_id, manufact_id
limit 1000;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 9, column 147.
  Expected one of:
          * _TERMINATOR

  Location:
  ...number = ss.ticket_number, 0) ??? by ss.item.item_sk, ss.ticket_...

  Write stats: received 1822 chars / 1822 bytes; tail: …'ar, brand_id, class_id,
  category_id, manufact_id\\nlimit 1000;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…coalesce(ws_qty, 0) as total_qty,
    coalesce(ss_amt, 0) + coalesce(cs_amt, 0) + coalesce(ws_amt, 0) as total_amt
order by total_qty desc
limit 100;`

  ```text
  …
  t);\nauto ws_amt <- sum(ws.ext_sales_price -
  coalesce(wr.return_amt, 0) ? ws.item.category = 'Books') by ws.sold_date.year,
  cast(ws.item.brand_id as bigint), cast(ws.item.class_id as bigint),
  cast(ws.item.category_id as bigint), cast(ws.item.manufact_id as
  bigint);\n\nselect\n    coalesce(ss_qty, 0) + coalesce(cs_qty, 0) +
  coalesce(ws_qty, 0) as total_qty,\n    coalesce(ss_amt, 0) + coalesce(cs_amt,
  0) + coalesce(ws_amt, 0) as total_amt\norder by total_qty desc\nlimit 100;") at
  line 19, column 121.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('__ANON_11', ', cast')]

  Location:
  ...ks') by ss.date_dim.year, cast ??? (ss.item.brand_id as bigint),

  Write stats: received 2385 chars / 2385 bytes; tail: …'e(ws_amt, 0) as
  total_amt\\norder by total_qty desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…coalesce(ws_qty, 0) as total_qty,
    coalesce(ss_amt, 0) + coalesce(cs_amt, 0) + coalesce(ws_amt, 0) as total_amt
order by total_qty desc
limit 100;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 19, column 117.
  Expected one of:
          * _TERMINATOR

  Location:
  ...'Books') by (ss.date_dim.year) ??? , (cast(ss.item.brand_id as bi...

  Write stats: received 2445 chars / 2445 bytes; tail: …'e(ws_amt, 0) as
  total_amt\\norder by total_qty desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…rand_id as bigint), cast(i.class_id as bigint), cast(i.category_id as bigint), cast(i.manufact_id as bigint));

select
    s01_q,
    s01_a
limit 10;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 25, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...egory = 'Books'  # Store 2001 ??? auto s01_q <- sum(ss.quantity

  Write stats: received 2006 chars / 2006 bytes; tail: …'ufact_id as
  bigint));\\n\\nselect\\n    s01_q,\\n    s01_a\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query78.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.web_sales as ws;
import raw.web_returns a… by ss_yr, ss.item.item_id, ss.customer.customer_id, ss_qty desc, ss_wc desc, ss_sp desc, other_qty asc, other_wc asc, other_sp asc, ratio
limit 100;`

  ```text
  refused to write 'query78.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '--having other_qty > 0\norder by
  ss_yr, ss.item.item_id, ss.customer.customer_id, ss_qty desc, ss_wc desc, ss_sp
  desc, other_qty asc, other_wc asc, other_sp asc, ratio\nlimit 100;') at line
  67, column 1.
  Expected one of:
          * HAVING
          * LIMIT
          * METADATA
          * ORDER
          * _TERMINATOR
          * WHERE
          * MERGE
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'other_sp')]

  Location:
  ...oalesce(cs_sp, 0) as other_sp ??? --having other_qty > 0 order b...

  Write stats: received 3293 chars / 3293 bytes; tail: …' other_qty asc, other_wc
  asc, other_sp asc, ratio\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query78.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.web_sales as ws;
import raw.web_returns a…  ss.date_dim.year,
    ss.item.item_id,
    ss.customer.customer_id,
    ss_qty,
    ss_wc,
    ss_sp,
    ss_ret_cnt
order by ss_qty desc
limit 10;`

  ```text
  refused to write 'query78.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 29, column 30.
  Expected one of:
          * SELECT

  Location:
   where ss.date_dim.year = 2000 ??? ;  # Return counts per (item,

  Write stats: received 1912 chars / 1912 bytes; tail: …'wc,\\n    ss_sp,\\n
  ss_ret_cnt\\norder by ss_qty desc\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…-09-22'::date
  and item.current_price > 50 and promo.channel_tv = 'N'
order by channel asc nulls first, outlet_identifier asc nulls first
limit 100;`

  ```text
  …
  tlet_identifier,\n
  sum(store_outlet_sales) by rollup channel, outlet_identifier as sales,\n
  sum(store_outlet_returns) by rollup channel, outlet_identifier as returns,\n
  sum(store_outlet_profit - store_outlet_net_loss) by rollup channel,
  outlet_identifier as profit\nwhere dd.date between '2000-08-23'::date and
  '2000-09-22'::date\n  and item.current_price > 50 and promo.channel_tv =
  'N'\norder by channel asc nulls first, outlet_identifier asc nulls first\nlimit
  100;") at line 31, column 172.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'concat')]

  Location:
  ...mo.channel_tv = 'N') by concat ??? ('store', ss.store.store_id);

  Write stats: received 4114 chars / 4114 bytes; tail: …'sc nulls first,
  outlet_identifier asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw.catalog_r…-09-22'::date
  and item.current_price > 50 and promo.channel_tv = 'N'
order by channel asc nulls first, outlet_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 34, column 140.
  Expected one of:
          * _TERMINATOR

  Location:
   item.current_price > 50), 0) ??? by (concat('store', ss.store.s...

  Write stats: received 2210 chars / 2210 bytes; tail: …'sc nulls first,
  outlet_identifier asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_returns:sr select sr.customer.customer_demographics.demo_sk as cust_demo, sr.customer_demographics.demo_sk as return_demo, count(sr.ticket_number) as cnt group by cust_demo, return_demo limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ount(sr.ticket_number) as cnt ??? group by cust_demo, return_dem...
  ```
- `trilogy run --import raw.store_returns:sr select sr.customer.customer_demographics.demo_sk as cust_demo, sr.customer_demographics.demo_sk as return_demo, count(sr.ticket_number) as cnt by cust_demo, return_demo having cust_demo = return_demo limit 10;`

  ```text
  --> 2:144
    |
  2 | select sr.customer.customer_demographics.demo_sk as cust_demo,
  sr.customer_demographics.demo_sk as return_demo, count(sr.ticket_number) as cnt
  by cust_demo, return_demo having cust_demo = return_demo limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...ount(sr.ticket_number) as cnt ??? by cust_demo, return_demo havi...
  ```
- `trilogy run --import raw.store_returns:sr select sr.customer.customer_demographics.demo_sk as cust_demo, sr.customer_demographics.demo_sk as return_demo, count(sr.ticket_number) as cnt by cust_demo, return_demo order by cnt desc limit 10;`

  ```text
  --> 2:144
    |
  2 | select sr.customer.customer_demographics.demo_sk as cust_demo,
  sr.customer_demographics.demo_sk as return_demo, count(sr.ticket_number) as cnt
  by cust_demo, return_demo order by cnt desc limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...ount(sr.ticket_number) as cnt ??? by cust_demo, return_demo orde...
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.sold_date.year = 2000

# Define rollup aggregate and grouping …       else 1
    end as rank
order by
    hierarchy_level desc nulls first,
    web_sales.item.category nulls first,
    rank nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 6, column 1.
  Expected one of:
          * SELECT

  Location:
  ...ggregate and grouping columns ??? auto total_net_paid <- sum(web...

  Write stats: received 1305 chars / 1305 bytes; tail: …'s.item.category nulls
  first,\\n    rank nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.sold_date.year = 2000

select
    web_sales.item.category,
   …        when hierarchy_level = 0 then rank_within_cat
        when hierarchy_level = 1 then rank_global
        else 1
    end nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 18, column 9.
  Expected one of:
          * MERGE
          * _TERMINATOR
          * LIMIT

  Location:
  ...nulls first,     case         ??? when hierarchy_level = 0 then

  Write stats: received 1022 chars / 1022 bytes; tail: …'en rank_global\\n
  else 1\\n    end nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.sold_date.year = 2000

select
    web_sales.item.category,
   …first,
    case
        when hierarchy_level = 0 then r_cat
        when hierarchy_level = 1 then r_all
        else 1
    end nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 18, column 9.
  Expected one of:
          * LIMIT
          * _TERMINATOR
          * MERGE

  Location:
  ...nulls first,     case         ??? when hierarchy_level = 0 then

  Write stats: received 935 chars / 935 bytes; tail: …'= 1 then r_all\\n
  else 1\\n    end nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.sold_date.year = 2000

select
    web_sales.item.category,
   …    web_sales.item.category nulls first,
    case when hierarchy_level = 0 then r1 when hierarchy_level = 1 then r2 else 1 end nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 17, column 10.
  Expected one of:
          * _TERMINATOR
          * LIMIT
          * MERGE

  Location:
  ...ategory nulls first,     case ??? when hierarchy_level = 0 then

  Write stats: received 895 chars / 895 bytes; tail: …'ierarchy_level = 1 then r2
  else 1 end nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.sold_date.year = 2000

select
    web_sales.item.category,
   … end desc nulls last) as rank
order by
    hierarchy_level desc nulls first,
    web_sales.item.category nulls first,
    rank nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 12, column 55.
  Expected one of:
          * RPAR

  Location:
  ...category) over (order by case ??? when g_cat = 0 and g_class = 1...

  Write stats: received 740 chars / 740 bytes; tail: …'s.item.category nulls
  first,\\n    rank nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.sold_date.year = 2000

def rollup_total_paid(metric) -> sum(me… total_net_paid desc) as rank
order by
    hierarchy_level desc nulls first,
    web_sales.item.category nulls first,
    rank nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'def') at line 5, column 1.
  Expected one of:
          * SELECT

  Location:
  ..._sales.sold_date.year = 2000  ??? def rollup_total_paid(metric)

  Write stats: received 898 chars / 898 bytes; tail: …'s.item.category nulls
  first,\\n    rank nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.sold_date.year = 2000

select
    web_sales.item.category,
   … 0 and g_class = 0 then total_net_paid
        when g_cat = 0 and g_class = 1 then total_net_paid
        else total_net_paid
    end desc
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHEN', 'when') at line 16, column 9.
  Expected one of:
          * LIMIT
          * _TERMINATOR
          * MERGE

  Location:
  ...nulls first,     case         ??? when g_cat = 0 and g_class = 0...

  Write stats: received 752 chars / 752 bytes; tail: …'net_paid\\n        else
  total_net_paid\\n    end desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge customer keys s…te_sk
select
    count_distinct(concat(ss.customer.last_name, '|', ss.customer.first_name, '|', ss.date_dim.date::string)) as unique_combos
limit 10;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 12, column 43.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._cust_date <- cs.order_number ??? by cs.bill_customer.customer_s...

  Write stats: received 1212 chars / 1212 bytes; tail: …", '|',
  ss.date_dim.date::string)) as unique_combos\\nlimit 10;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query87.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

merge ss.customer.custo…) = 0
select
    count_distinct(concat(ss.customer.last_name, '|', ss.customer.first_name, '|', ss.date_dim.date::string)) as unique_combos
limit 10;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 12, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._sk, cs.sold_date.date_sk     ??? where cs.sold_date.year = 2000...

  Write stats: received 971 chars / 971 bytes; tail: …", '|',
  ss.date_dim.date::string)) as unique_combos\\nlimit 10;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query90.preql --content import raw.web_sales as web_sales;

# Morning count
morning_count := count_distinct(web_sales.bill_hdemo.demo_sk
 …en 19 and 20);

select
    --morning_count,
    --evening_count,
    morning_count::numeric / evening_count::numeric as ratio
order by ratio
limit 1;`

  ```text
  …
  W
          * DATASOURCE_PARTIAL
          * PROPERTY
          * VALIDATE
          * PARAM
          * DEF
          * "merge"i
          * WITH
          * WHERE
          * UNIQUE
          * _DEF_TABLE
          * PARSE_COMMENT
          * SHORTHAND_MODIFIER
          * PUBLISH_ACTION
          * DATASOURCE
          * CHART
          * FROM
          * DATASOURCE_ROOT
          * PARAMETER
          * _PROPERTIES
          * IMPORT
          * $END
          * SELECT
          * MOCK
          * AUTO
          * ROWSET
          * COPY
          * SELF_IMPORT
          * CREATE
  Previous tokens: [Token('PARSE_COMMENT', '# Morning count\n')]

  Location:
  ...s web_sales;  # Morning count ??? morning_count := count_distinc...

  Write stats: received 621 chars / 621 bytes; tail: …'ic /
  evening_count::numeric as ratio\\norder by ratio\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query92.preql --content import raw.web_sales as ws;

# Per-item average extended discount amount (over all web sales in the window)
auto a…'::date and '2000-04-26'::date

select
  sum(ws.ext_discount_amt ? ws.ext_discount_amt > 1.3 * avg_ext_discount_by_item) as total
dy total
limit 100;`

  ```text
  refused to write 'query92.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'dy ') at line 11, column 1.
  Expected one of:
          * ORDER
          * _TERMINATOR
          * MERGE
          * COMMA
          * LIMIT
          * HAVING
          * METADATA
          * WHERE
  Previous tokens: [Token('IDENTIFIER', 'total')]

  Location:
  ...xt_discount_by_item) as total ??? dy total limit 100;

  Write stats: received 411 chars / 411 bytes; tail: …'1.3 *
  avg_ext_discount_by_item) as total\\ndy total\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

auto ss_pair <- hash(ss.customer.customer_id, ss.it…y_store,
    count_distinct(cs_pair ? cs_pair not in ss_pair) as only_catalog,
    count_distinct(ss_pair ? ss_pair in cs_pair) as in_both
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'ss.item.item_id') at line 4,
  column 47.
  Expected one of:
          * HASH_TYPE
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...hash(ss.customer.customer_id, ??? ss.item.item_id) where ss.date...

  Write stats: received 473 chars / 473 bytes; tail: …'distinct(ss_pair ? ss_pair
  in cs_pair) as in_both\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query97.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;

auto ss_pair <- concat(ss.customer.customer_id, '|'…y_store,
    count_distinct(cs_pair ? cs_pair not in ss_pair) as only_catalog,
    count_distinct(ss_pair ? ss_pair in cs_pair) as in_both
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 4, column 71.
  Expected one of:
          * _TERMINATOR

  Location:
  ...mer_id, '|', ss.item.item_id) ??? where ss.date_dim.year = 2000;...

  Write stats: received 487 chars / 487 bytes; tail: …'distinct(ss_pair ? ss_pair
  in cs_pair) as in_both\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query98.preql --content import raw.store_sales as store_sales;

# Filter to the date range
where store_sales.date_dim.date between '1999-0…
    store_sales.item.category,
    store_sales.item.class,
    store_sales.item.item_id,
    store_sales.item.item_desc,
    pct_of_class
limit 200;`

  ```text
  refused to write 'query98.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...me')  # Define per-item total ??? auto item_total_ext_sales <- s...

  Write stats: received 1086 chars / 1086 bytes; tail: …'\\n
  store_sales.item.item_desc,\\n    pct_of_class\\nlimit 200;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `other`

- `trilogy run query02_check.preql`

  ```text
  Multiple where clauses are not supported
  ```
- `trilogy run query02_check.preql`

  ```text
  Multiple where clauses are not supported
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.store_total', which is not in the
  SELECT projection (line 7). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.store_total
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.store_yt_2001', which is not in the
  SELECT projection (line 9). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.store_yt_2001
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_agg_sum_6502742331444045', 'local.total_returns',
  'local._virt_agg_sum_9615521547794243'} out of  with found
  {'local._virt_agg_sum_6502742331444045', 'local.total_sales', 'local.outlet'}
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ss_profit', 'local.sr_loss'} out of  with found
  {'store_sales.store.store_id', 'local.sr_loss', 'local.ss_sales',
  'local.ss_profit', 'store_returns.store.store_id', 'local.sr_returns'}
  ```
- `trilogy file write --content parameter zips string;

import raw.store_sales as store_sales;

# Count preferred customers per customer address ZIP
auto pc_cou…
    store_sales.store.store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.store_name
limit 100; query08.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.customer_address.zip, count(store_sales.customer.customer_sk ? store_sales.customer.preferred_cust_flag = 'Y') as pc_count where substring(store_sales.customer.customer_address.zip, 1, 2) in ('31','35') and pc_count > 10;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.pc_count) in the same statement where clause; move to the HAVING clause
  instead; Line: 2
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (_duckdb.BinderException) Binder Error: Cannot compare values
  of type VARCHAR and VARCHAR[] in IN/ANY/ALL clause - an explicit cast is
  required

  LINE 29: ... "cooperative"."store_sales_customer_customer_address_zip" in
  (select quizzical."_virt_func_split_4785012549328100...
                                                                         ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "store_sales_customer_customer_address_customer_address"."ca_zip" as
  "store_sales_customer_customer_address_zip",
      count("store_sales_customer_customer"."c_customer_sk") as "pc_by_zip"
  FROM
      "customer_add
  …
  BSTRING("store_sales_store_store"."s_zip",1,2) in (select
  questionable."valid_zip_prefix" from questionable where
  questionable."valid_zip_prefix" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."ss_item_sk",
      "store_sales_store_sales"."ss_ticket_number")
  SELECT
      "vacuous"."store_sales_store_store_name" as "store_sales_store_store_name",
      sum("vacuous"."store_sales_net_profit") as "total_net_profit"
  FROM
      "vacuous"
  GROUP BY
      1
  ORDER BY
      "vacuous"."store_sales_store_store_name" asc
  LIMIT (100)]

  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.customer_address.zip, count(store_sales.customer.customer_sk ? store_sales.custo…stomer.customer_address.zip,1,2) = '31' and store_sales.customer.customer_address.zip in ('31016','31029','31880','31671','31387') and pc_count > 10;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.pc_count) in the same statement where clause; move to the HAVING clause
  instead; Line: 2
  ```
- `trilogy run query10.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 26 column 12 (char 881). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_bcc', 'local.cat_bcc', 'local.web_bcc'} out of  with
  found {'catalog_sales.item.class', 'local.web_bcc', 'store_sales.item.class',
  'web_sales.item.brand', 'web_sales.item.class', 'local.store_bcc',
  'web_sales.item.category', 'store_sales.item.category',
  'catalog_sales.item.category', 'catalog_sales.item.brand', 'local.cat_bcc',
  'store_sales.item.brand'}
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_avg', which is not in the
  SELECT projection (line 45). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  Cannot reference an aggregate derived in the select
  (local.total_sales) in the same statement where clause; move to the HAVING
  clause instead; Line: 23
  ```
- `trilogy run query23.preql`

  ```text
  HAVING references 'local.prefix_date_pairs',
  'local.cust_spend', 'local.max_spend', which are not in the SELECT projection
  (line 22). Add them to SELECT, each prefixed with `--` so they stay out of the
  output rows — keep your HAVING as-is:
      select <your existing columns>, --local.prefix_date_pairs,
  --local.cust_spend, --local.max_spend
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "sparkling" not found!
  Candidate tables: "wakeful", "ss_store_sales"

  LINE 66: ...    SUBSTRING("cs_item_item"."i_item_desc",1,30) in
  (SUBSTRING("sparkling"."ss_item_item_desc",1,30)) and coalesce("ss_cus...
                                                                             ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "cs_catalog_sales"."cs_bill_customer_sk" as "ss_customer_customer_sk",
      "cs_catalog_sales"."cs_item_sk" as "ss_item_item_sk"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_
  …
  charming"."cust_spend") > 0.5 *
  "charming"."max_spend"
  )
  SELECT
      "protective"."cs_bill_customer_last_name" as "cs_bill_customer_last_name",
      "protective"."cs_bill_customer_first_name" as
  "cs_bill_customer_first_name",
      "protective"."total_sales" as "total_sales"
  FROM
      "protective"
  WHERE
      "protective"."cust_spend" > 0.5 * "protective"."max_spend"

  ORDER BY
      "protective"."cs_bill_customer_last_name" asc,
      "protective"."cs_bill_customer_first_name" asc,
      "protective"."total_sales" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "abundant" not found!
  Candidate tables: "uneven"

  LINE 35: ... SUBSTRING("cs_item_item"."i_item_desc",1,30) in
  (SUBSTRING("abundant"."ss_item_item_desc",1,30)) and coalesce("cs_bill...
                                                                          ^
  [SQL:
  WITH
  abundant as (
  SELECT
      "ss_item_item"."i_item_desc" as "ss_item_item_desc"
  FROM
      "item" as "ss_item_item"
  GROUP BY
      1),
  uneven as (
  SELECT
      "ss_store_sales"."ss_customer_sk" as "ss_customer_customer_sk",
      "ss_store_sales"."ss_item_sk" as "ss_item_item_sk
  …
  ce("cs_catalog_sales"."cs_item_sk","cs_item_item"."i_item_sk","ss_ite
  m_item"."i_item_sk","uneven"."ss_item_item_sk"))
  SELECT
      "juicy"."cs_bill_customer_last_name" as "cs_bill_customer_last_name",
      "juicy"."cs_bill_customer_first_name" as "cs_bill_customer_first_name",
      sum("juicy"."cs_quantity" * "juicy"."cs_list_price") as "total_sales"
  FROM
      "juicy"
  GROUP BY
      1,
      2
  ORDER BY
      "juicy"."cs_bill_customer_last_name" asc,
      "juicy"."cs_bill_customer_first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  Have
  {'MergeNode<cs.sold_date.year,local._virt_func_substring_5268364469350420,ss.da
  te_dim.date...9 more>': None} and need cs.sold_date.year = 2000 and
  cs.sold_date.moy = 2 and local._virt_agg_count_5825977775773128 > 4 and
  local._virt_agg_sum_5071420545018623 >
  multiply(0.5,local._virt_agg_max_9796661885369542@Grain<Abstract>)
  ```
- `trilogy run query23.preql`

  ```text
  HAVING references 'local.prefix_ct', 'local.cust_total',
  'local.mx', which are not in the SELECT projection (line 12). Add them to
  SELECT, each prefixed with `--` so they stay out of the output rows — keep your
  HAVING as-is:
      select <your existing columns>, --local.prefix_ct, --local.cust_total,
  --local.mx
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "sparkling" not found!
  Candidate tables: "wakeful", "ss_store_sales"

  LINE 66: ...    SUBSTRING("cs_item_item"."i_item_desc",1,30) in
  (SUBSTRING("sparkling"."ss_item_item_desc",1,30)) and coalesce("ss_cus...
                                                                             ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "cs_catalog_sales"."cs_bill_customer_sk" as "ss_customer_customer_sk",
      "cs_catalog_sales"."cs_item_sk" as "ss_item_item_sk"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      INNER JOIN "date_dim" as "cs_sold_date_
  …
  "cust_total","charming"."cust_total") > 0.5 *
  "charming"."mx"
  )
  SELECT
      "protective"."cs_bill_customer_last_name" as "cs_bill_customer_last_name",
      "protective"."cs_bill_customer_first_name" as
  "cs_bill_customer_first_name",
      "protective"."total_sales" as "total_sales"
  FROM
      "protective"
  WHERE
      "protective"."cust_total" > 0.5 * "protective"."mx"

  ORDER BY
      "protective"."cs_bill_customer_last_name" asc,
      "protective"."cs_bill_customer_first_name" asc,
      "protective"."total_sales" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query23.preql`

  ```text
  HAVING references 'local.mx', which is not in the SELECT
  projection (line 7). Add it to SELECT, each prefixed with `--` so it stays out
  of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.mx
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query09.preql`

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 31 column 12 (char 1440). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 36 column 12 (char 1541). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query30.preql`

  ```text
  Duplicate select output for local.cust_total; Line: 9
  ```
- `trilogy run query33.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.web_sum', 'local.store_sum', 'local.catalog_sum'} out of
  with found {'local.store_sum', 'store_sales.item.manufact_id'}
  ```
- `trilogy run query37.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run - --import raw.inventory:inventory duckdb`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "inventory_date_dim_year" must appear in the GROUP BY clause or must be part of
  an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(inventory_date_dim_year)"
  if the exact value of "inventory_date_dim_year" is not important.

  LINE 14:     "wakeful"."inventory_date_dim_year" as "inventory_date_dim_...
               ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "inventory_date_dim_date_dim"."d_moy" as "inventory_date_dim_moy",
      "inventory_date_dim_date_dim"."d_year" as "inventory_date_dim_year",
      "inventory_inventory"."inv_it
  …
  nventory_date_dim_date_dim"."d_date_sk" =
  "inventory_inventory"."inv_date_sk")
  SELECT
      "wakeful"."inventory_date_dim_year" as "inventory_date_dim_year",
      "wakeful"."inventory_item_item_sk" as "inventory_item_item_sk",
      "wakeful"."inventory_warehouse_warehouse_sk" as
  "inventory_warehouse_warehouse_sk",
      "wakeful"."inventory_date_dim_moy" as "inventory_date_dim_moy",
      "wakeful"."inventory_quantity_on_hand" as "avg_qty",
      stddev_samp("wakeful"."inventory_quantity_on_hand") as "std_qty"
  FROM
      "wakeful"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run - --import raw.inventory:inventory duckdb`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "inventory_warehouse_warehouse_sk" must appear in the GROUP BY clause or must
  be part of an aggregate function.
  Either add it to the GROUP BY list, or use
  "ANY_VALUE(inventory_warehouse_warehouse_sk)" if the exact value of
  "inventory_warehouse_warehouse_sk" is not important.

  LINE 16:     "wakeful"."inventory_warehouse_warehouse_sk" as "inventory_...
               ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "inventory_date_dim_date_dim"."d_moy" as "inventory_date_dim_moy",
      "inventory_inventory"."inv_item_sk" as "inventory_item_item_sk",
      "in
  …
  entory" on
  "inventory_date_dim_date_dim"."d_date_sk" = "inventory_inventory"."inv_date_sk"
  WHERE
      "inventory_date_dim_date_dim"."d_year" = 2001
  )
  SELECT
      "wakeful"."inventory_warehouse_warehouse_sk" as
  "inventory_warehouse_warehouse_sk",
      "wakeful"."inventory_item_item_sk" as "inventory_item_item_sk",
      "wakeful"."inventory_date_dim_moy" as "inventory_date_dim_moy",
      "wakeful"."inventory_quantity_on_hand" as "avg_qty",
      stddev_samp("wakeful"."inventory_quantity_on_hand") as "std_qty"
  FROM
      "wakeful"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run - --import raw.inventory:inventory duckdb`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "inventory_warehouse_warehouse_sk" must appear in the GROUP BY clause or must
  be part of an aggregate function.
  Either add it to the GROUP BY list, or use
  "ANY_VALUE(inventory_warehouse_warehouse_sk)" if the exact value of
  "inventory_warehouse_warehouse_sk" is not important.

  LINE 16:     "wakeful"."inventory_warehouse_warehouse_sk" as "inventory_...
               ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "inventory_date_dim_date_dim"."d_moy" as "inventory_date_dim_moy",
      "inventory_inventory"."inv_item_sk" as "inventory_item_item_sk",
      "in
  …
  entory" on
  "inventory_date_dim_date_dim"."d_date_sk" = "inventory_inventory"."inv_date_sk"
  WHERE
      "inventory_date_dim_date_dim"."d_year" = 2001
  )
  SELECT
      "wakeful"."inventory_warehouse_warehouse_sk" as
  "inventory_warehouse_warehouse_sk",
      "wakeful"."inventory_item_item_sk" as "inventory_item_item_sk",
      "wakeful"."inventory_date_dim_moy" as "inventory_date_dim_moy",
      "wakeful"."inventory_quantity_on_hand" as "avg_qty",
      stddev_samp("wakeful"."inventory_quantity_on_hand") as "std_qty"
  FROM
      "wakeful"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run - --import raw.inventory:inv duckdb`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "inv_warehouse_warehouse_sk" must appear in the GROUP BY clause or must be part
  of an aggregate function.
  Either add it to the GROUP BY list, or use
  "ANY_VALUE(inv_warehouse_warehouse_sk)" if the exact value of
  "inv_warehouse_warehouse_sk" is not important.

  LINE 16:     "wakeful"."inv_warehouse_warehouse_sk" as "inv_warehouse_wa...
               ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "inv_date_dim_date_dim"."d_moy" as "inv_date_dim_moy",
      "inv_inventory"."inv_item_sk" as "inv_item_item_sk",
      "inv_inventory"."inv_quantity_on_hand" as
  "_v
  …
  y" as "inv_inventory" on
  "inv_date_dim_date_dim"."d_date_sk" = "inv_inventory"."inv_date_sk"
  WHERE
      "inv_date_dim_date_dim"."d_year" = 2001
  )
  SELECT
      "wakeful"."inv_warehouse_warehouse_sk" as "inv_warehouse_warehouse_sk",
      "wakeful"."inv_item_item_sk" as "inv_item_item_sk",
      "wakeful"."inv_date_dim_moy" as "inv_date_dim_moy",
      "wakeful"."_virt_filter_quantity_on_hand_3620582209403506" as "avg_qty",
      stddev_samp("wakeful"."_virt_filter_quantity_on_hand_3620582209403506") as
  "std_qty"
  FROM
      "wakeful"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query39.preql duckdb`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run query39.preql duckdb`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "inv_warehouse_warehouse_sk" must appear in the GROUP BY clause or must be part
  of an aggregate function.
  Either add it to the GROUP BY list, or use
  "ANY_VALUE(inv_warehouse_warehouse_sk)" if the exact value of
  "inv_warehouse_warehouse_sk" is not important.

  LINE 16:     "wakeful"."inv_warehouse_warehouse_sk" as "inv_warehouse_wa...
               ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "inv_date_dim_date_dim"."d_moy" as "inv_date_dim_moy",
      "inv_inventory"."inv_item_sk" as "inv_item_item_sk",
      "inv_inventory"."inv_quantity_on_hand" as "inv
  …
  "inv_date_dim_date_dim"
      LEFT OUTER JOIN "inventory" as "inv_inventory" on
  "inv_date_dim_date_dim"."d_date_sk" = "inv_inventory"."inv_date_sk"
  WHERE
      "inv_date_dim_date_dim"."d_year" = 2001
  )
  SELECT
      "wakeful"."inv_warehouse_warehouse_sk" as "inv_warehouse_warehouse_sk",
      "wakeful"."inv_item_item_sk" as "inv_item_item_sk",
      "wakeful"."inv_date_dim_moy" as "inv_date_dim_moy",
      "wakeful"."inv_quantity_on_hand" as "avg_qty",
      stddev_samp("wakeful"."inv_quantity_on_hand") as "std_qty"
  FROM
      "wakeful"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query44.preql`

  ```text
  Recursion error building concept local.best_name with grain
  Grain<store_sales.item.item_sk> and lineage <Filter:
  ref:store_sales.item.product_name where ref:local.item_avg_net_profit >
  ref:local.threshold and ref:local.asc_rank = ref:local.position>. This is
  likely due to a circular reference.
  ```
- `trilogy run query44.preql`

  ```text
  Recursion error building concept local.rank with grain
  Grain<store_sales.item.item_sk> and lineage alias(ref:local.worst_rank). This
  is likely due to a circular reference.
  ```
- `trilogy run query44.preql`

  ```text
  HAVING references 'local.worst_position', which is not in the
  SELECT projection (line 36). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.worst_position
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  'local.desc_rank'
  ```
- `trilogy run query49.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query49.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.web_sale_key', 'local._virt_agg_sum_7576716528982320'} out of
  with found {'ws.sold_date.year', 'ws.net_profit', 'ws.net_paid_inc_ship',
  'ws.net_paid', 'local.web_sale_key', 'ws.item.item_id', 'ws.sold_date.date_sk',
  'ws.sold_date.moy', 'ws.quantity'}
  ```
- `trilogy run query49.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_agg_sum_7299919056097552',
  'local._virt_filter_web_return_amt_by_item_653538408078561',
  'local.web_sale_key',
  'local._virt_filter_web_return_qty_by_item_3137315531921794'} out of  with
  found {'ws.net_profit', 'ws.sold_date.year', 'ws.net_paid', 'ws.sold_date.moy',
  'local.web_sale_key', 'ws.item.item_id', 'ws.quantity'}
  ```
- `trilogy run query49.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query49.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query49.preql`

  ```text
  HAVING references 'local.web_sold_qty', 'local.web_net_paid',
  'local.web_ret_qty', 'local.web_ret_amt', which are not in the SELECT
  projection (line 43). Add them to SELECT, each prefixed with `--` so they stay
  out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.web_sold_qty, --local.web_net_paid,
  --local.web_ret_qty, --local.web_ret_amt
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query49.preql`

  ```text
  Cannot bind non-singleton concept local.channel
  (Granularity.MULTI_ROW, lineage concat(store,)) to a parameter.
  ```
- `trilogy run query49.preql`

  ```text
  HAVING references 'local.web_sold_qty', 'local.web_net_paid',
  'local.web_ret_qty', 'local.web_ret_amt', which are not in the SELECT
  projection (line 45). Add them to SELECT, each prefixed with `--` so they stay
  out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.web_sold_qty, --local.web_net_paid,
  --local.web_ret_qty, --local.web_ret_amt
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run --import raw/store_sales:ss select ss.date_dim.week_seq, count(ss.ticket_number) as cnt where ss.store.store_name = 'able' and ss.date_dim.year = 2001 order by week_seq limit 60;`

  ```text
  ORDER BY references 'local.week_seq', which is not in the
  SELECT projection (line 2). Add it to SELECT to sort by it — prefix with `--`
  to keep it out of the output rows, e.g. `select ..., --local.week_seq order by
  local.week_seq asc`.
  ```
- `trilogy run query59.preql`

  ```text
  HAVING references 'store_sales.date_dim.year', which is not
  in the SELECT projection (line 11). Add it to SELECT, each prefixed with `--`
  so it stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --store_sales.date_dim.year
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query60.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.web_total', 'local.store_total', 'local.catalog_total'} out
  of  with found {'ss.item.item_id', 'local.store_total'}
  ```
- `trilogy run query60.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.cs_per_item', 'local.ws_per_item', 'local.ss_per_item'} out
  of  with found {'local.ss_per_item', 'ws.item.item_id', 'local.cs_per_item',
  'local.ws_per_item', 'cs.item.item_id', 'ss.item.item_id'}
  ```
- `trilogy run query61.preql`

  ```text
  Value 'Y' is not valid for enum field
  'store_sales.promotion.channel_email'. Allowed values: 'N'.
  ```
- `trilogy run query65.preql`

  ```text
  ORDER BY references 'store_sales.item.item_sk', which is not
  in the SELECT projection (line 9). Add it to SELECT to sort by it — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --store_sales.item.item_sk order by store_sales.item.item_sk asc`.
  ```
- `trilogy run query70.preql duckdb`

  ```text
  HAVING references 'local.state_rank', which is not in the
  SELECT projection (line 10). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.state_rank
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query70.preql duckdb`

  ```text
  ORDER BY references 'local.within_parent_rank', which is not
  in the SELECT projection (line 16). Add it to SELECT to sort by it — prefix
  with `--` to keep it out of the output rows, e.g. `select ...,
  --local.within_parent_rank order by local.within_parent_rank asc`.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 28 column 12 (char 1888). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query75.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_func_cast_2806358268521162',
  'local._virt_func_cast_4941857551808284',
  'local._virt_func_cast_8753422998346619', 'local.ss_01_qty',
  'local._virt_func_cast_2363101125808202'} out of  with found
  {'local._virt_func_cast_5421735346467188',
  'local._virt_func_cast_314807062276216', 'local.ss_02_qty', 'local.ss_02_amt',
  'local._virt_func_cast_3033430593007949', 'ss.item.category_id',
  'ss.item.class_id', 'local.ss_01_qty', 'ss.item.brand_id',
  'local._virt_func_cast_8494832398423951', 'ss.item.manufact_id'}
  ```
- `trilogy run query76.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.cs_cnt', 'local.ws_amt', 'local.ss_cnt'} out of  with found
  {'cs.sold_date.qoy', 'ss.date_dim.year', 'ws.sold_date.year',
  'ws.item.category', 'local.cs_cnt', 'cs.sold_date.year', 'local.ws_cnt',
  'ss.date_dim.qoy', 'ss.item.category', 'local.ws_amt', 'local.ss_cnt',
  'ws.sold_date.qoy', 'cs.item.category'}
  ```
- `trilogy run query78.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 96 column 12 (char 3542). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query87.preql`

  ```text
  HAVING clause aggregate `count(<Filter: ref:cs.order_number
  where ref:cs.sold_date.year = 2000>) by ws.bill_customer.customer_sk,
  ws.sold_date.date_sk` is not in the SELECT projection (line 10). HAVING can
  only filter on off-grain or nested aggregates that are also computed in the
  SELECT. Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of
  the output rows, e.g. `select ..., --count(<Filter: ref:cs.order_number where
  ref:cs.sold_date.year = 2000>) by ws.bill_customer.customer_sk,
  ws.sold_date.date_sk`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run query91.preql`

  ```text
  Unable to import '.\catalog_returns.preql': [Errno 2] No such
  file or directory: '.\\catalog_returns.preql'. Did you mean:
  raw.catalog_returns?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 30 column 12 (char 1541). Re-issue the call with valid JSON arguments.
  ```
- `trilogy unit query93.preql`

  ```text
  Mocking not implemented for datatype BIGINT
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```

### `undefined-concept`

- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  customer_id. Suggestions: ['customer.login', 'customer.customer_id',
  'customer.birth_day']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  first_name.')
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run query14.preql duckdb`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  brand_id.')
  ```
- `trilogy run query42.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept: year.')
  ```
- `trilogy run query48.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  customer_demographics.marital_status. Suggestions:
  ['store_sales.customer_demographics.marital_status',
  'store_sales.customer.customer_demographics.marital_status',
  'store_sales.customer_demographics.education_status']")
  ```
- `trilogy run query49.preql`

  ```text
  (UndefinedConceptException(...), "line: 9: Undefined concept:
  ws.return_quantity. Suggestions: ['wr.return_quantity', 'ws.quantity',
  'wr.return_amt']")
  ```
- `trilogy run query56.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  cs.item.item_code. Suggestions: ['cs.item.item_desc', 'cs.item.item_id',
  'ws.item.item_desc']")
  ```
- `trilogy run query59.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  store_name.')
  ```
- `trilogy run query64.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept: item.
  Suggestions: ['item.size']")
  ```
- `trilogy run query68.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  last_name.')
  ```
- `trilogy run query76.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  category.')
  ```
- `trilogy run query76.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  category.')
  ```
- `trilogy run query76.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  category.')
  ```
- `trilogy run query76.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  category.')
  ```
- `trilogy run query76.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept: qoy.')
  ```
- `trilogy run query86.preql --displayed-rows 200`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  local.net_paid_inc_ship. Suggestions: ['web_sales.net_paid_inc_ship',
  'web_sales.net_paid_inc_ship_tax', 'web_sales.net_paid_inc_tax']")
  ```
- `trilogy run query99.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  sold_date.date. Suggestions: ['catalog_sales.sold_date.date',
  'catalog_sales.sold_date.date_sk', 'catalog_sales.sold_date.date_id']")
  ```

### `syntax-missing-alias`

- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

merge store_returns.ticket_numb…store_sales.store.store_id) as profit_s,
    sum(store_returns.net_loss) by rollup concat('store_', store_sales.store.store_id) as loss_s
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `sum(store_returns.net_loss) by rollup concat('store_',
  store_sales.store.store_id) as store_id_sum`
  Location:
  ...tore_id) as loss_s limit 100; ???

  Write stats: received 807 chars / 807 bytes; tail: …"('store_',
  store_sales.store.store_id) as loss_s\\nlimit 100;\\n".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;

select store_sales.item.brand, sum(store_sales.quantity) limit 2;

select store_sales.item.class, sum(store_sales.quantity) limit 2;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `sum(store_sales.quantity) as quantity_sum`
  Location:
  ...um(store_sales.quantity) limit ??? 2;  select store_sales.item.c...

  Write stats: received 172 chars / 172 bytes; tail: …'t store_sales.item.class,
  sum(store_sales.quantity) limit 2;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/inventory:inv select distinct inv.warehouse.warehouse_name limit 10; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct inv.warehouse.warehouse_name as
  distinct_inv_warehouse_warehouse_name`
  Location:
  ...ntory as inv; select distinct ??? inv.warehouse.warehouse_name l...
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.year, count(store_sales.ticket_number) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(store_sales.ticket_number) as
  ticket_number_count`
  Location:
  ...nt(store_sales.ticket_number) ??? limit 10;
  ```
- `trilogy file write query36.preql --content import raw.store_sales as store_sales;

where store_sales.date_dim.year = 2001
  and store_sales.store.state = 'TN…vel desc nulls first,
    case when hierarchy_level = 0 then store_sales.item.category end nulls first,
    within_parent_rank nulls first
limit 100;`

  ```text
  refused to write 'query36.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `sum(store_sales.net_profit) /
  nullif(sum(store_sales.ext_sales_price), 0) by rollup store_sales.item.category
  as sum_store_sales_net_profit_nullif_sum_st`
  Location:
  ..._sales.ext_sales_price), 0) by ??? rollup store_sales.item.categ...

  Write stats: received 917 chars / 917 bytes; tail: …'d nulls first,\\n
  within_parent_rank nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query41.preql -e -c import raw.item as item;

# Items matching any of the 8 attribute profiles
auto profile_manufact <- item.manufact
? (
…ere item.manufact_id between 1 and 500
and item.manufact in profile_manufact
select distinct item.product_name
order by item.product_name
limit 100;
`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct item.product_name as distinct_item_product_name`
  Location:
  ...ct distinct item.product_name ??? order by item.product_name lim...

  Write stats: received 1121 chars / 1121 bytes; tail: …'nct
  item.product_name\\norder by item.product_name\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query44.preql --content import raw.store_sales as store_sales;

# ============================================================
# Step 1: C…select unnest(generate_array(1, 10, 1)) as position) as pos
order by pos.position asc, best_performer_name desc, worst_performer_name desc
limit 100;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `worst_performer_name desc as worst_performer_name_desc`
  Location:
  ...performer_name desc limit 100; ???

  Write stats: received 1954 chars / 1954 bytes; tail: …'st_performer_name desc,
  worst_performer_name desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:ss select distinct ss.date_dim.year order by 1;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct ss.date_dim.year as
  distinct_ss_date_dim_year`
  Location:
  ..._sales as ss; select distinct ??? ss.date_dim.year order by 1;
  ```
- `trilogy run --import raw.web_sales:ws select distinct ws.sold_date.year order by 1;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct ws.sold_date.year as
  distinct_ws_sold_date_year`
  Location:
  ..._sales as ws; select distinct ??? ws.sold_date.year order by 1;
  ```
- `trilogy run --import raw.item:item select item.class, count(item.item_sk) where item.class like '%atern%' limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(item.item_sk) as item_sk_count`
  Location:
  ...em.class, count(item.item_sk) ??? where item.class like '%atern%...
  ```
- `trilogy run --import raw/date_dim:date_dim select date_dim.dow, date_dim.day_name, count(date_dim.date_sk) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(date_dim.date_sk) as date_sk_count`
  Location:
  ...name, count(date_dim.date_sk) ??? limit 10;
  ```
- `trilogy run --import raw/store_sales:ss select ss.date_dim.year, ss.date_dim.week_seq, count(ss.ticket_number) as cnt, sum(ss.ext_sales_price) where ss.date_dim.year = 2002 and ss.date_dim.week_seq between 5300 and 5400 limit 20;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `sum(ss.ext_sales_price) as
  ext_sales_price_sum`
  Location:
   cnt, sum(ss.ext_sales_price) ??? where ss.date_dim.year = 2002
  ```
- `trilogy file write query59.preql --content import raw.store_sales as store_sales;

# Per (store, week_seq, year) Sunday through Saturday sales totals
auto su…dim.year = 2001
   and sun_ratio is not null
order by store_name asc nulls first, store_id asc nulls first, this_week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `'this year' values (2001)
      -- 'next year' values via lead offset of 52 rows (matching this_week_seq +
  52 = next_week_seq)
      sun_sales / nullif(lead(sun_sales, 52) over (partition by
  store_sales.store.store_sk order by store_sales.date_dim.week_seq), 0) as
  this_year_values_2001_next_year_values_v`
  Location:
  ...eq,     -- 'this year' values ??? (2001)     -- 'next year' valu...

  Write stats: received 3020 chars / 3020 bytes; tail: …'d asc nulls first,
  this_week_seq asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select distinct store_sales.store.state limit 10; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct store_sales.store.state as
  distinct_store_sales_store_state`
  Location:
   store_sales; select distinct ??? store_sales.store.state limit
  ```
- `trilogy file write query80.preql --content # Query 80: Sales, Returns, and Profit by Channel and Outlet
# For period 2000-08-23 to 2000-09-22
# Items with cu…current_price > 50), 0) by rollup channel, outlet_identifier as profit
order by channel asc nulls first, outlet_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `coalesce(sum(sr.return_amt ? sr.date_dim.date between
  '2000-08-23'::date and '2000-09-22'::date and sr.item.current_price > 50), 0)
  by rollup channel as coalesce_sum_sr_return_amt_sr_date_dim_d`
  Location:
  ...tem.current_price > 50), 0) by ??? rollup channel, outlet_identi...

  Write stats: received 1921 chars / 1921 bytes; tail: …'sc nulls first,
  outlet_identifier asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql --content # Query 80: Sales, Returns, and Profit by Channel and Outlet
# For period 2000-08-23 to 2000-09-22
# Items with cu…e and '2000-09-22'::date and wr.item.current_price > 50), 0) as profit
order by channel asc nulls first, outlet_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `coalesce(sum(sr.return_amt ? sr.date_dim.date between
  '2000-08-23'::date and '2000-09-22'::date and sr.item.current_price > 50), 0)
  by rollup channel as coalesce_sum_sr_return_amt_sr_date_dim_d`
  Location:
  ...tem.current_price > 50), 0) by ??? rollup channel, outlet_identi...

  Write stats: received 3364 chars / 3364 bytes; tail: …'sc nulls first,
  outlet_identifier asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query82.preql --content import raw.item as item;
import raw.inventory as inventory;
import raw.store_sales as store_sales;

merge item.ite…00-05-25'::date and '2000-07-24'::date

select distinct
    item.item_id,
    item.item_desc,
    item.current_price
order by item.item_id
limit 100;`

  ```text
  refused to write 'query82.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct
      item.item_id as distinct_item_item_id`
  Location:
  ...lect distinct     item.item_id ??? ,     item.item_desc,     item...

  Write stats: received 534 chars / 534 bytes; tail: …'esc,\\n
  item.current_price\\norder by item.item_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.customer_address:ca select ca.city, count(ca.address_sk) where ca.city = 'Edgewood';`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(ca.address_sk) as address_sk_count`
  Location:
  ...ca.city, count(ca.address_sk) ??? where ca.city = 'Edgewood';
  ```
- `trilogy run --import raw.web_sales:ws --import raw.web_returns:wr select ws.sold_date.year, count(ws.order_number) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(ws.order_number) as
  order_number_count`
  Location:
  ....year, count(ws.order_number) ??? limit 5;
  ```

### `syntax-parse`

- `trilogy run query02.preql`

  ```text
  Could not resolve connections for query with output
  ['local.week_sequence_2001<Purpose.PROPERTY>Derivation.BASIC>',
  'local.sun_ratio<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query04.preql`

  ```text
  Could not resolve connections for query with output
  ['local.customer_code<Purpose.PROPERTY>Derivation.BASIC>',
  'store_sales.customer.first_name<Purpose.PROPERTY>Derivation.ROOT>',
  'local.store_yt_2001<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.catalog_yt_2001<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.web_yt_2001<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query05.preql`

  ```text
  Could not resolve connections for query with output
  ['local.ch<Purpose.CONSTANT>Derivation.CONSTANT>',
  'local.outlet<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_profit<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query05.preql`

  ```text
  Could not resolve connections for query with output
  ['local.total_sales<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query05.preql`

  ```text
  Could not resolve connections for query with output
  ['local.total_sales<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
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
- `trilogy run query23.preql`

  ```text
  Could not resolve connections for query with output
  ['cs.bill_customer.last_name<Purpose.PROPERTY>Derivation.ROOT>',
  'cs.bill_customer.first_name<Purpose.PROPERTY>Derivation.ROOT>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query23.preql`

  ```text
  Could not resolve connections for query with output
  ['cs.bill_customer.last_name<Purpose.PROPERTY>Derivation.ROOT>',
  'cs.bill_customer.first_name<Purpose.PROPERTY>Derivation.ROOT>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.prefix_ct<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.cust_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.mx<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query33.preql`

  ```text
  Could not resolve connections for query with output
  ['local.manufact_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.CONSTANT>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query37.preql`

  ```text
  Could not resolve connections for query with output
  ['catalog_sales.item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'catalog_sales.item.item_desc<Purpose.PROPERTY>Derivation.ROOT>',
  'catalog_sales.item.current_price<Purpose.PROPERTY>Derivation.ROOT>'] from
  current model.
  ```
- `trilogy run query49.preql`

  ```text
  Could not resolve connections for query with output
  ['ws.item.item_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.web_sold_qty<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.web_net_paid_amt<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.web_return_qty<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.web_return_amt<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query75.preql`

  ```text
  Could not resolve connections for query with output
  ['local.year<Purpose.PROPERTY>Derivation.BASIC>',
  'local.brand_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.class_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.category_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.manufact_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_qty<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_amt<Purpose.PROPERTY>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.customer:c --import raw.store_returns:sr select count(sr.ticket_number) as cnt where c.customer_sk = sr.customer.customer_sk and c.customer_demographics.demo_sk = sr.customer_demographics.demo_sk;`

  ```text
  Could not resolve connections for query with output
  ['local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.customer.customer_id, ss.ticket_number, ss.item.item_sk, sr.reason.desc, sr.return_quantity limit 10;`

  ```text
  Could not resolve connections for query with output
  ['ss.customer.customer_id<Purpose.PROPERTY>Derivation.ROOT>',
  'ss.ticket_number<Purpose.KEY>Derivation.ROOT>',
  'ss.item.item_sk<Purpose.KEY>Derivation.ROOT>',
  'sr.reason.desc<Purpose.PROPERTY>Derivation.ROOT>',
  'sr.return_quantity<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```
- `trilogy run -`

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
          * WHEN

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
          * WHEN

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
- `trilogy run query02.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_agg_sum_1524274500237560',
  'local._virt_agg_sum_4071775869425486'} out of  with found
  {'web_sales.sold_date.dow', 'web_sales.sold_date.week_seq',
  'local._virt_agg_sum_4071775869425486', 'web_sales.sold_date.year'}
  ```
- `trilogy run query02.preql`

  ```text
  Recursion error building concept local.s2001 with grain
  Grain<Abstract> and lineage <Filter: ref:local.sales_by_day where
  ref:catalog_sales.sold_date.year = 2001>. This is likely due to a circular
  reference.
  ```
- `trilogy run query02.preql`

  ```text
  Recursion error building concept local.sales_by_day with
  grain Grain<catalog_sales.sold_date.date_sk> and lineage
  add(sum(ref:web_sales.ext_sales_price)<abstract>,sum(ref:catalog_sales.ext_sale
  s_price)<['ref:catalog_sales.sold_date.year',
  'ref:catalog_sales.sold_date.week_seq', 'ref:catalog_sales.sold_date.dow']>).
  This is likely due to a circular reference.
  ```
- `trilogy run query02.preql`

  ```text
  Recursion error building concept local.sun_ratio with grain
  Grain<catalog_sales.item.item_sk,catalog_sales.time_dim.time_sk,web_sales.net_p
  aid_inc_ship> and lineage
  divide(ref:local.sun_2002,nullif(ref:local.sun_2001,0)). This is likely due to
  a circular reference.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 55 column 12 (char 2868). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query04.preql`

  ```text
  HAVING references 'local.catalog_2002', 'local.catalog_2001',
  'local.store_2002', 'local.store_2001', 'local.web_2002', 'local.web_2001',
  which are not in the SELECT projection (line 27). Add them to SELECT, each
  prefixed with `--` so they stay out of the output rows — keep your HAVING
  as-is:
      select <your existing columns>, --local.catalog_2002, --local.catalog_2001,
  --local.store_2002, --local.store_2001, --local.web_2002, --local.web_2001
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query04.preql`

  ```text
  maximum recursion depth exceeded
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_agg_sum_4448137742130880',
  'local._virt_agg_sum_4678507381734329', 'local.st_returns'} out of  with found
  {'local.st_sales', 'local._virt_agg_sum_4678507381734329', 'ss.store.store_sk'}
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.st_net_loss', 'local.st_net_profit'} out of  with found
  {'local.st_returns', 'sr.store.store_sk', 'local.st_net_loss',
  'ss.store.store_sk', 'local.st_net_profit', 'local.st_sales'}
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ss_profit', 'local.sr_returns'} out of  with found
  {'ss.store.store_sk', 'sr.store.store_sk', 'local.sr_returns',
  'ss.store.store_id', 'local.ss_sales', 'local.ss_profit'}
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.ss_profit', 'local.sr_returns', 'local.sr_loss'} out of  with
  found {'local.ss_sales', 'local.ss_profit', 'ss.store.store_sk',
  'ss.store.store_id'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 46 column 12 (char 2315). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query11.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.overall_avg', which is not in the
  SELECT projection (line 38). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 44 column 12 (char 2210). Re-issue the call with valid JSON arguments.
  ```

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
  Unexpected token Token('__ANON_7', 'by') at line 14, column 149.
  Expected one of:
          * _TERMINATOR

  Location:
   store_sales.ext_sales_price) ??? by store_sales.customer.custom...

  Write stats: received 3008 chars / 3008 bytes; tail:
  …'re_sales.customer.preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale….customer.first_name nulls first,
    store_sales.customer.last_name nulls first,
    store_sales.customer.preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('HAVING', 'having') at line 30, column 1.
  Expected one of:
          * SELECT

  Location:
  ...AND catalog ratio > web ratio ??? having (catalog_2002 / catalog...

  Write stats: received 2644 chars / 2644 bytes; tail:
  …'re_sales.customer.preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query06.preql --content import raw.store_sales as store_sales;

# Filter to January 2001 sales
where year(store_sales.date_dim.date) = 200…omer.customer_sk) as customer_count
having
    customer_count >= 10
order by
    customer_count asc nulls first,
    state asc nulls first
limit 100;`

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

- `trilogy run query35.preql`

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
  ustomer_demographics_marital_status","yumm
  y"."ss_customer_customer_demographics_marital_status") asc,
      coalesce("vacuous"."ss_customer_customer_demographics_dep_count","yummy"."s
  s_customer_customer_demographics_dep_count") asc,
      coalesce("vacuous"."ss_customer_customer_demographics_dep_employed_count","
  yummy"."ss_customer_customer_demographics_dep_employed_count") asc,
      coalesce("vacuous"."ss_customer_customer_demographics_dep_college_count","y
  ummy"."ss_customer_customer_demographics_dep_college_count") asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query64.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  store_returns_store_returns does not exist!
  Did you mean "store_returns"?

  LINE 9: ..." in (select store_returns_store_returns."item_item_sk" from
  store_returns_store_returns where store_returns_store_returns...
                                                                          ^
  [SQL:
  WITH
  divergent as (
  SELECT
      "store_returns_store_returns"."sr_ticket_number" as
  "store_returns_ticket_number"
  FROM
      "store_returns" as "store_returns_store_returns"
  WHERE
      "store_returns_store_returns"."sr_item_sk" in (select
  store_re
  …
  ar",
      "protective"."store_sales_customer_first_sales_date_year" as
  "first_sales_year",
      "protective"."store_sales_customer_first_shipto_date_year" as
  "first_shipto_year",
      "protective"."sale_line_count" as "sale_line_count",
      "protective"."total_wholesale_cost" as "total_wholesale_cost",
      "protective"."total_list_price" as "total_list_price",
      "protective"."total_coupon_amt" as "total_coupon_amt"
  FROM
      "premium"
      INNER JOIN "protective" on "premium"."item_item_sk" =
  "protective"."item_item_sk"
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query64.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  store_returns_item_item does not exist!
  Did you mean "store_returns"?

  LINE 16: ... store_returns_item_item."store_returns_item_item_sk" from
  store_returns_item_item where store_returns_item_item."stor...
                                                                         ^
  [SQL:
  WITH
  busy as (
  SELECT
      "store_returns_store_returns"."sr_ticket_number" as
  "store_returns_ticket_number"
  FROM
      "store_returns" as "store_returns_store_returns"
  GROUP BY
      1),
  cheerful as (
  SELECT
      "catalog_sales_catalog_sales"."cs_order_numb
  …
  ate_dim_year" as "sale_year",
      "premium"."store_sales_customer_first_sales_date_year" as
  "first_sales_year",
      "premium"."store_sales_customer_first_shipto_date_year" as
  "first_shipto_year",
      "premium"."sale_line_count" as "sale_line_count",
      "premium"."total_wholesale_cost" as "total_wholesale_cost",
      "premium"."total_list_price" as "total_list_price",
      "premium"."total_coupon_amt" as "total_coupon_amt"
  FROM
      "puzzled"
      INNER JOIN "premium" on "puzzled"."item_item_sk" = "premium"."item_item_sk"
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query82.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  store_sales_item_item does not exist!
  Did you mean "store_sales"?

  LINE 11: ... (select store_sales_item_item."store_sales_item_item_sk" from
  store_sales_item_item where store_sales_item_item."store_sa...
                                                                             ^
  [SQL:
  WITH
  highfalutin as (
  SELECT
      "inventory_inventory"."inv_date_sk" as "inventory_date_dim_date_sk",
      "inventory_inventory"."inv_item_sk" as "inventory_item_item_sk",
      "inventory_inventory"."inv_quantity_on_hand" as
  "inventory_quantity_on_ha
  …
  k"
  WHERE
      "item_item"."i_current_price" BETWEEN 62 AND 92 and
  "item_item"."i_manufact_id" in (129,270,821,423) and
  "highfalutin"."inventory_quantity_on_hand" BETWEEN 100 AND 500 and
  "inventory_date_dim_date_dim"."d_date" BETWEEN date '2000-05-25' AND date
  '2000-07-24' and "highfalutin"."inventory_item_item_sk" in (select
  store_sales_item_item."i_item_sk" from item as store_sales_item_item where
  store_sales_item_item."i_item_sk" is not null)

  GROUP BY
      1,
      2,
      3
  ORDER BY
      "item_item"."i_item_id" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query82.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  store_sales_item_item does not exist!
  Did you mean "store_sales"?

  LINE 11: ... (select store_sales_item_item."store_sales_item_item_sk" from
  store_sales_item_item where store_sales_item_item."store_sa...
                                                                             ^
  [SQL:
  WITH
  highfalutin as (
  SELECT
      "inventory_inventory"."inv_date_sk" as "inventory_date_dim_date_sk",
      "inventory_inventory"."inv_item_sk" as "inventory_item_item_sk",
      "inventory_inventory"."inv_quantity_on_hand" as
  "inventory_quantity_on_ha
  …
  k"
  WHERE
      "item_item"."i_current_price" BETWEEN 62 AND 92 and
  "item_item"."i_manufact_id" in (129,270,821,423) and
  "highfalutin"."inventory_quantity_on_hand" BETWEEN 100 AND 500 and
  "inventory_date_dim_date_dim"."d_date" BETWEEN date '2000-05-25' AND date
  '2000-07-24' and "highfalutin"."inventory_item_item_sk" in (select
  store_sales_item_item."i_item_sk" from item as store_sales_item_item where
  store_sales_item_item."i_item_sk" is not null)

  GROUP BY
      1,
      2,
      3
  ORDER BY
      "item_item"."i_item_id" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query87.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  ss_date_dim_date_dim does not exist!
  Did you mean "date_dim"?

  LINE 18: ..." not in (select ss_date_dim_date_dim."ws_sold_date_date" from
  ss_date_dim_date_dim where ss_date_dim_date_dim."ws_sold_da...
                                                                             ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "ws_web_sales"."ws_bill_customer_sk" as "ws_bill_customer_customer_sk",
      "ws_web_sales"."ws_sold_date_sk" as "ws_sold_date_date_sk"
  FROM
      "web_sales" as "ws_web_sales"
  GROUP BY
      1,
      2),
  quizzical as (
  SELECT

  …
  irst_name" is not null) and
  "questionable"."ws_sold_date_date" not in (select
  questionable."ws_sold_date_date" from questionable where
  questionable."ws_sold_date_date" is not null)

  GROUP BY
      1,
      2,
      3,
      "questionable"."ws_bill_customer_customer_sk",
      "questionable"."ws_sold_date_date_sk")
  SELECT
      count(distinct ("abundant"."ws_bill_customer_last_name" || '|' ||
  "abundant"."ws_bill_customer_first_name" || '|' ||
  cast("abundant"."ws_sold_date_date" as string))) as "unique_combos"
  FROM
      "abundant"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query87.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  cs_bill_customer_customer does not exist!
  Did you mean "customer"?

  LINE 10: ... cs_bill_customer_customer."cs_bill_customer_customer_sk" from
  cs_bill_customer_customer where cs_bill_customer_customer...
                                                                             ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "ss_store_sales"."ss_customer_sk" as "ss_customer_customer_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_dim_date_sk"
  FROM
      "store_sales" as "ss_store_sales"
  WHERE
      "ss_store_sales"."ss_customer_sk" not
  …
  r."c_customer_sk" is
  not null) and "ss_date_dim_date_dim"."d_date_sk" not in (select
  ws_sold_date_date_dim."d_date_sk" from date_dim as ws_sold_date_date_dim where
  ws_sold_date_date_dim."d_date_sk" is not null)

  GROUP BY
      1,
      2,
      3,
      "ss_customer_customer"."c_customer_sk",
      "ss_date_dim_date_dim"."d_date_sk")
  SELECT
      count(distinct ("uneven"."ss_customer_last_name" || '|' ||
  "uneven"."ss_customer_first_name" || '|' || cast("uneven"."ss_date_dim_date" as
  string))) as "unique_combos"
  FROM
      "uneven"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query87.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  cs_bill_customer_customer does not exist!
  Did you mean "customer"?

  LINE 11: ... cs_bill_customer_customer."cs_bill_customer_customer_sk" from
  cs_bill_customer_customer where cs_bill_customer_customer...
                                                                             ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "ss_store_sales"."ss_customer_sk" as "ss_customer_customer_sk",
      "ss_store_sales"."ss_sold_date_sk" as "ss_date_dim_date_sk",
      "ss_store_sales"."ss_ticket_number" as "ss_ticket_number"
  FROM
      "store_sales" as "ss
  …
   "ss_date_dim_date_dim"."d_date_sk"
      INNER JOIN "customer" as "ss_customer_customer" on
  "cheerful"."ss_customer_customer_sk" = "ss_customer_customer"."c_customer_sk"
  WHERE
      "ss_date_dim_date_dim"."d_year" = 2000 and
  "ss_customer_customer"."c_customer_sk" not in (select
  cs_bill_customer_customer."c_customer_sk" from customer as
  cs_bill_customer_customer where cs_bill_customer_customer."c_customer_sk" is
  not null)

  GROUP BY
      1)
  SELECT
      count("cooperative"."ss_ticket_number") as "cnt"
  FROM
      "cooperative"
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `type-error`

- `trilogy run query32.preql`

  ```text
  Cannot use BETWEEN with incompatible types DATE and STRING
  (low)
  ```
- `trilogy run query44.preql`

  ```text
  Invalid argument type 'STRING' passed into MAX function in
  position 1 from concept: store_sales.item.product_name. Valid: 'BIGINT',
  'BOOL', 'DATE', 'DATETIME', 'FLOAT', 'INTEGER', 'NUMBER', 'NUMERIC',
  'TIMESTAMP'.
  ```

### `cli-misuse`

- `trilogy run query02_check.preql --regex sold_date\.(year|week_seq|dow)`

  ```text
  '--regex' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
