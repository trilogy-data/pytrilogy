# Trilogy failure analysis — 20260531-035152

- Run `20260531-035151_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1862 | failed: 208 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 101 | 49% |
| `other` | 48 | 23% |
| `syntax-missing-alias` | 20 | 10% |
| `undefined-concept` | 17 | 8% |
| `join-resolution` | 12 | 6% |
| `cli-misuse` | 8 | 4% |
| `file-not-found` | 2 | 1% |

## Detail

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.physical_returns as physical_returns;

where physical_returns.store.state = 'TN'
and physical_returns.r…t_id,
    --cust_store_total,
    --store_avg
having cust_store_total > 1.2 * store_avg
order by physical_returns.billing_customer.text_id
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 6, column 1.
  Expected one of:
          * SELECT

  Location:
  ...urns.return_date.year = 2000  ??? auto cust_store_total <- sum(p...

  Write stats: received 536 chars / 536 bytes; tail: …'rder by
  physical_returns.billing_customer.text_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Daily sales for web and catalog
auto sales_amt <- all_sales.ext_sales_price …ek_seq + 53)
        / nullif(sat_2001, 0)
    , 0) as saturday_ratio
where all_sales.date.year = 2001
order by all_sales.date.week_seq asc
limit 60;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 93.
  Expected one of:
          * _TERMINATOR

  Location:
  ...0) by all_sales.date.week_seq ??? where all_sales.date.year = 20...

  Write stats: received 3371 chars / 3371 bytes; tail: …'e.year = 2001\\norder by
  all_sales.date.week_seq asc\\nlimit 60;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Daily sales value (ext_sales_price) for web/catalog only
auto sales_val <- a…fri_ratio,
    coalesce(s02_sat / nullif(s01_sat, 0), 0) as sat_ratio
where all_sales.date.year = 2001
order by all_sales.date.week_seq asc
limit 60;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 25, column 93.
  Expected one of:
          * _TERMINATOR

  Location:
  ...all_sales.date.week_seq - 53) ??? as ref_ws; auto s02_mon <- sum...

  Write stats: received 2999 chars / 2999 bytes; tail: …'e.year = 2001\\norder by
  all_sales.date.week_seq asc\\nlimit 60;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.all_sales as all_sales;

# Sales value for web and catalog only
auto wc_sales <- all_sales.ext_sales_pr…fri_ratio,
    coalesce(s02_sat / nullif(s01_sat, 0), 0) as sat_ratio
where all_sales.date.year = 2001
order by all_sales.date.week_seq asc
limit 60;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 16, column 128.
  Expected one of:
          * _TERMINATOR

  Location:
  ...(all_sales.date.week_seq - 53) ??? , all_sales.date.week_seq; aut...

  Write stats: received 2821 chars / 2821 bytes; tail: …'e.year = 2001\\norder by
  all_sales.date.week_seq asc\\nlimit 60;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Filter to the date range 2000-08-23 to 2000-09-06
where sales.date.date between …es.sales_channel as channel,
    outlet,
    total_sales,
    total_returns,
    total_profit
order by channel asc, outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...nel_dim_text_id with its kind ??? auto outlet <- case(     sales...

  Write stats: received 1048 chars / 1048 bytes; tail: …'ofit\\norder by channel
  asc, outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Derive the outlet name by prefixing the channel_dim_text_id with its kind
auto o…es.sales_channel as channel,
    outlet,
    total_sales,
    total_returns,
    total_profit
order by channel asc, outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 8, column 2.
  Expected one of:
          * WHEN

  Location:
  ..., sales.channel_dim_text_id) ) ??? ;  # Compute per-group metrics...

  Write stats: received 1055 chars / 1055 bytes; tail: …'ofit\\norder by channel
  asc, outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Derive the outlet name by prefixing the channel_dim_text_id with its kind
# STOR…es.sales_channel as channel,
    outlet,
    total_sales,
    total_returns,
    total_profit
order by channel asc, outlet asc nulls first
limit 100;`

  ```text
  …
  rns <- sum(sales.return_amount) by
  sales.sales_channel, outlet;\nauto total_profit <- sum(sales.net_profit -
  sales.return_net_loss) by sales.sales_channel, outlet;\n\n# Filter to the date
  range 2000-08-23 to 2000-09-06\nwhere sales.date.date between
  '2000-08-23'::date and '2000-09-06'::date\n\nselect\n    sales.sales_channel as
  channel,\n    outlet,\n    total_sales,\n    total_returns,\n
  total_profit\norder by channel asc, outlet asc nulls first\nlimit 100;") at
  line 6, column 16.
  Expected one of:
          * COMMA
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...let <- concat(     simple_case ??? (sales.sales_channel, 'STORE',...

  Write stats: received 1032 chars / 1032 bytes; tail: …'ofit\\norder by channel
  asc, outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Define outlet prefix based on sales channel
auto outlet_prefix <- case(
    sale… sum(sales.net_profit - sales.return_net_loss) by sales.sales_channel, outlet as total_profit
order by channel asc, outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 2.
  Expected one of:
          * WHEN

  Location:
   'web_site_',     'unknown_' ) ??? ;  # Build the outlet name aut...

  Write stats: received 866 chars / 866 bytes; tail: …'ofit\\norder by channel
  asc, outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Build the outlet name: prefix kind + text_id
# The outlet prefix is determined b… sum(sales.net_profit - sales.return_net_loss) by sales.sales_channel, outlet as total_profit
order by channel asc, outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 12, column 2.
  Expected one of:
          * WHEN

  Location:
   'web_site_',     'unknown_' ) ??? ;  # outlet = prefix || channe...

  Write stats: received 899 chars / 899 bytes; tail: …'ofit\\norder by channel
  asc, outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

auto outlet_prefix <- simple_case(sales.sales_channel, 'STORE', 'store_', 'CATALOG… sum(sales.net_profit - sales.return_net_loss) by sales.sales_channel, outlet as total_profit
order by channel asc, outlet asc nulls first
limit 100;`

  ```text
  …
  \n\nwhere sales.date.date
  between '2000-08-23'::date and '2000-09-06'::date\n\nselect\n
  sales.sales_channel as channel,\n    coalesce(outlet, '') as outlet,\n
  sum(sales.ext_sales_price) by sales.sales_channel, outlet as total_sales,\n
  sum(sales.return_amount) by sales.sales_channel, outlet as total_returns,\n
  sum(sales.net_profit - sales.return_net_loss) by sales.sales_channel, outlet as
  total_profit\norder by channel asc, outlet asc nulls first\nlimit 100;") at
  line 3, column 34.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...o outlet_prefix <- simple_case ??? (sales.sales_channel, 'STORE',...

  Write stats: received 688 chars / 688 bytes; tail: …'ofit\\norder by channel
  asc, outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:sales - duckdb`

  ```text
  --> 2:113
    |
  2 | select case(sales.sales_channel, 'STORE', 'store_', 'CATALOG',
  'catalog_page_', 'WEB', 'web_site_', 'unknown_') as prefix,
  sales.channel_dim_text_id as out, sales.ext_sales_price limit 10;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or fcase_simple_when
  Location:
  ...EB', 'web_site_', 'unknown_') ??? as prefix, sales.channel_dim_t...
  ```
- `trilogy run --import raw.all_sales:sales - duckdb`

  ```text
  …
   'CATALOG' then 'catalog_page_' when
  sales.sales_channel = 'WEB' then 'web_site_' else 'unknown_' end ||
  sales.channel_dim_text_id) as total_returns, sum(sales.net_profit -
  sales.return_net_loss) by rollup sales.sales_channel, (case when
  sales.sales_channel = 'STORE' then 'store_' when sales.sales_channel =
  'CATALOG' then 'catalog_page_' when sales.sales_channel = 'WEB' then
  'web_site_' else 'unknown_' end || sales.channel_dim_text_id) as total_profit
  order by channel asc, outlet asc nulls first limit 100;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
   by rollup sales.sales_channel ??? , (case when sales.sales_chann...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Define the outlet name (prefix + text_id)
auto outlet <- case
    when sales.sal… total_returns,
    @rollup_metrics(sales.net_profit - sales.return_net_loss) as total_profit
order by channel asc, outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 15, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
   rollup to keep it consistent ??? def rollup_metrics(metric) ->

  Write stats: received 912 chars / 912 bytes; tail: …'ofit\\norder by channel
  asc, outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:sales - duckdb`

  ```text
  --> 2:404
    |
  2 | where sales.date.date between '2000-08-23'::date and '2000-09-06'::date
  select sales.sales_channel as channel, coalesce(case when sales.sales_channel =
  'STORE' then 'store_' when sales.sales_channel = 'CATALOG' then 'catalog_page_'
  when sales.sales_channel = 'WEB' then 'web_site_' else 'unknown_' end ||
  sales.channel_dim_text_id, '') as outlet, sum(sales.ext_sales_price) by rollup
  sales.sales_channel, (case when sales.sales_channel = 'STORE' then 'store_'
  when sales.sales_channel = 'CATALOG' then 'catalog_page_' when
  sales.sales_channel = 'WEB' then 'web_site_' else 'unknown_' end ||
  sales.channel_dim_text_id) as total_sales order by channel asc, outlet asc
  nulls first limit 10;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
   by rollup sales.sales_channel ??? , (case when sales.sales_chann...
  ```
- `trilogy run --import raw.all_sales:sales - duckdb`

  ```text
  --> 2:390
    |
  2 | where sales.date.date between '2000-08-23'::date and '2000-09-06'::date
  select sales.sales_channel as channel, case when sales.sales_channel = 'STORE'
  then 'store_' when sales.sales_channel = 'CATALOG' then 'catalog_page_' when
  sales.sales_channel = 'WEB' then 'web_site_' else 'unknown_' end ||
  sales.channel_dim_text_id as outlet, sum(sales.ext_sales_price) by rollup
  sales.sales_channel, (case when sales.sales_channel = 'STORE' then 'store_'
  when sales.sales_channel = 'CATALOG' then 'catalog_page_' when
  sales.sales_channel = 'WEB' then 'web_site_' else 'unknown_' end ||
  sales.channel_dim_text_id) as total_sales order by channel asc, outlet asc
  nulls first limit 10;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
   by rollup sales.sales_channel ??? , (case when sales.sales_chann...
  ```
- `trilogy file write query06.preql --content import raw.physical_sales as physical_sales;

# Average current price of items in the same category
auto avg_cat_p…ysical_sales.row_counter) as line_item_count

having line_item_count >= 10
order by line_item_count asc nulls first, state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 15, column 65.
  Expected one of:
          * SELECT

  Location:
  ...t_price > 1.2 * avg_cat_price ??? by physical_sales.item.id  sel...

  Write stats: received 866 chars / 866 bytes; tail: …'item_count asc nulls
  first, state asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Set A: unique first-2-digit prefixes from the…ching_prefixes

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'x ') at line 9, column 9.
  Expected one of:
          * "@"
  Previous tokens: [Token('COMMA', ',')]

  Location:
      split(zips, ','),         ??? x -> substring(x, 1, 2)     )

  Write stats: received 1159 chars / 1159 bytes; tail: …'al_net_profit\\norder by
  store_sales.store.name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# First-2-digit prefixes from the parameter ZIP…_cust_cnt > 10

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 11, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ales.billing_customer.id)     ??? where store_sales.billing_cust...

  Write stats: received 886 chars / 886 bytes; tail: …'al_net_profit\\norder by
  store_sales.store.name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Distinct first-2-digit prefixes from paramete…n param_prefix

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_UNNEST', 'unnest(') at line 6, column 33.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'distinct')]

  Location:
  ...to param_prefixes <- distinct ??? unnest(split(zips, ',')); auto...

  Write stats: received 729 chars / 729 bytes; tail: …'al_net_profit\\norder by
  store_sales.store.name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# First-2-digit prefixes from the parameter lis…es.date.year = 1998
  and store_sales.date.quarter = 2
  and (substring(store_sales.store.zip, 1, 2)) in param_prefix
group by store_prefix
limit 20;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it.
  Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate
  at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g.
  `sum(sales.amount) by sales.store.id`).
  Location:
  ...e.zip, 1, 2)) in param_prefix ??? group by store_prefix limit 20...

  Write stats: received 442 chars / 442 bytes; tail: …'.zip, 1, 2)) in
  param_prefix\\ngroup by store_prefix\\nlimit 20;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;
import raw.customer as customer;

parameter zips string;

# First-2-digi… in valid_zips

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 12, column 60.
  Expected one of:
          * _TERMINATOR

  Location:
  ...(customer.address.zip, 1, 2)) ??? where pref_cnt > 10;  auto sto...

  Write stats: received 866 chars / 866 bytes; tail: …'al_net_profit\\norder by
  store_sales.store.name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;
import raw.customer as customer;

parameter zips string;

# First-2-digi…gh_pref_prefix

select
    store_sales.store.name,
    sum(store_sales.net_profit) as total_net_profit
order by store_sales.store.name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 13, column 42.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ref_prefix <- pref_zip_prefix ??? where pref_cnt > 10;  where st...

  Write stats: received 870 chars / 870 bytes; tail: …'al_net_profit\\norder by
  store_sales.store.name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;
import raw.customer as customer;

merge customer.id into ~store_sales.bi… and (substring(customer.address.zip, 1, 2)) = (substring(store_sales.store.zip, 1, 2))) as pref_count
order by store_sales.store.name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS',
  "--(substring(store_sales.store.zip, 1, 2)) as store_prefix,\n
  --count(customer.id ? customer.preferred_cust_flag = 'Y' and
  (substring(customer.address.zip, 1, 2)) = (substring(store_sales.store.zip, 1,
  2))) as pref_count\norder by store_sales.store.name asc\nlimit 100;") at line
  19, column 5.
  Expected one of:
          * _TERMINATOR
          * LIMIT
          * HAVING
          * WHERE
          * ORDER
          * MERGE
          * METADATA
          * COMMA
  Previous tokens: [Token('IDENTIFIER', 'pref_by_prefix')]

  Location:
   1, 2)) as pref_by_prefix     ??? --(substring(store_sales.store...

  Write stats: received 872 chars / 872 bytes; tail: …'as pref_count\\norder by
  store_sales.store.name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.physical_sales as ss;
import raw.all_sales as all_sales;

# ---- Frequent items ----
# Items (grouped b…rder by all_sales.billing_customer.last_name asc,
         all_sales.billing_customer.first_name asc,
         total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 11, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...(desc_prefix, ss.item.id)     ??? where ss.date.year between 200...

  Write stats: received 1668 chars / 1668 bytes; tail: …'st_name asc,\\n
  total_sales asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query31.preql -c import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Merge store and web county concepts so re…_q2_q1 as web_q2_over_q1,
    store_q2_q1 as store_q2_over_q1,
    web_q3_q2 as web_q3_over_q2,
    store_q3_q2 as store_q3_over_q2
order by county;
`

  ```text
  refused to write 'query31.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_OR', 'or') at line 16, column 35.
  Expected one of:
          * COMMA
          * RPAR

  Location:
   web_q2_q1 <- case(web_q1 = 0 ??? or web_q1 is null, null, web_q...

  Write stats: received 2046 chars / 2046 bytes; tail: …'er_q2,\\n    store_q3_q2
  as store_q3_over_q2\\norder by county;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query31.preql -c import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;

# Merge store and web county concepts so re…_q2_q1 as web_q2_over_q1,
    store_q2_q1 as store_q2_over_q1,
    web_q3_q2 as web_q3_over_q2,
    store_q3_q2 as store_q3_over_q2
order by county;
`

  ```text
  refused to write 'query31.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 16, column 58.
  Expected one of:
          * WHEN

  Location:
  ...q1 = 0, null, web_q2 / web_q1) ??? ; auto store_q2_q1 <- case(sto...

  Write stats: received 1949 chars / 1949 bytes; tail: …'er_q2,\\n    store_q3_q2
  as store_q3_over_q2\\norder by county;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query37.preql --content import raw.inventory as inventory;
import raw.catalog_sales as catalog_sales;

# Merge item concepts so both model…entory.item.text_id as item_code,
    inventory.item.desc as description,
    inventory.item.current_price
order by inventory.item.text_id
limit 100;`

  ```text
  …
        * DATASOURCE
          * SELECT
          * IMPORT
          * DATASOURCE_ROOT
          * TYPE
          * SHORTHAND_MODIFIER
          * _PROPERTIES
          * "merge"i
          * $END
          * PROPERTY
          * PARAMETER
          * WHERE
          * UNIQUE
          * PARSE_COMMENT
          * DATASOURCE_PARTIAL
          * PUBLISH_ACTION
          * COPY
          * VALIDATE
          * CREATE
          * PARAM
          * PURPOSE
          * MOCK
          * CHART
          * AUTO
          * PERSIST_MODE
  Previous tokens: [Token('PARSE_COMMENT', '# Has inventory with qty 100-500 on
  dates 2000-02-01 to 2000-04-01\n')]

  Location:
  ...ates 2000-02-01 to 2000-04-01 ??? and inventory.item.current_pri...

  Write stats: received 643 chars / 643 bytes; tail: …'tem.current_price\\norder
  by inventory.item.text_id\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query39.preql --content import raw.inventory as inventory;

# Only year 2001
where inventory.date.year = 2001

# Per (warehouse, item, mon…    warehouse_name asc,
    jan_mean asc nulls first,
    jan_cv asc nulls first,
    feb_mean asc nulls first,
    feb_cv asc nulls first
limit 100;`

  ```text
  refused to write 'query39.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 8, column 1.
  Expected one of:
          * SELECT

  Location:
  ...tes # Month 1 (January) stats ??? auto jan_mean <- avg(inventory...

  Write stats: received 1348 chars / 1348 bytes; tail: …'_mean asc nulls
  first,\\n    feb_cv asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query40.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Merge catalog returns into catalog sales by o…warehouse.state as warehouse_state,
    cs.item.id as item_code,
    net_before,
    net_after
order by warehouse_state asc, item_code asc
limit 100;`

  ```text
  refused to write 'query40.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 16, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...For sales on/after 2000-03-11 ??? auto net_before <- sum(cs.ext_...

  Write stats: received 977 chars / 977 bytes; tail: …'after\\norder by
  warehouse_state asc, item_code asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query40.preql --content import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Merge catalog returns into catalog sales by o…warehouse.state as warehouse_state,
    cs.item.id as item_code,
    net_before,
    net_after
order by warehouse_state asc, item_code asc
limit 100;`

  ```text
  refused to write 'query40.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 14, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...for sales on/after 2000-03-11 ??? auto net_before <- sum((cs.ext...

  Write stats: received 922 chars / 922 bytes; tail: …'after\\norder by
  warehouse_state asc, item_code asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any of the 8 attribute profiles
auto profile_items <- select
    item.m…as product_name
from item
where item.manufacturer_id between 1 and 500
  and item.manufact in profile_items.manufact
order by product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'item.manufact') at line 5,
  column 5.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...o profile_items <- select     ??? item.manufact, from item where...

  Write stats: received 1307 chars / 1307 bytes; tail: …'t in
  profile_items.manufact\\norder by product_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any of the 8 attribute profiles
auto profile_manufacts <- select item.m…ame as product_name
where item.manufacturer_id between 1 and 500
    and item.manufact in profile_manufacts.manufact
order by product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'item.manufact ') at line 4, column
  34.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...o profile_manufacts <- select ??? item.manufact from item where

  Write stats: received 1302 chars / 1302 bytes; tail: …'
  profile_manufacts.manufact\\norder by product_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any of the 8 attribute profiles
auto profile_manufacts <- item.manufact…ame as product_name
where item.manufacturer_id between 1 and 500
    and item.manufact in profile_manufacts.manufact
order by product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 4, column 41.
  Expected one of:
          * _TERMINATOR

  Location:
  ...le_manufacts <- item.manufact ??? where     (item.category = 'Bo...

  Write stats: received 1285 chars / 1285 bytes; tail: …'
  profile_manufacts.manufact\\norder by product_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query44.preql --content import raw.physical_sales as sales;

# Threshold: 90% of the average net profit on sales at store 1 where sale_add…duct_name as worst_performer_name
having pair_rank is not null
order by pair_rank asc, best_performer_name desc, worst_performer_name desc
limit 100;`

  ```text
  refused to write 'query44.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 14, column 47.
  Expected one of:
          * WHEN

  Location:
  ...<- case(best_rank <= 10, 1, 0) ??? ; auto is_worst_10 <- case(wor...

  Write stats: received 1028 chars / 1028 bytes; tail: …'st_performer_name desc,
  worst_performer_name desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query45.preql --content import raw.web_sales as web_sales;

# Get the text_ids of items whose internal item key is one of the specified va…ext_sales_price) as total_web_sales_price
order by
    web_sales.billing_customer.address.zip,
    web_sales.billing_customer.address.city
limit 100;`

  ```text
  refused to write 'query45.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 4, column 53.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ids <- web_sales.item.text_id ??? where web_sales.item.id in (2,...

  Write stats: received 1041 chars / 1041 bytes; tail: …'.zip,\\n
  web_sales.billing_customer.address.city\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query45.preql --content import raw.web_sales as web_sales;

# Get the text_ids of items whose internal item key is one of the specified va…ext_sales_price) as total_web_sales_price
order by
    web_sales.billing_customer.address.zip,
    web_sales.billing_customer.address.city
limit 100;`

  ```text
  refused to write 'query45.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 4, column 53.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ids <- web_sales.item.text_id ??? where web_sales.item.id in (2,...

  Write stats: received 1067 chars / 1067 bytes; tail: …'.zip,\\n
  web_sales.billing_customer.address.city\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query45.preql --content import raw.web_sales as web_sales;

# First 5 digits of ZIP code
auto zip_prefix <- substring(web_sales.billing_cu…ext_sales_price) as total_web_sales_price
order by
    web_sales.billing_customer.address.zip,
    web_sales.billing_customer.address.city
limit 100;`

  ```text
  refused to write 'query45.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
   or web_sales.item.text_id in ??? (       select web_sales.item....

  Write stats: received 818 chars / 818 bytes; tail: …'.zip,\\n
  web_sales.billing_customer.address.city\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query45.preql --content import raw.web_sales as web_sales;

# Get the set of item text_ids that belong to items whose id is in the target …ext_sales_price) as total_web_sales_price
order by
    web_sales.billing_customer.address.zip,
    web_sales.billing_customer.address.city
limit 100;`

  ```text
  refused to write 'query45.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 5, column 53.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ids <- web_sales.item.text_id ??? by web_sales.item.id;  # Filte...

  Write stats: received 575 chars / 575 bytes; tail: …'.zip,\\n
  web_sales.billing_customer.address.city\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query47.preql --content import raw.physical_sales as store_sales;

# Monthly sales total (price) per (category, brand, store, company) × (…ny_name,
    store_sales.date.year,
    store_sales.date.month_of_year,
    avg_monthly,
    monthly_total,
    prev_total,
    next_total
limit 100;`

  ```text
  refused to write 'query47.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 42, column 77.
  Expected one of:
          * SELECT

  Location:
  ..._sales.date.month_of_year = 1) ??? ;  select     store_sales.item...

  Write stats: received 2693 chars / 2694 bytes; tail: …'    monthly_total,\\n
  prev_total,\\n    next_total\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query49.preql --content import raw.all_sales as sales;
import raw.item;

# Dec 2001 sales only
where sales.date.year = 2001
  and sales.da…nk <= 10
order by
    channel asc,
    return_qty_rank asc nulls first,
    return_currency_rank asc nulls first,
    item asc nulls first
limit 100;`

  ```text
  refused to write 'query49.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 13, column 1.
  Expected one of:
          * SELECT

  Location:
  ...-item, per-channel aggregates ??? auto tot_return_qty <- sum(sal...

  Write stats: received 1306 chars / 1306 bytes; tail: …'cy_rank asc nulls
  first,\\n    item asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query50.preql --content import raw.physical_sales as store_sales;

# Filter to August 2001 returns only
where store_sales.return_date.year…sales.store.suite_number,
    store_sales.store.city,
    store_sales.store.county,
    store_sales.store.state,
    store_sales.store.zip
limit 100;`

  ```text
  refused to write 'query50.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('IDENTIFIER', 'store_sales.is_returned')]

  Location:
  ...een sale date and return date ??? auto elapsed_days <- date_diff...

  Write stats: received 1532 chars / 1532 bytes; tail:
  …'tore_sales.store.state,\\n    store_sales.store.zip\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query53.preql --content import raw.physical_sales as ss;

# Restrict to year 2000
where ss.date.year = 2000

# Restrict to items in one of…erly_total - avg_quarterly) / avg_quarterly > 0.1
order by
    avg_quarterly asc,
    quarterly_total asc,
    ss.item.manufacturer_id asc
limit 100;`

  ```text
  refused to write 'query53.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 18, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...quarter total by manufacturer ??? auto quarterly_total <- sum(ss...

  Write stats: received 1181 chars / 1181 bytes; tail: …'rterly_total asc,\\n
  ss.item.manufacturer_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query58.preql --content import raw.all_sales as s;

# Find the week_seq for 2000-01-03
auto target_week <- 5218;

# Per-channel extended s… web_total
    and store_total between 0.9 * web_total and 1.1 * web_total
order by item_code asc nulls first, store_total asc nulls first
limit 100;`

  ```text
  refused to write 'query58.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 85.
  Expected one of:
          * _TERMINATOR

  Location:
  ...annel = 'STORE') by s.item.id ??? where s.date.week_seq = target...

  Write stats: received 1277 chars / 1277 bytes; tail: …'code asc nulls first,
  store_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query59.preql --content import raw.physical_sales as store_sales;

# Per-line sale price

auto sale_price <- sum(store_sales.ext_sales_pri…date.year = 2001
order by store_sales.store.name nulls first, store_sales.store.text_id nulls first, store_sales.date.week_seq nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...ear_week_seq,     sun_sales / ??? (select sun_sales where store_...

  Write stats: received 2419 chars / 2419 bytes; tail: …'ulls first,
  store_sales.date.week_seq nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query59.preql --content import raw.physical_sales as store_sales;

# Per-day-of-week sales price at (store, week, day) grain

auto sun_sal…date.year = 2001
order by store_sales.store.name nulls first, store_sales.store.text_id nulls first, store_sales.date.week_seq nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 21, column 199.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ame, store_sales.store.text_id ??? , (store_sales.date.week_seq -...

  Write stats: received 4120 chars / 4120 bytes; tail: …'ulls first,
  store_sales.date.week_seq nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query59.preql --content import raw.physical_sales as store_sales;

auto sun_sales_this <- sum(store_sales.ext_sales_price ? store_sales.da…date.year = 2001
order by store_sales.store.name nulls first, store_sales.store.text_id nulls first, store_sales.date.week_seq nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 19, column 199.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ame, store_sales.store.text_id ??? , (store_sales.date.week_seq -...

  Write stats: received 4107 chars / 4107 bytes; tail: …'ulls first,
  store_sales.date.week_seq nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query59.preql --content import raw.physical_sales as store_sales;

auto sun_sales_this <- sum(store_sales.ext_sales_price ? store_sales.da…date.year = 2001
order by store_sales.store.name nulls first, store_sales.store.text_id nulls first, store_sales.date.week_seq nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 19, column 199.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ame, store_sales.store.text_id ??? , (store_sales.date.week_seq -...

  Write stats: received 3975 chars / 3975 bytes; tail: …'ulls first,
  store_sales.date.week_seq nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query59.preql --content import raw.physical_sales as store_sales;

auto test <- sum(store_sales.ext_sales_price) by store_sales.store.id, (store_sales.date.week_seq - 52);

select store_sales.store.id, test limit 10;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
  ...(store_sales.ext_sales_price) ??? by store_sales.store.id, (stor...

  Write stats: received 192 chars / 192 bytes; tail: …'week_seq -
  52);\\n\\nselect store_sales.store.id, test limit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query60.preql --content import raw.physical_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item concept…m code
select
    ss.item.text_id as item_code,
    store_sales_amt + catalog_sales_amt + web_sales_amt as total
order by item_code, total
limit 100;`

  ```text
  refused to write 'query60.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 11, column 76.
  Expected one of:
          * _TERMINATOR

  Location:
   ? ss.item.category = 'Music' ??? by ss.item.id;  # Store sales:...

  Write stats: received 1440 chars / 1440 bytes; tail: …' web_sales_amt as
  total\\norder by item_code, total\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query61.preql --content import raw.physical_sales as store_sales;

# Filter conditions:
# - Sales in November 1998
# - Items in 'Jewelry' …all_total as overall_total,
  (promo_channel_total / overall_total) * 100 as promotion_pct
order by promotion_channel_total, overall_total
limit 100;`

  ```text
  refused to write 'query61.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 16, column 1.
  Expected one of:
          * SELECT

  Location:
   of dmail, email, or tv = 'Y' ??? auto promo_channel_total <- su...

  Write stats: received 1012 chars / 1012 bytes; tail: …'t\\norder by
  promotion_channel_total, overall_total\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query62.preql --content import raw.web_sales as ws;

# Compute shipping lag in days per order line
auto shipping_lag <- date_diff(ws.date.…s_91_to_120,
    bucket_gt_120 as orders_gt_120
order by warehouse_name nulls first, ship_mode_type nulls first, web_site_name nulls first
limit 100;`

  ```text
  refused to write 'query62.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 7, column 121.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ing(ws.warehouse.name, 1, 20)) ??? , ws.ship_mode.type, ws.web_si...

  Write stats: received 1480 chars / 1480 bytes; tail: …'_mode_type nulls first,
  web_site_name nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query62.preql --content import raw.web_sales as ws;

# Compute shipping lag in days per order line
auto shipping_lag <- date_diff(ws.date.…s_91_to_120,
    bucket_gt_120 as orders_gt_120
order by warehouse_name nulls first, ship_mode_type nulls first, web_site_name nulls first
limit 100;`

  ```text
  refused to write 'query62.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 7, column 121.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ing(ws.warehouse.name, 1, 20)) ??? , (ws.ship_mode.type), (ws.web...

  Write stats: received 1534 chars / 1534 bytes; tail: …'_mode_type nulls first,
  web_site_name nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query63.preql --content import raw.physical_sales as ss;

# Filter conditions for the two item profiles
auto profile_a <- ss.item.category…0
    and abs(per_month_total - avg_monthly) / avg_monthly > 0.1
order by
    manager_id asc,
    avg_monthly asc,
    per_month_total asc
limit 100;`

  ```text
  refused to write 'query63.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 5, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...Children', 'Electronics')     ??? and ss.item.class in ('persona...

  Write stats: received 1406 chars / 1406 bytes; tail: …'asc,\\n    avg_monthly
  asc,\\n    per_month_total asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query64.preql --content import raw.physical_sales as store_sales
import raw.physical_returns as store_returns
import raw.catalog_sales as …phic.marital_status != store_sales.billing_customer.demographics.marital_status
having
    sale_year in (1999, 2000)
order by
    cnt desc
limit 100
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'import ') at line 2, column 1.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'store_sales')]

  Location:
  ...physical_sales as store_sales ??? import raw.physical_returns as...

  Write stats: received 2686 chars / 2686 bytes; tail: …'  sale_year in (1999,
  2000)\\norder by\\n    cnt desc\\nlimit 100\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query64.preql`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'store_sales.item.product_name')
  at line 14, column 5.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...auto grouped <-  select     ??? store_sales.item.product_name,...

  Write stats: received 4460 chars / 4460 bytes; tail: …'wholesale_sum asc,\r\\n
  yr2000.wholesale_sum asc\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query66.preql --content import raw.all_sales as all_sales;

# Filter: year 2001, time between 30838 and 59638, carrier in ('DHL','BARIAN')…eet::float) as total_ext_sales_per_sqft,
    sum(combined_net_x_qty) as total_net_x_qty
order by all_sales.warehouse.name asc nulls first
limit 100;
`

  ```text
  refused to write 'query66.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 7, column 52.
  Expected one of:
          * SELECT

  Location:
  ..._channel in ('WEB', 'CATALOG') ??? ;  # Per-channel, per-month, p...

  Write stats: received 2168 chars / 2168 bytes; tail: …'rder by
  all_sales.warehouse.name asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query66.preql --content import raw.all_sales as all_sales;

# Filter: year 2001, time between 30838 and 59638, carrier in ('DHL','BARIAN')…feet::float) as total_ext_sales_per_sqft,
    sum(combined_net_x_qty) as total_net_x_qty
order by all_sales.warehouse.name asc nulls first
limit 100;`

  ```text
  refused to write 'query66.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 18, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...e(cat_ext_sales_x_qty, 0)     ??? by (all_sales.date.month_seq,

  Write stats: received 2167 chars / 2167 bytes; tail: …'order by
  all_sales.warehouse.name asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query67.preql --content import raw.physical_sales as sales;

# Line revenue treating null sales_price as 0
auto line_revenue <- coalesce(s…
    coalesce(sales.date.month_of_year::string, '') asc,
    coalesce(sales.store.text_id, '') asc,
    summed_sales asc,
    cat_rank asc
limit 100;`

  ```text
  refused to write 'query67.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_BINDING', '->') at line 7, column 16.
  Expected one of:
          * LPAR
  Previous tokens: [Token('IDENTIFIER', 'rollup_rev')]

  Location:
  ...summed revenue def rollup_rev ??? -> sum(line_revenue)      by r...

  Write stats: received 3679 chars / 3679 bytes; tail: …", '') asc,\\n
  summed_sales asc,\\n    cat_rank asc\\nlimit 100;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as store_sales;

# Restrict to year 2000 sold dates
where store_sales.store.date.year = …_profit desc)
    end as within_parent_rank
order by
    hierarchy_level desc,
    store_sales.store.state asc,
    within_parent_rank asc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 8, column 1.
  Expected one of:
          * SELECT

  Location:
  ...pute state-level profit first ??? auto state_total_profit <- sum...

  Write stats: received 1288 chars / 1288 bytes; tail: …'sales.store.state
  asc,\\n    within_parent_rank asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as store_sales;

# Compute state-level profit for top-5 filtering
auto state_total_profi…    hierarchy_level,
    within_parent_rank
order by
    hierarchy_level desc,
    store_sales.store.state asc,
    within_parent_rank asc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 13, column 1.
  Expected one of:
          * SELECT

  Location:
  ...sc) <= 5  # Rollup aggregates ??? auto total_profit <- sum(store...

  Write stats: received 1482 chars / 1482 bytes; tail: …'sales.store.state
  asc,\\n    within_parent_rank asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as store_sales;

# Compute state-level profit for top-5 filtering
auto state_total_profi…    hierarchy_level,
    within_parent_rank
order by
    hierarchy_level desc,
    store_sales.store.state asc,
    within_parent_rank asc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 19, column 78.
  Expected one of:
          * WHEN

  Location:
   = 0, county_rank, state_rank) ??? ;  # Filter to year 2000 where...

  Write stats: received 1482 chars / 1482 bytes; tail: …'sales.store.state
  asc,\\n    within_parent_rank asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query70.preql --content import raw.physical_sales as store_sales;

# Compute state-level profit for top-5 filtering
auto state_total_profi…    hierarchy_level,
    within_parent_rank
order by
    hierarchy_level desc,
    store_sales.store.state asc,
    within_parent_rank asc
limit 100;`

  ```text
  refused to write 'query70.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 19, column 78.
  Expected one of:
          * WHEN

  Location:
   = 0, county_rank, state_rank) ??? ;  # Filter to year 2000 where...

  Write stats: received 1482 chars / 1482 bytes; tail: …'sales.store.state
  asc,\\n    within_parent_rank asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query72.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

merge cs.item.id into ~inv.item.id;
merge cs.warehou…t(cs.row_counter) as total_lines
order by total_lines desc, cs.item.desc asc, cs.warehouse.name asc, cs.sold_date.week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query72.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 12, column 26.
  Expected one of:
          * SELECT

  Location:
  ...tity   and cs.days_to_ship > 5 ??? ;  select     cs.item.desc,

  Write stats: received 800 chars / 800 bytes; tail: …'e.name asc,
  cs.sold_date.week_seq asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql --content import all_sales as all_sales;

# Filter to Books category items
where all_sales.item.category = 'Books'

# Per-li….date.year = 2001
    and qty_2001 > 0
    and cast(qty_2002 as float) / cast(qty_2001 as float) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...to treat missing returns as 0 ??? auto net_qty <- all_sales.quan...

  Write stats: received 2041 chars / 2041 bytes; tail: …' float) < 0.9\\norder by
  qty_diff asc, amt_diff asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...o treat missing returns as 0 ??? auto net_qty <- all_sales.quan...

  Write stats: received 1978 chars / 1978 bytes; tail: …'loat) < 0.9\r\\norder by
  qty_diff asc, amt_diff asc\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 5, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...es.item.category = 'Books'  ??? auto net_qty <- all_sales.quan...

  Write stats: received 1785 chars / 1785 bytes; tail: …'loat) < 0.9\r\\norder by
  qty_diff asc, amt_diff asc\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql --escapes -c import all_sales as all_sales;

where all_sales.item.category = 'Books'

auto net_qty <- all_sales.quantity - c…1 as amt_diff
having
    qty_2001 > 0
    and cast(qty_2002 as float) / cast(qty_2001 as float) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 5, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...ales.item.category = 'Books'  ??? auto net_qty <- all_sales.quan...

  Write stats: received 1755 chars / 1755 bytes; tail: …'float) < 0.9\\norder by
  qty_diff asc, amt_diff asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql --content import all_sales as all_sales;

where all_sales.item.category = 'Books'

auto net_qty <- all_sales.quantity - coal…01 as amt_diff
having
    qty_2001 > 0
    and cast(qty_2002 as float) / cast(qty_2001 as float) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 5, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...ales.item.category = 'Books'  ??? auto net_qty <- all_sales.quan...

  Write stats: received 1754 chars / 1754 bytes; tail: …' float) < 0.9\\norder by
  qty_diff asc, amt_diff asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query75.preql`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 5, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...es.item.category = 'Books'  ??? auto net_qty <- all_sales.quan...

  Write stats: received 1785 chars / 1785 bytes; tail: …'loat) < 0.9\r\\norder by
  qty_diff asc, amt_diff asc\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…price
where store_missing_ref or web_missing_ref or catalog_missing_ref
order by channel_label, missing_ref_label, year, quarter, category
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 22, column 6.
  Expected one of:
          * WHEN

  Location:
  ...'web',         'catalog'     ) ??? ;  auto missing_ref_label <-

  Write stats: received 1372 chars / 1372 bytes; tail: …'label,
  missing_ref_label, year, quarter, category\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query76.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…nel, missing_ref_label, date.year, date.quarter, item.category
order by channel, missing_ref_label, date.year, date.quarter, item.category
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it.
  Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate
  at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g.
  `sum(sales.amount) by sales.store.id`).
  Location:
   store_sales.store.id is null ??? group by channel, missing_ref_...

  Write stats: received 775 chars / 775 bytes; tail: …'ref_label, date.year,
  date.quarter, item.category\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query76.preql --content import raw.all_sales as all_sales;
import raw.catalog_sales as catalog_sales;

merge all_sales.item.id into ~catal…ull, 'store reference',
        web_null, 'web ship customer reference',
        'catalog ship address reference'
    ) as missing_ref_label
limit 5;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 8, column 54.
  Expected one of:
          * _TERMINATOR

  Location:
  ...sales.sales_channel = 'STORE' ??? and all_sales.channel_dim_id i...

  Write stats: received 760 chars / 760 bytes; tail: …" ship address
  reference'\\n    ) as missing_ref_label\\nlimit 5;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query76.preql --content import raw.all_sales as all_sales;
import raw.catalog_sales as catalog_sales;

merge all_sales.item.id into ~catal…ull, 'store reference',
        web_null, 'web ship customer reference',
        'catalog ship address reference'
    ) as missing_ref_label
limit 5;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 8, column 56.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ales.sales_channel = 'STORE') ??? and (all_sales.channel_dim_id

  Write stats: received 768 chars / 768 bytes; tail: …" ship address
  reference'\\n    ) as missing_ref_label\\nlimit 5;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query76.preql --content import raw.all_sales as all_sales;

# Channel-specific null reference conditions
# Store: channel_dim_id is null (…r (all_sales.sales_channel = 'CATALOG' and all_sales.bill_address.id is null)
order by channel, missing_ref_label, year, quarter, category
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 13, column 7.
  Expected one of:
          * WHEN

  Location:
  ...web',         'catalog'     ) ??? as channel,     case(

  Write stats: received 1135 chars / 1135 bytes; tail: …'annel,
  missing_ref_label, year, quarter, category\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query76.preql --content import raw.all_sales as all_sales;

select
    all_sales.sales_channel as channel,
    case(
        all_sales.sal…null)
    or (all_sales.sales_channel = 'CATALOG' and all_sales.bill_address.id is null)
order by channel, missing_ref_label, yr, qtr, cat
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 9, column 7.
  Expected one of:
          * WHEN

  Location:
  ...ship address reference'     ) ??? as missing_ref_label,     all_...

  Write stats: received 808 chars / 808 bytes; tail: …'order by channel,
  missing_ref_label, yr, qtr, cat\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query77.preql --content import raw.all_sales as all_sales;

# Date filter: 2000-08-23 through 2000-09-22
where all_sales.date.date between…turns,
    r_profit as profit
order by
    all_sales.sales_channel nulls first,
    all_sales.channel_dim_id nulls first,
    returns desc
limit 100;`

  ```text
  refused to write 'query77.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 6, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-22'::date  ??? auto r_sales <- sum(all_sales....

  Write stats: received 789 chars / 789 bytes; tail: …'ales.channel_dim_id nulls
  first,\\n    returns desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…,
    item asc,
    customer asc,
    s_qty desc,
    s_wc desc,
    s_sp desc,
    o_qty asc,
    o_wc asc,
    o_sp asc,
    ratio asc

limit 100;
`

  ```text
  refused to write 'query78.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 15, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('FALSE', 'false')]

  Location:
  ...re_sales.is_returned = false  ??? auto s_qty <- sum(store_sales....

  Write stats: received 2857 chars / 2857 bytes; tail: …' asc,\\n    o_wc asc,\\n
  o_sp asc,\\n    ratio asc\\n\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, sales.sales_channel, count(sales.quantity) as cnt where sales.date.year = 2000 group by sales.date.year, sales.sales_channel limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   where sales.date.year = 2000 ??? group by sales.date.year, sale...
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year as yr, sales.sales_channel as ch, sales.item.id as item, sum(sales.quantity) as tot_qty where sales.date.year = 2000 and sales.sales_channel = 'STORE' and not sales.is_returned group by yr, ch, item limit 5;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...RE' and not sales.is_returned ??? group by yr, ch, item limit 5;...
  ```
- `trilogy run --import raw.web_sales:ws --import raw.catalog_sales:cs select ws.item.id, ws.billing_customer.id, sum(ws.item.wholesale_cost * ws.quantity) as ww where ws.date.year = 2000 and not ws.is_returned group by ws.item.id, ws.billing_customer.id limit 5;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...= 2000 and not ws.is_returned ??? group by ws.item.id, ws.billin...
  ```
- `trilogy file write query79.preql --content import raw.physical_sales as store_sales;

# Ticket-level aggregates for coupon amount and net profit
auto ticket_…t,
    first_name asc nulls first,
    store_city asc nulls first,
    total_net_profit asc nulls first,
    ticket_number asc nulls first
limit 100;`

  ```text
  refused to write 'query79.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 4, column 140.
  Expected one of:
          * _TERMINATOR

  Location:
  ...e, billing_customer.first_name ??? , (substring(store_sales.store...

  Write stats: received 1466 chars / 1466 bytes; tail: …'sc nulls first,\\n
  ticket_number asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

# Date range filter
where all_sales.date.date between '2000-08-23'::date and '2000…nel, outlet as g_outlet,
    g_channel + g_outlet as level
order by
    level asc,
    channel asc nulls first,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'key ') at line 9, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...outlet identifier per channel ??? key outlet string;  select

  Write stats: received 1235 chars / 1235 bytes; tail: …'annel asc nulls
  first,\\n    outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;
where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  and a…e(all_sales.return_amount, 0)) as returns_total,
    sum(all_sales.net_profit) - sum(coalesce(all_sales.return_net_loss, 0)) as profit_total
limit 5;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 12, column 7.
  Expected one of:
          * WHEN

  Location:
  ...es.channel_dim_text_id)     ) ??? as outlet,     sum(all_sales.e...

  Write stats: received 766 chars / 766 bytes; tail:
  …'esce(all_sales.return_net_loss, 0)) as profit_total\\nlimit 5;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  and …s.return_amount, 0)) as returns,
    rollup_metric(all_sales.net_profit) - rollup_metric(coalesce(all_sales.return_net_loss, 0)) as profit
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('QUOTE', "'")]

  Location:
  ...s.promotion.channel_tv = 'N'  ??? def outlet_expr() -> case when...

  Write stats: received 942 chars / 942 bytes; tail:
  …'coalesce(all_sales.return_net_loss, 0)) as profit\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

def outlet_expr() -> case when all_sales.sales_channel = 'STORE' then concat('stor…s.return_amount, 0)) as returns,
    rollup_metric(all_sales.net_profit) - rollup_metric(coalesce(all_sales.return_net_loss, 0)) as profit
limit 100;`

  ```text
  …
  n\nwhere
  all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date\n  and
  all_sales.item.current_price > 50\n  and all_sales.promotion.channel_tv =
  'N'\n\nselect\n    all_sales.sales_channel as channel,\n    outlet_expr() as
  outlet,\n    rollup_metric(all_sales.ext_sales_price) as sales,\n
  rollup_metric(coalesce(all_sales.return_amount, 0)) as returns,\n
  rollup_metric(all_sales.net_profit) -
  rollup_metric(coalesce(all_sales.return_net_loss, 0)) as profit\nlimit 100;")
  at line 3, column 17.
  Expected one of:
          * IDENTIFIER
  Previous tokens: [Token('LPAR', '(')]

  Location:
  ...s all_sales;  def outlet_expr( ??? ) -> case when all_sales.sales...

  Write stats: received 942 chars / 942 bytes; tail:
  …'coalesce(all_sales.return_net_loss, 0)) as profit\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

auto outlet <- case when all_sales.sales_channel = 'STORE' then concat('store', al…all_sales.sales_channel, outlet as g_outlet
order by
    g_channel + g_outlet asc,
    channel asc nulls first,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
  ...nel, outlet as g_outlet order ??? by     g_channel + g_outlet as...

  Write stats: received 1218 chars / 1218 bytes; tail: …'annel asc nulls
  first,\\n    outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.inventory:inv select inv.item.id, inv.item.text_id, sum(inv.quantity_on_hand) as total_qty where inv.date.date between '2000-05-25'::date and '2000-07-24'::date and inv.item.manufacturer_id in (129, 270, 821, 423) group by inv.item.id having total_qty between 100 and 500 limit 10;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...er_id in (129, 270, 821, 423) ??? group by inv.item.id having to...
  ```
- `trilogy run --import raw.inventory:inv select inv.item.manufacturer_id, count(inv.item.id) as cnt, avg(inv.quantity_on_hand) as avg_qty where inv.date.date between '2000-05-25'::date and '2000-07-24'::date group by inv.item.manufacturer_id order by cnt desc limit 15;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...::date and '2000-07-24'::date ??? group by inv.item.manufacturer...
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.date.year = 2000

# define rollup aggregates
auto total_net_pa…at_rank
        else null
    end as rnk
order by
    level desc nulls first,
    web_sales.item.category nulls first,
    rnk nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 6, column 1.
  Expected one of:
          * SELECT

  Location:
  ...0  # define rollup aggregates ??? auto total_net_paid <- sum(web...

  Write stats: received 1107 chars / 1107 bytes; tail: …'es.item.category nulls
  first,\\n    rnk nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query87.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…|', store_sales.billing_customer.first_name, '|', store_sales.date.date::string)) as customer_combo_count
where store_sales.date.year = 2000
limit 1;`

  ```text
  …
     * DEF
          * "merge"i
          * PURPOSE
          * CHART
          * PERSIST_MODE
          * PUBLISH_ACTION
          * DATASOURCE_PARTIAL
          * PARSE_COMMENT
          * IMPORT
          * ROWSET
          * COPY
          * DATASOURCE_ROOT
          * AUTO
          * _DEF_TABLE
          * UNIQUE
          * _PROPERTIES
          * PARAM
          * WHERE
          * PARAMETER
          * DATASOURCE
          * FROM
          * SELF_IMPORT
          * $END
          * SHOW
          * WITH
          * TYPE
          * RAW_SQL
          * MOCK
          * SELECT
  Previous tokens: [Token('PARSE_COMMENT', '# store sales key\n')]

  Location:
  ...r anti-join # store sales key ??? store_sales.billing_customer.i...

  Write stats: received 923 chars / 923 bytes; tail: …'omer_combo_count\\nwhere
  store_sales.date.year = 2000\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query87.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…id::string, '|', store_sales.date.id::string) not in web_sales.concat(web_sales.billing_customer.id::string, '|', web_sales.date.id::string)
limit 1;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS',
  "(catalog_sales.bill_customer.id::string, '|',
  catalog_sales.sold_date.id::string)\n  and
  concat(store_sales.billing_customer.id::string, '|',
  store_sales.date.id::string) not in
  web_sales.concat(web_sales.billing_customer.id::string, '|',
  web_sales.date.id::string)\nlimit 1;") at line 20, column 116.
  Expected one of:
          * ORDER
          * HAVING
          * MERGE
          * LIMIT
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'catalog_sales.concat')]

  Location:
  ...g) not in catalog_sales.concat ??? (catalog_sales.bill_customer.i...

  Write stats: received 1363 chars / 1363 bytes; tail: …"ustomer.id::string, '|',
  web_sales.date.id::string)\\nlimit 1;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query87.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_s…t(catalog_sales.bill_customer.id::string, '|', catalog_sales.sold_date.id::string)) as have_catalog_match
where store_sales.date.year = 2000
limit 1;`

  ```text
  refused to write 'query87.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS',
  "(catalog_sales.bill_customer.id::string, '|',
  catalog_sales.sold_date.id::string)) as have_catalog_match\nwhere
  store_sales.date.year = 2000\nlimit 1;") at line 16, column 144.
  Expected one of:
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'catalog_sales.concat')]

  Location:
  ...tring) in catalog_sales.concat ??? (catalog_sales.bill_customer.i...

  Write stats: received 911 chars / 911 bytes; tail: …'ve_catalog_match\\nwhere
  store_sales.date.year = 2000\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query89.preql --content import raw.physical_sales as ss;

# Filter conditions for item categories/classes
# Group A: (Books, Electronics, …ry asc,
    ss.item.class asc,
    brand asc,
    ss.store.company_name asc,
    ss.date.month_of_year asc,
    total asc,
    avg_val asc
limit 100;`

  ```text
  refused to write 'query89.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 13, column 1.
  Expected one of:
          * SELECT

  Location:
  ...)     and ss.date.year = 1999 ??? ;  # Monthly total sales price...

  Write stats: received 1582 chars / 1582 bytes; tail: …'month_of_year asc,\\n
  total asc,\\n    avg_val asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query92.preql --content import raw.web_sales as web_sales;

# Filter: manufacturer 350, sold between 2000-01-27 and 2000-04-26
where web_s…eb_sales.ext_discount_amount ? web_sales.ext_discount_amount > 1.3 * avg_discount_per_item) as total_discount
order by total_discount desc
limit 100;`

  ```text
  refused to write 'query92.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...tem (in the same date window) ??? auto avg_discount_per_item <-

  Write stats: received 665 chars / 665 bytes; tail: …'m) as
  total_discount\\norder by total_discount desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query93.preql --content import raw.physical_sales as store_sales;

auto adjusted_amount <- 
    case(
        store_sales.return_quantity ….billing_customer.id as customer_id,
    sum(adjusted_amount) as total_adjusted_amount
order by total_adjusted_amount asc, customer_id asc
limit 100;`

  ```text
  refused to write 'query93.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 6, column 13.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...turn_quantity > 0             ??? and store_sales.return_reason....

  Write stats: received 502 chars / 502 bytes; tail: …'der by
  total_adjusted_amount asc, customer_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query93.preql --content import raw.physical_sales as store_sales;

# adjusted amount: when there's a matching return with reason 28, use (…customer_id,
    sum(adjusted_amount) as total_adjusted_amount
order by total_adjusted_amount asc nulls first, customer_id asc nulls first
limit 100;`

  ```text
  refused to write 'query93.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 10, column 48.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...ore_sales.return_quantity > 0 ??? and store_sales.return_reason....

  Write stats: received 686 chars / 686 bytes; tail: …'ount asc nulls first,
  customer_id asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query93.preql --content import raw.physical_sales as store_sales;

# When reason is 'reason 28', subtract return quantity from sold quanti…customer_id,
    sum(adjusted_amount) as total_adjusted_amount
order by total_adjusted_amount asc nulls first, customer_id asc nulls first
limit 100;`

  ```text
  refused to write 'query93.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 9, column 89.
  Expected one of:
          * WHEN

  Location:
  ...tore_sales.return_quantity, 0) ??? ;  auto adjusted_amount <- adj...

  Write stats: received 671 chars / 671 bytes; tail: …'ount asc nulls first,
  customer_id asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query93.preql --content import raw.physical_sales as store_sales;

# When reason is 'reason 28', subtract return quantity from sold quanti…customer_id,
    sum(adjusted_amount) as total_adjusted_amount
order by total_adjusted_amount asc nulls first, customer_id asc nulls first
limit 100;`

  ```text
  refused to write 'query93.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "(store_sales.return_reason.desc
  = 'reason 28', store_sales.return_quantity, 0);\n\nauto adjusted_amount <-
  adjusted_qty * store_sales.sales_price;\n\nselect\n
  store_sales.billing_customer.id as customer_id,\n    sum(adjusted_amount) as
  total_adjusted_amount\norder by total_adjusted_amount asc nulls first,
  customer_id asc nulls first\nlimit 100;") at line 9, column 18.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'simple_case')]

  Location:
  ...es.quantity      - simple_case ??? (store_sales.return_reason.des...

  Write stats: received 675 chars / 675 bytes; tail: …'ount asc nulls first,
  customer_id asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Merge the shared dimension…catalog_pair ? catalog_pair_key not in store_pair_key) as catalog_only,
    count(store_pair ? store_pair_key in catalog_pair_key) as both
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 19, column 106.
  Expected one of:
          * _TERMINATOR

  Location:
   store_sales.item.id::string) ??? by store_sales.billing_custome...

  Write stats: received 1412 chars / 1412 bytes; tail: …'air ? store_pair_key in
  catalog_pair_key) as both\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query97.preql --content import raw.physical_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Build unique (customer, it…atalog_pair_key not in store_pair_key) as catalog_only_count,
    count(store_pair_key ? store_pair_key in catalog_pair_key) as both_count
limit 100;`

  ```text
  refused to write 'query97.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 9, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...re_sales.item.id::string)     ??? by store_sales.billing_custome...

  Write stats: received 1031 chars / 1031 bytes; tail: …'store_pair_key in
  catalog_pair_key) as both_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query99.preql --content import raw/catalog_sales as cs;

where cs.ship_date.year = 2000

select
    substring(cs.warehouse.name, 1, 20) as…en 91 and 120) as ship_91_120,
    count(cs.order_number ? cs.days_to_ship > 120) as ship_gt_120
order by wh_name, ship_mode_type, cc_name
limit 100;`

  ```text
  …
  .name) as cc_name,\n    count(cs.order_number ?
  cs.days_to_ship <= 30) as ship_0_30,\n    count(cs.order_number ?
  cs.days_to_ship between 31 and 60) as ship_31_60,\n    count(cs.order_number ?
  cs.days_to_ship between 61 and 90) as ship_61_90,\n    count(cs.order_number ?
  cs.days_to_ship between 91 and 120) as ship_91_120,\n    count(cs.order_number
  ? cs.days_to_ship > 120) as ship_gt_120\norder by wh_name, ship_mode_type,
  cc_name\nlimit 100;') at line 1, column 11.
  Expected one of:
          * "as"
          * _TERMINATOR
          * IMPORT_DOT
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /catalog_sales as cs;  where c...

  Write stats: received 627 chars / 627 bytes; tail: …'_gt_120\\norder by
  wh_name, ship_mode_type, cc_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

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

- `trilogy file write ~query02.preql --content Let's think through this query step by step.`

  ```text
  …
  CHART
          * SHORTHAND_MODIFIER
          * ROWSET
          * AUTO
          * PURPOSE
          * CREATE
          * PUBLISH_ACTION
          * PARAMETER
          * PERSIST_MODE
          * DEF
          * SELF_IMPORT
          * COPY
          * $END
          * DATASOURCE_ROOT
          * DATASOURCE_PARTIAL
          * SHOW
          * IMPORT
          * DATASOURCE
          * PROPERTY
          * _DEF_TABLE
          * _PROPERTIES
          * FROM
          * SELECT
          * MOCK
          * PARSE_COMMENT
          * RAW_SQL
          * TYPE
          * WHERE
          * UNIQUE
          * WITH
          * PARAM
  Previous tokens: [None]

  Location:
   ??? Let's think through this query...

  Write stats: received 44 chars / 44 bytes; tail: …"Let's think through this
  query step by step.".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;
import raw.reason as reason;

# Bucket ticket quantities into ranges and co…t_paid end) as bucket_61_80,
    (case when bucket5_count > 165306 then bucket5_avg_discount else bucket5_avg_net_paid end) as bucket_81_100
limit 1;`

  ```text
  …
       * DATASOURCE
          * PARAMETER
          * $END
          * PERSIST_MODE
          * ROWSET
          * PROPERTY
          * WITH
          * VALIDATE
          * MOCK
          * DEF
          * PURPOSE
          * WHERE
          * TYPE
          * PARSE_COMMENT
          * UNIQUE
          * AUTO
          * RAW_SQL
          * SELECT
          * DATASOURCE_ROOT
          * "merge"i
          * DATASOURCE_PARTIAL
          * SHORTHAND_MODIFIER
          * FROM
          * CHART
          * CREATE
          * PUBLISH_ACTION
          * IMPORT
  Previous tokens: [Token('PARSE_COMMENT', '# Count of line items per quantity
  bucket\n')]

  Location:
  ...ine items per quantity bucket ??? bucket1_count = count(store_sa...

  Write stats: received 2742 chars / 2742 bytes; tail: …'unt else
  bucket5_avg_net_paid end) as bucket_81_100\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw/store_sales as store_sales;

# For store sales in the year 2001, return the average ticket quantity,
# …_sales_price,
    avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;`

  ```text
  …
  ', 'NM', 'KY')\n   and
  store_sales.net_profit between 150 and 300)\n  or\n
  (store_sales.sale_address.state in ('VA', 'TX', 'MS')\n   and
  store_sales.net_profit between 50 and 250)\n)\n\nselect\n
  avg(store_sales.quantity) as avg_ticket_qty,\n
  avg(store_sales.ext_sales_price) as avg_ext_sales_price,\n
  avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,\n
  sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;") at line 1,
  column 11.
  Expected one of:
          * _TERMINATOR
          * IMPORT_DOT
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /store_sales as store_sales;

  Write stats: received 2199 chars / 2199 bytes; tail:
  …'store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw/store_sales as store_sales;

where store_sales.date.year = 2001
and (
  (store_sales.customer_demograph…_sales_price,
    avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;`

  ```text
  …
  OR', 'NM',
  'KY')\n   and store_sales.net_profit between 150 and 300)\n  or\n
  (store_sales.sale_address.state in ('VA', 'TX', 'MS')\n   and
  store_sales.net_profit between 50 and 250)\n)\nselect\n
  avg(store_sales.quantity) as avg_ticket_qty,\n
  avg(store_sales.ext_sales_price) as avg_ext_sales_price,\n
  avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,\n
  sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;") at line 1,
  column 11.
  Expected one of:
          * _TERMINATOR
          * IMPORT_DOT
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /store_sales as store_sales;

  Write stats: received 1460 chars / 1460 bytes; tail:
  …'store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw/store_sales as store_sales;

where store_sales.date.year = 2001
and (
  (store_sales.customer_demograph…_sales_price,
    avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;`

  ```text
  …
  OR', 'NM',
  'KY')\n   and store_sales.net_profit between 150 and 300)\n  or\n
  (store_sales.sale_address.state in ('VA', 'TX', 'MS')\n   and
  store_sales.net_profit between 50 and 250)\n)\nselect\n
  avg(store_sales.quantity) as avg_ticket_qty,\n
  avg(store_sales.ext_sales_price) as avg_ext_sales_price,\n
  avg(store_sales.ext_wholesale_cost) as avg_ext_wholesale_cost,\n
  sum(store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;") at line 1,
  column 11.
  Expected one of:
          * _TERMINATOR
          * IMPORT_DOT
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /store_sales as store_sales;

  Write stats: received 1460 chars / 1460 bytes; tail:
  …'store_sales.ext_wholesale_cost) as total_ext_wholesale_cost;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query16.preql --content import raw.catalog_sales as catalog_sales;

# Restrict to catalog sales with ship date in range, shipping to GA,
#…alog_sales.ext_ship_cost) as total_extended_ship_cost,
    sum(catalog_sales.net_profit) as total_net_profit
order by distinct_order_count
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [211]: Expression in `by` clause must be wrapped in parens — write `by
  (expr1, expr2, ...)`. Bare identifiers (`by a, b`) work without parens, but any
  function call, cast, or other expression needs them.
  Location:
   warehouse # We filter orders ??? by this condition using inline...

  Write stats: received 975 chars / 975 bytes; tail: …'as
  total_net_profit\\norder by distinct_order_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `other`

- `trilogy run --import raw/store_sales:store_sales select store_sales.return_store.id as store_id, avg(sum(store_sales.return_amount)) by store_sales.return_cu…stomer_return where store_sales.return_store.state='TN' and store_sales.return_date.year=2000 and store_sales.return_customer.id is not null limit 5;`

  ```text
  HAVING references 'all_sales.date.year', which is not in the
  SELECT projection (line 14). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --all_sales.date.year
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
- `trilogy run query04.preql`

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 62 (char 61). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query08.preql --param zips=24128,76232`

  ```text
  ORDER BY references 'store_sales.store.name', which is not in
  the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --store_sales.store.name order by store_sales.store.name asc`.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  HAVING references 'store_sales.store.zip',
  'local.pref_prefixes', 'local.pref_cnt', which are not in the SELECT projection
  (line 13). Add them to SELECT, each prefixed with `--` so they stay out of the
  output rows — keep your HAVING as-is:
      select <your existing columns>, --store_sales.store.zip,
  --local.pref_prefixes, --local.pref_cnt
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  HAVING references 'store_sales.store.zip',
  'customer.address.zip', 'local.pref_cnt', which are not in the SELECT
  projection (line 13). Add them to SELECT, each prefixed with `--` so they stay
  out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --store_sales.store.zip,
  --customer.address.zip, --local.pref_cnt
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 7 column 21 (char 405). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  HAVING references 'local.ch_sales_total',
  'local.overall_avg_sale', which are not in the SELECT projection (line 25). Add
  them to SELECT, each prefixed with `--` so they stay out of the output rows —
  keep your HAVING as-is:
      select <your existing columns>, --local.ch_sales_total,
  --local.overall_avg_sale
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query18.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```
- `trilogy run query21.preql`

  ```text
  Unable to import '.\inventory.preql': [Errno 2] No such file
  or directory: '.\\inventory.preql'. Did you mean: raw.inventory?
  ```
- `trilogy run query27.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Table
  "store_sales_store_store" does not have a column named "S_CLOSED_DATE"

  Candidate bindings: : "s_closed_date_sk"

  LINE 14: ...    INNER JOIN "date_dim" as "store_sales_store_date_date" on
  "store_sales_store_store"."S_CLOSED_DATE" = "store_sales_st...
                                                                            ^
  [SQL:
  WITH
  cooperative as (
  SELECT
      "store_sales_store_sales"."SS_COUPON_AMT" as "store_sales_coupon_amt",
      "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
      "store_sales_store_sales"."SS_LIST_PRIC
  …
  ative"."store_sales_store_state" as "state",
      avg(cast("cooperative"."store_sales_quantity" as numeric(12,2))) as
  "avg_qty",
      avg(cast("cooperative"."store_sales_list_price" as numeric(12,2))) as
  "avg_list_price",
      avg(cast("cooperative"."store_sales_coupon_amt" as numeric(12,2))) as
  "avg_coupon",
      avg(cast("cooperative"."store_sales_sales_price" as numeric(12,2))) as
  "avg_sales_price"
  FROM
      "cooperative"
  GROUP BY
      ROLLUP (1, 2)
  ORDER BY
      "item_code" asc nulls first,
      "state" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query27.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING statement
  cannot be used without groups

  LINE 88:     grouping("juicy"."store_sales_item_id") as "_virt_agg_group...
               ^
  [SQL:
  WITH
  abundant as (
  SELECT
      "store_sales_store_sales"."SS_CUSTOMER_SK" as
  "store_sales_billing_customer_id",
      "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
      "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id",
      "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id"
  FROM
      "store_sales" as "store_sales_store_sales"
  GROUP BY
      1,
      2,
      3,

  …
  y"."avg_coupon" as "avg_coupon",
      "juicy"."avg_sales_price" as "avg_sales_price",
      CASE
          WHEN "vacuous"."_virt_agg_grouping_3472326529174567" = 1 or
  "juicy"."_virt_agg_grouping_7315987989213777" = 1 THEN 1
          ELSE 0
          END as "group_indicator"
  FROM
      "vacuous"
      INNER JOIN "juicy" on "vacuous"."store_sales_item_id" =
  "juicy"."store_sales_item_id" AND "vacuous"."store_sales_store_state" =
  "juicy"."store_sales_store_state"
  ORDER BY
      "item_code" asc nulls first,
      "state" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query28.preql`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```
- `trilogy run query35.preql`

  ```text
  Unable to import '.\customer.preql': [Errno 2] No such file
  or directory: '.\\customer.preql'. Did you mean: raw.customer?
  ```
- `trilogy run query44.preql`

  ```text
  HAVING references 'local.threshold', which is not in the
  SELECT projection (line 12). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.threshold
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  HAVING references 'local.threshold', which is not in the
  SELECT projection (line 10). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.threshold
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  HAVING references 'local.worst_rank', which is not in the
  SELECT projection (line 17). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.worst_rank
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  HAVING references 'local.worst_rank', which is not in the
  SELECT projection (line 16). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.worst_rank
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  HAVING references 'local.threshold', which is not in the
  SELECT projection (line 14). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.threshold
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  Recursion error building concept local.best_rank with grain
  Grain<local._virt_filter_id_737191589840652,local.avg_profit_by_item> and
  lineage rank() over [] order [OrderItem(expr=ref:local.avg_profit_by_item,
  order=<Ordering.ASCENDING: 'asc'>)]. This is likely due to a circular
  reference.
  ```
- `trilogy run query44.preql`

  ```text
  Recursion error building concept local.above_threshold with
  grain Grain<sales.item.id> and lineage ref:local.avg_profit_by_item >
  ref:local.threshold. This is likely due to a circular reference.
  ```
- `trilogy run query44.preql`

  ```text
  HAVING references 'sales.net_profit',
  'sales.sale_address.id', which are not in the SELECT projection (line 10). Add
  them to SELECT, each prefixed with `--` so they stay out of the output rows —
  keep your HAVING as-is:
      select <your existing columns>, --sales.net_profit, --sales.sale_address.id
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query44.preql`

  ```text
  Recursion error building concept local.best_rank with grain
  Grain<local._virt_filter_id_7022637098458648,local.avg_profit_by_item> and
  lineage rank() over [] order [OrderItem(expr=ref:local.avg_profit_by_item,
  order=<Ordering.ASCENDING: 'asc'>)]. This is likely due to a circular
  reference.
  ```
- `trilogy run query44.preql`

  ```text
  HAVING references 'local.worst_rank', which is not in the
  SELECT projection (line 14). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.worst_rank
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query51.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.web_running_total', 'local.store_running_total'} out of  with
  found {'local.web_running_total', 'web.date.date', 'local.web_daily_sales',
  'local.store_running_total', 'store.date.date', 'store.item.id',
  'local.store_daily_sales', 'web.item.id'}
  ```
- `trilogy run query54.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "source"

  LINE 95:     sum(CASE WHEN INVALID_REFERENCE_BUG_<Missing source reference to
  physical_sales.date.month_seq> BETWEEN...
                                                            ^
  [SQL:
  WITH
  young as (
  SELECT
      "web_sales_web_sales"."WS_BILL_CUSTOMER_SK" as
  "web_sales_billing_customer_id",
      "web_sales_web_sales"."WS_ITEM_SK" as "web_sales_item_id",
      "web_sales_web_sales"."WS_SOLD_DATE_SK" as "web_sales_date_id"
  FROM
      "web_sales" as "web_sales_web_sales"
  GROUP BY
      1,
      2,
      3),
  quizzical as (
  SELEC
  …
  unty> = INVALID_REFERENCE_BUG_<Missing source reference
  to physical_sales.billing_customer.address.county> and
  INVALID_REFERENCE_BUG_<Missing source reference to physical_sales.store.state>
  = INVALID_REFERENCE_BUG_<Missing source reference to
  physical_sales.billing_customer.address.state> THEN
  INVALID_REFERENCE_BUG_<Missing source reference to
  physical_sales.ext_sales_price> ELSE NULL END) is not null

  ORDER BY
      "macho"."segment" asc nulls first,
      "customer_count" asc nulls first,
      "segment_times_50" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 47 column 12 (char 2269). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query58.preql`

  ```text
  Cannot reference an aggregate derived in the select
  (local.store_total) in the same statement where clause; move to the HAVING
  clause instead; Line: 14
  ```
- `trilogy run query60.preql duckdb`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.catalog_sales_amt', 'local.store_sales_amt',
  'local.web_sales_amt'} out of  with found {'local.store_sales_amt',
  'ss.item.text_id'}
  ```
- `trilogy run query65.preql`

  ```text
  HAVING references 'local.store_avg_revenue', which is not in
  the SELECT projection (line 10). Add it to SELECT, each prefixed with `--` so
  it stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.store_avg_revenue
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query65.preql`

  ```text
  ORDER BY references 'store_sales.store.id', which is not in
  the SELECT projection (line 10). Add it to SELECT to sort by it — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --store_sales.store.id order by store_sales.store.id asc`.
  ```
- `trilogy run query70.preql`

  ```text
  All case expressions must have the same output datatype, got
  {<DataType.INTEGER: 'int'>, <DataType.STRING: 'string'>} from
  {'ref:store_sales.store.state': TraitDataType(type=<DataType.STRING: 'string'>,
  traits=['us_state_short']), '0': <DataType.INTEGER: 'int'>}
  ```
- `trilogy run query70.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Table
  "store_sales_store_store" does not have a column named "S_CLOSED_DATE"

  Candidate bindings: : "s_closed_date_sk"

  LINE 20: ...    INNER JOIN "date_dim" as "store_sales_store_date_date" on
  "store_sales_store_store"."S_CLOSED_DATE" = "store_sales_st...
                                                                            ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "store_sales_store_store"."S_STATE" as "store_sales_store_state",
      sum("store_sales_store_sales"."SS_NET_PROFIT") as "state_total_profit"
  FROM
      "store_sales" as "store_sales_stor
  …
  l",
      CASE
          WHEN "juicy"."hierarchy_level" = 0 THEN "concerned"."county_rank"
          ELSE "juicy"."state_rank"
          END as "within_parent_rank"
  FROM
      "concerned"
      FULL JOIN "juicy" on "concerned"."store_sales_store_county" =
  "juicy"."store_sales_store_county" AND "concerned"."store_sales_store_state" =
  "juicy"."store_sales_store_state"
  ORDER BY
      "juicy"."hierarchy_level" desc,
      coalesce("concerned"."store_sales_store_state","juicy"."store_sales_store_s
  tate") asc,
      "within_parent_rank" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query74.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Table
  "ss_store_store" does not have a column named "S_CLOSED_DATE"

  Candidate bindings: : "s_closed_date_sk"

  LINE 24:     LEFT OUTER JOIN "date_dim" as "ss_store_date_date" on
  "ss_store_store"."S_CLOSED_DATE" = "ss_store_date_date"...
                                                                     ^
  [SQL:
  WITH
  yummy as (
  SELECT
      "ws_web_sales"."WS_BILL_CUSTOMER_SK" as "ws_billing_customer_id",
      sum(CASE WHEN "ws_date_date"."D_YEAR" = 2001 THEN
  "ws_web_sales"."WS_NET_PAID" ELSE NULL END) as "ws_net_2001",
      sum(CASE WHEN "ws_date_date".
  …
  as "customer_code",
      "concerned"."ws_billing_customer_first_name" as
  "ws_billing_customer_first_name",
      "concerned"."ws_billing_customer_last_name" as
  "ws_billing_customer_last_name"
  FROM
      "concerned"
      INNER JOIN "thoughtful" on "concerned"."ws_billing_customer_id" =
  "thoughtful"."ws_billing_customer_id"
  WHERE
      "concerned"."ws_net_2001" > 0 and "concerned"."ws_net_2002" /
  "concerned"."ws_net_2001" > "thoughtful"."ss_net_2002" /
  "thoughtful"."ss_net_2001"

  ORDER BY
      "customer_code" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write --escapes -c import all_sales as all_sales;

# Filter to Books category items
where all_sales.item.category = 'Books'

# Per-line net sale…having
    qty_2001 > 0
    and cast(qty_2002 as float) / cast(qty_2001 as float) < 0.9
order by qty_diff asc, amt_diff asc
limit 100;
 query75.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query75.preql`

  ```text
  Unable to import '.\all_sales.preql': [Errno 2] No such file
  or directory: '.\\all_sales.preql'. Did you mean: raw.all_sales?
  ```
- `trilogy run query76.preql duckdb`

  ```text
  ORDER BY references 'local.year', which is not in the SELECT
  projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep
  it out of the output rows, e.g. `select ..., --local.year order by local.year
  asc`.
  ```
- `trilogy run query76.preql duckdb`

  ```text
  ORDER BY references 'local.year', which is not in the SELECT
  projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep
  it out of the output rows, e.g. `select ..., --local.year order by local.year
  asc`.
  ```
- `trilogy run query81.preql`

  ```text
  HAVING references 'local.state_avg', which is not in the
  SELECT projection (line 9). Add it to SELECT, each prefixed with `--` so it
  stays out of the output rows — keep your HAVING as-is:
      select <your existing columns>, --local.state_avg
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query81.preql`

  ```text
  ORDER BY references 'local.salutation', which is not in the
  SELECT projection (line 9). Add it to SELECT to sort by it — prefix with `--`
  to keep it out of the output rows, e.g. `select ..., --local.salutation order
  by local.salutation asc`.
  ```
- `trilogy run query82.preql`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 55 column 12 (char 2325). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 47 column 12 (char 2492). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query89.preql`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```
- `trilogy run query89.preql`

  ```text
  HAVING references 'ss.date.year', which is not in the SELECT
  projection (line 15). Add it to SELECT, each prefixed with `--` so it stays out
  of the output rows — keep your HAVING as-is:
      select <your existing columns>, --ss.date.year
  Alternatively move a row-level filter to WHERE; for an aggregate condition on a
  non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 31 column 12 (char 2000). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query97.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "catalog_sales_bill_customer_customers" not found!
  Candidate tables: "vacuous"

  LINE 36:     count(CASE WHEN
  "catalog_sales_bill_customer_customers"."C_CUSTOMER_SK"...
                               ^
  [SQL:
  WITH
  wakeful as (
  SELECT
      "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" as
  "catalog_sales_bill_customer_id",
      "catalog_sales_catalog_sales"."CS_ITEM_SK" as "catalog_sales_item_id"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      count(1) as "catalog_pairs"
  FROM

  …
  mers."C_CUSTOMER_SK" is not null) THEN
  "cooperative"."store_pairs" ELSE NULL END) as "store_only"
  FROM
      "cooperative"),
  abhorrent as (
  SELECT
      coalesce("uneven"."store_only",0) as "store_only",
      coalesce("young"."catalog_only",0) as "catalog_only"
  FROM
      "uneven"
      FULL JOIN "young" on 1=1)
  SELECT
      coalesce("abhorrent"."store_only",0) as "store_only",
      coalesce("abhorrent"."catalog_only",0) as "catalog_only",
      coalesce("uneven"."both",0) as "both"
  FROM
      "uneven"
      FULL JOIN "abhorrent" on 1=1
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query97.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "catalog_sales_bill_customer_customers" not found!
  Candidate tables: "concerned"

  LINE 79:     count(CASE WHEN
  "catalog_sales_bill_customer_customers"."C_CUSTOMER_SK"...
                               ^
  [SQL:
  WITH
  juicy as (
  SELECT
      "store_sales_store_sales"."SS_CUSTOMER_SK" as
  "catalog_sales_bill_customer_id",
      "store_sales_store_sales"."SS_ITEM_SK" as "catalog_sales_item_id",
      "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id"
  FROM
      "store_sales" as "store_sales_store_sales"
  GROUP BY
      1,
      2,

  …
  omers."C_CUSTOMER_SK" is not null) THEN
  "abundant"."store_pair_count" ELSE NULL END) as "store_only"
  FROM
      "abundant"),
  abhorrent as (
  SELECT
      coalesce("uneven"."store_only",0) as "store_only",
      coalesce("young"."catalog_only",0) as "catalog_only"
  FROM
      "uneven"
      FULL JOIN "young" on 1=1)
  SELECT
      coalesce("abhorrent"."store_only",0) as "store_only",
      coalesce("abhorrent"."catalog_only",0) as "catalog_only",
      coalesce("uneven"."both",0) as "both"
  FROM
      "uneven"
      FULL JOIN "abhorrent" on 1=1
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query97.preql`

  ```text
  (_duckdb.BinderException) Binder Error: Referenced table
  "questionable" not found!
  Candidate tables: "all_sales_billing_customer_customers"

  LINE 48:     count(CASE WHEN "questionable"."store_pair_count" > 0 and (
  "all_sales_bill...
                               ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "all_sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as
  "all_sales_billing_customer_id",
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
       1  as "all_sales_row_one",
       'CATALOG
  …
  ems"."I_ITEM_SK" in (select all_sales_item_items."I_ITEM_SK"
  from item as all_sales_item_items where all_sales_item_items."I_ITEM_SK" is not
  null) ) THEN "questionable"."store_pair_count" ELSE NULL END) as "debug"
  FROM
      "customer" as "all_sales_billing_customer_customers")
  SELECT
      coalesce("concerned"."total_store_pairs",0) as "total_store_pairs",
      coalesce("concerned"."total_catalog_pairs",0) as "total_catalog_pairs",
      coalesce("juicy"."debug",0) as "debug"
  FROM
      "juicy"
      FULL JOIN "concerned" on 1=1
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:all_sales select min(date.year), max(date.year), min(date.week_seq), max(date.week_seq), min(date.date), max(date.date) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `min(date.year) as year_min`
  Location:
  ...l_sales; select min(date.year) ??? , max(date.year), min(date.wee...
  ```
- `trilogy run --import raw/all_sales:all_sales select min(all_sales.date.week_seq), max(all_sales.date.week_seq) where all_sales.date.year <= 2002 limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `min(all_sales.date.week_seq) as
  week_seq_min`
  Location:
  ...t min(all_sales.date.week_seq) ??? , max(all_sales.date.week_seq)...
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Param unnest
auto param_zips <- unnest(split(…
    distinct substring(store_sales.store.zip, 1, 2) as store_prefix
where store_sales.date.year = 1998
  and store_sales.date.quarter = 2
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct substring(store_sales.store.zip, 1, 2) as
  distinct_substring_store_sales_store_zip`
  Location:
  ...select     distinct substring( ??? store_sales.store.zip, 1, 2) a...

  Write stats: received 333 chars / 333 bytes; tail: …'te.year = 1998\\n  and
  store_sales.date.quarter = 2\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

select distinct
    store_sales.store.name
order by store_sales.store.name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct
      store_sales.store.name as distinct_store_sales_store_name`
  Location:
  ...ct     store_sales.store.name ??? order by store_sales.store.nam...

  Write stats: received 156 chars / 156 bytes; tail: …'es.store.name\\norder by
  store_sales.store.name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.catalog_store_returns:csr select count(csr.item.id) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(csr.item.id) as id_count`
  Location:
  ...sr; select count(csr.item.id) ??? limit 10;
  ```
- `trilogy file write query41.preql --content import raw.item as item;

# Items matching any of the 8 attribute profiles
auto profile_manufacts <- item.manufact…ame as product_name
where item.manufacturer_id between 1 and 500
    and item.manufact in profile_manufacts.manufact
order by product_name
limit 100;`

  ```text
  refused to write 'query41.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `distinct item.product_name as distinct_item_product_name`
  Location:
  ...ct distinct item.product_name ??? as product_name where item.man...

  Write stats: received 1281 chars / 1281 bytes; tail: …'
  profile_manufacts.manufact\\norder by product_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.item:item select distinct item.category;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct item.category as
  distinct_item_category`
  Location:
  ...item as item; select distinct ??? item.category;
  ```
- `trilogy run --import raw.date:date select distinct date.month_of_year, date.year, date.month_seq where date.year = 1998 and date.month_of_year = 12 limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct date.month_of_year as
  distinct_date_month_of_year`
  Location:
  ...date as date; select distinct ??? date.month_of_year, date.year,...
  ```
- `trilogy run --import raw.item:item select distinct item.category limit 20; duckdb`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `distinct item.category as
  distinct_item_category`
  Location:
  ...item as item; select distinct ??? item.category limit 20;
  ```
- `trilogy file write query64.preql`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `first row (1999) values via lag
      lag(store_sales.date.year) over (partition by store_sales.item.id,
  store_sales.store.name, store_sales.store.zip order by store_sales.date.year)
  as first_row_1999_values_via_lag_lag_store_`
  Location:
  ...as cust_zip,     --first row ??? (1999) values via lag     lag...

  Write stats: received 3003 chars / 3003 bytes; tail: …'wholesale_sum asc,\r\\n
  second_wholesale_sum asc\r\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.ship_mode:sm select sm.carrier, count(sm.id);`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(sm.id) as id_count`
  Location:
  ...elect sm.carrier, count(sm.id) ??? ;
  ```
- `trilogy file write query76.preql --content import raw.all_sales as all_sales;

select
    all_sales.sales_channel as channel,
    simple_case(all_sales.sales…l_sales.bill_address.id is null)
order by channel, missing_ref_label, all_sales.date.year, all_sales.date.quarter, all_sales.item.category
limit 100;`

  ```text
  refused to write 'query76.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `all_sales.item.category as all_sales_item_category`
  Location:
  ...sales.item.category limit 100; ???

  Write stats: received 834 chars / 834 bytes; tail: …', all_sales.date.quarter,
  all_sales.item.category\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query78.preql --content import raw.physical_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_s…store_qty desc,
    store_wholesale desc,
    store_sp desc,
    other_qty asc,
    other_wholesale asc,
    other_sp asc,
    ratio asc

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
  refused to write 'query06.preql': not syntactically valid Trilogy.

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
  refused to write 'query78.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 39, column 44.
  Expected one of:
          * SELECT
  Previous tokens: [Token('__ANON_19', '1.49')]

  Location:
  ...sales_price,     -- store the ??? hidden aggregates for having

  Write stats: received 3259 chars / 3259 bytes; tail: …'_wholesale asc,\\n
  other_sp asc,\\n    ratio asc\\n\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.all_sales:sales select sales.date.year, sales.sales_channel, count(sales.quantity) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(sales.quantity) as quantity_count`
  Location:
  ...hannel, count(sales.quantity) ??? limit 10;
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

# Date range filter
where all_sales.date.date between '2000-08-23'::date and '2000…nel, outlet as g_outlet,
    g_channel + g_outlet as level
order by
    level asc,
    channel asc nulls first,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `outlet asc nulls first as outlet_asc_nulls_first`
  Location:
  ...let asc nulls first limit 100; ???

  Write stats: received 1176 chars / 1176 bytes; tail: …'annel asc nulls
  first,\\n    outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  and …l_sales.sales_channel, all_sales.channel_dim_text_id), 0) as profit_total
order by
    channel asc nulls first,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `outlet asc nulls first as outlet_asc_nulls_first`
  Location:
  ...let asc nulls first limit 100; ???

  Write stats: received 1192 chars / 1192 bytes; tail: …'annel asc nulls
  first,\\n    outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  and …G', concat('catalog_page', all_sales.channel_dim_text_id),
        'WEB', concat('web_site', all_sales.channel_dim_text_id)
    ) as outlet
limit 10;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `simple_case(all_sales.sales_channel,
          'STORE', concat('store', all_sales.channel_dim_text_id),
          'CATALOG', concat('catalog_page', all_sales.channel_dim_text_id),
          'WEB', concat('web_site', all_sales.channel_dim_text_id)
      ) as channel_dim_text_id_simple_case`
  Location:
  ..._id)     ) as outlet limit 10; ???

  Write stats: received 513 chars / 513 bytes; tail: …"e',
  all_sales.channel_dim_text_id)\\n    ) as outlet\\nlimit 10;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;
where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  and a…sales.channel_dim_text_id, 'CATALOG', 'catalog_' || all_sales.channel_dim_text_id, 'WEB', 'web_' || all_sales.channel_dim_text_id) as outlet
limit 5;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `simple_case(all_sales.sales_channel, 'STORE', 'store_' ||
  all_sales.channel_dim_text_id, 'CATALOG', 'catalog_' ||
  all_sales.channel_dim_text_id, 'WEB', 'web_' || all_sales.channel_dim_text_id)
  as channel_dim_text_id_simple_case`
  Location:
  ...im_text_id) as outlet limit 5; ???

  Write stats: received 456 chars / 456 bytes; tail: …" 'web_' ||
  all_sales.channel_dim_text_id) as outlet\\nlimit 5;".
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-22'::date
  and …then concat('web_site', all_sales.channel_dim_text_id)
    end) as profit
order by
    channel asc nulls first,
    outlet asc nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `(case when all_sales.sales_channel = 'STORE' then
  concat('store', all_sales.channel_dim_text_id)
           when all_sales.sales_channel = 'CATALOG' then concat('catalog_page',
  all_sales.channel_dim_text_id)
           when all_sales.sales_channel = 'WEB' then concat('web_site',
  all_sales.channel_dim_text_id)
      end) as case_when_all_sales_sales_channel_store_`
  Location:
  ...ollup all_sales.sales_channel, ??? (case when all_sales.sales_ch...

  Write stats: received 2240 chars / 2240 bytes; tail: …'annel asc nulls
  first,\\n    outlet asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query86.preql --content import raw.web_sales as web_sales;

where web_sales.date.year = 2000
select
    web_sales.item.category,
    web_s…el = 1 then cat_rnk else null end as rnk
order by
    level desc nulls first,
    web_sales.item.category nulls first,
    rnk nulls first
limit 100;`

  ```text
  refused to write 'query86.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `gid
      grouping_id(web_sales.item.category, web_sales.item.class) by rollup
  web_sales.item.category as gid_grouping_id_web_sales_item_category_`
  Location:
  ...aid,     --gid     grouping_id ??? (web_sales.item.category, web_...

  Write stats: received 1144 chars / 1144 bytes; tail: …'es.item.category nulls
  first,\\n    rnk nulls first\\nlimit 100;'.
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

- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_zip_prefix. Suggestions: ['store_prefix', 'store.zip', 'pref_prefixes']")
  ```
- `trilogy run query10.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  catalog_sales.ship_customer.id. Suggestions: ['catalog_sales.bill_customer.id',
  'catalog_sales.ship_mode.id', 'catalog_sales.ship_date.id']")
  ```
- `trilogy run query26.preql`

  ```text
  (UndefinedConceptException(...), 'line: 4: Undefined concept:
  local.quantity.')
  ```
- `trilogy run query33.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date.year. Suggestions: ['sales.date.year']")
  ```
- `trilogy run query38.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web_sales.sold_date.date. Suggestions: ['web_sales.date.date',
  'web_sales.ship_date.date', 'catalog_sales.sold_date.date']")
  ```
- `trilogy run query41.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  profile_manufacts.manufact. Suggestions: ['profile_manufacts']")
  ```
- `trilogy run query42.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept: year.')
  ```
- `trilogy run query42.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept: year.')
  ```
- `trilogy --debug run query42.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept: year.')
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
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\executor.py",
  …
  ine 548, in require
      return self._env.concepts  # type: ignore
             ~~~~~~~~~~~~~~~~~~^^^^^^^^^
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\core\models\environment.p
  y", line 299, in __getitem__
      self.raise_undefined(key, line_no, file)
      ~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\core\models\environment.p
  y", line 257, in raise_undefined
      raise UndefinedConceptException(message, matches)
  trilogy.core.exceptions.UndefinedConceptException:
  (UndefinedConceptException(...), 'Undefined concept: year.')
  ```
- `trilogy run query49.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept: item.id.
  Suggestions: ['item', 'sales.item.id']")
  ```
- `trilogy run query71.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.brand_id. Suggestions: ['all_sales.item.brand_id']")
  ```
- `trilogy run query73.preql`

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
- `trilogy run query19.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.brand_id. Suggestions: ['store_sales.item.brand_id']")
  ```
- `trilogy run query19.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date.year. Suggestions: ['store_sales.date.year']")
  ```

### `cli-misuse`

- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy run --import raw.physical_sales:ss --import raw.catalog_sales:cs merge ss.item.id into ~cs.item.id; merge ss.billing_customer.id into ~cs.bill_custom…and cs.sold_date.year=2001 and cs.sold_date.quarter in (1,2,3) and ss.billing_customer.id = cs.bill_customer.id and ss.item.id = cs.item.id limit 10;`

  ```text
  'merge ss.billing_customer.id into ~cs.bill_customer.id;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.physical_sales:ss --import raw.catalog_sales:cs merge ss.item.id into ~cs.item.id; merge ss.billing_customer.id into ~cs.bill_custom… sc, cs.bill_customer.id as cc where ss.date.year=2001 and ss.date.quarter=1 and cs.sold_date.year=2001 and cs.sold_date.quarter in (1,2,3) limit 10;`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.sales_channel:sc select distinct sc.sales_channel;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...hannel as sc; select distinct ??? sc.sales_channel;
  ```
- `trilogy run --import raw.sales_channel:sc select distinct sc.sales_channel as sc;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...hannel as sc; select distinct ??? sc.sales_channel as sc;
  ```
- `trilogy run --import raw.all_sales:s select distinct s.sales_channel as chan;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...l_sales as s; select distinct ??? s.sales_channel as chan;
  ```
- `trilogy run --import raw.all_sales select distinct all_sales.sales_channel as chan;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...aw.all_sales; select distinct ??? all_sales.sales_channel as cha...
  ```
- `trilogy run query73.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  item.id.')
  ```
- `trilogy run query73.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  row_counter. Suggestions: ['store_sales.row_counter']")
  ```
- `trilogy run query76.preql duckdb`

  ```text
  (UndefinedConceptException(...), "line: 11: Undefined
  concept: date.year. Suggestions: ['web_sales.date.year',
  'store_sales.date.year']")
  ```
- `trilogy run query79.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  billing_customer.last_name. Suggestions:
  ['store_sales.billing_customer.last_name',
  'store_sales.billing_customer.first_name',
  'store_sales.billing_customer.full_name']")
  ```

### `join-resolution`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type ArrayType<STRING>' passed into
  SUBSTRING function from function SPLIT in position 1. Valid: 'STRING'
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.intersecting_zips. Valid:
  'STRING'.
  ```

### `cli-misuse`

- `trilogy run duckdb --import raw.all_sales:sales select case(sales.sales_channel, 'STORE', 'store_', 'CATALOG', 'catalog_page_', 'WEB', 'web_site_', 'unknown_') as prefix, sales.channel_dim_text_id as out, sales.ext_sales_price limit 10;`

  ```text
  'select case(sales.sales_channel, 'STORE', 'store_', 'CATALOG', 'catalog_page_', 'WEB', 'web_site_', 'unknown_') as prefix, sales.channel_dim_text_id as out, sales.ext_sales_price limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.all_sales:sales duckdb select case(sales.sales_channel, 'STORE', 'store_', 'CATALOG', 'catalog_page_', 'WEB', 'web_site_', 'unknown_') as prefix, sales.channel_dim_text_id as out, sales.ext_sales_price limit 10;`

  ```text
  'select case(sales.sales_channel, 'STORE', 'store_', 'CATALOG', 'catalog_page_', 'WEB', 'web_site_', 'unknown_') as prefix, sales.channel_dim_text_id as out, sales.ext_sales_price limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql --show all`

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
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```

### `file-not-found`

- `trilogy run query37.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  catalog_sales_item_items does not exist!
  Did you mean "catalog_sales"?

  LINE 10: ... (select catalog_sales_item_items."catalog_sales_item_id" from
  catalog_sales_item_items where catalog_sales_item_items...
                                                                             ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "inventory_warehouse_inventory"."inv_item_sk" as "inventory_item_id"
  FROM
      "inventory" as "inventory_warehouse_inventory"
      INNER JOIN "date_dim" as "inventory_date_date" on
  "inventory_warehouse_inventory"."inv_d
  …
     "inventory_item_items"."I_ITEM_ID" as "item_code",
      "inventory_item_items"."I_ITEM_DESC" as "description",
      "inventory_item_items"."I_CURRENT_PRICE" as "inventory_item_current_price"
  FROM
      "item" as "inventory_item_items"
      INNER JOIN "cheerful" on "inventory_item_items"."I_ITEM_SK" =
  "cheerful"."inventory_item_id"
  WHERE
      "inventory_item_items"."I_CURRENT_PRICE" BETWEEN 68 AND 98 and
  "inventory_item_items"."I_MANUFACT_ID" in (677,940,694,808)

  GROUP BY
      1,
      2,
      3
  ORDER BY
      "item_code" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query64.preql`

  ```text
  (_duckdb.CatalogException) Catalog Error: Table with name
  store_returns_item_items does not exist!
  Did you mean "store_returns"?

  LINE 18: ... (select store_returns_item_items."store_returns_item_id" from
  store_returns_item_items where store_returns_item_items...
                                                                             ^
  [SQL:
  WITH
  friendly as (
  SELECT
      "store_returns_store_returns"."SR_TICKET_NUMBER" as
  "store_returns_ticket_number"
  FROM
      "store_returns" as "store_returns_store_returns"
  GROUP BY
      1),
  wakeful as (
  SELECT
      "store_sales_item_items"."I_ITEM_SK"
  …
  "cust_city" as "cust_city",
      "protective"."cust_zip" as "cust_zip",
      "protective"."sale_year" as "sale_year",
      "protective"."first_sales_yr" as "first_sales_yr",
      "protective"."first_shipto_yr" as "first_shipto_yr",
      "protective"."cnt" as "cnt",
      "protective"."wholesale_sum" as "wholesale_sum",
      "protective"."list_price_sum" as "list_price_sum",
      "protective"."coupon_sum" as "coupon_sum"
  FROM
      "protective"
  WHERE
      "protective"."sale_year" in (1999,2000)

  ORDER BY
      "protective"."cnt" desc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
