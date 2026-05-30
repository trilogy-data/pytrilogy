# Trilogy failure analysis — 20260530-212726

- Run `20260530-212725_base` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 493 | failed: 49 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 18 | 37% |
| `syntax-parse` | 17 | 35% |
| `undefined-concept` | 8 | 16% |
| `syntax-missing-alias` | 4 | 8% |
| `type-error` | 1 | 2% |
| `join-resolution` | 1 | 2% |

## Detail

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

- `trilogy file write query02.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

# Combine web and catalog sales: da… nullif(sum(sales_2001.value ? sales_2001.week_seq + 53 = sales_2002.week_seq and dow = 6), 0) as sat_ratio
order by wk_seq asc nulls first
limit 55;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 8, column 34.
  Expected one of:
          * _TERMINATOR

  Location:
  ...o sales_by_day <- daily_sales ??? by web_sales.sold_date.year, w...

  Write stats: received 1773 chars / 1773 bytes; tail: …'), 0) as
  sat_ratio\\norder by wk_seq asc nulls first\\nlimit 55;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

merge web_sales.sold_date.date_sk i…ow = 6) / nullif(sum(s2002.value ? s2002.week_seq = s2001.week_seq + 53 and s2002.dow = 6), 0) as sat_ratio
order by wk_seq asc nulls first
limit 55;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 10, column 34.
  Expected one of:
          * _TERMINATOR

  Location:
  ...o sales_by_day <- daily_sales ??? by catalog_sales.sold_date.yea...

  Write stats: received 1697 chars / 1697 bytes; tail: …'), 0) as
  sat_ratio\\norder by wk_seq asc nulls first\\nlimit 55;'.
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
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 8, column 1.
  Expected one of:
          * SELECT

  Location:
  ...rrent_price in their category ??? auto avg_cat_price <- avg(stor...

  Write stats: received 715 chars / 715 bytes; tail: …'_count asc nulls first,\\n
  state asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query06.preql --content import raw.store_sales as store_sales;

# Items whose current_price is > 1.2x the avg current_price in their categ…omer.customer_sk) as customer_count
having
    customer_count >= 10
order by
    customer_count asc nulls first,
    state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 7, column 78.
  Expected one of:
          * _TERMINATOR

  Location:
  ...t_price > 1.2 * avg_cat_price ??? by store_sales.item.item_sk;

  Write stats: received 765 chars / 765 bytes; tail: …'_count asc nulls first,\\n
  state asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Split param zips into an array
auto zips_arr <- split(zips, ',');

# First 2 digits of e…d contains(param_prefixes, store_zip_prefix)
    and contains(valid_pref_prefixes, store_zip_prefix)
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'x ') at line 7, column 50.
  Expected one of:
          * "@"
  Previous tokens: [Token('COMMA', ',')]

  Location:
   <- array_transform(zips_arr, ??? x -> substring(x, 1, 2));  # C...

  Write stats: received 1192 chars / 1192 bytes; tail: …'zip_prefix)\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

auto zips_arr <- split(zips, ',');

# Count preferred customers per ZIP
auto pref_cnt_by_z… and store_sales.date_dim.qoy = 2
    and contains(zips_arr, substring(store_sales.store.zip, 1, 2))
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 9, column 89.
  Expected one of:
          * _TERMINATOR

  Location:
  ...r.customer_address.zip, 1, 2) ??? by store_sales.customer.custom...

  Write stats: received 756 chars / 756 bytes; tail: …'zip, 1, 2))\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write test_query.preql --content import raw.store_sales as store_sales;

parameter zips string;

auto zips_arr <- split(zips, ',');
auto pref_cn… as total_net_profit
where
    store_sales.date_dim.year = 1998
    and store_sales.date_dim.qoy = 2
order by store_sales.store.store_name
limit 100;`

  ```text
  refused to write 'test_query.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'x ') at line 10, column 54.
  Expected one of:
          * "@"
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...<- len(array_filter(zips_arr, ??? x -> substring(x, 1, 2) = stor...

  Write stats: received 650 chars / 650 bytes; tail: …'dim.qoy = 2\\norder by
  store_sales.store.store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Total quantity per ticket_number
auto ticket_qty <- sum(store_sales.quan…ty between 81 and 100)
) as bucket_81_100;

select
    bucket_1_20,
    bucket_21_40,
    bucket_41_60,
    bucket_61_80,
    bucket_81_100
limit 1;
`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 13, column 3.
  Expected one of:
          * WHEN

  Location:
  ...icket_qty between 1 and 20) ) ??? as bucket_1_20;  # Bucket 2: 2...

  Write stats: received 2086 chars / 2086 bytes; tail: …' bucket_41_60,\\n
  bucket_61_80,\\n    bucket_81_100\\nlimit 1;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Total quantity per ticket_number
auto ticket_qty <- sum(store_sales.quan…) as bucket_41_60,
    case(b4_cnt > 10097, b4_disc, b4_paid) as bucket_61_80,
    case(b5_cnt > 165306, b5_disc, b5_paid) as bucket_81_100
limit 1;
`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 36, column 44.
  Expected one of:
          * WHEN

  Location:
  ...nt > 74129, b1_disc, b1_paid) ??? as bucket_1_20,     case(b2_cn...

  Write stats: received 2098 chars / 2098 bytes; tail: …'5_cnt > 165306, b5_disc,
  b5_paid) as bucket_81_100\\nlimit 1;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw.store_sales as store_sales;

# Demographic-and-price clause conditions
auto demo_price_match <-
    (st…g(store_sales.ext_wholesale_cost) as avg_extended_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_extended_wholesale_cost
limit 100;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 6, column 6.
  Expected one of:
          * RPAR
          * COMMA

  Location:
  ...ics.marital_status = 'M'      ??? and store_sales.customer.custo...

  Write stats: received 1877 chars / 1877 bytes; tail: …'_wholesale_cost) as
  total_extended_wholesale_cost\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Merge item dimensions…otal_count
having total_sales > all_avg
order by channel nulls first, brand_id nulls first, class_id nulls first, category_id nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 10, column 96.
  Expected one of:
          * _TERMINATOR

  Location:
  ...00 + ss.item.category_id::int ??? by (ss.item.brand_id, ss.item....

  Write stats: received 3140 chars / 3140 bytes; tail: …'t, class_id nulls first,
  category_id nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as catalog_sales;

# Filter: catalog sales in Q2 of 2001
where catalog_sales.sold_date.ye….bill_customer.customer_address.zip as zip_code,
    sum(catalog_sales.sales_price) as total_sales_price
order by zip_code asc nulls first
limit 100;`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 9, column 1.
  Expected one of:
          * SELECT

  Location:
  ...or per-line sales price > 500 ??? where (   substring(catalog_sa...

  Write stats: received 735 chars / 735 bytes; tail: …'tal_sales_price\\norder by
  zip_code asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.year, store_sales.date_dim.fy_year, store_sales.date_dim.qoy, count(store_sales.ticket_number) as cnt where store_sales.date_dim.year is not null group by 1,2,3 order by 1,2 limit 20;`

  ```text
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP
  BY — remove it. Grouping is automatic by the non-aggregated fields in your
  SELECT. To aggregate at a different grain than the select, write `agg(x) by
  dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...les.date_dim.year is not null ??? group by 1,2,3 order by 1,2 li...
  ```
- `trilogy file write query18.preql --content import raw.catalog_sales as catalog_sales;

# Filter conditions
where catalog_sales.sold_date.year = 1998
  and ca…unty nulls first,
    catalog_sales.item.item_id nulls first,
    g_country nulls first,
    g_state nulls first,
    g_county nulls first
limit 100;`

  ```text
  refused to write 'query18.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'def ') at line 11, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('RPAR', ')')]

  Location:
  ...up macro for all the averages ??? def rollup_avg(metric) -> avg(...

  Write stats: received 2638 chars / 2638 bytes; tail: …'    g_state nulls
  first,\\n    g_county nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import raw.catalog_sales as catalog_sales;

# Filter to items in Sports, Books, or Home categories sold between th…c nulls first,
    class asc nulls first,
    item_code asc nulls first,
    description asc nulls first,
    pct_of_class asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...extended sales price per item ??? auto item_ext_total <- sum(cat...

  Write stats: received 1121 chars / 1121 bytes; tail: …'asc nulls first,\\n
  pct_of_class asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `undefined-concept`

- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  s2002.value.')
  ```
- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  s2001.week_seq. Suggestions: ['wk_seq']")
  ```
- `trilogy run query07.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.item_id. Suggestions: ['store_sales.item.item_id']")
  ```
- `trilogy run query07.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date_dim.year. Suggestions: ['store_sales.date_dim.year',
  'store_sales.date_dim.fy_year']")
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  store_sales.customer.preferred_cust_flag.')
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  cs.ticket_number. Suggestions: ['ss.ticket_number', 'cs.order_number',
  'ss.store.suite_number']")
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  brand_id. Suggestions: ['ws.item.brand_id', 'ss.item.brand_id',
  'cs.item.brand_id']")
  ```
- `trilogy run query14.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  brand_id. Suggestions: ['ws.item.brand_id', 'ss.item.brand_id',
  'cs.item.brand_id']")
  ```

### `syntax-missing-alias`

- `trilogy file write query02.preql --content import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sales;

merge web_sales.sold_date.date_sk i…s wk_seq,
    sum(sun_2002 ? catalog_sales.sold_date.week_seq = sun_2001.week_seq + 53) by catalog_sales.sold_date.week_seq
order by wk_seq
limit 10;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `sum(sun_2002 ? catalog_sales.sold_date.week_seq =
  sun_2001.week_seq + 53) by catalog_sales.sold_date.week_seq as
  sum_sun_2002_catalog_sales_sold_date_wee`
  Location:
  ...sales.sold_date.week_seq order ??? by wk_seq limit 10;

  Write stats: received 4349 chars / 4349 bytes; tail: …'y
  catalog_sales.sold_date.week_seq\\norder by wk_seq\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

merge ss.store.store_sk into ~sr.store.store_sk;

#…t_profit2) by coalesce(st_outlet, concat('store_', coalesce(sr.store.store_id, 'unknown'))) as profit
order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `outlet nulls first as outlet_nulls_first`
  Location:
   outlet nulls first limit 100; ???

  Write stats: received 1297 chars / 1297 bytes; tail: …')) as profit\\norder by
  channel, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write test_query.preql --content import raw.store_sales as store_sales;

select
    store_sales.customer.preferred_cust_flag,
    count(store_sales.ticket_number)
limit 10;`

  ```text
  refused to write 'test_query.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y` Here: `count(store_sales.ticket_number) as ticket_number_count`
  Location:
  ...ore_sales.ticket_number) limit ??? 10;

  Write stats: received 139 chars / 139 bytes; tail: …'ed_cust_flag,\\n
  count(store_sales.ticket_number)\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date_dim.fy_year, store_sales.date_dim.qoy, count(store_sales.ticket_number) limit 100;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y` Here: `count(store_sales.ticket_number) as
  ticket_number_count`
  Location:
  ...nt(store_sales.ticket_number) ??? limit 100;
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  CONTAINS function in position 1 from concept: local.zips_arr. Valid: 'STRING'.
  ```

### `join-resolution`

- `trilogy run query14.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>',
  'item.brand_id<Purpose.PROPERTY>Derivation.ROOT>',
  'item.class_id<Purpose.PROPERTY>Derivation.ROOT>',
  'item.category_id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.level<Purpose.METRIC>Derivation.BASIC>',
  'local.overall_avg<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
