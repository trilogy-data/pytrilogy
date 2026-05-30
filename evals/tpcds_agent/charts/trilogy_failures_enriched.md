# Trilogy failure analysis — 20260530-022935

- Run `20260530-022934_enriched` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 707 | failed: 99 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 32 | 32% |
| `syntax-parse` | 32 | 32% |
| `cli-misuse` | 21 | 21% |
| `syntax-missing-alias` | 4 | 4% |
| `join-resolution` | 4 | 4% |
| `undefined-concept` | 4 | 4% |
| `type-error` | 1 | 1% |
| `file-not-found` | 1 | 1% |

## Detail

### `other`

- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw.date:date select date.week_seq, date.year, date.day_of_week order by date.id limit 20;`

  ```text
  ORDER BY references 'date.id', which is not in the SELECT
  projection (line 2). Add it to SELECT to sort by it — prefix with `--` to keep
  it out of the output rows, e.g. `select ..., --date.id order by date.id asc`.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.billing_customer.id as cid, all_sales.date.year as yr, all_sales.sales_channel as ch, sum(((all…iscount_amount) + all_sales.ext_sales_price) / 2) as yt where all_sales.date.year = 2001 and all_sales.sales_channel = 'CATALOG' and yt > 0 limit 20;`

  ```text
  Cannot reference an aggregate derived in the select
  (local.yt) in the same statement where clause; move to the HAVING clause
  instead; Line: 2
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 82 (char 81). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query06.preql`

  ```text
  Multiple where clauses are not supported
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 96 (char 95). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 87 (char 86). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query10.preql duckdb`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 73 (char 72). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 4 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 89 (char 88). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 5 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 6 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy run query13.preql`

  ```text
  Unable to import '.\physical_sales.preql': [Errno 2] No such
  file or directory: '.\\physical_sales.preql'. Did you mean: raw.physical_sales?
  ```
- `trilogy run -e SELECT count(1) FROM store_sales as ss JOIN date_dim as d ON ss.ss_sold_date_sk = d.d_date_sk WHERE d.d_year = 2001 LIMIT 10;`

  ```text
  Some scripts failed during execution.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 85 (char 84). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 1322 (char 1321). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 85 (char 84). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.
  ```
- `trilogy run query14.preql duckdb`

  ```text
  HAVING references 'local.overall_avg_sale', which is not in
  the SELECT projection (line 25). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.overall_avg_sale`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 86 (char 85). Re-issue the call with valid JSON arguments.
  ```

### `syntax-parse`

- `trilogy run --import raw.date:date select * from (select date.week_seq, date.year, date.day_of_week order by date.id limit 10);`

  ```text
  --> 2:8
    |
  2 | select * from (select date.week_seq, date.year, date.day_of_week order by
  date.id limit 10);
    |        ^---
    |
    = expected select_item
  Location:
  ...port raw.date as date; select ??? * from (select date.week_seq,
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.year, count(all_sales.billing_customer.id) as cnt where all_sales.date.year in (2001, 2002) group by all_sales.date.year order by all_sales.date.year;`

  ```text
  --> 2:115
    |
  2 | select all_sales.date.year, count(all_sales.billing_customer.id) as cnt
  where all_sales.date.year in (2001, 2002) group by all_sales.date.year order by
  all_sales.date.year;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...les.date.year in (2001, 2002) ??? group by all_sales.date.year o...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.billing_customer.id as cid, all_sales.date.year as yr, all_sales.sales_channel as ch, sum(((all…ustomer.id in (select all_sales.billing_customer.id where all_sales.date.year = 2001 and all_sales.sales_channel = 'CATALOG' having yt > 0) limit 20;`

  ```text
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside
  parens)? Trilogy does not support subqueries — joins are auto-resolved from
  dotted paths. To filter on a value that lives on a related dimension, reference
  its dot-path directly. Example: instead of `where ss.store_id in (select
  store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ..._sales.billing_customer.id in ??? (select all_sales.billing_cust...
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

# Date range filter
where all_sales.date.date between '2000-08-23'::date and '2000…nel as channel,
    outlet,
    total_sales,
    total_returns,
    total_profit
order by
    channel nulls first,
    outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 7, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...ness code prefixed with kind) ??? auto outlet <- case(     all_s...

  Write stats: received 1202 chars / 1202 bytes; tail: …'\\n    channel nulls
  first,\\n    outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date

auto …nel as channel,
    outlet,
    total_sales,
    total_returns,
    total_profit
order by
    channel nulls first,
    outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 5, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? auto outlet <- case(     all_s...

  Write stats: received 1072 chars / 1072 bytes; tail: …'\\n    channel nulls
  first,\\n    outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date

auto …sales.return_net_loss) by rollup all_sales.sales_channel, outlet as total_profit
order by
    channel nulls first,
    outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 5, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '2000-09-06'::date  ??? auto outlet <- case(     all_s...

  Write stats: received 849 chars / 849 bytes; tail: …'\\n    channel nulls
  first,\\n    outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

select
    all_sales.sales_channel as channel,
    case(
        all_sales.sales_c…ofit
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by
    channel nulls first,
    outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 9, column 7.
  Expected one of:
          * WHEN

  Location:
  ...es.channel_dim_text_id)     ) ??? as outlet,     sum(all_sales.e...

  Write stats: received 1753 chars / 1753 bytes; tail: …'\\n    channel nulls
  first,\\n    outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

select
    all_sales.sales_channel as channel,
    case(
        all_sales.sales_c…ofit
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by
    channel nulls first,
    outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('THEN', 'then') at line 6, column 43.
  Expected one of:
          * RPAR
          * COMMA

  Location:
  ...sales.sales_channel = 'STORE' ??? then concat('store_', all_sale...

  Write stats: received 1765 chars / 1765 bytes; tail: …'\\n    channel nulls
  first,\\n    outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

select
    all_sales.sales_channel as channel,
    concat(
        case(
         …ofit
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by
    channel nulls first,
    outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 10, column 10.
  Expected one of:
          * WHEN

  Location:
           'web_site_'         ) ??? ,         all_sales.channel_di...

  Write stats: received 1433 chars / 1433 bytes; tail: …'\\n    channel nulls
  first,\\n    outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

auto outlet_prefix <- case(
    all_sales.sales_channel = 'STORE',   'store_',
   …ofit
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by
    channel nulls first,
    outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 7, column 2.
  Expected one of:
          * WHEN

  Location:
  ...alog_page_',     'web_site_' ) ??? ;  auto outlet <- concat(outle...

  Write stats: received 768 chars / 768 bytes; tail: …'\\n    channel nulls
  first,\\n    outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.all_sales as all_sales;

auto channel_prefix <- case(
    all_sales.sales_channel = 'STORE',   'store_',
  …ofit
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by
    channel nulls first,
    outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 7, column 2.
  Expected one of:
          * WHEN

  Location:
  ...alog_page_',     'web_site_' ) ??? ;  auto outlet <- concat(chann...

  Write stats: received 770 chars / 770 bytes; tail: …'\\n    channel nulls
  first,\\n    outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -c import raw.all_sales as all_sales;
auto channel_prefix <- case(
    all_sales.sales_channel = 'STORE', 'store_',
    all_…total_profit
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by channel nulls first, outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 6, column 2.
  Expected one of:
          * WHEN

  Location:
  ...alog_page_',     'web_site_' ) ??? ; auto outlet <- concat(channe...

  Write stats: received 757 chars / 757 bytes; tail: …'order by channel nulls
  first, outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -c import raw.all_sales as all_sales;
auto outlet <- concat(
    case(
        all_sales.sales_channel = 'STORE', 'store_',
…total_profit
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by channel nulls first, outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('COMMA', ',') at line 7, column 6.
  Expected one of:
          * WHEN

  Location:
  ...e_',         'web_site_'     ) ??? ,     all_sales.channel_dim_te...

  Write stats: received 744 chars / 744 bytes; tail: …'order by channel nulls
  first, outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -c import raw.all_sales as all_sales;
auto outlet <- case(
    all_sales.sales_channel = 'STORE',   concat('store_', all_sal…total_profit
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by channel nulls first, outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 6, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ; select     all_sales.sales_c...

  Write stats: received 798 chars / 798 bytes; tail: …'order by channel nulls
  first, outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -c import raw.all_sales as all_sales;
auto outlet <- case(
    all_sales.sales_channel = 'STORE',   concat('store_', all_sal…total_profit
where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
order by channel nulls first, outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 6, column 2.
  Expected one of:
          * WHEN

  Location:
  ...l_sales.channel_dim_text_id) ) ??? ; select     all_sales.sales_c...

  Write stats: received 798 chars / 798 bytes; tail: …'order by channel nulls
  first, outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Set of 5-digit ZIP codes where >10 preferred …ching_stores

select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ling_customer.address.zip     ??? where billing_customer.preferr...

  Write stats: received 1256 chars / 1256 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

auto pref_cust_zip <- billing_customer.address.…ching_stores

select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 6, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ling_customer.address.zip     ??? where billing_customer.preferr...

  Write stats: received 841 chars / 841 bytes; tail: …'ofit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# ZIPs where >10 preferred customers have their…tore_sales.billing_customer.id) > 10;

select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
limit 5;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ling_customer.address.zip     ??? where store_sales.billing_cust...

  Write stats: received 424 chars / 424 bytes; tail: …'
  sum(store_sales.net_profit) as total_net_profit\\nlimit 5;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Preferred customer ZIP count
auto pref_cust_c…t
auto zips_list <- split(zips, ',');

select
    store_sales.store.name as store_name,
    sum(store_sales.net_profit) as total_net_profit
limit 5;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ling_customer.address.zip     ??? where pref_cust_count > 10;  #...

  Write stats: received 540 chars / 540 bytes; tail: …'
  sum(store_sales.net_profit) as total_net_profit\\nlimit 5;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Set A: the curated list provided via the zips…store_sales.net_profit) as total_net_profit
where store_sales.date.year = 1998
  and store_sales.date.quarter = 2
order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 11, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ling_customer.address.zip     ??? where store_sales.billing_cust...

  Write stats: received 700 chars / 700 bytes; tail: …'e_sales.date.quarter =
  2\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# Set A: the curated list provided via the zips…store_sales.net_profit) as total_net_profit
where store_sales.date.year = 1998
  and store_sales.date.quarter = 2
order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 60.
  Expected one of:
          * _TERMINATOR

  Location:
  ....billing_customer.address.zip ??? where store_sales.billing_cust...

  Write stats: received 696 chars / 696 bytes; tail: …'e_sales.date.quarter =
  2\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

auto pref_zip_counts <- count(store_sales.billi…p as pref_zip
        where store_sales.billing_customer.preferred_cust_flag = 'Y' and pref_zip_counts > 10
    )
order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...ore_sales.store.zip, 1, 2) in ??? (         select substring(pz,...

  Write stats: received 736 chars / 736 bytes; tail: …'ef_zip_counts > 10\\n
  )\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

# First 2 digits of parameter ZIPs
auto pz <- s… store_sales.date.year = 1998
    and store_sales.date.quarter = 2
    and store_sales.store.zip in matching_zips
order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 14, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ling_customer.address.zip     ??? where store_sales.billing_cust...

  Write stats: received 929 chars / 929 bytes; tail: …'ore.zip in
  matching_zips\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

auto param_zips <- split(zips, ',');

auto pref…sales.net_profit) as total_net_profit
where
    store_sales.date.year = 1998
    and store_sales.date.quarter = 2
order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ling_customer.address.zip     ??? where store_sales.billing_cust...

  Write stats: received 586 chars / 586 bytes; tail: …'e_sales.date.quarter =
  2\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.physical_sales as store_sales;

parameter zips string;

auto param_zips <- split(zips, ',');

auto pref…sales.net_profit) as total_net_profit
where
    store_sales.date.year = 1998
    and store_sales.date.quarter = 2
order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 5.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ling_customer.address.zip     ??? where store_sales.billing_cust...

  Write stats: received 582 chars / 582 bytes; tail: …'e_sales.date.quarter =
  2\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Step 1: Identify (brand, class, category) combos appearing in all three channels…es_channel nulls first,
    sales.item.brand_id nulls first,
    sales.item.class_id nulls first,
    sales.item.category_id nulls first
  limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 16, column 42.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ree_channels <- has_store > 0 ??? and has_catalog > 0 and has_we...

  Write stats: received 2156 chars / 2156 bytes; tail: …'s first,\\n
  sales.item.category_id nulls first\\n  limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Per combo, does it appear in STORE during 1999-2001?
auto has_store <- count(sal…es_channel nulls first,
    sales.item.brand_id nulls first,
    sales.item.class_id nulls first,
    sales.item.category_id nulls first
  limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 11, column 42.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ree_channels <- has_store > 0 ??? and has_catalog > 0 and has_we...

  Write stats: received 1857 chars / 1857 bytes; tail: …'s first,\\n
  sales.item.category_id nulls first\\n  limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql -e -c import raw.all_sales as sales;

# Per combo, does it appear in STORE during 1999-2001?
auto has_store <- count(sales.o…es_channel nulls first,
    sales.item.brand_id nulls first,
    sales.item.class_id nulls first,
    sales.item.category_id nulls first
  limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 11, column 42.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ree_channels <- has_store > 0 ??? and has_catalog > 0 and has_we...

  Write stats: received 1857 chars / 1857 bytes; tail: …'s first,\\n
  sales.item.category_id nulls first\\n  limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Check presence in each channel 1999-2001
auto has_store <- count(sales.order_id …es_channel nulls first,
    sales.item.brand_id nulls first,
    sales.item.class_id nulls first,
    sales.item.category_id nulls first
  limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 9, column 42.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ree_channels <- has_store > 0 ??? and has_catalog > 0 and has_we...

  Write stats: received 1414 chars / 1414 bytes; tail: …'s first,\\n
  sales.item.category_id nulls first\\n  limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;
auto has_store <- count(sales.order_id ? sales.sales_channel = 'STORE' and sales.da…ore > 0 and has_catalog > 0 and has_web > 0;
auto overall_avg_sale <- avg(sales.quantity * sales.list_price ? sales.date.year between 1999 and 2001);`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 5, column 42.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ree_channels <- has_store > 0 ??? and has_catalog > 0 and has_we...

  Write stats: received 762 chars / 762 bytes; tail: …'* sales.list_price ?
  sales.date.year between 1999 and 2001);'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;
auto has_store <- count(sales.order_id ? sales.sales_channel = 'STORE' and sales.da…e > 0
 and has_catalog > 0
 and has_web > 0;
auto overall_avg_sale <- avg(sales.quantity * sales.list_price ? sales.date.year between 1999 and 2001);`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 6, column 2.
  Expected one of:
          * _TERMINATOR

  Location:
  ...o all_three <- has_store > 0  ??? and has_catalog > 0  and has_w...

  Write stats: received 755 chars / 755 bytes; tail: …'* sales.list_price ?
  sales.date.year between 1999 and 2001);'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;
auto has_store <- count(sales.order_id ? sales.sales_channel = 'STORE' and sales.da…ales.item.category_id;
auto all_three <- has_store > 0 and has_catalog > 0 and has_web > 0;
same as has_store;
same as has_catalog;
same as has_web;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 5, column 33.
  Expected one of:
          * _TERMINATOR

  Location:
  ...to all_three <- has_store > 0 ??? and has_catalog > 0 and has_we...

  Write stats: received 706 chars / 706 bytes; tail: …'0;\\nsame as
  has_store;\\nsame as has_catalog;\\nsame as has_web;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `cli-misuse`

- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy read_file raw/physical_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/physical_returns.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file trilogy.toml`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/customer.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/address.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/store.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/date.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/physical_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy run rewrite query04.preql`

  ```text
  'query04.preql' looks like a file path, not a dialect. The dialect argument comes AFTER the input file.
    Try: trilogy run query04.preql <dialect>
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy read_file raw/physical_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy read_file raw/physical_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy explore raw/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
  ```
- `trilogy read_file raw/physical_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/web_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```
- `trilogy explore raw/database_description.txt`

  ```text
  Invalid value for 'PATH': File 'raw/database_description.txt' does not exist.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.date:date select min(date.week_seq), max(date.week_seq), min(date.year), max(date.year);`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ate; select min(date.week_seq) ??? , max(date.week_seq), min(date...
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.billing_customer.id, all_sales.date.year, count(all_sales.order_id) where all_sales.date.year in (2001, 2002) and all_sales.sales_channel = 'CATALOG' and all_sales.billing_customer.id in (1727) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ar, count(all_sales.order_id) ??? where all_sales.date.year in (...
  ```
- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.billing_customer.id from all_sales where all_sales.date.year in (2001, 2002) and all_sales.sales_channel = 'CATALOG' limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...as all_sales; select distinct ??? all_sales.billing_customer.id
  ```
- `trilogy run --import raw.physical_sales:store_sales select count(store_sales.ticket_number), store_sales.date.year where store_sales.date.year = 2001 limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...unt(store_sales.ticket_number) ??? , store_sales.date.year where
  ```

### `join-resolution`

- `trilogy run --import raw.all_sales:all_sales --import raw.customer:customer select all_sales.billing_customer.id, all_sales.date.year, all_sales.sales_channe….preferred_cust_flag where all_sales.date.year = 2002 and all_sales.sales_channel = 'STORE' and all_sales.billing_customer.id = customer.id limit 30;`

  ```text
  Could not resolve connections for query with output
  ['all_sales.billing_customer.id<Purpose.KEY>Derivation.ROOT>',
  'all_sales.date.year<Purpose.PROPERTY>Derivation.ROOT>',
  'all_sales.sales_channel<Purpose.KEY>Derivation.ROOT>',
  'local.yt<Purpose.METRIC>Derivation.AGGREGATE>',
  'customer.text_id<Purpose.UNIQUE_PROPERTY>Derivation.ROOT>',
  'customer.first_name<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.last_name<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.preferred_cust_flag<Purpose.PROPERTY>Derivation.ROOT>'] from current
  model.
  ```
- `trilogy run query16.preql`

  ```text
  No datasource exists for root concept
  cs.warehouse_id@Grain<cs.warehouse_id>, and no resolvable pseudonyms found from
  set(). This query is unresolvable from your environment. Check your datasources
  and imports to make sure this concept is bound.
  ```
- `trilogy run query16.preql`

  ```text
  No datasource exists for root concept
  cs.warehouse_id@Grain<cs.warehouse_id>, and no resolvable pseudonyms found from
  set(). This query is unresolvable from your environment. Check your datasources
  and imports to make sure this concept is bound.
  ```
- `trilogy run query16.preql`

  ```text
  No datasource exists for root concept
  cs.warehouse_id@Grain<cs.warehouse_id>, and no resolvable pseudonyms found from
  set(). This query is unresolvable from your environment. Check your datasources
  and imports to make sure this concept is bound.
  ```

### `undefined-concept`

- `trilogy run query06.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.category. Suggestions: ['physical_sales.item.category']")
  ```
- `trilogy run query08.preql --param zips=24128`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  billing_customer.id. Suggestions: ['store_sales.billing_customer.id',
  'store_sales.billing_customer.text_id', 'store_sales.billing_customer.login']")
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

### `type-error`

- `trilogy run query08.preql --param zips=24128`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  SUBSTRING function in position 1 from concept: local.pz. Valid: 'STRING'.
  ```

### `file-not-found`

- `trilogy run query12.preql`

  ```text
  Input 'query12.preql' does not exist.
  ```
