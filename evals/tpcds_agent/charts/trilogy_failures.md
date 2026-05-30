# Trilogy failure analysis — 20260530-022935

- Run `20260530-022934_base` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 690 | failed: 94 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 32 | 34% |
| `syntax-parse` | 31 | 33% |
| `syntax-missing-alias` | 11 | 12% |
| `undefined-concept` | 10 | 11% |
| `join-resolution` | 6 | 6% |
| `cli-misuse` | 4 | 4% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text
  Unable to import '.\store_returns.preql': [Errno 2] No such
  file or directory: '.\\store_returns.preql'. Did you mean: raw.store_returns?
  ```
- `trilogy run query01.preql`

  ```text
  HAVING references 'local.cust_store_return', which is not in
  the SELECT projection (line 9). Fix one of: (a) add it to SELECT — prefix with
  `--` to keep it out of the output rows, e.g. `select ...,
  --local.cust_store_return`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws --env merge cs.sold_date.date_sk into ~ws.sold_date.date_sk select cs.sold_date.week_seq as ws, sum(cs.ext_sales_price) + sum(ws.ext_sales_price) as combined where cs.sold_date.year = 2001 limit 10;`

  ```text
  Environment variable must be in KEY=VALUE format or be a path to an existing
  env file: merge cs.sold_date.date_sk into ~ws.sold_date.date_sk
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid \escape: line 1 column 85 (char 84). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting value: line 1 column 53 (char 52). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql`

  ```text
  Received invalid type <class
  'trilogy.core.models.author.Between'> ref:store_sales.date_dim.date between
  constant(2000-08-23) and constant(2000-09-06) as input to concept derivation:
  `auto date_condition <- store_sales.date_dim.date between '2000-08-23'::date
  and '2000-09-06'::date`
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query07.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 77 (char 76). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 77 (char 76). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 77 (char 76). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 77 (char 76). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 93 (char 92). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query10.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy run query10.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.web_sales:ws select count(ss.customer.customer_sk ? ss.date_dim.year = 2001) as c1, count(ws.bill_custom…ear = 2001) as c2, count(ss.customer.customer_sk ? ss.date_dim.year = 2001 and ws.bill_customer.customer_sk = ss.customer.customer_sk) as c3 limit 1;`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local._virt_filter_customer_sk_473070976342776',
  'local._virt_filter_customer_sk_2798107096837191'} out of  with found
  {'local._virt_filter_customer_sk_473070976342776', 'ss.customer.customer_sk'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 91 (char 90). Re-issue the call with valid JSON arguments.
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
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 95 (char 94). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 84 (char 83). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 97 (char 96). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 86 (char 85). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  trilogy error: 'args' must be a list of strings.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 3 column 1 (char 100). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 4 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Invalid control character at: line 1 column 95 (char 94). Re-issue the call with valid JSON arguments.
  ```

### `syntax-parse`

- `trilogy run --debug --import raw.date_dim:date_dim select date_dim.week_seq, date_dim.year, count(date_dim.date_sk) from date_dim where date_dim.year in (2001,2002) group by date_dim.week_seq, date_dim.year order by date_dim.week_seq limit 20;`

  ```text
  …
   automatic).
  Location:
  ...year, count(date_dim.date_sk) ??? from date_dim where date_dim.y...
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\pest_backend.p
  y", line 321, in parse_pest
      tree = parse_trilogy_syntax_tuple(text)
  ValueError:  --> 2:66
    |
  2 | select date_dim.week_seq, date_dim.year, count(date_dim.date_sk) from
  date_dim where date_dim.year in (2001,2002) group by date_dim.week_seq,
  date_dim.year order by date_dim.week_seq limit 20;
    |                                                                  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over

  The above exception was the direct cause of the following exception:

  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\scripts\parallel_executio
  n.py", line 578, in run_single_script_execution
      queries = exec.parse_text(
          text, root=base if isinstance(base, Path) else None
      )
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\executor.py", line
  700, in parse_text
      r
  …
  ```
- `trilogy file write query05.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as cata…mbine and rollup
select
    channel,
    outlet,
    total_sales,
    total_returns,
    total_profit
order by channel, outlet nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 29, column 240.
  Expected one of:
          * _TERMINATOR

  Location:
  ...date and '2000-09-06'::date)) ??? by store_sales.store.store_sk;...

  Write stats: received 3657 chars / 3657 bytes; tail: …'total_profit\\norder by
  channel, outlet nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -e -c import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as catalog_… as total_sales,
    store_returns_amt as total_returns,
    store_profit as total_profit
having 1=1
order by channel, outlet nulls first
limit 100;
`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 29, column 240.
  Expected one of:
          * _TERMINATOR

  Location:
  ...date and '2000-09-06'::date)) ??? by store_sales.store.store_sk;...

  Write stats: received 3819 chars / 3819 bytes; tail: …'\\nhaving 1=1\\norder by
  channel, outlet nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# First 2 digits of a ZIP code
auto zip_prefix <- substring(zip, 1, 2);

# Set of 5-digit …efix)

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 10, column 70.
  Expected one of:
          * _TERMINATOR

  Location:
  ...r.customer_address.zip, 1, 5) ??? where customer.preferred_cust_...

  Write stats: received 1314 chars / 1314 bytes; tail: …'fit) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Set of 5-digit ZIP codes where more than 10 preferred customers have their home address
# Pr…efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 70.
  Expected one of:
          * _TERMINATOR

  Location:
  ...r.customer_address.zip, 1, 5) ??? where customer.preferred_cust_...

  Write stats: received 1199 chars / 1199 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Set of 5-digit ZIP codes where more than 10 preferred customers have their home address
auto…efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 6, column 36.
  Expected one of:
          * _TERMINATOR

  Location:
  ...eligible_zip <- pref_cust_zip ??? where pref_cust_cnt > 10; auto...

  Write stats: received 1004 chars / 1004 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Set of 5-digit ZIP codes where more than 10 preferred customers have their home address
# Co…efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  …
  string(param_zips, 1, 2);\n\n# Prefixes that
  appear in BOTH sets\nauto matching_prefix <- param_prefix where param_prefix in
  eligible_prefix;\n\n# Store sales in 2nd quarter of 1998\nwhere
  store_sales.date_dim.year = 1998\n  and store_sales.date_dim.qoy = 2\n  and
  substring(store_sales.store.zip, 1, 2) in matching_prefix\n\nselect\n
  store_sales.store.store_name as store_name,\n    sum(store_sales.net_profit) as
  total_net_profit\n\norder by store_name asc\nlimit 100;\n") at line 5, column
  143.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'substring')]

  Location:
  ...address.zip) = 5) by substring ??? (customer.customer_address.zip...

  Write stats: received 1064 chars / 1064 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# First, get the set of 5-digit ZIP codes where more than 10 preferred customers
# have their …efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 36.
  Expected one of:
          * _TERMINATOR

  Location:
  ...kept_zip <- eligible_full_zip ??? where pref_cust_cnt > 10; auto...

  Write stats: received 980 chars / 980 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Count preferred customers per ZIP code
auto pref_cust_cnt <- count(customer.customer_sk ? cu…efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  …
  it(zips, ','), 1, 5), 1, 2);\n\n#
  Intersecting prefixes (those appearing in BOTH sets)\nauto matching_prefix <-
  param_prefix where param_prefix in eligible_prefix;\n\n# Final query\nwhere
  store_sales.date_dim.year = 1998\n  and store_sales.date_dim.qoy = 2\n  and
  substring(store_sales.store.zip, 1, 2) in matching_prefix\n\nselect\n
  store_sales.store.store_name as store_name,\n    sum(store_sales.net_profit) as
  total_net_profit\n\norder by store_name asc\nlimit 100;\n") at line 4, column
  143.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'substring')]

  Location:
  ...address.zip) = 5) by substring ??? (customer.customer_address.zip...

  Write stats: received 976 chars / 976 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Count preferred customers per ZIP code
auto pref_cust_cnt <- count(customer.customer_sk ? cu…efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 91.
  Expected one of:
          * _TERMINATOR

  Location:
  ...er_address.zip, 1, 5)), 1, 2) ??? where pref_cust_cnt > 10;  # P...

  Write stats: received 980 chars / 980 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Count preferred customers per ZIP code
auto pref_cust_cnt <- count(customer.customer_sk ? cu…efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 35.
  Expected one of:
          * _TERMINATOR

  Location:
   eligible_zip <- eligible_raw ??? where pref_cust_cnt > 10; auto...

  Write stats: received 1048 chars / 1048 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Count preferred customers per ZIP code
auto pref_cust_cnt <- count(customer.customer_sk ? cu…efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 93.
  Expected one of:
          * _TERMINATOR

  Location:
  ...r_address.zip, 1, 5)), 1, 2)) ??? where pref_cust_cnt > 10;  # P...

  Write stats: received 1038 chars / 1038 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Count preferred customers per 5-digit ZIP
auto pref_cust_cnt <- count(customer.customer_sk ?…efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 8, column 57.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ubstring(eligible_zip, 1, 2)) ??? and pref_cust_cnt > 10;  # Par...

  Write stats: received 965 chars / 965 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as web_sales;

# Filter: items in 'Sports', 'Books', or 'Home' categories, sold between 1999-…eb_sales.item.class as pct_of_class
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
  Unexpected token Token('ORDER_IDENTIFIER', 'auto ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...em total extended sales price ??? auto item_ext_sales_price <- s...

  Write stats: received 914 chars / 914 bytes; tail: …'de asc,\\n    description
  asc,\\n    pct_of_class asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query12.preql --content import raw.web_sales as web_sales;

# Filter: items in 'Sports', 'Books', or 'Home' categories, sold between 1999-…100.0 / class_total as pct_of_class
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
  ...:date and '1999-03-24'::date  ??? auto item_total <- sum(web_sal...

  Write stats: received 859 chars / 859 bytes; tail: …'de asc,\\n    description
  asc,\\n    pct_of_class asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select count(store_sales.ticket_number) as cnt, store_sales.customer.customer_demographics.marital_status as…p_count as hd_dep where store_sales.date_dim.year = 2001 and store_sales.customer.customer_address.country = 'United States' group by 2,3,4 limit 20;`

  ```text
  --> 2:352
    |
  2 | select count(store_sales.ticket_number) as cnt,
  store_sales.customer.customer_demographics.marital_status as ms,
  store_sales.customer.customer_demographics.education_status as ed,
  store_sales.customer.household_demographics.dep_count as hd_dep where
  store_sales.date_dim.year = 2001 and
  store_sales.customer.customer_address.country = 'United States' group by 2,3,4
  limit 20;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ess.country = 'United States' ??? group by 2,3,4 limit 20;
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…erent approach - compute per-channel aggregates directly

# Step 3: Compute overall average sale value across all three channels 1999-2001

select 1;`

  ```text
  …
  em.class_id,\n
  web_sales.item.category_id,\n        web_sales.quantity * web_sales.list_price
  as sale_value\n    where web_sales.sold_date.year = 2001\n      and
  web_sales.sold_date.moy = 11;\n\n# Union all channel Nov 2001 sales\n# But
  first, I need to think about how to do this in Trilogy...\n# Let me use a
  different approach - compute per-channel aggregates directly\n\n# Step 3:
  Compute overall average sale value across all three channels
  1999-2001\n\nselect 1;") at line 7, column 26.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'distinct')]

  Location:
  ...riod auto ss_items <- distinct ??? (store_sales.item.brand_id, st...

  Write stats: received 2427 chars / 2427 bytes; tail: …'ge sale value across all
  three channels 1999-2001\\n\\nselect 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…y ? catalog_sales.sold_date.year between 1999 and 2001)
    + count(web_sales.quantity ? web_sales.sold_date.year between 1999 and 2001));

select 1;`

  ```text
  …
  LECT
          * SHORTHAND_MODIFIER
          * SELF_IMPORT
          * WITH
          * IMPORT
          * PARSE_COMMENT
          * UNIQUE
          * RAW_SQL
          * PUBLISH_ACTION
          * DATASOURCE_PARTIAL
          * PARAM
          * WHERE
          * SHOW
          * TYPE
          * CHART
          * PARAMETER
          * _PROPERTIES
          * CREATE
          * DATASOURCE_ROOT
          * COPY
          * ROWSET
          * VALIDATE
          * FROM
          * PERSIST_MODE
          * MOCK
          * _DEF_TABLE
          * DATASOURCE
          * $END
  Previous tokens: [Token('PARSE_COMMENT', '# Store sales overall avg\n')]

  Location:
  ...ne  # Store sales overall avg ??? prop overall_avg_sale numeric

  Write stats: received 909 chars / 909 bytes; tail: …'web_sales.sold_date.year
  between 1999 and 2001));\\n\\nselect 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…b_sales.item.brand_id, web_sales.item.class_id, web_sales.item.category_id)
    where web_sales.sold_date.year between 1999 and 2001;

select 1 as x;`

  ```text
  …
  ore_sales.item.class_id, store_sales.item.category_id)\n    where
  store_sales.date_dim.year between 1999 and 2001;\n\nauto cs_bcc <-
  distinct(catalog_sales.item.brand_id, catalog_sales.item.class_id,
  catalog_sales.item.category_id)\n    where catalog_sales.sold_date.year between
  1999 and 2001;\n\nauto ws_bcc <- distinct(web_sales.item.brand_id,
  web_sales.item.class_id, web_sales.item.category_id)\n    where
  web_sales.sold_date.year between 1999 and 2001;\n\nselect 1 as x;') at line 25,
  column 24.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'distinct')]

  Location:
  ...-2001  auto ss_bcc <- distinct ??? (store_sales.item.brand_id, st...

  Write stats: received 1779 chars / 1779 bytes; tail: …'_sales.sold_date.year
  between 1999 and 2001;\\n\\nselect 1 as x;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale….year = 2001
      and web_sales.sold_date.moy = 11
    order by brand_id nulls first, class_id nulls first, category_id nulls first;

select 1 as x;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'store_sales.item.brand_id') at
  line 34, column 9.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ..._sales <-      select         ??? store_sales.item.brand_id,

  Write stats: received 2938 chars / 2938 bytes; tail: …'ass_id nulls first,
  category_id nulls first;\\n\\nselect 1 as x;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…     count(web_sales.net_paid_inc_ship) as ws_count
    where web_sales.sold_date.year = 2001
      and web_sales.sold_date.moy = 11;

select 1 as x;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WILDCARD_IDENTIFIER', 'store_sales.item.brand_id') at
  line 28, column 9.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...tegory <-      select         ??? store_sales.item.brand_id,

  Write stats: received 2394 chars / 2394 bytes; tail: …'2001\\n      and
  web_sales.sold_date.moy = 11;\\n\\nselect 1 as x;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…annel nulls first, store_sales.item.brand_id nulls first, store_sales.item.class_id nulls first, store_sales.item.category_id nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('GROUPING', 'grouping') at line 92, column 1.
  Expected one of:
          * MERGE
          * LIMIT
          * ORDER
          * HAVING
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'overall_avg_sale')]

  Location:
  ..._nov_sales > overall_avg_sale ??? grouping(store_sales.item.bran...

  Write stats: received 4245 chars / 4245 bytes; tail: …' first,
  store_sales.item.category_id nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales --import raw/catalog_returns:catalog_returns select catalog_sales.ship_addr.state, catalog_sales.call_ce…, catalog_sales.order_number from catalog_sales left anti join catalog_returns on catalog_sales.order_number = catalog_returns.order_number limit 10;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...e, catalog_sales.order_number ??? from catalog_sales left anti j...
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales --import raw/catalog_returns:catalog_returns select catalog_sales.ship_addr.state, catalog_sales.call_ce… catalog_sales merge catalog_sales.order_number into catalog_returns.order_number as anti_filter where catalog_returns.order_number is null limit 10;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
   catalog_sales.ship_date.date ??? from catalog_sales merge catal...
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales --import raw/catalog_returns:catalog_returns select catalog_sales.order_number, count_distinct(catalog_s…addr.state = 'GA' and catalog_sales.call_center.county = 'Williamson County' group by catalog_sales.order_number having warehouse_count > 1 limit 10;`

  ```text
  --> 3:385
    |
  3 | select catalog_sales.order_number,
  count_distinct(catalog_sales.warehouse.warehouse_sk) as warehouse_count,
  sum(catalog_sales.ext_ship_cost) as ship_cost, sum(catalog_sales.net_profit) as
  net_profit where catalog_sales.ship_date.date between '2002-02-01'::date and
  '2002-04-02'::date and catalog_sales.ship_addr.state = 'GA' and
  catalog_sales.call_center.county = 'Williamson County' group by
  catalog_sales.order_number having warehouse_count > 1 limit 10;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ....county = 'Williamson County' ??? group by catalog_sales.order_n...
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales --import raw/catalog_returns:catalog_returns merge catalog_sales.order_number into ~catalog_returns.orde…ounty = 'Williamson County' and catalog_sales.order_number not in (select catalog_returns.order_number) order by catalog_sales.order_number limit 20;`

  ```text
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside
  parens)? Trilogy does not support subqueries — joins are auto-resolved from
  dotted paths. To filter on a value that lives on a related dimension, reference
  its dot-path directly. Example: instead of `where ss.store_id in (select
  store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...log_sales.order_number not in ??? (select catalog_returns.order_...
  ```
- `trilogy run --import raw.store_sales:store_sales select stddev_samp(store_sales.quantity) as test limit 1;`

  ```text
  --> 2:19
    |
  2 | select stddev_samp(store_sales.quantity) as test limit 1;
    |                   ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...tore_sales; select stddev_samp ??? (store_sales.quantity) as test...
  ```
- `trilogy run --import raw.store_sales:store_sales select stddev_samp(store_sales.quantity as test) limit 1;`

  ```text
  --> 2:19
    |
  2 | select stddev_samp(store_sales.quantity as test) limit 1;
    |                   ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...tore_sales; select stddev_samp ??? (store_sales.quantity as test)...
  ```
- `trilogy run --import raw.store_sales:store_sales select stddev_samp(store_sales.quantity) as test limit 1;`

  ```text
  --> 2:19
    |
  2 | select stddev_samp(store_sales.quantity) as test limit 1;
    |                   ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...tore_sales; select stddev_samp ??? (store_sales.quantity) as test...
  ```
- `trilogy run --import raw.store_sales:store_sales select stddev(store_sales.quantity) as pop_stddev, stddev_samp(store_sales.quantity) as samp_stddev, variance(store_sales.quantity) as pop_var, variance_samp(store_sales.quantity) as samp_var limit 1;`

  ```text
  --> 2:63
    |
  2 | select stddev(store_sales.quantity) as pop_stddev,
  stddev_samp(store_sales.quantity) as samp_stddev,
  variance(store_sales.quantity) as pop_var, variance_samp(store_sales.quantity)
  as samp_var limit 1;
    |                                                               ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ty) as pop_stddev, stddev_samp ??? (store_sales.quantity) as samp...
  ```
- `trilogy run --import raw.store_sales:store_sales select stddev_pop(store_sales.quantity) as pop_stddev, stddev_samp(store_sales.quantity) as samp_stddev limit 1;`

  ```text
  --> 2:18
    |
  2 | select stddev_pop(store_sales.quantity) as pop_stddev,
  stddev_samp(store_sales.quantity) as samp_stddev limit 1;
    |                  ^---
    |
    = expected limit, order_by, where, having, dot_tail, bracket_tail,
  dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...store_sales; select stddev_pop ??? (store_sales.quantity) as pop_...
  ```

### `syntax-missing-alias`

- `trilogy run --debug --import raw.catalog_sales:catalog_sales select catalog_sales.sold_date.week_seq, catalog_sales.sold_date.year, count(catalog_sales.net_paid_inc_ship_tax) where catalog_sales.sold_date.year in (2001,2002) limit 20;`

  ```text
  …
  . `SELECT x+1 AS y`
  Location:
  ..._sales.net_paid_inc_ship_tax) ??? where catalog_sales.sold_date....
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\parsing\v2\pest_backend.p
  y", line 321, in parse_pest
      tree = parse_trilogy_syntax_tuple(text)
  ValueError:  --> 2:115
    |
  2 | select catalog_sales.sold_date.week_seq, catalog_sales.sold_date.year,
  count(catalog_sales.net_paid_inc_ship_tax) where catalog_sales.sold_date.year
  in (2001,2002) limit 20;
    |
  ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR,
  PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, aggregate_over, or window_sql_over

  The above exception was the direct cause of the following exception:

  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\scripts\parallel_executio
  n.py", line 578, in run_single_script_execution
      queries = exec.parse_text(
          text, root=base if isinstance(base, Path) else None
      )
    File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\executor.py", line
  700, in parse_text
      r
  …
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Rowset: 2-digit prefixes from ZIPs with >10 preferred customers
# store_sales imports both c…efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
   by store_name asc limit 100; ???

  Write stats: received 1575 chars / 1575 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Rowset: 2-digit prefixes from ZIPs with >10 preferred customers
rowset eligible_prefix <-
  …efix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit

order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
   by store_name asc limit 100; ???

  Write stats: received 835 chars / 835 bytes; tail: …'it) as
  total_net_profit\\n\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Rowset: 2-digit prefixes from ZIPs with >10 preferred customers
rowset eligible_prefix <-
  …refix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
   by store_name asc limit 100; ???

  Write stats: received 834 chars / 834 bytes; tail: …'fit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql -e -c parameter zips string;

# Rowset: 2-digit prefixes from ZIPs with >10 preferred customers
rowset eligible_prefix <-
  …refix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
   by store_name asc limit 100; ???

  Write stats: received 834 chars / 834 bytes; tail: …'fit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Rowset: 2-digit prefixes from ZIPs with >10 preferred customers
rowset eligible_prefix <…refix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by store_name asc
limit 100;
`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
   by store_name asc limit 100; ???

  Write stats: received 834 chars / 834 bytes; tail: …'fit) as
  total_net_profit\\norder by store_name asc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content parameter zips string;

# Rowset: 2-digit prefixes from ZIPs with >10 preferred customers
rowset eligible_prefix <…e_sales.store.zip, 1, 2)) in param_prefix

select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...et_profit) as total_net_profit ???

  Write stats: received 798 chars / 798 bytes; tail: …'re_name,\\n
  sum(store_sales.net_profit) as total_net_profit'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.web_sales:ws select count(ws.bill_customer.customer_sk) limit 1;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ws.bill_customer.customer_sk) ??? limit 1;
  ```
- `trilogy run --import raw.store_sales:store_sales select count(store_sales.ticket_number) where store_sales.date_dim.year = 2001;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...nt(store_sales.ticket_number) ??? where store_sales.date_dim.yea...
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…m any source
merge catalog_sales.item.item_sk into ~store_sales.item.item_sk;
merge web_sales.item.item_sk into ~store_sales.item.item_sk;

select 1;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...sales.item.item_sk;  select 1; ???

  Write stats: received 324 chars / 324 bytes; tail: …'ales.item.item_sk into
  ~store_sales.item.item_sk;\\n\\nselect 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/catalog_sales.preql select sold_date.year, item.item_id, item.category, item.class, bill_addr.country, bill_addr.state, bill_addr.county, avg(quantity) as avg_qty, avg(quantity) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...ty) as avg_qty, avg(quantity) ??? limit 5;
  ```

### `undefined-concept`

- `trilogy run query04.preql duckdb`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer_id.')
  ```
- `trilogy explore query04.preql`

  ```text
  Failed to parse query04.preql: (UndefinedConceptException(...), 'Undefined
  concept: customer_id.')
  ```
- `trilogy explore query04.preql --include-hidden`

  ```text
  Failed to parse query04.preql: (UndefinedConceptException(...), 'Undefined
  concept: customer_id.')
  ```
- `trilogy run query07.preql`

  ```text
  (UndefinedConceptException(...), 'line: 8: Undefined concept:
  local.quantity.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.customer_address.zip.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.customer_address.zip.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer.customer_address.zip.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  store_sales.customer.customer_address.zip.')
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), 'Undefined concept:
  customer_address.zip.')
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  ext_list_price. Suggestions: ['web_sales.ext_list_price',
  'web_sales.list_price', 'store_sales.ext_list_price']")
  ```

### `join-resolution`

- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws select cs.sold_date.year as yr, cs.sold_date.week_seq as ws, sum(cs.ext_sales_price) + su…r = 2001 and ws.sold_date.year = 2001 and cs.sold_date.week_seq = ws.sold_date.week_seq and cs.sold_date.dow = ws.sold_date.dow order by ws limit 10;`

  ```text
  Could not resolve connections for query with output
  ['local.yr<Purpose.PROPERTY>Derivation.BASIC>',
  'local.ws<Purpose.PROPERTY>Derivation.BASIC>',
  'local.combined<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws select cs.sold_date.week_seq as ws, sum(cs.ext_sales_price) + sum(ws.ext_sales_price) as combined where cs.sold_date.year = 2001 and ws.sold_date.year = 2001 limit 10;`

  ```text
  Could not resolve connections for query with output
  ['local.ws<Purpose.PROPERTY>Derivation.BASIC>',
  'local.combined<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['store_sales.customer.customer_demographics.gender<Purpose.PROPERTY>Derivation
  .ROOT>',
  'store_sales.customer.customer_demographics.marital_status<Purpose.PROPERTY>Der
  ivation.ROOT>',
  'store_sales.customer.customer_demographics.education_status<Purpose.PROPERTY>D
  erivation.ROOT>',
  'store_sales.customer.customer_demographics.purchase_estimate<Purpose.PROPERTY>
  Derivation.ROOT>',
  'store_sales.customer.customer_demographics.credit_rating<Purpose.PROPERTY>Deri
  vation.ROOT>',
  'store_sales.customer.customer_demographics.dep_count<Purpose.PROPERTY>Derivati
  on.ROOT>',
  'store_sales.customer.customer_demographics.dep_employed_count<Purpose.PROPERTY
  >Derivation.ROOT>',
  'store_sales.customer.customer_demographics.dep_college_count<Purpose.PROPERTY>
  Derivation.ROOT>', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>']
  from current model.
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales --import raw/catalog_returns:catalog_returns select catalog_sales.order_number, sum(catalog_sales.ext_sh…GA' and catalog_sales.call_center.county = 'Williamson County' and catalog_returns.order_number is null order by catalog_sales.order_number limit 20;`

  ```text
  Could not resolve connections for query with output
  ['catalog_sales.order_number<Purpose.PROPERTY>Derivation.ROOT>',
  'local.total_ship_cost<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_net_profit<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.ret_order_number<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales --import raw/catalog_returns:catalog_returns select catalog_sales.order_number, catalog_returns.order_nu…ate and catalog_sales.ship_addr.state = 'GA' and catalog_sales.call_center.county = 'Williamson County' order by catalog_sales.order_number limit 50;`

  ```text
  Could not resolve connections for query with output
  ['catalog_sales.order_number<Purpose.PROPERTY>Derivation.ROOT>',
  'catalog_returns.order_number<Purpose.PROPERTY>Derivation.ROOT>',
  'local.total_ship_cost<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_net_profit<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query19.preql`

  ```text
  Could not resolve connections for query with output
  ['local.brand_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.brand_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.manufacturer_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.manufacturer_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_extended_sales_price<Purpose.METRIC>Derivation.AGGREGATE>'] from
  current model.
  ```

### `cli-misuse`

- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; select cs.sold_date.week_seq as ws, sum(cs.ext_sales_price) + sum(ws.ext_sales_price) as combined where cs.sold_date.year = 2001 and ws.sold_date.year = 2001 limit 10;`

  ```text
  'select cs.sold_date.week_seq as ws, sum(cs.ext_sales_price) + sum(ws.ext_sales_price) as combined where cs.sold_date.year = 2001 and ws.sold_date.year = 2001 limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw.catalog_sales:cs --import raw.web_sales:ws merge cs.sold_date.date_sk into ~ws.sold_date.date_sk; select cs.sold_date.week_seq as ws, sum(cs.ext_sales_price) + sum(ws.ext_sales_price) as combined where cs.sold_date.year = 2001 limit 10; duck_db`

  ```text
  'select cs.sold_date.week_seq as ws, sum(cs.ext_sales_price) + sum(ws.ext_sales_price) as combined where cs.sold_date.year = 2001 limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_sales.preql --show datasources,imports`

  ```text
  Invalid value for '--show': 'datasources,imports' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```
- `trilogy run --param x=1 - import raw.web_sales as ws; select count(ws.bill_customer.customer_sk) limit 1;`

  ```text
  'import raw.web_sales as ws; select count(ws.bill_customer.customer_sk) limit 1;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
