# Trilogy failure analysis — 20260529-152957

- Run `20260529-152957_base` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.1
- `trilogy` calls: 365 | failed: 62 (17%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 24 | 39% |
| `syntax-parse` | 22 | 35% |
| `undefined-concept` | 8 | 13% |
| `join-resolution` | 6 | 10% |
| `cli-misuse` | 2 | 3% |

## Detail

### `other`

- `trilogy run query01.preql`

  ```text
  Unable to import '.\store_returns.preql': [Errno 2] No such
  file or directory: '.\\store_returns.preql'. Did you mean: raw.store_returns?
  ```
- `trilogy run query04.preql`

  ```text
  Have
  {'GroupNode<catalog_sales.bill_customer.customer_id,local.catalog_2001>': None,
  'GroupNode<local.store_2001,store_sales.customer.customer_id>': None,
  'GroupNode<local.web_2001,web_sales.bill_customer.customer_id>': None,
  'GroupNode<store_sales.customer.customer_id,store_sales.customer.first_name,sto
  re_sales.customer.last_name...1 more>': None} and need local.store_2001 > 0 and
  local.catalog_2001 > 0 and local.web_2001 > 0 and local.catalog_ratio >
  local.store_ratio and local.catalog_ratio > local.web_ratio
  ```
- `trilogy file write --content import raw.customer as customer;
import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.we…> web_ratio
order by customer_id nulls first, first_name nulls first, last_name nulls first, preferred_cust_flag nulls first
limit 100; query04.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales --import raw.customer:customer`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales --import raw.web_sales:web_sales --import raw.customer:customer`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy file write query05.preql -c`

  ```text
  Option '-c' requires an argument.
  ```
- `trilogy run query05.preql`

  ```text
  Cannot resolve query. No remaining priority concepts, have
  attempted {'local.store_loss_by_store', 'local.store_profit_by_store'} out of
  with found {'local.store_loss_by_store', 'local.store_sales_by_store',
  'local.store_profit_by_store', 'local.store_returns_by_store',
  'store_sales.store.store_sk', 'store_returns.store.store_sk'}
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 58 (char 57). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 52 (char 51). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 55 (char 54). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 55 (char 54). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 4 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy file write --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.reason as reason;

select
    count(store_sales.ticket_number) as total_line_items
limit 1; query09.preql`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```
- `trilogy run query10.preql`

  ```text
  ORDER BY references 'local.gender', which is not in the
  SELECT projection (line 10). Add it to SELECT to sort by it — prefix with `--`
  to keep it out of the output rows, e.g. `select ..., --local.gender order by
  local.gender asc`.
  ```
- `trilogy run query10.preql`

  ```text
  Have
  {'GroupNode<catalog_sales.ship_customer.customer_sk,local._virt_agg_count_38621
  60178035012>': None,
  'GroupNode<local._virt_agg_count_914004015738480,web_sales.bill_customer.custom
  er_sk>': None,
  'MergeNode<customer.customer_address.county,customer.customer_demographics.cred
  it_rating,customer.customer_demographics.dep_college_count...7 more>': None}
  and need
  BuildSubselectComparison(left=customer.customer_address.county@Grain<customer.c
  ustomer_address.address_sk>, right=('Rush County', 'Toole County', 'Jefferson
  County', 'Dona Ana County', 'La Porte County'),
  operator=<ComparisonOperator.IN: 'in'>) and
  local._virt_agg_count_8599725121322400 >= 1 and
  (local._virt_agg_count_914004015738480 >= 1 or
  local._virt_agg_count_3862160178035012 >= 1)
  ```
- `trilogy file write query10.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy file write query10.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 3 column 6 (char 98). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 4 column 2 (char 1833). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 4 column 2 (char 1214). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 65 (char 64). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 67 (char 66). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query18.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING statement
  cannot be used without groups

  LINE 153:     grouping(coalesce("vacuous"."catalog_sales_bill_customer_cu...
                ^
  [SQL:
  WITH
  thoughtful as (
  SELECT
      "catalog_sales_catalog_sales"."cs_bill_cdemo_sk" as
  "catalog_sales_bill_cdemo_demo_sk",
      "catalog_sales_catalog_sales"."cs_bill_customer_sk" as
  "catalog_sales_bill_customer_customer_sk",
      "catalog_sales_catalog_sales"."cs_item_sk" as "catalog_sales_item_item_sk",
      "catalog_sales_catalog_sales"."cs_sold_date_sk" as
  "catalog_sales_sold_date_date_sk"
  FROM
      "cat
  …
  ") asc nulls first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_c
  ountry","young"."catalog_sales_bill_customer_customer_address_country")) asc
  nulls first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_s
  tate","young"."catalog_sales_bill_customer_customer_address_state")) asc nulls
  first,
      grouping(coalesce("vacuous"."catalog_sales_bill_customer_customer_address_c
  ounty","young"."catalog_sales_bill_customer_customer_address_county")) asc
  nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query21.preql`

  ```text
  Unable to import '.\inventory.preql': [Errno 2] No such file
  or directory: '.\\inventory.preql'. Did you mean: raw.inventory?
  ```
- `trilogy file write query21.preql --content`

  ```text
  Option '--content' requires an argument.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Combine the two fact tables into a unified sales fe…s.sold_date.dow = 5) as fri_2002,
    sum(daily_sales ? cs.sold_date.year = 2002 and cs.sold_date.dow = 6) as sat_2002
order by week_seq nulls first;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_SUM', 'sum(') at line 32, column 5.
  Expected one of:
          * MERGE
          * LIMIT
          * COMMA
          * HAVING
          * WHERE
          * ORDER
          * _TERMINATOR
          * METADATA
  Previous tokens: [Token('IDENTIFIER', 'sat_2001')]

  Location:
  ...e week-of-year (week_seq)     ??? sum(daily_sales ? cs.sold_date...

  Write stats: received 2025 chars / 2025 bytes; tail: …'old_date.dow = 6) as
  sat_2002\\norder by week_seq nulls first;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.customer_id, store_sales.date_dim.year, sum((store_sales.ext_list_price - store_… as yt_total where store_sales.date_dim.year in (2001, 2002) group by store_sales.customer.customer_id, store_sales.date_dim.year order by 1 limit 5;`

  ```text
  --> 2:261
    |
  2 | select store_sales.customer.customer_id, store_sales.date_dim.year,
  sum((store_sales.ext_list_price - store_sales.ext_wholesale_cost -
  store_sales.ext_discount_amt + store_sales.ext_sales_price) / 2) as yt_total
  where store_sales.date_dim.year in (2001, 2002) group by
  store_sales.customer.customer_id, store_sales.date_dim.year order by 1 limit 5;
    |
  ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail,
  bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...date_dim.year in (2001, 2002) ??? group by store_sales.customer....
  ```
- `trilogy file write query05.preql -c import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as catalog_returns;
import raw.store_sales as store…, 0) as sales,
  coalesce(web_returns_by_site, 0) as returns,
  coalesce(web_profit_by_site, 0) as profit
order by channel, id nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 12, column 81.
  Expected one of:
          * SELECT

  Location:
  ...'::date and '2000-09-06'::date ??? ;  # Store channel: combine st...

  Write stats: received 2781 chars / 2781 bytes; tail: …'te, 0) as profit\\norder
  by channel, id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -c import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as catalog_returns;
import raw.store_sales as store…turns_by_site, 0) as returns,
  coalesce(web_profit_by_site, 0) as profit
  by web_sales.web_site.site_sk
order by channel, id nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 12, column 311.
  Expected one of:
          * _TERMINATOR

  Location:
  ...date and '2000-09-06'::date)) ??? by store_sales.store.store_sk;...

  Write stats: received 3487 chars / 3487 bytes; tail: …'web_site.site_sk\\norder
  by channel, id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -c import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as catalog_returns;
import raw.store_sales as store…0) as sales,
  coalesce(store_agg_returns, 0) as returns,
  coalesce(store_agg_profit - store_agg_loss, 0) as profit
  by store_sales.store.store_sk;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'by ') at line 19, column 3.
  Expected one of:
          * WHERE
          * _TERMINATOR
          * LIMIT
          * COMMA
          * METADATA
          * MERGE
          * ORDER
          * HAVING
  Previous tokens: [Token('IDENTIFIER', 'profit')]

  Location:
  ...tore_agg_loss, 0) as profit   ??? by store_sales.store.store_sk;...

  Write stats: received 1247 chars / 1247 bytes; tail: …'tore_agg_loss, 0) as
  profit\\n  by store_sales.store.store_sk;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -c import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as catalog_returns;
import raw.store_sales as store…s returns,
  coalesce(store_agg_profit - store_agg_loss, 0) as profit
  by store_sales.store.store_sk
  order by channel, id nulls first
  limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'by ') at line 19, column 3.
  Expected one of:
          * ORDER
          * LIMIT
          * COMMA
          * METADATA
          * MERGE
          * HAVING
          * _TERMINATOR
          * WHERE
  Previous tokens: [Token('IDENTIFIER', 'profit')]

  Location:
  ...tore_agg_loss, 0) as profit   ??? by store_sales.store.store_sk

  Write stats: received 1294 chars / 1294 bytes; tail: …'ore.store_sk\\n  order
  by channel, id nulls first\\n  limit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -c import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as catalog_returns;
import raw.store_sales as store…hannel,
  id,
  sum(sales) as total_sales,
  sum(returns) as total_returns,
  sum(profit) as total_profit
order by channel, id nulls first
limit 100;`

  ```text
  …
  t_agg_returns) by 'catalog' +
  sum(web_agg_returns) by 'web';\nauto channel_profit <- sum(store_agg_profit -
  store_agg_loss) by 'store' + sum(cat_agg_profit - cat_agg_loss) by 'catalog' +
  sum(web_agg_profit - web_agg_loss) by 'web';\n\nselect\n  channel,\n  id,\n
  sum(sales) as total_sales,\n  sum(returns) as total_returns,\n  sum(profit) as
  total_profit\norder by channel, id nulls first\nlimit 100;") at line 31, column
  47.
  Expected one of:
          * LPAR
          * STAR
          * ROLLUP
          * CUBE
          * "GROUPING"i
          * IDENTIFIER
  Previous tokens: [Token('__ANON_7', 'by')]

  Location:
  ...es <- sum(store_agg_sales) by ??? 'store' + sum(cat_agg_sales) b...

  Write stats: received 3479 chars / 3479 bytes; tail: …' as total_profit\\norder
  by channel, id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -c import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as catalog_returns;
import raw.store_sales as store…ng)) as id,
  coalesce(web_agg_sales, 0) as sales,
  coalesce(web_agg_returns, 0) as returns,
  coalesce(web_agg_profit - web_agg_loss, 0) as profit;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n') at line 32, column 1.
  Expected one of:
          * MERGE
          * METADATA
          * LIMIT
          * WHERE
          * HAVING
          * ORDER
          * COMMA
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'profit')]

  Location:
   store_agg_loss, 0) as profit ??? union select   'catalog' as ch...

  Write stats: received 3300 chars / 3300 bytes; tail: …'rns,\\n
  coalesce(web_agg_profit - web_agg_loss, 0) as profit;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query05.preql -c import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as catalog_returns;
import raw.store_sales as store…dim.date between '2000-08-23'::date and '2000-09-06'::date)) as total_profit
group by rollup(channel, id)
order by channel, id nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'group ') at line 20, column 1.
  Expected one of:
          * _TERMINATOR
          * HAVING
          * WHERE
          * LIMIT
          * MERGE
          * COMMA
          * METADATA
          * ORDER
  Previous tokens: [Token('IDENTIFIER', 'total_profit')]

  Location:
  ...9-06'::date)) as total_profit ??? group by rollup(channel, id) o...

  Write stats: received 1246 chars / 1246 bytes; tail: …'lup(channel, id)\\norder
  by channel, id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query06.preql --content import raw.store_sales as store_sales;

# Avg current price by category for all items
auto avg_price_by_category <…r_address.state as state,
    count(customer.customer_sk) as customer_count
order by customer_count asc nulls first, state asc nulls first
limit 100;`

  ```text
  refused to write 'query06.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'item.item_sk ') at line 7, column
  32.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'select')]

  Location:
  ...uto expensive_items <- select ??? item.item_sk where item.curren...

  Write stats: received 661 chars / 661 bytes; tail: …'omer_count asc nulls
  first, state asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.reason as reason;

# …the store sales that had a return with reason 1

# First, let's look at what we have
select
    count(store_sales.ticket_number) as total_line_items
`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a
  semicolon `;`.
  Location:
  ...et_number) as total_line_items ???

  Write stats: received 389 chars / 389 bytes; tail: …'ct\\n
  count(store_sales.ticket_number) as total_line_items\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw.store_sales as store_sales;

# Year 2001 filter
auto year_2001 <- store_sales.date_dim.year = 2001;

# …avg(store_sales.ext_wholesale_cost) as avg_extended_wholesale_cost,
  sum(store_sales.ext_wholesale_cost) as total_extended_wholesale_cost
limit 100;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 14, column 4.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...phics.marital_status = 'M'    ??? and store_sales.customer.custo...

  Write stats: received 2335 chars / 2335 bytes; tail: …'_wholesale_cost) as
  total_extended_wholesale_cost\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw.store_sales as store_sales;

# Year 2001 filter
auto year_2001 <- store_sales.date_dim.year = 2001;

# …avg(store_sales.ext_wholesale_cost) as avg_extended_wholesale_cost,
  sum(store_sales.ext_wholesale_cost) as total_extended_wholesale_cost
limit 100;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 14, column 4.
  Expected one of:
          * COMMA
          * RPAR

  Location:
  ...phics.marital_status = 'M'    ??? and store_sales.customer.custo...

  Write stats: received 2335 chars / 2335 bytes; tail: …'_wholesale_cost) as
  total_extended_wholesale_cost\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql -e -c import raw.store_sales as store_sales;

# Year 2001 filter
auto year_2001 <- store_sales.date_dim.year = 2001;

# --- …avg(store_sales.ext_wholesale_cost) as avg_extended_wholesale_cost,
  sum(store_sales.ext_wholesale_cost) as total_extended_wholesale_cost
limit 100;`

  ```text
  refused to write 'query13.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 9, column 4.
  Expected one of:
          * RPAR
          * COMMA

  Location:
  ...phics.marital_status = 'M'    ??? and store_sales.customer.custo...

  Write stats: received 1813 chars / 1813 bytes; tail: …'_wholesale_cost) as
  total_extended_wholesale_cost\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as catalog_sales;

# The question asks for "per-line catalog sales price" — this is sales…  )

select
    catalog_sales.bill_customer.customer_address.zip as zip_code,
    catalog_sales.sales_price
order by zip_code nulls first
limit 100;
`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS',
  "(catalog_sales.bill_customer.customer_address.zip, 1, 5) in ('85669', '86197',
  '88274', '83405', '86475', '85392', '85460', '80348', '81792')\n    or
  catalog_sales.bill_customer.customer_address.state in ('CA', 'WA', 'GA')\n
  or catalog_sales.sales_price > 500\n  )\n\nselect\n
  catalog_sales.bill_customer.customer_address.zip as zip_code,\n
  catalog_sales.sales_price\norder by zip_code nulls first\nlimit 100;\n") at
  line 18, column 11.
  Expected one of:
          * LOGICAL_OR
          * COMMA
          * LOGICAL_AND
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'substr')]

  Location:
  ...price > 500   and (     substr ??? (catalog_sales.bill_customer.c...

  Write stats: received 1236 chars / 1238 bytes; tail:
  …'_sales.sales_price\\norder by zip_code nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as catalog_sales;

# For catalog sales in the 2nd quarter of year 2001, report the total …select
    catalog_sales.bill_addr.zip as zip_code,
    sum(catalog_sales.sales_price) as total_sales_price
order by zip_code nulls first
limit 100;
`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "(catalog_sales.bill_addr.zip, 1,
  5) in ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348',
  '81792')\n    or catalog_sales.bill_addr.state in ('CA', 'WA', 'GA')\n    or
  catalog_sales.sales_price > 500\n  )\n\nselect\n    catalog_sales.bill_addr.zip
  as zip_code,\n    sum(catalog_sales.sales_price) as total_sales_price\norder by
  zip_code nulls first\nlimit 100;\n") at line 13, column 11.
  Expected one of:
          * LOGICAL_OR
          * COMMA
          * LOGICAL_AND
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'substr')]

  Location:
  ...ate.qoy = 2   and (     substr ??? (catalog_sales.bill_addr.zip,

  Write stats: received 938 chars / 938 bytes; tail: …' total_sales_price\\norder
  by zip_code nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as catalog_sales;

where catalog_sales.sold_date.year = 2001
  and catalog_sales.sold_dat…select
    catalog_sales.bill_addr.zip as zip_code,
    sum(catalog_sales.sales_price) as total_sales_price
order by zip_code nulls first
limit 100;
`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "(catalog_sales.bill_addr.zip, 1,
  5) in ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348',
  '81792') or catalog_sales.bill_addr.state in ('CA', 'WA', 'GA') or
  catalog_sales.sales_price > 500)\n\nselect\n    catalog_sales.bill_addr.zip as
  zip_code,\n    sum(catalog_sales.sales_price) as total_sales_price\norder by
  zip_code nulls first\nlimit 100;\n") at line 5, column 14.
  Expected one of:
          * RPAR
          * LOGICAL_AND
          * COMMA
          * LOGICAL_OR
  Previous tokens: [Token('IDENTIFIER', 'substr')]

  Location:
  ...old_date.qoy = 2   and (substr ??? (catalog_sales.bill_addr.zip,

  Write stats: received 499 chars / 499 bytes; tail: …' total_sales_price\\norder
  by zip_code nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as catalog_sales;

where catalog_sales.sold_date.year = 2001
  and catalog_sales.sold_dat…les.bill_customer.customer_address.zip as zip_code,
    sum(catalog_sales.sales_price) as total_sales_price
order by zip_code nulls first
limit 100;
`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS',
  "(catalog_sales.bill_customer.customer_address.zip, 1, 5) in ('85669', '86197',
  '88274', '83405', '86475', '85392', '85460', '80348', '81792'))\n\nselect\n
  catalog_sales.bill_customer.customer_address.zip as zip_code,\n
  sum(catalog_sales.sales_price) as total_sales_price\norder by zip_code nulls
  first\nlimit 100;\n") at line 5, column 125.
  Expected one of:
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'substr')]

  Location:
  ...es.sales_price > 500 or substr ??? (catalog_sales.bill_customer.c...

  Write stats: received 562 chars / 562 bytes; tail: …' total_sales_price\\norder
  by zip_code nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as catalog_sales;

where catalog_sales.sold_date.year = 2001
  and catalog_sales.sold_dat…les.bill_customer.customer_address.zip as zip_code,
    sum(catalog_sales.sales_price) as total_sales_price
order by zip_code nulls first
limit 100;
`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS',
  "(catalog_sales.bill_customer.customer_address.zip, 1, 5) in ('85669', '86197',
  '88274', '83405', '86475', '85392', '85460', '80348', '81792')\n  )\n\nselect\n
  catalog_sales.bill_customer.customer_address.zip as zip_code,\n
  sum(catalog_sales.sales_price) as total_sales_price\norder by zip_code nulls
  first\nlimit 100;\n") at line 8, column 14.
  Expected one of:
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'substr')]

  Location:
  ...ales_price > 500     or substr ??? (catalog_sales.bill_customer.c...

  Write stats: received 578 chars / 578 bytes; tail: …' total_sales_price\\norder
  by zip_code nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as catalog_sales;

where catalog_sales.sold_date.year = 2001
  and catalog_sales.sold_dat…les.bill_customer.customer_address.zip as zip_code,
    sum(catalog_sales.sales_price) as total_sales_price
order by zip_code nulls first
limit 100;
`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS',
  "(catalog_sales.bill_customer.customer_address.zip, 1, 5)) in ('85669',
  '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')\n
  )\n\nselect\n    catalog_sales.bill_customer.customer_address.zip as
  zip_code,\n    sum(catalog_sales.sales_price) as total_sales_price\norder by
  zip_code nulls first\nlimit 100;\n") at line 8, column 15.
  Expected one of:
          * RPAR
          * COMMA
          * LOGICAL_OR
          * LOGICAL_AND
  Previous tokens: [Token('IDENTIFIER', 'substr')]

  Location:
  ...les_price > 500     or (substr ??? (catalog_sales.bill_customer.c...

  Write stats: received 580 chars / 580 bytes; tail: …' total_sales_price\\norder
  by zip_code nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query15.preql --content import raw.catalog_sales as catalog_sales;

where catalog_sales.sold_date.year = 2001
  and catalog_sales.sold_dat…les.bill_customer.customer_address.zip as zip_code,
    sum(catalog_sales.sales_price) as total_sales_price
order by zip_code nulls first
limit 100;
`

  ```text
  refused to write 'query15.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS',
  "(catalog_sales.bill_customer.customer_address.zip, 5) in ('85669', '86197',
  '88274', '83405', '86475', '85392', '85460', '80348', '81792')\n  )\n\nselect\n
  catalog_sales.bill_customer.customer_address.zip as zip_code,\n
  sum(catalog_sales.sales_price) as total_sales_price\norder by zip_code nulls
  first\nlimit 100;\n") at line 8, column 12.
  Expected one of:
          * RPAR
  Previous tokens: [Token('IDENTIFIER', 'left')]

  Location:
  ....sales_price > 500     or left ??? (catalog_sales.bill_customer.c...

  Write stats: received 573 chars / 573 bytes; tail: …' total_sales_price\\norder
  by zip_code nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query16.preql --content import raw.catalog_sales as catalog_sales;

# Filter by ship date between 2002-02-01 and 2002-04-02 inclusive
wher…alog_sales.ext_ship_cost) as total_extended_ship_cost,
    sum(catalog_sales.net_profit) as total_net_profit
order by distinct_order_count
limit 100;`

  ```text
  refused to write 'query16.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy
  does not support subqueries — joins are auto-resolved from dotted paths. To
  filter on a value that lives on a related dimension, reference its dot-path
  directly. Example: instead of `where ss.store_id in (select store_id where
  store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...log_sales.order_number not in ??? (     select catalog_returns.o...

  Write stats: received 771 chars / 771 bytes; tail: …'as
  total_net_profit\\norder by distinct_order_count\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `undefined-concept`

- `trilogy run query03.preql`

  ```text
  (UndefinedConceptException(...), "line: 3: Undefined concept:
  item.brand. Suggestions: ['store_sales.item.brand']")
  ```
- `trilogy run query05.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web_returns.web_site.site_sk. Suggestions: ['web_sales.web_site.site_sk',
  'web_returns.item.item_sk', 'web_sales.web_site.site_id']")
  ```
- `trilogy run query06.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  item.current_price. Suggestions: ['store_sales.item.current_price',
  'store_sales.promotion.item.current_price']")
  ```
- `trilogy run query09.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_sales.reason.sk. Suggestions: ['store_returns.reason.sk',
  'store_sales.item.class', 'store_returns.reason.desc']")
  ```
- `trilogy run query09.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_sales.reason.sk. Suggestions: ['store_returns.reason.sk',
  'store_sales.item.class', 'store_returns.reason.desc']")
  ```
- `trilogy run query21.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  date_dim.date. Suggestions: ['inventory.date_dim.date',
  'inventory.date_dim.date_sk', 'inventory.date_dim.date_id']")
  ```
- `trilogy run query21.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  quantity_on_hand. Suggestions: ['inventory.quantity_on_hand']")
  ```
- `trilogy run query21.preql`

  ```text
  (UndefinedConceptException(...), "line: 10: Undefined
  concept: warehouse.warehouse_name. Suggestions:
  ['inventory.warehouse.warehouse_name', 'inventory.warehouse.warehouse_sk',
  'inventory.warehouse.warehouse_id']")
  ```

### `join-resolution`

- `trilogy run query10.preql`

  ```text
  Could not resolve connections for query with output
  ['local.gender<Purpose.PROPERTY>Derivation.BASIC>',
  'local.marital_status<Purpose.PROPERTY>Derivation.BASIC>',
  'local.education_status<Purpose.PROPERTY>Derivation.BASIC>',
  'local.purchase_estimate<Purpose.PROPERTY>Derivation.BASIC>',
  'local.credit_rating<Purpose.PROPERTY>Derivation.BASIC>',
  'local.dep_count<Purpose.PROPERTY>Derivation.BASIC>',
  'local.dep_employed_count<Purpose.PROPERTY>Derivation.BASIC>',
  'local.dep_college_count<Purpose.PROPERTY>Derivation.BASIC>',
  'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
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
  ['customer.customer_demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.customer_demographics.marital_status<Purpose.PROPERTY>Derivation.ROOT
  >',
  'customer.customer_demographics.education_status<Purpose.PROPERTY>Derivation.RO
  OT>',
  'customer.customer_demographics.purchase_estimate<Purpose.PROPERTY>Derivation.R
  OOT>',
  'customer.customer_demographics.credit_rating<Purpose.PROPERTY>Derivation.ROOT>
  ',
  'customer.customer_demographics.dep_count<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.customer_demographics.dep_employed_count<Purpose.PROPERTY>Derivation.
  ROOT>',
  'customer.customer_demographics.dep_college_count<Purpose.PROPERTY>Derivation.R
  OOT>', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
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
  ['customer.customer_demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.customer_demographics.marital_status<Purpose.PROPERTY>Derivation.ROOT
  >',
  'customer.customer_demographics.education_status<Purpose.PROPERTY>Derivation.RO
  OT>',
  'customer.customer_demographics.purchase_estimate<Purpose.PROPERTY>Derivation.R
  OOT>',
  'customer.customer_demographics.credit_rating<Purpose.PROPERTY>Derivation.ROOT>
  ',
  'customer.customer_demographics.dep_count<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.customer_demographics.dep_employed_count<Purpose.PROPERTY>Derivation.
  ROOT>',
  'customer.customer_demographics.dep_college_count<Purpose.PROPERTY>Derivation.R
  OOT>', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
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
  ['customer.customer_demographics.gender<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.customer_demographics.marital_status<Purpose.PROPERTY>Derivation.ROOT
  >',
  'customer.customer_demographics.education_status<Purpose.PROPERTY>Derivation.RO
  OT>',
  'customer.customer_demographics.purchase_estimate<Purpose.PROPERTY>Derivation.R
  OOT>',
  'customer.customer_demographics.credit_rating<Purpose.PROPERTY>Derivation.ROOT>
  ',
  'customer.customer_demographics.dep_count<Purpose.PROPERTY>Derivation.ROOT>',
  'customer.customer_demographics.dep_employed_count<Purpose.PROPERTY>Derivation.
  ROOT>',
  'customer.customer_demographics.dep_college_count<Purpose.PROPERTY>Derivation.R
  OOT>', 'local.customer_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_2<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_3<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_4<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_5<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.customer_count_6<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query11.preql`

  ```text
  Could not resolve connections for query with output
  ['local.customer_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.preferred_cust_flag<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query11.preql`

  ```text
  Could not resolve connections for query with output
  ['local.customer_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.preferred_cust_flag<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```

### `cli-misuse`

- `trilogy read_file`

  ```text
  No such command 'read_file'.
  ```
- `trilogy `

  ```text
  Usage: python -m trilogy.scripts.trilogy [OPTIONS] COMMAND [ARGS]...

    Trilogy CLI - A beautiful data productivity tool.

  Options:
    --version          Show version and exit.
    --debug            Enable debug mode (show tracebacks on errors)
    --debug-file TEXT  Write SQL debug output to the specified file path
    --help             Show this message and exit.

  Commands:
    agent        Pass off a multi-step orchestration task to an AI agent.
    agent-info   Output comprehensive CLI documentation for AI agents.
    database     Inspect the database configured in trilogy.toml.
    explore      Parse
  …
  ipts.
    public       Work with trilogy-public-models hosted at...
    refresh      Refresh stale assets in Trilogy scripts.
    render       Render a Trilogy markdown report to an image or HTML file.
    run          Execute a Trilogy script or query.
    serve        Start a FastAPI server to expose Trilogy models from a...
    unit         Run unit tests on Trilogy scripts with mocked datasources.

    NOTE FOR AI AGENTS (Claude, Gemini, GPT, etc.): For comprehensive usage
    documentation, run 'trilogy agent-info' to get a detailed AGENTS.md-style
    guide with all commands, options, and usage examples.
  ```
