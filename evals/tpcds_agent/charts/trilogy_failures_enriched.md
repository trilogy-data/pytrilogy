# Trilogy failure analysis — 20260529-152957

- Run `20260529-152957_enriched` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.1
- `trilogy` calls: 347 | failed: 48 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 16 | 33% |
| `join-resolution` | 12 | 25% |
| `other` | 8 | 17% |
| `cli-misuse` | 6 | 12% |
| `undefined-concept` | 3 | 6% |
| `type-error` | 2 | 4% |
| `syntax-missing-alias` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.store_returns as store_returns;

# For returns to stores in Tennessee in 2000, find customers whose tot…store_returns.customer.id
order by 
    store_returns.customer.id
having
    customer_store_returns > 1.2 * avg_customer_returns_per_store
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('HAVING', 'having') at line 18, column 1.
  Expected one of:
          * LIMIT
          * MERGE
          * _TERMINATOR

  Location:
      store_returns.customer.id ??? having     customer_store_retu...

  Write stats: received 865 chars / 865 bytes; tail: …'re_returns > 1.2 *
  avg_customer_returns_per_store\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql -e -c import raw.store_returns as store_returns;

# Customer-level total return amount per store in 2000 in TN stores
auto c…store_returns.customer.id
order by 
    store_returns.customer.id
limit 100
having
    customer_store_returns > 1.2 * avg_customer_returns_per_store;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'having\n    ') at line 16, column
  1.
  Expected one of:
          * _TERMINATOR
          * MERGE
  Previous tokens: [Token('__ANON_10', '100')]

  Location:
  ...returns.customer.id limit 100 ??? having     customer_store_retu...

  Write stats: received 613 chars / 613 bytes; tail: …'stomer_store_returns > 1.2
  * avg_customer_returns_per_store;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…by customer.text_id nulls first, customer.first_name nulls first, customer.last_name nulls first, customer.preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('LOGICAL_AND', 'and') at line 33, column 36.
  Expected one of:
          * _TERMINATOR

  Location:
  ...is_eligible <- store_2001 > 0 ??? and catalog_2001 > 0 and web_2...

  Write stats: received 2562 chars / 2562 bytes; tail: …'s first,
  customer.preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/unified_sales.preql select sales_channel, channel_dim_id, sum(ext_sales_price) as total_sales, sum(return_amount) as total_returns, sum(net_profit) - sum(return_net_loss) as profit group by sales_channel, channel_dim_id limit 10;`

  ```text
  --> 2:162
    |
  2 | select sales_channel, channel_dim_id, sum(ext_sales_price) as total_sales,
  sum(return_amount) as total_returns, sum(net_profit) - sum(return_net_loss) as
  profit group by sales_channel, channel_dim_id limit 10;
    |
  ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...um(return_net_loss) as profit ??? group by sales_channel, channe...
  ```
- `trilogy file write query05.preql --content import raw.unified_sales as unified;
import raw.date as date;

# Filter to the date range
where date.date between …rn_amount) as total_returns,
    sum(unified.net_profit) - sum(unified.return_net_loss) as profit
order by channel asc, id asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('_TERMINATOR', ';') at line 5, column 66.
  Expected one of:
          * SELECT

  Location:
  ...'::date and '2000-09-06'::date ??? ;  # Per-group aggregates by c...

  Write stats: received 829 chars / 829 bytes; tail: …'s profit\\norder by
  channel asc, id asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --all-rows --import raw/unified_sales.preql:unified select unified.sales_channel as channel, unified.channel_dim_id, count(unified.row_one) as cnt from unified group by unified.sales_channel, unified.channel_dim_id limit 10;`

  ```text
  Syntax [101]: Using FROM keyword? Trilogy does not have a
  FROM clause (Datasource resolution is automatic).
  Location:
  ...count(unified.row_one) as cnt ??? from unified group by unified....
  ```
- `trilogy file write query08.preql --content import raw.store_sales as store_sales;

parameter zips string;

# Split the zips parameter into individual ZIP cod…elect
  store_sales.store.s_store_name as store_name,
  sum(store_sales.ss_net_paid - store_sales.ss_net_paid_inc_tax) as total_net_profit
limit 100;`

  ```text
  refused to write 'query08.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', "(zip_code, 1, 2) || '%'\n  and
  ...\nselect\n  store_sales.store.s_store_name as store_name,\n
  sum(store_sales.ss_net_paid - store_sales.ss_net_paid_inc_tax) as
  total_net_profit\nlimit 100;") at line 15, column 42.
  Expected one of:
          * SELECT
  Previous tokens: [Token('IDENTIFIER', 'substr')]

  Location:
  ..._sales.store.s_zip like substr ??? (zip_code, 1, 2) || '%'   and

  Write stats: received 845 chars / 845 bytes; tail:
  …'re_sales.ss_net_paid_inc_tax) as total_net_profit\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.store_sales as store_sales;

parameter zips string;

# Get the first-2-digits of ZIP codes from the par…rred_cust_flag = 'Y'

select
  store_sales.store.name as store_name,
  sum(store_sales.net_profit) as total_net_profit
order by store_name
limit 100;`

  ```text
  …
  e >10 preferred customers live, independent of store sales\n# So we
  need to count preferred customers per address ZIP\n\nauto pref_cust_zip_count
  <- count_distinct(store_sales.customer.id) by
  store_sales.customer.address.zip;\n\nwhere store_sales.date_dim.year = 1998\n
  and store_sales.date_dim.quarter = 2\n  and
  store_sales.customer.preferred_cust_flag = 'Y'\n\nselect\n
  store_sales.store.name as store_name,\n  sum(store_sales.net_profit) as
  total_net_profit\norder by store_name\nlimit 100;") at line 9, column 5.
  Expected one of:
          * "@"
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...rm(     split(zips, ','),     ??? (z) -> substring(z, 1, 2)   )

  Write stats: received 869 chars / 869 bytes; tail: …'t_profit) as
  total_net_profit\\norder by store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query08.preql --content import raw.store_sales as store_sales;

parameter zips string;

# Step 1: find ZIP codes where >10 preferred custo…les.date.quarter = 2

select
  store_sales.store.name as store_name,
  sum(store_sales.net_profit) as total_net_profit
order by store_name
limit 100;`

  ```text
  …
  tore_sales.customer.* so we can reference\n#
  store_sales.customer.address.zip\n# Count customers per ZIP where
  preferred\n\nauto pref_count_by_zip <- count(store_sales.customer.id ?
  store_sales.customer.preferred_cust_flag = 'Y') by
  (substring(store_sales.customer.address.zip, 1, 5));\n\nwhere
  store_sales.date.year = 1998\n  and store_sales.date.quarter = 2\n\nselect\n
  store_sales.store.name as store_name,\n  sum(store_sales.net_profit) as
  total_net_profit\norder by store_name\nlimit 100;") at line 11, column 56.
  Expected one of:
          * "@"
  Previous tokens: [Token('COMMA', ',')]

  Location:
  ...y_transform(split(zips, ','), ??? (z) -> substring(z, 1, 2));  #...

  Write stats: received 1143 chars / 1143 bytes; tail: …'t_profit) as
  total_net_profit\\norder by store_name\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;
import raw.return_reason as return_reason;

# Restrict to reason with id 1
…ext_discount, bucket_5_net_paid);

select
    bucket_1_value,
    bucket_2_value,
    bucket_3_value,
    bucket_4_value,
    bucket_5_value
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 8, column 1.
  Expected one of:
          * SELECT

  Location:
  ...ine items per quantity bucket ??? auto bucket_1_count <- count(s...

  Write stats: received 2445 chars / 2445 bytes; tail: …'ket_3_value,\\n
  bucket_4_value,\\n    bucket_5_value\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

# Restrict to return_reason.id = 1 (via store_returns datasource which has… as bucket_1,
    bucket_2_value as bucket_2,
    bucket_3_value as bucket_3,
    bucket_4_value as bucket_4,
    bucket_5_value as bucket_5
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 7, column 1.
  Expected one of:
          * SELECT

  Location:
  ...ine items per quantity bucket ??? auto bucket_1_count <- count(s...

  Write stats: received 2516 chars / 2516 bytes; tail: …'4_value as bucket_4,\\n
  bucket_5_value as bucket_5\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql --content import raw.store_sales as store_sales;

where store_sales.return_reason.id = 1

auto bucket_1_count <- count(store…t bucket_1_value as bucket_1, bucket_2_value as bucket_2, bucket_3_value as bucket_3, bucket_4_value as bucket_4, bucket_5_value as bucket_5 limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 5, column 1.
  Expected one of:
          * SELECT

  Location:
  ...e_sales.return_reason.id = 1  ??? auto bucket_1_count <- count(s...

  Write stats: received 2206 chars / 2206 bytes; tail: …'ket_4_value as bucket_4,
  bucket_5_value as bucket_5 limit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e -c import raw.store_sales as store_sales;
where store_sales.return_reason.id = 1
select
    case(count(store_sales.row_co…discount_amount ? store_sales.quantity between 81 and 100), avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)) as bucket_5
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 4, column 235.
  Expected one of:
          * WHEN

  Location:
  ...s.quantity between 1 and 20)) ??? as bucket_1,     case(count(st...

  Write stats: received 1344 chars / 1344 bytes; tail: …'ore_sales.quantity
  between 81 and 100)) as bucket_5\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query09.preql -e --content import raw.store_sales as store_sales;
where store_sales.return_reason.id = 1
select case(count(store_sales.row…discount_amount ? store_sales.quantity between 81 and 100), avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)) as bucket_5 limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 3, column 238.
  Expected one of:
          * WHEN

  Location:
  ...s.quantity between 1 and 20)) ??? as bucket_1, case(count(store_...

  Write stats: received 1324 chars / 1324 bytes; tail: …'ore_sales.quantity
  between 81 and 100)) as bucket_5 limit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query16.preql --content import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as catalog_returns;

merge catalog_returns.s…alog_sales.ext_ship_cost) as total_ext_ship_cost,
    sum(catalog_sales.net_profit) as total_net_profit
order by distinct_order_count desc
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
  ...log_sales.order_number not in ??? (select catalog_returns.order_...

  Write stats: received 1162 chars / 1162 bytes; tail: …'tal_net_profit\\norder
  by distinct_order_count desc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query21.preql --content import raw.inventory as inventory;
import raw.item as item;
import raw.warehouse as warehouse;
import raw.date as …between 2.0/3.0 and 3.0/2.0
    or (before_total is null and after_total is null)
order by warehouse_name nulls first, item_id nulls first
limit 100;`

  ```text
  refused to write 'query21.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 7, column 38.
  Expected one of:
          * _TERMINATOR

  Location:
  ...ice_filtered_items <- item.id ??? where item.current_price betwe...

  Write stats: received 1727 chars / 1727 bytes; tail: …'y warehouse_name nulls
  first, item_id nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `join-resolution`

- `trilogy run query04.preql`

  ```text
  Could not resolve connections for query with output
  ['local.customer_id<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'local.first_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.last_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.preferred_cust_flag<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query04.preql`

  ```text
  Could not resolve connections for query with output
  ['local.customer_id<Purpose.UNIQUE_PROPERTY>Derivation.BASIC>',
  'store_sales.customer.first_name<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.customer.last_name<Purpose.PROPERTY>Derivation.ROOT>',
  'store_sales.customer.preferred_cust_flag<Purpose.PROPERTY>Derivation.ROOT>']
  from current model.
  ```
- `trilogy run query05.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.KEY>Derivation.BASIC>',
  'local.id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --import raw/unified_sales.preql:unified --import raw/date.preql:date where date.date between '2000-08-23'::date and '2000-09-06'::date select un…t) as total_returns, sum(unified.net_profit) - sum(unified.return_net_loss) as profit order by channel, unified.channel_dim_id nulls first limit 100;`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.KEY>Derivation.BASIC>',
  'unified.channel_dim_id<Purpose.KEY>Derivation.ROOT>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query05.preql`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.KEY>Derivation.BASIC>',
  'local.id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query05.preql --debug`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.KEY>Derivation.BASIC>',
  'local.id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model.
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\scripts\parallel_executio
  n.py", line 578, in run_single_script_execution
      queries = exec.parse_text(
          text, root=base if isinstance(base
  …
  \concept_s
  trategies_v3.py", line 517, in source_query_concepts
      raise UnresolvableQueryException(
          f"Could not resolve connections for query with output {error_strings}
  from current model."
      )
  trilogy.core.exceptions.UnresolvableQueryException: Could not resolve
  connections for query with output
  ['local.channel<Purpose.KEY>Derivation.BASIC>',
  'local.id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run --debug --import raw/unified_sales.preql:unified --import raw/date.preql:date where date.date between '2000-08-23'::date and '2000-09-06'::date
s…rn_amount) as total_returns,
    sum(unified.net_profit) - sum(unified.return_net_loss) as profit
order by channel asc, id asc nulls first
limit 100;`

  ```text
  Could not resolve connections for query with output
  ['local.channel<Purpose.KEY>Derivation.BASIC>',
  'local.id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model.
  Full traceback:
  Traceback (most recent call last):
    File
  "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\scripts\parallel_executio
  n.py", line 578, in run_single_script_execution
      queries = exec.parse_text(
          text, root=base if isinstance(base
  …
  \concept_s
  trategies_v3.py", line 517, in source_query_concepts
      raise UnresolvableQueryException(
          f"Could not resolve connections for query with output {error_strings}
  from current model."
      )
  trilogy.core.exceptions.UnresolvableQueryException: Could not resolve
  connections for query with output
  ['local.channel<Purpose.KEY>Derivation.BASIC>',
  'local.id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_sales<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.total_returns<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.profit<Purpose.METRIC>Derivation.BASIC>'] from current model.
  ```
- `trilogy run query16.preql`

  ```text
  No datasource exists for root concept
  catalog_returns.had_return@Grain<catalog_returns.item.id,catalog_returns.order_
  number>, and no resolvable pseudonyms found from set(). This query is
  unresolvable from your environment. Check your datasources and imports to make
  sure this concept is bound.
  ```
- `trilogy run query16.preql`

  ```text
  No datasource exists for root concept
  catalog_sales.warehouse_id@Grain<catalog_sales.warehouse_id>, and no resolvable
  pseudonyms found from set(). This query is unresolvable from your environment.
  Check your datasources and imports to make sure this concept is bound.
  ```
- `trilogy run query20.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_id<Purpose.KEY>Derivation.BASIC>',
  'local.item_desc<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_category<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_class<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_current_price<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_ext_sales_price<Purpose.METRIC>Derivation.BASIC>',
  'local.pct_of_class_sales<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query20.preql`

  ```text
  Could not resolve connections for query with output
  ['local.item_id<Purpose.KEY>Derivation.BASIC>',
  'local.item_desc<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_category<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_class<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_current_price<Purpose.PROPERTY>Derivation.BASIC>',
  'local.total_ext_sales_price<Purpose.METRIC>Derivation.BASIC>',
  'local.pct_of_class_sales<Purpose.PROPERTY>Derivation.BASIC>'] from current
  model.
  ```
- `trilogy run query21.preql`

  ```text
  Could not resolve connections for query with output
  ['local.warehouse_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.before_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.after_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `other`

- `trilogy run query01.preql`

  ```text
  HAVING references 'local.customer_store_returns', which is
  not in the SELECT projection (line 9). Fix one of: (a) add it to SELECT —
  prefix with `--` to keep it out of the output rows, e.g. `select ...,
  --local.customer_store_returns`; (b) move the filter to WHERE — for an
  aggregate condition on a non-output grain, write the aggregate inline as
  `agg(x) by grain` directly in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 103 (char 102). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query04.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy run --all-rows --import raw/unified_sales.preql:unified select unified.sales_channel, case when unified.channel_dim_id is null and unified.sales_chan…eturns, sum(unified.net_profit) - sum(unified.return_net_loss) as profit order by unified.sales_channel, unified.channel_dim_id nulls first limit 10;`

  ```text
  (_duckdb.BinderException) Binder Error: column
  "unified_channel_dim_id" must appear in the GROUP BY clause or must be part of
  an aggregate function.
  Either add it to the GROUP BY list, or use "ANY_VALUE(unified_channel_dim_id)"
  if the exact value of "unified_channel_dim_id" is not important.

  LINE 83:     "abundant"."unified_channel_dim_id" asc nulls first
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
      "unified_catalog_returns_unified"."CR_ITEM_SK" as "unified_item_id",
      "unified_catalog_returns_unified"."CR_ORDER_NUMBER" as "unified_order_id",
      "unified_catalog_returns_unified
  …
  d_return_amount") as "total_returns",
      sum("abundant"."unified_net_profit") -
  sum("cheerful"."unified_return_net_loss") as "profit"
  FROM
      "abundant"
      LEFT OUTER JOIN "cheerful" on "abundant"."unified_item_id" =
  "cheerful"."unified_item_id" AND "abundant"."unified_order_id" =
  "cheerful"."unified_order_id" AND "abundant"."unified_sales_channel" =
  "cheerful"."unified_sales_channel"
  GROUP BY
      1,
      2
  ORDER BY
      "abundant"."unified_sales_channel" asc,
      "abundant"."unified_channel_dim_id" asc nulls first
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 3 column 6 (char 93). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query08.preql`

  ```text
  This script requires parameter "zips" to be set in
  environment.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 3 column 10 (char 143). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 5 column 10 (char 125). Re-issue the call with valid JSON arguments.
  ```

### `cli-misuse`

- `trilogy run - --import raw/date.preql select date.week_seq, date.year, date.month_of_year, date.day_of_week, date.date limit 10;`

  ```text
  'select date.week_seq, date.year, date.month_of_year, date.day_of_week, date.date limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy list_files raw`

  ```text
  No such command 'list_files'.
  ```
- `trilogy explore raw/return_reason.preql`

  ```text
  Invalid value for 'PATH': File 'raw/return_reason.preql' does not exist.
  ```
- `trilogy list_files .`

  ```text
  No such command 'list_files'.
  ```
- `trilogy read_file raw/catalog_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy show --show all raw/catalog_sales.preql --regex warehouse_id`

  ```text
  No such command 'show'.
  ```

### `undefined-concept`

- `trilogy run query04.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  first_name. Suggestions: ['customer.first_name']")
  ```
- `trilogy run --import raw/unified_sales.preql:unified select sales_channel, channel_dim_id, sum(ext_sales_price) as total_sales, sum(return_amount) as total_returns, sum(net_profit) - sum(return_net_loss) as profit, date.date limit 10;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  local.sales_channel. Suggestions: ['unified.sales_channel']")
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_sales.date_dim.year. Suggestions: ['store_sales.date.year',
  'store_sales.store.date.year', 'store_sales.return_date.year']")
  ```

### `type-error`

- `trilogy run --import raw/unified_sales.preql:unified select unified.sales_channel as channel, unified.channel_dim_id, count(unified.row_one) as cnt, sum(unified.date.date) as sum_date limit 10;`

  ```text
  Invalid argument type 'DATE' passed into SUM function in
  position 1 from concept: unified.date.date. Valid: 'BIGINT', 'BOOL', 'FLOAT',
  'INTEGER', 'NUMBER', 'NUMERIC'.
  ```
- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type 'ArrayType<STRING>' passed into
  CONTAINS function in position 1 from concept: local.zip_list. Valid: 'STRING'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/unified_sales.preql:unified select unified.sales_channel as channel, unified.channel_dim_id, count(unified.row_one) as cnt, sum(unified.date.date) limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...s cnt, sum(unified.date.date) ??? limit 10;
  ```
