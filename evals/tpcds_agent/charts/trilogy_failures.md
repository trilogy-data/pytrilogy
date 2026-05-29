# Trilogy failure analysis — 20260528-151500

- Run `20260528-151500_base` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 679 | failed: 57 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 18 | 32% |
| `other` | 13 | 23% |
| `join-resolution` | 10 | 18% |
| `cli-misuse` | 8 | 14% |
| `syntax-missing-alias` | 4 | 7% |
| `undefined-concept` | 3 | 5% |
| `type-error` | 1 | 2% |

## Detail

### `syntax-parse`

- `trilogy file write query01.preql --content import raw.store_returns as store_returns;

# Filter to stores in Tennessee and returns in 2000
where store_return…omer_total_return as total_return
having customer_total_return > 1.2 * avg_return_by_store
order by store_returns.customer.customer_id asc
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 9, column 1.
  Expected one of:
          * SELECT

  Location:
   per store in the same period ??? auto customer_total_return <-

  Write stats: received 702 chars / 702 bytes; tail: …'e\\norder by
  store_returns.customer.customer_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query01.preql --content import raw.store_returns as store_returns;

where store_returns.store.state = 'TN'
  and year(store_returns.date_d…omer_total_return as total_return
having customer_total_return > 1.2 * avg_return_by_store
order by store_returns.customer.customer_id asc
limit 100;`

  ```text
  refused to write 'query01.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('IDENTIFIER', 'auto') at line 6, column 1.
  Expected one of:
          * SELECT

  Location:
  ...eturns.date_dim.date) = 2000  ??? auto customer_total_return <-

  Write stats: received 553 chars / 553 bytes; tail: …'e\\norder by
  store_returns.customer.customer_id asc\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run --import raw/item.preql:item select item.manufact_id, count(item.item_sk) as cnt group by item.manufact_id order by item.manufact_id asc limit 200;`

  ```text
  --> 2:53
    |
  2 | select item.manufact_id, count(item.item_sk) as cnt group by
  item.manufact_id order by item.manufact_id asc limit 200;
    |                                                     ^---
    |
    = expected metadata, limit, order_by, where, or having
  Location:
  ...d, count(item.item_sk) as cnt ??? group by item.manufact_id orde...
  ```
- `trilogy file write query09.preql --content import raw.reason as reason;
import raw.store_sales as store_sales;

# Reason table has no join path to store_sale…ore_sales.quantity between 81 and 100),
        avg(store_sales.net_paid ? store_sales.quantity between 81 and 100)
    ) as bucket_5_result
limit 1;`

  ```text
  refused to write 'query09.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 21, column 7.
  Expected one of:
          * WHEN

  Location:
  ...ntity between 1 and 20)     ) ??? as bucket_1_result,     case(

  Write stats: received 1985 chars / 1985 bytes; tail: …'antity between 81 and
  100)\\n    ) as bucket_5_result\\nlimit 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql --content import raw/store_sales as store_sales;

# Year 2001 filter
where
    store_sales.date_dim.year = 2001
    # Demogr…vg(store_sales.ext_wholesale_cost) as avg_extended_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_extended_wholesale_cost
limit 10;`

  ```text
  …
  tore_sales.customer.customer_address.state in ('VA', 'TX',
  'MS')\n                and store_sales.net_profit between 50 and 250\n
  )\n        )\n    )\nselect\n    avg(store_sales.quantity) as
  avg_ticket_quantity,\n    avg(store_sales.ext_sales_price) as
  avg_extended_sales_price,\n    avg(store_sales.ext_wholesale_cost) as
  avg_extended_wholesale_cost,\n    sum(store_sales.ext_wholesale_cost) as
  total_extended_wholesale_cost\nlimit 10;") at line 1, column 11.
  Expected one of:
          * IMPORT_DOT
          * _TERMINATOR
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /store_sales as store_sales;

  Write stats: received 2158 chars / 2158 bytes; tail: …'t_wholesale_cost) as
  total_extended_wholesale_cost\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query13.preql -e -c import raw/store_sales as store_sales;

# Year 2001 filter
where
    store_sales.date_dim.year = 2001
    # Demographi…vg(store_sales.ext_wholesale_cost) as avg_extended_wholesale_cost,
    sum(store_sales.ext_wholesale_cost) as total_extended_wholesale_cost
limit 10;`

  ```text
  …
  tore_sales.customer.customer_address.state in ('VA', 'TX',
  'MS')\n                and store_sales.net_profit between 50 and 250\n
  )\n        )\n    )\nselect\n    avg(store_sales.quantity) as
  avg_ticket_quantity,\n    avg(store_sales.ext_sales_price) as
  avg_extended_sales_price,\n    avg(store_sales.ext_wholesale_cost) as
  avg_extended_wholesale_cost,\n    sum(store_sales.ext_wholesale_cost) as
  total_extended_wholesale_cost\nlimit 10;") at line 1, column 11.
  Expected one of:
          * "as"
          * IMPORT_DOT
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /store_sales as store_sales;

  Write stats: received 2158 chars / 2158 bytes; tail: …'t_wholesale_cost) as
  total_extended_wholesale_cost\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy run - duck_db --import raw/catalog_sales.preql:catalog_sales --import raw/catalog_returns.preql:catalog_returns`

  ```text
  --> 4:1
    |
  4 | merge catalog_returns.order_number into catalog_sales.order_number;
    | ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail,
  PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...log_sales.order_number = 1592 ??? merge catalog_returns.order_nu...
  ```
- `trilogy file write query20.preql --content import raw.catalog_sales as catalog_sales;

# Filter for items in Sports, Books, or Home categories
# Sold between…sales.item.current_price
order by item_category asc, item_class asc, item_id asc, item_description asc, pct_of_class_total asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'group ') at line 16, column 1.
  Expected one of:
          * LIMIT
          * WHERE
          * ORDER
          * HAVING
          * METADATA
          * COMMA
          * MERGE
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'pct_of_class_total')]

  Location:
  ...m.class as pct_of_class_total ??? group by catalog_sales.item.it...

  Write stats: received 1023 chars / 1023 bytes; tail: …'scription asc,
  pct_of_class_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import raw.catalog_sales as catalog_sales;

# Filter for items in Sports, Books, or Home categories
# Sold between…sales.item.current_price
order by item_category asc, item_class asc, item_id asc, item_description asc, pct_of_class_total asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'group ') at line 16, column 1.
  Expected one of:
          * METADATA
          * COMMA
          * HAVING
          * LIMIT
          * ORDER
          * MERGE
          * WHERE
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'pct_of_class_total')]

  Location:
  ...m.class as pct_of_class_total ??? group by catalog_sales.item.it...

  Write stats: received 1023 chars / 1023 bytes; tail: …'scription asc,
  pct_of_class_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query20.preql --content import raw.catalog_sales as catalog_sales;

# Filter for items in Sports, Books, or Home categories
# Sold between…ss as pct_of_class_total
order by item_category asc, item_class asc, item_id asc, item_description asc, pct_of_class_total asc nulls first
limit 100;`

  ```text
  refused to write 'query20.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'group ') at line 8, column 1.
  Expected one of:
          * SELECT
  Previous tokens: [Token('DATE', 'date')]

  Location:
  ...:date and '1999-03-24'::date  ??? group by catalog_sales.item.it...

  Write stats: received 1024 chars / 1024 bytes; tail: …'scription asc,
  pct_of_class_total asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query22.preql --content import raw/inventory as inventory;

where inventory.date_dim.year = 2000
select
    inventory.item.product_name,
 …_name asc nulls first, inventory.item.brand asc nulls first, inventory.item.class asc nulls first, inventory.item.category asc nulls first
limit 100;`

  ```text
  refused to write 'query22.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '/inventory as
  inventory;\n\nwhere inventory.date_dim.year = 2000\nselect\n
  inventory.item.product_name,\n    inventory.item.brand,\n
  inventory.item.class,\n    inventory.item.category,\n
  avg(inventory.quantity_on_hand) as avg_qoh\norder by avg_qoh asc nulls first,
  inventory.item.product_name asc nulls first, inventory.item.brand asc nulls
  first, inventory.item.class asc nulls first, inventory.item.category asc nulls
  first\nlimit 100;') at line 1, column 11.
  Expected one of:
          * _TERMINATOR
          * IMPORT_DOT
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /inventory as inventory;  wher...

  Write stats: received 446 chars / 446 bytes; tail: …'ls first,
  inventory.item.category asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…tore_sales.date_dim.year between 2000 and 2003
having count(store_sales.ticket_number) by substring(store_sales.item.item_desc, 1, 30) > 4
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '(store_sales.item.item_desc, 1,
  30), store_sales.date_dim.date_sk;\n\nselect\n
  substring(store_sales.item.item_desc, 1, 30) as item_desc_prefix\nwhere
  store_sales.date_dim.year between 2000 and 2003\nhaving
  count(store_sales.ticket_number) by substring(store_sales.item.item_desc, 1,
  30) > 4\nlimit 100;') at line 10, column 85.
  Expected one of:
          * _TERMINATOR
  Previous tokens: [Token('IDENTIFIER', 'substring')]

  Location:
  ...es.ticket_number) by substring ??? (store_sales.item.item_desc, 1...

  Write stats: received 793 chars / 793 bytes; tail: …'
  substring(store_sales.item.item_desc, 1, 30) > 4\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql -e -c import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

…me,
    sum(web_sales.quantity * web_sales.list_price) as total_sales
order by last_name asc, first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 11, column 75.
  Expected one of:
          * _TERMINATOR

  Location:
  ..._sales.item.item_desc, 1, 30) ??? where store_sales.date_dim.yea...

  Write stats: received 2258 chars / 2258 bytes; tail: …'asc, first_name asc,
  total_sales asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql -e -c import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

…me,
    sum(web_sales.quantity * web_sales.list_price) as total_sales
order by last_name asc, first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n') at line 32, column 1.
  Expected one of:
          * WHERE
          * HAVING
          * METADATA
          * COMMA
          * LIMIT
          * ORDER
          * _TERMINATOR
          * MERGE
  Previous tokens: [Token('IDENTIFIER', 'total_sales')]

  Location:
  ...es.list_price) as total_sales ??? union where web_sales.sold_dat...

  Write stats: received 1849 chars / 1849 bytes; tail: …'asc, first_name asc,
  total_sales asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql -e -c import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

…b_sales.sold_date.moy = 2
  and freq_item_cnt > 4
  and best_customer
order by last_name asc, first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('SELECT', 'select') at line 31, column 1.
  Expected one of:
          * HAVING
          * ORDER
          * LIMIT
          * MERGE
          * _TERMINATOR

  Location:
  ...m_cnt > 4   and best_customer ??? select web_sales.bill_customer...

  Write stats: received 1847 chars / 1847 bytes; tail: …'asc, first_name asc,
  total_sales asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql -e -c import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

…les
having sum(web_sales.quantity * web_sales.list_price) is not null
order by last_name asc, first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  …
       * DATASOURCE
          * PURPOSE
          * SELECT
          * IMPORT
          * PARAMETER
          * UNIQUE
          * SELF_IMPORT
          * RAW_SQL
          * COPY
          * SHOW
          * WITH
          * _DEF_TABLE
          * FROM
          * WHERE
          * TYPE
          * SHORTHAND_MODIFIER
          * DEF
          * "merge"i
          * ROWSET
          * PROPERTY
          * CHART
          * DATASOURCE_ROOT
          * PUBLISH_ACTION
          * _PROPERTIES
          * AUTO
          * PERSIST_MODE
          * PARAM
          * VALIDATE
          * MOCK
          * $END
  Previous tokens: [Token('LINE_SEPARATOR', '\n')]

  Location:
  ...es asc nulls first limit 100; ??? union # Web sales Feb 2000 - f...

  Write stats: received 2027 chars / 2027 bytes; tail: …'asc, first_name asc,
  total_sales asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql -e -c import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

…les
having sum(web_sales.quantity * web_sales.list_price) is not null
order by last_name asc, first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n') at line 31, column 1.
  Expected one of:
          * LIMIT
          * _TERMINATOR
          * ORDER
          * MERGE
  Previous tokens: [Token('NULL', 'null')]

  Location:
  ...sales.list_price) is not null ??? union where web_sales.sold_dat...

  Write stats: received 1901 chars / 1901 bytes; tail: …'asc, first_name asc,
  total_sales asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query23.preql -e -c import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

…les
having sum(web_sales.quantity * web_sales.list_price) is not null
order by last_name asc, first_name asc, total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('ORDER_IDENTIFIER', 'union\n') at line 34, column 1.
  Expected one of:
          * _TERMINATOR
          * ORDER
          * LIMIT
          * MERGE
  Previous tokens: [Token('NULL', 'null')]

  Location:
  ...sales.list_price) is not null ??? union # Part 2: web sales wher...

  Write stats: received 2117 chars / 2117 bytes; tail: …'asc, first_name asc,
  total_sales asc nulls first\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `other`

- `trilogy run query01.preql`

  ```text
  HAVING references 'store_returns.store.store_sk', which is
  not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT —
  prefix with `--` to keep it out of the output rows, e.g. `select ...,
  --store_returns.store.store_sk`; (b) move the filter to WHERE — for an
  aggregate condition on a non-output grain, write the aggregate inline as
  `agg(x) by grain` directly in WHERE.
  ```
- `trilogy run query01.preql`

  ```text
  HAVING references 'store_returns.store.store_sk', which is
  not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT —
  prefix with `--` to keep it out of the output rows, e.g. `select ...,
  --store_returns.store.store_sk`; (b) move the filter to WHERE — for an
  aggregate condition on a non-output grain, write the aggregate inline as
  `agg(x) by grain` directly in WHERE.
  ```
- `trilogy run query01.preql`

  ```text
  WHERE clause aggregates at multiple grains are not allowed:
  `ref:store_returns.store.state = TN and year(ref:store_returns.date_dim.date) =
  2000 and
  sum(ref:store_returns.return_amt)<['ref:store_returns.customer.customer_sk',
  'ref:store_returns.store.store_sk']> >
  multiply(1.2,avg(sum(ref:store_returns.return_amt)<['ref:store_returns.customer
  .customer_sk',
  'ref:store_returns.store.store_sk']>)<['ref:store_returns.store.store_sk']>)`.
  Aggregates filter rows AFTER grouping - use HAVING (post-aggregate filter), or
  align all aggregates to the same `by` grain so the filter is a pure row-level
  pre-aggregate predicate; Line: 3
  ```
- `trilogy run query01.preql`

  ```text
  HAVING references 'store_returns.store.store_sk', which is
  not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT —
  prefix with `--` to keep it out of the output rows, e.g. `select ...,
  --store_returns.store.store_sk`; (b) move the filter to WHERE — for an
  aggregate condition on a non-output grain, write the aggregate inline as
  `agg(x) by grain` directly in WHERE.
  ```
- `trilogy run query01.preql`

  ```text
  (_duckdb.BinderException) Binder Error: aggregate function
  calls cannot be nested

  LINE 18:     "total_return" > 1.2 *
  avg(sum("store_returns_store_returns"."sr_return_amt"))
                                          ^
  [SQL: SELECT
      "store_returns_customer_customer"."c_customer_id" as
  "store_returns_customer_customer_id",
      "store_returns_store_returns"."sr_store_sk" as
  "store_returns_store_store_sk",
      sum("store_returns_store_returns"."sr_return_amt") as "total_return"
  FROM
      "store_returns" as "store_returns_store_returns"
      INNER JOIN "store" as "store_returns_store_store" on
  …
  e_sk" =
  "store_returns_time_dim_time_dim"."t_time_sk"
      LEFT OUTER JOIN "customer" as "store_returns_customer_customer" on
  "store_returns_store_returns"."sr_customer_sk" =
  "store_returns_customer_customer"."c_customer_sk"
  WHERE
      "store_returns_store_store"."s_state" = 'TN' and
  year("store_returns_date_dim_date_dim"."d_date") = 2000

  GROUP BY
      1,
      2
  HAVING
      "total_return" > 1.2 *
  avg(sum("store_returns_store_returns"."sr_return_amt"))

  ORDER BY
      "store_returns_customer_customer"."c_customer_id" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query01.preql`

  ```text
  HAVING references 'local.avg_return_per_store', which is not
  in the SELECT projection (line 6). Fix one of: (a) add it to SELECT — prefix
  with `--` to keep it out of the output rows, e.g. `select ...,
  --local.avg_return_per_store`; (b) move the filter to WHERE — for an aggregate
  condition on a non-output grain, write the aggregate inline as `agg(x) by
  grain` directly in WHERE.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 163 (char 162). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 133 (char 132). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run raw/catalog_sales.preql duck_db --import raw/catalog_sales.preql:catalog_sales select catalog_sales.order_number, catalog_sales.warehouse.warehouse_sk limit 5;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 154 (char 153). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query21.preql`

  ```text
  Unable to import '.\inventory.preql': [Errno 2] No such file
  or directory: '.\\inventory.preql'. Did you mean: raw.inventory?
  ```
- `trilogy run query23.preql`

  ```text
  (_duckdb.ParserException) Parser Error: syntax error at or
  near "="

  LINE 189: ...  "friendly"."cust_total" > 0.5 * "abhorrent"."max_cust_total" =
  True
                                                                              ^
  [SQL:
  WITH
  juicy as (
  SELECT
      "catalog_sales_catalog_sales"."cs_item_sk" as "web_sales_item_item_sk",
      "catalog_sales_catalog_sales"."cs_sold_date_sk" as
  "catalog_sales_sold_date_date_sk"
  FROM
      "catalog_sales" as "catalog_sales_catalog_sales"
  GROUP BY
      1,
      2),
  macho as (
  SELECT
      "store_sales_store_sales"."ss_customer_sk" as
  "store_sales_custom
  …
  P BY
      1,
      2,
      3,
      4,
      "kaput"."catalog_sales_net_paid_inc_ship",
      "kaput"."catalog_sales_sold_date_date_sk")
  SELECT
      "charming"."catalog_sales_bill_customer_last_name" as "last_name",
      "charming"."catalog_sales_bill_customer_first_name" as "first_name",
      sum("charming"."catalog_sales_quantity" *
  "charming"."_virt_filter_list_price_3408323415419430") as "total_sales"
  FROM
      "charming"
  GROUP BY
      1,
      2
  ORDER BY
      "last_name" asc,
      "first_name" asc,
      "total_sales" asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file write --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sales as catalog_sales;

# …sales.net_profit) as catalog_sale_net_profit
order by
    item_id asc,
    item_description asc,
    store_id asc,
    store_name asc
limit 100; path`

  ```text
  trilogy file write: `--content` takes a SINGLE string argument. Your args list put 2 separate tokens after --content (treating it like a shell command). In a tool call, pass the entire file body as one string element after --content, with newlines embedded literally — e.g.
    {"args": ["file", "write", "query70.preql", "--content", "import raw.store_sales as store_sales;\n\nselect ..."]}
  Alternatively use `--escapes` with a single-line `\n`-escaped string.
  ```

### `join-resolution`

- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket_1_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_net<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket_1_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_net<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['local.bucket_1_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_net<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['reason.id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.bucket_1_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_net<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['reason.id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.bucket_1_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_net<Purpose.METRIC>Derivation.AGGREGATE>'] from current
  model.
  ```
- `trilogy run query09.preql`

  ```text
  Could not resolve connections for query with output
  ['reason.id<Purpose.PROPERTY>Derivation.ROOT>',
  'local.bucket_1_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_1_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_2_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_3_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_4_avg_net<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_count<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_disc<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.bucket_5_avg_net<Purpose.METRIC>Derivation.AGGREGATE>'] from current
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
- `trilogy run query21.preql`

  ```text
  Could not resolve connections for query with output
  ['local.warehouse_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.before_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.after_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query21.preql`

  ```text
  Could not resolve connections for query with output
  ['local.warehouse_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.before_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.after_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```
- `trilogy run query21.preql`

  ```text
  Could not resolve connections for query with output
  ['local.warehouse_name<Purpose.PROPERTY>Derivation.BASIC>',
  'local.item_id<Purpose.PROPERTY>Derivation.BASIC>',
  'local.before_total<Purpose.METRIC>Derivation.AGGREGATE>',
  'local.after_total<Purpose.METRIC>Derivation.AGGREGATE>'] from current model.
  ```

### `cli-misuse`

- `trilogy run - --import raw/item:item select item.category, avg(item.current_price) as avg_price, count(item.item_sk) as cnt group by item.category limit 10;`

  ```text
  'select item.category, avg(item.current_price) as avg_price, count(item.item_sk) as cnt group by item.category limit 10;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run -i --import raw/catalog_sales.preql:catalog_sales select catalog_sales.order_number, catalog_sales.warehouse.warehouse_sk limit 5;`

  ```text
  'select catalog_sales.order_number, catalog_sales.warehouse.warehouse_sk limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run -i --import raw/catalog_sales.preql:catalog_sales select catalog_sales.order_number, catalog_sales.warehouse.warehouse_sk limit 5;`

  ```text
  'select catalog_sales.order_number, catalog_sales.warehouse.warehouse_sk limit 5;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/store_returns:store_returns --import raw/catalog_sales:catalog_sales merge store_returns.custom…alog_sales.item.item_sk into ~store_sales.item.item_sk; select store_sales.item.item_id, store_sales.item.item_desc, store_sales.store.state limit 5;`

  ```text
  'merge store_returns.item.item_sk into ~store_sales.item.item_sk;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy read_file query18.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/store_sales.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/store_returns.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/catalog_sales.preql`

  ```text
  No such command 'read_file'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.item.manufact_id, count(store_sales.ticket_number) where store_sales.item.manufact_id = 128 limit 10;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...nt(store_sales.ticket_number) ??? where store_sales.item.manufac...
  ```
- `trilogy run --import raw/store_sales:store_sales select count(store_sales.ticket_number) where store_sales.date_dim.year = 2001 limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...nt(store_sales.ticket_number) ??? where store_sales.date_dim.yea...
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.date_dim.quarter_name, store_sales.quantity, store_sales.item.item_id, store_sales.item.item_desc, store_sales.store.state limit 10 where store_sales.date_dim.quarter_name = '2001Q1';`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...desc, store_sales.store.state ??? limit 10 where store_sales.dat...
  ```
- `trilogy file write test_ss.preql --content import raw.store_sales as store_sales;

where store_sales.date_dim.year = 2001
  and store_sales.date_dim.moy = 4
select
    store_sales.item.item_id,
    store_sales.item.item_desc,
    sum(store_sales.net_profit)
limit 10;`

  ```text
  refused to write 'test_ss.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT
  x+1 AS y`
  Location:
  ...(store_sales.net_profit) limit ??? 10;

  Write stats: received 224 chars / 224 bytes; tail: …'es.item.item_desc,\\n
  sum(store_sales.net_profit)\\nlimit 10;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `undefined-concept`

- `trilogy run query02.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  dd.week_seq. Suggestions: ['ws.sold_date.week_seq', 'cs.sold_date.week_seq',
  'ws.ship_date.week_seq']")
  ```
- `trilogy run --import raw/date_dim:date_dim select date_dim.quarter_name where quarter_name like '%2001%' limit 5;`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  quarter_name. Suggestions: ['date_dim.quarter_name']")
  ```
- `trilogy run --import raw/inventory:inventory select date_dim.year, item.product_name, avg(quantity_on_hand) as avg_qoh limit 5;`

  ```text
  (UndefinedConceptException(...), "line: 2: Undefined concept:
  date_dim.year. Suggestions: ['inventory.date_dim.year',
  'inventory.date_dim.fy_year']")
  ```

### `type-error`

- `trilogy run query08.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,84093,2857…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Invalid argument type ArrayType<STRING>' passed into
  SUBSTRING function from function SPLIT in position 1. Valid: 'STRING'
  ```
