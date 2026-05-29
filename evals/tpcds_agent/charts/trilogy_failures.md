# Trilogy failure analysis — 20260529-010541

- Run `20260529-010540_base` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 268 | failed: 30 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 17 | 57% |
| `syntax-parse` | 6 | 20% |
| `file-not-found` | 4 | 13% |
| `join-resolution` | 1 | 3% |
| `undefined-concept` | 1 | 3% |
| `syntax-missing-alias` | 1 | 3% |

## Detail

### `other`

- `trilogy file write query01.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

# Merge the shared sold_date dimens…2002;

# Now match: 2001 week_seq = 2002 week_seq - 53
# For each 2001 week, find the corresponding 2002 week and compute ratios per dow
 --from-file`

  ```text
  Option '--from-file' requires an argument.
  ```
- `trilogy file exists query02.preql`

  ```text

  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 10 column 6 (char 158). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 46 (char 45). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 4 column 10 (char 152). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 67 (char 66). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 40 (char 39). Re-issue the call with valid JSON arguments.

  [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different action.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 55 (char 54). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query12.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy run query12.preql`

  ```text
  Unable to import '.\web_sales.preql': [Errno 2] No such file
  or directory: '.\\web_sales.preql'. Did you mean: raw.web_sales?
  ```
- `trilogy file write query16.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy run query16.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
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
- `trilogy run query18.preql`

  ```text
  (_duckdb.BinderException) Binder Error: GROUPING statement
  cannot be used without groups

  LINE 153:     grouping_id(coalesce("vacuous"."catalog_sales_bill_customer...
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
  log_sales_item_item_id","young"."catalog_sales_item
  _item_id") asc nulls first,
      grouping_id(coalesce("vacuous"."catalog_sales_bill_customer_customer_addres
  s_country","young"."catalog_sales_bill_customer_customer_address_country"),coal
  esce("vacuous"."catalog_sales_bill_customer_customer_address_state","young"."ca
  talog_sales_bill_customer_customer_address_state"),coalesce("vacuous"."catalog_
  sales_bill_customer_customer_address_county","young"."catalog_sales_bill_custom
  er_customer_address_county")) asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

# Merge the shared sold_date dimens…s
auto sales_2002 <- sum(combined_daily) by (web_sales.sold_date.week_seq, web_sales.sold_date.dow) where web_sales.sold_date.year = 2002;

select 1;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 16, column 107.
  Expected one of:
          * _TERMINATOR

  Location:
   catalog_sales.sold_date.dow) ??? where catalog_sales.sold_date....

  Write stats: received 1155 chars / 1155 bytes; tail: …'_date.dow) where
  web_sales.sold_date.year = 2002;\\n\\nselect 1;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query02.preql --content import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sales;

# Merge the shared sold_date dimens… web_sales.sold_date.dow = 6 and web_sales.sold_date.week_seq = catalog_sales.sold_date.week_seq + 53, 0), 0), 2) as sat_ratio
order by week_seq asc;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('WHERE', 'where') at line 8, column 119.
  Expected one of:
          * _TERMINATOR

  Location:
   catalog_sales.sold_date.dow) ??? where catalog_sales.sold_date....

  Write stats: received 2715 chars / 2715 bytes; tail: …'eek_seq + 53, 0), 0), 2)
  as sat_ratio\\norder by week_seq asc;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query04.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…web_ratio
order by
    customer_id nulls first,
    first_name nulls first,
    last_name nulls first,
    preferred_cust_flag nulls first
limit 100;`

  ```text
  refused to write 'query04.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('__ANON_7', 'by') at line 32, column 31.
  Expected one of:
          * _TERMINATOR

  Location:
  ...auto cat_ratio <- ca02 / ca01 ??? by catalog_sales.bill_customer...

  Write stats: received 2755 chars / 2755 bytes; tail: …' nulls first,\\n
  preferred_cust_flag nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query16.preql -c import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as catalog_returns;

# Merge order_number from cata…ales.ext_ship_cost) as total_extended_ship_cost,
    sum(catalog_sales.net_profit) as total_net_profit
order by distinct_order_count desc
limit 100;
`

  ```text
  refused to write 'query16.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('AS', 'as') at line 8, column 118.
  Expected one of:
          * _TERMINATOR

  Location:
  ...by catalog_sales.order_number ??? as wh_count;  # Flag if an ord...

  Write stats: received 1110 chars / 1110 bytes; tail: …'al_net_profit\\norder by
  distinct_order_count desc\\nlimit 100;\\n'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write raw/store_sales.preql --content import raw/date_dim as date_dim;`

  ```text
  refused to write 'raw/store_sales.preql': not syntactically valid Trilogy.

  Parse error:
  Unexpected token Token('DOUBLE_STRING_CHARS', '/date_dim as date_dim;') at line
  1, column 11.
  Expected one of:
          * _TERMINATOR
          * IMPORT_DOT
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /date_dim as date_dim;

  Write stats: received 32 chars / 32 bytes; tail: …'import raw/date_dim as
  date_dim;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```
- `trilogy file write query17.preql --content import raw/store_sales as store_sales;
import raw/store_returns as store_returns;
import raw/catalog_sales as cata…alog_sales.quantity), 0) as cs_quantity_cv
order by item_id asc nulls first, item_description asc nulls first, store_state asc nulls first
limit 100;`

  ```text
  …

  nullif(avg(store_returns.return_quantity), 0) as sr_return_quantity_cv,\n
  count(catalog_sales.quantity) as cs_quantity_count,\n
  avg(catalog_sales.quantity) as cs_quantity_avg,\n
  stddev(catalog_sales.quantity) as cs_quantity_stddev,\n
  stddev(catalog_sales.quantity) / nullif(avg(catalog_sales.quantity), 0) as
  cs_quantity_cv\norder by item_id asc nulls first, item_description asc nulls
  first, store_state asc nulls first\nlimit 100;') at line 1, column 11.
  Expected one of:
          * IMPORT_DOT
          * _TERMINATOR
          * "as"
  Previous tokens: [Token('IDENTIFIER', 'raw')]

  Location:
  import raw ??? /store_sales as store_sales; i...

  Write stats: received 2177 chars / 2177 bytes; tail: …'tion asc nulls first,
  store_state asc nulls first\\nlimit 100;'.
  If the tail looks cut off (mid-identifier, mid-statement) your response was
  likely truncated by max_tokens — re-issue with the COMPLETE file body, do not
  resend the same bytes. Pass --force to bypass validation only when you intend a
  partial draft.
  ```

### `file-not-found`

- `trilogy run query02.preql`

  ```text
  Input 'query02.preql' does not exist.
  ```
- `trilogy run --param date=2001-01-01 --param delay=0 query05.preql`

  ```text
  Input 'query05.preql' does not exist.
  ```
- `trilogy run query19.preql`

  ```text
  Input 'query19.preql' does not exist.
  ```
- `trilogy run --param START_DATE=1999-02-22 --param END_DATE=1999-03-24 query20.preql`

  ```text
  Input 'query20.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run --import raw/store_sales.preql:store_sales --import raw/item.preql:item select store_sales.item.item_sk, item.current_price, item.category limit 5;`

  ```text
  Could not resolve connections for query with output
  ['store_sales.item.item_sk<Purpose.KEY>Derivation.ROOT>',
  'item.current_price<Purpose.PROPERTY>Derivation.ROOT>',
  'item.category<Purpose.PROPERTY>Derivation.ROOT>'] from current model.
  ```

### `undefined-concept`

- `trilogy run --import raw/store_sales.preql:store_sales where store_sales.date_dim.date between '2001-01-01'::date and '2001-01-31'::date and store_sales.item…ddress.state, count(store_sales.customer.customer_sk) as customer_count having customer_count >= 10 order by customer_count asc, state asc limit 100;`

  ```text
  (UndefinedConceptException(...), 'Undefined concept: state.')
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales:store_sales select store_sales.customer.customer_demographics.marital_status, count(store_sales.ticket_number) limit 5;`

  ```text
  Syntax [201]: Missing alias? Alias must be specified with
  "AS" - e.g. `SELECT x+1 AS y`
  Location:
  ...nt(store_sales.ticket_number) ??? limit 5;
  ```
