# Trilogy failure analysis — 20260529-010541

- Run `20260529-010540_enriched` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 240 | failed: 19 (8%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 6 | 32% |
| `other` | 5 | 26% |
| `undefined-concept` | 4 | 21% |
| `cli-misuse` | 3 | 16% |
| `file-not-found` | 1 | 5% |

## Detail

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
  refused to write 'query16.preql': not syntactically valid Trilogy.

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

- `trilogy run query06.preql`

  ```text
  Unable to import '.\store_sales.preql': [Errno 2] No such
  file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 4 column 10 (char 106). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Extra data: line 1 column 111 (char 110). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file write query11.preql --content`

  ```text
  Option '--content' requires an argument.
  ```
- `trilogy run query20.preql`

  ```text
  Unable to import '.\catalog_sales.preql': [Errno 2] No such
  file or directory: '.\\catalog_sales.preql'. Did you mean: raw.catalog_sales?
  ```

### `undefined-concept`

- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web_sales.ext_list_price. Suggestions: ['web_sales.list_price',
  'store_sales.ext_list_price', 'web_sales.ext_sales_price']")
  ```
- `trilogy run query11.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  web_sales.ext_list_price. Suggestions: ['web_sales.list_price',
  'store_sales.ext_list_price', 'web_sales.ext_sales_price']")
  ```
- `trilogy run query19.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_sales.date_dim.date. Suggestions: ['store_sales.date.date',
  'store_sales.date.id', 'store_sales.date.quarter']")
  ```
- `trilogy run query19.preql`

  ```text
  (UndefinedConceptException(...), "Undefined concept:
  store_sales.date_date. Suggestions: ['store_sales.date.date',
  'store_sales.date.id', 'store_sales.date.quarter']")
  ```

### `cli-misuse`

- `trilogy explore raw/catalog_returns.prekl --grep order_number`

  ```text
  Invalid value for 'PATH': File 'raw/catalog_returns.prekl' does not exist.
  ```
- `trilogy read_file raw/customer.preql`

  ```text
  No such command 'read_file'.
  ```
- `trilogy read_file raw/customer_demographic.preql`

  ```text
  No such command 'read_file'.
  ```

### `file-not-found`

- `trilogy run query01.preql`

  ```text
  Input 'query01.preql' does not exist.
  ```
