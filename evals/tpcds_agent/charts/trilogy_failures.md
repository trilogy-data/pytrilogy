# Trilogy failure analysis — 20260523-023403

- Run `20260523-023403` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.1
- `trilogy` calls: 108 | failed: 19 (18%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 8 | 42% |
| `cli-misuse` | 5 | 26% |
| `other` | 4 | 21% |
| `undefined-concept` | 1 | 5% |
| `type-error` | 1 | 5% |

## Detail

### `syntax-parse`

- `trilogy run query40.preql`
  - --> 15:40 | 15 | ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT Location: catalog_sales.sol…
- `trilogy run query40.preql`
  - --> 15:40 | 15 | ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT Location: catalog_sales.sol…
- `trilogy run query40.preql`
  - --> 15:40 | 15 | ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT Location: catalog_sales.sol…
- `trilogy run query40.preql`
  - --> 13:111 | 13 | sum(catalog_sales.sales_price - coalesce(catalog_returns.refunded_cash, 0) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPE…
- `trilogy run query40.preql`
  - --> 13:111 | 13 | sum(catalog_sales.sales_price - coalesce(catalog_returns.refunded_cash, 0) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPE…
- `trilogy run query40.preql`
  - --> 13:111 | 13 | sum(catalog_sales.sales_price - coalesce(catalog_returns.refunded_cash, 0) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPE…
- `trilogy run -e LOG_LEVEL=DEBUG query40.preql`
  - --> 13:111 | 13 | sum(catalog_sales.sales_price - coalesce(catalog_returns.refunded_cash, 0) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPE…
- `trilogy run query99.preql`
  - --> 8:61 | 8 | count(catalog_sales.order_number) as orders_30_or_fewer ? date_diff(catalog_sales.ship_date.date, catalog_sales.sold_date.date, day) between 1 and 30, | ^--- | = expected metadata, limit, order_by, where, …

### `cli-misuse`

- `trilogy list`
  - No such command 'list'.
- `trilogy list`
  - No such command 'list'.
- `trilogy raw list`
  - No such command 'raw'.
- `trilogy raw list`
  - No such command 'raw'.
- `trilogy shell -c ls raw/`
  - No such command 'shell'.

### `other`

- `trilogy file list raw/`
  - exit_code: 1 --- stdout --- No such path: raw/ --- stderr ---
- `trilogy file list raw -r`
  - exit_code: 1 --- stdout --- No such path: raw --- stderr ---
- `trilogy file read query40.preql`
  - Use the `read_file(path)` tool to read files, not `trilogy file read`.
- `trilogy run --debug query62.preql`
  - 'query62.preql' is not a valid Dialects

### `undefined-concept`

- `trilogy run query03.preql`
  - (UndefinedConceptException(...), 'Undefined concept: year.') --- stderr ---

### `type-error`

- `trilogy run query91.preql`
  - Invalid argument type 'NUMERIC' passed into CONTAINS function in position 1 from concept: catalog_returns.returning_customer.customer_address.gmt_offset. Valid: 'STRING'. --- stderr ---
