# Trilogy failure analysis — 20260523-152402

- Run `20260523-152402` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 67 | failed: 14 (21%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 10 | 71% |
| `other` | 2 | 14% |
| `join-resolution` | 2 | 14% |

## Detail

### `syntax-parse`

- `trilogy run query40.preql`
  - --> 19:117 | 19 | sum((catalog_sales.ext_sales_price - coalesce(catalog_returns.refunded_cash, 0)) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARIS…
- `trilogy run query40.preql`
  - --> 19:117 | 19 | sum((catalog_sales.ext_sales_price - coalesce(catalog_returns.refunded_cash, 0)) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARIS…
- `trilogy run query40.preql`
  - --> 19:115 | 19 | sum(catalog_sales.ext_sales_price - coalesce(catalog_returns.refunded_cash, 0) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON…
- `trilogy run query40.preql`
  - --> 19:117 | 19 | sum((catalog_sales.ext_sales_price - coalesce(catalog_returns.refunded_cash, 0)) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARIS…
- `trilogy run query40.preql`
  - --> 19:115 | 19 | sum(catalog_sales.ext_sales_price - coalesce(catalog_returns.refunded_cash, 0) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON…
- `trilogy run query40.preql`
  - --> 19:115 | 19 | sum(catalog_sales.ext_sales_price - coalesce(catalog_returns.refunded_cash, 0) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON…
- `trilogy --debug run query40.preql`
  - --> 19:115 | 19 | sum(catalog_sales.ext_sales_price - coalesce(catalog_returns.refunded_cash, 0) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON…
- `trilogy run query40.preql`
  - --> 19:115 | 19 | sum(catalog_sales.ext_sales_price - coalesce(catalog_returns.refunded_cash, 0) ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON…
- `trilogy run query62.preql`
  - --> 5:40 | 5 | web_sales.warehouse.warehouse_name[:20] as w_name, | ^--- | = expected INT_LITERAL_PART or MULTILINE_STRING Location: ...ales.warehouse.warehouse_name[ ??? :20] as w_name, web_sales.... --- stderr ---
- `trilogy run --import raw/inventory:inventory select inventory.date_dim.date, count(inventory.item.item_sk) from inventory whe…`
  - Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic). Location: ...count(inventory.item.item_sk) ??? from inventory where inventory... --- stderr ---

### `other`

- `trilogy `
  - Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 45 (char 44). Re-issue the call with valid JSON arguments.
- `trilogy run query42.preql`
  - Cannot order by column store_sales.date_dim.year that is not in the output projection; line: 3 --- stderr ---

### `join-resolution`

- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.promotion.channel_email, catalog_sales.promotion.ch…`
  - Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic). Location: ...sales.promotion.channel_event ??? from catalog_sales limit 10; --- stderr ---
- `trilogy run query37.preql`
  - --> 10:34 | 10 | and inventory.item.item_sk in (select catalog_sales.item.item_sk) | ^--- | = expected access_chain or literal Location: ...nd inventory.item.item_sk in ( ??? select catalog_sales.item.item... --- stderr …
- `trilogy run query40.preql`
  - --> 15:66 | 15 | sum(catalog_sales.sales_price ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCE…
- `trilogy run query40.preql`
  - --> 15:66 | 15 | sum(catalog_sales.sales_price ? catalog_sales.sold_date.date | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCE…
- `trilogy run query62.preql`
  - --> 8:20 | 8 | count(distinct web_sales.order_number) ? date_diff(web_sales.ship_date.date, web_sales.sold_date.date, day) between 0 and 30 as count_0_to_30, | ^--- | = expected dot_tail, bracket_tail, dcolon_tail, COMPA…
- `trilogy run query99.preql`
  - --> 8:209 | 8 | count_distinct(catalog_sales.order_number ? (date_diff(catalog_sales.ship_date.date, catalog_sales.sold_date.date, DAY) >= 0 and date_diff(catalog_sales.ship_date.date, catalog_sales.sold_date.date, DAY) …
- `trilogy run query99.preql`
  - --> 8:208 | 8 | count_distinct(catalog_sales.order_number ? date_diff(catalog_sales.ship_date.date, catalog_sales.sold_date.date, DAY) >= 0 and date_diff(catalog_sales.ship_date.date, catalog_sales.sold_date.date, DAY) &…
- `trilogy run query99.preql`
  - --> 8:124 | 8 | count_distinct(catalog_sales.order_number ? date_diff(catalog_sales.ship_date.date, catalog_sales.sold_date.date, DAY) &lt;= 30) as orders_30_days_or_fewer, | ^--- | = expected LOGICAL_OR, LOGICAL_AND, do…
- `trilogy run query99.preql`
  - --> 8:115 | 8 | count(catalog_sales.order_number ? date_diff(catalog_sales.ship_date.date, catalog_sales.sold_date.date, DAY) &lt;= 30) as orders_30_days_or_fewer, | ^--- | = expected LOGICAL_OR, LOGICAL_AND, dot_tail, b…

### `cli-misuse`

- `trilogy explore raw/store_sales.preql`
  - Invalid value for 'PATH': File 'raw/store_sales.preql' does not exist.
- `trilogy database query duckdb --param SELECT DISTINCT p_channel_email, p_channel_event FROM promotion LIMIT 10`
  - No such command 'query'.

### `undefined-concept`

- `trilogy run --import raw/catalog_sales:catalog_sales select distinct catalog_sales.promotion.channel_email, catalog_sales.pro…`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...atalog_sales; select distinct ??? catalog_sales.promotion.channe... --- stderr ---
