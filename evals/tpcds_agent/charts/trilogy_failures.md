# Trilogy failure analysis — 20260523-034814

- Run `20260523-034814` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.1
- `trilogy` calls: 123 | failed: 23 (19%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 11 | 48% |
| `syntax-parse` | 9 | 39% |
| `cli-misuse` | 2 | 9% |
| `syntax-missing-alias` | 1 | 4% |

## Detail

### `other`

- `trilogy run --debug raw/inventory.preql`
  - 'raw/inventory.preql' is not a valid Dialects
- `trilogy file list raw/.`
  - exit_code: 1 --- stdout --- No such path: raw/. --- stderr ---
- `trilogy file list raw`
  - exit_code: 1 --- stdout --- No such path: raw --- stderr ---
- `trilogy file list raw/`
  - exit_code: 1 --- stdout --- No such path: raw/ --- stderr ---
- `trilogy run query26.preql`
  - Value 'Y' is not valid for enum field 'catalog_sales.promotion.channel_email'. Allowed values: 'N'. --- stderr ---
- `trilogy run query26.preql`
  - Value 'Y' is not valid for enum field 'catalog_sales.promotion.channel_email'. Allowed values: 'N'. --- stderr ---
- `trilogy run --debug query26.preql`
  - 'query26.preql' is not a valid Dialects
- `trilogy run query26.preql duckdb`
  - Value 'Y' is not valid for enum field 'catalog_sales.promotion.channel_email'. Allowed values: 'N'. --- stderr ---
- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.promotion.channel_email where catalog_sales.promoti…`
  - Value 'Y' is not valid for enum field 'catalog_sales.promotion.channel_email'. Allowed values: 'N'. --- stderr ---
- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.promotion.channel_event where catalog_sales.promoti…`
  - Value 'Y' is not valid for enum field 'catalog_sales.promotion.channel_event'. Allowed values: 'N'. --- stderr ---
- `trilogy run query42.preql`
  - Cannot order by column store_sales.date_dim.year that is not in the output projection; line: 3 --- stderr ---

### `syntax-parse`

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

### `syntax-missing-alias`

- `trilogy run --import raw/catalog_sales:catalog_sales select distinct catalog_sales.promotion.channel_email, catalog_sales.pro…`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...atalog_sales; select distinct ??? catalog_sales.promotion.channe... --- stderr ---
