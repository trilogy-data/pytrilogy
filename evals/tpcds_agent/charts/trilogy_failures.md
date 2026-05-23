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

- `trilogy run query82.preql`
  - Could not resolve connections for query with output ['item.item_id<Purpose.PROPERTY>Derivation.ROOT>', 'item.item_desc<Purpose.PROPERTY>Derivation.ROOT>', 'item.current_price<Purpose.PROPERTY>Derivation.ROOT>'] from curr…
- `trilogy run --import raw/inventory:inventory --import raw/store_sales:ss select inventory.date_dim.date, inventory.quantity_o…`
  - Could not resolve connections for query with output ['inventory.date_dim.date<Purpose.PROPERTY>Derivation.ROOT>', 'inventory.quantity_on_hand<Purpose.PROPERTY>Derivation.ROOT>', 'ss.item.item_id<Purpose.PROPERTY>Derivati…
