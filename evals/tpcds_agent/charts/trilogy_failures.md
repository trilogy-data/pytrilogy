# Trilogy failure analysis — 20260525-144850

- Run `20260525-144850` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 359 | failed: 36 (10%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 17 | 47% |
| `join-resolution` | 9 | 25% |
| `syntax-parse` | 3 | 8% |
| `syntax-missing-alias` | 3 | 8% |
| `undefined-concept` | 2 | 6% |
| `cli-misuse` | 1 | 3% |
| `file-not-found` | 1 | 3% |

## Detail

### `other`

- `trilogy run query05.preql`
  - maximum recursion depth exceeded --- stderr ---
- `trilogy run query05.preql`
  - maximum recursion depth exceeded --- stderr ---
- `trilogy run query06.preql`
  - Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales? --- stderr ---
- `trilogy `
  - Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 86 (char 85). Re-issue the call with valid JSON arguments.
- `trilogy run -`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run -`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run -`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run -`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run -`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run query09.preql`
  - Multiple where clauses are not supported --- stderr ---
- `trilogy run -`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run --import raw/store_sales:store_sales --import raw/web_sales:web_sales select store_sales.customer.customer_addres…`
  - Cannot resolve query. No remaining priority concepts, have attempted {'local.wcnt', 'local.cnt'} out of with found {'local.cnt', 'store_sales.customer.customer_address.county'} --- stderr ---
- `trilogy run query13.preql`
  - Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales? --- stderr ---
- `trilogy run query16.preql`
  - HAVING references 'warehouse.warehouse_sk', which is not in the SELECT projection (line 13). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --warehouse.warehouse_…
- `trilogy run query19.preql`
  - Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales? --- stderr ---
- `trilogy run query19.preql`
  - Unable to import '.\store_sales.preql': [Errno 2] No such file or directory: '.\\store_sales.preql'. Did you mean: raw.store_sales? --- stderr ---
- `trilogy run query21.preql`
  - Unable to import '.\inventory.preql': [Errno 2] No such file or directory: '.\\inventory.preql'. Did you mean: raw.inventory? --- stderr ---

### `join-resolution`

- `trilogy run query03.preql`
  - Could not resolve connections for query with output ['item.brand<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand_id<Purpose.PROPERTY>Derivation.ROOT>', 'store_sales.date_dim.year<Purpose.PROPERTY>Derivation.ROOT>', 'loca…
- `trilogy run query03.preql`
  - Could not resolve connections for query with output ['item.brand<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand_id<Purpose.PROPERTY>Derivation.ROOT>', 'store_sales.date_dim.year<Purpose.PROPERTY>Derivation.ROOT>', 'loca…
- `trilogy run query03.preql`
  - Could not resolve connections for query with output ['item.brand<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand_id<Purpose.PROPERTY>Derivation.ROOT>', 'store_sales.date_dim.year<Purpose.PROPERTY>Derivation.ROOT>', 'loca…
- `trilogy run query03.preql`
  - Could not resolve connections for query with output ['item.brand<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand_id<Purpose.PROPERTY>Derivation.ROOT>', 'store_sales.date_dim.year<Purpose.PROPERTY>Derivation.ROOT>', 'loca…
- `trilogy run query03.preql --debug`
  - Could not resolve connections for query with output ['item.brand<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand_id<Purpose.PROPERTY>Derivation.ROOT>', 'store_sales.date_dim.year<Purpose.PROPERTY>Derivation.ROOT>', 'loca…
- `trilogy run query05.preql`
  - Could not resolve connections for query with output ['local.channel<Purpose.CONSTANT>Derivation.CONSTANT>', 'local.id<Purpose.PROPERTY>Derivation.BASIC>', 'local.sales<Purpose.METRIC>Derivation.AGGREGATE>', 'local.return…
- `trilogy run query09.preql`
  - Could not resolve connections for query with output ['store_sales.quantity<Purpose.PROPERTY>Derivation.ROOT>', 'store_sales.ext_discount_amt<Purpose.PROPERTY>Derivation.ROOT>', 'store_sales.net_paid<Purpose.PROPERTY>Deri…
- `trilogy run query19.preql`
  - Could not resolve connections for query with output ['item.brand_id<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand<Purpose.PROPERTY>Derivation.ROOT>', 'item.manufact_id<Purpose.PROPERTY>Derivation.ROOT>', 'item.manufact…
- `trilogy run query19.preql`
  - Could not resolve connections for query with output ['item.brand_id<Purpose.PROPERTY>Derivation.ROOT>', 'item.brand<Purpose.PROPERTY>Derivation.ROOT>', 'item.manufact_id<Purpose.PROPERTY>Derivation.ROOT>', 'item.manufact…

### `syntax-parse`

- `trilogy run --import raw/store_sales:store_sales select store_sales.date_dim.moy, count_distinct(store_sales.item.item_sk) as…`
  - --> 2:82 | 2 | select store_sales.date_dim.moy, count_distinct(store_sales.item.item_sk) as cnt group by store_sales.date_dim.moy; | ^--- | = expected metadata, limit, order_by, where, or having Location: ...re_sales.ite…
- `trilogy run query04.preql`
  - --> 11:5 | 11 | by store_sales.customer.customer_sk | ^--- | = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT Location: ...ales.ext_sales_price) / 2 ??? by st…
- `trilogy run -`
  - Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic). Location: as cnt, store_sales.quantity ??? from raw.store_sales as store_... --- stderr ---

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales:store_sales --import raw/web_sales:web_sales select store_sales.customer.customer_addres…`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...e_sales.customer.customer_sk) ??? limit 5; --- stderr ---
- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.date_dim.year, count(store_sales.ticket_number) lim…`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...nt(store_sales.ticket_number) ??? limit 3; --- stderr ---
- `trilogy run --import raw/inventory:inventory select inventory.date_dim.date, sum(inventory.quantity_on_hand) by inventory.dat…`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...d) by inventory.date_dim.date ??? order by inventory.date_dim.da... --- stderr ---

### `undefined-concept`

- `trilogy run query16.preql`
  - (UndefinedConceptException(...), "Undefined concept: ship_date.date. Suggestions: ['catalog_sales.ship_date.date', 'catalog_sales.ship_date.date_sk', 'catalog_sales.ship_date.date_id']") --- stderr ---
- `trilogy run query21.preql`
  - (UndefinedConceptException(...), "Undefined concept: quantity_on_hand. Suggestions: ['inventory.quantity_on_hand']") --- stderr ---

### `cli-misuse`

- `trilogy explore raw/store_sales.preql --show concepts --grep date_dim.date --grep store_sales.date`
  - Got unexpected extra argument (store_sales.date)

### `file-not-found`

- `trilogy run query11.preql`
  - exit_code: 2 --- stdout --- Input 'query11.preql' does not exist. --- stderr ---
