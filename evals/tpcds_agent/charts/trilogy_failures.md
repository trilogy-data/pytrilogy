# Trilogy failure analysis — 20260522-205524

- Run `20260522-205524` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.1
- `trilogy` calls: 28 | failed: 4 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 2 | 50% |
| `other` | 1 | 25% |
| `syntax-missing-alias` | 1 | 25% |

## Detail

### `syntax-parse`

- `trilogy run --import raw/inventory.preql:inventory select inventory.item.manufact_id, count(inventory.item.item_sk) as cnt wh…`
  - --> 2:153 | 2 | select inventory.item.manufact_id, count(inventory.item.item_sk) as cnt where inventory.date_dim.date between '2000-05-25'::date and '2000-07-24'::date group by inventory.item.manufact_id order by cnt des…
- `trilogy run select s_store_name as store_name from store limit 100;`
  - Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic). Location: ...ct s_store_name as store_name ??? from store limit 100; --- stderr ---

### `other`

- `trilogy run query96.preql`
  - Value 'ese' is not valid for enum field 'store_sales.store.store_name'. Allowed values: 'ought'. --- stderr ---

### `syntax-missing-alias`

- `trilogy run select distinct s_store_name from store limit 100;`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: select distinct ??? s_store_name from store limit --- stderr ---
