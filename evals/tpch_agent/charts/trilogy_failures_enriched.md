# Trilogy failure analysis — 20260629-175016

- Run `20260629-175015_enriched` | `deepseek/deepseek-chat` | sf=0.1
- `trilogy` calls: 189 | failed: 8 (4%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 8 | 100% |

## Detail

### `other`

- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/order.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query13.preql`

  ```text
  Resolution error in query13.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 2 disconnected subgraphs: {local._cust_orders_bucket}; {local._cust_orders_cust_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query13.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read query16.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query17.preql`

  ```text
  Syntax error in query17.preql: 3 undefined concept references; fix all before re-running:
    - part.brand (line 3, col 7, in WHERE); did you mean: lineitem.part.brand, part.id?
    - part.container (line 4, col 7, in WHERE); did you mean: lineitem.part.container?
    - part.id (line 5, col 59, in WHERE); did you mean: lineitem.part.supplier.nation.region.id, lineitem.part.supplier.nation.id, lineitem.part.supplier.id, lineitem.part.id, lineitem.order.customer.nation.region.id, lineitem.order.customer.nation.id?
  ```
