# Trilogy failure analysis — 20260720-020432

- Run `20260720-020432_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 93 | failed: 17 (18%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 14 | 82% |
| `syntax-parse` | 2 | 12% |
| `syntax-missing-alias` | 1 | 6% |

## Detail

### `other`

- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3825713089.preql`

  ```text
  Resolution error in answer_3825713089.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 9). The requested concepts split into 3 disconnected subgraphs: {catalog_sales.bill_customer.customer_sk, catalog_sales.item.item_sk, catalog_sales.sold_date.moy, catalog_sales.sold_date.year, catalog_sales_net_profit}; {item_code, item_description, store_code, store_name, store_sales_net_profit, store_sales.customer.customer_sk, store_sales.date_dim.moy, store_sales.date_dim.year, store_sales.item.item_sk, store_sales.ticket_number}; {store_return_net_loss, store_returns.customer.customer_sk, store_returns.date_dim.moy, store_returns.date_dim.year, store_returns.item.item_sk, store_returns.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_3825713089.preql`

  ```text
  Syntax error in answer_3825713089.preql: Join chain repeats source `store_returns` (keys `store_returns.customer.customer_sk` and `store_returns.item.item_sk`). A `=` chain joins ONE key across distinct sources; join a composite key with `and` or separate clauses (e.g. `a.k1 = b.k1 and a.k2 = b.k2`). (line 26, column 1)
  ```
- `trilogy run answer_3825713089.preql`

  ```text
  Syntax error in answer_3825713089.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key. (line 9, column 1)
  ```
- `trilogy run answer_1809796058.preql`

  ```text
  Syntax error in answer_1809796058.preql: Undefined concept: matched.ticket_number. Suggestions: ['matched.customer_sk', 'matched.item_sk', 'ss.ticket_number', 'sr.ticket_number', 'sales_data.ticket_number', 'returns_data.ticket_number']
  ```
- `trilogy run answer_1809796058.preql`

  ```text
  Syntax error in answer_1809796058.preql: Undefined concept: store.store_sk. Suggestions: ['ss.store.store_sk', 'sr.store.store_sk', 'store.store_name', 'store.state', 'store.street_type', 'paired.store_sk']
  ```
- `trilogy file read answer_1809796058.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2869182220.preql`

  ```text
  Resolution error in answer_2869182220.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 2 disconnected subgraphs: {c.customer_address.city, c.customer_demographics.demo_sk, c.household_demographics.income_band.lower_bound, c.household_demographics.income_band.upper_bound, customer_code, full_name}; {sr.customer.customer_demographics.demo_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read answer_2869182220.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write answer_2869182220.preql --content import raw.store_returns as sr;
import raw.customer as c;

# Get the distinct demographic keys from retu…esce(c.first_name, '')) as full_name
    --sr.ticket_number as --tk,
    --sr.item.item_sk as --isk
order by
    c.customer_id nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:5
     |
  20 |     --sr.ticket_number as --tk,
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t_name, '')) as full_name     ??? --sr.ticket_number as --tk,
  ```
- `trilogy file write answer_2869182220.preql --content import raw.store_returns as sr;
import raw.customer as c;

# Use a scoped join between store_returns and…as --item_sk
union join sr.customer.customer_demographics.demo_sk = c.customer_demographics.demo_sk
order by
    c.customer_id nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:27
     |
  14 |     --sr.ticket_number as --ticket_number,
     |                           ^---
     |
     = expected IDENTIFIER
  Location:
  ...me,     --sr.ticket_number as ??? --ticket_number,     --sr.item...
  ```

### `syntax-missing-alias`

- `trilogy file write answer_2869182220.preql --content import raw.store_returns as sr;
import raw.customer as c;

# Get the demographic keys of customers who d…t_name, '')) as full_name,
    -- hidden grain fields
    --sr.ticket_number,
    --sr.item.item_sk
order by
    c.customer_id nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden grain fields
      --sr.ticket_number as hidden_grain_fields_sr_ticket_number`
  Location:
  ...) as full_name,     -- hidden ??? grain fields     --sr.ticket_n...
  ```
