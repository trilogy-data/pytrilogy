# Trilogy failure analysis — 20260811-015537

- Run `20260811-015536_enriched_noise` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 263 | failed: 9 (3%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 4 | 44% |
| `cli-misuse` | 3 | 33% |
| `undefined-concept` | 1 | 11% |
| `syntax-parse` | 1 | 11% |

## Detail

### `disabled-tool`

- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `cli-misuse`

- `trilogy file remove debug_null.preql`

  ```text
  No such command 'remove'.
  ```
- `trilogy explore root/store_sales.preql`

  ```text
  Invalid value for 'PATH': File 'root/store_sales.preql' does not exist.
  ```
- `trilogy file delete probe.preql probe2.preql probe3.preql probe4.preql probe5.preql probe6.preql`

  ```text
  Got unexpected extra arguments (probe2.preql probe3.preql probe4.preql probe5.preql probe6.preql)
  ```

### `undefined-concept`

- `trilogy run answer_1455459008.preql`

  ```text
  Syntax error in answer_1455459008.preql: 7 undefined concept references; fix all before re-running:
    - local.sales_price (line 9, in SELECT); did you mean: cs.sales_price, cs.ext_sales_price, cs.list_price?
    - sale_date.year (line 3, col 7, in WHERE); did you mean: cs.sale_date.year, sale_date.quarter, cs.return_date.year, cs.ship_date.year, cs.ship_customer.first_sales_date.year, cs.return_customer.first_sales_date.year?
    - sale_date.quarter (line 3, col 33, in WHERE); did you mean: cs.sale_date.quarter, sale_date.year, cs.return_date.quarter, cs.ship_date.quarter, cs.ship_customer.first_sales_date.quarter, cs.return_customer.first_sales_date.quarter?
    - billing_customer.current_address.zip (line 3, in WHERE); did you mean: cs.billing_customer.current_address.zip, billing_customer.current_address.state, cs.ship_customer.current_address.zip, cs.return_customer.current_address.zip, cs.return_refund_customer.current_address.zip, cs.return_address.zip?
    - billing_customer.current_address.state (line 5, col 8, in WHERE); did you mean: cs.billing_customer.current_address.state, billing_customer.current_address.zip, cs.ship_customer.current_address.state, cs.return_customer.current_address.state, cs.return_refund_customer.current_address.state, cs.return_address.state?
    - local.sales_price (line 6, col 8, in WHERE); did you mean: cs.sales_price, cs.ext_sales_price, cs.list_price?
    - billing_customer.current_address.zip (line 10, col 10, in ORDER BY); did you mean: cs.billing_customer.current_address.zip, billing_customer.current_address.state, cs.ship_customer.current_address.zip, cs.return_customer.current_address.zip, cs.return_refund_customer.current_address.zip, cs.return_address.zip?
  ```

### `syntax-parse`

- `trilogy file write probe3.preql`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...turn_customer.sk is not null
   ??? group by ss.item.id, ss.store....
  ```
