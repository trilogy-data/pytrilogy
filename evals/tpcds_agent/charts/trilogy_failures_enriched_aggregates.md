# Trilogy failure analysis — 20260813-023115

- Run `20260813-023115` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 264 | failed: 8 (3%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 4 | 50% |
| `disabled-tool` | 3 | 38% |
| `other` | 1 | 12% |

## Detail

### `syntax-parse`

- `trilogy file write probe5.preql --run-and-delete`

  ```text
  refused to write 'probe5.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:32
     |
  11 |     and a.channel_dim_text_id <> a.return_channel_dim_text_id
     |                                ^---
     |
     = expected sum_operator
  Location:
     and a.channel_dim_text_id < ??? > a.return_channel_dim_text_id...
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
   --> 9:6
    |
  9 |      by *),
    |      ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...rice) is not null) as n
        ??? by *),
       (where cs.sale_dat...
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  refused to write 'probe6.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:55
    |
  5 |     count(ss.return_quantity ? ss.return_customer.sk <> ss.customer.sk) as diff_cust_returns,
    |                                                       ^---
    |
    = expected sum_operator
  Location:
  ...tity ? ss.return_customer.sk < ??? > ss.customer.sk) as diff_cust...
  ```
- `trilogy file write answer_2133330107.preql --run`

  ```text
  refused to write 'answer_2133330107.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:57
    |
  6 |   and substring(ss.customer.current_address.zip, 1, 5) <> substring(ss.store.zip, 1, 5)
    |                                                         ^---
    |
    = expected sum_operator
  Location:
  ...r.current_address.zip, 1, 5) < ??? > substring(ss.store.zip, 1, 5...
  ```

### `disabled-tool`

- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `other`

- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: union arm 0 projects 3 column(s) but the output signature declares 1. Each arm must project exactly one column per output item, in order.
  ```
