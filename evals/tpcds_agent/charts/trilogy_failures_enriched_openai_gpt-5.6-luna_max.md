# Trilogy failure analysis — 20260814-132004

- Run `20260814-132003_enriched_openai_gpt-5.6-luna_max` | `openai/gpt-5.6-luna` | sf=1
- `trilogy` calls: 172 | failed: 10 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `cli-misuse` | 8 | 80% |
| `syntax-parse` | 2 | 20% |

## Detail

### `cli-misuse`

- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```
- `trilogy explore raw.web_sales`

  ```text
  Invalid value for 'PATH': File 'raw.web_sales' does not exist.
  ```
- `trilogy explore raw.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'raw.catalog_sales' does not exist.
  ```
- `trilogy explore raw.all_sales`

  ```text
  Invalid value for 'PATH': File 'raw.all_sales' does not exist.
  ```
- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```
- `trilogy explore raw.web_sales`

  ```text
  Invalid value for 'PATH': File 'raw.web_sales' does not exist.
  ```
- `trilogy explore raw.catalog_sales`

  ```text
  Invalid value for 'PATH': File 'raw.catalog_sales' does not exist.
  ```
- `trilogy explore raw.store_sales`

  ```text
  Invalid value for 'PATH': File 'raw.store_sales' does not exist.
  ```

### `syntax-parse`

- `trilogy file write answer_1858999935.preql --run`

  ```text
  refused to write 'answer_1858999935.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   = 6), 2) as saturday_ratio,
   ??? union join daily_a.week_sequen...
  ```
- `trilogy file write answer_3863442186.preql --run`

  ```text
  refused to write 'answer_3863442186.preql': not syntactically valid Trilogy.

  Parse error:
    --> 17:1
     |
  17 | select
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...le_date.year in (2001, 2002)
   ??? select
     s.billing_customer.i...
  ```
