# Trilogy failure analysis — 20260810-200846

- Run `20260810-200843_enriched_noise` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 221 | failed: 10 (5%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 4 | 40% |
| `cli-misuse` | 2 | 20% |
| `syntax-parse` | 2 | 20% |
| `other` | 1 | 10% |
| `undefined-concept` | 1 | 10% |

## Detail

### `disabled-tool`

- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

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

- `trilogy file cat raw/store_sales.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy agent-info syntax show`

  ```text
  No such command 'show'.
  ```

### `syntax-parse`

- `trilogy run --stdin`

  ```text
  Syntax error in stdin:  --> 1:3
    |
  1 | --stdin;
    |   ^---
    |
    = expected PURPOSE, PROPERTY, UNIQUE, or AUTO
  Location:
  -- ??? stdin;
  ```
- `trilogy file write check_tmp3.preql`

  ```text
  refused to write 'check_tmp3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...06'::date)) as total_returns
   ??? group by a.channel;
  ```

### `other`

- `trilogy run answer_3697706765.preql --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,61265,98294,25782,17920,18426,98235,40081,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Resolution error in answer_3697706765.preql: Could not resolve condition existence arguments ['local._virt_func_split_4785012549328100']
  ```

### `undefined-concept`

- `trilogy run answer_3697440276.preql`

  ```text
  Syntax error in answer_3697440276.preql: Undefined concept: c.rev. Suggestions: ['c.pref', 'c.yr', 'combined.rev']
  ```
