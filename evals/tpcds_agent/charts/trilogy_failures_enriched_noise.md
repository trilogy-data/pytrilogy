# Trilogy failure analysis — 20260810-211911

- Run `20260810-211903_enriched_noise` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 267 | failed: 12 (4%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 4 | 33% |
| `syntax-parse` | 3 | 25% |
| `cli-misuse` | 2 | 17% |
| `no-output` | 1 | 8% |
| `other` | 1 | 8% |
| `undefined-concept` | 1 | 8% |

## Detail

### `disabled-tool`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/repro.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/_q04_agent_rowset_union_join.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write inspect5.preql`

  ```text
  refused to write 'inspect5.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:24
    |
  5 |     count_distinct(i.id, i.category) as n_id_cat,
    |                        ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...n_id,
       count_distinct(i.id ??? , i.category) as n_id_cat,
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin:  --> 7:3
    |
  7 |   by cs.order_number
    |   ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ct(cs.warehouse.sk) as n_wh   ??? by cs.order_number having coun...
  ```
- `trilogy run -`

  ```text
  Syntax error in stdin:  --> 5:3
    |
  5 |   by asl.channel
    |   ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t(asl.warehouse.sk) as n_wh   ??? by asl.channel ;
  ```

### `cli-misuse`

- `trilogy file`

  ```text
  Usage: python -m trilogy.scripts.trilogy file [OPTIONS] COMMAND [ARGS]...

    Create, read, update, and delete files against local or remote backends.

    The same commands work against any backend Trilogy knows about. Only the
    local filesystem ships today; future releases will add cloud storage and
    remote git model backends, so write CLI-friendly scripts (and agent loops)
    against ``trilogy file`` instead of ad-hoc shell or python plumbing.

  Options:
    --help  Show this message and exit.

  Commands:
    delete  Delete the file or directory at PATH.
    exists  Exit 0 if PATH exists, 1 otherwise.
    list    List files at PATH (default: current directory).
    move    Move (or rename) SRC to DST.
    read    Read the file at PATH and write its contents to stdout.
    write   Write/overwrite the file at PATH.
  ```
- `trilogy file cat raw/catalog_sales.preql`

  ```text
  No such command 'cat'.
  ```

### `no-output`

- `trilogy run raw/all_sales.preql`

  ```text
  Nothing was executed: parsed 31 definition statement(s) (12 datasources, 11 imports, 5 concepts, 3 propertys) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```

### `other`

- `trilogy run answer_3863442186.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```

### `undefined-concept`

- `trilogy run -`

  ```text
  Syntax error in stdin: Undefined concept: catalog_ret_orders.cs_order_number. Suggestions: ['catalog_ret_orders.cs.order_number']
  ```
