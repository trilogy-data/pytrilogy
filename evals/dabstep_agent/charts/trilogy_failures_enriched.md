# Trilogy failure analysis — 20260711-192227

- Run `20260711-192227` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 14 | failed: 4 (29%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 4 | 100% |

## Detail

### `other`

- `trilogy file read manual.md`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read payments-readme.md`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read schema_notes.md`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2761220070.preql`

  ```text
  Syntax error in answer_2761220070.preql: ORDER BY references 'pymts.psp_reference', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --pymts.psp_reference order by pymts.psp_reference asc`.
  ```
