# Trilogy failure analysis — 20260602-131604

- Run `20260602-131553_enriched` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 20 | failed: 1 (5%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 1 | 100% |

## Detail

### `other`

- `trilogy run query05.preql`

  ```text
  SELECT output 'local.net_profit_after_returns' is defined by
  an expression that references 'local.net_profit_after_returns' itself (line
  34). This is a recursive self-reference: an alias cannot redefine a name its
  own calculation reads. Rename the output to a distinct name (e.g. `... as
  net_profit_after_returns_out`).
  ```
