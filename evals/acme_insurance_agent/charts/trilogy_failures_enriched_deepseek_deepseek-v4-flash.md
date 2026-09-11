# Trilogy failure analysis — 20260911-152007

- Run `20260911-152006_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 64 | failed: 1 (2%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 1 | 100% |

## Detail

### `undefined-concept`

- `trilogy file write answer_4002635042.preql --run`

  ```text
  Syntax error in answer_4002635042.preql: Undefined concept: policy.policy_number (line 3, col 8, in SELECT). Suggestions: ['pa.policy.policy_number', 'pa.coverage.policy.policy_number', 'pa.policy.policy_count']
  ```
