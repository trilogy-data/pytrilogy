# Trilogy failure analysis — 20260911-135033

- Run `20260911-135032_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 69 | failed: 1 (1%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 1 | 100% |

## Detail

### `undefined-concept`

- `trilogy file write probe_h.preql --run-and-delete`

  ```text
  Syntax error in probe_h.preql: Undefined concept: pol.policy.id. Suggestions: ['pol.policy_holder_id', 'pol.policy_count', 'pol.policy_number', 'p.policy.id', 'p.coverage.policy.id', 'pol.id']
  ```
