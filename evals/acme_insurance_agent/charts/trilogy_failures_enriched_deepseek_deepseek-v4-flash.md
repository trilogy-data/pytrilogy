# Trilogy failure analysis — 20260911-040324

- Run `20260911-040324_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 84 | failed: 3 (4%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 2 | 67% |
| `syntax-parse` | 1 | 33% |

## Detail

### `undefined-concept`

- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: Undefined concept: premium.policy_number (line 8, in SELECT). Suggestions: ['premium.policy.policy_number', 'premium.coverage.policy.policy_number', 'premium.policy.id', 'policy.policy_number', 'claim.policy.policy_number', 'coverage.policy.policy_number']
  ```
- `trilogy file write probe_cov.preql --run-and-delete`

  ```text
  Syntax error in probe_cov.preql: Undefined concept: coverage.policy_number (line 3, in SELECT). Suggestions: ['coverage.policy.policy_number', 'coverage.policy.id', 'coverage.policy.agent_id']
  ```

### `syntax-parse`

- `trilogy file write probe_policy.preql --run-and-delete`

  ```text
  refused to write 'probe_policy.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
     policy.policy_count as pc  ???
  ```
