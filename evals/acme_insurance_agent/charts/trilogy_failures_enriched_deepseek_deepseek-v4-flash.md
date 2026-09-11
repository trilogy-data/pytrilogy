# Trilogy failure analysis — 20260911-132452

- Run `20260911-132451_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 68 | failed: 2 (3%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `undefined-concept` | 1 | 50% |
| `join-resolution` | 1 | 50% |

## Detail

### `undefined-concept`

- `trilogy file write probe_claims.preql --run-and-delete`

  ```text
  Syntax error in probe_claims.preql: 2 undefined concept references; fix all before re-running:
    - claim.claim_count (line 2, col 8, in SELECT); did you mean: claim.claim_number, c.claim_count?
    - claim.claim_number (line 2, col 27, in SELECT); did you mean: claim.claim_count, c.claim_number?
  ```

### `join-resolution`

- `trilogy file write probe_claims.preql --run-and-delete`

  ```text
  Resolution error in probe_claims.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {c.claim_number, c.id}; {ca.loss_amount}. Are you missing a join or merge statement to relate them?
  ```
