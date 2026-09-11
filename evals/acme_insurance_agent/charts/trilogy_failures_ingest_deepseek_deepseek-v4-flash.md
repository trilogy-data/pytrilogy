# Trilogy failure analysis — 20260911-132452

- Run `20260911-132451_ingest_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 146 | failed: 3 (2%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 1 | 33% |
| `join-resolution` | 1 | 33% |
| `undefined-concept` | 1 | 33% |

## Detail

### `syntax-parse`

- `trilogy file write answer_1318992729.preql --run`

  ```text
  refused to write 'answer_1318992729.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:33
    |
  3 | where apr.party_role_code = 'AG';
    |                                 ^---
    |
    = expected THEN_LA, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...ere apr.party_role_code = 'AG' ??? ;    select apr.party_identifi...
  ```

### `join-resolution`

- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {apr.agreement_identifier, apr.party_identifier, apr.party_role_code}; {p.policy_number}. Are you missing a join or merge statement to relate them?
  ```

### `undefined-concept`

- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  Syntax error in probe_b.preql: Undefined concept: pr.nonexistent_field_xyz (line 3, col 8, in SELECT).
  ```
