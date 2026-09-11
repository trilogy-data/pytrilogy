# Trilogy failure analysis — 20260911-011644

- Run `20260911-011643_ingest_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 192 | failed: 3 (2%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 1 | 33% |
| `join-resolution` | 1 | 33% |
| `cli-misuse` | 1 | 33% |

## Detail

### `other`

- `trilogy explore root/Claim_Coverage.preql --show`

  ```text
  Option '--show' requires an argument.
  ```

### `join-resolution`

- `trilogy run probe_rel.preql --dry-run`

  ```text
  Resolution error in probe_rel.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {ccid}; {cid}. Are you missing a join or merge statement to relate them?
  ```

### `cli-misuse`

- `trilogy file delete probe_join.preql probe_rel.preql probe_rel2.preql`

  ```text
  Got unexpected extra arguments (probe_rel.preql probe_rel2.preql)
  ```
