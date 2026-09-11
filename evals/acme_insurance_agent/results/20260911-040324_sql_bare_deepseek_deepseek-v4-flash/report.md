# ACME Insurance Agent Eval — 20260911-040324

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 207.0s  |  Agent time: 179.1s  |  Avg/query: 16.3s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 82
- Tool calls: 170  →  {'run_query': 124, 'list_files': 11, 'write_file': 11, 'run_file': 11, 'return_control_to_user': 11, 'read_file': 2}
- `trilogy` subcommands: {}
- Tool success rate: 100% (170/170 ok)
- Tokens: 337566 total (320730 prompt + 16836 completion)
- Cache: 276096 prompt tokens served from cache (86%)  |  cache-adjusted cost: 89080 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `run_query` | 124 | 566 | 1345 | 0 | 0% |
| `list_files` | 11 | 116 | 157 | 0 | 0% |
| `write_file` | 11 | 54 | 54 | 0 | 0% |
| `run_file` | 11 | 297 | 338 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |
| `read_file` | 2 | 390 | 390 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'run_query': 1} — total 1

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 292 | 4 | 3 |  |  |
| 02 | pass | 1 | 1 | 475 | 352 | 3 |  |  |
| 03 | pass | 1 | 1 | 350 | 3 | 2 |  |  |
| 04 | pass | 1 | 1 | 312 | 4 | 2 |  |  |
| 05 | pass | 2 | 2 | 735 | 976 | 2 |  |  |
| 06 | pass | 1 | 1 | 297 | 4 | 1 |  |  |
| 07 | pass | 2 | 2 | 304 | 4 | 3 |  |  |
| 08 | pass | 1 | 1 | 396 | 6 | 3 |  |  |
| 09 | pass | 1 | 1 | 191 | 3 | 3 |  |  |
| 10 | pass | 1 | 1 | 43 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 42 | 1 | 1 |  |  |

- Candidate latency: 1358ms total vs reference 24ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Total claims = 2. Wrote and validated answer_2200255695.sql: `SELECT COUNT(*) AS total_claims FROM Claim;` — returns one row, one column.
