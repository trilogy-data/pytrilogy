# ACME Insurance Agent Eval — 20260911-132452

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 9/11** (82%)
- Status breakdown: {'pass': 9, 'fail': 2}
- Wall time: 76.9s  |  Agent time: 256.5s  |  Avg/query: 23.3s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 78
- Tool calls: 127  →  {'list_files': 10, 'run_query': 82, 'write_file': 12, 'run_file': 11, 'return_control_to_user': 11, 'read_file': 1}
- `trilogy` subcommands: {}
- Tool success rate: 99% (126/127 ok)
- Tokens: 292444 total (277992 prompt + 14452 completion)
- Cache: 241024 prompt tokens served from cache (87%)  |  cache-adjusted cost: 75522 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `run_query` | 82 | 619 | 1346 | 0 | 0% |
| `write_file` | 12 | 54 | 54 | 0 | 0% |
| `run_file` | 11 | 301 | 333 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |
| `list_files` | 10 | 121 | 157 | 0 | 0% |
| `read_file` | 1 | 40 | 40 | 0 | 0% |

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 271 | 5 | 4 |  |  |
| 02 | fail | 1 | 2 | 550 | 6 | 4 |  | result set differs from reference |
| 03 | pass | 1 | 1 | 670 | 6 | 4 |  |  |
| 04 | pass | 1 | 1 | 211 | 3 | 2 |  |  |
| 05 | pass | 2 | 2 | 413 | 6 | 2 |  |  |
| 06 | pass | 1 | 1 | 239 | 4 | 2 |  |  |
| 07 | pass | 2 | 2 | 282 | 5 | 3 |  |  |
| 08 | fail | 1 | 2 | 369 | 6 | 3 |  | result set differs from reference |
| 09 | pass | 1 | 1 | 183 | 3 | 4 |  |  |
| 10 | pass | 1 | 1 | 49 | 1 | 12 |  |  |
| 11 | pass | 1 | 1 | 41 | 1 | 1 |  |  |

- Candidate latency: 46ms total vs reference 41ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Saved answer_2200255695.sql: `SELECT COUNT(*) AS claim_count FROM Claim`, which returns 1 row with the total number of claims (2).
