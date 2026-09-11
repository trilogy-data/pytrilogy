# ACME Insurance Agent Eval — 20260911-011644

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 10/11** (91%)
- Status breakdown: {'pass': 10, 'fail': 1}
- Wall time: 129.5s  |  Agent time: 221.3s  |  Avg/query: 20.1s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 88
- Tool calls: 159  →  {'list_files': 8, 'run_query': 117, 'write_file': 11, 'run_file': 11, 'return_control_to_user': 11, 'read_file': 1}
- `trilogy` subcommands: {}
- Tool success rate: 100% (159/159 ok)
- Tokens: 384446 total (364222 prompt + 20224 completion)
- Cache: 321024 prompt tokens served from cache (88%)  |  cache-adjusted cost: 95524 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `run_query` | 117 | 572 | 1345 | 0 | 0% |
| `write_file` | 11 | 54 | 54 | 0 | 0% |
| `run_file` | 11 | 296 | 338 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |
| `list_files` | 8 | 118 | 157 | 0 | 0% |
| `read_file` | 1 | 1498 | 1498 | 0 | 0% |

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 292 | 5 | 12 |  |  |
| 02 | pass | 1 | 1 | 488 | 6 | 13 |  |  |
| 03 | fail | 1 | 1 | 150 | 2 | 316 |  | result set differs from reference |
| 04 | pass | 1 | 1 | 288 | 4 | 9 |  |  |
| 05 | pass | 2 | 2 | 418 | 6 | 11 |  |  |
| 06 | pass | 1 | 1 | 212 | 3 | 8 |  |  |
| 07 | pass | 2 | 2 | 288 | 4 | 3 |  |  |
| 08 | pass | 1 | 1 | 401 | 5 | 2 |  |  |
| 09 | pass | 1 | 1 | 342 | 2 | 3 |  |  |
| 10 | pass | 1 | 1 | 43 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 41 | 1 | 1 |  |  |

- Candidate latency: 39ms total vs reference 379ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Saved answer_2200255695.sql: SELECT COUNT(*) AS claim_count FROM Claim; — validated, returns 2 claims.
