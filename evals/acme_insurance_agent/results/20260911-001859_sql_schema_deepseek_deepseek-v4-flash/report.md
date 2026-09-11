# ACME Insurance Agent Eval — 20260911-001859

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 10/11** (91%)
- Status breakdown: {'pass': 10, 'fail': 1}
- Wall time: 102.8s  |  Agent time: 175.8s  |  Avg/query: 16.0s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 77
- Tool calls: 134  →  {'read_file': 11, 'list_files': 10, 'run_query': 80, 'write_file': 11, 'run_file': 11, 'return_control_to_user': 11}
- `trilogy` subcommands: {}
- Tool success rate: 98% (132/134 ok)
- Tokens: 382347 total (366547 prompt + 15800 completion)
- Cache: 313472 prompt tokens served from cache (86%)  |  cache-adjusted cost: 100222 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `run_query` | 80 | 458 | 1212 | 0 | 0% |
| `read_file` | 11 | 6388 | 6388 | 0 | 0% |
| `write_file` | 11 | 54 | 54 | 0 | 0% |
| `run_file` | 11 | 295 | 338 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |
| `list_files` | 10 | 151 | 151 | 0 | 0% |

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 283 | 4 | 3 |  |  |
| 02 | pass | 1 | 1 | 519 | 5 | 3 |  |  |
| 03 | fail | 1 | 1 | 152 | 2 | 3 |  | result set differs from reference |
| 04 | pass | 1 | 1 | 288 | 5 | 2 |  |  |
| 05 | pass | 2 | 2 | 432 | 8 | 4 |  |  |
| 06 | pass | 1 | 1 | 236 | 4 | 2 |  |  |
| 07 | pass | 2 | 2 | 292 | 4 | 2 |  |  |
| 08 | pass | 1 | 1 | 426 | 5 | 3 |  |  |
| 09 | pass | 1 | 1 | 180 | 3 | 4 |  |  |
| 10 | pass | 1 | 1 | 43 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 41 | 1 | 1 |  |  |

- Candidate latency: 42ms total vs reference 28ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Answer saved to answer_2200255695.sql: SELECT COUNT(*) FROM Claim → 2 claims. Validated with run_file, returns one row.
