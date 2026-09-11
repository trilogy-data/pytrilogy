# ACME Insurance Agent Eval — 20260911-132452

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 70.7s  |  Agent time: 230.6s  |  Avg/query: 21.0s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 64
- Tool calls: 99  →  {'read_file': 11, 'list_files': 9, 'run_query': 46, 'write_file': 11, 'run_file': 11, 'return_control_to_user': 11}
- `trilogy` subcommands: {}
- Tool success rate: 98% (97/99 ok)
- Tokens: 278322 total (266671 prompt + 11651 completion)
- Cache: 223616 prompt tokens served from cache (84%)  |  cache-adjusted cost: 77068 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `run_query` | 46 | 508 | 1345 | 0 | 0% |
| `read_file` | 11 | 6388 | 6388 | 0 | 0% |
| `write_file` | 11 | 54 | 54 | 0 | 0% |
| `run_file` | 11 | 298 | 338 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |
| `list_files` | 9 | 151 | 151 | 0 | 0% |

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 299 | 5 | 4 |  |  |
| 02 | pass | 1 | 1 | 503 | 8 | 4 |  |  |
| 03 | pass | 1 | 1 | 400 | 3 | 10 |  |  |
| 04 | pass | 1 | 1 | 305 | 5 | 2 |  |  |
| 05 | pass | 2 | 2 | 597 | 8 | 13 |  |  |
| 06 | pass | 1 | 1 | 294 | 5 | 2 |  |  |
| 07 | pass | 2 | 2 | 276 | 5 | 2 |  |  |
| 08 | pass | 1 | 1 | 445 | 8 | 12 |  |  |
| 09 | pass | 1 | 1 | 189 | 4 | 4 |  |  |
| 10 | pass | 1 | 1 | 43 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 42 | 1 | 20 |  |  |

- Candidate latency: 53ms total vs reference 74ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Created answer_2200255695.sql: SELECT COUNT(*) AS total_claims FROM Claim; — validated via run_file, returns one row/one column with value 2.
