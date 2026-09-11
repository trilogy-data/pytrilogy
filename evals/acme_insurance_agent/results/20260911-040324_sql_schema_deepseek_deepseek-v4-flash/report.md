# ACME Insurance Agent Eval — 20260911-040324

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 180.2s  |  Agent time: 152.2s  |  Avg/query: 13.8s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 71
- Tool calls: 116  →  {'read_file': 11, 'list_files': 11, 'run_query': 61, 'write_file': 11, 'run_file': 11, 'return_control_to_user': 11}
- `trilogy` subcommands: {}
- Tool success rate: 99% (115/116 ok)
- Tokens: 318318 total (306041 prompt + 12277 completion)
- Cache: 260992 prompt tokens served from cache (85%)  |  cache-adjusted cost: 83425 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `run_query` | 61 | 449 | 1345 | 0 | 0% |
| `read_file` | 11 | 6388 | 6388 | 0 | 0% |
| `list_files` | 11 | 151 | 151 | 0 | 0% |
| `write_file` | 11 | 54 | 54 | 0 | 0% |
| `run_file` | 11 | 299 | 338 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 415 | 4 | 3 |  |  |
| 02 | pass | 1 | 1 | 478 | 11 | 3 |  |  |
| 03 | pass | 1 | 1 | 437 | 3 | 2 |  |  |
| 04 | pass | 1 | 1 | 289 | 2 | 1 |  |  |
| 05 | pass | 2 | 2 | 409 | 9 | 2 |  |  |
| 06 | pass | 1 | 1 | 316 | 4 | 1 |  |  |
| 07 | pass | 2 | 2 | 292 | 4 | 2 |  |  |
| 08 | pass | 1 | 1 | 442 | 5 | 3 |  |  |
| 09 | pass | 1 | 1 | 234 | 3 | 3 |  |  |
| 10 | pass | 1 | 1 | 45 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 46 | 1 | 1 |  |  |

- Candidate latency: 47ms total vs reference 22ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Created answer_2200255695.sql: `SELECT COUNT(*) AS number_of_claims FROM Claim`, which returns a single row/column with the total claim count (2). Validated successfully with run_file.
