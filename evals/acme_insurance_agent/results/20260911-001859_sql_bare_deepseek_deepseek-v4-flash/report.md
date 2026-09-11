# ACME Insurance Agent Eval — 20260911-001859

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 10/11** (91%)
- Status breakdown: {'pass': 10, 'fail': 1}
- Wall time: 178.9s  |  Agent time: 329.5s  |  Avg/query: 30.0s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 102
- Tool calls: 206  →  {'run_query': 160, 'list_files': 10, 'write_file': 11, 'run_file': 11, 'return_control_to_user': 11, 'read_file': 3}
- `trilogy` subcommands: {}
- Tool success rate: 99% (204/206 ok)
- Tokens: 696972 total (654684 prompt + 42288 completion)
- Cache: 593408 prompt tokens served from cache (91%)  |  cache-adjusted cost: 162905 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `run_query` | 160 | 591 | 3076 | 0 | 0% |
| `write_file` | 11 | 54 | 54 | 0 | 0% |
| `run_file` | 11 | 295 | 338 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |
| `list_files` | 10 | 116 | 157 | 0 | 0% |
| `read_file` | 3 | 1498 | 1498 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'run_query': 1, 'list_files': 1} — total 2

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 297 | 5 | 3 |  |  |
| 02 | pass | 1 | 1 | 432 | 5 | 3 |  |  |
| 03 | fail | 1 | 1 | 150 | 3 | 3 |  | result set differs from reference |
| 04 | pass | 1 | 1 | 293 | 4 | 1 |  |  |
| 05 | pass | 2 | 2 | 695 | 5 | 2 |  |  |
| 06 | pass | 1 | 1 | 211 | 4 | 2 |  |  |
| 07 | pass | 2 | 2 | 286 | 5 | 4 |  |  |
| 08 | pass | 1 | 1 | 396 | 6 | 3 |  |  |
| 09 | pass | 1 | 1 | 321 | 4 | 3 |  |  |
| 10 | pass | 1 | 1 | 43 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 41 | 1 | 1 |  |  |

- Candidate latency: 43ms total vs reference 26ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.sql:
> 
> SELECT COUNT(*) AS claim_count FROM Claim;
> 
> Returns 1 row with value 2 (total number of claims in the Claim table).
