# ACME Insurance Agent Eval — 20260911-135033

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 67.3s  |  Agent time: 213.5s  |  Avg/query: 19.4s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 63
- Tool calls: 80  →  {'trilogy': 69, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 23, 'file': 28, 'explore': 18}
- `agent-info` directory follow-through: 5/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 5, 'query -> syntax': 1}
- Tool success rate: 99% (79/80 ok)
- Tokens: 426820 total (414704 prompt + 12116 completion)
- Cache: 337152 prompt tokens served from cache (81%)  |  cache-adjusted cost: 123383 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 69 | 3562 | 14449 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 704 | 5 | 4 |  |  |
| 02 | pass | 1 | 1 | 966 | 5 | 4 |  |  |
| 03 | pass | 1 | 1 | 989 | 4 | 280 |  |  |
| 04 | pass | 1 | 1 | 361 | 2 | 2 |  |  |
| 05 | pass | 2 | 2 | 1546 | 9 | 3 |  |  |
| 06 | pass | 1 | 1 | 292 | 2 | 2 |  |  |
| 07 | pass | 2 | 2 | 709 | 6 | 4 |  |  |
| 08 | pass | 1 | 1 | 705 | 4 | 3 |  |  |
| 09 | pass | 1 | 1 | 721 | 4 | 3 |  |  |
| 10 | pass | 1 | 1 | 108 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 101 | 1 | 1 |  |  |

- Candidate latency: 43ms total vs reference 307ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.preql (imports raw.claim and selects the built-in claim_count metric). It runs cleanly and returns one row with one column: claim_count = 2.
