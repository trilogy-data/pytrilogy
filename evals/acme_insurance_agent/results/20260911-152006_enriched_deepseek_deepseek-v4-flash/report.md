# ACME Insurance Agent Eval — 20260911-152007

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 63.0s  |  Agent time: 218.6s  |  Avg/query: 19.9s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 62
- Tool calls: 75  →  {'trilogy': 64, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 21, 'file': 27, 'explore': 16}
- `agent-info` directory follow-through: 6/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 6}
- Tool success rate: 99% (74/75 ok)
- Tokens: 412331 total (402038 prompt + 10293 completion)
- Cache: 325376 prompt tokens served from cache (81%)  |  cache-adjusted cost: 119493 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 64 | 3800 | 14449 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'trilogy': 1} — total 1

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 739 | 4 | 5 |  |  |
| 02 | pass | 1 | 1 | 956 | 7 | 4 |  |  |
| 03 | pass | 1 | 1 | 2364 | 9 | 2 |  |  |
| 04 | pass | 1 | 1 | 302 | 2 | 2 |  |  |
| 05 | pass | 2 | 2 | 1244 | 5 | 2 |  |  |
| 06 | pass | 1 | 1 | 379 | 3 | 3 |  |  |
| 07 | pass | 2 | 2 | 919 | 4 | 3 |  |  |
| 08 | pass | 1 | 1 | 672 | 5 | 5 |  |  |
| 09 | pass | 1 | 1 | 2094 | 5 | 3 |  |  |
| 10 | pass | 1 | 1 | 108 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 102 | 1 | 1 |  |  |

- Candidate latency: 46ms total vs reference 31ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.preql and validated it in one call:
> 
>     import raw.claim as claim;
>     select claim.claim_count as total_claims;
> 
> Result: 1 row, 1 column → total_claims = 2. The `claim.claim_count` metric (count of claim.id) at the global grain gives the total number of claims.
