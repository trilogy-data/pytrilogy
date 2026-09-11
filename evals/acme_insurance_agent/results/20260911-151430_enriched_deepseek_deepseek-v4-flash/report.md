# ACME Insurance Agent Eval — 20260911-151431

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 80.9s  |  Agent time: 256.1s  |  Avg/query: 23.3s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 66
- Tool calls: 79  →  {'trilogy': 68, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 22, 'file': 30, 'explore': 16}
- `agent-info` directory follow-through: 9/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 9}
- Tool success rate: 96% (76/79 ok)
- Tokens: 467790 total (456478 prompt + 11312 completion)
- Cache: 372992 prompt tokens served from cache (82%)  |  cache-adjusted cost: 132097 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 68 | 3896 | 14449 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'trilogy': 3} — total 3

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 976 | 6 | 5 |  |  |
| 02 | pass | 1 | 1 | 921 | 6 | 4 |  |  |
| 03 | pass | 1 | 1 | 1224 | 12 | 4 |  |  |
| 04 | pass | 1 | 1 | 337 | 2 | 2 |  |  |
| 05 | pass | 2 | 2 | 2578 | 10 | 3 |  |  |
| 06 | pass | 1 | 1 | 305 | 10 | 4 |  |  |
| 07 | pass | 2 | 2 | 914 | 6 | 4 |  |  |
| 08 | pass | 1 | 1 | 746 | 6 | 4 |  |  |
| 09 | pass | 1 | 1 | 2094 | 7 | 5 |  |  |
| 10 | pass | 1 | 1 | 98 | 2 | 1 |  |  |
| 11 | pass | 1 | 1 | 106 | 2 | 1 |  |  |

- Candidate latency: 69ms total vs reference 37ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.preql:
> 
>     import raw.claim as claim;
>     select claim.claim_count as number_of_claims;
> 
> It uses the built-in `claim_count` metric (count of claim.id) and returns one row with a single column: 2 total claims.
