# ACME Insurance Agent Eval — 20260911-132452

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 152.2s  |  Agent time: 487.0s  |  Avg/query: 44.3s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 88
- Tool calls: 157  →  {'trilogy': 146, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 22, 'file': 43, 'explore': 80, 'run': 1}
- `agent-info` directory follow-through: 7/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 7}
- Tool success rate: 98% (154/157 ok)
- Tokens: 883691 total (836422 prompt + 47269 completion)
- Cache: 728576 prompt tokens served from cache (87%)  |  cache-adjusted cost: 227973 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 146 | 2122 | 16829 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'trilogy': 2} — total 2

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 503 | 5 | 3 |  |  |
| 02 | pass | 1 | 1 | 886 | 15 | 4 |  |  |
| 03 | pass | 1 | 1 | 947 | 9 | 3 |  |  |
| 04 | pass | 1 | 1 | 376 | 5 | 2 |  |  |
| 05 | pass | 2 | 2 | 1559 | 12 | 2 |  |  |
| 06 | pass | 1 | 1 | 302 | 2 | 2 |  |  |
| 07 | pass | 2 | 2 | 525 | 5 | 3 |  |  |
| 08 | pass | 1 | 1 | 949 | 5 | 3 |  |  |
| 09 | pass | 1 | 1 | 981 | 5 | 3 |  |  |
| 10 | pass | 1 | 1 | 100 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 94 | 1 | 1 |  |  |

- Candidate latency: 65ms total vs reference 27ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.preql which counts the total number of claims via the Claim model's key:
> 
>     import root.Claim as c;
>     select count(c.claim_identifier) as total_claims;
> 
> Result (single row, single column): total_claims = 2.
