# ACME Insurance Agent Eval — 20260911-040324

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 689.9s  |  Agent time: 662.2s  |  Avg/query: 60.2s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 104
- Tool calls: 202  →  {'trilogy': 191, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 24, 'file': 72, 'explore': 95}
- `agent-info` directory follow-through: 7/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 7}
- Tool success rate: 92% (186/202 ok)
- Tokens: 1310282 total (1225120 prompt + 85162 completion)
- Cache: 1102976 prompt tokens served from cache (90%)  |  cache-adjusted cost: 317604 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 191 | 1813 | 16829 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'trilogy': 3} — total 3

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 520 | 4 | 3 |  |  |
| 02 | pass | 1 | 1 | 1254 | 5 | 3 |  |  |
| 03 | pass | 1 | 1 | 553 | 3 | 2 |  |  |
| 04 | pass | 1 | 1 | 367 | 2 | 2 |  |  |
| 05 | pass | 2 | 2 | 2608 | 5 | 3 |  |  |
| 06 | pass | 1 | 1 | 311 | 4 | 2 |  |  |
| 07 | pass | 2 | 2 | 494 | 4 | 2 |  |  |
| 08 | pass | 1 | 1 | 949 | 4 | 3 |  |  |
| 09 | pass | 1 | 1 | 985 | 5 | 3 |  |  |
| 10 | pass | 1 | 1 | 98 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 101 | 1 | 1 |  |  |

- Candidate latency: 38ms total vs reference 25ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.preql:
> 
> ```
> import root.Claim as claim;
> 
> select count(claim.claim_identifier) as claim_count;
> ```
> 
> Result: 1 row, claim_count = 2 (total claims), matching the Claim model's grain (claim_identifier) whose domain is {1, 2}.
