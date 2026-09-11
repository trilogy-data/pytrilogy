# ACME Insurance Agent Eval — 20260911-001859

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 148.6s  |  Agent time: 271.2s  |  Avg/query: 24.7s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 82
- Tool calls: 100  →  {'trilogy': 89, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 23, 'file': 45, 'explore': 19, 'database': 1, 'run': 1}
- `agent-info` directory follow-through: 8/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 8}
- Tool success rate: 95% (95/100 ok)
- Tokens: 648574 total (629671 prompt + 18903 completion)
- Cache: 535808 prompt tokens served from cache (85%)  |  cache-adjusted cost: 166347 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 89 | 3184 | 16829 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'trilogy': 4} — total 4

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 761 | 6 | 3 |  |  |
| 02 | pass | 1 | 1 | 1012 | 5 | 4 |  |  |
| 03 | pass | 1 | 1 | 1061 | 4 | 2 |  |  |
| 04 | pass | 1 | 1 | 361 | 2 | 2 |  |  |
| 05 | pass | 2 | 2 | 1821 | 9 | 2 |  |  |
| 06 | pass | 1 | 1 | 347 | 2 | 1 |  |  |
| 07 | pass | 2 | 2 | 762 | 4 | 2 |  |  |
| 08 | pass | 1 | 1 | 706 | 5 | 4 |  |  |
| 09 | pass | 1 | 1 | 715 | 3 | 3 |  |  |
| 10 | pass | 1 | 1 | 108 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 102 | 1 | 1 |  |  |

- Candidate latency: 42ms total vs reference 25ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote and validated `answer_2200255695.preql`:
> 
> ```
> import raw.claim as claim;
> 
> select claim.claim_count as total_claims;
> ```
> 
> Result: 2 claims (verified independently with `count(claim.id)` and `count_distinct(claim.claim_number)`, both = 2).
