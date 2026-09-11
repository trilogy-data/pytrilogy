# ACME Insurance Agent Eval — 20260911-001859

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 9/11** (82%)
- Status breakdown: {'pass': 9, 'fail': 2}
- Wall time: 407.7s  |  Agent time: 729.0s  |  Avg/query: 66.3s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 118
- Tool calls: 233  →  {'trilogy': 222, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 35, 'file': 79, 'explore': 91, 'database': 17}
- `agent-info` directory follow-through: 7/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'syntax example scoped-join -> syntax example existence-anti-join': 1, 'index -> query': 7, 'syntax example scoped-join -> syntax example query-structure': 1}
- Tool success rate: 82% (190/233 ok)
- Tokens: 1574419 total (1477973 prompt + 96446 completion)
- Cache: 1348224 prompt tokens served from cache (91%)  |  cache-adjusted cost: 361017 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 222 | 1655 | 16829 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'trilogy': 4} — total 4

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 802 | 7 | 3 |  |  |
| 02 | pass | 1 | 1 | 1424 | 6 | 3 |  |  |
| 03 | fail | 1 | 1 | 293 | 2 | 3 |  | result set differs from reference |
| 04 | pass | 1 | 1 | 328 | 4 | 2 |  |  |
| 05 | pass | 2 | 2 | 2921 | 19 | 2 |  |  |
| 06 | pass | 1 | 1 | 335 | 2 | 2 |  |  |
| 07 | pass | 2 | 2 | 774 | 7 | 3 |  |  |
| 08 | pass | 1 | 1 | 576 | 4 | 3 |  |  |
| 09 | fail | 1 | 1 | 1511 | 4 | 3 |  | result set differs from reference |
| 10 | pass | 1 | 1 | 98 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 93 | 1 | 1 |  |  |

- Candidate latency: 57ms total vs reference 26ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.preql:
> 
> ```
> import root.Claim as c;
> 
> select count(c.claim_identifier) as claim_count;
> ```
> 
> It validates and executes cleanly, returning claim_count = 2.
