# ACME Insurance Agent Eval — 20260911-011644

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 8/11** (73%)
- Status breakdown: {'pass': 8, 'fail': 3}
- Wall time: 432.1s  |  Agent time: 740.2s  |  Avg/query: 67.3s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 110
- Tool calls: 203  →  {'trilogy': 192, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 24, 'file': 63, 'explore': 102, 'run': 3}
- `agent-info` directory follow-through: 8/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 8}
- Tool success rate: 98% (200/203 ok)
- Tokens: 1640253 total (1540501 prompt + 99752 completion)
- Cache: 1409536 prompt tokens served from cache (91%)  |  cache-adjusted cost: 371671 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 192 | 1843 | 16829 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 491 | 4 | 3 |  |  |
| 02 | fail | 1 | 2 | 1122 | 5 | 3 |  | result set differs from reference |
| 03 | fail | 1 | 1 | 288 | 2 | 3 |  | result set differs from reference |
| 04 | pass | 1 | 1 | 424 | 3 | 1 |  |  |
| 05 | pass | 2 | 2 | 665 | 4 | 2 |  |  |
| 06 | pass | 1 | 1 | 335 | 2 | 2 |  |  |
| 07 | pass | 2 | 2 | 526 | 5 | 3 |  |  |
| 08 | fail | 1 | 2 | 707 | 5 | 3 |  | result set differs from reference |
| 09 | pass | 1 | 1 | 958 | 4 | 8 |  |  |
| 10 | pass | 1 | 1 | 98 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 93 | 1 | 1 |  |  |

- Candidate latency: 36ms total vs reference 30ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.preql and validated it with `--run`. It imports `root.Claim` and counts the distinct claim key:
> 
>     import root.Claim as c;
>     select count(c.claim_identifier) as claim_count;
> 
> Result: 1 row, claim_count = 2 (2 distinct claims).
