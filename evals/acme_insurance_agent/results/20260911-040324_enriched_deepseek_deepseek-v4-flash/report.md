# ACME Insurance Agent Eval — 20260911-040324

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 290.9s  |  Agent time: 261.4s  |  Avg/query: 23.8s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 80
- Tool calls: 95  →  {'trilogy': 84, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 22, 'file': 43, 'explore': 19}
- `agent-info` directory follow-through: 7/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 7}
- Tool success rate: 97% (92/95 ok)
- Tokens: 627040 total (607969 prompt + 19071 completion)
- Cache: 517376 prompt tokens served from cache (85%)  |  cache-adjusted cost: 161402 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 84 | 3278 | 16829 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'trilogy': 4} — total 4

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 828 | 5 | 3 |  |  |
| 02 | pass | 1 | 1 | 943 | 7 | 3 |  |  |
| 03 | pass | 1 | 1 | 1026 | 4 | 3 |  |  |
| 04 | pass | 1 | 1 | 367 | 2 | 2 |  |  |
| 05 | pass | 2 | 2 | 1531 | 21 | 2 |  |  |
| 06 | pass | 1 | 1 | 319 | 2 | 2 |  |  |
| 07 | pass | 2 | 2 | 709 | 5 | 3 |  |  |
| 08 | pass | 1 | 1 | 731 | 4 | 3 |  |  |
| 09 | pass | 1 | 1 | 715 | 4 | 3 |  |  |
| 10 | pass | 1 | 1 | 108 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 102 | 1 | 1 |  |  |

- Candidate latency: 56ms total vs reference 26ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote and validated answer_2200255695.preql. It imports raw.claim and selects the claim_count metric, yielding one row with the total number of claims: 2.
