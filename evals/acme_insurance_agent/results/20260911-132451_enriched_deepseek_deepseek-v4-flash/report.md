# ACME Insurance Agent Eval — 20260911-132452

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 79.7s  |  Agent time: 252.6s  |  Avg/query: 23.0s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 61
- Tool calls: 79  →  {'trilogy': 68, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 22, 'file': 31, 'explore': 15}
- `agent-info` directory follow-through: 3/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 3}
- Tool success rate: 98% (77/79 ok)
- Tokens: 424663 total (413243 prompt + 11420 completion)
- Cache: 329472 prompt tokens served from cache (80%)  |  cache-adjusted cost: 128138 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 68 | 3881 | 16829 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'trilogy': 2} — total 2

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 761 | 5 | 14 |  |  |
| 02 | pass | 1 | 1 | 947 | 9 | 17 |  |  |
| 03 | pass | 1 | 1 | 1000 | 5 | 4 |  |  |
| 04 | pass | 1 | 1 | 283 | 2 | 10 |  |  |
| 05 | pass | 2 | 2 | 1531 | 9 | 3 |  |  |
| 06 | pass | 1 | 1 | 319 | 2 | 13 |  |  |
| 07 | pass | 2 | 2 | 758 | 5 | 12 |  |  |
| 08 | pass | 1 | 1 | 781 | 5 | 3 |  |  |
| 09 | pass | 1 | 1 | 745 | 4 | 29 |  |  |
| 10 | pass | 1 | 1 | 108 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 102 | 1 | 1 |  |  |

- Candidate latency: 48ms total vs reference 107ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.preql and it ran cleanly: `import raw.claim as claim; select claim.claim_count as total_claims;` returns a single row/column with the total number of claims (2).
