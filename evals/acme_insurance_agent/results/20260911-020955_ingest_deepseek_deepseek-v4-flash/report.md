# ACME Insurance Agent Eval — 20260911-020955

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 8/11** (73%)
- Status breakdown: {'pass': 8, 'fail': 3}
- Wall time: 1074.1s  |  Agent time: 1039.4s  |  Avg/query: 94.5s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 115
- Tool calls: 228  →  {'trilogy': 217, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 28, 'file': 88, 'explore': 99, 'run': 2}
- `agent-info` directory follow-through: 5/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 5, 'syntax example query-structure -> syntax example scoped-join': 1}
- Tool success rate: 94% (214/228 ok)
- Tokens: 2215813 total (2065933 prompt + 149880 completion)
- Cache: 1929984 prompt tokens served from cache (93%)  |  cache-adjusted cost: 478827 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 217 | 1771 | 16829 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'trilogy': 8} — total 8

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 529 | 4 | 14 |  |  |
| 02 | pass | 1 | 1 | 1220 | 4 | 20 |  |  |
| 03 | fail | 1 | 1 | 289 | 2 | 8 |  | result set differs from reference |
| 04 | pass | 1 | 1 | 290 | 2 | 9 |  |  |
| 05 | pass | 2 | 2 | 741 | 6 | 10 |  |  |
| 06 | pass | 1 | 1 | 440 | 3 | 8 |  |  |
| 07 | pass | 2 | 2 | 521 | 5 | 10 |  |  |
| 08 | fail | 1 | 2 | 707 | 5 | 11 |  | result set differs from reference |
| 09 | fail | 1 | 1 | 1137 | 4 | 12 |  | result set differs from reference |
| 10 | pass | 1 | 1 | 98 | 1 | 7 |  |  |
| 11 | pass | 1 | 1 | 93 | 1 | 1 |  |  |

- Candidate latency: 37ms total vs reference 110ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.preql: `import root.Claim as c; select count(c.claim_identifier) as claim_count;`. It runs cleanly and returns 1 row: 2 claims (Claim grain = claim_identifier).
