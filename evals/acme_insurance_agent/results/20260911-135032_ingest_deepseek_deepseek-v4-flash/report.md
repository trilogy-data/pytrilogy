# ACME Insurance Agent Eval — 20260911-135033

- Model: `deepseek/deepseek-v4-flash`
- Scale factor: 1  |  Queries: 11  |  Trilogy: 0.3.353

## Result

- **Pass rate: 11/11** (100%)
- Status breakdown: {'pass': 11}
- Wall time: 87.5s  |  Agent time: 322.1s  |  Avg/query: 29.3s
- Agent exit code: 0

## Agent harness metrics

- LLM iterations: 73
- Tool calls: 113  →  {'trilogy': 102, 'return_control_to_user': 11}
- `trilogy` subcommands: {'agent-info': 26, 'file': 37, 'explore': 39}
- `agent-info` directory follow-through: 7/11 directory calls immediately followed by a drilldown
- `agent-info` transitions: {'index -> query': 7, 'syntax example scoped-join -> syntax example query-structure': 1, 'syntax example query-structure -> syntax example filtered-aggregate': 1}
- Tool success rate: 100% (113/113 ok)
- Tokens: 609261 total (580595 prompt + 28666 completion)
- Cache: 487036 prompt tokens served from cache (84%)  |  cache-adjusted cost: 170929 token-equivalents

## Tool-output sizes

| Tool | calls | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| `trilogy` | 102 | 2801 | 14449 | 0 | 0% |
| `return_control_to_user` | 11 | 26 | 26 | 0 | 0% |

- Repeated calls (same args seen earlier in same query): {'trilogy': 1} — total 1

## Per-query

| Query | Status | Ref rows | Cand rows | SQL len | Cand ms | Ref ms | Agg | Detail |
|------:|:-------|---------:|----------:|--------:|--------:|--------:|:---|:-------|
| 01 | pass | 2 | 2 | 496 | 5 | 4 |  |  |
| 02 | pass | 1 | 1 | 1254 | 7 | 5 |  |  |
| 03 | pass | 1 | 1 | 1292 | 7 | 3 |  |  |
| 04 | pass | 1 | 1 | 407 | 2 | 1 |  |  |
| 05 | pass | 2 | 2 | 667 | 5 | 2 |  |  |
| 06 | pass | 1 | 1 | 471 | 3 | 1 |  |  |
| 07 | pass | 2 | 2 | 558 | 5 | 3 |  |  |
| 08 | pass | 1 | 1 | 949 | 6 | 4 |  |  |
| 09 | pass | 1 | 1 | 985 | 6 | 3 |  |  |
| 10 | pass | 1 | 1 | 98 | 1 | 1 |  |  |
| 11 | pass | 1 | 1 | 93 | 1 | 1 |  |  |

- Candidate latency: 48ms total vs reference 28ms (11 queries timed)  |  aggregate-table candidates: 0/11

## Agent final message

> Wrote answer_2200255695.preql: `select count(c.claim_identifier) as claim_count;` over `root.Claim`. Runs cleanly and returns one row with the total number of claims (2).
