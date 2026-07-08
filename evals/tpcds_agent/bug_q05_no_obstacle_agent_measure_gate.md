# q05 token sink (930k) — NO framework obstacle; agent measure-gating correctness error

Run: `evals/tpcds_agent/results/20260708-135136_enriched` — q05 `fail`, 930,524 tokens,
31 iterations, 33 tool calls (13 `run`, 9 `explore`, 9 `file`), 7 tool_errors,
duration 191.6s.

## Verdict
**NO framework obstacle.** The 375s return-only-anchor perf bug (commit 3661e4243,
`project_q05_return_only_anchor_elision`) is FIXED and working: the agent's unified
`raw.all_sales` query plans and runs fast. The token sink is (a) agent authoring
iteration (7 self-inflicted syntax errors + model exploration re-sending a growing
918k-prompt-token context across 31 turns) and (b) the agent shipping a subtly wrong
query that it never detected. Classification: **AGENT correctness + agent-iteration
token burn.** Not residual-perf, not new-framework, not float32 drift.

## Timing (proof it is not a slow plan)
Agent's final `workspace/query05.preql` via scoring engine:
- `generate_sql`: 0.16s
- `execute`: 0.41s → 100 rows
- Every intermediate agent tool_call→tool_result gap in `agent_log.q05.jsonl` ≤ 2.4s
  (max), ~0.6–1s typical. No arm is slow. The 191.6s wall time is LLM latency across
  31 turns, not query execution. tool_output_stats total 95KB — outputs are tiny; the
  918k prompt tokens are cumulative context re-sent per turn (~30k × 31).

## The 7 errors are all agent authoring mistakes (helpful messages, not framework bugs)
- "3 undefined concept references" — agent typo'd `all_sales.channel` etc.
- "Cannot use BETWEEN with incompatible types DATE and STRING" — agent used untyped
  string literals instead of `::date`.
- "2 undefined concept references" — `s.date.date` (wrong namespace).
- ×2 "SELECT output 'local.net_profit' … recursive self-reference" — agent aliased
  `... as net_profit` reading `net_profit`.

## Wrong-result root cause (why status=fail) — AGENT, not engine
The final result diverges from the reference ONLY in the STORE channel; catalog and web
match to the cent:

| channel | agent sales | ref sales | diff |
|---|---|---|---|
| GRAND   | 112,460,149.50 | 112,458,734.70 | **+1,414.80** |
| store   | 54,275,046.91  | 54,273,632.11  | **+1,414.80** (returns +365.10, profit −5,331.75) |
| catalog | exact match | | |
| web     | exact match | | |

+0.0026%, but structural (not float drift), so it fails the 9-decimal scoring tolerance.

The agent gated each measure by the date window ONLY:
`sum(ext_sales_price ? (date.date between '2000-08-23' and '2000-09-06'))`
(agent `query05.preql` lines 27–29). It OMITTED the "own dim present" predicate. The
reference q05 enforces `store_sk = s_store_sk` as a per-arm INNER join, dropping
store_sales rows whose `ss_store_sk` is NULL. Those null-store rows survive the agent's
WHERE via the `OR (return_date … and return_channel_dim_id is not null)` branch (same
`(item.sk, order_id, channel)` grain carrying a valid return), so their sale amount is
counted — inflating store sales.

The canonical `tests/modeling/tpc_ds_duckdb/query05.preql` gets this right: its
`@windowed` macro includes `and id_field is not null` (lines 28–32), mirroring the
reference inner joins.

### A/B proof (single toggle)
Adding `and channel_dim_id is not null` to the sales/net_profit measures and
`and return_channel_dim_id is not null` to the return measures reproduces the reference
EXACTLY: GRAND `112458734.70 / 3255243.12 / -31584085.44`, store `54273632.11 /
1552466.33 / -24696015.40`. No other change needed.

## Trigger matrix
| variant | store sales | matches ref? |
|---|---|---|
| agent query05.preql (date-only measure gate) | 54,275,046.91 | NO (+1414.80) |
| + `dim is not null` on each measure | 54,273,632.11 | YES |
| reference query05.sql | 54,273,632.11 | — |
| canonical query05.preql (`@windowed` macro w/ id gate) | matches ref (passing test) | YES |

## Files
- Agent query: `evals/tpcds_agent/results/20260708-135136_enriched/workspace/query05.preql`
  (measures lines 27–29 lack the dim-null gate)
- Model: `.../workspace/raw/all_sales.preql` (store `channel_dim_text_id` from S_STORE_SK→S_STORE_ID join, lines 159–164)
- Canonical (correct): `tests/modeling/tpc_ds_duckdb/query05.preql` lines 28–44
- Prior perf fix (confirmed working): `node_generators/select_helpers/source_scoring.py`
  `membership_complete_grain_keys` (commit 3661e4243)

## Recommendation
No engine change. This is a correctness-track / agent-guidance item: steer agents to gate
each channel measure by "its own dim present" (mirroring the reference inner joins), as
the canonical `@windowed` macro does. The 930k token burn is agent iteration on a
fast-planning query, not a framework sink.
