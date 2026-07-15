# Silent-sink audit: q71, q75, q77, q94

**Run:** `results/20260715-033011_enriched`  
**Scope:** queries above 500k tokens without an obvious framework exception

## Conclusion

No silent framework miscompilation was found in q71, q75, or q77. Their final
candidates reproduce the reference rows and run in under one second. Their token
load is driven by repeated model exploration and cumulative tool output.

q94 also does not expose a framework discrepancy. Every saved candidate was
replayed directly against the reference. The agent alternated between counting
qualifying lines globally and grouping those lines by order; it never counted
distinct qualifying orders as requested.

## Metrics

| Query | Status | Tokens | Tool calls | Tool-output chars | Final runtime |
|---|---:|---:|---:|---:|---:|
| q71 | pass | 532,595 | 20 | 127,423 | 0.25–0.75s |
| q75 | pass | 778,373 | 23 | 129,560 | 0.18s |
| q77 | pass | 523,061 | 22 | 150,827 | 0.47s |
| q94 | fail | 844,844 | 24 | 137,204 | 0.05–0.06s |

None of the tool outputs were truncated. Individual model exploration responses
reached 41,245 characters. Because the conversation is sent back on every turn,
these outputs repeatedly inflate prompt tokens even when execution is fast.

## q71

The final query is the direct model expression of the question and scores all
1,031 rows correctly. It needed only one model import, one filter, and one
aggregate.

Before writing it, the agent made 13 discovery calls, including:

- broad exploration of `all_sales` multiple times;
- separate time exploration for store, catalog, and web models;
- `--show all`, `--show datasources`, and repeated `--show concepts` calls;
- two inline execution probes.

The final query was then executed twice despite succeeding on the first run.

**Classification:** tool-use/prompt-discipline churn. No framework bug.

## q75

The final query scores correctly and executes in about 0.18 seconds. Its generated
SQL is larger than q71's because it aggregates 2001 and 2002 separately and
joins the four-key year slices, but it is not pathological.

The agent used nine exploration calls, then six write/run cycles. Four early
files were diagnostic probes of line grain and null-return behavior; the first
complete year-over-year candidate already returned 100 rows and was rewritten
and executed again.

The final four `union join` clauses compile into the intended composite
relationship. There is no q64-style repeated-parent expansion.

**Classification:** cautious agent probing plus cumulative tool context. No
framework bug.

## q77

The final candidate scores all 43 rollup rows correctly. Its six independent
sales/return aggregates, three arm-local joins, relational union, and final
rollup execute in roughly 0.47 seconds.

The agent made 13 exploration calls before writing the query, including broad
model reads followed by targeted searches for the same outlet/date concepts.
Two tool calls were rejected before execution because the model emitted invalid
JSON arguments. The first complete query then passed; the agent rewrote and ran
it once more.

The generated SQL is 14,529 characters but does not show a silent result or
performance defect. q77 was actually cheaper than the prior run (523k versus
1.12M tokens), reinforcing that the sink is agent/tool interaction rather than
the final plan.

**Classification:** exploration and tool-call serialization churn. No framework
bug.

## q94

Reference result:

```text
(32, 65144.28, -19548.52)
```

All four saved candidates were recovered from the JSONL and executed directly:

| Attempt | Rows | First/count value | Diagnosis |
|---|---:|---:|---|
| 1 | 1 | 40 | Counts qualifying lines, not distinct orders |
| 2 | 32 | order id + line count | Groups by order instead of overall report |
| 3 | 1 | 40 | Again counts qualifying lines |
| 4/final | 32 | per-order line count | Hidden order grain remains in final query |

The final file contains:

```trilogy
count(grain(candidate_lines.order_number,
            candidate_lines.ext_ship_cost,
            candidate_lines.net_profit)) as order_count,
--candidate_lines.order_number as order_num
```

The commented projection is an intentional hidden-grain field in Trilogy. It
keeps the result at order grain and therefore produces 32 rows. Removing it
produces one row, but the expression still returns 40 because it counts candidate
lines. The correct aggregate is a distinct count of order number, which returns
32.

The framework consistently executes each authored grain. The agent's own run
outputs clearly exposed both 40-line and 32-row shapes, but it inferred that the
32 per-order rows represented success.

**Classification:** semantic/grain interpretation failure. No framework bug.

## Cross-query driver

These queries share a tool-economics problem:

1. `agent-info` provides a large baseline context.
2. Broad `explore` results can add ~41k characters each.
3. Agents often request `show all`, concepts, datasources, and then narrower
   regex searches over the same model.
4. Successful execution does not reliably stop another rewrite/re-run cycle.
5. Every earlier output remains in subsequent model prompts.

The result is high token usage without slow SQL or framework errors.

## Recommended changes

1. Add a short prompt rule: after one broad model exploration, use targeted
   regex exploration only; do not request `show all` plus concepts plus
   datasources for the same model.
2. Cache or suppress duplicate exploration payloads in the conversation,
   replacing repeats with a short “same result as call N” reference.
3. Reduce default exploration payloads or provide a compact concept-index mode;
   41k-character tool results are disproportionate for single-query tasks.
4. Stop after a successful run when the output grain and headline values match
   the question; require a stated discrepancy before another rewrite.
5. Add explicit guidance that “order count” means distinct orders when multiple
   candidate lines may share an order, and that hidden fields still determine
   output grain.

