# q23 churn (run 20260629-013151, ~1.10M tokens, FAILED) — verdict: NO new framework bug

Run: `evals/tpcds_agent/results/20260629-013151`, logs `agent_log.q23.{jsonl,conversation.txt}`,
final `workspace/query23.preql`.

## TL;DR

- **No new framework defect.** The two earlier q23 framework bugs (groupby-contains-aggregate,
  hidden-`--`-aggregate INVALID_REFERENCE) stay FIXED — neither reappears in this run.
- The whole run surfaced exactly **two** framework errors, both clean/actionable and self-corrected
  on the next write: `Syntax [103]` (agent wrote `GROUP BY`) and `Syntax [102]` (agent wrote a
  SQL subquery). Every `trilogy run` after the first write returned `exit_code: 0`.
- The dominant token sink is a **SILENT, INTENDED-semantics footgun**: `count(<key>)` is
  count-DISTINCT at that key's grain. `count(st.item.id)` grouped by `st.item.id` is therefore
  **always 1**, so `having cnt > 4` yields an **empty** frequent-items set → empty final result.
  The agent spent ~records 49–91 discovering this and switching to `count(st.line_item)`.
- The known NULL-customer `max(...)` footgun was **avoided** this run (the agent's `customer_total`
  rowset already had `st.customer.id is not null`, query23.preql line 18).
- Final query runs clean and returns **19 rows** (same count as the reference), but the run is
  scored FAILED on **row-value accuracy**: the agent's frequent-items rule (`count(line_item) > 4`
  per item — nearly every item qualifies) diverges from the canonical per-`(desc_trunc, item.id,
  date)` combo count. Pure modeling choice, not a framework issue.

## Reproduction of the footgun (workspace DB, minimized)

```preql
import raw.store_sales as st;
where st.date.year between 2000 and 2003
select st.item.id as item_id,
       count(st.item.id) as cnt_key,   -- always 1 (distinct count of the group key)
       count(st.line_item) as cnt_line -- true row count (up to 234)
order by cnt_line desc limit 5;
```

Result: `cnt_key = 1` for every row; `cnt_line` ranges 223–234. Adding `having cnt_key > 4`
returns **No results** — exactly the empty `frequent_items` the agent hit at log records 64–67.

`generate_sql` confirms `count(st.item.id)` is lowered to a count over a pre-grouped (item-grain)
CTE — i.e. distinct-at-grain — not a raw row count. This is by design.

## Why this is intended, not a bug

- Model documents it: `raw/store_sales.preql:49` —
  `# ... Count line items with count(line_item); do NOT use count(ticket_number), a receipt key
  that dedups and undercounts ...`.
- Engine documents it: `trilogy/parsing/v2/errors.py:74` — `count a key field: count(<key>)
  (counts are already distinct)`.
- Semantics live in the aggregate grain handling (`count` of a concept aggregates at that
  concept's grain → distinct); rendered via the count CTE, not `count(distinct …)` literal.

So `count(<key>)` returning distinct-at-grain is consistent, documented behavior. The trap is that
it is **silent** when the key is also the group key (always 1), producing an empty set with no
error — the single biggest source of q23 churn in this run.

## Classification

| obstacle | log evidence | class |
|---|---|---|
| `count(<key>)` silent count-distinct → empty `frequent_items` | recs 49–91 | KNOWN footgun (documented), agent thrash |
| "first 30 chars of desc as part of the grouping" interpretation | recs 25–100 | agent semantic ambiguity |
| `Syntax [103]` GROUP BY, `Syntax [102]` subquery | recs 36, 77 | agent habit; clean errors, self-fixed |
| NULL-customer max threshold | — | avoided this run (already filtered) |
| final accuracy miss (19 rows, wrong frequent set) | rec 97+ | agent modeling, not framework |

## Optional, low-priority follow-up (not a fix)

An agent-info gotcha for the recurring "count of (a,b) pairs / frequent items" TPC-DS pattern:
`count(<key>) by <that same key>` is always 1; to count rows use a non-key row marker
(`count(line_item)`), and to count distinct of another dimension use `count_distinct(other.id)`.
This one trap drove the bulk of q23's iterations across multiple runs.

Canonical `tests/modeling/tpc_ds_duckdb/query23.preql` builds clean (`generate_sql` → 1 statement)
and sidesteps all of the above with explicit `count(sales.order_id) by (desc_truncated, item.id,
date)` grain.
