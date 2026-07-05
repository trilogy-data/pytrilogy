# q64 (run 20260705-200535) — 1.4M-token FAIL is agent column choice, NOT a framework bug

## Symptom
q64 burned 1,387,762 tokens / 36 API calls and scored **FAIL**. The agent's final
`query64.preql` **builds and executes cleanly**, returns exactly 2 rows with the
**correct dimensions, addresses, years, and counts** — but two of the six measure
columns are wrong, so the multiset differs from the reference.

The `Syntax [224]: Using SELECT DISTINCT?` in the log is a red herring: it fired once
on an ad-hoc color-probe query (`select distinct lower(i.color) ...`, msg 72), the
message was clear and actionable, and the agent immediately rewrote without `distinct`
(msg 74) and got results. It did not drive the token sink.

## Minimal repro / proof
Candidate row 1 sums vs reference row 1 sums (dimensions identical, `cnt=1`, `coupon` identical):

| col | candidate | reference | ratio |
|-----|-----------|-----------|-------|
| wholesale_sum_1999 | 1904.80 | 95.24 | 20x |
| list_sum_1999      | 2819.00 | 140.95 | 20x |
| coupon_sum_1999    | 1488.38 | 1488.38 | 1x (match) |
| wholesale_sum_2000 | 317.34 | 52.89 | 6x |
| list_sum_2000      | 583.86 | 97.31 | 6x |

Exact integer ratios == `ss_quantity` of the single line in each group. The agent used
the **line-extended** columns `ss.ext_wholesale_cost` / `ss.ext_list_price`
(`= quantity * per-unit`), whereas the task ("sum **wholesale cost**, **list price**,
and coupon amount") and canonical `query64.sql` (`sum(ss_wholesale_cost)`,
`sum(ss_list_price)`) use the **per-unit** columns. `coupon_amt` has no `ext_` variant,
so it matched.

A/B (single toggle): patch candidate `ss.ext_wholesale_cost -> ss.wholesale_cost` and
`ss.ext_list_price -> ss.list_price`, change nothing else, re-run against the same
workspace DB → **result set is byte-identical to the reference** (`PATCHED == REF: True`).
This isolates the entire failure to those two column names; every join, filter, rowset,
subset/union self-pair, and grouping the engine produced is correct.

## Trigger matrix
| variant | result |
|---------|--------|
| candidate as-authored (ext_ cols) | 2 rows, wholesale/list inflated by quantity → FAIL |
| candidate with per-unit cols (only change) | matches reference exactly → PASS |
| reference `query64.sql` | 2 rows (the oracle) |
| canonical `tests/.../query64.preql` | `generate_sql` compiles fine on current engine (execute against the *workspace* DB only fails on a `raw.item` vs `item` schema-name mismatch, an env artifact, not an engine defect) |

## Root cause
Agent semantic column selection: `evals/.../workspace/query64.preql` lines 54–55 & 83–84
select `sum(ss.ext_wholesale_cost)` / `sum(ss.ext_list_price)` instead of the per-unit
`ss.wholesale_cost` / `ss.list_price` named by the task. Model docs
(`workspace/raw/store_sales.preql:30` "Per-unit list price", `:33` "Line-extended list
price", `:40` "Per-unit wholesale acquisition cost") clearly distinguish the two; the
agent picked the extended pair.

No framework file is at fault. The `Syntax [224]` DISTINCT guard is deliberate,
correct guidance (`trilogy/parsing/v2/errors.py:86`, raised in `lark_backend.py:164` /
`pest_backend.py:294`) and worked as designed.

## Token-sink explanation (no framework obstacle)
The 7 logged errors were all the agent's own Trilogy-syntax exploration, each with a
clear message the agent recovered from:
- WHERE/SELECT ordering mixed up (msg 19) — clear parse error.
- Bare `select 1;` probe appended after a rowset def (msgs 22/32/34) — parser wanted an
  alias/continuation; agent's own scaffolding, not the real query.
- `agg_1999.item_id` undefined (msgs 52/54) — agent initially misnamed the rowset
  `agg_sales`; error gave correct suggestions and the agent fixed it (final query's
  `union join agg_1999.item_id = agg_2000.item_id` works).
The burn is long between-attempt reasoning on a genuinely hard multi-rowset self-pair
query, not an engine hang or mis-resolution. The engine ultimately planned the whole
query correctly (proven by the A/B match).

## Classification
**Agent error (semantic column choice), with concrete proof.** NOT a residual of the
prior q64 fixes, NOT a regression, NOT a new framework bug. The construct family those
memory entries cover (cross-rowset coalescing / subset-union self-pair / correlated
filters) all resolved correctly here.

## Recommendation
No engine/guidance fix required for correctness. Optional guidance nicety (low value):
none of the errors misled the agent toward `ext_`; that was a pure reading-comprehension
miss on "wholesale cost" vs "line-extended". Leave as-is.
