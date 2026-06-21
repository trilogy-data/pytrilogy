# Union + Scoped-Join Agent Guidance — Baseline & Rebaseline (2026-06-11)

We introduced query-scoped `union(...)` (relational UNION TVF) and `inner|left join a = b`
(query-scoped join) and updated the AI/agent guidance to prefer them over the
`merge … align … derive` multi-select. This records the eval that measured the change.

## The join/union query set (`tests/modeling/tpc_ds_duckdb`)

Reference `.preql` files whose canonical answer uses a scoped join or the union TVF:

- **Scoped `join`**: q29, q40, q44, q46, q54, q64, q68, q72, q75, q82, q84
- **Relational `union(...)`**: q76, q77

Run set: `--query-ids 29,40,44,46,54,64,68,72,75,76,77,82,84` (13 queries), `--scale-factor 1`.

## Guidance changes (all validated: `tests/ai/test_syntax_examples.py` compiles+executes each example)

- `trilogy/ai/syntax_examples.py`: added `union-stack-channels` and `scoped-join` drill-down
  examples; **removed** the `aligned-multi-select` (multi-select) example.
- `trilogy/ai/constants.py` `RULE_PROMPT` (always-loaded): added a `union(...)` TVF bullet,
  extended the scoped-join bullet (self-pair preference + `= c` key-chaining), disambiguated
  the "no SQL `UNION`/`JOIN on`" rule from the supported forms, and scrubbed `align`/multi-select
  guidance.

## Results (DeepSeek agent, 1 run_eval pass per leg, sf=1)

| Leg | Baseline | Rebaseline | Flipped to PASS | Regressions |
|---|---|---|---|---|
| enriched | 1/13 (8%) | **4/13 (31%)** | q44, q75, q82 | none (q68 held) |
| ingest | 2/13 (15%) | **3/13 (23%)** | q29 | none (q40, q82 held) |

Baseline runs: `results/20260611-124628_{enriched,ingest}`.
Rebaseline runs: `results/20260611-133358_{enriched,ingest}`.

Where the agent adopted the new constructs it passed or got dramatically closer
(e.g. enriched q64 went 16→2 rows — now only a value diff; q76/q77 near-misses).

## Remaining enriched misses (from trace diagnosis)

### Group A — agent still didn't reach for the scoped join (actionable guidance gap)
- **q29** — single-key `merge` (left-outer) → 100 vs 1 rows. Needs scoped `inner join` + `having sum(...)>0`.
- **q40** — single-key `merge` on `order_number`; reference joins on BOTH `item.id` AND `order_number`
  (composite grain) → fanout.
- **q72** — `merge` on the wrong warehouse role; should be a scoped join on the inventory warehouse.
- **q77** — bypassed `union(...)` (hand-rolled `all_sales` + rollup); off-by-one row (unverified).

Pattern: agents **default to `merge`** for fact-to-fact blends instead of the preferred scoped
join, and when they join they often **match only one column of a composite key**.
Proposed next tweak: (1) stronger "blend facts in a query → scoped `inner join`, not `merge`"
steer; (2) add "match on EVERY shared grain key, not just one (composite-grain fanout)".

### Group B — pre-existing non-join issues (already in EVAL_LOOP_INSTRUCTIONS, out of scope here)
- **q76** — used union correctly; `count(line_item)` vs the `line_item_count`/row-flag measure (recurring count-lines).
- **q64** — used scoped join correctly; miss is demographic-inequality *timing* (apply after aggregation — documented perf idiom).
- **q84** — demographic-bridge navigation; **q54** — quirky store-count-multiply reference. Both flagged "hard".
