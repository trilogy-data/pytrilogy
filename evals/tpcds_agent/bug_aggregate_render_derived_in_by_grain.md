# Engine bug handoff: `INVALID_REFERENCE_BUG_<Cannot render aggregate ...>` emitted into SQL when a `by`-grain mixes a derived concept with a key it depends on

## Summary

The SQL renderer emits its internal sentinel string
`INVALID_REFERENCE_BUG_<Cannot render aggregate <addr> in CTE <name>: source_map
miss and CTE grain <X> != aggregate by-grain <Y>>` **directly into the generated
SQL**, which then fails at DuckDB parse time:

```
(_duckdb.ParserException) Parser Error: syntax error at or near "aggregate"
LINE n:     INVALID_REFERENCE_BUG_<Cannot render aggregate local.pair in CTE
quizzical: source_map miss and CTE grain Grain<ss.item.item_sk> !=
aggregate by-grain <['local.prefix', 'ss.item.item_sk']>...
```

The sentinel comes from `INVALID_REFERENCE_STRING(...)` at
`trilogy/dialect/base.py:1020` (the `AGGREGATE_ITEMS` branch of
`render_concept_sql`). It is a *guarded* failure mode — there are regression
tests asserting `"INVALID_REFERENCE_BUG" not in sql` (e.g.
`tests/complex/test_aggregate_of_aggregate.py`, `tests/optimization/test_join_hoist.py`,
`tests/modeling/funnel_analysis/test_funnel_analysis.py`) — but this trigger
slips past them.

This was the single largest failure cluster in the TPC-DS agent eval base leg,
run `20260601-025817_base` (7+ of 24 failures), all on **q23**. It also surfaces
on the enriched leg (`20260601-025817_enriched`, q23). The query is well-formed;
it must compile to valid SQL or raise a clean modeling error, never inject a
placeholder into emitted SQL.

## Minimal, deterministic repro

**Single model, 3 lines.** No merge, no cross-model anything.

```
import raw.store_sales as ss;
auto prefix <- substring(ss.item.item_desc, 1, 30);
auto pair  <- sum(1) by (prefix, ss.item.item_sk);
select prefix, ss.item.item_sk, pair limit 5;
```

```bash
trilogy run repro.preql duckdb
```

Fails at render time (data-independent — the placeholder is in the SQL string
before execution).

### What narrows it (tested against `20260601-025817_base/workspace`)

| `pair <- sum(1) by (...)` grain | result |
|---|---|
| `by (ss.item.item_sk, ss.date_dim.date_sk)` — plain keys only | ✅ runs |
| `by (prefix)` — derived concept ALONE | ✅ runs |
| `by (prefix, ss.item.item_sk)` — derived + the key it depends on | ❌ INVALID_REFERENCE_BUG |
| `by (pricebucket, ss.item.item_sk)` where `pricebucket <- ss.sales_price > 100` (derived from a **fact** column, not from the item key) | ✅ runs |
| outer re-agg `sum(pair) by (prefix)` on top | ❌ (same failure; re-agg is **not** required to trigger) |

**Trigger:** the aggregate's `by`-grain contains a **derived concept that is
functionally dependent on a key also present in the same `by`-grain** —
`prefix = substring(ss.item.item_desc, 1, 30)` is determined by
`ss.item.item_sk` (item_sk → item_desc → prefix), and both `prefix` and
`ss.item.item_sk` are in the grain.

### Smoking gun

The error message names the inconsistency directly:

```
CTE grain Grain<ss.item.item_sk> != aggregate by-grain <['local.prefix', 'ss.item.item_sk']>
```

The planner **prunes `prefix` from the CTE grain** (collapsing `(prefix, item_sk)`
to `(item_sk)` — presumably because `prefix` is functionally determined by
`item_sk`), but the aggregate's recorded `by`-grain still lists `local.prefix`.
At render time the `prefix` column is no longer in the CTE's `source_map`/output,
so `render_concept_sql` for the aggregate hits the `else` branch at
`base.py:1015-1025` and emits the sentinel instead of a column reference.

## Where to look

- `trilogy/dialect/base.py:1000-1025` — the `AGGREGATE_ITEMS` branch. The
  `source_map miss` fallback is the symptom, not the cause.
- The **grain-pruning / functional-dependency** logic that decides the CTE grain
  is `Grain<ss.item.item_sk>` while the aggregate by-grain stays
  `[local.prefix, ss.item.item_sk]`. Either (a) the CTE must keep `prefix` in its
  output/grain so the source_map resolves, or (b) the aggregate's by-grain must
  be reduced consistently with the CTE grain so the renderer looks up `item_sk`.
  The two must agree.
- Likely in `Grain` reduction / `from_concepts` and the node that builds the
  aggregate CTE's `source_map` (so a functionally-dependent derived concept in a
  `by` grain is preserved as an output column, or normalized away on both sides
  together).

## Suggested regression assertions once fixed

- The 3-line repro above runs and returns rows.
- Add an assertion that `sum(1) by (derived_of_key, key)` keeps `derived_of_key`
  in the CTE output (no `INVALID_REFERENCE_BUG`).
- Keep the existing `"INVALID_REFERENCE_BUG" not in sql` invariant; this trigger
  shows the existing tests don't cover the derived-concept-in-by-grain case.

## Agent workaround (what eventually passed)

Split the single mixed-grain aggregate into two, so no aggregate is grouped by a
derived concept alongside the key it depends on:

```
auto item_date_cnt <- count(ss.item.item_sk) by (ss.item.item_sk, ss.date_dim.date_sk);
auto cnt_by_prefix <- sum(item_date_cnt) by (substring(ss.item.item_desc, 1, 30));
```

The agent burned ~50 turns rediscovering this on q23; a framework fix removes the
thrash.

## Setup

Reuse the eval workspace `evals/tpcds_agent/results/20260601-025817_base/workspace/`
(has `raw/`, `tpcds.duckdb`, `trilogy.toml`); `cd` there and run the repro.
Render-time bug, so any populated TPC-DS schema works.

- Observed on `trilogy 0.3.275`.
- Distinct from `recursion_bug_handoff.md` (that is a build-time recursion on a
  multi-key cross-model **additive** aggregate; this is a render-time source_map
  miss on a single-model aggregate with a derived key in its `by`-grain).
