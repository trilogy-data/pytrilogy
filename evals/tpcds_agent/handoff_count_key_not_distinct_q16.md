# Handoff: `count(<key>)` returned 274,743 for 160,000 distinct orders (q16)

**Status: FIXED (2026-08-10).** The two-site change from the diagnosis is
implemented and validated. Repro:
`evals/tpcds_agent/repro_q16_count_key_not_distinct.py` (prints FIXED).
Regression tests:
`tests/modeling/tpc_ds_duckdb/test_q16_count_key_shared_filter_stream.py`
(xfail removed; all 3 pass, including the lone-`count(key)` no-DISTINCT guard).

## Symptom (historical)

`count(cs.order_number)` beside `count(cs.order_number ? cs.is_returned)`
returned 254,337 at sf=1 (274,743 in the eval's messy-warehouse variant) where
the truth is 160,000. `sum` was broken identically (20,349,624,832 vs
12,800,080,000) — verified fixed too.

## Root cause

The build step mints `order_number ? is_returned` as a per-row
`CASE WHEN cond THEN order_number END`. A mixed order (returned + unreturned
lines) yields `{orid, NULL}`, so the normalization `GROUP BY orid, minted_orid`
kept two rows for one order and every aggregate over the deduped stream
double-counted. The concept model already declared `keys=['cs.order_number']`;
dedup can't deliver that — the reduction has to be a collapse.

## The fix (landed)

1. `trilogy/core/models/execute.py` — new `CTE.filter_collapses_to_grain(c)`:
   a locally-computed FILTER whose `keys` are covered by the grouping CTE's
   grain but whose `where` reads columns outside it is a property of the
   grain, not a grouping key. `check_is_not_in_group` consults it and drops
   the virtual from GROUP BY. Guarded behind the CASE-elision check (CTE
   condition implies the predicate → rendered value is bare content, stays a
   group key), mirroring `has_local_aggregate`.
2. `trilogy/dialect/base.py` — `render_concept_sql` wraps such a concept in
   `max(...)` (the window alternative was unnecessary: the CTE already has a
   GROUP BY, so a plain aggregate collapse needs no extra node).

## Validation

- Repro + sum variant both exact at sf=1.
- `tests/engine/test_duckdb_filter.py` (the suite that killed the reverted
  COUNT(DISTINCT) attempt): 26 passed.
- Corpus A/B (worktree baseline at 1d50578cb vs patched tree, plus a no-op
  determinism leg): exactly **3 of 132** queries change, all the predicted
  footprint sites — tpc_ds q97-one/q97-two (presence virtuals) and tpc_h q21
  (`count(l_suppkey ? late)`), each losing the virtual from GROUP BY and
  gaining the `max(...)` collapse. All other queries byte-identical.
- Full suite `-m "not adventureworks_execution"` (run in 4 sequential chunks):
  ~7,738 passed, 0 failed. The only xpass is the pre-existing
  environment-dependent fakesnow xfail in `tests/execution/state`.

## Rejected approaches (kept so they don't get re-tried)

- **COUNT(DISTINCT) rewrite at bucket-partition time** — fires blind to
  whether the normalization GROUP is emitted; broke `count(x ? x)` over
  `const x <- unnest([...])`; doesn't generalize to `sum`.
- **Splitting the bucket and rejoining** — both aggregates correctly share the
  `{orid}` input grain; no partition-key change needed.
- **Minting a collapsed concept in the planner** — `minted_orid` already
  exists; nothing new needed.
- **`parsing/common.py:1156` grain asymmetry** — not touched; the renderer
  collapse fixes the SQL regardless, and the grain change alone was assessed
  necessary-but-not-sufficient. Still a valid loose thread if grain-level
  cleanliness is wanted later.
