# Make every TPC-DS question meaningful at sf=0.1

**Filed 2026-10-01. Not started.**

## Why

The eval defaults to sf=1 (`spec.py`, `default_scale_factor=1.0`) because at smaller
scales many TPC-DS spec predicates select nothing, and agents spin re-exploring instead of
accepting a 0-row answer. Running at sf=1 has a cost: a bad agent join (an accidental
cross product or fan-out over `store_sales` etc.) can OOM the test machine, especially
with several workers running at once. At sf=0.1 the same mistake is ~10x smaller and fails
fast.

## What to do

Adjust predicates (years, states, categories, price bands, date windows, etc.) so that every
question returns a non-empty, non-trivial result at sf=0.1, then flip the default.

1. Build the sf=0.1 DB (`--scale-factor 0.1` with a small `--query-ids` run populates the
   cache) and run every reference under `tests/modeling/tpc_ds_duckdb/query<NN>.sql`
   against it. List the queries with 0 rows, or with too few rows to tell a right answer
   from a wrong one (e.g. one row where a missing join key would go unnoticed).
2. For each one, widen or move the predicate until the result is meaningful. Keep these
   three in lockstep:
   - the prompt in `query_prompts.json`,
   - the reference `query<NN>.sql`,
   - the corpus `query<NN>.preql` (both live in `tests/modeling/tpc_ds_duckdb`; the
     modeling tests assert on them, so expect test updates).
3. Prefer changing predicate values over changing the question's shape. Each edited query
   stops being prompt-comparable with earlier runs (as with q29 on 2026-08-21), so record
   the changed ids here.
4. Change `default_scale_factor` to 0.1 and update the comment above it. Re-baseline every
   category (`ingest`, `enriched`, SQL legs) at 0.1. Baselines from different scale factors
   are not comparable.

## Watch for

- Queries that fan out (q29-style) can produce plausible-looking but wrong results at sf=0.1
  if the multiplicity they test for doesn't occur at that scale. Check that the property
  the question tests still occurs at sf=0.1, not just that rows come back.
- The scorer falls back to `PRAGMA tpcds()` answers only when no reference file exists, and
  those answers use the spec predicates. Any query without a reference file needs one once
  its predicate changes.
