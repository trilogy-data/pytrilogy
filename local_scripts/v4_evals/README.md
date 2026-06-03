# v4 correctness evals

v3-vs-v4 result-parity checks for the v4 discovery planner
(`CONFIG.use_v4_discovery`). Both harnesses generate + execute under each
planner on duckdb and compare result rows as a column-sorted, float-rounded
multiset. A v4 crash / hang / row diff is a correctness regression; SQL that
differs in shape but returns the same rows passes (that's a *structure* change,
inventoried in `../v4_sweep/triage.md`, not a parity bug).

These complement `../discovery_v4_compare.py` (TPC-DS vs v3 reference logs).

## Harnesses

- **`run_parity.py`** — generic. Each `cases/*.preql` is a self-contained
  program (inline datasources / consts + a final SELECT). Home for ad-hoc
  correctness repros lifted from failing suite tests.
  ```
  python local_scripts/v4_evals/run_parity.py            # all cases
  python local_scripts/v4_evals/run_parity.py filter_past_unnest
  ```

- **`tpc_h_v4_compare.py`** — TPC-H family (query01-22 + adhoc). Needs the
  sf=0.1 dataset at `tests/modeling/tpc_h/memory` (built by running any tpc_h
  test once). cache-dependent queries (adhoc06) are skipped.
  ```
  python local_scripts/v4_evals/tpc_h_v4_compare.py          # all
  python local_scripts/v4_evals/tpc_h_v4_compare.py 02 18 adhoc03
  ```

## Current state

### TPC-H — 17/28 parity (10 v4-attributable failures)

| Query | Failure | Root cause |
| --- | --- | --- |
| 02, 20 | crash: "… not reachable from any group" | filter inputs only exist past a row-shape barrier |
| 10 | crash: "Invalid input concepts to node" (nation.id) | missing parent node |
| 11 | crash: duckdb "must appear in GROUP BY" | invalid SQL emitted |
| 18 | 5 → 100 rows | fan-out (many customers per order) |
| 21, 22 | → 0 rows | filter drops everything |
| adhoc01 | first cols 100.0 vs 0.19 | wrong aggregate value |
| adhoc03 | 333 → 150000 | wrong value |
| adhoc07 | custkey fan-out | many customers per order_key |

Not v4: adhoc02 (v3 also errors — self-referencing aggregate); adhoc06
(cache-dependent skip — v4 returns the correct answer, its suite failure is
structure: it stops using the precomputed cache table).

### Generic cases

- `filter_past_unnest` — filter over an unnested-const value: v3 → 5 rows, v4 → 10 (filter dropped past the unnest barrier).
- `global_aggregate_filter` — `max(date) by *` filter + global aggregate in output: v4 emits ambiguous-alias SQL (BinderError).
