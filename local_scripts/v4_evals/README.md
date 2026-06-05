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

### TPC-H — 22/28 parity (5 v4-attributable failures)

Fixed: 02/20 (filter past barrier), 11 (cross-grain agg HAVING → FINAL WHERE),
21 (raw filter wrongly propagated past a `?` virtual-filter), adhoc07.

| Query | Failure | Root cause |
| --- | --- | --- |
| 10 | crash: "Invalid input concepts to node" (nation.id) | missing parent node |
| 18 | 5 → 100 rows | fan-out (many customers per order) + unapplied qty>300 HAVING |
| 22 | → 0 rows | global conditional aggregate `avg(? …)` loses its avg() wrapper |
| adhoc01 | first cols 100.0 vs 0.19 | wrong aggregate value (window/ratio) |
| adhoc03 | 333 → 150000 | wrong value (count vs key) |

Not v4: adhoc02 (v3 also errors — self-referencing aggregate); adhoc06
(cache-dependent skip — v4 returns the correct answer, its suite failure is
structure: it stops using the precomputed cache table).

### Generic cases

- `cross_grain_aggregate_filter` — per-key vs global aggregate compared in a WHERE (q11 shrink): v4 used to emit an ungrouped HAVING (BinderError); now matches v3 (post-aggregate WHERE over a cross-join). PASSES.
- `filter_past_unnest` — filter over an unnested-const value: now matches (filter no longer dropped past the unnest barrier). PASSES.
- `global_aggregate_filter` — `max(date) by *` filter + global aggregate in output: v4 emits ambiguous-alias SQL (BinderError). STILL FAILING (root cause E).
- `window_having_conditional_count` — windowed `rank` filtered by HAVING, selected beside `count(x ? case_dim is not null)`: v4 drops the `?` predicate in the window-HAVING context and counts every row. STILL FAILING (row mismatch). Distilled from ncaa `adhoc08`.
- `namespaced_property_without_key_datasource` — datasource binds a namespaced property (`device.category`) but not its key (`device.id`); v4 demands a datasource for the bare key and raises NoDatasourceException. STILL FAILING. Distilled from google_analytics `all_sites.device.id`.

q21's fix has no generic case: a flat single-table shrink makes v3 push the
filter too (its correctness depends on the multi-table structure), so v3 isn't
a valid oracle. Guarded by `test_virtual_filter_scoped_columns_*` unit tests +
the TPC-H q21 harness match instead.
