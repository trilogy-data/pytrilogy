# v4 compatibility audit (2026-06-09)

Fresh re-classification of every entry in `tests/v4_known_failing.py`, run in
isolation under `TRILOGY_V4_DISCOVERY=1 --runxfail`. Regenerate with
`python local_scripts/v4_classify.py` (writes `v4_classify.json`).

Supersedes the Jun-5 sweep snapshot (reasons/results/triage/SUMMARY + the
root-cause-A / fanout / agg-source briefs), all deleted — those bugs are fixed
and the data was stale.

## Headline

68 tracked entries:

| bucket | n | meaning |
| --- | --- | --- |
| **SHAPE** | 43 | correct rows, worse/different SQL — cosmetic, not a parity bug |
| **CRASH** | 11* | v4 raises — genuine feature/coverage gap |
| **ROWS**  | 8  | wrong rows / row count — genuine correctness gap |
| **RENDER**| 2  | date arithmetic dropped from SQL — genuine gap (asserted via SQL text, lives in SHAPE-ish but is functional) |
| **XPASS** | 1  | now passes in isolation — remove from list |

\* one more "CRASH" is environmental: `test_fifty` = `PermissionError` on a
`zquery_timing_*` file (Windows file lock), not a v4 bug.

So ~21 of 68 are genuine parity gaps; the other ~46 are SQL-shape/verbosity
(TPC-DS size ceilings + inlining CTE-shape snapshots).

**Remove now (stale):** `gcat/test_gcat.py::test_no_duplicates` — XPASSes in isolation.

## Genuine gaps, clustered by root cause

### C1 — merge / rowset fan-out (wrong rows) — 4
Big row blowups; v4's merge/multiselect dedup misses on the demo models.
- `engine/demo/test_demo_duckdb.py::test_merge` (7337 vs 8)
- `engine/demo/test_demo_duckdb.py::test_merge_basic` (26730 vs 17)
- `engine/demo/test_demo_duckdb_import.py::test_demo_merge_rowset_e2e` (7337 vs 8)
- `persistence/test_complex_persistence.py::test_complex` (16 vs 4)
- also `engine/demo/test_demo_duckdb.py::test_demo_e2e` crashes (NoDatasourceException, see C3)

→ **No distilled case.** Highest-value new parity case to add.

### C2 — pseudonym / recursive source-map crash — 4
`SyntaxError: Missing source map entry for <X> with pseudonyms {...}` at render.
- `modeling/hackernews/test_hackernews_queries.py::test_adhoc02` (root_parent.id / recursive_parent)
- `modeling/hackernews/test_hackernews_queries.py::test_adhoc03` (same)
- `modeling/test_complex.py::test_window_clone` (local.nums)
- `modeling/gcat/gcat2/test_gcat_two.py::test_refresh` (org.code / first_org)

→ **No distilled case.** Cluster shares the source-map-with-pseudonym render path.

### C3 — partial-source / namespaced-property-without-key crash — 3
`UnresolvableQueryException: … could only be resolved from partial sources` /
`NoDatasourceException`. A namespaced property is bound but its key has no
datasource. `cases/namespaced_property_without_key_datasource.preql` exists and
passes, so the *base* shape is fixed — these are remaining triggers it doesn't cover.
- `discovery/test_discovery.py::test_history_e2e_non_materialized_field` (total_customer_revenue)
- `modeling/gcat/test_gcat.py::test_join` (vehicle.name)
- `engine/demo/test_demo_duckdb.py::test_demo_e2e` (passenger.cabin@Grain<passenger.id>)

### C4 — "Invalid reference string" crash (ValueError) — 2
- `modeling/gcat/gcat2/test_gcat_two.py::test_extra_fields_two`
- `modeling/ncaa/test_ncaa.py::test_adhoc02`

### C5 — aggregate-derivation unresolvable crash — 1
- `engine/test_duckdb_filter.py::test_array_inclusion_aggregate`
  (`ord_count<…AGGREGATE>` — could not resolve connections)

### C6 — KeyError `ds~store_sales_unified` crash — 1
- `modeling/tpc_ds_duckdb/test_queries.py::test_twenty_three`

### C7 — date arithmetic dropped from render — 2
Interval subtraction (`DATE_ADD(..., INTERVAL -30 day)` / `datetime(...)`) missing.
- `engine/test_bigquery.py::test_date_diff_rendering`
- `engine/test_sqlite.py::test_date_diff_rendering`

### C8 — ambiguous forced-join off-by-one rows — 2
- `modeling/join_resolution/test_join_resolution.py::test_ambiguous_error_with_forced_join` (4 vs 3)
- `…::test_ambiguous_error_with_forced_join_order` (6 vs 5)

### C9 — misc wrong rows — 2
- `modeling/rides_example/test_ride_example.py::test_example_model` (1 vs 4)
- `modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_def_wrapped_filtered_aggregate_in_basic_expression_keeps_aggregate` (0 vs 2)

## Coverage-gap conclusion

Only `top_x_by_metric` is distilled in `v4_evals/failing_cases/`. None of the
genuine crash clusters (C2–C6) or the merge fan-out (C1) have a standalone
parity repro. Those are the new cases to author, in priority order:
**C1 (fan-out, wrong rows) → C2 (recursive/pseudonym crash) → C3 (partial-source
triggers) → C4/C5/C6 (one-off crashes) → C7 (render) → C8/C9.**

The 43 SHAPE entries need no eval case; they're tracked verbosity/inlining
regressions to close at the source-selection / render layer, gated on
`CONFIG.use_v4_discovery` in their SQL assertions.
