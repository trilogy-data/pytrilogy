# v4 compatibility audit (last refreshed 2026-06-11)

Fresh re-classification of every entry in `tests/v4_known_failing.py`, run in
isolation under `TRILOGY_V4_DISCOVERY=1 --runxfail`. Regenerate with
`python local_scripts/v4_classify.py` (writes `v4_classify.json`).

Supersedes the Jun-5 sweep snapshot (reasons/results/triage/SUMMARY + the
root-cause-A / fanout / agg-source briefs), all deleted — those bugs are fixed
and the data was stale.

## Headline (2026-06-11 re-run)

59 tracked entries (down from 68 — C1 merge fan-out, C3 demo_e2e, C4 ncaa
adhoc02, C2 gcat test_refresh, and the window_clone source-map crash all fixed;
window_clone + gcat test_no_duplicates removed from the list):

| bucket | n | meaning |
| --- | --- | --- |
| **SHAPE** | 41 | correct rows, worse/different SQL — cosmetic, not a parity bug |
| **CRASH** | 8* | v4 raises — genuine feature/coverage gap |
| **ROWS**  | 8  | wrong rows / row count — genuine correctness gap (incl. C7 render) |
| **TIMEOUT** | 2 | tpc-ds q21/q22 exceed the classify budget |

\* the classifier mis-buckets `tpc_ds_duckdb::test_seventy_six` as CRASH; it's a
SHAPE/size failure (`assert 16080 < 10000`, identical before/after the
window_clone fix). So **7 genuine crashes**, not 8.

So ~15 of 59 are genuine parity gaps (7 crash + 8 rows); the rest are
SQL-shape/verbosity (TPC-DS size ceilings + inlining CTE-shape snapshots).

### FIXED 2026-06-11 — C2 window_clone source-map crash
`test_complex.py::test_window_clone` (`SyntaxError: Missing source map entry for
local.nums`). Two-part fix in the v4 merge-join-key widener:
1. `parent_output_addresses` (projection.py) now excludes **hidden** parent
   outputs — a hidden output is dropped from that parent's CTE SELECT, so it's
   not readable by a consumer and must not count as "available".
2. `_widen_merge_join_keys` (strategy_builder.py) now skips carrying any sibling
   concept that is in the receiving parent's `existence_concepts` — an existence
   concept is consumed via a subselect (`WHERE x in (select …)`), never joined
   as a row column, so carrying it as a widenable output produced a dangling
   un-joined CTE reference (`Referenced table "highfalutin" not found`).
NOT the right axis: gating on `concept.derivation` (WINDOW/UNNEST/…) over-broadly
broke `test_window_over_rollup_preserves_grouping_rows`, which legitimately
carries a WINDOW result. The discriminator is existence-vs-row, not derivation.

## Genuine gaps, clustered by root cause

### C1 — merge / rowset fan-out (wrong rows) — 4
Big row blowups; v4's merge/multiselect dedup misses on the demo models.
- `engine/demo/test_demo_duckdb.py::test_merge` (7337 vs 8)
- `engine/demo/test_demo_duckdb.py::test_merge_basic` (26730 vs 17)
- `engine/demo/test_demo_duckdb_import.py::test_demo_merge_rowset_e2e` (7337 vs 8)
- `persistence/test_complex_persistence.py::test_complex` (16 vs 4)
- also `engine/demo/test_demo_duckdb.py::test_demo_e2e` crashes (NoDatasourceException, see C3)

→ **No distilled case.** Highest-value new parity case to add.

### C2 — recursive-CTE pseudonym source-map crash — 2 (was 4)
`SyntaxError: Missing source map entry for root_parent.id with pseudonyms
{'local.recursive_parent'}` at render — the recursive-CTE alias path.
- `modeling/hackernews/test_hackernews_queries.py::test_adhoc02` (root_parent.id / recursive_parent)
- `modeling/hackernews/test_hackernews_queries.py::test_adhoc03` (same)

FIXED & removed: `test_window_clone` (different root cause — existence-concept
carried as a row output, see headline) and `test_refresh` (gcat, fixed earlier).
The two remaining are a genuinely distinct bug: a recursive_parent pseudonym not
mapped onto its `root_parent.id` source. **Next open issue.**

→ **No distilled case.**

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
