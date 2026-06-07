# P0 crash 03 — subselect (closest-match) leaves an unresolved reference

**Test:** `tests/engine/demo/test_demo_duckdb_subselect.py::test_subselect_closest_warehouse`
**Exception:** `ValueError: Invalid reference string found in query` (INVALID_REFERENCE_BUG)
**Generator:** `trilogy/core/processing/v4_node_generators/subselect.py`

## Repro
```
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  "tests/engine/demo/test_demo_duckdb_subselect.py::test_subselect_closest_warehouse" \
  --runxfail --tb=short
```
v3 passes (drop the env var).

## Symptom
A "closest warehouse" subselect (correlated nearest-match) renders with an
unresolved column → `INVALID_REFERENCE_BUG` in the final SQL → `base.py:2314`
raises. Capture the full SQL from the repro to identify the exact missing column
(earlier sweep also surfaced a `NoDatasourceException` for `passenger.cabin` on a
neighbouring path — confirm which fires on a clean run).

## Hypothesis
The subselect generator doesn't propagate a correlated/outer concept into the
inner node's `input_concepts`/`output_concepts`, so the inner CTE can't expose
the column the outer references. Inspect `subselect.py` for how outer-scope
concepts are threaded into the inner search and projected back out. Same
upstream class as briefs 01–02.

## Done when
`test_subselect_closest_warehouse` passes under both engines; no
INVALID_REFERENCE_BUG; entry removed from `tests/v4_known_failing.py`; rerun
`tests/engine/demo` under v4 for fallout (note: `test_demo_e2e`, `test_merge`,
`test_merge_basic`, `test_demo_merge_rowset_e2e` are separate result-fan-out
regressions, expected to stay xfail).
