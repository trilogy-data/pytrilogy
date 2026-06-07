# P0 crash 01 — window `over <dimension>` drops the partition column

**Test:** `tests/complex/test_window_function_parsing.py::test_rank_by`
**Exception:** `ValueError: Invalid reference string found in query` (INVALID_REFERENCE_BUG)
**Generator:** `trilogy/core/processing/v4_node_generators/window.py`

## Repro
```
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  tests/complex/test_window_function_parsing.py::test_rank_by --runxfail --tb=short
```
v3 passes (drop the env var).

## Symptom
The window concepts are `user_country_rank <- rank user_id over country` and
`rank_derived <- 100 + row_number user_id over country`. `country` is a property
on the `users` datasource (`about_me: country`); the window is computed in a CTE
built from `posts`. The emitted CTE has:

```sql
highfalutin as (SELECT
    INVALID_REFERENCE_BUG as `country`,
    `posts`.`user_id` as `user_id`,
    row_number() over (partition by INVALID_REFERENCE_BUG) as `_virt_window_row_number_...`
FROM `bigquery-public-data`.`stackoverflow`.`post_history` as `posts`)
```

So the WindowNode tried to emit/partition-by `country`, but its parent (the
`posts` scan) never sourced `country` — that column lives on `users` and requires
a join. `compile_statement` then trips the strict-mode guard at `base.py:2314`.

## Hypothesis
The window's **partition (`over`) dimension** is not being treated as a required
input. `gen_window` sets `input_concepts=parent_outputs_needed(outputs, parents, conditions)`
— check whether `parent_outputs_needed` (in `v4_node_generators/common.py`)
includes the window lineage's `over` concepts, and whether the planner adds the
enrichment join so the parent actually *sources* that dimension from `users`.
Likely the `over`/partition concept is omitted from the demanded parent outputs,
so no join to `users` is planned and the column renders as the sentinel.

Compare with v3's `gen_window_node` enrichment (it pulls `over` concepts and joins
their source) — see memory note "metric grain = grouping keys" / the q67 window
multi-arg-keys path for how `over` keys are meant to widen demand.

## Done when
`test_rank_by` passes under both engines (assert `"rank() over (partition"` present,
no INVALID_REFERENCE_BUG); entry removed from `tests/v4_known_failing.py`; rerun
`tests/complex` under v4 to confirm no fallout. Good candidate for a duckdb
`failing_cases/` repro (rank over a dimension on a second table).
