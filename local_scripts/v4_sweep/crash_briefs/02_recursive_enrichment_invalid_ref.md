# P0 crash 02 — recursive CTE enrichment leaves an unresolved parent reference

**Test:** `tests/engine/test_duckdb.py::test_recursive_enrichment`
**Exception:** `ValueError: Invalid reference string found in query` (INVALID_REFERENCE_BUG)
**Generator:** `trilogy/core/processing/v4_node_generators/recursive.py`

## Repro
```
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  tests/engine/test_duckdb.py::test_recursive_enrichment --runxfail --tb=short
```
v3 passes (drop the env var).

## Symptom
A recursive query (parent/child node walk) followed by an enrichment. The first
CTE renders fine:

```sql
quizzical as (
SELECT "parent_nodes"."label" as "parent_label"
FROM ( select 1 as id, 'A' as label union all ... ) ...
```

…but a downstream CTE references a concept the recursive/enrichment node never
projected, so the final SQL contains `INVALID_REFERENCE_BUG` and `base.py:2314`
raises. (Capture the full SQL from the repro to see exactly which column.)

## Hypothesis
The recursive node generator builds the recursion CTE but the **enrichment join
that re-attaches the recursed key to the dimension/label** isn't sourcing one of
the demanded outputs — i.e. the enrichment parent's `output_concepts` is missing
the column the consumer reads. Inspect `recursive.py` for how it wires the
enrichment parent and whether it threads all demanded outputs (mirror v3's
recursive handling). Same upstream class as brief 01 (missing parent output), but
in the recursive generator.

## Done when
`test_recursive_enrichment` passes under both engines; no INVALID_REFERENCE_BUG;
entry removed from `tests/v4_known_failing.py`; rerun `tests/engine/test_duckdb.py`
under v4 for fallout.
