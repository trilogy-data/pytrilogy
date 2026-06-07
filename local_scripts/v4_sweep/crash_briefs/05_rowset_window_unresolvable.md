# P0 crash 05 — rowset + window selection is unresolvable under v4

**Test:** `tests/engine/demo/test_demo_duckdb_multi_table.py::test_rowset_shape`
**Exception:** `UnresolvableQueryException: Could not resolve connections for query
with output ['survivors.passenger.id<...Derivation.ROWSET>', ...,
'local.eldest<Purpose.PROPERTY>Derivation.WINDOW>'] from current model (v4 discovery).`
**Site:** `trilogy/core/query_processor.py:488` (`_get_query_node_v4`).
**Generators:** `v4_node_generators/rowset.py` + `window.py`.

## Repro
```
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  "tests/engine/demo/test_demo_duckdb_multi_table.py::test_rowset_shape" --runxfail --tb=short
```
v3 passes (drop the env var).

## Symptom
The select mixes rowset-derived passthrough columns (`survivors.passenger.*`,
Derivation.ROWSET) with a window concept (`local.eldest`, Derivation.WINDOW)
computed over them. `search_concepts_v4` returns None — no plan reconciles the
rowset passthrough grain with the window concept's required grain.

## Hypothesis
Per memory, v4 rowset handling buckets a rowset's outputs into one group/CTE
(`partition_rowsets`), and window concepts need their `over`/order inputs pulled
through. Here the window (`eldest`) sits *on top of* the rowset outputs; the
planner probably can't satisfy the window's input demand from the rowset node
(the rowset CTE doesn't expose what the window needs, or the window node isn't
allowed to take the rowset node as parent). Inspect how `rowset.py` exposes
outputs and whether `window.py`/the search can stack a window on a rowset-derived
parent. Related fixed cases: `rowset_rank_join_fanout`, `rowset_select_grain_dedup`
in `local_scripts/v4_evals/cases/`.

## Done when
`test_rowset_shape` passes under both engines; entry removed from
`tests/v4_known_failing.py`; rerun `tests/engine/demo` under v4 for fallout.
