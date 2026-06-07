# P0 crash 04 — struct-in-array model is unresolvable under v4

**Tests:**
- `tests/complex/test_structs.py::test_struct_in_array_parsing`
- `tests/complex/test_structs.py::test_struct_in_array_item_access`

**Exception:** `UnresolvableQueryException: Could not resolve connections for query
with output ['local.a<Purpose.KEY>Derivation.ROOT>', 'local.b<...>'] from current
model (v4 discovery).`
**Site:** `trilogy/core/query_processor.py:488` (`_get_query_node_v4`, when
`search_concepts_v4` returns `strategy_node is None`).

## Repro
```
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  tests/complex/test_structs.py::test_struct_in_array_parsing \
  tests/complex/test_structs.py::test_struct_in_array_item_access --runxfail --tb=short
```
v3 passes (drop the env var).

## Model (from the test)
```
key a int;
key b int;
key array_struct list<struct<a,b>>;
auto unnest_array <- unnest(array_struct);
datasource struct_array ( array_struct: array_struct )
  grain (array_struct)
  query '''select [{a: 1, b: 2}, {a: 3, b: 4}] as array_struct''';
```
v4 can't resolve `a`/`b` (the struct fields) as ROOT concepts — they're reachable
only by `unnest(array_struct)` then struct-field access, but the only datasource
binds `array_struct`.

## Hypothesis
`search_concepts_v4` has no path from the demanded struct-field keys (`a`, `b`)
back through the `unnest` of `array_struct` to the `struct_array` datasource. The
graph/edge construction (`v4_helper/concept_graph.py` / `edges.py`) likely doesn't
emit the unnest→struct-field reachability edge, so the ROOT search finds no
source and returns None. Compare v3's handling of unnest + struct-field access
(`v4_node_generators/unnest.py` exists — check it produces the field concepts as
outputs and that the graph links them to the array source).

## Done when
both struct tests pass under both engines; entries removed from
`tests/v4_known_failing.py`; rerun `tests/complex/test_structs.py` under v4.
Strong `failing_cases/` candidate — model is already tiny and self-contained.
