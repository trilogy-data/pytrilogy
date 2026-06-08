# Brief 02 implementation handoff — recursive enrichment connector

Read `02_recursive_enrichment_invalid_ref.md` first, then this. The DIAGNOSIS is
done and validated; this is an IMPLEMENTATION handoff. Memory
`project_v4_crash_briefs` has the full narrative.

## Repro (v4 must be explicitly enabled — the env var does NOT flip CONFIG)

```python
from trilogy.constants import CONFIG
CONFIG.use_v4_discovery = True          # REQUIRED. TRILOGY_V4_DISCOVERY=1 only flips it via the conftest fixture.
from pathlib import Path
from trilogy import Dialects
from trilogy.core.models.environment import Environment
ex = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=Path("tests/engine")))
ex.environment.parse("""
import recursive;
import recursive as parent;
auto first_parent <- recurse_edge(id, parent);
merge first_parent into parent.id;
""")
ex.generate_sql("where first_parent = 1 select id, parent.label;")   # currently raises INVALID_REFERENCE
```
Test: `tests/engine/test_duckdb.py::test_recursive_enrichment` (registry entry in
`tests/v4_known_failing.py` — remove when fixed). Run under v4 with `--runxfail`.

## Root cause (validated)

The bridge ALREADY derives the connector. `_resolve_bridge_graph`
(`v4_helper/source_planning.py`) returns a graph whose nodes include
`c~local._virt_func_recurse_edge_<hash>@Grain<local.id>`, linking `local.id` ↔
`parent.id`. But `_datasource_nodes_for_bridge` only materializes real `ds~`
datasource nodes (`for ds_node in plan.graph.datasources`) and silently DROPS the
derived connector node → `local.id` has no source → `INVALID_REFERENCE_BUG`.

Because of the brief-06 gate, `_bridge_plan` already RETURNS the plan here (the
virt connector is in `bridged - requested`), so the `_datasource_nodes_for_bridge`
path is reached.

## The fix (generic over ANY derivation, not just recursive)

In `_datasource_nodes_for_bridge`, after building the datasource parents, also
PLAN the derived connector nodes the bridge identified and append them as parents;
`_merge_component_sources` then joins on the pseudonym equivalence.

### Validated scaffold (this is ~90% of it — proven to reach `gen_recursive`):

```python
_orig = sp._datasource_nodes_for_bridge
def patched(request, plan, attempt):
    parents = _orig(request, plan, attempt) or []
    env = request.environment
    from trilogy.core.processing.concept_strategies_v4 import V4History, search_concepts
    planned = set()
    for concept in plan.concepts:
        for pseudo in (concept.address, *concept.pseudonyms):
            origin = env.alias_origin_lookup.get(pseudo)      # RECURSIVE origin lives HERE, not in env.concepts
            if origin is None or origin.lineage is None or origin.address in planned:
                continue
            info = search_concepts(
                mandatory_list=[origin],
                history=cast(V4History, request.history),
                environment=env, depth=request.depth + 1,
                g=request.graph, conditions=[],
                source_policy=request.source_policy,
            )
            if info.strategy_node is not None:
                planned.add(origin.address)
                parents.append(info.strategy_node)
    return parents or None
```

### THE OBSTACLE this scaffold hits: infinite recursion (RecursionError)

Planning the connector origin via `search_concepts` re-enters
`_datasource_nodes_for_bridge`, which sees the SAME pseudonym-origin and re-plans
it, forever. You MUST add a re-entry guard. Options (pick the cleanest):
- Thread a `frozenset` of "connector origin addresses currently being planned"
  through `SourceRequest` (or `V4History`); skip injecting an origin already in it.
  Add the origin to the set in the `search_concepts` call's request/history.
- Or only inject when `origin.address` is not already among the demanded outputs of
  the in-progress search.

### Three representations of the recursion (do NOT confuse them)

For `merge first_parent into parent.id`:
1. `benv.concepts['local.first_parent']` = DEMOTED ROOT, grain `{parent.id}`,
   `lineage=None`. **Not plannable** (no recursion).
2. `alias_origin_lookup['local.first_parent']` = the real RECURSIVE
   `recurse_edge(id, parent)`. **This is what you plan.** Recover it from
   `alias_origin_lookup` keyed by the concept's address OR any pseudonym.
3. Graph node `c~local._virt_func_recurse_edge_<hash>` — transient, NOT in
   `benv.concepts` (so `environment.concepts.get(addr)` returns None — that's why
   keying off `plan.concepts` + `alias_origin_lookup` is correct, not the graph node).

### The merge join

`local.first_parent.grain == {parent.id}` and
`parent.id.pseudonyms == {local.first_parent}` — a clean pseudonym equivalence.
`_merge_component_sources` / the merge planner must join the recursion parent
(outputs `first_parent`, `id`) to the parent-datasource parent (outputs
`parent.id`, `parent.label`) on `first_parent ≡ parent.id`. Verify it does; the
strategy_builder is pseudonym-aware (brief-04 work) so it likely already will, but
CONFIRM — this is the second place it can silently drop a column.

## v3 oracle (target plan — run the repro WITHOUT `CONFIG.use_v4_discovery`)

```
wakeful  = recursion:        (id, first_parent)              -- reads local edges/nodes
cheerful = parent datasource:(parent_id, parent_label)  WHERE parent_id = 1
FINAL: cheerful INNER JOIN wakeful ON cheerful.parent_id = wakeful.first_parent
       -> output id, parent_label
```
Expected results: 4 rows, last `parent_label == 'A'`. Second query in the test:
`where parent.label='A' select count(id) as a_children` → 1 row, `a_children == 4`.

## Guardrails / gotchas

- Keep it GENERIC: trigger on "a bridged concept has a derived `alias_origin_lookup`
  origin not otherwise materialized," NOT on `derivation == RECURSIVE`. Any merged
  derivation can need this.
- Only inject a connector when it's actually NEEDED (the datasource parents don't
  already cover the requested concept) — else you'll add recursion CTEs to queries
  that don't need them and regress plan shape. Gate on the concept being absent
  from the datasource parents' outputs.
- `concept_satisfiable` (projection.py:29) is connectivity-blind — if you end up
  needing a connectivity test in `_pick_alternative` or elsewhere, that's the
  function to make component-aware. (Not required if the fix stays in
  `_datasource_nodes_for_bridge` as above.)

## Validation (definition of done)

1. Repro passes under both engines; no `INVALID_REFERENCE_BUG`.
2. Remove the entry from `tests/v4_known_failing.py`; run
   `tests/engine/test_duckdb.py` under v4 (`TRILOGY_V4_DISCOVERY=1`) for fallout.
3. Broad parity sweep (the source-planning change is shared, high-blast-radius):
   `TRILOGY_V4_DISCOVERY=1 pytest tests/modeling tests/discovery tests/optimization
   tests/complex tests/persistence tests/scripts -q -rX -m "not adventureworks_execution"`
   — 0 failed; investigate any new XPASS for promotion, any new fail is a regression.
4. `ruff check . --fix && mypy trilogy && black .`
5. Drop a distilled `.preql` in `local_scripts/v4_evals/failing_cases/` if it
   reduces cleanly.
```
