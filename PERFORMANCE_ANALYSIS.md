# Performance Analysis: test_generate_queries_perf

**Date:** 2025-12-28 (Updated)
**Test:** `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_generate_queries_perf`
**Current Performance:** ~0.97s average per parse (target: <0.2s)
**Profiling Method:** cProfile with cumulative time analysis

## Executive Summary

The test parses the same simple query 5 times after loading 15 TPC-DS imports. Current average parse time is ~0.97s, which is ~5x slower than the 0.2s target.

### Improvements Made

The `generate_adhoc_graph` function was optimized with a pre-computed dependency graph approach:

| Metric | Original | After Optimization | Change |
|--------|----------|---------|--------|
| Average parse time | ~1.0s | 0.97s | **~3% faster** |
| `issubset` calls | 711,450 | 371,925 | **48% reduction** |
| `generate_adhoc_graph` cumulative | 1.32s | 1.16s | **12% reduction** |

The optimization replaced the O(n²) while loop with a topological traversal using pre-computed reverse dependencies. While this cut `issubset` calls in half, the overall parse time improvement is modest because other bottlenecks dominate.

---

## Current Top 10 Hotspots by Total Time (2025-12-28)

| Function | Total Time | Cumulative | Calls | Description |
|----------|------------|------------|-------|-------------|
| `build.py:1940(__build_concept)` | 0.215s | 1.10s | 43,905 | Concept building |
| `graph.py:573(add_nodes_from)` | 0.212s | 0.43s | 150 | NetworkX node addition |
| `graph_models.py:118(copy)` | 0.182s | 0.20s | 165 | ReferenceGraph.copy |
| `graph.py:975(add_edges_from)` | 0.181s | 0.62s | 120 | NetworkX edge addition |
| `main.py:303(model_construct)` | 0.171s | 0.50s | 31,686 | Pydantic model construction |
| `select_merge_node.py:108(create_pruned_concept_graph)` | 0.125s | 0.77s | 30 | Concept graph pruning |
| `env_processor.py:67(get_derivable_concepts)` | 0.111s | 0.19s | 6,150 | Derivable concept iteration |
| `node_merge_node.py:224(determine_induced_minimal_nodes)` | 0.107s | 1.29s | 30 | Steiner tree calculation |
| `copy.py:119(deepcopy)` | 0.095s | 0.20s | 77,698 | Deep copy operations |
| `env_processor.py:173(generate_adhoc_graph)` | 0.070s | 1.02s | 15 | Graph generation |

---

## Remaining Bottlenecks

### 1. `__build_concept` - Still the #1 Hotspot

**Location:** [build.py:1940](trilogy/core/models/build.py#L1940)
**Time:** 0.215s total, 1.10s cumulative
**Calls:** 43,905

**Status:** Unchanged from previous analysis. This remains the single largest hotspot.

**Improvement Opportunities:**
1. **Memoization cache:** Cache built concepts by address + grain
2. **Flyweight pattern:** Return existing instances instead of creating new ones
3. **Lazy building:** Defer concept building until actually needed

**Estimated Impact:** 40-50% reduction in build time

---

### 2. `create_pruned_concept_graph` - Graph Operations

**Location:** [select_merge_node.py:108](trilogy/core/processing/node_generators/select_merge_node.py#L108)
**Time:** 0.125s total, 0.77s cumulative
**Calls:** 30

**Status:** Improved from 0.255s to 0.125s total time (**51% faster**), still expensive due to:
- `ReferenceGraph.copy`: 0.182s (165 calls)
- Multiple iterations over edges/nodes

**Improvement Opportunities:**
1. **Copy-on-write graphs:** Only copy when modifications needed
2. **Maintain undirected version:** Cache undirected view
3. **Batch operations:** Remove edges in bulk

**Estimated Impact:** 30-40% reduction

---

### 3. `determine_induced_minimal_nodes` - Algorithm Overhead

**Location:** [node_merge_node.py:224](trilogy/core/processing/node_generators/node_merge_node.py#L224)
**Time:** 0.107s total, 1.29s cumulative
**Calls:** 30

**Status:** Steiner tree algorithm still expensive:
- `steiner_tree`: 0.111s (30 calls)
- `dijkstra`: 0.061s (60 calls) - **23% faster than before**

**Improvement Opportunities:**
1. **Cache undirected graph conversion**
2. **Simpler pathfinding for small node sets**
3. **Prune graph before running expensive algorithms**

**Estimated Impact:** 20-30% reduction

---

### 4. Pydantic Model Construction

**Time:** 0.171s in `model_construct`, 31,686 calls

**Status:** Unchanged overhead from Pydantic.

**Improvement Opportunities:**
1. **Use `__slots__`** on dataclass alternatives
2. **Consider plain dataclasses** for internal-only types
3. **Reduce model nesting**

**Estimated Impact:** 15-25% reduction

---

## Call Count Analysis (2025-12-28)

| Function | Calls | Notes |
|----------|-------|-------|
| `isinstance` | 1,978,796 | Type checking overhead |
| `dict.get` | 1,090,826 | Dictionary lookups |
| `getattr` | 786,233 | Attribute access |
| `dict.update` | 588,208 | Dictionary operations |
| `len` | 581,891 | Length checks |
| `set.issubset` | 371,925 | **Down from 711K (48% reduction)** |
| `Concept.address` | 302,599 | Property access (**13% reduction** from 348K) |
| `concept_to_node` | 67,635 | Graph node naming |
| `with_default_grain` | 21,510 | Grain normalization |

---

## Optimization Applied: Topological Dependency Traversal

The key change in `env_processor.py` was replacing the O(n²) while loop with a pre-computed dependency graph:

**Before:**
```python
# O(concepts * datasources * iterations) with up to 711K issubset calls
while not break_flag:
    break_flag = True
    for input_set, concept in basic_map:
        if input_set.issubset(complete_contains):  # Called 711K times
            ...
```

**After:**
```python
# Build dependency graph once
graph = build_basic_concept_graph(concepts)

# O(concepts) traversal using reverse dependencies
for derived in get_derivable_concepts(graph, available, already_present):
    ...
```

The new approach:
1. Pre-computes reverse dependencies (concept → dependents)
2. Starts from "root" concepts (those with all non-BASIC inputs)
3. Uses a queue to process only concepts whose dependencies are satisfied
4. Avoids redundant `issubset` checks by tracking processed concepts

---

## High-Impact Improvement Recommendations

### Priority 1: Cache Concept Building (NEW PRIORITY)

The `__build_concept` function is still the clear #1 bottleneck at 0.215s total time and 43,905 calls. Adding memoization would have the highest impact:

```python
# Example approach using address-based caching
_concept_cache: dict[tuple[str, str], BuildConcept] = {}

def __build_concept(concept, grain_key, ...):
    cache_key = (concept.canonical_address, grain_key)
    if cache_key in _concept_cache:
        return _concept_cache[cache_key]
    result = _do_build_concept(...)
    _concept_cache[cache_key] = result
    return result
```

### Priority 2: Reduce Graph Copy Operations

The `ReferenceGraph.copy` is now taking 0.182s (165 calls) - this has become more prominent. Consider:
1. Using view-based subgraphs where possible
2. Copy-on-write semantics
3. Reducing unnecessary copies

### Priority 3: Optimize Steiner Tree Calculation

The `determine_induced_minimal_nodes` function has high cumulative time due to graph algorithm overhead. Consider:
1. Caching the undirected graph conversion
2. Using BFS for small node sets instead of Dijkstra
3. Pre-pruning irrelevant nodes

---

## Profiling Command

```bash
.venv/Scripts/python.exe -m cProfile -s cumulative -o profile_output.prof \
    -m pytest tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_generate_queries_perf -v -s
```

## Next Steps

1. **Implement concept building cache** (highest impact)
2. **Optimize graph copy operations** in `create_pruned_concept_graph`
3. **Cache undirected graph** in Steiner tree calculations
4. Profile again to measure improvement
