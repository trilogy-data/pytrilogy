# Performance Analysis: test_generate_queries_perf

**Date:** 2025-12-21 (Updated)
**Test:** `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_generate_queries_perf`
**Current Performance:** ~0.97s average per parse (target: <0.2s)
**Profiling Method:** cProfile with cumulative time analysis

## Executive Summary

The test parses the same simple query 5 times after loading 15 TPC-DS imports. Current average parse time is ~0.97s, which is ~5x slower than the 0.2s target.

### Improvements Made

The `generate_adhoc_graph` function was optimized with a pre-computed dependency graph approach:

| Metric | Previous | Current | Change |
|--------|----------|---------|--------|
| Average parse time | ~1.0s | 0.97s | **~3% faster** |
| `issubset` calls | 711,450 | 371,925 | **48% reduction** |
| `generate_adhoc_graph` cumulative | 1.32s | 1.16s | **12% reduction** |

The optimization replaced the O(n²) while loop with a topological traversal using pre-computed reverse dependencies. While this cut `issubset` calls in half, the overall parse time improvement is modest because other bottlenecks dominate.

---

## Current Top 10 Hotspots by Total Time

| Function | Total Time | Cumulative | Calls | Description |
|----------|------------|------------|-------|-------------|
| `build.py:1911(__build_concept)` | 0.260s | 1.00s | 42,630 | Concept building |
| `select_merge_node.py:107(create_pruned_concept_graph)` | 0.255s | 0.77s | 30 | Concept graph pruning |
| `graph.py:573(add_nodes_from)` | 0.218s | 0.43s | 150 | NetworkX node addition |
| `graph.py:975(add_edges_from)` | 0.167s | 0.58s | 120 | NetworkX edge addition |
| `main.py:303(model_construct)` | 0.166s | 0.42s | 31,422 | Pydantic model construction |
| `env_processor.py:28(build_basic_concept_graph)` | 0.136s | 0.16s | 15 | Pre-compute dependency graph |
| `env_processor.py:67(get_derivable_concepts)` | 0.109s | 0.19s | 6,150 | Derivable concept iteration |
| `node_merge_node.py:224(determine_induced_minimal_nodes)` | 0.100s | 1.24s | 30 | Steiner tree calculation |
| `copy.py:119(deepcopy)` | 0.092s | 0.20s | 77,698 | Deep copy operations |
| `env_processor.py:169(generate_adhoc_graph)` | 0.068s | 1.16s | 15 | Graph generation |

---

## Remaining Bottlenecks

### 1. `__build_concept` - Still the #1 Hotspot

**Location:** [build.py:1911](trilogy/core/models/build.py#L1911)
**Time:** 0.260s total, 1.00s cumulative
**Calls:** 42,630

**Status:** Unchanged from previous analysis. This remains the single largest hotspot.

**Improvement Opportunities:**
1. **Memoization cache:** Cache built concepts by address + grain
2. **Flyweight pattern:** Return existing instances instead of creating new ones
3. **Lazy building:** Defer concept building until actually needed

**Estimated Impact:** 40-50% reduction in build time

---

### 2. `create_pruned_concept_graph` - Graph Operations

**Location:** [select_merge_node.py:107](trilogy/core/processing/node_generators/select_merge_node.py#L107)
**Time:** 0.255s total, 0.77s cumulative
**Calls:** 30

**Status:** Still expensive due to:
- `g.copy()`: 0.070s (165 calls to `ReferenceGraph.copy`)
- `to_undirected()`: 0.181s (60 calls)
- Multiple iterations over edges/nodes

**Improvement Opportunities:**
1. **Copy-on-write graphs:** Only copy when modifications needed
2. **Maintain undirected version:** Cache undirected view
3. **Batch operations:** Remove edges in bulk

**Estimated Impact:** 30-40% reduction

---

### 3. `determine_induced_minimal_nodes` - Algorithm Overhead

**Location:** [node_merge_node.py:224](trilogy/core/processing/node_generators/node_merge_node.py#L224)
**Time:** 0.100s total, 1.24s cumulative
**Calls:** 30

**Status:** Steiner tree algorithm still expensive:
- `steiner_tree`: 0.110s (30 calls)
- `dijkstra`: 0.079s (60 calls)

**Improvement Opportunities:**
1. **Cache undirected graph conversion**
2. **Simpler pathfinding for small node sets**
3. **Prune graph before running expensive algorithms**

**Estimated Impact:** 20-30% reduction

---

### 4. Pydantic Model Construction

**Time:** 0.166s in `model_construct`, 31,422 calls

**Status:** Unchanged overhead from Pydantic.

**Improvement Opportunities:**
1. **Use `__slots__`** on dataclass alternatives
2. **Consider plain dataclasses** for internal-only types
3. **Reduce model nesting**

**Estimated Impact:** 15-25% reduction

---

## Call Count Analysis

| Function | Calls | Notes |
|----------|-------|-------|
| `isinstance` | 1,984,290 | Type checking overhead |
| `dict.get` | 1,152,804 | Dictionary lookups |
| `getattr` | 784,711 | Attribute access |
| `set.issubset` | 371,925 | **Down from 711K (48% reduction)** |
| `dict.update` | 583,662 | Dictionary operations |
| `len` | 581,060 | Length checks |
| `Concept.address` | 348,241 | Property access |
| `list.append` | 342,091 | List operations |
| `concept_to_node` | 67,035 | Graph node naming |
| `with_default_grain` | 21,240 | Grain normalization |

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

The `__build_concept` function is now the clear #1 bottleneck at 0.26s total time and 42,630 calls. Adding memoization would have the highest impact:

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

The `create_pruned_concept_graph` function copies graphs frequently. Consider:
1. Using view-based subgraphs where possible
2. Maintaining a cached undirected version
3. Copy-on-write semantics

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
