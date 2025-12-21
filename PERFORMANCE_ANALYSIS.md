# Performance Analysis: test_generate_queries_perf

**Date:** 2025-12-21
**Test:** `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_generate_queries_perf`
**Current Performance:** ~1.0s average per parse (target: <0.2s)
**Profiling Method:** cProfile with cumulative time analysis

## Executive Summary

The test parses the same simple query 5 times after loading 15 TPC-DS imports. Current average parse time is ~1.0s, which is 5x slower than the 0.2s target. The major bottlenecks are:

1. **Graph Generation** (`generate_adhoc_graph`): 1.32s cumulative - excessive `issubset` calls
2. **Concept Building** (`__build_concept`): 0.91s cumulative - called 42,630 times
3. **Pruned Graph Creation** (`create_pruned_concept_graph`): 0.81s cumulative - expensive graph operations
4. **Steiner Tree / Dijkstra Algorithms**: 0.27s cumulative - graph pathfinding overhead
5. **NetworkX Operations**: Significant overhead in `to_undirected`, `copy`, `add_nodes_from`

---

## Top 10 Hotspots by Total Time (time spent in function itself)

| Function | Total Time | Calls | Description |
|----------|------------|-------|-------------|
| `env_processor.py:70(generate_adhoc_graph)` | 0.382s | 15 | Graph generation with 711K issubset calls |
| `select_merge_node.py:107(create_pruned_concept_graph)` | 0.270s | 30 | Concept graph pruning |
| `build.py:1906(__build_concept)` | 0.236s | 42,630 | Concept building |
| `graph.py:573(add_nodes_from)` | 0.213s | 150 | NetworkX node addition |
| `graph.py:975(add_edges_from)` | 0.171s | 120 | NetworkX edge addition |
| `pydantic/main.py:303(model_construct)` | 0.169s | 31,422 | Pydantic model construction |
| `node_merge_node.py:224(determine_induced_minimal_nodes)` | 0.103s | 30 | Steiner tree calculation |
| `copy.py:119(deepcopy)` | 0.094s | 77,698 | Deep copy operations |
| `digraph.py:666(add_edge)` | 0.089s | 64,545 | NetworkX edge addition |
| `graph_models.py:112(copy)` | 0.075s | 165 | ReferenceGraph copy |

---

## Detailed Analysis

### 1. `generate_adhoc_graph` - Critical Hotspot

**Location:** [env_processor.py:70](trilogy/core/env_processor.py#L70-L131)
**Time:** 0.382s total, 1.32s cumulative
**Calls:** 15 (once per query)

**Problem:** The nested while loop at lines 102-110 performs 711,450 `issubset` calls:

```python
while not break_flag:
    break_flag = True
    for input_set, concept in basic_map:
        if concept.canonical_address not in seen:
            if input_set.issubset(complete_contains):  # <-- 711K calls!
                break_flag = False
                seen.add(concept.canonical_address)
                eligible.append(concept)
                complete_contains.add(concept.canonical_address)
```

**Root Cause:** For each datasource (128 datasources * 15 queries), the loop iterates over all BASIC concepts repeatedly until no new concepts are found. The `issubset` check happens for every (concept, datasource) pair, every iteration.

**Improvement Opportunities:**
1. **Pre-compute closure:** Build the transitive closure of BASIC concepts once, then use direct lookup
2. **Index by first element:** Create an inverted index from `input_set` elements to concepts, reducing O(n) to O(1) lookups
3. **Incremental update:** When a concept is added, only check concepts that depend on it rather than all concepts
4. **Cache `basic_map`:** Currently rebuilt for each datasource; compute once and reuse

**Estimated Impact:** 50-70% reduction in `generate_adhoc_graph` time

---

### 2. `create_pruned_concept_graph` - Graph Operations

**Location:** [select_merge_node.py:107](trilogy/core/processing/node_generators/select_merge_node.py#L107-L228)
**Time:** 0.270s total, 0.81s cumulative
**Calls:** 30

**Problem:** Multiple expensive operations:
- `g.copy()` at line 117: 0.075s (165 calls to `ReferenceGraph.copy`)
- `g.to_undirected()` at line 198: 0.178s (30 calls)
- `nx.all_neighbors()` at line 179: 0.075s (58,800 calls)
- Iterating over all edges/nodes multiple times

**Improvement Opportunities:**
1. **Lazy undirected view:** Use `nx.Graph(g.edges())` or maintain undirected version
2. **Reduce copies:** Modify in-place where safe, or use view-based subgraphs
3. **Cache neighbor lookups:** Pre-compute `all_neighbors` once
4. **Batch edge operations:** Remove edges in bulk rather than individually

**Estimated Impact:** 30-40% reduction

---

### 3. `__build_concept` - Excessive Calls

**Location:** [build.py:1906](trilogy/core/models/build.py#L1906)
**Time:** 0.236s total, 0.91s cumulative
**Calls:** 42,630 (many redundant)

**Problem:** Concepts are rebuilt many times during environment materialization. Related calls:
- `_build_concept`: 42,630 calls
- `_build_column_assignment`: 18,645 calls
- `get_concept_arguments`: 92,205 calls
- `with_default_grain`: 21,240 calls

**Improvement Opportunities:**
1. **Memoization cache:** Cache built concepts by address + grain + source
2. **Flyweight pattern:** Return existing instances instead of creating new ones
3. **Lazy building:** Defer concept building until actually needed
4. **Reduce recursion:** The recursive `concept_arguments` traversal creates deep call stacks

**Estimated Impact:** 40-50% reduction in build time

---

### 4. `determine_induced_minimal_nodes` - Algorithm Overhead

**Location:** [node_merge_node.py:224](trilogy/core/processing/node_generators/node_merge_node.py#L224-L345)
**Time:** 0.103s total, 1.27s cumulative
**Calls:** 30

**Problem:** Uses expensive graph algorithms:
- `nx.to_undirected(G).copy()` at line 232
- `nx.multi_source_dijkstra_path` at line 287: 0.021s
- `ax.steinertree.steiner_tree` at line 297: 0.114s

**Improvement Opportunities:**
1. **Cache undirected graph:** Don't convert on every call
2. **Simpler pathfinding:** For small node sets, BFS may be faster than Dijkstra
3. **Alternative to Steiner tree:** Consider spanning tree approximation
4. **Prune graph first:** Remove irrelevant nodes before running expensive algorithms

**Estimated Impact:** 20-30% reduction

---

### 5. Pydantic/Model Construction Overhead

**Time:** 0.169s in `model_construct`, 0.426s total in pydantic
**Calls:** 31,422 `model_construct` calls

**Problem:** Heavy use of Pydantic models with frequent instantiation:
- `with_namespace`: 7,644 calls creating new Concept instances
- `reference`: 3,654 calls
- Various `model_construct` calls for namespacing

**Improvement Opportunities:**
1. **Use `__slots__`:** Reduce memory and construction overhead
2. **Consider dataclasses:** For internal-only types, dataclasses are faster
3. **Reduce model nesting:** Flatten structures where possible
4. **Lazy field computation:** Use `@property` with caching for derived fields

**Estimated Impact:** 15-25% reduction in model-related overhead

---

### 6. `concept_to_node` - String Operations

**Location:** [graph_models.py:85](trilogy/core/graph_models.py#L85-L95)
**Time:** 0.041s total
**Calls:** 67,035

**Problem:** String formatting on every call:
```python
r = f"c~{input.canonical_address}@{input.grain.str_no_condition}"
```

**Improvement Opportunities:**
1. **Already using stash:** Good! But stash lookup could be faster
2. **Pre-compute on concept:** Store node name as concept attribute
3. **Intern strings:** Python string interning for repeated values

**Estimated Impact:** 10-15% reduction in graph operations

---

### 7. ReferenceGraph.copy - Deep Copy Overhead

**Location:** [graph_models.py:112](trilogy/core/graph_models.py#L112-L121)
**Time:** 0.075s total
**Calls:** 165

**Problem:** Manual dictionary copying on every graph copy:
```python
g._node.update(self._node)
g._adj.update({k: dict(v) for k, v in self._adj.items()})
g._pred.update({k: dict(v) for k, v in self._pred.items()})
```

**Improvement Opportunities:**
1. **Copy-on-write:** Only copy when modifications are needed
2. **View-based subgraphs:** Use `G.subgraph(nodes).copy()` which is optimized
3. **Avoid deep copy of adjacency:** Share immutable parts

**Estimated Impact:** 15-20% reduction in graph copy time

---

## High-Impact Improvement Recommendations

### Priority 1: Fix `generate_adhoc_graph` issubset loop

```python
# Current: O(datasources * concepts * iterations)
# Proposed: O(datasources * concepts) with early termination

# Pre-compute which concepts depend on which
dependency_map = defaultdict(set)  # concept -> concepts that could be added if this exists
for input_set, concept in basic_map:
    for addr in input_set:
        dependency_map[addr].add(concept)

# Then use a queue-based approach instead of while loop
queue = deque(initial_concepts)
while queue:
    added = queue.popleft()
    for dependent in dependency_map.get(added.canonical_address, []):
        if dependent.canonical_address not in seen:
            if input_set_for[dependent].issubset(complete_contains):
                seen.add(dependent.canonical_address)
                eligible.append(dependent)
                complete_contains.add(dependent.canonical_address)
                queue.append(dependent)
```

### Priority 2: Cache concept building

```python
# Add memoization to _build_concept
@lru_cache(maxsize=None)
def _build_concept_cached(address: str, grain_key: str, ...):
    return __build_concept(...)
```

### Priority 3: Optimize graph operations

1. Maintain undirected version alongside directed graph
2. Use `nx.freeze()` for immutable graphs to enable optimizations
3. Batch node/edge operations where possible

### Priority 4: Reduce Pydantic overhead

1. Profile `model_construct` vs `__init__` paths
2. Consider `model_config = ConfigDict(extra='forbid', frozen=True)` for immutability benefits
3. Use slots where applicable

---

## Call Count Analysis

| Function | Calls | Notes |
|----------|-------|-------|
| `isinstance` | 1,984,351 | Type checking overhead |
| `dict.get` | 1,148,543 | Dictionary lookups |
| `getattr` | 784,684 | Attribute access |
| `set.issubset` | 712,245 | **711K from generate_adhoc_graph** |
| `dict.update` | 583,660 | Dictionary operations |
| `len` | 581,047 | Length checks |
| `Concept.address` | 348,241 | Property access |
| `list.append` | 332,530 | List operations |

---

## Profiling Command

```bash
.venv/Scripts/python.exe -m cProfile -s cumulative -o profile_output.prof \
    -m pytest tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_generate_queries_perf -v -s
```

## Next Steps

1. Implement Priority 1 fix for `generate_adhoc_graph`
2. Add concept building cache
3. Profile again to measure improvement
4. Iterate on remaining hotspots
