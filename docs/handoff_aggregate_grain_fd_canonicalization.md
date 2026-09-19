# Handoff: an abstract aggregate's identity depends on how its grain is *spelled*

Status: fixed at the aggregate's canonical name (2026-09-18, PR #697), with the sibling duplicate-name bug it widened, see the end.

Locked by `tests/discovery/test_aggregate_grain_fd_canonical.py` and the former strict xfail `test_merged_key_grain_reads_bound_derived.py::test_summary_reads_at_fd_equivalent_grain[beside_surviving_key_spelling]`.

## What happened

```
merge org.state_code into state.code;          # a property of org.code, respelled as a KEY
auto launch_count <- count(id);
datasource org_summary (Code: org.code, N: launch_count) grain (org.code) ...

select org.code, org.state_code, launch_count;  # read org_summary
select org.code, state.code,     launch_count;  # recomputed from base
```

An abstract aggregate has no grain of its own: the Factory resolves it at the select grain (`__build_concept` -> `_abstract_resolution_grain` -> `get_select_grain_and_keys`), that grain becomes the `by` list of its lineage, and the canonical name hashes the lineage. **The grain is part of the aggregate's identity**, so every canonical-keyed lookup (materialized roots, prebuilt-aggregate reuse, sibling dedupe) saw two spellings of one grouping as two concepts.

## Root cause: the author-side grain is only one FD step deep

The merge case was one instance of a wider, much more common gap. The select grain is already FD-minimized on the author side, by `concept_is_relevant`: a property whose keys are present drops out, as does a KEY whose `effective_keys` (FK-derived) are present. That check is **one step**, and it tests against the *unreduced* component list, so the pin depends on whether the middle link was named:

| components | pinned grain |
|---|---|
| `order_id, customer_id, region` | `order_id` |
| `order_id, region` | `order_id, region` |
| `org.code, org.state_code` | `org.code` |
| `org.code, state.code` (merge target) | `org.code, state.code` |

`select order_id, region, total` recomputed from base beside an `order_id`-grain summary that `select order_id, customer_id, region, total` read. That also answers the open question in the original write-up: the `org.state_code` spelling pinned at `Grain<org.code>` because a global merge does not rewrite the author environment, so `org.state_code` is still a PROPERTY of `org.code` there and the one-step rule drops it. `state.code` is a bare KEY with no keys, so it stayed.

## The fix: minimize the grain that feeds the hash, not the grain

`Factory._identity_lineage` (`trilogy/core/models/build.py`) reduces an aggregate's `by` to its FD-minimal key set **only for `generate_concept_name`**. The lineage keeps its full `by`. This is option (b) of the original proposal.

The reduction is `DomainGraph.determines` over the build's full graph (`assemble_full_graph`), the same closure the planner already trusts to say "grouping by `{grain, component}` reduces to `{grain}`" (`grain_utility`). A `by` member drops when the rest determine it. No second FD engine:

- **Key chains** come from the graph's declared FD edges (`effective_keys`), closed transitively.
- **Merge identity** comes from its EQUAL classes. A *global, non-partial* `merge s into t` declares EQUAL, so whatever determines `s` determines `t`. A partial merge declares SUBSET and a statement join SUBSET or INCOMPARABLE; neither enters an equivalence class, so they contribute nothing (that scoped-join equality is what caused the LEFT->FULL flip when it was used for group elision). It also makes the name a function of the environment alone: a datasource column and a query concept hash alike whatever joins the statement carries.

With the names equal, the candidate check in `_materialized_root_addresses` passes and #695's `_scan_rows_at_grain` FD gate finally gets consulted, which is what it was written for.

### Why not reduce the grain itself (tried first, reverted)

The first cut reduced the select grain in `concepts_to_grain_concepts_ordered`. It is the cleaner model (identity and `by` stay one thing, and two spellings in one statement collapse to one GROUP BY) and it fixed reuse, but CI came back with 9 failures, all real. An implicit `sum(x)` beside a determined column is the most common aggregate spelling there is, and shrinking its pin moves all of those queries off "group by key and property" onto "aggregate at the key, re-attach the property", a path with latent gaps that the explicit `by` spelling already hits on main:

- **Wrong rows** (5x `join_matrix/test_subset_presence_probe.py`): `merge st into store_id; where yr = 2001 select st, sname, sum(amt) by st` returned `(40, 'S40', None)` on main: the `stores` scan cannot apply the WHERE, and it was FULL-joined to the filtered aggregate, so a store with no 2001 sales leaked in. Not specific to the explicit `by`: `select store_id, sname, sum(amt)` pins `{store_id}` and leaked too. **Fixed in this PR** (`MergeNode` join proofs): the filtered-branch narrowing (`tighten_join_for_filtered_branch`) stood down for every `outer_relation_keys()` member, which includes EQUAL merge keys. Its veto is for authored union/full joins, which declare row intent; a `merge` declares identity over domains that are equal *unfiltered*, so the side that applied the WHERE drives. Locked by `tests/join_matrix/test_equal_merge_filtered_aggregate_at_key.py`.
- **`UnresolvableQueryException: keyless join`** (2x `tpc_ds_duckdb/test_partial_key_assembly_shapes.py`), also reachable on main with an explicit `by`: `select item_id, ticket, region, sum(amount) by item_id, ticket` where `sales` is keyed `(item_id, ticket)` and binds `customer_id` off the grain. The peeled `region` scan kept a merge key only when that *single* key FD-determined an output (`_relevant_root_preserve_keys`); a composite grain determines it only jointly, so the scan carried no key. **Fixed in this PR**: when the merge grain jointly determines an output no single key does, the scan keeps the whole grain, the assembly twin of the composite peel (`_composite_determining_grain`). Locked by `tests/discovery/test_dim_peel_composite_grain_axis.py` and the by-key parametrization of `test_partial_grain_with_customer_dim` (row-identical to the implicit spelling on TPC-DS sf0.01).
- q64 SQL grew 16.7k -> 25.7k chars and one semi-join pushdown shape was lost. Both belonged to the select-grain seam: reducing only the aggregate's `by` in the Factory moves neither.

All 9 pass with the identity-only seam, and a query no summary answers plans exactly as on main.

**Where reducing the lineage `by` stands now** (Factory seam: `build_lineage = _identity_lineage(...)`, `final_grain` from its `by`). With both fixes above it is row-correct across TPC-DS, TPC-H, thelook and the engine suites, and plan-neutral on 98 of 99 TPC-DS queries (q23 shrinks 8%). One blocker left, and it is plan quality, not correctness: `test_partial_grain_star_under_not_null` scans `store_sales` twice, once for the aggregate and once to carry the off-grain foreign key to the dimension, where the wide pin is one star and one GROUP BY. That is the host-first gap: an aggregate whose grain IS its fact's grain should host the dimension join instead of re-reading the fact. Close it and the lineage can carry the same minimal `by` the name hashes.

How this lands against the concerns in the original proposal:

1. **Seam.** Option (b). The divergence it warned about is narrow: same-canonical concepts with different `by` lists are FD-equivalent groupings, and the consumers that treat a canonical as one expression (the root scan) were fixed below to emit every name.
2. **Which minimal set.** Chain reduction over a key DAG has a unique result (the undetermined sources), independent of iteration order. Only a key cycle (`a <-> b`) is order-dependent, and there `sorted()` addresses decide; the datasource-column build and the query build share addresses, so both sides agree.
3. **Partial keys.** Sidestepped for merges (partial merges excluded), and moot for the GROUP BY since it is not touched. Rows are locked for plain and `~` bindings of the middle key, extension rows included (`test_partial_determined_key_rows`).
4. **Blast radius.** Only canonical names move, and only for aggregates beside a transitively-determined column. Fingerprints hash authored lineage, so they are untouched.
5. **The GROUP BY itself.** Untouched.

## Also fixed: two names for one materialized aggregate (pre-existing on main)

Putting more spellings on one canonical widens a bug main already had: once two outputs share a canonical *and* a datasource materializes it, only one name survived the root scan.

```
select order_id, total, sum(amount) by order_id as explicit;               # main: `total` silently missing from the result
select order_id, customer_id, total, sum(amount) by order_id as explicit;  # main: "Missing source reference to local.amount"
select order_id, region, total, sum(amount) by order_id as explicit;       # main: rows (6.0 beside 96.0); with the identity fix alone, a render error
```

The reference graph keys a concept node by canonical address, and `BuildEnvironment.canonical_concepts` keeps one concept per canonical. Two seams read the winner's *address* where they meant the expression, and both had to change:

- **Discovery** (`source_planning._local_concept_nodes_for_datasource`): the "this table binds it as a column" guard compared the winner's address (`explicit`) to the summary's column (`total`), so in a multi-table plan the summary scan lost the aggregate node. It now compares canonicals (`_datasource_binds_canonical`).
- **Binding** (`subgraph_concepts`): a subgraph's nodes were resolved back to one concept per node, so the other requested name had no output. The graph-driven callers now resolve a subgraph against their request and get every requested name sharing a canonical with a node; the scan builders take concepts, not node names (two callers were a pure concept -> node name -> concept round trip).

Either alone is not enough: discovery alone still drops the second name (silently, in the single-table case); binding alone never reaches the summary in a multi-table plan.
