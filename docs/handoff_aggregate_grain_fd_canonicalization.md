# Handoff: an abstract aggregate's identity depends on how its grain is *spelled*

Status: fixed at the author-side grain (2026-09-18, PR #697), with the sibling duplicate-name bug it widened, see the end.

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

`fd_minimal_addresses` (`trilogy/parsing/common.py`) is the author twin of the build-side key-hierarchy fold (`_key_reduces_to` in `concepts_to_build_grain_concepts`): a component drops when some declared key set of it reduces, transitively, to the rest.

- **Chain links are ROOT concepts only** (`_declared_keys`): a KEY's `effective_keys`, a PROPERTY's `keys`. A derived concept's keys can be conditional (an empty-grain FILTER virtual, `keys_are_conditional_fd`), so nothing chains through one.
- **Merge identity**: a KEY that a *global, non-partial* `merge s into t` equates to `s` is determined by whatever determines `s` (`Environment.equal_merge_sources`). Partial merges (`into ~t`) and statement-scoped joins hold only on matched rows and contribute nothing; that is the scoped-join equality that caused the LEFT->FULL flip when it was used for group elision, and why this does not read `environment.domain_graph`.

With the names equal, the candidate check in `_materialized_root_addresses` passes and #695's `_scan_rows_at_grain` FD gate finally gets consulted, which is what it was written for.

### Why not reduce the grain itself (tried first, reverted)

The first cut reduced the select grain in `concepts_to_grain_concepts_ordered`. It is the cleaner model (identity and `by` stay one thing, and two spellings in one statement collapse to one GROUP BY) and it fixed reuse, but CI came back with 9 failures, all real. An implicit `sum(x)` beside a determined column is the most common aggregate spelling there is, and shrinking its pin moves all of those queries off "group by key and property" onto "aggregate at the key, re-attach the property", a path with latent gaps that the explicit `by` spelling already hits on main:

- **Wrong rows** (5x `join_matrix/test_subset_presence_probe.py`): `merge st into store_id; where yr = 2001 select st, sname, sum(amt) by st` returns `(40, 'S40', None)` on main. The plan uses the `stores` dimension as the join spine and applies the WHERE only inside the aggregate, so a store with no 2001 sales leaks in. The implicit spelling was protected only by its wider pin. **Still open on main.**
- **`UnresolvableQueryException: keyless join`** (2x `tpc_ds_duckdb/test_partial_key_assembly_shapes.py`): two-fact aggregates at `{item.sk, ticket_number}` cannot re-attach `customer.current_address.state` once the customer key is off the grain.
- q64 SQL grew 16.7k -> 25.7k chars; one semi-join pushdown shape lost.

All 9 pass with the identity-only seam, and a query no summary answers plans exactly as on main. If those planner gaps are closed later, reducing the pin itself becomes viable and buys the sibling-GROUP-BY dedupe.

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
- **Binding** (`create_select_node_candidate`): the scan was rebuilt with one concept per node, so the other requested name had no output. Callers now pass the requested concepts and the scan emits every one sharing a canonical with a node it reads (`_canonical_siblings`).

Either alone is not enough: discovery alone still drops the second name (silently, in the single-table case); binding alone never reaches the summary in a multi-table plan.
