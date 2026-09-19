# Handoff: an abstract aggregate's identity depends on how its grain is *spelled*

Status: fixed at the aggregate's canonical name (2026-09-18, PR #697), with the sibling duplicate-name bug it widened, see the end. The lineage `by` followed on 2026-09-19, see "Follow-up".

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

`Factory._identity_lineage` (`trilogy/core/models/build.py`, since renamed `_fd_minimal_lineage`) reduces an aggregate's `by` to its FD-minimal key set. In #697 this was **only for `generate_concept_name`** and the lineage kept its full `by` (option (b) of the original proposal); the follow-up below applies it to the lineage itself.

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

## Follow-up: the lineage carries the minimal `by` too (2026-09-19)

Name and lineage are now one grouping. `Factory._fd_minimal_lineage` reduces the built aggregate's `by`, the concept's grain is that `by`, and the canonical name hashes the lineage as is: no second, name-only view of the aggregate. A ROLLUP/CUBE `by` is left alone, since each key there is a subtotal level and not only a grouping key (`test_rollup_by_is_not_reduced`).

The wide pin was doing the planner three favours by accident. Each is now a planner rule with its own proof obligation, so the plan is a function of the query's meaning and the explicit `by` spelling (which never had the wide pin, and crashed or double-scanned on main) gets them too.

### 1. A row-preserving aggregate hosts what its whole grain determines

The blocker. `test_partial_grain_star_under_not_null` scanned `store_sales` twice: under the wide pin `state` was a grouping key and rode the aggregate's fact read; under the minimal `by` it is an output the composite grain `(item, ticket)` determines, so it peeled to a second read of the only table keyed by that grain, the fact itself. Main had the same defect wherever the one-step rule already dropped the column:

```
select ss.item.sk, ss.ticket_number, ss.quantity, sum(ss.net_paid);
-- main: store_sales INNER JOIN store_sales on (item, ticket). Now: one scan.
```

`concept_graph._host_outputs_on_row_preserving_aggregates` widens the aggregate's *physical* grouping grain (`ConceptAttrs.grain_components`, the same seam `_aggregate_axis_members` widens) with a select output `X` when:

- the aggregate is **row-preserving**: its grain determines its whole `aggregate_input_grain`, so the input rows are already one per group and the GROUP BY reduces nothing;
- the **whole grain** determines `X` and no proper subset does (`_whole_grain_determines`): `X` belongs to the grain's own row (a fact property, a dimension behind a foreign key the fact binds off its grain). A column one key alone determines (`brand` by `item.sk`) still joins from that key's table after the fact;
- the grain **covers** `X`, not only determines it (`DomainGraph.covers`). An FD says `X` is unique per grain row, and a `~` binding proves that as well as any other: `order_item.id -> ~user.id` is true. Hosting reads `X` off the fact's rows, so those rows must also hold every value `X` owes, and a `~` binding says they do not (a user who never ordered has no `order_items` row). `covers` is `determines` that refuses an FD whose dependent some table binds partially beside the determinants, and so anything reached through it (`user.state` via `~user.id`). A first cut asked `determines` and then excluded KEYs to make thelook q19 pass; that was a proxy, and it would have hosted `user.state`. Hosted anyway, q19's rows stay right but its aggregate input becomes the whole extension spine, no longer one row per `id`, and the FINAL stitch goes null-safe. Under `covers` q19 hosts the completely bound `order.id` and refuses the two `~` keys: one `order_items` re-join fewer than main, no null-safe stitch;
- `X` is a row scalar (ROOT/BASIC/CONSTANT lineage only).

From there everything downstream is main's wide-pin path, which is why the blocker query renders byte-identical SQL to main. An aggregate that truly reduces (`sum(qty)` over the finer `sale_lines`) keeps the peel and joins the dimension after its rows collapse. It also covers single-key fact grains (`select order_id, region, total`), which TPC-DS cannot show: every fact there has a composite grain.

This is the narrow form of the "host-first" option. The optimizer option (spine-subsumption CTE merge) is not built and nothing now needs it.

### 2. ROOT co-sourcing follows the FD chain, not one step

`select ss.customer.sk, upper(state), sum(net_paid)` crashed with a keyless join under the minimal `by` (and on main under an explicit `by`). `partition_roots` splits ROOTs the concept graph never relates; `_property_key_pairs` related a property to its *declared* key only, the same one-step pattern as the original bug. `state`'s key is `address.sk`, which the query never names, and the wide pin had been papering over it with a `by` lineage edge. `_stamp_determining_key_roots` records the KEY roots that determine such a ROOT through the environment closure, and the pair rule reads them.

### 3. A scalar over a peeled dimension carries the peel key

Once co-sourced, `state` peels to a `customer -> address` scan keyed by `customer.sk`, but `upper(state)` stayed pinned at `address.sk`, so it could not carry the key to FINAL. `_anchor_scalars_to_dim_peel_key` re-anchors a BASIC that reads only a dim-peel scan to that scan's key. Relatedly, `_projected_scalar_root_args` no longer peels the arg of a scalar that is itself a grouping key (read before the GROUP BY, not after): `select order_id, upper(region), sum(amount)` crashed on main.

### Two spellings, two populations: the model's doing, not the planner's

```
select ss.customer.sk, ss.customer.first_name, sum(ss.net_paid);            -- main: 1001 rows (FULL JOIN customer)
select ss.customer.sk, ss.customer.current_address.state, sum(ss.net_paid); -- main:  915 rows (one GROUP BY over the fact)
```

`first_name` is one FD step from the key so it never entered main's pin; `state` is two, so it did. Under the minimal `by` both return 1001. That reads like a semantics change and is not one: `store_sales` binds `?customer.sk` with no `~`, which claims the fact holds the whole customer domain, and under that claim the two plans are the same rows. The data has 86 customers with no store sale. With a truthful `~?customer.sk`, main's two-hop plan returns 1001 as well (checked on sf0.01). Main's planner was principled; the model is not.

`trilogy integration` already says so. Concept validation (`validate_multi_datasource_concept`) reports `ss.customer.sk is missing values in datasource store_sales (max 1000, datasource 914) but is not marked as partial`, and the same for some twenty other foreign keys of the store channel alone (dates, times, demographics, addresses, `return_customer`). Nobody runs it on the benchmark model. Tagging them is its own change: every `~` key owes extension rows, and the TPC-DS reference SQL is inner joins.

Locks: `test_fact_grain_aggregate_reads_the_fact_once` and `test_scalar_over_dimension_two_hops_off_the_grouping_key` (TPC-DS), `tests/discovery/test_dim_peel_composite_grain_axis.py` (hosting, the reducing-aggregate peel that keeps #697's keyless-join fix exercised, both scalar shapes), and `test_lineage_and_grain_carry_the_minimal_by`.

How this lands against the concerns in the original proposal:

1. **Seam.** Option (b). The divergence it warned about is narrow: same-canonical concepts with different `by` lists are FD-equivalent groupings, and the consumers that treat a canonical as one expression (the root scan) were fixed below to emit every name.
2. **Which minimal set.** Chain reduction over a key DAG has a unique result (the undetermined sources), independent of iteration order. Only a key cycle (`a <-> b`) is order-dependent, and there `sorted()` addresses decide; the datasource-column build and the query build share addresses, so both sides agree.
3. **Partial keys.** Sidestepped for merges (partial merges excluded), and moot for the GROUP BY since it is not touched. Rows are locked for plain and `~` bindings of the middle key, extension rows included (`test_partial_determined_key_rows`).
4. **Blast radius.** In #697 only canonical names moved. With the follow-up the `by` moves too, for aggregates beside a transitively-determined column; see the follow-up for where plans and one row population change. Fingerprints hash authored lineage, so they are untouched.
5. **The GROUP BY itself.** Untouched in #697; FD-minimal since the follow-up, re-widened by the planner only for a row-preserving aggregate.

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
