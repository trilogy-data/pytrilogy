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

## Root cause: the author-side grain was only one FD step deep

The merge case was one instance of a wider, much more common gap. The select grain is already FD-minimized on the author side, by `concept_is_relevant`: a property whose keys are present drops out, as does a KEY whose `effective_keys` (FK-derived) are present. That check is **one step**, and it tests against the *unreduced* component list, so the answer depended on whether the middle link was named:

| components | grain before | after |
|---|---|---|
| `order_id, customer_id, region` | `order_id` | `order_id` |
| `order_id, region` | `order_id, region` | `order_id` |
| `org.code, org.state_code` | `org.code` | `org.code` |
| `org.code, state.code` (merge target) | `org.code, state.code` | `org.code` |

`select order_id, region, total` recomputed from base beside an `order_id`-grain summary that `select order_id, customer_id, region, total` read. That also answers the open question in the original write-up: the `org.state_code` spelling pinned at `Grain<org.code>` because a global merge does not rewrite the author environment, so `org.state_code` is still a PROPERTY of `org.code` there and the one-step rule drops it. `state.code` is a bare KEY with no keys, so it stayed.

The build side never had this problem: `concepts_to_build_grain_concepts` already folds the whole FK chain (`_key_reduces_to`). The aggregate is pinned from the *author* grain, before a `BuildEnvironment` exists, so it never saw that reduction.

## The fix

`concepts_to_grain_concepts_ordered` (`trilogy/parsing/common.py`) now runs the author twin of the build-side key-hierarchy reduction: a component drops when some declared key set of it reduces, transitively, to the retained components.

- **Chain links are ROOT concepts only** (`_declared_keys`): a KEY's `effective_keys`, a PROPERTY's `keys`. A derived concept's keys can be conditional (an empty-grain FILTER virtual, `keys_are_conditional_fd`), so nothing chains through one.
- **Merge identity**: a KEY that a *global, non-partial* `merge s into t` equates to `s` is determined by whatever determines `s` (`Environment.equal_merge_sources`). Partial merges (`into ~t`) and statement-scoped joins hold only on matched rows and contribute nothing; that is the scoped-join equality that caused the LEFT->FULL flip when it was used for group elision, and why this does not read `environment.domain_graph`.

How this lands against the concerns in the original proposal:

1. **Seam.** Neither option (a) nor (b). The existing author-side reducer is the seam: identity and the rendered `by` stay one thing, and nothing downstream has to tolerate a divergence.
2. **Which minimal set.** Chain reduction over a key DAG has a unique result (the undetermined sources), independent of iteration order. Only a key cycle (`a <-> b`) is order-dependent, and there `sorted()` addresses decide; the datasource-column build and the query build share addresses, so both sides agree.
3. **Partial keys.** Sidestepped for merges (partial merges excluded). For `~` *bindings* the rows are unchanged: A/B with the reduction patched off, on a fixture with extension rows, is identical for plain, `~` and `?` bindings of the middle key (`test_partial_determined_key_rows`).
4. **Blast radius.** Fingerprints hash authored lineage, not the pinned grain, so concept fingerprints are untouched. A `persist` whose select names a transitively-determined column gets a one-time fingerprint change through `SelectLineage.grain` (one model-aware rebuild). See the suite notes below for SQL churn.
5. **The GROUP BY itself.** Unchanged: projecting `state.code` beside an `org.code`-grain aggregate is the select's concern, and is exactly the plan the `org.state_code` spelling already produced.

Bonus: the recompute path gets cheaper. Two spellings in one statement (`count(id)` at `Grain<org.code, state.code>` beside `count(id) by org.code`) used to be two GROUP BYs over the joined org table; they are now one aggregate over `launches` alone.

## Also fixed: two names for one materialized aggregate (pre-existing on main)

Collapsing more spellings onto one canonical widens a bug main already had: once two outputs share a canonical *and* a datasource materializes it, only one name survived the root scan.

```
select order_id, total, sum(amount) by order_id as explicit;               # main: `total` silently missing from the result
select order_id, customer_id, total, sum(amount) by order_id as explicit;  # main: "Missing source reference to local.amount"
select order_id, region, total, sum(amount) by order_id as explicit;       # main: rows (6.0 beside 96.0); with the grain fix alone, a render error
```

The reference graph keys a concept node by canonical address, and `BuildEnvironment.canonical_concepts` keeps one concept per canonical. Two seams read the winner's *address* where they meant the expression, and both had to change:

- **Discovery** (`source_planning._local_concept_nodes_for_datasource`): the "this table binds it as a column" guard compared the winner's address (`explicit`) to the summary's column (`total`), so in a multi-table plan the summary scan lost the aggregate node. It now compares canonicals (`_datasource_binds_canonical`).
- **Binding** (`create_select_node_candidate`): the scan was rebuilt with one concept per node, so the other requested name had no output. Callers now pass the requested concepts and the scan emits every one sharing a canonical with a node it reads (`_canonical_siblings`).

Either alone is not enough: discovery alone still drops the second name (silently, in the single-table case); binding alone never reaches the summary in a multi-table plan.
