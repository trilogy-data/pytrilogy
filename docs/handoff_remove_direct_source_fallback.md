# Handoff: remove the pre-v4 cover search that `plan_source` falls back to

**Status**: DONE (2026-09-15, same day). Landed on `cleanup_work`; the current state is recorded in `docs/v4_network_discovery_design.md` §0.10, "The second cover search is gone (s58)". Two findings differed from the plan below: shape 2's cause was a connector-only cover winning the search (fixed in `network_search._reads_a_scan`), not seeding; and a fifth shape existed that the toggle run did not expose because `tests/engine` was unverified: requests made only of single-row concepts (grand totals, watermarks) have no search terminals and are routed to the render role by `plan_source._no_join_axis`. The `rollup_edges` block in `create_pruned_concept_graph` stays for that role. Originally: PLANNED, not started (2026-09-15). The derived-key union fix it builds on was narrowed after CI (PR #690): a union emits only derivations some other datasource is keyed on, because emitting every derivation made a channel partition union an INNER anchor and re-typed TPC-DS q05's FULL joins to LEFT (wrong rows) and grew q49 past its size budget. Removing the fallback must keep TPC-DS byte-identical for the same reason. Measured on `derived-key-union-lookup`
(commit `91f92ac99`, origin/main `7f0fe8aba` plus the derived-key union fix). Land
that PR first; this work touches the same union-injection code and should not tangle
with it.

## What the fallback is

v4 is the only planner, but `plan_source` (`v4_helper/source_planning.py`) still ends
in a pre-v4 cover search. After `_network_source` declines a ROOT request, the loop

```python
for accept_partial in ((False,) if request.require_full else (False, True)):
    direct = _direct_source(request, accept_partial)
```

calls `history.gen_select_node` -> `node_generators/select_node.gen_select_node` ->
`select_merge_node.gen_select_merge_node` -> `_source_concepts_via_graph`, which runs
`create_pruned_concept_graph` + `source_scoring.resolve_subgraphs` and then MERGES
whatever subgraphs come back. That is a second, independent datasource-cover search
over the reference graph (the `[GEN_ROOT_MERGE_NODE]` lines in a debug trace). It can
accept a cover the network already judged disconnected. The `ON 1=1` plan in
`tests/engine/test_duckdb_derived_key_union_lookup.py`'s bug report came from exactly
this path: the network declined, `plan_source` recursed with `conditions=None`, and
`_direct_source(accept_partial=True)` merged a union scan with a lookup regrouped to
`label` alone.

`_direct_source` has a SECOND role that must stay: it renders a one-scan solution the
network DID find (`decision.bridge is None`, the "defer" verdict). See "Non-goals".

## Evidence: how load-bearing the fallback role is

Experiment toggle (do not commit; apply, measure, revert):

```python
# in plan_source, just before the accept_partial loop
import os as _os
...
for accept_partial in ((False,) if request.require_full else (False, True)):
    if _os.environ.get("TRILOGY_EXPERIMENT") == "no_fallback" and decision is None:
        break
    direct = _direct_source(request, accept_partial)
```

`decision is None` means the network declined; `decision.bridge is None` means it
answered with a single scan and `_direct_source` is only rendering it.

Results with `TRILOGY_EXPERIMENT=no_fallback` (Windows venv, 16 GB machine; the full
suite in one process gets OOM-killed, run directories separately):

| Chunk | Result |
|---|---|
| `tests/core/processing tests/discovery tests/join_matrix` | 9 failed / 950 |
| `tests/complex tests/generators tests/nodes tests/optimization tests/test_*.py` | 0 failed / 2514 |
| `tests/modeling` (minus `tpc_ds_duckdb`), stdlib, rendering, persistence, execution, parse_refactor, stack_overflow, custom, hooks, helpers | 2 failed / 917 |
| TPC-DS DuckDB, SQL generation only for all 99 `queryNN*.preql` | 0 errors, 0 byte diffs vs baseline |
| `tests/engine` | UNVERIFIED: OOM-killed twice, then a third run hung after 80 min with under 5 min of CPU (suspect a network-backed engine test blocking); rerun with `-x --timeout` or in CI, and treat any new failure as a fifth shape |

A log of every fallback firing on the passing baseline (first chunk) recorded 15
firings, all belonging to the failing tests below. Nothing else silently routes
through it.

## The eleven failures, in four shapes

### 1. Additive rollup off a summary table at a coarser or filtered grain (7 tests)

- `tests/discovery/test_aggregate_handling.py::test_partial_additive_aggregate_rollup_sql`
- `tests/discovery/test_aggregate_handling.py::test_partial_sum_aggregate_rollup_sql`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_dimension_attribute_with_aggregate`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_dimension_filter_with_aggregate`
- `tests/discovery/test_aggregate_rollup_matching.py::test_summary_serves_local_namespace[named_coarser]`
- `tests/discovery/test_aggregate_rollup_matching.py::test_summary_serves_imported_namespace[named_coarser]`
- `tests/modeling/thelook_duckdb/test_thelook_queries.py::test_eighteen` (asserts `user_product_sales` is read and `order_items` is not)

Diagnosis: `build_source_network` labels candidates from the STATIC graph
(`_emitted_addresses`), whose `additive_rollup_edges` were computed at each metric's
declared grain in `env_processor.generate_adhoc_graph`. The fallback recomputes
`get_additive_rollup_concepts(..., target_grain=<request grain>)` per request inside
`create_pruned_concept_graph` (`select_merge_node.py`, the `rollup_edges` block), so
only it sees that `flight_agg` at (origin, destination, date) can SUM up to
(origin, date). With the toggle on, the ROOT group for
`[flight_count, flight_date, origin_code]` builds to `None` (no "declined" log line:
the terminal is simply unbound) and the aggregate generator recomputes from the raw
fact.

Fix: give the network the per-request rollup bindings. In `network_build`, extend a
candidate's emitted set with the addresses `get_additive_rollup_concepts` returns
for the request's terminals and grain (the bridge emitter already has the matching
render check, `source_planning._datasource_rolls_up_to`). Mark them non-stored.
Watch `_drop_dominated_arms` and `axis_families`: a rollup binding is a
GROUP-required read, and the emitter must still apply `force_group`
(`create_datasource_node` does, when the datasource grain is not a subset of the
target grain).

### 2. Subset join with a rowset member onto a root key (2 tests)

- `tests/join_matrix/test_subset_join_rowset_onto_root.py::test_projecting_member_key_is_group_axis`
- `tests/join_matrix/test_subset_join_rowset_onto_root.py::test_no_member_reference_unchanged`

Diagnosis: even `select l_key subset join web_cust.cust_sk = l_key;` becomes
`UnresolvableQueryException` with the toggle on. The rowset member rides into the
ROOT request as a search concept and no datasource candidate binds it, so the
network declines a request the fallback answers with a trivial `lsrc` scan. Fix
belongs in how `_search_concepts_for_bridge` / `_join_requirements` seed a subset
join whose one side is a rowset: the rowset side is not a datasource terminal and
should not block the cover (compare how `_derived_connector_nodes` supplies a
non-BASIC merge origin).

### 3. Error surface (2 tests)

- `tests/core/processing/test_v4_concept_graph.py::TestResolveMultiselect::test_unresolvable_arm_raises`
- `tests/modeling/stocks/test_stocks.py::test_filter`

Both expect `NoDatasourceException`, which only `gen_select_node`'s
`validate_query_is_resolvable` raises. Without the fallback the first "DID NOT RAISE"
and the second reaches rendering with `INVALID_REFERENCE_BUG` sentinels. Decide what
the ROOT planner raises when nothing binds a terminal (an `UnresolvableQueryException`
carrying `describe_incomplete_partitions` text is the obvious candidate) and make
`plan_source` raise it itself instead of returning `None` into a render.

### 4. The wrong covers it manufactures (the reason to remove it)

Not a test failure but the motivating bug: with the network's verdict overridden, the
fallback can stitch disconnected subgraphs. The keyless-join guard in
`join_resolution._raise_if_keyless_row_bearing_join` is FD-aware and did NOT catch
the `merge row_cell into cell` spelling, for two reasons worth their own tickets:

- `merge <derived> into <key>` rewrites the derived side into a KEY pseudonym with
  `keys=None`, severing the FD walk back to `tree_id`. Either keep the lineage keys
  on the merged concept or have `key_closure` follow a pseudonym's lineage inputs.
- The fallback regrouped a keyed lookup to a single non-key property (`SELECT label
  ... GROUP BY 1`) to hide the missing key. A dimension reduced to a distinct-values
  list and cross-joined is never intended.

## Plan

1. Rerun `tests/engine` under the toggle (it was never verified locally). Add any new
   failures to the shapes above.
2. Fix shape 1 in `network_build` with a regression test that pins the plan reads the
   summary table (the existing seven tests already do; add one at
   `tests/core/processing/` that asserts the candidate BINDING exists, so the
   contract is pinned below the SQL).
3. Fix shape 2. Pin with the two join_matrix tests, which already exist.
4. Settle shape 3: one exception type, raised by `plan_source`. Update the two tests.
5. Delete the fallback role: in `plan_source`, call `_direct_source` only when
   `decision is not None and decision.bridge is None` (the render role). Rerun the
   chunks above plus TPC-DS generation. Zero byte diffs on TPC-DS is the gate the
   design doc uses (`docs/v4_network_discovery_design.md`, "goldens").
6. Then delete what only the fallback role reached:
   - the `union_edges` / `union_derived_concepts` injection in
     `select_merge_node.create_pruned_concept_graph` (added by the derived-key fix
     purely to keep this path consistent);
   - the `rollup_edges` block there, once shape 1 is in the network;
   - the `conditions=None` recursion at the end of `plan_source` if nothing else
     reaches it (log it first, as with the firing log).
   `_source_concepts_via_graph` itself stays while `_direct_source` renders through
   it. Check `nodes/__init__.py::History.gen_select_node` is the only other caller.
7. Update `docs/v4_network_discovery_design.md` §0.10 with the new state and delete
   the "single-scan planners" wording from the decline log message in
   `_network_source`.

## Non-goals, and why

**Do not delete `_direct_source` / `gen_select_merge_node` wholesale.** A variant
that never called `_direct_source` and rendered every one-scan solution through
`_network_source(defer_single_scan=False)` + `_emit_bridge` produced 29 failures in
the first chunk and took 22 minutes instead of 50 seconds: the bridge emitter has no
memo for single-scan requests, so the network is re-searched per request. Replacing
the renderer is its own change and needs a memo keyed like `History.select_history`
plus the grain-aware `force_group` scoring that `create_datasource_node` applies.

## Working notes for this environment

- CI runs only on `pull_request` and `push: main` (`.github/workflows/pythonpackage.yml`);
  a pushed branch alone gets no checks. `gh` is not installed; the stored git
  credential is rejected by the REST API, so PRs are opened by hand from the
  `pull/new/<branch>` URL.
- The full suite in one pytest process gets killed for memory on this machine. Run
  directories separately; `tests/modeling/tpc_ds_duckdb` at sf=1 is the heaviest, and
  SQL generation alone (`Environment(working_path=...)` + `generate_sql` per file) is
  a cheap, complete proxy for plan changes.
- Importing `trilogy` from the editable install rewrites both `Cargo.toml` files and
  `Cargo.lock` to the current `__version__`; revert them before committing. The
  modeling suites write `*-perf.png`, `*-summary.md` and `zquery*.log` artifacts into
  `tests/modeling/*`; revert and `git clean` those too.
- The Bash tool mangles `\n` inside heredoc-fed Python; use `chr(10)` or the Edit tool.
- Version bumps: `trilogy/__init__.py` only, one patch level per PR.
- `black` must be 26.x to match the repo; the venv had 24.4.2 and reformats 185 files.
  `ruff check trilogy` has one pre-existing I001 in `v4_helper/network_search.py`;
  `mypy trilogy` has ten pre-existing errors. Neither is a regression signal.
