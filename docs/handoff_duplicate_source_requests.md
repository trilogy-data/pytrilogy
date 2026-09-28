# Duplicate source requests in v4 discovery

## Status: INVESTIGATED 2026-09-28, no planner change yet.

Tooling landed with the investigation (`local_scripts/plan_debugger/`):
`source_repeats.py` measures the repeats below, and the plan debugger's traces
show them step by step. Nothing in the planner has been changed.

## Summary

Within one statement, `plan_source` is often asked the exact same question
more than once and answers it from scratch each time. Across the thelook,
TPC-H and TPC-DS corpora:

    427 plan_source calls
     65 repeat an earlier request VERBATIM (same outputs, WHERE, deferred WHERE,
        flags, span scope, environment and history)
      0 of those repeats returned a structurally different plan
    0.16s of 2.01s total sourcing time is spent on the repeats

The repeats come from three callers, which are three different mechanisms:

| caller | repeats | mechanism |
|---|---|---|
| `parent_for_consumer` (in `_parent_nodes_for`) | 37 | a speculative narrower rebuild of a ROOT parent that is identical to the parent |
| `build_strategy_node` (the build loop) | 19 | two ROOT groups (the same root in two phases) with identical requests |
| `_fresh_final_root_projection` | 9 | FINAL re-sourcing a ROOT contributor the loop already built with the same request |

A fourth, related waste is NOT a verbatim repeat and does not show in that
count: a region DOMAIN group is built under its group scope, then FINAL
re-sources it under FINAL's scope and discards the built node (section 4).
That is the case visible in `adhoc04`.

## Reproduce

```bash
# counts, per file and by caller
.venv/Scripts/python.exe local_scripts/plan_debugger/source_repeats.py \
  "tests/modeling/thelook_duckdb/*.preql" "tests/modeling/tpc_h/*.preql" \
  "tests/modeling/tpc_ds_duckdb/query*.preql"

# one file's calls, with each repeat's call path
.venv/Scripts/python.exe local_scripts/plan_debugger/source_repeats.py --detail tests/modeling/tpc_h/query11.preql

# step through the same statement in the viewer (traces are gitignored; regenerate)
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py tests/modeling/tpc_h/query11.preql --open
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py tests/modeling/thelook_duckdb/adhoc04.preql \
  --rows --setup tests.modeling.thelook_duckdb.db_build:seed --open
```

`source_repeats.py` wraps `plan_source` in the three modules that import it
(`source_planning`, `strategy_builder`, `v4_node_generators/root`) and
fingerprints each request; see its docstring for exactly what the fingerprint
covers. In the viewer, each step's header shows the group being built and the
planner call path, and built groups whose node never reaches the FINAL tree are
struck through in the step list.

Worst files: `tpc_h/query11` (4 repeats of 5 calls), `tpc_ds/query14` (4 of
8), `tpc_ds/query10` and `query44` (3 each).

## 1. `parent_for_consumer`: a slice identical to its parent (37)

`strategy_builder.py` `_parent_nodes_for` -> `parent_for_consumer` (~line 766).
For a ROOT parent feeding a GROUPING consumer, it builds a narrower "slice" of
the parent carrying only the columns the consumer needs, and adopts the slice
only if it strictly prunes the leaf datasources
(`_leaf_datasource_ids(sliced) < _leaf_datasource_ids(node)`) or the parent
carries the wrong side of a scoped relation. Otherwise it discards the slice
and returns `node.copy()`.

The slice is attempted whenever `slice_addresses` is a strict subset of the
parent's outputs. A conditioned root scan exposes its WHERE's arguments beyond
what it was asked for. In `tpc_h/query11` the root was requested as
`{available_quantity, id, supply_cost, supplier.id}` with
`where supplier.nation.name = GERMANY`, and the built node also outputs
`supplier.nation.name` and `supplier.nation.id`. The consumer needs only the
four requested columns, so the slice request is EXACTLY the parent's original
request. The rebuild is identical, prunes nothing, and is discarded, once per
grouping consumer (3 times in q11: trace steps 19, 22, 25).

Fix options, cheapest first:
- Skip the slice when its outputs and conditions equal the request the parent
  was sourced with. Requires remembering each built ROOT's request (it is not
  on the node today).
- The request cache in section 5, which covers this and sections 2–3.

## 2. Twin ROOT groups with identical requests (19)

The same root in two phases, e.g. `grp:root:root:∅` and `grp:root:root_d1:∅`
in `query11` (trace steps 13 and 16): the d1 root feeds the condition-phase
aggregates. When the WHERE atoms placed on each twin are the same, the two
groups send identical requests. The groups are legitimately distinct in the
group graph; only their sourcing is duplicated. The cache in section 5 makes
the second one free. Deduplicating the groups themselves is a larger change
and not obviously right: the phases can diverge under other WHEREs.

## 3. FINAL re-sourcing an unchanged ROOT contributor (9 verbatim)

`_assemble_final_node` (~line 4130 onward) re-sources EVERY ROOT contributor
through `_fresh_final_root_projection` (~line 3097) and replaces the built
node, unless the group's WHERE atoms can't all be stated on the scan. The
comments give the reasons the request can differ from the build's:
- preserved merge keys are added (`preserve_keys`, `own_join_keys` narrowing);
- filter-only WHERE args that were peeled into the bucket are added;
- a region domain keeps FINAL's scope (section 4).

When none of those change the request, the re-source is verbatim (9 cases),
and the cache in section 5 makes it free.

## 4. Region domains built at group scope, then re-sourced at FINAL scope

Not a verbatim repeat, so not in the 65; `adhoc04` is the example (trace steps
18–23 build the domains, 31–34 re-source them).

- **The build loop** (`build_strategy_node`, ~line 4542) scopes every group,
  domains included, with
  `extent_free = ownership.suppressed_for(gid) | span_scope.owned`. For the
  `product.id` domain that suppresses `user.id`
  (`extent_free={user.id}`), and vice versa.
- **FINAL** (~line 4250) deliberately re-sources a domain under FINAL's own
  scope: "A domain is the FINAL's own rows and keeps the FINAL's scope: the
  customer domain completes its transitive `~` address there, and the orphan
  address's state is read off it (`test_licensed_transitive_attr_span`)."
  In adhoc04 that is `extent_free=∅`.

So the two requests differ by scope and both are planned. In adhoc04 the plans
come out the same (a plain `products` / `users` scan), the FINAL one is used,
and the built one is dead: the viewer strikes steps 20 and 23 through, since
their nodes never reach the FINAL tree.

**The built domain node is NOT always dead.** A domain can have readers of its
own: an aggregate evaluated over the region's rows
(`_feed_region_domains`' docstring, ~line 2760), and
`feed_region_domains_to_present_scalars`. Those readers consume the build-time
node under the group scope, and `parent_for_consumer` deliberately reads a
region domain "as built" (a re-sourced slice would hold no region). The built
node is also FINAL's fallback when the re-source is skipped (an atom the scan
cannot state).

Open question for whoever picks this up: should a domain be built under
FINAL's scope in the first place? The FINAL comment argues a domain's rows are
FINAL's rows. If that holds for its other readers too, building domains at
FINAL scope makes the FINAL re-source a verbatim repeat (free under section
5), and removes the scope mismatch between what the readers saw and what FINAL
emits. If the readers NEED the group scope, the two builds are genuinely
different questions and only the no-reader case (adhoc04) can drop one: skip
building a domain with no readers, and let FINAL source it once.

## 5. Proposed general fix: reuse identical requests within a statement

Remember each `plan_source` result keyed by the full request fingerprint, and
return a copy on a verbatim repeat. This mirrors the existing history cache
(`concept_strategies_v4.py` ~line 823: "a node may be mutated after being
cached; always store a copy"; `V4History.build_to_history` in
`v4_helper/history.py`). Callers mutate what they get back (`region_spans`,
`origin_group`, wrapping), so both the stored and returned nodes must be copies.

The fingerprint must be at least what `source_repeats.py` uses:
- output addresses;
- `conditions` and `deferred_conditions`;
- `require_full` and `complete_partials`;
- the full `span_scope`: `owned`, `unextended`, `extent_free`, `extent_free_carried`;
- environment and history identity. Adding environment identity dropped the
  count from 70 to 65: rowset bodies plan in their own environment and must
  not share entries.

Before trusting it, check anything else `plan_source` reads from the
environment that can change mid-statement (e.g. `statement_output_addresses`
and `statement_hidden_addresses` when a nested plan is running).

Evidence it is safe: 0 of 65 repeats produced a different plan (compared
structurally: node type, outputs, datasource, conditions, recursively). The
same measurement is the regression check. Then run the full suite, and do a
before/after comparison of the generated SQL: the `zquery_*.log` blob-hash
A/B over the modeling corpora should show byte-identical SQL, since a cache
hit returns the plan the repeat would have built.

Expected win: modest in time (about 8% of sourcing, which is itself a slice of
planning), larger in trace readability (37 discarded slices and the twin
sources disappear), and it makes section 4's fix a pure scope decision.

## Also noticed (not duplicates)

- `copy()` carries `region_spans` (and now `origin_group`) on only 5 of the 11
  node classes: base, filter, group, merge, select. `RecursiveNode`,
  `ConstantNode`, `SubselectNode`, `UnionNode`, `UnnestNode` and `WindowNode`
  construct and return without it, so a copy of one of those loses its
  region contract. Worth checking whether any of them can be a region domain
  or carry `region_spans`.
- adhoc04's final CTE groups by all 7 output columns (FINAL's
  `deduplicate_to_grain`) although its rows are already unique at those
  columns. Another session was working on this (`rows_unique_at_outputs` in
  `grain_utility.py`, `tests/engine/test_projected_row_identity.py`).
