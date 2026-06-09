# Handoff: q64 `join` form fails to plan — passthrough-dim grain tangle

**Status:** OPEN. Failing test: `tests/test_rowset_derived_twice_join_bugs.py::test_q64_join_form_plans`
(`xfail(strict=True)` — delete the marker when it passes). Repro query lives at
`tests/modeling/tpc_ds_duckdb/query64_join.preql`.

## One-line
Converting q64 from merge+align to the rowset `join` form fails at **plan time**
with `ValueError: Cannot resolve query. No remaining priority concepts`. The many
functionally-dependent **passthrough dimensions** carried through the two per-year
aggregates pick up tangled grains after the 3-key join collapse, and discovery
dead-ends.

## Context
Follow-on to the self-referential-key grain fix (2026-06-08, see
`handoff_q75_derived_twice_rowset_join.md` + memory
`project_rowset_derived_twice_self_key`). That fix unblocked the *simple*
shared-parent join (one join key, no extra passthrough dims). q64 is the hard
case: 3 join keys (`item_sk`, `s_name`, `s_zip`) plus ~12 extra passthrough dims
on the 99 arm (product name, sale-address fields, customer-address fields,
first-sales/first-shipto years) that are NOT determined by the 3 join keys.

This is plan-time only — `engine.generate_sql(text)` raises with **no tpcds data
loaded**, so it's cheap to iterate on.

## Repro
`tests/modeling/tpc_ds_duckdb/query64_join.preql` — the merge+align of
`query64.preql` rewritten as two aggregate rowsets (`agg_99`, `agg_00`) joined on
`item_sk`/`s_name`/`s_zip`, with the marital-status inequality and the
`cnt_00 <= cnt_99` filter in the leading `where`. Minimal hand-built repros with
only 1–2 passthrough dims that ARE determined by the join key resolve fine — the
trigger needs several passthrough dims spanning multiple entity hops
(sale_address, customer.address, customer.first_*_date.year), so use the real q64
shape as the repro until someone isolates a smaller one.

## Diagnosis (where to look)
The raised `ValueError` dumps the candidate grains; the smoking gun is grains like:
```
agg_99.p_name_99@Grain<agg_00.item_sk_00>
agg_99.b_sn_99@Grain<agg_00.item_sk_00, agg_00.s_name_00, agg_00.s_zip_00,
                     agg_99.b_city_99, agg_99.b_str_99, ... all other 99 dims ...>
```
i.e. a 99-side passthrough dim (`b_sn_99`, sale-address street number) ends up
with a grain that **mixes the join-target keys (the `agg_00.*` side the inner join
collapsed onto) with every other 99-side address dim**. No concept can be
prioritized because each appears in the others' grains — a cycle — so
`get_priority_concept` exhausts candidates and raises.

Root cause is grain assignment for non-key passthrough dims of an aggregate rowset
when a multi-key `join` collapses the key columns onto the sibling's. The dims
that are properties of an entity whose key is NOT selected (sale_address.id,
customer.address.id) form a mutual composite grain; after the join rewrites the
join keys to the target side, that composite grain absorbs the target keys and
becomes self-referential across the arm.

Key files:
- `trilogy/core/processing/discovery_utility.py`: `get_priority_concept` /
  `get_loop_iteration_targets` (where it dead-ends — start here to see what it's stuck on).
- `trilogy/core/processing/node_generators/rowset_node.py`: `_scoped_join_targets`
  + grain recompute (how the join target keys get advertised/collapsed).
- `trilogy/core/models/build.py`: `concepts_to_build_grain_concepts` /
  `_concept_is_relevant` / `_normalize_grain_components` (how passthrough-dim
  grains are computed and how scoped-merge key collapse is normalized).
- Compare against the *working* multiselect path (`query64.preql`): the align
  machinery groups each arm independently, so passthrough dims keep arm-local
  grains and never absorb the other arm's keys.

## Validation
1. `query64_join.preql` generates valid SQL → `test_q64_join_form_plans` passes; remove its `xfail`.
2. Execute it and confirm it matches `PRAGMA tpcds(64)` (the merge form already
   matches). Watch the join type: q64's merge emits `FULL JOIN ... is not distinct
   from` reduced to inner by `cnt_00 <= cnt_99`; the explicit `inner join` form
   must produce the same rows.
3. Then (optionally) convert `query64.preql` itself to the join form and keep
   `test_sixty_four` green. Note the existing perf guard in the q64 comments: the
   marital-status inequality is deferred to the outer aggregate on purpose to keep
   it out of the customer_demographics join planning (a LEFT->INNER upgrade DuckDB
   plans catastrophically — 0.05s -> 18s). Preserve that.

## Pitfalls
- Plan-time failure, no data needed — fast loop on `generate_sql`.
- Concurrent agents share the working tree; never mutate git state.
- Don't confuse with the q75 dedup-fusion bug (`handoff_q75_join_dedup_fusion.md`):
  that one plans fine but returns wrong numbers; this one fails to plan at all.
