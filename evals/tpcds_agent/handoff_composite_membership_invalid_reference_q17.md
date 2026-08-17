# Handoff: composite membership across two facts leaks `INVALID_REFERENCE_BUG` (q17)

**Status: FIXED (2026-08-16).** Shape (b) -- cross-model tuple membership inside
an inline-filtered aggregate -- now plans, off the same fact-anchored
co-occurrence island the plain-`where` path builds. Regression:
`tests/modeling/tpc_ds_duckdb/test_q17_composite_membership.py`. Originally from
`results/20260810-211903_enriched_aggregates/agent_log.q17.jsonl`.

## The defect (confirmed genuine, not mis-reported)

Probes against `tests/modeling/tpc_ds_duckdb` (generation only):

- (a) cross-model tuple membership in a **plain `where`** -- worked, and still
  works. The planner builds a shared existence source for the pair.
- (b) the same membership moved **inside an inline-filtered aggregate**
  (`count(grain(...) ? (ss.customer.sk, ss.item.sk) in
  (cs.billing_customer.sk, cs.item.sk)) by *`) -- FAILED. Both right-side
  concepts are from `cs`, but the v4 group-graph route sourced them
  independently, landing them on separate dimension parent CTEs (customer vs
  item), so `_common_existence_source` found no single name carrying both.
- (d) same-model tuple membership in a filtered aggregate where both sides
  live on one fact -- worked.

The right side of `X in Y` is scope-independent by the codebase's own stated
principle ("the set Y is by definition the UNFILTERED set" --
`_CleanFeederCache`, `strategy_builder.py`). Hosting the membership in a plain
`where` vs an inline-filtered aggregate changes which rows are *tested*, never
what the *set* is, so rejecting (b) while accepting (a) was a lost invariant,
not a principled constraint.

## The fix

The plain-`where` route (`condition_sources.py::resolve_existence_sources`)
searches each existence arg group **as one unit** (`search_parent(existence_args)`),
which is what produces the single co-occurrence island. The v4 group-graph route
flattened tuple groups into individual addresses at every touchpoint, so the
"these N addresses must come from ONE node" constraint was not representable.

`strategy_builder.py` now carries the arg group, not the address, as the unit of
existence sourcing:

- `_group_existence_arg_groups` / `_condition_existence_arg_groups` /
  `_filter_lineage_existence_arg_groups` / `_node_existence_arg_groups` return
  `list[tuple[BuildConcept, ...]]`.
- `_existence_parents_for` takes arg groups. Per group it asks
  `_covering_built_node` for a built group carrying EVERY component; a node
  covering only part of the tuple is no longer a candidate.
- When no built group covers a multi-component tuple, `_CleanFeederCache` builds
  the co-occurrence island from the whole tuple (one `search_concepts` over all
  its addresses, outputs sliced to those addresses) rather than joining the
  per-address dimension groups -- which is the bare-pair cross-product trap
  below.

`_existence_for_group` is gone; `_attach_existence_sources` calls
`_group_existence_arg_groups` directly (its parent-gathering half was dead --
the caller discarded the parents and `_attach_existence_to_node` re-derived
them).

Corpus footprint: **zero**. All 132 `query*.preql` under
`tests/modeling/tpc_ds_duckdb` + `tests/modeling/tpc_h` render byte-identical.
The new multi-component branch does fire in the corpus (9 multi-component groups,
all in query14's 3-tuple `cross_tuples` membership; 4 take the new feeder path)
and produces identical SQL there.

## Remedy caveat (still true, keep in docs/agent guidance)

`with pairs as select cs.billing_customer.sk as pc, cs.item.sk as pi;` (bare
pair, no fact column) plans as `customer FULL JOIN item on 1=1` -- a **dim
cross product**, silently wrong for "pairs present in catalog_sales". The
rowset must be anchored on the fact, e.g.

```trilogy
with pairs as
select cs.billing_customer.sk as pc, cs.item.sk as pi, count(cs.order_number) as _anchor;
```

which renders `SELECT ... FROM catalog_sales GROUP BY 1, 2` and produces the
correct existence semi-join. (The bare-pair cross product looks related to the
open q06 `on 1=1` scoped-join issue -- `project_q06_grain_drops_scoped_join_key`.)

## Still rejected, by design

- A pair whose components live on **two different facts**
  (`(ss.customer.sk, ss.item.sk) in (cs.billing_customer.sk, ws.item.sk)`) has no
  co-occurrence source at all. `render_composite_membership` raises the clean
  ValueError naming the logical concepts and the anchored-rowset remedy; the
  plain-`where` twin raises `DisconnectedConceptsException`. Covered by
  `test_split_fact_pair_error_names_concepts_and_remedy`.
- The FULL q17 pattern (shape (c)): the extra cs-side scalar conditions inside
  the filter (`cs.sale_date.year in (2001, 2002)`) ask for a *filtered* set,
  which the unfiltered-set principle deliberately does not express inline. The
  fact-anchored rowset stays the principled spelling.

## Open follow-up: shape (c) rejects with a poor error (pre-existing, separate)

Shape (c) now gets past the membership and dies at render with `Could not render
the query: Missing source reference to cs.sale_date.year; ...` plus a SQL dump
carrying `INVALID_REFERENCE_BUG` sentinels.

This is **not caused by the fix** -- it is the pre-existing surface for any
foreign-fact row predicate inside an inline-filtered aggregate. Control probe
with no membership at all:

```trilogy
auto x <- count(grain(ss.ticket_number, ss.item.sk)
    ? ss.sale_date.year = 2001 and cs.sale_date.year in (2001, 2002)) by *;
```

produces the identical "Missing source reference" dump today, while its plain
`where` twin raises the clean `DisconnectedConceptsException`. Before the fix,
shape (c) happened to trip the tuple-membership error first and so looked clean.

Mechanism: `raise_if_filter_disconnected` (the gate that surfaces a FILTER's
hidden condition concepts) only runs in `query_processor._plan_query_node` when
planning returned `None`. These shapes plan a node successfully -- one with
unsourceable references -- so the gate never fires. Additionally
`_filter_hidden_concepts` only inspects outputs whose lineage is *directly* a
`BuildFilterItem`, so a filter one hop under an AGGREGATE is invisible to it
even when the gate does run. Fixing this means running the connectivity check
pre-discovery over lineage-nested filters, which needs its own corpus A/B.
