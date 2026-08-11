# Handoff: composite membership across two facts leaks `INVALID_REFERENCE_BUG` (q17)

**Status: error surface FIXED (2026-08-10); capability gap CHARACTERIZED, not
implemented.** Originally from
`results/20260810-211903_enriched_aggregates/agent_log.q17.jsonl`.

## Reduction result (the localization the original handoff asked for)

Probes against `tests/modeling/tpc_ds_duckdb` (generation only):

- (a) cross-model tuple membership in a **plain `where`** — WORKS. The planner
  builds a shared existence source for the pair.
- (b) the same membership moved **inside an inline-filtered aggregate**
  (`count(grain(...) ? (ss.customer.sk, ss.item.sk) in
  (cs.billing_customer.sk, cs.item.sk)) by *`) — FAILS. Both right-side
  concepts are from `cs`, but inside the aggregate's scope they resolve
  through **separate dimension parent CTEs** (customer vs item), and
  `_common_existence_source` finds no single name carrying both. This is the
  first failing step; the extra conditions in the original probe only change
  *how* it fails.
- (c) full original probe — same failure, plus one component becomes entirely
  unresolvable, which is where `INVALID_REFERENCE_BUG<...>` leaked into the
  message.
- (d) same-model tuple membership in a filtered aggregate where both sides
  live on one fact — WORKS.

So: not COUNT- or model-specific. The capability gap is "composite membership
inside an inline-filtered aggregate whose right-side pair is only reachable
through separate dim enrichment CTEs". Making it work means planning the pair
as one existence island (like the plain-`where` path does) rather than
resolving components independently — planner work, not renderer work.

## Error-surface fix (landed)

`trilogy/dialect/base.py` `render_composite_membership`: the
single-existence-source failure now raises a clean error that

- names the **logical concepts** (`['cs.billing_customer.sk', 'cs.item.sk']`),
  never source CTE names, physical aliases, or `INVALID_REFERENCE_BUG`
  placeholders (it also fires when the sole resolution is an invalid
  placeholder, instead of emitting broken SQL);
- states the constraint in the language-reference wording (right side must
  come from ONE model or rowset);
- gives the **validated** remedy: stage the pair through a fact-anchored
  rowset.

Regression: `tests/modeling/tpc_ds_duckdb/test_q17_composite_membership_error.py`.

## Remedy caveat (important for docs/agent guidance)

`with pairs as select cs.billing_customer.sk as pc, cs.item.sk as pi;` (bare
pair, no fact column) plans as `customer FULL JOIN item on 1=1` — a **dim
cross product**, silently wrong for "pairs present in catalog_sales". The
rowset must be anchored on the fact, e.g.

```trilogy
with pairs as
select cs.billing_customer.sk as pc, cs.item.sk as pi, count(cs.order_number) as _anchor;
```

which renders `SELECT ... FROM catalog_sales GROUP BY 1, 2` and produces the
correct existence semi-join. The error message and the language reference
should keep recommending the anchored form. (The bare-pair cross product looks
related to the open q06 `on 1=1` scoped-join issue —
`project_q06_grain_drops_scoped_join_key`.)

## Open follow-ups

1. **Capability decision:** support (b) by planning the membership's right
   side as one existence island inside filtered-aggregate scopes, or keep the
   clean rejection. The plain-`where` path (a) proves the existence-island
   machinery exists; the gap is only in the inline-filtered-aggregate route.
2. Language reference already documents the ONE-source constraint; consider
   adding the fact-anchor caveat there too.

## Audit verdict (2026-08-11): planning gap, NOT principled — for the minimal case

The right side of `X in Y` is scope-independent by the codebase's own stated
principle ("the set Y is by definition the UNFILTERED set" —
`_CleanFeederCache`, `strategy_builder.py`). Hosting the membership in a plain
`where` vs an inline-filtered aggregate changes which rows are *tested*, never
what the *set* is. And the plain-`where` plan is verified semantically correct:
the island renders `SELECT pair FROM catalog_sales GROUP BY 1, 2` — fact-anchored
co-occurring pairs, not a dim cross product.

The mechanism of the gap: the plain-`where` route
(`condition_sources.py::resolve_existence_sources`) searches each existence
arg group **as one unit** (`search_parent(existence_args)`), which is what
produces the single co-occurrence island. The v4 group-graph route that hosts
filter-lineage predicates **flattens tuple groups into individual addresses at
every touchpoint** — `group_graph.py` (existence demand routed per-address to
whatever group already hosts it), `concept_graph.py` (per-address channel
classification), `strategy_builder.py::_group_existence_concepts` →
`_existence_parents_for` (per-concept lookup against already-built groups).
The tuple's co-occurrence constraint ("these N addresses must come from ONE
node") is simply not representable in that data model. A lost invariant, not a
decision — a principled rejection would have to reject the plain-`where` form
too.

**Two caveats:**

- The FULL q17 pattern is only *partly* a gap: the extra cs-side scalar
  conditions inside the filter (`cs.sale_date.year in (2001, 2002)`) ask for a
  *filtered* set, which the unfiltered-set principle deliberately does not
  express inline. The fact-anchored rowset remains the principled spelling for
  that; fixing (b) would not and should not make (c) work as written.
- Implementation caution: a fix must route the whole tuple to ONE
  co-occurrence host (group-level `search_parent`, like the plain-`where`
  path), likely building a NEW island group anchored on the shared lineage
  fact. Reusing the already-built per-address dim groups and joining them is
  exactly the bare-pair cross-product trap above.
