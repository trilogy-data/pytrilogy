# Bug/handoff: reading back a rowset that internally joins two datasources → DisconnectedConcepts

**Status:** OPEN, found 2026-06-22 (enriched q64). trilogy 0.3.285.
**Severity:** medium-high — a rowset whose body relates two datasources via a
query-scoped join works as an inline select, but the moment you read its outputs in
an outer query you get `DisconnectedConceptsException: ... missing a join or merge`.
The message is actively misleading: the join IS present, inside the rowset. The
agent (q64) could not recover and burned ~1.6M tokens.

**Minimal unit test (checked in):**
`tests/test_rowset_cross_datasource_outer_read.py` — baseline (single-datasource
rowset reads back), control (cross-datasource body inline resolves),
`xfail(strict=True)` for the outer read, and a pin on the current exception.
Self-contained fixture, no DB.

## Symptom

```trilogy
import a as a;   -- key aid, property av
import b as b;   -- key bid, property bv  (separate datasource, separate namespace)

with rs as inner join a.aid = b.bid
select a.aid as k, sum(a.av) as sa, sum(b.bv) as sb;

select rs.k, rs.sa, rs.sb;   -- DisconnectedConceptsException
--> split into {local._rs_k, local._rs_sa}; {local._rs_sb}
```

- Inline (no outer read) → OK: `inner join a.aid=b.bid select a.aid, sum(a.av), sum(b.bv)`.
- Single-datasource rowset, outer read → OK.
- Cross-datasource rowset, outer read → split by source datasource (`a` vs `b`).

The exact q64 form (catalog_sales `cs` joined to catalog_returns `cr` on item.id +
order_number, then read back) splits `{_cat_agg_item_id, _cat_agg_ext_list_price_sum}`
from `{_cat_agg_refund_sum}` — the cs-derived outputs vs the cr-derived output.

## Root cause

The disconnection is computed by `disconnected_components`
(`trilogy/core/processing/discovery_utility.py` ~L597): it partitions the requested
concepts by weakly-connected components of the environment reference graph, anchoring
each concept with `_anchor_nodes` (~L546) = the concept's own node + its **direct
source args'** default-grain nodes.

A rowset output's source args are the underlying datasource concepts:
`sum(b.bv)` → `b.bv` (b's component); `a.aid` / `sum(a.av)` → a's component. The
rowset's internal scoped join `a.aid = b.bid` is **query-local** — not a global
merge/FK, so it is absent from the environment graph. With no edge between the `a`
and `b` components, the rowset's own outputs fall into two components.

**Actual raise site:** `concept_strategies_v3.py:645` (`source_query_concepts`)
— it calls `disconnected_components(environment, required, g)` and raises when
`len(groups) > 1`. (`discovery_utility.py` ~L521 is a second caller; the q64 path
hits the strategies one.)

The upstream resolver in `discovery_utility.py` (~L340: "if it's derived from any
value in a rowset, ALL rowset items are upstream … use the rowset's already-namespaced
`derived_concepts`") already encodes rowset cohesion. `disconnected_components` does
not.

## Fix direction — and a dead-end already ruled out

Right mechanism: in `disconnected_components`, before computing components, group the
requested concepts by the rowset they belong to and **union their anchor nodes** so a
rowset is one connected unit. A rowset exposes a single grain from one CTE; its
outputs must never be partitioned by their pre-rowset datasource lineage.

**Dead-end (tried 2026-06-22, reverted):** grouping by
`c.derivation == Derivation.ROWSET` + `c.lineage.rowset.name` finds **nothing** at
this site. By the time the concepts reach `source_query_concepts`, the rowset outputs
are **flattened to their underlying expression** — e.g. for the repro:
`local._cat_agg_item_id` is `derivation=BASIC, lineage=BuildFunction`;
`local._cat_agg_e` / `_cat_agg_r` are `derivation=AGGREGATE,
lineage=BuildAggregateWrapper`. The `BuildRowsetItem` lineage is gone; the only
surviving rowset signal is the **address prefix** `_<rowset_name>_…`
(`_cat_agg_…`). So the membership grouping must be recovered another way:
- (a) map each materialized output address → its rowset via the BuildEnvironment's
  rowset registry / the `BuildRowsetLineage.derived_concepts` lists (find where those
  live for the build env at this stage), or
- (b) as a fallback, the `_<rowset_name>_` address prefix.

Then union the anchor nodes within each rowset group (the original mechanism is
sound; only the detection needs fixing).

Guards / don't-regress:
- A single-datasource rowset already reads back fine — keep it working (baseline
  test).
- Don't over-connect: only union outputs of the *same* rowset, not unrelated concepts
  that merely share a datasource. Two genuinely unrelated models with no
  rowset/join/merge between them must still report as disconnected — see
  `tests/core/processing/test_disconnected_components_e2e.py`.

## Provenance / related

Enriched eval q64 (catalog channel: per-item list-price vs refund, sale-vs-return
join). Part of the broader "cross-rowset / cross-model relation" theme (q05, q29,
q59, q64) where agents pre-aggregate across two sources in a rowset and then can't
use it. Sibling now-fixed item: outer scoped join between two rowsets
(`bug_outer_scoped_join_two_rowset_measures.md`). The prior q64 handoff
(`handoff_q64_join_grain_resolution.md`, resolved 2026-06-09) is a DIFFERENT
failure mode (the canonical join form's grain tangle) — this is the auto/rowset
read-back disconnection, not covered there.
