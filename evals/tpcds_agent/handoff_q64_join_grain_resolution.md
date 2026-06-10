# Handoff: q64 `join` form fails to plan — passthrough-dim grain tangle

**Status:** RESOLVED 2026-06-09. `test_q64_join_form_plans` passes (xfail marker
removed); q64 join executes and matches the multiselect form at sf=1 (2 rows).

## RESOLUTION 2026-06-09 — root cause was NOT the grain tangle

The grain tangle below was a *symptom*. The real cause: a scoped `join a = b`
collapses source→target via `scoped_merge_map`, but — unlike a global `merge`
(`environment.merge_concept`) — it never linked the source back onto the target
as a pseudonym, and its `alias_origin_lookup` entry resolved to the *target*
identity instead of the source's own derivation. So for a **derived** join key
(rowset output, basic transform, union — anything not a root datasource column)
the merged concept only knew the target's lineage and was unsourceable from the
source side → "No remaining priority concepts". This is **not rowset-specific**:
a plain `inner join da = db` on two basic-transform fields from different
datasources dead-ended identically, while the equivalent `merge da into ~db`
planned fine (joining `highfalutin.db = wakeful.da`).

**Fix (build.py only, ~39 lines):** mirror `merge_concept` for INNER scoped
joins — (1) augment the build `pseudonym_map` so source and target are mutual
pseudonyms; (2) in the `_build_environment` alias repopulation, build the INNER
source as its OWN identity (evict the collapsed `local_concepts` cache entry
across the build, then restore it) so `alias_origin_lookup` carries the source's
real derivation. Both gated to INNER (`scoped_inner_sources`): LEFT/FULL are
asymmetric (partial source + coalesce) and keep the existing full-join-key /
partial machinery. No change to `_scoped_join_targets`, `rowset_node.py`, or
`rowset_semantics.py` was needed — the earlier grain-tangle patches there were
all rendered redundant by this and reverted. Regression test:
`tests/test_join_merge_parity.py::test_scoped_join_on_nonrowset_derived_key`.

**LEFT and FULL** scoped joins on a derived key were also fixed in the same
session, so scoped joins now work across arbitrary (derived) concepts like the
global merge — see `handoff_scoped_full_join_derived_key.md`.

(Historical investigation below — the grain-tangle framing is superseded.)

## UPDATE 2026-06-09 — fresh findings + current context

**q64 is the LAST convertible multiselect.** Surveyed all merge+align multiselects in
`tpc_ds_duckdb`: q76 and q77 are **UNION shapes** (3 branches tagged with disjoint
constant channels — `'store'/'web'/'catalog'` — stacked via align+coalesce; no
cross-branch key match). Verified empirically that a query-scoped FULL JOIN on a
disjoint-constant key does NOT reproduce the union (returns a single malformed row),
so q76/q77 must stay multiselects (or get a real row-`union` construct). **q64 is the
only one that is a genuine join** (self-join: year-1999 store-sales slice vs year-2000
slice, joined on real keys item_sk/s_name/s_zip). It is **INNER** (the `cnt_00 <=
cnt_99` filter collapses the reference's full-outer-equivalent), so the recent
query-scoped FULL-JOIN work (registry-driven `scoped_full_join_keys` + SQL-correct
LEFT direction, branch `no_more_multiselect`) does **NOT** touch q64's path — this
grain tangle is fully independent of join *type*.

**The tangle is at BUILD time, not discovery.** Inspecting the materialized build env
(`env.materialize_for_select(scoped_joins=...)`) directly — before any discovery — the
concept grains are already tangled:
```
agg_99.p_name_99  -> grain {agg_00.item_sk_00}                       # CLEAN (item-dependent)
agg_99.item_sk_99 -> agg_00.item_sk_00, grain {agg_00.item_sk_00}    # collapsed, clean
agg_99.b_sn_99    -> grain {agg_00.item_sk_00, agg_00.s_name_00, agg_00.s_zip_00,
                            b_city_99, b_sn_99(SELF), b_str_99, b_zip_99, c_city_99,
                            c_sn_99, c_str_99, c_zip_99, fsyear_99, p_name_99,
                            s2year_99, syear_99}                     # TANGLED + SELF-REF
agg_99.cnt_99     -> grain {agg_00 3 keys, all 99 address dims}      # tangled (but not self, it's an agg)
```
So the discriminator is **single-entity vs multi-entity** passthrough dims:
- `p_name_99` is functionally dependent on `item` alone → grain collapses cleanly to the
  single join key `{agg_00.item_sk_00}`. Fine.
- The **address** dims (`b_*_99` = sale_address fields, `c_*_99` = customer.address fields,
  the `*year_99` first-sales/shipto years) belong to entities whose key is NOT selected,
  so they form a **mutual composite grain** — each one's grain contains every sibling
  address dim AND ITSELF. After the 3-key join collapse, that composite *also* absorbs the
  collapsed join targets (`agg_00.item_sk_00/s_name_00/s_zip_00`). Result: a set of
  concepts whose grains are mutually + self-referential → `get_priority_concept` can never
  find one whose grain is fully resolvable → "No remaining priority concepts".

The **self-reference** (`b_sn_99` ∈ `b_sn_99.grain`) is the same shape as the q75 self-key
bug (fixed via `x.keys.discard(x.address)` in `rowset_to_concepts_v2`,
`trilogy/parsing/v2/rowset_semantics.py`) — but that fix only stripped a single self-key;
here it's a whole mutually-referential *cluster* of multi-entity dims that also picks up
the join-target collapse, so the single-key discard doesn't cover it.

**Fix direction (recommended):** mirror what the multiselect does — source the aggregate
rowset (`agg_99`) **as a unit** (its own CTE at its own select grain) and pull the
passthrough dims from that CTE, instead of letting discovery grain-resolve each multi-entity
dim individually through the join collapse. Concretely, either (a) generalize the q75
self-key strip so a passthrough dim's grain drops the *whole* mutually-referential cluster
(keep only the genuine determining keys, e.g. collapse the address dims onto the rowset's
own grain anchor), or (b) make the build/discovery treat a joined aggregate-rowset's
non-key outputs as carried-at-rowset-grain (whole_grain-style) rather than re-derive their
grain post-collapse. Compare the build-env grains of `query64.preql` (multiselect, passing)
vs `query64_join.preql` (join, failing) for the same dims — the multiselect keeps them
arm-local; the join collapses them.

**Where to look (refined):** the tangle is created in `trilogy/core/models/build.py` grain
computation for rowset passthrough dims under scoped-merge collapse
(`concepts_to_build_grain_concepts` / `_concept_is_relevant` / `_normalize_grain_components`
/ `_abstract_resolution_grain`). `discovery_utility.py:get_priority_concept` is only where
the *symptom* surfaces. Repro to iterate (plan-time, no data):
```python
from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine_v2 import parse_text
MOD = Path('tests/modeling/tpc_ds_duckdb'); text=(MOD/'query64_join.preql').read_text()
env,parsed = parse_text(text, root=MOD); stmt=parsed[-1]
scoped=[(j.source_address,j.target_address,j.join_type) for j in stmt.join_clauses]
be = env.materialize_for_select(scoped_joins=scoped)
print(sorted(str(x) for x in be.concepts['agg_99.b_sn_99'].grain.components))
```

(Original report below — still accurate.)

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
