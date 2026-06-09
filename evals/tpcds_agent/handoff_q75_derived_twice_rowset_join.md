# Handoff: q75 / derived-twice rowset→rowset scoped JOIN (the shared-parent shape)

**Status:** GRAIN BUG RESOLVED 2026-06-08; full q75/q64 conversion still blocked by
an *orthogonal* nested-aggregate limitation (see below).

## RESOLUTION (2026-06-08): self-referential rowset key → grain collapse
Root cause of the `FULL JOIN ... on 1=1` cross-join was a **self-referential key**
on the join-key dimension, not a downstream grain/merge issue. When a dimension is
sourced *through* an intermediate rowset (`base.sales.item.brand` rather than the raw
`sales.item.brand`), that intermediate concept has empty keys, so its SELECT alias is
keyed by the source concept *itself*. `rowset_to_concepts_v2` then remaps that key
through `orig` and lands on the new concept's own address — `p.brand` keyed by
`p.brand`. A self-referential key makes `_concept_is_relevant` treat the dimension as
already-grouped and drop it from `BuildGrain.from_concepts`, collapsing the rowset
node grain to `Abstract`; with no shared grain key the outer merge falls back to
`1=1`, and the second arm groups by the dim but omits it from SELECT.

**Fix:** one line in `trilogy/parsing/v2/rowset_semantics.py::rowset_to_concepts_v2`
— after the key remap, `x.keys.discard(x.address)` so a remapped key never points at
its own concept. This makes the derived-twice case match the once-derived case
(empty keys → dim stays a free grain component). Also corrected **q54** (its
`count(rev_cust_id)` over the `my_revenue` rowset was silently rendering as a per-row
`CASE WHEN ... IS NOT NULL THEN 1 ELSE 0 END` instead of a real `count()`).
`tests/test_join_merge_parity.py::yoy_shared_parent` now passes (join + merge).
Full TPC-DS sweep 133 passed; broad suite 3821 passed.

**Still OPEN (orthogonal):** the *real* q75/q64 need nested aggregates
(`sum(deduped.cnt_per_row)` over `deduped.* = sum(...)`), which lower to invalid
`sum(sum(...))`. That fails the `merge` reference identically and is tracked by the
new xfail `yoy_shared_parent_nested_agg`. So q75/q64 stay multiselects until nested
aggregates are split into separate grouping passes.

---

(Original handoff below, for context.)

This handoff
was the remaining gap: two rowsets that each aggregate a **shared upstream rowset**
(q75's `deduped`, q64's `ss_rows_99/00`) and are then joined. This used to **silently
cross-join** (`FULL JOIN ... on 1=1`) — wrong data, no error.

## Goal
Make a query-scoped `join` between two rowsets that both derive from a third
rowset produce the same result as the equivalent `merge` / the SQL reference.
Concretely: unblock converting **q75** (and then **q64**) from a `merge`+`align`
multiselect to the single-SELECT rowset-`join` form. Until then, q75/q64 must stay
multiselects (they pass that way).

## Why this matters
q75/q64 are the only remaining TPC-DS yoy multiselects whose join form is blocked.
q76/q77 are *union* shapes and intentionally never convert. So this is the last
piece for "yoy multiselect → rowset join" parity. Independent-rowset yoy already
converts (e.g. the pattern in `tests/test_join_merge_parity.py::yoy_independent_rowsets`).

## The q75 shape (tests/modeling/tpc_ds_duckdb/query75.preql)
```trilogy
rowset deduped <- where sales.item.category='Books' and sales.date.year in (2001,2002)
select sales.date.year, sales.item.brand_id, ... , cnt_per_row, amt_per_row;   -- dedup grain

rowset year_pair <- where deduped.sales.date.year = 2002
  select deduped.sales.item.brand_id as brand_id_curr, ..., sum(deduped.cnt_per_row) as curr_cnt, ...
merge
  where deduped.sales.date.year = 2001
  select deduped.sales.item.brand_id as brand_id_prev, ..., sum(deduped.cnt_per_row) as prev_cnt, ...
align i_brand_id: brand_id_curr, brand_id_prev and i_class_id: ... (4 keys);
```
Both arms aggregate the **`deduped`** rowset (not the base fact directly), filtered
to different years, then align on 4 item-attribute keys. `deduped` exists for
UNION-DISTINCT dedup semantics across cs/ss/ws, so it can't simply be inlined.

The intended `join` form is two rowsets `curr`/`prev` that each
`sum(deduped.*) by <attrs>` under different year filters, joined on the 4 keys.

## Minimal repro (no nested-aggregate confound, no multi-key)
`base` is a row-grain passthrough rowset; `c`/`p` aggregate it per year and join on brand:
```trilogy
import sales as sales;
rowset base <- where sales.year in (2001,2002) select sales.sale_id, sales.item.brand, sales.year, sales.qty;
rowset c <- where base.sales.year = 2002 select base.sales.item.brand as brand, sum(base.sales.qty) as c_qty;
rowset p <- where base.sales.year = 2001 select base.sales.item.brand as brand, sum(base.sales.qty) as p_qty;
inner join c.brand = p.brand
select c.brand, c.c_qty, p.p_qty order by c.brand asc;
```
Data: items (1,br=10),(2,20),(3,30); sales rows year/qty:
2001 → br10:7, br20:3; 2002 → br10:7, br20:15, br30:4.
**Expected** `[(10,7,7),(20,15,3)]`. **Actual** 6-row cross product
`[(10,7,3),(10,7,7),(20,15,3),(20,15,7),(30,4,3),(30,4,7)]`. The `merge` form of the
same query ALSO breaks (`Binder Error: column "c_brand" not found`), so this is a
shared limitation, not join-specific.

## Root cause (diagnosed — start here)
Trace the rowset nodes (set `trilogy.constants.logger` to INFO, grep `GEN_ROWSET_NODE ... final output`):

- **Independent (working) case:** each rowset node's `final output` is
  `['p.brand', 'p.p_qty']` (or `['c.c_qty','p.brand']`) with grain **`Grain<p.brand>`**.
  The outer merge sees a shared `p.brand` grain key → joins on it.
- **Derived-twice (broken) case:** the nodes' `final output` still *lists* the key
  (`['c.c_qty','p.brand']`) BUT the node **grain is `Grain<Abstract>`**, not
  `Grain<p.brand>`. With no grain key, the outer merge's join-key inference finds
  nothing in common → `merge_node.create_full_joins` falls back to `1=1`. In the
  rendered SQL the *second* branch (`thoughtful`) even `GROUP BY br` but omits `br`
  from its SELECT — the dimension is computed but not projected.

So the failure is **grain collapse**: when the join-key dimension is sourced through
an intermediate rowset (`base.sales.item.brand` rather than the raw
`sales.item.brand`), the rowset node's recomputed grain degrades to Abstract, and
the join key stops anchoring the merge.

The grain is recomputed in `gen_rowset_node`:
```python
node.grain = BuildGrain.from_concepts([x for x in node.output_concepts
    if x.address not in [hidden non-rowset]])
```
Why does `from_concepts([c.c_qty, p.brand])` yield Abstract here but `Grain<p.brand>`
in the independent case? Likely because the advertised target `p.brand` carries a
different grain/keys when its lineage chains through `base` (a rowset) — chase
`p.brand.grain` / `.keys` and the `base.sales.item.brand` grain
(`Grain<base.sales.item.brand, base.sales.sale_id>`) to see why the brand key
doesn't survive as a standalone grain component. The `_scoped_join_targets` bridge
(below) DOES fire and advertise `p.brand`; the problem is downstream grain, not the
advertisement.

## How the independent-case fix works (context — don't re-do, build on)
`trilogy/core/processing/node_generators/rowset_node.py::_scoped_join_targets`:
when a scoped join collapses a rowset's derived key onto the sibling's (the join
*target*), the source rowset advertises that **target** concept sourced from the
shared base dimension it still exposes (`canonical.lineage.content.pseudonyms ∩
node_output_addresses`). The outer merge then joins on the canonical key. For a
computed key (q44 rank) there's no shared base column so it's inert. LEFT joins go
partial via `BuildEnvironment.scoped_partial_sources`.

Projection of a collapsed source is handled separately at render
(`utility.py::sort_select_output_processed` + `ProcessedQuery.scoped_merge_map`).

In the derived-twice case the bridge fires (target advertised) but the node grain
collapses — so the next fix is about **grain**, not advertisement.

## Likely shape of a correct fix
1. **Preserve the join-key grain through the intermediate rowset.** Ensure the
   rowset node's recomputed grain keeps `p.brand` (the advertised target / shared
   base) as a component when the key is sourced via another rowset. Investigate
   whether `_scoped_join_targets` should also normalize the target's grain to the
   shared base, or whether `BuildGrain.from_concepts` is dropping it because
   `p.brand.grain` references `base.*` keys that aren't in the node output.
2. **Make the second branch project its grouped dimension.** `thoughtful` groups by
   `br` but doesn't select it — confirm whether fixing the grain also fixes the
   projection, or whether the second arm needs the target added to its outputs the
   way the first arm got it.
3. **Fail loudly instead of cross-joining.** Independent of the real fix, a scoped
   `join` that can't find a real join key should NOT silently emit `FULL JOIN on
   1=1`. Add a guard (likely in `merge_node.create_full_joins` / the scoped-join
   path) that raises when a scoped join degenerates to `1=1`, so this class of bug
   surfaces as an error, not wrong data. The user explicitly flagged this footgun.
4. **Then** convert q75: replace the `merge`+`align` with two rowsets
   (`curr`/`prev`) aggregating `deduped` per year + a 4-key `inner join`. Keep the
   HAVING ratio and ordering. q64 is the harder follow-on (3-key align, marital-
   status cross-inequality deferred to the outer aggregate, and an OPEN align/derive
   name-collision GROUP-BY bug — see `handoff_scoped_join_yoy_rowset_parity.md` and
   the q64 notes in memory).

## Validation harness
- `tests/test_join_merge_parity.py::yoy_shared_parent` is the xfail for this exact
  shape (currently uses a *nested-aggregate* body — `sum(deduped.qty)` over
  `deduped.qty = sum(...)`. Prefer ALSO adding a clean non-nested case like the
  minimal repro above so the grain bug is isolated from the nested-agg issue).
- Flip `yoy_shared_parent` from xfail to pass when fixed; both the `join` and
  `merge` forms must equal the expected rows (the harness asserts both).
- Regression guards that MUST stay green: the full `tests/test_scoped_join.py`,
  `tests/test_join_merge_parity.py::yoy_independent_rowsets` and `rowset_fk_to_dim`,
  and TPC-DS `test_forty_four/forty_six/sixty_eight/ninety_seven_*` plus the full
  `tests/modeling/tpc_ds_duckdb/` sweep (133 passed at last run).
- After q75/q64 conversion: `test_seventy_five` / `test_sixty_four` must still match
  the PRAGMA/reference rows.

## Key files / functions
- `trilogy/core/processing/node_generators/rowset_node.py`: `gen_rowset_node`
  (grain recompute ~line 125), `_scoped_join_targets` (the target bridge).
- `trilogy/core/processing/nodes/merge_node.py`: `generate_joins` /
  `create_full_joins` (the `1=1` fallback when no shared grain key).
- `trilogy/core/models/build.py`: `_build_rowset_lineage` (~3130), grain
  normalization `_normalize_grain_components` (~3170), scoped substitution in
  `_build_concept` (~2554).
- `trilogy/core/processing/discovery_utility.py`: `get_priority_concept` (where the
  independent case used to dead-end before the fix).

## Pitfalls
- **Concurrent agents share this working tree.** Never mutate git state
  (no stash/reset/checkout of others' work).
- The `join` form here returns WRONG ROWS without erroring — don't trust "it ran";
  always diff against expected rows / the `merge` form.
- A failed `run_query` leaves a STALE `zquery<N>.log`; inspect SQL via
  `engine.generate_sql(text)[-1]`.
- Direct `execute_text` vs `PRAGMA tpcds(N)` can show false mismatches for
  tie-ordered queries; trust the pytest modeling test.
- `--` hides a select column (it is NOT a comment); a hidden derive-arg keyed off a
  non-align dim has previously forced spurious outer GROUP BYs (see memory).
