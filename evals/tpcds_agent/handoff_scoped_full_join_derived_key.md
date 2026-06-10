# Handoff: scoped FULL join on a *derived* key (spike)

**Status:** RESOLVED 2026-06-09. INNER, LEFT, and now FULL scoped joins on a
non-rowset derived key (`auto da <- amt+1; auto db <- cost+1; ... full join da = db`)
all work. Scoped joins now work across arbitrary (derived) concepts, like the
global merge.

## Resolution (what landed)

Three changes, all gated to the derived-key FULL path (root/rowset FULL keeps
its canonical-column machinery):

1. **Source both sides** (`build.py`, the gate loop): added
   `elif jt is JoinType.FULL and not _is_binding_keyed(s): scoped_merge_sources.add(s)`.
   FULL collapses source→target exactly like INNER, so the collapsed-away source
   needs the merge mechanism (mutual pseudonyms + own-identity build) to stay
   sourceable from its own derivation. With this, FULL plans + renders a real
   FULL JOIN, correct except the orders-only key column was dropped (the bug
   below).
2. **Coalesce the canonical key at the join CTE** (`merge_node.py`, after
   `resolve_concept_map`): for each FULL `BaseJoin`, union the two `ConceptPair`
   addresses' `source_map` entries so the canonical output (db) is sourced from
   BOTH sides (highfalutin=db AND wakeful=da). This makes the renderer emit
   `coalesce(highfalutin.db, wakeful.da)`. Same-address keys (root/rowset) are
   skipped — they already coalesce one shared column.
3. **Resolve the pseudonym column on the null-extendable side**, two spots:
   - `generate_source_map` (`query_processor.py`): a multi-source key now accepts
     a CTE that outputs a *pseudonym* of the key (wakeful outputs `da`, a
     pseudonym of `db`) — previously dropped because `db ∉ wakeful.output_columns`.
   - `CTE.get_alias` (`execute.py`): a second pass returns the pseudonym column's
     `safe_address` (`da`) when the concept (`db`) isn't an output column of that
     parent, so the coalesce emits each side's real column.

   The da↔db pseudonym DOES survive on the CTE output concepts at this point
   (the earlier spike's "pseudonym is lost" claim was about merge-node *input*
   concepts and no longer holds with change 1 in place).

Test: `test_scoped_full_join_on_nonrowset_derived_key` in
`tests/test_join_merge_parity.py` (orders da∈{10,20,99}, costs db∈{10,20,41} →
matched 10/20, orders-only 99, costs-only 41 via coalesce).

---

## (Original spike notes, for reference)

INNER and LEFT scoped joins on a non-rowset derived key work; FULL did not — it
dead-ended loudly (`ValueError: Cannot resolve query. No remaining
priority concepts`). This was the remaining piece of "scoped joins work across
arbitrary (derived) concepts, like the global merge."

## Context — what already works and how

See memory `project_scoped_join_mirrors_merge_concept` and
`handoff_q64_join_grain_resolution.md` (RESOLVED section). The mechanism:

- A scoped `join a = b` collapses source→target via `scoped_merge_map`. Unlike
  the global `merge`, that dropped the source's derivation, so a *derived* key
  was unsourceable from the source side. Fixed in `build.py` by mirroring
  `merge_concept` for `scoped_merge_sources`: mutual source↔target pseudonyms
  (`pseudonym_map` augmentation) + building the source as its OWN identity in
  `alias_origin_lookup` (evict the collapsed `local_concepts[source]` across
  `__build_concept`, then restore).
- `scoped_merge_sources` = all INNER sources + LEFT sources whose key is derived
  (not ROOT/ROWSET). **FULL sources are deliberately NOT included yet.**
- LEFT partiality: a derived key has no datasource column to carry
  `Modifier.PARTIAL`. The merge-mechanism keeps the partial source as a distinct
  output present only on the partial side, so `join_resolution.py` marks it
  partial by intersecting datasource outputs with `scoped_partial_derived`
  (= `scoped_merge_sources & scoped_partial_sources`, a new BuildEnvironment field).

## What FULL needs (the spike) — with concrete findings

**Step 1 is done and verified to work; step 2 is the real remaining piece.**

1. **Source both sides — DONE/EASY.** Add the FULL derived source to
   `scoped_merge_sources` (in `Factory.__init__`, the gate loop — there is a
   commented-out `elif jt is JoinType.FULL and not _is_binding_keyed(s)` placeholder
   right where LEFT is handled; un-defer it). With that, FULL **plans and renders a
   real `FULL JOIN`** with the correct structure and *correct results except one
   column* (see below). Sourcing is identical to INNER (FULL collapses source→target).

2. **The bug: the FULL-key coalesce drops one side.** For
   `full join da = db; select da, ...` with orders da∈{10,20,99}, costs db∈{10,20,40}:
   - matched (10,20) and the costs-only row (da coalesces to 40) are CORRECT;
   - the **orders-only row comes out `(None,3,None)` instead of `(99,3,None)`** — da's
     value is lost.
   Root cause: the inner CTE that performs the FULL JOIN projects the key from ONE
   side only —
   `SELECT "highfalutin"."db" as "db" ... FULL JOIN "wakeful" on "highfalutin"."db" = "wakeful"."da"` —
   so orders-only rows (highfalutin side NULL) get `db = NULL`. It must be
   `coalesce("highfalutin"."db", "wakeful"."da") as "db"`. (The OUTER CTE already
   coalesces, but by then both inputs only carry the lossy `db`.)

   **Why it's not trivial:** the `da<->db` pseudonym is LOST on the BuildConcepts by
   the time the merge node runs — `resolve_concept_map` (`nodes/base_node.py`) is
   called with `full_addresses={da,db}` but no input concept's `.pseudonyms`
   contains the other (verified by tracing). So you CANNOT key off concept
   pseudonyms. The pairing survives only in the join's `ConceptPair`
   (`join.concept_pairs` → `(left=db, right=da)`), which is exactly what already
   renders the correct `highfalutin.db = wakeful.da` ON clause.

   **Fix sketch (two parts):**
   - (a) In `nodes/merge_node.py`, after `source_map = resolve_concept_map(...)`
     (~line 483), for each `BaseJoin` with `join_type == FULL`, union the
     `source_map` entries of each `ConceptPair`'s two addresses, so
     `source_map[db]` includes BOTH highfalutin (db) and wakeful (da). That makes
     the coalesce in `safe_get_cte_value` (`dialect/base.py`, the 2-source branch)
     fire at the join CTE.
   - (b) Verify the renderer emits each side's ACTUAL column in that coalesce:
     `cte.get_alias(db, wakeful)` must yield `da`, not `db`. If it yields `db`
     (which wakeful doesn't have) you'll need get_alias to follow the pair/pseudonym
     per source. Check `CTE.get_alias` and `safe_get_cte_value`'s `_format`.
   - Keep it gated to the derived FULL path (don't disturb root/rowset FULL, which
     coalesce the canonical column literally on both sides — verified working).

## Repro / expected

```
key oid int; property oid.amt int;
datasource orders (o: oid, a: amt) grain (oid) address orders_tbl;
key cid int; property cid.cost int;
datasource costs (c: cid, k: cost) grain (cid) address costs_tbl;
auto da <- amt + 1; auto db <- cost + 1;
full join da = db
select da, sum(oid) as n_orders, sum(cid) as n_costs;
```
With orders da∈{10,20,99} and costs db∈{10,20,41}: FULL should yield
da=10,20 (matched), da=99 (orders-only, n_costs NULL), and a row for db=41
(costs-only, da NULL via coalesce, n_orders NULL). Currently dead-ends.

Add the parity-style test next to `test_scoped_left_join_on_nonrowset_derived_key`
in `tests/test_join_merge_parity.py`.

## Pitfalls

- Do NOT extend `scoped_merge_sources` to FULL without the coalesce — that would
  turn the loud dead-end into a silently-wrong INNER/LEFT-shaped result.
- Concurrent agents share the working tree; never mutate git state.
- Root/rowset FULL already works (`tests/test_scoped_join.py` FULL tests) — keep
  the derived-key path gated so you don't regress them (mirror how LEFT used
  `scoped_partial_derived`, not the broad `scoped_partial_sources`, to avoid the
  q77 rowset-LEFT regression).
