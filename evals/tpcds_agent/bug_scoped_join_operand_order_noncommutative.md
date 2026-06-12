# Bug: multi-key query-scoped INNER join is not commutative in operand order

**Status:** FIXED 2026-06-12 (found enriched eval q72). Reliable repro:
`evals/tpcds_agent/repro_scoped_join_direction.py` (now `identical=True` for all
three rows). Regression tests: `tests/test_shared_dimension_bridge.py` (pure
diamond, no merge/join) + `tests/test_scoped_join_bridge_commutativity.py`.

## Root cause + fix

Not specific to the scoped join at all — a general **bridge-pruning bug** in
`resolve_subgraphs` (`select_helpers/source_scoring.py`). The subset-drop
(`value.issubset(other_value)`) compares only *requested* concepts, so a
dimension datasource that surfaces just the shared attribute (`inv.date` →
`week_seq`) looks like a subset of the other fact's date dim (`cs.sold_date` →
`week_seq` + `year` + `id`) and is dropped — even though it is the ONLY join
path from the second fact (`inventory`) to `week_seq`. Dropping it joins the two
facts on item alone → ~18× fan-out. Operand order only flips *which* date dim
becomes the droppable "subset", hence the non-commutativity.

Reproduces with a PURE model (no `merge`, no `join`): two facts reaching one
shared `week_seq` through two different date dims (a diamond). See
`tests/test_shared_dimension_bridge.py`.

Fix: in the subset check, don't drop `key` if it is the unique join-bridge to a
datasource that is the *sole provider* of a requested concept (`key_only_bridges
& sole_providers`). A bridge to a datasource that is redundant elsewhere stays
prunable, so the ambiguous-alternative-path resolution
(`test_ambiguous_error_with_forced_join_order`) is preserved.

NOTE: the explicit global-`merge` form of the same shape is a SEPARATE, still-open
bug — it takes a different path (trailing `LEFT OUTER JOIN` on `week_seq` via a
distinct CTE that doesn't constrain). Not addressed here.

---
(original report below)

## Symptom

For an INNER join, `inner join A.k = B.k` must equal `inner join B.k = A.k`. In a
query-scoped join it does NOT, when BOTH conditions hold:

1. there are **two** scoped-join keys (q72: `item.id` AND `week_seq`), and
2. the SELECT projects a **property of one of the joined dimensions**
   (`cs.item.desc`) rather than the bare join key (`cs.item.id`).

Flipping the operands of both joins then fans the result out ~18x:

```
BUG: 2 keys + project item.desc        A=  53712  B= 955347  identical=False
control: 2 keys + project item.id      A= 961540  B= 961540  identical=True   <- bare key: commutative
control: 1 key  + project item.desc    A=  44595  B=  44595  identical=True   <- single key: commutative
```

`A` = `cs.item.id = inv.item.id` (+ `cs.sold_date.week_seq = inv.date.week_seq`);
`B` = the same two joins with operands swapped. Both are INNER. Run the repro to
reproduce (uses the raw ingested model + `tpcds.duckdb` in any
`results/<run>_enriched/workspace`).

## How it surfaced (q72)

The passing agent rep wrote `inner join cs.item.id = inv.item.id`; a failing rep
wrote `inner join inv.item.id = cs.item.id`. Same query otherwise — the operand
order alone flipped pass→fail (the `B` form fans out and over-counts). The agent
cannot be expected to pick the "right" side; an INNER join should commute.

The q72 **reference** avoids the whole thing by scoped-joining only on `item.id`
and putting the `week_seq` equality in `WHERE` (so there's a single join key) —
which is exactly the single-key control that stays commutative.

## Minimal trigger (both required)

- **Two scoped-join keys.** With one key the result is commutative even when
  projecting `item.desc` (control B).
- **Projecting a joined dimension's *property*** (`item.desc`), not the bare key.
  With two keys but projecting `item.id` it's commutative (control A).

So the interaction is: resolving a projected property of a joined dimension
(`cs.item` → `item.desc`) under a *multi-key* scoped join is sensitive to which
operand is written on the left. One order resolves the property at the correct
grain; the other fans it out (955347 vs 53712 rows).

## Where to look

The query-scoped join collapses join keys into equivalence groups and picks a
canonical key per group (see memory notes / `build.py` `merge_concept` mirroring,
`reference_scoped_join_blend_semantics`). With two keys + a projected dimension
property, the canonical-key / source-side choice likely depends on operand order,
changing which CTE supplies `cs.item.desc` and at what grain. Candidate areas:

- `trilogy/core/models/build.py` — scoped-join key collapse / `merge_concept`
  mutual-pseudonym + `alias_origin_lookup` (the INNER scoped-join path).
- `trilogy/core/processing/discovery_*` — how a projected property of a
  scoped-join key's dimension is sourced when ≥2 keys are present.

## Reduction notes (for whoever picks this up)

- Could NOT reproduce in a minimal synthetic model: two facts sharing `item`
  (+ optional `week` direct column, + `week_seq` via a date dimension, + warehouse
  FK) all stay commutative. The trigger needs the RAW (auto-ingested) model's
  inferred grain/FKs — so it may be a grain-inference interaction, not pure
  scoped-join logic. Start by diffing the generated SQL of `A` vs `B` from the
  repro and finding where `cs.item.desc`'s source CTE / GROUP BY grain differs.
- Earlier false leads (ruled out, don't rechase): a "3162 vs 3126" single-key
  signal was a `LIMIT` without a stable `ORDER BY` (non-deterministic subset);
  the demographic filters and `count` vs `count_distinct` do NOT matter (bisected).

## Repro

`python evals/tpcds_agent/repro_scoped_join_direction.py [WORKSPACE_DIR]`
Exits 1 and prints the table above when the bug reproduces.
