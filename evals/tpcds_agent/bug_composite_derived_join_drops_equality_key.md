# BUG: composite cross-rowset join with a derived key silently DROPS the equality co-key

**Classification:** framework bug — **SILENT wrong results** (no error, no crash). A cross-rowset
scoped join that ANDs a plain-equality key with a derived-expression key emits ONLY the derived
key; the equality key vanishes from the join, fanning the result out across all values of the
dropped key. This is the dangerous class — the query "runs fine" and returns corrupted numbers.

**Regression from the derived-rowset-join spike** (`_enrich_via_derived_join_key`, `rowset_node.py`).
Same code family as the INNER-disconnect fix and the LEFT-recursion fix; this is its composite-key
blind spot.

**Status:** FIXED 2026-06-30. `_enrich_via_derived_join_key` now also sources the
plain-equality co-keys (`scoped_inner|full|left` registries minus the derived key +
its pseudonyms/other-side) onto the enrich node and un-hides the anchor-side co-key,
so `get_node_joins` infers a join carrying EVERY authored key. Confirmed by
`repro_composite_derived_join_drops_equality_key.py` (4→2 rows) and
`tests/test_scoped_derived_rowset_join_matrix.py::test_composite_mixed_key_inner_join_keeps_equality_co_key`.

---

## What it is

```trilogy
rowset a <- where s.yr = 1 select s.store, s.period, sum(s.amt) as tot;
rowset b <- where s.yr = 2 select s.store, s.period, sum(s.amt) as tot;

select a.store, a.period, a.tot / b.tot as r
inner join a.store = b.store and a.period + 10 = b.period;
```

Intended join: same store, next period. Emitted join (from generated SQL):

```
INNER JOIN b ON a.period + 10 = b.period          -- a.store = b.store is GONE
```

So every `a` row matches `b` rows at `period+10` for **every** store. With one row per store
per period, the correct output is 2 rows (each store's own ratio); the bug returns 4, including
cross-store garbage (store1.tot / store2.tot):

```
store=1 period=1 ratio=0.05   <- 10 / 200  (store1 sales / store2 sales) WRONG
store=1 period=1 ratio=0.5
store=2 period=1 ratio=0.5
store=2 period=1 ratio=5.0    <- 100 / 20  WRONG
```

## Minimal repro

Self-contained, seeded, shows the wrong rows:

```
.venv/Scripts/python.exe evals/tpcds_agent/repro_composite_derived_join_drops_equality_key.py
```

## Trigger matrix (verified via generated SQL join ON clause)

| join written | emitted ON clause | equality key kept? |
|---|---|---|
| `a.store = b.store and a.period + 10 = b.period` | `a.period+10 = b.period` | ❌ **dropped** |
| `a.store = b.store and a.period = b.period` (both plain) | `a.store = b.store AND a.period = b.period` | ✅ kept |
| two SEPARATE clauses (`inner join a.store=b.store` / `inner join a.period+52=b.period`) | derived only | ❌ dropped |

Both the composite `and` form and the two-separate-clause form drop the equality key — so there
is **no join-syntax workaround** for the author. Only-plain-equality composites are fine, so the
trigger is specifically *equality key + derived-expression key in the same cross-rowset join*.

## Discovered on / impact

TPC-DS **q59** (store × week, this-year vs next-year `week_seq + 52` ratio). The agent's join
`inner join this_year.store_code = next_year.store_code and this_year.week_seq + 52 = next_year.week_seq`
dropped `store_code` → 2548 rows instead of ~260, every store's sales divided against every other
store's. The agent *sensed* the wrong row count but never found the cause and thrashed.

This is the likely root of several fan-out failures in the 20260630-235635 rebaseline where
candidate rows ≫ reference (q64 2→595, q73 1→82, q23 4→100, q66 5→48, q84 16→100, q17 23→100,
q77 44→76) — the per-entity period-over-period comparison pattern is common. Worth re-checking
those once fixed.

## Root cause / where to fix

`_enrich_via_derived_join_key` (`trilogy/core/processing/node_generators/rowset_node.py`,
introduced by the derived-join spike) merges the two rowsets over the derived key's pseudonym:

```python
return MergeNode(..., parents=[node, enrich_node],
    preexisting_conditions=conditions.conditional if conditions else None)
```

The merge is built from the DERIVED key alone; the sibling equality condition
(`a.store = b.store`) authored in the same scoped join is never carried into the MergeNode's join
condition. `_producible_derived_join_keys` / the enrichment only reasons about the single derived
key pair, so any co-key in the same join clause is lost.

Fix direction: when building the derived-key merge, include ALL other scoped-join conditions
between the same rowset pair (the plain-equality co-keys) in the MergeNode join — i.e. the merge
condition must be the FULL authored join predicate (derived key AND every co-key), not just the
derived key. Confirm the co-keys are available as real columns on both sides (they are: `store`
is a plain rowset output on both). Cross-check the INNER (`scoped_inner_join_keys`) and LEFT
(`scoped_left_anchor_keys`) paths both carry co-keys.

## Guardrails (must not regress)

- `tests/test_rowset_offset_join_contract.py` — single derived-key join, plain projection.
- `repro_derived_rowset_join.py` (INNER single-key) and `repro_left_derived_rowset_join_recursion.py`
  (LEFT single-key) must stay green.
- Two-plain-equality composite joins must keep both keys (control row in the matrix).
- Scoped-join correctness memories: q29, q78 (`scoped_left_anchor_keys`), q21.

## File pointers

- `trilogy/core/processing/node_generators/rowset_node.py` — `_enrich_via_derived_join_key` (MergeNode built from derived key only), `_producible_derived_join_keys`.
- `trilogy/core/processing/nodes/merge_node.py` — MergeNode join-condition construction.
- Sibling handoffs: `bug_derived_rowset_join_key_reaggregate_disconnect.md`, `bug_left_derived_rowset_join_recursion.md`.
