# BUG: composite scoped LEFT join with a MIXED derived+plain key splits and widens to FULL

**Classification:** framework bug — SILENT WRONG RESULTS. A scoped `left join` whose
composite key mixes a **derived expression** with a **plain** key renders as a FULL join on
the plain-key half, so unmatched right-side rows leak in (spurious NULL rows) instead of the
LEFT join the user asked for.

**Pre-existing / NOT a regression from the derived-rowset-join spike.** Reproduces on plain
projection (no re-aggregation, no `_enrich_via_derived_join_key` involvement) and is decided in
the scoped-join *type* machinery (`join_resolution.py` / `build.py`, the `scoped_left_anchor_keys`
family — same area as q78), not in the rowset enrichment. Surfaced by the derived-rowset-join
shape matrix (`tests/test_scoped_derived_rowset_join_matrix.py`).

**Status:** root-caused, recorded as an xfail. NOT fixed. Hand off to execute.

---

## Minimal repro

```trilogy
import sales as s;                  -- key sid; property period, region, amt
rowset agg <- select s.period, s.region, sum(s.amt) as tot;
rowset fut <- select s.period, s.region, sum(s.amt) as tot;

select agg.period, agg.region, agg.tot, fut.tot
left join agg.period + 53 = fut.period and agg.region = fut.region;   -- renders FULL
```

## Trigger matrix (the diagnostic part)

| composite key | join | result |
|---|---|---|
| `agg.period = fut.period and agg.region = fut.region` (both plain) | `left` | ✅ LEFT |
| `agg.period + 53 = fut.period and agg.region = fut.region` (mixed) | `left` | ❌ **FULL** |
| `agg.period + 53 = fut.period and agg.region = fut.region` (mixed) | `inner` | ✅ INNER |

Both-plain LEFT is correct (the `scoped_left_anchor_keys` machinery from q78 handles it). INNER
is correct. Only the **LEFT + mixed-key** intersection breaks. Independent of projection
(plain / re-aggregated both widen).

## Root cause

The composite key is meant to be ONE join: `agg LEFT JOIN fut ON (period+53 = fut.period AND
region = fut.region)`. Instead it is **decomposed into two nested joins at different levels**,
because the derived key and the plain key flow through different scoped-join paths:

```sql
... LEFT OUTER JOIN "cooperative"  on cheerful._virt_func_add_… = cooperative.fut_s_period      -- derived key: correct
... FULL JOIN        "questionable" on wakeful.agg_s_region = questionable.fut_s_region          -- plain key: WIDENED
                                    AND wakeful.agg_tot      = questionable.agg_tot
```

The derived-key half resolves to a clean `LEFT OUTER JOIN` on `_virt_func_add`. The plain-key
half (`region`) is emitted as a SEPARATE `FULL JOIN` (and spuriously also keys on the `agg_tot`
measure). Because the two halves of one composite key are not combined into a single ON clause,
the region half loses the left-anchor directionality and `reduce_join_types`/content-preservation
widens it to FULL — so right-only (`fut`) rows survive with NULL `agg.*`.

In `resolve_join_order_v2` the anchor is registered (`ANCHOR_KEY_NODES = {agg.s.region,
_virt_func_add}`, both partial on the `fut` side), but the derived key and the plain key end up on
different datasource-join boundaries, so the anchor-seeding that keeps the plain composite LEFT
directional doesn't span the split.

## Fix direction (for the executor)

The two halves of a single composite scoped join must resolve as ONE join with a combined ON
clause (and one directionality), regardless of whether individual keys are plain or derived —
rather than the derived key materializing at one CTE level and the plain key joining at another.
Likely in how the scoped merge map / `scoped_partial_derived` interact with composite keys, and
how `get_node_joins` groups connecting keys across the derived-key CTE boundary. The plain
composite-LEFT path (q78 `scoped_left_anchor_keys`) is the correct-behavior reference; the
derived key must not split the join.

## Guardrails (must not regress)

- `tests/test_scoped_derived_rowset_join_matrix.py` — the matrix; the `*-comp_mixed-left`
  correctness xfail flips to pass when fixed. Single-key derived joins and both-plain composite
  LEFT must stay correct.
- q78 (`scoped_left_join_multi_partial_anchor`), q29 (nullable inner not widened to full).

## File pointers

- `trilogy/core/processing/join_resolution.py` — `resolve_join_order_v2` / `get_node_joins` (join type + key grouping).
- `trilogy/core/models/build.py` — `_build_scoped_merge_index`, `scoped_left_anchor_keys`, `scoped_partial_derived`.
- `tests/test_scoped_derived_rowset_join_matrix.py::test_composite_mixed_key_left_join_should_not_widen` — the recorded xfail.
