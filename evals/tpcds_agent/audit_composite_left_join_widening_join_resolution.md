# Audit: composite mixed-key LEFT join 3× fanout — NOT the rollup refactor

## Verdict

`tests/test_scoped_derived_rowset_join_matrix.py::test_composite_mixed_key_left_join_should_not_widen`
is broken by the **`join_resolution.py` metric-payload change** (the q59
nullable-measure-stitch-keys work), not by the rollup/grouping build-time
refactor. Both landed bundled in commit `b3c7e7f7a big_rollup_grouping_fixes`,
which is why it looked like the rollup session's regression.

## Evidence

Commit bisect (test passes at every earlier state):

| commit | result |
|---|---|
| b76e8fbf9 / 8bd33d1bc / b0302b74a / 27d591c66 | pass |
| b3c7e7f7a (rollup refactor + join_resolution change) | **fail** |

File-level A/B inside b3c7e7f7a:

- b3c7e7f7a with ONLY `join_resolution.py` reverted to 27d591c66
  (full rollup refactor present): **pass**
- b3c7e7f7a with the rollup files (author.py, build.py, statements/author.py,
  select_finalize.py) reverted, NEW join_resolution kept: **fail**

## Mechanism

Generated-SQL diff (passing → broken), final stitch between the two rowset
arms:

```sql
- FULL JOIN "cooperative" on ...agg_s_region = ...fut_s_region
-                        AND ...agg_tot = ...agg_tot
+ FULL JOIN "cooperative" on ...agg_s_region = ...fut_s_region
```

The new `payload_nodes` rule in `resolve_join_order_v2` ("a shared METRIC only
stays a join key when it is the pair's sole connection") drops `agg_tot`
because `agg_s_region` also connects the pair. But region alone is NOT a row
identity for these arms — the composite scoped-join key's other component
(`agg.period + 53 = fut.period`) is a *derived* key living under different
addresses per side, and the shared metric was the planner's stand-in
reconstructing that identity. With only region, 3 periods per region
cross-match: `sum(agg.tot)/sum(fut.tot)` = 12.16 (exactly 3 × 4.05), and
right-only periods that must be NULL under LEFT narrowing acquire values
((54, 3.0) vs (54, None)).

## Suggested direction (for the join-resolution owner)

The q59 need (null-safe `=` on metric stitch keys, `get_modifiers` METRIC →
NULLABLE) is separable from the key-drop heuristic. Dropping a shared metric
from the join keys is only sound when the remaining connections form a
complete row identity for both sides (same class of invariant as the rollup
grouping-set identity work: join keys must identify rows, and "visible
non-metric columns" don't guarantee that). Either prove key coverage against
the arm grains before dropping, or restrict the drop to pairs where the
non-metric connections include every grain component of at least one side.
