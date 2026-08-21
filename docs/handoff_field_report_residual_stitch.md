# Handoff: output-invisible null-safe stitches survive in the field-report plan

## OPEN 2026-08-21 — regression from origin/main, currently masked by a rescoped test assert

`tests/engine/test_duckdb_partial_fk_field_report.py::test_field_report_select`
originally asserted `"is not distinct from" not in sql` over the WHOLE
rendered statement (the PR #652 contract: no null-safe join stitches). On
origin/main that holds. On this branch one stitch survives, and the test's
syntax assert was rescoped to the final output assembly as a stopgap. Rows
are correct either way (the full 940-row comparison passes); the residue is
a contract/perf regression, not a wrong-rows bug. The acceptance criterion
for this handoff is restoring the original whole-statement assert.

## Repro

```powershell
.venv/Scripts/python.exe -m pytest tests/engine/test_duckdb_partial_fk_field_report.py -q
```

passes today (rescoped assert). To see the residue, render the test's QUERY
against its MODEL and grep the SQL. Current tree renders, inside `uneven`:

```
INNER JOIN "cooperative" on "abundant"."item_id" = "cooperative"."item_id"
FULL JOIN "cheerful" on "abundant"."order_id" is not distinct from "cheerful"."order_id"
                    AND "abundant"."product_id" is not distinct from "cheerful"."product_id"
```

The pre-regression baseline (verifiable by shadowing
`trilogy/core/processing/join_resolution.py` and
`trilogy/core/optimizations/value_set_join_upgrade.py` from commit
`d6fbefd5f` over the current tree via PYTHONPATH, cwd outside the repo)
renders the SAME plan shape with plain equality everywhere, and one further
difference: `cooperative`'s internal `orders FULL items` narrows to
`RIGHT OUTER`. Stripping the modifiers unlocks that narrowing; the surviving
stitch blocks it. So fixing this also recovers join narrowing.

## Why the stitch is there, and why it cannot be removed at the gate

`_gate_nullable_by_host` (join_resolution.py) decides whether a null-safe
pair survives. At merges with no host basis, three cases exist:

- shared padding (one source's rows arriving twice) must pair (fuzzer
  `padding_provenance/shared_scan_merge`);
- a FAMILY-ANCHORED join — one that also pairs on a licensed `~` key —
  reunites one extension member's halves manufactured in two branches, and
  must pair or rows split/duplicate
  (`tests/engine/test_duckdb_partial_key_assembly.py::test_forked_with_status`
  and `test_forked_full_column_set` pin the rows);
- a bare null-safe pair (neither) pairs "missing" with "missing" across
  unrelated trees and strips (this killed the plan's second, worse stitch:
  `INNER ... item_id is not distinct from`, which on richer data would have
  cross-paired unrelated extension families).

The surviving `cheerful` stitch is family-anchored (`product_id` is a
licensed `~` key): the pairing is the product-30 member's two halves — a
1:1, mechanism-sound reunion. The gate CANNOT distinguish it from the forked
reunions because they are the same shape at plan time. The difference is
CONSUMER-side: in the forked plans the merge output feeds the final result,
so the reunion is load-bearing; in the field-report plan the final assembly
re-anchors all extension rows from the dimension span (`concerned`) and
consumes the metric branch (`yummy`) as the right side of a plain-equality
LEFT join on `item_id` — so every padded (NULL-item) row in that subtree is
OUTPUT-INVISIBLE. Also note `uneven` projects zero columns from `cheerful`.

## The fix: consumer-side invisibility proofs (optimizer layer)

Invariant to implement: a producer consumed exclusively as the RIGHT side of
plain-equality LEFT joins cannot surface rows whose join key is NULL — those
rows never match and LEFT keeps only the left side. That is a forced-non-null
proof on the producer's join-key column, the same soundness argument
`_inner_pair_rejections` already makes for INNER joins ("a NULL key never
matches plain `=`, so the producer row contributes nothing").

Where: `trilogy/core/optimizations/join_upgrade.py`.

- `_inner_pair_rejections` currently harvests only `JoinType.INNER`. Extend:
  for `LEFT_OUTER`, the RIGHT cte's pair address is forced (never the left's);
  for `RIGHT_OUTER`, the left's. Null-safe pairs still prove nothing.
- `_external_forced_map` then propagates the proof producer-ward through
  single-source projections and group keys exactly as it does today (the
  q64 `cnt_99` machinery). `yummy.item_id` renders single-source from
  `uneven`, `uneven.item_id` from `cooperative`, so the proof reaches the
  stitched merges.
- Gap: nothing today STRIPS a NULLABLE modifier. `UpgradeJoinOnGuards`
  changes join TYPES only; the `is not distinct from` text renders from
  `Modifier.NULLABLE` on the pair. Add modifier stripping when the proof
  covers the pair's key on the relevant side(s): a null-safe pair whose key
  is proven non-null on one side can never match a NULL on the other, so the
  null-safety is dead and plain `=` is row-identical. With the modifier gone
  the ordinary narrowing (FULL -> directional -> INNER) should reproduce the
  baseline (`cooperative` RIGHT OUTER included).
- Careful with the existing caveats in that file: `_blocked_partials`
  (partial keys reachable from a complete copy off the operand), COALESCE
  multi-source renders (mask one-sided NULLs — `_renders_exclusively_from`),
  row-limited CTEs, window/existence consumers. All already have guards;
  reuse them.

## Constraints — all of these must stay green

- `tests/engine/test_duckdb_partial_fk_field_report.py` with the ORIGINAL
  whole-statement assert restored (that restoration is the acceptance test).
- `tests/engine/test_duckdb_partial_key_assembly.py` — the forked reunions
  are row-pinned; their merges feed the output, so the invisibility proof
  must NOT fire there (their consumers read the merge directly, not through
  a plain-equality LEFT on the padded key).
- `tests/core/processing/test_join_padding_provenance.py` — the gate's
  no-basis matrix (anchored pairs / shared pairs / bare strips).
- `tests/engine/test_multi_fact_nullable_fk_extent.py` — the `?` extent
  contract, incl. the q98 SQL-shape test and the three-fact chain.
- `tests/join_matrix`, `tests/generators/test_utility.py`.
- Fuzzer: `.venv/Scripts/python.exe -m local_scripts.fuzzer` (228 cases; the
  `padding_provenance` family especially).
- Corpus A/B: render all `tests/modeling/{tpc_ds_duckdb,tpc_h,thelook_duckdb}/query*.preql`
  twice in ONE process (rule change on vs off) and byte-diff per query;
  row-validate any query whose SQL changes via its modeling test. Gate
  against the CURRENT tree, not stale goldens. A LEFT-side invisibility rule
  will likely fire broadly (it is a general narrowing) — expect and review a
  real footprint, don't assume zero.
- Never run two pytest processes concurrently (shared modeling DuckDB).

## Context docs

- docs/handoff_multi_fact_nullable_fk_extent.md — the `?` extent contract
  and the family-anchor gate decision this residue fell out of.
- docs/subset_union_join_design.md — row-preservation-by-default; narrowing
  only on proof. The invisibility proof is exactly such a proof.
