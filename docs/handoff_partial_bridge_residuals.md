# Handoff: partial-bridge residuals — status after the 2026-08-19 session

## Done this session

**Case B (unlicensed transitive-dim extension) is FIXED, and
`validate_partial_bridges` / `UnconstrainedPartialBridgeException` are
DELETED.** The two-`~` span now generates the union-of-branches shape
directly. See `docs/partial_bridge_pinning.md` for the ruling semantics (two
kinds of NULL, licensing, the span table).

### The Case B fix (one rule, `join_resolution.py::ensure_content_preservation`)

`get_join_type` was already right (INNER for the transitive-dim key); the FULL
came from `ensure_content_preservation` upgrading every join downstream of a
FULL to FULL — granting row-preservation to a relation with no license. The
rule now:

- a prior FULL still forces LEFT-preservation of the accumulated stream
  (padded rows carry NULL join keys and must survive);
- it grants the new RIGHT relation preservation only when this join is keyed
  ON the prior FULL's own coalesced spine keys (`review_keys <= pred_keys`) —
  every extension family carries the spine, so the relation spans the whole
  stream (q75's item over the sales/returns stitch relies on this);
- a join keyed off-spine (one side's non-key column, padded NULL for the
  other family) gets NO license from the upgrade: LEFT, not FULL. That was
  the ORPHAN leak.

Wrong turn to avoid re-trying: downgrading ALL post-FULL joins to LEFT (drop
`has_prior_right` unconditionally) breaks tpc-ds q75 — the plan-level rows
are identical (LEFT + WHERE ≡ INNER + WHERE), but condition routing downstream
of the join typing diverges and the rowset WHERE (`category = 'Books'`) lands
only on the returns branches, silently unfiltering the sales side. Keying the
exception on authored `full_join_keys` also fails: q75's FULL is
partial-driven, not union-declared (the registry is empty at that call).

### Guard removal checklist (all done)

- `partial_bridging.py` reduced to the healing half; `_UnionFind`,
  `_home_keys`, `validate_partial_bridges`, the `anchored` carve-out and
  `UnconstrainedPartialBridgeException` deleted; seam call removed from
  `query_processor.get_query_node`.
- Error-asserting tests converted to row assertions:
  `test_keys_without_fact_anchor`, `test_dims_without_fact_anchor`
  (engine assembly), `test_pair_fact_span` (tpc_ds shapes, truth-based),
  `test_adhoc01_unpinned_span`, `test_adhoc02_span_through_agg` (thelook,
  truth-based — 284 rows, byte-identical to the hand-built reference).
- `test_partial_grain_with_customer_dim` and `test_item_customer_grain`
  flipped XPASS(strict) on the fix and are promoted to plain tests.
- The tpc-ds pair-fact probe returns exactly 25210 paired + 86 cust-ext +
  0 all-null, no duplicate pairs, measure total preserved.
- Discovery `_PINNED_PAIR_QUERY` pins dropped; eval smoke re-targeted to the
  generating behavior (`evals/thelook_agent` recovery metric retired).
- `docs/partial_bridge_pinning.md` rewritten.

### New coverage: the nullability matrix

`tests/engine/test_duckdb_nullability_matrix.py` — 3 `~` dims (customer with
transitive address, product, store), value NULLs in two fact FKs (`~?`),
13 row-pinned cells: single-key domain, aggregation with a value-NULL group
beside an extension row, 2- and 3-key spans, fact-anchored, attribute-only,
transitive-dim licensing both ways (complete binding → no ORPHAN;
`~` binding → ORPHAN extension row), and pins removing both extensions and
value-NULL rows.

Key modeling contract surfaced by the matrix: **value NULLs in a bound
column MUST be declared `?` on the binding** (`c_sk: ~?customer.sk`).
Undeclared, the planner is entitled to assume the column non-null:
`is not null` pins may be stripped as tautological and NULL-keyed fact rows
kept or dropped inconsistently by plan shape. `~` speaks only to member
coverage.

---

## Case A — FIXED 2026-08-19: by-key aggregate beside extra keys strands the FINAL cover

Fixed as the group-graph placement change the probe predicted (see "the fix
shape" below — it held). Two rules in `group_graph.py`:

1. `_widen_mixed_scalar_basic_to_final_spine` (runs beside the WINDOW grain
   widening in `_compute_concept_sets`): a BASIC scalar whose lineage mixes a
   ROOT row stream with a by-key aggregate (`order_status <- case when amount
   = min(amount) by user_id ...`) is pointwise over the merged row stream, not
   over its args' composite grain. When it feeds FINAL and its grain is a
   proper subset of the FINAL merge grain, widen `fact.grain`/`native_grain`
   to that spine — the group then hosts the spine merge itself (parents: the
   dim-peel root + the by-key aggregate), computes the CASE over the full
   extension-bearing stream ('LATER', not join-NULL), and carries every key
   the cover needs. Three gates, each earned by a regression:
   - the spine must be the result's own row identity: every spine key is a
     mandatory output or FD-determined by the mandatory set (q59's raw fact
     keys under a store/week output otherwise re-grain the scalar onto raw
     rows and narrow the authored union fan-out);
   - a grouping parent that NULL-injects its keys (ROLLUP/CUBE,
     `nulls_grouping_keys`) disqualifies the group — re-graining pairs
     subtotal rows on their NULLed keys (q36/q70/q86 wrong rows);
   - fires only for BASIC groups with BOTH a ROOT and a grouping LINEAGE
     predecessor.
2. ROOT capability extension: a dim-peel ROOT's secondary member (its own
   entity key, held outside the primary+source-grain capability) becomes
   capability when a NON-grouping successor's grain names it — without this
   the widened basic can't pull `item_id` through and the merge stays
   keyless. Grouping successors are excluded (they source grain keys through
   their own fact parents), which keeps the eager-axis-demand failures of the
   reverted attempts out.

All four pinned tests flipped XPASS(strict) and are promoted to plain
asserts. Full TPC-DS battery green (174/174), join_matrix green, nullability
matrix green. Historic summary of the defect:

`_cover_groups_for_mandatory` (`v4_helper/strategy_builder.py`) picks the
most-downstream provider per concept. With `order_status <- case when amount =
user_first_amount ...` over `user_first_amount <- min(amount) by user_id`,
that strands `item_id` on the qty-aggregate group and
`order_id`/`product_id`/`user_id` on the BASIC status chain: two FINAL
contributors with disjoint key sets, which
`_satisfy_parent_projection_contract` skips (shape barriers). The merge
cross-joins — caught by `_raise_if_keyless_row_bearing_join` in one shape
(loud), shipped as a silent 6x fan-out in the other.

The real fix is an FD spine for the FINAL merge: a contributor carrying the
complete row key plus its FD image — but DEMAND-DRIVEN (repair only a cover
proven join-disconnected). The eager `_add_root_spine_keys` attempt was
reverted: usually no ROOT contributor survives in the cover, and when one
does the re-source surfaced wrong pair-cost joins (9-row fan-out).

Do not relax `_raise_if_keyless_row_bearing_join`; fix the cover.

### 2026-08-19 findings (probed, not yet attempted)

- Post the Case B join fix, BOTH faces now crash loud at the canary (the
  24-row silent fan-out face is gone) — strictly better starting point.
- The failing assemble's cover really is `{agg: [item_id, total_qty]}` +
  `{basic: [order_id, user_id, order_status]}` with `product_id` finding NO
  provider at all (`_cover_groups_for_mandatory` `continue`s). A ROOT
  `dim:item_id` group exists in `built` on some discovery iterations but
  outputs only `(amount, order_id, product_id, user_id)` — its own dim key
  was grouped away by the wrap.
- A pure spine injection (fresh `plan_source` over the mandatory ROOT keys,
  added as an extra FINAL parent when contributors are join-disconnected) is
  attractive — the spine plan is exactly the passing `test_keys_only` shape,
  extension rows included — **but is NOT sufficient for the pinned
  expectations**: they require `order_status` = `'LATER'` on extension rows
  (the CASE's ELSE over a NULL `amount`), which only falls out if the scalar
  is COMPUTED over the merged, extension-bearing stream. A separate BASIC
  contributor joined on `(order_id, user_id)` yields NULL there instead.
  Compare `test_by_dim_key_aggregate_vs_row_value` (passing): `is_biggest`
  is NULL on the extension row because a bare comparison propagates NULL —
  consistent, the CASE's ELSE is what flips it to a value.
- So the fix shape is: keep `user_first_amount` (the by-key aggregate) as a
  joined contributor, but evaluate the CASE (and any scalar over row values)
  at/after the FINAL merge — i.e. the BASIC group hosting a scalar that
  mixes row values with a by-key aggregate must not become a row-bearing
  FINAL contributor at its own grain; its expression belongs downstream of
  the spine merge. That is a group-graph placement question
  (`group_graph.py`), upstream of the cover.

### Tests (promoted to plain asserts on the fix)

- `tests/engine/test_duckdb_partial_key_assembly.py::test_forked_with_status`
- `...::test_forked_with_status_pinned`
- `...::test_forked_full_column_set`
- `tests/modeling/tpc_ds_duckdb/test_partial_key_assembly_shapes.py::test_partial_grain_with_by_key_aggregate`

The `_FORKED` fixture is the whole repro — no TPC-DS needed, runs in ~1s.
