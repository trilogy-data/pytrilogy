# HANDOFF — q94: `UpgradeJoinOnGuards` unsoundly upgrades a LEFT join to INNER on a shared semijoin key (silent row drop)

**Status:** FIXED 2026-07-05. Root cause differed from the theory below: the proof came from the
CTE's OWN membership condition (not `_inner_pair_rejections` cross-CTE transfer), enabled by two
compounding holes: (1) `_seed_ctes`/`_seed_addresses` picked an existence-only parent (the
`base_orders` WHERE-subselect) as the FROM seed, making the shared join keys look right-only;
(2) `_blocked_partials` used intersection (`binds & operand_ds`) instead of subset
(`binds <= operand_ds`), so a partial key rendering as COALESCE across both sides wasn't blocked.
Both fixed in `trilogy/core/optimizations/join_upgrade.py`; guards in
`tests/optimization/test_join_upgrade.py` (`test_semijoin_on_shared_key_keeps_padded_returns_rows`
and siblings). Agent's exact query94 now returns order count 32 (ground truth); P1/P2/P4 verified
unchanged; optimization + join_matrix + tpc-ds suites pass.

Follow-up (same day): the fix initially also relaxed q95's legitimately-INNER web_returns join
(its old INNER was licensed by the same unsound coalesce-key path — right answer, wrong reason).
Restored soundly by two additions: (a) `_seed_ctes` prefers the first join's `joinkey_pair.cte`s
over the dependency-order scan (the existence-parent misdetection recurred in q95 because
`existence_source_map` was empty for a filtered-concept membership); (b) `flag IS/= True` where the
flag's lineage is a boolean condition now yields that condition's non-null proofs
(`condition_utility._forced_true_proofs`) — `is_returned is True` proves `_returned_order_number`
non-null, which genuinely rejects the padded rows. Guards:
`test_flag_is_true_with_semijoin_upgrades_padding_join`, `test_flag_is_true_lineage_proofs`.
**Full diagnosis:** `evals/tpcds_agent/bug_q94_is_returned_leftjoin_upgraded_to_inner.md`
**Classification:** REAL framework bug, **SILENT** (wrong rows, no error). Optimizer soundness hole.
**Related:** same `join_upgrade.py` family the rule's own docstring cites for q64; sibling to the
q78 `strip_redundant_not_null` nullability-model bug — see
`handoff_q78_strip_redundant_not_null_intrinsic_nullability.md`. **Fix/review these two together
as one nullability-model hardening pass.**

## Symptom
q94 excludes orders that had any web return. The `web_sales` model exposes
`auto is_returned <- _returned_order_number is not null` (a NULL-padded column from a partial
`~` `web_returns` datasource). The agent correctly aggregated `bool_or(is_returned)` per order to
find no-return orders — but **every order came back `has_return = true`**, so the query returned
`[]`. Ground truth = **32 orders** (`(32, 65144.28, -19548.52)`).

The per-order aggregate CTE emits **`INNER JOIN "web_returns"`** where it must be LEFT OUTER —
dropping every non-returned (null-padded) line. At order grain `bool_or(is_returned)` can then
only be `true`, and zero-return orders vanish.

## Root cause
`UpgradeJoinOnGuards` (`trilogy/core/optimizations/join_upgrade.py`, class at **:834**;
cross-CTE proof gathering `_external_forced_map` **:735-831**; gate `_external_proofs` **:851-883**).

The rule upgrades LEFT→INNER when a downstream consumer "proves non-null" a producer output.
Here the **semijoin membership** `ws.order_number in base_orders.ws.order_number` renders as an
INNER equality on `order_number` (`_inner_pair_rejections` **:787**), which the rule reads as a
non-null proof on the producer's `order_number` — licensing the upgrade of the *web_returns* join.

This is **unsound**: `order_number` is the shared join key and is non-null on the padded
(non-returned) rows too — it renders as `coalesce(wr.WR_ORDER_NUMBER, ws.WS_ORDER_NUMBER)`,
non-null from the left source. Rejecting null-`order_number` does **not** reject the padded rows,
but the INNER upgrade removes them anyway. The COALESCE-mask guard at **:883**
(`len(set(cte.source_map.get(a))) == 1`) fails to exclude `order_number` because it registers
single-source (a real grain key mapped to the web_sales datasource) even though it *renders* as a
COALESCE across both sides.

## Trigger matrix (must hold after fix)
| # | shape | web_returns join | `is_returned=false` reachable? |
|---|---|---|---|
| P1 | plain `where ws.ship_address.state='IL'` + `bool_or(is_returned) by order` | LEFT | yes (546) ✅ |
| P2 | no filter + `bool_or(is_returned) by order` | LEFT | yes (17758) ✅ |
| **P3** | `where ws.order_number in bo.ws.order_number` + `bool_or(is_returned) by order` | **INNER** | **no (0)** ❌ must become LEFT |
| P4 | plain where, then downstream `where oal.hr=false` | LEFT | yes ✅ |

Differentiator: a **semijoin membership on the shared join key** (P3), not a filter on
`is_returned`. Adding web_sales measures does not change it.

**Toggle proof:** the agent's exact submitted `query94.preql` returns `[]` by default;
with `CONFIG.optimizations.upgrade_condition_joins = False` it returns order count **32** (matches
ground truth). (Shipping/profit totals still differ — a separate agent modeling choice, not this bug.)

## Fix direction (choose one; prefer the first)
1. **Exclude outer-join ON-keys from the forced-non-null set.** When gathering non-null proofs, a
   producer output that is one of the *outer join's own ON-keys* must not license upgrading that
   join — rejecting null on the key cannot reject the padded rows (the key survives on the padded
   side via the left source / COALESCE). Filter these out in `_external_forced_map` / `_external_proofs`.
2. **Fix the COALESCE-mask guard (:883)** to key off the join key's **render-time provenance on the
   padded side** (does it resolve to a COALESCE / multi-side value?) rather than `source_map`
   cardinality. `order_number` renders multi-side even though `source_map` says single-source.

Recommendation: #1 is the crisp invariant ("a join's own ON-key can't prove away that join's
padding"); #2 is the narrower guard fix. Consider both — #1 as the rule, #2 as defense in depth.

## Test to add
DuckDB codegen+execute test (near the join-upgrade / optimization tests): the P3 shape must render
LEFT (not INNER) and keep `is_returned=false` reachable; assert P1/P2/P4 still pass. Add an
end-to-end assertion that the canonical q94 form returns 32 orders. Keep a case that confirms a
*legitimate* LEFT→INNER upgrade (non-join-key non-null proof) still fires — don't over-suppress.

## Acceptance criteria
- P3 renders LEFT; `is_returned=false` reachable; canonical q94 → 32 orders.
- P1/P2/P4 unchanged; a genuine non-null-proof upgrade still upgrades (regression guard).
- **Re-check q64** (`is_returned` + `C_CURRENT_ADDR_SK is not null`, cited in the rule's docstring)
  after tightening — it must not regress.
- No regression in `tests/join_matrix/` and the optimization suite.
- `ruff check . --fix && mypy trilogy && black .` clean.

## Do NOT
- Do NOT disable `UpgradeJoinOnGuards` wholesale — it's a real optimization; only stop it from
  treating an outer join's own shared ON-key as a non-null proof against that join.
