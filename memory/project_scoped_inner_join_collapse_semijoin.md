---
name: project_scoped_inner_join_collapse_semijoin
description: scoped INNER join collapse silently drops the filter when one side isn't projected; fix is a datasource-level semijoin, not key-injection
metadata:
  type: project
---

OPEN (spiked + reverted 2026-07-01). A query-scoped `inner join a = b` folds both
keys onto one canonical (union-find + substitution/pseudonym in build.py
`_build_scoped_merge_index` / `_build_concept`). When ONE side contributes no
projected column, discovery sources the other side alone and drops the join
entirely → the INNER filter is SILENTLY lost. Confirmed on BOTH fact/dim
(`select a_id,a_val inner join fk=b_pk` keeps a-rows with no b match) AND
rowset↔rowset (q38/q14 intersection). Projecting BOTH sides works today (parity
tests do this); projecting the pruned side → `INVALID_REFERENCE_BUG`. The
`_validate_cross_rowset_inner_joins` guard (rowset_node.py) only papered over the
rowset case with a (nonsensical-for-scalars) "rewrite as union" error.

The user's call: remove the collapse; a scoped INNER is a real filtering join like
LEFT/FULL. Global `merge` KEEPS substitution (its identity contract asserts the
swap — see handoff_scoped_join_substitution_removal.md).

SPIKE (reverted, don't repeat the blunt part): de-collapsing the key
(identity+pseudonym via a new `scoped_inner_identity_sources`, exempt from
`_build_concept` subst + `_normalize_grain_components` + `scoped_key_merge_map`)
is CORRECT + necessary, and gated ROOT=ROOT it kept the rowset-derived machinery
(~25 `test_scoped_derived_rowset_join_matrix` tests) green. BUT the mechanism to
force the absent side in — injecting the bare join-key concept into
`search_concepts` in `get_query_node` (query_processor.py ~782) as hidden — is TOO
BLUNT: for a property/FK key shared via import (e.g. `week_seq`, a property of
`date`), injecting the bare key sources it from the SHARED DIMENSION (`dates`),
disconnected from the fact that owns the relationship (inventory) → the join
filters against a free-floating all-weeks dimension → filter lost. Regressed
TPC-DS **q72** + `test_scoped_join_bridge_commutativity` (extra week-200 row). Key
injection only works when the key IS the other datasource's grain (`fk=b_pk`).

CORRECT MECHANISM (the user's "tempting" semijoin, tie it to the datasource):
materialize the SPECIFIC other datasource grouped to the join key as a semijoin —
`a where a.k in (select k from b where <b's scoped where>)`. Grouping to the key
dedups (no fan-out; verified naturally for property keys). The de-collapse +
pseudonym lets a shared-column WHERE narrow both physical sides. Reuse the
existence machinery (`append_existence_check`, `existence_arguments`,
`render_composite_membership` in dialect/base.py — mapped in the 2026-07-01
explore).

Acceptance spec (all strict-xfail): `tests/test_cross_rowset_inner_join_planner.py`
(fact/dim one-side filter + no-fanout, rowset intersect select + count). Family:
[[project_q38_cross_rowset_inner_join_intersect_sentinel]],
[[project_crossrowset_inner_join_grainless_scalar]]. Do it incrementally: fact/dim
FK/property first, then rowset↔rowset (which also hits a mutual-pseudonym BUILD
cycle for shared-parent rowsets — `yoy_shared_parent` — when both keys keep
identity). Contracts to rewrite when it lands: `test_buildenv_*` (identity+pseudonym
not address-swap), retire `_validate_cross_rowset_inner_joins`.
