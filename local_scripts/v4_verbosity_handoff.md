# v4 verbosity / plan-size handoff (the migration-gating work)

Status: OPEN, `_TPCDS_SIZE` (rows correct, SQL longer than v3 → trips
`assert len(query) < ceiling`). **Not correctness** — full v4 sweep is 0 failed. This
is the main engineering left before v4 can be flipped on by default. For the genuine
*crashes* that were mis-bucketed here, see `v4_existence_recursion_handoff.md` (q10,
q2.1 are crashes, NOT size; q02 and q76 are already passing).

## Scope: 6 queries genuinely over ceiling (verbosity only)

Re-measured 2026-06-25 in isolation (`TRILOGY_V4_DISCOVERY=1`, real `engine` fixture):

| q | test | over by | dominant pattern |
| --- | --- | --- | --- |
| 2.2 | `test_two_two` | size | passthrough bloat + multi-fact many-sibling |
| 30.alt | `test_thirty_alt` | size | dimension re-join (pattern 2) |
| 73 | `test_seventy_three` | size | aggregate over-split + passthrough + dim re-join |
| 81 | `test_eighty_one` | size | dimension re-join (pattern 2) |

FIXED 2026-06-25 (`_spine_regraft_parent` in `group_graph.py`): **q47 `test_forty_seven`
and q57 `test_fifty_seven`** — NOT passthrough bloat as previously labeled. The bloat
was a redundant intermediate merge: the scalar BASIC `sum_minus_avg = sum_sales -
avg_monthly_sales` built its own CTE merging the base aggregate with the avg, then FINAL
re-joined the window node (`questionable`) — two joins where v3 needs one. v3 makes the
window the spine (it already exposes `sum_sales`) and joins `avg` straight onto it,
computing the difference inline. The fix adds a group-graph lineage edge from the
same-grain WINDOW sibling to the BASIC so the window's outputs ride through and
`_fold_descendant_contributors` collapses them into one FINAL contributor. Verified
stable across 5 hash seeds; broader v4 modeling sweep failure set unchanged; q72's
nondeterministic flake is unrelated (identical SQL with/without the change). Lock:
`tests/core/processing/test_v4_window_avg_difference.py`. (q47/q57 still listed in
`v4_known_failing.py` — prune after a full sweep; they now xpass.)

(Already cleared by the 2026-06-25 size fixes and now PASSING in isolation — prune from
`v4_known_failing.py` after a full sweep: q02 `test_two`, q76 `test_seventy_six`.)

Measure the v3-vs-v4 gap with `python -m local_scripts.v4_size_compare` (generation
proxy — runs higher than real because it skips datasource inlining; trust the real
test verdict for pass/fail, use the proxy only for relative deltas). Diagnose any one
query with:

```bash
.venv/Scripts/python.exe local_scripts/discovery_v4.py --query 73 \
  --diagnostics --diagnostics-dir local_scripts/v4_diagnostics --no-sql
```

Read `<stem>_strategy.md` (repeated datasources / unexpected fact scans) and
`<stem>_groups.md` (`__final__` merge grain + contributor contracts) before eyeballing
SQL.

## Three patterns, in recommended fix order

### 1. Passthrough-projection bloat — biggest, lowest-risk lever

v4 emits pure single-source projection CTEs (no join/group/agg/window/where) that just
re-project columns and should fold into their consumer. `_fold_passthrough_parents`
(`strategy_builder.py`) is supposed to absorb row-preserving projections but doesn't
fire for these shapes. Find why and widen it (it is pinned as a *physical redundancy*
optimization — it may absorb row-preserving projections but must not dissolve a
row-shape barrier; see `v4_audit.md` "Separation audit notes").

Touches q73, q2.2, and compounds pattern 2 (the `questionable ← cooperative`
passthrough in q81). (q47/q57 were mislabeled here — they were a redundant-merge issue,
now FIXED via `_spine_regraft_parent`; see the table note above.)

Likely investigation: a passthrough CTE survives when its consumer needs a different
*hidden* concept set, or when the parent is referenced by more than one consumer
(fold must be safe for the single-consumer case first). Confirm with the strategy
sidecar which CTE is the bare projection and who reads it.

### 2. Dimension-projection re-join — q81 & q30.alt (identical fingerprint)

The wide customer/address dimension projection gets re-sourced through the fact (4
joins + a dedup GROUP) instead of from `customer ⋈ customer_address` at customer
grain (v3: 1 join). **Full write-up with refined root cause and a regression trap
(q65) already exists: `local_scripts/v4_dimension_projection_rejoin_handoff.md`.**
Read it before touching this — a naive fix got q81 under ceiling but regressed q65;
the robust fix must be per-entity / FD-cluster aware. Higher risk than pattern 1.

### 3. Aggregate over-split — q73

v3 renders all joins + dims + GROUP in a single SELECT; v4 splits the dim projections
into their own CTEs first (v3 1 CTE → v4 4). Overlaps heavily with pattern 1 — try the
passthrough fold first and re-measure q73 before treating this as separate.

## Acceptance

- Each query's `test_*` passes under v4 (`len(query) < ceiling`) with rows unchanged.
- No regression in the 2026-06-25 size wins (q12/q20/q50/q62/q23/q94) or the
  join-stream aggregate-reuse locks (`test_v4_virt_filter_extra_cte.py`,
  `test_v4_dimension_projection_group.py`).
- Full v4 sweep stays at 0 failed. Re-run a full sweep before claiming any query
  cleared — the classifier only re-checks already-listed tests.
