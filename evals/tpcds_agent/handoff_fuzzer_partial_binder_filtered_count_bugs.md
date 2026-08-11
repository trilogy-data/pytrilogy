# Handoff: fuzzer extension found 2 partial-binder wrong-results bugs (+ 11 older regressions)

**Status: BUGS CHARACTERIZED with minimal repros, NOT fixed. From the
2026-08-11 fuzzer extension covering the new multi-arg `count_distinct` /
`grain()` counting and composite-membership surfaces.**

## What was added

Two families in `local_scripts/fuzzer/generate.py` (16 cases/seed; corpus
186 → 218 fixed-seed cases):

- `distinct_count` — multi-arg `count_distinct(a, b)`, the `count(distinct
  a, b)` alias, explicit `grain(...)` counting (value-distinct vs
  row-population semantics), filtered/having/bridged variants. Oracle idiom:
  DuckDB `count(distinct row(...))` matches grain() totality exactly (a
  struct with a NULL member is not NULL).
- `composite_membership` — tuple membership `(a, b) in (m.a, m.b)`
  cross-fact in plain WHERE (null-safe `is not distinct from` oracles),
  NOT IN, AND-composed, expression components, fact-anchored rowset pair
  sets, and the q17 remedy shape (rowset pair membership inside an
  inline-filtered aggregate). **All composite_membership cases pass** — the
  existence-island machinery is solid where it plans at all.

## BUG 1 — spurious partial-binder join fans out a filtered grain() count

Minimal repro (fuzzer case `*__distinct_count__filtered_grain_row_population`,
scripted reduction in the session notes):

```trilogy
-- model: groups (complete gid source), events (binds gid), and
-- returns (id, gid: ~group_id, amount)  <- PARTIAL binder, NOT in the query
select count(grain(group_id) ? event_amount > 4) as x;
```

- Without `returns` in the model: single events scan, correct.
- With `returns` declared (never referenced): the planner builds a CTE
  selecting `gid` FROM RETURNS, LEFT JOINs it onto events on gid, and
  coalesces the (identical) values into the hash input. Returns' row counts
  multiply matching events: edge seed counts 8 instead of 4. The answer of a
  query changes with the contents of a table it never mentions.

## BUG 2 — same trigger, different corruption for a NAMED derived concept

```trilogy
auto gid_label <- group_id + 0;
select count(gid_label ? event_amount > 4) as x;
```

With the partial binder present, the plan (a) GROUPs events to distinct
`(amount, gid)` pairs — dropping event multiplicity — then (b) self-joins
that CTE to its own projection on gid, fanning amounts within a group: edge
seed 7, correct 4 (dense passes by numeric coincidence — 4 both ways; do not
trust a green dense leg). The anonymous inline spelling
`count(group_id + 0 ? ...)` plans a clean single scan, so the divergence is
in the named-derivation route. Pinned as fuzzer case
`*__distinct_count__filtered_named_derived_key_count`.

**Shared trigger family:** inline-filtered `count` over a DERIVED concept
keyed on a shared key, in a model where a partial (`~`) binder for that key
exists. Plain `count(group_id ? cond)` and unfiltered `count(grain(...))`
are both correct; `count_distinct` variants survive because value-dedup masks
fan-out. Both bugs reproduce at HEAD (`7cfe045a0`) in a clean baseline
worktree — NOT caused by the 2026-08-11 count_distinct sugar (which is
hydration-only; the same plans fire for hand-written `grain()`).

## Semantics note pinned while writing oracles

`count(grain(x))` counts the POPULATION at x's grain (NULL x included) —
"counts null-bearing rows uniformly" per `fgrain`. Distinct VALUES (NULL
included) is `count_distinct(grain(x))`. The `fgrain` docstring's claim that
injectivity makes `count(grain(...))` equal `count_distinct(grain(...))`
holds only when every argument is a key (args at their own grain); for
property args they differ. Both semantics now have explicit fuzzer cases.

## Pre-existing red the full sweep surfaced (NOT from 2026-08-11 work)

The full corpus had not run since 2026-07-08; it was green (166 cases) on
2026-07-05. A HEAD-baseline worktree run reproduces all 20 old-family
failures identically, splitting into:

- **11 regressions introduced between 2026-07-05 (green) and HEAD**:
  `union__nullable_partition` (×2 seeds), `union__three_arm_partition` (×2),
  all three `rowset_boundary` readback cases (×2 —
  DisconnectedConceptsException at compile), and
  `dense__coalescing_presence__union_plain_composite` (mismatch).
- **9 `grouping_placement` failures** (rollup/cube extra-leaf-dim mismatches
  off-by-one-row + two BinderExceptions): the family was added AFTER the last
  green run and its docstring describes exactly these failure modes — likely
  never-passing targets (compare the tpc_h q3/q22 assertion-target
  precedent), not regressions. No green run exists to prove either way.

Repros for everything: `local_scripts/fuzzer/repros/<case_id>/` (repro.preql,
oracle.sql, generated.sql, result.json). Full-run report:
`local_scripts/fuzzer/runs/20260811T124947Z_seed0/`.

## Suggested next steps

1. Fix the partial-binder source election (bugs 1–2). Start where the
   filtered-aggregate route gathers sources for a derived concept's keys —
   whatever elects `returns` must skip partial binders the query never
   demanded (and must not join ANY extra source at row grain under a plain
   `count`). Gate with the fuzzer cases; both must go green with the
   `count(*)`-filter oracles.
2. Bisect the 11 July regressions (rowset_boundary compile breaks are the
   loudest — a DisconnectedConceptsException on a previously-planning query).
3. Decide `grouping_placement`'s status: targets or regressions; if targets,
   mark them via `accepted_compile_errors`/expected-fail metadata so real
   regressions aren't buried in known red.
