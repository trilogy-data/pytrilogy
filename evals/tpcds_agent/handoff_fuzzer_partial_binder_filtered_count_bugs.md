# Handoff: fuzzer extension found 2 partial-binder wrong-results bugs (+ 11 older regressions)

**Status: FIXED 2026-08-11 (follow-up session). Full fuzzer corpus 218/218.
Committed repros: `tests/engine/test_duckdb_fuzzer_regressions.py`.**

- **Bug 1** (spurious partial-binder election): a derived concept over a bare
  KEY inherited the key's fk-derived keys (`Environment.fk_derived_keys`,
  last-declared fact wins — here `returns`), re-keying the derivation onto an
  undemanded fact. Fixed in `trilogy/parsing/common.py`: a bare KEY arg
  contributes ITSELF in `concept_list_to_keys` and the transitive-keys loops
  of `function_to_concept`/`comparison_to_concept`; fk-derived keys stay
  datasource-level FD facts.
- **Bug 2** (named-derived filtered count): semantics settled as the handoff
  suggested — a DERIVED row expression's filtered count ranges over the
  condition's row population (matching the anonymous-inline plan); a BARE key
  keeps distinct-domain semantics. Fixed in `filter_item_to_concept`: the
  condition-grain widening (previously CONSTANT-content only) also applies to
  BASIC-derivation property content.

Original characterization below, kept for provenance.

---

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

## Pre-existing red the full sweep also surfaced

The full run additionally shows 20 old-family failures. Those were BISECTED
(2026-08-11, follow-up session) to the V4-default flip `a6161b981` (#602) —
all of them are V3→V4 parity gaps, none are from the fuzzer-extension work.
Full attribution table, per-symptom breakdown, and probe-methodology traps:
**`handoff_fuzzer_v4_default_regressions.md`**.

The 2 bugs in THIS handoff have different era-provenance (probed at
`418901be6`, 08-04, pre-flip v3-default, minimal repros):

- **Bug 1 PREDATES the flip and is engine-independent**: the pre-flip engine
  also joins `returns` and returns 5 (differently wrong — correct is 3
  distinct values or 4 rows). The spurious partial-binder source election
  lives in machinery both engines shared, so do not hunt for it in
  flip-delta code.
- **Bug 2's corruption is v4's**: pre-flip returned a self-consistent 3
  (distinct values of the named derived concept, matching
  `count(group_id ? cond)`); v4 returns 7, which matches NO semantic
  (rows = 4, distinct = 3). Note the semantics wobble the fix must settle:
  v3 counted a filtered named-derived concept by distinct VALUES, current v4
  intends row-population (per the anonymous-inline plan) — pick one and pin
  it in the fuzzer oracle.

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
2. The 20 old-family regressions: see
   `handoff_fuzzer_v4_default_regressions.md` for the attack order.
