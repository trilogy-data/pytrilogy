# LANDED — collapse the v4 cover-enumeration blowup (arm-vs-union branching)

Written s51 (2026-07-30), landed s53 (2026-07-30). Predecessor:
`handoff_v4_search_cost.md`.

## What landed

`_prune_subsumed_arms` in `trilogy/core/processing/v4_helper/network_search.py`:
when an obligation's satisfier list contains both a partition **arm** and the
**union** candidate that subsumes it, the arm is dropped from that list. The map
is built once per network by `_subsumed_arms` and carried on `SourceNetwork`
as `subsumed_arms` (a real field, in `signature()`, not a memo cell); the filter
is applied centrally at the end of `_compute_pending_obligations`, so every
obligation kind is covered by one rule.

Why the enumeration could not find this itself: it branches on satisfiers before
any cost is computed, so on q05 every subset of 12 arms became a distinct cover
that `_reduce` collapsed back to the same 7-source answer — 4,096 covers and a
`COVER_LIMIT` truncation.

**One deviation from the plan below: `IMPLIED_EXACT` arms are excluded from the
map.** The proposed rule was measured plan-neutral on TPC-DS but is *not* safe in
general — with `where sales_channel = 'WEB'` the pinned `ds~web_sales` arm and
its union both satisfy the same `cover` obligation, and pruning the arm made the
search plan the union plus a filter, re-reading the partitions the predicate
removes. The corpus has no query of that shape, so the A/B could not see it;
`TestArmUnionBranching::test_condition_pinned_arm_is_not_subsumed` now guards it.

## Measured

`local_scripts/v4_arm_prune_ab.py` (rewritten to A/B the LANDED rule — it now
disables `_subsumed_arms` for the baseline leg, generating the corpus twice in
one process):

```
covers   max  4096 ->   534   total  41,303 ->   3,450
plans    109 identical, 0 changed
```

`COVER_LIMIT` is 4,096, so the truncation is gone. The all-kinds spike measured
in s51 got further (max 297) but changed q23; with the `IMPLIED_EXACT` carve-out
the rule is strictly plan-neutral on the corpus.

Gates run: `pytest tests -m "not adventureworks_execution"`, mypy, black,
`ruff check --select E,F,I`.

## Evidence (gathered s51 — do not re-derive)

**Where the arms come from** — `local_scripts/v4_arm_branch_probe.py query05`:

```
12 arm candidates subsumed by union candidates
obligations BRANCHED ON whose satisfiers include an arm:
  labelable    21693   arm-only:      0   subsuming union also a satisfier:  18028
  colocated      116   arm-only:      0   subsuming union also a satisfier:    116
  cover           65   arm-only:      0   subsuming union also a satisfier:     65
```

It is overwhelmingly **`labelable`**, not `cover` (the guess in
`handoff_v4_search_cost.md` was wrong about which obligation mints them). And
**`arm-only` is 0 everywhere**: an arm is never the sole way to discharge an
obligation that offers it, so dropping arms can never strand one.

**Caveat on the goldens — RESOLVED, do not propagate it.** s51 recorded that
`v4_sql_golden/q11.sql` was stale (a concurrent workstream pruned a
`catalog_sales` scan the golden still contained). The goldens were re-snapshotted
in `ced92f96b`, and `v4_sql_snapshot.py check` reports **109 identical, 0
changed** against the s53 tree. The general rule still stands — gate a planner
change against the CURRENT TREE, and check the goldens' last commit before
trusting them — but the specific q11 exception is gone.

## Scripts

| script | what it answers |
|---|---|
| `v4_arm_prune_ab.py` | the landed rule's effect vs the tree — the regression gate |
| `v4_arm_branch_probe.py <queryNN>` | which obligation kinds offer arms, and whether an arm is ever the only satisfier |
| `v4_cover_yield.py <queryNN ...>` | covers emitted vs distinct reduced solutions, and the cover-size histogram |
| `v4_q05_covers.py <queryNN>` | dumps the base cover, the optional sources, and the pending obligations at the base |

`v4_arm_prune_spike.py` and `arm_prune_plugin.py` are deleted: they monkeypatched
a prune that is now in the tree, so running them would double-prune.

## After this

Next in `handoff_rust_candidates.md`: `network_search.py` is 45% of generation
tottime; the recommended follow-up is interning addresses to indices and making
bindings/grains/cover-sets bitmasks **in Python** before considering Rust. Doing
this arm-prune first was deliberate — it shrank the input to that work.

Measure with call counts, not seconds; this box varies 45–75 s on identical
runs. `local_scripts/v4_q05_profile.py <queryNN>` gives a cProfile with a
state-count control row.
