# Handoff: rule B (graph-proven subset narrowing through the ∦ veto) + carve-out cleanup

> **STATUS 2026-07-03: BOTH TASKS LANDED** — see docs/domain_graph_design.md
> "Landed (rule B + carve-out deletion, 2026-07-03)" for the full record.
> Both gcat acceptance tests flipped to passing; q64's 3 readback FULLs →
> LEFT (battery 106/106; 37.2s vs 39.0s at clean HEAD single-run). The 4th
> FULL (customer↔customer_address enrichment) deliberately NOT narrowed:
> both bindings are complete-in-schema with no `~` and no ⊑ evidence —
> narrowing it requires trusting undeclared fact-FK scan completeness,
> which the design rejects. Carve-out params + ignore sets deleted;
> `_own_coverage_partial` consults `graph.subset_sources()` directly.
> Stretch item (q04 null-safe reduction) not attempted.

Written 2026-07-03, end of the session that landed step-5-mechanical, the
endpoint-identity first tranche, and cross-CTE null-rejection propagation
(see docs/domain_graph_design.md "Landed" sections for all of it; owner is
checkpointing that work before this session starts).

**Owner-recalibrated goal**: TPC-DS SQL byte-stability is NOT required —
correctness + performance is the bar. Log diffs that narrow joins are wins.
Every TPC-DS test asserts result rows, so the battery is the correctness
oracle; the join-type census (below) is the perf metric.

## Task 1: rule B — narrow ∦-vetoed FULLs when subset direction is PROVABLE

### The gap

`UpgradeOuterFromKeySetEquivalence.optimize` (value_set_join_upgrade.py)
blanket-skips any join pair touching `self.full_join_keys` — the canonical
endpoints of ∦ declarations (query-scoped `full join`/`union join`). The veto
exists because ∦ says "neither domain contains the other," so the
completeness tests can't be trusted on the collapsed canonical address.

But an authored ∦ can be conservatively WRONG in one direction, and the
graph can prove it: in q64 (tests/modeling/tpc_ds_duckdb/query64.preql, the
`full join agg_99.store_sk_99 = agg_00.store_sk_00 = ss.store.id` family),
the agg-side keys are rowset outputs derived from `ss.store.id` /
`ss.item.id` / address ids — structural ⊑ edges by construction — and the
dimension datasources bind those base concepts COMPLETELY (BindingEdge
complete). So agg-key domain ⊑ dim domain, every agg row finds a dim
partner, and FULL → LEFT (preserving the dim side) is row-identical. The
downstream join that consumes the result is now INNER (cross-CTE
null-rejection landed 2026-07-03), which then rejects the dim-only padded
rows — so the chain ends INNER end-to-end.

This is proof-based ROW-IDENTITY narrowing, not declaration override: the ∦
stays in the graph; we only narrow when the rendered join provably keeps the
same surviving rows.

### Concrete implementation direction

- `_declared_subset_of` (value_set_join_upgrade.py) currently traverses
  DECLARED ⊑ edges only. Rule B ≈ a sibling check that also accepts
  STRUCTURAL ⊑ paths (rowset/filter lineage mints them —
  `structural_domain_edge` in domain_graph.py; conditions on the edges
  identify WHICH subset and never weaken containment, per
  `_subset_reachable`).
- The superset side must still prove it carries the key's full domain HERE —
  `_complete_values` with scan evidence (the dim is an authoritative scan
  binding the concept completely) and a filter-free chain, exactly like the
  existing `_pair_side_fully_matches` path.
- Release the `full_join_keys` veto in `optimize()` for pairs where this
  proof holds (the veto check happens before `_narrow_directionally` is ever
  consulted — restructure so proof-carrying pairs fall through to it).
- Nullability guards stay as-is (`pair.is_nullable or not _key_nullable` in
  `_narrow_directionally`).

### What must NOT narrow

- Genuine channel unions: q97 (store ∪ catalog customers), q77/q05/q09
  (channel FULLs). Their arms are NOT subset-provable (store_sales customers
  vs catalog_sales customers — no ⊑ edge either way) so the proof simply
  fails; verify the battery keeps them FULL.
- Anything where the superset side is filtered (a WHERE on another column
  drops domain values asymmetrically — `_accumulate_filter` must be None,
  same as today's directional path).

### Acceptance

- q64's 4 FULLs (zquery64.log: `customer FULL JOIN customer_address` in the
  enrichment CTE + 3 readback FULLs in `sedate`) narrow; battery 106/106
  rows correct; q64 wall-clock improves (33s→53s regression after the
  always-preserving flip is the standing complaint — time it before/after:
  `pytest tests/modeling/tpc_ds_duckdb/test_queries.py -k sixty_four`).
- **The two pre-existing gcat failures are the natural acceptance tests**:
  `tests/modeling/gcat/test_gcat.py::test_join_inclusion` and
  `::test_joint_join_concept_injection` fail at clean HEAD expecting
  LEFT OUTER where a vehicle→launch enrichment renders FULL — exactly the
  rule-B shape. Good odds rule B flips them to passing; if so the full suite
  goes fully green (the only other 2 failures are OpenAI-key tests).
- Census metric: `grep -c "FULL JOIN\|LEFT OUTER JOIN"
  tests/modeling/tpc_ds_duckdb/zquery*.log` — 91 outer joins at handoff.
- Guard sweeps: `tests/join_matrix/`, `tests/test_scoped_join.py`,
  `tests/optimization/test_value_set_join_upgrade.py` (the
  union-join-never-narrows cells are the veto's contract — they must keep
  passing; rule B narrows only PROVEN pairs).

## Task 2 (cleanup, natural rider): delete the partial-stamp carve-outs

`partial_closure` / `ignore_partial_addrs` parameters in
`_complete_distinct` / `_complete_values` / `_equal_intersection_complete` /
`_complete_via_preserved_base` (value_set_join_upgrade.py) are phase-2
workarounds: relation-induced PARTIAL stamps (the declarations' subset
sides) smear onto addresses and must be selectively ignored when proving a
side's coverage of its OWN concept.

The replacement substrate is landed: per-column origin stamps
(`BuildColumnAssignment.origin_address`, read by `_side_origins`) and the
graph's declared/binding edges. Per docs/domain_graph_design.md, the
carve-outs are "deletable once completeness consults origin nodes + the
graph directly": a stamp whose column's `origin_address` is a
declared-subset endpoint speaks to the RELATION, not the side's own
coverage — that's an origin-stamp query, not an address-set carve-out.
Doing this while rule B is in the same files avoids two passes over the
same evidence stack. Success = the parameters and `subset_join_map`-derived
ignore sets are gone, suite + battery + matrix green.

## Environment / process notes

- venv: `.venv/Scripts/python.exe`. Checks after repo-wide changes:
  `ruff check . --fix`, `mypy trilogy`, `black .`.
- Full suite: `pytest tests/ -q -m "not adventureworks_execution"`.
  Pre-existing failures at handoff (NOT yours): 2 OpenAI-key tests
  (tests/ai/test_providers_basic.py, tests/modeling/faa/test_llm.py) + the
  2 gcat tests above (verified failing at clean HEAD via worktree).
- Battery: `pytest tests/modeling/tpc_ds_duckdb/test_queries.py -q` (~3
  min); it regenerates zquery*.log — diffs are reviewable evidence now, not
  regressions, but every row-assertion must pass.
- ⛔ Never `git stash`/`checkout`/`reset` in the shared working tree
  (parallel agents); A/B against clean HEAD via a temporary `git worktree`.
- Stretch, if rule B lands early: q04 carries 6 `is not distinct from`
  join equalities — the null-safe-reduction counterpart
  (`SimplifyNullSafeJoins` is the existing rule to extend).
