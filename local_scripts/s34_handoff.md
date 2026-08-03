# Handoff — v4 network discovery: the design, and what still needs to be earned

Session history and per-fix mechanism live in `docs/v4_network_discovery_design.md`
(§0.3–§0.7). This file is the current design stated from first principles, the
honest list of patched complexity that should be consolidated, and operations.

## Status (post-s38: obligations + ladder purge + v4 default + determinism)

**v4 is the default planner** (`use_v4_discovery: bool = True`;
`TRILOGY_V4_DISCOVERY=0` forces v3 for comparison sweeps — v3 stays in-tree,
isolated). Plain `pytest` full suite: **6161 passed / 0 failed** (30 registry
xfails tracked in `tests/v4_known_failing.py`, including the parked ambiguity
test). Sweep 109/109 (~37s). Unit guardrails 25/25.

**Goldens are REFRESHED** (2026-07-26): `v4_sql_snapshot.py check` = 109
identical / 0 changed. The baseline is now the network planner with
deterministic CTE ordering. The cutover deltas (32 reviewed shape drifts) and
the stable-sort reorderings (20, all verified pure-reorder by line-multiset
comparison) are absorbed.

s38 landed, in order, each gate-verified before the next:
1. **Obligations replace the four repairs** (§0.8) — byte-identical A/B cutover.
2. **Ladder purge** (§0.9) — `SourcePolicy`/`SourceAttempt` deleted; the
   partiality MODE is gone end-to-end: no `accept_partial` anywhere in the v4
   chain, `SourceRequest.require_full` is the one constraint (completion
   sub-call only). Registry burned 43→30 by isolation re-verification.
3. **v4 default flip** — verified by the first-ever full plain-pytest sweep
   both before and after the collapse (identical counts).
4. **Ordering determinism** — `reorder_ctes` stable Kahn's; 109/109 corpus
   renders byte-identical across PYTHONHASHSEED values (was: q35 flip-flop).
   Regression harness: seed-pair render diff (see §0.9).

**Four-leg eval run IN FLIGHT**: `results/20260726-191755_{sql_bare,sql_schema,
ingest,enriched}`, concurrency 2, launched detached (PID 172760; stdout in the
session scratchpad `eval_run.log`). Funnel/charts render on completion.

## The design, from first principles

Everything lives in `v4_helper/network_search.py` (pure — selects sources, builds no
nodes) consumed by `source_planning.py::plan_source`.

### The model

Sourcing a ROOT request is **weighted set cover with join connectivity**:

- **Terminals** — the addresses the request needs bound: outputs, condition row
  arguments, grain keys, authored-join keys. Single-row concepts and internal
  addresses are excluded (they never drive connectivity). A BASIC expression over
  terminals already being sourced is not itself a terminal (it computes inline).
- **Candidates** — everything that can bind addresses: physical datasources, union
  families (partition arms read as one source), and derived connectors (a merge key
  with non-BASIC lineage has no scan; the candidate represents the subplan
  `_derived_connector_nodes` will materialize, binding the key plus its origin's
  grain keys).
- **Equivalence classes** — all addresses are collapsed to class representatives
  first (pseudonyms, merge twins, a concept's own canonical spelling, the graph's
  pseudonym edges). One value, one name; every rule below operates in class terms.
- **Bindings are labeled, never pruned** — each candidate carries per-class
  `full/partial`, `stored/computed`, `injected`, plus one condition-fit label
  (IMPLIED_EXACT / APPLIES / UNAFFECTED / DEFERRED / SENSITIVE). This is what
  replaced the ladder: partiality and condition fit are facts the one search reasons
  about, not global modes chosen before searching.
- **Grains** — each candidate's row identity, in class terms. Grain is what turns
  "these two share a key" into "this join is a lookup vs a blend".

### What makes a cover CORRECT (not just cheap)

Binding every terminal somewhere is necessary but nowhere near sufficient. The
invariants, each of which was originally a wrong-rows bug class:

1. **Join-connected**, on shared classes.
2. **Row identity in the spanning structure.** `joins_functionally(a,b)`: the shared
   keys cover one side's grain → the join is a lookup (restricts, never multiplies).
   A join covering neither grain is a blend — priced (`_blend_joins` counts blend
   edges in a minimum-blend spanning tree), not forbidden, because two facts related
   only through conformed dimensions are legitimate. Spanning-tree minimisation is
   what makes the count un-launderable: an extra source can only lower it by
   supplying a functional path, i.e. by actually fixing the join.
3. **Declared relations pair on both sides** (`JoinRequirement` /
   `_unpaired_join_keys`). A merged key sourced once satisfies coverage while one
   side of the authored equality has no way to produce it.
4. **A coalescing (`union join`) axis is bound only by the full member set**
   (`axis_families` / `axis_complete`). The unified axis is the union of the arms'
   domains; one arm's binding is downgraded to PARTIAL, and fullness is a property
   of the cover.
5. **Directed labelability** (`_functional_into` / `_broken_diamonds`). A source
   contributing terminals must be able to label its OWN rows with each requested
   terminal through an in-cover single-functional-hop lookup, whenever some
   candidate could supply one. Directional, because an undirected functional
   component can relate two facts through a shared dimension while pinning neither
   to the other.
6. **Minimality is validity, not cost** (`_reduce`). A source the rest of the cover
   makes redundant contributes only its join, and an extra join changes rows. But
   "redundant" is judged on the binding profile AND the join-structure triple
   (unpaired, blends, diamonds) — a source can bind nothing new and still be the
   only thing materializing a key where the other side can pair on it.

### The pipeline

```
build_source_network          # Stage A: label everything once, prune nothing
search_sources:
  _enumerate_covers           # Stage B: obligation-driven DFS — pop a state,
                              #   discharge the scarcest pending obligation by
                              #   branching on ALL its satisfiers, emit when
                              #   nothing is pending; soft branches upgrade
                              #   partially-covered terminals to full binders.
                              #   COVER_LIMIT / STATE_LIMIT (truncation reported)
  _connect                    # add ≤2 bridging sources if still disconnected
  _reduce                     # minimality: profile + connectivity + "no
                              #   obligation re-opens" + blends not worse
  _solution_for               # cost: 8 axes (unpaired, partial, completions,
                              #        blends, fanouts, sources, connectors, derived)
  _non_dominated              # Stage C: Pareto, then lexicographic winner;
                              #          surviving alternatives = honest ambiguity
plan_source                   # Stage D: single-scan → _direct_source (unless a
                              # binding is injected); else bridge emitter
```

**The obligation model** (§0.8) — the load-bearing asymmetry it resolves:
coverage asks ∃ (some binder per address), the correctness invariants ask ∀
(per source / per relation / per arm). A coverage-only enumeration stops
branching the moment an address is bound, so covers that bind an address in a
second place for structural reasons were unreachable — historically patched by
four post-hoc greedy repairs. `_pending_obligations` states both kinds in one
vocabulary — `cover`, `axis`, `paired`, `labelable`, `colocated` — and the DFS
discharges them uniformly. Soundness rests on two properties: obligations are
**monotone** (adding a source never re-opens one), and an obligation is
**minted only when a satisfier exists** (a requirement nothing could satisfy
is the request's own shape — that clause, not a special case, is what keeps a
fact-to-fact blend over conformed dimensions legal). Every alternative
discharge is an emitted cover, so carrier choice is judged by dominance, not
by repair order; second-order effects (a discharging source's own obligations)
are handled by the same loop, which is the fixpoint the repair chain lacked.

## Patched-complexity status — CONSOLIDATED s38 (§0.8)

The four (requirement shape → greedy repair → reduce guard) triples are gone:
`_pair_join_keys`, `_colocate_blended_grains`, `_complete_axis_families`,
`_repair_diamonds`, `_broken_diamonds`, and `_join_structure` are deleted, and
`_reduce` no longer needs per-invariant knowledge — its guard is "the drop
re-opens no obligation" (plus profile, connectivity, and blend-count
non-worsening; blends are the one pure-cost invariant). A future invariant
added as an obligation kind is automatically reachable by the search AND
protected under reduction. Query-case narratives came out of the docstrings;
the code states invariants, the design doc keeps the history.

What remains of the original critique, still true and now the live worklist:

- **`_connect` is the last unprincipled entry point** — the only place a
  source joins a cover with no invariant justifying it. Its replacement
  (disconnected = cross product, preferring functional paths) should be
  priced through the same functional/lookup machinery the obligations use.
- **Exemption carve-outs are boundary contracts written as special cases.**
  The arm-pinned exemption, "condition columns never pin," rowset members
  getting no family entry, presence probes never decomposable — each encodes
  a contract with a downstream emitter inside a Stage A constructor. Correct
  and tested, but they are where the next mole will surface; any new emitter
  contract should be surfaced as an explicit network fact, not a carve-out.
- **Blends are still a cost with a repair-shaped cousin** (`colocated`): the
  obligation fires when a fixer exists, the cost axis prices what no fixer
  can close. That split is principled but easy to forget — keep them adjacent.

## s38 final additions (post-commit, 2026-07-26 evening)

**`_connect` fabrication REPLACED for real** (after one reverted false start —
§0.9 has both attempts). Three obligations-native pieces: `_grain_classes`
infers a KEY-purpose grain for grainless datasources (root cause: the two
functionality predicates disagreed on empty grains); `labelable` walks
multi-hop functional chains via memoized `_functional_reach` (a composition
of lookups is a lookup); and connectivity is a last-resort `connected`
obligation — mergers-first satisfiers, fires only when nothing else is
pending, every bridge alternative priced by dominance instead of `_connect`'s
greedy first-found. `_connect` is a pure check; `MAX_BRIDGE_ADDITIONS` is
gone. Verified: gcat + multi_join green, sweep 109/109, seeds identical, q97
oracle exact, snapshot delta = ONE query (q84 — new plan, rows pass; absorb
into goldens after the final halves confirm).

**Eval run 20260726-191755 (four legs, 99 questions):** funnel
`db-only 78 → db+schema 91 → ingest 81 → enriched 85`. Enriched trails raw
SQL+schema by −6 (historical theme was ~−12); q05/q80 newly unlocked.
**error_scan over all 14 enriched misses: ZERO framework errors** — every
failure is "result set differs" (plus one exhausted), i.e. agent/guidance
work, not engine work. The 14 to mine per-query next:
q14 q27 q36 q45 q53 q58 q67 q72 q75 q81 q82 q90 q91 q97.

## ⚠ FOR THE AMBIGUITY WORKSTREAM (settled-tree regressions, attributed by A/B)

Six failures exist on the settled tree (post `simplified_network` +
`search_fixes`) that are NOT from the obligations work — verified by plugin
A/B: each of the three new obligation pieces was individually neutralized
(grain inference → declared-only, `_functional_reach` → empty, `_connect` →
old fabricating version) and all failures persist under every variant:

- `tests/engine/test_enum_unions.py::test_enum_union_arm_spanning_multiple_sources_{row_grain,aggregated,in_tvf}`
  (xpassed in isolation this morning pre-commits; fail solo now)
- `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_where_clause_inputs`
  (fails in-suite; passed solo — order-sensitivity, retrace under the new code)
- `tests/modeling/tpc_h/test_tpch_queries.py::test_{five,eight}` — the
  workstream's own tracked q5/q8 order-dep finding; now a hard
  `Invalid input concepts to node! ['part.supplier.nation.id'] missing
  non-hidden parent` solo.

Battery otherwise 799 passed on a clean serial run. A/B plugins for
re-verification live in the session scratchpad (`no_inference_plugin`,
`no_reach_plugin`, `old_connect_plugin`).

## The registry is the worklist now (30 gaps + 1 parked)

`tests/v4_known_failing.py`, by family — this is the distance to deleting
`use_v4_discovery` and deciding v3's fate:

- **18 × MASKED_LEAK** (exposed 2026-07-02, reasons stale — re-derive each
  failure mode with `pytest <id> --runxfail` in ISOLATION, not concurrent
  with other suites). Heterogeneous; expect several to share root causes
  with the rowset-readback family below.
- **5 × UNSWEPT_GAP** (first non-gate v4 sweep, s38): partition_persistence,
  hackernews adhoc03, cross_rowset_membership[plain_key],
  window_expression_join, validated_tvf_output. Each A/B'd — pre-existing,
  not obligation-engine regressions.
- **4 × ROWSET_XDS_RESIDUAL**: the FULL-body readback + b-side property
  carry family (registry header has the precise open description). One
  coherent fix, probably the highest-leverage single item.
- **2 × VERBOSITY** (bound_conversion presto, usa_names anonymous filter)
  and **1 × STRUCTURE** (nested_greatest watermark guard).
- **1 × PARKED_AMBIGUITY — ANOTHER AGENT'S LANE** (v3+v4-wide fix in
  flight; see MEMORY: ambiguity model-level standard prototyped).

## Remaining gaps (foundational, in priority order)

1. **`_connect` fabricates bridges — removal ATTEMPTED, REVERTED (§0.9).**
   Corpus census: 4/4786 widenings, all q97-family, all dominated; q97 row
   oracle built (≡ official reference ≡ v3, exact). But a check-only version
   broke gcat `test_array_agg` + `test_multi_join_assignments::test_select` —
   fabrication is corpus-dead, SUITE-LIVE. The replacement must be designed
   against those two shapes, with a FULL-SUITE census (corpus-only was the
   one-passing-area trap).
2. **Ambiguity's definition and home.** Today: non-dominated survivors with
   differing connector sets. The parked test resolves what the ladder called
   ambiguous; steer is that model-level ambiguity may belong at parse time.
   Decide, then unpark or delete the test.
3. **Nullability is not modeled** — zero references in the module. Join-type
   decisions downstream compensate (s36's honest-nullability fixes); the search
   itself is blind to it.
4. **Enumeration cost — deferred by explicit steer** (principled implementation
   first, optimize after). The obligation search is currently FASTER than
   enumerate+repair (37s vs 45s sweep), but branching on all satisfiers can
   explode on adversarial schemas; `COVER_LIMIT`/`STATE_LIMIT` truncation is
   reported but nothing consumes the report. Branch-and-bound over partial
   covers (prune when a prefix's monotone cost axes are already dominated) is
   the natural next step and only became possible with obligations in-search.
5. **The registry (30 entries) → eventual flag+v3 removal.** The ladder purge
   is DONE (SourcePolicy deleted, partiality mode gone, v4 default, goldens
   refreshed). What keeps `use_v4_discovery` alive is the v3 escape hatch and
   the 30 tracked v4 gaps in `tests/v4_known_failing.py`; burn those down,
   then decide v3's fate.

## Commands

```bash
# unit guardrail — run FIRST on any rule change, it is seconds
.venv/Scripts/python.exe -m pytest tests/core/processing/test_v4_network_search.py -q -p no:randomly

# generation sweep, 109 queries
.venv/Scripts/python.exe local_scripts/s33_network_burndown.py

# the gates — v4 is the DEFAULT now; no env var needed.
# TRILOGY_V4_DISCOVERY=0 forces the legacy v3 planner for comparison.
.venv/Scripts/python.exe -m pytest \
  tests/modeling/tpc_ds_duckdb tests/engine tests/join_matrix tests/modeling/gcat \
  tests/core/processing tests/test_shared_dimension_bridge.py tests/test_scoped_join.py \
  tests/modeling/join_resolution -q -p no:randomly

# shape drift vs goldens (REFRESHED 2026-07-26 — pin the network planner with
# deterministic ordering; any drift is a change to classify)
.venv/Scripts/python.exe local_scripts/v4_sql_snapshot.py check

# ordering-determinism regression check: render corpus under two hash seeds, diff
# (scratchpad/seed_render.py pattern — see design doc §0.9)
```

Do not run the battery and the sweep concurrently (zquery-log write collision →
spurious PermissionError).

## Traps (all still true)

- **Never call a fix done on ONE passing test.** Confirm against whole files, A/B
  the full area, count both ways.
- **A/B without touching git** (parallel agents share the tree): pytest plugin on
  PYTHONPATH monkeypatching rules to no-ops, `-p ab_off_plugin`. For HEAD
  comparison, `git worktree add <scratchpad>/head_tree HEAD` is safe (remove after).
- v4 is the DEFAULT everywhere now (flag flipped s38). `TRILOGY_V4_DISCOVERY=0`
  forces v3 in pytest runs; standalone scripts get v4 unless they set
  `CONFIG.use_v4_discovery = False` themselves.
- Callers import `plan_source` by name — monkeypatch the CALLER module's reference
  (`v4_node_generators.root.plan_source`), not just `source_planning.plan_source`.
- `ruff check . --fix` is DESTRUCTIVE with this venv's ruff 0.16. Use
  `--select E,F,I` on specific paths.
- Writing files from Python on Windows: always `encoding="utf-8"`.
- `tests/modeling/tpc_ds_duckdb/zquery*.log` and the perf PNGs churn on every
  battery run.
- If a test flips solo-vs-file, it is a NEW bug to trace, not "order-dependence"
  to shrug at — the known order-dependence root cause was fixed at the source
  (construction-order nullability, s36).
