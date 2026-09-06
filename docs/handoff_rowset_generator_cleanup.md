# Handoff: `gen_rowset` ignores its parents, under-delivers its outputs, and has grown into a 437-line function

**Status**: Tiers 1 and 2 LANDED, Tier 3 answered and declined; see
`## Resolution`. Audit performed 2026-09-05 on `refactoring` (clean tree at
`70c820954`, which is `origin/main`).

**Scope**: `trilogy/core/processing/v4_node_generators/rowset.py`, plus the
compensating carve-outs it has grown in `v4_helper/strategy_builder.py` and two
dead marker node types.

**Why now**: PR #673 just did this exact refactor for `gen_union`. ROWSET is the
last generator still on the pre-#673 pattern, and it shows the same symptoms the
union commit message describes.

---

## TL;DR for the implementing agent

Three tiers of work, independently landable, listed in `## Work items`:

- **Tier 1** is mechanical and behavior-preserving (dead params, a real import
  cycle, duplicated locals, a nested function). Land it first and alone; it makes
  the rest readable.
- **Tier 2** is structural but still behavior-preserving (kill or adopt the dead
  `RowsetNode` marker type, fix the `gen_subselect` plumbing bug found en route).
- **Tier 3** is the real fix: plan a rowset body as a stacked source with parents,
  the way #673 did for unions. Large. Do not start it before Tier 1 lands.

Do **not** treat the `## Latent, checked, did NOT materialize` section as a bug
list. Those were measured at zero occurrences. They are listed so nobody
re-discovers them and chases them blind.

## Resolution (2026-09-05)

Tiers 1 and 2 landed on `refactoring`. Tier 3 was answered in writing (below)
and is **off**: the fresh environment is irreducible, so the design was made
honest instead, exactly as the Tier 3 section prescribes.

What landed:

- 1.1 The cycle is gone. `strategy_builder` imports `build_node` at call time
  in its two consumers; `v4_helper/__init__` is unchanged. The three-way probe
  passes from a cold process in every order. A test that spied on
  `strategy_builder.build_node` now spies on the package attribute, which is
  the seam the lazy import resolves.
- 1.2 `gen_rowset` lost `preexisting_conditions` and `g`; `resolve_rowset`
  lost `g`. `parents` stays for dispatch uniformity and a non-empty list is
  logged at the boundary's depth (loud, not fatal: the suite has 4 live
  occurrences). `dispatch.build_node`'s docstring no longer claims ROWSET
  needs `g`, and the ROWSET and SUBSELECT branches are split.
- 1.3 A real `depth` is threaded `_search_concepts` to `_build_from_graph` to
  `build_strategy_node` to `build_node` to `gen_rowset`, so a nested body's
  trace indents under its parent. `resolve_rowset` keeps its seam for the
  direct unit tests.
- 1.4 One `subset_sources` local.
- 1.5 The handle loop iterates `derived_concepts` (`_rowset_handles`) instead
  of the whole environment; demanded handles still win ties, and a demanded
  handle of another rowset is still skipped. One trap, found by the full
  suite and invisible to the corpus render and the probe sweep:
  `environment.concepts.get(addr)` can return the CANONICAL a scoped-merge
  collapse substituted for the handle (a different address, e.g.
  `fut.s.period` maps to `local._virt_func_add_...`), and the authored handle
  then survives only in `alias_origin_lookup`. The helper offers both
  candidates by own address, as the old scan did;
  `tests/test_scoped_derived_rowset_join_matrix.py` pins it (3 cells flipped
  rows under the naive lookup). Add that file to any boundary gate.
- 1.6 `_declared_subset_anchors` and `_anchors_all_rowset` are module level;
  the declared-subset edges are scanned once per boundary, not once per
  handle.
- 1.7 The five stale `gen_rowset_node` docstrings are fixed.
- 2.1 `RowsetNode` is ADOPTED: the boundary is built as one. Its docstring
  now says what it is (a typed 1:1 projection with no behavior of its own).
  `MultiSelectMergeNode` is DELETED with its `__all__` entry and the
  `isinstance` clause in `_subtree_pinned_addresses`, which `git log -S` shows
  was added in #643 after #632 had already removed the only constructor, so
  the clause was vacuous from birth. The sniff at `merge_node.py:645` is left
  as is: it asks whether a MERGE's own outputs are rowset handles, which is a
  different question from "is this node the boundary", so an `isinstance`
  swap there would not be equivalent.
- 2.2 Triaged, no rewrites. All 41 hits are concept-level or group-level
  (`GroupAttrs.derivation`, bucket derivation, output-concept derivation);
  none asks a built node whether it is a boundary. Nothing to retype.
- 2.3 `dispatch` now forwards `preexisting_conditions` to SUBSELECT, the same
  contract every other parent-consuming generator gets, and the docstring says
  what actually happens to a hosted atom (recorded, not rendered). Measured
  over the sweep: 10 SUBSELECT dispatches, 0 with `conditions`, 0 with
  `preexisting_conditions`, so this has no footprint today.

Gates, all on the current tree with a control copy of the untouched package:

| gate | result |
| --- | --- |
| `ruff check . --fix` (default rules), `mypy trilogy`, `black .` | clean |
| corpus render, tpc_ds + tpc_ds/aggregates + tpc_h + thelook + gcat | 173 / 173 byte-identical |
| `python -m local_scripts.fuzzer` | 238 / 238 |
| probe sweep (`## Measurement harness`) | every count unchanged, see below |
| full suite `-m "not adventureworks_execution"` | green except pre-existing: `local_scripts/fuzzer/test_fuzzer.py` case counts (218 and 327 asserted, 238 and 357 generated since #672) and `tests/cli/test_cloud_live.py` (API token lacks `workspaces:read`) |

Probe sweep, baseline vs after:

| stat | before | after |
| --- | --- | --- |
| `calls` | 787 | 787 |
| `DISCARDED_PARENTS` | 4 | 4 |
| `DROPPED_OTHER_ROWSET_HANDLE` | 10 | 10 |
| `dropped_output` | 88 | 88 |
| `OUTPUT_ABSENT_FROM_STAMPED_ENV` | 52 | 52 |
| built as `RowsetNode` | 0 | 766 (the other 12 are condition wrappers above one) |

### Tier 3 answer: is the fresh environment irreducible? Yes.

The stated reason at `rowset.py:60-66` ("a plain root reads back as
`derivation=rowset`") does not reproduce on its own. For
`rowset agg <- select cat, sum(val) -> total, id where val > 1;` the OUTER
build environment holds `local.cat` as ROOT and `local._agg_total` as
AGGREGATE beside the `agg.*` ROWSET handles. The docstring was rewritten to
name the reasons that do hold.

What does hold is that a rowset body is a **statement**, where a union arm
(#673) is an **expression**. Measured over the 789 body plans in the sweep:

| body carries | count |
| --- | --- |
| its own WHERE | 259 |
| its own HAVING | 22 |
| its own LIMIT | 2 |
| its own ORDER BY | 3 |
| a multiselect / union TVF body (own arms, align) | 81 |
| its OWN query-scoped joins | 11 |
| nesting inside another body | 187 |

A union arm has none of these: it is a lineage over the SAME environment's
rows, so a scope label in the concept graph is enough to keep its roots from
bucketing with a sibling arm. A rowset body needs three things a label cannot
give it:

1. **Its own environment materialization.** The 11 bodies with their own
   `scoped_joins` build through `nested_select.build_nested_select` against a
   different canonical collapse (the body's join key becomes ONE canonical
   column with the authored side as a pseudonym), with fresh `BuildCaches`
   when the join set differs from the outer's. The concept graph and group
   graph consume one `BuildEnvironment` and one `ReferenceGraph`; the body's
   graph also bridges datasources the outer graph never joins.
2. **Statement-level condition hosting.** WHERE, `then where` stages, HAVING,
   LIMIT with its ORDER BY. Hosting these per body scope in the group graph is
   `search_concepts` re-implemented inside the concept graph, not a smaller
   mechanism.
3. **Recursion.** 187 bodies nest inside another body; each level repeats 1
   and 2.

So the two answers in `## Background` really are one decision, and the
decision is the right one. The acceptance criteria stay unmet on purpose:

- `strategy_builder`'s two carve-outs are now commented as permanent.
- `DISCARDED_PARENTS` stays 4 and is logged; the 4 are a non-handle concept
  bucketed with the handles (`local.overall_avg_sale`, a TVF-union arm).
- `DROPPED_OTHER_ROWSET_HANDLE` stays 10; those are foreign handles a boundary
  group carries for a deferred WHERE arg, which the FINAL merge sources from
  their own boundary, which is why the suite is green.
- The post hoc condition injection at the end of `resolve_rowset` stays: a
  consumer-side predicate over the boundary's rows has no other host.

---

## Background: why the file looks like this

### Why it does not use `parents`

Because the group graph has nothing to hand it. A ROWSET concept is a deliberate
**leaf** in the concept graph (`v4_helper/concept_graph.py:1507`): the walk stops
there and never descends into the body, so the ROWSET bucket has no predecessors.

Measured over 792 generator calls, sweeping
`tests/complex|engine|discovery|core|join_matrix|nodes|generators`
(see `## Measurement harness`):

| `len(parents)` | calls |
| --- | --- |
| 0 | 788 |
| 1 | **4** |

So "always empty" is *nearly* true. The 4 exceptions are silently discarded. They
occur when the bucket also carries a non-handle concept with its own lineage
(observed: `local.overall_avg_sale`; a TVF-union arm node whose parent produced
`local.___tvf_arm_0_a` and friends).

### Why it resources within

Two reasons, one stated and one structural.

- **Stated** (`rowset.py:60-66`): the *outer* build environment classifies the
  inner select's concepts under rowset aliasing, so a plain root inside the body
  reads back as `derivation=rowset`. Reusing the outer env mis-buckets the inner
  plan, so the body needs a fresh `BuildEnvironment` and graph.
- **Structural**: that fresh scope is exactly why the body cannot live in the
  outer concept graph, which is why there are no parents. The two answers are one
  decision seen from both ends.

Any Tier 3 attempt has to answer the stated reason head on. #673 answered the
equivalent question for unions by giving each arm its own scope **label** in the
concept graph rather than its own environment.

---

## The contract it breaks

`v4_node_generators/__init__.py:12-13` states the contract:

> The topological walker hands each generator its actual parents and asks it to
> project the listed outputs, nothing else.

`gen_rowset` violates both halves. Measured over 787 calls (the five-directory
sweep in `## Measurement harness`; `parents discarded` is 4 in both sweeps):

| behavior | count | note |
| --- | --- | --- |
| parents discarded | 4 | silent |
| listed outputs not produced | 98 | 88 non-handle, **10 another rowset's handles** |
| outputs produced but never listed | many | 8 "obligation" passes |

The 10 cross-rowset drops are live in the suite, not synthetic:
`tests/engine/test_duckdb_rowset.py::test_order_by_measure_through_nested_rowset_join_groups`
(planned `combined`, dropped `store_agg.item_code`) and
`tests/engine/test_duckdb_subquery.py::test_tuple_membership_grainless_output`
(planned `_subquery_1_64`, dropped `pairs.cat` and `pairs.val`).

### The caller already compensates

Two named carve-outs exist in `strategy_builder.py` purely because of the above.
Any Tier 3 fix should be able to delete both, and that is the acceptance test for
whether the refactor actually worked:

- `strategy_builder.py:4277-4287` sorts a boundary group's outputs so the group's
  own handles come first, because "`resolve_rowset` plans the rowset of the first
  handle it sees" and a foreign handle would otherwise hijack the boundary.
- `strategy_builder.py:4421-4429` excludes ROWSET from `satisfiable_outputs`
  pruning, with the comment "`gen_rowset` ignores parents".

### The precedent

PR #673 (`70c820954`) did this for UNION. Its commit message:

> A concept-level `union(...)` was planned as a JOIN of its arms. With no
> predicate `gen_union` discarded that merged parent, re-entered the planner per
> arm and got the right answer, so the bad join stayed invisible. Once a predicate
> needed a host it surfaced.

`union.py` went 131 to 94 lines, now consumes `parents`, and recovered the
ordinary meaning of `preexisting_conditions`. `rowset.py` shows the same tell:
conditions have no natural host, so they are injected post hoc at the very end
(`rowset.py:465-484`).

See `docs/handoff_union_arm_filter_leak.md` for how that landed. Reuse its shape.

---

## Shape of the file today

`resolve_rowset`, measured by AST:

| metric | value |
| --- | --- |
| lines | 437 |
| cyclomatic complexity | ~108 |
| distinct locals assigned | 46 |
| `if` / `for` / comprehensions | 31 / 15 / 20 |
| sequential mutation phases | 13 |
| comment lines vs code lines | 133 / 292 (46%) |
| direct unit tests | 3, two of which cover trivial bail paths |

Growth history (`git show <rev>:...rowset.py | wc -l`):

| rev | date | lines | PR |
| --- | --- | --- | --- |
| `74c4b70d0` | 2026-06-02 | 29 | #571 |
| `92abbe52a` | 2026-06-07 | 34 | #578 |
| `a6161b981` | 2026-08-08 | **486** | #602 V4 as default |
| `a65b13c9c` | 2026-08-16 | 499 | #645 |
| `2be34102b` | 2026-09-03 | 491 | #659 simplification |
| `8d874b7e9` | 2026-09-03 | 485 | #663 simplification follow-up |
| `HEAD` | 2026-09-05 | 485 | |

Two dedicated simplification passes shaved 14 lines total. This file did not
respond to the generic simplification sweep and needs its own structural change.

The 13 phases, for orientation:

| lines | phase |
| --- | --- |
| 77-92 | pick `rowset_outputs`, plus the presence-probe recovery path |
| 97-109 | plan the inner select (`plan_nested_select`) |
| 124-147 | build `produced`, re-expose pseudonym-coalesced contents (mutates `inner_node`) |
| 148-177 | main handle projection loop (scans the whole environment) |
| 179-229 | expose plain-select grain keys, unfiltered bodies only |
| 231-245 | multiselect align-arm concepts, as hidden outputs |
| 247-267 | demanded multiselect/union own concepts |
| 269-295 | OBLIGATION: presence probes |
| 297-358 | OBLIGATION: derived relation members |
| 360-397 | mark declared-subset sources partial |
| 399-445 | nullability mapping and boundary grain, with a multiselect override |
| 446-459 | construct the `SelectNode` |
| 460-484 | inject outer conditions |

---

## Work items

### Tier 1: mechanical, behavior-preserving. Land first, on its own.

**1.1 Fix the import cycle. This is a real shipped defect.**

```
$ python -c "import trilogy.core.processing.v4_node_generators"
ImportError: cannot import name 'build_node' from partially initialized module
```

Chain: `v4_node_generators/__init__` to `dispatch` to `aggregate` to
`common.py:7` to `v4_helper/__init__.py:42` to `strategy_builder.py:68` and back
into the partially-initialized package.

It only works today because something imports `v4_helper` first. `import trilogy`
first does **not** save it (verified). Any test, script, or tool that imports
`v4_node_generators` first breaks.

Likely fix: make `strategy_builder.py:68`'s `build_node` import lazy (function
local), or move `condition_row_args` out of `v4_helper/__init__`'s eager surface.
Prefer whichever leaves `v4_helper/__init__` unchanged for other consumers.

Verify with the three-way probe:

```bash
python -c "import trilogy.core.processing.v4_node_generators as m; print(m.build_node)"
python -c "import trilogy.core.processing.v4_helper"
python -c "import trilogy"
```

All three must pass from a cold process, in any order.

**1.2 Delete dead parameters.**

- `gen_rowset` (`rowset.py:27`): `parents` and `preexisting_conditions` are both
  unused. `dispatch.py:98-106` does not even pass `preexisting_conditions`.
- `resolve_rowset` (`rowset.py:49`): `g` is unused.

Caveat: generator signatures are uniform on purpose because `dispatch` calls them
positionally. Removing `parents` from `gen_rowset` alone would break that
uniformity. Options, in preference order:

1. Keep `parents` in the signature but assert it is empty and log when it is not,
   which converts finding 2 from silent to loud. This is the smallest honest
   change and is a prerequisite for Tier 3 anyway.
2. Drop `preexisting_conditions` and `g` outright; neither is load-bearing.

Also fix `dispatch.build_node`'s docstring, which claims ROWSET needs `g` "to
recursively plan its inner select". It does not.

**1.3 Collapse `gen_rowset` into `resolve_rowset` or justify the split.**

`resolve_rowset` has exactly one caller, four lines above it, which hardcodes
`depth=0`. `depth` only drives log indentation (`depth_to_prefix`); there is no
semantic recursion guard on it. Consequence: **nested rowsets log at top-level
depth**, which matters for a planner debugged by reading traces.

Either thread a real depth through `build_node`, or delete the parameter and stop
pretending. Note `tests/core/processing/test_v4_node_generators.py` calls
`resolve_rowset` directly, so keep a seam for it.

**1.4 De-duplicate locals.**

`subset_source_members` (`rowset.py:312`) and `subset_sources` (`rowset.py:376`)
are both `environment.domain_graph.subset_sources()`, which is memoized on the
domain graph, so they are the *same set object* under two names in one 437-line
function. Pick one name and hoist it.

**1.5 Stop scanning the whole environment for handles.**

`rowset.py:150-152` builds `handle_pool` as every environment concept plus every
alias origin, then filters by `derived`. Measured: mean 27.5 concepts scanned to
find mean 2.3 handles, roughly 12x overscan, 20898 iterations across the sweep.
The absolute cost is small on these models; the cost is legibility. Iterating
`lineage.rowset.derived_concepts` and looking each address up is equivalent.

Preserve the current ordering semantics: `[*rowset_outputs, *handle_pool]` with a
`seen` guard means demanded handles win ties.

**1.6 Lift the nested function.**

`_subset_anchors_all_rowset` (`rowset.py:378`) is defined inside `resolve_rowset`,
which AGENTS.md asks us not to do ("avoid defining functions inside functions to
make testing easier"). It also re-scans `environment.domain_graph.edges` once per
candidate handle. Lift it to module level and take `environment` as a parameter.

**1.7 Fix stale references.**

Five test files' docstrings reference `gen_rowset_node`, a name that no longer
exists: `tests/test_cross_rowset_join_rowset_as_set.py`,
`tests/test_rowset_cross_datasource_outer_read.py`,
`tests/test_rowset_generation_matrix.py`,
`tests/test_scoped_join_cross_rowset_membership_existence.py`,
`tests/test_scoped_join_cross_rowset_multi_where.py`.

### Tier 2: structural, still behavior-preserving.

**2.1 Resolve the dead marker node types.**

`RowsetNode(SelectNode)` (`nodes/select_node_v2.py:325`) is exported in
`nodes/__init__.py` `__all__` and documents a real guarantee:

> A distinct type so the regroup pass never regroups it: the wrapper is a 1:1
> projection of an already-final body, and a forced GROUP BY would dedup rows or
> omit raw projections.

**It is never instantiated.** `rowset.py:446` builds a bare `SelectNode`, whose own
docstring says "Select nodes actually fetch raw data from a table". The regroup
protection was reimplemented as derivation sniffing at `merge_node.py:645-655`
(`concept.derivation in (Derivation.ROWSET, Derivation.TVF_UNION)`).

`MultiSelectMergeNode` (`merge_node.py:865`) is in the same state: documented as a
marker type, zero instantiations anywhere in `trilogy/`.

Pick one and do it properly:

- **Adopt**: build `RowsetNode` in `rowset.py:446` and switch `merge_node.py`'s
  sniff to an `isinstance` check. This is the strongly-typed option and matches
  the project's "no getattr, isinstance narrowing" preference. It also gives the
  boundary a *type*, which is what the next item needs.
- **Delete**: remove both classes and their `__all__` entries, and rewrite the
  docstring at `merge_node.py:645` so it describes the sniff that actually runs.

Adopting is preferred. If you adopt, note `test_v4_node_generators.py:469` asserts
`isinstance(node, SelectNode)`, which still passes for a subclass, so tighten it.

Also note `tests/test_incomplete_condition_disconnected.py:48` lists `"RowsetNode<"`
in `INTERNAL_REPR_MARKERS` as a string that must not leak into an error message.
That assertion is currently vacuous because the type is never built. Adopting the
type makes it meaningful again.

**2.2 Because the boundary has no type, 41 sites re-derive it.**

`grep -rn "derivation == Derivation.ROWSET\|derivation != Derivation.ROWSET" trilogy/core/processing/`
returns 41 hits across 9 files:

| file | hits |
| --- | --- |
| `v4_helper/strategy_builder.py` | 13 |
| `v4_helper/group_graph.py` | 11 |
| `v4_helper/condition_placement.py` | 7 |
| `v4_helper/concept_graph.py` | 4 |
| `rowset_islanding.py` | 2 |
| `v4_node_generators/rowset.py` | 1 |
| `grain_utility.py`, `discovery_validation.py`, `discovery_utility.py` | 1 each |

Not all of these are node-level checks (many are concept-level and legitimate),
so do **not** mass-rewrite. Triage them after 2.1: any site asking "is this *node*
a rowset boundary?" should become an `isinstance` check.

**2.3 Fix `gen_subselect`'s dead condition plumbing (found en route, separate bug).**

`gen_subselect` (`subselect.py:34`) calls
`collapse_conditions(conditions, preexisting_conditions)`, but `dispatch.py:98-106`
calls it positionally and stops at `conditions`, so `preexisting_conditions` is
always `None` and the collapse is always a no-op. Its docstring's claim that "both
this-level and inherited atoms collapse into `preexisting_conditions`" is
unreachable.

Decide whether an ancestor-applied atom should reach a subselect. If yes, pass it
in `dispatch` (ROWSET and SUBSELECT currently share that branch, so split them).
If no, delete the parameter and correct the docstring. Do not leave it as is.

### Tier 3: the real fix. Do not start before Tier 1 lands.

Plan a rowset body as a stacked source with parents, mirroring #673.

The blocker is the stated reason in `rowset.py:60-66`: the outer environment
mis-classifies the body's concepts under rowset aliasing. #673 solved the
equivalent problem for unions **without** a separate environment, by walking each
arm's lineage under its own scope label (`arm:<identity>`) in the concept graph.
The union commit message explicitly says this was done "the way rowset internals
are labelled", so the labelling machinery (`nest_scope`, `arm_scope`,
`_scope_and_phase` in `concept_graph.py`) already exists and already has a rowset
notion of scope. The open question is whether a rowset *body* can be walked under
a body scope the way a union arm now is, or whether the fresh-environment
requirement is genuinely irreducible.

**Answer that question first, in writing, before touching code.** If the fresh
environment is irreducible, Tier 3 is off and the correct outcome is to make the
current design honest instead: assert the empty-parents precondition (1.2 option 1),
document the two `strategy_builder` carve-outs as permanent, and stop there.

Acceptance criteria if Tier 3 proceeds:

- `strategy_builder.py:4277-4287` (output ordering to stop boundary hijack) deleted.
- `strategy_builder.py:4421-4429`'s ROWSET entry in the `satisfiable_outputs`
  carve-out deleted.
- Cross-rowset handle drops go to zero (currently 10, see harness).
- Conditions get a real host instead of the post hoc injection at `rowset.py:465-484`.

---

## Latent, checked, did NOT materialize. Do not chase blind.

All three were instrumented across the same sweep and measured at **zero**
occurrences. Fix them as hygiene if you are already in the code, but do not open
an investigation.

- **Duplicate outputs.** `rowset.py:237-245` (the multiselect align-arm pass)
  appends to `handles` without updating `handle_addrs`, unlike every other pass
  (`254-267`, `275-295`, `313-358` all maintain it). A concept appearing in two
  align items would be emitted twice. **0 occurrences measured.**
- **Grain-exposure gate ignores `limit`.** `rowset.py:210-214` gates raw grain-key
  exposure on `where_clause is None and having_clause is None`, but not on
  `select.limit is None`, even though `rowset.py:396` in the same function treats
  a limit as row-narrowing for the partial-marking decision. Measured 2
  unfiltered-and-limited bodies, **0 of which exposed a raw grain key**. The
  unconditioned subset domain edge pinned in `tests/test_rowset_body_limit.py`
  appears to already cover it. Worth a deliberate decision and a comment, not a
  fix under pressure.
- **Outer conditions resolved against the inner scope.** `rowset.py:474-483` passes
  `environment=inner_env, graph=inner_g` to `resolve_and_inject_condition` for a
  condition that came from the *outer* group graph. Measured **0 of 12** injected
  conditions had a row argument absent from `inner_env`.

### One real but currently harmless mismatch

The boundary node is stamped `environment=inner_env` (`rowset.py:450`) while its
concepts are looked up from the *outer* `environment` (lines 87, 150-152, 279-282,
311-313, 320, 376, 383-388, 427).

Measured: **61 of 787** boundaries emit an output concept absent from their own
stamped environment. In every observed case it is the presence probe
(`local._virt_presence_*`) that `rowset.py:276-295` pulls from the outer env.

It works because the renderer resolves the probe inline off its member handle. It
is a live trap for any future code doing
`node.environment.concepts[output.address]` on a boundary, which would `KeyError`.
Either stamp the outer environment, or register the probe in `inner_env`, or add a
comment naming the exception.

---

## Cleared false leads

Recorded so nobody re-derives them.

- **`where_clause is None` at `rowset.py:210` does correctly cover staged
  `then where`.** `BuildSelectLineage.where_clause` is documented as the AND-fold
  of `where_clauses` and is "the canonical full row gate"
  (`models/build.py:2053-2062`). The plural field is a discovery convenience, not
  a second gate. No bug here.
- **Mutating `inner_node` at `rowset.py:146` (`add_output_concepts`) is safe.**
  `V4History.get_build_history` returns `node.copy()`, so a cached node is not
  aliased into the mutation.
- **The `handle_pool` overscan is not a perf problem** at current model sizes
  (20898 total iterations across the sweep). Treat 1.5 as a readability fix, and
  do not sell it as an optimization.
- **`ruff` is clean on this file.** The unused parameters are not caught because
  `ARG` is not in `lint.extend-select` (`pyproject.toml:30`, which enables only
  `I` on top of defaults). Enabling `ARG` repo-wide would false-positive across
  every generator, since `dispatch` requires uniform signatures. Do not enable it
  just for this.

---

## Measurement harness

Every number above is reproducible. The probes are pytest plugins that wrap
`dispatch._GENERATORS[Derivation.ROWSET]`; they touch no repo code.

Put this on `PYTHONPATH` as `rowset_probe.py` and run with `-p rowset_probe`:

```python
"""pytest plugin: what gen_rowset receives and what it returns."""
import json, os, sys, collections

STATS = collections.Counter()
SAMPLES = collections.defaultdict(list)
PLANS = []


def pytest_configure(config):
    # MUST come first: importing v4_node_generators cold hits the cycle in 1.1
    import trilogy.core.processing.v4_helper  # noqa: F401
    from trilogy.core.enums import Derivation
    from trilogy.core.processing.v4_node_generators import dispatch

    original = dispatch._GENERATORS[Derivation.ROWSET]
    rowset_mod = sys.modules["trilogy.core.processing.v4_node_generators.rowset"]
    orig_plan = rowset_mod.plan_nested_select

    def plan_probe(select, history, depth, label, exclude_derived=None,
                   hide_from_connectivity=None):
        r = orig_plan(select, history, depth, label, exclude_derived,
                      hide_from_connectivity)
        PLANS.append(r)
        return r

    rowset_mod.plan_nested_select = plan_probe

    def probe(outputs, parents, environment, conditions=None,
              preexisting_conditions=None, *, history, g):
        from trilogy.core.models.build import BuildRowsetItem
        mark = len(PLANS)
        result = original(outputs, parents, environment, conditions,
                          preexisting_conditions, history=history, g=g)
        STATS["calls"] += 1
        if parents:
            STATS["DISCARDED_PARENTS"] += 1
        if result is None:
            STATS["returned_None"] += 1
            return result
        STATS["boundaries"] += 1
        # checked for EVERY boundary, before the rowset-specific early return
        env = result.environment
        if any(c.address not in env.concepts
               and c.address not in env.alias_origin_lookup
               for c in result.output_concepts):
            STATS["OUTPUT_ABSENT_FROM_STAMPED_ENV"] += 1
        addrs = [c.address for c in result.output_concepts]
        if len(addrs) != len(set(addrs)):
            STATS["DUPLICATE_OUTPUT_CONCEPTS"] += 1

        plan = next((p for p in PLANS[mark:] if p is not None), None)
        rs = [o for o in outputs if isinstance(o.lineage, BuildRowsetItem)]
        if plan is None or not rs:
            return result
        planned = rs[0].lineage.rowset.name
        produced = set()
        for c in result.output_concepts:
            produced.add(c.address)
            produced.update(c.pseudonyms)
        for o in outputs:
            if o.address in produced:
                continue
            if isinstance(o.lineage, BuildRowsetItem) and o.lineage.rowset.name != planned:
                STATS["DROPPED_OTHER_ROWSET_HANDLE"] += 1
                if len(SAMPLES["other_rowset"]) < 5:
                    SAMPLES["other_rowset"].append(
                        {"planned": planned, "dropped": o.address})
            else:
                STATS["dropped_output"] += 1
        return result

    dispatch._GENERATORS[Derivation.ROWSET] = probe


def pytest_sessionfinish(session, exitstatus):
    with open(os.environ["ROWSET_PROBE_OUT"], "w") as f:
        json.dump({"stats": dict(STATS), "samples": dict(SAMPLES)}, f, indent=2)
```

```bash
export ROWSET_PROBE_OUT=/tmp/probe.json
export PYTHONPATH=/dir/containing/rowset_probe.py
.venv/Scripts/python.exe -m pytest \
  tests/complex tests/engine tests/discovery tests/join_matrix tests/core \
  -p rowset_probe -q -m "not adventureworks_execution"
```

Baseline on `70c820954` with exactly the command above (this table was produced by
running the block verbatim, not reconstructed):

| stat | value |
| --- | --- |
| `calls` | 787 |
| `boundaries` | 787 |
| `DISCARDED_PARENTS` | 4 |
| `DROPPED_OTHER_ROWSET_HANDLE` | 10 |
| `dropped_output` | 88 |
| `OUTPUT_ABSENT_FROM_STAMPED_ENV` | 61 |
| `DUPLICATE_OUTPUT_CONCEPTS` | absent (0) |
| `returned_None` | absent (0) |

`boundaries == calls` and `returned_None == 0` are themselves worth noting: none of
`resolve_rowset`'s four `return None` paths fire anywhere in this corpus. They are
reached only by the two direct unit tests in
`tests/core/processing/test_v4_node_generators.py`. Treat the docstring's claim
that "a recursive nested-rowset search can hand a bucket of plain roots here" as
unverified by the corpus.

Narrowing the directory set moves `calls` by a few (761 to 792 observed); the other
counts are stable. A successful Tier 3 drives `DISCARDED_PARENTS` and
`DROPPED_OTHER_ROWSET_HANDLE` to zero. Tier 1 and Tier 2 must leave every number
unchanged.

---

## Gates

Per AGENTS.md, for any repo-wide change:

```bash
ruff check . --fix
mypy trilogy
black .
```

Run `ruff check . --fix` with default rules. Do **not** narrow it to
`--select E,F,I`; that hides rules CI enforces.

Planner-relevant gates, all of which apply to Tier 3 and to any Tier 2 change that
touches `merge_node.py`:

- Full suite: `pytest -m "not adventureworks_execution"`. **Never run two pytest
  processes concurrently**; the modeling suites share duckdb files under
  `tests/modeling/*/` and produce phantom failures.
- Corpus render must cover `tpc_ds` **and** `tpc_h` **and** `gcat` **and**
  `thelook`. A `tpc_ds`-plus-`tpc_h`-only sweep has previously come back
  132/132 identical while turning gcat/thelook CI red.
- A `query*.preql` sweep misses `tests/modeling/tpc_ds_duckdb/aggregates/`, which
  is where planner changes bite hardest. Include it explicitly.
- Run the fuzzer: `python -m local_scripts.fuzzer`. A corpus-identical,
  suite-green planner change has previously shipped a silent cross join.
- Gate against the **current tree** (render the corpus twice in one process),
  not against committed goldens, and always run a no-op leg.
- The `tests/modeling/**/zquery_timing_*.log` and perf PNGs are committed by
  design. Expect diffs, do not revert them, and do not commit a log from an
  interrupted run.

Guardrail suites specific to this area:

- `tests/test_rowset_generation_matrix.py` (its docstring says it exists so a
  refactor of this generator can gate on it; run it around every change here)
- `tests/complex/test_rowset.py`
- `tests/engine/test_duckdb_rowset*.py`
- `tests/engine/test_duckdb_rowset_aggregate_filter_leak.py`
- `tests/test_rowset_body_limit.py`
- `tests/core/processing/test_v4_node_generators.py`
- `tests/core/processing/test_v4_nested_select_parity.py`
