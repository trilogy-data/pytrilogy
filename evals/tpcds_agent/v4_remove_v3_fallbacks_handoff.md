# Handoff: port the last v3-fallback derivations to native v4 generators

## Status: NOT STARTED (outline for handoff)

## Goal

Remove v4's remaining silent dependencies on the v3 node generators. Today the
v4 walker has native generators for most derivations, but **rowset**,
**multiselect**, and **recursive** still delegate to the v3-backed
`factory_dispatch`. Port each to a native `v4_node_generators` implementation so
the v4 plan is fully native, then delete the fallback machinery.

This is the natural follow-on to the window port (see
`rollup_window_bug_handoff.md`), which already removed WINDOW's v3 dependency and
hardened the dispatch so **implemented** derivations raise instead of silently
degrading.

## Current state (where the fallback lives)

`trilogy/core/processing/v4_node_generators/dispatch.py::build_node` is the v4
generator dispatch. It looks up `derivation` in `_GENERATORS` (the native
generators). Behavior after the window work:

- **In `_GENERATORS`** (ROOT, BASIC, AGGREGATE, WINDOW, FILTER, CONSTANT,
  UNNEST, UNION, SUBSELECT, GROUP_TO): run natively; an exception **propagates**
  (no fallback — a raise is a real bug).
- **Not in `_GENERATORS`** (ROWSET, MULTISELECT, RECURSIVE): `fn is None` →
  `_fallback_to_v3(...)`, which wraps the pre-built parents in a `source_concepts`
  callback and calls `v4_helper/factory_dispatch.build_node_for_group`, which runs
  the legacy `gen_<derivation>_node`.

So `_fallback_to_v3` and `factory_dispatch.py` are only load-bearing for these
three derivations. Every other handler in `factory_dispatch._HANDLERS` is already
dead (its derivation is handled natively and never reaches the fallback).

Top-level **multiselect** is a special case: it is intercepted *before* the
concept graph in `concept_strategies_v4.py::_search_concepts` (the `ms_concept`
check → `_resolve_multiselect`), which recurses per-arm through v4 and builds a
`MergeNode`/`SelectNode` directly. The fallback only fires for a multiselect that
is **not** the top-level mandatory concept (i.e. nested/wrapped — see q64).

## Which queries hit each fallback (curated test set, 75 queries)

Audited by monkeypatching `_fallback_to_v3` and running each `test_set.txt` query
through v4 planning. Totals: **rowset 24 hits, multiselect 1 hit, recursive 0**.

| Query | rowset | multiselect | note |
| --- | --- | --- | --- |
| q14 | 2 | | |
| q23 | 2 | | |
| q29 | 1 | | |
| q35 | 3 | | |
| q44 | 2 | | |
| q54 | 3 | | |
| q64 | 8 | 1 | rowset-wrapped multiselect (the lone multiselect fallback) |
| q69 | 3 | | |

The other 67 test-set queries hit **no** fallback. **Recursive** has **zero**
coverage in this set — it must get its own test query before its fallback can be
removed (otherwise removal is untested).

To re-audit at any time: monkeypatch `dispatch._fallback_to_v3` to record
`kw["derivation"]` per query, loop `local_scripts/discovery_v4.run_tpcds_query(qid)`
over `local_scripts/v4_compare/test_set.txt`, and print the per-query map. (No
DB needed — planning only.)

## What porting requires (per derivation)

### General pattern
A native v4 generator lives in `trilogy/core/processing/v4_node_generators/<x>.py`
with the uniform signature
`gen_x(outputs, parents, environment, conditions=None, preexisting_conditions=None)`
(ROOT also takes `history`/`g`). It receives **pre-built parent StrategyNodes**
and the group's `outputs` (from `GroupAttrs.output_concepts`); it must NOT
discover parents via a callback — stage 2 (group graph) already decided the
parents and outputs. Reuse `common.parent_outputs_needed(...)`. Register it in
`dispatch._GENERATORS`. Compare against the v3 generator
(`node_generators/<x>_node.py`) for the semantics to reproduce, but translate
from v3's callback-discovery idiom to v4's pre-built-parents idiom (see
`aggregate.py` / `window.py` for worked examples).

### 1. Rowset (24 hits — highest value)
- v3 generator: `node_generators/rowset_node.py::gen_rowset_node`.
- Already native in v4: the rowset's **inner** select. `build_concept_graph`
  walks rowset internals into a labeled sub-graph (`label=rowset.name`) whose
  inner concepts dispatch to native generators, and wires a bridge lineage edge
  inner-producer → outer-rowset-handle. `group_rules.partition_rowsets` buckets
  the rowset's outputs into one group (the q59 fix) with `derivation=rowset`.
- The gap: the **outer rowset boundary group** (`derivation=rowset`) has no
  native generator, so it falls back. A native `gen_rowset` must turn the
  already-built inner sub-plan (its `parents`) into the rowset handle node —
  i.e. project the rowset's outer outputs from the inner producer's outputs.
  Study what `gen_rowset_node` produces (likely a thin pass-through / alias node
  over the inner CTE) and reproduce it from `parents` + `outputs`.
- Risk: the inner/outer concept aliasing (`BuildRowsetItem.content`) — the
  outer alias address differs from the inner producer address; the generator
  must map between them (the bridge edge already encodes this in the graph).

### 2. Multiselect (1 hit — q64, nested/wrapped)
- v3 generator: `node_generators/multiselect_node.py::gen_multiselect_node`
  (and `extra_align_joins`, already reused by `_resolve_multiselect`).
- Top-level is already native (`_resolve_multiselect`). The only fallback is a
  multiselect that appears as a **group derivation** rather than the top-level
  mandatory concept (q64: a multiselect wrapped in a rowset, reached via the
  rowset inner walk).
- Two options:
  1. **Extend the interception**: detect a nested multiselect during the
     concept-graph / rowset-inner walk and resolve it via the existing
     `_resolve_multiselect` path (preferred — reuses working code, no new
     generator). Likely the cleaner fix and may fall out of the rowset port,
     since q64's multiselect is reached *through* the rowset.
  2. **Native group generator**: a `gen_multiselect` that stitches pre-built arm
     parents with `extra_align_joins` (mirrors `_resolve_multiselect` but from
     pre-built parents).
- Note: q64 depends on both rowset (8) and multiselect (1) fallbacks, so it's the
  integration test for getting these two to cooperate.

### 3. Recursive (0 test-set hits — needs coverage first)
- v3 generator: `node_generators/recursive_node.py::gen_recursive_node`.
- No curated query exercises it. **Before** removing its fallback, add a
  recursive test query (a `.preql` with a recursive CTE) to the suite and to
  `test_set.txt`, confirm it works via the v3 fallback, then port `gen_recursive`
  natively and confirm parity.

## Final cleanup (after all three are native)
1. Add `gen_rowset` / `gen_multiselect` / `gen_recursive` to `_GENERATORS`.
2. Delete `_fallback_to_v3` and the `_v3_build_node_for_group` import in
   `dispatch.py`; `build_node` then has no `fn is None` branch (every derivation
   is native — an unknown derivation should raise, not degrade).
3. Delete `trilogy/core/processing/v4_helper/factory_dispatch.py` entirely (it
   exists only to wrap v3 generators for the fallback; all its handlers are then
   unreachable). Remove its now-dead v3 `gen_*_node` imports.
4. Leave `node_generators/*_node.py` in place — v3 (`concept_strategies_v3`, the
   TPC-DS pytest suite) still uses them.

## Validation
- v4 signal is the compare harness only (the TPC-DS pytest suite runs v3):
  `python local_scripts/discovery_v4_compare.py --test-set` → expect **75/75
  match**, no gen_fail/exec_fail. Read the `- match:` / `- mismatch:` lines in
  `local_scripts/v4_compare/SUMMARY.md` (note: `grep match$` also matches
  "mismatch").
- Re-run the fallback audit (above) after each port → the ported derivation
  should drop to **0** hits. After all three, `_fallback_to_v3` is never called.
- `tests/core/processing/test_v4_*.py`, plus `ruff check trilogy`, `mypy trilogy`,
  `black trilogy tests`.
- Dataset prereq: run one `tests/modeling/tpc_ds_duckdb/` test once to generate
  the duckdb dataset the compare harness reads.

## Key files
- `trilogy/core/processing/v4_node_generators/dispatch.py` — `_GENERATORS`, `_fallback_to_v3` (remove)
- `trilogy/core/processing/v4_node_generators/{rowset,multiselect,recursive}.py` — new native generators
- `trilogy/core/processing/v4_helper/factory_dispatch.py` — delete after port
- `trilogy/core/processing/concept_strategies_v4.py` — `_resolve_multiselect` (extend for nested multiselect)
- `trilogy/core/processing/v4_helper/concept_graph.py` / `group_rules.py::partition_rowsets` — existing rowset graph handling to build on
- v3 references to mirror: `node_generators/{rowset,multiselect,recursive}_node.py`
