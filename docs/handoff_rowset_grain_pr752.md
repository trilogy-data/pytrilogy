# Handoff: rowset grain cleanup on PR #752

Branch `fix/tpcds-rebaseline-deepseek-flash`, worktree
`C:\Users\ethan\coding_projects\pytrilogy-evalpr`.

- **Pushed, CI green:** `8863649c2` (and everything before it).
- **Local only, not pushed:** `60feec048`, `adf885cb5`. Push once the suite
  below is clean.

## Goal

PR #752's B4 fix counted a rowset key distinctly with a special flag
(`counted_key_repeats`). The owner wants it gone: `count(key)` over a rowset
should dedup through the normal grain rules. Doing that exposed that rowset
boundaries lied about their grain. Finish replacing the lies with honest grains.

## Design rules (owner-confirmed; do not relitigate)

1. **No intrinsic rows.** A row is the tuple of the requested concepts.
   `count(key)` is distinct wherever the key comes from (rowset, set op).
   `count(grain(a, b))` counts distinct tuples. `count(property)` counts the
   property's grain tuples.
2. **A node's grain is the grain of the rows it emits.** An ungrouped node
   inherits its source's rows, so a datasource scan is at the datasource grain
   even when it does not project those columns. A grouped node is at what it
   groups by.
3. **A rowset boundary's grain is the rowset's full identity**, whether or not
   the query reads all of it. Never stamp the demanded subset.
4. **Join keys are requested through the graph, never inferred from a node's
   grain.** If a join needs a column, discovery or the concept/group graph
   requests it as an output.
5. **Set ops.**
   - A `UNION ALL` gets a synthetic per-arm key (`__arm`).
   - A set op's identity is each arm's grain positions, read through select
     aliases, plus the arm key. An arm whose grain is not among its outputs
     contributes its whole output tuple, as any select does.
   - Identity positions are keys, and so are outputs that are keys in every
     arm a tuple can come from. All other outputs are properties keyed on the
     identity.
   - Projections of a set op are distinct tuples: `select k, v` over a stack
     returns `(2, 5)` once. Aggregates range over the identity: `sum(v) by k`
     counts a cross-arm repeat twice.
6. No `__row` synthetic row key yet. The owner is not convinced it is needed.

## What is done (local commits)

- `counted_key_repeats` and its helpers are deleted.
  - `count(key)` over a rowset member ranges over the key's own values.
  - Any other aggregate over a member ranges over the rowset's identity
    (`_walk_scoped_aggregate_grain_inputs`, `concept_graph.py`).
- `_rowset_row_identity` returns a set op's key outputs.
- `resolve_rowset` stamps the honest grain:
  - a select body: `_body_grain`, the body grain mapped to handles;
  - a multiselect or union: the full derived handles;
  - the subset-join dedup (`_fans_subset_join`) is kept.
- Set-op lowering (`tvf_rules._lower_set_op`): the arm key, identity positions
  (`_identity_positions`, `_alias_source`, `_tuple_sources`, `_is_arm_key`),
  and output purposes and keys (`union_item_to_concept` takes `keys`).
- Inline expressions over rowset members take the rowset identity
  (`_aggregate_input_grain`). This fixed q97, which returned wrong rows.
- A `BASIC` over rowset handles gets upstream edges to the rowset handles it
  is keyed on (`_add_concept`).
- Tests updated to the agreed semantics:
  - stacked projections are now distinct tuples (`test_duckdb_rowset.py`,
    `test_duckdb_union_subset_join_key_set.py`, `test_v4_nested_select_parity.py`);
  - union output purposes (`test_parse_engine_v2.py`);
  - the q83 ordering assertion checks the ordered measure is grouped, not its
    alias.

## Open items

### 1. Two wrong-rows failures (the blocker)

`tests/engine/test_duckdb_rowset_null_group_rejoin.py::test_stand_in_band_pins_on_the_keyed_spelling`,
both models with `TWO_PROP_ROWSET` (`select order_number as o, item_desc as d,
item_name as n, quantity as q`), drop the unsold item's stand-in row
`('G', None)`. `NAME_ONLY_ROWSET` passes.

Leading hypothesis:

- Band is now (correctly) read with `s.o`, so the band stream exposes `s.o`.
- The FINAL merge then pairs the region domain with the band stream on more
  than the region axis `s.n`.
- `final_merge_grain` in `_assemble_final_node` is the union of contributors'
  projection grains, and `_widen_merge_join_keys` carries all of it.
- A stand-in member has no `s.o`, so it cannot match and drops out.

Fix direction (rule 4): the domain pairs on the region axis only.

**Diagnose first:**

- Dump `_widen_merge_join_keys` arguments and the final merge's joins for this
  query, old vs new.
- In-process A/B: monkeypatch `rowset._body_grain = lambda *a, **k: None` to get
  the old demanded-subset stamp.
- History: `region_reads`, `_add_region_domain_contributors`, and memory notes
  on stand-ins (`project_standin_key_full_and_rename_beside_hash`,
  `project_null_member_domain_and_one_sided_spine`).

### 2. Benchmark growth to review after item 1

- q29: 6202 → ~6643 characters.
- q64: 16808 → ~18030 characters.
- q76: the arm column, on top of the already-accepted growth.

"SQL must not move" means no added work. Shrink where the growth is a second
bug (memory `feedback_plan_shift_cost_is_often_a_second_bug`). Otherwise add
an entry to `tests/modeling/accepted_growth.toml` with a reason. Rebaseline
with `TRILOGY_BENCHMARK_REBASELINE=1`. Earlier in this work q64's dimension
joins briefly turned INNER → FULL; confirm that has not come back.

### 3. Re-check broad behaviour

Last broad run, before the current commit, had 4304 passed. Rerun everything
listed below.

### 4. Wrap-up

- Update the PR #752 description. B4 is now "normal grain resolution, no
  flag". Add the set-op identity model, the honest rowset grain, the
  networkx-free SCC in the Rust graph core, B8 rewritten over SCCs, and q87
  `count(grain(...))`.
- Then push and track CI. Never ask to run the full suite locally; push and
  watch CI.

## How to run things

- **Python:** the shared venv `..\pytrilogy\.venv\Scripts\python.exe`, run
  from the worktree root with `-m pytest`, so the worktree's `trilogy` wins.
- **Rust extension (required).** The branch adds
  `strongly_connected_components` to `trilogy/scripts/dependency/src/graph.rs`.
  The shared venv's compiled extension predates it, so most planning fails
  with `PyGraphCore has no attribute strongly_connected_components` unless you
  use a fresh build without installing it:

  ```bash
  python -m maturin build --release --out <scratch>/wheel     # from the worktree root
  # unzip only _preql_import_resolver/ from the wheel into <scratch>/ext
  PYTHONPATH=<scratch>/ext python -m pytest ...
  ```

  Do not `maturin develop` into the shared venv; it is installed editable
  against the main tree.
- **Targeted suites:** `tests/engine tests/discovery tests/core tests/complex
  tests/modeling tests/test_parse_engine_v2.py -m "not adventureworks_execution"`,
  about 8 minutes.
- **Before pushing:**
  - `ruff check .`, `black .`, `mypy trilogy` (only the existing
    `haybarn_engine.py:39` error is expected);
  - `python .scripts/check_benchmark_growth.py origin/main`.
- **Benchmark byproducts** (`*.png`, `*-summary.md`, `zquery_timing_*`,
  `memory_sf001/load.sql`, Cargo version bumps) are dirty in the worktree.
  Never commit them; commit with `git commit -- <paths>`.
