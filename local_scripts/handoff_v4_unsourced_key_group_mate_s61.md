# Handoff — v4 never sources an unreferenced scoped-join key-group mate (s61→s62)

**RESOLVED 2026-08-02 (s62).** The three join-matrix cells are fixed and
`tests/join_matrix/conftest.py::V4_FAILING` is EMPTY. Both parity registries
(`tests/v4_known_failing.py`, join-matrix `V4_FAILING`) are now empty.

## The fix (three parts, all required)

Root cause as diagnosed in s61: a scoped-join key-group member projected with
nothing else referencing its MATE never pulled that mate into the plan.

1. **`v4_helper/concept_graph.py::_unsourced_relation_mates`** — the ported
   "requesting half" of v3's `rowset_node._relation_key_group_mates`. After
   the mandatory/condition walks, adds unreferenced ROWSET-handle group mates
   of demanded members as first-class concept-graph nodes:
   - COALESCING member demanded → its unauthored rowset mates (bare-member
     projection).
   - SUBSET side demanded as a RAW member → its anchor rowset (the
     `subset join cust = members.mid` cells), via
     `domain_graph.subset_join_map()`.
   - Withheld: authored mates (probe machinery owns them — q35/q44), rowset
     LHS (union-reproject collapse-to-LHS), composite relations onto one
     anchor rowset (pinned clean error), ROOT mates (datasource machinery).

2. **`v4_helper/strategy_builder.py::_add_partial_completion_contributors`** —
   final assembly previously dropped the mate's boundary group (covers no
   mandatory output). Adds it as an axis-only contributor when (a) a covered
   member is PARTIAL in its contributor (subset direction; requires a
   COMPLETE mate — `_clear_groupmate_completed_partials` then un-marks it), or
   (b) the member is in a COALESCING relation (both sides partial by
   declaration, so the mate qualifies by exposing the member at all).
   Plus, in `_assemble_final_node`: the merge EMITS the coalescing mate as a
   hidden output (`axis_mates`) — otherwise parent dedup drops the side as
   redundant — and wraps in a forced GroupNode when the contract dedups
   (the merge's claimed grain hides the sides' finer-grain fan; MergeNode's
   rowset-output carve-out waves force_group through, so an explicit
   GroupNode is required, same as the probe-feeder branch).

3. **`v4_helper/group_graph.py::_refresh_final_contract`** — the
   `axis_only_projection` (no-dedup) contract now additionally requires some
   relation to have BOTH sides projected (q59 keys_only fan preserved); a
   SOLE projected member is the unified axis, deduped to output grain.

## Verification (s62)

- `tests/join_matrix` full: **310 passed, 0 failed** with `--runxfail`
  (both planner params).
- A/B gate `tests/join_matrix tests/engine tests/core/processing --runxfail`:
  **1460 passed / 0 failed** vs this tree's prior 3 failed / 1457 passed.
- `ruff --select E,F,I`, `mypy trilogy` (326 files), `black`: clean.
- Full suite (`pytest . -q -p no:randomly -m "not adventureworks_execution"`):
  **6309 passed, 102 skipped, 1 xfailed, 0 failed, 0 xpassed** (26m03s).
  vs s61 baseline 6306/4-xfailed: the 3 fixed cells moved xfail→pass; the
  1 surviving xfail is `tests/execution/state/test_schema_refresh.py::
  test_snowflake_type_change_triggers_refresh` — a fakesnow >= 0.11.4
  limitation (multi-statement persist + cursor.description), nothing to do
  with the planner. NOTE: s61's attribution of the 4th xfail to
  `test_property_hop_alignment_matrix.py` was stale — that file carries no
  xfail and is fully green under both planners.

## Next: v3 removal — DONE

Executed on branch `cleanup/remove-v3-discovery`: every item below landed as
written (relocations included), plus a second pass that deleted the v3-only
helpers those deletions freed inside the SHARED modules. The map is kept as
the record of what was removed.

- **v3-only, deletable:** `concept_strategies_v3.py`, `discovery_node_factory.py`,
  `node_generators/{basic,constant,filter,group,group_to,node_merge,subselect,synonym,union_select,unnest,window}_node.py`
  (~7.5k lines).
- **SHARED — keep:** `node_generators/{common,presence_probe,select_node,select_merge_node}.py`
  and `select_helpers/*` (`v4_helper/source_planning.py:199` calls
  `history.gen_select_node`).
- **Relocate before deleting:** `multiselect_node.extra_align_joins`,
  `recursive_node.GATING_CONCEPT`, `union_node.{build_layers,is_union}`,
  `rowset_node.{_interpose_limit_node,_scoped_joins_for_rowset}`,
  `concept_strategies_v3.append_existence_check` (v4 path calls it,
  query_processor ~769).
- **`query_processor.get_query_node`:** lines ~948–1061 are the v3 tail.
- **Flag:** `CONFIG.use_v4_discovery` (`constants.py:152`), `TRILOGY_V4_DISCOVERY`
  + `pytest_collection_modifyitems` in `tests/conftest.py`, `planner` fixture
  in `tests/join_matrix/conftest.py`.
- **~19 test files** import `concept_strategies_v3` (many only for `History`,
  re-exported from `processing/nodes`).
- **Footgun:** ~20 `local_scripts/` set `CONFIG.use_v4_discovery = False` —
  silent no-op once the field is gone (plain dataclass).

Also still open from s60 (plan quality, not correctness): 6 TPC-DS perf and
26 size counterexamples vs v3.
