# Handoff: rowset pairing is declared — implicit machinery removed

Landed 2026-09-30 (PR #702). A rowset's outputs pair with a concept outside
the rowset only through a declared relation (`subset join rs.key = key`), or
by projecting the concept inside the rowset and reading it through the
handle. A scalar rowset beside rows stays a keyless join. The undeclared form
is a `DisconnectedConceptsException` whose message names the join
(`rowset_relation_hints`, `discovery_utility.py`). Guards:
`tests/engine/test_duckdb_rowset_aggregate_filter_leak.py`,
`tests/core/processing/test_v4_root_partition.py` (last four tests),
`tests/complex/test_rowset.py` (alias-collision tests).

One contract worth knowing: under `subset join rs.key = key` the key is ONE
axis read from the superset side (docs/subset_union_join_design.md).
`select user_id, even.user_id` shows `(1, 1, None)` for a user the rowset
filtered out, and `count(rs.key)` counts the padded rows; the intersection is
a non-key value (`count(rs.amt)`).

## What was removed (follow-up to the gate)

The gate made the implicit form an error; the planner still carried three
pieces that paired a boundary with its base by lineage. All three are gone,
with no replacement: the discovery stack now speaks only in handle addresses
and authored relation members. Nothing had to be registered at rowset
creation — a declared `subset join` already puts the handle and the base key
in one scoped-join group, and that group is the whole join axis.

- `resolve_rowset_content_address` (`build_environment.py`) and every reader:
  `_rowset_join_key_addresses`, `_resolved_rowset_grain`,
  `_unwrapped_rowset_grain`, the BASIC-rename unwrap in `_final_merge_grain`,
  the resolved-grain sibling match in `_compute_concept_sets`, the rowset
  content hop in the keyless-join guard (`join_resolution.py`).
  `output_rowset_base_keys` became `output_rowset_grain_keys` (the handle
  grain as stated). `_rowset_base_grain` is now just `grain - rollup_padded`.
- The boundary generator's raw grain-key exposure
  (`v4_node_generators/rowset.py`, "A plain rowset's GRAIN keys are the
  shared join keys back to the outer query"): a boundary publishes handles
  only. The hazard the previous handoff predicted did not materialise; the
  cases it feared (`test_rowset_with_addition`, the derived-rowset join
  matrix, q14/q54/q64) all pass with the block deleted, because they were
  breaking on the *demand-driven* variant, not on removal.
- The `island_rowsets` parameter across `discovery_utility.py` and
  `link_rowset_outputs_for_connectivity`: islanding is unconditional. The
  nested body gate (`nested_select.py`) and the existence-argument gate
  (`query_processor.py`) now island too.

Two things surfaced by the deletion and fixed alongside it:

- **Inline scalar subquery granularity.** `(select cat_avg.avg_price where
  cat_avg.category = 'a')` in expression position was minted as a MULTI_ROW
  rowset handle (its content is a per-category aggregate), so the top-level
  gate already rejected it beside `price` and the nested gate followed suit
  once islanded. An inline `(select ...)` outside a membership RHS is one
  row by construct: `RowsetDerivationStatement.scalar` /
  `RowsetLineage.scalar` / `BuildRowsetLineage.scalar` carry that, the
  handle is stamped SINGLE_ROW with an empty grain at parse time, and
  `Concept.calculate_granularity` honours the flag at build time. A
  membership RHS (`x in (select ...)`) stays a set.
  `tests/core/processing/test_correlated_inline_subquery_error_hygiene.py`
  covers both, and the correlated form now refuses at the gate with the
  typed error instead of reaching the planner.
- **FINAL dedup under a rowset output.** `_group_to_grain_if_required`
  narrows the FINAL merge's outputs and sets `force_group`, but leaves
  `MergeNode.grain` at the claimed merge grain; the merge's rowset-output
  carve-out then tested the pregrain against that stale grain (`{s.d, s.o}`
  over `select s.d, band`) and dropped the GROUP BY. The unwrap had masked
  this by spelling the pin as the mangled content (`_s_o`), which
  `Grain.from_concepts` folded away. The carve-out now tests the grain the
  outputs carry (`merge_node.py`). Guard:
  `test_duckdb_rowset_null_group_rejoin.py::test_guest_rows_dedup_at_the_output_grain`.

  That change alone put a redundant FINAL `GROUP BY` on TPC-DS q46/q68:
  their outputs include `bought.amt`/`bought.profit`, rowset handles over
  aggregates whose by-grain is the rowset grain, and selecting an aggregate
  anchors the rows at its by-grain; `_concept_coverage_addresses` only knew
  that for a bare aggregate. It now covers a rowset handle over an aggregate
  at the handle's own grain (spelled at the authored base address by the
  declared relation, which is how `bought.store_sales.customer.sk` on the
  pregrain is reached). Guards: `test_forty_six` (`GROUP BY` count),
  `test_grain_utility.py::test_grain_satisfied_by_pregrain_rowset_aggregate_handle_covers_its_grain`.

`_final_merge_grain` keeps stating a ROWSET output's own grain (or its keys
when grainless) as the result's row grain; that is not an unwrap, and
`test_duckdb_scoped_join_expression_keys_through_wrappers.py::test_subset_join_nothing_projected`
depends on it.

## `auto join <cte_name>`

Owner's suggested follow-up: bind a rowset's outputs as a subset of their
licensed parents in one declaration, instead of one `subset join` per key.
Not designed. It is sugar over the scoped-join registration a `subset join`
performs; nothing in the planner needs to change for it.

## TPC-DS q44 renders LEFT where it rendered INNER

q44's outer `where ss.store.sk = 1` paired a base concept with its two rank
rowsets only through their bodies, which already carry the filter; the
restatement was dropped. Rows are identical, but the declared `subset join
descending.rnk_d = ascending.rnk_a` now renders LEFT from `ascending` with a
coalesced key (+306 chars, rebaselined). The narrowing pass does not prove
the two rank sets equal; if that proof is cheap it recovers the INNER.

## Hint quality

The correlated inline subquery's error suggests `subset join
cat_avg.category = _cat_avg_category`: the hint names the body-mangled alias
of the correlation target. The real answer there is that correlated inline
subqueries are not a supported shape; the hint generator could say so when
the disconnected side is a `_subquery_*` rowset.
