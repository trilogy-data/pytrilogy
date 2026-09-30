# Handoff: rowset pairing is declared — what is left

Landed 2026-09-30 (commit `90e56fd87`, PR #702). A rowset's outputs pair with
a concept outside the rowset only through a declared relation
(`subset join rs.key = key`), or by projecting the concept inside the rowset
and reading it through the handle. A scalar rowset beside rows stays a
keyless join. The undeclared form is a `DisconnectedConceptsException` whose
message names the join (`rowset_relation_hints`, `discovery_utility.py`).
Guards: `tests/engine/test_duckdb_rowset_aggregate_filter_leak.py`,
`tests/core/processing/test_v4_root_partition.py` (last four tests),
`tests/complex/test_rowset.py` (alias-collision tests).

One contract worth knowing before touching any of the below: under
`subset join rs.key = key` the key is ONE axis read from the superset side
(docs/subset_union_join_design.md). `select user_id, even.user_id` shows
`(1, 1, None)` for a user the rowset filtered out, and `count(rs.key)` counts
the padded rows; the intersection is a non-key value (`count(rs.amt)`).

## 1. The implicit machinery is still in the planner

The gate is what makes the implicit form an error; discovery itself still
knows how to pair a boundary with its base by lineage. It now runs only under
a declaration (where the relation has already canonicalized the handle's
grain onto the base address) or for scalar shapes, but it is three special
cases the authored-relation path could subsume:

- `_final_merge_grain` (`group_graph.py:920`) answers two questions with one
  set: the result's row grain (`from_concepts(outputs)`, handles as ordinary
  concepts) and the join axis a ROOT sibling needs (that grain unwrapped to
  base addresses, since a root scan never emits a handle). The ROWSET branch
  at `:949` is the unwrapping; `_rowset_join_key_addresses` (`:735`) is its
  helper. Splitting the two would delete the branch, but touches the
  authored-relation and mixed root/rowset cases in the same function.
- The boundary generator decides which base key to expose beneath a handle
  (`v4_node_generators/rowset.py:313`, "a key an EXPOSED handle already
  covers is not re-exposed"). Making it honour the group graph's demand
  outright broke `test_rowset_with_addition`, both `subset` rows of
  `test_scoped_derived_rowset_join_matrix`, tpc-ds q14/q54/q64 and a
  semi-join pushdown test (the second name for one value outranks an
  authored derived-key join), so it was left as is.
- `resolve_rowset_content_address` (`build_environment.py:363`) is read by
  the group graph (`_rowset_base_grain` `:786`, `_unwrapped_rowset_grain`
  `:798`, `output_rowset_base_keys` `projection.py:240`), the condition-root
  pairing (`_attach_condition_roots_to_rowset_consumers` `:669`,
  `_final_gate_rowset_base_keys` `:1738`) and the keyless-join guard
  (`join_resolution.py:1549`).

A cleanup would register `handle ⊑ base` at rowset creation the way a scoped
`subset join` does and let the authored machinery carry it, then delete the
three. Expect the boundary-generator hazard above to be the hard part.

## 2. Two gates still let the implicit form through

- The nested body gate, `nested_select.py:230`, passes
  `island_rowsets=False`: a rowset body reading another rowset's handle
  beside base concepts (`with b as select a.key, other_prop`) is not held to
  the rule. Flipping it should be a one-line change plus whatever the suite
  says; `_carried_union_columns` (`query_processor.py`) shows the one
  false-positive class the top-level flip needed.
- The existence-argument gate, `query_processor.py:832`, likewise: the RHS
  of `x in (<rowset handle ...>)` resolves in its own scope with islanding
  off.

## 3. `auto join <cte_name>`

Owner's suggested follow-up: bind a rowset's outputs as a subset of their
licensed parents in one declaration, instead of one `subset join` per key.
Not designed; item 1 is the natural place for it to land, since a rowset
that mints `handle ⊑ base` relations at creation is most of the feature.

## 4. TPC-DS q44 renders LEFT where it rendered INNER

q44's outer `where ss.store.sk = 1` paired a base concept with its two rank
rowsets only through their bodies, which already carry the filter; the
restatement was dropped. Rows are identical, but the declared `subset join
descending.rnk_d = ascending.rnk_a` now renders LEFT from `ascending` with a
coalesced key (+306 chars, rebaselined). The narrowing pass does not prove
the two rank sets equal; if that proof is cheap it recovers the INNER.
