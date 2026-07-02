# Join matrix

Systematic coverage of build-scoped join relations (query `join` clauses and
global `merge` statements — one relation, two scopes). New join behavior gets a
cell HERE first; the scattered per-bug files listed below are regression pins
that predate the matrix.

## Structure

Every cell asserts exact rows against a **python oracle computed from the same
row data that builds the datasources** — expectations are never hand-written,
so silent fan-out, widening, narrowing, and row loss all surface as row diffs.
The shared data (harness.py) is adversarial by construction: keys exclusive to
each side (LEFT vs FULL vs accidental INNER), duplicate key rows with
power-scaled values (fan-out changes sums), and an opt-in nullable axis
(NULL-key rows + `?` column declarations).

| file | axes |
|---|---|
| `test_two_source_matrix.py` | key kind (root / derived / rowset) × join type (LEFT / FULL) × form (join / merge) × nullable |
| `test_composite_matrix.py` | composite key kind (both-plain / plain+derived / derived+derived) × join type; fan-out uniqueness guard |
| `test_multiway_matrix.py` | 3-way derived-key relations: chained/pairwise join form vs merge stack |
| `test_consumption_matrix.py` | post-join WHERE on the optional side; `is not null` intersection idiom × form |

Known-broken cells are `xfail(strict=True)` with the defect description and
bug-doc pointer in the reason — a fix flips them loudly.

## Pinned defects (xfail cells)

- **Nullable rowset keys** — `?` nullability doesn't propagate through rowset
  lineage; NULL keys stop matching null-safely (two half-rows).
- **Nullable derived keys** — NULL derived key drops the row entirely, even the
  LEFT anchor's own row.
- **FULL derived key as LEFT operand** — `full join ta.w + 52 = nb.w` collapses
  the rowset key onto the derived expr as group canonical and drops the key or
  disconnects; author the derived expr on the right of `=`.

The q59 composite family (plain co-key dropped → fan-out; comp_mixed LEFT
widening) was FIXED 2026-07-02 in `_enrich_rowset_node` (rowset_node.py): a
scoped-join key-group member is never satisfied through its group-mate
pseudonym when computing the enrichment request — each joined side must
materialize its own column. The former live-failing guards in
`tests/test_scoped_derived_rowset_join_matrix.py` now assert correct rows.

## Legacy / adjacent join tests (regression pins, keep)

- `tests/test_join_merge_parity.py` — join/merge parity incl. positions,
  chained groups, N-way LEFT intersection, FULL-mix rejection.
- `tests/test_scoped_join.py` — build-time merge behavior pins, q29 nullable-FK
  handling, q78 chained-composite parse guard.
- `tests/test_scoped_join_permutations.py` — root-key permutations incl. the
  strict xfail for the root-OUTER binding-substitution incompleteness.
- `tests/test_scoped_derived_rowset_join_matrix.py` — derived-rowset join
  shapes; hosts the live-failing q59 shared-canonical guard.
- `tests/test_scoped_left_join_multi_partial_anchor.py` — q78 LEFT-anchor
  seeding (multi-partial dedup stays LEFT).
- `tests/test_rowset_generation_matrix.py` — rowset generate+execute sweep
  ("correct rows or clean error, never sentinel").
- `tests/test_scoped_join_{expression_keys,dim_bridge_outer_key,rowset_outer_blend,rowset_inner_filter,cross_rowset_*}.py`,
  `tests/test_rowset_derived_twice_join_bugs.py`,
  `tests/test_cross_rowset_*.py` — one file per historical defect.
- `tests/generators/test_merge_concept_node.py`,
  `tests/optimization/test_merge_coalesce_impute.py` — merge-as-imputation and
  canonical-collapse SQL shapes.
- `tests/discovery/test_canonical_collision_merge.py` — shared-canonical
  multiselect collision.
