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
| `test_two_source_matrix.py` | key kind (root / derived / rowset) × join type (LEFT / FULL / SUBSET / UNION) × form (join / merge) × nullable |
| `test_composite_matrix.py` | composite key kind (both-plain / plain+derived / derived+derived) × join type; fan-out uniqueness guard |
| `test_multiway_matrix.py` | 3-way derived-key relations: chained/pairwise join form vs merge stack |
| `test_consumption_matrix.py` | post-join WHERE on the optional side; `is not null` intersection idiom × form |
| `test_narrowing_matrix.py` | declared-domain narrowing proofs on HONEST data: EQUAL (`merge`) FULL→INNER under `narrow_equal_domain_joins`; UNION never narrows |

SUBSET/UNION (docs/subset_union_join_design.md) are the domain-declaration
spellings of the two landed relations: `subset join a = b` (a ⊆ b, superset
anchors — row parity with the swapped authored LEFT / `merge a into ~b`) and
`union join a = b` (neither contains the other — row parity with FULL). The
matrix pins that parity cell-for-cell; the always-preserving render flip is a
future re-ruling of these oracles.

Known-broken cells are `xfail(strict=True)` with the defect description and
bug-doc pointer in the reason — a fix flips them loudly.

## Pinned defects (xfail cells)

- **Nullable derived keys, LEFT** (2 cells) — the relation itself is null-safe
  and correctly typed; the final same-grain zip between the two aggregate
  branches is a generator-authored NodeJoin rendered INNER with plain `=` (the
  kb-side branch loses nullability through the kb→ka substitution rename), so
  the NULL key group drops at the zip. Expected to dissolve under the
  SUBSET/UNION redesign (docs/subset_union_join_design.md).

## Join typing semantics (landed 2026-07-02)

`get_join_type`: partials decide direction (partial = subset of VALUES, the
optional side). Nullability is orthogonal — it null-safes the equality and
affects type through ONE rule: a nullable side whose NULLs have no null-safe
partner (the other side NOT nullable) must be preserved, promoting to FULL.
When both sides are nullable, the null-safe equality pairs the NULL groups and
the authored direction stands. This resolves the authored-LEFT vs `merge ~`
consolidation conflict without a merge/join distinction.

## Fixed 2026-07-02 (formerly pinned here)

- **q59 composite family** (plain co-key dropped → fan-out; comp_mixed LEFT
  widening): `_enrich_rowset_node` no longer satisfies a scoped-join key-group
  member through its group-mate pseudonym — each joined side materializes its
  own column. The former live-failing guards in
  `tests/test_scoped_derived_rowset_join_matrix.py` now assert correct rows.
- **FULL derived key as LEFT operand**: the rowset-outer identity machinery now
  engages when EITHER endpoint of a FULL pair is rowset-keyed (FULL is
  symmetric); previously that direction substituted the derived key away.
- **NULL join keys silently not matching** (the half-row split / row-loss
  family): nullability now propagates through the rowset boundary (translation
  node), through BASIC derivations (`k + 1` over a `?` column), into
  GroupNode/MergeNode resolution, and generator-authored NodeJoins compute
  null-safety in `translate_node_joins` — so nullable keys render
  `is not distinct from`. FULL cells and all root-keyed cells are fully
  correct; LEFT cells are row-correct except the widening above.

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
