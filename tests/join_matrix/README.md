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
| `test_narrowing_matrix.py` | declared-domain narrowing proofs on HONEST data: EQUAL (`merge`) FULL→INNER by default; UNION never narrows; flag-off opt-out |
| `test_domain_validation.py` | opt-in lying-declaration checks (`trilogy/core/domain_validation.py`) |

## Join semantics (phase 2 landed 2026-07-03, docs/subset_union_join_design.md)

A relation declares DOMAIN knowledge, never row intent, and **rendering is
always row-preserving** — no join silently drops a row. Row restriction is an
explicit author predicate (the both-sides `is not null` idiom). The narrowing
pass (`UpgradeOuterFromKeySetEquivalence`) restores directional/INNER joins
only when provably row-identical; a *filtered* superset side never proves, so
those relations keep their preserving FULL.

- `subset join a = b` (≡ `merge a into ~b`, ≡ legacy `left join b = a`):
  a ⊆ b. On adversarial (declaration-violating) data with an unfiltered
  superset side, the narrowing legitimately drops the lying subset-side rows —
  the cells rule `expected_left`.
- `union join a = b` (≡ legacy `full join a = b`): neither domain contains the
  other; never narrows (`full_join_keys` veto), cells rule `expected_full`.
- non-partial `merge a into b`: the EQUAL declaration. With
  `narrow_equal_domain_joins` defaulted ON it narrows to INNER wherever both
  sides prove value-complete — on the adversarial rows that DROPS the
  side-exclusive rows (lying declaration = author error), so the merge-form
  full/union cells rule intersection (`expected_equal`).
- NULL is not a value: nullability never affects the declared relation; NULL
  keys bind null-safely (`is not distinct from`) and NULL groups pair rather
  than drop. The former derived-LEFT-zip xfails dissolved with the flip (the
  scan-level nullability stamp for computed BASIC keys keeps the zip
  null-safe, so narrowing to INNER stays row-identical).

## Legacy / adjacent join tests (regression pins, keep)

- `tests/test_join_merge_parity.py` — join/merge parity incl. positions,
  chained groups, preserving + explicit-filter idiom pairs.
- `tests/test_scoped_join.py` — build-time merge behavior pins, q29 nullable-FK
  handling, q78 chained-composite parse guard, preserving shared-base cells.
- `tests/test_scoped_join_permutations.py` — root-key permutations incl. the
  strict xfail for the root-OUTER binding-substitution incompleteness.
- `tests/test_scoped_derived_rowset_join_matrix.py` — derived-rowset join
  shapes; hosts the q59 shared-canonical guard.
- `tests/test_scoped_left_join_multi_partial_anchor.py` — q78 family:
  preserving render + explicit-filter restriction.
- `tests/test_rowset_generation_matrix.py` — rowset generate+execute sweep
  ("correct rows or clean error, never sentinel").
- `tests/test_rowset_cross_datasource_outer_read.py` — rowset-body scoped
  joins read back beside external properties (same-address provenance
  arbitration via `subset_binding_sources`).
- `tests/test_scoped_join_{expression_keys,dim_bridge_outer_key,rowset_outer_blend,rowset_inner_filter,cross_rowset_*}.py`,
  `tests/test_rowset_derived_twice_join_bugs.py`,
  `tests/test_cross_rowset_*.py` — one file per historical defect.
- `tests/generators/test_merge_concept_node.py`,
  `tests/optimization/test_merge_coalesce_impute.py` — merge-as-imputation and
  canonical-collapse SQL shapes.
- `tests/discovery/test_canonical_collision_merge.py` — shared-canonical
  multiselect collision.
