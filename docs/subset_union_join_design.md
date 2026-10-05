# SUBSET / UNION joins

A relation declares domain knowledge, not row intent. `subset join a = b`
says a's non-null values are contained in b's; `union join a = b` says neither
contains the other; `equal join a = b` (or a persistent `merge a into b`) says
the domains are one. NULL is not a value, so nullability never enters join
typing. Rendering is row-preserving by default and the narrowing pass narrows
FULL → LEFT/INNER only when provably row-identical; row restriction is always
an explicit author predicate (`where x is not null`).

## Landed (phase 1 — declarations + narrowing groundwork, 2026-07-02)

- **Syntax**: `subset join a = b` (a ⊆ b) and `union join a = b` parse in both
  grammars and normalize at hydration (`_normalize_select_join`) onto the two
  landed relation mechanisms — SUBSET to the superset-anchored LEFT_OUTER
  tuple (`merge a into ~b` scoped to the query), UNION to the FULL-registry
  relation. `SelectJoin.authored` keeps the declared form for round-trip
  rendering. Row contracts pinned cell-for-cell in `tests/join_matrix/`.
- `JoinType.SUBSET` / `JoinType.UNION` exist as parse-level enum members only
  and never reach SQL rendering — hydration (`_normalize_select_join`) is the
  single normalization point.

## Landed (phase 2 — the flip, 2026-07-03)

- **`get_join_type` decision table dissolved** (join_resolution.py). Rendering
  is preserving-by-default: any partial (SUBSET-declared) side renders FULL —
  partiality and nullability never interact (subset speaks to VALUES, NULL is
  not a value). Neither-side-partial keys are EQUAL by binding declaration and
  render INNER (both-nullable pairs bind null-safely instead of widening to
  FULL); a nullable side with no null-safe partner keeps a preserving
  directional join toward it. The nullable-vetoes-partial arbitration and the
  merge~-vs-authored-LEFT conflict are gone.
- **SUBSET-driven directional narrowing**
  (`UpgradeOuterFromKeySetEquivalence._narrow_directionally`): a FULL narrows
  to the directional join preserving the superset side — and a directional
  join whose preserved side fully matches narrows to INNER — when the subset
  side is DECLARED (CTE partial stamps, or the side-specific
  `subset_join_map` for derived keys) and the superset side PROVES value
  completeness. Evidence: `group_to_grain` grain membership, authoritative
  scans (including off-grain non-partial bindings), 1:1 passthrough /
  grain-arity rowset wrappers, BASIC/ROWSET lineage transfer
  (`_complete_values` — a derived key's domain is the image of its inputs'),
  and preserved-base join CTEs. A *filtered* superset side never proves —
  those FULLs stay FULL and row restriction is the author's explicit filter.
- **Same-address pair arbitration by provenance**: when several relations
  collapse onto one canonical group the partial stamps land symmetrically;
  per-column origin stamps (`BuildColumnAssignment.origin_address`) identify
  the true subset side of the rendered pair (docs/domain_graph_design.md).
- **Relations reach the optimizer as a `DomainGraph`** assembled in
  `process_query`; rowset-BODY scoped joins are folded in via
  `_collect_rowset_scoped_joins`.
- **EQUAL narrowing defaults ON** (`narrow_equal_domain_joins: bool = True`).
  The adversarial merge-form matrix cells were re-ruled to intersection
  semantics (lying declaration = author error). EQUAL evidence extends past
  classic key-set equivalence: value-completeness on both sides + equivalent
  filters, null-safing the pair when a side is nullable (NULL groups pair —
  never a silent drop), and EQUAL-trust intersection completeness for
  all-INNER zips of complete parents (the 3-way merge stack).
- **Opt-in lying-declaration validation**:
  `trilogy/core/domain_validation.py` — one containment COUNT per declared
  direction (`validate_domains`); must run against a clean (unmerged) parse
  since an active merge makes the check self-referential. Proof cells:
  `tests/join_matrix/test_domain_validation.py`.
- **Migrations applied**: TPC-DS q77 (arm-local `is not null` filters restore
  the reference's row drops; the only battery query that needed one — the
  rest narrow or already carry restricting filters); the intersection-idiom
  pins across the test suite (`tests/test_rowset_offset_join_contract.py`,
  `test_rowset_generation_matrix.py`, `test_join_merge_parity.py`, q78 anchor
  family, …) gained their explicit both-sides `is not null` filters with
  preserving-contract variants; agent docs (`trilogy/ai/constants.py`,
  `syntax_examples.py`) teach subset/union as primary with left/full as
  legacy aliases.
- **EQUAL spelling**: `merge a into b` stays the EQUAL declaration (it already
  carries identity semantics); no new keyword.

## Landed (phase 2b — `equal join`, 2026-09-30)

- **`equal join a = b`**: the EQUAL declaration at query scope, `merge a into
  b` scoped to one select. One domain — `a` is an alias of `b` — so the pair
  collapses onto one canonical key like a merge and narrows under the same
  rules (`equal_narrowable_keys`); nothing is trusted that a global merge does
  not trust. Carried as `JoinType.EQUAL` in the scoped-join tuple (a
  statement-scoped FULL tuple declares INCOMPARABLE, so the tuple itself has
  to say EQUAL); every reader of a FULL tuple reads an EQUAL one the same way.
- **Coverage is tautological on an unfiltered side.** A rowset boundary is
  complete by construction; under an EQUAL relation the other side, however
  it is filtered, is a subset of that one domain and fully matches it
  (`_pair_side_fully_matches`). TPC-DS q44 (two rowsets ranking the same rows
  two ways) declares `equal join descending.rnk_d = ascending.rnk_a` and
  renders INNER.
- **An INNER-joined key renders from one side.** A merged key both sides of
  an INNER join provide is one value per row; the renderer spells it from the
  first source instead of `coalesce(a.k, b.k)` (`CTE.inner_join_key_sources`).

## Residuals

- `left join` / `full join` (and `inner` / `right` / `cross`) spellings are
  **removed** — they still lex as `JOIN_TYPE` but are rejected at hydration
  (`select_statement_rules.join_clause`) with a migration hint pointing at the
  `subset` / `union` equivalents (mapping below). SUBSET/UNION/EQUAL are the only
  query-scoped join declarations.
- Row-identical zips between re-aggregated branches may still RENDER FULL when
  their evidence is ambiguous (e.g. a measure in the zip key set); rows are
  correct, the cost is perf. SQL-shape assertions were re-ruled to row
  contracts where this bites (comp_mixed).

## Migration mapping

| old spelling | becomes |
|---|---|
| `merge a into ~b` | `subset join a = b` (a ⊆ b) |
| `merge a into b` | `equal join a = b` (query scope); `merge` stays the persistent form |
| `full join a = b` | `union join a = b` |
| `left join a = b` | `subset join b = a` + explicit filter if rows must drop |
| scoped INNER (removed) | already: outer + `where x is not null` |
