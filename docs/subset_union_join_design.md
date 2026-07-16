# SUBSET / UNION joins — design sketch

Status: **phase 2 landed** (2026-07-03). Rendering is row-preserving by
default; the narrowing pass is load-bearing. See `merge_join_unification.md`
for the relation mechanisms these declarations ride on.

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
  `subset_binding_sources` (subset address → datasources natively binding it,
  computed on the author environment PRE-substitution) identifies the true
  subset side of the rendered pair. Full relation-scoped partiality
  provenance remains future work; this registry covers the double-relation
  readback family.
- **Registries** built in `process_query` and threaded through
  `optimize_ctes`: `full_join_keys` (UNION veto), `equal_join_keys`,
  `subset_join_map`, `scoped_canonical`, `subset_binding_sources`. Rowset-BODY
  scoped joins (`with rs as left join ... select ...`) are folded into the
  registries via `_collect_rowset_scoped_joins`.
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

## Deferred / residuals

- `left join` / `full join` (and `inner` / `right` / `cross`) spellings are
  **removed** — they still lex as `JOIN_TYPE` but are rejected at hydration
  (`select_statement_rules.join_clause`) with a migration hint pointing at the
  `subset` / `union` equivalents (mapping below). SUBSET/UNION are the only
  query-scoped join declarations.
- Row-identical zips between re-aggregated branches may still RENDER FULL when
  their evidence is ambiguous (e.g. a measure in the zip key set); rows are
  correct, the cost is perf. SQL-shape assertions were re-ruled to row
  contracts where this bites (comp_mixed).
- Full relation-scoped partiality provenance (stamps keyed by relation, not
  address) would subsume `subset_binding_sources` and let more double-relation
  shapes narrow. (The former root-OUTER binding-substitution xfail in
  `tests/test_scoped_join_permutations.py` was fixed pre-flip — binding-keyed
  OUTER sources stay on the identity path — but the address-level stamp
  ambiguity it exemplified is this same seam.) Phase 3 design:
  docs/domain_graph_design.md — an environment-level concept domain graph
  (directed domain relations with edge conditions + functional-dependency
  edges for cardinality reduction) replacing the registries and stamp
  carve-outs wholesale.

## Motivation

The current relation types (LEFT / FULL) smuggle a *row filter* into a
*relation declaration* — the same defect that led to removing scoped INNER
("INNER is just a filter"). A season of bugs traces to the entanglement:

- `get_join_type` must arbitrate partiality vs nullability vs authored
  direction, and every ruling breaks a different case (nullable-widening vs
  authored LEFT; merge~ consolidation vs strict LEFT; anchor-registry override
  corrupting multiselect aligns).
- The core conflict is irreducible in the LEFT/FULL paradigm: "optional side
  is partial AND nullable" demands FULL for consolidation (`merge X into
  ~date.*` must keep NULL-key fact rows) and LEFT for authored joins (drop
  unmatched optional rows) — the same signature, opposite intents.

The root insight (owner): **a relation should declare domain knowledge, not
row intent.** Partial (`~`) means "subset of *values*", and NULL is not a
value — nullability speaks to rows, subset speaks to domains, and the two
must never interact in join typing.

## The model

Two relation declarations (replacing scoped `left join` / `full join` and the
merge forms):

- **SUBSET JOIN a = b** — a's non-null values are contained in b's. Pure
  planner knowledge:
  - the superset side is authoritative for the key: no coalesce, single-source
    plans stay legal;
  - join-narrowing to INNER/LEFT is *provable* when the subset side fully
    matches (feeds `value_set_join_upgrade`);
  - join-tree anchoring seeds on the superset side (today's q78
    `scoped_left_anchor_keys`);
  - maps from today's `merge a into ~b` and (with an explicit filter, below)
    authored `left join`.
- **UNION JOIN a = b** — neither domain contains the other; coalesce is
  mandatory and both sides are needed for key completeness (today's
  FULL-registry machinery, unchanged). Maps from today's `full join` /
  non-partial `merge`.

Open sub-question: today's non-partial `merge a into b` asserts *equal*
domains (mutual subset) — the strongest optimization case (either side
authoritative). Decide whether EQUAL is its own declaration or spelled as
SUBSET both ways; recommendation: keep `merge` as the EQUAL declaration since
it already carries identity semantics.

**Rendering: always row-preserving.** Every relation renders FULL +
`is not distinct from`; nothing ever silently drops a row ("joins do not drop
nulls" becomes definitional instead of enforcement logic). The optimizer
narrows FULL → LEFT/INNER only when provably row-identical. Row restriction is
always an explicit author predicate — the established post-INNER idiom
(`where x is not null`).

## What this dissolves

- the entire `get_join_type` partial/nullable decision table (landed 2026-07-02
  form: partials decide direction; a nullable side whose NULLs have no
  null-safe partner promotes to FULL) — replaced by "FULL, narrow when
  provable";
- nullable-vetoes-partial and every widen/narrow arbitration;
- the merge~-vs-authored-LEFT NULL-row conflict (both FULL; restriction is
  explicit);
- the residual join-matrix xfails (derived-LEFT zip: the zip becomes FULL +
  null-safe by default).

## Costs / risks

1. **Performance becomes optimizer-dependent.** FULL + null-safe everywhere is
   hostile to engines (no hash-join fast path, weak pushdown). Today's
   directional types doubled as perf hints; the narrowing pass becomes
   load-bearing. SUBSET/EQUAL metadata is exactly what makes narrowing
   provable — implement narrowing BEFORE flipping defaults.
2. **Migration churn.** Every authored `left join` relying on dropping
   unmatched optional rows needs its explicit filter added (TPC-DS q64/q78
   etc.); `merge ~` → SUBSET and `merge` → EQUAL are mechanical.
3. **Lying subsets.** A declared SUBSET whose data has extra values (the q59
   shape authored against unclean data) is an author error; prefer a cheap
   opt-in validation (`trilogy validate`-style domain check) over engine
   heroics.

## Migration mapping

| today | becomes |
|---|---|
| `merge a into ~b` | `subset join a = b` (a ⊆ b) |
| `merge a into b` | EQUAL (keep `merge`?) |
| `full join a = b` | `union join a = b` |
| `left join a = b` | `subset join b = a` + explicit filter if rows must drop |
| scoped INNER (removed) | already: outer + `where x is not null` |

## Test strategy

`tests/join_matrix/` is the harness: the oracle computes expected rows from
row data, so re-ruling the cells for always-preserving semantics is a matter
of changing the oracle join functions. Phase 1 pinned subset/union cells to
the existing `expected_left` / `expected_full` oracles (pre-flip the rows are
identical by construction); the flip introduces `expected_subset` (preserve
unmatched subset-side rows) and re-rules those cells. EQUAL narrowing proofs
on honest data live in `test_narrowing_matrix.py`; extend with SUBSET cells
(fully-matching data must render LEFT/INNER post-optimization) when the flip
makes them live, so the perf contract stays pinned, not assumed.
