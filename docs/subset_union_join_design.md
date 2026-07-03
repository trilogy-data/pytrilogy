# SUBSET / UNION joins — design sketch

Status: **phase 1 landed** (2026-07-02, owner-approved direction). Successor
to the LEFT/FULL scoped-join paradigm; see `merge_join_unification.md` for the
landed relation mechanisms these declarations ride on.

## Landed (phase 1 — declarations + narrowing groundwork)

- **Syntax**: `subset join a = b` (a ⊆ b) and `union join a = b` parse in both
  grammars and normalize at hydration (`_normalize_select_join`) onto the two
  landed relation mechanisms — SUBSET to the superset-anchored LEFT_OUTER
  tuple (`merge a into ~b` scoped to the query; superset anchors, subset side
  partial, nullable promotion applies), UNION to the FULL-registry relation.
  `SelectJoin.authored` keeps the declared form for round-trip rendering.
  Row contracts pinned cell-for-cell in `tests/join_matrix/`.
- **EQUAL narrowing** (`CONFIG.optimizations.narrow_equal_domain_joins`,
  default OFF): a non-partial `merge a into b` is treated as an EQUAL domain
  declaration — its keys release the FULL-preservation veto in
  `UpgradeOuterFromKeySetEquivalence` and may narrow FULL → INNER, including
  between two *authoritative scans* (direct unfiltered single-datasource
  sides; that completeness evidence is only trusted for EQUAL-declared keys).
  Query-scoped `full join` / `union join` keys never narrow. Proof cells:
  `tests/join_matrix/test_narrowing_matrix.py`.
- Deliberately NOT landed: the render flip (below), reinterpretation of
  authored `left join`, agent-doc (`trilogy/ai/constants.py`) migration, and
  SUBSET-driven FULL → directional narrowing (pre-flip, LEFT is already the
  narrowest row-identical rendering for a declared subset — the case only
  materializes once defaults render FULL).

## Remaining (phase 2 — the flip)

- Flip rendering to always row-preserving (FULL + `is not distinct from`) with
  the narrowing pass load-bearing; dissolve the `get_join_type` decision table.
- SUBSET-driven FULL → directional narrowing (superset side complete +
  subset side null-partnered) — becomes live once defaults render FULL.
- Re-rule the `tests/join_matrix/` oracles for always-preserving semantics;
  the residual derived-LEFT-zip xfails should dissolve.
- Apply the migration mapping (below): authored `left join` needing row drops
  gains its explicit filter; deprecate/remove `left join`/`full join`;
  migrate agent docs (`trilogy/ai/constants.py`) and TPC-DS references.
- Decide the EQUAL spelling (keep `merge`?) and default
  `narrow_equal_domain_joins` on; add the opt-in lying-subset validation.

## Phase 2 implementation notes (learned building phase 1)

- **The zip xfails do NOT route through `get_join_type`.** Generator-authored
  NodeJoins (the derived-LEFT same-grain zip family) are typed in
  `translate_node_joins` / the merge-node paths. Dissolving the decision table
  flips only datasource-join typing; the preserving default must also land in
  those generator seams or the xfails survive the flip.
- **Defaulting `narrow_equal_domain_joins` on re-rules existing matrix
  cells.** The adversarial rows violate EQUAL by construction, and both sides
  of the root-key merge cells are *authoritative scans* — with the flag on
  they legitimately narrow to INNER and lose the side-exclusive rows. That
  becomes the correct ruling (lying declaration = author error), so the
  merge-form full cells' oracle moves to intersection semantics; do it
  deliberately, alongside the opt-in validation.
- **Narrowing coverage boundaries** (the perf-cliff map): completeness
  evidence is `group_to_grain` CTEs or (EQUAL-gated) authoritative scans;
  `UnionCTE` sides are opaque; a *filtered* superset side never proves
  subset-match — identical filters don't help, since a filter on another
  column can drop domain values asymmetrically. Post-flip, every FULL failing
  these tests stays FULL; measure against the TPC-DS timing logs before
  flipping.
- **Provenance seam**: EQUAL vs UNION classification lives in `process_query`
  (statement FULL tuples are subtracted from merge-derived equal keys — an
  authored `union join` on a merged key keeps the veto). Downstream of
  hydration, `subset join` is indistinguishable from swapped `left join`
  (deliberate); if phase 2 keeps `left join` row-drop semantics through a
  deprecation window, it needs a provenance registry at this same seam.
- `JoinType.SUBSET` / `JoinType.UNION` exist as parse-level enum members only
  and must never reach SQL rendering — hydration (`_normalize_select_join`)
  is the single normalization point.

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
