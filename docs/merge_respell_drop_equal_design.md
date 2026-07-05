# Respell `merge` as subset/union; demote EQUAL to a derived fact

Status: **proposed** (2026-07-04, owner design discussion). Not started. Builds
on `subset_union_join_design.md` (phases 1+2 landed), `merge_join_unification.md`
(landed), and feeds `domain_graph_design.md` (phase 3, approved).

## Proposal

Two changes, separable but mutually reinforcing:

1. **Respell global merge with the join vocabulary**: `merge subset a = b`
   (a ⊆ b) and `merge union a = b` replace `merge a into ~b` / `merge a into b`.
   One relation vocabulary, two scopes — `merge` becomes purely the "global
   scope" marker; the relation words are shared with query-scoped joins.
2. **Drop EQUAL as an authored relation.** No bespoke EQUAL spelling at either
   scope. EQUAL becomes a *derived* fact: a symmetric pair of subset
   declarations (`merge subset a = b` + `merge subset b = a`) yields
   EQUAL-strength planning. The lattice is union < subset < equal, and two
   authored words span it.

We considered and rejected dropping global merge entirely: a model file with no
selects has nowhere to author a scoped join, merges ride through imports
(conformed-dimension stitching, `environment.py` import propagation), and
phase 3 wants environment-level domain edges anyway — merge IS their natural
authoring statement. The capability stays; only the spelling and the EQUAL
claim change.

## Why EQUAL should not be authored

- **It is exactly mutual subset.** Everything EQUAL buys — either side
  authoritative for the key, INNER narrowing, no mandatory coalesce,
  EQUAL-trust intersection semantics — follows from a ⊆ b ∧ b ⊆ a and can be
  derived in the registries (`equal_join_keys` populated from symmetric subset
  pairs instead of plain-merge membership).
- **Validation already decomposes it directionally.** `validate_domains`
  (`trilogy/core/domain_validation.py`) runs one containment COUNT per
  declared direction; EQUAL is natively checked as two subset checks today.
- **The failure-mode asymmetry.** Plain merge is the strongest claim in the
  language: a lying EQUAL narrows to INNER and silently drops rows. Repo
  evidence says authors over-claim it routinely because it is the perf
  spelling — of the ~8 in-repo plain merges, most are truthfully subsets
  (`merge holdings.symbol.* into symbol.*` in stocks, `merge org.state_code
  into state.code` in gcat). Under mutual-subset spelling the lazy author
  declares one direction and the failure mode flips from silent row drop to
  "renders FULL, slower" — exactly the phase-2 preserving philosophy. Making
  the strong claim cost two deliberate statements is a feature: genuine domain
  equality between independently-loaded sources is rare and fragile (one
  late-arriving fact row falsifies it).
- **Phase 3 gets simpler.** The domain graph design carries directed ⊑ plus ≡
  edge types; with EQUAL derived, ≡ is the symmetric closure of ⊑ and the
  graph drops to one directed relation type plus ∦.

## Migration mapping

| today | becomes |
|---|---|
| `merge a into ~b` (a ⊆ b) | `merge subset a = b` |
| `merge a into b` (EQUAL, honest) | `merge subset a = b` + `merge subset b = a` |
| `merge a into b` (EQUAL, over-claimed) | `merge subset a = b` (the truthful direction) |
| `merge a into b` (aliasing/imputation) | see open question below |
| scoped `subset join` / `union join` | unchanged |

In-repo triage (2026-07-04 counts): 12 subset merges across 7 files
(mechanical); ~8 plain merges — stocks `holdings.*` (2) and gcat
`org.state_code` are over-claims → one-way subset; hackernews
`recursive_parent`/`parent`/`github.repo_url` (5) are definitional aliasing.
`tests/profiling/query*.preql` and `tpc_ds/query3.preql` hits on the `merge`
keyword are multiselect, not global merge — untouched.

## Engine work

All planner machinery is already shared (`merge_join_unification.md`); merges
are `(source, target, JoinType)` tuples on `Environment.merges` injected into
`Factory.scoped_joins`. The changes:

- **Grammar**: replace `merge_statement` argument shape in `trilogy.lark:161`
  and `trilogy.pest:203` (rust — wheel rebuild via maturin). Keyword `merge`
  is ALSO the multiselect keyword (`pest:191`) — the statement changes shape,
  the keyword stays reserved.
- **Hydration**: `parsing/v2/rules/merge_rules.py` + `MergeStatementV2`
  (syntax.py / statement_plans.py / statement_planner.py) +
  `_pending_merges` (semantic_state.py) — carry a relation type instead of a
  SHORTHAND_MODIFIER; `Environment.merges` tuples already carry `JoinType`, so
  the field shape may not change at all.
- **EQUAL derivation**: wherever plain-merge membership currently feeds
  `equal_join_keys` / EQUAL-trust (registry construction in `process_query`),
  substitute detection of symmetric SUBSET pairs. Same trust level — a
  declared symmetric pair is as deliberate as today's plain merge.
- **Render round-trip** (`parsing/render.py`), executor/dialect/metadata
  statement dispatch: mechanical respell.
- **Deprecation**: keep `merge a into [~]b` parsing as a legacy alias mapping
  onto the new forms (mirroring how `left`/`full join` were retained), or
  hard-cut — owner call.

## Costs / risks

1. **Plans weaken where authors under-declare.** One-directional subset where
   equality holds renders FULL/directional instead of INNER. Perf-only, and
   the right failure direction. Tooling mitigation: a `trilogy validate` hint
   ("b ⊆ a also holds on this data — declare it?").
2. **Symmetric pairs can drift** — deleting one line of the pair silently
   weakens plans. Same mitigation.
3. **The aliasing family reads oddly** (open question below).
4. **Test re-ruling**: `tests/test_join_merge_parity.py` (merge≡join parity
   becomes new-spelling parity), `tests/join_matrix/` EQUAL cells (adversarial
   intersection cells re-key to "both directions declared"),
   `test_merge_discovery.py`, plus agent docs (`trilogy/ai/constants.py`,
   `syntax_examples.py`) and the phase-2/3 design docs.

## Open question: aliasing / imputation

`merge recursive_parent into root_parent.id` (hackernews) declares
*definitional identity* — one side has no independent source and the relation
is not a claim about two datasets. Mechanically any relation type works (the
identity + pseudonym collapse path runs for every collapsed endpoint), and
direction is meaningless when one side is unbound, so a one-way
`merge subset derived = target` suffices. But "subset" is semantically mushy
for "is the same thing." Options: accept the mush; or give definitional
identity its own future spelling (it is arguably a concept-definition concern,
not a domain relation). Decide during implementation; do not let it
resurrect a general EQUAL.

## Also rejected along the way (2026-07-04 discussion)

- `subset join a ~= b` operator spelling (drop the relation keyword): the
  tilde attaches to the operator so subset direction stays implicit; `~=`
  means not-equal in MATLAB/Lua; a one-character typo silently changes a
  domain declaration whose lie drops rows; agent authorship favors words.
- Per-side markers (`join ~a = b` subset, `join ~a = ~b` union, `join a = b`
  EQUAL): elegant — matches per-side partial stamps and datasource `~`
  bindings, and the union-homogeneity rule falls out — but it makes EQUAL the
  *unmarked default* (the laziest spelling is the strongest claim) and single
  dropped tildes silently change semantics. Kept for the record in case the
  EQUAL-demotion lands first, which removes the unmarked-default hazard.
