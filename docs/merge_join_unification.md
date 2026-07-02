# Merge / query-scoped-join unification

Status: **done** (2026-07-02). A global `merge` and a query-scoped `join` are the
same relation declared at different scopes and resolve through one shared path;
there is no merge-specific machinery left in the planner.

## Semantics

- `merge a into b` — a FULL relation declared once on the environment.
- `merge a into ~b` — a LEFT relation (a is the optional/partial side, b the
  preserved anchor), equivalent to `left join b = a`.
- `<full|left> join a = b` — the same relations declared inside one SELECT.

**Invariant:** a query resolves *identically* whether the relation was declared
globally (merge) or locally (join) — same plan, same rows. The only difference
is **scope** (below). `tests/test_join_merge_parity.py` pins this, including the
asymmetric-domain LEFT case and the merge-as-imputation case (relation target
with no independent source, sourceable only through the other side's
derivation).

There is no INNER at the language level. A non-partial relation is FULL (keep
all rows, coalesce the key); a partial relation is LEFT. SQL may still narrow a
join to INNER via the optimizer when a downstream filter proves the optional
side non-null — a rendering optimization, not a language type.

## Scope (the one legitimate difference)

Which builds a relation is injected into:

- Environment merges apply to **every** statement and are re-injected at every
  sub-build boundary (`get_query_node` appends `environment.merges`; the
  multiselect align/derive/where factories filter `scoped_joins` down to the
  global subset in `build.py to_environment`).
- A query-scoped join lives on its statement's lineage and flows into that
  query's own sub-builds via `BuildCaches.scoped_joins` (a rowset body needs the
  outer query's joins to source a combined cross-fact input — TPC-DS q29), but
  dies with the statement.
- At a rowset-body boundary, `_scoped_joins_for_rowset` strips relations that
  reference the rowset's own outputs (self-reference recursion guard). Note
  `get_query_node` re-injects environment merges after that filter, so a merge
  survives the guard; this asymmetry is only observable for a relation whose
  endpoint is the enclosing rowset's own output.

## Shared mechanism (build.py `Factory.__init__`)

All relations land in `Factory.scoped_joins` and feed:

- `_build_scoped_merge_index` — union-find: LEFT roots on the source (anchor),
  FULL roots on the target; every displaced address maps to its group canonical.
  LEFT targets are marked partial.
- SUBSTITUTION (`scoped_merge_map` → `_build_concept` swap): collapsed
  addresses rebuild as their canonical everywhere (refs, grains, datasource
  column bindings). Root-keyed identity pairs therefore render as ONE physical
  column when a single source suffices.
- IDENTITY + pseudonym: **every collapsed-away endpoint** keeps its own
  identity in `alias_origin_lookup` and gets a mutual pseudonym to its
  canonical, so the relation is sourceable from either side — including when
  one side has no independent source (imputation/aliasing:
  `merge derived_metric into unbound_property`).
- Join typing at resolution: `scoped_full_join_keys` drives FULL,
  `scoped_left_anchor_keys` seeds the join tree on the LEFT anchor (q78
  directional dedup) — both now include merges and query joins alike.
- Coalesce of distinct physical key columns happens at the merge node.

## Known incompleteness (pre-existing, unchanged)

For a ROOT-keyed OUTER relation both mechanisms run: the authored source key
stays projectable via identity, but its datasource *binding* is substituted to
the canonical, so the FULL coalesce attaches only to the canonical side and a
single-source plan reads one table (no cross-domain union). See the strict
xfail in `tests/test_scoped_join_permutations.py`.

## History

Until 2026-07-02 a merge additionally performed an identity collapse keyed on
membership in `environment.merges` (three special-cases in `Factory.__init__`:
a `full_join_sources` exclusion, a `scoped_left_anchor_keys` exclusion, and a
merge-only branch in the pseudonym loop). Deleting them initially broke three
shapes — all `NoDatasourceException`s where the relation target had no
independent source. The fix was generalizing the shared path: wire identity +
mutual pseudonyms for *any* collapsed endpoint of a FULL/LEFT relation (which
also covers N-way chain displacement the direction-only logic missed), rather
than only for LEFT targets and binding-keyed FULL sources.
