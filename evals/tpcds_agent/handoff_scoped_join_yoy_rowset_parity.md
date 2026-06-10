# Handoff: scoped JOIN parity for the year-over-year (rowset→rowset same-base) shape

**Status:** RESOLVED (2026-06-08). `yoy_independent_rowsets` now passes as a real
`join`/`merge` parity case; `yoy_shared_parent` stays xfail (a deeper
rowset-derived-twice nested-aggregate limit that fails the `merge` reference
identically). See "The fix that landed" below. Original investigation notes kept
for context.

## The fix that landed
Two independent gaps, fixed separately:

1. **Rowset join resolution** (`rowset_node.py::_scoped_join_targets`). A scoped
   join collapses a rowset's derived key onto the sibling's key (the join
   *target*), but the source rowset advertised only the dropped source address.
   Now, when the rowset still outputs the shared base dimension underlying the
   collapsed key, it advertises the *target* join concept sourced from that base,
   so the outer merge joins both rowsets on the canonical key. A freshly-computed
   key (q44's rank) has no shared base column — substitution already labels the
   rowset's own column with the target address — so the bridge is inert there.
   LEFT joins mark the advertised target partial (new
   `BuildEnvironment.scoped_partial_sources`, threaded from the Factory).

2. **Projecting a collapsed join source** (`utility.py::sort_select_output_processed`
   + `ProcessedQuery.scoped_merge_map`). This was a GENERAL scoped-join bug, not
   rowset-specific: selecting the *source* side of ANY in-query join (e.g.
   `select orders.customer_id` where it collapses onto `customers.customer_id`)
   silently dropped the column at render, because the authoring output address
   isn't in the built CTE (which holds the canonical) and no pseudonym links them.
   The render now resolves an unmatched output through the scoped-merge map and
   renders the canonical column under the written source name. Unit tests:
   `test_projected_join_source_renders` / `_target_renders` in `test_scoped_join.py`.

REJECTED here (kept failing the graph validator / regressing): adding the source
as a pseudonym on the canonical at build time — post-substitution the source
address no longer exists as a concept, so `env_processor.add_concept` rejects the
pseudonym ("invalid pseudonym"). The render-layer map avoids that entirely.

## Goal
A query-scoped `join` should produce outcomes **identical to the equivalent
`merge`**, including the year-over-year self-join shape (q75/q64), while keeping
the *build-time* scoped-join machinery (it's the performant path). "Parity with
merge," not a new discovery path.

## Current tree state (safe, green)
- `trilogy/core/models/build.py` = **HEAD** (substitution baseline; all
  experiments reverted).
- Kept additions (safe, low-risk):
  - **Literal `X = X` self-join guard** in
    `trilogy/parsing/v2/rules/select_statement_rules.py` (`join_clause`) + test
    `test_literal_self_join_rejected` in `tests/test_scoped_join.py`. Rejects
    joining a concept to *itself* (degenerates to `1=1`). Distinct concepts that
    share a base (two rowset outputs) are still allowed.
  - **Parity harness** `tests/test_join_merge_parity.py`: runs each shape as a
    `join` and as the equivalent `merge` against real data, asserts both equal a
    concrete expected result. `rowset_fk_to_dim` (Bug A) passes;
    `yoy_independent_rowsets` and `yoy_shared_parent` are **xfail** (the gap).
- Suite is green: q44/q46/q68/q97/three-way/datasources all pass. q75/q64 remain
  multiselects (which work).

## How scoped joins work today (substitution)
1. `_build_scoped_merge_index` (build.py ~2188) builds a union-find
   `source → canonical` map from the join clauses (`scoped_merge_map`).
2. `Factory._build_concept` (build.py ~2560) **substitutes**: building a source
   concept returns the canonical target (address swap). So `c.brand` builds as
   `p.brand`; `orders.customer_id` builds as `customers.customer_id`.
3. **Datasources** flow through `_build_concept`, so their columns bind to the
   canonical → the datasource "provides" the canonical →
   `build_canonical_address_map` / `get_node_joins`
   (`trilogy/core/processing/join_resolution.py`) emit the join predicate.
4. **Rowsets**: the source FK output survives as an `alias_origin_lookup` entry
   (build.py ~3425) carrying the canonical as a pseudonym. `gen_rowset_node`
   (`trilogy/core/processing/node_generators/rowset_node.py`) has a **bridge**
   (`_pseudonym_bridge_keys`, committed in `rowset_fixes`): when a rowset can't
   satisfy requested optionals, it sources the dim on the FK's canonical
   pseudonym and merges. This is how **Bug A** (rowset FK → dim attributes)
   works today.

**Confirmed:** a rowset DOES provide the partial FK join key — the Bug A SQL is
`INNER JOIN customer ON questionable.bought_physical_sales_customer_id =
wakeful.customer_id`. The bridge exists to join the dim for the *attributes* the
rowset lacks, not to "provide customer.id."

## The gap (year-over-year)
Two rowsets each output a same-base key:
```trilogy
rowset c <- where sales.year = 2002 select sales.item.brand as brand, sum(sales.qty) as c_qty;
rowset p <- where sales.year = 2001 select sales.item.brand as brand, sum(sales.qty) as p_qty;
inner join c.brand = p.brand
select c.brand, c.c_qty, p.p_qty;
```
Both `c.brand` and `p.brand` resolve to base `sales.item.brand`. Under
substitution `c.brand → p.brand`, but the `c` rowset does **not advertise**
`p.brand` (its `derived_concepts` lists `c.brand`; `gen_rowset_node` emits
`c.brand` via alias_origin). Discovery dead-ends:
`ValueError: Cannot resolve query. No remaining priority concepts, have
attempted {'p.brand', 'c.c_qty'}`.

**Key open question:** q44 is *also* a rowset→rowset same-base join
(`ascending.rnk_a = descending.rnk_d`, both ranks over the same filtered `ss`)
and it **resolves correctly under substitution**, projecting the collapsed rank
as output. Why does q44's rank collapse resolve but the brand collapse doesn't?
Understanding this difference is the most promising route to a *targeted* fix.

## Approaches tried and REJECTED (evidence)
### 1. Pseudonym-link instead of substitution
`_build_concept` keeps the source address and adds the canonical as a pseudonym
(so discovery joins via the same canonical-pseudonym path `merge` uses).
- Unit suite: clean except the 4 `test_buildenv_*` contract tests (which assert
  substitution). Independent-rowset year-over-year + Bug A + three-way prune all
  worked.
- **Modeling regressions:** q44 (rank join) errors and q97 (datasource
  *multi*-collapse: two FKs → one `customer.id`, LEFT, existence CASE) row-
  mismatches. Substitution's collapse is load-bearing there. → **REJECTED.**

### 2. Rowset advertises the canonical (`derived_concepts` mapping) + sub-factory `scoped_joins`
Map `_build_rowset_lineage.derived_concepts` through `scoped_merge_map`, and pass
`scoped_joins` into the `_build_rowset_item` sub-factory so the map is populated.
- Year-over-year **resolves** and aggregates are **correct** (`(7,7),(15,3)`),
  with a real `INNER JOIN on thoughtful.p_brand = wakeful.p_brand`.
- **But breaks:**
  - **q68 (Bug A):** "Invalid reference string." The rowset advertises
    `customer.id` as a *full* provision, colliding with the dim (which provides
    the complete row + attributes). It should advertise it as **PARTIAL** (an FK
    key only) — exactly the datasource-FK model.
  - **q44:** "GROUP BY cannot contain window functions" — remapping a rank/window
    output's address is invalid.
  - **Projection bug (even in pure year-over-year):** the collapsed key selected
    as an *output* is dropped at render. `ProcessedQuery.output_columns` keeps
    `c.brand` (not hidden), but the final CTE exposes `p.brand`, and the renderer
    can't map `c.brand → p.brand` → the column vanishes from the final SELECT
    (it still appears in ORDER BY, so the column exists in the CTE).
- **Isolation:** sub-factory `scoped_joins` ALONE (no mapping) → q68/q44 resolve,
  year-over-year still fails. `derived_concepts` mapping ALONE → no-op (the
  sub-factory has no `scoped_joins`, so `scoped_merge_map` is empty there). So the
  mapping only fires *with* the sub-factory change, and that is exactly when it
  breaks q68/q44. → **REJECTED as a blanket change.**

## Likely shape of a correct fix
1. **Advertise the mapped key as PARTIAL**, like a datasource FK binding — not a
   full provision. This should remove the q68 collision (the dim still provides
   the complete row + attributes; the rowset just contributes the key). This is
   the user's core insight.
2. **Do not remap window/aggregate outputs** (q44's rank). Likely q44 should keep
   working via plain `_build_concept` substitution and be *excluded* from any
   `derived_concepts` remap. Better: find a uniform rule that needs no exclusion.
3. **Fix the projection drop**: a collapsed key selected as output must render —
   map `c.brand → p.brand` at render time via `scoped_merge_map` /
   `alias_origin_lookup`. Find where `output_columns` are matched to CTE source
   columns (dialect render path / `query_processor`) and resolve through the
   scoped merge.
4. **Answer the q44-vs-year-over-year question first** — the minimal change may
   fall out of why one resolves and the other doesn't under identical
   substitution.

## Key files / functions
- `trilogy/core/models/build.py`: `_build_scoped_merge_index` (~2188),
  `Factory._build_concept` substitution (~2560), `_build_rowset_item` (~3098),
  `_build_rowset_lineage` (~3130), scoped alias_origin population (~3425),
  `_normalize_grain_components` (~3168, collapses grain to canonical).
- `trilogy/core/processing/node_generators/rowset_node.py`: `gen_rowset_node` +
  `_pseudonym_bridge_keys` (the Bug A bridge; `_optional_satisfied` early-exit fix).
- `trilogy/core/processing/join_resolution.py`: `build_canonical_address_map`,
  `get_node_joins` (merge join path).
- `trilogy/core/processing/nodes/merge_node.py`: `generate_joins` /
  `create_full_joins` (the `1=1` fallback when `pregrain.components` is empty —
  what year-over-year hits when keys don't unify).

## Validation harness
- `tests/test_join_merge_parity.py` — flip the two year-over-year cases from
  xfail to pass when fixed. `rowset_fk_to_dim` must stay green.
- Regression guards that MUST stay green: `test_forty_four`, `test_forty_six`,
  `test_sixty_eight`, `test_ninety_seven_one`, `test_ninety_seven_two`, and
  `tests/test_scoped_join.py` (esp. `test_three_way_join_collapses_like_merge`,
  `test_aggregate_grouped_by_merged_key`, the `test_buildenv_*` substitution
  contracts).
- Full sweep: unit (~3618 passed) + `tpc_ds_duckdb` modeling (~106 passed).
  Pseudonym-link took modeling to 5-fail; that's the trap to avoid.

## Pitfalls
- **Concurrent agents share this working tree.** q46/q68 were concurrently
  converted to the single-rowset join form. Never mutate git state
  (no stash/reset/checkout of others' work).
- Direct `execute_text` vs `PRAGMA tpcds(N)` comparisons can show **false**
  mismatches for tie-ordered queries (observed for q44). Trust the actual pytest
  test, which is authoritative.
- A failed `run_query` leaves a STALE `zquery<N>.log`; inspect SQL via
  `engine.generate_sql(text)[-1]` instead.
