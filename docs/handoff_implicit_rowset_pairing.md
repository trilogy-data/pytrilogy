# Handoff: rowset pairing is declared — implicit machinery removed

Landed 2026-09-30 on PR #702 (`e9c355c9f` and the hint fix after it). A
rowset's outputs pair with a concept outside the rowset only through a
declared relation (`subset join rs.key = key`), or by projecting the concept
inside the rowset and reading it through the handle. A scalar rowset beside
rows stays a keyless join. The undeclared form is a
`DisconnectedConceptsException` whose message names the join
(`rowset_relation_hints`, `discovery_utility.py`). Guards:
`tests/engine/test_duckdb_rowset_aggregate_filter_leak.py`,
`tests/core/processing/test_v4_root_partition.py` (last four tests),
`tests/complex/test_rowset.py` (alias-collision tests).

One contract worth knowing: under `subset join rs.key = key` the key is ONE
axis read from the superset side (docs/subset_union_join_design.md).
`select user_id, even.user_id` shows `(1, 1, None)` for a user the rowset
filtered out, and `count(rs.key)` counts the padded rows; the intersection is
a non-key value (`count(rs.amt)`).

## What this session did

The gate made the implicit form an error; the planner still carried the
machinery that paired a boundary with its base by lineage. All of it is
gone, with no replacement: the discovery stack speaks only in handle
addresses and authored relation members. The previous handoff's idea of
registering `handle ⊑ base` at rowset creation was not needed — a declared
`subset join` already puts the handle and the base key in one scoped-join
group, and that group is the whole join axis. Method: stub the resolver to
identity, delete the boundary's raw-key exposure and the merge-grain
expansion, flip the two remaining gates, run the suite (9.7k tests); four
failures, each traced below. Full suite green; TPC-DS/TPC-H/thelook
`zquery*.log` SQL byte-identical except the previously rebaselined q44.

Deleted:

- `resolve_rowset_content_address` (`build_environment.py`) and every
  reader: `_rowset_join_key_addresses`, `_resolved_rowset_grain`,
  `_unwrapped_rowset_grain`, the BASIC-rename unwrap in
  `_final_merge_grain`, the resolved-grain sibling match in
  `_compute_concept_sets`, the rowset-content hop in the keyless-join guard
  (`join_resolution.py`). `output_rowset_base_keys` became
  `output_rowset_grain_keys`; `_rowset_base_grain` is `grain - rollup_padded`.
- The boundary generator's raw grain-key exposure
  (`v4_node_generators/rowset.py`): a boundary publishes handles only. The
  hazard the previous handoff predicted did not materialise — the tests it
  named broke on the *demand-driven* variant it tried, not on removal.
- The `island_rowsets` parameter across `discovery_utility.py` and
  `link_rowset_outputs_for_connectivity`: islanding is unconditional. The
  nested body gate (`nested_select.py`) and the existence-argument gate
  (`query_processor.py`) island too.
- Two hand-built unit tests of the implicit merge grain
  (`test_v4_group_behaviors.py`).

Fixed alongside, because the deletion exposed them:

1. **Inline scalar subquery granularity.** `(select cat_avg.avg_price where
   cat_avg.category = 'a')` in expression position was minted MULTI_ROW (its
   content is a per-category aggregate), so the top-level gate already
   rejected it beside `price` and the nested gate followed once islanded.
   An inline `(select ...)` outside a membership RHS is one row by
   construct: `RowsetDerivationStatement.scalar` → `RowsetLineage.scalar` →
   `BuildRowsetLineage.scalar`; `rowset_to_concepts_v2` stamps the handle
   SINGLE_ROW with an empty grain and `Concept.calculate_granularity`
   honours the flag at build time. A membership RHS stays a set. Guard:
   `test_correlated_inline_subquery_error_hygiene.py` (both tests; the
   correlated form now refuses at the gate with the typed error).
2. **FINAL dedup under a rowset output.** `_group_to_grain_if_required`
   narrows the FINAL merge's outputs and sets `force_group`, but leaves
   `MergeNode.grain` at the claimed merge grain; the merge's rowset-output
   carve-out tested the pregrain against that stale grain (`{s.d, s.o}` over
   `select s.d, band`) and dropped the GROUP BY. The unwrap had masked this
   by spelling the pin as the mangled content `_s_o`, which
   `Grain.from_concepts` folded away. The carve-out now tests the grain the
   outputs carry (`merge_node.py`). Guard:
   `test_duckdb_rowset_null_group_rejoin.py::test_guest_rows_dedup_at_the_output_grain`.
3. **Aggregate handle coverage.** (2) alone put a redundant FINAL `GROUP BY`
   on TPC-DS q46/q68: their outputs include `bought.amt`/`bought.profit`,
   rowset handles over aggregates whose by-grain is the rowset grain, and
   selecting an aggregate anchors the rows at its by-grain.
   `_concept_coverage_addresses` knew that only for a bare aggregate; it
   now covers a rowset handle over an aggregate at the handle's own grain
   (spelled at the authored base address, which is how the pregrain's
   `bought.store_sales.customer.sk` is reached through the relation's
   pseudonyms). Guards: `test_forty_six` (`GROUP BY` count),
   `test_grain_utility.py::test_grain_satisfied_by_pregrain_rowset_aggregate_handle_covers_its_grain`.
4. **Hint spelling.** A renamed body column (`with rs as select oid as k`)
   hinted `subset join rs.k = _rs_k`; `_spell_subset_join` now spells both
   sides through `_alias_source` (`rs.k = oid`).

`_final_merge_grain` still states a ROWSET output's own grain (its keys when
grainless) as the result's row grain. That is handle-spelled, not an unwrap;
`test_duckdb_scoped_join_expression_keys_through_wrappers.py::test_subset_join_nothing_projected`
cross-joins without it.

## Follow-ups: status (2026-09-30, second session)

1. **Correlated inline subquery: DONE.** The nested body gate knows the
   rowset whose body it judges (`plan_nested_select(..., rowset=)` →
   `raise_if_disconnected_for(..., scope=)`); an inline `(select ...)` whose
   WHERE reads an enclosing concept refuses as "a correlated subquery is not
   supported" and points at the enclosing-select spelling (`where price >
   1.2 * cat_avg.avg_price select sk subset join cat_avg.category =
   category`), which plans with the right rows. A body-minted aggregate
   output no longer hints a join on its mangled name (`_cat_avg_avg_price`).
   `test_correlated_inline_subquery_error_hygiene.py`.
2. **Membership RHS: DONE — a scoped join inside the subquery was already
   supported** (`oid in (select rs.k subset join rs.k = oid where cat =
   'a')`, clause order as for any select). The refusal now says "declare it
   inside the subquery, right after its select list".
3. **Body-declared join: PINNED.** `with b as select rs.k, cat subset join
   rs.k = oid` and its refusal ("inside the body of `b`") —
   `tests/complex/test_rowset_body_declared_join.py` (items 2 and 3).
4. **`rowset_source_grain` coverage candidate: KEEP.** Stubbed to identity
   in `_grain_coverage_addresses`: TPC-DS q14, q65 and
   `test_existence_feeder_pushdown.py::test_membership_feeders_do_not_chain`
   fail. It earns its keep; left alone.
5. **`auto join <cte_name>`**: still not designed; untouched.
6. **q44: DONE via `equal join`.** `equal join a = b` is `merge a into b`
   scoped to the query: one domain, `a` an alias of `b`, carried as
   `JoinType.EQUAL` in the scoped-join tuple (a statement-scoped FULL tuple
   declares INCOMPARABLE, so the tuple has to say EQUAL) and read wherever
   a FULL tuple is read. Narrowing is the merge's: under an authored EQUAL
   (`DomainGraph.declared_equal`) a rowset boundary is complete by
   construction and the other side, however filtered, fully matches it. A
   resolved EQUAL nobody declared (a lying `subset join a = rs.k` opposed by
   the filtered body's own `rs.k ⊑ a`) is NOT accepted — the preserving
   matrix cell `having_then_enrich_property_preserving` pins that. q44
   declares `equal join descending.rnk_d = ascending.rnk_a` and renders
   INNER (2916 chars; the pre-islanding plan was 2884 — the residue is the
   HAVING re-applied at the final after its push into `sparkling`).
   Grammar in both `trilogy.lark` and `trilogy.pest` (rebuild the extension).
7. Timing artifacts: unchanged this session.

Quality items uncovered and fixed alongside:

- **An INNER-joined key renders from one side.** `coalesce(a.k, b.k)` for a
  merged key both sides of an INNER join provide is one value per row; the
  renderer spells it from the first source (`CTE.inner_join_key_sources`,
  `safe_get_cte_value`). Affects every INNER-narrowed merge.
- **Dead `WITH` member on an existence-only filter over a query-backed
  table.** `select oid where oid in rs.k` emitted the base datasource's
  DatasourceCTE beside the FROM that rendered the same query inline
  (inlining declines query/file addresses). `_referenced_parents` in
  `datasource_to_cte` drops a base-datasource sub-CTE nothing names.
  `tests/core/test_cte_parents_referenced.py`.
