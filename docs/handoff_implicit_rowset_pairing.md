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

## Follow-ups for the next agent

1. **Correlated inline subquery: say so.** `where price > 1.2 * (select
   cat_avg.avg_price where cat_avg.category = category)` refuses with the
   generic disconnected error plus a `subset join cat_avg.category = category`
   hint. Correlated inline subqueries are not a supported shape; when the
   stranded side is a `_subquery_*` rowset (`SUBQUERY_NAMESPACE_PREFIX`) and
   the other side is read in the subquery's own WHERE, the message should say
   that instead of proposing a join. `rowset_relation_hints` is the place.
2. **Membership RHS over a rowset reading base concepts.** `select oid where
   oid in (select rs.k where cat = 'a')` now refuses (the existence gate
   islands): `cat` is outside `rs`. Reasonable under the rule, but the hint
   proposes `subset join rs.k = oid`, which is a statement-level relation and
   not what the author needs inside the RHS. Either the RHS scope should
   accept a scoped join of its own, or the hint should say "project `cat`
   inside `rs`". Decide which; `tests/core/processing/test_v4_existence_feeder_set_grain.py`
   is the neighbourhood.
3. **`with b as select rs.k, cat`** (a rowset body reading another rowset's
   handle beside a base concept) refuses at the nested gate and works with
   `subset join rs.k = oid` in the body. No test pins the body-declared form
   or the refusal; add one beside `tests/complex/test_rowset.py`.
4. **`grain_utility.concept_source_address` still unwraps a handle to its
   content** (`rowset_source_grain`, used by `_grain_coverage_addresses` and
   `grain_satisfied_by_pregrain`). It is grain comparison within one plan,
   not pairing, so it was left alone; but with handles now the only spelling
   the discovery stack uses, it is worth checking whether the
   `rowset_source_grain` candidate in `_grain_coverage_addresses` still
   earns its keep (stub it to identity and run the suite, as this session
   did for the resolver).
5. **`auto join <cte_name>`** (owner's suggested sugar: bind a rowset's
   outputs as a subset of their licensed parents in one declaration). Not
   designed. It is purely parser/registration work over the scoped-join
   groups a `subset join` mints; nothing in the planner needs to change.
6. **TPC-DS q44 renders LEFT where it rendered INNER** (+306 chars,
   rebaselined earlier on this PR). The declared `subset join
   descending.rnk_d = ascending.rnk_a` is not proven equal-domain by the
   narrowing pass; the two rank sets are the same rows ranked two ways, so
   the proof may be cheap.
7. **Benchmark timing artifacts** (`tests/modeling/*/zquery_timing_*.log`,
   `*-summary.md`, `*.png`) were regenerated by full-suite runs on a loaded
   machine and committed as regenerated; timings there are noise, SQL is
   unchanged.
