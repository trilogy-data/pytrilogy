# Handoff: final audit of `extension-row-null-semantics` (2026-10-09)

The pre-merge audit asked three questions: did generated SQL get simpler, does the
keyspace now overlap other nullability machinery, and is the code DRY. This doc
records what it found, what it fixed, and what is still open.

## 1. Generated SQL vs main (merge base 44d8513a8)

Compared using the committed `zquery<N>.log` files, `generated_sql` from
`origin/main` against `HEAD`.

| corpus | chars | CTEs | JOINs | GROUP BYs |
|---|---|---|---|---|
| TPC-DS (104 queries) | 389,314 -> 382,477 (-1.8%) | 356 -> 348 | 457 -> 457 | 189 -> 189 |
| TPC-H | 27,513 -> 27,046 (-1.7%) | 38 -> 38 | 65 -> 65 | 26 -> 26 |
| thelook | 12,432 -> 11,296 (-9.1%) | 15 -> 15 | 38 -> 35 | 20 -> 21 |

GROUP BYs were 192 before this audit's q16/q94/q95 fix.

**Execution time.** The 33 changed TPC-DS queries (sf=1, DuckDB) ran interleaved,
min of 3, main SQL vs branch SQL: **3.91s -> 3.65s (-6.6%)**. Three queries were
skipped because their SQL takes parameters (q08, q76, q77). Biggest wins:

- q64 0.43s -> 0.23s
- q35 0.18s -> 0.09s
- q57 0.15s -> 0.10s
- q29 0.12s -> 0.07s

Only q11 got slower (see below). The committed timing logs are not comparable
across the branch: the last branch run was about 3x slower on everything,
parse time included, so the machine was loaded.

**Growth, all read by hand:**

- **TPC-DS q11, +1009 chars, 2 more JOINs, 0.08s -> 0.16s.** This is a
  correctness fix. Main dropped the row-level WHERE (`sales.channel in (...)
  and year in (...)`) entirely, and its rows matched only because the HAVING
  implies it (memory: plan-build perf audit, "Dropped WHERE atoms"). The branch
  computes that WHERE as a second pass over the sales union, joins it to the
  aggregate, then GROUP BYs.
  - **Opportunity:** when the existence rows and the per-key aggregate read
    the same stream, the existence test could be one more HAVING term on the
    aggregate (`count(case when <row atoms> then 1 end) > 0`). That would
    drop the second scan. Not attempted.
- **TPC-DS q64, +279 chars, CTEs 14 -> 10, JOINs 18 -> 22.** Fewer CTEs and
  runs 2x faster. Fine.
- **TPC-DS q08, +1 GROUP BY.** The set `final_zips` is now projected
  directly. Same work.
- **thelook q22 (+119).** Aggregate-then-join (handoff item 7): the users
  domain LEFT joins a pre-aggregated stream. Same rows, less padding work.
- **thelook q08 (+50, new GROUP BY).** The select projects `id`, but the
  extension rows (users and products with no line) have NULL `id`, so
  `rows_unique_at_outputs` does not trust it as row identity and collapses
  duplicate padded rows. This is deliberate under the region semantics.

## 2. Fixed in this audit

- **A semijoin RHS regrouped an already-unique parent** (TPC-DS q16, q94, q95:
  one redundant GROUP BY each). `filter._rows_unique_at_set` read
  `parent.grain`, which is unset until resolve, so it always answered "not
  unique". It now reads the resolved grain. Only those three corpus plans
  moved, each one smaller.
- **`coalesce_duplicate_joins` dropped a padding guard.** When two copies of
  one join merged (`CTE.__add__`), the key pairs and modifiers were combined
  but `guard` was not. A guarded copy merged into an unguarded one lost its
  guard (`_absorb_join`). No reachable case is known; this closes the hole
  (`tests/test_coalesce_duplicate_cte_joins.py`).
- **`padding_sources` moved to `null_provenance.py`.** It was a private
  function in `join_resolution` read only by the optimizer.
- **Mechanical cleanups:** listed in section 5.

## 3. FIXED: a padded key with no witness (pre-existing, main too)

`_padding_guard` skipped the guard (a silent `continue`) when neither side
had a column NULL exactly where it has no row. Model:
`tests/engine/test_padded_null_pairing.py::SECOND_OPTIONAL_KEY` (a second
`?` key, `channel`, on the fact), now in `region_battery.py` as `channels`.
Bucket `z` (no orders) got the NULL channel's `fee = 50`. Fixed in three
parts (02a4f07c2, 09a8f95d9):

- **Host term only when needed.** The guard is `padded present or host
  absent`. Presence patterns over the join order (`_presence_after`) show
  whether any row lacks both; when none does, the host term is dropped, so a
  `?`-keyed host no longer blocks the guard.
- **Presence marker.** A padded side with no solid key projects a constant
  (`QueryDatasource.presence_marker`, `_virt_row_present_<hash>`), rendered by
  the CTE only, so planning never sees it. The inliner keeps such a CTE, and
  the renderer raises if the marker would render as anything but a column.
- **Pair inside the padding merge.** `_pair_inside_padding_streams` now
  reaches a stream that is a grouping or filtered projection over the
  padding merge (`_padding_path`). When the merge already reads the lookup's
  columns, they are carried up, but only through groups keyed on every
  region's own span: `count(customer_id) by bucket` unites cat's padding with
  value NULLs, and carrying `target` there split that group.

The channels battery moved 42 of 150 queries, every changed row a `z`/`shop`
padding row. TWO_REGIONS and LINE_ITEMS moved 0. Corpus moved 0.

A padded side that is a UNION has no marker yet and still logs and skips.
So does the in-source path (`_padding_witness`), where the padding happened
inside the side's own merge: a marker there would need to be aggregated up
through the grouping.

## 4. Keyspace vs the other nullability machinery

An audit compared the keyspace (`v4_helper/keyspace.py`, `region_domains`,
`region_reads`) against `null_provenance`, `join_resolution`'s `SideFacts`,
`partial_bridging`, `scan_partials` and `grain_utility`. Dead code: none.
Ranked overlaps:

1. **DONE (09a8f95d9).** `_merge_paddings` records each join's paddings once
   (per host for a LEFT, over every joined side for a RIGHT/FULL);
   `_region_padded_sides` derives from it, and the guard tests "some padded
   side present" over the fewest sides whose rows cover the rest. 0 plans
   moved. Probed: multi-host and multi-left joins occur only in the battery
   models, under a WHERE, and their rows were already right. Original
   finding: **Two loops in one merge decided which side an earlier join
   padded for a region.** These are `join_resolution._region_padded_sides` (key-pair
   NULLABLE typing) and `_merge_paddings_from` (guards). Both walk the same
   joins and read `held_spans`, but differ when a join has several hosts:
   - `_merge_paddings_from` records a padding only when exactly one left
     holds a region, and its LEFT branch only for `len(join.keys) == 1`;
   - `_region_padded_sides` unions every host.

   In a multi-host merge the pairing can fall through to `get_modifiers` with
   no `_MergePadding` recorded, so no guard is emitted. This is plausible
   wrong rows, not reproduced. **Proposal:** one pass that emits a
   `_MergePadding` per host, with `region_padded` derived from it. Write a
   two-host test first. It is probably the same fix family as section 3.
2. **MOSTLY DONE (6ed660cdb, 20b284510).** `BuildDatasource.partial_spellings`
   serves both `_partial_spelling` and `keyspace._source_facts`; the heal's
   lookup supply is `ModelFacts.lookup_supply` over `scope_facts` (the
   keyspace's `_carried`). 0 plans moved. Left: `_component_reach` vs
   `_connected`. The heal intersects raw spellings with it, and `_connected`
   answers over canonical entities, so swapping it needs the heal's sets
   canonicalized first. Original finding: **The pin-heal re-derived keyspace
   reach with different rules.** The pairs
   are:
   - `partial_bridging._partial_spelling` and `keyspace._source_facts` (which
     column's `~` licenses extension);
   - `_lookup_supply` and `keyspace._carried` (what an anchor reaches by
     lookup);
   - `_component_reach` and `keyspace._connected`.

   They differ on abstract grains (`_row_identities` FD-minimizes, the heal
   reads raw `ds.grain`), on spelling, and on generated `unnest` domains.
   **Proposal:** `decide_heal` reads `scope_facts(scope, environment)`, and
   one `partial_cause(ds, column)` helper serves both. Gate it on a corpus
   A/B and the pin-heal tests (`_ANCHORED`, the co-partial siblings).
3. **MEASURED.** Over the corpus, `_pads_beside` is decided by
   `span_padding` alone in exactly one call (TPC-DS q81,
   `cs.return_customer.sk` padded for `cs.item.sk`/`cs.order_number`); in the
   three batteries `held_spans` alone decides every case (137 held-only, 0
   padding-only). So the observed-padding half is still load-bearing, for
   q81. Original finding: **"This side holds region R" has two
   representations.** One is
   `region_spans`/`held_spans` (the contract). The other is
   `SideFacts.span_padding`, a join-tree walk via
   `null_provenance.span_padded_addresses`. `_pads_beside` ORs them. The
   keyspace could answer the per-key half (`not defined_on(key, R)`), but
   observed padding also covers owners with no domain group (ROW_STREAM,
   BOUNDARY, RELATION, PADDED) and completions. It also depends on the
   spellings item. The measurement above was the proposed next step;
   retiring `span_padding` means explaining q81 first.
4. **DONE (126ed0989).** Chaining moved no plan, so the two walks are one:
   `span_padded_addresses(source, spans)`. Original finding:
   **`extension_padded_addresses` vs `span_padded_addresses`.** They are
   already one walk (`_padded_addresses`); only the span walk passes
   `chain=True` and follows lookups chained off a padded key. The open
   question is about meaning, not duplication. Under an extent-free span, a
   chained lookup's padding (a customer's address off a padded customer) stays
   nullable in `get_node_joins`. Check whether the extension walk should chain
   too; if no plan moves, it can.
5. **DONE (126ed0989).** `SpanScope.extendable` names it and documents the
   three readings. Original finding: **`MergeNode` computed "spans this merge
   may extend" three ways**
   (`licensed_outputs`, `demanded_domains`, `coalesced`). Only
   `demanded_domains` subtracts `unextended`. Name the difference with a
   `SpanScope` property, or comment it.

Checked and NOT duplicates: `value_null_spans` vs `value_nullables` (a
statement fact vs a side fact); the three ROLLUP readings (they run at
different stages); `binding_is_complete`, `scan_partial_addresses` and
`complete_key_domain` (they compose); `Region.filtered` vs `_PartnerFacts`;
`null_on_padding`, which is already the single owner.

## 5. DRY / simplification

Applied (ef3865cbe..ed9350baf; mechanical, zero corpus plans moved):

- unused parameters dropped (`_split_strands_condition_scan`,
  `_d1_calc_subgraph`, `_final_gate_rowset_base_keys`,
  `_materialize_group_graph`, `_push_having_into_group_parent`);
- defaults every caller overrides removed;
- `keys_of`, `_row_parents`, `_readers` and `_visible_addresses` reused where
  they had been re-implemented;
- checks the types already guarantee removed, including `isinstance` filters
  over `concept_arguments`;
- `raise_if_disconnected_for` computes its grain-only edges and bound
  addresses once instead of three times;
- `optimizations.utils.output_addresses` is shared, and the join-upgrade
  dependency tuple is built once;
- `plan_trace_diff.py` moved to `local_scripts/plan_debugger/`.

Checked and left alone:

- A `_same_scope` helper: only one of the 11 `_scope_and_phase` calls
  compares two labels.
- A `domain_regions` helper: two of the four `region_of` sites filter out
  regions that don't resolve and two assert that they do, so one helper
  would change behaviour.
- `filtered_aggregate`'s wrapper check: it narrows the type.

Second pass (this session; each A/B'd on the corpus and the three region
batteries, 0 plans moved):

- **Atom identity**: both sites match by text (1395c1256).
- **"Has existence args"**: `_group_filter_has_existence` ignores a literal
  IN-list like the rest (126ed0989).
- **Pair-can-match-NULLs**: `execute.pair_matches_nulls`/`pair_modifiers`
  back join_upgrade, strip_redundant_not_null, reuse_parent_lookup and the
  renderer (126ed0989).
- **`row_parents`** lives in group_graph only; the `pred in attrs` filter
  never removed a parent (probed) (126ed0989).
- **Lineage walks**: `edges.lineage_predecessors`/`lineage_successors`, and
  `utility.walk_lineage` for the four BuildConcept closures (6f7bdbd1e).
- **Same names**: the two `_members_of` are `_own_members` and
  `_scope_members`. "solid" means the same thing at both sites (a group that
  sees no region rows), so it stays.
- **`{canonical, *members}`**: `BuildEnvironment.scoped_join_relations()` and
  `all_scoped_join_group_members()` (1395c1256). Two sites keep the loop:
  one needs the canonical, one the sorted order.
- **Long functions** (lines before -> after):
  - `build_strategy_node`'s group loop is `_build_group`, entered under
    `under_span_scope` (000c4d062);
  - `_assemble_final_node` 691 -> 416 (4372233b4);
  - `plan_condition_placements` 635 -> 176 (`_place_atom` keeps ~375)
    (7abc62e39);
  - `MergeNode._resolve` loses `_existence_only` and `_group_decision`
    (35f925449);
  - `_compute_concept_sets` 477 -> ~290 (f342743b9).

  Left: `_split_root_dimension_clusters` (182 lines, no clear seam) and
  `_place_atom`, which could split by placement reason.
- **`plan_trace`**: payloads live in `plan_trace_model`, loaded only while
  recording; import 25-29ms -> 0.6ms (087df3590).

Do NOT "simplify":

- `group_rules.merge_terminal_siblings`'s `output_addresses`: empty grains
  would start merging.
- `group_graph._regraft_candidate(environment=None)`: one caller relies on the
  exact-grain fallback.

## 6. Architectural items carried from `handoff_grain_pin_followups.md`

Still open, and still the largest structural debts. These are design work,
not refactors:

- **One `AddressClass` for a value's four spellings.** About 40 sites. The
  three maps it would replace differ in scope on purpose: environment-wide
  smallest pseudonym, request-scoped `_virt_*` plus graph pseudonyms, and
  per-merge skipping hidden columns. Design the scopes first. The canonical
  spelling being the smallest pseudonym (item 13) is the same root cause.
  Nothing asserts that a canonical-keyed map is read by a canonical.
- **`RowsetDefinition`.** It needs hooks at three parse stages, and the
  prefix scheme is already one place (`SemanticState.mangle_rowset_alias`).
  Low value.
- **Anchor heuristics** (`_always_beside`, `_held_beside`,
  `_co_held_only_beside`, `_may_pad`). These are build-time approximations
  of what `build_keyspace` knows: an over-pin costs plan size, an under-pin
  would cost rows. Moving the decision after `build_keyspace` makes it exact.
  No bug traced.
- **The filter-population claim is computed twice** (`MergeNode._join_proofs`
  and the builder's `applied_atoms`). Proposal: pass the group graph's
  constraint edges down as `JoinProofs.feeder_of`, retiring
  `_reads_partner`'s identifier walk. Not done: it threads group-graph edges
  through MergeNode to replace a five-line walk.
