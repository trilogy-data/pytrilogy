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

## 3. Open: wrong rows (pre-existing, main too)

**A padded key with no witness pairs with a value-NULL group.** Found by this
audit. `_padding_guard` needs a column that is NULL exactly where a side has
no row: a KEY the side never emits NULL for. When neither side has one, the
guard is skipped with `continue`.

- The padded side can lack one: a stream grouped to `(bucket, channel)` has
  dropped `order_id`.
- The host can lack one: a `?`-keyed dimension such as `targets(bucket:
  ?bucket)`, whose only key has a NULL member.

Model: TWO_REGIONS plus a second `?` key on orders.

```
key channel string; property channel.fee int;
root datasource channels (channel: ?channel, fee: fee) grain (channel)
  query '''select null as channel, 50 as fee union all select 'web', 60 union all select 'shop', 70''';
orders (order_id, customer_id: ~customer_id, amount, bucket: ~?bucket, channel: ~?channel)
  (100, 1, 10, NULL, 'web'), (101, 1, 20, 'a', NULL), (102, 2, 30, 'b', 'web')
```

Each of these gives bucket `z` (which has no orders) the NULL channel's
`fee = 50` where NULL is correct:

- `select bucket, channel, fee`
- `select bucket, channel, count(order_id) as n, fee`
- `select bucket, channel, sum(fee) by channel as f`
- `select customer_id, bucket, channel, target, fee`

The customer region in the same model is right, because `customers` has a
solid key.

Why the simple patch does not help: emitting `padded.witness is not null` when
only the host witness is missing fixes the merge inside `count(order_id)`. The
FINAL then joins `channels` again, null-safely, onto the grouped stream, which
has no witness. The real fix is a presence marker:

- when a padded side has no solid key, the merge projects a non-null
  constant for it (or keeps the grain key it grouped away);
- the guard tests that marker.

Add this model to `local_scripts/sql_ab/region_battery.py` when fixing. Neither
battery model has a second `?` key on the fact, so neither can reach this.
Until then the `continue` in `_padding_guard` is a silent skip
([[feedback_silent_planner_skip_is_a_wrong_answer]]). Consider at least logging
it.

## 4. Keyspace vs the other nullability machinery

An audit compared the keyspace (`v4_helper/keyspace.py`, `region_domains`,
`region_reads`) against `null_provenance`, `join_resolution`'s `SideFacts`,
`partial_bridging`, `scan_partials` and `grain_utility`. Dead code: none.
Ranked overlaps:

1. **Two loops in one merge decide which side an earlier join padded for a
   region.** These are `join_resolution._region_padded_sides` (key-pair
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
2. **The pin-heal re-derives keyspace reach with different rules.** The pairs
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
3. **"This side holds region R" has two representations.** One is
   `region_spans`/`held_spans` (the contract). The other is
   `SideFacts.span_padding`, a join-tree walk via
   `null_provenance.span_padded_addresses`. `_pads_beside` ORs them. The
   keyspace could answer the per-key half (`not defined_on(key, R)`), but
   observed padding also covers owners with no domain group (ROW_STREAM,
   BOUNDARY, RELATION, PADDED) and completions. It also depends on the
   spellings item. **Next step:** a debug-only cross-check that measures how
   often the two disagree on the corpus.
4. **`extension_padded_addresses` vs `span_padded_addresses`.** They are
   already one walk (`_padded_addresses`); only the span walk passes
   `chain=True` and follows lookups chained off a padded key. The open
   question is about meaning, not duplication. Under an extent-free span, a
   chained lookup's padding (a customer's address off a padded customer) stays
   nullable in `get_node_joins`. Check whether the extension walk should chain
   too; if no plan moves, it can.
5. **`MergeNode` computes "spans this merge may extend" three ways**
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

Left, because they can change plans and each needs a SQL A/B:

- **Atom identity.** `condition_placement` (~1710) matches atoms by
  `str(atom)`; `region_domains` (~927) matches by `is`.
- **"Has existence args" means three things.** These are
  `projection._has_concept_existence` (a literal IN-list does not count),
  `strategy_builder._group_filter_has_existence` (it does), and
  `any(atom.existence_arguments)` in `condition_placement`, `join_hoist` and
  `union_dim_pushdown`.
- **Pair-can-match-NULLs.** `join_upgrade._pair_can_match_nulls` is the full
  check; `strip_redundant_not_null` inlines it; `reuse_parent_lookup._plain`
  skips the side modifiers.
- **`_row_parents` is defined twice** (`strategy_builder`, `group_graph`; the
  second also requires `pred in attrs`).
- **Lineage walks.** Four BuildConcept-lineage closures in `root_partition`
  and `concept_strategies_v4` (only the last resolves through
  `environment.concepts`). Seven concept-graph edge walks could use
  `lineage_predecessors`/`lineage_successors` helpers in `edges.py`.
- **Same names, different meanings.** `_members_of`
  (`strategy_builder` vs `region_domains`) and "solid"
  (`group_graph._solid*` vs `extent_ownership.solid_groups`).
- **`{canonical, *members}` over `scoped_join_key_groups`.** About 19 sites;
  one `BuildEnvironment` method.
- **Long functions** (lines in function / lines this branch added):
  - `strategy_builder._assemble_final_node` (691/115);
  - `build_strategy_node` (608/266), which also sets
    `environment.span_scope` directly where the rest of the code uses
    `under_span_scope`;
  - `condition_placement.plan_condition_placements` (635/84);
  - `group_graph._compute_concept_sets` (477/152);
  - `root_partition._split_root_dimension_clusters` (182, all new);
  - `merge_node._resolve` (415).

  Each has an obvious extraction or two (the region-domain re-source block,
  the FINAL_SPAN_DOMAIN host branch, `_region_context`).
- **`plan_trace.py` costs 25-29ms at import** (about 45 frozen dataclasses)
  and nothing per query when off. Splitting `active`/`record`/`set_context`
  from the step classes would let those load lazily.

Do NOT "simplify":

- `group_rules.merge_terminal_siblings`'s `output_addresses`: empty grains
  would start merging.
- `group_graph._regraft_candidate(environment=None)`: one caller relies on the
  exact-grain fallback.

## 6. Architectural items carried from `handoff_grain_pin_followups.md`

Unchanged, and still the largest structural debts:

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
  `_reads_partner`'s identifier walk. Not done.
