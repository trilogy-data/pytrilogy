# Handoff: grain pin follow-ups

Open items left by the grain pin (`docs/grain_pin.md`, `trilogy/core/grain_pin.py`):
a NULL-absorbing expression read by a select is evaluated on the select's row.

## Wrong rows: all closed

The five strict xfails this handoff opened with are fixed and the tests now pass
as plain cases; each fix is a rule, named here so a regression has a home.

1. **Null-accepting WHERE over a pinned value on a `union join` axis.** The
   filter feeder was built from the partner's own rows but its join back was
   read as the relation's pairing (both sides already held both coalesced
   members) and the partner's nullable keys vetoed the population claim. Rules:
   a null-accepting atom whose every read is defined on every region is as exact
   as a null-rejecting one (`strategy_builder._reads_only_values_defined_everywhere`);
   a feeder filtering the partner's own rows holds every member the partner has
   (`grain_utility._reads_partner`); two sides that each hold both members of a
   coalescing relation are not that relation (`_joins_a_coalescing_relation`).
2. **Pinned WHERE over an aggregate by a no-ELSE CASE paired a padded NULL
   with the value-NULL group.** It was a bug: one address took two values in one
   statement. `s` restates the WHERE's own aggregate (whole groups pass or fail
   together), so it is computed once over the solid rows and joined to a stream
   the customer region already padded, null-safely on `pstatus`, whose NULLs
   there are MIXED: a value for order 100, padding for cat. Rule: a key NULL by
   absence never pairs with a value-NULL group. The join keeps the null-safe
   pairing and excludes the padded rows with a witness NULL exactly there
   (`join_resolution._padding_guard`: `order_id is not null`, a KEY of the
   padded side, never a property, which a `~?` NULL member's row NULLs too).
   `BaseJoin.condition` carries it to the CTE `Join`. Without a key witness the
   pairing stays as before; no case needs that today.
3. **A redefined rowset changed a reader's rows.** Two bugs. `rowset_witness`
   looked the body's keyspace up by the handle content's canonical spelling, the
   smallest pseudonym of the value: any earlier rowset whose hidden alias sorted
   first (`local._a_st` for `local._t_st`) missed and fell back to the un-pinned
   alias's keys (no redefinition needed; `rowset a <- ...` then `rowset t ...`
   dropped cat's row). The body keys what it declared. And `add_rowset` now
   retires a prior definition's outputs, their pseudonym mirrors and the body's
   hidden aliases (`Environment.retire_rowset`): `select s.a` read the old rows.
4. **A bound `is_returned` column read NULL beside the reason region.** The
   `returns` scan, partial on item and ticket, computed `ret_qty is not null`
   inline and the LEFT join padded the lines it lacks. Rule: a scan's inline
   derivation keyed on an entity it binds `~` is as partial as that key, in the
   network binding (`network_build._candidate`) and on the scan node
   (`select_node_v2.scan_stamps`), so a complete column elsewhere wins the
   merge's source map. The concept-graph edge stays: the merged root scan (sales
   beside returns) still computes it inline over complete rows (TPC-DS q64).
5. **The `by *` gate emitted a NULL row when the WHERE emptied every row.** The
   WHERE's proof narrowed the keyless FULL to RIGHT and the keyless pass only
   revisited FULLs. Rule: a keyless outer join where either side provably holds
   one row is INNER (`join_resolution.narrow_keyless_joins`).

Found by the audit that followed (both wrong on main too):

- **A WHERE on a derivation over a partition union was dropped.** A filtered
  ROOT merge was folded into its unfiltered sibling projection as a passthrough.
  Rule: a filtered parent folds only into a sibling whose own conditions or
  preexisting conditions hold every atom (`strategy_builder._rows_passed`).
- **A WHERE over `coalesce(ret, 0)` on a `~` property tested NULL for 0.** The
  scan computed the coalesce over its own rows and the filter ran above the
  LEFT join. Rule: a NULL-absorbing inline derivation a scan binds only
  partially is no search terminal; it is computed over the joined rows
  (`network_build._searched_terminals`, `absorbs_null`), and the unfiltered
  retry declines a WHERE that reads one (`_filters_a_partial_derivation`). A
  derivation that stays NULL on NULL inputs (`is_returned`) keeps scan hosting.

## Plan size

6. **A pinned row value built its own copy of the select's rows** - CLOSED.
   `test_forked_full_column_set` is back to 7 joins. The pinned `order_status`
   group had read root, both region domains and the order dimension itself and
   was INNER-joined back to the item-grain aggregate holding the same rows.
   The 7-join plan had only ever arisen because `item_id` was a pin anchor,
   so the BASIC read the aggregate as `item_id`'s producer; once the anchors
   narrowed to `product_id` the edge vanished. Rule: `_regraft_candidate`'s
   same-grain spine test compares rows, not key sets (`_same_rows`: grains
   that determine each other, an aggregate by (item, order) beside a BASIC
   at the item), so the BASIC rides the sibling's stream and the FINAL
   computes the CASE on it.
7. **An aggregate under a pinned reader took the region's rows** - CLOSED.
   `min(amount) by user_id` was fed the user region once its reader was no
   longer solid. Rule (`region_reads.evaluated_over_region`): grouped by the
   span key alone, an aggregate reads the solid rows; it is NULL on the
   extension row as it is where the FINAL pads the group it never had, and
   only one answering a padded row differently from no row (`count`,
   `zero_on_empty`) or a ROLLUP pass takes the region. Grouped by a property
   of the span (`by state`) it keeps the region: the lookup it needs joins
   the region anyway, so the feed is free. thelook q06/q07/q22 and tpc_h
   adhoc04 shrank; nothing grew. The one shape it moved was a second bug:
   two facts summed by `~cust_id` beside `custs` met FULL before the host
   arrived, because the join order's `multi_partial` bump ranked the partial
   feeders above the complete host. Rule (`_score_join_candidate`): a side
   that hosts the region seeds the tree, so its feeders hang LEFT off it.
8. **q84 +208 chars** - CLOSED, and not where this handoff had it. The
   partial stamp was on the presence probe (`coalesce(sr_cdemo_sk)`, a BASIC
   keyed on the subset-joined `~` key), not on `is_returned`; the plan was
   identical to the baseline's up to the optimizer. `UpgradeJoinOnGuards`
   then could not read `probe is not null` as proof the scan matched:
   `_blocked_partials` compared the consumer's `source_map` tokens (the
   operand's CTE name) with `_source_datasources(operand)` (the operand's
   own parents' tokens), so a partial value bound solely to a CTE operand
   was always "blocked", the join stayed LEFT and the `is_returned` atom
   stayed above it. Rule: an operand's tokens are its CTE name and the
   physical tables it renders from (`join_upgrade._source_datasources`).
   The "+124 before" was the same block under the earlier pin. q84 is 1876
   chars, its branch baseline.

## Design edges

9. **A stored-only value pinned to another keyspace** raises the generic
   `NoDatasourceException` for its unbound input
   (`test_stored_only_value_cannot_be_pinned_to_another_keyspace`). A message
   naming the keyspace mismatch ("stored only at the order keyspace") would help.
10. **IS [NOT] NULL is not NULL-absorbing**, by decision: it feeds presence probes
    and null-rejection proofs. A pinned expression inlines its named reads, so a
    CASE over `undelivered` sees `delivery_date is null` on the select's row, while
    `undelivered` itself stays NULL there. Revisit if the registry grows.
11. **Address vs canonical audit.** A pinned concept and the column persisting it
    share an address. Two planner checks compared by address and were fixed
    (`predicate_pushdown._parent_holds_the_same_concepts`, now also on the
    union-branch path, and `group_graph._scan_columns`). Still compared by
    address, unproven either way: `select_node_v2.scan_stamps` (`stored`),
    `join_resolution.complete_key_domain` / `merge_partial_addresses` on leaf
    scans, `source_scoring.membership_complete_grain_keys`. The bound twins in
    `tests/helpers/models.py` are the oracle that catches them.
12. **Anchor heuristics are build-time approximations** of what the keyspace
    knows: `_always_beside`, `_held_beside`, `_co_held_only_beside`, `_may_pad`.
    Each avoids a pin that cannot change a value; an over-pin costs plan size,
    an under-pin would cost rows. Moving the decision after `build_keyspace`
    would make it exact.
13. **Canonical spelling is the smallest pseudonym** (`keyspace._canonical_addresses`),
    so which address names a value's class depends on what else the session
    declared. Every map keyed by a canonical must be read by a canonical and
    every map keyed by a declared address by the declared one; item 3 was one
    place that crossed them. Nothing today asserts the discipline.

## DRYness and architectural concerns met on the way

Not bugs; places where the same fact is computed twice, or a decision lives
far from the information that decides it. Each cost an hour of this handoff.

- **Four spellings of one address.** A value is named by its authored address,
  its canonical (`_virt_*`) address, the smallest of its pseudonym class
  (`keyspace._canonical_addresses`) and, inside a rowset body, a mangled alias
  (`local._s_c`). Maps are keyed by whichever their author had to hand:
  `keys_by_address` by declared, `network_build.emitted` by the graph's
  canonical, `stored` by the column's real address (so a stored column shows
  `stored=False` under its canonical: `is_returned` on `sales` in
  `tests/engine/test_unmodelled_regions.py`), `_FACTS_CACHE.canonical` by the
  class minimum. Item 3 was one crossing; `network_build._keyed_on` was
  written to bridge another (retired: `scan_partial_addresses` matches both
  spellings). One `AddressClass` resolved once per build environment, with
  the four spellings as fields, would retire the remaining ad-hoc bridges
  (`equivalence`, `canonical.get(a, a)`, the `{address, canonical_address}`
  pairs `scan_partial_addresses` and `scan_stamps` build). `hidden_concepts`
  being `list[BuildConcept]` on `BuildDatasource` but `set[str]` on
  `QueryDatasource` is a typing wart only: `BuildConcept == str` compares
  addresses, so `join_resolution`'s string membership tests hold on both.
  Still the biggest item, about 40 call sites; the two refactors below are
  done and narrow it.
- **A guarded FULL join cannot lower on MySQL.** `_padding_guard` does not
  look at the join type and `full_join_lowering._validate` refuses any FULL
  with an ON predicate, telling the user to move a predicate they never wrote.
  No test reaches it yet. The key spine cannot carry the guard as written: a
  padded row the guard excludes from pairing must still come out unmatched,
  and `LEFT JOIN padded ON k <=> spine.k AND witness is not null` drops it.
  It would need its own spine rows, so the fix is a lowering case, not a
  guard change.
- **Partiality is stamped in three places that must agree** - DONE.
  `scan_partials.scan_partial_addresses` answers "what does this scan bind,
  and how fully" for the network candidate (`network_build._candidate`), the
  scan node's stamp (`select_node_v2.scan_stamps`) and the union node's
  (`create_union_datasource_candidate`). Where the three disagreed, the
  function decides: a hosted aggregate is no inline derivation (no other
  column binds it, so its keys' partiality is the only one it has); an
  exemption (a promoted span, a membership proof) applies before the inline
  step, so a derivation keyed on an exempt key is complete; the union stamp
  has the inline rule; both spellings of an address match. Zero corpus plans
  moved; `tests/core/processing/test_scan_partials.py` pins each clause.
- **Null provenance is re-derived per consumer** - DONE, as a memo. The
  walks (`nulls_are_values`, `extent_null_addresses`, `guest_padded_addresses`,
  `extension_padded_addresses`, `span_padded_addresses`,
  `rollup_padded_addresses`) live in `null_provenance.py` behind
  `ProvenanceMemo`, whose `of(source)` view answers each question once per
  source; `get_node_joins` builds `SideFacts`, the span-padding matrix, the
  padding witness and `get_modifiers` off one memo, and `plan_scope`, opened
  by `_process_query` around discovery and resolution, shares it across every
  merge of a plan. The scope ends before the optimizer, which rewrites joins
  on the same `QueryDatasource`s and walks uncached
  (`UpgradeOuterFromKeySetEquivalence`). Still open from the original note:
  `grain_utility._partner_facts` reads raw `nullable_concepts`, and
  `get_modifiers` still cannot see a key whose NULLs are both value and
  padding (item 2's guard handles it join-side).
- **`BaseJoin` grew a `condition` beside `Join.condition`.** Before this branch
  a plan-level join had no predicate and only the optimizer
  (`filtered_aggregate`) added one at the CTE level. Two places now build joins
  from an existing one (`join_hoist`, `union_dim_pushdown`); the first carries
  the condition, the second rebuilds from `d.key_pairs` and would drop it if a
  guarded join ever reached it (it cannot today: the guard needs a padded side,
  the pushdown a leaf dim scan). A single `BaseJoin.derive(...)` constructor
  used by every rebuild site would make that structural.
- **Condition placement has two exemption registries for the same question.**
  `_check_final_atoms_precede_aggregates` (an atom only at FINAL must be decided
  at every output aggregate's grain) and `_uncovered_grouping_placements` (copy a
  row atom to aggregates its host does not feed) both encode "a WHERE precedes
  its aggregates", with different reason allow-lists
  (`_COPIED_TO_UNCOVERED_GROUPINGS`) and different skips (the check skips atoms
  over aggregates; the copy skips placements naming FINAL). Item 2's `s` fell
  between them: pre-condition, never checked, right only through group
  atomicity. One predicate `atom_decided_for(aggregate)` used by both would
  close the gap and let item 7 be reasoned about in the same terms.
- **The filter-population claim is computed twice.** `MergeNode._join_proofs`
  decides `filtered_ids` from `preexisting_conditions`, and
  `strategy_builder` decides which atoms become `preexisting` for a pre-merge
  (`applied_atoms`, now with `_reads_only_values_defined_everywhere`). Item 1
  needed both: the second to let the atom through, the first to accept it.
  `_is_filter_population` then re-derives from the resolved sources what the
  builder knew from the group graph (that the feeder reads the partner). Passing
  the group graph's constraint edges down as `JoinProofs.feeder_of` would
  replace `_reads_partner`'s identifier walk with the fact itself.
- **Rowset redefinition had no owner.** `Environment.add_rowset` only recorded
  the lineage; the outputs were added by the semantic state and the hidden
  aliases by the select rule, so no one object knew what a rowset consisted of
  and `retire_rowset` reconstructs it by prefix (`_{name}_`) and lineage walk.
  A `RowsetDefinition(name, outputs, aliases, lineage)` registered once would
  make retire a deletion and give `_mangled_rowset_content_addresses`
  (`strategy_builder`) the same answer without the prefix test.
- **Test scaffolding.** `tests/helpers/rows.py` and the twin models are good; the
  `--runxfail` + strict-xfail idiom is the right contract for open bugs, but
  five modules each spelled their own `executor_for(model + extras)` fixture.
  A shared `twins(extra: str)` fixture factory would cut the next handoff's
  test diff in half.
