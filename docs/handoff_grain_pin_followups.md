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

## Plan size

6. **A pinned row value builds its own copy of the select's rows.**
   `test_forked_full_column_set` (`tests/engine/test_duckdb_partial_key_assembly.py`)
   went 6 -> 10 joins, rows right. The pinned `order_status` group reads both
   region domains plus the order dimension and is INNER-joined back null-safely
   on every key, while the main row stream already holds those rows. The fix is
   to evaluate a pinned BASIC on the FINAL merge's rows (the group-fold machinery,
   `_read_parents_in_place`, only folds into aggregates today).
7. **An aggregate under a pinned reader takes the region's rows.**
   `min(amount) by user_id` is fed the user region once its reader is no longer
   solid (`region_domains`: an aggregate grouped by a carried key is evaluated
   over the region). For `min`/`max`/`sum` the region adds only NULL groups the
   FINAL would pad anyway; only a count (`zero_on_empty`) or an inline argument
   taking a value on padding needs them. Broad: every span aggregate goes through
   `evaluated_over_region`.
8. **q84 +208 chars** over this branch's own baseline (still 213 chars under
   main, so no `accepted_growth.toml` entry). Item 4's rule stamps
   `ss.is_returned` partial on the `store_returns` scan, so the WHERE over it is
   no longer pushed into that scan but tested above it beside a presence probe,
   with the scan LEFT-joined. The
   atom null-rejects the derivation's own `~` input (`sr_ticket_number is not
   null`), so inside the scan it is exact: a scan may host an atom over a
   partial inline derivation when the atom rejects the rows the scan lacks.
   The earlier +124 (pinning before the keyspace that proves no padded row
   survives) is folded into this; the keyspace's `emptied_by` would remove both.

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
    (`predicate_pushdown._parent_holds_the_same_concepts`,
    `group_graph._scan_columns`); others likely remain. The bound twins in
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
  class minimum. Item 3 was one crossing; `network_build._keyed_on` had to be
  written to bridge another. One `AddressClass` resolved once per build
  environment, with the four spellings as fields, would retire the ad-hoc
  bridges (`equivalence`, `canonical.get(a, a)`, `_keyed_on`, `hidden_concepts`
  being `list[BuildConcept]` on `BuildDatasource` but `set[str]` on
  `QueryDatasource`, which `join_resolution` compares a string against).
- **Partiality is stamped in three places that must agree.** The datasource's
  `~` columns (`BuildDatasource.partial_concepts`), the network binding
  (`network_build._bindings_for`) and the scan node (`scan_stamps`) each decide
  what a scan provides partially; item 4 needed the same rule in two of them,
  and the third (the concept-graph edge, `env_processor.generate_adhoc_graph`)
  was the wrong place and grew q64. One function answering "what does this scan
  bind, and how fully" for the three callers would make the rule a rule.
- **Null provenance is re-derived per consumer.** `nulls_are_values`,
  `side_nullable`, `_span_padding_matrix`, `extent_null_addresses`,
  `guest_padded_addresses`, `_pairs_region_padding`, `_pads_for_different_members`
  and now `_padding_guard` each walk the parent chain to classify a NULL as
  value, padding, guest or rollup. `SideFacts` collects the results but is built
  inside `get_node_joins`, so the merge node, the grain narrowing pass and the
  optimizer (`UpgradeJoinOnGuards`) re-ask. A per-`QueryDatasource` cached
  `NullProvenance` (address -> kind, spans, witness key) would make item 2's
  guard a lookup and give `get_modifiers` the mixed case it cannot see today.
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
