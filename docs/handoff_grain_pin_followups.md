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

Found by the two-region probe (2026-10-08). The model is
`tests/engine/test_padded_null_pairing.py::TWO_REGIONS`: customers and
buckets each have members no order has, and the bucket key is `?` and bound on
a second table. A 210-query battery was run against main, against the commit
before item 7 (ff0781487) and against 02e809880, and every difference was
triaged by hand. All the fixes moved zero corpus plans.

- **Padding an earlier join of the same merge added paired with a value-NULL
  group** (main too). `customers LEFT orders`, then `orders.bucket <=>
  targets.bucket`, gave cat the NULL bucket's target. `_padding_guard` only
  knew padding inside an input. Rule: a join is guarded against the padding
  the joins before it added (`_padding_guards`, `_MergePadding`). The guard is
  the rows where the host has a row and the padded side has none:
  `orders.customer_id is not null or customers.customer_id is null`. A later
  side the guard kept apart joins the padding's `absent` set, so a coalesced
  key over both stays guarded. The guard is now structured
  (`BaseJoin.guard`/`Join.guard`, a CNF of side-bound NULL tests) and renders
  each witness off its own CTE. That retired `BaseJoin.condition` and the old
  "a witness no other side emits" restriction.
- **The guard's "the other side holds the region" exemption was dead.**
  `held_spans` holds graph nodes (`c~...`) and `span_padding` held bare span
  names. `span_padding` is now spelled the way `held_spans` is.
- **Two islands of FINAL contributors cross-joined ON 1=1.** One island was
  a customer aggregate beside the customer's name, the other a bucket
  aggregate beside its region domain. `_bridge_unpaired_parents` only rescued
  a lone contributor; it now bridges connected components
  (`_pairing_islands`). This was a regression of 3a4ea4cfa.
- **A count reading one region and padded onto another's rows read NULL,
  not 0.** `_zero_filled_counts` zero-fills any parent that misses a region
  another parent reads.
- **Item 7 regressions.** `select bucket, sum(amount) where name is null`
  raised "could only be placed after the aggregates", and with `or name =
  'ann'` the atom also decides which orders are summed. Rule: a WHERE that
  reads a value absent on a region (`Region.filtered`, decided in
  `build_keyspace`) feeds every region an aggregate's grain is carried on
  (`region_reads.filtered_beside`), so the WHERE is tested on one input that
  holds all of them. Feeding only the region the WHERE read an absent value
  of was tried first. It left the other region's domain unfiltered at FINAL
  (`where bucket is null` returned buckets a, b and z), and `where name is
  null` failed to render. An aggregate whose grain no filtered region carries
  keeps item 7's solid rows (`sum(amount) by customer_id where name is null`
  returns only bucket z, as it did after item 7 and unlike main).
- **A customer whose every order the WHERE rejected came back as a region
  row.** Main got this right; 925ff17bc broke it. The customers domain was
  FULL joined on the filtered aggregate's key alone: the orders stream was
  dropped as a redundant provider while the join was still LEFT, and
  `ensure_content_preservation` widened it to FULL afterwards. Rule:
  providers are restored once the join types are final
  (`restore_full_join_providers`).
- **An aggregate holding both regions' rows met cat before she existed.**
  `count(customer_id) by bucket` gave her 0 (main: 2); broken since
  d3cf6906c. Rule: among pivots on a held span, the one whose sides hold no
  other region goes first.

Still open from it:

- **No witness, no guard** (strict xfail
  `test_where_over_the_value_null_group_aggregate_keeps_padding_apart`). In
  `select customer_id, bucket, sum(target) by bucket where coalesce(sum(target)
  by bucket, 0) = 0`, the FINAL reads a stream (`customer_id`, `bucket`, the
  WHERE value) with no column that is NULL exactly on cat's padded row. The
  inner merge would have to carry one out. Main returns no rows.
- **Owner question: does a WHERE-only read add a region?** The convention
  the tests hold is that a WHERE filters and never adds a row: `select
  status, count(order_id) where name = 'cat'` is `[]`. Yet `select
  customer_id, sum(amount) where bucket = 'z' or name = 'cat'` returns the
  bucket region's `z` row, on main too.
- `select customer_id, count(customer_id) by bucket as cb where name = 'ann'`
  returns `(1, 1)` twice. This is the select's grain (customer, bucket)
  projected, and the same shape without the WHERE is no different. Main
  dedups it.

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

9. **A stored-only value pinned to another keyspace** - CLOSED. The
   `NoDatasourceException` now says the value is stored only at its own grain,
   that the select evaluates it on its row (a grain pin), and what that reads
   (`select_node._stored_pinned_readers`).
10. **IS [NOT] NULL is not NULL-absorbing**, by decision: it feeds presence probes
    and null-rejection proofs. A pinned expression inlines its named reads, so a
    CASE over `undelivered` sees `delivery_date is null` on the select's row, while
    `undelivered` itself stays NULL there. Revisit if the registry grows.
11. **Address vs canonical audit** - CLOSED: the four remaining sites never
    see a pinned value. A pinned concept and the column persisting it share an
    address. Two planner checks compared by address and were fixed earlier
    (`predicate_pushdown._parent_holds_the_same_concepts`, now also on the
    union-branch path, and `group_graph._scan_columns`). A detector was
    instrumented on the other four: `select_node_v2.scan_stamps` (`stored`),
    `join_resolution.complete_key_domain` and `merge_partial_addresses`, and
    `source_scoring.membership_complete_grain_keys`. It logged any address
    carrying two canonicals among the concepts each one compares. It ran over
    the pin and twin suites (555 tests) and the corpus. The control at
    `_scan_columns` fired 149 times; the four sites never fired on a pin. A
    pinned WHERE never reaches a scan's condition, because it is tested on the
    select's row. The corpus showed two harmless doubles: a metric column
    (`revenue` at the pair grain, a different `sum` canonical than the
    request's) at `scan_stamps`, which only the BASIC clause would read, and a
    multiselect align key (`report_date`) in two spellings of one value at
    `merge_partial_addresses`.
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
  Still the biggest item, about 40 call sites, and NOT a mechanical one: the
  three maps a shared class would replace differ in scope by design.
  `keyspace._canonical_addresses` is environment-wide, the smallest pseudonym.
  `network_build._equivalence_map` is request-scoped, folds in `_virt_*`
  spellings and graph pseudonym pairs, and keeps presence probes apart.
  `join_resolution.build_canonical_address_map` is per merge and skips hidden
  columns. A per-environment `AddressClass` needs those scopes designed
  first. The cost of crossing spellings is real: the dead `held_spans &
  span_padding` exemption (2026-10-08) was one more such crossing.
- **A guarded FULL join cannot lower on MySQL** - refused honestly now.
  The guard is `Join.guard`, apart from `Join.condition`, and `_validate`
  refuses it with the reason, a padded NULL kept apart from a value NULL that
  a spine would fold together
  (`test_guarded_full_join_is_refused_not_dropped`). Before this, a
  structured guard would have been silently dropped. Every guarded FULL
  found so far (the two-region battery) is refused earlier anyway: the
  padding host is the FROM base and joins on another key. Lowering one needs
  its own spine rows. Each arm emits `(k, g)`, with `g` the participant's
  guard (TRUE without one); a participant joins on `k <=> spine.k AND g =
  spine.g`, and at most one participant may carry a guard. That is renderer
  work (a computed arm column) with no reachable case to test it.
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
  (`UpgradeOuterFromKeySetEquivalence`). `grain_utility._partner_facts`
  reads raw `nullable_concepts` on purpose: a partner key that is NULL by
  padding is a member the filtered side lacks just as a value NULL is.
  Narrowing it to value NULLs moved no corpus plan and failed no engine test,
  so the docstring now carries the reason. `get_modifiers` still cannot see a
  key whose NULLs are both value and padding; the guard handles that on the
  join side.
- **`BaseJoin` grew a `condition` beside `Join.condition`** - replaced by
  the structured `guard`. The passes that rebuild a join from an existing one
  (`join_hoist`, `union_dim_pushdown`, `semi_join_pushdown`,
  `reuse_parent_lookup`) skip a guarded join (`Join.has_predicate`). Every
  pass that repoints CTEs walks `Join.cte_bindings`, which covers the key
  pairs and the guard terms alike.
- **Condition placement has two exemption registries for the same question.**
  `_check_final_atoms_precede_aggregates` (an atom only at FINAL must be decided
  at every output aggregate's grain) and `_uncovered_grouping_placements` (copy a
  row atom to aggregates its host does not feed) both encode "a WHERE precedes
  its aggregates", with different reason allow-lists
  (`_COPIED_TO_UNCOVERED_GROUPINGS`) and different skips (the check skips atoms
  over aggregates; the copy skips placements naming FINAL). Item 2's `s` fell
  between them: pre-condition, never checked, right only through group
  atomicity. One predicate `atom_decided_for(aggregate)` used by both would
  close the gap and let item 7 be reasoned about in the same terms. Tried on
  2026-10-08: letting the copy take FINAL span-domain placements cleared the
  refusal, but the rows came out wrong (buckets a and b survived `name is
  null`). The FINAL's rows came from a domain that does not carry `name`, so
  the FINAL copy of the atom had nothing to test. Feeding the aggregate the
  region fixed it instead. A unified predicate would have to know which FINAL
  contributor carries each read.
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
  (`strategy_builder`) the same answer without the prefix test. Not done: the
  outputs are pending when `add_rowset` runs (the semantic state commits them
  later), so the definition would need hooks at three parse stages. The
  prefix is already a single scheme (`SemanticState.mangle_rowset_alias`).
- **Test scaffolding** - DONE. `tests.helpers.rows.customer_twins(extra)`
  (and `twins(derived, materialized)`) build the twin. It deliberately
  returns fresh executors, because a rowset statement redefines the
  environment it runs in; each module scopes its own.
