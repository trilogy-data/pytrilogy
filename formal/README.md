# Formal semantics (Lean 4)

A small, dependency-free Lean 4 model of the Trilogy rules that decide
*which rows* a query returns: condition logic, WHERE / staged `then where` /
HAVING, aggregates over partitions, the "no intrinsic rows" set semantics,
grain, `~` padding with the grain pin, and which datasources may answer a
query. Every theorem is checked; there is no `sorry`.

```bash
cd formal
lake build        # elan installs the pinned toolchain from lean-toolchain
```

A clean build is under a minute. A proof by exhaustion over a bounded domain
(the ones under `set_option maxRecDepth`) is closed with `decide +kernel`:
plain `decide` evaluates the proposition in the elaborator and then again in
the kernel, which doubled the build. It is the same kernel check and adds no
axiom.

## How it is used

The model is the oracle for the fuzzer's `law` family
(`local_scripts/fuzzer/laws.py`). A theorem that two query shapes agree
becomes a case pairing two Trilogy programs. The runner compiles both and
compares rows (`FuzzCase.twin_trilogy`), so no hand-written SQL oracle is
needed, and the random datasets (`--random-datasets`) widen every law for
free. A theorem that two shapes *disagree*, or that two rules conflict, records
a design decision or a bug.

| Module | Rules | Laws exercised |
|---|---|---|
| `Logic.lean` | Kleene `and`/`or`, two-valued `not` | commutativity, De Morgan, double negation, excluded middle, `true` identity |
| `Agg.lean` | filters, `x ? p`, per-key partitions | count split over `not`, inclusion-exclusion, filtered agg = WHERE, grand/rollup totals, `max = -min(-x)` |
| `Staged.lean` | flat WHERE, `then where`, HAVING | row stage folds, staged = inline filter (2 and 3 stages), flat WHERE aggregate = membership, HAVING = outer WHERE |
| `Rows.lean` | distinct tuples, grain | keyed rowset keeps rows, filter pushdown through a rowset, `distinct = group`, FD extension, window = `by`, sibling aggregates; a stream paired with a copy of itself on its rows' identity is itself (`Rows.identity_pair_exact`), and on a coarser key squares each group (`Rows.coarse_pair_squares`) |
| `Domain.lean` | `~` padding, grain pin | materialization invariance, pin vs stored column, pin is projection-invariant, `sum(coalesce)` |
| `Sourcing.lean` | `complete where` arms, partition unions, the cell checker | a partition family unions back to the table (split, point, bool, `is null` arm, NULL-rejecting gate, a redundant copy of an arm), an arm answers under an implying WHERE, summary table = computed aggregate |
| `Rollup.lean` | a summary re-aggregated to a coarser grain under a WHERE; a query's own GROUP BY re-aggregated to its coarser grains | `sum`, `count`, `min` and `max` (so `bool_and`/`bool_or`) roll up exactly under any filter and target their grain determines, the grand total's no-row guard, a re-aggregated partial is itself a partial (so a chain of grains reads the rows once), a finer filter, a fanned-out partial's re-sum, `count_distinct` and `avg` cannot, but `avg` is the guarded quotient of a stored `sum` and `count`, and a stored column answers only an aggregate of its own argument, a filter on a stored group value is a filter on the summary key (`group_value_filter_exact`), also grouped by a dimension's column keyed on the grain (`group_value_filter_joined_exact`), and a filter on a group value at a coarser grain is the re-sum of the summary rows sharing its key, never a row's own partial, at the summary's own grain too (`coarse_group_value_filter_exact`, `coarse_gate_on_partial_misfilters`, `coarse_gate_at_summary_grain`), and so is a gate grouped by a dimension's column, computed by that column and joined back on its whole grain (`dimension_gate_exact`, `dimension_gate_by_its_key_misfilters`, `gate_joined_on_part_of_its_grain_misfilters`), and re-summed at its own key, never the target's (`gate_resummed_at_target_misfilters`), a re-sum beside the rows holding its partials is joined to them on the group key, never on the partial (`resum_lookup_exact`, `partial_is_not_a_key`), and a grain column's distinct values are the summary's (`count_distinct_grain_column_exact`) ; the WHERE filters every scan of the select's rows, a base-row total beside a summary-served aggregate included (`Rollup.where_filters_every_scan`), and a dimension's column filters every scan of its key (`Rollup.dimension_column_filters_every_scan`); a gate is re-summed at its own grain before it filters, never at the select's (`Rollup.gate_partial_resummed_off_its_grain`); a re-sum of stored partials reads no rows finer than them (`Rollup.resum_beside_finer_rows_multiplies`); a total over every row is rolled up from the summary beside any row atom (`Rollup.total_gate_from_summary`); a grain spelled through a rename is the grain it names, never a coarser partial of it (`Rollup.renamed_grain_same_groups`); a summary-rolled root the WHERE filters below its grain shares the scan that applies the WHERE (`Rollup.split_scan_reads_every_row`); groups of aggregates are acyclic only as wholes, so two that read each other through a re-aggregated partial split by depth (`Rollup.merged_groups_read_each_other`, `Rollup.split_groups_acyclic`); an aggregate coarser than the summary's rows is re-summed at its own grain and joined back on it (`Rollup.coarse_resum_joined_on_its_grain`, `Rollup.coarse_read_at_row_grain_misreads`), under a WHERE of columns its keys determine too (`Rollup.group_filter_commutes_with_resum`); a key predicate on the summary keeps exactly the keys a kept row has (`Rollup.key_filter_is_semijoin`), so a filtered row scan beside a summary carrying the same atoms is a passenger (`Rollup.filtered_passenger_multiplies`); an aggregate by a property of the row key is the re-sum of the summary's partials by it (`Rollup.property_resum_exact`), and one mixing a row key in pairs the rows on the property alone (`Rollup.mixed_grain_pairs_on_property`) (checked by `tests/discovery/test_sweep_390k_shapes.py`, `tests/discovery/test_sweep_270k_shapes.py`, `tests/discovery/test_sweep_210k_shapes.py`, `tests/discovery/test_sweep_150k_shapes.py`, `tests/discovery/test_rollup_resolution_laws.py`, `tests/discovery/test_spellings_60k_gates.py`, `tests/discovery/test_total_summary_reads.py`) |
| `Assert.lean` | datasource `assert` / `prior(...)` build checks: the outer join of the two sides, the edge rules, substitution into the constant check | the check is the documented rule on every group of either side, given every name substituted, exact literals and a driver-independent TRUE; a vanished group fails, a NULL key matches by identity (checked by `tests/execution/test_assertion_laws.py`) |
| `Functions.lean` | scalar functions as rendered per dialect: `/`, casts to an integer, `date_diff`, `week`, `day_of_week` | true division (a materialized quotient reads back), a cast is `round`, `date_diff` counts boundaries so it adds along a chain, `week` is ISO and `day_of_week` is 1 on Sunday on every dialect; ClickHouse's `sign(x) * floor(abs(x) + 0.5)` is `round` (`sign_round_is_round`), and its Monday-boundary `week` is not whole spans (`monday_boundaries_is_not_week_diff`) (checked by `tests/core/test_function_laws.py` on DuckDB, SQLite and ClickHouse) |
| `Cost.lean` | which answering datasource is read: a `complete where` arm stored at a grain | a source whose arm implies another's and whose grain is coarser stores no more rows on any data, a filtered table and a complete summary have no order by what they store but do by what they read: under a WHERE implying the table's arm, the coarser source applying it reads no more rows (`Cost.filtered_dominated_le`, `Cost.filtered_summary_strict`), a partial summary answers exactly under an implying WHERE and loses rows under any other, a pin to values in `S` implies `x in S` (checked by `tests/discovery/test_cheapest_source_laws.py`) |
| `Stack.lean` | a partition family stacked into one source: value-set arms, summaries stacked at their grain | `x in S` is the disjunction of `x = a` over `S`, so the cell checker covers families mixing `=` and `in` arms, a per-value arm choice can count a value twice, stacked summaries are exact and repeat no grain value when the grain determines each arm, and repeat one when it does not (checked by `tests/discovery/test_partition_family_laws.py`) |
| `Freshness.lean` | watermarks, the stale verdict, the incremental refresh | verdict = refresh has a row to write, incremental = rebuild under strict arrivals (checked by `tests/execution/state/test_freshness_laws.py`, not the query fuzzer) |
| `StateMerge.lean` | partition deltas folded into a state snapshot | disjoint overlays commute, replay is a no-op, newest claim wins as a semilattice join, the per-slice count adjustment (checked by `tests/execution/state/test_merge_laws.py`) |
| `NullReject.lean` | the null-rejection walk, membership, outer-join upgrade, nullability inference | a proved column is non-null on every kept row, a proof on a column read off the padded side turns a left join inner, a derived column is judged by its lineage, strict and total functions; a zero divisor is NULL, so `/` is strict and total only by a nonzero literal (`guarded_div_strict`, `literal_divisor_total`), and a persisted copy that declares nullable every column its write may hold NULL in reads back exactly under `is not null` (`inferred_copy_sound`); a gate holding on NULL, read off the side it already filtered and outer-joined, passes the padding (`gated_side_padding_passes_gate`) (checked by `tests/optimization/test_null_rejection_laws.py`, `tests/discovery/test_sweep_expr_200k_shapes.py`, `tests/core/test_null_function_conformance.py` and `tests/core/test_null_division_laws.py` on DuckDB, SQLite and ClickHouse) |
| `Validate.lean` | datasource validation: nullability, declared ranges, grain, complete bindings, `unique` properties, `complete where` containment | each check fails exactly when the claim the planner trusts fails, under Kleene WHERE and GROUP BY's NULL grouping (checked by `tests/core/validation/test_validation_laws.py` and `tests/core/validation/test_type_conformance.py`) |
| `Fixpoint.lean` | the optimizer driver's repeat-until-quiet passes; the WHERE-atom contract between fact removers (`StripRedundantNotNull`) and every other phase | under the contract a fact remover never changes the plan after its first run, so it cannot take part in a cycle; pushdown before its gate broke the contract and cycled (checked on every phase of every planned query in the test suite: `atom_contract.contract_violations`, and by `tests/optimization/test_fixpoint_contract_laws.py`) |
| `Rewrite.lean` | optimizer rules as rewrites of joined rows: the NOT NULL strip, outer-join upgrades, predicate pushdown and its removal, an identity GROUP BY, a matched outer join, an existence fold, a semi-join into an aggregate; a value a partial scan computes; a WHERE over an aggregate decided through a join; the per-query `~` heal; a FULL join beside a `~` subset; a row-level filter argument beside a gate; a filtered value at its keys' grain; a HAVING alias | each rule keeps the rows under the guard it checks (`strip_sound`, `full_to_left`, `left_to_inner`, `or_forces_side`, `push_inner`, `push_preserved`, `push_padded`, `remove_inner`, `push_group_key`, `push_window_partition`, `identity_group_exact`, `merged_group_sum_exact`, `matched_left_is_inner`, `existence_fold_exact`, `semi_join_exact`), with a counterexample per guard; a strict value pads as computed (`pad_strict`), a null-sensitive one does not; an atom false on an empty group is decided by the groups it kept (`kept_exact`), `count = 0` is not; a FULL to a `~` side whose rows all match is the LEFT join (`subset_full_is_left`), and a proof on the key forces no `~` lookup (`key_proof_keeps_lookup_gap`); a row-level argument joins on the key its row determines (`keyed_arg_exact`); a membership moved to a WHERE is read against its set, never the rows it passed through (`moved_membership_reads_its_set`) (checked by `tests/optimization/test_rule_soundness_laws.py` against a pre-joined twin and with each rule disabled, and `tests/dialect/test_having_alias_shadowing.py`) |
| `Unnest.lean` | filtering rows by the elements of an unnested list: the padded expansion, the kept rows, aggregates over them, how the elements pair with the rows they expand | a WHERE on an element keeps a row iff a padded element passes, a disjunct over the row alone keeps it whatever its list holds, an aggregate reads each kept row once, a filter keeps a sub-multiset of the expansion, pairing on the grouping key or on nothing overcounts, the key an element is merged into pads a member no list names (checked by `tests/unnest/test_unnest_filter_laws.py`) |
| `Binding.lean` | datasource bindings as functional dependencies: row determinants, the closure over every table's bindings, `row_keys`, the cross-table binding check | a table unique on its grain satisfies its own bindings, the closure is sound when every binding holds, one table's binding can break on another's rows, stacked arms are unique on grain plus discriminator and not on grain alone, a grainless table determines nothing (checked by `tests/core/test_binding_fd_laws.py` and `tests/core/validation/test_validation_laws.py`) |
| `Cover.lean` | which queries a count summary covers: targets and conditions that are functions of its key, filtered and distinct counts, totals beside a target, existence, relations keyed on the grain | a target is any function of the key under any spelling (a rename, a scalar of a stored column, a column listed twice), `count(x ? c)` is the re-sum of the groups `c` keeps, and `sum`/`min`/`max(x ? c)` the re-aggregate of `agg(x) by G, cols(c)` over them (`filtered_sum_exact`, `filtered_min_exact`, `filtered_max_exact`), which needs the count's `coalesce` (`uncoalesced_case_count_null`) and the condition's columns in the partial's key (`partial_off_condition_misreads`), or joined onto the summary's rows by a functional lookup before any re-sum that drops their key (`joined_case_exact`, `resum_then_case_exact`; `joined_fanout_double_counts`, `case_above_coarser_grouping_misreads`), in either spelling `agg(x ? c)` or `agg(case when c then x end)`, `count_distinct(k)` is `count(k)` where every row holds its own `k`, a total and a per-target count are independent re-sums, a complete summary proves which key values exist, a count by a value the key relates to re-sums the related groups, which is the summary over the DISTINCT `(key, value)` pairs (`distinct_pairs_resum_exact`, `relation_resum_count_exact`), and re-summing relation pairs double counts, including the pairs a list repeating a value expands to (`collapsed_fanout_double_counts`, `repeated_element_counts_once`), which a list declared `::unique` cannot do (`unique_list_pairs_are_distinct`) (checked by `tests/discovery/test_summary_cover_laws.py`, `tests/discovery/test_filtered_rollup_laws.py`, `tests/discovery/test_relation_rollup_laws.py` and the grouped cases of `tests/unnest/test_unnest_filter_laws.py`) |
| `SelectGrain.lean` | an aggregate with no `by` against one `by` fixed keys, wherever it appears | an unspecified aggregate is the aggregate `by` the select's raw grain (`bare_is_select_grain`), so it and any scalar over it are functions of that grain (`bare_on_grain`, `bare_expression_on_grain`), and in a select with no keys it is the total (`bare_empty_grain_is_total`); one `by` fixed keys is the same in every select (`specified_ignores_select`); a grain holding a window's partition reads the partition's rows alone (`bare_within_partition`); pinning a window's order aggregate to its first argument or to the tuple it ranks gives another value (`first_argument_pin_misreads`, `window_tuple_pin_misreads`), and so does building a membership's member at the select grain (`membership_pin_misreads`); a property selected beside its key is read with the key (`property_with_its_key_exact`), never off a group without it (`property_without_its_key_misjoins`), nor off rows wrapped short of the key they pair on (`wrap_without_pair_key_crosses`); a value over aggregates at two grains is one per pair the rows hold, never one per city and species off the two groups alone (`two_grain_value_without_its_pairs_invents`), and an aggregate rendered at a host whose grain does not determine its `by` is read at the host's (`host_grain_pin_misreads`), and a constant gated by such a value's groups alone keeps a city no row passes (`constant_gated_off_its_rows_invents`); a select of an aggregate `by` fixed keys groups by its value, a function of those keys (`fixed_on_keys`, `keys_refine_value`), so a bare aggregate beside it is the re-sum of the key groups with that value (`bare_beside_fixed_value`), and grouping by the keys in its place splits and repeats a tuple (`by_keys_split_tuple`, `fixed_value_rows_distinct`); an aggregate of that value, or of any scalar of it, reads it once per distinct key in each tuple (`ofValue`, `scalar_of_fixed_on_keys`, `of_value_repeat_invariant`), never per row (`fixed_value_per_row_double_counts`), and a rowset member holding it is an outer key (`rowset_member_dropped_totals`); a select's key rows are the keys of the rows its WHERE keeps (`keptKeys`, `kept_key_holds`), a literal membership is a disjunction of comparisons (`literal_membership_is_a_comparison`) and a count in one is decided on an absent key by whether 0 is a member (`count_membership_on_absent`), and an aggregate `by` a grain the select grain determines is a value of it (`determined_by_on_grain`); read off a gate's group, the keys lost a literal membership and a row atom beside the gate (`literal_membership_gate_dropped`, `gate_group_skips_row_atoms`), and a `by` grain holding a property joined on the other columns alone fanned out (`by_property_without_its_key_misjoins`); an atom drops whole groups only when it is decided per group key (`atom_on_group_key_drops_groups`), so one over a row column and a finer gate filters the rows the aggregates read (`mixed_atom_after_count_misreads`), and a join pair is implied only by pairs on its own side (`pair_implied_off_its_side_misjoins`); a kept tuple's columns are kept columns (`kept_pair_in_product`) but not conversely, so keys split over two filtered branches pair dropped tuples (`split_keys_pair_unkept`), a `by` value of keys the select determines is no grain key (`by_value_is_no_grain_key`; joined on as one, `by_value_as_key_misjoins`), and `c and c` filters as `c` (`conjunction_idempotent`); a WHERE filters the rows of every group it is applied in (`where_filters_group_rows`), so a host whose value is only joined beside another row stream filters none of that stream (`gate_host_skips_select_rows`); a value is paired by its grain (`value_paired_by_grain`), never by its own `=` (`value_pair_drops_null_group`); and the empty-total guard holds of the whole HAVING (`guard_conjoins_having`, `guard_precedence_emits_empty_total`); a HAVING `by` keys the select's rows bind decides whole grain rows (`having_by_bound_key_is_coarser`), and a gate's value read past a host of its atom equals its own group's only for a null-rejecting atom (`pastHost`, `gate_past_host_exact_if_null_rejecting`; `is null` passes the padding, `gate_read_past_its_host_passes_absent`), and a value at a grain the rows determine is projected with the rows, never paired with them short of its grain (`value_bucket_pairs_short_of_its_grain`); the select's keys come off one row stream holding their tuples (`keys_off_one_stream_exact`), never off two groups paired on a shared column (`keys_off_two_groups_pair_unheld`), and off rows the WHERE filtered (`key_off_unfiltered_stream_returns_rejected`); a dedup carries a column its outputs determine without adding rows (`dedup_carries_determined`); each argument of a WHERE atom that NULL lets hold is read off its own group (`gate_arg_past_host_exact_if_rejecting`, `conjunct_read_past_its_host_passes_absent`); and a side the FINAL filters carries the gate's keys itself (`dimension_side_pads_gate`); a gate the FINAL re-applies pairs on its whole grain (`gate_paired_short_of_its_grain`), a COUNT read past a host reads 0, not NULL, so it is read as on its own group only where the atom rejects 0 (`countPastHost`, `count_past_host_exact_if_zero_rejecting`; `count_arg_past_host_passes_absent`), and a WHERE argument's producer is a lookup onto the rows it is joined to (`lookup_keeps_rows`), never a side whose rows it keeps (`lookup_preserved_passes_padding`); a total pairs every row on the constant it is keyed by (`constant_key_pairs_every_row`), and a value pairs with the side its plan names, never keyless beside another exposing the column (`keyless_pairing_fans_out`), and an alias of a row value is no spelling of the select's pinned read of it (`alias_is_not_the_pinned_read`); a side a join pads is not the WHERE's population, whatever was applied beneath it (`padded_side_is_not_the_population`); a filtered row value is a value of its row, not of the select's grain (`filtered_value_is_per_row`), a gate's feeder is no row of the statement (`gate_feeder_is_no_population`), a column peeled onto a gate's key skips the row filters that key does not determine (`peel_skips_row_filter`), a rename keeps the filtered rows it was read on (`rename_keeps_filtered_rows`), a dimension peel pairs on every key it is identified by (`peel_paired_short_of_its_keys`), a parenthesized WHERE is its conjunction (`and3`, `parenthesized_conjunction_is_its_atoms`), a join widened to keep an earlier join's padding takes back its own type once that join pads nothing (`widening_outlives_its_padding`), and a `by *` gate the WHERE reads is no group of the select, so a one-row side crosses INNER (`one_row_cross_keeps_rows`, `gate_total_is_no_group`), and a total is read off its own row, never off a row stream that skipped the WHERE (`total_off_row_stream_keeps_rejected`); a key several sides pair is their coalesce, never the padded side's column alone (`coalesce_unpadded_member`, `pruned_coalesce_member_pads_gate`), a cross product is one relation whichever side it starts from (`mirrored_cross_join_is_one`, `second_cross_join_multiplies`), and FINAL re-reads no column finer than its rows (`final_reads_finer_column_off_gate`); a peel at keys the WHERE's atom is a function of applies the atom itself (`unfiltered_peel_keeps_rejected`), and a CTE two merged plans join from their own sides is one INNER conjunction (`rejoined_cte_is_one_conjunction`); a lookup of a key adds no tuple (`lookup_adds_no_tuple`), so beside keys split over two WHERE aggregates it still pairs dropped tuples (`lookup_beside_split_keys_pairs_unkept`), and a side is joined only on the keys its plan pairs it on, a column it merely exposes bridging nothing (`keyed_lookup_keeps_rows`, `exposed_column_joins_nothing`); a side unique on a key meets a row holding it once (`unique_side_one_partner`), so two groups beside finer rows pair through the rows, never on a column they share that identifies neither (`blend_beside_finer_rows_fans_out`), and a filtered copy of the rows pairs with them on their keys, never through a coarser group (`filtered_copy_paired_on_keys_exact`, `filtered_copy_paired_through_group_keeps_rejected`); a sole side the FINAL gates carries the gate's whole grain (`sole_gate_paired_short_of_its_grain`), a finished value beside finer rows is no partial of them (`finished_value_resummed_per_row`), and a gate that holds on NULL is never re-applied over a side's padding (`gate_on_padding_passes_rejected`); a value over groups is one row per tuple, never one per row of a finer bridge (`bridge_joined_whole_repeats_its_pairs`); a membership filters the key rows as any atom does (`membership_keys_are_kept_keys`; left off them, `membership_left_off_keys_keeps_rejected`), a set read in a subselect is no grain of the rows (`set_grain_is_no_row_grain`), a derived key's own grain is no key of its tuple (`derived_key_grain_splits_its_tuple`; nor of a re-sum at it, `resum_split_by_key_grain`), and a total is never read off per-key partials (`partial_read_as_total_splits`); a gate beside a membership filters the rows FINAL pairs the keys by (`gate_off_outputs_keeps_key_rows`), a re-sum of a nullable stored partial is nullable (`resum_of_nullable_partial_is_nullable`), and an aggregate of a value constant in its group is that value (`aggregate_of_group_constant`); the keys of two values at two grains are the rows' pairs, never their product (`keys_off_two_values_cross`), a gate joined to rows without its key keeps every row (`gate_key_read_by_a_set`), a distinct count is never recomputed at a finer grain (`distinct_count_at_a_finer_grain`), and a set's own rows are no rows of the select (`set_rows_are_no_bridge`); a set in an OR is read by its own membership (`set_read_by_its_own_membership`), a partial is no gate's value (`partial_is_no_gate_value`), a stored total is not re-maxed at keys beside it (`stored_total_not_remaxed`), a set's feeder is no row parent of the scan it gates (`feeder_is_no_row_parent`), a value over partials is no join key before the re-sum (`value_off_partials_is_no_key`), and a set's condition is read at its own keys (`set_condition_at_its_own_keys`); a derived key read off groups pairs with rows on nothing they hold (`derived_key_off_groups_pairs_on_nothing`), and a WHERE is applied once: a row predicate is idempotent, one over a total is not (`row_where_idempotent`, `where_over_total_not_idempotent`) (checked by `tests/discovery/test_sweep_windows_shapes.py`, `tests/discovery/test_sweep_compositions_shapes.py`, `tests/discovery/test_oracle_compositions_laws.py`, `tests/discovery/test_sweep_expr_350k_shapes.py`, `tests/discovery/test_sweep_expr_200k_shapes.py`, `tests/discovery/test_sweep_expr_150k_shapes.py`, `tests/discovery/test_sweep_expr_100k_shapes.py`, `tests/discovery/test_sweep_expr_60k_shapes.py`, `tests/discovery/test_sweep_390k_shapes.py`, `tests/discovery/test_sweep_330k_shapes.py`, `tests/discovery/test_sweep_270k_shapes.py`, `tests/discovery/test_sweep_210k_shapes.py`, `tests/discovery/test_sweep_150k_shapes.py`, `tests/test_cluster_key_lookup.py` (a cluster key looked up on grain columns the rows determine), `tests/discovery/test_spellings_60k_gates.py`, `tests/discovery/test_padded_population_gate.py`, `tests/discovery/test_total_beside_lookup_gate.py`, `tests/discovery/test_final_gate_lookups.py`, `tests/discovery/test_key_sources.py`, `tests/discovery/test_gate_value_sources.py`, `tests/discovery/test_where_hosts.py`, `tests/discovery/test_oracle_sweep_laws.py`, `tests/discovery/test_oracle_spellings_laws.py`, `tests/discovery/test_split_key_gates.py`, `tests/discovery/test_where_gate_pairing.py`, `tests/windows/test_window_order_aggregate_laws.py`, `tests/windows/test_window_order_expression_laws.py`, the window cases of `tests/discovery/test_rollup_resolution_laws.py` `tests/core/test_fixed_aggregate_select_laws.py` and `tests/core/test_fixed_value_lifecycle_laws.py`) |
| `Windows.lean` | what a window ranks and where ORDER BY sorts NULLs: a rank over the distinct tuples of its grain, the read-back onto rows, an empty `over ()`, per-engine NULL placement and its rendering | a rank over a grain's distinct values depends only on which values occur, a stream rank is right when it holds each value once and counts repeats otherwise, a read-back from a table unique on its key keeps one row per row, `over ()` is the total, the rendering places NULLs as asked on every engine, grouping by a window's value is the re-sum of its tuple's groups, never one group per tuple, an aggregate a window orders by or reads with no `by` is at the select grain, which, when it holds the partition, reads the partition's rows alone, a window over such an aggregate reads one row per group, and a filtered column beside it is no key of its rows (`filtered_passenger_repeats_groups`) (checked by `tests/discovery/test_oracle_windows_laws.py`, `tests/windows/test_window_laws.py`, `tests/windows/test_window_group_laws.py`, `tests/windows/test_window_order_aggregate_laws.py` and `tests/windows/test_window_order_expression_laws.py` on DuckDB and SQLite, `tests/windows/test_rank_beside_partial_merge.py` for a rank widened onto a partial merge, and `tests/dialect/test_null_order_renderings.py`) |
| `Filters.lean` | a `?` as a select target: what determines `x ? c`, what its rejected rows are, what a select of filtered values alone returns, and what a value its condition reads is built from | a condition and a value that are functions of a key make `x ? c` one (`filtered_value_on_its_keys`; off its keys one key shows two values, `filtered_value_off_its_keys_splits`), the rows a filter rejects in a group are one NULL row of it, so a filtered key names none of the rows it was read off and its source column is no key of a window's groups (`rejected_rows_share_one_null`, `source_column_splits_null_group`), filtered values alone are the rows the condition keeps, a membership included (`alone_keeps_passing_rows`, `case_alone_shows_rejected`), a set is no finer than the column it gates (`value_is_no_finer_than_its_reader`), a total gated by a total is a scalar (`total_gated_by_total_is_scalar`), a partial grouped by a reader of its own roll-up has no build order though its value is exact (`partial_grouped_by_its_reader`, `min_of_group_mins`), and a WHERE hosted beneath an aggregate only a set reads filters no row of the statement (`gate_on_a_set_filters_no_row`); a filter pairs on its grain only as rows unique there, so a column carried below it fans out and a collapse past a paired key crosses it (`carried_column_fans_out`, `collapse_drops_paired_key`), its keys and what its condition reads are columns of its rows (`filter_keys_stay_on_its_rows`), a WHERE hosted only beneath an aggregate its condition reads filters that aggregate's rows and none of its own (`gate_on_a_condition_filters_no_row`), a filter its keys decide is shown once per value (`decided_filter_repeats_per_key`), a filter an aggregate answers is still a value of the select's rows (`filter_over_aggregate_keeps_its_rows`) and meets a WHERE gate on the gate's whole grain (`filter_meets_gate_on_its_grain`), a filtered key identifies no row (`filtered_key_identifies_no_row`), a set holds none of the select's keys and an aggregate only sets read parents no value beside them (`set_holds_no_select_keys`, `set_input_is_no_row_parent`; a key read off such an aggregate beside another group's key still crosses them, `set_input_supplies_no_output`, open), a WHERE aggregate only such an aggregate reads still filters the select's rows (`gate_beside_set_filters_rows`), a WHERE reaches a filter through its row parents, not its condition's value or a set's input (`gate_reaches_a_filter_alone`, `gate_hosted_on_set_input_alone`), a filter of a gated aggregate holds groups that passed (`hosted_content_passed_gate`), a total splits no key (`total_splits_no_key`), a condition's aggregates meet it on their whole grain (`condition_aggregates_meet_per_row`), a regrafted key keeps its pair (`regraft_keeps_paired_key`, open), a partial chain grouped by its head's reader has no build order (`partial_chain_reads_itself`), a set supplies no column to a group beside it (`set_supplies_no_column`), a set planned alone under the WHERE gates its filter's own rows (`set_plan_gates_its_rows`), a select target FINAL re-sources reads the WHERE-kept set (`select_reads_kept_set`), and an aggregate only a set reads supplies no select key (`set_input_supplies_no_key`) (checked by `tests/discovery/test_oracle_filters_laws.py` and `tests/discovery/test_sweep_filters_shapes.py`) |
| `SetOps.lean` | the `except` / `intersect` TVFs against `not in` | a value is in `A except B` iff it is in `A` and matches nothing in `B` by NULL-safe equality, `intersect` likewise, three arms fold left, and `not in` drops a NULL that `except` keeps (checked by `tests/setops/test_setops_laws.py` on DuckDB and SQLite) |
| `Scalars.lean` | calendar offsets, the type of a date offset, `greatest` / `least`, `log` | `date_add` by months clamps to the month's last day, so `date_diff` counts back the offset and the day is a real day (`add_months_diff`, `add_months_valid`); a whole-day offset of a date compares as a date under text and coerced comparison alike (`typed_eq_agrees`); each argument falling back to the others makes a NULL-propagating extreme skip NULLs (`fallback_greatest_exact`); `log(x, b)` takes the base second (`base_first_misreads`) (checked by `tests/core/test_scalar_laws.py` on DuckDB, SQLite and ClickHouse, and `tests/dialect/test_scalar_law_renderings.py`) |
| `Types.lean` | the declared type of an expression as the column a persisted copy is stored in | a sum holds in the join of its operands' types and a product in the sum of their scales (`add_preserved`, `mul_preserved`), a remainder in its dividend's type (`rem_preserved`), `greatest` / `least` in the join of all arguments (`pick_preserved`); no decimal scale holds a quotient (`third_has_no_scale`), and a 4-byte float does not hold a double (`single_misses_double`); a remainder of the integer parts or of a decimal's unscaled integer drops the fraction (`integer_rem_drops_fraction`) and `x - m * trunc(x / m)` keeps it (`rem_by_trunc`) (checked by `tests/core/test_type_materialization_laws.py` on DuckDB, SQLite and ClickHouse, and `tests/dialect/test_type_law_renderings.py`) |
| `Padding.lean` | what an outer join puts in a row it could not match, and what a cast does to NULL | a NULL pad leaves `count` and `sum` of a right column equal to those over the matches (`null_pad_count_eq_matches`, `null_pad_sum_eq_matches`); a `0` pad counts an unmatched row and is the `max` of negative values (`default_pad_counts_unmatched`, `default_pad_max_raises`); a cast is strict (`cast_keeps_null`) (checked by `tests/engine/test_clickhouse_null_laws.py`, and by the window and select-grain law suites, which now run on ClickHouse through chdb beside DuckDB and SQLite) |
| `StringMatch.lean` | the `like` pattern language, `ilike`, `contains`, and a string literal as each dialect reads it | a pattern with its specials escaped matches exactly its string (`escaped_literal_exact`), so every literal pattern means the same under any LIKE; a pattern with no wildcard is its string's equality (`exact_pattern_is_eq`); SQLite's GLOB over the translated pattern is the case-sensitive match (`glob_sound`); `contains` is a case-folded infix (`contains_is_folded_infix`); ClickHouse's rewrite of a computed pattern (`ch_strip_sound`) and its doubled-backslash literal (`ch_literal_roundtrip`) read back the pattern and the string; a match is NULL on a NULL operand (`like_strict`) (checked by `tests/core/test_string_match_laws.py` on DuckDB, SQLite and ClickHouse, and `tests/dialect/test_string_match_renderings.py`) |
| `Strings.lean` | `len`, `substring`, `strpos`, case, `trim`, `replace` and the regex functions as characters, positions and groups | every engine's `substring` agrees once start ≥ 1 and count ≥ 0, and the renderer's clamped arguments keep the reference's characters for any start and count (`clamp_exact`, bounded; `native_positions`); the substring at `strpos(s, t)` of `len t` characters is `t` (`strpos_extract`), and an empty needle is found at 1 (`strpos_empty`); `LEN(s + 'x') - 1` is SQL Server's length (`marker_len`); `like '%'` holds for every string (`percent_matches_all`) and `x = c and x is not null` keeps what `x = c` keeps (`sibling_rejects_null`) (checked by `tests/core/test_string_function_laws.py` on DuckDB, SQLite and ClickHouse, and `tests/dialect/test_string_function_renderings.py`) |
| `Casts.lean` | casts and the literal expressions the parser folds: a decimal to a smaller scale, a string or number as a bool, a bool as a string, `sqrt` / `log` outside their domain, a comparison with a NULL literal, a CASE whose arms all fold false | a decimal rescale rounds half away and ClickHouse's round-then-cast keeps it (`rescale_exact`); one table of spellings reads a string as a bool (`text_bool_exact`) and a number is true iff nonzero (`ne_zero_exact`); `sqrt` and `log` are NULL outside their domain (`domain_guard_exact`), so they are NULL-introducing (`domain_null_introducing`); a folded comparison is the Kleene one and a NULL operand is left UNKNOWN (`fold_agrees`, `fold_null_unknown`); dropping false CASE arms keeps the value and no arm left is NULL (`drop_false_arms_exact`, `no_arm_is_null`) (checked by `tests/core/test_cast_laws.py` on DuckDB, SQLite and ClickHouse, and `tests/dialect/test_cast_renderings.py`) |
| `Readings.lean` | a string cast to a number or a date: the numeral grammar, the value it names, the rendered guards and reads per engine, a set of reads under membership | SQLite's GLOBs accept exactly the pattern's strings (`glob_number_exact`, bounded); the value read is the numeral's and an integer rounds it half away (`read_number_exact`); ClickHouse's read at 18 places then rounding is the exact rounding (`truncate_then_round_exact`, bounded), which truncating to the target's own scale is not (`truncate_to_target_misrounds`); a calendar that rolls an overflowing day gives a written day back iff it is real (`readback_exact`, bounded); a non-null string can read as NULL (`read_null_introducing`), and over a set with no NULL member equality is identity (`null_free_eq_exact`); a numeral past the target's range is NULL, never saturated (`saturate_in_range`, `saturation_is_not_the_value`), SQLite's digit-by-digit check of the 64-bit bound is the order (`digit_compare_exact`, bounded), and the range is checked on the rounded value (`round_can_leave_range`) (checked by `tests/core/test_text_read_laws.py`, `tests/core/test_instant_read_laws.py` on DuckDB, SQLite and ClickHouse, and `tests/dialect/test_text_read_renderings.py`) |
| `Instants.lean` | a string cast to a datetime or a timestamp, and `date(s)` / `datetime(s)` / `timestamp(s)` of a string: the day count, zones, UTC, each engine's precision | the day count is linear in the day of the month, so a rolled day has the written day's count and only reading it back tells them apart (`roll_keeps_count`, bounded); under one zone a read keeps the order of local times and a zone is a shift (`utc_monotone`, `utc_shift`), which can cross midnight (`read_instant_exact`); truncating to an engine's precision composes, keeps order, and decides a comparison with a value the engine holds as the exact instant does (`trunc_trunc`, `trunc_monotone`, `trunc_ge_exact`) (checked by `tests/core/test_instant_read_laws.py` on DuckDB, SQLite and ClickHouse, and `tests/dialect/test_text_read_renderings.py`) |
| `Writings.lean` | a float, a decimal or an instant cast to a string: shortest digits laid out as `repr`, a declared scale, a fraction without trailing zeros, a timestamp at UTC; a decimal quotient | a decimal's text reads back as its value and carries its scale (`decimal_read_write`, `decimal_scale_written`, bounded); `repr`'s layout reads back as the digits and exponent it laid out (`layout_read_exact`, bounded) and is fixed exactly for an exponent in [-4, 16) (`fixed_iff`); rounding to 15 places keeps a value of 15 digits or fewer (`round15_exact`, bounded); the stripped fraction reads back and does not depend on the precision the engine holds (`fraction_read_write`, `fraction_precision_free`, bounded) (checked by `tests/core/test_text_write_laws.py` on DuckDB, SQLite and ClickHouse) |
| `Sharing.lean` | a rendering's operand: how many times a cast or a clamped substring names it, its size when nested, the draws a volatile operand reads, SQLite's flattener | inlined, `n` levels hold `k ^ n` copies of the operand (`inline_nested_ge`) and bound they grow linearly (`bound_nested`), so binding wins once its own text is under the copies it saves (`bound_le_inline`); bound, every use reads one draw (`bind_draws_once`), and a binding that reads its row's column draws per row (`correlated_per_row`); flattening a binding is inlining it (`flattened_is_inline`) (checked by `tests/core/test_bound_operand_laws.py` on DuckDB, SQLite and ClickHouse, and on SQLite 3.45 where `/usr/bin/python3` has it) |
| `ValueSet.lean` | aggregates of a column a table stores at its own grain: a complete binding's values, repeat-blind and repeat-sensitive aggregates, `count` of a column against a stored row count | `min`, `max` and `count_distinct` are functions of their input's set of values (`value_set_min_exact`, `value_set_max_exact`, `value_set_count_distinct_exact`), and so is a scalar of the column alone (`SameValues.map`), so a complete binding answers them at any grain, and a column its table's grain determines holds the base rows' values under any predicate on that grain (`grain_column_values`, `grain_column_min_exact`); `count` and `sum` read repeats (`value_set_misses_repeats`); `count(cls)` is the re-sum of `count(id) by cls` over the non-null classes (`count_column_from_row_count`), and over every class it counts the NULL class's rows (`count_column_counts_null_group`) | (checked by `tests/discovery/test_value_set_laws.py`, with the row table in scope and hidden) |
| `Identity.lean` | a planned source's identifier, spelled from its members and grains; a scan's projection under a condition | unbracketed, wrapping a join and joining a wrapped side spell one identifier (`naive_wrap_of_join_collides`); with a join's members bracketed, no two sources to depth 2 share one (`bracketed_injective_bounded`); two projections of one table under one condition are projections of one read carrying both (`twin_scans_share_one_read`), and an aggregate of the table under it is the aggregate of the row twin's read (`group_reads_twin_rows`), never of a deduplicated one (`grouped_twin_drops_repeats`); a scan under a condition is its unconditioned twin filtered (`filter_reads_whole_twin`) | (checked by `tests/discovery/test_sweep_expr_500k_shapes.py` and `tests/optimization/test_merge_twin_scans.py`) |
| `Nullability.lean` | which computed values can be NULL, and what a join on a value claimed non-null loses | an aggregate other than a count is NULL on a group of all-NULL arguments (`max_of_null_group_is_null`), and so is an expression over it (`plus_of_null_is_null`); a count is 0 there (`count_of_null_group_is_zero`); a relation re-joined to itself on a NULL value with `=` loses the row (`plain_rejoin_drops_null_value`), with `is not distinct from` it keeps every row (`null_safe_rejoin_keeps_rows`) | (checked by `tests/discovery/test_sweep_expr_650k_shapes.py`) |

Every bug a law or oracle sweep finds is fixed with a regression test outside
the formalization suites too: the reduced shape, with rows checked by hand, in
a plain test file (`tests/discovery/test_sweep_<range>_shapes.py` or the
area's own module). A `*_laws.py` seed list or a fuzzer family is not that
test: they run in separate CI jobs and can be re-seeded or narrowed.

## What it found

- **`not (...)` was Kleene while `not p` is two-valued**
  (`Logic.mixed_negation_breaks_de_morgan`). `where not (p or q)` dropped a row
  with an UNKNOWN `p` that `where not p and not q` kept, so parentheses changed
  the answer. Compound negation now renders `coalesce(..., false) = false`, like
  atomic negation.
- **A bare boolean beside `and` blocked conjunct splitting.** In
  `where active and sum(x ? active and y > 0) by k > 0`, `decompose_condition`
  refused to split a conjunction with a bare `active` operand. The atom then
  landed in an invalid `HAVING`, and `where true and p` answered differently
  from `where p` (`Logic.and_true_left`). The parser now normalizes operands as
  a lone `where active` already was.
- **A pinned value shared a bucket with a comparison at its reads' grain.**
  `select customer_id, status, undelivered` gave the order-less customer NULL
  status, while `select customer_id, status` gave `'in-transit'`
  (`Pin.projection_invariant`). The sibling kept the shared bucket off the
  padded rows. Pinned BASICs now bucket apart.
- **A WHERE over a named derivation in a rowset with a `subset join` could
  not be placed.** `where has_amount = true` raised "could only be placed
  after the aggregates", while its inline spelling `nullable_amount is not
  null` planned (`Pin.named_eq_inline`). The joined rowset counted as a
  row-shape barrier the atom's rows crossed, though it is a side input. This
  was latent on main: TPC-DS q29's bare `is_returned` dodged it only because
  the conjunct-split bug above kept the whole WHERE in one piece.
- **`docs/modeling_concepts.md` contradicted `docs/grain_pin.md`** on the
  padded row of a CASE with an ELSE (`Domain.pin_contradicts_stored_reading`:
  no one value satisfies both). The engine follows the pin, and the
  modeling doc is corrected.
- **Partition unions lost and doubled rows** (`Sourcing.union_perm_of_partition`:
  a union of `complete where` arms equals the table when every row satisfies
  exactly one arm). The checker that admitted a family tested coverage by
  stepping an increment across each gap. It had four holes:
  - it never tested overlap, so arms `x <= 10` and `x >= 5` doubled every row
    in between (`Sourcing.overlap_double_counts`)
  - comparisons are Kleene, so a nullable discriminator's NULL rows were in no
    arm and vanished (`Sourcing.null_escapes_comparison_arms`)
  - a `datetime` is a Python `date`, so datetimes stepped by a whole day:
    `t <= 00:00` and `t >= 20:00` "met" (`Sourcing.day_step_misses_a_time`)
  - floats stepped by machine epsilon, which `100.0 ± ε` rounds away, so
    `x < 100.0` and `x > 100.0` lost `100.0`
    (`Sourcing.absorbed_epsilon_misses_the_cut`)

  The checker now evaluates each arm once per cell: each literal, each
  non-empty open gap between adjacent literals, and NULL. Comparisons are
  constant on a cell, so this is exact on dense and discrete domains alike
  (`Sourcing.checker_exact`). A family is a source only if it partitions the
  domain. `x is null` arms are understood, and a gate that proves the
  discriminator non-null drops NULL from the domain (`Sourcing.gated_union_perm`).
  A family the old checker rejected is now admitted: a nullable discriminator
  with an `is null` arm.
- **A union's answer came back in alphabetical column order.** The fuzzer's
  `union_*_rows` laws found that when the union was the whole answer, its arms
  projected in their own order, not the select's. Set operations pair columns
  by position, so every arm now projects in the union's order.
- **A materialized copy of an arm made its family unresolvable.** A
  `partial datasource ... complete where x <= 10` built beside the roots it
  copies overlaps their `x <= 10` arm. The family as a whole is then no
  partition (`Sourcing.duplicate_arm_double_counts`). Before the cell checker
  it doubled those rows. After it, every unfiltered query over the family
  raised "unresolvable". The planner now unions an exact cover of the cells,
  one arm per cell, preferring the widest column overlap and then the most
  materialized arms. Which cover it picks cannot change the rows
  (`Sourcing.cover_choice_irrelevant`). Enum families already chose arms per
  value.
- **String discriminators get equality cells.** A plain `string` family was
  always declined. With equality atoms only, every string that is not a
  literal agrees on every atom (`Sourcing.eq_atoms_agree_off_literals`), so
  `x = 'a'`, `x != 'a'`, `x is null` is proved a partition. Order atoms on
  strings still decline, since SQL collation is not Python's order.
- **Watermarks compared across Python types by string order.**
  `Freshness.verdict_agrees_with_refresh` holds only if the Python verdict
  orders values as SQL's `>` does. `_compare_watermark_values` fell back to
  `str()` whenever two values had different Python types, and `"10" < "9"`
  (`Freshness.string_order_misses_a_refresh`). A DECIMAL root beside a CSV
  target sniffed as DOUBLE read fresh while behind, so its new rows were never
  loaded. A DECIMAL watermark restored from a `--state-input` snapshot came
  back as text and failed the same way. Numbers now compare by value, and
  snapshots restore `Decimal`. The incremental refresh of a DECIMAL key also
  crashed: the dialect could not render a `Decimal` literal in its `k > m`
  filter.
- **A fan-out merge depended on what the deltas carried.** `merge_snapshots`
  promised order independence, and three inputs broke it or its counts.
  `--state-max-partitions` trimmed a worker's scoped delta, so rebuilt slices
  stayed stale in the merge and a newly built one was never recorded
  (`StateMerge.trimmed_delta_loses_a_rebuild`). A scoped list is no longer
  trimmed. A record that only scoped deltas had reached, as on an asset's first
  fan-out, took one worker's mid-run count as its base and counted every later
  slice as new: 8 slices read `total=13, stale=4` with all of them fresh
  (`StateMerge.unlisted_slice_counts_twice`). Two scoped records now drop the
  count, and the status comes from the slices. Two claims on one slice, from a
  retried worker or a whole-table probe, were last-listed-wins
  (`StateMerge.overlapping_order_matters`), and replaying an old whole-table
  delta onto a merged file rewound its counts. Every input, the base included,
  now folds oldest first, which is the newest-claim join. Separately, a trimmed
  whole-table probe kept base slices its own count disagreed with, so later
  slices were subtracted twice and a count reached `stale=-1`. Counts are now
  recounted from a list that holds every slice and bounded by a shorter one.
- **The null-rejection walk proved columns non-null that a kept row held
  NULL.** `gather_non_null_proofs` decides when a WHERE forces a padded side
  present, which lets the planner read a partial source alone or turn an
  outer join inner. `NullReject.proofs_sound` holds only if every function
  the walk descends into is strict, and only if every operator it trusts
  rejects a NULL. Five rules broke that, and each dropped rows from a model
  whose `returns` pad through a `~` key:
  - the walk descended into every function but a short list, so `concat`,
    `concat_ws`, `isnull`, `bool` and the aggregates counted as strict
    (`NullReject.skip_null_keeps_null`). `concat(ret::string, 'z') = 'z'`
    read `returns` alone and lost every unreturned line. The walk now takes
    an allowlist, `NULL_STRICT_FUNCTIONS`, that the conformance suite checks
    on DuckDB.
  - membership matches by identity, so `x in (1, null)` keeps a NULL `x`
    and `x not in (1, 3)` does too (`NullReject.mem_null_member_keeps_null`,
    `NullReject.not_mem_keeps_null`). Both counted as proofs. An `in` now
    proves its operand only over a set known to hold no NULL: non-null
    literals, or a ROOT column no binding makes nullable, through filters
    and strict functions. A filter's padding NULL is not a member, so such a
    set now matches with plain `=`. Before, q08's zip set carried the
    padding NULL as a member and matched a sale with no store.
  - a concept computed after the join counted as a column of the side it
    reads. `is_returned <- ret is not null` is `false` on the padded row, so
    `is_returned = false` turned the join inner and kept nothing
    (`NullReject.derived_flag_breaks_upgrade`). A proof now forces a side
    only through a column read off it, and a derived concept is judged by
    its lineage (`NullReject.proofs_subst`).
  - a COUNT is 0 on a group it has no row for. `count(ret) by order_id = 0`
    proved the count's side present, and a merge partial on `~order_id` did
    not zero-fill it, so the order with no returns vanished
    (`NullReject.zero_filled_count`). A COUNT now proves only when 0 fails
    the test, and a parent partial on a key another parent holds completely
    pads like a short region.
  - the nullability inference took every strict function as total. `x % 0`,
    an absent index or key, `array_sum([])` and `stddev` of one row are NULL
    from non-null inputs (`NullReject.mod_zero_not_total`), and are now
    `NULL_INTRODUCING_FUNCTIONS`.
- **A row derivation beside a grouped sibling lost its rows.** The law
  harness pairs the padded model with one over a pre-joined table.
  `(ret_plus > 2) or (sum(qty) by order_id > 7)` duplicated every line of an
  order and admitted lines whose own atom failed, because the condition's
  FINAL merge took the aggregate's `order_id` grain and dropped `item_id`.
  A BASIC contributor its grouped siblings' grain does not determine now
  advertises its own grain.
- **`not (...)` over a membership or a `between` failed to plan.** `not (x
  between 1 and 3)` raised "Cannot parse arg purpose", and `not (x in s)`
  buried the set inside `coalesce`, where it lost its existence edge.
  Membership is total, so its negation is the complementary membership
  (`NullReject.not_mem_is_complement`). The parser emits that complement and
  pushes `not` through `and` and `or` (`Logic.not_and`, `Logic.not_or`).
- **Date offsets rendered unparenthesized.** `date_add(d, day, n + 1)`
  rendered `n + 1 * INTERVAL 1 day`, and `date_sub(d, day, -1)` rendered
  `--1`, which comments out the rest of the statement. Postgres put the
  offset inside an interval string and had no `date_sub`. Found by the
  conformance probe.
- **Staged-where collisions are necessary, not conservative**
  (`Staged.same_address_two_stages_differ`). One address cannot carry both
  stages' values, which justifies `_validate_staged_where`.
- **Datasource validation passed sources that broke the claims the planner
  trusts.** A datasource's nullability, ranges, grain, complete bindings and
  `unique` properties are taken on trust by the planner. `validate` is the only
  thing that checks them, so each check must fail exactly when its claim does
  (`Validate.*_exact`). Four did not:
  - nullability was judged on a 100-row sample ordered NULL-first. The order
    was `SUM` over one `CASE` per column, and `SUM` renders only its first
    argument, so it sorted by the key's NULL flag and a NULL past row 100
    passed (`Validate.sample_misses_null`). A correct count order would still
    let rows NULL in nullable columns crowd out a row NULL in a non-nullable
    one (`Validate.null_count_order_misses`). It is now a full-table scan.
  - completeness compared distinct counts. Complete sources holding `{1, 2}`
    and `{2, 3}` passed (`Validate.card_check_misses_disjoint`), and a partial
    source's values were never read (`Validate.card_check_ignores_partial`).
    It is now an anti-join of every other source's values against each
    complete one.
  - a `unique` property counted keys with `count(distinct k)`, which skips a
    NULL key, so a value mapping to a NULL key and to `1` passed
    (`Validate.count_distinct_misses_null_key`). The planner groups by the
    property, where NULL is one key value (`Validate.distinct_with_null`).
  - the sampled range check compared `Decimal('0.30')` with the double
    nearest `0.3`, which is smaller, and failed `numeric(10,2)[0.1..0.3]`
    (`Validate.float_bound_rejects_its_decimal`).
- **Ingest and validation disagreed on types.** A BOOLEAN column validated as
  `int`, `float` and `numeric`, since Python's `bool` is an `int`. An ingested
  UUID column (`string`) crashed validation, `DECIMAL(10,2)[]` ingested as
  `numeric`, and DuckDB's bitstring `BIT` as `bool`. Unsigned integers and
  `TIMESTAMP_S/MS/NS` ingested as `unknown`. Found by the conformance probe.

- **A summary in scope changed answers or went unused.** A summary at
  `(city, species)` storing `count(tree_id)` answers any `sum` or `count`
  whose filter and target columns its grain determines
  (`Rollup.sum_exact`, `Rollup.count_group`), grand totals and aggregates
  inside a window included (issue #782). On main, 42 of the 108 cells of that
  matrix skipped the summary, failed to plan, or returned wrong rows:
  - a filtered grand total, a window over the count and a `sum` of per-species
    counts never read it: bucketing split the rolled-up root from its filter
    column, so the planner fell back to the row-level tables. The root is now
    marked `scan_filtered` and stays on the filter's scan, and an aggregate
    nested in a window rolls up at its own grain.
  - grouping by a derivation of the summary's key (a CASE over a property of
    `species`) never matched, since a derived column's canonical address is
    not an environment key.
  - a WHERE that reads both the summary and a joined dimension
    (`city = 'B' or tree_form = 'linden'`) returned wrong rows. The pinned
    summary scan silently dropped the atom on the joined column, the re-sum
    grouped by columns only the WHERE reads, an optimizer fold of the re-sum
    into its parent kept the GROUP BY but dropped the SUM, and a grand total
    cross-joined the unfiltered sum onto the filtered rows. Such a WHERE now
    joins the dimension onto the summary rows, filters them, then sums.
  - an empty summary's grand total returned one NULL row where the row-level
    plan returns none: the re-sum lacked the `HAVING count(*) > 0` guard
    (`Rollup.bare_sum_of_count_null`, `Rollup.grand_total_count`).

- **A summary's `min`/`max` went unused, and a HAVING over a re-summed count
  filtered the wrong rows.** `Rollup.min_exact` and `Rollup.max_exact` prove
  `min` of per-group minimums (and `max` of maximums) is exact under any key
  predicate, and need no distinct keys, since both are idempotent. The planner
  only treated `sum`/`count` as rollable, so every coarser `min`, `max`,
  `bool_and` or `bool_or` read the row-level tables. Extending the law matrix
  with a HAVING found a silent wrong answer: when the re-sum sat above a join
  (a target or WHERE column on a joined dimension), predicate pushdown moved
  `HAVING count(..) > 1` into the summary scan. The summary column shares the
  query aggregate's address there, so the filter tested each per-species count
  instead of the re-summed one. Pushdown now keeps a predicate on a value its
  CTE rolls up.

- **One aggregate at several grains grouped the rows once per grain, and an
  aggregate at another grain beside a summary returned wrong rows.**
  `Rollup.reaggregated_sum_is_partial` (and its `count`, `min` and `max`
  twins) proves that re-aggregating a GROUP BY at grain `g` to a coarser grain
  `h ∘ g` gives the table a GROUP BY at `h ∘ g` stores, so a query's own
  groups are a summary for every coarser grain it asks for. The planner now
  computes `sum(x) by city` beside `sum(x) by city, species` as the `SUM` of
  the finer value, and a chain of grains reads the rows once (a rewrite of
  the concept until 0.3.483; since then a sourcing election,
  `aggregate_rollup.elect_partials`, that changes no lineage). The new
  multi-grain axis of the law matrix (438 cells) found five wrong answers on
  main, all with a summary in scope and an aggregate whose grain is not the
  select's: `select tree_form, count(tree_id) by *` read 12 for a two-species
  form instead of 6, `count(..) by *` beside a per-city `sum` came back per
  city, and `sum(sum(x) by city) by *` beside `sum(x) by city` returned the
  inner value un-rolled, one row per species. In all of them a rollup flag
  meant for one grain was applied at another: a rolled root bundled with the
  select's scan, a rollup inherited past the node that had already finished
  it, and a re-sourced root that projected its finer filter column.

- **A summary answered `sum(x * 2)` with its `sum(x)`, and split a group an
  expression read.** `avg` does not re-aggregate, but it is the quotient of
  two aggregates that do: `Rollup.avg_of_parts_exact` proves
  `case when count = 0 then null else sum / count end` over a summary's
  re-summed `sum(x)` and `count(x)` is the filtered `AVG` of the base rows,
  the all-NULL group included. The guard is needed because BigQuery and
  Snowflake read `NULL / 0` as 0 (`Rollup.unguarded_quotient_zero_fills`).
  The planner now builds `avg(x)` as that quotient whenever a datasource
  stores both parts, so a summary answers it at any grain it answers `sum` at.
  Before, `avg` always read the row-level tables. Extending the law matrix to
  `avg`, and to expressions over each rollable aggregate, found two silent
  wrong answers on main:
  - the signature that matches a query aggregate to a stored column named the
    columns its argument reads, not the argument. A summary's `sum(height)`
    answered `sum(height * 2)`, `min(0 - height)` and `max(height + 100)`, and
    a query's own `sum(height)` partial answered `sum(height * 2) by city`
    with no summary in scope (`Rollup.stored_column_answers_its_own_argument`).
    The signature now identifies each argument whole.
  - a summary scan grouped to a derived grain's inputs (`case when species =
    'Acer' then 'b' else 'o' end` keeps `species`) holds one partial per input
    value. A node that read the partial into an expression, as in
    `sum(height) + 1`, dropped the pending re-sum, so the `'o'` group came
    back once per species (`Rollup.unsummed_partial_splits_a_group`). A node
    now carries the re-sum of every partial it reads, not only those it
    outputs.

- **A build check judged a group by another group's values, and by values it
  had rounded.** An `assert` is checked by substituting each group's values
  into the condition and running it as a constant query, so the verdict is the
  documented rule only if that substitution is exact (`Assert.check_exact`).
  Three steps were not:
  - a `by` key the condition names (`count(id) by g >= case when g = 0 then
    80 else 5 end`) was never substituted. The constant query resolved it
    against whatever row it found, so every group was held to one group's
    threshold, failing a good build or passing a bad one
    (`Assert.unsubstituted_key_reads_another_group`).
  - values went back to Trilogy text through the formatter's renderer. A
    `Decimal` was converted to a double first, so two `numeric(38,2)` sums a
    cent apart near `5e15` compared equal and `sum(x) = prior(sum(x))` passed
    a changed build (`Assert.lossy_literal_passes_a_change`,
    `Assert.decimal_as_double_merges_cents`). A double rendered as `1e+17` or
    `1e-05`, and a string holding a quote rendered as `'O'Brien'`; neither
    parses, so the check crashed. The renderer now writes positional floats,
    picks a quote the string does not break, and spells a `Decimal` as
    `'...'::numeric(p,s)`, a cast the parser now reads as an exact constant.
    `trilogy fmt` broke on the same floats and quoted strings.
  - the answer was read with `is True`, and SQLite and MySQL return `1`, so
    every assertion failed there (`Assert.is_true_rejects_sqlite_true`).

  Groups are matched by identity, as GROUP BY groups a NULL key; an `=` join
  would report a NULL group vanished from a build that kept it
  (`Assert.sql_eq_join_vanishes_null_group`).

- **A function meant one thing on DuckDB and another elsewhere.** Each
  dialect renders a function its own way, so a query means the same everywhere
  only if each rendering computes DuckDB's value. `tests/core/test_function_laws.py`
  checks the `Functions.lean` laws on DuckDB and SQLite; on main 74 of its 163
  cases failed.
  - `/` typed an integer quotient INTEGER though DuckDB computes `7 / 2 = 3.5`.
    A datasource persisted from `x / y` stored `4`, and the planner then
    answered `select id, ratio` from that copy
    (`Functions.int_typed_quotient_breaks_materialization`). An integer
    quotient is now a DOUBLE, as is `avg` of integers, which was a 4-byte FLOAT
    and stored `1/3` as `0.33333334`. SQLite, Postgres, SQL Server and Presto
    truncate an integer quotient, so `sum(x) / count(x)` was not `avg(x)` there
    (`Functions.trunc_ratio_is_not_avg`). They now cast the dividend.
  - a cast to an integer rounded a DECIMAL half away from zero but a DOUBLE
    half to even on DuckDB and Postgres, and truncated on SQLite and SQL
    Server, so `(5 / 2)::int` was `2` while `round(5 / 2, 0)` was `3`
    (`Functions.half_even_cast_is_not_round`). Every cast of a fraction is now
    `round` first (`Functions.cast_is_round`).
  - `date_diff` counted elapsed units on SQLite (`julianday` differences, which
    also rounded 120 minutes down to 119), Postgres (interval fields) and MySQL
    (`TIMESTAMPDIFF`). 23:00 to 01:00 was 0 days there and 1 on DuckDB, and the
    count did not add along a chain (`Functions.elapsed_not_additive`). It now
    counts the boundaries of both sides truncated to the part
    (`Functions.diff_add`, `Functions.diff_eq_zero_iff`). SQL Server rendered a
    `date_diff` it has no function for.
  - `week` was `%W` on SQLite (0 for the first days of 2020, which is ISO week
    1), Sunday-based on MySQL and BigQuery, and session-dependent on Snowflake.
    It is now the ISO week everywhere, consistent with `date_trunc(x, week)`.
  - `day_of_week` is 1 on Sunday on DuckDB, SQLite and MySQL. BigQuery
    subtracted one, though its own `date_part(x, day_of_week)` did not
    (`Functions.bigquery_minus_one_disagrees`). Presto, ClickHouse and
    Snowflake returned an ISO day, and Postgres and SQL Server rendered a
    `day_of_week()` they lack. Each now shifts its native day
    (`Functions.sunday_first_from_iso`, `Functions.datefirst_independent`).

- **A partial summary answered unfiltered rollups, and a cheaper source in
  scope went unread.** Every datasource that answers a query gives its rows, so
  the choice among them is cost alone. One order needs no statistics: a source
  whose `complete where` arm implies another's and whose grain is coarser
  stores no more rows on any data (`Cost.dominated_le`). A filtered table and a
  complete summary have no such order (`Cost.partial_and_summary_incomparable`).
  `tests/discovery/test_cheapest_source_laws.py` runs 63 queries over 38 sets of
  filtered tables and summaries and checks the rows and the table read. On
  main 602 of its 2394 cases failed:
  - 338 were wrong answers. A summary with `complete where city = 'A'`, or a
    `partial datasource` summary with no arm at all, was read as complete by
    every rollup path: `select count(tree_id)` gave 3 trees for 6 and
    `select city, count(tree_id)` gave B and C a count of 0
    (`Cost.unimplied_partial_summary_loses_rows`). The rollup matchers checked
    the summary's grain and filter columns but never its arm. A partial summary
    now rolls up only under a WHERE that implies its arm
    (`Cost.partial_summary_sum_exact`), and one with no arm never does. A `~`
    key column is padding, not missing rows, so it still rolls up.
  - 64 read such a summary where it did not answer and the rows matched only
    because the data did.
  - 176 read the full table beside a `complete where city in ('A', 'B')` copy
    under `city = 'A'`, `city = 'B'` or `(city = 'A' or city = 'B')`. Implication
    compared atoms by syntax. It now also compares value pins: `x = a`,
    `x in (...)` and disjunctions of them over one column imply `x in S` when
    their values lie in `S` (`Cost.value_set_implies`,
    `Cost.disjunction_is_union`). A NULL literal matches no row, so a list
    holding one is not reasoned over (`Cost.null_member_matches_nothing`).
  - 24 read a dominated summary: `sum_cs` beside its `complete where` copy for a
    coarser rollup, `sum_c` beside its copy for a filtered grand total, and
    `sum_cs` beside the coarser `sum_s` for a grand total. The pinned
    finer-filter scan, the `complete where` scan and the legacy select
    planner's twin dedup all broke ties by datasource name. Each now ranks a
    candidate by how many others in scope dominate it, which never puts the
    costlier one first, since dominance is transitive.

- **A partition family with a value-set arm could not union, and summaries
  stacked across arms repeated a grain value.** `Stack.lean` models stacking
  a family. On main, 119 of 630 law cases failed:
  - 117 declined to answer. The family `trees_a (complete where city = 'A')`
    and `trees_bc (complete where city in ('B', 'C'))` partitions an enum
    `city`, but the enum cover search picks arms one value at a time and read
    only `x = a` claims, so `x in (...)` arms never joined a family. Taking
    them per value would be unsound: `x = 'B'` for B and `x in ('B', 'C')`
    for C count B twice (`Stack.per_value_choice_double_counts`). Since
    `x in S` is the disjunction of `x = a` over `S`
    (`Stack.in_is_eq_disjunction`), the cell checker (`Sourcing.checker_exact`)
    now reads `in` atoms, and an enum family the per-value search cannot
    cover falls back to its exact covers. `x in ('A')` builds as a
    parenthesized literal, which neither the checker nor the value-pin
    implication read; both do now.
  - 2 wrong answers. Species summaries per city, `(species, n) grain
    (species) complete where city = 'A'` (and `'B'`, `'C'`), unioned into
    one species summary that stored `Acer` once per city
    (`Stack.unbound_arm_repeats_grain`). Stacking stores one row per grain
    value, each that value's whole aggregate, when the grain determines the
    arm (`Stack.stacked_keys_nodup`, `Stack.stacked_summary_sum_exact`). An
    arm then joined a family only when its discriminator was bound or
    followed from declared keys. The next entry replaces the bound case.
- **A table binding a key rewrote that key's identity, and summary families
  never served a rollup.** A datasource binding a KEY beside its grain made
  the grain that key's `keys` (`Environment.fk_derived_keys`, last
  declaration wins), so a per-city arm binding `city` beside grain `species`
  claimed species determines city everywhere. Bindings are now functional
  dependency facts that every FD reader consults: a table binding a column
  beside its grain says its row determinants determine the column, and an
  arm's row determinants include a bound discriminator, since an arm is
  unique on its grain only among its own rows
  (`Stack.unbound_arm_repeats_grain`). On main, 77 of 810 cases in
  `tests/discovery/test_partition_family_laws.py` failed: 9 wrong answers by
  `(city, species)` and 68 declines across stacked summary families and
  bound-arm families, which now serve rollups
  (`Stack.stacked_summary_sum_exact`). Retiring the last readers of the old
  keys turned up one more wrong answer: a pinned WHERE over a merged unnest
  axis split `species` off `tree_id`, which one table binds together, and
  re-read species by ecoregion alone, returning NULL species rows (2 cases in
  `tests/engine/test_grain_pin_unnest_merge_filter.py` fail on main). The
  next entry models the bindings in Lean.

- **A grainless table's columns read as constants, no check held a binding
  across tables, and filtering facts by a dimension's unnested list
  cross-joined.** `Binding.lean` models a binding as an FD from the table's
  row determinants. On main, 58 of 300 cases in
  `tests/core/test_binding_fd_laws.py` failed: `_fd_rows` gave a grainless
  table's columns an empty grain, so `build_fd_closure` with
  `include_empty_grain=True` put every key such a table binds into every
  closure (`Binding.grainless_binding_claims_a_constant`). A grainless table
  now contributes nothing (`Binding.grainless_determines_nothing`). Each
  table's grain check proves its own bindings (`Binding.binding_holds`), but
  the planner reads them model-wide, and a species dimension binding `city`
  breaks on trees whose species sits in two cities
  (`Binding.binding_breaks_on_another_table`). Validation now checks every
  binding over the union of the tables binding its columns
  (`Binding.cross_check_exact`; 23 of 41 validation cases have no check on
  main). It flagged an existing fixture whose two region summaries disagree.
  The fuzzer's new `merged_axis_pin` family reproduces #821's wrong answer
  (24 of 168 fail before that fix) and led to a wider one on main: a WHERE on
  the unnest of a dimension's list, reached through a fact's key, paired the
  fact rows with the elements on nothing (`ON 1=1`), so
  `select tree_id where ecoregion_id = 2` returned every tree. 16 of 17 cases
  in `tests/engine/test_unnest_dimension_list_filter.py` fail on main. A ROOT
  contributor's join identity is now the keys it delivers, an unnest carries
  a join key its parent emits, the FINAL reads a contributor's columns through
  the unnest a WHERE argument is read on, and a condition copy of the unnest
  joins after the expansion instead of before it. The keyless-join guard no
  longer exempts an unnest carrying its parent's columns.
  `_pin_input_placements`' FD test and the declared-keys test it replaced
  agree on every placement in 2,314 grain-pin, unnest and partial tests
  (4,377 calls, 20 with a host); no distinguishing shape was found.
  Two mismatches the family found on random seeds were its own fixture: the
  base `events` table binds `group_id` complete while a random group has no
  event, and a `~` merge of that key licenses padding the group. With honest
  data both spellings agree (`tests/engine/test_subset_merged_key_derivation.py`).

- **Predicate pushdown and the NOT NULL strip undid each other**
  (`Fixpoint.ungated_cycles`). Pushdown pushed `tree_id is not null` into a
  scan whose `tree_id` cannot be NULL; `StripRedundantNotNull` dropped it; the
  next pass pushed it again, so every such query ran the optimizer to its pass
  cap (issue #830). Pushdown now skips an atom the stripper would drop, judged
  by the stripper's own predicate (`Fixpoint.push_keeps`); the driver now
  checks that no phase adds an atom a fact remover drops
  (`Fixpoint.strip_never_cycles`).
  The check found a second pair at once: pushdown pushed a rowset's existence
  filter into a CTE whose parents already enforced it, and
  `PredicatePushdownRemove` dropped it every pass. That remover judges atoms by
  other atoms, so it is outside the model; pushdown now gates on it too.
- **Aggregates filtered by an unnested element overcounted, and a `~` binding
  of the list's key cross-joined it.** `Unnest.lean` models a WHERE on an
  element as keeping a row iff one of its padded elements passes. On main, 248
  of 324 cases in `tests/unnest/test_unnest_filter_laws.py` fail.
  `select city, count(tree_id) where tree_eco_region = 3` returned `A 3` for
  one matching tree: the condition copy of the unnest read the list from a
  private scan without `species` and joined the trees `ON 1=1`
  (`Unnest.keyless_pair_keeps_every_row`). It now reads the shared scan, and
  an aggregate joins an unnest beside its rows on its input grain, not its
  grouping key (`Unnest.coarse_pair_overcounts`). A list repeating the
  filtered value (`[3, 3]`) counted its tree twice: the input dedup skipped a
  column the WHERE pins, which an element is not unique on
  (`Unnest.pinned_element_repeats`, `Unnest.expansion_sum_overcounts`), and the
  FINAL's element filter squared the repeats (`Unnest.self_join_squares`).
  Spelled as the key the element is merged into, every aggregate raised
  "could only be placed after the aggregates": the unnest now feeds the
  aggregate as a row parent and answers for the merged key in placement. A
  WHERE that a row with no element can pass (`e > 1 or city = 'B'`) dropped
  such rows, or padded a NULL row from the key's table
  (`Unnest.or_keeps_disjunct`, `Unnest.key_table_pads_a_row`). Any table
  binding `~species` made the unnest read a slice of the list alone, so even
  `select event_id, region_id` cross-joined every event with every region.
  The `merged_axis_pin` family adds an aggregate over the merged key and an
  OR beside it; the full fuzzer passes 3,132 of 3,132.

- **A summary answered one value under one spelling only.** `Cover.lean`
  names six rules for when a count summary covers a query, and
  `tests/discovery/test_summary_cover_laws.py` runs each rule's shapes under
  five filters. On main, 169 of the 190 coverable cases missed and 2 raised;
  now 96 miss, none raise and none answer differently. A rename
  (`species as s`) failed the "aggregate at its own grain" gate, which compared
  grains by address (`Cover.target_by_value`). A column listed twice was refused
  outright (`Cover.duplicate_target_slice`). A scalar of a stored column
  (`plant_year::int`) was never offered by the summary, because derivable
  concepts were walked only from expressions over raw columns. `plant_date.year`
  beside a stored `year(plant_date)` was bound by name only, so its WHERE ran on
  the tree rows and joined the summary count. `count_distinct(tree_id)` now reads
  as `count(tree_id)` wherever every grouping and WHERE column is a function of
  `tree_id` (`Cover.count_distinct_of_key_exact`); since 0.3.483 the two
  share a rollup signature instead, additive wherever the grain summed away
  partitions the key. A re-summed partial read
  inside an expression (`n / count(x) by *`) was put in the GROUP BY and failed
  to bind, as did a grand total joined beside it; both shapes raised on main.
  Filtered counts, totals under a WHERE the target does not determine,
  existence and relations stay misses, held by strict xfails. One shape
  disagrees on main and here: `sum(case when bucket = 'Native' then
  count(tree_id) by bucket end)` reads 8 from the trees and 4 from the summary.
- **Plans that one optimizer rule happened to repair, and padded plans that
  dropped or invented rows** (`Rewrite.lean`, 0.3.441). Disabling each rule
  over the fuzzer corpus (1,305 cases, 2,140 firings) changed no rows, but
  the TPC-DS corpus and a padded model checked against a pre-joined twin
  found eleven bugs, most on main and several masked by the rule that ran:
  - A null-sensitive value computed in a `~` scan padded to NULL
    (`pad_null_sensitive`): `where returned = false` (`returned <- rqty is
    not null`) lost every order with no return over a query or file table,
    while `select order_id, returned` said `false`. Inlining hid it over a
    plain table. `in`/`not in` count as null-sensitive (membership is total).
  - `having c > 3` beside a FROM column `c` filtered on the column and
    returned nothing (`alias_shadowed`); the alias is kept only over CTEs.
  - The FINAL WHERE read a filter-only argument its deduplicated contributor
    had projected away; pushdown moved the atom and hid it.
  - A filtered value computed per row upstream and grouped at its key's grain
    split a key into two groups (`grouped_filter_fans_out`): q97 with
    `merge_aggregate` off returned (54151, 28207) against the reference's
    (54150, 28203).
  - `where count(order_id) by cust_id = 0 select reason, ...` returned every
    row: the atom was a HAVING on the aggregate and the rows joined it
    preserving (`absent_accepting_keeps_rejected`). An atom true on an empty
    group is now restated at FINAL.
  - A gated dimension peel lost its join key (hidden by the gate, or never
    scanned), so `where count(...) by cust_id > 1 select amount, name` failed
    to plan.
  - A WHERE on an order column dropped the `~` on a notes table for the query,
    losing customers with no note (`fk_anchor_keeps_padded`): an order
    carrying the customer is an anchor, and a value it reaches through another
    `~` table keeps its row.
  - In an extent-free group a `~` lookup keyed by the span (notes beside
    customers) joined INNER and lost the customer's region; a `~` side whose
    WHERE rejects only on the shared key is not the WHERE's population; a
    merge preserving a complete binder of an extent-free span is not partial
    on it.
  - `CollapseSingleParent` folded a CTE into a table its consumer had already
    inlined, so one name meant two sources: a binder error here, and a silent
    wrong column where the names overlap.
  Not a bug: `query-3`'s narrowed `subset join` over data that breaks the
  declaration.
- **Keys a FINAL join never carried, `~` sides joined as if complete, and one
  partition arm read for the whole family** (`Rewrite.lean`, 0.3.442). The
  padded/pre-joined harness, swept over 1,200 seeds and a 270-shape family
  of two customer gates beside a row atom, found five bugs, all on main
  (28 of 138 suite cases and 125 of the 270 family shapes fail there):
  - A row-level filter argument beside a gate (`where rqty > 1 or
    sum(amount) by cust_id > 8 select name`) joined its producer ON 1=1, so
    any customer's return passed every customer (`cross_arg_leaks`). The
    argument's row stream now exposes the gate's grain it can carry, as does
    a dimension peel, and a sole contributor holding that key applies the
    WHERE before its dedup (`keyed_arg_exact`). The same demand fixes the
    pre-joined model's keyless-join failures from the last round.
  - A WHERE proof on the join key narrowed a FULL to INNER through a `~`
    lookup keyed by it (notes by customer), dropping the order whose customer
    has no note (`key_proof_keeps_lookup_gap`).
  - A FULL to a `~` side marked the complete side's key nullable, so a
    filtered aggregate beside a NULL-accepting row atom (`amount is null`,
    `not (x != k)`) was not the WHERE's population and every customer came
    back (`null_accepting_row_keeps_rejected`). A FULL whose `~` right side
    has a partner for every row is now the LEFT join at plan time
    (`subset_full_is_left`).
  - A FINAL or gated filter merge forced INNER toward its feeders even when
    the node was the bare `~` notes scan: the customer with no note was lost,
    and a gate hosted on the notes peel kept customers it rejected
    (`preserving_exact`). The merge now lets join typing decide where a feeder
    carries the key complete, and drops the node's partial stamp there.
  - TPC-DS q83 with its heal blocked returned no rows: the per-channel
    presence counts read the returns family off its CATALOG arm alone. An arm
    in two minted unions (its family, and a cover mixing sales and returns on
    `channel`) was mapped to the mixed one, so the prune that prefers the
    family union never fired.
  `merge_irrelevant_group_by`, `drop_identity_group`, the value-set upgrade,
  `fold_existence_into_aggregate` and `push_semi_join_into_aggregate` now have
  laws (`identity_group_exact`, `merged_group_sum_exact`,
  `matched_left_is_inner`, `existence_fold_exact`, `semi_join_exact`), each
  with a counterexample for its guard; the suite ablates each and checks the
  last four fire. Not a bug: `select reason, rqty` over the `~` returns reads
  the returns rows only, since no output carries an order's own attributes.
- **Summaries read past a WHERE on their own values, partials read as
  values, and atoms that mix grains placed by column** (`Rollup.lean`,
  `Rewrite.lean`, 0.3.443). Extending the rollup harness with families of
  filters the summary's grain does not determine (1,100 shapes in six
  positions, 700 in exotic ones: memberships, rowsets, windows, aggregates at
  other grains) found no plan that read a summary under a finer filter, but
  four bugs elsewhere, all on main:
  - A WHERE over the aggregate a summary stores at its own grain (`select
    city, sum(height) where sum(height) by city, species > 3`) was dropped:
    the scan routes only scalar atoms, yet the rollup pinned it as if it
    applied them (`Rollup.dropped_group_filter_overcounts`). Beside a joined
    dimension (`... > 3 or tree_form = 'linden'`) the merge re-summed the
    condition's aggregate to a grain that does not determine it and tested
    the sum (`Rollup.resummed_condition_misfilters`): a binder error, or
    wrong rows at `tree_form`. At a grain derived from a joined dimension
    (`case when tree_form = ...`) the atom ran as a semijoin on the output
    grain and kept the group it rejects. The summary rows are now filtered
    at their own grain, then re-summed (`Rollup.group_value_filter_exact`);
    a grain the summary does not bind reads the base rows.
  - A membership on a summary grain key (`species in (select ...)`) widened
    the scan to that key, and `sum(height) * 2` read each row's partial as
    the value (`Rollup.unsummed_partial_splits_a_group`): one row per
    species. A stored aggregate read below its own grain is now a partial.
  - A pre-aggregate column was peeled onto its dimension when a filter on it
    alone drops whole groups, judged per column. Inside an OR with an order
    column (`amount = 4 or note is not null`), or in a clause beside an
    aggregate conjunct, it then could not be placed (the pre-joined model
    raised on 296 of a 360-shape family), or it filtered nothing:
    `count(order_id) by region` beside `name is null or name = 'ann'`
    counted every order in the region
    (`Rewrite.push_group_mixed_atom_changes_sum`). The
    peel is now judged per atom. Keeping such a column on the row stream
    exposed a gate that holds on an empty group (`not (sum(amount) by
    cust_id > 6)`) hosted on the aggregate alone while the row stream padded
    the customer back with a count of 0; it is now re-applied at FINAL
    (`Rewrite.absent_accepting_keeps_rejected`).
  - A `by *` gate beside a nested aggregate (`select sum(sum(x) by city)
    where count(id) by * > 1`) failed to plan: the FINAL WHERE was pushed
    below the aggregate on what the group graph said its rows carry, not on
    the node built.
  An expression over an aggregate at a summary's exact grain (`sum(height) *
  2` beside `(city, species)`) now reads the stored column instead of the
  base rows, and `count_distinct` of a summary grain column reads the
  summary.

- **A window ranked the rows of a finer key, and NULLs sorted per engine.**
  `Windows.lean` evaluates a window over the rows of its own grain. On main,
  355 of 1,120 cases in `tests/windows/test_window_laws.py` fail.
  `select id, rank(g) over (order by g)` returned 25 rows for 7 ids and
  ranked `b` 5, not 2: the window read the `(id, g)` scan it shared with `id`
  and joined back on `g` (`Windows.row_stream_rank_counts_repeats`,
  `Windows.stream_join_fans_out`). The window's input dedup took its grain
  from its arguments' grains, and a property argument sits at its key; it
  now takes the window's own grain, and the FINAL reads an ancestor's
  columns through a window only where that grain determines them (TPC-DS
  q44's `rank(product_name)` beside `item_id` joins back instead).
  `count(x) over ()` was read as `count(x)`
  at the select's grain, one row's `x`; an empty `over ()` is now `by *`
  (`Windows.empty_over_is_total`). A plain `asc` rendered the direction
  alone, so SQLite, MySQL, SQL Server and BigQuery put NULLs first and
  DuckDB put them last; a rank, a `lead` or a running `sum` ordered on a
  nullable column then differed per engine
  (`Windows.bare_direction_misplaces`). A plain direction now places NULLs
  last on every engine, `nulls auto` keeps the engine's placement instead of
  rendering invalid `asc nulls auto`, and SQL Server, which has no NULLS
  FIRST/LAST, leads with a NULL flag term as MySQL did
  (`Windows.render_places`). A datasource `order by` for a file target
  rendered the ordering's text directly, bypassing the dialect, so `desc
  nulls auto` reached DuckDB as written and the write failed. The set
  operators held: 2,304 random `except` / `intersect` / `union` queries with
  NULLs and duplicates matched `SetOps.lean` on both engines.
- **Four scalar functions answered differently per engine** (`Scalars.lean`).
  The base `log(x, b)` rendered `log(x, b)`, which SQLite, Postgres,
  Snowflake and MySQL read base first, so `log(100)` was `0.5` there
  (`Scalars.base_first_misreads`), and DuckDB raised on a base that was not a
  literal. It now renders the change of base (`LOG(b, x)` on Snowflake,
  `LOG(x, b)` on SQL Server), and the base may be any number, not only
  an integer. SQLite's `+n months` let the day overflow, so
  Jan 31 plus a month was Mar 2, two months on by `date_diff`
  (`Scalars.overflow_breaks_diff`); it now builds the first of the target
  month plus the clamped day (`Scalars.first_plus_clamped_day_exact`). Every
  `date_add` was typed DATE while DuckDB and Postgres returned a timestamp and
  SQLite a datetime string, so on SQLite `date_add(d, day, 1) = e` was false
  for every row (`Scalars.stamp_text_misses_date`); a whole-day offset or
  truncation of a date now renders a date, an offset of a datetime is typed
  as its input, and a sub-day offset of a date is a DATETIME. `greatest` /
  `least` had no SQLite rendering and returned NULL on BigQuery, Snowflake,
  MySQL and Presto when any argument was NULL, unlike DuckDB and Postgres
  (`Scalars.strict_drops_present`); those now render each argument falling
  back to the others (`Scalars.fallback_greatest_exact`). SQLite also gained
  `strpos`. 55 of the 97 law cases fail on main.
- **A persisted copy rounded what the live query computed** (`Types.lean`).
  The planner answers from a datasource it built (`persist`), whose columns
  take each concept's declared type, so a declared type narrower than the
  engine's value is a silent wrong answer once the copy is read. `sqrt` and
  `**` were typed INTEGER (`sqrt(2.33)` stored `2`, and `2 ** -1` an
  integer); every `%` was INTEGER (`2.3333 % 2` stored `0`,
  `Types.fractional_rem_is_not_int`); `greatest` / `least` took their first
  argument's type (`greatest(-7, -0.6667)` stored `-1`,
  `Types.first_argument_type_drops`); a decimal beside a float merged to the
  decimal (`Types.decimal_beside_float_rounds`; a union still casts a float
  arm to the decimal, by design, so its sums do not drift), a product kept its operands'
  scale (`Types.product_scale_rounds`), and a decimal quotient its dividend's
  (`Types.third_has_no_scale`). DuckDB's and MySQL's `FLOAT` are 4 bytes, so a
  persisted float kept 24 bits: `1234567.891` read back `1234567.875`
  (`Types.single_misses_double`); a float now persists as a double on every
  dialect (Presto had no `float` type at all). SQLite's casts to a date or
  datetime rendered `cast(x as TEXT)`, which kept the time, so `t::date = d`
  was false; they now render `date(x)` / `datetime(x)`. SQLite's `floor` and
  `ceil` ran SQLAlchemy's Python `floor`, which raised on NULL, and the base
  `**` was valid SQL only on DuckDB (`power(x, y)` now; SQL Server casts the
  base to a float, since its `POWER` keeps the first argument's type).
  DuckDB's `date_trunc` to a day or coarser returned a timestamp for a
  DATE-typed concept and now returns a date. 49 of the 141 law cases fail on
  main.

- **A filtered aggregate never read a summary** (`Cover.lean` §2, extended).
  `count(x ? c)`, `sum`, `min` and `max` of `x ? c` always read the rows: the
  rollup matcher compared the aggregate's argument whole, and `x ? c` is not
  `x`. A summary storing `agg(x)` at a grain that determines `c`'s columns
  answers it as `reagg(case when c then agg(x) end)` over its rows
  (`filtered_sum_exact`, `filtered_min_exact`, `filtered_max_exact`,
  `case_count_exact`; `coalesce(.., 0)` for a count, `uncoalesced_case_count_null`).
  The matcher now pairs the filtered aggregate with the stored one
  (`BuildConcept.filtered_rollup`, `BuildDatasource.filtered_column_for`) when
  the summary binds every column `c` reads, and the rollup render emits the
  CASE where it first re-aggregates the stored column; a re-aggregation of
  partials already filtered below is a plain one. The query's concepts are
  never rewritten. A count over the filter's rows guards its fan-out on the
  rows of `x`, not of `x ? c`. `count_distinct(k ? c)` follows, through
  `rollup_operator`. A summary storing `agg(x ? c)` itself is read as
  stored, with no CASE on top: it answers a condition on any column, a
  property of its key included (`test_stored_filtered_aggregate_is_read_as_stored`),
  and now beside a target that is such a property too, which read the rows on
  main. Serving it exposed three planner bugs. A nested
  partial grouped by a stored derived column (`count(x) by species,
  plant_year` against a stored `year(plant_date)`) rebuilt that column from
  the facts and joined it back onto the summary. Two walks read the
  statement's stale lineage after `_reaggregate_partials` had rewritten it
  (`count(x)` read as the `sum(count(x) by k)` it became): `_nested_rollup_roots`
  marked the old aggregate a summary root while the render used the new one,
  and `_summary_leaves` saw the old one's row columns and declined the summary.
  Resolving the second also serves the rule-4 totals beside a rename, a scalar
  or a `coalesce` of the key under a finer WHERE (the dashboard's
  `species:sp-top-species`), with no wrong answers, including over two species
  sharing a display name (`test_target_shared_by_two_keys`). The render of a
  rolled-up concept now takes the planned rollup, not a parent's stale
  spelling of it: `count_distinct(k ? c) / count_distinct(k)` raised a
  `KeyError`. `test_filtered_rollup_laws.py` (1,440 cases over seeded random
  trees with NULLs): all read the trees on main, 0 wrong; now 960 read the
  summary, and the 480 whose condition reads a property of the summary key
  (`bucket` of `species`) still read the trees, which `test_filtered_aggregate_answers`
  holds to the reference. The cover suite's 225 coverable cases: 102 miss on
  main, 72 now, none wrong, none raise; every open shape is now also held to
  the tree rows' answer.

- **A filtered aggregate whose condition reads a property of the summary key
  read the rows, and its CASE spelling raised** (`Cover.lean` §2, a joined
  condition). `count(tree_id ? bucket = 'Native')` beside a `(city, species)`
  summary read the trees: the summary does not bind `bucket`, and the scan
  that re-aggregated the stored column could not see it. A column of `c`
  that a functional lookup joins onto the summary's rows is a function of the
  key (`joined_case_exact`, `joined_case_count_exact`); a lookup that fans a
  row out double counts (`joined_fanout_double_counts`), and a re-sum below
  the CASE is exact only at a grouping that determines `c`
  (`resum_then_case_exact`, `case_above_coarser_grouping_misreads`). The
  source search now prices that plan: a summary binding `agg(x ? c)` off a
  stored `agg(x)` owes the columns of `c` it lacks (`SourceCandidate.owes`),
  and a new obligation, `JOINED`, requires an in-cover functional chain off
  its rows to a full binder of each (the walk `labelable` uses), minted with
  no satisfiers when none exists, so that binding is then no answer. The cost
  order decides the rest: a table storing `agg(x ? c)` itself owes nothing and
  wins on sources, with no precedence rule. The emitter reads a joined scan at
  its own grain and re-aggregates at the merge, where the CASE sees `c`; the
  render is unchanged (the CASE goes on at the first re-aggregation and
  raises if a column is missing). A grand total `agg(x ? c)`, and every
  rollable total beside it, now drives the search instead of being a cross
  joined scalar. Under a WHERE finer than the target the pinned
  `_plan_joined_filter_rollup` serves it, joining the filtered aggregate's
  columns and a target that is a root property keyed in the summary's grain
  when every aggregate groups by it (one at a coarser grain beside a
  condition column is not regrouped by it: the first version did, and
  `test_rollup_resolution_laws.py`'s `or_form` caught a split group). Its
  lookups are now one request per key set: two dimensions on different keys
  of the grain were one request the search bridged through the summary
  itself, a self-join. thelook's query 16 now reads `user_product_sales`
  joined once to `users` and once to `products` instead of `order_items`.
  `agg(case when c then x [else null] end)` is the same aggregate
  (`Cover.filtered` is that CASE row by row): discovery reads either
  spelling as the pair `(x, c)` (`aggregate_filter`), in the matcher, the
  joined columns, the fan-out guard, the distinct-count rewrite and the
  rollup signature, so a summary storing one spelling answers the other.
  `count(case when c then x end)` raised at parse on main
  (`_is_constant_count_argument` did not know a CASE branch).
  `test_filtered_rollup_laws.py` now runs every case in both spellings
  (2,880 per test): on main 480 answers raise (every CASE-spelled count) and
  1,920 read the trees; now all answer and read the summary. The stored-table
  cases (320, both spellings stored and asked): 240 fail on main, 0 now. The
  cover suite's 30 open filtered shapes (`property`, `or`, `ratio`,
  `beside_plain`, `list_property`, `distinct/filtered_ratio`) are served, none
  wrong. The tree_reporting dashboards' `summary:local-native-share` and
  `species:sp-biome-fit` (seven pinned cases) read the summary.

- **A group by an unnested element counted a row once per repeat, and a
  count by a related value never read a summary** (`Cover.lean` §6, the
  pairs a relation is joined on). `select ecoregion_id, count(tree_id)` said
  ecoregion 1 holds 6 trees where `select count(tree_id) where ecoregion_id = 1`
  said 5: a species listing it twice (`[1, 1]`) counted its tree twice
  (`Cover.repeated_element_counts_once`), and `sum(dbh)` likewise. The
  aggregate's input dedup to `(tree_id, ecoregion_id)` was elided because the
  unnest's rows claim that grain, which a list repeating an element breaks;
  `by biome` deduped and answered 5. The dedup is now kept, and built even
  where the parent claims the grain, whenever the grain pairs a row key with
  one of its list's elements; a literal `unnest([1, 2, 2])` has no parent row
  and keeps its repeats as rows (`test_duckdb_filter.py::test_filter_count`).
  On main, 192 of 576 grouped cases in `tests/unnest/test_unnest_filter_laws.py`
  answer wrong; none now. With that settled, rule 6 is served:
  `select ecoregion_id, count(tree_id)` (or `biome`, or the plain element
  `tree_eco_region`, beside other summary columns) beside a `(city, species)`
  summary reads the summary LEFT-joined to the DISTINCT
  `(species, ecoregion_id)` pairs and re-sums
  (`Cover.distinct_pairs_resum_exact`, `Cover.relation_resum_count_exact`);
  a merged key FULL-joins them, as the tree plan does, so a region only a
  species with no trees lists still answers 0. `trilogy/core/processing/relation_rollup.py`
  names a related target (an element of a list keyed in the summary's grain,
  a key merged with it, or a property of such a key) and checks the rollup at
  the grain with each one replaced by its list's keys, under the usual
  guards. `tests/discovery/test_relation_rollup_laws.py` (721 cases over
  random repeated, empty and NULL lists; three spellings, six shapes, four
  filters) answers as the trees everywhere; on main its 170 servable cases
  read the trees. A table storing the count by `ecoregion_id` itself is read
  as stored, with no relation join; on main 9 of its 10 cases disagree with
  the trees, whose plan double counted a repeated element. A list the model
  declares `::unique` (`std.collection`) repeats no element, so its pairs are
  already distinct (`Cover.unique_list_pairs_are_distinct`): the grouping and
  the relation join drop their dedup, `trilogy validate` reports a list that
  breaks the claim, and over honest data the declared plan answers as the
  undeclared one with one GROUP BY fewer (576 grouped and 180 relation cases). The cover suite's `relation/*` shapes are served with no
  WHERE. Under a WHERE they still read the trees, correctly: the group graph
  filters the list's parent rows, not a rolled root.

- **A group by a window's value grouped by the window's inputs instead**
  (`Windows.lean`, grouping by a window's value). With
  `auto l <- lag(x) over (order by id)`, `select l, count(id)` returned
  `l = 10` twice, with the counts of `x = 10` and `x = 20`: a window in a
  select gave the statement its keys in its place, so the count was by `x`.
  A value window is no function of its operand
  (`Windows.lag_reads_rows_not_values`), and even a rank, which is a
  function of its tuple, splits a rank into a group per tuple
  (`Windows.group_by_tuple_splits_value`); grouping by the value is the
  re-sum over the tuples of one value (`Windows.regroup_by_value_exact`).
  A window now contributes itself to a select's grain (one ordered by an
  aggregate, which inherits that grain, still contributes its keys); its
  keys include its order columns (`rank(g) over (order by x)` ranks
  `(g, x)`, and `lag(x) over (order by id)` is a property of `id`, not of
  `x`); and the FINAL dedups a window contributor whose rows are finer than
  the select (`select g, rank(x) over (order by g)` repeated `(g, rank)` once
  per `x`), except one ordered by an aggregate, whose rows already sit at the
  select's grain. On main, 644 of 832 cases in `tests/windows/test_window_group_laws.py`
  (value and numbering windows, partitioned or not, grouped alone, beside a
  partition column or beside the operand, as an `auto` and inline) answer
  wrong; none now.

- **A window's order aggregate was given a grain of its own** (`SelectGrain.lean`,
  `Windows.lean`, an aggregate a window orders by). An aggregate with no `by`
  is at the select's raw grain wherever it appears, exactly as if written
  `by` it (`SelectGrain.bare_is_select_grain`). The build pinned one in a
  window's ORDER BY to the window's first argument, so `rank(g) over
  (partition by p order by sum(y))` in `select p, g, ...` summed `y` per `g`
  over every partition (`SelectGrain.first_argument_pin_misreads`,
  `Windows.anchor_sum_crosses_partitions`). 0.3.459 moved the pin to the
  tuple the window ranks, which is the select grain only when the select is
  no finer: under `select g, p, rank(g) over (order by sum(y))` it ranked by
  the total per `g` while `sum(y)` projected beside it was per `(g, p)`
  (`SelectGrain.window_tuple_pin_misreads`). 0.3.462 removes the pin: the
  aggregate resolves at the select grain like any other, nested in an
  expression, in a CASE or behind a name alike
  (`SelectGrain.bare_expression_on_grain`). A window ordered by one is then a
  value per select-grain tuple, and its keys are the select grain: with only
  its arguments as keys, the summary plan joined `select city, species,
  rank(species) over (order by count(tree_id))` back on `species` and gave a
  species in two cities both ranks. A window ordered by an aggregate `by`
  fixed keys (or a `group ... by`) is the same in every select
  (`SelectGrain.specified_ignores_select`) and stays a value the select
  groups by (0.3.459). Its keys include those `by` keys, named or inline: the
  v2 parser did not count a bare name in `over (order by s)` as a dependency,
  so the window hydrated before `s <- sum(y) by g, p` and kept an
  `UndefinedConcept`, its keys came out `{g}`, and `select id, r` gave a row
  up to three ranks (`Windows.order_keys_dropped_fans_out`). The 0.3.459
  suite and the rollup suite's window oracle had encoded the tuple pin; both
  now state the select grain. TPC-H `adhoc03` (`where rank customer by
  sum(total_price) desc <= 10 select count(order.id)`) had relied on the
  first-argument pin: its select has no keys, so the sum is the total
  (`SelectGrain.bare_empty_grain_is_total`) and every customer ties; it now
  writes `sum(total_price) by customer`. On main, 144 of 256 cases in
  `tests/windows/test_window_order_aggregate_laws.py` and 22 of 416 in
  `tests/windows/test_window_order_expression_laws.py` (a `by g, p` order
  named or inline, per row, alone or beside `count`; a projected `sum(y)`
  beside the rank ordered by it; a WHERE gate) answer wrong; none now.

- **An aggregate `by` fixed keys stood in for its keys in a select**
  (`SelectGrain.lean`, "An aggregate `by` fixed keys in a select"). Such an
  aggregate is a value of its keys (`SelectGrain.fixed_on_keys`); the select
  groups by the value, and its rows are the distinct tuples it projects
  (`Rows.lean`). The group check counted the aggregate's `by` keys as covered
  by any target holding a non-aggregate column, so `select g, sum(y) by id
  as s, count(id)` skipped its GROUP BY and counted 1 per id where two ids
  share `(g, s)` (`SelectGrain.by_keys_split_tuple`); the same select with
  `s <- y + 0` counted 2. The check no longer counts them for a plain
  aggregate (a rowset handle over one still anchors its rows). The FINAL
  skipped its dedup for any grouped contributor, so `select g, sum(y) by id
  as s` returned one row per id and repeated `(g, s)`
  (`SelectGrain.fixed_value_rows_distinct`); it now dedups one whose grain the
  projected tuple does not hold. That dedup re-summed every rollable aggregate it carried,
  right for a summary scan's partial and wrong for a value at its own grain:
  `where ... select g having (count(id) by g, h) > 1` re-summed the count to
  `g`. It now re-sums only a partial (`pending_rollups`). TPC-DS q23, q30,
  q65, q79, q81 and q91 grew a dedup: each drops a key its aggregate is `by`
  (q91's call-center loss is `by` marital status, which the select does not
  project), so two key tuples can share a projected tuple. All answers match
  the reference. On main, 48 of 240 cases in
  `tests/core/test_fixed_aggregate_select_laws.py` answer wrong (16 counts
  and sums beside the value, 32 repeated tuples); none now.

- **A fixed aggregate lost its value after it was selected**
  (`SelectGrain.lean`, "An aggregate of a fixed value";
  `tests/core/test_fixed_value_lifecycle_laws.py`). `s <- sum(y) by g` is a
  value of `g`, so an aggregate over it reads it once per `g` in each select
  tuple (`SelectGrain.ofValue`), and so does one over any scalar of it
  (`SelectGrain.scalar_of_fixed_on_keys`). Four places read it otherwise:
  - The input grain of an aggregate over an inline expression
    (`_aggregate_input_grain`) skipped an inner aggregate and kept its keys
    only when a scalar of them sat beside it, so `select h, sum(s + 0)`,
    `sum(s::int)` and even `sum((sum(y) by g))` (parentheses alone) read `s`
    once per row: 16 where `sum(s)` gives 11
    (`SelectGrain.fixed_value_per_row_double_counts`). The keys are now kept.
  - The input-grain dedup (`_reads_inner_aggregate_rows`) recognised an
    aggregate argument but not a scalar of one, so `sum(t)` with `t <- s`
    (or `t <- s + 0`) beside `g, h` was not deduped to `(g, h)` and double
    counted. `_function_of` now holds an aggregate `by` keys within the
    grain to be a function of it.
  - A rowset member of an aggregate keeps the METRIC purpose, and its grain
    is itself, so `concept_is_relevant` read it as covered by the selection
    (by itself) and dropped it from the outer grain: `rowset r <- select s
    as v; select r.v, count(r.v)` counted the whole rowset on every row
    (`SelectGrain.rowset_member_dropped_totals`).
  - A persisted `select g, h, s` stores the value once per `(g, h)`. The
    rollup matcher read any stored column with `s`'s signature as a partial
    at the table's grain, so after the persist `select g, s` re-summed it
    over `h` (`b`: 3 became 6), a stored `sum(y) by id` answered `sum(y)`
    in place of the stored partial beside it, and `max(s)` read the re-sum.
    A column `by` keys the table's grain does not fall within is no longer a
    partial (`BuildDatasource.holds_partial`), and a table storing the
    requested value itself is not rolled up for it.

  On main, 87 of 2,792 cases answer wrong (40 aggregates of the value in
  four spellings, all 8 rowset members, 39 of 64 persist round trips); none
  now. WHERE and HAVING over the value, and rowsets of it, held on main
  (0 of 1,920).

- **ClickHouse read a NULL as its type's default** (`Padding.lean`;
  `tests/engine/test_clickhouse_null_laws.py`). ClickHouse types are NOT
  NULL unless wrapped in `Nullable`, and its defaults fill a missing value
  with `0` or `''`. Running the window and select-grain law suites on chdb
  found five places the dialect let that through:
  - An outer join padded with `0`: `select id, count(oid)` counted 1 order
    for a customer with none, and `max(amt)` over negative amounts was 0
    (`Padding.default_pad_counts_unmatched`, `Padding.default_pad_max_raises`).
    A statement with an outer join now ends `SETTINGS join_use_nulls = 1`
    (`ClickhouseDialect.render_settings`), which also reaches its CTEs.
  - `lag`/`lead` past a partition's edge returned `0` for a NOT NULL
    operand; the operand is now `toNullable` (36 window cases).
  - `x::int` raised on any NULL, as `CAST(x AS Int64)` targets a NOT NULL
    type; a scalar cast now targets `Nullable(T)` (`Padding.cast_keeps_null`).
  - A NULL-safe join key `a = b or (a is null and b is null)` has no join
    key ClickHouse can find, so it refused every join on a nullable key
    (12 select-grain cases, 70 window-group cases); it now renders `a <=> b`.
  - A `date`/`datetime` constant was bound as a parameter, which reaches
    ClickHouse as a string: `date_trunc(t, month)` raised. Those types now
    render as typed literals (`INLINE_PARAM_DATATYPES`).

  On main, 132 ClickHouse cases fail across the suites (14 of 28 in the
  NULL suite, 36 window, 70 window-group, 12 fixed-aggregate); none now.
  The window-order and fixed-value lifecycle suites held on ClickHouse.

- **ClickHouse and SQLite broke the function and type laws** (0.3.466;
  `Functions.lean`, `Types.lean`). The function, scalar and type law suites
  now run on chdb too; their harnesses had no ClickHouse DDL. On ClickHouse:
  - `f::int` truncated (`25.8` was `25`) and `round(f, 0)` of a float was
    half to even (`-2.5` was `-2`) (`Functions.trunc_cast_is_not_round`,
    `Functions.half_even_cast_is_not_round`). A float now rounds as
    `sign(x) * floor(abs(x) * 10^n + 0.5) / 10^n`
    (`Functions.sign_round_is_round`), and a cast of a float or a decimal to
    an integer rounds first.
  - `date_diff(a, b, week)` counted Monday boundaries (Sunday to Monday was
    1) (`Functions.monday_boundaries_is_not_week_diff`); it is now whole
    7-day spans of the day difference.
  - `n % 3` of a decimal took the remainder of its unscaled integer
    (`7.5 % 3` was `0`, `7.6 % 3` was `0.0001`)
    (`Types.integer_rem_drops_fraction`); it renders
    `x - m * trunc(x / m)` (`Types.rem_by_trunc`).
  - `greatest(i, f)` of an integer and a float raised; the integers are
    widened to the float the expression is typed.
  - `date_trunc(t, day)` was a DateTime, typed DATE; it is now `toDate`.
  - A persisted `numeric(p, s)` column was created as `Decimal64(4)(p,s)`,
    which raises; `numeric(p, s)` now renders `Decimal(p,s)`
    (`NUMERIC_TYPE_NAME`).

  On SQLite, `%` casts both sides to integers, so `f % 3` and `n % 3` dropped
  the fraction (`2.79 % 3` was `2`; `Types.integer_rem_drops_fraction`); a
  fractional operand now renders `mod(x, m)`. The copy suite compared a live
  value to its stored copy, which share the error, so
  `test_remainder_keeps_the_fraction` holds `%` to a reference.

  On main, 49 cases fail: 16 of 81 ClickHouse function cases, 25 of 82
  ClickHouse type cases, and 8 of 12 SQLite remainder cases; none now. The
  scalar suite held on ClickHouse (48 cases).

- **`x / 0` had five answers, and a persisted copy forgot its NULLs**
  (0.3.469; `NullReject.lean`). A zero divisor gave `inf` or `nan` on DuckDB,
  `inf`/`nan` or an error on ClickHouse (a float or a decimal), an error on
  Postgres, NULL on SQLite and MySQL, and 0 on BigQuery and Snowflake. `x / 0`
  and `x % 0` are now NULL on every dialect: the divisor renders
  `nullif(y, 0)` unless it is a nonzero literal, and BigQuery renders
  `safe_divide` (`NullReject.guarded_div_strict`). Following from it:
  - the nullability inference took `/` as total, so `x / y` of non-null
    columns was non-null; on SQLite, where `/ 0` was already NULL, a persisted
    `q <- x / y` read back under `q is not null` kept the NULL row
    (`NullReject.div_zero_not_total`). `/` and `%` are now
    `NULL_INTRODUCING_FUNCTIONS`, except by a nonzero literal, which keeps
    `x / 100` as nullable as `x` (`NullReject.literal_divisor_total`).
  - BigQuery's `coalesce(safe_divide(x, y), 0)` read a NULL dividend as 0
    (`NullReject.zero_fill_not_strict`), and Snowflake's `div0` a zero
    divisor (`NullReject.div0_zero_is_a_value`). Neither has an engine in CI;
    `tests/dialect/test_function_law_renderings.py` pins the renderings.
  - BigQuery has no `%` operator; `%` renders `MOD`.

  A persist declared every column of the datasource it creates non-null,
  whatever the select wrote: a `?` column, a quotient, `x + 1` of a `?` column,
  `sum(v)` over a `~` join. A read of the copy then dropped `x is not null` and
  returned the NULL rows, on every engine
  (`NullReject.undeclared_copy_keeps_null`); on ClickHouse the column was NOT
  NULL and stored the NULL as 0, so `count(x)` counted it
  (`NullReject.not_null_column_stores_zero`). A created datasource now binds
  `?` every column the planned write can hold NULL in (any CTE's nullable
  columns and the outputs derived from them, `written_nullable`;
  `NullReject.inferred_copy_sound`), and ClickHouse declares those columns
  `Nullable` (`ColumnInfo.nullable` now reads the `?`; it read `optional`,
  which nothing sets).

  On main, 24 of the 32 cases of `tests/core/test_null_division_laws.py`
  fail: every DuckDB and ClickHouse zero-divisor case, every persisted-copy
  case on all three engines, the nullability case and the ClickHouse DDL
  case; none now.

- **`like`, `ilike` and `contains` meant different things per engine, and a
  ClickHouse string lost its backslashes** (0.3.470; `StringMatch.lean`).
  `trilogy/core/like_patterns.py` now states one pattern language: `%`, `_`,
  and a backslash that makes the next character literal. `like` keeps case,
  `ilike` folds it, `contains` is a case-folded substring test, and a NULL
  operand gives NULL. Every dialect renders that:
  - SQLite's LIKE folds ASCII case, so `v like 'A%'` matched `abc`
    (`StringMatch.sqlite_like_folds_case`). `like` renders `GLOB` there: a
    literal pattern is translated at compile time (`to_glob`,
    `StringMatch.glob_sound`), a computed one by a `replace` chain.
  - DuckDB and SQLite had no LIKE escape, ClickHouse, Postgres, MySQL and
    BigQuery escape with a backslash, so `a\%c` matched `a\xc` on one engine
    and `a%c` on the other (`StringMatch.unescaped_wildcard_matches_more`).
    Every dialect that takes one renders `ESCAPE '\'`, and a literal pattern is
    written with its specials escaped (`canonical`,
    `StringMatch.escaped_literal_exact`; on SQL Server `[` too). ClickHouse
    keeps the backslash before an ordinary character
    (`StringMatch.clickhouse_keeps_ordinary_escape`), so a computed pattern is
    rewritten there (`StringMatch.ch_strip_sound`).
  - `ilike` rendered verbatim on BigQuery, SQL Server, Presto and Trino, which
    have no ILIKE; it is `lower(x) like lower(p)` there.
  - `contains(s, t)` was case-insensitive on DuckDB, SQLite and BigQuery,
    case-sensitive on ClickHouse (`StringMatch.clickhouse_contains_keeps_case`)
    and Snowflake, and `CONTAINS(s, t)`, not a substring function, on Postgres,
    SQL Server, Presto and Trino. Each now folds case
    (`StringMatch.contains_is_folded_infix`); SQL Server, whose `CHARINDEX`
    finds no empty needle, matches `'%' + escaped(t) + '%'`.
  - ClickHouse reads a backslash in a string literal as an escape, and its
    renderer and chdb's parameter inlining wrote one undoubled: `where v =
    'a\b'` matched nothing, `'\x'` failed to parse, and a persisted `'\n'` was
    stored as a newline (`StringMatch.backslash_literal_halved`). Both now
    double it (`StringMatch.ch_literal_roundtrip`).
  - The executor's colon escaping read `\'` as an escaped quote on every
    dialect, so `ESCAPE '\'` on DuckDB or SQLite swallowed the next bind
    parameter. It reads a backslash as plain on standard-SQL dialects
    (`STRING_LITERAL_BACKSLASH_ESCAPES`).

  A pattern with no wildcard is that string's equality
  (`StringMatch.exact_pattern_is_eq`), and the parser now plans it as one:
  `city like 'Boston'` read the whole table beside a partition whose
  `complete where` is `city = 'Boston'`, and now reads the partition.

  On main, 25 of the 45 cases `tests/core/test_string_match_laws.py` runs
  fail: every literal and computed-pattern case on all three engines, the
  ClickHouse `contains` case, SQLite's NULL case, the three equality-planning
  cases, the persisted copy on all three engines and three ClickHouse literal
  cases; none now.

- **String functions counted bytes on ClickHouse, read positions per engine,
  and replaced or extracted differently** (0.3.471; `Strings.lean`). DuckDB
  and SQLite agree on every `substring(s, start, n)` of a 17-by-17 grid of
  positions, so they are the reference; every other dialect renders it:
  - ClickHouse's `length`, `substring` and `position` and MySQL's `LENGTH`
    count UTF-8 bytes: `len('日本')` was 6, and `substring('日本語', 3, 2)`
    split a character (`Strings.byte_length_counts_bytes`,
    `Strings.byte_position_counts_bytes`). They render `lengthUTF8`,
    `substringUTF8`, `positionUTF8` and `CHAR_LENGTH`; ClickHouse's `upper`
    and `lower` were ASCII-only and render `upperUTF8` / `lowerUTF8`.
  - Positions outside `start ≥ 1, n ≥ 0` meant different things: ClickHouse
    read start 0 as empty and a negative count as "all but the last"
    (`Strings.clickhouse_zero_start`, `Strings.clickhouse_negative_count`,
    112 of the 289 grid cells), and Postgres and SQL Server read a negative
    start as a window before the string (`Strings.standard_negative_start`).
    Every engine agrees once `start ≥ 1` and `n ≥ 0`, so a dialect that is not
    the reference clamps other positions into that form
    (`render_window_substring`, `Strings.clamp_exact`); a positive literal
    start and a non-negative literal count render as before.
  - `regexp_replace` replaced only the first match on DuckDB and Postgres
    and every match elsewhere (`Strings.replace_first_only`); both now pass
    `'g'`.
  - ClickHouse's `extract` ignored the group asked for, returning group 1 for
    `regexp_extract(s, p, 2)` and for `0` (`Strings.clickhouse_extract_ignores_group`);
    an explicit group renders `regexpExtract(s, p, g)`.
  - On dialects with no engine in CI: BigQuery's bare `TRIM` strips every
    whitespace character (`Strings.bigquery_trim_strips_tabs`), so it trims
    `' '`; SQL Server has no `length` or `STRPOS` and its `LEN` drops trailing
    spaces, so length is `LEN(s + 'x') - 1` (`Strings.marker_len`) and
    `strpos` is `CHARINDEX` with an empty needle found at 1
    (`Strings.charindex_misses_empty`); Snowflake has no `STRPOS` and renders
    `POSITION`.

  Two plans got smaller. `like '%'` keeps every non-NULL string
  (`Strings.percent_matches_all`), so the parser states it as `is not null`,
  which the NOT NULL strip can drop. And the strip now drops
  `x is not null` beside a sibling of the same AND that rejects NULL in `x`
  (`x = 'b'`, `x like 'a%'`, an OR of such): `Strings.sibling_rejects_null`.

  On main, 12 of the 52 cases `tests/core/test_string_function_laws.py` runs
  fail: every ClickHouse length, substring, strpos, case, `regexp_extract` and
  persisted-copy case, DuckDB's `regexp_replace`, and four planning cases;
  none now.
- **A literal folded by the parser and a cast rendered per dialect disagreed
  with the engine** (0.3.472; `Casts.lean`). The parser folded `null = null`
  to TRUE and `1 = null` to FALSE, where a column holding the same values
  compares UNKNOWN (`Casts.null_eq_null_folded_true`,
  `Casts.literal_substitution_changes`); `where null = null` kept every row.
  A CASE whose every arm folded false and had no ELSE rendered `CASE END`,
  which no engine parses (`Casts.empty_case_rendered`); it is NULL now
  (`Casts.no_arm_is_null`). `'t'::bool` and `'1'::bool` folded to FALSE
  (`Casts.capitalize_fold_misreads_t`), and `'3.14159'::numeric(10,2)` folded
  to all five digits, which SQLite could not bind; the fold now reads a bool
  by `trilogy.core.casts.text_to_bool`, refuses a string with no reading,
  and rounds a decimal half away to its scale. `1 < null` raised a
  `TypeError` at parse.

  A cast to `numeric(p, s)` truncated on ClickHouse (`1.005` was `1.00`;
  `Casts.clickhouse_rescale_truncates`) and kept every digit on SQLite
  (`Casts.sqlite_keeps_digits`); both round half away now. SQLite read
  `'true'` as `0` (`Casts.sqlite_true_is_false`) and kept `2::bool` as `2`,
  which `= true` misses (`Casts.sqlite_int_bool_keeps_value`); BigQuery and
  Presto refuse `'t'` and `'yes'` (`Casts.bigquery_refuses_t`); a bool cast
  to a string was `'1'` on SQLite, MySQL and SQL Server
  (`Casts.bool_text_is_digit`). Each renders the reference now. Postgres has
  no `string`, `bytes` or `double` type, SQL Server no `string`, `bool`,
  `double` or `timestamptz`, and MySQL casts to `SIGNED`, not `INTEGER` or
  `BOOLEAN`; each now names a type it has.

  `sqrt(-1)` was `nan` on DuckDB and ClickHouse and NULL on SQLite, and
  `log(0)` was `-inf` (`Casts.nan_is_not_null`); both are NULL outside their
  domain on every dialect (`Casts.domain_guard_exact`), and a base of 1 is a
  NULL change of base. Both functions were listed total, so a persisted copy
  of `sqrt(i)` declared its column non-null, and reads of it dropped
  `r is not null` and returned NULL rows; they are NULL-introducing now
  (`Casts.domain_null_introducing`).

  On main, 49 of the 83 cases `tests/core/test_cast_laws.py` runs fail; none
  now.

- **A string cast to a number or a date read differently on every engine**
  (0.3.473; `Readings.lean`). ClickHouse's `CAST` read `' 12 '`, `'1.5'` and
  `'1e3'` as an integer as NULL (`Readings.clickhouse_refuses_space`); SQLite
  read the longest numeric prefix, so `'abc'` was 0, `'1e3'::int` was 1 and
  `'12abc'` 12 (`Readings.sqlite_reads_prefix`), and truncated `'2.5'::int`
  to 2 (`Readings.sqlite_truncates_fraction`); MySQL read a prefix as SQLite
  did; DuckDB raised on any string with no reading (`Readings.duckdb_raises`).
  `'2020-02-30'::date` was March 1st on SQLite and ClickHouse and
  `'2020-13-01'` was 1970-01-01 on ClickHouse
  (`Readings.rolled_day_is_not_the_day`,
  `Readings.clickhouse_month_thirteen_is_epoch`), and SQLite read `'1.5'` as a
  Julian day (`Readings.sqlite_julian_day`). One reading
  (`trilogy.core.casts.text_to_number`, `text_to_date`) is rendered now: a
  pattern guard (`regexp_full_match`, `match`, `REGEXP`, and on SQLite a
  conjunction of GLOBs, `Readings.glob_number_exact`) around the engine's own
  read, and NULL for a string with no reading, a bool spelling included. A
  date is DuckDB's `YYYY-M-D` or `YYYY/M/D` with an optional time, as
  TPC-DS's `'1980/5/3'::date` birth dates need; a float also reads `inf` and
  `nan`. The guard reads its operand as text, since a `string` concept can be
  bound to a column the engine holds as a date (TPC-DS's `_date_string`). The
  parser's fold of a literal reads the same way (`'1.5'::int` and
  `'1e3'::int` raised a Python `ValueError`) and still refuses a literal with
  no reading. A folded infinite float rendered `CAST('inf' AS REAL)`, which
  SQLite reads as 0 (`Readings.sqlite_text_infinity_is_zero`); it renders
  `9e999` there.

  The cast was listed NULL-strict and not NULL-introducing, so a persisted
  copy of `s::int` declared its column non-null and `x in S` over a set of
  reads rendered plain equality, dropping a NULL probe that matches the NULL
  member by identity (`Readings.null_free_claim_drops_match`); both read it as
  NULL-introducing now (`Readings.read_null_introducing`). The membership test
  found a ClickHouse bug of its own: its decorrelation of `exists (... where m
  <=> x)` lost the NULL match, so `i in m` over any nullable set dropped a
  NULL `i` and `not in` kept it. ClickHouse renders an uncorrelated `IN`
  with an explicit NULL-member check now.

  On main, 84 of the 120 cases `tests/core/test_text_read_laws.py` runs
  fail; none now.

- **A string cast to a datetime read differently on every engine, and a
  number past its target's range saturated or raised** (0.3.474;
  `Instants.lean`, `Readings.lean`). A string cast to `datetime` or
  `timestamp` was a bare CAST: DuckDB raised on any string with no reading,
  and its TIMESTAMP cast dropped a zone, so `'12:34:56+02:00'` was 12:34:56
  where SQLite and ClickHouse read 10:34:56 UTC (`Instants.duckdb_drops_zone`);
  a zoneless string cast to TIMESTAMPTZ was read in the session's zone.
  `'2020-01-05 24:00:00'` was the next midnight on DuckDB and ClickHouse and
  the text `24:00:00` on SQLite (`Instants.hour_24_is_not_a_clock`);
  `'2020-02-30 00:00:00'` was March 1st on SQLite
  (`Instants.rolled_instant_is_not_the_instant`); ClickHouse read
  `'2147483648'` as seconds since 1970, `'1.5'` as a day in 2026 and
  `'20200105'` as a day (`Instants.clickhouse_epoch_seconds`); SQLite read
  `'1.5'` as a Julian day, refused a leading space, a one-digit month and
  `2020/01/05`, and refuses a zone past 14 hours
  (`Instants.sqlite_zone_limit`). One reading (`trilogy.core.casts.text_to_datetime`)
  is rendered now: a date, an optional `HH:MM:SS[.f]` and an optional zone
  (`Z`, `+HH`, `+HHMM`, `+HH:MM`), naming the UTC instant, truncated to the
  engine's precision (microseconds, ClickHouse milliseconds, SQLite whole
  seconds). `date(s)`, `datetime(s)` and `timestamp(s)` of a string are its
  cast now; each rendered a bare CAST or `toDate`, which raised on DuckDB and
  ClickHouse and rolled the day on SQLite. A literal folds the same way
  (`fromisoformat` read `'20200105'` and kept a zone unconverted), a folded
  `timestamp` is an aware UTC instant, and on SQLite a folded instant is
  inlined as `datetime('...')`: bound as a parameter, its isoformat text
  (`+00:00`, microseconds) never equalled a read
  (`Instants.bound_text_is_not_datetime_text`).

  A numeral past its target's range is NULL, as DuckDB's `TRY_CAST` gives:
  SQLite's CAST saturated `'99999999999999999999'::int` to 2^63 - 1
  (`Readings.saturation_is_not_the_value`); ClickHouse's
  `accurateCastOrNull` from a decimal raised `Convert overflow`, failing the
  whole query over one such row (`Readings.clickhouse_overflow_raises`); and
  SQLite and ClickHouse kept `'12345678901.25'::numeric(12,2)`
  (`Readings.decimal_precision_unchecked`). The guard reads the rounded value
  (`Readings.round_can_leave_range`); on SQLite a whole numeral is compared
  with the 64-bit bound digit by digit (`Readings.digit_compare_exact`). A
  literal past the 64-bit range or the declared precision is refused.

  On main, 103 of the 124 cases `tests/core/test_instant_read_laws.py` runs
  fail; none now.

- **A value cast to a string had a text per engine** (0.3.475;
  `Writings.lean`). SQLite's CAST wrote a REAL at 15 significant digits on
  SQLite 3.45, so `0.1 + 0.2` was `'0.3'` and read back as another double
  (`Writings.sqlite_fifteen_digits`), and at 17 on 3.53
  (`'0.33333333333333332'`); ClickHouse wrote the float 1.0 as `'1'` and
  `1e20` as twenty-one digits (`Writings.clickhouse_drops_point`); SQLite and
  ClickHouse wrote a `numeric(12,2)` 3.00 as `'3.0'` and `'3'`
  (`Writings.scale_dropped`); ClickHouse wrote a `DateTime64(3)` with its
  padded milliseconds (`'.250'`, `'.000'`; `Writings.clickhouse_pads_fraction`);
  and DuckDB wrote a timestamp in the session's zone
  (`Writings.session_zone_written`). One text (`trilogy.core.casts`) is
  rendered now: a float as Python's `repr` (DuckDB's own VARCHAR; SQLite picks
  the fewest of 15 to 17 printf digits that read back, ClickHouse lays out
  `toString`'s shortest digits again), a decimal with exactly its declared
  scale, an instant with its fraction's trailing zeros dropped, and a
  timestamp at UTC followed by `+00:00`. A float literal is cast to a double
  first, so a float DuckDB computes as a DECIMAL (`n + 1.5`) writes `'2.5'`,
  not `'2.5000'`, and a decimal is cast to its declared type, so DuckDB's
  DOUBLE `avg(n)` and DECIMAL(12,2) `round(n, 2)` of a `numeric(12,4)` write
  four places as SQLite and ClickHouse do.

  ClickHouse divided a decimal at the dividend's scale, so `n / 3` of a
  `numeric(12,4)` was 0.3333 where the declared type of a decimal quotient is
  a double (`Writings.clickhouse_decimal_quotient`); it divides as a Float64
  now.

  On main, 24 of the 58 cases `tests/core/test_text_write_laws.py` runs fail;
  none now.
- **A rendering named its operand hundreds of times** (0.3.476;
  `Sharing.lean`). SQLite's datetime read named its operand 756 times in
  ~120 kB of its own text, a date read 162 times and a float write 42 times,
  so `cast(cast(cast(s as datetime) as string) as datetime)` was 77 MB of SQL
  (`Sharing.sqlite_instant_blowup`); ClickHouse's datetime write of a read
  named it 29 times and the same three levels were 2.1 MB
  (`Sharing.clickhouse_instant_blowup`). The nesting also overflowed the
  100-deep parser stack of SQLite before 3.46 (Ubuntu 24.04's 3.45.1), so
  every datetime read raised `parser stack overflow` there. A random operand
  was drawn once per use, so SQLite's float write chose its digit count and
  layout from one draw and printed another (`Sharing.inline_redraws`). Each
  operand is bound once now: on SQLite a correlated subquery per level of
  named parts, with an OFFSET so the flattener does not inline the levels
  back (`Sharing.flattened_program_blowup`: 1,866 compiled instructions
  instead of 291, and a datetime read 12 times slower than bound); an
  operand with no column stays inline, as SQLite evaluates an uncorrelated
  subquery once per query (`Sharing.hoisted_draw_is_shared`). On ClickHouse
  a date or instant read, an instant write and a clamped substring bind with
  `arrayMap`; a number read stays inline, as ClickHouse computes a repeated
  expression once and the lambda doubled its time. The zone of a SQLite
  instant read is now `ltrim` of the tail, with the fraction's shape checked
  in the guard, which saves a level.

  Over 100k rows a SQLite datetime read went from 13.6 s to 1.1 s, a date
  read from 5.0 s to 0.9 s and a float write from 0.52 s to 0.23 s; a
  ClickHouse datetime read from 240 ms to 70 ms. On main, 24 of the 51 cases
  `tests/core/test_bound_operand_laws.py` runs fail (the SQLite nesting case
  runs out of memory), and 37 of 126 SQLite cases of the read and write law
  suites fail on SQLite 3.45; none now.

- **A table at its own grain could not answer an aggregate of its own
  columns alone** (0.3.477; `ValueSet.lean`). `fits (fit_class: cls, n:
  n_by_cls) grain (cls)`, with `cls` derived from a row property or a
  property of a key `fits` does not store, answered `select cls, n_by_cls`
  but raised `NoDatasourceException` for `count_distinct(cls)`, `min(cls)`,
  `sum(n_by_cls)` and `count(n_by_cls)` once the row table was hidden, as a
  datasource `assert` hides it. Two rules were missing:
  - the binding-only walk followed lineage arguments, while the graph also
    reads a stored aggregate on its grain keys, so `cls` under `sum(n_by_cls)`
    was rebuilt from `species` instead of read from `fits`. The walk now
    follows the graph's own upstream edges (`concept_graph.upstream_concepts`).
  - a repeat-blind aggregate reads only its argument's values
    (`ValueSet.value_set_min_exact`, `value_set_max_exact`,
    `value_set_count_distinct_exact`), so where no datasource binds the
    argument's row identity, a complete binding of the argument is the
    identity it is read at (`concept_graph._reads_value_set`). `count(cls)`
    reads repeats (`ValueSet.value_set_misses_repeats`) and is still
    refused. It equals the re-sum of `count(id) by cls` over the non-null
    classes (`ValueSet.count_column_from_row_count`), not over every class
    (`ValueSet.count_column_counts_null_group`); that rewrite is not
    implemented.

- **A city's species lookup read the city's tree file** (0.3.485;
  `Cost.lean`, "Under the query's WHERE"). The Urban Tree Reporting species
  page asks `select common_names[1], tree_form, growth_rate` under
  `city = 'USBOS'`: the species with a tree in the city, looked up on the
  enrichment table (`Cover.exists_by_value`). Both a `complete where
  city = 'USBOS'` tree table and the `(city, species)` count summary answer
  it, and `Cost.partial_and_summary_incomparable` says neither stores fewer
  rows, but a scan reads only what the WHERE keeps, and there the summary
  reads one row per species and the table one per tree
  (`Cost.filtered_dominated_le`). The network search ranked the summary as
  "read in full beside a pre-filtered twin" and broke the remaining tie by
  name; `dominated_reads` now counts the tree table, not the summary. The
  same card under the nativeness cross-filter still read the trees: the
  network search marked every summary filter-sensitive for a WHERE on
  `native_locality_bucket`, a species property, without the
  property-of-a-grain-key exemption the select path already applies
  (`Cover.joined_case_exact`). The existence shapes in
  `tests/discovery/test_cheapest_source_laws.py` found two more:
  - `select species where city in ('A', 'B')` read the trees: a literal
    `in` list reports one empty existence group, which the condition prune
    read as a subselect the summary could not apply.
  - a `partial datasource` summary (no arm, or an arm the WHERE does not
    imply) joined between a city-grain summary and the species table paired
    every city with the species of its own rows only: `select form where
    city in ('A', 'B')` lost city B's linden. A source holding an unknown
    subset of its rows (no `~` claim) may end a join path but is no longer
    the only link between two others.

- **A WHERE on a stored group value read the trees at a dimension's grain**
  (0.3.486; `Rollup.lean`, "Filters on a stored group value"). `select
  tree_form, sum(height) where sum(height) by city, species > 3` beside a
  `(city, species)` summary is the summary rows the gate keeps, joined to
  the enrichment table on `species`, re-summed per form
  (`Rollup.group_value_filter_joined_exact`); filtering after the re-sum
  tests each form's total (`Rollup.joined_group_filter_after_resum_misfilters`).
  `_plan_joined_filter_rollup` already built that plan, but the root gate
  (`_finer_filter_reaches_rollup`) required every demand beside an
  aggregate atom to be a stored column of the summary, so `tree_form` sent
  it to the base rows. A dimension's column keyed on the summary's grain now
  passes (`aggregate_rollup.joinable_property`, shared with the source
  pin). 30 of the 105 group-value cases in
  `tests/discovery/test_rollup_resolution_laws.py` (the `form` and
  `city_form` grains) read the trees on main; answers were right either way.
  A CASE over `tree_form`, and a grand total under `... or tree_form =
  'linden'`, still read the base rows.

- **A WHERE on a coarser group value read the trees, and the pushdown that
  serving it exposed read a partial as the value** (0.3.487; `Rollup.lean`,
  "Filters on a group value at a coarser grain"). `select species,
  sum(height) where sum(height) by city > 6` beside a `(city, species)`
  summary is the summary rows gated by the city's re-sum, joined back on
  `city`, then re-summed (`Rollup.coarse_group_value_filter_exact`). The root
  gate never offered the summary: `_conditions_supported` read
  `sum(height) by city` as a column the summary must bind. Under `joined`
  (the pinned joined plan) an aggregate at a grain inside the summary's now
  passes, since that plan computes it in its own group. Serving it surfaced
  a wrong answer in predicate pushdown: the gate's group re-sums the
  summary's `h` under the gate's own address, so the scan below it holds one
  partial per species, and the WHERE was pushed onto it (`select sum(height)
  where sum(height) by city > 6` answered 7, not 17;
  `Rollup.coarse_gate_on_partial_misfilters`). A predicate no longer pushes
  into a parent whose child re-aggregates a column it reads
  (`_parent_holds_partial`). A row column beside the gate that the target
  does not group by (`... and species != 'Tilia'` at `tree_form`) widens the
  pinned request past the target, and the search that took over re-maxed the
  gate at `(species, tree_form)` (wrong) or failed to bind it, so those
  shapes still read the trees (`_coarse_gate_beside_row_columns`). 172 of the
  280 cases in `test_coarse_group_value_filter_reads_summary` read the base
  rows on main; answers are the same either way.

- **A re-sum beside its own partials was joined on the partial** (0.3.488;
  `Rollup.lean`, "A re-sum beside the rows holding its partials").
  `select tree_form, sum(height) as v, sum(sum(height) by city) by * as w
  where species = 'Acer'` returned no rows beside a `(city, species)`
  summary. The plan held `v` per `(tree_form, city)` in one node and its
  re-sum per `tree_form` in another, and join resolution paired the two on
  every column they share, `v` included: a group whose partials differ from
  their total (`broadleaf`: 3 and 7 against 10) or are NULL lost its row
  (`Rollup.partial_is_not_a_key`). Unfiltered, a dedup folded the re-sum
  node away and hid the bug; a WHERE applied in both nodes stopped the fold.
  A partial a side still re-aggregates is no join key
  (`get_node_joins`); joined on the group key alone each row reads its
  group's value (`Rollup.resum_lookup_exact`). 15 of the 320 cases in
  `test_resum_beside_other_grain_keeps_every_group` return wrong rows on
  main.
- **A gate on a re-sum of a stored value failed to plan, and a coarse gate at
  the summary's own grain read the trees** (0.3.488). `select tree_form,
  sum(height) where sum(sum(height) by city, species) by city > 6` raised
  "Missing source map entry for local.species" with the summary in scope:
  the FINAL carried the join key `species` through a dedup group whose inner
  merge held it as a hidden handle, and `_widen_scan_chain` counted a hidden
  output as carried. It now unhides it. `select city, species, sum(height)
  where sum(height) by city > 6` read the trees: `get_rollup_concepts`
  offers no rollup to the summary's own grain, so the joined plan was never
  tried. Under a coarse gate it now is; the re-sum is an identity group
  (`Rollup.coarse_gate_at_summary_grain`). 60 of the 336 cases in
  `test_coarse_group_value_filter_reads_summary` (the summary's grain, and
  the `nested` gates) read the trees or raised on main.
- **An aggregate member of an IN list lost its grain** (0.3.489;
  `SelectGrain.membership_pin_misreads`). `select city, count(tree_id) where
  count(tree_id) by species in (1)` returned no rows, with or without a
  summary: the `in` form builds a `SubselectComparison`, whose factory built
  its left side inline, dropping the `by`, so the gate read each city's own
  count, and the planner placed it as a HAVING. `=` and a parenthesized
  member were right. The member is now instantiated as a concept at its own
  grain, as either side of a plain comparison is. 42 of the 56 IN-list gates
  in `test_coarse_group_value_filter_reads_summary` (`in_list`,
  `form_in_list`) return wrong rows on main, and with the fix they read the
  summary.
- **A HAVING over an output's own aggregate failed to render beside a
  summary** (0.3.489). `select city, sum(height) as v having sum(height) by
  city > 6` raised "Missing source reference to local.height". The select
  pins `v` to `sum(height) by city`, so build-time HAVING normalization
  found the HAVING aggregate's signature among the outputs and skipped
  promoting it, but never pointed the HAVING at `v`. Rendered inline, it read
  `height`, which the summary scan does not carry. It now reads the output.
  All 120 cases of `test_having_on_own_grain_aggregate_reads_output` raise on
  main.
- **A gate at a dimension's column reads the summary** (0.3.489;
  `Rollup.dimension_gate_exact`). `select city, sum(height) where
  sum(height) by tree_form > 6` read the trees. `sum(height) by tree_form` is
  a function of each summary row (`tree_form` is keyed on `species`), so
  `keyed_aggregate` now accepts a grain of the summary's keys and dimension
  columns keyed on them. `_plan_joined_filter_rollup` computes the gate
  grouped by its own grain, never by the key the column hangs off (0.3.488's
  probe grouped it by `species`: `Rollup.dimension_gate_by_its_key_misfilters`),
  joins the dimension columns onto the summary rows first, and joins the gate
  onto those rows on its whole grain. Joined in one merge, a `(city,
  tree_form)` gate paired with the scan on `city` alone and kept every
  `tree_form` of a passing city (`Rollup.gate_joined_on_part_of_its_grain_misfilters`;
  20 wrong-row cases mid-round). 212 of the 548 cases in
  `test_coarse_group_value_filter_reads_summary` read the trees on main.
- **A property selected beside its key was read off a group without the
  key** (0.3.490; `SelectGrain.property_without_its_key_misjoins`). `select
  city, species, tree_form, max(height) by city, tree_form as g, count(tree_id)`
  returned 10 rows for 5, with or without a summary, pairing each species
  with every form of its city. The bare count groups by `(city, species)`
  (`tree_form` is a property of `species`), and the FINAL cover took
  `tree_form` from the more downstream `(city, tree_form)` group, which the
  merge could join on `city` alone. `elect_cover` now prefers a candidate
  carrying the property's selected key (`property_with_its_key_exact`). A
  WHERE gate at `(city, tree_form)` beside the same select misjoins the same
  way. Found by a random sweep against an independent SQL oracle; the law
  harness compares with and without the summary, and both shared it.
- **A coarse gate beside a key and its dimension column misfiltered with a
  summary** (0.3.490; `Rollup.gate_resummed_at_target_misfilters`). `select
  species, tree_form, count(tree_id) where sum(height) by city > 8` returned
  `[('Acer', 'broadleaf', 3)]` for `[('Acer', 'broadleaf', 1), ('Tilia',
  'linden', 1)]`. The pinned joined plan declined: `tree_form` is no
  component of the reduced target grain `(species)`. The search that took
  over read the gate off the same summary scan as the target and re-summed it
  per species, a HAVING at the select grain. The joined plan now accepts an
  output keyed on the target grain and joins it on like a filter column, so
  the shape reads the summary. Every `species_form` gate in
  `test_coarse_group_value_filter_reads_summary` exercises it.
- **A total gate under an OR misread off a summary** (0.3.490). `select
  city, sum(height) where count(tree_id) by city > 6 or max(height) by * > 6`
  raised "Missing source reference to local.height": the joined plan never
  joins a single-row gate onto the summary rows, and under an OR the caller
  cannot split it off. Declining the joined plan alone gave wrong rows
  (`[('B', 9)]`), and `select city, tree_form, sum(height) where city = 'A'
  or max(height) by * > 6` returned wrong rows on main too: the search read
  the stored `max` as the total of each target group. `total_gate_beside_rows`
  keeps the summary out of any conjunct mixing a total with row columns
  (0 of 972 such shapes wrong, from 30), and they read the trees.
- **A dimension gate beside an unstored gate at the summary's grain
  double counted** (0.3.490). `select city, count(tree_id) where
  count_distinct(tag) by city, species = 1 and sum(height) by city, tree_form
  > 1` returned `[('A', 2), ('B', 4)]` for `[('A', 1), ('B', 2)]`: the joined
  plan cannot source `count_distinct` from the summary, and the search joined
  the `(city, tree_form)` gate onto the other gate's `(city, species)` keys on
  `city` alone (`Rollup.gate_joined_on_part_of_its_grain_misfilters`).
  `_unsourced_beside_dimension_gate` keeps the summary out of that shape.
  Across the three files (`test_rollup_resolution_laws.py`, now with a
  `species_form` grain, `test_coarse_group_value_filter.py` and
  `test_property_beside_key.py`), 47 of 3,604 cases fail on main: 44 return
  wrong rows, 2 raise, and 1 (`city = 'A' or max(height) by * > 6` at `city`)
  is right off the summary, which this guard now leaves for the trees.


- **An oracle over the raw rows found seven more bugs** (0.3.491). 8,000
  random selects (keys including a property beside its key; projected,
  nested, `by` and `?` aggregates; WHERE row atoms, gates and literal
  memberships under and/or/not, parenthesized and bare; HAVING gates) were
  checked against Python evaluating Trilogy's semantics directly
  (`tests/discovery/test_oracle_sweep_laws.py` keeps 600 of them). Main
  failed 108 (44 with wrong rows, 4 rejected by design, the rest raising);
  this round fails 8 (1 with wrong rows, the same 4 rejected), each of them
  failing on main too.
  - A literal membership gate was read as a set lookup
    (`SelectGrain.literal_membership_gate_dropped`). Its existence arguments
    are `[()]`, so placement skipped the FINAL re-filter it gives the
    comparison: `select species, min(height) by * where max(height) by city,
    species in (7, 1)` returned all four species for one. `_reads_a_set`
    reads a literal list as a comparison
    (`literal_membership_is_a_comparison`), and `holds_on_absent_group`
    decides one on an absent key (`count_membership_on_absent`).
  - A key read off a WHERE gate's own group skipped the row atoms
    (`SelectGrain.gate_group_skips_row_atoms`): `where count_distinct(tag) by
    species = 1 and city = 'B'` returned species of city A, while the
    parenthesized spelling was right. `elect_cover` demotes a condition-phase
    candidate, which reads every row by design, when a WHERE row atom never
    reaches it (it keeps a padded region's distinction otherwise).
  - A `by` grain holding a property beside its key's grain joined on the
    other columns alone (`SelectGrain.by_property_without_its_key_misjoins`;
    0.3.490's case with the property not selected): `select city, species,
    max(height) by city, tree_form, count(tree_id)` returned 10 rows for 5.
    `elect_bridges` now elects a keying bridge (the row stream holding the
    determined key) for a pairing of two grouped suppliers on fewer keys
    than one's grain determines of the other's, and the FINAL joins it
    whatever the islands.
  - A gate keyed on a property no FINAL side carries (`where
    count_distinct(tag) by tree_form > 8` beside `city, species`) raised a
    keyless join; `_gate_key_row_stream` now also takes the scan holding it.
  - With a summary, a total beside a summary-served aggregate read the base
    rows without the WHERE (`Rollup.where_filters_every_scan`): `select
    species, sum(height) by *, max(min(height) by species) where species !=
    'Tilia'` returned 17 for 15. The summary's materialized root left the
    WHERE column and `height` in two root buckets. A WHERE row argument now
    co-sources with every root bound beside it (`where_row_arg`); the shape
    reads the trees.
  - `agg(x ? c)` at the summary's own grain beside a gate selected a column
    named after the output (`tree_summary.v0`): `get_alias` matched a stored
    partial only from a finer grain. It now reads the unfiltered partial at
    any grain (`Cover.filtered_sum_exact`); 13 sweep shapes raised.
  - A HAVING aggregate `by` a grain the select grain determines (`having
    sum(height) by tree_form > 5` beside `species`) was rejected as growing
    the grain; `_promotion_grows_grain` reads a property keyed in the select
    grain as determined (`SelectGrain.determined_by_on_grain`).
  12 of the 1,200 sweep cases and 12 of the 13 regressions in
  `tests/discovery/test_where_reaches_keys.py` fail on main.
  - A sole FINAL contributor re-aggregating a gated group re-applied the
    gate at its own grain and raised a keyless join: `select tree_form,
    min(sum(height) by city, species, tree_form) where (min(height) by
    species > 1 or city = 'B')`. The re-aggregate holds only the groups the
    gate kept (`SelectGrain.regrouped_gate_is_applied`), so
    `_group_applies_conditions` now reads a grouping parent's atoms when
    every input is that parent's output or a function of its grain. This
    also plans the `in` spellings the literal-membership fix had turned
    from accidentally right into errors.
- **A wider oracle sweep found eleven more** (0.3.492). 20,000 seeds of the
  0.3.491 generator; 8,000 of them A/B'd: main fails 10 (4 wrong rows, 6
  raising), this round 2 (one shape, failing on main too), with no shape main
  gets right failing here and the summary read on as many shapes as main.
  - An atom over a row column and a finer gate (`tree_form = 'linden' or
    max(height) by city, species = 3`) beside a count by `species`, with
    `tree_form` selected, was applied after the count
    (`SelectGrain.mixed_atom_after_count_misreads`): `tree_form` was peeled
    to a dimension join because the atom held an aggregate, so the atom
    became a species semijoin. A WHERE aggregate is now a mate of the row
    columns beside it, and peels only when every grouping grain, a
    summary-served one included, determines its grain
    (`atom_on_group_key_drops_groups`).
  - A join pair was pruned as implied by another side's pair
    (`SelectGrain.pair_implied_off_its_side_misjoins`): `select city,
    species, tree_form, count(tree_id) where sum(height) by city, tree_form <
    8 or max(height) by species = 6` paired each species with every form.
    `reduce_concept_pairs` prunes a pair only where an earlier join equated
    its side with the side whose pairs imply it.
  - The same mixed atom beside `min(height) by tree_form` (`where
    (tree_form = 'broadleaf' or count(tree_id) by city, species = 1) and
    species = 'Acer'`) raised a keyless join: with `tree_form` kept on the row
    stream, FINAL reads `species` off the filtered rows, not the gate's group.
  - A FINAL re-apply of a gate the filtered branch already regrouped raised a
    keyless join (`not (avg(height) by * = 3 or avg(height) by city, species
    > 8)` beside `min(sum(height) by city, species)` by `tree_form`):
    outputs the branch's own regroup holds are covered
    (`SelectGrain.regrouped_gate_is_applied`).
  - An atom over an aggregate's grain key (`city = 'B' or ...` on a group by
    `city`) was read as decided after the aggregate, so the join plan let the
    group "carry" the atom's other columns and paired two aggregates on
    `tree_form` neither held.
  - With a summary: a dimension's column filters every scan of its key
    (`Rollup.dimension_column_filters_every_scan`; `max(height) by * where
    tree_form is null` read every tree); a scan shared by an `agg(x ? c)`
    partial carries `c` up to the CASE; a gate's twin is not carried as a
    column of a stored partial's rows; and two guards keep the summary out of
    the search when the joined plan declines (a dimension gate at a grain the
    select does not hold beside a stored gate and a row column, and an
    unsourced gate beside a coarse twin of the target), both double counted or
    raised on main.
  28 of the 40 new sweep cases (`FOUND_SEEDS`; the other 12 are the same
  seeds on the model main gets right) and all 15 regressions in
  `tests/discovery/test_where_gate_pairing.py` fail on main.
- **A second oracle family found five more** (0.3.493). The first family's
  next 3,400 seeds held one wrong row; a generator of new spellings
  (`tests/discovery/test_oracle_spellings_laws.py`: keys-only and `tree_id`
  selects, `>=`/`<=`/`!=`, a gate `is null`, two gates compared or
  subtracted, `between`, `coalesce`, a gate inside `agg(x ? c)`, a compound
  HAVING) failed 391 of 20,000 queries on main (199 wrong rows, 192 raising).
  A/B'd against main on the same 20,000: 199 wrong and 192 raising there,
  127 and 146 here, 118 fixed and none main gets right broken; the first
  family's seeds 20,000-25,000 lose their 4 raises. The summary is read on
  as many first-family shapes as main (2,120); on the second it is no longer
  read on 7 keys-only shapes whose summary plan was wrong but happened to
  return the right rows (`select city where min(height) by city >
  min(height) by species` compared `min(lo)` with itself). JOINs: 72,180 on
  main, 72,344 here.
  - Keys split over two filtered branches were paired through rows the
    WHERE never filtered (`SelectGrain.split_keys_pair_unkept`): `select city,
    tree_form, count(tree_id) by tree_form, count(tree_id) by city where
    min(height) by city, species != 7` returned `(B, broadleaf)`. Each key was
    the filtered branch's own, so no FINAL re-applied the gate; the select's
    keys must now be held together by one filtered branch.
  - With a summary, a keys-only select's gate was filtered on the summary's
    partials re-summed at the select's grain
    (`Rollup.gate_partial_resummed_off_its_grain`): `select species where
    sum(height) by city <= 2` grouped `h` by species and returned Tilia. A
    source plan whose own condition reads a partial off its grain is declined,
    and the gate's own group answers; the summary is still read.
  - A `by` value beside a row key entered the select grain
    (`SelectGrain.by_value_as_key_misjoins`): in `select tree_id, sum(height) by
    tree_form as v0, count(tree_id ? ...)` the filter was keyed on `v0`, `v0`
    read as each row's `height`, and the join on it kept one tree of six.
    `minimize_by_bindings` drops a `by` value whose keys the others bind
    (`SelectGrain.by_value_is_no_grain_key`).
  - An extra FILTER side at FINAL was paired on columns of its input rows it
    never emits: `sum(height ? max(height) by tree_form <= 5)` beside `sum(height)
    by city` raised a keyless join on main; it now bridges through the rows.
  - `city = 'B' and city = 'B'` beside a negated gate (sweep seed 2537, the
    lead #956 left) raised "Could not find CTE": a parenthetical folded into
    itself became `X and X`, another CTE identity
    (`SelectGrain.conjunction_idempotent`).

- **The spellings family's wrong rows, 127 to 5** (0.3.494). Over the
  second family's 10,000 seeds on both models (20,000 queries), the branch
  this round started from returned 127 wrong rows and raised on 145; this
  round returns 5 wrong and raises on 119, 148 fixed and none broken. The
  first family's seeds 0-10,000 stay clean.
  - A WHERE was hosted only on the group a filtered aggregate's condition
    reads (`SelectGrain.gate_host_skips_select_rows`): in `select city,
    max(height ? (city = 'A' or sum(height) by * is null)) where (min(height)
    by * = 6 or city in ('B'))` the atom sat on the `sum(height) by *` group,
    and the select's `max` aggregated the raw rows beside that value, returning
    city A. Host election counted the select's aggregate as below the host,
    but only a value of the host is joined in; its rows come straight from the
    scan. An aggregate whose aggregated rows did not pass through a host now
    hosts the atom too (`_hosts_reading_unfiltered_rows`;
    `SelectGrain.where_filters_group_rows`). This was #960's open seed 346 and
    about 100 wrong rows besides.
  - Two groups both carrying a WHERE value (`max(height) by tree_form`) were
    joined on it with `=` beside `tree_form`, dropping the NULL `tree_form`
    group (`SelectGrain.value_pair_drops_null_group`). An aggregate paired
    with itself beside every key of its grain is now decided by the grain's
    pairs (`value_paired_by_grain`).
  - An atom over aggregates of two grains and a row column (`height > 3 or
    sum(height) by species - count(tree_id) by city = 2`) refused to host on
    the row stream because the stream produces `height`; it landed on a
    derived column's group, which rendered both aggregates at the row grain
    (`sum(height)` as `height`). Only the producers of the aggregates are
    excluded now.
  - A grand total's empty-group guard was appended to a disjunctive HAVING as
    `a or b and count(*) > 0`, so a select whose WHERE kept no row returned one
    NULL row (`SelectGrain.guard_precedence_emits_empty_total`). The HAVING is
    parenthesized.
  12 of the 18 cases in `tests/discovery/test_where_hosts.py` (the other 6
  guard shapes a first cut broke) and 148 of the spellings suite's 994 fail
  on the round's base.

- **The spellings family, seeds 10,000-20,000** (0.3.495). On main these
  20,000 queries returned 14 wrong rows and raised on 124; now 7 and 30, 101
  fixed and none broken. The first family's seeds 0-4,000 return the same
  rows and read the summary on the same shapes.
  - A HAVING `by` the select's rows bind was rejected as growing the grain:
    `select tree_id, max(height ? city = 'A') having count(tree_id) by city >
    2` (and `by v0` beside `city as v0`, the rename finalize writes into the
    `by`). `_grain_determined` read only property-to-key facts; it now reads
    the binding closure and resolves a pure rename
    (`SelectGrain.having_by_bound_key_is_coarser`). This was #960's seed 129
    and about 100 raises; seed 8920's wrong row went with it.
  - `select city, species, min(height ? max(height) by * = 6) where
    (avg(height) by * is null and city = 'B')` returned city B's species. The
    per-key group hosting the WHERE read `avg(height) by *` off the `?`
    total, itself a host of the atom: rejected there, its value was padding
    NULL, and `is null` held (`SelectGrain.gate_read_past_its_host_passes_absent`;
    exact for a null-rejecting atom, `gate_past_host_exact_if_null_rejecting`).
    A host now reads the aggregates of an atom that can hold on an absent
    group off an unfiltered sibling.
  - Promoting the HAVING value routed `select tree_id, species, max(height)
    by tree_form where species = 'Acer' having sum(height) by city, species >=
    2` into the path main takes for the explicit spelling `--sum(height) by
    city, species as s ... having s >= 2`, wrong on main: with the summary, the
    value was read with the rows and then bucketed at its own grain
    `(city, species)`, which the rows joined on `species` alone, doubling
    every tree (`SelectGrain.value_bucket_pairs_short_of_its_grain`). A value
    at a grain the rows determine is projected with the rows.
  15 of the 20 cases in `tests/discovery/test_gate_value_sources.py` fail on
  main (the others are the `is not null` twin, the HAVING spelling of the
  bucketed value, which main semijoined, and the explicit one without the
  summary), and 120 of the 1,120 in it and the spellings suite together.
- **The spellings family, seeds 20,000-30,000** (0.3.496). On main these
  20,000 queries returned 11 wrong rows (six seeds) and raised on 14; now none
  and 12, with no new wrong row or raise. The first family's seeds 0-4,000 stay
  clean; JOINs rise 0.2-0.3% and the summary is read on as many shapes. Seeds 10132, 11682, 19186 and 19236, open after 0.3.495, are fixed.
  - `select city, species, sum(height) by * where (max(height) by city,
    tree_form <= 3 or min(height) by species != 6)` returned `(B, Quercus)`:
    the FINAL cover took `city` off the gate's `(city, tree_form)` group and
    `species` off the `species` group, paired on `tree_form`
    (`SelectGrain.keys_off_two_groups_pair_unheld`). When no winner holds all
    the selected keys, they come off one group that does
    (`_keys_off_one_tuple`; `keys_off_one_stream_exact`).
  - `select species, tree_form, max(height) where min(height) by city,
    tree_form is null` returned every species. The FINAL re-applied the WHERE
    to the side supplying `tree_form`, re-sourced from the enrichment alone,
    while a second scan carried `city` to the gate; the enrichment met the
    gate through no tree and its padding passed
    (`SelectGrain.dimension_side_pads_gate`). A ROOT side holding a gate key
    of an atom that can hold on an absent group carries it itself
    (`_final_extra_sides`).
  - `select count(tree_id ? max(height) by * != 10), ... where (sum(height)
    by * is null and max(height) by city > 3)` returned a row with no row
    kept. The `?` stream read the total off the select's own total, a host of
    the WHERE that kept nothing, through a preserving join. 0.3.495 reroutes
    only atoms that hold on an absent group as a whole, and `max(...) > 3`
    makes this conjunction null-rejecting in `max` but not in the total
    (`SelectGrain.conjunct_read_past_its_host_passes_absent`; exact per
    argument, `gate_arg_past_host_exact_if_rejecting`). The reroute now asks
    per argument (`args_holding_on_absent`).
  - Seed 19186: `(city, tree_form)` came off the `?` filter's row stream,
    whose rows joined only beside the per-city aggregate hosting the WHERE
    (`SelectGrain.key_off_unfiltered_stream_returns_rejected`). The cover
    prefers a source whose rows passed every WHERE atom that can hold on an
    absent group; a one-row total is exempt.
  - Seed 19236: the row stream elected to bridge `min(height) by city,
    tree_form` onto the rows was asked for `tree_form`, held below a dedup it
    was never carried through, so the value met the rows on `city` alone. A
    keying bridge reaches for a stored key below it, through a dedup whose
    outputs determine the key (`SelectGrain.dedup_carries_determined`).
  15 of the 16 cases in `tests/discovery/test_key_sources.py` fail on main.
- **The spellings family, seeds 30,000-40,000** (0.3.497). On main these
  20,000 queries returned 9 wrong rows and one invalid SQL statement (six
  seeds); now 2 wrong rows (seed 30670, a lead), with no new wrong row and as
  many raises. Over seeds 0-40,000 JOINs rise 0.4% and the summary is read on 8
  fewer of 12,912 shapes; the first family's seeds 0-4,000 stay clean.
  - `select city, species, sum(height) by species, sum(min(height) by city,
    species, tree_form) where count_distinct(tag) by city, tree_form < 3`
    returned every row twice. The FINAL re-applied the gate, but the one side
    holding `tree_form` was folded into the sibling rendering `city, species`
    (`_fold_covered_contributors`), so the gate's group met the rows on `city`
    alone (`SelectGrain.gate_paired_short_of_its_grain`). A FINAL gate's grain
    keys now count as needed when folding (`_final_gate_keys`; seeds 35845,
    31847).
  - `select ..., max(height ? ...) where min(height) by species - count(tree_id)
    by * > 3` returned `(A, Quercus)`. The subtraction proves the count
    non-null, so 0.3.496 read it past a host of the WHERE, but a COUNT on a
    group no row formed is 0: `5 - 0 > 3` held on the padding
    (`SelectGrain.count_arg_past_host_passes_absent`). `args_holding_on_absent`
    now includes a COUNT unless a comparison of it to a literal rejects 0
    (seed 39340).
  - `select species, tree_form, sum(height) where (count(tree_id) by tree_form
    < count(tree_id) by species or avg(height) by city, tree_form is null)`
    returned every species. The cover took `tree_form` off the species count's
    own (condition) group, and the FINAL joined a producer of the WHERE's
    arguments FULL, keyed off another producer's columns: species with no kept
    row came back padded and passed `is null`
    (`SelectGrain.lookup_preserved_passes_padding`). The cover reads no output
    off a group producing an argument of a FINAL atom that holds on an absent
    group, and a lookup keyed off another lookup keeps no row of its own
    (`MergeNode.lookups`, `_cap_lookup_joins`; seed 31824).
  - `... where sum(height) by species is null having tree_form = 'broadleaf' or
    sum(height) by city, species < 0` with the summary rendered invalid SQL:
    predicate pushdown moved the HAVING into a group by `(city, species)` as a
    HAVING naming `tree_form`, a property of `species` but no grouped column.
    An atom over an aggregate now pushes into a group only when every row
    column it names is grouped (`_predicate_safe_past_grouping`; seed 35775).
  8 of the 10 cases in `tests/discovery/test_final_gate_lookups.py` fail on
  main.
- **The spellings family, seeds 40,000-60,000** (0.3.499). On main these
  40,000 queries returned no wrong row and raised on 51; now 5 raise, each
  only in a batch (it plans alone), with no new wrong row. The summary is read on 15 more shapes and lost on none, and
  JOINs rise 0.2%. Over seeds 0-40,000 the raises fall from 78 to 9 (5 of
  them batch-only), the same 5 wrong rows remain (the open leads), the summary
  is read on 24 more shapes and lost on none, and JOINs rise 0.14%; the first
  family's seeds 0-4,000 stay clean (one more summary read, one more JOIN).
  - `select city where sum(height) by tree_form < sum(height) by *` raised
    "Could not resolve connections" (22 of the 30 raises on seeds
    40,000-50,000). The rows a total filters are keyed on the `all_rows`
    constant, and reaching `tree_form` joins the trees to the enrichment. The
    cover search found that join, but a joined source plan refused the request
    for not binding the constant, which no scan does. A constant is a literal
    in any projection: the joined plan now carries it inline
    (`SelectGrain.constant_key_pairs_every_row`).
  - That plan exposed a name-class bug. An alias of a row value (`auto s2 <-
    status`, `s2` a pseudonym of `status`) joined the select's pinned read of
    `status` to the alias's class, so the orders' stored column, which stores
    the alias, answered the pinned read. A customer with no order then read
    the padded NULL, not the CASE's `'in-transit'`, and `status is null` kept
    it (`SelectGrain.alias_is_not_the_pinned_read`). A pseudonym the statement
    pins to its select's grain is now no spelling of an alias that is not
    pinned (`BuildEnvironment._pinned_apart`).
  - `select tree_id, avg(height) by species, min(height ? not (tag = 'x')) where
    max(height) by city, tree_form != 6 having count_distinct(tag) by city >=
    2` raised "keyless join". The FINAL plan elected two scans of the trees as
    bridges and paired the species value with only one of them, on `species`.
    The other scan, joined first, also exposed `species`, so the planned one
    was skipped as joining no new island, and the value met the rows with no
    keys (`SelectGrain.keyless_pairing_fans_out`). A bridge that is some
    contributor's only planned partner is now always joined.
  10 of the 10 cases in `tests/discovery/test_total_beside_lookup_gate.py`
  fail on main.
- **A padded side was read as the WHERE's population** (0.3.500,
  `SelectGrain.padded_side_is_not_the_population`). `select city, species,
  tree_form, min(height) by city, tree_form, sum(height ? min(height) by
  tree_form - count(tree_id) by tree_form < 0) where avg(height) by city,
  tree_form is null` returned every species, not Ulmus alone (spellings seeds
  30670 and 1117). The `?` filter's row stream FULL-joins the trees to
  per-form values that applied the WHERE; the FINAL counted that condition as
  filtering the stream, and refused the aggregate that applied it as the
  population because the stream's keys are NULL on its padded rows. A
  condition applied under a side a join pads now filters none of the joined
  rows (`collect_applied_conditions`), and a partner's NULL-keyed rows count as
  passing the WHERE only when that partner applied it
  (`_PartnerFacts.filtered_nullable`). 6 of the 8 cases in
  `tests/discovery/test_padded_population_gate.py` fail on main (the other 2
  are the `is not null` complement).
- **The batch-only raises were a model with a twin of the WHERE's aggregate**
  (0.3.500). The spellings sweep's "Missing source reference" raises that
  planned alone were read as state leaking between statements. Each one
  traces to a single output an earlier select defined (`v0`), and reproduces
  in a fresh executor with that concept declared: `auto xx <- max(max(height)
  by city, species)` beside `select city where max(height) by city, species
  is not null` raised on the summary model. The model's inner `max(height) by
  city, species` displaced the WHERE-scoped twin as the expression's
  canonical concept, the summary scan output the model's name, and the WHERE
  re-derived its value from lineage over a table with no `height`. The
  statement's own concept now keeps the slot. 5 of the 10 cases in
  `tests/discovery/test_declared_twin_gate.py` fail on main (the 5 on the base
  model are controls).
- **Seeds 60,000-90,000 of the spellings family, and the leads #988 left**
  (0.3.501). On main the range returned 13 wrong answers and 1 raise of
  60,000 queries; each traces to one of eight bugs, all fixed:
  - A filtered row value was read as a value of the select's grain
    (`SelectGrain.filtered_value_is_per_row`, seed 12917). `id ? c` built in a
    select by `(city, species)` takes that grain, and the FD facts read the
    grain as a dependency: a projection grouped by `(city, species, id ? c)`
    claimed `(city, species)` and the count over it was read off each tree as
    `1`. A FILTER value's keys are now its FD row, not its grain.
  - A gate's feeder came back as a row (`SelectGrain.gate_feeder_is_no_population`,
    seed 70075). `select tree_id, species where count(tree_id) by tree_form <=
    count(tree_id) by species and tree_form = 'linden'` returned Ulmus with a
    NULL `tree_id`: the merge hosting the counts beside the scan the row atom
    filtered was left to join typing, which preserved the species feeder. The
    merge now knows the scan applied the row atoms, so it is the population.
  - A selected column was peeled onto a gate's key past a row filter
    (`SelectGrain.peel_skips_row_filter`, seed 65409). `tree_form` was read off
    the enrichment joined to the gate's species, so a species whose only tree
    `height > 2` drops came back. A peel onto a key no select-phase aggregate
    groups by is now made only when every row filter is a value of that key.
  - A rename was re-sourced from an unfiltered scan
    (`SelectGrain.rename_keeps_filtered_rows`, seed 61261, and 62308 and
    87521). `city as v0` was moved off the WHERE's filtered scan onto a fresh
    scan of the trees, and a HAVING `count(tree_id) by city, species` over it
    counted rejected trees. A rename read off a scan holding a WHERE row
    argument keeps that scan.
  - A re-sum of stored counts was joined to the trees
    (`Rollup.resum_beside_finer_rows_multiplies`, seed 70457, and 62014).
    `count(tree_id) by tree_form` beside the summary re-summed its stored
    `count(tree_id)` partials after joining them to the trees on `(city,
    species)`, though the summary supplied every column: each partial counted
    once per tree. An unfiltered row scan finer than an aggregate's input
    grain that supplies nothing the other parents lack is dropped.
  - A stored gate value was paired short of its keys
    (`SelectGrain.peel_paired_short_of_its_keys`, seeds 85553, 21266, 33618).
    A summary column a HAVING reads is a dimension peel at `(city, species)`.
    It exposed only the value (a keyless join), and once it exposed its keys
    the FINAL still paired the per-tree value with it on `species` alone. A
    peel sending FINAL a value now exposes its keys, and a pairing short of a
    peel's keys is underkeyed, so a bridge holding both is elected.
  - A one-row WHERE argument was read off rows carrying it (seed 63248). The
    FINAL re-applying `count(tree_id) by *` read it off the most derived group
    carrying it, the per-tree filter, which pairs with no side; the `by *`
    group computing it is now preferred, and pairs as a scalar
    (`SelectGrain.constant_key_pairs_every_row`).
  - A parenthesized WHERE was one opaque atom
    (`SelectGrain.parenthesized_conjunction_is_its_atoms`, seed 18676). `where
    (a and b)` parsed as `(a and b) = true`, so no rule could place `a` or `b`
    apart, and a select the unparenthesized WHERE planned raised for a keyless
    join. The parser now reads it as the conjunction.
  Reading the parentheses as the conjunction exposed two shapes the opaque
  atom hid (seeds 1554 and 31946, wrong and raising on main without the
  parentheses): `(a and b) and c` now decomposes into three atoms, and a peel
  past a WHERE gate its key does not determine is not made.
  22 of the 29 cases in `tests/discovery/test_spellings_60k_gates.py` fail on
  main. Over spellings seeds 0-90,000 on both models, main returns 14 wrong
  answers and 5 raises of 180,000 queries, the branch none.
- **A total over every row was recomputed from the fact rows beside a
  summary** (0.3.502; `Rollup.total_gate_from_summary`,
  `Rollup.total_gate_from_summary_min`). A WHERE's `min(height) by *` is
  read before the WHERE, over every row, so a summary rolls it up whatever row
  atom sits beside it; so does the total of a select with no WHERE. The
  planner never marked a condition-only total as a summary root, and a select
  total beside rows at another grain planned as its own group over the trees,
  so `select city, species where height > 2 and min(height) by * <= 1`
  scanned the trees twice. 0.3.501's parser fix had made 16 parenthesized
  spellings hit this; before it the opaque atom re-sourced the total through
  a nested search that did read the summary. A one-row total is now its own
  summary-rolled group, cross-joined at FINAL when every atom reading it reads
  only one-row values; a keyless join onto a one-row side is INNER (a FULL
  join on `1=1` padded one NULL row once the gate left the rows empty); and a
  rolled total the plan cannot place falls back alone, keeping every other
  summary read. 5 of the 7 cases in
  `tests/discovery/test_total_summary_reads.py` fail on main (2 are guards).
  Over spellings seeds 0-90,000 x 2 models: summary reads 29,452 to 32,196
  (2,744 gained, none lost), JOINs 499,248 to 497,114, no wrong or raising
  query on either side. Of 0.3.501's 46 lost reads, 22 are regained; the other
  24 read the summary beside the same trees scan on 0.3.500, and 0.3.501's
  plan for them has as many scans and as few or fewer JOINs.
- **A widened join outlived its padding, and a WHERE gate was taken for a
  total of the select** (0.3.503). The four wrong or raising queries the
  oracle sweeps left on main, and one optimizer cycle:
  - `select city, species, tree_form, count_distinct(tag) by species as v0
    where max(height) by city, tree_form != 10` returned a `(B, Quercus)`
    row no tree has (spellings 38273,
    `SelectGrain.widening_outlives_its_padding`). The species table joined
    the per-`(city, form)` gate LEFT, and `ensure_content_preservation`
    widened the later join to the `(city, species)` pairs to LEFT to keep
    its padding. The WHERE proved the gate present and that join narrowed to
    INNER, but the widening stayed. A join now keeps the type it had before
    widening (`own_type`) and takes it back once nothing before it pads
    (`restore_unpadded_joins`).
  - `select count(min(height) by city, species) as v0 where
    (count_distinct(tag) by * = 2 and sum(height) by city < 2)` returned
    `(0,)` where the WHERE keeps no row (first family 5328, and 84194 over a
    summary's re-summed counts; `SelectGrain.gate_total_is_no_group`). Two
    select totals joined on nothing stay FULL, since each is the `()` group of
    its own rows. But the `by *` gate is read before the WHERE and holds a row
    either way: taken for a total, it padded the missing count, and the
    count's zero fill returned 0. A total only the WHERE reads now pairs as a
    one-row side (`SelectGrain.one_row_cross_keeps_rows`).
  - `select tree_form, sum(min(height) by species) as v0 where species =
    'Acer' or sum(height) by * = 5 having count(tree_id) by * < 3` raised a
    keyless join (first family 74021). The FINAL re-applied the row atom for
    contributors its host left unfiltered, after the atom had already been
    copied onto each of them, and FINAL had no key for `species`. That
    re-application is now dropped once the copies cover every contributor.
  - A parenthesized HAVING conjunction ran the optimizer to its pass cap
    (spellings 2936, 4849; `Fixpoint.conjunction_reported_as_new`,
    `Fixpoint.append_held_is_identity`). Pushdown compared the candidate
    `(a and b)` with the parent's AND-atoms one by one, never found it, and
    reported a change each pass that appending (which dedups atoms) never
    made. A conjunction is now held when each conjunct is, and so is an
    atom inside the parent's own parentheses (62992, 89184). No spellings
    query reaches the pass cap over seeds 0-90,000 (50 on main).
  Over seeds 0-90,000 x 2 models of both families, main returns 5 wrong
  answers and 2 raises, the branch none, with no summary read or scan lost
  and 8,933 fewer JOINs.
- **The 90k-150k sweep** (0.3.504). Seeds 90,000-150,000 of both oracle
  families had never run; on main they hold 3 wrong answers and 2 raises:
  - `select city, tree_form, sum(height) by * as v0, sum(height ? sum(height)
    by * >= 2) as v1 where max(height) by city, species is null` returned
    every `(city, form)` the trees hold (spellings 103208, 109374;
    `SelectGrain.total_off_row_stream_keeps_rejected`). The cover read the
    total off the `?` filter's row stream, which skipped the WHERE, and the
    FINAL joined the kept keys onto it. A total is read off its own one-row
    group whenever a stream skipped an atom that holds on an absent group.
  - `select tree_id, city as v0, max(height ? max(height) by city, species >=
    10) having max(height) by city, species < 5` raised (spellings 103923;
    `Rollup.renamed_grain_same_groups`). The HAVING's aggregate reads `by
    species, v0`, and the partial election compared grains by address, so its
    `by city, species` twin looked strictly finer. `_rolls_up_from` now
    compares grains through renames.
  - A `?` filter whose root row stream could not be sourced beside a
    summary-rolled `max(height) by tree_form` was built from its other parents
    and raised a keyless join (spellings 131836, summary only). An unbuilt
    group a reader needs now fails before the reader is built, and the search
    falls back to the base rows.
  - `select max(min(height) by city) as v0, min(min(height) by tree_form) as
    v1 where city = 'A'` read every city for `v1` off the summary (first
    family 93841; `Rollup.split_scan_reads_every_row`). The two rolled roots
    were split into two scans and `city` went with one. A scan-filtered root
    now shares the WHERE's scan, so both read one filtered summary scan.
  Spellings seeds 0-150,000 x 2 models: main 4 wrong and 3 raising, the
  branch none; nothing broken; no summary read lost; JOINs +63 of 816,002.
  First family 0-150,000: main 1 wrong, the branch none, the same reads,
  scans and JOINs.

- **The 150k-210k sweep** (0.3.506). Seeds 150,000-210,000 of both oracle
  families; on main the spellings hold 1 wrong answer, 1 invalid statement
  and 1 raise there, and the full A/B over 0-210,000 found one more wrong
  answer below 150,000 that the last round's sweep had passed:
  - `select tree_id, count(tree_id) by city, tree_form as v0, tag as v1 where
    not (min(height) by species != count_distinct(tag) by tree_form)` returned
    every tree (spellings 185026; `SelectGrain.pruned_coalesce_member_pads_gate`).
    The FINAL paired `min(height) by species` on `tree_form` from the row
    stream and from the LEFT-joined count; the pair reduction pruned the row
    stream's pair as implied by `species`, which left the count's padded
    `tree_form`. Several sides of one key are their coalesce, so a group is
    kept whole and `prune_outer_join_pairs` keeps the side nothing pads.
  - Two plans for one merge joined a one-row gate and a row stream on
    nothing, in opposite orders; merged into one CTE, it referenced the gate
    twice (spellings 177430, invalid; `SelectGrain.mirrored_cross_join_is_one`).
    `coalesce_duplicate_joins` now keeps one keyless cross join per pair of
    sides.
  - `min(height) by tree_form`, re-aggregated from `min(height) by species`,
    shared a group with `sum(height) by tree_form`, which a `?` filter beside
    `min(height) by species` read: the two groups read each other and the
    group graph raised a cycle (spellings 172552, summary only;
    `Rollup.merged_groups_read_each_other`). Groups that read each other split
    by each member's depth; no other group changes.
  - `select species, tree_form, min(height) where (max(height) by species > 8
    or city = 'B') and max(height) by city > 20` returned one row of NULLs
    (spellings 11684; `SelectGrain.final_reads_finer_column_off_gate`). FINAL
    re-applied the OR for the `tree_form` projection and read `city`, finer
    than its species rows, off the city gate joined FULL on nothing. A
    projection the host grain pins is re-filtered at FINAL only when the
    atom's columns are functions of that grain.
  - Efficiency: `select city, species, sum(height) by city` scanned the trees.
    An aggregate whose grain does not determine the select's rows is now
    re-summed from the summary at its own grain and joined back on it
    (`Rollup.coarse_resum_joined_on_its_grain`), when its grain is a strict
    subset of the row keys, no WHERE filters its rows and no aggregate at the
    select's rows groups by it; read at row grain it is wrong
    (`Rollup.coarse_read_at_row_grain_misreads`).
  - Spellings 0-210,000 x 2 models (420,000 queries): main 4 wrong, 1 raising
    and 2 invalid, the branch none; nothing broken; summary reads +755, scans
    -542, JOINs -2,008. The first family: nothing wrong on either side; summary
    reads +425, scans -401, JOINs -3,319.
- **The 210k-270k sweep** (0.3.508). Seeds 210,000-270,000 of both oracle
  families; on main the spellings hold 3 wrong answers and 1 invalid
  statement there, the first family none:
  - A `?` filter's re-sum of the summary beside a row scan the WHERE filtered
    joined each partial once per kept tree (spellings 262499, 267243;
    `Rollup.filtered_passenger_multiplies`, `Rollup.key_filter_is_semijoin`).
    A filtered finer root scan is dropped as a passenger when another root
    applies the same atoms.
  - A value peeled onto the summary at a key the WHERE's atom is a function of
    was joined back outer unfiltered and kept every key (spellings 237480;
    `SelectGrain.unfiltered_peel_keeps_rejected`). An aggregate peel the row
    stream's keys determine now carries the atom; a peel of plain columns does
    not (TPC-H q03's SQL grew 30% when it did).
  - Two merged plans each joined one CTE from their own side, referencing it
    twice (spellings 243699, invalid on the base model;
    `SelectGrain.rejoined_cte_is_one_conjunction`). `coalesce_duplicate_joins`
    folds an INNER re-join onto the join that brings in its later side.
  - Efficiency: `sum(height) by city where city = 'A'` beside the select's
    `(city, species)` rows re-sums the summary when every WHERE column is a
    function of the aggregate's keys
    (`Rollup.group_filter_commutes_with_resum`).
  - Spellings 0-270,000 x 2 models (540,000 queries): main 3 wrong and 1
    invalid, the branch none; nothing broken; summary reads +302, scans -228,
    JOINs +1,540. The first family: nothing wrong on either side; summary
    reads +317, scans -375, JOINs +699.

- **The 270k-330k sweep** (0.3.509). Seeds 270,000-330,000 of both oracle
  families; on main the spellings hold 2 wrong answers and 3 raises there, the
  first family none:
  - A `tree_form` lookup beside keys split over two WHERE aggregates
    (`max(height) by species`, `min(height) by city, species`) was not counted
    as covered, so FINAL never re-applied the WHERE and read `(city, species)`
    off the unfiltered group (spellings 304584;
    `SelectGrain.lookup_adds_no_tuple`,
    `SelectGrain.lookup_beside_split_keys_pairs_unkept`). A lookup a host's
    grain determines now counts as covered.
  - A bridge or a merged side exposing a column beside its planned keys
    (`city`, `tree_form` on a `?` filter; `tree_form` on a `dim:species`
    projection) was read as joining two islands, so the bridge that pairs them
    was skipped and the merge raised a keyless join (spellings 310634,
    273853; `SelectGrain.keyed_lookup_keeps_rows`,
    `SelectGrain.exposed_column_joins_nothing`). Islands are now built from
    the keys the plan pairs each side on.
  - Spellings 0-330,000 x 2 models (660,000 queries): main 2 wrong and 3
    raise, the branch none; nothing broken; summary reads +1 and scans +3 (the
    queries that raised now run), JOINs -14,476 (9,059 queries lost a JOIN, 270 gained one). The first family: nothing wrong on
    either side; summary reads and scans unchanged, JOINs -15,703 (11,108
    queries lost a JOIN, 19 gained one).

- **The 330k-390k sweep** (0.3.510). Seeds 330,000-390,000 of both oracle
  families; on main the spellings hold 1 wrong answer there (on both models),
  the first family none:
  - Two HAVING groups, `by species` and `by city, tree_form`, beside tree rows
    shared only `tree_form`, which identifies neither. The join plan paired
    them on it, so each tree met every species of its form and came back once
    per species (spellings 356401; `SelectGrain.blend_beside_finer_rows_fans_out`,
    `SelectGrain.unique_side_one_partner`). A blend between two groups that
    another supplier's rows determine is now underkeyed, and the row stream
    is elected to pair each on its own keys.
  - The re-sweep of 0-195,000 found three seeds main gets wrong, raises on or
    renders invalid SQL for, though 0.3.509's sweep reported them clean (its
    cached records predate #1014):
    - FINAL held the trees and their WHERE-filtered copy, both identified by
      nothing, and joined them only through the `(city, tree_form)` group, so
      every species of a kept form came back (62069;
      `SelectGrain.filtered_copy_paired_through_group_keeps_rejected`,
      `filtered_copy_paired_on_keys_exact`). Two scans identified by nothing
      now pair, as a lookup, on the keys the merge's suppliers are identified
      by.
    - A keyless INNER join was kept in both orientations when two CTEs
      merged, so neither side could be the base (96191). Its `unique_id` is
      now orientation-free, as a keyed INNER's already was
      (`SelectGrain.mirrored_cross_join_is_one`).
    - `JoinHoist` moved a join into a CTE and mapped every column of the
      joined CTE, one it hides included, so the SQL read a column the CTE
      does not project (153508). Only projected columns are mapped.
  - Efficiency: an aggregate by a property of the row keys (`by tree_form`
    beside `species`) is re-summed off the summary and joined back on the
    property (`Rollup.property_resum_exact`); a grain mixing a row key in
    (`by city, tree_form`) stays on the rows (`Rollup.mixed_grain_pairs_on_property`).
  - A/B: Spellings 0-390,000 x 2 models (780,000 queries) on the branch:
    nothing wrong, raising or invalid. On main, 356401 is wrong in the new
    range and 62069, 96191 and 153508 are wrong, raise or render invalid SQL
    below it.
- **A third oracle family, and the 390k-450k sweep** (0.3.511). The
  `expressions` family (`tests/discovery/test_oracle_expressions_laws.py`)
  writes what the first two never do: arithmetic over aggregates at one or
  two grains (`sum(height) / count(tree_id) by *`), an aggregate of a row
  expression, a property or derived value as a group key (`tag`, `height > 2
  as big`), and a totally ordered `order by ... limit`. On main 94 of its
  first 6,000 seeds are wrong, raise or render invalid SQL; the two older
  families hold 4 more in seeds 390,000-450,000:
  - `count(tree_id) + 1` is computed off its group's rows, and no row of a
    group reaches a column the group did not output. The join plan said the
    value could be widened to `tree_form`, which its species determines, and
    paired it with a `by city, tree_form` value on both keys; the built
    sides joined on `city` alone, or on nothing
    (`SelectGrain.value_over_group_holds_no_finer_key`,
    `value_over_group_bridged_exact`). A BASIC over grouped rows alone
    carries what they output, is a group's side when blends are counted, and
    the row stream holding both keys bridges it. A group at its input rows'
    own grain (`sum(height + 1)` by `tree_id`) is those rows, and still
    widens.
  - A bridge elected to key a pairing, not to join islands, was joined only
    at FINAL. `max(height) by city, tree_form * count(tree_id)` at `tree_id`
    merges its two inputs below FINAL, where the bridge was skipped because
    they already shared `city`
    (`SelectGrain.keying_bridge_joined_whatever_the_islands`). A BASIC's
    merge now keeps its keying bridges; an aggregate's does not, since a
    bridge joined whatever the islands repeats rows it would count.
  - The all-rows marker in a side's identity (`count(tree_id) / count(tree_id)
    by *`) was read as a key no bridge pairs on and no pairing determines, so
    the bridge was never elected and a lookup was a blend. A side grained by
    a value another side supplies (`v1` beside `tree_id`) is bridged on the
    keys that determine it.
  - A value at `(city, species)` over one input by species and one by city,
    tree_form paired them on `tree_form`: `(B, Quercus)`, a pair no tree has,
    came back (`SelectGrain.blend_under_a_finer_reader_invents_pairs`). A
    blend both sides of which the reader's grain determines is underkeyed.
  - `height > 2` is NULL where the height is. A comparison was read as never
    NULL, so a re-sum joined back on the derived key with a plain `=` and the
    NULL group's rows left a `by *` total
    (`SelectGrain.null_key_group_is_a_group`). A comparison or `between` of
    a nullable argument is nullable; `is [not] null` is not.
  - `select tree_id, sum(height + 1) * 10` rendered `height + 1 * 10`: an
    aggregate of one row is its argument, written bare into the expression
    reading it (`Scalars.bare_argument_reassociates`,
    `collapsed_argument_exact`). The collapsed argument is parenthesized.
  - Off the summary, a value over a partial (`max(height) by * - max(height)`
    by a derived form, `count(tree_id) * 10`) was computed on the summary's
    rows, one value per species, and a dedup kept each
    (`Rollup.value_off_a_partial_is_per_summary_row`). The partial is
    re-summed to its own grain before a BASIC reads it. This also plans the
    two shapes #929 left raising (`select g, level, scaled`).
  - A WHERE atom finer than a re-sum was applied above it: `select
    min(count(tree_id) by tree_form), max(height) where city = 'B' and
    species = 'Acer'` summed the stored counts by species, then joined the
    summary back to apply `city = 'B'` (first 446174, spellings 390302;
    `Rollup.filter_above_resum_keeps_rejected`, `filter_below_resum_exact`).
    The root fallback that filters above an unconditioned source declines
    when that source re-summed partials a row atom, or a gate's grain,
    splits; the rows answer. A total (`by *`) splits nothing and still reads
    the summary.
  - A HAVING `sum(height) by city, tree_form` re-summed the stored sums after
    FINAL joined them to a per-tree side on `(city, species)`: each partial
    counted once per tree (spellings 430944;
    `Rollup.resum_beside_finer_rows_multiplies`). A row stream finer than the
    partials' rows is taken once per row of theirs first.
  - A HAVING on a value computed off a re-summed partial (`count(tree_id) +
    min(height) by city, tree_form` off the summary, `having v1 <= 1`) was
    rendered in the WHERE of the CTE that re-sums it, which DuckDB rejects.
    An atom reading a value computed there off a rollup carrier is a HAVING
    atom, as one reading the carrier already was.
  - A/B: see the PR for the sweep totals. Left open: spellings 404253
    (invalid SQL); the expressions seeds it left open 0.3.512 fixed.
- **The expressions family to 60,000** (0.3.512). On main 26 of its 120,000
  queries are wrong and 103 raise; the branch leaves 6 and 7, and the two
  older families over the same seeds do not move. Fixed:
  - An expression of aggregates took the union of their `by` keys as its
    grain, and a bare aggregate added none: `count(tree_id) * min(height) by
    city` at `(city, species)` was keyed by `city`, grouped by `(city,
    value)` and joined back on `city`, so each species met its city's other
    values (2171, 4244; `SelectGrain.by_keys_alone_fan_out`,
    `bare_beside_by_is_select_grain`). With no `by` the grain walk descended
    into the aggregates' inputs: `count(tree_id) - count_distinct(tag)` was
    keyed by `tree_id`, and a summary row read beside it repeated per tree
    (43598). A bare aggregate is at the select grain; `grouping()` is a key
    of its pass, not a bare aggregate.
  - A re-sum of a partial (`count(tree_id) by *` off the per-`form0` counts)
    was judged to read rows no WHERE host filtered, since no input produces
    the column its function names, and a row atom was copied onto it where
    that column does not exist: `Missing source reference` (3102, 4180, 5715,
    5780, 48214; `SelectGrain.resum_reads_partial_rows`). A re-sum reads its
    partial's rows; the uncovered-host pass visits groups upstream first so
    a partial it filters covers its re-sum.
  - Bridge candidates were ranked by name on a tie, and `root_d1`, the
    condition-phase scan whose rows never passed the WHERE, sorted first:
    every tree came back beside LEFT-joined aggregates (33164;
    `SelectGrain.unfiltered_bridge_keeps_rejected`). A condition-phase
    group bridges an outer consumer last.
  - A derived key's lookup (`coalesce(tree_form, 'none')`) was regrafted off
    the gated rows onto a fresh scan of the forms when no WHERE row argument
    was a member of the rows; a gate filters them through a constraint edge,
    so the WHERE was dropped (9445, 58034;
    `SelectGrain.dimension_rescan_drops_gate`).
  - A `by *` total's table was peeled from the row stream under a WHERE when
    no atom read it, though it joins the WHERE's table: off the summary,
    `count_distinct(tag) by *` read every tree under `tree_form = 'linden'`
    (30258, 31752, 33606; `SelectGrain.peel_of_joined_table_reads_rejected`).
    A table sharing a column with the row stream's tables is not peeled.
- **The expressions family to 100,000** (0.3.513). On main 24 of its 200,000
  queries are wrong and 13 raise; the branch leaves 0 and 9 (the older
  families' seeds 0-60,000 lose their 5 wrong and break none). Fixed:
  - A sole side computing a value at `(city, form)` met a FINAL gate at
    `(city, species)` on `city` alone, keeping a form all of whose rows the
    gate rejected (38531), or on nothing, a keyless join (47126, 73006;
    `SelectGrain.sole_gate_paired_short_of_its_grain`). The side is
    widened to the gate's keys its rows hold, and the gate's join plan reads
    the side as built.
  - A finished bare aggregate joined beside a per-row stream was re-summed
    as a stored partial, once per row (99020, 99897;
    `SelectGrain.finished_value_resummed_per_row`). Only what a node holds as
    a rollup is a partial.
  - A side past a gate the merge re-applies was kept whole by a RIGHT join,
    and a gate holding on NULL (`sum(height) by city is null`) passed every
    rejected key's padding (2287, 7677, 23288, 42137, 50995, 60555, 62086,
    63137, 89358, 93329;
    `SelectGrain.gate_on_padding_passes_rejected`).
  - A constant carried at a key beside a gate was built as a bare constant
    claiming the key, and nothing sourced it.
- **The expressions family to 150,000** (0.3.514). On main 1 of its 300,000
  queries (seeds 0-150,000) is wrong and 13 raise; the branch leaves 0 and
  8, and breaks none. Fixed:
  - A value over a summary's rows by `species` beside a value `by tree_form`
    raised a keyless join: the plan pairs the two on `tree_form`, which
    `species` determines but the summary does not store, and no built group
    produced it keyed by `species` (62974, 66824, 105627, 127542, 147680).
    A planned key no built producer can look up is looked up off a scan of
    it beside its own keys, when the side holds them
    (`SelectGrain.lookup_keeps_rows`).
  - A value at `(city, tree_form)` over two aggregates, bridged by the rows
    by `(city, species)`, was one row per species, and FINAL joined each
    repeat back to the rows (112406;
    `SelectGrain.bridge_joined_whole_repeats_its_pairs`). A value over groups
    alone whose built rows are finer than its outputs is deduped to them
    before FINAL pairs it.
- **The expressions family to 200,000** (0.3.515). On main 4 of the
  100,000 queries over seeds 150,000-200,000 are wrong and 5 raise, and 10
  of 300,000 over seeds 0-150,000 raise; the branch leaves 0 wrong and 6
  raises (the `OPEN` leads), breaks none, and plans 1,369 fewer joins and 96
  fewer scans. Fixed:
  - A value over groups by `species` beside a value `by tree_form`, off the
    summary, was never widened to the sibling's key and no row stream held
    `tree_form` to bridge (17715). It looks the root property up off a scan
    beside its keys, unless another side holds the two together.
  - A per-tree value beside a value `by tree_form`, off the summary: the
    bridge holds `tree_form` below but never carried it (139791, 160693,
    160820, 177047, 187270, 187859, 28266, 75390). An island bridge is
    widened to the ROOT keys it joins on.
  - Beside a constant, the trees were wrapped and deduped to `city`,
    dropping the `species` they pair with `tree_form` on (193139;
    `SelectGrain.wrap_without_pair_key_crosses`). A wrap keeps the keys its
    side pairs on.
  - FINAL LEFT-joined a value gated by `sum(height) by tree_form is null`
    and re-applied the gate, which its padding passes (176152;
    `NullReject.gated_side_padding_passes_gate`). Such a side is joined INNER.
- **The expressions family to 350,000** (0.3.516). On main 2 of its 700,000
  queries (seeds 0-350,000) are wrong, 79 raise and 2 render invalid SQL;
  the branch leaves 0 of each and breaks none, at 407 more joins and 91
  more scans (over 50 queries). Fixed:
  - A value over aggregates at two grains, under a WHERE atom reading both:
    each group hosting the atom was said to hold the other's key, and the
    merge joined them ON 1=1 (205085, 78245;
    `SelectGrain.two_grain_value_without_its_pairs_invents`). A group holds
    no column of an atom it only passes through.
  - An atom hosted where `min(height) by city, species` was never joined,
    rendering the min at the host's grain (246819, 333909;
    `SelectGrain.host_grain_pin_misreads`).
  - An expression of constants was planned at no grain, and a `by *`
    aggregate inlined into a value made the all-rows constant look read by
    the select's grain; both take the select's grain.
  - A literal beside a WHERE aggregate its grain has no key for was gated by
    that aggregate alone and joined it on no key (67331, 104893, 349206;
    `SelectGrain.constant_gated_off_its_rows_invents`). A constant takes all
    of a WHERE's row feeders or none, and never hosts its atoms.
  - Reading stated columns (#1038), the FINAL fold dropped a side rendering
    a literal folded onto a sibling, or a key another side was owed but
    never given (24 seeds, e.g. 495, 21365, 99561). Folded constants count
    as the side's columns; undelivered owed keys do not.

- **The expressions family to 500,000** (0.3.519). On main its 300,000
  queries (seeds 350,000-500,000) are all right but for 68 the generator
  rejects; the branch breaks none, at 1,270 fewer scans and 2,313 fewer
  CTEs. Fixed:
  - Wrapping a join and joining a wrapped side spelled one identifier
    (`A_join_B_at_g` + `_at_g` is `A_join_` + `B_at_g_at_g`), so FINAL's
    merge folded its two parents into one, kept one of their joins, and read
    a column off a CTE it never joined (spellings 404253, the last open
    seed; `Identity.naive_wrap_of_join_collides`). A join's members are
    bracketed.
  - A scan filtered at plan time and its twin filtered by pushdown read one
    table under one WHERE twice (362641, 364578;
    `Identity.twin_scans_share_one_read`). `MergeTwinScans` folds an
    ungrouped, unjoined scan into a twin of the same table, condition and
    grain.

- **The expressions family to 650,000** (0.3.520). On main its 300,000
  queries (seeds 500,000-650,000) hold 2 wrong and 1 raising beside the 68
  the generator rejects; the branch fixes all three, breaks none, and plans
  5,857 fewer scans. Fixed:
  - A group never marked an aggregate over a nullable argument nullable, so
    FINAL's re-join of the `(city, tree_form)` group paired `max(height) +
    1` with `=` and dropped the group whose heights are all NULL (645783;
    `Nullability.max_of_null_group_is_null`,
    `Nullability.plain_rejoin_drops_null_value`). Any aggregate but a count
    over a nullable argument, and what propagates it, is nullable at its
    group.
  - A projection took a `by *` total's grain key that its merge parent hid
    as an input and raised (564837, summary model). `parent_outputs_needed`
    reads only a parent's usable outputs.
  - A grouped scan beside its ungrouped twin under one WHERE read the table
    again (500002; `Identity.group_reads_twin_rows`). `MergeTwinScans` groups
    it over the twin's rows when it reads only plain columns; a summary
    re-sum stays on its own scan.
  - The law suites caught two shapes the first cut broke: a group whose
    `sum` turned nullable, beside rows whose own WHERE atom proved it
    non-null, was preserved for its NULL group and invented a city
    (spellings 7067). A NULL key on one side that the other side's filters
    reject now preserves nothing (`_nullable_join`).

- **The expressions family to 800,000** (0.3.521). On main its 300,000
  queries (seeds 650,000-800,000) hold no wrong or raising seed beside 48
  refused, and seeds 650,000-725,000 with `--carry-state` none
  either. The branch breaks none and plans 1,189 fewer scans. Fixed:
  - A scan under a WHERE beside an unfiltered scan of the same table read the
    table again: the WHERE's `by` aggregates read every row, the select its
    kept rows (650115; `Identity.filter_reads_whole_twin`). `MergeTwinScans`
    rebases the filtered scan, grouped or not, over the unfiltered twin and
    applies its own condition there.
  - The A/B caught one shape the first cut broke (777104): the twin gained
    the read columns without the table's nullability, so
    `StripRedundantNotNull` read `height is not null` as tautological and
    dropped it. A column the rebase adds carries its table's claim.
  - Found probing the rebase, not by the sweep (the generator writes no
    membership): an aggregate a WHERE reads inside a membership's set and
    as an atom of its own raised `DisconnectedConceptsException`
    (`species in (species ? count(tree_id) by species > 1) and count(tree_id)
    by species > 0`). Its only consumer was the set, a side channel, so the
    backfill that joins a WHERE-only aggregate to the outputs skipped it. The
    backfill now looks past existence-only consumers
    (`tests/discovery/test_membership_beside_where_aggregate.py`).
  - A keyless select refused to compute the aggregate its WHERE gates on when
    both spelled it alike with an explicit `by` (`count(tree_id) by *` in
    each, 652494): the guard against a HAVING written as a WHERE matched it,
    though the WHERE reads every row at its `by` and the select the kept
    rows, and the planner keeps the two apart. Only the bare spelling is
    refused now. Every seed main refused in the swept ranges now plans and
    matches the oracle: 324 queries of the expressions family to 800,000,
    508 of `first` and 832 of `spellings` to 450,000
    (`tests/discovery/test_where_by_beside_select.py`).

- **A fourth family, `compositions`, to 30,000** (0.3.522). It composes the
  first three families' atoms with what none of them wrote: a membership
  (`x in (x ? c)`) beside the WHERE's own atoms, `/` by a value that can be
  0, an aggregate `by` a derived key, a nested aggregate inside arithmetic, a
  rank over the select's groups, and an `order by` an expression
  (`tests/discovery/test_oracle_compositions_laws.py`). On main its 60,000
  queries hold 476 wrong, 13 raising and 1 invalid. The branch leaves 35 wrong
  and 7 raising, breaks none, and plans 6 more scans and 118 more joins in
  60,000 queries. Fixed:
  - A membership OR'd with a gate beside a `by *` total was applied only to
    the total's input, so the key rows were every species
    (337; `SelectGrain.membership_left_off_keys_keeps_rejected`, the law
    `SelectGrain.membership_keys_are_kept_keys`). Placement now re-applies a
    set-reading atom at FINAL when the outputs hold every key its gates pair
    on, and copies it onto a select aggregate its host does not feed, as a
    row atom is.
  - A scan with no join read its first source's grain, which under a
    membership could be the set's (one row per `tree_form`), so a count over
    the trees collapsed to a 0/1 flag (5544;
    `SelectGrain.set_grain_is_no_row_grain`). The grain skips a source only a
    subselect reads.
  - `DropIdentityGroup` took a membership's set as the row source of a
    DISTINCT and dropped the GROUP BY (2087). A set the condition reads is no
    row source.
  - A projection grouped by a derived key's own grain (`species` of `fc`), so
    `(city, fc)` came out once per species, and FINAL trusted the grouping
    (2081; `SelectGrain.derived_key_grain_splits_its_tuple`). FINAL dedups a
    grouped node that holds a key its outputs do not determine.
  - On the summary model a value over a total (`min(height) + 0`) beside a
    nested aggregate read the total off the per-species partials the finer
    filter rollup kept for the nested one (876;
    `SelectGrain.partial_read_as_total_splits`). A projection that reads a
    partial its parent still holds per finer key now groups, which re-sums
    it, and a CTE re-summing a partial into a value guards an empty total
    (`count(*) > 0`) as an aggregate does (29674).
  - Two memberships whose sets read aggregates at the grains of the WHERE's
    own gate raised `Graph contains a cycle` (9202): the cyclic-group split
    followed lineage edges only. It follows a set's existence edges too.

- **`compositions` to 60,000** (0.3.523). On main seeds 0-60,000 (120,000
  queries) hold 89 wrong, 26 raising and 5 invalid; the branch leaves 26
  wrong, 23 raising and 1 invalid, breaks none, and plans 1 more scan, 262
  more joins and 84 fewer CTEs. Fixed:
  - A gate keyed off the outputs beside a membership (`max(height) by
    species < 1 or species in (...)` beside `city, tree_form`) was applied to
    the select aggregate's input only, so FINAL's key rows kept the rows it
    rejected (17410 and nine more;
    `SelectGrain.gate_off_outputs_keeps_key_rows`). FINAL re-applies a
    set-reading atom when a select-phase contributor holds its gates' keys or
    looks them up off a held key that is not the rows' identity; a gate that
    holds on an absent group still needs its keys held outright. A FINAL
    argument is read off an aggregate group only if the group computes it,
    never off one whose stated set carries it beside (25935).
  - On the summary model an aggregate the scan reads off a finer stored
    partial lost the partial column's `?`, so a value re-summed from it was
    paired with `=` and its NULL group came out twice (12435;
    `SelectGrain.resum_of_nullable_partial_is_nullable`).
  - A projection re-summing partials at a derived key (`sum(h) + count(id)`
    by `fc`) was widened with the key's own grain (`species`), one row per
    species (33686 and ten more; `SelectGrain.resum_split_by_key_grain`).
  - A summary's coarse rolled bucket shared its id with the entity dimension
    peeled onto the same key, so the dimension's scan was asked for the
    stored aggregate (`Missing source reference`, 28945). The rolled bucket
    has its own id.
  - An aggregate over a value its own CTE re-aggregates from a partial
    (`sum(max(h) by city)` grouped by city) rendered `sum(max(...))`, which
    the engine rejects (33591, 35053, 35546, 56071;
    `SelectGrain.aggregate_of_group_constant`). It renders the single-row
    formula of the re-aggregate.

- **`compositions` to 90,000** (0.3.524). On main seeds 0-90,000 (180,000
  queries) hold 44 wrong, 34 raising and 2 invalid; the branch leaves 10
  wrong, 8 raising and 2 invalid, breaks none, and plans 304 fewer scans,
  531 fewer joins and 977 fewer CTEs, with no summary read lost. Fixed:
  - Values by `city` and by `fc` met only through two groups at once (the
    `(city, species)` rows and `fc` by species); the bridge search tried
    one group at a time and FINAL cross-joined the values, so a form-less
    species' `(A, o)` leaked (15258, 44364;
    `SelectGrain.keys_off_two_values_cross`). Two groups are tried together
    once no single one joins an island.
  - A column a membership's set read was dropped from the select's rows
    though it also keyed a WHERE gate (`count(id) by city > 2` beside `species
    in (species ? city = 'A')`), so the gate joined the rows on nothing:
    most `keyless join` raises, and wrong key rows (9016, 24246, 26698 and
    seven more; `SelectGrain.gate_key_read_by_a_set`). A root is the set's
    alone only when everything downstream of it ends in a set.
  - A projection grouped by a derived key a sibling value is grouped by was
    widened with the key's own grain (50815;
    `SelectGrain.derived_key_grain_splits_its_tuple`).
  - A set feeds no rows. A group gated by one carried a value across its
    existence edge and recomputed it at its own grain
    (`count_distinct(tag) by form` per species; 48060, 54254, 55868;
    `SelectGrain.distinct_count_at_a_finer_grain`); FINAL elected a set's
    own rows as a bridge (55114) and read a gate's value off them (46333;
    `SelectGrain.set_rows_are_no_bridge`).
  - A gate FINAL re-applies beside a total paired the value's side on part
    of its grain, though the side carried the rest (63640, 60061;
    `SelectGrain.gate_paired_short_of_its_grain`): the merge pairs on the
    plan that asked for the carried keys.
  - Two plans of one CTE nesting the same WHERE differently (`(a and b) and
    c`, `a and (b and c)`) raised `Attempting to merge two ctes with
    different conditions` (49126, 59820; `Logic.and_assoc`). They merge on
    their atoms.

- **`compositions` to 130,000** (0.3.525). On main seeds 0-130,000 (260,000
  queries) hold 13 wrong, 11 raising and 3 invalid; the branch leaves 0
  wrong, 7 raising and 2 invalid, and breaks none. The fixes cost 5 scans and
  53 joins; the queries already right plan 27 fewer joins and 94 fewer CTEs,
  and no summary read is lost. Every fix is one of two readings: a membership's set is read by
  its own membership and feeds no rows, and a stored partial is no finished
  value until it is re-aggregated at its own grain. Fixed:
  - A set in an OR was ordered before every row argument of the OR, the WHERE
    aggregates included, so two sets each computed from an aggregate the
    other preceded closed a cycle (41277, 66218 raised `Graph contains a
    cycle`; `SelectGrain.set_read_by_its_own_membership`). A set now precedes
    only its own membership's left side.
  - A per-species gate beside a membership was asked of the root's scan, and
    the summary bound it to its `(city, species)` count (37601, 77768,
    85861; `SelectGrain.partial_is_no_gate_value`). A cross-row argument of a
    membership atom is fed and joined back like any gate.
  - A stored `by *` total a set reads was scanned beside the set's keys in
    one condition scan and re-maxed per species (47456;
    `SelectGrain.stored_total_not_remaxed`). A one-row root is its own
    condition scan, as in `partition_roots`.
  - A scan with a set's feeder as its one parent was read as at the feeder's
    grain, and the feeder's own `species = 'Acer'` as a pin on its rows, so a
    nested aggregate skipped the re-aggregation of its stored partials
    (23531, 15155, 34415, 84985; `SelectGrain.feeder_is_no_row_parent`).
  - A value over stored partials (`sum(h) by form - count(id) by form`) was
    computed per summary row and carried as a join key, so FINAL paired it
    with the finished value and kept nothing (38002, 94262;
    `SelectGrain.value_off_partials_is_no_key`). A key reading a pending
    partial is not carried.
  - A set's condition at keys its content lacks (`city ? count(id) by city,
    form > 3`) was answered by a stored count re-summed to the city (83051;
    `SelectGrain.set_condition_at_its_own_keys`). Such an aggregate is not
    read off a summary scan of the content's keys.

- **`compositions` to 200,000** (0.3.526). On main seeds 0-200,000 (400,000
  queries) hold 2 wrong, 14 raising and 8 invalid, all but three on the
  summary model; the branch leaves none and breaks none. The fixes cost 12
  scans and 39 joins, all in the queries they fix; the queries already right
  plan 10 fewer joins and none more, and no summary read is lost. Fixed:
  - A value the child of a merged aggregate groups by, or reads as its own
    input, was kept as a partial of the parent, so `select city, min(height)
    by city, species` re-minned the stored per-species minima to the city
    (found by a probe; `SelectGrain.value_at_its_own_grain_kept`).
  - A value a source stores at its own grain (`max(height) by tree_form` off
    the summary) was hosted on a row-preserving aggregate over another stored
    value, whose rows hold none of its keys, and the join back had no key
    (89278, 108631; `SelectGrain.stored_value_is_no_host_key`). It still
    rides a host over rows, or one at its own grain.
  - A filtered set's content column was read as a planned group, and the
    merge of the set beside its membership raised `Merge sources` (5754,
    32125; `SelectGrain.set_column_is_no_row`). The gate a FINAL condition
    reads is no longer produced by a set's own filter group (136658), and a
    semi-join repointed with the CTE it reads (152426).
  - A cross join to a CTE already joined INNER with no condition was emitted
    twice (`Ambiguous reference`; 32239, 71000); a window parent FINAL read
    a value through was sliced to its own outputs (13009, 127138); and a
    membership side's hidden column the WHERE reads stayed hidden (91633).

- **A fifth family, `windows`, to 12,000** (0.3.528). It writes what the
  fourth named as left: windows other than a single-term rank over the
  select's groups (`row_number`, `dense_rank`, `lag`, `lead`, running and
  partition aggregates, in both spellings), a membership inside a select
  target's `?`, a membership in a HAVING, and `/` by a `by` aggregate that can
  be 0 (`tests/discovery/test_oracle_windows_laws.py`). On main its 24,000
  queries held 971 wrong answers, 155 raises and 16 invalid statements; 21
  are left (6 wrong, 8 raising, 7 invalid, each `OPEN` in the suite), and no
  query that planned on main fails. The four older families, 20,000 queries
  each, are unchanged.
  - A window *reading* an aggregate with no `by` (`lag(min(h)) over (order
    by tall)`) was keyed on its order column's own key, so it ran over the
    trees and each group met itself as its predecessor: six rows for three
    groups (`Windows.property_key_rows_repeat_groups`,
    `Windows.repeated_group_lags_to_itself`). Only a window ordered by such
    an aggregate inherited the select grain; one reading it now does too
    (`Windows.window_over_groups_reads_groups`). 258 of the first 375
    failures were this.
  - A window's merge dropped the bridge the join plan elected to key a blend,
    so a value by `(city, tree_form)` met the trees it numbers on `city`
    alone: twelve rows for six trees
    (`SelectGrain.keying_bridge_under_window`). A window counts repeats as a
    BASIC does not, and now joins the bridge.
  - A set feeds no rows, also when it is written in a select target's `?`:
    the FINAL cover read `tree_form` off the set's own feeder
    (`SelectGrain.set_supplies_no_output`); a derived key was computed off
    it (`SelectGrain.set_is_no_row_parent`); and a filtered column's stream
    took its grain from the set beside it, so the aggregate over it was never
    computed (`SelectGrain.set_source_is_no_grain`).
  - A set written in the select or the HAVING reads the rows the WHERE kept
    (`SelectGrain.select_set_reads_kept_rows`): the two models disagreed, and
    under a WHERE aggregate neither applied it. An aggregate and the set
    reader computed from it are two build layers; the standalone feeder is
    planned under the statement's WHERE; and `PredicatePushdownRemove` no
    longer reads a set's feeder as a row parent that already applied an atom.
    A summary no longer answers `agg(x ? k in <set>)` from its partials: two
    memberships on one column read the feeder's own column as the row's.
  - A contributor folded into a sibling kept its WHERE only through a
    one-row total, which restricts no key: `select species, tree_form,
    max(max(h)) over () where city = 'B'` returned all four species.

- **The `windows` family to 60,000** (0.3.529). Seeds 12,000-60,000 (96,000
  queries) held 54 wrong answers, 51 raises and 18 invalid statements on main;
  25 are left (16 wrong, 9 raising), each failing on main too, and no query
  that was right on main fails. Nine of the 13 seeds 0.3.528 left `OPEN`
  pass. The four older families, 20,000 queries each, hold nothing new; the
  cost is joins (+433 in the family's 120,000 queries, +143 in `spellings`,
  +10 in `compositions`): a filtered stream is now paired on the keys it is
  asked to carry.
  - Two filtered columns of one aggregate (`select city, count(id ? city in
    <set>), count(id ? tag in <set>)`) are two streams of the trees. Each
    claimed the select's grain, so they were paired on `city` and every tree
    of a city met every tree of it: 16 for 4 (`Rows.coarse_pair_squares`;
    with no keys, a keyless join). A consumer that counts repeats asks its
    filtered row parents for the rows' identity, and a filtered stream is
    named by the keys it is asked to carry (`Rows.identity_pair_exact`).
  - A filtered column beside a window was kept as a key of the window's
    rows, so `row_number() over (partition by tree_form order by species)`
    numbered a species once per tree
    (`Windows.filtered_passenger_repeats_groups`).
  - A filter reading values at two grains (`city ? sum(h) by species <
    min(h) by city, tree_form`) joined them on `tree_form` alone and passed a
    city for a species it does not hold
    (`SelectGrain.blend_under_a_finer_reader_invents_pairs`): a filter's
    merge joins the bridge elected to key a blend, as a BASIC's and a
    window's do.
  - A derived key (`fc`) was computed off a group by `(city, tree_form)`
    that happened to hold its input, though `count(id) by fc` pairs it with
    the trees on `species`, which that group dropped: a keyless join
    (`SelectGrain.derived_key_off_groups_pairs_on_nothing`). A value with a
    reader other than FINAL is redirected onto grouped rows only when they
    hold its grain.
  - `PushFilteredAggregateInput` moved `x ? k in <set>` into the WHERE of a
    CTE whose only wiring for the set was the row parent the filtered column
    passed through, and rendered a column of a CTE that was never joined
    (`Rewrite.moved_membership_reads_its_set`); `CollapseSingleParent` folded
    a set into a parent that reads a set of the same address by subselect
    (the WHERE's copy of a set the select also writes) and rendered the
    parent's.
  - A HAVING on a column the select does not project is a semijoin on the
    select's keys, planned under the WHERE. The WHERE was also written into
    the set's own filter, where a set it reads was read again off the rows it
    had kept: `where tag in (tag ? count(id) by * - sum(h) by species > 3)
    having species in (...)` dropped every group, and a total in such a copy
    could not be planned at all (`Could not resolve existence clause`, 12 of
    the raises). A WHERE reading a set is no longer copied
    (`SelectGrain.where_over_total_not_idempotent`); a row predicate is safe
    to copy (`SelectGrain.row_where_idempotent`). A WHERE with an aggregate
    atom and no set is still copied, wrongly where the atom is not decided
    per whole group (`where h > 2 and count(id) by species >= 3 having tag =
    'y'` recounts two trees for three): dropping that copy raised on 78
    queries that plan on main, so it is a lead.
  - A window over a derived key beside a WHERE membership on the row key
    (`row_number() over (partition by city order by fc) where tree_id in
    (...)`) joined the key's stream to the rows on nothing and returned every
    `(city, fc)` pair: the rows' scan was not widened to the pairing key,
    because the set it reads was counted among its row parents
    (`SelectGrain.feeder_is_no_row_parent`).

- **A sixth family, `filters`, to 20,000** (0.3.531). It writes what the
  fifth names as left: a `?` as a select target outside an aggregate, alone,
  beside the select's keys, as a group key of aggregates and windows, in a
  HAVING and an `order by`, and a filtered aggregate beside the keys that
  decide its condition. Its oracle is the rule `gen_filter` documents: `x ?
  c` is `x` where `c` holds and NULL elsewhere, and filtered values alone are
  the rows the condition keeps. Every condition written is a function of the
  filtered value's keys (`Filters.filtered_value_on_its_keys`); what a
  condition off them shows is undecided and written up as a lead. On main
  seeds 0-20,000 (40,000 queries) hold 1,807 wrong answers, 1,880 raises and
  157 invalid statements; 282 are left (77 wrong, 192 raising, 13 invalid),
  and no query that was right on main fails. Seven queries (four seeds) that
  raised on main return wrong rows; every other one left fails on main the
  same way. The five older families, 20,000 queries
  each, have no status change; `windows` pays 100 joins over 81 queries for
  the filtered bridge and `spellings` 119 CTEs. Among the 36,156 queries right on both sides the family
  costs 297 joins over 231 queries and saves 475 CTEs.
  - A filtered key (`select city, tree_id ? height > 2 as f`) is `tree_id`
    where it is anything, so the group check read the rows as unique on
    `tree_id` and skipped the dedup: a city came back once per tree the
    filter rejected (`Filters.rejected_rows_share_one_null`). A filtered
    value stands for its source key only where a parent applied its
    condition.
  - A window over a filtered key grouped its input by the column the key was
    read off as well, so `row_number` numbered the NULL group once per
    height (`Filters.source_column_splits_null_group`). A value a parent
    hands over is read as itself.
  - Filtered values alone whose condition reads a set (`select city ? not
    (city in (city ? city = 'B')) as f`) stayed a CASE and showed the rejected
    rows as NULL (`Filters.case_alone_shows_rejected`): a membership was never
    moved into the node's WHERE, though its set is wired to that node either
    way. It moves for the statement's own values; in a sub-plan (a set's
    feeder, a HAVING's semijoin) it broke a windows seed and stays a CASE.
  - `select tree_id, city ? city in (city ? height > 2) as f where avg(height)
    by tree_form >= 1` recursed: the set claims the select's grain, which
    names `f`, and planned standalone beneath the WHERE it asked for `f`,
    which asks for the set (`Filters.value_is_no_finer_than_its_reader`).
    The 0.3.529 handoff's lead, and the family's largest group of raises on
    main.
  - `select species, tree_id ? min(height) by tree_form < 10 as f,
    min(height)` raised a graph cycle: the select's minimum, grouped by `f`,
    was elected as the partial `min(height) by tree_form` re-aggregates, and
    `f` reads that (`Filters.partial_grouped_by_its_reader`). A partial
    grouped by a reader of its roll-up is not elected; the value is read off
    the rows (`Filters.min_of_group_mins` says nothing is lost but a scan's
    reuse).
  - A WHERE aggregate consumed by an aggregate that only a set's condition
    reads (`species ? species in (species ? count(tree_id) by species >= 3)
    as f where sum(height) by city >= 1`) counted as hosted, so the
    statement's rows were given no gate and the placement raised `cannot be
    related to the query outputs` (`Filters.gate_on_a_set_filters_no_row`).
    Such a barrier no longer stands in for the rows, when it is no output of
    the statement and the WHERE reads no set of its own (the two cases a
    wider rule broke).
  - A total gated by a total (`(count(tree_id) by *) ? min(height) by * <=
    5`) was collapsed as a value some row of a group passes and rendered
    `max(CASE WHEN min(...) ...)`, which no engine accepts
    (`Filters.total_gated_by_total_is_scalar`).
  - `having f is not null` on a filtered column collapsed to its grain was
    rendered in the WHERE, over the `max(CASE ...)` it is computed by; it is
    a HAVING.
  - A window's input was grouped by the source of every output handed to it
    (`height` of `height > 2 as big`), so equal groups stayed apart, and it
    dropped a value its grain determines (`max(height) by tree_form` beside a
    window over species), which a later node then had no source for. It keeps
    what the grain determines and reads a handed value as itself.
  - A value `by city, tree_form` beside groups keyed by a filtered column was
    joined on `city` alone (`SelectGrain.by_property_without_its_key_misjoins`):
    the filtered rows could bridge them but, as an extra side, were never
    widened to `tree_form`. An extra filtered stream is widened to the rest of
    a supplier's grain its rows determine.
  - A WHERE aggregate was barred from a filtered column's rows by an
    aggregate inside the set that column reads by subselect: a set is no
    row-shape barrier of its reader.
  - Predicate pushdown read a column off a set feeder that is not in the
    CTE's FROM (`Rewrite.moved_membership_reads_its_set`), or not, by
    Python's hash seed: one seed planned right or rendered `Referenced table
    not found` from run to run, on main too.
  - The sweep runner ends a plan at 30 seconds: one seed loops in planning
    and stalled the pool.

- **`filters` to 40,000** (0.3.532). On main seeds 20,000-40,000 (40,000
  queries) hold 96 wrong answers, 243 raises and 11 invalid statements; the
  round fixes 25 of the wrong answers there and 13 queries left open in
  0-20,000, with no query right on main failing in either range. The five older
  families, seeds 0-10,000 (20,000 queries each), have no status change and
  no change in joins or scans. In 20,000-40,000 the round costs 45 joins over
  56 queries.
  - A filter carrying a row column its grain does not determine (`select
    city, height > 2 as big, tree_form ? tree_form in (<set>) as f`: the
    filter by `species` carried `city` per tree) was paired with the per-tree
    `big` on `species` alone, and B showed a short Acer it does not have
    (`Filters.carried_column_fans_out`). The cover takes such a column off
    the filter when a contributor the FINAL joins anyway holds it on rows of
    its own (not a group the filter reads, whose rows are the filter's), and
    the filter is then deduplicated to its grain before it pairs.
  - A filtered key collapsed to its own keys (`select city, tree_form, city ?
    sum(height) by * is null as f`) dropped the select's other key, so the
    value by `tree_form` beside it paired with every city
    (`Filters.collapse_drops_paired_key`). The collapse keeps the group's
    keys its parents hold.
  - A property shown without its key, renamed (`select tree_form as f where
    tree_form in (<set>) or tag = 'x'`), was grouped by that key hidden and
    showed broadleaf once per broadleaf species
    (`SelectGrain.hidden_key_repeats_property`). The FINAL dedup now counts a
    hidden key of a shown property, except a filtered value's own keys
    (`select name, bulk_name` is a row per product). No `?` is needed; it
    was reduced from a keyless-join raise in the family.
  - Tried and backed out: planning a filter read off its condition's
    aggregate alone as holding only what it renders, so the WHERE-kept rows
    bridge it to the select's other keys (`select city, tree_form, city ?
    sum(height) by city - count(tree_id) by city = 2 as f where max(height)
    by tree_form is not null` crosses cities and forms). It fixed that seed and
    broke 33 queries right on main, by keyless joins where the plan had read
    columns the filter passes through.
- **`filters` to 60,000** (0.3.534). On main seeds 40,000-60,000 (40,000
  queries) hold 67 wrong answers, 233 raises and 12 invalid statements. Over
  0-60,000 the round fixes 120 queries (42 wrong answers, 64 raises, 14
  invalid statements) and breaks none, with 296 fewer scans and 204 fewer
  joins (4,407 more CTEs; 65 summary reads lost, none of them adding a scan).
  The five older families, seeds 0-10,000, have no status change; `first`,
  `spellings`, `compositions` and `expressions` plan the same SQL cost, and
  `windows` has 12 fewer scans and 16 fewer joins.
  - A filtered key's keys were peeled off the row stream it reads: in
    `select tree_form, city ? city = 'A' as f where species in (<set>)` the
    WHERE's set made `species` a grouping key, `tree_form` moved to a scan of
    the species table, and the filter's rows held `f` alone. Paired with the
    forms on nothing, every form met every `f`
    (`Filters.filter_keys_stay_on_its_rows`). The peel now leaves a filter's
    keys, the columns its condition reads and the row columns a derived one
    is computed from (`fc` of `tree_form`) on the row stream. A filter an
    aggregate reads (`max(height ? c)`) keeps none there, a filtered
    aggregate's own group keys still peel, and so do a filtered column's keys
    beside a WHERE aggregate they do not determine (FINAL re-applies that
    aggregate on the peel's key). The condition's columns also cleared a
    keyless-join raise: `select tree_id, max(height) ? tree_form = 'linden'
    as f, min(height) by species` met the per-tree maximum and the species
    scan on nothing.
  - A WHERE hosted on the aggregate a filter's condition reads filtered only
    that aggregate's rows, and not the rows the filter shows: `select height
    ? count(tree_id) by
    city < 10 as f where min(height) by species < 4` showed the Quercus's 5
    (`Filters.gate_on_a_condition_filters_no_row`). A shown filter group a
    host feeds directly, whose own rows bypass every host, now counts as an
    uncovered contributor, and FINAL re-applies the atom where the filter's
    grain determines the keys of the atom's aggregates. A set planned under
    the WHERE (`_CleanFeederCache`, a HAVING membership's subselect) is left
    out: there the same rule cost 2,000 joins over 40,000 queries and broke
    six (below).
  - A filter its keys decide was grouped by those keys hidden: `select city,
    fc, tree_form ? species in (<set>) as f` showed `(A, b, NULL)` once per
    broadleaf species (`Filters.decided_filter_repeats_per_key`). The FINAL
    dedup's exception for a filtered value's keys now holds only where its
    condition reads finer than them (`select name, bulk_name` is a row per
    product because `quantity` is an order's).
  - Not fixed, and not counted above: a set planned under the WHERE reads
    rows the WHERE never filtered when its condition is an aggregate's.
    `select city ? city in (city ? max(height) by * > 0 and height = 5) as f
    where min(height) by species < 4` returns A on main and on the branch;
    the answer is empty.
- **`filters` to 80,000** (0.3.535). On main seeds 60,000-80,000 (40,000
  queries) hold 61 wrong answers, 199 raises and 15 invalid statements. Over
  0-80,000 the round fixes 138 queries (28 wrong answers, 110 raises; 25 in
  0-20,000, 83 in 20,000-60,000, 30 in 60,000-80,000) and breaks none right
  on main, with 576 fewer scans and 2,068 more joins (0.3%; 1,428 of them on
  1,316 queries main already answered). The five older families, seeds
  0-10,000, have no status change and no change in joins; the twin-scan
  rebase below takes 374 scans off them (`first` 180, `spellings` 112,
  `expressions` 39, `windows` 37, `compositions` 6).
  - A shown filter an aggregate answers was read off that aggregate alone:
    in `select city, tree_form, city ? sum(height) by city > 2 as f where
    max(height) by tree_form is not null` the filter's group claims the
    select's grain, `(city, tree_form)`, but its build dropped the row
    parent as redundant beside the city sum, so its rows held cities and no
    form. FINAL paired it with the WHERE's forms on nothing and city A met
    the linden only B has (`Filters.filter_over_aggregate_keeps_its_rows`).
    A filter only FINAL reads now asks its parents for the shown keys of
    its grain and the shown keys those determine, so the row stream stays
    beside the aggregate. A filter inside a set, one another group reads,
    and a filtered aggregate keyed by shown keys (which pairs on them) are
    left as they were: each broke seeds right on main when included. A
    sibling filter in the claimed grain is no key of these rows.
  - Efficiency: a grouped scan under a WHERE and a HAVING (`max(height) by
    city, species is not null` inside a set) never rebased onto its row
    twin under the same WHERE, because `MergeTwinScans` compared the whole
    condition. It now names the rows by the WHERE alone and keeps the HAVING
    on the grouped CTE; a HAVING that reads a set, or a QUALIFY, still
    scans apart.
  - Not fixed: 9919 and 28312 raised on main and return wrong rows. In
    `select species, (min(height) by tree_form) ? tree_form in (tree_form ?
    min(height) by city, species >= 0) as f` the cover elects the set's
    inner `by city, species` aggregate to supply `species`, and FINAL does
    not deduplicate it.

- **`filters` to 100,000** (0.3.536). On main seeds 80,000-100,000 (40,000
  queries) hold 62 wrong answers, 196 raises and 12 invalid statements. Over
  0-100,000 the round fixes 590 queries (58 wrong answers, 503 raises, 29
  invalid) and breaks none right on main, with 867 fewer joins and 4 more
  scans on queries main already answered. Three seeds (25203, 49355, 71788)
  that raised or were invalid on main now return wrong rows: each reaches a
  WHERE on an aggregate off the select's grain that main answers wrong alone.
  - A set holds only the rows its own condition kept, so the final cover
    elects no set to hold select keys (`set_holds_no_select_keys`), and no
    aggregate only sets read regrafts a derived key.
  - A shown filter only FINAL reads meets the WHERE gate on the gate's full
    grain, and passes its row parents through whole
    (`filter_meets_gate_on_its_grain`, `filtered_key_identifies_no_row`).
  - `DropIdentityGroup` takes no unique key off a parent that supplies a
    membership set; the gate backfill under a WHERE membership skips `by *`
    totals (`gate_beside_set_filters_rows`).
  - Two merged copies of one datasource that each joined the same CTE
    rendered it under one alias twice; the duplicate joins now coalesce.
  - Not fixed: excluding a set's inner aggregate from the cover
    (`set_input_supplies_no_output`) fixed 17550 but broke four seeds right
    on main, so it was dropped; 17550 is open again.

- **`filters` to 120,000** (0.3.537). On main seeds 100,000-120,000 (40,000
  queries) hold 26 wrong answers, 67 raises and 6 invalid statements. Over
  0-120,000 the round fixes 199 queries (75 wrong answers, 122 raises, 2
  invalid), with 817 fewer joins and 14 more scans on queries main already
  answered, and 42 fewer summary-table reads. 15 queries right on main (8
  seeds) now raise; none returns wrong rows.
  - A WHERE reaches a filter's rows through its row parents: a filter FINAL
    alone reads is gated whatever its grain (`gate_reaches_a_filter_alone`),
    and a gate goes on the statement's rows, not on a group only sets read
    (`gate_hosted_on_set_input_alone`). A filter whose content is a gated
    aggregate is not gated again (`hosted_content_passed_gate`), and a
    filtered value the other keys decide is no key of the FINAL rows.
  - A total in a filter's condition no longer makes it collapse to its keys
    (`total_splits_no_key`); a filter's bridges carry the grains of the
    aggregates its condition reads (`condition_aggregates_meet_per_row`).
  - A partial is not elected when its chain of re-aggregations is grouped by
    a reader of its head (`partial_chain_reads_itself`; it recursed or built
    a cycle on 48 seeds).
  - Not fixed: hosting the gate on the statement's rows raises where those
    hosts cannot carry the gate's grain (8532, 17300, 18372, 39289, 50368),
    and the total and content-hosted rules turn 59678, 72714 and 91381 into
    keyless joins beside a window or a derived key. All eight are `OPEN`.
  - Not fixed: 112517 (base) reads `fc` off the `(city, tree_form)` minimum a
    total rolls up and meets the tags on `city` alone
    (`regraft_keeps_paired_key`). Counting FINAL as a pairing reader of the
    regrafted group fixed it and broke TPC-DS q25 and q66, so it is `OPEN`.
- **`filters` to 140,000** (0.3.538). On main seeds 120,000-140,000 (40,000
  queries) hold 29 wrong answers, 76 raises and 7 invalid statements. Over
  0-140,000 the round fixes 150 queries (net 77 fewer wrong answers, 56 fewer
  raises, 10 fewer invalid), with 12,930 fewer joins, 108 fewer scans and
  2,060 more summary-table reads. 7 queries right on main now fail; all are
  `OPEN`.
  - A set beside a grouping group feeds it no rows, so it supplies no column
    the group's demand counts as provided (`set_supplies_no_column`).
  - A set planned under the WHERE reads the rows the WHERE kept: the
    WHERE's aggregates gate the set's row inputs, reached through its
    condition (`set_plan_gates_its_rows`), and the select's membership tests
    read that kept set (`select_reads_kept_set`).
  - An aggregate only a set reads supplies no select key, unless the select
    reads its own members (`set_input_supplies_no_key`).
  - Not fixed: gating a set on the summary model renders the gate off the
    summary scan, which raises (62004, 77287, 138477, 138625) or drops a row
    (47078). 25496 is main's stray WHERE row beside a window, which a limit
    used to hide. 129123 in full still pairs on one column: the filter cannot
    expose its grain keys.

## Limits

The model is a reference semantics, not a refinement proof of the planner.
Populations are lists, and conditions and aggregates are functions over them.
Joins appear only through padding and fan-out. Freshness assumes arrivals
land strictly past the watermark: a late row with a tied or NULL key is
invisible to the verdict and the refresh alike
(`Freshness.tie_is_invisible`, `Freshness.null_key_is_invisible`). That is a
property of `MAX`-based watermarks, not a bug the model can fix. `x / 0`
and `x % 0` are NULL on every dialect, so `/` is strict and total only by a
nonzero literal. The cell checker is proved
exact over integers; the Python twin applies the same cells to dense domains
without a successor step. Window frames other than the default
(`rows between ...`), ROLLUP subtotal rows and unions are modelled at the
level the laws need.
