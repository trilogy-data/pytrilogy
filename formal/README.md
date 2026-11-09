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
| `Rows.lean` | distinct tuples, grain | keyed rowset keeps rows, filter pushdown through a rowset, `distinct = group`, FD extension, window = `by`, sibling aggregates |
| `Domain.lean` | `~` padding, grain pin | materialization invariance, pin vs stored column, pin is projection-invariant, `sum(coalesce)` |
| `Sourcing.lean` | `complete where` arms, partition unions, the cell checker | a partition family unions back to the table (split, point, bool, `is null` arm, NULL-rejecting gate, a redundant copy of an arm), an arm answers under an implying WHERE, summary table = computed aggregate |
| `Rollup.lean` | a summary re-aggregated to a coarser grain under a WHERE; a query's own GROUP BY re-aggregated to its coarser grains | `sum`, `count`, `min` and `max` (so `bool_and`/`bool_or`) roll up exactly under any filter and target their grain determines, the grand total's no-row guard, a re-aggregated partial is itself a partial (so a chain of grains reads the rows once), a finer filter, a fanned-out partial's re-sum, `count_distinct` and `avg` cannot, but `avg` is the guarded quotient of a stored `sum` and `count`, and a stored column answers only an aggregate of its own argument, a filter on a stored group value is a filter on the summary key (`group_value_filter_exact`), and a grain column's distinct values are the summary's (`count_distinct_grain_column_exact`) (checked by `tests/discovery/test_rollup_resolution_laws.py`) |
| `Assert.lean` | datasource `assert` / `prior(...)` build checks: the outer join of the two sides, the edge rules, substitution into the constant check | the check is the documented rule on every group of either side, given every name substituted, exact literals and a driver-independent TRUE; a vanished group fails, a NULL key matches by identity (checked by `tests/execution/test_assertion_laws.py`) |
| `Functions.lean` | scalar functions as rendered per dialect: `/`, casts to an integer, `date_diff`, `week`, `day_of_week` | true division (a materialized quotient reads back), a cast is `round`, `date_diff` counts boundaries so it adds along a chain, `week` is ISO and `day_of_week` is 1 on Sunday on every dialect; ClickHouse's `sign(x) * floor(abs(x) + 0.5)` is `round` (`sign_round_is_round`), and its Monday-boundary `week` is not whole spans (`monday_boundaries_is_not_week_diff`) (checked by `tests/core/test_function_laws.py` on DuckDB, SQLite and ClickHouse) |
| `Cost.lean` | which answering datasource is read: a `complete where` arm stored at a grain | a source whose arm implies another's and whose grain is coarser stores no more rows on any data, a filtered table and a complete summary have no order, a partial summary answers exactly under an implying WHERE and loses rows under any other, a pin to values in `S` implies `x in S` (checked by `tests/discovery/test_cheapest_source_laws.py`) |
| `Stack.lean` | a partition family stacked into one source: value-set arms, summaries stacked at their grain | `x in S` is the disjunction of `x = a` over `S`, so the cell checker covers families mixing `=` and `in` arms, a per-value arm choice can count a value twice, stacked summaries are exact and repeat no grain value when the grain determines each arm, and repeat one when it does not (checked by `tests/discovery/test_partition_family_laws.py`) |
| `Freshness.lean` | watermarks, the stale verdict, the incremental refresh | verdict = refresh has a row to write, incremental = rebuild under strict arrivals (checked by `tests/execution/state/test_freshness_laws.py`, not the query fuzzer) |
| `StateMerge.lean` | partition deltas folded into a state snapshot | disjoint overlays commute, replay is a no-op, newest claim wins as a semilattice join, the per-slice count adjustment (checked by `tests/execution/state/test_merge_laws.py`) |
| `NullReject.lean` | the null-rejection walk, membership, outer-join upgrade, nullability inference | a proved column is non-null on every kept row, a proof on a column read off the padded side turns a left join inner, a derived column is judged by its lineage, strict and total functions; a zero divisor is NULL, so `/` is strict and total only by a nonzero literal (`guarded_div_strict`, `literal_divisor_total`), and a persisted copy that declares nullable every column its write may hold NULL in reads back exactly under `is not null` (`inferred_copy_sound`) (checked by `tests/optimization/test_null_rejection_laws.py`, `tests/core/test_null_function_conformance.py` and `tests/core/test_null_division_laws.py` on DuckDB, SQLite and ClickHouse) |
| `Validate.lean` | datasource validation: nullability, declared ranges, grain, complete bindings, `unique` properties, `complete where` containment | each check fails exactly when the claim the planner trusts fails, under Kleene WHERE and GROUP BY's NULL grouping (checked by `tests/core/validation/test_validation_laws.py` and `tests/core/validation/test_type_conformance.py`) |
| `Fixpoint.lean` | the optimizer driver's repeat-until-quiet passes; the WHERE-atom contract between fact removers (`StripRedundantNotNull`) and every other phase | under the contract a fact remover never changes the plan after its first run, so it cannot take part in a cycle; pushdown before its gate broke the contract and cycled (checked on every phase of every planned query in the test suite: `atom_contract.contract_violations`, and by `tests/optimization/test_fixpoint_contract_laws.py`) |
| `Rewrite.lean` | optimizer rules as rewrites of joined rows: the NOT NULL strip, outer-join upgrades, predicate pushdown and its removal, an identity GROUP BY, a matched outer join, an existence fold, a semi-join into an aggregate; a value a partial scan computes; a WHERE over an aggregate decided through a join; the per-query `~` heal; a FULL join beside a `~` subset; a row-level filter argument beside a gate; a filtered value at its keys' grain; a HAVING alias | each rule keeps the rows under the guard it checks (`strip_sound`, `full_to_left`, `left_to_inner`, `or_forces_side`, `push_inner`, `push_preserved`, `push_padded`, `remove_inner`, `push_group_key`, `push_window_partition`, `identity_group_exact`, `merged_group_sum_exact`, `matched_left_is_inner`, `existence_fold_exact`, `semi_join_exact`), with a counterexample per guard; a strict value pads as computed (`pad_strict`), a null-sensitive one does not; an atom false on an empty group is decided by the groups it kept (`kept_exact`), `count = 0` is not; a FULL to a `~` side whose rows all match is the LEFT join (`subset_full_is_left`), and a proof on the key forces no `~` lookup (`key_proof_keeps_lookup_gap`); a row-level argument joins on the key its row determines (`keyed_arg_exact`) (checked by `tests/optimization/test_rule_soundness_laws.py` against a pre-joined twin and with each rule disabled, and `tests/dialect/test_having_alias_shadowing.py`) |
| `Unnest.lean` | filtering rows by the elements of an unnested list: the padded expansion, the kept rows, aggregates over them, how the elements pair with the rows they expand | a WHERE on an element keeps a row iff a padded element passes, a disjunct over the row alone keeps it whatever its list holds, an aggregate reads each kept row once, a filter keeps a sub-multiset of the expansion, pairing on the grouping key or on nothing overcounts, the key an element is merged into pads a member no list names (checked by `tests/unnest/test_unnest_filter_laws.py`) |
| `Binding.lean` | datasource bindings as functional dependencies: row determinants, the closure over every table's bindings, `row_keys`, the cross-table binding check | a table unique on its grain satisfies its own bindings, the closure is sound when every binding holds, one table's binding can break on another's rows, stacked arms are unique on grain plus discriminator and not on grain alone, a grainless table determines nothing (checked by `tests/core/test_binding_fd_laws.py` and `tests/core/validation/test_validation_laws.py`) |
| `Cover.lean` | which queries a count summary covers: targets and conditions that are functions of its key, filtered and distinct counts, totals beside a target, existence, relations keyed on the grain | a target is any function of the key under any spelling (a rename, a scalar of a stored column, a column listed twice), `count(x ? c)` is the re-sum of the groups `c` keeps, and `sum`/`min`/`max(x ? c)` the re-aggregate of `agg(x) by G, cols(c)` over them (`filtered_sum_exact`, `filtered_min_exact`, `filtered_max_exact`), which needs the count's `coalesce` (`uncoalesced_case_count_null`) and the condition's columns in the partial's key (`partial_off_condition_misreads`), or joined onto the summary's rows by a functional lookup before any re-sum that drops their key (`joined_case_exact`, `resum_then_case_exact`; `joined_fanout_double_counts`, `case_above_coarser_grouping_misreads`), in either spelling `agg(x ? c)` or `agg(case when c then x end)`, `count_distinct(k)` is `count(k)` where every row holds its own `k`, a total and a per-target count are independent re-sums, a complete summary proves which key values exist, a count by a value the key relates to re-sums the related groups, which is the summary over the DISTINCT `(key, value)` pairs (`distinct_pairs_resum_exact`, `relation_resum_count_exact`), and re-summing relation pairs double counts, including the pairs a list repeating a value expands to (`collapsed_fanout_double_counts`, `repeated_element_counts_once`), which a list declared `::unique` cannot do (`unique_list_pairs_are_distinct`) (checked by `tests/discovery/test_summary_cover_laws.py`, `tests/discovery/test_filtered_rollup_laws.py`, `tests/discovery/test_relation_rollup_laws.py` and the grouped cases of `tests/unnest/test_unnest_filter_laws.py`) |
| `SelectGrain.lean` | an aggregate with no `by` against one `by` fixed keys, wherever it appears | an unspecified aggregate is the aggregate `by` the select's raw grain (`bare_is_select_grain`), so it and any scalar over it are functions of that grain (`bare_on_grain`, `bare_expression_on_grain`), and in a select with no keys it is the total (`bare_empty_grain_is_total`); one `by` fixed keys is the same in every select (`specified_ignores_select`); a grain holding a window's partition reads the partition's rows alone (`bare_within_partition`); pinning a window's order aggregate to its first argument or to the tuple it ranks gives another value (`first_argument_pin_misreads`, `window_tuple_pin_misreads`); a select of an aggregate `by` fixed keys groups by its value, a function of those keys (`fixed_on_keys`, `keys_refine_value`), so a bare aggregate beside it is the re-sum of the key groups with that value (`bare_beside_fixed_value`), and grouping by the keys in its place splits and repeats a tuple (`by_keys_split_tuple`, `fixed_value_rows_distinct`); an aggregate of that value, or of any scalar of it, reads it once per distinct key in each tuple (`ofValue`, `scalar_of_fixed_on_keys`, `of_value_repeat_invariant`), never per row (`fixed_value_per_row_double_counts`), and a rowset member holding it is an outer key (`rowset_member_dropped_totals`) (checked by `tests/windows/test_window_order_aggregate_laws.py`, `tests/windows/test_window_order_expression_laws.py`, the window cases of `tests/discovery/test_rollup_resolution_laws.py` `tests/core/test_fixed_aggregate_select_laws.py` and `tests/core/test_fixed_value_lifecycle_laws.py`) |
| `Windows.lean` | what a window ranks and where ORDER BY sorts NULLs: a rank over the distinct tuples of its grain, the read-back onto rows, an empty `over ()`, per-engine NULL placement and its rendering | a rank over a grain's distinct values depends only on which values occur, a stream rank is right when it holds each value once and counts repeats otherwise, a read-back from a table unique on its key keeps one row per row, `over ()` is the total, the rendering places NULLs as asked on every engine, grouping by a window's value is the re-sum of its tuple's groups, never one group per tuple, and an aggregate a window orders by with no `by` is at the select grain, which, when it holds the partition, reads the partition's rows alone (checked by `tests/windows/test_window_laws.py`, `tests/windows/test_window_group_laws.py`, `tests/windows/test_window_order_aggregate_laws.py` and `tests/windows/test_window_order_expression_laws.py` on DuckDB and SQLite, and `tests/dialect/test_null_order_renderings.py`) |
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
  rewrites `sum(x) by city` beside `sum(x) by city, species` as the `SUM` of
  the finer value, and a chain of grains reads the rows once. The new
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
  `tree_id` (`Cover.count_distinct_of_key_exact`). A re-summed partial read
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
  `distinct_key_counts`. A summary storing `agg(x ? c)` itself is read as
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
  first, so DuckDB's DECIMAL `1.50` writes `'1.5'`.

  ClickHouse divided a decimal at the dividend's scale, so `n / 3` of a
  `numeric(12,4)` was 0.3333 where the declared type of a decimal quotient is
  a double (`Writings.clickhouse_decimal_quotient`); it divides as a Float64
  now.

  On main, 19 of the 55 cases `tests/core/test_text_write_laws.py` runs fail;
  none now.

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
