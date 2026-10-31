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
| `Rollup.lean` | a summary re-aggregated to a coarser grain under a WHERE; a query's own GROUP BY re-aggregated to its coarser grains | `sum`, `count`, `min` and `max` (so `bool_and`/`bool_or`) roll up exactly under any filter and target their grain determines, the grand total's no-row guard, a re-aggregated partial is itself a partial (so a chain of grains reads the rows once), a finer filter, a fanned-out partial's re-sum, `count_distinct` and `avg` cannot, but `avg` is the guarded quotient of a stored `sum` and `count`, and a stored column answers only an aggregate of its own argument (checked by `tests/discovery/test_rollup_resolution_laws.py`) |
| `Assert.lean` | datasource `assert` / `prior(...)` build checks: the outer join of the two sides, the edge rules, substitution into the constant check | the check is the documented rule on every group of either side, given every name substituted, exact literals and a driver-independent TRUE; a vanished group fails, a NULL key matches by identity (checked by `tests/execution/test_assertion_laws.py`) |
| `Functions.lean` | scalar functions as rendered per dialect: `/`, casts to an integer, `date_diff`, `week`, `day_of_week` | true division (a materialized quotient reads back), a cast is `round`, `date_diff` counts boundaries so it adds along a chain, `week` is ISO and `day_of_week` is 1 on Sunday on every dialect (checked by `tests/core/test_function_laws.py` on DuckDB and SQLite) |
| `Cost.lean` | which answering datasource is read: a `complete where` arm stored at a grain | a source whose arm implies another's and whose grain is coarser stores no more rows on any data, a filtered table and a complete summary have no order, a partial summary answers exactly under an implying WHERE and loses rows under any other, a pin to values in `S` implies `x in S` (checked by `tests/discovery/test_cheapest_source_laws.py`) |
| `Stack.lean` | a partition family stacked into one source: value-set arms, summaries stacked at their grain | `x in S` is the disjunction of `x = a` over `S`, so the cell checker covers families mixing `=` and `in` arms, a per-value arm choice can count a value twice, stacked summaries are exact and repeat no grain value when the grain determines each arm, and repeat one when it does not (checked by `tests/discovery/test_partition_family_laws.py`) |
| `Freshness.lean` | watermarks, the stale verdict, the incremental refresh | verdict = refresh has a row to write, incremental = rebuild under strict arrivals (checked by `tests/execution/state/test_freshness_laws.py`, not the query fuzzer) |
| `StateMerge.lean` | partition deltas folded into a state snapshot | disjoint overlays commute, replay is a no-op, newest claim wins as a semilattice join, the per-slice count adjustment (checked by `tests/execution/state/test_merge_laws.py`) |
| `NullReject.lean` | the null-rejection walk, membership, outer-join upgrade, nullability inference | a proved column is non-null on every kept row, a proof on a column read off the padded side turns a left join inner, a derived column is judged by its lineage, strict and total functions (checked by `tests/optimization/test_null_rejection_laws.py` and `tests/core/test_null_function_conformance.py`) |
| `Validate.lean` | datasource validation: nullability, declared ranges, grain, complete bindings, `unique` properties, `complete where` containment | each check fails exactly when the claim the planner trusts fails, under Kleene WHERE and GROUP BY's NULL grouping (checked by `tests/core/validation/test_validation_laws.py` and `tests/core/validation/test_type_conformance.py`) |

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
    arm now joins a family only when its discriminator is bound (the FK rule
    claims the grain determines it) or follows from declared keys.

## Limits

The model is a reference semantics, not a refinement proof of the planner.
Populations are lists, and conditions and aggregates are functions over them.
Joins appear only through padding and fan-out. Freshness assumes arrivals
land strictly past the watermark: a late row with a tied or NULL key is
invisible to the verdict and the refresh alike
(`Freshness.tie_is_invisible`, `Freshness.null_key_is_invisible`). That is a
property of `MAX`-based watermarks, not a bug the model can fix. Nullability follows DuckDB: `x / 0` is
inf there, so `/` is total, though MySQL and SQLite return NULL. The cell checker is proved
exact over integers; the Python twin applies the same cells to dense domains
without a successor step. Windows, ROLLUP subtotal rows
and unions are modelled at the level the laws need.
