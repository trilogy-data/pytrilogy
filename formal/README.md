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
