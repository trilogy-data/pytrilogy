# Handoff: the next formalization round

`formal/` holds a dependency-free Lean 4 model of Trilogy's semantics. Each
round picks one area, models the documented behaviour, proves the laws, and
writes a counterexample theorem for every place the engine broke them. A Python
law suite then holds the implementation to the theorems. `formal/README.md`
lists every module and, under "What it found", every bug found so far. Read it
first.

Recent rounds: `Assert.lean` (#799, datasource `assert`/`prior`),
`Functions.lean` (#802, scalar functions per dialect), `Fixpoint.lean` (#833,
the optimizer's termination contract), `Cost.lean`
(which answering datasource is read: partial summaries, value-pin
implication, dominance ranking), `Stack.lean` (stacking a partition
family: value-set arms, arms the grain does not determine), #821, which
made datasource bindings functional-dependency facts in place of FK-derived
keys, `Binding.lean`, which models those facts (area 0 below records what
is left of it), and `Unnest.lean`, filtering rows by the elements of an
unnested list (area 0b).

## The recipe

1. **Probe before modelling.** Write a scratchpad script that runs the same
   Trilogy text on DuckDB and SQLite (`Dialects.X.default_executor(environment=Environment())`,
   then `execute_text(...)[-1].fetchall()`), or that pairs two query shapes the
   docs say are equal. Disagreements are the leads. #802 began with one
   20-line probe that showed six of them.
2. **Model in Lean** under `formal/Trilogy/<Area>.lean` and add the import to
   `formal/Trilogy.lean`. State:
   - the reference semantics, small and over `Int`/`List`. There is no
     Mathlib, so build fractions as structs (see `Functions.Frac`).
   - the laws a user relies on (`*_exact`, `*_add`, `*_iff`).
   - a named counterexample theorem per bug, closed by `decide`.

   Build with `cd formal && ~/.elan/bin/lake build` (`lake` is not on PATH).
   Keep the build warning-free: `if_pos`/`if_neg` are deprecated, so use
   `simp only [h, ↓reduceIte]`, and `omega` handles `%` by literals.
3. **Python law suite** in `tests/<area>/test_<area>_laws.py`. Its module
   docstring names the Lean file. Generate random data with seeded `random`,
   compute the reference in Python with the same definitions as the Lean
   model, and assert per row.
4. **A/B against main.** Copy `trilogy/` to the scratchpad, overwrite each file
   in `git diff --name-only origin/main -- trilogy` with `git show origin/main:<f>`,
   and run the suite from the scratchpad with `PYTHONPATH=<copy>` (the cwd must
   be outside the repo). Report "N of M fail on main" in the PR. Never use
   `git stash` or `git checkout`: parallel agents share the tree.
5. **Fix, then document.** Add a row to the module table in `formal/README.md`
   and a bullet under "What it found" that cites the theorems. Bump
   `trilogy/__init__.py`. Run `ruff check . --fix`, `black .` and
   `mypy trilogy`. Run only one pytest process at a time. Commit the TPC timing
   logs and charts the suites regenerate.
6. **CI gotchas seen in #802.**
   - `codecov/patch` goes red on code for dialects with no engine in CI
     (Postgres, MySQL, SQL Server, BigQuery, Presto). Pin those renderings in a
     `tests/dialect/` test, as `test_function_law_renderings.py` does.
   - ClickHouse runs for real through chdb on Linux CI only
     (`tests/engine/_clickhouse_cases.py`).
   - `tests/scripts/test_cli_consistency.py::test_ingest_output_passes_integration`
     ingests TPC-DS and validates it against the `std` types. That is how
     tightening `std.date.day_of_week` to `[1..7]` broke on `d_dow` (0-6),
     which led to `day_of_week_zero_index`.
7. **Lessons from #838 (`Unnest.lean`).**
   - Make the data honest for the claim the model states. A plain `merge` is
     EQUAL; random data that lists a region the other side lacks breaks it,
     and the padding that follows looks like a bug and isn't one.
   - Check the reference against a law the engine must keep before reading a
     mismatch as a bug: `P or false` equals `P`, a filter never adds rows, an
     OR keeps every row its disjuncts keep. Two of #838's bugs surfaced only as
     such a disagreement.
   - Before opening the PR, remove each planner change one at a time and rerun the suite
     plus the fuzzer family. Three of #838's changes turned out to be covered by
     later ones and were dropped.
   - Run the broad suite again after the last change. A late change in #838
     broke the literal `unnest([true, false, ...])` case in
     `tests/engine/test_duckdb.py::test_sum_bool`.
   - Adding fuzzer specs changes `CASES_PER_SEED` in
     `local_scripts/fuzzer/test_fuzzer.py`. CI does not run that file (it runs
     `pytest tests/` and `python -m local_scripts.fuzzer`), so run it yourself.
   - Reduce a big fuzzer repro by deleting statements one at a time while the
     wrong row persists: #838's `~group_id` cross join came down to one extra
     table.

## Candidate areas

**Suggested next: area 2, optimizer rule soundness.** `Fixpoint.lean` (#833)
proved the optimizer *terminates* under its WHERE-atom contract; nothing yet
proves a rule *preserves rows*. Start with `strip_redundant_not_null.py` and
`join_upgrade.py`, whose guards `NullReject.lean` already models, then move to
`predicate_pushdown.py`, the riskiest. The leads in areas 0, 0b, 1 and 1b are
smaller and suit a short round.

### 0. Binding FDs (done in `Binding.lean`; leads left)

`Binding.lean` models a binding as an FD from a table's row determinants,
proves the closure sound, and holds `build_fd_closure`, `binding_closure`,
`row_keys` and `bound_on` to it (`tests/core/test_binding_fd_laws.py`).
Validation checks every binding across the tables binding its columns, and
the `merged_axis_pin` fuzzer family covers #821's shape. See "What it found"
in `formal/README.md`. Leads left:
- **A family-level "grain is unique model-wide" declaration** (proposed, not
  built). An arm's row determinants carry its bound discriminator because an
  arm is unique on its grain only among its own rows
  (`Binding.arm_grain_alone_fails`). A family whose grain is unique across all
  arms (`tree_id` arms split by `city`) could declare it, say
  `partition family trees unique grain`, and its arms' determinants would
  then be the grain alone, so `tree_id -> city` would hold model-wide. The
  declaration is one more FD claim the data can break, so it needs a
  validation check: the grain check over the union of the arms, which
  `cross_check_exact` already covers if the family is treated as one table.
- **Not a bug: a derivation over a subset-merged key can pad a key that the
  plain spelling does not, when a COMPLETE binding's claim is false.** With
  `merge merged_group into ~group_id` beside `events (gid: group_id)` and a
  group with no event, `select group_id * 10 as g, event_id, group_name`
  read `group_id` through the `~` merge and padded that group, while
  `select group_id, ...` read `events`' complete binding and did not. The
  model claims every group has an event; completeness validation reports the
  claim, and with honest data (or `events (gid: ~group_id)`) both spellings
  agree (`tests/engine/test_subset_merged_key_derivation.py`). The fuzzer's
  random seeds break that claim for the base `events` table; the
  `merged_axis_pin` family lists regions only for groups with events.
- **Done in `Unnest.lean`**: the pinned CASE over a merged unnest key no
  longer reproduces (0 errors in 360 probes over 15 random seeds), and an
  aggregate filtered on the merged unnest key now plans. See area 0b.
- **Three readers of row determinants**: `BuildDatasource.row_determinants`,
  `Environment.binding_fds` (inline) and `domain_graph._row_determinants`
  compute the same set. The law suite pins the first two; folding them into
  one would remove the risk of drift.
- **`_pin_input_placements`**: the FD test and the declared-keys test agree on
  every placement the suite reaches (4,377 calls, 20 with a host). A shape
  where they differ needs a pin anchored on a key that a binding (not a
  declared key) relates to the atom's input, with the pin's scan reading that
  input; none was found.
- **Declared-key readers that are right as they are.**
  `_condition_source_concepts`, `_arm_scopes_sourcing`,
  `merge_node._existence_only` and `_stamp_determining_key_roots` read a
  concept's DECLARED `keys` on purpose: a property is read beside its key.
  Switching them to binding rows would pull every table binding a key into
  those plans.

### 0b. Unnest filters (done in `Unnest.lean`; leads left)

`tests/unnest/test_unnest_filter_laws.py` holds row, aggregate and paired-element
queries over a species-keyed list to the reference, in four spellings: the
element itself, the key it is `~`-merged into, a plain (EQUAL) merge over
honest data, and the `~` merge beside a second table binding `~species`.
Leads left:
- **The keyless-join guard still exempts an unnest emitting only its
  elements.** The plans that built one (a private scan of the list for a
  condition copy, a root slice of the list alone) are fixed, but the guard
  cannot tell a deduped list column from a literal `unnest([...])`. Marking the
  UnnestNode's source (literal vs column) would let it.
- **A plain merge over data that breaks it pads.** With `eco` listing a
  region no list names, `select tree_id where ecoregion_id = k or ...` read
  `ecoregion_id` from `eco` and padded a NULL tree. With the element now read
  in place the row filter no longer joins `eco`, but a selected key still
  pads a member no tree lists, by design (the suite expects `(None, k)`).
  Completeness validation of a merge over a computed (unnest) origin would
  report the broken claim; it does not check one today.
- **Unnest of a fact-level list** (a property of the row key itself, not of a
  dimension) is not in the suite.

### 1. Answering from intermediate aggregates (`Rollup.lean` follow-ons)

`Rollup.lean` proves that `sum`/`count`/`min`/`max` re-aggregate exactly under
any filter on columns the grain determines, and that `count_distinct` and `avg`
cannot. `avg` from a stored `sum` and `count` is done
(`Rollup.avg_of_parts_exact`; the Factory builds `avg(x)` as the guarded
quotient when a datasource stores both parts, see `trilogy/core/average_parts.py`).
That round also fixed `rollup_signature` (it matched `sum(x * 2)` to a stored
`sum(x)`) and a partial read into an expression without its re-sum. Open
questions:
- **An expression over an aggregate at a summary's exact grain** (`select city,
  species, sum(height) * 2` over a `(city, species)` summary) reads the base
  rows. The bare aggregate is matched by the statement-level datasource search;
  the nested one goes through the aggregate-derivation path, which never looks
  for an exact-grain materialized column. Answers are correct, so this is a
  missed serve. `EXACT_GRAIN` in the harness marks it.
- `max(avg(x) by species)` and `avg(x)` beside `avg(x) by *` decline to the base
  rows, as the `sum` versions of those shapes do (`MIXED_GRAINS`).
- `count_distinct` from a summary keyed by the distinct column itself. This is
  exact when the column is in the summary's grain.
- A filter on a column the summary's grain does *not* determine must decline.
  Hunt for a planner path that accepts it anyway.
- The harness is `tests/discovery/test_rollup_resolution_laws.py`. Extend its
  matrix rather than starting a new one.

### 1b. Cheapest source (`Cost.lean` follow-ons)

`tests/discovery/test_cheapest_source_laws.py` holds the planner to
`Cost.dominated_le`: never read a source an answering source in scope
dominates. Open leads:
- `source_dominance.dominates` compares grains by component subset, not the FD
  closure, so a summary at `(city)` does not count as coarser than a row table
  at `(tree_id)`. Sound, only incomplete.
- The network search's only dominance axis is `unfiltered_sources`
  (an IMPLIED_EXACT twin with the same columns). Two rollup bindings at
  different grains tie there; the matrix has no shape that reaches it yet.
- Done in #821: stacked summary families serve rollups
  (`summary_family` in `tests/discovery/test_partition_family_laws.py`), and
  an arm binding its discriminator outside its grain no longer claims the
  grain determines it model-wide (area 0).
- Not a bug: a summary binding `city: ~city` claims absent cities have no
  rows (padding), so it rolls up as complete and absent cities count 0.

### 2. Optimizer rule soundness

**Already done: termination (`Fixpoint.lean`, #833).** The driver reruns its
phases until a pass changes nothing. `atom_contract.py` registers *removers*
(phases that drop WHERE atoms) with the predicate they drop by; adders gate on
`removable`, and under `enforce_fixpoint_contract` (on in the test suite and
the fuzzer) the driver raises when a phase adds an atom a fact remover would
drop, or hits its pass cap. `Fixpoint.strip_never_cycles` proves a fact
remover never re-fires under the contract. Its own limits, worth extending:
`PredicatePushdownRemove` judges atoms by *other* atoms and is gated but
outside the model, and merges (`CollapseSingleParent`) are exempt as finite.
Termination says nothing about rows: a rule can converge and still drop or
duplicate them.

Each rule in `trilogy/core/optimizations/` rewrites CTEs and must preserve rows.
A natural Lean shape is a rule as a function on a small plan AST, with the
theorem `eval (rule p) = eval p` under the rule's guard, plus a counterexample
showing the guard is necessary. Good first targets, roughly in order of how
self-contained they are:
- `strip_redundant_not_null.py`: dropping `x is not null` is sound only when
  the column is proved non-null. `NullReject.lean` already has the proof
  machinery, and the rule's predicate is now the registered fact remover in
  `atom_contract.py`, so the soundness theorem and `Fixpoint`'s `R` are the
  same predicate. Prove `R a -> eval (strip p) = eval p`.
- `join_upgrade.py` and `value_set_join_upgrade.py`: outer to inner. Compare
  `NullReject.proofs_sound`.
- `merge_irrelevant_group_by.py` and `identity_group.py`: a GROUP BY is a no-op
  when its keys are a key of the input (`Rows.lean` has `distinct = group`).
- `predicate_pushdown.py`: pushing past a NULL-extending join or an aggregate.
  The guards include `_predicate_safe_past_null_extension`. This one is
  historically the riskiest, and #833 rewrote it substantially (gating on
  `removable`), so re-probe it rather than trusting older notes.
- `existence_having_fold.py` and `semi_join_pushdown.py`.

Test each rule by running a query with the rule disabled and enabled and
comparing rows: a twin-run harness like the fuzzer's `law` family
(`local_scripts/fuzzer/laws.py`). Per the standing rules, count corpus firings
of any guard you change, and run `python -m local_scripts.fuzzer` for planner
or optimizer changes.

### 3. Leftover function leads (verified on main after #802)

| Lead | DuckDB | SQLite | Notes |
|---|---|---|---|
| `date_add('2020-01-31'::date, month, 1)` | `2020-02-29` (clamps) | `2020-03-02` (overflows) | a month-end law: `date_diff(d, date_add(d, month, n), month) = n` |
| `7 / 0` | `inf` | `NULL` | BigQuery `COALESCE(SAFE_DIVIDE(..),0)` and Snowflake `DIV0` give `0`. `NullReject` treats `/` as total, which holds on DuckDB only. Decide one semantics. |
| `date_add` on a `date` | returns a timestamp | returns text | check the declared output type |
| Presto `date_part` | base `date_part(x, part)` | | not a Presto function; renders invalid SQL |
| SQL Server `date_trunc`, `date_part` | | | the base renderings are likely invalid T-SQL |
| `decimal / decimal` | DOUBLE on DuckDB | | the declared type is the merged NUMERIC; check materialization as in #802 |

`tests/core/test_function_laws.py` and `formal/Trilogy/Functions.lean` are the
place to extend for these.

### 4. Other untouched areas

- **Windows:** `lag`/`lead`/`rank` over ties and NULL ordering per dialect.
  Ordering of NULLs is not modelled anywhere.
- **`union`/`except`/`intersect` TVFs:** duplicate handling and NULL equality,
  where set operations treat NULLs as equal and `=` does not.
- **`coalesce`/`case` type merging:** whether a merged type stores every arm's
  value, the same materialization law as #802.
