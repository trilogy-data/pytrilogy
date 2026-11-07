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
is left of it), `Unnest.lean`, filtering rows by the elements of an
unnested list (area 0b), `Rewrite.lean`, optimizer rules as rewrites
that keep rows (area 2, two rounds: #843 and the row-argument round), and
the `Rollup.lean` follow-on round (0.3.443: group-value filters, partials
under a membership, per-atom peels; area 1), and `Windows.lean` /
`SetOps.lean` (0.3.444 and 0.3.445: what a window ranks, `over ()`, NULL
placement per engine, set operators; area 4), and `Scalars.lean`
(0.3.447: month-end offsets, the type of a date offset, `greatest`/`least`,
`log`; area 3), `Types.lean` (0.3.448: a declared type holds every
value a persisted copy stores; area 3b), and `Cover.lean` §2 extended
(0.3.450: `agg(x ? c)` read from a stored `agg(x)`; 0.3.452: a condition
joined onto the summary, priced in the source search; area 1c), and
`Cover.lean` §6 (0.3.457: a group by an unnested element, and counts by a
value the summary key relates to; area 1c), and `Windows.lean` extended
(0.3.458: grouping by a window's value; 0.3.459: an aggregate a window
orders by; area 4), and `SelectGrain.lean` (0.3.462: an aggregate with no
`by` is at the select's raw grain, a window's order included; this corrects
0.3.459's pin; area 4), and `SelectGrain.lean` extended (0.3.463: a select of
an aggregate `by` fixed keys groups by its value and returns distinct
tuples; area 4), and `SelectGrain.lean` extended again (0.3.464: an
aggregate of a fixed value, a rowset member of one, and a persisted copy of
one read the value once per key; area 4).

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
4. **A/B against main.** Extract main's package to the scratchpad and run the
   branch's suite against it from there (the native parser lives in
   site-packages, so the copy needs nothing else):

   ```bash
   git archive origin/main trilogy | tar -x -C <scratch>
   cd <scratch> && PYTHONPATH=<scratch> <repo>/.venv/bin/python -m pytest \
     --import-mode=append <repo>/tests/<area>/test_<area>_laws.py -q
   ```

   `--import-mode=append` is required: `tests/` is a package, so pytest's
   default mode prepends the repo root to `sys.path` and the branch's
   `trilogy/` silently wins over `PYTHONPATH` (lesson 16). Check once with a
   test that prints `trilogy.__file__`. Report "N of M fail on main" in the PR.
   Never use `git stash` or `git checkout`: parallel agents share the tree.
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

8. **Lessons from `Rewrite.lean` (area 2).**
   - Disable each optimizer rule and compare rows, and also compare against
     an oracle. Rule-off and rule-on agree on any bug they share, and most
     of this round's bugs were in the plan the rules received: a rule
     (inlining, collapse, pushdown) happened to repair it. Ablating every
     toggle over the fuzzer changed nothing; a padded model checked against
     the same model over one pre-joined table found eleven bugs.
   - Query-addressed tables are never inlined. The fuzzer's datasources are
     all query-addressed, so it never exercises `datasource_inlining`, and
     plain-table tests miss what inlining hides. Run both addressings
     (`SPLIT` / `SPLIT_QUERY` in the rule suite).
   - The TPC-DS corpus at sf=0.1 ablates in ~20 minutes; it found q97's
     fan-out. A size-budget failure after a fix is worth reading, not
     rebaselining: two of this round's overruns were a heal the fix had
     stopped making by mistake.

9. **Lessons from the row-argument round (`Rewrite.lean`, 0.3.442).**
   - Sweep a FAMILY, not only random seeds. 1,200 random seeds found four
     bugs; enumerating one shape family (two customer gates beside a row
     atom, 270 queries) found 125 failures on main and, mid-round, 13 new
     silent wrong answers that the random sweep never reached.
   - Turning an error into rows is not progress until the rows are checked:
     a key exposed for a join made 13 shapes that raised on main return wrong
     rows. Diff the wrong-answer SET against main, not the failure count.
   - Fix missing keys at demand (`group_graph._group_inputs`), not by late
     widening: a key added after a dedup or inside a grouped node is lost.
   - A rule's guard that reads a node's partiality must read the node actually
     built: a bucket-level "this peel is `~`" test over-approximated (the peel
     also read orders) and turned correct plans into planning errors.
   - Count firings with the rule's real return value: `optimize` returns a
     tuple, and counting its truthiness made every rule "fire" everywhere.
   - Revert each change against BOTH oracles (the law suite and the family):
     two changes the law suite needed were redundant, and one the law suite
     did not need was what the family needed.

10. **Lessons from the rollup follow-on round (0.3.443).**
    - Hunting for a plan that WRONGLY reads a summary found none, but the
      hunt's own shapes found four other bugs. Put the hunt's queries in
      unusual positions (a membership, a rowset, an aggregate filter at
      another grain, an OR with a joined dimension), not just the expected
      ones: every bug this round came from a position, not a filter.
    - A family whose ORACLE raises is a family worth reading. The pre-joined
      FLAT model raised on 296 of 360 mixed-atom shapes on main; once it
      planned, 8 padded shapes that also raised on main came back WRONG
      (a gate true on an empty group). The fix that unblocks a shape owes
      the rows it returns: check them by hand.
    - A guard that reads the group graph's claim (`carries`) instead of the
      node built can be wrong; check the built node and fall back
      (`_push_row_condition_before_group`).

11. **Lessons from `Windows.lean` / `SetOps.lean` (0.3.444, 0.3.445).** Two runs
    took this area in parallel (#854 and #856) and found the same two bugs
    with the same designs; what follows is what the second run added.
    - Probe the doc's own invariants, not just dialect pairs: "a concept has
      one value whatever else is projected" found the rank fan-out
      (`select id, v, rank v` vs `select v, rank v`), which DuckDB and SQLite
      agreed on.
    - Before calling a padded or dropped row a bug, check the data honours
      every binding claim. `facts (sid: sid)` claims every `sid` has a fact;
      random dimensions without facts made a correct regraft look wrong, and
      a guard written against it was dropped once `~sid` made the data
      honest (0 of 384 failures with or without it).
    - A dialect's "default" is a semantic choice. Pin the engines' documented
      defaults (`NULL_SORT`, `tests/dialect/test_null_order_renderings.py`)
      and simulate each rendering against them, so dialects with no engine
      in CI (Postgres, Snowflake, SQL Server, BigQuery) are still held to
      the law; an engine that is in CI gets a raw-SQL check of the default
      (`tests/engine/test_clickhouse_chdb.py`).
    - Every path that renders an ordering must go through the dialect. The
      file-target `order by` (`trilogy/execution/write_order.py`) wrote the
      ordering's text and sent `desc nulls auto` to DuckDB.

12. **Lessons from `Scalars.lean` (0.3.447).**
    - Probe a long list of literal expressions on two engines at once
      (`select f(...) as x` per function). One 45-line probe found all four
      bugs this round; most were one-liners in a dialect map.
    - Check the declared type against what each engine returns, not only the
      value. `date_add(d, day, 1)` printed the right day everywhere, but
      SQLite returned `'2020-02-01 00:00:00'` for a DATE, which no stored
      date equals as text: a WHERE matched no rows.
    - A dialect default can be a whole semantics: `greatest` skips NULLs on
      DuckDB, Postgres, SQL Server and current ClickHouse, and propagates
      them on BigQuery, Snowflake, MySQL, Presto and SQLite. Choose the
      reference once, and render the fallback where the engine differs; SQLite's
      `max` is strict too, so it runs the fallback in CI for every strict dialect.
    - The Lean toolchain could not be installed in this session either, so
      every proof is `decide` over a bounded domain, or `show` to the unfolded
      term plus `omega`.

13. **Lessons from `Types.lean` (0.3.448).**
    - Persist every expression in a sweep and read the table back raw. The
      planner preferred the base table in every probe, so a query never read
      the copy and no answer looked wrong; the column types of the copy were
      already wrong.
    - Compare a concept's declared type with the engine's `typeof` over a long
      list of expressions at once: one sweep found `sqrt`, `%`, `**`,
      `greatest`, and the decimal merges, all in `trilogy/core/functions.py`.
    - Check a dialect's type names, not only its functions: `float` is 8 bytes
      on Postgres, SQL Server and Snowflake and 4 on DuckDB and MySQL, and is
      not a type on Presto.
    - A SQLite error from "user-defined function raised exception" is
      SQLAlchemy's Python `floor`, not SQLite's.

14. **Lessons from the filtered-aggregate round (`Cover.lean`, 0.3.450).**
    - Match what was asked; never rewrite it into what a summary serves. The
      round first built `agg(x ? c)` in the Factory as `reagg(case when c then
      agg(x) by G, cols(c) end)`. It answered right, but it decided the plan
      before the planner chose a source (it scanned the trees twice when no
      summary applied), changed the concept a stored `count(x ? c)` would
      match, and added a stage whose order mattered (`count_distinct(k ? c)`
      was rewritten too late to be seen). It was replaced by a matcher rule
      and a CASE in the rollup render; the law suites did not change.
    - A summary may store the filtered aggregate itself. The first matcher
      version wrapped that column in the CASE again and crashed; the render
      now applies it only where it reaches a table storing the unfiltered
      partial alone. Test the exact match beside every new rollup path.
    - Apply a filter where the stored column is first re-aggregated, and only
      there: a target that is a property of the summary key re-aggregates
      twice, and the second pass, after the join, has no condition columns.
      A render that cannot see them raises; it never skips the CASE.
    - A strict xfail hides a crash as well as a miss. Hold every `OPEN` shape
      to the tree rows' answer in its own test: a `KeyError` this round sat
      behind one.
    - Re-test a handoff's prediction rather than trust it. The cover handoff
      said resolving `_summary_leaves` would answer per species; with the
      nested walk also resolving it did not, over data built to break it
      (two species sharing a display name).

15. **Lessons from the joined-condition round (`Cover.lean`, 0.3.452).**
    - A pinned source path that runs before the search wins by position, not
      by cost. The first version of this round planned the joined CASE in a
      pinned path and declined it when another table stored `agg(x ? c)`
      itself: a precedence rule written into one path. Stating the join as an
      obligation of the binding (`ObligationKind.JOINED`) let the cost order
      choose, and the no-join table won on `sources` with no special case.
      `_plan_finer_filter_rollup` still has the old shape (`joined` is its
      first sort key); it is the next candidate for the same move.
    - An obligation that can never be discharged must still be minted, with
      no satisfiers: every other kind is skipped when nothing can satisfy it,
      which here would accept a summary with no join and an unfiltered sum.
    - The cover enumeration runs in Rust (`network_search.rs`). A new
      obligation kind goes into both, with its string-order rank in `Kind`, and
      into the parity cases of `test_v4_network_search.py`.
    - Two spellings of one value should be one key in discovery. `x ? c` and
      `case when c then x end` were matched, guarded and signed apart, and the
      CASE form of a count did not parse. Run the law suite in every spelling
      as an axis, not as a sample.

16. **Lessons from the relation round (`Cover.lean` §6, 0.3.457).**
    - The A/B recipe's `PYTHONPATH=<copy>` lost to pytest's rootdir insertion:
      `tests/` is a package, so pytest prepends the repo and the branch's
      `trilogy/` is imported. The first A/B of this round showed 0 failures on
      main for that reason; step 4 now passes `--import-mode=append`. A/B
      numbers in earlier rounds' PRs that were run through pytest the old way
      may have compared the branch to itself.
    - Before serving a shape from a summary, check the tree plan against the
      model's own invariant (a group's answer is the answer filtered to it).
      `select ecoregion_id, count(tree_id)` disagreed with its `where
      ecoregion_id = 1` twin, so no summary path could have matched it
      honestly; the handoff's "match the tree plan, whatever it does" was
      matching a bug.
    - A dedup elided because a node CLAIMS a grain is only as right as the
      claim. An unnest's rows claim `(row key, element)` and break it when a
      list repeats; a literal `unnest([...])` has no row key and its repeats
      are rows. The test that tells them apart is whether the grain holds a
      key beside the element.
    - The group graph decides where a WHERE lands before source planning sees
      it: under a filter on the list's parent it filters those rows, not a
      rolled root, so a relation rollup marked under a WHERE silently drops
      it. Mark only what the plan downstream will filter.
    - Ablate guards in combination. The `at_own_grain` guard in
      `_relation_rollup_source` ablates clean alone, and is what keeps 15
      cases right once the unnest-root marking is also off.

17. **Lessons from the window-grouping round (`Windows.lean`, 0.3.458).**
    - A lead that reads like a modelling nit can be a wrong answer one step
      away. "A value window's keys are its operand" was recorded as why a
      suite compared sets; grouping by the window (`select l, count(id)`)
      counted by the operand. Before parking a lead, put the concept in each
      position a select gives it: alone, beside an aggregate, beside its own
      inputs.
    - A substitution that is right for one consumer is wrong for another:
      a window's keys stand in for it as a join key, never as a group key,
      unless the map from keys to value is injective
      (`Windows.group_by_tuple_splits_value`).
    - A law suite that reuses a cached executor across cases shares its
      environment: an `auto w` from one case was still defined in the next,
      and the failures moved with `-n`. Give each case a fresh environment
      when the query defines concepts.

18. **Lessons from the order-aggregate round (`Windows.lean`, 0.3.459).**
    - A parse-time rule and the build step it predicts must agree. The
      grain contribution treated every aggregate under a window as
      inheriting the select's grain, while the build already pinned a bare
      order aggregate to the window's anchor; the exemption was written
      against the parse-time view and kept the old split for shapes the
      build had fixed. `_pinned_by_window` now names the same condition as
      `_window_order_by_items`.
    - A correctness fix can be an efficiency fix: pinning the order
      aggregate to the select's own groups let the plan read it from the
      GROUP BY, and a test that pinned the old join (`test_names`) was
      pinning the wrong answer's plan. Read the SQL a shape test holds
      before rebaselining it.
    - An existing law suite's oracle can encode the bug. The rollup suite's
      window oracle summed per the window content alone ("at the window
      content's grain"); it now sums per the tuple ranked.

19. **Lessons from the select-grain round (`SelectGrain.lean`, 0.3.462).**
    - Check a "bug" against the language's own rule before fixing it. An
      aggregate with no `by` is at the select's raw grain, always. 0.3.459
      read a window's order aggregate as needing a grain of its own and
      pinned it to the tuple the window ranks; the next round extended the
      pin to nested and named aggregates, and TPC-DS q36/q70/q86 (reference
      answers) and the rollup window tests went red. The pin was the bug:
      the real one was the first-argument pin before it, which the select
      grain already fixes.
    - A law suite whose oracle mirrors the fix proves nothing about the
      rule. Both 0.3.459's suite and the rollup oracle were rewritten to the
      pin; state the oracle from the rule (`by <select grain>`) and compare
      the spelling `by <select grain>` directly.
    - Once a value depends on the select grain, its KEYS are the select
      grain. A window ordered by a bare aggregate kept its arguments as keys,
      and a summary plan joined it back on them and fanned out; set at the
      select context (`_window_reads_select_grain`), the read-back is on the
      whole grain.
    - `group x by k` fixes its grain like `sum(x) by k`; a walk that only
      exempts `AggregateWrapper.by` reads it as select-grain
      (`usa_names::test_group_by_with_existing`).
    - A parser that hydrates in dependency order is only as right as its
      dependency scan. `extract_dependencies` read CONCEPT_LITERAL nodes,
      and a bare name in `over (order by s)` is an ORDER_IDENTIFIER token,
      so the window could hydrate first and keep an `UndefinedConcept`. The
      symptom was a wrong key set three layers away (a join on `g` alone);
      look at the stored lineage before the planner.

20. **Lessons from the fixed-aggregate round (`SelectGrain.lean`, 0.3.463).**
    - Pair a value with its own twin. `sum(y) by id` over a table unique on
      `id` is `y`; `select g, s, count(id)` with `s <- y + 0` and with
      `s <- sum(y) by id` must agree, and they did not. A twin finds the
      bug without an oracle and points at what differs (here, derivation).
    - A coverage shortcut that holds for one target can be reached by
      another. The group check stopped counting `by` keys only when every
      target column was an aggregate, as if a non-aggregate beside one
      anchored the rows to its keys; it does not.
    - Removing a skip exposes what the skip hid. Letting the FINAL dedup a
      grouped contributor reached `_dedup_to_targets`' re-sum, written for a
      summary scan's partials, and re-summed a computed count under a HAVING
      (TPC-DS q23 returned 91 rows for 4). Ask whether the aggregate is a
      partial at the node's grain (`pending_rollups`) before re-summing it.
    - Read a size-budget overrun against the rule before rebaselining. Six
      TPC-DS queries grew a GROUP BY; each drops a key its aggregate is `by`,
      so the dedup is the distinct-tuple rule, not a regression.
    - An ablation that writes artifacts can poison the next one. A failing
      TPC-DS size check restores the committed log, so ablating before
      committing the rebaseline compared later ablations to the old sizes; a
      skip for identity dedups looked load-bearing until it was rerun after
      the commit, and was dropped.

21. **Lessons from the fixed-value lifecycle round (`SelectGrain.lean`, 0.3.464).**
    - Follow a value through its life, not just its first select: filtered,
      gated, wrapped in a rowset, aggregated, persisted and read back. The
      positions the previous round listed (WHERE, HAVING, rowset) held on
      main (0 of 1,920); every bug was one step further on (an aggregate of
      the value, a rowset member's outer grain, a read back after persist).
    - Spell the same value five ways (`s`, `t <- s`, `t <- s + 0`, `(s + 0)`,
      inline) and diff them before writing an oracle. Parentheses alone
      (`sum((sum(y) by g))`) changed the answer; the twins pointed straight
      at the walk that treats a `BuildConcept` argument and a `Function` one
      differently.
    - A twin can share the bug. `t <- s` was the "named" baseline and was
      itself wrong beside `g, h`; only the reference showed which side of a
      disagreement was right.
    - A persisted copy is a claim the planner reads back. Diff every read
      before and after a `persist`; a table storing a `by` value at a finer
      grain looked like a summary partial to the rollup matcher, and three
      unrelated reads went wrong at once.
    - Helper names collide in `strategy_builder.py` (a second
      `_reads_aggregate` silently shadowed the first); grep before naming.

## Candidate areas

**Suggested next: the area-1, area-2 and area-4 leads below, or the area-3
leads left.**

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
`sum(x)`) and a partial read into an expression without its re-sum.

Done in 0.3.443 (see "What it found" in `formal/README.md`): an expression
over an exact-grain aggregate reads the stored column; `count_distinct` of a
grain column is proved and served (`Rollup.count_distinct_grain_column_exact`);
a WHERE over a stored group value filters the summary rows before the re-sum
(`Rollup.group_value_filter_exact`); a membership on a grain key re-sums the
partial. The finer-filter hunt (about 1,800 shapes) found no summary read under
a filter its grain does not determine. Open questions:
- `count_distinct` of a grain column under a filter or grain over `tree_form`
  reads `trees ⋈ enrichment`, not `tree_summary ⋈ enrichment`: a cost choice
  (area 1b's dominance is by component subset), correct either way.
  `DISTINCT_GRAIN_COLUMNS` in the harness marks it.
- `max(avg(x) by species)` and `avg(x)` beside `avg(x) by *` decline to the base
  rows, as the `sum` versions of those shapes do (`MIXED_GRAINS`).
- `select tree_form, sum(sum(height) by city, species) where sum(height) by *
  > 100` reads the base rows; the same shape without `tree_form` is served.
- A WHERE over a stored group value declines to the base rows at a grain
  the summary does not bind (`tree_form`, a CASE over it), and for a grand
  total under an OR that joins `tree_form` (`GROUP_VALUE_GRAINS` in the
  harness). Serving them needs `_plan_joined_filter_rollup` to also group by
  a joined dimension's column.
- A WHERE over a stored aggregate at a grain COARSER than the summary's
  (`sum(height) by city > 6` beside a `(city, species)` summary) reads the
  base rows. Filtering the summary needs the condition's value joined back on
  `city`; `_plan_joined_filter_rollup` could source it.
- The harness is `tests/discovery/test_rollup_resolution_laws.py`. Extend its
  matrix rather than starting a new one.

### 1c. Summary coverage (`Cover.lean` follow-ons)

See `docs/handoff_summary_cover_next.md`: the approach, and the open items in
order. Items 1 (filtered aggregates, now for `sum`/`min`/`max` too) and 2
(totals under a finer WHERE) are done in 0.3.450, but for one case. Leads
from that round:
- **Done in 0.3.452: a condition on a property of the summary key**
  (`bucket` of `species`, `common_names[1]`), as a `JOINED` obligation of the
  summary's binding in the source search, and both spellings of a filtered
  aggregate (`aggregate_filter`). Left: `_plan_finer_filter_rollup` is still
  a pinned path that runs before the search with its own ranking; the
  scan reads the summary at its full grain under a joined condition, where a
  re-sum to the target plus the condition's keys would be exact
  (`Cover.resum_then_case_exact`) and smaller.
- **Done in 0.3.457: rule 6, relations keyed on the grain**, with no WHERE
  (`_plan_relation_rollup`, `trilogy/core/processing/relation_rollup.py`;
  `tests/discovery/test_relation_rollup_laws.py`). Leads left:
  - Under a WHERE the relation shapes read the trees. The group graph places
    the WHERE on the list's parent rows (`native_ecoregions` by `species`), so
    a rolled root never sees it; marking under a WHERE dropped it (wrong rows
    under `species` and `bucket` filters). Serving it needs the group graph to
    hand a rolled root the WHERE its rows need, as `scan_filtered_roots` does
    for a finer filter.
  - `species, ecoregion_id, sum(height)` under `merge ... into ecoregion_id`
    reads the trees (`OPEN` in the relation suite): the request carries the
    list beside the merged key.
  - `_plan_relation_rollup` is a pinned path like `_plan_finer_filter_rollup`,
    and declines when a table storing a matching aggregate binds a related
    target itself; the `JOINED`-obligation move (lesson 15) applies to both.
    It does not see a table binding the target's KEY: `biome` beside a
    `(city, ecoregion_id)` summary reads the species summary through the
    relation join (right, but the larger read).
  - A list declared `::unique` (`std.collection`) skips both dedups and is
    validated; `unique` is matched by its last name segment, so a model's own
    `type unique list<...>` means the same. Other paths that dedup an unnest
    (a WHERE on an element) do not read the claim yet.
  - Grouping by an unnested element now dedups `(row key, element)`, but
    `select ecoregion_id, tree_id` still returns a pair once per repeat. That
    is the documented element-keyed-by-ordinal behaviour
    (`test_element_pairs_with_the_row_it_expands`); the aggregate's input
    grain and the select's rows now disagree on repeats, which is worth a
    decision.
- A summary storing `agg(x)` at exactly the target grain does not answer
  `agg(x ? c)`: the matcher pairs them only where the partials are
  re-aggregated.
- **The statement's lineage is still stale** after `_reaggregate_partials`.
  This round resolved the two walks that read it (`_nested_rollup_roots`,
  `_summary_leaves`) and the rollup render; a rewrite of the statement's own
  nested lineage would retire the class.
- `siblings/scalar` under a WHERE on a stored column right-joins the summary
  to the trees' distinct `(city, species, plant_year)`: right, but a scan the
  summary makes unnecessary.

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

### 2. Optimizer rule soundness (done in `Rewrite.lean`; leads left)

`Rewrite.lean` proves the NOT NULL strip, outer-join upgrades (FULL to LEFT,
LEFT to INNER, the OR rule) and pushdown (into an INNER side, a preserved
side, a padded side under a null-rejecting atom, past GROUP BY keys and a
window's partition, never past a LIMIT) keep the rows, with a counterexample
per guard. `tests/optimization/test_rule_soundness_laws.py` holds the real
planner to a pre-joined twin over table and query addresses, disables each
rule over the same queries, and checks `redundant_not_null`'s premise
directly: every column it would strip is never NULL in its CTE before the
WHERE. "What it found" in `formal/README.md` lists the eleven fixes. Leads:
- Done in 0.3.442 (see "What it found" in `formal/README.md`): the
  row-argument cross join, the FLAT keyless-join failures, q83 unhealed
  (`test_eighty_three_unhealed`), and laws for the identity GROUP BY, the
  value-set upgrade, the existence fold and the semi-join pushdown.
- Done in 0.3.443: both "could only be placed after the aggregates" errors
  (a pre-aggregate column peeled per column, not per atom), and a `by *` gate
  beside a nested aggregate. `MIXED_ATOMS` in the suite is the family.
- **`merge_irrelevant_group_by` fires on no shape of the soundness model.**
  0.3.443 counted its real firings over the regressions and 300 random
  queries in all three addressings, plus rowset bodies over a DISTINCT: zero.
  Its preconditions (a grouped child with no joins and no WHERE over one
  grouped parent it alone reads) seem pre-empted by `collapse_single_parent`,
  which runs first. Count its firings over the TPC-DS corpus before writing
  more shapes; if it never fires, it may be dead.
  Its law is proved (`merged_group_sum_exact`, `dropped_dedup_double_counts`)
  and the suite ablates it, but that ablation compares identical plans. Find
  a shape that fires it (a child aggregate over a pure-DISTINCT parent CTE;
  rowset bodies did not) and add it to `FIRES`. `fold_existence_into_aggregate`
  fires only under table addressing.
- **The mixed partition cover.** `union_sources` mints `web_sales +
  catalog_returns + store_returns` as a cover of `channel` in TPC-DS
  `all_sales`, because `return_channel_dim_id` is bound by those three and
  not by `web_returns`. 0.3.442 stops it shadowing the returns family, but a
  union of different facts is not a partition of one table; arms of one
  family should share a fact identity.
- **A gated `~` peel is hosted at the peel** and is now built preserving the
  gate side (`condition_injection.completed_by_feeders`). Rewrite.lean models
  the row-level consequence (`preserving_exact`), not the peel itself.
- `PredicatePushdownRemove`'s whole-condition path (every parent covers the
  WHERE) does not check `_parent_nullable_in_cte`, which the per-atom path
  does. No shape reaching it was found (it needs a column read only off a
  padded parent that another parent also filters); `Rewrite.remove_past_padding`
  says what goes wrong if one does.
- Not a bug: TPC-DS `query-3`'s `subset join wr_grouped.wr_wp_id =
  ws_grouped.ws_wp_id` over returns listing web pages no sale has; key-set
  equivalence narrows on the declaration, by design.

### 3. Leftover function leads (verified on main after #802)

Done in `Scalars.lean` (0.3.447): the month-end clamp, `date_add`'s declared
type (a whole-day offset of a date is a date; a datetime keeps its type),
`greatest`/`least` over the present arguments, `log`'s base order, SQLite
`strpos`. Leads left:

| Lead | DuckDB | SQLite | Notes |
|---|---|---|---|
| `7 / 0` | `inf` | `NULL` | BigQuery `COALESCE(SAFE_DIVIDE(..),0)` and Snowflake `DIV0` give `0`. `NullReject` treats `/` as total, which holds on DuckDB only. Decide one semantics. |
| `date_trunc(t, month)` of a datetime | a timestamp, typed DATE | a date (0.3.447) | DuckDB casts the truncation of a DATETIME back to datetime; comparisons coerce there, so no wrong answer was found, but the value is not its declared type |
| `null::int` on ClickHouse | | | renders `CAST(NULL, 'Int64')`, which ClickHouse refuses; a NULL literal needs `Nullable(...)` |
| `split`, `hash(.., md5)` on SQLite | | not functions | raise rather than answer; no portable rendering yet |

Done in `Types.lean` (0.3.448): `date_trunc` on DuckDB is now a date, and the
declared types of `sqrt`, `**`, `%`, `greatest` / `least`, decimal products,
decimal quotients and a decimal beside a float hold their values. Leads left:

- **Decimal precision.** A sum keeps its operands' precision
  (`numeric(12,4) + numeric(12,4)` is `numeric(12,4)`), so a persisted sum can
  overflow; that raises rather than rounds, so it is loud, but `sum` of a
  decimal column over many rows is the likeliest real case.
- **`random(n)`** is typed FLOAT and renders `random()` on DuckDB (a fraction)
  but `abs(random()) % n` on SQLite (an integer below `n`). Decide what it means.
- **CASE arms of differing column types** (`case when b then i else f end`)
  are rejected on purpose while `coalesce(i, f)` merges; a literal arm is
  widened. If that strictness is relaxed, the arms merge by `Types.join`.
- **Chained casts** (`t::date::datetime`) parse the second type as a trait of
  the first and raise.
| Presto `date_part` | base `date_part(x, part)` | | not a Presto function; renders invalid SQL |
| SQL Server `date_trunc`, `date_part` | | | the base renderings are likely invalid T-SQL |
| `decimal / decimal` | DOUBLE on DuckDB | | the declared type is the merged NUMERIC; check materialization as in #802 |

`tests/core/test_function_laws.py` and `formal/Trilogy/Functions.lean` are the
place to extend for these.

### 4. Windows, ORDER BY and set operators (done in `Windows.lean`, `SetOps.lean`; leads left)

`tests/windows/test_window_laws.py` holds numbering windows, value windows
(`lag`/`lead`, aggregate windows with and without an order) and ORDER BY NULL
placement to a reference over random rows with NULLs and ties, on DuckDB and
SQLite. `tests/dialect/test_null_order_renderings.py` pins every dialect's
rendering. A plain direction places NULLs last everywhere (DuckDB's
placement); `nulls auto` is the engine's own. Leads left:
- **Done in 0.3.463: a select of an aggregate at a finer grain** returns the
  distinct tuples, as `select g, x` does, and a bare aggregate beside it is
  per that tuple (`SelectGrain.by_keys_split_tuple`,
  `tests/core/test_fixed_aggregate_select_laws.py`). Leads left:
  - TPC-DS q23's rowset `select item.sk having ss_combo_count > 4` now
    groups twice: by `(item.sk, ss_combo_count)`, then by `item.sk`. The
    first is the FINAL dedup of the rowset keeping the HAVING column; one
    group would do.
  - A FINAL dedup keeps the hidden join keys a node exposed (q30 groups by
    `return_customer.sk`, q79 by `pos_address.sk`, beside the projected
    columns), so it groups by more than the tuple needs.
  - Done in 0.3.464: WHERE and HAVING over the value, rowsets of it, an
    aggregate of it or of a scalar of it in any spelling, a rowset member as
    an outer key, and a persisted copy read back
    (`tests/core/test_fixed_value_lifecycle_laws.py`). Leads left:
    - A table storing `sum(y) by g` at grain `(g, h)` is now never read for
      `s`: the planner falls back to the base rows. `max(p.s)` grouped by a
      target holding `g` (any value of a repeated one) would answer from the
      table; that is an efficiency, not a correctness, gap.
    - Ranks and other windows ordered by a scalar of the value (`rank g by
      (s + 0)`) are not in the suite; the twins for them parse only in the
      named spelling.
    - A rowset member keeps its source's METRIC purpose. The relevance fix
      treats a member that is its own grain as a key; a member at a coarser
      grain than the rowset still reads as covered by its grain.
- **Done in 0.3.458: a value window's keys were its operand**, and a group by
  a window's value grouped by those keys (`select l, count(id)` counted by
  `x`; `Windows.regroup_by_value_exact`, `tests/windows/test_window_group_laws.py`).
  Done in 0.3.459 and corrected in 0.3.462: a window ordered by an
  aggregate `by` fixed keys groups by its value; one ordered by an aggregate
  with no `by` reads it at the select grain (`SelectGrain.bare_is_select_grain`),
  gives the select its keys, and is keyed on the select grain
  (`tests/windows/test_window_order_aggregate_laws.py`,
  `tests/windows/test_window_order_expression_laws.py`). Left:
  - a select of only a window ordered by a bare aggregate (`select r`) has
    the window's keys as its grain, so ranks repeat once per tuple. That is
    the select-grain rule followed through; decide whether such a select
    should warn, as the finer-grain lead above asks.
  - a window ordered by a bare aggregate in a select with no keys (only
    aggregates projected, often a WHERE gate such as TPC-H `adhoc03`) orders
    by the grand total, so every row ties
    (`SelectGrain.bare_empty_grain_is_total`). That is the rule; a warning
    that names `by <argument>` would catch the likely intent.
  - a window ranks the tuples of its grain, so `rank(g) over (order by
    sum(y))` under `select g, p` counts two `p` of one `g` that tie twice;
    the suites hold that reading, the same as `order by sum(y) by g, p`.
  - `lag`/`lead` ordered by an aggregate navigate the operand's rows, so
    ties in the aggregate leave their order to the engine; the suite does
    not cover them.
- **SQL Server orders by a select alias inside the NULL flag.** T-SQL
  resolves an alias only as a bare ORDER BY term, so `CASE WHEN "alias" IS
  NULL ...` fails where the FINAL orders by a column derived in it. No
  engine runs in CI; render through the expression there or keep the alias
  bare and wrap the select.
- **Frames.** `rows between ...` and `row_number` over ties are not modelled;
  the suite only orders `row_number`-like navigation on a total order.
- **`union`/`except`/`intersect` TVFs** are modelled in `SetOps.lean` and
  held by `tests/setops/test_setops_laws.py` (2,304 random queries with
  NULLs and duplicates, no wrong answers); `tests/engine/test_duckdb_setops.py`
  pins the SQL. `union` keeps the bag and is not in the Lean model.
- **`coalesce`/`case` type merging:** whether a merged type stores every arm's
  value, the same materialization law as #802.
