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
one read the value once per key; area 4), and `Padding.lean` (0.3.465: an
outer join pads NULL and a cast keeps NULL on ClickHouse too; the window and
select-grain law suites now run on chdb; area 5), and `Functions.lean` /
`Types.lean` extended (0.3.466: the function, scalar and type law suites run
on chdb; a cast and `round` round half away, `week` is whole spans, and a
remainder keeps its fraction on ClickHouse and SQLite; area 5), and
`NullReject.lean` extended (0.3.469: `x / 0` and `x % 0` are NULL on every
dialect, and a persisted copy declares `?` every column its write can hold
NULL in; areas 3 and 5), and `StringMatch.lean` (0.3.470: one pattern language
for `like`, `ilike` and `contains` on every dialect, a wildcard-free `like` is
an equality, and a ClickHouse string keeps its backslashes; area 6), and
`Strings.lean` (0.3.471: `len`, `substring` and `strpos` count characters,
one reading of `substring` positions, `regexp_replace` replaces every match,
`regexp_extract` returns the group asked for, `like '%'` is `is not null`, and
the NOT NULL strip reads a sibling's proof; area 7), and `Casts.lean`
(0.3.472: a literal the parser folds is the value the engine computes, a
decimal cast rounds half away to its scale, one reading of a string as a
bool, and `sqrt` / `log` are NULL outside their domain; area 8), and
`Readings.lean` (0.3.473: a string cast to a number or a date has one reading
on every engine and is NULL without one, a cast from a string is
NULL-introducing, and ClickHouse membership keeps NULL identity; area 9), and `Instants.lean` (0.3.474: a string cast to a datetime or a timestamp
names one UTC instant on every engine, `date(s)` / `datetime(s)` /
`timestamp(s)` of a string are its cast, and a numeral past its target's
range is NULL; area 10), and `Writings.lean` (0.3.475: a float, a decimal
and an instant cast to a string have one text on every engine that reads back
as the value, and a decimal quotient is a double on ClickHouse; area 11), and
`Sharing.lean` (0.3.476: a cast or a clamped substring evaluates its operand
once, so nested casts grow linearly, a random operand is drawn once, and the
SQLite reads parse on SQLite 3.45; area 12), and `ValueSet.lean` (0.3.477:
a table at its own grain answers repeat-blind aggregates of its columns, and
aggregates of a column it stores at that grain, from its own rows; area 1),
and the coarse-gate follow-on (0.3.488: a re-sum is never joined on its
partial, a gate on a re-sum of a stored value plans, and a coarse gate at the
summary's own grain reads it; area 1), and the oracle sweeps (0.3.490 to
0.3.495: selects checked against Python over the raw rows, not against
another plan, and A/B'd against main for the summary reads and joins they
cost; 0.3.493 adds a second family of spellings; 0.3.494 takes that
family from 127 wrong rows to 5; 0.3.495 to 0.3.498 sweep seeds 10,000-60,000;
area 1).
`count(cls)` beside a stored `count(id) by cls` is the re-sum over the
non-null classes (`ValueSet.count_column_from_row_count`) but the planner
does not rewrite it; that rewrite is open.

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

22. **Lessons from the ClickHouse NULL round (`Padding.lean`, 0.3.465).**
    - Run the law suites on every engine CI has, not only the two they were
      written for. chdb is in CI on Linux; adding it to the window and
      select-grain suites found five dialect bugs no DuckDB/SQLite probe could,
      and the area-4 leads this round set out to probe (windows ordered by a
      scalar of a fixed value in every spelling, a rowset member at a coarser
      grain) held on main (0 of ~800).
    - An engine whose types are NOT NULL by default reads NULL as a value in
      many places: an outer join's pad, `lag` past an edge, a cast. Diff a
      left join's padded row first; `count` of the right key is the probe.
    - Raw SQL in a fixture is part of the harness's portability:
      `cast(null as varchar)` raises on ClickHouse, `nullif('', '')` does not.
    - A ClickHouse statement setting placed after the final select applies to
      its CTEs and to the select of an `INSERT`; one setting per statement is
      enough for a padded join anywhere in it.
    - Time the new dialect's share of a suite before adding it to CI: chdb
      runs the select-grain fixtures slower than DuckDB, so ClickHouse runs
      the first seed only (`seeds_for`).

23. **Lessons from the ClickHouse function round (0.3.466).**
    - A suite that compares two engine answers (a live value and its
      persisted copy) cannot see an error both share. `n % 3` dropped the
      fraction on ClickHouse and SQLite in both, and the copy suite passed;
      only a test against a Python reference found it. Give each operator one
      reference check before trusting a round-trip suite with it.
    - chdb is one session per process: a table one executor creates is there
      for the next. A harness that creates fixed table names drops them first.
    - A dialect's base type name can be parametric already (`Decimal64(4)`);
      appending `(p,s)` to it is invalid DDL. `NUMERIC_TYPE_NAME` names the
      parametric form separately.

24. **Lessons from the zero-divisor round (`NullReject.lean`, 0.3.469).**
    - A semantic choice the dialects split on five ways is a correctness bug
      even where no single engine is wrong: decide it once, render it
      everywhere, and hold every engine to a Python reference for it.
    - A dialect's answer feeds the planner's claims. `/` was listed total
      because DuckDB, the reference, gave `inf`; the strip of `is not null`
      trusted that claim on SQLite, where it was false.
    - Follow a value through its lifecycle: a persisted copy is a new
      datasource whose bindings are claims too. The copy declared every
      column non-null, so reads of it dropped `is not null` on every engine.
      Probe `select c where c is not null` and `count(c)` against the copy,
      for a `?` column, a derivation of one, and an aggregate over a `~`
      join; and on ClickHouse look at the DDL, which is NOT NULL by default.
    - chdb's result conversion prints `inf` and `nan` as `None`, so a
      ClickHouse probe of `x / 0` looks like NULL; `coalesce(x / 0, -1)` or
      `x / 0 is null` tells them apart.

25. **Lessons from the string-matching round (`StringMatch.lean`, 0.3.470).**
    - Sweep the renderings of every dialect, not only the engines in CI. The
      engine probe found three splits; rendering the same three predicates
      for all ten dialects found four more (`ilike` and `contains` emitted as
      functions four engines do not have).
    - A data-driven matrix needs data that comes close to its claims. Random
      patterns against random strings almost never matched, and the computed
      pattern suite passed on main; patterns derived from each row's own
      value (an escaped special, a swapped case, a spliced wildcard) failed
      on all three engines.
    - A fix in the rendering can trip a pass that reads the rendered SQL. The
      executor's colon escaping read `'\'` as an unterminated literal and
      swallowed the next bind parameter on SQLite; only a test that bound a
      parameter after a `like` caught it.
    - Equal predicates should be one predicate to the planner. `city like
      'Boston'` and `city = 'Boston'` were the same filter, and only the
      second reached a partition's `complete where`.
    - Ablate every change, including the plumbing: chdb's parameter escaping
      failed nothing until a test selected a string constant, which is the
      only path that binds one there.

26. **Lessons from the cast and folding round (`Casts.lean`, 0.3.472).**
    - Probe literals AND columns. A literal probe (`select f(...) as x`)
      tests the parser's fold as much as the engine: `null = null`,
      `'t'::bool` and `'3.14159'::numeric(10,2)` never reached an engine.
      Look at the generated SQL of every literal that disagrees; then run the
      same expression over a column, where the dialect's rendering decides.
    - A probe harness on SQLite loses its table after one failing statement
      (the uncommitted insert rolls back), and every later query returns no
      rows. Rebuild the fixture per query before reading an empty result as a
      bug.
    - The NULL-introducing list is a claim, like the zero divisor in lesson
      24. Moving `sqrt` and `log` to NULL outside their domain made the
      "total function" conformance test fail first: read that failure as the
      claim to change, not as a regression.
    - A decimal on SQLite is a REAL, and a float tie that is not exact in
      binary (`2.675`) rounds by the binary value there and by the shortest
      decimal on DuckDB and ClickHouse. Hold SQLite and float columns to
      binary-exact ties (multiples of 1/8).

27. **Lessons from the string-reading round (`Readings.lean`, 0.3.473).**
    - Probe a column, not only a literal, for every cast source. The literal
      probe of #902 raised a parse error on `'abc'::int` on all three engines
      and looked consistent; the same strings in a column gave three answers
      (an error, 0, NULL), and ClickHouse refused `' 12 '` and `'1.5'`, which
      every other engine reads.
    - Define the reference by a grammar, not by an engine. DuckDB's own
      reading accepts `'1_000'`, `'0x10'`, `'inf'` and `'2020-1-5'`, which
      nothing else does; a pattern guard around each engine's read gave one
      answer with little new code, and a portable POSIX pattern with no
      backslash or brace survives every dialect's literal and parameter
      syntax (ClickHouse's `{name:Type}` would read `{4}`).
    - Where an engine has no regex, a conjunction of GLOBs can still state a
      regular grammar exactly; fuzz the rendering against the pattern before
      trusting it (31k strings, then `Readings.glob_number_exact`).
    - A new NULL source is a planner claim twice over: nullability (the
      persisted copy) and membership (`_null_free_set`). Test `x in S` with a
      NULL probe, not `x in S and x is not null`, which hides the claim: a
      plain `IN` and a stripped `is not null` agree.
    - A law suite's new axis can expose an engine bug unrelated to the round:
      ClickHouse decorrelates a correlated `exists (... <=> ...)` into a join
      that loses NULL identity. Test the shape apart from the round's feature
      (`test_membership_is_identity`) so the fix is held on its own.
    - DuckDB's `TRY_CAST(... AS DATE)` accepts trailing whitespace after a
      date but not after a time; trim before the read, not only in the guard.
    - Run the modeling suites before choosing a grammar. The first grammar
      was strict `YYYY-MM-DD`; TPC-DS builds birth dates as `'1980/5/3'`,
      binds a physical DATE column to a `string` concept, and renders
      non-finite floats as `'inf'::float`. Each broke only there, and each is
      a reading real models rely on: widen the reference to it, then hold
      every engine to the wider one.
    - A read guard costs SQL size wherever the planner inlines the cast:
      TPC-DS q99 reads `date.date` sixteen times. Measure the growth with
      the guard replaced by the bare cast before raising a size gate; here
      every query matched or beat its old size without it.

28. **Lessons from the oracle sweep round (0.3.490).**
    - A suite that compares the summary plan to the base plan cannot see a
      bug both share. A random sweep against SQL written by hand over the
      raw rows found one in the base plan (a property misjoined off a group
      without its key) that 3,000 harness cases had passed.
    - Know the language before writing the oracle: Trilogy's `not` is
      two-valued (`not (null > 6)` holds), and a WHERE aggregate reads every
      row, not its peers' filtered ones. The first sweep's "bugs" there were
      the oracle's.
    - Add the grain the planner reduces. `species, tree_form` reduces to
      `species`, so any check written over grain components
      (`_finer_filter_rollup_source`'s "every output is a grain key") sees a
      projected property as a stranger. Probe a key beside its property.
    - A pinned plan that declines hands its request to the search, which
      has none of its guarantees. Ablate the pin (return None) and diff: the
      search's mistakes are the ones the pin hides.

29. **Lessons from the wider oracle round (0.3.491).**
    - Generate the spellings a user writes, not one canonical form. This
      round's first generator parenthesized every compound WHERE; the bare
      `gate and row-atom` spelling lost the row atom on main, and the
      parenthesized one did not.
    - A literal `in (...)` is the cheapest variant to add and found the most:
      its existence arguments are `[()]`, truthy, so placement read it as a
      set lookup. Test such a list with `any(...)`. Three more sites in
      `condition_placement.py` still test the list's truthiness (the
      membership-producer route, the region-domain check, the disconnected
      raise); none showed up in the sweep, but they are the next place to
      look. Changing `existence_arguments` itself to `[]` broke four shapes.
    - Compare a family against its `=` twin before calling a new error a
      regression: two `in` shapes began raising mid-round, and their `=`
      spellings already did on main. Fixing the twin's error removed both.
    - Ablate every change against the committed sweep (a pytest plugin that
      monkeypatches one change off), not only the suite; each of this
      round's eight changes failed something when removed.

30. **Lessons from the wider sweep round (0.3.492).**
    - Run the sweep past the committed seeds before fixing anything: 20,000
      seeds of 0.3.491's generator found eleven more bugs in four families,
      none of which the committed 600 seeds reached. Keep the new seeds that
      failed on main (`FOUND_SEEDS`) rather than raising `SEEDS`.
    - Measure efficiency in the same A/B, not only correctness: per shape,
      whether the summary is read and how many CTEs and joins the SQL has
      (scratchpad `effsweep.py` over `shape(seed)`). The first cut of this
      round's summary guards fixed every wrong row and lost the summary on 44
      shapes main read correctly, and `test_rollup_resolution_laws.py` failed
      56 cases on it; narrowing each guard to the shape that broke brought it
      back to 1. A guard written for one repro needs a census of what else it
      catches.
    - The grain the planner reduces bites guards too: `city, species,
      tree_form` reduces to `(city, species)`, so a guard asking "does the
      select hold `tree_form`" must read the selected concepts, not
      `BuildGrain.from_concepts` (lesson 28).
    - Ablate per file against main's copy: copy the package to the
      scratchpad, `git show origin/main:<file>` one changed file over it, and
      rerun the shapes in question; a file whose revert restores main's
      behaviour holds the change to look at.
    - An FD that prunes a join pair is a fact about one side's rows. A pair
      off another left side is implied only once an earlier join equated the
      two sides on the determinant (`_Agreement` in `join_resolution.py`);
      the tree-reporting city builds (`test_city_build_never_pairs_on_filter_columns`)
      are the case where they were equated, the oracle sweep the case where
      they were not.

31. **Lessons from the spellings round (0.3.493).**
    - A second generator found more in 10,000 seeds than 20,000 more seeds of
      the first did (the first family's next 3,400 seeds held one wrong row).
      Keys-only selects alone exposed the summary re-summing a gate partial at
      the select's grain: every first-family select had an aggregate target.
    - Check the oracle's scope for each new spelling: an aggregate inside a
      select target's `?` condition is select-phase and reads the WHERE-kept
      rows, not every row as a WHERE aggregate does.
    - Fix where the value is consumed, not where it is offered. Dropping the
      summary's rollup binding from the network fixed the repros but read the
      trees on 248 harness cases (a partial feeding an aggregate group is the
      design); declining only a plan whose own condition reads the partial at
      a grain not its own kept every summary read.
    - A per-column check of "every output is the filtered branch's own" says
      nothing about tuples: two branches each holding kept values of one key
      pair dropped tuples through an unfiltered row stream.
    - Revert a fix that turns wrong rows into wrong rows elsewhere, even when
      it fixes the repro: rerun the committed sweep seeds after every planner
      change, not only the new tests.

32. **Lessons from the host-election round (0.3.494).**
    - "Upstream of" is not "filters". Host election treats a group as covered
      by any ancestor host, but an aggregate's value joined beside a row
      stream filters nothing in that stream. Ask which group's ROWS a consumer
      aggregates (the producers of its aggregated argument, not its `by` or
      the values joined in) and whether those rows passed a host.
    - Adding hosts creates new joins. Hosting the same atom on two groups made
      both carry the WHERE's value, and the join then paired on it with plain
      `=`; seeds that were right before lost their NULL group. Rerun the
      whole family after a placement change, and diff the wrong-row set, not
      the count.
    - A nested aggregate is the counterexample to "a ROOT predecessor is an
      unfiltered row stream": the outer aggregate has a ROOT edge for its keys
      but aggregates its inner group's value (seed 19430 of the first family).
    - Read the rendered SQL for precedence. `a or b and count(*) > 0` looked
      like a planner bug in three seeds and was a string join in the dialect.

33. **Lessons from the coarse-value round (#964).**
    - A fix that changes a root's shape can expose every adapter that only
      worked because of the old shape. Peeling a `by *` aggregate's inputs
      (#953) left the trees as a bare scan, and the projection to a value's
      grain had only ever fired on a merge: a leaf scan reported no inputs,
      so a value per species was computed once per tree and joined back on
      species (n³ rows). Test the adapter on both shapes.
    - Stage 2 and the build must agree on what a row stream holds. A value
      by genus stitched on genus holds genus (`GroupAttrs.joined_on`), but its
      emitted grain did not say so, so no sibling was asked to pair on it;
      the contracts were also read before every edge's channel was set.
    - A root asked to carry a stored key its rows determine sources it there:
      a scan cannot be widened through a join after the fact. A derived key
      (a spine, an unnested value) is not a lookup, and sourcing it off the
      scan loses its extent (`test_canonical_collision_merge.py`).
    - `test_global_fallback_oracle_laws.py` bounds every CTE by the widest
      table as well as checking rows; a fan-out within the row count (per
      species) needs a per-CTE count, which the unit file does.

34. **Lessons from the bound-HAVING round (0.3.495).**
    - A validation that rejects a shape hides the plans behind it: lifting
      the HAVING rejection on `tree_id` selects turned 97 raises into right
      rows, but also promoted HAVING values main had semijoined into the
      path main takes for the explicit `--s` spelling, which duplicated rows
      with the summary. Probe the explicit spelling of a rewrite's output.
    - A probe script that puts the repo on `sys.path` imports the branch even
      under `PYTHONPATH=<main copy>`; print `trilogy.__file__` in every A/B
      probe, not only in the suite (lesson 16).
    - Diff efficiency on the shapes a change reroutes, not only on the ones
      it fixes: every summary read lost and gained here was one family (a
      `tree_id` select with a HAVING gate).
    - A value read past a host is the kept groups' value; an atom that holds
      on NULL turns the host's padding into a pass. Ask which group a host
      reads its atom's inputs from, not only which rows it filters.

35. **Lessons from the key-source round (0.3.496).**
    - Narrow every new preference to the atoms that can hold on an absent
      group, as 0.3.495 narrowed the reroute. The cover's "rows passed the
      WHERE" preference over every gate atom fixed its repro, raised on three
      new seeds and cost 620 JOINs on 919 changed shapes; over atoms that hold
      on NULL alone the round's whole JOIN cost is 0.2-0.3%. Likewise carrying a gate key
      on the ROOT side lost 18 summary reads until it was limited to such
      gates (null-rejecting ones were right through the second scan).
    - "Does the atom hold on an absent group" is a per-argument question: a
      conjunct proving one aggregate non-null proves nothing of the others.
    - Attribute efficiency per change: a scratchpad `effabl.py` replays only
      the shapes whose SQL changed, once per change switched off (by
      monkeypatch, or a temporary env switch removed afterwards). One change
      (the FINAL-args reroute) fixed nothing the others did not and cost 120
      JOINs, so it was dropped.
    - A batch harness that plans hundreds of statements in one process can
      fail a shape that plans alone (first family 2717): when a shape fails
      only in a batch, bisect the prefix before reading the planner.

36. **Lessons from the class-beside-lookups round (#972).**
    - #970 fixed the shape its repro had (a leaf scan beside one-row
      constants) and narrowed every rule to it; the reported model reached
      species through partial lookups, so the trees were a merge, not a leaf
      scan, and the class was derived from the species itself. Before 0.3.492
      the plan was right by accident: the class table sat in the root bucket,
      so the merge's grain held `fit_class` and the grain-claim guard grouped.
      A fix narrowed to a repro needs a generator over the ingredients the
      reporter lists, not the repro alone
      (`test_partial_lookup_fallback_oracle_laws.py`).
    - The projection adapter read a parent's inputs as what it could project:
      an attribute only a sibling supplies made the projection unrenderable,
      and a value the parent computes (the class) was never projected at all.
    - A BASIC step offered FINAL every selected column it could reach, so a
      class by species carried `tree_id` and held one row per tree.
    - A nullable side is not the fact by being nullable: a lookup the other
      side's rows determine names no row with its NULL key
      (`SideFacts.determined`). Preserving it kept a species whose trees the
      WHERE dropped.
    - Only a written `by *` is a constant of the query; a bare total is the
      select's aggregate over its rows. Peeling a written total's table under
      a WHERE is exact when no atom reads that table and every selected value
      of the total also reads the kept rows; otherwise the gate needs the
      cross join (`_peel_exact_under_where`).
    - Generated data must hold the model's declarations: a `~genus` fit naming
      a genus the complete (`?genus`) enrichment lacks is not a planner bug.

37. **Lessons from the keyed-class round (#977).**
    - #975 again fixed a model that was not the reporter's: theirs was public
      (`greenmtnboy/tree_reporting`), and its `city` and fallback class are
      keys, built through an update statement's HAVING over hidden columns.
      Plan the reporter's own model before writing a repro; reduce from it
      (drop target columns while a keyless per-row CTE survives) instead of
      building up from the ingredients listed.
    - A join plan's identity is a claim. A row step claiming a coarser grain
      still holds one row per source row, so a side the plan pairs as a lookup
      is held at its keys before the merge (`_hold_lookup_sides`); the plan
      alone does not make it one.
    - The FINAL carried planned keys by widening only. A key no widening
      reaches (a derived `cell` the per-tree step determines) was dropped
      silently and the sides paired on what else they shared (`city`); it is
      looked up from its producer, as a group merge already did
      (`_project_planned_keys`).
    - What a coarser step offers FINAL covers WHERE arguments too: a per-tree
      flag carried beside a value by cell put a row per tree under each cell.
    - A guard added where a list is re-wrapped (`zip` over parents a contract
      may split) silently dropped a parent: apply it before the list changes.
    - A FINAL survivor of a passthrough fold holds its absorbed sibling's
      rows but kept only its own side of the plan: the pairing the sibling
      was planned on (the trees with a flag by city, species) named no side
      and resolve joined keyless, or refused. The survivor pairs, and is
      widened, as every group it holds.

38. **Lessons from the FINAL-lookup round (0.3.497).**
    - A fix that changes how one join is typed can remove the domain another
      relied on. Capping every WHERE-argument producer's join to non-preserving
      fixed seed 31824 and broke three shapes main was right on, where the
      producer's FULL join was what supplied a dimension's key domain; capping
      only a producer keyed off another producer kept the fix and broke none.
      Rerun the whole family after every narrowing, not only the repros.
    - A COUNT is "proved non-null" for free: it never is NULL. What decides
      whether a host's padding passes is its value there, 0. Per-argument
      checks need the absent value of each argument, not its nullability.
    - Ablate efficiency per change, then narrow by the argument the bug reads:
      the cover's "no output off a condition group" preference cost 12 summary
      reads over every such group, 7 once limited to groups producing an
      argument of the FINAL's absent-holding atom.

39. **Lessons from the constant-carry round (0.3.498).**
    - A sweep with no wrong rows still has a round in it. Seeds 40,000-60,000
      returned none on main; their 51 raises were two bugs, both of which
      threw away a correct plan at the last step. Group raises by message and
      reduce the largest group first.
    - A guard that compares a request's names with a plan's columns has to
      know which names no column carries. The joined source plan refused any
      request naming a constant (`all_rows`, the key of a `by *` total)
      because the cover search never binds one, while the one-scan path
      rendered it inline.
    - A refusal can be load-bearing. Carrying the constant on every joined
      plan made `test_derived_key_domain.py` return a customer with no order
      under `status is null`, but only after another statement had run on
      the same executor: the joined plan filters the stored `status` beside a
      LEFT JOIN that pads it, while the model reads `status` derived above
      the join. The carry is limited to conditions over aggregates; seeds
      42921 and 59635 (a row atom beside the total) still raise. Run whole
      test files, not single cases, before calling a planner change clean:
      the cases share executors.
    - Column overlap is not a pairing. `_join_elected_bridges` skipped a
      bridge once another exposed the same columns, but resolve pairs on the
      plan, which named only the skipped bridge. Decide what to join from the
      plan the join is resolved against, not from what the nodes expose.
    - Seeds 30670 and 1117 are not fixed by giving `tree_form` to the
      filtered `(city, species)` host in the cover alone. That fixed the
      reduced repro, but on the full seed the FINAL still elected the `?`
      filter's unfiltered row stream as a bridge (from stage-2 facts, where
      the host has no `tree_form`), and typed it as the preserved side. The
      WHERE there holds on a padded row (`count_distinct(...) != 10` reads 0
      past the host), so the host is rightly not the WHERE's population, and
      seed 1117 then raised. The fix has to make the host carry the property
      in stage 2 (grouping by `species, tree_form` is the same rows), so no
      bridge is elected. Separately, `collect_applied_conditions` counts a
      condition applied under a side a join pads as filtering the branch's
      rows; that is wrong, but changing it alone fixed nothing here.

## Candidate areas

**Done in 0.3.476: a rendering evaluates its operand once (`Sharing.lean`;
area 12 below).**

**Suggested next: the area-12 leads (a `random` concept is drawn once per
reference, which needs a decision on what `random` means; other dialects'
clamped substring), the area-11 leads (other dialects' writes), the area-10 leads (Trilogy `int` width, a date with a zone,
precision), the area-9 leads (dialects with no engine in CI), the area-5 leads
(decimal products, the persisted-copy leads), the area-1, area-2 and area-4
leads below, or the area-3 leads left.**

### 12. Sharing (done in `Sharing.lean`; leads left)

`tests/core/test_bound_operand_laws.py` measures how many times each cast
and clamped substring names its operand on DuckDB, SQLite and ClickHouse (by
rendering two operands of different lengths), holds three nested casts to
linear size, a random operand to one draw (its float text is `repr` of one
value), SQLite's compiled program to linear in its SQL (the flattener guard),
and runs the SQLite reads through `/usr/bin/python3` when its SQLite is
before 3.46 (Ubuntu 24.04). Leads left:
- **CI does not run the SQLite 3.45 check.** The laws job's ubuntu-26.04
  image has a newer system SQLite, so `test_sqlite_reads_parse_before_3_46`
  skips there; it runs on an Ubuntu 24.04 machine. A job with an old
  SQLite (a pinned runner image or a built amalgamation) would hold it.
- **A `random` concept is drawn once per reference (wrong answers).** With
  `auto x <- random(1000000) / 7.0;`, `select id, x, cast(x as string)`
  renders `random()` twice and the columns disagree on every row on DuckDB
  and SQLite; `select id, x where x > 0.5` filters on one draw and returns
  another, so rows with `x < 0.5` come back; `select x, count(id)` treats
  `x` as a keyless constant (one row). ClickHouse happens to agree because
  it computes identical expressions once. What `random` is (per row of the
  query's grain, or one value per query as its keylessness says) has to be
  decided first; either way a volatile concept must be computed once and
  read thereafter (materialized in a CTE, never inlined). The area-3 lead on
  `random(n)` (a fraction on DuckDB, an integer below `n` on SQLite) is the
  same decision.
- **A column-free volatile operand on SQLite** (`random(100)::string`) is
  still inlined and drawn per use: SQLite evaluates a subquery with no
  column once per query, so binding it would make it one value for every
  row. It resolves with the lead above.
- **DuckDB** names the operand twice in a number or date read and four
  times in an instant read or write (guard, zone test, two `TRY_CAST`
  branches), so nested instant reads grow 4^n in text; DuckDB computes the
  repeats once, so only the SQL grows. A `TIMESTAMPTZ` cast of the text
  with `'Z'` appended when it has no zone would make it two.
- **Other dialects' clamped substring** names a computed start up to 19
  times (Postgres, MySQL, BigQuery, Snowflake, Presto, SQL Server; area 7).
  None has an engine in CI; Postgres and Presto could bind with a lateral
  subquery or a lambda once checked on an engine.
- **SQLite 3.45 and three nested instant reads.** Each bound read is five
  subquery levels; two nested reads parse on 3.45, three still overflow its
  parser stack. Fewer levels (inlining the month into the day level) would
  raise it.
- **SQLite read cost.** Bound, a datetime read is ~10 µs a row (1.1 s per
  100k rows), most of it the date check and the zone GLOBs; a number read
  ~8 µs. Unbound it was 136 µs.
- **ClickHouse number reads** name the operand three times and the float
  write twice: ClickHouse computes the repeats once, and binding them with
  `arrayMap` doubled the number read's time (21 to 46 ms per 100k rows).

### 11. Writings (done in `Writings.lean`; leads left)

`tests/core/test_text_write_laws.py` holds a float column (short and
17-digit values, infinities, `-0.0`, NaN), a `numeric(12,4)` column, its
product and rescale, `datetime` / `timestamp` reads of a string, literals and
the DuckDB session zone, cast to `string`, to `trilogy.core.casts`
(`float_to_text`, `decimal_to_text`, `instant_to_text`) on DuckDB, SQLite and
chdb, plus each text read back and a decimal quotient. Leads left:
- **Done in 0.3.476: SQLite's parser stack.** The instant read binds its
  parts once and parses on SQLite 3.45 (area 12).
- **SQLite's 16th and 17th digits.** Its printf is exact to 15 significant
  digits only, so a double needing 16 or 17 is written with a last digit
  that reads back as the value but is not `repr`'s
  (`'1462468587316101.3'` for `…101.2`; ~1% of random 64-bit doubles), and a
  subnormal is written with 15 digits (`'4.94065645841247e-324'`). Exact
  digits need exact arithmetic, which SQL there does not have.
- **Other dialects** still render a bare CAST to a string: Postgres,
  BigQuery, Snowflake, Presto, SQL Server and MySQL write floats, decimals
  and timestamps with their own CAST, unchecked against `repr` here. None has
  an engine in CI; pin a rendering per dialect once each is probed.
- **Declared decimal types.** `round(n, 2)` and `avg(n)` of a
  `numeric(12,4)` are declared `numeric(12,4)`, so their text has four
  places on every engine, though DuckDB computes DECIMAL(12,2) and DOUBLE.
  Declaring `round`'s scale as its argument (when a literal) would make the
  text `'1.23'`; it changes a persisted copy's column type too (`Types.lean`).
- **A date, an integer, a bool** already agree on every engine CI runs; a
  date before year 1000 and a negative year are not checked.
- **A decimal past 15 digits on SQLite** is a REAL, so its text holds 15
  significant digits; a `numeric(18, 4)` past that is approximate there.
- **Implicit writes.** `concat` refuses a number (it must be cast), and
  persisting a float into a string column, or a CSV export, goes through the
  driver, not this rendering.
- **SQL size.** Done in 0.3.476: every SQLite and ClickHouse cast names
  its operand once or a fixed few times (area 12). Each cast still carries
  ~1.5-5 kB of its own text; a model that casts in many places pays it each
  time.

### 10. Instants and ranges (done in `Instants.lean`, `Readings.lean`; leads left)

`tests/core/test_instant_read_laws.py` holds a string column cast to
`datetime` and `timestamp` and passed to `datetime(s)`, `timestamp(s)` and
`date(s)`, `is not null` over the reads, a comparison with a whole second,
the order of reads, a persisted copy, a bound constant, literal folds and the
DuckDB session zone, plus integer and decimal reads at their range's edges,
to `trilogy.core.casts` on DuckDB, SQLite and chdb;
`tests/dialect/test_text_read_renderings.py` pins MySQL. Leads left:
- **The width of a Trilogy `int`.** It is INTEGER (32-bit) on DuckDB and
  64-bit on SQLite and ClickHouse, so `'2147483648'::int` is NULL on DuckDB
  only, and `2147483647 + 1` raises there (area 8). The suite holds each
  engine to its own width; one width (BIGINT on DuckDB) would make `int` mean
  one thing, at the cost of every DuckDB DDL and literal type.
- **Precision.** DuckDB keeps microseconds, ClickHouse's `DateTime64(3)`
  milliseconds, SQLite's `datetime()` whole seconds; each truncates. A
  persisted copy of a microsecond instant from DuckDB into ClickHouse loses
  digits. `DateTime64(6)` would match DuckDB; SQLite would need
  `strftime('%Y-%m-%d %H:%M:%f')` everywhere `datetime()` is rendered, or
  text comparisons break (`bound_text_is_not_datetime_text`).
- **A date with a zone.** `'2020-01-05 23:00:00-05:00'::date` is NULL (the
  date pattern takes no zone), though its UTC instant is on January 6th;
  `date(datetime(s))` reads it. Reading the UTC day would make `date(s)`
  depend on the zone; reading the local day would make it differ from
  `date(datetime(s))`. Either is a choice to state first.
- **ClickHouse's server zone.** `DateTime64(3)` with no zone displays in
  the server's zone; the read parses in UTC, and chdb's server is UTC, so the
  suite cannot see a server in another zone. `timestamp` is
  `DateTime64(3, 'UTC')` and is exact.
- **MySQL instants.** A zone is left unread (NULL): CAST reads only
  `+HH:MM`, into the session's zone. A read applying the offset as SQLite's
  does would close it, once it runs on an engine. Postgres, BigQuery,
  Presto, Snowflake and SQL Server still render a bare CAST (area 9).
- **Ranges beyond Decimal128.** ClickHouse reads a numeral through
  `Decimal128(18)`, so one with more than 20 whole digits is NULL even for a
  `numeric(38, 0)`; SQLite reads a fraction or exponent through a REAL, so a
  value within 1024 of 2^63 is checked approximately.
- **Year edges.** The pattern takes `0000` to `9999`; a zone can carry
  `9999-12-31 23:00:00-05:00` past year 9999 (Python refuses it, as the
  reference does) and ClickHouse's `DateTime64` holds 1900-2299 only. The
  suite stays in 1971-2100.

### 9. String readings (done in `Readings.lean`; leads left)

`tests/core/test_text_read_laws.py` holds a string column cast to `int`,
`numeric(12,s)`, `float`, `date` and `bool`, `is null` / `is not null` over
the casts, a persisted copy, membership in a set of reads, membership
identity over nullable and NULL-free sets, and literal folds to
`trilogy.core.casts` on DuckDB, SQLite and chdb;
`tests/dialect/test_text_read_renderings.py` pins MySQL. Leads left:
- **Dialects with no engine in CI.** Postgres, BigQuery, Presto, Snowflake
  and SQL Server still render a bare CAST of a string: they raise on a string
  with no reading (not NULL), and Postgres, BigQuery, Presto and SQL Server
  raise on `'1.5'::int` where the reference rounds. Loud, not wrong; each
  needs its regex function (`~`, `REGEXP_CONTAINS`, `regexp_like`,
  `REGEXP_LIKE`; SQL Server has none, `TRY_CAST(x AS FLOAT)` is close) and
  an integer read through a decimal. MySQL's rendering is pinned but not run.
- **Done in 0.3.474: overflow.** A numeral past the target's range is NULL
  on every engine CI runs and on MySQL (area 10); `'1e400'::float` is an
  infinity on each.
- **Long fractions on SQLite.** SQLite reads a fraction through a REAL, so
  a tie not exact in binary (`'2.675'::numeric(12,2)`) and a fraction past
  double precision (`'0.49999999999999999'::int`) round by the binary value;
  the suite holds SQLite to binary-exact ties, as `Casts` does.
- **Done in 0.3.474: datetimes** (area 10). A date with a time of day
  still reads its day only, and a date with a zone is NULL.
- **ClickHouse dates before 1970.** The read goes through `Date32` and
  casts to `Date`, whose range is 1970-2149; the suite stays inside it.
  DuckDB also reads two- and five-digit years, `24:00:00`, zones and `BC`,
  which the reference refuses.
- **NaN.** SQLite has no NaN, so `'nan'::float` is NULL there; DuckDB
  orders NaN above every number (`nan > 0` is TRUE) and ClickHouse does not.
  MySQL has no infinity, so its float read takes numerals only.
- **SQL size.** The DuckDB guard is ~360 characters and is repeated at every
  inlined read; a model that binds a typed column to a `string` concept pays
  it for nothing. Declaring the binding's real type avoids it.
- **Unicode digits and whitespace.** The pattern's digits are ASCII and its
  whitespace POSIX `[[:space:]]` (which includes `\v` and `\f`, which the
  Python reference does not strip); DuckDB's TRY_CAST of `'١٢'` is NULL as
  the reference is, other engines unchecked.

### 8. Casts and folded constants (done in `Casts.lean`; leads left)

`tests/core/test_cast_laws.py` holds decimal rescaling, string / number /
bool casts, `sqrt` and `log` domains, folded NULL comparisons, a CASE with no
arm left and the string-literal cast folds to a Python reference on DuckDB,
SQLite and chdb; `tests/dialect/test_cast_renderings.py` pins the other
dialects. Leads left:
- **Done in 0.3.473: a string with no reading** is NULL on every engine CI
  runs and on MySQL, and a cast from a string is NULL-introducing (area 9).
- **Done in 0.3.475: a float or a decimal as a string** is written one way
  on every engine CI runs (area 11).
- **Overflow.** `2147483647 + 1` raises on DuckDB (an INTEGER literal), and
  a float cast to an integer out of range raises on DuckDB, saturates on
  SQLite and wraps on ClickHouse.
- **A float tie not exact in binary** (`2.675::numeric(12,2)`): DuckDB and
  ClickHouse round the shortest decimal (`2.68`), SQLite the binary value
  (`2.67`). The literal `2.675` is a decimal on DuckDB and a double
  elsewhere, so `round(2.675, 2)` splits the same way.
- **Snowflake `bool`** as a cast target name is unchecked (BOOLEAN is the
  documented name). BigQuery refuses a FLOAT64 cast to BOOL.
- **SQLite has no `stddev_samp`, `var_samp`, `any_value`, `array_agg`,
  `md5`/`sha256`** and `date_diff(..., quarter)`; each raises there.
  ClickHouse's `groupArray` of a group with no non-NULL value is `[]`, not
  NULL as on DuckDB.

### 7. String functions (done in `Strings.lean`; leads left)

`tests/core/test_string_function_laws.py` holds `len`, `substring` (literal and
computed positions, zero and negative ones included), `strpos`, `upper` /
`lower`, `trim`, `replace`, `regexp_replace`, `regexp_extract` and a persisted
copy to a Python reference on DuckDB, SQLite and chdb;
`tests/dialect/test_string_function_renderings.py` pins the other dialects and
runs the clamped substring on DuckDB. Leads left:
- **SQLite has no `regexp_*` or `split`.** They raise there. The executor
  could register Python functions on its connection, but SQL written for
  another SQLite would still lack them.
- **SQLite's `upper` and `lower` are ASCII-only**; the case law runs ASCII
  there. Full case mappings differ too: `upper('ß')` is `ẞ` on DuckDB and
  `SS` on ClickHouse, and `lower('İ')` is one character or two.
- **A computed substring position is repeated.** The clamped form names
  `start` up to eight times; a long expression (a `strpos(...) + 1`) makes the
  SQL long, though engines evaluate it once. A dialect whose own reading of a
  negative start matches (BigQuery, Snowflake, MySQL, Presto, ClickHouse all
  count from the end) could skip that part of the clamp, once each is checked
  on an engine.
- **`regexp_*` on dialects with no engine in CI.** Postgres, MySQL, SQL
  Server and Snowflake render `REGEXP_CONTAINS` / `REGEXP_EXTRACT`, which they
  do not have; BigQuery's `REGEXP_EXTRACT` drops the group argument and
  returns NULL, not `''`, on no match. Presto and Trino `trim` all whitespace.
- **Collation** (area 6) applies here too: MySQL's `LOCATE` and SQL Server's
  `CHARINDEX` fold case under the default collation.

### 6. String matching (done in `StringMatch.lean`; leads left)

`tests/core/test_string_match_laws.py` holds `like`, `not like`, `ilike`,
`not ilike` over literal and computed patterns, `contains`, NULL operands, the
equality planning of an exact pattern, string literals and a persisted copy to
`trilogy.core.like_patterns` on DuckDB, SQLite and chdb;
`tests/dialect/test_string_match_renderings.py` pins the other dialects.
Leads left:
- **Collation.** MySQL's and SQL Server's default collations are
  case-insensitive, so `like` and `=` both fold case there, and SQL Server's
  `=` ignores trailing spaces while its LIKE does not. Neither has an engine
  in CI; a `COLLATE` (or `BINARY`) rendering would hold them to the law, and
  would change `=` too.
- **Unicode case.** `ilike` and `contains` fold with the engine's `lower`:
  ASCII only on SQLite, Unicode on DuckDB and Postgres, and BigQuery's
  `CONTAINS_SUBSTR` also NFKC-normalizes. The suite's alphabet is ASCII.
- **Pattern edges left to the engine.** A computed pattern ending in a lone
  backslash (DuckDB and Postgres raise), a computed pattern holding `char(1)`
  to `char(3)` on SQLite (the `replace` chain parks escapes there), and `[` in
  a computed pattern on SQL Server (a character class).
- **The Trilogy literal grammar.** A string literal cannot end in a backslash,
  and `\'` keeps its backslash in the value; `contains(v, '\')` does not
  parse.
- **More planning from patterns.** Done in 0.3.471: `like '%'` is
  `is not null`, and the strip reads a sibling's proof. Left: a prefix pattern
  (`'Bos%'`) implies a range a partition or a value pin could read.

### 5. ClickHouse (chdb in CI; `Padding.lean` done, the function, scalar and type suites run there; leads left)

Every law suite with a DuckDB/SQLite harness that runs a typed fixture now
runs on chdb. Leads left, probed on chdb 26.9:
- Done in 0.3.469: `7 / 0`, `0 / 0` and `7 % 0` are NULL here too, and a
  persisted copy declares `Nullable` the columns its write can hold NULL in.
  Leads left from that round:
  - `written_nullable` reads every CTE's nullable set, so a column a later
    CTE filters `is not null` is still declared `?` in the copy. That is
    sound but keeps a filter on reads of the copy that a precise answer
    would strip.
  - A persist into a datasource the model declares keeps the model's
    bindings: a declared non-null column the select can write NULL into is
    not checked at write time. Validation reports it after the fact.
  - `x / y` with a constant divisor that is not a literal (`x / (4 / 2)`)
    is nullable; folding constants first would keep it non-null.
- A product of decimals is `Decimal(18, s1 + s2)` whatever the operands'
  precision, so `n * m` of two `numeric(12,4)` raises `Decimal math overflow`
  above 18 digits although the declared type is `numeric(24,8)`. It is loud,
  not wrong; a cast to the declared type first would fix it.
- `round(f, n)` with `n > 0` is `sign(x) * floor(abs(x) * 10^n + 0.5) / 10^n`
  on a binary float, as DuckDB computes it; a tie that is not exact in binary
  (`round(1.005, 2)`) is not held to a decimal reference anywhere.
- `%` of a float or a decimal on Postgres, BigQuery and the other dialects
  with no engine in CI is not checked (Postgres has no `double % int`).
- `lag`/`lead` of an array operand would fail `toNullable`; no suite covers it.

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
- Done in 0.3.486: a WHERE over a stored group value at a dimension's
  column keyed on the grain (`tree_form`) reads the summary
  (`Rollup.group_value_filter_joined_exact`). It still declines to the base
  rows for a CASE over `tree_form` (`form_case`: the gate and the pin accept
  only a lineage-free property), and for a grand total under an OR that
  joins `tree_form`, whose request carries `tree_form` beside a total `v`
  (`GROUP_VALUE_GRAINS` in the harness).
- Done in 0.3.487: a WHERE over an aggregate at a grain COARSER than the
  summary's (`sum(height) by city > 6` beside a `(city, species)` summary)
  reads the summary (`Rollup.coarse_group_value_filter_exact`): the joined
  plan computes the gate in its own group and joins it back on `city`. Left:
  - a row column beside the gate that the target does not group by
    (`... and species != 'Tilia'` at `tree_form`; `... or tree_form =
    'linden'` at `city`) adds the column to the pinned request, whose
    aggregates are then not at its grain; the search that takes over
    re-aggregates the gate at the widened grain (wrong rows). A guard on the
    bridge emitter (`_local_concept_nodes_for_datasource` emitting a rolled
    aggregate only where the bridge grain holds its grain) turned that into
    a planning error, not a plan, so it was dropped;
    `_coarse_gate_beside_row_columns` declines these shapes;
  - a gate at a grain the summary's grain does not hold (`sum(height) by
    tree_form > 6`) reads the trees. Relaxing `_keyed_aggregate` and
    `_coarse_gate` to any grain the summary reaches (a property keyed in its
    grain) gave WRONG rows in 0.3.488's probe: `_plan_joined_filter_rollup`
    grouped the gate by `species`, the key `tree_form` is a property of, not
    by `tree_form` (lesson 17's substitution). The joined plan must group a
    gate by the dimension column itself before this can be served;
  - the gate's value is summed from the summary when it stores the
    argument (`sum`, `count`), from the trees otherwise (`avg`,
    `count_distinct`); the `by *` total gate always reads the trees (a
    condition-only aggregate, which the rollup branch of
    `_root_derived_concepts` never marks).
- Done in 0.3.488: a re-sum beside the rows holding its partials is joined
  on the group key, never on the partial (`Rollup.partial_is_not_a_key`; the
  join on `v` lost every group whose partials differ from their total once a
  WHERE stopped the dedup that folded the re-sum away); a gate on a re-sum of
  a stored value (`sum(sum(height) by city, species) by city > 6`) plans at
  `tree_form` (a hidden join handle is unhidden when carried); a coarse gate
  at the summary's own grain reads the summary
  (`Rollup.coarse_gate_at_summary_grain`). Left:
  - `select city, sum(height) as v having sum(height) by city > 6` raises
    "Missing source reference to local.height" with the summary in scope:
    the HAVING renders the gate from `height`, which the summary scan does
    not carry. `having sum(height) > 6` and `having v > 6` plan;
  - `count(tree_id)` beside `sum(height) by *` reads the base rows
    (`MIXED_GRAINS`, `test_resum_beside_other_grain_keeps_every_group`).
- Done in 0.3.489: a gate at a dimension's column (`sum(height) by
  tree_form > 6`, `sum(height) by city, tree_form > 4`) reads the summary
  (`Rollup.dimension_gate_exact`): the gate is grouped by its own grain and
  joined back on all of it, onto summary rows that already carry the
  dimension column. A HAVING over an output's own aggregate (`having
  sum(height) by city > 6` beside `sum(height) as v`) reads `v`. An
  aggregate member of an IN list (`count(x) by k in (1)`) keeps its grain
  (`SelectGrain.membership_pin_misreads`; wrong rows on main with or without
  a summary). Probed and correct, left as leads:
  - `select sum(height) where sum(sum(height) by city, species) by tree_form
    > 6` reads the trees at the grand total, and at `city` reads the summary
    and the trees (`form_nested` in the harness). The same gate by `city` is
    served at every grain;
  - a HAVING gate at another grain beside a `count`/`max` target (`select
    city, tree_form, count(tree_id) having sum(height) by city > 8`, `select
    city, max(height) having sum(height) by * > 10`) reads the trees, where
    a `sum` target is served;
  - `select city, species, sum(height) by city as v` (a coarser aggregate
    projected at the summary's grain) and `select city, sum(height),
    sum(height) by species` read the trees;
  - `agg(x ? <group-value gate>)` (`sum(height ? sum(height) by city > 6)`)
    reads the trees;
  - `select city, sum(height) where sum(height) by city > 6` renders the
    gate as a grouped CTE with a HAVING, joined back and filtered again; at
    the target's own grain it is one HAVING.
- Done in 0.3.490, from a random sweep (1,800 gate shapes) against an
  independent SQL oracle, not the without-summary plan:
  - a property selected beside its key comes with the key
    (`SelectGrain.property_without_its_key_misjoins`; wrong rows on main
    with no summary and no WHERE: `select city, species, tree_form,
    max(height) by city, tree_form, count(tree_id)`). The FINAL cover
    (`elect_cover`) prefers a candidate carrying the selected key;
  - a gate beside a key and its dimension column (`select species,
    tree_form ... where sum(height) by city > 8`) reads the summary; the
    search had re-summed the gate per species
    (`Rollup.gate_resummed_at_target_misfilters`);
  - a total gate under an OR beside a row column or a keyed gate reads the
    trees (`total_gate_beside_rows`; with a keyed gate it raised, and
    otherwise the stored `max(height)` was read as each target group's
    total: `city = 'B' or max(height) by * > 6` dropped city A on main);
  - a dimension gate beside an unstored gate at the summary's grain reads
    the trees (`_unsourced_beside_dimension_gate`; it double counted).
  Left:
  - the search path a summary takes when the joined plan declines still
    re-sums a coarse rollable gate at the target's grain, and joins a gate
    keyed on a dimension column onto summary keys without the column. Every
    shape that reaches it is now guarded off the summary
    (`_coarse_gate_beside_row_columns`, `total_gate_beside_rows`,
    `_unsourced_beside_dimension_gate`); the root fix is the bridge emitting
    a rolled gate only at its own grain (0.3.487 tried it and got planning
    errors). Forcing `_finer_filter_rollup_source` to return None reproduces
    it on `select species, count(tree_id) where sum(height) by city > 8`;
  - the guarded shapes read the trees: a total gate under an OR could be
    computed once and cross-joined onto the summary rows, and an `avg` gate at the summary's
    grain could be read from the trees beside summary-served targets;
  - `select species, tree_form, max(height) by city, tree_form, count(tree_id)`
    (a `by` key outside the select) returns rows at `(species, city)`;
    check that against `SelectGrain` before calling it a bug.
- Done in 0.3.491, from a wider oracle sweep (8,000 selects: projected,
  nested, `by` and `?` aggregates; literal memberships; bare top-level
  and/or; HAVING gates), committed as 600 seeds in
  `tests/discovery/test_oracle_sweep_laws.py` (see "What it found"): a
  literal membership is a comparison (`_reads_a_set`,
  `_literal_membership_on_absent`); a key comes off a select-phase group,
  never the WHERE gate's when a row atom misses the gate's (`elect_cover`); a `by` grain holding a property
  beside its key's grain is joined through a keying bridge
  (`elect_bridges`, `_underkeyed_blends`); a WHERE row argument co-sources
  with every root bound beside it (`where_row_arg`); `agg(x ? c)` at a
  summary's own grain reads the stored partial (`get_alias`); a HAVING `by`
  a grain the select grain determines is accepted; a sole FINAL
  contributor re-aggregating a gated group does not re-apply the gate
  (`_atoms_of_groups_aggregated`). Left (each fails on main too):
  - `select city, species, tree_form, avg(height) by * as v0, ... where
    ((sum(height) by city, species < 6 or tree_form is null) and (species =
    'Acer' and species in ('Acer', 'Ulmus')))` reads `v0` over every row
    (4.25 for 3.0), with or without a summary;
  - `select sum(height ? city in ('B')), count(min(height) by tree_form)`
    raises "Missing source reference to local.city" with the summary in
    scope;
  - two keyless joins: `select species, min(height) by tree_form where
    ((tree_form = 'broadleaf' or count(tree_id) by city, species = 1) and
    species = 'Acer')`, and with the summary `select city, species,
    bool_or(tall), sum(height) where (sum(height) by tree_form > 5 or
    sum(height) by city = 1) or (species in ('Acer', 'Ulmus') and species !=
    'Tilia')` (sweep seeds 7162 and 6745);
  - a total beside a summary-served aggregate now reads the trees (both
    roots on one scan); the summary could serve it with the WHERE applied to
    the trees scan as well.
- Done in 0.3.492, from a 20,000-seed run of the same generator (see "What
  it found"): an atom over a row column and a finer gate filters the rows
  the aggregates read, never a peeled dimension's semijoin
  (`SelectGrain.mixed_atom_after_count_misreads`); a join pair is implied
  only on its own side (`SelectGrain.pair_implied_off_its_side_misjoins`);
  a dimension's column filters every scan of its key
  (`Rollup.dimension_column_filters_every_scan`); a filtered partial's scan
  carries its condition column; a gate's twin is never a column of a stored
  partial's rows; an atom over a grain key is decided before the aggregate in
  the join plan; a regrouped branch's outputs need no FINAL re-apply. Left:
  - `select min(height), sum(height) by * where not (sum(height) by city,
    tree_form = 1) and (city = 'B' and city = 'B')` raises "Could not find
    CTE for datasource ...filtered_by..." with or without a summary (sweep
    seed 2537); the same WHERE with one `city = 'B'` plans;
  - `select city, bool_and(tall) where ((city = 'B' and max(height) by city >
    10) or (min(height) by city, tree_form in (4, 5) or avg(height) by city >
    10)) having max(height) by city < 10` reads the trees with the summary in
    scope (`_dimension_gate_beside_another_grain`); main read the summary
    and was right. The guard's root is the search path's FINAL join of a
    `(city, tree_form)` gate onto summary rows without `tree_form`: the root
    fix is joining the dimension onto the summary rows when a FINAL
    re-applies a gate keyed on its column;
  - the summary-path guards (`_coarse_gate_beside_row_columns`,
    `total_gate_beside_rows`, `_unsourced_beside_dimension_gate`,
    `_dimension_gate_beside_another_grain`, `_unsourced_beside_rolled_gate`)
    are five shapes of one gap: the search that takes over when the joined
    plan declines. Making `_plan_joined_filter_rollup` source an unstored
    gate from the trees (instead of declining) would retire most of them.
- Done in 0.3.493, from a second generator family
  (`tests/discovery/test_oracle_spellings_laws.py`: keys-only and `tree_id`
  selects, `>=`/`<=`/`!=`, a gate `is null`, two gates compared or
  subtracted, `between`, `coalesce`, a gate inside `agg(x ? c)`, a compound
  HAVING): keys split over two filtered branches pair only kept tuples
  (`SelectGrain.split_keys_pair_unkept`); a gate is never filtered on a
  summary partial off its own grain
  (`Rollup.gate_partial_resummed_off_its_grain`); a `by` value of keys the
  select's bindings determine is no grain key
  (`SelectGrain.by_value_is_no_grain_key`, `by_value_as_key_misjoins`); an
  extra FILTER side is joined as built; `c and c` folds to `c`
  (`SelectGrain.conjunction_idempotent`, seed 2537). Left:
  - seed 346: `select species, count(tree_id ? sum(height) by tree_form is
    not null) where count(tree_id) by species < 2` drops the WHERE. The gate
    is placed only on the `tree_form` group the filter's condition reads;
    the filtered count's own rows (the root beside it) skip it. Hosting the
    atom on every group below a host that also reads unfiltered rows fixed
    it but broke seeds 96 and 208 (both right on main), so it was dropped;
  - seed 129: `having max(height) by city, species = 6` beside `city as v0`
    in a `tree_id` select is read as `by species, v0` and rejected;
  - the HAVING rejections on `tree_id` selects generally (`having
    min(height) by city <= 10`): `city` is a key, not a property of
    `tree_id`, so `_grain_determined` does not see the binding FD
    (`minimize_by_bindings` now does, for a `by` in the select);
  - a filtered aggregate with a gate in its condition beside a WHERE gate
    comparing two grains still raises "Missing source reference" or "global
    (by *) aggregate ... rendered at keyed grain" (both on main too).
- Done in 0.3.494, from 10,000 seeds of the spellings family (127 wrong rows
  on the round's base, 5 after): a WHERE hosts every aggregate whose
  aggregated rows did not pass through its host, not only the group a
  filtered aggregate's condition reads (`SelectGrain.gate_host_skips_select_rows`,
  `_hosts_reading_unfiltered_rows`; seed 346); an aggregate paired with itself
  beside its whole grain is decided by the grain's pairs
  (`SelectGrain.value_pair_drops_null_group`, `_aggregate_beside_its_grain`);
  a cross-grain atom over a row column hosts on the row stream that produces
  the column; the empty-total guard is conjoined to a parenthesized HAVING
  (`SelectGrain.guard_precedence_emits_empty_total`). Left:
  - seed 1117: `select city, species, tree_form, max(height), max(height ?
    not (...)), sum(height ? count(tree_id) by tree_form != 1) where
    sum(height) by species is null` returns every key: FINAL's cover
    (`elect_cover`) takes the keys off a `?` filter's row stream (the most
    downstream exposer), which the WHERE never filtered. Hosting the atom on
    FILTER groups fixes it but breaks seeds 96 and 208 again (as in 0.3.493);
    the fix belongs in the cover, preferring a key source whose rows passed a
    host;
  - seed 8920: `select tree_id where (sum(height) by city - max(height) by
    tree_form > 2 and height is not null) having max(height) by tree_form >=
    1` returns nothing: the WHERE's `max(height) by tree_form` is read off the
    HAVING gate's twin, grouped over the kept rows with the HAVING applied;
  - seed 920 (summary only): `select tree_id, species, sum(height) by species
    as v0 where count_distinct(tag) by * - count(tree_id) by city, species <
    1` returns `b1`;
  - 119 raises remain; the most common are "Could not resolve connections"
    on `tree_id` selects (12), HAVING `by` rejections on `tree_id` selects
    (32, seed 129's family) and "Missing source reference to local.height" (6).
- Done in 0.3.495, from seeds 10,000-20,000 of the spellings family (on
  main: 14 wrong, 124 raising, of 20,000 queries; after: 7 wrong, 30
  raising; 101 fixed, none broken; the first family's seeds 0-4,000 return
  the same rows and summary reads, with 22 more JOINs of 16,214): a HAVING `by` keys the select's rows bind (`count(id)
  by city` beside `tree_id`, or `by v0` once finalize renames `city as v0`) is
  coarser than the select (`SelectGrain.having_by_bound_key_is_coarser`;
  `_grain_determined` reads the binding closure and resolves a pure rename;
  seed 129's family, about 100 raises, and seed 8920); a group hosting a WHERE
  atom that can hold on an absent group reads its aggregates off an
  unfiltered sibling, never off another host of the atom (`SelectGrain.gate_read_past_its_host_passes_absent`,
  `_gate_args_filtered_here`); a value read with the rows at a grain they
  determine is projected with the rows (`value_bucket_pairs_short_of_its_grain`,
  `_wrap_for_grain`). Left:
  - seeds 10132, 11682 and 19186: a negated (or `is null`) gate re-applied at
    FINAL passes the rows a dimension projection pads in. `select species,
    tree_form, max(height) where min(height) by city, tree_form is null`
    returns a species with no trees at all once the enrichment lists one,
    which the select without the WHERE does not: the FINAL's fresh root
    projection (`_resourced_root_contributor`) reads the enrichment alone, and
    its join to the gate's group on `tree_form` is RIGHT then FULL
    (`ensure_content_preservation` after the LEFT join of the gate's group);
  - seed 19236: `select tree_id, species, sum(height ? count_distinct(tag) by
    species < 0), min(height) by city, tree_form as v1, city as v2 where ...`
    pairs `v1` on `city` alone: the row contributor is the BASIC group
    computing `v2`, and the join plan's carry demands (`root_carry_demand`)
    are only read for ROOT contributors, so `tree_form` is never carried;
  - seed 12917 (summary only) duplicates a group under `having v1 = 5 or
    city = 'A'`; seed 1117 and seed 920 as in 0.3.494;
  - 32 `tree_id` selects with a HAVING gate no longer read the summary (17
    others now do): the HAVING value is promoted beside the rows instead of
    semijoined, and the promoted value is sourced from the trees;
  - 30 raises remain: "Could not resolve connections" on `tree_id` selects
    (16) and keyless joins (10) are the most common. Seed 18676's keyless join
    (on main too) planned when the input rerouting also covered null-rejecting
    atoms; narrowed to atoms that hold on an absent group, it raises again,
    and the narrowing saved the JOINs that rerouting cost elsewhere.
- Done in 0.3.496, from seeds 20,000-30,000 of the spellings family (on main:
  11 wrong and 14 raising of 20,000 queries; after: none wrong and 12 raising,
  nothing new broken; the first family's seeds 0-4,000 stay clean): the
  select's keys come off one group holding their tuples when no cover winner
  holds them all (`SelectGrain.keys_off_two_groups_pair_unheld`,
  `final_cover._keys_off_one_tuple`); a ROOT side holding the key of a FINAL
  gate that can hold on an absent group carries it, rather than a second scan
  (`SelectGrain.dimension_side_pads_gate`, `_final_extra_sides`; seeds 10132,
  11682, 24647, 28671); the input reroute of 0.3.495 asks per aggregate
  argument whether NULL lets the atom hold (`args_holding_on_absent`;
  `conjunct_read_past_its_host_passes_absent`, seed 22018); the cover prefers
  a key source whose rows passed every such atom, a one-row total aside
  (`key_off_unfiltered_stream_returns_rejected`, seed 19186); a keying bridge
  reaches a stored key through a dedup whose outputs determine it
  (`dedup_carries_determined`, `_widen_determined_dedup`, seed 19236). Left:
  - seed 1117: the cover now names the hosting aggregate for `city, species`,
    but `tree_form` (keyed by the selected `species`) still comes off the `?`
    filter's unfiltered row stream, and the FINAL renders the keys off that
    stream and joins the aggregates onto it preserving. The atom is placed on
    every aggregate, so `_uncovered_exposing_output_contributor` (which also
    counts every descendant of a host as covered, lesson 32) never runs for
    it; narrowing its `covered` to `_rows_filtered_by` changed nothing here;
  - seed 12917 (summary only), seed 18676 (keyless join, on main too);
  - first family seed 2717 with the summary raises "Missing source reference
    to local.height" after about 640 other queries in the same process, and
    plans alone: state leaks between statements (on main too; run the shapes
    of `changed.json`-style batches in order to reproduce);
  - 12 raises remain on seeds 20,000-30,000: "Could not resolve connections"
    on `tree_id` selects (8) and keyless joins (3) are most of them;
  - the cost: JOINs 55,193 to 55,296 on the spellings seeds and 16,237 to
    16,282 on the first family's; summary reads 3,273 to 3,274 (4 lost, 5
    gained) and 1,741 to 1,742 (1 lost, 2 gained).
- Done in 0.3.497, from seeds 30,000-40,000 of the spellings family (on
  main: 9 wrong, one invalid SQL statement and 33 raising, of 20,000 queries;
  the first family's seeds 0-4,000 stay clean): a gate the FINAL re-applies pairs on its
  whole grain, so a side holding one of its keys is not folded into a sibling
  without it (`SelectGrain.gate_paired_short_of_its_grain`,
  `_final_gate_keys`; seeds 35845, 31847); a COUNT read past a host reads 0,
  not NULL (`count_arg_past_host_passes_absent`; `args_holding_on_absent`
  includes it unless a literal comparison rejects 0; seed 39340); a WHERE
  argument's producer keyed off another producer is a lookup, and the cover
  reads no output off a group producing an argument of a FINAL atom that
  holds on an absent group (`lookup_preserved_passes_padding`,
  `MergeNode.lookups`; seed 31824); an atom over an aggregate is pushed into a
  group only when every row column it names is grouped (seed 35775, invalid
  SQL with the summary). Left:
  - seed 30670 (and 1117): `select city, species, tree_form, min(height) by
    city, tree_form, sum(height ? ...) where (... and avg(height) by city,
    tree_form is null)` takes `tree_form` off the `?` filter's unfiltered row
    stream. The host by `(city, species)` determines `tree_form` but never
    carries it, and nothing re-applies the WHERE at FINAL. The fix is to
    carry a looked-up property on a filtered host (a scan beneath it binds
    it), not to choose among unfiltered sources;
  - seeds 12917 (summary only) and 18676 as before; first family 2717 and
    spellings 7061 raise only in a batch (state leaks between statements);
  - the cost, over spellings seeds 0-40,000: JOINs 220,528 to 221,483,
    summary reads 12,912 to 12,904 (8 lost, none gained: 6 to the cover's
    condition-group preference, 3 to the count reroute, one shared), raises
    94 to 94 (seeds 18300 and 36581 swap, each raising only in a batch); the
    first family's seeds 0-4,000: JOINs 16,282 to 16,327, summary reads 1,742
    to 1,741 (seed 2385).
- Done in 0.3.498, from seeds 40,000-60,000 of the spellings family (on
  main: no wrong rows and 51 raising of 40,000 queries; after: 5 raising,
  nothing new wrong; seeds 0-40,000: 78 raising to 9, the same 5 wrong, 24
  more summary reads, JOINs +0.14%; the first family's seeds 0-4,000 stay
  clean): a joined source plan carries a requested constant
  inline (`SelectGrain.constant_key_pairs_every_row`; a `by *` total beside a
  gate keyed on `tree_form` raised "Could not resolve connections"), and a
  bridge that is some contributor's only planned partner is joined even when
  another bridge exposes the same column (`keyless_pairing_fans_out`,
  `_sole_partners`; "keyless join"). Left:
  - seeds 42921 and 59635: the total beside a row atom (`city = 'B' and
    ...`) still raises, since the constant is carried only under conditions
    over aggregates. The fix is the padded stored-column filter itself;
  - keyless joins on seeds 21266 and 33618 (summary only), not yet read;
  - seeds 30670 and 1117 (lesson 39), 12917 (summary only), 18676 (still a
    keyless join: nested aggregates by `city, species, tree_form` beside a
    `by *` total in an OR and a HAVING by `tree_form`);
  - the 5 raises left on seeds 40,000-60,000 all plan alone and raise only in
    a batch, with the summary ("Missing source reference to local.height" or
    `tree_id`, on `select city, tree_form where max(height) by city, species <
    10` and similar): the state leak between statements is now most of what
    the sweep reaches.
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
- **The statement's lineage no longer goes stale** (0.3.483): the
  `_reaggregate_partials` rewrite is gone. The election it made lives in
  `BuildEnvironment.partial_sources` and the group node re-aggregates the
  partial as a rollup (`QueryDatasource.partial_sources`); every copy of a
  concept keeps its authored lineage.
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
- The network search's dominance axis is `dominated_reads`: a source read
  in full beside an IMPLIED_EXACT twin with the same columns, or an
  IMPLIED_EXACT table beside a row-complete coarser source applying its arm
  (`Cost.filtered_dominated_le`, 0.3.485; the `lookup-*` shapes of the
  matrix reach it). It reads grains by binding, not the FD closure, like
  `source_dominance.dominates`; two rollup bindings at different grains with
  no arm still tie.
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

Done in `NullReject.lean` (0.3.469): `7 / 0` and `7 % 0` are NULL on every
dialect.

Done in `Scalars.lean` (0.3.447): the month-end clamp, `date_add`'s declared
type (a whole-day offset of a date is a date; a datetime keeps its type),
`greatest`/`least` over the present arguments, `log`'s base order, SQLite
`strpos`. Leads left:

| Lead | DuckDB | SQLite | Notes |
|---|---|---|---|
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
