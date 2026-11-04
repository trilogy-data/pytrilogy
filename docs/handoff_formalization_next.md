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
placement per engine, set operators; area 4).

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

## Candidate areas

**Suggested next: the area-1, area-2 and area-4 leads below, or area 3.**

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
order. It starts with a pre-existing wrong answer on main, a CASE over a
per-bucket aggregate that fans out.

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

### 4. Windows, ORDER BY and set operators (done in `Windows.lean`, `SetOps.lean`; leads left)

`tests/windows/test_window_laws.py` holds numbering windows, value windows
(`lag`/`lead`, aggregate windows with and without an order) and ORDER BY NULL
placement to a reference over random rows with NULLs and ties, on DuckDB and
SQLite. `tests/dialect/test_null_order_renderings.py` pins every dialect's
rendering. A plain direction places NULLs last everywhere (DuckDB's
placement); `nulls auto` is the engine's own. Leads left:
- **A select of an aggregate at a finer grain repeats tuples.** `select g,
  sum(x) by id as s` declares grain `{s, g}` but returns one row per `id`, so
  `(c, NULL)` comes back three times. The FINAL dedup skips a GroupNode
  contributor ("aggregates sit at their own grain"). Decide whether the
  select is the distinct tuples, as `select g, x` is.
- **A value window's keys are its operand.** `lag(x)` gets keys `{x}`, though
  two rows with the same `x` lag differently; the statement grain of `select
  lag(x) over (order by id)` is then `{x}` while the rows are per `id`. The
  law suite compares value windows as sets for this reason. The window's
  grain (`{id}`) is right; its keys are not.
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
