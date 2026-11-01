# Handoff: summary coverage, the next round (after PR #842)

Goal: the tree_reporting dashboards answer from the published count summaries
wherever that gives the same answer, and never answer differently from the
tree rows. PR #842 turned the #827 list into six rules in
`formal/Trilogy/Cover.lean`, plus a law suite that runs every rule. Read
`Cover.lean`'s module docstring first: it is the spec.

## The approach: rules first, shapes second

Don't fix the shape that was reported. Name the rule it falls under, prove the
rule, then generate every spelling of it. The reported shapes were symptoms.
`species as s`, `species as v, species as l`, `plant_year::int` and
`plant_date.year` were four bugs in three files, but one rule
(`Cover.target_by_value`: a target is any function of the key). The suite
then found spellings nobody had reported. One was a WHERE on `plant_year`
beside a select of `plant_date.year`, which crashed at render once the select
side was fixed.

Each round:

1. **State the rule in `Cover.lean`** over the existing `Rollup` vocabulary
   (`stored`, `keep`, `slice`, `groups`). A rule is sound when its key
   predicate or target is a function of the summary key. Then it reduces to
   `Rollup.count_coalesce` / `Rollup.reaggregate_partial_count` in a few lines.
   Also write the counterexample, closed by `decide`: the shape that looks
   similar and is NOT sound. `collapsed_fanout_double_counts` is the model
   (a species with two forest ecoregions counts twice per biome when you
   re-sum relation pairs). The counterexample becomes a guard case in the
   suite.
   Build: `cd formal && ~/.elan/bin/lake build` (keep it warning-free).
2. **Add the shapes to `tests/discovery/test_summary_cover_laws.py`** under
   that rule's dict. Each runs under all five `FILTERS` and must (a) answer
   as the tree rows do and (b) read the summary. A shape the rule does
   not cover goes in `UNSERVABLE` and must read the trees. Unfixed shapes go
   in `OPEN` as strict xfails with the rule in the reason. A fix makes
   them XPASS, and you delete the entry.
3. **Classify before fixing.** A miss is fine; a wrong answer is not. For a
   per-case MISS / WRONG / ERR table, use a probe that imports the test
   module and calls `_run(BASE, q)` then `_run(BASE + SUMMARY, q)`. Compare
   against main through `git archive origin/main trilogy` extracted to a
   scratch dir and put first on `sys.path`.
4. **Fix the invariant, not the call site.** Most fixes in #842 replaced an
   address comparison with a value comparison (`renamed_source`, canonical
   addresses), or closed a walk that stopped one level short
   (`get_derivable_concepts` seeded only from raw-column expressions).
   Grep for the sibling checks: `at_own_grain` had three callers and
   `finer_than_grain` made the same mistake.
5. **After every change**, run the cover suite, `test_rollup_resolution_laws.py`,
   `test_partition_family_laws.py` and `tests/modeling/tree_reporting`
   (about 9 minutes together). A tree_reporting served-from failure that
   says `summary` where the pin says `trees` is a flip: delete the pin in
   `expectations.py`, never edit it.

Gotchas:
- A summary's inline query embeds the tree SQL, so detect "reads the trees"
  by the quoted alias `"trees"`, not by the SQL text.
- The statement's lineage objects go stale after `_reaggregate_partials`
  (query_processor.py) rewrites concepts in the environment. A walk
  that doesn't `resolve` through `environment.concepts` sees the old
  lineage. That stale walk is currently what keeps the open K3 item below
  safe, so don't "fix" it alone.
- Trilogy's `count(tree_id) by ecoregion_id` counts a list repeating an
  element once per repeat, while `by biome` dedupes. A summary path
  must match the tree plan, whatever it does.

## Open items, in order

### 0. Pre-existing wrong answer on main: a CASE over a per-bucket aggregate fans out

```
select sum(case when bucket = 'Native' then count(tree_id) by bucket else null end) as n;
```

On the cover suite's model (`BASE` in the test file), the tree plan returns
**8**. The answer is **4**: Native has 4 trees, and the summary plan says 4.
`select sum(count(tree_id) by bucket)` (7, correct) shows the aggregate is
fine; the CASE breaks it. The final CTE reads `bucket` for the CASE from
`enrichment`, which sits at species grain, not from the per-bucket aggregate
CTE that already outputs `bucket`:

```
SELECT sum(CASE WHEN "quizzical"."bucket" = 'Native' THEN "wakeful"."_virt_agg_count_..." END)
FROM "wakeful"
INNER JOIN "quizzical" on "wakeful"."bucket" is not distinct from "quizzical"."bucket"
```

`wakeful` is (bucket, count), and `quizzical` is (species, bucket). The join
repeats each bucket once per species (Native has 2), so the sum doubles. It
reproduces with no summary in scope, so this is a planner sourcing bug, not
a rollup one.

How to take it:
- Model it first. The law: a scalar over an aggregate's own grain columns is
  evaluated on the aggregate's rows. Reading a grain column from a finer table
  joined back on it multiplies each row by that table's rows per value. This
  belongs in `Rows.lean` (FD extension) or a small `Cover.lean` counterexample
  closed by `decide`: per-bucket count `[(N, 4)]`, joined to
  `[(s1, N), (s2, N)]`, sums to 8.
- Then find why the CASE's `bucket` binds to the enrichment scan. The
  aggregate group outputs `bucket`, so the FINAL should read it there. Suspects:
  stage-2 projection or region reads choosing a source for a BASIC's input
  when the same address is offered by two parents, one at a finer grain (the
  join-plan parts 11-20 area; see `project_join_plan_part*` memory notes).
- Add the shape to the suite. Vary it: `max(...)`, a CASE over `count(...) by
  species` with a species property, a CASE over the key itself. It belongs
  under `SIBLINGS` or a new "expression over an aggregate" rule, and the
  summary-off half of `_assert_same_answer` catches it.

### 1. Rule 2, filtered counts (`summary:local-native-share`, `species:sp-biome-fit`)

`count(x ? c)` with `c` a function of the key. `_is_rollable_aggregate` and
the rollup signature reject a `BuildFilterItem` argument. The re-sum is
`sum(case when c then n end)` (`Cover.case_count_exact`), and it must be
rendered where `c`'s columns are. With a species property, that is after the
enrichment join, not at the summary scan, so the rollup render in
`dialect/base.py` `_render_concept_sql` needs the condition in scope.
Ruled out: a build-time rewrite to `sum(count(x) by A ? c)`. It misses too,
and a filtered count is 0 for an empty group where the rewrite is NULL.
Once this lands, `count_distinct(k ? c)` follows through `distinct_key_counts`
(the F2 KPIs).

### 2. Rule 4 under a WHERE the target does not determine (`species:sp-top-species`)

A WHERE finer than the target routes through `_plan_finer_filter_rollup`
(one pinned summary scan). `_summary_leaves` sees the stale total
(`count(tree_id)`) and declines, so the query reads the trees, correctly.
Resolving the walk through the environment lets it plan, but the pinned plan
then GROUPs BY the partials' own values and answers per species: wrong rows.
Fix the pinned plan's grouping first (partials re-summed, never deduped),
then the walk. Repro: the `siblings/scalar` shape under `city`.

### 3. Rule 5, existence (`species:context`)

A select with no aggregate is sourced from its columns' bindings, and the
summary is never offered as proof that a key value exists in the filtered
rows (`Cover.exists_by_value`). This is source selection: `source_planning.py`
and `test_cheapest_source_laws.py`.

### 4. Rule 6, relations keyed on the grain (the nativeness charts)

Join the summary to DISTINCT `(species, value)` pairs and re-sum, matching
the tree plan's multiplicity per element (see the gotcha above). Keep
`fans_out_rollup` and `_group_fanning_parents` intact: they are what holds the
answers right today.

### 5. Small ones

- Rule 1 corner: a scalar of a key (`split(species, ' ')[1]`) under a WHERE
  on `plant_year` isn't read from the year summary.
- `select common_names[1] as c where plant_year > 2000` (and the genus
  version) raises a keyless-join error with no summary at all. These are
  skipped in `TREE_PLAN_RAISES`.

## Done means

Each fixed shape XPASSes and its `OPEN` entry is deleted. Its dashboard pin
in `tests/modeling/tree_reporting/expectations.py` is deleted. The three suites
above and CI are green.

Re-run the sf probe against **this branch and main on the same sf checkout**.
Old probe files used an older sf model and aren't comparable. The PR #842
baseline is main 52/73, branch 62/73. Recipe:
`sf_tree_reporting/docs/PYTRILOGY_BLOCKERS_HANDOFF.md`, using a fresh detached
sf worktree.

The studio pyserver venv has a stale parser extension (`bit_and` parse error).
Copy `pytrilogy/.venv/Lib/site-packages/_preql_import_resolver` to a scratch
dir and put it on `PYTHONPATH` after the trilogy source.
