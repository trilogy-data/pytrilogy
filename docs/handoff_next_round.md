# Handoff: the next formalization round (after #865, 0.3.450)

You are picking up the formalization series: one area per round. In each
round you model the documented behaviour in Lean, prove the laws, write a
counterexample theorem for each bug, hold the planner to the theorems with a
Python law suite, and fix what that exposes. The general recipe and the
lessons from every earlier round are in `docs/handoff_formalization_next.md`.
Read its "The recipe" and items 12–14 of the lessons before writing code.
`formal/README.md` lists every module, and under "What it found", every bug
found so far.

**The recommended target is a filtered aggregate whose condition reads a
property of the summary key.** It is the last open piece of the round that
just merged, and it is what the tree_reporting dashboards need. If you take
another area instead, the candidates are listed at the end.

## Setup

```bash
cd pytrilogy
uv venv .venv -q && uv pip install -q --python .venv/bin/python -e ".[all]" -r requirements-test.txt
cd formal && ~/.elan/bin/lake build      # lake is not on PATH; keep the build warning-free
```

- The editable install rebuilds the Rust parser and touches
  `crates/trilogy-io/Cargo.toml` and the two files in
  `trilogy/scripts/dependency/Cargo.*`. Revert those before committing.
- **A/B against main:** run
  `git archive origin/main trilogy | tar -x -C <scratch>/main`, then run the
  probe or suite with `PYTHONPATH=<scratch>/main` from a directory outside
  the repo.
- Run one pytest process at a time. The full suite takes about 26 minutes.
  - Skip `-m adventureworks_execution` and `tests/engine/test_bigquery.py`.
  - Twelve tests fail without Google credentials or GCS keys: Snowflake,
    BigQuery ETL, GCS persistence, `ncaa::test_adhoc03` and
    `the_look::test_wrapped_aggregate_with_dependent_by_grain`.
- CI also runs `python -m local_scripts.fuzzer` (522 cases).
- `codecov/patch` reads low until every build job has uploaded coverage. The
  two `vgi` jobs upload first, and they cover almost nothing. Wait for the
  `ubuntu-latest-py3.x` jobs before acting on it.

## What #865 did (the starting point)

A summary storing `agg(x)` now answers `agg(x ? c)` for `count`, `sum`, `min`,
`max` and, through `distinct_key_counts`, `count_distinct`, when the summary
**binds every column `c` reads**. The proof is
`Cover.filtered_sum_exact`, `filtered_min_exact`, `filtered_max_exact` and
`case_count_exact`.

| Piece | Where |
|---|---|
| `agg(x ? c)` → the signature of the stored `agg(x)` plus the filter | `BuildConcept.filtered_rollup` (`trilogy/core/models/build.py`) |
| The stored column for it, used only on rollup paths | `BuildDatasource.filtered_column_for`; `rollup_column_for` falls back to it |
| The matcher gate: summary binds `c`'s columns, no aggregate in `c` | `_datasource_has_matching_rollup_aggregate` (`trilogy/core/processing/aggregate_rollup.py`) |
| A scan passing raw partials up to a later re-sum | the "partial of itself" rule in `create_datasource_node` (`node_generators/select_helpers/datasource_nodes.py`) |
| `reagg(CASE WHEN c THEN n END)`, with `coalesce(…, 0)` for a count | rollup branch of `_render_concept_sql` (`trilogy/dialect/base.py`) |
| The CASE goes on only where the walk reaches a table storing the unfiltered partial alone | `_reads_unfiltered_partial` (`base.py`) |
| Fan-out guard reads the rows of `x`, not of `x ? c` | `_aggregate_inputs` (`aggregate_rollup.py`) |

**A design rule from the maintainer: never rewrite the concept the user asked
for into something a summary happens to serve.** The first version of #865 did
exactly that in the Factory, and it was rejected. It decided the plan before
the planner chose a source. It changed the concept that a stored
`count(x ? c)` would have matched. It added a rewrite stage whose order
mattered. Extend matching and rendering instead. If an invariant fails later,
the match fails and the query falls back to the rows; it cannot be rewritten
into something wrong.

## The target: a condition on a property of the summary key

```
select city, count(tree_id ? bucket = 'Native') as n;      -- bucket is a property of species
```

The summary stores `count(tree_id)` at `(city, species)`, and `bucket` comes
from the `enrichment` dimension. Today this reads the tree rows. The answer is
right; the plan is slow.

**Why it doesn't match today.** The matcher requires the summary to bind
`bucket` (`_conditions_supported(datasource, where)` with no
`concepts_by_address`). If you allow property reach there (pass `{}`), the
summary scan tries to re-aggregate on its own and render
`CASE WHEN bucket …` with no `bucket` in scope: `Missing source reference to
local.bucket`. Re-aggregation at the scan only sees the scan's columns.

**The plan shape already exists for a WHERE.**
`select city, count(tree_id) where bucket = 'Native'` already plans as: the
summary scan keeps `species`, joins `enrichment`, and re-sums after the join.
`merge_rollup_concepts` is the join-then-re-sum, and #865 made it work for
filtered concepts with bound conditions. The difference is that a WHERE's
columns enter the search as conditions, while a filtered aggregate's
condition columns enter as nothing.

**Two problems to solve, in this order:**

1. **The soundness guard, in Lean first.** The CASE moves from "the first step
   that re-aggregates the stored column" to "the first step whose rows carry
   `c`'s columns". Steps below it then re-sum unfiltered partials. That is
   exact only if every grouping below the CASE still determines `c`'s columns
   (groups by `species` when `c` reads `bucket`).
   - Write the law: re-summing partials at a grain that determines `c`, then
     the CASE, equals the CASE at the summary grain.
   - Write the counterexample: one step drops `species`, so a group mixes
     Native and non-Native rows and the CASE counts them all or none.
   - Model it on `Cover.partial_off_condition_misreads`.
   - Today a missing column makes the render raise. Keep that property: a
     plan that would put the CASE above a grouping that does not determine
     `c` must not match. It must never render with a skipped or misplaced
     CASE.
2. **Condition inputs that do not group.** The rolled root's scan needs
   `species` (the key of `bucket`) in its target, and the merge needs
   `bucket` as an input that is neither an output nor a grouping key.
   - Look at how `materialized_root_addresses` (`concept_strategies_v4.py`)
     gathers `condition_args_by_address` from the WHERE, and how the root
     cluster carries them.
   - Don't use the HAVING presence probes (`having_presence_probes` in
     `query_processor.py`): they add outputs, which changes the grain.
   - Fix it where the inputs are demanded, not by widening late. Lesson 9.3
     in the general handoff covers the same trap for keys.

**Tests that tell you you're done.** Each of these flips from a strict xfail
to a pass; delete its mark when it does.

- `tests/discovery/test_filtered_rollup_laws.py`: 480 strict xfails marked
  `JOINED_CONDITION` (conditions `property` and `or`).
  `test_filtered_aggregate_answers` already holds those cases to the
  reference, and must stay green throughout.
- `tests/discovery/test_summary_cover_laws.py`: the `OPEN` entries for
  `("filtered", shape)` with `property`, `or`, `ratio`, `beside_plain` and
  `list_property`. `list_property` reads `common_names[1]`, a scalar of a
  property.
- `tests/modeling/tree_reporting/expectations.py`: the `"trees"` pins for
  `summary:local-native-share` (and check `species:sp-biome-fit`, which may
  also need rule 6). A flip to `summary` means deleting the pin, never
  editing it.
- Add a law-suite target that groups by a column that does not determine
  `bucket` (`city` alone, plus `plant_year`). It is the shape where a
  misplaced CASE would be wrong; prove it never is.

## Gotchas from #865

- **Check the exact match next to every new rollup path.** A summary may store
  `agg(x ? c)` itself. The first matcher version wrapped that column in the
  CASE again and crashed; `test_stored_filtered_aggregate_is_read_as_stored`
  pins it (80 cases, a stored filtered column under a `bucket` condition).
- **A strict xfail hides crashes as well as misses.** Keep the answer
  assertion in its own test, not behind the xfail
  (`test_filtered_aggregate_answers`, `test_open_shape_answers_as_the_trees`).
- **The statement's lineage is stale after `_reaggregate_partials`.** Walks
  over it must `resolve` through `environment.concepts`.
  `_nested_rollup_roots`, `_summary_leaves` and the rollup render (which uses
  the planned concept from `cte.rollup_concepts`) already do. A new walk must
  too.
- **Probe with `_run(BASE + SUMMARY, q)` from the cover suite.**
  `classify.py`-style loops over every shape × filter give a
  MISS / WRONG / ERR table in about 30 seconds. Run the same loop against
  main for the A/B.
- **Remove each planner change one at a time** and rerun both law suites
  before opening the PR. Pin anything a change fixes that the suites don't
  reach, as `test_rewritten_partials_read_the_summary` does for three of
  #865's fixes.
- **The test data's claims must be true.** Every species in `trees` has an
  `enrichment` row, and `bucket` may be NULL. A random tree whose species
  the dimension lacks makes a correct inner join look like a dropped row.

## Smaller leads in the same area

- A summary storing `agg(x)` at exactly the target grain does not answer
  `agg(x ? c)`. The matcher pairs them only where partials are re-aggregated;
  serving it needs the CASE without a GROUP BY.
- `siblings/scalar` under a WHERE on a stored column right-joins the summary to
  the trees' distinct `(city, species, plant_year)`. The answer is right; the
  scan is unnecessary. It is an `OPEN` entry in the cover suite.
- Retire stale lineage as a class: make `_reaggregate_partials` rewrite the
  statement's nested lineage, not only its top-level selection, then drop the
  per-walk `resolve`s.

## Other areas, if you pick a different round

All of these are in `docs/handoff_formalization_next.md` with detail:
- **Area 4.** A select of an aggregate at a finer grain repeats tuples
  (`select g, sum(x) by id`); a value window's keys are its operand. Both
  need a decision on semantics first.
- **Area 3.** Decimal precision of a persisted `sum`; `random(n)` means
  different things per dialect; chained casts (`t::date::datetime`) raise.
- **Area 2.** `merge_irrelevant_group_by` never fired on any shape. Count its
  firings over TPC-DS before writing more shapes; it may be dead.
- **Area 1b.** Source dominance compares grains by component subset, not by
  the FD closure.

## Done means

- The Lean module builds warning-free, with a law and a counterexample per
  bug.
- The xfails above flip, and each flip's mark or pin is deleted.
- The A/B numbers ("N of M fail on main") go in the PR.
- `formal/README.md` gets a row update and a "What it found" bullet, and
  `docs/handoff_formalization_next.md` gets a lessons item and the area-1c
  update.
- `trilogy/__init__.py` is bumped.
- `ruff`, `black` and `mypy trilogy` pass.
- The full suite and the fuzzer are green apart from the credential failures
  listed above.
- The TPC timing logs and charts the suites regenerate are committed.
