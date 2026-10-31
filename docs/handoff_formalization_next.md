# Handoff: the next formalization round

`formal/` holds a dependency-free Lean 4 model of Trilogy's semantics. Each
round picks one area, models the documented behaviour, proves the laws, and
writes a counterexample theorem for every place the engine broke them. A Python
law suite then holds the implementation to the theorems. `formal/README.md`
lists every module and, under "What it found", every bug found so far. Read it
first.

Recent rounds: `Assert.lean` (#799, datasource `assert`/`prior`), and
`Functions.lean` (#802, scalar functions per dialect), and `Cost.lean`
(which answering datasource is read: partial summaries, value-pin
implication, dominance ranking).

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

## Candidate areas

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
- Partition families of partial summaries (`complete where` arms that union
  to the table) are untested as rollup sources.
- Not a bug: a summary binding `city: ~city` claims absent cities have no
  rows (padding), so it rolls up as complete and absent cities count 0.

### 2. Optimizer rule soundness

Each rule in `trilogy/core/optimizations/` rewrites CTEs and must preserve rows.
A natural Lean shape is a rule as a function on a small plan AST, with the
theorem `eval (rule p) = eval p` under the rule's guard, plus a counterexample
showing the guard is necessary. Good first targets, roughly in order of how
self-contained they are:
- `strip_redundant_not_null.py`: dropping `x is not null` is sound only when
  the column is proved non-null. `NullReject.lean` already has the proof
  machinery.
- `join_upgrade.py` and `value_set_join_upgrade.py`: outer to inner. Compare
  `NullReject.proofs_sound`.
- `merge_irrelevant_group_by.py` and `identity_group.py`: a GROUP BY is a no-op
  when its keys are a key of the input (`Rows.lean` has `distinct = group`).
- `predicate_pushdown.py`: pushing past a NULL-extending join or an aggregate.
  The guards include `_predicate_safe_past_null_extension`. This one is
  historically the riskiest.
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
